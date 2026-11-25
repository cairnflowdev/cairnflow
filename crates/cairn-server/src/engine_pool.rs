// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A fixed thread pool for the engines of many ranges.
//!
//! An engine is `!Send` (INV-4), so it must live entirely on **one** thread —
//! but nothing requires that thread to run only **one** engine. Before this
//! pool each range had its own OS thread, with its own `current_thread`
//! runtime: a node with 256 ranges meant 256 threads, 256 timers, 256 IO
//! drivers (measured: ~1 thread and ~0.35 MiB per idle partition). Here there
//! are `n` fixed threads; each runs **one** `current_thread` runtime + **one**
//! `LocalSet`, and that `LocalSet` holds the lifecycles of many ranges. A
//! range's engine is born, runs and dies on exactly one thread — the compiler
//! still enforces that through `!Send`, just as before.
//!
//! # Which thread a range goes to
//!
//! The thread currently holding the fewest ranges (counting live ranges).
//! Ranges do not move between threads — once an engine is open its memory
//! belongs to that thread.
//!
//! # The price
//!
//! Ranges sharing a thread share its CPU: a long computation without an
//! `await` in range A's engine makes range B on the same thread wait.
//! Previously it only stalled A itself. Two things keep this price small:
//!
//! - an engine mostly waits on IO (object store, SDK) rather than computing;
//! - SlateDB's heaviest computation — compaction and GC — runs on the node's
//!   own background runtime
//!   ([`cairn_store::ObjectStores::with_background_runtime`]), not on the
//!   ranges' threads.
//!
//! # How many threads
//!
//! The default is [`EnginePool::default_threads`] = the number of cores. Fewer,
//! and two busy ranges compete for one core while another core idles; more
//! adds no CPU, only context switches and sleeping threads. Tune it with
//! `--engine-threads`.

use {
    std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    },
    tokio::sync::{mpsc, watch},
};

/// **The node-wide turn cap** (D-c): `--max-concurrent-turns` is split evenly
/// among the engines the node leads, not a cap per range — otherwise every
/// range split would raise the node's total cap by one more share, exactly
/// what this cap exists to prevent.
///
/// The split rounds **down** (the shares never exceed the total cap), with at
/// least 1 per engine: a node leading more ranges than the total cap still
/// lets each range run one turn — better slightly over the cap than a range
/// standing still.
///
/// **The even share is a guarantee, not a private cap**: beyond its share, a
/// busy range may **borrow** slots the node is leaving idle ([`NodeTurns`]).
/// A rigid even split once left a range with 94 000 pending commands crawling
/// along on 6 slots while the node's other ranges left their shares idle. The
/// total **never** exceeds the cap (the cap protects the customer's SDK
/// deployment): a range below its share while the node is full is
/// "starving", and as long as any range is starving nobody borrows more — a
/// borrowed turn, once done, hands its slot back to it.
#[derive(Clone, Debug)]
pub struct TurnBudget(Arc<BudgetInner>);

#[derive(Debug)]
struct BudgetInner {
    total: usize,
    leading: AtomicUsize,
    /// Turns running on the node = the sum of `turns` over the **live** cells.
    /// Kept as one atomic so the slot-request path takes no lock: every change
    /// of a cell adds **exactly the change** here, and a dying cell subtracts
    /// exactly what it still holds — so the total always equals the sum of the
    /// live cells, with no accumulating counter to leak.
    in_use: AtomicUsize,
    /// Number of cells currently `starving`, same rule.
    starving: AtomicUsize,
    /// Changes whenever the number of led engines changes, or the node has room again.
    changed: watch::Sender<u64>,
    /// The node's per-tenant rate buckets + `vt` — see [`crate::rate`].
    tenants: crate::rate::TenantBook,
}

/// The share of **one** range scheduler in the node's counter.
#[derive(Debug, Default)]
struct Slot {
    /// Turns running in the range — a copy of its `turns_in_use`.
    turns: AtomicUsize,
    /// The range is below its guaranteed share while the node is full.
    starving: AtomicBool,
}

impl TurnBudget {
    pub fn new(total: usize) -> Self {
        let tenants = crate::rate::TenantBook::new();
        tenants.spawn_sweeper();
        Self::with_tenants(total, tenants)
    }

    /// Like [`Self::new`] with a given tenant registry, without the background
    /// sweep — [`crate::flow`]'s sim calls `sweep` itself on its own clock.
    pub(crate) fn with_tenants(total: usize, tenants: crate::rate::TenantBook) -> Self {
        Self(Arc::new(BudgetInner {
            total: total.max(1),
            leading: AtomicUsize::new(0),
            in_use: AtomicUsize::new(0),
            starving: AtomicUsize::new(0),
            changed: watch::Sender::new(0),
            tenants,
        }))
    }

    /// The node's tenant registry (rate buckets, `vt`).
    pub fn tenants(&self) -> &crate::rate::TenantBook {
        &self.0.tenants
    }

    pub fn total(&self) -> usize {
        self.0.total
    }

    /// One engine's share right now.
    pub fn share(&self) -> usize {
        (self.0.total / self.0.leading.load(Ordering::Acquire).max(1)).max(1)
    }

    /// An engine became leader: count it until the guard is dropped.
    pub(crate) fn lead(&self) -> Leading {
        self.0.leading.fetch_add(1, Ordering::AcqRel);
        self.0.changed.send_modify(|n| *n += 1);
        Leading(Arc::clone(&self.0))
    }

    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.0.changed.subscribe()
    }

    /// A new cell in the node's counter, for one range's scheduler. The cell
    /// lives as long as [`NodeTurns`] does.
    pub(crate) fn node_turns(&self) -> NodeTurns {
        NodeTurns {
            inner: Arc::clone(&self.0),
            mine: Slot::default(),
        }
    }
}

/// A range's share of the node's turn cap — so a busy range can borrow slots
/// other ranges leave idle.
///
/// **The total is the sum of the live cells, not an accumulating counter.**
/// The first version added/subtracted a shared atomic per turn, and any path
/// that forgot to give back (a permit abandoned on step-down, a range retiring
/// with a turn in flight, an old term's permit released after the new term
/// had reset) leaked forever: in a 10-minute chaos run the node reported full
/// while running a single turn. Now a cell dies with its scheduler (range
/// retired, owner changed — each term attaches a new cell), so nothing can
/// leak beyond one term.
///
/// Cross-thread reads take no lock: "is there room" is read before adding, so
/// two ranges grabbing the last slot at once overshoot the cap by one turn —
/// rare, and it clears itself when a turn finishes.
///
/// The node's total is two atomics (`in_use`, `starving`) to which each cell
/// adds/subtracts exactly its own share (previously every `turn_room` locked a
/// node-wide `Mutex` to sum the cells). A dying (dropped) cell gives back
/// exactly what it still holds, so the total does not leak across an owner
/// change.
#[derive(Debug)]
pub(crate) struct NodeTurns {
    inner: Arc<BudgetInner>,
    mine: Slot,
}

/// Sim: a snapshot of the node's counter and one range's cell (FL-7).
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct NodeProbe {
    pub(crate) in_use: usize,
    pub(crate) total: usize,
    pub(crate) leading: usize,
    pub(crate) starving: usize,
    /// This cell's turns, and whether this cell is starving.
    pub(crate) mine: usize,
    pub(crate) mine_starving: bool,
}

impl NodeTurns {
    #[cfg(test)]
    pub(crate) fn probe(&self) -> NodeProbe {
        NodeProbe {
            in_use: self.in_use(),
            total: self.inner.total,
            leading: self.inner.leading.load(Ordering::Acquire),
            starving: self.inner.starving.load(Ordering::Acquire),
            mine: self.mine.turns.load(Ordering::Acquire),
            mine_starving: self.mine.starving.load(Ordering::Acquire),
        }
    }

    fn in_use(&self) -> usize {
        self.inner.in_use.load(Ordering::Acquire)
    }

    /// The node is still under its total cap.
    pub(crate) fn has_room(&self) -> bool {
        self.in_use() < self.inner.total
    }

    /// The node leads more ranges than the total cap: the guaranteed share
    /// (floor 1 per range) may run even over the cap — better slightly over
    /// than a range standing still.
    pub(crate) fn oversubscribed(&self) -> bool {
        self.inner.leading.load(Ordering::Acquire) > self.inner.total
    }

    /// Whether any range is waiting for its guaranteed share — if so, nobody may borrow.
    pub(crate) fn anyone_starving(&self) -> bool {
        self.inner.starving.load(Ordering::Acquire) > 0
    }

    pub(crate) fn starve(&self) {
        if !self.mine.starving.swap(true, Ordering::AcqRel) {
            self.inner.starving.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn fed(&self) {
        if self.mine.starving.swap(false, Ordering::AcqRel) {
            self.inner.starving.fetch_sub(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn take(&self) {
        self.mine.turns.fetch_add(1, Ordering::AcqRel);
        self.inner.in_use.fetch_add(1, Ordering::AcqRel);
    }

    /// Gives back `n` turns of this range. `true` ⇒ the node just went from
    /// full to having room: the caller tells every range with
    /// [`Self::announce`] so waiting ranges retry — after the call into the
    /// scheduling core, because the core touches no channels.
    pub(crate) fn give(&self, n: usize) -> bool {
        if n == 0 {
            return false;
        }
        let had = self
            .mine
            .turns
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(n))
            })
            .unwrap_or(0);
        let gave = had.min(n);
        if gave == 0 {
            return false;
        }
        let before = self.inner.in_use.fetch_sub(gave, Ordering::AcqRel);
        let after = before.saturating_sub(gave);
        before >= self.inner.total && after < self.inner.total
    }

    /// Tells every range (through [`TurnBudget::changes`]) that the node has room again.
    pub(crate) fn announce(&self) {
        self.inner.changed.send_modify(|c| *c += 1);
    }
}

impl Drop for NodeTurns {
    /// A cell dies with its scheduler: the turns it still counts (including
    /// abandoned turns in flight) leave the total at once — tell the other
    /// ranges to retry.
    fn drop(&mut self) {
        let turns = self.mine.turns.swap(0, Ordering::AcqRel);
        if turns > 0 {
            self.inner.in_use.fetch_sub(turns, Ordering::AcqRel);
        }
        let starving = self.mine.starving.swap(false, Ordering::AcqRel);
        if starving {
            self.inner.starving.fetch_sub(1, Ordering::AcqRel);
        }
        if turns > 0 || starving {
            self.inner.changed.send_modify(|c| *c += 1);
        }
    }
}

/// An engine that is leading, counted in [`TurnBudget`].
pub(crate) struct Leading(Arc<BudgetInner>);

impl Drop for Leading {
    fn drop(&mut self) {
        self.0.leading.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.send_modify(|n| *n += 1);
    }
}

/// Work sent to one pool thread: a `Send` closure that builds a `!Send` future
/// **on** the target thread.
pub(crate) type Job = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + Send>;

struct Worker {
    tx: mpsc::UnboundedSender<Job>,
    /// Number of jobs (range lifecycles) alive on this thread.
    load: Arc<AtomicUsize>,
}

/// The engine thread pool. `Clone` is cheap; threads exit once every clone has
/// been dropped **and** every job on them has finished.
#[derive(Clone)]
pub struct EnginePool {
    workers: Arc<Vec<Worker>>,
    /// The node's turn cap (D-c). `None` ⇒ each engine keeps its own cap.
    budget: Option<TurnBudget>,
    /// The node's range-lifecycle measurements — see [`crate::range_stats`].
    stats: crate::range_stats::RangeStats,
    /// The node's per-handler summary — see [`crate::stats`].
    handlers: crate::stats::HandlerStats,
    /// `node_id` of the node owning the pool: every job sets it as the
    /// thread's node, so node-conditioned failpoints know where they are.
    node: Option<Arc<str>>,
    /// The node's per-range load registry — see [`crate::autosplit`].
    load: crate::autosplit::LoadBook,
    /// The node's stream RAM cap — see [`crate::streams`].
    streams: crate::streams::StreamBook,
    /// The query layer's read-handle registry.
    query_ranges: crate::query::QueryRanges,
    /// The node's egress policy: the tenant client of every range term is
    /// built from it.
    egress: Arc<crate::egress::EgressPolicy>,
    /// The node's request-identity signer.
    identity: crate::identity::Identity,
    /// The issuer of segment read URLs for SDK v6.
    value_urls: Arc<crate::value_urls::ValueUrls>,
}

impl std::fmt::Debug for EnginePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnginePool")
            .field("threads", &self.threads())
            .field("load", &self.load())
            .finish()
    }
}

impl EnginePool {
    /// Default number of threads: the number of cores the machine reports (at least 1).
    pub fn default_threads() -> usize {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    }

    /// Builds `threads` threads (at least 1), each with one `current_thread`
    /// runtime + one `LocalSet`. `Err` if a thread or runtime cannot be built.
    pub fn new(threads: usize) -> std::io::Result<Self> {
        let mut workers = Vec::new();
        for i in 0..threads.max(1) {
            workers.push(spawn_worker(i)?);
        }
        Ok(Self {
            workers: Arc::new(workers),
            budget: None,
            stats: Default::default(),
            handlers: Default::default(),
            node: None,
            load: Default::default(),
            streams: Default::default(),
            query_ranges: Default::default(),
            egress: Default::default(),
            identity: Default::default(),
            value_urls: Default::default(),
        })
    }

    /// A pool **with no threads of its own**: every range runs on the caller's
    /// `LocalSet` (must be called inside a `LocalSet`). Only for deterministic
    /// simulation: the whole cluster lives on one `current_thread` runtime with
    /// a paused clock, so engines must not live on any other runtime. Panics
    /// when not inside a `LocalSet`.
    #[cfg(feature = "testkit")]
    pub fn inline() -> Self {
        Self {
            workers: Arc::new(vec![spawn_inline_worker()]),
            budget: None,
            stats: Default::default(),
            handlers: Default::default(),
            node: None,
            load: Default::default(),
            streams: Default::default(),
            query_ranges: Default::default(),
            egress: Default::default(),
            identity: Default::default(),
            value_urls: Default::default(),
        }
    }

    /// Attaches the `node_id` of the owning node — see the `node` field.
    pub fn with_node(mut self, node: &str) -> Self {
        self.node = Some(Arc::from(node));
        self
    }

    /// Sets the **node-wide** turn cap for every engine built on this pool (D-c).
    pub fn with_turn_budget(mut self, total: usize) -> Self {
        self.budget = Some(TurnBudget::new(total));
        self
    }

    /// Cap on a tenant's running turns on the node — after
    /// [`Self::with_turn_budget`]; ignored when the node has no turn cap.
    pub fn with_tenant_turn_cap(self, cap: Option<usize>) -> Self {
        if let Some(b) = &self.budget {
            b.tenants().set_turn_cap(cap);
        }
        self
    }

    pub fn turn_budget(&self) -> Option<&TurnBudget> {
        self.budget.as_ref()
    }

    pub fn range_stats(&self) -> &crate::range_stats::RangeStats {
        &self.stats
    }

    pub fn handler_stats(&self) -> &crate::stats::HandlerStats {
        &self.handlers
    }

    pub fn load_book(&self) -> &crate::autosplit::LoadBook {
        &self.load
    }

    /// Sets the node's stream RAM cap and temporary-stream release grace for
    /// every engine built on this pool.
    pub fn with_streams(mut self, cfg: &crate::streams::StreamConfig) -> Self {
        self.streams =
            crate::streams::StreamBook::new(cfg.node_ram).with_idle_release(cfg.idle_release);
        self
    }

    pub fn stream_book(&self) -> &crate::streams::StreamBook {
        &self.streams
    }

    /// The query layer's read-handle registry.
    pub fn query_ranges(&self) -> &crate::query::QueryRanges {
        &self.query_ranges
    }

    /// Sets the node's egress policy.
    pub fn with_egress(mut self, policy: Arc<crate::egress::EgressPolicy>) -> Self {
        self.egress = policy;
        self
    }

    pub fn egress(&self) -> &Arc<crate::egress::EgressPolicy> {
        &self.egress
    }

    /// Sets the node's request-identity signer.
    pub fn with_identity(mut self, identity: crate::identity::Identity) -> Self {
        self.identity = identity;
        self
    }

    pub fn identity(&self) -> &crate::identity::Identity {
        &self.identity
    }

    /// Sets the node's issuer of segment read URLs.
    pub fn with_value_urls(mut self, urls: Arc<crate::value_urls::ValueUrls>) -> Self {
        self.value_urls = urls;
        self
    }

    pub fn value_urls(&self) -> &Arc<crate::value_urls::ValueUrls> {
        &self.value_urls
    }

    pub fn threads(&self) -> usize {
        self.workers.len()
    }

    /// Number of ranges alive on each thread.
    pub fn load(&self) -> Vec<usize> {
        self.workers
            .iter()
            .map(|w| w.load.load(Ordering::Relaxed))
            .collect()
    }

    /// Sends `job` to the thread holding the fewest ranges. `Err` if that
    /// thread has died (its runtime panicked).
    pub(crate) fn spawn(&self, job: Job) -> Result<(), Job> {
        // A pool thread only runs the ranges of one node, so setting the
        // thread's node on every job is enough. Without the `failpoints`
        // feature this does nothing.
        let job: Job = match (&self.node, cairn_store::failpoint::enabled()) {
            (Some(node), true) => {
                let node = Arc::clone(node);
                Box::new(move || {
                    cairn_store::failpoint::set_thread_node(&node);
                    job()
                })
            }
            _ => job,
        };
        let w = self
            .workers
            .iter()
            .min_by_key(|w| w.load.load(Ordering::Relaxed))
            .expect("the pool has at least one thread");
        w.load.fetch_add(1, Ordering::Relaxed);
        w.tx.send(job).map_err(|e| {
            w.load.fetch_sub(1, Ordering::Relaxed);
            e.0
        })
    }
}

/// The process's background runtime for SlateDB compaction and GC.
///
/// Multi-threaded, shared by every Db of every node in the process — the
/// thread count does not follow the number of ranges. `max(1, cores / 4)`:
/// compaction is background work and must not take more than a quarter of the
/// engines' CPU; several compactions running at once queue here instead of
/// crowding into engine threads.
///
/// Built on first call, lives until the process ends (a static is never
/// dropped — there is no safe place to drop a runtime that is running another
/// Db's work).
pub fn background_runtime() -> std::io::Result<tokio::runtime::Handle> {
    static RT: std::sync::OnceLock<Result<tokio::runtime::Runtime, String>> =
        std::sync::OnceLock::new();
    let rt = RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(background_threads())
            .thread_name("cairn-bg")
            .enable_all()
            .build()
            .map_err(|e| e.to_string())
    });
    match rt {
        Ok(rt) => Ok(rt.handle().clone()),
        Err(e) => Err(std::io::Error::other(e.clone())),
    }
}

/// Number of threads of [`background_runtime`].
pub fn background_threads() -> usize {
    (EnginePool::default_threads() / 4).max(1)
}

/// Subtracts the thread's load when a job ends — on every way out, panics
/// included (`LocalSet` swallows a job's panic, and subtracting by hand after
/// the `await` would leave that thread's load skewed forever, with new work
/// avoiding it).
struct Unload(Arc<AtomicUsize>);

impl Drop for Unload {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn spawn_worker(i: usize) -> std::io::Result<Worker> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
    let load = Arc::new(AtomicUsize::new(0));
    let (built_tx, built_rx) = std::sync::mpsc::channel::<std::io::Result<()>>();
    let thread_load = Arc::clone(&load);
    std::thread::Builder::new()
        .name(format!("cairn-engine-{i}"))
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = built_tx.send(Err(e));
                    return;
                }
            };
            let _ = built_tx.send(Ok(()));
            // `LocalSet` is what allows `spawn_local` — `!Send` tasks running
            // on exactly this thread. Await the `LocalSet` itself (not
            // `run_until`): it only completes once the job-receiving loop has
            // exited (every `EnginePool` dropped) **and** every range on the
            // thread has finished.
            let local = tokio::task::LocalSet::new();
            local.spawn_local(async move {
                while let Some(job) = rx.recv().await {
                    let load = Unload(Arc::clone(&thread_load));
                    tokio::task::spawn_local(async move {
                        let _load = load;
                        job().await;
                    });
                }
            });
            rt.block_on(local);
        })?;
    match built_rx.recv() {
        Ok(Ok(())) => Ok(Worker { tx, load }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::other("engine thread died during build")),
    }
}

/// The worker of [`EnginePool::inline`]: the job-receiving loop is a
/// `spawn_local` task on the current `LocalSet` instead of an OS thread.
#[cfg(feature = "testkit")]
fn spawn_inline_worker() -> Worker {
    let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
    let load = Arc::new(AtomicUsize::new(0));
    let thread_load = Arc::clone(&load);
    tokio::task::spawn_local(async move {
        while let Some(job) = rx.recv().await {
            let load = Unload(Arc::clone(&thread_load));
            tokio::task::spawn_local(async move {
                let _load = load;
                job().await;
            });
        }
    });
    Worker { tx, load }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{cell::Cell, rc::Rc, time::Duration},
    };

    /// `!Send` work runs on the pool's threads, and the threads are shared evenly.
    #[test]
    fn non_send_work_runs_on_pool_threads_and_spreads_evenly() {
        let pool = EnginePool::new(2).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        for _ in 0..4 {
            let tx = tx.clone();
            let mut stop = stop_rx.clone();
            pool.spawn(Box::new(move || {
                Box::pin(async move {
                    // `Rc` ⇒ a `!Send` future: it can only run if it lives
                    // entirely on one thread.
                    let here = Rc::new(Cell::new(0));
                    here.set(1);
                    let name = std::thread::current().name().map(str::to_owned);
                    tx.send(name).unwrap();
                    let _ = stop.wait_for(|s| *s).await;
                    assert_eq!(here.get(), 1);
                })
            }))
            .map_err(|_| "pool died")
            .unwrap();
        }
        let mut names: Vec<_> = (0..4)
            .map(|_| rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap())
            .collect();
        names.sort();
        assert_eq!(
            names,
            [
                "cairn-engine-0",
                "cairn-engine-0",
                "cairn-engine-1",
                "cairn-engine-1"
            ]
        );
        assert_eq!(pool.load(), [2, 2]);
        stop_tx.send(true).unwrap();
        let t0 = tokio::time::Instant::now();
        while pool.load() != [0, 0] {
            assert!(t0.elapsed() < Duration::from_secs(5), "job did not finish");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Inline pool: `!Send` work runs on the calling thread itself, in its
    /// `LocalSet` — no OS thread is built.
    #[cfg(feature = "testkit")]
    #[test]
    fn inline_pool_runs_on_callers_localset() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let here = std::thread::current().id();
        local.block_on(&rt, async move {
            let pool = EnginePool::inline();
            let (tx, rx) = tokio::sync::oneshot::channel();
            pool.spawn(Box::new(move || {
                Box::pin(async move {
                    let rc = Rc::new(Cell::new(std::thread::current().id()));
                    tokio::task::yield_now().await;
                    let _ = tx.send(rc.get());
                })
            }))
            .map_err(|_| "pool died")
            .unwrap();
            assert_eq!(rx.await.unwrap(), here);
            assert_eq!(pool.threads(), 1);
            while pool.load() != [0] {
                tokio::task::yield_now().await;
            }
        });
    }
}
