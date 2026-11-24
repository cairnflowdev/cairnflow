// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-handler summaries — inspired by TiDB's statement summary: a bounded LRU,
//! rotating windows, evicted entries merged into one "others" row.
//!
//! The **key** is `(tenant, service, handler, deployment)`. The deployment is part of
//! the key because the usual fix for a broken handler is deploying a new revision, and
//! the operator must see the healed revision separately from the old one.
//!
//! **Each entry** counts SDK turns, invocations stopped by state (completed,
//! failed, paused), failed attempts (turns ending with a retry or a thrown error),
//! suspensions, durable steps, total run time, and a turn duration histogram to
//! give p50/p99/max.
//!
//! **The hot path has no shared lock.** Each leader term of a range has its own
//! [`Shard`], written only by that range's engine thread (its mutex is uncontended,
//! except for the collector once per second). The background collector
//! ([`HandlerStats::drain`], a 1 s tick, like `metering`) swaps each shard's map out and
//! merges it into the node's summary.
//!
//! **Windows** of 5 minutes, aligned to wall-clock time; 12 windows (one hour) are kept
//! in RAM. **Limit** 2 000 keys per node, evicted by last seen (LRU). An evicted key's
//! numbers are merged into the `(others)` row **of that same tenant** — so a tenant key
//! still sees its own totals, and nobody sees another tenant's numbers. `(others)` rows
//! do not count against the limit: their number is bounded by the number of tenants.
//!
//! **No Prometheus labels here** (deliberately, like `flow/`): the cardinality lives in
//! the bounded LRU, not in metrics.
//!
//! **The second user** is poison-handler quarantine: [`HandlerStats::health`] returns
//! the number of failed attempts, the number of *distinct* invocations that failed and the
//! number of completed invocations per handler, over a span of windows — exactly the three
//! numbers the quarantine rule needs. Two sources of truth for the same question are
//! forbidden by design, so quarantine reads from here.
//!
//! A **turn** here is one SDK call (`turn::drive_one`, the same place metering measures
//! `run_ms`). The slot wait, engine wait and durability wait of the whole run are
//! recorded by the turn trace ([`TurnTrace`]) for the slow log.

use cairn_store::entropy::HashMap;
// The runtime's clock: outside simulation it is `std::time::Instant`, inside
// deterministic simulation it is the virtual clock.
use tokio::time::Instant;
use {
    cairn_store::InvocationId,
    serde::{Deserialize, Serialize},
    std::{
        cell::RefCell,
        collections::BTreeMap,
        future::Future,
        sync::{Arc, Mutex},
        time::Duration,
    },
};

/// Length of one window.
pub const WINDOW_MS: u64 = 300_000;
/// Number of windows kept in RAM.
pub const KEPT_WINDOWS: u64 = 12;
/// Limit on keys per node (not counting `(others)` rows).
pub const DEFAULT_CAPACITY: usize = 2_000;
/// Service/handler name of the row merging evicted keys.
pub const OTHERS: &str = "(others)";
/// Limit on failed invocation ids remembered per entry — enough for the quarantine rule's
/// "≥ 5 distinct invocations", and it bounds the memory of a handler failing on every input.
pub const FAILED_IDS_CAP: usize = 32;

/// Identity of one summary row.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HandlerKey {
    pub tenant: String,
    pub service: String,
    pub handler: String,
    /// Id of the deployment the invocation is bound to — empty in the `(others)` row.
    pub deployment: String,
}

impl HandlerKey {
    fn others(tenant: &str) -> Self {
        Self {
            tenant: tenant.to_string(),
            service: OTHERS.into(),
            handler: OTHERS.into(),
            deployment: String::new(),
        }
    }

    pub fn is_others(&self) -> bool {
        self.service == OTHERS && self.handler == OTHERS && self.deployment.is_empty()
    }
}

// ─────────────────────────────────────────────── duration histogram

/// Log histogram of turn durations, in µs: 4 buckets per power of 2, so a
/// quantile read from it is off by at most 25 % (the upper bound of its bucket), and never
/// exceeds `max` (measured exactly).
///
/// Bucket `i < 4` is exactly `i` µs; bucket `i ≥ 4` has `e = i/4 + 1`, covering
/// `[(4 + i%4) << (e-2), (5 + i%4) << (e-2))`. The Vec only reaches the highest bucket
/// ever seen, so a steady handler costs a few dozen `u32`s.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Durations {
    counts: Vec<u32>,
    max_us: u64,
}

impl Durations {
    fn bucket(us: u64) -> usize {
        if us < 4 {
            return us as usize;
        }
        let e = 63 - us.leading_zeros() as u64;
        let sub = (us >> (e - 2)) & 3;
        (4 * (e - 1) + sub) as usize
    }

    /// Upper bound (exclusive) of bucket `i`, µs.
    fn upper(i: usize) -> u64 {
        if i < 4 {
            return i as u64 + 1;
        }
        let e = (i / 4 + 1) as u32;
        let sub = (i % 4) as u64;
        (5 + sub).checked_shl(e - 2).unwrap_or(u64::MAX)
    }

    pub fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        let i = Self::bucket(us);
        if self.counts.len() <= i {
            self.counts.resize(i + 1, 0);
        }
        self.counts[i] = self.counts[i].saturating_add(1);
        self.max_us = self.max_us.max(us);
    }

    pub fn merge(&mut self, o: &Durations) {
        if self.counts.len() < o.counts.len() {
            self.counts.resize(o.counts.len(), 0);
        }
        for (a, b) in self.counts.iter_mut().zip(&o.counts) {
            *a = a.saturating_add(*b);
        }
        self.max_us = self.max_us.max(o.max_us);
    }

    pub fn count(&self) -> u64 {
        self.counts.iter().map(|c| u64::from(*c)).sum()
    }

    /// Quantile `q` ∈ (0, 1], µs. `0` when empty.
    pub fn quantile_us(&self, q: f64) -> u64 {
        let n = self.count();
        if n == 0 {
            return 0;
        }
        let rank = ((q * n as f64).ceil() as u64).clamp(1, n);
        let mut seen = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            seen += u64::from(*c);
            if seen >= rank {
                return Self::upper(i).min(self.max_us);
            }
        }
        self.max_us
    }

    pub fn max_us(&self) -> u64 {
        self.max_us
    }

    /// Buckets and `max` — to store and merge elsewhere (durable statistics).
    pub fn parts(&self) -> (&[u32], u64) {
        (&self.counts, self.max_us)
    }

    pub fn from_parts(counts: Vec<u32>, max_us: u64) -> Self {
        Self { counts, max_us }
    }
}

// ─────────────────────────────────────────────────────── one entry

/// The numbers of one key in one window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Entry {
    pub turns: u64,
    pub completed: u64,
    pub failed: u64,
    pub paused: u64,
    pub failed_attempts: u64,
    pub suspensions: u64,
    pub steps: u64,
    pub run_us: u64,
    /// CU of the turns: steps + ⌈run_ms / 100⌉ per turn.
    /// Does not include the 1 CU at invocation creation — creation is not a handler turn.
    pub cu: u64,
    pub durations: Durations,
    /// Ids of invocations that had a failed attempt, distinct, at most
    /// [`FAILED_IDS_CAP`]; `failed_ids_full` = some id was dropped because of the limit.
    failed_ids: Vec<InvocationId>,
    failed_ids_full: bool,
}

impl Entry {
    fn note_failed_id(&mut self, id: &InvocationId) {
        if self.failed_ids.contains(id) {
            return;
        }
        if self.failed_ids.len() < FAILED_IDS_CAP {
            self.failed_ids.push(*id);
        } else {
            self.failed_ids_full = true;
        }
    }

    pub fn merge(&mut self, o: &Entry) {
        self.turns += o.turns;
        self.completed += o.completed;
        self.failed += o.failed;
        self.paused += o.paused;
        self.failed_attempts += o.failed_attempts;
        self.suspensions += o.suspensions;
        self.steps += o.steps;
        self.run_us += o.run_us;
        self.cu += o.cu;
        self.durations.merge(&o.durations);
        for id in &o.failed_ids {
            self.note_failed_id(id);
        }
        self.failed_ids_full |= o.failed_ids_full;
    }
}

/// The outcome of one run, as the summary counts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    Completed,
    Failed,
    Paused,
    /// A retryable failed turn: the SDK's `Retry`, the SDK wait limit exceeded, or an
    /// infrastructure error mid-turn (not a loss of write authority).
    FailedAttempt,
    /// Suspended waiting for something external (a timer, a `ctx.call` result, a signal).
    Suspended,
}

impl RunOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Paused => "paused",
            Self::FailedAttempt => "retry",
            Self::Suspended => "suspended",
        }
    }
}

// ─────────────────────────────────────────────────────── shard

/// Window start mark → key → numbers. Nested two levels so that a lookup hit does not
/// copy the key.
type Accum = HashMap<u64, HashMap<HandlerKey, Entry>>;

fn window_of(ms: u64) -> u64 {
    ms - ms % WINDOW_MS
}

fn now_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The write part of **one** leader term of a range. Only that range's engine thread
/// writes; the collector takes it away every second.
pub struct Shard {
    node: HandlerStats,
    acc: Arc<Mutex<Accum>>,
}

impl Shard {
    fn bump(&self, key: &HandlerKey, f: impl FnOnce(&mut Entry)) {
        let w = window_of(now_ms());
        if let Ok(mut m) = self.acc.lock() {
            // Look up first, copy the key only if missing: a handler's second and later
            // turns within the same window allocate nothing.
            let win = m.entry(w).or_default();
            match win.get_mut(key) {
                Some(e) => f(e),
                None => f(win.entry(key.clone()).or_default()),
            }
        }
    }

    /// One SDK turn done: the duration and the durable step count of **that turn**.
    pub fn turn(&self, key: &HandlerKey, took: Duration, steps: u64) {
        self.bump(key, |e| {
            e.turns += 1;
            e.steps += steps;
            e.run_us += took.as_micros() as u64;
            e.cu += steps + crate::metering::run_cu(took);
            e.durations.record(took);
        });
    }

    /// The outcome of one run of invocation `id`.
    pub fn outcome(&self, key: &HandlerKey, id: &InvocationId, o: RunOutcome) {
        self.bump(key, |e| match o {
            RunOutcome::Completed => e.completed += 1,
            RunOutcome::Failed => {
                e.failed += 1;
                e.note_failed_id(id);
            }
            RunOutcome::Paused => e.paused += 1,
            RunOutcome::FailedAttempt => {
                e.failed_attempts += 1;
                e.note_failed_id(id);
            }
            RunOutcome::Suspended => e.suspensions += 1,
        });
    }

    /// Settings of the node this shard belongs to (slow log, capture).
    pub fn node(&self) -> &HandlerStats {
        &self.node
    }
}

// ─────────────────────────────────────────────────────── node

/// One row of `/stats/handlers`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HandlerRow {
    /// Node that measured this row — filled in at the HTTP layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub deployment: String,
    pub turns: u64,
    pub completed: u64,
    pub failed: u64,
    pub paused: u64,
    pub failed_attempts: u64,
    pub suspensions: u64,
    pub steps: u64,
    pub run_ms: u64,
    /// See [`Entry::cu`]. Older nodes do not send it: it reads as 0.
    #[serde(default)]
    pub cu: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// The quarantine action applied to this identity (`observe` shows up
    /// here) — filled in at the HTTP layer from the node's list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine: Option<String>,
}

/// Result of one summary read.
#[derive(Clone, Debug)]
pub struct StatsView {
    /// Start mark of the oldest window merged, ms.
    pub from_ms: u64,
    /// Time of the read, ms.
    pub to_ms: u64,
    pub windows: u64,
    pub rows: Vec<HandlerRow>,
}

/// The three numbers the quarantine rule reads, per handler, over a span.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandlerHealth {
    pub turns: u64,
    /// Failed attempts (retries, mid-turn errors) plus invocations ending `failed`.
    pub failed_attempts: u64,
    /// Number of *distinct* invocations that failed — capped at [`FAILED_IDS_CAP`].
    pub failed_invocations: usize,
    /// `true` = there may really be more than `failed_invocations` (the cap).
    pub failed_invocations_capped: bool,
    pub completed: u64,
}

#[derive(Default)]
struct Summary {
    /// Window start mark → key → numbers.
    windows: BTreeMap<u64, HashMap<HandlerKey, Entry>>,
    /// When each key was last written — the eviction order.
    last_seen: HashMap<HandlerKey, u64>,
    evicted: u64,
}

struct Inner {
    capacity: usize,
    shards: Mutex<Vec<Arc<Mutex<Accum>>>>,
    summary: Mutex<Summary>,
    slow: crate::slowlog::SlowLog,
    captures: crate::capture::Captures,
    quarantine: crate::quarantine::Quarantine,
}

/// Per-handler summary of **one node**. `Clone` is cheap.
#[derive(Clone)]
pub struct HandlerStats(Arc<Inner>);

impl std::fmt::Debug for HandlerStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerStats")
            .field("capacity", &self.0.capacity)
            .finish()
    }
}

impl Default for HandlerStats {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl HandlerStats {
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Inner {
            capacity: capacity.max(1),
            shards: Mutex::new(Vec::new()),
            summary: Mutex::new(Summary::default()),
            slow: crate::slowlog::SlowLog::default(),
            captures: crate::capture::Captures::default(),
            quarantine: crate::quarantine::Quarantine::default(),
        }))
    }

    /// A new shard for a leader term. Drop ⇒ the collector takes whatever is left and
    /// then discards it.
    pub fn shard(&self) -> Shard {
        let acc = Arc::new(Mutex::new(Accum::default()));
        if let Ok(mut s) = self.0.shards.lock() {
            s.push(Arc::clone(&acc));
        }
        Shard {
            node: self.clone(),
            acc,
        }
    }

    pub fn slow_log(&self) -> &crate::slowlog::SlowLog {
        &self.0.slow
    }

    pub fn captures(&self) -> &crate::capture::Captures {
        &self.0.captures
    }

    /// The node's poison-handler quarantine list — the second user
    /// of [`Self::health`].
    pub fn quarantine(&self) -> &crate::quarantine::Quarantine {
        &self.0.quarantine
    }

    /// One collection tick: bring every shard into the summary, rotate windows, enforce the limit.
    pub fn drain(&self) {
        self.drain_at(now_ms())
    }

    fn drain_at(&self, now: u64) {
        let mut taken = Vec::new();
        if let Ok(mut shards) = self.0.shards.lock() {
            for s in shards.iter() {
                if let Ok(mut m) = s.lock() {
                    if !m.is_empty() {
                        taken.push(std::mem::take(&mut *m));
                    }
                }
            }
            // A shard whose term is over: only we hold it — just took what was left.
            shards.retain(|s| Arc::strong_count(s) > 1);
        }
        let Ok(mut sum) = self.0.summary.lock() else {
            return;
        };
        for m in taken {
            for (w, keys) in m {
                for (key, e) in keys {
                    if !key.is_others() {
                        let seen = sum.last_seen.entry(key.clone()).or_default();
                        *seen = (*seen).max(now);
                    }
                    sum.windows
                        .entry(w)
                        .or_default()
                        .entry(key)
                        .or_default()
                        .merge(&e);
                }
            }
        }
        let oldest = window_of(now).saturating_sub((KEPT_WINDOWS - 1) * WINDOW_MS);
        sum.windows.retain(|w, _| *w >= oldest);
        let Summary {
            windows, last_seen, ..
        } = &mut *sum;
        last_seen.retain(|k, _| windows.values().any(|w| w.contains_key(k)));
        let over = sum.last_seen.len().saturating_sub(self.0.capacity);
        if over > 0 {
            let mut by_age: Vec<(u64, HandlerKey)> =
                sum.last_seen.iter().map(|(k, t)| (*t, k.clone())).collect();
            by_age.sort();
            for (_, key) in by_age.into_iter().take(over) {
                sum.last_seen.remove(&key);
                let others = HandlerKey::others(&key.tenant);
                for w in sum.windows.values_mut() {
                    if let Some(e) = w.remove(&key) {
                        w.entry(others.clone()).or_default().merge(&e);
                    }
                }
                sum.evicted += 1;
            }
        }
    }

    /// Every window still in RAM, oldest first — read by the durable statistics flusher
    /// ([`crate::stats_history`]).
    pub fn windows_snapshot(&self) -> Vec<(u64, Vec<(HandlerKey, Entry)>)> {
        self.0
            .summary
            .lock()
            .map(|s| {
                s.windows
                    .iter()
                    .map(|(w, m)| (*w, m.iter().map(|(k, e)| (k.clone(), e.clone())).collect()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Number of keys tracked (not counting `(others)`), and the number of evictions so far.
    pub fn tracked(&self) -> (usize, u64) {
        self.0
            .summary
            .lock()
            .map(|s| (s.last_seen.len(), s.evicted))
            .unwrap_or_default()
    }

    /// Merge the windows touching `[now - span, now]`. `tenant = None` = every
    /// tenant. Rows sorted by `run_ms` descending.
    pub fn view(&self, span: Duration, tenant: Option<&str>) -> StatsView {
        self.view_at(now_ms(), span, tenant)
    }

    fn merged(&self, now: u64, span: Duration) -> (u64, u64, HashMap<HandlerKey, Entry>) {
        let from = window_of(now.saturating_sub(span.as_millis() as u64));
        let mut out: HashMap<HandlerKey, Entry> = HashMap::default();
        let mut used = 0;
        if let Ok(sum) = self.0.summary.lock() {
            for (_, w) in sum.windows.range(from..) {
                used += 1;
                for (k, e) in w {
                    out.entry(k.clone()).or_default().merge(e);
                }
            }
        }
        (from, used, out)
    }

    fn view_at(&self, now: u64, span: Duration, tenant: Option<&str>) -> StatsView {
        let (from, windows, merged) = self.merged(now, span);
        let ms = |us: u64| us as f64 / 1000.0;
        let mut rows: Vec<HandlerRow> = merged
            .into_iter()
            .filter(|(k, _)| tenant.is_none_or(|t| k.tenant == t))
            .map(|(k, e)| HandlerRow {
                node: None,
                tenant: k.tenant,
                service: k.service,
                handler: k.handler,
                deployment: k.deployment,
                turns: e.turns,
                completed: e.completed,
                failed: e.failed,
                paused: e.paused,
                failed_attempts: e.failed_attempts,
                suspensions: e.suspensions,
                steps: e.steps,
                run_ms: e.run_us / 1000,
                cu: e.cu,
                p50_ms: ms(e.durations.quantile_us(0.5)),
                p99_ms: ms(e.durations.quantile_us(0.99)),
                max_ms: ms(e.durations.max_us()),
                quarantine: None,
            })
            .collect();
        rows.sort_by(|a, b| {
            b.run_ms.cmp(&a.run_ms).then_with(|| {
                (&a.tenant, &a.service, &a.handler, &a.deployment).cmp(&(
                    &b.tenant,
                    &b.service,
                    &b.handler,
                    &b.deployment,
                ))
            })
        });
        StatsView {
            from_ms: from,
            to_ms: now,
            windows,
            rows,
        }
    }

    /// Per-handler health over the windows touching `[now - span, now]` — the source of
    /// numbers for poison-handler quarantine. No `(others)` row: it is not a handler
    /// that could be quarantined.
    ///
    /// The numbers read here lag the turn that just ran by at most one collection tick (1 s).
    pub fn health(&self, span: Duration) -> Vec<(HandlerKey, HandlerHealth)> {
        self.health_at(now_ms(), span)
    }

    fn health_at(&self, now: u64, span: Duration) -> Vec<(HandlerKey, HandlerHealth)> {
        let (_, _, merged) = self.merged(now, span);
        let mut out: Vec<_> = merged
            .into_iter()
            .filter(|(k, _)| !k.is_others())
            .map(|(k, e)| {
                (
                    k,
                    HandlerHealth {
                        turns: e.turns,
                        failed_attempts: e.failed_attempts + e.failed,
                        failed_invocations: e.failed_ids.len(),
                        failed_invocations_capped: e.failed_ids_full,
                        completed: e.completed,
                    },
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The background collection loop, every `every`. The task stops when the handle is cancelled.
    pub fn spawn_aggregator(&self, every: Duration) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(cairn_store::io_source::tagged("stats", async move {
            loop {
                cairn_store::pace::beat(every).await;
                me.drain();
            }
        }))
    }
}

// ─────────────────────────────────────────────── trace of one run

/// Trace of **one run** (`turn::drive`): from asking for a slot until releasing it.
/// It lives in a task-local, so the scattered measuring points (`Lease::released`,
/// `Lease::sync`, slot wait, SDK wait) can write into it without threading another
/// parameter through ten signatures. Outside a run (background tasks, flushers), every
/// `note_*` is a no-op.
#[derive(Clone, Debug, Default)]
pub struct TurnTrace {
    started: Option<Instant>,
    /// The handler's key — known from the first SDK turn. `None` = the run never reached
    /// the SDK (already done, paused, slot wait limit exceeded).
    pub key: Option<HandlerKey>,
    pub turns: u32,
    pub steps: u64,
    turn_steps: u64,
    /// CU of the run's turns not yet settled into the bucket — see
    /// [`take_run_cu`].
    cu: u64,
    pub durable_waits: u32,
    pub durable_wait: Duration,
    pub engine_wait: Duration,
    pub permit_wait: Duration,
    pub sdk: Duration,
    pub run: Duration,
}

impl TurnTrace {
    /// Since the run started.
    pub fn elapsed(&self) -> Duration {
        self.started.map(|t| t.elapsed()).unwrap_or_default()
    }
}

tokio::task_local! {
    static TRACE: RefCell<TurnTrace>;
}

/// Run `fut` in a new trace; returns the result with the trace.
///
/// A plain function returning a future, not an `async fn`: an `async fn` keeps `fut` in
/// its not-yet-run state **and** inside the inner `async move` block — two copies, each the
/// size of the whole run (a task waiting for a slot must be small).
pub fn traced<T>(fut: impl Future<Output = T>) -> impl Future<Output = (T, TurnTrace)> {
    let start = TurnTrace {
        started: Some(Instant::now()),
        ..Default::default()
    };
    TRACE.scope(RefCell::new(start), async move {
        let out = fut.await;
        let t = TRACE
            .try_with(|t| t.try_borrow_mut().map(|mut t| std::mem::take(&mut *t)).ok())
            .ok()
            .flatten()
            .unwrap_or_default();
        (out, t)
    })
}

fn with(f: impl FnOnce(&mut TurnTrace)) {
    let _ = TRACE.try_with(|t| {
        if let Ok(mut t) = t.try_borrow_mut() {
            f(&mut t);
        }
    });
}

/// An SDK command just became a durable step.
pub fn note_step() {
    with(|t| {
        t.steps += 1;
        t.turn_steps += 1;
    });
}

/// A wait for the WAL to flush up to a mark (`Lease::sync`, the waiting branch).
pub fn note_durable_wait(d: Duration) {
    with(|t| {
        t.durable_waits += 1;
        t.durable_wait += d;
    });
}

/// Waiting to take the engine back after a release point.
pub fn note_engine_wait(d: Duration) {
    with(|t| t.engine_wait += d);
}

/// Waiting for a scheduler slot.
pub fn note_permit_wait(d: Duration) {
    with(|t| t.permit_wait += d);
}

/// Waiting for the SDK (send, frame read, a whole `REQUEST_RESPONSE` turn).
/// Time the current run has spent in the SDK so far (`0` outside a run) — the handler's
/// own share of a turn, excluding engine and durability waits.
pub fn sdk_so_far() -> Duration {
    TRACE
        .try_with(|t| t.try_borrow().map(|t| t.sdk).unwrap_or_default())
        .unwrap_or_default()
}

pub fn note_sdk(d: Duration) {
    with(|t| t.sdk += d);
}

/// Take away the CU of the current run's turns that are not yet settled.
/// Outside a run: 0.
pub fn take_run_cu() -> u64 {
    let mut cu = 0;
    with(|t| cu = std::mem::take(&mut t.cu));
    cu
}

/// An SDK turn starts, for handler `key`.
pub fn turn_started(key: &HandlerKey) {
    with(|t| {
        if t.key.as_ref() != Some(key) {
            t.key = Some(key.clone());
        }
        t.turn_steps = 0;
    });
}

/// An SDK turn done after `took`: record it in `shard` with the turn's step count.
pub fn turn_finished(shard: &Shard, key: &HandlerKey, took: Duration) {
    let mut steps = 0;
    with(|t| {
        steps = std::mem::take(&mut t.turn_steps);
        t.turns += 1;
        t.run += took;
        t.cu += steps + crate::metering::run_cu(took);
    });
    shard.turn(key, took, steps);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(t: &str, h: &str) -> HandlerKey {
        HandlerKey {
            tenant: t.into(),
            service: "S".into(),
            handler: h.into(),
            deployment: "dp_1".into(),
        }
    }

    #[test]
    fn histogram_buckets_cover_every_value() {
        for us in [
            0u64,
            1,
            3,
            4,
            5,
            7,
            8,
            9,
            15,
            16,
            1_000,
            123_456,
            9_999_999,
            u64::MAX / 2,
        ] {
            let i = Durations::bucket(us);
            let lo = if i == 0 { 0 } else { Durations::upper(i - 1) };
            assert!(
                lo <= us && us < Durations::upper(i),
                "{us} in bucket {i} [{lo}, {})",
                Durations::upper(i)
            );
        }
    }

    #[test]
    fn quantile_off_by_at_most_one_element_and_never_above_max() {
        let mut d = Durations::default();
        for ms in 1..=100u64 {
            d.record(Duration::from_millis(ms));
        }
        let p50 = d.quantile_us(0.5) as f64;
        let p99 = d.quantile_us(0.99) as f64;
        assert!((50_000.0..=62_500.0).contains(&p50), "p50 {p50}");
        assert!((99_000.0..=100_000.0).contains(&p99), "p99 {p99}");
        assert_eq!(d.max_us(), 100_000);
        assert_eq!(Durations::default().quantile_us(0.5), 0);
    }

    /// The CU of a turn = durable steps + ⌈duration / 100 ms⌉, added to the
    /// per-handler summary and to the run's trace (taken away exactly once).
    #[tokio::test]
    async fn turn_cu_goes_into_summary_and_trace() {
        let s = HandlerStats::new(10);
        let sh = s.shard();
        let k = key("t1", "cu");
        let (taken, _) = traced(async {
            turn_started(&k);
            note_step();
            note_step();
            turn_finished(&sh, &k, Duration::from_millis(250));
            turn_started(&k);
            turn_finished(&sh, &k, Duration::from_millis(1));
            (take_run_cu(), take_run_cu())
        })
        .await;
        assert_eq!(taken, (2 + 3 + 1, 0), "taken away exactly once");
        s.drain();
        let v = s.view(Duration::from_secs(300), Some("t1"));
        assert_eq!(v.rows[0].cu, 6);
    }

    #[test]
    fn aggregates_cumulatively_and_splits_by_tenant() {
        let s = HandlerStats::new(10);
        let a = s.shard();
        let b = s.shard();
        let id = [1u8; 24];
        a.turn(&key("t1", "h"), Duration::from_millis(3), 2);
        a.outcome(&key("t1", "h"), &id, RunOutcome::Completed);
        b.turn(&key("t1", "h"), Duration::from_millis(5), 1);
        b.outcome(&key("t2", "h"), &id, RunOutcome::FailedAttempt);
        s.drain();
        let v = s.view(Duration::from_secs(300), None);
        let row = v.rows.iter().find(|r| r.tenant == "t1").unwrap();
        assert_eq!((row.turns, row.steps, row.completed), (2, 3, 1));
        assert!(row.max_ms >= 5.0 && row.max_ms < 5.1, "{row:?}");
        let only = s.view(Duration::from_secs(300), Some("t2"));
        assert_eq!(only.rows.len(), 1);
        assert_eq!(only.rows[0].failed_attempts, 1);
    }

    #[test]
    fn shard_of_ended_term_is_drained_then_dropped() {
        let s = HandlerStats::new(10);
        {
            let sh = s.shard();
            sh.turn(&key("t", "h"), Duration::from_millis(1), 0);
        }
        s.drain();
        assert_eq!(s.0.shards.lock().unwrap().len(), 0);
        assert_eq!(s.view(Duration::from_secs(60), None).rows[0].turns, 1);
    }

    #[test]
    fn over_cap_oldest_key_goes_to_its_tenants_others() {
        let s = HandlerStats::new(2);
        let sh = s.shard();
        let t0 = 1_000 * WINDOW_MS;
        for (i, h) in ["a", "b", "c"].iter().enumerate() {
            sh.turn(
                &key(if i == 0 { "x" } else { "y" }, h),
                Duration::from_millis(1),
                1,
            );
            // Each key last seen at a different collection tick.
            s.drain_at(t0 + i as u64);
        }
        let (tracked, evicted) = s.tracked();
        assert_eq!((tracked, evicted), (2, 1));
        // Write into the real "now" — read it back at the wall-clock mark.
        let v = s.view(Duration::from_secs(3_600), None);
        let others = v
            .rows
            .iter()
            .find(|r| r.service == OTHERS)
            .expect("an (others) row");
        assert_eq!((others.tenant.as_str(), others.turns), ("x", 1));
        assert!(!v.rows.iter().any(|r| r.handler == "a"), "{:?}", v.rows);
        // Totals unchanged across evictions.
        assert_eq!(v.rows.iter().map(|r| r.turns).sum::<u64>(), 3);
    }

    #[test]
    fn windows_older_than_an_hour_are_dropped() {
        let s = HandlerStats::new(10);
        let now = 2_000 * WINDOW_MS + 7;
        {
            let mut sum = s.0.summary.lock().unwrap();
            for back in [0u64, 11, 12, 30] {
                let w = window_of(now) - back * WINDOW_MS;
                sum.windows.entry(w).or_default().insert(
                    key("t", "h"),
                    Entry {
                        turns: 1,
                        ..Default::default()
                    },
                );
            }
        }
        s.drain_at(now);
        let sum = s.0.summary.lock().unwrap();
        assert_eq!(
            sum.windows.len(),
            2,
            "keeps the current window and the 11 before it"
        );
    }

    #[test]
    fn health_counts_failed_invocations_once_and_capped() {
        let s = HandlerStats::new(10);
        let sh = s.shard();
        let k = key("t", "h");
        for i in 0..40u8 {
            sh.outcome(&k, &[i; 24], RunOutcome::FailedAttempt);
            sh.outcome(&k, &[i; 24], RunOutcome::FailedAttempt);
        }
        sh.outcome(&key("t", "ok"), &[0; 24], RunOutcome::Completed);
        s.drain();
        let h = s.health(Duration::from_secs(300));
        let bad = h.iter().find(|(k, _)| k.handler == "h").unwrap().1;
        assert_eq!(bad.failed_attempts, 80);
        assert_eq!(bad.failed_invocations, FAILED_IDS_CAP);
        assert!(bad.failed_invocations_capped);
        let ok = h.iter().find(|(k, _)| k.handler == "ok").unwrap().1;
        assert_eq!((ok.completed, ok.failed_invocations), (1, 0));
    }

    #[tokio::test]
    async fn trace_lives_only_within_the_run() {
        note_step(); // outside a scope: no-op
        let s = HandlerStats::new(10);
        let sh = s.shard();
        let k = key("t", "h");
        let ((), t) = traced(async {
            note_permit_wait(Duration::from_millis(2));
            turn_started(&k);
            note_step();
            note_step();
            turn_finished(&sh, &k, Duration::from_millis(4));
            note_durable_wait(Duration::from_millis(1));
        })
        .await;
        assert_eq!((t.turns, t.steps, t.durable_waits), (1, 2, 1));
        assert_eq!(t.key, Some(k));
        assert_eq!(t.permit_wait, Duration::from_millis(2));
        s.drain();
        assert_eq!(s.view(Duration::from_secs(60), None).rows[0].steps, 2);
    }
}
