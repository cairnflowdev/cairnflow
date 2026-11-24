// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A partition's engine: who may hold it, when it must be released, and what
//! may have changed once it is released.
//!
//! [`Engine`] is still `!Send` and lives on exactly one thread (D10, INV-4).
//! That thread runs many *tasks*, and the engine is handed between them through
//! a lock. This module is the whole rulebook of that handover — the rules live
//! in the **types**, not only in comments.
//!
//! # Four rules
//!
//! 1. **No network calls while holding the engine.** Every function touching
//!    the network — [`crate::transport::Transport::send`], `invoker::call`,
//!    `open_bidi`, `BidiSession::send`/`next_frame` — requires a [`Released`],
//!    which only [`Lease::released`] can create and only lends out inside the
//!    I/O closure. A violation is a compile error. Why: the receiver of an
//!    inter-partition message must take *its own* engine; holding ours across
//!    the send means two partitions sending to each other at once wait for
//!    each other forever (`tests/transport_engine.rs`). The only exception as
//!    to *where* a `Released` is created: [`IoSet`] hands it to a separate task
//!    that holds no `Lease`, and the issuer only waits for the result through
//!    [`Lease::next_io`].
//! 2. **No outside effect before a durability ticket.** Answering a client
//!    and letting the SDK continue require [`Committed`]: the ticket is only
//!    issued once `durable_seq` has passed the seq of the latest write **at the
//!    time of asking** ([`Lease::sync`]). Sending an inter-partition message
//!    requires a **send** ticket [`SendTicket`] (LF-1/LF-2): `durable_seq` has
//!    passed the seq of the latest write **with send rows** at the time of
//!    asking ([`Lease::sync_sends`]) — the message only carries the bytes of
//!    send rows, so that is what must be durable before the other side sees it
//!    (IR-1). A `Committed` converts into a `SendTicket`, not the other way
//!    round.
//! 3. **Never hold two invocation locks at once**, and the order is
//!    `invocation → engine`: release the engine before waiting for an
//!    invocation lock ([`Lease::wait`]). [`Locks::invocation`] returns an
//!    *owned* guard, so a place needing a second lock must consume the first
//!    one first.
//! 4. **Background paths run no SDK turns and never stand waiting for someone
//!    else's turn.** The drainers (outbox, signals, inbound), background ticks
//!    (timers, retries) and recovery only *write* the wake-up into the journal
//!    and hand the invocation to the scheduler (`drainer::Runner`); invocation
//!    locks are only *tried* ([`Locks::try_invocation`]) — busy ⇒ skip the
//!    row, and the guard holding it kicks the drainer on release.
//!
//! # Engine release points — the complete list
//!
//! | Where | Waits for |
//! |---|---|
//! | [`Lease::released`] | network I/O: SDK, inter-partition messages |
//! | [`Lease::next_io`] | one job of an [`IoSet`] (inter-partition messages per destination) to finish |
//! | [`Lease::sync`], [`Lease::sync_sends`] (`Wait` branch) | the WAL to be flushed up to the mark |
//! | [`Locks::invocation`] | an invocation's lock |
//! | `Drainer::wait_for`, `Drainer::wait_full_pass` | the drainer to reach a mark / finish a pass |
//! | `turn::acquire_turn` | a scheduler slot |
//!
//! **Durable state may have changed when any of the points above returns.**
//! Whoever carries a decision across one must check again: a turn checks
//! durable state right before running (`turn::gate_turn`, both before and
//! after waiting for a slot); a queue scan result carried across one is only
//! safe because each queue has exactly one reader (`crate::drainer`).
//!
//! # What the engine lock protects, beyond ordinary contention
//!
//! [`Engine`] carries `pending_*_removals`: the source of a completion (a
//! timer, outbox, inbound or signal row) is *staged* there and then swept into
//! the next commit — this is how "delete the source in the same commit" is
//! enforced. Between staging and sweeping there may be an `.await` (a store
//! read), but no **release point**: `Engine::deliver` and `Engine::step` take
//! `&mut Engine`, and such a function holds no [`Lease`] to release. Adding a
//! release point between those two steps would require changing the signature
//! — so the compiler catches it.

use cairn_store::entropy::{HashMap, HashSet};
use {
    cairn_engine::{Engine, InvocationId},
    cairn_store::{Committed, SendTicket},
    std::{
        cell::RefCell, future::Future, marker::PhantomData, ops::AsyncFnOnce, rc::Rc, sync::Arc,
    },
    tokio::sync::{Mutex, OwnedMutexGuard},
};

/// The engine shared between the tasks of **one thread**, plus the rule for who
/// takes it first at the **start of a term** ([`Precedence`]).
///
/// `Arc` rather than `Rc` for the mutex only because `lock_owned()` requires
/// it. There is no intention to send it across threads, and no need to guard
/// against it: `Engine` is still `!Send`, so if anyone tried the compiler would
/// stop it — INV-4 is still kept by the type system, not by convention. The
/// precedence part is `Rc` (`Cell`): it too lives only on that thread.
#[derive(Clone)]
pub struct EngineLock {
    mutex: Arc<Mutex<Engine>>,
    precedence: Rc<Precedence>,
}

// clippy suggests `Rc` because this `Arc` never crosses a thread — right about
// the fact, wrong about the conclusion: `OwnedMutexGuard` (what lets a `Lease`
// live independently of any stack frame) can only be built from an `Arc`.
// Switching to `Rc` loses `Lease`, and `Lease` is this whole module.
#[allow(clippy::arc_with_non_send_sync)]
pub fn engine_lock(engine: Engine) -> EngineLock {
    EngineLock {
        mutex: Arc::new(Mutex::new(engine)),
        precedence: Rc::new(Precedence {
            open: std::cell::Cell::new(true),
            ..Precedence::default()
        }),
    }
}

impl EngineLock {
    /// Takes the engine out of the lock — only when no other copy remains
    /// (stepping down: `seal` takes the engine by value). Someone still holds
    /// one ⇒ the lock is returned.
    pub fn into_inner(self) -> std::result::Result<Engine, Self> {
        let Self { mutex, precedence } = self;
        match Arc::try_unwrap(mutex) {
            Ok(m) => Ok(m.into_inner()),
            Err(mutex) => Err(Self { mutex, precedence }),
        }
    }

    /// The start of the term is over: from now on background work queues FIFO with every other task.
    pub fn end_precedence(&self) {
        self.precedence.close();
    }

    /// A start-of-term job (`restore_flow`, the first `recover`) begins: the
    /// precedence window closes once every such guard has been dropped (done,
    /// failed or cancelled).
    pub fn opening_job(&self) -> OpeningJob {
        let p = &self.precedence;
        p.jobs.set(p.jobs.get() + 1);
        OpeningJob(Rc::clone(p))
    }

    /// Closes the window if no start-of-term job remains (a term with no such
    /// job: an empty deployment registry, and the build ran before the serving
    /// loop).
    pub fn end_precedence_if_idle(&self) {
        if self.precedence.jobs.get() == 0 {
            self.end_precedence();
        }
    }

    /// Commands waiting for the engine that background work must yield to
    /// (window still open) — so background work can release early at its own
    /// release points.
    pub fn command_waiting(&self) -> bool {
        let p = &self.precedence;
        p.open.get() && p.waiting.get() > 0
    }

    /// Takes the mutex according to the current task's class ([`Precedence`]).
    async fn lock(&self) -> OwnedMutexGuard<Engine> {
        match class() {
            Class::Command => {
                let _waiting = Waiting::new(&self.precedence);
                let guard = self.mutex.clone().lock_owned().await;
                let p = &self.precedence;
                p.served.set(p.served.get().wrapping_add(1));
                return guard;
            }
            Class::Runner => return self.mutex.clone().lock_owned().await,
            Class::Background => {}
        }
        let p = &self.precedence;
        let start = p.served.get();
        loop {
            // `Notified` is built **before** the check: `notify_waiters` wakes
            // every future already built, even unpolled — no change is missed.
            loop {
                let changed = p.changed.notified();
                if !p.yields(start) {
                    break;
                }
                changed.await;
            }
            let guard = self.mutex.clone().lock_owned().await;
            if !p.yields(start) {
                return guard;
            }
            // A command arrived after we had queued on the mutex and stands
            // behind us: give up the turn (the mutex goes to the next in line —
            // other background work gives up too, then the command), and wait
            // outside until the command has taken it.
            drop(guard);
        }
    }
}

/// **Commands go before background work at the start of a term.**
///
/// On becoming leader, the term's tasks — the `restore` chunks, the first
/// drain pass, the first background tick, `recover`, the index background
/// build — all ask for the engine as soon as the serving loop runs; tokio's
/// mutex is FIFO, so the first command (arriving a few ms later) queues behind
/// them: measured as ~3 background holds, 59–80 ms of "waiting for the engine"
/// at H = 10⁶ cold. Background work has nobody waiting on it; commands do.
///
/// The rule, only within the **start-of-term window** (opened when the lock is
/// built, closed once the VO-lock build and the first `recover` are done —
/// [`EngineLock::opening_job`]):
///
/// - a **background** task ([`BACKGROUND`], per the [`holding_as`] label)
///   asking for the engine (first take, or retaking after a release point)
///   while a command is waiting stays out until no command is waiting; one
///   already queued on the mutex when a command arrives behind it gives up its
///   turn to the command when its turn comes;
/// - **bounded**: each request of background work yields to at most
///   [`YIELD_MAX`] command takes, then queues as usual — a burst of commands
///   does not starve the lock build (the slot-granting gate, FL-8) or the
///   drainer;
/// - turns handed out by the scheduler (`Runner`: `run_dispatched`,
///   `run_granted` — [`runner_turn`]) take it FIFO as before: they neither
///   yield nor are yielded to. At the start of a term `recover` hands out all
///   R turns; counting them as commands would make each `restore` chunk yield
///   up to `YIELD_MAX` turns just to park their tickets (the slot-granting
///   gate is still closed, FL-8) — measured as ~10 % slower handover at
///   H = 10⁶;
/// - every other task (client commands, including the first turn of
///   `/invoke`) is a **command**.
///
/// This only changes **who takes the engine next** among the tasks already
/// waiting — it adds no release point and changes nothing a hold does. Every
/// such order was already allowed (FIFO is only tokio's choice): M4 and
/// `range_model.rs` pick any enabled action at each step. Outside the window:
/// FIFO as before.
#[derive(Default)]
struct Precedence {
    /// The start-of-term window is still open.
    open: std::cell::Cell<bool>,
    /// Start-of-term jobs not finished yet ([`OpeningJob`]).
    jobs: std::cell::Cell<usize>,
    /// Non-background tasks waiting for the mutex.
    waiting: std::cell::Cell<usize>,
    /// Number of times non-background tasks have taken the mutex (for the yield cap).
    served: std::cell::Cell<u64>,
    /// `waiting`/`served`/`open` just changed — wakes background work that is yielding.
    changed: tokio::sync::Notify,
}

impl Precedence {
    fn close(&self) {
        self.open.set(false);
        self.changed.notify_waiters();
    }

    /// Whether background work asking since `served == start` must yield.
    fn yields(&self, start: u64) -> bool {
        self.open.get()
            && self.waiting.get() > 0
            && self.served.get().wrapping_sub(start) < YIELD_MAX
    }
}

/// The maximum number of command takes one request of background work yields
/// to ([`Precedence`]). A command's hold at the start of a term is a few ms (a
/// cold read), so the cap adds about a hundred ms for background work under a
/// burst of commands.
pub const YIELD_MAX: u64 = 16;

/// The holder label ([`holding_as`]) of **background work** — term tasks
/// nobody is waiting on for an answer ([`Precedence`]).
pub const BACKGROUND: [&str; 7] = [
    "tick",
    "drain",
    "query_build",
    "hint_sweep",
    "recover",
    "rescue",
    "restore",
];

/// The current task's class in front of the engine lock ([`Precedence`]).
enum Class {
    /// Someone is waiting for an answer: background work yields to it at the start of a term.
    Command,
    /// A turn handed out by the scheduler ([`runner_turn`]): FIFO.
    Runner,
    /// The term's background work ([`BACKGROUND`]).
    Background,
}

fn class() -> Class {
    if HOLDER
        .try_with(|h| BACKGROUND.contains(&cairn_store::dbstats::HOLDERS[*h]))
        .unwrap_or(false)
    {
        Class::Background
    } else if RUNNER.try_with(|_| ()).is_ok() {
        Class::Runner
    } else {
        Class::Command
    }
}

tokio::task_local! {
    /// Whether the current task is a turn handed out by the scheduler ([`runner_turn`]).
    static RUNNER: ();
}

/// Marks `fut` as a turn handed out by the scheduler (`actor::run_dispatched`,
/// `run_granted`): it takes the engine FIFO, outside the start-of-term
/// precedence rule ([`Precedence`]).
pub fn runner_turn<F: Future>(fut: F) -> tokio::task::futures::TaskLocalFuture<(), F> {
    RUNNER.scope((), fut)
}

/// A command waiting for the mutex: counted when built, uncounted when dropped (taken or cancelled).
struct Waiting<'a>(&'a Precedence);

impl<'a> Waiting<'a> {
    fn new(p: &'a Precedence) -> Self {
        p.waiting.set(p.waiting.get() + 1);
        Self(p)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let p = self.0;
        p.waiting.set(p.waiting.get() - 1);
        p.changed.notify_waiters();
    }
}

/// Guard of a start-of-term job ([`EngineLock::opening_job`]). Only holds the
/// precedence part, not the engine: stepping down takes the engine out of the
/// lock once every task has stopped.
pub struct OpeningJob(Rc<Precedence>);

impl Drop for OpeningJob {
    fn drop(&mut self) {
        let p = &self.0;
        p.jobs.set(p.jobs.get() - 1);
        if p.jobs.get() == 0 {
            p.close();
        }
    }
}

/// Proof that the task is **not** holding the engine.
///
/// Only [`Lease::released`] can create one (private field, no constructor),
/// and it is only lent out while the I/O closure runs. Every function touching
/// the network requires a `&Released`, so the compiler refuses every network
/// call made while the engine is still held.
///
/// ```compile_fail
/// // Cannot be built from outside the module.
/// let _ = cairn_server::lease::Released { _not_send: std::marker::PhantomData };
/// ```
pub struct Released {
    _not_send: PhantomData<*const ()>,
}

#[cfg(test)]
impl Released {
    /// A ticket for unit tests of network functions (`invoker`) — there is no
    /// engine to release. Only under `cfg(test)`: real builds still only create
    /// tickets in `Lease::released`.
    pub(crate) fn for_test() -> Self {
        Self {
            _not_send: PhantomData,
        }
    }
}

/// A task's right to use the engine, which can be released temporarily.
///
/// `Deref`/`DerefMut` down to [`Engine`] is deliberate: every place in `turn/`
/// writing `engine.status(..)` stays word for word, and functions that only
/// touch the engine still declare `&mut Engine` — deref coercion takes care
/// of the join. That way the diff of this change sits at the *boundary*, not
/// spread across the logic.
pub struct Lease {
    lock: EngineLock,
    /// `None` exactly while waiting for the SDK's I/O. `Some` at every other time.
    guard: Option<OwnedMutexGuard<Engine>>,
    /// When the engine was taken (or retaken) — for the hold-time measurement
    /// per holder ([`holding_as`], `cairnflow_range_engine_hold_ms_total`).
    since: tokio::time::Instant,
}

tokio::task_local! {
    /// The engine holder of the current task — an index into
    /// [`cairn_store::dbstats::HOLDERS`]. Absent ⇒ `other`.
    static HOLDER: usize;
}

/// Runs `fut` with holder label `holder` (a name in
/// [`cairn_store::dbstats::HOLDERS`]): every [`Lease`] this task releases is
/// counted under that label (engine hold time per kind of background work).
/// With nested labels the inner one wins; child tasks do not inherit it.
///
/// A plain function, not `async`, and `fut` is boxed at once: a turn's future
/// is very large in debug builds, and every `async` layer wraps one more copy
/// onto the engine thread's stack (a stack overflow in
/// `tests/concurrency.rs`).
pub fn holding_as<F: Future>(
    holder: &'static str,
    fut: F,
) -> tokio::task::futures::TaskLocalFuture<usize, std::pin::Pin<Box<F>>> {
    HOLDER.scope(cairn_store::dbstats::holder(holder), Box::pin(fut))
}

impl Lease {
    pub async fn acquire(lock: &EngineLock) -> Self {
        let asked = tokio::time::Instant::now();
        let guard = lock.lock().await;
        cairn_store::probe::mark("engine.acq_us", asked.elapsed().as_micros() as u64);
        Self {
            lock: lock.clone(),
            guard: Some(guard),
            since: tokio::time::Instant::now(),
        }
    }

    /// Time the engine has been held since the latest take (or retake) — so
    /// background work can release itself once it exceeds its per-hold budget
    /// (the drainer).
    pub fn held_for(&self) -> std::time::Duration {
        self.since.elapsed()
    }

    /// A command is waiting for the engine that this background work holds, so
    /// it should release early at its next release point — only within the
    /// start-of-term window ([`Precedence`]).
    pub fn command_waiting(&self) -> bool {
        self.lock.command_waiting()
    }

    /// Records the engine hold time since the latest take, before releasing.
    fn note_hold(&self) {
        if let Some(g) = &self.guard {
            let holder = HOLDER
                .try_with(|h| *h)
                .unwrap_or(cairn_store::dbstats::HOLDERS.len() - 1);
            g.writer().note_engine_hold(holder, self.since.elapsed());
        }
    }

    /// Runs an I/O job **while not holding the engine**, then takes it back.
    ///
    /// `io` receives a `&Released` — proof that the engine is released — and
    /// every function touching the network ([`crate::transport::Transport::send`],
    /// `invoker::call`, `open_bidi`, `BidiSession::send`/`next_frame`) requires
    /// exactly that proof. `Released` can only be created here, and the
    /// higher-ranked signature of `io` does not let it escape the closure. The
    /// consequence: **a network call while holding the engine is a compile
    /// error**, no longer a rule in a comment.
    ///
    /// That rule is not aesthetics. The receiver of an inter-partition message
    /// must take its own engine to write the message before acking; a sender
    /// holding its engine across the send means two partitions sending to each
    /// other at once wait for each other forever (`tests/transport_engine.rs`).
    ///
    /// **Durable state may have changed when this function returns.** A caller
    /// carrying an `Invocation` across it must be covered by the invocation
    /// lock (see [`Locks::invocation`]); a queue scan result carried across it
    /// is only safe because each queue has exactly one reader
    /// ([`crate::drainer`]).
    pub async fn released<T>(&mut self, io: impl AsyncFnOnce(&Released) -> T) -> T {
        self.note_hold();
        self.guard = None;
        // The token dies before the engine is taken back.
        let out = {
            let token = Released {
                _not_send: PhantomData,
            };
            // The engine is released, the I/O has not started.
            cairn_store::failpoint!("lease::released::io");
            io(&token).await
        };
        let reacquire = tokio::time::Instant::now();
        self.guard = Some(self.lock.lock().await);
        self.since = tokio::time::Instant::now();
        cairn_store::probe::mark("engine.reacq_us", reacquire.elapsed().as_micros() as u64);
        crate::stats::note_engine_wait(reacquire.elapsed());
        out
    }

    /// Waits for something **other than network I/O** — a lock, a flush mark, a
    /// slot, durability — while not holding the engine.
    ///
    /// The same mechanism as [`Self::released`], just without handing a
    /// `Released` to anyone: what is being awaited does not need it, and not
    /// handing it out keeps the list of network call sites exactly the list of
    /// `released` call sites.
    pub async fn wait<F: Future>(&mut self, fut: F) -> F::Output {
        self.released(async move |_: &Released| fut.await).await
    }

    /// Waits for one job of `io` to finish — **releasing the engine while
    /// waiting**. `None` when the set is empty. See [`IoSet`].
    pub async fn next_io<T: 'static>(&mut self, io: &mut IoSet<T>) -> Option<T> {
        while !io.set.is_empty() {
            let done = self.wait(io.set.join_next()).await;
            if let Some(out) = io.finish(done) {
                return Some(out);
            }
        }
        None
    }

    /// A durability ticket for **everything this task may have seen**, without
    /// holding the engine while waiting.
    ///
    /// This is one half of commit batching; the other half is
    /// [`cairn_store::PartitionWriter::commit`] no longer waiting for
    /// durability itself. The two halves only make sense together: `commit`
    /// pushes the write into SlateDB's `current_wal` and returns at once, and
    /// because the mutex is released **here**, other turns get to push their
    /// writes into the same buffer before it is flushed. One flush, n
    /// invocations.
    ///
    /// **A mark, not an item.** The previous version *took away* the writer's
    /// pending handle; a task arriving later found the slot empty and got a
    /// ticket at once, while the data it had just read (the earlier task's)
    /// was still not durable — `tests/durability.rs` measured `status`
    /// answering after 1ms on non-durable data. Now the mark is captured **at
    /// the time of asking** (the seq of the latest write, i.e. everything a
    /// read may have seen), and only then is the engine released to wait. Many
    /// waiters on the same mark wake on the same flush; waiting for more than
    /// needed is harmless, and since the WAL is sequential, covering is free.
    ///
    /// **A suspended grant** (an overdue session demotes itself while its
    /// renewal may still win the CAS): wait, do not return an error, and
    /// release the engine while waiting. Revived ⇒ a ticket as usual, under
    /// the new deadline; lost ⇒ `LeaseLost` as before. No ack, no read answer
    /// in that interval.
    pub async fn sync(&mut self) -> cairn_store::Result<Committed> {
        self.sync_with(false).await
    }

    /// Like [`Self::sync`], for an **answer** at the leader: plus a **read
    /// fence** — a conditional GET on the incarnation's session record, batched
    /// for the whole node, issued **after** every read of the answer. `alive()`
    /// only trusts the node's clock; with a frozen clock (a sleeping machine, a
    /// paused VM) a node waking up believes it still leads while the range has
    /// moved to someone else, and answers from a stale copy. A fence that finds
    /// the session no longer `live` ⇒ `LeaseLost` (the client gets `NotLeader`
    /// and asks the new leader).
    ///
    /// Applies to tickets that must wait for durability too: the PUT making the
    /// write durable may have reached the store **before** the node froze, so
    /// finishing the durability wait proves nothing about the read. The fence
    /// runs **in parallel** with the durability wait — a writing answer pays
    /// `max(PUT, GET)`, not the sum.
    pub async fn sync_reply(&mut self) -> cairn_store::Result<Committed> {
        self.sync_with(true).await
    }

    /// Like [`Self::sync_reply`], for the task's **final** answer: consumes
    /// the `Lease`, releases the engine before the wait and does not take it
    /// back — the task needs no engine after the ticket. Taking it back only to
    /// release it at once means queueing behind every hold that slipped in
    /// during the wait (measured: 34 ms after the first drain pass at H = 10⁶
    /// cold).
    pub async fn sync_reply_owned(self) -> cairn_store::Result<Committed> {
        self.sync_owned_with(true).await
    }

    /// Like [`Self::sync`], for a final answer without a fence ([`Self::sync_reply_owned`]).
    pub async fn sync_owned(self) -> cairn_store::Result<Committed> {
        self.sync_owned_with(false).await
    }

    /// A **send** ticket for every send row (outbox, signal outbox) this task
    /// may have seen, without holding the engine while waiting (LF-1).
    ///
    /// Like [`Self::sync`], but capturing the mark of the latest write **with
    /// send rows** (`PartitionWriter::send_durability`): the drainer does not
    /// wait for a turn-ending commit or another turn's commit that holds
    /// nothing it sends. A write with send rows whose handle is lost (LF-3) ⇒
    /// commit a barrier row (`PartitionWriter::send_barrier`, with the engine
    /// held) and wait for it. A suspended grant: wait as `sync` does.
    pub async fn sync_sends(&mut self) -> cairn_store::Result<SendTicket> {
        loop {
            if let Some(settle) = self.writer().paused() {
                self.wait(settle).await?;
                continue;
            }
            match self.writer().send_durability() {
                // Just fell into suspension between two checks: wait as above.
                Err(cairn_store::StoreError::LeaseLost) if self.writer().paused().is_some() => {}
                Err(e) => return Err(e),
                Ok(cairn_store::SendDurability::Ready(t)) => return Ok(t),
                Ok(cairn_store::SendDurability::Barrier) => {
                    self.writer_mut().send_barrier().await?;
                }
                Ok(cairn_store::SendDurability::Wait(pending)) => {
                    let no_fence = None::<std::future::Ready<cairn_store::Result<()>>>;
                    let committed = self.wait_pending(pending, no_fence).await?;
                    return Ok(SendTicket::from(&committed));
                }
            }
        }
    }

    /// Waits for `pending` to be durable (plus the read fence, if any), **releasing the engine** while waiting.
    async fn wait_pending(
        &mut self,
        pending: cairn_store::Pending,
        fence: Option<impl Future<Output = cairn_store::Result<()>> + 'static>,
    ) -> cairn_store::Result<Committed> {
        let wait = self.durable_wait(pending, fence);
        self.wait(wait).await
    }

    /// The waiting part of a ticket: `pending` durable and the read fence (if
    /// any), in parallel — a `'static` future, borrowing no engine: the caller
    /// waits on it **after** releasing the engine ([`Self::wait_pending`] takes
    /// it back, [`Self::sync_reply_owned`] does not).
    fn durable_wait(
        &self,
        pending: cairn_store::Pending,
        fence: Option<impl Future<Output = cairn_store::Result<()>> + 'static>,
    ) -> impl Future<Output = cairn_store::Result<Committed>> + 'static {
        let partition = self.partition().0;
        crate::metrics::sdk::DURABLE_WAITS.bump(partition);
        async move {
            cairn_store::probe::mark("sync.begin", u64::from(partition));
            let started = tokio::time::Instant::now();
            let r = async {
                // A point **inside** the wait: the engine is released, so
                // blocking here does not block the range's other turns.
                cairn_store::failpoint!("lease::sync::wait", err);
                let fence = async {
                    match fence {
                        Some(f) => f.await,
                        None => Ok(()),
                    }
                };
                let (durable, fenced) = futures::join!(pending.durable(), fence);
                let durable = durable?;
                fenced.map(|()| durable)
            }
            .await;
            crate::metrics::latency::COMMIT.observe(started.elapsed());
            cairn_store::probe::mark("sync.end", u64::from(partition));
            crate::stats::note_durable_wait(started.elapsed());
            r
        }
    }

    /// The part of a ticket done **while holding the engine**: wait out a grant
    /// suspension (release, then retake — rare), capture the durability mark
    /// and build the read fence.
    async fn ticket(
        &mut self,
        fenced: bool,
    ) -> cairn_store::Result<(
        cairn_store::Durability,
        Option<impl Future<Output = cairn_store::Result<()>> + 'static>,
    )> {
        let durability = loop {
            match self.writer().paused() {
                Some(settle) => self.wait(settle).await?,
                None => match self.writer().durability() {
                    // Just fell into suspension between two checks: wait as above.
                    Err(cairn_store::StoreError::LeaseLost) if self.writer().paused().is_some() => {
                    }
                    d => break d?,
                },
            }
        };
        // The fence request is only issued when the future is first polled —
        // after every read of the answer, all of which finished before here.
        let fence = if fenced {
            self.writer().read_fence()
        } else {
            None
        };
        Ok((durability, fence))
    }

    async fn sync_with(&mut self, fenced: bool) -> cairn_store::Result<Committed> {
        let (durability, fence) = self.ticket(fenced).await?;
        match durability {
            // Everything is durable ⇒ the ticket is issued on the spot, no
            // await, no mutex release. This is the path of most `sync` calls —
            // a redundant call is nearly free, and that is what keeps "call it
            // at every boundary" affordable.
            cairn_store::Durability::Ready(c) => {
                if let Some(fence) = fence {
                    self.wait(fence).await?;
                }
                Ok(c)
            }
            cairn_store::Durability::Wait(pending) => self.wait_pending(pending, fence).await,
        }
    }

    /// Like [`Self::sync_with`] for the task's **final** ticket: consumes the
    /// `Lease`, releases the engine before the wait and **does not take it
    /// back**. The durability mark and the fence are still captured while
    /// holding — the ticket promises exactly what [`Self::sync_with`] promises.
    async fn sync_owned_with(mut self, fenced: bool) -> cairn_store::Result<Committed> {
        let (durability, fence) = self.ticket(fenced).await?;
        match durability {
            cairn_store::Durability::Ready(c) => {
                drop(self);
                if let Some(fence) = fence {
                    fence.await?;
                }
                Ok(c)
            }
            cairn_store::Durability::Wait(pending) => {
                let wait = self.durable_wait(pending, fence);
                drop(self);
                wait.await
            }
        }
    }
}

/// Network I/O jobs running on **separate tasks** of the engine thread, in
/// parallel with each other and with the task that issued them.
///
/// [`Lease::released`] hands `Released` to exactly one closure, and the task
/// holding the `Lease` does nothing else until the closure finishes. The
/// outbox drainer needs more than that: send to many destination ranges at
/// once and **apply the result of whichever destination finishes first** while
/// other destinations are still sending. Applying a result needs the engine,
/// so the sending must live outside that task.
///
/// Rule 1 still holds **per task**: a sending task gets a [`Released`] for
/// its whole life because it holds no `Lease` — a `'static` closure cannot
/// borrow the issuer's `Lease`, and no other task hands it one. The issuing
/// task never waits on the network while holding the engine: it only waits for
/// results through [`Lease::next_io`] (releasing the engine like
/// [`Lease::wait`]). While it holds the engine to apply one result, the I/O of
/// the other destinations keeps running — nobody waits for anybody, so there
/// is no wait cycle for rule 1 to prevent (`tests/transport_engine.rs`).
///
/// Drop cancels every job still in flight (`JoinSet`): their outcome is like a
/// send that hit its limit — it may have arrived, the source row is intact, it
/// is resent later and the receiver deduplicates by key (IR-5).
pub struct IoSet<T> {
    set: tokio::task::JoinSet<T>,
}

impl<T: 'static> Default for IoSet<T> {
    fn default() -> Self {
        Self {
            set: tokio::task::JoinSet::new(),
        }
    }
}

impl<T: 'static> IoSet<T> {
    /// Issues `io` on a separate task of this thread (`spawn_local`). The
    /// failpoint's current node (task-local) goes along.
    pub fn spawn(&mut self, io: impl AsyncFnOnce(&Released) -> T + 'static) {
        let node = cairn_store::failpoint::current_node();
        self.set.spawn_local(async move {
            let run = async move {
                let token = Released {
                    _not_send: PhantomData,
                };
                cairn_store::failpoint!("lease::released::io");
                io(&token).await
            };
            match node {
                Some(n) => cairn_store::failpoint::in_node(&n, run).await,
                None => run.await,
            }
        });
    }

    /// Number of jobs still in flight.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// A finished job, if any — without waiting.
    pub fn try_next(&mut self) -> Option<T> {
        loop {
            let done = self.set.try_join_next()?;
            if let Some(out) = self.finish(Some(done)) {
                return Some(out);
            }
        }
    }

    /// A panic of a sending task propagates to the issuer; a cancelled task
    /// (runtime shutting down) is as if it never existed — its source row stays.
    fn finish(&mut self, done: Option<Result<T, tokio::task::JoinError>>) -> Option<T> {
        match done? {
            Ok(out) => Some(out),
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(_) => None,
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.note_hold();
    }
}

impl std::ops::Deref for Lease {
    type Target = Engine;

    fn deref(&self) -> &Engine {
        self.guard
            .as_ref()
            .expect("the engine is only absent inside Lease::released")
    }
}

impl std::ops::DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut Engine {
        self.guard
            .as_mut()
            .expect("the engine is only absent inside Lease::released")
    }
}

/// What to do when an id that a failed take asked for is released ([`Locks::on_release`]).
pub type ReleaseHook = Rc<dyn Fn(&InvocationId)>;

/// The partition's other two locks.
#[derive(Default)]
pub struct Locks {
    /// One lock per invocation *currently* in use. The table shrinks when the
    /// last guard of an id is released — keeping every id that ever ran forever
    /// is a slow memory leak no test sees.
    inv: RefCell<HashMap<InvocationId, Arc<Mutex<()>>>>,
    /// Ids that [`Self::try_invocation`] asked for and **failed** to take —
    /// i.e. rows are waiting for their release.
    wanted: RefCell<HashSet<InvocationId>>,
    /// Called when an id in `wanted` has just been released: kicks the drainer
    /// to rescan, and parked tickets nacked because of that lock ask for a slot
    /// again.
    wake: RefCell<Option<ReleaseHook>>,
}

impl Locks {
    /// Locks an invocation. Releases the engine while waiting (rule 1).
    pub async fn invocation<'a>(&'a self, engine: &mut Lease, id: InvocationId) -> InvGuard<'a> {
        let slot = {
            let mut map = self.inv.borrow_mut();
            map.entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let guard = engine.wait(slot.lock_owned()).await;
        InvGuard {
            locks: self,
            id,
            guard: Some(guard),
        }
    }

    /// Tries to lock an invocation **without waiting**. `None` = someone holds it.
    ///
    /// For the outbox drainer: it must not stand waiting for the lock of an
    /// invocation in the middle of a turn — that means waiting out someone
    /// else's whole SDK turn, during which no row of the whole partition is
    /// sent. When the take fails the id is recorded in `wanted`, and the guard
    /// holding it kicks the drainer on release ([`Self::on_release`]) so the
    /// skipped rows are handled at once.
    pub fn try_invocation(&self, id: InvocationId) -> Option<InvGuard<'_>> {
        let slot = {
            let mut map = self.inv.borrow_mut();
            map.entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        match slot.try_lock_owned() {
            Ok(guard) => Some(InvGuard {
                locks: self,
                id,
                guard: Some(guard),
            }),
            Err(_) => {
                self.wanted.borrow_mut().insert(id);
                None
            }
        }
    }

    /// Whether anyone holds the lock of `id` — asking without taking, and
    /// without leaving a "someone is waiting" mark.
    pub fn is_busy(&self, id: &InvocationId) -> bool {
        self.inv
            .borrow()
            .get(id)
            .is_some_and(|m| m.try_lock().is_err())
    }

    /// What to do when an id that [`Self::try_invocation`] failed to take is
    /// released. Set once when the partition is built.
    pub fn on_release(&self, wake: ReleaseHook) {
        *self.wake.borrow_mut() = Some(wake);
    }
}

/// The right to run a turn for exactly one invocation.
///
/// Owned rather than borrowed: `drive` must *consume* it before locking the
/// next invocation in the `EXCLUSIVE` queue, and the cheapest way to guarantee
/// that is to make the compiler refuse reuse.
pub struct InvGuard<'a> {
    locks: &'a Locks,
    id: InvocationId,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for InvGuard<'_> {
    fn drop(&mut self) {
        // Release the lock **before** counting: this guard itself holds one
        // `Arc` branch, so counting before releasing means the table never
        // shrinks.
        self.guard = None;
        let mut map = self.locks.inv.borrow_mut();
        if map
            .get(&self.id)
            .is_some_and(|slot| Arc::strong_count(slot) == 1)
        {
            map.remove(&self.id);
        }
        drop(map);
        // Rows were skipped because of exactly this lock ⇒ kick the drainer to
        // rescan. Only kick when someone needs it — kicking on every release
        // would make the drainer spin at the end of every turn.
        if self.locks.wanted.borrow_mut().remove(&self.id) {
            if let Some(wake) = self.locks.wake.borrow().as_ref() {
                wake(&self.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::flow::Scheduler,
        cairn_store::{LeadershipGrant, PartitionId, PartitionWriter, QueueName, TenantId},
        futures::FutureExt,
        slatedb::object_store::memory::InMemory,
        std::task::Poll,
    };

    async fn engine() -> Engine {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = slatedb::Db::builder("p0", store)
            .build()
            .await
            .expect("open SlateDB");
        let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
            .await
            .expect("open writer");
        Engine::open(writer).await.expect("open engine")
    }

    fn queue(service: &str) -> QueueName {
        QueueName {
            tenant: TenantId::default(),
            scope: None,
            limit: None,
            exclusive: false,
            service: service.into(),
            object_key: String::new(),
        }
    }

    /// Cancelling a task **right when it has just received a slot and is
    /// waiting to retake the engine** must not drop the slot.
    ///
    /// The interleaving (exactly the shape of `turn::acquire_turn` under
    /// `JoinSet::shutdown` on losing leadership — `s3_slowdown` caught it as a
    /// panic of `Permit::drop` on the engine thread):
    ///
    /// 1. The range has one slot; P0 holds it.
    /// 2. Task A holds the engine and calls `wait(scheduler.acquire(..))`:
    ///    releases the engine, stands waiting for a slot.
    /// 3. Task B takes the engine and holds it.
    /// 4. P0 releases ⇒ the scheduler grants the slot to A. A wakes, receives
    ///    the `Permit` from the inner future, then stands waiting to retake the
    ///    engine (B holds it).
    /// 5. The test cuts in here: A is cancelled.
    ///
    /// Must see: no panic, the slot goes back to the scheduler — the next
    /// requester is granted at once.
    #[tokio::test(flavor = "current_thread")]
    async fn wait_cancelled_while_relocking_engine_returns_the_turn() {
        let lock = engine_lock(engine().await);
        let s = Scheduler::new(1);
        let p0 = s.acquire(&queue("P0"), [0; 24], 0).await;

        let mut a = Box::pin(async {
            let mut lease = Lease::acquire(&lock).await;
            let p = lease.wait(s.acquire(&queue("A"), [1; 24], 1)).await;
            s.release_all(p);
        });
        assert!(futures::poll!(a.as_mut()).is_pending(), "A awaits slot");

        let b = Lease::acquire(&lock)
            .now_or_never()
            .expect("A has released the engine");
        s.release_all(p0);
        assert!(
            matches!(futures::poll!(a.as_mut()), Poll::Pending),
            "A must wait for the engine (B holds it)"
        );
        assert_eq!(s.turns_in_use(), 1, "A has received the slot");

        drop(a);
        assert_eq!(s.turns_in_use(), 0, "cancelled task's slot was lost");
        let next = s
            .acquire(&queue("C"), [2; 24], 2)
            .now_or_never()
            .expect("the next requester must be granted at once");
        s.release_all(next);
        drop(b);
    }

    type Task<'a> = std::pin::Pin<Box<dyn Future<Output = ()> + 'a>>;

    /// A task asks for the engine, records its name in `order` when it gets it, then releases at once.
    fn ask<'a>(
        lock: &'a EngineLock,
        order: &'a RefCell<Vec<&'static str>>,
        holder: Option<&'static str>,
        name: &'static str,
    ) -> Task<'a> {
        let fut = async move {
            let lease = Lease::acquire(lock).await;
            order.borrow_mut().push(name);
            drop(lease);
        };
        match holder {
            Some(h) => Box::pin(holding_as(h, fut)),
            None => Box::pin(fut),
        }
    }

    /// Polls every unfinished task in turn, many rounds, until all are done.
    async fn run_all(tasks: &mut [Option<Task<'_>>]) {
        for _ in 0..64 {
            for t in tasks.iter_mut() {
                if let Some(f) = t {
                    if futures::poll!(f.as_mut()).is_ready() {
                        *t = None;
                    }
                }
            }
            if tasks.iter().all(Option::is_none) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("tasks not done after 64 poll rounds");
    }

    /// **Commands go before background work at the start of a term.**
    ///
    /// The interleaving (exactly the start-of-term shape that was measured:
    /// the first command queued behind ~3 holds of background tasks, 59–80 ms
    /// of "waiting for the engine" at H = 10⁶ cold):
    ///
    /// 1. Task H holds the engine (the first `restore` chunk).
    /// 2. Three background jobs of the term ask for the engine and queue:
    ///    `restore` (the next chunk), `drain` (the first drain pass), `tick`
    ///    (the first background tick).
    /// 3. Command C (a status read — a command task, no background label) asks
    ///    for the engine, **after** all three.
    /// 4. H releases. The test polls the background jobs before C in every
    ///    round.
    ///
    /// Must see: C takes the engine right after H, before all three background
    /// jobs; then the background jobs take it in the order they asked.
    #[tokio::test(flavor = "current_thread")]
    async fn command_goes_before_queued_background_at_term_start() {
        let lock = engine_lock(engine().await);
        let order = RefCell::new(Vec::new());
        let h = Lease::acquire(&lock).await;
        let mut tasks = vec![
            Some(ask(&lock, &order, Some("restore"), "restore")),
            Some(ask(&lock, &order, Some("drain"), "drain")),
            Some(ask(&lock, &order, Some("tick"), "tick")),
            Some(ask(&lock, &order, None, "command")),
        ];
        for t in tasks.iter_mut().flatten() {
            assert!(futures::poll!(t.as_mut()).is_pending(), "H holds engine");
        }
        drop(h);
        run_all(&mut tasks).await;
        assert_eq!(
            *order.borrow(),
            ["command", "restore", "drain", "tick"],
            "the command must take the engine before the queued background work"
        );
    }

    /// The start of the term is over ([`EngineLock::end_precedence`]): like
    /// [`command_goes_before_queued_background_at_term_start`] but with the
    /// window closed. Must see: FIFO as before the precedence rule — the
    /// command goes after the three background jobs.
    #[tokio::test(flavor = "current_thread")]
    async fn background_is_fifo_after_the_opening() {
        let lock = engine_lock(engine().await);
        lock.end_precedence();
        let order = RefCell::new(Vec::new());
        let h = Lease::acquire(&lock).await;
        let mut tasks = vec![
            Some(ask(&lock, &order, Some("restore"), "restore")),
            Some(ask(&lock, &order, Some("drain"), "drain")),
            Some(ask(&lock, &order, Some("tick"), "tick")),
            Some(ask(&lock, &order, None, "command")),
        ];
        for t in tasks.iter_mut().flatten() {
            assert!(futures::poll!(t.as_mut()).is_pending(), "H holds engine");
        }
        drop(h);
        run_all(&mut tasks).await;
        assert_eq!(*order.borrow(), ["restore", "drain", "tick", "command"]);
    }

    /// **Yielding is bounded**: a burst of commands does not starve background
    /// work.
    ///
    /// 1. The start-of-term window is open; H holds the engine; the drainer
    ///    asks for the engine.
    /// 2. Two commands are always waiting: whenever one takes the engine a new
    ///    one arrives, up to 100 commands.
    /// 3. H releases.
    ///
    /// Must see: the drainer takes the engine after at most `YIELD_MAX` + 2
    /// commands (the yields have hit their cap, plus commands already queued
    /// ahead of it when it re-entered the queue).
    #[tokio::test(flavor = "current_thread")]
    async fn background_yields_to_at_most_yield_max_commands() {
        let lock = engine_lock(engine().await);
        let order = RefCell::new(Vec::new());
        let h = Lease::acquire(&lock).await;
        let mut drain = Some(ask(&lock, &order, Some("drain"), "drain"));
        let mut commands = std::collections::VecDeque::new();
        for _ in 0..2 {
            commands.push_back(ask(&lock, &order, None, "command"));
        }
        let mut asked = 2;
        assert!(futures::poll!(drain.as_mut().unwrap().as_mut()).is_pending());
        for c in commands.iter_mut() {
            assert!(futures::poll!(c.as_mut()).is_pending());
        }
        drop(h);
        for _ in 0..1000 {
            if let Some(d) = drain.as_mut() {
                if futures::poll!(d.as_mut()).is_ready() {
                    drain = None;
                }
            }
            // At most one command finishes per round — the drainer is polled
            // between two takes, as if each task were a real task of the
            // engine thread.
            for i in 0..commands.len() {
                if futures::poll!(commands[i].as_mut()).is_ready() {
                    drop(commands.remove(i));
                    if asked < 100 {
                        let mut c = ask(&lock, &order, None, "command");
                        let _ = futures::poll!(c.as_mut());
                        commands.push_back(c);
                        asked += 1;
                    }
                    break;
                }
            }
            if drain.is_none() && commands.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(drain.is_none() && commands.is_empty(), "tasks not done");
        let order = order.borrow();
        let at = order.iter().position(|n| *n == "drain").expect("drainer");
        assert!(
            at <= YIELD_MAX as usize + 2,
            "the drainer took the engine after {at} commands — cap {} + 2",
            YIELD_MAX
        );
        assert!(
            at >= YIELD_MAX as usize,
            "the drainer must yield to waiting commands ({at})"
        );
    }

    /// **The final answer does not wait to retake the engine.** Measured at
    /// H = 10⁶ cold: the first read command released the engine to wait for
    /// its durability ticket + read fence, the first drain pass took the engine
    /// meanwhile and held it 34 ms, and the command waited 34 ms to retake it
    /// only to release it right after answering.
    ///
    /// 1. Command C holds the engine, commits a write (not durable yet).
    /// 2. C asks for a durability ticket for its final answer
    ///    (`sync_reply_owned`): releases the engine while waiting for the WAL
    ///    flush.
    /// 3. Task B takes the engine meanwhile and holds it until the test lets go.
    ///
    /// Must see: C's ticket arrives while B still holds the engine (C does not
    /// retake it).
    #[tokio::test(flavor = "current_thread")]
    async fn final_reply_does_not_wait_to_reacquire_the_engine() {
        let lock = engine_lock(engine().await);
        let mut c = Lease::acquire(&lock).await;
        let mut m = cairn_store::Mutations::new();
        m.pin_deployment(&[1; 24], "d");
        c.writer_mut().commit(m).await.expect("commit");
        let mut reply = Box::pin(c.sync_reply_owned());
        assert!(
            futures::poll!(reply.as_mut()).is_pending(),
            "write not durable yet — C waits, engine released"
        );
        let b = Lease::acquire(&lock)
            .now_or_never()
            .expect("C released the engine while waiting for the ticket");
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), reply).await;
        assert!(
            got.is_ok(),
            "the final answer's ticket waited to retake the engine B holds"
        );
        got.unwrap().expect("durability ticket");
        drop(b);
    }

    /// **A turn handed out by the scheduler is not a command.**
    ///
    /// 1. The start-of-term window is open; H holds the engine.
    /// 2. The next `restore` chunk asks for the engine, then a `Runner` turn
    ///    (start of term: `recover` hands out turns while the slot-granting gate
    ///    is still closed — the turn only parks its ticket).
    /// 3. H releases.
    ///
    /// Must see: FIFO — `restore` first, the turn after. Counted as a command,
    /// the turn would make each `restore` chunk yield to many turns just to
    /// park tickets, and the gate would open late.
    #[tokio::test(flavor = "current_thread")]
    async fn runner_turn_does_not_jump_the_restore() {
        let lock = engine_lock(engine().await);
        let order = RefCell::new(Vec::new());
        let h = Lease::acquire(&lock).await;
        let turn: Task<'_> = Box::pin(runner_turn(ask(&lock, &order, Some("turn"), "turn")));
        let mut tasks = vec![
            Some(ask(&lock, &order, Some("restore"), "restore")),
            Some(turn),
        ];
        for t in tasks.iter_mut().flatten() {
            assert!(futures::poll!(t.as_mut()).is_pending(), "H holds engine");
        }
        drop(h);
        run_all(&mut tasks).await;
        assert_eq!(*order.borrow(), ["restore", "turn"]);
    }
}
