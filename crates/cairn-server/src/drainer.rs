// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! One drainer per partition, and a monotonic mark to synchronise with it.
//!
//! # Why exactly one reader
//!
//! Before this module, `drain_outbox` was called opportunistically from five places —
//! including from `/invoke`. Two concurrent drain turns would pick up the same rows and
//! process them twice, so there had to be a lock; and that lock was held **for the whole
//! loop**, and inside the loop is a whole SDK turn for each child. The measured
//! consequence: one fan-out closed the whole partition's front door for as long as it was
//! draining.
//!
//! The fix is not finer-grained locking. That was tried and rolled back — splitting the lock
//! plus a "claim" table in RAM turned the benchmark green but broke a semantics already
//! shipped, and the patch for that still had a race window. The deeper reason: the outbox
//! table is keyed by `(invocation, completion)`, so the question "which drain turn does this
//! row belong to" **cannot be represented** — every patch has to spawn an auxiliary book in
//! RAM, and that book is exactly where it breaks.
//!
//! With one reader that question does not exist. No claim table, no drain lock. The whole
//! class of bugs disappears **by definition**, not by a patch.
//!
//! # Why a mark, not an event
//!
//! Whoever needs to synchronise (`/invoke` must answer after the work it spawned has run)
//! keeps the highest `seq` it just produced and **compares numbers** with the published
//! mark. There is no waiting for a "just drained a batch" notification.
//!
//! That is exactly the fix for the mistake of the previous attempt. Waiting for an event
//! always leaves a window between observing and registering to wait, and a window like that
//! is timing-sensitive: the e2e suite went red 2/90, then green again when logging was
//! added. Comparing a number you hold with a monotonic mark leaves **nothing to miss**,
//! because the mark is not a moment — it is a state, and a state can be reread.
//!
//! The concrete shape is in [`Drainer::reached`]: **read the mark, mark it as seen, and
//! only then wait for a change**. All three steps are needed, and so is the order: it turns
//! "the mark was passed earlier" and "the mark has just been passed" into the same code
//! path. That is the property `mark_already_passed_does_not_wait` pins down.

use {
    crate::lease::Lease,
    cairn_engine::InvocationId,
    std::{
        cell::{Cell, RefCell},
        rc::Rc,
        time::Duration,
    },
    tokio::{
        sync::{mpsc, watch, Notify},
        time::Instant,
    },
};

/// A turn that needs to run, handed from the background path to the scheduler.
#[derive(Clone, Copy, Debug)]
pub struct RunRequest {
    pub id: InvocationId,
    /// `Some` when this is a scheduled retry: the turn carries the number of attempts so far
    /// (for `ctx.attempt`) and the next reschedule has been set before running — the turn
    /// entering `Running` rewrites it with `attempt + 1`, a turn that finishes without
    /// needing another retry cancels exactly that reschedule (`cairn_lifecycle::Planned`).
    pub planned: Option<cairn_lifecycle::Planned>,
    /// `Some` when the turn carries an undelivered timer: the background tick saw the
    /// invocation busy so it could not write it, and hands the wake-up to the task as well —
    /// the task is allowed to *wait* for the lock, the background tick is not.
    pub timer: Option<TimerWake>,
}

#[derive(Clone, Copy, Debug)]
pub struct TimerWake {
    pub completion_id: u32,
    pub due_ms: u64,
}

/// The hand-over path: "this invocation is durable and already queued — let it run."
///
/// # Why the drain loop does not run the child itself
///
/// The drain loop used to do two jobs in one: `dispatch_call` created the callee (a few
/// commits), then `drive` ran **a whole SDK turn** of it. The second job is many times more
/// expensive than the first, and it does not belong on the drain path: draining is
/// *emitting* work, not *doing* it.
///
/// Split apart, the shape becomes exactly the one the scheduler was built for: an
/// invocation no longer has to drive the next one by hand when it finishes; each task waits
/// for its own slot and the scheduler wakes it.
///
/// Each child becomes a task waiting for its own slot — exactly like a call coming from
/// `/invoke`. That way the concurrency cap applies to them like to every other turn, and the
/// service order is the order in the **durable queue**, not "whoever the one just finished
/// happened to see".
///
/// The channel is **unbounded** on purpose: blocking here would block the drain loop, i.e.
/// rebuild exactly the bottleneck just removed. But the channel only carries **data**
/// ([`RunRequest`], a few dozen bytes): the **intake gate** is bounded — the serve loop only
/// takes a request while the number of tasks passing the gate (reading state, the place in
/// the queue, asking for a slot) is below [`ADMIT_CAP`] ([`Self::admit`]). A turn that
/// passes the gate without being granted a slot right away **parks a ticket** in the
/// scheduler's line (`flow::Ticket`) and the task ends: no task lives for an invocation
/// that has not been granted a slot. A slot granted to the ticket ⇒ the serve loop spawns
/// the turn; the invocation lock busy at that moment ⇒ the slot goes back to the scheduler
/// (nack), the ticket waits for the lock to be released. The real cap on turns is still in
/// the scheduler, measurable through `/vqueues`.
#[derive(Clone)]
pub struct Runner {
    tx: mpsc::UnboundedSender<RunRequest>,
    /// Timers handed to the scheduler whose carrying turn has not finished. The background
    /// tick runs a few times a second; without this book a long turn would make each tick
    /// hand over one more copy of the same timer — harmless for correctness (the machine
    /// skips completions already delivered), but each copy is a wasted SDK turn.
    timers_in_flight: Rc<std::cell::RefCell<cairn_store::entropy::HashSet<(InvocationId, u32)>>>,
    /// Handed-over turns whose running task has not finished, by invocation — so the safety
    /// net ([`Rescue`]) knows "someone has already been handed this work", even while the
    /// task has not yet won the invocation lock (still in the channel, or waiting for the
    /// engine).
    queued: Rc<RefCell<cairn_store::entropy::HashMap<InvocationId, u32>>>,
    /// Tasks passing the intake gate ([`Self::admit`]) and the "room just freed" signal.
    admitting: Rc<Cell<usize>>,
    room: Rc<Notify>,
}

/// Cap on tasks passing a range's **intake gate** at once: reading state, the place in the
/// queue, asking for a slot — each task one engine hold, then it parks a ticket or runs the
/// turn. A task waiting for the invocation lock of a turn in flight holds no place
/// ([`Admit`] is released before waiting).
pub const ADMIT_CAP: usize = 64;

/// A place at the intake gate ([`Runner::admit`]) — released on drop.
pub struct Admit {
    admitting: Rc<Cell<usize>>,
    room: Rc<Notify>,
}

impl Drop for Admit {
    fn drop(&mut self) {
        let n = self.admitting.get();
        self.admitting.set(n.saturating_sub(1));
        if n == ADMIT_CAP {
            self.room.notify_one();
        }
    }
}

impl Runner {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<RunRequest>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx,
                timers_in_flight: Rc::default(),
                queued: Rc::default(),
                admitting: Rc::default(),
                room: Rc::new(Notify::new()),
            },
            rx,
        )
    }

    /// Whether the intake gate has room — the serve loop only takes requests when it does.
    pub fn has_room(&self) -> bool {
        self.admitting.get() < ADMIT_CAP
    }

    /// Wait until the intake gate has room again (after [`Self::has_room`] returned `false`).
    pub async fn room(&self) {
        if !self.has_room() {
            self.room.notified().await;
        }
    }

    /// A place at the intake gate, for a task that has just taken a request.
    pub fn admit(&self) -> Admit {
        self.admitting.set(self.admitting.get() + 1);
        Admit {
            admitting: Rc::clone(&self.admitting),
            room: Rc::clone(&self.room),
        }
    }

    /// Hand an invocation to the scheduler. Returns right away.
    ///
    /// A missing receiver = the partition is stepping down. No work is lost: the invocation
    /// is durable and still in the queue, so the next leader picks it up in recovery
    /// (INV-3). So this is a log line, not an error.
    pub fn queue(&self, id: InvocationId) {
        cairn_store::probe::mark("runner.queue", 0);
        self.send(RunRequest {
            id,
            planned: None,
            timer: None,
        });
    }

    /// Hand over an invocation **together with** its undelivered timer.
    pub fn queue_timer(&self, id: InvocationId, completion_id: u32, due_ms: u64) {
        if !self
            .timers_in_flight
            .borrow_mut()
            .insert((id, completion_id))
        {
            return;
        }
        self.send(RunRequest {
            id,
            planned: None,
            timer: Some(TimerWake {
                completion_id,
                due_ms,
            }),
        });
    }

    /// Hand over a scheduled retry — see [`RunRequest::planned`].
    pub fn queue_planned(&self, id: InvocationId, planned: cairn_lifecycle::Planned) {
        self.send(RunRequest {
            id,
            planned: Some(planned),
            timer: None,
        });
    }

    /// Timer `(id, completion_id)` handed to a turn that has not finished.
    pub fn timer_in_flight(&self, id: &InvocationId, completion_id: u32) -> bool {
        self.timers_in_flight
            .borrow()
            .contains(&(*id, completion_id))
    }

    /// Whether any handed-over turn of `id` has not finished.
    pub fn is_queued(&self, id: &InvocationId) -> bool {
        self.queued.borrow().contains_key(id)
    }

    /// The task running turn `req` has just taken it from the channel: the returned guard
    /// counts that turn as finished when dropped — on every exit path, cancellation included
    /// — and carries the turn's **wake-up duty** ([`WakeDuty`], nack by default): dropped
    /// while the turn has not acked, it marks the `rescue` safety net for that id.
    ///
    /// The guard also removes the timer the turn carries from `timers_in_flight` when dropped
    /// (the turn finished, the ticket was withdrawn or lost with the term): the next tick
    /// hands it over again if the timer row is still there (i.e. that turn failed before it
    /// could write).
    pub fn running(&self, req: &RunRequest, rescue: &Rc<Rescue>) -> Dispatched {
        Dispatched {
            _queued: QueuedTurn {
                queued: Rc::clone(&self.queued),
                id: req.id,
            },
            duty: WakeDuty::new(Rc::clone(rescue), req.id),
            _timer: req.timer.map(|t| TimerInFlight {
                set: Rc::clone(&self.timers_in_flight),
                key: (req.id, t.completion_id),
            }),
        }
    }

    fn send(&self, req: RunRequest) {
        *self.queued.borrow_mut().entry(req.id).or_default() += 1;
        if self.tx.send(req).is_err() {
            drop(QueuedTurn {
                queued: Rc::clone(&self.queued),
                id: req.id,
            });
            tracing::debug!(
                invocation = %req.id.iter().map(|b| format!("{b:02x}")).collect::<String>(),
                "could not hand over to the scheduler — the partition is stepping down"
            );
        }
    }
}

/// Guard of [`Runner::running`]: the handed-over turn is running, together with its wake-up
/// duty.
pub struct Dispatched {
    _queued: QueuedTurn,
    duty: WakeDuty,
    _timer: Option<TimerInFlight>,
}

/// A timer handed over with a turn that has not finished (`Runner::timers_in_flight`) —
/// removed on drop.
struct TimerInFlight {
    set: Rc<std::cell::RefCell<cairn_store::entropy::HashSet<(InvocationId, u32)>>>,
    key: (InvocationId, u32),
}

impl Drop for TimerInFlight {
    fn drop(&mut self) {
        self.set.borrow_mut().remove(&self.key);
    }
}

impl Dispatched {
    /// The turn's wake-up duty — given to `turn::drive*`, the only place that knows whether
    /// the turn has left a durable wake-up.
    pub fn duty(&mut self) -> &mut WakeDuty {
        &mut self.duty
    }
}

/// A handed-over turn whose running task has not finished ([`Runner::is_queued`]).
struct QueuedTurn {
    queued: Rc<RefCell<cairn_store::entropy::HashMap<InvocationId, u32>>>,
    id: InvocationId,
}

impl Drop for QueuedTurn {
    fn drop(&mut self) {
        let mut queued = self.queued.borrow_mut();
        if let Some(n) = queued.get_mut(&self.id) {
            *n -= 1;
            if *n == 0 {
                queued.remove(&self.id);
            }
        }
    }
}

/// **The wake-up duty of a turn — nack by default** (a lesson from Temporal: a delivered
/// task only disappears once it is acked).
///
/// Whichever task runs a turn of an invocation (a turn handed over by the scheduler,
/// `/invoke`, `/send`, `/resume`, an external signal) holds one for that id, and
/// `turn::drive*` demands it: a turn cannot run without one. The turn only
/// [`ack`](Self::ack)s once it knows the invocation **needs nobody to remember it**: it has
/// ended or is `paused`, it is suspended waiting for a durable event, it has a durable retry
/// reschedule (written in the commit that ends the turn, or set before the turn — a turn
/// with a reschedule set beforehand), or write authority was lost (the next term
/// `recover`s).
///
/// Dropped without an ack — a `?` returning `Err` without leaving a reschedule, a panic
/// anywhere in the task, the task cancelled mid-term — it **marks the safety net**
/// ([`Rescue::nack`]) for the id. Previously each error path had to remember to call
/// `lost_wake`, and panicking tasks had to be wrapped separately (`rescued_on_panic`); a new
/// `?` or a new command task that forgot the wrapping was a gap (FL-16). Now that gap cannot
/// be represented: forgetting to ack is a redundant mark, not a lost wake-up.
///
/// `Drop` runs both during unwinding and when the task is cancelled, so it **only touches
/// RAM**: no I/O, no waiting, no `info` logging (AGENTS rule 3, §6 hot path), and no panic
/// ([`Rescue::nack`] does not borrow a `RefCell` that is already borrowed).
pub struct WakeDuty {
    rescue: Rc<Rescue>,
    id: InvocationId,
    acked: bool,
}

impl WakeDuty {
    pub fn new(rescue: Rc<Rescue>, id: InvocationId) -> Self {
        Self {
            rescue,
            id,
            acked: false,
        }
    }

    /// The invocation no longer needs the safety net (see the type head). Can be called
    /// more than once.
    pub fn ack(&mut self) {
        self.acked = true;
    }

    /// The id this duty belongs to.
    pub fn id(&self) -> InvocationId {
        self.id
    }
}

impl Drop for WakeDuty {
    fn drop(&mut self) {
        if !self.acked {
            self.rescue.nack(self.id);
        }
    }
}

/// The first reconciliation after a mark waits this long (with jitter); each later round
/// that is still dirty waits twice as long, up to [`RESCUE_MAX_BACKOFF`].
///
/// Not 0 on purpose: the place that marks is usually a task that **is** failing and still
/// holds the invocation lock; reconciling right away would find it "busy" and waste a
/// round. And the cause of the mark (a throttling store) needs a while to pass.
pub const RESCUE_FIRST_BACKOFF: Duration = Duration::from_millis(500);
/// Cap on the backoff between two reconciliation rounds.
pub const RESCUE_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Cap on the book of marked ids. On overflow, switch to a full scan — one scan like
/// `recover`, which does not grow with the number of failures.
pub const RESCUE_CAP: usize = 10_000;

/// When the safety net stops handing an invocation over again and moves it to `paused` with
/// code [`cairn_engine::INFRA_STUCK`]: after `strikes` consecutive marks, or when the streak
/// of marks has lasted `after` — whichever comes first. "Consecutive" = none of its turns
/// finished between two marks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InfraStuck {
    pub strikes: u32,
    pub after: Duration,
}

impl Default for InfraStuck {
    fn default() -> Self {
        Self {
            strikes: 50,
            after: Duration::from_secs(3600),
        }
    }
}

/// **The safety net of a term**: reconcile by state, not by event.
///
/// Every path that wakes an unfinished invocation — a retry reschedule, a timer, an
/// outbox/inbound row, `recover`'s scan on becoming leader — is an *event* written durably
/// or handed over. There are places where that very event cannot be written: `recover`'s
/// scan fails, a turn fails and its reschedule fails too, a turn fails before it even
/// reaches the reschedule point. Then nothing wakes the invocation until the next leader
/// change — on a single node, never.
///
/// We do not patch each place to make the event "certainly written": that is exactly what
/// cannot be done while the store is failing. The failing place only **marks** — in RAM, no
/// I/O — and the term's `rescue_loop` ([`crate::actor`]) looks at the **durable state**
/// after a backoff: the invocation still `running`, no reschedule, nobody working on it ⇒
/// hand it over again through [`Runner`]. A failed reconciliation marks again, backing off
/// twice as long up to the cap. Looking at state there is no event to miss: one successful
/// reconciliation puts everything marked right, however many failures came before it.
///
/// Bounded in all three dimensions: the id book ([`RESCUE_CAP`], full scan on overflow),
/// the tick (backoff up to [`RESCUE_MAX_BACKOFF`]), and the work per round (one scan like
/// `recover`, or one state read + one reschedule read per id). The healthy path marks
/// nothing, so the net costs nothing. No duplicate turns: an invocation whose lock someone
/// holds, or that already has a handed-over turn not yet finished ([`Runner::is_queued`]),
/// is not handed over again.
///
/// **Backoff per id**: each id carries its number of **consecutive** marks (none of its
/// turns finished between two — [`Self::clear`]) and its own reconciliation deadline: the
/// `n`-th waits `jittered(min(0.5 s × 2ⁿ⁻¹, 30 s))`. A deterministically failing id is no
/// longer handed over again every 0.25–0.5 s forever, and other ids are still rescued right
/// away. Past the [`InfraStuck`] threshold (default 50 times or 1 hour) the reconciliation
/// round moves it to `paused` 577 instead of handing it over again — it does not spin
/// silently forever.
///
/// Lives as long as one term, like [`Drainer`]: the previous term's marks are lost with
/// the RAM, and the next term's `recover` is the full reconciliation.
#[derive(Default)]
pub struct Rescue {
    /// Invocations whose wake-up an error path may have dropped, together with their streak
    /// of marks.
    ids: RefCell<std::collections::BTreeMap<InvocationId, Strikes>>,
    /// The deadline of a **full** scan (`recover`'s scan failed, or the book overflowed), if
    /// one is pending.
    full: Cell<Option<Instant>>,
    /// Backoff of the next full scan — doubles with every failed scan.
    full_backoff: Cell<Duration>,
    wake: Notify,
    /// The term is over ([`Self::close`]): new marks are dropped.
    closed: Cell<bool>,
}

/// An id's streak of consecutive marks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Strikes {
    /// Number of consecutive marks.
    pub count: u32,
    /// The first mark of the streak.
    pub since: Instant,
    /// The latest mark.
    pub last: Instant,
    /// The reconciliation deadline, if waiting for reconciliation. `None` = only the count
    /// is kept (the previous round handed it over again or saw a durable reschedule).
    pub due: Option<Instant>,
}

impl Strikes {
    /// Whether it has passed the `paused` 577 threshold.
    pub fn is_stuck(&self, limit: InfraStuck, now: Instant) -> bool {
        self.count >= limit.strikes || now.saturating_duration_since(self.since) >= limit.after
    }
}

/// Backoff of the `count`-th reconciliation of an id.
fn backoff_of(count: u32) -> Duration {
    RESCUE_FIRST_BACKOFF
        .saturating_mul(1u32 << count.saturating_sub(1).min(16))
        .min(RESCUE_MAX_BACKOFF)
}

impl Rescue {
    pub fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// `id`'s wake-up may have been dropped: one more mark in its streak, reconciled after
    /// that mark's backoff. Returns right away. If the book is full, switch to a full scan.
    pub fn mark(&self, id: InvocationId) {
        if self.closed.get() {
            return;
        }
        crate::metrics::flow::bump(&crate::metrics::flow::RESCUE_MARKED);
        self.strike(id);
        self.wake.notify_one();
    }

    /// [`Self::mark`] from the `Drop` of an unacked [`WakeDuty`] (nack by default). Runs
    /// during unwinding too: if the book is already borrowed (the panic fired inside one of
    /// the net's own operations), switch to a full scan instead of panicking a second time.
    pub(crate) fn nack(&self, id: InvocationId) {
        if self.closed.get() {
            return;
        }
        crate::metrics::flow::bump(&crate::metrics::flow::RESCUE_MARKED);
        crate::metrics::flow::bump(&crate::metrics::flow::RESCUE_NACKED);
        if self.ids.try_borrow_mut().is_ok() {
            self.strike(id);
        } else {
            self.schedule_full(Instant::now());
        }
        self.wake.notify_one();
    }

    /// The term is over: work in flight is about to be cancelled, and its wake-up duties
    /// belong to the next term's `recover` — marks from here on are dropped, not counted (a
    /// step-down is not a forgotten reschedule).
    pub(crate) fn close(&self) {
        self.closed.set(true);
    }

    /// One more mark for `id` (not counted in metrics): the streak grows, the
    /// reconciliation deadline follows the new step.
    pub(crate) fn strike(&self, id: InvocationId) {
        let now = Instant::now();
        let mut ids = self.ids.borrow_mut();
        if !ids.contains_key(&id) && ids.len() >= RESCUE_CAP {
            drop(ids);
            self.schedule_full(now);
            return;
        }
        let st = ids.entry(id).or_insert(Strikes {
            count: 0,
            since: now,
            last: now,
            due: None,
        });
        st.count = st.count.saturating_add(1);
        st.last = now;
        st.due = Some(now + jittered(backoff_of(st.count)));
    }

    /// Rescan every unfinished invocation at the next reconciliation. Returns right away.
    pub fn mark_all(&self) {
        if self.closed.get() {
            return;
        }
        crate::metrics::flow::bump(&crate::metrics::flow::RESCUE_MARKED);
        self.schedule_full(Instant::now());
        self.wake.notify_one();
    }

    fn schedule_full(&self, now: Instant) {
        if self.full.get().is_none() {
            let b = self.full_backoff.get().max(RESCUE_FIRST_BACKOFF);
            self.full_backoff.set(b);
            self.full.set(Some(now + jittered(b)));
        }
    }

    /// The last full scan failed: reschedule it, backing off twice as long.
    pub(crate) fn full_failed(&self) {
        let b = (self.full_backoff.get().max(RESCUE_FIRST_BACKOFF) * 2).min(RESCUE_MAX_BACKOFF);
        self.full_backoff.set(b);
        self.full.set(Some(Instant::now() + jittered(b)));
    }

    /// Reconcile `id` again after the current step's backoff, **without** growing the streak
    /// — it is busy (someone is working on it), not a new failure.
    pub(crate) fn later(&self, id: InvocationId) {
        if let Some(st) = self.ids.borrow_mut().get_mut(&id) {
            st.due = Some(Instant::now() + jittered(backoff_of(st.count)));
        }
    }

    /// A turn of `id` has just finished: the streak of consecutive failures is broken. The
    /// normal path of a turn — with an empty book it is only one RefCell borrow.
    pub fn clear(&self, id: &InvocationId) {
        let mut ids = self.ids.borrow_mut();
        if !ids.is_empty() {
            ids.remove(id);
        }
    }

    /// Take out for a reconciliation round every id **that is due** (with its streak; its
    /// deadline is removed), and whether a full scan is needed. Ids not yet due stay. Ids no
    /// longer waiting for reconciliation that have been quiet past the threshold cap are
    /// forgotten.
    pub(crate) fn take(
        &self,
        now: Instant,
        limit: InfraStuck,
    ) -> (Vec<(InvocationId, Strikes)>, bool) {
        let mut out = Vec::new();
        self.ids.borrow_mut().retain(|id, st| {
            match st.due {
                Some(d) if d <= now => {
                    out.push((*id, *st));
                    st.due = None;
                }
                Some(_) => {}
                None if now.saturating_duration_since(st.last) >= limit.after => return false,
                None => {}
            }
            true
        });
        let full = matches!(self.full.get(), Some(d) if d <= now);
        if full {
            self.full.set(None);
        }
        (out, full)
    }

    /// The last full scan finished clean: its backoff goes back to the first step.
    pub(crate) fn full_done(&self) {
        self.full_backoff.set(RESCUE_FIRST_BACKOFF);
    }

    /// Forget `id`: it is no longer `running` (done, suspended, `paused`).
    pub(crate) fn forget(&self, id: &InvocationId) {
        self.ids.borrow_mut().remove(id);
    }

    /// The current streak of `id`, if any.
    pub fn strikes(&self, id: &InvocationId) -> Option<Strikes> {
        self.ids.borrow().get(id).copied()
    }

    /// The earliest reconciliation deadline, if anything is waiting.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        let ids = self.ids.borrow().values().filter_map(|s| s.due).min();
        match (ids, self.full.get()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.next_due().is_some()
    }

    /// Wait for a new mark (or one that is already there).
    pub(crate) async fn changed(&self) {
        self.wake.notified().await;
    }
}

pub use cairn_store::pace::jittered;

/// Cap on waiting for the mark on the synchronisation path.
///
/// **Not a latency target** — the healthy path never reaches it. It exists for exactly one
/// case: a *stuck* row (a target not yet in this node's deployment registry) cannot be
/// drained, so the mark cannot pass it, so the waiters behind it would wait forever.
///
/// On hitting the cap the caller rereads the durable state and returns `Waiting` — exactly
/// the answer the old path gave in the same situation, because the old `drain_outbox` also
/// gave up on meeting a stuck row. So this is not a new outcome.
const WAIT_CEILING: Duration = Duration::from_secs(30);

/// A partition's drainer, and the mark it publishes.
pub struct Drainer {
    /// "There are new rows." Carries nothing more than that — the information is in the mark.
    nudge: Notify,
    /// Drained up to this `seq`. Monotonically increasing.
    mark: watch::Sender<u64>,
    /// Rows arrived **while** a turn was running.
    ///
    /// Without this flag a nudge while busy is a nudge nobody hears: [`Notify::notify_one`]
    /// keeps one permit, but the running turn may already have scanned past the new row's
    /// position. The flag turns "may have missed it" into "certainly scan one more turn".
    dirty: Cell<bool>,
    /// Number of drain turns **started**. Together with `passes_done`, it lets a waiter ask
    /// "has a complete turn started *after* I asked" — the question of the queues without a
    /// `seq` (signal, inbound): rows produced before the question are certainly seen by such
    /// a turn.
    passes_started: Cell<u64>,
    passes_done: watch::Sender<u64>,
    /// Undecodable rows seen in this term: `(queue, seq)`. The row is kept, so every drain
    /// turn rereads it — this book is there to say it loudly and handle the waiter **once**,
    /// not on every turn. Bounded.
    poisoned: RefCell<std::collections::BTreeMap<(PoisonQueue, u64), bool>>,
    /// The term's book of outbox rows read ([`Backlog`]). `None` while a drain turn holds it;
    /// a turn failing mid-way does not give it back ⇒ the next turn rescans from 0.
    outbox: RefCell<Option<Backlog>>,
    /// The term's per-target circuit breaker: target range → the range table version when a
    /// send to it hit `SEND_TIMEOUT`.
    breaker: RefCell<cairn_store::entropy::HashMap<cairn_store::RangeId, u64>>,
    /// The term's books of inbound and signal rows ([`RowLedger`], by [`PoisonQueue`]); `None`
    /// while a drain turn holds them.
    ledgers: RefCell<[Option<RowLedger>; 2]>,
    /// The deployment registry the drainer saw in its latest turn, and its generation
    /// ([`Self::catalog_gen`]). Keeps the `Arc` itself so the pointer comparison can never
    /// mistake a new copy allocated exactly where the old one was just freed.
    catalog: RefCell<(Option<std::sync::Arc<crate::registry::Catalog>>, u64)>,
}

/// Outbox rows the drainer has read but **not yet seen disappear** — the term's book in
/// RAM.
///
/// Why it exists: a drain turn used to reread the outbox from `seq` 0 on every turn. A
/// fan-out parent has `Call` rows that must win the parent's lock; while the parent runs a
/// turn all its rows stay, and the next turn — nudged by the parent's own guard on release,
/// or by someone else's work — rereads all those thousands of rows (two point reads per
/// row). Quadratic in the number of children. This book lets the next turn **skip reading**
/// rows that last stayed because the lock of a still-busy invocation, and skip rescanning
/// the stretch already covered.
///
/// **A hint, not a work book** (INV-3): pending work still sits in the outbox table. If the
/// book is lost (leader change, restart, a failed turn) the next turn rescans from 0 —
/// exactly the old path.
///
/// **Invariant:** every live outbox row with `seq < scanned` is in `rows`. It holds
/// because:
/// - the open scan returns **every** live row from the cursor in `seq` order, and each row
///   it returns is written into the book before `scanned` moves past it;
/// - a new `seq` is always larger than every `seq` issued (`Engine::next_message_seq`), so
///   rows produced later never fall below `scanned`; rewriting a still-live row (D-O2) keeps
///   the old `seq` — a row already in the book — or takes a new `seq`;
/// - a row **only leaves the book when a read sees it is gone** — not when the drainer
///   believes it has finished processing it (`Engine::deliver` has a path that commits
///   nothing); a row that is gone does not come back at the same `seq`: the only delete
///   (`stage_outbox_removal`) also removes its D-O2 entry.
#[derive(Default)]
pub(crate) struct Backlog {
    /// The next turn's open scan starts here.
    scanned: u64,
    /// `seq` → `Some(id)`: the row last stayed because `id`'s lock was busy; `None`: must be
    /// reread in the next turn (stayed for another reason, or was processed but not yet seen
    /// to disappear).
    rows: std::collections::BTreeMap<u64, Option<InvocationId>>,
    /// Index of `rows`: the `None` part.
    retry: std::collections::BTreeSet<u64>,
    /// Index of `rows`: the `Some` part, by lock holder.
    parked: cairn_store::entropy::HashMap<InvocationId, std::collections::BTreeSet<u64>>,
}

impl Backlog {
    pub(crate) fn scanned(&self) -> u64 {
        self.scanned
    }

    /// The scan cursor has reached `next`: every live row below it has been [`Self::note`]d.
    pub(crate) fn scanned_to(&mut self, next: u64) {
        debug_assert!(next >= self.scanned, "the scan cursor only moves forward");
        self.scanned = self.scanned.max(next);
    }

    /// Row `seq` has just been read and considered: it stays because `blocked_by`'s lock is
    /// busy (`Some`), or must be reread in the next turn (`None`).
    pub(crate) fn note(&mut self, seq: u64, blocked_by: Option<InvocationId>) {
        self.unindex(seq);
        self.rows.insert(seq, blocked_by);
        match blocked_by {
            Some(id) => {
                self.parked.entry(id).or_default().insert(seq);
            }
            None => {
                self.retry.insert(seq);
            }
        }
    }

    /// A read saw that row `seq` is gone.
    pub(crate) fn forget(&mut self, seq: u64) {
        self.unindex(seq);
        self.rows.remove(&seq);
    }

    fn unindex(&mut self, seq: u64) {
        match self.rows.get(&seq) {
            Some(Some(id)) => {
                if let Some(set) = self.parked.get_mut(id) {
                    set.remove(&seq);
                    if set.is_empty() {
                        self.parked.remove(id);
                    }
                }
            }
            Some(None) => {
                self.retry.remove(&seq);
            }
            None => {}
        }
    }

    /// Whether any row is waiting for an invocation's lock.
    pub(crate) fn has_parked(&self) -> bool {
        !self.parked.is_empty()
    }

    /// Whether row `seq` is in the book.
    pub(crate) fn contains(&self, seq: u64) -> bool {
        self.rows.contains_key(&seq)
    }

    /// Rows to reconsider in this turn, by `seq`: every `None` row, and the rows of lock
    /// holders that `free` says may have released. `free` is asked exactly once for each lock
    /// holder with rows in the book.
    pub(crate) fn due(
        &self,
        mut free: impl FnMut(InvocationId) -> bool,
    ) -> Vec<(u64, Option<InvocationId>)> {
        let mut out: Vec<(u64, Option<InvocationId>)> =
            self.retry.iter().map(|seq| (*seq, None)).collect();
        for (id, seqs) in &self.parked {
            if free(*id) {
                out.extend(seqs.iter().map(|seq| (*seq, Some(*id))));
            }
        }
        out.sort_unstable_by_key(|(seq, _)| *seq);
        out
    }
}

/// Why an inbound/signal row stays in the drainer's book ([`RowLedger`]) — which is also
/// **the condition for rereading it**.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Park {
    /// Reread in the next turn: processed but not yet seen to disappear, or staying for a
    /// reason with no change signal (target send failed, corrupt row, corrupt tenant).
    Again,
    /// This invocation's lock is busy: reread when it is released — the lock holder nudges
    /// the drainer on release (the "someone is waiting" mark of `Locks`).
    Lock(InvocationId),
    /// The L0 throttle deferred `StartCall` to this callee: reread in the next turn, but only
    /// if that turn has not deferred — a deferral defers the whole rest of the turn
    /// (`l0::Throttle::defer_inbound`), so reading the row only to hear "deferred" is a
    /// waste. The callee still goes into the turn's `busy`, as when it is read and then
    /// deferred.
    Shed(InvocationId),
    /// The callee's service is not yet in the node's deployment registry: reread when the
    /// registry changes (the generation of [`Drainer::catalog_gen`] when the row stayed).
    Catalog(u64),
}

/// Inbound (or signal) rows the drainer has read but **not yet seen disappear** — the
/// term's book in RAM (the same pattern as the outbox's [`Backlog`]).
///
/// Why it exists: `drain_inbound`/`drain_signals` used to start from `seq` 0 on every turn,
/// so rows that stayed (target busy, `NotYet`, L0 throttle) were read and decoded again on
/// every turn — 10 000 throttled `StartCall`s meant 10 000 reads per drain turn for the
/// whole throttling period. The book records why a row stays ([`Park`]) and the next turn
/// only rereads rows whose reason has changed; the open scan starts from the previous
/// turn's cursor, not from 0.
///
/// **A hint, not a work book** (INV-3): pending work still sits in the table. If the book
/// is lost (a failed turn, a leader change) the next turn rereads from 0 — the old path.
///
/// **Invariant** (like [`Backlog`]): every live row with `seq < scanned` is in `rows`. The
/// open scan returns every live row from the cursor in `seq` order and each row is written
/// before the cursor moves past it; a new `seq` is always larger than every `seq` issued; a
/// row only leaves the book when a read sees it is gone (a processed row enters the book as
/// [`Park::Again`]). Debug builds check it at the end of the turn with a full scan.
#[derive(Default)]
pub(crate) struct RowLedger {
    scanned: u64,
    rows: std::collections::BTreeMap<u64, Park>,
}

impl RowLedger {
    pub(crate) fn scanned(&self) -> u64 {
        self.scanned
    }

    /// Every live row below `next` has been [`Self::note`]d.
    pub(crate) fn scanned_to(&mut self, next: u64) {
        debug_assert!(next >= self.scanned, "the scan cursor only moves forward");
        self.scanned = self.scanned.max(next);
    }

    pub(crate) fn note(&mut self, seq: u64, park: Park) {
        self.rows.insert(seq, park);
    }

    /// A read saw that row `seq` is gone.
    pub(crate) fn forget(&mut self, seq: u64) {
        self.rows.remove(&seq);
    }

    pub(crate) fn contains(&self, seq: u64) -> bool {
        self.rows.contains_key(&seq)
    }

    /// Whether any row is waiting for a reason to change (not [`Park::Again`]). A book with
    /// no such row saves no read compared with reading from 0 (cheap thanks to the index's
    /// mark): the caller drops the book.
    pub(crate) fn has_parked(&self) -> bool {
        self.rows.values().any(|p| *p != Park::Again)
    }

    /// Rows to consider in this turn, by `seq`: every [`Park::Again`] and [`Park::Shed`] (the
    /// caller skips throttled rows when the turn has deferred), rows waiting for a lock that
    /// `free` says may have been released, rows waiting for the deployment registry when the
    /// generation differs from `catalog`. `free` is asked exactly once per invocation.
    pub(crate) fn due(
        &self,
        catalog: u64,
        mut free: impl FnMut(InvocationId) -> bool,
    ) -> Vec<(u64, Park)> {
        let mut asked: cairn_store::entropy::HashMap<InvocationId, bool> = Default::default();
        self.rows
            .iter()
            .filter(|(_, park)| match park {
                Park::Again | Park::Shed(_) => true,
                Park::Lock(id) => *asked.entry(*id).or_insert_with(|| free(*id)),
                Park::Catalog(gen) => *gen != catalog,
            })
            .map(|(seq, park)| (*seq, *park))
            .collect()
    }
}

/// The queue holding an undecodable row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PoisonQueue {
    Inbound,
    Signal,
}

/// Result of [`Drainer::note_poison`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoisonSight {
    /// First time in the term: say it loudly, count it, then take care of the waiter.
    New,
    /// Already said; the work for the waiter is not done (it is busy) — do it again.
    Pending,
    /// Done — go past it.
    Settled,
}

/// Cap on a term's book of corrupt rows. Past the cap new rows are still kept, just no
/// longer said out loud — the metric has been non-zero for a long time by then.
const POISON_CAP: usize = 4_096;

impl Drainer {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            nudge: Notify::new(),
            mark: watch::Sender::new(0),
            dirty: Cell::new(false),
            passes_started: Cell::new(0),
            passes_done: watch::Sender::new(0),
            poisoned: RefCell::default(),
            outbox: RefCell::default(),
            breaker: RefCell::default(),
            ledgers: RefCell::default(),
            catalog: RefCell::default(),
        })
    }

    /// Take `queue`'s row book for a drain turn ([`RowLedger`]). A book with no row waiting
    /// for a reason to change saves nothing: an empty book, read from 0.
    pub(crate) fn take_ledger(&self, queue: PoisonQueue) -> RowLedger {
        let l = self.ledgers.borrow_mut()[queue as usize]
            .take()
            .unwrap_or_default();
        if l.has_parked() {
            l
        } else {
            RowLedger::default()
        }
    }

    /// Give the book back after a drain turn **without errors**.
    pub(crate) fn keep_ledger(&self, queue: PoisonQueue, l: RowLedger) {
        self.ledgers.borrow_mut()[queue as usize] = Some(l);
    }

    /// The drainer starts a turn with deployment registry `catalog`: if it differs from the
    /// previous turn's copy the generation goes up — rows waiting for the registry
    /// ([`Park::Catalog`]) are reread.
    pub(crate) fn observe_catalog(&self, catalog: &std::sync::Arc<crate::registry::Catalog>) {
        let mut c = self.catalog.borrow_mut();
        if !c
            .0
            .as_ref()
            .is_some_and(|seen| std::sync::Arc::ptr_eq(seen, catalog))
        {
            c.0 = Some(catalog.clone());
            c.1 += 1;
        }
    }

    /// The deployment registry generation of the current drain turn ([`Self::observe_catalog`]).
    pub(crate) fn catalog_gen(&self) -> u64 {
        self.catalog.borrow().1
    }

    /// **Per-target circuit breaker**: range `dest` has had a send hit `SEND_TIMEOUT` under
    /// range table version `version`, and no send to it has been acked since.
    ///
    /// A turn's `down` book only keeps targets that failed **within** the turn: the next
    /// turn would pay a full `SEND_TIMEOUT` again for the same hung target, and the drainer —
    /// one for every queue of the range — would stand still with it. A target in the circuit
    /// breaker only gets **one** probe batch per turn, with a `PROBE_TIMEOUT` cap; an ack
    /// removes it. Keyed by range id **and** table version (like `down`, keyed by the routed
    /// target): once the table changes the old entry means nothing, and the next send goes
    /// out with the full cap. The book belongs to the term — a leader change means a new
    /// book.
    pub(crate) fn tripped(&self, dest: cairn_store::RangeId, version: u64) -> bool {
        self.breaker.borrow().get(&dest) == Some(&version)
    }

    /// A send to `dest` has just hit its cap under table `version`.
    pub(crate) fn trip(&self, dest: cairn_store::RangeId, version: u64) {
        let mut b = self.breaker.borrow_mut();
        // Entries of an old table mean nothing: clean them up on write, the book does not
        // swell.
        b.retain(|_, v| *v == version);
        if b.insert(dest, version).is_none() {
            tracing::debug!(dest = dest.0, version, "destination breaker tripped");
        }
    }

    /// A send to `dest` has just been fully acked.
    pub(crate) fn untrip(&self, dest: cairn_store::RangeId) {
        if self.breaker.borrow_mut().remove(&dest).is_some() {
            tracing::debug!(dest = dest.0, "destination breaker cleared");
        }
    }

    /// Take the term's outbox row book for a drain turn ([`Backlog`]). Not there yet (first
    /// turn, or the previous turn failed) ⇒ an empty book: scan from 0.
    pub(crate) fn take_backlog(&self) -> Backlog {
        self.outbox.borrow_mut().take().unwrap_or_default()
    }

    /// Give the book back after a drain turn **without errors**.
    pub(crate) fn keep_backlog(&self, b: Backlog) {
        *self.outbox.borrow_mut() = Some(b);
    }

    /// Record a read that hit corrupt row `(queue, seq)`.
    pub(crate) fn note_poison(&self, queue: PoisonQueue, seq: u64) -> PoisonSight {
        let mut seen = self.poisoned.borrow_mut();
        match seen.get(&(queue, seq)) {
            Some(true) => PoisonSight::Settled,
            Some(false) => PoisonSight::Pending,
            None if seen.len() >= POISON_CAP => PoisonSight::Settled,
            None => {
                seen.insert((queue, seq), false);
                PoisonSight::New
            }
        }
    }

    /// The work for corrupt row `(queue, seq)` is done (the waiter has an outcome, or there
    /// is nobody to tell): later turns just go past it.
    pub(crate) fn settle_poison(&self, queue: PoisonQueue, seq: u64) {
        if let Some(done) = self.poisoned.borrow_mut().get_mut(&(queue, seq)) {
            *done = true;
        }
    }

    /// A drain turn starts. Only the drainer calls this.
    pub(crate) fn begin_pass(&self) {
        self.passes_started.set(self.passes_started.get() + 1);
    }

    /// The drain turn that just started has gone through every queue. Only the drainer calls
    /// this.
    pub(crate) fn end_pass(&self) {
        let n = self.passes_started.get();
        self.passes_done.send_if_modified(|cur| {
            if n > *cur {
                *cur = n;
                true
            } else {
                false
            }
        });
    }

    /// Nudge, then wait for **a complete drain turn that started after the call** to finish.
    /// Releases the engine while waiting; bounded like [`Self::wait_for`].
    ///
    /// The target is `started + 1`: if a turn is running it may already have gone past the
    /// position of the row we just produced, so we must wait for the next turn — the `dirty`
    /// flag guarantees there is one.
    pub async fn wait_full_pass(&self, engine: &mut Lease) -> bool {
        let target = self.passes_started.get() + 1;
        self.nudge();
        let mut rx = self.passes_done.subscribe();
        let reached = async move {
            loop {
                if *rx.borrow_and_update() >= target {
                    return true;
                }
                if rx.changed().await.is_err() {
                    return false;
                }
            }
        };
        engine
            .wait(tokio::time::timeout(WAIT_CEILING, reached))
            .await
            .unwrap_or(false)
    }

    /// "There are new rows" — returns right away, waits for nothing.
    ///
    /// This is what replaces `drain_outbox(..)` at every opportunistic call site. The
    /// `/invoke` path carries on without touching anyone else's SDK turn.
    pub fn nudge(&self) {
        self.dirty.set(true);
        self.nudge.notify_one();
    }

    /// Nudge, then wait for the mark to pass `seq`. **Releases the engine while waiting.**
    ///
    /// Releasing is mandatory, not an optimisation: the drainer needs that very engine to do
    /// the work we are waiting for. Holding it while waiting is holding a lock while waiting
    /// for someone who needs the lock — exactly rule 1 of [`crate::lease`], and here it is a
    /// matter of life and death rather than mere politeness.
    ///
    /// `false` = the cap was hit. The caller must reread the durable state, it must not
    /// assume the drain has finished.
    pub async fn wait_for(&self, engine: &mut Lease, seq: u64) -> bool {
        self.nudge();
        match engine
            .wait(tokio::time::timeout(WAIT_CEILING, self.reached(seq)))
            .await
        {
            Ok(done) => done,
            Err(_) => {
                tracing::warn!(
                    waiting_for = seq,
                    drained_to = *self.mark.borrow(),
                    "drain mark wait hit its cap — a stuck row?"
                );
                false
            }
        }
    }

    /// Wait for the mark to pass `seq`. Does not touch the engine, has no cap.
    ///
    /// Split from [`Self::wait_for`] so the *hard* part — the comparison — can be tested
    /// without building a whole partition. The previous attempt showed this is exactly the
    /// part worth testing.
    async fn reached(&self, seq: u64) -> bool {
        let mut rx = self.mark.subscribe();
        // **Read the mark, mark it as seen, THEN wait for a change.** That order is what
        // closes the race window: if the mark passes `seq` right between the comparison and
        // the wait, `changed()` returns right away because the version has moved past the
        // mark we just set. There is no wake-up to miss.
        //
        // This is where it differs completely from the previous attempt: there the waiter
        // registered for **an event** ("a batch was just released"), and between observing
        // and registering there was always a gap. Here we compare a number we hold with a
        // published number; that gap does not exist because the mark is not a moment, it is
        // a state.
        loop {
            if *rx.borrow_and_update() >= seq {
                return true;
            }
            // The sender vanished = the partition is stepping down. Not "drained", so it goes
            // down the same path as hitting the cap.
            if rx.changed().await.is_err() {
                return false;
            }
        }
    }

    /// The current mark. Read only, no waiting.
    pub fn mark(&self) -> u64 {
        *self.mark.borrow()
    }

    /// Publish "drained up to `seq`". Only moves forward, never back.
    ///
    /// If it could move back it would no longer be a mark: a waiter that has seen
    /// `m >= seq` and moved on has no way of being called back.
    pub(crate) fn publish(&self, seq: u64) {
        self.mark.send_if_modified(|cur| {
            if seq > *cur {
                *cur = seq;
                true
            } else {
                false
            }
        });
    }

    /// Wait until there is work. Returns when there are new rows (or there already were).
    pub(crate) async fn wait_for_work(&self) {
        if self.dirty.replace(false) {
            return;
        }
        self.nudge.notified().await;
        self.dirty.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mark passed **before** anyone waited ⇒ the waiter goes on right away.
    ///
    /// This is the case the wait-for-an-event approach gets wrong, and it is the most common
    /// case: the drainer is faster than the caller. Registering for "a batch was just
    /// released" here means registering for a notification already fired — hanging forever.
    #[tokio::test]
    async fn mark_already_passed_does_not_wait() {
        let d = Drainer::new();
        d.publish(7);
        assert!(d.reached(3).await);
        assert!(d.reached(7).await, "the comparison must be ≥, not >");
    }

    /// The mark passes **while** waiting ⇒ the waiter wakes.
    #[tokio::test]
    async fn mark_passed_while_waiting_wakes() {
        let d = Drainer::new();
        let waiter = Rc::clone(&d);
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async move {
                let h = tokio::task::spawn_local(async move { waiter.reached(5).await });
                // Yield so the waiter gets to register, only then publish. This is the shape
                // we *want* to be right; the harder case (publishing before registering) is
                // covered by the test above.
                tokio::task::yield_now().await;
                d.publish(5);
                assert!(h.await.unwrap());
            })
            .await;
    }

    /// The mark **does not move back**. If it could, it would no longer be a mark: a waiter
    /// that has seen enough and moved on has no way of being called back.
    #[tokio::test]
    async fn mark_only_moves_forward() {
        let d = Drainer::new();
        d.publish(9);
        d.publish(4);
        assert_eq!(*d.mark.borrow(), 9);
        assert!(d.reached(9).await);
    }

    fn nth_id(i: u32) -> InvocationId {
        let mut b = InvocationId::default();
        b[..4].copy_from_slice(&i.to_be_bytes());
        b
    }

    /// The outbox row book: `due` returns every row to reread and the rows of lock holders
    /// that have released, by `seq`, asking `free` once per lock holder; rows of lock holders
    /// still busy do not come out.
    #[test]
    fn row_ledger_returns_rows_to_recheck_by_seq() {
        let (a, b) = (nth_id(1), nth_id(2));
        let mut s = Backlog::default();
        s.note(5, Some(a));
        s.note(3, None);
        s.note(7, Some(b));
        s.note(9, Some(a));
        s.note(4, None);
        assert!(s.has_parked());
        let mut asked = Vec::new();
        let due = s.due(|id| {
            asked.push(id);
            id == a
        });
        asked.sort();
        assert_eq!(asked, vec![a, b], "each lock holder asked exactly once");
        assert_eq!(due, vec![(3, None), (4, None), (5, Some(a)), (9, Some(a))]);
        let due = s.due(|_| true);
        assert_eq!(
            due.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
            vec![3, 4, 5, 7, 9]
        );
    }

    /// Rewriting a row moves it between the two indexes; a row only leaves the book through
    /// `forget`; once no row waits for a lock, `has_parked` turns off.
    #[test]
    fn row_ledger_records_and_forgets_keeping_index_consistent() {
        let a = nth_id(1);
        let mut s = Backlog::default();
        s.note(5, Some(a));
        s.note(5, None);
        assert!(!s.has_parked(), "the row has moved to rereading");
        assert_eq!(s.due(|_| false), vec![(5, None)]);
        s.note(5, Some(a));
        assert_eq!(s.due(|_| false), vec![]);
        assert!(s.contains(5));
        s.forget(5);
        assert!(!s.contains(5) && !s.has_parked());
        assert_eq!(s.due(|_| true), vec![]);
        s.scanned_to(10);
        s.scanned_to(10);
        assert_eq!(s.scanned(), 10);
    }

    /// The inbound/signal book: each reason has its own reread condition — `Again`/`Shed`
    /// always go into the list (the drainer itself waves aside throttled rows once the turn
    /// has deferred), `Lock` when the lock holder may have released (asked once per id),
    /// `Catalog` when the deployment registry generation has changed; the list is by `seq`.
    #[test]
    fn inbound_ledger_reloads_by_reason() {
        let (a, b) = (nth_id(1), nth_id(2));
        let mut s = RowLedger::default();
        assert!(!s.has_parked());
        s.note(1, Park::Again);
        assert!(
            !s.has_parked(),
            "with only reread rows the book saves nothing"
        );
        s.note(2, Park::Lock(a));
        s.note(3, Park::Shed(b));
        s.note(4, Park::Catalog(7));
        s.note(5, Park::Lock(a));
        assert!(s.has_parked());
        let mut asked = Vec::new();
        let due = s.due(7, |id| {
            asked.push(id);
            false
        });
        assert_eq!(asked, vec![a], "the lock asked once per id");
        assert_eq!(due, vec![(1, Park::Again), (3, Park::Shed(b))]);
        let due = s.due(8, |_| true);
        assert_eq!(
            due.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        s.forget(2);
        assert!(!s.contains(2) && s.contains(5));
        s.scanned_to(6);
        assert_eq!(s.scanned(), 6);
    }

    /// The safety net is bounded: the id book does not grow past [`RESCUE_CAP`] — on
    /// overflow it switches to a full scan; `take` takes every due mark; the jitter lies in
    /// `[d/2, d]`.
    #[tokio::test(start_paused = true)]
    async fn rescue_net_is_capped_and_take_drains_due_marks() {
        let r = Rescue::default();
        let limit = InfraStuck::default();
        assert!(!r.is_dirty());
        for i in 0..RESCUE_CAP as u32 + 5 {
            r.strike(nth_id(i));
        }
        assert!(r.is_dirty());
        tokio::time::advance(RESCUE_FIRST_BACKOFF).await;
        let (ids, full) = r.take(Instant::now(), limit);
        assert_eq!(ids.len(), RESCUE_CAP, "the book does not exceed its cap");
        assert!(full, "an overflowing book means a full scan");
        assert!(!r.is_dirty(), "take takes every due mark");
        r.mark_all();
        tokio::time::advance(RESCUE_FIRST_BACKOFF).await;
        let (ids, full) = r.take(Instant::now(), limit);
        assert!(ids.is_empty() && full);
        for _ in 0..100 {
            let d = jittered(Duration::from_millis(500));
            assert!(
                d >= Duration::from_millis(250) && d <= Duration::from_millis(500),
                "{d:?}"
            );
        }
    }

    /// **Backoff per id**: the `n`-th consecutive mark of the same id waits twice as long as
    /// the previous one (up to the cap), other ids are not dragged along, and a finished turn
    /// (`clear`) puts the streak back to the start. Past the threshold ⇒ `is_stuck`.
    #[tokio::test(start_paused = true)]
    async fn rescue_net_backs_off_per_id_and_knows_when_stuck() {
        let r = Rescue::default();
        let limit = InfraStuck {
            strikes: 4,
            after: Duration::from_secs(3600),
        };
        let (bad, good) = (nth_id(1), nth_id(2));
        for n in 1..=4u32 {
            r.strike(bad);
            let due = r
                .strikes(&bad)
                .and_then(|s| s.due)
                .expect("waiting for reconciliation");
            let wait = due - Instant::now();
            let full = backoff_of(n);
            assert!(
                wait >= full / 2 && wait <= full,
                "mark {n}: {wait:?} outside [{:?}, {full:?}]",
                full / 2
            );
            tokio::time::advance(wait).await;
            let (ids, _) = r.take(Instant::now(), limit);
            assert_eq!(ids.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![bad]);
            assert_eq!(ids[0].1.is_stuck(limit, Instant::now()), n >= 4, "mark {n}");
        }
        assert_eq!(backoff_of(1), RESCUE_FIRST_BACKOFF);
        assert_eq!(backoff_of(30), RESCUE_MAX_BACKOFF);
        // Another id starts at the first step, not at `bad`'s step.
        r.strike(good);
        let wait = r.strikes(&good).and_then(|s| s.due).expect("waiting") - Instant::now();
        assert!(wait <= RESCUE_FIRST_BACKOFF, "{wait:?}");
        // A turn finished ⇒ the streak is broken.
        r.clear(&bad);
        r.strike(bad);
        assert_eq!(r.strikes(&bad).map(|s| s.count), Some(1));
        // The time threshold.
        tokio::time::advance(Duration::from_secs(3600)).await;
        let st = r.strikes(&bad).expect("still there");
        assert!(
            st.is_stuck(limit, Instant::now()),
            "a streak lasting longer than `after` is stuck"
        );
    }

    /// A handed-over turn is counted until the task running it finishes — including two
    /// overlapping turns of the same invocation.
    #[test]
    fn runner_counts_delivered_turns_until_done() {
        let (runner, mut rx) = Runner::new();
        let id = InvocationId::default();
        runner.queue(id);
        runner.queue(id);
        assert!(runner.is_queued(&id));
        let rescue = Rescue::new();
        let mut a = runner.running(&rx.try_recv().unwrap(), &rescue);
        let b = runner.running(&rx.try_recv().unwrap(), &rescue);
        a.duty().ack();
        drop(a);
        assert!(runner.is_queued(&id), "one turn not finished yet");
        assert!(rescue.strikes(&id).is_none(), "an acked turn does not mark");
        drop(b);
        assert!(!runner.is_queued(&id));
        assert_eq!(
            rescue.strikes(&id).map(|s| s.count),
            Some(1),
            "a turn dropped without an ack ⇒ marks the safety net (nack by default)"
        );
        drop(rx);
        runner.queue(id);
        assert!(
            !runner.is_queued(&id),
            "a failed hand-over (channel closed) must not be counted"
        );
        assert_eq!(
            rescue.strikes(&id).map(|s| s.count),
            Some(1),
            "a failed hand-over is not a turn — no mark"
        );
    }

    /// **Nack by default**: the wake-up duty marks the safety net on every drop path without
    /// an ack — including while unwinding a panic, and even when the net's book is already
    /// borrowed (no second panic) —, nothing once acked, and nothing after the term has
    /// ended.
    #[test]
    fn wake_duty_nacks_by_default() {
        let rescue = Rescue::new();
        let id = |b: u8| {
            let mut id = InvocationId::default();
            id[0] = b;
            id
        };
        // ack ⇒ no mark
        let mut d = WakeDuty::new(Rc::clone(&rescue), id(1));
        d.ack();
        drop(d);
        assert!(rescue.strikes(&id(1)).is_none());
        // ordinary drop ⇒ mark
        drop(WakeDuty::new(Rc::clone(&rescue), id(2)));
        assert_eq!(rescue.strikes(&id(2)).map(|s| s.count), Some(1));
        // unwind ⇒ mark, the original panic carries on
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _d = WakeDuty::new(Rc::clone(&rescue), id(3));
            panic!("turn panic");
        }));
        assert!(r.is_err());
        assert_eq!(rescue.strikes(&id(3)).map(|s| s.count), Some(1));
        // book already borrowed ⇒ full scan, no panic
        {
            let _held = rescue.ids.borrow_mut();
            drop(WakeDuty::new(Rc::clone(&rescue), id(4)));
        }
        assert!(rescue.strikes(&id(4)).is_none());
        assert!(rescue.full.get().is_some(), "switched to a full scan");
        // term ended ⇒ dropped
        rescue.close();
        drop(WakeDuty::new(Rc::clone(&rescue), id(5)));
        rescue.mark(id(6));
        assert!(rescue.strikes(&id(5)).is_none());
        assert!(rescue.strikes(&id(6)).is_none());
    }

    /// A nudge while the drainer is busy **must not be lost**.
    ///
    /// `notify_one` keeps one permit, but the running turn may already have scanned past the
    /// new row's position — so that permit is not enough. The `dirty` flag turns "may have
    /// missed it" into "certainly scan one more turn".
    #[tokio::test]
    async fn poke_while_busy_still_rescans() {
        let d = Drainer::new();
        d.nudge();
        // The first turn takes the work.
        d.wait_for_work().await;
        // Rows arrive while that turn runs.
        d.nudge();
        // The second turn must start right away, without waiting for anyone to wake it.
        tokio::time::timeout(Duration::from_millis(200), d.wait_for_work())
            .await
            .expect("a nudge while busy was lost — the row waits for the next tick");
    }
}
