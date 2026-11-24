// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The term's background work: running handed-over turns, draining, ticks, flow recovery,
//! rule polling, recover, the safety net, the background build of query indexes.

use super::*;

/// Run an invocation the drain loop just emitted (the drain loop itself runs no SDK
/// turns).
///
/// There is nobody to return an error to — the caller was answered long ago.
///
/// **On failure it must leave a reschedule.** The outbox row that produced this turn was
/// cut in the same commit that delivered the id to the caller, so there is nothing left to
/// replay: if the first turn failed and we only logged it, the invocation would sit still in
/// `Inbox` until the next leader change (`recover` is the only place that picks it up
/// again). A reschedule puts it on the `fire_retries` path, which runs on every background
/// tick.
///
/// This hole existed before — the old `drive_now` path inside the drain loop dropped it in
/// exactly the same way — but moving turns out of the drain loop makes it easier to hit, so
/// it is patched here.
///
/// `done` carries the turn's wake-up duty (nack by default): if the turn panics, is
/// cancelled, or returns an error without leaving a reschedule, it falls unacked and marks
/// the safety net for `req.id`.
///
/// **Intake gate**: the task passes the gate (`admit`), then asks for a slot; not granted
/// right away, it parks a ticket in the scheduler's line and ends here — the turn runs
/// later in [`run_granted`].
pub(super) async fn run_dispatched(
    sh: &Shared,
    req: crate::drainer::RunRequest,
    done: crate::drainer::Dispatched,
    admit: crate::drainer::Admit,
) {
    cairn_store::probe::mark("runner.run", 0);
    let id = req.id;
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let ctx = sh.ctx(&deployments);
    let r = turn::drive_dispatched(&mut engine, &ctx, req, done, admit).await;
    after_turn(sh, &id, r.map(|_| ()));
}

/// The turn of a parked ticket just granted a slot (`flow::Ticket`).
///
/// **Win the invocation lock synchronously, on the first poll** — before any `await`: the
/// task never holds a slot while waiting for the invocation lock (the order "invocation lock
/// first, slot second" of every other path; the reverse is two tasks waiting on each
/// other). Busy (the drainer, a cancel, a turn with a caller holds it) ⇒ nack: the slot is
/// returned, the ticket waits for the lock to be released and then asks again. A ticket
/// withdrawn while waiting to be spawned (another turn took over) ⇒ do nothing.
pub(super) async fn run_granted(sh: &Shared, g: crate::flow::Granted) {
    let id = g.id();
    let Some(guard) = sh.locks.try_invocation(id) else {
        sh.scheduler.nack(g);
        return;
    };
    let Some(mut claimed) = sh.scheduler.claim(g) else {
        return;
    };
    cairn_store::probe::mark("runner.granted", 0);
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    let ctx = sh.ctx(&deployments);
    let (pending, turn) = turn::dispatched_wake(&engine, &claimed.run);
    let r = turn::drive_granted(
        &mut engine,
        &ctx,
        id,
        claimed.permit,
        pending,
        turn,
        claimed.done.duty(),
        claimed.waited,
    )
    .await;
    drop(guard);
    after_turn(sh, &id, r.map(|_| ()));
}

/// After a background turn (run or parked): nudge the drainer, report lost write
/// authority, log errors.
fn after_turn(sh: &Shared, id: &InvocationId, r: Result<()>) {
    // The turn may have produced outbox/signal rows — nudge the drainer instead of waiting
    // for the background tick.
    sh.drainer.nudge();
    let Err(e) = r else {
        return;
    };
    if e.is_fenced() {
        sh.fenced.notify_one();
        return;
    }
    // No rescheduling here: `drive` has already rescheduled on every one of its error
    // paths — scheduling again would mean two rows for one invocation, two turns running
    // later, and a backoff that never grows.
    tracing::warn!(invocation = %cairn_store::id_hex(id), error = %e, "scheduler-run turn failed, rescheduled");
}

/// The outbox drainer's loop. Runs until the partition steps down.
///
/// Its shape is a "wait for work → do it → publish the mark" loop, and all three parts are
/// cheap when there is no work: the wait is a `Notify`, the first scan is one seek into the
/// index, and the mark only changes when it advances.
pub(super) async fn drain_loop(sh: Shared) {
    // The old leader's rows may be waiting right at becoming leader — do not wait for
    // anyone to nudge.
    sh.drainer.nudge();
    loop {
        sh.drainer.wait_for_work().await;
        cairn_store::probe::mark("drainer.wake", 0);
        let mut engine = Lease::acquire(&sh.engine).await;
        let deployments = sh.registry.snapshot();
        let ctx = sh.ctx(&deployments);
        sh.drainer.begin_pass();
        // The deployment registry changed ⇒ inbound rows waiting for the registry are reread.
        sh.drainer.observe_catalog(&deployments);
        sh.l0.begin_pass();
        // **One reader for every queue of the partition**: inbound, signal, outbox. There is
        // no drain lock — what keeps two drain turns from picking up the same row is that
        // there is only one reader.
        //
        // **Inbound first**: the outbox and signal drain turns end by waiting for every
        // target still in flight (`Sender::finish`), up to `SEND_TIMEOUT` for a hung
        // target. With inbound after them, messages for **this very** range — completions,
        // `StartCall` — would wait on an unrelated target; going first, they only wait for
        // the next turn. A hung target goes into the drainer's circuit breaker, so the next
        // turn only pays `PROBE_TIMEOUT` for it.
        cairn_store::probe::mark("drainer.inbound", 0);
        //
        // **In chunks**: each drainer releases the engine between two of its chunks (row cap
        // or the per-hold budget, `turn::DRAIN_HOLD_BUDGET`), and between two queues once the
        // hold has exceeded its budget — one hold of `drain` does not grow with the backlog.
        // Turns, commands and background ticks slip into the release points; a release point
        // lies between two rows (M4).
        let inbound = turn::drain_inbound(&mut engine, &ctx).await.map(|_| ());
        turn::release_if_over_budget(&mut engine).await;
        let signals = turn::drain_signals(&mut engine, &ctx).await.map(|_| ());
        turn::release_if_over_budget(&mut engine).await;
        let outbox = turn::drain_outbox(&mut engine, &ctx).await;
        cairn_store::probe::mark("drainer.done", 0);
        // Release the engine **before** publishing: the waiters waking up will go to win it
        // right away, and holding it one more tick just to fire a number would make them
        // queue for nothing.
        drop(engine);

        let mut fenced = false;
        match outbox {
            Ok(mark) => sh.drainer.publish(mark),
            // There is nobody to return the error to. The rows stay intact in the table so
            // nothing is lost — only slower, and the background tick will nudge again. The
            // mark does **not** advance: a turn that failed mid-way proves nothing about the
            // rows it did not reach.
            Err(e) if e.is_fenced() => fenced = true,
            Err(e) => tracing::warn!(error = %e, "outbox drain turn failed, retrying next tick"),
        }
        for (what, r) in [("signal", signals), ("inbound", inbound)] {
            match r {
                Ok(()) => {}
                Err(e) if e.is_fenced() => fenced = true,
                Err(e) => tracing::warn!(error = %e, row = what, "drain turn failed, retrying"),
            }
        }
        // The turn is over — even if it failed: waiters for a complete turn must be let go
        // (and reread the durable state), not hang on an error.
        sh.drainer.end_pass();
        if fenced {
            sh.fenced.notify_one();
            return;
        }
    }
}

/// When the serve loop must wake to fire the background tick: the next tick, or earlier if
/// the reschedule table has an earlier deadline. A pure function of `now`.
///
/// The deadline is wall time (ms) — the same clock as the background tick's `now_ms` —
/// converted to the monotonic clock with [`Now::mono_at`](cairn_store::clock::Now::mono_at):
/// both numbers come from **one** read of `now`, rounded up to the next millisecond. A
/// deadline so far out it overflows `Instant` (the SDK sets `wake_up_time` freely) ⇒ only
/// the tick — overflowing here would be a panic in the serve loop.
pub(super) fn wake_at(
    next_tick: tokio::time::Instant,
    due_ms: Option<u64>,
    now: cairn_store::clock::Now,
) -> tokio::time::Instant {
    match due_ms.and_then(|d| now.mono_at(d)) {
        Some(at) => next_tick.min(at),
        None => next_tick,
    }
}

/// Fire one background tick in its own task, with a flag against overlapping ticks.
pub(super) fn spawn_background_tick(
    tasks: &mut JoinSet<()>,
    sh: &Shared,
    ticking: &Rc<Cell<bool>>,
) {
    if sh.registry.is_empty() {
        return;
    }
    // Lower the flag when the task ends by **any** path — return, panic, or cancellation.
    // Lowering it by hand at the end of the task would leave the flag stuck at `true`
    // forever after a panic in the tick: the ticker's guard switches off for good and the
    // timers/retries of the whole partition stop silently until the leader changes.
    struct ClearOnDrop(Rc<Cell<bool>>);
    impl Drop for ClearOnDrop {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }
    ticking.set(true);
    let sh = sh.clone();
    let clear = ClearOnDrop(ticking.clone());
    tasks.spawn_local(async move {
        let _clear = clear;
        crate::lease::holding_as("tick", background_tick(&sh)).await;
    });
}

/// **Recovering flow control on becoming leader: rebuilding the VO locks, in chunks.**
///
/// The RAM counters go back to 0 on a leader change (`Scheduler::reset`); what must be
/// rebuilt is the **VO locks**, because locks must never be lost. Each chunk is **one**
/// engine hold:
///
/// 1. Read at most `restore_chunk` rows of the `Invocations` index after the cursor, and
///    the queue entries of every unfinished invocation among them
///    ([`cairn_engine::Engine::restore_chunk`]).
/// 2. Lifecycle core `Event::LeaderUp` for each entry: an entry in `Running` ⇒ the old
///    leader died mid-turn — push it out (to `Inbox`, or `Suspended` if the queue holds the
///    VO lock), no counter added; an entry in a lock-holding stage of a live invocation ⇒
///    rebuild **exactly that lock, and only the lock** (a turn that died mid-way may already
///    have committed a state read); an entry holding the lock of an invocation that has
///    stopped for good ⇒ remove it (an orphan, not a lock).
/// 3. Every stage change of the chunk in **one** commit (`restore_stages`), then rebuild
///    the RAM locks — still within the same hold: a command slipping in cannot end an
///    invocation between its classification and the rebuild of its lock (M4
///    `restore_lock_after_release`).
///
/// Release the engine between two chunks (like `recover`). Meanwhile the range **serves
/// commands** — reads, creates, cancels, the drainer, background ticks, `recover` — but
/// **grants no slots**: the scheduler's gate stays closed until the last chunk
/// ([`crate::flow::Scheduler::open_gate`], FL-8). Correct because nothing puts an entry
/// **into** a lock-holding stage without going through a slot grant: every entry holding a
/// lock during the rebuild belongs to the previous term, the rebuild meets it in its state
/// as of when its chunk passed, and commands slipping in only change it in the releasing
/// direction (cancelling or finishing removes the entry; releasing a lock not yet rebuilt
/// does nothing, releasing a rebuilt lock works as usual). Invocations created during the
/// rebuild are in `Inbox` — holding no lock; the queue `seq` counter has been primed from
/// the durable mark (`floors::QUEUE_HIGH`), otherwise the shell runs the whole rebuild
/// **before** the first command ([`super::leadership`]).
///
/// There is no second rebuild within a term: the task lives as long as the term
/// (`JoinSet::shutdown` on stepping down), and the next term has its own `Shared`,
/// scheduler and engine — the old term's half-done rebuild cannot touch anything of the
/// new term.
///
/// Invariant on leaving this function: **no counter is non-zero**, and the gate is open.
pub(super) async fn restore_flow(sh: &Shared, started: tokio::time::Instant) -> Result<()> {
    hold_restore().await;
    let mut engine = Lease::acquire(&sh.engine).await;
    // The engine is held — the point only `return`s the error, it does not stop
    // (AGENTS.md §5).
    cairn_store::failpoint!("actor::restore_flow", err);
    let (mut rows_seen, mut requeued, mut stale, mut locks) = (0usize, 0usize, 0usize, 0usize);
    let mut max_seq = 0u64;
    let mut after: Option<InvocationId> = None;
    loop {
        let (rows, next) = engine
            .restore_chunk(after.as_ref(), sh.restore_chunk)
            .await?;
        rows_seen += rows.len();
        let mut writes = Vec::new();
        let mut restore = Vec::new();
        for r in &rows {
            max_seq = max_seq.max(r.slot.seq);
            let plan = turn::leader_up(&mut engine, r).await?;
            let write = match plan.stage {
                lc::StageChange::Keep | lc::StageChange::Insert(_) => None,
                lc::StageChange::Move(to) => {
                    if to == lc::Stage::Inbox {
                        requeued += 1;
                    }
                    Some(cairn_engine::StageWrite::Move(turn::stage_to(to)))
                }
                lc::StageChange::Remove => {
                    stale += 1;
                    Some(cairn_engine::StageWrite::Remove)
                }
            };
            if let Some(w) = write {
                writes.push((r.clone(), w));
            }
            if plan.ram.vo == lc::Vo::Restore {
                let lock = r
                    .slot
                    .name
                    .lock_name()
                    .expect("the core builds VO locks only for EXCLUSIVE queues");
                restore.push((lock, r.id));
            }
        }
        turn::restore_stages(&mut engine, &writes).await?;
        // The VO locks are rebuilt **after** the chunk's commit (the Plan's RAM work), within
        // the same hold, and only the locks: no counters.
        locks += restore.len();
        for (lock, id) in restore {
            sh.scheduler.restore_lock(lock, id);
        }
        let Some(cursor) = next else { break };
        after = Some(cursor);
        // Release the engine between two chunks: commands, turns (not yet granted a slot),
        // the drainer go first.
        engine
            .wait(async {
                tokio::task::yield_now().await;
                hold_restore().await;
            })
            .await;
    }
    engine.finish_restore(max_seq);
    if stale > 0 {
        tracing::warn!(
            entry = stale,
            "removed orphaned queue entry of a finished invocation"
        );
    }
    debug_assert!(
        sh.scheduler.usage_is_empty(),
        "recovery: no counter may be non-zero before the grant gate opens"
    );
    // A once-per-term event, not on the turn path.
    if requeued > 0 || locks > 0 {
        tracing::info!(
            entry = rows_seen,
            requeued = requeued,
            locks_rebuilt = locks,
            "recovered the flow control queue"
        );
    }
    sh.stats.update(engine.range().id.0, |t| {
        t.restore_ms = Some(crate::range_stats::ms(started.elapsed()));
    });
    // No `recover` will run (the deployment registry is empty): opening ends here.
    if sh.registry.is_empty() {
        engine.end_opening();
    }
    // Open the gate **after** releasing the engine: `ungate` is O(tickets waiting at the
    // gate) of RAM work, it needs no engine.
    drop(engine);
    sh.scheduler.open_gate();
    Ok(())
}

/// Failpoint `actor::restore_hold` enabled (`return`) ⇒ the lock rebuild waits before the
/// next chunk, **outside** the engine, yielding the thread — the range keeps serving
/// commands. Does nothing in a build without the `failpoints` feature.
async fn hold_restore() {
    while restore_held() {
        cairn_store::pace::poll_gap(std::time::Duration::from_millis(20)).await;
    }
}

fn restore_held() -> bool {
    cairn_store::failpoint!("actor::restore_hold", |_| true);
    false
}

/// Apply the node's shared rule book if it is newer than the copy the engine has applied.
/// No I/O: the node-level poll loop reads the object store (`node::spawn_rules_poller`),
/// every range only compares round numbers.
///
/// `set_rules` filters by `version` itself, so applying redundantly (a failed read keeps
/// the old copy) is cheap.
pub(super) fn refresh_rules(sh: &Shared) {
    let Some(store) = &sh.rules else {
        return;
    };
    let published = store.published();
    if published.round == sh.rules_seen.get() {
        return;
    }
    sh.rules_seen.set(published.round);
    if let Some(book) = published.book {
        sh.scheduler.set_rules((*book).clone());
    }
}

/// On becoming leader: wait for the node's first read (bounded, the node has just started),
/// then apply the shared copy — do not serve under "no caps" when the rule book is only one
/// GET away. On a node that is already running the first round finished long ago: no wait,
/// no GET (the same for the child ranges of a split).
pub(super) async fn first_rules(sh: &Shared) {
    if let Some(store) = &sh.rules {
        store.first_round(super::serve::RULES_READ_BOUND).await;
    }
    refresh_rules(sh);
}

/// Run on every invocation **that has work to run** after becoming leader.
///
/// An invocation running half-way when the process died has no timer to wake it and no
/// outbox row pointing at it — without a rescan it sits still in `Running` forever, and
/// there is no error to see.
///
/// **Only `running`, not `suspended`** ([`cairn_engine::Engine::runnable_invocations_unbounded`]).
/// A suspended invocation waiting for an event that has not come (a one-hour timer, a call
/// not yet returned, a signal not yet sent) has nothing to do: rerunning it is an SDK turn
/// only to suspend again. The earlier version did exactly that, for every suspended
/// invocation, on every becoming-leader — and split, merge and failover are all
/// becoming-leader. Measured: the first new call to a child range queued behind 5k such
/// turns, waiting 10 s on `file://`, 70 s on SeaweedFS, and the storm dragged on for 15
/// minutes. When the event comes the normal path wakes it: timers and reschedules through
/// the derived index on the background tick, completions/signals through the outbox/inbound
/// drain loop. And a suspended invocation that **has** been loaded with what wakes it while
/// its turn has not run yet is already `running` (`Engine::deliver` changes the state in the
/// same commit that writes the notification) — so it is still covered here.
///
/// **Reconcile, do not rescan**: runs at the start of the term and every time the
/// deployment registry changes — there may be turns going on then. An invocation **someone
/// is working on** (holding the lock, or with a handed-over turn not yet finished) is not
/// handed over again: the same rule as [`rescue_round`]. At the start of a term nobody is
/// doing anything yet, so the rule misses nothing.
///
/// **In chunks**: see [`reconcile_running`]. When a new pass starts (a later
/// `DeploymentsChanged` command) the pass scanning half-way stops at its next chunk — the
/// new pass starts **from where the old one stands** and then wraps around to scan the
/// beginning: rescanning from the start would mean that a registry changing faster than a
/// full scan never gets the tail of the index reconciled.
pub(super) async fn recover(sh: &Shared) {
    // Before winning the engine: stopping here is "became leader without recovering".
    cairn_store::failpoint!("actor::recover");
    let mut engine = Lease::acquire(&sh.engine).await;
    // From here the scan has started: a later `DeploymentsChanged` command needs a new pass
    // (the registry may have changed after this pass's read).
    sh.reconcile_pending.set(false);
    let generation = sh.recover_generation.get().wrapping_add(1);
    sh.recover_generation.set(generation);
    let scanned = reconcile_running(sh, &mut engine, &Default::default(), Some(generation)).await;
    if !matches!(scanned, Err(ScanStop::Superseded)) {
        // Round finished (or failed: the safety net rescans everything): the next pass
        // starts from the beginning.
        sh.recover_at.set(None);
    }
    if !matches!(scanned, Err(ScanStop::Superseded)) {
        // The final whole-table scan of the opening has run — even if it failed: the safety
        // net rescans outside the opening. If the pass was replaced, the new pass ends the
        // opening.
        engine.end_opening();
    }
    match scanned {
        Ok(done) => {
            if done.running > 0 {
                tracing::info!(
                    running = done.running,
                    queued = done.queued,
                    "recovered unfinished invocations (the rest are busy or wait for their retry)"
                );
            }
        }
        Err(ScanStop::Superseded) => {}
        Err(ScanStop::Fenced) => sh.fenced.notify_one(),
        Err(ScanStop::Failed(e)) => {
            // No stepping down as in `restore_flow`: a failed scan breaks nothing running,
            // it only leaves unfinished invocations sitting still. The safety net rescans
            // with backoff — previously nobody rescanned for the rest of the term. Chunks
            // already handed over stay handed over; the rescan skips ids someone is working
            // on.
            tracing::warn!("cannot scan unfinished invocations, the rescue net will rescan: {e}");
            sh.rescue.mark_all();
        }
    }
}

/// Result of one [`reconcile_running`] pass that went through the whole index.
#[derive(Debug, Default)]
pub(super) struct ScanDone {
    /// `running` invocations the scan met.
    pub(super) running: usize,
    /// Of those, how many were handed a turn.
    pub(super) queued: usize,
}

/// Why a [`reconcile_running`] pass stopped half-way.
#[derive(Debug)]
pub(super) enum ScanStop {
    /// Write authority lost.
    Fenced,
    /// A new `recover` pass has started — it rescans from the beginning.
    Superseded,
    /// A read failed (not a loss of write authority).
    Failed(ActorError),
}

/// **Reconcile every `running` invocation of the range, in chunks** — the scan of
/// [`recover`] and the full scan of [`rescue_round`].
///
/// The `Invocations` index includes finished invocations until they are purged (default
/// retention 24 h, `0` keeps them forever): one engine hold across the whole index is
/// O(history), and every turn of the range waits for it. Each chunk reads at most
/// [`cairn_engine::RECOVER_CHUNK`] index rows, asks the lifecycle core `Event::Recover` for
/// each `running` id (skipping the ids in `skip`) and hands over the turn **within that
/// same hold**, then releases the engine before the next chunk.
///
/// Correct because the rule "someone is working on it, so do not hand it over again"
/// (FL-20) already allows turns to slip in between: each id is asked "is anyone working on
/// it" and "does it have a reschedule" (the retry row read within the same hold,
/// [`turn::recover_one`]) right before the hand-over. An id that became `running` in a
/// chunk already passed has its turn handed over by the path that changed it (completion
/// delivery, signal, creation); an id someone was working on when its chunk passed gets a
/// durable wake-up (FL-17) or a safety-net mark from whoever was working on it.
///
/// `generation`: the [`recover`] pass — starts from where the previous pass stands
/// (`Shared::recover_at`, wrapping around to scan the beginning), records where it stands
/// after each chunk, stops at its next chunk once a new pass has started. Cancelled on losing
/// the lease: the term's tasks are `shutdown` on stepping down, and the read after being
/// fenced returns `Fenced`.
pub(super) async fn reconcile_running(
    sh: &Shared,
    engine: &mut Lease,
    skip: &std::collections::BTreeSet<InvocationId>,
    generation: Option<u64>,
) -> std::result::Result<ScanDone, ScanStop> {
    let mut done = ScanDone::default();
    let mut ring = ScanRing::from(generation.and_then(|_| sh.recover_at.get()));
    loop {
        let (ids, next) = match runnable_chunk(engine, ring.after(), sh.recover_chunk).await {
            Ok(x) => x,
            Err(e) if e.is_fenced() => return Err(ScanStop::Fenced),
            Err(e) => return Err(ScanStop::Failed(e)),
        };
        let (ids, finished) = ring.advance(ids, next);
        for id in ids {
            if skip.contains(&id) {
                continue;
            }
            done.running += 1;
            // **Hand over, do not run**: each invocation becomes a task waiting for its own
            // slot, like every other turn.
            //
            // **Except invocations with a pending retry reschedule.** That reschedule is
            // durable, carries the number of attempts so far, and was set *before* the last
            // turn ran — so it is what runs the invocation on, at the right backoff step and
            // with the right `ctx.attempt`.
            let attendance = turn::attendance(&sh.locks, &sh.runner, &id);
            let plan = turn::recover_one(engine, attendance, &id).await;
            if plan.then == lc::Then::Reply(lc::Reply::Abort) {
                return Err(ScanStop::Fenced);
            }
            if plan.ram.handout.is_some() {
                sh.runner.queue(id);
                done.queued += 1;
            }
        }
        if finished {
            return Ok(done);
        }
        if generation.is_some() {
            // Still within the chunk's hold: a new pass starting after the release below sees
            // exactly this position.
            sh.recover_at.set(ring.position());
        }
        // Release the engine between two chunks: waiting turns and commands go first.
        engine.wait(tokio::task::yield_now()).await;
        if generation.is_some_and(|g| g != sh.recover_generation.get()) {
            return Err(ScanStop::Superseded);
        }
    }
}

/// The cursor of one [`reconcile_running`] pass over the `Invocations` index: the next chunk
/// reads after [`Self::after`]; [`Self::advance`] takes the chunk just read (`ids` in key
/// order, `next` = the next chunk's cursor, `None` = end of the index) and returns the ids
/// belonging to this pass together with a "done" flag. [`Self::position`] is where the pass
/// stands.
///
/// **One round**: the pass starts after `start` (where the replaced pass stood), goes to
/// the end of the index, then scans from the beginning up to `start` — every id exactly
/// once. Replaced at every chunk boundary, each pass still advances at least one chunk: the
/// tail of the index is reached within a bounded number of passes.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ScanRing {
    after: Option<InvocationId>,
    start: Option<InvocationId>,
    wrapped: bool,
}

impl ScanRing {
    /// A new pass; `from` is where the replaced pass stands (`None` = from the beginning).
    pub(super) fn from(from: Option<InvocationId>) -> Self {
        Self {
            after: from,
            start: from,
            wrapped: false,
        }
    }

    pub(super) fn after(&self) -> Option<&InvocationId> {
        self.after.as_ref()
    }

    pub(super) fn position(&self) -> Option<InvocationId> {
        self.after
    }

    pub(super) fn advance(
        &mut self,
        ids: Vec<InvocationId>,
        next: Option<InvocationId>,
    ) -> (Vec<InvocationId>, bool) {
        match (self.wrapped, self.start) {
            // The beginning after wrapping around: reaching `start` means done.
            (true, Some(start)) => {
                let past = ids.last().is_some_and(|l| *l >= start) || next.is_none();
                let ids: Vec<InvocationId> = ids.into_iter().filter(|i| *i <= start).collect();
                self.after = next;
                (ids, past)
            }
            // End of the index: wrap around to the beginning if the pass did not start there.
            (false, Some(_)) if next.is_none() => {
                self.wrapped = true;
                self.after = None;
                (ids, false)
            }
            _ => {
                self.after = next;
                (ids, next.is_none())
            }
        }
    }
}

/// **The safety net's loop**: sleep until the earliest reconciliation deadline — one
/// deadline per id, backing off by its own streak of marks — then reconcile what is due.
/// See [`crate::drainer::Rescue`].
pub(super) async fn rescue_loop(sh: Shared) {
    loop {
        let Some(at) = sh.rescue.next_due() else {
            sh.rescue.changed().await;
            continue;
        };
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(at) => {}
            // A new mark may be due earlier: recompute.
            () = sh.rescue.changed() => continue,
        }
        if rescue_round(&sh).await.is_err() {
            // Write authority lost: the term is about to end, the next term's `recover` is the
            // full reconciliation.
            sh.fenced.notify_one();
            return;
        }
    }
}

/// One reconciliation round: take every mark that is due, look at the durable state, hand
/// over again the `running` invocations that nothing will wake. Like [`recover`] — skipping
/// invocations with a retry reschedule —, plus three mid-term rules:
/// - an invocation **someone is working on** (holding the lock, or with a handed-over turn
///   not yet finished) is not handed over again (no duplicate turn); a marked id that is
///   busy is reconciled again after its backoff (the place that marks is usually the very
///   task that is failing, still holding the lock);
/// - failing to read an id's state is one more failure of that same id (one more backoff
///   step);
/// - an id whose streak of marks has passed the [`crate::drainer::InfraStuck`] threshold
///   is moved to `paused` with code [`cairn_engine::INFRA_STUCK`] instead of being handed
///   over again.
///
/// `Err` only when write authority is lost.
pub(super) async fn rescue_round(sh: &Shared) -> std::result::Result<(), ()> {
    let mut engine = Lease::acquire(&sh.engine).await;
    let now = tokio::time::Instant::now();
    let (marked, full) = sh.rescue.take(now, sh.infra_stuck);
    let fenced = |e: &ActorError| e.is_fenced();
    // Ids with a due mark, plus their streak of marks. The full scan (if any) runs **after**
    // them, in chunks, and skips them: a marked id keeps its streak.
    let marked_ids: std::collections::BTreeSet<InvocationId> =
        marked.iter().map(|(id, _)| *id).collect();
    let mut requeued = 0u64;
    for (id, strikes) in marked.into_iter().map(|(id, st)| (id, Some(st))) {
        // Lifecycle core `Event::RescueRound`: no duplicate turn (if someone is working on it,
        // leave it; marked ids are reconsidered later), a read failure is one more failure,
        // past the threshold `paused` 577, a durable reschedule is what runs the invocation on.
        let event = lc::Event::RescueRound {
            marked: strikes.is_some(),
            stuck: strikes.is_some_and(|st| st.is_stuck(sh.infra_stuck, now)),
        };
        let mut view = turn::rescue_view(turn::attendance(&sh.locks, &sh.runner, &id));
        let plan = loop {
            let plan = turn::decide_lifecycle(&view, &event);
            match plan.need {
                None => break plan,
                Some(lc::Need::Status) => {
                    let status = async {
                        // The engine is held — the point only `return`s the error, it does
                        // not stop (FL-16).
                        cairn_store::failpoint!("actor::rescue_round::status", err);
                        engine.status(&id).await.map_err(ActorError::from)
                    };
                    match status.await {
                        Ok(s) => view.status = Some(turn::status_of(s.as_ref())),
                        Err(e) => {
                            if !fenced(&e) {
                                tracing::debug!(invocation = %cairn_store::id_hex(&id), error = %e, "rescue net: cannot read status, backing off one more step");
                            }
                            view.read_error = Some((lc::Need::Status, turn::read_err(&e)));
                        }
                    }
                }
                Some(lc::Need::Retry) => {
                    match engine.retry_row(&id).await.map_err(ActorError::from) {
                        Ok(r) => view.retry = Some(r),
                        Err(e) => {
                            if !fenced(&e) {
                                tracing::debug!(invocation = %cairn_store::id_hex(&id), error = %e, "rescue net: cannot read the retry schedule");
                            }
                            view.read_error = Some((lc::Need::Retry, turn::read_err(&e)));
                        }
                    }
                }
                Some(other) => unreachable!("RescueRound does not read {other:?}"),
            }
        };
        if plan.then == lc::Then::Reply(lc::Reply::Abort) {
            return Err(());
        }
        if plan.status == Some(lc::Status::Paused(lc::Pause::InfraStuck)) {
            let st = strikes.expect("paused 577 ⇒ a strike streak exists");
            match pause_stuck(sh, &mut engine, id, st, now).await {
                Ok(()) => {}
                Err(e) if fenced(&e) => return Err(()),
                Err(e) => {
                    tracing::warn!(invocation = %cairn_store::id_hex(&id), error = %e, "rescue net: could not record paused 577 yet, next round retries");
                    sh.rescue.later(id);
                }
            }
            continue;
        }
        match plan.ram.rescue {
            lc::Mark::Later => sh.rescue.later(id),
            lc::Mark::Strike => sh.rescue.strike(id),
            lc::Mark::Forget => sh.rescue.forget(&id),
            lc::Mark::None | lc::Mark::Clear => {}
        }
        if plan.ram.handout.is_some() {
            sh.runner.queue(id);
            requeued += 1;
        }
    }
    if full {
        match reconcile_running(sh, &mut engine, &marked_ids, None).await {
            Ok(done) => {
                sh.rescue.full_done();
                requeued += done.queued as u64;
            }
            Err(ScanStop::Fenced) => return Err(()),
            Err(ScanStop::Failed(e)) => {
                crate::metrics::flow::bump(&crate::metrics::flow::RESCUE_FAILED);
                tracing::warn!(error = %e, "rescue net: cannot scan unfinished invocations, will rescan");
                sh.rescue.full_failed();
            }
            Err(ScanStop::Superseded) => unreachable!("the rescue scan has no generation"),
        }
    }
    drop(engine);
    if requeued > 0 {
        crate::metrics::flow::add(&crate::metrics::flow::RESCUED, requeued);
        tracing::warn!(
            requeued,
            "rescue net redelivered an invocation with nothing left to wake it"
        );
    }
    Ok(())
}

/// Move an invocation the safety net has handed over again too many times to `paused`
/// with code [`cairn_engine::INFRA_STUCK`]. Holds the invocation lock across the commit: no
/// turn slips in between the "nobody is working on it" check and the write.
pub(super) async fn pause_stuck(
    sh: &Shared,
    engine: &mut Lease,
    id: InvocationId,
    st: crate::drainer::Strikes,
    now: tokio::time::Instant,
) -> Result<()> {
    let (Some(_guard), _) = turn::claim(&sh.locks, &sh.runner, id) else {
        sh.rescue.later(id);
        return Ok(());
    };
    let message = format!(
        "INFRA_STUCK: the rescue net redelivered {} times in a row over {} s and the turn still failed before \
         leaving a durable retry — check the node logs; fix the store or the binary, then resume",
        st.count,
        now.saturating_duration_since(st.since).as_secs()
    );
    turn::pause_infra(engine, &sh.ctx(&sh.registry.snapshot()), id, &message).await?;
    sh.rescue.forget(&id);
    crate::metrics::flow::bump(&crate::metrics::flow::INFRA_STUCK);
    tracing::error!(
        invocation = %cairn_store::id_hex(&id),
        count = st.count,
        "invocation stuck on infrastructure — paused 577"
    );
    Ok(())
}

/// One chunk of the range's `running` invocations — the scan of [`reconcile_running`]: the
/// `running` ids in the chunk and the next chunk's cursor (`None` = end of the index).
pub(super) async fn runnable_chunk(
    engine: &Lease,
    after: Option<&InvocationId>,
    chunk: usize,
) -> Result<(Vec<InvocationId>, Option<InvocationId>)> {
    // The engine is held — the point only `return`s the error, it does not stop.
    cairn_store::failpoint!("actor::recover::scan", err);
    Ok(engine.runnable_chunk(after, chunk.max(1)).await?)
}

/// One tick of the background loop.
///
/// Ordinary errors here **must not** kill the engine thread: a deployment that is
/// temporarily unreachable would stop every timer and every retry for good if we exited.
/// Log, then try again on the next tick.
///
/// Losing write authority is entirely different, and must be split off: retrying a commit
/// that has been fenced is repeating forever something that never succeeds, every 200ms,
/// silently.
pub(super) async fn background_tick(sh: &Shared) {
    // Before the tick, not after: a tick running under the old rule book while the new one
    // is already on the store is exactly the latency the poll produces, but there is no
    // reason to add one more tick to it.
    refresh_rules(sh);
    // The suspended tenant set: unsuspending wakes the waiters even when nobody asks for a
    // new slot.
    sh.scheduler.sync_tenants();
    let now = sh.clock.wall_ms();
    sh.lift.borrow_mut().add(
        sh.scheduler.take_lifted(),
        now,
        sh.scheduler.suspended_retry(),
    );
    let mut engine = Lease::acquire(&sh.engine).await;
    let deployments = sh.registry.snapshot();
    // Unsuspended ⇒ turns rescheduled during the suspension run right away, without waiting
    // for their reschedule tick, one bounded batch per tick. On failure (not a fence) the
    // cursor stands still: the next tick retries that batch.
    if !sh.lift.borrow().is_idle() {
        // Take it out of the cell while waiting: no `borrow` held across `.await` (background
        // ticks do not overlap, so nobody else touches this cell meanwhile).
        let mut sweep = std::mem::take(&mut *sh.lift.borrow_mut());
        match turn::wake_lifted(&mut engine, &sh.ctx(&deployments), now, &mut sweep).await {
            // More rows: wake again right after this tick, like `fire_retries` hitting its cap.
            Ok(true) => sh.next_due.offer(now),
            Ok(false) => {}
            Err(e) if e.is_fenced() => {
                tracing::warn!("background tick hit a fence — this node is no longer leader");
                sh.fenced.notify_one();
                return;
            }
            Err(e) => {
                tracing::warn!("could not fire the parked retries of an unsuspended tenant: {e}")
            }
        }
        *sh.lift.borrow_mut() = sweep;
    }
    let ticked = match turn::tick(&mut engine, &sh.ctx(&deployments), now).await {
        // Export the range's audit rows — only when rows are pending.
        Ok(_) => crate::audit::export(&mut engine).await.map(|_| ()),
        Err(e) => Err(e),
    };
    match ticked {
        Ok(_) => {}
        Err(e) if e.is_fenced() => {
            tracing::warn!("background tick hit a fence — this node is no longer leader");
            sh.fenced.notify_one();
        }
        Err(e) => tracing::warn!("background tick failed, retrying: {e}"),
    }
}

/// **Background build of the query indexes** of the term: call `Engine::query_build_step`
/// until `Idle`. Each step is one chunk ([`cairn_engine::QUERY_BUILD_CHUNK`] rows) lying
/// entirely within one engine hold; between two steps the engine is yielded to live turns
/// (`pace`). On an error that is not a fence, back off and start over (the background build
/// only puts); on a fence, stop — the next term carries on building.
pub(super) async fn query_build_loop(sh: Shared) {
    count_schedules(&sh).await;
    publish_index_states(&sh).await;
    if !build_until_idle(&sh).await {
        return;
    }
    publish_index_states(&sh).await;
    // On-demand counters in parallel with the hint sweep (which waits [`HINT_SWEEP_DELAY`]
    // first): the first asker does not wait for the other loop.
    futures::join!(
        crate::lease::holding_as("hint_sweep", hint_sweep(&sh)),
        counts_when_asked(&sh)
    );
}

/// The range's `cairnflow_schedules` gauge: count the schedule index once per term — after a
/// split/merge the range's number is the child Db's, not the parent's. Background work at
/// the start of the term, not on the path to the first command (scanning an empty index cost
/// 17 ms of cold reads at H = 10⁶ before the serve loop ran). Count and set within **one**
/// hold: a schedule create/delete command commits and then `adjust`s within its own hold, so
/// if the count sees the commit, its `adjust` has already run (and is overwritten by the
/// correct number), and if not, the `adjust` runs afterwards. A read error only leaves the
/// gauge short.
async fn count_schedules(sh: &Shared) {
    let engine = Lease::acquire(&sh.engine).await;
    if !engine.schedules() {
        return;
    }
    if let Ok(n) = engine.schedule_count().await {
        crate::metrics::schedules::set_range(engine.range().id.0, n as u64);
    }
}

/// Call [`cairn_engine::Engine::query_build_step`] until `Idle`. `false` on losing write
/// authority (the next term carries on building).
async fn build_until_idle(sh: &Shared) -> bool {
    let mut cursor = cairn_engine::BuildCursor::default();
    let mut backoff = cairn_store::pace::Backoff::new(
        std::time::Duration::from_millis(200),
        std::time::Duration::from_secs(30),
    );
    let mut rows = 0usize;
    loop {
        // A test keeps the range inside the "building" window: stand outside the engine,
        // yield the thread — no turn is blocked.
        if build_held() {
            cairn_store::pace::poll_gap(std::time::Duration::from_millis(50)).await;
            continue;
        }
        let step = {
            let mut engine = Lease::acquire(&sh.engine).await;
            engine
                .query_build_step(&mut cursor, cairn_engine::QUERY_BUILD_CHUNK)
                .await
                .map_err(ActorError::from)
        };
        match step {
            Ok(cairn_engine::BuildStep::Idle) => {
                if rows > 0 {
                    tracing::info!(rows, "query index build finished");
                }
                return true;
            }
            Ok(cairn_engine::BuildStep::Progress { rows: n }) => {
                rows += n;
                cairn_store::pace::poll_gap(std::time::Duration::from_millis(5)).await;
            }
            Err(e) if e.is_fenced() => return false,
            Err(e) => {
                tracing::warn!(error = %e, "background query index build failed, will redo");
                cursor = cairn_engine::BuildCursor::default();
                backoff.wait().await;
            }
        }
    }
}

/// **On-demand query counters**: wait for the term's first asker (`QueryCounts::snapshot`
/// while not yet built), then build (`query_build_step` scans `ByCreated` once the counters
/// have been asked for). If they were already built along with the background build (the
/// engine does not go on demand) nobody wakes it — the task waits until the end of the
/// term, at no cost.
async fn counts_when_asked(sh: &Shared) {
    let counts = Lease::acquire(&sh.engine).await.query_counts().clone();
    counts.asked().await;
    build_until_idle(sh).await;
}

/// **Build the artifact byte counter** of the term (`Engine::artifact_bytes_step`): only
/// when some tenant carries `max-artifact-bytes=` (reading the limit snapshot, without
/// holding the engine) — self-hosting pays no hot-range scan at all. One engine hold per
/// chunk, yielding between two chunks; done once finished (live writes keep the counter
/// correct until the end of the term). On an error that is not a fence, back off and go on
/// from the counter's cursor; on a fence, stop.
pub(super) async fn artifact_bytes_loop(sh: Shared) {
    let Some(limits) = Lease::acquire(&sh.engine).await.tenant_limits_source() else {
        return;
    };
    let mut backoff = cairn_store::pace::Backoff::new(
        std::time::Duration::from_millis(200),
        std::time::Duration::from_secs(30),
    );
    loop {
        let wanted = limits
            .snapshot()
            .limited()
            .any(|(_, l)| l.max_artifact_bytes.is_some());
        if !wanted {
            cairn_store::pace::beat(crate::tenant_usage::DEFAULT_EVERY).await;
            continue;
        }
        let step = {
            let mut engine = Lease::acquire(&sh.engine).await;
            engine
                .artifact_bytes_step(cairn_engine::QUERY_BUILD_CHUNK)
                .await
                .map_err(ActorError::from)
        };
        match step {
            Ok(Some(_)) => cairn_store::pace::poll_gap(std::time::Duration::from_millis(5)).await,
            Ok(None) => return,
            Err(e) if e.is_fenced() => return,
            Err(e) => {
                tracing::debug!(error = %e, "artifact byte counter build failed, will continue");
                backoff.wait().await;
            }
        }
    }
}

/// Failpoint `query::hint_sweep_now` enabled (`return`) ⇒ `true`: the hint sweep does not
/// wait [`HINT_SWEEP_DELAY`] (tests). Always `false` without `failpoints`.
fn hint_sweep_now() -> bool {
    cairn_store::failpoint!("query::hint_sweep_now", |_| true);
    false
}

/// Failpoint `query::build_hold` enabled (`return`) ⇒ `true`. Always `false` in a build
/// without the `failpoints` feature.
fn build_held() -> bool {
    cairn_store::failpoint!("query::build_hold", |_| true);
    false
}

/// The range's `cairnflow_query_index_state` gauge: read the state marks (one point GET,
/// holding the engine) — when the term starts and when the background build finishes. On
/// error keep the old number.
async fn publish_index_states(sh: &Shared) {
    let (range, states) = {
        let engine = Lease::acquire(&sh.engine).await;
        (engine.range().id.0, engine.query_index_states().await)
    };
    if let Ok(states) = states {
        crate::metrics::query_index::set_range(
            range,
            states
                .into_iter()
                .map(|(ix, s)| (ix, s == cairn_engine::IndexState::Readable))
                .collect(),
        );
    }
}

/// Wait after the background build finishes before sweeping hint rows: becoming leader does
/// not pay any extra block-range reads (`open_cost.rs`).
const HINT_SWEEP_DELAY: std::time::Duration = std::time::Duration::from_secs(60);

/// Hint rows per chunk of the sweep — each row is up to 3 reads (state, segmented state,
/// metadata) **within** one engine hold: 4 rows ⇒ ≤ 12 reads per hold (it used to be 1 024
/// rows).
const HINT_SWEEP_CHUNK: usize = 4;

/// Rest between two chunks: ≤ 16 hint rows (≤ 48 reads) per second per range — a bounded
/// background tax, not an O(number of objects) burst of reads.
const HINT_SWEEP_GAP: std::time::Duration = std::time::Duration::from_millis(250);

/// Once a full sweep of a range on this node is done, rest this long before sweeping again
/// — even across many terms. Redundant hint rows only cost space (readers check, Q-16);
/// sweeping every term under rapid leader changes would reread O(number of objects)
/// continuously.
const HINT_SWEEP_EVERY: std::time::Duration = std::time::Duration::from_secs(3600);

/// Progress of the hint sweep of **one range on this node** — lives across terms (the
/// range's leadership loop keeps it, `Shared::hint`), lost when the process dies. A new term
/// carries on from the cursor instead of rescanning from the start; once a full round is
/// done it rests [`HINT_SWEEP_EVERY`]. The cursor is only a starting point: each chunk reads
/// the latest state within the same engine hold as the delete (Q-16), so a stale cursor
/// (the range moved to another node and back) causes no wrong deletion — at worst it misses
/// rows until the next round.
#[derive(Default)]
pub(super) struct HintSweep {
    cursor: cairn_engine::HintCursor,
    done_at: Option<tokio::time::Instant>,
}

/// **The hint row sweep** (`Engine::hint_sweep_step`): [`HINT_SWEEP_DELAY`] after the
/// background build finishes, chunks of [`HINT_SWEEP_CHUNK`] rows per engine hold, resting
/// [`HINT_SWEEP_GAP`] between two chunks, carrying on from the cursor of the range on this
/// node ([`HintSweep`]). On error, stop — keep the cursor, the next term carries on
/// sweeping.
async fn hint_sweep(sh: &Shared) {
    if !hint_sweep_now() {
        cairn_store::pace::beat(HINT_SWEEP_DELAY).await;
    }
    let rested = sh
        .hint
        .borrow()
        .done_at
        .is_some_and(|t| sh.clock.mono().saturating_duration_since(t) < HINT_SWEEP_EVERY);
    if rested {
        return;
    }
    loop {
        cairn_store::failpoint!("query::hint_sweep_step");
        let mut cursor = sh.hint.borrow().cursor.clone();
        let step = {
            let mut engine = Lease::acquire(&sh.engine).await;
            engine.hint_sweep_step(&mut cursor, HINT_SWEEP_CHUNK).await
        };
        match step {
            Ok(Some(_)) => {
                sh.hint.borrow_mut().cursor = cursor;
                cairn_store::pace::poll_gap(HINT_SWEEP_GAP).await;
            }
            Ok(None) => {
                let mut h = sh.hint.borrow_mut();
                h.cursor = cairn_engine::HintCursor::default();
                h.done_at = Some(sh.clock.mono());
                return;
            }
            Err(e) => {
                tracing::debug!(error = %e, "hint row cleanup failed, the next term resumes it");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// Chunked `recover` replaced at every chunk boundary (the deployment registry changes
    /// faster than a full scan). An index of 10 ids, chunks of 3; each pass reads **one**
    /// chunk and is then replaced, the new pass starts from where the old one stood
    /// ([`super::ScanRing::position`]).
    ///
    /// Must see: the last id is scanned within ≤ 4 passes, and a pass that is not replaced
    /// goes all the way round — every id exactly once (from the inherited position to the
    /// end, then the beginning). Before the fix: a new pass rescanned from the start — ids
    /// 4…10 were never reached.
    #[test]
    fn superseded_recover_inherits_its_cursor() {
        use super::{InvocationId, ScanRing};
        let index: Vec<InvocationId> = (1..=10u8).map(|n| [n; 24]).collect();
        // `runnable_chunk`: at most 3 ids after `after`, plus the next chunk's cursor.
        let chunk = |after: Option<&InvocationId>| {
            let rest: Vec<InvocationId> = index
                .iter()
                .filter(|i| after.is_none_or(|a| *i > a))
                .copied()
                .collect();
            let ids: Vec<InvocationId> = rest.iter().take(3).copied().collect();
            let next = (rest.len() > 3).then(|| ids[2]);
            (ids, next)
        };
        let mut from = None;
        let mut seen = std::collections::BTreeSet::new();
        for _pass in 0..4 {
            let mut ring = ScanRing::from(from);
            let (ids, next) = chunk(ring.after());
            let (ids, _) = ring.advance(ids, next);
            seen.extend(ids);
            from = ring.position();
        }
        assert!(
            seen.contains(&[10; 24]),
            "the tail of the index is never scanned: {seen:?}"
        );
        // One pass not replaced, inheriting a middle position: every id exactly once.
        let mut ring = ScanRing::from(Some([4; 24]));
        let mut order = Vec::new();
        for _ in 0..10 {
            let (ids, next) = chunk(ring.after());
            let (ids, finished) = ring.advance(ids, next);
            order.extend(ids.iter().map(|i| i[0]));
            if finished {
                break;
            }
        }
        assert_eq!(order, vec![5, 6, 7, 8, 9, 10, 1, 2, 3, 4]);
    }

    use {super::wake_at, cairn_store::clock::Now, std::time::Duration, tokio::time::Instant};

    /// Size of the state of the future `f` returns — no need to build the arguments.
    fn future_size<A, B, C, D, F: std::future::Future>(_: impl FnOnce(A, B, C, D) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    fn future_size2<A, B, F: std::future::Future>(_: impl FnOnce(A, B) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    /// **A task waiting for a slot must be small.**
    ///
    /// Each invocation the drainer emits is a `run_dispatched` task, and that task lives for
    /// the whole wait for the engine and then for a slot. Its future is allocated in full
    /// right at spawn time, with the size of its **largest** state — i.e. the whole SDK turn,
    /// even though the task has no slot yet. Measured 61 784 B per task (release): a backlog
    /// of 3·10⁵ invocations is ~18 GB, the process gets compressed/swapped out, and the
    /// engine thread stalls on page faults — engine holds of 7–19 s.
    ///
    /// Must see: the future of `run_dispatched` ≤ 16 KiB in a debug build (the part of the
    /// turn after the slot lives in its own box, allocated only once there is a slot).
    #[test]
    fn waiting_turn_task_is_small() {
        let size = future_size(super::run_dispatched);
        assert!(
            size <= 16 * 1024,
            "future of the turn task waiting for a slot: {size} B"
        );
    }

    /// **Slot waiters are data**: the task above only lives through the gate (one engine
    /// hold); a waiter is a ticket in the scheduler's line. The turn of a granted ticket is
    /// a `run_granted` task (≤ the turn cap at once).
    ///
    /// Must see: a ticket ≤ 512 B (one `Box`, plus the core's `Pending` entry); the future
    /// of `run_granted` ≤ 16 KiB in a debug build.
    #[test]
    fn parked_waiter_is_data() {
        let ticket = std::mem::size_of::<crate::flow::Ticket>();
        assert!(ticket <= 512, "parked ticket: {ticket} B");
        let size = future_size2(super::run_granted);
        assert!(
            size <= 16 * 1024,
            "future of a granted ticket's turn: {size} B"
        );
        eprintln!("ticket {ticket} B, run_granted {size} B");
    }

    /// `wake_at` is a pure function of `now`: no deadline ⇒ the tick; a deadline earlier
    /// than the tick ⇒ wake at the start of the millisecond after the deadline, computed from
    /// **the same** read; a deadline already past ⇒ right away; a deadline later than the
    /// tick or overflowing `Instant` ⇒ the tick.
    #[tokio::test(start_paused = true)]
    async fn wake_at_is_a_function_of_now() {
        let mono = Instant::now();
        let now = Now {
            wall: Duration::from_millis(50_000),
            mono,
        };
        let tick = mono + Duration::from_millis(200);
        assert_eq!(wake_at(tick, None, now), tick);
        assert_eq!(
            wake_at(tick, Some(50_100), now),
            mono + Duration::from_millis(101)
        );
        assert_eq!(wake_at(tick, Some(49_000), now), mono);
        assert_eq!(wake_at(tick, Some(50_500), now), tick);
        assert_eq!(wake_at(tick, Some(u64::MAX), now), tick);
    }
}
