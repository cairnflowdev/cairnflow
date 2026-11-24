// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The leadership lifecycle of a range: `leadership_loop`, stepping down (`step_down`),
//! quiesce/seal/retire, `reject_until_leader`.

use super::*;

/// The "engine ready" signal: synchronous for [`EngineHandle::spawn_with_transport`] (the
/// caller blocks its thread), async for [`EngineHandle::spawn_in_pool`].
pub(super) enum ReadyTx {
    Sync(std::sync::mpsc::Sender<std::result::Result<(), String>>),
    Async(oneshot::Sender<std::result::Result<(), String>>),
}

impl ReadyTx {
    fn send(self, r: std::result::Result<(), String>) -> std::result::Result<(), ()> {
        match self {
            Self::Sync(tx) => tx.send(r).map_err(|_| ()),
            Self::Async(tx) => tx.send(r).map_err(|_| ()),
        }
    }
}

/// Why the serve loop ended.
pub(super) enum Step {
    /// The command channel closed, or `Shutdown` was received. Stop for good.
    Stop(AfterSeal),
    /// Write authority lost, or `Resign`. Drop the writer, win again from scratch.
    LostLeadership(AfterSeal),
    /// `Quiesce` or `Retire`: seal, keep the lease (see [`quiesced`]).
    Quiesce(AfterSeal),
    /// The store under the writer has failed (closed, or can no longer be made durable).
    /// Step down, **rest for one tick**, and only then win again — winning again right away
    /// on a failing store is a hot open-then-die loop.
    StoreFailed,
}

/// The rest after [`Step::StoreFailed`] before winning leadership again.
pub(super) const STORE_FAILED_COOLDOWN: Duration = Duration::from_secs(1);

/// Backoff between two failed engine opens: 0.5 s doubling up to [`OPEN_RETRY_MAX`],
/// jitter in the upper half; back to 0.5 s after every successful open. It used to be a
/// fixed 0.5 s: under a prolonged 503 each leaderless range added 4 PUT/s to the very
/// throttling.
fn open_retry_backoff() -> cairn_store::pace::Backoff {
    cairn_store::pace::Backoff::new(Duration::from_millis(500), OPEN_RETRY_MAX)
}

/// Cap on the rest between two failed engine opens. 8 s rather than 30 s (following Neon's
/// advice on reconnect backoff): after the store recovers, the range waits at most this long
/// before trying again — the same order as a leader change through the session TTL, so it
/// does not make leaderless time an order of magnitude longer; it still cuts the
/// acquire/release PUTs of a failing range 16-fold.
pub(super) const OPEN_RETRY_MAX: Duration = Duration::from_secs(8);

/// The lifecycle of a node: win leadership → serve → lose → win again.
///
/// This is where principle N3 is enforced. While there is **no** leadership yet, every
/// command is still received and answered — with `NotLeader`. No queueing: a client hanging
/// for 30 seconds before learning it hit the wrong node is a far worse experience than an
/// error returned right away with retry guidance.
pub(super) async fn leadership_loop(
    factory: EngineFactory,
    registry: Registry,
    transport: Transport,
    rules: Option<RuleStore>,
    opts: LoopOptions,
    mut rx: mpsc::Receiver<Command>,
    ready_tx: ReadyTx,
) {
    let LoopOptions {
        tick_interval,
        readiness,
        limits,
        retiring,
        sealed_epoch,
        budget,
        stats,
        handlers,
        load,
        streams,
        waiters,
        query_ranges,
        egress,
        identity,
        value_urls,
    } = opts;
    let client = invoker::Clients::new(egress, identity).with_value_urls(value_urls);
    // `Option` so we report exactly once: the other side only receives once.
    let mut ready = Some(ready_tx);
    if matches!(readiness, Readiness::Immediately) {
        if let Some(tx) = ready.take() {
            let _ = tx.send(Ok(()));
        }
    }

    let mut open_backoff = open_retry_backoff();
    // The hint sweep carries its cursor on across the range's terms on this node.
    let hint = Rc::new(std::cell::RefCell::new(HintSweep::default()));
    loop {
        // Win leadership, **answering while winning**. These two branches run concurrently
        // on purpose: the lower one is what keeps a follower a process that can talk.
        //
        // `biased` (deterministic in the sim without needing the runtime's RNG): if it wins,
        // take it, even if `Shutdown` arrives at the same moment — that command is still in
        // the channel, and the serve loop answers it the right way (seal, return the lease);
        // picking the lower branch would abandon a just-won leadership without returning it.
        // No starvation: `factory` finishes once and that is it.
        let leadership = tokio::select! {
            biased;
            r = factory() => r,
            _ = reject_until_leader(&mut rx, &retiring) => {
                if let Some(tx) = ready.take() {
                    let _ = tx.send(Ok(()));
                }
                return;
            }
        };

        let Leadership { mut engine, keeper } = match leadership {
            Ok(l) => l,
            Err(e) => {
                // `WhenLeader` means the caller treats opening the store as a startup
                // condition. Report the real error instead of retrying silently.
                if let Some(tx) = ready.take() {
                    if matches!(readiness, Readiness::WhenLeader) {
                        let _ = tx.send(Err(e.to_string()));
                        return;
                    }
                    let _ = tx.send(Ok(()));
                }
                if matches!(
                    &e,
                    EngineError::Store(cairn_store::StoreError::RangeRetired { .. })
                ) {
                    // Tombstone: the range has retired — do not win, do not open its Db, do
                    // not retry. Exit for good.
                    tracing::info!("range retired — engine exits");
                    retiring.store(true, Ordering::Release);
                    return;
                }
                // Exponential backoff with jitter: each failure is one acquire PUT + one
                // release PUT added to the store's throttling. Rest while still answering, as
                // after `StoreFailed`; `biased`: stop commands first.
                let pause = open_backoff.next_pause();
                cairn_store::retry::ops::OPEN_ENGINE.note_retry();
                tracing::warn!(error = %e, pause_ms = pause.as_millis() as u64, "cannot open the engine, retrying");
                tokio::select! {
                    biased;
                    () = reject_until_leader(&mut rx, &retiring) => return,
                    () = cairn_store::pace::retry_pause(pause) => {}
                }
                continue;
            }
        };
        if let Some(tx) = ready.take() {
            let _ = tx.send(Ok(()));
        }
        open_backoff = open_retry_backoff();

        // The outcome notifier of `/invoke`: a commit that takes an invocation to
        // `completed`/`failed`/`paused` wakes the waiters of exactly that id at submission.
        // Only a hint — the answer still goes through the durable ticket and the read fence
        // (`on_settled`).
        {
            let waiters = waiters.clone();
            engine
                .writer_mut()
                .set_status_hook(Box::new(move |id, tag| {
                    if InvocationStatus::is_settled_tag(tag) {
                        waiters.notify(id);
                    }
                }));
        }
        // Hook attached: waiters poll sparsely until `serve` returns (`fed` is dropped right
        // after that, before stepping down).
        let fed = waiters.feed();
        // Read handle of the query layer: registered once the range has opened, removed right
        // after serving — **before** the seal closes the Db.
        let query_guard = query_ranges.register(engine.read_handle());

        // One more leading engine ⇒ every engine's share of the node's turn cap shrinks
        // (D-c). The guard lives for the whole term.
        let _leading = budget.as_ref().map(|b| b.lead());

        // Watch the store **from outside the engine lock**: take the handle before the engine
        // goes into the lock.
        let health = engine.writer().health();
        // This term's streams: durable streams ride along with every batch of the writer, and
        // are durable by its `durable_seq`. With the `streams/table` gate off ⇒ streams only
        // live in RAM: the hub takes no durable streams, the writer has no rider — no
        // `Streams` row is written.
        let hub = streams.term(engine.streams());
        {
            let health = health.clone();
            hub.attach(move || health.durable_seq());
        }
        if engine.streams() {
            engine
                .writer_mut()
                .set_rider(Box::new(crate::streams::StreamRider(Arc::clone(&hub))));
        }
        // The engine's range — fixed for the whole term, so capture it once here.
        let range = *engine.range();
        // A new engine each term ⇒ the nearest deadline is new too (empty): the serve loop's
        // first tick runs right away and its scan rebuilds the deadline.
        let next_due = engine.next_due().clone();
        let clock = Arc::clone(engine.clock());
        // Tenant state: the scheduler reads the same source as the engine.
        let tenant_src = engine.tenant_limits_source();
        let (runner, mut run_rx) = crate::drainer::Runner::new();
        let range_load = load.range(range.id.0);
        let l0 = {
            let health = health.clone();
            crate::l0::Throttle::new(
                limits.l0_throttle,
                crate::l0::l0_cap(),
                move || health.l0_views(),
                range_load.l0_gauge(),
            )
        };
        let shared = Shared {
            next_due,
            clock,
            engine: engine_lock(engine),
            locks: Rc::new(Locks::default()),
            registry: registry.clone(),
            client: client.clone(),
            transport: transport.clone(),
            fenced: Rc::new(Notify::new()),
            scheduler: Scheduler::new(match &budget {
                Some(b) => b.share(),
                None => limits.max_concurrent_turns,
            }),
            sdk_timeout: limits.sdk_timeout,
            sdk_timeout_max: limits.sdk_timeout_max,
            queue_wait: limits.queue_wait,
            rules: rules.clone(),
            rules_seen: Rc::new(Cell::new(0)),
            drainer: crate::drainer::Drainer::new(),
            rescue: crate::drainer::Rescue::new(),
            runner,
            stats: stats.clone(),
            // This term's own shard: only this engine thread writes to it.
            handlers: Rc::new(handlers.shard()),
            load: range_load,
            l0: Rc::new(l0),
            quarantine: Rc::new(crate::quarantine::RangeView::new(handlers.quarantine())),
            streams: hub,
            live: Rc::default(),
            replay: Rc::new(crate::turn::ReplayHints::new(limits.rr_delta_replay)),
            lift: Rc::default(),
            reconcile_pending: Rc::new(Cell::new(false)),
            recover_generation: Rc::new(Cell::new(0)),
            recover_at: Rc::new(Cell::new(None)),
            infra_stuck: limits.infra_stuck,
            restore_chunk: limits.restore_chunk,
            recover_chunk: limits.recover_chunk,
            hint: Rc::clone(&hint),
            audit: None,
        };
        if let Some(src) = tenant_src {
            shared.scheduler.attach_tenants(src, limits.suspended_retry);
        }
        // Rows the drainer skipped because the invocation lock was busy are processed as
        // soon as that lock is released — without waiting for the next background tick.
        // Parked tickets nacked because the invocation lock was busy ask for a slot again
        // when that lock is released.
        {
            let drainer = Rc::clone(&shared.drainer);
            let scheduler = shared.scheduler.clone();
            shared.locks.on_release(Rc::new(move |id| {
                drainer.nudge();
                scheduler.lock_freed(id);
            }));
        }

        // **Every becoming-leader must recover again**, not only the first. A node that has
        // just come up may be holding the suspended work of a dead leader — it lives in the
        // state tables, not in anyone's RAM (INV-3), so it only needs to be read out and run
        // on. Skip this step and every unfinished invocation of the old leader sits still
        // forever.
        // **Recover flow control BEFORE taking any command.**
        //
        // The counters live in RAM, so a leader change loses them all — and that is a *good*
        // property: a system that naturally drops back to a clean state on every leader
        // change cannot accumulate counter leaks, they are wiped before they can become an
        // incident. What must be rebuilt is the **VO locks**, because locks must never be
        // lost.
        shared.scheduler.reset();
        let restore_started = tokio::time::Instant::now();
        // **The VO locks must exist before the first slot grant** (FL-8): the slot gate is
        // closed until `restore_flow` has gone all the way through. The rebuild runs in
        // chunks as **a task of the serve loop** (see `serve`): the range answers commands —
        // reads, creates, cancels — while it scans, so the first command does not wait for a
        // pass through the whole history.
        //
        // Except when the queue `seq` counter is not available without a scan (the durable
        // mark `floors::QUEUE_HIGH` missing or a mismatched mark — the first time after an
        // upgrade, the previous writer died before its first commit): then create commands
        // cannot be served yet, and the rebuild runs **before** the serve loop, as it used
        // to. This phase reads no commands; on losing the lease it gives up right away.
        shared.scheduler.close_gate();
        let primed = Lease::acquire(&shared.engine).await.queue_seq_primed();
        if !primed {
            let restore =
                crate::lease::holding_as("restore", restore_flow(&shared, restore_started));
            let lost = async {
                match keeper.as_ref() {
                    Some(k) => k.lost().await,
                    None => std::future::pending().await,
                }
            };
            let restored = tokio::select! {
                biased;
                () = lost => Err(ActorError::NotLeader),
                r = restore => r,
            };
            if let Err(e) = restored {
                // Failing to rebuild the queues means not knowing who holds which VO lock.
                // Carrying on would let two invocations run on one object — so step down and
                // let another node try.
                tracing::error!(error = %e, "cannot recover the queue — yielding leadership");
                shared.fenced.notify_one();
            }
        }
        first_rules(&shared).await;

        let step = serve(
            &shared,
            range,
            &retiring,
            budget.as_ref(),
            keeper.as_ref(),
            &mut rx,
            &mut run_rx,
            tick_interval,
            health,
            limits.durability_stall,
            primed.then_some(restore_started),
        )
        .await;
        // Serving is over: the range's next outcome (if any) is committed by another engine,
        // this term's hook no longer notifies. Drop the flag **and wake every waiter** — they
        // ask right away, see the range changed or lost its leader, then follow the dense
        // poll as before the notifier existed; nobody sleeps through a whole sparse poll
        // tick.
        drop(fed);
        drop(query_guard);
        // Temporary streams live exactly as long as the term: SSE readers receive `moved`
        // (+ `reset`), the RAM goes back to the node.
        shared.streams.end_term();

        // Step down: seal the writer, then (unless quiescing) return the lease — **while still
        // answering commands**. `seal` waits for the pending part to become durable, flushes,
        // then closes the store; if the WAL cannot be flushed (store failing, network to the
        // store cut) it waits up to the cap, and a step-down loop that does not read the
        // command channel is a silent node — clients don't know where to go, operators don't
        // know why.
        //
        // `Shutdown`/`Resign`/`Quiesce`/`Retire` commands are answered **after** the seal has
        // finished: their callers trust that the Db is closed when they get the answer — the
        // split protocol clones the Db right after.
        //
        // If the store has already failed, the pending part almost certainly can no longer be
        // made durable — waiting out its full cap only slows down winning it back.
        let seal_timeout = seal_timeout(
            matches!(step, Step::StoreFailed),
            keeper.as_ref().and_then(LeaseKeeper::wal_stall_max),
        );
        let (mut after, quiescing, kind) = match step {
            Step::Stop(a) => (a, false, StepKind::Stop),
            Step::LostLeadership(a) => (a, false, StepKind::Lost),
            Step::StoreFailed => (AfterSeal::default(), false, StepKind::StoreFailed),
            Step::Quiesce(a) => (a, true, StepKind::Lost),
        };
        let seal_started = tokio::time::Instant::now();
        let (sealed, mut keeper, epoch) = step_down(
            shared.engine,
            keeper,
            // Quiesce keeps the lease: the Db is closed but the range is still ours, so no
            // node reopens it while the split coordinator is cloning it.
            !quiescing,
            seal_timeout,
            &mut rx,
            quiescing,
            &mut after,
        )
        .await;
        if let Some(e) = epoch {
            sealed_epoch.store(e, Ordering::Release);
        }

        if quiescing {
            stats.update(range.id.0, |t| {
                t.seal_ms = Some(crate::range_stats::ms(seal_started.elapsed()));
            });
        }
        if quiescing && sealed.is_ok() && !after.stop {
            // **Really closed**, lease still held. Answer the one asking for quiesce, then
            // either retire right away (`Retire` already received) or stay quiesced waiting
            // for the next command.
            after.answer(&sealed);
            if !after.retire.is_empty() {
                let r = match keeper.take() {
                    Some(k) => k.retire().await.map_err(ActorError::from),
                    None => Ok(()),
                };
                retiring.store(true, Ordering::Release);
                after.answer_retire(&r);
                tracing::info!("range retired — engine exits");
                return;
            }
            match quiesced(keeper, &mut rx, &retiring, after.holder.take()).await {
                AfterQuiesce::Exit => return,
                AfterQuiesce::Reacquire => continue,
            }
        }

        // Quiesce failed (the seal missed) or was interrupted by `Shutdown`: do not keep the
        // lease for a Db we are not sure is closed.
        if let Some(k) = keeper.take() {
            k.release().await;
        }
        after.answer(&sealed);
        if !after.retire.is_empty() {
            // No longer leader: the tombstone cannot be written — just exit for good. The
            // coordinator learns it through the error if the tombstone is what it needs.
            let r = if quiescing {
                sealed
                    .as_ref()
                    .map(|_| ())
                    .map_err(|e| ActorError::SealFailed(e.to_string()))
            } else {
                Ok(())
            };
            retiring.store(true, Ordering::Release);
            after.answer_retire(&r);
            return;
        }
        if after.stop {
            return;
        }
        match kind {
            StepKind::Stop => return,
            StepKind::Lost => {
                tracing::warn!("leadership lost — reacquiring from scratch");
            }
            StepKind::StoreFailed => {
                tracing::warn!(
                    "store failed — pausing {STORE_FAILED_COOLDOWN:?} before reacquiring"
                );
                // Rest while still answering: commands arriving now get NotLeader. `biased`:
                // a stop command arriving right as the rest ends stops — the lower branch is
                // only ready on `Shutdown`/`Retire`/a closed channel.
                tokio::select! {
                    biased;
                    () = reject_until_leader(&mut rx, &retiring) => return,
                    () = tokio::time::sleep(STORE_FAILED_COOLDOWN) => {}
                }
            }
        }
    }
}

/// The kind of step-down, after the commands waiting for an answer have been split off.
pub(super) enum StepKind {
    Stop,
    Lost,
    StoreFailed,
}

pub(super) type Reply = oneshot::Sender<Result<()>>;

/// Commands waiting for the step-down's outcome — answered **after** the seal finishes.
#[derive(Default)]
pub(super) struct AfterSeal {
    /// `Resign`/`Quiesce`: receive the seal result.
    pub(super) replies: Vec<Reply>,
    /// `Shutdown`: receive the seal result — unless the seal failed **because we were
    /// fenced**: then the Db is no longer ours, the purpose of the seal (nobody writes
    /// through this writer any more) has come true, and shutting down is a success. A
    /// split's `Quiesce` is different — the coordinator must not clone a Db someone else is
    /// writing — so it still receives the error.
    pub(super) shutdowns: Vec<Reply>,
    /// `Retire`: receive the result of writing the tombstone.
    pub(super) retire: Vec<Reply>,
    /// The tether of the coordinator that asked for quiesce — carried over to [`quiesced`].
    pub(super) holder: Option<QuiesceHold>,
    /// There was a `Shutdown`, or the command channel has closed: stop for good after
    /// stepping down.
    pub(super) stop: bool,
}

impl AfterSeal {
    pub(super) fn reply(reply: Reply) -> Self {
        Self {
            replies: vec![reply],
            ..Self::default()
        }
    }

    /// Queue a command that arrives **during** the step-down. While quiescing, a command
    /// aimed at a key gets `WrongRange` (the range is closing — reload the table), not
    /// `NotLeader`.
    pub(super) fn take(&mut self, cmd: Option<Command>, quiescing: bool) {
        match cmd {
            None => self.stop = true,
            Some(Command::Shutdown { reply }) => {
                self.shutdowns.push(reply);
                self.stop = true;
            }
            Some(Command::Retire { reply }) => self.retire.push(reply),
            Some(Command::Quiesce { reply, holder, .. }) if quiescing => {
                self.holder = self.holder.take().or(holder);
                self.replies.push(reply)
            }
            Some(Command::Resign { reply }) if quiescing => self.replies.push(reply),
            Some(c) if quiescing && c.target_key().is_some() => c.reject_wrong_range(),
            Some(c) => c.reject_not_leader(),
        }
    }

    fn answer(&mut self, sealed: &Result<()>) {
        for r in self.replies.drain(..) {
            let _ = r.send(match sealed {
                Ok(()) => Ok(()),
                Err(e) => Err(ActorError::SealFailed(e.to_string())),
            });
        }
        for r in self.shutdowns.drain(..) {
            let _ = r.send(match sealed {
                Ok(()) => Ok(()),
                Err(e) if e.is_fenced() => Ok(()),
                Err(e) => Err(ActorError::SealFailed(e.to_string())),
            });
        }
    }

    fn answer_retire(&mut self, r: &Result<()>) {
        for reply in self.retire.drain(..) {
            let _ = reply.send(match r {
                Ok(()) => Ok(()),
                Err(e) => Err(ActorError::SealFailed(e.to_string())),
            });
        }
    }
}

/// Seal the writer, and release the lease if `release` — **still answering commands** while
/// waiting, queueing those that must be answered after the seal into `after`. Returns the
/// seal result together with the lease still held (when `release = false`).
pub(super) async fn step_down(
    engine: EngineLock,
    keeper: Option<LeaseKeeper>,
    release: bool,
    seal_timeout: Duration,
    rx: &mut mpsc::Receiver<Command>,
    quiescing: bool,
    after: &mut AfterSeal,
) -> (Result<()>, Option<LeaseKeeper>, Option<u64>) {
    cairn_store::failpoint!("actor::step_down");
    let work = async move {
        // Take the engine out of the lock to `seal` — it takes `self` by value: a sealed
        // writer has no way back to life. `serve` has stopped every task before returning,
        // so the `Err` branch means someone still holds the engine who should not. No panic
        // — handing over leadership without a seal is only one TTL slower.
        // Along with the `writer_epoch` of the writer just sealed successfully — the
        // split/merge coordinator closes the source conditional on it.
        let (sealed, epoch) = match engine.into_inner() {
            Ok(engine) => {
                let epoch = engine.writer().writer_epoch();
                match tokio::time::timeout(seal_timeout, engine.seal()).await {
                    Ok(Ok(())) => (Ok(()), Some(epoch)),
                    // A seal error is not alarming: if we were fenced the store is already
                    // closed, and the purpose of the seal — no more writes next time — has
                    // come true. But whoever waits for "closed" must know.
                    Ok(Err(e)) => {
                        tracing::debug!(error = %e, "sealing the writer on step-down failed");
                        (Err(ActorError::from(e)), None)
                    }
                    Err(_) => {
                        tracing::warn!(
                            "seal exceeded {seal_timeout:?} — abandoning the unsealed writer; \
                             the next leader fences it via writer_epoch"
                        );
                        (
                            Err(ActorError::SealFailed(format!(
                                "seal exceeded {seal_timeout:?}"
                            ))),
                            None,
                        )
                    }
                }
            }
            Err(_) => {
                tracing::warn!("a task still holds the engine on step-down — seal skipped");
                (
                    Err(ActorError::SealFailed(
                        "a task still holds the engine".into(),
                    )),
                    None,
                )
            }
        };
        let mut keeper = keeper;
        if sealed.is_err() {
            // A dirty handover: count and mark it. The keeper's handover rotates the log
            // stream before writing the owner record (`LeaseKeeper::rotate_live_logs`): the
            // receiver does not seal the node's live log.
            if let Some(k) = keeper.as_ref() {
                k.note_dirty_yield();
            }
        }
        if sealed.is_ok() {
            // Every frame of the range is in L0: with the shared WAL, the handover / tombstone
            // write below clears `wal_segment` — the receiver replays nothing. If the seal
            // missed, the handover is **dirty**: the receiver seals our log.
            if let Some(k) = keeper.as_mut() {
                if let Err(e) = k.writer_sealed().await {
                    tracing::warn!(error = %e, "cannot CAS the clean owner record after seal");
                }
            }
        }
        let keeper = match (release, keeper) {
            (true, Some(k)) => {
                k.release().await;
                None
            }
            (_, k) => k,
        };
        (sealed, keeper, epoch)
    };
    tokio::pin!(work);
    loop {
        // `biased`: once the work is done, return right away; commands are still received
        // every time the work is unfinished. No starvation: `work` is asked first in every
        // round, so a flood of commands does not hold it back.
        tokio::select! {
            biased;
            r = &mut work => return r,
            cmd = rx.recv() => {
                let closed = cmd.is_none();
                after.take(cmd, quiescing);
                if closed {
                    // Channel closed: nobody left to answer — just wait for the rest.
                    return work.await;
                }
            }
        }
    }
}

/// The receiving end of the quiesce tether ([`EngineHandle::quiesce_held`]). The coordinator
/// holds the sending end and never sends anything: the sending end dropping is the signal.
pub type QuiesceHold = oneshot::Receiver<()>;

/// Outcome of the quiesced state.
pub(super) enum AfterQuiesce {
    /// Retire, or stop for good.
    Exit,
    /// Drop the quiesce (split rolled back, or lease lost) — win again from scratch, reopen
    /// the Db.
    Reacquire,
}

/// The **quiesced** state: the Db is closed, the lease still held — no node reopens the Db
/// while the coordinator is cloning it (the clone steps of a split). Every command aimed at
/// a key gets `WrongRange`.
///
/// - `Retire` ⇒ write the tombstone, exit for good (the retire step of a split).
/// - `Resign` ⇒ release the lease, win again from scratch — the **rollback** path of an
///   uncommitted split: the range reopens as usual, nothing of it has been modified since
///   the quiesce.
/// - `Shutdown`, channel closed ⇒ release the lease, stop.
/// - Lease lost ⇒ win again from scratch (another node may have taken it and rolled back).
/// - The coordinator vanished (the `holder` tether broke, SM-15) ⇒ like `Resign`: the
///   engine constructor wins again, finds an intent nobody coordinates, and rolls back.
pub(super) async fn quiesced(
    mut keeper: Option<LeaseKeeper>,
    rx: &mut mpsc::Receiver<Command>,
    retiring: &AtomicBool,
    mut holder: Option<QuiesceHold>,
) -> AfterQuiesce {
    loop {
        let lost = async {
            match &keeper {
                Some(k) => k.lost().await,
                None => std::future::pending().await,
            }
        };
        let gone = async {
            match holder.as_mut() {
                // The sending end never sends anything: returning means it has dropped.
                Some(h) => {
                    let _ = h.await;
                }
                None => std::future::pending().await,
            }
        };
        // `biased`, commands first: the coordinator sends `Retire`/`Resign` and waits for the
        // answer **before** dropping the tether, so its commands always come before the
        // "vanished" signal.
        let cmd = tokio::select! {
            biased;
            cmd = rx.recv() => cmd,
            () = lost => {
                tracing::warn!("lease lost during quiesce — reacquiring from scratch");
                return AfterQuiesce::Reacquire;
            }
            () = gone => {
                tracing::warn!("split/merge coordinator vanished while the range was quiescing — releasing, reacquiring (backing off while the intent remains)");
                if let Some(k) = keeper.take() {
                    k.release().await;
                }
                return AfterQuiesce::Reacquire;
            }
        };
        match cmd {
            None => {
                if let Some(k) = keeper.take() {
                    k.release().await;
                }
                return AfterQuiesce::Exit;
            }
            Some(Command::Retire { reply }) => {
                let r = match keeper.take() {
                    Some(k) => k.retire().await.map_err(ActorError::from),
                    None => Ok(()),
                };
                retiring.store(true, Ordering::Release);
                tracing::info!("range retired — engine exits");
                let _ = reply.send(r);
                return AfterQuiesce::Exit;
            }
            Some(Command::Shutdown { reply }) => {
                if let Some(k) = keeper.take() {
                    k.release().await;
                }
                let _ = reply.send(Ok(()));
                return AfterQuiesce::Exit;
            }
            Some(Command::Resign { reply }) => {
                if let Some(k) = keeper.take() {
                    k.release().await;
                }
                let _ = reply.send(Ok(()));
                return AfterQuiesce::Reacquire;
            }
            // Already closed.
            Some(Command::Quiesce {
                reply, holder: h, ..
            }) => {
                holder = holder.or(h);
                let _ = reply.send(Ok(()));
            }
            Some(c) if c.target_key().is_some() => c.reject_wrong_range(),
            Some(c) => c.reject_not_leader(),
        }
    }
}

/// Cap on the `seal` step when handing over leadership, with the native WAL.
///
/// Not a target: a healthy seal finishes within one flush tick. The cap here is for exactly
/// one case — the WAL cannot be flushed — and past the cap the writer is dropped without a
/// seal: safe, because the next leader fences it with `writer_epoch` (the second fencing
/// layer).
pub(super) const SEAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Like [`SEAL_TIMEOUT`], when stepping down **because** the store has failed.
pub(super) const SEAL_TIMEOUT_FAILED_STORE: Duration = Duration::from_secs(1);

/// The part of the seal cap set aside for flushing the memtable to L0 plus closing the Db,
/// on top of waiting for the WAL. A full memtable (64 MiB) to S3 Standard is a few seconds;
/// past ten seconds the store is so slow that a dirty handover (and the receiver replaying)
/// is no longer more expensive than waiting longer.
pub(super) const SEAL_FLUSH_BUDGET: Duration = Duration::from_secs(10);

/// Cap on one memtable flush **while serving** (`FlushMemtable`, before the quiesce window
/// of a split/merge), while holding the engine. The same budget as the flush part of the
/// seal cap: a flush that does not make it within that would not make it in the later seal
/// either.
pub(super) const FLUSH_MEMTABLE_BOUND: Duration = SEAL_FLUSH_BUDGET;

/// The seal cap when stepping down. `wal_stall` = the `stall_max` of the shared WAL the Db
/// writes into ([`LeaseKeeper::wal_stall_max`]), `None` with the native WAL.
///
/// With the shared WAL, a seal past the cap is a **dirty** handover: the owner record keeps
/// the node's log pointer and the receiver seals it. That used to be the **live** log and
/// the node failed stop; now the keeper rotates the stream before writing the owner record,
/// so the price is that every range of the stream steps down and is won again (the
/// receiver replays). The cap must still exceed every wait the log stream can bear without
/// rotating by itself — `stall_max` — plus the memtable flush. That rule stands even when
/// stepping down because the store failed: with the Db closed the seal returns an error
/// right away instead of waiting, and a log stream stalled past `stall_max` has already
/// rotated by itself. A short cap here would only trade one wait for a rotation of the
/// whole stream.
pub(crate) fn seal_timeout(store_failed: bool, wal_stall: Option<Duration>) -> Duration {
    match wal_stall {
        Some(stall) => stall + SEAL_FLUSH_BUDGET,
        None if store_failed => SEAL_TIMEOUT_FAILED_STORE,
        None => SEAL_TIMEOUT,
    }
}

/// Cap on waiting for work in flight when `Retire` reaches a range that is not quiesced.
pub(super) const RETIRE_GRACE: Duration = Duration::from_secs(10);

/// Return `NotLeader` for every command while the node has no write authority.
///
/// Returns when the channel closes or when `Shutdown` is received — the only two cases in
/// which the leadership acquisition loop must stop for good.
pub(super) async fn reject_until_leader(rx: &mut mpsc::Receiver<Command>, retiring: &AtomicBool) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Shutdown { reply } => {
                let _ = reply.send(Ok(()));
                return;
            }
            // Not leading: there is no tombstone to write — just exit for good.
            Command::Retire { reply } => {
                retiring.store(true, Ordering::Release);
                let _ = reply.send(Ok(()));
                return;
            }
            // The range is retiring: "try again on another node" is the wrong way — no node
            // will take it any more. Tell the caller to reload the table.
            other if other.target_key().is_some() && retiring.load(Ordering::Acquire) => {
                other.reject_wrong_range()
            }
            other => other.reject_not_leader(),
        }
    }
}

/// The relation between the seal cap and the shared WAL budget holds for every log stream
/// configuration, including PUT caps tuned through command-line flags.
#[cfg(test)]
mod seal_timeout_tests {
    use {super::*, cairn_store::NodeWalConfig};

    #[test]
    fn seal_outlasts_shared_wal_stall_budget_plus_flush() {
        for (put_timeout_ms, attempts) in
            [(2_000, 3), (6_000, 3), (500, 1), (10_000, 5), (2_000, 0)]
        {
            let cfg = NodeWalConfig {
                put_timeout: Duration::from_millis(put_timeout_ms),
                put_attempts: attempts,
                ..NodeWalConfig::default()
            };
            for store_failed in [false, true] {
                let t = seal_timeout(store_failed, Some(cfg.stall_max()));
                assert!(
                    t >= cfg.stall_max() + SEAL_FLUSH_BUDGET,
                    "seal cap {t:?} does not exceed stall_max {:?} + memtable flush",
                    cfg.stall_max()
                );
            }
        }
        assert_eq!(seal_timeout(false, None), SEAL_TIMEOUT);
        assert_eq!(seal_timeout(true, None), SEAL_TIMEOUT_FAILED_STORE);
    }
}
