// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The term's serve loop: `Shared` and `serve`.

use super::*;

/// Everything a command task needs, gathered so it is `clone`d once and then `move`d into
/// the task.
#[derive(Clone)]
pub(super) struct Shared {
    pub(super) engine: EngineLock,
    pub(super) locks: Rc<Locks>,
    pub(super) registry: Registry,
    pub(super) client: invoker::Clients,
    pub(super) transport: Transport,
    /// A task has just hit `Fenced`. The serve loop wakes up and steps down.
    ///
    /// A separate channel is needed because a task can no longer `return` out of the loop
    /// the way it could when everything ran in place.
    pub(super) fenced: Rc<Notify>,
    /// The partition's counters, VO locks and scheduling loop.
    ///
    /// It fully replaces the old `Semaphore`: the turn cap is now **a resource in the
    /// scheduler** on a par with counters and locks. A brake outside the eligibility loop
    /// leaves the scheduler unaware of whom it is blocking, so it does not wake the right
    /// one on release — and "fairness" turns into whoever-asks-again-faster.
    pub(super) scheduler: Scheduler,
    pub(super) sdk_timeout: Option<Duration>,
    /// Upper bound on the `inactivityTimeout` a contract declares.
    pub(super) sdk_timeout_max: Duration,
    pub(super) queue_wait: Option<Duration>,
    /// The cluster-level rule book. `None` ⇒ no caps at all — valid, and the default of a
    /// cluster that has set no rules.
    pub(super) rules: Option<RuleStore>,
    /// The round of the shared rule book ([`RuleStore::published`]) the engine has applied:
    /// differs from the current round ⇒ apply the new copy. No I/O — the node-level poll
    /// loop reads it.
    pub(super) rules_seen: Rc<Cell<u64>>,
    /// The path that hands a durable invocation to the scheduler (the drain loop runs no
    /// SDK turns).
    ///
    /// The other end of the channel is held by [`serve`], so it lives exactly as long as one
    /// leader term: stepping down closes the channel, and the remaining work stays in the
    /// durable queue for the next leader.
    pub(super) runner: crate::drainer::Runner,
    /// The node's range lifecycle measurements.
    pub(super) stats: crate::range_stats::RangeStats,
    /// This term's part of the per-handler summary.
    pub(super) handlers: Rc<crate::stats::Shard>,
    /// This range's load — written by the engine thread, no lock waits.
    pub(super) load: Arc<crate::autosplit::RangeLoad>,
    /// This term's L0 throttle.
    pub(super) l0: Rc<crate::l0::Throttle>,
    /// A copy of the node's quarantine list.
    pub(super) quarantine: Rc<crate::quarantine::RangeView>,
    /// This term's streams: a new hub each term, `end_term` when the term ends.
    pub(super) streams: Arc<crate::streams::RangeStreams>,
    /// The cancel bell of running turns.
    pub(super) live: Rc<turn::LiveTurns>,
    /// Incremental replay hints — the term's RAM: a leader change, a split/merge, a node
    /// reopening ⇒ a new registry, the next turn replays in full.
    pub(super) replay: Rc<turn::ReplayHints>,
    /// The partition's outbox drainer. See [`crate::drainer`].
    ///
    /// One per `Shared`, i.e. one per leader term — and every command task sees exactly that
    /// one. That is where "exactly one reader" is enforced: there is no path that builds a
    /// second drainer without building a second `Shared`.
    pub(super) drainer: Rc<crate::drainer::Drainer>,
    /// The term's safety net — see [`crate::drainer::Rescue`]. Its reconciliation loop is
    /// [`rescue_loop`].
    pub(super) rescue: Rc<crate::drainer::Rescue>,
    /// The unfinished pull of rescheduled rows of a just-unsuspended tenant — only the
    /// background loop reads/writes it, between ticks; the term's RAM.
    pub(super) lift: Rc<std::cell::RefCell<turn::LiftSweep>>,
    /// The nearest deadline of the reschedule table — the engine's handle, read outside the
    /// engine lock so the serve loop sleeps until exactly the deadline.
    pub(super) next_due: NextDue,
    /// The range's clock — the engine's own clock: the wall time of the background tick and
    /// the wake-up at the deadline are read from here.
    pub(super) clock: Arc<dyn cairn_store::clock::Clock>,
    /// A [`recover`] already scheduled that has not started scanning — a later
    /// `DeploymentsChanged` command merges into it.
    pub(super) reconcile_pending: Rc<Cell<bool>>,
    /// Sequence number of the newest [`recover`] that has started scanning: a pass scanning
    /// in chunks that sees this number change stops — the new pass starts from where it
    /// stands ([`Self::recover_at`]).
    pub(super) recover_generation: Rc<Cell<u64>>,
    /// Where the scanning [`recover`] stands on the `Invocations` index — written after each
    /// chunk, within the chunk's engine hold. A new pass inherits it and then wraps around
    /// to scan the beginning: if the deployment registry changes faster than a full scan,
    /// the tail of the index is still reached.
    pub(super) recover_at: Rc<Cell<Option<InvocationId>>>,
    /// The safety net's `paused` 577 threshold.
    pub(super) infra_stuck: crate::drainer::InfraStuck,
    /// Index rows per chunk of the VO lock rebuild ([`restore_flow`]).
    pub(super) restore_chunk: usize,
    /// [`super::TurnLimits::recover_chunk`].
    pub(super) recover_chunk: usize,
    /// Progress of the range's hint sweep on this node, across terms ([`HintSweep`]).
    pub(super) hint: Rc<std::cell::RefCell<HintSweep>>,
    /// Audit note of the command running in this task (`Command::Audited`) — `None` in
    /// every other task.
    pub(super) audit: Option<Rc<crate::audit::AuditNote>>,
}

/// Cap on waiting for an engine to answer a `DeploymentsChanged` reminder from the node's
/// background loop. A serving engine answers within a few ms; past the cap it is not
/// reading commands yet (recovering) — the reminder is redundant for it.
pub const NOTIFY_WAIT: Duration = Duration::from_secs(2);

/// Cap on waiting for one `resign` from the node's background loop (balancing, voluntary
/// release). Enough for one seal at its full cap ([`SEAL_TIMEOUT`]) plus returning the
/// lease; past the cap the loop moves on — the command is still in the channel, the engine
/// does it when its turn comes.
pub const RESIGN_WAIT: Duration = Duration::from_secs(30);

/// The rule book re-poll tick — a **net**: 30 s (it used to be 2 s).
///
/// **Not** equal to the tick, and that is deliberate: the tick runs a few times a second and
/// each ask is a `GET` on the object store, for something that changes a few times a day.
/// **One poll loop per node** reads it ([`crate::node`], `spawn_rules_poller`), and the
/// ranges' engines pick up the shared copy ([`RuleStore::published`]) on the background tick
/// — previously each range did its own GET, so the cost multiplied by the number of ranges.
/// A rule change reaches the poll loop through the generation of [`RuleStore`]: the node
/// writes and bumps it, and tells its peers ([`crate::registry_sync`]) so they bump it — the
/// loop rereads right away. This tick only catches missed notifications; `/scheduler`
/// exposes `rulesVersion` so it can be seen.
pub(crate) const RULES_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Cap on one rule book GET of the poll loop, and on how long an engine that just became
/// leader waits for the node's first read.
pub(crate) const RULES_READ_BOUND: Duration = Duration::from_secs(10);

/// A failed rule book read ⇒ the next round after this long, without waiting a full poll
/// tick.
pub(crate) const RULES_RETRY: Duration = Duration::from_secs(1);

impl Shared {
    /// Build a [`turn::Ctx`] for a task. The deployment snapshot must live outside `Ctx`, so
    /// it is returned alongside — the caller keeps both.
    pub(super) fn ctx<'a>(&'a self, deployments: &'a crate::registry::Catalog) -> turn::Ctx<'a> {
        turn::Ctx {
            clients: &self.client,
            deployments,
            transport: &self.transport,
            locks: &self.locks,
            drainer: &self.drainer,
            runner: &self.runner,
            rescue: &self.rescue,
            sdk_timeout: self.sdk_timeout,
            sdk_timeout_max: self.sdk_timeout_max,
            scheduler: &self.scheduler,
            queue_wait: self.queue_wait,
            stats: &self.handlers,
            load: &self.load,
            l0: &self.l0,
            quarantine: &self.quarantine,
            streams: &self.streams,
            live: &self.live,
            audit: self.audit.as_deref(),
            replay: &self.replay,
        }
    }

    /// Answer the caller, and report if the error is "just lost write authority".
    ///
    /// Answer **before** reporting: the waiting client is entitled to know right away, and
    /// `NotLeader` is a valid answer, not an incident that has to be cleaned up before it
    /// may be told.
    /// Force durability, **then** answer the caller.
    ///
    /// Answering a client is the biggest external effect cairn has: from that second on
    /// someone holds an id and trusts it can be looked up, or holds a result and trusts it
    /// is real. Since `PartitionWriter::commit` stopped waiting for durability by itself
    /// (see [`cairn_store::Committed`]), keeping that promise is this place's job.
    ///
    /// **Why `sync` sits in `finish` rather than at sixteen call sites.** Both ways run
    /// correctly when written correctly; only one of them can break by being *forgotten*.
    /// Taking the `Lease` means no place can answer without passing the engine through
    /// here, and the durability step has no way around it.
    ///
    /// **Consumes the `Lease`**: the answer is the task's last job, so the ticket releases
    /// the engine before waiting and does not win it back — winning it back only to release
    /// it right away would queue behind every hold that slipped in during the wait. A task
    /// that still has work with the engine after the answer goes through
    /// [`Self::finish_held`].
    ///
    /// **Forcing durability fails ⇒ the client gets an error**, even when `r` is `Ok`. That
    /// is not excess defensiveness: `sync` is the only place a zombie is caught (a fenced
    /// writer still accepts `put`, the fence only fires at the durability step), so an `Ok`
    /// sent before it is a promise we have no right to make yet.
    pub(super) async fn finish<T>(
        &self,
        reply: oneshot::Sender<Result<T>>,
        r: Result<T>,
        engine: Lease,
    ) {
        // Ticket **and** read fence: an answer on the leader does not go out when the
        // bucket says our session has ended.
        // The answer is the task's last job: the ticket releases the engine and does not
        // win it back.
        let r = match engine.sync_reply_owned().await {
            Ok(_durable) => r,
            Err(e) => Err(ActorError::from(e)),
        };
        let fenced = r.as_ref().err().is_some_and(ActorError::is_fenced);
        let _ = reply.send(r);
        if fenced {
            self.fenced.notify_one();
        }
    }

    /// Like [`Self::finish`] for a task that **still has work with the engine** after the
    /// answer (`/send`: answer, then run the first turn behind the caller's back): the
    /// ticket releases the engine while waiting, then wins it back.
    pub(super) async fn finish_held<T>(
        &self,
        reply: oneshot::Sender<Result<T>>,
        r: Result<T>,
        engine: &mut Lease,
    ) {
        let r = match engine.sync_reply().await {
            Ok(_durable) => r,
            Err(e) => Err(ActorError::from(e)),
        };
        let fenced = r.as_ref().err().is_some_and(ActorError::is_fenced);
        let _ = reply.send(r);
        if fenced {
            self.fenced.notify_one();
        }
    }

    /// Like [`Self::finish`], **without** the read fence: the durable ticket and `alive` are
    /// still enough, only the GET of the session record is skipped.
    ///
    /// Only for answers the fence cannot prove anything more about:
    ///
    /// - **seen by nobody**: the asker drops it and asks again — the "not done yet" of
    ///   [`Command::Settled`]. The answer that goes out afterwards passes the fence itself;
    /// - **only asserting durable history**: the "message is in the range" of
    ///   [`Command::Inbound`]. The durable ticket proves every write of ours reached the store
    ///   **before** we were fenced (LS-4), and the new owner replays them — that statement
    ///   holds even if our clock is wrong.
    ///
    /// An answer that reads state (which a new owner may have changed) must **not** take
    /// this path: that is exactly the stale read the fence blocks.
    pub(super) async fn finish_unexposed<T>(
        &self,
        reply: oneshot::Sender<Result<T>>,
        r: Result<T>,
        engine: Lease,
    ) {
        self.finish_unexposed_then(reply, r, engine, || {}).await
    }

    /// Like [`Self::finish_unexposed`]; `on_durable` runs when the answer is `Ok` and
    /// durable, **before** it goes out — the measurement of an ack does not come after that
    /// very ack.
    pub(super) async fn finish_unexposed_then<T>(
        &self,
        reply: oneshot::Sender<Result<T>>,
        r: Result<T>,
        engine: Lease,
        on_durable: impl FnOnce(),
    ) {
        let r = match engine.sync_owned().await {
            Ok(_durable) => r,
            Err(e) => Err(ActorError::from(e)),
        };
        if r.is_ok() {
            on_durable();
        }
        let fenced = r.as_ref().err().is_some_and(ActorError::is_fenced);
        let _ = reply.send(r);
        if fenced {
            self.fenced.notify_one();
        }
    }
}

/// Tasks of **the whole term** — the drainer, the rate loop, the safety net. One dying means
/// the term loses a leg; quiesce stops all three before waiting for work in flight.
struct TermTasks {
    drain: tokio::task::AbortHandle,
    rate: tokio::task::AbortHandle,
    rescue: tokio::task::AbortHandle,
    /// The chunked VO lock rebuild, if it runs within the serve loop: dying mid-way means
    /// the term never grants a slot — step down.
    restore: Option<tokio::task::AbortHandle>,
}

impl TermTasks {
    fn contains(&self, id: tokio::task::Id) -> bool {
        [self.drain.id(), self.rate.id(), self.rescue.id()].contains(&id)
            || self.restore.as_ref().is_some_and(|r| r.id() == id)
    }

    fn abort(&self) {
        self.drain.abort();
        self.rate.abort();
        self.rescue.abort();
        if let Some(r) = &self.restore {
            r.abort();
        }
    }
}

/// The serve loop while **being** leader.
///
/// This loop does nothing long: it receives commands, spawns tasks, and goes back to
/// receiving. Everything that touches the engine is in a task, and every task must go
/// through [`Lease`] — the only place the engine changes hands. Commands are dispatched by
/// [`on_command`]; the `select!` loop here only coordinates.
#[allow(clippy::too_many_arguments)]
pub(super) async fn serve(
    sh: &Shared,
    range: RangeEntry,
    retiring: &AtomicBool,
    budget: Option<&crate::engine_pool::TurnBudget>,
    keeper: Option<&LeaseKeeper>,
    rx: &mut mpsc::Receiver<Command>,
    run_rx: &mut mpsc::UnboundedReceiver<crate::drainer::RunRequest>,
    tick_interval: Option<Duration>,
    mut health: cairn_store::WriterHealth,
    stall_after: Option<Duration>,
    restore: Option<tokio::time::Instant>,
) -> Step {
    // This range is led here: the load statistics count written bytes by this term's
    // writer, and `/ranges/stats` lists it.
    let _load = sh.load.lead(health.clone());
    // Watch for `durable_seq` standing still while writes are not yet durable. The watch
    // tick is a quarter of the cap, so stepping down happens at the latest at 1.25× the cap.
    let mut stall_check =
        stall_after.map(|d| tokio::time::interval((d / 4).max(Duration::from_millis(50))));
    let mut stalled: Option<(u64, tokio::time::Instant)> = None;
    // This range's share of the node's turn cap changes when the node gains/loses
    // leadership of another range (D-c).
    let mut budget_changes = budget.map(|b| b.changes());
    // Subscribe, then read: the share captured when building the scheduler may be stale —
    // another range became leader while this one was recovering (`restore_flow`,
    // `recover`), and `changes()` marks the value at subscription time as seen.
    if let Some(b) = budget {
        sh.scheduler.attach_node_budget(b);
        sh.scheduler.set_max_turns(b.share());
    }
    // Background tick: wake at the **nearest deadline** of the reschedule table, or at the
    // tick if no deadline is earlier — the tick is a safety net, no longer the timer
    // precision. The first tick runs right away: it scans and rebuilds this term's
    // deadline.
    //
    // The next tick is measured from when the previous one fired, like
    // `MissedTickBehavior::Delay`: one long-running tick must not pile up into a burst.
    let mut next_tick = tokio::time::Instant::now();
    let mut tasks: JoinSet<()> = JoinSet::new();
    // The background tick must not overlap itself: the tick runs in a task, and two ticks
    // piling on each other is exactly the storm `MissedTickBehavior::Delay` exists to
    // avoid. (A tick no longer has any engine release point, so two overlapping ticks do
    // not deliver duplicate timers — this flag is now only a saving.)
    let ticking = Rc::new(Cell::new(false));
    let term = spawn_term_tasks(sh, &mut tasks, restore);
    // Cap on waiting for work in flight of a quiesce — set when the command is received.
    let mut quiesce_grace = Duration::ZERO;
    // The L0 throttle tick: only runs while throttling or with deferred new work — a range
    // that is not throttled pays no wake-up at all.
    let mut l0_tick: Option<tokio::time::Instant> = None;

    let mut step = loop {
        // Lease loss is detected in **two** places, and both are needed. Here is layer 1:
        // the lease layer reports early, before spending I/O. Layer 2 is the `Fenced` error
        // returned by the commit itself — the only thing that catches a node just waking
        // from a GC pause, since it knows nothing it could report by itself. Layer 2 now
        // goes through `sh.fenced`, because a task cannot leave the loop by itself.
        let cmd = tokio::select! {
            biased;

            _ = sh.fenced.notified() => break Step::LostLeadership(AfterSeal::default()),
            _ = async { keeper.expect("guarded branch").lost().await }, if keeper.is_some() => {
                break Step::LostLeadership(AfterSeal::default());
            }
            // The store closed under our feet: fenced (another leader came up) or failed for
            // good (a WAL flush error past its retries). That error is not `Fenced`, so
            // nothing used to bring the partition down — it stayed on as a leader that could
            // write nothing, with its lease still renewed regularly.
            reason = health.closed() => break on_store_closed(reason),
            _ = async { stall_check.as_mut().expect("guarded branch").tick().await }, if stall_check.is_some() => {
                match on_stall_check(&health, &mut stalled, stall_after) {
                    Some(step) => break step,
                    None => continue,
                }
            }
            r = async { budget_changes.as_mut().expect("guarded branch").changed().await },
                if budget_changes.is_some() =>
            {
                match (r, budget) {
                    // The share changed, or the node just got room again.
                    (Ok(()), Some(b)) => sh.scheduler.node_turns_changed(b.share()),
                    _ => budget_changes = None,
                }
                continue;
            }
            () = tokio::time::sleep_until(wake_at(next_tick, sh.next_due.get(), sh.clock.now())),
                if tick_interval.is_some() && !ticking.get() =>
            {
                // Drop the deadline **before** scanning: the scan rebuilds it from the store,
                // and any write in between lowers it by itself. Without dropping it, a past
                // deadline that the scan picks nothing up for (the row of an invocation in
                // the middle of a turn) would wake this loop continuously.
                sh.next_due.clear();
                spawn_background_tick(&mut tasks, sh, &ticking);
                next_tick = tokio::time::Instant::now() + tick_interval.expect("guarded branch");
                continue;
            }
            // The deadline was just lowered (the engine wrote an earlier timer): recompute
            // the wake-up time.
            () = sh.next_due.lowered(), if tick_interval.is_some() => continue,
            // L0 throttle: take a sample so the throttle level can come down even with no
            // commands, and nudge the drainer for deferred `StartCall`s/child calls.
            // (The branch expression is still evaluated when the guard is off — so no
            // `expect`.)
            () = tokio::time::sleep_until(l0_tick.unwrap_or_else(tokio::time::Instant::now)), if l0_tick.is_some() => {
                let (nudge, active) = sh.l0.tick();
                if nudge {
                    sh.drainer.nudge();
                }
                l0_tick = active.then(|| tokio::time::Instant::now() + sh.l0.config().defer_retry);
                continue;
            }
            () = sh.l0.woken(), if l0_tick.is_none() => {
                l0_tick = Some(tokio::time::Instant::now() + sh.l0.config().defer_retry);
                continue;
            }
            // A task finishing is when `ticking` may just have changed, so the loop must
            // wake to re-evaluate the guards above. It is also where the `JoinSet` is
            // reaped instead of growing with the number of commands served.
            done = tasks.join_next(), if !tasks.is_empty() => {
                match on_task_done(done, &term) {
                    Some(step) => break step,
                    None => continue,
                }
            }

            // An invocation the drain loop just emitted. It is already durable and already
            // in the queue; what remains is **waiting for its own slot**, and that is the
            // job of a separate task — not of the drain loop.
            //
            // Placed before `rx.recv()` in the `biased` order: work already received should
            // get done, not queue behind a stream of new requests.
            // A parked ticket just granted a slot: its turn starts before new work — it holds
            // a slot.
            () = sh.scheduler.tickets_changed() => {
                spawn_granted(sh, &mut tasks);
                continue;
            }
            // The intake gate is bounded (`drainer::ADMIT_CAP`): when full, the request stays
            // in the channel (data) until a task passes the gate.
            Some(req) = run_rx.recv(), if sh.runner.has_room() => {
                spawn_run(sh, &mut tasks, req);
                continue;
            }
            () = sh.runner.room(), if !sh.runner.has_room() => continue,

            cmd = rx.recv() => cmd,
        };

        let Some(cmd) = cmd else {
            break Step::Stop(AfterSeal {
                stop: true,
                ..AfterSeal::default()
            });
        };
        if let Some(step) = on_command(sh, &mut tasks, &range, retiring, cmd, &mut quiesce_grace) {
            break step;
        }
    };

    // **Quiesce: wait for work in flight, bounded** — see [`quiesce_in_flight`].
    let drain_started = tokio::time::Instant::now();
    if let Step::Quiesce(after) = &mut step {
        quiesce_in_flight(sh, &mut tasks, rx, after, &term, quiesce_grace).await;
    }

    // **No task may live past this point.** `leadership_loop` is about to take the engine
    // out of the lock to seal it, and a task still holding a `Lease` would make that fail
    // — the node would hand over leadership while the old writer can still write.
    //
    // `shutdown` cancels rather than waits, and that is right: losing leadership means
    // every further commit will be fenced, so waiting is only waiting for a chain of
    // errors. Cancelling mid-way is safe because each `step` commits whole or commits
    // nothing — being cancelled between two commits is exactly like a `kill -9` between
    // two commits, which is the case `recover` has always handled (INV-3).
    //
    // The wake-up duty of a turn cancelled here belongs to the next term's `recover`
    // (INV-3): close the safety net first, so they do not become marks of a term that has
    // ended (the default nack counts forgotten reschedules, not step-downs).
    sh.rescue.close();
    tasks.shutdown().await;
    // Granted but not yet spawned tickets hold the slot return path to this very
    // scheduler: drop them here. The invocation of every ticket is still durable — the next
    // term's `recover` hands it over again.
    sh.scheduler.close();
    if matches!(step, Step::Quiesce(_)) {
        sh.stats.update(range.id.0, |t| {
            t.drain_ms = Some(crate::range_stats::ms(drain_started.elapsed()));
        });
    }
    step
}

/// Build the term's tasks, plus the start-of-term recovery.
fn spawn_term_tasks(
    sh: &Shared,
    tasks: &mut JoinSet<()>,
    restore: Option<tokio::time::Instant>,
) -> TermTasks {
    // **Rebuild the VO locks in chunks while serving** (FL-8) — `Some(start time)` ⇒ the
    // rebuild did not run before this loop (`leadership_loop`). On failure ⇒ nobody knows
    // who holds which lock: step down, as when it runs before the loop (LS-9); the gate
    // never opens in this term.
    let restore = restore.map(|restore_started| {
        let sh = sh.clone();
        // Start-of-term work: the "commands before background work" window
        // (`lease::EngineLock`) stays open until it and the first `recover` are done.
        let opening = sh.engine.opening_job();
        tasks.spawn_local(crate::lease::holding_as("restore", async move {
            let _opening = opening;
            if let Err(e) = restore_flow(&sh, restore_started).await {
                if !e.is_fenced() {
                    tracing::error!(error = %e, "cannot recover the queue — yielding leadership");
                }
                sh.fenced.notify_one();
            }
        }))
    });
    // **The partition's drainer, and only one.** It lives exactly as long as this leader
    // term: `tasks` is `shutdown` on stepping down, so there is no way for two terms to
    // have two drainers overlapping on the same store.
    //
    // It takes no drain lock. What keeps two drain turns from picking up the same row is
    // that there is only one reader — see [`crate::drainer`].
    let drain = {
        let sh = sh.clone();
        tasks.spawn_local(crate::lease::holding_as("drain", async move {
            drain_loop(sh).await
        }))
    };
    // The rate tier's clock: tokens come with time, not with any release — this loop wakes
    // the token waiters. Stops together with the drainer on quiesce.
    let rate = {
        let scheduler = sh.scheduler.clone();
        tasks.spawn_local(async move { scheduler.rate_loop().await })
    };
    // The term's safety net: sleeps until an error path marks it, then reconciles against
    // the durable state. Stops together with the drainer on quiesce — it only hands work
    // over, and work not yet handed over is still durable for the next leader.
    let rescue = {
        let sh = sh.clone();
        tasks.spawn_local(crate::lease::holding_as("rescue", async move {
            rescue_loop(sh).await
        }))
    };
    // **Every becoming-leader must recover again** (see [`recover`]) — but in a task, not
    // before this loop: the scan may be long, and the serve loop must answer commands
    // (`Resign`, `DeploymentsChanged`, reads) meanwhile. It only hands work over, so running
    // in parallel with commands is right.
    if !sh.registry.is_empty() && !sh.reconcile_pending.replace(true) {
        let sh = sh.clone();
        let opening = sh.engine.opening_job();
        tasks.spawn_local(crate::lease::holding_as("recover", async move {
            let _opening = opening;
            recover(&sh).await
        }));
    }
    // No start-of-term work (the rebuild already ran before this loop, the deployment
    // registry is empty): the priority window ends right away.
    sh.engine.end_precedence_if_idle();
    // Background build of the query indexes: once per term, until every descriptor is
    // `readable` and no rows of removed codes remain. Not inside the split window (the
    // child range's term starts after that) and does not block opening.
    {
        let sh = sh.clone();
        tasks.spawn_local(crate::lease::holding_as("query_build", async move {
            query_build_loop(sh).await
        }));
    }
    // The range's artifact byte counter: once per term, only when some tenant carries
    // `max-artifact-bytes=`.
    {
        let sh = sh.clone();
        // The `query_build` label: the same kind of work (building counters in chunks), and
        // it is background work — it yields to commands at the start of the term.
        tasks.spawn_local(crate::lease::holding_as("query_build", async move {
            artifact_bytes_loop(sh).await
        }));
    }
    TermTasks {
        drain,
        rate,
        rescue,
        restore,
    }
}

/// The term's store has closed — because of a fence or because it failed for good.
fn on_store_closed(reason: Option<slatedb::CloseReason>) -> Step {
    match reason {
        Some(slatedb::CloseReason::Fenced) => Step::LostLeadership(AfterSeal::default()),
        other => {
            tracing::error!(reason = ?other, "partition store closed — stepping down");
            Step::StoreFailed
        }
    }
}

/// One watch tick of `durable_seq`: `Some(StoreFailed)` if it has stood still past the cap
/// while writes are not yet durable.
fn on_stall_check(
    health: &cairn_store::WriterHealth,
    stalled: &mut Option<(u64, tokio::time::Instant)>,
    stall_after: Option<Duration>,
) -> Option<Step> {
    match (health.lag(), *stalled) {
        (None, _) => *stalled = None,
        (Some((_, durable)), Some((seen, since))) if seen == durable => {
            let limit = stall_after.expect("guarded branch");
            if since.elapsed() >= limit {
                tracing::error!(
                    stalled_ms = since.elapsed().as_millis() as u64,
                    "write not durable and durable_seq stalled past the limit — stepping down"
                );
                return Some(Step::StoreFailed);
            }
        }
        (Some((_, durable)), _) => *stalled = Some((durable, tokio::time::Instant::now())),
    }
    None
}

/// A task of the term just finished.
///
/// A task panic is now **silent**: its `reply` is dropped and the caller only sees
/// `Stopped`. When everything ran in place, a panic killed the engine thread — noisy but
/// impossible to hide. Say it loudly here so stability is not traded for invisibility. A
/// task running a turn has already marked the safety net for its id: the wake-up duty
/// ([`crate::drainer::WakeDuty`]) falls unacked during unwinding.
///
/// The drainer, the rate loop and the safety net are work of **the whole term**: one dying
/// means the term loses a leg (nobody drains the outbox, nobody rescues) — step down and
/// win again, which rebuilds every task of the term from scratch.
fn on_task_done(
    done: Option<std::result::Result<(), tokio::task::JoinError>>,
    term: &TermTasks,
) -> Option<Step> {
    if let Some(Err(e)) = done {
        if e.is_panic() {
            tracing::error!(error = %e, "a command task panicked");
        }
        if term.contains(e.id()) {
            tracing::error!("a term task (drain/rate/rescue net) died — stepping down to rebuild");
            return Some(Step::LostLeadership(AfterSeal::default()));
        }
    }
    None
}

/// Spawn the turn of an invocation the drain loop just emitted — through the intake gate
/// (`Runner::admit`): the caller only takes the request when the gate has room.
fn spawn_run(sh: &Shared, tasks: &mut JoinSet<()>, req: crate::drainer::RunRequest) {
    let sh = sh.clone();
    // The guard is created **before** the spawn: a task cancelled before even its first
    // poll still returns the turn and marks the safety net (nack by default).
    let done = sh.runner.running(&req, &sh.rescue);
    let admit = sh.runner.admit();
    let alive = sh.scheduler.turn_task();
    // A turn handed over by the scheduler: FIFO at the engine lock (`lease::runner_turn`).
    tasks.spawn_local(crate::lease::runner_turn(crate::lease::holding_as(
        "turn",
        async move {
            let _alive = alive;
            run_dispatched(&sh, req, done, admit).await
        },
    )));
}

/// A parked ticket just granted a slot, or the invocation lock of a nacked ticket just
/// released: tickets waiting for the lock ask again, each granted ticket gets a turn task.
fn spawn_granted(sh: &Shared, tasks: &mut JoinSet<()>) {
    sh.scheduler.requeue_freed();
    for g in sh.scheduler.take_ready() {
        let sh = sh.clone();
        let alive = sh.scheduler.turn_task();
        tasks.spawn_local(crate::lease::runner_turn(crate::lease::holding_as(
            "turn",
            async move {
                let _alive = alive;
                run_granted(&sh, g).await
            },
        )));
    }
}

/// **Quiesce: wait for work in flight, bounded.** The drainer stops right away — it only
/// emits new work, and everything it has not emitted is still in the state tables (INV-3)
/// for the next leader. Turns **running** (holding a turn slot) are allowed to finish up
/// to `grace`; past the cap they are cancelled as on stepping down. Tasks only **waiting
/// for a slot** are not waited for: they have done nothing, their invocation is durable in
/// the queue, and what they wait on (the VO lock of a suspended invocation) may only open
/// thanks to the very drainer just stopped. The same goes for parked tickets (FL-30) —
/// including tickets just granted that have not become turns: their slot is returned right
/// away ([`crate::flow::Scheduler::retire_ready`]), otherwise the turn cap gets stuck on
/// slots nobody runs and quiesce waits out its full cap. Commands arriving during the wait
/// do not start work: aimed at a key they get `WrongRange`, the rest are queued into
/// `after`.
async fn quiesce_in_flight(
    sh: &Shared,
    tasks: &mut JoinSet<()>,
    rx: &mut mpsc::Receiver<Command>,
    after: &mut AfterSeal,
    term: &TermTasks,
    quiesce_grace: Duration,
) {
    term.abort();
    // Close the slot gate first: slots returned from here on are granted to nobody — no new
    // turn starts in the ending term, and returning the slots of granted tickets does not
    // grant the next ticket in a cascade.
    sh.scheduler.close_gate();
    // Parked tickets are not waited for: a slot granted to a ticket that has not become a
    // turn is returned — here and every time a running turn releases a slot to a ticket.
    sh.scheduler.retire_ready();
    let deadline = tokio::time::Instant::now() + quiesce_grace;
    let mut poll = tokio::time::interval(Duration::from_millis(20));
    loop {
        if sh.scheduler.turns_in_use() == 0 {
            break;
        }
        // `biased`, the order is deliberate: a task just finished is counted first (a turn
        // finishing right as the cap runs out is a finished turn, not a cancelled one); then
        // the cap — a flood of commands cannot push it back; then commands; the poll tick
        // last, since whichever branch fires, the top of the loop polls again. Tasks are
        // finite, so the first branch does not starve the cap.
        tokio::select! {
            biased;
            done = tasks.join_next() => if done.is_none() { break },
            () = tokio::time::sleep_until(deadline) => {
                tracing::warn!(
                    remaining = tasks.len(),
                    "quiesce exceeded {quiesce_grace:?} — cancelling turns still running"
                );
                break;
            }
            cmd = rx.recv() => {
                let closed = cmd.is_none();
                after.take(cmd, true);
                if closed {
                    break;
                }
            }
            () = sh.scheduler.tickets_changed() => sh.scheduler.retire_ready(),
            _ = poll.tick() => {}
        }
    }
}
