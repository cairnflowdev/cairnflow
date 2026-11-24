// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The shell of the lifecycle core (`cairn_lifecycle`).
//!
//! One call into the core is: **read an [`lc::View`] → `step(view, event)` → execute the
//! [`lc::Plan`] → report the next event**. This file holds three jobs that each turn path
//! used to do in its own way:
//!
//! - answer [`lc::Need`] with engine reads ([`Asker::ask`]) — in the same order, with the
//!   same number of reads the core's branch needs;
//! - write the durable part of the Plan (stage, retry row) in **one** commit ([`commit`] →
//!   `Engine::apply_lifecycle`);
//! - emit the side-resource effects [`lc::Side`] ([`side`]) — a no-op today (streams, and
//!   blobs once their feature gate opens, are closed or have attempts marked through here,
//!   never by calling the primitives directly).
//!
//! The Plan's RAM work (handing over a turn, releasing a slot, acking the wake-up duty)
//! lives at the call site, because it is tied to that site's guards (`TurnPermit`,
//! `WakeDuty`, `Claim`) — the general rule: only after the commit succeeds.

use {super::*, cairn_engine::RowWrite, cairn_lifecycle as lc};

/// Ask the core (also for the shell outside `turn/` — `actor::rescue_round`).
///
/// No flags: production is the core itself (`cairn_lifecycle::Bugs::PRODUCTION ==
/// Bugs::NONE`). The shell was wired in by a series of `refactor:` commits that asked the
/// core with the old behaviour (`Bugs::BEFORE_STEP_3`), then turned each flag off with a
/// `test:`/`fix:` pair.
pub(crate) fn decide(view: &lc::View, event: &lc::Event) -> lc::Plan {
    lc::step(view, event)
}

/// P1 (durable status) compacted for the core (the engine's mapping — one place for the
/// shell and the testkit).
pub(crate) fn status_of(s: Option<&InvocationStatus>) -> lc::Status {
    cairn_engine::lifecycle_status(s)
}

pub(crate) fn stage_to(s: lc::Stage) -> Stage {
    match s {
        lc::Stage::Inbox => Stage::Inbox,
        lc::Stage::Running => Stage::Running,
        lc::Stage::Suspended => Stage::Suspended,
        lc::Stage::Paused => Stage::Paused,
    }
}

/// P2 (queue slot) compacted for the core.
pub(crate) fn slot_of(s: &VQueueSlot) -> lc::Slot {
    cairn_engine::lifecycle_slot(s)
}

/// The node parameters the core needs.
pub(crate) fn knobs(ctx: &Ctx<'_>) -> lc::Knobs {
    lc::Knobs {
        defer_ms: busy_defer_ms(ctx),
        watch_after_ms: watch_after_ms(ctx),
        parked_defer_ms: lc::PARKED_DEFER_MS,
    }
}

pub(crate) fn wake_kind(w: &Wake) -> lc::WakeKind {
    match w {
        Wake::Completion(_) => lc::WakeKind::Completion,
        Wake::Signal(_) => lc::WakeKind::Signal,
    }
}

/// Turn `turn` together with the wake-up `pending` (if any) it carries.
pub(crate) fn carrying(turn: lc::Turn, pending: Option<&(Wake, Source)>) -> lc::Turn {
    lc::Turn {
        wake: pending.map(|(w, _)| wake_kind(w)),
        ..turn
    }
}

/// **The shell's only "busy" question**: every path builds [`lc::Attendance`] here, then
/// the core decides from it — no place asks `Locks`/`Runner` by itself any more to learn
/// whether it is busy. Ask, **do not win**, leave no "someone is waiting for the lock" mark
/// (background tick, `recover`, safety net: each holds the engine for the whole tick, so no
/// task can win the lock between the question and the write).
///
/// Busy = the invocation lock is held (a turn in flight or waiting for a slot) **or** there
/// is a handed-over turn not yet finished (`Runner::is_queued`: its task may not hold the
/// lock yet — FL-20).
pub(crate) fn attendance(
    locks: &Locks,
    runner: &crate::drainer::Runner,
    id: &InvocationId,
) -> lc::Attendance {
    lc::Attendance {
        lock_held: locks.is_busy(id),
        queued: runner.is_queued(id),
        ..lc::Attendance::IDLE
    }
}

/// [`attendance`] for a caller that **already holds** `id`'s lock (the drainer's "write,
/// then hand over", the safety net writing `paused`): the lock is not busy because of
/// anyone else.
pub(crate) fn attendance_holding(
    runner: &crate::drainer::Runner,
    id: &InvocationId,
) -> lc::Attendance {
    lc::Attendance {
        queued: runner.is_queued(id),
        ..lc::Attendance::IDLE
    }
}

/// Win `id`'s lock **if free**, without waiting — for places that must write under the
/// lock (the drainer, the safety net, poison rows). Comes with [`attendance`]'s "busy"
/// answer: a missed attempt ⇒ `lock_held`; won ⇒ like [`attendance_holding`]. On a miss
/// `Locks` puts the id into the wait list and nudges the drainer when the holder releases.
pub(crate) fn claim<'c>(
    locks: &'c Locks,
    runner: &crate::drainer::Runner,
    id: InvocationId,
) -> (Option<InvGuard<'c>>, lc::Attendance) {
    let guard = locks.try_invocation(id);
    let attendance = lc::Attendance {
        lock_held: guard.is_none(),
        ..attendance_holding(runner, &id)
    };
    (guard, attendance)
}

/// A bare View for the questions of the start of a term and of the safety net (no node
/// parameter needed).
fn bare(attendance: lc::Attendance) -> lc::View {
    lc::View::new(0, lc::Knobs::DEFAULT, attendance)
}

/// `restore_flow` for one entry of the queue snapshot (lifecycle core `Event::LeaderUp`,
/// FL-8): read the state only when the core needs it (an entry holding the lock of an
/// EXCLUSIVE queue).
pub(crate) async fn leader_up(
    engine: &mut Lease,
    row: &cairn_engine::VQueueRow,
) -> Result<lc::Plan> {
    let mut view = bare(lc::Attendance::IDLE).with_slot(Some(slot_of(&row.slot)));
    loop {
        let plan = decide(&view, &lc::Event::LeaderUp);
        match plan.need {
            None => return Ok(plan),
            Some(lc::Need::Status) => {
                view.status = Some(status_of(engine.status(&row.id).await?.as_ref()));
            }
            Some(other) => unreachable!("LeaderUp does not read {other:?}"),
        }
    }
}

/// `recover` for one `running` invocation of one chunk of the scan (lifecycle core
/// `Event::Recover`): if there is a retry reschedule, that reschedule is what runs it on.
///
/// The retry row is read when the core needs it (`Need::Retry` — only ids nobody is working
/// on), **within the same engine hold as the hand-over**: a turn slipping in between two
/// chunks may set a new reschedule, so a set captured before the scan
/// (`Engine::pending_retries_among`, FL-18) could be wrong for a later chunk. On a read
/// failure the core decides: write authority lost ⇒ `Reply::Abort`; other errors ⇒ run
/// (running one extra time beats forgetting it).
pub(crate) async fn recover_one(
    engine: &Lease,
    attendance: lc::Attendance,
    id: &InvocationId,
) -> lc::Plan {
    let mut view = bare(attendance).with_status(lc::Status::Running);
    loop {
        let plan = decide(&view, &lc::Event::Recover);
        match plan.need {
            None => return plan,
            Some(lc::Need::Retry) => match engine.retry_row(id).await.map_err(ActorError::from) {
                Ok(r) => view.retry = Some(r),
                Err(e) => view.read_error = Some((lc::Need::Retry, read_err(&e))),
            },
            Some(other) => unreachable!("Recover does not read {other:?}"),
        }
    }
}

/// A failed read of the reconciliation round: the core knows how to handle it (back off,
/// drop the round).
pub(crate) fn read_err(e: &ActorError) -> lc::ReadErr {
    if e.is_fenced() {
        lc::ReadErr::Fenced
    } else {
        lc::ReadErr::Other
    }
}

/// A bare View of the safety net for one id (lifecycle core `Event::RescueRound`).
pub(crate) fn rescue_view(attendance: lc::Attendance) -> lc::View {
    bare(attendance)
}

/// The outcome the SDK reports, for [`lc::Event::TurnEnded`]. `Throttled` and
/// `AlreadyInvoked` cannot reach the end of a turn: the throttle comes before `Running` and
/// the ingress path has no turn; a preflight refusal because the tenant is suspended ends
/// with the core's own plan (`Ran::Settled`), not through `TurnEnded`.
pub(crate) fn outcome_of(o: &InvokeOutcome) -> lc::Outcome {
    match o {
        InvokeOutcome::Completed(_) => lc::Outcome::Completed,
        InvokeOutcome::Failed { .. } => lc::Outcome::Failed,
        InvokeOutcome::Paused { .. } => lc::Outcome::Paused,
        InvokeOutcome::Waiting => lc::Outcome::Waiting,
        InvokeOutcome::Retry { delay_ms, .. } => lc::Outcome::Retry {
            delay_ms: *delay_ms,
        },
        InvokeOutcome::Throttled { .. } | InvokeOutcome::AlreadyInvoked => {
            unreachable!("{o:?} does not end a turn that entered `Running`")
        }
    }
}

/// The one asking the core for **one** invocation: the View being built, together with the
/// full version of what has been read (the shell builds answers from it, no rereads).
pub(crate) struct Asker {
    pub view: lc::View,
    /// The full state read (`Some(None)` = no invocation).
    pub status: Option<Option<InvocationStatus>>,
    /// The queue entry read (`Some(None)` = none).
    pub slot: Option<Option<VQueueSlot>>,
    /// A read error returned to the core instead of `?` (paths that know how to handle read
    /// errors: the safety net, `recover`, the reschedule after an error).
    tolerant: bool,
    failed: Option<ActorError>,
    /// The completion the drainer is delivering (`Need::Waiting`): the wait tree is
    /// evaluated before and after it. `None` ⇒ "unknown".
    arriving: Option<u32>,
    /// The caller's machine that the `Need::Waiting` read loaded (warm machine or journal)
    /// — the delivery path reuses it, no second load.
    pub machine: Option<std::result::Result<Invocation, cairn_engine::EngineError>>,
    /// The range's streams — the Plan executor applies `Side` to them ([`side`]).
    pub streams: std::sync::Arc<crate::streams::RangeStreams>,
}

impl Asker {
    pub(crate) fn new(now_ms: u64, ctx: &Ctx<'_>, attendance: lc::Attendance) -> Self {
        Self {
            view: lc::View::new(now_ms, knobs(ctx), attendance),
            status: None,
            slot: None,
            tolerant: false,
            failed: None,
            arriving: None,
            machine: None,
            streams: std::sync::Arc::clone(ctx.streams),
        }
    }

    /// The drainer is delivering completion `id` (for `Need::Waiting`).
    pub(crate) fn arriving(mut self, completion: u32) -> Self {
        self.arriving = Some(completion);
        self
    }

    /// Read errors become `view.read_error` for the core to decide.
    pub(crate) fn tolerant(mut self) -> Self {
        self.tolerant = true;
        self
    }

    /// State already read at the call site.
    pub(crate) fn with_status(mut self, s: Option<InvocationStatus>) -> Self {
        self.view.status = Some(status_of(s.as_ref()));
        self.status = Some(s);
        self
    }

    /// Retry row already known at the call site (just scanned, or the turn itself wrote it).
    pub(crate) fn with_row(mut self, r: Option<lc::RetryRow>) -> Self {
        self.view.retry = Some(r);
        self
    }

    /// Treat the `need` read as having failed with `e` (a test point placed before the read).
    pub(crate) fn fail(&mut self, need: lc::Need, e: ActorError) {
        let kind = if e.is_fenced() {
            lc::ReadErr::Fenced
        } else {
            lc::ReadErr::Other
        };
        self.view.read_error = Some((need, kind));
        self.failed = Some(e);
    }

    /// The read error returned to the core, for logging.
    pub(crate) fn failed_text(&self) -> String {
        self.failed
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    }

    /// Due time of the current retry row, if known — for the commit that deletes the old
    /// index row.
    pub(crate) fn row_due(&self) -> Option<u64> {
        self.view.retry.flatten().map(|r| r.due)
    }

    /// Ask the core about `event`, reading every cell it asks for.
    pub(crate) async fn ask(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'_>,
        id: InvocationId,
        event: lc::Event,
    ) -> Result<lc::Plan> {
        loop {
            let plan = decide(&self.view, &event);
            let Some(need) = plan.need else {
                return Ok(plan);
            };
            if self.view.read_error.is_some_and(|(n, _)| n == need) {
                // The core does not handle errors of this read: return the original error
                // like `?`.
                return Err(self.failed.take().expect("read error was recorded"));
            }
            match self.read(engine, ctx, id, need).await {
                Ok(()) => {}
                Err(e) if self.tolerant => self.fail(need, e),
                Err(e) => return Err(e),
            }
        }
    }

    async fn read(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'_>,
        id: InvocationId,
        need: lc::Need,
    ) -> Result<()> {
        match need {
            lc::Need::Status => {
                let s = engine.status(&id).await?;
                self.view.status = Some(status_of(s.as_ref()));
                self.status = Some(s);
            }
            lc::Need::Slot => {
                let s = engine.vqueue_of(&id).await?;
                self.view.slot = Some(s.as_ref().map(slot_of));
                self.slot = Some(s);
            }
            lc::Need::Retry => self.view.retry = Some(engine.retry_row(&id).await?),
            lc::Need::JournalLen => self.view.journal_len = Some(engine.journal_len(&id).await?),
            lc::Need::Contract => self.view.contract = Some(contract_of(engine, ctx, id).await?),
            lc::Need::Policy { step } => {
                let d = retry_policy(engine, ctx, &id, step).await?;
                self.view = self.view.with_policy(step, d);
            }
            lc::Need::Waiting => {
                let w = self.waiting(engine, id).await?;
                self.view = self.view.with_waiting(w);
            }
        }
        Ok(())
    }

    /// The wait tree of `id`'s last suspension (engine RAM) evaluated against the caller
    /// machine's set of delivered completions, before and after the incoming completion. No
    /// tree, or the machine cannot be loaded ⇒ "unknown" (the core wakes as before).
    async fn waiting(&mut self, engine: &mut Lease, id: InvocationId) -> Result<lc::Waiting> {
        let (Some(c), Some(tree)) = (self.arriving, engine.waiting_tree(&id).cloned()) else {
            return Ok(lc::Waiting::Unknown);
        };
        let machine = super::replayed(engine, id).await?;
        let w = match &machine {
            Ok(inv) => {
                // Look up the machine's set (O(log n) per leaf), building no array: a
                // `ctx.all` tree with a thousand leaves is evaluated on every completion.
                let signals = cairn_runtime::SettledSignals {
                    signals: inv.settled_signals(),
                };
                let before = cairn_runtime::suspension_satisfied_by(
                    &tree,
                    &|id| inv.is_settled(id),
                    signals,
                );
                let after = cairn_runtime::suspension_satisfied_by(
                    &tree,
                    &|id| id == c || inv.is_settled(id),
                    signals,
                );
                lc::Waiting::Known { before, after }
            }
            Err(_) => lc::Waiting::Unknown,
        };
        self.machine = Some(machine);
        Ok(w)
    }
}

/// The preflight contract of `id` from the deployment it is bound to (the path that has not
/// loaded the invocation; `drive_one`'s preflight fills this cell from the loaded copy).
async fn contract_of(engine: &mut Lease, ctx: &Ctx<'_>, id: InvocationId) -> Result<lc::Contract> {
    let inv = engine.load(id, params_for(&id)).await?;
    Ok(preflight_contract(
        &pinned_deployment(ctx.deployments, &inv),
        &inv.target,
    ))
}

/// The View's `contract` cell from the invocation's deployment (`pinned_deployment`).
pub(crate) fn preflight_contract(
    pinned: &std::result::Result<Option<&Deployment>, String>,
    target: &Target,
) -> lc::Contract {
    let contract = pinned
        .as_ref()
        .ok()
        .copied()
        .flatten()
        .map(|d| d.contract(&target.service, &target.handler));
    lc::Contract {
        pinned: match pinned {
            Ok(Some(_)) => lc::Pinned::Found,
            Ok(None) => lc::Pinned::Undeclared,
            Err(_) => lc::Pinned::NotLoaded,
        },
        max_attempts: contract.as_ref().and_then(|c| c.retry_policy_max_attempts),
        on_max: match contract
            .as_ref()
            .map_or(crate::discovery::OnMaxAttempts::Pause, |c| {
                c.on_max_attempts()
            }) {
            crate::discovery::OnMaxAttempts::Pause => lc::OnMax::Pause,
            crate::discovery::OnMaxAttempts::Kill => lc::OnMax::Kill,
        },
    }
}

/// Turn the Plan's retry row into an engine write; `old_due` is the due time of the current
/// row if the shell knows it.
pub(crate) fn row_write(change: lc::RowChange, old_due: Option<u64>) -> RowWrite {
    match change {
        lc::RowChange::Keep => RowWrite::Keep,
        lc::RowChange::Set(row) => RowWrite::Set { old_due, row },
        lc::RowChange::Clear => RowWrite::Clear { old_due },
    }
}

pub(crate) fn stage_write(change: lc::StageChange) -> cairn_engine::StageWrite {
    match change {
        lc::StageChange::Keep => cairn_engine::StageWrite::Keep,
        lc::StageChange::Move(s) => cairn_engine::StageWrite::Move(stage_to(s)),
        lc::StageChange::Remove => cairn_engine::StageWrite::Remove,
        // Building an entry needs the queue and `seq` — a separate path (`ensure_queued`,
        // `start_invocation`), not through here.
        lc::StageChange::Insert(_) => unreachable!("Insert goes through ensure_queued/create"),
    }
}

/// The durable part of `plan` (stage, retry row) in one commit. `old_due`: the due time of
/// the current row known to the caller (just scanned, or the turn itself wrote it), if the
/// core does not read it.
pub(crate) async fn commit(
    engine: &mut Lease,
    id: InvocationId,
    asker: &Asker,
    plan: &lc::Plan,
    old_due: Option<u64>,
) -> Result<()> {
    if plan.stage == lc::StageChange::Keep && plan.retry == lc::RowChange::Keep {
        return Ok(());
    }
    engine
        .apply_lifecycle(
            &id,
            asker.slot.as_ref(),
            stage_write(plan.stage),
            row_write(plan.retry, asker.row_due().or(old_due)),
        )
        .await?;
    Ok(())
}

/// Like [`commit`], plus the error of the attempt that just failed in the **same** commit:
/// with a `failure` there is a commit even when the Plan changes neither stage nor row. The
/// error is diagnostic data for the operator — the core does not read it, so it is not a
/// question of the core; it only has to go with the commit that schedules the next attempt,
/// so that attempt (preflight out of attempts) sees it even if the leader changes in
/// between.
pub(crate) async fn commit_noting(
    engine: &mut Lease,
    id: InvocationId,
    asker: &Asker,
    plan: &lc::Plan,
    old_due: Option<u64>,
    failure: Option<&cairn_store::keys::LastFailure>,
) -> Result<()> {
    let Some(failure) = failure else {
        return commit(engine, id, asker, plan, old_due).await;
    };
    engine
        .apply_lifecycle_noting(
            &id,
            asker.slot.as_ref(),
            stage_write(plan.stage),
            row_write(plan.retry, asker.row_due().or(old_due)),
            failure,
        )
        .await?;
    Ok(())
}

/// The durable part of a Plan that **ends in failure** (`status = Failed`): the state with
/// code and message, the waiters, the cancel cascading to children, the retry row, and the
/// Plan's stage — **one** commit (`Engine::fail_in`). Admin cancel, cancel cascading to
/// children, preflight `Kill`.
pub(crate) async fn fail(
    engine: &mut Lease,
    id: InvocationId,
    code: u32,
    message: &str,
    asker: &Asker,
    plan: &lc::Plan,
) -> Result<cairn_engine::CancelOutcome> {
    debug_assert_eq!(plan.status, Some(lc::Status::Failed));
    let out = engine
        .fail_in(
            &id,
            code,
            message,
            asker.slot.as_ref(),
            stage_write(plan.stage),
        )
        .await?;
    if out == cairn_engine::CancelOutcome::Cancelled {
        side(&asker.streams, &id, plan.side);
    }
    Ok(out)
}

/// The durable part of a Plan that **moves to `paused`** with code and message, together
/// with the Plan's stage and retry row — **one** commit (`Engine::pause_in`). Preflight out
/// of attempts, safety net 577, poison row 577.
pub(crate) async fn pause(
    engine: &mut Lease,
    id: InvocationId,
    code: u32,
    message: &str,
    asker: &Asker,
    plan: &lc::Plan,
) -> Result<bool> {
    debug_assert!(plan.status.is_some_and(lc::Status::is_paused));
    let paused = engine
        .pause_in(
            &id,
            code,
            message,
            asker.slot.as_ref(),
            stage_write(plan.stage),
            row_write(plan.retry, asker.row_due()),
        )
        .await?;
    if paused {
        side(&asker.streams, &id, plan.side);
    }
    Ok(paused)
}

/// The artifacts of `old` go to `new` **before** the restart commit (AP-10): the journal is
/// copied byte for byte, so the refs in it must be readable at `new`, even after `old` is
/// purged. The server-side copy runs with the engine released (D16), then one commit of
/// `new`'s `Artifacts` rows. `continueAsNew` (`keep == 1` with a new input) only copies the
/// artifacts whose hex sha appears in the new input — the rest of the old journal does not
/// come along.
///
/// Dying mid-way: the rerun finds `new`'s rows (if committed) and reuses its epoch — the
/// copy can overwrite. If `new` never comes to be ⇒ its rows and objects leak (a leak only
/// costs space, AGENTS.md §1 rule 5).
async fn copy_artifacts(
    engine: &mut Lease,
    old: &InvocationId,
    new: &InvocationId,
    keep: u32,
    input: Option<&Bytes>,
) -> Result<()> {
    let Some(store) = engine.artifact_store().cloned() else {
        return Ok(());
    };
    let store_err =
        |e: cairn_store::StoreError| ActorError::from(cairn_engine::EngineError::from(e));
    let rows = engine
        .writer()
        .artifacts_of_unbounded(old)
        .await
        .map_err(store_err)?;
    let wanted: Vec<_> = rows
        .into_iter()
        .filter_map(|(sha, row)| row.map(|r| (sha, r)))
        .filter(|(sha, _)| match (keep, input) {
            (1, Some(input)) => {
                let hex = cairn_store::artifact::sha_hex(sha);
                input.windows(hex.len()).any(|w| w == hex.as_bytes())
            }
            _ => true,
        })
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    // The restart will be refused (`from` too long, workflow run): copy nothing — the rows
    // of an id that never exists would keep its directory forever.
    if engine.restart_refusal(old, keep).await?.is_some() {
        return Ok(());
    }
    let Some((target, _)) = engine.head(old).await? else {
        return Ok(());
    };
    let tenant = target.tenant.as_str().to_string();
    let epoch = match engine
        .writer()
        .artifact_epoch(new)
        .await
        .map_err(store_err)?
    {
        Some(e) => e,
        None => {
            let mut b = [0u8; 8];
            cairn_store::entropy::fill(&mut b);
            u64::from_be_bytes(b)
        }
    };
    let to = cairn_store::artifact::Owner {
        tenant: tenant.clone(),
        id: *new,
        epoch,
    };
    let copies: Vec<_> = wanted
        .iter()
        .map(|(sha, r)| {
            (
                cairn_store::artifact::Owner {
                    tenant: tenant.clone(),
                    id: *old,
                    epoch: r.epoch,
                },
                *sha,
            )
        })
        .collect();
    engine
        .released(async |_: &crate::lease::Released| {
            for (from, sha) in &copies {
                store.copy(from, &to, sha).await?;
            }
            Ok::<_, cairn_store::StoreError>(())
        })
        .await
        .map_err(store_err)?;
    let mut muts = cairn_store::Mutations::new();
    // The artifact byte counter: `new` has no metadata yet ⇒ not counted here; `restart_in`
    // adds it in the commit that creates the new copy.
    for (sha, r) in &wanted {
        engine
            .stage_artifact_put(
                &mut muts,
                new,
                sha,
                &cairn_store::keys::ArtifactRow {
                    epoch,
                    size: r.size,
                    content_type: r.content_type.clone(),
                },
            )
            .await?;
    }
    engine.writer_mut().commit(muts).await.map_err(store_err)?;
    Ok(())
}

/// The durable part of `restart`/`continue_as_new` per the Plan: if the old copy has not
/// finished it ends as `completed {"continuedAsNew"}` — the Plan's stage and retry row —
/// in the same commit that creates the new copy (`Engine::restart_in`). The old copy's
/// artifacts are copied to the new one first ([`copy_artifacts`], AP-10).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn restart(
    engine: &mut Lease,
    old: InvocationId,
    new: InvocationId,
    keep: u32,
    input: Option<Bytes>,
    deployment: Option<&str>,
    queue: Option<(&QueueName, u64)>,
    plan: &lc::Plan,
    streams: &crate::streams::RangeStreams,
) -> Result<cairn_engine::RestartOutcome> {
    copy_artifacts(engine, &old, &new, keep, input.as_ref()).await?;
    let out = engine
        .restart_in(
            &old,
            &new,
            keep,
            input,
            deployment,
            queue,
            row_write(plan.retry, None),
        )
        .await?;
    if matches!(out, cairn_engine::RestartOutcome::Restarted { .. }) {
        side(streams, &old, plan.side);
    }
    Ok(out)
}

/// `pause_stuck`/`poison_row`: lifecycle core `Event::Pause` — `paused` 577, stage kept as is
/// (a status/stage mismatch that is valid and harmless). `false` when the invocation has
/// stopped for good or does not exist.
pub(crate) async fn pause_infra(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    message: &str,
) -> Result<bool> {
    let now = engine.clock().wall_ms();
    let mut asker = Asker::new(now, ctx, lc::Attendance::IDLE);
    let plan = asker
        .ask(
            engine,
            ctx,
            id,
            lc::Event::Pause {
                reason: lc::Pause::InfraStuck,
            },
        )
        .await?;
    if plan.status.is_none() {
        return Ok(false);
    }
    pause(
        engine,
        id,
        cairn_engine::INFRA_STUCK,
        message,
        &asker,
        &plan,
    )
    .await
}

/// `resume` (lifecycle core `Event::AdminResume`): `paused → running` in one commit,
/// returning the turn to run on (`Then::Drive`, counting from 0 again) — or `None` when
/// there is nothing to run (missing, not `paused`).
pub(crate) async fn resume(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
) -> Result<(cairn_engine::ResumeOutcome, Option<lc::Turn>)> {
    use cairn_engine::ResumeOutcome;
    let now = engine.clock().wall_ms();
    let mut asker = Asker::new(now, ctx, lc::Attendance::IDLE);
    let plan = asker.ask(engine, ctx, id, lc::Event::AdminResume).await?;
    match plan.then {
        lc::Then::Reply(lc::Reply::NotFound) => Ok((ResumeOutcome::NotFound, None)),
        lc::Then::Reply(_) => Ok((
            ResumeOutcome::NotPaused(
                asker
                    .status
                    .clone()
                    .flatten()
                    .expect("not paused ⇒ status was read"),
            ),
            None,
        )),
        lc::Then::Drive(turn) => {
            let outcome = engine.resume(&id).await?;
            side(&asker.streams, &id, plan.side);
            // The rerun turn counts `attempt` from 0 at exactly the stalled step: its stream
            // records must not be taken for duplicates of the previous run.
            if matches!(outcome, ResumeOutcome::Resumed) {
                asker.streams.reset(&id);
            }
            let run = matches!(outcome, ResumeOutcome::Resumed).then_some(turn);
            Ok((outcome, run))
        }
        other => unreachable!("AdminResume: {other:?}"),
    }
}

/// Cancel `id` (lifecycle core `Event::AdminCancel`, code [`cairn_engine::CANCELLED`]) —
/// the caller holds its lock (an admin cancel waits for the lock; the drainer's cancel
/// cascading to children has already `try`'d it). State `failed`, waiters, the cancel
/// cascading to children, the retry row and the queue entry in **one** commit, then return
/// the VO lock (RAM).
///
/// Already ended ⇒ cancelling again **repairs** an orphan queue entry.
pub(crate) async fn cancel(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    message: &str,
) -> Result<cairn_engine::CancelOutcome> {
    use cairn_engine::CancelOutcome;
    let now = engine.clock().wall_ms();
    let mut asker = Asker::new(now, ctx, lc::Attendance::IDLE);
    let plan = asker.ask(engine, ctx, id, lc::Event::AdminCancel).await?;
    match plan.then {
        lc::Then::Reply(lc::Reply::NotFound) => Ok(CancelOutcome::NotFound),
        lc::Then::Reply(lc::Reply::AlreadyFinished) => {
            let status = asker
                .status
                .clone()
                .flatten()
                .expect("ended ⇒ status was read");
            // No entry (the normal path): nothing to commit.
            if let Err(e) = release_slot(engine, ctx, id, &asker, &plan).await {
                if e.is_fenced() {
                    return Err(e);
                }
                tracing::warn!(invocation = %cairn_store::id_hex(&id), error = %e, "could not remove the orphaned queue entry yet");
            }
            Ok(CancelOutcome::AlreadyFinished(status))
        }
        _ => {
            let out = fail(engine, id, cairn_engine::CANCELLED, message, &asker, &plan).await?;
            if out != CancelOutcome::Cancelled {
                return Ok(out);
            }
            if plan.then == lc::Then::Dequeue {
                // Remove the queue entry in a second commit. **A failed dequeue does not
                // fail the cancel**: the terminal state is already durable, the VO lock is
                // released in RAM; a remaining orphan entry is cleaned up by a later
                // cancel or by the next term's `restore_flow`.
                if let Err(e) = release_queue_slot(engine, ctx, id).await {
                    if e.is_fenced() {
                        return Err(e);
                    }
                    tracing::warn!(invocation = %cairn_store::id_hex(&id), error = %e, "cancel is durable but not yet removed from the queue — VO lock released");
                }
            } else {
                free_lock(ctx, id, &asker, &plan);
            }
            Ok(out)
        }
    }
}

/// Remove an invocation that has stopped for good from the queue and return its VO lock
/// (lifecycle core `Event::Dequeue`). Used where an invocation ends **outside** a turn, so
/// there is no permit to release — the lock must be returned explicitly.
///
/// **The RAM lock is released on every exit path**, even when the read or commit fails: a
/// terminal invocation has no turn left to protect, and keeping the lock would lock the
/// object hard until the end of the term. A remaining queue entry does not become "two
/// holders at once": `restore_flow` cleans up the entry of an invocation that has stopped
/// for good instead of rebuilding a lock for it (FL-8).
pub(crate) async fn release_queue_slot(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
) -> Result<()> {
    let now = engine.clock().wall_ms();
    let mut asker = Asker::new(now, ctx, lc::Attendance::IDLE);
    let plan = match asker.ask(engine, ctx, id, lc::Event::Dequeue).await {
        Ok(p) => p,
        Err(e) => {
            ctx.scheduler.release_locks_of(id);
            return Err(e);
        }
    };
    release_slot(engine, ctx, id, &asker, &plan).await
}

async fn release_slot(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    asker: &Asker,
    plan: &lc::Plan,
) -> Result<()> {
    // The dequeue commit failed — only `return` the error.
    let dequeued = async {
        cairn_store::failpoint!("turn::release_queue_slot", err);
        commit(engine, id, asker, plan, None).await
    }
    .await;
    free_lock(ctx, id, asker, plan);
    dequeued
}

/// Return the VO lock in the scheduler's book per `Plan::ram.vo` (after the commit).
pub(crate) fn free_lock(ctx: &Ctx<'_>, id: InvocationId, asker: &Asker, plan: &lc::Plan) {
    if plan.ram.vo != lc::Vo::Free {
        return;
    }
    if let Some(lock) = asker
        .slot
        .as_ref()
        .and_then(|s| s.as_ref())
        .and_then(|s| s.name.lock_name())
    {
        ctx.scheduler.release_lock(lock, id);
    }
}

/// Rebuild the missing queue entry of a live invocation (lifecycle core
/// `StageChange::Insert(Inbox)` of `Event::Drive`; data from before flow control).
pub(crate) async fn enqueue(
    engine: &mut Lease,
    id: InvocationId,
    queue: &QueueName,
    seq: u64,
) -> Result<()> {
    engine.enqueue_existing(&id, queue, seq).await?;
    Ok(())
}

/// The busy-deferral of a whole background tick (lifecycle core `Event::RetryDue`, busy
/// branch) — **one** commit for every row, not one per invocation.
pub(crate) async fn defer(
    engine: &mut Lease,
    rows: &[(InvocationId, Option<u64>, lc::RetryRow)],
) -> Result<()> {
    engine.set_retry_rows(rows).await?;
    Ok(())
}

/// The durable part of `restore_flow` (lifecycle core `Event::LeaderUp` for each entry):
/// every stage change — pushing out of `Running`, removing orphan entries — in **one**
/// commit.
pub(crate) async fn restore_stages(
    engine: &mut Lease,
    writes: &[(cairn_engine::VQueueRow, cairn_engine::StageWrite)],
) -> Result<()> {
    engine.apply_stages(writes).await?;
    Ok(())
}

/// The `Side` of lifecycle core `Event::Finished` for the outcome the SDK reports — the same
/// mapping as `cairn_lifecycle::step::finished`: ended ⇒ seal `Terminal`, `paused` ⇒
/// `Paused`; suspended, retrying ⇒ none.
pub(crate) fn finished_side(o: &InvokeOutcome) -> Option<lc::Side> {
    match o {
        InvokeOutcome::Completed(_) | InvokeOutcome::Failed { .. } => {
            Some(lc::Side::Seal(lc::Seal::Terminal))
        }
        InvokeOutcome::Paused { .. } => Some(lc::Side::Seal(lc::Seal::Paused)),
        _ => None,
    }
}

/// Execute the side-resource effects of a committed Plan.
///
/// The only side resource today is the invocation's stream. Every `Side` goes through
/// **one** place in the shell: streams (and artifacts later) are closed or have attempts
/// marked here, never by calling the primitives directly from the turn paths.
pub(crate) fn side(
    streams: &crate::streams::RangeStreams,
    id: &InvocationId,
    side: Option<lc::Side>,
) {
    match side {
        // Seal: ended, `paused`, the old copy of a `restart` — readers read to the end, then
        // receive `end`; a temporary stream stays readable for `STREAM_LINGER`, then `410`.
        Some(lc::Side::Seal(_)) => streams.seal(id),
        // A new attempt: the identity `(step, attempt, k)` of the records already says
        // enough — the hub inserts `superseded` when the first record of the new attempt
        // arrives.
        Some(lc::Side::NewAttempt { .. }) | None => {}
    }
}
