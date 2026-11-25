// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The engine on the BTreeMap store model.
//!
//! Two parts:
//!
//! 1. **The engine-layer contract on both stores.** The same random scenario
//!    of the operations `turn/` and `actor/` call (create, turns with a fake
//!    SDK, `begin_turn`/`end_turn`, scheduled retries, timers, queues,
//!    recovery) runs on `Engine<Db>` (SlateDB, in-memory object store) and
//!    `Engine<RangeModel>`: every returned result must be identical, and after
//!    sealing, **every byte** in the store must be identical. Plus two
//!    durability contracts: "committed ≠ durable" and the `writer_epoch`
//!    fence, checked through `PartitionWriter`.
//! 2. **Random deaths on the model.** The turn path and the recovery path
//!    (following exactly the engine call sequence of `turn::drive_with_permit`,
//!    `turn::tick`, `turn::fire_retries`, `actor::recover`) run with deaths
//!    everywhere — each death keeps a random **prefix** of the non-durable WAL
//!    tail. After every reopen the invariants are checked; at the end it heals
//!    and runs until every invocation finishes (liveness mode).
//!
//! `Driver` **does not copy** the server's call sequence: every lifecycle
//! decision goes through the core itself (`cairn_lifecycle::step`), like the
//! shell `turn/life.rs`; `Driver` only reads the slots the core asks for and
//! executes the `Plan` with the same engine operations. The store model has
//! throttling, busy states (a lock being held, a turn already handed over),
//! infrastructure errors, `maxAttempts` and the attempt-count check.
//!
//! The order of acquiring the engine (FIFO, or the rule "commands go before
//! background work at the start of a term", `lease::EngineLock`) is not
//! `Driver` state: after each lock-rebuild chunk, a create command slips in or
//! not depending on the seed, and outside becoming leader every step of the
//! world chooses randomly among commands, turns, background ticks and the
//! drainer — commands going before background work is already covered by the
//! test.

// Tests run on real time; only `sim_cluster` must be deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{bail, Result},
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, InvocationStatus, RowWrite, Source, StageWrite, Target},
    cairn_lifecycle as lc,
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Completion, CompletionResult, Event, FailureAction, Outcome, StartParams},
    cairn_store::{
        keys::{self, Table},
        make_invocation_id,
        testkit::{RangeBucket, RangeModel},
        ClusterConfig, Durability, LeadershipGrant, PartitionId, PartitionKey, PartitionWriter,
        QueueName, RangeIter, RangeStore, ScanHow, StoreError,
    },
    slatedb::{config::Settings, object_store::memory::InMemory, Db},
    std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        sync::Arc,
        time::Duration,
    },
};

/// The watchdog deadline after a turn starts (`sdk_timeout` + margin, shortened).
const WATCH_AFTER: u64 = 30_000;
const T0: u64 = 1_700_000_000_000;

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn queue() -> QueueName {
    QueueName {
        tenant: Default::default(),
        scope: None,
        limit: None,
        exclusive: false,
        service: "Svc".into(),
        object_key: String::new(),
    }
}

/// A deterministic pseudo-random generator (LCG).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n.max(1)
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next(100) < pct
    }
}

// ───────────────────────────────────────────────────────────── fake SDK

/// The program of one invocation.
#[derive(Clone, Copy, Debug)]
enum Plan {
    /// `Output` ngay.
    Quick,
    /// `ctx.sleep` until `at`, then `Output`.
    Sleep { at: u64 },
}

fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn output_frames() -> Vec<Frame> {
    vec![
        frame(
            MessageType::OUTPUT,
            Flags::NONE,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"ok")),
                )),
                name: String::new(),
            },
        ),
        frame(MessageType::END, Flags::NONE, &pb::EndMessage {}),
    ]
}

/// The SDK frame answering a turn, from the messages `step(Start)` emits.
///
/// A failed turn issues no command (like M4's `Retry`): a turn that both
/// writes an entry and fails makes the core count the attempt at the position
/// **after** its own entry, while the attempt-count check here counts by the
/// position at the SDK call — two ways of stating the same rule, not
/// comparable with each other. So the model cannot build "a retry scheduled
/// earlier at an old position" (`planned_stale_position`): M4 and
/// `cairn-server/tests/lifecycle.rs::preset_schedule_recounted_when_journal_grows` cover it.
fn sdk_frames(replay: &[Message], plan: Plan, fail: bool) -> Vec<Frame> {
    if fail {
        return vec![frame(
            MessageType::ERROR,
            Flags::NONE,
            &pb::ErrorMessage {
                code: 500,
                message: "transient sdk failure".into(),
                ..Default::default()
            },
        )];
    }
    match plan {
        Plan::Quick => output_frames(),
        Plan::Sleep { at } => {
            let slept = replay.iter().any(|m| matches!(m, Message::Sleep(_)));
            let woke = replay
                .iter()
                .any(|m| matches!(m, Message::SleepCompletion(_)));
            let mut out = Vec::new();
            if !slept {
                out.push(frame(
                    MessageType::SLEEP,
                    Flags::NONE,
                    &pb::SleepCommandMessage {
                        wake_up_time: at,
                        result_completion_id: 1,
                        name: String::new(),
                    },
                ));
            }
            if woke {
                out.extend(output_frames());
            } else {
                out.push(frame(
                    MessageType::SUSPENSION,
                    Flags::NONE,
                    &pb::SuspensionMessage {
                        waiting_completions: vec![1],
                        ..Default::default()
                    },
                ));
            }
            out
        }
    }
}

// ───────────────────────────────────────────────────────── call sequence

/// The result of one turn, like `InvokeOutcome` (shortened).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Turned {
    Done,
    Suspended,
    Retry,
    Skipped,
    /// The permit-wait cap ran out because of the budget (core `Event::Throttled`)
    /// — the throttle path of a turn with a caller (`resume`, an external
    /// signal); a background turn parks a ticket.
    Throttled,
    /// A background turn parks a ticket in the scheduler's queue behind the
    /// gate: no task, no lock; the grant continues from `PermitGranted`.
    Parked,
    /// `paused` 575 — preflight ran out of `maxAttempts`.
    Exhausted,
    /// An infrastructure error after `Running` (core `Event::TurnFailed`).
    Failed,
}

/// The death point within a turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashAt {
    Never,
    AfterRunning,
    AfterSdk,
}

/// The environment of a turn: retryable SDK failure, infrastructure error, throttling, death.
#[derive(Clone, Copy, Debug)]
struct Env {
    fail: bool,
    infra: bool,
    throttled: bool,
    /// The background turn is not granted a permit right at the gate: it parks a ticket.
    park: bool,
    crash: CrashAt,
}

impl Env {
    const CALM: Env = Env {
        fail: false,
        infra: false,
        throttled: false,
        park: false,
        crash: CrashAt::Never,
    };
}

/// The process dies in the middle of an operation.
struct Crashed;

/// The ghost variables of the attempt-count check (like M4's `ATTEMPTS`), by
/// `(invocation, journal position)`: the SDK has been called `calls` times at
/// that position, and `lost` turns died between the `Running` commit and the
/// call (FL-19 allows counting them — an upper bound: that commit may be lost
/// in the death). By position, not just the last position: a death keeping a
/// WAL prefix can pull the journal back to an old position, while the durable
/// retry row of that position still carries the attempts counted. Survives
/// every death (it belongs to the world, not to the node's RAM).
#[derive(Clone, Copy, Debug, Default)]
struct Attempts {
    calls: u32,
    lost: u32,
}

type Ghost = BTreeMap<(InvocationId, u32), Attempts>;

/// A handed-over turn (`Runner`): the core's turn, and the not-yet-delivered
/// timer it carries (`Runner::queue_timer` — a timer the background tick met
/// while the invocation was busy).
type Handout = (InvocationId, lc::Turn, Option<(u64, u32)>);

/// The `maxAttempts` deadline in the store model (the `Contract` of every
/// invocation).
const MAX_ATTEMPTS: u32 = 4;

/// The chunk of the recovery scan in `Driver::recover` — small so that every
/// leader-up goes over the chunk cursor on both `Db` and the model (product:
/// 1 024).
const CHUNK: usize = 2;

/// The chunk of the inbound drain round (product: `INBOUND_BATCH` = 64) —
/// small so that a round with a backlog of a few rows passes the release point
/// between two chunks.
const DRAIN_CHUNK: usize = 2;

/// A "node": the engine plus the service loop's RAM (handed-over work, locks
/// being held, delivered timers).
///
/// **Does not copy the server's call sequence**: every lifecycle decision goes
/// through the core itself (`cairn_lifecycle::step`) — `Driver` is only a
/// small shell like `turn/life.rs`: it reads the slots the core asks for from
/// the engine, executes the `Plan` with the same engine operations
/// (`apply_lifecycle`, `set_retry_rows`, `pause_in`, `apply_stages`), and
/// reports the next event.
struct Driver<S: cairn_store::RangeStore> {
    e: Engine<S>,
    now: u64,
    /// `runner.queue` — turns handed over, not yet run. Lost on death.
    runnable: VecDeque<Handout>,
    /// Tickets parked in the scheduler's queue: turns that passed the gate
    /// (`Drive`) but have not been granted a permit yet — still handed-over
    /// turns (`Runner::queued`), holding no lock. Lost on death.
    parked: VecDeque<Handout>,
    /// Invocations for which "a long turn is holding the lock" at the next
    /// background tick (the busy model: the core sees `Attendance::lock_held`).
    /// Lost on death.
    held: BTreeSet<InvocationId>,
    /// Timers delivered along with a turn not yet run (`Runner::timers_in_flight`).
    timers_in_flight: BTreeSet<(InvocationId, u32)>,
    /// The inbound drain round in progress: `(cursor, watermark at the start
    /// of the round)`. The drainer releases the engine between two chunks; other
    /// steps (receiving messages, turns, background ticks, deaths) slip in
    /// there. Lost on death.
    drain_pass: Option<(u64, u64)>,
    attempts: Ghost,
    /// The trace of every returned result (for the two-store contract part).
    trace: Vec<String>,
}

type InvocationId = [u8; 24];

/// A retry row read back as the engine's `RowWrite`.
fn row_write(change: lc::RowChange, old_due: Option<u64>) -> RowWrite {
    match change {
        lc::RowChange::Keep => RowWrite::Keep,
        lc::RowChange::Set(row) => RowWrite::Set { old_due, row },
        lc::RowChange::Clear => RowWrite::Clear { old_due },
    }
}

fn stage_write(change: lc::StageChange) -> StageWrite {
    let to = |s: lc::Stage| match s {
        lc::Stage::Inbox => cairn_store::Stage::Inbox,
        lc::Stage::Running => cairn_store::Stage::Running,
        lc::Stage::Suspended => cairn_store::Stage::Suspended,
        lc::Stage::Paused => cairn_store::Stage::Paused,
    };
    match change {
        lc::StageChange::Keep => StageWrite::Keep,
        lc::StageChange::Move(s) => StageWrite::Move(to(s)),
        lc::StageChange::Remove => StageWrite::Remove,
        lc::StageChange::Insert(_) => unreachable!("the model does not rebuild entries"),
    }
}

/// What the shell reads for the core: the view and the full queue entry (for the commit).
struct Asked {
    plan: lc::Plan,
    view: lc::View,
    slot: Option<Option<cairn_engine::VQueueSlot>>,
}

impl<S: cairn_store::RangeStore> Driver<S> {
    fn new(e: Engine<S>, now: u64) -> Self {
        Self {
            e,
            now,
            runnable: VecDeque::new(),
            parked: VecDeque::new(),
            held: BTreeSet::new(),
            timers_in_flight: BTreeSet::new(),
            drain_pass: None,
            attempts: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    fn log(&mut self, what: impl std::fmt::Debug) {
        self.trace.push(format!("{what:?}"));
    }

    /// The "busy" question (`turn::attendance`).
    fn attendance(&self, id: &InvocationId) -> lc::Attendance {
        lc::Attendance {
            lock_held: self.held.contains(id),
            queued: self
                .runnable
                .iter()
                .chain(&self.parked)
                .any(|(q, ..)| q == id),
            parked: self.parked.iter().any(|(q, ..)| q == id),
            ..lc::Attendance::IDLE
        }
    }

    /// Asks the core about `event` for `id`, reading every slot it asks for
    /// (`turn::life::Asker`): a fixed contract (`MAX_ATTEMPTS`, `PAUSE`), the
    /// handler declares no policy.
    async fn ask(&self, id: &InvocationId, mut view: lc::View, event: lc::Event) -> Result<Asked> {
        let mut slot = None;
        loop {
            let plan = lc::step(&view, &event);
            let Some(need) = plan.need else {
                return Ok(Asked { plan, view, slot });
            };
            match need {
                lc::Need::Status => {
                    view.status = Some(cairn_engine::lifecycle_status(
                        self.e.status(id).await?.as_ref(),
                    ))
                }
                lc::Need::Slot => {
                    let s = self.e.vqueue_of(id).await?;
                    view.slot = Some(s.as_ref().map(cairn_engine::lifecycle_slot));
                    slot = Some(s);
                }
                lc::Need::Retry => view.retry = Some(self.e.retry_row(id).await?),
                lc::Need::JournalLen => view.journal_len = Some(self.e.journal_len(id).await?),
                lc::Need::Contract => {
                    view.contract = Some(lc::Contract {
                        pinned: lc::Pinned::Found,
                        max_attempts: Some(MAX_ATTEMPTS),
                        on_max: lc::OnMax::Pause,
                    })
                }
                lc::Need::Policy { step } => view = view.with_policy(step, None),
                // `Driver` has no completion drainer (the core only asks for the
                // wait tree at `CompletionArrived`); "unknown" is the safe answer.
                lc::Need::Waiting => view.waiting = Some(lc::Waiting::Unknown),
            }
        }
    }

    fn view(&self, attendance: lc::Attendance) -> lc::View {
        lc::View::new(
            self.now,
            lc::Knobs {
                defer_ms: 5_000,
                watch_after_ms: WATCH_AFTER,
                parked_defer_ms: lc::PARKED_DEFER_MS,
            },
            attendance,
        )
    }

    /// The durable part of `asked.plan` (stage, retry row) — `turn::life::commit`.
    async fn commit(
        &mut self,
        id: &InvocationId,
        asked: &Asked,
        old_due: Option<u64>,
    ) -> Result<()> {
        let p = &asked.plan;
        if p.stage == lc::StageChange::Keep && p.retry == lc::RowChange::Keep {
            return Ok(());
        }
        let known = asked.view.retry.flatten().map(|r| r.due);
        self.e
            .apply_lifecycle(
                id,
                asked.slot.as_ref(),
                stage_write(p.stage),
                row_write(p.retry, known.or(old_due)),
            )
            .await?;
        Ok(())
    }

    /// Creates an invocation in the queue's `Inbox`, `Running` (core `Submitted`).
    ///
    /// The new `seq` must be above the `seq` of every live entry (FL-5) — even
    /// when the create command arrives between two chunks of the lock rebuild,
    /// with the counter seeded from the durable watermark
    /// (`floors::QUEUE_HIGH`).
    async fn create(&mut self, id: InvocationId) -> Result<()> {
        let q = queue();
        let live = self.e.all_vqueues().await?.iter().map(|r| r.slot.seq).max();
        let seq = self.e.next_queue_seq(&q).await?;
        if live.is_some_and(|m| seq <= m) {
            bail!("FL-5: new queue seq {seq} not above the live entries {live:?}");
        }
        let r = self
            .e
            .create(
                id,
                &target(),
                CreateOptions {
                    queue: Some((&q, seq)),
                    ..Default::default()
                },
                StartParams::default(),
                Bytes::from_static(b"req"),
            )
            .await
            .map(|_| ());
        self.log(("create", seq, &r));
        r?;
        self.runnable.push_back((id, lc::Turn::PLAIN, None));
        Ok(())
    }

    /// Checks the attempt count at SDK call number `calls + 1` at position `len`
    /// of `id`: every earlier call was counted, and no attempt was counted
    /// without the SDK being called, beyond the number of turns that died
    /// between the `Running` commit and the call.
    fn check_attempt(&mut self, id: &InvocationId, len: u32, rc: u32) -> Result<()> {
        let a = self.attempts.entry((*id, len)).or_default();
        if rc < a.calls || rc > a.calls + a.lost {
            bail!(
                "attempt count of {}: ctx.attempt {rc} at position {len}, SDK called {} times, {} turns died after `Running`",
                id[8],
                a.calls,
                a.lost
            );
        }
        a.calls += 1;
        Ok(())
    }

    /// One turn — exactly the shell's event sequence (`turn::drive*`): the gate
    /// (`Drive`), throttling (`Throttled` — a turn with a caller) or parking a
    /// ticket (a background turn not granted right away), the post-permit
    /// gate plus the `Running` commit (`PermitGranted`), preflight
    /// (`TurnStarted`), the SDK, the end (`TurnEnded`) or an infrastructure
    /// error (`TurnFailed`).
    async fn turn(
        &mut self,
        (id, turn, timer): Handout,
        plan: Plan,
        env: Env,
    ) -> Result<std::result::Result<Turned, Crashed>> {
        if let Some((_, cid)) = timer {
            self.timers_in_flight.remove(&(id, cid));
        }
        let turn = lc::Turn {
            wake: timer.map(|_| lc::WakeKind::Completion),
            ..turn
        };
        let calm = lc::Attendance::IDLE;
        // The pre-permit gate.
        let gate = self
            .ask(&id, self.view(calm), lc::Event::Drive { turn })
            .await?;
        self.log(("gate", gate.view.status));
        if let lc::Then::Reply(r) = gate.plan.then {
            if let (Some((due, cid)), lc::WakeWrite::Discard) = (timer, gate.plan.wake) {
                self.e.drop_timer(due, &id, cid).await?;
            }
            return Ok(Ok(match r {
                lc::Reply::Ended => Turned::Done,
                _ => Turned::Skipped,
            }));
        }
        // Asking for a permit withdraws a parked ticket of the same invocation
        // (`Scheduler::adopt`): one entry per invocation in the queue (M4
        // `adopt_keeps_ticket`).
        self.parked.retain(|(q, ..)| *q != id);
        if env.park {
            // Not granted right away: park a ticket, the task ends. The
            // undelivered timer goes with the ticket (`Runner::timers_in_flight`
            // was cleared at the start of the turn as before).
            self.parked
                .push_back((id, lc::Turn { wake: None, ..turn }, timer));
            self.log(("parked", turn.planned.is_some()));
            return Ok(Ok(Turned::Parked));
        }
        if env.throttled {
            let waiting = lc::Attendance {
                permit: lc::Permit::Waiting,
                ..calm
            };
            let t = self
                .ask(
                    &id,
                    self.view(waiting),
                    lc::Event::Throttled {
                        turn,
                        after_ms: 1_000,
                    },
                )
                .await?;
            self.commit(&id, &t, None).await?;
            self.log(("throttled", &t.plan.retry));
            return Ok(Ok(Turned::Throttled));
        }
        self.granted((id, turn, timer), plan, env).await
    }

    /// A parked ticket is granted a permit (`run_granted` — the invocation lock
    /// is taken in the same grant): the turn continues from the post-permit gate.
    async fn run_parked(
        &mut self,
        (id, turn, timer): Handout,
        plan: Plan,
        env: Env,
    ) -> Result<std::result::Result<Turned, Crashed>> {
        let turn = lc::Turn {
            wake: timer.map(|_| lc::WakeKind::Completion),
            ..turn
        };
        self.log(("granted", turn.planned.is_some()));
        self.granted((id, turn, timer), plan, env).await
    }

    /// The post-permit part of a turn: the post-permit gate + the `Running`
    /// commit (`PermitGranted`), preflight, SDK, end.
    async fn granted(
        &mut self,
        (id, turn, timer): Handout,
        plan: Plan,
        env: Env,
    ) -> Result<std::result::Result<Turned, Crashed>> {
        let calm = lc::Attendance::IDLE;
        let started = lc::Attendance {
            permit: lc::Permit::Started,
            ..calm
        };
        // The post-permit gate, then the `Running` commit.
        let began = self
            .ask(&id, self.view(started), lc::Event::PermitGranted { turn })
            .await?;
        let lc::Then::Preflight { retry_count: rc } = began.plan.then else {
            self.commit(&id, &began, None).await?;
            return Ok(Ok(Turned::Skipped));
        };
        self.commit(&id, &began, turn.planned.map(|p| p.next_due))
            .await?;
        let begun = match began.plan.retry {
            lc::RowChange::Set(r) => Some(r.due),
            _ => None,
        };
        if env.crash == CrashAt::AfterRunning {
            // The attempt may already have been counted (if the `Running`
            // commit survived the death), at the position after the wake-up
            // the turn carries.
            let len = self.e.journal_len(&id).await? + u32::from(timer.is_some());
            self.attempts.entry((id, len)).or_default().lost += 1;
            return Ok(Err(Crashed));
        }
        // The wake-up the turn carries: written before calling the SDK (`drive_one`).
        if let Some((due, cid)) = timer {
            let mut inv = self.e.load(id, StartParams::default()).await?;
            self.e.step(&mut inv, Event::Start).await?;
            self.e
                .deliver(
                    &mut inv,
                    Completion {
                        id: cid,
                        result: CompletionResult::Timer,
                    },
                    Source::Timer { due_ms: due },
                )
                .await?;
        }
        // Preflight.
        let pre = self
            .ask(
                &id,
                self.view(started),
                lc::Event::TurnStarted {
                    turn,
                    retry_count: rc,
                },
            )
            .await?;
        match pre.plan.then {
            lc::Then::CallSdk { .. } => {}
            lc::Then::Reply(lc::Reply::Paused) => {
                // Out of attempts: only when the SDK has been called enough at this position.
                let len = self.e.journal_len(&id).await?;
                let a = self.attempts.get(&(id, len)).copied().unwrap_or_default();
                if a.calls + a.lost < MAX_ATTEMPTS {
                    bail!(
                        "paused 575 of {} after {} SDK calls, {} turns died — fewer than {MAX_ATTEMPTS}",
                        id[8],
                        a.calls,
                        a.lost
                    );
                }
                self.e
                    .pause_in(
                        &id,
                        cairn_engine::MAX_ATTEMPTS_EXHAUSTED,
                        "out of attempts",
                        pre.slot.as_ref(),
                        stage_write(pre.plan.stage),
                        row_write(pre.plan.retry, begun),
                    )
                    .await?;
                self.log(("exhausted", rc));
                return Ok(Ok(Turned::Exhausted));
            }
            other => bail!("unexpected preflight: {other:?}"),
        }
        // The journal must be durable before the SDK sees it (`awaiting_sdk`).
        self.e.writer().wait_durable().await?;
        let len = self.e.journal_len(&id).await?;
        self.check_attempt(&id, len, rc)?;
        if env.infra {
            let f = self
                .ask(
                    &id,
                    self.view(started),
                    lc::Event::TurnFailed {
                        turn,
                        at: lc::FailedAt::AfterRunning { retry_count: rc },
                        err: lc::TurnErr::Other,
                    },
                )
                .await?;
            self.commit(&id, &f, begun).await?;
            self.log(("infra", rc, &f.plan.retry));
            return Ok(Ok(Turned::Failed));
        }
        let params = StartParams {
            retry_count_since_last_stored_entry: rc,
            ..Default::default()
        };
        let mut inv = self.e.load(id, params).await?;
        let replay = self.e.step(&mut inv, Event::Start).await?;
        for f in sdk_frames(&replay, plan, env.fail) {
            self.e.step(&mut inv, Event::FromSdk(f)).await?;
        }
        if env.crash == CrashAt::AfterSdk {
            return Ok(Err(Crashed));
        }
        let (outcome, turned) = match inv.outcome() {
            Some(Outcome::Completed(_)) => (lc::Outcome::Completed, Turned::Done),
            Some(Outcome::Suspended(_)) => (lc::Outcome::Waiting, Turned::Suspended),
            Some(Outcome::Failed {
                action: FailureAction::Retry { .. },
                ..
            }) => (lc::Outcome::Retry { delay_ms: None }, Turned::Retry),
            other => bail!("unexpected turn outcome: {other:?}"),
        };
        let ended = self
            .ask(
                &id,
                self.view(started),
                lc::Event::TurnEnded {
                    turn,
                    retry_count: rc,
                    outcome,
                },
            )
            .await?;
        self.commit(&id, &ended, begun).await?;
        self.log(("turn", turn.planned.is_some(), &turned));
        Ok(Ok(turned))
    }

    /// The background tick (`turn::tick` + `turn::fire_retries`): core
    /// `TimerDue` for each due timer, `RetryDue` for each retry row —
    /// schedules the next one **before** delivering, moves the rows of busy
    /// invocations in one commit.
    async fn tick(&mut self) -> Result<()> {
        // The cap as in the shell (`turn::timers::MAX_TIMERS_PER_TICK`).
        let due = self.e.due_timers(self.now, 256).await?;
        self.log(("due_timers", &due));
        for (due_ms, id, cid) in due {
            let in_flight = self.timers_in_flight.contains(&(id, cid));
            let t = self
                .ask(
                    &id,
                    self.view(self.attendance(&id)),
                    lc::Event::TimerDue { in_flight },
                )
                .await?;
            match t.plan.wake {
                lc::WakeWrite::None => {
                    if let Some(turn) = t.plan.ram.handout {
                        self.timers_in_flight.insert((id, cid));
                        self.runnable.push_back((
                            id,
                            lc::Turn { wake: None, ..turn },
                            Some((due_ms, cid)),
                        ));
                    }
                }
                lc::WakeWrite::Discard => self.e.drop_timer(due_ms, &id, cid).await?,
                lc::WakeWrite::Deliver(_) => {
                    // `settle_completion`: the warm machine, or load cold and
                    // only replay, deliver, store the machine.
                    let (mut inv, _) = self.e.settle_machine(id, StartParams::default()).await??;
                    self.e
                        .deliver(
                            &mut inv,
                            Completion {
                                id: cid,
                                result: CompletionResult::Timer,
                            },
                            Source::Timer { due_ms },
                        )
                        .await?;
                    self.e.keep_warm(inv);
                    if let Some(turn) = t.plan.ram.handout {
                        self.runnable.push_back((id, turn, None));
                    }
                }
            }
        }
        let due = self.e.due_retry_rows(self.now, 256).await?;
        self.log(("due_retries", &due));
        let mut busy = Vec::new();
        for (id, row) in due {
            let v = self.view(self.attendance(&id)).with_retry(Some(row));
            let r = self.ask(&id, v, lc::Event::RetryDue { row }).await?;
            match (r.plan.retry, r.plan.ram.handout) {
                (lc::RowChange::Set(to), None) => busy.push((id, Some(row.due), to)),
                (_, handout) => {
                    self.commit(&id, &r, Some(row.due)).await?;
                    if let Some(turn) = handout {
                        self.runnable.push_back((id, turn, None));
                    }
                }
            }
        }
        self.e.set_retry_rows(&busy).await?;
        Ok(())
    }

    /// Drains every visible inbound row (including non-durable ones — the
    /// drainer reads at level `Memory`): finishes the round in progress (if
    /// any), then one new full round — rows received after the in-progress
    /// round's watermark belong to the new round, like the drain loop (the
    /// next round is triggered by a poke). Returns `(completion id, seq of the
    /// deleting commit)` of the deleted rows — seq `0` if the commit was durable
    /// right away.
    async fn drain_inbound(&mut self) -> Result<Vec<(u32, u64)>> {
        let mut out = Vec::new();
        let mut fresh = self.drain_pass.is_none();
        loop {
            let (done, rows) = self.drain_chunk().await?;
            out.extend(rows);
            if done {
                if fresh {
                    self.log(("drain_inbound", out.len()));
                    return Ok(out);
                }
                fresh = true;
            }
        }
    }

    /// One chunk of the inbound drain round (a copy of `turn::drain_inbound`,
    /// the read and delete part): the round starts at `seq` 0 with the
    /// watermark at its start (`outbox_high_water`), each chunk reads up to
    /// `DRAIN_CHUNK` rows from the cursor, deletes each processed row, then
    /// **releases** — the world's next step can be anything. Returns `true`
    /// when the round is done (no rows left, or a row past the watermark).
    async fn drain_chunk(&mut self) -> Result<(bool, Vec<(u32, u64)>)> {
        let (mut cursor, high) = match self.drain_pass.take() {
            Some(p) => p,
            None => (0, self.e.outbox_high_water().await?),
        };
        let rows = self.e.inbound_from(cursor, DRAIN_CHUNK).await?;
        let full = rows.len() == DRAIN_CHUNK;
        let mut out = Vec::new();
        let mut past_high = false;
        for (key, _) in rows {
            if key.seq > high {
                past_high = true;
                break;
            }
            cursor = key.seq + 1;
            let cid = key.completion_id;
            self.e.drop_inbound(key).await?;
            let seq = self
                .e
                .writer()
                .health()
                .lag()
                .map_or(0, |(latest, _)| latest);
            out.push((cid, seq));
        }
        let done = past_high || !full;
        if !done {
            self.drain_pass = Some((cursor, high));
        }
        self.log(("drain_chunk", out.len(), done));
        Ok((done, out))
    }

    /// Purges invocations past retention (a copy of the engine part of `turn::tick`).
    async fn purge_due(&mut self) -> Result<Vec<InvocationId>> {
        let due = self.e.due_purges(self.now, 64).await?;
        self.log(("due_purges", &due));
        let mut out = Vec::new();
        for (due_ms, id) in due {
            self.e.purge(&id, Some(due_ms)).await?;
            out.push(id);
        }
        Ok(out)
    }

    /// Becoming leader: the old term's RAM is lost.
    fn begin_term(&mut self) {
        self.runnable.clear();
        self.parked.clear();
        self.held.clear();
        self.timers_in_flight.clear();
        self.drain_pass = None;
    }

    /// One chunk of `actor::restore_flow`: the entries of unfinished
    /// invocations within the `Invocations` index chunk after `after`, core
    /// `LeaderUp` for each entry, every stage change of the chunk in one commit.
    /// Returns the next chunk's cursor (`None` = done); `max_seq` gathers the
    /// `seq`s seen.
    async fn restore_step(
        &mut self,
        after: Option<InvocationId>,
        max_seq: &mut u64,
    ) -> Result<Option<InvocationId>> {
        let (rows, next) = self.e.restore_chunk(after.as_ref(), CHUNK).await?;
        let mut writes = Vec::new();
        for r in &rows {
            *max_seq = (*max_seq).max(r.slot.seq);
            let v = self
                .view(lc::Attendance::IDLE)
                .with_slot(Some(cairn_engine::lifecycle_slot(&r.slot)));
            let p = self.ask(&r.id, v, lc::Event::LeaderUp).await?.plan;
            if p.stage != lc::StageChange::Keep {
                writes.push((r.clone(), stage_write(p.stage)));
            }
        }
        self.e.apply_stages(&writes).await?;
        self.log(("restore_flow", rows.len(), writes.len(), next.is_some()));
        Ok(next)
    }

    /// Becoming leader (`actor::restore_flow` + `actor::recover`) with no
    /// command in between: every lock-rebuild chunk, then the recovery scan.
    async fn recover(&mut self) -> Result<()> {
        self.begin_term();
        let (mut after, mut max_seq) = (None, 0);
        while let Some(c) = self.restore_step(after, &mut max_seq).await? {
            after = Some(c);
        }
        self.e.finish_restore(max_seq);
        self.recover_scan().await
    }

    /// The recovery scan of `actor::recover`: core `Recover` for each `running`
    /// (the durable scheduled retry is what continues it).
    async fn recover_scan(&mut self) -> Result<()> {
        // Like `actor::recover`: the status index in chunks, each `running` id
        // asks the core `Recover` once, the retry row is read when the core
        // needs it (`Need::Retry`) — within the same hold as the delivery.
        let mut after = None;
        loop {
            let (ids, next) = self.e.runnable_chunk(after.as_ref(), CHUNK).await?;
            self.log(("runnable", &ids, &next));
            for id in ids {
                let mut v = self
                    .view(lc::Attendance::IDLE)
                    .with_status(lc::Status::Running);
                let plan = loop {
                    let plan = lc::step(&v, &lc::Event::Recover);
                    match plan.need {
                        None => break plan,
                        Some(lc::Need::Retry) => v.retry = Some(self.e.retry_row(&id).await?),
                        Some(other) => unreachable!("Recover does not read {other:?}"),
                    }
                };
                if let Some(turn) = plan.ram.handout {
                    self.runnable.push_back((id, turn, None));
                }
            }
            match next {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        self.e.end_opening();
        Ok(())
    }
}

/// A cross-partition message to an invocation not in the range (like
/// `progress_floors`): the drainer only reads and then deletes the row —
/// enough for the ordering watermark.
fn inbound_msg(cid: u32) -> cairn_engine::InterPartition {
    cairn_engine::InterPartition::Completion(cairn_engine::DeliverCompletion {
        target: inv_id(200),
        completion_id: cid,
        result: cairn_engine::CompletionPayload::Void,
    })
}

// ─────────────────────────────────────────────────────── opening the engine

/// A fixed clock `T0` for both sides: since format 10 the ending status rows
/// carry the class-entry timestamp and the metadata carries `created_ms` —
/// with the real wall clock two runs would differ by a few ms in exactly those
/// bytes.
async fn engine_on<S: RangeStore>(store: S) -> Result<Engine<S>> {
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), store).await?;
    Ok(Engine::open(w)
        .await?
        .with_clock(Arc::new(cairn_store::clock::ManualClock::new(T0))))
}

async fn slate_db(store: &Arc<InMemory>, flush: Duration) -> Result<Db> {
    Ok(Db::builder("range", store.clone())
        .with_settings(Settings {
            flush_interval: Some(flush),
            ..Default::default()
        })
        .build()
        .await?)
}

/// Every key → value in the store, through [`RangeStore`].
async fn dump<S: RangeStore>(s: &S) -> Result<Vec<(Bytes, Bytes)>> {
    let mut it = s.scan(vec![0], vec![0xFF; 4], ScanHow::Small).await?;
    let mut out = Vec::new();
    while let Some(r) = it.next().await? {
        out.push((r.key, r.value));
    }
    Ok(out)
}

// ───────────────────────────────────────────────── part 1: the contract

/// A deterministic (by seed) scenario of turn-path operations — no deaths.
async fn scripted<S: RangeStore>(d: &mut Driver<S>, seed: u64) -> Result<()> {
    let mut rng = Lcg(seed);
    let plans: Vec<Plan> = (0..12)
        .map(|i| match i % 3 {
            0 => Plan::Quick,
            _ => Plan::Sleep {
                at: T0 + 1_000 * (1 + i as u64),
            },
        })
        .collect();
    let mut created = 0u8;
    let mut fails: BTreeMap<InvocationId, u64> = BTreeMap::new();
    for _ in 0..80 {
        match rng.next(6) {
            0 if (created as usize) < plans.len() => {
                d.create(inv_id(created)).await?;
                fails.insert(inv_id(created), rng.next(2));
                created += 1;
            }
            1 | 2 => {
                // A parked ticket is granted, or a handed-over turn runs —
                // sometimes parking a ticket after the gate.
                let parked = !d.parked.is_empty() && rng.chance(40);
                let h = if parked {
                    d.parked.pop_front()
                } else {
                    d.runnable.pop_front()
                };
                if let Some(h) = h {
                    let plan = plans[h.0[8] as usize];
                    let left = fails.get_mut(&h.0).expect("created");
                    let fail = *left > 0;
                    *left = left.saturating_sub(1);
                    let env = Env {
                        fail,
                        throttled: rng.chance(10),
                        park: !parked && rng.chance(15),
                        ..Env::CALM
                    };
                    let _ = if parked {
                        d.run_parked(h, plan, env).await?
                    } else {
                        d.turn(h, plan, env).await?
                    };
                }
            }
            3 => d.tick().await?,
            4 => d.now += rng.next(3_000),
            _ => {
                let id = inv_id(rng.next(u64::from(created.max(1))) as u8);
                let a = d.e.status(&id).await?;
                let b = d.e.journal_len(&id).await?;
                let c = d.e.vqueue_of(&id).await?.map(|s| (s.stage, s.seq));
                let p = d.e.pending_retry(&id).await?;
                d.log(("read", a, b, c, p));
            }
        }
    }
    d.recover().await?;
    Ok(())
}

/// The same scenario on SlateDB and on the model: **every** returned result is
/// identical; after sealing, every key and value in the store is identical; and
/// a reopen (loading the FL-18 durable watermarks) gives the same recovery
/// result.
#[tokio::test]
async fn same_scenario_on_db_and_model_gives_same_result() -> Result<()> {
    for seed in 1..=6u64 {
        let mem = Arc::new(InMemory::new());
        let mut slate = Driver::new(
            engine_on(slate_db(&mem, Duration::from_millis(5)).await?).await?,
            T0,
        );
        scripted(&mut slate, seed).await?;
        let bucket = RangeBucket::new();
        let mut model = Driver::new(engine_on(bucket.open()).await?, T0);
        scripted(&mut model, seed).await?;
        assert_eq!(
            slate.trace.len(),
            model.trace.len(),
            "seed {seed}: trace length"
        );
        if std::env::var("CAIRN_MODEL_STATS").is_ok() {
            let n = |p: &str| slate.trace.iter().filter(|t| t.contains(p)).count();
            eprintln!(
                "seed {seed}: trace of {} lines, turns {}, done {}, timers {}",
                slate.trace.len(),
                n("(\"turn\""),
                n("Done"),
                slate
                    .trace
                    .iter()
                    .filter(|t| t.starts_with("(\"due_timers\", [("))
                    .count()
            );
        }
        for (i, (a, b)) in slate.trace.iter().zip(&model.trace).enumerate() {
            assert_eq!(a, b, "seed {seed}: diverges at step {i}");
        }
        slate.e.seal().await?;
        model.e.seal().await?;

        // Every byte in the store, through a raw open.
        let raw = slate_db(&mem, Duration::from_millis(5)).await?;
        let on_slate = dump(&raw).await?;
        raw.close().await?;
        let on_model = bucket.durable_entries();
        assert_eq!(
            on_slate.len(),
            on_model.len(),
            "seed {seed}: number of keys in the store"
        );
        for (a, b) in on_slate.iter().zip(&on_model) {
            assert_eq!(a, b, "seed {seed}: key/value differs in the store");
        }

        // Reopen: durable watermarks, recovery.
        let mut slate = Driver::new(
            engine_on(slate_db(&mem, Duration::from_millis(5)).await?).await?,
            T0,
        );
        let mut model = Driver::new(engine_on(bucket.open()).await?, T0);
        assert_eq!(slate.e.writer().rows_below_floors().await?, vec![]);
        assert_eq!(model.e.writer().rows_below_floors().await?, vec![]);
        slate.recover().await?;
        model.recover().await?;
        assert_eq!(slate.trace, model.trace, "seed {seed}: recovery");
        slate.e.seal().await?;
        model.e.seal().await?;
    }
    Ok(())
}

/// Committed ≠ durable, on both stores: right after a commit the ticket is
/// `Wait`; `Pending::durable` returns the ticket; after that the ticket is
/// granted in place.
async fn commit_is_not_durable<S: RangeStore>(store: S) -> Result<()> {
    let e = engine_on(store).await?;
    e.writer().wait_durable().await?;
    let mut d = Driver::new(e, T0);
    d.create(inv_id(1)).await?;
    let pending = match d.e.writer().durability()? {
        Durability::Wait(p) => p,
        Durability::Ready(_) => bail!("a ticket right after the commit"),
    };
    pending.durable().await?;
    assert!(matches!(d.e.writer().durability()?, Durability::Ready(_)));
    d.e.seal().await?;
    Ok(())
}

#[tokio::test]
async fn non_durable_commit_on_db() -> Result<()> {
    let mem = Arc::new(InMemory::new());
    commit_is_not_durable(slate_db(&mem, Duration::from_millis(300)).await?).await
}

#[tokio::test]
async fn non_durable_commit_on_model() -> Result<()> {
    commit_is_not_durable(RangeBucket::new().open()).await
}

/// The epoch fence, on both stores: a new writer fences the old one — the old
/// writer's commit never gets a ticket (`Fenced`), and the new writer does not
/// see it.
async fn fenced_writer_never_gets_a_ticket<S: RangeStore>(
    old: impl std::future::Future<Output = Result<S>>,
    new: impl FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<S>>>>,
) -> Result<()> {
    let mut a = Driver::new(engine_on(old.await?).await?, T0);
    a.create(inv_id(1)).await?;
    a.e.writer().wait_durable().await?;
    let mut b = Driver::new(engine_on(new().await?).await?, T0);
    // The zombie: the commit may still "succeed" into its memtable…
    let r = a.create(inv_id(2)).await;
    // …but it never gets a ticket.
    let ticket = a.e.writer().wait_durable().await;
    assert!(
        r.is_err() || matches!(ticket, Err(StoreError::Fenced)),
        "a fenced writer still gets a ticket: commit {r:?}, ticket {:?}",
        ticket.map(|_| ())
    );
    assert!(b.e.status(&inv_id(1)).await?.is_some());
    assert_eq!(
        b.e.status(&inv_id(2)).await?,
        None,
        "the new writer sees the zombie's row"
    );
    b.create(inv_id(3)).await?;
    b.e.writer().wait_durable().await?;
    b.e.seal().await?;
    Ok(())
}

#[tokio::test]
async fn fenced_writer_gets_no_ticket_on_db() -> Result<()> {
    let mem = Arc::new(InMemory::new());
    let m2 = mem.clone();
    fenced_writer_never_gets_a_ticket(slate_db(&mem, Duration::from_millis(5)), move || {
        Box::pin(async move { slate_db(&m2, Duration::from_millis(5)).await })
    })
    .await
}

#[tokio::test]
async fn fenced_writer_gets_no_ticket_on_model() -> Result<()> {
    let bucket = RangeBucket::new();
    let b2 = bucket.clone();
    fenced_writer_never_gets_a_ticket(async move { Ok(bucket.open()) }, move || {
        Box::pin(async move { Ok(b2.open()) })
    })
    .await
}

// ──────────────────────────────────────── part 2: random deaths, the model

/// Invariants on the **durable part** of the bucket — read straight from the "object storage".
///
/// An entity row has its index row (the index is a hint: a stale index row is
/// allowed, a **missing** one is not — a scan by index would never see that
/// entity again): status ↔ `Invocations` (same tag), timers, retries, queues.
/// Only when the derived zone has its mark — not during an unfinished rebuild.
fn durable_indexes_cover_entities(bucket: &RangeBucket) -> Vec<String> {
    let all: BTreeMap<Bytes, Bytes> = bucket.durable_entries().into_iter().collect();
    // No `DERIVED_READY` mark = the derived zone is mid-rebuild (the mark is
    // deleted **first**, written back **last**): the next open rebuilds from
    // the start.
    if !all.contains_key(&Bytes::from(keys::derived_ready())) {
        return Vec::new();
    }
    let has = |k: Vec<u8>| all.contains_key(&Bytes::from(k));
    let mut bad = Vec::new();
    for (k, v) in &all {
        let missing = match keys::table_of(k) {
            Some(Table::InvocationStatus) => keys::parse_invocation_status(k).and_then(|id| {
                let ix = Bytes::from(keys::ix_invocation(&id));
                (all.get(&ix).map(|t| &t[..]) != v.get(..1)).then(|| format!("Invocations {id:?}"))
            }),
            Some(Table::Timers) => {
                keys::parse_timer(k)
                    .zip(keys::decode_due(v))
                    .and_then(|((id, c), (due, _))| {
                        (!has(keys::ix_timer(due, &id, c))).then(|| format!("Timer {due} {id:?}"))
                    })
            }
            Some(Table::Retries) => {
                keys::parse_retry(k)
                    .zip(keys::decode_due(v))
                    .and_then(|(id, (due, _))| {
                        (!has(keys::ix_retry(due, &id))).then(|| format!("Retry {due} {id:?}"))
                    })
            }
            Some(Table::VQueueIndex) => keys::parse_vqueue_index(k)
                .zip(keys::decode_vqueue_index(v))
                .and_then(|(id, (q, stage, seq))| {
                    (!has(keys::ix_vqueue(&q, stage, seq, &id)))
                        .then(|| format!("VQueue {stage:?} {seq} {id:?}"))
                }),
            _ => None,
        };
        bad.extend(missing);
    }
    bad
}

/// The world of one seed: the bucket, the current "node", and what has been
/// promised to clients (acked ⇒ durable).
struct World {
    seed: u64,
    rng: Lcg,
    bucket: RangeBucket,
    node: Option<Driver<RangeModel>>,
    plans: BTreeMap<InvocationId, Plan>,
    /// The number of failing turns each invocation still has (transient SDK failures).
    fails: BTreeMap<InvocationId, u64>,
    /// Creation acked (the durable ticket was granted) — must remain after every death.
    acked_created: BTreeSet<InvocationId>,
    /// Result acked — must remain `Completed` after every death.
    acked_done: BTreeSet<InvocationId>,
    /// `purge` was called (may be lost on death — then the status is still intact).
    purged: BTreeSet<InvocationId>,
    /// The number of inbound messages received — every one must be drained before healing ends.
    inbound_next: u32,
    /// The range's generation: changed ⇒ the next open rebuilds the derived zone.
    gen: u64,
    rebuilds: usize,
    now: u64,
    /// The engine's clock (purge deadlines), always equal to `now`.
    clock: Arc<cairn_store::clock::ManualClock>,
    crashes: usize,
    /// The ghost variables of the attempt-count check, across every death.
    ghost: Ghost,
    /// Outbox rows written (keys) and the number issued — for the "send" step (LF-1).
    outbox: Vec<Bytes>,
    outbox_next: u64,
    /// Rows "handed to the transport" after a send ticket — must be in the
    /// durable prefix after every death (IR-1).
    sent: BTreeSet<Bytes>,
    /// The receiver's ack duty (LF-4): messages answered `200` (only after the
    /// receiving commit is durable), messages not yet `200` (the sender will
    /// resend), and messages the drainer deleted — with the seq of the deleting
    /// commit (durable ⇔ seq ≤ the bucket's `durable_seq`).
    inbound_acked: BTreeSet<u32>,
    inbound_unacked: BTreeSet<u32>,
    inbound_drained: BTreeMap<u32, u64>,
    /// A separate random source for the artifact step: does not touch the
    /// `rng` sequence, so every seed's scenario stays as before.
    art: Lcg,
    /// A separate random source for create commands slipping in between two
    /// lock-rebuild chunks: does not touch the `rng` sequence.
    mid: Lcg,
    /// The number of create commands that slipped in between two lock-rebuild chunks.
    mid_creates: usize,
    /// The number of times the built artifact byte counter was compared with an exhaustive count.
    artifact_checks: usize,
    /// The number of inbound drain chunks that ended with the round not done —
    /// the next step slips in between two chunks.
    mid_drains: usize,
    /// The number of background turns that parked a ticket — other steps slip in between parking and granting.
    parks: usize,
}

impl World {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: Lcg(seed),
            bucket: RangeBucket::new(),
            node: None,
            plans: BTreeMap::new(),
            fails: BTreeMap::new(),
            acked_created: BTreeSet::new(),
            acked_done: BTreeSet::new(),
            purged: BTreeSet::new(),
            inbound_next: 0,
            gen: 0,
            rebuilds: 0,
            now: T0,
            clock: Arc::new(cairn_store::clock::ManualClock::new(T0)),
            crashes: 0,
            ghost: Ghost::new(),
            outbox: Vec::new(),
            outbox_next: 0,
            sent: BTreeSet::new(),
            inbound_acked: BTreeSet::new(),
            inbound_unacked: BTreeSet::new(),
            inbound_drained: BTreeMap::new(),
            art: Lcg(seed ^ 0xA57_1FAC),
            mid: Lcg(seed ^ 0x5E5_70E0),
            mid_creates: 0,
            artifact_checks: 0,
            mid_drains: 0,
            parks: 0,
        }
    }

    /// **The artifact byte counter = an exhaustive count** (Q-19) after every
    /// step: now and then an artifact PUT through the shell's path
    /// (`stage_artifact_put` + commit, also for an id already purged or never
    /// created), one build chunk of 3 rows; once built, the counter must equal
    /// Σ `size` of the `Artifacts` rows with metadata, rescanned through the
    /// engine's read. Every term (after a death, a seal) rebuilds from the
    /// start.
    async fn artifact_step(&mut self) -> Result<()> {
        let ids: Vec<InvocationId> = self.plans.keys().copied().collect();
        if self.art.chance(40) && !ids.is_empty() {
            let id = if self.art.chance(10) {
                inv_id(250)
            } else {
                ids[self.art.next(ids.len() as u64) as usize]
            };
            let n = self.art.next(3) as u8;
            let e = &mut self.node.as_mut().expect("node").e;
            let mut m = cairn_store::Mutations::new();
            e.stage_artifact_put(
                &mut m,
                &id,
                &[n; 32],
                &keys::ArtifactRow {
                    epoch: 1,
                    size: 10 * (u64::from(n) + 1),
                    content_type: "x".into(),
                },
            )
            .await?;
            e.writer_mut().commit(m).await?;
        }
        let e = &mut self.node.as_mut().expect("node").e;
        if self.art.chance(50) {
            e.artifact_bytes_step(3).await?;
        }
        let Some(got) = e.artifact_bytes().snapshot() else {
            return Ok(());
        };
        let mut want = BTreeMap::<String, u64>::new();
        for (k, v) in e.writer().rows_under(&[0u8], false, usize::MAX).await? {
            let Some((id, _)) = keys::parse_artifact(&k) else {
                continue;
            };
            if let (Some(row), Some(meta)) = (keys::ArtifactRow::decode(&v), e.meta(&id).await?) {
                *want.entry(meta.tenant).or_default() += row.size;
            }
        }
        let want: Vec<(String, u64)> = want.into_iter().filter(|(_, n)| *n > 0).collect();
        if got != want {
            bail!(
                "seed {}: artifact byte counter {got:?} ≠ exhaustive count {want:?}",
                self.seed
            );
        }
        self.artifact_checks += 1;
        Ok(())
    }

    fn advance(&mut self, ms: u64) {
        self.now += ms;
        self.clock.set_wall_ms(self.now);
    }

    fn node(&mut self) -> &mut Driver<RangeModel> {
        self.node.as_mut().expect("a node is up")
    }

    /// Opens a new term, checks the invariants, then recovers.
    async fn start_term(&mut self) -> Result<()> {
        if self.rng.chance(8) {
            self.gen += 1;
        }
        let w = PartitionWriter::open(
            LeadershipGrant::single_node(PartitionId(0)),
            self.bucket.open(),
        )
        .await?;
        let cluster = ClusterConfig::single();
        let mut range = cluster.range(PartitionId(0)).expect("range 0");
        range.gen = self.gen;
        let mut e = Engine::open_in_range(w, cluster, range)
            .await?
            .with_retention(Some(3_000))
            .with_clock(self.clock.clone());
        if e.rebuilt().is_some() {
            self.rebuilds += 1;
        }
        let below = e.writer().rows_below_floors().await?;
        if !below.is_empty() {
            bail!(
                "seed {}: FL-18 — rows below the watermark after open: {below:?}",
                self.seed
            );
        }
        for id in &self.acked_created {
            if e.status(id).await?.is_none() && !self.purged.contains(id) {
                bail!("seed {}: lost acked creation {id:?}", self.seed);
            }
        }
        for id in &self.acked_done {
            let s = e.status(id).await?;
            if !matches!(s, Some(InvocationStatus::Completed(_)))
                && !(s.is_none() && self.purged.contains(id))
            {
                bail!("seed {}: lost an acked result {id:?}", self.seed);
            }
        }
        let invalid = e.lifecycle_violations().await?;
        if !invalid.is_empty() {
            bail!(
                "seed {}: lifecycle state outside the valid set after open: {invalid:?}",
                self.seed
            );
        }
        let durable: BTreeSet<Bytes> = self
            .bucket
            .durable_entries()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        if let Some(k) = self.sent.iter().find(|k| !durable.contains(*k)) {
            bail!(
                "seed {}: LF-1 — an outbox row handed to the transport is not in the durable prefix: {k:?}",
                self.seed
            );
        }
        let bad = durable_indexes_cover_entities(&self.bucket);
        if !bad.is_empty() {
            bail!(
                "seed {}: a durable entity is missing its index row: {bad:?}",
                self.seed
            );
        }
        // LF-4: an acked message either still has a durable inbound row, or has
        // a durable deletion — losing both ⇒ the `200` went out before the
        // receiving commit was durable.
        let mut present = BTreeSet::new();
        let mut cursor = 0;
        loop {
            let rows = e.inbound_from(cursor, 64).await?;
            let Some((last, _)) = rows.last() else { break };
            cursor = last.seq + 1;
            present.extend(rows.iter().map(|(k, _)| k.completion_id));
        }
        let durable_seq = self.bucket.durable_seq();
        let drained_durably = |c: &u32| {
            self.inbound_drained
                .get(c)
                .is_some_and(|seq| *seq <= durable_seq)
        };
        if let Some(cid) = self
            .inbound_acked
            .iter()
            .find(|c| !present.contains(*c) && !drained_durably(c))
        {
            bail!(
                "seed {}: LF-4 — message {cid} was acked but has no durable row left and was never drained",
                self.seed
            );
        }
        let mut d = Driver::new(e, self.now);
        d.attempts = std::mem::take(&mut self.ghost);
        // Chunked lock rebuild: the queue `seq` counter is seeded from the
        // durable watermark ⇒ create commands slip in between two chunks, like
        // the service loop; otherwise the rebuild runs to the end first (the
        // shell's `leadership_loop`). No turns: the permit gate stays closed
        // until the last chunk.
        d.begin_term();
        let primed = d.e.queue_seq_primed();
        let (mut after, mut max_seq) = (None, 0);
        loop {
            match d.restore_step(after, &mut max_seq).await? {
                Some(c) => after = Some(c),
                None => break,
            }
            if primed && self.mid.chance(30) {
                // 100..140: outside the ids of ordinary steps (< 10) and the
                // fixed ids 200, 201, 250 of the direct-write steps.
                let id = inv_id(100 + self.mid.next(40) as u8);
                if let std::collections::btree_map::Entry::Vacant(v) = self.plans.entry(id) {
                    v.insert(Plan::Quick);
                    self.fails.insert(id, 0);
                    d.create(id).await?;
                    self.mid_creates += 1;
                }
            }
        }
        d.e.finish_restore(max_seq);
        d.recover_scan().await?;
        // A sender without a `200` resends (IR-2): messages not acked that the
        // new leader no longer has, and that were never drained.
        for cid in self.inbound_unacked.clone() {
            if !present.contains(&cid) && !drained_durably(&cid) {
                d.e.accept_message(&inbound_msg(cid)).await?;
            }
        }
        self.node = Some(d);
        Ok(())
    }

    /// **FL-17 within a term**: every unfinished invocation has a wake-up —
    /// durable (a retry / watchdog row for `running`, a timer row for
    /// `suspended`) or a turn handed over in RAM (`runner.queue`, the
    /// "not covered" part of FL-17).
    async fn check_wakes(&mut self) -> Result<()> {
        let ids: Vec<_> = self.plans.keys().copied().collect();
        let d = self.node.as_ref().expect("node");
        // Lifecycle validity: `cairn_lifecycle::valid` after every step.
        let invalid = d.e.lifecycle_violations().await?;
        if !invalid.is_empty() {
            bail!(
                "seed {}: lifecycle state outside the valid set: {invalid:?}",
                self.seed
            );
        }
        for id in ids {
            let queued = d.runnable.iter().chain(&d.parked).any(|(q, ..)| *q == id);
            let retry = d.e.writer().get(&keys::retry(&id)).await?.is_some();
            let timer = d.e.writer().get(&keys::timer(&id, 1)).await?.is_some();
            let ok = match d.e.status(&id).await? {
                Some(InvocationStatus::Running) => queued || retry,
                Some(InvocationStatus::Suspended) => queued || timer,
                _ => true,
            };
            if !ok {
                bail!(
                    "seed {}: FL-17 — unfinished invocation {} has nothing left to wake it: {:?}, queue {:?}",
                    self.seed,
                    id[8],
                    d.e.status(&id).await?,
                    d.e.vqueue_of(&id).await?.map(|s| s.stage)
                );
            }
        }
        Ok(())
    }

    /// Records the messages just deleted, with the seq of each one's deleting commit.
    fn note_drained(&mut self, drained: Vec<(u32, u64)>) {
        self.inbound_drained.extend(drained);
    }

    /// The process dies: keeps a random prefix of the non-durable tail.
    fn crash(&mut self) {
        let pending = self.bucket.pending() as u64;
        let keep = self.rng.next(pending + 1) as usize;
        self.bucket.crash(keep);
        if let Some(n) = self.node.take() {
            self.ghost = n.attempts;
        }
        self.crashes += 1;
    }

    /// Waits for durability, then "acks" — like `Lease::sync` before answering a client.
    async fn ack(&mut self) -> Result<bool> {
        Ok(self.node().e.writer().wait_durable().await.is_ok())
    }

    /// One random step. `Ok(false)` = the process just died.
    async fn step(&mut self, ids: u8) -> Result<bool> {
        match self.rng.next(10) {
            0 | 1 => {
                let n = self.rng.next(u64::from(ids)) as u8;
                let id = inv_id(n);
                if self.plans.contains_key(&id) {
                    return Ok(true);
                }
                let plan = if self.rng.chance(50) {
                    Plan::Quick
                } else {
                    Plan::Sleep {
                        at: self.now + 500 + self.rng.next(5_000),
                    }
                };
                self.plans.insert(id, plan);
                // Up to `MAX_ATTEMPTS + 1` failures: some invocations run out of attempts.
                let fails = self.rng.next(u64::from(MAX_ATTEMPTS) + 2);
                self.fails.insert(id, fails);
                self.node().create(id).await?;
                if self.rng.chance(60) && self.ack().await? {
                    self.acked_created.insert(id);
                }
            }
            2..=4 => {
                // A parked ticket is granted, or a handed-over turn runs.
                let parked = !self.node().parked.is_empty() && self.rng.chance(35);
                let h = if parked {
                    self.node().parked.pop_front()
                } else {
                    self.node().runnable.pop_front()
                };
                let Some(h) = h else {
                    return Ok(true);
                };
                let id = h.0;
                let plan = self.plans[&id];
                let crash = match self.rng.next(12) {
                    0 => CrashAt::AfterRunning,
                    1 => CrashAt::AfterSdk,
                    _ => CrashAt::Never,
                };
                // A background turn not granted right away parks a ticket;
                // throttling is the path of a turn with a caller (`resume`, an
                // external signal).
                let park = !parked && self.rng.chance(15);
                let throttled = !park && self.rng.chance(5);
                let infra = !throttled && !park && self.rng.chance(5);
                // An SDK failure is only used up when the SDK is called.
                let fail = !throttled && !park && !infra && self.fails[&id] > 0;
                let env = Env {
                    fail,
                    infra,
                    throttled,
                    park,
                    crash,
                };
                let r = if parked {
                    self.node().run_parked(h, plan, env).await?
                } else {
                    self.node().turn(h, plan, env).await?
                };
                if matches!(r, Ok(Turned::Parked)) {
                    self.parks += 1;
                }
                if fail && matches!(r, Ok(Turned::Retry)) {
                    *self.fails.get_mut(&id).expect("created") -= 1;
                }
                match r {
                    Err(Crashed) => {
                        self.crash();
                        return Ok(false);
                    }
                    Ok(Turned::Done) => {
                        if self.ack().await? {
                            self.acked_done.insert(id);
                        }
                    }
                    Ok(_) => {}
                }
            }
            5 => {
                self.node().now = self.now;
                // The busy model: a long turn holds this invocation's lock
                // throughout the tick (the core sees `Attendance::lock_held`).
                if self.rng.chance(30) {
                    let n = self.rng.next(u64::from(ids)) as u8;
                    self.node().held.insert(inv_id(n));
                }
                self.node().tick().await?;
                self.node().held.clear();
                if self.rng.chance(50) {
                    for id in self.node().purge_due().await? {
                        self.purged.insert(id);
                    }
                }
            }
            6 => match self.rng.next(5) {
                // A turn writes outbox rows (sometimes followed by a commit
                // without send rows — the turn-ending commit), as `turn/`
                // produces send rows.
                3 => {
                    self.outbox_next += 1;
                    let n = self.outbox_next;
                    let id = inv_id(200);
                    let mut m = cairn_store::Mutations::new();
                    m.push_outbox(PartitionKey(0), n, &id, n as u32, b"call");
                    self.node().e.writer_mut().commit(m).await?;
                    self.outbox
                        .push(Bytes::from(keys::outbox(PartitionKey(0), &id, n as u32)));
                    if self.rng.chance(50) {
                        let mut m = cairn_store::Mutations::new();
                        m.set_invocation_status(&inv_id(201), b"x");
                        self.node().e.writer_mut().commit(m).await?;
                    }
                }
                // The drainer: gathers every visible outbox row, takes a send ticket, "hands over".
                4 => {
                    let mut seen = Vec::new();
                    for k in &self.outbox {
                        if self
                            .node
                            .as_ref()
                            .expect("node")
                            .e
                            .writer()
                            .get(k)
                            .await?
                            .is_some()
                        {
                            seen.push(k.clone());
                        }
                    }
                    loop {
                        match self.node().e.writer().send_durability()? {
                            cairn_store::SendDurability::Ready(_) => break,
                            cairn_store::SendDurability::Wait(p) => {
                                p.durable().await?;
                                break;
                            }
                            cairn_store::SendDurability::Barrier => {
                                self.node().e.writer_mut().send_barrier().await?;
                            }
                        }
                    }
                    self.sent.extend(seen);
                }
                0 => {
                    self.inbound_next += 1;
                    let cid = self.inbound_next;
                    let msg = inbound_msg(cid);
                    self.node().e.accept_message(&msg).await?;
                    // `200` only after the receiving commit is durable (`finish_unexposed_then`).
                    if self.rng.chance(60) && self.ack().await? {
                        self.inbound_acked.insert(cid);
                    } else {
                        self.inbound_unacked.insert(cid);
                    }
                }
                // One chunk of the drain round: another step slips in between two chunks.
                1 => {
                    let (done, drained) = self.node().drain_chunk().await?;
                    self.note_drained(drained);
                    if !done {
                        self.mid_drains += 1;
                    }
                }
                _ => {
                    let dt = self.rng.next(4_000);
                    self.advance(dt);
                }
            },
            7 => self.bucket.flush(),
            8 => {
                self.crash();
                return Ok(false);
            }
            _ => {
                if self.rng.chance(30) {
                    let n = self.node.take().expect("node");
                    self.ghost = n.attempts;
                    n.e.seal().await?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Healing: no more deaths, run turns and background ticks until every
    /// invocation finishes. Unfinished work with nothing left to wake it ⇒ a
    /// liveness error.
    async fn heal(&mut self) -> Result<()> {
        if self.node.is_none() {
            self.start_term().await?;
        }
        for _ in 0..400 {
            loop {
                // Parked tickets first: permits are free, every ticket is granted.
                let (h, parked) = match self.node().parked.pop_front() {
                    Some(h) => (h, true),
                    None => match self.node().runnable.pop_front() {
                        Some(h) => (h, false),
                        None => break,
                    },
                };
                let id = h.0;
                let plan = self.plans[&id];
                let fail = self.fails[&id] > 0;
                let env = Env { fail, ..Env::CALM };
                let r = if parked {
                    self.node().run_parked(h, plan, env).await?
                } else {
                    self.node().turn(h, plan, env).await?
                };
                if fail && matches!(r, Ok(Turned::Retry)) {
                    *self.fails.get_mut(&id).expect("created") -= 1;
                }
                if matches!(r, Ok(Turned::Done)) && self.ack().await? {
                    self.acked_done.insert(id);
                }
            }
            let mut left = Vec::new();
            for id in self.plans.keys().copied().collect::<Vec<_>>() {
                match self.node().e.status(&id).await? {
                    // Creation not acked and lost on death: the client was promised nothing.
                    // Purged (retention) also counts as done.
                    None if !self.acked_created.contains(&id) || self.purged.contains(&id) => {}
                    Some(InvocationStatus::Completed(_)) => {}
                    // Out of attempts (the attempt-count check ran at preflight).
                    Some(InvocationStatus::Paused {
                        code: cairn_engine::MAX_ATTEMPTS_EXHAUSTED,
                        ..
                    }) => {}
                    other => left.push((id, other)),
                }
            }
            if left.is_empty() {
                // FL-15/FL-18: every inbound message received can be drained —
                // no row stuck below the watermark, out of the scan's reach.
                let drained = self.node().drain_inbound().await?;
                self.note_drained(drained);
                self.node().e.writer().wait_durable().await?;
                let stuck = self
                    .bucket
                    .durable_entries()
                    .into_iter()
                    .filter(|(k, _)| keys::table_of(k) == Some(Table::Inbound))
                    .count();
                if stuck > 0 {
                    bail!(
                        "seed {}: {stuck} inbound messages are never drained",
                        self.seed
                    );
                }
                return Ok(());
            }
            self.advance(10_000);
            self.node().now = self.now;
            self.node().tick().await?;
            for id in self.node().purge_due().await? {
                self.purged.insert(id);
            }
        }
        let mut stuck = Vec::new();
        for id in self.plans.keys().copied().collect::<Vec<_>>() {
            let s = self.node().e.status(&id).await?;
            if !matches!(
                s,
                Some(InvocationStatus::Completed(_))
                    | Some(InvocationStatus::Paused {
                        code: cairn_engine::MAX_ATTEMPTS_EXHAUSTED,
                        ..
                    })
                    | None
            ) {
                let slot = self.node().e.vqueue_of(&id).await?;
                let retry = self.node().e.pending_retry(&id).await?;
                stuck.push((id[8], s, slot.map(|s| s.stage), retry));
            }
        }
        bail!(
            "seed {}: liveness — invocations never finish (after {} deaths): {stuck:?}",
            self.seed,
            self.crashes
        )
    }
}

/// **The turn path and the recovery path on the model, with random deaths.**
///
/// Each seed: 200 random steps over 10 invocations (create, turns with
/// transiently failing SDKs, background ticks, the clock, WAL flushes, deaths,
/// seals); deaths mid-turn (after the `Running` commit, after the SDK) or
/// between steps, each keeping a random prefix of the non-durable WAL tail.
///
/// Must see, after **every** step: `cairn_lifecycle::valid` for every
/// invocation; after **every** reopen: FL-18 (`rows_below_floors` empty);
/// everything acked is still intact (INV-5); every durable entity row has its
/// index row. At the end, healing: every invocation still in the store reaches
/// `Completed` (FL-17 — no invocation loses its wake-up). After every step
/// that survives: the built artifact byte counter equals the exhaustive count.
#[tokio::test]
async fn turns_and_recovery_on_model_with_random_crashes() -> Result<()> {
    let seeds: u64 = std::env::var("CAIRN_MODEL_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let mut artifact_checks = 0;
    let mut mid_creates = 0;
    let mut mid_drains = 0;
    let mut parks = 0;
    for seed in 1..=seeds {
        let mut w = World::new(seed);
        w.start_term().await?;
        for _ in 0..200 {
            if w.step(10).await? {
                w.check_wakes().await?;
                w.artifact_step().await?;
            } else {
                w.start_term().await?;
            }
        }
        w.heal().await?;
        artifact_checks += w.artifact_checks;
        mid_creates += w.mid_creates;
        mid_drains += w.mid_drains;
        parks += w.parks;
        if std::env::var("CAIRN_MODEL_STATS").is_ok() {
            eprintln!(
                "seed {seed}: {} deaths, {} rebuilds, {} invocations, {} creations acked, {} completions acked, {} purged, {} inbound",
                w.crashes,
                w.rebuilds,
                w.plans.len(),
                w.acked_created.len(),
                w.acked_done.len(),
                w.purged.len(),
                w.inbound_next
            );
        }
    }
    // Create commands slipping in between two lock-rebuild chunks (the queue
    // `seq` counter seeded from the durable watermark) must really happen.
    anyhow::ensure!(
        mid_creates > seeds as usize / 4,
        "create commands between two lock-rebuild chunks hardly ever happen: {mid_creates} times / {seeds} seeds"
    );
    // A drain round stopping between two chunks (another step slipping in)
    // must really happen.
    eprintln!("drain chunks stopping mid-round: {mid_drains} times / {seeds} seeds");
    anyhow::ensure!(
        mid_drains > seeds as usize / 4,
        "the drain round hardly ever stops between two chunks: {mid_drains} times / {seeds} seeds"
    );
    // A background turn parking a ticket and then being granted after another
    // step slipped in (a background tick, the drainer, a create command, a
    // death) must really happen.
    eprintln!("parked tickets: {parks} times / {seeds} seeds");
    anyhow::ensure!(
        parks > seeds as usize,
        "background turns hardly ever park a ticket: {parks} times / {seeds} seeds"
    );
    anyhow::ensure!(
        artifact_checks > seeds as usize,
        "the artifact byte counter is hardly ever compared: {artifact_checks} times / {seeds} seeds"
    );
    Ok(())
}
