// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! "Hello durable" on one node, on real SlateDB.
//!
//! This is where the driver's two contracts are **enforced**, not just
//! described: durable before sending, and pending work lives in the state
//! table in the same commit as the journal entry that produced it.
//!
//! The most important test is [`crash_at_every_step_then_restart`] — it is the
//! core durability criterion, but run through real storage rather than an
//! in-RAM journal.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, Invocation, InvocationStatus, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Event, Outcome, StartParams},
    cairn_store::{LeadershipGrant, PartitionId, PartitionWriter},
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{collections::VecDeque, path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";
const INV: [u8; 24] = [0x77; 24];
const SERVICE: &str = "OrderService";
const OBJECT: &str = "order-42";
/// The absolute wake-up time of `ctx.sleep` (ms since the epoch).
const WAKE_AT: u64 = 1_700_000_000_000;

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: SERVICE.into(),
        handler: "submit".into(),
        object_key: OBJECT.into(),
    }
}

/// The `owner_pk` of the state of `OrderService/order-42`.
fn owner() -> cairn_store::PartitionKey {
    cairn_store::keys::object_owner(&cairn_store::TenantId::default(), None, SERVICE, OBJECT)
        .expect("has an object key")
}

// ───────────────────────────────────────────────────────────── fake SDK

#[derive(Clone, Debug)]
enum Step {
    GetState {
        key: &'static str,
        id: u32,
    },
    Sleep {
        at: u64,
        id: u32,
    },
    Run {
        id: u32,
        result: &'static str,
    },
    SetState {
        key: &'static str,
        value: &'static str,
    },
    Output {
        value: &'static str,
    },
}

fn sample_program() -> Vec<Step> {
    vec![
        Step::GetState {
            key: "count",
            id: 1,
        },
        Step::Sleep { at: WAKE_AT, id: 2 },
        Step::Run {
            id: 3,
            result: "charged",
        },
        Step::SetState {
            key: "count",
            value: "1",
        },
        Step::Output { value: "done" },
    ]
}

struct Sdk {
    program: Vec<Step>,
    cursor: usize,
    emitted: std::collections::BTreeSet<usize>,
    proposed: std::collections::BTreeSet<usize>,
    done: std::collections::BTreeSet<u32>,
    known: u32,
    seen: u32,
    replayed_commands: usize,
    ended: bool,
    side_effect_runs: u32,
}

impl Sdk {
    fn new(program: Vec<Step>) -> Self {
        Self {
            program,
            cursor: 0,
            emitted: Default::default(),
            proposed: Default::default(),
            done: Default::default(),
            known: 0,
            seen: 0,
            replayed_commands: 0,
            ended: false,
            side_effect_runs: 0,
        }
    }

    fn on_message(&mut self, msg: &Message) -> Vec<Frame> {
        if let Message::Start(s) = msg {
            self.known = s.known_entries;
            self.seen = 0;
            return if self.known == 0 {
                self.drain()
            } else {
                vec![]
            };
        }
        if self.seen < self.known {
            self.seen += 1;
            if !matches!(msg, Message::Input(_)) {
                match completion_id(msg) {
                    Some(id) => {
                        self.done.insert(id);
                    }
                    None => {
                        // A replayed command: mark it as already in the journal.
                        self.emitted.insert(self.replayed_commands);
                        self.replayed_commands += 1;
                    }
                }
            }
            return if self.seen == self.known {
                self.drain()
            } else {
                vec![]
            };
        }
        match msg {
            Message::CommandAck(_) => vec![],
            Message::ProposeRunCompletionAck(a) => {
                self.done.insert(a.completion_id);
                self.drain()
            }
            other => {
                if let Some(id) = completion_id(other) {
                    self.done.insert(id);
                }
                self.drain()
            }
        }
    }

    fn drain(&mut self) -> Vec<Frame> {
        let mut out = Vec::new();
        while self.cursor < self.program.len() {
            let step = self.program[self.cursor].clone();
            if !self.emitted.contains(&self.cursor) {
                self.emit(&step, &mut out);
                self.emitted.insert(self.cursor);
            }
            if let Step::Run { id, result } = &step {
                if !self.done.contains(id) && !self.proposed.contains(&self.cursor) {
                    self.side_effect_runs += 1;
                    self.proposed.insert(self.cursor);
                    out.push(frame(
                        MessageType::PROPOSE_RUN_COMPLETION,
                        Flags::REQUESTED_ACK,
                        &pb::ProposeRunCompletionMessage {
                            result_completion_id: *id,
                            result: Some(pb::propose_run_completion_message::Result::Value(
                                Bytes::from_static(result.as_bytes()),
                            )),
                        },
                    ));
                }
            }
            match awaited(&step) {
                Some(id) if !self.done.contains(&id) => break,
                _ => self.cursor += 1,
            }
        }
        if self.cursor >= self.program.len() && !self.ended {
            self.ended = true;
            out.push(frame(MessageType::END, Flags::NONE, &pb::EndMessage {}));
        }
        out
    }

    fn emit(&mut self, step: &Step, out: &mut Vec<Frame>) {
        match step {
            Step::GetState { key, id } => out.push(frame(
                MessageType::GET_STATE,
                Flags::NONE,
                &pb::GetStateCommandMessage {
                    key: Bytes::from_static(key.as_bytes()),
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            Step::Sleep { at, id } => out.push(frame(
                MessageType::SLEEP,
                Flags::NONE,
                &pb::SleepCommandMessage {
                    wake_up_time: *at,
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            Step::Run { id, .. } => out.push(frame(
                MessageType::RUN,
                Flags::NONE,
                &pb::RunCommandMessage {
                    result_completion_id: *id,
                    name: String::new(),
                },
            )),
            Step::SetState { key, value } => out.push(frame(
                MessageType::SET_STATE,
                Flags::NONE,
                &pb::SetStateCommandMessage {
                    key: Bytes::from_static(key.as_bytes()),
                    value: Some(cairn_protocol::value::inline(Bytes::from_static(
                        value.as_bytes(),
                    ))),
                    name: String::new(),
                },
            )),
            Step::Output { value } => out.push(frame(
                MessageType::OUTPUT,
                Flags::NONE,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(value.as_bytes())),
                    )),
                    name: String::new(),
                },
            )),
        }
    }
}

fn awaited(step: &Step) -> Option<u32> {
    match step {
        Step::GetState { id, .. } | Step::Sleep { id, .. } | Step::Run { id, .. } => Some(*id),
        _ => None,
    }
}

fn completion_id(msg: &Message) -> Option<u32> {
    Some(match msg {
        Message::GetStateCompletion(n) => n.completion_id,
        Message::SleepCompletion(n) => n.completion_id,
        Message::RunCompletion(n) => n.completion_id,
        _ => return None,
    })
}

fn frame<M: prost::Message>(ty: MessageType, flags: Flags, m: &M) -> Frame {
    let wire = encode_message(ty, flags, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

// ───────────────────────────────────────────────────────────── infrastructure

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn open_engine(root: &PathBuf) -> Result<Engine> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
    let settings = Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    };
    let db = Db::builder(DB, store)
        .with_settings(settings)
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

/// Runs the invocation until it stops, or until `budget` `step`s are used up
/// (simulating a process dying midway). Returns `true` if cut off by the budget.
async fn drive(
    engine: &mut Engine,
    inv: &mut Invocation,
    sdk: &mut Sdk,
    mut budget: Option<usize>,
    first: Vec<Message>,
) -> Result<bool> {
    let mut queue: VecDeque<Message> = VecDeque::from(first);
    loop {
        while let Some(m) = queue.pop_front() {
            for f in sdk.on_message(&m) {
                if let Some(b) = budget.as_mut() {
                    if *b == 0 {
                        return Ok(true);
                    }
                    *b -= 1;
                }
                queue.extend(engine.step(inv, Event::FromSdk(f)).await?);
            }
        }
        if inv.outcome().is_some() {
            return Ok(false);
        }
        // Nothing left to do ⇒ waiting for a timer. Jump the clock to the deadline.
        let due = engine.due_timers(WAKE_AT, usize::MAX).await?;
        let Some((due_ms, _, cid)) = due.into_iter().find(|(_, id, _)| *id == inv.id) else {
            return Ok(false); // really stuck — let the test assert it
        };
        if let Some(b) = budget.as_mut() {
            if *b == 0 {
                return Ok(true);
            }
            *b -= 1;
        }
        queue.extend(engine.fire_timer(inv, due_ms, cid).await?);
    }
}

async fn run_to_completion(root: &PathBuf) -> Result<(Engine, Invocation, Sdk)> {
    let mut engine = open_engine(root).await?;
    let mut inv = engine
        .create(
            INV,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"req"),
        )
        .await?;
    let mut sdk = Sdk::new(sample_program());
    let first = engine.step(&mut inv, Event::Start).await?;
    let cut = drive(&mut engine, &mut inv, &mut sdk, None, first).await?;
    assert!(!cut);
    Ok((engine, inv, sdk))
}

// ───────────────────────────────────────────────────────────── test

/// An invocation using `ctx.run` + `ctx.sleep` + state runs correctly from start
/// to end, and everything it leaves behind is **durable**.
#[tokio::test]
async fn hello_durable() -> Result<()> {
    let root = fresh_root("hello")?;
    let (mut engine, inv, sdk) = run_to_completion(&root).await?;

    match inv.outcome() {
        Some(Outcome::Completed(o)) => {
            let v = match &o.result {
                Some(pb::output_command_message::Result::Value(v)) => v.content.clone(),
                other => panic!("wrong output: {other:?}"),
            };
            assert_eq!(v, Bytes::from_static(b"done"));
        }
        other => panic!("not completed: {other:?}"),
    }
    assert_eq!(sdk.side_effect_runs, 1, "ctx.run must run exactly once");

    // Status and state are both durable.
    assert_eq!(
        engine.status(&INV).await?,
        Some(InvocationStatus::Completed(
            Bytes::from_static(b"done").into()
        ))
    );
    assert_eq!(
        engine
            .writer()
            .state(
                owner(),
                &cairn_store::TenantId::default(),
                None,
                SERVICE,
                OBJECT,
                b"count"
            )
            .await?,
        Some(Bytes::from_static(b"1")),
        "SetState not durable"
    );
    // A processed timer must disappear from the table, otherwise it fires again forever.
    assert!(
        engine.due_timers(WAKE_AT, usize::MAX).await?.is_empty(),
        "a fired timer is still in the table"
    );
    engine.seal().await?;
    Ok(())
}

/// **The core durability criterion.**
///
/// Die at every step, restart from exactly what was durable, and the result
/// must be identical. Unlike the test of the same name in `cairn-runtime`: here
/// the journal goes through real SlateDB, so it also forces the storage layer
/// and the commit order.
#[tokio::test]
async fn crash_at_every_step_then_restart() -> Result<()> {
    // The reference run, in its own directory.
    let reference_root = fresh_root("reference")?;
    let (engine, _, _) = run_to_completion(&reference_root).await?;
    let reference_journal = engine.writer().journal_unbounded(&INV).await?;
    let reference_status = engine.status(&INV).await?;
    let step_count = reference_journal.len();
    engine.seal().await?;
    assert!(step_count >= 6, "program too short to be meaningful");

    for k in 1..=step_count {
        let root = fresh_root(&format!("crash-{k}"))?;

        // ── Run one: k steps run, then the process dies.
        {
            let mut engine = open_engine(&root).await?;
            let mut inv = engine
                .create(
                    INV,
                    &target(),
                    CreateOptions::default(),
                    StartParams::default(),
                    Bytes::from_static(b"req"),
                )
                .await?;
            let mut sdk = Sdk::new(sample_program());
            let first = engine.step(&mut inv, Event::Start).await?;
            drive(&mut engine, &mut inv, &mut sdk, Some(k), first).await?;
            // Force what was just written to be durable, **then** die. That is
            // the right place to die for this test: the server lets a turn
            // touch the outside (answer the client, call the SDK, send a
            // cross-partition message) only after forcing durability, so what a
            // real `kill -9` leaves behind is always "durable up to some step".
            // Dying *before* the forced flush is also correct, but it simulates
            // a death that nobody outside ever knew had done anything.
            engine.writer().wait_durable().await?;
            // No `seal` — a sudden death, exactly like a crash.
            drop(engine);
        }

        // ── Run two: a new leader reloads from storage.
        let mut engine = open_engine(&root).await?;
        let mut inv = engine.load(INV, StartParams::default()).await?;
        let mut sdk = Sdk::new(sample_program());
        let first = engine.step(&mut inv, Event::Start).await?;
        let cut = drive(&mut engine, &mut inv, &mut sdk, None, first).await?;
        assert!(!cut, "k={k}: cannot continue after the restart");

        assert_eq!(
            engine.writer().journal_unbounded(&INV).await?,
            reference_journal,
            "k={k}: journal after recovery differs from the reference journal"
        );
        assert_eq!(
            engine.status(&INV).await?,
            reference_status,
            "k={k}: result differs"
        );
        assert_eq!(
            engine
                .writer()
                .state(
                    owner(),
                    &cairn_store::TenantId::default(),
                    None,
                    SERVICE,
                    OBJECT,
                    b"count"
                )
                .await?,
            Some(Bytes::from_static(b"1")),
            "k={k}: state does not match"
        );
        assert!(
            engine.due_timers(WAKE_AT, usize::MAX).await?.is_empty(),
            "k={k}: timer left over after recovery"
        );
        engine.seal().await?;
    }
    Ok(())
}

/// INV-3 in its most concrete form: pending work must live in **the state table**, not
/// trong RAM.
///
/// Runs until `ctx.sleep` is waiting, then kills the process. The timer must
/// still be in the `timers` table and readable by a completely new engine — if
/// it lived only in memory, this invocation would sleep forever.
///
/// **Why there is a `wait_durable()` before "killing".** `commit` no longer
/// waits for durability itself — it pushes into the WAL and returns, so that
/// many turns share one flush (`cairn-store/tests/group_commit.rs`). The
/// lose-on-death window therefore widens by exactly one flush, and that is safe
/// **precisely because** nothing goes out within that window: the server only
/// answers the client, calls the SDK or sends cross-partition messages after
/// forcing durability (see `Shared::finish`, `awaiting_sdk`,
///
/// So the right place to "kill" in this test is **after** that boundary.
/// Without the forced flush the test still goes red, but red because it
/// simulates a death production never exposes — i.e. it measures the wrong
/// thing. What this test guards is INV-3: *pending work lives in the state
/// table, not in RAM*.
#[tokio::test]
async fn pending_timer_survives_restart() -> Result<()> {
    let root = fresh_root("timer-survives")?;
    {
        let mut engine = open_engine(&root).await?;
        let mut inv = engine
            .create(
                INV,
                &target(),
                CreateOptions::default(),
                StartParams::default(),
                Bytes::from_static(b"req"),
            )
            .await?;
        let mut sdk = Sdk::new(sample_program());
        let first = engine.step(&mut inv, Event::Start).await?;
        // Run only until the sleep command is durable: GetState, then Sleep.
        drive(&mut engine, &mut inv, &mut sdk, Some(2), first).await?;
        engine.writer().wait_durable().await?;
        drop(engine);
    }

    let mut engine = open_engine(&root).await?;
    let timers = engine.due_timers(WAKE_AT, usize::MAX).await?;
    assert_eq!(
        timers,
        vec![(WAKE_AT, INV, 2)],
        "a pending timer is not in the state table — INV-3 broken"
    );
    // Not due yet ⇒ must not be picked up.
    assert!(engine.due_timers(WAKE_AT - 1, usize::MAX).await?.is_empty());
    engine.seal().await?;
    Ok(())
}

/// A state mutation and the journal entry that produced it must become durable
/// **at the same time**.
///
/// The state machine does not replay `WriteState` on replay. So if the two
/// were in different commits, a death exactly between them would leave the
/// journal saying "set" and the store saying "not yet" — and no replay can fix
/// it.
#[tokio::test]
async fn state_and_journal_durable_in_one_commit() -> Result<()> {
    let root = fresh_root("atomic")?;
    let (engine, _, _) = run_to_completion(&root).await?;
    engine.seal().await?;

    // A new engine reads back from storage: if the journal has SetState, the state must have a value.
    let engine = open_engine(&root).await?;
    let journal = engine.writer().journal_unbounded(&INV).await?;
    let has_set_state = journal.iter().any(|(_, b)| {
        cairn_runtime::JournalEntry::decode(b.clone())
            .map(|e| e.ty == MessageType::SET_STATE)
            .unwrap_or(false)
    });
    assert!(has_set_state, "no SetState in journal: bad program");
    assert_eq!(
        engine
            .writer()
            .state(
                owner(),
                &cairn_store::TenantId::default(),
                None,
                SERVICE,
                OBJECT,
                b"count"
            )
            .await?,
        Some(Bytes::from_static(b"1")),
        "the journal has SetState but the state is empty — the two are not in one commit"
    );
    engine.seal().await?;
    Ok(())
}

/// A journal with holes is broken data, not "temporarily missing" — it must
/// report an error rather than replay on with shifted indexes.
#[tokio::test]
async fn journal_with_holes_is_rejected() -> Result<()> {
    let root = fresh_root("gap")?;
    let mut engine = open_engine(&root).await?;
    let mut inv = engine
        .create(
            INV,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"req"),
        )
        .await?;
    let mut sdk = Sdk::new(sample_program());
    let first = engine.step(&mut inv, Event::Start).await?;
    drive(&mut engine, &mut inv, &mut sdk, Some(2), first).await?;

    // Insert an entry at an index far ahead, making a hole.
    let mut muts = cairn_store::Mutations::new();
    muts.append_journal(&INV, 99, b"\x04\x00");
    engine.writer_mut().commit(muts).await?;

    let err = engine
        .load(INV, StartParams::default())
        .await
        .expect_err("a journal with holes still loaded");
    assert!(
        matches!(err, cairn_engine::EngineError::JournalGap(_)),
        "wrong error kind: {err}"
    );
    Ok(())
}
