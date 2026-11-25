// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The trace set's scenarios — each points at the harness test it was taken from.
//!
//! Handler names (`counter`, `sleepRunSleep`, …) are a contract with the
//! runner: every SDK implements a handler of the same name and behaviour
//! (`conformance/README.md`, Handlers section).

use {
    super::{
        sdk::{Op, SigKey, Transport},
        sim::{self, Fire, Inject, Scenario, SignalFire},
    },
    cairn_protocol::MessageType,
};

const MANDATORY: &str = "crates/cairn-runtime/tests/mandatory.rs";
const PHASE4: &str = "crates/cairn-runtime/tests/phase4.rs";

/// `sample_program()` of `mandatory.rs`: read state, sleep, a side effect, write state.
fn counter_ops() -> Vec<Op> {
    vec![
        Op::Get("count"),
        Op::Await(0),
        Op::Sleep(1_000),
        Op::Await(1),
        Op::Run {
            name: "charge",
            json: "\"charged\"",
            slow: false,
            tokens: &[],
        },
        Op::Set("count", "1"),
        Op::Return("\"done\""),
    ]
}

fn counter(transport: Transport, wire: u32) -> Scenario {
    let mut sc = Scenario::new(
        &format!("counter-{}-v{wire}", tag(transport)),
        "counter",
        transport,
        wire,
    );
    sc.description = "The program of mandatory test 1: get → sleep → run → set → return. Three \
                      suspensions (state, timer, the run's ack), the last run replays the whole \
                      journal and the SDK may only return End."
        .into();
    sc.source = vec![
        format_src(MANDATORY, "replay_is_bit_exact_at_every_kill_point"),
        format_src(MANDATORY, "durable_side_effect_does_not_rerun_on_replay"),
    ];
    sc.spec = vec![
        "4 Turns on one stream",
        "8.1",
        "8.3",
        "10 End is not a journal entry",
    ];
    sc.cells = vec![1];
    sc.ops = counter_ops();
    sc.input = "\"req\"";
    sc.state = vec![("count", "0")];
    sc
}

fn tag(t: Transport) -> &'static str {
    match t {
        Transport::Bidi => "bidi",
        Transport::Rr => "rr",
    }
}

pub fn all() -> Vec<sim::Trace> {
    let mut out = Vec::new();

    // ── Mandatory test 1: run to the end, then continue from EVERY durable prefix.
    let base = sim::run(counter(Transport::Rr, 1));
    let reference = base.journal.clone();
    out.push(base);
    out.push(sim::run(counter(Transport::Bidi, 1)));
    out.push(sim::run(counter(Transport::Bidi, 3)));
    // Prefix p = Input + (p-1) entries: the process died right after persist
    // number p-1. The full prefix (dying between Output and End) is the
    // `verify` connection every completed trace has.
    for p in 2..reference.len() {
        let mut sc = counter(Transport::Rr, 1);
        sc.name = format!("counter-resume-{p:02}-rr-v1");
        sc.description = format!(
            "A new leader picks up the journal durable up to entry {} (the process died right \
             after persist number {}). The SDK continuing must produce exactly the journal of the \
             run that did not die.",
            p - 1,
            p - 1
        );
        sc.initial = Some(reference[..p].to_vec());
        let t = sim::run(sc);
        assert_eq!(
            t.journal, reference,
            "{}: the journal differs from the run that did not die",
            t.scenario.name
        );
        out.push(t);
    }

    // ── Mandatory test 4: the code changed between two runs ⇒ 570, PAUSE.
    {
        let mut sc = Scenario::new("journal-mismatch-rr-v1", "counterChanged", Transport::Rr, 1);
        sc.description = "The journal has get → sleep; the new code does get → set. The SDK must \
                          report 570 (JOURNAL_MISMATCH) with behavior PAUSE, emitting no command."
            .into();
        sc.source = vec![
            "crates/cairn-runtime/tests/mandatory.rs::journal_mismatch_reports_570_without_retry"
                .into(),
        ];
        sc.spec = vec!["6", "8.5"];
        sc.cells = vec![4];
        sc.ops = vec![
            Op::Get("count"),
            Op::Await(0),
            Op::Set("count", "9"),
            Op::Return("\"other\""),
        ];
        sc.input = "\"req\"";
        sc.state = vec![("count", "0")];
        sc.initial = Some(reference[..4].to_vec());
        out.push(sim::run(sc));
    }

    // ── Same kind, different name: the journal's `Run` is a different step ⇒
    //    570, the closure does not run.
    {
        let mut sc = Scenario::new("run-renamed-rr-v1", "counterRenamed", Transport::Rr, 1);
        sc.description = "The journal has get → sleep → Run \"charge\" (no result yet); the new \
                          code calls run(\"charge-v2\") at exactly that place. Same entry kind but \
                          a different name is a different step: the SDK must report 570 with \
                          behavior PAUSE, not run the closure, and emit no command."
            .into();
        sc.source = vec!["sdk/typescript/test/handler.test.ts".into()];
        sc.spec = vec!["6", "8.5"];
        sc.cells = vec![4];
        sc.ops = counter_ops();
        if let Some(Op::Run { name, .. }) = sc.ops.iter_mut().find(|o| matches!(o, Op::Run { .. }))
        {
            *name = "charge-v2";
        }
        sc.input = "\"req\"";
        sc.state = vec![("count", "0")];
        let run = reference
            .iter()
            .position(|e| e.ty == MessageType::RUN)
            .expect("the counter journal has a Run");
        sc.initial = Some(reference[..=run].to_vec());
        out.push(sim::run(sc));
    }

    // ── Cell 2: notifications arriving out of order.
    {
        let mut sc = Scenario::new("out-of-order-bidi-v3", "outOfOrder", Transport::Bidi, 3);
        sc.description = "Two pending sleeps; the runtime returns completion 2 then 1 on the open \
                          stream. The SDK must match by id, not by position."
            .into();
        sc.source = vec![format_src(MANDATORY, "out_of_order_notifications")];
        sc.spec = vec!["8.1"];
        sc.cells = vec![2];
        sc.ops = out_of_order_ops();
        sc.fire = vec![
            Fire {
                after_sdk_frames: Some(2),
                id: 2,
            },
            Fire {
                after_sdk_frames: Some(2),
                id: 1,
            },
        ];
        out.push(sim::run(sc));

        let mut sc = Scenario::new("out-of-order-rr-v1", "outOfOrder", Transport::Rr, 1);
        sc.description = "Like out-of-order-bidi-v3 but outside a turn: completion 2 is durable \
                          before 1, so the replayed part of the next turn carries them in the \
                          reverse of declaration order."
            .into();
        sc.source = vec![format_src(MANDATORY, "out_of_order_notifications")];
        sc.spec = vec!["8.1", "P3"];
        sc.cells = vec![2];
        sc.ops = out_of_order_ops();
        sc.fire = vec![
            Fire {
                after_sdk_frames: None,
                id: 2,
            },
            Fire {
                after_sdk_frames: None,
                id: 1,
            },
        ];
        out.push(sim::run(sc));
    }

    // ── Cell 3: run/ack order (spec section 8.2).
    {
        let mut sc = Scenario::new("run-ack-order-bidi-v3", "sleepRunSleep", Transport::Bidi, 3);
        sc.description = "Exactly the scenario of spec section 8.2: the first run sees \
                          SleepCompletion(1), ProposeRunCompletionAck(3), SleepCompletion(2); the \
                          verify connection replays SleepCompletion(1), RunCompletion(3), \
                          SleepCompletion(2) — the ack takes exactly the place of RunCompletion."
            .into();
        sc.source = vec![format_src(
            MANDATORY,
            "run_ack_order_is_preserved_when_the_journal_is_replayed",
        )];
        sc.spec = vec!["8.2", "4 Turns on one stream"];
        sc.cells = vec![3];
        sc.ops = sleep_run_sleep_ops();
        // SDK frames: 1 Sleep, 2 Sleep, 3 Run, 4 ProposeRunCompletion, 5 Suspension.
        sc.fire = vec![
            Fire {
                after_sdk_frames: Some(3),
                id: 1,
            },
            Fire {
                after_sdk_frames: Some(5),
                id: 2,
            },
        ];
        out.push(sim::run(sc));

        let mut sc = Scenario::new("run-no-ack-rr-v1", "sleepRunSleep", Transport::Rr, 1);
        sc.description = "Spec section 8.2 in REQUEST_RESPONSE: the SDK MUST NOT ask for an ack \
                          (section 4), the runtime records RunCompletion right after \
                          SleepCompletion(1) and replays it at exactly that place."
            .into();
        sc.source = vec![format_src(
            MANDATORY,
            "without_ack_request_notification_is_sent_directly",
        )];
        sc.spec = vec!["8.2", "4 Transport"];
        sc.cells = vec![3];
        sc.ops = sleep_run_sleep_ops();
        sc.fire = vec![Fire {
            after_sdk_frames: Some(3),
            id: 1,
        }];
        out.push(sim::run(sc));
    }

    // ── Phase 4 (wire 1): eager state, state keys, promises, suspension trees.
    {
        let mut sc = Scenario::new("eager-state-bidi-v1", "eagerCount", Transport::Bidi, 1);
        sc.description =
            "Start carries state_map: ctx.get answers locally with GetEagerState (the \
                          result embedded in the command), no completion."
                .into();
        sc.source = vec![
            format_src(PHASE4, "start_carries_state_map"),
            format_src(PHASE4, "eager_state_journal_only"),
        ];
        sc.spec = vec!["5", "9"];
        sc.ops = vec![
            Op::Get("count"),
            Op::Await(0),
            Op::Set("count", "1"),
            Op::Return("\"done\""),
        ];
        sc.state = vec![("count", "0")];
        sc.eager_state = true;
        out.push(sim::run(sc));

        let mut sc = Scenario::new("state-keys-rr-v1", "stateKeys", Transport::Rr, 1);
        sc.description =
            "ctx.getKeys() on the lazy path: GetStateKeys promises completion 0x8006.".into();
        sc.source = vec![format_src(PHASE4, "get_state_keys_full_lifecycle")];
        sc.spec = vec!["5", "8.1"];
        sc.ops = vec![Op::GetKeys, Op::Await(0), Op::Return("[\"a\",\"b\"]")];
        sc.state = vec![("a", "1"), ("b", "2")];
        out.push(sim::run(sc));

        let mut sc = Scenario::new("promise-bidi-v1", "promise", Transport::Bidi, 1);
        sc.description = "CompletePromise then GetPromise on the same promise.".into();
        sc.source = vec![format_src(PHASE4, "promise_get_and_complete_yield_tasks")];
        sc.spec = vec!["5"];
        sc.ops = vec![
            Op::PromiseResolve("approve", "\"ok\""),
            Op::PromiseGet("approve"),
            Op::Await(0),
            Op::Return("\"ok\""),
        ];
        out.push(sim::run(sc));

        let mut sc = Scenario::new("all-combinator-bidi-v3", "allOf", Transport::Bidi, 3);
        sc.description = "ctx.all over two sleeps: the Suspension carries an ALL_COMPLETED node. \
                          One completion is not enough — the runtime closes the stream; only with \
                          both does it open a new turn."
            .into();
        sc.source = vec![format_src(
            PHASE4,
            "suspension_satisfied_follows_combinator",
        )];
        sc.spec = vec!["8.3", "4 Turns on one stream"];
        sc.ops = vec![
            Op::Sleep(1_000),
            Op::Sleep(2_000),
            Op::AwaitAll(vec![0, 1]),
            Op::Return("\"both\""),
        ];
        sc.fire = vec![Fire {
            after_sdk_frames: Some(2),
            id: 1,
        }];
        out.push(sim::run(sc));
    }

    // ── Signals as combinator leaves — bounded waits.
    //    Added only; the 28 older traces keep their bytes (CG-5).
    {
        let race_ops = || {
            vec![
                Op::Sleep(60_000),
                Op::Signal(SigKey::Name("approve")),
                Op::Race(vec![0, 1]),
                Op::ReturnWinner("\"sleep\""),
            ]
        };
        let mut sc = Scenario::new(
            "race-signal-vs-sleep-bidi-v4",
            "raceSignal",
            Transport::Bidi,
            4,
        );
        sc.description = "ctx.race([sleep(60000), signal(\"approve\")]): the Suspension is ONE \
                          tree — the sleep's completion (field 1) and the signal name (field 3). \
                          The signal arrives before the timer ⇒ the signal wins, the result is its \
                          payload."
            .into();
        sc.source = vec!["sdk/typescript/test/handler.test.ts".into()];
        sc.spec = vec!["7", "8.3", "8.7"];
        sc.ops = race_ops();
        sc.signals = vec![SignalFire {
            id: SigKey::Name("approve"),
            json: "\"ok\"",
        }];
        out.push(sim::run(sc));

        let mut sc = Scenario::new("race-signal-vs-sleep-rr-v3", "raceSignal", Transport::Rr, 3);
        sc.description = "Like race-signal-vs-sleep-bidi-v4 in REQUEST_RESPONSE, but nobody sends \
                          the signal: the timer fires ⇒ the sleep wins."
            .into();
        sc.source = vec!["sdk/typescript/test/handler.test.ts".into()];
        sc.spec = vec!["7", "8.3"];
        sc.ops = race_ops();
        out.push(sim::run(sc));

        let mut sc = Scenario::new(
            "race-awakeable-vs-sleep-bidi-v4",
            "raceAwakeable",
            Transport::Bidi,
            4,
        );
        sc.description = "ctx.race([awakeable().promise, sleep(60000)]): the awakeable is signal \
                          number 17 (field 2) next to the sleep's completion (field 1). An outside \
                          system resolves it ⇒ the awakeable wins."
            .into();
        sc.source = vec!["crates/cairn-server/src/ingress/external.rs::complete_awakeable".into()];
        sc.spec = vec!["7", "8.3", "8.7"];
        sc.ops = vec![
            Op::Awakeable,
            Op::Sleep(60_000),
            Op::Race(vec![0, 1]),
            Op::ReturnWinner("\"sleep\""),
        ];
        sc.signals = vec![SignalFire {
            id: SigKey::Idx(17),
            json: "\"approve\"",
        }];
        out.push(sim::run(sc));

        let mut sc = Scenario::new(
            "all-completion-and-signal-bidi-v4",
            "allSignal",
            Transport::Bidi,
            4,
        );
        sc.description =
            "ctx.all([sleep(1000), signal(4)]): an ALL_COMPLETED node carrying both a \
                          completion and a signal. The signal arrives first, the timer later: one \
                          leaf is not enough — the runtime closes the stream; only with both does \
                          it open a new turn."
                .into();
        sc.source = vec![format_src(
            PHASE4,
            "suspension_satisfied_follows_combinator",
        )];
        sc.spec = vec!["7", "8.3"];
        sc.ops = vec![
            Op::Sleep(1_000),
            Op::Signal(SigKey::Idx(4)),
            Op::AwaitAll(vec![0, 1]),
            Op::ReturnValueOf(1),
        ];
        sc.signals = vec![SignalFire {
            id: SigKey::Idx(4),
            json: "7",
        }];
        out.push(sim::run(sc));
    }

    // ── Wire version negotiation.
    {
        let mut sc = Scenario::new("scoped-call-bidi-v2", "scopedCall", Transport::Bidi, 2);
        sc.description =
            "Wire 2: ctx.scoped(...).call carries scope (field 7) and limit_key (8).".into();
        sc.source = vec!["crates/cairn-server/src/discovery.rs::negotiate_wire_version".into()];
        sc.spec = vec!["2", "9.1"];
        sc.ops = scoped_call_ops();
        sc.call_result = "42";
        out.push(sim::run(sc));

        let mut sc = Scenario::new("scoped-call-refused-rr-v1", "scopedCall", Transport::Rr, 1);
        sc.description = "Wire 1: an old runtime silently ignores scope, so the SDK refuses \
                          ctx.scoped with a terminal error (Output failure + End), emitting no \
                          Call."
            .into();
        sc.source = vec!["crates/cairn-server/src/discovery.rs::negotiate_wire_version".into()];
        sc.spec = vec!["2", "9.1"];
        sc.ops = scoped_call_ops();
        out.push(sim::run(sc));

        let mut sc = Scenario::new("spread-call-rr-v3", "spreadCall", Transport::Rr, 3);
        sc.description = "ctx.call(..., { placement: \"spread\" }) carries placement = \
                          CALL_PLACEMENT_SPREAD in field 13, after name (12); the wire version \
                          does not change."
            .into();
        sc.source = vec!["crates/cairn-engine/src/engine/effects.rs::callee_pk_for".into()];
        sc.spec = vec!["7", "9"];
        sc.ops = vec![
            Op::SpreadCall {
                service: "Callee",
                handler: "work",
                input: "1",
            },
            Op::Await(0),
            Op::Return("42"),
        ];
        sc.call_result = "42";
        out.push(sim::run(sc));

        let mut sc = Scenario::new("replay-only-rr-v3", "counter", Transport::Rr, 3);
        sc.description = "Wire 3: Start.replay_only — the SDK stops at the first command beyond \
                          the journal, before any closure, and ends with ReplayReport { matched = \
                          1 }."
            .into();
        sc.source = vec!["crates/cairn-server/src/replay.rs".into()];
        sc.spec = vec!["9.1 Trial replay (v3)"];
        sc.ops = counter_ops();
        sc.input = "\"req\"";
        sc.state = vec![("count", "0")];
        sc.replay_only = true;
        sc.initial = Some(reference[..3].to_vec());
        out.push(sim::run(sc));
    }

    // ── The side channel: frames on the connection, never an entry, waiting for no ack.
    {
        let mut sc = Scenario::new("keepalive-side-bidi-v3", "slowRun", Transport::Bidi, 3);
        sc.description = "Wire 3, Start.keepalive_interval_ms = 20: the Run command goes on the \
                          wire before the closure, then Keepalive (side channel) until the closure \
                          finishes. The runner holds the closure until it sees the first \
                          Keepalive; extra Keepalives are ignored."
            .into();
        sc.source = vec!["crates/cairn-server/src/turn/sdk.rs::drive_bidi".into()];
        sc.spec = vec!["9.1 Keepalive (v3)", "9.2"];
        sc.ops = vec![
            Op::Run {
                name: "slow",
                json: "\"done\"",
                slow: true,
                tokens: &[],
            },
            Op::Return("\"done\""),
        ];
        sc.keepalive_ms = Some(20);
        out.push(sim::run(sc));

        // Ephemeral streams (wire v4): `StreamAppend` is a side channel — no
        // entry, no ack — and replay-aware.
        for (transport, tag) in [(Transport::Bidi, "bidi"), (Transport::Rr, "rr")] {
            let mut sc = Scenario::new(&format!("stream-side-{tag}-v4"), "streamRun", transport, 4);
            sc.description = "Wire 4: a ctx.run closure appends two records to the ephemeral \
                              stream `ui`. The SDK sends one StreamAppend (side channel) before \
                              the turn-ending frame, identified by (command position, attempt, k); \
                              the journal has no entry of it, and a rerun (step already journaled) \
                              does not send it again."
                .into();
            sc.source = vec![
                "crates/cairn-server/src/turn/sdk.rs::side_frame".into(),
                "crates/cairn-server/tests/streams.rs".into(),
            ];
            sc.spec = vec!["9.2", "streams on the side channel"];
            sc.ops = vec![
                Op::Run {
                    name: "llm",
                    json: "\"ab\"",
                    slow: false,
                    tokens: &["{\"tok\":\"a\"}", "{\"tok\":\"b\"}"],
                },
                Op::Return("\"done\""),
            ];
            out.push(sim::run(sc));
        }

        let mut sc = counter(Transport::Bidi, 3);
        sc.name = "custom-side-bidi-v3".into();
        sc.description = "The runtime inserts a custom frame 0xFC01 (side channel) between two \
                          notifications. The SDK must not treat it as an entry: the journal \
                          indexes and every later frame stay the same."
            .into();
        sc.source =
            vec!["crates/cairn-protocol/src/frame.rs (0xFC00+ forwarded as raw bytes)".into()];
        sc.spec = vec!["5 Custom", "9.2"];
        sc.cells = vec![];
        sc.inject = vec![Inject {
            after_sdk_frames: 1,
            ty: 0xFC01,
            payload: b"cairn-conformance",
        }];
        out.push(sim::run(sc));
    }

    // ── Parallel branches: results are delivered in journal order; a branch
    //    waiting for its proposal does not block the other branch.
    for (transport, wire) in [(Transport::Rr, 1), (Transport::Bidi, 3)] {
        let mut sc = Scenario::new(
            &format!("parallel-branches-{}-v{wire}", tag(transport)),
            "parallelBranches",
            transport,
            wire,
        );
        sc.description = "Promise.all of two branches, each branch two synchronous ctx.run calls. \
                          The first turn emits Run a1, Run b1 then two proposals, suspending on \
                          FIRST(1, 2) — every proposal of the turn. The next turn delivers the \
                          RunCompletions in journal order: branch a goes on (Run a2 + proposal) \
                          first, then branch b; the command sequence is a function of the journal."
            .into();
        sc.source = vec!["sdk/typescript/test/parallel.test.ts".into()];
        sc.spec = vec!["4", "8.1", "parallel branches"];
        sc.ops = vec![
            Op::Branches(vec![
                vec![("a1", "\"a1\""), ("a2", "\"a2\"")],
                vec![("b1", "\"b1\""), ("b2", "\"b2\"")],
            ]),
            Op::Return("[[\"a1\",\"a2\"],[\"b1\",\"b2\"]]"),
        ];
        out.push(sim::run(sc));
    }

    // ── A command matches the journal by every field, not only by kind.
    {
        let mut src = Scenario::new("call-original", "callRetargeted", Transport::Rr, 1);
        src.ops = vec![
            Op::Call {
                service: "Callee",
                handler: "a",
                input: "1",
            },
            Op::Await(0),
            Op::Return("\"x\""),
        ];
        let original = sim::run(src).journal;
        let call = original
            .iter()
            .position(|e| e.ty == MessageType::CALL)
            .expect("the journal has a Call");
        let mut sc = Scenario::new("call-retargeted-rr-v1", "callRetargeted", Transport::Rr, 1);
        sc.description = "The journal has a Call to Callee/a (no result yet); the new code calls \
                          Callee/b at exactly that place. Same entry kind but a different target \
                          is a different command: the SDK must report 570 with behavior PAUSE, \
                          emitting no command."
            .into();
        sc.source = vec!["sdk/typescript/test/handler.test.ts (command diff)".into()];
        sc.spec = vec!["6", "8.5"];
        sc.cells = vec![4];
        sc.ops = vec![
            Op::Call {
                service: "Callee",
                handler: "b",
                input: "1",
            },
            Op::Await(0),
            Op::Return("\"x\""),
        ];
        sc.initial = Some(original[..=call].to_vec());
        out.push(sim::run(sc));

        let mut sc = Scenario::new(
            "state-key-changed-rr-v1",
            "stateKeyChanged",
            Transport::Rr,
            1,
        );
        sc.description = "The journal has GetState \"count\"; the new code reads \"total\" at \
                          exactly that place. The SDK must report 570 with behavior PAUSE, \
                          emitting no command."
            .into();
        sc.source = vec!["sdk/typescript/test/handler.test.ts (command diff)".into()];
        sc.spec = vec!["6", "8.5"];
        sc.cells = vec![4];
        sc.ops = vec![Op::Get("total"), Op::Await(0), Op::Return("\"x\"")];
        sc.input = "\"req\"";
        sc.state = vec![("count", "0")];
        sc.initial = Some(reference[..2].to_vec());
        out.push(sim::run(sc));
    }

    // ── The runtime speaks a version above the cap the SDK declares.
    out.extend(version_too_new());

    // ── Wire v6: segmented values.
    out.extend(segmented());

    // ── Wire v5 (spec section 9.3): incremental replay of REQUEST_RESPONSE.
    out.extend(v5_delta());
    out
}

/// Three v6 traces on a sealed `history` state: a 40-byte segment (`"` + 39
/// `a`) ‖ an inline tail (`aaaaaaaaaa"`). The runtime attaches the read URL of
/// every ref to `Start.segment_urls` (`prepare_down`, turn opening); the SDK
/// downloads the segment through the URL (the runner serves it from the trace's
/// `segments` table), checks the sha256, assembles. Writing back a value that
/// has the read prefix ⇒ the ref is reused and only the rest is sent; the shell
/// keeps the ref (`Event::Sealed`), a small tail is not sealed further. Only
/// `REQUEST_RESPONSE`: BIDI's mid-turn batches (`SegmentUrls`) follow the
/// shell's batch boundaries, which the sim does not model.
fn segmented() -> Vec<sim::Trace> {
    let history = &sim::SEGMENTS[0];
    let tail = "aaaaaaaaaa\"";
    let base = |name: &str, handler: &'static str, description: &str| {
        let mut sc = Scenario::new(name, handler, Transport::Rr, 6);
        sc.description = description.into();
        sc.source = vec![
            "crates/cairn-server/src/turn/values.rs (prepare_down, seal_frame, seal_output)".into(),
            "sdk/typescript/test/values.test.ts".into(),
        ];
        sc.spec = vec!["wire v6 segmented values", "segment URLs, reused refs"];
        sc.seg_state = vec![("history", history, tail)];
        sc
    };
    let mut out = Vec::new();

    let mut sc = base(
        "segmented-get-rr-v6",
        "segmentedGet",
        "Read segmented state and return it as is. The next turn: Start.segment_urls carries the \
         refs' URLs; the SDK downloads, checks the sha256, assembles the value; Output reuses the \
         ref (byte-identical prefix) and carries only the tail.",
    );
    sc.ops = vec![Op::Get("history"), Op::Await(0), Op::ReturnOf(0)];
    out.push(sim::run(sc));

    let mut sc = base(
        "segmented-set-reuse-tail-rr-v6",
        "segmentedSetReuseTail",
        "Read segmented state, write it back with \"b\" appended: SetState carries the ref of the \
         segment read (byte-identical prefix) and only the rest; the runtime keeps the ref \
         (Event::Sealed).",
    );
    sc.ops = vec![
        Op::Get("history"),
        Op::Await(0),
        Op::SetExtended {
            key: "history",
            from: 0,
            suffix: "b",
        },
        Op::Return("\"ok\""),
    ];
    out.push(sim::run(sc));

    let mut sc = base(
        "segmented-run-result-rr-v6",
        "segmentedRunResult",
        "Read segmented state, a ctx.run returns it with \"c\" appended: ProposeRunCompletion \
         carries segmented_value (reused ref + the rest), RunCompletion goes into the journal \
         segmented and the next turn the SDK reassembles it from the URL.",
    );
    sc.ops = vec![
        Op::Get("history"),
        Op::Await(0),
        Op::RunExtended {
            name: "extend",
            from: 0,
            suffix: "c",
        },
        Op::Return("\"ok\""),
    ];
    out.push(sim::run(sc));
    out
}

/// The deployment settled on v4, the SDK process was downgraded to a version
/// declaring a v3 cap without re-registering: the SDK must return `415` before
/// running the handler — clamping the version to the cap would read data in
/// another version's layout (v6: `content` is only the tail of a segmented
/// value). The connection is the opening of a real turn (`Start` + `Input`).
fn version_too_new() -> Vec<sim::Trace> {
    [Transport::Rr, Transport::Bidi]
        .into_iter()
        .map(|transport| {
            let mut sc = Scenario::new(
                &format!("version-too-new-{}-v4", tag(transport)),
                "counter",
                transport,
                4,
            );
            sc.description = "The runtime speaks wire v4, the SDK declares maxProtocolVersion = 3 \
                              (an old version after the deployment settled on v4). The SDK must \
                              return 415, no frames, without running the handler."
                .into();
            sc.source =
                vec!["sdk/typescript/test/serve.test.ts (version above the SDK cap)".into()];
            sc.spec = vec!["2"];
            sc.ops = vec![Op::Return("\"done\"")];
            sc.input = "\"req\"";
            let opened = sim::run(sc.clone());
            sc.ops = counter_ops();
            sc.sdk_max_wire = Some(3);
            let first = &opened.conns[0];
            sim::Trace {
                initial_len: opened.initial_len,
                conns: vec![sim::Conn {
                    frames: first
                        .frames
                        .iter()
                        .filter(|f| f.from == sim::From::Runtime)
                        .take(1 + opened.initial_len)
                        .cloned()
                        .collect(),
                    close: sim::Close::Sdk,
                    verify: false,
                    replay: None,
                    status: Some(415),
                }],
                journal: opened.journal[..opened.initial_len].to_vec(),
                outcome: sim::Final::Refused,
                scenario: sc,
            }
        })
        .collect()
}

/// Three v5 traces on the program of mandatory test 1: every turn after the
/// first, the runtime sends a delta per the hint; a hit, a miss because the
/// digest disagrees, a miss because the SDK lost its cache. Both miss traces
/// must end with **exactly** the journal of the hit trace.
fn v5_delta() -> Vec<sim::Trace> {
    let base = |name: &str| {
        let mut sc = counter(Transport::Rr, 5);
        sc.name = name.into();
        sc.replay_cache = true;
        sc.spec = vec!["9.3", "4", "8.3"];
        sc.cells = vec![1];
        sc.source = vec!["incremental replay of REQUEST_RESPONSE".into()];
        sc
    };
    let mut hit = base("delta-hit-rr-v5");
    hit.description = "Wire v5, the SDK keeps a cache: from the second turn on the runtime sends \
                       only entries [B, N) with Start.replay_from = B, Start.prefix_digest = h_B. \
                       The SDK splices the cache [0, B) with the rest, runs as a full replay, and \
                       returns cairn-replay-cache = N. The verify connection (after End) is a full \
                       replay."
        .into();
    let hit = sim::run(hit);
    let mut miss = base("delta-miss-rr-v5");
    miss.description = "Like delta-hit-rr-v5, but the third connection carries a prefix_digest \
                        that disagrees with the SDK's cache: the SDK must return 409 + \
                        cairn-replay: miss, an empty body, without running the handler; the \
                        runtime resends that very turn as a full replay."
        .into();
    miss.tamper_digest_at = Some(2);
    let miss = sim::run(miss);
    let mut restart = base("delta-after-restart-rr-v5");
    restart.description = "Like delta-hit-rr-v5, but the SDK restarts (losing its cache) before \
                           the third connection: the delta reaches an SDK with no entry ⇒ a 409 \
                           miss, the runtime resends it whole; the SDK keeps a cache from then on."
        .into();
    restart.sdk_restart_at = Some(2);
    let restart = sim::run(restart);
    for t in [&miss, &restart] {
        assert_eq!(
            t.journal, hit.journal,
            "{}: a miss changed the journal",
            t.scenario.name
        );
        assert!(
            t.conns
                .iter()
                .any(|c| c.replay.as_ref().is_some_and(|r| r.miss)),
            "{}: no missed connection",
            t.scenario.name
        );
    }
    assert!(
        hit.conns
            .iter()
            .filter(|c| c.replay.as_ref().is_some_and(|r| r.cache.is_some()))
            .count()
            >= 2,
        "delta-hit-rr-v5: too few turns keep the cache"
    );
    // Round 2: two replicas without affinity — the fourth connection lands on
    // the old replica (its cache stopped at the previous turn), the SDK misses
    // with the part it still holds, the runtime resends from `k`. On a real SDK
    // the runner brings the SDK into exactly that state by replaying an old turn
    // on a second endpoint (`stale_sdk`).
    let mut held = base("delta-miss-held-rr-v5");
    held.description = "Like delta-hit-rr-v5, with two replicas and no affinity: the fourth \
                        connection lands on the old replica (holding fewer entries than B). The \
                        SDK returns 409 + cairn-replay: miss + cairn-replay-held: k:h_k (the part \
                        it still holds, k = min(held, N)); the runtime checks h_k against the \
                        current journal and resends once from k."
        .into();
    held.stale_sdk_at = Some(3);
    held.resend_from_held = true;
    let held = sim::run(held);
    assert_eq!(
        held.journal, hit.journal,
        "delta-miss-held-rr-v5: a miss changed the journal"
    );
    assert!(
        held.conns
            .iter()
            .any(|c| c.replay.as_ref().is_some_and(|r| r.held.is_some())),
        "delta-miss-held-rr-v5: no miss carrying the held part"
    );
    vec![hit, miss, restart, held]
}

fn format_src(file: &str, test: &str) -> String {
    format!("{file}::{test}")
}

fn out_of_order_ops() -> Vec<Op> {
    vec![
        Op::Sleep(1_000),
        Op::Sleep(2_000),
        Op::Await(0),
        Op::Await(1),
        Op::Return("\"both\""),
    ]
}

fn sleep_run_sleep_ops() -> Vec<Op> {
    vec![
        Op::Sleep(1_000),
        Op::Sleep(2_000),
        Op::Run {
            name: "pay",
            json: "\"result\"",
            slow: false,
            tokens: &[],
        },
        Op::Await(0),
        Op::Await(1),
        Op::Return("\"done\""),
    ]
}

fn scoped_call_ops() -> Vec<Op> {
    vec![
        Op::ScopedCall {
            scope: "tenant-a",
            limit_key: "k1",
            service: "Callee",
            handler: "work",
            input: "1",
        },
        Op::Await(0),
        Op::Return("42"),
    ]
}
