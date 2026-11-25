// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Incremental replay of `REQUEST_RESPONSE` (wire v5,
//! `docs/protocol-v1.md` §9.3) on a real node.
//!
//! A hand-written h1 fake SDK (`common/rr_sdk.rs`, modelled on `rr_compression.rs`).
//!
//! Every test checks the same thing first: the invocation ends **exactly as a
//! run without the cache would** (output, number of commands the SDK emits),
//! whether the delta path hits or misses.
//!
//! Must see:
//! - SDK keeps a cache ⇒ from the second turn on the runtime sends the previous
//!   turn's `replay_from = N`, and the request body is much smaller than a full
//!   replay; `--no-rr-delta-replay` ⇒ every turn is full.
//! - SDK does not declare `replayCache`, or the SDK only speaks v4 ⇒ no delta
//!   turn, no v5 header, content-type ≤ v4.
//! - SDK loses its cache every turn ⇒ each delta turn gets `409` and the runtime
//!   resends in full **at once** (not counted as an attempt); after
//!   `WINDOW_TURNS` turns with more than half missing, the deployment turns delta
//!   off by itself — no further `409`.
//! - Cache corrupted by one byte ⇒ exactly one turn misses, then it hits again.
//! - Two replicas without affinity ⇒ a miss carries the part still held
//!   (`cairn-replay-held`), the runtime resends from `k` — no auto-off, bytes
//!   ≤ 0.2 × full; `k` absent or wrong ⇒ full; a miss on the send-from-`k` turn
//!   too ⇒ full; auto-off by **bytes** (delta ≥ 0.95 × full within one window).
//!   Sticky by cookie ⇒ hits like a single replica, and this invocation's cookie
//!   never reaches another invocation's turn.
//! - `cairn-affinity: fly=<id>` ⇒ the next turn carries `fly-force-instance-id`.
//! - The hint is lost when the node reopens or the range splits (new term) ⇒ the
//!   next turn is full, then it hits again; it survives a suspension waiting on a
//!   timer when nothing changed.
//! - `restart` from an entry ⇒ new id, its first turn is full.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_server::node::{NodeConfig, NodeMode},
    common::{
        invocation, register,
        rr_sdk::{expect_ok, run_one, CacheMode, Held, Lb, Sdk, Spec, PAYLOAD},
        send, within, TEST_DEADLINE,
    },
    std::time::Duration,
};

async fn node_with(f: impl FnOnce(&mut NodeConfig)) -> Result<cairn_server::node::RunningNode> {
    let mut cfg = common::test_config(NodeMode::single(1));
    f(&mut cfg);
    common::start_led(cfg).await
}

/// The runtime's auto-off window (`cairn_server::turn::replay::WINDOW_TURNS`).
const WINDOW_TURNS: usize = 20;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delta_hits_after_the_first_turn() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn(Spec::new("Hit")).await?;
        let dep = register(&url, &sdk.url).await?;
        ensure!(dep["wireVersion"] == 5, "{dep}");
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let turns = sdk.turns_of(&raw);
        ensure!(turns.len() == 7, "turns: {turns:?}");
        ensure!(turns.iter().all(|t| !t.miss), "misses: {turns:?}");
        ensure!(turns[0].replay_from == 0);
        for w in turns.windows(2) {
            ensure!(
                w[1].replay_from == w[0].known,
                "the next turn must skip exactly the previous turn's N: {w:?}"
            );
            ensure!(w[1].content_type.ends_with(".v5"), "{w:?}");
        }
        // Delta body ≈ one step; the last turn's full body ≈ six steps.
        let last = turns.last().unwrap();
        ensure!(
            last.wire_bytes < 2 * PAYLOAD + 1024,
            "last turn's delta body {} bytes",
            last.wire_bytes
        );
        ensure!(common::metric(&url, "cairnflow_rr_replay_total{result=\"hit\"}").await? >= 6);
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_switch_and_undeclared_sdks_replay_full() -> Result<()> {
    // `--no-rr-delta-replay`.
    let node = node_with(|c| c.limits.rr_delta_replay = false).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn(Spec::new("Off")).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let turns = sdk.turns_of(&raw);
        ensure!(
            turns.len() == 7 && turns.iter().all(|t| t.replay_from == 0),
            "{turns:?}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r?;
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        // A v5 SDK that does not declare `replayCache`; an SDK that only speaks v4 though it declares it.
        for (name, max_wire, declare) in [("NoCache", 5, false), ("V4", 4, true)] {
            let mut spec = Spec::new(name);
            spec.max_wire = max_wire;
            spec.declare = declare;
            let sdk = Sdk::spawn(spec).await?;
            register(&url, &sdk.url).await?;
            let (_, raw, st) = run_one(&url, &sdk).await?;
            expect_ok(&st, 6)?;
            let turns = sdk.turns_of(&raw);
            ensure!(
                turns.len() == 7 && turns.iter().all(|t| t.replay_from == 0 && !t.miss),
                "{name}: {turns:?}"
            );
            if max_wire == 4 {
                ensure!(
                    turns.iter().all(|t| t.content_type.ends_with(".v4")),
                    "{turns:?}"
                );
            }
        }
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// Order: (1) turn k returns `cairn-replay-cache: N` but the SDK keeps nothing;
/// (2) on turn k+1 the runtime sends a delta; (3) the SDK answers `409`; (4) the
/// runtime resends in full **at once**, the SDK runs it. Repeat until a window of
/// `WINDOW_TURNS` delta turns has more than half missing ⇒ the deployment turns
/// delta off. Must see: exactly `WINDOW_TURNS` `409`s, each followed at once by
/// a full turn of the same invocation; the invocation completes correctly; no
/// turn is counted as a failed attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_cache_misses_resend_full_and_auto_off() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("Forget");
        spec.mode = CacheMode::Forget;
        spec.runs = 30;
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 30)?;
        let turns = sdk.turns_of(&raw);
        let misses = turns.iter().filter(|t| t.miss).count();
        ensure!(misses == WINDOW_TURNS, "misses {misses}: {turns:?}");
        for w in turns.windows(2) {
            if w[0].miss {
                ensure!(
                    !w[1].miss && w[1].replay_from == 0,
                    "after a miss it must be full: {w:?}"
                );
                ensure!(w[1].known == w[0].known, "resend that same turn: {w:?}");
                ensure!(w[1].retry == 0, "a resend is not an attempt: {w:?}");
            }
        }
        ensure!(turns.iter().filter(|t| !t.miss).count() == 31, "{turns:?}");
        ensure!(
            st["retryCount"].as_u64().unwrap_or(0) == 0,
            "a miss was counted as an attempt: {st}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_cache_byte_misses_once() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("Corrupt");
        spec.mode = CacheMode::CorruptBefore(4);
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let turns = sdk.turns_of(&raw);
        ensure!(turns.iter().filter(|t| t.miss).count() == 1, "{turns:?}");
        let after: Vec<_> = turns.iter().skip_while(|t| !t.miss).skip(2).collect();
        ensure!(
            !after.is_empty() && after.iter().all(|t| t.replay_from > 0 && !t.miss),
            "hits again after one full turn: {turns:?}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// Order (byte-based miss rule): (1) two replicas behind a random load balancer,
/// each keeping the journal up to the last turn **it** served; (2) a delta lands
/// on the stale replica ⇒ `409` with `cairn-replay-held: k:h_k` (the part it
/// still holds); (3) the runtime checks `h_k` against the current journal and
/// resends **from k** once. Must see: no auto-off (the last turn is still a
/// delta), every resend after a miss with a valid `k` starts at exactly `k`, and
/// the total bytes sent to the SDK ≤ 0.2 × a full replay (measured with the same
/// scenario on a `--no-rr-delta-replay` node).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_replicas_resend_from_held_prefix_sticky_cookie_hits() -> Result<()> {
    let off = node_with(|c| c.limits.rr_delta_replay = false).await?;
    let full = async {
        let mut spec = Spec::new("RandomFull");
        spec.lb = Lb::Random;
        spec.runs = 50;
        let sdk = Sdk::spawn(spec).await?;
        register(&off.url(), &sdk.url).await?;
        let (_, raw, st) = run_one(&off.url(), &sdk).await?;
        expect_ok(&st, 50)?;
        Ok::<usize, anyhow::Error>(sdk.turns_of(&raw).iter().map(|t| t.wire_bytes).sum())
    }
    .await;
    off.shutdown().await;
    let full = full?;
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("Random");
        spec.lb = Lb::Random;
        spec.runs = 50;
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 50)?;
        let turns = sdk.turns_of(&raw);
        ensure!(
            turns.iter().any(|t| t.miss),
            "a random balancer must produce misses: {turns:?}"
        );
        for w in turns.windows(2) {
            if w[0].miss && w[0].held > 0 && w[0].replay_from != w[0].held {
                ensure!(w[1].replay_from == w[0].held, "resend from k: {w:?}");
                ensure!(w[1].retry == 0, "not counted as an attempt: {w:?}");
            }
        }
        let tail = &turns[turns.len() - 10..];
        ensure!(
            tail.iter().any(|t| t.replay_from > 0 && !t.miss),
            "must not turn itself off: {tail:?}"
        );
        let sent: usize = turns.iter().map(|t| t.wire_bytes).sum();
        ensure!(
            sent * 5 <= full,
            "bytes to SDK {sent} > 0.2 × full {full}: {turns:?}"
        );

        let mut spec = Spec::new("Sticky");
        spec.lb = Lb::StickyCookie;
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, a, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let (_, b, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        for raw in [&a, &b] {
            let turns = sdk.turns_of(raw);
            ensure!(
                turns.iter().all(|t| !t.miss),
                "sticky cookie ⇒ hits: {turns:?}"
            );
            ensure!(
                turns[0].cookie.is_none(),
                "the first turn carries nobody's cookie"
            );
            let want = format!("lb=r{}; other=1", turns[0].replica);
            for t in &turns[1..] {
                ensure!(t.cookie.as_deref() == Some(want.as_str()), "{t:?} ≠ {want}");
                ensure!(t.replica == turns[0].replica && t.replay_from > 0, "{t:?}");
            }
        }
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fly_affinity_header_is_sent_back() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("Fly");
        spec.fly = true;
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let turns = sdk.turns_of(&raw);
        ensure!(turns[0].fly.is_none());
        ensure!(
            turns[1..].iter().all(|t| t.fly.as_deref() == Some("m0abc")),
            "{turns:?}"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// Order: (1) two turns hit; (2) the third turn schedules `ctx.sleep(1500)` and
/// suspends; (3) the test steps into this gap — (a) does nothing, (b) stops the
/// node and reopens it on the same store, (c) splits range 0; (4) the timer
/// fires, the next turn runs. Must see: (a) the turn after the timer is still a
/// delta (the hint survives the suspension); (b), (c) the turn after the timer is
/// full (new term, new hint book) and the next one hits again; correct output in
/// every case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hints_survive_waiting_and_reset_on_new_term() -> Result<()> {
    for case in ["none", "restart", "split"] {
        let stores = common::memory_stores();
        let mk = || {
            let mut cfg = NodeConfig::new(stores.clone(), NodeMode::single(1));
            cfg.tick_interval = Some(Duration::from_millis(50));
            cfg.flush_interval = Some(Duration::from_millis(5));
            cfg
        };
        let mut node = common::start_led(mk()).await?;
        let mut spec = Spec::new("Nap");
        spec.sleep_ms = 1500;
        spec.runs = 4;
        let sdk = Sdk::spawn(spec).await?;
        register(&node.url(), &sdk.url).await?;
        let (code, v) = send(&node.url(), "Nap", "run", None, "{}").await?;
        ensure!(code == 200 || code == 202, "{v}");
        let id = v["id"].as_str().unwrap_or_default().to_string();
        // Wait for the timer-scheduling turn (the third turn).
        within(TEST_DEADLINE, "timer-scheduling turn", async {
            while sdk.log().len() < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        })
        .await?;
        match case {
            "restart" => {
                node.shutdown().await;
                node = common::start_led(mk()).await?;
            }
            "split" => {
                let resp = reqwest::Client::new()
                    .post(format!("{}/ranges/0/split", node.url()))
                    .send()
                    .await?;
                ensure!(resp.status() == 200, "split: {}", resp.text().await?);
            }
            _ => {}
        }
        let url = node.url();
        let st = within(TEST_DEADLINE, "invocation done", async {
            loop {
                let (_, st) = invocation(&url, &id).await?;
                if st["status"] == "completed" {
                    return Ok(st);
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await?;
        node.shutdown().await;
        expect_ok(&st, 4)?;
        let turns = sdk.log();
        ensure!(turns.iter().all(|t| !t.miss), "{case}: {turns:?}");
        let after = &turns[3];
        if case == "none" {
            ensure!(
                after.replay_from > 0,
                "{case}: the hint must survive the suspension: {turns:?}"
            );
        } else {
            ensure!(
                after.replay_from == 0,
                "{case}: a new term must be full: {turns:?}"
            );
            ensure!(turns[4].replay_from > 0, "{case}: hits again: {turns:?}");
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_from_entry_starts_full() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let sdk = Sdk::spawn(Spec::new("Again")).await?;
        register(&url, &sdk.url).await?;
        let (id, _, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 6)?;
        let resp = reqwest::Client::new()
            .post(format!("{url}/invocations/{id}/restart?from=5"))
            .send()
            .await?;
        ensure!(resp.status() == 200, "restart: {}", resp.status());
        let v: serde_json::Value = resp.json().await?;
        let new = v["new"].as_str().ok_or_else(|| anyhow!("{v}"))?.to_string();
        common::wait_status(&url, &new, "completed").await?;
        let raw: Vec<u8> = (0..new.len() / 2)
            .map(|i| u8::from_str_radix(&new[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        let turns = sdk.turns_of(&raw);
        ensure!(!turns.is_empty() && turns[0].replay_from == 0, "{turns:?}");
        ensure!(turns.iter().all(|t| !t.miss), "{turns:?}");
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// Order: (1) the SDK keeps the right journal but answers `409` to **every** delta
/// turn, reporting `k − 1` with the real `h_{k−1}`; (2) the runtime resends from
/// `k − 1`; (3) the SDK misses again; (4) the runtime sends in full. Must see:
/// exactly the chain miss(B) → miss(k−1) → full on every delta turn; after one
/// window, delta bytes ≥ 0.95 × full ⇒ auto-off (no further `409`); the
/// invocation completes as a run without the cache would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn miss_at_resend_from_k_falls_back_to_full_then_byte_auto_off() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        let mut spec = Spec::new("Shorter");
        spec.mode = CacheMode::MissAlways;
        spec.held = Held::Shorter;
        spec.runs = 30;
        let sdk = Sdk::spawn(spec).await?;
        register(&url, &sdk.url).await?;
        let (_, raw, st) = run_one(&url, &sdk).await?;
        expect_ok(&st, 30)?;
        let turns = sdk.turns_of(&raw);
        let mut deltas = 0;
        let mut i = 0;
        while i < turns.len() {
            let t = &turns[i];
            if t.replay_from > 0 {
                deltas += 1;
                ensure!(t.miss, "MissAlways: {t:?}");
                let k = &turns[i + 1];
                if t.held > 0 {
                    ensure!(
                        k.replay_from == t.held && k.miss,
                        "resend from k, then a miss: {t:?} {k:?}"
                    );
                    let f = &turns[i + 2];
                    ensure!(
                        f.replay_from == 0 && !f.miss && f.known == t.known,
                        "then full: {f:?}"
                    );
                    i += 3;
                    continue;
                }
                ensure!(k.replay_from == 0 && !k.miss, "{k:?}");
                i += 2;
                continue;
            }
            i += 1;
        }
        ensure!(
            deltas == WINDOW_TURNS,
            "byte-based auto-off after one window: {deltas} delta turns"
        );
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}

/// `409` without the held part (a v5 SDK from before held-part reporting), or
/// with a wrong `k` — digest mismatch, `k > known_entries`, `k = 0` ⇒ the
/// runtime sends in full at once, never from `k`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_or_absent_held_prefix_resends_full() -> Result<()> {
    let node = node_with(|_| {}).await?;
    let url = node.url();
    let r = async {
        for (name, held) in [
            ("HeldNone", Held::None),
            ("HeldBad", Held::BadDigest),
            ("HeldBig", Held::TooBig),
            ("HeldZero", Held::Zero),
        ] {
            let mut spec = Spec::new(name);
            spec.mode = CacheMode::MissAlways;
            spec.held = held;
            let sdk = Sdk::spawn(spec).await?;
            register(&url, &sdk.url).await?;
            let (_, raw, st) = run_one(&url, &sdk).await?;
            expect_ok(&st, 6)?;
            let turns = sdk.turns_of(&raw);
            ensure!(turns.iter().any(|t| t.miss), "{name}: {turns:?}");
            for w in turns.windows(2) {
                if w[0].miss {
                    ensure!(
                        w[1].replay_from == 0 && !w[1].miss && w[1].known == w[0].known,
                        "{name}: after a miss it must be full: {w:?}"
                    );
                }
            }
        }
        Ok(())
    }
    .await;
    node.shutdown().await;
    r
}
