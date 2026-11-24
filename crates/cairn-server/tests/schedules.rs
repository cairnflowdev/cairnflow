// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Server-side schedules: firing in the same range, firing in another range,
//! missed ticks.
//!
//! Schedules are set through an actor command (`PartitionHandle::schedule`, the
//! one the `/schedules` API calls); the test drives time with
//! `/admin/tick?now_ms=` — the background loop runs on real time, and the
//! test's ticks lie in the future so it does not interfere.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Result},
    bytes::Bytes,
    cairn_engine::schedule::{LastKind, Overlap, SpecRow},
    cairn_server::{
        node::RunningNode,
        schedules::{PutSchedule, ScheduleAnswer, ScheduleOp, ScheduleView},
    },
    cairn_store::{id_hex, InvocationId, PartitionId},
    common::fake_sdk::{FakeSdk, Step, HOLD_SIGNAL},
    std::time::Duration,
};

const MINUTE: u64 = 60_000;

fn put(name: &str, service: &str, handler: &str, key: &str) -> PutSchedule {
    PutSchedule {
        tenant: Default::default(),
        scope: None,
        name: name.into(),
        spec: SpecRow::Every(MINUTE),
        service: service.into(),
        handler: handler.into(),
        key: key.into(),
        limit_key: None,
        input: Bytes::from_static(b"{}"),
        overlap: if key.is_empty() {
            Overlap::Skip
        } else {
            Overlap::Allow
        },
        paused: false,
        jitter_ms: 0,
    }
}

/// Schedule command on the range holding the schedule's pk (one range: range 0).
async fn op(node: &RunningNode, op: ScheduleOp) -> Result<ScheduleAnswer> {
    op_on(node, 0, op).await
}

async fn op_on(node: &RunningNode, range: u32, op: ScheduleOp) -> Result<ScheduleAnswer> {
    let h = node
        .partitions
        .get(PartitionId(range))
        .ok_or_else(|| anyhow!("node does not hold range {range}"))?;
    Ok(h.schedule(op).await?)
}

/// The range currently holding partition key `pk`, per `/cluster`.
async fn range_of(url: &str, pk: u64) -> Result<u32> {
    let v: serde_json::Value = reqwest::get(format!("{url}/cluster")).await?.json().await?;
    let hex = format!("{pk:016x}");
    v["ranges"]
        .as_array()
        .and_then(|rs| {
            rs.iter().find(|r| {
                r["start"].as_str().is_some_and(|s| s <= hex.as_str())
                    && r["end"].as_str().is_some_and(|e| hex.as_str() <= e)
            })
        })
        .and_then(|r| r["range"].as_u64())
        .map(|r| r as u32)
        .ok_or_else(|| anyhow!("no range contains {hex}: {v}"))
}

async fn view(node: &RunningNode, id: InvocationId) -> Result<ScheduleView> {
    match op(node, ScheduleOp::Get(id)).await? {
        ScheduleAnswer::View(Some(v)) => Ok(v),
        other => bail!("GET schedule: {other:?}"),
    }
}

async fn tick(url: &str, now_ms: u64) -> Result<()> {
    let r = reqwest::Client::new()
        .post(format!("{url}/admin/tick?now_ms={now_ms}"))
        .send()
        .await?;
    ensure!(r.status().is_success(), "tick: {}", r.status());
    Ok(())
}

async fn entry0(url: &str, id: &str) -> Result<String> {
    let v: serde_json::Value = reqwest::get(format!("{url}/invocations/{id}/journal"))
        .await?
        .json()
        .await?;
    Ok(v[0].to_string())
}

/// **Firing in the same range** (CG-12): an `every 60 s` schedule with an
/// unkeyed target.
///
/// 1. `PUT` ⇒ `next_at` is a multiple of 60 s after the current time.
/// 2. A tick exactly at the mark ⇒ the run with id
///    `schedule_run_id(schedule, mark)` is created and completes; entry 0
///    carries `cairnflow-schedule: nightly@<mark>`.
/// 3. The schedule row moves on: `next_at = mark + 60 s`, `fired = 1`, `last`
///    has the run, status `completed` read in the same range.
/// 4. Another tick at the same mark ⇒ no second run (CG-10, same range).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_fire_creates_the_run_and_advances_in_one_commit() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "fire in same range", async {
        common::register(&url, &sdk.url).await?;
        let ScheduleAnswer::Put {
            view: v,
            created: true,
            ..
        } = op(
            &node,
            ScheduleOp::Put(Box::new(put("nightly", "Report", "build", ""))),
        )
        .await?
        else {
            bail!("the first PUT must create");
        };
        let at = v.row.next_at;
        ensure!(at % MINUTE == 0 && v.due == at, "{v:?}");
        tick(&url, at).await?;
        let run = id_hex(&cairn_store::schedule_run_id(&v.id, at));
        common::wait_status(&url, &run, "completed").await?;
        // The journal payload is hex.
        let e0 = entry0(&url, &run).await?;
        let hexed = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        ensure!(
            e0.contains(&hexed("cairnflow-schedule"))
                && e0.contains(&hexed(&format!("nightly@{at}"))),
            "entry 0 lacks the schedule header: {e0}"
        );
        let v = view(&node, v.id).await?;
        ensure!(
            v.row.next_at == at + MINUTE && v.due == at + MINUTE,
            "{v:?}"
        );
        ensure!(v.row.fired == 1 && v.row.fire_seq == 1, "{v:?}");
        ensure!(
            v.row.last.len() == 1
                && id_hex(&v.row.last[0].id) == run
                && v.row.last[0].kind == LastKind::Fired
                && v.last_status == vec![Some("completed")],
            "{v:?}"
        );
        let starts = sdk.starts_len();
        tick(&url, at).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        ensure!(
            sdk.starts_len() == starts,
            "another tick at the same mark created a second run"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`overlap: skip`** (CG-15): the target suspends waiting for a signal, so the
/// first run is still alive across the next two marks.
///
/// 1. Mark 1 ⇒ run 1, suspended.
/// 2. Mark 2, mark 3 ⇒ skipped (`skipped = 2`), no overlapping run.
/// 3. Signal ⇒ run 1 completes; mark 4 ⇒ run 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_never_overlaps() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Slow", "hold", vec![Step::Suspend, Step::output("x")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "overlap skip", async {
        common::register(&url, &sdk.url).await?;
        let ScheduleAnswer::Put { view: v, .. } = op(
            &node,
            ScheduleOp::Put(Box::new(put("slow", "Slow", "hold", ""))),
        )
        .await?
        else {
            bail!("PUT");
        };
        let at = v.row.next_at;
        tick(&url, at).await?;
        let first = id_hex(&cairn_store::schedule_run_id(&v.id, at));
        common::wait_status(&url, &first, "suspended").await?;
        for k in 1..=2 {
            tick(&url, at + k * MINUTE).await?;
        }
        let w = view(&node, v.id).await?;
        ensure!(w.row.skipped == 2 && w.row.fired == 1, "{w:?}");
        ensure!(
            w.row.last.iter().map(|l| l.kind).collect::<Vec<_>>()
                == vec![LastKind::Fired, LastKind::Skipped, LastKind::Skipped],
            "{w:?}"
        );
        ensure!(sdk.starts_len() == 1, "overlapping runs");
        let s = reqwest::Client::new()
            .post(format!("{url}/invocations/{first}/signal"))
            .json(&serde_json::json!({ "signal": HOLD_SIGNAL }))
            .send()
            .await?;
        ensure!(s.status() == 200, "signal {}", s.status());
        common::wait_status(&url, &first, "completed").await?;
        tick(&url, at + 3 * MINUTE).await?;
        let second = id_hex(&cairn_store::schedule_run_id(&v.id, at + 3 * MINUTE));
        common::wait_status(&url, &second, "suspended").await?;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Manual fire**: ignores `paused`, does not move `next_at`; resending the same
/// request id ⇒ the same run (`created = false`). `PUT` with the same definition
/// ⇒ no-op, does not move `next_at`; `DELETE` ⇒ no more firing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trigger_put_noop_and_delete() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "manual fire", async {
        common::register(&url, &sdk.url).await?;
        let mut def = put("adhoc", "Report", "build", "");
        def.paused = true;
        let ScheduleAnswer::Put { view: v, .. } =
            op(&node, ScheduleOp::Put(Box::new(def.clone()))).await?
        else {
            bail!("PUT");
        };
        ensure!(v.due == u64::MAX && v.row.paused, "{v:?}");
        let trig = |request: &str| ScheduleOp::Trigger {
            id: v.id,
            request: request.into(),
        };
        let ScheduleAnswer::Triggered(Some(Some((run, true)))) = op(&node, trig("r1")).await?
        else {
            bail!("the first trigger must create");
        };
        common::wait_status(&url, &id_hex(&run), "completed").await?;
        let ScheduleAnswer::Triggered(Some(Some((again, false)))) = op(&node, trig("r1")).await?
        else {
            bail!("a trigger with the same request id must give the same run");
        };
        ensure!(again == run);
        let w = view(&node, v.id).await?;
        ensure!(w.due == u64::MAX && w.row.fired == 1, "{w:?}");

        // Same definition ⇒ no-op.
        let ScheduleAnswer::Put {
            changed: false,
            view: same,
            ..
        } = op(&node, ScheduleOp::Put(Box::new(def))).await?
        else {
            bail!("a PUT with the same hash must be a no-op");
        };
        ensure!(same.row.fired == 1);
        let ScheduleAnswer::Deleted(true) = op(&node, ScheduleOp::Delete(v.id)).await? else {
            bail!("DELETE");
        };
        let ScheduleAnswer::View(None) = op(&node, ScheduleOp::Get(v.id)).await? else {
            bail!("schedule still there after DELETE");
        };
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Firing in another range** (CG-10): a schedule whose target is the virtual
/// object `Cart/sweep` key `u1` on a four-range node — the schedule and the run
/// are in two different ranges (the test picks schedule names until they
/// differ).
///
/// 1. A tick at the mark ⇒ a `Fire` row + the schedule row moved on (one
///    commit); the drainer sends a `StartCall` keyed `schedule/<name>/<mark>` to
///    the range of `Cart/u1`.
/// 2. The run has id `idempotent_invocation_id(…, that key)`, completes, entry 0
///    carries `cairnflow-schedule`.
/// 3. Two manual fires with the same request id ⇒ two `Fire` rows, **one** run
///    in the target range (deduplication).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_fire_crosses_ranges_once() -> Result<()> {
    use cairn_store::{partition_key_for_schedule, partition_key_for_target, TenantId};
    let sdk = FakeSdk::builder()
        .object("Cart", "sweep", vec![Step::output("swept")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(4)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "fire in another range", async {
        common::register(&url, &sdk.url).await?;
        let tenant = TenantId::default();
        let target_pk = partition_key_for_target(&tenant, None, "Cart", "u1", None)
            .ok_or_else(|| anyhow!("target pk"))?;
        let target_range = range_of(&url, target_pk.0).await?;
        let mut name = String::new();
        let mut home = target_range;
        for i in 0..64 {
            name = format!("sweep-{i}");
            home = range_of(&url, partition_key_for_schedule(&tenant, None, &name).0).await?;
            if home != target_range {
                break;
            }
        }
        ensure!(home != target_range, "no schedule name in another range");
        let ScheduleAnswer::Put { view: v, .. } = op_on(
            &node,
            home,
            ScheduleOp::Put(Box::new(put(&name, "Cart", "sweep", "u1"))),
        )
        .await?
        else {
            bail!("PUT");
        };
        let at = v.row.next_at;
        tick(&url, at).await?;
        let idem = cairn_engine::schedule::fire_idempotency_key(&name, at);
        let run_of = |idem: &str| {
            let key =
                partition_key_for_target(&tenant, None, "Cart", "u1", Some(idem)).expect("pk");
            id_hex(&cairn_store::idempotent_invocation_id(
                key, &tenant, None, "Cart", "sweep", "u1", idem,
            ))
        };
        let run = run_of(&idem);
        common::wait_status(&url, &run, "completed").await?;
        let e0 = entry0(&url, &run).await?;
        let hexed = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        ensure!(
            e0.contains(&hexed(&format!("{name}@{at}"))),
            "entry 0 lacks the schedule header: {e0}"
        );
        let w = match op_on(&node, home, ScheduleOp::Get(v.id)).await? {
            ScheduleAnswer::View(Some(w)) => w,
            other => bail!("{other:?}"),
        };
        ensure!(w.row.next_at == at + MINUTE && w.row.fired == 1, "{w:?}");
        ensure!(
            id_hex(&w.row.last[0].id) == run,
            "last points at the wrong run: {w:?}"
        );

        // Two manual fires with the same request id ⇒ one run.
        for _ in 0..2 {
            let a = op_on(
                &node,
                home,
                ScheduleOp::Trigger {
                    id: v.id,
                    request: "r1".into(),
                },
            )
            .await?;
            ensure!(
                matches!(a, ScheduleAnswer::Triggered(Some(Some(_)))),
                "{a:?}"
            );
        }
        let manual = run_of(&cairn_engine::schedule::manual_idempotency_key(&name, "r1"));
        common::wait_status(&url, &manual, "completed").await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        ensure!(
            sdk.starts_of("Cart", "sweep").len() == 2,
            "mark + manual fire must give exactly two runs: {}",
            sdk.starts_of("Cart", "sweep").len()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Missed marks**: the node "sleeps" through five marks (the first tick at
/// mark + 5 minutes) ⇒ fires **once** for the latest mark, the other five go
/// into `missed`; the next mark is computed from the current time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missed_marks_fire_once_for_the_latest() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "missed marks", async {
        common::register(&url, &sdk.url).await?;
        let ScheduleAnswer::Put { view: v, .. } = op(
            &node,
            ScheduleOp::Put(Box::new(put("late", "Report", "build", ""))),
        )
        .await?
        else {
            bail!("PUT");
        };
        let at = v.row.next_at;
        let late = at + 5 * MINUTE;
        tick(&url, late + 10).await?;
        let run = id_hex(&cairn_store::schedule_run_id(&v.id, late));
        common::wait_status(&url, &run, "completed").await?;
        let w = view(&node, v.id).await?;
        ensure!(
            w.row.missed == 5 && w.row.fired == 1 && w.row.next_at == late + MINUTE,
            "{w:?}"
        );
        ensure!(sdk.starts_len() == 1, "missed marks must fire exactly once");
        let missed =
            common::metric(&url, "cairnflow_schedule_fires_total{outcome=\"missed\"}").await?;
        ensure!(missed >= 5, "metric missed {missed}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`jitter_ms`**: the real fire time is `mark + H(id, mark) mod jitter`,
/// clamped below the next mark; the run's header carries the **nominal** mark.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jitter_delays_the_fire_but_not_the_mark() -> Result<()> {
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "jitter", async {
        common::register(&url, &sdk.url).await?;
        // Pick names until the jitter is non-zero (deterministic hash).
        let mut found = None;
        for i in 0..32 {
            let mut def = put(&format!("jit-{i}"), "Report", "build", "");
            def.jitter_ms = 30_000;
            let ScheduleAnswer::Put { view: v, .. } =
                op(&node, ScheduleOp::Put(Box::new(def))).await?
            else {
                bail!("PUT");
            };
            ensure!(
                v.due >= v.row.next_at && v.due < v.row.next_at + 30_000,
                "{v:?}"
            );
            if v.due > v.row.next_at {
                found = Some(v);
                break;
            }
        }
        let v = found.ok_or_else(|| anyhow!("no schedule has a non-zero jitter"))?;
        let at = v.row.next_at;
        tick(&url, v.due - 1).await?;
        let run = id_hex(&cairn_store::schedule_run_id(&v.id, at));
        tokio::time::sleep(Duration::from_millis(200)).await;
        ensure!(
            reqwest::get(format!("{url}/invocations/{run}"))
                .await?
                .status()
                == 404,
            "fired before the jittered time"
        );
        tick(&url, v.due).await?;
        common::wait_status(&url, &run, "completed").await?;
        let e0 = entry0(&url, &run).await?;
        let hexed = |s: &str| s.bytes().map(|b| format!("{b:02x}")).collect::<String>();
        ensure!(e0.contains(&hexed(&format!("{}@{at}", v.row.name))), "{e0}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// A 24-byte id from hex.
fn id24(hex: &str) -> Result<InvocationId> {
    ensure!(hex.len() == 48, "id {hex:?}");
    let mut out = [0u8; 24];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)?;
    }
    Ok(out)
}

/// `METHOD url` with an optional JSON body ⇒ `(code, JSON)`.
async fn call(
    method: reqwest::Method,
    url: String,
    body: Option<serde_json::Value>,
    headers: &[(&str, &str)],
) -> Result<(u16, serde_json::Value)> {
    let mut req = reqwest::Client::new().request(method, url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req.send().await?;
    let code = r.status().as_u16();
    let text = r.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// **The `/schedules` API**:
///
/// 1. `PUT` creates (`created`, `changed`), `PUT` again with the same definition
///    ⇒ no-op, `nextAt` does not move; a spec that never matches, `everyMs`
///    below the limit, a missing handler, a keyed target with `overlap: skip` ⇒
///    `400`/`404`.
/// 2. `GET` has the next 10 marks; after a tick at the mark,
///    `last[0].status = completed`.
/// 3. `trigger` with the same `Idempotency-Key` ⇒ the same id (`created` the
///    first time).
/// 4. `pause` ⇒ `nextAt: null`; `resume` ⇒ back again.
/// 5. `GET /schedules` lists; the `/scope/{scope}/…` variant with a key works.
/// 6. `DELETE` ⇒ `GET` `404`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_api_put_get_trigger_pause_delete() -> Result<()> {
    use reqwest::Method;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .object("Cart", "sweep", vec![Step::output("swept")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(4)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "schedule API", async {
        common::register(&url, &sdk.url).await?;
        let s = |p: &str| format!("{url}/schedules{p}");
        let def = serde_json::json!({
            "everyMs": 60_000,
            "target": { "service": "Report", "handler": "build" },
            "input": { "x": 1 }
        });
        let (code, v) = call(Method::PUT, s("/nightly"), Some(def.clone()), &[]).await?;
        ensure!(code == 200 && v["created"] == true && v["changed"] == true, "{code} {v}");
        let at = v["nextAt"].as_u64().ok_or_else(|| anyhow!("nextAt {v}"))?;
        ensure!(at % MINUTE == 0 && v["fireAt"] == at && v["overlap"] == "skip", "{v}");
        let (code, v2) = call(Method::PUT, s("/nightly"), Some(def), &[]).await?;
        ensure!(code == 200 && v2["created"] == false && v2["changed"] == false, "{v2}");
        ensure!(v2["nextAt"] == at, "PUT with the same definition moved nextAt: {v2}");
        for (bad, want) in [
            (serde_json::json!({"cron": "0 0 30 2 *", "target": {"service": "Report", "handler": "build"}}), 400),
            (serde_json::json!({"everyMs": 5, "target": {"service": "Report", "handler": "build"}}), 400),
            (serde_json::json!({"cron": "* * * * *", "everyMs": 60_000, "target": {"service": "Report", "handler": "build"}}), 400),
            (serde_json::json!({"everyMs": 60_000, "target": {"service": "Report", "handler": "nope"}}), 404),
            (serde_json::json!({"everyMs": 60_000, "overlap": "skip", "target": {"service": "Cart", "handler": "sweep", "key": "u1"}}), 400),
            (serde_json::json!({"cron": "* * * * *", "timeZone": "Mars/Olympus", "target": {"service": "Report", "handler": "build"}}), 400),
        ] {
            let (code, v) = call(Method::PUT, s("/bad"), Some(bad.clone()), &[]).await?;
            ensure!(code == want, "{bad} ⇒ {code} {v}");
        }
        let (code, _) = call(Method::PUT, s("/a%20b"), Some(serde_json::json!({"everyMs": 60_000, "target": {"service": "Report", "handler": "build"}})), &[]).await?;
        ensure!(code == 400, "name with a space: {code}");

        let (code, g) = call(Method::GET, s("/nightly"), None, &[]).await?;
        ensure!(code == 200 && g["upcoming"].as_array().map(Vec::len) == Some(10), "{g}");
        tick(&url, at).await?;
        let run = id_hex(&cairn_store::schedule_run_id(&id24(g["id"].as_str().unwrap_or_default())?, at));
        common::wait_status(&url, &run, "completed").await?;
        let (_, g) = call(Method::GET, s("/nightly"), None, &[]).await?;
        ensure!(
            g["last"][0]["id"] == run.as_str()
                && g["last"][0]["outcome"] == "fired"
                && g["last"][0]["status"] == "completed"
                && g["fired"] == 1
                && g["nextAt"] == at + MINUTE,
            "{g}"
        );

        let (code, t1) = call(Method::POST, s("/nightly/trigger"), None, &[("idempotency-key", "k1")]).await?;
        ensure!(code == 200 && t1["created"] == true, "{code} {t1}");
        let (_, t2) = call(Method::POST, s("/nightly/trigger"), None, &[("idempotency-key", "k1")]).await?;
        ensure!(t2["id"] == t1["id"] && t2["created"] == false, "{t2}");

        let (code, p) = call(Method::POST, s("/nightly/pause"), None, &[]).await?;
        ensure!(code == 200 && p["paused"] == true && p["nextAt"].is_null(), "{p}");
        let (code, p) = call(Method::POST, s("/nightly/resume"), None, &[]).await?;
        ensure!(code == 200 && p["paused"] == false && p["nextAt"].is_u64(), "{p}");

        let (code, sc) = call(
            Method::PUT,
            format!("{url}/scope/acme/schedules/sweep"),
            Some(serde_json::json!({"everyMs": 60_000, "target": {"service": "Cart", "handler": "sweep", "key": "u1"}})),
            &[],
        )
        .await?;
        ensure!(code == 200 && sc["scope"] == "acme" && sc["overlap"] == "allow", "{code} {sc}");
        let (code, l) = call(Method::GET, s(""), None, &[]).await?;
        let names: Vec<&str> = l["schedules"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x["name"].as_str()).collect())
            .unwrap_or_default();
        ensure!(code == 200 && names.contains(&"nightly") && names.contains(&"sweep"), "{l}");
        ensure!(l["partial"] == false, "{l}");

        let (code, d) = call(Method::DELETE, s("/nightly"), None, &[]).await?;
        ensure!(code == 200 && d["deleted"] == true, "{code} {d}");
        let (code, _) = call(Method::GET, s("/nightly"), None, &[]).await?;
        ensure!(code == 404, "GET after DELETE: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The `PUT /schedules` body has limits on `jitterMs` and `target.key`.**
///
/// - `jitterMs = u64::MAX`: accepted before the fix; `jittered` casts
///   `u64 → i64` into a negative number ⇒ fire time 0 ⇒ the schedule fires on
///   every background tick, bypassing `--schedule-min-period-ms`.
/// - `jitterMs` above 24 h: no schedule needs it, accepting it opens the door to
///   huge numbers.
/// - `target.key` of 70 000 bytes: accepted before the fix; the fire hits
///   `KeyTooLong` in the target range's `admit`.
///
/// Must see: all three `400`; `jitterMs` of exactly 24 h and a 4 096-byte key
/// are still accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_body_bounds_jitter_and_target_key() -> Result<()> {
    use reqwest::Method;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .object("Cart", "sweep", vec![Step::output("swept")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "schedule body limits", async {
        common::register(&url, &sdk.url).await?;
        let s = |p: &str| format!("{url}/schedules{p}");
        let every = |jitter: serde_json::Value| {
            serde_json::json!({
                "everyMs": 60_000,
                "jitterMs": jitter,
                "target": { "service": "Report", "handler": "build" }
            })
        };
        let keyed = |len: usize| {
            serde_json::json!({
                "everyMs": 60_000,
                "target": { "service": "Cart", "handler": "sweep", "key": "k".repeat(len) }
            })
        };
        for (what, body, want) in [
            ("jitter u64::MAX", every(serde_json::json!(u64::MAX)), 400),
            (
                "jitter 24 h + 1",
                every(serde_json::json!(86_400_001u64)),
                400,
            ),
            ("jitter 24 h", every(serde_json::json!(86_400_000u64)), 200),
            ("70 000-byte key", keyed(70_000), 400),
            ("4 096-byte key", keyed(4_096), 200),
        ] {
            let (code, v) = call(Method::PUT, s("/bounds"), Some(body), &[]).await?;
            ensure!(code == want, "{what}: {code} {v}");
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The schedule list is not silently cut at the range's 1 000 schedules (all
/// tenants).**
///
/// 1. One range. Tenant `bravo` has 1 001 schedules; tenant `default` one
///    `paused` schedule (`due = u64::MAX` ⇒ the end of the `(due, id)` index).
/// 2. `GET /schedules` of `default`.
///
/// Before the fix: the range returned the first 1 000 rows **before** filtering
/// by tenant ⇒ `default`'s schedule vanished, with `partial: false`.
///
/// Must see: the `paused` schedule is in the list, or `partial: true`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_list_is_not_silently_cut_by_other_tenants() -> Result<()> {
    use reqwest::Method;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(120), "schedule list", async {
        common::register(&url, &sdk.url).await?;
        let (code, v) = call(
            Method::POST,
            format!("{url}/deployments?tenant=bravo"),
            Some(serde_json::json!({ "uri": sdk.url })),
            &[],
        )
        .await?;
        ensure!(code < 300, "deployment bravo: {code} {v}");
        for i in 0..1_001 {
            let mut def = put(&format!("b-{i:04}"), "Report", "build", "");
            def.tenant = cairn_store::TenantId::new("bravo")?;
            op(&node, ScheduleOp::Put(Box::new(def))).await?;
        }
        let mut mine = put("mine", "Report", "build", "");
        mine.paused = true;
        op(&node, ScheduleOp::Put(Box::new(mine))).await?;
        let (code, l) = call(Method::GET, format!("{url}/schedules"), None, &[]).await?;
        ensure!(code == 200, "{code} {l}");
        let names: Vec<&str> = l["schedules"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x["name"].as_str()).collect())
            .unwrap_or_default();
        ensure!(
            names.contains(&"mine") || l["partial"] == true,
            "default's schedule is missing from the list while partial = {}: {} schedules",
            l["partial"],
            names.len()
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The `Fire` path — a `Fire` row that can never be created does not block the
/// range's outbox drainer.**
///
/// 1. One range. Schedule `bomb` has a keyed target with a 70 000-byte key —
///    written directly through an engine command (a schedule from before the
///    `PUT` limit). A tick at the mark ⇒ an outbox `Fire` row (a keyed target
///    always goes through the outbox); its `admit` hits `KeyTooLong`.
/// 2. Then `Caller/run` calls `Echo/ping` — the outbox `Call` row sits behind
///    the failed `Fire` row.
///
/// Before the fix the `admit` error propagated (`?`) and dropped the whole drain
/// pass; the next pass met the same row again ⇒ no row after it ever went out.
/// Must see: `Caller/run` completes within 10 s; the failed `Fire` row does not
/// stay forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfireable_fire_row_does_not_stall_the_outbox() -> Result<()> {
    let sdk = FakeSdk::builder()
        .object("Cart", "sweep", vec![Step::output("swept")])
        .object(
            "Caller",
            "run",
            vec![Step::call("Echo", "ping", Some("e1")), Step::output("done")],
        )
        .object("Echo", "ping", vec![Step::output("pong")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "failed Fire", async {
        common::register(&url, &sdk.url).await?;
        let ScheduleAnswer::Put { view: v, .. } = op(
            &node,
            ScheduleOp::Put(Box::new(put("bomb", "Cart", "sweep", &"k".repeat(70_000)))),
        )
        .await?
        else {
            bail!("PUT schedule");
        };
        tick(&url, v.row.next_at).await?;
        let (code, body) = common::invoke(&url, "Caller", "run", Some("c1"), "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        let id = body["id"].as_str().unwrap_or_default().to_string();
        tokio::time::timeout(
            Duration::from_secs(10),
            common::wait_status(&url, &id, "completed"),
        )
        .await
        .map_err(|_| anyhow!("Caller/run not done — outbox stuck behind the failed Fire row"))??;
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Like [`schedule_list_is_not_silently_cut_by_other_tenants`], but stricter:
/// `default`'s list **has** its schedule and is **not** `partial` — the range
/// lists in chunks with a cursor and filters by tenant below the engine, so
/// other tenants' schedules do not use up the asking tenant's limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_list_pages_past_other_tenants() -> Result<()> {
    use reqwest::Method;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(120), "chunked list", async {
        common::register(&url, &sdk.url).await?;
        let (code, v) = call(
            Method::POST,
            format!("{url}/deployments?tenant=bravo"),
            Some(serde_json::json!({ "uri": sdk.url })),
            &[],
        )
        .await?;
        ensure!(code < 300, "deployment bravo: {code} {v}");
        for i in 0..1_001 {
            let mut def = put(&format!("b-{i:04}"), "Report", "build", "");
            def.tenant = cairn_store::TenantId::new("bravo")?;
            op(&node, ScheduleOp::Put(Box::new(def))).await?;
        }
        let mut mine = put("mine", "Report", "build", "");
        mine.paused = true;
        op(&node, ScheduleOp::Put(Box::new(mine))).await?;
        let (code, l) = call(Method::GET, format!("{url}/schedules"), None, &[]).await?;
        ensure!(code == 200, "{code} {l}");
        let names: Vec<&str> = l["schedules"]
            .as_array()
            .map(|a| a.iter().filter_map(|x| x["name"].as_str()).collect())
            .unwrap_or_default();
        ensure!(
            names == ["mine"] && l["partial"] == false,
            "default's list: {names:?}, partial = {}",
            l["partial"]
        );
        let (code, l) = call(
            Method::GET,
            format!("{url}/schedules?tenant=bravo"),
            None,
            &[],
        )
        .await?;
        ensure!(code == 200, "{code}");
        let n = l["schedules"].as_array().map(Vec::len).unwrap_or(0);
        ensure!(
            n == 1_000 && l["partial"] == true,
            "bravo: {n} schedules, partial = {} (limit 1 000 of the asking tenant)",
            l["partial"]
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// Build a one-range node with `n` schedules of tenant `bravo` and one paused
/// schedule `mine` (due `u64::MAX` — the end of the `ScheduleDue` index) of
/// `default`.
async fn node_with_bravo_schedules(
    n: usize,
    cfg: cairn_server::node::NodeConfig,
) -> Result<(RunningNode, FakeSdk)> {
    use reqwest::Method;
    let sdk = FakeSdk::builder()
        .service("Report", "build", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(cfg).await?;
    let url = node.url();
    common::register(&url, &sdk.url).await?;
    let (code, v) = call(
        Method::POST,
        format!("{url}/deployments?tenant=bravo"),
        Some(serde_json::json!({ "uri": sdk.url })),
        &[],
    )
    .await?;
    ensure!(code < 300, "deployment bravo: {code} {v}");
    for i in 0..n {
        let mut def = put(&format!("b-{i:05}"), "Report", "build", "");
        def.tenant = cairn_store::TenantId::new("bravo")?;
        op(&node, ScheduleOp::Put(Box::new(def))).await?;
    }
    let mut mine = put("mine", "Report", "build", "");
    mine.paused = true;
    op(&node, ScheduleOp::Put(Box::new(mine))).await?;
    Ok((node, sdk))
}

/// **`GET /schedules` stops at a per-request chunk limit.**
///
/// With chunked listing, the range lists in chunks of 256 `ScheduleDue` index
/// rows (sorted by due time, not by tenant) up to the **end** of the index when
/// the asking tenant has few schedules: a `view` key of a tenant with no
/// schedules would scan the whole schedule index of every tenant (plus one point
/// read per row) on every request.
///
/// Order: `bravo` has 2 100 schedules (> 8 chunks × 256), `default` has one
/// paused schedule at the end of the index. `default` asks `GET /schedules`.
///
/// Must see: the range stops after the per-request chunk limit and reports
/// `partial` + `truncatedRanges`, without scanning up to the last schedule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_list_stops_at_the_page_cap() -> Result<()> {
    let (node, _sdk) = node_with_bravo_schedules(2_100, common::solo_config(1)).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(180), "chunk limit", async {
        let (code, l) = call(reqwest::Method::GET, format!("{url}/schedules"), None, &[]).await?;
        ensure!(code == 200, "{code} {l}");
        ensure!(
            l["partial"] == true
                && l["truncatedRanges"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty()),
            "range scanned past the chunk limit: partial = {}, truncatedRanges = {}",
            l["partial"],
            l["truncatedRanges"]
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **`GET /schedules` charges the tenant's read bucket.** Each chunk is up to 256
/// index rows plus 256 point reads under the range's engine; this path used to
/// charge nothing (only `/archive` and `/audit` charged), so the only limit was
/// the key's rate limit.
///
/// Must see: a small bucket (1 500 units, refilling 1/s), 600 schedules of
/// `bravo` (3 chunks) ⇒ the first request `200`, the next `429`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_list_spends_the_read_budget() -> Result<()> {
    let mut cfg = common::solo_config(1);
    cfg.query_tenant_budget = cairn_server::query::http::TenantBudget {
        capacity: 1_500,
        per_second: 1,
    };
    let (node, _sdk) = node_with_bravo_schedules(600, cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(120), "read bucket", async {
        let (code, l) = call(reqwest::Method::GET, format!("{url}/schedules"), None, &[]).await?;
        ensure!(code == 200, "first: {code} {l}");
        let (code, l) = call(reqwest::Method::GET, format!("{url}/schedules"), None, &[]).await?;
        ensure!(code == 429, "drained, still lists: {code} {l}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}
