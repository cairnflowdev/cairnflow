// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! GC barriers: the D-d round (deleting the Db of retired ranges) runs on **one**
//! node, deletes only Dbs old enough, honours barriers, can dry-run, and checks again
//! right before deleting.
//!
//! The window between the check and the delete needs a failpoint, so its test lives
//! in `tests/failpoints.rs`.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    cairn_store::PartitionId,
    common::{
        cluster_nodes, fabricate_retired_range, single_leaders, wait_probe, wall_ms,
        LONG_RETIRED_MS,
    },
    std::time::Duration,
};

async fn post_json(url: String) -> Result<(u16, serde_json::Value)> {
    let resp = reqwest::Client::new().post(url).send().await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

/// A one-node cluster, one range. Always in cluster mode — tombstones and owner records
/// only exist there.
fn solo_cluster() -> cairn_server::node::NodeConfig {
    let mut cfg = common::test_config(cairn_server::node::NodeMode {
        partitions: 1,
        lease_ttl: Duration::from_secs(2),
        node_id: Some("node-solo".into()),
    });
    cfg.node_wal.window = Duration::from_millis(5);
    cfg
}

/// The raw content of `owners/range-<r>` (empty prefix).
async fn raw_owner(stores: &cairn_store::ObjectStores, r: u32) -> Result<String> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let got = stores
        .main()
        .store()
        .get(&Path::from(format!("owners/range-{r}")))
        .await?;
    Ok(String::from_utf8(got.bytes().await?.to_vec())?)
}

// ─────────────────────────────────────────────── one runner

/// **One runner.** The D-d round runs only on the leader of the live range with the
/// smallest `RangeId`; every other node skips its turn. Before: every node ran it (safe
/// because deletes are idempotent, but N LIST/GETs per round and the logs of N nodes
/// deleting the same thing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_runs_only_on_leader_of_smallest_live_range() -> Result<()> {
    let (_bucket, nodes) = cluster_nodes(&["gc-a", "gc-b"], 2, Duration::from_secs(1)).await?;
    let refs: Vec<_> = nodes.iter().collect();
    let r = common::within(Duration::from_secs(40), "one D-d runner", async {
        // The balancer may move a leader mid-way: only judge when the two probes
        // before and after the question agree.
        loop {
            let probe = wait_probe(&refs, Duration::from_secs(20), "one leader per range", |p| {
                single_leaders(p).is_some()
            })
            .await?;
            let leaders = single_leaders(&probe).ok_or_else(|| anyhow!("{probe:?}"))?;
            let mut runs = Vec::new();
            for n in &nodes {
                runs.push(cairn_server::gc::is_runner(&n.node.cluster, &n.node.partitions).await);
            }
            if single_leaders(&common::leaders_by_probe(&refs).await).as_ref() != Some(&leaders) {
                continue;
            }
            let smallest = *leaders.keys().min().ok_or_else(|| anyhow!("empty table"))?;
            let runner = leaders[&smallest];
            for (i, r) in runs.into_iter().enumerate() {
                ensure!(
                    r == (i == runner),
                    "node {i}: is_runner={r}, but the leader of the smallest range ({smallest}) is node {runner}"
                );
            }
            break;
        }
        Ok(())
    })
    .await;
    for n in nodes {
        n.node.shutdown().await;
    }
    r
}

/// **The first D-d round does not wait a full tick.** A node's round slept `every`
/// (default 10 minutes; chaos and soak: 60 s) **before** its first "am I the runner"
/// question, so a node living shorter than `every` never ran D-d: a soak killing every
/// 10 s had a median node life of 20 s, only ~45/470 lives per node got past 60 s.
/// The same shape as the shared WAL sweep before `FIRST_SWEEP_AFTER`.
///
/// Order: a retired range nobody references (100) exists first; the node comes up with
/// `every` 10 minutes, `min_age` 0.
///
/// Must see: Db 100 is deleted within a few seconds of the node coming up, without
/// waiting 10 minutes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_dd_round_runs_soon_after_the_node_starts() -> Result<()> {
    let mut cfg = solo_cluster();
    let stores = cfg.stores.clone();
    // The range table exists before the fake Db: a bucket with `ranges/`/`owners/` but
    // no `cluster.conf` makes the node refuse to start (SM-21).
    cairn_store::ClusterConfig::open(stores.main().store(), stores.main().prefix(), 1).await?;
    fabricate_retired_range(&stores, 100, Some(wall_ms() - LONG_RETIRED_MS)).await?;
    cfg.gc_retired_dbs = Some(cairn_server::gc::GcConfig {
        every: Duration::from_secs(600),
        min_age: Duration::ZERO,
        mode: cairn_server::gc::GcMode::Delete,
    });
    let node = common::start_led(cfg).await?;
    let r = common::within(
        Duration::from_secs(30),
        "first D-d round after the node comes up",
        async {
            while stores.range_db_exists(PartitionId(100)).await? {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────────────── 3.2: min_age

/// **`min_age` (30 minutes).** A range is old enough to delete only when its tombstone
/// is older than `min_age`. A newly written tombstone carries `retired_ms=`; a format 9
/// record without that line uses the object's `last_modified`. Before: deleted on the
/// first sighting — under the feet of a follower reader still open or a long iterator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retired_db_younger_than_min_age_is_kept() -> Result<()> {
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(40), "D-d min_age", async {
        // A real tombstone from a split carries `retired_ms`.
        let before = wall_ms();
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        let t0 = std::time::Instant::now();
        let tomb = loop {
            let raw = raw_owner(&stores, 0).await?;
            if raw.contains("retired=1") {
                break raw;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "range 0 not a tombstone yet: {raw}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let at: u64 = tomb
            .lines()
            .find_map(|l| l.strip_prefix("retired_ms="))
            .ok_or_else(|| anyhow!("tombstone without retired_ms: {tomb}"))?
            .parse()?;
        ensure!(
            before <= at && at <= wall_ms(),
            "retired_ms={at} outside [{before}, now]"
        );

        let now = wall_ms();
        fabricate_retired_range(&stores, 100, Some(now)).await?; // just retired
        fabricate_retired_range(&stores, 101, None).await?; // format 9: age from last_modified
        fabricate_retired_range(&stores, 102, Some(now - LONG_RETIRED_MS)).await?; // old enough

        let deleted = cairn_server::node::gc_retired_dbs_once(&stores, &node.cluster)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(deleted == 1, "D-d must delete just range 102: {deleted}");
        for (r, want) in [(0, true), (100, true), (101, true), (102, false)] {
            ensure!(
                stores.range_db_exists(PartitionId(r)).await? == want,
                "Db of range {r}: want present={want}"
            );
        }
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────────────── barriers

async fn put_barrier(
    url: &str,
    id: &str,
    body: serde_json::Value,
) -> Result<(u16, serde_json::Value)> {
    let resp = reqwest::Client::new()
        .put(format!("{url}/admin/gc/barriers/{id}"))
        .json(&body)
        .send()
        .await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

async fn get_json(url: String) -> Result<(u16, serde_json::Value)> {
    let resp = reqwest::get(url).await?;
    let code = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((
        code,
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text)),
    ))
}

async fn gc_once(
    stores: &cairn_store::ObjectStores,
    node: &cairn_server::node::RunningNode,
) -> Result<usize> {
    cairn_server::node::gc_retired_dbs_once(stores, &node.cluster)
        .await
        .map_err(|e| anyhow!("{e}"))
}

/// **GC barriers.** `PUT /admin/gc/barriers/{id}` with a mandatory, capped TTL; a live
/// barrier keeps the Db of the ranges it covers; TTL=0 removes it; an expired barrier is
/// ignored and the D-d runner deletes it; an unreadable barrier (header of a newer
/// binary) blocks every range and is **not** deleted automatically. A late registrant —
/// a barrier covering a range already deleted — gets `missing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_barriers_hold_deletion_until_removed_or_expired() -> Result<()> {
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(40), "GC barriers", async {
        let old = Some(wall_ms() - LONG_RETIRED_MS);
        for r in [100, 101, 102] {
            fabricate_retired_range(&stores, r, old).await?;
        }
        let exists = |r: u32| {
            let stores = stores.clone();
            async move { stores.range_db_exists(PartitionId(r)).await }
        };

        // Rejected: no TTL, TTL over the cap, no holder, empty protects, bad id.
        for (id, body) in [
            ("a", serde_json::json!({ "holder": "t", "protects": "all" })),
            ("a", serde_json::json!({ "holder": "t", "protects": "all", "ttlMs": 25 * 3600 * 1000u64 })),
            ("a", serde_json::json!({ "protects": "all", "ttlMs": 60_000 })),
            ("a", serde_json::json!({ "holder": "t", "protects": [], "ttlMs": 60_000 })),
            ("a", serde_json::json!({ "holder": "t", "protects": "ranges:", "ttlMs": 60_000 })),
            (".a", serde_json::json!({ "holder": "t", "protects": "all", "ttlMs": 60_000 })),
        ] {
            let (code, body) = put_barrier(&url, id, body.clone()).await?;
            ensure!(code == 400, "{id} must be rejected: {code} {body}");
        }

        // A barrier covering 100 and 101; 102 is not covered.
        let (code, body) = put_barrier(
            &url,
            "investigation",
            serde_json::json!({ "holder": "ops", "protects": [100, 101], "ttlMs": 60_000, "reason": "investigating" }),
        )
        .await?;
        ensure!(code == 200, "{code} {body}");
        ensure!(body["previous"] == "absent" && body["missing"] == serde_json::json!([]), "{body}");
        let (code, list) = get_json(format!("{url}/admin/gc/barriers")).await?;
        ensure!(code == 200 && list["barriers"][0]["id"] == "investigation", "{code} {list}");
        ensure!(list["barriers"][0]["protects"] == "ranges:100,101", "{list}");
        ensure!(list["barriers"][0]["expired"] == false, "{list}");

        ensure!(gc_once(&stores, &node).await? == 1, "only 102 is uncovered");
        ensure!(exists(100).await? && exists(101).await? && !exists(102).await?);

        // Narrow it to 100 (renewal = overwrite).
        let (code, body) = put_barrier(
            &url,
            "investigation",
            serde_json::json!({ "holder": "ops", "protects": "ranges:100", "ttlMs": 60_000 }),
        )
        .await?;
        ensure!(code == 200 && body["previous"] == "active", "{code} {body}");
        ensure!(gc_once(&stores, &node).await? == 1 && !exists(101).await?, "101 no longer covered");
        ensure!(exists(100).await?);

        // TTL=0 removes it.
        let (code, body) = put_barrier(&url, "investigation", serde_json::json!({ "ttlMs": 0 })).await?;
        ensure!(code == 200 && body["deleted"] == true, "{code} {body}");
        let (code, _) = get_json(format!("{url}/admin/gc/barriers/investigation")).await?;
        ensure!(code == 404, "barrier removed yet still there: {code}");
        ensure!(gc_once(&stores, &node).await? == 1 && !exists(100).await?);

        // A late registrant: 100 has already been deleted.
        let (code, body) = put_barrier(
            &url,
            "late",
            serde_json::json!({ "holder": "backup", "protects": [100, 0], "ttlMs": 60_000 }),
        )
        .await?;
        ensure!(code == 200 && body["missing"] == serde_json::json!([100]), "{code} {body}");

        // An expired barrier: ignored at once; the D-d round deletes the object only
        // once it has been expired for more than `BARRIER_REAP_GRACE`
        // (`barrier_reap_spares_a_barrier_that_just_expired`).
        fabricate_retired_range(&stores, 103, old).await?;
        let (code, body) = put_barrier(
            &url,
            "tiny",
            serde_json::json!({ "holder": "t", "protects": "all", "ttlMs": 1 }),
        )
        .await?;
        ensure!(code == 200, "{code} {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (_, one) = get_json(format!("{url}/admin/gc/barriers/tiny")).await?;
        ensure!(one["expired"] == true, "{one}");
        ensure!(gc_once(&stores, &node).await? == 1 && !exists(103).await?, "expired barrier still blocks");
        let (code, one) = get_json(format!("{url}/admin/gc/barriers/tiny")).await?;
        ensure!(
            code == 200 && one["expired"] == true,
            "a just-expired barrier must stay until its grace ends: {code} {one}"
        );

        // An unreadable barrier (newer binary): blocks every range, nobody deletes it automatically.
        {
            use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
            stores
                .main()
                .store()
                .put(
                    &Path::from("gc-barriers/from-the-future"),
                    PutPayload::from(&b"cairnflow-gc-barrier/2\nprotects=ranges:1\nexpires_ms=1\n"[..]),
                )
                .await?;
        }
        fabricate_retired_range(&stores, 104, old).await?;
        ensure!(gc_once(&stores, &node).await? == 0 && exists(104).await?, "a foreign barrier must block");
        let (_, one) = get_json(format!("{url}/admin/gc/barriers/from-the-future")).await?;
        ensure!(one["error"].is_string(), "{one}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────────────── dry run

/// **Dry run.** `GET /admin/gc/plan` lists what the D-d round would delete (with bytes)
/// and every blocked range with its blocker — `reference`, `age`, `barrier` — without
/// deleting anything. The `plan` mode (`--gc-retired-dbs=plan`) deletes nothing
/// either, expired barriers included; the delete mode does exactly what the plan says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_plan_lists_eligible_and_blockers_without_deleting() -> Result<()> {
    use cairn_server::gc::{GcConfig, GcMode};
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let sdk = common::fake_sdk::FakeSdk::builder()
        .object("Counter", "add", vec![common::fake_sdk::Step::output("ok")])
        .spawn()
        .await?;
    let r = common::within(Duration::from_secs(40), "D-d plan", async {
        // The parent has rows ⇒ the two children reference its SSTs (an empty parent would not).
        common::register(&url, &sdk.url).await?;
        for i in 0..4 {
            let (code, body) = common::invoke(&url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
            ensure!(code == 200, "{code} {body}");
        }
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split: {code} {view}");
        // 0's tombstone ages (old enough) — only the two children's references still block it.
        let t0 = std::time::Instant::now();
        let tomb = loop {
            let raw = raw_owner(&stores, 0).await?;
            if raw.contains("retired=1") {
                break raw;
            }
            ensure!(t0.elapsed() < Duration::from_secs(10), "range 0 not a tombstone yet");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let old = wall_ms() - LONG_RETIRED_MS;
        let aged: String = tomb
            .lines()
            .filter(|l| !l.starts_with("retired_ms="))
            .map(|l| format!("{l}\n"))
            .chain([format!("retired_ms={old}\n")])
            .collect();
        stores
            .main()
            .store()
            .put(&Path::from("owners/range-0"), PutPayload::from(aged.into_bytes()))
            .await?;

        fabricate_retired_range(&stores, 100, Some(old)).await?;
        fabricate_retired_range(&stores, 101, Some(wall_ms())).await?;
        fabricate_retired_range(&stores, 102, Some(old)).await?;
        let (code, body) = put_barrier(
            &url,
            "hold-102",
            serde_json::json!({ "holder": "backup", "protects": [102], "ttlMs": 60_000, "reason": "backup" }),
        )
        .await?;
        ensure!(code == 200, "{code} {body}");
        let (code, body) = put_barrier(
            &url,
            "expired",
            serde_json::json!({ "holder": "old", "protects": "all", "ttlMs": 1 }),
        )
        .await?;
        ensure!(code == 200, "{code} {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (code, plan) = get_json(format!("{url}/admin/gc/plan")).await?;
        ensure!(code == 200, "{code} {plan}");
        ensure!(plan["tableVersion"] == node.cluster.version(), "{plan}");
        let eligible = plan["eligible"].as_array().ok_or_else(|| anyhow!("{plan}"))?;
        ensure!(eligible.len() == 1 && eligible[0]["range"] == 100, "{plan}");
        let bytes = eligible[0]["bytes"].as_u64().unwrap_or(0);
        ensure!(bytes > 0 && plan["bytes"] == bytes, "{plan}");
        let by = |r: u32| {
            plan["blocked"]
                .as_array()
                .and_then(|b| b.iter().find(|x| x["range"] == r))
                .map(|x| x["by"].as_str().unwrap_or_default().to_string())
        };
        ensure!(by(0).as_deref() == Some("reference"), "{plan}");
        ensure!(by(101).as_deref() == Some("age"), "{plan}");
        ensure!(by(102).as_deref() == Some("barrier"), "{plan}");
        ensure!(plan["expiredBarriers"] == serde_json::json!(["expired"]), "{plan}");
        ensure!(stores.range_db_exists(PartitionId(100)).await?, "the plan must delete nothing");

        // Plan mode: deletes no Db, deletes no barrier.
        let dry = GcConfig {
            mode: GcMode::Plan,
            ..GcConfig::default()
        };
        let round = cairn_server::gc::run_once(&stores, &node.cluster, &dry).await?;
        ensure!(round.deleted.is_empty() && round.plan.eligible.len() == 1, "{round:?}");
        ensure!(stores.range_db_exists(PartitionId(100)).await?);
        let (code, _) = get_json(format!("{url}/admin/gc/barriers/expired")).await?;
        ensure!(code == 200, "a dry run must not delete barriers");

        // Delete mode: exactly the plan.
        let round = cairn_server::gc::run_once(&stores, &node.cluster, &GcConfig::default()).await?;
        ensure!(round.deleted == vec![100], "{round:?}");
        for (r, want) in [(0, true), (100, false), (101, true), (102, true)] {
            ensure!(stores.range_db_exists(PartitionId(r)).await? == want, "range {r}: want present={want}");
        }
        // A just-expired barrier waits out `BARRIER_REAP_GRACE` before being deleted.
        let (code, _) = get_json(format!("{url}/admin/gc/barriers/expired")).await?;
        ensure!(code == 200, "a just-expired barrier must stay until its grace ends: {code}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────────────────────────── SM-11: proxy tombstones

/// Waits (up to 10 s) for the owner record of `r` to become a tombstone.
async fn wait_tombstone(stores: &cairn_store::ObjectStores, r: u32) -> Result<()> {
    let t0 = std::time::Instant::now();
    loop {
        let raw = raw_owner(stores, r).await?;
        if raw.contains("retired=1") {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "range {r} not a tombstone yet: {raw}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn put_raw(stores: &cairn_store::ObjectStores, path: &str, body: &str) -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    stores
        .main()
        .store()
        .put(&Path::from(path), PutPayload::from(body.to_string()))
        .await?;
    Ok(())
}

/// **SM-11 — the proxy tombstone round writes only what it can prove.** A real table
/// after two splits (0 → 2, 3; 3 → 4, 5), an intent in flight on range 1 (promising 6,
/// 7) and `next_range_id` pushed to 20. Owner records built by hand for ranges outside
/// the table:
///
/// | range | record | must |
/// |---|---|---|
/// | 0 | real tombstone | stay silent (already a tombstone) |
/// | 3 | owner dead, not released, has `wal_segment`, `data` | **tombstone**, drop `wal_segment`, keep every other field |
/// | 9 | owner alive but released | **tombstone** |
/// | 6 | owner dead, promised by an intent | keep: `promised` |
/// | 8 | owner is the node's live session | keep: `ownerLive` |
/// | 10 | cannot be decoded | keep: `unreadable` |
/// | 11 | owner dead, unknown `wal_kind` | keep: `unknownWalKind` |
/// | 25 | owner dead, id not allocated | keep: `unallocated` |
///
/// The second round writes nothing. A cluster being upgraded ⇒ the whole round skips
/// its turn; once the upgrade is over ⇒ the next round writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_retirer_writes_only_what_it_can_prove() -> Result<()> {
    use cairn_server::gc::{OrphanKept, OrphanRetirer};
    use cairn_store::{PartitionTable, RangeEntry, RangeIntent};

    let mut cfg = solo_cluster();
    cfg.mode = cairn_server::node::NodeMode {
        partitions: 2,
        lease_ttl: Duration::from_secs(2),
        node_id: Some("node-solo".into()),
    };
    cfg.retire_orphans_every = None;
    cfg.reconcile_intents_every = None;
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "proxy tombstones", async {
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split 0: {code} {view}");
        wait_tombstone(&stores, 0).await?;
        let (code, view) = post_json(format!("{url}/ranges/3/split")).await?;
        ensure!(code == 200, "split 3: {code} {view}");
        wait_tombstone(&stores, 3).await?;
        // An intent in flight on range 1 (promising 6, 7), then push next_range_id to 20:
        // 8..19 allocated, no range, nobody promised them.
        let v = node.cluster.table().version();
        let marked = node
            .cluster
            .update(v, |t| {
                let ranges = t
                    .ranges()
                    .iter()
                    .map(|r| {
                        if r.id == PartitionId(1) {
                            RangeEntry {
                                intent: Some(RangeIntent::Splitting {
                                    at: r.start + (r.end - r.start) / 2,
                                    left: PartitionId(6),
                                    right: PartitionId(7),
                                }),
                                ..*r
                            }
                        } else {
                            *r
                        }
                    })
                    .collect();
                PartitionTable::from_entries(t.version() + 1, 8, ranges)
            })
            .await?;
        ensure!(matches!(marked, cairn_store::TableUpdate::Applied(_)), "{marked:?}");
        let ranges = node.cluster.table().ranges().to_vec();
        node.cluster
            .update(v + 1, |t| PartitionTable::from_entries(t.version() + 1, 20, ranges))
            .await?;

        let me = node.session.current().ok_or_else(|| anyhow!("no session"))?;
        let dead = "cairnflow-owner/1\nterm=7\nsession=dead/1\naddress=http://dead\nwal_kind=shared\nwal_segment=dead/1:0@5\nwal_floor_id=4\ndata=2\n";
        put_raw(&stores, "owners/range-3", dead).await?;
        put_raw(&stores, "owners/range-6", dead).await?;
        put_raw(&stores, "owners/range-25", dead).await?;
        put_raw(&stores, "owners/range-8", &format!("cairnflow-owner/1\nterm=1\nsession={me}\nwal_kind=shared\n")).await?;
        put_raw(&stores, "owners/range-9", &format!("cairnflow-owner/1\nterm=1\nsession={me}\nreleased=1\nwal_kind=shared\n")).await?;
        put_raw(&stores, "owners/range-10", "not-a-record\n").await?;
        put_raw(&stores, "owners/range-11", "cairnflow-owner/1\nterm=1\nsession=dead/1\nwal_kind=future\n").await?;
        let untouched = [6, 8, 10, 11, 25];
        let mut raw_before = Vec::new();
        for r in untouched {
            raw_before.push(raw_owner(&stores, r).await?);
        }

        let session = node.session.clone();
        let mut retirer = OrphanRetirer::default();
        let since = wall_ms();
        let round = retirer.run_once(&stores, &node.cluster, &session).await?;
        ensure!(round.table_version == v + 2, "{round:?}");
        let mut retired = round.retired.clone();
        retired.sort();
        ensure!(retired == vec![3, 9], "must tombstone exactly 3 and 9: {round:?}");
        let mut kept: Vec<(u32, OrphanKept)> = round.kept.iter().map(|k| (k.range, k.by)).collect();
        kept.sort_by_key(|k| k.0);
        ensure!(
            kept == vec![
                (6, OrphanKept::Promised),
                (8, OrphanKept::OwnerLive),
                (10, OrphanKept::Unreadable),
                (11, OrphanKept::UnknownWalKind),
                (25, OrphanKept::Unallocated),
            ],
            "keep reasons: {round:?}"
        );
        let tomb = raw_owner(&stores, 3).await?;
        let rec = cairn_store::OwnerRecord::decode(tomb.as_bytes())?;
        ensure!(rec.retired && rec.released, "{tomb}");
        ensure!(rec.retired_ms.is_some_and(|ms| ms >= since), "{tomb}");
        let want = cairn_store::OwnerRecord::decode(dead.as_bytes())?;
        ensure!(
            rec.term == want.term
                && rec.session == want.session
                && rec.wal_kind == want.wal_kind
                && rec.wal_floor_id == want.wal_floor_id
                && rec.data == want.data,
            "the tombstone must keep every other field: {tomb}"
        );
        // Only the node taking over a range reads `wal_segment`, and a range carrying
        // a tombstone is never taken over again — a tombstone that copies a dead
        // owner's segment keeps that log from ever being cleaned up for good.
        ensure!(
            !want.wal_segments.is_empty() && rec.wal_segments.is_empty(),
            "a tombstone must not keep a dead owner's WAL segment: {tomb}"
        );
        ensure!(raw_owner(&stores, 9).await?.contains("retired=1"));
        for (r, before) in untouched.iter().zip(&raw_before) {
            ensure!(&raw_owner(&stores, *r).await? == before, "record of range {r} changed");
        }

        // Second round: nothing new.
        let again = retirer.run_once(&stores, &node.cluster, &session).await?;
        ensure!(again.retired.is_empty(), "{again:?}");

        // Upgrading ⇒ skip the turn; upgrade over ⇒ write.
        put_raw(&stores, "owners/range-12", dead).await?;
        let cv = {
            use slatedb::object_store::{path::Path, ObjectStoreExt};
            let got = stores.main().store().get(&Path::from("cluster-version")).await?;
            String::from_utf8(got.bytes().await?.to_vec())?
        };
        let upgrading = cv
            .lines()
            .filter(|l| !l.starts_with("upgrading="))
            .chain(std::iter::once("upgrading=0->1@ghost/1"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        put_raw(&stores, "cluster-version", &upgrading).await?;
        let skipped = retirer.run_once(&stores, &node.cluster, &session).await?;
        ensure!(skipped.skipped.is_some() && skipped.retired.is_empty(), "{skipped:?}");
        ensure!(!raw_owner(&stores, 12).await?.contains("retired=1"));
        put_raw(&stores, "cluster-version", &cv).await?;
        let after = retirer.run_once(&stores, &node.cluster, &session).await?;
        ensure!(after.retired == vec![12], "{after:?}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ──────────────────────── tombstones on a node without `--cluster`

/// **Tombstones on a node without `--cluster`.** Formerly such a node
/// (`SingleProcess`) had no session, so the engine had no keeper, and `retire` wrote
/// nothing: the parent range left the table without a tombstone — `verify` reported
/// `orphan_db`, and D-d never saw the parent. The first patch (`gc::retire_alone`) was a
/// separate tombstone write path; now such a node is a one-node cluster and the
/// tombstone goes the cluster's way (the engine's keeper, SM-11 when it dies).
///
/// 1. Node `NodeMode::single`, one range, nothing written (an empty parent: the
///    children do not stand on any SST of the parent, so D-d may delete the parent at
///    once).
/// 2. Split 0 → `l`, `r`; merge `l` + `r` → `m`.
/// 3. A D-d round with `min_age` 0.
///
/// Must see: after each step, the ranges that just left the table (0, then `l`, `r`)
/// carry a tombstone; `verify` reports nothing; the D-d round deletes the Db of all
/// three and `verify` is still clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_process_split_and_merge_leave_tombstones_that_gc_reaps() -> Result<()> {
    let cfg = common::test_config(cairn_server::node::NodeMode::single(1));
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(
        Duration::from_secs(60),
        "tombstone on a node without --cluster",
        async {
            let verify = || async {
                cairn_server::ctl::Ctl::new(stores.clone())
                    .verify(cairn_server::ctl::VerifyOptions {
                        open_dbs: false,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow!("{e}"))
            };
            // A range just split/merged may have no leader for a short while (`503`).
            let settled = |u: String| async move {
                let t0 = std::time::Instant::now();
                loop {
                    let (code, body) = post_json(u.clone()).await?;
                    if code != 503 || t0.elapsed() > Duration::from_secs(20) {
                        return Ok::<_, anyhow::Error>((code, body));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            };
            let children = |view: &serde_json::Value| -> Result<(u32, u32)> {
                let c: Vec<u32> = view["report"]["children"]
                    .as_array()
                    .ok_or_else(|| anyhow!("{view}"))?
                    .iter()
                    .map(|r| r["range"].as_u64().unwrap_or(0) as u32)
                    .collect();
                ensure!(c.len() == 2, "{view}");
                Ok((c[0], c[1]))
            };

            let (code, view) = settled(format!("{url}/ranges/0/split")).await?;
            ensure!(code == 200, "split 0: {code} {view}");
            let (l, rr) = children(&view)?;
            wait_tombstone(&stores, 0).await?;
            let rep = verify().await?;
            ensure!(rep.clean(), "after split: {:?}", rep.findings);

            let (code, view) = settled(format!("{url}/ranges/merge?left={l}&right={rr}")).await?;
            ensure!(code == 200, "merge {l}+{rr}: {code} {view}");
            for p in [l, rr] {
                wait_tombstone(&stores, p).await?;
            }
            let rep = verify().await?;
            ensure!(rep.clean(), "after merge: {:?}", rep.findings);

            let gc = cairn_server::gc::GcConfig {
                min_age: Duration::ZERO,
                ..cairn_server::gc::GcConfig::default()
            };
            let round = cairn_server::gc::run_once(&stores, &node.cluster, &gc)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let mut deleted = round.deleted.clone();
            deleted.sort();
            let mut want = vec![0, l, rr];
            want.sort();
            ensure!(deleted == want, "D-d must delete Dbs {want:?}: {round:?}");
            for p in want {
                ensure!(
                    !stores.range_db_exists(PartitionId(p)).await?,
                    "Db of range {p} still there"
                );
            }
            let rep = verify().await?;
            ensure!(rep.clean(), "after D-d: {:?}", rep.findings);
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

// ──────────────────────── SM-14: a retired parent without an owner record

/// Writes a little work to range 0 (keys spread over both halves) and waits for it. Returns the ids.
async fn fill_range_0(url: &str, sdk: &common::fake_sdk::FakeSdk) -> Result<Vec<String>> {
    common::register(url, &sdk.url).await?;
    let mut ids = Vec::new();
    for i in 0..16 {
        let (code, body) =
            common::invoke(url, "Counter", "add", Some(&format!("k-{i}")), "x").await?;
        ensure!(code == 200, "invoke k-{i}: {code} {body}");
        ids.push(
            body["id"]
                .as_str()
                .ok_or_else(|| anyhow!("{body}"))?
                .to_string(),
        );
    }
    for id in &ids {
        common::wait_status(url, id, "completed").await?;
    }
    Ok(ids)
}

/// `cairn-ctl verify` does not open Dbs (only manifests and `external_dbs`).
async fn verify_manifests(
    stores: &cairn_store::ObjectStores,
) -> Result<cairn_server::ctl::VerifyReport> {
    cairn_server::ctl::Ctl::new(stores.clone())
        .verify(cairn_server::ctl::VerifyOptions {
            open_dbs: false,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow!("{e}"))
}

/// **SM-14 does not discard a Db that a range in the table still stands on** (a data
/// loss risk).
///
/// A bucket that a single-process node split **before** the range-level migration
/// framework has a retired parent without an owner record. The test builds exactly that
/// shape:
///
/// 1. A one-node cluster, one range; write work to both halves of range 0 (the split
///    flushes it into SSTs of 0).
/// 2. Split 0 → `l`, `r`: the children stand on 0's SSTs (`external_dbs` lists 0).
/// 3. Delete `owners/range-0` — the shape of a pre-migration bucket: the parent outside
///    the table, with a manifest, without an owner record.
/// 4. One `OrphanRetirer` round (cluster mode).
/// 5. Shut the node down, rebuild it on the same bucket (no block cache left), read
///    back all the work written.
///
/// Must see: the round does not discard 0 and keeps it with reason `referenced`; 0's Db
/// is still there; `verify` is clean (no `external_db` missing its source); after the
/// rebuild, all work is readable and `completed`. Before the fix: SM-14's second proof
/// ("has a manifest but no owner record ⇒ never entered the table") discarded 0's Db
/// under the feet of `l` and `r`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_round_keeps_retired_parent_without_owner_that_children_stand_on() -> Result<()> {
    use cairn_server::gc::{OrphanKept, OrphanRetirer};
    use common::fake_sdk::{FakeSdk, Step};

    let mut cfg = solo_cluster();
    cfg.retire_orphans_every = None;
    cfg.reconcile_intents_every = None;
    let stores = cfg.stores.clone();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "ownerless parent", async {
        let ids = fill_range_0(&url, &sdk).await?;
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split 0: {code} {view}");
        wait_tombstone(&stores, 0).await?;
        let mut standing = Vec::new();
        for c in node.cluster.table().partitions() {
            let ext = stores.external_ranges(c).await?.unwrap_or_default();
            if ext.contains(&PartitionId(0)) {
                standing.push(c.0);
            }
        }
        ensure!(
            !standing.is_empty(),
            "no child stands on 0 — the test did not build the right shape"
        );

        // (3) A bucket before the range-level migration: the parent has no owner record.
        let main = stores.main();
        cairn_store::delete_owner(
            main.store(),
            main.prefix(),
            cairn_store::proof::DeleteProof::assume_owner(PartitionId(0)),
        )
        .await?;

        let session = node.session.clone();
        let round = OrphanRetirer::default()
            .run_once(&stores, &node.cluster, &session)
            .await?;
        ensure!(
            !round.discarded.contains(&0),
            "the round discarded the parent's Db while {standing:?} still stand on it: {round:?}"
        );
        ensure!(
            round
                .kept
                .iter()
                .any(|k| k.range == 0 && k.by == OrphanKept::Referenced),
            "must keep 0 with reason referenced: {round:?}"
        );
        ensure!(
            stores.range_db_exists(PartitionId(0)).await?,
            "Db of 0 lost"
        );
        let rep = verify_manifests(&stores).await?;
        ensure!(
            !rep.findings.iter().any(|f| f.check == "external_db"),
            "child lost its source: {:?}",
            rep.findings
        );
        Ok(ids)
    })
    .await;
    node.shutdown().await;
    let ids = r?;

    // (5) Rebuild on the same bucket and read back.
    let mut cfg = solo_cluster();
    cfg.retire_orphans_every = None;
    cfg.reconcile_intents_every = None;
    cfg.stores = stores.clone();
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(
        Duration::from_secs(30),
        "read back after the rebuild",
        async {
            for id in &ids {
                let (code, st) = common::invocation(&url, id).await?;
                ensure!(
                    code == 200 && st["status"] == "completed",
                    "{id}: {code} {st}"
                );
                ensure!(st["output"] == "ok", "{id}: {st}");
            }
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **The other direction: a true orphan child is still discarded.** The fix adds the
/// check "no range in the table stands on `r`"; the orphan child of a rollback has
/// nobody standing on it, so it still goes. This is the second proof (no owner record,
/// standing only on retired ranges); the first proof (standing on a live range) is
/// covered by `split.rs::half_done_roll_back_is_finished_by_a_later_round`.
///
/// 1. A one-node cluster; write work to range 0, split 0 → `l`, `r` (0 becomes a
///    tombstone).
/// 2. Push `next_range_id` up by 1: id `b` allocated, outside the table, promised by
///    nobody.
/// 3. Clone 0 → `b`, without an owner record — the shape of an orphan child whose
///    roller-back deleted the owner record (or had not taken it yet) and then died,
///    with its source retired afterwards (seen in a chaos run).
/// 4. One `OrphanRetirer` round.
///
/// Must see: `b` is discarded; 0 (a tombstone with children standing on it) is not;
/// `verify` is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_round_still_discards_true_orphans_nobody_stands_on() -> Result<()> {
    use cairn_server::gc::OrphanRetirer;
    use cairn_store::PartitionTable;
    use common::fake_sdk::{FakeSdk, Step};

    let mut cfg = solo_cluster();
    cfg.retire_orphans_every = None;
    cfg.reconcile_intents_every = None;
    let stores = cfg.stores.clone();
    let sdk = FakeSdk::builder()
        .object("Counter", "add", vec![Step::output("ok")])
        .spawn()
        .await?;
    let node = common::start_led(cfg).await?;
    let url = node.url();
    let r = common::within(Duration::from_secs(60), "true orphan child", async {
        fill_range_0(&url, &sdk).await?;
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split 0: {code} {view}");
        wait_tombstone(&stores, 0).await?;

        let t = node.cluster.table();
        let b = PartitionId(t.next_range_id());
        let ranges = t.ranges().to_vec();
        let up = node
            .cluster
            .update(t.version(), |t| {
                PartitionTable::from_entries(t.version() + 1, b.0 + 1, ranges)
            })
            .await?;
        ensure!(matches!(up, cairn_store::TableUpdate::Applied(_)), "{up:?}");
        stores
            .clone_range(
                PartitionId(0),
                b,
                (
                    cairn_store::PartitionKey(0),
                    cairn_store::PartitionKey(u64::MAX),
                ),
            )
            .await?;
        ensure!(
            stores.external_ranges(b).await?.unwrap_or_default() == vec![PartitionId(0)],
            "b must stand only on 0"
        );

        let session = node.session.clone();
        let round = OrphanRetirer::default()
            .run_once(&stores, &node.cluster, &session)
            .await?;
        ensure!(
            round.discarded == vec![b.0],
            "must discard exactly {}: {round:?}",
            b.0
        );
        ensure!(
            !stores.range_db_exists(b).await?,
            "Db of orphan child {} still there",
            b.0
        );
        ensure!(
            stores.range_db_exists(PartitionId(0)).await?,
            "Db of tombstone 0 discarded"
        );
        let rep = verify_manifests(&stores).await?;
        ensure!(rep.clean(), "{:?}", rep.findings);
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────────── orphan child cleanup on a nudge

/// The grid of the orphan child round in the nudge tests: one hour, like the default —
/// the test is over before the grid fires, so the only thing that can clean the orphan
/// child is a nudge.
const ORPHAN_GRID: Duration = Duration::from_secs(3600);

/// **Nudge (b): a range that left the table and whose owner just died is tombstoned
/// within a few seconds** (SM-11).
///
/// Interleaving:
/// 1. A one-node cluster, orphan child round grid [`ORPHAN_GRID`]. The node has been
///    the D-d runner since it started leading range 0; its first round (nudge (c)) has
///    passed.
/// 2. Push `next_range_id` up by 1: id `x` allocated, outside the table, promised by
///    nobody.
/// 3. A `ghost` session (no engine) opens on the same bucket; the owner record of `x`
///    points to that session, not released, no tombstone — the shape of "the leader
///    died between the table commit and the tombstone write", before the leader dies.
///    The node's tracker sees `ghost` alive.
/// 4. `ghost` dies like `kill -9` (its renewal loop stops).
///
/// Must see: within 15 s (TTL 2 s of silence, then the tracker CASes `expired`), the
/// owner record of `x` carries a tombstone. Before: the round only ran on its tick, and
/// with a one-hour grid it had not run even once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_whose_owner_just_died_is_tombstoned_by_a_nudge() -> Result<()> {
    use cairn_store::PartitionTable;

    let mut cfg = solo_cluster();
    cfg.retire_orphans_every = Some(ORPHAN_GRID);
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(Duration::from_secs(60), "nudge (b)", async {
        // (1) Past the first round of the new runner (nudge (c)).
        tokio::time::sleep(cairn_server::gc::RUNNER_CHECK_EVERY + Duration::from_secs(1)).await;
        // (2)
        let t = node.cluster.table();
        let x = PartitionId(t.next_range_id());
        let ranges = t.ranges().to_vec();
        let up = node
            .cluster
            .update(t.version(), |t| {
                PartitionTable::from_entries(t.version() + 1, x.0 + 1, ranges)
            })
            .await?;
        ensure!(matches!(up, cairn_store::TableUpdate::Applied(_)), "{up:?}");
        // (3)
        let ghost = cairn_store::Session::establish(
            std::sync::Arc::clone(stores.main().store()),
            stores.main().prefix(),
            cairn_store::LeaseConfig::new()
                .with_node_id("ghost")
                .with_ttl(Duration::from_secs(2)),
        )
        .await?;
        let gid = ghost
            .current()
            .ok_or_else(|| anyhow!("ghost has no session"))?;
        put_raw(
            &stores,
            &format!("owners/range-{}", x.0),
            &format!("cairnflow-owner/1\nterm=1\nsession={gid}\nwal_kind=shared\n"),
        )
        .await?;
        let t0 = std::time::Instant::now();
        while !node.session.tracker().live_nodes().contains("ghost") {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "tracker does not see ghost"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ensure!(
            !raw_owner(&stores, x.0).await?.contains("retired=1"),
            "tombstone before the owner died"
        );
        // (4)
        ghost.crash_handle().crash();
        let t0 = std::time::Instant::now();
        loop {
            let raw = raw_owner(&stores, x.0).await?;
            if raw.contains("retired=1") {
                break;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(15),
                "15 s after the owner died, range {} is not a tombstone — no nudge happened: {raw}",
                x.0
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(ghost);
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **Nudge (c): a node that just became the D-d runner discards an existing orphan
/// child at once** (SM-14).
///
/// Interleaving:
/// 1. Node `n1` alone, one range, grid [`ORPHAN_GRID`]; its first round has passed.
///    Split 0 → `l`, `r` (0 becomes a tombstone).
/// 2. Push `next_range_id` up by 1, clone 0 → `b`, without an owner record (as in
///    `orphan_round_still_discards_true_orphans_nobody_stands_on`).
/// 3. Node `n2` (same bucket, same grid) joins the cluster; `n1` shuts down cleanly.
///    `n2` takes every range and becomes the runner.
///
/// Must see: within 20 s after `n1` shuts down, `b`'s Db is gone. Before: `n2`'s round
/// only ran on its tick, and with a one-hour grid it had not run even once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_is_discarded_as_soon_as_a_node_becomes_the_runner() -> Result<()> {
    use cairn_store::PartitionTable;

    let mut cfg = solo_cluster();
    cfg.retire_orphans_every = Some(ORPHAN_GRID);
    let stores = cfg.stores.clone();
    let n1 = common::start_led(cfg).await?;
    let url = n1.url();
    let b = common::within(Duration::from_secs(60), "build an orphan child", async {
        tokio::time::sleep(cairn_server::gc::RUNNER_CHECK_EVERY + Duration::from_secs(1)).await;
        let (code, view) = post_json(format!("{url}/ranges/0/split")).await?;
        ensure!(code == 200, "split 0: {code} {view}");
        wait_tombstone(&stores, 0).await?;
        let t = n1.cluster.table();
        let b = PartitionId(t.next_range_id());
        let ranges = t.ranges().to_vec();
        let up = n1
            .cluster
            .update(t.version(), |t| {
                PartitionTable::from_entries(t.version() + 1, b.0 + 1, ranges)
            })
            .await?;
        ensure!(matches!(up, cairn_store::TableUpdate::Applied(_)), "{up:?}");
        stores
            .clone_range(
                PartitionId(0),
                b,
                (
                    cairn_store::PartitionKey(0),
                    cairn_store::PartitionKey(u64::MAX),
                ),
            )
            .await?;
        Ok(b)
    })
    .await;
    let b = match b {
        Ok(b) => b,
        Err(e) => {
            n1.shutdown().await;
            return Err(e);
        }
    };
    let mut cfg2 = common::test_config(cairn_server::node::NodeMode {
        partitions: 1,
        lease_ttl: Duration::from_secs(2),
        node_id: Some("node-solo-2".into()),
    });
    cfg2.stores = stores.clone();
    cfg2.node_wal.window = Duration::from_millis(5);
    cfg2.retire_orphans_every = Some(ORPHAN_GRID);
    let n2 = cairn_server::node::start(cfg2)
        .await
        .map_err(|e| anyhow!("build n2: {e}"))?;
    n1.shutdown().await;
    let r = common::within(Duration::from_secs(30), "nudge (c)", async {
        let t0 = std::time::Instant::now();
        while stores.range_db_exists(b).await? {
            ensure!(
                t0.elapsed() < Duration::from_secs(20),
                "20 s after n2 became the runner, the Db of orphan child {} is still there — no nudge happened",
                b.0
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let rep = verify_manifests(&stores).await?;
        ensure!(rep.clean(), "{:?}", rep.findings);
        Ok(())
    })
    .await;
    n2.shutdown().await;
    r
}

// ─────────────────────────────────────── state scrubber

/// The scrubber's counters are process statics: tests reading their deltas run one at
/// a time (`cargo test` runs the tests of one binary in a shared process).
static SCRUB_STATS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// **The scrubber only counts findings that persist over two consecutive rounds, and
/// fixes nothing.**
///
/// 1. A one-node cluster, D-d does **not** run. Fabricate retired range 102, a
///    31-minute-old tombstone, referenced by nobody — exactly what D-d deletes
///    (`retired_unreferenced`, (h)).
/// 2. Round 1: a new finding, not counted yet (it may be the half-done state of a tick).
/// 3. Round 2: still there ⇒ counted, metric = 1; Db 102 **still there** (a dry run).
/// 4. A D-d round deletes it; round 3: the metric goes back to 0.
///
/// Must see: metric 0 → 1 → 0, and the Db is lost only after the D-d round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scrubber_counts_only_findings_that_persist_two_rounds() -> Result<()> {
    use cairn_server::scrub::{stats, ScrubConfig, Scrubber};
    let _stats = SCRUB_STATS.lock().await;
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(Duration::from_secs(40), "three scrub rounds", async {
        fabricate_retired_range(&stores, 102, Some(wall_ms() - LONG_RETIRED_MS)).await?;
        let scfg = ScrubConfig {
            every: Duration::from_secs(1),
            gc_min_age: Some(Duration::from_secs(60)),
        };
        let mut s = Scrubber::default();
        let r1 = s
            .run_once(&stores, &scfg)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            r1.persistent.is_empty(),
            "round 1 already counted: {:?}",
            r1.persistent
        );
        ensure!(r1.fresh >= 1, "round 1 misses the forgotten retired Db");
        ensure!(stats::findings("retired_unreferenced") == 0);
        let r2 = s
            .run_once(&stores, &scfg)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            r2.persistent
                .iter()
                .any(|f| f.check == "retired_unreferenced"
                    && f.range == Some(102)
                    && f.heals == Some("D-d")),
            "round 2 must count Db 102: {:?}",
            r2.persistent
        );
        ensure!(stats::findings("retired_unreferenced") == 1);
        ensure!(
            stores.range_db_exists(PartitionId(102)).await?,
            "the scrubber is a dry run yet Db 102 is gone"
        );
        let deleted = cairn_server::node::gc_retired_dbs_once(&stores, &node.cluster)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(deleted == 1, "D-d must delete Db 102, deleted {deleted}");
        let r3 = s
            .run_once(&stores, &scfg)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            r3.persistent.is_empty(),
            "still there after D-d: {:?}",
            r3.persistent
        );
        ensure!(stats::findings("retired_unreferenced") == 0);
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The scrubber tolerates per-item failures.** Seen in a 4 h soak: 28/31 rounds failed
/// under the `s3-standard-faults` profile — one failing LIST `ranges/<id>`, `nodewal/`
/// or GET `sessions/` failed the whole round.
///
/// 1. Node `scan-a` (shared WAL) comes up, runs, then shuts down cleanly: session
///    `closed`, its log is the log of a dead incarnation. Also fabricate retired ranges
///    101–104.
/// 2. The scrubber runs on a separate store wrapping the same bucket, failing:
///    - **forever** every LIST `ranges/103…` (an item that is broken for good),
///    - **forever** every GET `sessions/…` (one broken item per incarnation),
///    - **once** a LIST `owners` (a transient error of a shared read).
///
/// Must see: the round **finishes** (`Ok`) — the transient error is retried, the items
/// broken for good are skipped and counted — and the round is counted neither as clean
/// nor as having findings (skips over the threshold ⇒ incomplete), nor as a failed
/// round. Before the fix: `Err`, a failed round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scrubber_round_survives_per_item_read_errors() -> Result<()> {
    use {
        cairn_server::scrub::{stats, ScrubConfig, Scrubber},
        cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
        std::sync::atomic::Ordering::Relaxed,
    };
    let _stats = SCRUB_STATS.lock().await;
    let bucket = common::bucket();
    bucket.allow(
        "retired_unreferenced",
        "the test fabricates retired ranges 101–104 and does not run D-d",
    );
    let a = common::start_cluster_node(&bucket, "scan-a", 2, Duration::from_secs(2)).await?;
    let plain = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    ));
    common::within(Duration::from_secs(60), "one scrub round under faults", async {
        wait_probe(&[&a], Duration::from_secs(20), "a leads", |p| {
            single_leaders(p).is_some()
        })
        .await?;
        a.node.shutdown().await;
        for r in 101..=104 {
            fabricate_retired_range(&plain, r, Some(wall_ms() - LONG_RETIRED_MS)).await?;
        }
        let faulty = FaultyStore::wrap(bucket.clone());
        faulty.add_rule(Rule::new(
            OpSet::LIST,
            PathMatch::Contains("ranges/103".into()),
            Action::Fail { times: None },
        ));
        faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("sessions/".into()),
            Action::Fail { times: None },
        ));
        faulty.add_rule(Rule::new(
            OpSet::LIST,
            PathMatch::Prefix("owners".into()),
            Action::FailNth { skip: 0 },
        ));
        let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
            faulty.clone(),
            "",
            "memory://",
        ));
        let before = (
            stats::ROUNDS_CLEAN.load(Relaxed),
            stats::ROUNDS_FOUND.load(Relaxed),
            stats::ROUNDS_FAILED.load(Relaxed),
        );
        let (incomplete0, skipped0) = (
            stats::ROUNDS_INCOMPLETE.load(Relaxed),
            stats::SKIPPED.load(Relaxed),
        );
        let round = Scrubber::default()
            .run_once(
                &stores,
                &ScrubConfig {
                    every: Duration::from_secs(1),
                    gc_min_age: None,
                },
            )
            .await;
        ensure!(faulty.injected().0 >= 1, "fault rule did not fire");
        let round = round.map_err(|e| anyhow!("one broken item failed the whole round: {e}"))?;
        let after = (
            stats::ROUNDS_CLEAN.load(Relaxed),
            stats::ROUNDS_FOUND.load(Relaxed),
            stats::ROUNDS_FAILED.load(Relaxed),
        );
        ensure!(
            after == before,
            "a round with skips over the threshold must be incomplete, not clean/with findings/failed: \
             {before:?} → {after:?} ({round:?})"
        );
        // The skipped items are named and counted; the round is incomplete.
        let items: Vec<&str> = round.skipped.iter().map(|s| s.item.as_str()).collect();
        ensure!(
            items.contains(&"ranges/103"),
            "ranges/103 missing from the skipped items: {items:?}"
        );
        ensure!(
            round.skipped.iter().any(|s| s.check == "unheld_wal_log"
                && s.session
                    .as_deref()
                    .is_some_and(|x| x.starts_with("scan-a/"))),
            "scan-a's incarnation missing from the skipped items: {:?}",
            round.skipped
        );
        ensure!(
            !round.complete,
            "{} skipped items out of {} items yet the round is complete",
            round.skipped.len(),
            round.items
        );
        ensure!(stats::ROUNDS_INCOMPLETE.load(Relaxed) == incomplete0 + 1);
        ensure!(stats::SKIPPED.load(Relaxed) == skipped0 + round.skipped.len() as u64);
        Ok(())
    })
    .await
}

/// **(g) a shared WAL log nobody holds**: the log of an incarnation that has ended,
/// listed by no owner record — the dead log sweep (NW-8) of **another** node must
/// clean it up.
///
/// 1. Node a (shared WAL) runs an invocation, then shuts down cleanly: a clean release
///    drops the segment from the owner record (NW-6), session `closed`, the log object
///    stays.
/// 2. No node alive ⇒ `verify --wal-logs` sees a's log, `heals` NW-8.
/// 3. Node b comes up on the same bucket ⇒ its first sweep cleans a's log.
///
/// Must see: (2) exactly one finding for a's incarnation; after (3) none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_flags_unheld_dead_log_until_a_live_node_sweeps_it() -> Result<()> {
    use cairn_server::ctl::{Ctl, VerifyOptions};
    let bucket = common::bucket();
    let sdk = common::fake_sdk::FakeSdk::builder()
        .service("Svc", "h", vec![common::fake_sdk::Step::output("1")])
        .spawn()
        .await?;
    let start =
        |id: &'static str| common::start_cluster_node(&bucket, id, 1, Duration::from_secs(2));
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    ));
    let ctl = Ctl::new(stores);
    let unheld = || async {
        let rep = ctl
            .verify(VerifyOptions {
                open_dbs: false,
                wal_logs: true,
                ..VerifyOptions::default()
            })
            .await
            .map_err(|e| anyhow!("{e}"))?;
        Ok::<_, anyhow::Error>(
            rep.findings
                .into_iter()
                .filter(|f| f.check == "unheld_wal_log")
                .collect::<Vec<_>>(),
        )
    };
    common::within(
        Duration::from_secs(60),
        "dead log seen, then cleaned",
        async {
            let a = start("scrub-a").await?;
            common::wait_probe(
                &[&a],
                Duration::from_secs(20),
                "a is leader of range 0",
                |p| single_leaders(p).is_some(),
            )
            .await?;
            common::register(&a.node.url(), &sdk.url).await?;
            let (code, body) = common::invoke(&a.node.url(), "Svc", "h", None, "{}").await?;
            ensure!(code == 200, "{code} {body}");
            ensure!(unheld().await?.is_empty(), "a live node's log was reported");
            a.node.shutdown().await;
            let found = unheld().await?;
            ensure!(
                found.len() == 1
                    && found[0].detail.contains("scrub-a/")
                    && found[0].heals == Some("NW-8"),
                "a's log after a clean shutdown: {found:?}"
            );
            let b = start("scrub-b").await?;
            let t0 = std::time::Instant::now();
            loop {
                if unheld().await?.is_empty() {
                    break;
                }
                ensure!(
                    t0.elapsed() < Duration::from_secs(30),
                    "node b did not clean a's log: {:?}",
                    unheld().await?
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            b.node.shutdown().await;
            Ok(())
        },
    )
    .await
}

// ─────────────────────────────────────────────── trash

/// **The trash purge round only really deletes what nobody needs.**
///
/// 1. D-d deletes the Db of range 102 (retired, old enough) ⇒ a `range-db 102` entry.
/// 2. A `range-db 0` entry — range 0 is **still in the table** — fakes a wrong delete.
/// 3. A purge round with `keep = 0`.
///
/// Must see: the entry of 102 is really deleted; the entry of 0 is kept with decision
/// `StillNeeded` (the `trash::purge::still_needed` tripwire is there).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trash_round_purges_only_what_nobody_needs() -> Result<()> {
    use {
        cairn_store::trash::{EntryId, Subject, Verdict},
        slatedb::object_store::{path::Path, ObjectStoreExt},
    };
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(Duration::from_secs(40), "trash purge round", async {
        fabricate_retired_range(&stores, 102, Some(wall_ms() - LONG_RETIRED_MS)).await?;
        ensure!(gc_once(&stores, &node).await? == 1, "D-d left range 102");

        let scratch = Path::from("scratch/mistaken-delete");
        stores
            .main()
            .store()
            .put(&scratch, bytes::Bytes::from_static(b"x").into())
            .await?;
        let wrong = EntryId::new(wall_ms());
        stores.trash_bins()[0]
            .move_into(&wrong, &Subject::RangeDb(0), "test: mistake", &[scratch])
            .await?;

        let round = cairn_server::gc::purge_trash_once(&stores, &node.cluster, Duration::ZERO)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(round.purged.len() == 1, "{round:?}");
        ensure!(
            matches!(round.kept.as_slice(), [(id, Verdict::StillNeeded(_))] if *id == wrong),
            "{round:?}"
        );
        let left: Vec<_> = stores.trash_bins()[0]
            .entries()
            .await?
            .into_iter()
            .filter_map(|e| e.subject)
            .collect();
        ensure!(left == [Subject::RangeDb(0)], "left {left:?}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **The age of a trash entry has a lower bound from the store clock.** The entry id
/// carries the ms of **the mover** (a node may be days slow); the purge round only
/// checks the clock of the runner itself against the store.
///
/// 1. Range 103 retired long ago (nobody needs its Db).
/// 2. A `range-db 103` entry just written, whose id carries ms from 8 days ago (as if
///    moved in by a node 8 days slow).
/// 3. A purge round with `keep` 7 days.
///
/// Must see: the entry is kept — its `last_modified` (the store clock) is a few seconds
/// old. Before the fix: the age was taken from the id alone ⇒ really deleted at once,
/// the 7-day undo net down to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trash_age_is_bounded_by_the_store_clock() -> Result<()> {
    use {
        cairn_store::trash::{EntryId, Subject},
        slatedb::object_store::{path::Path, ObjectStoreExt},
    };
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(
        Duration::from_secs(40),
        "entry age by the store clock",
        async {
            fabricate_retired_range(&stores, 103, Some(wall_ms() - LONG_RETIRED_MS)).await?;
            let scratch = Path::from("scratch/slow-mover");
            stores
                .main()
                .store()
                .put(&scratch, bytes::Bytes::from_static(b"x").into())
                .await?;
            let slow = EntryId::new(wall_ms() - 8 * 24 * 3_600_000);
            stores.trash_bins()[0]
                .move_into(
                    &slow,
                    &Subject::RangeDb(103),
                    "test: node 8 days slow",
                    &[scratch],
                )
                .await?;
            let round = cairn_server::gc::purge_trash_once(
                &stores,
                &node.cluster,
                Duration::from_secs(7 * 24 * 3600),
            )
            .await
            .map_err(|e| anyhow!("{e}"))?;
            ensure!(
                round.purged.is_empty(),
                "an entry just written was really deleted by a slow node's id: {round:?}"
            );
            ensure!(
                stores.trash_bins()[0]
                    .entries()
                    .await?
                    .iter()
                    .any(|e| e.id == slow),
                "entry gone"
            );
            Ok(())
        },
    )
    .await;
    node.shutdown().await;
    r
}

/// **`cluster.conf` missing ⇒ the table-based delete round does not run on the in-RAM
/// table.**
///
/// 1. Range 105 retired long ago, referenced by nobody — D-d with `min_age` 0 may
///    delete it.
/// 2. Delete `cluster.conf` from outside (the node keeps running, the in-RAM table is
///    still there).
/// 3. One D-d round.
///
/// Must see: the round fails (it cannot prove the table) and Db 105 is intact —
/// AGENTS.md rule 5. Then give the table back for `verify`.
///
/// Before the fix: `reload` treated a missing table as "unchanged", the proof gate
/// compared against the in-RAM table ⇒ D-d moved Db 105 into the trash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rounds_delete_nothing_while_the_range_table_is_missing() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt};
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(Duration::from_secs(40), "missing table", async {
        fabricate_retired_range(&stores, 105, Some(wall_ms() - LONG_RETIRED_MS)).await?;
        let main = stores.main();
        let conf = Path::from("cluster.conf");
        let saved = main.store().get(&conf).await?.bytes().await?;
        main.store().delete(&conf).await?;
        let round = gc_once(&stores, &node).await;
        main.store().put(&conf, saved.into()).await?;
        ensure!(
            !matches!(round, Ok(n) if n > 0),
            "D-d deletes Dbs while the table is missing: {round:?}"
        );
        ensure!(
            stores
                .range_db_exists(cairn_store::PartitionId(105))
                .await?,
            "Db 105 gone"
        );
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

/// **D-d starves while a promised child has no manifest yet.** A child promised by an
/// intent stands only on its source and on what the source references (G7: a clone
/// copies the source's `external_dbs`), and the source is a live range whose manifest
/// has been read. A child without a manifest adds no reference.
///
/// Order:
/// 1. A two-range table, both with a Db. Range 40 retired, referenced by nobody.
/// 2. The D-d plan (`min_age` 0): range 40 is eligible.
/// 3. A split intent on range 0, two promised children, none with a Db yet.
///
/// Must see: the plan after (3) still finds range 40 eligible. Before the fix: a child
/// without a manifest ⇒ `unknown` ⇒ every retired Db kept for the whole operation —
/// with splits/merges back to back, D-d deletes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promised_child_without_manifest_does_not_block_gc() -> Result<()> {
    let bucket = common::bucket();
    bucket.allow(
        "stuck_intent",
        "the test leaves a split intent on the table, with no coordinator",
    );
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = slatedb::object_store::path::Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let cluster = cairn_store::ClusterConfig::open(&store, &root, 2).await?;
    // (1)
    for r in [0u32, 1] {
        cairn_store::PartitionWriter::open(
            cairn_store::LeadershipGrant::single_node(PartitionId(r)),
            stores.open_db(r).await?,
        )
        .await?
        .seal()
        .await?;
    }
    fabricate_retired_range(&stores, 40, Some(wall_ms())).await?;
    // (2)
    let plan = cairn_server::gc::plan(&stores, &cluster, Duration::ZERO, wall_ms()).await?;
    ensure!(
        plan.eligible.iter().any(|e| e.range == 40),
        "bad setup: range 40 must be eligible before any intent: {plan:?}"
    );
    // (3)
    let t = cluster.table();
    let zero = *t
        .range(PartitionId(0))
        .ok_or_else(|| anyhow!("no range 0: {t:?}"))?;
    let intent = cairn_store::RangeIntent::Splitting {
        at: zero.start + (zero.end - zero.start) / 2 + 1,
        left: PartitionId(t.next_range_id()),
        right: PartitionId(t.next_range_id() + 1),
    };
    let marked = cluster
        .update(t.version(), |t| {
            let ranges = t
                .ranges()
                .iter()
                .map(|e| {
                    if e.id == zero.id {
                        cairn_store::RangeEntry {
                            intent: Some(intent),
                            ..*e
                        }
                    } else {
                        *e
                    }
                })
                .collect();
            cairn_store::PartitionTable::from_entries(
                t.version() + 1,
                t.next_range_id() + 2,
                ranges,
            )
        })
        .await?;
    ensure!(
        matches!(marked, cairn_store::TableUpdate::Applied(_)),
        "could not write the intent: {marked:?}"
    );
    let plan = cairn_server::gc::plan(&stores, &cluster, Duration::ZERO, wall_ms()).await?;
    ensure!(
        plan.eligible.iter().any(|e| e.range == 40),
        "a promised child without a manifest blocks the whole D-d round: {plan:?}"
    );
    Ok(())
}

/// **The reap round deletes a barrier renewal.** `reap_expired_barriers` reads a
/// barrier, sees it expired, then DELETEs unconditionally (object_store has no
/// conditional DELETE): a holder renewing right when the barrier has just expired by
/// the D-d runner's clock (a skew of a few seconds is acceptable, `gc_barrier.rs`) has
/// its renewal fall into the read–delete window and get deleted.
///
/// Order:
/// 1. Barrier `just-expired` expired 1 s ago (a window a renewal can still hit);
///    barrier `long-expired` expired 2 hours ago.
/// 2. One D-d round.
///
/// Must see: `just-expired` is still there (only a barrier expired for longer than the
/// grace period is deleted — an expired barrier still blocks nothing), `long-expired`
/// is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barrier_reap_spares_a_barrier_that_just_expired() -> Result<()> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let bucket = common::bucket();
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> = bucket.clone();
    let root = Path::default();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        store.clone(),
        "",
        "memory://",
    ));
    let cluster = cairn_store::ClusterConfig::open(&store, &root, 1).await?;
    cairn_store::PartitionWriter::open(
        cairn_store::LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?
    .seal()
    .await?;
    // (1)
    let now = wall_ms();
    for (id, expires) in [
        ("just-expired", now - 1_000),
        ("long-expired", now - 2 * 3_600_000),
    ] {
        store
            .put(
                &Path::from(format!("gc-barriers/{id}")),
                PutPayload::from(format!(
                    "cairnflow-gc-barrier/1\nholder=t\nprotects=all\nexpires_ms={expires}\nreason=test\n"
                )),
            )
            .await?;
    }
    // (2)
    cairn_server::gc::run_once(&stores, &cluster, &cairn_server::gc::GcConfig::default()).await?;
    let exists = |id: &'static str| {
        let store = store.clone();
        async move {
            match store.head(&Path::from(format!("gc-barriers/{id}"))).await {
                Ok(_) => Ok(true),
                Err(slatedb::object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(anyhow!(e)),
            }
        }
    };
    ensure!(
        exists("just-expired").await?,
        "a just-expired barrier was deleted — a renewal falling into the read–delete window would be lost"
    );
    ensure!(
        !exists("long-expired").await?,
        "a barrier expired long ago must be deleted"
    );
    Ok(())
}

// ─────────────── D-d tolerates per-item failures

/// **One range whose read or delete fails must not drop the whole D-d round** (the
/// same shape as the scrubber bug fixed before).
///
/// 1. A one-range bucket (0, its Db has a manifest). Fabricate four retired ranges old
///    enough, 101–104.
/// 2. D-d runs on a store wrapping the same bucket, failing **forever**:
///    - GET `owners/range-103` (reading a candidate's tombstone — the plan step),
///    - DELETE of every object under `ranges/102/` (the delete step).
///
/// Must see: the round **finishes** (`Ok`), deletes exactly 101 and 104; the Dbs of 102
/// and 103 are intact (a read or delete failure ⇒ that range is not deleted, AGENTS.md
/// rule 5); 103 is blocked as `unreadable`, 102 is in `failed`; the counter
/// `cairnflow_gc_skipped_total` grows at both steps. Before the fix:
/// `Err` right in the plan (`?` on reading 103's tombstone), no range deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dd_round_survives_per_item_errors() -> Result<()> {
    use {
        cairn_server::gc::{stats, BlockedBy, GcConfig},
        cairn_store::{
            testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
            ClusterConfig, LeadershipGrant, ObjectStores, PartitionWriter, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
        std::sync::{atomic::Ordering::Relaxed, Arc},
    };
    let bucket = Arc::new(InMemory::new());
    let plain = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let shared: Arc<dyn ObjectStore> = bucket.clone();
    let cluster = ClusterConfig::open(&shared, &Path::from(""), 1).await?;
    let w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        plain.open_db(0).await?,
    )
    .await?;
    w.seal().await?;
    let old = wall_ms() - LONG_RETIRED_MS;
    for r in 101..=104 {
        fabricate_retired_range(&plain, r, Some(old)).await?;
    }
    let faulty = FaultyStore::wrap(bucket.clone());
    faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("owners/range-103".into()),
        Action::Fail { times: None },
    ));
    faulty.add_rule(Rule::new(
        OpSet::DELETE,
        PathMatch::Contains("ranges/102/".into()),
        Action::Fail { times: None },
    ));
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let before = (
        stats::SKIPPED_PLAN.load(Relaxed),
        stats::SKIPPED_DELETE.load(Relaxed),
    );
    let round = common::within(Duration::from_secs(60), "faulty D-d round", async {
        Ok(cairn_server::gc::run_once(&stores, &cluster, &GcConfig::default()).await)
    })
    .await?;
    ensure!(faulty.injected().0 >= 1, "fault rule did not fire");
    let round = round.map_err(|e| anyhow!("one broken range failed the whole D-d round: {e}"))?;
    let mut deleted = round.deleted.clone();
    deleted.sort_unstable();
    ensure!(deleted == vec![101, 104], "{round:?}");
    ensure!(
        round
            .plan
            .blocked
            .iter()
            .any(|b| b.range == 103 && b.by == BlockedBy::Unreadable),
        "{round:?}"
    );
    ensure!(
        round.failed.iter().map(|b| b.range).collect::<Vec<_>>() == vec![102],
        "{round:?}"
    );
    // The counter is a process static: ≥, not ==, since other tests may run in parallel.
    ensure!(
        stats::SKIPPED_PLAN.load(Relaxed) > before.0,
        "the plan step did not count"
    );
    ensure!(
        stats::SKIPPED_DELETE.load(Relaxed) > before.1,
        "the delete step did not count"
    );
    for (r, want) in [
        (0, true),
        (101, false),
        (102, true),
        (103, true),
        (104, false),
    ] {
        ensure!(
            plain.range_db_exists(PartitionId(r)).await? == want,
            "range {r}: want present={want}"
        );
    }
    Ok(())
}

// ─────────────────────── clock skew gate

/// The test thread's wall clock is skewed by `ahead` from real time (installed through
/// `cairn_store::entropy::install`; applies only to futures running on the test thread).
struct SkewedClock(Duration);

impl cairn_store::entropy::Source for SkewedClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now() + self.0
    }
    fn process_id(&self) -> u32 {
        std::process::id()
    }
    fn fill(&self, buf: &mut [u8]) {
        // Different enough between calls; no real randomness needed.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0x51);
        for chunk in buf.chunks_mut(8) {
            let n = NEXT.fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed);
            chunk.copy_from_slice(&n.to_le_bytes()[..chunk.len()]);
        }
    }
}

/// **A node running GC has a clock ahead of the store** (G9, S3-4).
///
/// 1. Range 100 just retired (its tombstone carries `retired_ms` = real time), range 101
///    just retired with a tombstone without `retired_ms` (age from `last_modified`); a
///    trash entry just created.
/// 2. The wall clock of the node running the rounds jumps an hour ahead (broken NTP, a
///    VM restored from a snapshot).
/// 3. A D-d round (default `min_age` 30 minutes) and a trash purge round (`keep` 30
///    minutes) on that node.
///
/// Must see: nothing deleted, the D-d round and the proxy tombstone round record why
/// they skipped their turn, `cairnflow_gc_clock_skew_ms` measures about an hour. Before
/// the fix: a tombstone looked an hour old ⇒ the Db was deleted right after retiring,
/// the trash entry purged before its time — every GC age was computed from one node's
/// clock, and nobody checked it against the store. A clock an hour behind is blocked
/// too (a tombstone written then carries an old timestamp); with a correct clock the
/// gate is open: D-d with `min_age` 0 deletes both ranges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_rounds_delete_nothing_when_the_node_clock_runs_ahead_of_the_store() -> Result<()> {
    use {
        cairn_store::trash::{EntryId, Subject},
        slatedb::object_store::{path::Path, ObjectStoreExt},
    };
    let cfg = solo_cluster();
    let stores = cfg.stores.clone();
    let node = common::start_led(cfg).await?;
    let r = common::within(Duration::from_secs(40), "clock skew gate", async {
        fabricate_retired_range(&stores, 100, Some(wall_ms())).await?;
        fabricate_retired_range(&stores, 101, None).await?;
        let scratch = Path::from("scratch/just-dropped");
        stores
            .main()
            .store()
            .put(&scratch, bytes::Bytes::from_static(b"x").into())
            .await?;
        let fresh = EntryId::new(wall_ms());
        stores.trash_bins()[0]
            .move_into(&fresh, &Subject::RangeDb(100), "test: fresh", &[scratch])
            .await?;

        // A candidate for the proxy tombstone round (id not allocated — the round only needs work).
        put_raw(
            &stores,
            "owners/range-9",
            "cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nwal_kind=shared\n",
        )
        .await?;
        let ahead =
            cairn_store::entropy::install(std::rc::Rc::new(SkewedClock(Duration::from_secs(3600))));
        let round = cairn_server::gc::run_once(
            &stores,
            &node.cluster,
            &cairn_server::gc::GcConfig::default(),
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.deleted.is_empty(),
            "D-d deleted {:?} by a skewed clock",
            round.deleted
        );
        ensure!(
            round.skipped.is_some(),
            "the D-d round did not record why it skipped: {round:?}"
        );
        let purge = cairn_server::gc::purge_trash_once(
            &stores,
            &node.cluster,
            Duration::from_secs(30 * 60),
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            purge.purged.is_empty(),
            "trash purged by a skewed clock: {purge:?}"
        );
        for r in [100, 101] {
            ensure!(
                stores.range_db_exists(PartitionId(r)).await?,
                "Db of range {r} deleted"
            );
        }
        ensure!(
            stores.trash_bins()[0].entries().await?.len() == 1,
            "the trash entry just created was purged"
        );
        let orphans = cairn_server::gc::OrphanRetirer::default()
            .run_once(&stores, &node.cluster, &node.session)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            orphans.skipped.is_some() && orphans.kept.is_empty(),
            "{orphans:?}"
        );
        // The gate's measurement (same function; the metric is global, parallel tests overwrite it).
        let skew = cairn_server::gc::measure_clock_skew(&stores)
            .await
            .map_err(|e| anyhow!("{e}"))?
            .as_millis();
        ensure!(
            (3_590_000..3_610_000).contains(&skew),
            "measured skew {skew} ms"
        );
        drop(ahead);

        {
            struct Behind;
            impl cairn_store::entropy::Source for Behind {
                fn now(&self) -> std::time::SystemTime {
                    std::time::SystemTime::now() - Duration::from_secs(3600)
                }
                fn process_id(&self) -> u32 {
                    std::process::id()
                }
                fn fill(&self, buf: &mut [u8]) {
                    SkewedClock(Duration::ZERO).fill(buf)
                }
            }
            let _behind = cairn_store::entropy::install(std::rc::Rc::new(Behind));
            let orphans = cairn_server::gc::OrphanRetirer::default()
                .run_once(&stores, &node.cluster, &node.session)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            ensure!(
                orphans.skipped.is_some(),
                "a clock behind is not blocked: {orphans:?}"
            );
        }

        let orphans = cairn_server::gc::OrphanRetirer::default()
            .run_once(&stores, &node.cluster, &node.session)
            .await
            .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            orphans.skipped.is_none(),
            "a correct clock is blocked: {orphans:?}"
        );
        let round = cairn_server::gc::run_once(
            &stores,
            &node.cluster,
            &cairn_server::gc::GcConfig {
                min_age: Duration::ZERO,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;
        ensure!(
            round.skipped.is_none(),
            "a correct clock is blocked: {round:?}"
        );
        let mut deleted = round.deleted.clone();
        deleted.sort();
        ensure!(deleted == [100, 101], "{round:?}");
        Ok(())
    })
    .await;
    node.shutdown().await;
    r
}

// ─────────────────────── background rounds do not "sleep first"

/// **A node that just started runs its first scrub round and its first trash purge
/// round within a few seconds**, like the D-d round, instead of waiting a full tick
/// (`--scrub-every-s` 600 s; trash purge `keep / 8`).
///
/// 1. Before the node comes up: a trash entry `range-db 102` (a range not in the table)
///    2 hours old by its id, written `TRASH_KEEP` + 0.5 s before the node comes up (old
///    enough by the store clock too).
/// 2. One node, one range; scrubber at the default tick, `trash_keep` = `TRASH_KEEP`.
///
/// Must see: within `RUNNER_CHECK_EVERY` + 10 s from when the node leads the range, the
/// scrubber has read the bucket (source `scrub`) and the trash entry has really been
/// deleted. Before the fix: the first scrub round after 600 s. The trash half is
/// weaker than at first (`trash_keep` 1 hour, a 450 s purge tick): an entry is old
/// enough by the store only once it has been there a full `keep`, so the test's `keep`
/// is short and so is the purge tick (`keep / 8`, clamped to 1 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_scrub_and_trash_rounds_run_right_after_start() -> Result<()> {
    use {
        cairn_store::trash::{EntryId, Subject},
        slatedb::object_store::{path::Path, ObjectStoreExt},
    };
    let bucket = common::bucket();
    let stores = cairn_store::ObjectStores::single(cairn_store::StoreLocation::from_store(
        bucket.clone(),
        "",
        "memory://",
    ));
    let scratch = Path::from("scratch/retired");
    stores
        .main()
        .store()
        .put(&scratch, bytes::Bytes::from_static(b"x").into())
        .await?;
    let old = EntryId::new(wall_ms() - 2 * 60 * 60 * 1000);
    stores.trash_bins()[0]
        .move_into(&old, &Subject::RangeDb(102), "test: old entry", &[scratch])
        .await?;
    const TRASH_KEEP: Duration = Duration::from_secs(3);
    tokio::time::sleep(TRASH_KEEP + Duration::from_millis(500)).await;
    let n =
        common::start_cluster_node_tweaked(&bucket, "fresh", 1, Duration::from_secs(2), |cfg| {
            cfg.scrub_every = Some(cairn_server::scrub::DEFAULT_EVERY);
            cfg.trash_keep = Some(TRASH_KEEP);
        })
        .await?;
    let r = async {
        wait_probe(
            &[&n],
            Duration::from_secs(20),
            "node is leader of range 0",
            |p| single_leaders(p).is_some(),
        )
        .await?;
        let t0 = std::time::Instant::now();
        let bound = cairn_server::gc::RUNNER_CHECK_EVERY + Duration::from_secs(10);
        loop {
            let scrubbed = n
                .store
                .counts_by_source()
                .into_iter()
                .any(|(src, _, _, c)| src == "scrub" && c > 0);
            let purged = stores.trash_bins()[0].entries().await?.is_empty();
            if scrubbed && purged {
                return Ok(());
            }
            ensure!(
                t0.elapsed() < bound,
                "{bound:?} after leading: scrubber ran {scrubbed}, trash purged {purged}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    .await;
    n.node.shutdown().await;
    r
}

// ─────────────── D-d reads references once per table version

/// **D-d reads the live ranges' manifests once per table version within a round.**
/// Before the fix, the proof of **each** candidate (`DeleteProof::retired_db`) re-read
/// the manifest of **every** live range: a round deleting `N` Dbs on a cluster of `L`
/// ranges cost `L × (1 + N)` manifest reads.
///
/// 1. A bucket with two live ranges (0, 1; their Dbs have manifests). Fabricate four
///    retired ranges old enough, 101–104, nobody standing on them.
/// 2. One D-d round on a store wrapping the same bucket, recording every GET.
///
/// Must see: the round deletes all four; each live range's manifest is GET **once**
/// (the table does not change during the round: one version, one reference set for the
/// plan and the four proofs). The second barrier (a checkpoint on the retired Db
/// itself) is still read per candidate — not counted here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dd_reads_live_manifests_once_per_table_version() -> Result<()> {
    use {
        cairn_server::gc::GcConfig,
        cairn_store::{
            testkit::FaultyStore, ClusterConfig, LeadershipGrant, ObjectStores, PartitionWriter,
            StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore},
        std::sync::Arc,
    };
    let bucket = Arc::new(InMemory::new());
    let plain = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let shared: Arc<dyn ObjectStore> = bucket.clone();
    let cluster = ClusterConfig::open(&shared, &Path::from(""), 2).await?;
    for r in 0..2 {
        let w = PartitionWriter::open(
            LeadershipGrant::single_node(PartitionId(r)),
            plain.open_db(r).await?,
        )
        .await?;
        w.seal().await?;
    }
    let old = wall_ms() - LONG_RETIRED_MS;
    for r in 101..=104 {
        fabricate_retired_range(&plain, r, Some(old)).await?;
    }
    let faulty = FaultyStore::wrap(bucket.clone());
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    faulty.record_reads();
    let round = common::within(Duration::from_secs(60), "one D-d round", async {
        cairn_server::gc::run_once(&stores, &cluster, &GcConfig::default())
            .await
            .map_err(|e| anyhow!("{e}"))
    })
    .await?;
    let reads = faulty.take_reads();
    let mut deleted = round.deleted.clone();
    deleted.sort_unstable();
    ensure!(deleted == vec![101, 102, 103, 104], "{round:?}");
    for live in 0..2 {
        let dir = format!("ranges/{live}/manifest/");
        let n = reads
            .iter()
            .filter(|(p, _)| p.as_ref().contains(&dir))
            .count();
        println!("D-d: {n} manifest GETs of live range {live} in one round deleting 4 Dbs");
        ensure!(
            n == 1,
            "manifest of live range {live} GET {n} times in one round (one table version) — must be 1"
        );
    }
    Ok(())
}

// ─────────────────── trash reading rounds do not scale with the trash size

/// A bucket with one live range and `n` entries **not held in place** in the trash
/// (like Dbs deleted by D-d, dead WAL logs, operation logs: what the trash keeps for 7
/// days by split history), on a request-counting store.
async fn trash_of(
    n: u32,
) -> Result<(
    std::sync::Arc<cairn_store::testkit::FaultyStore>,
    cairn_store::ObjectStores,
    cairn_store::ClusterConfig,
)> {
    use {
        cairn_store::{
            testkit::FaultyStore,
            trash::{EntryId, Subject, Trash},
            ClusterConfig, ObjectStores, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
        std::sync::Arc,
    };
    let bucket = Arc::new(InMemory::new());
    let shared: Arc<dyn ObjectStore> = bucket.clone();
    let cluster = ClusterConfig::open(&shared, &Path::from(""), 1).await?;
    let bin = Trash::new(Arc::clone(&shared), &Path::from(""));
    let now = wall_ms();
    for k in 0..n {
        let p = Path::from(format!("ops/{}", 1000 + k));
        shared.put(&p, b"x".to_vec().into()).await?;
        bin.move_into(
            &EntryId::new(now + u64::from(k)),
            &Subject::Op(1000 + k),
            "W5-G",
            &[p],
        )
        .await?;
    }
    let faulty = FaultyStore::wrap(bucket);
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    Ok((faulty, stores, cluster))
}

/// Requests of the **second** run of each trash reading round on a bucket with `n`
/// entries: (tenant purge, artifact sweep, value sweep).
async fn trash_loop_requests(n: u32) -> Result<(u64, u64, u64)> {
    use {
        cairn_server::partitions::Partitions,
        cairn_store::{artifact::ArtifactStore, trash::Trash, value::ValueStore},
        std::sync::Arc,
    };
    let (faulty, stores, cluster) = trash_of(n).await?;
    let main = stores.main();
    let handles = Partitions::default();
    let purger = cairn_server::tenant_purge::Purger {
        stores: stores.clone(),
        cluster: cluster.clone(),
        partitions: handles.clone(),
        routing: cairn_server::routing::RoutingTable::new(),
        secret: None,
        client: reqwest::Client::new(),
        settings: cairn_server::tenants::TenantSettings::new(Some((
            Arc::clone(main.store()),
            main.prefix(),
        ))),
        keys: None,
        rules: None,
        keep: cairn_store::tenant_purge::KEEP,
        last: Arc::default(),
        compactions: Arc::default(),
        watches: Arc::default(),
        lots_per_range: cairn_server::tenant_purge::LOTS_PER_RANGE,
        sweeps: Arc::default(),
        checks: Arc::default(),
        bases: Arc::default(),
    };
    let second = |before: &cairn_store::testkit::StoreCounts| faulty.counts().since(before).total();
    purger.run_once().await.map_err(|e| anyhow!("{e}"))?;
    let before = faulty.counts();
    purger.run_once().await.map_err(|e| anyhow!("{e}"))?;
    let purge = second(&before);

    let trash = Trash::new(Arc::clone(main.store()), main.prefix());
    let artifacts = ArtifactStore::new(Arc::clone(main.store()), main.prefix());
    let sweep = cairn_server::artifact_sweep::Sweep {
        artifacts: &artifacts,
        trash: &trash,
        cluster: &cluster,
        handles: &handles,
        min_age: Duration::from_secs(3600),
        keep: Duration::from_secs(3600),
        now_ms: wall_ms(),
    };
    sweep.once(&mut None).await.map_err(|e| anyhow!("{e}"))?;
    let before = faulty.counts();
    sweep.once(&mut None).await.map_err(|e| anyhow!("{e}"))?;
    let artifact = second(&before);

    let values = ValueStore::new(Arc::clone(main.store()), main.prefix());
    let sweep = cairn_server::value_sweep::Sweep {
        values: &values,
        trash: &trash,
        cluster: &cluster,
        handles: &handles,
        min_age: Duration::from_secs(3600),
        keep: Duration::from_secs(3600),
        now_ms: wall_ms(),
    };
    sweep.once(&mut None).await.map_err(|e| anyhow!("{e}"))?;
    let before = faulty.counts();
    sweep.once(&mut None).await.map_err(|e| anyhow!("{e}"))?;
    let value = second(&before);
    Ok((purge, artifact, value))
}

/// Requests of the **second** call of `GET /admin/tenants/acme/purge`
/// (`Purger::status`) on a bucket with `n` trash entries not held in place.
async fn purge_status_requests(n: u32) -> Result<u64> {
    use {cairn_server::partitions::Partitions, std::sync::Arc};
    let (faulty, stores, cluster) = trash_of(n).await?;
    let main = stores.main();
    let purger = cairn_server::tenant_purge::Purger {
        stores: stores.clone(),
        cluster,
        partitions: Partitions::default(),
        routing: cairn_server::routing::RoutingTable::new(),
        secret: None,
        client: reqwest::Client::new(),
        settings: cairn_server::tenants::TenantSettings::new(Some((
            Arc::clone(main.store()),
            main.prefix(),
        ))),
        keys: None,
        rules: None,
        keep: cairn_store::tenant_purge::KEEP,
        last: Arc::default(),
        compactions: Arc::default(),
        watches: Arc::default(),
        lots_per_range: cairn_server::tenant_purge::LOTS_PER_RANGE,
        sweeps: Arc::default(),
        checks: Arc::default(),
        bases: Arc::default(),
    };
    let acme = cairn_store::TenantId::new("acme")?;
    purger.status(&acme).await.map_err(|e| anyhow!("{e}"))?;
    let before = faulty.counts();
    purger.status(&acme).await.map_err(|e| anyhow!("{e}"))?;
    Ok(faulty.counts().since(&before).total())
}

/// **`GET /admin/tenants/{t}/purge` does not scale with the trash size.** Seen in a
/// 60-minute chaos run: purging tenant `chaos-purge` reached `purged` after ~10 minutes
/// (the log on the bucket), but every later probe of the harness went over its 15 s
/// cap — the trash had 7 671 entries, each probe read the `entry` of **every** entry
/// (`Trash::entries`) under a slow S3 profile — so the harness kept the first answer
/// (`deleting`) and reported it stuck.
///
/// 1. Two buckets, with trashes of 20 and 80 entries not held in place.
/// 2. `Purger::status` twice; count the requests of the second call.
///
/// Must see: equal at 20 and 80 entries — an entry already classified is not GET again
/// (the same `Seen` memory as the purge round). Before the fix: a probe re-GETs every
/// `entry`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn purge_status_does_not_reread_every_trash_entry() -> Result<()> {
    let small = purge_status_requests(20).await?;
    let big = purge_status_requests(80).await?;
    println!("GET purge: requests of the second probe — 20 entries {small}, 80 entries {big}");
    ensure!(
        small == big,
        "the probe scales with the trash size: 20 entries {small}, 80 entries {big}"
    );
    Ok(())
}

/// **Three trash reading rounds do not scale with the trash size.** Before the fix,
/// every tenant purge round (300 s, even when no tenant is `deleting`), every artifact
/// sweep and every value sweep GET **every** trash entry to find its few entries held
/// in place: measured ≈ 8 100 GETs per round, 89 377 requests/h for the tenant purge
/// alone (an idle-cost measurement).
///
/// 1. Two buckets, one range each, trashes of 20 and 80 entries not held in place;
///    no tenant, artifact or value.
/// 2. Each round runs twice on the same runner; count the requests of the second run
///    (`FaultyStore::counts`).
///
/// Must see: the requests of the second run are equal at 20 and 80 entries, and under a
/// small cap — an entry already classified is not GET again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trash_loops_do_not_reread_every_entry_each_round() -> Result<()> {
    let small = trash_loop_requests(20).await?;
    let big = trash_loop_requests(80).await?;
    println!(
        "second-run requests (tenant purge, artifact, value): 20 entries {small:?}, 80 entries {big:?}"
    );
    ensure!(
        small == big,
        "requests per round grow with the trash size: 20 entries {small:?}, 80 entries {big:?}"
    );
    for (what, n) in [
        ("purge tenant", big.0),
        ("artifact", big.1),
        ("value", big.2),
    ] {
        ensure!(n <= 8, "round {what}: {n} requests at 80 entries, max 8");
    }
    Ok(())
}

// ─────────────────────────── D-d does not scale with the number of swept tombstones

/// Requests of the **second** D-d round on a bucket with one live range, `n` tombstones
/// whose Db was deleted for good before, and three retired ranges old enough that still
/// have a Db (the first round deletes them): `(total, GET owners/)`.
async fn dd_second_round(n: u32) -> Result<(u64, u64)> {
    use {
        cairn_server::gc::GcConfig,
        cairn_store::{
            testkit::{FaultyStore, Op, PathClass},
            ClusterConfig, LeadershipGrant, ObjectStores, PartitionWriter, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
        std::sync::Arc,
    };
    let bucket = Arc::new(InMemory::new());
    let plain = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let shared: Arc<dyn ObjectStore> = bucket.clone();
    let cluster = ClusterConfig::open(&shared, &Path::from(""), 1).await?;
    let w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        plain.open_db(0).await?,
    )
    .await?;
    w.seal().await?;
    let old = wall_ms() - LONG_RETIRED_MS;
    for r in 1000..1000 + n {
        let owner = format!(
            "cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\nwal_kind=slatedb\nretired_ms={old}\n"
        );
        shared
            .put(
                &Path::from(format!("owners/range-{r}")),
                owner.into_bytes().into(),
            )
            .await?;
    }
    for r in 101..=103 {
        fabricate_retired_range(&plain, r, Some(old)).await?;
    }
    let faulty = FaultyStore::wrap(bucket.clone());
    let stores = ObjectStores::single(StoreLocation::from_store(faulty.clone(), "", "memory://"));
    let gc = GcConfig::default();
    let first = cairn_server::gc::run_once(&stores, &cluster, &gc)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let mut deleted = first.deleted.clone();
    deleted.sort_unstable();
    ensure!(deleted == vec![101, 102, 103], "{first:?}");
    let before = faulty.counts();
    let second = cairn_server::gc::run_once(&stores, &cluster, &gc)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    ensure!(second.deleted.is_empty(), "{second:?}");
    let d = faulty.counts().since(&before);
    Ok((d.total(), d.get(Op::Get, PathClass::Lease)))
}

/// **D-d does not scale with the number of tombstones whose Db is deleted for good.**
/// Before the fix, every D-d round GET the owner record of **every** tombstone outside
/// the table and LISTed its Db's manifests, even when the Db was deleted long ago: ≈ 2
/// requests per tombstone per round, and tombstones never decrease (the fence
/// `retired_range_is_never_reacquired`). Measured: 153 GETs of `owners/` per round
/// with 150 tombstones.
///
/// 1. Two buckets, one live range each, 10 and 40 tombstones whose Db was deleted, plus
///    three retired ranges old enough that still have a Db.
/// 2. The first D-d round deletes those three Dbs. Count the requests of the second round.
///
/// Must see: the second round GETs no owner record, and the total requests are equal at
/// 10 and 40 tombstones — including the three tombstones the first round just swept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dd_round_cost_does_not_grow_with_swept_tombstones() -> Result<()> {
    let small = dd_second_round(10).await?;
    let big = dd_second_round(40).await?;
    println!("second D-d round (total, GET owners/): {small:?} at 10 tombstones, {big:?} at 40");
    ensure!(
        big.1 == 0,
        "the second round still GETs {} owner records with 40+3 swept tombstones",
        big.1
    );
    ensure!(
        small.0 == big.0,
        "D-d round requests grow with the tombstone count: 10 {small:?}, 40 {big:?}"
    );
    Ok(())
}

/// **The swept-tombstone log does not hide a Db for longer than one re-read of every
/// tombstone** (AGENTS.md rule 5: the log may only make D-d *keep* too much, and is
/// bounded).
///
/// 1. A bucket with one live range, one tombstone (1000) whose Db was deleted. The
///    first D-d round (log missing ⇒ read every tombstone) writes 1000 into the log.
/// 2. The Db of 1000 reappears (as with a manual `cairn-ctl trash restore`).
/// 3. The next D-d round: the log is fresh ⇒ it does not look at 1000, does not delete.
///    `verify` (h) still sees it (`ranges/1000` has objects ⇒ not skipped).
/// 4. Push the log's `full-pass-ms=` into the past (beyond `FULL_PASS_EVERY`).
///
/// Must see: the round of step 3 keeps the Db; `verify` reports `retired_unreferenced`
/// for 1000; the round after step 4 deletes it and writes 1000 into the log again with
/// a new timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swept_ledger_never_hides_a_db_past_a_full_pass() -> Result<()> {
    use {
        cairn_server::gc::GcConfig,
        cairn_store::{
            gc_swept, ClusterConfig, LeadershipGrant, ObjectStores, PartitionWriter, StoreLocation,
        },
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
        std::sync::Arc,
    };
    let bucket = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let shared: Arc<dyn ObjectStore> = bucket.clone();
    let root = Path::from("");
    let cluster = ClusterConfig::open(&shared, &root, 1).await?;
    let w = PartitionWriter::open(
        LeadershipGrant::single_node(PartitionId(0)),
        stores.open_db(0).await?,
    )
    .await?;
    w.seal().await?;
    let old = wall_ms() - LONG_RETIRED_MS;
    let owner = format!(
        "cairnflow-owner/1\nterm=1\nsession=dead/1\nreleased=1\nretired=1\nwal_kind=slatedb\nretired_ms={old}\n"
    );
    shared
        .put(&Path::from("owners/range-1000"), owner.into_bytes().into())
        .await?;
    let gc = GcConfig::default();
    let dd = || async {
        cairn_server::gc::run_once(&stores, &cluster, &gc)
            .await
            .map_err(|e| anyhow!("{e}"))
    };
    let first = dd().await?;
    ensure!(first.deleted.is_empty(), "{first:?}");
    let swept = gc_swept::read(&shared, &root).await;
    ensure!(
        swept.contains(1000),
        "the first round must write 1000 into the log: {swept:?}"
    );

    fabricate_retired_range(&stores, 1000, Some(old)).await?;
    let blind = dd().await?;
    ensure!(
        blind.deleted.is_empty() && blind.plan.skipped_swept == 1,
        "the log is fresh: the round does not look at 1000 — {blind:?}"
    );
    ensure!(
        stores.range_db_exists(PartitionId(1000)).await?,
        "the reappeared Db must still be there"
    );
    let rep = cairn_server::ctl::Ctl::new(stores.clone())
        .verify(cairn_server::ctl::VerifyOptions {
            open_dbs: false,
            retired_after: Some(Duration::ZERO),
            ..Default::default()
        })
        .await?;
    ensure!(
        rep.findings
            .iter()
            .any(|f| f.check == "retired_unreferenced" && f.range == Some(1000)),
        "verify (h) must see the reappeared Db under a logged id: {:?}",
        rep.findings
    );

    let p = Path::from(gc_swept::OBJECT);
    let raw = String::from_utf8(shared.get(&p).await?.bytes().await?.to_vec())?;
    let aged: String = raw
        .lines()
        .map(|l| {
            if l.starts_with("full-pass-ms=") {
                "full-pass-ms=1\n".to_string()
            } else {
                format!("{l}\n")
            }
        })
        .collect();
    shared.put(&p, aged.into_bytes().into()).await?;
    let full = dd().await?;
    ensure!(
        full.deleted == vec![1000],
        "the re-read of every tombstone must delete the reappeared Db: {full:?}"
    );
    let swept = gc_swept::read(&shared, &root).await;
    ensure!(
        swept.contains(1000) && !swept.full_pass_due(wall_ms()),
        "log rebuilt: {swept:?}"
    );
    Ok(())
}
