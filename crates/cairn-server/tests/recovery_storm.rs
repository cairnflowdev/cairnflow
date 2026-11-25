// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Becoming leader must not re-deliver an SDK turn to an invocation that **nothing
//! wakes**.
//!
//! `recover` (on every leadership change — split, merge, failover) used to re-deliver
//! a turn to every `suspended` invocation, even one sleeping for an hour. Each one
//! cost a turn (the SDK receives the journal again and suspends again), and those turns
//! queued ahead of new calls: the first call to a child range waited 10 s on `file://`,
//! 70 s on SeaweedFS, and the storm dragged on for up to 15 minutes. An invocation waiting
//! for a future event must stay idle until the normal path (timer, outbox, inbound, signal)
//! wakes it.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_server::node::{self, NodeConfig, NodeMode, RunningNode},
    cairn_store::{ObjectStores, StoreLocation},
    common::{
        bench,
        fake_sdk::{FakeSdk, Step},
    },
    slatedb::object_store::memory::InMemory,
    std::{sync::Arc, time::Duration},
};

async fn start_on(bucket: &Arc<InMemory>) -> Result<RunningNode> {
    let stores = ObjectStores::single(StoreLocation::from_store(bucket.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))
}

async fn send_ids(url: &str, service: &str, handler: &str, n: usize) -> Result<Vec<String>> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let (code, body) = common::send(url, service, handler, None, "{}").await?;
        ensure!(code == 200 || code == 202, "{code} {body}");
        ids.push(body["id"].as_str().unwrap_or_default().to_string());
    }
    Ok(ids)
}

/// N invocations sleeping for an hour, a leader change (the node stops, another node opens the
/// same store): the first new call is served **without** any turn of the N others running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sleeping_invocations_are_not_redriven_on_leader_change() -> Result<()> {
    const N: usize = 30;
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service(
            "Sleeper",
            "nap",
            vec![
                Step::Sleep(3_600_000),
                Step::Output(Bytes::from_static(b"\"awake\"")),
            ],
        )
        .service(
            "Echo",
            "now",
            vec![Step::Output(Bytes::from_static(b"\"ok\""))],
        )
        .spawn()
        .await?;

    let a = start_on(&bucket).await?;
    let setup = common::within(Duration::from_secs(30), "N invocations sleep", async {
        common::register(&a.url(), &sdk.url).await?;
        let ids = send_ids(&a.url(), "Sleeper", "nap", N).await?;
        for id in &ids {
            common::wait_status(&a.url(), id, "suspended").await?;
        }
        Ok(())
    })
    .await;
    a.shutdown().await;
    setup?;
    let before = sdk.starts_of("Sleeper", "nap").len();
    ensure!(
        before == N,
        "each invocation runs exactly one turn before sleeping: {before}"
    );

    let b = start_on(&bucket).await?;
    let served = common::within(Duration::from_secs(30), "first new call", async {
        let (code, body) = common::invoke(&b.url(), "Echo", "now", None, "{}").await?;
        ensure!(code == 200, "{code} {body}");
        Ok(())
    })
    .await;
    // Give the recovery path (if it still means to run anything) time to run.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let redriven = sdk.starts_of("Sleeper", "nap").len() - before;
    b.shutdown().await;
    served?;
    ensure!(
        redriven == 0,
        "becoming leader re-delivered {redriven}/{N} turns to invocations sleeping for an hour — nothing wakes them"
    );
    Ok(())
}

/// The other side of the same contract: an invocation waiting on a **near** timer is still
/// woken across a leader change — by the timer, not by `recover`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sleeping_invocation_still_wakes_after_leader_change() -> Result<()> {
    let bucket = common::bucket();
    let sdk = FakeSdk::builder()
        .service(
            "Sleeper",
            "short",
            vec![
                Step::Sleep(1_500),
                Step::Output(Bytes::from_static(b"\"awake\"")),
            ],
        )
        .spawn()
        .await?;

    let a = start_on(&bucket).await?;
    let id = common::within(Duration::from_secs(20), "invocation sleeps", async {
        common::register(&a.url(), &sdk.url).await?;
        let id = send_ids(&a.url(), "Sleeper", "short", 1).await?.remove(0);
        common::wait_status(&a.url(), &id, "suspended").await?;
        Ok(id)
    })
    .await;
    a.shutdown().await;
    let id = id?;

    let b = start_on(&bucket).await?;
    let done = common::within(
        Duration::from_secs(20),
        "the timer wakes it after the leader change",
        async { common::wait_status(&b.url(), &id, "completed").await },
    )
    .await;
    b.shutdown().await;
    done?;
    Ok(())
}

/// `recover` scans the status index **in chunks**:
/// its longest engine hold does not grow with the number of completed invocations.
///
/// Order (one range, an `EngineHandle` on a local directory, like `lifecycle.rs`):
/// 1. Engine stopped: write `H` `completed` status rows directly (with their
///    `Invocations` index, `Mutations::set_invocation_status`) and create `K`
///    `running` invocations spread evenly among them (`Engine::create`, no turn has
///    run yet) — like an old leader that died with a long history and some unfinished work.
/// 2. A new leader opens on the same directory, the deployment registry has the fake SDK: the
///    term's `recover` scans the index and delivers `K` turns; turns interleave between chunks.
///
/// Must see: every `running` invocation is delivered **exactly once** (the SDK receives
/// exactly `K` `Start`s, one per id) and completes; `recover` takes the engine at least
/// `H / 1024` times (once per chunk — not holding the engine across the whole history); the
/// longest hold of `holder="recover"` is under the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_scans_in_chunks_whatever_the_history() -> Result<()> {
    const H: usize = 50_000;
    const K: usize = 16;
    let got = recover_scan::run("recover-chunks", H, K).await?;
    eprintln!("recover_scans_in_chunks_whatever_the_history: {got:?}");
    let min_holds = (H / recover_scan::CHUNK) as u64;
    ensure!(
        got.recover_holds >= min_holds,
        "recover took the engine {} times for {H} index rows — must be ≥ {min_holds} (once per chunk of {})",
        got.recover_holds,
        recover_scan::CHUNK
    );
    ensure!(
        got.recover_max_ms <= recover_scan::HOLD_BOUND_MS,
        "recover's longest engine hold {:.1} ms > limit {} ms",
        got.recover_max_ms,
        recover_scan::HOLD_BOUND_MS
    );
    Ok(())
}

/// **Rebuilding the VO locks in chunks, serving commands meanwhile** (FL-8; the goal
/// is that no background hold grows with history).
///
/// Order:
/// 1. A range with `H` completed invocations — `Invocations` index rows and, for each
///    one, a queue entry already popped (`push_vqueue` then `pop_vqueue`: a tombstone
///    in the queue index, as every real invocation leaves) — and `K`
///    `suspended` invocations holding the VO locks of `K` different objects.
/// 2. A new leader opens on the same directory (the previous writer's durable mark matches ⇒
///    the queue `seq` counter is available). A read command is sent right as the rebuild starts.
///
/// Must see: all `K` locks are rebuilt; `restore` takes the engine at least `H / 1024`
/// times (once per chunk — not holding the engine across the whole history); the longest
/// hold of `holder="restore"` is under the limit; the first read command is answered **before**
/// the rebuild finishes (the `restore` hold count at that point is below the final one). The old code:
/// one hold walking every tombstone, before the first command.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_rebuilds_locks_in_chunks_while_serving() -> Result<()> {
    const H: usize = 50_000;
    const K: usize = 8;
    let got = recover_scan::restore_run("restore-chunks", H, K).await?;
    eprintln!("restore_rebuilds_locks_in_chunks_while_serving: {got:?}");
    ensure!(got.locks == K, "rebuilt {} VO locks, want {K}", got.locks);
    let min_holds = (H / recover_scan::CHUNK) as u64;
    ensure!(
        got.holds >= min_holds,
        "restore took the engine {} times for {H} index rows — must be ≥ {min_holds} (once per chunk of {})",
        got.holds,
        recover_scan::CHUNK
    );
    ensure!(
        got.max_ms <= recover_scan::HOLD_BOUND_MS,
        "restore's longest engine hold {:.1} ms > limit {} ms",
        got.max_ms,
        recover_scan::HOLD_BOUND_MS
    );
    ensure!(
        got.holds_at_first_reply < got.holds,
        "the first read command was answered only after the rebuild finished ({} / {} holds)",
        got.holds_at_first_reply,
        got.holds
    );
    Ok(())
}

/// **The drainer releases the engine between chunks, whatever the backlog** (the goal:
/// every hold of a background holder ≤ 50 ms).
///
/// Order:
/// 1. A range with `B` inbound messages waiting to be drained — `Completion`s to an invocation
///    that is not in this range (each row is one question to the core and one commit deleting
///    the row, like every real completion).
/// 2. A new leader opens on the same directory; the term's first drain pass walks all `B`
///    rows.
///
/// Must see: every row is drained (none left); `drain` takes the engine at
/// least `B / INBOUND_BATCH` times — no more than `INBOUND_BATCH` rows per hold;
/// the longest hold of `holder="drain"` is under the limit. The old code: one hold for the
/// whole drain pass (~770 rows, 0.5–0.6 s under a backlog of 3·10⁵).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_releases_the_engine_between_chunks_whatever_the_backlog() -> Result<()> {
    const B: usize = 5_000;
    let got = recover_scan::drain_run("drain-chunks", B).await?;
    eprintln!("drain_releases_the_engine_between_chunks_whatever_the_backlog: {got:?}");
    ensure!(got.left == 0, "{} inbound rows left after drain", got.left);
    let min_holds = (B / recover_scan::INBOUND_BATCH) as u64;
    ensure!(
        got.holds >= min_holds,
        "drain took the engine {} times for {B} inbound rows — must be ≥ {min_holds} (no more than {} rows per hold)",
        got.holds,
        recover_scan::INBOUND_BATCH
    );
    ensure!(
        got.max_ms <= recover_scan::HOLD_BOUND_MS,
        "drain's longest engine hold {:.1} ms > limit {} ms",
        got.max_ms,
        recover_scan::HOLD_BOUND_MS
    );
    Ok(())
}

/// A bench of the chunked scans: the longest engine hold of
/// `recover` and the time to the `Start` of the last `running` invocation, by `H`
/// (`CAIRN_BENCH_RECOVER_H`, default `0,10000,50000`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "bench: cargo test -p cairn-server --test recovery_storm recover_scan_bench -- --ignored --nocapture"]
async fn recover_scan_bench() -> Result<()> {
    for h in common::bench::levels("CAIRN_BENCH_RECOVER_H", &[0, 10_000, 50_000]) {
        let got = recover_scan::run(&format!("recover-bench-{h}"), h, 16).await?;
        eprintln!(
            "H={h:>7}  recover: longest hold {:>7.1} ms, {:>4} holds, total {:>8.1} ms; to last Start {:>7.1} ms",
            got.recover_max_ms, got.recover_holds, got.recover_total_ms, got.last_start_ms
        );
    }
    Ok(())
}

/// Tooling for the tests above — kept apart, sharing nothing with the top of the file.
mod recover_scan {
    use {
        super::common::fake_sdk::{FakeSdk, Step},
        anyhow::{anyhow, ensure, Result},
        bytes::Bytes,
        cairn_engine::{CreateOptions, Engine, InvocationOutput, InvocationStatus, Target},
        cairn_server::{
            actor::{EngineHandle, Leadership, Readiness, TurnLimits},
            Registry,
        },
        cairn_store::{
            dbstats::{holder, DbStats},
            make_invocation_id, LeadershipGrant, Mutations, PartitionId, PartitionKey,
            PartitionWriter,
        },
        slatedb::{
            config::Settings,
            object_store::{local::LocalFileSystem, ObjectStore},
            Db,
        },
        std::{
            path::{Path, PathBuf},
            sync::Arc,
            time::{Duration, Instant, SystemTime},
        },
    };

    /// The chunk size of `recover`'s scan.
    pub const CHUNK: usize = 1024;
    /// The test's limit on `recover`'s engine hold — wide compared to the 50 ms goal
    /// because of debug builds and a machine shared with other work; what the test pins is
    /// "does not grow with H", the real numbers are printed by the bench.
    pub const HOLD_BOUND_MS: f64 = 250.0;

    #[derive(Debug)]
    pub struct Got {
        pub recover_holds: u64,
        pub recover_max_ms: f64,
        pub recover_total_ms: f64,
        /// From building the leader to the `Start` of the last `running` invocation.
        pub last_start_ms: f64,
    }

    fn id(n: usize, running: bool) -> [u8; 24] {
        let mut r = [0u8; 16];
        r[..8].copy_from_slice(&(n as u64).to_be_bytes());
        r[8] = u8::from(running);
        make_invocation_id(PartitionKey(0), r)
    }

    fn fresh_root(name: &str) -> Result<PathBuf> {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        if root.exists() {
            std::fs::remove_dir_all(&root)?;
        }
        std::fs::create_dir_all(&root)?;
        Ok(root)
    }

    async fn open_db(root: &Path) -> Result<Db> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root)?);
        Ok(Db::builder("partition-0", store)
            .with_settings(Settings {
                flush_interval: Some(Duration::from_millis(10)),
                ..Default::default()
            })
            .build()
            .await?)
    }

    /// Step 1: `h` `completed` rows and `k` `running` invocations spread evenly.
    async fn seed(root: &Path, h: usize, k: usize) -> Result<Vec<[u8; 24]>> {
        let db = open_db(root).await?;
        let writer =
            PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
        let mut engine = Engine::open(writer).await?;
        let done =
            InvocationStatus::Completed(InvocationOutput::Inline(Bytes::from_static(b"\"done\"")))
                .encode(1);
        for start in (0..h).step_by(4096) {
            let mut muts = Mutations::new();
            for n in start..(start + 4096).min(h) {
                muts.set_invocation_status(&id(n, false), &done);
            }
            engine.writer_mut().commit(muts).await?;
        }
        let target = Target {
            tenant: Default::default(),
            scope: None,
            service: "Echo".into(),
            handler: "run".into(),
            object_key: String::new(),
        };
        let step = (h / k.max(1)).max(1);
        let mut running = Vec::with_capacity(k);
        for j in 0..k {
            let rid = id(j * step + step / 2, true);
            engine
                .create(
                    rid,
                    &target,
                    CreateOptions::default(),
                    cairn_runtime::StartParams::default(),
                    Bytes::from_static(b"{}"),
                )
                .await?;
            running.push(rid);
        }
        engine.seal().await?;
        Ok(running)
    }

    fn spawn_runtime(root: &Path, registry: Registry, stats: Arc<DbStats>) -> Result<EngineHandle> {
        let root = root.to_path_buf();
        EngineHandle::spawn_with_transport(
            Box::new(move || {
                let root = root.clone();
                let stats = stats.clone();
                Box::pin(async move {
                    let db = open_db(&root).await.expect("open SlateDB");
                    let writer =
                        PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
                            .await?
                            .with_db_stats(Some(stats));
                    Ok(Leadership {
                        engine: Engine::open(writer).await?,
                        keeper: None,
                    })
                })
            }),
            registry,
            None,
            Readiness::WhenLeader,
            cairn_server::transport::Transport::disconnected(),
            TurnLimits {
                sdk_timeout: None,
                ..TurnLimits::default()
            },
            None,
        )
        .map_err(Into::into)
    }

    /// The measurements of the lock rebuild ([`restore_run`]).
    #[derive(Debug)]
    pub struct Restored {
        pub locks: usize,
        pub holds: u64,
        pub max_ms: f64,
        /// `restore`'s hold count when the first read command was answered.
        pub holds_at_first_reply: u64,
    }

    /// Step 1 of `restore_rebuilds_locks_in_chunks_while_serving`: `h` completed
    /// invocations, each leaving a tombstone in the queue index, and `k` `suspended`
    /// invocations holding the VO locks of `k` objects.
    async fn seed_restore(root: &Path, h: usize, k: usize) -> Result<()> {
        use cairn_store::{QueueName, Stage, TenantId};
        let db = open_db(root).await?;
        let writer =
            PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
        let mut engine = Engine::open(writer).await?;
        let queue = |key: String| {
            QueueName {
                tenant: TenantId::default(),
                scope: None,
                limit: None,
                exclusive: true,
                service: "Obj".into(),
                object_key: key,
            }
            .encode()
        };
        let done =
            InvocationStatus::Completed(InvocationOutput::Inline(Bytes::from_static(b"\"done\"")))
                .encode(1);
        let hist = queue("hist".into());
        for start in (0..h).step_by(4096) {
            let mut muts = Mutations::new();
            for n in start..(start + 4096).min(h) {
                let rid = id(n, false);
                muts.set_invocation_status(&rid, &done);
                muts.push_vqueue(&hist, Stage::Inbox, n as u64 + 1, &rid);
                muts.pop_vqueue(&hist, Stage::Inbox, n as u64 + 1, &rid);
            }
            engine.writer_mut().commit(muts).await?;
        }
        let suspended = InvocationStatus::Suspended.encode(1);
        let step = (h / k.max(1)).max(1);
        let mut muts = Mutations::new();
        for j in 0..k {
            let rid = id(j * step + step / 2, true);
            muts.set_invocation_status(&rid, &suspended);
            muts.push_vqueue(
                &queue(format!("k{j}")),
                Stage::Suspended,
                (h + j) as u64 + 1,
                &rid,
            );
        }
        engine.writer_mut().commit(muts).await?;
        engine.seal().await?;
        Ok(())
    }

    /// Step 1 then step 2 of `restore_rebuilds_locks_in_chunks_while_serving`.
    pub async fn restore_run(name: &str, h: usize, k: usize) -> Result<Restored> {
        let root = fresh_root(name)?;
        seed_restore(&root, h, k).await?;
        let stats = DbStats::new();
        let rt = spawn_runtime(&root, Registry::new(), stats.clone())?;
        let ix = holder("restore");
        let r = async {
            rt.status(id(0, false)).await?;
            let holds_at_first_reply = stats.snapshot().holds[ix];
            let deadline = Instant::now() + Duration::from_secs(60);
            let locks = loop {
                let locks = rt.flow().await?.scheduler.locks_held;
                let before = stats.snapshot().holds[ix];
                tokio::time::sleep(Duration::from_millis(100)).await;
                let after = stats.snapshot().holds[ix];
                if locks >= k && after == before {
                    break locks;
                }
                ensure!(
                    Instant::now() < deadline,
                    "60 s and only {locks}/{k} VO locks rebuilt"
                );
            };
            let snap = stats.snapshot();
            Ok(Restored {
                locks,
                holds: snap.holds[ix],
                max_ms: snap.hold_max_us[ix] as f64 / 1000.0,
                holds_at_first_reply,
            })
        }
        .await;
        rt.shutdown().await?;
        r
    }

    /// The number of inbound rows the drainer reads per chunk (`turn::inbound::INBOUND_BATCH`).
    pub const INBOUND_BATCH: usize = 64;

    /// The measurements of the drain pass ([`drain_run`]).
    #[derive(Debug)]
    pub struct Drained {
        /// Inbound rows left after the drain pass.
        pub left: usize,
        pub holds: u64,
        pub max_ms: f64,
    }

    /// The target of the `Completion` messages in [`seed_inbound`]: not in the range.
    fn absent() -> [u8; 24] {
        id(usize::MAX, false)
    }

    /// Step 1 of `drain_releases_the_engine_between_chunks_whatever_the_backlog`:
    /// `b` `Completion` messages to an invocation that does not exist ([`absent`]), with
    /// distinct completion ids (the inbound row key is derived from the message content).
    async fn seed_inbound(root: &Path, b: usize) -> Result<()> {
        use cairn_engine::{CompletionPayload, DeliverCompletion, InterPartition};
        let db = open_db(root).await?;
        let writer =
            PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
        let mut engine = Engine::open(writer).await?;
        for start in (0..b).step_by(1024) {
            let msgs: Vec<_> = (start..(start + 1024).min(b))
                .map(|n| {
                    InterPartition::Completion(DeliverCompletion {
                        target: absent(),
                        completion_id: n as u32 + 1,
                        result: CompletionPayload::Void,
                    })
                })
                .collect();
            engine.accept_messages(&msgs).await?;
        }
        engine.seal().await?;
        Ok(())
    }

    /// Step 1 then step 2 of
    /// `drain_releases_the_engine_between_chunks_whatever_the_backlog`.
    pub async fn drain_run(name: &str, b: usize) -> Result<Drained> {
        let root = fresh_root(name)?;
        seed_inbound(&root, b).await?;
        let stats = DbStats::new();
        let rt = spawn_runtime(&root, Registry::new(), stats.clone())?;
        let ix = holder("drain");
        let r = async {
            // Done when `drain` has taken the engine and takes it no more within a
            // window — the hold count only grows on release.
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                let before = stats.snapshot().holds[ix];
                tokio::time::sleep(Duration::from_millis(300)).await;
                let after = stats.snapshot().holds[ix];
                if after > 0 && after == before {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "120 s and the drainer is not done ({after} holds)"
                );
            }
            let snap = stats.snapshot();
            Ok((snap.holds[ix], snap.hold_max_us[ix] as f64 / 1000.0))
        }
        .await;
        rt.shutdown().await?;
        let (holds, max_ms) = r?;
        let db = open_db(&root).await?;
        let writer =
            PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
        let mut engine = Engine::open(writer).await?;
        let left = engine.inbound_from(0, b + 1).await?.len();
        engine.seal().await?;
        Ok(Drained {
            left,
            holds,
            max_ms,
        })
    }

    /// Step 1 then step 2; returns `recover`'s measurements.
    pub async fn run(name: &str, h: usize, k: usize) -> Result<Got> {
        let root = fresh_root(name)?;
        let running = seed(&root, h, k).await?;
        let sdk = FakeSdk::builder()
            .service(
                "Echo",
                "run",
                vec![Step::Output(Bytes::from_static(b"\"ok\""))],
            )
            .spawn()
            .await?;
        let mut d = cairn_server::discovery::discover(
            &cairn_server::egress::TenantHttp::h1(Default::default()),
            &sdk.url,
            None,
        )
        .await
        .map_err(|e| anyhow!("discover fake SDK: {e:?}"))?;
        d.id = "dp_recover_scan".into();
        let registry = Registry::new();
        registry.upsert(d);
        let stats = DbStats::new();
        let began = SystemTime::now();
        let rt = spawn_runtime(&root, registry, stats.clone())?;
        let r = async {
            let ix = holder("recover");
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                let mut done = 0;
                for rid in &running {
                    if matches!(rt.status(*rid).await?, Some(InvocationStatus::Completed(_))) {
                        done += 1;
                    }
                }
                // The scan has reached the end of the index: the hold count no longer grows.
                let before = stats.snapshot().holds[ix];
                tokio::time::sleep(Duration::from_millis(100)).await;
                let after = stats.snapshot().holds[ix];
                if done == running.len() && after == before && after > 0 {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "60 s and only {done}/{} running invocations completed",
                    running.len()
                );
            }
            let starts = sdk.starts_of("Echo", "run");
            let mut ids: Vec<Bytes> = starts.iter().map(|s| s.id.clone()).collect();
            ids.sort();
            ids.dedup();
            ensure!(
                starts.len() == k && ids.len() == k,
                "each running invocation is delivered exactly once: {} Starts for {} distinct ids, want {k}",
                starts.len(),
                ids.len()
            );
            let last = starts.iter().map(|s| s.at).max();
            let snap = stats.snapshot();
            Ok(Got {
                recover_holds: snap.holds[ix],
                recover_max_ms: snap.hold_max_us[ix] as f64 / 1000.0,
                recover_total_ms: snap.hold_us[ix] as f64 / 1000.0,
                last_start_ms: last
                    .and_then(|t| t.duration_since(began).ok())
                    .map_or(0.0, |d| d.as_secs_f64() * 1000.0),
            })
        }
        .await;
        rt.shutdown().await?;
        r
    }
}

// ── Bench "becoming leader with a large history" ────────────────

/// One leadership takeover on an existing bucket: measured from the `node::start` call.
struct Takeover {
    /// Until the first command (a status read through the engine) is served.
    first_served: Duration,
    /// Until the SDK has received again the turns of all `r` `running` invocations.
    last_redriven: Duration,
    /// Until the **first** turn is re-delivered and granted a slot (the SDK receives its first `Start`) —
    /// no turn is granted a slot until the lock rebuild
    /// has finished.
    first_redriven: Option<Duration>,
    gets: u64,
    holds: bench::Holds,
    node: RunningNode,
}

async fn take_over(
    store: &Arc<cairn_store::testkit::FaultyStore>,
    sdk: &FakeSdk,
    disk_cache: &std::path::Path,
    r: usize,
) -> Result<Takeover> {
    let mut cfg = bench::config(store);
    cfg.disk_cache = Some(cairn_store::disk_cache::DiskCacheConfig::new(disk_cache));
    let before_starts = sdk.starts_count_of("Gated", "run");
    let before = store.counts();
    let t0 = std::time::Instant::now();
    let node = node::start(cfg)
        .await
        .map_err(|e| anyhow!("build node: {e}"))?;
    let probe = bench::id(1, 0);
    let first_served = loop {
        // The range id comes from the table (like `common::start_led`), not assumed to be 0.
        let range = node.cluster.table().ranges().first().map(|r| r.id);
        if let Some(h) = range.and_then(|r| node.partitions.get(r)) {
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(500), h.status(probe)).await
            {
                break t0.elapsed();
            }
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(600),
            "no command served after 600 s"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    eprintln!("[takeover] first command after {first_served:?}");
    let mut said = std::time::Instant::now();
    let mut first_redriven = None;
    let last_redriven = loop {
        let n = sdk.starts_count_of("Gated", "run") - before_starts;
        if n >= 1 && first_redriven.is_none() {
            first_redriven = Some(t0.elapsed());
        }
        if n >= r {
            break t0.elapsed();
        }
        if said.elapsed() > Duration::from_secs(10) {
            eprintln!("[takeover] re-delivered {n}/{r} after {:?}", t0.elapsed());
            said = std::time::Instant::now();
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(1800),
            "not all {r} invocations re-delivered after 1800 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let gets = store
        .counts()
        .since(&before)
        .op(cairn_store::testkit::Op::Get);
    let holds = bench::Holds::read(&node.url()).await?;
    Ok(Takeover {
        first_served,
        last_redriven,
        first_redriven,
        gets,
        holds,
        node,
    })
}

/// The wave size when building the history of the takeover bench: the backlog never exceeds this.
const STORM_WAVE: usize = 10_000;

/// **Becoming leader with a large history** (the baseline for the bounded-hold
/// goal). A measurement, run by hand:
///
/// ```sh
/// STORM_H=0,10000,100000 STORM_R=100,10000 \
///   cargo test --release -p cairn-server --test recovery_storm \
///   takeover_with_large_history -- --ignored --nocapture
/// ```
///
/// For each cell (H, R): node A on `memory://` (wrapped in `FaultyStore` to count GETs) receives
/// H `StartCall` messages to `Echo` (completes at once — history) then R messages to `Gated`
/// (stopped at the fake SDK's gate — `running`), then shuts down. Node B opens the same bucket
/// with an empty disk cache (**cold**), the gate open: measure from `node::start` until the first
/// command (a status read through the engine) is served and until the SDK has received again the
/// turns of all R invocations; the GETs in between; engine hold time per holder (total,
/// longest). B shuts down; node C opens the same bucket and the same disk cache directory
/// (**warm**: the SSTs B read are on disk), measures again — except the "re-delivered" column: R completed on B.
///
/// The history is built through the real path (`/internal/inbound/batch` → create → SDK turn),
/// not written straight into the Db: every row of a completed invocation (status,
/// index, metadata, journal) is exactly as production writes it. Sent in waves of
/// [`STORM_WAVE`] (`bench::build`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn takeover_with_large_history() -> Result<()> {
    let hs = bench::levels("STORM_H", &[0, 10_000]);
    let rs = bench::levels("STORM_R", &[100]);
    println!(
        "\n  {:>8} | {:>6} | {:>5} | {:>11} | {:>12} | {:>12} | {:>7} | {:>8} | engine hold (holder total/longest ms)",
        "H", "R", "cache", "first cmd ms", "first grant ms", "redrive ms", "GET", "build H s"
    );
    for &h in &hs {
        for &r in &rs {
            let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
            let sdk = FakeSdk::builder()
                .service("Echo", "run", vec![Step::output("ok")])
                .gated_service("Gated", "run", vec![Step::output("ok")])
                .spawn()
                .await?;
            // ── A: build history H and R running invocations.
            let a = common::start_led(bench::config(&store)).await?;
            common::register(&a.url(), &sdk.url).await?;
            let t_h = std::time::Instant::now();
            // In waves, polled by count (`bench::build`): sending all of H at once
            // then copying the `Start` list on every poll tick made 10⁶ unbuildable.
            bench::build(&a, &sdk, 1, "Echo", h, STORM_WAVE, "completed").await?;
            // Every 1000th id and the last 64 ids complete (the turn runs before the
            // `completed` commit).
            for i in (0..h).step_by(1000).chain(h.saturating_sub(64)..h) {
                common::wait_status(&a.url(), &bench::hex(&bench::id(1, i)), "completed").await?;
            }
            let built = t_h.elapsed();
            eprintln!("[takeover] H={h}: history built after {built:?}");
            bench::send(&a, 2, "Gated", None, r).await?;
            eprintln!("[takeover] R={r} sent; shutting down A");
            a.shutdown().await;
            eprintln!("[takeover] A is down");
            // Open the gate before the new leader comes up: re-delivered turns complete at once, so
            // "re-delivered" measures until the SDK has received the turns of all R invocations. On A
            // they are `running` (stopped at the gate or waiting for their turn).
            sdk.gate().open();

            let cache = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
                .join(format!("recovery-storm-{h}-{r}"));
            let _ = std::fs::remove_dir_all(&cache);
            std::fs::create_dir_all(&cache)?;
            // The warm run: R already completed in the cold run — measure only the first command and the scan.
            for (label, wait) in [("cold", r), ("warm", 0)] {
                let t = take_over(&store, &sdk, &cache, wait).await?;
                println!(
                    "  {:>8} | {:>6} | {:>5} | {:>11} | {:>12} | {:>12} | {:>7} | {:>8.1} | {}",
                    h,
                    r,
                    label,
                    t.first_served.as_millis(),
                    t.first_redriven
                        .map_or_else(|| "-".to_string(), |d| d.as_millis().to_string()),
                    t.last_redriven.as_millis(),
                    t.gets,
                    built.as_secs_f64(),
                    t.holds.summary(),
                );
                t.node.shutdown().await;
            }
            let _ = std::fs::remove_dir_all(&cache);
        }
    }
    Ok(())
}

/// **The cost of one create by history size** (an open question: "creating invocations
/// on a range slows down with history"). A measurement, run by
/// hand:
///
/// ```sh
/// GROW_H=1000000 GROW_WAVE=10000 \
///   cargo test --release -p cairn-server --test recovery_storm \
///   create_cost_by_history -- --ignored --nocapture
/// ```
///
/// A one-range node on `memory://` (wrapped in `FaultyStore`). The history is built through the
/// real path as in the takeover bench (`/internal/inbound/batch` → create → `Echo` turn →
/// `completed`), but **in waves** of `GROW_WAVE`: the next wave is sent only once the SDK has
/// received every `Start` of the previous wave and the last id is `completed` — the backlog never
/// exceeds one wave, so any column that grows with H grows with **history**, not with
/// the queue. Each wave prints the cost **per invocation**: process CPU time,
/// store requests, SST blocks read (hits + misses of the RAM block cache), engine hold time
/// per holder; plus the L0/sorted-run counts at the end of the wave. The SDK is polled with
/// `starts_count_of` (no copy of the `Start` list).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run by hand"]
async fn create_cost_by_history() -> Result<()> {
    use cairn_store::testkit::Op;
    let total = bench::levels("GROW_H", &[100_000])[0];
    let wave = bench::levels("GROW_WAVE", &[10_000])[0].max(1);
    let old_poll = std::env::var("GROW_OLD_POLL").is_ok_and(|v| v == "1");
    let store = cairn_store::testkit::FaultyStore::wrap(Arc::new(InMemory::new()));
    let sdk = FakeSdk::builder()
        .service("Echo", "run", vec![Step::output("ok")])
        .spawn()
        .await?;
    let a = common::start_led(bench::config(&store)).await?;
    common::register(&a.url(), &sdk.url).await?;
    println!(
        "\n  {:>8} | {:>6} | {:>7} | {:>6} | {:>7} | {:>6} | {:>6} | {:>6} | {:>8} | {:>3} | {:>3} | engine hold µs/inv (holder); holds/inv; longest since open ms",
        "H", "wave s", "inv/s", "CPU µs", "GET", "PUT", "LIST", "DEL", "SST blks", "L0", "SR"
    );
    let mut h = 0usize;
    let t_all = std::time::Instant::now();
    while h < total {
        let n = wave.min(total - h);
        let before = bench::Probe::take(&a.url(), &store).await?;
        bench::send_range(&a, 1, "Echo", None, h..h + n).await?;
        // `GROW_OLD_POLL=1`: poll like the old takeover bench (copying the whole list every 50 ms)
        // — a control for the polling.
        let count = || {
            if old_poll {
                sdk.starts_of("Echo", "run").len()
            } else {
                sdk.starts_count_of("Echo", "run")
            }
        };
        while count() < h + n {
            ensure!(
                before.at.elapsed() < Duration::from_secs(1800),
                "wave H={h} not done after 1800 s: {}/{} Start",
                sdk.starts_count_of("Echo", "run"),
                h + n
            );
            tokio::time::sleep(Duration::from_millis(if old_poll { 50 } else { 20 })).await;
        }
        common::wait_status(&a.url(), &bench::hex(&bench::id(1, h + n - 1)), "completed").await?;
        let after = bench::Probe::take(&a.url(), &store).await?;
        let per = |x: f64| x / n as f64;
        let d = after.store.since(&before.store);
        let op = |o| per(d.op(o) as f64);
        let holds: Vec<String> = after
            .holds
            .by
            .iter()
            .filter_map(|(k, (t, _, _))| {
                let t0 = before.holds.by.get(k).map_or(0.0, |v| v.0);
                let us = per((t - t0) * 1000.0);
                (us >= 1.0).then(|| format!("{k} {us:.0}"))
            })
            .collect();
        let (mh, mm) = after.holds.max();
        let secs = after.at.duration_since(before.at).as_secs_f64();
        println!(
            "  {:>8} | {:>6.1} | {:>7.0} | {:>6.0} | {:>7.2} | {:>6.2} | {:>6.3} | {:>6.3} | {:>8.1} | {:>3.0} | {:>3.0} | {}; {:.2}; {mm:.0} ({mh})",
            h,
            secs,
            n as f64 / secs,
            per((after.cpu_s - before.cpu_s) * 1e6),
            op(Op::Get),
            op(Op::Put),
            op(Op::List),
            op(Op::Delete),
            per(after.blocks - before.blocks),
            after.l0_ssts,
            after.sorted_runs,
            holds.join(", "),
            per(after.holds.count_since(&before.holds)),
        );
        // `db.write` waits for SlateDB's backpressure (≥ 10 ms each time) — the wait lies
        // **inside** the writer's engine hold.
        println!(
            "           write throttle {:.1} s, flusher stalled on L0 {:.0} times",
            after.write_stall_s - before.write_stall_s,
            after.l0_stalls - before.l0_stalls
        );
        h += n;
    }
    eprintln!("[grow] built H={total} after {:?}", t_all.elapsed());
    // The cost of the old polling itself (`starts_of(..).len()`, the old takeover bench polled
    // every 50 ms): copying the whole `Start` list under the fake SDK's lock.
    let t = std::time::Instant::now();
    let copied = sdk.starts_of("Echo", "run").len();
    eprintln!(
        "[grow] starts_of(..).len() at H={copied}: {:?} per poll",
        t.elapsed()
    );
    a.shutdown().await;
    Ok(())
}
