// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Query-layer benchmark (`bench/query/README.md`).
//!
//! `#[ignore]` — run by hand, release build:
//!
//! ```sh
//! CAIRN_BENCH_PER_RANGE=10000 cargo test --release -p cairn-query --test bench -- --ignored --nocapture
//! ```
//!
//! Four `InMemory` ranges, `CAIRN_BENCH_PER_RANGE` invocations per range
//! (default 10 000), 1 % `failed`, 30 % `completed`, the rest `active`. Each
//! query runs 20 times, printing the median time and the read units. The "no
//! index" path is a query whose filter cannot be pushed down — scan all of
//! `ByCreated`, then read metadata per row, about the work of the old list
//! (reading entry 0 per row).
//!
//! Interference: median and p99 of `create` on one range for 2 s, alone and
//! while another task runs the newest page continuously on every range. The
//! `InMemory` store: measures CPU/lock contention in the process, not store I/O.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "measurement outside the deterministic simulation"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{BuildCursor, BuildStep, CreateOptions, Engine, Target},
    cairn_query::{run, Limits, QueryCtx, TenantScope},
    cairn_runtime::StartParams,
    cairn_store::{
        clock::ManualClock, make_invocation_id, ClusterConfig, LeadershipGrant, PartitionId,
        PartitionKey, PartitionWriter, QueueName, TenantId,
    },
    slatedb::{object_store::memory::InMemory, Db},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

fn queue(service: &str) -> QueueName {
    QueueName {
        tenant: TenantId::new("acme").unwrap(),
        scope: None,
        limit: None,
        exclusive: false,
        service: service.into(),
        object_key: String::new(),
    }
}

async fn create(
    e: &mut Engine,
    cluster: &ClusterConfig,
    p: usize,
    i: usize,
) -> Result<cairn_store::InvocationId> {
    let range = cluster.range(PartitionId(p as u32)).unwrap();
    let mut unique = [0u8; 16];
    unique[..8].copy_from_slice(&(i as u64).to_be_bytes());
    let id = make_invocation_id(PartitionKey(range.start + i as u64), unique);
    let service = format!("Svc{}", i % 20);
    let q = queue(&service);
    let seq = e.next_queue_seq(&q).await?;
    e.create(
        id,
        &Target {
            tenant: TenantId::new("acme").unwrap(),
            scope: None,
            service,
            handler: "run".into(),
            object_key: String::new(),
        },
        CreateOptions {
            queue: Some((&q, seq)),
            ..Default::default()
        },
        StartParams::default(),
        Bytes::new(),
    )
    .await?;
    Ok(id)
}

/// `create` on range 0 for 2 s, latency of each call (ms).
async fn measure(
    e0: &mut Engine,
    cluster: &ClusterConfig,
    clock: &ManualClock,
    base: usize,
) -> Result<Vec<f64>> {
    let mut lat = Vec::new();
    let until = Instant::now() + Duration::from_secs(2);
    for k in 0.. {
        if Instant::now() >= until {
            break;
        }
        clock.advance(Duration::from_millis(3));
        let t = Instant::now();
        create(e0, cluster, 0, base + k).await?;
        lat.push(t.elapsed().as_secs_f64() * 1e3);
    }
    Ok(lat)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn p99(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[(v.len() * 99 / 100).min(v.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual measurement: --ignored --nocapture"]
async fn bench_query_paths() -> Result<()> {
    let per_range: usize = std::env::var("CAIRN_BENCH_PER_RANGE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let cluster = ClusterConfig::equal(4);
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let mut engines = Vec::new();
    for p in 0..4u32 {
        let db = Db::builder(format!("range-{p}"), Arc::new(InMemory::new()))
            .build()
            .await?;
        let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(p)), db).await?;
        engines.push(
            Engine::open_in_cluster(w, cluster.clone())
                .await?
                .with_clock(clock.clone()),
        );
    }
    let t0 = Instant::now();
    for i in 0..per_range * 4 {
        let p = i % 4;
        clock.advance(Duration::from_millis(3));
        let id = create(&mut engines[p], &cluster, p, i).await?;
        match i % 100 {
            0 => {
                engines[p].cancel(&id, "bench").await?;
            }
            1..=30 => {
                engines[p].pause_with(&id, 575, "bench").await?;
            }
            _ => {}
        }
    }
    for e in &mut engines {
        let mut c = BuildCursor::default();
        while e.query_build_step(&mut c, 1024).await? != BuildStep::Idle {}
    }
    println!(
        "built {} invocations on 4 ranges: {:.1} s",
        per_range * 4,
        t0.elapsed().as_secs_f64()
    );
    let ctx = |engines: &[Engine]| {
        QueryCtx::new(
            TenantScope::One("acme".into()),
            engines.iter().map(Engine::read_handle).collect(),
        )
    };
    // Wait for the index marks to reach `Remote`.
    for e in &engines {
        let h = e.read_handle();
        while h.snapshot()?.readable_indexes().await?.len()
            < cairn_store::query_index::QUERY_INDEXES.len()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    println!("| query | median ms | read units |\n|---|---|---|");
    for (name, sql) in [
        ("newest page (50)", "SELECT id, service, created_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 50"),
        ("count by class (counters)", "SELECT class, sum(count) FROM sys_invocation_counts GROUP BY class"),
        ("rare class `failed` (50)", "SELECT id FROM sys_invocation WHERE class = 'failed' ORDER BY created_at DESC, id LIMIT 50"),
        ("by service (50)", "SELECT id FROM sys_invocation WHERE service = 'Svc7' ORDER BY created_at DESC, id LIMIT 50"),
        ("ordered by transitioned_at (50)", "SELECT id FROM sys_invocation ORDER BY transitioned_at DESC, id LIMIT 50"),
        ("no index: filter not pushed down (50)", "SELECT id FROM sys_invocation WHERE idempotency_key IS NULL ORDER BY created_at DESC, id LIMIT 50"),
    ] {
        let mut ms = Vec::new();
        let mut read = 0;
        for _ in 0..20 {
            let c = ctx(&engines);
            let stats = c.stats.clone();
            let t = Instant::now();
            run(
                c,
                sql,
                Limits {
                    read_budget: u64::MAX,
                    ..Limits::default()
                },
            )
            .await?;
            ms.push(t.elapsed().as_secs_f64() * 1e3);
            read = stats.units();
        }
        println!("| {name} | {:.2} | {read} |", median(ms));
    }

    // Baseline: the old list — newest page of 50 rows through
    // `list_invocations_where` (tenant + `created_desc`, scan up to the cap,
    // then read entry 0 per row), per range then merged, like `GET /invocations`.
    {
        let filter = cairn_engine::ListFilter {
            tenant: Some(TenantId::new("acme").unwrap()),
            created_desc: true,
            ..Default::default()
        };
        let mut ms = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let mut rows = 0;
            for e in &engines {
                rows += e.list_invocations_where(50, &filter).await?.rows.len();
            }
            ms.push(t.elapsed().as_secs_f64() * 1e3);
            assert!(rows >= 50.min(per_range * 4));
        }
        println!(
            "| old list `GET /invocations` (50, baseline) | {:.2} | — |",
            median(ms)
        );
    }

    // Interference: `create` on range 0, alone, then alongside the query loop.
    let handles: Vec<_> = engines.iter().map(Engine::read_handle).collect();
    let mut e0 = engines.remove(0);
    let alone = measure(&mut e0, &cluster, &clock, 10_000_000).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let loader = {
        let stop = stop.clone();
        let handles = handles.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let c = QueryCtx::new(TenantScope::One("acme".into()), handles.clone());
                let _ = run(
                    c,
                    "SELECT id FROM sys_invocation ORDER BY created_at DESC, id LIMIT 50",
                    Limits::default(),
                )
                .await;
                n += 1;
            }
            n
        })
    };
    let busy = measure(&mut e0, &cluster, &clock, 20_000_000).await?;
    stop.store(true, Ordering::Relaxed);
    let queries = loader.await?;
    println!(
        "\n| create on range 0 (2 s) | calls | median ms | p99 ms |\n|---|---|---|---|\n| alone | {} | {:.3} | {:.3} |\n| with {queries} newest-page queries | {} | {:.3} | {:.3} |",
        alone.len(),
        median(alone.clone()),
        p99(alone),
        busy.len(),
        median(busy.clone()),
        p99(busy)
    );
    Ok(())
}
