// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `sys_invocation` and `sys_invocation_counts` over four real ranges: plan
//! shape, read cost, path chooser, tenant, lockdown, and an exhaustive oracle.
//!
//! Setup: four `Engine`s on four `InMemory` Dbs (table `ClusterConfig::equal(4)`),
//! invocations in all four ranges with two tenants, three services, roots and
//! children, four classes; the index + counter backfill runs to completion; the
//! query layer reads through `read_handle`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    cairn_engine::{BuildCursor, BuildStep, CreateOptions, Engine, ReplyTo, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_query::{
        explain,
        remote::{serve, RemoteScan, Scan, ScanRequest, ScanResponse, WirePath},
        run, run_with_params, Limits, Param, QueryCtx, QueryError, TenantScope, Warning,
        MAX_SQL_BYTES,
    },
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        clock::ManualClock, id_hex, make_invocation_id, query_index::Class,
        range_read::RangeReadHandle, ClusterConfig, LeadershipGrant, PartitionId, PartitionKey,
        PartitionWriter, QueueName, TenantId,
    },
    datafusion::arrow::{
        array::{Array, AsArray, RecordBatch},
        datatypes::TimestampMillisecondType,
    },
    slatedb::{object_store::memory::InMemory, Db},
    std::{sync::Arc, time::Duration},
};

const T0: u64 = 1_700_000_000_000;

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

/// An invocation as the oracle knows it.
#[derive(Clone, Debug)]
struct Truth {
    id: String,
    tenant: String,
    service: String,
    handler: String,
    class: Class,
    root: bool,
    created: u64,
    t: u64,
    key: String,
    idempotency_key: Option<String>,
    tagged: bool,
}

struct Fx {
    engines: Vec<Engine>,
    truth: Vec<Truth>,
}

impl Fx {
    fn ctx(&self, tenant: TenantScope) -> QueryCtx {
        QueryCtx::new(
            tenant,
            self.engines.iter().map(Engine::read_handle).collect(),
        )
    }
}

fn queue(tenant: &str, service: &str) -> QueueName {
    QueueName {
        tenant: TenantId::new(tenant).unwrap(),
        scope: None,
        limit: None,
        exclusive: false,
        service: service.into(),
        object_key: String::new(),
    }
}

/// `n` invocations per range. Class by `class_of(i)`; `i % 5 == 0` is a child.
async fn fixture(n: usize, class_of: impl Fn(usize) -> Class) -> Result<Fx> {
    fixture_with(n, class_of, true).await
}

/// [`fixture`]; `indexed == false` skips the background index build, so every
/// range is still `indexing` and `sys_invocation` takes the fallback path.
async fn fixture_with(n: usize, class_of: impl Fn(usize) -> Class, indexed: bool) -> Result<Fx> {
    let cluster = ClusterConfig::equal(4);
    let clock = Arc::new(ManualClock::new(T0));
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
    let mut truth = Vec::new();
    let caller = ReplyTo {
        caller: [0xEE; 24],
        completion_id: 1,
    };
    for i in 0..n * 4 {
        let p = i % 4;
        let range = cluster.range(PartitionId(p as u32)).unwrap();
        let mut unique = [0u8; 16];
        unique[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let id = make_invocation_id(PartitionKey(range.start + i as u64), unique);
        let tenant = if i % 7 == 3 { "zeta" } else { "acme" };
        let service = format!("Svc{}", i % 3);
        let handler = format!("h{}", i % 2);
        let key = if i % 3 == 0 {
            format!("o{}", i % 2)
        } else {
            String::new()
        };
        let idempotency_key = (i % 6 == 1).then(|| format!("idem-{i}"));
        let tagged = i % 4 == 1;
        // Two consecutive invocations with the same timestamp: order within the timestamp is by id.
        if i % 2 == 0 {
            clock.advance(Duration::from_millis(1000));
        }
        let created = cairn_store::clock::Clock::wall_ms(&*clock);
        let e = &mut engines[p];
        let q = queue(tenant, &service);
        let seq = e.next_queue_seq(&q).await?;
        let root = i % 5 != 0;
        e.create(
            id,
            &Target {
                tenant: TenantId::new(tenant).unwrap(),
                scope: None,
                service: service.clone(),
                handler: handler.clone(),
                object_key: key.clone(),
            },
            CreateOptions {
                reply_to: (!root).then_some(&caller),
                queue: Some((&q, seq)),
                idempotency_key: idempotency_key.as_deref(),
                tags: if tagged { &[("team", "a")] } else { &[] },
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
        let class = class_of(i);
        clock.advance(Duration::from_millis(7));
        let t = match class {
            Class::Active => created,
            _ => cairn_store::clock::Clock::wall_ms(&*clock),
        };
        match class {
            Class::Active => {}
            Class::Paused => {
                e.pause_with(&id, 575, "x").await?;
            }
            Class::Failed => {
                e.cancel(&id, "cancel").await?;
            }
            Class::Completed => {
                let mut inv = e.load(id, StartParams::default()).await?;
                e.step(&mut inv, Event::Start).await?;
                for f in [
                    frame(
                        MessageType::OUTPUT,
                        &pb::OutputCommandMessage {
                            result: Some(pb::output_command_message::Result::Value(
                                cairn_protocol::value::inline(Bytes::from_static(b"1")),
                            )),
                            name: String::new(),
                        },
                    ),
                    frame(MessageType::END, &pb::EndMessage {}),
                ] {
                    e.step(&mut inv, Event::FromSdk(f)).await?;
                }
            }
        }
        truth.push(Truth {
            id: id_hex(&id),
            tenant: tenant.into(),
            service,
            handler,
            class,
            root,
            created,
            t,
            key,
            idempotency_key,
            tagged,
        });
    }
    if !indexed {
        return Ok(Fx { engines, truth });
    }
    for e in &mut engines {
        let mut c = BuildCursor::default();
        while e.query_build_step(&mut c, 1024).await? != BuildStep::Idle {}
    }
    // The read contract: the query layer sees the `Remote` level, possibly one
    // tick after the commit — wait until every range's index mark is visible at
    // that level.
    for e in &engines {
        let h = e.read_handle();
        let t0 = std::time::Instant::now();
        while h.snapshot()?.readable_indexes().await?.len()
            < cairn_store::query_index::QUERY_INDEXES.len()
        {
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "mark did not reach the Remote level"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Ok(Fx { engines, truth })
}

fn mixed(i: usize) -> Class {
    match i % 4 {
        0 => Class::Active,
        1 => Class::Completed,
        2 => Class::Failed,
        _ => Class::Paused,
    }
}

fn strings(b: &[RecordBatch], col: usize) -> Vec<String> {
    b.iter()
        .flat_map(|b| {
            let a = b.column(col).as_string::<i32>();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn times(b: &[RecordBatch], col: usize) -> Vec<u64> {
    b.iter()
        .flat_map(|b| {
            let a = b.column(col).as_primitive::<TimestampMillisecondType>();
            (0..a.len()).map(|i| a.value(i) as u64).collect::<Vec<_>>()
        })
        .collect()
}

/// Oracle: id theo `[time DESC, id ASC]`.
fn oracle(
    truth: &[Truth],
    keep: impl Fn(&Truth) -> bool,
    transition: bool,
    n: usize,
) -> Vec<String> {
    let mut v: Vec<&Truth> = truth.iter().filter(|t| keep(t)).collect();
    let time = |t: &Truth| if transition { t.t } else { t.created };
    v.sort_by(|a, b| time(b).cmp(&time(a)).then(a.id.cmp(&b.id)));
    v.into_iter().take(n).map(|t| t.id.clone()).collect()
}

const LIST: &str =
    "SELECT id, created_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 20";

/// **The latest page**: no `SortExec`, `fetch` pushed down to the leaves, ≤ 2
/// seeks per range (root stream + child stream), 0 point reads.
#[tokio::test]
async fn plan_shape_list_page() -> Result<()> {
    let fx = fixture(25, mixed).await?;
    let plan = explain(fx.ctx(TenantScope::One("acme".into())), LIST).await?;
    ensure!(!plan.contains("SortExec"), "SortExec left:\n{plan}");
    ensure!(
        plan.contains("CairnScanExec: table=sys_invocation, path=ByCreated")
            && plan.contains("fetch=20"),
        "leaves got no fetch:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn list_page_reads_index_only() -> Result<()> {
    let fx = fixture(25, mixed).await?;
    let ctx = fx.ctx(TenantScope::One("acme".into()));
    let stats = ctx.stats.clone();
    let out = run(ctx, LIST, Limits::default()).await?;
    let want = oracle(&fx.truth, |t| t.tenant == "acme", false, 20);
    ensure!(
        strings(&out.batches, 0) == want,
        "{:?} ≠ {want:?}",
        strings(&out.batches, 0)
    );
    let (seeks, rows, points) = stats.snapshot();
    ensure!(points == 0, "point reads: {points}");
    ensure!(seeks <= 2 * 4, "seek: {seeks}");
    ensure!(
        rows <= 4 * 2 * 20,
        "rows: {rows} — each stream reads at most `LIMIT` rows"
    );
    ensure!(!out.partial, "{:?}", out.warnings);
    Ok(())
}

/// **A rare class only reads its own stream**: 2 % `failed` among `completed`.
#[tokio::test]
async fn rare_class_reads_only_its_stream() -> Result<()> {
    let fx = fixture(60, |i| {
        if i % 50 == 7 {
            Class::Failed
        } else {
            Class::Completed
        }
    })
    .await?;
    let sql = "SELECT id FROM sys_invocation WHERE class = 'failed' ORDER BY created_at DESC, id";
    let plan = explain(fx.ctx(TenantScope::All), sql).await?;
    ensure!(plan.contains("path=ByTransition"), "{plan}");
    let ctx = fx.ctx(TenantScope::All);
    let stats = ctx.stats.clone();
    let out = run(ctx, sql, Limits::default()).await?;
    let want = oracle(&fx.truth, |t| t.class == Class::Failed, false, usize::MAX);
    ensure!(strings(&out.batches, 0) == want);
    let (_, rows, points) = stats.snapshot();
    ensure!(
        rows == want.len() as u64 && points == 0,
        "read {rows} rows, {points} points"
    );
    Ok(())
}

/// **The path chooser follows the counters**: `failed` at 25 % ⇒ a value filter
/// on `ByCreated`, not the `ByTransition` stream.
#[tokio::test]
async fn planner_uses_counts_to_pick_stream() -> Result<()> {
    let fx = fixture(20, mixed).await?;
    let sql =
        "SELECT id FROM sys_invocation WHERE class = 'failed' ORDER BY created_at DESC, id LIMIT 5";
    let plan = explain(fx.ctx(TenantScope::All), sql).await?;
    ensure!(plan.contains("path=ByCreated"), "{plan}");
    let out = run(fx.ctx(TenantScope::All), sql, Limits::default()).await?;
    ensure!(strings(&out.batches, 0) == oracle(&fx.truth, |t| t.class == Class::Failed, false, 5));
    Ok(())
}

/// **Counting does not read the store** and matches an exhaustive count.
#[tokio::test]
async fn counts_read_nothing() -> Result<()> {
    let fx = fixture(20, mixed).await?;
    let ctx = fx.ctx(TenantScope::All);
    let stats = ctx.stats.clone();
    let out = run(
        ctx,
        "SELECT class, sum(count) AS n FROM sys_invocation_counts GROUP BY class ORDER BY class",
        Limits::default(),
    )
    .await?;
    ensure!(
        stats.units() == 0 && stats.snapshot().0 == 0,
        "read the store"
    );
    let got: Vec<(String, u64)> = {
        let names = strings(&out.batches, 0);
        let n: Vec<u64> = out
            .batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column(1)
                    .as_primitive::<datafusion::arrow::datatypes::UInt64Type>();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        names.into_iter().zip(n).collect()
    };
    let mut want: Vec<(String, u64)> = Class::ALL
        .iter()
        .map(|c| {
            (
                c.name().to_string(),
                fx.truth.iter().filter(|t| t.class == *c).count() as u64,
            )
        })
        .filter(|(_, n)| *n > 0)
        .collect();
    want.sort();
    ensure!(got == want, "{got:?} ≠ {want:?}");
    Ok(())
}

/// **Tenancy is structural**: the `acme` key never sees a `zeta` row — not
/// even when filtering on the tenant directly, point-reading a `zeta` id, or
/// through the counters.
#[tokio::test]
async fn tenant_is_structural() -> Result<()> {
    let fx = fixture(20, mixed).await?;
    let acme = || fx.ctx(TenantScope::One("acme".into()));
    let out = run(
        acme(),
        "SELECT DISTINCT tenant FROM sys_invocation",
        Limits::default(),
    )
    .await?;
    ensure!(strings(&out.batches, 0) == vec!["acme".to_string()]);
    let out = run(
        acme(),
        "SELECT id FROM sys_invocation WHERE tenant = 'zeta'",
        Limits::default(),
    )
    .await?;
    ensure!(out.batches.iter().all(|b| b.num_rows() == 0));
    let zeta = fx.truth.iter().find(|t| t.tenant == "zeta").unwrap();
    let out = run(
        acme(),
        &format!("SELECT id FROM sys_invocation WHERE id = '{}'", zeta.id),
        Limits::default(),
    )
    .await?;
    ensure!(
        out.batches.iter().all(|b| b.num_rows() == 0),
        "point read leaked a tenant"
    );
    let out = run(
        acme(),
        "SELECT DISTINCT tenant FROM sys_invocation_counts",
        Limits::default(),
    )
    .await?;
    ensure!(strings(&out.batches, 0) == vec!["acme".to_string()]);
    Ok(())
}

/// **The by-id path reads exactly one range**: two point reads, no seek.
#[tokio::test]
async fn owner_path_hits_one_range() -> Result<()> {
    let fx = fixture(10, mixed).await?;
    let t = &fx.truth[13];
    let ctx = fx.ctx(TenantScope::All);
    let stats = ctx.stats.clone();
    let out = run(
        ctx,
        &format!(
            "SELECT id, service, class, status FROM sys_invocation WHERE id = '{}'",
            t.id
        ),
        Limits::default(),
    )
    .await?;
    ensure!(strings(&out.batches, 0) == vec![t.id.clone()]);
    ensure!(strings(&out.batches, 2) == vec![t.class.name().to_string()]);
    let (seeks, _, points) = stats.snapshot();
    ensure!(seeks == 0 && points == 2, "seek {seeks}, points {points}");
    Ok(())
}

/// **Actual order = declared order**, both axes, both directions, no `SortExec`.
#[tokio::test]
async fn scan_order_matches_declared() -> Result<()> {
    let fx = fixture(15, mixed).await?;
    for (order, transition, desc) in [
        ("created_at DESC, id ASC", false, true),
        ("created_at ASC, id DESC", false, false),
        ("transitioned_at DESC, id ASC", true, true),
        ("transitioned_at ASC, id DESC", true, false),
    ] {
        let time = if transition {
            "transitioned_at"
        } else {
            "created_at"
        };
        let sql = format!("SELECT id, {time} FROM sys_invocation ORDER BY {order} LIMIT 25");
        let plan = explain(fx.ctx(TenantScope::All), &sql).await?;
        ensure!(!plan.contains("SortExec"), "{order}:\n{plan}");
        let out = run(fx.ctx(TenantScope::All), &sql, Limits::default()).await?;
        let mut want = oracle(&fx.truth, |_| true, transition, usize::MAX);
        if !desc {
            want.reverse();
        }
        want.truncate(25);
        ensure!(strings(&out.batches, 0) == want, "{order}");
        let ts = times(&out.batches, 1);
        ensure!(
            ts.windows(2)
                .all(|w| if desc { w[0] >= w[1] } else { w[0] <= w[1] }),
            "{order}: {ts:?}"
        );
    }
    Ok(())
}

/// **Lockdown**: DDL, DML, statements, table functions, recursive CTEs,
/// functions allocating outside the pool, file reads — all refused as the
/// caller's error.
#[tokio::test]
async fn sql_surface_is_locked() -> Result<()> {
    let fx = fixture(2, mixed).await?;
    for sql in [
        "CREATE TABLE t AS SELECT 1",
        "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '/etc/passwd'",
        "INSERT INTO sys_invocation VALUES ('x')",
        "COPY (SELECT 1) TO '/tmp/x.csv'",
        "SET datafusion.execution.batch_size = 1",
        "SELECT * FROM generate_series(1, 10)",
        "SELECT * FROM range(10)",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r",
        "SELECT repeat('a', 1000000000)",
        "SELECT lpad('a', 1000000000)",
        "SELECT * FROM '/etc/passwd'",
        "DROP TABLE sys_invocation",
        // Scalar array-generating functions (from a security review) — small numbers: the old version really ran them.
        "SELECT range(1, 10)",
        "SELECT generate_series(1, 10)",
        "SELECT array_repeat(1, 10)",
    ] {
        match run(fx.ctx(TenantScope::All), sql, Limits::default()).await {
            Err(e) if e.is_client() => {}
            other => anyhow::bail!("{sql}: {other:?}"),
        }
    }
    // `information_schema` is open.
    run(
        fx.ctx(TenantScope::All),
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'sys_invocation'",
        Limits::default(),
    )
    .await?;
    Ok(())
}

/// **Memory, read and row caps**.
#[tokio::test]
async fn memory_cap_is_enforced() -> Result<()> {
    let fx = fixture(50, mixed).await?;
    let tiny = Limits {
        memory_bytes: 1024,
        ..Limits::default()
    };
    let sql = "SELECT handler, array_agg(id) FROM sys_invocation GROUP BY handler ORDER BY handler";
    match run(fx.ctx(TenantScope::All), sql, tiny).await {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!("memory cap did not stop it: {other:?}"),
    }
    let few_reads = Limits {
        read_budget: 10,
        ..Limits::default()
    };
    match run(
        fx.ctx(TenantScope::All),
        "SELECT id FROM sys_invocation",
        few_reads,
    )
    .await
    {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!("read cap did not stop it: {other:?}"),
    }
    let few_rows = Limits {
        max_rows: 5,
        ..Limits::default()
    };
    match run(
        fx.ctx(TenantScope::All),
        "SELECT id FROM sys_invocation",
        few_rows,
    )
    .await
    {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!("row cap did not stop it: {other:?}"),
    }
    Ok(())
}

/// **The fallback path reads the hot region in chunks, under the read budget**
/// (from a security review). While a range is still building its indexes,
/// `sys_invocation` scans the range's hot region instead:
///
/// 1. a tenant sends `SELECT id FROM sys_invocation` while a range is `indexing`;
/// 2. the fallback scan used to read the whole hot region into memory with no
///    limit, and only then charged the rows to the read budget — the budget
///    stopped the query after the memory was already spent.
///
/// Must see: a query over a budget smaller than the hot region stops after at
/// most one chunk past the budget; with the default budget the fallback answer
/// across many chunks is the same as the oracle's, with an `indexing` warning.
#[tokio::test]
async fn fallback_scan_stops_at_the_read_budget() -> Result<()> {
    let fx = fixture_with(300, mixed, false).await?;
    // The query layer reads the `Remote` level, possibly one tick after the
    // commit (there is no index mark to wait for here): wait until the
    // fallback answer covers every write, so the hot region is full below.
    let want = oracle(&fx.truth, |t| t.tenant == "acme", false, usize::MAX);
    let t0 = std::time::Instant::now();
    let out = loop {
        let out = run(
            fx.ctx(TenantScope::One("acme".into())),
            "SELECT id FROM sys_invocation ORDER BY created_at DESC, id",
            Limits::default(),
        )
        .await?;
        let got = strings(&out.batches, 0);
        if got == want {
            break out;
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "fallback rows differ from the oracle: {} rows ≠ {}",
            got.len(),
            want.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    ensure!(
        out.warnings.iter().any(|w| w.reason == "indexing"),
        "{:?}",
        out.warnings
    );

    let budget = 50;
    let ctx = fx.ctx(TenantScope::All);
    let stats = ctx.stats.clone();
    let few_reads = Limits {
        read_budget: budget,
        ..Limits::default()
    };
    match run(ctx, "SELECT id FROM sys_invocation", few_reads).await {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!("read cap did not stop the fallback scan: {other:?}"),
    }
    let (_, rows, _) = stats.snapshot();
    ensure!(
        rows <= budget + 256,
        "the fallback scan read {rows} rows before the budget of {budget} stopped it"
    );
    Ok(())
}

/// **Scalar array-generating functions do not exist** (from a security
/// review): the **scalar** versions of `range`/`generate_series` allocate by
/// argument — `SELECT range(1, 500000000)` is ~4 GB outside the pool while
/// returning a single row; `array_repeat`/`array_resize` likewise. Today they
/// are not registered (DataFusion is built without the `nested_expressions`
/// feature); the test pins that, including the `list_*` aliases, in case the
/// feature is ever turned on.
#[tokio::test]
async fn scalar_range_is_locked() -> Result<()> {
    let fx = fixture(2, mixed).await?;
    for sql in [
        "SELECT range(1, 10)",
        "SELECT range(10)",
        "SELECT generate_series(1, 10)",
        "SELECT array_repeat(1, 10)",
        "SELECT list_repeat(1, 10)",
        "SELECT array_resize(make_array(1), 10)",
        "SELECT list_resize(make_array(1), 10)",
    ] {
        match run(fx.ctx(TenantScope::All), sql, Limits::default()).await {
            Err(e) if e.is_client() => {}
            other => anyhow::bail!("{sql}: {other:?}"),
        }
    }
    Ok(())
}

/// Three 100-row `VALUES` tables cross-joined — 10⁶ rows out, in blocks of 256 rows.
fn cross_join_100_cubed() -> String {
    let values = (1..=100)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT a.x, b.x, c.x FROM (VALUES {values}) a(x), (VALUES {values}) b(x), \
         (VALUES {values}) c(x)"
    )
}

/// **The row cap stops the statement before results are collected** (from a
/// security review): the old version `collect()`ed the whole result before
/// counting, so the row cap could not bound memory. Must see: `Budget` when
/// `max_rows` is exceeded — stopping at row `max_rows + 1`, without counting
/// all 10⁶ rows.
#[tokio::test]
async fn rows_cap_stops_before_materialising() -> Result<()> {
    let fx = fixture(2, mixed).await?;
    let limits = Limits {
        max_rows: 10,
        ..Limits::default()
    };
    match run(fx.ctx(TenantScope::All), &cross_join_100_cubed(), limits).await {
        Err(QueryError::Budget(m)) if !m.contains("1000000") => {}
        other => anyhow::bail!("row cap must stop before collecting: {other:?}"),
    }
    Ok(())
}

/// **Results count against the statement's pool** (from a security review):
/// the old version collected results outside the pool, so the memory cap did
/// not stop a statement with little computation but many output rows. Must
/// see: 10⁶ rows × 3 columns (~24 MB) with a 256 KiB pool ⇒ `Budget`, even
/// though `max_rows` is wide enough.
#[tokio::test]
async fn result_counts_against_the_pool() -> Result<()> {
    let fx = fixture(2, mixed).await?;
    let limits = Limits {
        memory_bytes: 256 << 10,
        max_rows: 2_000_000,
        ..Limits::default()
    };
    match run(fx.ctx(TenantScope::All), &cross_join_100_cubed(), limits).await {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!(
            "results must count against the pool: {:?}",
            other.map(|o| o.batches.iter().map(|b| b.num_rows()).sum::<usize>())
        ),
    }
    Ok(())
}

/// One condition of the statement generator: the SQL and the matching oracle predicate.
type Cond = (String, Box<dyn Fn(&Truth) -> bool>);

/// Seed of the deterministic generator (xorshift).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[(self.next() % xs.len() as u64) as usize]
    }
}

/// **`/query` matches the exhaustive oracle** under a random statement
/// generator: every `Exact` form (=, IN, OR, time range, LIKE prefix, root),
/// plus a filter that cannot be pushed down (`<>`), on both axes and both
/// directions, with/without `LIMIT`, with both roles.
#[tokio::test]
async fn query_matches_oracle() -> Result<()> {
    let fx = fixture(30, mixed).await?;
    oracle_rounds(&fx, 200, |t| fx.ctx(t)).await
}

async fn oracle_rounds(
    fx: &Fx,
    rounds: usize,
    make: impl Fn(TenantScope) -> QueryCtx,
) -> Result<()> {
    let mut rng = Rng(0x5EED_CAFE);
    let mid = fx.truth[fx.truth.len() / 2].created;
    for round in 0..rounds {
        let mut conds: Vec<Cond> = Vec::new();
        for _ in 0..(rng.next() % 3) {
            let c: Cond = match rng.next() % 9 {
                0 => {
                    let s = rng.pick(&["Svc0", "Svc1", "Svc2"]).to_string();
                    (
                        format!("service = '{s}'"),
                        Box::new(move |t: &Truth| t.service == s),
                    )
                }
                1 => (
                    "class IN ('failed', 'paused')".into(),
                    Box::new(|t: &Truth| matches!(t.class, Class::Failed | Class::Paused)),
                ),
                2 => (
                    "(handler = 'h0' OR handler = 'h1')".into(),
                    Box::new(|_: &Truth| true),
                ),
                3 => (
                    format!("created_at >= to_timestamp_millis({mid})"),
                    Box::new(move |t: &Truth| t.created >= mid),
                ),
                4 => (
                    format!("transitioned_at < to_timestamp_millis({mid})"),
                    Box::new(move |t: &Truth| t.t < mid),
                ),
                5 => (
                    "service LIKE 'Svc1%'".into(),
                    Box::new(|t: &Truth| t.service.starts_with("Svc1")),
                ),
                6 => ("root = true".into(), Box::new(|t: &Truth| t.root)),
                7 => (
                    "handler <> 'h0'".into(),
                    Box::new(|t: &Truth| t.handler != "h0"),
                ),
                _ => {
                    let c = *rng.pick(&Class::ALL);
                    (
                        format!("class = '{}'", c.name()),
                        Box::new(move |t: &Truth| t.class == c),
                    )
                }
            };
            conds.push(c);
        }
        let transition = rng.next().is_multiple_of(3);
        let desc = !rng.next().is_multiple_of(4);
        let limit = *rng.pick(&[None, Some(1), Some(7), Some(40)]);
        let tenant = if rng.next().is_multiple_of(2) {
            TenantScope::All
        } else {
            TenantScope::One("acme".into())
        };
        let time = if transition {
            "transitioned_at"
        } else {
            "created_at"
        };
        let (dir, idir) = if desc {
            ("DESC", "ASC")
        } else {
            ("ASC", "DESC")
        };
        let mut sql = "SELECT id FROM sys_invocation".to_string();
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(
                &conds
                    .iter()
                    .map(|(s, _)| s.as_str())
                    .collect::<Vec<_>>()
                    .join(" AND "),
            );
        }
        sql.push_str(&format!(" ORDER BY {time} {dir}, id {idir}"));
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        let only = match &tenant {
            TenantScope::One(t) => Some(t.clone()),
            TenantScope::All => None,
        };
        let mut want = oracle(
            &fx.truth,
            |t| only.as_ref().is_none_or(|o| *o == t.tenant) && conds.iter().all(|(_, f)| f(t)),
            transition,
            usize::MAX,
        );
        if !desc {
            want.reverse();
        }
        if let Some(n) = limit {
            want.truncate(n);
        }
        let out = run(make(tenant), &sql, Limits::default()).await?;
        let got = strings(&out.batches, 0);
        ensure!(
            got == want,
            "round {round}: {sql}\n got {got:?}\n want {want:?}"
        );
        ensure!(!out.partial, "round {round}: {:?}", out.warnings);
    }
    Ok(())
}

// ─────────────────────────────────────────────────────── remote scans

/// A fake in-process remote node: serves ranges 2–3 with the very
/// `cairn_query::remote::serve`, requests and responses going through JSON.
struct InProc {
    ranges: Vec<u32>,
    handles: Vec<RangeReadHandle>,
    mode: Mode,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Up,
    Down,
    /// The coordinating node carries a newer descriptor version (rolling upgrade).
    NewerIndex,
    /// The remote node takes a snapshot and then hangs — like a long scan cancelled midway.
    Hang,
}

#[async_trait::async_trait]
impl RemoteScan for InProc {
    fn node(&self) -> &str {
        "node-b"
    }
    fn ranges(&self) -> &[u32] {
        &self.ranges
    }
    async fn scan(&self, mut req: ScanRequest) -> Result<ScanResponse, QueryError> {
        let wire = |e: serde_json::Error| QueryError::Internal(e.to_string());
        match self.mode {
            Mode::Down => return Err(QueryError::Internal("connection lost".into())),
            Mode::Hang => {
                let _held: Vec<_> = self
                    .handles
                    .iter()
                    .map(|h| h.snapshot())
                    .collect::<Result<_, _>>()
                    .map_err(QueryError::from)?;
                std::future::pending::<()>().await;
            }
            Mode::NewerIndex => {
                if let Scan::Invocation {
                    path: WirePath::Index { version, .. },
                    ..
                } = &mut req.scan
                {
                    *version += 1;
                }
            }
            Mode::Up => {}
        }
        let req: ScanRequest =
            serde_json::from_str(&serde_json::to_string(&req).map_err(wire)?).map_err(wire)?;
        let resp = serve(self.handles.clone(), req).await?;
        serde_json::from_str(&serde_json::to_string(&resp).map_err(wire)?).map_err(wire)
    }
}

impl Fx {
    /// Ranges 0–1 local, ranges 2–3 on "node-b".
    fn split_ctx(&self, tenant: TenantScope, mode: Mode) -> QueryCtx {
        let h: Vec<RangeReadHandle> = self.engines.iter().map(Engine::read_handle).collect();
        let remote = InProc {
            ranges: h[2..].iter().map(|h| h.range().id.0).collect(),
            handles: h[2..].to_vec(),
            mode,
        };
        QueryCtx::new(tenant, h[..2].to_vec()).with_remotes(vec![Arc::new(remote)])
    }
}

/// **A statement spanning several nodes** matches the exhaustive oracle — the
/// same statement generator, half the ranges on a remote node; the counters
/// add up both sides.
#[tokio::test]
async fn query_spans_nodes_in_process() -> Result<()> {
    let fx = fixture(30, mixed).await?;
    oracle_rounds(&fx, 80, |t| fx.split_ctx(t, Mode::Up)).await?;
    let sql = "SELECT class, sum(count) FROM sys_invocation_counts GROUP BY class ORDER BY class";
    let all = run(fx.ctx(TenantScope::All), sql, Limits::default()).await?;
    let split = run(
        fx.split_ctx(TenantScope::All, Mode::Up),
        sql,
        Limits::default(),
    )
    .await?;
    ensure!(strings(&all.batches, 0) == strings(&split.batches, 0));
    ensure!(format!("{:?}", all.batches) == format!("{:?}", split.batches));
    Ok(())
}

/// **A remote node that does not answer ⇒ `partial` + the range name**, the local part is still right.
#[tokio::test]
async fn remote_down_is_partial() -> Result<()> {
    let fx = fixture(10, mixed).await?;
    let out = run(
        fx.split_ctx(TenantScope::All, Mode::Down),
        "SELECT id, range FROM sys_invocation ORDER BY created_at DESC, id",
        Limits::default(),
    )
    .await?;
    ensure!(out.partial);
    let mut ranges: Vec<u32> = out.warnings.iter().filter_map(|w| w.range).collect();
    ranges.sort();
    let far: Vec<u32> = fx.engines[2..]
        .iter()
        .map(|e| e.read_handle().range().id.0)
        .collect();
    ensure!(ranges == far, "{:?}", out.warnings);
    let near: Vec<u32> = fx.engines[..2]
        .iter()
        .map(|e| e.read_handle().range().id.0)
        .collect();
    let got: Vec<u32> = out
        .batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(1)
                .as_primitive::<datafusion::arrow::datatypes::UInt32Type>();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    ensure!(!got.is_empty() && got.iter().all(|r| near.contains(r)));
    Ok(())
}

/// **Descriptor versions differ between nodes ⇒ `indexing`, results still
/// right**: the remote node does not use an index it does not have in the right
/// version; it scans metadata instead.
#[tokio::test]
async fn mixed_index_versions_report_indexing() -> Result<()> {
    let fx = fixture(10, mixed).await?;
    let out = run(
        fx.split_ctx(TenantScope::All, Mode::NewerIndex),
        LIST,
        Limits::default(),
    )
    .await?;
    let want = oracle(&fx.truth, |_| true, false, 20);
    ensure!(strings(&out.batches, 0) == want);
    let far: Vec<u32> = fx.engines[2..]
        .iter()
        .map(|e| e.read_handle().range().id.0)
        .collect();
    for r in far {
        ensure!(
            out.warnings.contains(&Warning {
                range: Some(r),
                reason: "indexing".into()
            }),
            "{:?}",
            out.warnings
        );
    }
    Ok(())
}

// ─────────────────────────────────────────────────────── keyed tables

/// **An unkeyed block table is refused**: a statement that does not name
/// exactly one entity is the caller's error, and never scans a whole range.
#[tokio::test]
async fn bulk_table_without_key_is_refused() -> Result<()> {
    let fx = fixture(2, mixed).await?;
    for sql in [
        "SELECT * FROM sys_journal",
        "SELECT * FROM sys_journal WHERE index = 0",
        "SELECT * FROM sys_state WHERE service = 'Cart'",
        "SELECT * FROM sys_promise WHERE key = 'k'",
        "SELECT * FROM sys_artifact",
        "SELECT * FROM sys_log WHERE kind = 'data'",
        "SELECT * FROM sys_vqueue_entry WHERE key = 'o0'",
    ] {
        match run(fx.ctx(TenantScope::All), sql, Limits::default()).await {
            Err(QueryError::Sql(m)) if m.contains("requires a filter") => {}
            other => anyhow::bail!("{sql}: {other:?}"),
        }
    }
    Ok(())
}

fn ints(b: &[RecordBatch], col: usize) -> Vec<i64> {
    b.iter()
        .flat_map(|b| {
            let a = b
                .column(col)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// **`sys_journal` matches the engine's journal** — a local range, a range on a
/// remote node, and through a `JOIN` from `sys_invocation` (the key derived
/// through the join condition). The `acme` key cannot read `zeta`'s journal.
#[tokio::test]
async fn journal_matches_engine() -> Result<()> {
    let fx = fixture(6, mixed).await?;
    for t in fx
        .truth
        .iter()
        .filter(|t| t.class == Class::Completed)
        .take(4)
    {
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        let e = fx
            .engines
            .iter()
            .find(|e| e.owns_id(&id))
            .expect("has an owner range");
        let want: Vec<i64> = e
            .journal_entries(&id)
            .await?
            .iter()
            .map(|(i, _, _)| i64::from(*i))
            .collect();
        ensure!(want.len() >= 2, "journal too short: {want:?}");
        let sql = format!(
            "SELECT index, ty, payload_len FROM sys_journal WHERE id = '{}' ORDER BY index",
            t.id
        );
        for (how, ctx) in [
            ("local", fx.ctx(TenantScope::All)),
            ("half remote", fx.split_ctx(TenantScope::All, Mode::Up)),
        ] {
            let out = run(ctx, &sql, Limits::default()).await?;
            ensure!(
                ints(&out.batches, 0) == want,
                "{how}: {:?} ≠ {want:?}",
                ints(&out.batches, 0)
            );
            ensure!(!out.partial, "{how}: {:?}", out.warnings);
        }
        let join = format!(
            "SELECT i.id, count(*) FROM sys_invocation i JOIN sys_journal j ON j.id = i.id \
             WHERE i.id = '{}' GROUP BY i.id",
            t.id
        );
        let out = run(fx.ctx(TenantScope::All), &join, Limits::default()).await?;
        ensure!(
            out.batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 1,
            "JOIN: {:?}",
            out.batches
        );
        let other = if t.tenant == "acme" { "zeta" } else { "acme" };
        let out = run(
            fx.ctx(TenantScope::One(other.into())),
            &sql,
            Limits::default(),
        )
        .await?;
        ensure!(
            out.batches.iter().all(|b| b.num_rows() == 0),
            "tenant leaked"
        );
    }
    Ok(())
}

/// **`sys_state` of an object**: two state keys written directly in the owner
/// range; a statement by `service + key` reads exactly them, another tenant
/// does not see them.
#[tokio::test]
async fn state_reads_one_object() -> Result<()> {
    let mut fx = fixture(1, mixed).await?;
    let tenant = TenantId::new("acme").unwrap();
    let pk = cairn_store::keys::object_owner(&tenant, None, "Cart", "c-1").unwrap();
    let e = fx
        .engines
        .iter_mut()
        .find(|e| e.owns_key(pk))
        .expect("has an owner range");
    let mut m = cairn_store::Mutations::new();
    m.set_state(pk, &tenant, None, "Cart", "c-1", b"items", b"[1,2]")
        .set_state(pk, &tenant, None, "Cart", "c-1", b"owner", b"\"an\"");
    e.writer_mut().commit(m).await?;
    let sql = "SELECT state_key, value, value_len FROM sys_state \
               WHERE service = 'Cart' AND key = 'c-1' ORDER BY state_key";
    let t0 = std::time::Instant::now();
    loop {
        let out = run(
            fx.ctx(TenantScope::One("acme".into())),
            sql,
            Limits::default(),
        )
        .await?;
        if strings(&out.batches, 0) == vec!["items".to_string(), "owner".to_string()] {
            ensure!(strings(&out.batches, 1) == vec!["[1,2]".to_string(), "\"an\"".to_string()]);
            break;
        }
        ensure!(t0.elapsed() < Duration::from_secs(10), "{:?}", out.batches);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let out = run(
        fx.ctx(TenantScope::One("zeta".into())),
        sql,
        Limits::default(),
    )
    .await?;
    ensure!(
        out.batches.iter().all(|b| b.num_rows() == 0),
        "tenant leaked"
    );
    // `cluster-admin` only with an explicit tenant.
    let out = run(
        fx.split_ctx(TenantScope::All, Mode::Up),
        "SELECT state_key FROM sys_state WHERE tenant = 'acme' AND service = 'Cart' AND key = 'c-1'",
        Limits::default(),
    )
    .await?;
    ensure!(strings(&out.batches, 0).len() == 2, "{:?}", out.batches);
    Ok(())
}

/// **`sys_state` also reads segmented values**: a `SegmentedState` row shows
/// up with `value = NULL`, `value_len` = the logical length, merged in key
/// order with the `State` rows.
#[tokio::test]
async fn state_reads_segmented_values_with_logical_len() -> Result<()> {
    let mut fx = fixture(1, mixed).await?;
    let tenant = TenantId::new("acme").unwrap();
    let pk = cairn_store::keys::object_owner(&tenant, None, "Chat", "c-1").unwrap();
    let e = fx
        .engines
        .iter_mut()
        .find(|e| e.owns_key(pk))
        .expect("has an owner range");
    let row = cairn_store::value::SegmentedRow {
        len: 3 << 19,
        refs: vec![cairn_store::value::SegRef {
            sha: [1; 32],
            gen: 1,
            len: (3 << 19) - 2,
        }],
        tail: bytes::Bytes::from_static(b"]}"),
    };
    let mut m = cairn_store::Mutations::new();
    m.set_state(pk, &tenant, None, "Chat", "c-1", b"a", b"1")
        .put_segmented_state(pk, &tenant, None, "Chat", "c-1", b"messages", &row)
        .set_state(pk, &tenant, None, "Chat", "c-1", b"z", b"2");
    e.writer_mut().commit(m).await?;
    let sql = "SELECT state_key, value, value_len FROM sys_state \
               WHERE service = 'Chat' AND key = 'c-1'";
    let t0 = std::time::Instant::now();
    loop {
        let out = run(
            fx.ctx(TenantScope::One("acme".into())),
            sql,
            Limits::default(),
        )
        .await?;
        let keys = strings(&out.batches, 0);
        if keys.len() == 3 {
            ensure!(keys == vec!["a", "messages", "z"], "key order: {keys:?}");
            let nulls: Vec<bool> = out
                .batches
                .iter()
                .flat_map(|b| {
                    (0..b.num_rows())
                        .map(|i| b.column(1).is_null(i))
                        .collect::<Vec<_>>()
                })
                .collect();
            ensure!(
                nulls == vec![false, true, false],
                "value NULL only on segmented rows: {nulls:?}"
            );
            let lens = ints(&out.batches, 2);
            ensure!(lens == vec![1, 3 << 19, 1], "logical length: {lens:?}");
            break;
        }
        ensure!(t0.elapsed() < Duration::from_secs(10), "{:?}", out.batches);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────── UI questions
//
// Each UI question answered on the ranges is one test: the SQL statement the
// UI sends, compared with the exhaustive oracle over `truth`. Questions that
// need the server (view tables, REST) are in
// `cairn-server/tests/query_api.rs::ui_question_*`.

/// Every cell of every row, joined with `|`.
fn table(b: &[RecordBatch]) -> Vec<String> {
    use datafusion::arrow::util::display::array_value_to_string;
    b.iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| {
                (0..b.num_columns())
                    .map(|c| array_value_to_string(b.column(c), i).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect()
}

async fn ask(fx: &Fx, sql: &str) -> Result<Vec<RecordBatch>> {
    let out = run(fx.ctx(TenantScope::All), sql, Limits::default()).await?;
    ensure!(!out.partial, "{sql}: {:?}", out.warnings);
    Ok(out.batches)
}

async fn same_ids(fx: &Fx, sql: &str, want: Vec<String>) -> Result<()> {
    let got = strings(&ask(fx, sql).await?, 0);
    ensure!(got == want, "{sql}\n got {got:?}\n want {want:?}");
    Ok(())
}

async fn path_of(fx: &Fx, sql: &str) -> Result<String> {
    let plan = explain(fx.ctx(TenantScope::All), sql).await?;
    Ok(plan
        .split("path=")
        .nth(1)
        .and_then(|r| r.split(',').next())
        .unwrap_or_default()
        .to_string())
}

/// Question 1: how many invocations per class, by service.
#[tokio::test]
async fn ui_question_1() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let got = table(
        &ask(
            &fx,
            "SELECT service, class, sum(count) FROM sys_invocation_counts \
             GROUP BY service, class ORDER BY service, class",
        )
        .await?,
    );
    let mut want: std::collections::BTreeMap<(String, &str), u64> = Default::default();
    for t in &fx.truth {
        *want.entry((t.service.clone(), t.class.name())).or_default() += 1;
    }
    let want: Vec<String> = want
        .into_iter()
        .map(|((s, c), n)| format!("{s}|{c}|{n}"))
        .collect();
    ensure!(got == want, "{got:?} ≠ {want:?}");
    Ok(())
}

/// Question 4: the handlers failing most.
#[tokio::test]
async fn ui_question_4() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let got = table(
        &ask(
            &fx,
            "SELECT service, handler, sum(count) AS n FROM sys_invocation_counts \
             WHERE class = 'failed' GROUP BY service, handler \
             ORDER BY n DESC, service, handler LIMIT 3",
        )
        .await?,
    );
    let mut want: std::collections::BTreeMap<(String, String), u64> = Default::default();
    for t in fx.truth.iter().filter(|t| t.class == Class::Failed) {
        *want
            .entry((t.service.clone(), t.handler.clone()))
            .or_default() += 1;
    }
    let mut want: Vec<_> = want.into_iter().collect();
    want.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let want: Vec<String> = want
        .into_iter()
        .take(3)
        .map(|((s, h), n)| format!("{s}|{h}|{n}"))
        .collect();
    ensure!(got == want, "{got:?} ≠ {want:?}");
    Ok(())
}

/// Question 10: the latest.
#[tokio::test]
async fn ui_question_10() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    ensure!(path_of(&fx, LIST).await? == "ByCreated");
    same_ids(&fx, LIST, oracle(&fx.truth, |_| true, false, 20)).await
}

/// Question 11: roots only.
#[tokio::test]
async fn ui_question_11() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE root ORDER BY created_at DESC, id LIMIT 15",
        oracle(&fx.truth, |t| t.root, false, 15),
    )
    .await
}

/// Question 12: by service / handler — the `ByService` stream, handler filtered on the value.
#[tokio::test]
async fn ui_question_12() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let sql = "SELECT id FROM sys_invocation WHERE service = 'Svc1' AND handler = 'h0' \
               ORDER BY created_at DESC, id LIMIT 10";
    ensure!(path_of(&fx, sql).await? == "ByService");
    same_ids(
        &fx,
        sql,
        oracle(
            &fx.truth,
            |t| t.service == "Svc1" && t.handler == "h0",
            false,
            10,
        ),
    )
    .await
}

/// Question 13: by status.
#[tokio::test]
async fn ui_question_13() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE class = 'paused' ORDER BY created_at DESC, id LIMIT 10",
        oracle(&fx.truth, |t| t.class == Class::Paused, false, 10),
    )
    .await
}

/// Question 14: ordered by the latest status change.
#[tokio::test]
async fn ui_question_14() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let sql = "SELECT id FROM sys_invocation ORDER BY transitioned_at DESC, id LIMIT 12";
    ensure!(path_of(&fx, sql).await? == "ByTransition");
    same_ids(&fx, sql, oracle(&fx.truth, |_| true, true, 12)).await
}

/// Question 15: recent failures, filtered by failure time.
#[tokio::test]
async fn ui_question_15() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let mid = fx.truth[fx.truth.len() / 3].t;
    same_ids(
        &fx,
        &format!(
            "SELECT id FROM sys_invocation WHERE class = 'failed' \
             AND transitioned_at >= to_timestamp_millis({mid}) \
             ORDER BY transitioned_at DESC, id"
        ),
        oracle(
            &fx.truth,
            |t| t.class == Class::Failed && t.t >= mid,
            true,
            usize::MAX,
        ),
    )
    .await
}

/// Question 16: stuck, oldest first (reverse scan).
#[tokio::test]
async fn ui_question_16() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let sql = "SELECT id FROM sys_invocation WHERE class IN ('active', 'paused') \
               ORDER BY created_at ASC, id DESC LIMIT 5";
    let plan = explain(fx.ctx(TenantScope::All), sql).await?;
    ensure!(!plan.contains("SortExec"), "{plan}");
    let mut want = oracle(
        &fx.truth,
        |t| matches!(t.class, Class::Active | Class::Paused),
        false,
        usize::MAX,
    );
    want.reverse();
    want.truncate(5);
    same_ids(&fx, sql, want).await
}

/// Question 17: paused needing attention, latest to enter the class first.
#[tokio::test]
async fn ui_question_17() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE class = 'paused' ORDER BY transitioned_at DESC, id",
        oracle(&fx.truth, |t| t.class == Class::Paused, true, usize::MAX),
    )
    .await
}

/// Question 20: the invocations of one object.
#[tokio::test]
async fn ui_question_20() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE service = 'Svc0' AND key = 'o0' \
         ORDER BY created_at DESC, id",
        oracle(
            &fx.truth,
            |t| t.service == "Svc0" && t.key == "o0",
            false,
            usize::MAX,
        ),
    )
    .await
}

/// Question 22: by idempotency key.
#[tokio::test]
async fn ui_question_22() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE idempotency_key = 'idem-7'",
        oracle(
            &fx.truth,
            |t| t.idempotency_key.as_deref() == Some("idem-7"),
            false,
            usize::MAX,
        ),
    )
    .await
}

/// Question 24: children of X — `ByAttr[$parent]`.
#[tokio::test]
async fn ui_question_24() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let parent = id_hex(&[0xEE; 24]);
    let sql = format!(
        "SELECT id FROM sys_invocation WHERE parent_id = '{parent}' ORDER BY created_at DESC, id"
    );
    ensure!(path_of(&fx, &sql).await? == "ByAttr");
    same_ids(&fx, &sql, oracle(&fx.truth, |t| !t.root, false, usize::MAX)).await
}

/// Question 27: by user tag (JSON column).
#[tokio::test]
async fn ui_question_27() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE tags LIKE '%\"team\":\"a\"%' \
         ORDER BY created_at DESC, id",
        oracle(&fx.truth, |t| t.tagged, false, usize::MAX),
    )
    .await
}

/// Question 29: typing part of an id.
#[tokio::test]
async fn ui_question_29() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let t = &fx.truth[17];
    let prefix = &t.id[..20];
    same_ids(
        &fx,
        &format!("SELECT id FROM sys_invocation WHERE id LIKE '{prefix}%'"),
        oracle(&fx.truth, |x| x.id.starts_with(prefix), false, usize::MAX),
    )
    .await
}

/// Question 30: creation time range.
#[tokio::test]
async fn ui_question_30() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let (lo, hi) = (fx.truth[10].created, fx.truth[30].created);
    same_ids(
        &fx,
        &format!(
            "SELECT id FROM sys_invocation WHERE created_at >= to_timestamp_millis({lo}) \
             AND created_at < to_timestamp_millis({hi}) ORDER BY created_at DESC, id"
        ),
        oracle(
            &fx.truth,
            |t| t.created >= lo && t.created < hi,
            false,
            usize::MAX,
        ),
    )
    .await
}

/// Question 31: duration ≥ X.
#[tokio::test]
async fn ui_question_31() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    same_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE duration_ms >= 5 ORDER BY created_at DESC, id",
        oracle(
            &fx.truth,
            |t| t.class.is_ended() && t.t - t.created >= 5,
            false,
            usize::MAX,
        ),
    )
    .await
}

/// Question 33: counting with an arbitrary filter.
#[tokio::test]
async fn ui_question_33() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let got = table(
        &ask(
            &fx,
            "SELECT count(*) FROM (SELECT id FROM sys_invocation \
             WHERE class = 'completed' AND service = 'Svc2' LIMIT 10000)",
        )
        .await?,
    );
    let want = fx
        .truth
        .iter()
        .filter(|t| t.class == Class::Completed && t.service == "Svc2")
        .count();
    ensure!(got == vec![want.to_string()], "{got:?} ≠ {want}");
    Ok(())
}

/// Question 35: details of one invocation (status, timestamps, parent, tags).
#[tokio::test]
async fn ui_question_35() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let t = fx
        .truth
        .iter()
        .find(|t| !t.root && t.tagged)
        .expect("has a tagged child");
    let got = table(
        &ask(
            &fx,
            &format!(
                "SELECT class, root, parent_id, tags, created_at = to_timestamp_millis({}) \
                 FROM sys_invocation WHERE id = '{}'",
                t.created, t.id
            ),
        )
        .await?,
    );
    let want = format!(
        "{}|false|{}|{{\"team\":\"a\"}}|true",
        t.class.name(),
        id_hex(&[0xEE; 24])
    );
    ensure!(got == vec![want.clone()], "{got:?} ≠ {want}");
    Ok(())
}

/// **Cancelling a statement releases its snapshots**:
///
/// 1. Half the ranges on a fake remote node; the remote node takes a snapshot
///    of its ranges and then hangs.
/// 2. The statement has a 300 ms deadline; past it ⇒ `query_budget`, and the
///    statement's future is dropped together with the remote node's future
///    (like a cancelled HTTP request).
///
/// Must see: after the statement, no handle still has a live snapshot — neither
/// the snapshot the remote node holds nor those of the local ranges scanned.
#[tokio::test]
async fn query_cancel_releases_snapshot() -> Result<()> {
    let fx = fixture(10, mixed).await?;
    let handles: Vec<RangeReadHandle> = fx.engines.iter().map(Engine::read_handle).collect();
    let limits = Limits {
        deadline: Duration::from_millis(300),
        ..Limits::default()
    };
    let ctx = fx.split_ctx(TenantScope::All, Mode::Hang);
    // The handles in `split_ctx` are copies sharing counters with `handles`.
    match run(ctx, LIST, limits).await {
        Err(QueryError::Budget(_)) => {}
        other => anyhow::bail!("statement must time out: {other:?}"),
    }
    for h in &handles {
        ensure!(
            h.live_snapshots() == 0,
            "range {} still has {} snapshots",
            h.range().id.0,
            h.live_snapshots()
        );
    }
    // A normal statement leaves no snapshot behind either.
    run(fx.ctx(TenantScope::All), LIST, Limits::default()).await?;
    ensure!(handles.iter().all(|h| h.live_snapshots() == 0));
    Ok(())
}

/// Question 41: who holds a slot, who waits — the entries of one queue
/// (`sys_vqueue_entry`). Unfinished invocations are still in the queue;
/// finished ones have been withdrawn.
#[tokio::test]
async fn ui_question_41() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let got = strings(
        &ask(
            &fx,
            "SELECT id FROM sys_vqueue_entry WHERE tenant = 'acme' AND service = 'Svc0' ORDER BY id",
        )
        .await?,
        0,
    );
    let mut want: Vec<String> = fx
        .truth
        .iter()
        .filter(|t| t.tenant == "acme" && t.service == "Svc0" && !t.class.is_ended())
        .map(|t| t.id.clone())
        .collect();
    want.sort();
    ensure!(!want.is_empty() && got == want, "{got:?} ≠ {want:?}");
    // Another tenant does not see `acme`'s queue.
    let out = run(
        fx.ctx(TenantScope::One("zeta".into())),
        "SELECT id FROM sys_vqueue_entry WHERE tenant = 'acme' AND service = 'Svc0'",
        Limits::default(),
    )
    .await?;
    ensure!(out.batches.iter().all(|b| b.num_rows() == 0));
    Ok(())
}

/// Question 19: delayed calls not yet due — the table runs without a key (the
/// fixture has no delayed calls: empty).
#[tokio::test]
async fn ui_question_19() -> Result<()> {
    let fx = fixture(4, mixed).await?;
    let out = ask(
        &fx,
        "SELECT caller_id, due_at_ms FROM sys_delayed ORDER BY due_at_ms",
    )
    .await?;
    ensure!(out.iter().all(|b| b.num_rows() == 0));
    Ok(())
}

/// **Keyed tables**: timers, streams, stream records, promise waiters — rows
/// written directly in the owner range of a real invocation/object, read back
/// correctly through SQL; another tenant does not see them.
#[tokio::test]
async fn a2_keyed_tables_read_their_rows() -> Result<()> {
    use cairn_store::keys::{ChunkBody, ChunkRecord, StreamChunk};
    let mut fx = fixture(2, mixed).await?;
    let t = fx
        .truth
        .iter()
        .find(|t| t.tenant == "acme")
        .unwrap()
        .clone();
    let id = cairn_store::id_from_hex(&t.id).unwrap();
    let other_hex = fx.truth.iter().find(|o| o.id != t.id).unwrap().id.clone();
    let other = cairn_store::id_from_hex(&other_hex).unwrap();
    let tenant = TenantId::new("acme").unwrap();
    let pk = cairn_store::keys::object_owner(&tenant, None, "Flow", "w-1").unwrap();
    let rec = |offset: u64, body: &'static [u8]| ChunkRecord {
        offset,
        step: 1,
        attempt: 0,
        k: offset as u32,
        body: ChunkBody::Data(Bytes::from_static(body)),
    };
    for e in fx.engines.iter_mut() {
        if !e.owns_id(&id) && !e.owns_key(pk) {
            continue;
        }
        let mut m = cairn_store::Mutations::new();
        if e.owns_id(&id) {
            let span = |offset: u64, v: serde_json::Value| ChunkRecord {
                offset,
                step: 2,
                attempt: 0,
                k: offset as u32,
                body: ChunkBody::Data(Bytes::from(v.to_string())),
            };
            m.put_stream_chunk(
                &id,
                "$trace",
                0,
                &StreamChunk {
                    total_bytes: 0,
                    dropped: 0,
                    records: vec![
                        span(0, serde_json::json!({"kind": "start", "span_id": "a", "name": "agent", "ts": 1})),
                        span(1, serde_json::json!({"kind": "start", "span_id": "b", "parent_id": "a", "name": "llm", "ts": 2})),
                        span(2, serde_json::json!({"kind": "end", "span_id": "b", "ts": 5, "status": "ok", "attrs": {"input_tokens": 10, "output_tokens": 3}})),
                    ],
                },
            );
            let watch = cairn_engine::OutboxEntry::Attach(cairn_engine::OutboundWatch {
                caller: id,
                completion_id: 4,
                target: other,
            })
            .encode();
            m.push_outbox(cairn_store::partition_key_of(&id), 1, &id, 4, &watch)
                .push_signal_outbox(2, &id, 5, b"sig")
                .push_inbound(
                    cairn_store::partition_key_of(&id),
                    3,
                    cairn_store::keys::InboundKind::Completion,
                    &other,
                    11,
                    b"12345",
                )
                .add_attachment(&id, &other, 12);
            m.add_timer(T0 + 60_000, &id, 7, b"").put_stream_chunk(
                &id,
                "out",
                0,
                &StreamChunk {
                    total_bytes: 6,
                    dropped: 0,
                    records: vec![rec(0, b"abc"), rec(1, b"def")],
                },
            );
        }
        if e.owns_key(pk) {
            m.add_promise_waiter(pk, &tenant, None, "Flow", "w-1", "approval", &id, 9);
        }
        e.writer_mut().commit(m).await?;
    }
    let acme = || fx.ctx(TenantScope::One("acme".into()));
    let until = |sql: String, want: Vec<String>| async move {
        let t0 = std::time::Instant::now();
        loop {
            let out = run(acme(), &sql, Limits::default()).await?;
            let got = table(&out.batches);
            if got == want {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "{sql}: {got:?} ≠ {want:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    until(
        format!(
            "SELECT completion_id, due_at_ms FROM sys_timer WHERE id = '{}'",
            t.id
        ),
        vec![format!("7|{}", T0 + 60_000)],
    )
    .await?;
    until(
        format!(
            "SELECT name, records, total_bytes, last_offset FROM sys_stream WHERE id = '{}' ORDER BY name",
            t.id
        ),
        vec!["$trace|3|0|2".into(), "out|2|6|1".into()],
    )
    .await?;
    until(
        format!(
            "SELECT offset, kind, body FROM sys_stream_record WHERE id = '{}' AND name = 'out' ORDER BY offset",
            t.id
        ),
        vec!["0|data|abc".into(), "1|data|def".into()],
    )
    .await?;
    until(
        "SELECT name, waiter_id, completion_id FROM sys_promise_waiter WHERE service = 'Flow' AND key = 'w-1'"
            .into(),
        vec![format!("approval|{}|9", t.id)],
    )
    .await?;
    until(
        format!(
            "SELECT seq, kind, completion_id, target FROM sys_outbox WHERE id = '{}' ORDER BY seq",
            t.id
        ),
        vec![format!("1|attach|4|{other_hex}"), "2|signal|5|".into()],
    )
    .await?;
    until(
        format!(
            "SELECT caller_id, completion_id FROM sys_attachment WHERE id = '{}'",
            t.id
        ),
        vec![format!("{other_hex}|12")],
    )
    .await?;
    until(
        format!(
            "SELECT span_id, parent_id, name, end_ts, unfinished, input_tokens FROM sys_span WHERE id = '{}' ORDER BY span_id",
            t.id
        ),
        vec!["a||agent||true|".into(), "b|a|llm|5|false|10".into()],
    )
    .await?;
    // No `id` filter: cluster-admin only.
    let refused = run(acme(), "SELECT * FROM sys_inbound", Limits::default()).await;
    ensure!(
        matches!(&refused, Err(e) if e.is_client()),
        "acme can read sys_inbound: {:?}",
        refused.map(|o| table(&o.batches))
    );
    let refused = run(acme(), "SELECT * FROM sys_outbox", Limits::default()).await;
    ensure!(
        matches!(&refused, Err(e) if e.is_client()),
        "acme can scan sys_outbox"
    );
    let all = run(
        fx.ctx(TenantScope::All),
        "SELECT kind, id, completion_id, bytes FROM sys_inbound",
        Limits::default(),
    )
    .await?;
    ensure!(
        table(&all.batches) == vec![format!("completion|{other_hex}|11|5")],
        "{:?}",
        table(&all.batches)
    );
    let all = run(
        fx.ctx(TenantScope::All),
        &format!(
            "SELECT kind, completion_id FROM sys_outbox WHERE id <> '' AND substr(id, 1, 48) = '{}' ORDER BY seq",
            t.id
        ),
        Limits::default(),
    )
    .await?;
    ensure!(
        table(&all.batches) == vec!["attach|4".to_string(), "signal|5".into()],
        "{:?}",
        table(&all.batches)
    );
    let zeta = run(
        fx.ctx(TenantScope::One("zeta".into())),
        &format!("SELECT * FROM sys_timer WHERE id = '{}'", t.id),
        Limits::default(),
    )
    .await?;
    ensure!(
        zeta.batches.iter().all(|b| b.num_rows() == 0),
        "tenant leaked"
    );
    for tbl in ["sys_outbox", "sys_attachment", "sys_span"] {
        let zeta = run(
            fx.ctx(TenantScope::One("zeta".into())),
            &format!("SELECT * FROM {tbl} WHERE id = '{}'", t.id),
            Limits::default(),
        )
        .await?;
        ensure!(
            zeta.batches.iter().all(|b| b.num_rows() == 0),
            "{tbl}: tenant leaked"
        );
    }
    Ok(())
}

/// **Detail columns of `sys_invocation`** (UI question 18):
/// `attempt`/`retry_at`, `last_failure_*`, `purge_at`, `hash_chain_len`,
/// `pinned_deployment` read the id's own rows; `failure_*` of the
/// `failed`/`paused` classes; a row without a detail row is NULL. A list page
/// that does not project them costs no extra reads.
#[tokio::test]
async fn ui_question_18() -> Result<()> {
    let mut fx = fixture(2, mixed).await?;
    let t = fx
        .truth
        .iter()
        .find(|t| t.tenant == "acme")
        .unwrap()
        .clone();
    let id = cairn_store::id_from_hex(&t.id).unwrap();
    for e in fx.engines.iter_mut() {
        if !e.owns_id(&id) {
            continue;
        }
        let mut m = cairn_store::Mutations::new();
        m.add_retry(
            T0 + 90_000,
            &id,
            cairn_lifecycle::RetryRow::retry(T0 + 90_000, 3, 7, 0)
                .encode()
                .as_slice(),
        )
        .set_last_failure(
            &id,
            &cairn_store::keys::LastFailure::new(503, "transient", 2, T0 + 80_000),
        )
        .add_purge(T0 + 86_400_000, &id)
        .set_chain(&id, 4, &[9; 32])
        .pin_deployment(&id, "dp_next");
        e.writer_mut().commit(m).await?;
    }
    let sql = format!(
        "SELECT attempt, CAST(retry_at AS BIGINT), last_failure_code, last_failure_message, \
         CAST(last_failure_at AS BIGINT), CAST(purge_at AS BIGINT), hash_chain_len, pinned_deployment \
         FROM sys_invocation WHERE id = '{}'",
        t.id
    );
    let want = vec![format!(
        "3|{}|503|transient|{}|{}|4|dp_next",
        T0 + 90_000,
        T0 + 80_000,
        T0 + 86_400_000
    )];
    let t0 = std::time::Instant::now();
    loop {
        let out = run(
            fx.ctx(TenantScope::One("acme".into())),
            &sql,
            Limits::default(),
        )
        .await?;
        let got = table(&out.batches);
        if got == want {
            break;
        }
        ensure!(t0.elapsed() < Duration::from_secs(10), "{got:?} ≠ {want:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The `failed` class carries an error code; a row without a detail row is NULL.
    let out = run(
        fx.ctx(TenantScope::All),
        "SELECT count(*), count(failure_code), count(attempt) FROM sys_invocation WHERE class = 'failed'",
        Limits::default(),
    )
    .await?;
    let failed = fx.truth.iter().filter(|t| t.class == Class::Failed).count();
    ensure!(failed > 0, "fixture has no failed invocation");
    ensure!(
        table(&out.batches) == vec![format!("{failed}|{failed}|0")],
        "{:?}",
        table(&out.batches)
    );
    // A page that does not project detail columns: read units unchanged when detail rows exist.
    let c = fx.ctx(TenantScope::All);
    let stats = c.stats.clone();
    run(
        c,
        "SELECT id FROM sys_invocation ORDER BY created_at DESC, id LIMIT 5",
        Limits::default(),
    )
    .await?;
    let plain = stats.units();
    let c = fx.ctx(TenantScope::All);
    let stats = c.stats.clone();
    run(
        c,
        "SELECT id, purge_at FROM sys_invocation ORDER BY created_at DESC, id LIMIT 5",
        Limits::default(),
    )
    .await?;
    ensure!(
        stats.units() == plain + 5,
        "projecting purge_at: {} units, not projecting: {plain}",
        stats.units()
    );
    Ok(())
}

/// **`sys_object`** (UI question 42): a service's objects from the `ObjectKeys`
/// hint index, checked at read time.
///
/// 1. `Cart/c-1`: two invocations (ids in the object's owner range) — `invocations = 2`.
/// 2. `Cart/c-2`: one invocation, then purged, only state left — `has_state`.
/// 3. `Cart/c-3`: one invocation, then purged, no state — the hint row is still
///    in the range but **hidden**.
/// 4. Missing `service` ⇒ refused; another tenant does not see them.
#[tokio::test]
async fn sys_object_lists_live_objects() -> Result<()> {
    let mut fx = fixture(1, mixed).await?;
    let tenant = TenantId::new("acme").unwrap();
    let mut n = 0u8;
    let mut make = |key: &str| {
        n += 1;
        let pk = cairn_store::keys::object_owner(&tenant, None, "Cart", key).unwrap();
        (pk, make_invocation_id(pk, [n; 16]))
    };
    let objs: Vec<(String, PartitionKey, cairn_store::InvocationId)> =
        [("c-1", 0), ("c-1", 1), ("c-2", 0), ("c-3", 0)]
            .iter()
            .map(|(k, _)| {
                let (pk, id) = make(k);
                (k.to_string(), pk, id)
            })
            .collect();
    for (key, pk, id) in &objs {
        let e = fx
            .engines
            .iter_mut()
            .find(|e| e.owns_key(*pk))
            .expect("has an owner range");
        let q = queue("acme", "Cart");
        let seq = e.next_queue_seq(&q).await?;
        e.create(
            *id,
            &Target {
                tenant: tenant.clone(),
                scope: None,
                service: "Cart".into(),
                handler: "add".into(),
                object_key: key.clone(),
            },
            CreateOptions {
                queue: Some((&q, seq)),
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
        if key == "c-2" {
            let mut m = cairn_store::Mutations::new();
            m.set_state(*pk, &tenant, None, "Cart", "c-2", b"items", b"[]");
            e.writer_mut().commit(m).await?;
        }
        if key != "c-1" {
            e.cancel(id, "purge").await?;
            e.purge_now(id).await?;
        }
    }
    let sql =
        "SELECT key, has_state, invocations FROM sys_object WHERE service = 'Cart' ORDER BY key";
    let want = vec!["c-1|false|2".to_string(), "c-2|true|0".into()];
    let t0 = std::time::Instant::now();
    loop {
        let out = run(
            fx.ctx(TenantScope::One("acme".into())),
            sql,
            Limits::default(),
        )
        .await?;
        let got = table(&out.batches);
        if got == want {
            break;
        }
        ensure!(t0.elapsed() < Duration::from_secs(10), "{got:?} ≠ {want:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let refused = run(
        fx.ctx(TenantScope::One("acme".into())),
        "SELECT * FROM sys_object",
        Limits::default(),
    )
    .await;
    ensure!(
        matches!(&refused, Err(e) if e.is_client()),
        "missing service was not refused"
    );
    let zeta = run(
        fx.ctx(TenantScope::One("zeta".into())),
        sql,
        Limits::default(),
    )
    .await?;
    ensure!(table(&zeta.batches).is_empty(), "tenant leaked");
    Ok(())
}

/// Adds an `active` invocation of tenant `acme`, service `Extra`, to range
/// `n % 4` — its own id, outside the fixture's ids — with optional scope,
/// deployment and schedule. Returns the hex id.
async fn add_extra(
    fx: &mut Fx,
    n: u64,
    scope: Option<&str>,
    deployment: Option<&str>,
    schedule: Option<&str>,
) -> Result<String> {
    let p = (n % 4) as usize;
    let range = ClusterConfig::equal(4)
        .range(PartitionId(p as u32))
        .expect("range");
    let mut unique = [0xF0u8; 16];
    unique[8..].copy_from_slice(&n.to_be_bytes());
    let id = make_invocation_id(PartitionKey(range.start + 1_000_000 + n), unique);
    let q = queue("acme", "Extra");
    let e = &mut fx.engines[p];
    let seq = e.next_queue_seq(&q).await?;
    let header = schedule.map(|s| cairn_engine::schedule::header_value(s, T0 + n));
    e.create(
        id,
        &Target {
            tenant: TenantId::new("acme").unwrap(),
            scope: scope.map(|s| cairn_store::Scope::new(s).unwrap()),
            service: "Extra".into(),
            handler: "run".into(),
            object_key: String::new(),
        },
        CreateOptions {
            queue: Some((&q, seq)),
            deployment,
            schedule: header.as_deref().map(|h| cairn_engine::ScheduleStamp {
                header: h,
                row: None,
            }),
            ..Default::default()
        },
        StartParams::default(),
        Bytes::new(),
    )
    .await?;
    Ok(id_hex(&id))
}

/// Asks until the result equals `want` (sorted) — new writes reach the `Remote` level after one tick.
async fn eventually_ids(fx: &Fx, sql: &str, mut want: Vec<String>) -> Result<()> {
    want.sort();
    let t0 = std::time::Instant::now();
    loop {
        let mut got = strings(&ask(fx, sql).await?, 0);
        got.sort();
        if got == want {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < Duration::from_secs(10),
            "{sql}\n got {got:?}\n want {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Question 21: the invocations of one scope. Three invocations in scope
/// `team-a` (spread over two ranges), one in another scope, one unscoped.
///
/// Must see: exactly the three ids of `team-a`. The `scope` filter is a
/// residual after the scan path (like questions 20/22: correct, but ranges are
/// not yet pruned by `H(scope)`).
#[tokio::test]
async fn ui_question_21() -> Result<()> {
    let mut fx = fixture(3, mixed).await?;
    let mut want = Vec::new();
    for n in [1, 2, 5] {
        want.push(add_extra(&mut fx, n, Some("team-a"), None, None).await?);
    }
    add_extra(&mut fx, 3, Some("team-b"), None, None).await?;
    add_extra(&mut fx, 4, None, None, None).await?;
    eventually_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE scope = 'team-a'",
        want,
    )
    .await
}

/// Question 26: the runs of one schedule. Two firings of `nightly` (two
/// ranges), one of `hourly`.
///
/// Must see: exactly two ids, and the scan path is `ByAttr` (seek
/// `$schedule`), not a full-table scan.
#[tokio::test]
async fn ui_question_26() -> Result<()> {
    let mut fx = fixture(3, mixed).await?;
    let mut want = Vec::new();
    for n in [11, 12] {
        want.push(add_extra(&mut fx, n, None, None, Some("nightly")).await?);
    }
    add_extra(&mut fx, 13, None, None, Some("hourly")).await?;
    let sql = "SELECT id FROM sys_invocation WHERE schedule = 'nightly'";
    eventually_ids(&fx, sql, want).await?;
    let path = path_of(&fx, sql).await?;
    ensure!(path == "ByAttr", "path {path}");
    Ok(())
}

/// Question 28: by deployment at creation. Two invocations of `dp_old`, one of
/// `dp_new`.
///
/// Must see: exactly two ids, path `ByAttr` (seek `$deployment`).
#[tokio::test]
async fn ui_question_28() -> Result<()> {
    let mut fx = fixture(3, mixed).await?;
    let mut want = Vec::new();
    for n in [21, 22] {
        want.push(add_extra(&mut fx, n, None, Some("dp_old"), None).await?);
    }
    add_extra(&mut fx, 23, None, Some("dp_new"), None).await?;
    let sql = "SELECT id FROM sys_invocation WHERE deployment = 'dp_old'";
    eventually_ids(&fx, sql, want).await?;
    let path = path_of(&fx, sql).await?;
    ensure!(path == "ByAttr", "path {path}");
    Ok(())
}

/// Question 32: an "at least" filter on journal length.
///
/// Must see: `journal_len` of every invocation equals `Engine::journal_len`
/// (a real count of the whole journal), and `WHERE journal_len >= 2` returns
/// exactly the set with ≥ 2 entries — one reverse scan fetching one key per
/// row, without reading the whole journal.
#[tokio::test]
async fn ui_question_32() -> Result<()> {
    let fx = fixture(3, mixed).await?;
    let mut lens = Vec::new();
    for t in &fx.truth {
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        let e = fx
            .engines
            .iter()
            .find(|e| e.owns_id(&id))
            .expect("owner range");
        lens.push((t.id.clone(), e.journal_len(&id).await?));
    }
    ensure!(
        lens.iter().any(|(_, n)| *n >= 2) && lens.iter().any(|(_, n)| *n < 2),
        "the fixture needs both long and short journals: {lens:?}"
    );
    let out = ask(&fx, "SELECT id, journal_len FROM sys_invocation").await?;
    let got: std::collections::BTreeMap<String, i64> =
        strings(&out, 0).into_iter().zip(ints(&out, 1)).collect();
    for (id, n) in &lens {
        ensure!(
            got.get(id) == Some(&i64::from(*n)),
            "{id}: {:?} ≠ {n}",
            got.get(id)
        );
    }
    let want: Vec<String> = lens
        .iter()
        .filter(|(_, n)| *n >= 2)
        .map(|(id, _)| id.clone())
        .collect();
    eventually_ids(
        &fx,
        "SELECT id FROM sys_invocation WHERE journal_len >= 2",
        want,
    )
    .await
}

/// **Bind parameters**: `$1` gives the same result **and the same read cost**
/// as the statement written with a literal — binding happens on the logical
/// plan before the optimiser, so the filter is still pushed down to the index.
/// A SQL-injection string is just a value that does not match. Missing, extra,
/// skipped or named parameters are the caller's error.
#[tokio::test]
async fn params_bind_like_literals() -> Result<()> {
    let fx = fixture(12, mixed).await?;
    let lit = run(
        fx.ctx(TenantScope::All),
        "SELECT id FROM sys_invocation WHERE service = 'Svc1' AND handler = 'h0' ORDER BY id",
        Limits::default(),
    )
    .await?;
    let bound = run_with_params(
        fx.ctx(TenantScope::All),
        "SELECT id FROM sys_invocation WHERE service = $1 AND handler = $2 ORDER BY id",
        &[Param::Str("Svc1".into()), Param::Str("h0".into())],
        Limits::default(),
    )
    .await?;
    let want = strings(&lit.batches, 0);
    ensure!(!want.is_empty(), "the fixture must have Svc1/h0");
    ensure!(
        strings(&bound.batches, 0) == want,
        "result differs from the literal"
    );
    ensure!(
        bound.read == lit.read,
        "read cost: parameter {} ≠ literal {}",
        bound.read,
        lit.read
    );

    let evil = run_with_params(
        fx.ctx(TenantScope::All),
        "SELECT id FROM sys_invocation WHERE service = $1",
        &[Param::Str("Svc1' OR '1'='1".into())],
        Limits::default(),
    )
    .await?;
    ensure!(
        evil.batches.iter().all(|b| b.num_rows() == 0),
        "parameter escaped the string"
    );

    let kinds = run_with_params(
        fx.ctx(TenantScope::All),
        "SELECT CAST($1 AS BIGINT) + 1 AS n, $2 AS b, $3 IS NULL AS z",
        &[Param::Int(41), Param::Bool(true), Param::Null],
        Limits::default(),
    )
    .await?;
    ensure!(
        table(&kinds.batches) == ["42|true|true"],
        "{:?}",
        table(&kinds.batches)
    );

    let one = || vec![Param::Str("Svc1".into())];
    for (sql, params) in [
        ("SELECT id FROM sys_invocation WHERE service = $1", vec![]),
        (
            "SELECT id FROM sys_invocation WHERE service = $1",
            vec![Param::Str("a".into()), Param::Str("b".into())],
        ),
        (
            "SELECT id FROM sys_invocation WHERE service = 'Svc1'",
            one(),
        ),
        ("SELECT id FROM sys_invocation WHERE service = $2", one()),
        ("SELECT id FROM sys_invocation WHERE service = $name", one()),
    ] {
        match run_with_params(fx.ctx(TenantScope::All), sql, &params, Limits::default()).await {
            Err(e) if e.is_client() => {}
            other => anyhow::bail!("{sql} {params:?}: {other:?}"),
        }
    }
    Ok(())
}

// ──────────────────────────────── remaining UI questions
//
// Keyed questions the console sends from the invocation details and
// Workflows/State. Besides the right rows, each test pins the **cost**: the
// statement's `read` is equal on fixtures of 2 and 8 invocations per range —
// the statement is a keyed read and does not grow with the bucket. Statements
// use bind parameters like the console.

/// One case of [`flat_cost`]: the fixture with rows added, the statement, parameters, expected rows.
type Case = (Fx, String, Vec<Param>, Vec<String>);

/// Runs `case` on two fixture sizes; asks until the rows are right; `read` must be equal.
async fn flat_cost<F, Fut>(case: F) -> Result<()>
where
    F: Fn(Fx) -> Fut,
    Fut: std::future::Future<Output = Result<Case>>,
{
    let mut reads = Vec::new();
    for n in [2, 8] {
        let (fx, sql, params, want) = case(fixture(n, mixed).await?).await?;
        let t0 = std::time::Instant::now();
        let read = loop {
            let out = run_with_params(
                fx.ctx(TenantScope::One("acme".into())),
                &sql,
                &params,
                Limits::default(),
            )
            .await?;
            if table(&out.batches) == want {
                ensure!(!out.partial, "{sql}: {:?}", out.warnings);
                break out.read;
            }
            ensure!(
                t0.elapsed() < Duration::from_secs(10),
                "{sql} {params:?}: {:?} ≠ {want:?}",
                table(&out.batches)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        reads.push(read);
    }
    ensure!(
        reads[0] > 0,
        "statement counted no read units — empty cost comparison"
    );
    ensure!(
        reads[0] == reads[1],
        "read cost grows with the data: {reads:?}"
    );
    Ok(())
}

/// Commits `m` into every range that `owns` picks.
async fn commit_where(
    fx: &mut Fx,
    owns: impl Fn(&Engine) -> bool,
    m: impl Fn() -> cairn_store::Mutations,
) -> Result<()> {
    for e in fx.engines.iter_mut().filter(|e| owns(e)) {
        e.writer_mut().commit(m()).await?;
    }
    Ok(())
}

/// Question 23: by workflow id. The run's id is derived from `(tenant, scope,
/// service, key)` (`workflow_invocation_id`), so the statement is a **point read**.
///
/// Must see: exactly one row, path `Point`, cost independent of size.
#[tokio::test]
async fn ui_question_23() -> Result<()> {
    flat_cost(|mut fx| async move {
        let tenant = TenantId::new("acme").unwrap();
        let pk = cairn_store::keys::object_owner(&tenant, None, "Wf", "w-1").unwrap();
        let id = cairn_store::workflow_invocation_id(pk, &tenant, None, "Wf", "w-1");
        let q = queue("acme", "Wf");
        let e = fx
            .engines
            .iter_mut()
            .find(|e| e.owns_key(pk))
            .expect("has an owner range");
        let seq = e.next_queue_seq(&q).await?;
        e.create(
            id,
            &Target {
                tenant,
                scope: None,
                service: "Wf".into(),
                handler: "run".into(),
                object_key: "w-1".into(),
            },
            CreateOptions {
                queue: Some((&q, seq)),
                kind: cairn_engine::InvocationKind::WorkflowRun,
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
        let hex = id_hex(&id);
        let sql = "SELECT id, service, key FROM sys_invocation WHERE id = $1".to_string();
        let plan = explain(
            fx.ctx(TenantScope::All),
            &format!("SELECT id FROM sys_invocation WHERE id = '{hex}'"),
        )
        .await?;
        ensure!(plan.contains("path=point"), "{plan}");
        Ok((
            fx,
            sql,
            vec![Param::Str(hex.clone())],
            vec![format!("{hex}|Wf|w-1")],
        ))
    })
    .await
}

/// Question 25: the whole tree from a root — `WHERE root_id = $1`, one
/// `ByAttr[$root]` seek per range. Three descendants of one root sit in three
/// different ranges.
#[tokio::test]
async fn ui_question_25() -> Result<()> {
    flat_cost(|mut fx| async move {
        let root_hex = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme")
            .unwrap()
            .id
            .clone();
        let root = cairn_store::id_from_hex(&root_hex).unwrap();
        let caller = ReplyTo {
            caller: root,
            completion_id: 1,
        };
        let mut want = Vec::new();
        for n in [31u64, 32, 33] {
            let p = (n % 4) as usize;
            let range = ClusterConfig::equal(4)
                .range(PartitionId(p as u32))
                .unwrap();
            let mut unique = [0xD0u8; 16];
            unique[8..].copy_from_slice(&n.to_be_bytes());
            let id = make_invocation_id(PartitionKey(range.start + 2_000_000 + n), unique);
            let q = queue("acme", "Tree");
            let e = &mut fx.engines[p];
            let seq = e.next_queue_seq(&q).await?;
            e.create(
                id,
                &Target {
                    tenant: TenantId::new("acme").unwrap(),
                    scope: None,
                    service: "Tree".into(),
                    handler: "run".into(),
                    object_key: String::new(),
                },
                CreateOptions {
                    reply_to: Some(&caller),
                    queue: Some((&q, seq)),
                    root_id: Some(root),
                    ..Default::default()
                },
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
            want.push(id_hex(&id));
        }
        want.sort();
        let plan = explain(
            fx.ctx(TenantScope::All),
            &format!("SELECT id FROM sys_invocation WHERE root_id = '{root_hex}'"),
        )
        .await?;
        ensure!(plan.contains("path=ByAttr"), "{plan}");
        Ok((
            fx,
            "SELECT id FROM sys_invocation WHERE root_id = $1 ORDER BY id".into(),
            vec![Param::Str(root_hex)],
            want,
        ))
    })
    .await
}

/// Question 36 (the read part): the journal of one invocation, `WHERE id = $1`
/// — a read by id prefix. "What is it waiting for" (entries without a
/// completion) needs decoded columns and is pinned elsewhere.
#[tokio::test]
async fn ui_question_36() -> Result<()> {
    flat_cost(|fx| async move {
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme" && t.class == Class::Completed)
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        let e = fx.engines.iter().find(|e| e.owns_id(&id)).unwrap();
        let want: Vec<String> = e
            .journal_entries(&id)
            .await?
            .iter()
            .map(|(i, _, _)| i.to_string())
            .collect();
        ensure!(want.len() >= 2, "{want:?}");
        Ok((
            fx,
            "SELECT index FROM sys_journal WHERE id = $1 ORDER BY index".into(),
            vec![Param::Str(t.id)],
            want,
        ))
    })
    .await
}

/// Question 37: the log of one invocation, in the console's polling form:
/// `WHERE id = $1 AND offset > $2` — only records after the offset already seen.
#[tokio::test]
async fn ui_question_37() -> Result<()> {
    use cairn_store::keys::{ChunkBody, ChunkRecord, StreamChunk};
    flat_cost(|mut fx| async move {
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme")
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        commit_where(
            &mut fx,
            |e| e.owns_id(&id),
            || {
                let rec = |offset: u64, body: &'static [u8]| ChunkRecord {
                    offset,
                    step: 1,
                    attempt: 0,
                    k: offset as u32,
                    body: ChunkBody::Data(Bytes::from_static(body)),
                };
                let mut m = cairn_store::Mutations::new();
                m.put_stream_chunk(
                    &id,
                    "$log",
                    0,
                    &StreamChunk {
                        total_bytes: 9,
                        dropped: 0,
                        records: vec![rec(0, b"one"), rec(1, b"two"), rec(2, b"six")],
                    },
                );
                m
            },
        )
        .await?;
        Ok((
            fx,
            "SELECT offset, kind, body FROM sys_log WHERE id = $1 AND offset > $2 ORDER BY offset"
                .into(),
            vec![Param::Str(t.id), Param::Int(0)],
            vec!["1|data|two".into(), "2|data|six".into()],
        ))
    })
    .await
}

/// Question 37 (spans): the trace of one invocation for the console's Trace tab —
/// `sys_span WHERE id = $1`, reading the `$trace` stream of exactly that
/// invocation (a point read by id, like the log).
///
/// Must see: spans merging `start` ∪ `end` (an unclosed span is `unfinished`),
/// cost independent of size.
#[tokio::test]
async fn ui_question_37_spans() -> Result<()> {
    use cairn_store::keys::{ChunkBody, ChunkRecord, StreamChunk};
    flat_cost(|mut fx| async move {
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme")
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        commit_where(
            &mut fx,
            |e| e.owns_id(&id),
            || {
                let span = |offset: u64, v: serde_json::Value| ChunkRecord {
                    offset,
                    step: 2,
                    attempt: 0,
                    k: offset as u32,
                    body: ChunkBody::Data(Bytes::from(v.to_string())),
                };
                let mut m = cairn_store::Mutations::new();
                m.put_stream_chunk(
                    &id,
                    "$trace",
                    0,
                    &StreamChunk {
                        total_bytes: 0,
                        dropped: 0,
                        records: vec![
                            span(0, serde_json::json!({"kind": "start", "span_id": "a", "name": "pick", "ts": 10})),
                            span(1, serde_json::json!({"kind": "start", "span_id": "b", "parent_id": "a", "name": "llm", "ts": 12})),
                            span(2, serde_json::json!({"kind": "end", "span_id": "b", "parent_id": "a", "name": "llm", "ts": 15, "status": "ok", "attrs": {"input_tokens": 10, "output_tokens": 3}})),
                        ],
                    },
                );
                m
            },
        )
        .await?;
        Ok((
            fx,
            "SELECT span_id, parent_id, name, start_ts, end_ts, status, unfinished, start_lost, \
             attrs, input_tokens, output_tokens, step, attempt FROM sys_span WHERE id = $1 \
             ORDER BY span_id"
                .into(),
            vec![Param::Str(t.id)],
            vec![
                "a||pick|10|||true|false|{}|||2|0".into(),
                "b|a|llm|12|15|ok|false|false|{\"input_tokens\":10,\"output_tokens\":3}|10|3|2|0".into(),
            ],
        ))
    })
    .await
}

/// Two streams of one invocation for [`ui_question_38`]: the user stream `out`
/// (two chunks — records 0, 1, then a gap and record 3) and `$log` (one
/// chunk).
fn two_streams(id: &cairn_store::InvocationId) -> cairn_store::Mutations {
    use cairn_store::keys::{ChunkBody, ChunkRecord, StreamChunk};
    let rec = |offset: u64, body: ChunkBody| ChunkRecord {
        offset,
        step: 3,
        attempt: 1,
        k: offset as u32,
        body,
    };
    let data = |b: &'static [u8]| ChunkBody::Data(Bytes::from_static(b));
    let mut m = cairn_store::Mutations::new();
    m.put_stream_chunk(
        id,
        "out",
        0,
        &StreamChunk {
            total_bytes: 6,
            dropped: 0,
            records: vec![rec(0, data(b"tok-a")), rec(1, data(b"b"))],
        },
    );
    m.put_stream_chunk(
        id,
        "out",
        2,
        &StreamChunk {
            total_bytes: 9,
            dropped: 4,
            records: vec![rec(2, ChunkBody::Gap { dropped: 4 }), rec(3, data(b"end"))],
        },
    );
    m.put_stream_chunk(
        id,
        "$log",
        0,
        &StreamChunk {
            total_bytes: 3,
            dropped: 0,
            records: vec![rec(0, data(b"one"))],
        },
    );
    m
}

/// Question 38 (streams): the stream list of one invocation for the console's
/// Streams tab — `sys_stream WHERE id = $1`, read by id prefix like the log.
///
/// Must see: one row per stream (the user stream next to `$log`), chunk count,
/// record count, two cumulative counters taken from the last chunk, the last
/// offset; cost independent of size.
#[tokio::test]
async fn ui_question_38() -> Result<()> {
    flat_cost(|mut fx| async move {
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme")
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        commit_where(&mut fx, |e| e.owns_id(&id), || two_streams(&id)).await?;
        Ok((
            fx,
            "SELECT name, chunks, records, total_bytes, dropped, last_offset FROM sys_stream \
             WHERE id = $1 ORDER BY name"
                .into(),
            vec![Param::Str(t.id)],
            vec!["$log|1|1|3|0|0".into(), "out|2|4|9|4|3".into()],
        ))
    })
    .await
}

/// Question 38 (records): the records of one stream after the offset already
/// seen — the Streams tab's polling form (`WHERE id = $1 AND name = $2 AND
/// offset > $3`), like question 37.
///
/// Must see: only records after the offset, a gap is `gap` with the number of
/// dropped records, the step and attempt of each record; cost independent of size.
#[tokio::test]
async fn ui_question_38_records() -> Result<()> {
    flat_cost(|mut fx| async move {
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme")
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        commit_where(&mut fx, |e| e.owns_id(&id), || two_streams(&id)).await?;
        Ok((
            fx,
            "SELECT offset, step, attempt, kind, body FROM sys_stream_record \
             WHERE id = $1 AND name = $2 AND offset > $3 ORDER BY offset"
                .into(),
            vec![Param::Str(t.id), Param::Str("out".into()), Param::Int(0)],
            vec![
                "1|3|1|data|b".into(),
                "2|3|1|gap|4 records dropped".into(),
                "3|3|1|data|end".into(),
            ],
        ))
    })
    .await
}

/// Question 42: a service's objects for the Objects & State screen —
/// `sys_object WHERE service = $1`, the `ObjectKeys` hint index (checked at
/// read time). Cost by the service's object count, not by bucket size.
///
/// Must see: two `Cart` objects (one holding two invocations, one with only
/// state left), cost independent of fixture size.
#[tokio::test]
async fn ui_question_42() -> Result<()> {
    flat_cost(|mut fx| async move {
        let tenant = TenantId::new("acme").unwrap();
        for (n, key) in [(1u8, "c-1"), (2, "c-1"), (3, "c-2")] {
            let pk = cairn_store::keys::object_owner(&tenant, None, "Cart", key).unwrap();
            let id = make_invocation_id(pk, [n; 16]);
            let e = fx.engines.iter_mut().find(|e| e.owns_key(pk)).unwrap();
            let q = queue("acme", "Cart");
            let seq = e.next_queue_seq(&q).await?;
            e.create(
                id,
                &Target {
                    tenant: tenant.clone(),
                    scope: None,
                    service: "Cart".into(),
                    handler: "add".into(),
                    object_key: key.into(),
                },
                CreateOptions {
                    queue: Some((&q, seq)),
                    ..Default::default()
                },
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
            if key == "c-2" {
                let mut m = cairn_store::Mutations::new();
                m.set_state(pk, &tenant, None, "Cart", "c-2", b"items", b"[]");
                e.writer_mut().commit(m).await?;
                e.cancel(&id, "purge").await?;
                e.purge_now(&id).await?;
            }
        }
        Ok((
            fx,
            "SELECT key, scope, has_state, invocations FROM sys_object WHERE service = $1 \
             ORDER BY key LIMIT $2"
                .into(),
            vec![Param::Str("Cart".into()), Param::Int(500)],
            vec!["c-1||false|2".into(), "c-2||true|0".into()],
        ))
    })
    .await
}

/// Question 39: the state of one object — `WHERE service = $1 AND key = $2`,
/// read by the object's owner prefix.
#[tokio::test]
async fn ui_question_39() -> Result<()> {
    flat_cost(|mut fx| async move {
        let tenant = TenantId::new("acme").unwrap();
        let pk = cairn_store::keys::object_owner(&tenant, None, "Cart", "c-1").unwrap();
        commit_where(
            &mut fx,
            |e| e.owns_key(pk),
            || {
                let mut m = cairn_store::Mutations::new();
                m.set_state(pk, &tenant, None, "Cart", "c-1", b"items", b"[1,2]")
                    .set_state(pk, &tenant, None, "Cart", "c-1", b"owner", b"\"an\"");
                m
            },
        )
        .await?;
        Ok((
            fx,
            "SELECT state_key, value FROM sys_state WHERE service = $1 AND key = $2 \
             ORDER BY state_key"
                .into(),
            vec![Param::Str("Cart".into()), Param::Str("c-1".into())],
            vec!["items|[1,2]".into(), "owner|\"an\"".into()],
        ))
    })
    .await
}

/// Question 40: the promises of one workflow — `WHERE service = $1 AND key =
/// $2`, read by owner prefix. One value promise, one failure promise.
#[tokio::test]
async fn ui_question_40() -> Result<()> {
    use cairn_engine::{PromiseRecord, ReplyResult};
    flat_cost(|mut fx| async move {
        let tenant = TenantId::new("acme").unwrap();
        let pk = cairn_store::keys::object_owner(&tenant, None, "Flow", "w-1").unwrap();
        let by = [0xABu8; 24];
        commit_where(
            &mut fx,
            |e| e.owns_key(pk),
            || {
                let ok = PromiseRecord {
                    result: ReplyResult::Value(Bytes::from_static(b"\"yes\"")),
                    completer: (by, 3),
                }
                .encode();
                let bad = PromiseRecord {
                    result: ReplyResult::Failure {
                        code: 500,
                        message: "no".into(),
                    },
                    completer: (by, 4),
                }
                .encode();
                let mut m = cairn_store::Mutations::new();
                m.set_promise(pk, &tenant, None, "Flow", "w-1", "approval", &ok)
                    .set_promise(pk, &tenant, None, "Flow", "w-1", "review", &bad);
                m
            },
        )
        .await?;
        Ok((
            fx,
            "SELECT name, ok, value, failure_code FROM sys_promise \
             WHERE service = $1 AND key = $2 ORDER BY name"
                .into(),
            vec![Param::Str("Flow".into()), Param::Str("w-1".into())],
            vec!["approval|true|\"yes\"|".into(), "review|false||500".into()],
        ))
    })
    .await
}

/// Question 36 (the "what is it waiting for" part): `entry_lite` of
/// `sys_journal`. Written directly after the last entry of an `active`
/// invocation: a `Run` that has its result notification, a `Sleep` that does not.
///
/// Must see: `Run` `completed: true`, `Sleep` `completed: false` with
/// `wakeUpAtMs`; notifications carry no `completed`; the body of a
/// `RunCompletion` is the JSON `{encoding: json, text}`; read cost flat in data size.
#[tokio::test]
async fn ui_question_36_waiting_on() -> Result<()> {
    let mut reads = Vec::new();
    for n in [2, 8] {
        let mut fx = fixture(n, mixed).await?;
        let t = fx
            .truth
            .iter()
            .find(|t| t.tenant == "acme" && t.class == Class::Active)
            .unwrap()
            .clone();
        let id = cairn_store::id_from_hex(&t.id).unwrap();
        let e = fx.engines.iter_mut().find(|e| e.owns_id(&id)).unwrap();
        let next = e.journal_entries(&id).await?.len() as u32;
        let entry = |ty: MessageType, m: Vec<u8>| {
            cairn_runtime::journal::JournalEntry::new(ty, Bytes::from(m)).encode()
        };
        let mut m = cairn_store::Mutations::new();
        m.append_journal(
            &id,
            next,
            &entry(
                MessageType::RUN,
                prost::Message::encode_to_vec(&pb::RunCommandMessage {
                    result_completion_id: 1,
                    name: "reserve".into(),
                }),
            ),
        )
        .append_journal(
            &id,
            next + 1,
            &entry(
                MessageType::RUN_COMPLETION,
                prost::Message::encode_to_vec(&pb::RunCompletionNotificationMessage {
                    completion_id: 1,
                    result: Some(pb::run_completion_notification_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(br#"{"ok":true}"#)),
                    )),
                }),
            ),
        )
        .append_journal(
            &id,
            next + 2,
            &entry(
                MessageType::SLEEP,
                prost::Message::encode_to_vec(&pb::SleepCommandMessage {
                    wake_up_time: T0 + 60_000,
                    result_completion_id: 2,
                    name: String::new(),
                }),
            ),
        );
        e.writer_mut().commit(m).await?;
        let sql = "SELECT index, entry_lite, body FROM sys_journal WHERE id = $1 AND index >= $2 \
                   ORDER BY index";
        let params = [Param::Str(t.id.clone()), Param::Int(i64::from(next))];
        let t0 = std::time::Instant::now();
        let out = loop {
            let out = run_with_params(
                fx.ctx(TenantScope::One("acme".into())),
                sql,
                &params,
                Limits::default(),
            )
            .await?;
            if out.batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 3 {
                break out;
            }
            ensure!(t0.elapsed() < Duration::from_secs(10), "{:?}", out.batches);
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        ensure!(!out.partial, "{:?}", out.warnings);
        let lite: Vec<serde_json::Value> = strings(&out.batches, 1)
            .iter()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        ensure!(
            lite[0]["kind"] == "Run" && lite[0]["completed"] == true,
            "{lite:?}"
        );
        ensure!(lite[0]["name"] == "reserve", "{lite:?}");
        ensure!(lite[1].get("completed").is_none(), "{lite:?}");
        ensure!(
            lite[2]["kind"] == "Sleep"
                && lite[2]["completed"] == false
                && lite[2]["wakeUpAtMs"] == T0 + 60_000,
            "{lite:?}"
        );
        let body = &out.batches[0].column(2);
        let body = body.as_string::<i32>();
        let run_body: serde_json::Value = serde_json::from_str(body.value(1))?;
        ensure!(
            run_body["encoding"] == "json" && run_body["text"] == r#"{"ok":true}"#,
            "{run_body}"
        );
        ensure!(body.is_null(0) && body.is_null(2), "Run/Sleep have no body");
        reads.push(out.read);
    }
    ensure!(
        reads[0] > 0 && reads[0] == reads[1],
        "read cost grows with the data: {reads:?}"
    );
    Ok(())
}

/// Question 10, **the next page**: the console has no server cursor — the next
/// page reads `created_at <= the timestamp of the previous page's last row`
/// (a scan bound like question 30) and drops the ids already seen at exactly
/// that timestamp itself.
///
/// Must see: exactly the oracle's ids (ordered `created_at DESC, id`) from the
/// timestamp down, and the next page costs **exactly as much** as page 1 with
/// the same `LIMIT` — the bound is pushed down as the scan start, without
/// rescanning the part above it. (The cost of page 1 itself is the business of
/// `plan_shape_list_page`.)
#[tokio::test]
async fn ui_question_10_page() -> Result<()> {
    for n in [12, 48] {
        let fx = fixture(n, mixed).await?;
        let acme = |t: &Truth| t.tenant == "acme";
        let first = oracle(&fx.truth, acme, false, 5);
        let cursor = fx
            .truth
            .iter()
            .find(|t| Some(&t.id) == first.last())
            .unwrap()
            .created;
        let ask = |sql: &'static str, params: Vec<Param>| {
            let ctx = fx.ctx(TenantScope::One("acme".into()));
            async move { run_with_params(ctx, sql, &params, Limits::default()).await }
        };
        let page1 = ask(
            "SELECT id FROM sys_invocation ORDER BY created_at DESC, id LIMIT $1",
            vec![Param::Int(6)],
        )
        .await?;
        let page2 = ask(
            "SELECT id FROM sys_invocation WHERE created_at <= to_timestamp_millis($1) \
             ORDER BY created_at DESC, id LIMIT $2",
            vec![Param::Int(cursor as i64), Param::Int(6)],
        )
        .await?;
        let want = oracle(&fx.truth, |t| acme(t) && t.created <= cursor, false, 6);
        ensure!(!page2.partial, "{:?}", page2.warnings);
        let got = strings(&page2.batches, 0);
        ensure!(got == want, "{got:?} ≠ {want:?}");
        ensure!(
            page2.read > 0 && page2.read == page1.read,
            "n={n}: next page {} ≠ page 1 {}",
            page2.read,
            page1.read
        );
    }
    Ok(())
}

/// Environment variable choosing the case of [`long_operator_chain_child`]: `plus` or `or`.
const CHAIN_CHILD: &str = "CAIRN_QUERY_CHAIN_CHILD";
/// The case's term count (optional), to measure the threshold.
const CHAIN_TERMS: &str = "CAIRN_QUERY_CHAIN_TERMS";

/// Stack cap (bytes, optional) of the thread running the case — default 2 MiB
/// like tokio; `67108864` is the query runtime's `QUERY_STACK`.
const CHAIN_STACK: &str = "CAIRN_QUERY_CHAIN_STACK";

/// The statement of one case: a chain of operators of equal precedence —
/// sqlparser builds it in a loop into a left-deep tree of depth `n`, never
/// touching `recursion_limit`. The relational cases: chained set operations,
/// comma-separated relations in `FROM`, `JOIN` chains — all building a left-deep
/// plan tree of depth `n`.
fn chain_sql(case: &str, n: usize) -> String {
    const CTE: &str = "WITH t AS (SELECT 1 AS x) SELECT 1 FROM t a";
    match case {
        "plus" => format!("SELECT 1{}", "+1".repeat(n)),
        "or" => format!("SELECT 1 WHERE {}1=1", "1=1 OR ".repeat(n)),
        "union" => format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(n)),
        "union_distinct" => format!("SELECT 1{}", " UNION SELECT 1".repeat(n)),
        "intersect" => format!("SELECT 1{}", " INTERSECT SELECT 1".repeat(n)),
        "except" => format!("SELECT 1{}", " EXCEPT SELECT 1".repeat(n)),
        "comma" => format!(
            "{CTE}{}",
            (0..n).map(|i| format!(",t b{i}")).collect::<String>()
        ),
        "join" => format!(
            "{CTE}{}",
            (0..n)
                .map(|i| format!(" JOIN t b{i} ON true"))
                .collect::<String>()
        ),
        "cross_join" => format!(
            "{CTE}{}",
            (0..n)
                .map(|i| format!(" CROSS JOIN t b{i}"))
                .collect::<String>()
        ),
        // A chain of CTEs each reading the previous one — each CTE is one plan level.
        "cte_chain" => format!(
            "WITH c0 AS (SELECT 1 AS x){} SELECT x FROM c{n}",
            (1..=n)
                .map(|i| format!(",c{i} AS (SELECT x FROM c{})", i - 1))
                .collect::<String>()
        ),
        // Each CTE reads the previous CTE **twice** — DataFusion inlines a CTE at
        // every reference, so the tree being walked grows as 2^n even though the
        // text and the token count are linear.
        "cte_fanout_join" => format!(
            "WITH a0 AS (SELECT 1 AS x){} SELECT count(*) FROM a{n}",
            (1..=n)
                .map(|i| format!(
                    ",a{i} AS (SELECT p.x FROM a{j} p JOIN a{j} q ON true)",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        "cte_fanout_union" => format!(
            "WITH a0 AS (SELECT 1 AS x){} SELECT count(*) FROM a{n}",
            (1..=n)
                .map(|i| format!(
                    ",a{i} AS (SELECT x FROM a{j} UNION ALL SELECT x FROM a{j})",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        "cte_fanout_scalar" => format!(
            "WITH a0 AS (SELECT 1 AS x){} SELECT x FROM a{n}",
            (1..=n)
                .map(|i| format!(
                    ",a{i} AS (SELECT (SELECT max(x) FROM a{j}) + (SELECT max(x) FROM a{j}) AS x)",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        // A CTE nested in a subquery: a branch at each level, each level a new `WITH`.
        "cte_fanout_nested" => {
            let mut q = "SELECT 1 AS x".to_string();
            for _ in 0..n {
                q = format!("WITH c AS ({q}) SELECT p.x FROM c p JOIN c q ON true");
            }
            q
        }
        // `GROUP BY CUBE(c1..cn)` — DataFusion builds 2^n grouping sets.
        "cube" => format!(
            "SELECT count(*) FROM (VALUES ({})) GROUP BY CUBE({})",
            (1..=n).map(|i| i.to_string()).collect::<Vec<_>>().join(","),
            (1..=n)
                .map(|i| format!("column{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        // CUBE mixed with another expression ⇒ `powerset` allocates 2^n before the 4096 cap.
        "cube_mixed" => format!(
            "SELECT count(*) FROM (VALUES ({})) GROUP BY CUBE({}), column{}",
            (1..=n + 1)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(","),
            (1..=n)
                .map(|i| format!("column{i}"))
                .collect::<Vec<_>>()
                .join(","),
            n + 1
        ),
        // Four ways of writing a CTE that the token counter did not recognise.
        "cte_fanout_noas" => format!(
            "WITH a0 (SELECT 1 AS x){} SELECT count(*) FROM a{n}",
            (1..=n)
                .map(|i| format!(
                    ",a{i} (SELECT p.x FROM a{j} p JOIN a{j} q ON true)",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        "cte_fanout_squoted" => format!(
            "WITH 'a0' AS (SELECT 1 AS x){} SELECT count(*) FROM a{n}",
            (1..=n)
                .map(|i| format!(
                    ",'a{i}' AS (SELECT p.x FROM a{j} p JOIN a{j} q ON true)",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        "cte_fanout_dotted" => format!(
            "WITH \"s.a0\" AS (SELECT 1 AS x){} SELECT count(*) FROM s.a{n}",
            (1..=n)
                .map(|i| format!(
                    ",\"s.a{i}\" AS (SELECT p.x FROM s.a{j} p JOIN s.a{j} q ON true)",
                    j = i - 1
                ))
                .collect::<String>()
        ),
        "cte_fanout_keyword" => {
            let mut q = "SELECT 1 AS x".to_string();
            for _ in 0..n {
                q = format!("WITH value AS ({q}) SELECT p.x FROM value p JOIN value q ON true");
            }
            q
        }
        // A `VALUES` CTE `n` columns wide cross-joined 62 times.
        "cte_wide" => format!(
            "WITH w AS (VALUES ({})) SELECT count(*) FROM {}",
            vec!["1"; n].join(","),
            (1..=62)
                .map(|i| format!("w a{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        // A body of 500 operators on a column, inlined 62 times.
        "cte_ops" => format!(
            "WITH c AS (SELECT {} AS y FROM (VALUES (1)) t(x)) SELECT count(*) FROM {}",
            vec!["x"; n].join("+"),
            (1..=62)
                .map(|i| format!("c a{i}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        // A flat list (control): separate columns, no levels built.
        "columns" => format!(
            "SELECT 1 AS a{}",
            (0..n).map(|i| format!(",1 AS b{i}")).collect::<String>()
        ),
        other => panic!("unknown case {other}"),
    }
}

/// The largest element count of case `case` for which the statement is still ≤ [`MAX_SQL_BYTES`].
fn fill_64k(case: &str) -> usize {
    let (mut lo, mut hi) = (1usize, MAX_SQL_BYTES);
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if chain_sql(case, mid).len() <= MAX_SQL_BYTES {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// **A long SQL statement is refused, and the process survives.**
///
/// A read-only key sends `SELECT 1+1+…+1` (4·10⁵ terms, ~800 KiB, under the
/// extractor's 2 MiB cap) or `… WHERE 1=1 OR 1=1 OR …` (2·10⁵). An expression
/// tree of depth N: planning is synchronous CPU that `timeout_at` cannot cut,
/// and the tree's `Drop`/`Display` are compiler-generated recursion — on a
/// thread with a 2 MiB stack (tokio's default, like the query runtime) that can
/// overflow the stack, i.e. **abort the whole process**, not panic.
///
/// Each case runs in a child process ([`long_operator_chain_child`]) on a 2 MiB
/// thread. Must see: the child exits normally, the statement returns a
/// client-side error (`is_client`), in < 2 s.
#[test]
fn long_operator_chain_is_refused() -> Result<()> {
    for (case, n) in [("plus", 400_000usize), ("or", 200_000)] {
        let started = std::time::Instant::now();
        let out = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "long_operator_chain_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHAIN_CHILD, case)
            .env(CHAIN_TERMS, n.to_string())
            .output()?;
        let took = started.elapsed();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let tail = |s: &str| {
            s.chars()
                .rev()
                .take(600)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>()
        };
        ensure!(
            out.status.success(),
            "{case} × {n}: child process died ({:?}) after {took:?}\nstdout: {}\nstderr: {}",
            out.status,
            tail(&stdout),
            tail(&stderr)
        );
        ensure!(
            stdout.contains("OUTCOME refused"),
            "{case} × {n}: statement not refused client-side after {took:?}: {}",
            tail(&stdout)
        );
        ensure!(
            took < Duration::from_secs(2),
            "{case} × {n}: refused too slowly ({took:?})"
        );
    }
    Ok(())
}

/// **A deep relational tree is refused before DataFusion.**
///
/// `check_sql` counts expression operators, not set operations, `JOIN`s,
/// comma-separated relations in `FROM` or CTE chains: each of those builds a
/// left-deep plan tree as deep as the element count without touching
/// `recursion_limit` (which only counts bracket nesting). Measured on the base
/// (debug build, 64 MiB thread like `QUERY_STACK`, statements filling 64 KiB):
/// `UNION ALL` with 3 448 branches plans in 10.6 s, `INTERSECT` in 76 s —
/// synchronous CPU that the statement's 10 s deadline cannot cut, taking one of
/// the query runtime's two threads.
///
/// Each case runs in a child process ([`long_operator_chain_child`]) on a
/// 64 MiB thread. Must see: the child exits normally, the statement is refused
/// by `check_sql` (a client-side error naming the cap) in < 2 s.
#[test]
fn deep_relation_tree_is_refused() -> Result<()> {
    for case in [
        "union",
        "union_distinct",
        "intersect",
        "except",
        "comma",
        "join",
        "cross_join",
        "cte_chain",
    ] {
        let started = std::time::Instant::now();
        let out = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "long_operator_chain_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHAIN_CHILD, case)
            .env(CHAIN_TERMS, "0")
            .env(CHAIN_STACK, (64usize << 20).to_string())
            .output()?;
        let took = started.elapsed();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        ensure!(
            out.status.success(),
            "{case}: child process died ({:?}) after {took:?}\nstderr: {}",
            out.status,
            stderr
                .chars()
                .rev()
                .take(600)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>()
        );
        // The child's `println!` lands after libtest's "test … ... ", on the same line.
        let line = stdout
            .lines()
            .find(|l| l.contains("OUTCOME"))
            .unwrap_or_default();
        ensure!(
            line.contains("OUTCOME refused") && line.contains("the limit is"),
            "{case}: statement not refused by check_sql after {took:?}: {line}"
        );
        ensure!(
            took < Duration::from_secs(2),
            "{case}: refused too slowly ({took:?})"
        );
    }
    Ok(())
}

/// **A CTE referenced several times is refused by its size after inlining.**
/// DataFusion inlines a CTE's body at every reference; each level reading the
/// previous one twice ⇒ a 2^k tree being walked, even though the statement is
/// under 2 KB and under the 64/64 token caps. Measured on the base (release,
/// 64 MiB thread): `JOIN` twice over 15 levels 2.6 s / 413 MiB, 20 levels
/// 120 s / 5.5 GB; a scalar subquery twice over 10 levels 2.7 s, 14 levels
/// > 120 s; `UNION ALL` over 14 levels 0.8 s; nested CTEs over 14 levels 2.6 s.
///
/// Each case runs in a child process on a 64 MiB thread. Must see: the child
/// exits normally, the statement is refused by `check_sql` (a client-side error
/// naming the cap) in < 2 s.
#[test]
fn cte_fanout_is_refused() -> Result<()> {
    refused_quickly(&[
        // Sizes chosen so that the unfixed version exceeds 2 s without having
        // time to eat all the RAM before the parent kills the child (release:
        // join over 20 levels 120 s, 5.5 GB).
        ("cte_fanout_join", 20),
        ("cte_fanout_union", 18),
        ("cte_fanout_scalar", 14),
        ("cte_fanout_nested", 18),
    ])
}

/// Runs each case in a child process ([`long_operator_chain_child`], 64 MiB
/// thread) watched by the parent: killed past 20 s or 2 GiB of RSS. Must see:
/// the child exits normally, the statement is refused by `check_sql` (a
/// client-side error naming the cap) in < 2 s.
fn refused_quickly(cases: &[(&str, usize)]) -> Result<()> {
    for &(case, n) in cases {
        let started = std::time::Instant::now();
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "long_operator_chain_child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHAIN_CHILD, case)
            .env(CHAIN_TERMS, n.to_string())
            .env(CHAIN_STACK, (64usize << 20).to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let pid = child.id().to_string();
        let killed = loop {
            if child.try_wait()?.is_some() {
                break None;
            }
            let rss_kib = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &pid])
                .output()
                .ok()
                .and_then(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
                .unwrap_or(0);
            if rss_kib > 2 << 20 {
                child.kill()?;
                break Some(format!("RSS {} MiB", rss_kib >> 10));
            }
            if started.elapsed() > Duration::from_secs(20) {
                child.kill()?;
                break Some("over 20 s".to_string());
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let out = child.wait_with_output()?;
        let took = started.elapsed();
        if let Some(why) = killed {
            anyhow::bail!("{case} × {n}: planning killed ({why})");
        }
        ensure!(
            out.status.success(),
            "{case} × {n}: child died ({:?}) after {took:?}",
            out.status
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        let line = stdout
            .lines()
            .find(|l| l.contains("OUTCOME"))
            .unwrap_or_default();
        ensure!(
            line.contains("OUTCOME refused") && line.contains("the limit is"),
            "{case} × {n}: statement not refused by check_sql after {took:?}: {line}"
        );
        ensure!(
            took < Duration::from_secs(2),
            "{case} × {n}: refused too slowly ({took:?})"
        );
    }
    Ok(())
}

/// **Forms the token-based counter does not see.** `GROUP BY CUBE` grows 2^n
/// grouping sets (mixed with another expression ⇒ `powerset` allocates 2^n
/// before DataFusion's 4 096 cap); CTEs written without `AS`, with single-quoted
/// names, dotted names, keyword names (the token counter does not recognise
/// them ⇒ the repeated-CTE hole reopens); wide `VALUES` CTEs or bodies with
/// many operators joined/inlined 62 times.
#[test]
fn shapes_the_token_counter_missed_are_refused() -> Result<()> {
    refused_quickly(&[
        ("cube", 20),
        ("cube_mixed", 36),
        ("cte_fanout_noas", 18),
        ("cte_fanout_squoted", 18),
        ("cte_fanout_dotted", 18),
        ("cte_fanout_keyword", 18),
        ("cte_wide", 20_000),
        ("cte_ops", 500),
    ])
}

/// **Counting by size after CTE inlining.** Must see: a CTE used once, a short
/// CTE chain, a CTE used a few times pass; a few levels of double branching
/// (including through `UNION`, a scalar subquery, a CTE nested in a subquery,
/// a name shadowed in an inner scope) are refused; a column with the CTE's name
/// (`a.x`) is not counted as a reference; "FROM first" and `VALUE` cost blocks.
#[test]
fn cte_inlining_is_counted() {
    use cairn_query::check_sql;
    let ok = |sql: &str| check_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let refused = |sql: &str| assert!(check_sql(sql).is_err_and(|e| e.is_client()), "{sql}");
    ok("WITH t AS (SELECT 1 AS x) SELECT t.x, t.x, t.x FROM t");
    ok("WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a), c AS (SELECT x FROM b) SELECT x FROM c");
    ok("WITH t AS (SELECT 1 AS x) SELECT * FROM t p JOIN t q ON true JOIN t r ON true");
    let fan = |k: usize, body: &dyn Fn(usize) -> String| {
        format!(
            "WITH a0 AS (SELECT 1 AS x){} SELECT * FROM a{k}",
            (1..=k)
                .map(|i| format!(",a{i} AS ({})", body(i - 1)))
                .collect::<String>()
        )
    };
    refused(&fan(8, &|j| {
        format!("SELECT p.x FROM a{j} p JOIN a{j} q ON true")
    }));
    refused(&fan(8, &|j| {
        format!("SELECT x FROM a{j} UNION ALL SELECT x FROM a{j}")
    }));
    refused(&fan(8, &|j| {
        format!("SELECT (SELECT max(x) FROM a{j}) + (SELECT max(x) FROM a{j}) AS x")
    }));
    // A CTE nested in a subquery, with the same name at every level.
    let mut q = "SELECT 1 AS x".to_string();
    for _ in 0..8 {
        q = format!("WITH c AS ({q}) SELECT p.x FROM c p JOIN c q ON true");
    }
    refused(&format!("SELECT * FROM ({q}) s"));
    // A shadowed name: the outer `a4` is heavy, the `a4` in the subquery light —
    // the reference after the subquery is still the outer `a4`.
    let heavy = (1..=4)
        .map(|i| {
            format!(
                ",a{i} AS (SELECT p.x FROM a{j} p JOIN a{j} q ON true)",
                j = i - 1
            )
        })
        .collect::<String>();
    refused(&format!(
        "WITH a0 AS (SELECT 1 AS x){heavy} SELECT 1 FROM (WITH a4 AS (SELECT 1 AS x) SELECT x FROM a4) s, a4 u, a4 v"
    ));
    // Control: three levels of double branching (`a3`, 15 blocks) used once ⇒
    // passes (the definition counts in full once: 1 + 3 + 7 + 15, plus 15 at the
    // use site).
    let three = (1..=3)
        .map(|i| {
            format!(
                ",a{i} AS (SELECT p.x FROM a{j} p JOIN a{j} q ON true)",
                j = i - 1
            )
        })
        .collect::<String>();
    ok(&format!(
        "WITH a0 AS (SELECT 1 AS x){three} SELECT 1 FROM a3 u"
    ));
    // Double branching used only in the body of a CTE nobody reads still counts (seed 9 664).
    refused(&format!(
        "WITH a0 AS (SELECT 1 AS x){heavy}, z AS (SELECT 1 FROM a4 p, a4 q) SELECT 1"
    ));
    // "FROM first" and `VALUE` are blocks.
    let from_first = format!(
        "WITH a0 AS (SELECT 1 AS x){} SELECT 1",
        (1..=70)
            .map(|i| format!(",a{i} AS (FROM a0)"))
            .collect::<String>()
    );
    refused(&from_first);
}

/// **`EXPLAIN` of a valid statement is still accepted** (the
/// `lineage::call_lineage_reaches_the_callee` test sends `EXPLAIN SELECT …`
/// through `/query`; before the AST checker, DataFusion accepted `EXPLAIN`,
/// `EXPLAIN VERBOSE`, `EXPLAIN ANALYZE` — `ANALYZE` executes the statement, as
/// running it does). Must see: those forms pass `check_sql`; the inner
/// statement is still checked as usual (CUBE over 20 columns, DDL refused even
/// when wrapped in `EXPLAIN`).
#[test]
fn explain_of_a_valid_query_is_accepted() {
    use cairn_query::check_sql;
    for sql in [
        "EXPLAIN SELECT id FROM sys_invocation WHERE root_id = 'x'",
        "EXPLAIN VERBOSE SELECT 1",
        "EXPLAIN ANALYZE SELECT 1",
    ] {
        check_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cube = format!(
        "EXPLAIN SELECT count(*) FROM (VALUES ({})) GROUP BY CUBE({})",
        (1..=20)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(","),
        (1..=20)
            .map(|i| format!("column{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for sql in [
        cube.as_str(),
        "EXPLAIN CREATE TABLE t AS SELECT 1",
        "EXPLAIN EXPLAIN SELECT 1",
    ] {
        assert!(check_sql(sql).is_err_and(|e| e.is_client()), "{sql}");
    }
}

/// **The relation and block caps count in the right places.** Commas outside
/// `FROM` (column lists, `IN (…)`, `GROUP BY`, `ORDER BY`, function arguments)
/// do not count; `ON`/`USING` do not close `FROM`; a statement exactly at the
/// cap passes, one unit over the cap is refused with a client-side error.
#[test]
fn relation_and_block_limits_count_the_right_tokens() {
    use cairn_query::{check_sql, MAX_SQL_BLOCKS, MAX_SQL_RELATIONS};
    let wide = format!(
        "SELECT {} FROM t WHERE x IN ({}) GROUP BY {} ORDER BY {}",
        (0..200)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        (0..200)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        (0..100)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        (0..100)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", "),
    );
    check_sql(&wide).expect("a flat list is not a relation");
    let joins = |n: usize| {
        format!(
            "SELECT 1 FROM t a{}",
            (0..n)
                .map(|i| format!(" JOIN t b{i} ON a.x = b{i}.x"))
                .collect::<String>()
        )
    };
    // Token cap (before parsing): 64 joins pass, 65 do not.
    assert!(check_sql(&joins(MAX_SQL_RELATIONS + 1))
        .is_err_and(|e| e.is_client() && e.to_string().contains("relations")));
    // On the AST, a base table counts 48 columns: 5 tables pass (240), 6 tables
    // exceed the join schema's width cap of 256.
    check_sql(&joins(4)).expect("5 base tables under the width cap");
    assert!(check_sql(&joins(5)).is_err_and(|e| e.is_client() && e.to_string().contains("column")));
    // A comma after `ON` still separates relations.
    let after_on = format!(
        "SELECT 1 FROM t a JOIN t b ON a.x = b.x{}",
        (0..MAX_SQL_RELATIONS)
            .map(|i| format!(", t c{i}"))
            .collect::<String>()
    );
    assert!(check_sql(&after_on).is_err(), "comma after ON not counted");
    let unions = |n: usize| format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(n));
    check_sql(&unions(MAX_SQL_BLOCKS / 2 - 1)).expect("under the block cap");
    assert!(check_sql(&unions(MAX_SQL_BLOCKS)).is_err_and(|e| e.is_client()));
}

/// The child process of [`long_operator_chain_is_refused`]: runs one case on a
/// thread with a 2 MiB stack and prints `OUTCOME <outcome> <time> <error>`.
/// Without the [`CHAIN_CHILD`] variable it does nothing.
#[test]
#[ignore = "child process of long_operator_chain_is_refused"]
fn long_operator_chain_child() {
    let Ok(case) = std::env::var(CHAIN_CHILD) else {
        return;
    };
    let n: usize = std::env::var(CHAIN_TERMS)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400_000);
    let n = if n == 0 { fill_64k(&case) } else { n };
    let stack: usize = std::env::var(CHAIN_STACK)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2 << 20);
    let sql = chain_sql(&case, n);
    eprintln!("CASE {case} n={n} bytes={} stack={stack}", sql.len());
    std::thread::Builder::new()
        .stack_size(stack)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let started = std::time::Instant::now();
            let r = rt.block_on(run(
                QueryCtx::new(TenantScope::All, Vec::new()),
                &sql,
                Limits::default(),
            ));
            let took = started.elapsed();
            let (outcome, why) = match &r {
                Err(e) if e.is_client() => ("refused", e.to_string()),
                Err(e) => ("error", e.to_string()),
                Ok(_) => ("ok", String::new()),
            };
            drop(r);
            drop(sql);
            println!(
                "OUTCOME {outcome} {took:?} {}",
                why.chars().take(200).collect::<String>()
            );
        })
        .expect("2 MiB thread")
        .join()
        .expect("child thread does not panic");
}

// ───────────────────────────── referee: adversarial statements by grammar

/// The "adversarial" statement generator: branching CTEs (every way of writing
/// a name — plain, without `AS`, single-quoted, double-quoted with dots,
/// keywords), `CUBE`/`ROLLUP`/`GROUPING SETS`, wide `VALUES`, joins, set
/// operations, subqueries. Statements may be semantically wrong — only those
/// `check_sql` lets through are measured.
struct Adversary {
    rng: Rng,
}

/// A generated CTE: its name at definition, its name at reference.
struct Named {
    def: String,
    reference: String,
}

impl Adversary {
    fn below(&mut self, n: u64) -> u64 {
        self.rng.next() % n.max(1)
    }

    fn coin(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// `below(big)` with probability `percent`, otherwise `below(small)`.
    fn sometimes(&mut self, percent: u64, big: u64, small: u64) -> u64 {
        let n = if self.coin(percent) { big } else { small };
        self.below(n)
    }

    fn values(&mut self) -> String {
        let cols = 1 + match self.below(20) {
            0 => self.below(2_000),
            1..=4 => self.below(300),
            _ => self.below(8),
        };
        let rows = 1 + self.below(if cols > 100 { 2 } else { 40 });
        let row = format!("({})", vec!["1"; cols as usize].join(","));
        format!("VALUES {}", vec![row; rows as usize].join(","))
    }

    fn group_by(&mut self) -> String {
        let n = 1 + self.below(40) as usize;
        let cols = (1..=n)
            .map(|i| format!("column{i}"))
            .collect::<Vec<_>>()
            .join(",");
        match self.below(5) {
            0 => format!(" GROUP BY CUBE({cols})"),
            1 => format!(" GROUP BY CUBE({cols}), column{}", n + 1),
            2 => format!(" GROUP BY ROLLUP({cols}), ROLLUP({cols})"),
            3 => format!(" GROUP BY GROUPING SETS (({cols}), ({cols}))"),
            _ => format!(" GROUP BY {cols}"),
        }
    }

    /// One relation of a `FROM`.
    fn relation(&mut self, visible: &[Named], depth: u32) -> String {
        let alias = format!("r{}", self.below(1_000_000));
        match self.below(4) {
            0 | 1 if !visible.is_empty() => {
                let n = &visible[self.below(visible.len() as u64) as usize];
                format!("{} {alias}", n.reference)
            }
            2 if depth > 0 => format!("({}) {alias}", self.query(visible, depth - 1)),
            3 => "sys_invocation".to_string(),
            _ => format!("({}) {alias}", self.values()),
        }
    }

    fn select(&mut self, visible: &[Named], depth: u32) -> String {
        let n = 1 + self.sometimes(30, 6, 3);
        let mut from = self.relation(visible, depth);
        for _ in 1..n {
            let r = self.relation(visible, depth);
            from = match self.below(3) {
                0 => format!("{from}, {r}"),
                1 => format!("{from} JOIN {r} ON true"),
                _ => format!("{from} CROSS JOIN {r}"),
            };
        }
        let projection = match self.below(4) {
            0 if depth > 0 => format!(
                "(SELECT count(*) FROM {}) + (SELECT count(*) FROM {}) AS x",
                self.relation(visible, depth - 1),
                self.relation(visible, depth - 1)
            ),
            1 => "*".to_string(),
            2 => "count(*) AS x".to_string(),
            _ => "1 AS x".to_string(),
        };
        let group = if self.coin(25) {
            self.group_by()
        } else {
            String::new()
        };
        format!("SELECT {projection} FROM {from}{group}")
    }

    fn body(&mut self, visible: &[Named], depth: u32) -> String {
        let mut q = self.select(visible, depth);
        for _ in 0..self.sometimes(20, 6, 2) {
            let op = *self
                .rng
                .pick(&["UNION ALL", "UNION", "INTERSECT", "EXCEPT"]);
            q = format!("{q} {op} {}", self.select(visible, depth));
        }
        q
    }

    fn query(&mut self, outer: &[Named], depth: u32) -> String {
        if !self.coin(60) {
            return self.body(outer, depth);
        }
        let mut visible: Vec<Named> = outer
            .iter()
            .map(|n| Named {
                def: n.def.clone(),
                reference: n.reference.clone(),
            })
            .collect();
        let tag = self.below(1_000_000);
        let style = self.below(6);
        let count = 1 + self.sometimes(40, 24, 4);
        let mut defs = Vec::new();
        for i in 0..count {
            let base = format!("c{tag}_{i}");
            let (def, reference) = match style {
                0 => (format!("{base} AS"), base.clone()),
                1 => (base.clone(), base.clone()),
                2 => (format!("'{base}' AS"), base.clone()),
                3 => (format!("\"s.{base}\" AS"), format!("s.{base}")),
                4 => ("value AS".to_string(), "value".to_string()),
                _ => (
                    format!("\"{}\" AS", base.to_uppercase()),
                    format!("\"{}\"", base.to_uppercase()),
                ),
            };
            // Body: the previous level used twice (double branching) or an arbitrary statement.
            let body = match visible.last() {
                Some(prev) if self.coin(70) => format!(
                    "SELECT p.x FROM {r} p JOIN {r} q ON true",
                    r = prev.reference
                ),
                _ => self.body(&visible, depth.saturating_sub(1)),
            };
            defs.push(format!("{def} ({body})"));
            visible.push(Named { def, reference });
        }
        let main = self.body(&visible, depth);
        format!("WITH {} {main}", defs.join(", "))
    }
}

/// Environment variable: the referee's seed count (default 400).
const ADVERSARY_SEEDS: &str = "CAIRN_QUERY_ADVERSARY_SEEDS";
/// Environment variable choosing the child mode of [`adversarial_queries_plan_fast`].
const ADVERSARY_CHILD: &str = "CAIRN_QUERY_ADVERSARY_CHILD";

/// **The referee — every statement `check_sql` lets through plans fast and does
/// not eat RAM.** Not a hand-written case list: statements generated by grammar
/// ([`Adversary`]) with fixed seeds; the child runs them in sequence, printing
/// `SEED n` before each one; a statement that passes `check_sql` is
/// `explain`ed (logical and physical planning). The parent watches the child's
/// RSS and kills it past 2 GiB or 600 s.
///
/// Must see: the child exits normally, no statement takes more than 1 s to plan
/// (release; debug 5 s), the child's RSS stays under the cap.
#[test]
fn adversarial_queries_plan_fast() -> Result<()> {
    let started = std::time::Instant::now();
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "adversarial_queries_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ADVERSARY_CHILD, "1")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let pid = child.id().to_string();
    // Read the child's output in parallel: a full pipe (≥ 64 KiB of "SEED n") stalls the child.
    let mut pipe = child.stdout.take().expect("stdout piped");
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = std::io::Read::read_to_string(&mut pipe, &mut s);
        s
    });
    let rss_cap_kib = 2u64 << 20;
    let mut peak = 0u64;
    let killed = loop {
        if child.try_wait()?.is_some() {
            break None;
        }
        let rss = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0);
        peak = peak.max(rss);
        if rss > rss_cap_kib {
            child.kill()?;
            break Some(format!("RSS {} MiB", rss >> 10));
        }
        if started.elapsed() > Duration::from_secs(600) {
            child.kill()?;
            break Some("over 600 s".to_string());
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let status = child.wait()?;
    let stdout = reader.join().unwrap_or_default();
    let last_seed = stdout
        .lines()
        .rev()
        .find(|l| l.contains("SEED "))
        .unwrap_or_default();
    if let Some(why) = killed {
        anyhow::bail!("child killed ({why}) at {last_seed}");
    }
    ensure!(status.success(), "child died ({status:?}) at {last_seed}");
    let slow: Vec<&str> = stdout.lines().filter(|l| l.contains("SLOW ")).collect();
    ensure!(
        slow.is_empty(),
        "statement passed check_sql but plans slowly:\n{}",
        slow.join("\n")
    );
    let summary = stdout
        .lines()
        .find(|l| l.contains("ADVERSARY "))
        .unwrap_or_default();
    ensure!(!summary.is_empty(), "child printed no summary: {last_seed}");
    eprintln!("{summary} peak_rss={} MiB", peak >> 10);
    Ok(())
}

/// The child of [`adversarial_queries_plan_fast`]: a 64 MiB thread like `QUERY_STACK`.
#[test]
#[ignore = "child process of adversarial_queries_plan_fast"]
fn adversarial_queries_child() {
    use std::io::Write as _;
    if std::env::var(ADVERSARY_CHILD).is_err() {
        return;
    }
    let seeds: u64 = std::env::var(ADVERSARY_SEEDS)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    // First seed (optional) — to reproduce a reported seed, printing the whole statement.
    let first: u64 = std::env::var("CAIRN_QUERY_ADVERSARY_FROM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let full = std::env::var("CAIRN_QUERY_ADVERSARY_FULL").is_ok();
    let budget = if cfg!(debug_assertions) {
        Duration::from_secs(5)
    } else {
        Duration::from_secs(1)
    };
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let (mut passed, mut planned, mut worst) = (0u64, 0u64, Duration::ZERO);
            for seed in first..first + seeds {
                let mut a = Adversary {
                    rng: Rng(
                        0x9e37_79b9_7f4a_7c15 ^ (seed.wrapping_add(1).wrapping_mul(0x2545_f491))
                    ),
                };
                let sql = a.query(&[], 2);
                println!("SEED {seed}");
                let _ = std::io::stdout().flush();
                if cairn_query::check_sql(&sql).is_err() {
                    continue;
                }
                passed += 1;
                if full {
                    println!("SQL {sql}");
                    let _ = std::io::stdout().flush();
                }
                let started = std::time::Instant::now();
                let r = rt.block_on(explain(QueryCtx::new(TenantScope::All, Vec::new()), &sql));
                let took = started.elapsed();
                worst = worst.max(took);
                if r.is_ok() {
                    planned += 1;
                }
                if took > budget {
                    let shown = if full { sql.len() } else { sql.len().min(300) };
                    println!("SLOW seed={seed} took={took:?} sql={}", &sql[..shown]);
                }
            }
            println!(
                "ADVERSARY seeds={seeds} passed_check={passed} planned={planned} worst={worst:?}"
            );
        })
        .expect("64 MiB thread")
        .join()
        .expect("does not panic");
}

/// **`SELECT * REPLACE (…)` and `UNION … BY NAME` are refused.** The AST
/// checker does not walk the expressions of `REPLACE` (subqueries and CTE
/// references in it weigh 0 — reopening the repeated-CTE hole), and takes the
/// width of a set operation's left side while `BY NAME` takes the union of the
/// names. The console's statements do not use these two forms. Must see: a
/// caller-side error naming the forbidden form; `EXCLUDE` and plain
/// `UNION ALL` still pass.
#[test]
fn replace_and_by_name_are_refused() {
    use cairn_query::check_sql;
    for sql in [
        "SELECT * REPLACE (1 AS id) FROM sys_invocation",
        "SELECT * REPLACE ((SELECT 1) AS id) FROM sys_invocation",
        "SELECT t.* REPLACE (1 AS x) FROM (SELECT 1 AS x) t",
        "SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS b",
        "SELECT 1 AS a UNION BY NAME SELECT 2 AS b",
        "SELECT 1 AS a UNION DISTINCT BY NAME SELECT 2 AS b",
    ] {
        let e = check_sql(sql).expect_err(sql);
        assert!(e.is_client(), "{sql}: {e}");
        let msg = e.to_string();
        assert!(
            msg.contains("REPLACE") || msg.contains("BY NAME"),
            "{sql}: message does not name the forbidden form: {msg}"
        );
    }
    for sql in [
        "SELECT * EXCLUDE (id) FROM sys_invocation",
        "SELECT 1 AS a UNION ALL SELECT 2 AS a",
    ] {
        check_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// **The child process chooses paths like a coordinating node.** The `/query`
/// child has no ranges: every range goes through the node like a "remote
/// node", and the path chooser reads the class counts the node sends along
/// ([`cairn_query::invocation::class_counts`]). Must see: for each class (rare
/// and common), with/without `LIMIT`, the rows **and read units** match the
/// statement run in place on the same ranges.
#[tokio::test]
async fn worker_ctx_reads_like_the_coordinator() -> Result<()> {
    let fx = fixture(60, mixed).await?;
    let h: Vec<RangeReadHandle> = fx.engines.iter().map(Engine::read_handle).collect();
    let worker = || {
        let local = fx.ctx(TenantScope::All);
        let mut ctx =
            QueryCtx::new(TenantScope::All, Vec::new()).with_remotes(vec![Arc::new(InProc {
                ranges: h.iter().map(|h| h.range().id.0).collect(),
                handles: h.clone(),
                mode: Mode::Up,
            })]);
        ctx.class_counts = Some(cairn_query::invocation::class_counts(&local));
        ctx
    };
    for class in ["active", "completed", "failed", "paused"] {
        for limit in ["", " LIMIT 5"] {
            let sql = format!(
                "SELECT id FROM sys_invocation WHERE class = '{class}' ORDER BY created_at DESC, id{limit}"
            );
            let here = run(fx.ctx(TenantScope::All), &sql, Limits::default()).await?;
            let there = run(worker(), &sql, Limits::default()).await?;
            ensure!(
                strings(&here.batches, 0) == strings(&there.batches, 0),
                "{sql}: rows differ"
            );
            ensure!(
                here.read == there.read,
                "{sql}: {} read in place, {} in the child",
                here.read,
                there.read
            );
        }
    }
    Ok(())
}
