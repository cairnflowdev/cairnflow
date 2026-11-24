// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Durable progress watermarks (FL-18) on real storage.
//!
//! The scan watermarks of the four deadline indexes (timer, retry, purge,
//! delayed) and the three ordering indexes (outbox, signal, inbound) survive a
//! reopen: the next writer scans from the watermark, not from 0 over the
//! tombstones of every processed row. In exchange this must hold: a watermark
//! never passes an unprocessed row — even when the process dies midway, when a
//! writer that does not know watermarks (an old binary) writes in between, and
//! when the derived zone is rebuilt.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    cairn_engine::Engine,
    cairn_store::{
        keys::{self, Index},
        make_invocation_id, LeadershipGrant, Mutations, PartitionId, PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";
/// The deadline of the row still live after every other row is processed — the watermark must stop here.
const FAR: u64 = 5_000;
const NOW: u64 = 1_000;

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

fn fresh_store(name: &str) -> Result<Arc<dyn ObjectStore>> {
    let root: PathBuf = std::env::temp_dir().join(format!("cairnflow-floors-{name}"));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?))
}

fn settings() -> Settings {
    Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    }
}

async fn open_engine(store: &Arc<dyn ObjectStore>) -> Result<Engine> {
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

fn completion_msg(completion_id: u32) -> cairn_engine::InterPartition {
    cairn_engine::InterPartition::Completion(cairn_engine::DeliverCompletion {
        target: inv_id(1),
        completion_id,
        result: cairn_engine::CompletionPayload::Void,
    })
}

/// One drain round like `turn::drain_inbound`.
async fn drain_inbound_pass(engine: &mut Engine) -> Result<usize> {
    let mut cursor = 0;
    let mut n = 0;
    loop {
        let rows = engine.inbound_from(cursor, 64).await?;
        let Some((last, _)) = rows.last() else {
            return Ok(n);
        };
        cursor = last.seq + 1;
        for (key, _) in rows {
            engine.drop_inbound(key).await?;
            n += 1;
        }
    }
}

/// Each deadline index: `n` rows with deadlines `100..100+n` written and then
/// all processed (leaving tombstones), plus one live row with deadline
/// [`FAR`]. Inbound: `n` messages received and then all drained. Then one scan
/// tick per index (the RAM watermark advances) and one ordinary commit (the
/// watermark goes with it).
async fn churn(engine: &mut Engine, n: u8) -> Result<()> {
    let w = engine.writer_mut();
    let mut m = Mutations::new();
    for i in 0..n {
        let id = inv_id(i);
        let due = 100 + u64::from(i);
        m.add_timer(due, &id, 1, b"")
            .add_retry(due, &id, &[0; 8])
            .add_purge(due, &id)
            .add_delayed(due, &id, 1, b"");
    }
    let far = inv_id(200);
    m.add_timer(FAR, &far, 1, b"")
        .add_retry(FAR, &far, &[0; 8])
        .add_purge(FAR, &far)
        .add_delayed(FAR, &far, 1, b"");
    w.commit(m).await?;

    let mut m = Mutations::new();
    for (due, id, cid) in w.due_timers(NOW, usize::MAX).await?.rows {
        m.remove_timer(due, &id, cid);
    }
    for (due, id, _) in w.due_retries(NOW, 1_000).await?.rows {
        m.remove_retry(due, &id);
    }
    for (due, id) in w.due_purges(NOW, 1_000).await?.rows {
        m.remove_purge(due, &id);
    }
    for (due, id, cid, _) in w.due_delayed(NOW, 1_000).await?.rows {
        m.remove_delayed(due, &id, cid);
    }
    w.commit(m).await?;
    // The next background tick: each scan walks over the tombstones just left, once.
    w.due_timers(NOW, usize::MAX).await?;
    w.due_retries(NOW, 1_000).await?;
    w.due_purges(NOW, 1_000).await?;
    w.due_delayed(NOW, 1_000).await?;

    for cid in 0..u32::from(n) {
        engine.accept_message(&completion_msg(100 + cid)).await?;
    }
    assert_eq!(drain_inbound_pass(engine).await?, usize::from(n));
    // An empty drain round (the inbound watermark passes the stretch just drained), then an ordinary commit.
    assert_eq!(drain_inbound_pass(engine).await?, 0);
    engine.accept_message(&completion_msg(999)).await?;
    engine.writer().wait_durable().await?;
    Ok(())
}

/// **The watermark survives a reopen.**
///
/// 1. Writer A: per deadline index 20 rows written and then processed, one live
///    row at deadline [`FAR`]; 20 inbound messages received and then drained,
///    one still pending. One scan tick, one ordinary commit. Seal.
/// 2. Writer B opens the same Db; one scan tick per index.
///
/// Must see: B's first scan of each deadline index starts at deadline [`FAR`]
/// (not at 0), and the first inbound drain round starts at the `seq` of the
/// pending message — walking over none of the tombstones A left.
#[tokio::test]
async fn progress_floor_survives_reopen() -> Result<()> {
    let store = fresh_store("reopen")?;
    let mut a = open_engine(&store).await?;
    churn(&mut a, 20).await?;
    let pending_seq = a.inbound_from(0, 64).await?[0].0.seq;
    a.seal().await?;

    let mut b = open_engine(&store).await?;
    let w = b.writer();
    w.take_due_scans();
    w.due_timers(NOW, usize::MAX).await?;
    w.due_retries(NOW, 1_000).await?;
    w.due_purges(NOW, 1_000).await?;
    w.due_delayed(NOW, 1_000).await?;
    let mut starts = w.take_due_scans();
    starts.sort_by_key(|(ix, _)| *ix as u8);
    assert_eq!(
        starts,
        vec![
            (Index::Timer, FAR),
            (Index::Retry, FAR),
            (Index::Delayed, FAR)
        ],
        "the new writer's first deadline scan starts at the durable watermark; purge: \
         the watermark is already above `now` so no scan"
    );

    w.take_seq_scans();
    let rows = b.inbound_from(0, 64).await?;
    assert_eq!(rows.len(), 1, "the pending message is still seen");
    let scans = b.writer().take_seq_scans();
    assert_eq!(
        scans,
        vec![(Index::Inbound, pending_seq)],
        "the new writer's first drain round starts at the durable watermark"
    );
    assert_eq!(
        b.writer()
            .tombstones_from(Index::Inbound, pending_seq)
            .await?,
        0
    );
    b.seal().await?;
    Ok(())
}

/// **A writer that does not know watermarks writes in between** (an old binary,
/// a tool): the durable watermark is dropped, it does not hide the rows it wrote.
///
/// 1. Writer A leaves the timer watermark at [`FAR`] (like [`progress_floor_survives_reopen`]).
/// 2. A Db opened directly (not through `PartitionWriter` — like a binary from
///    before durable watermarks) writes a timer with deadline 10, below the
///    watermark, without touching the watermark key.
/// 3. Writer B opens.
///
/// Must see: B's `due_timers` returns the timer with deadline 10 — the
/// watermark's `writer_epoch` mark belongs to A, not to the writer right before B.
#[tokio::test]
async fn floor_dropped_when_unaware_writer_interleaves() -> Result<()> {
    let store = fresh_store("old-writer")?;
    let mut a = open_engine(&store).await?;
    churn(&mut a, 20).await?;
    a.seal().await?;

    let id = inv_id(77);
    let db = Db::builder(DB, store.clone())
        .with_settings(settings())
        .build()
        .await?;
    db.put(keys::timer(&id, 3), keys::encode_due(10, b""))
        .await?;
    db.put(keys::ix_timer(10, &id, 3), b"").await?;
    db.close().await?;

    let b = open_engine(&store).await?;
    let due = b.writer().due_timers(NOW, usize::MAX).await?.rows;
    assert!(
        due.contains(&(10, id, 3)),
        "a timer written below the watermark by a writer unaware of it is hidden: {due:?}"
    );
    b.seal().await?;
    Ok(())
}

/// **A row written below the watermark, then the process dies** — the watermark is lowered with the row's commit.
///
/// 1. Writer A leaves the watermark at [`FAR`]; writer B opens (loads the watermark).
/// 2. B writes a timer, retry, purge and delayed row with deadline 10 — below
///    the watermark —, waits for durability, then dies (no seal, no further
///    commit).
/// 3. Writer C opens.
///
/// Must see: C sees all four rows.
#[tokio::test]
async fn row_written_below_floor_then_crash_still_seen() -> Result<()> {
    let store = fresh_store("lower-then-crash")?;
    let mut a = open_engine(&store).await?;
    churn(&mut a, 20).await?;
    a.seal().await?;

    let mut b = open_engine(&store).await?;
    let id = inv_id(88);
    let mut m = Mutations::new();
    m.add_timer(10, &id, 1, b"")
        .add_retry(10, &id, &[0; 8])
        .add_purge(10, &id)
        .add_delayed(10, &id, 1, b"");
    b.writer_mut().commit(m).await?;
    b.writer().wait_durable().await?;
    drop(b);

    let c = open_engine(&store).await?;
    let w = c.writer();
    assert!(w
        .due_timers(NOW, usize::MAX)
        .await?
        .rows
        .contains(&(10, id, 1)));
    assert!(w
        .due_retries(NOW, 1_000)
        .await?
        .rows
        .iter()
        .any(|r| (r.0, r.1) == (10, id)));
    assert!(w.due_purges(NOW, 1_000).await?.rows.contains(&(10, id)));
    assert!(w
        .due_delayed(NOW, 1_000)
        .await?
        .rows
        .iter()
        .any(|r| (r.0, r.1, r.2) == (10, id, 1)));
    c.seal().await?;
    Ok(())
}

/// **Rebuilding the derived zone resets the watermark to 0**: `seq`s are
/// reissued consecutively from above every `seq` already issued
/// (`derived_rebuild_issues_no_seq_below_tombstone`).
///
/// 1. Writer A: 30 inbound messages received and then drained (inbound
///    watermark ~31), 3 still pending.
/// 2. Rebuild the derived zone (same mark) — the 3 messages get `seq` 34..=36.
/// 3. One ordinary commit, seal; writer B opens (mark matches, loads the
///    watermark).
///
/// Must see: both A (after the rebuild) and B see all 3 messages.
#[tokio::test]
async fn derived_rebuild_resets_floor_to_0() -> Result<()> {
    let store = fresh_store("rebuild")?;
    let mut a = open_engine(&store).await?;
    for cid in 0..30 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    assert_eq!(drain_inbound_pass(&mut a).await?, 30);
    assert_eq!(drain_inbound_pass(&mut a).await?, 0);
    for cid in 100..103 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    let marker = a
        .writer()
        .derived_marker()
        .await?
        .expect("engine opened with a mark");
    a.writer_mut().rebuild_derived(&marker).await?;
    let seqs: Vec<u64> = a
        .inbound_from(0, 64)
        .await?
        .iter()
        .map(|(k, _)| k.seq)
        .collect();
    assert_eq!(
        seqs,
        vec![34, 35, 36],
        "the rebuild reissues seq consecutively from above issued seqs"
    );
    a.accept_message(&completion_msg(200)).await?;
    a.seal().await?;

    let mut b = open_engine(&store).await?;
    let rows = b.inbound_from(0, 64).await?;
    assert_eq!(rows.len(), 4, "old watermark hides pending");
    b.seal().await?;
    Ok(())
}

/// **A watchdog row (FL-17) across a reopen**, and recovery reading scheduled
/// retries from the watermark.
///
/// 1. Writer A leaves the retry watermark at [`FAR`]; writer B opens (loads the
///    watermark), scans retries (the watermark rises to [`FAR`]).
/// 2. B opens a turn of `x` with a watchdog row at deadline 500 — below the
///    watermark —, schedules a real retry for `y` at deadline 700; waits for
///    durability; dies.
/// 3. Writer C opens.
///
/// Must see: C's `pending_retries_among([x, y, z])` is `{y}` — a watchdog row
/// is not a retry, `z` has no row; C's `due_retries(600)` returns `x`'s
/// watchdog row (it is still `x`'s durable wake-up); no row below the
/// watermark (FL-18).
#[tokio::test]
async fn watch_row_survives_reopen_and_recovery_reads_retry_from_floor() -> Result<()> {
    let store = fresh_store("watch")?;
    let mut a = open_engine(&store).await?;
    churn(&mut a, 20).await?;
    a.seal().await?;

    let mut b = open_engine(&store).await?;
    b.writer().due_retries(NOW, 1_000).await?;
    assert_eq!(b.writer().retry_floor(), FAR);
    let (x, y, z) = (inv_id(31), inv_id(32), inv_id(33));
    b.begin_turn(&x, Some(500), None).await?;
    assert_eq!(
        b.writer().retry_floor(),
        500,
        "a watchdog row below the watermark lowers it"
    );
    b.schedule_retry(&y, 700, 2, 0).await?;
    b.writer().wait_durable().await?;
    drop(b);

    let mut c = open_engine(&store).await?;
    assert_eq!(c.writer().rows_below_floors().await?, vec![]);
    assert!(c.writer().retry_floor() <= 500);
    let pending = c.pending_retries_among(&[x, y, z]).await?;
    assert_eq!(pending.into_iter().collect::<Vec<_>>(), vec![y]);
    let due = c.due_retries(600, 64).await?;
    assert!(
        due.iter().any(|r| (r.0, r.1) == (500, x)),
        "x's watchdog row does not fire: {due:?}"
    );
    c.seal().await?;
    Ok(())
}

/// A deterministic pseudo-random generator (LCG) — the test repeats by seed.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

/// **FL-18 under random deaths**: the durable watermark never passes a row still
/// in the store, wherever the process dies.
///
/// Each round (8 seeds × 12 rounds): a writer opens the same Db, does a batch
/// of random work — writing deadline rows (random deadlines, some below the
/// watermark), scanning and processing due rows, receiving and draining
/// inbound messages —, then either seals, or dies after waiting for
/// durability, or dies **without** waiting for durability (the non-durable
/// tail is lost).
///
/// Must see: right after every open, `rows_below_floors` is empty — no index
/// row (deadline) or live row (ordering) lies below the watermark the new
/// writer loaded.
#[tokio::test]
async fn durable_floor_never_passes_a_row_under_random_crash() -> Result<()> {
    for seed in 1..=8u64 {
        let store = fresh_store(&format!("fuzz-{seed}"))?;
        let mut rng = Lcg(seed);
        let mut now = 1_000u64;
        let mut cid = 0u32;
        for round in 0..12 {
            let mut e = open_engine(&store).await?;
            let below = e.writer().rows_below_floors().await?;
            assert!(
                below.is_empty(),
                "seed {seed} round {round}: rows below the watermark after open: {below:?}"
            );
            for _ in 0..(5 + rng.next(10)) {
                match rng.next(6) {
                    0 | 1 => {
                        let id = inv_id(rng.next(40) as u8);
                        let due = now.saturating_sub(500) + rng.next(1_500);
                        let mut m = Mutations::new();
                        match rng.next(4) {
                            0 => m.add_timer(due, &id, rng.next(3) as u32, b""),
                            1 => m.add_retry(due, &id, &[0; 8]),
                            2 => m.add_purge(due, &id),
                            _ => m.add_delayed(due, &id, rng.next(3) as u32, b""),
                        };
                        e.writer_mut().commit(m).await?;
                    }
                    2 => {
                        let w = e.writer_mut();
                        let mut m = Mutations::new();
                        for (due, id, c) in w.due_timers(now, 5).await?.rows {
                            m.remove_timer(due, &id, c);
                        }
                        for (due, id, _) in w.due_retries(now, 5).await?.rows {
                            m.remove_retry(due, &id);
                        }
                        for (due, id) in w.due_purges(now, 5).await?.rows {
                            m.remove_purge(due, &id);
                        }
                        for (due, id, c, _) in w.due_delayed(now, 5).await?.rows {
                            m.remove_delayed(due, &id, c);
                        }
                        if !m.is_empty() {
                            w.commit(m).await?;
                        }
                    }
                    3 => {
                        cid += 1;
                        e.accept_message(&completion_msg(cid)).await?;
                    }
                    4 => {
                        drain_inbound_pass(&mut e).await?;
                    }
                    _ => now += rng.next(400),
                }
            }
            match rng.next(3) {
                0 => e.seal().await?,
                1 => {
                    e.writer().wait_durable().await?;
                    drop(e);
                }
                _ => drop(e),
            }
        }
    }
    Ok(())
}

/// **A dropped durable watermark must not pull the `seq` counter below old
/// tombstones.**
///
/// The `seq` counter of the three ordering indexes is seeded at open from
/// `seq_heads`: the largest `seq` of a live row, not below the durable
/// watermark. The durable watermark is only usable when its mark belongs to
/// the writer right before; a writer that opens and dies before its first
/// commit (chaos: `kill -9` every 10 s) makes the next writer drop the
/// watermark. If every row has been drained by then, the counter goes back to 1
/// — new rows land **below** the whole stretch of old rows' tombstones, and
/// the watermark only advances to the last live row, so every later drain round
/// walks that stretch again, until compaction removes it. In a 60-minute chaos
/// run, the affected range read 5–10× more blocks per commit, until the end of
/// the run.
///
/// 1. Writer A: 200 inbound messages received and then all drained; one empty
///    drain round; one more message received and drained; seal.
/// 2. Writer B opens and dies, committing nothing (the watermark mark is still A's).
/// 3. Writer C opens; receives one message, drains; receives a second message.
///
/// Must see: C's messages carry `seq`s above every `seq` A issued, and C's
/// second drain round starts where there are no tombstones above.
#[tokio::test]
async fn dropped_floor_does_not_pull_seq_below_old_tombstone() -> Result<()> {
    let store = fresh_store("seq-restart")?;
    let mut a = open_engine(&store).await?;
    for cid in 0..200 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    assert_eq!(drain_inbound_pass(&mut a).await?, 200);
    assert_eq!(drain_inbound_pass(&mut a).await?, 0);
    a.accept_message(&completion_msg(500)).await?;
    assert_eq!(drain_inbound_pass(&mut a).await?, 1);
    a.writer().wait_durable().await?;
    a.seal().await?;

    let b = open_engine(&store).await?;
    drop(b);

    let mut c = open_engine(&store).await?;
    c.accept_message(&completion_msg(600)).await?;
    let first = c.inbound_from(0, 64).await?;
    assert_eq!(first.len(), 1);
    assert!(
        first[0].0.seq > 201,
        "the seq counter went below every seq writer A issued: {}",
        first[0].0.seq
    );
    assert_eq!(drain_inbound_pass(&mut c).await?, 1);
    c.accept_message(&completion_msg(601)).await?;
    c.writer().take_seq_scans();
    assert_eq!(c.inbound_from(0, 64).await?.len(), 1);
    let scans = c.writer().take_seq_scans();
    let start = scans
        .iter()
        .find(|(ix, _)| *ix == Index::Inbound)
        .map(|(_, s)| *s)
        .expect("the drain round has one open scan");
    assert_eq!(
        c.writer().tombstones_from(Index::Inbound, start).await?,
        0,
        "the second drain round walked over tombstones of rows A drained (starting at seq {start})"
    );
    c.seal().await?;
    Ok(())
}

/// **Seeding the `seq` counter at the start of a term does not walk over
/// tombstones below the durable watermark** (measured at 10⁶ history: the
/// first drain after becoming leader held the engine ~0.5 s, `seq_heads`
/// 270–400 ms at 3·10⁵).
///
/// `seq_heads` runs lazily at the term's first `seq` issue (`prime_outbox`),
/// within one engine hold. The old version scanned in one go from the **outbox**
/// watermark to the end of the **inbound** index: the durable watermarks of
/// signals and inbound were not used, so the scan walked over the tombstones of
/// every inbound message ever drained until compaction removed them. Every
/// invocation created through `/internal/inbound` leaves such a tombstone.
///
/// 1. Writer A: 200 inbound messages received and then all drained; one empty
///    drain round (the inbound watermark advances); one more message received
///    and left pending; seal.
/// 2. Writer B opens the same Db; one seeding (`outbox_high_water`).
///
/// Must see: the seeding walks over 0 tombstones (the old version: 200); the
/// counter is still above every `seq` A issued; the pending message is still
/// seen.
#[tokio::test]
async fn term_start_seq_never_walks_tombstones_below_floor() -> Result<()> {
    let store = fresh_store("prime-floors")?;
    let mut a = open_engine(&store).await?;
    for cid in 0..200 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    assert_eq!(drain_inbound_pass(&mut a).await?, 200);
    assert_eq!(drain_inbound_pass(&mut a).await?, 0);
    a.accept_message(&completion_msg(500)).await?;
    let pending = a.inbound_from(0, 64).await?[0].0.seq;
    a.writer().wait_durable().await?;
    a.seal().await?;

    let mut b = open_engine(&store).await?;
    assert_eq!(
        b.writer().tombstones_from(Index::Inbound, 0).await?,
        200,
        "premise: drained messages still leave tombstones on the index"
    );
    b.writer().take_seq_scans();
    let high = b.outbox_high_water().await?;
    let mut walked = 0;
    for (ix, from) in b.writer().take_seq_scans() {
        walked += b.writer().tombstones_from(ix, from).await?;
    }
    assert_eq!(
        walked, 0,
        "the term's first seq seeding walked over {walked} tombstones of drained inbound messages — \
         scanning from the outbox watermark instead of each index's own watermark"
    );
    assert!(
        high >= pending,
        "the seq counter ({high}) is below an issued seq ({pending})"
    );
    let rows = b.inbound_from(0, 64).await?;
    assert_eq!(rows.len(), 1, "the pending message is still seen");
    assert_eq!(rows[0].0.seq, pending);
    b.seal().await?;
    Ok(())
}

/// **Rebuilding the derived zone issues no `seq` below old tombstones** (same
/// family as `dropped_floor_does_not_pull_seq_below_old_tombstone`): step 2 of
/// `rebuild_derived` deletes every old index row (tombstones at the old
/// `seq`s); reissuing from 1 would put live rows **below** that whole stretch
/// of tombstones, and the scan watermark only advances to the last live row —
/// every drain round would walk the tombstone stretch again until compaction
/// removes it.
///
/// 1. Writer A: 200 inbound messages received and then drained, 3 still pending
///    (`seq` 201..=203).
/// 2. Rebuild the derived zone (same mark); seal.
/// 3. Writer B opens: one drain round, receives one more message, rescans from
///    the start.
///
/// Must see: the 3 pending messages carry consecutive `seq`s above 203 (above
/// every issued `seq`); the scan of the drain round after that starts where
/// there are no tombstones above.
#[tokio::test]
async fn derived_rebuild_issues_no_seq_below_tombstone() -> Result<()> {
    let store = fresh_store("rebuild-high")?;
    let mut a = open_engine(&store).await?;
    for cid in 0..200 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    assert_eq!(drain_inbound_pass(&mut a).await?, 200);
    for cid in 300..303 {
        a.accept_message(&completion_msg(cid)).await?;
    }
    let marker = a
        .writer()
        .derived_marker()
        .await?
        .expect("engine opened with a mark");
    a.writer_mut().rebuild_derived(&marker).await?;
    let seqs: Vec<u64> = a
        .inbound_from(0, 64)
        .await?
        .iter()
        .map(|(k, _)| k.seq)
        .collect();
    assert_eq!(seqs.len(), 3, "rebuild keeps pending: {seqs:?}");
    assert!(
        seqs[0] > 203 && seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "the rebuild issued a seq below an issued seq (≤ 203): {seqs:?}"
    );
    // In the product the rebuild runs at open, before the counter is seeded:
    // the next writer seeds the counter from the rebuilt rows.
    a.writer().wait_durable().await?;
    a.seal().await?;
    let mut b = open_engine(&store).await?;
    assert_eq!(drain_inbound_pass(&mut b).await?, 3);
    b.accept_message(&completion_msg(400)).await?;
    b.writer().take_seq_scans();
    assert_eq!(b.inbound_from(0, 64).await?.len(), 1);
    let scans = b.writer().take_seq_scans();
    let start = scans
        .iter()
        .find(|(ix, _)| *ix == Index::Inbound)
        .map(|(_, s)| *s)
        .expect("the drain round has one open scan");
    assert_eq!(
        b.writer().tombstones_from(Index::Inbound, start).await?,
        0,
        "the drain round after the rebuild walked over tombstones (starting at seq {start})"
    );
    b.seal().await?;
    Ok(())
}

/// **A capped timer scan continues from the cursor, it does not stay at the
/// head of the index.** Due rows the call site could not delete yet (the
/// invocation is mid-turn — the timer was handed to that turn) stay in the
/// index. Without a cursor they take the whole cap of every tick and the
/// timers after them never get their turn.
///
/// 1. Ten timers with deadlines `100..110`, one timer with deadline `FAR`; no
///    row is deleted.
/// 2. `due_timers(NOW, 3)` four times, then write a timer with deadline 50
///    (below the cursor), then twice more.
///
/// Must see: batches `100..103`, `103..106`, `106..109`, `109` — the batches
/// that hit the cap have `next_due` ≤ `NOW`, the last one has `next_due` =
/// `FAR`; the next round starts again at the head of the index and sees the
/// row with deadline 50 first; the timer watermark never passes the lowest row
/// still in the store (`rows_below_floors` empty).
#[tokio::test]
async fn capped_timer_scan_resumes_after_its_cursor() -> Result<()> {
    let store = fresh_store("timer-cursor")?;
    let mut e = open_engine(&store).await?;
    let w = e.writer_mut();
    let mut m = Mutations::new();
    for i in 0..10u8 {
        m.add_timer(100 + u64::from(i), &inv_id(i), 1, b"");
    }
    m.add_timer(FAR, &inv_id(200), 1, b"");
    w.commit(m).await?;

    let dues = |rows: &[(u64, [u8; 24], u32)]| rows.iter().map(|r| r.0).collect::<Vec<_>>();
    for want in [
        vec![100, 101, 102],
        vec![103, 104, 105],
        vec![106, 107, 108],
    ] {
        let got = w.due_timers(NOW, 3).await?;
        assert_eq!(dues(&got.rows), want);
        assert!(
            got.next_due.is_some_and(|d| d <= NOW),
            "cap reached ⇒ the next deadline is the next due row: {:?}",
            got.next_due
        );
        assert!(w.rows_below_floors().await?.is_empty());
    }
    let got = w.due_timers(NOW, 3).await?;
    assert_eq!(dues(&got.rows), vec![109]);
    assert_eq!(got.next_due, Some(FAR), "none due ⇒ next is FAR");

    // A new row below the cursor (deadline passed): the next round sees it first.
    let mut m = Mutations::new();
    m.add_timer(50, &inv_id(201), 1, b"");
    w.commit(m).await?;
    assert!(w.rows_below_floors().await?.is_empty());
    let got = w.due_timers(NOW, 3).await?;
    assert_eq!(dues(&got.rows), vec![50, 100, 101]);
    let got = w.due_timers(NOW, 3).await?;
    assert_eq!(dues(&got.rows), vec![102, 103, 104]);
    assert!(w.rows_below_floors().await?.is_empty());
    assert!(w.due_floor(Index::Timer) <= 50, "watermark passed row 50");
    e.seal().await?;
    Ok(())
}
