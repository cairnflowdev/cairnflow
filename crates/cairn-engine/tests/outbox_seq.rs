// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The outbox ordering index (`Index::Outbox`) on real storage.
//!
//! This table exists so the drainer reads in **production order** rather than
//! key order. But it only buys that if a main row has exactly **one** index row
//! — and what threatens that property is the most everyday thing the engine
//! does: the state machine replays tasks for every pending completion on
//! *every* replay.
//!
//! So the index's exit criterion is not "does the index work", but "does
//! replaying twice spawn anything extra".

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
    cairn_engine::{CreateOptions, Engine, Invocation, OutboxEntry, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        make_invocation_id, LeadershipGrant, PartitionId, PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Caller".into(),
        handler: "run".into(),
        object_key: "don-1".into(),
    }
}

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("cairnflow-outboxseq-{name}"));
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
    let db = Db::builder("partition-0", store)
        .with_settings(settings)
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

async fn start(engine: &mut Engine, n: u8) -> Result<Invocation> {
    let id = inv_id(n);
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    Ok(inv)
}

/// A `ctx.call` to another service, waiting for the result.
fn call(completion_id: u32, id_completion: u32) -> Frame {
    frame(
        MessageType::CALL,
        &pb::CallCommandMessage {
            service_name: "Payment".into(),
            handler_name: "charge".into(),
            key: "acct-1".into(),
            parameter: Bytes::new(),
            headers: vec![],
            idempotency_key: None,
            result_completion_id: completion_id,
            invocation_id_notification_idx: id_completion,
            name: String::new(),
            scope: None,
            limit_key: None,
            placement: 0,
            tags: vec![],
        },
    )
}

/// **The index's exit criterion.** Replaying the task twice ⇒ one index row,
/// one call.
///
/// This is the case that made D-O1 reject "switch the outbox key straight to
/// `seq`", and the case D-O2 exists to block. If `push_outbox` issued a new
/// `seq` for an `(invocation, completion)` that already has a row, the main
/// table would still be right — it is deterministically keyed, a rewrite is an
/// overwrite — but the **index** would have two rows pointing at the same main
/// row. The drainer reads the index, so it would see two pieces of work and
/// send that call twice.
///
/// Exactly what the main table's deduplication cannot save: it deduplicates by
/// *identity*, not by the number of times something is seen.
#[tokio::test]
async fn replayed_task_adds_no_index_row() -> Result<()> {
    let root = fresh_root("replay")?;
    let mut engine = open_engine(&root).await?;

    let mut a = start(&mut engine, 1).await?;
    engine.step(&mut a, Event::FromSdk(call(1, 2))).await?;

    let after_first = engine.writer().all_outbox_seq_unbounded().await?;
    assert_eq!(
        after_first.len(),
        1,
        "a single `ctx.call` must give exactly one index row"
    );
    let first_seq = after_first[0].0;

    // Replay: reload from the journal and run again from `Start` — exactly the
    // code path recovery after a crash takes, and exactly where the state
    // machine replays tasks for pending completions.
    for run in 2..=3 {
        let mut lai = engine.load(inv_id(1), StartParams::default()).await?;
        engine.step(&mut lai, Event::Start).await?;

        let rows_after = engine.writer().all_outbox_seq_unbounded().await?;
        assert_eq!(
            rows_after.len(),
            1,
            "replay number {run} spawned an extra index row ⇒ the drainer would send this call twice"
        );
        assert_eq!(
            rows_after[0].0, first_seq,
            "replay number {run} issued a new `seq` instead of reusing the old one (D-O2)"
        );
    }

    // And the main table still has exactly one row — the deduplication of
    // `(invocation, completion)` is not touched.
    assert_eq!(engine.pending_outbox(16).await?.len(), 1);
    Ok(())
}

/// `seq` follows **production order**, not the main table's key order.
///
/// The main table is keyed by `(invocation, completion)`, and an invocation id
/// is a hashed number — so its scan order carries no information about
/// production order. That is the reason the whole index table exists; a test
/// that reads back exactly the production order is the only way to know it
/// really buys that.
#[tokio::test]
async fn cursor_reads_in_creation_order() -> Result<()> {
    let root = fresh_root("order")?;
    let mut engine = open_engine(&root).await?;

    // Three invocations, ids increasing *numerically*, but we make them call in
    // reverse order. A reader that followed the main table would return them by id.
    let mut invs = Vec::new();
    for n in [3u8, 1, 2] {
        invs.push((n, start(&mut engine, n).await?));
    }
    for (_, inv) in invs.iter_mut() {
        engine.step(inv, Event::FromSdk(call(1, 2))).await?;
    }

    let rows = engine.pending_outbox(16).await?;
    assert_eq!(rows.len(), 3);

    let seqs: Vec<u64> = rows.iter().map(|(r, _)| r.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "cursor must return increasing `seq`");

    let doc_ra: Vec<[u8; 24]> = rows.iter().map(|(r, _)| r.key.0).collect();
    assert_eq!(
        doc_ra,
        vec![inv_id(3), inv_id(1), inv_id(2)],
        "read in production order, not the main table's key order"
    );
    Ok(())
}

/// Reading on from the cursor skips exactly the drained part, not a new snapshot.
#[tokio::test]
async fn read_continues_from_cursor() -> Result<()> {
    let root = fresh_root("contro")?;
    let mut engine = open_engine(&root).await?;

    for n in 1..=3u8 {
        let mut inv = start(&mut engine, n).await?;
        engine.step(&mut inv, Event::FromSdk(call(1, 2))).await?;
    }

    let all_rows = engine.pending_outbox(16).await?;
    assert_eq!(all_rows.len(), 3);

    let after_first_row = engine.outbox_from(all_rows[0].0.seq + 1, 16).await?;
    assert_eq!(
        after_first_row.len(),
        2,
        "reading from the cursor must skip drained rows"
    );
    assert_eq!(after_first_row[0].0, all_rows[1].0);
    Ok(())
}

/// Cutting a row makes **both** rows disappear — no orphan pointer is left.
///
/// Missing the index row would leave a pointer to a main row that no longer
/// exists, and the drainer would have to guess whether that means "processed"
/// or "broken". Nothing to guess is the property we want, so it deserves a
/// test.
#[tokio::test]
async fn trimming_a_row_deletes_its_index() -> Result<()> {
    let root = fresh_root("cat")?;
    let mut engine = open_engine(&root).await?;

    let mut a = start(&mut engine, 1).await?;
    engine.step(&mut a, Event::FromSdk(call(1, 2))).await?;

    let rows = engine.pending_outbox(16).await?;
    let (row, entry) = &rows[0];
    assert!(matches!(entry, OutboxEntry::Call(_)));

    engine.drop_outbox(*row).await?;

    assert!(engine.pending_outbox(16).await?.is_empty());
    assert!(
        engine.writer().all_outbox_seq_unbounded().await?.is_empty(),
        "an index row left ⇒ the cursor points at an empty slot"
    );
    assert!(
        engine
            .writer()
            .get(&cairn_store::keys::outbox(row.owner, &row.key.0, row.key.1))
            .await?
            .is_none(),
        "a main row left ⇒ the work is never drained again"
    );
    Ok(())
}

/// One drain round like `turn::drain_outbox`: read from cursor 0, advance by
/// `seq`, delete every row read.
async fn drain_outbox_pass(engine: &mut Engine) -> Result<usize> {
    let mut cursor = 0;
    let mut n = 0;
    loop {
        let rows = engine.outbox_from(cursor, 32).await?;
        let Some((last, _)) = rows.last() else {
            return Ok(n);
        };
        cursor = last.seq + 1;
        for (row, _) in rows {
            engine.drop_outbox(row).await?;
            n += 1;
        }
    }
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

fn completion_msg(completion_id: u32) -> cairn_engine::InterPartition {
    cairn_engine::InterPartition::Completion(cairn_engine::DeliverCompletion {
        target: inv_id(1),
        completion_id,
        result: cairn_engine::CompletionPayload::Void,
    })
}

/// The total number of tombstones on the path of the ordering-index scans since
/// the previous reading — each scan walks over every row deleted since where it
/// starts.
async fn tombstones_walked(engine: &Engine) -> Result<u64> {
    let mut n = 0;
    for (ix, from) in engine.writer().take_seq_scans() {
        n += engine.writer().tombstones_from(ix, from).await?;
    }
    Ok(n)
}

/// **An empty drain round does not walk over the tombstones of drained rows.**
///
/// The drainers (`turn::drain_outbox`, `drain_inbound`) open each round with
/// `outbox_from(0, …)` / `inbound_from(0, …)`. Drained rows are deleted, but a
/// delete in an LSM is a tombstone write, and tombstones lie on the scan path
/// until compaction removes them. Scanning from 0 every round therefore costs
/// O(rows ever drained) rather than O(rows pending): after the
/// `call`/`send`/`xcall` scenarios of `harness/bench/load.ts`, CPU per call grew 4–8×
/// and `sample` pointed at
///
/// 1. Produce and drain all `N` outbox rows (`N` `ctx.call`s), one row per
///    round — like the drain tick after every SDK turn.
/// 2. Receive and drain all `N` inbound messages.
/// 3. One more empty drain round of each drainer.
///
/// Must see: the empty round in step 3 walks over O(1) tombstones, not ~`N` per
/// index. Counted by the number of tombstone keys from the start point of each
/// scan, not by time.
#[tokio::test]
async fn empty_drain_pass_skips_tombstones_of_drained_rows() -> Result<()> {
    const N: u8 = 200;
    let root = fresh_root("tombstone")?;
    let mut engine = open_engine(&root).await?;

    for n in 1..=N {
        let mut inv = start(&mut engine, n).await?;
        engine.step(&mut inv, Event::FromSdk(call(1, 2))).await?;
        assert_eq!(drain_outbox_pass(&mut engine).await?, 1);
    }
    for cid in 0..u32::from(N) {
        engine.accept_message(&completion_msg(100 + cid)).await?;
        assert_eq!(drain_inbound_pass(&mut engine).await?, 1);
    }
    assert!(
        engine
            .writer()
            .tombstones_from(cairn_store::keys::Index::Outbox, 0)
            .await?
            >= u64::from(N),
        "premise: drained rows still leave tombstones on the index"
    );

    engine.writer().take_seq_scans();
    assert_eq!(drain_outbox_pass(&mut engine).await?, 0);
    assert_eq!(drain_inbound_pass(&mut engine).await?, 0);
    let walked = tombstones_walked(&engine).await?;
    assert!(
        walked <= 2,
        "an empty drain round walked over {walked} tombstones after {N} outbox rows + {N} inbound messages drained \
         — every round rescans the index from the start"
    );
    Ok(())
}

/// The scan watermark must not skip **live** rows below it.
///
/// 1. Three calls ⇒ three outbox rows `a < b < c`. One round reads all three
///    (the watermark passes `c`) but drains only `b` — `a` and `c` are skipped
///    as when the caller's lock is busy or the target is dead.
/// 2. Replay `a` from the journal: the state machine rewrites its row with the
///    old `seq` (D-O2) — an index row written **below** the watermark.
///
/// Must see: the next round still reads `a` then `c`, exactly once each, by
/// `seq`; once everything is drained, an empty round walks over no tombstones.
#[tokio::test]
async fn skipped_row_below_floor_is_still_reread() -> Result<()> {
    let root = fresh_root("skipped")?;
    let mut engine = open_engine(&root).await?;

    for n in 1..=3u8 {
        let mut inv = start(&mut engine, n).await?;
        engine.step(&mut inv, Event::FromSdk(call(1, 2))).await?;
    }
    let rows = engine.outbox_from(0, 32).await?;
    assert_eq!(rows.len(), 3);
    let (a, b, c) = (rows[0].0, rows[1].0, rows[2].0);
    engine.drop_outbox(b).await?;

    let lai = engine.outbox_from(0, 32).await?;
    let seqs: Vec<u64> = lai.iter().map(|(r, _)| r.seq).collect();
    assert_eq!(
        seqs,
        vec![a.seq, c.seq],
        "a row skipped below the watermark must be read again"
    );
    // Reading on from the cursor (the second loop of the same round) returns no rows.
    assert!(engine.outbox_from(c.seq + 1, 32).await?.is_empty());

    let mut replayed = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut replayed, Event::Start).await?;
    let rows_after = engine.outbox_from(0, 32).await?;
    let seqs: Vec<u64> = rows_after.iter().map(|(r, _)| r.seq).collect();
    assert_eq!(
        seqs,
        vec![a.seq, c.seq],
        "rewriting row `a` with its old `seq` (D-O2) below the watermark: must be seen exactly once"
    );

    assert_eq!(drain_outbox_pass(&mut engine).await?, 2);
    engine.writer().take_seq_scans();
    assert_eq!(drain_outbox_pass(&mut engine).await?, 0);
    assert_eq!(tombstones_walked(&engine).await?, 0);
    Ok(())
}

/// An index row written with a `seq` **smaller than the watermark** — issued
/// earlier, committed late — is still seen by the drainer.
///
/// 1. Three inbound messages, all drained: the watermark passes the `seq` of
///    all three.
/// 2. Write directly (not through the engine's `seq` issuer) an inbound row
///    carrying the first message's `seq` — `seq` issue order differs from
///    commit order.
///
/// Must see: `inbound_from(0, …)` returns that row. The watermark does not
/// assume a new row's `seq` is always greater than every row seen; `commit`
/// catches every index row written below the watermark.
#[tokio::test]
async fn late_commit_row_below_floor_is_still_seen() -> Result<()> {
    let root = fresh_root("late")?;
    let mut engine = open_engine(&root).await?;

    for cid in 0..3 {
        engine.accept_message(&completion_msg(100 + cid)).await?;
    }
    let first = engine.inbound_from(0, 64).await?[0].0.seq;
    assert_eq!(drain_inbound_pass(&mut engine).await?, 3);
    assert!(engine.inbound_from(0, 64).await?.is_empty());

    let msg = completion_msg(999);
    let mut muts = cairn_store::Mutations::new();
    muts.push_inbound(
        PartitionKey(0),
        first,
        cairn_store::InboundKind::Completion,
        &inv_id(1),
        999,
        &msg.encode(),
    );
    engine.writer_mut().commit(muts).await?;

    let rows = engine.inbound_from(0, 64).await?;
    assert_eq!(rows.len(), 1, "late commit below watermark missed");
    assert_eq!((rows[0].0.seq, rows[0].0.completion_id), (first, 999));
    Ok(())
}

/// **The retry scan watermark**: a removed scheduled retry is not scanned again
/// on the next tick — the watermark jumps over a stretch of only tombstones —,
/// and a scheduled retry written **below** the watermark (a deadline in the
/// past) is still seen: the commit lowers the watermark.
///
/// Must see: after removing everything, the watermark is `u64::MAX` (no rows
/// left); a row written afterwards with a 1 ms deadline is still in `due_retries`.
#[tokio::test]
async fn cleared_retry_not_rescanned_and_row_below_floor_still_seen() -> Result<()> {
    let root = fresh_root("retry-floor")?;
    let mut engine = open_engine(&root).await?;
    for n in 1..=5u8 {
        engine
            .schedule_retry(&inv_id(n), 100 + n as u64, 1, 0)
            .await?;
    }
    let due = engine.due_retries(1_000, 64).await?;
    assert_eq!(due.len(), 5);
    assert_eq!(
        engine.writer().retry_floor(),
        101,
        "the watermark stops at the first live row"
    );
    for (d, id, _, _) in due {
        engine.cancel_retry(&id, d).await?;
    }
    assert!(engine.due_retries(1_000, 64).await?.is_empty());
    assert_eq!(
        engine.writer().retry_floor(),
        u64::MAX,
        "no rows left: the watermark jumps over every tombstone"
    );

    engine.schedule_retry(&inv_id(9), 1, 1, 0).await?;
    assert_eq!(
        engine.writer().retry_floor(),
        1,
        "a commit writing below the watermark lowers it"
    );
    let due = engine.due_retries(1_000, 64).await?;
    assert_eq!(
        due.iter().map(|r| r.1).collect::<Vec<_>>(),
        vec![inv_id(9)],
        "a row below the watermark was missed"
    );
    Ok(())
}

/// **The watchdog row**: `begin_turn` writes it in the same commit as
/// `Running`; `pending_retry` does not treat it as a retry (recovery on
/// becoming leader does not wait for it); it still fires like a scheduled
/// retry; `end_turn` removes it.
#[tokio::test]
async fn watch_row_fires_like_retry_and_clears_with_final_commit() -> Result<()> {
    let root = fresh_root("watch")?;
    let mut engine = open_engine(&root).await?;
    let _inv = start(&mut engine, 1).await?;
    let id = inv_id(1);
    engine.begin_turn(&id, Some(500), None).await?;
    assert_eq!(
        engine.pending_retry(&id).await?,
        None,
        "a watchdog row is not a retry"
    );
    let due = engine.due_retries(1_000, 64).await?;
    assert_eq!(
        due.iter().map(|r| (r.0, r.1, r.2)).collect::<Vec<_>>(),
        vec![(500, id, 0)]
    );
    engine
        .end_turn(&id, false, cairn_store::Stage::Suspended, Some(500), None)
        .await?;
    assert!(
        engine.due_retries(1_000, 64).await?.is_empty(),
        "end_turn removes the watchdog row"
    );
    // Ending with a retry: one commit, the new row is a real retry.
    engine.begin_turn(&id, Some(600), None).await?;
    engine
        .end_turn(
            &id,
            false,
            cairn_store::Stage::Suspended,
            Some(600),
            Some((700, 1, 3)),
        )
        .await?;
    assert_eq!(engine.pending_retry(&id).await?, Some(700));
    let due = engine.due_retries(1_000, 64).await?;
    assert_eq!(
        due.iter().map(|r| (r.0, r.2, r.3)).collect::<Vec<_>>(),
        vec![(700, 1, 3)]
    );
    Ok(())
}

/// **Stale index rows count toward the scan round's cap.** Ten invocations
/// have a **stale** retry index row with deadline 100 (the entity row moved
/// away while the write did not delete the old index row — like the
/// `PermitGranted` of a `planned` turn deleting by the fire's `next_due` while
/// the row had been moved far out because of a parked ticket), plus one live
/// scheduled retry with deadline 200. Scan with a cap of 2.
///
/// Must see: each scan round reads ≤ 2 index rows, stale rows included — the
/// live scheduled retry does not arrive in the first round, it arrives after a
/// few rounds (the background loop wakes again right away because the next
/// deadline is ≤ `now`). Before the fix: stale rows did not count toward the
/// cap — the first round read and cleaned up the whole cluster in one hold
/// (after 24 h, a wave of `planned` tickets leaves a whole cluster with the
/// same deadline).
#[tokio::test]
async fn stray_index_rows_count_toward_scan_cap() -> Result<()> {
    let root = fresh_root("stale-retry")?;
    let mut engine = open_engine(&root).await?;
    // A live invocation (`running`, with a queue entry): the row move belongs
    // to an unfinished invocation, like the background tick's moves — a retry
    // row of an invocation without status or entry is outside the valid set
    // (`lifecycle-check`).
    let q = cairn_store::QueueName {
        tenant: Default::default(),
        scope: None,
        limit: None,
        exclusive: false,
        service: "Caller".into(),
        object_key: String::new(),
    };
    for n in (1..=10u8).chain([42]) {
        let seq = engine.next_queue_seq(&q).await?;
        let mut inv = engine
            .create(
                inv_id(n),
                &target(),
                CreateOptions {
                    queue: Some((&q, seq)),
                    ..Default::default()
                },
                StartParams::default(),
                Bytes::new(),
            )
            .await?;
        engine.step(&mut inv, Event::Start).await?;
    }
    for n in 1..=10u8 {
        engine.schedule_retry(&inv_id(n), 100, 1, 0).await?;
        // Move the entity row far out without deleting the index row with deadline 100.
        engine
            .apply_lifecycle(
                &inv_id(n),
                None,
                cairn_engine::StageWrite::Keep,
                cairn_engine::RowWrite::Set {
                    old_due: Some(99),
                    row: cairn_lifecycle::RetryRow::retry(1_000_000_000, 1, 0, 0),
                },
            )
            .await?;
    }
    engine.schedule_retry(&inv_id(42), 200, 1, 0).await?;
    let first = engine.due_retries(1_000, 2).await?;
    assert!(
        first.is_empty(),
        "the first round read more than the cap of 2 index rows: {first:?}"
    );
    let mut got = Vec::new();
    for _ in 0..10 {
        got.extend(engine.due_retries(1_000, 2).await?);
        if !got.is_empty() {
            break;
        }
    }
    assert_eq!(
        got.iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        vec![(200, inv_id(42))]
    );
    Ok(())
}
