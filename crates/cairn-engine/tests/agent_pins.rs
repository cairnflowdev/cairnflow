// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Pins of the **existing** behaviour that artifacts and the `Streams` table
//! build on. Every test here was green on the code before those features; a
//! later change of any of these behaviours fixes exactly the test that pins it,
//! in the same commit.
//!
//! - Restart copies the prefix: `restart` copies entries 1..keep of the old
//!   copy **byte for byte** to the new id, and the old copy can be purged while
//!   the new one is still running. An artifact reference in the prefix
//!   therefore has **two** owners after a restart — the reason for AP-10.
//! - Idempotent ids are reused: the id of an invocation with an idempotency
//!   key is deterministic; purging and recreating with the same key yields
//!   **the same** id. An artifact path without an epoch would make cleaning up
//!   the old copy delete the new copy's too — the reason for AP-8.
//! - Unknown table codes: a binary that does not know table code `0x18`
//!   ignores its rows, and purge **leaks** them — the reason the `Streams`
//!   table needs the `streams/table` gate. The binary now knows the table; the
//!   test moved to `streams_table.rs` (the reverse direction).
//! - Unknown entry-0 headers (`cairnflow-artifact-epoch`) do not break an old
//!   binary: load, run, restart (copied verbatim) and purge all work.

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
    cairn_engine::{CreateOptions, Engine, InvocationStatus, PurgeOutcome, RestartOutcome, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Event, JournalEntry, StartParams},
    cairn_store::{
        idempotent_invocation_id, make_invocation_id, LeadershipGrant, Mutations, PartitionId,
        PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const PK: u64 = 0x0102_0304_0506_0708;

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root =
        std::env::temp_dir().join(format!("cairnflow-agentpins-{name}-{}", std::process::id()));
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

/// The new copy's queue on restart — as the shell issues it (a `running` new
/// copy must be in a queue: the validity check when `lifecycle-check` is on).
async fn requeue(engine: &mut Engine) -> Result<(cairn_store::QueueName, u64)> {
    let q = cairn_store::QueueName {
        tenant: Default::default(),
        scope: None,
        limit: None,
        exclusive: false,
        service: "Agent".into(),
        object_key: String::new(),
    };
    let seq = engine.next_queue_seq(&q).await?;
    Ok((q, seq))
}

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Agent".into(),
        handler: "run".into(),
        object_key: String::new(),
    }
}

fn id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(PK), [n; 16])
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

/// A `ctx.run` that already has a result — a journal command carrying a
/// `value` payload (the shape of a step with a large output that an artifact
/// would replace with a reference).
fn run_with(name: &str, cid: u32, value: &[u8]) -> Vec<Frame> {
    vec![
        frame(
            MessageType::RUN,
            &pb::RunCommandMessage {
                result_completion_id: cid,
                name: name.into(),
            },
        ),
        frame(
            MessageType::PROPOSE_RUN_COMPLETION,
            &pb::ProposeRunCompletionMessage {
                result_completion_id: cid,
                result: Some(pb::propose_run_completion_message::Result::Value(
                    Bytes::copy_from_slice(value),
                )),
            },
        ),
    ]
}

/// Creates `id` and then runs two `ctx.run`s with output; returns the journal length.
async fn create_with_runs(engine: &mut Engine, id: [u8; 24], idem: Option<&str>) -> Result<u32> {
    let mut inv = engine
        .create(
            id,
            &target(),
            CreateOptions {
                idempotency_key: idem,
                ..CreateOptions::default()
            },
            StartParams::default(),
            Bytes::from_static(b"\"in\""),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    for (i, f) in run_with("a", 1, &[0xAA; 4096])
        .into_iter()
        .chain(run_with("b", 2, &[0xBB; 4096]))
        .enumerate()
    {
        let _ = i;
        engine.step(&mut inv, Event::FromSdk(f)).await?;
    }
    Ok(engine.journal_len(&id).await?)
}

async fn finish(engine: &mut Engine, id: &[u8; 24]) -> Result<()> {
    engine.fail_with(id, 500, "done (test)").await?;
    Ok(())
}

/// **Restart copies the prefix.** `restart` keeping `keep` entries copies
/// entries 1..keep byte for byte to the new id; purging the old copy (already
/// `completed` with `continuedAsNew`) works while the new one is still
/// `running`, and does not touch the new copy's journal.
///
/// Must see: entry i ≥ 1 of the new copy is **byte-identical** to entry i of the
/// old one (an artifact reference in it then has two owners); purging the old
/// copy returns `Purged`; the new copy keeps its whole journal and is `running`.
#[tokio::test]
async fn restart_copies_prefix_bytes_and_old_purges_while_new_runs() -> Result<()> {
    let root = fresh_root("k1")?;
    let mut engine = open_engine(&root).await?;
    let (old, new) = (id(1), id(2));
    let len = create_with_runs(&mut engine, old, None).await?;
    assert!(len >= 3, "journal needs entry 0 + runs: {len}");
    let old_rows = engine.writer().journal_unbounded(&old).await?;

    let (q, seq) = requeue(&mut engine).await?;
    let out = engine
        .restart(&old, &new, len, None, None, Some((&q, seq)))
        .await?;
    assert!(matches!(out, RestartOutcome::Restarted { .. }), "{out:?}");
    let new_rows = engine.writer().journal_unbounded(&new).await?;
    assert_eq!(new_rows.len(), old_rows.len());
    for ((i, a), (j, b)) in old_rows.iter().zip(&new_rows).skip(1) {
        assert_eq!(i, j);
        assert_eq!(a, b, "entry {i}: new copy differs");
    }
    // Entry 0 differs: it carries `cairnflow-restarted-from`.
    assert_ne!(old_rows[0].1, new_rows[0].1);

    assert!(matches!(
        engine.status(&old).await?,
        Some(InvocationStatus::Completed(_))
    ));
    assert_eq!(engine.status(&new).await?, Some(InvocationStatus::Running));
    let purged = engine.purge_now(&old).await?;
    assert!(matches!(purged, PurgeOutcome::Purged), "{purged:?}");
    assert!(engine.writer().journal_unbounded(&old).await?.is_empty());
    assert_eq!(engine.writer().journal_unbounded(&new).await?, new_rows);
    assert_eq!(engine.status(&new).await?, Some(InvocationStatus::Running));
    Ok(())
}

/// **Idempotent ids are reused.** The id of an invocation carrying an
/// idempotency key is a function of the key set; after a purge, recreating
/// with the same key yields **the same** id and a new journal in exactly that
/// place.
///
/// Must see: the two derivations agree; after the purge it can be recreated and
/// the status goes back to `running` with a journal of only entry 0.
#[tokio::test]
async fn purge_then_recreate_with_same_idempotency_key_reuses_the_id() -> Result<()> {
    let root = fresh_root("k2")?;
    let mut engine = open_engine(&root).await?;
    let t = target();
    let derive = || {
        idempotent_invocation_id(
            PartitionKey(PK),
            &t.tenant,
            None,
            &t.service,
            &t.handler,
            &t.object_key,
            "chat-42",
        )
    };
    let first = derive();
    assert_eq!(first, derive(), "the derivation must be deterministic");

    create_with_runs(&mut engine, first, Some("chat-42")).await?;
    finish(&mut engine, &first).await?;
    assert!(matches!(
        engine.purge_now(&first).await?,
        PurgeOutcome::Purged
    ));
    assert_eq!(engine.status(&first).await?, None);

    let again = derive();
    assert_eq!(again, first, "same key after purge gives the same id");
    let mut inv = engine
        .create(
            again,
            &t,
            CreateOptions {
                idempotency_key: Some("chat-42"),
                ..CreateOptions::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    assert_eq!(
        engine.status(&again).await?,
        Some(InvocationStatus::Running)
    );
    assert_eq!(engine.journal_len(&again).await?, 1);
    Ok(())
}

// The unknown-table pin (`binary_without_0x18_leaks_its_rows_on_purge`) pinned the
// behaviour of a binary that did not know the `Streams` table; it was replaced
// by `streams_table.rs::purge_deletes_stream_rows_with_the_journal` — the same
// raw keys, the reverse direction. Old binaries still leak: the `streams/table`
// gate keeps them out.

/// Adds header `k: v` to entry 0 of `id` (overwritten by an ordinary commit).
async fn add_entry0_header(engine: &mut Engine, id: &[u8; 24], k: &str, v: &str) -> Result<()> {
    let raw = engine.writer().journal_unbounded(id).await?[0].1.clone();
    let first = JournalEntry::decode(raw)?;
    let frame = Frame::new(first.ty, Flags::NONE, first.payload.clone());
    let Ok(Message::Input(mut msg)) = cairn_protocol::decode(&frame) else {
        anyhow::bail!("entry 0 is not Input");
    };
    msg.headers.push(pb::Header {
        key: k.into(),
        value: v.into(),
    });
    let wire = Message::Input(msg).encode(Flags::NONE);
    let entry = JournalEntry::new(MessageType::INPUT, wire.slice(cairn_protocol::HEADER_LEN..));
    let mut muts = Mutations::new();
    muts.append_journal(id, 0, &entry.encode());
    engine.writer_mut().commit(muts).await?;
    Ok(())
}

fn entry0_headers(raw: &Bytes) -> Result<Vec<(String, String)>> {
    let first = JournalEntry::decode(raw.clone())?;
    let frame = Frame::new(first.ty, Flags::NONE, first.payload.clone());
    let Ok(Message::Input(msg)) = cairn_protocol::decode(&frame) else {
        anyhow::bail!("entry 0 is not Input");
    };
    Ok(msg.headers.into_iter().map(|h| (h.key, h.value)).collect())
}

/// **Unknown entry-0 headers.** A `cairnflow-artifact-epoch` header in entry 0
/// does not break today's binary: reloading the machine, replaying, running
/// on, restart (copying the header verbatim) and purge all work.
///
/// Must see: `load` + `Start` succeed and replay every entry; the rebuilt copy
/// carries **the same** header value (today: copied verbatim — a change that
/// gives the copy a new epoch fixes this test in the same commit); both can be
/// purged.
#[tokio::test]
async fn unknown_entry0_header_is_carried_and_ignored() -> Result<()> {
    let root = fresh_root("entry0-header")?;
    let (old, new) = (id(4), id(5));
    let len = {
        let mut engine = open_engine(&root).await?;
        let len = create_with_runs(&mut engine, old, None).await?;
        add_entry0_header(
            &mut engine,
            &old,
            "cairnflow-artifact-epoch",
            "00000000deadbeef",
        )
        .await?;
        engine.writer().wait_durable().await?;
        len
    };
    // Reopen: a leader change, the warm machine is lost.
    let mut engine = open_engine(&root).await?;
    let mut inv = engine.load(old, StartParams::default()).await?;
    let replay = engine.step(&mut inv, Event::Start).await?;
    assert!(
        replay.len() >= len as usize,
        "replay must cover all {len} entries: {}",
        replay.len()
    );
    let (q, seq) = requeue(&mut engine).await?;
    let out = engine
        .restart(&old, &new, len, None, None, Some((&q, seq)))
        .await?;
    assert!(matches!(out, RestartOutcome::Restarted { .. }), "{out:?}");
    let h = entry0_headers(&engine.writer().journal_unbounded(&new).await?[0].1)?;
    assert!(
        h.contains(&("cairnflow-artifact-epoch".into(), "00000000deadbeef".into())),
        "restart today copies the header verbatim: {h:?}"
    );
    assert!(matches!(
        engine.purge_now(&old).await?,
        PurgeOutcome::Purged
    ));
    finish(&mut engine, &new).await?;
    assert!(matches!(
        engine.purge_now(&new).await?,
        PurgeOutcome::Purged
    ));
    Ok(())
}
