// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `Streams` table (`0x18`, bulk zone): the chunk rows of durable streams
//! (`$log`, `$trace`, `ctx.stream(name, {durable: true})`).
//!
//! Key `0x02 ‖ owner_pk ‖ 0x18 ‖ id ‖ len ‖ name ‖ first_offset(u64 BE)` (the
//! same layout pinned in `agent_pins.rs`). The tests here build **raw** keys
//! so they can run even on a binary that does not know the table: the row must
//! be an entity row of the invocation's owner (it follows the range on a split),
//! and purge deletes it together with the journal.

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
    cairn_engine::{CreateOptions, Engine, PurgeOutcome, Target},
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

const PK: u64 = 0x0a0b_0c0d_0e0f_1011;

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-streamtable-{name}-{}",
        std::process::id()
    ));
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

fn id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(PK), [n; 16])
}

async fn create(engine: &mut Engine, id: [u8; 24]) -> Result<()> {
    let target = Target {
        tenant: Default::default(),
        scope: None,
        service: "Agent".into(),
        handler: "run".into(),
        object_key: String::new(),
    };
    let mut inv = engine
        .create(
            id,
            &target,
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"\"in\""),
        )
        .await?;
    engine.step(&mut inv, Event::Start).await?;
    Ok(())
}

/// The raw key of a chunk row `(id, name, first_offset)`.
fn chunk_key(id: &[u8; 24], name: &str, first: u64) -> Vec<u8> {
    let mut k = vec![0x02];
    k.extend_from_slice(&PK.to_be_bytes());
    k.push(0x18);
    k.extend_from_slice(id);
    k.push(name.len() as u8);
    k.extend_from_slice(name.as_bytes());
    k.extend_from_slice(&first.to_be_bytes());
    k
}

/// A chunk row is an **entity row** of the invocation's owner: `table_of`
/// recognises it, the owner is the pk in the id — so the split projection keeps
/// it with the journal.
#[tokio::test]
async fn stream_rows_are_entity_rows_of_the_invocation_owner() {
    let key = chunk_key(&id(1), "$log", 0);
    assert_eq!(
        cairn_store::keys::table_of(&key).map(|t| t as u8),
        Some(0x18),
        "code 0x18 must be the Streams table"
    );
    assert_eq!(cairn_store::keys::owner_of(&key), Some(PartitionKey(PK)));
}

/// Purge deletes **every** chunk row of the invocation — every stream, every
/// chunk — in the purge commit, and does not touch rows of another invocation
/// with the same pk.
///
/// Must see: after purging `a`, the three rows of `a` are gone; `b`'s row remains.
#[tokio::test]
async fn purge_deletes_stream_rows_with_the_journal() -> Result<()> {
    let root = fresh_root("purge")?;
    let mut engine = open_engine(&root).await?;
    let (a, b) = (id(2), id(3));
    create(&mut engine, a).await?;
    create(&mut engine, b).await?;
    let mine = [
        chunk_key(&a, "$log", 0),
        chunk_key(&a, "$log", 17),
        chunk_key(&a, "$trace", 0),
    ];
    let theirs = chunk_key(&b, "$log", 0);
    for k in mine.iter().chain([&theirs]) {
        engine.writer().store().put(k, b"chunk").await?;
    }
    engine.fail_with(&a, 500, "done (test)").await?;
    assert!(matches!(engine.purge_now(&a).await?, PurgeOutcome::Purged));
    for k in &mine {
        assert_eq!(
            engine.writer().store().get(k).await?,
            None,
            "purge must delete chunk row {k:?}"
        );
    }
    assert!(
        engine.writer().store().get(&theirs).await?.is_some(),
        "another invocation's row must remain"
    );
    Ok(())
}

fn data(offset: u64, step: u32, text: &str) -> cairn_store::keys::ChunkRecord {
    cairn_store::keys::ChunkRecord {
        offset,
        step,
        attempt: 0,
        k: 0,
        body: cairn_store::keys::ChunkBody::Data(Bytes::copy_from_slice(text.as_bytes())),
    }
}

/// Reading by offset: the chunk containing `from` and every later chunk, in
/// order; the last chunk carries the next offset and the cumulative counter;
/// streams with another name (even a name that is a prefix) do not mix in.
///
/// Must see: `from` = 0, 2 (middle of the first chunk), 3 (start of the second
/// chunk), 5 (end of the stream) return the right sets of chunks; `stream_tail`
/// is the second chunk.
#[tokio::test]
async fn stream_chunks_read_from_any_offset_in_order() -> Result<()> {
    use cairn_store::{keys::StreamChunk, Mutations};
    let root = fresh_root("read")?;
    let mut engine = open_engine(&root).await?;
    let a = id(4);
    create(&mut engine, a).await?;
    let one = StreamChunk {
        total_bytes: 3,
        dropped: 0,
        records: vec![data(0, 1, "a"), data(1, 1, "b"), data(2, 2, "c")],
    };
    let two = StreamChunk {
        total_bytes: 5,
        dropped: 1,
        records: vec![data(3, 3, "d"), data(4, 3, "e")],
    };
    let mut muts = Mutations::new();
    muts.put_stream_chunk(&a, "$log", 0, &one)
        .put_stream_chunk(&a, "$log", 3, &two)
        .put_stream_chunk(&a, "$logx", 0, &two);
    engine.writer_mut().commit(muts).await?;

    let w = engine.writer();
    let firsts =
        |v: Vec<(u64, Option<StreamChunk>)>| v.into_iter().map(|(f, _)| f).collect::<Vec<_>>();
    assert_eq!(
        firsts(w.stream_chunks_unbounded(&a, "$log", 0, 1 << 20).await?),
        [0, 3]
    );
    assert_eq!(
        firsts(w.stream_chunks_unbounded(&a, "$log", 2, 1 << 20).await?),
        [0, 3]
    );
    assert_eq!(
        firsts(w.stream_chunks_unbounded(&a, "$log", 3, 1 << 20).await?),
        [3]
    );
    assert_eq!(
        firsts(w.stream_chunks_unbounded(&a, "$log", 5, 1 << 20).await?),
        Vec::<u64>::new()
    );
    // Byte cap: the first chunk is always present.
    assert_eq!(
        firsts(w.stream_chunks_unbounded(&a, "$log", 0, 1).await?),
        [0]
    );
    let got = w.stream_chunks_unbounded(&a, "$log", 0, 1 << 20).await?;
    assert_eq!(got[0].1.as_ref(), Some(&one));
    let (first, tail) = w.stream_tail_unbounded(&a, "$log").await?.expect("chunk");
    assert_eq!((first, tail.as_ref()), (3, Some(&two)));
    assert_eq!(tail.unwrap().next_offset(first), 5);
    assert_eq!(w.stream_tail_unbounded(&a, "ui").await?, None);
    Ok(())
}

/// **Artifact rows**: purge deletes every `Artifacts` row of the invocation in
/// the purge commit, without touching rows of another invocation; the epoch is
/// read from the row.
///
/// Must see: before the purge `artifact_epoch(a)` = 7; after the purge `a` has
/// no row left, `b` still does.
#[tokio::test]
async fn purge_deletes_artifact_rows_with_the_journal() -> Result<()> {
    use cairn_store::keys::ArtifactRow;
    let root = fresh_root("artifacts")?;
    let mut engine = open_engine(&root).await?;
    let (a, b) = (id(4), id(5));
    create(&mut engine, a).await?;
    create(&mut engine, b).await?;
    let row = |epoch| ArtifactRow {
        epoch,
        size: 3,
        content_type: "text/plain".into(),
    };
    let mut muts = cairn_store::Mutations::new();
    muts.put_artifact(&a, &[1; 32], &row(7))
        .put_artifact(&a, &[2; 32], &row(7))
        .put_artifact(&b, &[1; 32], &row(9));
    engine.writer_mut().commit(muts).await?;
    assert_eq!(engine.writer().artifact_epoch(&a).await?, Some(7));
    assert_eq!(
        cairn_store::keys::table_of(&cairn_store::keys::artifact(&a, &[1; 32])).map(|t| t as u8),
        Some(0x19)
    );
    engine.fail_with(&a, 500, "done (test)").await?;
    assert!(matches!(engine.purge_now(&a).await?, PurgeOutcome::Purged));
    assert!(engine.writer().artifacts_of_unbounded(&a).await?.is_empty());
    assert_eq!(engine.writer().artifacts_of_unbounded(&b).await?.len(), 1);
    Ok(())
}
