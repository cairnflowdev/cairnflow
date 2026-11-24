// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `cairnflow-state-lock` mark of entry 0 (D-W5): the rule "a virtual
//! object's `SHARED` handlers do not write state" is fixed **at invocation
//! creation**, like the `cairnflow-call-prefix` mark.
//!
//! The engine layer does not read the deployment registry: the turn layer
//! decides whether this turn runs a VO's `SHARED` handler and then calls
//! [`Invocation::mark_shared_object_handler`]. Here we check the engine's part —
//! the mark is written on every new entry 0 and on `restart`'s successor; with
//! the mark the machine rejects write commands; without it (an entry 0 from an
//! old binary) the old rule is kept; an unmarked turn (`EXCLUSIVE`) writes as
//! before.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{anyhow, bail, ensure, Result},
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, Invocation, RestartOutcome, Target, HDR_STATE_LOCK},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Event, JournalEntry, Outcome, StartParams},
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
        service: "Cart".into(),
        handler: "peek".into(),
        object_key: "k".into(),
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

fn set_state(k: &str, v: &str) -> Event {
    Event::FromSdk(frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::copy_from_slice(k.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::copy_from_slice(
                v.as_bytes(),
            ))),
            name: String::new(),
        },
    ))
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "cairnflow-state-lock-{name}-{}",
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

async fn create(engine: &mut Engine, n: u8) -> Result<()> {
    engine
        .create(
            inv_id(n),
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    Ok(())
}

/// The entry-0 headers of `id`.
async fn entry0_headers(engine: &Engine, id: &[u8; 24]) -> Result<Vec<pb::Header>> {
    let rows = engine.writer().journal_unbounded(id).await?;
    let (_, raw) = rows.first().ok_or_else(|| anyhow!("no journal"))?;
    let entry = JournalEntry::decode(raw.clone())?;
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    let Message::Input(input) = cairn_protocol::decode(&frame)? else {
        bail!("entry 0 is not Input");
    };
    Ok(input.headers)
}

fn marked(headers: &[pb::Header]) -> bool {
    headers
        .iter()
        .any(|h| h.key == HDR_STATE_LOCK && h.value == "on")
}

/// Rewrites entry 0 without the mark — exactly the entry 0 a binary from before D-W5 wrote.
async fn strip_mark(engine: &mut Engine, id: &[u8; 24]) -> Result<()> {
    let mut headers = entry0_headers(engine, id).await?;
    let rows = engine.writer().journal_unbounded(id).await?;
    let entry = JournalEntry::decode(rows[0].1.clone())?;
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    let Message::Input(mut input) = cairn_protocol::decode(&frame)? else {
        bail!("entry 0 is not Input");
    };
    headers.retain(|h| h.key != HDR_STATE_LOCK);
    input.headers = headers;
    let wire = Message::Input(input).encode(Flags::NONE);
    let entry0 = JournalEntry::new(MessageType::INPUT, wire.slice(cairn_protocol::HEADER_LEN..));
    let mut m = cairn_store::Mutations::new();
    m.append_journal(id, 0, &entry0.encode());
    engine.writer_mut().commit(m).await?;
    Ok(())
}

/// Loads `n`, (optionally) marks the turn as a VO `SHARED` handler, runs `Start`
/// and then one `SetState`.
async fn turn_writing_state(engine: &mut Engine, n: u8, shared: bool) -> Result<Invocation> {
    let mut inv = engine.load(inv_id(n), StartParams::default()).await?;
    if shared {
        inv.mark_shared_object_handler();
    }
    engine.step(&mut inv, Event::Start).await?;
    engine.step(&mut inv, set_state("items", "\"x\"")).await?;
    Ok(inv)
}

fn refused(inv: &Invocation) -> bool {
    matches!(
        inv.outcome(),
        Some(Outcome::Failed { code, message, .. })
            if *code == cairn_protocol::PROTOCOL_VIOLATION && message.contains("SHARED")
    )
}

/// Every new entry 0 carries the mark.
#[tokio::test]
async fn new_entry0_carries_the_mark() -> Result<()> {
    let root = fresh_root("new")?;
    let mut engine = open_engine(&root).await?;
    create(&mut engine, 1).await?;
    ensure!(marked(&entry0_headers(&engine, &inv_id(1)).await?));
    Ok(())
}

/// Marked + a `SHARED` turn ⇒ 571, the write command does not enter the
/// journal. The same invocation, an unmarked turn (`EXCLUSIVE`) ⇒ writes as before.
#[tokio::test]
async fn marked_shared_turn_refuses_writes_and_exclusive_turn_writes() -> Result<()> {
    let root = fresh_root("marked")?;
    let mut engine = open_engine(&root).await?;
    create(&mut engine, 1).await?;
    create(&mut engine, 2).await?;

    let inv = turn_writing_state(&mut engine, 1, true).await?;
    ensure!(refused(&inv), "marked SHARED turn: {:?}", inv.outcome());
    ensure!(
        inv.known_entries() == 1,
        "a rejected write command must not enter the journal: {} entries",
        inv.known_entries()
    );

    let inv = turn_writing_state(&mut engine, 2, false).await?;
    ensure!(
        inv.outcome().is_none(),
        "an EXCLUSIVE turn can write: {:?}",
        inv.outcome()
    );
    ensure!(inv.known_entries() == 2, "SetState in the journal");
    Ok(())
}

/// An unmarked entry 0 (an old binary, an invocation in flight during a binary
/// upgrade) ⇒ the old rule: a `SHARED` turn can still write.
#[tokio::test]
async fn unmarked_entry0_keeps_the_old_rule() -> Result<()> {
    let root = fresh_root("unmarked")?;
    let mut engine = open_engine(&root).await?;
    create(&mut engine, 1).await?;
    strip_mark(&mut engine, &inv_id(1)).await?;
    ensure!(!marked(&entry0_headers(&engine, &inv_id(1)).await?));

    let inv = turn_writing_state(&mut engine, 1, true).await?;
    ensure!(
        inv.outcome().is_none(),
        "an invocation created before D-W5 must be able to write: {:?}",
        inv.outcome()
    );
    ensure!(inv.known_entries() == 2, "SetState in the journal");
    Ok(())
}

/// `restart` of an unmarked invocation ⇒ the successor **has** the mark: the
/// successor is a new invocation, created under the new rule. The kept prefix
/// (with the old `SetState`) can be replayed — the machine only checks new
/// commands.
#[tokio::test]
async fn restart_marks_the_successor() -> Result<()> {
    let root = fresh_root("restart")?;
    let mut engine = open_engine(&root).await?;
    create(&mut engine, 1).await?;
    strip_mark(&mut engine, &inv_id(1)).await?;
    // The old copy wrote state under the old rule: journal = [Input, SetState].
    let inv = turn_writing_state(&mut engine, 1, true).await?;
    ensure!(inv.outcome().is_none());

    let new = inv_id(2);
    // The new copy enters the queue the way the real shell does
    // (`turn::retry::restart`): `Running` without a place in a queue is a
    // tripwire of `lifecycle-check`.
    let q = cairn_store::QueueName {
        tenant: Default::default(),
        scope: None,
        limit: None,
        exclusive: false,
        service: "Cart".into(),
        object_key: String::new(),
    };
    let seq = engine.next_queue_seq(&q).await?;
    let out = engine
        .restart(&inv_id(1), &new, 2, None, None, Some((&q, seq)))
        .await?;
    ensure!(
        matches!(out, RestartOutcome::Restarted { .. }),
        "restart: {out:?}"
    );
    ensure!(
        marked(&entry0_headers(&engine, &new).await?),
        "the successor must carry the mark"
    );
    ensure!(
        !marked(&entry0_headers(&engine, &inv_id(1)).await?),
        "the old copy keeps its entry 0"
    );

    // The successor, a SHARED turn: the prefix replays, a new write command is rejected.
    let mut inv = engine.load(new, StartParams::default()).await?;
    inv.mark_shared_object_handler();
    engine.step(&mut inv, Event::Start).await?;
    ensure!(
        inv.outcome().is_none(),
        "replaying the prefix: {:?}",
        inv.outcome()
    );
    engine.step(&mut inv, set_state("items", "\"y\"")).await?;
    ensure!(
        refused(&inv),
        "the successor's new write command: {:?}",
        inv.outcome()
    );
    Ok(())
}
