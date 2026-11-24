// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Segmented state values in the engine.
//!
//! The tests build the `SegmentedState`/`ValueRefs` rows with `Mutations` (the
//! way the sealing path writes them) and then check the three invariants of
//! `engine/values.rs`:
//!
//! | Action | Must see |
//! |---|---|
//! | `SetState` on a segmented key | the `SegmentedState` row and the holders `State(k)` removed in the same commit; other `Invocation` holders remain |
//! | lazy `GetState` on a segmented-only key | a `Segmented` error, not "absent" |
//! | `GetStateKeys`, `ClearAllState` | see segmented keys; `ClearAll` removes the rows and holders |
//! | eager state | segmented keys do not go into `state_map`, `partial_state` is set |
//! | `purge` | removes every holder `Invocation(id)`, does not touch other ids |
//! | gate off | reads nothing, removes nothing (the tables are empty by construction) |

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, EngineError, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, Message, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        keys, make_invocation_id,
        value::{self, Holder, Owner, SegRef, SegmentedRow},
        LeadershipGrant, Mutations, PartitionId, PartitionKey, PartitionWriter, StoreError,
        TenantId,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const KEY: &[u8] = b"messages";

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Chat".into(),
        handler: "send".into(),
        object_key: "c1".into(),
    }
}

fn owner() -> (PartitionKey, Owner) {
    value::object_owner(&TenantId::default(), None, "Chat", "c1").expect("keyed object")
}

fn inv_id(n: u8) -> [u8; 24] {
    // Same pk as the object: a VO's invocation lives in its range.
    make_invocation_id(owner().0, [n; 16])
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

fn sdk<M: prost::Message>(ty: MessageType, m: &M) -> Event {
    Event::FromSdk(frame(ty, m))
}

fn set_state(k: &[u8], v: &str) -> Event {
    sdk(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::copy_from_slice(k),
            value: Some(cairn_protocol::value::inline(Bytes::copy_from_slice(
                v.as_bytes(),
            ))),
            name: String::new(),
        },
    )
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root =
        std::env::temp_dir().join(format!("cairnflow-segmented-{name}-{}", std::process::id()));
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn open_engine(root: &PathBuf, values: bool) -> Result<Engine> {
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
    Ok(Engine::open(writer).await?.with_values(values))
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

const SHA: [u8; 32] = [0xab; 32];

fn row() -> SegmentedRow {
    SegmentedRow {
        len: 10 + 4,
        refs: vec![SegRef {
            sha: SHA,
            gen: 1,
            len: 10,
        }],
        tail: Bytes::from_static(b"tail"),
    }
}

/// Built the way the sealing path writes: the `SegmentedState` row of `KEY`, the
/// holder `State(KEY)` and the holder `Invocation(other)` of the same segment.
async fn stage(engine: &mut Engine, other: u8) -> Result<()> {
    let (pk, o) = owner();
    let t = TenantId::default();
    let mut m = Mutations::new();
    m.put_segmented_state(pk, &t, None, "Chat", "c1", KEY, &row())
        .put_value_ref(
            pk,
            &t,
            &o,
            &Holder::State(Bytes::from_static(KEY)),
            &SHA,
            1,
            10,
        )
        .put_value_ref(pk, &t, &o, &Holder::Invocation(inv_id(other)), &SHA, 1, 10);
    engine.writer_mut().commit(m).await?;
    Ok(())
}

async fn segmented_row(engine: &Engine) -> Result<Option<Bytes>> {
    let (pk, _) = owner();
    Ok(engine
        .writer()
        .segmented_state(pk, &TenantId::default(), None, "Chat", "c1", KEY)
        .await?)
}

/// The holders left for the owner.
async fn holders(engine: &Engine) -> Result<Vec<Holder>> {
    let (pk, o) = owner();
    let prefix = keys::value_refs_prefix(pk, &TenantId::default(), &o);
    Ok(engine
        .writer()
        .value_refs_under_unbounded(&prefix)
        .await?
        .into_iter()
        .filter_map(|(k, _)| keys::parse_value_ref(&k).map(|r| r.holder))
        .collect())
}

async fn state(engine: &Engine, k: &[u8]) -> Result<Option<Bytes>> {
    let (pk, _) = owner();
    Ok(engine
        .writer()
        .state(pk, &TenantId::default(), None, "Chat", "c1", k)
        .await?)
}

#[tokio::test]
async fn set_state_drops_segmented_row_and_its_state_holders() -> Result<()> {
    let root = fresh_root("set")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine.step(&mut inv, set_state(KEY, "\"new\"")).await?;

    ensure!(
        segmented_row(&engine).await?.is_none(),
        "segmented row remains"
    );
    ensure!(
        state(&engine, KEY).await?.as_deref() == Some(&b"\"new\""[..]),
        "new value inline"
    );
    let h = holders(&engine).await?;
    ensure!(
        h == vec![Holder::Invocation(inv_id(9))],
        "only the other id's Invocation holder remains: {h:?}"
    );
    Ok(())
}

#[tokio::test]
async fn lazy_get_of_segmented_key_answers_refs_not_bytes() -> Result<()> {
    let root = fresh_root("get")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    let out = engine
        .step(
            &mut inv,
            sdk(
                MessageType::GET_STATE,
                &pb::GetStateCommandMessage {
                    key: Bytes::from_static(KEY),
                    result_completion_id: 1,
                    name: String::new(),
                },
            ),
        )
        .await?;
    let v = out
        .iter()
        .find_map(|m| match m {
            Message::GetStateCompletion(c) => match &c.result {
                Some(pb::get_state_completion_notification_message::Result::Value(v)) => {
                    Some(v.clone())
                }
                _ => None,
            },
            _ => None,
        })
        .expect("GetStateCompletion carries a value, not absent");
    let l = cairn_protocol::value::Logical::of(Some(&v))?;
    ensure!(
        l.len() == 14 && l.refs().len() == 1 && l.carried().as_ref() == b"tail",
        "the engine returns ref + tail, no assembly: {l:?}"
    );
    // The notification in the journal carries a ref ⇒ holder `Invocation(1)` in the same commit.
    ensure!(
        holders(&engine)
            .await?
            .contains(&Holder::Invocation(inv_id(1))),
        "an entry carrying a ref has a holder"
    );
    Ok(())
}

/// A `SetState` frame the shell has sealed: `seg` is a reused old segment (the
/// `State` holder already exists) and `fresh` a segment just PUT.
fn sealed_set(refs: &[(u8, u64)], tail: &'static [u8]) -> cairn_protocol::Frame {
    use cairn_protocol::value::{Logical, SegmentRef};
    let refs: Vec<_> = refs
        .iter()
        .map(|&(b, gen)| SegmentRef {
            sha256: [b; 32],
            len: 10,
            gen,
        })
        .collect();
    let len = 10 * refs.len() as u64 + tail.len() as u64;
    frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::from_static(KEY),
            value: Some(
                Logical::Segmented {
                    refs,
                    tail: Bytes::from_static(tail),
                    len,
                }
                .encode(),
            ),
            name: String::new(),
        },
    )
}

/// **Segmented write, reusing an old ref.** Removing the old holders comes
/// **before** the new write in the batch: a reused ref has the same holder key.
///
/// Must see: the `SegmentedState` row carries two refs; holders `State(k)` for
/// both the old ref (surviving the remove-then-write batch) and the new ref;
/// the entry's holder `Invocation(1)`; the inline `State` row absent.
#[tokio::test]
async fn sealed_set_reuses_held_ref_and_adds_fresh_one() -> Result<()> {
    let root = fresh_root("sealed")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    let out = engine
        .step_sealed(
            &mut inv,
            sealed_set(&[(0xab, 1), (0xcd, 2)], b"t"),
            vec![([0xcd; 32], 2)],
        )
        .await?;
    ensure!(
        inv.outcome().is_none(),
        "a valid sealed frame goes on: {:?} {out:?}",
        inv.outcome()
    );
    let raw = segmented_row(&engine).await?.expect("segmented row");
    let row = SegmentedRow::decode(&raw).expect("row readable");
    ensure!(row.len == 21 && row.refs.len() == 2, "{row:?}");
    let h = holders(&engine).await?;
    let st = Holder::State(Bytes::from_static(KEY));
    ensure!(
        h.iter().filter(|x| **x == st).count() == 2,
        "State holders of the old and the new ref: {h:?}"
    );
    ensure!(
        h.contains(&Holder::Invocation(inv_id(1))),
        "Invocation(1) holder: {h:?}"
    );
    ensure!(
        state(&engine, KEY).await?.is_none(),
        "no inline State row left"
    );
    Ok(())
}

/// **An unknown ref is rejected**: a ref not in the key's current value and not
/// just PUT may be waiting for deletion. Must see: a `ForeignSegment` error,
/// nothing committed (the old row intact).
#[tokio::test]
async fn sealed_set_with_foreign_ref_is_refused() -> Result<()> {
    let root = fresh_root("foreign")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    let r = engine
        .step_sealed(&mut inv, sealed_set(&[(0xee, 7)], b""), vec![])
        .await;
    ensure!(
        matches!(
            r,
            Err(EngineError::Store(StoreError::ForeignSegment { .. }))
        ),
        "an unknown ref must be rejected: {r:?}"
    );
    ensure!(
        segmented_row(&engine).await?.as_deref() == Some(&row().encode()[..]),
        "old row intact"
    );
    Ok(())
}

/// **A reused ref held by the invocation's own journal.** A v6 SDK remembers
/// `refs[0..k]` after a `set` (`handler.ts`) to reuse them in the next `set` of
/// the same turn; the shell may have merged them out of the current row. The
/// ref is still alive: the holder `Invocation(id)` of the journaled entry holds
/// it.
///
/// 1. Current row: ref `ab`. Invocation 1 holds ref `cd` (its old entry), `cd`
///    is no longer in the row.
/// 2. Invocation 1: `SetState` reusing `cd` ⇒ accepted, the row carries `cd`.
/// 3. Invocation 2 (not holding `cd`): the same frame ⇒ `ForeignSegment`.
#[tokio::test]
async fn sealed_set_may_reuse_a_ref_its_own_journal_holds() -> Result<()> {
    let root = fresh_root("own-journal")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    let (pk, o) = owner();
    let mut m = Mutations::new();
    m.put_value_ref(
        pk,
        &TenantId::default(),
        &o,
        &Holder::Invocation(inv_id(1)),
        &[0xcd; 32],
        2,
        10,
    );
    engine.writer_mut().commit(m).await?;
    for n in [2u8, 1] {
        create(&mut engine, n).await?;
    }
    let mut other = engine.load(inv_id(2), StartParams::default()).await?;
    engine.step(&mut other, Event::Start).await?;
    let r = engine
        .step_sealed(&mut other, sealed_set(&[(0xcd, 2)], b"t"), vec![])
        .await;
    ensure!(
        matches!(
            r,
            Err(EngineError::Store(StoreError::ForeignSegment { .. }))
        ),
        "an invocation not holding cd: must be rejected: {r:?}"
    );
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    let out = engine
        .step_sealed(&mut inv, sealed_set(&[(0xcd, 2)], b"t"), vec![])
        .await?;
    ensure!(
        inv.outcome().is_none(),
        "a ref in its own journal: accepted: {:?} {out:?}",
        inv.outcome()
    );
    let raw = segmented_row(&engine).await?.expect("segmented row");
    let row = SegmentedRow::decode(&raw).expect("row readable");
    ensure!(
        row.refs.len() == 1 && row.refs[0].sha == [0xcd; 32],
        "{row:?}"
    );
    Ok(())
}

/// A `FromSdk` frame carrying a segment is still 571 (an SDK ≤ v5 may not send refs itself).
#[tokio::test]
async fn unsealed_frame_with_segments_is_still_a_violation() -> Result<()> {
    let root = fresh_root("unsealed")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .step(&mut inv, Event::FromSdk(sealed_set(&[(0xab, 1)], b"")))
        .await?;
    ensure!(
        matches!(inv.outcome(), Some(cairn_runtime::Outcome::Failed { code, .. })
            if *code == cairn_protocol::PROTOCOL_VIOLATION),
        "{:?}",
        inv.outcome()
    );
    Ok(())
}

#[tokio::test]
async fn eager_state_leaves_segmented_keys_to_lazy_reads() -> Result<()> {
    let root = fresh_root("eager")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    let out = engine.step(&mut inv, Event::Start).await?;
    let start = out
        .iter()
        .find_map(|m| match m {
            Message::Start(s) => Some(s.clone()),
            _ => None,
        })
        .expect("Start down to the SDK");
    ensure!(start.partial_state, "segmented key ⇒ partial_state");
    ensure!(
        !start.state_map.iter().any(|e| e.key.as_ref() == KEY),
        "segmented key not in state_map"
    );
    Ok(())
}

#[tokio::test]
async fn state_keys_and_clear_all_see_segmented_keys() -> Result<()> {
    let root = fresh_root("clear")?;
    let mut engine = open_engine(&root, true).await?;
    stage(&mut engine, 9).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine.step(&mut inv, set_state(b"x", "1")).await?;
    let out = engine
        .step(
            &mut inv,
            sdk(
                MessageType::GET_STATE_KEYS,
                &pb::GetStateKeysCommandMessage {
                    result_completion_id: 1,
                    name: String::new(),
                },
            ),
        )
        .await?;
    let keys: Vec<Bytes> = out
        .iter()
        .find_map(|m| match m {
            Message::GetStateKeysCompletion(c) => c.state_keys.as_ref().map(|k| k.keys.clone()),
            _ => None,
        })
        .expect("GetStateKeysCompletion");
    ensure!(
        keys == vec![Bytes::from_static(KEY), Bytes::from_static(b"x")],
        "keys of both tables, in order: {keys:?}"
    );

    engine
        .step(
            &mut inv,
            sdk(
                MessageType::CLEAR_ALL_STATE,
                &pb::ClearAllStateCommandMessage {
                    name: String::new(),
                },
            ),
        )
        .await?;
    ensure!(
        segmented_row(&engine).await?.is_none(),
        "ClearAll removes the segmented row"
    );
    ensure!(
        state(&engine, b"x").await?.is_none(),
        "ClearAll removes the inline row"
    );
    ensure!(
        holders(&engine).await? == vec![Holder::Invocation(inv_id(9))],
        "ClearAll removes State holders"
    );
    Ok(())
}

#[tokio::test]
async fn purge_releases_only_its_own_invocation_holders() -> Result<()> {
    let root = fresh_root("purge")?;
    let mut engine = open_engine(&root, true).await?;
    create(&mut engine, 1).await?;
    stage(&mut engine, 1).await?;
    // The same segment still has holder `Invocation(2)` — another id, it must remain.
    let (pk, o) = owner();
    let mut m = Mutations::new();
    m.put_value_ref(
        pk,
        &TenantId::default(),
        &o,
        &Holder::Invocation(inv_id(2)),
        &SHA,
        1,
        10,
    );
    engine.writer_mut().commit(m).await?;

    engine.purge(&inv_id(1), None).await?;
    let mut h = holders(&engine).await?;
    h.sort();
    let mut want = vec![
        Holder::State(Bytes::from_static(KEY)),
        Holder::Invocation(inv_id(2)),
    ];
    want.sort();
    ensure!(h == want, "purge removes only its own id's holders: {h:?}");
    Ok(())
}

#[tokio::test]
async fn gate_off_reads_and_drops_nothing() -> Result<()> {
    let root = fresh_root("off")?;
    let mut engine = open_engine(&root, false).await?;
    stage(&mut engine, 1).await?;
    create(&mut engine, 1).await?;
    let mut inv = engine.load(inv_id(1), StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine.step(&mut inv, set_state(KEY, "\"new\"")).await?;
    engine.purge(&inv_id(1), None).await?;
    ensure!(
        segmented_row(&engine).await?.is_some(),
        "gate off: tables not read"
    );
    ensure!(
        holders(&engine).await?.len() == 2,
        "gate off: holders not removed"
    );
    Ok(())
}

/// The owner of an unkeyed invocation differs from the object's owner.
#[test]
fn unkeyed_owner_is_the_id() {
    let id = inv_id(5);
    let (pk, o) = value::invocation_owner(&id);
    assert_eq!(pk, owner().0);
    assert_ne!(o, owner().1);
}
