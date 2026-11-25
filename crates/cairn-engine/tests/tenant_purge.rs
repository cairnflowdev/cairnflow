// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Logical deletion of one tenant's rows in a range Db (tenant purge):
//! `Engine::purge_tenant_rows` scans the whole range in batches with a cursor
//! and deletes **exactly** the rows of one tenant.
//!
//! Runs on the store model (`RangeModel`) with a manual clock, like
//! `tenant_quota.rs`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    bytes::{Bytes, BytesMut},
    cairn_engine::{CreateOptions, Engine, Target, TenantPurgeCursor},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        clock::ManualClock,
        keys::{self, Region, RowOwner},
        make_invocation_id,
        proof::TenantPurgeGrant,
        testkit::RangeBucket,
        InvocationId, LeadershipGrant, PartitionId, PartitionKey, PartitionWriter, TenantId,
    },
    std::{collections::BTreeSet, sync::Arc},
};

const T0: u64 = 1_700_000_000_000;

type Eng = Engine<cairn_store::testkit::RangeModel>;

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    FrameDecoder::new()
        .decode(&mut BytesMut::from(&wire[..]))
        .unwrap()
        .unwrap()
}

fn sleep(id: u32) -> Frame {
    frame(
        MessageType::SLEEP,
        &pb::SleepCommandMessage {
            wake_up_time: T0 + 3_600_000,
            result_completion_id: id,
            name: String::new(),
        },
    )
}

fn set_state(key: &str) -> Frame {
    frame(
        MessageType::SET_STATE,
        &pb::SetStateCommandMessage {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Some(cairn_protocol::value::inline(Bytes::from_static(b"1"))),
            name: String::new(),
        },
    )
}

fn output() -> Frame {
    frame(
        MessageType::OUTPUT,
        &pb::OutputCommandMessage {
            result: Some(pb::output_command_message::Result::Value(
                cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
            )),
            name: String::new(),
        },
    )
}

async fn engine() -> Eng {
    let bucket = RangeBucket::new();
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), bucket.open())
        .await
        .unwrap();
    Engine::open(w)
        .await
        .unwrap()
        .with_clock(Arc::new(ManualClock::new(T0)))
        .with_streams(true)
}

/// An invocation of `tenant` (object `key`, or unkeyed) running `frames`. The pk
/// follows the product's rule: object ⇒ hash of the target; unkeyed ⇒ random.
async fn invoke(e: &mut Eng, tenant: &str, key: Option<&str>, frames: Vec<Frame>) -> InvocationId {
    let tenant = TenantId::new(tenant).unwrap();
    let pk = match key {
        Some(k) => cairn_store::partition_key_for_target(&tenant, None, "Svc", k, None).unwrap(),
        None => {
            let mut b = [0u8; 8];
            cairn_store::entropy::fill(&mut b);
            PartitionKey(u64::from_be_bytes(b))
        }
    };
    let target = Target {
        tenant,
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: key.unwrap_or_default().into(),
    };
    let mut seed = [0u8; 16];
    cairn_store::entropy::fill(&mut seed);
    let id = make_invocation_id(pk, seed);
    let mut inv = e
        .create(
            id,
            &target,
            CreateOptions::default(),
            StartParams::default(),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
    e.step(&mut inv, Event::Start).await.unwrap();
    for f in frames {
        if inv.outcome().is_some() {
            break;
        }
        e.step(&mut inv, Event::FromSdk(f)).await.unwrap();
    }
    id
}

/// Every key of the Db except the range's own keys (marks, watermarks — they change periodically).
async fn keys_of(e: &Eng) -> BTreeSet<Vec<u8>> {
    let mut out = BTreeSet::new();
    for r in Region::ALL {
        let (lo, hi) = r.bounds();
        for k in e
            .writer()
            .keys_between(lo, hi, r == Region::Bulk, usize::MAX)
            .await
            .unwrap()
        {
            if keys::row_owner(&k) != RowOwner::Range {
                out.insert(k.to_vec());
            }
        }
    }
    out
}

/// Runs rounds until a full round deletes nothing. Returns `(total rows
/// deleted, number of batches, unresolvable in the last round)`.
async fn purge_all(e: &mut Eng, tenant: &str, budget: usize) -> (u64, u32, u64) {
    let grant = TenantPurgeGrant::assume(TenantId::new(tenant).unwrap());
    let (mut total, mut lots) = (0, 0);
    loop {
        let (mut round, mut unattributed) = (0, 0);
        let mut cursor: Option<TenantPurgeCursor> = None;
        loop {
            let step = e.purge_tenant_rows(&grant, cursor, budget).await.unwrap();
            lots += 1;
            round += step.deleted;
            unattributed += step.unattributed;
            match step.next {
                Some(c) => cursor = Some(c),
                None => break,
            }
            assert!(lots < 10_000, "does not stop");
        }
        total += round;
        if round == 0 {
            return (total, lots, unattributed);
        }
    }
}

/// **Deletes exactly one tenant**: `other` has an object with state and a
/// pending timer, and one finished invocation; `acme` has the same plus more.
/// Purge `acme` with batches of 3 rows (many batches, the cursor cutting in
/// the middle of an `owner_pk` group).
///
/// Must see: after the purge, the key set (minus the range's keys) is
/// **exactly** the set before `acme` wrote anything; no unresolvable rows; a
/// second round deletes nothing; `other`'s invocations are still readable;
/// purging `acme` again does not touch
/// `other`.
#[tokio::test]
async fn purge_removes_every_row_of_one_tenant_and_nothing_else() {
    let mut e = engine().await;
    let o1 = invoke(&mut e, "other", Some("k"), vec![set_state("a"), sleep(1)]).await;
    let o2 = invoke(&mut e, "other", None, vec![output()]).await;
    let before = keys_of(&e).await;
    assert!(!before.is_empty());

    invoke(
        &mut e,
        "acme",
        Some("k"),
        vec![set_state("a"), set_state("b"), sleep(1)],
    )
    .await;
    invoke(&mut e, "acme", Some("k2"), vec![set_state("x"), output()]).await;
    invoke(&mut e, "acme", None, vec![sleep(1)]).await;
    invoke(&mut e, "acme", None, vec![output()]).await;
    let with_acme = keys_of(&e).await;
    assert!(with_acme.is_superset(&before) && with_acme.len() > before.len() + 10);

    let (deleted, lots, unattributed) = purge_all(&mut e, "acme", 3).await;
    assert_eq!(unattributed, 0, "unresolvable rows");
    assert!(lots > 3, "many batches: {lots}");
    let after = keys_of(&e).await;
    let left: Vec<_> = after.difference(&before).collect();
    let lost: Vec<_> = before.difference(&after).collect();
    assert!(left.is_empty(), "acme rows left: {left:02x?}");
    assert!(lost.is_empty(), "other rows lost: {lost:02x?}");
    assert_eq!(
        deleted as usize,
        with_acme.len() - before.len(),
        "deleted count matches acme's key count"
    );
    for id in [o1, o2] {
        assert!(
            e.status_at(&id).await.unwrap().is_some(),
            "other lost its status"
        );
    }
    let (again, _, _) = purge_all(&mut e, "acme", 1000).await;
    assert_eq!(again, 0);
    assert_eq!(keys_of(&e).await, before);
}

/// A row whose owner the deleter cannot resolve — an inbound message of an
/// unknown id whose `owner_pk` group has no entity left — is **kept** and
/// counted, not deleted.
#[tokio::test]
async fn unattributed_rows_are_kept_and_counted() {
    let mut e = engine().await;
    let ghost = make_invocation_id(PartitionKey(42), [7; 16]);
    let mut m = cairn_store::Mutations::new();
    m.raw_put(
        keys::inbound(PartitionKey(42), keys::InboundKind::Completion, &ghost, 1),
        &keys::encode_seq(1, b"x"),
    );
    e.writer_mut().commit(m).await.unwrap();
    let before = keys_of(&e).await;
    let (deleted, _, unattributed) = purge_all(&mut e, "acme", 10).await;
    assert_eq!((deleted, unattributed), (0, 1));
    assert_eq!(keys_of(&e).await, before);
}

/// An artifact row `(id, [n; 32], size)` through the shell's path:
/// `stage_artifact_put` then commit.
async fn put_artifact(e: &mut Eng, id: &InvocationId, n: u8, size: u64) {
    let mut muts = cairn_store::Mutations::new();
    e.stage_artifact_put(
        &mut muts,
        id,
        &[n; 32],
        &keys::ArtifactRow {
            epoch: 1,
            size,
            content_type: "x".into(),
        },
    )
    .await
    .unwrap();
    e.writer_mut().commit(muts).await.unwrap();
}

/// **A tenant purge subtracts its artifact bytes**: artifact rows go in pass 1,
/// metadata in pass 2 — the counter delta is computed from the metadata still
/// present.
///
/// 1. `acme` and `other` each have two invocations carrying artifacts; the
///    counter is built.
/// 2. Purge `acme` with batches of 2 rows (many batches).
///
/// Must see: the counter only has `other` left, with exactly its old total.
#[tokio::test]
async fn tenant_purge_drops_artifact_bytes() {
    let mut e = engine().await;
    for (tenant, size) in [("acme", 100), ("other", 7), ("acme", 50), ("other", 3)] {
        let id = invoke(&mut e, tenant, None, vec![output()]).await;
        put_artifact(&mut e, &id, 1, size).await;
        put_artifact(&mut e, &id, 2, size).await;
    }
    while e.artifact_bytes_step(4).await.unwrap().is_some() {}
    assert_eq!(
        e.artifact_bytes().snapshot(),
        Some(vec![("acme".into(), 300), ("other".into(), 20)])
    );
    let (_, lots, unattributed) = purge_all(&mut e, "acme", 2).await;
    assert!(lots > 2 && unattributed == 0, "{lots} {unattributed}");
    assert_eq!(
        e.artifact_bytes().snapshot(),
        Some(vec![("other".into(), 20)])
    );
}

/// **A check batch** (the tombstone's final check): `find_tenant_rows` uses the
/// same scan and classifier as the delete batch but deletes nothing and stops
/// at the tenant's first row.
///
/// Must see: `acme` rows remain ⇒ `found == 1`, the key set unchanged; after
/// the purge, one full check round (cap of 8 keys per batch, many batches) ⇒
/// `found == 0` in every batch — `other`'s rows are never seen as `acme`'s.
#[tokio::test]
async fn find_tenant_rows_sees_rows_and_deletes_nothing() {
    let mut e = engine().await;
    invoke(&mut e, "other", Some("k"), vec![set_state("a"), output()]).await;
    invoke(&mut e, "acme", Some("k"), vec![set_state("a"), output()]).await;
    let grant = TenantPurgeGrant::assume(TenantId::new("acme").unwrap());
    let before = keys_of(&e).await;
    let step = e.find_tenant_rows(&grant, None, 3).await.unwrap();
    assert_eq!(step.found, 1, "must see acme's rows: {step:?}");
    assert_eq!(step.deleted, 0);
    assert!(step.next.is_some(), "stops at the first row");
    assert_eq!(keys_of(&e).await, before, "check batch deleted rows");
    purge_all(&mut e, "acme", 3).await;
    let mut cursor: Option<TenantPurgeCursor> = None;
    let mut lots = 0;
    loop {
        let step = e.find_tenant_rows(&grant, cursor, 1).await.unwrap();
        lots += 1;
        assert_eq!(step.found, 0, "acme rows left after purge: {step:?}");
        match step.next {
            Some(c) => cursor = Some(c),
            None => break,
        }
        assert!(lots < 10_000, "does not stop");
    }
    assert!(lots > 1, "check round needs many batches (8 keys each)");
}
