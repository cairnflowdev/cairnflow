// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Storage-layer tests. Each test pins an invariant of the design, not a
//! function — if an invariant breaks, exactly one test must go red and its name
//! must say which.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::Result,
    cairn_store::{
        partition_key_of, Durability, InvocationId, LeadershipGrant, Mutations, PartitionId,
        PartitionKey, PartitionReader, PartitionWriter, QueueName,
    },
    slatedb::{
        config::{DbReaderOptions, Settings},
        object_store::{local::LocalFileSystem, ObjectStore},
        Db, DbReader, DbReaderMode,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const DB: &str = "partition-0";
const INV_A: InvocationId = [0xAA; 24];
const INV_B: InvocationId = [0xBB; 24];

/// `owner_pk` of the state of object `(service, key)`, no scope.
fn obj(service: &str, key: &str) -> PartitionKey {
    cairn_store::keys::object_owner(&cairn_store::TenantId::default(), None, service, key)
        .expect("has an object key")
}

/// The EXCLUSIVE queue of `OrderService/order-42`, no scope.
fn queue() -> Vec<u8> {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: None,
        limit: None,
        exclusive: true,
        service: "OrderService".into(),
        object_key: "order-42".into(),
    }
    .encode()
}

/// A queue whose object key exceeds the cap — to check that the read path is blocked too.
fn long_queue(object_key: &str) -> Vec<u8> {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: None,
        limit: None,
        exclusive: true,
        service: "svc".into(),
        object_key: object_key.into(),
    }
    .encode()
}

fn fresh_root(name: &str) -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

fn store(root: &PathBuf) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?))
}

/// `commit` **and** force durability — the shape every outward path of the
/// server uses.
///
/// Since `commit` stopped waiting for durability itself, a test that writes
/// `commit(..)` and then reads with a new `DbReader` reads short: the reader
/// sees only what has reached the store. That is not the test's fault but a
/// contract that moved — so the move is named, here, once. A test that
/// deliberately inspects the window *between* the two steps calls both steps by
/// hand.
async fn commit_durable(w: &mut PartitionWriter, m: Mutations) -> Result<()> {
    w.commit(m).await?;
    w.wait_durable().await?;
    Ok(())
}

async fn open_writer(root: &PathBuf) -> Result<PartitionWriter> {
    let settings = Settings {
        flush_interval: Some(Duration::from_millis(20)),
        ..Default::default()
    };
    let db = Db::builder(DB, store(root)?)
        .with_settings(settings)
        .build()
        .await?;
    Ok(PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?)
}

/// Wait until the reader reflects a change.
///
/// **Not to hide a bug — these are the system's real semantics.**
/// `PartitionReader` is a separate `DbReader`, and it was measured: its
/// freshness is bounded below by `manifest_poll_interval`, because SlateDB has
/// no API to force a refresh at once. A reader opened *after* the commit sees it
/// at once (WAL replay on open); an already open reader has to wait for the poll
/// round. A test asserting it immediately makes a wrong assumption.
async fn eventually<F, Fut>(what: &str, mut f: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if f().await? {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("over 5s and the reader still does not see: {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn open_reader(root: &PathBuf) -> Result<PartitionReader> {
    let opts = DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(50),
        checkpoint_lifetime: Duration::from_secs(10),
        ..Default::default()
    };
    let db = DbReader::builder(DB, store(root)?)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(opts)
        .build()
        .await?;
    Ok(PartitionReader::new(db))
}

/// INV-5 — nobody may see a result before the data is durable.
///
/// **The fence moved, the promise did not.** `commit` used to wait for
/// durability itself; now it only pushes into the WAL, and the wait is
/// `durability()` → `Pending::durable()` — changed so that many turns share one
/// flush (see `tests/group_commit.rs`, 44.6×). What did *not* change is what
/// this test checks: after the forced-durability step the data must be on the
/// object store, no sleep, no poll.
///
/// How: open a **brand-new** `DbReader`. A reader can read only what is on the
/// store; it does not see the writer's memtable.
#[tokio::test]
async fn data_durable_after_forced_flush() -> Result<()> {
    let root = fresh_root("inv5")?;
    let mut w = open_writer(&root).await?;

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"RUNNING")
        .append_journal(&INV_A, 0, b"input")
        .push_outbox(
            partition_key_of(&INV_A),
            1,
            &INV_A,
            1,
            b"call OrderService/submit",
        );
    // Both steps by hand, on purpose: this is a test *about* those two steps, so
    // it must not go through `commit_durable` — which hides them from every other
    // test.
    w.commit(m).await?;
    let _committed = match w.durability()? {
        // A ticket is issued only after `await_durable` returns — INV-5 at the type level.
        Durability::Wait(p) => p.durable().await?,
        Durability::Ready(_) => panic!("ticket right after commit — nobody waited for durability"),
    };

    let r = open_reader(&root).await?;
    assert_eq!(
        r.invocation_status(&INV_A).await?.as_deref(),
        Some(b"RUNNING".as_ref()),
        "durability was forced but the data is not on the store — INV-5 broken"
    );
    assert_eq!(r.pending_outbox(10).await?.len(), 1);

    // The mark has passed ⇒ a ticket can be granted in place. This path is what
    // makes calling `sync` at every boundary almost free.
    assert!(
        matches!(w.durability()?, Durability::Ready(_)),
        "nothing in flight, yet no ticket granted"
    );

    r.close().await?;
    w.seal().await?;
    Ok(())
}

/// INV-4 — when a second writer comes up, the first loses its right to write.
///
/// This is SlateDB's `writer_epoch` layer, the only layer that stops an old
/// leader in a GC pause. It was measured at the process level; here we pin that
/// our storage layer *translates* that error correctly into `is_fenced()`,
/// because the runtime branches on it to return `NotLeader` instead of
/// retrying.
#[tokio::test]
async fn old_writer_fenced_when_new_writer_starts() -> Result<()> {
    let root = fresh_root("inv4")?;
    let mut w1 = open_writer(&root).await?;

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"RUNNING");
    commit_durable(&mut w1, m).await?;

    // The new leader comes up.
    let mut w2 = open_writer(&root).await?;

    // **The fence fires at the forced-durability step, not at the write** —
    // exactly what was measured, and now it reads straight out of the test: the
    // zombie's `commit` is accepted, and `durable()` is where it learns it is
    // dead. Since every outward effect requires the ticket only `durable()`
    // grants, the zombie gets nothing done.
    let mut m = Mutations::new();
    m.set_invocation_status(&INV_B, b"ZOMBIE");
    let err = commit_durable(&mut w1, m)
        .await
        .expect_err("the old writer still writes after being fenced — INV-4 broken");
    let err: cairn_store::StoreError = err.downcast().expect("must be a StoreError");
    assert!(
        err.is_fenced(),
        "the error must be recognisable as a lost write right, got: {err}"
    );

    // And the zombie's write does not get into the data.
    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"OWNED-BY-W2");
    commit_durable(&mut w2, m).await?;

    let r = open_reader(&root).await?;
    assert_eq!(
        r.invocation_status(&INV_A).await?.as_deref(),
        Some(b"OWNED-BY-W2".as_ref())
    );
    assert!(
        r.invocation_status(&INV_B).await?.is_none(),
        "the zombie's write got in"
    );

    r.close().await?;
    w2.seal().await?;
    Ok(())
}

/// A revoked lease makes commit fail **before** spending I/O (early refusal).
#[tokio::test]
async fn lost_lease_blocks_before_write() -> Result<()> {
    let root = fresh_root("lease")?;
    let grant = LeadershipGrant::single_node(PartitionId(0));
    let validity = grant.validity_handle();

    let settings = Settings {
        flush_interval: Some(Duration::from_millis(20)),
        ..Default::default()
    };
    let db = Db::builder(DB, store(&root)?)
        .with_settings(settings)
        .build()
        .await?;
    let mut w = PartitionWriter::open(grant, db).await?;

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"RUNNING");
    commit_durable(&mut w, m).await?;

    validity.revoke();
    assert!(!validity.is_valid());

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_B, b"AFTER-LEASE-LOST");
    let err = w.commit(m).await.expect_err("lease lost, yet still writes");
    assert!(err.is_fenced());
    Ok(())
}

/// The journal must read back **in exact command-index order** — the contract
/// of the REPLAY phase of the protocol. The test deliberately interleaves two
/// invocations and writes indexes out of increasing order, because both happen
/// for real.
#[tokio::test]
async fn journal_reads_back_in_replay_order() -> Result<()> {
    let root = fresh_root("journal")?;
    let mut w = open_writer(&root).await?;

    let mut m = Mutations::new();
    for i in [3u32, 0, 2, 1] {
        m.append_journal(&INV_A, i, format!("A-entry-{i}").as_bytes());
        m.append_journal(&INV_B, i, format!("B-entry-{i}").as_bytes());
    }
    commit_durable(&mut w, m).await?;

    let r = open_reader(&root).await?;
    let journal = r.journal(&INV_A).await?;
    let indices: Vec<u32> = journal.iter().map(|(i, _)| *i).collect();
    assert_eq!(
        indices,
        vec![0, 1, 2, 3],
        "the journal is not in replay order"
    );
    assert_eq!(journal[2].1.as_ref(), b"A-entry-2");
    assert_eq!(
        r.journal(&INV_B).await?.len(),
        4,
        "the journals of two invocations got mixed"
    );

    r.close().await?;
    w.seal().await?;
    Ok(())
}

/// Timers must be picked in due order, and a timer not yet due must **not** be picked.
#[tokio::test]
async fn timers_popped_in_due_order() -> Result<()> {
    let root = fresh_root("timers")?;
    let mut w = open_writer(&root).await?;

    let mut m = Mutations::new();
    for (due, cid) in [(5_000u64, 1u32), (1_000, 2), (3_000, 3), (9_000, 4)] {
        m.add_timer(due, &INV_A, cid, format!("wake-{cid}").as_bytes());
    }
    commit_durable(&mut w, m).await?;

    let r = open_reader(&root).await?;
    let due = r.due_timers(5_000).await?;
    let times: Vec<u64> = due.iter().map(|(t, _, _)| *t).collect();
    assert_eq!(
        times,
        vec![1_000, 3_000, 5_000],
        "a timer not yet due was picked"
    );

    // After processing, the timer must disappear from the table — otherwise it fires forever.
    let mut m = Mutations::new();
    m.remove_timer(1_000, &INV_A, 2);
    commit_durable(&mut w, m).await?;
    eventually("a processed timer leaves the table", || async {
        Ok(r.due_timers(5_000).await?.len() == 2)
    })
    .await?;

    r.close().await?;
    w.seal().await?;
    Ok(())
}

/// INV-3 — pending work survives the loss of the writer.
///
/// A failover at the storage level: the writer dies (no seal, like a crash), a
/// new leader opens the store and must pick up exactly the pending work —
/// because it lives in the state tables, not in the dead leader's RAM.
#[tokio::test]
async fn pending_work_survives_failover() -> Result<()> {
    let root = fresh_root("inv3")?;
    let mut w1 = open_writer(&root).await?;

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"RUNNING")
        .append_journal(&INV_A, 0, b"input")
        // The outward call and the timer must both be in the SAME commit as the
        // journal entry that produced them.
        .push_outbox(
            partition_key_of(&INV_A),
            1,
            &INV_A,
            1,
            b"call PaymentService/charge",
        )
        .push_outbox(partition_key_of(&INV_A), 2, &INV_A, 2, b"reply to caller")
        .add_timer(1_000, &INV_A, 7, b"timeout")
        .push_vqueue(&queue(), cairn_store::Stage::Inbox, 1, &INV_B);
    commit_durable(&mut w1, m).await?;

    // The old leader vanishes without sealing — exactly like a crash.
    drop(w1);

    let _w2 = open_writer(&root).await?;
    let r = open_reader(&root).await?;

    let outbox = r.pending_outbox(10).await?;
    assert_eq!(
        outbox.iter().map(|((_, c), _)| *c).collect::<Vec<_>>(),
        vec![1, 2],
        "the new leader does not pick up the outbox — INV-3 broken, outward call lost"
    );
    assert_eq!(r.due_timers(1_000).await?.len(), 1, "pending timer lost");
    assert_eq!(
        r.journal(&INV_A).await?.len(),
        1,
        "journal lost after failover"
    );

    r.close().await?;
    Ok(())
}

/// Virtual-object state: write, read, delete — and no mixing between two objects.
#[tokio::test]
async fn state_does_not_leak_between_objects() -> Result<()> {
    let root = fresh_root("state")?;
    let mut w = open_writer(&root).await?;

    let mut m = Mutations::new();
    m.set_state(
        obj("OrderService", "order-1"),
        &cairn_store::TenantId::default(),
        None,
        "OrderService",
        "order-1",
        b"total",
        b"100",
    )
    .set_state(
        obj("OrderService", "order-2"),
        &cairn_store::TenantId::default(),
        None,
        "OrderService",
        "order-2",
        b"total",
        b"200",
    )
    .set_state(
        obj("CartService", "order-1"),
        &cairn_store::TenantId::default(),
        None,
        "CartService",
        "order-1",
        b"total",
        b"300",
    );
    commit_durable(&mut w, m).await?;

    let r = open_reader(&root).await?;
    assert_eq!(
        r.state(
            obj("OrderService", "order-1"),
            &cairn_store::TenantId::default(),
            None,
            "OrderService",
            "order-1",
            b"total"
        )
        .await?
        .as_deref(),
        Some(b"100".as_ref())
    );
    assert_eq!(
        r.state(
            obj("OrderService", "order-2"),
            &cairn_store::TenantId::default(),
            None,
            "OrderService",
            "order-2",
            b"total"
        )
        .await?
        .as_deref(),
        Some(b"200".as_ref())
    );
    assert_eq!(
        r.state(
            obj("CartService", "order-1"),
            &cairn_store::TenantId::default(),
            None,
            "CartService",
            "order-1",
            b"total"
        )
        .await?
        .as_deref(),
        Some(b"300".as_ref()),
        "the state of two different services got mixed"
    );

    let mut m = Mutations::new();
    m.clear_state(
        obj("OrderService", "order-1"),
        &cairn_store::TenantId::default(),
        None,
        "OrderService",
        "order-1",
        b"total",
    );
    commit_durable(&mut w, m).await?;
    eventually("deleted state disappears", || async {
        Ok(r.state(
            obj("OrderService", "order-1"),
            &cairn_store::TenantId::default(),
            None,
            "OrderService",
            "order-1",
            b"total",
        )
        .await?
        .is_none())
    })
    .await?;
    assert_eq!(
        r.state(
            obj("OrderService", "order-2"),
            &cairn_store::TenantId::default(),
            None,
            "OrderService",
            "order-2",
            b"total"
        )
        .await?
        .as_deref(),
        Some(b"200".as_ref()),
        "deleting one object lost another object's state"
    );

    r.close().await?;
    w.seal().await?;
    Ok(())
}

/// A reader must be able to report its freshness. Without this number a
/// "stuck follower" cannot be monitored — a silent failure found by measurement.
#[tokio::test]
async fn reader_reports_staleness() -> Result<()> {
    let root = fresh_root("freshness")?;
    let mut w = open_writer(&root).await?;

    let mut m = Mutations::new();
    m.set_invocation_status(&INV_A, b"RUNNING");
    commit_durable(&mut w, m).await?;

    let r = open_reader(&root).await?;
    let f = r.freshness();
    assert!(
        f.durable_seq > 0,
        "the reader does not know how far it replayed ⇒ a stall cannot be monitored"
    );
    assert!(
        f.manifest_seq <= f.durable_seq,
        "D'ᵢ ({}) exceeds T ({}) — absurd, the manifest cannot cover past what was replayed",
        f.manifest_seq,
        f.durable_seq
    );

    r.close().await?;
    w.seal().await?;
    Ok(())
}

/// A key over the cap is **refused**, never reaching SlateDB.
///
/// Two hazards, one fence:
///
/// 1. SlateDB `assert!(key.len() <= u16::MAX)` in `WriteBatch` — exceeding it
///    is a *panic*, and a panic on the write path kills the engine thread: a
///    whole partition stalls because of one long object key.
/// 2. `put_var`'s length prefix is a `u16`, so a segment longer than 65535
///    wraps around and two different segment sets encode to the same key — see
///    `aliasing_key_pairs_exist`.
///
/// A segment's bytes live *inside* the key, so (2) implies (1): checking the
/// length of the built key is enough for both.
///
/// Object keys are set from outside (query param `?key=`, `ctx.call`), so this
/// is not a theoretical risk.
#[tokio::test]
async fn overlong_key_rejected_on_both_paths() -> Result<()> {
    let root = fresh_root("key-too-long")?;
    let mut w = open_writer(&root).await?;

    let too_long = "k".repeat(65_536); // exactly where the u16 prefix wraps to 0

    // ── Write path: the error surfaces at commit, not as a panic, and nothing is written.
    let mut m = Mutations::new();
    m.set_state(
        obj("svc", &too_long),
        &cairn_store::TenantId::default(),
        None,
        "svc",
        &too_long,
        b"x",
        b"1",
    );
    assert!(m.is_empty(), "failed mutation still counted in the batch");
    let err = w.commit(m).await.unwrap_err();
    assert!(
        matches!(err, cairn_store::StoreError::KeyTooLong { .. }),
        "a too-long key slipped down to SlateDB: {err}"
    );

    // ── One failed mutation fails **the whole batch**: no committing half of
    //    it. A valid journal entry that goes with it is not written either.
    let mut m = Mutations::new();
    m.append_journal(&INV_A, 0, b"hop-le");
    m.set_state(
        obj("svc", &too_long),
        &cairn_store::TenantId::default(),
        None,
        "svc",
        &too_long,
        b"x",
        b"1",
    );
    assert!(w.commit(m).await.is_err());
    assert!(
        w.journal_unbounded(&INV_A).await?.is_empty(),
        "a batch with a failed mutation was partly written — commit is no longer atomic"
    );

    // ── Read path: must be blocked too. A wrapped key points into *another*
    //    object's cell, so reading with it would read their state.
    assert!(w
        .state(
            obj("svc", &too_long),
            &cairn_store::TenantId::default(),
            None,
            "svc",
            &too_long,
            b"x"
        )
        .await
        .is_err());
    assert!(w
        .vqueue_entries_unbounded(&long_queue(&too_long))
        .await
        .is_err());
    assert!(w
        .idempotency_target(
            &cairn_store::TenantId::default(),
            None,
            "svc",
            "h",
            &too_long,
            "i"
        )
        .await
        .is_err());

    // ── And a key just under the cap still works: the fence must not
    //    overreach. 10 key-head bytes (region ‖ owner ‖ table) + 2 tenant prefix
    //    (empty) + 2 scope prefix (empty) + 2 prefix + "svc" + 2 prefix + key +
    //    user_key = 65535.
    let at_limit = "k".repeat(65_535 - 10 - 2 - 2 - 2 - 3 - 2 - 1);
    let mut m = Mutations::new();
    m.set_state(
        obj("svc", &at_limit),
        &cairn_store::TenantId::default(),
        None,
        "svc",
        &at_limit,
        b"x",
        b"1",
    );
    commit_durable(&mut w, m).await?;
    assert_eq!(
        w.state(
            obj("svc", &at_limit),
            &cairn_store::TenantId::default(),
            None,
            "svc",
            &at_limit,
            b"x"
        )
        .await?
        .as_deref(),
        Some(&b"1"[..])
    );

    w.seal().await?;
    Ok(())
}

/// A concrete pair: both encode to `00 00 | 00 00 | k*65534 | 00 00`.
///
/// `A` is 65536 long, so its prefix wraps to `0000`, and since `A` **starts**
/// with two `00` bytes those two bytes read back exactly as `B`'s prefix. The
/// cut between the two segments shifts by two bytes while the byte stream stays
/// the same — exactly the ambiguity length prefixes exist to prevent.
#[test]
fn aliasing_key_pairs_exist() {
    let a = format!("\0\0{}", "k".repeat(65_534)); // 65536 bytes, starting 00 00
    let b = format!("{}\0\0", "k".repeat(65_534)); // 65536 bytes, ending 00 00

    // Not calling `keyspace::state` (it has a debug_assert); rebuild exactly the
    // encoding, to show the overlap is a property of the *format*, not of a bug.
    let encode = |svc: &str, obj: &str| {
        let mut out = vec![0x03u8];
        for seg in [svc, obj] {
            out.extend_from_slice(&(seg.len() as u16).to_be_bytes());
            out.extend_from_slice(seg.as_bytes());
        }
        out
    };
    assert_ne!(a, b, "two object keys must be two different things");
    assert_eq!(
        encode(&a, ""),
        encode("", &b),
        "if these two DIFFER, the u16 cap is no longer a necessary fence"
    );
}

/// **WAL and SST really live in two different stores** (D7).
///
/// This cannot be checked by reading the code: `with_wal_object_store` is one
/// call, and a call in the wrong place still compiles, still runs, still returns
/// correct data — only the whole WAL goes to the wrong store. A two-tier
/// configuration means something only if it really separates; so the check must
/// look at the real objects.
#[tokio::test]
async fn wal_and_sst_live_in_different_stores() -> Result<()> {
    use {
        cairn_store::{ObjectStores, StoreLocation},
        slatedb::object_store::{memory::InMemory, ObjectStore as _},
    };

    let main: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wal: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::split(
        StoreLocation::from_store(Arc::clone(&main), "prod", "memory://main/prod"),
        StoreLocation::from_store(Arc::clone(&wal), "", "memory://wal"),
    )?;

    // The D12 gate runs first, in the same order as `main.rs`.
    stores.assert_conditional_put().await?;

    let db = stores.open_db(0).await?;
    let mut writer =
        PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut muts = Mutations::new();
    muts.append_journal(&INV_A, 0, b"entry");
    commit_durable(&mut writer, muts).await?;
    writer.seal().await?;

    let paths = |store: &Arc<dyn ObjectStore>| {
        let store = Arc::clone(store);
        async move {
            use futures::StreamExt;
            store
                .list(None)
                .map(|m| m.map(|m| m.location.to_string()))
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
        }
    };
    let main_paths = paths(&main).await?;
    let wal_paths = paths(&wal).await?;

    // WAL only in the WAL store.
    assert!(
        wal_paths.iter().any(|p| p.contains("/wal/")),
        "no WAL object found in the WAL store: {wal_paths:?}"
    );
    assert!(
        !main_paths.iter().any(|p| p.contains("/wal/")),
        "a WAL object slipped into the main store: {main_paths:?}"
    );

    // The manifest only in the main store — this is where `writer_epoch` lives,
    // so it must be on the tier that passed the main tier's conditional-PUT gate.
    assert!(
        main_paths.iter().any(|p| p.contains("manifest")),
        "no manifest found in the main store: {main_paths:?}"
    );
    assert!(
        !wal_paths.iter().any(|p| p.contains("manifest")),
        "a manifest slipped into the WAL store: {wal_paths:?}"
    );

    // And the root path follows the main store's prefix, for both sides.
    assert!(
        wal_paths.iter().all(|p| p.starts_with("prod/ranges/0/")),
        "the WAL is not under the shared root: {wal_paths:?}"
    );
    Ok(())
}

/// **Two layers of defence, each checked separately.**
///
/// This test builds exactly the failover scenario and asserts the hardest thing
/// to see: after node B becomes leader, node A **can no longer write even though
/// it does not know it has lost the right**. A is "killed" here by dropping its
/// renewal loop without revoking anything — exactly like a process hit by
/// `kill -9` or stuck in a GC pause.
///
/// The error A gets must be `Fenced` (layer 2, SlateDB's `writer_epoch`), not
/// `LeaseLost` (layer 1). Telling these two apart is the whole point of the
/// test: if only layer 1 blocked, the system would rely on the old leader
/// *knowing* it is dead — and a frozen process knows nothing.
#[tokio::test]
async fn frozen_old_leader_cannot_write_after_b_starts() -> Result<()> {
    use {
        cairn_store::{Claim, LeaseConfig, ObjectStores, Session, StoreError, StoreLocation},
        slatedb::object_store::{memory::InMemory, path::Path},
        std::time::Instant,
    };

    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(Arc::clone(&raw), "", "memory://"));
    stores.assert_conditional_put().await?;

    let cfg = |node: &str| {
        LeaseConfig::new()
            .with_node_id(node)
            .with_ttl(Duration::from_millis(300))
            .with_renew_interval(Duration::from_millis(30))
    };
    let root = Path::default();
    let session = |node: &str| Session::establish(Arc::clone(&raw), &root, cfg(node));

    // A becomes leader and can write.
    let a = session("a").await?;
    let Claim::Won(grant_a, keeper_a) = a.claim(PartitionId(0)).await? else {
        panic!("A did not get the empty partition");
    };
    let mut writer_a = PartitionWriter::open(grant_a, stores.open_db(0).await?).await?;
    let mut muts = Mutations::new();
    muts.append_journal(&INV_A, 0, b"from-a");
    commit_durable(&mut writer_a, muts).await?;

    // A is killed. The session renewal loop stops but does **not** revoke: a
    // killed process gets no chance to clean up, and a process in a GC pause
    // does not even know it needs to.
    a.crash_handle().crash();
    let validity_a = keeper_a.abort_without_revoking();
    assert!(validity_a.is_valid(), "A still believes it is the leader");

    // B waits until A's session has been silent past the TTL, marks it, then comes up.
    let b = session("b").await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let (grant_b, keeper_b) = loop {
        if let Claim::Won(g, k) = b.claim(PartitionId(0)).await? {
            break (g, k);
        }
        assert!(Instant::now() < deadline, "B did not come up within 5s");
        tokio::time::sleep(Duration::from_millis(30)).await;
    };
    assert!(grant_b.term() > 1, "term must increase on owner change");
    let mut writer_b = PartitionWriter::open(grant_b, stores.open_db(0).await?).await?;

    // B reads what A wrote — 0 bytes of data movement (G5).
    assert_eq!(writer_b.journal_unbounded(&INV_A).await?.len(), 1);

    // **Layer 2.** A still believes it is the leader (layer 1 does not block),
    // but B has bumped `writer_epoch`, so A fails at the **forced-durability**
    // step — not at the write. That is the core of the measurement, and the
    // reason every outward effect must require the ticket only that step
    // grants: A has no way to ack anything to anyone.
    assert!(validity_a.is_valid(), "layer 1 must NOT block in this test");
    let mut muts = Mutations::new();
    muts.append_journal(&INV_B, 0, b"from-a-zombie");
    writer_a.commit(muts).await?;
    let err = match writer_a.durability() {
        Ok(cairn_store::Durability::Wait(p)) => p.durable().await.unwrap_err(),
        Ok(cairn_store::Durability::Ready(_)) => panic!("zombie got a ticket right after writing"),
        Err(e) => e,
    };
    assert!(
        matches!(err, StoreError::Fenced),
        "must be fenced at the SlateDB layer, got: {err:?}"
    );

    // And the zombie's write leaves no trace.
    assert!(writer_b.journal_unbounded(&INV_B).await?.is_empty());

    let mut muts = Mutations::new();
    muts.append_journal(&INV_B, 0, b"of-b");
    commit_durable(&mut writer_b, muts).await?;
    keeper_b.release().await;
    b.close().await;
    Ok(())
}

/// **`file://` passes the D12 gate but CANNOT form a cluster.**
///
/// A real limitation, found while writing this test:
/// `LocalFileSystem::put_opts` accepts `PutMode::Create` but returns `not yet
/// implemented` for `PutMode::Update`. So it is **enough to fence** (WAL SSTs
/// are PUT with `Create`) but **not enough to hold a lease** (renewal is a CAS
/// on the etag).
///
/// Those two capabilities are separate, so they are two separate gates. Merging
/// them would also block the `Dev / single node` shape — perfectly valid, it
/// just has no leader election that needs CAS.
///
/// This test locks both halves: passes this gate, fails the other.
#[tokio::test]
async fn file_uri_fences_but_cannot_form_cluster() -> Result<()> {
    use cairn_store::{ObjectStores, StoreError, StoreLocation};

    let root = fresh_root("lease-local")?;
    let stores = ObjectStores::single(StoreLocation::parse(&format!("file://{}", root.display()))?);

    stores.assert_conditional_put().await?;

    let err = stores.assert_compare_and_swap().await.unwrap_err();
    assert!(
        matches!(err, StoreError::CompareAndSwapUnsupported { .. }),
        "must fail the CAS gate, got: {err:?}"
    );
    // The message must point at the way out, not just say "no".
    let msg = err.to_string();
    assert!(
        msg.contains("single node"),
        "the message lacks the way out: {msg}"
    );
    Ok(())
}

/// Conversely: a store with CAS passes the gate, and the lease renews over many
/// rounds.
///
/// Many rounds, not one: `PutMode::Update` is really used only by renewals, not
/// by the first claim (that one is `Create`).
#[tokio::test]
async fn cas_store_renews_across_many_rounds() -> Result<()> {
    use {
        cairn_store::{Claim, LeaseConfig, ObjectStores, Session, StoreLocation},
        slatedb::object_store::{memory::InMemory, path::Path},
    };

    let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::single(StoreLocation::from_store(Arc::clone(&raw), "", "memory://"));
    stores.assert_compare_and_swap().await?;

    let a = Session::establish(
        Arc::clone(&raw),
        &Path::default(),
        LeaseConfig::new()
            .with_node_id("a")
            .with_ttl(Duration::from_millis(300))
            .with_renew_interval(Duration::from_millis(30)),
    )
    .await?;
    let Claim::Won(_grant, keeper) = a.claim(PartitionId(0)).await? else {
        panic!("did not get the empty partition");
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        keeper.is_valid(),
        "the session must survive many renewal rounds, not just the first open"
    );
    keeper.release().await;
    a.close().await;
    Ok(())
}

/// **A follower reads through `ObjectStores::open_reader`**, on two real tiers.
///
/// Three things pinned, in order of importance:
///
/// 1. The reader sees what the writer wrote — both the journal and
///    `invocation_status` — within a **bounded** delay (a few poll cycles), not
///    "eventually".
/// 2. The healthy reader invariant holds: `durable_seq ≥ manifest_seq`.
/// 3. The reader **writes nothing** to the bucket: `FollowLatest` creates no
///    checkpoint. This is the "a follower has no write right" discipline pushed
///    down to the object level — a reader quietly writing checkpoints would pass
///    every functional test.
#[tokio::test]
async fn follower_sees_writer_writes_without_writing_to_bucket() -> Result<()> {
    use {
        cairn_store::{ObjectStores, StoreLocation},
        slatedb::object_store::memory::InMemory,
    };

    let main: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let wal: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stores = ObjectStores::split(
        StoreLocation::from_store(Arc::clone(&main), "prod", "memory://main/prod"),
        StoreLocation::from_store(Arc::clone(&wal), "", "memory://wal"),
    )?;
    stores.assert_conditional_put().await?;

    let db = stores.open_db(0).await?;
    let mut writer =
        PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut muts = Mutations::new();
    muts.append_journal(&INV_A, 0, b"entry-0")
        .set_invocation_status(&INV_A, b"\x00"); // Running
    commit_durable(&mut writer, muts).await?;

    let object_names = |store: &Arc<dyn ObjectStore>| {
        let store = Arc::clone(store);
        async move {
            use futures::StreamExt;
            store
                .list(None)
                .map(|m| m.map(|m| m.location.to_string()))
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
        }
    };
    let main_before = object_names(&main).await?;
    let wal_before = object_names(&wal).await?;

    let poll = Duration::from_millis(50);
    let reader = stores.open_reader(0, poll).await?;

    // (1) A bounded delay: give it a few poll cycles, not "wait forever".
    let deadline = std::time::Instant::now() + poll * 40;
    let journal = loop {
        let j = reader.journal(&INV_A).await?;
        if !j.is_empty() {
            break j;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("the reader did not see the write after 40 poll cycles");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(journal, vec![(0u32, bytes::Bytes::from_static(b"entry-0"))]);
    assert_eq!(
        reader.invocation_status(&INV_A).await?,
        Some(bytes::Bytes::from_static(b"\x00"))
    );

    // (2) The reader invariant while healthy.
    let f = reader.freshness();
    assert!(
        f.durable_seq >= f.manifest_seq,
        "a healthy reader with durable {} < manifest {}",
        f.durable_seq,
        f.manifest_seq
    );

    // (3) The reader leaves no write behind.
    assert_eq!(
        object_names(&main).await?,
        main_before,
        "the reader WROTE to the main store — FollowLatest must create no object"
    );
    assert_eq!(
        object_names(&wal).await?,
        wal_before,
        "the reader WROTE to the WAL store"
    );

    reader.close().await?;
    writer.seal().await?;
    Ok(())
}

/// A reader polling the manifest of an unchanged range: each poll is a single
/// GET (probing `id+1`, 404), with no extra GET of `gc/manifest.boundary` (fork
/// patch F7).
///
/// Before F7 each poll was two GETs: probe the next version, then GET the
/// boundary to make sure the cached version had not been replaced by GC. F7
/// drops the second GET when the process itself confirmed that very version
/// within a window shorter than GC's `min_age` (cairn: half of `gc_min_age`),
/// because GC deletes the next version only `min_age` after it was created.
///
/// Must see: 40 poll ticks of the reader (and an idle writer and coordinator)
/// GET the boundary no more than 3 times — before F7 it was ~40.
#[tokio::test]
async fn reader_poll_skips_boundary_get_each_tick() -> Result<()> {
    use {
        cairn_store::{
            testkit::{FaultyStore, Op},
            ObjectStores, StoreLocation,
        },
        slatedb::object_store::memory::InMemory,
    };

    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(
        faulty.clone(),
        "cairn",
        "memory://cairn",
    ));
    let db = stores.open_db(0).await?;
    let mut writer =
        PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let mut muts = Mutations::new();
    muts.set_invocation_status(&INV_A, b"\x00");
    commit_durable(&mut writer, muts).await?;
    writer.flush_memtable().await?;

    let poll = Duration::from_millis(50);
    let reader = stores.open_reader(0, poll).await?;
    let boundary_gets = || -> u64 {
        faulty
            .counts_by_source_object()
            .into_iter()
            .filter(|(_, op, _, kind, _)| *op == Op::Get && *kind == "boundary")
            .map(|(.., n)| n)
            .sum()
    };
    // The first poll after opening may still check the boundary (nothing confirmed yet).
    tokio::time::sleep(poll * 4).await;
    let before = boundary_gets();
    tokio::time::sleep(poll * 40).await;
    let during = boundary_gets() - before;
    assert!(
        during <= 3,
        "{during} boundary GETs in 40 poll ticks of an unchanged range"
    );

    reader.close().await?;
    writer.seal().await?;
    Ok(())
}

/// Hitting the L0 cap must not stall writes for a whole poll tick (F2).
///
/// SlateDB blocks memtable flushes while L0 is full (`l0_max_ssts` = 8) and
/// reopens only once the writer **sees** a compacted manifest. Before F2 that
/// path was all polling: the coordinator saw a new L0 at the
/// `DEFAULT_POLLS.compactor` tick, a worker picked up the job at the
/// `compaction_worker` tick, the writer saw the new manifest at the `manifest`
/// tick — one flush was measured stalling ~40 s. After F2 the writer signals
/// the coordinator on a new L0 and the coordinator signals the writer when the
/// manifest changes, so a flush waits only for the compaction itself (a few
/// tens of ms on `memory://`).
///
/// The test flushes 16 L0s in a row — twice the cap — with cairn's default poll
/// intervals, and requires **each** flush to finish under 5 s, far below the
/// old 30 s poll tick.
#[tokio::test(flavor = "multi_thread")]
async fn l0_cap_does_not_stall_writes_until_poll_tick() -> Result<()> {
    use {
        cairn_store::{ObjectStores, StoreLocation},
        slatedb::{
            config::{FlushOptions, FlushType},
            object_store::memory::InMemory,
        },
        std::time::Instant,
    };

    let stores = ObjectStores::single(StoreLocation::from_store(
        Arc::new(InMemory::new()),
        "",
        "memory://",
    ));
    let db = stores.open_db(0).await?;
    let mut slowest = Duration::ZERO;
    // The overall cap only makes old code answer red instead of hanging: each
    // time it hit the cap there it cost up to ~85 s (coordinator 30 s + worker up
    // to 45 s because of jitter + manifest 10 s).
    tokio::time::timeout(Duration::from_secs(300), async {
        for i in 0..16u32 {
            db.put(format!("k{i:04}").as_bytes(), &[b'v'; 256]).await?;
            let t0 = Instant::now();
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
            slowest = slowest.max(t0.elapsed());
        }
        anyhow::Ok(())
    })
    .await??;
    db.close().await?;
    println!("slowest memtable flush: {slowest:?}");
    anyhow::ensure!(
        slowest < Duration::from_secs(5),
        "a memtable flush stalled {slowest:?} — hitting the L0 cap still waits for a poll tick"
    );
    Ok(())
}

/// Reopening a range when L0 **has hit the cap** and a compaction job of an old
/// (dead) process is still `Running` — must not stall for a compactor poll tick
/// (300 s, `DEFAULT_POLLS`). A bug found by the shared-WAL simulation (seed 33,
/// `shared_wal_sim.rs`).
///
/// A dead worker's `Running` job blocks every other L0 compaction of the tree;
/// before the fork's follow-up patch to F2, the coordinator reclaimed it only on
/// the manifest poll round, and a writer stalled at the L0 cap flushes no new
/// L0, so no signal woke it. After the patch, the `commit_compacted` round (1 s,
/// running only while there is a job) reclaims jobs whose heartbeat expired and
/// wakes the worker; cairn sets a 2 s heartbeat and a 10 s expiry
/// (`config/tuning.rs`), so the flush waits ≲ 10 s.
///
/// Setup: process A opens the range, its first compaction is stuck reading an
/// SST (the `compacted/` GET hangs) so the job stays `Running`; A keeps flushing
/// up to 8 L0s and then "dies" (every request of A hangs). Process B reopens the
/// range on the same bucket and flushes.
#[tokio::test(flavor = "multi_thread")]
async fn reopen_at_l0_cap_with_orphan_compaction_job_does_not_wait_for_poll() -> Result<()> {
    use {
        cairn_store::{
            testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
            ObjectStores, StoreLocation,
        },
        slatedb::{
            config::{FlushOptions, FlushType},
            object_store::memory::InMemory,
        },
        std::time::Instant,
    };

    async fn flush_l0(db: &Db, i: u32) -> Result<()> {
        db.put(format!("k{i:04}").as_bytes(), &[b'v'; 256]).await?;
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await?;
        Ok(())
    }

    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a_store = FaultyStore::wrap(Arc::clone(&bucket));
    let a = ObjectStores::single(StoreLocation::from_store(a_store.clone(), "", "memory://"));
    // Compaction reads source SSTs with GET; the writer's L0 flushes only PUT.
    // Hanging GETs ⇒ the first job is stuck `Running`, the writer can still flush.
    a_store.add_rule(Rule::new(
        OpSet::GET | OpSet::HEAD,
        PathMatch::Contains("/compacted/".into()),
        Action::Hang,
    ));
    let db_a = a.open_db(0).await?;
    let l0_max = a.l0_max_ssts() as u32;
    for i in 0..l0_max {
        tokio::time::timeout(Duration::from_secs(10), flush_l0(&db_a, i))
            .await
            .map_err(|_| anyhow::anyhow!("A could not flush L0 number {i}"))??;
    }
    // Wait for A's worker to take the job and write its first heartbeat, then A "dies".
    tokio::time::sleep(Duration::from_secs(1)).await;
    a_store.add_rule(Rule::new(OpSet::ALL, PathMatch::Any, Action::Hang));

    let b = ObjectStores::single(StoreLocation::from_store(
        Arc::clone(&bucket),
        "",
        "memory://",
    ));
    let db_b = b.open_db(0).await?;
    let t0 = Instant::now();
    let flushed = tokio::time::timeout(Duration::from_secs(60), flush_l0(&db_b, l0_max)).await;
    let took = t0.elapsed();
    // No `close` while the flush is still stalled: close waits for that very flush.
    anyhow::ensure!(
        flushed.is_ok() && took < Duration::from_secs(20),
        "the first flush after reopening stalled {took:?} behind an orphan compaction job — waiting for a poll tick"
    );
    flushed??;
    db_b.close().await?;
    drop(db_a);
    Ok(())
}

/// The S3 client cairn builds from `s3://…` must carry cairn's retry cap, not
/// object_store's 180 s default: every GET/PUT cairn makes directly (owner
/// records, `cluster.conf`, sessions) could hang that long under a burst of
/// 503s.
///
/// Must see: the client's configuration (printed via `Debug`, no network
/// needed) has `retry_timeout` equal to `S3_CLIENT_RETRY_TIMEOUT`.
#[test]
fn s3_client_carries_cairn_retry_budget() {
    let loc =
        cairn_store::StoreLocation::parse("s3://cairn-retry-test/prefix").expect("build S3 client");
    let dbg = format!("{:?}", loc.store());
    let want = format!("retry_timeout: {:?}", cairn_store::S3_CLIENT_RETRY_TIMEOUT);
    assert!(
        dbg.contains(&want),
        "`{want}` missing from the client configuration: {dbg}"
    );
    assert!(
        cairn_store::S3_CLIENT_RETRY_TIMEOUT <= std::time::Duration::from_secs(30),
        "the S3 client cap must be shorter than the 180 s default"
    );
}

/// SlateDB's `Admin` operations (reading manifests, listing checkpoints…) retry
/// object-store errors **forever** in production
/// (`object_store_max_retries: None`). The background loops calling them —
/// range sizes for auto split, the reference scanner/GC, orphan-child cleanup —
/// hang with them while the store stays broken.
///
/// 1. Range 0's Db has a manifest.
/// 2. Every `manifest` GET fails, forever.
/// 3. Call `range_size`, `range_checkpoints`, `referenced_ranges`.
///
/// Must see: each operation returns an error within its cap (virtual time),
/// without hanging — and `referenced_ranges` treats a read error as "cannot
/// prove" (rule 5).
#[tokio::test(start_paused = true)]
async fn slatedb_admin_reads_are_bounded() {
    use {
        cairn_store::{
            testkit::{Action, FaultyStore, OpSet, PathMatch, Rule},
            ObjectStores, StoreLocation,
        },
        slatedb::object_store::memory::InMemory,
    };
    let store = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    stores
        .open_db(0)
        .await
        .expect("open Db")
        .close()
        .await
        .expect("close Db");
    store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("manifest".into()),
        Action::Fail { times: None },
    ));
    let bound = Duration::from_secs(600);
    let r = tokio::time::timeout(bound, stores.range_size(PartitionId(0)))
        .await
        .expect("range_size must be bounded");
    assert!(r.is_err(), "{r:?}");
    let r = tokio::time::timeout(bound, stores.range_checkpoints(PartitionId(0)))
        .await
        .expect("range_checkpoints must be bounded");
    assert!(r.is_err(), "{r:?}");
    let refs = tokio::time::timeout(bound, stores.referenced_ranges(&[PartitionId(0)]))
        .await
        .expect("referenced_ranges must be bounded")
        .expect("failed read ⇒ unknown, not an error");
    assert!(
        refs.unknown.is_some(),
        "a failed read must count as unprovable"
    );
}
