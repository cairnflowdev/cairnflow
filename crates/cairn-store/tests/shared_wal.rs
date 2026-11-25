// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The per-node shared WAL.
//!
//! Two groups of tests:
//!
//! - **11 tests of the design's acceptance list** (the normal path and the
//!   caps): PUTs per node, ack after PUT and in order, failover, zombies,
//!   cooperative handover, GC, the WAL-kind mark, hung PUTs, followers, noisy
//!   neighbours.
//! - **One regression test for each counterexample C1–C7** that model checking
//!   of the shared WAL design found. Each test walks exactly the shape of the
//!   counterexample's trace on a real store (in-memory + `FaultyStore`).
//!
//! A "node" here is a `Session` + a `NodeWal` on the same bucket — enough to
//! have two incarnations compete for a range without building a server.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use {
    anyhow::{bail, ensure, Result},
    bytes::Bytes,
    cairn_store::{
        acquire_shared, keys,
        testkit::{
            Action, FaultyStore, Gate, Op, OpSet, PathClass, PathMatch, Rule, StoreCounts, TripAt,
            Tripwire,
        },
        Claim, LeadershipGrant, LeaseConfig, LeaseKeeper, LogId, Mutations, NodeTailers, NodeWal,
        NodeWalConfig, NodeWals, ObjectStores, PartitionId, PartitionKey, PartitionWriter, Session,
        StoreLocation, WalKind,
    },
    futures::TryStreamExt,
    slatedb::object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
};

const TTL: Duration = Duration::from_millis(400);
const PACE: Duration = Duration::from_millis(50);

struct Bucket {
    faulty: Arc<FaultyStore>,
    stores: ObjectStores,
}

fn bucket() -> Bucket {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let stores = ObjectStores::single(StoreLocation::from_store(
        faulty.clone(),
        "cairn",
        "memory://cairn",
    ));
    Bucket { faulty, stores }
}

/// Like [`bucket`], but the WAL tier declares `atomic_put = false`
/// (`--wal-atomic-put false`): the store may tear objects (MinIO EC, quorum) —
/// the shared WAL must not resolve an ambiguous log PUT by reading back.
fn torn_bucket() -> Bucket {
    let b = bucket();
    Bucket {
        stores: b.stores.with_wal_atomic_put(Some(false)),
        ..b
    }
}

fn fast() -> NodeWalConfig {
    NodeWalConfig {
        window: Duration::from_millis(5),
        stripes: 2,
        inflight: 1,
        object_cap: 1 << 20,
        // The product's linger: every test of this file runs the default cut
        // rule; a test that needs another L sets its own.
        linger: NodeWalConfig::default().linger,
        frame_age_flush: Duration::from_secs(3600),
        put_timeout: Duration::from_millis(500),
        put_attempts: 2,
        gc_interval: Duration::from_millis(50),
        meter: None,
        rotation_net: None,
    }
}

struct Node {
    name: &'static str,
    session: Session,
    wal: NodeWal,
}

async fn node(b: &Bucket, name: &'static str, cfg: NodeWalConfig) -> Result<Node> {
    let session = Session::establish(
        b.faulty.clone(),
        &Path::from("cairn"),
        LeaseConfig::new()
            .with_node_id(name)
            .with_ttl(TTL)
            .with_renew_interval(PACE),
    )
    .await?;
    let id = session.current().expect("session just opened");
    let wal = NodeWal::start(&b.stores, id, cfg);
    Ok(Node { name, session, wal })
}

/// Take range `range` and open a writer through the shared-WAL sequence.
async fn acquire(b: &Bucket, n: &Node, range: u32) -> Result<(PartitionWriter, LeaseKeeper)> {
    for _ in 0..200 {
        match n.session.claim(PartitionId(range)).await? {
            Claim::Won(grant, mut keeper) => {
                let db = acquire_shared(&b.stores, &n.wal, &mut keeper, range).await?;
                return Ok((PartitionWriter::open(grant, db).await?, keeper));
            }
            Claim::Held(_) => tokio::time::sleep(PACE).await,
        }
    }
    bail!("{} did not get range {range}", n.name)
}

fn id(n: u8) -> [u8; 24] {
    let mut id = [0u8; 24];
    id[23] = n;
    id
}

/// Write `value` into the status row of invocation `n` and wait for it to be **durable**.
async fn put(w: &mut PartitionWriter, n: u8, value: &[u8]) -> Result<()> {
    let mut m = Mutations::new();
    m.set_invocation_status(&id(n), value);
    w.commit(m).await?;
    w.wait_durable().await?;
    Ok(())
}

async fn get(w: &PartitionWriter, n: u8) -> Result<Option<Bytes>> {
    Ok(w.get(&keys::invocation_status(&id(n))).await?)
}

/// The names of the log's objects (data, seal), by seq.
async fn objects(b: &Bucket, log: &LogId) -> Result<Vec<u64>> {
    let dir = Path::from(format!(
        "cairn/nodewal/{}/{}/{}",
        log.session.node, log.session.inc, log.stripe
    ));
    let listed: Vec<_> = b.faulty.inner().list(Some(&dir)).try_collect().await?;
    let mut seqs: Vec<u64> = listed
        .iter()
        .filter(|m| !m.location.as_ref().contains("/floor/"))
        .filter_map(|m| m.location.filename()?.parse().ok())
        .collect();
    seqs.sort();
    Ok(seqs)
}

async fn owner(b: &Bucket, range: u32) -> Result<cairn_store::OwnerRecord> {
    Ok(cairn_store::read_owner(
        &(b.faulty.clone() as Arc<dyn ObjectStore>),
        &Path::from("cairn"),
        PartitionId(range),
    )
    .await?
    .expect("the range must have an owner record"))
}

/// Wait until `f` holds, at most `limit`.
async fn eventually<F, Fut>(limit: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = tokio::time::Instant::now();
    while start.elapsed() < limit {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

// ───────────────────────────────────────────────── 11 acceptance tests

/// 16 ranges busy at once on one node ⇒ the PUTs into the node log follow
/// **window × stream**, not the number of ranges. With the original WAL every
/// write round costs 16 PUTs.
#[tokio::test]
async fn wal_put_count_scales_with_nodes_not_ranges() -> Result<()> {
    let b = bucket();
    // No GC tick while measuring: the log's index is one PUT per GC tick, not
    // per window — at the 50 ms of `fast()` it would mix into the count.
    let cfg = NodeWalConfig {
        window: Duration::from_millis(30),
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut writers, mut _keepers) = (Vec::new(), Vec::new());
    for r in 0..16 {
        let (w, k) = acquire(&b, &a, r).await?;
        writers.push(w);
        _keepers.push(k);
    }
    let before = b.faulty.counts();
    let rounds = 5;
    for round in 0..rounds {
        // Every range issues a commit at almost the same time and then waits for durability.
        for w in writers.iter_mut() {
            let mut m = Mutations::new();
            m.set_invocation_status(&id(round), b"v");
            w.commit(m).await?;
        }
        futures::future::try_join_all(writers.iter().map(|w| w.wait_durable())).await?;
    }
    let puts = b
        .faulty
        .counts()
        .since(&before)
        .get(Op::Put, PathClass::NodeWal);
    // At most a few objects per stream per round (a window may cut a round in two).
    assert!(
        puts <= rounds as u64 * 2 * 3,
        "{puts} node-log PUTs for {rounds} rounds × 16 ranges"
    );
    assert!(
        puts < rounds as u64 * 16 / 2,
        "{puts} PUTs — close to one PUT per range per round"
    );
    Ok(())
}

/// A gate blocking the node log's PUTs ⇒ no commit of any range on the node is
/// considered durable (D11).
#[tokio::test]
async fn shared_wal_ack_only_after_put() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w1, _k1) = acquire(&b, &a, 1).await?;
    let gate = Gate::closed();
    b.faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Gate(gate.clone())));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"x");
    w0.commit(m).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"y");
    w1.commit(m).await?;
    let both = async { futures::future::try_join(w0.wait_durable(), w1.wait_durable()).await };
    tokio::pin!(both);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut both)
            .await
            .is_err(),
        "a commit became durable while the node log's PUT was still blocked"
    );
    assert!(gate.blocked() >= 1);
    gate.open();
    tokio::time::timeout(Duration::from_secs(5), both).await??;
    Ok(())
}

/// The PUT of object n still hangs ⇒ object n+1 is not issued yet (one PUT in
/// flight per stream) ⇒ n+1 cannot be acked before n.
#[tokio::test]
async fn ack_in_order_within_stripe() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    // Ranges 0 and 2 share stream 0.
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    let gate = Gate::closed();
    let rule = b
        .faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Gate(gate.clone())));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"first");
    w0.commit(m).await?;
    assert!(eventually(Duration::from_secs(2), || async { gate.blocked() == 1 }).await);
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"second");
    w2.commit(m).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        gate.blocked(),
        1,
        "the second object was in flight while the first was not yet durable"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), w2.wait_durable())
            .await
            .is_err(),
        "a later write was acked while the earlier object still hung"
    );
    b.faulty.remove_rule(rule);
    gate.open();
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    Ok(())
}

/// The path of object `seq` in stream 0's log of `n`.
fn object_path(n: &Node, seq: u64) -> String {
    let log = n.wal.log_of(0);
    format!(
        "nodewal/{}/{}/{}/{seq:020}",
        log.session.node, log.session.inc, log.stripe
    )
}

/// W = 2: object n+1 is in flight and **durable** while n still hangs — but the
/// range in n+1 is still not acked until n is durable.
#[tokio::test]
async fn ack_in_order_with_two_puts_in_flight() -> Result<()> {
    let b = bucket();
    let a = node(
        &b,
        "a",
        NodeWalConfig {
            inflight: 2,
            ..fast()
        },
    )
    .await?;
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    let gate = Gate::closed();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(object_path(&a, 0)),
        Action::Gate(gate.clone()),
    ));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"first");
    w0.commit(m).await?;
    assert!(eventually(Duration::from_secs(2), || async { gate.blocked() == 1 }).await);
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"second");
    w2.commit(m).await?;
    let log = a.wal.log_of(0);
    assert!(
        eventually(Duration::from_secs(2), || async {
            objects(&b, &log).await.unwrap() == vec![1]
        })
        .await,
        "W = 2 but the second object is not in flight"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), w2.wait_durable())
            .await
            .is_err(),
        "object #1 was acked while #0 still hung"
    );
    b.faulty.remove_rule(rule);
    gate.open();
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    Ok(())
}

/// W = 2: #0 still in flight, #1 already on the store; B seals at the hole #0 ⇒
/// #1 is above the seal, outside the canonical part: B does not replay it, A
/// does not ack it.
#[tokio::test]
async fn seal_below_landed_object_discards_it() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        inflight: 2,
        put_timeout: Duration::from_secs(20),
        put_attempts: 1,
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    let gate = Gate::closed();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(object_path(&a, 0)),
        Action::Gate(gate.clone()),
    ));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"in-hole");
    w0.commit(m).await?;
    assert!(eventually(Duration::from_secs(2), || async { gate.blocked() == 1 }).await);
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"above-hole");
    w2.commit(m).await?;
    let log = a.wal.log_of(0);
    assert!(
        eventually(Duration::from_secs(2), || async {
            objects(&b, &log).await.unwrap() == vec![1]
        })
        .await
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), w2.wait_durable())
            .await
            .is_err(),
        "A acked object #1 lying above the hole #0"
    );
    b.faulty.remove_rule(rule);
    k0.crash_handle().crash();
    k2.crash_handle().crash();

    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 2).await?;
    assert_eq!(
        objects(&b, &log).await?,
        vec![0, 1],
        "the seal is not at the hole #0"
    );
    assert_eq!(get(&wb, 2).await?, None, "B replayed above the seal");
    gate.open();
    assert!(w0.wait_durable().await.is_err());
    assert!(
        w2.wait_durable().await.is_err(),
        "A acked an object above the seal"
    );
    Ok(())
}

/// Node A dies with frames of **three** ranges not yet in L0 ⇒ the new owner of
/// each range sees exactly the acked data.
#[tokio::test]
async fn failover_replays_frames_of_dead_node() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let mut keepers = Vec::new();
    for r in 0..3 {
        let (mut w, k) = acquire(&b, &a, r).await?;
        put(&mut w, r as u8, format!("from-a-{r}").as_bytes()).await?;
        keepers.push((w, k));
    }
    // A dies: the session ends, nothing is flushed to L0.
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget(keepers);

    let bn = node(&b, "b", fast()).await?;
    for r in 0..3 {
        let (w, _k) = acquire(&b, &bn, r).await?;
        assert_eq!(
            get(&w, r as u8).await?.as_deref(),
            Some(format!("from-a-{r}").as_bytes()),
            "range {r} lost acked rows"
        );
    }
    Ok(())
}

/// A freezes, B seals and takes the range, A wakes up and writes on ⇒ A's PUT
/// misses, A fail-stops, B sees no frame of A after the seal.
#[tokio::test]
async fn zombie_node_cannot_write_after_seal() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut wa, ka) = acquire(&b, &a, 0).await?;
    put(&mut wa, 1, b"before").await?;
    // Freeze A: stop renewing, pin validity — A still believes it is leader.
    ka.crash_handle().crash();

    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 1).await?.as_deref(), Some(&b"before"[..]));

    // A wakes up and writes.
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"zombie");
    let late = match wa.commit(m).await {
        Ok(()) => wa.wait_durable().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(
        late.is_err(),
        "zombie A was acked after its own log had been sealed by B"
    );
    let mut fs = a.wal.fail_stopped();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), fs.wait_for(|v| v.is_some()))
            .await
            .is_ok(),
        "A did not fail-stop when its log was sealed"
    );
    assert_eq!(get(&wb, 2).await?, None, "B saw zombie frame after seal");
    Ok(())
}

/// **A zombie writes into the seal's slot of a log that was finally removed**
/// (M3 model `without_the_clock_forgetting_a_dead_log_lets_its_zombie_ack`,
/// reproduced intermittently by [`zombie_node_cannot_write_after_seal`]).
/// Assumes a broken clock: the zombie's grant is pinned valid after its
/// session is considered dead.
///
/// Interleaving:
/// 1. A takes range 0 and writes; A freezes (stops renewing, validity pinned).
/// 2. B takes range 0: seals A's log, replays, drops A's segment from the owner
///    record. No owner record lists that log any more.
/// 3. Node C comes up (a scan round early enough, in case B's hinted round
///    runs before A's session is considered dead). Wait for the round that
///    finally removes A's log (a `wal-log` entry in the trash).
/// 4. A wakes up and writes.
///
/// Must see: A's write is not acked, and A fail-stops. Before the fix, the
/// final removal moved the seal into the trash too: A's PUT into the old
/// seal's slot succeeded and was acked — a lost ack.
#[tokio::test]
async fn zombie_cannot_ack_after_its_sealed_log_is_forgotten() -> Result<()> {
    use cairn_store::trash::{Subject, Trash};
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut wa, ka) = acquire(&b, &a, 0).await?;
    put(&mut wa, 1, b"before").await?;
    let log = a.wal.log_of(0);
    // (1)
    ka.crash_handle().crash();
    // (2)
    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 1).await?.as_deref(), Some(&b"before"[..]));
    // (3)
    let _cn = node(&b, "c", fast()).await?;
    let bin = Trash::new(b.faulty.clone(), &Path::from("cairn"));
    let forgotten = eventually(Duration::from_secs(15), || async {
        bin.entries()
            .await
            .unwrap_or_default()
            .iter()
            .any(|e| matches!(&e.subject, Some(Subject::WalLog(l)) if *l == log))
    })
    .await;
    assert!(
        forgotten,
        "bad setup: B's scan round has not finally removed log {log}"
    );
    // (4)
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"zombie");
    let late = match wa.commit(m).await {
        Ok(()) => wa.wait_durable().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(
        late.is_err(),
        "zombie A was acked after its sealed log was finally removed"
    );
    Ok(())
}

/// Cooperative handover (clean seal, then release) ⇒ the taker reads no object
/// of the old node log.
#[tokio::test]
async fn cooperative_handoff_replays_nothing() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut wa, mut ka) = acquire(&b, &a, 0).await?;
    put(&mut wa, 1, b"v1").await?;
    let floor = a.wal.last_wal_id(0).expect("the range has written");
    // Exactly the actor's step-down sequence: seal, mark clean, yield.
    wa.seal().await?;
    ka.writer_sealed().await?;
    ka.release().await;
    let rec = owner(&b, 0).await?;
    assert!(rec.released && rec.wal_segments.is_empty(), "{rec:?}");
    assert!(rec.wal_floor_id >= Some(floor), "{rec:?}");

    let bn = node(&b, "b", fast()).await?;
    let before = b.faulty.counts();
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    let reads = b.faulty.counts().since(&before);
    assert_eq!(
        reads.get(Op::Get, PathClass::NodeWal) + reads.get(Op::Head, PathClass::NodeWal),
        0,
        "the taker read the old node log after a clean handover"
    );
    assert_eq!(get(&wb, 1).await?.as_deref(), Some(&b"v1"[..]));
    Ok(())
}

/// An object holding a frame of a cold range is not deleted until that range
/// flushes; forcing a flush by frame age frees it within ≤ T.
#[tokio::test]
async fn gc_keeps_object_until_all_ranges_flushed() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        window: Duration::from_millis(40),
        frame_age_flush: Duration::from_millis(600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    // Ranges 0 and 2 share stream 0: one object holds frames of both. An
    // opening write sets the next cut deadline one window later (a row that
    // arrives while the stream is idle is cut at once), so the two commits
    // below go into the same object.
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    put(&mut w0, 9, b"opening").await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"hot");
    w0.commit(m).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"cold");
    w2.commit(m).await?;
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    let log = a.wal.log_of(0);
    let target = *objects(&b, &log).await?.last().expect("an object exists");

    // Only range 0 flushes: the object must remain (range 2 is not in L0 yet).
    w0.flush_memtable().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        objects(&b, &log).await?.contains(&target),
        "GC deleted an object that still has a frame not yet in L0"
    );

    // Range 2 writes nothing more: forcing a flush by frame age (T = 600 ms)
    // pushes it into L0, and GC deletes the object.
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&target)
        })
        .await,
        "forcing a flush by frame age did not free GC"
    );
    assert_eq!(get(&w2, 2).await?.as_deref(), Some(&b"cold"[..]));
    Ok(())
}

/// **A failed DELETE does not make the floor GC round drop progress that is
/// already durable** (seen in a 4-hour soak: the WAL floor stood still for
/// > 600 s three times on the live log of an incarnation alive for 4 hours).
///
/// `gc_log` writes the floor and then deletes the batch of objects below it; a
/// failed DELETE (the `s3-standard-faults*` profile injects 0.2 % PUT/DELETE
/// faults) made the whole round return `Err` **after** the floor was already
/// durable. The caller dropped the result: it did not record the new floor,
/// did not trim the in-RAM index, did not report the floor — the next round
/// started again from the old floor and deleted the whole batch again. The
/// batch grows over the incarnation's life, so from ~2 000 objects almost every
/// round had a failed DELETE: the GC round stretched to minutes (the floor on
/// the bucket stood still between rounds), the index and its writes swelled,
/// and the node's floor alert never fired.
///
/// 1. Node `a`, range 0 writes 12 durable commits (one object per commit, not
///    in L0 yet, so GC cannot delete anything).
/// 2. The DELETE of the log's third object fails **once**.
/// 3. Range 0 flushes to L0; GC deletes all 12 objects.
///
/// Must see: every object is eventually deleted (no leak), and no object other
/// than the failed one is DELETEd more than once — the next round retries only
/// what failed.
#[tokio::test]
async fn failed_gc_delete_keeps_the_durable_floor() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for n in 0..12 {
        put(&mut w, n, b"v").await?;
    }
    let log = a.wal.log_of(0);
    let before = objects(&b, &log).await?;
    ensure!(before.len() >= 12, "one object per commit: {before:?}");
    let dir = format!(
        "cairn/nodewal/{}/{}/{}",
        log.session.node, log.session.inc, log.stripe
    );
    let failing = before[2];
    b.faulty.add_rule(Rule::new(
        OpSet::DELETE,
        PathMatch::Contains(format!("{dir}/{failing:020}")),
        Action::Fail { times: Some(1) },
    ));
    b.faulty.record_writes();
    w.flush_memtable().await?;
    ensure!(
        eventually(Duration::from_secs(5), || async {
            let left = objects(&b, &log).await.unwrap();
            before.iter().all(|s| !left.contains(s))
        })
        .await,
        "GC did not delete every object already in L0: {:?} remain",
        objects(&b, &log).await?
    );
    // A few more GC ticks (50 ms) so that a re-delete of the whole batch — if any — shows up.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut deletes: BTreeMap<u64, usize> = BTreeMap::new();
    for (op, path) in b.faulty.take_writes() {
        let path = path.as_ref().to_string();
        let Some(name) = path.strip_prefix(&format!("{dir}/")) else {
            continue;
        };
        if op == Op::Delete {
            if let Ok(seq) = name.parse::<u64>() {
                *deletes.entry(seq).or_default() += 1;
            }
        }
    }
    let again: Vec<_> = deletes
        .iter()
        .filter(|(seq, n)| **n > 1 && **seq != failing)
        .collect();
    ensure!(
        again.is_empty(),
        "the GC round after a failed DELETE re-deleted deleted objects (seq → count): {again:?}"
    );
    Ok(())
}

/// Opening with a WAL kind that differs from the mark in the manifest ⇒
/// refused, rows are not silently dropped.
#[tokio::test]
async fn wal_kind_mismatch_refuses_open() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"only-in-node-log").await?;
    // A range Db on the shared WAL: opening it with the native WAL must fail.
    assert!(
        b.stores.open_db(0).await.is_err(),
        "a Db on the shared WAL opened with the native WAL — rows in the node log are dropped"
    );
    // And the other way round: a Db on the native WAL cannot open with the shared WAL.
    let native = b.stores.open_db(7).await?;
    native.close().await?;
    let mut k7 = match a.session.claim(PartitionId(7)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 7"),
    };
    assert!(
        acquire_shared(&b.stores, &a.wal, &mut k7, 7).await.is_err(),
        "a Db on the native WAL opened with the shared WAL"
    );
    Ok(())
}

/// **A hung PUT ⇒ the range stalls at most `stall_max`, then the stripe
/// rotates** (formerly the node stepped down by itself).
///
/// 1. Node a holds range 0 (stripe 0).
/// 2. Every PUT onto a's stripe-0 log hangs — including the seal a itself puts
///    on that log.
/// 3. Range 0 commits.
///
/// Must see: the commit fails within the cap (NW-2: no ack); the node does
/// **not** fail-stop; range 0 now writes into a new log of the same
/// incarnation, with a stripe id outside `0..S`;
/// `cairnflow_nodewal_stripe_rotations_total{cause="stall"}` rises.
#[tokio::test]
async fn hung_put_bounds_stall_then_rotates_its_stripe() -> Result<()> {
    use cairn_store::nodewal::stats::{stripe_rotations, RotationCause};
    let b = bucket();
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_millis(200),
        put_attempts: 2,
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let stall = cfg.stall_max();
    let stripes = cfg.stripes;
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let log = a.wal.log_of(0);
    let before = stripe_rotations(RotationCause::Stall);
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(format!(
            "nodewal/{}/{}/{}/",
            log.session.node, log.session.inc, log.stripe
        )),
        Action::Hang,
    ));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"hang");
    w.commit(m).await?;
    let started = tokio::time::Instant::now();
    let res = tokio::time::timeout(stall * 3, w.wait_durable()).await;
    assert!(
        matches!(res, Ok(Err(_))),
        "a commit on a hung PUT must fail within the cap, got {res:?}"
    );
    assert!(
        started.elapsed() <= stall * 2,
        "stalled {:?} > cap {stall:?}",
        started.elapsed()
    );
    assert!(
        eventually(Duration::from_secs(5), || async { a.wal.log_of(0) != log }).await,
        "range 0's stripe did not rotate"
    );
    let rotated = a.wal.log_of(0);
    assert_eq!(rotated.session, log.session, "rotation changed incarnation");
    assert!(
        rotated.stripe >= stripes,
        "new stripe id {} equals one of the initial ids",
        rotated.stripe
    );
    assert!(
        stripe_rotations(RotationCause::Stall) > before,
        "cairnflow_nodewal_stripe_rotations_total{{cause=\"stall\"}} did not rise"
    );
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "a hung PUT took down the whole node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    Ok(())
}

/// **The id claim of a rotating stream tolerates an ambiguous `Create`** (a new
/// conditional write — `LogStore::claim`).
///
/// 1. Node a (S = 2) holds range 0; every PUT onto a's stripe-0 log hangs.
/// 2. The first id claim (stripe 2: `Create` of floor 0) reaches the store and
///    then gets `AlreadyExists` from object_store's own retry.
///
/// Must see: the stream does not read back but abandons id 2 (one extra id
/// does no harm) and claims id 3; the node does not fail-stop.
#[tokio::test]
async fn rotation_claim_skips_an_id_whose_create_was_ambiguous() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    let b = bucket();
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_millis(200),
        put_attempts: 2,
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let log = a.wal.log_of(0);
    assert_eq!(log.stripe, 0, "bad setup: range 0 must be on stripe 0");
    let dir = |stripe: u16| format!("nodewal/{}/{}/{stripe}/", log.session.node, log.session.inc);
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(dir(0)),
        Action::Hang,
    ));
    b.faulty.add_rule(Rule::ambiguous_put(
        &format!("{}floor/", dir(2)),
        Ambiguity::Retried,
    ));
    assert!(put(&mut w, 1, b"hang").await.is_err());
    assert!(
        eventually(Duration::from_secs(5), || async { a.wal.log_of(0) != log }).await,
        "range 0's stripe did not rotate"
    );
    assert_eq!(a.wal.log_of(0).stripe, 3, "ambiguous id 2 not abandoned");
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "an ambiguous id claim took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    Ok(())
}

/// **A failed id claim of a rotating stream does not fail-stop** (seen in a
/// chaos run: a node slow on every WAL PUT ⇒ the floor-0 `Create` of the new
/// log is slow too ⇒ 18 `wal_stall`s).
///
/// 1. Node a (S = 2, `stall_max` 400 ms, with a meter) holds range 0 (slot 0);
///    every PUT onto a's stripe-0 log hangs, and every floor PUT (`/floor/`)
///    of a hangs — the new id claim (`Create` of floor 0) never finishes.
/// 2. Range 0 commits ⇒ the stream stalls until `stall_max` ⇒ rotates; the
///    claim fails.
/// 3. Wait 4 s (longer than an old claim round: 4 × the cap).
/// 4. Remove the floor-hang rule.
///
/// Must see: in step 3 the node does **not** fail-stop, slot 0 has no stream
/// yet (the slot's log is still the old one; taking range 2 — slot 0 — is
/// refused, range 1 — slot 1 — is still taken and writable), the meter drains
/// (`draining=1`); after step 4 the background retry claims a new id, slot 0
/// has a stream again (range 2 is taken and writable) and the node stops
/// draining. Formerly a failed claim ⇒ a `wal_stall` fail-stop.
#[tokio::test]
async fn failed_rotation_claim_keeps_the_slot_rotated_and_retries() -> Result<()> {
    let b = bucket();
    let meter = cairn_store::health::SlowMeter::new("a", Duration::from_secs(1));
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_millis(200),
        put_attempts: 2,
        gc_interval: Duration::from_secs(3600),
        meter: Some(meter.clone()),
        // Another node can still take ranges (the failed-rotation net): the slot waits in the
        // background.
        rotation_net: Some(cairn_store::nodewal::RotationNet::new(|| true)),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let log = a.wal.log_of(0);
    assert_eq!(log.stripe, 0, "bad setup: range 0 must be on stripe 0");
    let mine = format!("nodewal/{}/{}/", log.session.node, log.session.inc);
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(format!("{mine}0/")),
        Action::Hang,
    ));
    let floors = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("/floor/".into()),
        Action::Hang,
    ));
    assert!(
        put(&mut w, 1, b"hang").await.is_err(),
        "a commit on a hung PUT became durable"
    );
    drop(w);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "a failed id claim took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.wal.log_of(0), log, "slot 0 got a stream; claim hangs");
    assert!(
        meter.published().draining,
        "the node does not drain while a slot has no stream"
    );
    // A range mapped to the slot without a stream: refused to open. A range of the other slot:
    // opens normally.
    let mut k2 = match a.session.claim(PartitionId(2)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 2"),
    };
    assert!(
        acquire_shared(&b.stores, &a.wal, &mut k2, 2).await.is_err(),
        "range 2 opened into slot 0 without a stream"
    );
    drop(k2);
    let (mut w1, _k1) = acquire(&b, &a, 1).await?;
    put(&mut w1, 2, b"o-1").await?;

    assert!(b.faulty.remove_rule(floors));
    assert!(
        eventually(Duration::from_secs(20), || async { a.wal.log_of(0) != log }).await,
        "the background retry did not claim a new id"
    );
    assert!(
        eventually(Duration::from_secs(2), || async {
            !meter.published().draining
        })
        .await,
        "the node still drains after every slot has a stream"
    );
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    put(&mut w2, 3, b"o-0-new").await?;
    assert!(a.wal.fail_stopped().borrow().is_none());
    Ok(())
}

/// **A slow PUT that still arrives is not cancelled** (like upstream SlateDB,
/// no per-PUT cap).
///
/// 1. Node a holds range 0; a 500 ms cap per attempt, a `stall_max` budget of
///    1 s.
/// 2. Every PUT onto a's next log object sleeps 750 ms **before** reaching the
///    store (`Action::Delay`: cancelling the future cancels the request too).
/// 3. Range 0 commits.
///
/// Must see: the commit is durable, the node does not fail-stop. Formerly each
/// attempt was cancelled by `put_timeout` at 500 ms, before it could arrive:
/// the budget ran out without any attempt finishing. Now the slow attempt keeps
/// running, a parallel `Create` starts after 500 ms; the first one arrives at
/// 750 ms.
#[tokio::test]
async fn slow_put_is_not_cancelled_at_put_timeout() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..put_budget()
    };
    let slow = cfg.put_timeout * 3 / 2;
    assert!(
        slow < cfg.stall_max(),
        "bad setup: the slow PUT must finish within the budget"
    );
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let inc = a.session.current();
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(next_object_path(&b, &a).await?),
        Action::Delay(slow),
    ));
    put(&mut w, 1, b"slow-but-arrives")
        .await
        .map_err(|e| anyhow::anyhow!("a slow PUT within the budget was not acked: {e}"))?;
    assert_eq!(get(&w, 1).await?.as_deref(), Some(&b"slow-but-arrives"[..]));
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "fail-stop because of a slow PUT: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.session.current(), inc);
    Ok(())
}

/// **Dropping the last `NodeWals` copy while the incarnation is alive does not
/// fail-stop the session** (`graceful_shutdown::sigterm_hands_ranges_over_before_ttl`
/// was red under load — B waited for the whole TTL before leading A's ranges).
///
/// Interleaving (the graceful shutdown of `cairnflow` after `SIGTERM`):
/// 1. The node has a live session; `NodeWals` has built the incarnation's WAL
///    (each range's engine holds a `NodeWals` copy through its builder).
/// 2. `RunningNode::shutdown` stops every engine (seal, clean yield). The last
///    engine thread exits and drops the last `NodeWals` copy — **before**
///    `session.close()`.
/// 3. `Drop` of `NodeWals` stops the WAL ⇒ the stream dies with "incarnation …
///    has ended" ⇒ the fail-stop watcher task calls `Session::fail_stop` ⇒
///    `lose`: abandons the live incarnation, writes `closed`, wakes the renew
///    loop — which opens a new incarnation, racing the `close()` that is
///    cancelling it. A `Create` PUT that reached the store while the task was
///    cancelled before writing `current` ⇒ a `live` session nobody renews, and
///    peers consider the node alive until the TTL runs out.
/// 4. The test drops the only `NodeWals` copy and waits (with a cap) to see
///    whether the incarnation changes.
///
/// Must see: the incarnation does not change, the bucket has exactly one
/// incarnation, and the `close()` afterwards writes `closed` for that very
/// incarnation. The other direction — a real fail-stop (`NodeWal::fail_stop`)
/// — still abandons the incarnation.
#[tokio::test]
async fn dropping_the_last_node_wals_does_not_fail_stop_a_live_session() -> Result<()> {
    let b = bucket();
    let session = Session::establish(
        b.faulty.clone(),
        &Path::from("cairn"),
        LeaseConfig::new()
            .with_node_id("a")
            .with_ttl(TTL)
            .with_renew_interval(PACE),
    )
    .await?;
    let first = session.current().expect("session just opened");
    let wals = NodeWals::follow(&b.stores, &session, fast());
    drop(wals.get(&first)?);
    let mut live = session.live_changes();
    live.borrow_and_update();

    drop(wals);
    // The old code lost the incarnation within a few ms; the cap is a cap on waiting for an event.
    let changed = tokio::time::timeout(TTL * 2, live.changed()).await;
    assert!(
        changed.is_err() && session.current().as_ref() == Some(&first),
        "dropping NodeWals abandoned the live incarnation {first} — now {:?}",
        session.current()
    );
    let listed: Vec<_> = b
        .faulty
        .list(Some(&Path::from("cairn/sessions/a")))
        .try_collect()
        .await?;
    assert_eq!(
        listed.len(),
        1,
        "an extra incarnation was opened: {:?}",
        listed
            .iter()
            .map(|m| m.location.to_string())
            .collect::<Vec<_>>()
    );

    // The other direction: a real WAL fail-stop still abandons the incarnation.
    let wals = NodeWals::follow(&b.stores, &session, fast());
    let wal = wals.get(&first)?;
    wal.fail_stop(cairn_store::failstop::Cause::Other, "injected fault");
    tokio::time::timeout(TTL * 4, live.changed())
        .await
        .expect("a real fail-stop must abandon the incarnation")?;
    assert_ne!(session.current().as_ref(), Some(&first));
    drop((wal, wals));

    session.close().await;
    let store: Arc<dyn ObjectStore> = b.faulty.clone();
    let (rec, _) = cairn_store::inspect::session_record(&store, &Path::from("cairn"), &first)
        .await?
        .expect("session record");
    assert_eq!(rec?.state, cairn_store::SessionState::Closed);
    Ok(())
}

/// A follower reads the right data of ranges on the shared WAL; GETs go by
/// **log**, not by range.
#[tokio::test]
async fn follower_reads_through_node_tailer() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut writers, mut _keepers) = (Vec::new(), Vec::new());
    for r in 0..4 {
        let (w, k) = acquire(&b, &a, r).await?;
        writers.push(w);
        _keepers.push(k);
    }
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let mut readers = Vec::new();
    for r in 0..4 {
        readers.push(
            b.stores
                .open_reader_with(r, Duration::from_secs(3600), Some(&tailers))
                .await?,
        );
    }
    for (r, w) in writers.iter_mut().enumerate() {
        put(w, 1, format!("v{r}").as_bytes()).await?;
    }
    // An object is exposed only when a later object carries an `acked_upto` covering it (C5).
    for w in writers.iter_mut() {
        put(w, 2, b"push-mark").await?;
    }
    for (r, reader) in readers.iter().enumerate() {
        let want = format!("v{r}");
        assert!(
            eventually(Duration::from_secs(5), || async {
                reader
                    .invocation_status(&id(1))
                    .await
                    .ok()
                    .flatten()
                    .as_deref()
                    == Some(want.as_bytes())
            })
            .await,
            "follower range {r} does not see acked rows"
        );
    }
    assert!(
        tailers.followed_logs() <= 2,
        "{} tailers for 4 ranges on 2 streams",
        tailers.followed_logs()
    );
    Ok(())
}

/// One range continuously writing cap-sized objects ⇒ commits of other ranges
/// on the node wait no more than a few windows.
#[tokio::test]
async fn noisy_range_bounded_latency() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        window: Duration::from_millis(10),
        object_cap: 256 << 10,
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut noisy, _kn) = acquire(&b, &a, 0).await?;
    let (mut quiet, _kq) = acquire(&b, &a, 2).await?;
    let big = vec![7u8; 200 << 10];
    let mut worst = Duration::ZERO;
    for i in 0..20u8 {
        let mut m = Mutations::new();
        m.set_invocation_status(&id(i), &big);
        noisy.commit(m).await?;
        let t = tokio::time::Instant::now();
        put(&mut quiet, i, b"small").await?;
        worst = worst.max(t.elapsed());
    }
    noisy.wait_durable().await?;
    assert!(
        worst < Duration::from_millis(250),
        "a small commit waited {worst:?} behind the noisy range"
    );
    Ok(())
}

// ─────────────────────────────────────────── PUT bound under saturation

/// 16 ranges writing continuously (each range: commit, wait for durability,
/// repeat) for `secs` seconds on one node; returns the PUTs into the WAL (the
/// node log with `shared`, the native WAL with `None`) and the number of
/// durable commits.
async fn saturate(b: &Bucket, wal: Option<&Node>, secs: u64) -> Result<(u64, u64)> {
    let mut writers = Vec::new();
    let mut keepers = Vec::new();
    for r in 0..16u32 {
        match wal {
            Some(n) => {
                let (w, k) = acquire(b, n, r).await?;
                writers.push(w);
                keepers.push(k);
            }
            None => {
                let db = b.stores.open_db(100 + r).await?;
                writers.push(
                    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(100 + r)), db)
                        .await?,
                );
            }
        }
    }
    let before = b.faulty.counts();
    let index_before = index_puts(b);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let commits =
        futures::future::try_join_all(writers.iter_mut().enumerate().map(|(r, w)| async move {
            let mut n = 0u64;
            while tokio::time::Instant::now() < deadline {
                let mut m = Mutations::new();
                m.set_invocation_status(&id((n % 200) as u8), format!("{r}-{n}").as_bytes());
                w.commit(m).await?;
                w.wait_durable().await?;
                n += 1;
            }
            anyhow::Ok(n)
        }))
        .await?
        .into_iter()
        .sum();
    let d = b.faulty.counts().since(&before);
    let class = if wal.is_some() {
        PathClass::NodeWal
    } else {
        PathClass::Wal
    };
    drop(keepers);
    // The log's index is PUT per object count, not per window: the bound counts
    // data PUTs.
    let index = index_puts(b) - index_before;
    Ok((d.get(Op::Put, class) - index, commits))
}

/// Shared-WAL log index PUTs (`<log>/index`) on this store.
fn index_puts(b: &Bucket) -> u64 {
    b.faulty
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, object, _)| *op == Op::Put && *object == "nodewal-index")
        .map(|(.., n)| n)
        .sum()
}

/// **PUT bound:** one node with 16 saturated ranges issues ≤ one WAL PUT per
/// window per stream (`StoreCounts`), against the native WAL issuing per
/// range. The numbers are printed (`--nocapture`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn criterion6_saturated_node_puts_per_window_per_stripe() -> Result<()> {
    let secs = std::env::var("CAIRN_C6_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let b = bucket();
    let cfg = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..NodeWalConfig::default()
    };
    let (window, stripes) = (cfg.window, cfg.stripes as u64);
    let a = node(&b, "a", cfg).await?;
    let (shared_puts, shared_commits) = saturate(&b, Some(&a), secs).await?;
    let (native_puts, native_commits) = saturate(&b, None, secs).await?;
    let windows = Duration::from_secs(secs).as_millis() as u64 / window.as_millis() as u64;
    let bound = windows * stripes;
    println!(
        "PUT bound ({secs} s, window {window:?}, S = {stripes}): shared WAL {shared_puts} PUTs / \
         {shared_commits} durable commits (bound {bound} = {windows} windows × {stripes} streams); \
         native WAL {native_puts} PUTs / {native_commits} durable commits"
    );
    // +S: the last window may be cut short at both ends of the measured span.
    assert!(
        shared_puts <= bound + stripes,
        "{shared_puts} PUTs > {bound} (one PUT per window per stream)"
    );
    Ok(())
}

/// Shared-WAL data PUTs (not counting the index) since `before`.
fn data_puts_since(b: &Bucket, before: &StoreCounts, index_before: u64) -> u64 {
    b.faulty
        .counts()
        .since(before)
        .get(Op::Put, PathClass::NodeWal)
        - (index_puts(b) - index_before)
}

/// **Linger** (`NodeWalConfig::linger`).
///
/// 1. The stream has been idle longer than the window.
/// 2. Commit m1 (without waiting for durability).
/// 3. 20 ms later, commit m2, then wait for durability.
///
/// Must see: L = 0 — m1 is cut at once (one PUT before m2 arrives), m2 goes
/// into its own object: 2 PUTs (the old behaviour, pinned). L = 200 ms — no PUT
/// yet when m2 arrives, m1 and m2 share one object: 1 PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linger_coalesces_commits_arriving_close_after_idle() -> Result<()> {
    for (linger, want) in [(Duration::ZERO, 2), (Duration::from_millis(200), 1)] {
        let b = bucket();
        let cfg = NodeWalConfig {
            stripes: 1,
            linger,
            gc_interval: Duration::from_secs(3600),
            ..fast()
        };
        let n = node(&b, "a", cfg).await?;
        let (mut w, _keeper) = acquire(&b, &n, 0).await?;
        w.wait_durable().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let before = b.faulty.counts();
        let index_before = index_puts(&b);

        let mut m = Mutations::new();
        m.set_invocation_status(&id(1), b"m1");
        w.commit(m).await?;
        if linger.is_zero() {
            // Cut at once: wait (with a cap) until m1's PUT has been issued.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while data_puts_since(&b, &before, index_before) == 0 {
                anyhow::ensure!(tokio::time::Instant::now() < deadline, "m1 was not cut");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !linger.is_zero() {
            assert_eq!(
                data_puts_since(&b, &before, index_before),
                0,
                "linger {linger:?}: m1 was cut before its deadline"
            );
        }
        let mut m = Mutations::new();
        m.set_invocation_status(&id(2), b"m2");
        w.commit(m).await?;
        w.wait_durable().await?;
        assert_eq!(
            data_puts_since(&b, &before, index_before),
            want,
            "linger {linger:?}"
        );
    }
    Ok(())
}

/// **The default linger is 1 ms** and it **reduces** PUTs under sparse load.
/// PUTs are counted on a virtual clock (`start_paused`): no wall-clock timing,
/// no dependence on machine load.
///
/// Each burst, with L = 0 and then L = default:
///
/// 1. The stream is idle for 50 ms (≫ the 5 ms window).
/// 2. Commit m1 (without waiting for durability) — the first append of a batch
///    of SDK frames.
/// 3. Yield to the runtime until the stream has cut m1, or up to a cap of
///    1 000 yields (the virtual clock stands still all that time: the test is
///    always ready to run, so the runtime does not advance time by itself).
/// 4. Time advances 0.5 ms — the measured gap between two appends of a batch
///    is 0.2–0.5 ms.
/// 5. Commit m2, wait for durability.
///
/// Must see: L = 0 — m1 is cut at once in step 3, m2 goes into its own object:
/// 2 PUTs per burst (the Nagle shape). L = default — no PUT before m2, m1 and
/// m2 share one object: 1 PUT per burst. And the default is exactly 1 ms.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn default_linger_is_1ms_and_halves_puts_of_a_sparse_burst() -> Result<()> {
    const BURSTS: u64 = 5;
    let default = NodeWalConfig::default().linger;
    assert_eq!(default, Duration::from_millis(1), "default linger");
    for (linger, per_burst) in [(Duration::ZERO, 2), (default, 1)] {
        let b = bucket();
        let cfg = NodeWalConfig {
            stripes: 1,
            linger,
            gc_interval: Duration::from_secs(3600),
            ..fast()
        };
        let n = node(&b, "a", cfg).await?;
        let (mut w, _keeper) = acquire(&b, &n, 0).await?;
        w.wait_durable().await?;
        let before = b.faulty.counts();
        let index_before = index_puts(&b);
        for burst in 0..BURSTS {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let start = data_puts_since(&b, &before, index_before);
            let mut m = Mutations::new();
            m.set_invocation_status(&id(1), format!("m1-{burst}").as_bytes());
            w.commit(m).await?;
            for _ in 0..1_000 {
                if data_puts_since(&b, &before, index_before) > start {
                    break;
                }
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_micros(500)).await;
            let early = data_puts_since(&b, &before, index_before) - start;
            if linger.is_zero() {
                assert_eq!(early, 1, "L = 0, burst {burst}: m1 must be cut at once");
            } else {
                assert_eq!(early, 0, "L = {linger:?}, burst {burst}: m1 cut early");
            }
            let mut m = Mutations::new();
            m.set_invocation_status(&id(2), format!("m2-{burst}").as_bytes());
            w.commit(m).await?;
            w.wait_durable().await?;
        }
        assert_eq!(
            data_puts_since(&b, &before, index_before),
            per_burst * BURSTS,
            "linger {linger:?}: shared-WAL data PUTs over {BURSTS} bursts"
        );
    }
    Ok(())
}

/// **A huge linger does not take the stream down.** `Stripe::run` computes the
/// cut deadline `first row + L`; with L large enough the `Instant` addition
/// overflows and panics in the stream's task — the commit is never durable.
/// `NodeWalConfig::linger` is clamped at `MAX_LINGER` (100 ms, also the cap of
/// `--wal-linger-us`).
///
/// 1. A stream with `linger = Duration::MAX`.
/// 2. Commit, wait for durability (5 s cap).
///
/// Must see: durable, exactly 1 data PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn huge_linger_is_clamped_not_overflowing() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        stripes: 1,
        linger: Duration::MAX,
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let n = node(&b, "a", cfg).await?;
    let (mut w, _keeper) = acquire(&b, &n, 0).await?;
    let before = b.faulty.counts();
    let index_before = index_puts(&b);
    tokio::time::timeout(Duration::from_secs(5), put(&mut w, 1, b"v"))
        .await
        .map_err(|_| anyhow::anyhow!("commit not durable within 5 s with a huge linger"))??;
    assert_eq!(data_puts_since(&b, &before, index_before), 1);
    Ok(())
}

// ─────────────────────────────────────────── commit latency (bench load.ts)

/// `k` committers at once on **one** writer (like the engine: commit inside the
/// lock, wait for durability outside it) for `secs` seconds; returns the
/// commit → durable latency of each commit, sorted.
///
/// `jitter`: each committer pauses a pseudo-random time in `[0, jitter)` before
/// each commit — commits arrive spread out, not all at once right after an ack.
async fn commit_latencies(
    w: PartitionWriter,
    k: usize,
    secs: u64,
    jitter: Duration,
) -> Result<Vec<Duration>> {
    let w = tokio::sync::Mutex::new(w);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let per = futures::future::try_join_all((0..k).map(|c| {
        let w = &w;
        async move {
            let mut out = Vec::new();
            let mut n = 0u64;
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ c as u64;
            while tokio::time::Instant::now() < deadline {
                if !jitter.is_zero() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let us = x % jitter.as_micros() as u64;
                    tokio::time::sleep(Duration::from_micros(us)).await;
                }
                let t = tokio::time::Instant::now();
                let pending = {
                    let mut w = w.lock().await;
                    let mut m = Mutations::new();
                    m.set_invocation_status(&id((c * 16 + (n % 16) as usize) as u8), b"v");
                    w.commit(m).await?;
                    w.durability()?
                };
                if let cairn_store::Durability::Wait(p) = pending {
                    p.durable().await?;
                }
                out.push(t.elapsed());
                n += 1;
            }
            anyhow::Ok(out)
        }
    }))
    .await?;
    let mut all: Vec<Duration> = per.into_iter().flatten().collect();
    all.sort();
    Ok(all)
}

fn pct(xs: &[Duration], p: usize) -> Duration {
    xs[(xs.len() * p / 100).min(xs.len() - 1)]
}

/// The `load.ts` bench (one partition, 8 concurrent turns, `memory://`) showed
/// the shared WAL ~2.5 ms slower per commit than the native WAL. On `memory://`
/// a PUT is almost instant, so commit latency is almost only the wait for the
/// cut: with the native WAL, the wait for the next flush tick. Two load shapes:
///
/// - **all at once** (8 committers commit again as soon as they are acked —
///   like the bench): every commit arrives right after a cut, so both WALs wait
///   almost a whole window; the shared WAL must not be slower than the native
///   WAL by more than 1 ms (before the fix: +2 ms, because the cut deadline
///   counted from the first row and drifted with timer latency);
/// - **spread out** (a random pause before each commit): the average wait is
///   half a window — p50 ≤ half a window + 3 ms (before the fix: 8 ms).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_latency_on_memory_not_worse_than_native() -> Result<()> {
    let secs = std::env::var("CAIRN_LAT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let window = Duration::from_millis(10);
    let b = bucket();
    let a = node(
        &b,
        "a",
        NodeWalConfig {
            window,
            gc_interval: Duration::from_secs(3600),
            frame_age_flush: Duration::from_secs(3600),
            ..NodeWalConfig::default()
        },
    )
    .await?;
    let (w, _k) = acquire(&b, &a, 0).await?;
    let shared = commit_latencies(w, 8, secs, Duration::ZERO).await?;
    let (w, _k1) = acquire(&b, &a, 2).await?;
    let spread = commit_latencies(w, 8, secs, window * 3).await?;
    let stores = b.stores.clone().with_flush_interval(window);
    let db = stores.open_db(50).await?;
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(50)), db).await?;
    let native = commit_latencies(w, 8, secs, Duration::ZERO).await?;
    let (sp, np) = (pct(&shared, 50), pct(&native, 50));
    println!(
        "commit latency, 8 writers, window/flush tick {window:?}: shared WAL p50 {sp:?} p90 {:?} ({} commits); \
         native WAL p50 {np:?} p90 {:?} ({} commits)",
        pct(&shared, 90),
        shared.len(),
        pct(&native, 90),
        native.len()
    );
    let rp = pct(&spread, 50);
    println!(
        "spread out: shared WAL p50 {rp:?} p90 {:?} ({} commits)",
        pct(&spread, 90),
        spread.len()
    );
    assert!(
        sp <= np + Duration::from_millis(1),
        "all at once: shared WAL p50 {sp:?} > native WAL {np:?} + 1 ms"
    );
    assert!(
        rp <= window / 2 + Duration::from_millis(3),
        "spread out: shared WAL p50 {rp:?} > half a window + 3 ms"
    );
    Ok(())
}

// ───────────────────────────────────────── split/merge on the shared WAL

/// Close a range cleanly like the actor's step-down: seal, mark clean, yield.
async fn seal_and_release(w: PartitionWriter, mut k: LeaseKeeper) -> Result<()> {
    w.seal().await?;
    k.writer_sealed().await?;
    k.release().await;
    Ok(())
}

const FULL_SPAN: (PartitionKey, PartitionKey) = (PartitionKey(0), PartitionKey(u64::MAX));

/// A sealed parent on the shared WAL ⇒ it can be cloned, and the child carries
/// the shared-WAL mark and opens through `acquire_shared` with every acked row.
#[tokio::test]
async fn sealed_shared_parent_clones_and_child_opens_shared() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"from-parent").await?;
    seal_and_release(w, k).await?;
    b.stores
        .clone_range(PartitionId(0), PartitionId(5), FULL_SPAN)
        .await?;
    let (child, _kc) = acquire(&b, &a, 5).await?;
    assert_eq!(get(&child, 1).await?.as_deref(), Some(&b"from-parent"[..]));
    Ok(())
}

/// Quiesce: the Db is sealed while the lease is **still held** ⇒
/// `writer_sealed` has already CASed a clean owner record, so a clone works
/// right away while the lease is held. Sealed without `writer_sealed`, the
/// record still has a segment ⇒ conservative: refused.
#[tokio::test]
async fn quiesced_shared_range_clones_while_lease_held() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, mut k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"quiesce").await?;
    w.seal().await?;
    assert!(
        b.stores
            .clone_range(PartitionId(0), PartitionId(5), FULL_SPAN)
            .await
            .is_err(),
        "clone while the owner record still has a wal_segment"
    );
    b.stores
        .discard_range_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(5)),
            &[PartitionId(0)],
            None,
        )
        .await?;
    k.writer_sealed().await?;
    let rec = owner(&b, 0).await?;
    assert!(!rec.released && rec.wal_segments.is_empty(), "{rec:?}");
    b.stores
        .clone_range(PartitionId(0), PartitionId(5), FULL_SPAN)
        .await?;
    let (child, _kc) = acquire(&b, &a, 5).await?;
    assert_eq!(get(&child, 1).await?.as_deref(), Some(&b"quiesce"[..]));
    k.retire().await?;
    Ok(())
}

/// A parent on the shared WAL with frames not yet in L0 ⇒ the clone is refused
/// (acked rows are not silently lost).
#[tokio::test]
async fn open_shared_parent_refuses_clone() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"not-yet-in-l0").await?;
    assert!(
        b.stores
            .clone_range(PartitionId(0), PartitionId(5), FULL_SPAN)
            .await
            .is_err(),
        "cloned a shared-WAL Db with frames not yet in L0"
    );
    Ok(())
}

/// A child of a **native-WAL** parent (an old bucket): the child Db is a clone
/// of the sealed parent, so its WAL is empty; the child's first owner record is
/// on the shared WAL (every new range), `adopt_shared_wal` switches the mark and
/// the Db opens with the shared WAL with every row. Conversely, a native-WAL Db
/// that still has rows in its WAL cannot be switched.
#[tokio::test]
async fn child_of_native_parent_adopts_shared_wal() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let db = b.stores.open_db(0).await?;
    let mut parent =
        PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    put(&mut parent, 1, b"from-parent").await?;
    parent.seal().await?;
    b.stores
        .clone_range(PartitionId(0), PartitionId(5), FULL_SPAN)
        .await?;
    // The node's open: switch the mark first, then open with the shared WAL.
    b.stores.adopt_shared_wal(5).await?;
    let (child, _kc) = acquire(&b, &a, 5).await?;
    assert_eq!(owner(&b, 5).await?.wal_kind, WalKind::Shared);
    assert_eq!(get(&child, 1).await?.as_deref(), Some(&b"from-parent"[..]));

    // A native-WAL Db with rows not yet in L0: the mark cannot be switched.
    let db = b.stores.open_db(7).await?;
    let mut w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(7)), db).await?;
    put(&mut w, 2, b"in-wal").await?;
    assert!(
        b.stores.adopt_shared_wal(7).await.is_err(),
        "switched the WAL-kind mark of a Db that still has rows in its WAL"
    );
    Ok(())
}

// ───────────────────────────────────────────────── orphan logs

/// A node yields cleanly and shuts down: nobody seals its log (the owner
/// records no longer have any segment), so the log's objects used to live
/// forever (a one-hour chaos run left 38 248 objects). Another node's
/// orphan-log scan seals the log of the ended incarnation and GC deletes every
/// data object, keeping only the seal.
#[tokio::test]
async fn orphan_log_of_cleanly_closed_node_is_collected() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    for i in 0..5 {
        put(&mut w, i, b"v").await?;
    }
    let log = a.wal.log_of(0);
    // Count **before** sealing: after `seal` every frame is in L0, so `a`'s own
    // GC (50 ms tick) may delete every object of the log before the node shuts
    // down — under heavy load that does happen (once, while 50 simulation seeds
    // ran in parallel). Then `b`'s scan has nothing left to do, but the verdict
    // below still holds.
    let left = objects(&b, &log).await?;
    assert!(!left.is_empty(), "no object to clean up");
    w.seal().await?;
    let mut k = k;
    k.writer_sealed().await?;
    k.release().await;
    a.session.close().await;
    a.wal.shutdown();

    let bn = node(&b, "b", fast()).await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            // ≤ 1: GC deletes the data, only the seal is left — and now the
            // seal goes too (`dead_log_seal_and_floor_are_removed_when_unreferenced`).
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the orphan log was not cleaned up: {:?} remain",
        objects(&b, &log).await?
    );
    // The range still reads correctly after the old log was cleaned up.
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 4).await?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

/// A dead node's log that no owner record references any more, with every data
/// object GCed ⇒ the floor and the index go too — they used to live forever,
/// one directory per node death. The seal stays together with the `forgotten`
/// mark: the fence against a zombie of that incarnation.
#[tokio::test]
async fn dead_log_seal_and_floor_are_removed_when_unreferenced() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    for i in 0..5 {
        put(&mut w, i, b"v").await?;
    }
    let log = a.wal.log_of(0);
    w.seal().await?;
    let mut k = k;
    k.writer_sealed().await?;
    k.release().await;
    a.session.close().await;
    a.wal.shutdown();

    let bn = node(&b, "b", fast()).await?;
    assert!(
        eventually(Duration::from_secs(10), || async {
            forgotten(&b, &log).await.unwrap()
        })
        .await,
        "a dead log nobody references was not finally removed: {:?}",
        log_dirs(&b, &log.session.node).await?
    );
    let (mut wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 4).await?.as_deref(), Some(&b"v"[..]));
    put(&mut wb, 5, b"w").await?;
    // `cairn-ctl wal floors` says which logs were finally removed: such a log is
    // still listed (seal + mark), so whoever counts "removed logs" (the chaos
    // invariant) counts marks.
    let floors = cairn_store::nodewal::inspect::log_floors(&b.stores).await?;
    let of = |l: &LogId| floors.iter().find(|f| f.log == *l);
    assert!(of(&log).is_some_and(|f| f.forgotten), "{floors:?}");
    assert!(
        of(&bn.wal.log_of(0)).is_some_and(|f| !f.forgotten),
        "{floors:?}"
    );
    Ok(())
}

/// The scan never seals the log of a **live** incarnation.
///
/// Interleaving (bounded events, no `sleep`):
/// 1. Node A takes range 0 and writes: A's log has data objects.
/// 2. Node B comes up; B's shared WAL goes through a `FaultyStore` layer of its
///    own (B's session does not), on which the GET of A's session stops at
///    gate `first`. Only B's dead-log scan reads another incarnation's session
///    through this layer.
/// 3. Wait for B's scan to stop at `first` — it has seen A's log and is about
///    to ask "is A dead yet". Add gate `second` (closed), then open `first`.
/// 4. Wait for a **later** B scan to stop at `second`: the earlier round has
///    finished deciding about A's log.
///
/// Must see: A can still write and does not fail-stop (had the log been sealed,
/// A's next PUT would see the seal and A would fail-stop — NW-2).
#[tokio::test]
async fn orphan_sweep_leaves_live_logs_alone() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for i in 0..3 {
        put(&mut w, i, b"alive").await?;
    }
    let log = a.wal.log_of(0);

    let view = FaultyStore::wrap(b.faulty.clone());
    let first = Gate::closed();
    let a_session = PathMatch::Contains(format!("sessions/a/{}", log.session.inc));
    view.add_rule(Rule::new(
        OpSet::GET,
        a_session.clone(),
        Action::Gate(first.clone()),
    ));
    let b_session = Session::establish(
        b.faulty.clone(),
        &Path::from("cairn"),
        LeaseConfig::new()
            .with_node_id("b")
            .with_ttl(TTL)
            .with_renew_interval(PACE),
    )
    .await?;
    let b_wal = NodeWal::start(
        &ObjectStores::single(StoreLocation::from_store(
            view.clone(),
            "cairn",
            "memory://cairn",
        )),
        b_session.current().expect("session just opened"),
        fast(),
    );

    let wait_blocked = |g: &Gate, what: &'static str| {
        let g = g.clone();
        async move {
            let t0 = std::time::Instant::now();
            while g.blocked() == 0 {
                if t0.elapsed() > Duration::from_secs(10) {
                    bail!("B's scan did not reach {what}");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        }
    };
    wait_blocked(&first, "first gate").await?;
    let second = Gate::closed();
    view.add_rule(Rule::new(
        OpSet::GET,
        a_session,
        Action::Gate(second.clone()),
    ));
    first.open();
    // The later round does not come back to ask about A ⇒ B considers A's log
    // done — that is, sealed it. A's next write tells for sure.
    let swept_again = wait_blocked(&second, "second gate (later scan)").await;

    let wrote = put(&mut w, 9, b"still-alive").await;
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "a live node had its log sealed"
    );
    wrote?;
    swept_again?;
    second.open();
    b_wal.shutdown();
    b_session.close().await;
    Ok(())
}

// ─────────────────────────────────── regressions for counterexamples C1–C7

/// The simulation, seed 12: an ended session is removed from the bucket and a
/// node with the same name restarts ⇒ the new incarnation must **not** reuse
/// the old `inc` — if it did, the new log would write under the old log's
/// name, below its GC floor, and whoever seals the old log (scanning from the
/// floor) would miss acked rows.
#[tokio::test]
async fn restarted_node_never_reuses_incarnation_after_session_gc() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let first = a.session.current().expect("alive");
    a.session.close().await;
    a.wal.shutdown();
    // Like the tracker's GC: delete the ended session's record.
    let path = Path::from(format!("cairn/sessions/{}/{}", first.node, first.inc));
    b.faulty.inner().delete(&path).await?;
    // In reality ≥ `GC_AFTER_TTLS` TTLs pass between the two opens; the test
    // only needs the millisecond clock to have ticked.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let again = node(&b, "a", fast()).await?;
    let second = again.session.current().expect("alive");
    assert!(
        second.inc > first.inc,
        "{first} then {second}: incarnation reused after the session was cleaned up"
    );
    assert_ne!(again.wal.log_of(0), a.wal.log_of(0));
    Ok(())
}

/// The simulation, seed 2: the CAS adding a segment to the owner record is
/// **torn** (it reaches the store, the client gets an error). Treating it as a
/// miss leaves the record pointing at our log while we drop the range: nobody
/// can take the range (the record points at our live session) until we take it
/// back ourselves and fail-stop. Reading the record back resolves it.
#[tokio::test]
async fn torn_owner_cas_during_acquire_is_resolved_by_reread() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let mut k = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 0"),
    };
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-0".into()),
        Action::Trip(Tripwire::new(0, TripAt::Torn)),
    ));
    let db = acquire_shared(&b.stores, &a.wal, &mut k, 0).await?;
    let rec = owner(&b, 0).await?;
    assert_eq!(
        rec.wal_segments.last().map(|s| &s.log),
        Some(&a.wal.log_of(0)),
        "{rec:?}"
    );
    a.wal.forget_db(0);
    db.close().await?;
    Ok(())
}

/// The simulation, seed 48: a range takeover abandoned halfway (after the Db
/// was opened, before it was handed to the engine) left the range "still open"
/// on the stream; before the fix every retake on the same incarnation failed
/// ("old Db not closed yet") and the range never had an owner until the node
/// fail-stopped.
#[tokio::test]
async fn reacquire_after_abandoned_open_on_same_incarnation() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let mut k = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 0"),
    };
    let gate = Gate::closed();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-0".into()),
        Action::Gate(gate.clone()),
    ));
    let abandoned = tokio::time::timeout(
        Duration::from_millis(300),
        acquire_shared(&b.stores, &a.wal, &mut k, 0),
    )
    .await;
    assert!(abandoned.is_err(), "first takeover must be abandoned");
    b.faulty.remove_rule(rule);
    gate.open();
    drop(k);
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"reclaimed").await?;
    assert_eq!(get(&w, 1).await?.as_deref(), Some(&b"reclaimed"[..]));
    assert!(a.wal.fail_stopped().borrow().is_none());
    Ok(())
}

/// **C1 after a dirty yield.** A range whose owner record still points at the
/// log of **this very incarnation** after a dirty yield, and then the same node
/// takes the range back. Before the fix: `take_over` met its own **live** log ⇒
/// the whole node fail-stopped (C1). Now a dirty yield rotates the range's
/// stream **before** writing the owner record: the segment points at a log that
/// has stopped acking, and the retake seals + replays it like another node's
/// log (NW-5).
///
/// 1. Node a holds ranges 0 and 2 (same stream 0), one durable row each.
/// 2. Range 0's keeper drops while the Db is not sealed ⇒ a dirty yield, the
///    `wal_segment` stays.
/// 3. Range 2 writes on.
/// 4. a takes back range 0, then range 2, in the same incarnation.
///
/// Must see: (3) fails without taking the node down — stream 0 has rotated and
/// all its ranges step down; (4) a does not fail-stop, same incarnation; range
/// 0 has its acked row and writes on into the new log; range 2 has its acked
/// row.
#[tokio::test]
async fn range_lost_while_alive_reacquires_from_the_rotated_log() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let inc = a.session.current();
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    let log0 = a.wal.log_of(0);
    assert_eq!(a.wal.log_of(2), log0, "ranges 0 and 2 must share a stream");
    put(&mut w0, 1, b"v").await?;
    put(&mut w2, 2, b"w").await?;
    // 2. A **dirty** yield: the keeper is dropped while the Db is not sealed ⇒ the `wal_segment`
    // stays.
    drop(k0);
    assert!(
        eventually(Duration::from_secs(3), || async {
            owner(&b, 0).await.unwrap().released
        })
        .await
    );
    let rec = owner(&b, 0).await?;
    assert!(
        rec.wal_segments.iter().any(|s| s.log == log0),
        "a dirty yield must keep the segment pointing at the range's log: {rec:?}"
    );
    // 3.
    assert!(
        put(&mut w2, 3, b"after-dirty-yield").await.is_err(),
        "the stream of the dirty-yielded range must have rotated: range 2 on the same stream still writes"
    );
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "the dirty yield took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    drop((w0, w2, k2));
    // 4. The new stream's id claim runs in the background.
    assert!(
        eventually(Duration::from_secs(5), || async { a.wal.log_of(0) != log0 }).await,
        "stream 0 did not rotate"
    );
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    assert_eq!(
        get(&w0, 1).await?.as_deref(),
        Some(&b"v"[..]),
        "the dirty-yielded range lost acked rows"
    );
    put(&mut w0, 4, b"log-new").await?;
    let (w2, _k2) = acquire(&b, &a, 2).await?;
    assert_eq!(
        get(&w2, 2).await?.as_deref(),
        Some(&b"w"[..]),
        "a range on the same stream lost acked rows"
    );
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "retaking the range after a dirty yield took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.session.current(), inc, "the incarnation changed");
    Ok(())
}

/// **C1 still stands** for a record pointing at a live log **without** going
/// through any keeper yield (the keeper is dropped without writing anything —
/// `forget`, like the split coordinator): no rotation, so the node's own retake
/// meets its own live log ⇒ no replay, fail-stop (NW-5).
#[tokio::test]
async fn own_live_log_listed_without_a_release_still_fails_stop() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    let log0 = a.wal.log_of(0);
    k.forget();
    assert!(owner(&b, 0)
        .await?
        .wal_segments
        .iter()
        .any(|s| s.log == log0));
    let mut k2 = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("could not take it back"),
    };
    assert!(acquire_shared(&b.stores, &a.wal, &mut k2, 0).await.is_err());
    assert!(
        a.wal.fail_stopped().borrow().is_some(),
        "no fail-stop when meeting its own live log"
    );
    Ok(())
}

/// **A dirty yield, then another node takes the range.**
///
/// 1. Node a holds ranges 0, 2 (stream 0) and 1 (stream 1), one durable row
///    each.
/// 2. Range 0's keeper drops while the Db is not sealed ⇒ a dirty yield, the
///    segment pointing at the stream-0 log stays.
/// 3. Node b takes range 0: seals a's stream-0 log, replays.
/// 4. a writes into range 2 (stream 0), then range 1 (stream 1).
///
/// Must see: b reads range 0's acked row; a does not fail-stop, same
/// incarnation; range 1 is writable. Before the fix: the write in (4) on stream
/// 0 met b's seal ⇒ a fail-stopped (`wal_fenced`) — one range failing during a
/// yield took down the whole node.
#[tokio::test]
async fn dirty_yield_taken_by_another_node_does_not_fail_stop_the_node() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let inc = a.session.current();
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w1, _k1) = acquire(&b, &a, 1).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    assert_eq!(
        a.wal.log_of(2),
        a.wal.log_of(0),
        "ranges 0 and 2 must share a stream"
    );
    assert_ne!(a.wal.log_of(1), a.wal.log_of(0), "range 1 on other stream");
    put(&mut w0, 1, b"v").await?;
    put(&mut w1, 2, b"x").await?;
    put(&mut w2, 3, b"y").await?;
    // 2.
    drop(k0);
    assert!(
        eventually(Duration::from_secs(3), || async {
            owner(&b, 0).await.unwrap().released
        })
        .await
    );
    // 3.
    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(
        get(&wb, 1).await?.as_deref(),
        Some(&b"v"[..]),
        "b lost acked rows of the dirty-yielded range"
    );
    // 4.
    let _ = put(&mut w2, 4, b"after-seal").await;
    put(&mut w1, 5, b"other-stream").await?;
    assert_eq!(get(&w1, 5).await?.as_deref(), Some(&b"other-stream"[..]));
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "another node taking the dirty-yielded range took down a: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.session.current(), inc, "the incarnation changed");
    Ok(())
}

/// **Form B** (the most common cause of `c1` in chaos runs, 18 of 27): the
/// seal is done — every frame of the range is in L0 — but the clean owner-record
/// CAS misses, and so does the yield.
///
/// 1. Node a holds range 0, writes one durable row, seals the Db.
/// 2. The session's background loop stops (`crash`: as if the retry tick had
///    not come yet), every owner-record PUT of range 0 fails: `writer_sealed`
///    misses, the yield sits in `pending_release` — the record is still a's and
///    still lists a's live log.
/// 3. The store recovers; a reclaims range 0 in the same incarnation.
///
/// Must see: the takeover completes, the row is there, a does not fail-stop.
/// Before the fix: the record still listed a's live log ⇒ `take_over` met C1
/// and fail-stopped — even though no frame needed replaying. Rotating the
/// stream does not cure this case: there is no dirty yield.
#[tokio::test]
async fn self_reclaim_after_a_lost_clean_owner_cas_does_not_fail_stop() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, mut k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    w.seal().await?;
    // 2.
    a.session.crash_handle().crash();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-0".into()),
        Action::Fail { times: None },
    ));
    assert!(
        k.writer_sealed().await.is_err(),
        "the clean record CAS must miss"
    );
    k.release().await;
    b.faulty.remove_rule(rule);
    let rec = owner(&b, 0).await?;
    ensure!(
        !rec.released && !rec.wal_segments.is_empty(),
        "bad setup: the yield must not have reached the bucket: {rec:?}"
    );
    // 3.
    let (grant, mut k2) = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(g, k) => (g, k),
        Claim::Held(_) => bail!("could not take it back"),
    };
    let db = acquire_shared(&b.stores, &a.wal, &mut k2, 0).await;
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "reclaiming after a missed clean CAS took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    let w = PartitionWriter::open(grant, db?).await?;
    assert_eq!(get(&w, 1).await?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

/// **C2.** The sealer reads the log while name #0 is still empty; meanwhile A
/// writes #0, #1, flushes, and GC deletes #0. The `Create` of the seal at #0
/// succeeds on the name just freed — re-reading the floor must see that seal as
/// stale and seal again at #1.
#[tokio::test]
async fn seal_on_gc_freed_name_is_detected() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    // Ranges 0 and 2 share A's stream 0.
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    let log = a.wal.log_of(0);
    let crash = k0.crash_handle();

    // B will read A's #0 — block that GET.
    let get_gate = Gate::closed();
    let first_object = format!(
        "nodewal/{}/{}/{}/{:020}",
        log.session.node, log.session.inc, log.stripe, 0
    );
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains(first_object),
        Action::Gate(get_gate.clone()),
    ));
    crash.crash();
    let bn = node(&b, "b", fast()).await?;
    let take = tokio::spawn({
        let stores = b.stores.clone();
        let wal = bn.wal.clone();
        let session = bn.session.clone();
        async move {
            loop {
                // The grant is `!Send`: drop it at once, do not hold it across an await; the test
                // rebuilds it.
                let won = match session.claim(PartitionId(2)).await? {
                    Claim::Won(_, keeper) => Some(keeper),
                    Claim::Held(_) => None,
                };
                match won {
                    Some(mut keeper) => {
                        let db = acquire_shared(&stores, &wal, &mut keeper, 2).await?;
                        return anyhow::Ok((db, keeper));
                    }
                    None => tokio::time::sleep(PACE).await,
                }
            }
        }
    });
    assert!(eventually(Duration::from_secs(5), || async { get_gate.blocked() >= 1 }).await);

    // A (a zombie that does not know it has lost) writes #0 then #1, flushes range 0, GC deletes
    // #0.
    put(&mut w0, 1, b"drained").await?;
    w0.flush_memtable().await?;
    put(&mut w2, 2, b"acked-o-1").await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&0)
        })
        .await,
        "GC did not delete #0"
    );
    b.faulty.remove_rule(rule);
    get_gate.open();

    let (db, _kb) = take.await??;
    let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(2)), db).await?;
    assert_eq!(
        get(&w, 2).await?.as_deref(),
        Some(&b"acked-o-1"[..]),
        "the seal landed below acked data"
    );
    Ok(())
}

/// **C3′.** A's PUT #0 is still in flight when B seals ⇒ the seal lands **at**
/// that hole, not above it; A's PUT misses and nothing is acked.
#[tokio::test]
async fn seal_lands_on_inflight_gap_not_above_it() -> Result<()> {
    let b = bucket();
    // A's PUT stays in flight longer than B's whole takeover: no timeout, no retry.
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_secs(20),
        put_attempts: 1,
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    let gate = Gate::closed();
    let rule = b
        .faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Gate(gate.clone())));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"in-flight");
    w.commit(m).await?;
    assert!(eventually(Duration::from_secs(2), || async { gate.blocked() == 1 }).await);
    // A's PUT is already behind the gate; B's seal (same name) must go through.
    b.faulty.remove_rule(rule);
    k.crash_handle().crash();

    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    let log = a.wal.log_of(0);
    assert_eq!(
        objects(&b, &log).await?,
        vec![0],
        "the seal is not at the first hole"
    );
    gate.open();
    assert!(
        w.wait_durable().await.is_err(),
        "A's PUT after the seal was acked"
    );
    assert_eq!(get(&wb, 1).await?, None);
    Ok(())
}

/// **C4.** The new owner's segment is in `wal_segment` **before** its first
/// frame: as long as the owner-record CAS has not finished, `acquire_shared`
/// does not return the Db — nobody has a writer to write a frame with.
#[tokio::test]
async fn owner_segment_written_before_first_frame() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let mut k = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 0"),
    };
    let gate = Gate::closed();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-0".into()),
        Action::Gate(gate.clone()),
    ));
    let log = a.wal.log_of(0);
    {
        let acquiring = acquire_shared(&b.stores, &a.wal, &mut k, 0);
        tokio::pin!(acquiring);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut acquiring)
                .await
                .is_err(),
            "the Db was returned before the segment was in the owner record"
        );
        assert!(gate.blocked() >= 1, "takeover skipped the owner CAS");
        assert!(
            objects(&b, &log).await?.is_empty(),
            "a frame existed before the segment was written"
        );
        b.faulty.remove_rule(rule);
        gate.open();
        let db = acquiring.await?;
        a.wal.forget_db(0);
        db.close().await?;
    }
    let rec = owner(&b, 0).await?;
    assert_eq!(
        rec.wal_segments.last().map(|s| &s.log),
        Some(&log),
        "{rec:?}"
    );
    Ok(())
}

/// **C4′.** A release clears `wal_segment` only when the Db is cleanly sealed;
/// a dirty release keeps the segments for the taker to seal and replay.
#[tokio::test]
async fn cooperative_release_flushes_before_clearing_segments() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    // Release without marking clean: the segments stay.
    k.release().await;
    let rec = owner(&b, 0).await?;
    assert!(rec.released && !rec.wal_segments.is_empty(), "{rec:?}");

    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(
        get(&wb, 1).await?.as_deref(),
        Some(&b"v"[..]),
        "a dirty release lost acked rows"
    );
    Ok(())
}

/// **C5.** On a store that may tear objects (`atomic_put = false`), a follower
/// does not expose an object the writer has not acked (no `acked_upto` header
/// covers it yet). An atomic store exposes contiguously from the floor
/// ([`follower_exposes_contiguous_objects_on_an_atomic_store`]).
#[tokio::test]
async fn follower_exposes_only_acked_objects() -> Result<()> {
    let b = torn_bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(10));
    let reader = b
        .stores
        .open_reader_with(0, Duration::from_secs(3600), Some(&tailers))
        .await?;
    put(&mut w, 1, b"one").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        reader.invocation_status(&id(1)).await?,
        None,
        "the follower exposed an object no acked_upto mark covers yet"
    );
    put(&mut w, 2, b"two").await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            reader
                .invocation_status(&id(1))
                .await
                .ok()
                .flatten()
                .as_deref()
                == Some(&b"one"[..])
        })
        .await
    );
    Ok(())
}

/// **NW-10.** On an `atomic_put` store a follower exposes **every data object
/// contiguous from the floor**, without waiting for an `acked_upto` header to
/// cover it: an object it has seen is one whole write, and the sealer
/// (read-after-write consistent, S3-1) sees it too, so the seal can only land in
/// the first hole above.
///
/// 1. The writer writes `one` (object #0), then the log goes **quiet** — no
///    object after it.
/// 2. The follower reads #0.
///
/// Must see: the follower returns `one` within a few poll ticks (10 ms),
/// without waiting for object #1 or a forced flush by frame age (T = 3 600 s
/// here). Formerly a follower exposed only `seq ≤ acked_upto` (C5) ⇒ it never
/// saw `one` in this gap.
#[tokio::test]
async fn follower_exposes_contiguous_objects_on_an_atomic_store() -> Result<()> {
    let b = bucket();
    assert!(b.stores.wal().atomic_put());
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(10));
    let reader = b
        .stores
        .open_reader_with(0, Duration::from_secs(3600), Some(&tailers))
        .await?;
    put(&mut w, 1, b"one").await?;
    assert!(
        eventually(Duration::from_secs(2), || async {
            reader
                .invocation_status(&id(1))
                .await
                .ok()
                .flatten()
                .as_deref()
                == Some(&b"one"[..])
        })
        .await,
        "the follower does not expose the last object of a quiet log on an atomic store"
    );
    assert_eq!(
        reader.unexposed_tail_age(),
        None,
        "the tail is not exposed on an atomic store"
    );
    Ok(())
}

/// **C6.** Taking a range after failover ⇒ the replayed rows are already in L0
/// (manifest stored) **before** the Db is handed to the engine.
#[tokio::test]
async fn takeover_flushes_replayed_rows_before_serving() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    k.crash_handle().crash();
    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    let manifest = slatedb::admin::AdminBuilder::new(b.stores.range_path(0), b.faulty.clone())
        .build()
        .read_manifest(None)
        .await?
        .expect("manifest");
    assert!(
        manifest.replay_after_wal_id() >= 1,
        "replayed rows not yet in L0: {manifest:?}"
    );
    drop(wb);
    Ok(())
}

/// **C7.** Taking a range after failover ⇒ `wal_segment` holds only the new
/// owner's segment **before** serving.
#[tokio::test]
async fn takeover_prunes_old_segments_before_serving() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    k.crash_handle().crash();
    let bn = node(&b, "b", fast()).await?;
    let (_wb, _kb) = acquire(&b, &bn, 0).await?;
    let rec = owner(&b, 0).await?;
    assert_eq!(rec.wal_segments.len(), 1, "{rec:?}");
    assert_eq!(rec.wal_segments[0].log, bn.wal.log_of(0));
    Ok(())
}

// ─────────────────────────────── per-range flush window (D-h)

/// A range sets a 200 ms window (a cheap tier) on a node with a 5 ms window:
/// its successive commits follow the 200 ms grid — fewer PUTs, slower, exactly
/// what that tier sells. Removing the setting ⇒ back to the node's grid.
#[tokio::test]
async fn range_window_spaces_cuts_for_that_range() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 0, b"open").await?;

    a.wal.set_range_window(0, Some(Duration::from_millis(200)));
    let t0 = std::time::Instant::now();
    for i in 1..=6 {
        put(&mut w, i, b"slow").await?;
    }
    let slow = t0.elapsed();
    assert!(
        slow >= Duration::from_millis(800),
        "6 commits on a 200 ms window took {slow:?} — the range's window was not applied"
    );

    a.wal.set_range_window(0, None);
    let t0 = std::time::Instant::now();
    for i in 7..=12 {
        put(&mut w, i, b"fast").await?;
    }
    let fast_took = t0.elapsed();
    assert!(
        fast_took < slow / 2,
        "the range's window was removed yet it is still slow: {fast_took:?} vs {slow:?}"
    );
    assert_eq!(get(&w, 12).await?.as_deref(), Some(&b"fast"[..]));
    Ok(())
}

/// **G-c:** a node's log still has frames of a retired range whose Db D-d has
/// deleted. That range's `refs()` reads a manifest that no longer exists ⇒ an
/// error ⇒ `gc_log` returns `Err` ⇒ the log's floor is stuck forever (a leak,
/// no data loss). A range carrying a tombstone (`retired=1`) without a manifest
/// needs no frame.
#[tokio::test]
async fn gc_floor_advances_past_frames_of_deleted_retired_range() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        window: Duration::from_millis(40),
        frame_age_flush: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    // Ranges 0 and 2 share stream 0: one object holds frames of both (like
    // `gc_keeps_object_until_all_ranges_flushed`).
    let (mut w0, mut k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    put(&mut w0, 9, b"opening").await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"to-be-retired");
    w0.commit(m).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"cold");
    w2.commit(m).await?;
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    let log = a.wal.log_of(0);
    let target = *objects(&b, &log).await?.last().expect("an object exists");

    // Range 0 retires as after a split: seal, clean owner record, tombstone —
    // then D-d deletes its Db. The object remains because range 2 has not flushed.
    w0.seal().await?;
    k0.writer_sealed().await?;
    k0.retire().await?;
    b.stores
        .delete_retired_db(
            cairn_store::proof::DeleteProof::assume_db(PartitionId(0)),
            None,
        )
        .await?;
    assert!(!b.stores.range_db_exists(PartitionId(0)).await?);
    assert!(objects(&b, &log).await?.contains(&target));

    // Range 2 flushes: nobody needs the object any more ⇒ GC must delete it.
    w2.flush_memtable().await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&target)
        })
        .await,
        "the GC floor is stuck at a frame of a retired range whose Db was deleted"
    );
    Ok(())
}

/// **A floor stuck because a retired range still has a Db whose manifest still
/// points at frames in the log** (caught in chaos by the invariant "the shared
/// WAL floor never stands still for more than 10 minutes": the log of a dead
/// incarnation stood still > 600 s, pinned by a tombstoned range that still had
/// a Db).
///
/// In chaos, the pointer was a checkpoint still in the retired Db's manifest:
/// each checkpoint references the WAL span `(replay_after, next_wal_sst_id)` of
/// the moment it was taken, and a retired Db has no writer or GC left to clean
/// up checkpoints. That Db lives until D-d deletes it — that is, until the
/// child ranges stop listing it in `external_dbs`, which may take very long.
/// But nobody replays the WAL of a tombstoned range (D22): children and forks
/// read only SSTs. The test builds the most general condition — a tombstone set
/// while the manifest still points at unflushed frames:
///
/// 1. Ranges 0 and 2 share a stream; one durable commit each ⇒ one object holds
///    frames of both. No range flushes.
/// 2. Range 0 carries a tombstone, its Db **remains**, the writer is dropped
///    (no seal), the node forgets the Db.
/// 3. Range 2 flushes.
///
/// Must see: GC deletes the object — no range needs any frame in it any more.
#[tokio::test]
async fn gc_floor_advances_past_frames_of_retired_range_whose_db_remains() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        window: Duration::from_millis(40),
        frame_age_flush: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    put(&mut w0, 9, b"opening").await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"to-be-retired");
    w0.commit(m).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"cold");
    w2.commit(m).await?;
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    let log = a.wal.log_of(0);
    let target = *objects(&b, &log).await?.last().expect("an object exists");

    k0.retire().await?;
    drop(w0);
    // Like the engine stepping down: the node no longer has the range's Db open.
    a.wal.forget_db(0);
    assert!(b.stores.range_db_exists(PartitionId(0)).await?);

    w2.flush_memtable().await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&target)
        })
        .await,
        "the GC floor is stuck at a frame of a tombstoned range that still has a Db"
    );
    Ok(())
}

/// **A hung reference read must not hang the floor GC round** (every I/O await
/// in a background loop has a cap). `stored_refs` reads the stored manifest
/// through `Admin::referenced_wal_ranges`, and `Admin`'s store retries
/// **forever** in production (`object_store_max_retries: None`): one read that
/// hangs or keeps failing kept `gc_loop` stuck at that log, and every log
/// queued after it on the node was no longer cleaned up.
///
/// 1. Ranges 0 and 2 share a stream (log L0), one object `x` holds frames of
///    both. Range 1 is on the other stream (log L1), one durable commit.
/// 2. Range 0 flushes and the node forgets its Db (GC reads references from
///    the stored manifest).
/// 3. Every manifest GET of range 0 does `action` (hang / fail) — forever.
/// 4. Ranges 2 and 1 flush: no frame is referenced any more.
///
/// Must see (virtual time): L1's object is deleted within the cap — the GC
/// round does not stall at L0; `x` **remains** while range 0's references
/// cannot be read (keep when not provable), `cairn-ctl wal floors` returns
/// within the cap and reports that range 0 pins L0; once the fault is removed
/// `x` is deleted.
async fn gc_survives_stuck_reference_read(action: Action) -> Result<()> {
    let bound = Duration::from_secs(600);
    let b = bucket();
    let cfg = NodeWalConfig {
        window: Duration::from_millis(40),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    let (mut w1, _k1) = acquire(&b, &a, 1).await?;
    let l0 = a.wal.log_of(0);
    let l1 = a.wal.log_of(1);
    ensure!(l0 == a.wal.log_of(2) && l0 != l1, "stream layout changed");
    put(&mut w0, 9, b"opening").await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(1), b"range-0");
    w0.commit(m).await?;
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"range-2");
    w2.commit(m).await?;
    w0.wait_durable().await?;
    w2.wait_durable().await?;
    put(&mut w1, 3, b"range-1").await?;
    let x = *objects(&b, &l0).await?.last().expect("an object on L0");
    let y = *objects(&b, &l1).await?.last().expect("an object on L1");

    w0.flush_memtable().await?;
    drop(w0);
    a.wal.forget_db(0);
    let stuck = b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/ranges/0/manifest".into()),
        action,
    ));
    w2.flush_memtable().await?;
    w1.flush_memtable().await?;

    ensure!(
        eventually(bound, || async {
            !objects(&b, &l1).await.unwrap().contains(&y)
        })
        .await,
        "the GC round stalled at L0 while range 0's reference read hung: L1's object {y} was not cleaned up"
    );
    ensure!(
        objects(&b, &l0).await?.contains(&x),
        "deleted object {x} while range 0's references could not be read (keep when not provable)"
    );
    let floors = tokio::time::timeout(bound, cairn_store::nodewal::inspect::log_floors(&b.stores))
        .await
        .map_err(|_| anyhow::anyhow!("`cairn-ctl wal floors` hung on a hung reference read"))??;
    let pinned = floors
        .iter()
        .find(|l| l.log == l0)
        .and_then(|l| l.pinned_by)
        .map(|(range, _)| range);
    ensure!(
        pinned == Some(0),
        "a failed read must keep (pinned by range 0), saw {pinned:?}"
    );

    b.faulty.remove_rule(stuck);
    ensure!(
        eventually(bound, || async {
            !objects(&b, &l0).await.unwrap().contains(&x)
        })
        .await,
        "the fault is gone yet L0's object {x} is still not cleaned up"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn gc_survives_hung_reference_read() -> Result<()> {
    gc_survives_stuck_reference_read(Action::Hang).await
}

#[tokio::test(start_paused = true)]
async fn gc_survives_failing_reference_read() -> Result<()> {
    gc_survives_stuck_reference_read(Action::Fail { times: None }).await
}

// ───────────────────────────── who pins the floor, paced flushes

/// An unflushed range keeps an object at the floor ⇒ the node's floor report
/// names exactly that range, with a growing pin age; once the range has
/// flushed ⇒ nobody pins any more.
#[tokio::test]
async fn floor_report_names_the_pinning_range() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    // Ranges 0 and 2 share stream 0. Range 2 writes first, so its frame is in
    // the lowest object; range 0 flushes, range 2 does not (T = 1 hour).
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    let (mut w2, _k2) = acquire(&b, &a, 2).await?;
    put(&mut w2, 2, b"cold").await?;
    put(&mut w0, 1, b"hot").await?;
    w0.flush_memtable().await?;
    let log = a.wal.log_of(2);
    assert!(
        eventually(Duration::from_secs(5), || async {
            let r = a.wal.floors();
            r.logs
                .iter()
                .any(|l| l.log == log && l.pinned_by.map(|(range, _)| range) == Some(2))
                && r.age() >= Duration::from_millis(200)
        })
        .await,
        "the floor report does not name range 2: {:?}",
        a.wal.floors()
    );
    let r = a.wal.floors();
    assert_eq!(r.node, "a");
    assert_eq!(
        r.pinned().keys().copied().collect::<Vec<_>>(),
        vec![2],
        "{r:?}"
    );

    w2.flush_memtable().await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            let r = a.wal.floors();
            r.pinned().is_empty() && r.age() == Duration::ZERO
        })
        .await,
        "flushed yet the floor is still pinned: {:?}",
        a.wal.floors()
    );
    // The report of every incarnation in the process (for `/metrics`) has this node.
    assert!(cairn_store::nodewal::floor_reports(Some("a"))
        .iter()
        .any(|r| r.node == "a"));
    Ok(())
}

/// Forced flushes by frame age are **paced** — when eight idle ranges pass the
/// age at once, at most 4 flushes are in flight at a time, the range with the
/// oldest unflushed frame goes first, and a range that already has a flush in
/// flight is not flushed again on top of it. The earlier version did a
/// `tokio::spawn` of one flush for every range past the age on each tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_flush_is_paced_oldest_first() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        frame_age_flush: Duration::from_millis(800),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let mut ws = Vec::new();
    for r in 0..8u32 {
        ws.push(acquire(&b, &a, r).await?);
    }
    // One gate per range on its L0 SST PUT: a flush that reaches the gate stops there.
    let gates: Vec<Gate> = (0..8u32)
        .map(|r| {
            let g = Gate::closed();
            b.faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("cairn/ranges/{r}/compacted/")),
                Action::Gate(g.clone()),
            ));
            g
        })
        .collect();
    // Range r writes at t0 + r × 80 ms: a lower range has an older unflushed frame.
    for (r, (w, _)) in ws.iter_mut().enumerate() {
        put(w, r as u8, b"v").await?;
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let blocked: Vec<usize> = gates.iter().map(Gate::blocked).collect();
    let flushing: Vec<u32> = (0..8u32).filter(|r| blocked[*r as usize] > 0).collect();
    assert_eq!(
        flushing,
        vec![0, 1, 2, 3],
        "flushes standing at a gate, by range: {blocked:?}"
    );
    assert!(
        blocked.iter().all(|n| *n <= 1),
        "overlapping flushes on one range: {blocked:?}"
    );

    // Open the gates: the remaining four ranges are flushed in turn, and the floor is no longer
    // pinned.
    for g in &gates {
        g.open();
    }
    assert!(
        eventually(Duration::from_secs(10), || async {
            a.wal.floors().pinned().is_empty()
        })
        .await,
        "the floor is still pinned after the gates opened: {:?}",
        a.wal.floors()
    );
    for (r, (w, _)) in ws.iter().enumerate() {
        assert_eq!(get(w, r as u8).await?.as_deref(), Some(&b"v"[..]));
    }
    Ok(())
}

/// A **small** scan (`due_timers`, `due_retries`, queues, journal — every scan
/// that is not `bulk`) reads an SST block from the store **once**, then takes
/// it from the block cache.
///
/// SlateDB's `ScanOptions::default()` leaves `cache_blocks: false` (unlike
/// `ReadOptions`), so formerly each background tick of each range was one GET
/// for every block it touched that the cache did not have yet — every block of
/// a range that had just changed owner, and every SST that compaction produced.
/// Measured in `s3_slowdown`: ~160 GET/s onto **one** SST while the cluster had
/// stopped taking load. Under S3-style throttling those very GETs ate the
/// bucket's whole read budget, and a range that lost its leader could no
/// longer reopen.
///
/// Order: node a writes a timer, flushes to an L0 SST, hands over cleanly →
/// node b (its own cold cache — like a new leader in another process) takes
/// the range → one scan to load the blocks → ten more scans. Must see: the ten
/// later scans make no GET onto `compacted/`.
#[tokio::test]
async fn small_scans_read_sst_blocks_once() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut wa, mut ka) = acquire(&b, &a, 0).await?;
    let mut m = Mutations::new();
    m.add_timer(u64::MAX / 2, &id(1), 1, b"x");
    wa.commit(m).await?;
    wa.wait_durable().await?;
    wa.flush_memtable().await?;
    wa.seal().await?;
    ka.writer_sealed().await?;
    ka.release().await;

    // Same bucket, a new block cache: another process.
    let cold = Bucket {
        faulty: b.faulty.clone(),
        stores: ObjectStores::single(StoreLocation::from_store(
            b.faulty.clone(),
            "cairn",
            "memory://cairn",
        )),
    };
    let bn = node(&cold, "b", fast()).await?;
    let (wb, _kb) = acquire(&cold, &bn, 0).await?;
    let first = wb.due_timers(1, usize::MAX).await?;
    assert!(
        first.rows.is_empty() && first.next_due.is_some(),
        "the timer must still be there: {:?}",
        first.next_due
    );
    let before = b.faulty.counts();
    for _ in 0..10 {
        wb.due_timers(1, usize::MAX).await?;
    }
    let reads = b.faulty.counts().since(&before);
    assert_eq!(
        reads.get(Op::Get, PathClass::Compacted),
        0,
        "every small scan GETs the SST block from the store again"
    );
    Ok(())
}

// ───────────────────────────── three shared-WAL bugs found by chaos

/// A short `put_timeout` for the PUT-failure tests: a `stall_max` budget of 2 × 500 ms.
fn put_budget() -> NodeWalConfig {
    NodeWalConfig {
        put_timeout: Duration::from_millis(500),
        put_attempts: 2,
        ..fast()
    }
}

/// **`stripe::put` retried without pausing** (found with `s3_slowdown`).
///
/// Under 503s returned at once (S3-style throttling, no client retry loop),
/// each attempt missed within a few tens of µs. `put_attempts` back-to-back
/// misses ran out in ~0.1 ms and then fail-stopped the **whole** incarnation —
/// every range of the node had to be reclaimed — while the stream's stall
/// budget (`stall_max` = `put_timeout × put_attempts`) was barely used.
///
/// 1. Node a holds range 0.
/// 2. The next three PUTs of node a's log miss at once (more than
///    `put_attempts` = 2).
/// 3. Range 0 writes one commit.
///
/// Must see: the commit is durable, the node does not fail-stop — the fault
/// burst is shorter than the budget.
#[tokio::test]
async fn fast_put_errors_back_off_within_stall_budget() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", put_budget()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    b.faulty.add_rule(Rule::nodewal_put(
        Some("a"),
        Action::Fail { times: Some(3) },
    ));
    put(&mut w, 1, b"past-503-burst").await?;
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "a short PUT fault burst killed the incarnation: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(get(&w, 1).await?.as_deref(), Some(&b"past-503-burst"[..]));
    Ok(())
}

/// **An ambiguous log PUT fail-stops and is never acked** (a deliberate
/// exception to the ambiguous-CAS rule 7 of AGENTS.md).
///
/// 1. Node a holds range 0.
/// 2. The next PUT of node a's log (`Create` at `seq`) **reaches the store**,
///    then the answer says otherwise: object_store retries a 5xx by itself and
///    the retry sees `AlreadyExists` (`Retried`), or an error and then the
///    stream's retry loop meets `AlreadyExists` on the next attempt (`Error`).
///
/// Must see: the commit is **not** acked and the incarnation fail-stops — the
/// stream does not read back to tell "our earlier send" from "already sealed".
/// On a store where objects may tear (MinIO EC, multi-AZ quorum) our read could
/// see the data while the sealer sees nothing and puts its seal on the other
/// half: an ack based on that read is a lost acked write. The price is a node
/// going down needlessly on each ambiguous log PUT — in exchange for no path
/// that acks an object not surely durable. Every other object goes through
/// `cas::put_resolving`; changing this spot changes the approved design.
///
/// This is the behaviour of a store that declares `atomic_put = false`
/// (`--wal-atomic-put false`) — the test runs on [`torn_bucket`]. On an atomic
/// store the stream reads back:
/// [`own_ambiguous_log_put_is_recognized_and_acked`],
/// [`late_landing_log_put_is_recognized_and_acked`].
#[tokio::test]
async fn ambiguous_log_put_fails_stop_without_ack() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let b = torn_bucket();
        let cfg = NodeWalConfig {
            gc_interval: Duration::from_secs(3600),
            ..put_budget()
        };
        let a = node(&b, "a", cfg).await?;
        let (mut w, _k) = acquire(&b, &a, 0).await?;
        b.faulty.add_rule(Rule::nodewal_put(
            Some("a"),
            Action::Ambiguous {
                reply,
                skip: 0,
                times: Some(1),
            },
        ));
        assert!(
            put(&mut w, 1, b"ambiguous").await.is_err(),
            "{reply:?}: commit acked on an ambiguous log PUT"
        );
        let mut fs = a.wal.fail_stopped();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), fs.wait_for(|v| v.is_some()))
                .await
                .is_ok(),
            "{reply:?}: ambiguous log PUT yet the incarnation did not fail-stop"
        );
    }
    Ok(())
}

/// The other side of `fast_put_errors_back_off_within_stall_budget`: fast
/// errors that **last** still fail-stop (NW-2: never ack an object not surely
/// durable), but only after using most of the `stall_max` budget, not after
/// ~0.1 ms.
#[tokio::test]
async fn persistent_fast_put_errors_fail_stop_after_stall_budget() -> Result<()> {
    let b = bucket();
    let cfg = put_budget();
    let stall = cfg.stall_max();
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    b.faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Fail { times: None }));
    let started = tokio::time::Instant::now();
    let res = put(&mut w, 1, b"never-durable").await;
    let took = started.elapsed();
    assert!(res.is_err(), "commit durable on a log that cannot be PUT");
    let mut fs = a.wal.fail_stopped();
    assert!(fs.wait_for(|v| v.is_some()).await.is_ok());
    assert!(
        took >= stall / 2,
        "fail-stop after {took:?}, less than half the budget {stall:?}"
    );
    assert!(took <= stall * 2, "stalled {took:?} > cap {stall:?}");
    Ok(())
}

/// **Dead-log cleanup does not finish within one incarnation's life** (chaos
/// `--s3-profile s3-standard`, a kill every 10 s).
///
/// 1. Four incarnations `d0`…`d3` write, flush the memtable, yield cleanly and
///    die: each leaves a log of ~40 objects that no owner record lists — only
///    the dead-log scan can clean it up. (A dead log that is still listed stays
///    untouched until its range is reclaimed, with no floor GC any more; so the
///    test builds logs that are **not** listed.)
/// 2. A slow store: every GET of those logs' objects takes 15 ms.
/// 3. Incarnation `s` opens. On the first GC tick, `sweep_dead_logs` seals the
///    logs one by one (~600 ms per log)…
/// 4. …and `s` dies after 700 ms — before sealing **every** log, that is,
///    before the GC phase. The next incarnation `s` starts over, because the
///    `swept`/`sealed` books live in RAM.
///
/// Must see: after a few such lives, every data object of the four logs is gone
/// (only the seals remain) — each life leaves **durable** progress on the
/// bucket, not only in RAM.
#[tokio::test]
async fn dead_log_sweep_progresses_across_short_lived_incarnations() -> Result<()> {
    let b = bucket();
    // Node `d` does not GC its own log: the objects must remain until it dies.
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let mut dead = Vec::new();
    for (r, name) in ["d0", "d1", "d2", "d3"].into_iter().enumerate() {
        let n = node(&b, name, lazy.clone()).await?;
        let (mut w, k) = acquire(&b, &n, r as u32).await?;
        for i in 0..40 {
            put(&mut w, i, b"v").await?;
        }
        w.flush_memtable().await?;
        seal_and_release(w, k).await?;
        dead.push((n.wal.log_of(r as u32), n));
    }
    // Stop every background loop before any session ends: no `d` node gets to
    // scan another `d` node's log.
    for (_, n) in &dead {
        n.wal.shutdown();
    }
    for (_, n) in &dead {
        n.session.close().await;
    }
    for (log, _) in &dead {
        assert!(
            objects(&b, log).await?.len() >= 40,
            "log {log} is missing objects"
        );
    }
    b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/nodewal/d".into()),
        Action::Delay(Duration::from_millis(15)),
    ));
    let mut lives = 0;
    loop {
        let mut left = 0;
        for (log, _) in &dead {
            left += objects(&b, log).await?.len().saturating_sub(1);
        }
        if left == 0 {
            break;
        }
        if lives == 6 {
            bail!("after {lives} short lives, {left} data objects remain in dead logs");
        }
        lives += 1;
        let s = node(&b, "s", fast()).await?;
        tokio::time::sleep(Duration::from_millis(700)).await;
        s.wal.shutdown();
        s.session.close().await;
    }
    Ok(())
}

/// `replay_after_wal_id` in the **stored** manifest of range `range`'s Db.
async fn stored_replay_after(b: &Bucket, range: u32) -> Result<u64> {
    let admin = slatedb::admin::AdminBuilder::new(
        b.stores.range_path(range),
        b.faulty.clone() as Arc<dyn ObjectStore>,
    )
    .build();
    admin
        .referenced_wal_ranges()
        .await?
        .into_iter()
        .find_map(|r| match r.0 {
            // Only the current manifest has an `Included` lower bound (fork patch F1).
            std::ops::Bound::Included(s) => Some(s),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("range {range}: no current manifest found"))
}

/// Flush range `range`'s memtable so that the last commit's row **is in an L0
/// SST** while the manifest's `replay_after_wal_id` stops **below** its frame —
/// returns that frame's id.
///
/// SlateDB freezes the memtable at the WAL's `last_flushed_wal_id` **at freeze
/// time** (`flush_batch_writer`), then waits for the memtable's rows to be
/// durable before uploading the SST. A commit that is in the memtable while its
/// frame is not durable yet gets uploaded with the old mark. Under real load
/// that is common: every flush has commits passing between the freeze and the
/// ack. Here the log PUT is held at a gate until the flush has frozen.
async fn flush_leaving_gap(
    b: &Bucket,
    n: &Node,
    w: &mut PartitionWriter,
    range: u32,
    key: u8,
) -> Result<u64> {
    let gate = Gate::closed();
    let rule = b
        .faulty
        .add_rule(Rule::nodewal_put(Some(n.name), Action::Gate(gate.clone())));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(key), b"in-gap");
    w.commit(m).await?;
    assert!(
        eventually(Duration::from_secs(5), || async { gate.blocked() >= 1 }).await,
        "the frame's PUT did not reach the gate"
    );
    let opener = async {
        // A pause so the flush can freeze the memtable (one message to the batch
        // writer). If it is too short the condition below misses — the test
        // fails loudly, never green by mistake.
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.open();
    };
    let (flushed, ()) = tokio::join!(w.flush_memtable(), opener);
    flushed?;
    b.faulty.remove_rule(rule);
    let wal_id = n.wal.last_wal_id(range).expect("range on a stream");
    let replay_after = stored_replay_after(b, range).await?;
    assert!(
        replay_after < wal_id,
        "could not build the gap: replay_after {replay_after} ≥ frame {wal_id}"
    );
    Ok(wal_id)
}

/// The object of `log` holding the range's frame `wal_id` — the last object can hold it.
async fn last_object(b: &Bucket, log: &LogId) -> Result<u64> {
    Ok(*objects(b, log).await?.last().expect("the log has objects"))
}

/// **A range that changes leader faster than the forced-flush period never
/// flushes** (chaos: the invariant "the floor stands still ≤ 600 s" missed at
/// 605 s): the takeover branch.
///
/// 1. Range 0's leader a flushes its memtable with a gap: the row of the last
///    frame F is in L0, `replay_after_wal_id` = F − 1.
/// 2. a dies (no seal). b takes the range: seals a's log, replays from F − 1.
///    F's row has seq ≤ `last_l0_seq`, so it is filtered out: the memtable
///    after replay is **empty**, the flush of the replayed rows (C6) is a
///    no-op, `replay_after_wal_id` stays at F − 1.
/// 3. The range is idle: no flush after that. Forced flushing by frame age
///    counts only the current leader's frames — b has none.
///
/// Must see: a's log is finally removed (only the seal remains) — F is not
/// pinned forever.
#[tokio::test]
async fn takeover_covers_replayed_frames_already_in_l0() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        frame_age_flush: Duration::from_millis(600),
        ..fast()
    };
    let a = node(&b, "a", cfg.clone()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    flush_leaving_gap(&b, &a, &mut w, 0, 2).await?;
    let log = a.wal.log_of(0);
    a.wal.shutdown();
    a.session.close().await;
    drop(w);

    let bn = node(&b, "b", cfg).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 2).await?.as_deref(), Some(&b"in-gap"[..]));
    assert!(
        eventually(Duration::from_secs(5), || async {
            forgotten(&b, &log).await.unwrap()
        })
        .await,
        "the old leader's log still has {:?}: a frame already in L0 is pinned by replay_after = {}",
        objects(&b, &log).await?,
        stored_replay_after(&b, 0).await?
    );
    Ok(())
}

/// The forced-flush-by-age branch of the same bug: the **live** leader of an
/// idle range flushes with a gap (like
/// `takeover_covers_replayed_frames_already_in_l0`, step 1). Frame F is still
/// in the stream's "unflushed" queue, so past age T it is force-flushed — but
/// the memtable is empty, the flush is a no-op, and F pins the log's floor
/// until the range writes again.
///
/// Must see: within a few T, the object holding F is deleted by GC.
#[tokio::test]
async fn forced_flush_covers_frame_already_in_l0() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        frame_age_flush: Duration::from_millis(600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    flush_leaving_gap(&b, &a, &mut w, 0, 2).await?;
    let log = a.wal.log_of(0);
    let target = last_object(&b, &log).await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&target)
        })
        .await,
        "a forced flush by age did not free a frame already in L0: replay_after = {}, floor {:?}",
        stored_replay_after(&b, 0).await?,
        a.wal.floors()
    );
    assert_eq!(get(&w, 2).await?.as_deref(), Some(&b"in-gap"[..]));
    Ok(())
}

/// The checkpoint branch of the same bug: the checkpoint the compactor writes
/// before every manifest update (SlateDB: lives 15 minutes) references the WAL
/// span `(replay_after, next_wal_sst_id)` of the manifest when it was taken.
/// With the shared WAL **nobody** replays from a checkpoint: a takeover replays
/// from the current manifest, a clone demands an empty WAL (`SharedWalAdmin`),
/// a fork takes only what is in L0, a follower writes no checkpoint. Yet the
/// floor GC treated that span as referenced.
///
/// 1. Range 0 has four L0 SSTs; the fourth flush leaves a gap at frame F.
/// 2. The compactor merges L0 and writes a checkpoint of the manifest with the
///    gap.
/// 3. The range writes on and flushes: the current manifest has moved past F.
///
/// Must see: the object holding F is deleted by GC — it does not wait until the
/// checkpoint expires.
#[tokio::test]
async fn compactor_checkpoint_does_not_pin_shared_wal_frames() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for i in 0..3 {
        put(&mut w, i, b"v").await?;
        w.flush_memtable().await?;
    }
    put(&mut w, 3, b"v").await?;
    let gap = flush_leaving_gap(&b, &a, &mut w, 0, 4).await?;
    let log = a.wal.log_of(0);
    let target = last_object(&b, &log).await?;
    let admin = slatedb::admin::AdminBuilder::new(
        b.stores.range_path(0),
        b.faulty.clone() as Arc<dyn ObjectStore>,
    )
    .build();
    assert!(
        eventually(Duration::from_secs(10), || async {
            !admin.list_checkpoints(None).await.unwrap().is_empty()
        })
        .await,
        "the compactor did not merge the four L0 SSTs"
    );
    let pinned_by_checkpoint = admin.referenced_wal_ranges().await?.iter().any(|r| {
        matches!(r.0, std::ops::Bound::Excluded(s) if s < gap)
            && matches!(r.1, std::ops::Bound::Excluded(e) if gap < e)
    });
    assert!(pinned_by_checkpoint, "checkpoint misses frame {gap}");
    put(&mut w, 5, b"v").await?;
    w.flush_memtable().await?;
    assert!(stored_replay_after(&b, 0).await? >= gap);
    assert!(
        eventually(Duration::from_secs(5), || async {
            !objects(&b, &log).await.unwrap().contains(&target)
        })
        .await,
        "the compactor's checkpoint pins frame {gap}: {:?}",
        a.wal.floors()
    );
    Ok(())
}

/// **Sealing does not survive one failed GET** (exposed by `s3_slowdown` after
/// `stripe::put` stopped fail-stopping at once under 503s).
///
/// 1. Node a writes many commits into range 0 and dies: its log is long.
/// 2. A flaky store: the first few GETs onto a's log return I/O errors (503s
///    that skip the client's retry loop).
/// 3. Node b takes range 0: seals a's log, reading each object.
///
/// Must see: the takeover succeeds on the first try, with every row. Formerly
/// one failed GET failed the whole seal, and the actor reopened the engine from
/// scratch after 0.5 s — with a long log and a throttled read budget, there was
/// almost never one full pass of GETs without an error, and the range had no
/// leader (acked invocations stuck).
#[tokio::test]
async fn takeover_seal_rides_out_transient_get_errors() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for i in 0..20 {
        put(&mut w, i, b"v").await?;
    }
    a.wal.shutdown();
    a.session.close().await;
    drop(w);
    b.faulty.add_rule(Rule::new(
        OpSet::GET | OpSet::LIST,
        PathMatch::Contains("cairn/nodewal/a/".into()),
        Action::Fail { times: Some(3) },
    ));
    let bn = node(&b, "b", fast()).await?;
    let claimed = loop {
        match bn.session.claim(PartitionId(0)).await? {
            Claim::Won(grant, keeper) => break (grant, keeper),
            Claim::Held(_) => tokio::time::sleep(PACE).await,
        }
    };
    let (grant, mut keeper) = claimed;
    let db = acquire_shared(&b.stores, &bn.wal, &mut keeper, 0).await?;
    let wb = PartitionWriter::open(grant, db).await?;
    assert_eq!(get(&wb, 19).await?.as_deref(), Some(&b"v"[..]));
    Ok(())
}

/// **Sealing loops forever on an inconsistent store** (a liveness audit: the
/// outer loop of `NodeWal::seal` had no attempt cap).
///
/// 1. Node a writes into range 0 and dies: its log has data, not sealed.
/// 2. An inconsistent store: every PUT onto a's log returns `Precondition`
///    (`Create` takes it as "already there"), while a GET at that spot still
///    sees nothing.
/// 3. Node b takes range 0: seals a's log.
///
/// Must see: the takeover **returns an error** within the cap (the open fails,
/// the leadership loop backs off and retries), and does not loop forever —
/// looping forever holds the lease without opening, and also blocks the GC
/// round waiting on the same seal slot.
#[tokio::test]
async fn seal_gives_up_on_an_inconsistent_store() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for i in 0..3 {
        put(&mut w, i, b"v").await?;
    }
    a.wal.shutdown();
    a.session.close().await;
    drop(w);
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("cairn/nodewal/a/".into()),
        Action::Precondition,
    ));
    let bn = node(&b, "b", fast()).await?;
    let (_grant, mut keeper) = loop {
        match bn.session.claim(PartitionId(0)).await? {
            Claim::Won(grant, keeper) => break (grant, keeper),
            Claim::Held(_) => tokio::time::sleep(PACE).await,
        }
    };
    let r = tokio::time::timeout(
        Duration::from_secs(60),
        acquire_shared(&b.stores, &bn.wal, &mut keeper, 0),
    )
    .await;
    let Ok(r) = r else {
        anyhow::bail!("sealing looped over 60 s on an inconsistent store — no cap");
    };
    anyhow::ensure!(r.is_err(), "an inconsistent store, yet the seal finished");
    Ok(())
}

/// **The scan batch is taken up by logs that only have a seal left** (a
/// 20-minute chaos run: six dead logs stood at their floor for 610 s, "pinned
/// by ?"; the same first 16 logs by name order were scanned again 27 times).
///
/// 1. 20 incarnations `a00`…`a19` write, flush and die (no yield): each one's
///    range owner record still lists its log, so after GC the log has only the
///    seal left and is **not** finally removed — each new incarnation sees it
///    again as an unknown dead log.
/// 2. Incarnation `zz` writes 40 commits, flushes and **yields cleanly**
///    before dying: no owner record lists its log. Its name sorts last. (A
///    dead log that is still listed stays untouched until its range is
///    reclaimed; only unlisted logs can be cleaned up by the scan.)
/// 3. The `s` incarnations live for exactly one GC tick and die.
///
/// Must see: `zz`'s data objects are cleaned up. A batch that counted every
/// log let each `s` life get through only the first 16 logs — exactly the `a`
/// logs, done since the first life (or still listed).
#[tokio::test]
async fn dead_log_sweep_is_not_starved_by_logs_already_swept() -> Result<()> {
    let b = bucket();
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let mut dead = Vec::new();
    let mut writers = Vec::new();
    let names: Vec<&'static str> = (0..20)
        .map(|i| &*Box::leak(format!("a{i:02}").into_boxed_str()))
        .chain(["zz"])
        .collect();
    for (r, name) in names.iter().enumerate() {
        let n = node(&b, name, lazy.clone()).await?;
        let (mut w, k) = acquire(&b, &n, r as u32).await?;
        let commits = if *name == "zz" { 40 } else { 1 };
        for i in 0..commits {
            put(&mut w, i, b"v").await?;
        }
        w.flush_memtable().await?;
        dead.push((n.wal.log_of(r as u32), n));
        if *name == "zz" {
            seal_and_release(w, k).await?;
        } else {
            writers.push((w, k));
        }
    }
    for (_, n) in &dead {
        n.wal.shutdown();
    }
    for (_, n) in &dead {
        n.session.close().await;
    }
    drop(writers);
    let zz = dead.last().expect("zz").0.clone();
    assert!(objects(&b, &zz).await?.len() >= 40);
    // One GC tick per life: the first tick at 400 ms, death at 700 ms.
    let one_tick = NodeWalConfig {
        gc_interval: Duration::from_millis(400),
        ..fast()
    };
    for lives in 1..=6 {
        let s = node(&b, "s", one_tick.clone()).await?;
        tokio::time::sleep(Duration::from_millis(700)).await;
        s.wal.shutdown();
        s.session.close().await;
        if objects(&b, &zz).await?.len() <= 1 {
            return Ok(());
        }
        if lives == 6 {
            bail!(
                "after {lives} incarnation lives, log zz still has {} objects: the scan batch only covers logs already done",
                objects(&b, &zz).await?.len()
            );
        }
    }
    Ok(())
}

/// **An incarnation that lives shorter than `gc_interval` never scans dead
/// logs** (a smoke run with `--profile all`: 12 of 88 processes ever ran a scan,
/// dead logs of 500–2 700 objects stood at floor 0 for over 600 s without being
/// pinned).
///
/// 1. Incarnation `d` writes 40 commits, yields cleanly and dies: its log still
///    has ~40 objects that no owner record lists — only the dead-log scan can
///    clean it up.
/// 2. Incarnation `s` opens with the **default** GC tick (`gc_interval` 30 s),
///    lives 2 s and dies; repeat up to three lives. That is the shape of chaos:
///    `kill -9` every 10 s on three nodes, each process living ~20–30 s.
///
/// Must see: the data objects of log `d` are cleaned up within those three
/// lives. Before the fix, `gc_loop` slept a full `gc_interval` before its first
/// round: no life ever reached the scan, and each later life started counting
/// from 0 again.
#[tokio::test]
async fn dead_log_sweep_runs_within_a_life_shorter_than_gc_interval() -> Result<()> {
    let b = bucket();
    let default_pace = NodeWalConfig {
        gc_interval: NodeWalConfig::default().gc_interval,
        ..fast()
    };
    assert!(default_pace.gc_interval >= Duration::from_secs(10));
    let d = node(&b, "d", default_pace.clone()).await?;
    let (mut w, k) = acquire(&b, &d, 0).await?;
    for i in 0..40 {
        put(&mut w, i, b"v").await?;
    }
    seal_and_release(w, k).await?;
    let log = d.wal.log_of(0);
    d.wal.shutdown();
    d.session.close().await;
    assert!(
        objects(&b, &log).await?.len() >= 40,
        "log {log} is missing objects"
    );

    for lives in 1..=3 {
        let s = node(&b, "s", default_pace.clone()).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        s.wal.shutdown();
        s.session.close().await;
        if objects(&b, &log).await?.len() <= 1 {
            return Ok(());
        }
        if lives == 3 {
            bail!(
                "after {lives} incarnation lives of 2 s (gc_interval {:?}), the dead log still has {} objects",
                default_pace.gc_interval,
                objects(&b, &log).await?.len()
            );
        }
    }
    Ok(())
}

/// **A seal that fails once leaves the dead log skipped for the incarnation's
/// whole life.**
///
/// 1. Incarnation `d` writes 40 commits, yields cleanly and dies: its log still
///    has ~40 objects that no owner record lists, only the dead-log scan can
///    clean it up.
/// 2. Every GET into `d`'s log fails the first 8 times — exactly the
///    `retry_io` budget of one read while sealing.
/// 3. Incarnation `s` opens (50 ms GC tick): the first scan seals `d`'s log,
///    runs out of its 8 attempts and the seal fails. Then the store heals.
///
/// Must see: within the same incarnation `s`, a later scan seals again and the
/// log is GCed. Before the fix, `NodeWal::sealed` left an empty slot in the
/// `sealed` map, and the scan treated every log with a slot as "known" — the
/// log stood at its floor until `s` died.
#[tokio::test]
async fn dead_log_sweep_retries_a_failed_seal_in_the_same_incarnation() -> Result<()> {
    let b = bucket();
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let d = node(&b, "d", lazy).await?;
    let (mut w, k) = acquire(&b, &d, 0).await?;
    for i in 0..40 {
        put(&mut w, i, b"v").await?;
    }
    seal_and_release(w, k).await?;
    let log = d.wal.log_of(0);
    d.wal.shutdown();
    d.session.close().await;
    assert!(
        objects(&b, &log).await?.len() >= 40,
        "log {log} is missing objects"
    );

    b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/nodewal/d/".into()),
        Action::Fail { times: Some(8) },
    ));
    let _s = node(&b, "s", fast()).await?;
    assert!(
        eventually(Duration::from_secs(10), || async {
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the dead log still has {} objects after the seal failed once",
        objects(&b, &log).await?.len()
    );
    Ok(())
}

/// One dead incarnation `d` with ~40 objects left in its log of range 0, listed by no
/// owner record — only the dead-log sweep can clean it up.
async fn one_dead_log(b: &Bucket) -> Result<LogId> {
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let d = node(b, "d", lazy).await?;
    let (mut w, k) = acquire(b, &d, 0).await?;
    for i in 0..40 {
        put(&mut w, i, b"v").await?;
    }
    seal_and_release(w, k).await?;
    let log = d.wal.log_of(0);
    d.wal.shutdown();
    d.session.close().await;
    ensure!(
        objects(b, &log).await?.len() >= 40,
        "log {log} is missing objects"
    );
    Ok(log)
}

/// **One failed owner-record read dropped a whole dead-log sweep round** (24 h soak:
/// with ~450 owner records read twice per round and 0.1 % of GETs failing, only ~40 % of
/// rounds got through both snapshots; a failed round waited a fixed `gc_interval`, and
/// incarnations living 10–30 s rarely purged anything — dead logs stood unpurged for up to
/// 73 minutes).
///
/// 1. `d` dies, leaving a log no owner record lists.
/// 2. The first GET of an owner record fails once.
/// 3. `s` opens with a one-hour `gc_interval`: its only sweep within the test is the first
///    one, 1 s after start.
///
/// Must see: the dead log purged within that first round — the snapshot reads the record
/// again instead of failing the round.
#[tokio::test]
async fn dead_log_sweep_survives_one_failed_owner_read() -> Result<()> {
    let b = bucket();
    let log = one_dead_log(&b).await?;
    b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/owners/".into()),
        Action::Fail { times: Some(1) },
    ));
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let _s = node(&b, "s", lazy).await?;
    ensure!(
        eventually(Duration::from_secs(10), || async {
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the dead log still has {} objects after one failed owner read",
        objects(&b, &log).await?.len()
    );
    Ok(())
}

/// **A failed sweep round waited a whole `gc_interval` before the next one** (same soak:
/// an incarnation living 10–30 s never reached a second round after a failed first one).
///
/// 1. `d` dies, leaving a log no owner record lists.
/// 2. The LIST of `owners/` fails 12 times in a row — more than one round's own retries.
/// 3. `s` opens with a one-hour `gc_interval`.
///
/// Must see: the dead log purged within 30 s — after a failed round the sweep comes back
/// within seconds (paced, capped at `gc_interval`), not after an hour.
#[tokio::test]
async fn dead_log_sweep_retries_soon_after_a_failed_round() -> Result<()> {
    let b = bucket();
    let log = one_dead_log(&b).await?;
    b.faulty.add_rule(Rule::new(
        OpSet::LIST,
        PathMatch::Contains("cairn/owners".into()),
        Action::Fail { times: Some(12) },
    ));
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let _s = node(&b, "s", lazy).await?;
    ensure!(
        eventually(Duration::from_secs(30), || async {
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the dead log still has {} objects 30 s after a failed sweep round",
        objects(&b, &log).await?.len()
    );
    Ok(())
}

/// Builds `n` incarnations `<prefix>00`, `<prefix>01`… that died **cleanly**:
/// each takes two ranges (one range per stream, from `first_range`), writes one
/// commit, seals, yields and closes its session. No owner record lists any of
/// their logs any more, but each log still has data objects — only the
/// dead-log scan can clean them up.
async fn dead_backlog(b: &Bucket, prefix: &str, n: u32, first_range: u32) -> Result<()> {
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    for i in 0..n {
        let name: &'static str = Box::leak(format!("{prefix}{i:02}").into_boxed_str());
        let x = node(b, name, lazy.clone()).await?;
        for r in [first_range + 2 * i, first_range + 2 * i + 1] {
            let (mut w, k) = acquire(b, &x, r).await?;
            put(&mut w, 1, b"v").await?;
            seal_and_release(w, k).await?;
        }
        x.session.close().await;
        x.wal.shutdown();
    }
    Ok(())
}

/// Every object (data, seal, floor) under `cairn/nodewal/<prefix>`.
async fn nodewal_objects(b: &Bucket, prefix: &str) -> Result<usize> {
    // A `list` prefix follows path segments: list all of `nodewal/`, then filter.
    let dir = Path::from("cairn/nodewal");
    let listed: Vec<_> = b.faulty.inner().list(Some(&dir)).try_collect().await?;
    Ok(listed
        .iter()
        .filter(|m| {
            m.location
                .as_ref()
                .starts_with(&format!("cairn/nodewal/{prefix}"))
        })
        .count())
}

/// A **finally removed** log: the `forgotten` mark is present, no floor or index
/// is left, and at most one numbered object remains — the seal, left behind as
/// the fence against zombies (`LogStore::forget`).
fn forgotten_dir(names: &[String]) -> bool {
    names.iter().any(|n| n == "forgotten")
        && names
            .iter()
            .filter(|n| *n != "forgotten")
            .all(|n| n.parse::<u64>().is_ok())
        && names.iter().filter(|n| n.parse::<u64>().is_ok()).count() <= 1
}

/// The names (relative to the log directory) of every object under each log
/// directory of `cairn/nodewal/<prefix>…`, by log.
async fn log_dirs(
    b: &Bucket,
    prefix: &str,
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let dir = Path::from("cairn/nodewal");
    let listed: Vec<_> = b.faulty.inner().list(Some(&dir)).try_collect().await?;
    let mut out: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for m in listed {
        let p = m.location.as_ref();
        let Some(rest) = p.strip_prefix("cairn/nodewal/") else {
            continue;
        };
        if !rest.starts_with(prefix) {
            continue;
        }
        let parts: Vec<&str> = rest.splitn(4, '/').collect();
        if parts.len() < 4 {
            continue;
        }
        out.entry(parts[..3].join("/"))
            .or_default()
            .push(parts[3].to_string());
    }
    Ok(out)
}

/// Every log under `cairn/nodewal/<prefix>…` is finally removed (see [`forgotten_dir`]).
async fn all_forgotten(b: &Bucket, prefix: &str) -> Result<bool> {
    let dirs = log_dirs(b, prefix).await?;
    Ok(!dirs.is_empty() && dirs.values().all(|names| forgotten_dir(names)))
}

/// `log` is finally removed (see [`forgotten_dir`]).
async fn forgotten(b: &Bucket, log: &LogId) -> Result<bool> {
    let key = format!("{}/{}/{}", log.session.node, log.session.inc, log.stripe);
    Ok(log_dirs(b, &log.session.node)
        .await?
        .get(&key)
        .is_some_and(|names| forgotten_dir(names)))
}

/// **A backlog of dead logs is never finally removed when each life is shorter
/// than one scan** (a soak run: at the stop there were 255 logs with only a
/// seal left that no owner record listed; each process re-sealed 48 logs on
/// average and then died).
///
/// 1. 20 incarnations `a00`…`a19` die cleanly, each leaving two logs with data
///    that no owner record lists (40 logs).
/// 2. Every GET into those logs is 30 ms slow (a store under load): one scan
///    over all 40 logs takes more than a second.
/// 3. The `s` incarnations live 700 ms and die, one after another — the shape
///    of chaos killing every 10 s when a scan is longer than a process's life.
///
/// Must see: after at most 15 lives, every object of the 40 logs (data, seal,
/// floor) is gone. Before the fix, seal + floor were finally removed only
/// **once, at the end** of `sweep_dead_logs`: no life finished a scan, so logs
/// already GCed stayed behind with only a seal, and each new life spent time
/// re-sealing them.
#[tokio::test]
async fn dead_log_backlog_is_forgotten_across_lives_shorter_than_a_sweep() -> Result<()> {
    let b = bucket();
    dead_backlog(&b, "a", 20, 0).await?;
    assert!(nodewal_objects(&b, "a").await? >= 40);
    b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/nodewal/a".into()),
        Action::Delay(Duration::from_millis(30)),
    ));
    for lives in 1..=15 {
        let s = node(&b, "s", fast()).await?;
        tokio::time::sleep(Duration::from_millis(700)).await;
        s.wal.shutdown();
        s.session.close().await;
        if all_forgotten(&b, "a").await? {
            return Ok(());
        }
        if lives == 15 {
            let left = log_dirs(&b, "a").await?;
            let undone = left.values().filter(|n| !forgotten_dir(n)).count();
            bail!("after {lives} incarnation lives of 700 ms, {undone}/40 dead logs are not finally removed");
        }
    }
    Ok(())
}

/// **A finally removed log is re-sealed in every life** (a 4-hour soak on
/// SeaweedFS: 1 422 logs were "finally removed" 5 928 times, 4 536 times with
/// only the seal just written again; after more than two hours the scan could
/// no longer reach the logs with data within a node's ~48 s life, and 27 dead
/// logs stood at their floor for over 600 s). SeaweedFS keeps empty
/// directories in a LIST with a delimiter.
///
/// 1. Incarnation `a00` dies cleanly, leaving two logs no owner record lists.
/// 2. Node `b` scans and finally removes them: no object is left under `a00`'s
///    log directories, but the store still lists those directories
///    (`keep_empty_dirs`). `b` shuts down.
/// 3. Close a gate on every PUT into `nodewal/a…`.
/// 4. Incarnation `d00` dies cleanly like `a00` (its log still has data — a
///    sign the scan has run), then node `c` (a new life that knows no log)
///    comes up and scans.
///
/// Must see: `c` cleans up `d00`'s log, and meanwhile and for at least 1.5 s
/// after, no PUT request goes into `a00`'s log directories (the gate holds
/// nothing). A directory without objects is not a log that needs sealing.
/// Before the fix: the scan listed directories with a delimited LIST, re-sealed
/// (PUT a seal at 0), then finally removed — once per life, for every log that
/// had ever died.
#[tokio::test]
async fn forgotten_dead_log_is_not_resealed_on_a_store_that_keeps_empty_dirs() -> Result<()> {
    let b = bucket();
    b.faulty.keep_empty_dirs();
    dead_backlog(&b, "a", 1, 0).await?;
    assert!(nodewal_objects(&b, "a").await? >= 2);
    let bn = node(&b, "b", fast()).await?;
    assert!(
        eventually(Duration::from_secs(10), || async {
            all_forgotten(&b, "a").await.unwrap()
        })
        .await,
        "bad setup: a00's dead log was not finally removed"
    );
    let dirs = b
        .faulty
        .list_with_delimiter(Some(&Path::from("cairn/nodewal")))
        .await?;
    assert!(
        dirs.common_prefixes
            .iter()
            .any(|p| p.as_ref().starts_with("cairn/nodewal/a")),
        "bad setup: the store must still list a00's empty directory"
    );
    bn.wal.shutdown();
    bn.session.close().await;

    let gate = Gate::closed();
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("cairn/nodewal/a".into()),
        Action::Gate(gate.clone()),
    ));
    dead_backlog(&b, "d", 1, 2).await?;
    assert!(nodewal_objects(&b, "d").await? >= 2);
    let c = node(&b, "c", fast()).await?;
    let start = tokio::time::Instant::now();
    let mut swept_at = None;
    while start.elapsed() < Duration::from_secs(15) {
        if gate.blocked() > 0 {
            gate.open();
            bail!("the scan PUT into the directory of a finally removed log: it re-sealed a log with no objects left");
        }
        match swept_at {
            None if all_forgotten(&b, "d").await? => swept_at = Some(tokio::time::Instant::now()),
            Some(t) if t.elapsed() > Duration::from_millis(1500) => break,
            _ => {}
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gate.open();
    anyhow::ensure!(
        swept_at.is_some(),
        "c did not clean up d00's dead log within 15 s"
    );
    c.wal.shutdown();
    c.session.close().await;
    Ok(())
}

/// **Finally removing a shared-WAL log by mistake** (the NW-7 bug class: GC
/// trusts a wrong fact and deletes something still needed). `LogStore::forget`
/// moves the data, floor and index of a dead log that no owner record lists.
///
/// 1. Incarnation `t00` dies cleanly, leaving two logs no owner record lists.
/// 2. Incarnation `s` scans and finally removes them.
///
/// Must see: both logs have only the seal and the `forgotten` mark left, the
/// trash has one `wal-log` entry per log (data, floor, index — a small log fits
/// one entry), and restoring puts them back in place.
#[tokio::test]
async fn dead_log_forgotten_by_mistake_is_restored() -> Result<()> {
    use cairn_store::trash::{Subject, Trash};
    let b = bucket();
    dead_backlog(&b, "t", 1, 0).await?;
    assert!(nodewal_objects(&b, "t").await? > 0);
    let s = node(&b, "s", fast()).await?;
    let gone = eventually(Duration::from_secs(10), || async {
        all_forgotten(&b, "t").await.unwrap()
    })
    .await;
    s.wal.shutdown();
    s.session.close().await;
    if !gone {
        bail!("t00's dead logs are not finally removed after 10 s (the seal stays, see `forgotten_dir`)");
    }
    let bin = Trash::new(b.faulty.clone(), &Path::from("cairn"));
    let mine: Vec<_> = bin
        .entries()
        .await?
        .into_iter()
        .filter(|e| matches!(&e.subject, Some(Subject::WalLog(l)) if l.session.node == "t00"))
        .collect();
    if mine.len() != 2 || mine.iter().any(|e| e.objects == 0) {
        bail!("the trash must have two wal-log entries with objects for t00: {mine:?}");
    }
    let before_restore = nodewal_objects(&b, "t").await?;
    for e in &mine {
        let r = bin.restore(&e.id).await?;
        if !r.conflicts.is_empty() || r.restored == 0 {
            bail!("restore {}: {r:?}", e.id);
        }
    }
    if nodewal_objects(&b, "t").await? <= before_restore {
        bail!("the restore put no object of the log back into nodewal/");
    }
    Ok(())
}

/// **An interrupted final removal leaves the rest behind forever** (the
/// progress of dead-log cleanup must be a pure function of the bucket).
///
/// 1. Incarnation `h00` dies cleanly, leaving two logs no owner record lists.
/// 2. The **first** copy of those logs into the trash fails: the final removal
///    of one log has written the `forgotten` mark (the mark is written before
///    anything moves) and then stops — the shape of a process dying in the
///    middle of `LogStore::forget`.
/// 3. Incarnation `s` lives on, and later scans run on a healed store.
///
/// Must see: both logs reach exactly the "finally removed" shape (only the seal
/// and the mark). Before the fix, the scan skipped every log carrying the mark:
/// the part not yet moved (floor, index — and also data objects) stayed behind
/// forever, in every life.
#[tokio::test]
async fn forget_cut_short_is_finished_by_a_later_round() -> Result<()> {
    let b = bucket();
    dead_backlog(&b, "h", 1, 0).await?;
    assert!(nodewal_objects(&b, "h").await? > 0);
    // The trash copy destination: `<prefix>/trash/<day>/<id>/obj/nodewal/h00/…`.
    b.faulty.add_rule(Rule::new(
        OpSet::COPY,
        PathMatch::Contains("obj/nodewal/h00/".into()),
        Action::FailNth { skip: 0 },
    ));
    let s = node(&b, "s", fast()).await?;
    let done = eventually(Duration::from_secs(10), || async {
        all_forgotten(&b, "h").await.unwrap()
    })
    .await;
    let left = log_dirs(&b, "h").await?;
    s.wal.shutdown();
    s.session.close().await;
    anyhow::ensure!(
        done,
        "an interrupted final removal was not completed: {left:?}"
    );
    Ok(())
}

/// **A dead log GCed while still listed is never finally removed during that
/// incarnation's life** (the `swept` book in RAM).
///
/// 1. Incarnation `d` (lazy GC) takes ranges 0 and 2 — same stream 0, one log
///    —, writes, flushes both memtables and dies without yielding: two owner
///    records list `d`'s log.
/// 2. Node `b` (alive for the whole test) takes range 0: seals `d`'s log,
///    replays, drops `d`'s segment from range 0's record. Range 2 still lists
///    the log. Give `b` time to GC that log (the old code GCed up to the seal —
///    every frame is in L0 —, saw that range 2 still lists it so did not
///    finally remove it, and then wrote the log into `swept`).
/// 3. Node `c` (lazy GC: none of its background loops gets to run) takes range
///    2 — `d`'s log is no longer listed — and shuts down at once.
///
/// Must see: `d`'s log is finally removed (by `b`). Before the fix, `b`'s scan
/// skipped every log in `swept`: the log stayed until `b` died.
#[tokio::test]
async fn dead_log_collected_while_listed_is_forgotten_once_unlisted() -> Result<()> {
    let b = bucket();
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let d = node(&b, "d", lazy.clone()).await?;
    let (mut w0, k0) = acquire(&b, &d, 0).await?;
    let (mut w2, k2) = acquire(&b, &d, 2).await?;
    for i in 0..5 {
        put(&mut w0, i, b"v").await?;
        put(&mut w2, i, b"w").await?;
    }
    w0.flush_memtable().await?;
    w2.flush_memtable().await?;
    let log = d.wal.log_of(0);
    assert_eq!(log, d.wal.log_of(2), "bad setup: ranges must share a log");
    d.wal.shutdown();
    d.session.close().await;
    drop((w0, k0, w2, k2));

    // (2)
    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    assert_eq!(get(&wb, 4).await?.as_deref(), Some(&b"v"[..]));
    // The old code: wait for GC to reach the seal (an observable event). Now
    // there is no floor GC for dead logs — the wait only times out.
    let _ = eventually(Duration::from_secs(2), || async {
        objects(&b, &log).await.unwrap().len() <= 1
    })
    .await;
    assert!(
        owner(&b, 2)
            .await?
            .wal_segments
            .iter()
            .any(|s| s.log == log),
        "bad setup: range 2 must still list d's log"
    );
    // (3)
    let cn = node(&b, "c", lazy).await?;
    let (wc, kc) = acquire(&b, &cn, 2).await?;
    assert_eq!(get(&wc, 4).await?.as_deref(), Some(&b"w"[..]));
    cn.wal.shutdown();
    drop((wc, kc));
    assert!(
        !owner(&b, 2)
            .await?
            .wal_segments
            .iter()
            .any(|s| s.log == log),
        "bad setup: range 2 still lists d's log after c took it"
    );
    let forgotten_now = eventually(Duration::from_secs(5), || async {
        forgotten(&b, &log).await.unwrap()
    })
    .await;
    let left = log_dirs(&b, &log.session.node).await?;
    bn.wal.shutdown();
    bn.session.close().await;
    cn.session.close().await;
    anyhow::ensure!(
        forgotten_now,
        "a dead log nobody lists is not finally removed after 5 s: {left:?}"
    );
    Ok(())
}

/// Incarnation `d` (lazy GC) takes range `range`, writes 40 commits, flushes its
/// memtable and dies **without yielding**: the owner record still lists its
/// log, and the log still has ~40 objects whose frames are all in L0. Returns
/// that log.
async fn dead_owner_of(b: &Bucket, name: &'static str, range: u32) -> Result<LogId> {
    let lazy = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let d = node(b, name, lazy).await?;
    let (mut w, k) = acquire(b, &d, range).await?;
    for i in 0..40 {
        put(&mut w, i, b"v").await?;
    }
    w.flush_memtable().await?;
    let log = d.wal.log_of(range);
    d.wal.shutdown();
    d.session.close().await;
    drop((w, k));
    assert!(
        objects(b, &log).await?.len() >= 40,
        "log {log} is missing objects"
    );
    Ok(log)
}

/// **A log sealed at takeover is GCed only on the `gc_interval` tick** (a soak
/// run: a log sealed at takeover by two nodes, both died before GCing it, the
/// floor stood still for 639 s).
///
/// 1. Incarnation `s` opens with the **default** GC tick (30 s) and passes the
///    early round (1 s) while there is nothing to do yet.
/// 2. Incarnation `d` writes 40 commits, flushes, dies without yielding.
/// 3. `s` takes `d`'s range: seals `d`'s log, replays, flushes, drops the old
///    segment.
///
/// Must see: the data objects of log `d` go within a few seconds. Before the
/// fix, a log sealed at takeover was GCed only in the early round and on the
/// `gc_interval` tick — a process living 20–30 s under chaos that took a range
/// after its first second never GCed that log.
#[tokio::test]
async fn takeover_sealed_log_is_collected_without_waiting_for_gc_interval() -> Result<()> {
    let b = bucket();
    let default_pace = NodeWalConfig {
        gc_interval: NodeWalConfig::default().gc_interval,
        ..fast()
    };
    let s = node(&b, "s", default_pace).await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let log = dead_owner_of(&b, "d", 0).await?;
    let (ws, _ks) = acquire(&b, &s, 0).await?;
    assert_eq!(get(&ws, 39).await?.as_deref(), Some(&b"v"[..]));
    assert!(
        eventually(Duration::from_secs(5), || async {
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the log sealed at takeover still has {} objects after 5 s",
        objects(&b, &log).await?.len()
    );
    Ok(())
}

/// **A log sealed at takeover queues behind the whole dead-log scan** (a soak
/// run: the first scan line appeared 13–25 s after the process opened, and GC
/// of the sealed log ran only after that scan).
///
/// 1. 20 incarnations `a00`…`a19` die cleanly, leaving 40 logs with data; every
///    GET into them is 100 ms slow — the first scan takes several seconds.
/// 2. Incarnation `d` writes 40 commits, flushes, dies without yielding.
/// 3. Incarnation `s` (default GC tick) takes `d`'s range right after opening,
///    before the early round.
///
/// Must see: the data objects of log `d` go within 3 s of `s` opening. Before
/// the fix, the early round ran the dead-log scan **first** and only then GCed
/// the log sealed at takeover.
#[tokio::test]
async fn takeover_sealed_log_is_collected_before_a_long_dead_log_sweep() -> Result<()> {
    let b = bucket();
    dead_backlog(&b, "a", 20, 100).await?;
    let log = dead_owner_of(&b, "d", 0).await?;
    b.faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("cairn/nodewal/a".into()),
        Action::Delay(Duration::from_millis(100)),
    ));
    let default_pace = NodeWalConfig {
        gc_interval: NodeWalConfig::default().gc_interval,
        ..fast()
    };
    let started = tokio::time::Instant::now();
    let s = node(&b, "s", default_pace).await?;
    let (ws, _ks) = acquire(&b, &s, 0).await?;
    assert_eq!(get(&ws, 39).await?.as_deref(), Some(&b"v"[..]));
    let left = Duration::from_secs(3).saturating_sub(started.elapsed());
    assert!(
        eventually(left, || async {
            objects(&b, &log).await.unwrap().len() <= 1
        })
        .await,
        "the log sealed at takeover still has {} objects 3 s after the incarnation opened",
        objects(&b, &log).await?.len()
    );
    Ok(())
}

/// **A tailer nobody follows any more keeps running forever** (a liveness
/// audit): `NodeTailers` never pruned tailers, even after the range's follower
/// reader was evicted.
///
/// 1. Node `a` writes range 0. A follower opens a shared-WAL reader of range 0:
///    one tailer follows `a`'s log.
/// 2. The reader is dropped (the follower evicts a reader nobody asks).
///
/// Must see: the number of running tailers goes back to 0. Before the fix, the
/// tailer GOT the next object every tick, plus a LIST every 10 misses, for the
/// node's whole life.
#[tokio::test]
async fn tailer_of_a_log_nobody_follows_is_retired() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let reader = tailers.reader(0).await?.expect("shared-WAL range 0");
    assert_eq!(tailers.followed_logs(), 1);
    drop(reader);
    assert!(
        eventually(Duration::from_secs(3), || async {
            tailers.followed_logs() == 0
        })
        .await,
        "{} tailers still running after the log's last reader was dropped",
        tailers.followed_logs()
    );
    Ok(())
}

/// **A lagging tailer does not stop when its log disappears** (a liveness
/// audit).
///
/// 1. Node `a` writes range 0; a follower opens a reader and the tailer catches
///    up with `a`'s log.
/// 2. `a` yields cleanly and dies; every object of the log (data, floor) is
///    deleted — as when `LogStore::forget` finally removes a dead log nobody
///    references. The tailer never sees a seal.
/// 3. The reader still exists (it has not refreshed its list of logs yet).
///
/// Must see: the tailer stops. Before the fix it stopped only on meeting a
/// seal: the GET saw `None` forever, one GET per tick, for ever.
#[tokio::test]
async fn tailer_stops_when_its_log_vanishes() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    for i in 0..3 {
        put(&mut w, i, b"v").await?;
    }
    let log = a.wal.log_of(0);
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let _reader = tailers.reader(0).await?.expect("shared-WAL range 0");
    tokio::time::sleep(Duration::from_millis(300)).await;
    seal_and_release(w, k).await?;
    a.session.close().await;
    a.wal.shutdown();
    let dir = Path::from(format!(
        "cairn/nodewal/{}/{}/{}",
        log.session.node, log.session.inc, log.stripe
    ));
    let listed: Vec<_> = b.faulty.inner().list(Some(&dir)).try_collect().await?;
    assert!(!listed.is_empty());
    for m in listed {
        b.faulty.inner().delete(&m.location).await?;
    }
    assert!(
        eventually(Duration::from_secs(3), || async {
            tailers.followed_logs() == 0
        })
        .await,
        "the tailer still runs on log {log}, which has disappeared"
    );
    Ok(())
}

/// `nodewal` GETs of the requests labelled `source` (`cairn_store::io_source`).
fn nodewal_gets(b: &Bucket, source: &str) -> u64 {
    b.faulty
        .counts_by_source()
        .into_iter()
        .filter(|(s, op, class, _)| *s == source && *op == Op::Get && *class == PathClass::NodeWal)
        .map(|(.., n)| n)
        .sum()
}

/// **Taking one range does not read the whole dead log.** A `chaos.ts --cost`
/// run saw reading dead logs at takeover take ~46% of the run's GETs: every
/// taker GOT **every** object from the floor to the hole, even when its range
/// had frames in only part of them — and two surviving nodes taking ranges of
/// the dead node both read the whole log again.
///
/// 1. Node `a` leads ranges 0 and 2 (same stream 0 with `stripes = 2`) and
///    writes 20 durable commits per range, interleaved — ~40 objects, each
///    object holding one range's frame.
/// 2. Wait a few of `a`'s GC ticks (`gc_interval` 50 ms), then `a` dies without
///    flushing anything.
/// 3. Node `b` takes range 0.
///
/// Must see: `b` sees all 20 acked rows of range 0, and the takeover GETs no
/// more than the objects holding range 0's frames plus a few requests (index,
/// hole, seal) — not the whole log. Then `b` takes range 2 and also sees all
/// its rows.
#[tokio::test]
async fn takeover_reads_only_objects_of_its_range() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    const N: u8 = 20;
    for i in 0..N {
        put(&mut w0, i, format!("d0-{i}").as_bytes()).await?;
        put(&mut w2, i, format!("d2-{i}").as_bytes()).await?;
    }
    let log = a.wal.log_of(0);
    assert_eq!(log, a.wal.log_of(2), "ranges 0 and 2 must share a stream");
    let total = objects(&b, &log).await?.len() as u64;
    assert!(total >= u64::from(2 * N), "log has only {total} objects");
    // A few of `a`'s GC ticks (50 ms) — the log has nothing to GC: no frame is
    // in L0 yet (`frame_age_flush` 1 hour).
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w0, k0, w2, k2));

    let bn = node(&b, "b", fast()).await?;
    let before = nodewal_gets(&b, "test.acquire");
    let (w, _k) = cairn_store::io_source::tagged("test.acquire", acquire(&b, &bn, 0)).await?;
    let gets = nodewal_gets(&b, "test.acquire") - before;
    for i in 0..N {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("d0-{i}").as_bytes()),
            "range 0 lost acked row {i}"
        );
    }
    assert!(
        gets <= u64::from(N) + 5,
        "taking range 0 GOT {gets} objects of a {total}-object log (range 0 has frames in {N})"
    );
    let (w, _k) = acquire(&b, &bn, 2).await?;
    for i in 0..N {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("d2-{i}").as_bytes()),
            "range 2 lost acked row {i}"
        );
    }
    Ok(())
}

/// The sealer leaves an index **up to the seal**: another node that later takes
/// another range of the same dead log GETs one index and its own range's
/// objects, without probing again the tail the writer had not written into the
/// index yet.
///
/// 1. `a` (GC tick 1 hour — it never writes an index) writes 20 interleaved
///    commits for ranges 0 and 2 and dies.
/// 2. `b` takes range 0: probes the whole log from the floor (no index), seals,
///    writes the index up to the seal.
/// 3. `c` takes range 2.
///
/// Must see: `c` sees all of range 2's rows and GETs no more than 20 + a few
/// requests.
#[tokio::test]
async fn later_taker_reads_index_left_by_the_sealer() -> Result<()> {
    let b = bucket();
    let slow_gc = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", slow_gc).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    const N: u8 = 20;
    for i in 0..N {
        put(&mut w0, i, format!("d0-{i}").as_bytes()).await?;
        put(&mut w2, i, format!("d2-{i}").as_bytes()).await?;
    }
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w0, k0, w2, k2));

    let bn = node(&b, "b", fast()).await?;
    let (w, _k) = acquire(&b, &bn, 0).await?;
    assert_eq!(
        get(&w, N - 1).await?.as_deref(),
        Some(format!("d0-{}", N - 1).as_bytes())
    );
    let cn = node(&b, "c", fast()).await?;
    let before = nodewal_gets(&b, "test.acquire");
    let (w, _k) = cairn_store::io_source::tagged("test.acquire", acquire(&b, &cn, 2)).await?;
    let gets = nodewal_gets(&b, "test.acquire") - before;
    for i in 0..N {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("d2-{i}").as_bytes()),
            "range 2 lost acked row {i}"
        );
    }
    assert!(
        gets <= u64::from(N) + 5,
        "the later taker GOT {gets} objects — it did not use the index the sealer left"
    );
    Ok(())
}

/// The index is a **leniently read** object: if it is corrupt the sealer probes
/// from the floor as if there were none, and replay is still complete.
#[tokio::test]
async fn garbled_log_index_falls_back_to_reading_from_the_floor() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    for i in 0..10 {
        put(&mut w, i, format!("v{i}").as_bytes()).await?;
    }
    let log = a.wal.log_of(0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w, k));
    let index = Path::from(format!(
        "cairn/nodewal/{}/{}/{}/index",
        log.session.node, log.session.inc, log.stripe
    ));
    assert!(
        b.faulty.inner().head(&index).await.is_ok(),
        "the writer must have written the index after a few GC ticks"
    );
    b.faulty
        .inner()
        .put(&index, Bytes::from_static(b"CFNI junk").into())
        .await?;

    let bn = node(&b, "b", fast()).await?;
    let (w, _k) = acquire(&b, &bn, 0).await?;
    for i in 0..10 {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("v{i}").as_bytes()),
            "lost row {i} with a corrupt index"
        );
    }
    Ok(())
}

/// The taker does not GET objects whose frames of the range are all in L0: a
/// frame with a wal id ≤ the manifest's `replay_after_wal_id` is never
/// replayed, and that mark only grows — reading the manifest before reading
/// the log is enough to skip them.
///
/// 1. `a` leads ranges 0 and 2 (same stream). It writes 20 interleaved commits
///    per range, then flushes range 0's memtable to L0; range 2 does not flush,
///    so it pins the log's floor at the first object — `a`'s GC deletes nothing.
/// 2. `a` writes 5 more commits for range 0, waits a few GC ticks (index), then
///    dies.
/// 3. `b` takes range 0.
///
/// Must see: `b` sees all 25 rows of range 0, and GETs no more than 5 + a few
/// requests.
#[tokio::test]
async fn takeover_skips_objects_already_in_l0() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    for i in 0..20 {
        put(&mut w2, i, b"d2").await?;
        put(&mut w0, i, format!("d0-{i}").as_bytes()).await?;
    }
    w0.flush_memtable().await?;
    for i in 20..25 {
        put(&mut w0, i, format!("d0-{i}").as_bytes()).await?;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w0, k0, w2, k2));

    let bn = node(&b, "b", fast()).await?;
    let before = nodewal_gets(&b, "test.acquire");
    let (w, _k) = cairn_store::io_source::tagged("test.acquire", acquire(&b, &bn, 0)).await?;
    let gets = nodewal_gets(&b, "test.acquire") - before;
    for i in 0..25 {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("d0-{i}").as_bytes()),
            "range 0 lost acked row {i}"
        );
    }
    assert!(
        gets <= 5 + 5,
        "taking range 0 GOT {gets} objects — it read frames already in L0"
    );
    Ok(())
}

/// **A tailer that has caught up backs off its GETs**, and still sees new writes
/// within the cap. Formerly a tailer GOT the next object on every poll tick
/// forever — even on an idle log.
///
/// 1. `a` writes range 0; a follower (20 ms poll) opens a reader and catches up.
/// 2. 1 s with no writes: count the tailer's `nodewal` GETs.
/// 3. `a` writes row 2 and then row 3 (the later object's `acked_upto` mark
///    covers row 2).
///
/// Must see: GETs during the idle 1 s ≤ 15 (without backoff: ~50); row 2 shows
/// up within 1 s.
#[tokio::test]
async fn caught_up_tailer_backs_off_and_still_sees_new_writes() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let reader = b
        .stores
        .open_reader_with(0, Duration::from_secs(3600), Some(&tailers))
        .await?;
    put(&mut w, 1, b"one").await?;
    put(&mut w, 9, b"stamp").await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            reader
                .invocation_status(&id(1))
                .await
                .ok()
                .flatten()
                .is_some()
        })
        .await,
        "the follower did not catch up"
    );
    let before = nodewal_gets(&b, "nodewal.tailer");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = nodewal_gets(&b, "nodewal.tailer") - before;
    put(&mut w, 2, b"two").await?;
    put(&mut w, 3, b"three").await?;
    let t0 = tokio::time::Instant::now();
    assert!(
        eventually(Duration::from_secs(5), || async {
            reader
                .invocation_status(&id(2))
                .await
                .ok()
                .flatten()
                .as_deref()
                == Some(&b"two"[..])
        })
        .await,
        "the follower does not see the new row after the tailer backed off"
    );
    let seen_after = t0.elapsed();
    assert!(
        idle <= 15,
        "a caught-up tailer made {idle} GETs in an idle 1 s (20 ms poll)"
    );
    assert!(
        seen_after <= Duration::from_secs(1),
        "the new row showed up after {seen_after:?} — backed off past the cap"
    );
    Ok(())
}

/// **A `Stream`-mode reader does not poll the manifest on the follower tick.**
/// New writes arrive through the tailer; when GC passes the point the reader
/// needs, the iterator returns `WalTruncated` and the reader loads the manifest
/// **at once** (fork patch F1). Periodic manifest polling is only a safety net
/// — it used to run on the follower tick (500 ms by default, LIST + GET each
/// time), most of an idle reader's GETs.
///
/// 1. `a` writes range 0; a follower opens a shared-WAL reader with a 50 ms
///    tick and catches up.
/// 2. 1 s with no writes: count `manifest` GETs.
/// 3. `a` flushes its memtable; `a`'s GC (50 ms) deletes objects below the
///    floor — the reader hits a truncation. `a` writes on.
///
/// Must see: ≤ 2 manifest GETs in the idle 1 s (before the fix: ~20); the
/// reader sees the row written after the truncation within 2 s.
#[tokio::test]
async fn stream_reader_polls_manifest_slowly_and_still_follows() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let reader = b
        .stores
        .open_reader_with(0, Duration::from_millis(50), Some(&tailers))
        .await?;
    put(&mut w, 1, b"one").await?;
    put(&mut w, 2, b"stamp").await?;
    assert!(
        eventually(Duration::from_secs(5), || async {
            reader
                .invocation_status(&id(1))
                .await
                .ok()
                .flatten()
                .is_some()
        })
        .await,
        "the follower did not catch up"
    );
    let manifest_gets = || b.faulty.counts().get(Op::Get, PathClass::Manifest);
    let before = manifest_gets();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let idle = manifest_gets() - before;
    w.flush_memtable().await?;
    let log = a.wal.log_of(0);
    assert!(
        eventually(Duration::from_secs(5), || async {
            objects(&b, &log).await.unwrap().is_empty()
        })
        .await,
        "GC did not delete objects already in L0"
    );
    put(&mut w, 3, b"three").await?;
    put(&mut w, 4, b"stamp").await?;
    let t0 = tokio::time::Instant::now();
    assert!(
        eventually(Duration::from_secs(5), || async {
            reader
                .invocation_status(&id(3))
                .await
                .ok()
                .flatten()
                .as_deref()
                == Some(&b"three"[..])
        })
        .await,
        "the follower does not see the row written after the truncation"
    );
    let seen_after = t0.elapsed();
    assert!(
        idle <= 2,
        "an idle Stream reader GOT the manifest {idle} times in 1 s"
    );
    assert!(
        seen_after <= Duration::from_secs(2),
        "the row after the truncation showed up after {seen_after:?}"
    );
    Ok(())
}

/// **An incarnation that dies before its first GC tick still leaves an index.**
/// The writer used to write the index only on each GC tick (30 s by default) —
/// under a burst of kills many incarnations die before that tick, and two or
/// three surviving nodes all read its whole log again (a `--cost` run: most of
/// the takeover's remaining GETs). The writer now also writes the index
/// whenever there are enough new acked objects, without waiting for the GC
/// tick: index PUTs stay a small fraction of data PUTs.
///
/// 1. `a` (GC tick 1 hour — the GC tick never comes) leads ranges 0 and 2
///    (same stream), writes 100 interleaved durable commits per range — ~200
///    objects.
/// 2. Wait 1.5 s, then `a` dies.
/// 3. `b` takes range 0.
///
/// Must see: `b` sees all of range 0's rows, and GETs no more than 100 (range
/// 0's objects) + 30 (the tail not yet indexed, the index, the seal) — not the
/// whole log.
#[tokio::test]
async fn young_log_is_indexed_without_waiting_for_gc_interval() -> Result<()> {
    let b = bucket();
    let slow_gc = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", slow_gc).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w2, k2) = acquire(&b, &a, 2).await?;
    const N: u8 = 100;
    for i in 0..N {
        put(&mut w0, i, format!("d0-{i}").as_bytes()).await?;
        put(&mut w2, i, format!("d2-{i}").as_bytes()).await?;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w0, k0, w2, k2));

    let bn = node(&b, "b", fast()).await?;
    let before = nodewal_gets(&b, "test.acquire");
    let (w, _k) = cairn_store::io_source::tagged("test.acquire", acquire(&b, &bn, 0)).await?;
    let gets = nodewal_gets(&b, "test.acquire") - before;
    for i in 0..N {
        assert_eq!(
            get(&w, i).await?.as_deref(),
            Some(format!("d0-{i}").as_bytes()),
            "range 0 lost acked row {i}"
        );
    }
    assert!(
        gets <= u64::from(N) + 30,
        "taking range 0 GOT {gets} objects — a young incarnation's log has no index"
    );
    Ok(())
}

/// **The GC floor must not pass a frame the stored manifest does not cover yet**
/// (NW-7). An old bug, exposed by seed 9 of `shared_wal_sim` (the log index PUT
/// shifted that seed's `LogPut After` trap point).
///
/// SlateDB updates the manifest's `replay_after_wal_id` **in memory**
/// (`apply_uploaded_state`) **before** writing the manifest to the bucket.
/// `Inner::refs` read the in-memory manifest of the open Db, so a GC tick
/// falling into that gap advanced the log's floor and deleted objects the
/// **stored** manifest still needed; if the node died before the manifest
/// reached the bucket, the taker replayed from the old mark while the frames
/// were gone — and acked rows with them.
///
/// 1. `a` leads range 0, writes 5 durable commits.
/// 2. Every manifest PUT of range 0 is blocked at a gate that never opens; `a`
///    flushes its memtable in the background — the SST reaches the bucket, the
///    in-memory manifest advances, the stored manifest stands still.
/// 3. A few of `a`'s GC ticks (50 ms).
/// 4. `a` dies; remove the blocking rule (`a`'s hanging PUT still hangs); `b`
///    takes range 0.
///
/// Must see: `b` sees all 5 rows.
#[tokio::test]
async fn gc_floor_never_passes_frames_the_stored_manifest_does_not_cover() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    for i in 0..5 {
        put(&mut w, i, format!("v{i}").as_bytes()).await?;
    }
    let gate = Gate::closed();
    let rule = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("cairn/ranges/0/manifest/".into()),
        Action::Gate(gate.clone()),
    ));
    // The flush waits for the manifest forever; the other branch waits for it to
    // reach the gate, then lets a few of `a`'s GC ticks run in the "SST uploaded,
    // manifest not yet" gap — then stops waiting for the flush (SlateDB's flush
    // still hangs at the gate).
    let reached = tokio::select! {
        _ = w.flush_memtable() => false,
        reached = async {
            let reached =
                eventually(Duration::from_secs(5), || async { gate.blocked() >= 1 }).await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            reached
        } => reached,
    };
    assert!(reached, "the flush's manifest PUT did not reach the gate");
    a.session.close().await;
    a.wal.shutdown();
    std::mem::forget((w, k));
    b.faulty.remove_rule(rule);

    let bn = node(&b, "b", fast()).await?;
    let (wb, _kb) = acquire(&b, &bn, 0).await?;
    for i in 0..5 {
        assert_eq!(
            get(&wb, i).await?.as_deref(),
            Some(format!("v{i}").as_bytes()),
            "lost acked row {i}: GC deleted a frame the stored manifest does not cover yet"
        );
    }
    Ok(())
}

// ──────────────── measuring and pinning the fail-stop radius

/// Wait (with a cap) for `n` to fail-stop and return its first cause.
async fn fail_stop_cause(n: &Node) -> Option<cairn_store::failstop::Cause> {
    let mut fs = n.wal.fail_stopped();
    // 30 s: a hung PUT ⇒ the stream rotates, then the id claim (a cap of
    // 2 × `stall_max` per operation) runs out before the net — no other node ⇒
    // fail-stop.
    tokio::time::timeout(Duration::from_secs(30), fs.wait_for(|v| v.is_some()))
        .await
        .ok()?
        .ok()?;
    n.wal.fail_stop_cause()
}

/// The path of the next seq of `n`'s stream 0 (the first data object not yet present).
async fn next_object_path(b: &Bucket, n: &Node) -> Result<String> {
    let next = objects(b, &n.wal.log_of(0))
        .await?
        .last()
        .map_or(0, |s| s + 1);
    Ok(object_path(n, next))
}

/// **Every shared-WAL fail-stop path carries the right label**
/// (`cairnflow_failstop_total{cause}`) — the baseline for the fail-stop radius.
///
/// 1. The log PUT reaches the store and then returns an error; the next attempt
///    sees `AlreadyExists` ⇒ `wal_ambiguous`.
/// 2. The log PUT reaches the store, and the answer belongs to object_store's
///    own retry: `AlreadyExists` on the **first attempt** ⇒ `wal_fenced` —
///    indistinguishable from a real seal when the store declares
///    `atomic_put = false` (these two cases run on [`torn_bucket`]; an atomic
///    store reads back).
/// 3. The log PUT hangs past `stall_max` ⇒ the stream rotates, the id claim
///    hangs too, there is no net (`rotation_net`: no other node can take the
///    ranges) ⇒ `wal_stall`.
/// 4. The owner-record CAS misses at takeover ⇒ `owner_cas_lost`.
/// 5. The owner record points at the node's own live log ⇒ `c1`. The keeper is
///    dropped without yielding (`forget`): a dirty yield now rotates the stream
///    before writing the owner record, so it no longer leads here.
///
/// Must see: `NodeWal`'s label is right in all five cases. (A bare `NodeWal`,
/// no `NodeWals`: nobody carries the fail-stop to the `Session` — that part is
/// in [`node_wals_carry_the_fail_stop_cause_to_the_session`].)
#[tokio::test]
async fn fail_stop_cause_names_each_wal_path() -> Result<()> {
    use cairn_store::{failstop::Cause, testkit::Ambiguity};
    let quiet = || NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..put_budget()
    };
    // The two ambiguous-PUT labels remain only on an `atomic_put = false` store:
    // an atomic store resolves them by reading back.
    for (reply, want) in [
        (Ambiguity::Error, Cause::WalAmbiguous),
        (Ambiguity::Retried, Cause::WalFenced),
    ] {
        let b = torn_bucket();
        let a = node(&b, "a", quiet()).await?;
        let (mut w, _k) = acquire(&b, &a, 0).await?;
        b.faulty.add_rule(Rule::nodewal_put(
            Some("a"),
            Action::Ambiguous {
                reply,
                skip: 0,
                times: Some(1),
            },
        ));
        assert!(put(&mut w, 1, b"ambiguous").await.is_err());
        assert_eq!(fail_stop_cause(&a).await, Some(want), "{reply:?}");
    }

    let b = bucket();
    let a = node(&b, "a", quiet()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    b.faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Hang));
    assert!(put(&mut w, 1, b"hang").await.is_err());
    assert_eq!(fail_stop_cause(&a).await, Some(Cause::WalStall));

    let b = bucket();
    let a = node(&b, "a", quiet()).await?;
    let mut k = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("did not get range 0"),
    };
    // Someone else changes the owner record: `Precondition` does not apply, and
    // reading back finds a record that is not this write's ⇒ lost.
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/".into()),
        Action::Precondition,
    ));
    assert!(acquire_shared(&b.stores, &a.wal, &mut k, 0).await.is_err());
    assert_eq!(fail_stop_cause(&a).await, Some(Cause::OwnerCasLost));

    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v").await?;
    k.forget();
    let mut k2 = match a.session.claim(PartitionId(0)).await? {
        Claim::Won(_, k) => k,
        Claim::Held(_) => bail!("could not take it back"),
    };
    assert!(acquire_shared(&b.stores, &a.wal, &mut k2, 0).await.is_err());
    assert_eq!(fail_stop_cause(&a).await, Some(Cause::C1));
    Ok(())
}

/// **Our own ambiguous write is recognised and acked.** An `atomic_put = false`
/// store keeps the old behaviour: [`ambiguous_log_put_fails_stop_without_ack`].
///
/// 1. Node a holds range 0.
/// 2. The next PUT of node a's log (`Create` at `seq`) **reaches the store**,
///    then the answer says otherwise: (a) an error, and the stream's next
///    attempt sees `AlreadyExists`; (b) object_store retries a 5xx by itself,
///    and cairn's **first** attempt already sees `AlreadyExists`.
/// 3. The stream reads the header of the object at `seq` (an `atomic_put`
///    store): it is a data object of this very stripe with exactly the bytes
///    sent ⇒ the outcome is "ours".
///
/// Must see: the commit is durable and readable, the node does **not**
/// fail-stop, the incarnation does not change.
#[tokio::test]
async fn own_ambiguous_log_put_is_recognized_and_acked() -> Result<()> {
    use cairn_store::testkit::Ambiguity;
    for reply in [Ambiguity::Error, Ambiguity::Retried] {
        let b = bucket();
        let cfg = NodeWalConfig {
            gc_interval: Duration::from_secs(3600),
            ..put_budget()
        };
        let a = node(&b, "a", cfg).await?;
        let (mut w, _k) = acquire(&b, &a, 0).await?;
        let inc = a.session.current();
        b.faulty.add_rule(Rule::nodewal_put(
            Some("a"),
            Action::Ambiguous {
                reply,
                skip: 0,
                times: Some(1),
            },
        ));
        put(&mut w, 1, b"ours")
            .await
            .map_err(|e| anyhow::anyhow!("{reply:?}: our own write was not acked: {e}"))?;
        assert_eq!(get(&w, 1).await?.as_deref(), Some(&b"ours"[..]));
        assert!(
            a.wal.fail_stopped().borrow().is_none(),
            "{reply:?}: fail-stop because of our own write: {:?}",
            a.wal.fail_stopped().borrow()
        );
        assert_eq!(a.session.current(), inc, "{reply:?}: incarnation changed");
    }
    Ok(())
}

/// **A PUT that lands late after its attempt was cancelled does not take the
/// node down.**
///
/// 1. Node a holds range 0; a 500 ms cap per attempt.
/// 2. The next PUT of node a's log flies 505 ms before reaching the store —
///    `put_timeout` has cancelled the attempt's future, but the request still
///    arrives (`Action::LandLate`).
/// 3. The stream's next attempt (after a 12.5–25 ms backoff) sends the same
///    `Create`: the object exists ⇒ `AlreadyExists`. Formerly: `wal_ambiguous`,
///    fail-stop.
///
/// Must see: the stream reads back, sees its own object ⇒ ack; the node does
/// not fail-stop. (If the next attempt arrives before the late one, it wins the
/// `Create` and is acked — the same outcome, so the test holds in both orders.)
#[tokio::test]
async fn late_landing_log_put_is_recognized_and_acked() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        gc_interval: Duration::from_secs(3600),
        ..put_budget()
    };
    let late = cfg.put_timeout + Duration::from_millis(5);
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let inc = a.session.current();
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(next_object_path(&b, &a).await?),
        Action::EachOnce(Box::new(Action::LandLate(late))),
    ));
    put(&mut w, 1, b"arrives-late")
        .await
        .map_err(|e| anyhow::anyhow!("our own late-landing PUT was not acked: {e}"))?;
    assert_eq!(b.faulty.landed_late(), 1, "late rule did not fire");
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "fail-stop because of our own late-landing PUT: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.session.current(), inc);
    Ok(())
}

/// **A late-landing log PUT racing a seal at the same `seq` ⇒ `Fenced`, no
/// ack** (the "someone else's" outcome of the read-back).
///
/// Interleaving:
/// 1. Node a holds range 0 and writes one durable row; a freezes (stops
///    renewing, validity pinned — a still believes it is leader).
/// 2. a commits again. The log PUT at the next `seq` flies 3 s before reaching
///    the store (`LandLate`, only a's first send; a 6 s cap per attempt, so a
///    keeps waiting).
/// 3. Meanwhile a's session expires; node b takes range 0 and seals a's log:
///    the first hole is `seq` ⇒ b's seal is at `seq`.
/// 4. a's PUT arrives: the `Create` meets the seal ⇒ `AlreadyExists`. a's
///    stream reads back the object at `seq` (an `atomic_put` store): a seal,
///    not our bytes.
///
/// Must see: a's commit is **not** acked, a fail-stops with the label
/// `wal_fenced` (a real seal), the object at `seq` is the seal, b does not see
/// the second row. An ack here would be a lost write: b has replayed and served
/// without it.
#[tokio::test]
async fn late_log_put_racing_a_seal_at_the_same_seq_is_fenced() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_secs(6),
        put_attempts: 2,
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut wa, ka) = acquire(&b, &a, 0).await?;
    put(&mut wa, 1, b"before").await?;
    let log = a.wal.log_of(0);
    let seq = objects(&b, &log).await?.last().map_or(0, |s| s + 1);
    // (1)
    ka.crash_handle().crash();
    // (2)
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(object_path(&a, seq)),
        Action::EachOnce(Box::new(Action::LandLate(Duration::from_secs(3)))),
    ));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"after-seal");
    wa.commit(m).await?;
    let (acked, taken) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(20), wa.wait_durable()),
        async {
            // (3)
            let bn = node(&b, "b", fast()).await?;
            let (wb, kb) = acquire(&b, &bn, 0).await?;
            // a's PUT is still in flight: the only thing at `seq` is b's seal.
            let at_seq = objects(&b, &log).await?.last().copied();
            anyhow::Ok((bn, wb, kb, at_seq))
        }
    );
    let (_bn, wb, _kb, at_seq) = taken?;
    assert_eq!(
        at_seq,
        Some(seq),
        "bad setup: b must seal at seq {seq} before a's PUT arrives"
    );
    assert_eq!(get(&wb, 1).await?.as_deref(), Some(&b"before"[..]));
    // (4)
    assert!(
        matches!(acked, Ok(Err(_))),
        "a's PUT was acked (or hung) although b's seal is at seq {seq}: {:?}",
        acked.map(|r| r.map(|_| ()))
    );
    assert_eq!(b.faulty.landed_late(), 1, "late rule did not fire");
    assert_eq!(
        fail_stop_cause(&a).await,
        Some(cairn_store::failstop::Cause::WalFenced)
    );
    assert_eq!(
        objects(&b, &log).await?.last().copied(),
        Some(seq),
        "the seal's slot must be the seq of the late-landing PUT"
    );
    assert_eq!(get(&wb, 2).await?, None, "b sees the post-seal PUT row");
    Ok(())
}

/// **A hung PUT takes down only its stripe, not the node** (the same shape as
/// [`hung_put_bounds_stall_then_rotates_its_stripe`]).
///
/// 1. Node a holds range 0 (stripe 0) and range 1 (stripe 1).
/// 2. Every log PUT of stripe 0 hangs.
/// 3. Range 0 commits: never acked (NW-2), an error within the cap.
/// 4. After `stall_max`, stripe 0 rotates to a new stripe id in the same
///    incarnation.
/// 5. The store recovers; range 0 yields and node a reclaims it.
///
/// Must see: the node does not fail-stop, the incarnation does not change,
/// range 1 (another stripe) still acks new commits after stripe 0 failed;
/// reclaiming range 0 replays the old (sealed) log — the rows acked before the
/// hang are still there — and then writes into the new log.
#[tokio::test]
async fn hung_put_fences_only_its_stripe() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_millis(200),
        put_attempts: 2,
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let stall = cfg.stall_max();
    let a = node(&b, "a", cfg).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    let (mut w1, _k1) = acquire(&b, &a, 1).await?;
    put(&mut w0, 0, b"before-hang").await?;
    assert_ne!(
        a.wal.log_of(0),
        a.wal.log_of(1),
        "the two ranges must be on two stripes"
    );
    let inc = a.session.current();
    let log0 = a.wal.log_of(0);
    b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(format!(
            "nodewal/{}/{}/{}/",
            log0.session.node, log0.session.inc, log0.stripe
        )),
        Action::Hang,
    ));
    let res = tokio::time::timeout(stall * 3, put(&mut w0, 1, b"hang")).await;
    assert!(
        matches!(res, Ok(Err(_))),
        "a commit on a hung PUT must fail within the cap, got {res:?}"
    );
    tokio::time::sleep(stall).await;
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "a hung PUT on one stripe took down the whole node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(a.session.current(), inc, "the incarnation changed");
    put(&mut w1, 2, b"other-stripe").await?;
    assert_eq!(get(&w1, 2).await?.as_deref(), Some(&b"other-stripe"[..]));
    // (5) The store recovers; range 0 yields (dirty: the segment pointing at the
    // old log stays) and node a itself reclaims it on the new stripe. The old
    // log of this very incarnation has stopped acking, so the takeover seals and
    // replays it like another node's log (not C1): the rows acked before the
    // hang are still there.
    b.faulty.clear_rules();
    drop((w0, k0));
    assert!(
        eventually(Duration::from_secs(5), || async {
            owner(&b, 0).await.unwrap().released
        })
        .await,
        "range 0 did not yield"
    );
    let (mut w0, _k0) = acquire(&b, &a, 0).await?;
    assert_eq!(
        get(&w0, 0).await?.as_deref(),
        Some(&b"before-hang"[..]),
        "lost acked rows of the rotated log"
    );
    put(&mut w0, 3, b"after-rotate").await?;
    assert_ne!(a.wal.log_of(0), log0, "range 0 still writes the old log");
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "reclaiming the range on a rotated log took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    // A seal lies above the acked object; the hinted round may already have
    // finally removed the old log (the owner record no longer lists it) — the
    // seal stays, the data objects go into the trash.
    let left = objects(&b, &log0).await?;
    assert!(
        left.len() >= 2 || (left.len() == 1 && forgotten(&b, &log0).await?),
        "the old log must have a seal above the acked object: {left:?}"
    );
    Ok(())
}

/// **The log of a rotated stream is finally removed while the incarnation is
/// alive** (a 4-hour soak: two rotated logs of node 0 — 728 and 709 objects,
/// sealed, listed by no owner record — stood for 1 677 s until the end of the
/// run because their incarnation did not die).
///
/// A rotated log has no floor GC, and the final removal used to demand the
/// incarnation's death (`DeleteProof::dead_log`), so the log stayed for the
/// node's whole life — every rotation leaked the whole old log, on a node alive
/// for weeks.
///
/// 1. Node a holds range 0 (stripe 0), writes one durable row.
/// 2. Every PUT onto a's stripe-0 log hangs ⇒ the next commit fails, stripe 0
///    rotates.
/// 3. The store recovers; range 0 yields dirty, and a itself reclaims it on the
///    new stream — the takeover replays the old (sealed) log and then drops the
///    old segment from the owner record.
///
/// Must see: the owner record no longer lists the old log; within 5 s the old
/// log carries the `forgotten` mark (the seal stays) while a's session is still
/// alive and a does not fail-stop; the rows acked before the hang are still
/// readable.
#[tokio::test]
async fn rotated_log_is_forgotten_while_its_incarnation_lives() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        put_timeout: Duration::from_millis(200),
        put_attempts: 2,
        ..fast()
    };
    let stall = cfg.stall_max();
    let a = node(&b, "a", cfg).await?;
    let (mut w0, k0) = acquire(&b, &a, 0).await?;
    put(&mut w0, 0, b"before-hang").await?;
    let inc = a.session.current();
    let log0 = a.wal.log_of(0);
    let hang = b.faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains(format!(
            "nodewal/{}/{}/{}/",
            log0.session.node, log0.session.inc, log0.stripe
        )),
        Action::Hang,
    ));
    let res = tokio::time::timeout(stall * 3, put(&mut w0, 1, b"hang")).await;
    assert!(
        matches!(res, Ok(Err(_))),
        "a commit on a hung PUT must fail within the cap, got {res:?}"
    );
    assert!(
        eventually(Duration::from_secs(5), || async { a.wal.log_of(0) != log0 }).await,
        "range 0's stripe did not rotate"
    );
    assert!(b.faulty.remove_rule(hang));
    drop((w0, k0));
    assert!(
        eventually(Duration::from_secs(5), || async {
            owner(&b, 0).await.unwrap().released
        })
        .await,
        "range 0 did not yield"
    );
    let (w0, _k0) = acquire(&b, &a, 0).await?;
    assert_eq!(
        get(&w0, 0).await?.as_deref(),
        Some(&b"before-hang"[..]),
        "lost acked rows of the rotated log"
    );
    assert!(
        eventually(Duration::from_secs(5), || async {
            owner(&b, 0)
                .await
                .unwrap()
                .wal_segments
                .iter()
                .all(|s| s.log != log0)
        })
        .await,
        "the owner record still lists the rotated log"
    );
    assert!(
        eventually(Duration::from_secs(5), || async {
            forgotten(&b, &log0).await.unwrap()
        })
        .await,
        "the rotated log of a live incarnation was not finally removed: {:?}",
        objects(&b, &log0).await?
    );
    assert_eq!(a.session.current(), inc, "the incarnation changed");
    assert!(
        a.wal.fail_stopped().borrow().is_none(),
        "removing the rotated log took down the node: {:?}",
        a.wal.fail_stopped().borrow()
    );
    assert_eq!(get(&w0, 0).await?.as_deref(), Some(&b"before-hang"[..]));
    Ok(())
}

/// **The `NodeWals` watcher task carries the WAL's cause to the `Session`**, and
/// `lose` counts exactly once under that label.
///
/// 1. Session a, `NodeWals` builds the WAL of the first incarnation.
/// 2. The WAL fail-stops with cause `c1`, then once more with `wal_stall`.
///
/// Must see: the incarnation changes; `cairnflow_failstop_total{cause="c1"}`
/// rises, and the second label does not replace the first. (In this binary only
/// this test carries `c1` to the `Session`: the other tests use a bare
/// `NodeWal`.)
#[tokio::test]
async fn node_wals_carry_the_fail_stop_cause_to_the_session() -> Result<()> {
    use cairn_store::failstop::{count, Cause};
    let b = bucket();
    let session = Session::establish(
        b.faulty.clone(),
        &Path::from("cairn"),
        LeaseConfig::new()
            .with_node_id("a")
            .with_ttl(TTL)
            .with_renew_interval(PACE),
    )
    .await?;
    let first = session.current().expect("session just opened");
    let wals = NodeWals::follow(&b.stores, &session, fast());
    let wal = wals.get(&first)?;
    let mut live = session.live_changes();
    live.borrow_and_update();
    let before = count(Cause::C1);
    wal.fail_stop(Cause::C1, "injected fault");
    wal.fail_stop(Cause::WalStall, "injected fault, second time");
    assert_eq!(wal.fail_stop_cause(), Some(Cause::C1), "first label lost");
    tokio::time::timeout(TTL * 4, live.changed())
        .await
        .expect("a fail-stop must abandon the incarnation")?;
    assert_ne!(session.current().as_ref(), Some(&first));
    assert!(
        eventually(Duration::from_secs(2), || async {
            count(Cause::C1) > before
        })
        .await,
        "abandoning the incarnation was not counted under c1"
    );
    drop((wal, wals));
    session.close().await;
    Ok(())
}

/// **G19 — SlateDB's `Remote` filter is right with the shared WAL** (the
/// drainer's IR-1 tripwire, `docs/reliability/irreversible.md` §0.3).
/// `PartitionWriter::get_durable` reads with `DurabilityLevel::Remote` — its
/// mark is SlateDB's `last_remote_persisted_seq`, which the shared WAL pushes
/// through `SharedWriterInit`. The assumption: that mark **is** the
/// `durable_seq` the writer uses to issue tickets.
///
/// Order: one durable write; close the node log's PUT gate; one more write
/// (submitted, not durable); open the gate, wait for durability.
///
/// Must see: the durable write is visible at the durable level; the not yet
/// durable write is visible at the `Memory` level but **not** at the durable
/// level, and `durable_seq` is below its seq; once durable it is visible at
/// both levels.
#[tokio::test]
async fn remote_read_sees_exactly_the_durable_prefix_with_a_shared_wal() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"durable").await?;
    let key = |n| keys::invocation_status(&id(n));
    ensure!(
        w.get_durable(&key(1)).await?.as_deref() == Some(&b"durable"[..]),
        "a durable write is not visible at the durable level"
    );
    let gate = Gate::closed();
    b.faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Gate(gate.clone())));
    let mut m = Mutations::new();
    m.set_invocation_status(&id(2), b"not-yet");
    w.commit(m).await?;
    let t0 = std::time::Instant::now();
    while gate.blocked() == 0 {
        ensure!(
            t0.elapsed() < Duration::from_secs(5),
            "the write did not reach the log PUT gate"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    ensure!(
        get(&w, 2).await?.is_some(),
        "a write just submitted must be visible at the Memory level"
    );
    ensure!(
        w.get_durable(&key(2)).await?.is_none(),
        "the Remote filter sees a write still standing at the log PUT gate — its mark is past durable_seq"
    );
    ensure!(
        w.latest_send_seq().is_none() && !w.caught_up(),
        "the writer must know there is a write not yet durable"
    );
    gate.open();
    w.wait_durable().await?;
    ensure!(
        w.get_durable(&key(2)).await?.as_deref() == Some(&b"not-yet"[..]),
        "a durable write the Remote filter does not see yet — its mark lags behind durable_seq"
    );
    Ok(())
}

// ───────────────────── G25: shadowed versions at memtable flush (SlateDB #2208)
//
// Since 0.18, a memtable flush with the WAL enabled drops shadowed versions with
// the retention mark `max(durable_seq, last_seq)` instead of `durable_seq`
// (`flush.rs` `compute_flush_retention_seq`). Upstream proves it for the native
// WAL and the native `DbReader`: L0 is exposed only after the WAL is durable up
// to `last_seq` (#1255), and the reader raises its durable mark to
// `last_l0_seq`. The three tests below pin that for cairn's shared WAL and
// followers (`docs/reliability/irreversible.md` §0.3 G25). They passed both
// before and after the upgrade past #2208.

/// The stored manifest of range `range` (not through the Db's RAM).
async fn stored_manifest(b: &Bucket, range: u32) -> Result<slatedb::VersionedManifest> {
    slatedb::admin::AdminBuilder::new(b.stores.range_path(range), b.faulty.clone())
        .build()
        .read_manifest(None)
        .await?
        .ok_or_else(|| anyhow::anyhow!("range {range}: no manifest yet"))
}

/// **G25, case (a) — L0 is exposed only after the shared WAL is durable up to
/// the memtable's `last_seq`.** The Db's durable mark on the shared WAL is the
/// `last_flushed_seq` that `Stripe::flush` reports through `WalFlushed`; the
/// manifest writer publishes only L0s with `last_seq ≤` that mark.
///
/// 1. Durably write `v0` into a key; close the node's log PUT gate.
/// 2. Overwrite the key twice (`v1`, `v2`): both stand at the gate, not durable.
/// 3. Flush the memtable, wait 300 ms with the gate still closed.
/// 4. Open the gate, wait for the flush.
///
/// Must see: in step 3 the flush has not returned and the stored manifest is
/// unchanged (`last_l0_seq`, L0 count); after step 4 the flush returns, the
/// manifest has a new L0 with a new `last_l0_seq`, and the key reads `v2`.
#[tokio::test]
async fn g25a_l0_is_published_only_after_the_shared_wal_is_durable() -> Result<()> {
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"v0").await?;
    let before = stored_manifest(&b, 0).await?;
    let gate = Gate::closed();
    let rule = b
        .faulty
        .add_rule(Rule::nodewal_put(Some("a"), Action::Gate(gate.clone())));
    for value in [&b"v1"[..], b"v2"] {
        let mut m = Mutations::new();
        m.set_invocation_status(&id(1), value);
        w.commit(m).await?;
    }
    ensure!(
        eventually(Duration::from_secs(5), || async { gate.blocked() >= 1 }).await,
        "the log PUT did not reach the gate"
    );
    let flush = w.flush_memtable();
    tokio::pin!(flush);
    ensure!(
        tokio::time::timeout(Duration::from_millis(300), &mut flush)
            .await
            .is_err(),
        "the memtable flush returned while the shared WAL was not durable"
    );
    let gated = stored_manifest(&b, 0).await?;
    ensure!(
        gated.last_l0_seq() == before.last_l0_seq() && gated.l0().len() == before.l0().len(),
        "the manifest published L0 before the shared WAL was durable: last_l0_seq {} → {}, L0 {} → {}",
        before.last_l0_seq(),
        gated.last_l0_seq(),
        before.l0().len(),
        gated.l0().len()
    );
    gate.open();
    tokio::time::timeout(Duration::from_secs(10), flush)
        .await
        .map_err(|_| anyhow::anyhow!("the flush did not return after the gate opened"))??;
    b.faulty.remove_rule(rule);
    let after = stored_manifest(&b, 0).await?;
    ensure!(
        after.last_l0_seq() > before.last_l0_seq() && after.l0().len() > before.l0().len(),
        "the flush did not publish L0: last_l0_seq {} → {}",
        before.last_l0_seq(),
        after.last_l0_seq()
    );
    ensure!(get(&w, 1).await?.as_deref() == Some(&b"v2"[..]));
    Ok(())
}

/// **G25, case (b) — a follower never sees a key go back or disappear when the
/// key is overwritten across many flushes.** Each L0 keeps only the key's last
/// version; a follower whose durable mark is still below that L0's `last_seq`
/// would filter out the last version and have no older one left to read —
/// unless it raises its mark to `last_l0_seq` when loading the manifest.
///
/// The leader overwrites a key in bursts: each burst is three back-to-back
/// commits (no durability wait between them), then a memtable flush — even
/// bursts wait for durability before flushing, odd bursts flush at once (the
/// SST upload starts while the WAL is not yet durable up to `last_seq`). The
/// follower (manifest 20 ms, tailer 20 ms) reads the key continuously
/// meanwhile.
///
/// Must see: once the follower has seen the key, it never reads `None` or a
/// number smaller than one it has seen; in the end it sees the last number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g25b_follower_never_sees_an_overwritten_key_go_back_or_vanish() -> Result<()> {
    const ROUNDS: u64 = 20;
    const LAST: u64 = ROUNDS * 3;
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    let tailers = NodeTailers::new(&b.stores, Duration::from_millis(20));
    let reader = b
        .stores
        .open_reader_with(0, Duration::from_millis(20), Some(&tailers))
        .await?;
    let leader = async {
        for round in 0..ROUNDS {
            for i in 1..=3 {
                let mut m = Mutations::new();
                m.set_invocation_status(&id(1), format!("{:06}", round * 3 + i).as_bytes());
                w.commit(m).await?;
            }
            if round % 2 == 0 {
                w.wait_durable().await?;
            }
            w.flush_memtable().await?;
        }
        // An object is exposed to the follower only when a later object carries an `acked_upto`
        // covering it (C5).
        put(&mut w, 2, b"push-mark").await?;
        Ok::<_, anyhow::Error>(())
    };
    let follower = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut seen: Option<u64> = None;
        let mut reads = 0u64;
        loop {
            // A read error (a manifest refresh half done) is not a vanished key: read again.
            if let Ok(got) = reader.invocation_status(&id(1)).await {
                reads += 1;
                let got = match got {
                    Some(v) => Some(std::str::from_utf8(&v)?.parse::<u64>()?),
                    None => None,
                };
                match (seen, got) {
                    (Some(s), None) => bail!("the follower saw the key vanish after {s}"),
                    (Some(s), Some(n)) if n < s => bail!("follower: key went back {s} → {n}"),
                    (_, Some(n)) => seen = Some(n),
                    (None, None) => {}
                }
                if seen == Some(LAST) {
                    return Ok::<_, anyhow::Error>(reads);
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "the follower did not reach the last number {LAST}: at {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let ((), reads) = tokio::try_join!(leader, follower)?;
    ensure!(
        reads > ROUNDS,
        "the follower read too few times ({reads}) for the test to mean anything"
    );
    Ok(())
}

/// **G25, case (c) — an open snapshot keeps an old version the memtable
/// shadows.** The query layer's read path (`range_read.rs`) reads through
/// `Db::snapshot`; a flush must keep every version an open snapshot still
/// needs, even when that version is shadowed in the same memtable.
///
/// Durably write `old`; open a snapshot; durably write `new` over the same key
/// (same memtable); flush the memtable.
///
/// Must see: a new L0 is stored; the old snapshot reads `old`, a new snapshot
/// reads `new`.
#[tokio::test]
async fn g25c_open_snapshot_keeps_a_version_its_memtable_shadows() -> Result<()> {
    use cairn_store::{
        query_index::{QueryCounts, QUERY_INDEXES},
        range_read::RangeReadHandle,
        RangeEntry,
    };
    let b = bucket();
    let a = node(&b, "a", fast()).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    put(&mut w, 1, b"old").await?;
    let before = stored_manifest(&b, 0).await?.last_l0_seq();
    let handle = RangeReadHandle::new(
        w.store().clone(),
        LeadershipGrant::single_node(PartitionId(0)).validity_handle(),
        RangeEntry::new(0, u64::MAX, PartitionId(0)),
        Arc::new(QueryCounts::default()),
        QUERY_INDEXES,
    );
    let old = handle.snapshot()?;
    put(&mut w, 1, b"new").await?;
    w.flush_memtable().await?;
    ensure!(
        stored_manifest(&b, 0).await?.last_l0_seq() > before,
        "the flush did not publish L0"
    );
    let key = keys::invocation_status(&id(1));
    ensure!(
        old.get(&key).await?.as_deref() == Some(&b"old"[..]),
        "the old snapshot lost a version it still needs after the flush"
    );
    ensure!(handle.snapshot()?.get(&key).await?.as_deref() == Some(&b"new"[..]));
    Ok(())
}

/// **A forced flush by frame age does not chase its own fence** (found when
/// `backup.rs::backup_and_restore_keep_segmented_values` waited 60 s for a
/// flush, and as a flush to L0 taking over 20 s).
///
/// Interleaving (reproduces ~1 in 40 runs of the backup test under 64 parallel
/// copies):
/// 1. A write arrives when the range's oldest unflushed frame is already past
///    its age: `should_flush_memtable` ⇒ SlateDB freezes the memtable **inside
///    that very write**, with the `last_flushed_wal_id` mark of that moment —
///    the write's own frame is not durable yet, so it is in L0 but outside the
///    mark (a gap).
/// 2. The forced-flush round meets the frame in the gap: flushing the memtable
///    is a no-op (empty memtable) ⇒ `flush_through` writes a fence. The fence
///    write again meets a frame past its age (the frame of step 1) ⇒ freezes at
///    once again with the mark before the fence; the flush after the fence is
///    durable has an empty memtable ⇒ the mark stops at the frame of step 1.
///    The fence lands in the gap.
/// 3. The next tick force-flushes that fence, writes a new fence… forever.
///
/// Setup: frame age 100 ms, one row written every 130 ms (every write meets a
/// frame past its age ⇒ a gap in step 1), then leave the range idle.
///
/// Must see: after 1 s idle, the next 2 s have no log PUT (every frame is below
/// the stored mark). Before the fix: one fence per forced-flush tick — one log
/// PUT, one L0 SST, one manifest write every ~200 ms (at the default age of
/// 60 s: every ~minute, forever, for every idle range that ever fell into the
/// gap), and the log's GC floor stood at the newest fence.
#[tokio::test]
async fn forced_flush_does_not_chase_its_own_barrier() -> Result<()> {
    let b = bucket();
    let cfg = NodeWalConfig {
        frame_age_flush: Duration::from_millis(100),
        gc_interval: Duration::from_secs(3600),
        ..fast()
    };
    let a = node(&b, "a", cfg).await?;
    let (mut w, _k) = acquire(&b, &a, 0).await?;
    for i in 0..12 {
        put(&mut w, i, b"v").await?;
        tokio::time::sleep(Duration::from_millis(130)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let before = b.faulty.counts();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let puts = b
        .faulty
        .counts()
        .since(&before)
        .get(Op::Put, PathClass::NodeWal);
    assert_eq!(
        puts, 0,
        "an idle range still PUT the shared-WAL log {puts} times in 2 s — the forced flush chases its own fence"
    );
    for i in 0..12 {
        assert_eq!(get(&w, i).await?.as_deref(), Some(&b"v"[..]));
    }
    Ok(())
}
