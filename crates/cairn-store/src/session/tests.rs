// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use {
    super::*,
    crate::testkit::{
        Action, Ambiguity, ContentEtagStore, FaultyStore, Gate, OpSet, PathMatch, Rule,
    },
    slatedb::object_store::memory::InMemory,
};

const TTL: Duration = Duration::from_millis(300);
const PACE: Duration = Duration::from_millis(50);

fn cfg(node: &str) -> LeaseConfig {
    LeaseConfig::new()
        .with_node_id(node)
        .with_ttl(TTL)
        .with_renew_interval(PACE)
        .with_address(format!("http://{node}"))
}

fn root() -> Path {
    Path::from("")
}

async fn session_on(store: Arc<dyn ObjectStore>, node: &str) -> Session {
    Session::establish(store, &root(), cfg(node)).await.unwrap()
}

async fn eventually(deadline: Duration, what: &str, mut ok: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !ok() {
        assert!(t0.elapsed() < deadline, "over {deadline:?}: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until the grant loses **validity** (LS-2: by the clock, ⅔·TTL since
/// the last renewal was issued) — no longer the moment `lost()` fires (the
/// suspension cap). Polls every ms; returns the time waited.
async fn invalid_within(keeper: &LeaseKeeper, bound: Duration, what: &str) -> Duration {
    let t0 = Instant::now();
    while keeper.is_valid() {
        assert!(t0.elapsed() < bound, "over {bound:?}: {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    t0.elapsed()
}

async fn won(s: &Session, p: u32) -> (LeadershipGrant, LeaseKeeper) {
    match s.claim(PartitionId(p)).await.unwrap() {
        Claim::Won(g, k) => (g, k),
        Claim::Held(by) => panic!("p{p} not claimed: {by:?}"),
    }
}

async fn held(s: &Session, p: u32) -> Option<OwnerRecord> {
    match s.claim(PartitionId(p)).await.unwrap() {
        Claim::Won(..) => panic!("p{p} claimed while it should be held"),
        Claim::Held(by) => by,
    }
}

async fn state_of(store: &Arc<dyn ObjectStore>, id: &SessionId) -> Option<SessionState> {
    read_session(store, &root(), id)
        .await
        .unwrap()
        .and_then(|(r, _)| r.ok())
        .map(|r| r.state)
}

#[test]
fn session_record_roundtrips() {
    let r = SessionRecord {
        node: "host/1/2".into(),
        beat: 9,
        address: Some("http://h:1".into()),
        ingress: Some("http://h:2".into()),
        knows: Some(4),
        slow: Some(64),
        p99_put_ms: Some(1500),
        draining: true,
        owns: BTreeMap::from([(0, 3), (5, 1)]),
        state: SessionState::Live,
        mode: SessionMode::Direct,
        challenged: None,
    };
    assert_eq!(SessionRecord::decode(&r.encode()).unwrap(), r);
    let bare = SessionRecord {
        address: None,
        ingress: None,
        knows: None,
        slow: None,
        p99_put_ms: None,
        draining: false,
        owns: BTreeMap::new(),
        state: SessionState::Expired,
        ..r
    };
    assert_eq!(SessionRecord::decode(&bare.encode()).unwrap(), bare);
}

#[test]
fn broken_session_records_are_rejected() {
    for raw in [
        &b"junk"[..],
        b"cairnflow-session/1\nbeat=1\nstate=live\n",
        b"cairnflow-session/1\nnode=a\nstate=live\n",
        b"cairnflow-session/1\nnode=a\nbeat=1\nstate=zombie\n",
        b"cairnflow-session/1\nnode=a\nbeat=1\nstate=live\nowns=1\n",
        b"cairnflow-session/2\nnode=a\nbeat=1\nstate=live\n",
    ] {
        assert!(SessionRecord::decode(raw).is_err(), "{raw:?}");
    }
}

/// **Opening a range with an unknown `wal_kind` ⇒ refused.** An owner record
/// left by a newer binary (`wal_kind=future-kind`, yielded) must not be claimed
/// by us and overwritten as `slatedb`: opening the Db with the native WAL while
/// unflushed data sits in the node log silently drops acked rows. The record
/// must stay intact.
#[tokio::test(flavor = "multi_thread")]
async fn unknown_wal_kind_refuses_open() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = owner::owner_path(&root(), PartitionId(0));
    let raw = "cairnflow-owner/1\nterm=4\nsession=newer/1\nreleased=1\nwal_kind=future-kind\n";
    store
        .put(&path, PutPayload::from(raw.as_bytes().to_vec()))
        .await
        .unwrap();
    let a = session_on(store.clone(), "a").await;
    let err = match a.claim(PartitionId(0)).await {
        Ok(Claim::Won(..)) => panic!("claimed a range with an unknown wal_kind"),
        Ok(Claim::Held(by)) => panic!("must be a refusal error, got Held({by:?})"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, StoreError::UnknownWalKind { range: 0, kind } if kind == "future-kind"),
        "{err:?}"
    );
    let after = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(
        &after[..],
        raw.as_bytes(),
        "the owner record was overwritten"
    );
    assert!(a.owned().is_empty());
    // Another range is still claimed normally.
    let _ = won(&a, 1).await;
    a.close().await;
}

#[tokio::test]
async fn deadline_before_earliest_expire() {
    let t = Instant::now();
    assert!(t + TTL - renew_deadline(t, TTL) >= TTL / 4);
}

/// A content-etag store (like S3): a healthy session is never marked, because
/// the beat increases at every renewal.
#[tokio::test(flavor = "multi_thread")]
async fn healthy_session_never_expired_on_content_etag_store() {
    let store: Arc<dyn ObjectStore> = Arc::new(ContentEtagStore::default());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let a_id = a.current().unwrap();
    tokio::time::sleep(TTL * 3).await;
    assert_eq!(
        a.current(),
        Some(a_id.clone()),
        "A changed incarnation although healthy"
    );
    assert!(!b.tracker().is_dead(&a_id));
    assert_eq!(
        b.tracker().live_nodes(),
        BTreeSet::from(["a".into(), "b".into()])
    );
    assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Live));
    a.close().await;
    b.close().await;
}

/// A session that stops renewing is marked after one silent TTL — not sooner.
#[tokio::test(flavor = "multi_thread")]
async fn stalled_session_expired_after_ttl_not_before() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let a_id = a.current().unwrap();
    // B must have seen A at least once first.
    eventually(TTL * 2, "B sees A", || {
        b.tracker().live_nodes().contains("a")
    })
    .await;
    tokio::time::sleep(PACE * 2).await;
    let t0 = Instant::now();
    a.crash_handle().crash();
    let tracker = b.tracker();
    eventually(TTL * 3, "B marks A", || tracker.is_dead(&a_id)).await;
    let took = t0.elapsed();
    assert!(
        took >= TTL - PACE * 2,
        "marked after {took:?}, sooner than a TTL"
    );
    assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Expired));
    b.close().await;
}

/// A prolonged failing LIST of `sessions/` must not make the tracker stop
/// observing **known** sessions.
///
/// Interleaving:
/// 1. A and B are alive; B's tracker has LISTed and seen A.
/// 2. From here on every LIST of `sessions/` fails (no LIST permission,
///    throttled by prefix).
/// 3. A dies (`crash`): A's record is still `live`.
/// 4. B's observation round: when a LIST is due (every TTL), the LIST fails.
///
/// Must see: B still GETs A's record every round and marks it `expired` after
/// one silent TTL, then claims A's ranges. Before the fix, `refresh` returned
/// the error at the LIST before the `observe` round, so A was never declared
/// dead.
#[tokio::test(flavor = "multi_thread")]
async fn failed_list_still_observes_known_sessions() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let a_id = a.current().unwrap();
    let (_g, k) = won(&a, 3).await;
    eventually(TTL * 2, "B sees A", || {
        b.tracker().live_nodes().contains("a")
    })
    .await;
    faulty.add_rule(Rule::new(
        OpSet::LIST,
        PathMatch::Contains("sessions".into()),
        Action::Fail { times: None },
    ));
    a.crash_handle().crash();
    std::mem::forget(k);
    let tracker = b.tracker();
    eventually(TTL * 5, "B marks A despite the failing LIST", || {
        tracker.is_dead(&a_id)
    })
    .await;
    assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Expired));
    let _ = won(&b, 3).await;
    b.close().await;
}

/// An owner marked while frozen: its next renewal misses, every grant is lost,
/// and it opens a **new** incarnation instead of reviving the old one.
#[tokio::test(flavor = "multi_thread")]
async fn paused_owner_learns_expiry_on_next_renew() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    // A usurper marks A — exactly the CAS the tracker does. A is still
    // renewing so the CAS may miss: re-read and try again.
    loop {
        let (rec, version) = read_session(&store, &root(), &a_id).await.unwrap().unwrap();
        let expired = SessionRecord {
            state: SessionState::Expired,
            ..rec.unwrap()
        };
        let put = store
            .put_opts(
                &session_path(&root(), &a_id),
                PutPayload::from(expired.encode()),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;
        match put {
            Ok(_) => break,
            Err(OsError::Precondition { .. }) => continue,
            Err(e) => panic!("{e}"),
        }
    }
    tokio::time::timeout(TTL, keeper.lost())
        .await
        .expect("A's grant still alive after its session was marked");
    eventually(TTL * 2, "A opens a new incarnation", || {
        a.current().is_some_and(|id| id != a_id)
    })
    .await;
    assert!(a.current().unwrap().inc > a_id.inc);
    assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Expired));
    a.close().await;
}

/// Writing `owners/` succeeds but renewing the session hangs: the partition's
/// grant still loses validity by the **session's** deadline.
#[tokio::test(flavor = "multi_thread")]
async fn grant_bounded_by_session_when_owner_put_succeeds_but_renew_hangs() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Hang,
    ));
    let t0 = Instant::now();
    let (grant, keeper) = won(&a, 0).await;
    let child = grant.validity_handle();
    invalid_within(&keeper, TTL, "the grant lives longer than the session").await;
    assert!(!child.is_valid());
    assert!(
        t0.elapsed() <= TTL,
        "the grant lost validity after {:?}",
        t0.elapsed()
    );
}

/// A renewal that timed out but actually landed is adopted — the node is not dropped.
#[tokio::test(flavor = "multi_thread")]
async fn landed_renewal_after_timeout_is_adopted() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    let rule = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::DelayResponse(PACE + PACE / 2),
    ));
    tokio::time::sleep(PACE * 4).await;
    faulty.remove_rule(rule);
    tokio::time::sleep(PACE * 4).await;
    assert_eq!(
        a.current(),
        Some(a_id),
        "A dropped its session because of a renewal that had written"
    );
    assert!(keeper.is_valid());
    a.close().await;
}

/// A PUT of `sessions/` slower than the renewal pace but still within the
/// self-demotion deadline (the chaos `--slow-node` run: PUT +1.4 s, TTL 6 s,
/// pace 1.2 s ⇒ the slow node lost its session every ~5.5 s).
///
/// 1. A opens a session, claims p0. The tokio clock is paused: only the
///    injected latency advances.
/// 2. Every PUT of `sessions/` waits 90 ms **before** reaching the store (like
///    chaos's `DelayStore` — dropping the wait means no write). Pace 50 ms,
///    ⅔ TTL = 200 ms.
/// 3. A per-attempt cap equal to the pace ⇒ every renewal is cancelled before
///    writing ⇒ past the deadline at 200 ms. A pace measured from when the
///    previous attempt **finished** ⇒ the next one is issued at 90 + 50 =
///    140 ms and finishes at 230 ms > 200 ms ⇒ also lost.
///
/// Must see: after 20 TTLs still the same incarnation, the grant still valid,
/// the beat increasing.
#[tokio::test(start_paused = true)]
async fn renewal_slower_than_pace_keeps_session() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    let before = read_session(&faulty.inner().clone(), &root(), &a_id)
        .await
        .unwrap()
        .unwrap()
        .0
        .unwrap()
        .beat;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Delay(Duration::from_millis(90)),
    ));
    tokio::time::sleep(TTL * 20).await;
    assert_eq!(
        a.current(),
        Some(a_id.clone()),
        "A lost its session because a PUT was slower than the pace"
    );
    assert!(
        keeper.is_valid(),
        "the grant was lost although the session remains"
    );
    let after = read_session(&faulty.inner().clone(), &root(), &a_id)
        .await
        .unwrap()
        .unwrap()
        .0
        .unwrap()
        .beat;
    assert!(
        after >= before + 20,
        "renewal not running: {before} → {after}"
    );
    a.close().await;
}

/// A PUT slower than the self-demotion deadline: the grant loses validity at
/// ⅔·TTL (LS-2), and the session must still be lost at the suspension cap — a
/// deadline-based attempt cap must not turn into "wait forever". Every renewal
/// here **wins** the CAS, only after its own deadline: a win whose new deadline
/// is still in the past does not move the cap, so a node whose every PUT is
/// that slow does not stay suspended forever.
#[tokio::test(start_paused = true)]
async fn renewal_slower_than_deadline_loses_session() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let (_g, keeper) = won(&a, 0).await;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Delay(TTL),
    ));
    let t0 = Instant::now();
    let took = invalid_within(&keeper, TTL, "grant alive, no renewal in time").await;
    assert!(
        took <= TTL.mul_f32(2.0 / 3.0) + PACE,
        "validity lost after {took:?}"
    );
    tokio::time::timeout(TTL * 2, keeper.lost())
        .await
        .expect("a PUT that won but came back late moved the suspension cap");
    assert!(
        t0.elapsed() <= TTL + TTL / 3 + PACE,
        "lost after {:?}",
        t0.elapsed()
    );
    a.close().await;
}

/// An ambiguous renewal (written, then an error returned — resolved by
/// re-reading) is adopted with **its own** issue mark, not the mark of the
/// later attempt.
///
/// 1. A opens a session, claims p0 (clock paused); the renewal nudged by
///    `claim` finishes. T0 = the injection moment, before the next renewal.
/// 2. Renewal #1 is issued within one pace: the store writes beat b, then
///    returns `Err`.
/// 3. #2 is issued one pace later, CAS from the old version ⇒ `Precondition`;
///    re-reading finds beat b — ours. An observer may have seen b since #1
///    wrote it.
/// 4. Every later PUT of `sessions/` hangs.
///
/// Must see: the grant loses validity before T0 + pace + ⅔ TTL. Adopting
/// #2's mark would push the deadline one more pace — past the mark the proof
/// at the top of the file relies on.
#[tokio::test(start_paused = true)]
async fn ambiguous_renewal_counts_from_its_own_issue_time() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let (_g, keeper) = won(&a, 0).await;
    // The renewal nudged by `claim` (reporting `owns`) finishes before the injection.
    tokio::time::sleep(PACE / 5).await;
    let t0 = Instant::now();
    let sessions = || PathMatch::Contains("sessions/".into());
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        sessions(),
        Action::Ambiguous {
            reply: Ambiguity::Error,
            skip: 0,
            times: Some(1),
        },
    ));
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        sessions(),
        Action::GateAfter {
            pass: 2,
            gate: Gate::closed(),
        },
    ));
    invalid_within(&keeper, TTL * 2, "the grant lives although renewals hang").await;
    let bound = PACE + TTL.mul_f32(2.0 / 3.0);
    assert!(
        t0.elapsed() <= bound,
        "lost after {:?} > {bound:?}",
        t0.elapsed()
    );
    a.close().await;
}

/// One hung renewal PUT (a stuck connection) does not eat the whole deadline:
/// the renewal's cap is the remaining deadline, but every pace without an answer
/// issues one more attempt in parallel. Must see: the same incarnation, the
/// grant still valid.
#[tokio::test(start_paused = true)]
async fn hung_renewal_put_is_hedged_not_waited_out() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    tokio::time::sleep(PACE / 5).await;
    // Only the first PUT of `sessions/` from here hangs; later ones go through.
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::EachOnce(Box::new(Action::Hang)),
    ));
    tokio::time::sleep(TTL * 5).await;
    assert_eq!(
        a.current(),
        Some(a_id),
        "A lost its session because of one hung PUT"
    );
    assert!(keeper.is_valid());
    a.close().await;
}

/// Whether `keeper`'s `lost()` has returned yet — without waiting.
fn has_lost(keeper: &LeaseKeeper) -> bool {
    futures::FutureExt::now_or_never(keeper.lost()).is_some()
}

/// **Revival by CAS.**
///
/// 1. A opens a session, claims p0 (the tokio clock is paused: only the
///    injected latency advances).
/// 2. Every PUT of `sessions/` by A stands at a gate: the renewal has been
///    issued but has not reached the store — a network slow by ~one TTL.
/// 3. Past ⅔·TTL no renewal has come back: the grant loses validity by the
///    clock (LS-2) — no ack, no read.
/// 4. At 0.9·TTL from the injection (no observer can CAS `expired` yet: it must
///    be silent for one TTL from when it saw the last beat), the gate opens:
///    the renewal CAS reaches the store from **exactly the previous version**
///    ⇒ it wins — proof that nobody marked A expired or overwrote the record.
///
/// Must see: during (3) the grant is invalid yet `lost()` does **not** fire
/// (suspended, not lost); after (4) the same incarnation, p0's grant valid
/// again, `lost()` never fired. Before the fix: past ⅔·TTL ⇒ `lose()` —
/// revoking every grant, opening a new incarnation, every range claimed again
/// and replayed.
#[tokio::test(start_paused = true)]
async fn late_renewal_that_wins_cas_revives_the_session() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    // The renewal nudged by `claim` (reporting `owns`) finishes before the injection.
    tokio::time::sleep(PACE / 5).await;
    let gate = Gate::closed();
    let rule = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Gate(gate.clone()),
    ));
    tokio::time::sleep(TTL.mul_f32(0.9)).await;
    assert!(
        !keeper.is_valid(),
        "the grant is still valid past ⅔·TTL (LS-2)"
    );
    assert!(
        !has_lost(&keeper),
        "the grant was dropped while suspended, before the CAS was known to miss"
    );
    assert_eq!(
        a.current(),
        Some(a_id.clone()),
        "A dropped its session while suspended"
    );
    gate.open();
    faulty.remove_rule(rule);
    tokio::time::sleep(PACE * 4).await;
    assert_eq!(
        a.current(),
        Some(a_id.clone()),
        "A dropped its session although the renewal CAS won"
    );
    assert!(
        keeper.is_valid(),
        "the renewal CAS won yet the grant did not revive"
    );
    assert!(!has_lost(&keeper));
    // The record is still `live`, the beat keeps increasing (the renewal pace runs again).
    let rec = read_session(&faulty.inner().clone(), &root(), &a_id)
        .await
        .unwrap()
        .unwrap()
        .0
        .unwrap();
    assert_eq!(rec.state, SessionState::Live);
    a.close().await;
}

/// Suspension **with a cap**. Every PUT of `sessions/` hangs forever: the grant
/// loses validity at ⅔·TTL (LS-2), but is only lost for good (`lost()`,
/// `lose`) at the suspension cap — one TTL plus a third of a TTL from the last
/// renewal — not sooner, not later. Before the fix: lost for good right at
/// ⅔·TTL.
#[tokio::test(start_paused = true)]
async fn paused_session_is_lost_at_the_pause_cap() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    tokio::time::sleep(PACE / 5).await;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Hang,
    ));
    let t0 = Instant::now();
    tokio::time::timeout(TTL * 2, keeper.lost())
        .await
        .expect("a suspension without a cap");
    let took = t0.elapsed();
    // The last successful renewal was issued within one pace before `t0`.
    let cap = TTL + TTL / 3;
    assert!(
        took + PACE >= cap,
        "lost after {took:?}: sooner than the suspension cap {cap:?}"
    );
    assert!(
        took <= cap + PACE,
        "lost after {took:?}: past the cap {cap:?}"
    );
    eventually(PACE, "past the cap, incarnation kept", || {
        a.current() != Some(a_id.clone())
    })
    .await;
    a.close().await;
}

/// Suspended, then the CAS **misses** ⇒ `lose` as before. A is blocked from
/// renewing past ⅔·TTL; meanwhile an observer CASes `expired` onto A's record
/// (exactly the tracker's operation). The gate opens: A's renewal CAS misses,
/// re-reading finds `expired`.
///
/// Must see: the grant lost for good (`lost()`), A opens a new incarnation, the
/// old record in a terminal state — cannot be revived. Green before the fix
/// too (lost at ⅔·TTL): this is the net for the fix's miss branch.
#[tokio::test(flavor = "multi_thread")]
async fn late_renewal_that_loses_cas_loses_the_session() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.inner().clone();
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    let gate = Gate::closed();
    let rule = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Gate(gate.clone()),
    ));
    // The record stands still under the gate: the observer's `expired` CAS wins.
    let (rec, version) = read_session(&store, &root(), &a_id).await.unwrap().unwrap();
    let expired = SessionRecord {
        state: SessionState::Expired,
        ..rec.unwrap()
    };
    store
        .put_opts(
            &session_path(&root(), &a_id),
            PutPayload::from(expired.encode()),
            PutOptions::from(PutMode::Update(version)),
        )
        .await
        .unwrap();
    tokio::time::sleep(TTL.mul_f32(0.8)).await;
    assert!(!keeper.is_valid());
    gate.open();
    faulty.remove_rule(rule);
    tokio::time::timeout(TTL, keeper.lost())
        .await
        .expect("the renewal CAS missed yet the grant is still alive");
    eventually(TTL * 2, "A opens a new incarnation", || {
        a.current().is_some_and(|id| id != a_id)
    })
    .await;
    // Before the fix `lose` at ⅔·TTL overwrote it with `closed` (an
    // unconditional PUT, reaching the store when the gate opens) — also a
    // terminal state.
    assert!(
        matches!(
            state_of(&store, &a_id).await,
            Some(SessionState::Expired | SessionState::Closed)
        ),
        "the old record must be in a terminal state"
    );
    a.close().await;
}

/// **The read fence at the leader**, batched.
///
/// 1. A opens a session, claims p0. A's GETs of `sessions/` stand at a gate.
/// 2. One reply calls the fence: the first batch's GET is issued and stands at
///    the gate.
/// 3. 63 more replies call the fence while that GET is in flight — they wait
///    for the next batch (their GET must be issued **after** their read).
/// 4. Open the gate.
///
/// Must see: all 64 pass, with exactly **two** GETs of `sessions/` — one per
/// batch, not one per reply. Then an observer CASes `expired` onto A's record:
/// the next fence is refused — whatever A's clock says.
#[tokio::test(flavor = "multi_thread")]
async fn read_fence_batches_and_refuses_after_expiry() {
    use crate::testkit::{Op, PathClass};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.inner().clone();
    let a = session_on(faulty.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g, keeper) = won(&a, 0).await;
    let fence = keeper
        .validity
        .read_fence()
        .expect("a grant under a session must carry the read fence");
    let gate = Gate::closed();
    let rule = faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("sessions/".into()),
        Action::Gate(gate.clone()),
    ));
    let before = faulty.counts().get(Op::Get, PathClass::Lease);
    let first = tokio::spawn(fence());
    eventually(TTL, "the first batch's GET reaches the gate", || {
        gate.blocked() == 1
    })
    .await;
    let rest: Vec<_> = (0..63).map(|_| tokio::spawn(fence())).collect();
    // Let 63 requests get into the channel before the first batch finishes.
    tokio::time::sleep(PACE / 5).await;
    gate.open();
    faulty.remove_rule(rule);
    assert!(
        first.await.unwrap(),
        "the session is still live yet the fence refused"
    );
    for r in rest {
        assert!(
            r.await.unwrap(),
            "the session is still live yet the fence refused"
        );
    }
    let gets = faulty.counts().get(Op::Get, PathClass::Lease) - before;
    assert_eq!(
        gets, 2,
        "the fence must cost one GET per batch, not one per reply"
    );

    // An observer marks A expired (A is still renewing so the CAS may miss:
    // re-read and try again).
    loop {
        let (rec, version) = read_session(&store, &root(), &a_id).await.unwrap().unwrap();
        let expired = SessionRecord {
            state: SessionState::Expired,
            ..rec.unwrap()
        };
        let put = store
            .put_opts(
                &session_path(&root(), &a_id),
                PutPayload::from(expired.encode()),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;
        match put {
            Ok(_) => break,
            Err(OsError::Precondition { .. }) => continue,
            Err(e) => panic!("{e}"),
        }
    }
    assert!(
        !fence().await,
        "the fence let it through although the session record is expired"
    );
    a.close().await;
}

/// The session of a dead node **is torn**: GET changes the etag, the CAS
/// always misses. After `UNEXPIRABLE_AFTER_TTLS` silent TTLs it used to be
/// considered dead. A torn object that **cannot** be healed (GET flips the etag
/// back and forth, every PUT misses — even an overwrite): before the fix B
/// considered A dead after [`UNEXPIRABLE_AFTER_TTLS`] TTLs by its own clock
/// alone.
///
/// Must see: B does **not** consider A dead — there is no fence on the bucket;
/// it only warns and counts `rearm_stats::FAILED`. The record stays `live`.
#[tokio::test(flavor = "multi_thread")]
async fn torn_session_that_cannot_be_rearmed_is_never_declared_dead() {
    let shared = Arc::new(InMemory::new());
    let a = session_on(shared.clone(), "node-a").await;
    let b_store = FaultyStore::wrap(shared.clone());
    let b = session_on(b_store.clone(), "node-b").await;
    let a_id = a.current().unwrap();
    eventually(TTL * 2, "B sees A", || {
        b.tracker().live_nodes().contains("node-a")
    })
    .await;
    let torn = PathMatch::Contains("node-a".into());
    b_store.add_rule(Rule::new(OpSet::GET, torn.clone(), Action::FlipEtag));
    b_store.add_rule(Rule::new(OpSet::PUT, torn, Action::Precondition));
    let failed = rearm_stats::FAILED.load(std::sync::atomic::Ordering::Relaxed);
    a.crash_handle().crash();
    let tracker = b.tracker();
    eventually(TTL * 6, "B tries to overwrite the torn record", || {
        rearm_stats::FAILED.load(std::sync::atomic::Ordering::Relaxed) > failed
    })
    .await;
    tokio::time::sleep(TTL * 2).await;
    assert!(!tracker.is_dead(&a_id), "considered dead without any fence");
    assert_eq!(
        state_of(&(shared as Arc<dyn ObjectStore>), &a_id).await,
        Some(SessionState::Live)
    );
    b.close().await;
}

/// The record of a **dead** node torn MinIO-EC style (GET returns the old
/// version, the etag flips, the CAS misses) that an unconditional PUT can heal
/// ([`Action::Tear`]).
///
/// Must see: after [`UNEXPIRABLE_AFTER_TTLS`] TTLs, B overwrites the record
/// (the Tear is healed), waits **one more TTL**, then the `expired` CAS wins —
/// only then is A dead; the record on the bucket is `expired`.
#[tokio::test(flavor = "multi_thread")]
async fn torn_session_of_dead_node_is_rearmed_then_expired() {
    let shared = Arc::new(InMemory::new());
    let a = session_on(shared.clone(), "node-a").await;
    let b_store = FaultyStore::wrap(shared.clone());
    let b = session_on(b_store.clone(), "node-b").await;
    let a_id = a.current().unwrap();
    eventually(TTL * 2, "B sees A", || {
        b.tracker().live_nodes().contains("node-a")
    })
    .await;
    let tear = crate::testkit::Tear::new();
    b_store.add_rule(Rule::new(
        OpSet::GET | OpSet::HEAD | OpSet::PUT,
        PathMatch::Contains("sessions/node-a".into()),
        Action::Tear(tear.clone()),
    ));
    a.crash_handle().crash();
    let tracker = b.tracker();
    eventually(TTL * 6, "B overwrites the torn record", || tear.healed()).await;
    let rearmed = Instant::now();
    assert!(!tracker.is_dead(&a_id), "dead right at the overwrite");
    eventually(TTL * 3, "B marks A after the overwrite", || {
        tracker.is_dead(&a_id)
    })
    .await;
    assert!(
        rearmed.elapsed() >= TTL - PACE * 2,
        "considered dead {:?} after the overwrite — sooner than a TTL",
        rearmed.elapsed()
    );
    assert_eq!(
        state_of(&(shared as Arc<dyn ObjectStore>), &a_id).await,
        Some(SessionState::Expired)
    );
    b.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn claim_matrix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let b_id = b.current().unwrap();

    // Nobody has claimed it ⇒ term 1.
    let (_g, k0) = won(&a, 0).await;
    assert_eq!(k0.term(), 1);
    // Someone else's live session ⇒ held.
    assert_eq!(held(&b, 0).await.map(|r| r.session.node), Some("a".into()));
    // The current incarnation itself ⇒ reclaimed, the term increases. Drop
    // hands the yield to the session's background loop; that write may win the
    // CAS before the claim right below ⇒ `Held(None)` — exactly the contract
    // "someone just changed the record, try again after a pace". Retry as the
    // real callers do.
    drop(k0);
    let k0 = loop {
        match a.claim(PartitionId(0)).await.unwrap() {
            Claim::Won(_, k) => break k,
            Claim::Held(None) => tokio::time::sleep(PACE).await,
            Claim::Held(Some(by)) => panic!("p0 held by {by:?}"),
        }
    };
    assert_eq!(k0.term(), 2);
    // Yielded ⇒ anyone claims it at once.
    k0.release().await;
    let (_g, kb) = won(&b, 0).await;
    assert_eq!(kb.term(), 3);

    // A session that has ended ⇒ claimable.
    let (_g, _k1) = won(&b, 1).await;
    b.close().await;
    assert_eq!(state_of(&store, &b_id).await, Some(SessionState::Closed));
    let (_g, k1) = won(&a, 1).await;
    assert_eq!(k1.term(), 2);

    // A session that does not exist ⇒ claimable.
    let ghost = SessionId {
        node: "ma".into(),
        inc: 1,
    };
    owner::write_owner(
        &store,
        &root(),
        PartitionId(2),
        &OwnerRecord {
            term: 5,
            session: ghost,
            address: None,
            released: false,
            wal_kind: owner::WalKind::SlateDb,
            retired: false,
            wal_segments: vec![],
            wal_floor_id: None,
            data: None,
            retired_ms: None,
        },
        None,
    )
    .await
    .unwrap();
    let (_g, k2) = won(&a, 2).await;
    assert_eq!(k2.term(), 6);

    // A live session the tracker **has not seen yet** ⇒ GET directly, held.
    let c_store: Arc<dyn ObjectStore> = store.clone();
    let c = session_on(c_store, "c").await;
    let (_g, _k3) = won(&c, 3).await;
    assert!(held(&a, 3).await.is_some());
    c.close().await;
    a.close().await;
}

/// Our own abandoned incarnation ⇒ reclaimed at once, without waiting for anyone to mark it.
#[tokio::test(flavor = "multi_thread")]
async fn own_abandoned_incarnation_is_reclaimable() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let old = a.current().unwrap();
    let (_g, k) = won(&a, 0).await;
    // Renewals miss for a while ⇒ the session is lost by deadline.
    let r = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Fail { times: None },
    ));
    tokio::time::timeout(TTL * 2, k.lost())
        .await
        .expect("session lost");
    faulty.remove_rule(r);
    let t = Instant::now();
    loop {
        if a.current().is_some_and(|id| id != old) {
            break;
        }
        assert!(t.elapsed() < TTL * 3, "A could not open a new incarnation");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (_g, k) = won(&a, 0).await;
    assert_eq!(k.term(), 2);
    a.close().await;
}

/// **An owner-record CAS with an ambiguous result is resolved by re-reading.**
/// object_store 0.14 retries a 5xx by itself, even a conditional PUT: the first
/// attempt wrote, the retry sees `Precondition` (`Update`) or `AlreadyExists`
/// (`Create`).
///
/// Interleaving:
/// 1. A claims range 3 (an ambiguous `Create`) — A's record is on the bucket.
/// 2. A's keeper CASes `data=` (an ambiguous `Update`) — like every write of
///    the keeper (`add_wal_segment` when claiming a range on the shared WAL,
///    the migration's `set_data`).
///
/// Must see: both are **wins** — `Claim::Won`, `set_data` returns `Ok` and the
/// next CAS uses exactly the new version. Before the fix: `Claim::Held(None)`
/// and a false `LeaseLost` — with the shared WAL, `acquire_shared` fail-stops
/// the whole node.
#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_owner_cas_is_resolved_by_reading_back() {
    use crate::testkit::Ambiguity;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let a = session_on(store.clone(), "a").await;
        let rule = || {
            Rule::new(
                OpSet::PUT,
                PathMatch::Contains("owners/range-3".into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            )
        };
        let r = faulty.add_rule(rule());
        let mut k = match a.claim(PartitionId(3)).await {
            Ok(Claim::Won(_, k)) => k,
            Ok(Claim::Held(by)) => {
                panic!("{reply:?}: A's record is on the bucket, yet claim gave Held({by:?})")
            }
            Err(e) => panic!("{reply:?}: A's record is on the bucket, yet claim gave {e:?}"),
        };
        faulty.remove_rule(r);
        let r = faulty.add_rule(rule());
        k.set_data(7)
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: data= written, keeper said {e:?}"));
        faulty.remove_rule(r);
        k.set_data(8)
            .await
            .expect("the next CAS must use the new version");
        let rec = owner::read_owner(&store, &root(), PartitionId(3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.data, Some(8), "{reply:?}");
        a.close().await;
    }
}

/// The `Create` opening an incarnation wrote, but the answer says otherwise
/// (the retry's `AlreadyExists`, or an error) — resolved by re-reading.
///
/// Interleaving:
/// 1. A opens a session: the `Create` PUT of `sessions/a/<inc>` reaches the
///    store, the answer is ambiguous.
/// 2. Before the fix: `AlreadyExists` ⇒ `inc + 1` and another incarnation is
///    opened; an error ⇒ `establish` fails, the next LIST sees `<inc>` and opens
///    `<inc + 1>`.
///
/// Must see: the bucket has only **one** incarnation of A and it is the current
/// one — no ghost `live` copy nobody renews (peers count it as a live node
/// until its TTL runs out, like an earlier ghost-session bug).
#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_open_does_not_leave_a_ghost_incarnation() {
    use futures::TryStreamExt;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        faulty.add_rule(Rule::ambiguous_put("sessions/a/", reply));
        let a = Session::establish(store.clone(), &root(), cfg("a"))
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: incarnation written, establish failed: {e}"));
        let listed: Vec<_> = store
            .list(Some(&Path::from("sessions/a")))
            .try_collect()
            .await
            .unwrap();
        let me = a.current().expect("a live session");
        assert_eq!(listed.len(), 1, "{reply:?}: {listed:?}");
        assert_eq!(
            listed[0].location,
            record::session_path(&root(), &me),
            "{reply:?}"
        );
        a.close().await;
    }
}

/// The hardest window of the boot-time open: the `Create` of `<inc>` **has
/// reached** the store but returned an error, and `put_resolving`'s re-read
/// failed too — whether it wrote is unknown.
///
/// 1. A opens a session through `establish_within` (the boot path) on a store
///    whose first PUT of `sessions/a/` is ambiguous (`Ambiguity::Error`) and
///    whose first re-reading GET fails (503).
/// 2. The retry must use the **same** `<inc>` and the same contents: the
///    `Create` misses with `AlreadyExists`, re-reading finds ours ⇒ it won.
///
/// Must see: it opens, the bucket has only one incarnation of A and it is the
/// current one. Retrying from scratch (LISTing again) would see `<inc>` and
/// open `<inc + 1>`, leaving a ghost `live` session.
#[tokio::test(flavor = "multi_thread")]
async fn boot_open_retries_the_same_incarnation() {
    use {crate::testkit::OpSet, futures::TryStreamExt};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    faulty.add_rule(Rule::ambiguous_put("sessions/a/", Ambiguity::Error));
    faulty.add_rule(Rule::new(
        OpSet::GET,
        crate::testkit::PathMatch::Contains("sessions/a/".into()),
        crate::testkit::Action::FailNth { skip: 0 },
    ));
    let policy = crate::retry::Policy::new(Duration::from_millis(1), Duration::from_millis(5), 4);
    let a = Session::establish_within(store.clone(), &root(), cfg("a"), &policy)
        .await
        .unwrap_or_else(|e| panic!("the boot-time open did not retry: {e}"));
    let listed: Vec<_> = store
        .list(Some(&Path::from("sessions/a")))
        .try_collect()
        .await
        .unwrap();
    let me = a.current().expect("a live session");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].location, record::session_path(&root(), &me));
    a.close().await;
}

/// The ambiguous variant: the keeper's earlier write **has reached** the store
/// but the keeper does not know the new version (a clean-record CAS abandoned
/// past its cap). Built here by rewriting exactly the record's contents: the
/// contents do not change, the version does.
///
/// Must see: the yield CAS misses on our own record, recognizes it (same
/// session + term, not yielded) and CASes again — the record becomes
/// `released`. Before the fix, the miss was taken as "someone changed it": the
/// record still pointed at our live session, and no node claimed the range as
/// long as A lived.
#[tokio::test(flavor = "multi_thread")]
async fn release_after_unseen_own_write_still_releases() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let (_g, k) = won(&a, 6).await;
    let path = owner::owner_path(&root(), PartitionId(6));
    let body = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.put(&path, PutPayload::from(body)).await.unwrap();
    k.release().await;
    let rec = owner::read_owner(&store, &root(), PartitionId(6))
        .await
        .unwrap()
        .unwrap();
    assert!(rec.released, "the yield missed on our own record: {rec:?}");
    let _ = won(&b, 6).await;
    a.close().await;
    b.close().await;
}

/// The keeper's **segment add** reaches the store without the keeper knowing —
/// the PUT returns an error after writing, and the re-read fails too — so the
/// keeper keeps the old segment list. Then the keeper yields.
///
/// Order:
/// 1. A claims range 8.
/// 2. `add_wal_segment(S)`: the owner-record PUT **is applied**, then returns an
///    error; both re-reads fail ⇒ `Err`, the keeper still thinks the list has
///    no `S`.
/// 3. A yields: the CAS on the old version misses; the record is still A's
///    (session + term), not yielded, but lists `S`, which the keeper does not
///    know.
///
/// Must see: the record becomes `released` **and still lists `S`** (a dirty
/// yield: the claimer seals and replays — no segment is dropped), B claims the
/// range. Before the fix: the yield took `Ok(false)` as done, the record pointed
/// at A's live session forever — no node claimed the range as long as A lived.
#[tokio::test(flavor = "multi_thread")]
async fn release_after_ambiguous_segment_add_still_releases_and_keeps_the_segment() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let (_g, mut k) = won(&a, 8).await;
    let seg = owner::WalSegment {
        log: crate::owner::LogId::parse("a/1:0").expect("log id"),
        first_wal_id: 5,
    };
    let put = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-8".into()),
        Action::Ambiguous {
            reply: Ambiguity::Error,
            skip: 0,
            times: Some(1),
        },
    ));
    let get = faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("owners/range-8".into()),
        // Twice: the re-read of `cas::put_resolving` and the keeper's.
        Action::Fail { times: Some(2) },
    ));
    assert!(
        k.add_wal_segment(seg.clone()).await.is_err(),
        "the segment add must return an error (written first, then failed, the re-read failed)"
    );
    faulty.remove_rule(put);
    faulty.remove_rule(get);
    let landed = owner::read_owner(&store, &root(), PartitionId(8))
        .await
        .unwrap()
        .unwrap();
    assert!(
        landed.wal_segments.contains(&seg),
        "broken setup: the segment add must have reached the store: {landed:?}"
    );
    k.release().await;
    eventually(TTL * 3, "the record becomes released", || {
        !a.owned().contains_key(&8)
    })
    .await;
    let rec = owner::read_owner(&store, &root(), PartitionId(8))
        .await
        .unwrap()
        .unwrap();
    assert!(rec.released, "the record must become released: {rec:?}");
    assert!(
        rec.wal_segments.contains(&seg),
        "the yield must not drop a segment the record lists: {rec:?}"
    );
    let _ = won(&b, 8).await;
    a.close().await;
    b.close().await;
}

/// **Reclaiming by itself after a clean yield that has not reached the bucket**
/// (seen as 18 of 27 C1 hits in a chaos run).
///
/// Order:
/// 1. A claims p10 and p11, adds to each record a segment `S` pointing at A's
///    own log; both ranges' Dbs are "sealed" (`mark_wal_clean`): every frame is
///    in L0.
/// 2. A's background loop stops (`crash`: as before its retry tick), every
///    owner-record PUT fails: the clean-record CAS does not happen, two yields
///    sit in `pending_release` — the records on the bucket are still A's and
///    still list `S`.
/// 3. At p11, a segment `T` the keeper does not know reaches the bucket (an
///    ambiguous segment add).
/// 4. The store comes back; A reclaims p10 and p11 in the same incarnation.
///
/// Must see: p10 — the reclaimed record carries the WAL part of the pending
/// yield (no segment): as if the yield arrived first and the reclaim after.
/// p11 — the record disagrees with what the keeper knows: keep `S` and `T` as
/// they are (conservative, as before). Before the fix: p10 kept `S` — claiming
/// the range met A's own live log (C1) and the whole node fail-stopped.
#[tokio::test(flavor = "multi_thread")]
async fn reclaim_applies_the_pending_release_only_over_the_record_it_was_made_for() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = session_on(store.clone(), "a").await;
    let me = a.current().expect("a session just opened");
    let s_seg = owner::WalSegment {
        log: crate::owner::LogId {
            session: me,
            stripe: 0,
        },
        first_wal_id: 1,
    };
    let (_g10, mut k10) = won(&a, 10).await;
    let (_g11, mut k11) = won(&a, 11).await;
    k10.add_wal_segment(s_seg.clone()).await.unwrap();
    k11.add_wal_segment(s_seg.clone()).await.unwrap();
    k10.mark_wal_clean(1);
    k11.mark_wal_clean(1);
    // 2.
    a.crash_handle().crash();
    let rule = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-1".into()),
        Action::Fail { times: None },
    ));
    k10.release().await;
    k11.release().await;
    faulty.remove_rule(rule);
    for p in [10, 11] {
        let rec = owner::read_owner(&store, &root(), PartitionId(p))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !rec.released && rec.wal_segments == vec![s_seg.clone()],
            "p{p}: the yield must not have reached the bucket: {rec:?}"
        );
    }
    // 3.
    let t_seg = owner::WalSegment {
        log: crate::owner::LogId::parse("z/9:0").expect("log id"),
        first_wal_id: 7,
    };
    let o = owner::read_observed(&store, &root(), PartitionId(11))
        .await
        .unwrap()
        .unwrap();
    let mut rec = o.record.clone();
    rec.wal_segments.push(t_seg.clone());
    owner::write_owner(&store, &root(), PartitionId(11), &rec, Some(o.version))
        .await
        .unwrap()
        .expect("CAS of the unknown segment");
    // 4.
    let (_g, k10) = won(&a, 10).await;
    let on_bucket = owner::read_owner(&store, &root(), PartitionId(10))
        .await
        .unwrap()
        .unwrap();
    assert!(
        k10.wal().segments.is_empty() && on_bucket.wal_segments.is_empty(),
        "p10: the reclaim must apply the pending clean yield: keeper {:?}, bucket {on_bucket:?}",
        k10.wal()
    );
    let (_g, k11) = won(&a, 11).await;
    assert_eq!(
        k11.wal().segments,
        vec![s_seg, t_seg],
        "p11: a record disagreeing with the keeper must keep its segments as they are"
    );
}

/// Two claimers of an empty partition: exactly one wins.
#[tokio::test(flavor = "multi_thread")]
async fn two_claimers_one_wins() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let (ra, rb) = tokio::join!(a.claim(PartitionId(7)), b.claim(PartitionId(7)));
    let wins = [ra.unwrap(), rb.unwrap()]
        .into_iter()
        .filter(|c| matches!(c, Claim::Won(..)))
        .count();
    assert_eq!(wins, 1);
    a.close().await;
    b.close().await;
}

/// A failed yield is retried in the background until it is written; the
/// `owns` hint drops the partition only **after** it is written.
#[tokio::test(flavor = "multi_thread")]
async fn release_retries_until_written_then_unlists() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = session_on(store.clone(), "a").await;
    let (_g, k) = won(&a, 4).await;
    let r = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-4".into()),
        Action::Fail { times: Some(3) },
    ));
    k.release().await;
    assert!(
        a.owned().contains_key(&4),
        "dropped from owns before the yield was done"
    );
    eventually(TTL * 3, "the yield is written", || {
        !a.owned().contains_key(&4)
    })
    .await;
    faulty.remove_rule(r);
    let rec = owner::read_owner(&store, &root(), PartitionId(4))
        .await
        .unwrap()
        .unwrap();
    assert!(rec.released);
    a.close().await;
}

/// A hung background yield retry must not block the session's renewal.
///
/// Interleaving:
/// 1. A holds p4 and p5.
/// 2. From here PUTs of `owners/range-4` hang (a real S3 client retries for up
///    to 180 s).
/// 3. p4's keeper is dropped (the factory failed after `claim`): the yield is
///    handed to the session to retry in the background.
/// 4. The background loop runs that yield and hangs on the PUT.
///
/// Must see: the session still renews — p5 is still valid after several TTLs;
/// once the rule is removed the p4 yield is eventually written. Before the
/// fix, `retry_releases` ran inside `keeper_loop` without a cap: the next
/// renewal never came, A lost its session at ⅔ TTL and every range with it.
#[tokio::test(flavor = "multi_thread")]
async fn hung_background_release_does_not_block_renewal() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let a = session_on(store.clone(), "a").await;
    let a_id = a.current().unwrap();
    let (_g4, k4) = won(&a, 4).await;
    let (_g5, k5) = won(&a, 5).await;
    let hang = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("owners/range-4".into()),
        Action::Hang,
    ));
    drop(k4);
    tokio::time::sleep(TTL * 5).await;
    assert!(
        k5.is_valid(),
        "a yield hanging in the background lost the session"
    );
    assert_eq!(
        a.current(),
        Some(a_id),
        "changed incarnation because of a hung yield"
    );
    faulty.remove_rule(hang);
    eventually(
        TTL * 5,
        "the p4 yield is written once the store heals",
        || !a.owned().contains_key(&4),
    )
    .await;
    let rec = owner::read_owner(&store, &root(), PartitionId(4))
        .await
        .unwrap()
        .unwrap();
    assert!(rec.released);
    a.close().await;
}

/// GC deletes only sessions that have ended, never a live one.
#[tokio::test(flavor = "multi_thread")]
async fn gc_deletes_only_terminal_sessions() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let b = session_on(store.clone(), "b").await;
    let c = session_on(store.clone(), "c").await;
    let (a_id, c_id) = (a.current().unwrap(), c.current().unwrap());
    c.close().await;
    tokio::time::sleep(TTL * (GC_AFTER_TTLS + 3)).await;
    assert_eq!(
        state_of(&store, &c_id).await,
        None,
        "a closed session was not cleaned up"
    );
    assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Live));
    a.close().await;
    b.close().await;
}

/// Two live incarnations of the same node count as one node.
#[tokio::test(flavor = "multi_thread")]
async fn live_nodes_counts_distinct_node_ids() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for inc in [1, 2] {
        let rec = SessionRecord {
            node: "x".into(),
            beat: 1,
            address: None,
            ingress: None,
            knows: None,
            slow: None,
            p99_put_ms: None,
            draining: false,
            owns: BTreeMap::new(),
            state: SessionState::Live,
            mode: SessionMode::Direct,
            challenged: None,
        };
        let id = SessionId {
            node: "x".into(),
            inc,
        };
        store
            .put(&session_path(&root(), &id), PutPayload::from(rec.encode()))
            .await
            .unwrap();
    }
    let a = session_on(store.clone(), "a").await;
    a.tracker().refresh().await.unwrap();
    assert_eq!(
        a.tracker().live_nodes(),
        BTreeSet::from(["a".into(), "x".into()])
    );
    a.close().await;
}

/// A hung renewal ⇒ validity is lost by the clock, at ⅔ TTL, waiting for nobody.
#[tokio::test(flavor = "multi_thread")]
async fn revokes_by_deadline_when_renew_hangs() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let (_g, k) = won(&a, 0).await;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Hang,
    ));
    let took = invalid_within(&k, TTL, "validity not lost while renewals hang").await;
    assert!(took <= TTL.mul_f32(2.0 / 3.0) + PACE * 2);
}

/// A blocked partition thread does not miss the renewal pace: the session runs
/// on the process's multi-threaded runtime.
#[test]
fn renews_while_partition_thread_blocked() {
    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let handle = multi.handle().clone();
    let (a, k, before) = local.block_on(async {
        let a = Session::establish(store.clone(), &root(), cfg("a").with_runtime(handle))
            .await
            .unwrap();
        let (_g, k) = won(&a, 0).await;
        let id = a.current().unwrap();
        let beat = read_session(&store, &root(), &id)
            .await
            .unwrap()
            .unwrap()
            .0
            .unwrap()
            .beat;
        (a, k, beat)
    });
    // Synchronously block the current_thread thread for longer than a TTL.
    std::thread::sleep(TTL + TTL / 2);
    local.block_on(async {
        assert!(
            k.is_valid(),
            "the grant was lost because the partition thread was blocked"
        );
        let id = a.current().unwrap();
        let beat = read_session(&store, &root(), &id)
            .await
            .unwrap()
            .unwrap()
            .0
            .unwrap()
            .beat;
        assert!(beat >= before + 5, "renewal not running: {before} → {beat}");
    });
    multi.block_on(a.close());
}

/// An ended session is **permanent** (`expired`/`closed` never go back to
/// `live`): the tracker reads it once and stops, not GETting it again every
/// round. Under a burst of kills each node keeps a few dead incarnations in its
/// table for `GC_AFTER_TTLS` TTLs, and every observation round would GET them
/// all again.
///
/// 1. The bucket has a `closed` session `x/1`; A opens a session, A's tracker
///    sees `x/1` dead.
/// 2. A runs 10 observation rounds (pace = `PACE`).
///
/// Must see: no GET of `sessions/x/1` by the tracker during those 10 rounds.
#[tokio::test(flavor = "multi_thread")]
async fn terminal_session_is_not_read_again_every_round() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let x = SessionId {
        node: "x".into(),
        inc: 1,
    };
    let rec = SessionRecord {
        node: "x".into(),
        beat: 7,
        address: None,
        ingress: None,
        knows: None,
        slow: None,
        p99_put_ms: None,
        draining: false,
        owns: BTreeMap::new(),
        state: SessionState::Closed,
        mode: SessionMode::Direct,
        challenged: None,
    };
    store
        .put(&session_path(&root(), &x), PutPayload::from(rec.encode()))
        .await
        .unwrap();
    let a = session_on(store.clone(), "a").await;
    let tracker = a.tracker();
    eventually(TTL * 3, "A sees that x/1 has finished", || {
        tracker.is_dead(&x)
    })
    .await;
    let gets = || {
        faulty
            .counts_by_source()
            .into_iter()
            .filter(|(src, op, ..)| *src == "session.tracker" && *op == crate::testkit::Op::Get)
            .map(|(.., n)| n)
            .sum::<u64>()
    };
    let before = gets();
    tokio::time::sleep(PACE * 10).await;
    let after = gets();
    assert!(tracker.is_dead(&x), "x/1 must still be dead");
    assert_eq!(
        after - before,
        0,
        "the tracker GETs the ended session again {} times in 10 rounds",
        after - before
    );
    a.close().await;
}

/// Under the local directory's exclusive lock, a session still `live` at
/// startup belongs to a run that has died — it expires at once, even when the
/// `expired` CAS answers ambiguously (resolved by re-reading).
///
/// 1. A opens a session, claims p0, then `abort`s (like `kill -9`: the record
///    stays `live`).
/// 2. B opens a session, then `close`s (the record is `closed`).
/// 3. The next PUT of `sessions/a/` answers ambiguously; call
///    `expire_sessions_under_lock`.
/// 4. C opens a session and claims p0.
///
/// Must see: only A is marked, A is `expired`, B stays `closed`, and C claims
/// p0 at once — without waiting for a TTL.
#[tokio::test(flavor = "multi_thread")]
async fn sessions_expire_at_once_under_the_local_lock() {
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().expect("alive");
        let _ = won(&a, 0).await;
        a.abort();
        // A renewal in flight at `abort` (if any) lands before we read.
        tokio::time::sleep(PACE * 3).await;
        let b = session_on(store.clone(), "b").await;
        let b_id = b.current().expect("alive");
        b.close().await;

        assert_eq!(
            state_of(&store, &a_id).await,
            Some(SessionState::Live),
            "{reply:?}"
        );
        faulty.add_rule(Rule::ambiguous_put("sessions/a/", reply));
        let proof = crate::local::LocalLock(Vec::new());
        let expired = expire_sessions_under_lock(&store, &root(), &proof)
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
        assert_eq!(expired, vec![a_id.clone()], "{reply:?}");
        assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Expired));
        assert_eq!(state_of(&store, &b_id).await, Some(SessionState::Closed));

        let c = Session::establish(
            store.clone(),
            &root(),
            cfg("c").with_ttl(Duration::from_secs(60)),
        )
        .await
        .unwrap();
        let t0 = Instant::now();
        let _ = won(&c, 0).await;
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "{reply:?}: claimed p0 after {:?}",
            t0.elapsed()
        );
        c.close().await;
    }
}

/// `cairnflow_failstop_total{cause}` counts **once** per abandoned
/// incarnation, by the cause the caller gives: a second `fail_stop` on the same
/// (already lost) incarnation does not count again. Lost because a renewal
/// missed its deadline ⇒ the label is `session_expired`.
///
/// `Cause::Other` is used for the exact count: nothing else in this binary
/// produces it (the `NodeWal` shutdown carrying `Other` does not reach
/// `Session`), so tests sharing the process do not skew the number.
#[tokio::test(flavor = "multi_thread")]
async fn fail_stop_counts_once_per_incarnation_by_cause() {
    use crate::failstop::{count, Cause};
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let a = session_on(faulty.clone(), "a").await;
    let first = a.current().unwrap();
    let before = count(Cause::Other);
    a.fail_stop(&first, Cause::Other, "test").await;
    a.fail_stop(&first, Cause::Other, "test, second time").await;
    assert_eq!(count(Cause::Other), before + 1);

    eventually(TTL * 3, "A opens a new incarnation", || {
        a.current().is_some_and(|id| id != first)
    })
    .await;
    let expired = count(Cause::SessionExpired);
    let (_g, k) = won(&a, 0).await;
    faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("sessions/".into()),
        Action::Fail { times: None },
    ));
    tokio::time::timeout(TTL * 2, k.lost())
        .await
        .expect("session lost");
    // The grants are revoked **before** counting (`lose`): wait, do not read at once.
    eventually(
        TTL,
        "lost by deadline without counting session_expired",
        || count(Cause::SessionExpired) > expired,
    )
    .await;
    assert_eq!(count(Cause::Other), before + 1);
    faulty.clear_rules();
    a.close().await;
}

/// The test's wall clock: `None` = real time, `Some(t)` = stopped at `t`
/// (installed through [`crate::entropy::install`] for the current thread — a
/// `current_thread` runtime).
#[derive(Default)]
struct TestClock(std::cell::Cell<Option<std::time::SystemTime>>);

impl crate::entropy::Source for TestClock {
    #[allow(
        clippy::disallowed_methods,
        reason = "real time when the test sets no time"
    )]
    fn now(&self) -> std::time::SystemTime {
        self.0.get().unwrap_or_else(std::time::SystemTime::now)
    }
    fn process_id(&self) -> u32 {
        1
    }
    fn fill(&self, buf: &mut [u8]) {
        // No real randomness needed: distinct enough between calls.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        for chunk in buf.chunks_mut(8) {
            let n = NEXT.fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed);
            chunk.copy_from_slice(&n.to_le_bytes()[..chunk.len()]);
        }
    }
}

/// `inc` must not be taken again from the wall clock.
///
/// 1. A opens session `a/<inc₁>`.
/// 2. A closes; the session record is cleaned up like the tracker's GC (after
///    `GC_AFTER_TTLS` TTLs) — while the shared WAL log `nodewal/a/<inc₁>/…` may
///    remain (seal, floor).
/// 3. The node's wall clock goes back to exactly `inc₁` ms (NTP stepping back,
///    a VM restored from a snapshot).
/// 4. A restarts with the same `node_id`.
///
/// Must see: the new incarnation is greater than `inc₁`. Before the fix the
/// lower bound was `max(listed + 1, now_ms)` = `inc₁` (the LIST no longer sees
/// the old session): a duplicate incarnation ⇒ the new log has the old log's
/// name, writes below its GC floor, and whoever seals the old log misses acked
/// rows (found by the simulation, seed 12).
#[tokio::test]
async fn restarted_node_never_reuses_incarnation_after_session_gc() {
    let clock = std::rc::Rc::new(TestClock::default());
    let _clock = crate::entropy::install(clock.clone());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = session_on(store.clone(), "a").await;
    let first = a.current().expect("alive");
    a.close().await;
    store
        .delete(&record::session_path(&root(), &first))
        .await
        .unwrap();
    clock.0.set(Some(
        std::time::UNIX_EPOCH + Duration::from_millis(first.inc),
    ));
    let again = session_on(store.clone(), "a").await;
    let second = again.current().expect("alive");
    again.close().await;
    assert!(
        second.inc > first.inc,
        "{first} then {second}: incarnation reused after the session cleanup and a clock step back"
    );
}

/// Ambiguity for the incarnation marker: the marker's `Create` **has reached**
/// the store but returned an error (`Error`), or the `AlreadyExists` of
/// object_store's own retry (`Retried`) — resolved by re-reading.
///
/// Must see: the marker is recognized as ours (its contents carry the open's
/// nonce), the session opens at **exactly** the marker's `inc` — no jump to
/// `inc + 1` leaving an unused marker, no failed `establish`.
#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_incarnation_marker_is_resolved_by_reading_back() {
    use futures::TryStreamExt;
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        faulty.add_rule(Rule::ambiguous_put("incarnations/a/", reply));
        let a = Session::establish(store.clone(), &root(), cfg("a"))
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: marker written, establish failed: {e}"));
        let me = a.current().expect("a live session");
        let markers: Vec<_> = store
            .list(Some(&Path::from("incarnations/a")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(markers.len(), 1, "{reply:?}: {markers:?}");
        assert_eq!(
            markers[0].location,
            incarnation::marker_path(&root(), &me),
            "{reply:?}"
        );
        a.close().await;
    }
}

/// The marker is a lower bound, and only the largest marker stays.
///
/// 1. A opens and closes three times; after each, the session record is
///    cleaned up.
/// 2. The clock stands still at a mark **before** all three (no lower bound
///    from the time).
///
/// Must see: `inc` increases strictly across the three opens; the bucket keeps
/// only the last open's marker (older markers are cleaned up once a newer one
/// exists).
#[tokio::test]
async fn incarnation_markers_bound_inc_and_only_the_newest_stays() {
    use futures::TryStreamExt;
    let clock = std::rc::Rc::new(TestClock::default());
    let _clock = crate::entropy::install(clock.clone());
    clock
        .0
        .set(Some(std::time::UNIX_EPOCH + Duration::from_secs(1)));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut seen: Vec<SessionId> = Vec::new();
    for _ in 0..3 {
        let a = session_on(store.clone(), "a").await;
        let id = a.current().expect("alive");
        a.close().await;
        store
            .delete(&record::session_path(&root(), &id))
            .await
            .unwrap();
        if let Some(prev) = seen.last() {
            assert!(id.inc > prev.inc, "{prev} then {id}");
        }
        seen.push(id);
    }
    let markers: Vec<_> = store
        .list(Some(&Path::from("incarnations/a")))
        .try_collect()
        .await
        .unwrap();
    let last = seen.last().unwrap();
    assert_eq!(
        markers
            .iter()
            .map(|m| m.location.clone())
            .collect::<Vec<_>>(),
        vec![incarnation::marker_path(&root(), last)],
        "only the largest marker stays"
    );
}

/// The "dead" verdict of `dead_unexpirable` has no fence on the bucket.
///
/// 1. A is alive, renewing steadily on the real store, holding range 0.
/// 2. B looks at `sessions/node-a/` through a **torn and stale** object
///    ([`Action::Tear`]: GET returns a frozen copy, the etag flips; the CAS
///    misses) — B never sees A's beat increase, and cannot CAS `expired`.
/// 3. Past [`UNEXPIRABLE_AFTER_TTLS`] TTLs, B considers A dead and `claim`s
///    range 0.
///
/// Must see: B never wins range 0 while A's grant is still valid. Before the
/// fix: B won (only B's clock decided, nothing on the bucket stopped A from
/// renewing) ⇒ two nodes believed they led range 0 (LS-3).
#[tokio::test(flavor = "multi_thread")]
async fn torn_view_of_a_live_owner_never_leads_to_claim() {
    let shared = Arc::new(InMemory::new());
    let a = session_on(shared.clone(), "node-a").await;
    let (a_grant, _a_keeper) = won(&a, 0).await;
    let a_valid = a_grant.validity_handle();
    let b_store = FaultyStore::wrap(shared.clone());
    let b = session_on(b_store.clone(), "node-b").await;
    eventually(TTL * 2, "B sees A", || {
        b.tracker().live_nodes().contains("node-a")
    })
    .await;
    b_store.add_rule(Rule::new(
        OpSet::GET | OpSet::HEAD | OpSet::PUT,
        PathMatch::Contains("sessions/node-a".into()),
        Action::Tear(crate::testkit::Tear::new()),
    ));
    let t0 = Instant::now();
    let mut won_after = None;
    while t0.elapsed() < TTL * (UNEXPIRABLE_AFTER_TTLS + 5) {
        if let Claim::Won(..) = b.claim(PartitionId(0)).await.unwrap() {
            assert!(
                !a_valid.is_valid(),
                "B claimed range 0 after {:?} while A's grant was still valid",
                t0.elapsed()
            );
            won_after = Some(t0.elapsed());
            break;
        }
        tokio::time::sleep(PACE).await;
    }
    // Liveness: the overwrite fences A (its next renewal misses ⇒ `lose`), then
    // one TTL later the `expired` CAS wins — B claims the range.
    assert!(won_after.is_some(), "B never claims range 0");
    b.close().await;
    a.close().await;
}

// ───────────────────────────────── a node alone (LS-16)

mod solo {
    use super::*;

    /// Turns the `control/quiet-lease` gate on on the bucket (like `cairn-ctl
    /// cluster enable-quiet-lease --apply` after checking `knows`).
    pub(super) async fn gate_on(store: &Arc<dyn ObjectStore>) {
        let q = crate::quiet_lease::QuietLease {
            on: true,
            knows: crate::migrate::QUIET_LEASE,
            by: "test".into(),
            at_ms: 0,
            ..Default::default()
        };
        assert!(crate::quiet_lease::write(store, &root(), &q, None)
            .await
            .unwrap());
    }

    /// The session record `id` on the bucket (decoded).
    pub(super) async fn record(store: &Arc<dyn ObjectStore>, id: &SessionId) -> SessionRecord {
        read_session(store, &root(), id)
            .await
            .unwrap()
            .expect("the record must exist")
            .0
            .expect("the record must decode")
    }

    /// Waits until `s`'s record on the bucket is at `mode=solo` without a challenge.
    pub(super) async fn until_solo(store: &Arc<dyn ObjectStore>, s: &Session, bound: Duration) {
        let id = s.current().unwrap();
        let t0 = Instant::now();
        loop {
            let r = record(store, &id).await;
            if r.mode == SessionMode::Solo && r.challenged.is_none() {
                return;
            }
            assert!(
                t0.elapsed() < bound,
                "over {bound:?}: alone, gate on, record not in mode=solo: {r:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn session_puts(writes: &[(crate::testkit::Op, Path)], node: &str) -> usize {
        writes
            .iter()
            .filter(|(op, p)| {
                *op == crate::testkit::Op::Put
                    && p.as_ref().starts_with(&format!("sessions/{node}/"))
            })
            .count()
    }

    fn session_lists(f: &FaultyStore) -> u64 {
        f.counts_by_source_object()
            .iter()
            .filter(|(_, op, _, kind, _)| *op == crate::testkit::Op::List && *kind == "sessions")
            .map(|(.., n)| n)
            .sum()
    }

    /// `solo_node_sends_no_session_put_while_alone` (LS-16).
    ///
    /// 1. The gate is on; A is the only session on the bucket, holding p3.
    /// 2. A CASes its record to `mode=solo` (one `direct` renewal), then stops
    ///    PUTting.
    ///
    /// Must see: during 10·TTL after entering the mode, **no** PUT to A's
    /// session record, and p3's grant valid throughout (renewed by a
    /// self-checking `If-None-Match` GET, `304`). Before quiet leases: one PUT
    /// per renewal pace.
    #[tokio::test(flavor = "multi_thread")]
    async fn solo_node_sends_no_session_put_while_alone() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        faulty.record_writes();
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 10 {
            assert!(
                k.is_valid(),
                "A's grant lost validity after {:?} in solo mode",
                t0.elapsed()
            );
            tokio::time::sleep(PACE / 2).await;
        }
        let puts = session_puts(&faulty.take_writes(), "a");
        assert_eq!(
            puts, 0,
            "a node alone still PUTs its session record {puts} times in 10·TTL"
        );
        a.close().await;
    }

    /// `solo_node_sends_no_session_list_while_alone`: in solo mode the tracker
    /// stops LISTing `sessions/` every TTL (a newcomer always challenges, and the
    /// challenge changes the etag so the owner sees it through the
    /// self-checking GET); only the 10-minute net remains. Must see: 0 LISTs of
    /// `sessions/` in the 10·TTL after entering the mode.
    #[tokio::test(flavor = "multi_thread")]
    async fn solo_node_sends_no_session_list_while_alone() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        until_solo(&store, &a, TTL * 4).await;
        // An observation round in flight when entering the mode may finish.
        tokio::time::sleep(PACE * 2).await;
        let before = session_lists(&faulty);
        tokio::time::sleep(TTL * 10).await;
        let lists = session_lists(&faulty) - before;
        assert_eq!(
            lists, 0,
            "a node alone still LISTs sessions/ {lists} times in 10·TTL"
        );
        a.close().await;
    }

    /// `restarted_incarnation_takes_over_after_challenge_and_ttl`.
    ///
    /// 1. A (node `a`) is alone at `mode=solo`, holding p3.
    /// 2. A's process dies (`abort`: writes nothing, the record stays
    ///    `live solo`).
    /// 3. A new process of the **same** node opens incarnation B; B's tracker
    ///    sees A's `solo` record not ended.
    /// 4. B CASes in `challenged=<B>` (τ = when the CAS is confirmed), waits a
    ///    TTL with no answer, CASes `expired`, then claims p3.
    ///
    /// Must see: B's `challenged` line on A's record; A marked `expired` **no
    /// sooner** than a TTL from when that line appeared (the silence rule LS-1
    /// does not apply to `solo`: A has been silent since entering the mode); B
    /// claims p3.
    #[tokio::test(flavor = "multi_thread")]
    async fn restarted_incarnation_takes_over_after_challenge_and_ttl() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        // A is silent (no PUT) since entering the mode — for longer than a TTL.
        tokio::time::sleep(TTL * 2).await;
        a.abort();
        std::mem::forget(k);
        let b = session_on(store.clone(), "a").await;
        let t0 = Instant::now();
        let challenged_at = loop {
            let r = record(&store, &a_id).await;
            assert!(
                r.state == SessionState::Live,
                "A marked {:?} before being challenged (the silence rule applied to solo)",
                r.state
            );
            if r.challenged.is_some() {
                break Instant::now();
            }
            assert!(
                t0.elapsed() < TTL * 3,
                "B does not challenge A's solo record"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let expired_at = loop {
            if state_of(&store, &a_id).await == Some(SessionState::Expired) {
                break Instant::now();
            }
            assert!(
                challenged_at.elapsed() < TTL * 4,
                "B does not expire A after the challenge"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        let took = expired_at - challenged_at;
        assert!(
            took >= TTL - PACE,
            "A expired {took:?} after the challenge, sooner than a TTL"
        );
        let _ = won(&b, 3).await;
        b.close().await;
    }

    /// `second_process_same_node_challenges_and_both_go_direct_without_pingpong`.
    ///
    /// 1. A is alone at `solo`, holding p3, **still alive**.
    /// 2. A second process of the same node (two pods during a rolling update)
    ///    opens B.
    /// 3. B challenges; A's next self-checking GET sees `challenged` (not a
    ///    renewal), A CASes back to `direct`, `beat+1`, dropping the line —
    ///    the answer.
    /// 4. B sees the beat increase: A is alive. From here the two sessions are an
    ///    ordinary cluster.
    ///
    /// Must see: A keeps its incarnation and p3 throughout (the grant is not
    /// lost), B never expires A; after the answer, in 10·TTL no record goes
    /// back to `solo` (no ping-pong).
    #[tokio::test(flavor = "multi_thread")]
    async fn second_process_same_node_challenges_and_both_go_direct_without_pingpong() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        let b = session_on(store.clone(), "a").await;
        let b_id = b.current().unwrap();
        let t0 = Instant::now();
        let mut answered = false;
        while t0.elapsed() < TTL * 12 {
            assert!(!has_lost(&k), "A lost p3 after {:?}", t0.elapsed());
            assert_eq!(a.current(), Some(a_id.clone()), "A changed incarnation");
            let ra = record(&store, &a_id).await;
            assert_eq!(ra.state, SessionState::Live, "B expired a live A");
            let rb = record(&store, &b_id).await;
            if answered {
                assert_eq!(
                    ra.mode,
                    SessionMode::Direct,
                    "A went back to solo while B is alive"
                );
                assert_eq!(
                    rb.mode,
                    SessionMode::Direct,
                    "B entered solo while A is alive"
                );
            } else if ra.mode == SessionMode::Direct && ra.challenged.is_none() && ra.beat > 2 {
                answered = true;
            }
            tokio::time::sleep(PACE / 2).await;
        }
        assert!(answered, "A did not answer the challenge");
        assert!(!b.tracker().is_dead(&a_id));
        assert!(held(&b, 3).await.is_some(), "B claimed p3 of a live A");
        a.close().await;
        b.close().await;
    }

    /// `joining_node_challenges_and_cluster_forms`: as above with a
    /// **different** node joining. Must see: A answers, both see each other alive
    /// (`live_nodes`), A keeps p3, B does not get p3.
    #[tokio::test(flavor = "multi_thread")]
    async fn joining_node_challenges_and_cluster_forms() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        let b = session_on(store.clone(), "b").await;
        eventually(TTL * 4, "A answers, both see each other", || {
            a.tracker().live_nodes().contains("b") && b.tracker().live_nodes().contains("a")
        })
        .await;
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 4 {
            assert!(!has_lost(&k), "A lost p3");
            tokio::time::sleep(PACE).await;
        }
        let r = record(&store, &a_id).await;
        assert_eq!(
            (r.state, r.mode.clone()),
            (SessionState::Live, SessionMode::Direct),
            "{r:?}"
        );
        assert!(held(&b, 3).await.is_some(), "B claimed p3 of a live A");
        a.close().await;
        b.close().await;
    }

    /// `frozen_solo_node_ranges_move_after_ttl_not_before`.
    ///
    /// 1. A is alone at `solo`, holding p3.
    /// 2. A freezes (`CrashHandle`: the background loop stops, the grant is
    ///    pinned — the process believes it still leads).
    /// 3. B (another node) joins, challenges, waits a TTL, expires A, claims p3.
    ///
    /// Must see: B does **not** get p3 before a TTL from the challenge; after
    /// that it does.
    #[tokio::test(flavor = "multi_thread")]
    async fn frozen_solo_node_ranges_move_after_ttl_not_before() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        k.crash_handle().crash();
        std::mem::forget(k);
        let b = session_on(store.clone(), "b").await;
        let t0 = Instant::now();
        let mut challenged_at = None;
        loop {
            if challenged_at.is_none() && record(&store, &a_id).await.challenged.is_some() {
                challenged_at = Some(Instant::now());
            }
            if let Claim::Won(..) = b.claim(PartitionId(3)).await.unwrap() {
                let at = challenged_at.expect("B claimed p3 without a challenge");
                assert!(
                    at.elapsed() >= TTL - PACE,
                    "B claimed p3 {:?} after the challenge, sooner than a TTL",
                    at.elapsed()
                );
                break;
            }
            assert!(t0.elapsed() < TTL * 6, "B never claims p3");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.close().await;
    }

    /// `solo_node_cut_from_bucket_pauses_then_loses`: A's self-checking GET (and
    /// every request to the session record) fails. Must see: the grant loses
    /// validity within ⅔·TTL + one pace (no GET renews it), suspended but not
    /// lost, then lost at the suspension cap — like a failing renewal PUT (LS-2).
    #[tokio::test(flavor = "multi_thread")]
    async fn solo_node_cut_from_bucket_pauses_then_loses() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let (_g, k) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        faulty.add_rule(Rule::new(
            OpSet::ALL,
            PathMatch::Contains("sessions/a/".into()),
            Action::Fail { times: None },
        ));
        let took = invalid_within(
            &k,
            TTL.mul_f32(2.0 / 3.0) + PACE * 3,
            "A loses validity when cut from the bucket",
        )
        .await;
        assert!(
            !has_lost(&k),
            "A lost at once after {took:?} instead of suspending"
        );
        let t0 = Instant::now();
        while !has_lost(&k) {
            assert!(
                !k.is_valid(),
                "the grant revived without reaching the bucket"
            );
            assert!(
                t0.elapsed() < TTL * 2,
                "A never drops its ranges despite being cut from the bucket"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        a.close().await;
    }

    /// `ambiguous_challenge_cas_resolves_by_reading_back` (ambiguity resolved by
    /// re-reading).
    ///
    /// 1. A is alone at `solo`, then dies (`abort`).
    /// 2. B's challenge CAS reaches the store **and is applied**, but B gets an
    ///    ambiguous answer: the `Precondition` of object_store's own retry, or an
    ///    error.
    ///
    /// Must see: B re-reads, recognizes its own `challenged` line, counts the TTL
    /// from there, then expires A and claims p3 — not stuck, no stacked
    /// challenges.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_challenge_cas_resolves_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let shared = Arc::new(InMemory::new());
            let store: Arc<dyn ObjectStore> = shared.clone();
            gate_on(&store).await;
            let a = session_on(store.clone(), "a").await;
            let a_id = a.current().unwrap();
            let (_g, k) = won(&a, 3).await;
            until_solo(&store, &a, TTL * 4).await;
            a.abort();
            std::mem::forget(k);
            let b_store = FaultyStore::wrap(shared.clone());
            b_store.add_rule(Rule::ambiguous_put(
                &format!("sessions/a/{}", a_id.inc),
                reply,
            ));
            let b = session_on(b_store.clone(), "b").await;
            let t0 = Instant::now();
            loop {
                if let Claim::Won(..) = b.claim(PartitionId(3)).await.unwrap() {
                    break;
                }
                assert!(t0.elapsed() < TTL * 6, "{reply:?}: B does not claim p3");
                tokio::time::sleep(PACE).await;
            }
            let (_, err, retried) = b_store.injected();
            assert_eq!(
                err + retried,
                1,
                "{reply:?}: the ambiguity rule did not fire"
            );
            assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Expired));
            b.close().await;
        }
    }

    /// `challenge_racing_owns_write_is_retried`.
    ///
    /// 1. A is alone at `solo`; B reads A's `solo` record (etag `E1`).
    /// 2. B's challenge CAS (If-Match `E1`) is held at a gate on its way to the
    ///    store; meanwhile A claims p4 — A's `owns` write (CAS, `beat+1`) changes
    ///    the record to `E2`.
    /// 3. The gate opens: B's CAS misses (`Precondition`).
    ///
    /// Must see: B re-reads (the beat increased), challenges again on `E2`, A
    /// answers; A keeps both p3 and p4, is not expired.
    #[tokio::test(flavor = "multi_thread")]
    async fn challenge_racing_owns_write_is_retried() {
        let shared = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        gate_on(&store).await;
        let a = session_on(store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g3, k3) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        let b_store = FaultyStore::wrap(shared.clone());
        let gate = Gate::closed();
        let rule = b_store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(format!("sessions/a/{}", a_id.inc)),
            Action::GateAfter {
                pass: 0,
                gate: gate.clone(),
            },
        ));
        let b = session_on(b_store.clone(), "b").await;
        eventually(TTL * 3, "B's challenge CAS reaches the gate", || {
            gate.blocked() > 0
        })
        .await;
        let (_g4, k4) = won(&a, 4).await;
        eventually(TTL * 2, "A writes owns with p4", || {
            futures::executor::block_on(record(&store, &a_id))
                .owns
                .contains_key(&4)
        })
        .await;
        gate.open();
        b_store.remove_rule(rule);
        eventually(TTL * 4, "A answers the repeated challenge", || {
            b.tracker().live_nodes().contains("a") && a.tracker().live_nodes().contains("b")
        })
        .await;
        tokio::time::sleep(TTL * 2).await;
        assert!(!has_lost(&k3) && !has_lost(&k4), "A lost ranges");
        assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Live));
        let r = record(&store, &a_id).await;
        assert_eq!(r.mode, SessionMode::Direct, "{r:?}");
        a.close().await;
        b.close().await;
    }

    /// The reverse direction of `challenge_racing_owns_write_is_retried` — found
    /// in a chaos run.
    ///
    /// 1. A is alone at `solo` (etag `E1`), holding p3. B (a new node) reads A's
    ///    `solo` record; B's challenge CAS (If-Match `E1`) is held at gate `gb`.
    /// 2. A claims p4 ⇒ `needs_write` ⇒ a CAS keeping `solo`, `beat+1`, If-Match
    ///    `E1` — held at gate `ga`, not at the store yet.
    /// 3. Open `gb`: B's challenge applies on `E1` — A's record verbatim plus
    ///    `challenged=<B>`, the beat unchanged.
    /// 4. Open `ga`: A's CAS misses (`Precondition`); A re-reads and finds its own
    ///    `live` record, the old beat, carrying the `challenged` line.
    ///
    /// Must see: A **answers** (a CAS back to `direct` from the etag just read —
    /// as when `self_check` sees that line), keeps its incarnation, p3 and p4; B
    /// does not expire A. Before this test's `fix:`, `renew_once`'s re-read
    /// branch took an unknown beat as "the session was changed by someone else"
    /// ⇒ `lose`, and the whole node dropped every range.
    #[tokio::test(flavor = "multi_thread")]
    async fn owns_write_racing_challenge_answers_instead_of_losing() {
        let shared = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        gate_on(&store).await;
        let a_store = FaultyStore::wrap(shared.clone());
        let a = session_on(a_store.clone(), "a").await;
        let a_id = a.current().unwrap();
        let (_g3, k3) = won(&a, 3).await;
        until_solo(&store, &a, TTL * 4).await;
        let own_record = format!("sessions/a/{}", a_id.inc);
        let gate_rule = |gate: &Gate| {
            Rule::new(
                OpSet::PUT,
                PathMatch::Contains(own_record.clone()),
                Action::GateAfter {
                    pass: 0,
                    gate: gate.clone(),
                },
            )
        };
        let gb = Gate::closed();
        let b_store = FaultyStore::wrap(shared.clone());
        let rb = b_store.add_rule(gate_rule(&gb));
        let b = session_on(b_store.clone(), "b").await;
        eventually(TTL * 3, "B's challenge CAS reaches the gate", || {
            gb.blocked() > 0
        })
        .await;
        let ga = Gate::closed();
        let ra = a_store.add_rule(gate_rule(&ga));
        let (_g4, k4) = won(&a, 4).await;
        eventually(
            TTL * 2,
            "A's CAS keeping solo (owns with p4) reaches the gate",
            || ga.blocked() > 0,
        )
        .await;
        gb.open();
        eventually(
            TTL,
            "B's challenge is applied on top of A's solo record",
            || {
                futures::executor::block_on(record(&store, &a_id))
                    .challenged
                    .is_some()
            },
        )
        .await;
        b_store.remove_rule(rb);
        ga.open();
        a_store.remove_rule(ra);
        eventually(TTL * 4, "A answers, both see each other", || {
            b.tracker().live_nodes().contains("a") && a.tracker().live_nodes().contains("b")
        })
        .await;
        tokio::time::sleep(TTL * 2).await;
        assert_eq!(a.current(), Some(a_id.clone()), "A changed incarnation");
        assert!(!has_lost(&k3) && !has_lost(&k4), "A lost ranges");
        assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Live));
        let r = record(&store, &a_id).await;
        assert_eq!(r.mode, SessionMode::Direct, "{r:?}");
        assert!(r.owns.contains_key(&4), "{r:?}");
        a.close().await;
        b.close().await;
    }

    /// Ambiguity for the **CAS entering the mode**: A's `mode=solo` CAS reaches
    /// the store but the answer is ambiguous. Must see: A re-reads, accepts its
    /// own `solo` record as confirmed (the self-checking GET compares only with
    /// that record's etag), then stops PUTting; the same incarnation, the grant
    /// valid throughout.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_enter_solo_cas_is_resolved_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            gate_on(&store).await;
            // The first PUT to `sessions/a/` is the incarnation's `Create`; the
            // next is the CAS entering the mode.
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains("sessions/a/".into()),
                Action::Ambiguous {
                    reply,
                    skip: 1,
                    times: Some(1),
                },
            ));
            let a = session_on(store.clone(), "a").await;
            let a_id = a.current().unwrap();
            let (_g, k) = won(&a, 3).await;
            until_solo(&store, &a, TTL * 4).await;
            tokio::time::sleep(TTL).await;
            faulty.record_writes();
            let t0 = Instant::now();
            while t0.elapsed() < TTL * 4 {
                assert!(
                    k.is_valid(),
                    "{reply:?}: the grant lost validity after the ambiguous CAS entering the mode"
                );
                tokio::time::sleep(PACE / 2).await;
            }
            assert_eq!(
                session_puts(&faulty.take_writes(), "a"),
                0,
                "{reply:?}: renewal PUTs still happen"
            );
            assert_eq!(a.current(), Some(a_id), "{reply:?}: A changed incarnation");
            a.close().await;
        }
    }

    /// Ambiguity for the **answer CAS**: A is at `solo`, B challenges, A's CAS
    /// back to `direct` reaches the store but the answer is ambiguous. Must see:
    /// A re-reads and finds its own beat, keeps its incarnation and p3; B does
    /// not expire A.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_answer_cas_is_resolved_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            gate_on(&store).await;
            let a = session_on(store.clone(), "a").await;
            let a_id = a.current().unwrap();
            let (_g, k) = won(&a, 3).await;
            until_solo(&store, &a, TTL * 4).await;
            // A at `solo` PUTs nothing: A's next PUT to its record is the
            // answer. B writes its challenge through its own store (not through
            // the rule).
            let r = faulty.add_rule(Rule::ambiguous_put(
                &format!("sessions/a/{}", a_id.inc),
                reply,
            ));
            let b = session_on(faulty.inner().clone(), "b").await;
            eventually(TTL * 4, "A answers, both see each other", || {
                a.tracker().live_nodes().contains("b") && b.tracker().live_nodes().contains("a")
            })
            .await;
            tokio::time::sleep(TTL * 2).await;
            let (_, err, retried) = faulty.injected();
            assert_eq!(
                err + retried,
                1,
                "{reply:?}: the ambiguity rule did not fire"
            );
            faulty.remove_rule(r);
            assert!(!has_lost(&k), "{reply:?}: A lost p3");
            assert_eq!(
                a.current(),
                Some(a_id.clone()),
                "{reply:?}: A changed incarnation"
            );
            assert_eq!(state_of(&store, &a_id).await, Some(SessionState::Live));
            a.close().await;
            b.close().await;
        }
    }
}

/// Delegated renewal at the session layer (LS-14/LS-15): the coordinator is a
/// session holding the role (`try_coordinate`) and its tracker; heartbeats and
/// answers are fed in directly by the test (`Session::on_lease_answer`,
/// `SessionTracker::heard`) — the network and scheduling are in
/// `cairn-server/src/control.rs`.
mod delegation {
    use super::*;

    /// `a` holds the coordinator role; returns the term.
    async fn coordinate(a: &Session) -> u64 {
        a.try_coordinate()
            .await
            .unwrap()
            .expect("holds the coordinator role")
    }

    /// `b` receives `coord`'s permission (term `term`) and CASes into delegated
    /// mode; waits for that CAS to be confirmed.
    async fn delegate(b: &Session, coord: &SessionId, term: u64) {
        let beat = b.self_report().expect("b alive").beat;
        b.on_lease_answer(coord, term, LeaseAnswer::Grant, Instant::now(), beat);
        eventually(TTL * 2, "b enters mode=delegated", || {
            b.delegated_to() == Some((coord.clone(), term))
        })
        .await;
    }

    /// One ack from `coord` for `b`'s heartbeat sent at `sent`.
    fn ack(b: &Session, coord: &SessionId, term: u64, sent: Instant) {
        let beat = b.self_report().map_or(0, |r| r.beat);
        b.on_lease_answer(coord, term, LeaseAnswer::Ack, sent, beat);
    }

    /// Ack `b` steadily every `PACE` until `stop` — like a healthy coordinator.
    fn ack_loop(
        b: Session,
        coord: SessionId,
        term: u64,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                ack(&b, &coord, term, Instant::now());
                tokio::time::sleep(PACE).await;
            }
        })
    }

    /// (Model M1's `ack_after_own_lose_breaks_ls3`): after `lose` — past the
    /// suspension cap — the grant is revoked for good, and no ack revives it.
    ///
    /// Interleaving:
    /// 1. A holds the role; B delegates to A, holds range 0, acked steadily.
    /// 2. Acks stop, and every PUT of B hangs (the fallback CAS cannot get
    ///    through): B's grant is suspended at ⅔·TTL, lost at the suspension cap
    ///    — B `lose`s, writing `closed`.
    /// 3. A **new** ack (sent "now") from A for B's old incarnation arrives.
    ///
    /// Must see: range 0's grant does not become valid again; B no longer has
    /// the old incarnation.
    #[tokio::test(flavor = "multi_thread")]
    async fn ack_after_pause_cap_never_revives() {
        let shared = Arc::new(InMemory::new());
        let a = session_on(shared.clone(), "node-a").await;
        let b_store = FaultyStore::wrap(shared.clone());
        let b = session_on(b_store.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let (_g, keeper) = won(&b, 0).await;
        let old = b.current().unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        tokio::time::sleep(TTL).await;
        assert!(keeper.is_valid(), "the grant ended while still acked");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        b_store.add_rule(Rule::new(OpSet::PUT, PathMatch::Any, Action::Hang));
        eventually(
            TTL * 3,
            "B loses its incarnation at the suspension cap",
            || b.current().as_ref() != Some(&old),
        )
        .await;
        assert!(!keeper.is_valid());
        // A late ack for the old incarnation: nothing comes back to life.
        b.on_lease_answer(&a_id, term, LeaseAnswer::Ack, Instant::now(), u64::MAX);
        for beat in 0..8 {
            b.on_lease_answer(&a_id, term, LeaseAnswer::Ack, Instant::now(), beat);
        }
        tokio::time::sleep(PACE).await;
        assert!(!keeper.is_valid(), "an ack after lose revived the grant");
        a.close().await;
    }

    /// (Model M1's `ack_before_delegation_confirmed_breaks_ls3`): an ack only
    /// renews once the delegation CAS **has been confirmed**.
    ///
    /// Interleaving:
    /// 1. A holds the role; B holds range 0 at `direct`.
    /// 2. Every PUT to B's session record is blocked at a gate; A permits
    ///    delegation: B's keeper issues the `mode=delegated` CAS — stuck at the
    ///    gate.
    /// 3. B's grant loses validity (⅔·TTL, suspended); meanwhile A's acks keep
    ///    arriving.
    ///
    /// Must see: range 0's grant does **not** become valid again before the gate
    /// opens; once the gate opens the CAS lands, the re-read finds B's delegated
    /// record, and from then on new acks renew.
    #[tokio::test(flavor = "multi_thread")]
    async fn ack_before_delegation_cas_lands_does_not_extend() {
        let shared = Arc::new(InMemory::new());
        let a = session_on(shared.clone(), "node-a").await;
        let b_store = FaultyStore::wrap(shared.clone());
        let b = session_on(b_store.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        let (_g, keeper) = won(&b, 0).await;
        let gate = Gate::closed();
        let rule = b_store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(format!("sessions/node-b/{}", b_id.inc)),
            Action::Gate(gate.clone()),
        ));
        let beat = b.self_report().unwrap().beat;
        b.on_lease_answer(&a_id, term, LeaseAnswer::Grant, Instant::now(), beat);
        invalid_within(&keeper, TTL, "the grant ends when every renewal is stuck").await;
        // Within the suspension (cap: the last issue + 4/3·TTL), early enough
        // that the CAS can still revive it when the gate opens.
        let t0 = Instant::now();
        while t0.elapsed() < TTL / 6 {
            for beat in beat..beat + 4 {
                b.on_lease_answer(&a_id, term, LeaseAnswer::Ack, Instant::now(), beat);
            }
            assert!(
                !keeper.is_valid(),
                "an ack renewed the grant before the delegation CAS was confirmed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b_store.remove_rule(rule);
        gate.open();
        // The stuck CAS reaching the store is either the delegation CAS —
        // confirmed, then falling back at once (the fallback mark is when it was
        // issued, already past TTL/3, no ack yet) —, or the `direct` renewal in
        // flight when the permission arrived (the keeper serializes). Either
        // way: B keeps its session. After the re-delegation delay, the next
        // permission puts B into the mode, and from then on acks renew.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        let t1 = Instant::now();
        while b.delegated_to().is_none() || !keeper.is_valid() {
            assert_eq!(b.current(), Some(b_id.clone()), "B lost its session");
            assert!(
                t1.elapsed() < TTL * 4,
                "B does not re-enter delegated mode after the gate opens"
            );
            let beat = b.self_report().unwrap().beat;
            b.on_lease_answer(&a_id, term, LeaseAnswer::Grant, Instant::now(), beat);
            tokio::time::sleep(PACE / 2).await;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        a.close().await;
        b.close().await;
    }

    /// (Model M1's `delegating_to_a_lost_coordinator_breaks_ls3_with_literal_yield`):
    /// an observer deferring to the coordinator counts the TTL from `max(when it
    /// saw the role lost, when it saw this version)`, not only from when it saw
    /// the role lost.
    ///
    /// Interleaving:
    /// 1. X holds the role at term `t`, then shuts down (`close`: releases the
    ///    role, writes `closed`); observer O reads `control/coordinator` — sees
    ///    the role lost at `L`.
    /// 2. 2·TTL later, B (still believing in role `X:t`, a late permission) CASes
    ///    `mode=delegated coord=X:t` at `d` — a `direct` renewal: B's grant is
    ///    valid until `d + ⅔·TTL`. After that every PUT of B hangs (B cannot fall
    ///    back).
    /// 3. O reads B's record: a new version (the beat increased) ⇒ the mark
    ///    restarts from there.
    ///
    /// Must see: O does not CAS `expired` on B's record before `d + 0.9·TTL` (by
    /// the letter of the rule O would expire it at once, `L + TTL < d`), and
    /// expires it afterwards; by then B's grant has ended.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegating_to_a_lost_coordinator_is_not_expired_early() {
        let shared: Arc<InMemory> = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        let x = session_on(store.clone(), "node-x").await;
        let o = session_on(store.clone(), "node-o").await;
        let b_store = FaultyStore::wrap(shared.clone());
        let b = session_on(b_store.clone(), "node-b").await;
        let x_id = x.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&x).await;
        let (_g, keeper) = won(&b, 0).await;
        x.close().await;
        o.read_coordinator().await.unwrap();
        tokio::time::sleep(TTL * 2).await;
        o.read_coordinator().await.unwrap();
        let d = Instant::now();
        delegate(&b, &x_id, term).await;
        b_store.add_rule(Rule::new(OpSet::PUT, PathMatch::Any, Action::Hang));
        let tracker = o.tracker();
        loop {
            let expired = state_of(&store, &b_id).await == Some(SessionState::Expired);
            if expired {
                let at = d.elapsed();
                assert!(
                    at >= TTL.mul_f32(0.9),
                    "O expired a record delegating to a lost role after {at:?}: < 1 TTL"
                );
                assert!(!keeper.is_valid(), "expired while B's grant is still valid");
                break;
            }
            assert!(
                d.elapsed() < TTL * 4,
                "O never expires the record delegating to a lost role"
            );
            assert!(tracker.state_of(&b_id).is_some_and(|s| !s.is_terminal()) || expired);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        o.close().await;
    }

    /// An undecodable record in a cluster with a coordinator is treated as
    /// `delegated` — an observer other than the coordinator does not expire it;
    /// the coordinator expires it once its fence has passed **after** it saw the
    /// breakage.
    ///
    /// Interleaving:
    /// 1. A holds the role; O reads `control/coordinator`; B dies (`crash`).
    /// 2. The test overwrites B's record with junk bytes.
    /// 3. 3·TTL: O and A both GET it every pace; A does **not** run the fence.
    /// 4. A runs the fence (passes).
    ///
    /// Must see: after (3) the record is still junk (nobody CASes `expired`);
    /// after (4) A CASes `expired` within 2·TTL.
    #[tokio::test(flavor = "multi_thread")]
    async fn undecodable_record_is_not_expired_by_a_direct_observer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = session_on(store.clone(), "node-a").await;
        let o = session_on(store.clone(), "node-o").await;
        let b = session_on(store.clone(), "node-b").await;
        let b_id = b.current().unwrap();
        coordinate(&a).await;
        o.read_coordinator().await.unwrap();
        eventually(TTL * 2, "O and A see B", || {
            o.tracker().live_nodes().contains("node-b")
                && a.tracker().live_nodes().contains("node-b")
        })
        .await;
        b.crash_handle().crash();
        store
            .put(
                &session_path(&root(), &b_id),
                PutPayload::from_static(b"\x00junk"),
            )
            .await
            .unwrap();
        tokio::time::sleep(TTL * 3).await;
        assert_eq!(
            state_of(&store, &b_id).await,
            None,
            "the junk record was changed before A's fence passed"
        );
        assert!(a.coordinator_fence().await, "A's fence passes");
        eventually(TTL * 2, "A expires the junk record", || {
            a.tracker().is_dead(&b_id)
        })
        .await;
        assert_eq!(state_of(&store, &b_id).await, Some(SessionState::Expired));
        a.close().await;
        o.close().await;
    }

    /// A torn `delegated` record (MinIO EC) of a dead node is not wedged forever:
    /// the coordinator overwrites it **keeping the `mode` line**, then CASes
    /// `expired`.
    ///
    /// Interleaving:
    /// 1. A holds the role; B delegates to A, then dies (`crash`).
    /// 2. A's store sees B's record torn (`Tear`): GET frozen, the etag flipping
    ///    back and forth, the CAS always missing.
    /// 3. One TTL after τ, A CASes `expired` — missing forever; past
    ///    `UNEXPIRABLE_AFTER_TTLS` TTLs A overwrites it (healing the object),
    ///    counts another TTL, then the CAS wins.
    ///
    /// Must see: B's record becomes `expired` within 7·TTL, still carrying
    /// `mode=delegated coord=A:t`.
    #[tokio::test(flavor = "multi_thread")]
    async fn torn_delegated_record_is_rearmed_not_wedged() {
        let shared = Arc::new(InMemory::new());
        let a_store = FaultyStore::wrap(shared.clone());
        let a = session_on(a_store.clone(), "node-a").await;
        let b = session_on(shared.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        eventually(TTL * 2, "A sees B delegating to it", || {
            matches!(
                a.tracker().lease_view(&b_id),
                LeaseView::DelegatedToMe { .. }
            )
        })
        .await;
        let tear = crate::testkit::Tear::new();
        a_store.add_rule(Rule::new(
            OpSet::GET | OpSet::HEAD | OpSet::PUT,
            PathMatch::Contains("sessions/node-b".into()),
            Action::Tear(tear.clone()),
        ));
        b.crash_handle().crash();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        eventually(TTL * 7, "A expires the torn delegated record", || {
            a.tracker().is_dead(&b_id)
        })
        .await;
        assert!(tear.healed(), "A did not overwrite the torn record");
        let store: Arc<dyn ObjectStore> = shared.clone();
        let r = solo::record(&store, &b_id).await;
        assert_eq!(r.state, SessionState::Expired);
        assert!(
            r.mode.delegated_to(&a_id, term),
            "the overwrite lost the mode line: {r:?}"
        );
        a.close().await;
    }

    /// Every owner write increases the beat, so changing `owns` A→B→A is still a
    /// new version (the etag is a function of the contents on S3/SeaweedFS —
    /// `ContentEtagStore`), and the coordinator resets the silence mark each
    /// time.
    ///
    /// Interleaving:
    /// 1. A holds the role; B delegates to A; B is acked (the test feeds the acks
    ///    into B directly), but A records **no** heartbeat (A's τ comes only from
    ///    versions).
    /// 2. Every TTL/2, B holds and then releases range 0 — `owns` goes `{}` →
    ///    `{0}` → `{}`: each one a CAS keeping the mode, `beat+1`.
    ///
    /// Must see: throughout 3·TTL B's record is not expired (A sees the beat
    /// increase each time); once the changes stop A expires it after ~one TTL
    /// (the test is sensitive).
    #[tokio::test(flavor = "multi_thread")]
    async fn owns_aba_rewrite_still_resets_suspicion() {
        let store: Arc<dyn ObjectStore> = Arc::new(ContentEtagStore::default());
        let a = session_on(store.clone(), "node-a").await;
        let b = session_on(store.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 3 {
            let (_g, k) = won(&b, 0).await;
            tokio::time::sleep(TTL / 4).await;
            drop(k);
            tokio::time::sleep(TTL / 4).await;
            assert_eq!(
                state_of(&store, &b_id).await,
                Some(SessionState::Live),
                "A expired B although B writes a new version every TTL/4 ({:?})",
                t0.elapsed()
            );
        }
        let quiet = Instant::now();
        eventually(TTL * 3, "A expires B once B stops writing", || {
            a.tracker().is_dead(&b_id)
        })
        .await;
        assert!(quiet.elapsed() >= TTL.mul_f32(0.8), "A expired B too early");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        a.close().await;
    }

    /// Ack rule: the coordinator remembers every `expired` CAS it has issued,
    /// even when the result is ambiguous, and never acks that session again.
    ///
    /// Interleaving:
    /// 1. A holds the role; B delegates to A, is acked (fed directly) — A records
    ///    no heartbeat.
    /// 2. One TTL later, A's tracker issues the `expired` CAS: A's store returns
    ///    an error (not applied — the "PUT timed out" shape, which may have been
    ///    applied).
    ///
    /// Must see: B's `lease_view` at A is `Ended` (the heartbeat answer will be
    /// `expired`, never `ack`) although B's record is still `live`.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_expire_cas_blocks_further_acks() {
        let shared = Arc::new(InMemory::new());
        let a_store = FaultyStore::wrap(shared.clone());
        let a = session_on(a_store.clone(), "node-a").await;
        let b = session_on(shared.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        eventually(TTL * 2, "A sees B delegating to it", || {
            matches!(
                a.tracker().lease_view(&b_id),
                LeaseView::DelegatedToMe { .. }
            )
        })
        .await;
        a_store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("sessions/node-b".into()),
            Action::Fail { times: None },
        ));
        eventually(TTL * 3, "A issues the expired CAS for B", || {
            a.tracker().expire_issued(&b_id)
        })
        .await;
        assert_eq!(a.tracker().lease_view(&b_id), LeaseView::Ended);
        let store: Arc<dyn ObjectStore> = shared.clone();
        assert_eq!(state_of(&store, &b_id).await, Some(SessionState::Live));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        a.close().await;
        b.close().await;
    }

    /// A delegated node does not write its record whenever the slow score or p99
    /// changes — only when the score **crosses the threshold** (the cap); and a
    /// draining node (LS-11) goes back to `direct` and does not delegate again
    /// while draining (the PUT renewal is the measurement sample for its
    /// recovery).
    ///
    /// 1. A holds the role; B (with a meter) delegates to A, acked steadily.
    /// 2. B's meter publishes score 30 then 60 (below the cap): no CAS.
    /// 3. Publishes 100: one CAS keeping the mode (beat + 1, `slow=100`).
    /// 4. B drains (`draining`): a CAS back to `direct`, and stays at `direct`
    ///    even when a permission arrives.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegated_node_writes_only_on_slow_threshold_and_falls_back_when_draining() {
        let shared = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        let a = session_on(shared.clone(), "node-a").await;
        let meter = crate::health::SlowMeter::new("node-b-meter", Duration::from_millis(100));
        let b = Session::establish(
            shared.clone(),
            &root(),
            cfg("node-b").with_meter(meter.clone()),
        )
        .await
        .unwrap();
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        // A hears B's heartbeats like a healthy coordinator: without them A's τ
        // counts only from record versions, and two steps below the cap
        // (2 × 3·PACE = TTL) are enough for A to expire B before the CAS at the
        // cap when scheduling runs late.
        let heard = {
            let (a, b, b_id, stop) = (a.clone(), b.clone(), b_id.clone(), stop.clone());
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let beat = b.self_report().map_or(0, |r| r.beat);
                    a.tracker().heard(&b_id, Instant::now(), beat);
                    tokio::time::sleep(PACE).await;
                }
            })
        };
        let beat0 = solo::record(&store, &b_id).await.beat;
        for score in [30.0, 60.0] {
            meter.publish(score, Some(7));
            tokio::time::sleep(PACE * 3).await;
        }
        assert_eq!(
            solo::record(&store, &b_id).await.beat,
            beat0,
            "wrote the record while the score only changed below the cap"
        );
        meter.publish(100.0, Some(900));
        eventually(TTL, "a CAS when the score reaches the cap", || {
            b.self_report().is_some_and(|r| r.beat > beat0)
        })
        .await;
        tokio::time::sleep(PACE * 2).await;
        let r = solo::record(&store, &b_id).await;
        assert!(
            r.mode.delegated_to(&a_id, term) && r.slow == Some(100),
            "{r:?}"
        );
        meter.set_draining(true);
        eventually(TTL, "a draining node goes back to direct", || {
            b.mode() == SessionMode::Direct
        })
        .await;
        let t0 = Instant::now();
        while t0.elapsed() < TTL * 3 {
            let beat = b.self_report().unwrap().beat;
            b.on_lease_answer(&a_id, term, LeaseAnswer::Grant, Instant::now(), beat);
            assert_eq!(
                b.mode(),
                SessionMode::Direct,
                "a draining node delegated again"
            );
            tokio::time::sleep(PACE).await;
        }
        assert_eq!(b.current(), Some(b_id));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        heard.await.unwrap();
        a.close().await;
        b.close().await;
    }

    /// **The release tripwire**: a delegated node whose self-checking GET finds
    /// its record ended while the grant is still valid revokes at once, counts
    /// `REVOKED_WHILE_VALID` and loses the session. Built here by breaking the
    /// rules: the test writes `expired` straight onto the record while acks
    /// still arrive — the correct protocol never gets there.
    ///
    /// Must see: the counter increases, range 0's grant ends at once, B moves to
    /// a new incarnation.
    #[tokio::test(flavor = "multi_thread")]
    async fn revoked_while_valid_tripwire_fires_and_loses() {
        use std::sync::atomic::Ordering::Relaxed;
        let shared = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        let a = session_on(shared.clone(), "node-a").await;
        let b = session_on(shared.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let (_g, keeper) = won(&b, 0).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        tokio::time::sleep(PACE * 2).await;
        assert!(keeper.is_valid());
        let before = delegation_stats::REVOKED_WHILE_VALID.load(Relaxed);
        let mut rec = solo::record(&store, &b_id).await;
        rec.state = SessionState::Expired;
        store
            .put(
                &session_path(&root(), &b_id),
                PutPayload::from(rec.encode()),
            )
            .await
            .unwrap();
        eventually(TTL, "the tripwire fires", || {
            delegation_stats::REVOKED_WHILE_VALID.load(Relaxed) > before
        })
        .await;
        assert!(
            !keeper.is_valid(),
            "the grant is still valid after the tripwire"
        );
        eventually(TTL * 2, "B moves to a new incarnation", || {
            b.current().is_some_and(|id| id != b_id)
        })
        .await;
        stop.store(true, Relaxed);
        acks.await.unwrap();
        a.close().await;
        b.close().await;
    }

    /// **A fallback CAS that fails immediately is retried at a pace, not spun**
    /// (found by the simulation: `sim_cluster` hung at 100% CPU, virtual time
    /// standing still).
    ///
    /// Interleaving:
    /// 1. A holds the role; B delegates to A, is acked, then acks stop.
    /// 2. Every PUT of B fails **immediately** (`Fail` — like a store refusing
    ///    connections, or the sim's store returning an error at virtual time 0).
    /// 3. The fallback mark (last ack + TTL/3 + jitter) passes: the keeper CASes
    ///    back to `direct` — failing —, wakes at that mark (already in the past),
    ///    retries at once, … until the suspension cap.
    ///
    /// Must see: in the 2·TTL after the PUTs fail, B issues no more than a few
    /// dozen session-record PUTs (the retry pace while suspended is
    /// `renew_interval/4`). Before the fix: thousands — a hot loop; on a
    /// virtual-time runtime it never yields time.
    #[tokio::test(flavor = "multi_thread")]
    async fn failing_fallback_cas_is_paced_not_spun() {
        use crate::testkit::{Op, PathClass};
        let shared = Arc::new(InMemory::new());
        let a = session_on(shared.clone(), "node-a").await;
        let b_store = FaultyStore::wrap(shared.clone());
        let b = session_on(b_store.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let term = coordinate(&a).await;
        delegate(&b, &a_id, term).await;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acks = ack_loop(b.clone(), a_id.clone(), term, stop.clone());
        tokio::time::sleep(PACE * 2).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        acks.await.unwrap();
        b_store.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains("sessions/node-b".into()),
            Action::Fail { times: None },
        ));
        let before = b_store.counts().get(Op::Put, PathClass::Lease);
        tokio::time::sleep(TTL * 2).await;
        let puts = b_store.counts().get(Op::Put, PathClass::Lease) - before;
        // The retry pace while suspended: TTL/5/4 = 15 ms ⇒ ≤ ~40 attempts in
        // 2·TTL, plus opening a new incarnation (backing off by `retry_pause`).
        assert!(
            puts <= 100,
            "{puts} PUTs in 2·TTL — the keeper spins when the fallback CAS fails"
        );
        a.close().await;
        b.close().await;
    }

    /// Ambiguity for the CAS entering delegated mode: a CAS that applied but
    /// answered ambiguously (`Err`, or the `Precondition` of object_store's own
    /// retry) is resolved by re-reading — B recognizes its own delegated record,
    /// no `lose`.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_delegate_cas_is_resolved_by_reading_back() {
        for reply in [Ambiguity::Error, Ambiguity::Retried] {
            let shared = Arc::new(InMemory::new());
            let a = session_on(shared.clone(), "node-a").await;
            let b_store = FaultyStore::wrap(shared.clone());
            let b = session_on(b_store.clone(), "node-b").await;
            let a_id = a.current().unwrap();
            let b_id = b.current().unwrap();
            let term = coordinate(&a).await;
            b_store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("sessions/node-b/{}", b_id.inc)),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            delegate(&b, &a_id, term).await;
            assert_eq!(
                b.current(),
                Some(b_id.clone()),
                "{reply:?}: B lost its session"
            );
            let store: Arc<dyn ObjectStore> = shared.clone();
            assert!(solo::record(&store, &b_id)
                .await
                .mode
                .delegated_to(&a_id, term));
            a.close().await;
            b.close().await;
        }
    }

    /// Ambiguity for the CAS falling back to `direct`: as above, after the
    /// coordinator's `fallback` answer.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_fallback_cas_is_resolved_by_reading_back() {
        for reply in [Ambiguity::Error, Ambiguity::Retried] {
            let shared = Arc::new(InMemory::new());
            let a = session_on(shared.clone(), "node-a").await;
            let b_store = FaultyStore::wrap(shared.clone());
            let b = session_on(b_store.clone(), "node-b").await;
            let a_id = a.current().unwrap();
            let b_id = b.current().unwrap();
            let term = coordinate(&a).await;
            delegate(&b, &a_id, term).await;
            b_store.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(format!("sessions/node-b/{}", b_id.inc)),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            let beat = b.self_report().unwrap().beat;
            b.on_lease_answer(&a_id, term, LeaseAnswer::Fallback, Instant::now(), beat);
            eventually(TTL * 2, "B falls back to direct", || {
                b.mode() == SessionMode::Direct
            })
            .await;
            assert_eq!(
                b.current(),
                Some(b_id.clone()),
                "{reply:?}: B lost its session"
            );
            let store: Arc<dyn ObjectStore> = shared.clone();
            assert_eq!(solo::record(&store, &b_id).await.mode, SessionMode::Direct);
            a.close().await;
            b.close().await;
        }
    }

    /// Ambiguity for the CAS falling back to `direct` **when an ack comes back
    /// while the CAS is in flight** (found in a chaos run).
    ///
    /// Interleaving:
    /// 1. A holds the role; B holds range 0, then delegates to A. No ack arrives.
    /// 2. The fallback mark (the delegation CAS's issue time + TTL/3 + jitter)
    ///    passes: B's keeper issues the CAS back to `direct`, `beat+1` — stuck at
    ///    the gate.
    /// 3. An ack from A for a heartbeat B sent "now" arrives (heartbeats fly in
    ///    parallel with the fallback CAS): `Delegation::base` moves, the new
    ///    fallback mark is in the future.
    /// 4. The gate opens: the fallback CAS **applies** on the store, but B gets an
    ///    ambiguous error (`Err` — like the failpoint `session::renew::after_put`).
    ///    B's `Current` is still the old `delegated` copy, while the bucket has
    ///    B's own `direct` copy.
    /// 5. The next keeper round has not reached the new fallback mark, `owns` is
    ///    unchanged ⇒ a self-checking GET (`delegated_check`) instead of another
    ///    fallback CAS.
    ///
    /// Must see: that GET recognizes the `direct` copy carrying the beat of B's
    /// own ambiguous CAS (re-read and compared with the intended contents) — B
    /// keeps its incarnation and range 0, at `direct`. Before this test's
    /// `fix:`, `delegated_check` took every record other than `Current::written`
    /// as "the delegated record was changed by someone else" ⇒ `lose`, and the
    /// whole node dropped every range.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_fallback_cas_then_ack_is_recognised_by_self_check() {
        let shared = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = shared.clone();
        let a = session_on(shared.clone(), "node-a").await;
        let b_store = FaultyStore::wrap(shared.clone());
        let b = session_on(b_store.clone(), "node-b").await;
        let a_id = a.current().unwrap();
        let b_id = b.current().unwrap();
        let term = coordinate(&a).await;
        let (_g, keeper) = won(&b, 0).await;
        delegate(&b, &a_id, term).await;
        let own_record = PathMatch::Contains(format!("sessions/node-b/{}", b_id.inc));
        let gate = Gate::closed();
        let held = b_store.add_rule(Rule::new(
            OpSet::PUT,
            own_record.clone(),
            Action::Gate(gate.clone()),
        ));
        b_store.add_rule(Rule::new(
            OpSet::PUT,
            own_record,
            Action::Ambiguous {
                reply: Ambiguity::Error,
                skip: 0,
                times: Some(1),
            },
        ));
        eventually(TTL, "B's fallback CAS at gate", || gate.blocked() > 0).await;
        assert_eq!(b.delegated_to(), Some((a_id.clone(), term)));
        ack(&b, &a_id, term, Instant::now());
        b_store.remove_rule(held);
        gate.open();
        eventually(TTL, "the fallback CAS applies on the store", || {
            futures::executor::block_on(solo::record(&store, &b_id)).mode == SessionMode::Direct
        })
        .await;
        // The new fallback mark (ack + TTL/3 + jitter) has not come: the next round self-checks.
        tokio::time::sleep(TTL * 2).await;
        assert_eq!(b.current(), Some(b_id.clone()), "B changed incarnation");
        assert!(!has_lost(&keeper), "B lost range 0");
        assert_eq!(b.mode(), SessionMode::Direct);
        assert_eq!(b.delegated_to(), None);
        let r = solo::record(&store, &b_id).await;
        assert_eq!(r.state, SessionState::Live, "{r:?}");
        assert_eq!(r.mode, SessionMode::Direct, "{r:?}");
        a.close().await;
        b.close().await;
    }
}

/// **An in-flight yield of an old term must not delete the pending yield of a
/// newer term of the same range.**
///
/// Interleaving:
/// 1. A holds p4 at term t (keeper K1). PUTs of `owners/range-4` fail: K1
///    cannot yield ⇒ PR1 (term t) goes into `pending_release`; the background
///    loop retries and fails.
/// 2. Change rules: PUTs of `owners/range-4` stand at a gate. The background
///    loop's next attempt — PR1, snapshotted from the map — stands at the gate;
///    remove the gate rule (only that request stands).
/// 3. A reclaims p4 itself at term t+1 (K2; `claim_with` deletes PR1), then K2
///    fails to yield (the PUT fails again) ⇒ PR2 (term t+1) goes into the map.
/// 4. Open the gate: PR1's PUT reaches the bucket with the old version ⇒ the
///    CAS misses, the record is already at term t+1 ⇒ `write_release` returns
///    `Ok(false)`. Remove the failure rule.
///
/// Must see: PR2 is still in the map and is eventually written — the record
/// becomes `released`, p4 leaves `owns`. Before the fix: the `Ok(Ok(_))` branch
/// deleted the range's entry — by then PR2 —, the owner record pointed at A's
/// live session forever, other nodes got `Held`.
#[tokio::test(flavor = "multi_thread")]
async fn stale_release_retry_keeps_the_newer_pending_release() {
    let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    // A long TTL: the background attempt is capped at one TTL
    // (`release_retry_bound`), and it must stay alive through step 3.
    let a = Session::establish(
        store.clone(),
        &root(),
        cfg("a").with_ttl(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let owner_put = || PathMatch::Contains("owners/range-4".into());
    // 1.
    let (_g1, k1) = won(&a, 4).await;
    let t1 = k1.term;
    let fail = faulty.add_rule(Rule::new(
        OpSet::PUT,
        owner_put(),
        Action::Fail { times: None },
    ));
    k1.release().await;
    assert!(
        a.0.state
            .lock()
            .unwrap()
            .pending_release
            .get(&4)
            .is_some_and(|r| r.term == t1),
        "K1 must leave PR1"
    );
    // 2.
    let gate = Gate::closed();
    let held = faulty.add_rule(Rule::new(
        OpSet::PUT,
        owner_put(),
        Action::Gate(gate.clone()),
    ));
    faulty.remove_rule(fail);
    a.0.release_wake.notify_one();
    eventually(
        Duration::from_secs(3),
        "the background attempt stands at the gate",
        || gate.blocked() == 1,
    )
    .await;
    faulty.remove_rule(held);
    // 3.
    let (_g2, k2) = won(&a, 4).await;
    let t2 = k2.term;
    assert!(t2 > t1);
    let fail = faulty.add_rule(Rule::new(
        OpSet::PUT,
        owner_put(),
        Action::Fail { times: None },
    ));
    k2.release().await;
    assert!(
        a.0.state
            .lock()
            .unwrap()
            .pending_release
            .get(&4)
            .is_some_and(|r| r.term == t2),
        "K2 must leave PR2"
    );
    // 4.
    // The background loop is sequential: PR2 is only tried in the next round,
    // once the PR1 attempt (a CAS miss, then a re-read — a GET, not touching
    // the PUT rule) is done; removing the failure rule at once does not let PR2
    // go before PR1.
    gate.open();
    faulty.remove_rule(fail);
    eventually(
        Duration::from_secs(5),
        "PR2 is written, p4 leaves owns",
        || !a.owned().contains_key(&4),
    )
    .await;
    let rec = owner::read_owner(&store, &root(), PartitionId(4))
        .await
        .unwrap()
        .unwrap();
    assert!(
        rec.released && rec.term == t2,
        "the record must be the PR2 yield: {rec:?}"
    );
    a.close().await;
}
