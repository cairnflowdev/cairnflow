// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The D-d loop — removing the Dbs of retired ranges — and its barriers.
//!
//! A round has two halves:
//!
//! 1. **Plan** ([`plan`]): for each tombstoned range whose Db is still on the
//!    bucket, it is either *eligible* for deletion or *blocked* — with a
//!    reason.
//! 2. **Execute** ([`run_once`]): delete each eligible range.
//!
//! **Barriers** ([`cairn_store::gc_barrier`]): a party outside the loop —
//! backup, export, an operator — places a `gc-barriers/<id>` with an expiry to
//! keep the Dbs of some ranges (or all ranges). An expired barrier is ignored,
//! and the loop's runner deletes it; an unreadable barrier blocks every range
//! and nobody deletes it on their own.
//!
//! The order of checks is what keeps it safe: the table is reloaded *first*,
//! and the set of live ranges is taken from that table; every ambiguity (a
//! failed read, a live Db without a manifest yet, an unknown prefix) counts as
//! "still referenced". A retired range never comes back to the table (D22) and
//! is never a clone source again, so "nobody references it", once true, is
//! true forever.
//!
//! **The swept-tombstone log** ([`cairn_store::gc_swept`]): a tombstone whose
//! Db an earlier round already read as absent is not re-read in later rounds
//! — no GET of its owner record, no LIST of its manifest — so the cost of a
//! round does not grow with the number of tombstones (tombstones are kept
//! forever). The log only removes candidates; every
//! `gc_swept::FULL_PASS_EVERY` rounds every tombstone is read again and the log
//! is rebuilt.
//!
//! **Per-entry fault tolerance** (as in the state scrubber): a range whose
//! read fails during planning (tombstone, manifest, Db size) becomes
//! [`BlockedBy::Unreadable`]; a range whose deletion step fails (rebuilding the
//! proof, deleting) goes into [`GcRound::failed`]. Both are **kept** (rule 5),
//! counted ([`stats`], `cairnflow_gc_skipped_total`), and the round goes on
//! with the other ranges. Only a failure of a shared read — the table, the
//! `owners/` LIST, barriers, the reference set, the upgrade gate — fails the
//! whole round.
//!
//! **The clock-skew gate** ([`clock_gate`]): every age GC uses — tombstones
//! (`retired_ms` or `last_modified`), expired barriers, trash entries — is
//! compared with the wall clock of the node running the round. Before the
//! first deletion of a round (D-d, tombstone-on-behalf / orphan-child cleanup,
//! trash purge), the node PUTs a probe object, HEADs it, and compares
//! `last_modified` with its own clock; skew beyond [`clock_skew_limit`]
//! (`min_age/4`) ⇒ the round drops **every** deletion (keep — rule 5), counts
//! `cairnflow_gc_clock_blocked_total{round}`, and warns.

use {
    crate::partitions::Partitions,
    cairn_store::{
        gc_barrier::{self, GcBarrier, Protects, StoredBarrier},
        ClusterConfig, ObjectStores, PartitionId, Session, StoreError,
    },
    serde::{Deserialize, Serialize},
    std::{collections::BTreeSet, time::Duration},
};

/// Ranges eligible at planning (nobody references them, old enough, no
/// barrier) for which the most recent **execution** round could not rebuild a
/// `DeleteProof` — kept, not deleted (`cairnflow_gc_unproven_candidates`; the
/// idea of separating proven from unproven candidates comes from Lance's
/// `cleanup.rs`). A non-zero value that persists is the sign of a proof gate
/// disagreeing with the plan.
pub static UNPROVEN_CANDIDATES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Default interval of the D-d loop.
pub const DEFAULT_EVERY: Duration = Duration::from_secs(600);

/// A node's D-d loop first asks "am I the runner" after this long (or after
/// `every` if that is shorter), and asks again at the same interval until it
/// is the runner; only **after a round has run** does it rest a full `every`.
/// The loop used to rest `every` before the first question: a node living
/// shorter than `every` never ran D-d (a 4 h soak killing a node every 10 s
/// saw only ~45 of 470 lives per node reach 60 s), and a new runner waited up
/// to one `every`. The question only reads the node's RAM ([`is_runner`]);
/// each node still runs at most one round per `every`. Like
/// `FIRST_SWEEP_AFTER` of the shared-WAL sweep.
pub const RUNNER_CHECK_EVERY: Duration = Duration::from_secs(5);

/// Minimum tombstone age before the Db is deleted: larger than the sum of the
/// known intervals — an idle follower reader closes itself after 5 minutes,
/// the compactor checkpoint lasts 10 minutes, SlateDB's `min_age` is 5
/// minutes.
pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(30 * 60);

/// Cap on a barrier's TTL: there are no unbounded barriers. A holder needing
/// longer renews it — like PD's service safepoint, every `TTL / 3`.
pub const MAX_BARRIER_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Whether the D-d loop deletes for real, or only logs the plan (`--gc-retired-dbs=plan`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcMode {
    Delete,
    Plan,
}

/// The node's D-d loop configuration.
#[derive(Clone, Debug)]
pub struct GcConfig {
    pub every: Duration,
    pub min_age: Duration,
    pub mode: GcMode,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            every: DEFAULT_EVERY,
            min_age: DEFAULT_MIN_AGE,
            mode: GcMode::Delete,
        }
    }
}

/// Why a retired range cannot be deleted yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockedBy {
    /// An unexpired barrier covers this range (or an unreadable barrier).
    Barrier,
    /// The tombstone is younger than `min_age`.
    Age,
    /// A live range still lists this Db in its `external_dbs` (or it cannot be
    /// proven that none does).
    Reference,
    /// The cluster is migrating (`upgrading=` in `cluster-version`): the whole
    /// round skips, no range is eligible.
    Upgrading,
    /// A read failed (tombstone, manifest, Db size of this very range): cannot
    /// be proven — kept, reconsidered next round.
    Unreadable,
    /// The second barrier: the Db's own manifest still has a final checkpoint
    /// of a Db that still exists (named), or an unnamed one (from buckets
    /// created before checkpoints were named) — although the reference scan
    /// said nobody.
    Checkpoint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Blocked {
    pub range: u32,
    pub by: BlockedBy,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Eligible {
    pub range: u32,
    /// Bytes under `ranges/<id>` on both stores.
    pub bytes: u64,
    /// Age of the tombstone at planning time.
    pub age_ms: u64,
}

/// The plan of one D-d round.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcPlan {
    /// The range-table version the plan is based on.
    pub table_version: u64,
    pub eligible: Vec<Eligible>,
    pub blocked: Vec<Blocked>,
    /// Total bytes given back if every eligible range is deleted.
    pub bytes: u64,
    /// Expired barriers: ignored; the loop's runner deletes them once they have
    /// been expired for longer than [`BARRIER_REAP_GRACE`].
    pub expired_barriers: Vec<String>,
    /// The cluster is migrating — the runner, in human-readable form. When
    /// present, `eligible` is empty and the execution round skips.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrading: Option<String>,
    /// Tombstones whose Db the plan **proves** is gone for good: the owner
    /// record carries the tombstone and the manifest is absent, both read in
    /// this plan — candidates for the swept-tombstone log
    /// ([`cairn_store::gc_swept`]).
    #[serde(skip)]
    pub swept: Vec<u32>,
    /// Tombstones the plan skipped because they are in the swept-tombstone log (no GET, no LIST).
    #[serde(skip)]
    pub skipped_swept: usize,
}

/// The result of one round.
#[derive(Clone, Debug, Default)]
pub struct GcRound {
    pub plan: GcPlan,
    pub deleted: Vec<u32>,
    /// Eligible ranges whose deletion step failed (rebuilding the proof,
    /// deleting) — kept, retried next round.
    pub failed: Vec<Blocked>,
    /// The round skipped — the reason: the cluster is migrating, or the
    /// clock-skew gate. A table change after planning does **not** make the
    /// round skip: each range's proof is built on its own snapshot.
    pub skipped: Option<String>,
    /// `ops/<key>` operation records of settled ops just moved to the trash
    /// ([`reap_settled_ops`]).
    pub ops_reaped: Vec<u32>,
}

/// Counters of the D-d loop on this node, accumulated.
pub mod stats {
    use std::sync::atomic::AtomicU64;

    /// Ranges skipped at planning because a read failed ([`super::BlockedBy::Unreadable`]).
    pub static SKIPPED_PLAN: AtomicU64 = AtomicU64::new(0);
    /// Eligible ranges whose deletion step failed ([`super::GcRound::failed`]).
    pub static SKIPPED_DELETE: AtomicU64 = AtomicU64::new(0);
}

/// Retired ranges that the node's **most recent** D-d plan kept because of a
/// checkpoint barrier, by kind — the gauge
/// `cairnflow_gc_checkpoint_held{kind}`. `unnamed` counts Dbs held by an
/// unnamed checkpoint (buckets created before checkpoints were named): still
/// non-zero means the old bucket still has that many Dbs that cannot be
/// cleaned up.
pub mod checkpoint_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static NAMED: AtomicU64 = AtomicU64::new(0);
    pub static UNNAMED: AtomicU64 = AtomicU64::new(0);
    pub static DISAGREE: AtomicU64 = AtomicU64::new(0);
    pub static UNREADABLE: AtomicU64 = AtomicU64::new(0);

    /// Counters of one planning pass.
    #[derive(Debug, Default)]
    pub(super) struct Held {
        named: u64,
        unnamed: u64,
        disagree: u64,
        unreadable: u64,
    }

    pub(super) fn count(h: &mut Held, k: cairn_split::Keep) {
        use cairn_split::Keep;
        match k {
            Keep::NamedCheckpoint => h.named += 1,
            Keep::UnnamedCheckpoint => h.unnamed += 1,
            Keep::ProofsDisagree => h.disagree += 1,
            _ => h.unreadable += 1,
        }
    }

    pub(super) fn publish(h: &Held) {
        NAMED.store(h.named, Ordering::Relaxed);
        UNNAMED.store(h.unnamed, Ordering::Relaxed);
        DISAGREE.store(h.disagree, Ordering::Relaxed);
        UNREADABLE.store(h.unreadable, Ordering::Relaxed);
    }
}

/// The lineage the node's **most recent** D-d plan saw — the gauges
/// `cairnflow_owner_tombstones`, `cairnflow_retired_dbs`. Only the node
/// running D-d (`is_runner`) has numbers; other nodes that have made no plan
/// print nothing (`OBSERVED`).
pub mod lineage_stats {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Owner records carrying a tombstone (`retired`) outside the range table.
    pub static TOMBSTONES: AtomicU64 = AtomicU64::new(0);
    /// Among those, tombstones whose Db is still on the bucket — waiting for
    /// D-d (old enough or not, held or not).
    pub static RETIRED_DBS: AtomicU64 = AtomicU64::new(0);
    /// At least one D-d plan has been made on this node.
    pub static OBSERVED: AtomicBool = AtomicBool::new(false);

    pub(super) fn publish(tombstones: u64, retired_dbs: u64) {
        TOMBSTONES.store(tombstones, Ordering::Relaxed);
        RETIRED_DBS.store(retired_dbs, Ordering::Relaxed);
        OBSERVED.store(true, Ordering::Relaxed);
    }
}

/// Wall clock, ms — the same origin as the tombstone's `retired_ms`.
pub fn wall_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ─────────────────────── the clock-skew gate

/// The clock-skew gate's probe object, under the main store's prefix. Not
/// customer data: nobody reads its content, every measurement overwrites it;
/// its absence means nothing (the next measurement recreates it).
pub const CLOCK_PROBE: &str = "gc-clock-probe";

/// Floor of the skew threshold: S3's `last_modified` has a resolution of a
/// second, plus one PUT. Below this floor the measurement cannot tell real
/// skew from noise — a small `min_age` (tests, `min_age` 0) uses the floor.
pub const CLOCK_SKEW_FLOOR: Duration = Duration::from_secs(2);

/// Limit on each I/O of the measurement (PUT, HEAD).
const CLOCK_PROBE_BOUND: Duration = Duration::from_secs(10);

/// The maximum skew between the round-running node's clock and the store's
/// clock that a round deleting by age `min_age` can tolerate: `min_age/4`, no
/// less than [`CLOCK_SKEW_FLOOR`]. A quarter, so every age (tombstones written
/// by other nodes, barriers placed from outside) keeps a margin wider than the
/// skew.
pub fn clock_skew_limit(min_age: Duration) -> Duration {
    (min_age / 4).max(CLOCK_SKEW_FLOOR)
}

/// Which round the clock-skew gate blocked — the metric's `round` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockRound {
    /// D-d.
    Dbs,
    /// Tombstone-on-behalf and orphan-child cleanup.
    Orphans,
    /// Trash purge.
    Trash,
}

impl ClockRound {
    pub const ALL: [ClockRound; 3] = [ClockRound::Dbs, ClockRound::Orphans, ClockRound::Trash];

    pub fn label(self) -> &'static str {
        match self {
            ClockRound::Dbs => "dbs",
            ClockRound::Orphans => "orphans",
            ClockRound::Trash => "trash",
        }
    }
}

/// Measurements of the clock-skew gate, accumulated on this node.
pub mod clock_stats {
    use std::sync::atomic::AtomicU64;

    /// |skew| (ms) of the latest measurement — `cairnflow_gc_clock_skew_ms`.
    pub static SKEW_MS: AtomicU64 = AtomicU64::new(0);
    /// Rounds blocked, by [`super::ClockRound`] (in the order of `ALL`).
    pub static BLOCKED: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
}

/// Measures the skew between the node's wall clock (`cairn_store::entropy`)
/// and the store's clock: PUT [`CLOCK_PROBE`] between two clock readings `t0`,
/// `t1`, then HEAD. The write reached the store within `[t0, t1]` in real
/// time, so a correct `last_modified` must lie in `[t0 − 1 s, t1]` (second
/// resolution); the skew is the distance to that interval. Two runners
/// measuring at once are still correct: the other's write also falls within
/// our interval if it lands between our PUT and our HEAD.
pub async fn measure_clock_skew(stores: &ObjectStores) -> Result<Duration, StoreError> {
    use slatedb::object_store::{path::Path, ObjectStoreExt, PutPayload};
    let main = stores.main();
    let path = if main.prefix().as_ref().is_empty() {
        Path::from(CLOCK_PROBE)
    } else {
        Path::from(format!("{}/{CLOCK_PROBE}", main.prefix().as_ref()))
    };
    let io = |e: String| StoreError::Io(format!("measuring clock skew: {e}"));
    // Transient errors are retried (an overwriting PUT is idempotent); each
    // attempt carries its own `t0`, so a retry does not widen the comparison
    // interval.
    let policy =
        cairn_store::retry::Policy::new(Duration::from_millis(100), Duration::from_secs(1), 3)
            .with_attempt_bound(CLOCK_PROBE_BOUND);
    let t0 = std::sync::Mutex::new(cairn_store::entropy::now());
    cairn_store::retry::run(
        &cairn_store::retry::ops::GC_CLOCK_PROBE,
        &policy,
        cairn_store::retry::Never,
        || {
            *t0.lock().expect("mutex") = cairn_store::entropy::now();
            let body = format!(
                "cairnflow-gc-clock-probe/1\nat_ms={}\n",
                cairn_store::entropy::now_ms()
            );
            main.store().put(&path, PutPayload::from(body.into_bytes()))
        },
    )
    .await
    .map_err(|e| io(e.to_string()))?;
    let meta = cairn_store::retry::run(
        &cairn_store::retry::ops::GC_CLOCK_PROBE,
        &policy,
        cairn_store::retry::Never,
        || main.store().head(&path),
    )
    .await
    .map_err(|e| io(e.to_string()))?;
    let t0 = *t0.lock().expect("mutex");
    let t1 = cairn_store::entropy::now();
    let store_at: std::time::SystemTime = meta.last_modified.into();
    let earliest = t0 - Duration::from_secs(1);
    Ok(if store_at < earliest {
        earliest.duration_since(store_at).unwrap_or_default()
    } else {
        store_at.duration_since(t1).unwrap_or_default()
    })
}

/// The **clock-skew gate** of a round deleting by age: `Ok` = may delete;
/// `Err(reason)` = drop every deletion of the round — skew beyond `limit`, or
/// it could not be measured (rule 5: cannot prove ⇒ keep). Counts and warns
/// when it blocks.
pub async fn clock_gate(
    stores: &ObjectStores,
    limit: Duration,
    round: ClockRound,
) -> Result<(), String> {
    use std::sync::atomic::Ordering;
    let why = match measure_clock_skew(stores).await {
        Ok(skew) => {
            clock_stats::SKEW_MS.store(skew.as_millis() as u64, Ordering::Relaxed);
            if skew <= limit {
                return Ok(());
            }
            format!(
                "node clock is {} ms off the store's (limit {} ms)",
                skew.as_millis(),
                limit.as_millis()
            )
        }
        Err(e) => format!("cannot measure clock skew: {e}"),
    };
    clock_stats::BLOCKED[ClockRound::ALL
        .iter()
        .position(|r| *r == round)
        .expect("ALL")]
    .fetch_add(1, Ordering::Relaxed);
    tracing::warn!(round = round.label(), reason = %why, "GC: dropping every deletion of the round (clock-skew gate)");
    Err(why)
}

/// **Plans** one round at `now_ms`, deleting nothing.
pub async fn plan(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    min_age: Duration,
    now_ms: u64,
) -> Result<GcPlan, StoreError> {
    plan_skipping(stores, cluster, min_age, now_ms, &BTreeSet::new()).await
}

/// Like [`plan`], skipping entirely (no owner-record GET, no manifest LIST)
/// the ranges outside the table listed in `skip` — ids from the swept-tombstone
/// log that the caller already knows have no Db (`verify`: the log minus the
/// `ranges/<id>` that still have objects). Each skipped id only removes a
/// candidate: the plan never adds anything because of it.
pub async fn plan_skipping(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    min_age: Duration,
    now_ms: u64,
    skip: &BTreeSet<u32>,
) -> Result<GcPlan, StoreError> {
    plan_with(
        stores,
        cluster,
        min_age,
        now_ms,
        &mut cairn_store::proof::RoundRefs::default(),
        skip,
    )
    .await
}

/// Like [`plan`], reading the reference set through `round` — [`run_once`]
/// reuses it for the proofs of every candidate on the same table version.
async fn plan_with(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    min_age: Duration,
    now_ms: u64,
    round: &mut cairn_store::proof::RoundRefs,
    skip: &BTreeSet<u32>,
) -> Result<GcPlan, StoreError> {
    cluster.reload().await?;
    // **One** table snapshot for the whole plan (a finding of M2): the live
    // range set, the candidate filter and the version all come from here. If
    // the RAM table changes midway (one commit) the plan stays consistent
    // with itself, and the deletion step catches the divergence.
    let table = cluster.table();
    let table_version = table.version();
    // Live ranges **plus** children in flight (promised in an intent): a child
    // already cloned stands on its source's ancestors before the table
    // commits. A child without a manifest yet can only stand on what its
    // source references — it adds nothing (`referenced_ranges_with`).
    let in_table: Vec<PartitionId> = table.partitions().collect();
    let promised = table.promised();
    let mut live = in_table.clone();
    live.extend(promised.iter().copied());
    // The gap between the table snapshot and the rest of the plan: the RAM
    // table may change here.
    cairn_store::failpoint!("gc::plan::after_snapshot");
    let main = stores.main();
    let mut out = GcPlan {
        table_version,
        ..GcPlan::default()
    };
    let barriers = gc_barrier::list_barriers(main.store(), main.prefix()).await?;
    out.expired_barriers = barriers
        .iter()
        .filter(|b| b.barrier.as_ref().is_ok_and(|b| b.expired(now_ms)))
        .map(|b| b.id.clone())
        .collect();
    // Read the live ranges' manifests once, and only when some candidate got
    // through every cheaper gate — through `round`, so the candidates' proofs
    // reuse it while the table version has not changed.
    let mut refs: Option<cairn_store::RangeRefs> = None;
    // A range whose read fails ⇒ keep it, count it, go on. It never becomes
    // "eligible": every branch below only pushes into `eligible` after the
    // reads are done.
    let unreadable = |out: &mut GcPlan, id: PartitionId, what: &str, e: StoreError| {
        stats::SKIPPED_PLAN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(range = id.0, error = %e, "D-d: cannot read {what} — keeping, reconsidered next round");
        out.blocked.push(Blocked {
            range: id.0,
            by: BlockedBy::Unreadable,
            detail: format!("{what}: {e}"),
        });
    };
    let mut held = checkpoint_stats::Held::default();
    let (mut tombstones, mut retired_dbs) = (0u64, 0u64);
    for id in cairn_store::inspect::owner_ranges(main.store(), main.prefix()).await? {
        if table.contains(id) || live.contains(&id) {
            continue;
        }
        // The swept-tombstone log: a tombstone already seen without a Db — do not read it again.
        if skip.contains(&id.0) {
            tombstones += 1;
            out.skipped_swept += 1;
            continue;
        }
        let r = match cairn_store::inspect::owner_observed(main.store(), main.prefix(), id).await {
            Ok(Some(o)) if o.record.retired => cairn_store::RetiredRange {
                range: id,
                retired_ms: o.record.retired_ms,
                modified_ms: o.modified_ms,
            },
            Ok(_) => continue,
            Err(e) => {
                unreadable(&mut out, id, "the owner record", e);
                continue;
            }
        };
        tombstones += 1;
        match stores.range_db_exists(id).await {
            Ok(true) => retired_dbs += 1,
            Ok(false) => {
                out.swept.push(id.0);
                continue;
            }
            Err(e) => {
                unreadable(&mut out, id, "manifest", e);
                continue;
            }
        }
        if let Some(detail) = blocking_barriers(&barriers, id, now_ms) {
            out.blocked.push(Blocked {
                range: id.0,
                by: BlockedBy::Barrier,
                detail,
            });
            continue;
        }
        let age_ms = now_ms.saturating_sub(r.since_ms());
        if u128::from(age_ms) < min_age.as_millis() {
            out.blocked.push(Blocked {
                range: id.0,
                by: BlockedBy::Age,
                detail: format!(
                    "tombstone {} s old, not yet {} s{}",
                    age_ms / 1000,
                    min_age.as_secs(),
                    if r.retired_ms.is_none() {
                        " (by last_modified)"
                    } else {
                        ""
                    }
                ),
            });
            continue;
        }
        if refs.is_none() {
            refs = Some(round.get(stores, &table).await?.1.clone());
        }
        // Chaos fault injection: a deliberately broken GC that skips the
        // reference gate — only to prove `cairn-ctl verify` catches the
        // consequences.
        let skip_refs = cairn_store::failpoint::fires("gc::skip_reference_check");
        let refs = refs.as_ref().expect("just read");
        // The core decides (`cairn_split::Event::ReapPlan`) on the **same**
        // table snapshot the reference set uses.
        let mut view = cairn_split::View {
            retired: Some(cairn_split::Retired {
                in_table: table.contains(id),
                promised: promised.contains(&id),
                tomb: true,
                db: true,
            }),
            gate: Some(Ok(cairn_split::Gate {
                table_version,
                refs_version: table_version,
                refs: if skip_refs {
                    cairn_split::Refs::Clear
                } else {
                    refs.refs_of(id)
                },
                refs_in_flight: true,
                barrier: false,
                moved: false,
            })),
            ..cairn_split::View::default()
        };
        // The second barrier: a final checkpoint on `id`'s own manifest, read
        // lazily when the core asks for it (the reference scan has said
        // nobody). `DeleteProof::retired_db` reads it again before deleting.
        let mut pins = None;
        let verdict = loop {
            match cairn_split::step(&view, &cairn_split::Event::ReapPlan) {
                cairn_split::Plan::Need(cairn_split::Need::Pins) => {
                    let read = cairn_store::proof::read_pins(stores, id, &live).await;
                    view.pins = Some(match &read {
                        Ok(p) => Ok(p.core()),
                        Err(_) => Err(cairn_split::ReadErr),
                    });
                    pins = Some(read);
                }
                other => break other,
            }
        };
        match verdict {
            cairn_split::Plan::Eligible => {}
            cairn_split::Plan::Keep(cairn_split::Keep::Unreadable) if pins.is_some() => {
                let e = match pins {
                    Some(Err(e)) => e,
                    _ => StoreError::Io("checkpoint".into()),
                };
                checkpoint_stats::count(&mut held, cairn_split::Keep::Unreadable);
                unreadable(&mut out, id, "the checkpoint on the manifest", e);
                continue;
            }
            cairn_split::Plan::Keep(
                k @ (cairn_split::Keep::NamedCheckpoint
                | cairn_split::Keep::UnnamedCheckpoint
                | cairn_split::Keep::ProofsDisagree),
            ) => {
                checkpoint_stats::count(&mut held, k);
                let detail = match &pins {
                    Some(Ok(p)) => p.blocker(),
                    _ => None,
                }
                .unwrap_or_else(|| format!("core keeps: {k:?}"));
                if k == cairn_split::Keep::ProofsDisagree {
                    tracing::warn!(range = id.0, reason = %detail, version = table_version, "D-d (plan): the scan says nobody stands on the retired Db, but a live Db still has a named final checkpoint on it — keeping");
                }
                out.blocked.push(Blocked {
                    range: id.0,
                    by: BlockedBy::Checkpoint,
                    detail,
                });
                continue;
            }
            cairn_split::Plan::Keep(k) => {
                out.blocked.push(Blocked {
                    range: id.0,
                    by: BlockedBy::Reference,
                    detail: refs
                        .blocker(id)
                        .unwrap_or_else(|| format!("core keeps: {k:?}")),
                });
                continue;
            }
            other => unreachable!("the core returned {other:?} for ReapPlan"),
        }
        let bytes = match stores.range_bytes(id).await {
            Ok(b) => b,
            Err(e) => {
                unreadable(&mut out, id, "the Db size", e);
                continue;
            }
        };
        out.bytes += bytes;
        out.eligible.push(Eligible {
            range: id.0,
            bytes,
            age_ms,
        });
    }
    checkpoint_stats::publish(&held);
    lineage_stats::publish(tombstones, retired_dbs);
    // The upgrade gate: a migration in progress ⇒ delete nothing. Placed here,
    // not at the call sites, so every path into D-d (the node's loop,
    // `/admin/gc/plan`, tests) goes through it.
    if let Some(u) = cairn_store::migrate::upgrading(main.store(), main.prefix()).await? {
        let detail = format!("cluster is upgrading: {u}");
        for e in std::mem::take(&mut out.eligible) {
            out.blocked.push(Blocked {
                range: e.range,
                by: BlockedBy::Upgrading,
                detail: detail.clone(),
            });
        }
        out.bytes = 0;
        out.upgrading = Some(detail);
    }
    Ok(out)
}

/// Which barrier still blocks `r` at `now_ms`, in human-readable form; `None` = no barrier.
fn blocking_barriers(barriers: &[StoredBarrier], r: PartitionId, now_ms: u64) -> Option<String> {
    let by: Vec<String> = barriers
        .iter()
        .filter(|b| b.blocks(r, now_ms))
        .map(|b| match &b.barrier {
            Ok(g) => format!("{} (holder={}, reason={})", b.id, g.holder, g.reason),
            Err(e) => format!("{} (unreadable: {e})", b.id),
        })
        .collect();
    (!by.is_empty()).then(|| by.join("; "))
}

/// A barrier is only deleted once it has been expired for longer than this.
/// object_store's DELETE is unconditional, so a renewal landing in the gap
/// between the re-read and the delete would be lost. With this grace, such a
/// renewal would have to arrive when the barrier has been expired for more
/// than an hour by the D-d runner's clock — clock skew is assumed under 150 s
/// (G9) — and a barrier expired that long has covered nothing for a long
/// time. An expired barrier is still **ignored** at once (blocks no range);
/// only cleaning up the object waits.
pub const BARRIER_REAP_GRACE: Duration = Duration::from_secs(3600);

/// Deletes the barriers the plan saw as expired **beyond
/// [`BARRIER_REAP_GRACE`]** — re-reading each one right before deleting it: if
/// its holder just renewed it, leave it alone.
async fn reap_expired_barriers(stores: &ObjectStores, ids: &[String]) {
    let main = stores.main();
    let cutoff = wall_ms().saturating_sub(BARRIER_REAP_GRACE.as_millis() as u64);
    for id in ids {
        let still_expired = match gc_barrier::read_barrier(main.store(), main.prefix(), id).await {
            Ok(Some(b)) => b.barrier.as_ref().is_ok_and(|b| b.expired(cutoff)),
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(barrier = %id, error = %e, "D-d: cannot re-read an expired barrier — leaving it to the next round");
                false
            }
        };
        if !still_expired {
            continue;
        }
        match gc_barrier::delete_barrier(main.store(), main.prefix(), id).await {
            Ok(()) => tracing::info!(barrier = %id, "D-d: deleting an expired barrier"),
            Err(e) => tracing::warn!(barrier = %id, error = %e, "D-d: deleting barrier failed"),
        }
    }
}

/// One D-d round per `cfg`: plan, then (except in `Plan` mode) delete each
/// eligible range. Idempotent — two nodes running the same tick do no harm.
pub async fn run_once(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    cfg: &GcConfig,
) -> Result<GcRound, StoreError> {
    // One reference set per table version for the whole round: the plan and
    // the proofs of every candidate — previously the proof of **each**
    // candidate re-read the manifests of every live range.
    let mut round_refs = cairn_store::proof::RoundRefs::default();
    // The swept-tombstone log: skip tombstones already seen without a Db;
    // every `FULL_PASS_EVERY` rounds (or when the log is absent/corrupt) read
    // every tombstone again and rebuild the log.
    let main = stores.main();
    let ledger = cairn_store::gc_swept::read(main.store(), main.prefix()).await;
    let now = wall_ms();
    let full_pass = ledger.full_pass_due(now);
    let skip = if full_pass {
        BTreeSet::new()
    } else {
        ledger.ids().clone()
    };
    let plan = plan_with(stores, cluster, cfg.min_age, now, &mut round_refs, &skip).await?;
    let mut round = GcRound {
        plan,
        ..GcRound::default()
    };
    let ledger_update = |round: &GcRound, more: Vec<u32>| {
        let mut swept = round.plan.swept.clone();
        swept.extend(more);
        let unread = round
            .plan
            .blocked
            .iter()
            .chain(&round.failed)
            .filter(|b| b.by == BlockedBy::Unreadable)
            .map(|b| b.range)
            .collect();
        cairn_store::gc_swept::Update {
            swept,
            full_pass_at: full_pass.then_some(now),
            unread,
        }
    };
    if cfg.mode == GcMode::Plan {
        tracing::info!(
            eligible = ?round.plan.eligible,
            blocked = ?round.plan.blocked,
            bytes = round.plan.bytes,
            "D-d (dry run): plan"
        );
        return Ok(round);
    }
    if let Some(u) = round.plan.upgrading.clone() {
        tracing::info!(reason = %u, "D-d: skipping the round");
        round.skipped = Some(u);
        return Ok(round);
    }
    // Old enough operation records (cheap filter by LIST; the proof re-reads each record).
    let ops_due = due_ops(stores, cfg.min_age, wall_ms()).await;
    // The clock-skew gate: the plan computes ages with our clock. Measure only
    // when there is something to delete — an idle round costs no extra
    // request.
    if !round.plan.eligible.is_empty()
        || !round.plan.expired_barriers.is_empty()
        || !ops_due.is_empty()
    {
        if let Err(why) = clock_gate(stores, clock_skew_limit(cfg.min_age), ClockRound::Dbs).await {
            round.skipped = Some(why);
            write_swept(stores, &ledger, &ledger_update(&round, Vec::new())).await;
            return Ok(round);
        }
    }
    reap_expired_barriers(stores, &round.plan.expired_barriers).await;
    let mut unproven = 0;
    for e in round.plan.eligible.clone() {
        let r = PartitionId(e.range);
        // The TOCTOU gap: table and references have been checked, nothing deleted yet.
        cairn_store::failpoint!("gc::between_check_and_delete", |arg: Option<String>| {
            Err(cairn_store::failpoint::error(
                "gc::between_check_and_delete",
                arg,
            ))
        });
        // The proof gate: rebuild the proof right before deleting — reload the
        // table, tombstone, barriers, references and checkpoints on **one**
        // snapshot, and the proof carries the version of that very snapshot.
        // The plan is only a list of candidates: a table change since the plan
        // (a split/merge commit, a new range, a newly retired range) does not
        // make the new proof wrong, so the round goes on (it used to skip the
        // whole round when the table changed, and under continuous automatic
        // splitting D-d almost never deleted anything — seen in a 4 h soak).
        // Not provable on the new snapshot ⇒ keep that range. The deletion
        // checks the proof's version again (`DeleteProof::check`): a table
        // change in that gap ⇒ keep that range (`failed`), the round goes on.
        //
        // A deletion step failing for one range (reading the tombstone /
        // barriers / references to rebuild the proof, or the deletion itself)
        // ⇒ keep that range, count it, go on.
        let proven = match cairn_store::proof::DeleteProof::retired_db_with(
            stores,
            cluster,
            r,
            wall_ms(),
            &mut round_refs,
        )
        .await
        {
            Ok(p) => p,
            Err(err) => {
                delete_failed(&mut round, e.range, err);
                continue;
            }
        };
        let proof = match proven {
            Ok(p) => p,
            Err(why) => {
                tracing::info!(range = e.range, reason = %why, "D-d: no longer provable after the check — keeping the Db");
                unproven += 1;
                continue;
            }
        };
        let n = match stores.delete_retired_db(proof, Some(cluster)).await {
            Ok(n) => n,
            Err(err) => {
                delete_failed(&mut round, e.range, err);
                continue;
            }
        };
        tracing::info!(
            range = e.range,
            objects = n,
            bytes = e.bytes,
            age_s = e.age_ms / 1000,
            "D-d: deleting the Db of a retired range nobody references any more"
        );
        round.deleted.push(e.range);
    }
    UNPROVEN_CANDIDATES.store(unproven, std::sync::atomic::Ordering::Relaxed);
    // The swept-tombstone log: a Db just deleted goes into the log only when
    // re-reading the manifest finds it absent (the deletion is durable); a
    // failed read ⇒ not added, the next round reads it again as before.
    let mut gone = Vec::new();
    for &r in &round.deleted {
        if let Ok(false) = stores.range_db_exists(PartitionId(r)).await {
            gone.push(r);
        }
    }
    write_swept(stores, &ledger, &ledger_update(&round, gone)).await;
    round.ops_reaped = reap_settled_ops(stores, cluster, &ops_due, cfg.min_age).await;
    for b in &round.plan.blocked {
        tracing::debug!(range = b.range, by = ?b.by, detail = %b.detail, "D-d: retired range blocked");
    }
    Ok(round)
}

/// Writes the next version of the swept-tombstone log. Failure or a lost CAS ⇒
/// only logged: the next round reads those tombstones again as before the log
/// existed.
async fn write_swept(
    stores: &ObjectStores,
    old: &cairn_store::gc_swept::Swept,
    up: &cairn_store::gc_swept::Update,
) {
    use cairn_store::gc_swept::{write, Wrote};
    let main = stores.main();
    match write(main.store(), main.prefix(), old, up).await {
        Ok(Wrote::Written) => {
            tracing::debug!(
                added = up.swept.len(),
                reread = up.full_pass_at.is_some(),
                "D-d: wrote the swept-tombstone log"
            )
        }
        Ok(Wrote::Unchanged) => {}
        Ok(Wrote::Lost) => {
            tracing::debug!("D-d: the swept-tombstone log just changed underfoot — the next round writes it again")
        }
        Err(e) => {
            tracing::warn!(error = %e, "D-d: cannot write the swept-tombstone log — the next round reads those tombstones again")
        }
    }
}

/// Records under `ops/` whose `last_modified` is older than `min_age` at
/// `now_ms` — one LIST. A failed LIST ⇒ no records (the next round retries);
/// sets the gauge of records seen.
async fn due_ops(stores: &ObjectStores, min_age: Duration, now_ms: u64) -> Vec<u32> {
    let main = stores.main();
    match cairn_store::ledger::list_ops(main.store(), main.prefix()).await {
        Ok(all) => {
            ops_stats::LISTED.store(all.len() as u64, std::sync::atomic::Ordering::Relaxed);
            all.into_iter()
                .filter(|(_, modified)| {
                    u128::from(now_ms.saturating_sub(*modified)) >= min_age.as_millis()
                })
                .map(|(k, _)| k)
                .collect()
        }
        Err(e) => {
            tracing::warn!(error = %e, "D-d: cannot LIST ops/ — leaving it to the next round");
            Vec::new()
        }
    }
}

/// **Operation-record cleanup**: the `ops/<key>` record of a settled op —
/// `Committed` with every source tombstoned, `Cleaned` with every child's Db
/// and owner record gone — is moved to the trash through the gate
/// [`cairn_store::proof::DeleteProof::settled_op`] (the core decides,
/// `Event::ReapLedger`), once D-d's `min_age` has passed since the record's
/// last write. Not provable or a failed read ⇒ keep, reconsider next round.
///
/// Runs in the D-d loop (not the 15 s orphan loop): same runner, same upgrade
/// and clock-skew gates, a 10-minute interval. Cost per round: one LIST of
/// `ops/`; for each old-enough record one GET of the record, one GET of the
/// table, 1–2 owner-record GETs (`Committed`) or 1–2 × (a manifest read + an
/// owner-record GET) (`Cleaned`); each cleaned record adds the trash's
/// HEAD + entry PUT + COPY + DELETE — once in the op's life.
pub async fn reap_settled_ops(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    due: &[u32],
    min_age: Duration,
) -> Vec<u32> {
    let main = stores.main();
    let mut reaped = Vec::new();
    for &key in due {
        let proven = cairn_store::proof::DeleteProof::settled_op(
            stores,
            cluster,
            key,
            min_age.as_millis() as u64,
            wall_ms(),
        )
        .await;
        let proof = match proven {
            Ok(Ok(p)) => p,
            Ok(Err(why)) => {
                tracing::debug!(key, reason = %why, "op record cleanup: keeping");
                continue;
            }
            Err(e) => {
                ops_stats::FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(key, error = %e, "op record cleanup: read failed — keeping, reconsidered next round");
                continue;
            }
        };
        match cairn_store::ledger::delete_op(main.store(), main.prefix(), proof).await {
            Ok(()) => {
                ops_stats::REAPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(
                    key,
                    "op record cleanup: op settled — moving ops/{key} to the trash"
                );
                reaped.push(key);
            }
            Err(e) => {
                ops_stats::FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(key, error = %e, "op record cleanup: moving to the trash failed — keeping");
            }
        }
    }
    reaped
}

/// Counters of the operation-record cleanup.
pub mod ops_stats {
    use std::sync::atomic::AtomicU64;

    /// Records moved to the trash, accumulated (`cairnflow_gc_ops_reaped_total`).
    pub static REAPED: AtomicU64 = AtomicU64::new(0);
    /// Failed cleanups of one record (read or move), accumulated.
    pub static FAILED: AtomicU64 = AtomicU64::new(0);
    /// Number of records under `ops/` at the latest LIST (`cairnflow_gc_ops_ledgers`).
    pub static LISTED: AtomicU64 = AtomicU64::new(0);
}

/// The deletion step of range `range` failed: keep the Db, count it, record it in [`GcRound::failed`].
fn delete_failed(round: &mut GcRound, range: u32, err: StoreError) {
    stats::SKIPPED_DELETE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!(range, error = %err, "D-d: deletion step failed — keeping the Db, retried next round");
    round.failed.push(Blocked {
        range,
        by: BlockedBy::Unreadable,
        detail: err.to_string(),
    });
}

/// One trash-purge round: entries past `keep` are checked again against
/// durable facts read **after** learning there are expired entries — the
/// table reloaded, then the `external_dbs` of every live range and every owner
/// record — before deleting for real. The cluster is migrating ⇒ skip the
/// round, as D-d does.
pub async fn purge_trash_once(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    keep: Duration,
) -> Result<cairn_store::trash::PurgeRound, StoreError> {
    let main = stores.main();
    if let Some(u) = cairn_store::migrate::upgrading(main.store(), main.prefix()).await? {
        tracing::info!(reason = ?u, "trash: skipping the round (cluster is migrating)");
        return Ok(Default::default());
    }
    // The clock-skew gate runs inside the fact read: only once there are
    // expired entries (by our clock), i.e. right when we are about to delete.
    let skewed = std::sync::atomic::AtomicBool::new(false);
    let round = cairn_store::trash::purge_expired_store_aged(
        &stores.trash_bins(),
        keep,
        wall_ms(),
        || async {
            if let Err(why) = clock_gate(stores, clock_skew_limit(keep), ClockRound::Trash).await {
                skewed.store(true, std::sync::atomic::Ordering::Relaxed);
                return Err(StoreError::Io(why));
            }
            cluster.reload().await?;
            cairn_store::trash::facts(stores, cluster.partitions()).await
        },
    )
    .await;
    match round {
        Err(_) if skewed.load(std::sync::atomic::Ordering::Relaxed) => Ok(Default::default()),
        r => r,
    }
}

/// Whether this node is the **runner** of the D-d loop: the leader of the
/// range with the smallest `RangeId` alive in the table.
///
/// No extra lease: leadership already exists. During a leader change two
/// nodes may both believe they are the runner for one tick — still safe,
/// because deletion is idempotent and each one checks again before deleting.
/// Asked through `flow`: the command answers in place, without queueing
/// behind the range's turns.
pub async fn is_runner(cluster: &ClusterConfig, partitions: &Partitions) -> bool {
    let Some(smallest) = cluster.partitions().into_iter().min_by_key(|p| p.0) else {
        return false;
    };
    let Some(h) = partitions.get(smallest) else {
        return false;
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), h.flow()).await,
        Ok(Ok(_))
    )
}

// ─────────────────────────────────────────── tombstone on behalf (SM-11)

/// The **safety-net interval** of the tombstone-on-behalf / orphan-child
/// cleanup loop: one hour. The loop runs on **kicks** — a split/merge that
/// just left the table or rolled back ([`intent_settled`]), another session
/// that just ended (`SessionTracker::ended`), the node just becoming the
/// runner ([`is_runner`] changed) — and the safety net only catches what no
/// kick reported (`cairn-ctl`, a kick missed while nobody was the runner).
/// Each round costs two LISTs (`owners/`, `ranges/`), plus one LIST per range
/// directory **outside the table** and one GET per record that left the table
/// not yet known to be a tombstone. Rule 5: slow cleanup only costs space. It
/// used to be 15 s, ~240·(2 + ranges) LISTs/h.
pub const DEFAULT_RETIRE_ORPHANS_EVERY: Duration = Duration::from_secs(60 * 60);

/// After a round left work unfinished because of possibly transient errors
/// ([`OrphanRound::needs_retry`]): the first retry after this long, doubling up
/// to the safety-net interval.
pub const ORPHAN_RETRY_FIRST: Duration = Duration::from_secs(1);

/// Whether the table changing from `old` to `new` **finished** (committed or
/// rolled back) a split/merge: a range carrying an intent in `old` has left
/// the table or carries another intent in `new`. That is kick (a) of the
/// orphan-child cleanup loop: a rollback leaves orphan children behind when
/// its cleanup is cut short (SM-14); a commit leaves sources waiting for their
/// owner's tombstone — if the owner has died nobody writes it (SM-11). Only a
/// hint of when to run the loop: every decision is still made on the table the
/// loop reloads.
pub fn intent_settled(
    old: &cairn_store::PartitionTable,
    new: &cairn_store::PartitionTable,
) -> bool {
    old.ranges().iter().any(|r| {
        r.intent
            .is_some_and(|i| new.range(r.id).is_none_or(|n| n.intent != Some(i)))
    })
}

/// Why the tombstone-on-behalf loop does **not** tombstone a range outside the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OrphanKept {
    /// A child promised by an intent in flight: its Db and owner record exist
    /// before the table commits.
    Promised,
    /// An id never issued (`≥ next_range_id`).
    Unallocated,
    /// The owner is still alive (or cannot be proven dead).
    OwnerLive,
    /// A read failed: the owner record, or the owner's session.
    Unreadable,
    /// The record carries a WAL kind this binary does not know — left to a binary that does.
    UnknownWalKind,
    /// The tombstone CAS lost: the record just changed. The next round reads it again.
    CasLost,
    /// An orphan child of a rolled-back intent whose Db cleanup failed — the
    /// Db and owner record stay intact, the next round continues.
    DiscardFailed,
    /// There is proof `r` never entered the table, but a range in the table
    /// still lists it in its `external_dbs` — or it cannot be proven that none
    /// does (a live range's manifest corrupt/missing, an unknown prefix). SM-14.
    Referenced,
    /// An unexpired GC barrier covers the Db about to be cleaned up.
    Barrier,
    /// The table changed between the reference check and the cleanup — the next round checks again.
    TableMoved,
    /// The operation record of the op that created the range says the op may
    /// have committed (`Prepared`, `Committed`): the range may have been in
    /// the table — left to SM-11/D-d.
    MayHaveCommitted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Kept {
    pub range: u32,
    pub by: OrphanKept,
    pub detail: String,
}

/// The result of one tombstone-on-behalf round.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrphanRound {
    /// The table version the round is based on (`0` = the round needed no table read).
    pub table_version: u64,
    /// Ranges just tombstoned.
    pub retired: Vec<u32>,
    /// Orphan children of a rolled-back intent just cleaned up (Db, final
    /// checkpoint on the source range, owner record).
    pub discarded: Vec<u32>,
    pub kept: Vec<Kept>,
    /// The round skipped — the reason (the cluster is upgrading).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
}

impl OrphanRound {
    /// The round left work that an **early** round could finish — skipped
    /// (upgrade, clock skew), or kept because a read/cleanup failed, a CAS was
    /// lost, the table just changed, or references could not be proven absent
    /// (a live range's manifest is corrupt). The background loop retries with
    /// exponential backoff instead of waiting for the one-hour safety net.
    /// Kept because the owner is alive, an intent is in flight, the record says
    /// the op may have committed, or a barrier: wait for a kick (a session
    /// ending, a table change) or the safety net.
    pub fn needs_retry(&self) -> bool {
        self.skipped.is_some()
            || self.kept.iter().any(|k| {
                matches!(
                    k.by,
                    OrphanKept::Unreadable
                        | OrphanKept::CasLost
                        | OrphanKept::DiscardFailed
                        | OrphanKept::TableMoved
                        | OrphanKept::Referenced
                )
            })
    }
}

/// **The tombstone-on-behalf loop** (SM-11): writes the tombstone of a range
/// that left the table when its owner died before writing it. The tombstone is
/// written by the leader when the engine retires (`reconcile` → `retire`); a
/// leader dying between the table commit and that moment means nobody takes
/// the range again (it has left the table), so nobody writes the tombstone —
/// the Db is orphaned forever (D-d only deletes tombstoned Dbs) and its WAL
/// reference pins the log floor.
///
/// The same loop also finishes cleaning up **the orphan children of a
/// rollback** (SM-14, [`discard_if_orphan_child`]) — before considering a
/// tombstone, because tombstoning and letting D-d delete the Db would leave
/// the child's final checkpoint on the live source range forever.
///
/// Remembers in RAM the ranges already seen as tombstones: a tombstone is
/// permanent, so later rounds do not GET them again.
#[derive(Debug, Default)]
pub struct OrphanRetirer {
    tombstones: std::collections::BTreeSet<u32>,
    /// D-d's "ranges referenced by live ranges" set
    /// ([`ObjectStores::referenced_ranges`]) at one table version — kept across
    /// rounds until the table changes. See [`LiveRefs`].
    live_refs: LiveRefs,
}

impl OrphanRetirer {
    /// One round. Tombstones range `r` only when all four of these can be
    /// proven (SM-11); every ambiguity keeps it, with a reason in
    /// [`OrphanRound::kept`]:
    ///
    /// 1. `r` is **not** in the table at the version just reloaded
    ///    (committed);
    /// 2. no intent of that table promises `r` — children of a split/merge in
    ///    flight have a Db and owner record **before** the table commits;
    /// 3. `r < next_range_id` — an issued id. These three together are
    ///    permanent (D22, `check_successor`): a table change after the read
    ///    cannot bring `r` back either, so the table need not stand still
    ///    until the CAS;
    /// 4. the owner record has been yielded, or the owner's session is dead by
    ///    **exactly the rule of [`Session::claim`]** (the tracker, or a GET
    ///    finding it gone / ended). Whoever may take the range back may also
    ///    tombstone it in its place.
    ///
    /// The tombstone is written with a CAS on exactly the version read in (4),
    /// as `retire` does: if the owner (or a taker) changes the record midway,
    /// the CAS loses and the next round reads it again. The tombstone copies
    /// every other field of the record (session, term, WAL index) — exactly
    /// what the leader would write.
    pub async fn run_once(
        &mut self,
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        session: &Session,
    ) -> Result<OrphanRound, StoreError> {
        let main = stores.main();
        let (store, prefix) = (main.store(), main.prefix());
        let mut round = OrphanRound::default();
        // Cheap filter: one LIST, skipping known tombstones and ranges in the
        // RAM table. The RAM table is only a filter (if stale, the next round
        // catches it); decisions rest on the table reloaded below.
        let ram = cluster.table();
        // `ranges/` is LISTed too: an orphan child whose owner record the
        // rollback already deleted (or never took) is only visible through its
        // Db directory. Directories of ranges in the RAM table or of known
        // tombstones need no emptiness check — they are filtered right below.
        let wanted = |r: PartitionId| !self.tombstones.contains(&r.0) && !ram.contains(r);
        let mut candidates: Vec<PartitionId> =
            cairn_store::inspect::owner_ranges(store, prefix).await?;
        candidates.extend(stores.range_dirs_where(wanted).await?);
        candidates.sort_by_key(|r| r.0);
        candidates.dedup();
        candidates.retain(|r| !self.tombstones.contains(&r.0) && !ram.contains(*r));
        if candidates.is_empty() {
            return Ok(round);
        }
        // The upgrade gate, as in D-d: a migration step may be going through
        // owner records. Not a safety condition — the CAS is the arbiter — but
        // a way not to cut in while the operator upgrades; waiting is cheap.
        if let Some(u) = cairn_store::migrate::upgrading(store, prefix).await? {
            let why = format!("cluster is upgrading: {u}");
            tracing::info!(reason = %why, "tombstone on behalf: skipping the round");
            round.skipped = Some(why);
            return Ok(round);
        }
        // The clock-skew gate: the tombstone carries `retired_ms` from our
        // clock (D-d computes ages from it), and the orphan-child cleanup
        // compares barriers with it. The threshold follows D-d's default
        // `min_age`.
        if let Err(why) = clock_gate(
            stores,
            clock_skew_limit(DEFAULT_MIN_AGE),
            ClockRound::Orphans,
        )
        .await
        {
            round.skipped = Some(why);
            return Ok(round);
        }
        cluster.reload().await?;
        let table = cluster.table();
        round.table_version = table.version();
        let promised = table.promised();
        for r in candidates {
            let mut keep = |by: OrphanKept, detail: String| {
                tracing::debug!(range = r.0, by = ?by, detail = %detail, "tombstone on behalf: keeping");
                round.kept.push(Kept {
                    range: r.0,
                    by,
                    detail,
                });
            };
            // (1) Entered the table between the filter and the reload: a live range, nothing to do.
            if table.contains(r) {
                continue;
            }
            // (3)
            if r.0 >= table.next_range_id() {
                keep(
                    OrphanKept::Unallocated,
                    format!("unissued id (next_range_id={})", table.next_range_id()),
                );
                continue;
            }
            // (2)
            if promised.contains(&r) {
                keep(
                    OrphanKept::Promised,
                    "a child of a split/merge in flight".into(),
                );
                continue;
            }
            // (1)–(3) are permanent: `r` never enters the table again. If its
            // Db stands on a **live** range, or it has a Db but no owner
            // record, the intent that created it never committed (SM-14) ⇒ `r`
            // is the orphan child of a rollback: clean it up for good,
            // including the final checkpoint on the source range — tombstoning
            // and letting D-d delete the Db would leave that checkpoint
            // behind.
            match discard_if_orphan_child(
                stores,
                cluster,
                &table,
                r,
                &mut self.live_refs,
                cairn_split::Remover::OrphanRound,
            )
            .await
            {
                Ok(OrphanChild::Discarded) => {
                    round.discarded.push(r.0);
                    continue;
                }
                Ok(OrphanChild::NotProven) => {}
                Ok(OrphanChild::Kept(by, detail)) => {
                    keep(by, detail);
                    continue;
                }
                Err(e) => {
                    keep(OrphanKept::DiscardFailed, e.to_string());
                    continue;
                }
            }
            let observed = match cairn_store::inspect::owner_observed(store, prefix, r).await {
                Ok(Some(o)) => o,
                // No owner record (a Db directory without a manifest — a
                // half-done cleanup — or no Db at all), or it vanished between
                // LIST and GET (the rollback path deletes the child's record):
                // nothing to tombstone.
                Ok(None) => continue,
                Err(e) => {
                    keep(OrphanKept::Unreadable, format!("owner record: {e}"));
                    continue;
                }
            };
            let rec = &observed.record;
            if rec.retired {
                self.tombstones.insert(r.0);
                continue;
            }
            // (4) — the core decides (`Event::Tomb`, SM-11); the owner's death
            // is only read when the owner has not yielded.
            let owner = if let cairn_store::WalKind::Unknown(_) = &rec.wal_kind {
                cairn_split::Owner::UnknownWal
            } else if rec.released {
                cairn_split::Owner::Released
            } else {
                cairn_split::Owner::Held {
                    dead: match session.is_session_dead(&rec.session).await {
                        Ok(d) => Ok(d),
                        Err(e) => {
                            tracing::debug!(range = r.0, error = %e, "tombstone on behalf: cannot read the owner's session");
                            Err(cairn_split::ReadErr)
                        }
                    },
                }
            };
            let view = cairn_split::View {
                orphan: Some(cairn_split::Orphan {
                    in_table: table.contains(r),
                    promised: promised.contains(&r),
                    allocated: r.0 < table.next_range_id(),
                    owner: Ok(owner),
                }),
                ..cairn_split::View::default()
            };
            match cairn_split::step(&view, &cairn_split::Event::Tomb) {
                cairn_split::Plan::Tombstone => {}
                cairn_split::Plan::Keep(cairn_split::Keep::UnknownWal) => {
                    if let cairn_store::WalKind::Unknown(kind) = &rec.wal_kind {
                        keep(OrphanKept::UnknownWalKind, format!("wal_kind={kind}"));
                    }
                    continue;
                }
                cairn_split::Plan::Keep(cairn_split::Keep::OwnerLive) => {
                    keep(
                        OrphanKept::OwnerLive,
                        format!("owner {} not yet proven dead", rec.session),
                    );
                    continue;
                }
                cairn_split::Plan::Keep(cairn_split::Keep::Unreadable) => {
                    keep(
                        OrphanKept::Unreadable,
                        format!("session {}: unreadable", rec.session),
                    );
                    continue;
                }
                cairn_split::Plan::Keep(cairn_split::Keep::Promised) => {
                    keep(
                        OrphanKept::Promised,
                        "a child of a split/merge in flight".into(),
                    );
                    continue;
                }
                cairn_split::Plan::Keep(cairn_split::Keep::Unallocated) => {
                    keep(OrphanKept::Unallocated, "id not issued yet".into());
                    continue;
                }
                cairn_split::Plan::Ignore => continue,
                other => unreachable!("the core returned {other:?} for Tomb"),
            }
            // Tripwire: reaching here means a tombstone is about to be written.
            cairn_store::failpoint!("gc::retire_orphan::before_cas");
            let tomb = cairn_store::OwnerRecord {
                released: true,
                retired: true,
                retired_ms: Some(wall_ms()),
                wal_segments: tomb_segments(),
                ..rec.clone()
            };
            match cairn_store::inspect::cas_owner(store, prefix, r, &tomb, Some(observed.version))
                .await
            {
                Ok(true) => {
                    tracing::info!(
                        range = r.0,
                        owner = %rec.session,
                        released = rec.released,
                        version = table.version(),
                        "tombstone on behalf: the range left the table, its owner died before writing the tombstone"
                    );
                    self.tombstones.insert(r.0);
                    round.retired.push(r.0);
                }
                Ok(false) => keep(OrphanKept::CasLost, "the owner record just changed".into()),
                Err(e) => keep(OrphanKept::CasLost, format!("tombstone CAS: {e}")),
            }
        }
        Ok(round)
    }
}

/// The `wal_segment` of a tombstone written on behalf (SM-11,
/// `cairn-ctl range force-retire`): **empty**. Only a range's taker reads
/// `wal_segment` (sealing, replay), and a tombstoned range is never taken again
/// (LS-8), and its children already cloned from its L0 after quiesce (SM-4). Copying
/// the dead owner's segments would keep that log listed forever and never
/// cleaned up for good (NW-8); dropping them only removes this range's own
/// reference — a log still listed by another owner record is still kept.
pub fn tomb_segments() -> Vec<cairn_store::WalSegment> {
    Vec::new()
}

/// D-d's "ranges referenced by live ranges" set
/// ([`ObjectStores::referenced_ranges`]: every range that the Dbs of the
/// ranges in the table still list in their `external_dbs`) at one table
/// version, for [`discard_if_orphan_child`]. Read lazily — only once a
/// candidate has passed every cheaper gate — and kept until the table version
/// changes: one manifest GET per live range per version, not per round.
///
/// Keeping it across rounds is conservative: at a fixed version, a live
/// range's `external_dbs` only **shrinks** (compaction collects `sst_ids`, GC
/// `detach` drops entries) — its Db is created once, before it enters the
/// table — so the set read is a superset of the real set later on. A set with
/// unprovable parts (`unknown`: a live Db without a manifest yet, a failed
/// read, an unknown prefix) is not kept: it may heal by itself without the
/// table changing.
#[derive(Debug, Default)]
pub(crate) struct LiveRefs(Option<(u64, cairn_store::RangeRefs)>);

impl LiveRefs {
    async fn get(
        &mut self,
        stores: &ObjectStores,
        table: &cairn_store::PartitionTable,
    ) -> Result<&cairn_store::RangeRefs, StoreError> {
        if self
            .0
            .as_ref()
            .is_none_or(|(v, refs)| *v != table.version() || refs.unknown.is_some())
        {
            let refs = stores
                .referenced_ranges(&table.partitions().collect::<Vec<_>>())
                .await?;
            self.0 = Some((table.version(), refs));
        }
        Ok(&self.0.as_ref().expect("just set").1)
    }
}

/// The result of [`discard_if_orphan_child`].
#[derive(Debug)]
pub(crate) enum OrphanChild {
    /// Cleaned up the Db and owner record.
    Discarded,
    /// Cannot prove `r` never entered the table — left to SM-11 and D-d.
    NotProven,
    /// There is proof `r` never entered the table, but it cannot be proven
    /// that nobody stands on it (or the table just changed) — kept, with a
    /// reason.
    Kept(OrphanKept, String),
}

/// Cleans up `r` if it can be proven to be **the orphan child of a
/// rolled-back intent** and **nobody stands on it**: the caller has proven `r`
/// is not in `table`, not promised and already issued (permanently outside the
/// table — SM-1); this adds **one of two** proofs that `r` never entered the
/// table (SM-14):
///
/// - `r`'s Db is listed in the `external_dbs` of a range **in `table`**.
///   Committing a split/merge takes every source out of the table, and a
///   source never comes back (ids are not reused).
/// - `r`'s Db has a manifest while `r` **has no owner record**. The
///   coordinator takes ownership of every child (`claim_children`) **before**
///   the commit CAS, and the owner record of a range that was ever in the
///   table is never deleted while its Db still has a manifest: tombstones are
///   permanent, D-d only deletes Dbs, and every other `delete_owner` path only
///   cleans up children of an intent the CAS abandoned (`discard_orphan`,
///   `cairn-ctl intent clear`, this very function) or ranges that no longer
///   have a Db (`discard_own_orphans`). This proof holds even when every
///   source has retired — the gap in the first proof (the rollback dies while
///   cleaning up the children, and a later split/merge takes the source out
///   of the table).
///
/// **Both proofs are inferences from history**, not what deletion needs. The
/// second proof is wrong on a bucket that a single-process node split before
/// the range-level migration framework existed: there the retired parent has
/// no owner record, and the children still stand on it. So before cleaning
/// up, the function checks **directly** what deletion needs, with exactly
/// D-d's checks (rule 5):
///
/// 1. no range in `table` lists `r` in its `external_dbs`
///    ([`ObjectStores::referenced_ranges`]; a failed read, a live Db without a
///    manifest yet, an unknown prefix ⇒ keep). Applies to **both** proofs:
///    with the first one it should never see anything (an uncommitted child
///    is cloned by nobody), but a bucket built by an old binary or by an
///    operator's hand can break the history that inference relies on, and the
///    reference set already read is shared anyway;
/// 2. no GC barrier covers `r` (references from **outside** the table —
///    `cairn-ctl fork` — are only visible through barriers, SM-9);
/// 3. the table version right before deleting is still `table`'s version (as
///    in D-d).
///
/// Seeing a real reference in (1) is the "must never happen" path on buckets
/// born after the range-level migration framework: the tripwire
/// `gc::discard_orphan::referenced`.
///
/// The gate [`cairn_store::proof::DeleteProof::abandoned_child_db`] checks
/// (1)–(3) again on its own snapshot; checking here is to keep the range with
/// a classified reason ([`OrphanKept`]) and to reuse [`LiveRefs`] for the
/// whole round — the gate refusing ⇒ `Err` (kept,
/// [`OrphanKept::DiscardFailed`]).
///
/// Never in the table ⇒ nobody reads it. A failed owner-record read ⇒ `Err`
/// (kept). Clean up the Db (with the final checkpoints on the ancestors that
/// still have manifests) **then** the owner record; a failed Db cleanup keeps
/// both, returns the error, and the next round continues.
pub(crate) async fn discard_if_orphan_child(
    stores: &ObjectStores,
    cluster: &ClusterConfig,
    table: &cairn_store::PartitionTable,
    r: PartitionId,
    live_refs: &mut LiveRefs,
    by: cairn_split::Remover,
) -> Result<OrphanChild, StoreError> {
    use cairn_split::{Child, Event, Gate, Keep, Need, Plan, Refs, View};
    let main = stores.main();
    let sources = stores.external_ranges(r).await?;
    let live: Vec<PartitionId> = sources
        .iter()
        .flatten()
        .copied()
        .filter(|s| table.contains(*s))
        .collect();
    // The owner record is only read when it decides (the second proof: has a
    // Db, does not stand on a live range).
    let owner = if sources.is_some() && live.is_empty() {
        cairn_store::read_owner(main.store(), main.prefix(), r)
            .await?
            .is_some()
    } else {
        true
    };
    let proof = if !live.is_empty() {
        "stands on a live range"
    } else {
        "never had an owner record"
    };
    // The core decides (`Event::Discard`); the facts of the direct check
    // (1)–(3) are read lazily in the same order as before: the reference set,
    // barriers, then the table version.
    let mut view = View {
        child: Some(Child {
            in_table: table.contains(r),
            promised: table.promised().contains(&r),
            allocated: r.0 < table.next_range_id(),
            db: sources.is_some(),
            owner,
            stands_on_live: !live.is_empty(),
        }),
        ..View::default()
    };
    let (mut referenced_by, mut unknown): (Option<u32>, Option<String>) = (None, None);
    let mut barrier: Option<Option<String>> = None;
    let mut rechecked = false;
    loop {
        match cairn_split::step(&view, &Event::Discard(by)) {
            // The record of the op that created `r` — `Cleaned` is positive
            // proof of "never in the table", `Prepared`/`Committed` proof of
            // the opposite. A failed read ⇒ infer as before the records
            // existed (the core); the gate still reads it again.
            Plan::Need(Need::Ledger) => {
                view.ledger = Some(
                    match cairn_store::ledger::read_op_of_child(main.store(), main.prefix(), r.0)
                        .await
                    {
                        Ok(o) => Ok(o.map(|o| o.record.phase)),
                        Err(e) => {
                            tracing::debug!(range = r.0, error = %e, "orphan-child cleanup: cannot read the operation record");
                            Err(cairn_split::ReadErr)
                        }
                    },
                )
            }
            Plan::Need(Need::Gate) => {
                // (1) No range in the table stands on `r` — the same set as D-d's.
                let refs = live_refs.get(stores, table).await?;
                referenced_by = refs.by.get(&r.0).copied();
                unknown = refs.unknown.clone();
                view.gate = Some(Ok(Gate {
                    table_version: table.version(),
                    refs_version: table.version(),
                    refs: if referenced_by.is_some() {
                        Refs::Referenced
                    } else if unknown.is_some() {
                        Refs::Unknown
                    } else {
                        Refs::Clear
                    },
                    refs_in_flight: false,
                    barrier: false,
                    moved: false,
                }));
            }
            Plan::DiscardDb if barrier.is_none() => {
                // (2) GC barriers.
                let barriers = gc_barrier::list_barriers(main.store(), main.prefix()).await?;
                let by = blocking_barriers(&barriers, r, wall_ms());
                if let Some(Ok(g)) = &mut view.gate {
                    g.barrier = by.is_some();
                }
                barrier = Some(by);
            }
            Plan::DiscardDb if !rechecked => {
                // (3) Check again right before deleting: a table change ⇒ the reference set is stale.
                cairn_store::failpoint!("gc::discard_orphan::before_recheck");
                cluster.reload().await?;
                if let Some(Ok(g)) = &mut view.gate {
                    g.moved = cluster.version() != table.version();
                }
                rechecked = true;
            }
            Plan::DiscardDb => break,
            Plan::Keep(Keep::NotProven) | Plan::DeleteOwner | Plan::Ignore => {
                return Ok(OrphanChild::NotProven)
            }
            Plan::Keep(Keep::Referenced) => {
                if let Some(by) = referenced_by {
                    // Tripwire: about to clean up a Db that a live range stands on.
                    cairn_store::failpoint!("gc::discard_orphan::referenced");
                    tracing::warn!(
                        range = r.0,
                        referenced_by = by,
                        proof = proof,
                        version = table.version(),
                        "orphan-child cleanup: a range outside the table with proof it never entered the table, but a live range still stands on it — keeping (a bucket from before range-level migration?)"
                    );
                    return Ok(OrphanChild::Kept(
                        OrphanKept::Referenced,
                        format!(
                            "live range {by} still lists it in its external_dbs (proof: {proof})"
                        ),
                    ));
                }
                return Ok(OrphanChild::Kept(
                    OrphanKept::Referenced,
                    unknown.unwrap_or_default(),
                ));
            }
            Plan::Keep(Keep::Barrier) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::Barrier,
                    barrier.flatten().unwrap_or_default(),
                ))
            }
            Plan::Keep(Keep::TableMoved) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::TableMoved,
                    format!(
                        "range table changed from version {} to {} after the check",
                        table.version(),
                        cluster.version()
                    ),
                ))
            }
            Plan::Keep(Keep::Promised) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::Promised,
                    "a child of a split/merge in flight".into(),
                ))
            }
            // The record says the op may have committed: the range may have
            // been in the table — the cleanup loop leaves it to the tombstone
            // branch (SM-11) and D-d; the coordinator keeps it.
            Plan::Keep(Keep::MayHaveCommitted) if by == cairn_split::Remover::OrphanRound => {
                return Ok(OrphanChild::NotProven)
            }
            Plan::Keep(Keep::MayHaveCommitted) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::MayHaveCommitted,
                    "the operation record says the op that created it may have committed".into(),
                ))
            }
            Plan::Keep(Keep::Unallocated) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::Unallocated,
                    format!("unissued id (next_range_id={})", table.next_range_id()),
                ))
            }
            Plan::Keep(k) => {
                return Ok(OrphanChild::Kept(
                    OrphanKept::DiscardFailed,
                    format!("core keeps: {k:?}"),
                ))
            }
            other => unreachable!("the core returned {other:?} for Discard({by:?})"),
        }
    }
    // Tripwire: about to clean up the Db of a range outside the table.
    cairn_store::failpoint!("gc::discard_orphan::before_discard");
    crate::split::discard_child_now(stores, cluster, r, &live).await?;
    tracing::info!(
        range = r.0,
        sources = ?live.iter().map(|p| p.0).collect::<Vec<_>>(),
        proof = proof,
        version = table.version(),
        "orphan-child cleanup: a range outside the table never entered it — the intent that created it was rolled back"
    );
    Ok(OrphanChild::Discarded)
}

// ─────────────────────────────────────────── admin API

/// What `/admin/gc/*` needs: the bucket, the range table, and the node's
/// `min_age` (for the plan — even when the node's D-d loop is off: operators
/// look before turning it on).
#[derive(Clone)]
pub struct GcAdmin {
    pub stores: ObjectStores,
    pub cluster: ClusterConfig,
    pub min_age: Duration,
}

/// Error of the barrier API: `Bad` ⇒ `400`, otherwise ⇒ `500`.
#[derive(Debug)]
pub enum GcAdminError {
    Bad(String),
    Store(StoreError),
}

impl From<StoreError> for GcAdminError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}

/// Body of `PUT /admin/gc/barriers/{id}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BarrierRequest {
    /// Who holds it — required: GC logs name whoever is blocking.
    pub holder: Option<String>,
    /// `"all"`, `"ranges:1,2"`, hay `[1, 2]`.
    pub protects: Option<serde_json::Value>,
    /// Required, `≤` [`MAX_BARRIER_TTL`]. `0` ⇒ remove the barrier.
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub reason: String,
}

/// A barrier as the API returns it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BarrierView {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protects: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<bool>,
    /// An unreadable barrier (blocks every range, nobody deletes it on their own).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl BarrierView {
    fn of(b: &StoredBarrier, now_ms: u64) -> Self {
        match &b.barrier {
            Ok(g) => Self {
                id: b.id.clone(),
                holder: Some(g.holder.clone()),
                protects: Some(g.protects.to_string()),
                expires_ms: Some(g.expires_ms),
                reason: Some(g.reason.clone()),
                expired: Some(g.expired(now_ms)),
                error: None,
            },
            Err(e) => Self {
                id: b.id.clone(),
                holder: None,
                protects: None,
                expires_ms: None,
                reason: None,
                expired: None,
                error: Some(e.clone()),
            },
        }
    }
}

fn protects_of(v: &serde_json::Value) -> Option<Protects> {
    match v {
        serde_json::Value::String(s) => Protects::parse(s),
        serde_json::Value::Array(ids) => {
            let set: Option<std::collections::BTreeSet<u32>> = ids
                .iter()
                .map(|i| i.as_u64().and_then(|i| u32::try_from(i).ok()))
                .collect();
            set.filter(|s| !s.is_empty()).map(Protects::Ranges)
        }
        _ => None,
    }
}

impl GcAdmin {
    /// The plan of the D-d round if it ran now.
    pub async fn plan(&self) -> Result<GcPlan, StoreError> {
        plan(&self.stores, &self.cluster, self.min_age, wall_ms()).await
    }

    pub async fn list_barriers(&self) -> Result<Vec<BarrierView>, StoreError> {
        let main = self.stores.main();
        let now = wall_ms();
        Ok(gc_barrier::list_barriers(main.store(), main.prefix())
            .await?
            .iter()
            .map(|b| BarrierView::of(b, now))
            .collect())
    }

    pub async fn get_barrier(&self, id: &str) -> Result<Option<BarrierView>, GcAdminError> {
        gc_barrier::check_barrier_id(id).map_err(GcAdminError::Bad)?;
        let main = self.stores.main();
        Ok(gc_barrier::read_barrier(main.store(), main.prefix(), id)
            .await?
            .map(|b| BarrierView::of(&b, wall_ms())))
    }

    /// Places, renews or (TTL=0) removes barrier `id`. Returns the barrier
    /// written together with:
    /// - `previous`: `absent` | `active` | `expired` | `unreadable` — a late
    ///   renewer learns its barrier **expired** midway (GC may have run in that
    ///   gap);
    /// - `missing`: the covered ranges whose Db is no longer on the bucket and
    ///   that are not in the table — a late registrant learns it lost, rather
    ///   than losing silently.
    pub async fn put_barrier(
        &self,
        id: &str,
        req: BarrierRequest,
    ) -> Result<serde_json::Value, GcAdminError> {
        gc_barrier::check_barrier_id(id).map_err(GcAdminError::Bad)?;
        let main = self.stores.main();
        let now = wall_ms();
        let ttl = req.ttl_ms.ok_or_else(|| {
            GcAdminError::Bad("ttlMs required (0 = remove); no unbounded barriers".into())
        })?;
        let previous = match gc_barrier::read_barrier(main.store(), main.prefix(), id).await? {
            None => "absent",
            Some(StoredBarrier {
                barrier: Err(_), ..
            }) => "unreadable",
            Some(StoredBarrier { barrier: Ok(b), .. }) if b.expired(now) => "expired",
            Some(_) => "active",
        };
        if ttl == 0 {
            gc_barrier::delete_barrier(main.store(), main.prefix(), id).await?;
            tracing::info!(barrier = %id, "removed GC barrier");
            return Ok(
                serde_json::json!({ "id": id, "deleted": previous != "absent", "previous": previous }),
            );
        }
        if u128::from(ttl) > MAX_BARRIER_TTL.as_millis() {
            return Err(GcAdminError::Bad(format!(
                "ttlMs {ttl} exceeds the cap {} — renew periodically instead of placing a long barrier",
                MAX_BARRIER_TTL.as_millis()
            )));
        }
        let holder = req.holder.filter(|h| !h.trim().is_empty()).ok_or_else(|| {
            GcAdminError::Bad("holder is required: GC logs name whoever is blocking".into())
        })?;
        let protects = req.protects.as_ref().and_then(protects_of).ok_or_else(|| {
            GcAdminError::Bad(
                "protects must be \"all\", \"ranges:<id>,…\" or a non-empty [<id>, …]".into(),
            )
        })?;
        let barrier = GcBarrier {
            holder,
            protects,
            expires_ms: now + ttl,
            reason: req.reason,
        };
        gc_barrier::put_barrier(main.store(), main.prefix(), id, &barrier).await?;
        tracing::info!(barrier = %id, holder = %barrier.holder, protects = %barrier.protects, ttl_ms = ttl, "placed GC barrier");
        let mut missing = Vec::new();
        if let Protects::Ranges(ids) = &barrier.protects {
            for r in ids {
                let r = PartitionId(*r);
                if !self.cluster.contains(r) && !self.stores.range_db_exists(r).await? {
                    missing.push(r.0);
                }
            }
        }
        let stored = StoredBarrier {
            id: id.to_string(),
            barrier: Ok(barrier),
        };
        let mut out = serde_json::to_value(BarrierView::of(&stored, now)).unwrap_or_default();
        out["previous"] = previous.into();
        out["missing"] = missing.into();
        Ok(out)
    }
}
