// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Per-node leases** (D20): one session per process, renewed once for every partition of
//! the node — replacing per-partition leases, which renewed each partition separately and
//! made the idle cost grow with the number of partitions.
//!
//! # Three objects
//!
//! - `sessions/<node_id>/<inc>` — the session of one incarnation: `beat` grows on every
//!   renewal (a CAS every TTL/5), `owns` is a **hint** of the partitions it holds, `state`
//!   is `live`, `expired` or `closed`. The last two states are **permanent**: a lost
//!   session means opening a new incarnation, never reviving the old one.
//! - `owners/range-<id>` ([`crate::owner`]) — the source of truth about a range's owner,
//!   written only when the owner changes; carries `wal_kind` (D-f).
//!
//! # Why it is correct
//!
//! The owner issues its k-th renewal at `t_k` with `beat = b_k`; it is valid until
//! `t_k + ⅔·TTL` (the deadline is taken from the **issue** time of the renewal that won the
//! CAS, `extend_until`). An observer ([`SessionTracker`]) remembers the largest beat `m` it
//! has seen and a mark `τ` — when it **received** the first GET response carrying `m`. When
//! `now − τ ≥ TTL` and `m` has not grown, it CASes `state=expired` onto the etag it just
//! read. If that CAS succeeds at `X`, then `t_m + ⅔·TTL ≤ τ + ⅔·TTL < τ + TTL ≤ X`: every
//! grant of the owner has expired before `X` (assuming the two clocks' rates differ by less
//! than 1.5×). The owner's next renewal carries the old etag and so misses — the owner
//! knows it has lost **every** partition.
//!
//! The `expired` mark is **required**: without it, a frozen node waking up would renew
//! successfully and keep serving reads on the partitions it has lost.
//!
//! # Suspension and revival by CAS
//!
//! Past `t_k + ⅔·TTL` with no renewal back yet, the grant is **suspended**: no longer valid
//! (no acks, no reads — LS-2), but the engine does not step down and no range is yielded.
//! The renewal loop keeps trying: a CAS that wins from **exactly the previous version**
//! moves the deadline to `t_j + ⅔·TTL` of that very issue `t_j` and the grant runs again.
//! Correct because the proof above does not need the owner to still be within its own
//! deadline: it only needs "an observer CASing `expired` (or overwriting) changes the
//! version" — a winning renewal CAS ⇒ nobody has done that ⇒ nobody has taken a range
//! (taking requires the session to be dead on the bucket, LS-3); an observer sees the new
//! beat at `τ' ≥ t_j` so `t_j + ⅔·TTL < τ' + TTL`, as with every renewal. A CAS that misses
//! and whose re-read shows an ended record, an unknown beat or a missing record ⇒ `lose` as
//! before. Suspension is bounded (`pause_cap`: the last issue + TTL + TTL/3, past which an
//! observer has almost certainly CASed `expired`) — past the cap with no CAS back ⇒ `lose`,
//! so a hung renewal loop does not hold ranges forever. The cap counts from the last
//! **validity** deadline, not from the last CAS: a win whose new deadline is still in the
//! past (a PUT slower than ⅔·TTL) does not move the cap, so a node whose every PUT is that
//! slow still drops its ranges after the cap instead of staying suspended forever while
//! nobody can take its ranges.
//!
//! A partition's grant is nested under the session's validity
//! ([`LeadershipGrant::from_session`]): it cannot outlive the session, even when the
//! `owners/` write succeeds while the session renewal is hanging.
//!
//! Writes are still protected by SlateDB's `writer_epoch`, as always — this layer only
//! decides who *should* lead and blocks stale reads.

use crate::entropy::HashSet;
// The runtime's clock: outside simulation it is `std::time::Instant`, inside deterministic
// simulation it is the virtual clock.
use tokio::time::Instant;
use {
    crate::{
        error::{Result, StoreError},
        owner::{self, OwnerRecord, SessionId},
        writer::{LeadershipGrant, PartitionId, ReadFence, ValidityHandle},
    },
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
        UpdateVersion,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::sync::{watch, Notify},
};

mod record;
pub use record::*;
mod tracker;
pub use tracker::*;
mod membership;
pub use membership::*;
mod claim;
pub use claim::*;
mod renew;
use renew::*;
mod keeper;
pub use keeper::*;
mod under_lock;
pub use under_lock::*;
mod coordinator;
pub use coordinator::*;
mod delegate;
use delegate::*;
pub use delegate::{stats as delegation_stats, LeaseAnswer, REDELEGATE_AFTER_TTLS};
mod fence;
mod incarnation;
pub use fence::stats as read_fence_stats;

/// Default tick of the owner-record watch loop's **safety net**: each tick one conditional
/// GET (`If-None-Match`, almost always `304`) per range we hold. The fast path is the
/// taker's report ([`Session::on_owner_taken`] at the taker, [`Session::owner_changed`] at
/// the old owner) and the coordinator's `view` (`cairn-server/src/control.rs`); the net
/// catches it when both are lost (network to the taker and the coordinator, the taker dying
/// right after its CAS, `cairn-ctl`). This layer only handles liveness: the old owner's
/// writes are stopped by `writer_epoch`/the seal (LS-4), reads by the read fence (LS-5).
/// Earlier: one LIST of `owners/` per TTL, then 10 minutes.
pub const DEFAULT_OWNER_WATCH: Duration = Duration::from_secs(60);

/// Tick of reading the `control/quiet-lease` gate while we are the only unfinished session
/// or in `solo`: one GET with `If-None-Match` (Class B, almost always `304`). The gate is
/// also read once right after opening an incarnation. Turning the gate on takes effect
/// within one tick; turning it off (the downgrade runbook) brings the node back to `direct`
/// within one tick.
pub const DEFAULT_QUIET_LEASE_POLL: Duration = Duration::from_secs(60);

/// Tick of the tracker's `sessions/` LIST **safety net** while in `solo` (the loops differ
/// when alone). Today (direct) it is one LIST per TTL.
pub const DEFAULT_QUIET_LIST_EVERY: Duration = Duration::from_secs(10 * 60);

/// Taking a range from **another** session that may still believe it is the owner (has
/// not yielded) — what the taker reports to the old owner. Only a hint: the old owner GETs
/// the owner record itself before revoking its grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerTaken {
    pub range: PartitionId,
    /// The old owner's session, according to the record the taker's CAS was based on.
    pub from: SessionId,
    /// The old owner's internal port (`address=` of the owner record). `None` ⇒ nowhere to
    /// report.
    pub address: Option<String>,
    /// Term of the new record.
    pub term: u64,
}

/// Listener of a session's [`OwnerTaken`] ([`Session::on_owner_taken`]).
pub type OwnerTakenHook = Arc<dyn Fn(OwnerTaken) + Send + Sync>;

/// Ranges the old owner was told just changed owner, waiting for the watch loop to
/// re-read.
#[derive(Default)]
pub(super) struct OwnerHints {
    pub(super) ranges: Mutex<BTreeSet<u32>>,
    pub(super) notify: Notify,
}

/// Lease configuration.
#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// How long **without seeing the lease change** before an observer considers the old
    /// owner dead and tries to take over. Measured by the observer's clock.
    pub ttl: Duration,
    /// Session renewal tick (and the observation tick, when [`Self::observe_interval`] is
    /// absent).
    ///
    /// Must be smaller than `ttl` by a significant margin: the owner may only hold its
    /// rights until `2/3 * ttl` after the last renewal, so a tick sparser than that means
    /// demoting itself between two successful renewals.
    pub renew_interval: Duration,
    /// Tick of the loop observing other nodes' sessions ([`SessionTracker`]): one GET per
    /// other unfinished session per round. `None` ⇒ equal to `renew_interval`. Cluster nodes
    /// set TTL/3: sparser than the renewal tick because it only decides how fast a dead node
    /// is **detected** (at most one tick later), not who holds the rights.
    pub observe_interval: Option<Duration>,
    /// The identity **of this process**, not of the machine.
    ///
    /// The session lives at `sessions/<node_id>/<inc>`; reusing the id across runs is still
    /// safe — the next run opens a new incarnation (a larger `inc`) and waits for the old
    /// incarnation to be marked like any other node.
    pub node_id: String,
    /// The HTTP address this node takes requests on, e.g. `http://10.0.0.7:8080`.
    ///
    /// Written into the session and owner records so that **the routing table needs no
    /// channel of its own**: who holds which partition is already in the bucket, so "where
    /// is the leader of partition p" is a GET, not a gossip protocol.
    ///
    /// With `None` the partition still runs, there is just nobody pointing the way to it.
    ///
    /// This is the node's **internal port** — where peers send cross-range messages and ask
    /// for range releases; clients cannot reach it. The address for clients is
    /// [`Self::ingress`].
    pub address: Option<String>,
    /// The node's **ingress** address (the customer API) — what is returned to clients in
    /// the `leader` hint of a `503` and in `/cluster`. `None` ⇒ nobody points clients to
    /// this node (clients ask again).
    pub ingress: Option<String>,
    /// The runtime running the renewal and observation loops. `None` ⇒ the runtime of the
    /// caller of [`Session::establish`].
    ///
    /// Must not be a partition's `current_thread`: an engine task blocking the thread there
    /// would stall the renewal loop too, and the whole node would be considered dead just
    /// because one partition is busy.
    pub runtime: Option<tokio::runtime::Handle>,
    /// The **safety-net** tick of the owner-record watch loop — see [`Session::establish`].
    /// `None` ⇒ [`DEFAULT_OWNER_WATCH`] (1 minute; earlier 10 minutes, and before that
    /// `ttl`).
    pub owner_watch: Option<Duration>,
    /// The last migration step this binary knows — written into the session record
    /// (`knows=`). Default [`crate::migrate::KNOWS`].
    pub knows: u32,
    /// The node's PUT latency meter: each renewal is measured into it, and what it
    /// publishes (`slow=`, `p99_put_ms=`, `draining=`) is written into the session record on
    /// each renewal. `None` = no measuring, no publishing.
    pub meter: Option<crate::health::SlowMeter>,
    /// Tick of reading the quiet renewal gate — [`DEFAULT_QUIET_LEASE_POLL`].
    pub quiet_lease_poll: Duration,
    /// Tick of the `sessions/` LIST safety net while in `solo` — [`DEFAULT_QUIET_LIST_EVERY`].
    pub quiet_list_every: Duration,
}

impl LeaseConfig {
    /// TTL 10s, renewal every 2s, a node id generated per process.
    ///
    /// 10s is not an optimal number — it is the number **safe for GC pauses**. An early
    /// writer-fence experiment measured that an old leader waking up after 30s is still
    /// fenced, so a short TTL does not cause data loss; it only trades the rate of needless
    /// failovers for detection time. It can be lowered once there are measurements of real
    /// pauses.
    pub fn new() -> Self {
        Self {
            ttl: Duration::from_secs(10),
            renew_interval: Duration::from_secs(2),
            observe_interval: None,
            node_id: process_identity(),
            address: None,
            ingress: None,
            runtime: None,
            owner_watch: None,
            knows: crate::migrate::KNOWS,
            meter: None,
            quiet_lease_poll: DEFAULT_QUIET_LEASE_POLL,
            quiet_list_every: DEFAULT_QUIET_LIST_EVERY,
        }
    }

    /// Tick of reading the quiet renewal gate. See [`Self::quiet_lease_poll`].
    pub fn with_quiet_lease_poll(mut self, every: Duration) -> Self {
        self.quiet_lease_poll = every;
        self
    }

    /// Tick of the `sessions/` LIST safety net while in `solo`. See
    /// [`Self::quiet_list_every`].
    pub fn with_quiet_list_every(mut self, every: Duration) -> Self {
        self.quiet_list_every = every;
        self
    }

    /// The PUT latency meter. See [`Self::meter`].
    pub fn with_meter(mut self, meter: crate::health::SlowMeter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// The last migration step the binary knows. See [`Self::knows`].
    pub fn with_knows(mut self, knows: u32) -> Self {
        self.knows = knows;
        self
    }

    /// Tick of the owner-record watch loop. See [`Self::owner_watch`].
    pub fn with_owner_watch(mut self, every: Duration) -> Self {
        self.owner_watch = Some(every);
        self
    }

    pub fn with_address(mut self, addr: impl Into<String>) -> Self {
        self.address = Some(addr.into());
        self
    }

    /// Ingress address. See [`Self::ingress`].
    pub fn with_ingress(mut self, addr: impl Into<String>) -> Self {
        self.ingress = Some(addr.into());
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn with_renew_interval(mut self, d: Duration) -> Self {
        self.renew_interval = d;
        self
    }

    /// Tick of the observation loop. See [`Self::observe_interval`].
    pub fn with_observe_interval(mut self, d: Duration) -> Self {
        self.observe_interval = Some(d);
        self
    }

    pub fn with_node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }

    pub fn with_runtime(mut self, rt: tokio::runtime::Handle) -> Self {
        self.runtime = Some(rt);
        self
    }
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// A unique identity for one **run** of the process.
fn process_identity() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "node".into());
    let nanos = crate::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{host}/{}/{nanos}", crate::entropy::process_id())
}

/// Upper bound for the first release write. Past it, the session retries.
pub const RELEASE_WRITE_BOUND: Duration = Duration::from_secs(2);

/// How many TTLs a session may stand still before, if it cannot be marked `expired`, an
/// observer overwrites it so the next CAS wins — see `SessionTracker::rearm`.
const UNEXPIRABLE_AFTER_TTLS: u32 = 3;

/// How long a session stays in an ended state before it is cleaned up from the bucket.
const GC_AFTER_TTLS: u32 = 6;

/// Minimum gap between two observation rounds, even when asked continuously.
const MIN_REFRESH_GAP: Duration = Duration::from_millis(200);

/// The self-demotion deadline, counted from when the renewal was **issued**.
fn renew_deadline(issued_at: Instant, ttl: Duration) -> Instant {
    issued_at + ttl.mul_f32(2.0 / 3.0)
}

/// The suspension window after the self-demotion deadline: the suspension cap is
/// `issued_at + TTL + TTL/3` — one silent TTL plus one observation tick (TTL/3 on cluster
/// nodes), past which an observer has almost certainly CASed `expired` and every renewal
/// misses.
fn pause_after_deadline(ttl: Duration) -> Duration {
    (ttl + ttl / 3).saturating_sub(ttl.mul_f32(2.0 / 3.0))
}

/// Counter of **revivals**: a suspended grant (past the self-demotion deadline) whose
/// renewal then won the CAS from exactly the previous version and moved the deadline — the
/// node keeps every range, no replay. Metric `cairnflow_session_revived_total`. A
/// suspension ending in a lost session is counted in `cairnflow_failstop_total`
/// (`session_expired` = past the cap, `session_lost` = the CAS missed).
pub mod pause_stats {
    use std::sync::atomic::AtomicU64;

    pub static REVIVED: AtomicU64 = AtomicU64::new(0);
}

fn io(e: OsError) -> StoreError {
    StoreError::LeaseIo(e.to_string())
}

#[cfg(test)]
mod tests;
