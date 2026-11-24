// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Node health: a **slow score** from signals that already exist, and the `shedding` marker
//! so that only one node at a time gives up leadership on its own.
//!
//! The score update rule and its defaults are adapted from TiKV's
//! `components/health_controller/src/slow_score.rs` (Apache-2.0, see NOTICE): a 1–100 score
//! in AIMD style — a round with timeouts multiplies, a clean round subtracts linearly so that
//! going from 100 down to 1 takes exactly one recovery window. It differs from TiKV in the
//! signal source: cairn sends no probe requests. Both sources are PUTs the node has to make
//! anyway:
//!
//! - **session renewal** (`renew_once`): a conditional PUT every TTL/5, even when the node is
//!   idle — so every round has samples;
//! - **shared WAL PUT attempts** (`Stripe::put`): present when the node has work.
//!
//! A "timeout" is a renewal slower than [`SlowMeter::slow_put`] or a failed WAL PUT attempt.
//! The commit wait time (`latency::COMMIT`) does **not** go into the score: it is a
//! process-wide histogram (the harness runs several nodes in one process), and it is itself
//! the WAL PUT plus queueing — already counted above.
//!
//! The meter sits on the hot path of the shared WAL, so it only uses atomics: no locks, no
//! allocations.

// The runtime's clock: outside simulation it is `std::time::Instant`, inside deterministic
// simulation it is the virtual clock.
use tokio::time::Instant;
use {
    crate::{
        error::{Result, StoreError},
        owner::SessionId,
        session::Session,
    },
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, Weak,
        },
        time::Duration,
    },
};

/// The object kind of a measured PUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutKind {
    /// Renewal of `sessions/<node>/<inc>`.
    Session,
    /// An object of the shared WAL log.
    Wal,
}

impl PutKind {
    pub const ALL: [PutKind; 2] = [PutKind::Session, PutKind::Wal];

    pub fn as_str(self) -> &'static str {
        match self {
            PutKind::Session => "session",
            PutKind::Wal => "wal",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// PUT latency buckets, ms. Denser than the shared `/metrics` histogram because a round's
/// p99 is read from these very buckets and compared at 2× against the cluster median.
pub const PUT_BUCKETS_MS: [u64; 24] = [
    1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1_000, 1_500, 2_000,
    3_000, 5_000, 7_000, 10_000,
];
const NB: usize = PUT_BUCKETS_MS.len() + 1;

#[allow(clippy::declare_interior_mutable_const)]
const Z: AtomicU64 = AtomicU64::new(0);

fn bucket_of(d: Duration) -> usize {
    let ms = d.as_millis() as u64;
    PUT_BUCKETS_MS
        .iter()
        .position(|b| ms <= *b)
        .unwrap_or(PUT_BUCKETS_MS.len())
}

#[derive(Debug)]
struct Hist {
    buckets: [AtomicU64; NB],
    sum_us: AtomicU64,
    count: AtomicU64,
}

impl Hist {
    const fn new() -> Self {
        Hist {
            buckets: [Z; NB],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

/// One round of the meter, taken out with [`SlowMeter::take_round`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Round {
    /// Number of PUTs measured (failed ones included).
    pub samples: u64,
    /// Number of "timeouts": renewals slower than [`SlowMeter::slow_put`], failed WAL PUT
    /// attempts.
    pub timeouts: u64,
    /// The round's PUT latency p99, ms (the bucket's upper bound). `None` = no samples.
    pub p99_ms: Option<u64>,
}

#[derive(Debug)]
struct MeterInner {
    node: String,
    slow_put: Duration,
    /// Cumulative per kind, for `/metrics`.
    total: [Hist; 2],
    round: Hist,
    timeouts: AtomicU64,
    /// Published score ×1000; 0 = no round yet.
    score_milli: AtomicU64,
    /// Published p99 + 1; 0 = none yet.
    p99_plus1: AtomicU64,
    draining: AtomicBool,
    /// The shared WAL has a slot that rotated but has not yet acquired a new stream: the
    /// node drains like a slow node — refuses to take ranges, publishes `draining=1` — until
    /// every slot has a stream again.
    wal_draining: AtomicBool,
    /// The node's monotonic clock drifts in rate relative to the store beyond the threshold
    /// (`cairn-server/src/clock_rate.rs`): the node drains like a slow node — refuses to
    /// take ranges, publishes `draining=1` — until the clock is back within the threshold.
    clock_draining: AtomicBool,
}

/// The PUT latency meter of **one** node. `Clone` is cheap, every copy shares one set of
/// counters.
#[derive(Clone, Debug)]
pub struct SlowMeter(Arc<MeterInner>);

/// Every live meter in the process — so `/metrics` can filter by node without a path from
/// ingress to the session (the harness runs several nodes in one process; same approach as
/// `nodewal::floor_reports`).
static METERS: Mutex<Vec<Weak<MeterInner>>> = Mutex::new(Vec::new());

impl SlowMeter {
    /// The meter of node `node`; a renewal slower than `slow_put` is a timeout (the node's
    /// default: 1 s).
    pub fn new(node: impl Into<String>, slow_put: Duration) -> Self {
        let inner = Arc::new(MeterInner {
            node: node.into(),
            slow_put,
            total: [Hist::new(), Hist::new()],
            round: Hist::new(),
            timeouts: AtomicU64::new(0),
            score_milli: AtomicU64::new(0),
            p99_plus1: AtomicU64::new(0),
            draining: AtomicBool::new(false),
            wal_draining: AtomicBool::new(false),
            clock_draining: AtomicBool::new(false),
        });
        if let Ok(mut all) = METERS.lock() {
            all.retain(|w| w.strong_count() > 0);
            all.push(Arc::downgrade(&inner));
        }
        SlowMeter(inner)
    }

    pub fn node(&self) -> &str {
        &self.0.node
    }

    /// The threshold at which one renewal counts as a timeout.
    pub fn slow_put(&self) -> Duration {
        self.0.slow_put
    }

    fn observe(&self, kind: PutKind, d: Duration) {
        let b = bucket_of(d);
        let us = d.as_micros() as u64;
        for h in [&self.0.total[kind.index()], &self.0.round] {
            h.buckets[b].fetch_add(1, Ordering::Relaxed);
            h.sum_us.fetch_add(us, Ordering::Relaxed);
            h.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One session renewal took `d` (for a timed-out one: until the wait was abandoned).
    pub fn renewed(&self, d: Duration) {
        self.observe(PutKind::Session, d);
        if d > self.0.slow_put {
            self.0.timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One shared WAL PUT attempt took `d`; `ok = false` is a timeout.
    pub fn wal_attempt(&self, d: Duration, ok: bool) {
        self.observe(PutKind::Wal, d);
        if !ok {
            self.0.timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Takes out the current round and starts a new one.
    pub fn take_round(&self) -> Round {
        let r = &self.0.round;
        let counts: Vec<u64> = r
            .buckets
            .iter()
            .map(|b| b.swap(0, Ordering::Relaxed))
            .collect();
        r.sum_us.swap(0, Ordering::Relaxed);
        r.count.swap(0, Ordering::Relaxed);
        let samples: u64 = counts.iter().sum();
        let timeouts = self
            .0
            .timeouts
            .swap(0, Ordering::Relaxed)
            .min(samples.max(1));
        Round {
            samples,
            timeouts,
            p99_ms: quantile_ms(&counts, 0.99),
        }
    }

    /// Publishes the score and p99 of the round just finished — the session record reads
    /// them at the next renewal, `/metrics` reads them at scrape time. A round with no
    /// samples at all (`None`: an idle delegated node) publishes **no** p99, rather than
    /// keeping the old round's number: the median in `slow.rs` leaves that node out.
    pub fn publish(&self, score: f64, p99_ms: Option<u64>) {
        let milli = (score.clamp(1.0, 100.0) * 1000.0).round() as u64;
        self.0.score_milli.store(milli, Ordering::Relaxed);
        self.0
            .p99_plus1
            .store(p99_ms.map_or(0, |p| p.saturating_add(1)), Ordering::Relaxed);
    }

    pub fn set_draining(&self, on: bool) {
        self.0.draining.store(on, Ordering::Relaxed);
    }

    /// Whether the node's shared WAL has (`true`) or no longer has (`false`) a slot that
    /// rotated without a new stream yet. Separate from [`Self::set_draining`] of the slow
    /// score loop: the two sources turn on/off independently, and the session record
    /// publishes draining when either is on.
    pub fn set_wal_draining(&self, on: bool) {
        self.0.wal_draining.store(on, Ordering::Release);
    }

    /// Xem [`Self::set_wal_draining`].
    pub fn wal_draining(&self) -> bool {
        self.0.wal_draining.load(Ordering::Acquire)
    }

    /// Whether the node's clock drifts in rate (`true`) or is back within the threshold
    /// (`false`). A third source, independent of the two above.
    pub fn set_clock_draining(&self, on: bool) {
        self.0.clock_draining.store(on, Ordering::Release);
    }

    /// Xem [`Self::set_clock_draining`].
    pub fn clock_draining(&self) -> bool {
        self.0.clock_draining.load(Ordering::Acquire)
    }

    /// What the session record publishes: `slow=`, `p99_put_ms=`, `draining=`.
    pub fn published(&self) -> Published {
        let milli = self.0.score_milli.load(Ordering::Relaxed);
        let p99 = self.0.p99_plus1.load(Ordering::Relaxed);
        Published {
            slow: (milli > 0).then(|| (milli as f64 / 1000.0).round() as u32),
            p99_put_ms: (p99 > 0).then(|| p99 - 1),
            draining: self.0.draining.load(Ordering::Relaxed)
                || self.wal_draining()
                || self.clock_draining(),
        }
    }

    /// The published score, exact (not rounded). 1 when there has been no round yet.
    pub fn score(&self) -> f64 {
        let milli = self.0.score_milli.load(Ordering::Relaxed);
        if milli == 0 {
            1.0
        } else {
            milli as f64 / 1000.0
        }
    }

    /// The cumulative histogram of kind `kind`, for `/metrics`.
    pub fn histogram(&self, kind: PutKind) -> HistSnapshot {
        let h = &self.0.total[kind.index()];
        let mut acc = 0;
        let buckets = PUT_BUCKETS_MS
            .iter()
            .zip(h.buckets.iter())
            .map(|(le, b)| {
                acc += b.load(Ordering::Relaxed);
                (*le, acc)
            })
            .collect();
        HistSnapshot {
            buckets,
            sum_us: h.sum_us.load(Ordering::Relaxed),
            count: h.count.load(Ordering::Relaxed),
        }
    }
}

/// The meter of node `node` in this process, if any (for `/metrics`).
pub fn meter_of(node: &str) -> Option<SlowMeter> {
    let mut all = METERS.lock().ok()?;
    all.retain(|w| w.strong_count() > 0);
    all.iter()
        .filter_map(Weak::upgrade)
        .find(|m| m.node == node)
        .map(SlowMeter)
}

/// A cumulative histogram: `(le_ms, cumulative count)`, total µs, sample count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistSnapshot {
    pub buckets: Vec<(u64, u64)>,
    pub sum_us: u64,
    pub count: u64,
}

/// What a node publishes in its session record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Published {
    pub slow: Option<u32>,
    pub p99_put_ms: Option<u64>,
    pub draining: bool,
}

/// Quantile `q` of the buckets, ms (the upper bound of the bucket containing it). The last
/// bucket (over 10 s) reports twice the last bound.
fn quantile_ms(counts: &[u64], q: f64) -> Option<u64> {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    let rank = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut acc = 0;
    for (i, c) in counts.iter().enumerate() {
        acc += c;
        if acc >= rank {
            return Some(
                PUT_BUCKETS_MS
                    .get(i)
                    .copied()
                    .unwrap_or(PUT_BUCKETS_MS[PUT_BUCKETS_MS.len() - 1] * 2),
            );
        }
    }
    None
}

// ─────────────────────────────────────────────────────────── AIMD score

/// The timeout ratio from which a round doubles the score (TiKV: 0.1 for disks).
const RATIO_THRESHOLD: f64 = 0.1;

/// Slow score 1–100. Pure, no I/O: the caller feeds in each round.
#[derive(Clone, Debug)]
pub struct SlowScore {
    value: f64,
    /// Each clean round subtracts `100 · round / recovery window` — by default 100/30.
    step_down: f64,
}

impl SlowScore {
    /// `round` is the length of one round, `recover` is the time for the score to go from
    /// 100 down to 1 when every round is clean (TiKV `min_ttr`).
    pub fn new(round: Duration, recover: Duration) -> Self {
        let step_down = 100.0 * round.as_secs_f64() / recover.as_secs_f64().max(1e-3);
        SlowScore {
            value: 1.0,
            step_down,
        }
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    /// Feeds in one round. A round with no samples carries no information: unchanged.
    /// A round with timeouts: multiply by `1 + min(ratio, 0.1) / 0.1` (at most ×2), capped at
    /// 100. A clean round: subtract one step, floor 1.
    pub fn update(&mut self, r: &Round) -> f64 {
        if r.timeouts > 0 {
            let ratio = r.timeouts as f64 / r.samples.max(r.timeouts) as f64;
            let near = ratio.min(RATIO_THRESHOLD) / RATIO_THRESHOLD;
            self.value = (self.value * (1.0 + near)).min(100.0);
        } else if r.samples > 0 {
            self.value = (self.value - self.step_down).max(1.0);
        }
        self.value
    }
}

// ─────────────────────────────────────────────────────── marker `shedding`

pub(crate) const SHED_HEADER: &str = "cairnflow-shedding/1";

fn shed_path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from("shedding")
    } else {
        Path::from(format!("{}/shedding", prefix.as_ref()))
    }
}

/// The content of the `shedding` object (lenient read: unknown lines ignored).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShedRecord {
    /// The session of the node that is shedding.
    pub session: SessionId,
    /// Increased each time the holder refreshes — others measure "standing still" by it.
    pub beat: u64,
    /// Why it is shedding, for the operator.
    pub reason: String,
    /// The holder has recovered and handed the marker back.
    pub released: bool,
}

impl ShedRecord {
    pub fn encode(&self) -> Vec<u8> {
        let reason: String = self
            .reason
            .chars()
            .map(|c| if c == '\n' { ' ' } else { c })
            .collect();
        let mut out = format!(
            "{SHED_HEADER}\nsession={}\nbeat={}\nreason={reason}\n",
            self.session, self.beat
        );
        if self.released {
            out.push_str("released=1\n");
        }
        out.into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<ShedRecord> {
        let bad = |reason: String| StoreError::LeaseCorrupt { reason };
        let text =
            std::str::from_utf8(raw).map_err(|e| bad(format!("shedding is not UTF-8: {e}")))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != SHED_HEADER {
            if let Some(e) = crate::format::newer_header("shedding", SHED_HEADER, header) {
                return Err(e);
            }
            return Err(bad(format!("missing header {SHED_HEADER}")));
        }
        let (mut session, mut beat, mut reason, mut released) = (None, 0, String::new(), false);
        for line in lines {
            match line.split_once('=') {
                Some(("session", v)) => session = SessionId::parse(v),
                Some(("beat", v)) => beat = v.parse().unwrap_or(0),
                Some(("reason", v)) => reason = v.to_string(),
                Some(("released", v)) => released = v == "1",
                _ => {}
            }
        }
        Ok(ShedRecord {
            session: session.ok_or_else(|| bad("shedding is missing session".into()))?,
            beat,
            reason,
            released,
        })
    }
}

/// Reads the `shedding` marker, if present.
pub async fn read_shed(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Option<(Result<ShedRecord>, UpdateVersion)>> {
    let got = match store.get(&shed_path(prefix)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::LeaseIo(e.to_string())),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got
        .bytes()
        .await
        .map_err(|e| StoreError::LeaseIo(e.to_string()))?;
    Ok(Some((ShedRecord::decode(&raw), version)))
}

/// The "this node is shedding" marker — one node at a time. Taken by CAS; the holder
/// refreshes it every round (increasing `beat`); others consider it abandoned when the
/// holder's session is dead, or when `beat` has stood still for one `ttl` **by the
/// observer's own clock** (never comparing the clocks of two machines).
pub struct ShedMarker {
    store: Arc<dyn ObjectStore>,
    path: Path,
    ttl: Duration,
    /// The copy we hold: version for the CAS, the beat written, the session when taken.
    held: Option<(UpdateVersion, ShedRecord)>,
    /// Observation of someone else's marker: (content key, when first seen).
    seen: Option<(String, Instant)>,
}

impl ShedMarker {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path, ttl: Duration) -> Self {
        ShedMarker {
            store,
            path: shed_path(prefix),
            ttl,
            held: None,
            seen: None,
        }
    }

    pub fn is_held(&self) -> bool {
        self.held.is_some()
    }

    /// CASes the marker. Ambiguity is resolved by reading back (rule 7): the record carries
    /// our session + `beat`, so identical content is our own write — before this, an
    /// object_store retry turned a marker we had just taken into "someone else holds it".
    async fn write(&self, rec: &ShedRecord, mode: PutMode) -> Result<Option<UpdateVersion>> {
        match crate::cas::put_resolving(&self.store, &self.path, rec.encode().into(), mode).await {
            Ok(crate::cas::Cas::Written(v)) => Ok(Some(v)),
            Ok(crate::cas::Cas::Lost) => Ok(None),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Tries to take the marker for the current session of `session`. `Ok(true)` = ours.
    /// `Ok(false)` = another node is shedding (or someone just took it first).
    pub async fn try_acquire(&mut self, session: &Session, reason: &str) -> Result<bool> {
        if self.held.is_some() {
            return Ok(true);
        }
        let Some(me) = session.current() else {
            return Ok(false);
        };
        let mut rec = ShedRecord {
            session: me.clone(),
            beat: 1,
            reason: reason.to_string(),
            released: false,
        };
        let mode = match read_shed(&self.store, &self.path_prefix()).await? {
            None => PutMode::Create,
            Some((found, version)) => {
                let free = match &found {
                    Ok(r) if r.released || r.session == me => true,
                    Ok(r) => {
                        session.is_session_dead(&r.session).await?
                            || self.stale(format!("{}#{}", r.session, r.beat))
                    }
                    // Cannot be read: only considered abandoned after standing still for
                    // one `ttl`.
                    Err(_) => self.stale(format!("broken:{:?}", version.e_tag)),
                };
                if !free {
                    return Ok(false);
                }
                if let Ok(r) = &found {
                    rec.beat = r.beat + 1;
                }
                PutMode::Update(version)
            }
        };
        match self.write(&rec, mode).await? {
            Some(version) => {
                self.seen = None;
                self.held = Some((version, rec));
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Whether content `key` has been seen standing still for at least `ttl` (since the
    /// previous call).
    fn stale(&mut self, key: String) -> bool {
        match &self.seen {
            Some((k, at)) if *k == key => at.elapsed() >= self.ttl,
            _ => {
                self.seen = Some((key, Instant::now()));
                false
            }
        }
    }

    fn path_prefix(&self) -> Path {
        // `read_shed` takes the prefix; the marker is always at `<prefix>/shedding`.
        let s = self.path.as_ref();
        match s.rsplit_once('/') {
            Some((p, _)) => Path::from(p),
            None => Path::default(),
        }
    }

    /// Refreshes the held marker (increasing `beat`). `Ok(false)` = lost: someone considered
    /// us abandoned and took it.
    pub async fn renew(&mut self) -> Result<bool> {
        let Some((version, mut rec)) = self.held.take() else {
            return Ok(false);
        };
        rec.beat += 1;
        match self.write(&rec, PutMode::Update(version.clone())).await {
            Ok(Some(v)) => {
                self.held = Some((v, rec));
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => {
                rec.beat -= 1;
                self.held = Some((version, rec));
                Err(e)
            }
        }
    }

    /// Hands the marker back (writes `released=1` by CAS). If that fails, so be it: others
    /// consider it abandoned after one `ttl`.
    pub async fn release(&mut self) {
        let Some((version, mut rec)) = self.held.take() else {
            return;
        };
        rec.beat += 1;
        rec.released = true;
        if let Err(e) = self.write(&rec, PutMode::Update(version)).await {
            tracing::debug!(error = %e, "could not hand back the shedding marker — others consider it abandoned after the TTL");
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::LeaseConfig, slatedb::object_store::memory::InMemory};

    fn round(samples: u64, timeouts: u64) -> Round {
        Round {
            samples,
            timeouts,
            p99_ms: None,
        }
    }

    /// AIMD like TiKV: a timeout ratio ≥ 0.1 doubles; 1 → 100 takes seven rounds; a clean
    /// round subtracts 100 · round / recovery window (100/30 with 10 s and 5 minutes).
    #[test]
    fn aimd_doubles_on_timeouts_and_decays_linearly() {
        let mut s = SlowScore::new(Duration::from_secs(10), Duration::from_secs(300));
        let mut rounds = 0;
        while s.value() < 100.0 {
            s.update(&round(5, 1));
            rounds += 1;
        }
        assert_eq!(rounds, 7);
        // A ratio below the threshold multiplies by less.
        let mut t = SlowScore::new(Duration::from_secs(10), Duration::from_secs(300));
        t.update(&round(20, 1));
        assert!((t.value() - 1.5).abs() < 1e-9, "{}", t.value());
        // Clean rounds: 100 → < 10 in 28 rounds (100/30 per round).
        let mut clean = 0;
        while s.value() >= 10.0 {
            s.update(&round(5, 0));
            clean += 1;
        }
        assert_eq!(clean, 28);
        // A round with no samples: unchanged.
        let v = s.value();
        s.update(&round(0, 0));
        assert_eq!(s.value(), v);
        for _ in 0..100 {
            s.update(&round(5, 0));
        }
        assert_eq!(s.value(), 1.0);
    }

    #[test]
    fn meter_counts_slow_renewals_and_failed_wal_attempts() {
        let m = SlowMeter::new("n-meter", Duration::from_millis(100));
        m.renewed(Duration::from_millis(5));
        m.renewed(Duration::from_millis(150));
        m.wal_attempt(Duration::from_millis(3), true);
        m.wal_attempt(Duration::from_millis(400), false);
        let r = m.take_round();
        assert_eq!((r.samples, r.timeouts), (4, 2));
        assert_eq!(r.p99_ms, Some(500));
        assert_eq!(
            m.take_round(),
            Round::default(),
            "a new round starts from 0"
        );
        assert_eq!(m.histogram(PutKind::Session).count, 2);
        assert_eq!(m.histogram(PutKind::Wal).count, 2);
        assert_eq!(m.published(), Published::default());
        m.publish(37.4, Some(500));
        m.set_draining(true);
        assert_eq!(
            m.published(),
            Published {
                slow: Some(37),
                p99_put_ms: Some(500),
                draining: true
            }
        );
        assert!(meter_of("n-meter").is_some());
    }

    /// A round with no PUT samples at all (an idle delegated node: no renewal by PUT, no WAL
    /// writes) publishes **no** p99 — it does not keep the old round's p99 — so that the
    /// median in `slow.rs` leaves that node out instead of computing with a stale number.
    #[test]
    fn round_without_samples_publishes_no_p99() {
        let m = SlowMeter::new("n-idle", Duration::from_millis(100));
        m.publish(5.0, Some(40));
        assert_eq!(m.published().p99_put_ms, Some(40));
        m.publish(5.0, None);
        assert_eq!(
            m.published().p99_put_ms,
            None,
            "a round with no samples kept the old p99"
        );
        assert_eq!(m.published().slow, Some(5));
    }

    #[test]
    fn shed_record_roundtrips_and_ignores_unknown_lines() {
        let r = ShedRecord {
            session: SessionId {
                node: "a".into(),
                inc: 7,
            },
            beat: 3,
            reason: "slow=100\nx".into(),
            released: true,
        };
        let back = ShedRecord::decode(&r.encode()).unwrap();
        assert_eq!(back.reason, "slow=100 x");
        assert_eq!(back.session, r.session);
        assert!(back.released);
        let mut raw = r.encode();
        raw.extend_from_slice(b"future=1\n");
        assert!(ShedRecord::decode(&raw).is_ok());
        assert!(ShedRecord::decode(b"cairnflow-shedding/2\nsession=a/1\n").is_err());
    }

    async fn session(store: &Arc<dyn ObjectStore>, id: &str) -> Session {
        Session::establish(
            Arc::clone(store),
            &Path::default(),
            LeaseConfig::new()
                .with_node_id(id)
                .with_ttl(Duration::from_millis(300))
                .with_renew_interval(Duration::from_millis(50)),
        )
        .await
        .unwrap()
    }

    /// Rule 7: the marker CAS was written but the reply says otherwise. Must see: taking and
    /// refreshing **succeed** — not "someone else holds it" for our own marker, nor "lost"
    /// when nobody took it.
    #[tokio::test(flavor = "multi_thread")]
    async fn ambiguous_marker_writes_are_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let sa = session(&store, "a").await;
            let mut a = ShedMarker::new(
                Arc::clone(&store),
                &Path::default(),
                Duration::from_secs(60),
            );
            faulty.add_rule(Rule::ambiguous_put("shedding", reply));
            assert!(
                a.try_acquire(&sa, "slow").await.unwrap_or(false),
                "{reply:?}: the marker was written but reported not taken"
            );
            faulty.add_rule(Rule::ambiguous_put("shedding", reply));
            assert!(
                a.renew().await.unwrap_or(false),
                "{reply:?}: the refresh was written but reported lost"
            );
            assert!(
                a.renew().await.unwrap(),
                "{reply:?}: the version after resolving is unusable"
            );
            sa.close().await;
        }
    }

    /// One node at a time: b cannot take it while a holds and refreshes it; when a hands it
    /// back, b takes it; when a dies (session closed), b takes it immediately; when the
    /// marker stands still for one TTL, b takes it even though a's session is still alive.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_shedder_at_a_time() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (sa, sb) = (session(&store, "a").await, session(&store, "b").await);
        let ttl = Duration::from_millis(400);
        let mut a = ShedMarker::new(Arc::clone(&store), &Path::default(), ttl);
        let mut b = ShedMarker::new(Arc::clone(&store), &Path::default(), ttl);
        assert!(a.try_acquire(&sa, "slow").await.unwrap());
        assert!(!b.try_acquire(&sb, "slow").await.unwrap());
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(a.renew().await.unwrap());
            assert!(
                !b.try_acquire(&sb, "slow").await.unwrap(),
                "a is still refreshing"
            );
        }
        a.release().await;
        assert!(b.try_acquire(&sb, "slow").await.unwrap());
        let (rec, _) = read_shed(&store, &Path::default()).await.unwrap().unwrap();
        assert_eq!(rec.unwrap().session.node, "b");

        // b stands still (does not refresh) ⇒ a takes it after one TTL.
        let mut a = ShedMarker::new(Arc::clone(&store), &Path::default(), ttl);
        assert!(!a.try_acquire(&sa, "slow").await.unwrap());
        tokio::time::sleep(ttl + Duration::from_millis(50)).await;
        assert!(a.try_acquire(&sa, "slow").await.unwrap());
        assert!(!b.renew().await.unwrap(), "b knows it has lost the marker");

        // a closes its session ⇒ b takes it immediately.
        sa.close().await;
        let mut b = ShedMarker::new(
            Arc::clone(&store),
            &Path::default(),
            Duration::from_secs(3600),
        );
        assert!(b.try_acquire(&sb, "slow").await.unwrap());
        sb.close().await;
    }
}
