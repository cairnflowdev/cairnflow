// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-range L0: the signal, metrics and a soft throttle before the wall.
//!
//! The L0 wall: SlateDB's flusher refuses to upload memtables once the number
//! of L0 views reaches `l0_max_ssts` (8). Immutable memtables pile up to
//! `max_unflushed_bytes`, then `db.write` stalls — while the turn holds the
//! engine. Stalled long enough, the range steps down by itself
//! (`durability_stall`). This module reads that signal **cheaply**: the L0 view
//! count from the writer's in-RAM manifest watch
//! ([`WriterHealth::l0_views`]), no GET at all.
//!
//! # The throttle (inspired by TiKV's flow controller)
//!
//! From `soft` (5 against a cap of 8) it **refuses new work with a
//! probability** instead of letting the range stall completely at the cap: the
//! probability rises linearly with the view count, up to 0.9 at `cap − 1` —
//! never 100 %, so small work still gets through. Lowering has hysteresis: the
//! probability only drops once the view count has been lower **throughout**
//! `cooldown` (5 s), so the throttle does not oscillate with every
//! flush/compaction.
//!
//! **Only new work is throttled**, in three places: `Invoke`/`Submit` commands
//! at ingress (returning [`crate::actor::ActorError::Overloaded`], creating
//! nothing), `StartCall` arriving from another range (accepted durably, then
//! the `inbound` row is left in place), and local child calls (`dispatch_call`
//! leaves the outbox row in place). Completions, timers, retries, signals and
//! replies are **never** throttled: draining the backlog is what brings L0
//! down, blocking it would jam ourselves.
//!
//! The throttle state lives on the range's **engine thread** ([`Throttle`],
//! `Cell`, no locks); metrics go through an atomic [`L0Gauge`] in the node's
//! load registry.

// The runtime's clock: outside simulation it is `std::time::Instant`, in
// deterministic simulation the virtual clock.
use tokio::time::Instant;
use {
    cairn_store::{dbstats::DbStatsSnapshot, WriteStallTotals, WriterHealth},
    std::{
        cell::Cell,
        fmt::Write,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::sync::Notify,
};

/// Knobs of the L0 throttle.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct L0ThrottleConfig {
    /// L0 view count at which throttling starts. `0` = throttle off.
    pub soft: usize,
    /// Maximum refusal probability (at `cap − 1` and above). Never 1: small
    /// work must still get through.
    pub max_reject: f64,
    /// Lower the throttle level only once the view count has been lower throughout this long.
    pub cooldown: Duration,
    /// `Retry-After` of `429 range_overloaded`.
    pub retry_after: Duration,
    /// Retry interval for deferred new work (`StartCall`, local child calls) —
    /// and the L0 sampling interval while throttling. Runs only while
    /// throttling: an idle range costs nothing.
    pub defer_retry: Duration,
}

pub const DEFAULT_L0_SOFT: usize = 5;
pub const DEFAULT_L0_MAX_REJECT: f64 = 0.9;
pub const DEFAULT_L0_COOLDOWN: Duration = Duration::from_secs(5);

impl Default for L0ThrottleConfig {
    fn default() -> Self {
        Self {
            soft: DEFAULT_L0_SOFT,
            max_reject: DEFAULT_L0_MAX_REJECT,
            cooldown: DEFAULT_L0_COOLDOWN,
            retry_after: Duration::from_secs(1),
            defer_retry: Duration::from_millis(250),
        }
    }
}

impl L0ThrottleConfig {
    /// Throttle off.
    pub fn off() -> Self {
        Self {
            soft: 0,
            ..Self::default()
        }
    }

    pub fn enabled(&self) -> bool {
        self.soft > 0 && self.max_reject > 0.0
    }
}

/// The L0 cap the range's Db runs with (SlateDB's `l0_max_ssts`).
pub fn l0_cap() -> usize {
    slatedb::Settings::default().l0_max_ssts
}

/// Probability of refusing new work at `l0` views (before the lowering
/// hysteresis).
///
/// `l0 < soft` ⇒ 0; from `soft` to `cap − 1` it rises linearly, reaching `max`
/// at `cap − 1` and staying there. With the defaults (5, 8, 0.9): 5 → 0.3;
/// 6 → 0.6; 7, 8 → 0.9 — i.e. linear from 0 at `soft − 1`. `cap ≤ soft` ⇒ a
/// step: `max` from `soft`.
pub fn reject_probability(l0: usize, soft: usize, cap: usize, max: f64) -> f64 {
    if soft == 0 || l0 < soft {
        return 0.0;
    }
    let band = cap.saturating_sub(soft).max(1);
    let step = (l0 - soft + 1).min(band);
    max.clamp(0.0, 1.0) * step as f64 / band as f64
}

/// Lowering hysteresis: the throttle level rises to the target at once, but
/// only falls to the **highest** level the target reached during one whole
/// `cooldown` window below the current level. It returns to 0 only when no
/// sample in a whole `cooldown` was above 0 (`l0 ≤ soft − 1`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Hold {
    level: f64,
    lower_since: Option<Instant>,
    lower_peak: f64,
}

impl Hold {
    pub fn level(&self) -> f64 {
        self.level
    }

    pub fn update(&mut self, target: f64, now: Instant, cooldown: Duration) -> f64 {
        if target >= self.level {
            self.level = target;
            self.lower_since = None;
            return self.level;
        }
        match self.lower_since {
            None => {
                self.lower_since = Some(now);
                self.lower_peak = target;
            }
            Some(since) => {
                self.lower_peak = self.lower_peak.max(target);
                if now.saturating_duration_since(since) >= cooldown {
                    self.level = self.lower_peak;
                    // New window, measured from the level just lowered to.
                    self.lower_since = (target < self.level).then_some(now);
                    self.lower_peak = target;
                }
            }
        }
        self.level
    }
}

/// Kind of new work throttled — the metric's `kind` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shed {
    /// `Invoke`/`Submit` at ingress — `429 range_overloaded`.
    Ingress = 0,
    /// `StartCall` arriving from another range — the `inbound` row is left in place.
    Inbound = 1,
    /// A child call targeting this range — the outbox row is left in place.
    Dispatch = 2,
}

impl Shed {
    const ALL: [Shed; 3] = [Shed::Ingress, Shed::Inbound, Shed::Dispatch];

    fn label(self) -> &'static str {
        match self {
            Shed::Ingress => "ingress",
            Shed::Inbound => "inbound",
            Shed::Dispatch => "dispatch",
        }
    }
}

/// A range's throttle measurements, readable from outside the engine thread (`/metrics`).
#[derive(Default, Debug)]
pub struct L0Gauge {
    /// Current throttle level, in thousandths.
    reject_permille: AtomicU64,
    shed: [AtomicU64; 3],
}

impl L0Gauge {
    pub fn reject_ratio(&self) -> f64 {
        self.reject_permille.load(Ordering::Relaxed) as f64 / 1000.0
    }

    pub fn shed(&self, kind: Shed) -> u64 {
        self.shed[kind as usize].load(Ordering::Relaxed)
    }
}

/// The throttle state of **one term** of leading a range. Lives on the engine
/// thread (`Rc`), every field is a `Cell`: no locks, no I/O — callable on the
/// turn's hot path.
pub struct Throttle {
    cfg: L0ThrottleConfig,
    cap: usize,
    /// Current L0 view count — from [`WriterHealth::l0_views`] on a real node.
    l0: Box<dyn Fn() -> usize>,
    gauge: Arc<L0Gauge>,
    hold: Cell<Hold>,
    rng: Cell<u64>,
    /// A drain pass has deferred one `StartCall` / one child call: everything
    /// after it in the **same pass** is deferred too, so the creation order
    /// among them is preserved (FL-5: two calls to the same object run in call
    /// order).
    sticky_inbound: Cell<bool>,
    sticky_dispatch: Cell<bool>,
    /// Some work is deferred, waiting for a later drain pass.
    deferred: Cell<bool>,
    /// Tells the serving loop to start the throttle tick ([`Self::tick`]): work
    /// was just deferred, or the throttle level just left 0.
    wake: Notify,
}

impl Throttle {
    pub fn new(
        cfg: L0ThrottleConfig,
        cap: usize,
        l0: impl Fn() -> usize + 'static,
        gauge: Arc<L0Gauge>,
    ) -> Self {
        // Xorshift seeded **once** from `entropy`: good enough for coin flips,
        // and flipping coins on the hot path does not touch the operating
        // system's source. The seed used to mix in the gauge's heap address —
        // ASLR made the sim unreproducible; through `entropy` the sim seeds it
        // from its seed.
        let seed = {
            let mut b = [0u8; 8];
            cairn_store::entropy::fill(&mut b);
            u64::from_le_bytes(b)
        };
        gauge.reject_permille.store(0, Ordering::Relaxed);
        Self {
            cfg,
            cap,
            l0: Box::new(l0),
            gauge,
            hold: Cell::new(Hold::default()),
            rng: Cell::new(seed | 1),
            sticky_inbound: Cell::new(false),
            sticky_dispatch: Cell::new(false),
            deferred: Cell::new(false),
            wake: Notify::new(),
        }
    }

    /// Throttle off — for paths without a real writer.
    pub fn disabled() -> Self {
        Self::new(L0ThrottleConfig::off(), 8, || 0, Arc::default())
    }

    pub fn config(&self) -> &L0ThrottleConfig {
        &self.cfg
    }

    /// The throttle level (refusal probability) right now. Cheap when not throttling: one watch read.
    pub fn level(&self) -> f64 {
        if !self.cfg.enabled() {
            return 0.0;
        }
        let target = reject_probability((self.l0)(), self.cfg.soft, self.cap, self.cfg.max_reject);
        let mut hold = self.hold.get();
        let before = hold.level();
        if target == 0.0 && before == 0.0 {
            return 0.0;
        }
        let level = hold.update(target, Instant::now(), self.cfg.cooldown);
        self.hold.set(hold);
        if level != before {
            self.gauge
                .reject_permille
                .store((level * 1000.0).round() as u64, Ordering::Relaxed);
            if before == 0.0 {
                self.wake.notify_one();
            }
        }
        level
    }

    /// Flips a coin with probability `p`.
    fn roll(&self, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        let mut x = self.rng.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.set(x);
        ((x >> 11) as f64 / (1u64 << 53) as f64) < p
    }

    fn count(&self, kind: Shed) {
        self.gauge.shed[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Whether a command creating a new invocation at ingress is accepted.
    /// `false` ⇒ `Overloaded` (unless the invocation already exists — the
    /// caller checks).
    pub fn admit_ingress(&self) -> bool {
        if self.roll(self.level()) {
            self.count(Shed::Ingress);
            return false;
        }
        true
    }

    fn defer(&self, sticky: &Cell<bool>, kind: Shed) -> bool {
        if !sticky.get() {
            if !self.roll(self.level()) {
                return false;
            }
            sticky.set(true);
        }
        self.count(kind);
        if !self.deferred.replace(true) {
            self.wake.notify_one();
        }
        true
    }

    /// Defer this `StartCall` to a later drain pass? (The `inbound` row is
    /// already durable; the sender has been acked.)
    pub fn defer_inbound(&self) -> bool {
        // Test: throttle as if L0 were right at the wall — defer the whole
        // rest of the pass, no coin flip.
        if cairn_store::failpoint::fires("l0::defer_inbound") {
            self.sticky_inbound.set(true);
        }
        self.defer(&self.sticky_inbound, Shed::Inbound)
    }

    /// This drain pass has deferred a `StartCall` — every remaining `StartCall`
    /// of the pass will be deferred too ([`Self::defer_inbound`]). The drainer
    /// uses it to avoid re-reading rows already throttled. Read-only, no coin
    /// flip.
    pub fn deferring_inbound(&self) -> bool {
        self.sticky_inbound.get()
    }

    /// Defer creating this local child call to a later drain pass?
    pub fn defer_dispatch(&self) -> bool {
        self.defer(&self.sticky_dispatch, Shed::Dispatch)
    }

    /// Start of a drain pass: the previous pass's "defer the whole rest"
    /// decision expires.
    pub fn begin_pass(&self) {
        self.sticky_inbound.set(false);
        self.sticky_dispatch.set(false);
    }

    /// Waits until the serving loop has to start the throttle tick.
    pub async fn woken(&self) {
        self.wake.notified().await
    }

    /// One throttle tick of the serving loop: sample L0 (so the throttle level
    /// can fall even when no command arrives), report whether the drainer must
    /// be kicked (work was deferred), and whether another tick is needed.
    pub fn tick(&self) -> (bool, bool) {
        let level = self.level();
        let nudge = self.deferred.replace(false);
        (nudge, level > 0.0 || nudge)
    }
}

/// One L0 metric line for a range this node leads.
#[derive(Clone, Debug, Default)]
pub struct RangeL0Row {
    pub range: u32,
    pub l0_views: u64,
    pub write_stall: WriteStallTotals,
    /// `None` when the writer has no SlateDB recorder attached.
    pub db: Option<DbStatsSnapshot>,
    /// Current throttle level (probability of refusing new work).
    pub reject_ratio: f64,
    /// New work throttled, by [`Shed`].
    pub shed: [u64; 3],
    /// Number of ancestor Dbs in the in-RAM manifest's `external_dbs`
    /// ([`WriterHealth::external_depth`]); `None` with the store model.
    pub external_depth: Option<u64>,
}

impl RangeL0Row {
    pub fn of(range: u32, health: &WriterHealth, gauge: &L0Gauge) -> Self {
        Self {
            range,
            l0_views: health.l0_views() as u64,
            write_stall: health.write_stall(),
            db: health.db_stats(),
            reject_ratio: gauge.reject_ratio(),
            shed: Shed::ALL.map(|k| gauge.shed(k)),
            external_depth: health.external_depth().map(|d| d as u64),
        }
    }
}

/// Prints the per-range L0 metric lines. Only the form with a `range` label
/// exists (no total with the same name — see `metrics::counter_per_partition`
/// on the `sum()` double-counting trap).
pub fn render(out: &mut String, rows: &[RangeL0Row]) {
    type Col = (
        &'static str,
        &'static str,
        &'static str,
        fn(&RangeL0Row) -> Option<String>,
    );
    let cols: [Col; 12] = [
        (
            "cairnflow_range_l0_views",
            "gauge",
            "L0 views in the writer's in-memory manifest (cap l0_max_ssts, default 8)",
            |r| Some(r.l0_views.to_string()),
        ),
        (
            "cairnflow_range_write_stalls_total",
            "counter",
            "db.write calls that waited ≥ 10 ms (SlateDB backpressure)",
            |r| Some(r.write_stall.count.to_string()),
        ),
        (
            "cairnflow_range_write_stall_seconds_total",
            "counter",
            "Total time db.write waited ≥ 10 ms, seconds",
            |r| Some(format!("{:.6}", r.write_stall.micros as f64 / 1e6)),
        ),
        (
            "cairnflow_range_slatedb_backpressure_total",
            "counter",
            "SlateDB backpressure_count: writes that waited because unflushed data exceeded max_unflushed_bytes",
            |r| r.db.map(|d| d.backpressure.to_string()),
        ),
        (
            "cairnflow_range_slatedb_l0_stalls_total",
            "counter",
            "SlateDB l0_stall_count: times the flusher refused to upload a memtable because L0 hit its cap",
            |r| r.db.map(|d| d.l0_stalls.to_string()),
        ),
        (
            "cairnflow_range_slatedb_l0_ssts",
            "gauge",
            "SlateDB l0_sst_count: L0 SST count",
            |r| r.db.map(|d| d.l0_ssts.to_string()),
        ),
        (
            "cairnflow_range_slatedb_sorted_runs",
            "gauge",
            "SlateDB sorted_run_count: sorted runs below L0 — every read crosses each overlapping run",
            |r| r.db.map(|d| d.sorted_runs.to_string()),
        ),
        (
            "cairnflow_range_slatedb_memtable_bytes",
            "gauge",
            "SlateDB total_mem_size_bytes: memtable bytes (mutable and immutable) not yet flushed to L0",
            |r| r.db.map(|d| d.mem_bytes.to_string()),
        ),
        (
            "cairnflow_range_block_cache_hits_total",
            "counter",
            "SST data block reads that hit the node's in-RAM block cache",
            |r| r.db.map(|d| d.block_hits.to_string()),
        ),
        (
            "cairnflow_range_block_cache_misses_total",
            "counter",
            "SST data block reads that missed the in-RAM block cache — went to the disk cache or object store",
            |r| r.db.map(|d| d.block_misses.to_string()),
        ),
        (
            "cairnflow_range_l0_reject_ratio",
            "gauge",
            "L0 throttle level: probability of refusing new work on the range (0 = no throttle, max 0.9)",
            |r| Some(format!("{:.3}", r.reject_ratio)),
        ),
        (
            "cairnflow_range_external_depth",
            "gauge",
            "Ancestor Dbs this range's Db still reads SSTs from (distinct external_dbs entries in the writer's in-memory manifest)",
            |r| r.external_depth.map(|d| d.to_string()),
        ),
    ];
    if rows.is_empty() {
        return;
    }
    for (name, kind, help, value) in cols {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for r in rows {
            if let Some(v) = value(r) {
                let _ = writeln!(out, "{name}{{range=\"{}\"}} {v}", r.range);
            }
        }
    }
    // Scan watermarks of the three ordered indexes (writer, `SeqFloor`): per range and index.
    type SeqCol = (
        &'static str,
        &'static str,
        &'static str,
        fn(&cairn_store::dbstats::SeqSlotSnapshot) -> u64,
    );
    let seq_cols: [SeqCol; 5] = [
        (
            "cairnflow_range_seq_floor",
            "gauge",
            "Scan floor of an ordered index (outbox, signal, inbound) in the current writer term: drain passes scan from here",
            |s| s.floor,
        ),
        (
            "cairnflow_range_seq_head",
            "gauge",
            "Highest seq a scan of the ordered index saw in this writer term — head minus floor is the span each drain pass crosses",
            |s| s.head,
        ),
        (
            "cairnflow_range_seq_below",
            "gauge",
            "Rows that may still be live below the scan floor of the ordered index (capped at 4096, then the floor stops moving)",
            |s| s.below,
        ),
        (
            "cairnflow_range_seq_scans_total",
            "counter",
            "Open scans of the ordered index",
            |s| s.scans,
        ),
        (
            "cairnflow_range_seq_frozen_scans_total",
            "counter",
            "Open scans from the floor that could not move it because the below-floor set hit its cap",
            |s| s.frozen,
        ),
    ];
    const SEQ_INDEXES: [&str; 3] = ["outbox", "signal", "inbound"];
    for (name, kind, help, value) in seq_cols {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for r in rows {
            let Some(db) = r.db else { continue };
            for (i, ix) in SEQ_INDEXES.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "{name}{{range=\"{}\",index=\"{ix}\"}} {}",
                    r.range,
                    value(&db.seq[i])
                );
            }
        }
    }
    // Engine hold time per holder (`Lease`, `lease::holding_as`): which
    // background work is eating the range's engine.
    let name = "cairnflow_range_engine_hold_ms_total";
    let _ = writeln!(
        out,
        "# HELP {name} Milliseconds the range's engine was held, by holder (turn, background tick, outbox drain, query index build, hint sweep, recovery, rescue net, invocation list, leader-start flow restore, other)"
    );
    let _ = writeln!(out, "# TYPE {name} counter");
    for r in rows {
        let Some(db) = r.db else { continue };
        for (i, h) in cairn_store::dbstats::HOLDERS.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{range=\"{}\",holder=\"{h}\"}} {:.3}",
                r.range,
                db.hold_us[i] as f64 / 1000.0
            );
        }
    }
    // Hold count per holder: how many times the engine was taken.
    let name = "cairnflow_range_engine_holds_total";
    let _ = writeln!(
        out,
        "# HELP {name} Times the range's engine was acquired and released, by holder"
    );
    let _ = writeln!(out, "# TYPE {name} counter");
    for r in rows {
        let Some(db) = r.db else { continue };
        for (i, h) in cairn_store::dbstats::HOLDERS.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{range=\"{}\",holder=\"{h}\"}} {}",
                r.range, db.holds[i]
            );
        }
    }
    // Longest hold per holder: the limit applies to **each** hold, which the
    // total above cannot show.
    let name = "cairnflow_range_engine_hold_max_ms";
    let _ = writeln!(
        out,
        "# HELP {name} Longest single hold of the range's engine since the Db opened, by holder"
    );
    let _ = writeln!(out, "# TYPE {name} gauge");
    for r in rows {
        let Some(db) = r.db else { continue };
        for (i, h) in cairn_store::dbstats::HOLDERS.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{range=\"{}\",holder=\"{h}\"}} {:.3}",
                r.range,
                db.hold_max_us[i] as f64 / 1000.0
            );
        }
    }
    let name = "cairnflow_range_l0_shed_total";
    let _ = writeln!(
        out,
        "# HELP {name} New work throttled by L0: ingress (429 range_overloaded), inbound (StartCall deferred), dispatch (local child call deferred)"
    );
    let _ = writeln!(out, "# TYPE {name} counter");
    for r in rows {
        for k in Shed::ALL {
            let _ = writeln!(
                out,
                "{name}{{range=\"{}\",kind=\"{}\"}} {}",
                r.range,
                k.label(),
                r.shed[k as usize]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::rc::Rc};

    #[test]
    fn renders_l0_rows() {
        let rows = [
            RangeL0Row {
                range: 3,
                l0_views: 6,
                write_stall: WriteStallTotals {
                    count: 2,
                    micros: 1_500_000,
                },
                db: Some(DbStatsSnapshot {
                    backpressure: 4,
                    l0_stalls: 1,
                    l0_ssts: 6,
                    hold_us: {
                        let mut h = [0; cairn_store::dbstats::HOLDERS.len()];
                        h[cairn_store::dbstats::holder("query_build")] = 2_500;
                        h
                    },
                    hold_max_us: {
                        let mut h = [0; cairn_store::dbstats::HOLDERS.len()];
                        h[cairn_store::dbstats::holder("recover")] = 1_250;
                        h
                    },
                    ..Default::default()
                }),
                reject_ratio: 0.6,
                shed: [5, 1, 0],
                external_depth: Some(2),
            },
            RangeL0Row {
                range: 9,
                ..Default::default()
            },
        ];
        let mut out = String::new();
        render(&mut out, &rows);
        assert!(
            out.contains("cairnflow_range_l0_views{range=\"3\"} 6\n"),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_seq_floor{range=\"3\",index=\"outbox\"} 0\n"),
            "{out}"
        );
        assert!(
            !out.contains("cairnflow_range_seq_floor{range=\"9\""),
            "a range without a recorder prints no watermark: {out}"
        );
        assert!(
            out.contains("cairnflow_range_l0_views{range=\"9\"} 0\n"),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_write_stall_seconds_total{range=\"3\"} 1.500000\n"),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_slatedb_backpressure_total{range=\"3\"} 4\n"),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_l0_reject_ratio{range=\"3\"} 0.600\n"),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_l0_shed_total{range=\"3\",kind=\"ingress\"} 5\n"),
            "{out}"
        );
        assert!(
            out.contains(
                "cairnflow_range_engine_hold_ms_total{range=\"3\",holder=\"query_build\"} 2.500\n"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "cairnflow_range_engine_hold_ms_total{range=\"3\",holder=\"other\"} 0.000\n"
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "cairnflow_range_engine_hold_max_ms{range=\"3\",holder=\"recover\"} 1.250\n"
            ),
            "{out}"
        );
        assert!(
            out.contains("cairnflow_range_external_depth{range=\"3\"} 2\n"),
            "{out}"
        );
        assert!(
            !out.contains("cairnflow_range_external_depth{range=\"9\"}"),
            "a range with unknown lineage (model) prints nothing: {out}"
        );
        // A range without a recorder: no SlateDB lines, the other lines are still there.
        assert!(!out.contains("slatedb_l0_ssts{range=\"9\"}"), "{out}");
        assert_eq!(
            out.matches("# TYPE cairnflow_range_l0_views gauge").count(),
            1
        );
    }

    #[test]
    fn no_rows_no_output() {
        let mut out = String::new();
        render(&mut out, &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn probability_is_linear_from_soft_to_cap_minus_one() {
        let p = |l0| reject_probability(l0, 5, 8, 0.9);
        assert_eq!(p(0), 0.0);
        assert_eq!(p(4), 0.0);
        assert!((p(5) - 0.3).abs() < 1e-9);
        assert!((p(6) - 0.6).abs() < 1e-9);
        assert!((p(7) - 0.9).abs() < 1e-9);
        assert!(
            (p(8) - 0.9).abs() < 1e-9,
            "never above max, even at the cap"
        );
        assert!((p(30) - 0.9).abs() < 1e-9);
        assert_eq!(reject_probability(7, 0, 8, 0.9), 0.0, "soft = 0 is off");
        assert_eq!(
            reject_probability(5, 5, 5, 0.9),
            0.9,
            "cap ≤ soft: stepwise"
        );
        assert_eq!(reject_probability(7, 5, 8, 3.0), 1.0, "max clamped [0, 1]");
    }

    #[test]
    fn hold_rises_at_once_and_falls_only_after_cooldown() {
        let cd = Duration::from_secs(5);
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_millis(s);
        let mut h = Hold::default();
        assert_eq!(h.update(0.9, at(0), cd), 0.9, "rises at once");
        // Down to 0 but not yet 5 s: keep 0.9.
        assert_eq!(h.update(0.0, at(1_000), cd), 0.9);
        assert_eq!(h.update(0.0, at(5_999), cd), 0.9);
        // One sample goes back up midway: the window resets.
        assert_eq!(h.update(0.9, at(6_000), cd), 0.9);
        assert_eq!(h.update(0.3, at(6_500), cd), 0.9);
        assert_eq!(h.update(0.0, at(9_000), cd), 0.9);
        // A full 5 s below: lower to the window's highest level (0.3), not yet 0.
        assert!((h.update(0.0, at(11_500), cd) - 0.3).abs() < 1e-9);
        // Another 5 s of all 0: back to 0.
        assert!((h.update(0.0, at(14_000), cd) - 0.3).abs() < 1e-9);
        assert_eq!(h.update(0.0, at(16_500), cd), 0.0);
    }

    fn throttle_at(l0: Rc<Cell<usize>>) -> Throttle {
        Throttle::new(
            L0ThrottleConfig::default(),
            8,
            move || l0.get(),
            Arc::default(),
        )
    }

    #[test]
    fn below_soft_never_sheds_and_never_reads_clock() {
        let l0 = Rc::new(Cell::new(4));
        let t = throttle_at(Rc::clone(&l0));
        for _ in 0..10_000 {
            assert!(t.admit_ingress());
            assert!(!t.defer_inbound());
            assert!(!t.defer_dispatch());
        }
        assert_eq!(
            t.tick(),
            (false, false),
            "no throttle ⇒ the serving loop needs no tick"
        );
        assert_eq!(t.gauge.shed(Shed::Ingress), 0);
    }

    #[test]
    fn at_cap_minus_one_sheds_about_ninety_percent_never_all() {
        let l0 = Rc::new(Cell::new(7));
        let t = throttle_at(Rc::clone(&l0));
        let n = 20_000;
        let rejected = (0..n).filter(|_| !t.admit_ingress()).count();
        let ratio = rejected as f64 / n as f64;
        assert!((0.87..0.93).contains(&ratio), "refusal ratio {ratio}");
        assert_eq!(t.gauge.shed(Shed::Ingress), rejected as u64);
        assert!((t.gauge.reject_ratio() - 0.9).abs() < 1e-9);
    }

    #[test]
    fn deferral_is_sticky_within_a_pass_and_wakes_the_loop() {
        let l0 = Rc::new(Cell::new(7));
        let t = throttle_at(Rc::clone(&l0));
        t.begin_pass();
        // Flip until one defer: from then on every `StartCall` in the pass is deferred.
        let mut first = None;
        for i in 0..1_000 {
            if t.defer_inbound() {
                first = Some(i);
                break;
            }
        }
        assert!(first.is_some());
        for _ in 0..100 {
            assert!(
                t.defer_inbound(),
                "after the first defer, the whole pass defers — order kept"
            );
        }
        // Child calls have their own flag: not set yet.
        assert!(!t.sticky_dispatch.get());
        let (nudge, active) = t.tick();
        assert!(nudge, "deferred work ⇒ kick the drainer");
        assert!(active);
        assert!(!t.tick().0, "one kick per batch of deferrals");
        // A new pass: no longer sticky.
        t.begin_pass();
        assert!(!t.sticky_inbound.get());
    }

    /// The coin-flip seed comes from `entropy`: the same installed source ⇒
    /// two independently built throttles (gauges at different addresses) flip
    /// the same sequence; the seed used to mix in the gauge's heap address.
    #[test]
    fn coin_flip_seed_comes_from_entropy() {
        struct Fixed;
        impl cairn_store::entropy::Source for Fixed {
            fn now(&self) -> std::time::SystemTime {
                std::time::UNIX_EPOCH
            }
            fn process_id(&self) -> u32 {
                1
            }
            fn fill(&self, buf: &mut [u8]) {
                buf.iter_mut()
                    .enumerate()
                    .for_each(|(i, b)| *b = (i as u8).wrapping_mul(37));
            }
        }
        let _g = cairn_store::entropy::install(Rc::new(Fixed));
        let rolls = || {
            let t = throttle_at(Rc::new(Cell::new(4)));
            (0..256).map(|_| t.roll(0.5)).collect::<Vec<_>>()
        };
        let (a, b) = (rolls(), rolls());
        assert_eq!(a, b);
        assert!(a.iter().any(|x| *x) && a.iter().any(|x| !*x));
    }

    #[test]
    fn disabled_never_sheds() {
        let t = Throttle::new(L0ThrottleConfig::off(), 8, || 8, Arc::default());
        assert!((0..1_000).all(|_| t.admit_ingress()));
        assert_eq!(t.level(), 0.0);
    }
}
