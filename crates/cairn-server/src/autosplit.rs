// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Automatic split** by load and by size (inspired by TiKV's split
//! controller and split check).
//!
//! Three layers, each with one job:
//!
//! 1. **Per-range load statistics** — [`RangeLoad`], written on the range's
//!    engine thread, never waiting on a lock: command count, turn time and a
//!    reservoir of 64 `owner_pk` samples weighted by turn time. The node's
//!    collector ([`LoadBook::rotate`], every 1 s) cuts them into windows and
//!    keeps 60 windows.
//! 2. **Approximate size** — one manifest GET
//!    ([`cairn_store::ObjectStores::range_size`], the visible part thanks to
//!    fork patch F6), only once the range has written another
//!    `size_check_diff` bytes and at most once every `size_check_every`.
//! 3. **The decider** — [`AutoSplit`], on the range's own leader: hot for
//!    `hot_secs` consecutive windows or larger than `max_range_bytes` ⇒
//!    suggest; in `on` mode, split, under four safety nets.
//! 4. **Merge suggestions** — only suggested, never merged automatically, in
//!    every mode except `off`: two adjacent ranges both cold for `merge_cold`
//!    in a row (6 h by default), total size < ¼ `max_range_bytes`, neither in
//!    a split cooldown, `external_depth` of both ≤ 1. Each node contributes
//!    the [`MergeFacts`] of the ranges it leads; the HTTP layer pairs them
//!    ([`merge_rows`]), with `scope=cluster` over the facts of every node.
//!
//! The default mode is `suggest`: observe first, automate later.

// The runtime's clock: outside simulation it is `std::time::Instant`, in
// deterministic simulation the virtual clock.
use tokio::time::Instant;
use {
    cairn_store::{PartitionKey, WriterHealth},
    serde::{Deserialize, Serialize},
    std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

/// Length of one statistics window.
pub const WINDOW: Duration = Duration::from_secs(1);
/// Number of windows kept in RAM per range.
pub const KEPT_WINDOWS: usize = 60;
/// Size of a window's `owner_pk` sample reservoir.
pub const RESERVOIR: usize = 64;
/// One owner holding this much of the weight or more ⇒ no split (`hot_owner`),
/// and the same when the best split point is skewed by this much or more.
pub const HOT_OWNER_SHARE: f64 = 0.8;
/// A window is **cold** when the range's share of turn slots is below this
/// fraction of the hot threshold: two cold ranges merged use under ⅕ of the
/// hot threshold, far from the split threshold — a merge does not invite a
/// split right after.
pub const COLD_FRACTION_OF_HOT: f64 = 0.1;
/// Maximum `external_depth` of each range in a merge suggestion: merging a
/// range that still stands on a long ancestor chain lengthens that chain.
pub const MERGE_MAX_EXTERNAL_DEPTH: u64 = 1;

/// `--auto-split=off|suggest|on`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AutoSplitMode {
    /// Statistics only; no suggestions, no splits.
    Off,
    /// Suggest through `GET /ranges/suggestions`, do not split.
    #[default]
    Suggest,
    /// Split for real, under the safety nets.
    On,
}

impl AutoSplitMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Suggest => "suggest",
            Self::On => "on",
        }
    }
}

impl std::str::FromStr for AutoSplitMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "off" => Ok(Self::Off),
            "suggest" => Ok(Self::Suggest),
            "on" => Ok(Self::On),
            other => Err(format!("--auto-split {other:?}: choose off, suggest or on")),
        }
    }
}

/// Knobs of automatic splitting.
#[derive(Clone, Debug, PartialEq)]
pub struct AutoSplitConfig {
    pub mode: AutoSplitMode,
    /// A range using this share of the node's turn slots in a window is "hot".
    /// Turn slots = `--max-concurrent-turns` × the window length.
    pub hot_share: f64,
    /// Number of **consecutive** hot windows before suggesting; one cold window
    /// restarts the count. At most [`KEPT_WINDOWS`].
    pub hot_secs: u32,
    /// Cap on a range's visible size.
    pub max_range_bytes: u64,
    /// Re-read the manifest to estimate the size only once the range has
    /// written this many more bytes since last time…
    pub size_check_diff: u64,
    /// …and no sooner than this after last time.
    pub size_check_every: Duration,
    /// A range (and its two children) is not split automatically again within this.
    pub cooldown: Duration,
    /// A node splits automatically at most once per this.
    pub node_interval: Duration,
    /// Suggest merging two adjacent ranges once both have been cold this long
    /// in a row. `ZERO` = no merge suggestions.
    pub merge_cold: Duration,
    /// `on` mode splits for load only while the table has fewer ranges than this (`0` =
    /// no cap); splits for size are not capped. There is no automatic merge, so without a
    /// cap a low `hot_share` can keep splitting until the nodes saturate.
    pub max_ranges: usize,
}

impl Default for AutoSplitConfig {
    fn default() -> Self {
        Self {
            mode: AutoSplitMode::default(),
            hot_share: 0.5,
            hot_secs: 60,
            max_range_bytes: 8 << 30,
            size_check_diff: 64 << 20,
            size_check_every: Duration::from_secs(300),
            cooldown: Duration::from_secs(600),
            node_interval: Duration::from_secs(60),
            merge_cold: Duration::from_secs(6 * 3600),
            max_ranges: 128,
        }
    }
}

impl AutoSplitConfig {
    /// Sets a command-line flag. `flag` is the full flag name (`--auto-split`,
    /// `--auto-split-hot-share`, …, `--max-range-bytes`).
    pub fn set(&mut self, flag: &str, value: &str) -> Result<(), String> {
        let num = |v: &str| -> Result<u64, String> {
            v.parse::<u64>()
                .map_err(|_| format!("{flag} {v:?} is not a non-negative integer"))
        };
        match flag {
            "--auto-split" => self.mode = value.parse()?,
            "--auto-split-hot-share" => {
                let f: f64 = value
                    .parse()
                    .map_err(|_| format!("{flag} {value:?} is not a number"))?;
                if !(f > 0.0 && f <= 1.0) {
                    return Err(format!("{flag} must be in (0, 1]"));
                }
                self.hot_share = f;
            }
            "--auto-split-hot-secs" => {
                let n = num(value)?;
                if n == 0 || n > KEPT_WINDOWS as u64 {
                    return Err(format!("{flag} must be in 1..={KEPT_WINDOWS}"));
                }
                self.hot_secs = n as u32;
            }
            "--max-range-bytes" => self.max_range_bytes = num(value)?.max(1),
            "--auto-split-size-check-bytes" => self.size_check_diff = num(value)?,
            "--auto-split-size-check-ms" => {
                self.size_check_every = Duration::from_millis(num(value)?)
            }
            "--auto-split-cooldown-ms" => self.cooldown = Duration::from_millis(num(value)?),
            "--auto-split-node-interval-ms" => {
                self.node_interval = Duration::from_millis(num(value)?)
            }
            "--auto-split-merge-cold-ms" => self.merge_cold = Duration::from_millis(num(value)?),
            "--auto-split-max-ranges" => {
                self.max_ranges = usize::try_from(num(value)?)
                    .map_err(|_| format!("{flag} {value:?} is too large"))?
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        Ok(())
    }

    /// Whether flag `flag` belongs to automatic splitting — `main` passes them to [`Self::set`].
    pub fn owns(flag: &str) -> bool {
        flag.starts_with("--auto-split") || flag == "--max-range-bytes"
    }
}

// ─────────────────────────────────────────── per-range load statistics

/// The load of **one** range on this node. The range's engine thread writes;
/// the node's collector ([`LoadBook::rotate`]) cuts windows; readers
/// (`/ranges/stats`, the decider) only read history.
///
/// **The hot path never waits on a lock.** The counters are atomics. The
/// reservoir sits behind a mutex the engine thread only `try_lock`s: the
/// collector holds it a few µs per second, and a sample landing in exactly
/// that gap is dropped — the reservoir is a sample, losing one changes
/// nothing.
///
/// One copy per range id per node, living across terms (a range that becomes
/// leader again on this node continues its history). Ids are never reused
/// (D22).
pub struct RangeLoad {
    range: u32,
    /// Keyed commands of the range that passed the range check (`actor::serve`).
    ops: AtomicU64,
    /// µs of turns finished in the open window.
    turn_us: AtomicU64,
    /// Accumulated turn µs — for the `cairnflow_range_turn_ms_total` metric.
    turn_us_total: AtomicU64,
    /// µs the turns finished in the open window spent in the SDK (the handler itself):
    /// the hot signal ([`turn_share`]).
    handler_us: AtomicU64,
    ops_total: AtomicU64,
    reservoir: Mutex<Reservoir>,
    /// Number of serving loops leading this range on the node (0 or 1).
    leading: AtomicUsize,
    /// The writer of the leading term — written bytes are read from it.
    writer: Mutex<Option<WriterHealth>>,
    history: Mutex<History>,
    /// The range's L0 throttle — the engine thread writes, `/metrics` reads.
    l0: Arc<crate::l0::L0Gauge>,
    /// When the row was created — a row that never led here (a child that
    /// received statistics and then went to another node) must also be
    /// forgotten.
    created: Instant,
}

impl RangeLoad {
    fn new(range: u32) -> Self {
        Self {
            range,
            ops: AtomicU64::new(0),
            turn_us: AtomicU64::new(0),
            turn_us_total: AtomicU64::new(0),
            handler_us: AtomicU64::new(0),
            ops_total: AtomicU64::new(0),
            reservoir: Mutex::new(Reservoir::new(range)),
            leading: AtomicUsize::new(0),
            writer: Mutex::new(None),
            history: Mutex::new(History::default()),
            l0: Arc::default(),
            created: Instant::now(),
        }
    }

    pub fn range(&self) -> u32 {
        self.range
    }

    /// The range's L0 throttle measurements — the term's [`crate::l0::Throttle`] writes into it.
    pub fn l0_gauge(&self) -> Arc<crate::l0::L0Gauge> {
        Arc::clone(&self.l0)
    }

    /// One keyed command of the range just passed the range check.
    pub fn note_op(&self) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.ops_total.fetch_add(1, Ordering::Relaxed);
    }

    /// One SDK turn of an invocation with partition key `owner` just finished
    /// after `took`. The reservoir sample is weighted by the handler time.
    /// A turn of `owner` finished: `took` from start to end, of which `handler` was spent
    /// in the SDK (the handler itself), the rest waiting for the engine or for durability.
    pub fn note_turn(&self, owner: PartitionKey, took: Duration, handler: Duration) {
        let us = (took.as_micros() as u64).max(1);
        self.turn_us.fetch_add(us, Ordering::Relaxed);
        self.turn_us_total.fetch_add(us, Ordering::Relaxed);
        let handler_us = (handler.min(took).as_micros() as u64).max(1);
        self.handler_us.fetch_add(handler_us, Ordering::Relaxed);
        if let Ok(mut r) = self.reservoir.try_lock() {
            r.offer(owner.0, handler_us as f64);
        }
    }

    /// A serving loop starts leading the range with writer `health`. The guard
    /// gives it back on step-down.
    pub fn lead(self: &Arc<Self>, health: WriterHealth) -> Leading {
        if let Ok(mut h) = self.history.lock() {
            h.writer_seen = health.bytes_written();
            // While not leading here we see no load: restart the cold count
            // (merge suggestions rely only on what this node has seen).
            if self.leading.load(Ordering::Acquire) == 0 {
                h.cold_from = Some(Instant::now());
            }
        }
        if let Ok(mut w) = self.writer.lock() {
            *w = Some(health);
        }
        self.leading.fetch_add(1, Ordering::AcqRel);
        Leading(Arc::clone(self))
    }

    pub fn is_leading(&self) -> bool {
        self.leading.load(Ordering::Acquire) > 0
    }

    /// Closes the open window (of length `len`) into history.
    fn rotate(&self, at_ms: u64, len: Duration) {
        let ops = self.ops.swap(0, Ordering::Relaxed);
        let turn_us = self.turn_us.swap(0, Ordering::Relaxed);
        let handler_us = self.handler_us.swap(0, Ordering::Relaxed);
        let samples = self
            .reservoir
            .lock()
            .map(|mut r| r.take())
            .unwrap_or_default();
        let written = self
            .writer
            .lock()
            .ok()
            .and_then(|w| w.as_ref().map(|h| h.bytes_written()));
        let Ok(mut h) = self.history.lock() else {
            return;
        };
        let bytes = match written {
            Some(now) => {
                let d = now.saturating_sub(h.writer_seen);
                h.writer_seen = now;
                d
            }
            None => 0,
        };
        h.bytes_since_size_check += bytes;
        let turn_ms = turn_us as f64 / 1000.0;
        let handler_ms = handler_us as f64 / 1000.0;
        let sample_weight = if samples.is_empty() {
            0.0
        } else {
            handler_ms / samples.len() as f64
        };
        h.push(Window {
            at_ms,
            len_ms: len.as_secs_f64() * 1000.0,
            ops,
            turn_ms,
            handler_ms,
            bytes,
            samples,
            sample_weight,
        });
    }

    /// A copy of the history (oldest first).
    pub(crate) fn windows(&self) -> Vec<Window> {
        self.history
            .lock()
            .map(|h| h.windows.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn with_history<T>(&self, f: impl FnOnce(&mut History) -> T) -> Option<T> {
        self.history.lock().ok().map(|mut h| f(&mut h))
    }

    pub fn turn_ms_total(&self) -> u64 {
        self.turn_us_total.load(Ordering::Relaxed) / 1000
    }
}

/// Guard of [`RangeLoad::lead`].
pub struct Leading(Arc<RangeLoad>);

impl Drop for Leading {
    fn drop(&mut self) {
        if self.0.leading.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Ok(mut w) = self.0.writer.lock() {
                *w = None;
            }
            if let Ok(mut h) = self.0.history.lock() {
                h.led_until = Some(Instant::now());
            }
        }
    }
}

/// Weighted reservoir (A-Res, Efraimidis–Spirakis): each sample gets the key
/// `ln(u) / w`, keeping the [`RESERVOIR`] largest keys — the probability of a
/// turn being present is proportional to its time. Random numbers come from a
/// private xorshift seeded by the range id: good enough for sampling, no need
/// for the operating system's source on the hot path.
#[derive(Debug)]
struct Reservoir {
    rng: u64,
    items: Vec<(f64, u64)>,
}

impl Reservoir {
    fn new(seed: u32) -> Self {
        Self {
            rng: 0x9e37_79b9_7f4a_7c15 ^ (seed as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9) | 1,
            items: Vec::with_capacity(RESERVOIR),
        }
    }

    fn uniform(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        // (0, 1]: never 0, so `ln` is finite.
        ((x >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }

    fn offer(&mut self, owner: u64, weight: f64) {
        let key = self.uniform().ln() / weight;
        if self.items.len() < RESERVOIR {
            self.items.push((key, owner));
            return;
        }
        let (min_i, min_k) =
            self.items
                .iter()
                .enumerate()
                .fold(
                    (0, f64::INFINITY),
                    |(mi, mk), (i, (k, _))| if *k < mk { (i, *k) } else { (mi, mk) },
                );
        if key > min_k {
            self.items[min_i] = (key, owner);
        }
    }

    fn take(&mut self) -> Vec<u64> {
        self.items.drain(..).map(|(_, o)| o).collect()
    }
}

/// A closed window.
#[derive(Clone, Debug)]
pub(crate) struct Window {
    pub at_ms: u64,
    pub len_ms: f64,
    pub ops: u64,
    /// Wall time of the turns finished in the window (statistics, the metric).
    pub turn_ms: f64,
    /// The part of `turn_ms` spent in the SDK — the handler itself, without engine and
    /// durability waits: the hot signal.
    pub handler_ms: f64,
    pub bytes: u64,
    /// `owner_pk` of the samples; each sample weighs `sample_weight` ms of handler
    /// (samples were taken in proportion to time, so splitting the window's
    /// total evenly is an unbiased estimate).
    pub samples: Vec<u64>,
    pub sample_weight: f64,
}

/// The history of one range — windows, latest measured size.
#[derive(Debug, Default)]
pub(crate) struct History {
    pub windows: VecDeque<Window>,
    /// The writer's `bytes_written` at the previous collection.
    writer_seen: u64,
    /// Bytes written since the latest size read.
    pub bytes_since_size_check: u64,
    pub size: Option<SizeCheck>,
    /// When the last serving loop stopped leading — to clean up ranges that moved elsewhere.
    led_until: Option<Instant>,
    /// Start of the current run of cold windows: the moment of becoming leader
    /// on this node, moved forward at every window that is not cold.
    pub cold_from: Option<Instant>,
    /// The latest size read for a merge suggestion, failed ones included — at
    /// most one per `size_check_every`.
    size_tried: Option<Instant>,
}

impl History {
    fn push(&mut self, w: Window) {
        self.windows.push_back(w);
        while self.windows.len() > KEPT_WINDOWS {
            self.windows.pop_front();
        }
    }
}

/// The latest size read.
#[derive(Clone, Debug)]
pub(crate) struct SizeCheck {
    pub at: Instant,
    pub size: cairn_store::RangeSize,
}

/// The load registry of every range on **one** node. `Clone` is cheap; the
/// engine pool carries it to each range, like [`crate::range_stats::RangeStats`].
#[derive(Clone, Default)]
pub struct LoadBook(Arc<Mutex<BTreeMap<u32, Arc<RangeLoad>>>>);

/// A range no longer led on this node for longer than this has its history dropped.
const FORGET_AFTER: Duration = Duration::from_secs(120);

impl LoadBook {
    /// The row of range `range` (created if missing).
    pub fn range(&self, range: u32) -> Arc<RangeLoad> {
        let mut m = self.0.lock().expect("load registry is not poisoned");
        Arc::clone(
            m.entry(range)
                .or_insert_with(|| Arc::new(RangeLoad::new(range))),
        )
    }

    /// Every row, by id.
    pub(crate) fn all(&self) -> Vec<Arc<RangeLoad>> {
        self.0
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Closes the window of every range; cleans up ranges no longer led here for a long time.
    pub fn rotate(&self, at_ms: u64, len: Duration) {
        for l in self.all() {
            l.rotate(at_ms, len);
        }
        // The copy from `all()` has been dropped: `strong_count > 1` means a
        // serving loop (a live term) still holds this row.
        if let Ok(mut m) = self.0.lock() {
            m.retain(|_, l| {
                l.is_leading()
                    || Arc::strong_count(l) > 1
                    || l.with_history(|h| h.led_until.unwrap_or(l.created).elapsed() < FORGET_AFTER)
                        .unwrap_or(false)
            });
        }
    }

    /// The two children of a split receive **½ of the parent's statistics**
    /// (like TiKV's split check): each parent window becomes a child window
    /// with half the commands, half the turn time, half the bytes; of the
    /// samples, only those within the child's key range are kept. The child's
    /// own windows (if any) come after.
    pub(crate) fn inherit(&self, parent: u32, children: &[(u32, u64, u64)]) {
        let Some(p) = self.all().into_iter().find(|l| l.range == parent) else {
            return;
        };
        let windows = p.windows();
        for (c, lo, hi) in children {
            let child = self.range(*c);
            child.with_history(|h| {
                let own: Vec<Window> = h.windows.drain(..).collect();
                for w in &windows {
                    h.push(Window {
                        ops: w.ops / 2,
                        turn_ms: w.turn_ms / 2.0,
                        handler_ms: w.handler_ms / 2.0,
                        bytes: w.bytes / 2,
                        samples: w
                            .samples
                            .iter()
                            .copied()
                            .filter(|o| lo <= o && o <= hi)
                            .collect(),
                        ..w.clone()
                    });
                }
                for w in own {
                    h.push(w);
                }
            });
        }
    }

    /// L0 metrics of the ranges this node leads — read from the term's writer,
    /// no GET at all.
    pub fn l0_rows(&self) -> Vec<crate::l0::RangeL0Row> {
        self.all()
            .iter()
            .filter_map(|l| {
                let w = l.writer.lock().ok()?;
                Some(crate::l0::RangeL0Row::of(l.range, w.as_ref()?, &l.l0))
            })
            .collect()
    }

    /// Writer and L0 throttle of the ranges this node leads — for the
    /// standalone-child enforcer after splits (`crate::lineage`). No GET at
    /// all.
    pub(crate) fn leading_writers(&self) -> Vec<(u32, WriterHealth, Arc<crate::l0::L0Gauge>)> {
        self.all()
            .iter()
            .filter_map(|l| {
                let w = l.writer.lock().ok()?.clone()?;
                Some((l.range, w, Arc::clone(&l.l0)))
            })
            .collect()
    }

    /// Accumulated turn time per range (ms) — a metric.
    pub fn turn_ms_totals(&self) -> Vec<(u32, u64)> {
        self.all()
            .iter()
            .filter(|l| l.turn_us_total.load(Ordering::Relaxed) > 0)
            .map(|l| (l.range, l.turn_ms_total()))
            .collect()
    }

    /// Snapshot for `GET /ranges/stats`: the ranges this node leads.
    /// `capacity_turns` is the node's turn cap — the denominator of `turnShare`.
    pub fn view(&self, capacity_turns: usize, detail: bool) -> Vec<RangeLoadView> {
        self.all()
            .iter()
            .filter(|l| l.is_leading())
            .map(|l| RangeLoadView::of(l, capacity_turns, detail))
            .collect()
    }
}

/// The range's share of the node's turn slots in window `w`, counting the time its turns
/// spent in the handler ([`Window::handler_ms`]) — not the wall time: waits for the
/// engine and for durability grow as the node slows (more ranges, more background work),
/// and counting them made ranges look hotter the slower the node got, so they split more.
pub(crate) fn turn_share(w: &Window, capacity_turns: usize) -> f64 {
    let cap = capacity_turns.max(1) as f64 * w.len_ms.max(1.0);
    w.handler_ms / cap
}

/// The heaviest owner in `samples` (`owner`, weight) and its share.
pub(crate) fn top_owner(samples: &[(u64, f64)]) -> Option<(u64, f64)> {
    let mut by: BTreeMap<u64, f64> = BTreeMap::new();
    let mut total = 0.0;
    for (o, w) in samples {
        *by.entry(*o).or_default() += w;
        total += w;
    }
    if total <= 0.0 {
        return None;
    }
    by.into_iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(o, w)| (o, w / total))
}

/// The weighted samples of windows `windows`.
pub(crate) fn weighted_samples<'a>(
    windows: impl IntoIterator<Item = &'a Window>,
) -> Vec<(u64, f64)> {
    windows
        .into_iter()
        .flat_map(|w| w.samples.iter().map(move |o| (*o, w.sample_weight)))
        .filter(|(_, w)| *w > 0.0)
        .collect()
}

/// One row of `GET /ranges/stats`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeLoadView {
    pub range: u32,
    /// The answering node (filled in by the HTTP layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Number of 1 s windows held (at most 60).
    pub windows: usize,
    pub ops_total: u64,
    pub turn_ms_total: u64,
    /// Average over the windows held.
    pub ops_per_sec: f64,
    pub turn_ms_per_sec: f64,
    pub bytes_per_sec: f64,
    /// Share of the node's turn slots, latest window.
    pub turn_share: f64,
    /// Number of latest consecutive windows with a turn-slot share of
    /// `hot_share` or more — filled in by the decider; `None` when automatic
    /// splitting is off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_streak: Option<u32>,
    /// The heaviest owner in the samples of the windows held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_owner: Option<OwnerShare>,
    /// The latest measured visible size, bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_checked_ms_ago: Option<u64>,
    pub bytes_since_size_check: u64,
    /// Each window (`?detail=1`), oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent: Vec<WindowView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerShare {
    /// The owner's partition key, 16 hex digits (the same form as split's `?at=`).
    pub owner_pk: String,
    pub share: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowView {
    pub at_ms: u64,
    pub ops: u64,
    pub turn_ms: f64,
    pub bytes: u64,
    pub samples: usize,
}

impl RangeLoadView {
    fn of(l: &RangeLoad, capacity_turns: usize, detail: bool) -> Self {
        let windows = l.windows();
        let secs: f64 = windows.iter().map(|w| w.len_ms).sum::<f64>() / 1000.0;
        let per_sec = |v: f64| if secs > 0.0 { v / secs } else { 0.0 };
        let (size, since) = l
            .with_history(|h| {
                (
                    h.size
                        .as_ref()
                        .map(|s| (s.size.visible_bytes, s.at.elapsed().as_millis() as u64)),
                    h.bytes_since_size_check,
                )
            })
            .unwrap_or((None, 0));
        Self {
            range: l.range,
            node: None,
            windows: windows.len(),
            ops_total: l.ops_total.load(Ordering::Relaxed),
            turn_ms_total: l.turn_ms_total(),
            ops_per_sec: per_sec(windows.iter().map(|w| w.ops as f64).sum()),
            turn_ms_per_sec: per_sec(windows.iter().map(|w| w.turn_ms).sum()),
            bytes_per_sec: per_sec(windows.iter().map(|w| w.bytes as f64).sum()),
            turn_share: windows
                .last()
                .map_or(0.0, |w| turn_share(w, capacity_turns)),
            hot_streak: None,
            top_owner: top_owner(&weighted_samples(&windows)).map(|(o, share)| OwnerShare {
                owner_pk: format!("{o:016x}"),
                share,
            }),
            size_bytes: size.map(|(b, _)| b),
            size_checked_ms_ago: size.map(|(_, ago)| ago),
            bytes_since_size_check: since,
            recent: if detail {
                windows
                    .iter()
                    .map(|w| WindowView {
                        at_ms: w.at_ms,
                        ops: w.ops,
                        turn_ms: w.turn_ms,
                        bytes: w.bytes,
                        samples: w.samples.len(),
                    })
                    .collect()
            } else {
                Vec::new()
            },
        }
    }
}

/// Wall clock, ms.
pub(crate) fn wall_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ─────────────────────────────────────────── choosing the key

/// Result of choosing a key over a set of weighted samples.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum KeyChoice {
    /// Split at `at` (first key of the right child); `balance` = |left − right| / total.
    Split { at: u64, balance: f64 },
    /// One owner holds too much weight (or no split point is balanced enough):
    /// splitting does not help — that is flow control's job.
    HotOwner { owner: u64, share: f64 },
}

/// **Weighted median** of `samples` (`owner_pk`, weight) within
/// `[start, end]`. Candidates are the gaps between two adjacent owners seen;
/// choose the gap that leaves the two sides least skewed, and split in the
/// **middle** of the gap — owners not seen within the gap are shared evenly
/// by both sides. `None` = no samples.
pub(crate) fn choose_key(samples: &[(u64, f64)], start: u64, end: u64) -> Option<KeyChoice> {
    let mut by: BTreeMap<u64, f64> = BTreeMap::new();
    for (o, w) in samples {
        if start <= *o && *o <= end && *w > 0.0 {
            *by.entry(*o).or_default() += w;
        }
    }
    let total: f64 = by.values().sum();
    if total <= 0.0 {
        return None;
    }
    let (top, top_w) = by
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(o, w)| (*o, *w))
        .expect("at least one owner");
    let hot = KeyChoice::HotOwner {
        owner: top,
        share: top_w / total,
    };
    if top_w / total >= HOT_OWNER_SHARE {
        return Some(hot);
    }
    let owners: Vec<(u64, f64)> = by.into_iter().collect();
    let mut best: Option<(f64, u64)> = None;
    let mut left = 0.0;
    for pair in owners.windows(2) {
        let ((lo, w), (hi, _)) = (pair[0], pair[1]);
        left += w;
        let balance = ((2.0 * left - total) / total).abs();
        if best.is_none_or(|(b, _)| balance < b) {
            // `lo < at ≤ hi`, middle of the gap.
            best = Some((balance, lo + 1 + (hi - lo - 1) / 2));
        }
    }
    match best {
        Some((balance, at)) if balance < HOT_OWNER_SHARE => Some(KeyChoice::Split { at, balance }),
        _ => Some(hot),
    }
}

/// The midpoint of `[start, end]`, as the default of `POST /ranges/{id}/split`.
pub(crate) fn midpoint(start: u64, end: u64) -> u64 {
    start + (end - start) / 2 + 1
}

/// Number of latest consecutive windows with a turn-slot share of `hot_share` or more.
pub(crate) fn hot_streak(windows: &[Window], capacity_turns: usize, hot_share: f64) -> usize {
    windows
        .iter()
        .rev()
        .take_while(|w| turn_share(w, capacity_turns) >= hot_share)
        .count()
}

// ─────────────────────────────────────────── suggestions

/// Why a range is suggested.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// Hot for `hot_secs` consecutive windows.
    Load,
    /// Larger than `max_range_bytes`.
    Size,
    /// Hot, but concentrated on one owner — **no** split.
    HotOwner,
    /// Two adjacent ranges both cold for long and small — a merge suggestion,
    /// never done automatically.
    Merge,
}

/// One row of `GET /ranges/suggestions`. A merge row (`reason: merge`):
/// `range` is the left range, `mergeWith` the right one, `sizeBytes` the total size.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Suggestion {
    pub range: u32,
    pub reason: Reason,
    /// The split key (16 hex digits, split's `?at=` form). `None` with `hot_owner`.
    pub at: Option<String>,
    /// |left − right| / total at the split key, by the load samples (0 =
    /// perfectly balanced). With `hot_owner` it is that owner's share. `None`
    /// when splitting for size with no SST fragment to balance by.
    pub balance: Option<f64>,
    /// What blocks the split: `max_ranges`, `cooldown`, `node_rate`, `intent`,
    /// `upgrading`, `l0`, `busy`, `conflict`, `failed`. `None` = can split (in `on` mode,
    /// about to split).
    pub blocked_by: Option<String>,
    /// The hot `owner_pk` (hex) with `hot_owner`.
    pub owner: Option<String>,
    /// Average turn-slot share over the run of hot windows.
    pub turn_share: f64,
    /// Number of consecutive hot windows.
    pub hot_secs: u32,
    /// The latest measured visible size, bytes.
    pub size_bytes: Option<u64>,
    /// Wall clock (ms) since which this reason has held continuously.
    pub since_ms: u64,
    /// The suggesting node (filled in by the HTTP layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// The right range of a merge suggestion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_with: Option<u32>,
    /// Merge suggestion: seconds both ranges have been cold in a row (of the less-cold one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_secs: Option<u64>,
}

/// Facts about a range the node leads, for merge suggestions. Reads only the
/// node's RAM; the size is automatic splitting's latest manifest read.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeFacts {
    pub range: u32,
    /// Cold in a row for this long, ms (since becoming leader on this node).
    pub cold_ms: u64,
    /// The latest measured visible size. `None` = not measured yet.
    pub size_bytes: Option<u64>,
    /// The writer's `external_depth`. `None` = unknown (store model).
    pub external_depth: Option<u64>,
    /// Within the cooldown of an automatic split.
    pub cooling: bool,
    /// The leading node (filled in by the HTTP layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

/// Whether window `w` is cold ([`COLD_FRACTION_OF_HOT`]).
pub(crate) fn is_cold(w: &Window, capacity_turns: usize, hot_share: f64) -> bool {
    turn_share(w, capacity_turns) < hot_share * COLD_FRACTION_OF_HOT
}

/// Pairs merge suggestions from `facts` (ranges without facts are not paired)
/// on table `table`. Left to right, greedily: a range appears in at most one
/// suggestion. If the table still has an intent, the row carries
/// `blockedBy: intent`, as split suggestions do. `now_ms` is the wall clock —
/// `sinceMs` = when the pair first met `merge_cold`.
pub fn merge_rows(
    cfg: &AutoSplitConfig,
    table: &cairn_store::PartitionTable,
    facts: &[MergeFacts],
    now_ms: u64,
) -> Vec<Suggestion> {
    if cfg.mode == AutoSplitMode::Off || cfg.merge_cold.is_zero() {
        return Vec::new();
    }
    let by: BTreeMap<u32, &MergeFacts> = facts.iter().map(|f| (f.range, f)).collect();
    let need = cfg.merge_cold.as_millis() as u64;
    let eligible = |id: u32| {
        by.get(&id).copied().filter(|f| {
            f.cold_ms >= need
                && f.size_bytes.is_some()
                && f.external_depth
                    .is_some_and(|d| d <= MERGE_MAX_EXTERNAL_DEPTH)
                && !f.cooling
        })
    };
    let ranges = table.ranges();
    let any_intent = ranges.iter().any(|r| r.intent.is_some());
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < ranges.len() {
        let (l, r) = (&ranges[i], &ranges[i + 1]);
        let pair = match (eligible(l.id.0), eligible(r.id.0)) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        };
        let Some((a, b)) = pair else {
            i += 1;
            continue;
        };
        let size = a.size_bytes.unwrap_or(0) + b.size_bytes.unwrap_or(0);
        if l.end.checked_add(1) != Some(r.start) || size >= cfg.max_range_bytes / 4 {
            i += 1;
            continue;
        }
        let cold = a.cold_ms.min(b.cold_ms);
        out.push(Suggestion {
            range: l.id.0,
            reason: Reason::Merge,
            at: None,
            balance: None,
            blocked_by: any_intent.then(|| "intent".to_string()),
            owner: None,
            turn_share: 0.0,
            hot_secs: 0,
            size_bytes: Some(size),
            since_ms: now_ms.saturating_sub(cold - need),
            node: a.node.clone(),
            merge_with: Some(r.id.0),
            cold_secs: Some(cold / 1000),
        });
        i += 2;
    }
    out
}

/// Automatic splits finished on this node (counted for metrics).
pub static AUTO_SPLITS: AtomicU64 = AtomicU64::new(0);
/// Automatic splits that wrote the intent and then failed (rolled back).
pub static AUTO_SPLITS_FAILED: AtomicU64 = AtomicU64::new(0);

/// After a temporary refusal, wait this long before retrying that range.
const RETRY_AFTER: Duration = Duration::from_secs(30);
/// In `suggest` mode, re-probe blockers that need a GET (upgrade, L0) at most this often.
const PROBE_EVERY: Duration = Duration::from_secs(30);

#[derive(Default)]
struct DeciderState {
    suggestions: BTreeMap<u32, Suggestion>,
    /// Do not split this range automatically before this — the cooldown, also
    /// for the children of a split.
    cooldown_until: BTreeMap<u32, Instant>,
    /// The latest attempt was refused temporarily: retry later, and the reason.
    retry: BTreeMap<u32, (Instant, String)>,
    /// The result of probing blockers in `suggest` mode.
    probed: BTreeMap<u32, (Instant, Option<String>)>,
    /// The node's latest automatic split (at most one per `node_interval`).
    last_split: Option<Instant>,
    /// `last_split` was restored from the op ledger on the bucket (once per process: the
    /// newest ledger is the cluster's latest split or merge).
    seeded: bool,
    /// Ranges whose birth was looked up in the op ledger (their cooldown restored).
    born_checked: std::collections::BTreeSet<u32>,
    /// An automatic split in progress.
    in_flight: bool,
}

/// The decider's choice for one round: the range to split now and where (`on` mode), and
/// the ranges to probe for blockers and where (`suggest` mode).
type Chosen = (Option<(u32, u64)>, Vec<(u32, u64)>);

/// What [`choose`] needs to know from outside the decider's own state.
struct Around {
    now: Instant,
    /// Some range of the table carries a split/merge intent.
    any_intent: bool,
    /// Number of ranges in the table.
    ranges: usize,
}

/// The decider's choice for one round, a pure function of its state, the round's
/// suggestions and `around`: marks each suggestion's `blocked_by`, and returns the range
/// to split now (`on` mode, at most one) and the ranges to probe for blockers (`suggest`
/// mode). Sets `in_flight` when it picks a split.
fn choose(
    st: &mut DeciderState,
    fresh: &mut BTreeMap<u32, Suggestion>,
    cfg: &AutoSplitConfig,
    around: &Around,
) -> Chosen {
    let now = around.now;
    let any_intent = around.any_intent;
    let mut act: Option<(u32, u64)> = None;
    let mut probe: Vec<(u32, u64)> = Vec::new();
    st.cooldown_until.retain(|_, t| *t > now);
    st.retry.retain(|_, (t, _)| *t > now);
    st.probed
        .retain(|_, (t, _)| now.duration_since(*t) < PROBE_EVERY);
    let node_rate = st
        .last_split
        .is_some_and(|t| now.duration_since(t) < cfg.node_interval);
    // Hottest first: load before size, larger turn-slot share first.
    let mut order: Vec<&mut Suggestion> = fresh.values_mut().collect();
    order.sort_by(|a, b| {
        (a.reason != Reason::Load)
            .cmp(&(b.reason != Reason::Load))
            .then(b.turn_share.total_cmp(&a.turn_share))
    });
    for s in order {
        if let Some(old) = st.suggestions.get(&s.range) {
            if old.reason == s.reason {
                s.since_ms = old.since_ms;
            }
        }
        if s.reason == Reason::HotOwner {
            continue;
        }
        let capped =
            s.reason == Reason::Load && cfg.max_ranges > 0 && around.ranges >= cfg.max_ranges;
        let blocked = if capped {
            Some("max_ranges".to_string())
        } else if st.cooldown_until.contains_key(&s.range) {
            Some("cooldown".to_string())
        } else if node_rate || (st.in_flight && act.is_none()) {
            Some("node_rate".to_string())
        } else if any_intent {
            Some("intent".to_string())
        } else if let Some((_, why)) = st.retry.get(&s.range) {
            Some(why.clone())
        } else {
            None
        };
        s.blocked_by = blocked;
        let at =
            s.at.as_deref()
                .and_then(|a| u64::from_str_radix(a, 16).ok());
        match (s.blocked_by.is_none(), at, cfg.mode) {
            (true, Some(at), AutoSplitMode::On) if act.is_none() && !st.in_flight => {
                act = Some((s.range, at));
            }
            // Only one split per round: the rest waits for the node's cap.
            (true, Some(_), AutoSplitMode::On) => s.blocked_by = Some("node_rate".into()),
            (true, Some(at), AutoSplitMode::Suggest) => match st.probed.get(&s.range) {
                Some((_, why)) => s.blocked_by = why.clone(),
                None => probe.push((s.range, at)),
            },
            _ => {}
        }
    }
    if act.is_some() {
        st.in_flight = true;
    }
    (act, probe)
}

// ─────────────────────────────────────────── one node's automatic splitting

/// Automatic splitting of **one** node: the load registry, collector and
/// decider. `Clone` is cheap; ingress keeps a copy for `/ranges/stats`,
/// `/ranges/suggestions`.
#[derive(Clone)]
pub struct AutoSplit(Arc<Inner>);

struct Inner {
    cfg: AutoSplitConfig,
    book: LoadBook,
    /// The node's turn cap — the denominator of the turn-slot share.
    capacity_turns: usize,
    /// The node's split coordinator. `None` ⇒ statistics only.
    ops: Option<crate::split::RangeOps>,
    state: Mutex<DeciderState>,
}

impl AutoSplit {
    pub fn new(
        cfg: AutoSplitConfig,
        book: LoadBook,
        capacity_turns: usize,
        ops: Option<crate::split::RangeOps>,
    ) -> Self {
        Self(Arc::new(Inner {
            cfg,
            book,
            capacity_turns: capacity_turns.max(1),
            ops,
            state: Mutex::new(DeciderState::default()),
        }))
    }

    pub fn config(&self) -> &AutoSplitConfig {
        &self.0.cfg
    }

    pub fn book(&self) -> &LoadBook {
        &self.0.book
    }

    fn state(&self) -> std::sync::MutexGuard<'_, DeciderState> {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Rows of `GET /ranges/stats` for the ranges this node leads.
    pub fn stats(&self, detail: bool) -> Vec<RangeLoadView> {
        let mut rows = self.0.book.view(self.0.capacity_turns, detail);
        if self.0.cfg.mode != AutoSplitMode::Off {
            for r in &mut rows {
                if let Some(l) = self.0.book.all().into_iter().find(|l| l.range == r.range) {
                    r.hot_streak = Some(hot_streak(
                        &l.windows(),
                        self.0.capacity_turns,
                        self.0.cfg.hot_share,
                    ) as u32);
                }
            }
        }
        rows
    }

    /// This node's **split** rows of `GET /ranges/suggestions`. Merge rows are
    /// paired in the HTTP layer from [`Self::merge_facts`] ([`merge_rows`]).
    pub fn suggestions(&self) -> Vec<Suggestion> {
        self.state().suggestions.values().cloned().collect()
    }

    /// Facts for merge suggestions about the ranges this node leads — RAM
    /// only, no request at all. Empty in `off` mode or when merging is off.
    pub fn merge_facts(&self) -> Vec<MergeFacts> {
        let cfg = &self.0.cfg;
        if cfg.mode == AutoSplitMode::Off || cfg.merge_cold.is_zero() || self.0.ops.is_none() {
            return Vec::new();
        }
        let now = Instant::now();
        let cooling: Vec<u32> = self
            .state()
            .cooldown_until
            .iter()
            .filter(|(_, t)| **t > now)
            .map(|(r, _)| *r)
            .collect();
        self.0
            .book
            .all()
            .iter()
            .filter(|l| l.is_leading())
            .filter_map(|l| {
                let depth = l
                    .writer
                    .lock()
                    .ok()
                    .and_then(|w| w.as_ref().and_then(|h| h.external_depth()))
                    .map(|d| d as u64);
                l.with_history(|h| MergeFacts {
                    range: l.range,
                    cold_ms: h
                        .cold_from
                        .map_or(0, |t| now.saturating_duration_since(t).as_millis() as u64),
                    size_bytes: h.size.as_ref().map(|s| s.size.visible_bytes),
                    external_depth: depth,
                    cooling: cooling.contains(&l.range),
                    node: None,
                })
            })
            .collect()
    }

    /// Merge suggestions over the node's current table from `facts` ([`merge_rows`]).
    pub fn merge_rows(&self, facts: &[MergeFacts]) -> Vec<Suggestion> {
        match &self.0.ops {
            Some(ops) => merge_rows(&self.0.cfg, &ops.cluster.table(), facts, wall_ms()),
            None => Vec::new(),
        }
    }

    /// The node's background loop: every [`WINDOW`] closes the window of every
    /// range, then (except in `off` mode) considers suggestions and — in `on`
    /// mode — splits. Dropping the handle ⇒ stop.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(cairn_store::io_source::tagged("autosplit", async move {
            let mut tick = tokio::time::interval(WINDOW);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            let mut last = Instant::now();
            loop {
                tick.tick().await;
                let now = Instant::now();
                self.0.book.rotate(wall_ms(), now - last);
                last = now;
                if self.0.cfg.mode != AutoSplitMode::Off {
                    self.decide().await;
                }
            }
        }))
    }

    /// Re-reads the range's size once enough has been written:
    /// `size_check_diff` bytes since last time, and no sooner than
    /// `size_check_every`.
    ///
    /// Merge suggestions need the size of cold ranges, which rarely write
    /// enough: a range cold for `merge_cold` with no measurement yet is read
    /// **once** (retried at most every `size_check_every`). A cold range
    /// hardly writes, so that measurement stays usable until it has written
    /// `size_check_diff` — no extra request at all while idle.
    async fn check_size(&self, ops: &crate::split::RangeOps, load: &RangeLoad) {
        let cfg = &self.0.cfg;
        let due = load
            .with_history(|h| {
                let by_writes = h.bytes_since_size_check >= cfg.size_check_diff.max(1)
                    && h.size
                        .as_ref()
                        .is_none_or(|s| s.at.elapsed() >= cfg.size_check_every);
                let for_merge = !cfg.merge_cold.is_zero()
                    && h.size.is_none()
                    && h.cold_from.is_some_and(|t| t.elapsed() >= cfg.merge_cold)
                    && h.size_tried
                        .is_none_or(|t| t.elapsed() >= cfg.size_check_every);
                if for_merge {
                    h.size_tried = Some(Instant::now());
                }
                by_writes || for_merge
            })
            .unwrap_or(false);
        if !due {
            return;
        }
        match ops
            .stores
            .range_size(cairn_store::PartitionId(load.range))
            .await
        {
            Ok(size) => {
                load.with_history(|h| {
                    h.size = Some(SizeCheck {
                        at: Instant::now(),
                        size,
                    });
                    h.bytes_since_size_check = 0;
                });
            }
            Err(e) => {
                tracing::debug!(range = load.range, error = %e, "cannot read the range size");
                // A failure still counts as one read: do not hammer a
                // misbehaving store with GETs.
                load.with_history(|h| {
                    if let Some(s) = &mut h.size {
                        s.at = Instant::now();
                    }
                    h.bytes_since_size_check = 0;
                });
            }
        }
    }

    /// One round of consideration.
    async fn decide(&self) {
        let Some(ops) = &self.0.ops else {
            return;
        };
        let cfg = &self.0.cfg;
        let mut fresh: BTreeMap<u32, Suggestion> = BTreeMap::new();
        for load in self.0.book.all() {
            if !load.is_leading() {
                continue;
            }
            let Some(entry) = ops
                .cluster
                .table()
                .range(cairn_store::PartitionId(load.range))
                .copied()
            else {
                continue;
            };
            if entry.start == entry.end {
                continue;
            }
            // The cold run: the newest window is not cold ⇒ restart the count.
            let warm = load
                .windows()
                .last()
                .is_some_and(|w| !is_cold(w, self.0.capacity_turns, cfg.hot_share));
            if warm {
                load.with_history(|h| h.cold_from = Some(Instant::now()));
            }
            self.check_size(ops, &load).await;
            let windows = load.windows();
            let streak = hot_streak(&windows, self.0.capacity_turns, cfg.hot_share);
            let load_hot = streak >= cfg.hot_secs as usize;
            let size = load.with_history(|h| h.size.clone()).flatten();
            let size_hot = size
                .as_ref()
                .is_some_and(|s| s.size.visible_bytes > cfg.max_range_bytes);
            if !load_hot && !size_hot {
                continue;
            }
            let hot_windows = &windows[windows.len() - streak..];
            let mean_share = if hot_windows.is_empty() {
                0.0
            } else {
                hot_windows
                    .iter()
                    .map(|w| turn_share(w, self.0.capacity_turns))
                    .sum::<f64>()
                    / hot_windows.len() as f64
            };
            let by_load = load_hot
                .then(|| choose_key(&weighted_samples(hot_windows), entry.start, entry.end))
                .flatten();
            let by_size = || {
                let pieces: Vec<(u64, f64)> = size
                    .as_ref()
                    .map(|s| s.size.pieces.iter().map(|(o, b)| (*o, *b as f64)).collect())
                    .unwrap_or_default();
                match choose_key(&pieces, entry.start, entry.end) {
                    Some(KeyChoice::Split { at, balance }) => (at, Some(balance)),
                    _ => (midpoint(entry.start, entry.end), None),
                }
            };
            let (reason, at, balance, owner) = match by_load {
                Some(KeyChoice::Split { at, balance }) => {
                    (Reason::Load, Some(at), Some(balance), None)
                }
                // One hot owner: splitting for load is useless. Over the size
                // cap it still splits for size.
                Some(KeyChoice::HotOwner { owner, share }) if !size_hot => {
                    (Reason::HotOwner, None, Some(share), Some(owner))
                }
                Some(KeyChoice::HotOwner { .. }) => {
                    let (at, balance) = by_size();
                    (Reason::Size, Some(at), balance, None)
                }
                // Hot but no samples yet (turns too short to enter the reservoir): the midpoint.
                None if load_hot => (
                    Reason::Load,
                    Some(midpoint(entry.start, entry.end)),
                    None,
                    None,
                ),
                None => {
                    let (at, balance) = by_size();
                    (Reason::Size, Some(at), balance, None)
                }
            };
            fresh.insert(
                load.range,
                Suggestion {
                    range: load.range,
                    reason,
                    at: at.map(|a| format!("{a:016x}")),
                    balance,
                    blocked_by: None,
                    owner: owner.map(|o| format!("{o:016x}")),
                    turn_share: mean_share,
                    hot_secs: streak as u32,
                    size_bytes: size.as_ref().map(|s| s.size.visible_bytes),
                    since_ms: wall_ms(),
                    node: None,
                    merge_with: None,
                    cold_secs: None,
                },
            );
        }

        // Blockers known in RAM; choose one range to split (`on` mode) or to
        // probe (`suggest` mode).
        let any_intent = ops
            .cluster
            .table()
            .ranges()
            .iter()
            .any(|r| r.intent.is_some());
        self.restore_from_bucket(ops, &fresh).await;
        let around = Around {
            now: Instant::now(),
            any_intent,
            ranges: ops.cluster.table().ranges().len(),
        };
        let (act, probe) = {
            let mut st = self.state();
            let chosen = choose(&mut st, &mut fresh, cfg, &around);
            st.suggestions = fresh;
            chosen
        };

        // `suggest` mode: probe blockers that need a GET, at an interval.
        for (range, at) in probe {
            let why = match ops.split_blocker(cairn_store::PartitionId(range), at).await {
                Ok(b) => b.map(|b| b.label().to_string()),
                Err(e) => Some(format!("busy: {e}")),
            };
            let mut st = self.state();
            st.probed.insert(range, (Instant::now(), why.clone()));
            if let Some(s) = st.suggestions.get_mut(&range) {
                s.blocked_by = why;
            }
        }

        if let Some((range, at)) = act {
            let me = self.clone();
            let ops = ops.clone();
            tokio::spawn(async move { me.split_now(&ops, range, at).await });
        }
    }

    /// Restores, from the op ledger on the bucket, what the decider used to keep only in
    /// RAM — so a restart (or a range led by another node than the one that split it) no
    /// longer erases it:
    ///
    /// - the node's split interval, once per process: the newest ledger younger than
    ///   `node_interval` counts as this node's latest split (conservative: it may be
    ///   another node's);
    /// - the cooldown of each range suggested for the first time: a range born (its
    ///   creating op's ledger last written) less than `cooldown` ago cools for the rest.
    ///
    /// Ages compare the store's `last_modified` with this node's wall clock, clamped to
    /// `[0, cooldown]`. A failed read counts as "just split" — the full interval or
    /// cooldown — and is read again later. The ledger stays at least `--gc-min-age-s`
    /// (1800 s by default) after its op settled, longer than the default cooldown.
    async fn restore_from_bucket(
        &self,
        ops: &crate::split::RangeOps,
        fresh: &BTreeMap<u32, Suggestion>,
    ) {
        use cairn_store::retry;
        const READ_BOUND: Duration = Duration::from_secs(5);
        let cfg = &self.0.cfg;
        if cfg.mode != AutoSplitMode::On {
            return;
        }
        let store = ops.stores.main().store();
        let prefix = ops.stores.main().prefix();
        let age = |written_ms: u64| Duration::from_millis(wall_ms().saturating_sub(written_ms));
        if !self.state().seeded {
            let listed = retry::deadline(
                &retry::ops::AUTOSPLIT_LEDGER_READ,
                READ_BOUND,
                cairn_store::ledger::list_ops(store, prefix),
            )
            .await;
            let now = Instant::now();
            let mut st = self.state();
            match listed {
                Ok(Ok(ledgers)) => {
                    st.seeded = true;
                    if let Some(newest) = ledgers.iter().map(|(_, ms)| *ms).max() {
                        let age = age(newest);
                        if age < cfg.node_interval {
                            let at = now.checked_sub(age).unwrap_or(now);
                            st.last_split = Some(st.last_split.map_or(at, |t| t.max(at)));
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "auto-split: cannot list the op ledger — counting a split just now");
                    st.last_split = Some(now);
                }
                Err(_) => {
                    tracing::debug!(
                        "auto-split: listing the op ledger timed out — counting a split just now"
                    );
                    st.last_split = Some(now);
                }
            }
        }
        let todo: Vec<u32> = {
            let st = self.state();
            fresh
                .values()
                .filter(|s| matches!(s.reason, Reason::Load | Reason::Size))
                .map(|s| s.range)
                .filter(|r| !st.born_checked.contains(r) && !st.cooldown_until.contains_key(r))
                .collect()
        };
        for range in todo {
            let born = retry::deadline(
                &retry::ops::AUTOSPLIT_LEDGER_READ,
                READ_BOUND,
                cairn_store::ledger::op_of_child_written_ms(store, prefix, range),
            )
            .await;
            let now = Instant::now();
            let mut st = self.state();
            match born {
                Ok(Ok(written)) => {
                    st.born_checked.insert(range);
                    if let Some(ms) = written {
                        let age = age(ms).min(cfg.cooldown);
                        if age < cfg.cooldown {
                            st.cooldown_until.insert(range, now + (cfg.cooldown - age));
                        }
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    st.cooldown_until.insert(range, now + cfg.cooldown);
                }
            }
        }
    }

    /// `on` mode: splits range `range` at `at`. Runs in its own task — the
    /// collection loop does not stand waiting for a split of several seconds.
    async fn split_now(&self, ops: &crate::split::RangeOps, range: u32, at: u64) {
        // Give the flag back on every way out, panics included: a stuck flag
        // means automatic splitting is silently off until the next restart.
        struct InFlight<'a>(&'a AutoSplit);
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                self.0.state().in_flight = false;
            }
        }
        let _in_flight = InFlight(self);
        let p = cairn_store::PartitionId(range);
        let blocked = match ops.split_blocker(p, at).await {
            Ok(b) => b.map(|b| b.label().to_string()),
            Err(e) => Some(format!("busy: {e}")),
        };
        if let Some(why) = blocked {
            let mut st = self.state();
            st.retry.insert(range, (Instant::now() + RETRY_AFTER, why));
            return;
        }
        let parent = ops.cluster.table().range(p).copied();
        tracing::info!(range, at = %format!("{at:016x}"), "auto-splitting range");
        let r = ops.split_exclusive(p, at).await;
        let now = Instant::now();
        let mut st = self.state();
        match r {
            Ok((table, report)) => {
                AUTO_SPLITS.fetch_add(1, Ordering::Relaxed);
                st.last_split = Some(now);
                st.suggestions.remove(&range);
                let children: Vec<(u32, u64, u64)> = parent
                    .map(|par| {
                        table
                            .ranges()
                            .iter()
                            .filter(|r| r.start >= par.start && r.end <= par.end && r.id != p)
                            .map(|r| (r.id.0, r.start, r.end))
                            .collect()
                    })
                    .unwrap_or_default();
                for (c, _, _) in &children {
                    st.cooldown_until.insert(*c, now + self.0.cfg.cooldown);
                }
                drop(st);
                self.0.book.inherit(range, &children);
                tracing::info!(
                    range,
                    ?children,
                    window_ms = report.window_ms,
                    "auto-split range"
                );
            }
            Err(e) => {
                use crate::split::RangeOpError as E;
                let (why, wait) = match &e {
                    E::Busy(_) => ("busy", RETRY_AFTER),
                    E::Conflict(_) => ("conflict", Duration::from_secs(5)),
                    E::NotLeader { .. } => ("busy", Duration::from_secs(5)),
                    // The intent was written and then failed (rolled back): a
                    // write pause has already been spent — count it against
                    // the node's cap.
                    E::Failed(_) => {
                        AUTO_SPLITS_FAILED.fetch_add(1, Ordering::Relaxed);
                        st.last_split = Some(now);
                        ("failed", self.0.cfg.node_interval.max(RETRY_AFTER))
                    }
                    E::BadRequest(_) | E::NotFound(_) => ("failed", RETRY_AFTER),
                };
                tracing::warn!(range, error = %e, "auto-split did not finish — retrying later");
                st.retry.insert(range, (now + wait, why.to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_split_defaults_are_pinned() {
        let c = AutoSplitConfig::default();
        assert_eq!(c.mode, AutoSplitMode::Suggest, "default mode");
        assert_eq!(c.max_range_bytes, 8 << 30, "default size cap");
        assert_eq!((c.hot_share, c.hot_secs), (0.5, 60));
        assert_eq!(c.size_check_diff, 64 << 20);
        assert_eq!(c.size_check_every, Duration::from_secs(300));
        assert_eq!(c.cooldown, Duration::from_secs(600));
        assert_eq!(c.node_interval, Duration::from_secs(60));
        assert_eq!(c.max_ranges, 128, "load splits stop at 128 ranges");
        assert_eq!(
            c.merge_cold,
            Duration::from_secs(6 * 3600),
            "merge suggestion after 6 h cold"
        );
    }

    /// A suggestion for range `range` with reason `reason`, splittable at the midpoint.
    fn suggestion(range: u32, reason: Reason, share: f64) -> Suggestion {
        Suggestion {
            range,
            reason,
            at: Some(format!("{:016x}", u64::MAX / 2)),
            balance: None,
            blocked_by: None,
            owner: None,
            turn_share: share,
            hot_secs: 5,
            size_bytes: None,
            since_ms: 0,
            node: None,
            merge_with: None,
            cold_secs: None,
        }
    }

    /// **`on` mode stops splitting for load at `--auto-split-max-ranges`** (a 24 h soak
    /// with `hot_share` 0.005 grew from 8 to 528 ranges and saturated the box: nothing in
    /// the decider made splitting stop, and there is no automatic merge).
    ///
    /// 1. `on` mode, cap 4. One range hot for load, the table already has 4 ranges.
    /// 2. The same with 3 ranges in the table.
    /// 3. 4 ranges, but the range is over the size cap (reason `size`).
    ///
    /// Must see: (1) no split, `blocked_by: max_ranges`; (2) the split goes ahead; (3) the
    /// split goes ahead — the cap bounds splits for load, not for size.
    #[test]
    fn on_mode_blocks_load_splits_at_max_ranges() {
        let mut cfg = AutoSplitConfig::default();
        cfg.set("--auto-split", "on").expect("mode");
        cfg.set("--auto-split-max-ranges", "4")
            .expect("the cap flag");
        let around = |ranges| Around {
            now: Instant::now(),
            any_intent: false,
            ranges,
        };
        let round = |reason, ranges| {
            let mut st = DeciderState::default();
            let mut fresh = BTreeMap::from([(7, suggestion(7, reason, 0.9))]);
            let (act, _) = choose(&mut st, &mut fresh, &cfg, &around(ranges));
            (act, fresh[&7].blocked_by.clone())
        };
        let (act, why) = round(Reason::Load, 4);
        assert_eq!(act, None, "a load split at the cap");
        assert_eq!(why.as_deref(), Some("max_ranges"));
        let (act, why) = round(Reason::Load, 3);
        assert!(act.is_some(), "below the cap: {why:?}");
        let (act, why) = round(Reason::Size, 4);
        assert!(act.is_some(), "a size split at the cap: {why:?}");
    }

    fn facts(range: u32, cold_h: u64, size: u64, depth: u64) -> MergeFacts {
        MergeFacts {
            range,
            cold_ms: cold_h * 3_600_000,
            size_bytes: Some(size),
            external_depth: Some(depth),
            cooling: false,
            node: None,
        }
    }

    /// Four ranges `[0..4)` splitting the keyspace evenly, ids 1..=4 in key order.
    fn table4() -> cairn_store::PartitionTable {
        let q = u64::MAX / 4;
        cairn_store::PartitionTable::from_ranges(
            1,
            vec![
                (0, q, cairn_store::PartitionId(1)),
                (q + 1, 2 * q, cairn_store::PartitionId(2)),
                (2 * q + 1, 3 * q, cairn_store::PartitionId(3)),
                (3 * q + 1, u64::MAX, cairn_store::PartitionId(4)),
            ],
        )
        .expect("the table covers everything")
    }

    /// All four conditions met ⇒ suggest adjacent pairs, left to right, one
    /// pair per range; one condition missing ⇒ none.
    #[test]
    fn merge_rows_pair_adjacent_cold_small_ranges() {
        let cfg = AutoSplitConfig::default();
        let gib = 1u64 << 30;
        let t = table4();
        let now = 100 * 3_600_000;
        // All four cold, small: two pairs (1,2), (3,4) — not (2,3).
        let all: Vec<MergeFacts> = (1..=4).map(|r| facts(r, 7, gib / 4, 0)).collect();
        let rows = merge_rows(&cfg, &t, &all, now);
        let pairs: Vec<(u32, Option<u32>)> = rows.iter().map(|r| (r.range, r.merge_with)).collect();
        assert_eq!(pairs, vec![(1, Some(2)), (3, Some(4))]);
        let r = &rows[0];
        assert_eq!(r.reason, Reason::Merge);
        assert_eq!(r.size_bytes, Some(gib / 2));
        assert_eq!(r.cold_secs, Some(7 * 3600));
        // A full 6 h since 1 h ago.
        assert_eq!(r.since_ms, now - 3_600_000);
        assert_eq!(r.blocked_by, None);
        assert!(r.at.is_none());

        let with = |i: usize, f: MergeFacts| {
            let mut v = all.clone();
            v[i] = f;
            merge_rows(&cfg, &t, &v, now)
                .iter()
                .map(|r| (r.range, r.merge_with))
                .collect::<Vec<_>>()
        };
        // Range 1 cold for only 5 h: (2,3) instead of (1,2).
        assert_eq!(with(0, facts(1, 5, gib / 4, 0)), vec![(2, Some(3))]);
        // Total size ≥ ¼ of the cap (8 GiB ⇒ 2 GiB): no pair (1,2).
        assert_eq!(with(0, facts(1, 7, 2 * gib, 0)), vec![(2, Some(3))]);
        // Ancestor depth 2.
        assert_eq!(with(1, facts(2, 7, gib / 4, 2)), vec![(3, Some(4))]);
        // Depth 1 is still fine.
        assert_eq!(
            with(1, facts(2, 7, gib / 4, 1)),
            vec![(1, Some(2)), (3, Some(4))]
        );
        // Within a split cooldown.
        let mut cooling = facts(4, 7, gib / 4, 0);
        cooling.cooling = true;
        assert_eq!(with(3, cooling), vec![(1, Some(2))]);
        // Size not measured yet, or depth unknown: no suggestion.
        let mut unknown = facts(1, 7, 0, 0);
        unknown.size_bytes = None;
        assert_eq!(with(0, unknown), vec![(2, Some(3))]);
        let mut unknown = facts(1, 7, 0, 0);
        unknown.external_depth = None;
        assert_eq!(with(0, unknown), vec![(2, Some(3))]);
        // Facts missing for one range (the node leading it did not answer).
        assert_eq!(
            merge_rows(&cfg, &t, &all[..3], now)
                .iter()
                .map(|r| (r.range, r.merge_with))
                .collect::<Vec<_>>(),
            vec![(1, Some(2))]
        );
        // Off: `merge_cold` 0 or `off` mode.
        let off = AutoSplitConfig {
            merge_cold: Duration::ZERO,
            ..AutoSplitConfig::default()
        };
        assert!(merge_rows(&off, &t, &all, now).is_empty());
        let off = AutoSplitConfig {
            mode: AutoSplitMode::Off,
            ..AutoSplitConfig::default()
        };
        assert!(merge_rows(&off, &t, &all, now).is_empty());
    }

    /// A cold window: below ⅒ of the hot threshold.
    #[test]
    fn cold_window_is_a_tenth_of_hot() {
        let w = |turn_ms: f64| Window {
            at_ms: 0,
            len_ms: 1000.0,
            ops: 0,
            turn_ms,
            handler_ms: turn_ms,
            bytes: 0,
            samples: Vec::new(),
            sample_weight: 0.0,
        };
        // 10 turn slots × 1 s; hot threshold 0.5 ⇒ cold below 0.05 (500 ms of turns).
        assert!(is_cold(&w(0.0), 10, 0.5));
        assert!(is_cold(&w(499.0), 10, 0.5));
        assert!(!is_cold(&w(500.0), 10, 0.5));
    }

    #[test]
    fn reservoir_samples_in_proportion_to_turn_time() {
        // Owner 1: 10 turns × 90 ms; owner 2: 90 turns × 1 ms. By turn time,
        // owner 1 holds 900/990 ≈ 91 % — the reservoir must see that, even
        // though owner 2 has nine times as many turns.
        let mut hits = 0usize;
        let mut total = 0usize;
        for round in 0..50u32 {
            let mut r = Reservoir::new(round);
            for i in 0..100 {
                if i % 10 == 0 {
                    r.offer(1, 90_000.0);
                } else {
                    r.offer(2, 1_000.0);
                }
            }
            let s = r.take();
            assert!(s.len() <= RESERVOIR);
            hits += s.iter().filter(|o| **o == 1).count();
            total += s.len();
        }
        // Not repeatable (there are 10 turns of owner 1 for 64 slots, so the
        // cap is 10/64), but owner 1 must be present for nearly all 10 of its
        // turns.
        assert!(hits as f64 / total as f64 > 0.14, "{hits}/{total}");
    }

    #[test]
    fn top_owner_weighs_by_turn_time() {
        let s = [(5, 1.0), (5, 1.0), (7, 8.0)];
        let (o, share) = top_owner(&s).unwrap();
        assert_eq!(o, 7);
        assert!((share - 0.8).abs() < 1e-9);
        assert_eq!(top_owner(&[]), None);
    }

    #[test]
    fn weighted_median_splits_load_not_key_space() {
        // Load concentrated in the first quarter of [0, 2^63): the midpoint
        // (2^62) would put all the load on the left; the weighted median
        // splits the load in half.
        let q = 1u64 << 61;
        let samples: Vec<(u64, f64)> = (0..16).map(|i| (i * (q / 16), 1.0)).collect();
        let Some(KeyChoice::Split { at, balance }) = choose_key(&samples, 0, (1 << 63) - 1) else {
            panic!("must be splittable");
        };
        let left = samples.iter().filter(|(o, _)| *o < at).count();
        assert_eq!(left, 8, "key {at:x} splits {left}/16");
        assert!(balance < 1e-9);
        assert!(at < q, "the key must lie in the hot quarter");
    }

    #[test]
    fn one_heavy_owner_is_hot_owner() {
        let samples = [(10, 9.0), (20, 0.5), (30, 0.5)];
        assert_eq!(
            choose_key(&samples, 0, 100),
            Some(KeyChoice::HotOwner {
                owner: 10,
                share: 0.9
            })
        );
        assert!(matches!(
            choose_key(&[(5, 1.0)], 0, 100),
            Some(KeyChoice::HotOwner { owner: 5, .. })
        ));
        assert_eq!(choose_key(&[], 0, 100), None);
        // Samples outside the range do not count.
        assert_eq!(choose_key(&[(500, 1.0)], 0, 100), None);
    }

    #[test]
    fn split_key_stays_inside_the_range() {
        for (lo, hi) in [
            (0u64, 1u64),
            (7, 8),
            (u64::MAX - 1, u64::MAX),
            (0, u64::MAX),
        ] {
            let Some(KeyChoice::Split { at, .. }) = choose_key(&[(lo, 1.0), (hi, 1.0)], lo, hi)
            else {
                panic!("two balanced owners must be splittable");
            };
            assert!(lo < at && at <= hi, "{lo}..{hi} → {at}");
        }
        assert_eq!(midpoint(0, u64::MAX), 1 << 63);
    }

    fn window(turn_ms: f64) -> Window {
        Window {
            at_ms: 0,
            len_ms: 1000.0,
            ops: 0,
            turn_ms,
            handler_ms: turn_ms,
            bytes: 0,
            samples: Vec::new(),
            sample_weight: 0.0,
        }
    }

    #[test]
    fn one_cold_window_resets_the_streak() {
        // Cap of 4 turns ⇒ 4000 ms of slots per window; hot = from 2000 ms.
        let w: Vec<Window> = [2500.0, 2500.0, 100.0, 2000.0, 3000.0].map(window).into();
        assert_eq!(hot_streak(&w, 4, 0.5), 2);
        assert_eq!(hot_streak(&w[..2], 4, 0.5), 2);
        assert_eq!(hot_streak(&w[..3], 4, 0.5), 0);
    }

    /// **Engine and durability waits do not make a range hot** (24 h soak: as the node
    /// slowed, turns grew from 215 to 602 ms of wall time each — mostly waiting for the
    /// engine and for commits — so ranges looked hotter and split more, which slowed the
    /// node further).
    ///
    /// Two ranges with the same handler time per turn (100 ms × 10 turns); one turn's wall
    /// time is 100 ms, the other's 1 s.
    ///
    /// Must see: the same turn-slot share.
    #[test]
    fn engine_and_commit_waits_do_not_heat_a_range() {
        let book = LoadBook::default();
        let (busy, waiting) = (book.range(1), book.range(2));
        let ms = Duration::from_millis;
        for _ in 0..10 {
            busy.note_turn(PartitionKey(1), ms(100), ms(100));
            waiting.note_turn(PartitionKey(1), ms(1_000), ms(100));
        }
        book.rotate(0, Duration::from_secs(1));
        let (b, w) = (&busy.windows()[0], &waiting.windows()[0]);
        assert_eq!(
            turn_share(b, 8),
            turn_share(w, 8),
            "the waiting range looks hotter: {b:?} vs {w:?}"
        );
    }

    #[test]
    fn children_inherit_half_of_the_parent() {
        let book = LoadBook::default();
        let parent = book.range(1);
        parent.with_history(|h| {
            h.push(Window {
                ops: 10,
                turn_ms: 400.0,
                bytes: 100,
                samples: vec![5, 50, 500],
                sample_weight: 1.0,
                ..window(0.0)
            })
        });
        book.inherit(1, &[(2, 0, 99), (3, 100, 1000)]);
        let (l, r) = (book.range(2).windows(), book.range(3).windows());
        assert_eq!((l[0].ops, l[0].turn_ms, l[0].bytes), (5, 200.0, 50));
        assert_eq!(l[0].samples, vec![5, 50]);
        assert_eq!(r[0].samples, vec![500]);
    }

    #[test]
    fn flags_parse_and_reject_nonsense() {
        let mut c = AutoSplitConfig::default();
        c.set("--auto-split", "on").unwrap();
        c.set("--auto-split-hot-share", "0.25").unwrap();
        c.set("--auto-split-hot-secs", "5").unwrap();
        c.set("--max-range-bytes", "1024").unwrap();
        c.set("--auto-split-cooldown-ms", "1500").unwrap();
        assert_eq!(c.mode, AutoSplitMode::On);
        assert_eq!(
            (c.hot_share, c.hot_secs, c.max_range_bytes),
            (0.25, 5, 1024)
        );
        assert_eq!(c.cooldown, Duration::from_millis(1500));
        c.set("--auto-split-merge-cold-ms", "0").unwrap();
        assert_eq!(c.merge_cold, Duration::ZERO);
        assert!(AutoSplitConfig::owns("--auto-split-merge-cold-ms"));
        assert!(c.set("--auto-split", "maybe").is_err());
        assert!(c.set("--auto-split-hot-share", "0").is_err());
        assert!(c.set("--auto-split-hot-share", "1.5").is_err());
        assert!(c.set("--auto-split-hot-secs", "61").is_err());
        assert!(AutoSplitConfig::owns("--auto-split-hot-secs"));
        assert!(AutoSplitConfig::owns("--max-range-bytes"));
        assert!(!AutoSplitConfig::owns("--max-concurrent-turns"));
    }
}
