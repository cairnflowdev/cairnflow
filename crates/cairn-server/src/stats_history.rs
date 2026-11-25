// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Durable per-handler statistics**: the 24 h – 30 day charts that the in-RAM summary
//! (12 windows of 5 minutes, [`crate::stats`]) cannot hold.
//!
//! Each node flushes its own numbers into the object `stats/<tenant>/<yyyy-mm-dd>/<node>.<boot>`
//! — the `metering` pattern: **accumulated over the day, overwritten**, one object per
//! `(tenant, day, process)`, no process writes another process's object (no CAS). The
//! content is an **hourly** series: one line per `(hour, service, handler, deployment)`,
//! with a duration histogram (mergeable across nodes, quantiles computed at read time).
//!
//! 5-minute windows that are fully closed (one more window has passed) are **absorbed**
//! into the day's settled part; windows still open are recomputed on every flush. So the
//! object is always the process's total up to the latest flush, including the running hour.
//!
//! Reading ([`read`], `GET /stats/history`): merges the objects of every node over the
//! latest `days` days (limit [`MAX_DAYS`]). The route reads through a [`HistoryCache`]:
//! closed days are not LISTed again, open days are reused for 30 s — a warm console's
//! 30-day question costs only 2 LISTs.
//!
//! **Limits to state plainly:** a dead node (`kill -9`) loses its unflushed part (at most
//! one flush tick); a turn spanning the boundary of an absorbed window only has its late
//! part in RAM. Expired objects are **not** deleted yet (that needs a new trash subject).

use {
    crate::stats::{Entry, HandlerKey, HandlerStats, WINDOW_MS},
    serde::{Deserialize, Serialize},
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload},
    std::{collections::BTreeMap, sync::Arc},
};

/// Maximum number of days read.
pub const MAX_DAYS: u64 = 30;
const DAY_MS: u64 = 86_400_000;
const HOUR_MS: u64 = 3_600_000;

/// One stored line: the numbers of one `(hour, handler)` of one process.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HourRow {
    pub hour_ms: u64,
    pub service: String,
    pub handler: String,
    pub deployment: String,
    pub turns: u64,
    pub completed: u64,
    pub failed: u64,
    pub paused: u64,
    pub failed_attempts: u64,
    pub suspensions: u64,
    pub steps: u64,
    pub run_us: u64,
    pub cu: u64,
    /// Turn duration histogram buckets ([`crate::stats::Durations`]).
    pub counts: Vec<u32>,
    pub max_us: u64,
}

impl HourRow {
    fn add_entry(&mut self, e: &Entry) {
        self.turns += e.turns;
        self.completed += e.completed;
        self.failed += e.failed;
        self.paused += e.paused;
        self.failed_attempts += e.failed_attempts;
        self.suspensions += e.suspensions;
        self.steps += e.steps;
        self.run_us += e.run_us;
        self.cu += e.cu;
        let (counts, max_us) = e.durations.parts();
        self.add_hist(counts, max_us);
    }

    fn add(&mut self, o: &HourRow) {
        self.turns += o.turns;
        self.completed += o.completed;
        self.failed += o.failed;
        self.paused += o.paused;
        self.failed_attempts += o.failed_attempts;
        self.suspensions += o.suspensions;
        self.steps += o.steps;
        self.run_us += o.run_us;
        self.cu += o.cu;
        self.add_hist(&o.counts, o.max_us);
    }

    fn add_hist(&mut self, counts: &[u32], max_us: u64) {
        if self.counts.len() < counts.len() {
            self.counts.resize(counts.len(), 0);
        }
        for (a, b) in self.counts.iter_mut().zip(counts) {
            *a = a.saturating_add(*b);
        }
        self.max_us = self.max_us.max(max_us);
    }
}

#[derive(Serialize, Deserialize)]
struct DayObject {
    v: u32,
    rows: Vec<HourRow>,
}

type RowKey = (u64, String, String, String);
type DayRows = BTreeMap<RowKey, HourRow>;

fn day_of(ms: u64) -> String {
    cairn_store::archive::utc_date(ms)
}

fn add_window(
    into: &mut BTreeMap<(String, String), DayRows>,
    w: u64,
    rows: &[(HandlerKey, Entry)],
) {
    let hour = w - w % HOUR_MS;
    for (k, e) in rows {
        let day = into.entry((k.tenant.clone(), day_of(w))).or_default();
        let r = day
            .entry((
                hour,
                k.service.clone(),
                k.handler.clone(),
                k.deployment.clone(),
            ))
            .or_insert_with(|| HourRow {
                hour_ms: hour,
                service: k.service.clone(),
                handler: k.handler.clone(),
                deployment: k.deployment.clone(),
                ..HourRow::default()
            });
        r.add_entry(e);
    }
}

#[derive(Default)]
struct State {
    /// Windows `< absorbed_upto` are in `closed`.
    absorbed_upto: u64,
    closed: BTreeMap<(String, String), DayRows>,
    /// The body last written for each `(tenant, day)` — unchanged means no PUT.
    written: BTreeMap<(String, String), Vec<u8>>,
}

/// The flusher of one process. `Clone` is cheap.
#[derive(Clone)]
pub struct History {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    node: String,
    stats: HandlerStats,
    state: Arc<tokio::sync::Mutex<State>>,
}

fn dir(prefix: &Path, tenant: &str, day: &str) -> Path {
    let p = prefix.as_ref();
    Path::from(if p.is_empty() {
        format!("stats/{tenant}/{day}")
    } else {
        format!("{p}/stats/{tenant}/{day}")
    })
}

impl History {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        node: String,
        stats: HandlerStats,
    ) -> Self {
        Self {
            store,
            prefix,
            node,
            stats,
            state: Arc::default(),
        }
    }

    pub async fn flush(&self) {
        self.flush_at(cairn_store::entropy::now_ms()).await
    }

    /// One flush at wall-clock time `now` (tests push the clock forward).
    pub async fn flush_at(&self, now: u64) {
        let windows = self.stats.windows_snapshot();
        let mut st = self.state.lock().await;
        // Absorb the fully closed windows (one more window has passed: the late numbers of
        // a turn spanning the boundary have been collected).
        let mut upto = st.absorbed_upto;
        for (w, rows) in &windows {
            if *w >= st.absorbed_upto && w + 2 * WINDOW_MS <= now {
                add_window(&mut st.closed, *w, rows);
                upto = upto.max(w + WINDOW_MS);
            }
        }
        st.absorbed_upto = upto;
        let mut all = st.closed.clone();
        for (w, rows) in &windows {
            if *w >= st.absorbed_upto {
                add_window(&mut all, *w, rows);
            }
        }
        // Days before yesterday are never written again — even when the flush tick is stuck
        // for long and an old window is only absorbed now: readers cache closed days
        // relying on that ([`HistoryCache`]); that late part only stays in RAM.
        let keep = day_of(now.saturating_sub(DAY_MS));
        for ((tenant, day), rows) in all {
            if day.as_str() < keep.as_str() {
                continue;
            }
            let body = serde_json::to_vec(&DayObject {
                v: 1,
                rows: rows.into_values().collect(),
            })
            .unwrap_or_default();
            let key = (tenant.clone(), day.clone());
            if st.written.get(&key) == Some(&body) {
                continue;
            }
            let path = dir(&self.prefix, &tenant, &day).join(format!(
                "{}.{}",
                crate::metering::path_segment(&self.node),
                crate::metering::boot_id()
            ));
            match self.store.put(&path, PutPayload::from(body.clone())).await {
                Ok(_) => {
                    st.written.insert(key, body);
                }
                Err(e) => {
                    tracing::debug!(%tenant, %day, error = %e, "durable statistics flush failed, retrying next tick")
                }
            }
        }
        // Keep two days in RAM (yesterday still receives the day's last window).
        st.closed.retain(|(_, d), _| d.as_str() >= keep.as_str());
        st.written.retain(|(_, d), _| d.as_str() >= keep.as_str());
    }
}

/// One row of `GET /stats/history`: one `(hour, handler)`, merged over every node.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRow {
    pub hour_ms: u64,
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub deployment: String,
    pub turns: u64,
    pub completed: u64,
    pub failed: u64,
    pub paused: u64,
    pub failed_attempts: u64,
    pub suspensions: u64,
    pub steps: u64,
    pub run_ms: u64,
    pub cu: u64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

/// Clock skew margin between reader and writer when treating a day as closed.
pub const HISTORY_SKEW_MS: u64 = HOUR_MS;
/// Time limit for reusing open days (today, yesterday): the Usage screen's two
/// questions, many tabs, many people of one tenant share one round of LISTs. It is really
/// `min(this limit, half the flush tick)` ([`HistoryCache::new`]) — new numbers are delayed
/// by no more than half a flush tick.
pub const HISTORY_FRESH_MS: u64 = 30_000;
/// Closed days are kept at most this long: a tenant purged and then created again under the
/// same name, on a node that never saw the `deleting` state, does not keep old numbers past an hour.
pub const HISTORY_CLOSED_TTL_MS: u64 = HOUR_MS;
/// (Estimated) byte limit of the cache per node; past it, the least recently used day is dropped.
pub const HISTORY_CACHE_BYTES: usize = 16 << 20;

/// Whether the day starting at `start` (ms, start of the UTC day) is fully closed by clock `now`.
///
/// The flusher only writes days `≥ yesterday` by its own clock ([`History::flush_at`]).
/// The writer's clock behind the reader's by no more than [`HISTORY_SKEW_MS`] ⇒ with
/// `start + 2 days + skew ≤ now`, the writer's "yesterday" is already after this day.
fn closed(start: u64, now: u64) -> bool {
    start + 2 * DAY_MS + HISTORY_SKEW_MS <= now
}

struct Cached {
    rows: Arc<DayRows>,
    at_ms: u64,
    /// Taken when the day was closed — only this copy is used as a closed day.
    closed: bool,
    bytes: usize,
    used: u64,
}

#[derive(Default)]
struct CacheInner {
    days: BTreeMap<(String, String), Cached>,
    bytes: usize,
    tick: u64,
}

/// The read cache of `GET /stats/history`, one per node.
///
/// - **Closed** days ([`closed`]) do not change on the bucket: kept until
///   [`HISTORY_CLOSED_TTL_MS`], no LIST/GET again.
/// - **Open** days are kept for [`HISTORY_FRESH_MS`].
///
/// Only a copy of the bucket: dropping it at any time is correct, it only costs requests.
/// For a `deleting`/`purged` tenant the caller calls [`Self::forget`] and reads directly.
#[derive(Clone)]
pub struct HistoryCache {
    inner: Arc<std::sync::Mutex<CacheInner>>,
    fresh_ms: u64,
}

impl Default for HistoryCache {
    fn default() -> Self {
        Self::new(std::time::Duration::from_secs(60))
    }
}

impl HistoryCache {
    /// A cache for a node flushing every `flush`: open days are reused for at most
    /// `min(HISTORY_FRESH_MS, flush / 2)`.
    pub fn new(flush: std::time::Duration) -> Self {
        Self {
            inner: Arc::default(),
            fresh_ms: (flush.as_millis() as u64 / 2).min(HISTORY_FRESH_MS),
        }
    }

    /// Drop every day of `tenant`.
    pub fn forget(&self, tenant: &str) {
        if let Ok(mut c) = self.inner.lock() {
            let gone: Vec<_> = c
                .days
                .keys()
                .filter(|(t, _)| t == tenant)
                .cloned()
                .collect();
            for k in gone {
                if let Some(e) = c.days.remove(&k) {
                    c.bytes -= e.bytes;
                }
            }
        }
    }

    fn get(&self, tenant: &str, day: &str, closed: bool, now: u64) -> Option<Arc<DayRows>> {
        let mut c = self.inner.lock().ok()?;
        c.tick += 1;
        let tick = c.tick;
        let e = c.days.get_mut(&(tenant.to_string(), day.to_string()))?;
        let age = now.checked_sub(e.at_ms)?;
        let fresh = if closed {
            e.closed && age < HISTORY_CLOSED_TTL_MS
        } else {
            age < self.fresh_ms
        };
        if !fresh {
            return None;
        }
        e.used = tick;
        Some(Arc::clone(&e.rows))
    }

    fn put(&self, tenant: &str, day: &str, rows: Arc<DayRows>, closed: bool, now: u64) {
        let bytes = rows
            .values()
            .map(|r| {
                96 + r.service.len() + r.handler.len() + r.deployment.len() + 4 * r.counts.len()
            })
            .sum::<usize>()
            + 64;
        if bytes > HISTORY_CACHE_BYTES {
            return;
        }
        let Ok(mut c) = self.inner.lock() else {
            return;
        };
        c.tick += 1;
        let used = c.tick;
        let key = (tenant.to_string(), day.to_string());
        if let Some(old) = c.days.remove(&key) {
            c.bytes -= old.bytes;
        }
        while c.bytes + bytes > HISTORY_CACHE_BYTES {
            let Some(lru) = c
                .days
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = c.days.remove(&lru) {
                c.bytes -= e.bytes;
            }
        }
        c.bytes += bytes;
        c.days.insert(
            key,
            Cached {
                rows,
                at_ms: now,
                closed,
                bytes,
                used,
            },
        );
    }
}

/// Like [`read`], through `cache` ([`HistoryCache`]).
pub async fn read_cached(
    cache: &HistoryCache,
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    days: u64,
    now: u64,
) -> Result<Vec<HistoryRow>, String> {
    read_with(Some(cache), store, prefix, tenant, days, now).await
}

/// The hourly series of `tenant` over the latest `days` days (up to `now`),
/// merging every object of every node. Corrupt objects are skipped (lenient read).
pub async fn read(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    days: u64,
    now: u64,
) -> Result<Vec<HistoryRow>, String> {
    read_with(None, store, prefix, tenant, days, now).await
}

/// Every object of one day, merged by `(hour, service, handler, deployment)`.
async fn read_day(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    day: &str,
) -> Result<DayRows, String> {
    use futures::TryStreamExt;
    let listed: Vec<_> = store
        .list(Some(&dir(prefix, tenant, day)))
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    let mut merged: DayRows = BTreeMap::new();
    for m in listed {
        let Ok(got) = store.get(&m.location).await else {
            continue;
        };
        let Ok(raw) = got.bytes().await else {
            continue;
        };
        let Ok(obj) = serde_json::from_slice::<DayObject>(&raw) else {
            continue;
        };
        for r in &obj.rows {
            add_row(&mut merged, r);
        }
    }
    Ok(merged)
}

fn add_row(into: &mut DayRows, r: &HourRow) {
    let k = (
        r.hour_ms,
        r.service.clone(),
        r.handler.clone(),
        r.deployment.clone(),
    );
    into.entry(k)
        .or_insert_with(|| HourRow {
            hour_ms: r.hour_ms,
            service: r.service.clone(),
            handler: r.handler.clone(),
            deployment: r.deployment.clone(),
            ..HourRow::default()
        })
        .add(r);
}

async fn read_with(
    cache: Option<&HistoryCache>,
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    days: u64,
    now: u64,
) -> Result<Vec<HistoryRow>, String> {
    let mut merged: DayRows = BTreeMap::new();
    for d in 0..days.clamp(1, MAX_DAYS) {
        let at = now.saturating_sub(d * DAY_MS);
        let day = day_of(at);
        let is_closed = closed(at - at % DAY_MS, now);
        let rows = match cache.and_then(|c| c.get(tenant, &day, is_closed, now)) {
            Some(rows) => rows,
            None => {
                let rows = Arc::new(read_day(store, prefix, tenant, &day).await?);
                if let Some(c) = cache {
                    c.put(tenant, &day, Arc::clone(&rows), is_closed, now);
                }
                rows
            }
        };
        for r in rows.values() {
            add_row(&mut merged, r);
        }
    }
    Ok(merged
        .into_values()
        .map(|r| {
            let d = crate::stats::Durations::from_parts(r.counts.clone(), r.max_us);
            let ms = |us: u64| us as f64 / 1000.0;
            HistoryRow {
                hour_ms: r.hour_ms,
                tenant: tenant.to_string(),
                service: r.service,
                handler: r.handler,
                deployment: r.deployment,
                turns: r.turns,
                completed: r.completed,
                failed: r.failed,
                paused: r.paused,
                failed_attempts: r.failed_attempts,
                suspensions: r.suspensions,
                steps: r.steps,
                run_ms: r.run_us / 1000,
                cu: r.cu,
                p50_ms: ms(d.quantile_us(0.5)),
                p99_ms: ms(d.quantile_us(0.99)),
                max_ms: ms(d.max_us()),
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::memory::InMemory, std::time::Duration};

    fn key(tenant: &str, h: &str) -> HandlerKey {
        HandlerKey {
            tenant: tenant.into(),
            service: "S".into(),
            handler: h.into(),
            deployment: "d".into(),
        }
    }

    /// **Two nodes, one hour, merged at read time**: each node flushes its own
    /// object; the read merges numbers and histograms; a fully closed window is still there
    /// after RAM drops it; flushing again with no change does not PUT; other tenants do not mix in.
    #[tokio::test]
    async fn history_merges_nodes_and_survives_ram() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("p");
        let now = cairn_store::entropy::now_ms();
        let mut nodes = Vec::new();
        for n in ["a", "b"] {
            let stats = HandlerStats::default();
            let shard = stats.shard();
            for _ in 0..3 {
                shard.turn(&key("acme", "h"), Duration::from_millis(10), 2);
            }
            shard.turn(&key("zeta", "h"), Duration::from_millis(10), 1);
            stats.drain();
            let h = History::new(store.clone(), prefix.clone(), n.into(), stats);
            h.flush_at(now).await;
            nodes.push(h);
        }
        let rows = read(&store, &prefix, "acme", 1, now).await.unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!((rows[0].turns, rows[0].steps), (6, 12));
        assert!(
            rows[0].p50_ms >= 8.0 && rows[0].p50_ms <= 12.5,
            "{}",
            rows[0].p50_ms
        );
        // Thirty minutes later: the fully closed window is absorbed; flushing again keeps the numbers.
        for h in &nodes {
            h.flush_at(now + 6 * WINDOW_MS).await;
        }
        // Two days: `now` + 30 minutes may already be in a new UTC day, where `now`'s
        // hour is no longer within a one-day window.
        let again = read(&store, &prefix, "acme", 2, now + 6 * WINDOW_MS)
            .await
            .unwrap();
        assert_eq!(again.iter().map(|r| r.turns).sum::<u64>(), 6);
        let zeta = read(&store, &prefix, "zeta", 1, now).await.unwrap();
        assert_eq!(zeta.iter().map(|r| r.turns).sum::<u64>(), 2);
    }

    use cairn_store::testkit::{FaultyStore, Op, PathClass};

    fn ops(store: &FaultyStore, op: Op) -> u64 {
        let c = store.counts();
        PathClass::ALL.iter().map(|&k| c.get(op, k)).sum()
    }

    /// The middle of today's UTC day — `now + 31 s` does not cross the day boundary.
    fn mid_day() -> u64 {
        let now = cairn_store::entropy::now_ms();
        now - now % DAY_MS + DAY_MS / 2
    }

    /// One object of day `day` of process `node` with one line of `turns` turns.
    async fn seed_day(
        store: &Arc<dyn ObjectStore>,
        prefix: &Path,
        tenant: &str,
        day_ms: u64,
        node: &str,
    ) {
        let hour = day_ms - day_ms % DAY_MS;
        let body = serde_json::to_vec(&DayObject {
            v: 1,
            rows: vec![HourRow {
                hour_ms: hour,
                service: "S".into(),
                handler: "h".into(),
                deployment: "d".into(),
                turns: 1,
                ..HourRow::default()
            }],
        })
        .unwrap();
        let path = dir(prefix, tenant, &day_of(day_ms)).join(format!("{node}.0"));
        store.put(&path, PutPayload::from(body)).await.unwrap();
    }

    /// **The flusher never writes a day before yesterday** (what [`HistoryCache`] relies
    /// on to cache closed days):
    ///
    /// 1. A process has turns at `now` but its flush tick is stuck.
    /// 2. The flush tick runs again two days plus the clock skew margin later — only then
    ///    is `now`'s window absorbed.
    ///
    /// Must see: no PUT at all (`now`'s day is closed for every reader).
    #[tokio::test]
    async fn closed_days_are_never_rewritten() {
        let counted = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = counted.clone();
        let now = cairn_store::entropy::now_ms();
        let stats = HandlerStats::default();
        stats
            .shard()
            .turn(&key("acme", "h"), Duration::from_millis(10), 1);
        stats.drain();
        let h = History::new(store, Path::from("p"), "a".into(), stats);
        h.flush_at(now + 2 * DAY_MS + HOUR_MS).await;
        assert_eq!(ops(&counted, Op::Put), 0, "rewrote a closed day");
    }

    /// **The 30-day question only LISTs open days, and merges nearby asks**:
    ///
    /// 1. The bucket has one object for each of 30 days.
    /// 2. Ask 10 times at once, then once 31 s later, then once 2 h later.
    ///
    /// Must see: the first ask 30 LISTs; the next nine 0 (open days still fresh); after
    /// 31 s exactly 2 LISTs (today, yesterday); the numbers returned always as a direct read.
    #[tokio::test]
    async fn cache_lists_open_days_only() {
        let counted = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = counted.clone();
        let prefix = Path::from("p");
        let now = mid_day();
        for d in 0..MAX_DAYS {
            seed_day(&store, &prefix, "acme", now - d * DAY_MS, "a").await;
        }
        let direct = read(&store, &prefix, "acme", MAX_DAYS, now).await.unwrap();
        assert_eq!(direct.len(), MAX_DAYS as usize);
        let cache = HistoryCache::default();
        let base = ops(&counted, Op::List);
        let first = read_cached(&cache, &store, &prefix, "acme", MAX_DAYS, now)
            .await
            .unwrap();
        assert_eq!(first, direct);
        assert_eq!(ops(&counted, Op::List) - base, MAX_DAYS);
        for _ in 0..9 {
            let again = read_cached(&cache, &store, &prefix, "acme", MAX_DAYS, now)
                .await
                .unwrap();
            assert_eq!(again, direct);
        }
        assert_eq!(
            ops(&counted, Op::List) - base,
            MAX_DAYS,
            "asks within 30 s must reuse"
        );
        let later = now + 31_000;
        let before = ops(&counted, Op::List);
        let fresh = read_cached(&cache, &store, &prefix, "acme", MAX_DAYS, later)
            .await
            .unwrap();
        assert_eq!(
            ops(&counted, Op::List) - before,
            2,
            "only today and yesterday"
        );
        assert_eq!(
            fresh,
            read(&store, &prefix, "acme", MAX_DAYS, later)
                .await
                .unwrap()
        );
    }

    /// **`forget` drops closed days too** (tenant purge): the tenant's objects are
    /// deleted, `forget`, ask again ⇒ empty.
    #[tokio::test]
    async fn forget_drops_closed_days() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("p");
        let now = mid_day();
        for d in 0..5 {
            seed_day(&store, &prefix, "acme", now - d * DAY_MS, "a").await;
        }
        let cache = HistoryCache::default();
        let rows = read_cached(&cache, &store, &prefix, "acme", 5, now)
            .await
            .unwrap();
        assert_eq!(rows.len(), 5);
        use futures::TryStreamExt;
        let all: Vec<_> = store
            .list(Some(&Path::from("p/stats/acme")))
            .try_collect()
            .await
            .unwrap();
        for m in all {
            store.delete(&m.location).await.unwrap();
        }
        cache.forget("acme");
        let rows = read_cached(&cache, &store, &prefix, "acme", 5, now)
            .await
            .unwrap();
        assert!(rows.is_empty(), "{rows:?}");
    }
}
