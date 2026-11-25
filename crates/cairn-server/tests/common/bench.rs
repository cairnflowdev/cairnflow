// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Shared tooling for the `#[ignore]` baseline benches (`recovery_storm.rs`,
//! `timer_precision.rs`, `concurrency.rs`): build many invocations quickly through
//! the real path, read engine hold time from `/metrics`, measure RSS.

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{InterPartition, StartCall},
    cairn_server::node::{NodeConfig, NodeMode, RunningNode},
    cairn_store::{testkit::FaultyStore, ObjectStores, StoreLocation},
    std::{collections::BTreeMap, sync::Arc, time::Duration},
};

/// The bench's levels, from the environment variable `name` (a comma-separated list).
pub fn levels(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| default.to_vec())
}

/// The `n`-th id of batch `tag` (partition key 0 — a one-range node holds every key).
pub fn id(tag: u8, n: usize) -> [u8; 24] {
    let mut r = [0u8; 16];
    r[0] = tag;
    r[8..].copy_from_slice(&(n as u64).to_be_bytes());
    cairn_store::make_invocation_id(cairn_store::PartitionKey(0), r)
}

pub fn hex(id: &[u8; 24]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// A `StartCall` message with no waiter, as another range's drainer sends it:
/// callee `id` runs `service.run`, in `scope` if given. Inbound rows are keyed by
/// `sender` (a resend overwrites exactly the old row), so each message has its own
/// `sender`: `(id(0xEE ‖ tag), seq)`.
pub fn start_call(
    id: [u8; 24],
    service: &str,
    scope: Option<&str>,
    tag: u8,
    seq: u32,
) -> InterPartition {
    let mut from = [0xEE; 16];
    from[1] = tag;
    InterPartition::StartCall(StartCall {
        callee: id,
        service: service.into(),
        handler: "run".into(),
        object_key: String::new(),
        parameter: Bytes::from_static(b"{}"),
        reply_to: None,
        idempotency_key: None,
        scope: scope.map(str::to_string),
        limit_key: None,
        tenant: None,
        sender: (
            cairn_store::make_invocation_id(cairn_store::PartitionKey(0), from),
            seq,
        ),
        schedule: None,
        lineage: Default::default(),
    })
}

/// Send `n` `StartCall` messages to `service` (ids `id(tag, 0..n)`) through the
/// internal port, in batches of 64 messages (one commit per batch on the receiving
/// side, like a real drainer), at most 8 batches in flight. A refused batch (`503`,
/// `429`…) is resent.
pub async fn send(
    node: &RunningNode,
    tag: u8,
    service: &str,
    scope: Option<&str>,
    n: usize,
) -> Result<()> {
    send_range(node, tag, service, scope, 0..n).await
}

/// Like [`send`], for ids `id(tag, ids)` — building history in waves.
pub async fn send_range(
    node: &RunningNode,
    tag: u8,
    service: &str,
    scope: Option<&str>,
    ids: std::ops::Range<usize>,
) -> Result<()> {
    use futures::StreamExt;
    let end = ids.end;
    let batches: Vec<std::ops::Range<usize>> =
        ids.step_by(64).map(|s| s..(s + 64).min(end)).collect();
    let mut sends = futures::stream::iter(batches)
        .map(|r| async move {
            let enc: Vec<Bytes> = r
                .map(|i| start_call(id(tag, i), service, scope, tag, i as u32).encode())
                .collect();
            let body = InterPartition::encode_batch(&enc).to_vec();
            loop {
                let resp =
                    super::post_internal(node, "/internal/inbound/batch", body.clone()).await?;
                if resp.status().is_success() {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .buffer_unordered(8);
    while let Some(r) = sends.next().await {
        r?;
    }
    Ok(())
}

/// Build `n` invocations `service.run` (ids `id(tag, 0..n)`) through the real path,
/// **in waves** of `wave`: a wave is only sent once the SDK has received every
/// `Start` of the previous wave and the wave's last id has status `want`. The backlog
/// never exceeds one wave, and the poll loop counts `Start`s with
/// [`FakeSdk::starts_count_of`] — without copying the `Start` list.
///
/// The old benches sent all `n` at once and then polled `starts_of(..).len()` every
/// 20–50 ms: each poll copied the whole list under the lock that the fake SDK's
/// handler needs to record a `Start`, so the build rate dropped with the number
/// already built.
pub async fn build(
    node: &RunningNode,
    sdk: &super::fake_sdk::FakeSdk,
    tag: u8,
    service: &str,
    n: usize,
    wave: usize,
    want: &str,
) -> Result<()> {
    let base = sdk.starts_count_of(service, "run");
    let wave = wave.max(1);
    let mut done = 0;
    while done < n {
        let end = (done + wave).min(n);
        send_range(node, tag, service, None, done..end).await?;
        let t = std::time::Instant::now();
        while sdk.starts_count_of(service, "run") < base + end {
            anyhow::ensure!(
                t.elapsed() < Duration::from_secs(1800),
                "wave {done}..{end} of {service}: not every Start after 1800 s"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        super::wait_status(&node.url(), &hex(&id(tag, end - 1)), want).await?;
        done = end;
    }
    Ok(())
}

/// The configuration of a one-range node on `store` (not `common::bucket()`: a bench
/// needs no `verify` on cleanup, and must count requests).
pub fn config(store: &Arc<FaultyStore>) -> NodeConfig {
    let stores = ObjectStores::single(StoreLocation::from_store(store.clone(), "", "memory://"));
    let mut cfg = NodeConfig::new(stores, NodeMode::single(1));
    cfg.tick_interval = Some(Duration::from_millis(50));
    cfg.flush_interval = Some(Duration::from_millis(5));
    cfg
}

/// The range's engine hold time (one-range node) per holder, from `/metrics`.
#[derive(Clone, Debug, Default)]
pub struct Holds {
    /// holder → (total ms, longest hold ms, number of holds).
    pub by: BTreeMap<String, (f64, f64, f64)>,
}

impl Holds {
    pub async fn read(url: &str) -> Result<Self> {
        let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
        // A one-range node: the line of the only range, any id will do.
        let pick = |family: &str| -> BTreeMap<String, f64> {
            let prefix = format!("{family}{{range=\"");
            text.lines()
                .filter_map(|l| {
                    let rest = l.strip_prefix(&prefix)?;
                    let (_, rest) = rest.split_once("\",holder=\"")?;
                    let (holder, v) = rest.split_once("\"} ")?;
                    Some((holder.to_string(), v.parse().ok()?))
                })
                .collect()
        };
        let max = pick("cairnflow_range_engine_hold_max_ms");
        let count = pick("cairnflow_range_engine_holds_total");
        Ok(Self {
            by: pick("cairnflow_range_engine_hold_ms_total")
                .into_iter()
                .map(|(h, t)| {
                    let m = max.get(&h).copied().unwrap_or(0.0);
                    let c = count.get(&h).copied().unwrap_or(0.0);
                    (h, (t, m, c))
                })
                .collect(),
        })
    }

    /// The longest hold over every holder, ms, and that holder.
    pub fn max(&self) -> (String, f64) {
        self.by
            .iter()
            .map(|(h, (_, m, _))| (h.clone(), *m))
            .fold((String::new(), 0.0), |a, b| if b.1 > a.1 { b } else { a })
    }

    /// The number of holds (every holder) from `earlier` until now.
    pub fn count_since(&self, earlier: &Self) -> f64 {
        let total = |h: &Self| h.by.values().map(|(_, _, c)| c).sum::<f64>();
        total(self) - total(earlier)
    }

    /// The number of holds per holder from `earlier` until now — `holder n` for every
    /// holder with holds in that interval (which holders account for the holds while
    /// the queue stands still).
    pub fn counts_since(&self, earlier: &Self) -> String {
        self.by
            .iter()
            .filter_map(|(h, (_, _, c))| {
                let before = earlier.by.get(h).map_or(0.0, |(_, _, c)| *c);
                (c - before > 0.0).then(|| format!("{h} {:.0}", c - before))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `holder total/longest ms` for every holder with a total > 0.
    pub fn summary(&self) -> String {
        self.by
            .iter()
            .filter(|(_, (t, _, _))| *t > 0.0)
            .map(|(h, (t, m, _))| format!("{h} {t:.0}/{m:.0}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The process's RSS, MiB (`ps -o rss`).
pub fn rss_mib() -> f64 {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<f64>()
                .ok()
        })
        .map(|kib| kib / 1024.0)
        .unwrap_or(0.0)
}

/// The process's CPU time (user + sys), seconds (`ps -o time`, in the form
/// `[[dd-]hh:]mm:ss.cc`).
pub fn cpu_secs() -> f64 {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &pid])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    let (days, rest) = match out.split_once('-') {
        Some((d, r)) => (d.parse::<f64>().unwrap_or(0.0), r.to_string()),
        None => (0.0, out),
    };
    days * 86_400.0
        + rest.split(':').fold(0.0, |acc, part| {
            acc * 60.0 + part.parse::<f64>().unwrap_or(0.0)
        })
}

/// One per-range metric line (one-range node), the sum over all ranges if several.
pub fn range_metric(text: &str, family: &str) -> f64 {
    let prefix = format!("{family}{{range=\"");
    text.lines()
        .filter_map(|l| l.strip_prefix(&prefix))
        .filter_map(|rest| rest.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum()
}

/// A snapshot of one range's cost (one-range node) to compare two moments: engine
/// hold time per holder, store requests, SST blocks read (hits + misses of the RAM
/// block cache — a measure of **read volume**, independent of wall time), CPU time.
pub struct Probe {
    pub holds: Holds,
    pub store: cairn_store::testkit::StoreCounts,
    pub blocks: f64,
    pub l0_ssts: f64,
    pub sorted_runs: f64,
    /// Seconds `db.write` waited ≥ 10 ms (SlateDB backpressure) — waiting **inside**
    /// the writer's engine hold.
    pub write_stall_s: f64,
    /// Times SlateDB's flusher stalled because L0 was full.
    pub l0_stalls: f64,
    pub cpu_s: f64,
    pub at: std::time::Instant,
}

impl Probe {
    pub async fn take(url: &str, store: &FaultyStore) -> Result<Self> {
        let text = reqwest::get(format!("{url}/metrics")).await?.text().await?;
        Ok(Self {
            holds: Holds::read(url).await?,
            store: store.counts(),
            blocks: range_metric(&text, "cairnflow_range_block_cache_hits_total")
                + range_metric(&text, "cairnflow_range_block_cache_misses_total"),
            l0_ssts: range_metric(&text, "cairnflow_range_slatedb_l0_ssts"),
            sorted_runs: range_metric(&text, "cairnflow_range_slatedb_sorted_runs"),
            write_stall_s: range_metric(&text, "cairnflow_range_write_stall_seconds_total"),
            l0_stalls: range_metric(&text, "cairnflow_range_slatedb_l0_stalls_total"),
            cpu_s: cpu_secs(),
            at: std::time::Instant::now(),
        })
    }
}

/// Sample RSS every 200 ms until `stop` changes; returns the peak, MiB.
pub fn rss_peak(stop: tokio::sync::watch::Receiver<bool>) -> tokio::task::JoinHandle<f64> {
    tokio::spawn(async move {
        let mut peak = rss_mib();
        while !*stop.borrow() {
            peak = peak.max(rss_mib());
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        peak.max(rss_mib())
    })
}
