// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Slow log — inspired by TiDB's slow query log: a threshold, per-group thresholds,
//! and a **per-second limit** — the slow log must not become the thing that slows
//! things down.
//!
//! A run (`turn::drive`: from asking for a slot until releasing it) longer than the
//! tenant's threshold leaves **one** log line, target `cairnflow::slow_log`, level
//! `warn`, with structured fields: total time, SDK turn count, durable step count,
//! the count and time of durability waits, engine waits, slot waits, SDK waits. With
//! `CAIRNFLOW_LOG=json` it is one JSON line.
//!
//! The node's default threshold is `--slow-log-ms` (5 s); a tenant sets its own with
//! `PUT /admin/tenants/{tenant} {"slowMs": n}` (the `tenants` object, read leniently).
//! Limit of [`LINES_PER_SECOND`] lines per second per node; beyond it they are counted in
//! `cairnflow_slow_log_dropped_total` instead of printed.
//!
//! Printed lines also go into a **ring** of [`RING_MAX`] lines per node:
//! `GET /admin/slow-runs` and the `sys_slow_run` table read it — the slow log can be
//! queried without a log system. The ring only takes lines that passed the per-second
//! limit, so it is also limited to 10 lines/s.
//!
//! Hot path: one atomic read (the lowest threshold of every tenant). Only a run that
//! exceeds it looks up the threshold map.

use cairn_store::entropy::HashMap;
use {
    crate::stats::{HandlerKey, RunOutcome, TurnTrace},
    cairn_store::InvocationId,
    std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        time::Duration,
    },
};

/// The node's default threshold.
pub const DEFAULT_THRESHOLD: Duration = Duration::from_secs(5);
/// Limit on slow log lines per second per node.
pub const LINES_PER_SECOND: u32 = 10;
/// Number of recent slow runs each node keeps for queries.
pub const RING_MAX: usize = 1_000;

/// One slow run, as its log line.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlowRun {
    pub at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub deployment: String,
    pub invocation: String,
    pub outcome: String,
    pub total_ms: u64,
    pub threshold_ms: u64,
    pub turns: u64,
    pub steps: u64,
    pub run_ms: u64,
    pub sdk_ms: u64,
    pub durable_waits: u64,
    pub durable_wait_ms: u64,
    pub engine_wait_ms: u64,
    pub permit_wait_ms: u64,
}

/// Lines printed, process-wide.
pub static LOGGED: AtomicU64 = AtomicU64::new(0);
/// Lines dropped for exceeding the per-second limit, process-wide.
pub static DROPPED: AtomicU64 = AtomicU64::new(0);

struct Inner {
    default_ms: AtomicU64,
    /// Per-tenant thresholds, ms.
    overrides: RwLock<HashMap<String, u64>>,
    /// `min(default, every override)` — a shorter run skips the map lookup.
    min_ms: AtomicU64,
    /// `(Unix second, lines printed within that second)`.
    window: Mutex<(u64, u32)>,
    /// The [`RING_MAX`] most recent slow runs, oldest first.
    ring: Mutex<std::collections::VecDeque<SlowRun>>,
}

/// Threshold + limit of the slow log on one node. `Clone` is cheap.
#[derive(Clone)]
pub struct SlowLog(Arc<Inner>);

impl Default for SlowLog {
    fn default() -> Self {
        let ms = DEFAULT_THRESHOLD.as_millis() as u64;
        Self(Arc::new(Inner {
            default_ms: AtomicU64::new(ms),
            overrides: RwLock::new(HashMap::default()),
            min_ms: AtomicU64::new(ms),
            window: Mutex::new((0, 0)),
            ring: Mutex::default(),
        }))
    }
}

impl SlowLog {
    fn recompute(&self) {
        let d = self.0.default_ms.load(Ordering::Relaxed);
        let m = self
            .0
            .overrides
            .read()
            .map(|o| o.values().copied().fold(d, u64::min))
            .unwrap_or(d);
        self.0.min_ms.store(m, Ordering::Relaxed);
    }

    /// The node's default threshold.
    pub fn set_default(&self, d: Duration) {
        self.0
            .default_ms
            .store(d.as_millis() as u64, Ordering::Relaxed);
        self.recompute();
    }

    /// Replace all per-tenant thresholds (re-read from the `tenants` object).
    pub fn set_overrides(&self, o: HashMap<String, u64>) {
        if let Ok(mut cur) = self.0.overrides.write() {
            *cur = o;
        }
        self.recompute();
    }

    pub fn threshold(&self, tenant: &str) -> Duration {
        let ms = self
            .0
            .overrides
            .read()
            .ok()
            .and_then(|o| o.get(tenant).copied())
            .unwrap_or_else(|| self.0.default_ms.load(Ordering::Relaxed));
        Duration::from_millis(ms)
    }

    /// Slow runs still in the ring, newest first; filtered by `tenant`.
    pub fn recent(&self, tenant: Option<&str>) -> Vec<SlowRun> {
        let Ok(ring) = self.0.ring.lock() else {
            return Vec::new();
        };
        ring.iter()
            .rev()
            .filter(|r| tenant.is_none_or(|t| r.tenant == t))
            .cloned()
            .collect()
    }

    fn remember(&self, run: SlowRun) {
        if let Ok(mut ring) = self.0.ring.lock() {
            if ring.len() >= RING_MAX {
                ring.pop_front();
            }
            ring.push_back(run);
        }
    }

    /// Whether second `sec` still has a print slot — and use it up if so.
    fn admit(&self, sec: u64) -> bool {
        let Ok(mut w) = self.0.window.lock() else {
            return false;
        };
        if w.0 != sec {
            *w = (sec, 0);
        }
        if w.1 < LINES_PER_SECOND {
            w.1 += 1;
            true
        } else {
            false
        }
    }

    /// Consider a run that just finished: above the threshold, print it (or count it as dropped).
    /// `true` = printed.
    pub fn consider(
        &self,
        trace: &TurnTrace,
        key: &HandlerKey,
        id: &InvocationId,
        outcome: Option<RunOutcome>,
    ) -> bool {
        let total = trace.elapsed();
        if (total.as_millis() as u64) < self.0.min_ms.load(Ordering::Relaxed) {
            return false;
        }
        let threshold = self.threshold(&key.tenant);
        if total < threshold {
            return false;
        }
        let now_ms = cairn_store::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let sec = now_ms / 1000;
        if !self.admit(sec) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        LOGGED.fetch_add(1, Ordering::Relaxed);
        let ms = |d: Duration| d.as_millis() as u64;
        tracing::warn!(
            target: "cairnflow::slow_log",
            tenant = %key.tenant,
            service = %key.service,
            handler = %key.handler,
            deployment = %key.deployment,
            invocation = %cairn_store::id_hex(id),
            outcome = outcome.map(RunOutcome::as_str).unwrap_or("error"),
            total_ms = ms(total),
            threshold_ms = ms(threshold),
            turns = trace.turns,
            steps = trace.steps,
            run_ms = ms(trace.run),
            sdk_ms = ms(trace.sdk),
            durable_waits = trace.durable_waits,
            durable_wait_ms = ms(trace.durable_wait),
            engine_wait_ms = ms(trace.engine_wait),
            permit_wait_ms = ms(trace.permit_wait),
            "slow run"
        );
        self.remember(SlowRun {
            at_ms: now_ms,
            node: None,
            tenant: key.tenant.to_string(),
            service: key.service.to_string(),
            handler: key.handler.to_string(),
            deployment: key.deployment.to_string(),
            invocation: cairn_store::id_hex(id),
            outcome: outcome
                .map(RunOutcome::as_str)
                .unwrap_or("error")
                .to_string(),
            total_ms: ms(total),
            threshold_ms: ms(threshold),
            turns: u64::from(trace.turns),
            steps: trace.steps,
            run_ms: ms(trace.run),
            sdk_ms: ms(trace.sdk),
            durable_waits: u64::from(trace.durable_waits),
            durable_wait_ms: ms(trace.durable_wait),
            engine_wait_ms: ms(trace.engine_wait),
            permit_wait_ms: ms(trace.permit_wait),
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_tenant_threshold_and_minimum_threshold() {
        let s = SlowLog::default();
        assert_eq!(s.threshold("a"), DEFAULT_THRESHOLD);
        s.set_overrides(HashMap::from_iter([("a".to_string(), 100)]));
        assert_eq!(s.threshold("a"), Duration::from_millis(100));
        assert_eq!(s.threshold("b"), DEFAULT_THRESHOLD);
        assert_eq!(s.0.min_ms.load(Ordering::Relaxed), 100);
        s.set_default(Duration::from_millis(50));
        assert_eq!(s.0.min_ms.load(Ordering::Relaxed), 50);
        s.set_overrides(HashMap::default());
        assert_eq!(s.0.min_ms.load(Ordering::Relaxed), 50);
    }

    fn run(i: u64, tenant: &str) -> SlowRun {
        SlowRun {
            at_ms: i,
            node: None,
            tenant: tenant.into(),
            service: "S".into(),
            handler: "h".into(),
            deployment: "d".into(),
            invocation: format!("{i:048x}"),
            outcome: "completed".into(),
            total_ms: 9_000,
            threshold_ms: 5_000,
            turns: 1,
            steps: 1,
            run_ms: 1,
            sdk_ms: 1,
            durable_waits: 0,
            durable_wait_ms: 0,
            engine_wait_ms: 0,
            permit_wait_ms: 0,
        }
    }

    /// **The ring is bounded**: keeps exactly the [`RING_MAX`] newest lines, returns
    /// the newest first, filtered by tenant.
    #[test]
    fn slow_ring_is_bounded() {
        let s = SlowLog::default();
        for i in 0..(RING_MAX as u64 + 250) {
            s.remember(run(i, if i % 2 == 0 { "a" } else { "b" }));
        }
        let all = s.recent(None);
        assert_eq!(all.len(), RING_MAX);
        assert_eq!(all[0].at_ms, RING_MAX as u64 + 249, "newest first");
        assert_eq!(all.last().unwrap().at_ms, 250, "oldest remaining");
        assert!(s.recent(Some("a")).iter().all(|r| r.tenant == "a"));
        assert_eq!(s.recent(Some("a")).len(), RING_MAX / 2);
    }

    #[test]
    fn per_second_cap() {
        let s = SlowLog::default();
        let admitted = (0..25).filter(|_| s.admit(7)).count();
        assert_eq!(admitted, LINES_PER_SECOND as usize);
        assert!(s.admit(8), "a new second has a new slot");
    }
}
