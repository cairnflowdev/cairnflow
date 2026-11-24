// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The cluster-wide state scrubber** — `cairn-ctl verify` running in the background,
//! **dry**: metrics and logs only, deletes nothing, repairs nothing.
//!
//! Inspired by Neon's scrubber: every event-style cleanup path (intent roll-back,
//! tombstones on behalf, orphan-child cleanup, dead-log sweep, D-d) needs a
//! **state**-style scrubber as its net — one that sees work every other path missed,
//! instead of waiting for a chaos run to stumble on it.
//!
//! - **One runner** for the whole cluster: the leader of the lowest range
//!   ([`crate::gc::is_runner`]), like D-d. Two nodes both believing they are the runner
//!   for one tick only cost double GETs.
//! - **The checks** = [`Ctl::verify`] minus opening Dbs (expensive): the table, the
//!   ranges' manifests, `external_dbs`, orphan `ranges/<id>` (including Dbs without an
//!   owner record), orphan checkpoints, hanging intents, shared-WAL logs nobody holds,
//!   and — when D-d is on — unreferenced retired Dbs that D-d should already have deleted.
//! - **A finding only counts when it is still present in two rounds in a row.** At any
//!   moment a live cluster always has half-finished state (a parent that just left the
//!   table without its tombstone yet, the log of a node that just died not yet swept):
//!   if it is still there in the next round, the self-healing rounds (`Finding::heals`)
//!   have had a whole tick and could not fix it.
//!
//! - **One broken entry does not break the whole round.** Each read retries with a limit
//!   ([`cairn_store::inspect::SCAN_RETRY`]); if it still fails the entry is skipped and
//!   counted ([`ScrubRound::skipped`], metric `cairnflow_scrub_skipped_total`), and the
//!   round goes on. The previous round's findings on a skipped entry are **kept** into the
//!   next round (it has not been seen to disappear). Skipping more than [`skip_limit`]
//!   entries makes the round **incomplete** (`result="incomplete"`), neither clean nor
//!   with findings, and the finding gauges do not change. Only the round-wide reads (the
//!   table, LISTing `owners/`, `ranges/`, `nodewal/`) failing past their limit fail the round.
//!
//! Cost per round: LIST `owners/` + GET each owner record, LIST `ranges/`, one manifest
//! read (LIST + GET) and the checkpoint list per live range, LIST `nodewal/` per node +
//! GET the session record per incarnation + LIST (count) per dead incarnation, LIST
//! `gc-barriers/` when D-d is on. Measured (one node, `memory://`): ~8 + 14·N requests per
//! round with N ranges.
//!
//! **The tick follows a request budget**: `--scrub-every-s`
//! (600 s) is the **fastest** tick; a round costing `C` requests (counted for real, every
//! request of the round — [`cairn_store::ObjectStores::counting`]) makes the next round wait
//! `max(every, C / budget)` ([`next_wait`]). The default budget
//! ([`default_budget`]) is the cost of one round over [`BUDGET_RANGES`] ranges per
//! `every`: up to that many ranges the tick is unchanged, beyond it the period stretches in
//! proportion to the number of ranges, each round still complete. The age of the latest
//! complete round is the metric `cairnflow_scrub_last_complete_age_seconds` (alerted past 6 h).

use {
    crate::ctl::{wal_log_subject, Ctl, CtlError, Finding, Skipped, VerifyOptions, CHECKS},
    cairn_store::ObjectStores,
    std::{
        collections::BTreeSet,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
};

/// Default tick (`--scrub-every-s`) — the fastest tick when the budget does not bite.
pub const DEFAULT_EVERY: Duration = Duration::from_secs(600);

/// Number of ranges up to which the default budget keeps exactly the `every` tick
/// (the cost at 64 ranges stays as it was before the budget existed).
pub const BUDGET_RANGES: u64 = 64;

/// Estimated cost of one round over [`BUDGET_RANGES`] ranges: measured ~8 + 14·64 = 904 requests
/// (`idle_cost.rs::scrub_requests_per_hour_are_capped_by_the_budget_as_ranges_grow`,
/// one node), leaving a margin for a few nodes (LIST `nodewal/`, GET of session records) and
/// tombstones in `owners/`.
pub const BUDGET_ROUND_REQUESTS: u64 = 1_000;

/// Default budget (requests/hour) for the tick `every`: the cost of one round over
/// [`BUDGET_RANGES`] ranges per `every` — 6 000/h at 600 s. A shorter tick
/// (chaos runs use 60 s) raises the budget with it, so the break point stays at 64 ranges.
pub fn default_budget(every: Duration) -> u64 {
    let every = every.as_secs_f64().max(1.0);
    (BUDGET_ROUND_REQUESTS as f64 * 3600.0 / every).ceil() as u64
}

/// Wait before the next round after a round costing `requests` requests: `every`, or longer
/// so that requests/hour stay within `budget` (`0` = no limit).
pub fn next_wait(every: Duration, budget: u64, requests: u64) -> Duration {
    if budget == 0 {
        return every;
    }
    every.max(Duration::from_secs_f64(
        requests as f64 * 3600.0 / budget as f64,
    ))
}

/// The node's scrubber configuration.
#[derive(Clone, Debug)]
pub struct ScrubConfig {
    pub every: Duration,
    /// D-d's `min_age` when D-d is on — check (h) only runs with it: with D-d off a
    /// retired Db lying around is by design, not a leak.
    pub gc_min_age: Option<Duration>,
}

/// Counters for `/metrics`.
pub mod stats {
    use super::*;

    /// Findings still present in two rounds in a row as of the latest round, per
    /// [`CHECKS`] (same order).
    pub static FINDINGS: [AtomicU64; CHECKS.len()] = [const { AtomicU64::new(0) }; CHECKS.len()];
    /// Wall clock (ms) of the latest **complete** round on this node (0 = none yet).
    pub static LAST_COMPLETE_MS: AtomicU64 = AtomicU64::new(0);
    /// Wall clock (ms) when this node first saw itself as the scrubber runner (0 =
    /// never) — the age mark while there is no complete round yet.
    pub static RUNNER_SINCE_MS: AtomicU64 = AtomicU64::new(0);
    /// Number of requests of the latest round (counted for real).
    pub static ROUND_REQUESTS: AtomicU64 = AtomicU64::new(0);
    /// Current wait between two rounds, ms — `every` or longer per the budget.
    pub static WAIT_MS: AtomicU64 = AtomicU64::new(0);
    /// Rounds without any finding still present.
    pub static ROUNDS_CLEAN: AtomicU64 = AtomicU64::new(0);
    /// Rounds with at least one finding still present.
    pub static ROUNDS_FOUND: AtomicU64 = AtomicU64::new(0);
    /// Failed rounds (a store read failed) — the next round starts from scratch.
    pub static ROUNDS_FAILED: AtomicU64 = AtomicU64::new(0);
    /// Rounds that finished but skipped more than [`super::skip_limit`] entries — incomplete.
    pub static ROUNDS_INCOMPLETE: AtomicU64 = AtomicU64::new(0);
    /// Skipped entries (reads failed past the retry limit), accumulated over rounds.
    pub static SKIPPED: AtomicU64 = AtomicU64::new(0);

    pub fn findings(check: &str) -> u64 {
        CHECKS
            .iter()
            .position(|c| *c == check)
            .map_or(0, |i| FINDINGS[i].load(Ordering::Relaxed))
    }

    /// Age (seconds) of the latest complete round at `now_ms`; with no complete round yet it
    /// counts from when the node became the runner. `None` = the node has never been the
    /// runner — not printed, so the alert's `min(...)` only sees runners.
    pub fn last_complete_age_s(now_ms: u64) -> Option<f64> {
        let at = LAST_COMPLETE_MS
            .load(Ordering::Relaxed)
            .max(RUNNER_SINCE_MS.load(Ordering::Relaxed));
        (at > 0).then(|| now_ms.saturating_sub(at) as f64 / 1000.0)
    }

    /// The node just saw itself as the runner — set the first mark.
    pub fn note_runner(now_ms: u64) {
        let _ = RUNNER_SINCE_MS.compare_exchange(0, now_ms, Ordering::Relaxed, Ordering::Relaxed);
    }
}

/// Wall clock, ms (the deterministic simulation's source).
pub fn wall_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The latest scrubber round on this node (`sys_scrub`):
/// written by the scrub task, read by `GET /admin/scrub`. Only the node currently the runner
/// has a round.
#[derive(Clone, Default)]
pub struct ScrubLast(std::sync::Arc<std::sync::Mutex<Option<LastRound>>>);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastRound {
    pub at_ms: u64,
    pub items: usize,
    pub complete: bool,
    pub fresh: usize,
    pub skipped: usize,
    /// Findings still present in two rounds in a row.
    pub findings: Vec<LastFinding>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastFinding {
    pub check: String,
    pub range: Option<u32>,
    pub detail: String,
    pub heals: Option<String>,
}

impl ScrubLast {
    pub fn record(&self, round: &ScrubRound) {
        let at_ms = wall_ms();
        let last = LastRound {
            at_ms,
            items: round.items,
            complete: round.complete,
            fresh: round.fresh,
            skipped: round.skipped.len(),
            findings: round
                .persistent
                .iter()
                .map(|f| LastFinding {
                    check: f.check.to_string(),
                    range: f.range,
                    detail: f.detail.clone(),
                    heals: f.heals.map(str::to_string),
                })
                .collect(),
        };
        if let Ok(mut g) = self.0.lock() {
            *g = Some(last);
        }
    }

    pub fn get(&self) -> Option<LastRound> {
        self.0.lock().ok().and_then(|g| g.clone())
    }
}

/// Result of one round.
#[derive(Clone, Debug, Default)]
pub struct ScrubRound {
    /// Findings still present from the previous round.
    pub persistent: Vec<Finding>,
    /// Findings first seen in this round — waiting for the next round to confirm.
    pub fresh: usize,
    /// Entries skipped because their reads failed past the retry limit.
    pub skipped: Vec<Skipped>,
    /// Number of entries attempted.
    pub items: usize,
    /// `skipped` is within [`skip_limit`] of `items`.
    pub complete: bool,
    /// Number of requests this round sent to the store.
    pub requests: u64,
}

/// Skip threshold of a round that still counts as complete: 2 % of the entries, at least 1.
/// A single skipped entry only delays the findings on it by one round (old findings are
/// kept); more than that means the store is failing widely, and "clean" would be a lie.
pub fn skip_limit(items: usize) -> usize {
    (items / 50).max(1)
}

/// Keys of the previous round that this round **has not looked at** because their entries
/// were skipped: kept, as if still seen — one failed read must not make a long-lived
/// finding start counting from scratch.
fn unseen(prev: &BTreeSet<Key>, skipped: &[Skipped]) -> Vec<Key> {
    prev.iter()
        .filter(|(check, range, subject)| {
            skipped.iter().any(|s| match (s.range, &s.session) {
                // A skipped range: every finding on that range.
                (Some(r), _) => *range == Some(r),
                // A skipped incarnation: its WAL log.
                (None, Some(session)) => {
                    *check == "unheld_wal_log" && *subject == wal_log_subject(session)
                }
                // A whole check skipped.
                (None, None) => *check == s.check,
            })
        })
        .cloned()
        .collect()
}

/// State between rounds: the previous round's findings, and the findings already
/// announced (each once, until it disappears).
#[derive(Debug, Default)]
pub struct Scrubber {
    prev: BTreeSet<Key>,
    reported: BTreeSet<Key>,
    /// Number of requests of the latest round, failed rounds included — for [`next_wait`].
    last_requests: u64,
}

type Key = (&'static str, Option<u32>, String);

/// A key stable across rounds: details carry ages or object counts that change every round.
fn key(f: &Finding) -> Key {
    let subject = match f.check {
        // `shared WAL log of <node>/<inc> (session …) still has N objects …`
        "unheld_wal_log" => f.detail.split(" (").next().unwrap_or_default().to_string(),
        _ => String::new(),
    };
    (f.check, f.range, subject)
}

impl Scrubber {
    /// Number of requests of the latest round (failed rounds included).
    pub fn last_requests(&self) -> u64 {
        self.last_requests
    }

    /// One round. Never writes anything to the bucket.
    pub async fn run_once(
        &mut self,
        stores: &ObjectStores,
        cfg: &ScrubConfig,
    ) -> Result<ScrubRound, CtlError> {
        let counter = std::sync::Arc::new(AtomicU64::new(0));
        let r = self.round(&stores.counting(&counter), cfg).await;
        self.last_requests = counter.load(Ordering::Relaxed);
        stats::ROUND_REQUESTS.store(self.last_requests, Ordering::Relaxed);
        r.map(|round| ScrubRound {
            requests: self.last_requests,
            ..round
        })
    }

    async fn round(
        &mut self,
        stores: &ObjectStores,
        cfg: &ScrubConfig,
    ) -> Result<ScrubRound, CtlError> {
        let opts = VerifyOptions {
            open_dbs: false,
            wal_logs: true,
            // One full D-d tick after the tombstones are old enough.
            retired_after: cfg.gc_min_age.map(|a| a + cfg.every),
            skip_unreadable: true,
            ..VerifyOptions::default()
        };
        let rep = match Ctl::new(stores.clone()).verify(opts).await {
            Ok(r) => r,
            Err(e) => {
                stats::ROUNDS_FAILED.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        let mut now: BTreeSet<Key> = rep.findings.iter().map(key).collect();
        now.extend(unseen(&self.prev, &rep.skipped));
        let mut round = ScrubRound {
            items: rep.items,
            complete: rep.skipped.len() <= skip_limit(rep.items),
            skipped: rep.skipped,
            ..ScrubRound::default()
        };
        for f in rep.findings {
            if self.prev.contains(&key(&f)) {
                round.persistent.push(f);
            } else {
                round.fresh += 1;
            }
        }
        stats::SKIPPED.fetch_add(round.skipped.len() as u64, Ordering::Relaxed);
        if round.complete {
            stats::LAST_COMPLETE_MS.store(wall_ms(), Ordering::Relaxed);
            for (i, check) in CHECKS.iter().enumerate() {
                let n = round
                    .persistent
                    .iter()
                    .filter(|f| f.check == *check)
                    .count();
                stats::FINDINGS[i].store(n as u64, Ordering::Relaxed);
            }
            if round.persistent.is_empty() {
                stats::ROUNDS_CLEAN.fetch_add(1, Ordering::Relaxed);
            } else {
                stats::ROUNDS_FOUND.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // The finding gauges keep the numbers of the latest complete round.
            stats::ROUNDS_INCOMPLETE.fetch_add(1, Ordering::Relaxed);
        }
        if !round.skipped.is_empty() {
            let sample: Vec<String> = round
                .skipped
                .iter()
                .take(5)
                .map(|s| format!("{} ({}): {}", s.item, s.check, s.error))
                .collect();
            tracing::warn!(
                skipped = round.skipped.len(),
                items = round.items,
                limit = skip_limit(round.items),
                complete = round.complete,
                "scrubber: skipped {} entries whose reads failed past the retry limit{} — e.g. {}",
                round.skipped.len(),
                if round.complete {
                    ""
                } else {
                    ", round INCOMPLETE"
                },
                sample.join("; ")
            );
        }
        for f in &round.persistent {
            if self.reported.insert(key(f)) {
                tracing::warn!(
                    check = f.check,
                    range = ?f.range,
                    healed_by = f.heals.unwrap_or("none"),
                    "scrubber: {} — still present after two rounds (dry run, nothing repaired)",
                    f.detail
                );
            }
        }
        self.reported.retain(|k| now.contains(k));
        self.prev = now;
        tracing::debug!(
            persistent = round.persistent.len(),
            fresh = round.fresh,
            skipped = round.skipped.len(),
            items = round.items,
            "scrubber: round done"
        );
        Ok(round)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default budget keeps the 600 s tick up to [`BUDGET_RANGES`] ranges (measured cost
    /// ~8 + 14·N), beyond that it stretches in proportion to the ranges; `0` = no limit.
    #[test]
    fn pace_by_budget() {
        let every = DEFAULT_EVERY;
        let budget = default_budget(every);
        assert_eq!(budget, 6_000);
        let round = |n: u64| 8 + 14 * n;
        assert_eq!(next_wait(every, budget, round(16)), every);
        assert_eq!(next_wait(every, budget, round(64)), every);
        let w128 = next_wait(every, budget, round(128));
        assert!(w128 > every && w128 < every * 2, "{w128:?}");
        // Requests/hour stay within the budget as the number of ranges grows.
        for n in [128u64, 1024, 4096] {
            let per_hour =
                round(n) as f64 * 3600.0 / next_wait(every, budget, round(n)).as_secs_f64();
            assert!(per_hour <= budget as f64 + 1.0, "{n} ranges: {per_hour}");
        }
        assert_eq!(next_wait(every, 0, round(4096)), every);
        // A short tick (chaos 60 s): the budget grows with it, the break point stays at 64 ranges.
        let fast = Duration::from_secs(60);
        assert_eq!(next_wait(fast, default_budget(fast), round(64)), fast);
    }

    #[test]
    fn full_round_age() {
        // The only test in the lib binary that touches these two static marks.
        assert_eq!(stats::last_complete_age_s(10_000), None);
        stats::note_runner(1_000);
        stats::note_runner(5_000);
        assert_eq!(stats::last_complete_age_s(4_000), Some(3.0));
        stats::LAST_COMPLETE_MS.store(3_000, Ordering::Relaxed);
        assert_eq!(stats::last_complete_age_s(4_000), Some(1.0));
    }

    #[test]
    fn skip_threshold() {
        assert_eq!(skip_limit(0), 1);
        assert_eq!(skip_limit(10), 1);
        assert_eq!(skip_limit(100), 2);
        assert_eq!(skip_limit(1000), 20);
    }

    fn skipped(range: Option<u32>, session: Option<&str>, check: &'static str) -> Skipped {
        Skipped {
            check,
            range,
            session: session.map(str::to_string),
            item: "x".into(),
            error: "503".into(),
        }
    }

    /// A skipped entry keeps its old findings; other entries do not.
    #[test]
    fn keeps_findings_of_skipped_items() {
        let prev: BTreeSet<Key> = [
            ("orphan_db", Some(7), String::new()),
            ("db", Some(8), String::new()),
            ("unheld_wal_log", None, wal_log_subject("n1/5")),
            ("unheld_wal_log", None, wal_log_subject("n2/9")),
            ("stuck_intent", Some(3), String::new()),
        ]
        .into_iter()
        .collect();
        let kept = unseen(
            &prev,
            &[
                skipped(Some(7), None, "orphan_db"),
                skipped(None, Some("n1/5"), "unheld_wal_log"),
            ],
        );
        assert_eq!(
            kept,
            vec![
                ("orphan_db", Some(7), String::new()),
                ("unheld_wal_log", None, wal_log_subject("n1/5")),
            ]
        );
        let kept = unseen(&prev, &[skipped(None, None, "stuck_intent")]);
        assert_eq!(kept, vec![("stuck_intent", Some(3), String::new())]);
    }
}
