// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A slow node sheds its own leadership.
//!
//! A "grey" node — still alive, still renewing its session, but every PUT to the bucket
//! takes whole seconds — used to be detected only when its lease expired, that is
//! never: the lease still renews, only slowly. Every range it leads suffers that latency.
//!
//! The round here runs every [`SlowConfig::round`] (10 s):
//!
//! 1. **Measure**: take one round from the node's PUT meter
//!    ([`cairn_store::health::SlowMeter`] — session renewals and the shared WAL's PUT
//!    attempts, no probe requests), feed it into an AIMD score 1–100
//!    ([`cairn_store::health::SlowScore`]). A round in which the node ran out of turn slots
//!    more than half the time is skipped: the node is **busy**, not slow (inspired by TiKV,
//!    which ignores timeouts while the CPU is busy).
//! 2. **Publish**: the round's score and PUT p99 go into the session record at the next
//!    renewal, so every node sees every node's score without an extra object.
//! 3. **Decide to shed**: a score of 100 for two rounds in a row **and** our PUT p99
//!    above 2× the median p99 of the other live nodes ⇒ take the `shedding` marker
//!    ([`cairn_store::health::ShedMarker`], one node at a time). Once taken, turn on
//!    `draining` on the [`Balancer`] (refuse to take ranges), publish
//!    `draining=1`, then resign the ranges held, [`SlowConfig::shed_batch`] ranges
//!    every [`SlowConfig::shed_gap`]. Compared with the median rather than an absolute
//!    threshold: if the bucket is uniformly slow nobody sheds.
//! 4. **Recover**: a score below [`SlowConfig::recover_below`] continuously for
//!    [`SlowConfig::recover_for`] ⇒ remove `draining`, give the marker back, take ranges
//!    again by the fair share as usual.
//! 5. **Last net**: never shed while we are the only live node; while draining, if every
//!    other node is slow too (score ≥ [`ALSO_SLOW`]) or no other node is left, remove
//!    `draining` at once — shedding with nobody to take over is causing an incident.

// The runtime's clock: outside simulation it is `std::time::Instant`, inside
// deterministic simulation it is the virtual clock.
use tokio::time::Instant;
use {
    crate::{balance::Balancer, engine_pool::NodeTurns, partitions::Partitions},
    cairn_store::{
        health::{Published, ShedMarker, SlowMeter, SlowScore},
        PartitionId, Session,
    },
    std::{collections::BTreeMap, time::Duration},
};

/// A score at or above this is "slow too" for the last net (step 5).
pub const ALSO_SLOW: u32 = 50;

/// Knobs of the slowness score round. The defaults are the design values; tests shorten them.
#[derive(Clone, Debug)]
pub struct SlowConfig {
    /// Length of one scoring round.
    pub round: Duration,
    /// A session renewal longer than this is a "timeout".
    pub slow_put: Duration,
    /// The score takes this long to go from 100 down to 1 when every round is clean (100/30 per
    /// 10 s round with 5 minutes).
    pub decay: Duration,
    /// Shed when slow. Off still measures and publishes the score.
    pub shed: bool,
    /// Consecutive rounds at score 100 before shedding.
    pub shed_rounds: u32,
    /// Recover when the score is below this …
    pub recover_below: f64,
    /// … continuously for this long.
    pub recover_for: Duration,
    /// A `shedding` marker standing still this long is treated as abandoned by others.
    pub marker_ttl: Duration,
    /// Shed this many ranges at a time …
    pub shed_batch: usize,
    /// … this far apart.
    pub shed_gap: Duration,
}

impl Default for SlowConfig {
    fn default() -> Self {
        Self {
            round: Duration::from_secs(10),
            slow_put: Duration::from_secs(1),
            decay: Duration::from_secs(300),
            shed: true,
            shed_rounds: 2,
            recover_below: 10.0,
            recover_for: Duration::from_secs(300),
            marker_ttl: Duration::from_secs(120),
            shed_batch: 2,
            shed_gap: Duration::from_secs(1),
        }
    }
}

/// How many times per round the node's turn slots are looked at, to tell busy from slow.
const BUSY_SAMPLES: u32 = 10;

/// The decider's next step, after one round.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Step {
    /// Do nothing.
    Nothing,
    /// Shedding conditions met: try to take the marker, with the reason.
    Shed(String),
    /// Draining and still draining.
    Stay,
    /// Remove `draining`, with the reason.
    Recover(&'static str),
}

/// The shed/recover decider — pure, no I/O, so each branch can be tested.
pub(crate) struct Decider {
    cfg: SlowConfig,
    /// Consecutive rounds at score 100.
    at_top: u32,
    /// Draining (the marker is taken).
    pub(crate) draining: bool,
    /// Since when the score has been below `recover_below`.
    low_since: Option<Instant>,
}

/// Median of the values (the upper middle element when the count is even).
fn median(mut v: Vec<u64>) -> Option<u64> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    Some(v[v.len() / 2])
}

impl Decider {
    pub(crate) fn new(cfg: SlowConfig) -> Self {
        Self {
            cfg,
            at_top: 0,
            draining: false,
            low_since: None,
        }
    }

    /// One round: our score, our PUT p99, and the health of the other live nodes.
    pub(crate) fn step(
        &mut self,
        score: f64,
        p99: Option<u64>,
        peers: &BTreeMap<String, Published>,
        now: Instant,
    ) -> Step {
        self.at_top = if score >= 100.0 { self.at_top + 1 } else { 0 };
        // Other nodes can still take work: alive and not draining.
        let others: Vec<&Published> = peers.values().filter(|p| !p.draining).collect();
        if !self.draining {
            self.low_since = None;
            if !self.cfg.shed || others.is_empty() || self.at_top < self.cfg.shed_rounds {
                return Step::Nothing;
            }
            let (Some(mine), Some(mid)) = (
                p99,
                median(others.iter().filter_map(|p| p.p99_put_ms).collect()),
            ) else {
                return Step::Nothing;
            };
            if mine > mid.saturating_mul(2) {
                return Step::Shed(format!(
                    "slow={score:.0} p99_put_ms={mine} median_other_nodes={mid}"
                ));
            }
            return Step::Nothing;
        }
        if others.is_empty() {
            return Step::Recover("no other live node");
        }
        if others
            .iter()
            .all(|p| p.slow.is_some_and(|s| s >= ALSO_SLOW))
        {
            return Step::Recover("every other node is slow too");
        }
        if score < self.cfg.recover_below {
            let since = *self.low_since.get_or_insert(now);
            if now.duration_since(since) >= self.cfg.recover_for {
                return Step::Recover("score recovered");
            }
        } else {
            self.low_since = None;
        }
        Step::Stay
    }
}

/// What the slowness score round needs in order to shed (cluster only).
pub(crate) struct Shedder {
    pub session: Session,
    pub balancer: Balancer,
    pub handles: Partitions,
    pub marker: ShedMarker,
}

impl Shedder {
    /// Resign every range the balancer holds, `batch` ranges every `gap`.
    async fn shed(&self, batch: usize, gap: Duration) {
        let held: Vec<u32> = self.balancer.held().into_iter().collect();
        for (i, chunk) in held.chunks(batch.max(1)).enumerate() {
            if i > 0 {
                cairn_store::pace::beat(gap).await;
            }
            if !self.balancer.is_draining() {
                return;
            }
            let resigns = chunk.iter().filter_map(|p| {
                let h = self.handles.get(PartitionId(*p))?;
                // Bounded: the shedding round renews the `shedding` marker; standing
                // behind an engine that does not read its commands would let another node take the marker.
                Some(async move {
                    (
                        *p,
                        tokio::time::timeout(crate::actor::RESIGN_WAIT, h.resign()).await,
                    )
                })
            });
            for (p, res) in futures::future::join_all(resigns).await {
                match res {
                    Ok(Ok(())) => tracing::info!(partition = p, "slow node — range handed over"),
                    Ok(Err(e)) => {
                        tracing::warn!(partition = p, error = %e, "resign missed, retrying next round")
                    }
                    Err(_) => tracing::warn!(
                        partition = p,
                        "resign not done after {:?}, reconsidering next round",
                        crate::actor::RESIGN_WAIT
                    ),
                }
            }
        }
    }

    fn set_draining(&self, meter: &SlowMeter, on: bool) {
        self.balancer.set_draining(on);
        meter.set_draining(on);
    }
}

/// Whether another node can take the ranges we cannot serve: alive, not draining, and
/// not every such node is slow (score ≥ [`ALSO_SLOW`]) — the same last net as the
/// [`Decider`]'s. Also the net of a failed shared-WAL stream rotation
/// (`NodeWalConfig::rotation_net`): false ⇒ the whole bucket is slow, fail-stop
/// as before instead of keeping a rotated slot nobody serves.
pub(crate) fn peers_can_serve(peers: &BTreeMap<String, Published>) -> bool {
    let others: Vec<&Published> = peers.values().filter(|p| !p.draining).collect();
    !others.is_empty()
        && !others
            .iter()
            .all(|p| p.slow.is_some_and(|s| s >= ALSO_SLOW))
}

/// Run the node's slowness score round. The task exits when cancelled (node shutdown).
pub(crate) fn spawn(
    cfg: SlowConfig,
    meter: SlowMeter,
    turns: Option<NodeTurns>,
    mut shedder: Option<Shedder>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(cairn_store::io_source::tagged("slow", async move {
        let mut score = SlowScore::new(cfg.round, cfg.decay);
        let mut decider = Decider::new(cfg.clone());
        let tick = cfg.round / BUSY_SAMPLES;
        loop {
            let mut full = 0;
            for _ in 0..BUSY_SAMPLES {
                cairn_store::pace::beat(tick).await;
                if turns.as_ref().is_some_and(|t| !t.has_room()) {
                    full += 1;
                }
            }
            let round = meter.take_round();
            if full * 2 > BUSY_SAMPLES {
                tracing::debug!(
                    full,
                    "node ran out of turn slots for more than half the round — busy, not counted as slow"
                );
            } else {
                score.update(&round);
            }
            meter.publish(score.value(), round.p99_ms);
            tracing::debug!(
                score = score.value(),
                samples = round.samples,
                timeout = round.timeouts,
                p99_ms = ?round.p99_ms,
                "slowness score round"
            );
            let Some(sh) = shedder.as_mut() else {
                continue;
            };
            let peers = sh.session.membership().peer_health();
            match decider.step(score.value(), round.p99_ms, &peers, Instant::now()) {
                Step::Nothing => {}
                Step::Shed(reason) => match sh.marker.try_acquire(&sh.session, &reason).await {
                    Ok(true) => {
                        tracing::warn!(reason = %reason, "node clearly slower than the cluster — shedding leadership");
                        decider.draining = true;
                        sh.set_draining(&meter, true);
                        sh.shed(cfg.shed_batch, cfg.shed_gap).await;
                    }
                    Ok(false) => {
                        tracing::info!(reason = %reason, "node slow, but another node is shedding — waiting")
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "cannot read/write the shedding marker — retrying next round")
                    }
                },
                Step::Stay => {
                    match sh.marker.renew().await {
                        Ok(true) => {}
                        Ok(false) => {
                            tracing::debug!(
                                "the shedding marker is no longer ours — still draining"
                            )
                        }
                        Err(e) => tracing::debug!(error = %e, "cannot renew the shedding marker"),
                    }
                    // A range landing in our hands while draining (a missed resign…) is shed again.
                    if !sh.balancer.held().is_empty() {
                        sh.shed(cfg.shed_batch, cfg.shed_gap).await;
                    }
                }
                Step::Recover(why) => {
                    tracing::warn!(
                        reason = why,
                        score = score.value(),
                        "stop draining — taking ranges by the fair share again"
                    );
                    decider.draining = false;
                    sh.set_draining(&meter, false);
                    sh.marker.release().await;
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The net of a failed rotation: only while another node can take the ranges.
    #[test]
    fn peers_can_serve_needs_a_live_not_slow_not_draining_peer() {
        let mut peers = BTreeMap::new();
        assert!(!peers_can_serve(&peers), "the only live node");
        peers.insert("b".to_string(), peer(80, 5000));
        assert!(!peers_can_serve(&peers), "every other node is slow too");
        peers.insert(
            "c".to_string(),
            Published {
                draining: true,
                ..peer(1, 10)
            },
        );
        assert!(
            !peers_can_serve(&peers),
            "the remaining healthy node is draining"
        );
        peers.insert("d".to_string(), peer(1, 10));
        assert!(peers_can_serve(&peers));
    }

    fn peer(slow: u32, p99: u64) -> Published {
        Published {
            slow: Some(slow),
            p99_put_ms: Some(p99),
            draining: false,
        }
    }

    fn peers(v: &[Published]) -> BTreeMap<String, Published> {
        v.iter()
            .enumerate()
            .map(|(i, p)| (format!("n{i}"), *p))
            .collect()
    }

    fn cfg() -> SlowConfig {
        SlowConfig {
            recover_for: Duration::from_secs(60),
            ..SlowConfig::default()
        }
    }

    /// Two rounds in a row at 100 **and** p99 > 2× the other nodes' median.
    #[test]
    fn sheds_after_two_rounds_at_top_when_slower_than_median() {
        let now = Instant::now();
        let others = peers(&[peer(1, 5), peer(1, 7), peer(90, 900)]);
        let mut d = Decider::new(cfg());
        assert_eq!(
            d.step(100.0, Some(300), &others, now),
            Step::Nothing,
            "only one round so far"
        );
        assert!(matches!(
            d.step(100.0, Some(300), &others, now),
            Step::Shed(_)
        ));
        // A round below 100 starts over.
        let mut d = Decider::new(cfg());
        d.step(100.0, Some(300), &others, now);
        d.step(99.0, Some(300), &others, now);
        assert_eq!(d.step(100.0, Some(300), &others, now), Step::Nothing);
        // Not slower than 2× the median (median = 7): no shedding.
        let mut d = Decider::new(cfg());
        d.step(100.0, Some(14), &others, now);
        assert_eq!(d.step(100.0, Some(14), &others, now), Step::Nothing);
        // Other nodes do not publish p99 (old binaries): cannot compare, no shedding.
        let mut d = Decider::new(cfg());
        let blind = peers(&[Published::default()]);
        d.step(100.0, Some(300), &blind, now);
        assert_eq!(d.step(100.0, Some(300), &blind, now), Step::Nothing);
        // Off: never sheds.
        let mut d = Decider::new(SlowConfig {
            shed: false,
            ..cfg()
        });
        d.step(100.0, Some(300), &others, now);
        assert_eq!(d.step(100.0, Some(300), &others, now), Step::Nothing);
    }

    /// The only live node (or every other node draining) does not shed.
    #[test]
    fn never_sheds_when_alone() {
        let now = Instant::now();
        let mut d = Decider::new(cfg());
        let none = BTreeMap::new();
        for _ in 0..5 {
            assert_eq!(d.step(100.0, Some(5000), &none, now), Step::Nothing);
        }
        let draining = peers(&[Published {
            draining: true,
            ..peer(1, 1)
        }]);
        assert_eq!(d.step(100.0, Some(5000), &draining, now), Step::Nothing);
    }

    /// Recovery: score < 10 continuously for `recover_for`; a score jumping back up restarts
    /// the count. Last net: every other node slow too, or nobody left, removes it at once.
    #[test]
    fn recovers_after_low_score_window_or_when_others_slow_too() {
        let t0 = Instant::now();
        let others = peers(&[peer(1, 5), peer(1, 6)]);
        let mut d = Decider::new(cfg());
        d.draining = true;
        assert_eq!(d.step(100.0, Some(300), &others, t0), Step::Stay);
        assert_eq!(d.step(5.0, Some(3), &others, t0), Step::Stay);
        assert_eq!(
            d.step(5.0, Some(3), &others, t0 + Duration::from_secs(30)),
            Step::Stay
        );
        assert_eq!(
            d.step(20.0, Some(3), &others, t0 + Duration::from_secs(40)),
            Step::Stay
        );
        assert_eq!(
            d.step(5.0, Some(3), &others, t0 + Duration::from_secs(70)),
            Step::Stay,
            "count restarted"
        );
        assert_eq!(
            d.step(5.0, Some(3), &others, t0 + Duration::from_secs(130)),
            Step::Recover("score recovered")
        );

        let mut d = Decider::new(cfg());
        d.draining = true;
        let slow_too = peers(&[peer(64, 400), peer(100, 900)]);
        assert_eq!(
            d.step(100.0, Some(300), &slow_too, t0),
            Step::Recover("every other node is slow too")
        );
        let mixed = peers(&[peer(64, 400), peer(1, 5)]);
        assert_eq!(d.step(100.0, Some(300), &mixed, t0), Step::Stay);
        assert_eq!(
            d.step(100.0, Some(300), &BTreeMap::new(), t0),
            Step::Recover("no other live node")
        );
    }
}
