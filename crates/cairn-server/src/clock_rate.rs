// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **A node stops leading when its clock runs at the wrong rate** (LS-2).
//!
//! Every lease assumes the nodes' monotonic clocks run within 1.5× of each
//! other: the owner's grant is ⅔·TTL on its own clock, an observer expires it
//! after one TTL on its own — an owner slower than the observer by more than
//! 1.5× keeps a grant alive past the moment someone else takes the range.
//! Read/write safety does not rely on this (LS-4, LS-5: `writer_epoch`, log
//! sealing, read fences), but LS3_LEADER_IS_RECORDED_OWNER, the coordinator's
//! expiry rule (LS-15) and the solo-mode challenge (LS-16) do. Before this
//! check the assumption was silent.
//!
//! The loop here runs every [`StoreClockConfig::check_every`] (10 s) and reads
//! the rate of the node's monotonic clock against store time
//! ([`StoreClock::rate`]: the `Date` header of requests already being made, a
//! 5-minute window, a certain interval `[lo, hi]`):
//!
//! 1. **Skewed** — the whole interval outside `1 ± `[`TOLERANCE`] — ⇒ the
//!    clock is **not trusted**: metric, `error` log; release the coordinator
//!    role (and do not take it back —
//!    [`crate::control::Control::set_clock_trusted`]); set
//!    `SlowMeter::clock_draining` (the balancer refuses every range, the
//!    session record announces `draining=1`) and yield every range held. A
//!    ±10% threshold: two nodes both within it differ by at most 1.1/0.9 ≈
//!    1.22×, well under 1.5× (±20% would give exactly 1.5× when two nodes skew
//!    in opposite directions).
//! 2. **Back within bounds** — the whole interval inside `1 ± TOLERANCE/2`
//!    (two-level hysteresis so it does not flap) ⇒ undrain, take ranges as
//!    usual.
//! 3. Not certain (too few samples, the interval straddles a threshold) ⇒ keep
//!    the current state.
//! 4. **Last safety net** (like the slow node's LS-11): if no other node can
//!    take ranges ([`crate::slow::peers_can_serve`]), do not drain — releasing
//!    with nobody to take over is a self-inflicted outage, and there is no
//!    other observer for us to be skewed against — still metric and an `error`
//!    log on every check; once a peer is back, drain at once.
//!
//! A store without a `Date` header (`memory://`, `file://`) gives no samples,
//! so the loop never concludes anything — one process or one machine.

use {
    crate::{balance::Balancer, control::Control, partitions::Partitions},
    cairn_store::{
        health::SlowMeter,
        store_clock::{Rate, StoreClock, StoreClockConfig},
        PartitionId, Session,
    },
    std::sync::atomic::{AtomicU64, Ordering},
};

/// Rate off by more than `1 ± TOLERANCE` (certain) ⇒ stop leading.
pub const TOLERANCE: f64 = 0.10;

/// Measurements for `/metrics` (accumulated per process — one node per
/// process outside tests).
pub mod stats {
    use super::*;

    /// Most recent measured rate (monotonic / store) × 1000; 0 = not measured yet.
    pub static RATE_MILLI: AtomicU64 = AtomicU64::new(0);
    /// Number of measuring loops currently treating the clock as untrusted (0/1 with one node).
    pub static UNTRUSTED: AtomicU64 = AtomicU64::new(0);
    /// Transitions to untrusted.
    pub static STEPDOWNS: AtomicU64 = AtomicU64::new(0);
}

/// Conclusion of one check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Certainly off by more than the threshold.
    Skewed,
    /// Certainly within half the threshold.
    Fine,
    /// Not certain — keep the current state.
    Unsure,
}

/// Conclusion from a measurement (see the module head).
pub fn verdict(rate: Option<Rate>) -> Verdict {
    match rate {
        Some(r) if r.outside(TOLERANCE) => Verdict::Skewed,
        Some(r) if r.inside(TOLERANCE / 2.0) => Verdict::Fine,
        _ => Verdict::Unsure,
    }
}

/// Repeats the `error` log while still skewed.
const LOG_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// A loop's "clock untrusted" flag, keeping [`stats::UNTRUSTED`] correct even
/// when the task is cancelled while untrusted (node shutdown).
struct Untrusted(bool);

impl Untrusted {
    fn set(&mut self, on: bool) {
        if on != self.0 {
            if on {
                stats::UNTRUSTED.fetch_add(1, Ordering::Relaxed);
            } else {
                stats::UNTRUSTED.fetch_sub(1, Ordering::Relaxed);
            }
            self.0 = on;
        }
    }
}

impl Drop for Untrusted {
    fn drop(&mut self) {
        self.set(false);
    }
}

/// What the loop needs to stop leading.
pub(crate) struct StepDown {
    pub session: Session,
    pub balancer: Balancer,
    pub handles: Partitions,
    pub meter: SlowMeter,
    pub control: Control,
}

impl StepDown {
    /// Yields every range the balancer holds (with a limit per range).
    async fn resign_all(&self) {
        let held: Vec<u32> = self.balancer.held().into_iter().collect();
        let resigns = held.iter().filter_map(|p| {
            let h = self.handles.get(PartitionId(*p))?;
            Some(async move {
                (
                    *p,
                    tokio::time::timeout(crate::actor::RESIGN_WAIT, h.resign()).await,
                )
            })
        });
        for (p, res) in futures::future::join_all(resigns).await {
            match res {
                Ok(Ok(())) => tracing::warn!(partition = p, "clock rate skewed — range yielded"),
                Ok(Err(e)) => {
                    tracing::warn!(partition = p, error = %e, "resign failed, next round retries")
                }
                Err(_) => tracing::warn!(
                    partition = p,
                    "resign not done after {:?}, next round checks again",
                    crate::actor::RESIGN_WAIT
                ),
            }
        }
    }
}

/// Runs the loop measuring the node's clock rate. Without `clock` (the store
/// is not HTTP) it does not run.
pub(crate) fn spawn(clock: StoreClock, step: StepDown) -> tokio::task::JoinHandle<()> {
    let cfg: StoreClockConfig = clock.config();
    tokio::spawn(cairn_store::io_source::tagged("clock_rate", async move {
        let mut untrusted = Untrusted(false);
        // `error` log on the state change, repeated every minute while still skewed.
        let mut last_log: Option<(tokio::time::Instant, bool)> = None;
        loop {
            cairn_store::pace::beat(cfg.check_every).await;
            let rate = clock.rate();
            if let Some(r) = rate {
                stats::RATE_MILLI.store((r.mid * 1000.0).round() as u64, Ordering::Relaxed);
            }
            match (untrusted.0, verdict(rate)) {
                (false, Verdict::Skewed) => {
                    untrusted.set(true);
                    last_log = None;
                    step.control.set_clock_trusted(false);
                    stats::STEPDOWNS.fetch_add(1, Ordering::Relaxed);
                }
                (true, Verdict::Fine) => {
                    untrusted.set(false);
                    step.control.set_clock_trusted(true);
                    step.meter.set_clock_draining(false);
                    tracing::warn!(
                        rate = ?rate.map(|r| (r.lo, r.hi)),
                        "node clock back within rate bounds against the store — taking ranges again by fair share"
                    );
                }
                _ => {}
            }
            if !untrusted.0 {
                continue;
            }
            // Untrusted: every check releases everything again (a range that
            // slipped back to us, a role taken before the flag was set).
            let r = rate.map(|r| (r.lo, r.hi));
            if step.session.coordinating().is_some() {
                step.session.release_coordinator().await;
            }
            let peers = crate::slow::peers_can_serve(&step.session.membership().peer_health());
            let log = last_log.is_none_or(|(at, p)| p != peers || at.elapsed() >= LOG_EVERY);
            if log {
                last_log = Some((tokio::time::Instant::now(), peers));
            }
            if peers {
                if log {
                    tracing::error!(
                    rate = ?r,
                    threshold = TOLERANCE,
                    "node monotonic clock rate is off against the store beyond the threshold — stopping leadership \
                     (releasing ranges and the coordinator role, taking nothing back until within bounds)"
                    );
                }
                step.meter.set_clock_draining(true);
                if !step.balancer.held().is_empty() {
                    step.resign_all().await;
                }
            } else {
                if log {
                    tracing::error!(
                        rate = ?r,
                        threshold = TOLERANCE,
                        "node monotonic clock rate is off against the store beyond the threshold — no \
                         other node can take ranges, so keeping them (releasing would be a self-inflicted outage)"
                    );
                }
                step.meter.set_clock_draining(false);
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration};

    fn rate(lo: f64, hi: f64) -> Option<Rate> {
        Some(Rate {
            lo,
            hi,
            mid: (lo + hi) / 2.0,
            span: Duration::from_secs(300),
        })
    }

    /// Conclude only when certain: the whole interval outside ±10% ⇒ skewed;
    /// the whole interval inside ±5% ⇒ fine; anything else (straddling a
    /// threshold, between the two levels, not measured) ⇒ not certain.
    #[test]
    fn verdict_needs_the_whole_interval() {
        assert_eq!(verdict(rate(1.28, 1.32)), Verdict::Skewed);
        assert_eq!(verdict(rate(0.75, 0.78)), Verdict::Skewed);
        assert_eq!(verdict(rate(0.99, 1.01)), Verdict::Fine);
        assert_eq!(verdict(rate(1.04, 1.049)), Verdict::Fine);
        assert_eq!(verdict(rate(1.08, 1.12)), Verdict::Unsure, "straddles 1.1");
        assert_eq!(verdict(rate(1.06, 1.09)), Verdict::Unsure, "in 5%–10%");
        assert_eq!(verdict(rate(0.5, 1.5)), Verdict::Unsure, "too wide");
        assert_eq!(verdict(None), Verdict::Unsure);
    }
}
