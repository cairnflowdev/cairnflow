// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Time-based waits of product code — one seam.
//!
//! A bare `tokio::time::sleep(…).await` in `crates/*/src` is blocked by
//! `scripts/lint-sources.sh`; waits go through here, **naming the kind of wait**:
//!
//! - [`beat`] — the tick of a background loop: rest one period between two rounds of
//!   work. Counted from the call (the end of the previous round), like the old `sleep` —
//!   not an `interval`: a long round does not make the ticks denser, and the loop's store
//!   requests per hour stay the same.
//! - [`poll_gap`] — the gap between two checks of a condition; the caller keeps its own
//!   bound (a deadline or a count).
//! - [`retry_pause`], [`Backoff`] — the wait before a retry after an error; [`Backoff`] is
//!   the shared backoff rule: doubling up to a cap, with [`jittered`].
//!
//! **The clock** is the tokio runtime's timer: on a real node it is the monotonic clock;
//! in deterministic simulation (`start_paused`) it is virtual time — so nothing sleeps by
//! real time, and background loops advance as soon as the runtime is idle. Today the
//! three functions are just named `tokio::time::sleep`; the names are there so the
//! simulator (jittering ticks by seed, counting pending work) and readers know **why** a
//! loop sleeps, and so every new `sleep` has to pick a kind instead of slipping past the
//! lint.
//!
//! Waiting together with another signal (stop, message, deadline) is still a `select!`
//! branch with `tokio::time::sleep`/`sleep_until` — that does not belong here.

use std::time::Duration;

/// The tick of a background loop: rest `period`, counted from now.
pub async fn beat(period: Duration) {
    tokio::time::sleep(period).await;
}

/// The gap between two checks of a condition. The caller keeps the bound.
pub async fn poll_gap(gap: Duration) {
    tokio::time::sleep(gap).await;
}

/// Rest `pause` before a retry after an error. The shared backoff rule is [`Backoff`];
/// this function is for places with their own rule (linear backoff, fixed tick).
pub async fn retry_pause(pause: Duration) {
    tokio::time::sleep(pause).await;
}

/// `d` stretched randomly within `[d/2, d]` — separates nodes retrying on the same tick.
/// Random bytes come from [`crate::entropy`] (seedable by the simulator).
pub fn jittered(d: Duration) -> Duration {
    let mut b = [0u8; 4];
    crate::entropy::fill(&mut b);
    let half = d / 2;
    half + half.mul_f64(f64::from(u32::from_le_bytes(b)) / f64::from(u32::MAX))
}

/// Capped exponential backoff with jitter: the k-th rest is `jittered(min(first·2^k, max))`.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    next: Duration,
    max: Duration,
}

impl Backoff {
    pub fn new(first: Duration, max: Duration) -> Self {
        Self { next: first, max }
    }

    /// The next rest (jittered), then double the level for the following one. For places
    /// that need the number in advance (to log it, to cut it by the remaining budget)
    /// before calling [`retry_pause`].
    pub fn next_pause(&mut self) -> Duration {
        let pause = jittered(self.next);
        self.next = (self.next * 2).min(self.max);
        pause
    }

    /// [`Self::next_pause`], then rest that long.
    pub async fn wait(&mut self) {
        retry_pause(self.next_pause()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jittered_lies_in_upper_half() {
        let d = Duration::from_millis(200);
        for _ in 0..1_000 {
            let j = jittered(d);
            assert!(j >= d / 2 && j <= d, "{j:?}");
        }
    }

    /// The backoff level doubles up to the cap; each rest is that time's level, jittered.
    #[test]
    fn backoff_doubles_up_to_cap() {
        let mut b = Backoff::new(Duration::from_millis(50), Duration::from_millis(300));
        let levels = [50, 100, 200, 300, 300];
        for ms in levels {
            let p = b.next_pause();
            let level = Duration::from_millis(ms);
            assert!(
                p >= level / 2 && p <= level,
                "{p:?} outside [{ms}/2, {ms}] ms"
            );
        }
    }

    /// The three kinds of wait are runtime timers: under a paused clock they advance by
    /// virtual time, not by real time.
    #[tokio::test(start_paused = true)]
    #[allow(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "compared with real time to prove it does not sleep by real time"
    )]
    async fn waits_on_runtime_clock() {
        let t0 = tokio::time::Instant::now();
        let real = std::time::Instant::now();
        beat(Duration::from_secs(60)).await;
        poll_gap(Duration::from_secs(60)).await;
        retry_pause(Duration::from_secs(60)).await;
        Backoff::new(Duration::from_secs(60), Duration::from_secs(60))
            .wait()
            .await;
        assert!(t0.elapsed() >= Duration::from_secs(210));
        assert!(real.elapsed() < Duration::from_secs(5));
    }
}
