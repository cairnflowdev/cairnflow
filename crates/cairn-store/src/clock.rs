// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Injectable clock.
//!
//! Decision code **receives** time as a parameter instead of reading the clock itself: pure
//! functions take a [`Now`] (or one of its two halves), and long-lived stateful places (the
//! engine, a range's serve loop) hold a [`Clock`] injected at construction. Reading the clock
//! only happens in the shell — the call sites that enter the core.
//!
//! **Two clocks, never mixed.**
//! - **Wall clock** (Unix ms) for every **durable** deadline: timers, retry times, cleanup
//!   times, watchdog rows — they live on the bucket and must mean something to other nodes and
//!   to the next start. Source: [`crate::entropy::now`].
//! - **Monotonic** (`tokio::time::Instant`) for every in-RAM wait: how long to sleep, rate
//!   buckets, background-loop ticks. Source: the tokio runtime's clock.
//!
//! Converting a wall deadline to a monotonic wake-up needs **both readings taken at the same
//! time** — that is [`Now`] and [`Now::mono_at`]; nowhere else may add the two clocks together
//! on its own.
//!
//! **The sim drives both without special code:** [`SystemClock`] reads wall time through
//! `entropy` (the sim installs a 2026-01-01 epoch plus virtual time) and monotonic time through
//! the runtime (the sim runs `start_paused`, so that is virtual time). Outside the sim
//! [`SystemClock`] **is** `SystemTime::now()` and `Instant::now()` — no allocation, no lock;
//! with `testkit`, exactly one extra thread-local read of `entropy`. [`ManualClock`] is for unit
//! tests that want to move time by hand.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use tokio::time::Instant;

/// One reading of both clocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Now {
    /// Wall time since `UNIX_EPOCH` (keeps the sub-millisecond part for [`Self::mono_at`]).
    pub wall: Duration,
    /// The runtime's monotonic clock.
    pub mono: Instant,
}

impl Now {
    /// Reads [`SystemClock`].
    pub fn read() -> Self {
        SystemClock.now()
    }

    /// Wall time, Unix ms.
    pub fn wall_ms(&self) -> u64 {
        self.wall.as_millis() as u64
    }

    /// The monotonic instant at which wall time has reached **past** the deadline `due_ms`, i.e.
    /// when `wall_ms >= due_ms` is certainly true.
    ///
    /// Rounds **up** to the next millisecond: a deadline `d` fires when `now_ms >= d`, so waking
    /// at `d − 0.x ms` is a scan that picks up nothing. A deadline already passed ⇒
    /// `self.mono + 1 ms` or earlier. `None` when adding to `Instant` overflows — a deadline set
    /// by the SDK (`wake_up_time`) can be that far away.
    pub fn mono_at(&self, due_ms: u64) -> Option<Instant> {
        let until_due = Duration::from_millis(due_ms.saturating_add(1)).saturating_sub(self.wall);
        self.mono.checked_add(until_due)
    }
}

/// Injectable time source. See the module docs.
pub trait Clock: Send + Sync + fmt::Debug {
    /// Wall time since `UNIX_EPOCH`.
    fn wall(&self) -> Duration;
    /// Monotonic clock.
    fn mono(&self) -> Instant;
    /// Wall time, Unix ms.
    fn wall_ms(&self) -> u64 {
        self.wall().as_millis() as u64
    }
    /// Both, read back to back.
    fn now(&self) -> Now {
        Now {
            wall: self.wall(),
            mono: self.mono(),
        }
    }
}

/// The clock of a real node — and of the sim, through `entropy` and the virtual runtime.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn wall(&self) -> Duration {
        crate::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
    }

    fn mono(&self) -> Instant {
        Instant::now()
    }
}

/// Hand-driven clock for unit tests: wall time is settable, monotonic time is the instant at
/// construction plus whatever was [`advance`](Self::advance)d. The two clocks move together —
/// `advance` moves both — unless the test deliberately calls [`set_wall_ms`](Self::set_wall_ms)
/// (wall time jumps, like an NTP correction).
#[derive(Debug)]
pub struct ManualClock {
    wall_ms: AtomicU64,
    base: Instant,
    mono_ns: AtomicU64,
}

impl ManualClock {
    /// Wall time `wall_ms`; monotonic time starts at `Instant::now()` at construction.
    pub fn new(wall_ms: u64) -> Self {
        Self {
            wall_ms: AtomicU64::new(wall_ms),
            base: Instant::now(),
            mono_ns: AtomicU64::new(0),
        }
    }

    /// Moves both clocks forward by `d`.
    pub fn advance(&self, d: Duration) {
        self.wall_ms
            .fetch_add(d.as_millis() as u64, Ordering::AcqRel);
        self.mono_ns
            .fetch_add(d.as_nanos() as u64, Ordering::AcqRel);
    }

    /// Sets wall time; monotonic time stands still.
    pub fn set_wall_ms(&self, wall_ms: u64) {
        self.wall_ms.store(wall_ms, Ordering::Release);
    }
}

impl Clock for ManualClock {
    fn wall(&self) -> Duration {
        Duration::from_millis(self.wall_ms.load(Ordering::Acquire))
    }

    fn mono(&self) -> Instant {
        self.base + Duration::from_nanos(self.mono_ns.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wall deadline converted to a monotonic wake-up from **one** reading: a future deadline
    /// ⇒ wake at the start of the millisecond after it (keeping the wall clock's fraction); a past
    /// deadline ⇒ at once; an extremely distant deadline ⇒ no panic (`None` if `Instant`
    /// overflows).
    #[tokio::test(start_paused = true)]
    async fn mono_at_converts_deadline_from_one_read() {
        let c = ManualClock::new(10_000);
        let now = c.now();
        assert_eq!(
            now.mono_at(10_250),
            Some(now.mono + Duration::from_millis(251))
        );
        assert_eq!(
            now.mono_at(10_000),
            Some(now.mono + Duration::from_millis(1))
        );
        assert_eq!(now.mono_at(9_000), Some(now.mono));
        // Whether it overflows depends on the platform (macOS's `Instant` can hold even
        // `u64::MAX` ms); what must hold is no panic and no early wake-up.
        assert!(now
            .mono_at(u64::MAX)
            .is_none_or(|t| t > now.mono + Duration::from_secs(86_400)));
        let frac = Now {
            wall: Duration::from_micros(10_000_700),
            mono: now.mono,
        };
        assert_eq!(frac.wall_ms(), 10_000);
        assert_eq!(
            frac.mono_at(10_250),
            Some(now.mono + Duration::from_micros(250_300))
        );
    }

    /// `advance` moves both clocks; `set_wall_ms` moves only wall time.
    #[tokio::test(start_paused = true)]
    async fn manual_clock_moved_by_hand() {
        let c = ManualClock::new(5);
        let a = c.now();
        c.advance(Duration::from_millis(30));
        let b = c.now();
        assert_eq!(b.wall_ms(), 35);
        assert_eq!(b.mono - a.mono, Duration::from_millis(30));
        c.set_wall_ms(1);
        assert_eq!(c.wall_ms(), 1);
        assert_eq!(c.mono(), b.mono);
    }

    /// `SystemClock` is `entropy::now_ms` plus the runtime's clock.
    #[tokio::test(start_paused = true)]
    async fn system_clock_is_entropy_and_runtime() {
        let t0 = SystemClock.mono();
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(SystemClock.mono() - t0, Duration::from_secs(3));
        let w = SystemClock.wall_ms();
        assert!(w > 1_700_000_000_000);
        assert!(w.abs_diff(crate::entropy::now_ms()) < 60_000);
    }
}
