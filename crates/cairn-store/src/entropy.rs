// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The process's sources of nondeterminism — wall time, pid, random bytes — through a single
//! place.
//!
//! Product code calls [`now`], [`process_id`] and [`fill`] instead of `SystemTime::now()`,
//! `std::process::id()` and `getrandom::fill`. By default these three functions **are** those
//! three calls.
//!
//! With the `testkit` feature, the deterministic simulation installs a [`Source`] for the
//! **current thread** with [`install`]: the whole simulated cluster lives on one thread
//! (`current_thread` runtime, inline engines — `NodeConfig::inline_engines`), so a per-thread
//! override covers everything without touching other tests running in parallel in the same
//! process. There is no global lock: the default path only reads one thread-local (when the
//! feature is on) and then falls back to the system call.
//!
//! SlateDB has its own seams (`DbBuilder::with_seed`, `with_system_clock`):
//! [`crate::ObjectStores::open_db`] takes the seed and the clock from here when a source is
//! installed — see [`slatedb_seed`] and [`slatedb_clock`]. So do `Admin`
//! (`config::admin_builder`) and `DbReader` (`reader_trust`): each SlateDB builder has its own
//! RNG (retry jitter, checkpoint ids), and a builder that forgets to seed makes the sim's trace
//! diverge — `scripts/lint-sources.sh` check 9.

// This is the seam `clippy.toml` points to: the only place allowed to call wall time, pid,
// `getrandom` and build std's `HashMap` directly.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "the seam of the nondeterminism sources"
)]

use std::time::SystemTime;

/// Wall time.
pub fn now() -> SystemTime {
    #[cfg(feature = "testkit")]
    if let Some(t) = with_source(|s| s.now()) {
        return t;
    }
    SystemTime::now()
}

/// Unix milliseconds of [`now`].
pub fn now_ms() -> u64 {
    now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The process's pid (used as part of an identifier, not as a key).
pub fn process_id() -> u32 {
    #[cfg(feature = "testkit")]
    if let Some(p) = with_source(|s| s.process_id()) {
        return p;
    }
    std::process::id()
}

/// A random number fixed for the lifetime of the process — the identity of **one start** (the
/// suffix of metering object names: not the pid, because in a container the pid is often the
/// same across starts). Drawn from the operating system on first call.
///
/// With an installed source, it is the source's [`Source::boot_nonce`] — no random bytes are
/// drawn, and no value sticks from a previous run: the sim runs many clusters in one process,
/// one source per cluster.
pub fn boot_nonce() -> u64 {
    #[cfg(feature = "testkit")]
    if let Some(n) = with_source(|s| s.boot_nonce()) {
        return n;
    }
    static BOOT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *BOOT.get_or_init(|| {
        let mut b = [0u8; 8];
        getrandom::fill(&mut b).expect("the operating system's random source is unreadable");
        u64::from_le_bytes(b)
    })
}

/// Fills `buf` with random bytes. Panics if the operating system's source is unreadable — as
/// the earlier `getrandom` call sites did.
pub fn fill(buf: &mut [u8]) {
    #[cfg(feature = "testkit")]
    if with_source(|s| s.fill(buf)).is_some() {
        return;
    }
    getrandom::fill(buf).expect("the operating system's random source is unreadable");
}

/// Seed for `DbBuilder::with_seed` of a new Db — `None` (SlateDB draws from the operating
/// system itself) when no source is installed.
pub fn slatedb_seed() -> Option<u64> {
    #[cfg(feature = "testkit")]
    if let Some(seed) = with_source(|s| {
        let mut b = [0u8; 8];
        s.fill(&mut b);
        u64::from_le_bytes(b)
    }) {
        return Some(seed);
    }
    None
}

/// Clock for `DbBuilder::with_system_clock` — `None` (SlateDB's default clock) when no source
/// is installed.
pub fn slatedb_clock() -> Option<std::sync::Arc<dyn slatedb_common::clock::SystemClock>> {
    #[cfg(feature = "testkit")]
    if let Some(base) = with_source(|s| s.now()) {
        return Some(std::sync::Arc::new(SourceClock::new(base)));
    }
    None
}

#[cfg(feature = "testkit")]
pub use sim::{install, Installed, Source};

#[cfg(feature = "testkit")]
use sim::{with_source, SourceClock};

#[cfg(feature = "testkit")]
mod sim {
    use {
        chrono::{DateTime, Utc},
        slatedb_common::clock::{SystemClock, SystemClockTicker},
        std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, time::Duration, time::SystemTime},
    };

    /// A replacement nondeterminism source, installed per thread with [`install`].
    pub trait Source {
        fn now(&self) -> SystemTime;
        fn process_id(&self) -> u32;
        fn fill(&self, buf: &mut [u8]);
        /// See [`super::boot_nonce`]. By default derived from [`Self::process_id`]: fixed for the
        /// source, draws nothing from [`Self::fill`].
        fn boot_nonce(&self) -> u64 {
            u64::from(self.process_id())
        }
    }

    thread_local! {
        static SOURCE: RefCell<Option<Rc<dyn Source>>> = const { RefCell::new(None) };
    }

    pub(super) fn with_source<T>(f: impl FnOnce(&dyn Source) -> T) -> Option<T> {
        // Take the `Rc` copy out before calling: the source may call back in here.
        let s = SOURCE.with(|s| s.borrow().clone())?;
        Some(f(&*s))
    }

    /// Installs `source` for the current thread until [`Installed`] is dropped (then the old
    /// source — usually none — comes back).
    pub fn install(source: Rc<dyn Source>) -> Installed {
        let prev = SOURCE.with(|s| s.borrow_mut().replace(source));
        Installed { prev }
    }

    /// Xem [`install`].
    #[must_use = "dropping it immediately uninstalls the source immediately"]
    pub struct Installed {
        prev: Option<Rc<dyn Source>>,
    }

    impl Drop for Installed {
        fn drop(&mut self) {
            let prev = self.prev.take();
            SOURCE.with(|s| *s.borrow_mut() = prev);
        }
    }

    /// The sim's SlateDB clock: the wall epoch comes from the source when the Db is opened, and
    /// it advances with `tokio::time::Instant` (the sim runtime's virtual clock) — exactly like
    /// `DefaultSystemClock`, only the epoch differs.
    #[derive(Debug)]
    pub(super) struct SourceClock {
        base: DateTime<Utc>,
        start: tokio::time::Instant,
    }

    impl SourceClock {
        pub(super) fn new(base: SystemTime) -> Self {
            Self {
                base: base.into(),
                start: tokio::time::Instant::now(),
            }
        }
    }

    impl SystemClock for SourceClock {
        fn now(&self) -> DateTime<Utc> {
            self.base + self.start.elapsed()
        }

        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(tokio::time::sleep(duration))
        }

        fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
            SystemClockTicker::new(self, duration)
        }
    }
}

/// The hasher of every `HashMap`/`HashSet` in cairn code ([`HashMap`], [`HashSet`]). Without
/// `testkit` it is std's `RandomState` — a random key per process, resisting HashDoS, as
/// before. With `testkit` it is SipHash with a fixed key: map iteration order is a function of
/// the content, so the deterministic simulation replays in the same order (iteration order
/// decides the order of requests to the store).
#[cfg(not(feature = "testkit"))]
pub type HashState = std::hash::RandomState;
/// See the non-`testkit` version.
#[cfg(feature = "testkit")]
pub type HashState = std::hash::BuildHasherDefault<std::hash::DefaultHasher>;

/// `std::collections::HashMap` with [`HashState`]. Build it with `default()`.
pub type HashMap<K, V> = std::collections::HashMap<K, V, HashState>;
/// `std::collections::HashSet` with [`HashState`]. Build it with `default()`.
pub type HashSet<T> = std::collections::HashSet<T, HashState>;

#[cfg(all(test, feature = "testkit"))]
mod tests {
    use {
        super::*,
        std::{cell::Cell, rc::Rc, time::Duration},
    };

    struct Fixed(Cell<u8>);

    impl Source for Fixed {
        fn now(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(42)
        }

        fn process_id(&self) -> u32 {
            7
        }

        fn fill(&self, buf: &mut [u8]) {
            for b in buf {
                self.0.set(self.0.get().wrapping_add(1));
                *b = self.0.get();
            }
        }
    }

    /// With nothing installed the three functions are system calls; installed, they are the
    /// installed source; dropping the guard restores the old state — and only on the installing
    /// thread.
    #[test]
    fn source_installed_per_thread_and_removed_on_drop() {
        assert!(now_ms() > 1_700_000_000_000);
        assert_eq!(process_id(), std::process::id());
        let boot = boot_nonce();
        assert_eq!(boot_nonce(), boot, "one process ⇒ one boot id");
        assert_eq!(slatedb_seed(), None);
        assert!(slatedb_clock().is_none());
        {
            let _g = install(Rc::new(Fixed(Cell::new(0))));
            assert_eq!(now_ms(), 42_000);
            assert_eq!(process_id(), 7);
            assert_eq!(boot_nonce(), 7, "from the source, no bytes drawn");
            let mut b = [0u8; 3];
            fill(&mut b);
            assert_eq!(b, [1, 2, 3]);
            assert_eq!(
                slatedb_seed(),
                Some(u64::from_le_bytes([4, 5, 6, 7, 8, 9, 10, 11]))
            );
            assert!(slatedb_clock().is_some());
            // Another thread does not see this thread's source.
            let other = std::thread::spawn(process_id).join().unwrap();
            assert_eq!(other, std::process::id());
        }
        assert_eq!(process_id(), std::process::id());
        assert_eq!(boot_nonce(), boot);
        assert!(now_ms() > 1_700_000_000_000);
    }

    /// With `testkit`, [`HashMap`] iteration order depends only on the sequence of operations:
    /// two maps built independently with the same insert sequence (with std, two different
    /// `RandomState`s) iterate identically.
    #[test]
    fn testkit_hashmap_iterates_deterministically() {
        let build = || -> HashMap<String, u32> { (0..64).map(|i| (format!("k{i}"), i)).collect() };
        let a: Vec<_> = build().into_keys().collect();
        let b: Vec<_> = build().into_keys().collect();
        assert_eq!(a, b);
        let s: HashSet<u32> = (0..64).map(|i| i * 7919).collect();
        let t: HashSet<u32> = (0..64).map(|i| i * 7919).collect();
        assert_eq!(s.iter().collect::<Vec<_>>(), t.iter().collect::<Vec<_>>());
    }
}
