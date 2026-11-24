// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Failpoint tooling for tests — only with both `testkit` and `failpoints` on.
//!
//! Inspired by TiDB's `pkg/testkit/testfailpoint` (`Enable` removes itself when the test
//! ends, `EnableCall` injects a closure) and `pkg/testkit/stepped.go` (interleaving two
//! threads step by step, with timeouts).
//!
//! ```ignore
//! let ctl = StepController::new();            // scenario lock: tests run sequentially
//! let paused = ctl.pause_at("split::quiesce");
//! let split = tokio::spawn(async move { ops.split(p, None).await });
//! paused.expect_stopped(Duration::from_secs(10)).await; // never reached ⇒ red
//! // ... slip work into the gap between quiesce and clone ...
//! paused.resume();
//! ```
//!
//! **Blocking blocks the thread.** `fail`'s callback is a synchronous function, run right
//! where the point is placed. On a multi-thread runtime it blocks through
//! `tokio::task::block_in_place` (the worker's other tasks move elsewhere); on a
//! single-thread runtime the whole runtime stops — tests using `StepController` must run
//! with `#[tokio::test(flavor = "multi_thread")]`, and a point on the engine thread (its
//! own runtime) stops every range of that thread.
//!
//! Waiting here waits on a real thread (`Condvar`), so time limits are measured with
//! `std::time::Instant`: tokio's clock does not run in a synchronous callback, and this
//! tooling is not used under deterministic simulation.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "the failpoint harness blocks real threads, outside the simulation"
)]

use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

/// The cap on one block. If a `Paused` leaks (a forgotten `resume`, a test panicking
/// midway before the drop runs), the blocked thread goes on by itself after this long,
/// with a stderr line — a hanging test is worse than a red one.
const MAX_HOLD: Duration = Duration::from_secs(120);

/// Configures a failpoint for the guard's lifetime; `Drop` removes it (RAII).
///
/// `actions` uses `fail::cfg` syntax: `return(msg)`, `1*return`, `sleep(100)`,
/// `50%return->off`, `panic`, `pause`, … A syntax error panics — a misconfigured test must
/// not carry on as if the point were on.
///
/// In a multi-node harness, use [`Self::on_nodes`] (or [`StepController::fail_on`]) so the
/// point fires only on the given nodes — the registry is global, so an unconditional point
/// acts on **every** node.
#[must_use = "the failpoint is removed as soon as the guard is dropped"]
pub struct FailGuard {
    // Field order is drop order: remove the point first, then the node restriction —
    // the other way round there would be a gap in which the point fires on every node.
    _point: fail::FailGuard,
    _only: Option<crate::failpoint::NodeOnly>,
}

impl FailGuard {
    pub fn new(name: &str, actions: &str) -> FailGuard {
        FailGuard {
            _point: fail::FailGuard::new(name, actions)
                .unwrap_or_else(|e| panic!("bad failpoint configuration {name}={actions:?}: {e}")),
            _only: None,
        }
    }

    /// Calls `f` every time a thread passes point `name` (TiDB's `EnableCall`).
    pub fn callback(name: &str, f: impl Fn() + Send + Sync + 'static) -> FailGuard {
        FailGuard {
            _point: fail::FailGuard::with_callback(name, f)
                .unwrap_or_else(|e| panic!("bad failpoint callback {name}: {e}")),
            _only: None,
        }
    }

    /// Lets the point fire only on `nodes` (by `node_id`) for the guard's lifetime — see
    /// [`crate::failpoint::only_on`].
    pub fn on_nodes(mut self, name: &str, nodes: &[&str]) -> FailGuard {
        self._only = Some(crate::failpoint::only_on(name, nodes));
        self
    }
}

/// The failpoint controller of **one** test.
///
/// Holds `fail`'s global scenario lock (`FailScenario`): two tests of the same binary
/// never configure the registry at the same time. Starting clears every point (including
/// those of an earlier test that panicked) and loads the `FAILPOINTS` environment
/// variable; `Drop` clears again.
pub struct StepController {
    _scenario: fail::FailScenario<'static>,
}

impl Default for StepController {
    fn default() -> Self {
        Self::new()
    }
}

impl StepController {
    pub fn new() -> StepController {
        assert!(
            crate::failpoint::enabled(),
            "StepController needs the `failpoints` feature — without it every point is off"
        );
        let scenario = fail::FailScenario::setup();
        crate::failpoint::clear_node_limits();
        StepController {
            _scenario: scenario,
        }
    }

    /// Turns `name` on with `actions` until the guard is dropped — see [`FailGuard`].
    pub fn fail(&self, name: &str, actions: &str) -> FailGuard {
        FailGuard::new(name, actions)
    }

    /// Like [`Self::fail`], but only on `nodes`. The restriction is set **before** the
    /// point is turned on: there is no gap in which the point fires on another node.
    pub fn fail_on(&self, nodes: &[&str], name: &str, actions: &str) -> FailGuard {
        let only = crate::failpoint::only_on(name, nodes);
        FailGuard {
            _point: fail::FailGuard::new(name, actions)
                .unwrap_or_else(|e| panic!("bad failpoint configuration {name}={actions:?}: {e}")),
            _only: Some(only),
        }
    }

    /// Like [`Self::pause_at`], but only stops the threads of `nodes`.
    pub fn pause_at_on(&self, nodes: &[&str], name: &str) -> Paused {
        let only = crate::failpoint::only_on(name, nodes);
        let mut paused = self.pause_at(name);
        if let Some(g) = paused.guard.as_mut() {
            g._only = Some(only);
        }
        paused
    }

    /// Stops the **first** thread that reaches `name`, until [`Paused::resume`] (or a
    /// drop). Threads arriving while it is stopped pass through and are only counted
    /// ([`Paused::hits`]).
    pub fn pause_at(&self, name: &str) -> Paused {
        let gate = Arc::new(PauseGate::default());
        let g = Arc::clone(&gate);
        let point = name.to_string();
        let guard = FailGuard::callback(name, move || g.arrive(&point));
        Paused {
            name: name.to_string(),
            gate,
            guard: Some(guard),
        }
    }
}

#[derive(Default)]
struct PauseGate {
    st: Mutex<PauseState>,
    cv: Condvar,
}

#[derive(Default)]
struct PauseState {
    hits: u64,
    holding: bool,
    released: bool,
}

impl PauseGate {
    fn arrive(&self, name: &str) {
        {
            let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
            st.hits += 1;
            if st.released || st.holding {
                return;
            }
            st.holding = true;
            self.cv.notify_all();
        }
        let wait = || {
            let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
            let deadline = Instant::now() + MAX_HOLD;
            while !st.released {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    eprintln!(
                        "failpoint {name}: stopped for over {MAX_HOLD:?} with nobody resuming — going on by itself"
                    );
                    return;
                }
                st = self
                    .cv
                    .wait_timeout(st, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(wait)
            }
            _ => wait(),
        }
    }
}

/// A stop point that was set. Drop ⇒ releases the stopped thread **then** removes the point
/// (this order is mandatory: `fail::remove` waits for a running callback to finish).
#[must_use = "dropping it at once resumes at once"]
pub struct Paused {
    name: String,
    gate: Arc<PauseGate>,
    guard: Option<FailGuard>,
}

impl Paused {
    /// Waits until a thread is stopped at this point. **A timeout is mandatory:** a point
    /// never reached makes the test red (a panic), not hang.
    pub async fn expect_stopped(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .gate
                .st
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .holding
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "failpoint {}: no thread arrived within {timeout:?}",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Number of times a thread has passed the point (including the one stopped now).
    pub fn hits(&self) -> u64 {
        self.gate.st.lock().unwrap_or_else(|e| e.into_inner()).hits
    }

    /// Releases the stopped thread and removes the point.
    pub fn resume(self) {}
}

impl Drop for Paused {
    fn drop(&mut self) {
        {
            let mut st = self.gate.st.lock().unwrap_or_else(|e| e.into_inner());
            st.released = true;
            self.gate.cv.notify_all();
        }
        self.guard.take();
    }
}
