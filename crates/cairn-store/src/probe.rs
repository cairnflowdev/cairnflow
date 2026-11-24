// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-stage timestamps for latency research.
//!
//! **Measurement only, off by default.** Without the `probe` feature, [`mark`] is an empty
//! `#[inline(always)]` function — release binaries carry nothing. With the feature but
//! without `CAIRNFLOW_PROBE_OUT` set, each call is one atomic read.
//!
//! With both the feature and the variable: each call pushes `(ns since enabled, name,
//! argument)` into a `Vec` behind a **global** `Mutex`, and an OS thread dumps it to the
//! file every 200 ms (one line `ns name argument`). A global mutex breaks the hot-path rule
//! (AGENTS §6) — acceptable because it only exists in research binaries, and at the
//! measuring load (`xcall` at concurrency 1) there is no contention. Numbers at
//! concurrency 64 on this binary are **not** to be used as throughput.
//!
//! The measuring scenario has only one call in flight at a time, so sorting the marks by
//! time yields the sequential chain of one round; `bench/latency-floor/timeline.py` cuts
//! that chain.

#[cfg(feature = "probe")]
mod imp {
    use std::{
        io::Write,
        sync::{
            atomic::{AtomicU8, Ordering},
            Mutex, OnceLock,
        },
    };

    /// 0 = variable not checked yet, 1 = off, 2 = on.
    static STATE: AtomicU8 = AtomicU8::new(0);
    static EPOCH: OnceLock<tokio::time::Instant> = OnceLock::new();
    static BUF: Mutex<Vec<(u64, &'static str, u64)>> = Mutex::new(Vec::new());

    fn enabled() -> bool {
        match STATE.load(Ordering::Relaxed) {
            1 => false,
            2 => true,
            _ => init(),
        }
    }

    #[cold]
    fn init() -> bool {
        let out = std::env::var_os("CAIRNFLOW_PROBE_OUT").filter(|v| !v.is_empty());
        let on = out.is_some();
        if STATE
            .compare_exchange(
                0,
                if on { 2 } else { 1 },
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            if let Some(path) = out {
                EPOCH.get_or_init(tokio::time::Instant::now);
                std::thread::spawn(move || dump(path));
            }
        }
        STATE.load(Ordering::Relaxed) == 2
    }

    fn dump(path: std::ffi::OsString) {
        let Ok(mut file) = std::fs::File::create(&path) else {
            return;
        };
        loop {
            std::thread::sleep(std::time::Duration::from_millis(200));
            let batch = std::mem::take(&mut *BUF.lock().unwrap_or_else(|e| e.into_inner()));
            let mut s = String::with_capacity(batch.len() * 32);
            for (ns, name, a) in batch {
                use std::fmt::Write as _;
                let _ = writeln!(s, "{ns} {name} {a}");
            }
            let _ = file.write_all(s.as_bytes());
            let _ = file.flush();
        }
    }

    pub fn mark(name: &'static str, a: u64) {
        if !enabled() {
            return;
        }
        let Some(epoch) = EPOCH.get() else {
            return;
        };
        let ns = epoch.elapsed().as_nanos() as u64;
        BUF.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((ns, name, a));
    }
}

/// Records a mark `name` with a number `a` (range, seq, µs…) — see the module header.
#[cfg(feature = "probe")]
pub use imp::mark;

/// Records a mark — does nothing without the `probe` feature.
#[cfg(not(feature = "probe"))]
#[inline(always)]
pub fn mark(_name: &'static str, _a: u64) {}
