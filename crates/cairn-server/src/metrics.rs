// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Counting and exposing metrics in Prometheus text format — observability.
//!
//! No metrics library is pulled in: nine `AtomicU64`s and one function that
//! prints text are the whole need for now, and a dependency is one more thing
//! operators have to trust. Switch to `prometheus`/`metrics` when real
//! histograms are needed.
//!
//! The scope of measurement is **what the HTTP layer sees**: calls through
//! ingress, cancel commands, `/admin/tick` ticks. Invocations progressing in
//! the *background tick* (timers firing, retries) do not pass through here —
//! measuring them means putting counters into the engine thread and changing
//! the signature of `EngineHandle::spawn`; do it when there is a real need
//! instead of guessing the shape in advance. Gauges (partitions, readers,
//! deployments) read state at scrape time, so they are complete.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    /// Calls to `/invoke/...`, counted on receipt.
    pub invoke_requests: AtomicU64,
    /// Outcomes of calls, as seen from ingress.
    pub invoke_completed: AtomicU64,
    pub invoke_waiting: AtomicU64,
    pub invoke_failed: AtomicU64,
    pub invoke_paused: AtomicU64,
    /// Calls answered `429` because the wait limit for a slot ran out.
    pub invoke_throttled: AtomicU64,
    /// Cancel commands that really cancelled (no-ops not counted).
    pub cancelled: AtomicU64,
    /// `/admin/tick` ticks and the total number of invocations advanced by them.
    pub ticks: AtomicU64,
    pub tick_advanced: AtomicU64,
}

impl Metrics {
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

/// The path down to the SDK, counted exactly where it happens.
///
/// **Why process-wide statics rather than fields of [`Metrics`]:** they are
/// bumped from inside the engine thread (`turn/`), which has no path to the
/// struct ingress holds — that thread receives `&mut Engine` and `Ctx`, not
/// HTTP state. Adding one more parameter to ten signatures to count three
/// numbers is the wrong price; and three global `AtomicU64`s have nothing to
/// race on.
///
/// These three numbers answer exactly one question, the question `BIDI_STREAM`
/// exists to answer: *how many round trips down to the SDK does an invocation
/// cost?*
pub mod sdk {
    use {
        super::{AtomicU64, Ordering},
        std::{collections::BTreeMap, sync::Mutex},
    };

    /// A down-to-SDK counter: the whole process's total, and the part split by
    /// range (`RangeId`).
    ///
    /// The split part is a map by range id, not a fixed-size array: range ids
    /// are **not contiguous** and never reused (D22), so after a few splits
    /// they exceed any preset bound. A retired range keeps its row until the
    /// process dies — a Prometheus counter must not go backwards.
    ///
    /// **Why the split is needed.** A process holding many partitions runs many
    /// engines in parallel, so the total cannot answer the question
    /// `BIDI_STREAM` exists to answer — *how many round trips down to the SDK
    /// does an invocation cost?* Subtracting two readings to get the cost of
    /// one invocation requires nobody else adding in between, and in a
    /// multi-partition cluster somebody always does. Splitting by range narrows
    /// "somebody else" down to invocations of the same range.
    ///
    /// **Both** are kept: the total is the existing alerting surface (see
    /// [`Self::total`] and `TIMEOUTS`), the split is the diagnostic surface.
    /// Dropping the total would break someone else's dashboard to gain a thing
    /// they did not ask for.
    pub struct Counter {
        total: AtomicU64,
        per_range: Mutex<BTreeMap<u32, u64>>,
    }

    impl Default for Counter {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Counter {
        pub const fn new() -> Self {
            Self {
                total: AtomicU64::new(0),
                per_range: Mutex::new(BTreeMap::new()),
            }
        }

        /// Adds one, to both the total and the range of the invocation that caused it.
        pub fn bump(&self, range: u32) {
            self.total.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut m) = self.per_range.lock() {
                *m.entry(range).or_default() += 1;
            }
        }

        pub fn total(&self) -> u64 {
            self.total.load(Ordering::Relaxed)
        }

        /// The part of one range. Never bumped ⇒ 0.
        pub fn of(&self, range: u32) -> u64 {
            self.per_range
                .lock()
                .ok()
                .and_then(|m| m.get(&range).copied())
                .unwrap_or(0)
        }

        /// Every non-zero range, by id.
        pub fn per_range(&self) -> Vec<(u32, u64)> {
            self.per_range
                .lock()
                .map(|m| m.iter().map(|(r, v)| (*r, *v)).collect())
                .unwrap_or_default()
        }
    }

    /// `REQUEST_RESPONSE` turns — each one is an HTTP request **and** the
    /// runtime resending the whole journal.
    pub static RR_TURNS: Counter = Counter::new();
    /// `BIDI_STREAM` streams opened — each one is a connection.
    pub static BIDI_STREAMS: Counter = Counter::new();
    /// Turns rerun **inside** an open stream. Each one is exactly one HTTP
    /// round trip and one journal resend that did not have to be paid.
    pub static BIDI_RESUMES: Counter = Counter::new();
    /// Turns dropped because the SDK made no progress within the allowed limit.
    ///
    /// A non-zero value means one of two things: a deployment has a handler
    /// running longer than `--inactivity-timeout-ms` (raise the limit, or move
    /// the long work into a separate invocation), or the SDK is really hung.
    /// Standing still at 0 is the healthy state — so it is worth alerting on,
    /// and therefore its **total** must always be present in `/metrics`, even
    /// when 0.
    pub static TIMEOUTS: Counter = Counter::new();
    /// Times a turn **really waited** for a WAL flush (`Lease::sync` met a
    /// non-durable write). Each one is a flush on an invocation's path — tens
    /// of ms on S3 — so this measures how well serial commits were reduced.
    pub static DURABLE_WAITS: Counter = Counter::new();

    /// The transport mode of a turn, the `mode` label of [`BYTES`].
    #[derive(Clone, Copy)]
    pub enum Mode {
        Bidi = 0,
        Rr = 1,
    }

    /// The direction of the bytes, the `dir` label of [`BYTES`].
    #[derive(Clone, Copy)]
    pub enum Dir {
        ToSdk = 0,
        FromSdk = 1,
    }

    /// HTTP body bytes between the runtime and the customer's endpoint, by
    /// `[mode][dir]`: request bodies the runtime sends (`to_sdk` — the replayed
    /// journal in `REQUEST_RESPONSE`, per-command frames in `BIDI_STREAM`) and
    /// response bodies read (`from_sdk`). This is what the provider's egress
    /// charge (a cloud NAT, a hosting platform) is paid on — the unit for
    /// comparing the two modes, not bytes on the wire (HTTP headers, h2
    /// framing and TLS are not included). Atomics only: counted on every turn's
    /// path.
    pub static BYTES: [[AtomicU64; 2]; 2] = [
        [AtomicU64::new(0), AtomicU64::new(0)],
        [AtomicU64::new(0), AtomicU64::new(0)],
    ];

    pub fn add_bytes(mode: Mode, dir: Dir, n: usize) {
        BYTES[mode as usize][dir as usize].fetch_add(n as u64, Ordering::Relaxed);
    }

    pub fn bytes(mode: Mode, dir: Dir) -> u64 {
        BYTES[mode as usize][dir as usize].load(Ordering::Relaxed)
    }

    /// `cairnflow_sdk_bytes_total{mode,dir}` — all four lines, even when 0.
    pub fn render_bytes(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_sdk_bytes_total";
        let _ = writeln!(
            out,
            "# HELP {name} HTTP body bytes between the runtime and SDK endpoints, by mode and direction"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (mode, m) in [("BIDI_STREAM", Mode::Bidi), ("REQUEST_RESPONSE", Mode::Rr)] {
            for (dir, d) in [("to_sdk", Dir::ToSdk), ("from_sdk", Dir::FromSdk)] {
                let _ = writeln!(
                    out,
                    "{name}{{mode=\"{mode}\",dir=\"{dir}\"}} {}",
                    bytes(m, d)
                );
            }
        }
    }

    /// The replay outcome of a `REQUEST_RESPONSE` turn (wire v5), the `result`
    /// label of `cairnflow_rr_replay_total`: `hit` (delta, the SDK could join
    /// it), `miss` (delta, the SDK missed ⇒ resent in full at once), `full`
    /// (full: no hint, the deployment declares no cache), `disabled` (a hint
    /// existed but `--no-rr-delta-replay`, or the deployment has turned itself
    /// off after many misses).
    #[derive(Clone, Copy)]
    /// `partial`: after a miss, the resend from the part the SDK still holds
    /// (`cairn-replay-held`) hit. Every `409` is a `miss`.
    pub enum Replay {
        Hit = 0,
        Miss = 1,
        Full = 2,
        Disabled = 3,
        Partial = 4,
    }

    pub static REPLAY: [AtomicU64; 5] = [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ];
    /// Bytes of the replayed part **not** sent thanks to delta hits (before compression).
    pub static REPLAY_SAVED_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Times a deployment turned delta off on a range (> 50 % misses / 20 turns).
    pub static RR_REPLAY_AUTO_OFF: AtomicU64 = AtomicU64::new(0);

    pub fn replay(r: Replay) {
        REPLAY[r as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    /// Upper bounds of the `cairnflow_rr_journal_bytes` buckets (bytes of the
    /// replayed part of one RR turn).
    pub const RR_JOURNAL_BUCKETS: [u64; 8] = [
        64 << 10,
        256 << 10,
        1 << 20,
        4 << 20,
        10 << 20,
        16 << 20,
        32 << 20,
        64 << 20,
    ];
    /// Warning threshold for RR journal size (in the style of Temporal): past
    /// it ⇒ one `warn` log per invocation per range term, suggesting moving
    /// large payloads to `ctx.artifact`. No hard cap.
    pub const RR_JOURNAL_WARN_BYTES: u64 = 10 << 20;
    pub static RR_JOURNAL: [AtomicU64; 9] = [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ];
    pub static RR_JOURNAL_SUM: AtomicU64 = AtomicU64::new(0);

    /// An RR turn whose replayed part is `bytes` bytes (before compression, full or delta).
    pub fn note_rr_journal(bytes: u64) {
        let i = RR_JOURNAL_BUCKETS
            .iter()
            .position(|le| bytes <= *le)
            .unwrap_or(RR_JOURNAL_BUCKETS.len());
        RR_JOURNAL[i].fetch_add(1, Ordering::Relaxed);
        RR_JOURNAL_SUM.fetch_add(bytes, Ordering::Relaxed);
    }

    /// `cairnflow_rr_replay_total{result}`, `cairnflow_rr_replay_saved_bytes_total`,
    /// `cairnflow_rr_replay_auto_off_total` — every line, even when 0.
    pub fn render_replay(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_rr_replay_total";
        let _ = writeln!(
            out,
            "# HELP {name} REQUEST_RESPONSE turns by incremental replay outcome (wire v5)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (label, r) in [
            ("hit", Replay::Hit),
            ("miss", Replay::Miss),
            ("full", Replay::Full),
            ("disabled", Replay::Disabled),
            ("partial", Replay::Partial),
        ] {
            let _ = writeln!(
                out,
                "{name}{{result=\"{label}\"}} {}",
                REPLAY[r as usize].load(Ordering::Relaxed)
            );
        }
        for (name, help, c) in [
            (
                "cairnflow_rr_replay_saved_bytes_total",
                "Replay bytes not sent to the SDK thanks to a delta hit (before compression)",
                &REPLAY_SAVED_BYTES,
            ),
            (
                "cairnflow_rr_replay_auto_off_total",
                "Times a deployment turned incremental replay off on a range because of too many misses",
                &RR_REPLAY_AUTO_OFF,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {}", c.load(Ordering::Relaxed));
        }
        let name = "cairnflow_rr_journal_bytes";
        let _ = writeln!(
            out,
            "# HELP {name} Journal replay bytes per REQUEST_RESPONSE turn (before compression)"
        );
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut acc = 0;
        for (i, le) in RR_JOURNAL_BUCKETS.iter().enumerate() {
            acc += RR_JOURNAL[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {acc}");
        }
        acc += RR_JOURNAL[RR_JOURNAL_BUCKETS.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {acc}");
        let _ = writeln!(out, "{name}_sum {}", RR_JOURNAL_SUM.load(Ordering::Relaxed));
        let _ = writeln!(out, "{name}_count {acc}");
    }

    /// Every counter, with name and description — so `render` need not remember the list.
    pub fn all() -> [(&'static str, &'static str, &'static Counter); 5] {
        [
            (
                "cairnflow_sdk_rr_turns",
                "REQUEST_RESPONSE turns: one HTTP request and one journal resend each",
                &RR_TURNS,
            ),
            (
                "cairnflow_sdk_bidi_streams",
                "BIDI_STREAM streams opened to the SDK",
                &BIDI_STREAMS,
            ),
            (
                "cairnflow_sdk_bidi_resumes",
                "Turns re-run on an open stream — round-trip saved",
                &BIDI_RESUMES,
            ),
            (
                "cairnflow_sdk_timeouts",
                "Turns dropped because the SDK made no progress within the allowed limit",
                &TIMEOUTS,
            ),
            (
                "cairnflow_durable_waits",
                "Times a turn waited for a WAL flush tick (sync hit a write not yet durable)",
                &DURABLE_WAITS,
            ),
        ]
    }
}

/// Latency histograms — to see the **p99**, which a total counter cannot tell.
///
/// Hand-written, no library: an array of `AtomicU64` per fixed bucket is
/// enough for the Prometheus format (cumulative `_bucket{le}`, `_sum`,
/// `_count`), for the same reason as the rest of the module — a dependency is
/// one more thing operators have to trust. Buckets are in ms, from 1 ms (a WAL
/// flush on memory/disk) to 10 s (the SDK's default silence limit is 60 s, but
/// a 10 s turn is already an anomaly worth a look).
pub mod latency {
    use super::{AtomicU64, Ordering};

    pub const BUCKETS_MS: [u64; 13] = [
        1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000,
    ];

    pub struct Histogram {
        buckets: [AtomicU64; 13],
        /// Observations beyond the last bucket (`le="+Inf"` = `count`).
        sum_us: AtomicU64,
        count: AtomicU64,
    }

    impl Histogram {
        pub const fn new() -> Self {
            #[allow(clippy::declare_interior_mutable_const)]
            const Z: AtomicU64 = AtomicU64::new(0);
            Self {
                buckets: [Z; 13],
                sum_us: AtomicU64::new(0),
                count: AtomicU64::new(0),
            }
        }

        pub fn observe(&self, d: std::time::Duration) {
            let ms = d.as_millis() as u64;
            if let Some(i) = BUCKETS_MS.iter().position(|b| ms <= *b) {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
            self.sum_us
                .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        pub fn render(&self, out: &mut String, name: &str, help: &str) {
            use std::fmt::Write;
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} histogram");
            let mut acc = 0;
            for (i, b) in BUCKETS_MS.iter().enumerate() {
                acc += self.buckets[i].load(Ordering::Relaxed);
                let _ = writeln!(out, "{name}_bucket{{le=\"{}\"}} {acc}", *b as f64 / 1000.0);
            }
            let count = self.count.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
            let _ = writeln!(
                out,
                "{name}_sum {}",
                self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
            );
            let _ = writeln!(out, "{name}_count {count}");
        }
    }

    impl Histogram {
        /// Like [`Self::render`] but only the sample lines, carrying `labels`
        /// (`k="v"`, no braces) — for a family with many series; the caller
        /// prints HELP/TYPE.
        pub fn render_samples(&self, out: &mut String, name: &str, labels: &str) {
            use std::fmt::Write;
            let mut acc = 0;
            for (i, b) in BUCKETS_MS.iter().enumerate() {
                acc += self.buckets[i].load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "{name}_bucket{{{labels},le=\"{}\"}} {acc}",
                    *b as f64 / 1000.0
                );
            }
            let count = self.count.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{{labels},le=\"+Inf\"}} {count}");
            let _ = writeln!(
                out,
                "{name}_sum{{{labels}}} {}",
                self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
            );
            let _ = writeln!(out, "{name}_count{{{labels}}} {count}");
        }
    }

    impl Default for Histogram {
        fn default() -> Self {
            Self::new()
        }
    }

    /// One **durability wait** (`Lease::sync` met a non-durable write): commit
    /// latency as seen from the turn — the WAL flush plus the queue before it.
    pub static COMMIT: Histogram = Histogram::new();
    /// One `drive` of an invocation, from getting a slot until it stops (done,
    /// suspended, rescheduled) — including every SDK turn of that drive.
    pub static TURN: Histogram = Histogram::new();
    /// How late a timer fires relative to its deadline (target ≤ 20 ms).
    pub static WAKEUP: Histogram = Histogram::new();

    pub fn all() -> [(&'static str, &'static str, &'static Histogram); 3] {
        [
            (
                "cairnflow_commit_wait_seconds",
                "Time a turn waits for a durable commit (WAL flush tick)",
                &COMMIT,
            ),
            (
                "cairnflow_drive_seconds",
                "Time of one invocation run, from getting a slot until it stops",
                &TURN,
            ),
            (
                "cairnflow_timer_lateness_seconds",
                "How late a timer fired relative to its due time",
                &WAKEUP,
            ),
        ]
    }
}

/// HTTP responses of the public port and ack latency: the `5xx` rate and the
/// p99 of work-accepting commands are the first two signals customers see.
/// Counted at the outermost layer of the public router ([`observe`]); the
/// console's static files and the internal port are not counted. Bounded
/// labels: the status class, and work-accepting routes by route pattern (no
/// real path, no tenant).
pub mod http {
    use {
        super::{latency::Histogram, AtomicU64, Ordering},
        axum::{extract::MatchedPath, http::Request, middleware::Next, response::Response},
    };

    const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];
    static RESPONSES: [AtomicU64; 5] = [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ];

    /// Work-accepting commands and their routes. `send`: answers once the
    /// invocation is durable (a pure ack, INV-5); `invoke`: waits for the
    /// outcome up to a limit, then `202` — includes run time; `signal`: a
    /// durable signal into an invocation.
    const ACK_ROUTES: [(&str, &str); 6] = [
        ("/invoke/{service}/{handler}/send", "send"),
        ("/scope/{scope}/invoke/{service}/{handler}/send", "send"),
        ("/invoke/{service}/{handler}", "invoke"),
        ("/scope/{scope}/invoke/{service}/{handler}", "invoke"),
        ("/invocations/{id}/signal", "signal"),
        ("/invocations/batch/{op}", "batch"),
    ];
    const ACK_KINDS: [&str; 4] = ["send", "invoke", "signal", "batch"];
    static ACK: [Histogram; 4] = [
        Histogram::new(),
        Histogram::new(),
        Histogram::new(),
        Histogram::new(),
    ];

    fn ack_kind(path: &str) -> Option<usize> {
        let kind = ACK_ROUTES.iter().find(|(p, _)| *p == path)?.1;
        ACK_KINDS.iter().position(|k| *k == kind)
    }

    /// The outermost layer of the public router: counts every response by
    /// status class, measures time to response for work-accepting commands
    /// answering `2xx`.
    pub async fn observe(req: Request<axum::body::Body>, next: Next) -> Response {
        let kind = req
            .extensions()
            .get::<MatchedPath>()
            .and_then(|p| ack_kind(p.as_str()));
        let start = tokio::time::Instant::now();
        let resp = next.run(req).await;
        let status = resp.status().as_u16();
        if let Some(i) = (status / 100).checked_sub(1).map(usize::from) {
            if let Some(c) = RESPONSES.get(i) {
                c.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let (Some(k), true) = (kind, resp.status().is_success()) {
            ACK[k].observe(start.elapsed());
        }
        resp
    }

    pub(super) fn render(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_http_responses_total";
        let _ = writeln!(
            out,
            "# HELP {name} Responses of the public API (not the console's static files nor the internal port), by status class"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (i, class) in CLASSES.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{class=\"{class}\"}} {}",
                RESPONSES[i].load(Ordering::Relaxed)
            );
        }
        let name = "cairnflow_ack_seconds";
        let _ = writeln!(
            out,
            "# HELP {name} Time from receiving a work request to its 2xx response (send: durable ack; invoke: includes the wait for the result), by route kind"
        );
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (i, kind) in ACK_KINDS.iter().enumerate() {
            ACK[i].render_samples(out, name, &format!("route=\"{kind}\""));
        }
    }

    #[cfg(test)]
    mod tests {
        /// Every work-accepting route of the router is in the table — changing a
        /// route pattern and forgetting the table leaves the histogram mute.
        #[test]
        fn ack_routes_match_the_router() {
            let src = include_str!("ingress/mod.rs");
            for (path, _) in super::ACK_ROUTES {
                assert!(
                    src.contains(&format!("\"{path}\"")),
                    "route {path} is no longer in ingress/mod.rs"
                );
            }
        }
    }
}

/// Requests to the object store by kind of operation — see
/// [`cairn_store::metered`].
fn store_requests(out: &mut String) {
    use {cairn_store::metered::StoreOp, std::fmt::Write};
    let name = "cairnflow_store_requests_total";
    let _ = writeln!(
        out,
        "# HELP {name} Requests sent to the object store by operation (put, list, copy are class A; get, head class B); a list counts once per call"
    );
    let _ = writeln!(out, "# TYPE {name} counter");
    for op in StoreOp::ALL {
        let _ = writeln!(
            out,
            "{name}{{op=\"{}\"}} {}",
            op.as_str(),
            cairn_store::metered::requests(op)
        );
    }
}

/// Machine RAM and disk — see [`crate::host`].
fn host(out: &mut String, h: &crate::host::HostStats) {
    use std::fmt::Write;
    if let Some(v) = h.rss_bytes {
        gauge(
            out,
            "cairnflow_process_resident_bytes",
            "Resident memory of the node process",
            v,
        );
    }
    if let Some(v) = h.memory_limit_bytes {
        gauge(
            out,
            "cairnflow_memory_limit_bytes",
            "Memory available to the node: the cgroup limit if set, else the machine's memory",
            v,
        );
    }
    if !h.disks.is_empty() {
        for (name, help, pick) in [
            (
                "cairnflow_disk_free_bytes",
                "Free bytes for the node on the disk holding a local directory it writes (data, disk_cache)",
                0usize,
            ),
            (
                "cairnflow_disk_total_bytes",
                "Size of the disk holding a local directory the node writes (data, disk_cache)",
                1,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} gauge");
            for (dir, free, total) in &h.disks {
                let v = if pick == 0 { *free } else { *total };
                let _ = writeln!(out, "{name}{{dir=\"{dir}\"}} {v}");
            }
        }
    }
}

/// Inter-range calls and messages — process-wide statics for the same reason
/// as [`sdk`]: bumped from the engine thread (`turn/`) and from
/// [`crate::transport`].
///
/// Only `AtomicU64`s in fixed arrays, indexed by enums: no lock on the turn's
/// path, and labels only carry bounded values — no range ids, no service
/// names. Every label combination is printed even when 0, so the ratio
/// `remote / (local + remote)` always has a series to divide.
pub mod interrange {
    use super::{latency::Histogram, AtomicU64, Ordering};

    /// Which range the callee is in, relative to the caller.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Placement {
        /// Same range: created in place, the id handed over in the same commit.
        Local,
        /// Another range: `StartCall` goes through the transport.
        Remote,
    }

    impl Placement {
        pub const ALL: [Self; 2] = [Self::Local, Self::Remote];

        pub fn label(self) -> &'static str {
            match self {
                Self::Local => "local",
                Self::Remote => "remote",
            }
        }
    }

    /// The tier of the pk derivation table the call falls into
    /// (`partition_key_for_target`, then `partition_key_for_callee`), plus a
    /// workflow's `run` (the id is a function of `(scope, service, key)`).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum KeyClass {
        Scoped,
        Object,
        Idempotent,
        Workflow,
        /// No scope, no object key, no idempotency key: pk by
        /// `(caller, completion)` — the tier that call-tree colocation groups
        /// together.
        Unkeyed,
    }

    impl KeyClass {
        pub const ALL: [Self; 5] = [
            Self::Scoped,
            Self::Object,
            Self::Idempotent,
            Self::Workflow,
            Self::Unkeyed,
        ];

        pub fn label(self) -> &'static str {
            match self {
                Self::Scoped => "scoped",
                Self::Object => "object",
                Self::Idempotent => "idempotent",
                Self::Workflow => "workflow",
                Self::Unkeyed => "unkeyed",
            }
        }
    }

    /// Kind of inter-range message — one to one with `cairn_engine::InterPartition`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum MessageKind {
        StartCall,
        Completion,
        Attach,
        Peek,
        Signal,
        Cancel,
    }

    impl MessageKind {
        pub const ALL: [Self; 6] = [
            Self::StartCall,
            Self::Completion,
            Self::Attach,
            Self::Peek,
            Self::Signal,
            Self::Cancel,
        ];

        pub fn of(msg: &cairn_engine::InterPartition) -> Self {
            use cairn_engine::InterPartition as M;
            match msg {
                M::StartCall(_) => Self::StartCall,
                M::Completion(_) => Self::Completion,
                M::Attach(_) => Self::Attach,
                M::PeekOutput(_) => Self::Peek,
                M::Signal(_) => Self::Signal,
                M::Cancel(_) => Self::Cancel,
            }
        }

        pub fn label(self) -> &'static str {
            match self {
                Self::StartCall => "start_call",
                Self::Completion => "completion",
                Self::Attach => "attach",
                Self::Peek => "peek",
                Self::Signal => "signal",
                Self::Cancel => "cancel",
            }
        }
    }

    /// The outcome of **one** `Transport::send` (including resends after
    /// learning a new table within the same send).
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Outcome {
        /// The receiver wrote it durably.
        Ok,
        /// The receiver returned an error status (`4xx`/`5xx` other than `409 wrong_range`).
        Rejected,
        /// `409 wrong_range` while the receiver's table is not newer than ours.
        WrongRange,
        /// Unreachable: leader unknown, the leader declares no address, an HTTP
        /// error, or `SEND_TIMEOUT` ran out.
        Unreachable,
    }

    impl Outcome {
        pub const ALL: [Self; 4] = [
            Self::Ok,
            Self::Rejected,
            Self::WrongRange,
            Self::Unreachable,
        ];

        pub fn label(self) -> &'static str {
            match self {
                Self::Ok => "ok",
                Self::Rejected => "rejected",
                Self::WrongRange => "wrong_range",
                Self::Unreachable => "unreachable",
            }
        }
    }

    #[allow(clippy::declare_interior_mutable_const)]
    const Z: AtomicU64 = AtomicU64::new(0);
    #[allow(clippy::declare_interior_mutable_const)]
    const CALL_ROW: [AtomicU64; 5] = [Z; 5];
    #[allow(clippy::declare_interior_mutable_const)]
    const MESSAGE_ROW: [AtomicU64; 4] = [Z; 4];

    static CALLS: [[AtomicU64; 5]; 2] = [CALL_ROW; 2];
    static MESSAGES: [[AtomicU64; 4]; 6] = [MESSAGE_ROW; 6];

    /// The outbox drainer's `engine.sync()` before a send (`sync_and_send`):
    /// waiting for the outbox row — and every write before it — to be durable.
    pub static SYNC: Histogram = Histogram::new();
    /// One send of the per-destination send task (outbox or signal), engine
    /// released: every HTTP request of the messages handed to it (one batch,
    /// or a few consecutive batches) plus the durable write at the receiver.
    /// Retaking the engine is not included.
    pub static SEND: Histogram = Histogram::new();
    /// Messages per **durable** inbound commit at the receiver: `1` for
    /// per-message routing, `k` for a batch (`/internal/inbound/batch`).
    /// Recorded before the ack goes out.
    pub static BATCH: Sizes = Sizes::new();

    /// Outbox drainer counters: drain passes, rows read from the store, in-place
    /// lock attempts that failed because the lock was busy, rows finished. The
    /// ratio rows read / rows finished measures how much blocked rows are
    /// re-read.
    pub mod drain {
        use super::{AtomicU64, Ordering};

        pub static PASSES: AtomicU64 = AtomicU64::new(0);
        pub static ROWS_READ: AtomicU64 = AtomicU64::new(0);
        pub static LOCK_BUSY: AtomicU64 = AtomicU64::new(0);
        pub static ROWS_HANDLED: AtomicU64 = AtomicU64::new(0);
        /// Inbound / signal rows the drainer read from the store: measures how
        /// much rows left in place (busy destination, undecided, L0 throttle)
        /// are re-read.
        pub static INBOUND_ROWS_READ: AtomicU64 = AtomicU64::new(0);
        pub static SIGNAL_ROWS_READ: AtomicU64 = AtomicU64::new(0);

        pub fn add(c: &AtomicU64, n: u64) {
            c.fetch_add(n, Ordering::Relaxed);
        }

        pub(crate) fn render(out: &mut String) {
            use std::fmt::Write;
            for (name, help, c) in [
                (
                    "cairnflow_outbox_drain_passes_total",
                    "Outbox drain passes run",
                    &PASSES,
                ),
                (
                    "cairnflow_outbox_rows_read_total",
                    "Outbox rows the drainer read from the store",
                    &ROWS_READ,
                ),
                (
                    "cairnflow_outbox_lock_busy_total",
                    "Times the drainer tried to lock an outbox row in place while the lock was busy",
                    &LOCK_BUSY,
                ),
                (
                    "cairnflow_outbox_rows_handled_total",
                    "Outbox rows the drainer finished: in place, or after the receiver's ack",
                    &ROWS_HANDLED,
                ),
                (
                    "cairnflow_inbound_rows_read_total",
                    "Inbound rows the drainer read from the store",
                    &INBOUND_ROWS_READ,
                ),
                (
                    "cairnflow_signal_rows_read_total",
                    "Signal outbox rows the drainer read from the store",
                    &SIGNAL_ROWS_READ,
                ),
            ] {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} counter");
                let _ = writeln!(out, "{name} {}", c.load(Ordering::Relaxed));
            }
        }
    }

    /// Buckets of [`Sizes`]: powers of 2 up to the batch cap (`MAX_BATCH_MESSAGES`).
    pub const SIZE_BUCKETS: [u64; 7] = [1, 2, 4, 8, 16, 32, 64];

    /// A histogram of an integer (batch size) — the same shape as
    /// [`super::latency::Histogram`], with buckets by message count.
    pub struct Sizes {
        buckets: [AtomicU64; 7],
        sum: AtomicU64,
        count: AtomicU64,
    }

    impl Sizes {
        pub const fn new() -> Self {
            Self {
                buckets: [Z; 7],
                sum: AtomicU64::new(0),
                count: AtomicU64::new(0),
            }
        }

        pub fn observe(&self, n: u64) {
            if let Some(i) = SIZE_BUCKETS.iter().position(|b| n <= *b) {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
            self.sum.fetch_add(n, Ordering::Relaxed);
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        pub fn render(&self, out: &mut String, name: &str, help: &str) {
            use std::fmt::Write;
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} histogram");
            let mut acc = 0;
            for (i, b) in SIZE_BUCKETS.iter().enumerate() {
                acc += self.buckets[i].load(Ordering::Relaxed);
                let _ = writeln!(out, "{name}_bucket{{le=\"{b}\"}} {acc}");
            }
            let count = self.count.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
            let _ = writeln!(out, "{name}_sum {}", self.sum.load(Ordering::Relaxed));
            let _ = writeln!(out, "{name}_count {count}");
        }
    }

    impl Default for Sizes {
        fn default() -> Self {
            Self::new()
        }
    }

    /// A call that handed an id to the caller.
    pub fn call(p: Placement, k: KeyClass) {
        CALLS[p as usize][k as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn calls(p: Placement, k: KeyClass) -> u64 {
        CALLS[p as usize][k as usize].load(Ordering::Relaxed)
    }

    pub fn message(kind: MessageKind, outcome: Outcome) {
        MESSAGES[kind as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn messages(kind: MessageKind, outcome: Outcome) -> u64 {
        MESSAGES[kind as usize][outcome as usize].load(Ordering::Relaxed)
    }

    pub fn render(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_calls_total";
        let _ = writeln!(
            out,
            "# HELP {name} ctx.call/ctx.send calls that handed an id to the caller, by callee placement and key tier"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for p in Placement::ALL {
            for k in KeyClass::ALL {
                let _ = writeln!(
                    out,
                    "{name}{{placement=\"{}\",key_class=\"{}\"}} {}",
                    p.label(),
                    k.label(),
                    calls(p, k)
                );
            }
        }
        let name = "cairnflow_interrange_messages_total";
        let _ = writeln!(
            out,
            "# HELP {name} Cross-range messages this node sent, by kind and outcome"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for kind in MessageKind::ALL {
            for o in Outcome::ALL {
                let _ = writeln!(
                    out,
                    "{name}{{kind=\"{}\",outcome=\"{}\"}} {}",
                    kind.label(),
                    o.label(),
                    messages(kind, o)
                );
            }
        }
        SYNC.render(
            out,
            "cairnflow_interrange_sync_seconds",
            "Outbox drainer waiting for durable outbox rows before a cross-range send",
        );
        SEND.render(
            out,
            "cairnflow_interrange_send_seconds",
            "One cross-range send of the per-destination send task, engine released: the HTTP requests plus the durable write at the receiver",
        );
        BATCH.render(
            out,
            "cairnflow_interrange_batch_messages",
            "Messages per durable inbound commit at the receiver: 1 for per-message routing, k for a batch",
        );
        drain::render(out);
    }
}

/// The latency floor of `xcall`: the polling of `/invoke`, what wakes it, and
/// whether the drainer has to wait for durability before a send. Bounded
/// labels, no range ids.
pub mod floor {
    use super::{AtomicU64, Ordering};

    /// The answer to one `Command::Settled` query from `wait_for_settled`.
    #[derive(Clone, Copy, Debug)]
    pub enum Answer {
        /// "Not done" — ask again.
        Pending,
        /// An outcome (`completed`/`failed`/`paused`) — returned to the client.
        Final,
        /// An error while asking (leader lost, a range nobody leads, …).
        Error,
    }

    /// What woke `wait_for_settled` up to ask.
    #[derive(Clone, Copy, Debug)]
    pub enum Wakeup {
        /// The grid tick, 5 → 50 ms.
        Grid,
        /// The outcome notifier.
        Notify,
    }

    /// A drain round's send ticket: ready (everything durable) or had to wait.
    #[derive(Clone, Copy, Debug)]
    pub enum Ticket {
        Ready,
        Wait,
    }

    static POLLS: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
    static WAKEUPS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
    static SEND_WAITS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

    pub fn poll(a: Answer) {
        POLLS[a as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn wakeup(w: Wakeup) {
        WAKEUPS[w as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn send_wait(t: Ticket) {
        SEND_WAITS[t as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Times an invariant tripwire caught a violation and carried on (not
    /// sending that round) — bounded `check`: `ir1` (LF-1).
    static VIOLATIONS_IR1: AtomicU64 = AtomicU64::new(0);

    pub fn invariant_violation(check: &'static str) {
        match check {
            "ir1" => VIOLATIONS_IR1.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Waiters registered in the waiter registries of every range on the node:
    /// a guard created `+1`, a guard dropped `-1`. Back to 0 when no `/invoke`
    /// is waiting.
    static WAITERS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

    pub fn waiters_add(n: i64) {
        WAITERS.fetch_add(n, Ordering::Relaxed);
    }

    /// The current value of the waiter gauge — for the "no leak" test.
    pub fn waiters() -> i64 {
        WAITERS.load(Ordering::Relaxed)
    }

    pub fn render(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_invoke_waiters";
        let _ = writeln!(
            out,
            "# HELP {name} /invoke waiters registered with the outcome waiter registries of the ranges on this node"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {}", waiters());
        let name = "cairnflow_invariant_violations_total";
        let _ = writeln!(
            out,
            "# HELP {name} Invariant violations caught by a tripwire in a release build (that round is not sent)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(
            out,
            "{name}{{check=\"ir1\"}} {}",
            VIOLATIONS_IR1.load(Ordering::Relaxed)
        );
        for (name, help, label, values, cells) in [
            (
                "cairnflow_invoke_polls_total",
                "Times /invoke asked a range whether it is done, by answer",
                "answer",
                &["pending", "final", "error"][..],
                &POLLS[..],
            ),
            (
                "cairnflow_invoke_wakeups_total",
                "Times /invoke woke up to ask, by what woke it: grid tick or outcome notifier",
                "by",
                &["grid", "notify"][..],
                &WAKEUPS[..],
            ),
            (
                "cairnflow_drain_send_waits_total",
                "Drainer send rounds by durable ticket: ready or had to wait",
                "ticket",
                &["ready", "wait"][..],
                &SEND_WAITS[..],
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            for (v, c) in values.iter().zip(cells) {
                let _ = writeln!(
                    out,
                    "{name}{{{label}=\"{v}\"}} {}",
                    c.load(Ordering::Relaxed)
                );
            }
        }
    }
}

/// Flow control, counted inside the engine thread — process-wide statics for
/// the same reason as [`sdk`].
///
/// **Per-queue statistics are NOT persisted**, and that is a choice rather
/// than an omission: INV-3 says *pending work* must live in the state tables,
/// and statistics are not pending work. With a RocksDB merge operator
/// persisting them would be nearly free; in cairn every increment of a number
/// is a commit through the object store. Kept in RAM, lost on a leader
/// change, and said so plainly on the endpoint.
///
/// Labels are by **kind** of resource, not by concrete value: scopes are
/// user-chosen strings, and a metric label chosen by outsiders is a way to
/// blow up Prometheus. To see who blocks whom, ask `GET /vqueues`.
pub mod flow {
    use super::{AtomicU64, Ordering};

    /// Live tasks running the node's background turns (gauge): the sum of
    /// [`crate::flow::SchedulerRow::turn_tasks`] over every range.
    pub static TURN_TASKS: AtomicU64 = AtomicU64::new(0);
    /// Parked tickets of background turns in the scheduler's queue (gauge): the
    /// sum of [`crate::flow::SchedulerRow::parked`] over every range — slot
    /// waiters are data, not tasks.
    pub static PARKED: AtomicU64 = AtomicU64::new(0);
    /// Turns dropped because the wait limit for a slot ran out — rescheduled.
    ///
    /// Non-zero means some tenant is hitting its cap, or the partition's turn
    /// cap is full. That is **billing information**, not an error.
    pub static THROTTLED: AtomicU64 = AtomicU64::new(0);
    /// Turns blocked at a resource, by kind.
    pub static BLOCKED_LOCK: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKED_CONCURRENCY: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKED_TURNS: AtomicU64 = AtomicU64::new(0);
    /// Turns refused a slot because the tenant is `suspended`.
    pub static BLOCKED_TENANT: AtomicU64 = AtomicU64::new(0);
    /// Child turns that **already had a slot** stopped right before calling
    /// the endpoint because the tenant was just suspended: a slot granted
    /// before the suspension, a turn waiting for durability/the engine, or a
    /// follow-up turn of the same run. Non-zero is normal around a suspension;
    /// rising steadily when nobody suspends any tenant is a bug.
    pub static REFUSED_AT_CALL: AtomicU64 = AtomicU64::new(0);
    /// Turns blocked because the tenant hit its turn cap on the node
    /// (`--max-turns-per-tenant`).
    pub static BLOCKED_TENANT_TURNS: AtomicU64 = AtomicU64::new(0);
    /// Slots granted.
    pub static GRANTED: AtomicU64 = AtomicU64::new(0);
    /// Total time spent waiting for slots, milliseconds — divide by
    /// [`GRANTED`] for the average.
    ///
    /// Not a histogram: a real histogram needs a metrics library, and this
    /// module deliberately has none (see the file head). Total + count answer
    /// the real operational question ("are waits getting longer") without
    /// pulling in one more thing to trust.
    pub static WAIT_MS_TOTAL: AtomicU64 = AtomicU64::new(0);
    /// **Due** retry appointments where the background tick found the
    /// invocation busy (a task holding its lock — usually waiting for a slot).
    /// Each one is a scanned row + a read on the engine thread that makes no
    /// progress; rising steadily with the number of ticks is the sign of a
    /// rescheduling storm.
    pub static RETRY_BUSY: AtomicU64 = AtomicU64::new(0);
    /// The term's rescue net: times an error path marked "a wake-up may have
    /// been lost". Non-zero means the store (or the turn path) failed at a
    /// point where the invocation used to stay stuck until the next leader
    /// change.
    pub static RESCUE_MARKED: AtomicU64 = AtomicU64::new(0);
    /// The part of [`RESCUE_MARKED`] caused by the **default nack**: a turn
    /// ended — returning `Err` without leaving an appointment, panicking,
    /// cancelled mid-term — without acking its wake-up duty
    /// ([`crate::drainer::WakeDuty`]).
    pub static RESCUE_NACKED: AtomicU64 = AtomicU64::new(0);
    /// Invocations the rescue net found `running` with nothing left to wake
    /// them, and handed over again — each one is a rescued invocation.
    pub static RESCUED: AtomicU64 = AtomicU64::new(0);
    /// The rescue net's reconciliation round failed (a store read failed) — marked again.
    pub static RESCUE_FAILED: AtomicU64 = AtomicU64::new(0);
    /// Inbound/signal rows that cannot be decoded: **kept**, not deleted —
    /// counted once per row per term. Non-zero means messages written by a
    /// newer binary, or corruption on the store; whoever waits for them (if
    /// known) is already `paused` 577.
    pub static POISON_ROWS: AtomicU64 = AtomicU64::new(0);
    /// Invocations the rescue net moved to `paused` 577: handed over again
    /// past the threshold while the turn still failed before leaving a durable
    /// appointment.
    pub static INFRA_STUCK: AtomicU64 = AtomicU64::new(0);

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
}

/// The drainer "writes then delivers" completions to the caller: how many
/// deliveries, and how many of them had to reload the caller's journal from
/// the store and replay it. The ratio `replays / settled` is the O(J) cost per
/// completion that write-then-deliver removes: 2 for inbound messages
/// before it, 1 after.
pub mod settle {
    use super::{AtomicU64, Ordering};

    /// Deliveries of a completion through the drainer path
    /// (`settle_completion`, `deliver_inbound_completion`), including those
    /// where `Engine::deliver` saw a duplicate and only cleaned up the source
    /// row.
    pub static SETTLED: AtomicU64 = AtomicU64::new(0);
    /// Times that path loaded the caller's journal from the store (`Engine::load`).
    pub static REPLAYS: AtomicU64 = AtomicU64::new(0);
    /// The part of [`SETTLED`] delivered while **keeping** the caller
    /// `suspended`: the wait tree of its last suspension is not satisfied yet,
    /// no SDK turn.
    pub static HELD: AtomicU64 = AtomicU64::new(0);
    /// The warm machine by outcome: `hit`, `cold` (no machine), `stale` (a
    /// machine whose journal tail does not match — dropped, loaded cold),
    /// `evict` (dropped for the range's byte cap). `stale` is only non-zero
    /// when the journal changed outside the delivery path between two
    /// deliveries — an interleaved turn, a delivery commit that did not reach
    /// the store.
    static WARM: [AtomicU64; 4] = [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ];
    const WARM_LABELS: [&str; 4] = ["hit", "cold", "stale", "evict"];

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    /// Where a delivery's machine came from; every non-`hit` is a journal load
    /// ([`REPLAYS`]).
    pub fn warm(how: cairn_engine::WarmUse) {
        use cairn_engine::WarmUse as W;
        let i = match how {
            W::Hit => 0,
            W::Cold => 1,
            W::Stale => 2,
        };
        WARM[i].fetch_add(1, Ordering::Relaxed);
        if how != W::Hit {
            bump(&REPLAYS);
        }
    }

    /// Warm machines dropped for the byte cap.
    pub fn evicted(n: u64) {
        WARM[3].fetch_add(n, Ordering::Relaxed);
    }

    pub fn render(out: &mut String) {
        super::counter(
            out,
            "cairnflow_settle_total",
            "Completions the drainer delivered to the caller (write then deliver)",
            SETTLED.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_settle_replays_total",
            "Completion deliveries that had to reload and replay the caller's journal from the store",
            REPLAYS.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_settle_held_total",
            "Delivered completions that left the caller suspended because its wait tree is not yet satisfied",
            HELD.load(Ordering::Relaxed),
        );
        use std::fmt::Write;
        let name = "cairnflow_warm_machine_total";
        let _ = writeln!(
            out,
            "# HELP {name} Caller warm machine on the completion delivery path: hit, cold, stale (journal tail mismatch), evict (byte cap)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (label, c) in WARM_LABELS.iter().zip(&WARM) {
            let _ = writeln!(
                out,
                "{name}{{outcome=\"{label}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }
    }
}

/// Agent primitives: streams, `$log`/`$trace`, artifacts, and follower readers
/// by opening reason.
///
/// Process-wide statics like [`sdk`]: streams and artifacts are counted in the
/// engine thread (`turn/`) and in background tasks, with no path to
/// [`Metrics`]. Every family printed is always present (even at 0), so
/// dashboards and alerts (`ops_pack`) can reference it before the feature is
/// on.
/// Per-range query index state:
/// `cairnflow_query_index_state{index, state}` = the number of ranges the node
/// holds with descriptor `index` in `state` (`readable`/`building`). The
/// background build loop records it at the start and at the end of each term.
pub mod query_index {
    use std::{collections::BTreeMap, sync::Mutex};

    /// `(descriptor name, readable)` of each range — a process may run several
    /// nodes (tests), so printing filters by the ranges the node holds, like
    /// [`super::schedules`].
    static PER_RANGE: Mutex<BTreeMap<u32, Vec<(&'static str, bool)>>> = Mutex::new(BTreeMap::new());

    pub fn set_range(range: u32, states: Vec<(&'static str, bool)>) {
        if let Ok(mut m) = PER_RANGE.lock() {
            m.insert(range, states);
        }
    }

    pub fn forget_range(range: u32) {
        if let Ok(mut m) = PER_RANGE.lock() {
            m.remove(&range);
        }
    }

    /// `(index, state, number of ranges)` over the ranges `led` says the node
    /// holds; every descriptor seen has both lines (0 when no range).
    pub fn snapshot(led: impl Fn(u32) -> bool) -> Vec<(&'static str, &'static str, u64)> {
        let mut out: BTreeMap<&'static str, [u64; 2]> = BTreeMap::new();
        if let Ok(m) = PER_RANGE.lock() {
            for (r, states) in m.iter() {
                for (ix, readable) in states {
                    let row = out.entry(ix).or_default();
                    if led(*r) {
                        row[usize::from(!*readable)] += 1;
                    }
                }
            }
        }
        out.into_iter()
            .flat_map(|(ix, [r, b])| [(ix, "readable", r), (ix, "building", b)])
            .collect()
    }

    pub fn render(out: &mut String, rows: &[(&'static str, &'static str, u64)]) {
        use std::fmt::Write;
        if rows.is_empty() {
            return;
        }
        let name = "cairnflow_query_index_state";
        let _ = writeln!(
            out,
            "# HELP {name} Ranges on this node with query index descriptors in each state (readable: usable by the path chooser; building: background build not finished)"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        for (ix, state, n) in rows {
            let _ = writeln!(out, "{name}{{index=\"{ix}\",state=\"{state}\"}} {n}");
        }
    }
}

/// Server-side schedules: the outcome of each fire time, fire latency, the
/// number of schedules of the ranges the node holds.
pub mod schedules {
    use {
        super::{AtomicU64, Ordering},
        std::{collections::BTreeMap, sync::Mutex},
    };

    /// The `outcome` label of `cairnflow_schedule_fires_total`.
    /// `refused`: the tenant is `limited`/`suspended`.
    pub const OUTCOMES: [&str; 5] = ["fired", "skipped", "missed", "throttled", "refused"];
    static FIRES: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
    static LAG_MS_SUM: AtomicU64 = AtomicU64::new(0);
    static LAG_COUNT: AtomicU64 = AtomicU64::new(0);
    /// Upper bounds (ms) of the fire-latency histogram buckets; beyond them is the `+Inf` bucket.
    const LAG_BOUNDS_MS: [u64; 9] = [
        100, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 300_000,
    ];
    static LAG_BUCKETS: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];
    /// Number of schedules per range, recorded when a range opens and on
    /// `PUT`/`DELETE` — **not** on the fire path (the hot path only touches
    /// the atomics above).
    static PER_RANGE: Mutex<BTreeMap<u32, u64>> = Mutex::new(BTreeMap::new());

    /// One fire time with outcome `outcome` (one of [`OUTCOMES`]).
    pub fn fire(outcome: &str) {
        if let Some(i) = OUTCOMES.iter().position(|o| *o == outcome) {
            FIRES[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `n` missed fire times collapsed into one firing.
    pub fn missed(n: u64) {
        FIRES[2].fetch_add(n, Ordering::Relaxed);
    }

    /// Latency from the fire deadline to the firing, ms.
    pub fn lag(ms: u64) {
        LAG_MS_SUM.fetch_add(ms, Ordering::Relaxed);
        LAG_COUNT.fetch_add(1, Ordering::Relaxed);
        if let Some(i) = LAG_BOUNDS_MS.iter().position(|b| ms <= *b) {
            LAG_BUCKETS[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of schedules of range `range` (when the range opens).
    pub fn set_range(range: u32, n: u64) {
        if let Ok(mut m) = PER_RANGE.lock() {
            m.insert(range, n);
        }
    }

    /// Range `range` is no longer on this node (retired, yielded).
    pub fn forget_range(range: u32) {
        if let Ok(mut m) = PER_RANGE.lock() {
            m.remove(&range);
        }
    }

    /// A new schedule (`+1`) or one just deleted (`-1`) on range `range`.
    pub fn adjust(range: u32, delta: i64) {
        if let Ok(mut m) = PER_RANGE.lock() {
            let v = m.entry(range).or_default();
            *v = v.saturating_add_signed(delta);
        }
    }

    /// Total schedules of the ranges `led` says the node holds.
    pub fn total(led: impl Fn(u32) -> bool) -> u64 {
        PER_RANGE
            .lock()
            .map(|m| m.iter().filter(|(r, _)| led(**r)).map(|(_, v)| *v).sum())
            .unwrap_or(0)
    }

    pub fn render(out: &mut String, schedules: u64) {
        use std::fmt::Write;
        super::gauge(
            out,
            "cairnflow_schedules",
            "Server-side schedules held by the ranges on this node",
            schedules,
        );
        let name = "cairnflow_schedule_fires_total";
        let _ = writeln!(
            out,
            "# HELP {name} Schedule ticks by outcome: fired, skipped (overlap skip), missed (folded into one firing), throttled (L0 throttle, fires later)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (o, c) in OUTCOMES.iter().zip(&FIRES) {
            let _ = writeln!(
                out,
                "{name}{{outcome=\"{o}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }
        let name = "cairnflow_schedule_fire_lag_seconds";
        let _ = writeln!(
            out,
            "# HELP {name} Delay from a schedule's due time to its firing"
        );
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cum = 0;
        for (b, c) in LAG_BOUNDS_MS.iter().zip(&LAG_BUCKETS) {
            cum += c.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{}\"}} {cum}", *b as f64 / 1000.0);
        }
        let _ = writeln!(
            out,
            "{name}_bucket{{le=\"+Inf\"}} {}",
            LAG_COUNT.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "{name}_sum {:.3}",
            LAG_MS_SUM.load(Ordering::Relaxed) as f64 / 1000.0
        );
        let _ = writeln!(out, "{name}_count {}", LAG_COUNT.load(Ordering::Relaxed));
    }
}

/// The `tenants` registry and per-tenant caps.
/// The data-plane audit log.
pub mod audit {
    use super::{counter, AtomicU64, Ordering};

    /// `Table::Audit` rows exported as objects.
    pub static EXPORTED: AtomicU64 = AtomicU64::new(0);
    /// Audit objects that already had other content (must never happen with
    /// deterministic names) — the row is kept.
    pub static CONFLICTS: AtomicU64 = AtomicU64::new(0);

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    pub(super) fn render(out: &mut String) {
        let v = |c: &AtomicU64| c.load(Ordering::Relaxed);
        counter(
            out,
            "cairnflow_audit_exported_total",
            "Audit rows in the range exported as objects",
            v(&EXPORTED),
        );
        counter(
            out,
            "cairnflow_audit_conflicts_total",
            "Audit object already had different content — row kept, verify reports it",
            v(&CONFLICTS),
        );
    }
}

/// Authentication failures at ingress: only counted, with one merged log line
/// per minute — a Bearer `401` is **not** blocked by IP (a 256-bit key cannot
/// be guessed; behind a proxy, blocking by IP hurts customers behind a shared
/// NAT).
pub mod auth_failures {
    use super::{AtomicU64, Ordering};

    /// Kind of failure.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Kind {
        /// `Authorization: Bearer` carrying an invalid key.
        Bearer,
        /// A console session cookie that is wrong, expired or revoked.
        Cookie,
        /// No credentials at all on a route that needs a key.
        Missing,
        /// `POST <base>/session` with a wrong key.
        Login,
    }

    impl Kind {
        pub const ALL: [Kind; 4] = [Kind::Bearer, Kind::Cookie, Kind::Missing, Kind::Login];

        fn label(self) -> &'static str {
            match self {
                Kind::Bearer => "bearer",
                Kind::Cookie => "cookie",
                Kind::Missing => "missing",
                Kind::Login => "login",
            }
        }
    }

    static COUNTS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
    /// Requests answered `429 key_rate_limited`.
    pub static KEY_RATE_LIMITED: AtomicU64 = AtomicU64::new(0);
    /// The second of the latest merged log line, and the total logged up to then.
    static LOGGED_AT_S: AtomicU64 = AtomicU64::new(0);
    static LOGGED_TOTAL: AtomicU64 = AtomicU64::new(0);
    /// Interval of the merged log line.
    const LOG_EVERY_S: u64 = 60;

    pub fn get(kind: Kind) -> u64 {
        COUNTS[kind as usize].load(Ordering::Relaxed)
    }

    /// Counts one failure; more than a minute after the previous log line,
    /// logs one `warn` line merging every failure since — a probing burst
    /// does not become a logging burst.
    pub fn note(kind: Kind) {
        COUNTS[kind as usize].fetch_add(1, Ordering::Relaxed);
        let now = crate::auth::now_s();
        let last = LOGGED_AT_S.load(Ordering::Relaxed);
        if now < last.saturating_add(LOG_EVERY_S)
            || LOGGED_AT_S
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let total: u64 = Kind::ALL.iter().map(|k| get(*k)).sum();
        let since = total.saturating_sub(LOGGED_TOTAL.swap(total, Ordering::Relaxed));
        tracing::warn!(
            failures = since,
            bearer = get(Kind::Bearer),
            cookie = get(Kind::Cookie),
            missing = get(Kind::Missing),
            login = get(Kind::Login),
            "ingress authentication failures (aggregated since the previous log; per-kind columns are totals since start)"
        );
    }

    pub(super) fn render(out: &mut String) {
        use std::fmt::Write;
        let name = "cairnflow_auth_failures_total";
        let _ = writeln!(
            out,
            "# HELP {name} Ingress authentication failures by kind — counted only, not blocked"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for k in Kind::ALL {
            let _ = writeln!(out, "{name}{{kind=\"{}\"}} {}", k.label(), get(k));
        }
        let name = "cairnflow_key_rate_limited_total";
        let _ = writeln!(
            out,
            "# HELP {name} Ingress requests refused with 429 because one API key exceeded --key-rate"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {}", KEY_RATE_LIMITED.load(Ordering::Relaxed));
    }
}

pub mod tenant {
    use super::{counter, gauge, AtomicU64, Ordering};

    /// Re-reads of the `tenants` registry refused because the object is corrupt.
    pub static BOOK_REJECTED_TOTAL: AtomicU64 = AtomicU64::new(0);
    /// Ingress requests answered `413` because of `max-payload=`.
    pub static PAYLOAD_REJECTED: AtomicU64 = AtomicU64::new(0);
    /// `POST /deployments` answered `429` because of `deploy-rate=`.
    pub static DEPLOY_RATE_LIMITED: AtomicU64 = AtomicU64::new(0);

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn render(out: &mut String, book_rejected: u64) {
        let v = |c: &AtomicU64| c.load(Ordering::Relaxed);
        gauge(
            out,
            "cairnflow_tenants_book_rejected",
            "1 when the tenants registry on the bucket is corrupt and the node keeps the old registry",
            book_rejected,
        );
        counter(
            out,
            "cairnflow_tenants_book_rejected_total",
            "Reloads of the tenants registry rejected because the object is corrupt",
            v(&BOOK_REJECTED_TOTAL),
        );
        counter(
            out,
            "cairnflow_tenant_payload_rejected_total",
            "Ingress requests rejected with 413 because of the tenant's max-payload",
            v(&PAYLOAD_REJECTED),
        );
        counter(
            out,
            "cairnflow_tenant_deploy_rate_limited_total",
            "POST /deployments rejected with 429 because of the tenant's deploy-rate",
            v(&DEPLOY_RATE_LIMITED),
        );
    }
}

pub mod agent {
    use super::{AtomicU64, Ordering};

    /// Stream records accepted into `StreamHub`, by stream kind: `[temporary, durable]`.
    pub static STREAM_RECORDS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
    /// Stream payload bytes accepted, by stream kind: `[temporary, durable]`.
    pub static STREAM_BYTES: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
    /// Stream records dropped (stream cap, the node's RAM cap; duplicates not
    /// counted) — every drop shows readers a `gap`.
    pub static STREAM_DROPPED: AtomicU64 = AtomicU64::new(0);
    /// `StreamHub` RAM bytes of every range the node holds — **the** counter
    /// of the node's RAM cap (one atomic, no shared lock).
    pub static STREAM_RAM_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Commits made only to make streams durable: `[flush when full, log_flush_ms]` (AP-16).
    pub static STREAM_FLUSH_COMMITS: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
    /// `Streams` rows carried by commits (`Rider::carried`), the number of
    /// commits carrying at least one row, and the number of **extra** rows —
    /// more than one row for the same stream in one commit (must be 0).
    pub static STREAM_ROWS_CARRIED: AtomicU64 = AtomicU64::new(0);
    pub static STREAM_ROW_COMMITS: AtomicU64 = AtomicU64::new(0);
    pub static STREAM_ROWS_EXTRA: AtomicU64 = AtomicU64::new(0);
    /// SSE connections open on the node.
    pub static SSE_OPEN: AtomicU64 = AtomicU64::new(0);
    /// SSE connections refused because of the node's cap (`429`).
    pub static SSE_REJECTED: AtomicU64 = AtomicU64::new(0);
    /// `$trace` records dropped because the OTLP export channel was full.
    pub static OTEL_SPANS_DROPPED: AtomicU64 = AtomicU64::new(0);
    pub static OTEL_LOGS_DROPPED: AtomicU64 = AtomicU64::new(0);
    /// `$log`/`$trace` records dropped because of the invocation's cap: `[$log, $trace]`.
    pub static LOG_DROPPED: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
    /// Artifact PUTs (each one creates an `artifacts/…` object).
    pub static ARTIFACT_PUTS: AtomicU64 = AtomicU64::new(0);
    /// Segments copied to the receiver's owner when delivering segmented output
    /// larger than its cap (COPY).
    pub static VALUE_COPIES: AtomicU64 = AtomicU64::new(0);
    /// Segments the shell sealed (state, `Output`, `ctx.run` results, merges)
    /// and their bytes.
    pub static VALUE_SEALED: AtomicU64 = AtomicU64::new(0);
    pub static VALUE_SEALED_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Segment merges on the write path.
    pub static VALUE_MERGES: AtomicU64 = AtomicU64::new(0);
    /// Segment bytes sent out through the `proxy` route (`GET /values/v1/{token}`).
    pub static VALUE_PROXY_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Scans of a range's `Deployment` index to count the invocations pinned to
    /// a deployment (`Command::PinnedCounts`).
    pub static PINNED_SCANS: AtomicU64 = AtomicU64::new(0);
    pub static ARTIFACT_PUT_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Artifact GETs (cache misses during replay, or reads through the API).
    pub static ARTIFACT_GETS: AtomicU64 = AtomicU64::new(0);
    /// Ref resolutions that hit the range's cache.
    pub static ARTIFACT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    /// Artifact cache bytes of every range on the node — the counter of the node's cap.
    pub static ARTIFACT_CACHE_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Open follower readers, by the reason of the query that opened them
    /// ([`crate::follower::Reason`]). Streams, logs and queries **never** open
    /// a reader (AP-13): there are only two reasons.
    pub static FOLLOWER_READERS_OPEN: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];

    pub fn bump(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    /// Saturating subtraction at 0 (gauge).
    pub fn sub(c: &AtomicU64, n: u64) {
        let _ = c.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(n))
        });
    }

    fn labeled<const N: usize>(
        out: &mut String,
        name: &str,
        help: &str,
        kind: &str,
        label: &str,
        values: &[&str; N],
        cells: &[AtomicU64; N],
    ) {
        use std::fmt::Write;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for (v, c) in values.iter().zip(cells) {
            let _ = writeln!(
                out,
                "{name}{{{label}=\"{v}\"}} {}",
                c.load(Ordering::Relaxed)
            );
        }
    }

    pub fn render(out: &mut String) {
        let durable = ["false", "true"];
        labeled(
            out,
            "cairnflow_stream_records_total",
            "StreamHub stream records received, by stream kind (durable)",
            "counter",
            "durable",
            &durable,
            &STREAM_RECORDS,
        );
        labeled(
            out,
            "cairnflow_stream_bytes_total",
            "Stream payload bytes received, by stream kind (durable)",
            "counter",
            "durable",
            &durable,
            &STREAM_BYTES,
        );
        super::counter(
            out,
            "cairnflow_stream_dropped_total",
            "Stream records dropped because of a cap (readers see a gap)",
            STREAM_DROPPED.load(Ordering::Relaxed),
        );
        super::gauge(
            out,
            "cairnflow_stream_ram_bytes",
            "StreamHub RAM bytes held by all ranges on the node",
            STREAM_RAM_BYTES.load(Ordering::Relaxed),
        );
        labeled(
            out,
            "cairnflow_stream_flush_commits_total",
            "Commits made only to make streams durable (AP-16): full = flush when full, timer = log_flush_ms",
            "counter",
            "reason",
            &["full", "timer"],
            &STREAM_FLUSH_COMMITS,
        );
        super::counter(
            out,
            "cairnflow_stream_rows_carried_total",
            "Streams rows carried by commits (piggybacked or flushed)",
            STREAM_ROWS_CARRIED.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_stream_row_commits_total",
            "Commits carrying at least one Streams row",
            STREAM_ROW_COMMITS.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_stream_rows_extra_total",
            "Extra Streams rows for the same stream in one commit (must be 0)",
            STREAM_ROWS_EXTRA.load(Ordering::Relaxed),
        );
        super::gauge(
            out,
            "cairnflow_stream_sse_open",
            "Open SSE connections on the node",
            SSE_OPEN.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_stream_sse_rejected_total",
            "SSE connections refused because of the node's cap (429)",
            SSE_REJECTED.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_otel_user_spans_dropped_total",
            "$trace records dropped because the OTLP export channel is full",
            OTEL_SPANS_DROPPED.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_otel_user_logs_dropped_total",
            "$log records dropped because the OTLP export channel is full",
            OTEL_LOGS_DROPPED.load(Ordering::Relaxed),
        );
        labeled(
            out,
            "cairnflow_log_dropped_total",
            "$log/$trace records dropped because of the invocation's cap",
            "counter",
            "stream",
            &["$log", "$trace"],
            &LOG_DROPPED,
        );
        super::counter(
            out,
            "cairnflow_value_copies_total",
            "Segments copied to the receiver's owner when a large output is delivered",
            VALUE_COPIES.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_value_segments_sealed_total",
            "Value segments sealed and PUT by the node (state, outputs, ctx.run results, merges)",
            VALUE_SEALED.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_value_sealed_bytes_total",
            "Bytes of value segments sealed and PUT by the node",
            VALUE_SEALED_BYTES.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_value_merges_total",
            "Value segment merges on the write path (each replaces adjacent segments with one)",
            VALUE_MERGES.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_deployment_pinned_scans_total",
            "Range scans counting unfinished invocations per deployment (one per range per count)",
            PINNED_SCANS.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_value_proxy_bytes_total",
            "Value segment bytes served to SDKs through the proxy route",
            VALUE_PROXY_BYTES.load(Ordering::Relaxed),
        );
        {
            use cairn_store::value::stats as v;
            super::counter(
                out,
                "cairnflow_value_cache_hits_total",
                "Value segment reads served by the range's cache",
                v::load(&v::CACHE_HITS),
            );
            super::counter(
                out,
                "cairnflow_value_cache_misses_total",
                "Value segment reads that went to the object store",
                v::load(&v::CACHE_MISSES),
            );
            super::gauge(
                out,
                "cairnflow_value_cache_bytes",
                "Value segment cache bytes held by all ranges on the node",
                v::load(&v::CACHE_BYTES),
            );
            super::counter(
                out,
                "cairnflow_value_fetched_bytes_total",
                "Value segment bytes read from the object store to assemble, copy or merge",
                v::load(&v::FETCHED_BYTES),
            );
        }
        super::counter(
            out,
            "cairnflow_artifact_puts_total",
            "Artifact PUTs (each creates one artifacts/… object)",
            ARTIFACT_PUTS.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_artifact_put_bytes_total",
            "Artifact bytes PUT",
            ARTIFACT_PUT_BYTES.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_artifact_gets_total",
            "Artifact GETs (cache miss on replay, or read through the API)",
            ARTIFACT_GETS.load(Ordering::Relaxed),
        );
        super::counter(
            out,
            "cairnflow_artifact_cache_hits_total",
            "Artifact ref resolutions that hit the range's cache",
            ARTIFACT_CACHE_HITS.load(Ordering::Relaxed),
        );
        super::gauge(
            out,
            "cairnflow_artifact_cache_bytes",
            "Artifact cache bytes held by all ranges on the node",
            ARTIFACT_CACHE_BYTES.load(Ordering::Relaxed),
        );
        labeled(
            out,
            "cairnflow_follower_readers_open",
            "Open follower readers, by the reason of the request that opened them (AP-13: streams, logs and queries do not open readers)",
            "gauge",
            "reason",
            &["status", "journal"],
            &FOLLOWER_READERS_OPEN,
        );
    }
}

/// One line with a `range` label — for per-reader gauges and the split part
/// of counters.
pub struct PartitionGauge {
    pub partition: u32,
    pub value: u64,
}

/// Prints one counter in exactly the Prometheus text grammar.
fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Like [`gauge_per_partition`], but `TYPE counter`.
///
/// A name **different** from the total's (`_by_range_total`, not the same
/// name with an extra label) is deliberate: a metric family with both an
/// unlabelled line and labelled lines is the `sum()` double-counting trap. With
/// two names nobody can confuse them.
fn counter_per_partition(out: &mut String, name: &str, help: &str, rows: &[PartitionGauge]) {
    use std::fmt::Write;
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(out, "# HELP {name} {help} (per range)");
    let _ = writeln!(out, "# TYPE {name} counter");
    for r in rows {
        let _ = writeln!(out, "{name}{{range=\"{}\"}} {}", r.partition, r.value);
    }
}

fn gauge_per_partition(out: &mut String, name: &str, help: &str, rows: &[PartitionGauge]) {
    use std::fmt::Write;
    if rows.is_empty() {
        return;
    }
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    for r in rows {
        let _ = writeln!(out, "{name}{{range=\"{}\"}} {}", r.partition, r.value);
    }
}

/// A gauge snapshot at scrape time — ingress fills it in, this only prints.
pub struct Gauges {
    pub partition_count: u64,
    /// Version of the current range table — changes on every split/merge.
    pub table_version: u64,
    pub partitions_with_leader: u64,
    pub deployments: u64,
    pub reader_healthy: Vec<PartitionGauge>,
    pub reader_durable_seq: Vec<PartitionGauge>,
    pub reader_manifest_seq: Vec<PartitionGauge>,
    /// Accumulated turn time per range on this node, ms.
    pub range_turn_ms: Vec<PartitionGauge>,
    /// Per-range L0 — see [`crate::l0`].
    pub range_l0: Vec<crate::l0::RangeL0Row>,
    /// The GC floor of the node's shared WAL — empty when the node does not use
    /// the shared WAL.
    pub nodewal: Vec<cairn_store::nodewal::FloorReport>,
    /// The node's PUT meter and slowness score. `None` outside a cluster.
    pub slow: Option<cairn_store::health::SlowMeter>,
    /// Term of the coordinator role the node holds; `None` = not held.
    pub coordinator_term: Option<u64>,
    /// Number of schedules of the ranges the node holds.
    pub schedules: u64,
    /// The node's SST disk cache; `None` = off — not printed.
    pub disk_cache: Option<cairn_store::disk_cache::DiskCacheSnapshot>,
    /// `cairnflow_query_index_state` — xem [`query_index`].
    pub query_index: Vec<(&'static str, &'static str, u64)>,
    /// `1` when the `tenants` registry on the bucket is corrupt and the node keeps the old one.
    pub tenants_book_rejected: u64,
    /// Machine RAM and disk; `None` = not sampled.
    pub host: Option<crate::host::HostStats>,
}

pub fn render(m: &Metrics, g: &Gauges) -> String {
    let mut out = String::with_capacity(2048);
    let c = |a: &AtomicU64| a.load(Ordering::Relaxed);
    counter(
        &mut out,
        "cairnflow_invoke_requests_total",
        "Service calls received through this node's ingress",
        c(&m.invoke_requests),
    );
    counter(
        &mut out,
        "cairnflow_invoke_completed_total",
        "Calls that ended completed within the request",
        c(&m.invoke_completed),
    );
    counter(
        &mut out,
        "cairnflow_invoke_waiting_total",
        "Calls that returned waiting (suspended on a timer/call)",
        c(&m.invoke_waiting),
    );
    counter(
        &mut out,
        "cairnflow_invoke_failed_total",
        "Calls that ended failed within the request",
        c(&m.invoke_failed),
    );
    counter(
        &mut out,
        "cairnflow_invoke_paused_total",
        "Calls that ended paused within the request",
        c(&m.invoke_paused),
    );
    counter(
        &mut out,
        "cairnflow_invoke_throttled_total",
        "Calls answered 429 because the slot wait limit ran out (durable, retry scheduled)",
        c(&m.invoke_throttled),
    );
    counter(
        &mut out,
        "cairnflow_cancelled_total",
        "Invocations cancelled through the admin API on this node",
        c(&m.cancelled),
    );
    counter(
        &mut out,
        "cairnflow_admin_ticks_total",
        "/admin/tick ticks run",
        c(&m.ticks),
    );
    counter(
        &mut out,
        "cairnflow_admin_tick_advanced_total",
        "Invocations advanced by /admin/tick",
        c(&m.tick_advanced),
    );
    for (name, help, h) in latency::all() {
        h.render(&mut out, name, help);
    }
    for (name, help, c) in sdk::all() {
        counter(&mut out, &format!("{name}_total"), help, c.total());
        // The per-range split **only has lines for ranges that were ever
        // bumped**. The alerting surface is the total above, always present
        // even at 0; the per-range series are for diagnosis.
        let rows: Vec<PartitionGauge> = c
            .per_range()
            .into_iter()
            .map(|(partition, value)| PartitionGauge { partition, value })
            .collect();
        counter_per_partition(&mut out, &format!("{name}_by_range_total"), help, &rows);
    }
    sdk::render_bytes(&mut out);
    sdk::render_replay(&mut out);
    interrange::render(&mut out);
    floor::render(&mut out);
    settle::render(&mut out);
    agent::render(&mut out);
    tenant::render(&mut out, g.tenants_book_rejected);
    audit::render(&mut out);
    auth_failures::render(&mut out);
    counter(
        &mut out,
        "cairnflow_flow_granted_total",
        "Slots granted to a turn",
        c(&flow::GRANTED),
    );
    counter(
        &mut out,
        "cairnflow_flow_wait_ms_total",
        "Total slot wait time (ms); divide by granted_total for the average",
        c(&flow::WAIT_MS_TOTAL),
    );
    counter(
        &mut out,
        "cairnflow_flow_throttled_total",
        "Turns dropped because the slot wait limit ran out — retry scheduled",
        c(&flow::THROTTLED),
    );
    gauge(
        &mut out,
        "cairnflow_flow_turn_tasks",
        "Tasks of this node running a background turn: admission, waiting for a slot, or the granted turn",
        c(&flow::TURN_TASKS),
    );
    gauge(
        &mut out,
        "cairnflow_flow_parked",
        "Background turns parked in a scheduler queue as data, without a task, waiting for a slot",
        c(&flow::PARKED),
    );
    counter(
        &mut out,
        "cairnflow_flow_retry_busy_total",
        "Due retries skipped by the background tick because the invocation was busy",
        c(&flow::RETRY_BUSY),
    );
    counter(
        &mut out,
        "cairnflow_rescue_marked_total",
        "Times an error path marked a wakeup as possibly lost (the term's rescue net)",
        c(&flow::RESCUE_MARKED),
    );
    counter(
        &mut out,
        "cairnflow_rescue_nacked_total",
        "Times a turn ended without acking its wakeup duty (default nack) — part of rescue_marked",
        c(&flow::RESCUE_NACKED),
    );
    counter(
        &mut out,
        "cairnflow_rescue_requeued_total",
        "Running invocations with nothing left to wake them that the rescue net re-dispatched",
        c(&flow::RESCUED),
    );
    counter(
        &mut out,
        "cairnflow_rescue_round_failed_total",
        "Rescue net reconcile rounds that failed on a store read error — marked again",
        c(&flow::RESCUE_FAILED),
    );
    counter(
        &mut out,
        "cairnflow_rescue_infra_stuck_total",
        "Invocations the rescue net moved to paused 577 (INFRA_STUCK) after too many consecutive re-dispatches",
        c(&flow::INFRA_STUCK),
    );
    counter(
        &mut out,
        "cairnflow_poison_rows_total",
        "Inbound/signal rows that could not be decoded — kept, not deleted (once per row per term)",
        c(&flow::POISON_ROWS),
    );
    counter(
        &mut out,
        "cairnflow_slow_log_total",
        "Runs over the slow log threshold that were logged",
        c(&crate::slowlog::LOGGED),
    );
    counter(
        &mut out,
        "cairnflow_slow_log_dropped_total",
        "Slow log lines dropped because the node's per-second cap was exceeded",
        c(&crate::slowlog::DROPPED),
    );
    {
        // The state scrubber: only the D-d runner has non-zero numbers; every
        // node prints all labels so cluster-wide `max(...)` alerts work.
        use {crate::scrub::stats as s, std::fmt::Write};
        let name = "cairnflow_scrub_findings";
        let _ = writeln!(
            out,
            "# HELP {name} State scrubber findings still present in two consecutive rounds, by verify check"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        for (i, check) in crate::ctl::CHECKS.iter().enumerate() {
            let _ = writeln!(out, "{name}{{check=\"{check}\"}} {}", c(&s::FINDINGS[i]));
        }
        let name = "cairnflow_scrub_rounds_total";
        let _ = writeln!(
            out,
            "# HELP {name} State scrubber rounds run on this node, by result"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (result, v) in [
            ("clean", &s::ROUNDS_CLEAN),
            ("found", &s::ROUNDS_FOUND),
            ("failed", &s::ROUNDS_FAILED),
            ("incomplete", &s::ROUNDS_INCOMPLETE),
        ] {
            let _ = writeln!(out, "{name}{{result=\"{result}\"}} {}", c(v));
        }
        counter(
            &mut out,
            "cairnflow_scrub_skipped_total",
            "State scrubber items skipped because reads kept failing after the retry limit",
            c(&s::SKIPPED),
        );
        // Interval by request budget.
        for (name, help, v) in [
            (
                "cairnflow_scrub_round_requests",
                "Object store requests sent by the last state scrubber round on this node",
                c(&s::ROUND_REQUESTS) as f64,
            ),
            (
                "cairnflow_scrub_wait_seconds",
                "Current pause between two state scrubber rounds: --scrub-every-s, or longer when a round costs more than --scrub-requests-per-hour allows",
                c(&s::WAIT_MS) as f64 / 1000.0,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {v}");
        }
        // Only nodes that have ever run the scrubber print the age: alerts take
        // a cluster-wide `min(...)`, so the current runner decides.
        if let Some(age) = s::last_complete_age_s(crate::scrub::wall_ms()) {
            let name = "cairnflow_scrub_last_complete_age_seconds";
            let _ = writeln!(
                out,
                "# HELP {name} Seconds since the last complete state scrubber round on this node (since it became the scrubber runner, if no round completed yet)"
            );
            let _ = writeln!(out, "# TYPE {name} gauge");
            let _ = writeln!(out, "{name} {age}");
        }
    }
    {
        // Incarnations dropped, by first cause: the baseline of the goal "a
        // single store error does not take down the whole node". All labels
        // printed, even at 0.
        use {cairn_store::failstop::Cause, std::fmt::Write};
        let name = "cairnflow_failstop_total";
        let _ = writeln!(
            out,
            "# HELP {name} Incarnations this node abandoned (fail-stop or lost session), by first cause"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for cause in Cause::ALL {
            let _ = writeln!(
                out,
                "{name}{{cause=\"{}\"}} {}",
                cause.label(),
                cairn_store::failstop::count(cause)
            );
        }
        // Dirty yields: a range stepped down while its owner record still
        // points at a live shared-WAL log — the fail-stop after that carries
        // the `dirty_yield` label above.
        let name = "cairnflow_dirty_yields_total";
        let _ = writeln!(
            out,
            "# HELP {name} Ranges this node stepped down from with the owner record possibly still pointing at its live shared WAL log (seal failed or timed out, or the clean owner-record CAS failed)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {}", cairn_store::failstop::dirty_yields());
        // Outcomes of re-reading ambiguous shared-WAL log PUTs. All labels printed.
        use cairn_store::nodewal::stats::{put_resolved, PutResolved};
        let name = "cairnflow_nodewal_put_resolved_total";
        let _ = writeln!(
            out,
            "# HELP {name} Shared WAL log PUTs that hit AlreadyExists and were read back, by outcome"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for o in PutResolved::ALL {
            let _ = writeln!(
                out,
                "{name}{{outcome=\"{}\"}} {}",
                o.label(),
                put_resolved(o)
            );
        }
        // Shared-WAL streams rotated instead of fail-stopping the whole node. All labels printed.
        use cairn_store::nodewal::stats::{stripe_rotations, RotationCause};
        let name = "cairnflow_nodewal_stripe_rotations_total";
        let _ = writeln!(
            out,
            "# HELP {name} Shared WAL stream rotated to a new stripe id (log PUT stalled, outcome unknown, or a range yielded with an unsealed Db), by cause"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for c in RotationCause::ALL {
            let _ = writeln!(
                out,
                "{name}{{cause=\"{}\"}} {}",
                c.label(),
                stripe_rotations(c)
            );
        }
        // Dead-log sweep rounds and purged logs. All labels printed.
        use cairn_store::nodewal::stats::{logs_purged, sweep_rounds, SweepOutcome};
        let name = "cairnflow_nodewal_sweep_rounds_total";
        let _ = writeln!(
            out,
            "# HELP {name} Full sweep rounds over dead shared WAL logs, by outcome (again: more work or a failed log, the next round runs within seconds)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for o in SweepOutcome::ALL {
            let _ = writeln!(
                out,
                "{name}{{outcome=\"{}\"}} {}",
                o.label(),
                sweep_rounds(o)
            );
        }
        let name = "cairnflow_nodewal_logs_purged_total";
        let _ = writeln!(
            out,
            "# HELP {name} Dead or rotated shared WAL logs purged to the trash (seal kept)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {}", logs_purged());
        gauge(
            &mut out,
            "cairnflow_gc_unproven_candidates",
            "Ranges eligible for D-d whose DeleteProof the latest pass could not rebuild — kept",
            c(&crate::gc::UNPROVEN_CANDIDATES),
        );
    }
    {
        // D-d per-entry fault tolerance: ranges kept because a read failed at
        // planning, or because the deletion step failed. All labels printed.
        use {crate::gc::stats as s, std::fmt::Write};
        let name = "cairnflow_gc_skipped_total";
        let _ = writeln!(
            out,
            "# HELP {name} Retired ranges the D-d pass skipped (Db kept) because a read or delete failed, by step"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (stage, v) in [("plan", &s::SKIPPED_PLAN), ("delete", &s::SKIPPED_DELETE)] {
            let _ = writeln!(out, "{name}{{stage=\"{stage}\"}} {}", c(v));
        }
    }
    {
        // D-d's second barrier: ranges the latest plan kept because of a final
        // checkpoint on their own manifest, and the two directions of
        // disagreement between the scan and the checkpoint.
        use {crate::gc::checkpoint_stats as s, std::fmt::Write};
        let name = "cairnflow_gc_checkpoint_held";
        let _ = writeln!(
            out,
            "# HELP {name} Retired ranges the latest D-d plan kept because of a final checkpoint on their own manifest, by kind"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        for (kind, v) in [
            ("named", &s::NAMED),
            ("unnamed", &s::UNNAMED),
            ("disagree", &s::DISAGREE),
            ("unreadable", &s::UNREADABLE),
        ] {
            let _ = writeln!(out, "{name}{{kind=\"{kind}\"}} {}", c(v));
        }
        use cairn_store::proof::stats as p;
        let name = "cairnflow_gc_proofs_disagree_total";
        let _ = writeln!(
            out,
            "# HELP {name} Times the D-d delete step found the reference scan and the checkpoints on the Db itself disagreeing, by direction"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (direction, v) in [
            ("scan_clear", &p::PROOFS_DISAGREE),
            ("scan_referenced", &p::SCAN_ONLY),
        ] {
            let _ = writeln!(out, "{name}{{direction=\"{direction}\"}} {}", c(v));
        }
        // Range-table history: errors here do not fail the table change, so
        // they are only visible here and through `cairn-ctl table restore`.
        use cairn_store::cluster_history::stats as h;
        let name = "cairnflow_cluster_history_failures_total";
        let _ = writeln!(
            out,
            "# HELP {name} Range table history (cluster-history/) failures after a won table change, by step: write (the version is missing), conflict (a different table already holds that version; kept), prune"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (step, v) in [
            ("write", &h::WRITE_FAILED),
            ("conflict", &h::CONFLICT),
            ("prune", &h::PRUNE_FAILED),
        ] {
            let _ = writeln!(out, "{name}{{step=\"{step}\"}} {}", c(v));
        }
    }
    {
        // Lineage per the latest D-d plan: tombstones and retired Dbs waiting
        // for deletion. Only nodes that have made a D-d plan print it.
        use {crate::gc::lineage_stats as s, std::sync::atomic::Ordering};
        if s::OBSERVED.load(Ordering::Relaxed) {
            gauge(
                &mut out,
                "cairnflow_owner_tombstones",
                "Owner records carrying a retirement tombstone (ranges no longer in the range table) at the latest D-d plan",
                c(&s::TOMBSTONES),
            );
            gauge(
                &mut out,
                "cairnflow_retired_dbs",
                "Retired ranges whose Db is still on the bucket, waiting for D-d, at the latest D-d plan",
                c(&s::RETIRED_DBS),
            );
        }
    }
    {
        // The `ops/` operation-record cleanup round.
        use {crate::gc::ops_stats as s, std::fmt::Write};
        let name = "cairnflow_gc_ops_reaped_total";
        let _ = writeln!(
            out,
            "# HELP {name} ops/ records of settled ops that the D-d pass moved to the trash, by result"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (outcome, v) in [("reaped", &s::REAPED), ("failed", &s::FAILED)] {
            let _ = writeln!(out, "{name}{{outcome=\"{outcome}\"}} {}", c(v));
        }
        gauge(
            &mut out,
            "cairnflow_gc_ops_ledgers",
            "Op records under ops/ at the latest LIST of the D-d pass",
            c(&s::LISTED),
        );
    }
    {
        // The GC clock-skew gate.
        use {crate::gc::clock_stats as s, std::fmt::Write};
        gauge(
            &mut out,
            "cairnflow_gc_clock_skew_ms",
            "|Skew| between the node clock and the store's last_modified at the latest GC gate measurement, ms",
            c(&s::SKEW_MS),
        );
        let name = "cairnflow_gc_clock_blocked_total";
        let _ = writeln!(
            out,
            "# HELP {name} Age-based delete passes blocked by the clock skew gate (all deletes skipped), by pass"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (i, round) in crate::gc::ClockRound::ALL.iter().enumerate() {
            let _ = writeln!(
                out,
                "{name}{{round=\"{}\"}} {}",
                round.label(),
                c(&s::BLOCKED[i])
            );
        }
    }
    {
        // Clock rate.
        use {crate::clock_rate::stats as s, std::fmt::Write};
        let name = "cairnflow_clock_rate";
        let _ = writeln!(
            out,
            "# HELP {name} Rate of the node's monotonic clock relative to store time (Date header, 5-minute window) at the latest measurement; 0 = not measured yet"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {:.3}", c(&s::RATE_MILLI) as f64 / 1000.0);
        gauge(
            &mut out,
            "cairnflow_clock_rate_untrusted",
            "Node clock rate off by more than ±10% relative to the store (1) — node stops leading — or not (0)",
            c(&s::UNTRUSTED),
        );
        counter(
            &mut out,
            "cairnflow_clock_rate_stepdowns_total",
            "Times the node stopped leading because its monotonic clock rate was off by more than ±10% relative to the store",
            c(&s::STEPDOWNS),
        );
    }
    {
        // A silent session whose `expired` CAS keeps losing (a torn object, a
        // stale read): the observer overwrites the record and waits one TTL.
        // `failed` rising = a session nobody can mark — its ranges are stuck
        // until the store can read/write again. All labels printed.
        use {cairn_store::rearm_stats as s, std::fmt::Write};
        let name = "cairnflow_session_rearm_total";
        let _ = writeln!(
            out,
            "# HELP {name} Overwrites of a stale session record that could not mark it expired, by result"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (outcome, v) in [("written", &s::WRITTEN), ("failed", &s::FAILED)] {
            let _ = writeln!(out, "{name}{{outcome=\"{outcome}\"}} {}", c(v));
        }
    }
    {
        use std::fmt::Write;
        let name = "cairnflow_coordinator_term";
        let _ = writeln!(
            out,
            "# HELP {name} Term of the coordinator role this node holds, 0 when not held"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name} {}", g.coordinator_term.unwrap_or(0));
    }
    counter(
        &mut out,
        "cairnflow_lease_delegated_revoked_while_valid_total",
        "Delegated renewal tripwire: the delegated node's self-check GET found its record ended while its grant was still valid — protocol error, must be 0",
        c(&cairn_store::delegation_stats::REVOKED_WHILE_VALID),
    );
    counter(
        &mut out,
        "cairnflow_session_revived_total",
        "Times an expired session demoted itself (grant paused) then revived because a renewal won the CAS from the exact previous version — node keeps all ranges, no replay",
        c(&cairn_store::pause_stats::REVIVED),
    );
    {
        // The read fence at the leader: one conditional GET on the session
        // record per **batch** of answers. `waiters / Σ batches` is the average
        // batch size — the measure of the request cost. All labels printed.
        use {cairn_store::read_fence_stats as s, std::fmt::Write};
        counter(
            &mut out,
            "cairnflow_read_fence_waiters_total",
            "Leader answers that passed the read fence",
            c(&s::WAITERS),
        );
        let name = "cairnflow_read_fence_total";
        let _ = writeln!(
            out,
            "# HELP {name} Read fence batches at the leader (one session GET per batch), by result"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (outcome, v) in [
            ("confirmed", &s::CONFIRMED),
            ("refused", &s::REFUSED),
            ("failed", &s::FAILED),
        ] {
            let _ = writeln!(out, "{name}{{outcome=\"{outcome}\"}} {}", c(v));
        }
    }
    {
        // The shared retry / limit primitive: one `op` label per operation, all
        // labels printed even at 0.
        use {cairn_store::retry::ops, std::fmt::Write};
        let rows = ops::ALL.map(|o| (o.name, o.counts()));
        for (name, help, pick) in [
            (
                "cairnflow_retry_retries_total",
                "Retries of the shared retry primitive, by operation",
                (|c| c.retries) as fn(cairn_store::retry::OpCounts) -> u64,
            ),
            (
                "cairnflow_retry_exhausted_total",
                "Limit hits (attempts, total time or a deadline), by operation",
                |c| c.exhausted,
            ),
            (
                "cairnflow_retry_permanent_total",
                "Retries stopped on a permanent error, by operation",
                |c| c.permanent,
            ),
            (
                "cairnflow_retry_ambiguous_total",
                "Stops on an ambiguous result — returned to the caller to resolve by reading back, by operation",
                |c| c.ambiguous,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            for (op, counts) in rows {
                let _ = writeln!(out, "{name}{{op=\"{op}\"}} {}", pick(counts));
            }
        }
    }
    {
        use {crate::ingress::forward as f, std::fmt::Write};
        let name = "cairnflow_forward_total";
        let _ = writeln!(
            out,
            "# HELP {name} Forward requests (x-cairn-forward: leader) by outcome: relayed = the leader answered, refused = could not be sent (original 503 returned), ambiguous = sent but no response (504)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (result, n) in [
            ("relayed", &f::RELAYED),
            ("refused", &f::REFUSED),
            ("ambiguous", &f::AMBIGUOUS),
        ] {
            let _ = writeln!(out, "{name}{{result=\"{result}\"}} {}", c(n));
        }
    }
    {
        use crate::registry_gen as g;
        gauge(
            &mut out,
            "cairnflow_registry_stale",
            "1 when the node is refusing requests authenticated by registry keys because it cannot confirm the registry is fresh",
            c(&g::STALE),
        );
        counter(
            &mut out,
            "cairnflow_registry_stale_rejected_total",
            "Requests answered 503 because the key registry may not be fresh (1.8b)",
            c(&g::STALE_REJECTED),
        );
    }
    {
        use {crate::quarantine as q, std::fmt::Write};
        counter(
            &mut out,
            "cairnflow_quarantine_detected_total",
            "Poison handlers this node just added to the quarantine list",
            c(&q::DETECTED),
        );
        counter(
            &mut out,
            "cairnflow_quarantine_held_total",
            "New invocations created as paused 576 because of a quarantine hold",
            c(&q::HELD),
        );
        counter(
            &mut out,
            "cairnflow_quarantine_cooled_total",
            "Turns blocked at the scheduler's cooldown tier",
            c(&q::COOLED),
        );
        let name = "cairnflow_quarantine_entries";
        let _ = writeln!(
            out,
            "# HELP {name} Unexpired quarantine entries known to this node, by action"
        );
        let _ = writeln!(out, "# TYPE {name} gauge");
        for (action, v) in [
            ("observe", &q::ENTRIES_OBSERVE),
            ("cooldown", &q::ENTRIES_COOLDOWN),
            ("hold", &q::ENTRIES_HOLD),
        ] {
            let _ = writeln!(out, "{name}{{action=\"{action}\"}} {}", c(v));
        }
    }
    {
        use std::fmt::Write;
        let name = "cairnflow_flow_blocked_total";
        let _ = writeln!(
            out,
            "# HELP {name} Turns blocked at a flow control resource, by kind"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (kind, v) in [
            ("lock", &flow::BLOCKED_LOCK),
            ("concurrency", &flow::BLOCKED_CONCURRENCY),
            ("turns", &flow::BLOCKED_TURNS),
            ("tenant", &flow::BLOCKED_TENANT),
            ("tenant_turns", &flow::BLOCKED_TENANT_TURNS),
        ] {
            let _ = writeln!(out, "{name}{{resource=\"{kind}\"}} {}", c(v));
        }
    }
    counter(
        &mut out,
        "cairnflow_replay_budget_throttled_total",
        "SDK turns rescheduled without calling the endpoint because the node replay RAM budget was full",
        crate::replay_budget::NODE.throttled(),
    );
    gauge(
        &mut out,
        "cairnflow_replay_budget_bytes",
        "Journal bytes held in RAM by running SDK turns on this node",
        crate::replay_budget::NODE.used(),
    );
    counter(
        &mut out,
        "cairnflow_flow_refused_at_call_total",
        "SDK turns that already held a permit and stopped right before calling the endpoint because the tenant became suspended",
        c(&flow::REFUSED_AT_CALL),
    );
    gauge(
        &mut out,
        "cairnflow_partitions",
        "Ranges in the current range table",
        g.partition_count,
    );
    gauge(
        &mut out,
        "cairnflow_range_table_version",
        "Version of the current range table",
        g.table_version,
    );
    gauge(
        &mut out,
        "cairnflow_partitions_with_leader",
        "Partitions with a leader according to the routing table",
        g.partitions_with_leader,
    );
    gauge(
        &mut out,
        "cairnflow_deployments",
        "Deployments in this node's registry",
        g.deployments,
    );
    counter(
        &mut out,
        "cairnflow_auto_split_total",
        "Automatic range splits completed on this node",
        c(&crate::autosplit::AUTO_SPLITS),
    );
    counter(
        &mut out,
        "cairnflow_auto_split_failed_total",
        "Automatic splits that wrote an intent then failed (rolled back)",
        c(&crate::autosplit::AUTO_SPLITS_FAILED),
    );
    {
        use {crate::lineage::stats as l, std::fmt::Write as _};
        let name = "cairnflow_lineage_compactions_submitted_total";
        let _ = writeln!(
            out,
            "# HELP {name} Full compactions this node requested so a range stops standing on its ancestor ranges, by reason: depth (reads SSTs of too many ancestors) or ancestor_age (an ancestor retired long ago)"
        );
        let _ = writeln!(out, "# TYPE {name} counter");
        for (reason, v) in [
            ("depth", &l::SUBMITTED_DEPTH),
            ("ancestor_age", &l::SUBMITTED_AGE),
        ] {
            let _ = writeln!(out, "{name}{{reason=\"{reason}\"}} {}", c(v));
        }
    }
    counter(
        &mut out,
        "cairnflow_lineage_compactions_total",
        "Full compactions requested by the lineage loop that finished (GC then detaches the ancestors)",
        c(&crate::lineage::stats::COMPACTED),
    );
    // Only the per-range form exists: there is no unlabelled total with the
    // same name (see `counter_per_partition` on the `sum()` double-counting
    // trap).
    counter_per_partition(
        &mut out,
        "cairnflow_range_turn_ms_total",
        "Time of SDK turns completed on this node, ms — which range uses turn slots",
        &g.range_turn_ms,
    );
    gauge_per_partition(
        &mut out,
        "cairnflow_reader_healthy",
        "Whether the open follower reader is healthy (R-G): 1 healthy, 0 wedged",
        &g.reader_healthy,
    );
    gauge_per_partition(
        &mut out,
        "cairnflow_reader_durable_seq",
        "WAL seq the reader has replayed up to",
        &g.reader_durable_seq,
    );
    gauge_per_partition(
        &mut out,
        "cairnflow_reader_manifest_seq",
        "WAL seq the manifest covers — durable lagging behind for long means wedged",
        &g.reader_manifest_seq,
    );
    crate::l0::render(&mut out, &g.range_l0);
    schedules::render(&mut out, g.schedules);
    query_index::render(&mut out, &g.query_index);
    if let Some(d) = &g.disk_cache {
        counter(
            &mut out,
            "cairnflow_disk_cache_hits_total",
            "SST part reads that hit the disk cache",
            d.hits,
        );
        counter(
            &mut out,
            "cairnflow_disk_cache_misses_total",
            "SST part reads through the disk cache that missed — read from the object store",
            d.misses(),
        );
        counter(
            &mut out,
            "cairnflow_disk_cache_evictions_total",
            "Entries evicted from the disk cache for reaching --disk-cache-bytes",
            d.evicted_keys,
        );
        gauge(
            &mut out,
            "cairnflow_disk_cache_bytes",
            "Bytes in the SST disk cache",
            d.bytes,
        );
    }
    nodewal_floors(&mut out, &g.nodewal);
    if let Some(m) = &g.slow {
        slow_meter(&mut out, m);
    }
    http::render(&mut out);
    store_requests(&mut out);
    if let Some(h) = &g.host {
        host(&mut out, h);
    }
    out
}

/// Slowness score, drain flag and PUT latency histograms by object kind. Only
/// within a cluster.
fn slow_meter(out: &mut String, m: &cairn_store::health::SlowMeter) {
    use {cairn_store::health::PutKind, std::fmt::Write};
    let name = "cairnflow_slow_score";
    let _ = writeln!(
        out,
        "# HELP {name} Node slowness score, 1 (healthy) to 100 (slow), per 10 s round"
    );
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {:.3}", m.score());
    gauge(
        out,
        "cairnflow_draining",
        "Whether the node is shedding leadership because it is slow (1) or not (0)",
        m.published().draining as u64,
    );
    let name = "cairnflow_object_put_seconds";
    let _ = writeln!(out, "# HELP {name} PUT latency to the object store by object kind (session renewal, shared WAL log)");
    let _ = writeln!(out, "# TYPE {name} histogram");
    for kind in PutKind::ALL {
        let h = m.histogram(kind);
        let k = kind.as_str();
        for (le, n) in &h.buckets {
            let _ = writeln!(
                out,
                "{name}_bucket{{kind=\"{k}\",le=\"{}\"}} {n}",
                *le as f64 / 1000.0
            );
        }
        let _ = writeln!(out, "{name}_bucket{{kind=\"{k}\",le=\"+Inf\"}} {}", h.count);
        let _ = writeln!(
            out,
            "{name}_sum{{kind=\"{k}\"}} {}",
            h.sum_us as f64 / 1_000_000.0
        );
        let _ = writeln!(out, "{name}_count{{kind=\"{k}\"}} {}", h.count);
    }
}

/// The shared-WAL floor: floor age per node — the line is present even at 0,
/// so a "floor stuck for more than X" alert has a series to look at — and the
/// ranges pinning the floor (only lines for ranges currently pinning it).
fn nodewal_floors(out: &mut String, reports: &[cairn_store::nodewal::FloorReport]) {
    use std::fmt::Write;
    if reports.is_empty() {
        return;
    }
    let name = "cairnflow_nodewal_floor_age_seconds";
    let _ = writeln!(
        out,
        "# HELP {name} How long the shared WAL GC floor has been stuck at a pinned object (longest-pinned log)"
    );
    let _ = writeln!(out, "# TYPE {name} gauge");
    for r in reports {
        let _ = writeln!(
            out,
            "{name}{{node=\"{}\"}} {:.3}",
            r.node,
            r.age().as_secs_f64()
        );
    }
    let pinned: std::collections::BTreeSet<u32> = reports
        .iter()
        .flat_map(|r| r.pinned().into_keys())
        .collect();
    if pinned.is_empty() {
        return;
    }
    let name = "cairnflow_nodewal_floor_pinned";
    let _ = writeln!(
        out,
        "# HELP {name} 1 if the range pins the GC floor of a shared WAL log (frames not yet flushed to L0)"
    );
    let _ = writeln!(out, "# TYPE {name} gauge");
    for range in pinned {
        let _ = writeln!(out, "{name}{{range=\"{range}\"}} 1");
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        cairn_store::{
            nodewal::{FloorReport, LogFloor},
            LogId,
        },
        std::time::Duration,
    };

    /// Floor age per node, and one `floor_pinned` line for each range pinning
    /// it (a range pinning two logs gets only one line).
    #[test]
    fn renders_nodewal_floor_gauges() {
        let log = |s: &str| LogId::parse(s).expect("log id");
        let g = Gauges {
            partition_count: 1,
            table_version: 1,
            partitions_with_leader: 1,
            deployments: 0,
            reader_healthy: Vec::new(),
            reader_durable_seq: Vec::new(),
            reader_manifest_seq: Vec::new(),
            range_turn_ms: Vec::new(),
            slow: None,
            coordinator_term: None,
            schedules: 0,
            disk_cache: None,
            query_index: Vec::new(),
            tenants_book_rejected: 0,
            host: None,
            range_l0: Vec::new(),
            nodewal: vec![FloorReport {
                node: "a".into(),
                logs: vec![
                    LogFloor {
                        log: log("a/1:0"),
                        floor: 4,
                        pinned_by: Some((7, 12)),
                        pinned_for: Duration::from_millis(2500),
                    },
                    LogFloor {
                        log: log("b/3:1"),
                        floor: 9,
                        pinned_by: Some((7, 40)),
                        pinned_for: Duration::from_secs(1),
                    },
                    LogFloor {
                        log: log("a/1:1"),
                        floor: 2,
                        pinned_by: None,
                        pinned_for: Duration::ZERO,
                    },
                ],
            }],
        };
        let text = render(&Metrics::default(), &g);
        assert!(
            text.contains("cairnflow_nodewal_floor_age_seconds{node=\"a\"} 2.500\n"),
            "{text}"
        );
        assert_eq!(
            text.matches("cairnflow_nodewal_floor_pinned{range=\"7\"} 1\n")
                .count(),
            1,
            "{text}"
        );
        assert!(!text.contains("floor_pinned{range=\"0\"}"));
    }

    /// Slowness score, drain flag and PUT histograms by object kind.
    #[test]
    fn renders_slow_score_and_put_histogram() {
        let m = cairn_store::health::SlowMeter::new("render-slow", Duration::from_secs(1));
        m.renewed(Duration::from_millis(4));
        m.wal_attempt(Duration::from_millis(1200), false);
        m.publish(42.0, Some(1500));
        let g = Gauges {
            partition_count: 1,
            table_version: 1,
            partitions_with_leader: 1,
            deployments: 0,
            reader_healthy: Vec::new(),
            reader_durable_seq: Vec::new(),
            reader_manifest_seq: Vec::new(),
            range_turn_ms: Vec::new(),
            range_l0: Vec::new(),
            nodewal: Vec::new(),
            slow: Some(m),
            coordinator_term: None,
            schedules: 0,
            disk_cache: None,
            query_index: Vec::new(),
            tenants_book_rejected: 0,
            host: None,
        };
        let text = render(&Metrics::default(), &g);
        assert!(text.contains("cairnflow_slow_score 42.000\n"), "{text}");
        assert!(text.contains("cairnflow_draining 0\n"), "{text}");
        assert!(
            text.contains("cairnflow_object_put_seconds_bucket{kind=\"session\",le=\"0.005\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("cairnflow_object_put_seconds_bucket{kind=\"wal\",le=\"1\"} 0\n"),
            "{text}"
        );
        assert!(
            text.contains("cairnflow_object_put_seconds_count{kind=\"wal\"} 1\n"),
            "{text}"
        );
    }

    /// The `cairnflow_schedules` gauge: counted on becoming leader, added to /
    /// subtracted from on `PUT`/`DELETE`, forgotten when the range leaves the
    /// node; only ranges held are added. Ranges 4 000 000+ so as not to clash
    /// with other tests in the same process.
    #[test]
    fn schedules_gauge_tracks_ranges() {
        use super::schedules::{adjust, forget_range, set_range, total};
        let mine = |r: u32| (4_000_000..4_000_010).contains(&r);
        set_range(4_000_000, 3);
        adjust(4_000_000, 1);
        adjust(4_000_001, 1);
        adjust(4_000_001, -1);
        adjust(4_000_001, -1);
        set_range(4_000_002, 7);
        assert_eq!(total(mine), 11, "4 + 0 + 7");
        forget_range(4_000_002);
        assert_eq!(total(mine), 4);
        assert_eq!(total(|r| r == 4_000_000), 4);
    }
}
