// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The store's time, read from the `Date` header.**
//!
//! Every lease assumes the monotonic clocks of the nodes run apart by less than 1.5×
//! (LS-1, LS-2): the owner's grant is ⅔·TTL on its own clock, an observer expires it after
//! one TTL on its own. Before this module nobody measured that at run time (the GC's clock
//! gate only measures **wall-clock skew**). Here each node compares the elapsed time on its
//! monotonic clock with the elapsed time on **the store's clock** — one reference shared by
//! every node.
//!
//! **No extra request.** Every HTTP response from S3 carries a `Date` header (the server's
//! time when it answered, one-second resolution). [`DateConnector`] wraps the HTTP client
//! object_store builds for `s3://` and samples `Date` from the requests already made
//! (renewal, self-check GETs, the shared WAL…), at most one sample every
//! [`StoreClockConfig::sample_every`] — one atomic comparison while it is not due.
//! Non-HTTP stores (`memory://`, `file://`) have no samples: one process or one machine,
//! no other node's clock to drift from.
//!
//! **A bounded measurement, not an estimate.** The server stamps `Date` inside the
//! `[sent, received]` interval of the request on our clock, and the stamp's true time lies
//! in `[S, S + resolution)`. With two samples at least [`StoreClockConfig::min_span`]
//! apart, the rate (monotonic / store) lies for certain within [`Rate::lo`]…[`Rate::hi`]
//! — the user concludes "skewed" only when **the whole interval** is outside the
//! threshold, so request latency never causes a false alarm.

use {
    crate::clock::Clock,
    slatedb::object_store::client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
        ReqwestConnector,
    },
    std::{
        collections::VecDeque,
        fmt,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::Instant,
};

/// Knobs of the measurement. Defaults are for a real node; tests shorten them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreClockConfig {
    /// The two samples compared are at most this far apart (the oldest sample still in
    /// the window). Longer means tighter bounds: the 1 s resolution and the request
    /// latency are divided by it.
    pub window: Duration,
    /// Below this between two samples, nothing is concluded yet.
    pub min_span: Duration,
    /// At most one sample every this long.
    pub sample_every: Duration,
    /// A request longer than this (sent → received) is not sampled: bounds too wide.
    pub max_rtt: Duration,
    /// Cadence at which the user checks the rate (`cairn-server/src/clock_rate.rs`).
    pub check_every: Duration,
}

impl Default for StoreClockConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(5 * 60),
            min_span: Duration::from_secs(3 * 60),
            sample_every: Duration::from_secs(5),
            max_rtt: Duration::from_secs(2),
            check_every: Duration::from_secs(10),
        }
    }
}

/// One sample: the store's `Date` stamp (`store`, since `UNIX_EPOCH`, resolution `res`)
/// stamped inside the `[sent, recv]` interval of our monotonic clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    pub store: Duration,
    pub res: Duration,
    pub sent: Instant,
    pub recv: Instant,
}

/// Rate of the node's monotonic clock relative to the store clock, with bounds:
/// `> 1` = the node's clock runs faster than the store's.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rate {
    /// Certain lower bound.
    pub lo: f64,
    /// Certain upper bound.
    pub hi: f64,
    /// Middle value (for the metric).
    pub mid: f64,
    /// Distance between the two samples compared (our clock).
    pub span: Duration,
}

impl Rate {
    /// The whole interval lies outside `1 ± tolerance`: certainly skewed.
    pub fn outside(&self, tolerance: f64) -> bool {
        self.lo > 1.0 + tolerance || self.hi < 1.0 - tolerance
    }

    /// The whole interval lies inside `1 ± tolerance`: certainly within the threshold.
    pub fn inside(&self, tolerance: f64) -> bool {
        self.lo >= 1.0 - tolerance && self.hi <= 1.0 + tolerance
    }
}

/// Store-time samples of one store layer. `Clone` is cheap; every copy shares one log.
#[derive(Clone)]
pub struct StoreClock(Arc<Inner>);

struct Inner {
    clock: Arc<dyn Clock>,
    cfg: StoreClockConfig,
    base: Instant,
    /// `recv` of the last sample, ms since `base` + 1 (0 = none yet): the cheap gate of
    /// [`StoreClock::record`] — no lock while it is not due.
    last_ms: AtomicU64,
    samples: Mutex<VecDeque<Sample>>,
}

impl fmt::Debug for StoreClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreClock")
            .field("cfg", &self.0.cfg)
            .field("samples", &self.0.samples.lock().map(|s| s.len()).ok())
            .finish()
    }
}

impl StoreClock {
    /// Sample log on clock `clock` (real node: [`crate::clock::SystemClock`]).
    pub fn new(clock: Arc<dyn Clock>, cfg: StoreClockConfig) -> Self {
        let base = clock.mono();
        Self(Arc::new(Inner {
            clock,
            cfg,
            base,
            last_ms: AtomicU64::new(0),
            samples: Mutex::new(VecDeque::new()),
        }))
    }

    /// A real node's log: [`crate::clock::SystemClock`], default knobs.
    pub fn system() -> Self {
        Self::new(
            Arc::new(crate::clock::SystemClock),
            StoreClockConfig::default(),
        )
    }

    pub fn config(&self) -> StoreClockConfig {
        self.0.cfg
    }

    /// The node's monotonic time on the log's clock — the `sent`/`recv` mark of a sample.
    pub fn now(&self) -> Instant {
        self.0.clock.mono()
    }

    /// Records a sample (dropped if [`StoreClockConfig::sample_every`] has not passed
    /// since the previous sample, or the request exceeded [`StoreClockConfig::max_rtt`]).
    pub fn record(&self, store: Duration, res: Duration, sent: Instant, recv: Instant) {
        let cfg = &self.0.cfg;
        if recv < sent || recv - sent > cfg.max_rtt {
            return;
        }
        let at = recv.saturating_duration_since(self.0.base).as_millis() as u64 + 1;
        let last = self.0.last_ms.load(Ordering::Relaxed);
        if last != 0 && at.saturating_sub(last) < cfg.sample_every.as_millis() as u64 {
            return;
        }
        if self
            .0
            .last_ms
            .compare_exchange(last, at, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let Ok(mut s) = self.0.samples.lock() else {
            return;
        };
        s.push_back(Sample {
            store,
            res,
            sent,
            recv,
        });
        // Keep enough for one window, plus one window so the oldest sample always remains.
        let keep = cfg.window * 2;
        while s
            .front()
            .is_some_and(|f| recv.saturating_duration_since(f.recv) > keep)
        {
            s.pop_front();
        }
    }

    /// Rate over a pair of samples: the newest one and the oldest one still in the
    /// [`StoreClockConfig::window`]. `None` before [`StoreClockConfig::min_span`] is
    /// reached, or when the newest sample is older than one window (the store stopped
    /// answering — nothing new to say).
    pub fn rate(&self) -> Option<Rate> {
        let cfg = &self.0.cfg;
        let now = self.now();
        let s = self.0.samples.lock().ok()?;
        let j = *s.back()?;
        if now.saturating_duration_since(j.recv) > cfg.window {
            return None;
        }
        let i = *s
            .iter()
            .find(|i| j.recv.saturating_duration_since(i.recv) <= cfg.window)?;
        rate_between(&i, &j, cfg.min_span)
    }
}

/// Rate between two samples `i` (old) and `j` (new), with bounds — see the module head.
pub fn rate_between(i: &Sample, j: &Sample, min_span: Duration) -> Option<Rate> {
    let gap = j.sent.checked_duration_since(i.recv)?;
    if gap < min_span {
        return None;
    }
    let secs = |d: Duration| d.as_secs_f64();
    let (si, sj) = (secs(i.store), secs(j.store));
    // Store time between the two stamps, given each stamp's resolution.
    let store_lo = sj - si - secs(i.res);
    let store_hi = sj + secs(j.res) - si;
    if store_lo <= 0.0 {
        return None;
    }
    // Monotonic time between the two stampings.
    let mono_lo = secs(j.sent.checked_duration_since(i.recv)?);
    let mono_hi = secs(j.recv.checked_duration_since(i.sent)?);
    let mid_mono = (mono_lo + mono_hi) / 2.0;
    let mid_store = (store_lo + store_hi) / 2.0;
    Some(Rate {
        lo: mono_lo / store_hi,
        hi: mono_hi / store_lo,
        mid: mid_mono / mid_store,
        span: gap,
    })
}

/// The time in a `Date` header in IMF-fixdate form (RFC 9110 §5.6.7:
/// `Sun, 06 Nov 1994 08:49:37 GMT`), since `UNIX_EPOCH`. Obsolete forms (RFC 850,
/// asctime) ⇒ `None` — S3 servers do not send them, and dropping the sample is safe.
pub fn parse_http_date(v: &str) -> Option<Duration> {
    let mut it = v.split_ascii_whitespace();
    let _wkday = it.next()?.strip_suffix(',')?;
    let day: u32 = it.next()?.parse().ok()?;
    let month = match it.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: i64 = it.next()?.parse().ok()?;
    let mut hms = it.next()?.split(':');
    let h: u64 = hms.next()?.parse().ok()?;
    let m: u64 = hms.next()?.parse().ok()?;
    let s: u64 = hms.next()?.parse().ok()?;
    if it.next()? != "GMT" || it.next().is_some() || hms.next().is_some() {
        return None;
    }
    if !(1..=31).contains(&day) || h > 23 || m > 59 || s > 60 || year < 1970 {
        return None;
    }
    // Days since 1970-01-01 (Howard Hinnant's `days_from_civil` algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (i64::from(month) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = u64::try_from(days).ok()? * 86_400 + h * 3600 + m * 60 + s;
    Some(Duration::from_secs(secs))
}

/// The S3 client's [`HttpConnector`]: object_store's default reqwest client, wrapped to
/// sample the `Date` header into `clock`.
#[derive(Debug)]
pub struct DateConnector {
    clock: StoreClock,
}

impl DateConnector {
    pub fn new(clock: StoreClock) -> Self {
        Self { clock }
    }
}

impl HttpConnector for DateConnector {
    fn connect(
        &self,
        options: &slatedb::object_store::ClientOptions,
    ) -> slatedb::object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(DateSampler {
            inner,
            clock: self.clock.clone(),
        }))
    }
}

#[derive(Debug)]
struct DateSampler {
    inner: HttpClient,
    clock: StoreClock,
}

#[async_trait::async_trait]
impl HttpService for DateSampler {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let sent = self.clock.now();
        let resp = self.inner.execute(req).await?;
        let recv = self.clock.now();
        if let Some(at) = resp
            .headers()
            .get("date")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_http_date)
        {
            self.clock.record(at, Duration::from_secs(1), sent, recv);
        }
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::clock::ManualClock};

    /// IMF-fixdate yields the exact Unix second; odd forms ⇒ `None`.
    #[test]
    fn parses_imf_fixdate() {
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(Duration::from_secs(784_111_777))
        );
        assert_eq!(
            parse_http_date("Mon, 19 Oct 2026 06:13:38 GMT"),
            Some(Duration::from_secs(1_792_390_418))
        );
        assert_eq!(
            parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"),
            Some(Duration::ZERO)
        );
        assert_eq!(
            parse_http_date("Tue, 29 Feb 2028 23:59:59 GMT"),
            Some(Duration::from_secs(1_835_481_599))
        );
        for bad in [
            "",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Sun Nov  6 08:49:37 1994",
            "Sun, 06 Nov 1994 08:49:37 UTC",
            "Sun, 06 Foo 1994 08:49:37 GMT",
            "Sun, 32 Nov 1994 08:49:37 GMT",
            "Sun, 06 Nov 1994 24:00:00 GMT",
            "Sun, 06 Nov 1994 08:49 GMT",
        ] {
            assert_eq!(parse_http_date(bad), None, "{bad:?}");
        }
    }

    fn sample(store_s: f64, sent: Instant, rtt_ms: u64) -> Sample {
        Sample {
            store: Duration::from_secs_f64(store_s),
            res: Duration::from_secs(1),
            sent,
            recv: sent + Duration::from_millis(rtt_ms),
        }
    }

    /// Certain bounds: a clock at the right rate ⇒ the interval contains 1 and is narrow
    /// after 5 minutes; 1.3× fast ⇒ the whole interval is above 1.1; slow ⇒ the whole
    /// interval is below 0.9; below `min_span` ⇒ nothing said yet.
    #[tokio::test(start_paused = true)]
    async fn rate_bounds_hold_the_true_rate() {
        let t0 = Instant::now();
        let span = Duration::from_secs(300);
        for (true_rate, want_outside) in
            [(1.0, false), (1.3, true), (1.0 / 1.3, true), (1.02, false)]
        {
            // The store advances 300 s; the node 300·r. 400 ms latency per request, the
            // true stamp in the middle; store seconds are truncated (resolution).
            let i = sample(1_000.0, t0, 400);
            let mono = Duration::from_secs_f64(span.as_secs_f64() * true_rate);
            let j = sample((1_000.0f64 + 300.0 + 0.7).floor(), t0 + mono, 400);
            let r = rate_between(&i, &j, Duration::from_secs(120)).expect("enough span");
            assert!(
                r.lo <= true_rate * 1.0001 && true_rate <= r.hi * 1.0001,
                "r={true_rate}: {r:?}"
            );
            assert_eq!(r.outside(0.10), want_outside, "r={true_rate}: {r:?}");
            assert!(r.hi - r.lo < 0.02, "bounds too wide: {r:?}");
        }
        let i = sample(1_000.0, t0, 10);
        let j = sample(1_060.0, t0 + Duration::from_secs(60), 10);
        assert_eq!(rate_between(&i, &j, Duration::from_secs(120)), None);
    }

    /// [`DateConnector`] samples the `Date` of a real S3 client response — even an error
    /// response (`404`), with no extra request.
    #[tokio::test]
    async fn date_connector_samples_real_responses() {
        use {
            slatedb::object_store::{aws::AmazonS3Builder, path::Path, ObjectStoreExt},
            tokio::io::{AsyncReadExt, AsyncWriteExt},
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = Arc::new(AtomicU64::new(0));
        let count = served.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                count.fetch_add(1, Ordering::Relaxed);
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nDate: Mon, 19 Oct 2026 06:13:38 GMT\r\n\
                          Content-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            }
        });
        let clock = StoreClock::system();
        let store = AmazonS3Builder::new()
            .with_bucket_name("b")
            .with_region("us-east-1")
            .with_access_key_id("k")
            .with_secret_access_key("s")
            .with_endpoint(format!("http://{addr}"))
            .with_allow_http(true)
            .with_http_connector(DateConnector::new(clock.clone()))
            .build()
            .unwrap();
        let r = store.head(&Path::from("x")).await;
        assert!(r.is_err(), "404 ⇒ NotFound");
        assert_eq!(served.load(Ordering::Relaxed), 1, "exactly one request");
        let s = clock.0.samples.lock().unwrap().clone();
        assert_eq!(s.len(), 1, "{s:?}");
        assert_eq!(s[0].store, Duration::from_secs(1_792_390_418));
        assert_eq!(s[0].res, Duration::from_secs(1));
        assert!(s[0].sent <= s[0].recv);
    }

    /// The log takes at most one sample every `sample_every`, drops requests over
    /// `max_rtt`, compares the newest sample with the oldest in the window; newest sample
    /// older than one window ⇒ `None`.
    #[tokio::test(start_paused = true)]
    async fn store_clock_samples_and_measures() {
        let manual = Arc::new(ManualClock::new(0));
        let cfg = StoreClockConfig {
            window: Duration::from_secs(10),
            min_span: Duration::from_secs(5),
            sample_every: Duration::from_secs(1),
            max_rtt: Duration::from_millis(500),
            check_every: Duration::from_millis(100),
        };
        let c = StoreClock::new(manual.clone(), cfg);
        // The node runs 1.3× fast: every store second, the node clock advances 1.3 s.
        let mut store = Duration::from_secs(5_000);
        for k in 0..30 {
            let sent = c.now();
            let recv = sent + Duration::from_millis(5);
            c.record(store, Duration::from_millis(1), sent, recv);
            // A request slower than the cap: dropped.
            c.record(
                store,
                Duration::from_millis(1),
                sent,
                sent + Duration::from_secs(1),
            );
            if k == 0 {
                assert_eq!(c.rate(), None, "one sample");
            }
            manual.advance(Duration::from_millis(1_300));
            store += Duration::from_secs(1);
        }
        let r = c.rate().expect("enough samples");
        assert!(r.outside(0.10) && r.lo > 1.25 && r.hi < 1.35, "{r:?}");
        assert!(r.span <= cfg.window, "{r:?}");
        manual.advance(cfg.window * 2);
        assert_eq!(c.rate(), None, "sample is stale");
    }
}
