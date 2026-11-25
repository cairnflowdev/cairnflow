// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! S3-style throttling for [`super::Action::SlowDown`].
//!
//! **What S3 does** (the "Best practices design patterns: optimizing Amazon S3
//! performance" documentation, to be rechecked when calibrating): each *partitioned
//! prefix* sustains about 3 500 PUT/COPY/POST/DELETE and 5 500 GET/HEAD per second. Above
//! that S3 returns `503 SlowDown`. S3 splits hot prefixes into several partitions on its
//! own, **gradually** — while it splits, 503s keep coming. A new bucket starts with few
//! partitions.
//!
//! **The model here:**
//!
//! - A partition is a prefix by path **segment** (`a/b/c` has the prefixes `a`, `a/b`,
//!   …). S3 splits by lexicographic key ranges, not by `/`; by segment is a good enough
//!   approximation for cairn's layout (`ranges/<id>/…`, `nodewal/<node>/…`). Initially
//!   every path falls into the partition at depth [`SlowDownConfig::initial_depth`] (0 =
//!   the whole bucket is one partition — the shape of a new bucket).
//! - Each partition has two token buckets, write and read, refilled at the rate, with a
//!   capacity of one second. Out of tokens ⇒ 503.
//! - A partition **throttled continuously** (no more than 1 s between two 503s) for
//!   [`SlowDownConfig::split_after`] splits: from then on requests go down to child
//!   partitions by the next segment, each child with a full budget. At
//!   [`SlowDownConfig::max_depth`] it stops splitting. A hot object (a single path) is
//!   never rescued by splitting — just like S3.
//! - **Client-side retries.** A real 503 reaches `object_store`'s HTTP client first, and
//!   that client retries 5xx errors itself (10 attempts, 100 ms backoff doubling with
//!   jitter, capped at 15 s, 30 s total — the policy cairn builds) before returning
//!   `object_store::Error::Generic`. `FaultyStore` sits **above** the client, so it
//!   simulates that very retry loop ([`ClientRetry`]) — otherwise 503s would reach cairn
//!   at once, harsher than real S3. `client_retry: None` is that harsh mode, on purpose.
//! - Out of attempts ⇒ `Error::Generic { store: "S3", .. }` carrying the text
//!   `503 Service Unavailable` and `SlowDown` — exactly the variant `object_store` returns,
//!   and the variant SlateDB's `RetryingObjectStore` considers retryable (every error but
//!   `AlreadyExists`/`Precondition`/`NotFound`/…).
//!
//! The clock is `tokio::time::Instant`, so it runs under `start_paused`.

use {
    super::{latency::SimRng, Op},
    slatedb::object_store::{path::Path, Error},
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::time::Instant,
};

/// The 5xx retry loop of `object_store`'s HTTP client — exactly the policy cairn builds for
/// the real S3 client ([`crate::config::s3_client_retry`]): 0.14's
/// `BackoffConfig::default()`, total [`crate::config::S3_CLIENT_RETRY_TIMEOUT`] (30 s;
/// object_store's default is 180 s).
#[derive(Clone, Debug, PartialEq)]
pub struct ClientRetry {
    pub max_retries: u32,
    pub init_backoff: Duration,
    pub max_backoff: Duration,
    pub base: f64,
    /// Past this since the first attempt, stop retrying even with attempts left.
    pub timeout: Duration,
}

impl Default for ClientRetry {
    fn default() -> Self {
        ClientRetry {
            max_retries: 10,
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(15),
            base: 2.0,
            timeout: crate::config::S3_CLIENT_RETRY_TIMEOUT,
        }
    }
}

/// Parameters of the throttling rule.
#[derive(Clone, Debug, PartialEq)]
pub struct SlowDownConfig {
    /// PUT/COPY/DELETE (and multipart creation, rename) per second per partition.
    pub write_rate: f64,
    /// GET/HEAD/LIST per second per partition.
    pub read_rate: f64,
    /// Depth (number of segments) of the initial partitions.
    pub initial_depth: usize,
    /// Never split deeper than this many segments.
    pub max_depth: usize,
    /// A partition throttled continuously for this long splits.
    pub split_after: Duration,
    /// `None` = 503s reach the caller at once, no retry.
    pub client_retry: Option<ClientRetry>,
    /// Seeds the backoff jitter.
    pub seed: u64,
}

impl SlowDownConfig {
    /// S3 Standard's figures (see the module head). `split_after` is an **assumption**: AWS
    /// does not publish how fast splitting goes, only "gradually".
    pub fn s3() -> SlowDownConfig {
        SlowDownConfig {
            write_rate: 3500.0,
            read_rate: 5500.0,
            initial_depth: 0,
            max_depth: 6,
            split_after: Duration::from_secs(60),
            client_retry: Some(ClientRetry::default()),
            seed: 0,
        }
    }
}

/// A snapshot of one throttling rule's figures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlowDownStats {
    /// Number of 503 responses issued (including those the simulated client swallowed).
    pub rejected: u64,
    /// Number of retries by the simulated client after a 503.
    pub retries: u64,
    /// Number of requests whose 503 reached the caller (out of attempts).
    pub surfaced: u64,
    /// Number of partition splits.
    pub splits: u64,
}

/// A throttling rule. Cheap to clone, every clone shares the state.
#[derive(Clone, Debug)]
pub struct SlowDown(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    cfg: SlowDownConfig,
    state: Mutex<State>,
    rejected: AtomicU64,
    retries: AtomicU64,
    surfaced: AtomicU64,
    splits: AtomicU64,
}

#[derive(Debug)]
struct State {
    parts: BTreeMap<String, Part>,
    rng: SimRng,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn full(rate: f64, now: Instant) -> Bucket {
        Bucket {
            tokens: rate,
            at: now,
        }
    }

    fn take(&mut self, rate: f64, now: Instant) -> bool {
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + dt * rate).min(rate);
        self.at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct Part {
    split: bool,
    write: Bucket,
    read: Bucket,
    /// When the current run of continuous 503s started.
    hot_since: Option<Instant>,
    last_reject: Option<Instant>,
}

/// Two 503s further apart than this are two different hot spells.
const HOT_GAP: Duration = Duration::from_secs(1);

fn is_write(op: Op) -> bool {
    !matches!(op, Op::Get | Op::Head | Op::List)
}

impl SlowDown {
    pub fn new(cfg: SlowDownConfig) -> SlowDown {
        let rng = SimRng::new(cfg.seed);
        SlowDown(Arc::new(Inner {
            cfg,
            state: Mutex::new(State {
                parts: BTreeMap::new(),
                rng,
            }),
            rejected: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            surfaced: AtomicU64::new(0),
            splits: AtomicU64::new(0),
        }))
    }

    pub fn config(&self) -> &SlowDownConfig {
        &self.0.cfg
    }

    pub fn stats(&self) -> SlowDownStats {
        SlowDownStats {
            rejected: self.0.rejected.load(Ordering::Relaxed),
            retries: self.0.retries.load(Ordering::Relaxed),
            surfaced: self.0.surfaced.load(Ordering::Relaxed),
            splits: self.0.splits.load(Ordering::Relaxed),
        }
    }

    /// The **leaf** partitions present (not split), in lexicographic order.
    pub fn partitions(&self) -> Vec<String> {
        let st = self.0.state.lock().unwrap();
        st.parts
            .iter()
            .filter(|(_, p)| !p.split)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// One attempt: `true` if the partition still has budget, `false` is a 503.
    fn try_admit(&self, op: Op, path: &Path, now: Instant) -> bool {
        let cfg = &self.0.cfg;
        let segs: Vec<String> = path.parts().map(|p| p.as_ref().to_string()).collect();
        let mut st = self.0.state.lock().unwrap();
        let mut depth = cfg.initial_depth.min(segs.len());
        let key = loop {
            let key = segs[..depth].join("/");
            let part = st.parts.entry(key.clone()).or_insert_with(|| Part {
                split: false,
                write: Bucket::full(cfg.write_rate, now),
                read: Bucket::full(cfg.read_rate, now),
                hot_since: None,
                last_reject: None,
            });
            if part.split && depth < segs.len() {
                depth += 1;
                continue;
            }
            break key;
        };
        let part = st.parts.get_mut(&key).expect("just inserted");
        let ok = if is_write(op) {
            part.write.take(cfg.write_rate, now)
        } else {
            part.read.take(cfg.read_rate, now)
        };
        let cooled = |p: &Part| {
            p.last_reject
                .is_none_or(|t| now.saturating_duration_since(t) > HOT_GAP)
        };
        if ok {
            if cooled(part) {
                part.hot_since = None;
            }
            return true;
        }
        self.0.rejected.fetch_add(1, Ordering::Relaxed);
        if part.hot_since.is_none() || cooled(part) {
            part.hot_since = Some(now);
        }
        part.last_reject = Some(now);
        let hot_for = part
            .hot_since
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
        if hot_for >= cfg.split_after && depth < cfg.max_depth && depth < segs.len() {
            part.split = true;
            self.0.splits.fetch_add(1, Ordering::Relaxed);
        }
        false
    }

    /// Lets the request through, or returns a 503 once the simulated client has exhausted its retries.
    pub(super) async fn admit(&self, op: Op, path: &Path) -> Result<(), Error> {
        let start = Instant::now();
        let mut retries = 0u32;
        // Exactly object_store's `Backoff::next` algorithm: the first sleep is `init`, each
        // later one is drawn uniformly in [init, previous × base), clamped at `max`.
        let mut next = self
            .0
            .cfg
            .client_retry
            .as_ref()
            .map_or(0.0, |r| r.init_backoff.as_secs_f64());
        loop {
            if self.try_admit(op, path, Instant::now()) {
                return Ok(());
            }
            let r = match &self.0.cfg.client_retry {
                Some(r) if retries < r.max_retries && start.elapsed() < r.timeout => r,
                _ => {
                    self.0.surfaced.fetch_add(1, Ordering::Relaxed);
                    return Err(slow_down_error(op, path, start.elapsed(), retries));
                }
            };
            let init = r.init_backoff.as_secs_f64();
            let sleep = next;
            let u = self.0.state.lock().unwrap().rng.unit();
            next = (init + u * (next * r.base - init).max(0.0)).min(r.max_backoff.as_secs_f64());
            retries += 1;
            self.0.retries.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs_f64(sleep)).await;
        }
    }
}

/// An error shaped like the one `object_store` returns when S3 throttles and the client has
/// exhausted its retries.
fn slow_down_error(op: Op, path: &Path, elapsed: Duration, retries: u32) -> Error {
    let method = match op {
        Op::Get | Op::List => "GET",
        Op::Head => "HEAD",
        Op::Delete => "DELETE",
        Op::PutMultipart => "POST",
        _ => "PUT",
    };
    Error::Generic {
        store: "S3",
        source: format!(
            "Error performing {method} {path} in {elapsed:?}, after {retries} retries - \
             Server returned non-2xx status code: 503 Service Unavailable: \
             <Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error> \
             (FaultyStore)"
        )
        .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(rate: f64) -> SlowDownConfig {
        SlowDownConfig {
            write_rate: rate,
            read_rate: rate,
            initial_depth: 0,
            max_depth: 4,
            split_after: Duration::from_secs(5),
            client_retry: None,
            seed: 1,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_admits_rate_per_second_then_503() {
        let s = SlowDown::new(cfg(10.0));
        let p = Path::from("b/x");
        let now = Instant::now();
        let admitted = (0..20).filter(|_| s.try_admit(Op::Put, &p, now)).count();
        assert_eq!(admitted, 10);
        // Reads have their own budget.
        assert!(s.try_admit(Op::Get, &p, now));
        // Half a second later, 5 are refilled.
        let later = now + Duration::from_millis(500);
        assert_eq!(
            (0..20).filter(|_| s.try_admit(Op::Put, &p, later)).count(),
            5
        );
        let e = s.admit(Op::Put, &p).await.unwrap_err();
        let msg = e.to_string();
        assert!(matches!(e, Error::Generic { store: "S3", .. }), "{e:?}");
        assert!(msg.contains("503") && msg.contains("SlowDown"), "{msg}");
        assert_eq!(s.stats().surfaced, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_pressure_splits_hot_prefix_into_children() {
        let s = SlowDown::new(cfg(10.0));
        let a = Path::from("root/ranges/1/wal/x");
        let b = Path::from("root/ranges/2/wal/y");
        // 40 requests/s split over two paths, on one partition sustaining 10/s.
        let mut admitted_late = 0;
        for tick in 0..(20 * 10) {
            tokio::time::advance(Duration::from_millis(100)).await;
            let now = Instant::now();
            for i in 0..4 {
                let p = if i % 2 == 0 { &a } else { &b };
                if s.try_admit(Op::Put, p, now) && tick >= 190 {
                    admitted_late += 1;
                }
            }
        }
        let st = s.stats();
        // "" → "root" → "root/ranges" → "root/ranges/{1,2}": three splits.
        assert!(st.splits >= 3, "{st:?} {:?}", s.partitions());
        assert!(
            s.partitions().contains(&"root/ranges/1".to_string()),
            "{:?}",
            s.partitions()
        );
        // After splitting, each path does 20/s on a 10/s partition — still throttled, but
        // getting twice as much as at the start.
        assert!(admitted_late >= 18, "{admitted_late}");
    }

    #[tokio::test(start_paused = true)]
    async fn single_hot_object_never_splits_past_its_own_path() {
        let mut c = cfg(5.0);
        c.max_depth = 10;
        c.split_after = Duration::from_secs(1);
        let s = SlowDown::new(c);
        let p = Path::from("a/b");
        for _ in 0..200 {
            tokio::time::advance(Duration::from_millis(50)).await;
            for _ in 0..3 {
                s.try_admit(Op::Put, &p, Instant::now());
            }
        }
        // Splitting ends at "a/b": there is no child deeper than the object itself.
        assert_eq!(s.stats().splits, 2, "{:?}", s.partitions());
        assert!(s.partitions().contains(&"a/b".to_string()));
    }

    #[tokio::test(start_paused = true)]
    async fn client_retry_absorbs_short_bursts_like_object_store() {
        let mut c = cfg(2.0);
        c.client_retry = Some(ClientRetry::default());
        let s = SlowDown::new(c);
        let p = Path::from("x");
        let t0 = Instant::now();
        for _ in 0..6 {
            s.admit(Op::Put, &p).await.unwrap();
        }
        let st = s.stats();
        assert_eq!(st.surfaced, 0, "{st:?}");
        assert!(st.retries >= 1 && st.rejected == st.retries, "{st:?}");
        // Four requests over the 2/s budget must wait at least ~1.5 virtual seconds.
        assert!(
            t0.elapsed() >= Duration::from_millis(1500),
            "{:?}",
            t0.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn client_retry_gives_up_after_max_retries() {
        let mut c = cfg(0.0001);
        c.client_retry = Some(ClientRetry {
            max_retries: 3,
            ..ClientRetry::default()
        });
        let s = SlowDown::new(c);
        let p = Path::from("x");
        // Capacity < 1 token: no request gets through.
        let e = s.admit(Op::Get, &p).await.unwrap_err();
        assert!(e.to_string().contains("after 3 retries"), "{e}");
        assert_eq!(s.stats().retries, 3);
        assert_eq!(s.stats().rejected, 4);
    }
}
