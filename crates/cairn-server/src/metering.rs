// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant metering — what billing is computed from.
//!
//! Four numbers per `(tenant, hour)`: invocations created, **durable steps**
//! (SDK commands written to the journal), run time (ms, from when a run gets a
//! slot until it stops), and **CU** combined from the other three
//! ([`run_cu`]); plus tokens, bytes exchanged with the SDK endpoint, and
//! payload-weighted **billing units** ([`Usage::units`] — counted at the
//! range's commit, only once durable). No external DB: each node keeps its own
//! numbers in RAM and periodically flushes them as objects
//! `metering/<tenant>/<yyyy-mm-ddThh>/<node>.<boot>` — **accumulated within
//! the hour, overwritten**, so an object is always the total of **that
//! process** for that hour up to its latest flush. `<boot>` is the process's
//! random id ([`boot_id`]): a node restarted with a fixed `--node-id` counts
//! from 0 again, and writing to the old name would overwrite the previous
//! life's numbers. No process writes another process's object ⇒ no CAS
//! needed. Reading merges every object of a `(tenant, hour)`
//! (`GET /metering`) — including the bare `<node>` names written by older
//! binaries (a new object, read leniently: no `DATA_FORMAT` change).
//!
//! A graceful shutdown flushes one last time after every engine has stopped
//! ([`Flusher::flush`], called from `RunningNode::shutdown`).
//!
//! **A limit that must be stated:** a node that dies (`kill -9`) loses what
//! it has not flushed (at most one flush interval).
//! Metering here is operational data good enough for tiered billing, not a
//! ledger; an exact per-step ledger would have to live in the commit of the
//! step itself (one row per step — twice the write cost), and that is a
//! different decision.

use {
    serde::{Deserialize, Serialize},
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt, PutPayload},
    std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    },
};

/// Tokens and cost of one model within a `(tenant, hour)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_micros: u64,
}

impl ModelUsage {
    fn add(&mut self, o: &ModelUsage) {
        self.input_tokens += o.input_tokens;
        self.output_tokens += o.output_tokens;
        self.cost_micros += o.cost_micros;
    }
}

/// Maximum number of models tracked separately per `(tenant, hour)`: model
/// names come from SDK spans, so they must not bloat the registry in RAM or
/// the flushed object. Model `MODELS_MAX + 1` onwards is added to
/// [`OTHER_MODELS`].
pub const MODELS_MAX: usize = 32;
/// Names longer than this many bytes are cut (at a character boundary).
pub const MODEL_NAME_MAX: usize = 64;
/// The row merging the models beyond [`MODELS_MAX`].
pub const OTHER_MODELS: &str = "(other)";

/// The numbers of one `(tenant, hour)`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    pub invocations: u64,
    pub steps: u64,
    pub run_ms: u64,
    /// The shared unit for throttling and invoices — see [`run_cu`]. Objects
    /// flushed before this field existed lack it: read as 0.
    #[serde(default)]
    pub cu: u64,
    /// Input/output tokens summed from `$trace` spans on receipt. Objects
    /// flushed before this field existed lack it: read as 0.
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    /// Token cost (micro-USD) from the `pricing` table at receipt.
    #[serde(default)]
    pub cost_micros: u64,
    /// The per-model share of tokens/cost — only spans with a `model`; the
    /// totals above include tokens of unknown model. Objects flushed before
    /// this field existed lack it: read as empty (lenient read, no
    /// `DATA_FORMAT` change). Older binaries reading new objects ignore the
    /// unknown field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub models: BTreeMap<String, ModelUsage>,
    /// Bytes **on the wire** sent to the tenant's endpoint (the turn's request
    /// body, after compression — exactly what the cell pays egress for) and
    /// received back. Objects flushed before this field existed lack it: read
    /// as 0 (lenient read).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub sdk_bytes_out: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub sdk_bytes_in: u64,
    /// Payload-weighted **billing units** (shaped like Restate Cloud's public
    /// "action" pricing): each durable command once durable counts
    /// `max(1, ⌈payload / 64 KiB⌉)`, each invocation adds 2, each new artifact
    /// counts by its bytes. Counted at commit, only once durable
    /// (`cairn_store::units`), drained into this registry at flush. Objects
    /// flushed before this field existed lack it: read as 0 (lenient read).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub units: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

impl Usage {
    fn add(&mut self, o: &Usage) {
        self.invocations += o.invocations;
        self.steps += o.steps;
        self.run_ms += o.run_ms;
        self.cu += o.cu;
        self.input_tokens += o.input_tokens;
        self.output_tokens += o.output_tokens;
        self.cost_micros += o.cost_micros;
        for (m, u) in &o.models {
            self.model_entry(m).add(u);
        }
        self.sdk_bytes_out += o.sdk_bytes_out;
        self.sdk_bytes_in += o.sdk_bytes_in;
        self.units += o.units;
    }

    /// The row of model `name` (long names cut, overflow beyond [`MODELS_MAX`] merged).
    fn model_entry(&mut self, name: &str) -> &mut ModelUsage {
        let mut end = name.len().min(MODEL_NAME_MAX);
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        let name = &name[..end];
        let key = if self.models.contains_key(name)
            || self.models.len() < MODELS_MAX
            || name == OTHER_MODELS
        {
            name
        } else {
            OTHER_MODELS
        };
        self.models.entry(key.to_string()).or_default()
    }
}

/// One CU of time is this many microseconds of running.
const CU_RUN_US: u128 = 100_000;

/// The run-time part of CU for **one turn**: `⌈run_ms / 100⌉`, rounded up in
/// microseconds — any turn that really ran costs at least 1 CU.
///
/// **CU** = durable steps + `⌈run_ms / 100⌉` per turn + 1 per invocation
/// created. One number for both throttling (the bucket of `crate::rate`) and
/// invoices: the customer sees the same number in a `429` and in
/// `GET /metering`.
pub fn run_cu(d: std::time::Duration) -> u64 {
    d.as_micros().div_ceil(CU_RUN_US) as u64
}

/// `(tenant, UTC hour as yyyy-mm-ddThh)` → numbers, accumulated for the process.
static METER: Mutex<BTreeMap<(String, String), Usage>> = Mutex::new(BTreeMap::new());

/// Clears the process's counting registry — for deterministic simulation,
/// where many runs share one process: a later run must not flush the numbers
/// of an earlier one.
#[cfg(feature = "testkit")]
pub fn reset_for_sim() {
    if let Ok(mut m) = METER.lock() {
        m.clear();
    }
    cairn_store::units::reset();
}

/// The current UTC hour, `yyyy-mm-ddThh`.
pub fn hour_now() -> String {
    hour_of(
        cairn_store::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    )
}

/// `yyyy-mm-ddThh` of a Unix timestamp (seconds) — Gregorian calendar, no library.
pub fn hour_of(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let hour = (secs % 86_400) / 3_600;
    // Howard Hinnant's inverse "days from civil" algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hour:02}")
}

fn bump(tenant: &str, f: impl FnOnce(&mut Usage)) {
    if let Ok(mut m) = METER.lock() {
        f(m.entry((tenant.to_string(), hour_now())).or_default());
    }
}

pub fn invocation(tenant: &str) {
    bump(tenant, |u| {
        u.invocations += 1;
        u.cu += 1;
    });
}

pub fn steps(tenant: &str, n: u64) {
    if n > 0 {
        bump(tenant, |u| {
            u.steps += n;
            u.cu += n;
        });
    }
}

/// The loaded price table (`crate::pricing`): model → price.
static PRICES: Mutex<BTreeMap<String, crate::pricing::Price>> = Mutex::new(BTreeMap::new());

/// Replaces the process's price table (loaded from the `pricing` object).
pub fn set_prices(models: BTreeMap<String, crate::pricing::Price>) {
    if let Ok(mut p) = PRICES.lock() {
        *p = models;
    }
}

/// Tokens of one `$trace` span: added in RAM, flushed on the same interval — no
/// extra commit. A model without a price ⇒ cost 0.
pub fn tokens(tenant: &str, model: Option<&str>, input: u64, output: u64) {
    if input == 0 && output == 0 {
        return;
    }
    let cost = model
        .and_then(|m| PRICES.lock().ok().and_then(|p| p.get(m).copied()))
        .map_or(0, |p| p.cost(input, output));
    bump(tenant, |u| {
        u.input_tokens += input;
        u.output_tokens += output;
        u.cost_micros += cost;
        if let Some(m) = model.filter(|m| !m.is_empty()) {
            u.model_entry(m).add(&ModelUsage {
                input_tokens: input,
                output_tokens: output,
                cost_micros: cost,
            });
        }
    });
}

/// Bytes on the wire with `tenant`'s endpoint: once per RR turn, once per
/// BIDI stream when it closes — not per frame.
pub fn sdk_bytes(tenant: &str, out: u64, received: u64) {
    if out == 0 && received == 0 {
        return;
    }
    bump(tenant, |u| {
        u.sdk_bytes_out += out;
        u.sdk_bytes_in += received;
    });
}

/// The time of **one** turn — CU is rounded per turn ([`run_cu`]).
pub fn run_time(tenant: &str, d: std::time::Duration) {
    bump(tenant, |u| {
        u.run_ms += d.as_millis() as u64;
        u.cu += run_cu(d);
    });
}

/// Drains the **durable** billing units of every range in the process
/// (`cairn_store::units::collect`) into the registry, by the hour of the
/// commit. Off the turn path: runs at flush and at snapshot.
fn absorb_units() {
    let units = cairn_store::units::collect();
    if units.is_empty() {
        return;
    }
    if let Ok(mut m) = METER.lock() {
        for ((tenant, hour), n) in units {
            m.entry((tenant, hour_of(hour.saturating_mul(3_600))))
                .or_default()
                .units += n;
        }
    }
}

/// Snapshot of the process's numbers — for tests and for flushing.
pub fn snapshot() -> BTreeMap<(String, String), Usage> {
    absorb_units();
    METER.lock().map(|m| m.clone()).unwrap_or_default()
}

fn dir(prefix: &Path) -> String {
    if prefix.as_ref().is_empty() {
        "metering".into()
    } else {
        format!("{}/metering", prefix.as_ref())
    }
}

/// The process's random id (16 hex digits), once for the whole life of
/// [`METER`]: the suffix of this process's flushed object names. Not the pid —
/// in a container the pid is often the same across restarts. Goes through
/// [`cairn_store::entropy::boot_nonce`] so deterministic simulation gets the
/// id of the run itself.
pub fn boot_id() -> String {
    format!("{:016x}", cairn_store::entropy::boot_nonce())
}

/// A node's flusher: the background interval and the final flush at graceful
/// shutdown share the "already flushed" registry (`last`).
#[derive(Clone)]
pub struct Flusher {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    node: String,
    last: Arc<tokio::sync::Mutex<BTreeMap<(String, String), Usage>>>,
}

impl Flusher {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, node: String) -> Self {
        Self {
            store,
            prefix,
            node,
            last: Arc::default(),
        }
    }

    /// One flush ([`flush`]).
    pub async fn flush(&self) {
        let mut last = self.last.lock().await;
        flush(&self.store, &self.prefix, &self.node, &mut last).await;
    }
}

/// Flushes this process's numbers: one object per `(tenant, hour)` changed
/// since last time. Keeps the two most recent hours in RAM (the previous hour
/// can still receive late numbers from a turn straddling the hour boundary);
/// older ones get one last flush and are dropped.
pub async fn flush(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    node: &str,
    last: &mut BTreeMap<(String, String), Usage>,
) {
    let now = snapshot();
    let hour = hour_now();
    for ((tenant, h), usage) in &now {
        if last.get(&(tenant.clone(), h.clone())) == Some(usage) {
            continue;
        }
        let path = Path::from(format!(
            "{}/{tenant}/{h}/{}.{}",
            dir(prefix),
            segment(node),
            boot_id()
        ));
        let body = serde_json::to_vec(usage).unwrap_or_default();
        match store.put(&path, PutPayload::from(body)).await {
            Ok(_) => {
                last.insert((tenant.clone(), h.clone()), usage.clone());
            }
            Err(e) => {
                tracing::debug!(tenant, hour = %h, error = %e, "metering flush failed, next tick retries")
            }
        }
    }
    // Drop hours already flushed and older than the previous hour.
    let keep_from = {
        let secs = cairn_store::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        hour_of(secs.saturating_sub(3_600))
    };
    if let Ok(mut m) = METER.lock() {
        m.retain(|(t, h), u| {
            h.as_str() >= keep_from.as_str()
                || h == &hour
                || last.get(&(t.clone(), h.clone())) != Some(u)
        });
    }
    last.retain(|(_, h), _| h.as_str() >= keep_from.as_str());
}

/// The total of every node for `(tenant, hour)`, read from the bucket.
pub async fn read(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    hour: &str,
) -> Result<Usage, String> {
    let dir = Path::from(format!("{}/{tenant}/{hour}", dir(prefix)));
    let mut total = Usage::default();
    let listed = list(store, &dir).await?;
    for loc in listed {
        if let Ok(got) = store.get(&loc).await {
            if let Ok(b) = got.bytes().await {
                if let Ok(u) = serde_json::from_slice::<Usage>(&b) {
                    total.add(&u);
                }
            }
        }
    }
    Ok(total)
}

/// Cap on the number of hours of [`read_range`]: 31 days.
pub const RANGE_HOURS_MAX: i64 = 31 * 24;

/// The hour index (hours since 1970-01-01T00 UTC) of `yyyy-mm-ddThh`; `None`
/// when malformed — it is also an object path segment, so everything else is
/// rejected.
pub fn hour_index(h: &str) -> Option<i64> {
    let b = h.as_bytes();
    let shape = b.len() == 13
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            _ => c.is_ascii_digit(),
        });
    if !shape {
        return None;
    }
    let num = |r: std::ops::Range<usize>| h[r].parse::<i64>().ok();
    let (y, m, d, hh) = (num(0..4)?, num(5..7)?, num(8..10)?, num(11..13)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 {
        return None;
    }
    // Howard Hinnant's "days from civil" — the inverse of [`hour_of`].
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let idx = days * 24 + hh;
    // A date that does not exist (31/02) does not round-trip to the same string.
    (hour_of((idx * 3_600) as u64) == h).then_some(idx)
}

/// The total of every node for each hour with numbers in `[from, to]` for
/// `tenant`, ordered by hour. One LIST by tenant prefix starting at `from`
/// (`list_with_offset`); no early stop by order, because file stores do not
/// promise lexicographic order.
pub async fn read_range(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tenant: &str,
    from: &str,
    to: &str,
) -> Result<Vec<(String, Usage)>, String> {
    use futures::TryStreamExt;
    let base = format!("{}/{tenant}", dir(prefix));
    let dir_path = Path::from(base.clone());
    // Every object of hour `from` has a name > `<base>/<from>` (plus `/node.boot`).
    let offset = Path::from(format!("{base}/{from}"));
    let listed: Vec<_> = store
        .list_with_offset(Some(&dir_path), &offset)
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    let mut by_hour: BTreeMap<String, Usage> = BTreeMap::new();
    for meta in listed {
        let rel = meta
            .location
            .as_ref()
            .strip_prefix(dir_path.as_ref())
            .map(|r| r.trim_start_matches('/'))
            .unwrap_or_default();
        let Some(hour) = rel.split('/').next().filter(|h| hour_index(h).is_some()) else {
            continue;
        };
        if hour < from || hour > to {
            continue;
        }
        let hour = hour.to_string();
        if let Ok(got) = store.get(&meta.location).await {
            if let Ok(b) = got.bytes().await {
                if let Ok(u) = serde_json::from_slice::<Usage>(&b) {
                    by_hour.entry(hour).or_default().add(&u);
                }
            }
        }
    }
    Ok(by_hour.into_iter().collect())
}

async fn list(store: &Arc<dyn ObjectStore>, dir: &Path) -> Result<Vec<Path>, String> {
    use futures::TryStreamExt;
    let listed: Vec<_> = store
        .list(Some(dir))
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    Ok(listed.into_iter().map(|m| m.location).collect())
}

/// A node id as a path segment: every character outside `[A-Za-z0-9_.-]` becomes `_`.
pub(crate) fn path_segment(node: &str) -> String {
    segment(node)
}

fn segment(node: &str) -> String {
    node.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The boot id goes through `entropy`: deterministic simulation runs many
    /// clusters in one process, each with its own source — the id must belong
    /// to **the installed source**, not to the first run in the process, and
    /// reading it must not draw any random byte from the source (drawing on
    /// the first read but not later ones shifts the random stream between two
    /// runs of the same seed — seed 1 of `sim_cluster` diverged exactly here).
    #[test]
    fn boot_id_follows_installed_entropy_source() {
        struct Src(u32);
        impl cairn_store::entropy::Source for Src {
            fn now(&self) -> std::time::SystemTime {
                std::time::UNIX_EPOCH
            }
            fn process_id(&self) -> u32 {
                self.0
            }
            fn fill(&self, _: &mut [u8]) {
                panic!("boot_id drew random bytes from the source");
            }
        }
        let a = {
            let _g = cairn_store::entropy::install(std::rc::Rc::new(Src(1)));
            (boot_id(), boot_id())
        };
        let b = {
            let _g = cairn_store::entropy::install(std::rc::Rc::new(Src(2)));
            boot_id()
        };
        assert_eq!(a.0, a.1, "one source ⇒ one id for life");
        assert_ne!(
            a.0, b,
            "another source ⇒ another id — not stuck with the previous run's id"
        );
    }

    /// `CU = steps + ⌈run_ms / 100⌉ + 1 per invocation`: added to `cu` at
    /// exactly the three places that count the three older numbers, and the
    /// three older numbers do not change.
    #[test]
    fn cu_is_steps_plus_time_ceiling_plus_one_per_invocation() {
        let t = format!("cu-test-{}", cairn_store::entropy::process_id());
        invocation(&t);
        steps(&t, 3);
        run_time(&t, std::time::Duration::from_millis(250));
        run_time(&t, std::time::Duration::from_micros(1));
        let u = snapshot()
            .into_iter()
            .find(|((tt, _), _)| *tt == t)
            .map(|(_, u)| u)
            .unwrap();
        assert_eq!((u.invocations, u.steps, u.run_ms), (1, 3, 250));
        // 1 (invocation) + 3 (steps) + ⌈250/100⌉ + ⌈0.001/100⌉ = 1 + 3 + 3 + 1.
        assert_eq!(u.cu, 8);
        assert_eq!(run_cu(std::time::Duration::ZERO), 0);
        assert_eq!(run_cu(std::time::Duration::from_millis(100)), 1);
        assert_eq!(run_cu(std::time::Duration::from_micros(100_001)), 2);
    }

    #[test]
    fn utc_hours_follow_calendar() {
        assert_eq!(hour_of(0), "1970-01-01T00");
        // 2026-10-10 10:30:00 UTC.
        assert_eq!(hour_of(1_791_628_200), "2026-10-10T10");
        // Leap day.
        assert_eq!(hour_of(951_782_400 + 3_600 * 23), "2000-02-29T23");
    }
}
