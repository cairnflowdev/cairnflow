// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Partition keys and the partition table — the "ids carry no partition number" half of
//! cairn's key design.
//!
//! The old id format carried **the partition number** in its first two bytes, so N was
//! frozen forever: changing N meant changing the id→partition mapping of every id ever
//! issued. This module splits apart the two questions the old format mixed into one:
//!
//! 1. **Where an id belongs in the key space** — a `u64` [`PartitionKey`], deterministic
//!    in the call's target, carried in the id forever.
//! 2. **Who currently holds that place** — a versioned [`PartitionTable`] that divides
//!    `u64` into contiguous ranges, one [`RangeId`] each. The table can change at runtime
//!    (CAS on `cluster.conf`, see [`crate::ClusterConfig::update`]); the key in the id
//!    cannot.
//!
//! Keeping a stable key inside the id and a separate table mapping key ranges to
//! partitions is what lets ranges split and merge without touching any id already issued.
//!
//! Range identity (ids never reused, `gen`, `intent`) follows D22.

use {
    crate::{
        error::{Result, StoreError},
        ids::PartitionId,
        scope::{Scope, TenantId},
    },
    sha2::{Digest, Sha256},
    std::sync::Arc,
};

/// The position of an invocation in the `u64` key space.
///
/// It sits in the **first 8 bytes** (big-endian) of the 24-byte id. Big-endian so the
/// byte order of ids matches the numeric order of keys — a key range is a contiguous id
/// range, and that is the precondition for one day splitting partitions by byte range
/// (SlateDB's `projection_range`) without rewriting data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey(pub u64);

/// Domain tag of the partition key hash. The `/2` suffix is id format v2; this hash is a
/// permanent contract — changing it changes the partition of every object key.
const KEY_DOMAIN: &[u8] = b"cairnflow-pk/2";

/// The first eight bytes of SHA-256 over a **domain-tagged, length-prefixed** tuple.
///
/// - Domain tag + `kind` separate the four key sources: scope `"x"` does not collide with
///   object key `("x", "")`.
/// - Length prefix: `("ab","c")` and `("a","bc")` do not mix their byte streams.
/// - SHA-256 rather than FNV: FNV-1a has weak avalanche in the low bits (changing the
///   completion id barely changes `h % N` — it once had to be patched with splitmix in
///   `turn.rs`), and `sha2` is already a dependency.
///
/// **Not a security property.** With any public hash function, a caller wanting to land on
/// a chosen partition only needs to try about N keys. What we need here is stability and
/// even spread, not resistance against an attacker.
fn hash_tuple(kind: &str, segs: &[&[u8]]) -> PartitionKey {
    let mut h = Sha256::new();
    h.update(KEY_DOMAIN);
    for seg in std::iter::once(kind.as_bytes()).chain(segs.iter().copied()) {
        h.update((seg.len() as u64).to_be_bytes());
        h.update(seg);
    }
    let digest = h.finalize();
    PartitionKey(u64::from_be_bytes(
        digest[..8].try_into().expect("SHA-256 is 32 bytes long"),
    ))
}

/// Partition key for a call about to be created **in tenant `tenant`**. The same four
/// levels as [`crate::ClusterConfig::partition_for_target`], considered in order:
///
/// 0. **A scope** ⇒ `H(scope)`, beating everything else. Every target in one scope shares
///    **one** key, hence one partition under *every* table — the precondition for the
///    scope's concurrency counter being a variable in RAM (D-F1).
/// 1. **An object key** ⇒ `H(service, key)`: two invocations of the same object must meet
///    in the same queue, otherwise `EXCLUSIVE` loses its meaning.
/// 2. **An idempotency key** ⇒ `H(service, idem)`: the `idempotency` table is state of the
///    target partition, so every call with the same key must reach the same place.
/// 3. **No key** ⇒ `None`. The caller assigns one itself: random at HTTP ingress,
///    [`partition_key_for_callee`] for `ctx.call`.
///
/// **The tenant goes into the hash**: `Cart/x` of two tenants is two objects — two
/// partition keys, hence two places in the keyspace (the owner of a state key is this
/// partition key). The tenant is not a level of its own: with no key it is still `None`,
/// the tenant does not invent a target. Tenant `default` keeps **exactly** the hash from
/// before tenants existed; other tenants hash under a `tenant-…` tag with the tenant as the
/// first segment — two different tag families, so no key tuple of one tenant shares a byte
/// stream with a key tuple of another.
///
/// An empty key is not a key — `object_key == ""` falls through to the next level, exactly
/// as `partition_for_target` treats it today.
pub fn partition_key_for_target(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    object_key: &str,
    idempotency_key: Option<&str>,
) -> Option<PartitionKey> {
    let hash = |kind: &str, segs: &[&[u8]]| -> PartitionKey {
        if tenant.is_default() {
            hash_tuple(kind, segs)
        } else {
            let mut all: Vec<&[u8]> = Vec::with_capacity(segs.len() + 1);
            all.push(tenant.as_str().as_bytes());
            all.extend_from_slice(segs);
            hash_tuple(&format!("tenant-{kind}"), &all)
        }
    };
    if let Some(s) = scope {
        // Through `Scope::partition_key`, not `as_str`: the escape hatch for a huge scope
        // (spreading one scope over k partitions) is to change exactly that function, and
        // it stays cheap only as long as there is no second place hashing scopes itself.
        Some(hash("scope", &[s.partition_key().as_bytes()]))
    } else if !object_key.is_empty() {
        Some(hash("object", &[service.as_bytes(), object_key.as_bytes()]))
    } else {
        idempotency_key.map(|idem| hash("idem", &[service.as_bytes(), idem.as_bytes()]))
    }
}

/// Partition key of a **schedule**: `H("schedule", scope, name)` — a new hash kind, with
/// the same `tenant-<kind>` rule for tenants other than `default`. A schedule is not a
/// target: it has its own place, and its unkeyed runs live in the same range (a run's id
/// carries this pk).
pub fn partition_key_for_schedule(
    tenant: &TenantId,
    scope: Option<&Scope>,
    name: &str,
) -> PartitionKey {
    let scope_str = scope.map(|s| s.as_str()).unwrap_or("");
    if tenant.is_default() {
        hash_tuple("schedule", &[scope_str.as_bytes(), name.as_bytes()])
    } else {
        hash_tuple(
            "tenant-schedule",
            &[
                tenant.as_str().as_bytes(),
                scope_str.as_bytes(),
                name.as_bytes(),
            ],
        )
    }
}

/// Partition key of an **unkeyed** callee of `ctx.call`: `H(caller ‖ cid)`.
///
/// Deterministic, not random: the same outbox row processed again (after failover, after a
/// retry) must give the same id, hence the same key. And **independent of N** — unlike
/// `Engine::callee_id` today, which takes the target partition as a parameter.
pub fn partition_key_for_callee(caller: &[u8], completion_id: u32) -> PartitionKey {
    hash_tuple("callee", &[caller, &completion_id.to_be_bytes()])
}

/// The part of the pk that an unkeyed callee **inherits** from its caller under the
/// prefix-grouping rule: the high 32 bits.
///
/// A whole call tree lives inside a window 2³² wide in the key space. A range is 2⁶⁴/R
/// wide, so that window is cut by a range boundary with probability ≈ R/2³²; the remaining
/// low 32 bits are enough for autosplit to choose a cut point **inside** a hot tree.
pub const CALLEE_PREFIX_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// Partition key of an **unkeyed** callee under the prefix-grouping rule (gate
/// `calls/prefix`):
///
/// ```text
/// pk = (pk_caller & HI) | (H("callee/prefix", caller, cid) & !HI)
/// ```
///
/// - **Deterministic**, like [`partition_key_for_callee`]: the same `(caller, cid)` gives
///   the same key. But which rule applies to a call does **not** follow the gate at write
///   or flush time: it belongs to the caller, fixed in entry 0 when the caller was created,
///   and the v6 row carries this key — a call written again or flushed again after the gate
///   is enabled must give **the same** id (IR-9). See
///   `cairn_engine::OutboundCall::callee_pk`.
/// - Its own domain tag (`callee/prefix`): the low 32 bits do not share a byte stream with
///   the old rule, so no call under the two rules accidentally gets the same key.
/// - The root of the tree (an unkeyed HTTP invocation) has a random pk, so each tree has
///   its own random prefix: load across trees still spreads evenly.
pub fn partition_key_for_callee_prefix(
    caller_key: PartitionKey,
    caller: &[u8],
    completion_id: u32,
) -> PartitionKey {
    let low = hash_tuple("callee/prefix", &[caller, &completion_id.to_be_bytes()]).0;
    PartitionKey((caller_key.0 & CALLEE_PREFIX_MASK) | (low & !CALLEE_PREFIX_MASK))
}

// ───────────────────────────────────────────────────────────── table

/// Identity of a range (D22): allocated increasingly from the table's `next_range_id`,
/// **never reused** — a retired range keeps its number forever, so an old id met again in
/// a log or in the bucket is never mistaken for another range.
///
/// One range = one Db = one engine, so the writer/engine side still calls it
/// [`PartitionId`]: it is **the same number**. The old name remains in the engine's code
/// (renaming it there would touch almost every line).
pub type RangeId = PartitionId;

/// Work in progress on a range, written into the table **before** the data step of a
/// split/merge and cleared at the commit step (D24). The table only carries it; its meaning
/// belongs to the split/merge protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeIntent {
    /// Split at `at`: left child `[start, at)`, right child `[at, end]`.
    Splitting {
        at: u64,
        left: RangeId,
        right: RangeId,
    },
    /// Merge with the adjacent range `with` into the new range `into`.
    Merging { with: RangeId, into: RangeId },
}

/// One row of the table: the key span `[start, end]` (both ends included), id, `gen`, and
/// work in progress if any.
///
/// `gen` follows CockroachDB's rule: split ⇒ left child `gen+1`, right child copies;
/// merge ⇒ `max+1`. Used to recognise orphaned child Dbs after a crash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeEntry {
    pub start: u64,
    pub end: u64,
    pub id: RangeId,
    pub gen: u64,
    pub intent: Option<RangeIntent>,
}

impl RangeEntry {
    /// A new range, `gen` 0, no work in progress.
    pub fn new(start: u64, end: u64, id: RangeId) -> Self {
        Self {
            start,
            end,
            id,
            gen: 0,
            intent: None,
        }
    }

    pub fn contains_key(&self, key: PartitionKey) -> bool {
        self.start <= key.0 && key.0 <= self.end
    }
}

/// Key of the table block in `cluster.conf` v3.
const TABLE_VERSION: &str = "table_version";
const NEXT_RANGE_ID: &str = "next_range_id";
const RANGE: &str = "range";

/// The range table: a versioned mapping from partition key spans → ranges (one Db, one
/// engine).
///
/// Invariants, enforced in every constructor (there is no way to build a table that skips
/// them):
///
/// - the spans **cover** `0..=u64::MAX` completely, no gaps, no overlaps;
/// - `ranges` is sorted by `start`, so [`Self::partition_of_key`] is a binary search;
/// - each id has exactly **one** span, every id `< next_range_id` — ids are **not
///   contiguous**: split/merge punch holes in the sequence and nobody fills them (D22);
/// - an id that an `intent` is about to allocate is already below `next_range_id` and is
///   not yet any range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionTable {
    version: u64,
    next_range_id: u32,
    ranges: Arc<[RangeEntry]>,
    /// `k=v` lines whose key this binary does not know — from a newer binary — kept
    /// **verbatim**, in the order met, so [`Self::encode`] writes them back exactly as
    /// before. Without it, an old node finishing a split would silently delete every field a
    /// newer node had added to `cluster.conf`.
    extra: Arc<[String]>,
}

impl PartitionTable {
    /// Divides `u64` evenly into `n` ranges, ids `0..n`, `gen` 0, version 1.
    ///
    /// Range `i` is `[⌊i·2⁶⁴/n⌋, ⌊(i+1)·2⁶⁴/n⌋ − 1]` — computed in `u128` so it does not
    /// overflow, and range lengths differ by at most 1.
    ///
    /// # Panics
    ///
    /// When `n == 0`. `ClusterConfig::open` checks the partition count beforehand.
    pub fn equal(n: u16) -> Self {
        assert!(n > 0, "partition count must be ≥ 1");
        const SPACE: u128 = 1 << 64;
        let ranges: Arc<[_]> = (0..n as u128)
            .map(|i| {
                let start = i * SPACE / n as u128;
                let end = (i + 1) * SPACE / n as u128 - 1;
                RangeEntry::new(start as u64, end as u64, PartitionId(i as u32))
            })
            .collect();
        Self {
            version: 1,
            next_range_id: u32::from(n),
            ranges,
            extra: Arc::new([]),
        }
    }

    /// Shortcut for tests: ranges `(start, end, id)`, `gen` 0, no intent,
    /// `next_range_id` = largest id + 1.
    pub fn from_ranges(version: u64, ranges: Vec<(u64, u64, RangeId)>) -> Result<Self> {
        let next = ranges.iter().map(|r| r.2 .0 + 1).max().unwrap_or(0);
        Self::from_entries(
            version,
            next,
            ranges
                .into_iter()
                .map(|(s, e, id)| RangeEntry::new(s, e, id))
                .collect(),
        )
    }

    /// Builds a table from arbitrary rows, **checking every invariant**. Input order does
    /// not matter; gaps, overlaps, duplicate ids or ids beyond `next_range_id` are all
    /// refused with a reason.
    pub fn from_entries(
        version: u64,
        next_range_id: u32,
        mut ranges: Vec<RangeEntry>,
    ) -> Result<Self> {
        let bad = |reason: String| StoreError::BadClusterConfig { reason };
        if version == 0 {
            return Err(bad("table_version must be ≥ 1".into()));
        }
        if ranges.is_empty() {
            return Err(bad("the range table has no ranges".into()));
        }
        if ranges.len() > u16::MAX as usize {
            return Err(bad(format!(
                "the table has {} ranges, cap {}",
                ranges.len(),
                u16::MAX
            )));
        }
        ranges.sort_by_key(|r| r.start);
        let mut expected_start: Option<u64> = Some(0);
        for r in &ranges {
            let (start, end) = (r.start, r.end);
            if start > end {
                return Err(bad(format!(
                    "span {start:016x}-{end:016x} of range {} has its start after its end",
                    r.id.0
                )));
            }
            match expected_start {
                // The previous span already reached `u64::MAX` and spans remain ⇒ overlap.
                None => {
                    return Err(bad(format!(
                        "span {start:016x}-{end:016x} overlaps the previous span (already covered to the end of u64)"
                    )))
                }
                Some(want) if start < want => {
                    return Err(bad(format!(
                        "span {start:016x}-{end:016x} overlaps the previous span (expected from {want:016x})"
                    )))
                }
                Some(want) if start > want => {
                    return Err(bad(format!(
                        "gap {want:016x}-{:016x}: keys there belong to no range",
                        start - 1
                    )))
                }
                Some(_) => {}
            }
            expected_start = end.checked_add(1);
        }
        if let Some(want) = expected_start {
            return Err(bad(format!(
                "gap {want:016x}-{:016x}: keys there belong to no range",
                u64::MAX
            )));
        }

        let mut seen = std::collections::BTreeSet::new();
        for r in &ranges {
            if !seen.insert(r.id.0) {
                return Err(bad(format!("range {} appears in two spans", r.id.0)));
            }
            if r.id.0 >= next_range_id {
                return Err(bad(format!(
                    "range {} is not below {NEXT_RANGE_ID}={next_range_id}",
                    r.id.0
                )));
            }
        }
        // Ids an intent is about to allocate: already taken off `next_range_id` (allocated in
        // the same CAS that writes the intent), not yet any range.
        let fresh = |id: RangeId, what: &str| {
            if seen.contains(&id.0) || id.0 >= next_range_id {
                Err(bad(format!(
                    "intent {what}: range {} must be an id already allocated but not yet a range",
                    id.0
                )))
            } else {
                Ok(())
            }
        };
        for (i, r) in ranges.iter().enumerate() {
            match r.intent {
                None => {}
                Some(RangeIntent::Splitting { at, left, right }) => {
                    if !(r.start < at && at <= r.end) {
                        return Err(bad(format!(
                            "range {}: split point {at:016x} lies outside ({:016x}, {:016x}]",
                            r.id.0, r.start, r.end
                        )));
                    }
                    if left == right {
                        return Err(bad(format!("range {}: children share one id", r.id.0)));
                    }
                    fresh(left, "splitting")?;
                    fresh(right, "splitting")?;
                }
                Some(RangeIntent::Merging { with, into }) => {
                    let neighbour = |j: Option<usize>| j.and_then(|j| ranges.get(j)).map(|n| n.id);
                    if neighbour(i.checked_sub(1)) != Some(with)
                        && neighbour(i.checked_add(1)) != Some(with)
                    {
                        return Err(bad(format!(
                            "range {}: merging with {} but the two ranges are not adjacent",
                            r.id.0, with.0
                        )));
                    }
                    fresh(into, "merging")?;
                }
            }
        }
        Ok(Self {
            version,
            next_range_id,
            ranges: ranges.into(),
            extra: Arc::new([]),
        })
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    /// The unknown lines kept verbatim — see the `extra` field.
    pub fn extra(&self) -> &[String] {
        &self.extra
    }

    /// This table carrying the unknown lines `extra` in place of its own.
    /// [`crate::ClusterConfig::update`] uses it to copy the current table's unknown lines
    /// into the next table that the caller rebuilt from the ranges.
    pub fn with_extra(mut self, extra: &[String]) -> Self {
        self.extra = extra.into();
        self
    }

    /// The next id to allocate. Never decreases.
    pub fn next_range_id(&self) -> u32 {
        self.next_range_id
    }

    /// Number of ranges — also the number of engines each node runs.
    pub fn partition_count(&self) -> u16 {
        self.ranges.len() as u16
    }

    /// Ids of every range, in key order. **Not contiguous**: split/merge leave holes.
    pub fn partitions(&self) -> impl Iterator<Item = RangeId> + '_ {
        self.ranges.iter().map(|r| r.id)
    }

    pub fn contains(&self, p: RangeId) -> bool {
        self.range(p).is_some()
    }

    /// The row of range `p`, if that range is currently in the table.
    pub fn range(&self, p: RangeId) -> Option<&RangeEntry> {
        self.ranges.iter().find(|r| r.id == p)
    }

    /// The ranges, sorted by `start`.
    pub fn ranges(&self) -> &[RangeEntry] {
        &self.ranges
    }

    /// Ids that this table's intents have allocated but that are not yet ranges: children
    /// of a split/merge in flight. Their Dbs and owner records may already exist before the
    /// table commits. Increasing, no duplicates.
    pub fn promised(&self) -> Vec<RangeId> {
        let mut out: Vec<RangeId> = self
            .ranges
            .iter()
            .filter_map(|r| r.intent)
            .flat_map(|i| match i {
                RangeIntent::Splitting { left, right, .. } => vec![left, right],
                RangeIntent::Merging { into, .. } => vec![into],
            })
            .collect();
        out.sort_by_key(|p| p.0);
        out.dedup();
        out
    }

    /// The range holding `key`. Total — the table covers `u64` completely, so there is
    /// always an answer.
    pub fn range_of_key(&self, key: PartitionKey) -> &RangeEntry {
        // The first range with start > key, minus one. The first range starts at 0 so `i ≥ 1`.
        let i = self.ranges.partition_point(|r| r.start <= key.0);
        &self.ranges[i - 1]
    }

    /// Id of the range holding `key`.
    pub fn partition_of_key(&self, key: PartitionKey) -> RangeId {
        self.range_of_key(key).id
    }

    /// The table block of `cluster.conf` v3 — **readable by eye**, like the lease record:
    ///
    /// ```text
    /// table_version=3
    /// next_range_id=7
    /// range=0000000000000000-7fffffffffffffff:0:0
    /// range=8000000000000000-ffffffffffffffff:4:1:splitting(at=c000000000000000,left=5,right=6)
    /// ```
    ///
    /// Each line is `range=<start>-<end>:<id>:<gen>[:<intent>]`. Fixed 16-digit hex so the
    /// lines align in columns and can be compared by eye; `end` is inclusive so the last
    /// range can be written `ffffffffffffffff` without needing `2⁶⁴`. No header: the header
    /// (and the `format=` marker) belong to `cluster.conf`, and this block sits below them.
    pub fn encode(&self) -> String {
        let mut out = format!(
            "{TABLE_VERSION}={}\n{NEXT_RANGE_ID}={}\n",
            self.version, self.next_range_id
        );
        for r in self.ranges.iter() {
            out.push_str(&format!(
                "{RANGE}={:016x}-{:016x}:{}:{}",
                r.start, r.end, r.id.0, r.gen
            ));
            match r.intent {
                None => {}
                Some(RangeIntent::Splitting { at, left, right }) => out.push_str(&format!(
                    ":splitting(at={at:016x},left={},right={})",
                    left.0, right.0
                )),
                Some(RangeIntent::Merging { with, into }) => {
                    out.push_str(&format!(":merging(with={},into={})", with.0, into.0))
                }
            }
            out.push('\n');
        }
        for line in self.extra.iter() {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    /// The inverse of [`Self::encode`]. Lines without `=` (headers) are skipped so that
    /// `ClusterConfig` can pass the whole file in; `k=v` lines with an unknown key are kept
    /// verbatim in `extra`; but a line **with** a table key that is damaged is an error, no
    /// guessing.
    pub fn decode(text: &str) -> Result<Self> {
        let bad = |reason: String| StoreError::BadClusterConfig { reason };
        let mut version: Option<u64> = None;
        let mut next: Option<u32> = None;
        let mut ranges = Vec::new();
        let mut extra = Vec::new();
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k {
                TABLE_VERSION => {
                    let n = v
                        .parse()
                        .map_err(|_| bad(format!("{TABLE_VERSION} is not a number: {v:?}")))?;
                    if version.replace(n).is_some() {
                        return Err(bad(format!("{TABLE_VERSION} appears twice")));
                    }
                }
                NEXT_RANGE_ID => {
                    let n = v
                        .parse()
                        .map_err(|_| bad(format!("{NEXT_RANGE_ID} is not a number: {v:?}")))?;
                    if next.replace(n).is_some() {
                        return Err(bad(format!("{NEXT_RANGE_ID} appears twice")));
                    }
                }
                RANGE => ranges.push(parse_range(v).ok_or_else(|| {
                    bad(format!(
                        "bad range line: {v:?}, expected <hex16>-<hex16>:<id>:<gen>[:<intent>]"
                    ))
                })?),
                _ => extra.push(line.to_string()),
            }
        }
        let version = version.ok_or_else(|| bad(format!("missing {TABLE_VERSION}")))?;
        let next = next.ok_or_else(|| bad(format!("missing {NEXT_RANGE_ID}")))?;
        Ok(Self::from_entries(version, next, ranges)?.with_extra(&extra))
    }
}

/// Exactly 16 hex digits — no short form, so a hand-edited line missing a digit does not
/// silently become a different range.
fn hex16(s: &str) -> Option<u64> {
    (s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| u64::from_str_radix(s, 16).ok())
        .flatten()
}

/// `<hex16>-<hex16>:<id>:<gen>[:<intent>]`.
fn parse_range(v: &str) -> Option<RangeEntry> {
    let mut parts = v.split(':');
    let (start, end) = parts.next()?.split_once('-')?;
    let id = PartitionId(parts.next()?.parse().ok()?);
    let gen = parts.next()?.parse().ok()?;
    let intent = match parts.next() {
        None => None,
        Some(raw) => Some(parse_intent(raw)?),
    };
    if parts.next().is_some() {
        return None;
    }
    Some(RangeEntry {
        start: hex16(start)?,
        end: hex16(end)?,
        id,
        gen,
        intent,
    })
}

/// `splitting(at=<hex16>,left=<id>,right=<id>)` or `merging(with=<id>,into=<id>)`. Fields
/// must be complete and in order — the only form `encode` produces.
fn parse_intent(raw: &str) -> Option<RangeIntent> {
    let (kind, rest) = raw.split_once('(')?;
    let body = rest.strip_suffix(')')?;
    let fields: Vec<(&str, &str)> = body
        .split(',')
        .map(|f| f.split_once('='))
        .collect::<Option<_>>()?;
    let id = |s: &str| s.parse().ok().map(PartitionId);
    match (kind, fields.as_slice()) {
        ("splitting", [("at", at), ("left", l), ("right", r)]) => Some(RangeIntent::Splitting {
            at: hex16(at)?,
            left: id(l)?,
            right: id(r)?,
        }),
        ("merging", [("with", w), ("into", i)]) => Some(RangeIntent::Merging {
            with: id(w)?,
            into: id(i)?,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **The D-F1 gate.** Same scope ⇒ same partition key, whatever the service / object
    /// key / idempotency key — hence the same partition under *every* table, not just every
    /// N of the even division.
    ///
    /// This is the precondition for a scope's concurrency counter being a `u32` variable in
    /// the RAM of one process. Break it and flow control would need a distributed counter.
    #[test]
    fn same_scope_same_key_for_any_table() {
        let scope = Scope::new("acme").unwrap();
        let want =
            partition_key_for_target(&TenantId::default(), Some(&scope), "cart", "", None).unwrap();
        let targets = [
            ("cart", "", None),
            ("cart", "user-1", None),
            ("billing", "user-2", Some("k")),
            ("other", "", Some("k2")),
        ];
        for (svc, key, idem) in targets {
            assert_eq!(
                partition_key_for_target(&TenantId::default(), Some(&scope), svc, key, idem),
                Some(want),
                "svc={svc} key={key:?} idem={idem:?}"
            );
        }

        let mut tables: Vec<PartitionTable> = [1u16, 2, 3, 8, 64, 999]
            .into_iter()
            .map(PartitionTable::equal)
            .collect();
        // And an uneven table, a different version — "every table", not just even division.
        tables.push(
            PartitionTable::from_ranges(
                9,
                vec![
                    (0, 10, PartitionId(1)),
                    (11, u64::MAX / 3, PartitionId(0)),
                    (u64::MAX / 3 + 1, u64::MAX, PartitionId(2)),
                ],
            )
            .unwrap(),
        );
        for t in &tables {
            let p = t.partition_of_key(want);
            for (svc, key, idem) in targets {
                let k =
                    partition_key_for_target(&TenantId::default(), Some(&scope), svc, key, idem)
                        .unwrap();
                assert_eq!(t.partition_of_key(k), p, "table {t:?}");
            }
        }

        // Not degenerate into a constant: a different scope gives a different key.
        let other = Scope::new("globex").unwrap();
        assert_ne!(
            partition_key_for_target(&TenantId::default(), Some(&other), "cart", "", None),
            Some(want)
        );
    }

    /// The four levels of `partition_key_for_target`, and the domain tags separating them.
    #[test]
    fn target_key_precedence_and_domains() {
        // An object key beats an idempotency key.
        assert_eq!(
            partition_key_for_target(&TenantId::default(), None, "cart", "u-1", Some("k")),
            partition_key_for_target(&TenantId::default(), None, "cart", "u-1", None),
        );
        // No key ⇒ the caller assigns one itself.
        assert_eq!(
            partition_key_for_target(&TenantId::default(), None, "cart", "", None),
            None
        );
        // Same string, different source ⇒ different key.
        let s = Scope::new("x").unwrap();
        let by_scope = partition_key_for_target(&TenantId::default(), Some(&s), "", "", None);
        let by_object = partition_key_for_target(&TenantId::default(), None, "x", "y", None);
        let by_idem = partition_key_for_target(&TenantId::default(), None, "x", "", Some("y"));
        assert_ne!(by_scope, by_object);
        assert_ne!(by_object, by_idem);
        // Length prefix.
        assert_ne!(
            partition_key_for_target(&TenantId::default(), None, "ab", "c", None),
            partition_key_for_target(&TenantId::default(), None, "a", "bc", None),
        );
    }

    /// The key is a permanent contract: ids users have stored carry it. Pin a value so
    /// whoever changes the hash has to change this test — and read this sentence.
    #[test]
    fn partition_key_derivation_is_pinned() {
        let k =
            partition_key_for_target(&TenantId::default(), None, "cart", "user-42", None).unwrap();
        assert_eq!(
            k,
            partition_key_for_target(&TenantId::default(), None, "cart", "user-42", None).unwrap()
        );
        assert_eq!(format!("{:016x}", k.0), PINNED_OBJECT_KEY);
        let c = partition_key_for_callee(&[7u8; 24], 3);
        assert_eq!(format!("{:016x}", c.0), PINNED_CALLEE_KEY);
    }
    const PINNED_OBJECT_KEY: &str = "f5bb8c375fda0bbc";
    const PINNED_CALLEE_KEY: &str = "abe3691a5d624045";

    /// The prefix-grouping rule: the callee keeps **exactly** the caller's high 32 bits,
    /// the low 32 bits come from a hash with its own domain tag — and it is a permanent
    /// contract too since the first v6 outbox row carried it, so pin it.
    #[test]
    fn callee_prefix_keeps_the_callers_high_bits() {
        let caller_key = PartitionKey(0x1234_5678_9abc_def0);
        let caller = [7u8; 24];
        let k = partition_key_for_callee_prefix(caller_key, &caller, 3);
        assert_eq!(k.0 & CALLEE_PREFIX_MASK, 0x1234_5678_0000_0000);
        assert_eq!(k, partition_key_for_callee_prefix(caller_key, &caller, 3));
        assert_ne!(k, partition_key_for_callee_prefix(caller_key, &caller, 4));
        assert_eq!(format!("{:016x}", k.0), PINNED_PREFIX_CALLEE_KEY);
        // The low 32 bits do not match the old rule (different domain tag).
        assert_ne!(
            k.0 & !CALLEE_PREFIX_MASK,
            partition_key_for_callee(&caller, 3).0 & !CALLEE_PREFIX_MASK
        );
    }
    const PINNED_PREFIX_CALLEE_KEY: &str = "1234567892bec480";

    /// A whole call tree (every level inheriting the same prefix) lies inside one range of
    /// the even table when the 2³² window does not touch a range boundary; the low 32 bits
    /// still spread evenly inside the window — where autosplit can cut a hot tree.
    #[test]
    fn callee_prefix_tree_stays_in_one_window_and_spreads_inside_it() {
        let root = make_root_key();
        let mut frontier = vec![(root, [0x11u8; 24])];
        let mut keys = Vec::new();
        let mut serial = 0u32;
        for _ in 0..3 {
            let mut next = Vec::new();
            for (k, caller) in &frontier {
                for cid in 0..10u32 {
                    let child = partition_key_for_callee_prefix(*k, caller, cid);
                    // The child's id: its 8-byte pk + a unique part (here a serial
                    // number), like `make_invocation_id`.
                    serial += 1;
                    let mut id = [0u8; 24];
                    id[..8].copy_from_slice(&child.0.to_be_bytes());
                    id[8..12].copy_from_slice(&serial.to_be_bytes());
                    next.push((child, id));
                    keys.push(child);
                }
            }
            frontier = next;
        }
        assert_eq!(keys.len(), 10 + 100 + 1000);
        for k in &keys {
            assert_eq!(k.0 & CALLEE_PREFIX_MASK, root.0 & CALLEE_PREFIX_MASK);
        }
        // Inside the window: split the low 32 bits into 8 buckets, each within ±25%.
        let mut counts = [0usize; 8];
        for k in &keys {
            counts[((k.0 & !CALLEE_PREFIX_MASK) >> 29) as usize] += 1;
        }
        let even = keys.len() as f64 / 8.0;
        for c in counts {
            assert!((c as f64 - even).abs() / even <= 0.25, "{counts:?}");
        }
    }

    fn make_root_key() -> PartitionKey {
        PartitionKey(0x5a5a_5a5a_0000_0001)
    }

    /// Complete cover, no gaps, no overlaps, ids `0..n`, lengths differ by ≤ 1.
    #[test]
    fn equal_split_covers_u64_without_gaps() {
        for n in [1u16, 2, 3, 7, 1024, 65535] {
            let t = PartitionTable::equal(n);
            assert_eq!(t.partition_count(), n);
            assert_eq!(t.version(), 1);
            assert_eq!(t.next_range_id(), u32::from(n));
            let r = t.ranges();
            assert_eq!(r.len(), n as usize);
            assert_eq!(r[0].start, 0, "n={n}");
            assert_eq!(r[r.len() - 1].end, u64::MAX, "n={n}");
            let (mut lo, mut hi) = (u64::MAX, 0u64);
            for (i, w) in r.iter().enumerate() {
                assert_eq!(w.id, PartitionId(i as u32));
                assert_eq!((w.gen, w.intent), (0, None));
                assert!(w.start <= w.end);
                let len = w.end - w.start;
                lo = lo.min(len);
                hi = hi.max(len);
                if i > 0 {
                    assert_eq!(w.start, r[i - 1].end + 1, "n={n} i={i}");
                }
                // Both ends of each range map to that very range.
                assert_eq!(t.partition_of_key(PartitionKey(w.start)), w.id);
                assert_eq!(t.partition_of_key(PartitionKey(w.end)), w.id);
            }
            assert!(hi - lo <= 1, "n={n}: ranges differ {lo}..{hi}");
            // `from_ranges` agrees with `equal` — the same set of invariants.
            let tuples = r.iter().map(|w| (w.start, w.end, w.id)).collect();
            assert_eq!(PartitionTable::from_ranges(1, tuples).unwrap(), t);
        }
    }

    /// 10k keys over 8 partitions, each partition within ±10% of an even share.
    ///
    /// Both object keys and the callees of **one** caller: the second path is exactly where
    /// FNV-1a once failed (the low bits barely moved across completion ids).
    #[test]
    fn keys_spread_evenly() {
        const KEYS: usize = 10_000;
        const N: u16 = 8;
        let t = PartitionTable::equal(N);
        let even = KEYS as f64 / N as f64;
        let check = |what: &str, keys: &mut dyn Iterator<Item = PartitionKey>| {
            let mut counts = [0usize; N as usize];
            for k in keys {
                counts[t.partition_of_key(k).0 as usize] += 1;
            }
            for (p, &c) in counts.iter().enumerate() {
                let dev = (c as f64 - even).abs() / even;
                assert!(
                    dev <= 0.10,
                    "{what}: partition {p} has {c}, deviation {dev:.3} — {counts:?}"
                );
            }
        };
        check(
            "object key",
            &mut (0..KEYS).map(|i| {
                partition_key_for_target(
                    &TenantId::default(),
                    None,
                    "svc",
                    &format!("key-{i}"),
                    None,
                )
                .unwrap()
            }),
        );
        check(
            "scope",
            &mut (0..KEYS).map(|i| {
                let s = Scope::new(&format!("tenant-{i}")).unwrap();
                partition_key_for_target(&TenantId::default(), Some(&s), "svc", "", None).unwrap()
            }),
        );
        let caller = [0x42u8; 24];
        check(
            "callee",
            &mut (0..KEYS as u32).map(|cid| partition_key_for_callee(&caller, cid)),
        );
    }

    #[test]
    fn table_roundtrip() {
        for n in [1u16, 2, 3, 7, 1024] {
            let t = PartitionTable::equal(n);
            assert_eq!(PartitionTable::decode(&t.encode()).unwrap(), t);
        }
        // Non-contiguous ids, non-zero gen, both kinds of intent.
        let t = PartitionTable::from_entries(
            7,
            12,
            vec![
                RangeEntry {
                    gen: 2,
                    intent: Some(RangeIntent::Merging {
                        with: PartitionId(4),
                        into: PartitionId(11),
                    }),
                    ..RangeEntry::new(0, 0xff, PartitionId(9))
                },
                RangeEntry::new(0x100, 0x7fff_ffff_ffff_ffff, PartitionId(4)),
                RangeEntry {
                    gen: 1,
                    intent: Some(RangeIntent::Splitting {
                        at: 0xc000_0000_0000_0000,
                        left: PartitionId(7),
                        right: PartitionId(8),
                    }),
                    ..RangeEntry::new(0x8000_0000_0000_0000, u64::MAX, PartitionId(2))
                },
            ],
        )
        .unwrap();
        assert_eq!(t.partition_count(), 3);
        assert_eq!(
            t.partitions().collect::<Vec<_>>(),
            [PartitionId(9), PartitionId(4), PartitionId(2)]
        );
        let text = t.encode();
        assert_eq!(
            text,
            "table_version=7\nnext_range_id=12\n\
             range=0000000000000000-00000000000000ff:9:2:merging(with=4,into=11)\n\
             range=0000000000000100-7fffffffffffffff:4:0\n\
             range=8000000000000000-ffffffffffffffff:2:1:splitting(at=c000000000000000,left=7,right=8)\n"
        );
        assert_eq!(PartitionTable::decode(&text).unwrap(), t);

        // Inside a full `cluster.conf`: the header is skipped, unknown `k=v` lines are kept
        // verbatim and written back after the table's lines.
        let conf = format!("cairnflow-cluster/3\nformat=6\n{text}");
        let with_extra = PartitionTable::decode(&conf).unwrap();
        assert_eq!(with_extra.extra(), ["format=6"]);
        assert_eq!(with_extra.clone().with_extra(&[]), t);
        assert_eq!(with_extra.encode(), format!("{text}format=6\n"));
        assert_eq!(
            PartitionTable::decode(&with_extra.encode()).unwrap(),
            with_extra
        );
    }

    #[test]
    fn decode_rejects_gap_and_overlap() {
        let ok = "table_version=1\nnext_range_id=2\n\
                  range=0000000000000000-7fffffffffffffff:0:0\n\
                  range=8000000000000000-ffffffffffffffff:1:0\n";
        PartitionTable::decode(ok).unwrap();

        for (what, raw, needle) in [
            (
                "gap in the middle",
                "table_version=1\nnext_range_id=2\n\
                 range=0000000000000000-7ffffffffffffffe:0:0\n\
                 range=8000000000000000-ffffffffffffffff:1:0\n",
                "gap 7fffffffffffffff",
            ),
            (
                "overlap",
                "table_version=1\nnext_range_id=2\n\
                 range=0000000000000000-8000000000000000:0:0\n\
                 range=8000000000000000-ffffffffffffffff:1:0\n",
                "overlaps",
            ),
            (
                "missing start",
                "table_version=1\nnext_range_id=1\n\
                 range=0000000000000001-ffffffffffffffff:0:0\n",
                "gap 0000000000000000",
            ),
            (
                "missing end",
                "table_version=1\nnext_range_id=1\n\
                 range=0000000000000000-fffffffffffffffe:0:0\n",
                "gap ffffffffffffffff",
            ),
            (
                "overlap after covering to the end",
                "table_version=1\nnext_range_id=2\n\
                 range=0000000000000000-ffffffffffffffff:0:0\n\
                 range=ffffffffffffffff-ffffffffffffffff:1:0\n",
                "overlaps",
            ),
            (
                "start after end",
                "table_version=1\nnext_range_id=1\n\
                 range=0000000000000000-ffffffffffffffff:0:0\n\
                 range=0000000000000002-0000000000000001:0:0\n",
                "start after its end",
            ),
            (
                "one id two spans",
                "table_version=1\nnext_range_id=2\n\
                 range=0000000000000000-7fffffffffffffff:1:0\n\
                 range=8000000000000000-ffffffffffffffff:1:0\n",
                "two spans",
            ),
            (
                "id not below next_range_id",
                "table_version=1\nnext_range_id=2\n\
                 range=0000000000000000-7fffffffffffffff:0:0\n\
                 range=8000000000000000-ffffffffffffffff:2:0\n",
                "next_range_id=2",
            ),
            (
                "split child duplicates an existing range",
                "table_version=1\nnext_range_id=4\n\
                 range=0000000000000000-7fffffffffffffff:0:0:splitting(at=4000000000000000,left=1,right=3)\n\
                 range=8000000000000000-ffffffffffffffff:1:0\n",
                "intent splitting",
            ),
            (
                "split point outside the range",
                "table_version=1\nnext_range_id=4\n\
                 range=0000000000000000-7fffffffffffffff:0:0:splitting(at=0000000000000000,left=2,right=3)\n\
                 range=8000000000000000-ffffffffffffffff:1:0\n",
                "split point",
            ),
            (
                "merge with a non-adjacent range",
                "table_version=1\nnext_range_id=4\n\
                 range=0000000000000000-3fffffffffffffff:0:0:merging(with=2,into=3)\n\
                 range=4000000000000000-7fffffffffffffff:1:0\n\
                 range=8000000000000000-ffffffffffffffff:2:0\n",
                "not adjacent",
            ),
            (
                "unknown intent",
                "table_version=1\nnext_range_id=1\n\
                 range=0000000000000000-ffffffffffffffff:0:0:resharding(x=1)\n",
                "bad range line",
            ),
            (
                "missing gen",
                "table_version=1\nnext_range_id=1\n\
                 range=0000000000000000-ffffffffffffffff:0\n",
                "bad range line",
            ),
            (
                "no ranges",
                "table_version=1\nnext_range_id=1\n",
                "has no ranges",
            ),
            (
                "short hex",
                "table_version=1\nnext_range_id=1\nrange=0-ffffffffffffffff:0:0\n",
                "bad range line",
            ),
            (
                "missing version",
                "next_range_id=1\nrange=0000000000000000-ffffffffffffffff:0:0\n",
                "missing table_version",
            ),
            (
                "missing next_range_id — a v2 table",
                "table_version=1\npartition_count=1\n\
                 range=0000000000000000-ffffffffffffffff:0\n",
                "bad range line",
            ),
            (
                "version 0",
                "table_version=0\nnext_range_id=1\n\
                 range=0000000000000000-ffffffffffffffff:0:0\n",
                "≥ 1",
            ),
        ] {
            let err = PartitionTable::decode(raw).expect_err(what);
            assert!(
                matches!(err, StoreError::BadClusterConfig { .. }),
                "{what}: {err:?}"
            );
            assert!(err.to_string().contains(needle), "{what}: {err}");
        }
    }
}
