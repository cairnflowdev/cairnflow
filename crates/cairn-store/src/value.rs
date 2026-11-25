// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Segmented values: immutable segments on the bucket, the
//! [`crate::keyspace::Table::SegmentedState`] rows and the holder rows
//! [`crate::keyspace::Table::ValueRefs`].
//!
//! ```text
//! <prefix>/values/<tenant>/<pk:016x>/<owner-hex>/<sha256-hex>-<gen:016x>
//! ```
//!
//! - **Owner** ([`Owner`]) = the dedup and GC scope: a keyed/scoped object
//!   (`sha256(DOMAIN ‖ 0x01 ‖ object_rest)`) or an unkeyed invocation
//!   (`sha256(DOMAIN ‖ 0x02 ‖ id)`). Every holder of an owner lives in the same range
//!   (`pk`), so the deletion proof reads one Db.
//! - **`gen`** is issued fresh at every seal (from `entropy`): once `(sha, gen)` has no
//!   holder left it never gets one again — the deletion proof
//!   [`crate::proof::DeleteProof::value_unreferenced`] is permanent.
//! - **Immutable, written once:** PUT `Create` through [`crate::cas::put_resolving`]
//!   (rule 7). The path contains the sha, so an `AlreadyExists` with the same bytes is ours.
//! - **AP-1** (applied to segments): a segment is durable **before** the commit that
//!   carries its holder row.
//! - **Deletion:** held in place ([`crate::trash::Subject::Value`]), then actually deleted
//!   after the retention period, checked once more (the values sweep of cairn-server).
//!
//! A new, leniently read object (absent = none), one `OBJECT_POLICIES` line; no
//! `DATA_FORMAT` bump. Product paths write segments only when the `values/segmented` gate
//! is on.

use {
    crate::{error::StoreError, partition_table::PartitionKey, InvocationId, Result},
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    },
    std::sync::Arc,
};

/// Root directory on the bucket.
pub const DIR: &str = "values";

/// Hash domain of the owner — changing it changes every path.
const OWNER_DOMAIN: &[u8] = b"cairnflow/value-owner/1";

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// The dedup and GC scope of a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Owner(pub [u8; 32]);

impl Owner {
    /// Owner of a keyed or scoped object — `object_rest` is the key body shared by
    /// State/Promises (`keyspace::object_rest`).
    pub fn of_object_rest(object_rest: &[u8]) -> Owner {
        Owner(hash(0x01, object_rest))
    }

    /// Owner of an unkeyed invocation.
    pub fn of_invocation(id: &InvocationId) -> Owner {
        Owner(hash(0x02, id))
    }

    pub fn hex(&self) -> String {
        crate::artifact::sha_hex(&self.0)
    }
}

/// `(pk, owner)` of a keyed or scoped object — the same pk as every State row and every
/// invocation of it. `None` = an unkeyed, unscoped service (D-a): its values belong to
/// [`invocation_owner`].
pub fn object_owner(
    tenant: &crate::scope::TenantId,
    scope: Option<&crate::scope::Scope>,
    service: &str,
    object_key: &str,
) -> Option<(PartitionKey, Owner)> {
    let pk = crate::keyspace::object_owner(tenant, scope, service, object_key)?;
    let rest = crate::keyspace::object_rest_bytes(tenant, scope, service, object_key);
    Some((pk, Owner::of_object_rest(&rest)))
}

/// `(pk, owner)` of an unkeyed invocation: the pk is in the id.
pub fn invocation_owner(id: &InvocationId) -> (PartitionKey, Owner) {
    (
        crate::cluster::partition_key_of(id),
        Owner::of_invocation(id),
    )
}

fn hash(kind: u8, body: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(OWNER_DOMAIN);
    h.update([kind]);
    h.update(body);
    h.finalize().into()
}

/// Reference to a segment (same meaning as `cairn_protocol::value::SegmentRef`; this
/// crate does not depend on the protocol).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegRef {
    pub sha: [u8; 32],
    pub gen: u64,
    pub len: u64,
}

/// One segment object: the unit of hold-in-place and deletion.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Segment {
    pub tenant: String,
    pub pk: PartitionKey,
    pub owner: Owner,
    pub sha: [u8; 32],
    pub gen: u64,
}

/// A holder of a segment — the `holder` part of the `ValueRefs` key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Holder {
    /// The current value of one of the owner's state keys.
    State(Bytes),
    /// The journal of an invocation (removed in `Engine::purge`).
    Invocation(InvocationId),
    /// A promise's value — reserved, no path produces it yet.
    Promise(String),
}

impl Holder {
    pub(crate) const STATE: u8 = 0x01;
    pub(crate) const INVOCATION: u8 = 0x02;
    pub(crate) const PROMISE: u8 = 0x03;
}

/// Value of a `ValueRefs` row: `v1 ‖ varint seg_len`.
pub fn encode_ref_row(seg_len: u64) -> Vec<u8> {
    let mut out = vec![1u8];
    put_varint(&mut out, seg_len);
    out
}

/// Inverse of [`encode_ref_row`]; `None` = corrupt (strict read: the caller keeps).
pub fn decode_ref_row(raw: &[u8]) -> Option<u64> {
    let (&v, rest) = raw.split_first()?;
    if v != 1 {
        return None;
    }
    let mut r = Varints(rest);
    let len = r.varint()?;
    r.0.is_empty().then_some(len)
}

/// A `SegmentedState` row: the logical value = the segments in order, then the tail.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentedRow {
    pub len: u64,
    pub refs: Vec<SegRef>,
    pub tail: Bytes,
}

impl SegmentedRow {
    /// `v1 ‖ varint len ‖ varint n ‖ n × (sha[32] ‖ gen u64 BE ‖ varint
    /// seg_len) ‖ tail`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.refs.len() * 48 + self.tail.len());
        out.push(1);
        put_varint(&mut out, self.len);
        put_varint(&mut out, self.refs.len() as u64);
        for r in &self.refs {
            out.extend_from_slice(&r.sha);
            out.extend_from_slice(&r.gen.to_be_bytes());
            put_varint(&mut out, r.len);
        }
        out.extend_from_slice(&self.tail);
        out
    }

    /// **Strict** read: unknown version, missing bytes, `len` not matching the sum, no
    /// segment at all ⇒ `None` — the caller treats it as an error (misreading state is
    /// worse than failing loudly).
    pub fn decode(raw: &Bytes) -> Option<SegmentedRow> {
        let (&v, rest) = raw.split_first()?;
        if v != 1 {
            return None;
        }
        let mut r = Varints(rest);
        let len = r.varint()?;
        let n = r.varint()?;
        if n == 0 || n > (r.0.len() / 41) as u64 {
            return None;
        }
        let mut refs = Vec::with_capacity(n as usize);
        let mut sum = 0u64;
        for _ in 0..n {
            let sha: [u8; 32] = r.take(32)?.try_into().ok()?;
            let gen = u64::from_be_bytes(r.take(8)?.try_into().ok()?);
            let seg_len = r.varint()?;
            sum = sum.checked_add(seg_len)?;
            refs.push(SegRef {
                sha,
                gen,
                len: seg_len,
            });
        }
        let tail = raw.slice_ref(r.0);
        sum = sum.checked_add(tail.len() as u64)?;
        (sum == len).then_some(SegmentedRow { len, refs, tail })
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

struct Varints<'a>(&'a [u8]);

impl<'a> Varints<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(n)?;
        self.0 = tail;
        Some(head)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let (&b, rest) = self.0.split_first()?;
            self.0 = rest;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }
}

/// One `(tenant, pk, owner)` directory as the sweep sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub tenant: String,
    pub pk: PartitionKey,
    pub owner: Owner,
    /// `(sha, gen, last_modified ms)` of every object in the directory (within the page).
    pub segments: Vec<([u8; 32], u64, u64)>,
}

/// The node's shared RAM cap for the segment cache, separate from the artifact cache.
pub const CACHE_NODE_BYTES: u64 = 128 << 20;

/// Process counters (metric `cairnflow_value_*`) — atomic, no shared lock.
pub mod stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Segment GETs that hit the range's cache.
    pub static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    /// Segment GETs that missed the cache (went to the store), including stores without a cache.
    pub static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
    /// Segment cache bytes of every range on the node — the figure from the latest insert.
    pub static CACHE_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Bytes read from the store to assemble, copy or merge.
    pub static FETCHED_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Segment hashes done by [`super::ValueStore::get_verified`] (only on a cache miss).
    pub static VERIFY_HASHES: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    pub fn load(c: &AtomicU64) -> u64 {
        c.load(Ordering::Relaxed)
    }
}

/// Result of [`ValueStore::get_verified`].
#[derive(Debug)]
pub enum Verified {
    /// Object absent.
    Missing,
    /// The bytes read do not match the ref's sha — corrupt bytes are never sent.
    Mismatch,
    /// Bytes that matched the sha.
    Bytes(Bytes),
}

/// The segment store on the bucket. With [`Self::with_cache`], GETs go through the range's
/// cache (which only holds bytes that matched their sha) and freshly PUT bytes go straight
/// into the cache — segments are immutable, so a cache hit is always the right bytes.
///
/// The cache is keyed by the **full object** (path: tenant, pk, owner, sha, gen), not by
/// sha: one range holds segments of many tenants and owners, and keying by sha would let
/// a GET under owner A hit owner B's bytes with the same content — a cross read, or a
/// "is this content in the cache" oracle (found in a security review).
#[derive(Clone)]
pub struct ValueStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    cache: Option<Arc<crate::artifact::ArtifactCache>>,
    budget: Option<crate::artifact::CacheBudget>,
}

impl std::fmt::Debug for ValueStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueStore")
            .field("prefix", &self.prefix.as_ref())
            .finish()
    }
}

impl ValueStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        ValueStore {
            store,
            prefix: prefix.clone(),
            cache: None,
            budget: None,
        }
    }

    /// Segment cache of **one range**, sharing the node's common cap `budget`. Lost on a
    /// leader change, like the artifact cache.
    pub fn with_cache(mut self, budget: crate::artifact::CacheBudget) -> Self {
        self.cache = Some(Arc::new(crate::artifact::ArtifactCache::new(
            budget.clone(),
        )));
        self.budget = Some(budget);
        self
    }

    /// Cache key of `s`: sha256 of the object path — distinct per tenant, pk, owner, sha
    /// and gen.
    fn cache_key(&self, s: &Segment) -> [u8; 32] {
        crate::artifact::sha256(self.path(s).as_ref().as_bytes())
    }

    fn remember(&self, s: &Segment, bytes: Bytes) {
        if let (Some(c), Some(b)) = (&self.cache, &self.budget) {
            c.insert(self.cache_key(s), bytes);
            stats::CACHE_BYTES.store(b.used(), std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn under(&self, rel: &str) -> Path {
        if self.prefix.as_ref().is_empty() {
            Path::from(rel)
        } else {
            Path::from(format!("{}/{rel}", self.prefix.as_ref()))
        }
    }

    /// Directory of one owner.
    pub fn dir(&self, tenant: &str, pk: PartitionKey, owner: &Owner) -> Path {
        self.under(&format!("{DIR}/{tenant}/{:016x}/{}", pk.0, owner.hex()))
    }

    pub fn path(&self, s: &Segment) -> Path {
        Path::from(format!(
            "{}/{}-{:016x}",
            self.dir(&s.tenant, s.pk, &s.owner).as_ref(),
            crate::artifact::sha_hex(&s.sha),
            s.gen
        ))
    }

    /// PUT segment `s` (bytes `bytes`; the sha must match — the caller checks).
    /// Idempotent: the object already exists with the same bytes ⇒ done; different bytes
    /// ⇒ error, no overwrite.
    pub async fn put(&self, s: &Segment, bytes: Bytes) -> Result<()> {
        let path = self.path(s);
        match crate::cas::put_resolving(&self.store, &path, bytes.clone(), PutMode::Create).await {
            Ok(crate::cas::Cas::Written(_)) => {
                // The caller has checked the sha of `bytes` (sealing hashes them).
                self.remember(s, bytes);
                Ok(())
            }
            Ok(crate::cas::Cas::Lost) => Err(StoreError::Io(format!(
                "{path}: object already exists with content of another sha — not overwriting"
            ))),
            Err(e) => Err(io(e)),
        }
    }

    /// The segment's bytes; `None` = absent. With a cache: a hit (by path) does not go to
    /// the store; bytes read go into the cache only when they match the sha (the caller
    /// still checks length and sha as before).
    pub async fn get(&self, s: &Segment) -> Result<Option<Bytes>> {
        if let Some(b) = self.cache.as_ref().and_then(|c| c.get(&self.cache_key(s))) {
            stats::add(&stats::CACHE_HITS, 1);
            return Ok(Some(b));
        }
        stats::add(&stats::CACHE_MISSES, 1);
        match self.store.get(&self.path(s)).await {
            Ok(g) => {
                let b = g.bytes().await.map_err(io)?;
                stats::add(&stats::FETCHED_BYTES, b.len() as u64);
                if self.cache.is_some() && crate::artifact::sha256(&b) == s.sha {
                    self.remember(s, b.clone());
                }
                Ok(Some(b))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// The segment's bytes **already matched against the sha** — for the unlocked read
    /// path (`GET /values/v1/{token}`). A cache hit ⇒ no rehash: the cache only takes bytes
    /// that matched the sha (on read) or bytes whose PUT caller checked them. A miss ⇒ GET,
    /// then hash **off** the runtime's worker threads (`spawn_blocking`): up to 16 MiB
    /// per request is a few dozen ms of CPU.
    pub async fn get_verified(&self, s: &Segment) -> Result<Verified> {
        if let Some(b) = self.cache.as_ref().and_then(|c| c.get(&self.cache_key(s))) {
            stats::add(&stats::CACHE_HITS, 1);
            return Ok(Verified::Bytes(b));
        }
        stats::add(&stats::CACHE_MISSES, 1);
        let b = match self.store.get(&self.path(s)).await {
            Ok(g) => g.bytes().await.map_err(io)?,
            Err(OsError::NotFound { .. }) => return Ok(Verified::Missing),
            Err(e) => return Err(io(e)),
        };
        stats::add(&stats::FETCHED_BYTES, b.len() as u64);
        let (b, sha) = tokio::task::spawn_blocking(move || {
            let sha = crate::artifact::sha256(&b);
            (b, sha)
        })
        .await
        .map_err(|e| StoreError::Io(format!("hashing a segment failed: {e}")))?;
        stats::add(&stats::VERIFY_HASHES, 1);
        if sha != s.sha {
            return Ok(Verified::Mismatch);
        }
        if self.cache.is_some() {
            self.remember(s, b.clone());
        }
        Ok(Verified::Bytes(b))
    }

    /// Meta of the segment; `None` = absent.
    pub async fn head(&self, s: &Segment) -> Result<Option<ObjectMeta>> {
        match self.store.head(&self.path(s)).await {
            Ok(m) => Ok(Some(m)),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// Every object in one owner's directory.
    pub async fn objects(
        &self,
        tenant: &str,
        pk: PartitionKey,
        owner: &Owner,
    ) -> Result<Vec<ObjectMeta>> {
        let dir = self.dir(tenant, pk, owner);
        self.store.list(Some(&dir)).try_collect().await.map_err(io)
    }

    /// One page: at most `max_objects` objects after `after` (`None` = from the start),
    /// grouped by `(tenant, pk, owner)`. A directory cut off at the end of the page is
    /// returned on the next page. Unknown objects under `values/` are skipped (no guessing).
    pub async fn list_page(
        &self,
        after: Option<&Path>,
        max_objects: usize,
    ) -> Result<(Vec<Listed>, Option<Path>)> {
        let root = self.under(DIR);
        let mut s = match after {
            Some(a) => self.store.list_with_offset(Some(&root), a),
            None => self.store.list(Some(&root)),
        };
        let mut out: Vec<Listed> = Vec::new();
        let mut last: Option<Path> = None;
        let mut n = 0usize;
        while let Some(meta) = s.try_next().await.map_err(io)? {
            n += 1;
            let Some(seg) = self.segment_of(&meta.location) else {
                last = Some(meta.location);
                continue;
            };
            let ms = meta.last_modified.timestamp_millis().max(0) as u64;
            match out.last_mut() {
                Some(l) if l.tenant == seg.tenant && l.pk == seg.pk && l.owner == seg.owner => {
                    l.segments.push((seg.sha, seg.gen, ms));
                }
                _ => {
                    if n > max_objects {
                        return Ok((out, last));
                    }
                    out.push(Listed {
                        tenant: seg.tenant,
                        pk: seg.pk,
                        owner: seg.owner,
                        segments: vec![(seg.sha, seg.gen, ms)],
                    })
                }
            }
            last = Some(meta.location);
        }
        Ok((out, None))
    }

    /// Segment from an object path; `None` = not a segment object.
    pub fn segment_of(&self, p: &Path) -> Option<Segment> {
        let rel = if self.prefix.as_ref().is_empty() {
            p.as_ref()
        } else {
            p.as_ref()
                .strip_prefix(self.prefix.as_ref())?
                .strip_prefix('/')?
        };
        let mut parts = rel.split('/');
        if parts.next()? != DIR {
            return None;
        }
        let tenant = parts.next()?.to_string();
        let pk = parts.next()?;
        if pk.len() != 16 {
            return None;
        }
        let pk = PartitionKey(u64::from_str_radix(pk, 16).ok()?);
        let owner = Owner(crate::artifact::sha_from_hex(parts.next()?)?);
        let (sha, gen) = parts.next()?.split_once('-')?;
        let sha = crate::artifact::sha_from_hex(sha)?;
        if gen.len() != 16 {
            return None;
        }
        let gen = u64::from_str_radix(gen, 16).ok()?;
        parts.next().is_none().then_some(Segment {
            tenant,
            pk,
            owner,
            sha,
            gen,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> SegmentedRow {
        SegmentedRow {
            len: 3 + 10 + 20,
            refs: vec![
                SegRef {
                    sha: [1; 32],
                    gen: 7,
                    len: 10,
                },
                SegRef {
                    sha: [2; 32],
                    gen: u64::MAX,
                    len: 20,
                },
            ],
            tail: Bytes::from_static(b"abc"),
        }
    }

    /// **`get_verified`: a cache miss checks the sha; a cache hit does not go to the
    /// store and does not rehash.** Must see: absent ⇒ `Missing`; bytes with the wrong
    /// sha ⇒ `Mismatch` (not cached); right bytes ⇒ `Bytes`; an object overwritten
    /// afterwards still returns the checked bytes from the cache.
    #[tokio::test]
    async fn get_verified_checks_on_miss_and_trusts_the_cache() {
        use slatedb::object_store::{memory::InMemory, ObjectStoreExt};
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let values = ValueStore::new(store.clone(), &Path::from(""))
            .with_cache(crate::artifact::CacheBudget::new(1 << 20));
        let good = Bytes::from_static(b"segment-good-sha");
        let seg = Segment {
            tenant: "acme".into(),
            pk: PartitionKey(7),
            owner: Owner([3; 32]),
            sha: crate::artifact::sha256(&good),
            gen: 1,
        };
        assert!(matches!(
            values.get_verified(&seg).await.unwrap(),
            Verified::Missing
        ));
        store
            .put(&values.path(&seg), Bytes::from_static(b"bad").into())
            .await
            .unwrap();
        assert!(matches!(
            values.get_verified(&seg).await.unwrap(),
            Verified::Mismatch
        ));
        store
            .put(&values.path(&seg), good.clone().into())
            .await
            .unwrap();
        let hashes = stats::load(&stats::VERIFY_HASHES);
        match values.get_verified(&seg).await.unwrap() {
            Verified::Bytes(b) => assert_eq!(b, good),
            other => panic!("{other:?}"),
        }
        assert!(
            stats::load(&stats::VERIFY_HASHES) > hashes,
            "a cache miss must hash"
        );
        store
            .put(&values.path(&seg), Bytes::from_static(b"overwrite").into())
            .await
            .unwrap();
        match values.get_verified(&seg).await.unwrap() {
            Verified::Bytes(b) => assert_eq!(b, good, "a cache hit must return checked bytes"),
            other => panic!("cache hit but the store was read again: {other:?}"),
        }
    }

    #[test]
    fn segmented_row_round_trips() {
        let r = row();
        let raw = Bytes::from(r.encode());
        assert_eq!(SegmentedRow::decode(&raw), Some(r));
    }

    #[test]
    fn segmented_row_is_read_strictly() {
        let raw = row().encode();
        // Unknown version.
        let mut v2 = raw.clone();
        v2[0] = 2;
        assert_eq!(SegmentedRow::decode(&Bytes::from(v2)), None);
        // `len` not matching the sum (one extra tail byte).
        let mut long = raw.clone();
        long.push(b'x');
        assert_eq!(SegmentedRow::decode(&Bytes::from(long)), None);
        // Bytes missing in the middle of a ref.
        assert_eq!(
            SegmentedRow::decode(&Bytes::copy_from_slice(&raw[..20])),
            None
        );
        // No segment: not a segmented row.
        let empty = SegmentedRow {
            len: 1,
            refs: vec![],
            tail: Bytes::from_static(b"a"),
        };
        assert_eq!(SegmentedRow::decode(&Bytes::from(empty.encode())), None);
        assert_eq!(SegmentedRow::decode(&Bytes::new()), None);
    }

    #[test]
    fn ref_row_round_trips_and_is_strict() {
        assert_eq!(decode_ref_row(&encode_ref_row(300)), Some(300));
        assert_eq!(decode_ref_row(&[2, 1]), None);
        assert_eq!(decode_ref_row(&[1]), None);
        assert_eq!(decode_ref_row(&[1, 1, 0]), None);
    }

    #[test]
    fn owners_of_object_and_invocation_never_collide() {
        let id = [9u8; 24];
        assert_ne!(Owner::of_invocation(&id), Owner::of_object_rest(&id));
    }

    #[test]
    fn segment_path_round_trips() {
        let vs = ValueStore::new(
            Arc::new(slatedb::object_store::memory::InMemory::new()),
            &Path::from("p"),
        );
        let s = Segment {
            tenant: "acme".into(),
            pk: PartitionKey(0xdead_beef),
            owner: Owner([3; 32]),
            sha: [4; 32],
            gen: 0x0102_0304_0506_0708,
        };
        let p = vs.path(&s);
        assert!(p.as_ref().starts_with("p/values/acme/00000000deadbeef/"));
        assert!(p.as_ref().ends_with("-0102030405060708"));
        assert_eq!(vs.segment_of(&p), Some(s));
        assert_eq!(vs.segment_of(&Path::from("p/values/acme/x")), None);
        assert_eq!(vs.segment_of(&Path::from("p/artifacts/acme")), None);
    }

    /// The cache is keyed by the full object, not by sha (found in a security review).
    /// Segment X of owner B is in the cache after its PUT; a GET of the same sha under
    /// owner A, under another tenant, or under another `gen` must go to the store
    /// (absent ⇒ `None`), never return B's bytes.
    #[tokio::test]
    async fn cache_never_serves_another_objects_bytes() {
        let vs = ValueStore::new(
            Arc::new(slatedb::object_store::memory::InMemory::new()),
            &Path::from(""),
        )
        .with_cache(crate::artifact::CacheBudget::new(1 << 20));
        let bytes = Bytes::from_static(b"segment of B");
        let b = Segment {
            tenant: "other".into(),
            pk: PartitionKey(1),
            owner: Owner([2; 32]),
            sha: crate::artifact::sha256(&bytes),
            gen: 5,
        };
        vs.put(&b, bytes.clone()).await.unwrap();
        assert_eq!(vs.get(&b).await.unwrap(), Some(bytes));
        for s in [
            Segment {
                owner: Owner([1; 32]),
                ..b.clone()
            },
            Segment {
                tenant: "acme".into(),
                ..b.clone()
            },
            Segment {
                gen: 6,
                ..b.clone()
            },
        ] {
            assert_eq!(vs.get(&s).await.unwrap(), None, "{s:?}");
        }
    }

    /// Rule 7: a segment's PUT `Create` may have landed while the reply reports an error
    /// or `AlreadyExists` (object_store retries on its own). Must see: `put` reports done
    /// and the bytes read back are right; a repeat PUT of the same bytes is still done
    /// (idempotent); the same path with different bytes is an error, no overwrite.
    #[tokio::test]
    async fn ambiguous_put_counts_as_written() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let vs = ValueStore::new(faulty.clone(), &Path::from("p"));
            let s = Segment {
                tenant: "acme".into(),
                pk: PartitionKey(7),
                owner: Owner([3; 32]),
                sha: crate::artifact::sha256(b"segment"),
                gen: 42,
            };
            faulty.add_rule(Rule::ambiguous_put("p/values/", reply));
            vs.put(&s, Bytes::from_static(b"segment"))
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: PUT landed yet errored: {e}"));
            assert_eq!(
                vs.get(&s).await.unwrap().as_deref(),
                Some(&b"segment"[..]),
                "{reply:?}"
            );
            vs.put(&s, Bytes::from_static(b"segment"))
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: repeat PUT of the same bytes: {e}"));
            assert!(
                vs.put(&s, Bytes::from_static(b"other")).await.is_err(),
                "{reply:?}: different bytes must fail"
            );
            assert_eq!(
                vs.get(&s).await.unwrap().as_deref(),
                Some(&b"segment"[..]),
                "{reply:?}: no overwrite"
            );
        }
    }
}
