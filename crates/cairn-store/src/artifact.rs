// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Invocation artifacts: large bytes that a handler moves out of the journal with
//! `ctx.artifact.put`, so the journal carries only a ref (sha256).
//!
//! ```text
//! <prefix>/artifacts/<tenant>/<id-hex>/<epoch:016x>/<sha256-hex>
//! ```
//!
//! - **Immutable, written once:** a `Create` PUT through [`crate::cas::put_resolving`]. The
//!   path contains the content's sha, so an `AlreadyExists` that reads back the same bytes is
//!   ours; different bytes means corruption — an error, never an overwrite.
//! - **AP-1:** the object is durable **before** the commit that carries the
//!   [`crate::keys::ArtifactRow`] row and before any journal entry pointing at it — the turn
//!   layer finishes the PUT before it handles the next frame.
//! - **Epoch (AP-8):** a deterministic id is recreated after a purge; the epoch (64 bits from
//!   `entropy`, allocated lazily at the first artifact, recorded in the row) separates the
//!   directories of the two lives. The sweep only touches an `(id, epoch)` directory that the
//!   leader has proven no longer has any matching row
//!   ([`crate::proof::DeleteProof::purged_invocation`]).
//! - **Deletion:** held in place ([`crate::trash::Trash::hold_in_place`]) and only really
//!   deleted after the hold period, checking again.
//!
//! A new object, no `DATA_FORMAT` bump (D27): an old binary does not read `artifacts/`.

use {
    crate::{error::StoreError, ids::id_hex, InvocationId, Result},
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    },
    std::sync::Arc,
};

/// Deadline from PUT to committing the row (AP-9): past it, PUT again before committing. The
/// sweep only touches directories older than `min_age` ≥ 2 × `T_PUT`.
pub const T_PUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Maximum size of one artifact: 16 MiB minus 1 KiB — the `ArtifactPut` frame also carries the
/// sha, the length and the content type, and frames are capped at 16 MiB
/// (`cairn_protocol::frame::DEFAULT_MAX_FRAME_LEN`). With the cap equal to the frame cap, an
/// artifact exactly at the cap could never reach this point.
pub const MAX_BYTES: usize = (16 << 20) - 1024;

const DIR: &str = "artifacts";

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Lowercase hex of a sha256.
pub fn sha_hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

/// Inverse of [`sha_hex`].
pub fn sha_from_hex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// sha256 of `bytes`.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).into()
}

/// One `(tenant, id, epoch)` directory — the unit of holding in place and of deletion.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Owner {
    pub tenant: String,
    pub id: InvocationId,
    pub epoch: u64,
}

/// An artifact directory as the sweep sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub owner: Owner,
    pub objects: usize,
    /// The newest `last_modified` in the directory (ms).
    pub newest_ms: u64,
}

#[derive(Clone)]
pub struct ArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl std::fmt::Debug for ArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactStore")
            .field("prefix", &self.prefix.as_ref())
            .finish()
    }
}

impl ArtifactStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        ArtifactStore {
            store,
            prefix: prefix.clone(),
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

    /// The directory of one life.
    pub fn dir(&self, o: &Owner) -> Path {
        self.under(&format!(
            "{DIR}/{}/{}/{:016x}",
            o.tenant,
            id_hex(&o.id),
            o.epoch
        ))
    }

    pub fn path(&self, o: &Owner, sha: &[u8; 32]) -> Path {
        Path::from(format!("{}/{}", self.dir(o).as_ref(), sha_hex(sha)))
    }

    /// PUTs `bytes` (sha `sha`) into directory `o`. Idempotent: the object already exists with the
    /// same bytes ⇒ done.
    pub async fn put(&self, o: &Owner, sha: &[u8; 32], bytes: Bytes) -> Result<()> {
        let path = self.path(o, sha);
        match crate::cas::put_resolving(&self.store, &path, bytes, PutMode::Create).await {
            Ok(crate::cas::Cas::Written(_)) => Ok(()),
            Ok(crate::cas::Cas::Lost) => Err(StoreError::Io(format!(
                "{path}: object already exists with content of a different sha — not overwriting"
            ))),
            Err(e) => Err(io(e)),
        }
    }

    /// PUTs again (overwriting) to refresh the object's age (AP-9): `put` is a `Create`, which does
    /// not write when the object already exists, so it refreshes nothing. Same sha is the same
    /// bytes, so overwriting does not change the content.
    pub async fn refresh(&self, o: &Owner, sha: &[u8; 32], bytes: Bytes) -> Result<()> {
        self.store
            .put_opts(&self.path(o, sha), bytes.into(), PutMode::Overwrite.into())
            .await
            .map(|_| ())
            .map_err(io)
    }

    /// Copies artifact `sha` from directory `from` to `to` with a server-side copy
    /// (restart-as-new, AP-10). Overwriting is fine: same sha is the same bytes.
    pub async fn copy(&self, from: &Owner, to: &Owner, sha: &[u8; 32]) -> Result<()> {
        self.store
            .copy(&self.path(from, sha), &self.path(to, sha))
            .await
            .map_err(io)
    }

    /// The artifact's bytes; `None` = absent.
    pub async fn get(&self, o: &Owner, sha: &[u8; 32]) -> Result<Option<Bytes>> {
        match self.store.get(&self.path(o, sha)).await {
            Ok(g) => Ok(Some(g.bytes().await.map_err(io)?)),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// Every object in directory `o`.
    pub async fn objects(&self, o: &Owner) -> Result<Vec<ObjectMeta>> {
        let dir = self.dir(o);
        self.store.list(Some(&dir)).try_collect().await.map_err(io)
    }

    /// One page of artifact directories: lists at most `max_objects` objects after path `after`
    /// (`None` = from the start), grouped by `(tenant, id, epoch)`. Returns the directories
    /// **wholly inside the page** and the path to continue from (`None` = done). A directory cut
    /// off at the end of the page is returned on the next page.
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
            let Some(owner) = self.owner_of(&meta.location) else {
                last = Some(meta.location);
                continue;
            };
            let ms = meta.last_modified.timestamp_millis().max(0) as u64;
            match out.last_mut() {
                Some(l) if l.owner == owner => {
                    l.objects += 1;
                    l.newest_ms = l.newest_ms.max(ms);
                }
                _ => {
                    if n > max_objects {
                        // Page full: the new directory starts on the next page.
                        return Ok((out, last));
                    }
                    out.push(Listed {
                        owner,
                        objects: 1,
                        newest_ms: ms,
                    })
                }
            }
            last = Some(meta.location);
        }
        Ok((out, None))
    }

    /// `(tenant, id, epoch)` from the path of an artifact object.
    pub fn owner_of(&self, p: &Path) -> Option<Owner> {
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
        let id = crate::ids::id_from_hex(parts.next()?)?;
        let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
        sha_from_hex(parts.next()?)?;
        parts
            .next()
            .is_none()
            .then_some(Owner { tenant, id, epoch })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, slatedb::object_store::memory::InMemory};

    fn owner(epoch: u64) -> Owner {
        Owner {
            tenant: "t".into(),
            id: [3; 24],
            epoch,
        }
    }

    #[tokio::test]
    async fn put_is_idempotent_and_pages_group_by_owner() {
        let a = ArtifactStore::new(Arc::new(InMemory::new()), &Path::from("p"));
        let body = Bytes::from_static(b"hello");
        let sha = sha256(&body);
        a.put(&owner(1), &sha, body.clone()).await.unwrap();
        a.put(&owner(1), &sha, body.clone()).await.unwrap();
        assert_eq!(a.get(&owner(1), &sha).await.unwrap(), Some(body.clone()));
        assert_eq!(a.get(&owner(2), &sha).await.unwrap(), None);
        a.put(&owner(2), &sha256(b"b"), Bytes::from_static(b"b"))
            .await
            .unwrap();
        a.put(&owner(2), &sha256(b"c"), Bytes::from_static(b"c"))
            .await
            .unwrap();
        let p = a.path(&owner(1), &sha);
        assert_eq!(a.owner_of(&p), Some(owner(1)));
        let (all, next) = a.list_page(None, 100).await.unwrap();
        assert_eq!(next, None);
        assert_eq!(
            all.iter()
                .map(|l| (l.owner.epoch, l.objects))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2)]
        );
        // A 1-object page: the first directory is whole, the second moves to the next page.
        let (first, next) = a.list_page(None, 1).await.unwrap();
        assert_eq!(first.len(), 1);
        let (rest, end) = a.list_page(next.as_ref(), 10).await.unwrap();
        assert_eq!((rest.len(), rest[0].objects, end), (1, 2, None));
        assert_eq!(sha_from_hex(&sha_hex(&sha)), Some(sha));
    }
}

/// The node-wide RAM cap for the artifact cache.
pub const CACHE_NODE_BYTES: u64 = 256 << 20;

/// The node-wide cap, shared by every range's cache through one atomic counter — no shared
/// lock.
#[derive(Clone, Debug)]
pub struct CacheBudget {
    used: Arc<std::sync::atomic::AtomicU64>,
    cap: u64,
}

impl CacheBudget {
    pub fn new(cap: u64) -> Self {
        Self {
            used: Arc::default(),
            cap,
        }
    }

    /// Bytes held, summed over every range.
    pub fn used(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::Acquire)
    }

    fn reserve(&self, n: u64) -> bool {
        use std::sync::atomic::Ordering;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                (u + n <= self.cap).then_some(u + n)
            })
            .is_ok()
    }

    fn release(&self, n: u64) {
        self.used.fetch_sub(n, std::sync::atomic::Ordering::AcqRel);
    }
}

/// The artifact cache of **one range**, keyed by sha — same sha is the same bytes, whatever the
/// invocation. It lives with the range's engine, so it is lost on a leader change. When full,
/// it evicts the range's own oldest entry; when there is not enough room, it does not keep the
/// entry. Never the source of truth: the route still checks the `Artifacts` row first.
#[derive(Debug)]
pub struct ArtifactCache {
    budget: CacheBudget,
    inner: std::sync::Mutex<CacheInner>,
}

#[derive(Debug, Default)]
struct CacheInner {
    map: crate::entropy::HashMap<[u8; 32], Bytes>,
    order: std::collections::VecDeque<[u8; 32]>,
    bytes: u64,
}

impl ArtifactCache {
    pub fn new(budget: CacheBudget) -> Self {
        Self {
            budget,
            inner: Default::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn get(&self, sha: &[u8; 32]) -> Option<Bytes> {
        self.lock().map.get(sha).cloned()
    }

    /// Keeps `bytes` of `sha` if there is room (evicting this range's oldest entry when needed).
    pub fn insert(&self, sha: [u8; 32], bytes: Bytes) {
        let n = bytes.len() as u64;
        if n > self.budget.cap {
            return;
        }
        let mut inner = self.lock();
        if inner.map.contains_key(&sha) {
            return;
        }
        while !self.budget.reserve(n) {
            let Some(old) = inner.order.pop_front() else {
                return;
            };
            if let Some(b) = inner.map.remove(&old) {
                inner.bytes -= b.len() as u64;
                self.budget.release(b.len() as u64);
            }
        }
        inner.bytes += n;
        inner.order.push_back(sha);
        inner.map.insert(sha, bytes);
    }

    /// Bytes held by this range.
    pub fn bytes(&self) -> u64 {
        self.lock().bytes
    }
}

impl Drop for ArtifactCache {
    fn drop(&mut self) {
        let n = self.lock().bytes;
        self.budget.release(n);
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    /// Two ranges share a 10-byte cap: a full range evicts its own oldest entry; another range
    /// without enough room does not keep the entry; drop returns the room.
    #[test]
    fn range_caches_share_the_node_cap() {
        let budget = CacheBudget::new(10);
        let a = ArtifactCache::new(budget.clone());
        let b = ArtifactCache::new(budget.clone());
        a.insert([1; 32], Bytes::from_static(b"aaaa"));
        a.insert([2; 32], Bytes::from_static(b"bbbb"));
        assert_eq!(budget.used(), 8);
        a.insert([3; 32], Bytes::from_static(b"cccc"));
        assert_eq!(
            a.get(&[1; 32]),
            None,
            "the range's own oldest entry is evicted"
        );
        assert_eq!(a.get(&[3; 32]).as_deref(), Some(&b"cccc"[..]));
        b.insert([4; 32], Bytes::from_static(b"dddd"));
        assert_eq!(
            b.get(&[4; 32]),
            None,
            "another range without enough room: not kept"
        );
        b.insert([5; 32], Bytes::from_static(b"dd"));
        assert_eq!(budget.used(), 10);
        a.insert([9; 32], Bytes::from_static(b"over cap eleven"));
        assert_eq!(a.get(&[9; 32]), None);
        drop(a);
        assert_eq!(budget.used(), 2);
    }
}
