// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Store on a local directory** (`file://`, the `--data-dir` path).
//!
//! `LocalFileSystem` has `PutMode::Create` (SlateDB's `writer_epoch` fence, the shared WAL
//! log) but no CAS on the etag, while the lease layer renews sessions and takes ranges by
//! CAS. A local directory cannot be shared between machines, so a node on it is the **only**
//! node of the bucket. [`DirLock`] turns that from an assumption into a fact — an exclusive
//! lock (`flock`) on the directory, released by the operating system when the process dies,
//! even on `kill -9` — and under that lock:
//!
//! - [`LocalCas`] emulating CAS inside the process is correct: no other process writes to
//!   the directory;
//! - a session still `live` on the bucket at startup belongs to an earlier run that **has
//!   died** (it released the lock), so expiring it immediately is exactly what an observer
//!   would do after a silent TTL ([`crate::expire_sessions_under_lock`]).
//!
//! `cairn-ctl` does not hold the lock: it reads, and every write it makes goes through
//! `cas::put_resolving_or_overwrite` as before.

use {
    bytes::Bytes,
    futures::stream::BoxStream,
    slatedb::object_store::{
        path::Path, CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, RenameOptions, Result,
    },
    std::{fmt, ops::Range, sync::Arc},
};

/// A node's exclusive locks on a local store ([`crate::ObjectStores::exclusive_local`]):
/// **proof** that no other process writes to the bucket. Held until dropped.
#[derive(Debug)]
pub struct LocalLock(pub(crate) Vec<DirLock>);

impl LocalLock {
    /// The locked main store directory.
    pub fn dir(&self) -> &std::path::Path {
        self.0[0].dir()
    }
}

/// An exclusive lock on a local store directory, held until dropped.
///
/// `flock(LOCK_EX | LOCK_NB)` on the directory itself (adding no object to the bucket). The
/// lock belongs to the open file description: a second open — even in the same process —
/// is refused.
#[derive(Debug)]
pub struct DirLock {
    _file: std::fs::File,
    dir: std::path::PathBuf,
}

impl DirLock {
    /// Takes the lock on `dir`, without waiting. Already held ⇒ an error that says why.
    pub fn acquire(dir: &std::path::Path) -> crate::Result<DirLock> {
        let bad = |reason: String| crate::StoreError::BadStoreUri {
            uri: format!("file://{}", dir.display()),
            reason,
        };
        let file = std::fs::File::open(dir)
            .map_err(|e| bad(format!("cannot open the directory to take the lock: {e}")))?;
        match file.try_lock() {
            Ok(()) => Ok(DirLock {
                _file: file,
                dir: dir.to_path_buf(),
            }),
            Err(std::fs::TryLockError::WouldBlock) => Err(bad(
                "the directory is held under an exclusive lock by another node — a local \
                 directory runs only one node (use an S3-compatible store for several nodes)"
                    .into(),
            )),
            Err(std::fs::TryLockError::Error(e)) => Err(bad(format!(
                "could not take the exclusive lock on the directory: {e}"
            ))),
        }
    }

    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }
}

/// Wraps a store that does **not** support `PutMode::Update` (such as `LocalFileSystem`)
/// and emulates it **within one process**: lock, HEAD, compare the etag, then overwrite.
///
/// Only correct when every conditional writer goes through the same wrapper — one process,
/// one node: in production that is under [`DirLock`]
/// ([`crate::ObjectStores::exclusive_local`]). Benchmarks also use it to measure splits on
/// `file://`.
pub struct LocalCas {
    inner: Arc<dyn ObjectStore>,
    lock: tokio::sync::Mutex<()>,
}

impl LocalCas {
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<LocalCas> {
        Arc::new(LocalCas {
            inner,
            lock: tokio::sync::Mutex::new(()),
        })
    }
}

impl fmt::Debug for LocalCas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalCas")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for LocalCas {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalCas({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocalCas {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let slatedb::object_store::PutMode::Update(want) = &opts.mode else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        let _held = self.lock.lock().await;
        let current = self.inner.head(location).await?;
        if current.e_tag != want.e_tag {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: "etag mismatch".into(),
            });
        }
        opts.mode = slatedb::object_store::PutMode::Overwrite;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
