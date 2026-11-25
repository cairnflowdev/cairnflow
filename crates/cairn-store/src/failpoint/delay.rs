// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A store slowed down by a failpoint — for the real processes of the chaos harness
//! (`--slow-node`, `--l0-pressure`).
//!
//! `FaultyStore` (testkit) only lives inside the test process, and the simulated store
//! profile is only read at startup. This is the thing that can be switched on/off **at run
//! time**, through each node's `POST /admin/failpoints`: a point configured `return(<ms>)`
//! makes every matching operation wait `<ms>` milliseconds (asynchronously, without blocking
//! a thread) before it really runs. `return(<ms>:<string>)` only waits when the path
//! contains `<string>`. Every other action (`off`, `pause`, …) has `fail`'s meaning.
//!
//! Each chaos node is a process, so enabling a point on one node's `/admin/failpoints` is
//! already "conditional on the node"; the [`super::only_on`] limit is still checked like for
//! every other point.
//!
//! Only present with the `failpoints` feature: the release build wraps nothing.

use {
    async_trait::async_trait,
    bytes::Bytes,
    futures::stream::BoxStream,
    slatedb::object_store::{
        path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    },
    std::{fmt, ops::Range, sync::Arc, time::Duration},
};

/// Which operations are slowed down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelayOps {
    /// PUT (including conditional and multipart PUT) — a node with a slow network disk.
    Writes,
    /// PUT and GET — the compactor reads old SSTs and writes new SSTs.
    ReadsAndWrites,
}

/// A wrapping `ObjectStore` that waits according to failpoint `name` before each matching
/// operation.
pub struct DelayStore {
    inner: Arc<dyn ObjectStore>,
    name: &'static str,
    ops: DelayOps,
}

impl DelayStore {
    pub fn wrap(inner: Arc<dyn ObjectStore>, name: &'static str, ops: DelayOps) -> Arc<Self> {
        debug_assert!(
            super::NAMES.contains(&name),
            "failpoint {name} is not in NAMES"
        );
        Arc::new(Self { inner, name, ops })
    }

    async fn wait(&self, location: &Path) {
        if let Some(d) = delay_for(self.name, location) {
            tokio::time::sleep(d).await;
        }
    }
}

/// The delay point `name` currently asks for `location`, if it is enabled.
fn delay_for(name: &str, location: &Path) -> Option<Duration> {
    if !super::node_allows(name) {
        return None;
    }
    let arg = super::hit(name)??;
    let (ms, only) = match arg.split_once(':') {
        Some((ms, only)) => (ms, Some(only)),
        None => (arg.as_str(), None),
    };
    let ms: u64 = match ms.trim().parse() {
        Ok(ms) => ms,
        Err(_) => {
            tracing::warn!(failpoint = name, arg = %arg, "argument is not `<ms>[:<string>]` — ignored");
            return None;
        }
    };
    if only.is_some_and(|s| !location.as_ref().contains(s)) {
        return None;
    }
    Some(Duration::from_millis(ms))
}

impl fmt::Debug for DelayStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayStore")
            .field("name", &self.name)
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for DelayStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayStore({}, {})", self.name, self.inner)
    }
}

#[async_trait]
impl ObjectStore for DelayStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.wait(location).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.wait(location).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if self.ops == DelayOps::ReadsAndWrites {
            self.wait(location).await;
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if self.ops == DelayOps::ReadsAndWrites {
            self.wait(location).await;
        }
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
        self.wait(to).await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.wait(to).await;
        self.inner.rename_opts(from, to, options).await
    }
}
