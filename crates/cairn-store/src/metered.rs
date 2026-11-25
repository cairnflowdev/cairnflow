// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Counts object store requests by operation kind: the request rate is the cost of a
//! deployment — PUT/LIST/COPY are class A, GET/HEAD class B in S3-style pricing — and
//! previously no metric showed it.
//!
//! [`MeteredStore`] wraps the node's store (`main`, `wal`) right below every other layer
//! (SST disk cache, failpoints), so only requests that **really go out to the store** are
//! counted. The counters belong to the **process** (a harness with several nodes in one
//! process shares them), like the replay RAM budget. One `list` counts as one call, not one
//! per page; multipart counts the upload open, not each part — a lower bound on the number
//! of class A requests, enough to see trends and alert.

use {
    async_trait::async_trait,
    bytes::Bytes,
    futures::{stream::BoxStream, StreamExt},
    slatedb::object_store::{
        path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    },
    std::{
        fmt,
        ops::Range,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    },
};

/// Operation kind, the `op` label of `cairnflow_store_requests_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    Get = 0,
    Head = 1,
    Put = 2,
    List = 3,
    Delete = 4,
    Copy = 5,
}

impl StoreOp {
    pub const ALL: [StoreOp; 6] = [
        StoreOp::Get,
        StoreOp::Head,
        StoreOp::Put,
        StoreOp::List,
        StoreOp::Delete,
        StoreOp::Copy,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StoreOp::Get => "get",
            StoreOp::Head => "head",
            StoreOp::Put => "put",
            StoreOp::List => "list",
            StoreOp::Delete => "delete",
            StoreOp::Copy => "copy",
        }
    }
}

static REQUESTS: [AtomicU64; 6] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

fn bump(op: StoreOp) {
    REQUESTS[op as usize].fetch_add(1, Ordering::Relaxed);
}

/// The number of requests sent for `op` since the process started.
pub fn requests(op: StoreOp) -> u64 {
    REQUESTS[op as usize].load(Ordering::Relaxed)
}

/// A wrapping `ObjectStore` that counts each request by [`StoreOp`].
pub struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    /// `None` = the process counters ([`requests`]); `Some` = the wrapper's own counter,
    /// every operation kind sharing one number ([`Self::counting`]).
    local: Option<Arc<AtomicU64>>,
}

impl MeteredStore {
    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self { inner, local: None })
    }

    /// Counts every request (every operation kind, with the same counting rules as
    /// [`Self::wrap`]) into the caller's `counter`, **not** into the process counters — to
    /// measure the cost of a background loop (the status scanner). This extra wrapping
    /// layer sits above the node's counting layer, so the request is still counted once in
    /// `cairnflow_store_requests_total`.
    pub fn counting(inner: Arc<dyn ObjectStore>, counter: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            local: Some(counter),
        })
    }

    fn bump(&self, op: StoreOp) {
        match &self.local {
            Some(c) => {
                c.fetch_add(1, Ordering::Relaxed);
            }
            None => bump(op),
        }
    }
}

impl fmt::Debug for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeteredStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for MeteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeteredStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MeteredStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.bump(StoreOp::Put);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.bump(StoreOp::Put);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.bump(if options.head {
            StoreOp::Head
        } else {
            StoreOp::Get
        });
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Each range is one GET with `Range` in the S3 client.
        for _ in ranges {
            self.bump(StoreOp::Get);
        }
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let local = self.local.clone();
        self.inner.delete_stream(
            locations
                .inspect(move |_| match &local {
                    Some(c) => {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    None => bump(StoreOp::Delete),
                })
                .boxed(),
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.bump(StoreOp::List);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.bump(StoreOp::List);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.bump(StoreOp::List);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.bump(StoreOp::Copy);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        // S3 has no rename: the client does a copy then a delete.
        self.bump(StoreOp::Copy);
        self.bump(StoreOp::Delete);
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        slatedb::object_store::{memory::InMemory, ObjectStoreExt},
    };

    /// Every operation reaching the store is counted under the right kind (process
    /// counters: compare the difference).
    #[tokio::test]
    async fn counts_each_request_by_op() {
        let s = MeteredStore::wrap(Arc::new(InMemory::new()));
        let before: Vec<u64> = StoreOp::ALL.iter().map(|op| requests(*op)).collect();
        let p = Path::from("a/b");
        s.put(&p, PutPayload::from_static(b"x")).await.unwrap();
        s.get(&p).await.unwrap();
        s.head(&p).await.unwrap();
        let _: Vec<_> = s.list(Some(&Path::from("a"))).collect().await;
        s.copy(&p, &Path::from("a/c")).await.unwrap();
        s.delete(&p).await.unwrap();
        let after: Vec<u64> = StoreOp::ALL.iter().map(|op| requests(*op)).collect();
        // An outer layer with its own counter: it counts all six operations into its
        // counter, while the process counters still count once at the inner layer.
        let mine = Arc::new(AtomicU64::new(0));
        let outer = MeteredStore::counting(s.clone(), mine.clone());
        let q = Path::from("a/d");
        outer.put(&q, PutPayload::from_static(b"x")).await.unwrap();
        outer.get(&q).await.unwrap();
        outer.head(&q).await.unwrap();
        let _: Vec<_> = outer.list(Some(&Path::from("a"))).collect().await;
        outer.copy(&q, &Path::from("a/e")).await.unwrap();
        outer.delete(&q).await.unwrap();
        assert_eq!(mine.load(Ordering::Relaxed), 6);
        let twice: Vec<u64> = StoreOp::ALL.iter().map(|op| requests(*op)).collect();
        for (i, op) in StoreOp::ALL.iter().enumerate() {
            assert!(twice[i] > after[i], "{} missed inner layer", op.as_str());
        }
        for (i, op) in StoreOp::ALL.iter().enumerate() {
            assert!(
                after[i] > before[i],
                "{} was not counted: {} → {}",
                op.as_str(),
                before[i],
                after[i]
            );
        }
    }
}
