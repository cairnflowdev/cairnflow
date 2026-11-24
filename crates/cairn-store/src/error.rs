// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

use slatedb::{CloseReason, ErrorKind};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// This writer is no longer the valid writer of the partition. Another writer
    /// has opened the store and bumped `writer_epoch`.
    ///
    /// **Do not retry on the same writer.** The right path: drop the writer, return
    /// `NotLeader` to the client (principle N3), and let the lease tier decide who goes next.
    #[error("writer was fenced — no longer the leader of the partition")]
    Fenced,

    /// The lease was lost before any I/O was spent. Same meaning as `Fenced` for the
    /// caller, the only difference is that we stop earlier.
    #[error("lost lease before commit")]
    LeaseLost,

    /// The store is closed.
    #[error("store is closed")]
    Closed,

    /// The key exceeds SlateDB's limit.
    ///
    /// Return an error instead of letting it reach `WriteBatch`, where SlateDB `assert!`s and
    /// **panics** — a panic on the write path kills the engine thread too, i.e. the whole
    /// partition stalls because of one long object key. See [`crate::keys::MAX_KEY`].
    #[error("table key {what} is {len} bytes long, the limit is {max}")]
    KeyTooLong {
        what: &'static str,
        len: usize,
        max: usize,
    },

    /// The state key has a [`crate::keys::Table::SegmentedState`] row that this read path
    /// cannot assemble yet: an error, not treated as absent, and the tail is not read as the
    /// whole value. In the first stage of segmented values no product path writes that
    /// row.
    #[error("state key {key} holds a segmented value that this binary cannot read yet")]
    Segmented { key: String },

    /// A sealing frame of the shell reuses a segment that is neither in the key's current
    /// value nor just PUT: a ref nobody holds may be waiting for deletion — refuse, do not
    /// commit.
    #[error("state key {key} references a segment it does not hold")]
    ForeignSegment { key: String },

    /// A [`crate::keys::Table::SegmentedState`] row could not be decoded (strict read).
    #[error("segmented state row for key {key} is corrupt")]
    CorruptSegmented { key: String },

    /// The URI of a store tier could not be resolved.
    #[error("store address {uri:?} is not usable: {reason}")]
    BadStoreUri { uri: String, reason: String },

    /// **The D12 gate failed.** The store swallows `PutMode::Create`.
    ///
    /// This is a startup error, not an operational one: SlateDB's fence against a zombie
    /// leader *is* the conditional PUT, so running on such a store means running with no
    /// fence at all — and the incident that comes out of it is silent data loss, not an
    /// error somebody will see.
    ///
    /// There is no `--force` flag. Latency can be traded, correctness cannot.
    #[error(
        "store tier {tier} ({store}) does not honor conditional PUT: {reason}\nWithout this fence an old leader can overwrite the new leader's data. Change the store (SeaweedFS and MinIO measured as passing; Garage v1.0.1 failed)."
    )]
    ConditionalPutUnsupported {
        tier: String,
        store: String,
        reason: String,
    },

    /// The store accepts `PutMode::Create` but cannot do CAS on the etag.
    ///
    /// Separate from [`Self::ConditionalPutUnsupported`] because these are two different
    /// capabilities and they block two different things: missing `Create` means **losing
    /// correctness** (no fence); missing CAS means **losing leader election** (data stays
    /// safe thanks to the `Create` fence, but no node can hold a lease). `LocalFileSystem`
    /// has the former and not the latter — a node on `--data-dir` gets through thanks to a
    /// CAS emulated under the directory's exclusive lock, hence only one
    /// node.
    #[error(
        "store ({store}) cannot do CAS on etag: {reason}\nThe lease tier needs this to elect a leader. Multiple nodes need an S3-compatible store (MinIO, SeaweedFS, S3); a single node on a local directory uses --data-dir / file:// (the node holds an exclusive lock on the directory)."
    )]
    CompareAndSwapUnsupported { store: String, reason: String },

    /// The rule book cannot be decoded, or cannot be written after several CAS attempts.
    ///
    /// Refuse the whole object instead of skipping a line we do not understand: running on
    /// with a rule book *missing one rule* means running with a looser cap than the operator
    /// thinks — exactly the expensive failure flow control exists to prevent.
    #[error("rule book is not usable: {reason}")]
    BadRuleBook { reason: String },

    /// A tenant's deployment catalog cannot be read.
    #[error("deployment catalog is not usable: {reason}")]
    BadCatalog { reason: String },

    /// The catalog edit function refused (e.g. a conditional move whose pointer has
    /// changed), or CAS missed more times than allowed.
    #[error("deployment catalog unchanged: {reason}")]
    CatalogRejected { reason: String },

    /// Deployments are immutable: an id that already exists is not written again.
    #[error("deployment {id} already exists — deployments are immutable, ids are not reused")]
    DeploymentExists { id: String },

    /// Invalid deployment id (it is a segment of an object path).
    #[error(
        "invalid deployment id: {id:?} — must be `dp_` + lowercase letters/digits, ≤ 64 bytes"
    )]
    BadDeploymentId { id: String },

    /// Invalid scope / limit key / pattern.
    #[error(transparent)]
    Ident(#[from] crate::scope::IdentError),

    /// The cluster config cannot be read, or the node disagrees with the cluster on the
    /// partition count.
    ///
    /// A startup error, with no flag to skip it. See [`crate::ClusterConfig::open`].
    #[error("cluster config is not usable: {reason}")]
    BadClusterConfig { reason: String },

    /// `cluster.conf` is absent while the bucket already holds cluster data (`ranges/`,
    /// `partition-*`, `owners/`). The table cannot be rebuilt from the rest of the bucket
    /// (owner records do not carry the key span), and a new table would hand out again ids
    /// that collide with live Dbs (breaking D22). A startup error, with no flag to skip it;
    /// permanent as far as `retry` is concerned (the operator must bring the object back:
    /// `cairn-ctl table restore` from `cluster-history/`).
    #[error(
        "cluster.conf is missing at {path} but the bucket already holds range data (ranges/ or \
         owners/) — refusing to start: a new range table here would reuse the ids of live \
         ranges and lose data. Do not start any node until the table is back: rebuild it from \
         cluster-history/ with `cairn-ctl table restore` (dry run first, then --apply), or \
         restore the last version of cluster.conf from bucket versioning. See \
         docs/runbooks/cluster-conf-lost.md"
    )]
    MissingClusterConfig { path: String },

    /// The lease object cannot be decoded.
    ///
    /// Refuse instead of guessing: a misread lease is a *misgranted* lease, and
    /// "guessing it is free" means granting leadership on top of a leader that is
    /// healthy.
    #[error("corrupt lease record: {reason}")]
    LeaseCorrupt { reason: String },

    /// Infrastructure error while reading/writing the lease object. Retryable.
    #[error("I/O error in the lease tier: {0}")]
    LeaseIo(String),

    /// Infrastructure error while reading/writing an administrative object of the bucket
    /// (format marker).
    #[error("I/O error with the object store: {0}")]
    Io(String),

    /// A write path was called against the keyspace rules (e.g. clearing an "index row"
    /// whose key lies outside the derived region). A programming error, not an
    /// infrastructure error.
    #[error("keyspace violation: {0}")]
    Corrupt(String),

    /// **The bucket carries data in a different format from this binary.**
    ///
    /// A startup error, with no flag to skip it: reading data of another format reports no
    /// error at all, it just misunderstands every byte. `found = None` means the bucket has
    /// partition data but no marker — i.e. it was written by a binary from before markers
    /// existed. See `format.rs`.
    ///
    /// Never advise deleting the bucket (D27): newer means upgrade the binary; older than
    /// the frozen format 9 means only the old binary can read it.
    #[error(
        "bucket carries data format {} but this binary reads format {expected}.\n{}",
        match found { Some(v) => v.to_string(), None => "old (no marker)".to_string() },
        data_format_hint(*found, *expected)
    )]
    DataFormat { found: Option<u32>, expected: u32 },

    /// **The object carries a header (or version marker) newer than this binary
    /// understands.** The bucket was upgraded by a newer binary.
    ///
    /// Stop loudly instead of running on: reading past it means running with half the
    /// configuration, writing back with the old layout silently erases what the new binary
    /// added.
    #[error(
        "{object} carries {header:?} — newer than this binary understands. The bucket was upgraded by a newer binary: upgrade this binary (it does not overwrite and does not keep running with that object)"
    )]
    NewerFormat { object: String, header: String },

    /// The wait timed out and the partition still has another owner.
    ///
    /// Not a failure: this is the normal answer for a follower node. The caller either
    /// keeps waiting, or serves reads and leaves it as is.
    #[error("partition {partition} has another leader")]
    LeaseUnavailable { partition: u32 },

    /// The range's owner record carries a WAL kind this binary does not know how to read
    /// (D-f).
    ///
    /// Refuse to open, do not overwrite the record: opening the Db with the native WAL when
    /// its unflushed part lives in another WAL silently drops acked rows. The range stays
    /// without a leader until a binary that understands that kind claims it.
    #[error(
        "range {range}: owner record carries wal_kind={kind:?} which this binary cannot read — refusing to open; upgrade the binary to serve this range"
    )]
    UnknownWalKind { range: u32, kind: String },

    /// The range's owner record still carries the native WAL (`wal_kind=slatedb`) after the
    /// range-Db-tier migration steps: the native WAL is no longer served — only the
    /// `wal/shared` step opens a native-WAL Db, to flush it. Getting here means the cluster
    /// (`version`) has not reached that step yet, or the binary's registry does not have it
    /// (a test registry). Do not open; the range stays without a leader.
    #[error(
        "range {range}: Db still has a native WAL but the cluster is at version {version}, not yet at the wal/shared migration step — the native WAL is no longer served; not opening"
    )]
    NativeWal { range: u32, version: u32 },

    /// The range is retired (its owner record is a tombstone): nobody may claim it, nobody
    /// may open its Db any more. The range's engine exits for good when it meets this
    /// error.
    #[error("range {range} is retired — not claimed, its Db is not reopened")]
    RangeRetired { range: u32 },

    #[error("lower-layer error: {0}")]
    Slate(#[from] slatedb::Error),
}

impl StoreError {
    /// `Err` if the key exceeds [`crate::keys::MAX_KEY`].
    pub(crate) fn key(what: &'static str, key: &[u8]) -> Result<()> {
        crate::keyspace::check_key(key).map_err(|len| Self::KeyTooLong {
            what,
            len,
            max: crate::keyspace::MAX_KEY,
        })
    }

    /// Classify SlateDB errors. `Closed(Fenced)` is the signal of losing write rights and
    /// must be kept apart from ordinary infrastructure errors because the handling is
    /// entirely different: one yields leadership, the other retries.
    pub(crate) fn from_slate(e: slatedb::Error) -> Self {
        match e.kind() {
            ErrorKind::Closed(CloseReason::Fenced) => Self::Fenced,
            ErrorKind::Closed(_) => Self::Closed,
            _ => Self::Slate(e),
        }
    }

    /// Whether this error means "yield leadership".
    pub fn is_fenced(&self) -> bool {
        matches!(self, Self::Fenced | Self::LeaseLost)
    }
}

/// The advice of [`StoreError::DataFormat`] by direction of the mismatch.
fn data_format_hint(found: Option<u32>, expected: u32) -> String {
    match found {
        Some(v) if v > expected => "Bucket is newer than this binary: upgrade this binary.".into(),
        Some(v) if v >= 9 => {
            format!("Run a binary with a migration path from format {v} (keep the bucket).")
        }
        _ => "Formats before 9 (frozen, D27) have no migration path: that data is only readable by its old binary. For a new cluster point at another prefix; keep the old data if you still need it."
            .into(),
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;
