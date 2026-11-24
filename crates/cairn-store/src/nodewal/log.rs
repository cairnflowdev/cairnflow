// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operations on **one** shared WAL log, on one bucket (M = 1).
//!
//! ```text
//! <wal-prefix>/nodewal/<node>/<inc>/<stripe>/<seq:020>       data object, or seal
//! <wal-prefix>/nodewal/<node>/<inc>/<stripe>/floor/<f:020>   GC floor (empty)
//! <wal-prefix>/nodewal/<node>/<inc>/<stripe>/index           index (overwritten; read leniently)
//! ```
//!
//! A Q-of-M quorum (D-g) would replace this type with one offering the same four
//! operations over M buckets; the rest of the shared WAL does not change.

use {
    super::format::{self, Decoded},
    crate::{
        error::{Result, StoreError},
        owner::LogId,
    },
    bytes::Bytes,
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    },
    std::sync::Arc,
};

/// Name of the "purged" mark in a log's directory ([`LogStore::forget`]). A new object,
/// read leniently: its **presence** says the log has been purged (the purge round does not
/// seal the log again, `verify` does not count it); absent = not purged — exactly like
/// every older bucket.
pub(crate) const FORGOTTEN: &str = "forgotten";

/// First line of the mark. Each seal that stays behind also gets a `seal <seq>` line, so an
/// unfinished purge can be completed without sealing again
/// ([`LogStore::forgotten_seals`]). Read leniently: an old mark (first line only) or an
/// unknown line ⇒ the seal position is unknown and the reader probes for it.
const FORGOTTEN_HEADER: &str = "cairnflow-nodewal-forgotten/1";

fn forgotten_body(seals: &[u64]) -> Bytes {
    let mut body = format!("{FORGOTTEN_HEADER}\n");
    for s in seals {
        body.push_str(&format!("seal {s}\n"));
    }
    Bytes::from(body)
}

/// The seals the mark records; `None` = the mark does not say (an old mark, or damaged).
fn parse_forgotten(bytes: &[u8]) -> Option<Vec<u64>> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    if lines.next()? != FORGOTTEN_HEADER {
        return None;
    }
    let seals = lines
        .map(|l| l.strip_prefix("seal ")?.parse::<u64>().ok())
        .collect::<Option<Vec<u64>>>()?;
    (!seals.is_empty()).then_some(seals)
}

/// Objects per trash entry when purging a log, and the number of entries moved in
/// parallel: a dead log keeps all its objects until the purge, and within one entry the
/// objects are moved sequentially (copy, then delete). In a 10-minute chaos run on the
/// `s3-standard` profile with 64 objects per entry, a log of ~100 objects took ~40 s —
/// longer than the life of many processes under a kill every 10 s, and nodes purging the
/// same log overlapped for longer. One entry costs one extra PUT and one HEAD.
const FORGET_CHUNK: usize = 16;
const FORGET_IN_FLIGHT: usize = 16;

/// Result of one `Create`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Created {
    /// We created the object.
    Yes,
    /// The name already has an object (someone else's, or our own from an earlier send).
    AlreadyExists,
}

#[derive(Clone)]
pub(crate) struct LogStore {
    store: Arc<dyn ObjectStore>,
    dir: String,
    /// Root of the trash ([`Self::delete_all`]) and the identity of the log.
    root: Path,
    log: LogId,
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Root directory of every shared WAL log on the WAL store.
pub(crate) fn nodewal_root(wal_prefix: &Path) -> String {
    if wal_prefix.as_ref().is_empty() {
        "nodewal".into()
    } else {
        format!("{}/nodewal", wal_prefix.as_ref())
    }
}

impl LogStore {
    pub fn new(store: Arc<dyn ObjectStore>, wal_prefix: &Path, log: &LogId) -> Self {
        let dir = format!(
            "{}/{}/{}/{}",
            nodewal_root(wal_prefix),
            log.session.node,
            log.session.inc,
            log.stripe
        );
        LogStore {
            store,
            dir,
            root: wal_prefix.clone(),
            log: log.clone(),
        }
    }

    fn object(&self, seq: u64) -> Path {
        Path::from(format!("{}/{seq:020}", self.dir))
    }

    fn floor_dir(&self) -> Path {
        Path::from(format!("{}/floor", self.dir))
    }

    fn floor_marker(&self, f: u64) -> Path {
        Path::from(format!("{}/floor/{f:020}", self.dir))
    }

    /// `PutMode::Create` at `seq`.
    pub async fn create(&self, seq: u64, bytes: Bytes) -> Result<Created> {
        match self
            .store
            .put_opts(
                &self.object(seq),
                PutPayload::from(bytes),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(Created::Yes),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => {
                Ok(Created::AlreadyExists)
            }
            Err(e) => Err(io(e)),
        }
    }

    /// Bytes of the object at `seq`, undecoded — to compare with the bytes we sent when
    /// re-reading an ambiguous PUT (`stripe::put`). `Ok(None)` = absent.
    pub async fn get_raw(&self, seq: u64) -> Result<Option<Bytes>> {
        match self.store.get(&self.object(seq)).await {
            Ok(got) => got.bytes().await.map(Some).map_err(io),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// Reads and decodes the object at `seq`. `Ok(None)` = absent.
    pub async fn get(&self, seq: u64) -> Result<Option<Decoded>> {
        match self.store.get(&self.object(seq)).await {
            Ok(got) => {
                let bytes = got.bytes().await.map_err(io)?;
                format::decode(&bytes).map(Some)
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    fn index_path(&self) -> Path {
        Path::from(format!("{}/index", self.dir))
    }

    /// Writes (overwrites) the log's index. Both writers — the writer on its GC tick, and a
    /// sealer after reading the tail — write the truth about immutable objects, so whichever
    /// copy wins is correct; an older copy is only shorter.
    pub async fn put_index(&self, bytes: Bytes) -> Result<()> {
        self.store
            .put(&self.index_path(), PutPayload::from(bytes))
            .await
            .map(|_| ())
            .map_err(io)
    }

    /// The log's index, **read leniently**: absent or undecodable ⇒ `None` (the caller reads
    /// the log as if there were no index). An I/O error is returned as an error.
    pub async fn index(&self) -> Result<Option<format::LogIndex>> {
        match self.store.get(&self.index_path()).await {
            Ok(got) => {
                let bytes = got.bytes().await.map_err(io)?;
                match format::decode_index(&bytes) {
                    Ok(ix) => Ok(Some(ix)),
                    Err(e) => {
                        tracing::debug!(dir = %self.dir, error = %e, "shared WAL log index unreadable — ignored");
                        Ok(None)
                    }
                }
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(io(e)),
        }
    }

    /// The log's GC floor: the largest `f` with an object `floor/<f>`; 0 if none.
    pub async fn floor(&self) -> Result<u64> {
        let dir = self.floor_dir();
        let listed: Vec<_> = self
            .store
            .list(Some(&dir))
            .try_collect()
            .await
            .map_err(io)?;
        Ok(listed
            .iter()
            .filter_map(|m| m.location.filename()?.parse::<u64>().ok())
            .max()
            .unwrap_or(0))
    }

    /// The log's directory on the WAL store.
    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// **Claims** the log for a new stream (stream rotation): `Create` of floor 0 — an
    /// object every log already has (floor 0 = "nothing GCed yet", read as no floor), so
    /// no new kind of object is added. `AlreadyExists` ⇒ the id is already in use (or it is
    /// our own ambiguous `Create`) — the caller skips that id.
    pub async fn claim(&self) -> Result<Created> {
        match self
            .store
            .put_opts(
                &self.floor_marker(0),
                PutPayload::from(Bytes::new()),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) => Ok(Created::Yes),
            Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => {
                Ok(Created::AlreadyExists)
            }
            Err(e) => Err(io(e)),
        }
    }

    /// Writes floor `f` (`Create`; already present means done).
    pub async fn write_floor(&self, f: u64) -> Result<()> {
        match self
            .store
            .put_opts(
                &self.floor_marker(f),
                PutPayload::from(Bytes::new()),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(_) | Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => Ok(()),
            Err(e) => Err(io(e)),
        }
    }

    /// **Purges** the log: every object — data, floors, index — **except the seals at
    /// `seals`**, and leaves the [`FORGOTTEN`] mark behind. Only for logs of a dead
    /// incarnation that no owner record references any more: nobody will seal or replay it
    /// again. A dead log has no floor GC, so what is moved away usually includes every data
    /// object it has.
    ///
    /// **The seal stays** (model M3, `forget_keeps_seal`): it is a fence on the bucket that
    /// needs no clock. Moving it away would let a zombie of that incarnation — a grant still
    /// valid after the session was declared dead, i.e. a broken clock assumption — PUT into
    /// the old seal's exact spot and ack (model M3,
    /// `tests/m3.rs::without_the_clock_forgetting_a_dead_log_lets_its_zombie_ack`).
    /// Cost: two objects per dead log (seal, mark), forever.
    ///
    /// The mark — carrying the seal positions — is written **before** anything is moved:
    /// dying midway, the log is never sealed again from floor 0 (a new seal elsewhere, and
    /// the next purge would then move the real seal away), and the next purge round calls
    /// this function again with the seals read from the mark ([`Self::forgotten_seals`]) to
    /// finish — however many times it is called, the outcome is the same.
    ///
    /// Objects are **moved into the trash** ([`crate::trash`]), not really deleted — one
    /// entry per [`FORGET_CHUNK`] objects. Returns the number of objects moved.
    ///
    /// Gate: `proof` must be [`crate::proof::DeleteProof::dead_log`] for exactly this log.
    pub async fn forget(&self, seals: &[u64], proof: crate::proof::DeleteProof) -> Result<usize> {
        use futures::StreamExt;
        proof
            .check(&crate::trash::Subject::WalLog(self.log.clone()), None)
            .await?;
        let mark = Path::from(format!("{}/{FORGOTTEN}", self.dir));
        self.store
            .put(&mark, PutPayload::from(forgotten_body(seals)))
            .await
            .map_err(io)?;
        let mut keep: Vec<Path> = seals.iter().map(|s| self.object(*s)).collect();
        keep.push(mark);
        let dir = Path::from(self.dir.as_str());
        let listed: Vec<Path> = self
            .store
            .list(Some(&dir))
            .map_ok(|m| m.location)
            .try_filter(|p| std::future::ready(!keep.contains(p)))
            .try_collect()
            .await
            .map_err(io)?;
        if listed.is_empty() {
            return Ok(0);
        }
        let chunks: Vec<Vec<Path>> = listed.chunks(FORGET_CHUNK).map(<[Path]>::to_vec).collect();
        let moved: Vec<Result<usize>> = futures::stream::iter(chunks)
            .map(|chunk| {
                let trash = crate::trash::Trash::new(Arc::clone(&self.store), &self.root);
                let subject = crate::trash::Subject::WalLog(self.log.clone());
                async move {
                    trash
                        .move_into(
                            &crate::trash::EntryId::new(crate::entropy::now_ms()),
                            &subject,
                            "shared WAL log of a dead incarnation, listed by no owner record (seal stays)",
                            &chunk,
                        )
                        .await
                }
            })
            .buffer_unordered(FORGET_IN_FLIGHT)
            .collect()
            .await;
        moved.into_iter().sum()
    }

    /// The seals that stay behind for a log that **already carries the mark** (an unfinished
    /// [`Self::forget`]): read from the mark. If the mark does not say (an old mark, from
    /// before marks recorded seals), GET every object numbered `seqs` still on the log and
    /// keep every one that is a seal — or that cannot be decoded (rule: what is unknown is
    /// kept). Never seals again.
    pub async fn forgotten_seals(
        &self,
        seqs: &std::collections::BTreeSet<u64>,
    ) -> Result<Vec<u64>> {
        let mark = Path::from(format!("{}/{FORGOTTEN}", self.dir));
        let body = match self.store.get(&mark).await {
            Ok(got) => Some(got.bytes().await.map_err(io)?),
            Err(OsError::NotFound { .. }) => None,
            Err(e) => return Err(io(e)),
        };
        if let Some(seals) = body.as_deref().and_then(parse_forgotten) {
            return Ok(seals);
        }
        let mut seals = Vec::new();
        for &seq in seqs {
            match self.get(seq).await {
                Ok(Some(Decoded::Data(_))) | Ok(None) => {}
                Ok(Some(Decoded::Seal { .. })) | Err(StoreError::Corrupt(_)) => seals.push(seq),
                Err(e) => return Err(e),
            }
        }
        Ok(seals)
    }

    /// The log has been purged by [`Self::forget`]: the [`FORGOTTEN`] mark is present.
    /// Read-only (one HEAD) — for offline tools (`cairn-ctl wal floors`).
    pub async fn forgotten(&self) -> Result<bool> {
        match self
            .store
            .head(&Path::from(format!("{}/{FORGOTTEN}", self.dir)))
            .await
        {
            Ok(_) => Ok(true),
            Err(OsError::NotFound { .. }) => Ok(false),
            Err(e) => Err(io(e)),
        }
    }

    /// Deletes the object at `seq` (absent is fine).
    pub async fn delete(&self, seq: u64) -> Result<()> {
        match self.store.delete(&self.object(seq)).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(e) => Err(io(e)),
        }
    }

    /// Deletes the floor objects below `f` — only the largest floor is needed.
    pub async fn delete_floors_below(&self, f: u64) -> Result<()> {
        let dir = self.floor_dir();
        let listed: Vec<_> = self
            .store
            .list(Some(&dir))
            .try_collect()
            .await
            .map_err(io)?;
        for m in listed {
            if m.location
                .filename()
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|g| g < f)
            {
                match self.store.delete(&m.location).await {
                    Ok(()) | Err(OsError::NotFound { .. }) => {}
                    Err(e) => return Err(io(e)),
                }
            }
        }
        Ok(())
    }
}
