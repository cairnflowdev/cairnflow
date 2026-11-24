// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Purging dead logs: `list_dirs`, `sweep_loop`, `hint_loop`, `forget_unlisted`.
//!
//! **Each round is a pure function of the bucket.** The log of a dead incarnation (or of
//! a rotated stream — even while its incarnation is still alive, in which case that very
//! incarnation purges it) has no floor GC: it stays whole until no owner record lists it
//! any more — that is, until its last range has been taken over and the taker has dropped
//! the old segments (C7) —, and is then sealed (if not yet) and purged once
//! ([`LogStore::forget`]: the seal and the `forgotten` mark stay, the rest goes to the
//! trash). No round keeps progress in RAM: candidates come from a LIST of `nodewal/`, death
//! from the session record, references from a snapshot of the owner records
//! ([`crate::proof::ReferencedLogs`]). Dying midway, the next round redoes it correctly:
//! the mark is written before anything is moved, so a log with the mark that still has
//! objects besides the seal is an unfinished purge — finish it, do not seal again.
//!
//! Historical bugs of the "dead-log progress in RAM" class (see `docs/guides/nodewal.md`,
//! common failure modes) that this design closes: a sweep not finishing within the life of
//! an incarnation; lives shorter than `gc_interval` never sweeping (12 of 88 in one run);
//! logs with only a seal left piling up (floor stuck for 639 s); a single failed seal
//! dropping the log for the rest of the process's life; a log GCed while still listed
//! staying in `swept` until the node died; an interrupted purge leaving the remainder
//! behind forever.

use super::*;

/// Direct subdirectories under `dir` (last name segment).
pub(crate) async fn list_dirs(store: &Arc<dyn ObjectStore>, dir: &str) -> Result<Vec<String>> {
    let listed = store
        .list_with_delimiter(Some(&Path::from(dir)))
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?;
    Ok(listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().map(str::to_string))
        .collect())
}

/// The full sweep runs every this many GC ticks — and again within seconds
/// ([`SWEEP_AGAIN_FIRST`], doubling up to one tick) while the previous round still has
/// work it can do right away (more than [`SWEEP_BATCH`] logs, or a failed read).
pub(super) const SWEEP_EVERY_GC: u32 = 10;

/// Maximum number of logs one round seals + purges: the rest goes to the next tick.
pub(super) const SWEEP_BATCH: usize = 16;

/// First rest after a round that failed or left work it can do right away; doubles up to
/// `gc_interval`. Waiting a whole `gc_interval` (30 s) meant an incarnation living 10–30 s
/// under chaos never reached a second round after a failed first one.
pub(super) const SWEEP_AGAIN_FIRST: Duration = Duration::from_secs(1);

/// Logs purged at once by one round (each purge moves its own chunks in parallel too).
/// Purging one log after another took ~1–1.5 s per log on a slow store — 15–25 s for a
/// batch, as long as an incarnation lived under chaos.
pub(super) const FORGET_LOGS_IN_FLIGHT: usize = 4;

/// Retries of one session record read by the sweep (a read: always safe to try again).
const SESSION_READ_RETRY: crate::retry::Policy =
    crate::retry::Policy::new(Duration::from_millis(100), Duration::from_secs(1), 4)
        .with_total(Duration::from_secs(5));

/// The first full sweep runs after this long (or after `gc_interval` if shorter), without
/// waiting a whole period: an incarnation living shorter than the period still sweeps once.
/// This is a cadence, not progress.
pub(super) const FIRST_SWEEP_AFTER: Duration = Duration::from_secs(1);

/// Hints after a range acquisition are gathered for this long (or `gc_interval` if
/// shorter) before running: when a node dies, the survivors acquire a batch of ranges within
/// a few seconds, and one round for the whole batch replaces one owner-record snapshot per
/// range.
pub(super) const HINT_GATHER: Duration = Duration::from_millis(250);

/// The full sweep: LIST `nodewal/`; every log of another incarnation — and every log of a
/// rotated stream of this very incarnation — that is not purged (or is half purged) is a
/// candidate.
pub(super) async fn sweep_loop(inner: Weak<Inner>) {
    let mut wait = match inner.upgrade() {
        Some(i) => i.cfg.gc_interval.min(FIRST_SWEEP_AFTER),
        None => return,
    };
    let mut again: Option<crate::pace::Backoff> = None;
    loop {
        crate::pace::beat(wait).await;
        let Some(inner) = inner.upgrade() else { return };
        let more = match sweep_dead_logs(&inner).await {
            Ok(more) => {
                super::stats::note_sweep(if more {
                    super::stats::SweepOutcome::Again
                } else {
                    super::stats::SweepOutcome::Done
                });
                more
            }
            Err(e) => {
                super::stats::note_sweep(super::stats::SweepOutcome::Failed);
                tracing::debug!(error = %e, "sweeping dead shared WAL logs");
                true
            }
        };
        let gc = inner.cfg.gc_interval;
        wait = if more {
            again
                .get_or_insert_with(|| crate::pace::Backoff::new(SWEEP_AGAIN_FIRST.min(gc), gc))
                .next_pause()
        } else {
            again = None;
            gc * SWEEP_EVERY_GC
        };
    }
}

/// The hint round: logs that a range acquisition on this node has just dropped from the
/// owner record (`acquire_shared_with_hint`) — they may have just stopped being listed. It
/// only cuts the latency of the purge; the full sweep still catches everything this round
/// misses.
pub(super) async fn hint_loop(inner: Weak<Inner>, mut rx: mpsc::UnboundedReceiver<Vec<LogId>>) {
    while let Some(first) = rx.recv().await {
        let gather = match inner.upgrade() {
            Some(i) => i.cfg.gc_interval.min(HINT_GATHER),
            None => return,
        };
        crate::pace::beat(gather).await;
        let mut logs: std::collections::BTreeSet<LogId> = first.into_iter().collect();
        while let Ok(more) = rx.try_recv() {
            logs.extend(more);
        }
        let Some(inner) = inner.upgrade() else { return };
        if let Err(e) = forget_hinted(&inner, logs).await {
            tracing::debug!(error = %e, "purging shared WAL logs after a range acquisition");
        }
    }
}

/// A log as the LIST sees it: numbered objects, whether it has the `forgotten` mark, and
/// whether it has any other object (floors, index, unknown names).
#[derive(Debug, Default)]
pub(super) struct Listing {
    seqs: std::collections::BTreeSet<u64>,
    forgotten: bool,
    other: bool,
}

impl Listing {
    fn note(&mut self, name: &str) {
        if name == log::FORGOTTEN {
            self.forgotten = true;
        } else if let Ok(seq) = name.parse::<u64>() {
            self.seqs.insert(seq);
        } else {
            self.other = true;
        }
    }

    /// Work left: not purged, or half purged — objects remain besides the mark and a seal.
    fn pending(&self) -> bool {
        !self.forgotten || self.other || self.seqs.len() > 1
    }
}

/// One full sweep round. Returns `true` when the next tick must keep sweeping.
///
/// Candidates come from **one recursive LIST** of `nodewal/`, not by walking directories:
/// a store that keeps empty directories (SeaweedFS) lists logs that are long gone forever,
/// and sealing a log that has no objects left recreates it (seen in a soak run).
pub(super) async fn sweep_dead_logs(inner: &Arc<Inner>) -> Result<bool> {
    let root = log::nodewal_root(&inner.wal_prefix);
    let mut found: BTreeMap<LogId, Listing> = BTreeMap::new();
    {
        use futures::TryStreamExt;
        let mut listed = inner.wal_store.list(Some(&Path::from(root.as_str())));
        while let Some(m) = listed
            .try_next()
            .await
            .map_err(|e| StoreError::Io(e.to_string()))?
        {
            let Some(rest) = m
                .location
                .as_ref()
                .strip_prefix(root.as_str())
                .and_then(|r| r.strip_prefix('/'))
            else {
                continue;
            };
            let mut parts = rest.splitn(4, '/');
            let (Some(node), Some(inc), Some(stripe), Some(name)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let (Ok(inc), Ok(stripe)) = (inc.parse::<u64>(), stripe.parse::<u16>()) else {
                continue;
            };
            let log = LogId {
                session: SessionId {
                    node: node.to_string(),
                    inc,
                },
                stripe,
            };
            // Our own logs: only rotated streams. The current stream's log has its own
            // floor GC (`gc_loop`); sealing it would be suicide.
            if log.session == inner.session && !inner.is_retired(&log) {
                continue;
            }
            found.entry(log).or_default().note(name);
        }
    }
    let candidates: Vec<(LogId, Listing)> =
        found.into_iter().filter(|(_, l)| l.pending()).collect();
    forget_unlisted(inner, candidates, Some(SWEEP_BATCH)).await
}

/// The hint round on exactly the logs `logs`: one LIST per log (its directory).
async fn forget_hinted(
    inner: &Arc<Inner>,
    logs: std::collections::BTreeSet<LogId>,
) -> Result<bool> {
    use futures::{StreamExt, TryStreamExt};
    let listed: Vec<(LogId, Listing)> = futures::stream::iter(logs)
        .map(|log| {
            let store = Arc::clone(&inner.wal_store);
            let prefix = inner.wal_prefix.clone();
            list_log(store, prefix, log)
        })
        .buffered(GC_DELETES_IN_FLIGHT)
        .try_filter_map(|x| std::future::ready(Ok(x)))
        .try_collect()
        .await?;
    let candidates = listed.into_iter().filter(|(_, l)| l.pending()).collect();
    forget_unlisted(inner, candidates, None).await
}

/// One LIST of `log`'s directory; `None` = no objects left (nothing to purge, and sealing
/// it would recreate it).
async fn list_log(
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    log: LogId,
) -> Result<Option<(LogId, Listing)>> {
    use futures::TryStreamExt;
    let ls = LogStore::new(Arc::clone(&store), &prefix, &log);
    let dir = format!("{}/", ls.dir());
    let mut listing = Listing::default();
    let mut any = false;
    let mut objects = store.list(Some(&Path::from(ls.dir())));
    while let Some(m) = objects
        .try_next()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?
    {
        let Some(name) = m.location.as_ref().strip_prefix(dir.as_str()) else {
            continue;
        };
        any = true;
        listing.note(name);
    }
    Ok(any.then_some((log, listing)))
}

/// The grounds for "nobody writes this log any more" of a candidate, read **before** any
/// owner-record snapshot.
#[derive(Clone, Debug)]
enum Ended {
    /// The log's incarnation is dead ([`crate::proof::DeleteProof::dead_log`]).
    Death(crate::proof::SessionDeath),
    /// The log of a rotated stream of this very incarnation
    /// ([`crate::proof::DeleteProof::rotated_log`]).
    Retired(crate::proof::StreamRetired),
}

/// One log a round tried to purge: the log, its seals, what ended it, and the outcome
/// (`None` = no proof, kept).
type Purged = (LogId, Vec<u64>, &'static str, Option<Result<usize>>);

/// The core shared by both rounds. For each candidate whose incarnation **is dead** — or
/// that is the log of a **rotated** stream of this very incarnation (the stream stopped
/// acking before asking to rotate, so "rotated" can stand in for death; before this, the
/// log stayed whole until the node died — in a 4 h soak, 728 objects stood for 28
/// minutes):
///
/// 1. the first owner-record snapshot **filters**: a log still listed waits — not sealed,
///    nothing of it is read;
/// 2. the remaining logs are sealed (a log not yet purged: [`NodeWal::sealed`]) or have
///    their seals read from the mark (a half-purged log: [`LogStore::forgotten_seals`] — not
///    sealed again);
/// 3. the second snapshot, taken **after** the seal is on the bucket, is the proof
///    ([`crate::proof::DeleteProof::dead_log`]: death is read before every snapshot); a log
///    it does not list is purged.
///
/// Why the snapshot after the seal is needed (M3 `snapshot_after_seal`): every frame a
/// zombie can ack lies below the seal, and the segment pointing at its log is CASed before
/// its first frame (C4), hence before the seal, hence before the snapshot — a snapshot that
/// sees no segment means every such frame is already in L0 (C7). A snapshot **before** the
/// seal does not cover a segment the zombie CASes after it (broken clock). Earlier, the
/// purge required the GC floor to have reached the seal, and that condition implicitly
/// covered this point.
///
/// Logs of a live incarnation are never touched — except our own rotated logs: sealing a
/// live log kills that node (NW-8). Returns `true` when there is work it can do right away
/// (more than `batch`, or a failed read/write).
async fn forget_unlisted(
    inner: &Arc<Inner>,
    mut candidates: Vec<(LogId, Listing)>,
    batch: Option<usize>,
) -> Result<bool> {
    if candidates.is_empty() {
        return Ok(false);
    }
    // Start at a random position: no fixed leading slice (in name order) gets to eat the
    // whole batch of every round.
    if candidates.len() > 1 {
        let mut b = [0u8; 8];
        crate::entropy::fill(&mut b);
        let at = (u64::from_le_bytes(b) % candidates.len() as u64) as usize;
        candidates.rotate_left(at);
    }
    let mut sessions: std::collections::BTreeSet<SessionId> = Default::default();
    for (log, _) in &candidates {
        if log.session != inner.session {
            sessions.insert(log.session.clone());
        }
    }
    // Ask the session of every candidate (in parallel) **before** any snapshot of the set
    // of still-referenced logs: a snapshot after the death covers every segment that
    // incarnation added before it died (the `dead_log` gate).
    let deaths: BTreeMap<SessionId, Result<Option<crate::proof::SessionDeath>>> = {
        use futures::StreamExt;
        futures::stream::iter(sessions)
            .map(|session| {
                let store = Arc::clone(inner.stores.main().store());
                let prefix = inner.stores.main().prefix().clone();
                async move {
                    let death = crate::retry::run(
                        &crate::retry::ops::SWEEP_SESSION_READ,
                        &SESSION_READ_RETRY,
                        crate::retry::Never,
                        || crate::session::read_session(&store, &prefix, &session),
                    )
                    .await
                    .map(|read| {
                        crate::proof::SessionDeath::observed(
                            &session,
                            read.as_ref().map(|(r, _)| r.as_ref().map_err(|_| ())),
                        )
                    });
                    (session, death)
                }
            })
            .buffered(GC_DELETES_IN_FLIGHT)
            .collect()
            .await
    };
    let mut failed = false;
    let mut dead = Vec::new();
    for (log, listing) in candidates {
        if log.session == inner.session {
            // Our own log is only a candidate once its stream has rotated.
            if inner.is_retired(&log) {
                let retired = crate::proof::StreamRetired::observed(&log);
                dead.push((log, listing, Ended::Retired(retired)));
            }
            continue;
        }
        match &deaths[&log.session] {
            Ok(Some(death)) => dead.push((log, listing, Ended::Death(death.clone()))),
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(log = %log, error = %e, "purging shared WAL log: reading the session failed");
                failed = true;
            }
        }
    }
    if dead.is_empty() {
        return Ok(failed);
    }
    // (1) Filter.
    let filter = match inner.referenced_logs().await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "purging shared WAL logs: reading owner records failed, keeping every log this round");
            return Ok(true);
        }
    };
    // The in-RAM seal copies only spare range takers a re-read: a log nobody lists any more
    // is not taken over from by anyone.
    inner.evict_sealed(|log| filter.lists(log)).await;
    let waiting = dead.iter().filter(|(log, ..)| filter.lists(log)).count();
    let mut unlisted: Vec<_> = dead
        .into_iter()
        .filter(|(log, ..)| !filter.lists(log))
        .collect();
    let more = batch.is_some_and(|b| unlisted.len() > b);
    if let Some(b) = batch {
        unlisted.truncate(b);
    }
    // (2) Seals on the bucket — in parallel: with a process life shorter than one
    // sequential round, (3) would never be reached.
    let sealed: Vec<_> = {
        use futures::StreamExt;
        futures::stream::iter(unlisted)
            .map(|(log, listing, death)| {
                let inner = Arc::clone(inner);
                async move {
                    let store =
                        LogStore::new(Arc::clone(&inner.wal_store), &inner.wal_prefix, &log);
                    let seals = if listing.forgotten {
                        store.forgotten_seals(&listing.seqs).await
                    } else {
                        NodeWal(inner).sealed(&log).await.map(|s| vec![s.seal_at])
                    };
                    let seen = crate::proof::SealSeen::observed(&log);
                    (log, store, seals, death, seen)
                }
            })
            .buffer_unordered(GC_DELETES_IN_FLIGHT)
            .collect()
            .await
    };
    let mut ready = Vec::new();
    for (log, store, seals, death, seen) in sealed {
        match seals {
            Ok(seals) => ready.push((log, store, seals, death, seen)),
            Err(e) => {
                tracing::debug!(log = %log, error = %e, "purging shared WAL log: sealing / reading the seal failed, keeping");
                failed = true;
            }
        }
    }
    if ready.is_empty() {
        return Ok(more || failed);
    }
    // (3) Proof: the snapshot after the seal.
    let referenced = match inner.referenced_logs().await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "purging shared WAL logs: reading owner records failed, keeping every log this round");
            return Ok(true);
        }
    };
    // Each log is proven and purged on its own (the proof is per log); a few at once.
    let purged: Vec<Purged> = {
        use futures::StreamExt;
        let referenced = &referenced;
        futures::stream::iter(ready)
            .map(|(log, store, seals, ended, seen)| async move {
                let (proof, what) = match &ended {
                    Ended::Death(death) => (
                        crate::proof::DeleteProof::dead_log(&log, death, &seen, referenced),
                        "of a dead incarnation",
                    ),
                    Ended::Retired(retired) => (
                        crate::proof::DeleteProof::rotated_log(&log, retired, &seen, referenced),
                        "of a rotated stream",
                    ),
                };
                let outcome = match proof {
                    Ok(proof) => Some(store.forget(&seals, proof).await),
                    Err(_) => None,
                };
                (log, seals, what, outcome)
            })
            .buffer_unordered(FORGET_LOGS_IN_FLIGHT)
            .collect()
            .await
    };
    let mut forgotten = 0;
    for (log, seals, what, outcome) in purged {
        match outcome {
            None => continue,
            Some(Ok(n)) => {
                forgotten += 1;
                super::stats::note_purged();
                tracing::info!(log = %log, objects = n, seal = ?seals, "purged shared WAL log {what} (seal stays)");
            }
            Some(Err(e)) => {
                tracing::debug!(log = %log, error = %e, "purging a dead shared WAL log");
                failed = true;
            }
        }
        inner.evict_sealed(|l| *l != log).await;
    }
    tracing::debug!(waiting, forgotten, "shared WAL log purge round");
    Ok(more || failed)
}
