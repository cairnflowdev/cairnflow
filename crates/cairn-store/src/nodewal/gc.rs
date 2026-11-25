// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Combined log GC by durable floor — **only for the incarnation's own live logs**:
//! references (`refs`), floors (`LogFloor`, `floor_reports`), `gc_loop`, the index. Dead
//! logs and logs of rotated streams have no floor GC: they stay whole until they are purged
//! (`sweep.rs` — the same holds for the incarnation's own rotated logs while it is still
//! alive).

use super::*;
use std::collections::BTreeSet;

/// A floor stuck at a pinned object for longer than this logs a `warn`, repeated every
/// such interval while it stays stuck.
pub const FLOOR_WARN_AFTER: Duration = Duration::from_secs(600);

/// Every incarnation built in the process — so `/metrics` can read the floors without a
/// path from ingress to [`NodeWals`]. Dropped ones are pruned on read.
pub(super) static LIVE: Mutex<Vec<Weak<Inner>>> = Mutex::new(Vec::new());

/// Floor report of every incarnation **still running** in the process (for `/metrics`),
/// filtered by node when `node` is given — the test harness runs several nodes in one
/// process, and each node may only expose its own floors.
pub fn floor_reports(node: Option<&str>) -> Vec<FloorReport> {
    let live: Vec<Arc<Inner>> = {
        let mut all = LIVE.lock().expect("mutex");
        all.retain(|w| w.strong_count() > 0);
        all.iter().filter_map(Weak::upgrade).collect()
    };
    live.into_iter()
        .filter(|i| node.is_none_or(|n| i.session.node == n))
        .filter(|i| i.stripes().iter().any(|s| !s.is_dead()))
        .map(|i| i.floor_report())
        .collect()
}

/// The floor of one log as the GC loop sees it, with the time it started being pinned.
pub(super) struct FloorState {
    floor: u64,
    pub(super) pinned_by: Option<(u32, u64)>,
    /// When the floor started standing at `floor` **while pinned**: reset whenever the floor
    /// advances or stops being pinned — a log idle for a long time that only now has an
    /// object is not counted as pinned since it went idle.
    since: tokio::time::Instant,
    /// The latest `warn` for this pinning episode.
    warned: Option<tokio::time::Instant>,
}

/// The part of a range's WAL references that the combined GC needs.
pub(crate) type Refs = Vec<WalFileRange>;

/// The GC floor of one log, as the combined GC loop saw it on its latest round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogFloor {
    pub log: LogId,
    /// Lowest data object still on the bucket.
    pub floor: u64,
    /// The **first** range whose reference blocks the floor, with the wal id of the held
    /// frame. `None` = no object above the floor — nobody pins the floor.
    pub pinned_by: Option<(u32, u64)>,
    /// How long the floor has stood at the pinned object (0 when not pinned).
    pub pinned_for: Duration,
}

/// The floors of every log an incarnation is cleaning (its own live logs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FloorReport {
    /// The incarnation's node.
    pub node: String,
    pub logs: Vec<LogFloor>,
}

impl FloorReport {
    /// Floor age of the node: the log pinned the longest.
    pub fn age(&self) -> Duration {
        self.logs
            .iter()
            .map(|l| l.pinned_for)
            .max()
            .unwrap_or_default()
    }

    /// Range pinning the floor of at least one log → longest pinning time.
    pub fn pinned(&self) -> BTreeMap<u32, Duration> {
        let mut out = BTreeMap::new();
        for l in &self.logs {
            if let Some((range, _)) = l.pinned_by {
                let e = out.entry(range).or_insert(Duration::ZERO);
                *e = (*e).max(l.pinned_for);
            }
        }
        out
    }
}

pub(crate) fn contains(range: &WalFileRange, id: u64) -> bool {
    let after = match range.0 {
        Bound::Included(s) => id >= s,
        Bound::Excluded(s) => id > s,
        Bound::Unbounded => true,
    };
    let before = match range.1 {
        Bound::Included(e) => id <= e,
        Bound::Excluded(e) => id < e,
        Bound::Unbounded => true,
    };
    after && before
}

/// Result of one [`Inner::gc_log`] round on one log.
pub(super) struct GcRound {
    /// Floor after the round — durable if it differs from the floor at the start.
    pub(super) floor: u64,
    pub(super) pinned_by: Option<(u32, u64)>,
    /// Objects below `floor` whose delete failed: the next round retries just those.
    pub(super) undeleted: Vec<u64>,
}

/// Deletes objects `seqs` — all of them **below the log's durable floor** (NW-7: every
/// frame in them is outside every reference, the floor was written first) — in parallel,
/// bounded. Does not stop at the first failure: returns the seqs whose delete failed, to
/// retry later. A late retry is as safe as the first attempt — the proof is the durable
/// floor, not the time of the delete; a seal never lies below the floor (C2), and a log id
/// is never reused while the incarnation is alive.
pub(super) async fn delete_below_floor(
    store: &LogStore,
    seqs: impl IntoIterator<Item = u64>,
) -> Vec<u64> {
    use futures::StreamExt;
    let mut failed: Vec<u64> = futures::stream::iter(seqs)
        .map(|s| async move { store.delete(s).await.err().map(|e| (s, e)) })
        .buffer_unordered(GC_DELETES_IN_FLIGHT)
        .filter_map(|r| async move { r })
        .map(|(s, e)| {
            tracing::debug!(seq = s, error = %e, "deleting an object below the shared WAL floor failed — retrying next round");
            s
        })
        .collect()
        .await;
    failed.sort_unstable();
    failed
}

/// A range's WAL references by the **stored** manifest — for ranges not open on this node,
/// and for `cairn-ctl wal floors` (offline). See [`Inner::refs`] for the `Included` →
/// `Excluded` bound and why checkpoints reference nothing.
pub(crate) async fn stored_refs(stores: &ObjectStores, range: u32) -> Result<Refs> {
    // A range carrying a tombstone needs no frame: nobody acquires it again (D22), so
    // nobody replays its WAL; children and forks only read SSTs (clone requires a source
    // with no uncovered WAL, F3). Even when its Db **still exists** — a child range still
    // lists it in `external_dbs` — and the manifest (usually a checkpoint taken while the
    // range still had replayed, unflushed frames, which nobody cleans up after retirement)
    // still points at frames in the log: without skipping it, the log's floor would stand
    // until D-d deletes the Db (seen in chaos runs: the log of a dead incarnation stood
    // for >600 s). G-c is a special case: the Db has been deleted, there is no manifest left
    // to read.
    if is_retired(stores, range).await? {
        return Ok(Vec::new());
    }
    // Bounded (`admin_io`): an error or timeout ⇒ `gc_log` skips the round and keeps
    // everything (delete only when provable).
    let referenced = stores.stored_wal_refs(range).await?;
    // Only the current manifest has an `Included` lower bound (the GC contract of fork
    // patch F1: checkpoints use `Excluded`): keep exactly that one, converted to `Excluded` like [`Inner::refs`].
    // If it is not found (should not happen), keep everything SlateDB lists — conservative.
    let current: Refs = referenced
        .iter()
        .filter_map(|r| match r.0 {
            Bound::Included(s) => Some(WalFileRange(Bound::Excluded(s), r.1)),
            _ => None,
        })
        .collect();
    Ok(if current.is_empty() {
        referenced
    } else {
        current
    })
}

/// The range carries a tombstone (`retired=1`). An unreadable owner record is an error —
/// `gc_log` skips the round and keeps everything (delete only when provable).
pub(super) async fn is_retired(stores: &ObjectStores, range: u32) -> Result<bool> {
    let main = stores.main();
    Ok(
        crate::owner::read_owner(main.store(), main.prefix(), crate::ids::PartitionId(range))
            .await?
            .is_some_and(|r| r.retired),
    )
}

impl Inner {
    /// The range's WAL references: `(replay_after_wal_id, ∞)` of the **current** manifest —
    /// read straight from the Db open on this node, otherwise from the stored manifest
    /// ([`stored_refs`]).
    ///
    /// Differs from SlateDB's GC in two places:
    ///
    /// - **The mark** of the current manifest: SlateDB keeps the `replay_after_wal_id` file
    ///   (`Included`) because the native WAL fences a zombie writer by the very name of the
    ///   next WAL file. The shared WAL fences with the log's seal, and `SharedWriterInit`
    ///   picks a first id above every id seen — the frame at the mark is already in L0, so
    ///   there is no need to keep it. Keeping it would make every idle range pin one object
    ///   forever.
    /// - **Checkpoints reference no frame.** SlateDB keeps the
    ///   `(replay_after, next_wal_sst_id)` span of each checkpoint for whoever opens a Db
    ///   from that checkpoint and then replays the WAL. With the shared WAL nobody does
    ///   that: acquiring a range replays from the current manifest; clone requires an empty
    ///   WAL ([`SharedWalAdmin`]); `fork` only takes what is already in L0
    ///   ([`crate::ObjectStores::fork_range`]); followers read `FollowLatest` through the
    ///   tailer and write no checkpoint. As for checkpoints, the compactor writes one living
    ///   15 minutes before each manifest update (to pin SSTs, not the WAL), and the manifest
    ///   of that moment often has a gap — frames in the gap used to pin the log's floor for
    ///   up to 15 minutes (seen in chaos runs, the floor stood for 605 s; NW-7).
    ///
    /// **Open Db: only trust a mark already on the stored manifest.** SlateDB updates the
    /// in-memory manifest's `replay_after_wal_id` (`apply_uploaded_state`) **before** it
    /// writes the manifest to the bucket. Trusting the in-memory mark, a GC tick falling into
    /// that window would delete objects the stored manifest still needs, and a node dying
    /// before the manifest reached the bucket would lose acked rows (delete only when
    /// provable). An in-memory mark not past the confirmed mark is used directly; past it,
    /// the stored manifest is read once (`durable_replay_after`) — once per L0 flush of the
    /// range, not once per GC tick.
    async fn refs(&self, range: u32) -> Result<Refs> {
        let open = self.dbs.lock().expect("mutex").get(&range).cloned();
        if let Some(db) = open {
            let in_memory = db.manifest().replay_after_wal_id();
            let confirmed = self
                .durable_replay_after
                .lock()
                .expect("mutex")
                .get(&range)
                .copied();
            let mark = match confirmed {
                Some(c) if in_memory <= c => in_memory,
                _ => {
                    let stored = self
                        .stores
                        .manifest_head(range)
                        .await?
                        .map_or(0, |h| h.replay_after_wal_id);
                    let mut map = self.durable_replay_after.lock().expect("mutex");
                    let c = map.entry(range).or_insert(stored);
                    *c = (*c).max(stored);
                    in_memory.min(stored)
                }
            };
            return Ok(vec![WalFileRange(Bound::Excluded(mark), Bound::Unbounded)]);
        }
        stored_refs(&self.stores, range).await
    }

    /// One GC round on one log: advance the floor over the contiguous prefix of objects
    /// whose every frame is outside every reference, write the floor **first**, then delete.
    /// Returns the new floor, the **first** range that stopped the floor (with the wal id
    /// of the held frame) — `None` when the floor stopped because the objects ran out — and
    /// the objects below the new floor whose delete failed.
    ///
    /// `Err` only when the floor has **not** advanced (reading references or writing the
    /// floor failed). Once the floor is durable, a failed delete must not lose it: returning
    /// `Err` here would make the caller drop the new floor, the next round would start again
    /// from the old floor and delete the whole batch again — the batch grows with the life
    /// of the incarnation, and with a DELETE failure rate around 0.2 % almost every round
    /// fails from ~2 000 objects on: the GC round stretches to minutes, and the floor on the
    /// bucket stands still between two rounds.
    async fn gc_log(
        &self,
        store: &LogStore,
        floor: u64,
        index: &BTreeMap<u64, Vec<(u32, u64)>>,
        cache: &mut HashMap<u32, Refs>,
    ) -> Result<GcRound> {
        let mut at = floor;
        let mut pinned_by = None;
        while let Some(frames) = index.get(&at) {
            for (range, id) in frames {
                if !cache.contains_key(range) {
                    cache.insert(*range, self.refs(*range).await?);
                }
                if cache[range].iter().any(|r| contains(r, *id)) {
                    pinned_by = Some((*range, *id));
                    break;
                }
            }
            if pinned_by.is_some() {
                break;
            }
            at += 1;
        }
        let mut undeleted = Vec::new();
        if at > floor {
            store.write_floor(at).await?;
            // The floor is durable: the delete order no longer matters. If a seal lands on a
            // name just deleted, the sealer sees the floor past it (C2), as with a sequential delete.
            undeleted = delete_below_floor(store, floor..at).await;
            // Old floor marks: the next floor advance deletes every mark below it again.
            if let Err(e) = store.delete_floors_below(at).await {
                tracing::debug!(error = %e, "deleting old floor marks of the shared WAL log");
            }
        }
        Ok(GcRound {
            floor: at,
            pinned_by,
            undeleted,
        })
    }

    /// Records the floor `floor` of `log` after a GC round; a floor standing at an object
    /// pinned for longer than [`FLOOR_WARN_AFTER`] logs a `warn` naming the pinning range.
    fn note_floor(&self, log: &LogId, floor: u64, pinned_by: Option<(u32, u64)>) {
        let now = tokio::time::Instant::now();
        let mut map = self.floors.lock().expect("mutex");
        let e = map.entry(log.clone()).or_insert(FloorState {
            floor,
            pinned_by: None,
            since: now,
            warned: None,
        });
        if e.floor != floor || pinned_by.is_none() || e.pinned_by.is_none() {
            e.since = now;
            e.warned = None;
        }
        e.floor = floor;
        e.pinned_by = pinned_by;
        let Some((range, wal_id)) = pinned_by else {
            return;
        };
        let age = now.duration_since(e.since);
        if age >= FLOOR_WARN_AFTER
            && e.warned
                .is_none_or(|w| now.duration_since(w) >= FLOOR_WARN_AFTER)
        {
            e.warned = Some(now);
            tracing::warn!(
                log = %log,
                floor = floor,
                range,
                wal_id,
                stuck_s = age.as_secs(),
                "shared WAL floor stuck — range {range} has not flushed frame {wal_id} to L0"
            );
        }
    }

    fn retain_floors(&self, keep: impl Fn(&LogId) -> bool) {
        self.floors
            .lock()
            .expect("mutex")
            .retain(|log, _| keep(log));
    }

    pub(super) fn floor_report(&self) -> FloorReport {
        let now = tokio::time::Instant::now();
        let map = self.floors.lock().expect("mutex");
        let mut logs: Vec<LogFloor> = map
            .iter()
            .map(|(log, st)| LogFloor {
                log: log.clone(),
                floor: st.floor,
                pinned_by: st.pinned_by,
                pinned_for: if st.pinned_by.is_some() {
                    now.duration_since(st.since)
                } else {
                    Duration::ZERO
                },
            })
            .collect();
        logs.sort_by_key(|l| l.log.to_string());
        FloorReport {
            node: self.session.node.clone(),
            logs,
        }
    }
}

/// The GC floor seen for each log, between ticks of [`gc_loop`].
pub(super) type Floors = HashMap<LogId, u64>;

pub(super) async fn gc_loop(inner: Weak<Inner>) {
    let mut floors: Floors = HashMap::default();
    // Objects below the durable floor whose delete failed, by log: each round retries just
    // those (instead of the whole batch from the old floor).
    let mut undeleted: HashMap<LogId, BTreeSet<u64>> = HashMap::default();
    loop {
        let every = match inner.upgrade() {
            Some(i) => i.cfg.gc_interval,
            None => return,
        };
        crate::pace::beat(every).await;
        let Some(inner) = inner.upgrade() else { return };
        let mut cache = HashMap::default();
        let stripes = inner.stripes();
        // A rotated stream has left its slot: the old log no longer has floor GC, so its
        // floor is no longer reported either — reporting it would make it look like a floor
        // pinned forever.
        floors.retain(|log, _| stripes.iter().any(|s| s.log == *log));
        // Logs that left the loop (rotated streams): their leftovers below the floor go with
        // the purge of that log (`LogStore::forget` removes every data object).
        undeleted.retain(|log, _| stripes.iter().any(|s| s.log == *log && !s.is_dead()));
        for s in &stripes {
            let Some(left) = undeleted.get_mut(&s.log) else {
                continue;
            };
            let failed = delete_below_floor(&s.store, std::mem::take(left)).await;
            left.extend(failed);
        }
        undeleted.retain(|_, left| !left.is_empty());
        inner.retain_floors(|log| stripes.iter().any(|s| s.log == *log && !s.is_dead()));
        // Our own logs: the index of the acked objects.
        for s in &stripes {
            if s.is_dead() {
                continue;
            }
            let floor = *floors.get(&s.log).unwrap_or(&0);
            let index = s.index_from(floor);
            let Some(&first) = index.keys().next() else {
                inner.note_floor(&s.log, floor, None);
                continue;
            };
            let floor = match inner
                .gc_log(&s.store, first.max(floor), &index, &mut cache)
                .await
            {
                Ok(round) => {
                    s.forget_below(round.floor);
                    floors.insert(s.log.clone(), round.floor);
                    inner.note_floor(&s.log, round.floor, round.pinned_by);
                    if !round.undeleted.is_empty() {
                        undeleted
                            .entry(s.log.clone())
                            .or_default()
                            .extend(round.undeleted);
                    }
                    round.floor
                }
                Err(e) => {
                    tracing::debug!(log = %s.log, error = %e, "shared WAL log GC");
                    first.max(floor)
                }
            };
            write_index(&inner, s, &index, floor).await;
        }
    }
}

/// Writes the index of our log when there are new acked objects since the last write:
/// objects `[floor, last]`, each with the frames it carries. Whoever seals this log after
/// we die reads one GET instead of the whole indexed part, and whoever acquires a range only
/// GETs the objects with frames of that range.
///
/// `index` only holds **acked** objects (`Stripe::ack`), and acks are in order: every seq
/// in it has a data object, and the seal (if someone seals us) lies above it. Written on
/// each GC tick **that has something to write** and every [`INDEX_EVERY_OBJECTS`] new
/// objects ([`index_loop`]); an idle stream writes nothing.
async fn write_index(
    inner: &Inner,
    s: &Stripe,
    index: &BTreeMap<u64, Vec<(u32, u64)>>,
    floor: u64,
) {
    let live: BTreeMap<u64, Vec<(u32, u64)>> =
        index.range(floor..).map(|(k, v)| (*k, v.clone())).collect();
    let Some(&upto) = live.keys().next_back() else {
        return;
    };
    if inner
        .indexed
        .lock()
        .expect("mutex")
        .get(&s.log)
        .is_some_and(|&u| u >= upto)
    {
        return;
    }
    let Some(bytes) = format::encode_index(&live) else {
        return;
    };
    match s.store.put_index(bytes).await {
        Ok(()) => {
            let mut indexed = inner.indexed.lock().expect("mutex");
            let e = indexed.entry(s.log.clone()).or_insert(upto);
            *e = (*e).max(upto);
        }
        Err(e) => tracing::debug!(log = %s.log, error = %e, "writing the shared WAL log index"),
    }
}

/// The writer writes the log's index once this many new objects have been acked since the
/// last write, without waiting for the GC tick: an incarnation dying before the first GC
/// tick (30 s) still leaves an index, and the tail a sealer must read in full is no more
/// than this plus one [`INDEX_POLL`] tick. Index PUTs are at most 1/64 of the stream's
/// data PUTs.
pub(super) const INDEX_EVERY_OBJECTS: i64 = 64;

/// Tick of the index loop checking the number of new objects — one short lock of each
/// stream's `State`, no I/O if not enough.
pub(super) const INDEX_POLL: Duration = Duration::from_secs(1);

/// The loop writing the index by object count (see [`INDEX_EVERY_OBJECTS`]); the GC tick
/// still writes the remainder.
pub(super) async fn index_loop(inner: Weak<Inner>) {
    let mut every = tokio::time::interval(INDEX_POLL);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    every.tick().await;
    loop {
        every.tick().await;
        let Some(inner) = inner.upgrade() else { return };
        for s in &inner.stripes() {
            if s.is_dead() {
                continue;
            }
            let tip = s.acked_tip();
            let last = inner
                .indexed
                .lock()
                .expect("mutex")
                .get(&s.log)
                .map_or(-1, |&u| u as i64);
            if tip - last < INDEX_EVERY_OBJECTS {
                continue;
            }
            // The stream's in-memory index has already been cut at the previous GC floor.
            let index = s.index_from(0);
            let floor = index.keys().next().copied().unwrap_or(0);
            write_index(&inner, s, &index, floor).await;
        }
    }
}

impl Inner {
    /// Every log that an owner record — tombstones included — still lists in `wal_segments`
    /// ([`crate::owner::referenced_logs`]). An unreadable record is an error — the caller
    /// keeps everything (delete only when provable).
    pub(super) async fn referenced_logs(&self) -> Result<crate::proof::ReferencedLogs> {
        crate::proof::ReferencedLogs::read(&self.stores, GC_DELETES_IN_FLIGHT).await
    }
}
