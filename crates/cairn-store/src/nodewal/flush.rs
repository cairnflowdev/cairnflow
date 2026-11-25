// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Forced flush by frame age: `age_flush_loop`, `flush_through`.

use super::*;

/// Number of forced flushes by frame age in flight at once on one node.
pub const MAX_FORCED_FLUSHES: usize = 4;

/// Bound of one forced flush by frame age: SlateDB's memtable flush retries forever, and a
/// hung flush would hold its place in [`MAX_FORCED_FLUSHES`] forever. Past the bound ⇒ give
/// this one up; the next tick picks the range again if its frame is still too old. Same
/// size as the Db open bound: a full memtable to S3 Standard takes a few seconds.
pub const FORCED_FLUSH_BOUND: Duration = crate::config::DEFAULT_OPEN_DB_BOUND;

/// Picks the ranges to force-flush on this tick (inspired by TiKV's purge scheduling in
/// raftstore-v2): skips ranges with a flush in flight (`busy`) and ranges whose flush
/// finished on the previous tick (`recent`), puts ranges **pinning the floor** (`pinning`)
/// first, then by age of the oldest unflushed frame, descending, and takes at most `room`.
pub(super) fn pick_forced_flushes(
    mut stale: Vec<(u32, Duration)>,
    busy: &HashSet<u32>,
    recent: &HashSet<u32>,
    pinning: &HashSet<u32>,
    room: usize,
) -> Vec<u32> {
    stale.retain(|(r, _)| !busy.contains(r) && !recent.contains(r));
    stale.sort_by_key(|(r, age)| (!pinning.contains(r), std::cmp::Reverse(*age), *r));
    stale.into_iter().take(room).map(|(r, _)| r).collect()
}

/// Forced flush by frame age (D-j) for **idle** ranges: SlateDB only asks
/// `should_flush_memtable` on a write, so a range that no longer writes needs this nudge.
///
/// **Paced**: at most [`MAX_FORCED_FLUSHES`] flushes in flight at once, ranges pinning the
/// floor longest first, never two overlapping flushes of one range, and ranges flushed on
/// the previous tick are skipped. The earlier version did a `tokio::spawn` of one flush for
/// **every** over-age range on each tick — a node with a few hundred idle ranges meant a
/// few hundred SST uploads at once, and while a flush was still stuck the next tick spawned
/// more.
pub(super) async fn age_flush_loop(inner: Weak<Inner>) {
    // Flushes in flight; cancelling the loop (the task is aborted when the incarnation
    // ends) cancels them too.
    let mut flights: tokio::task::JoinSet<u32> = tokio::task::JoinSet::new();
    let mut busy: HashSet<u32> = HashSet::default();
    // Range that finished flushing → when it finished.
    let mut done: HashMap<u32, tokio::time::Instant> = HashMap::default();
    loop {
        let every = match inner.upgrade() {
            Some(i) => (i.cfg.frame_age_flush / 4).max(Duration::from_millis(100)),
            None => return,
        };
        crate::pace::beat(every).await;
        let Some(inner) = inner.upgrade() else { return };
        while let Some(r) = flights.try_join_next() {
            if let Ok(range) = r {
                busy.remove(&range);
                done.insert(range, tokio::time::Instant::now());
            }
        }
        // A `JoinError` (task panic) does not return the range id: clean `busy` up by the
        // number of tasks still alive instead of keeping it forever.
        if flights.is_empty() {
            busy.clear();
        }
        done.retain(|_, t| t.elapsed() < every);
        let recent: HashSet<u32> = done.keys().copied().collect();
        let age = inner.cfg.frame_age_flush;
        let dbs: HashMap<u32, Db> = inner.dbs.lock().expect("mutex").clone();
        let mut stale = Vec::new();
        // Range → wal id of its oldest unflushed frame: the forced flush must cover it.
        let mut through: HashMap<u32, u64> = HashMap::default();
        let mut stripe_of: HashMap<u32, Arc<stripe::Stripe>> = HashMap::default();
        for s in &inner.stripes() {
            for (range, _, _) in s.stale_ranges(age) {
                if let Some(db) = dbs.get(&range) {
                    s.forget_flushed(range, db.manifest().replay_after_wal_id());
                }
            }
            for (range, oldest, id) in s.stale_ranges(age) {
                if dbs.contains_key(&range) {
                    stale.push((range, oldest));
                    through.insert(range, id);
                    stripe_of.insert(range, Arc::clone(s));
                }
            }
        }
        let pinning: HashSet<u32> = inner
            .floors
            .lock()
            .expect("mutex")
            .values()
            .filter_map(|f| f.pinned_by.map(|(r, _)| r))
            .collect();
        let room = MAX_FORCED_FLUSHES.saturating_sub(flights.len());
        for range in pick_forced_flushes(stale, &busy, &recent, &pinning, room) {
            let (Some(db), Some(&id)) = (dbs.get(&range).cloned(), through.get(&range)) else {
                continue;
            };
            busy.insert(range);
            let stripe = stripe_of.get(&range).cloned();
            flights.spawn(async move {
                // Cover up to the over-age frame, not just "flush": if its rows are already
                // in L0 (the gap of the previous flush) the memtable is empty and a flush is
                // a no-op.
                let hold = stripe.as_ref().map(|s| (s, range));
                if let Err(e) = flush_through(&db, id, FORCED_FLUSH_BOUND, hold).await {
                    tracing::debug!(range, error = %e, "forced flush by frame age");
                }
                range
            });
        }
    }
}

/// Flushes the memtable so that the manifest's `replay_after_wal_id` is **≥ `through`**.
///
/// A plain memtable flush is not enough: SlateDB freezes the memtable at
/// `last_flushed_wal_id` **as of the freeze**, so the rows of a frame not yet durable at
/// that moment still go into the L0 SST while the mark stops below the frame (the gap). The
/// next flush, if the memtable is empty — an idle range, or every replayed row already in
/// L0 and so filtered by `last_l0_seq` — is a no-op, and the frame in the gap pins the
/// log's floor until the range writes again (seen in chaos runs: a range changing leader
/// faster than the forced-flush period never flushed). In that case write a **fence** — a
/// delete of the key [`crate::keys::seal_barrier`], outside every range, like
/// `PartitionWriter::seal` — wait for it to be durable and flush again: the memtable has a
/// row, and the mark reaches the fence's frame.
///
/// The fence is written while the range is held back from freezing by frame age (`hold`,
/// the stream the range writes to): if the write met an over-age frame — the very frame in
/// the gap — and froze at once, the mark would stop **before** the fence, the fence would
/// fall into the gap, and the next forced-flush tick would chase it with a new fence,
/// forever (one log PUT, one L0 SST and one manifest write per tick for an idle range; the
/// full check suite caught it).
///
/// Bounded by `bound`: past the bound ⇒ `Io`.
pub(crate) async fn flush_through(
    db: &Db,
    through: u64,
    bound: Duration,
    hold: Option<(&Arc<stripe::Stripe>, u32)>,
) -> Result<()> {
    crate::retry::deadline(
        &crate::retry::ops::FLUSH_THROUGH,
        bound,
        flush_through_unbounded(db, through, hold),
    )
    .await?
}

/// Body of [`flush_through`], **unbounded** — every caller wraps it in `retry::deadline`
/// (`scripts/lint-unbounded.allow`).
pub(super) async fn flush_through_unbounded(
    db: &Db,
    through: u64,
    hold: Option<(&Arc<stripe::Stripe>, u32)>,
) -> Result<()> {
    let memtable = FlushOptions {
        flush_type: FlushType::MemTable,
    };
    db.flush_with_options(memtable.clone())
        .await
        .map_err(StoreError::from_slate)?;
    if db.manifest().replay_after_wal_id() >= through {
        return Ok(());
    }
    let _hold = hold.map(|(s, range)| s.hold_age_freeze(range));
    db.delete_with_options(
        crate::keyspace::seal_barrier(),
        &slatedb::config::WriteOptions::default(),
    )
    .await
    .map_err(StoreError::from_slate)?
    .await_durable()
    .await
    .map_err(StoreError::from_slate)?;
    db.flush_with_options(memtable)
        .await
        .map_err(StoreError::from_slate)
}
