// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Draining signals and inbound, delivering completions from inbound.

use {
    super::*,
    crate::{
        drainer::{Park, RowLedger},
        metrics::interrange::drain as drain_metrics,
    },
};

/// Deliver a signal to an invocation in this partition.
///
/// The target has stopped for good or does not exist ⇒ the signal is a no-op and the source
/// row is cleaned up: delivering a notification to a terminal invocation is rerunning it —
/// the same rule as for the timers of a cancelled invocation. A duplicate (the machine has
/// already seen the identity) is skipped by the machine itself, and the source row is still
/// cleaned up in the same turn.
pub(super) async fn deliver_signal_and_drive<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    ds: cairn_engine::DeliverSignal,
    source: Source,
    duty: &mut WakeDuty,
) -> Result<()> {
    let mut guard = Some(ctx.locks.invocation(engine, ds.target).await);
    // A human's signal: the audit row goes with the signal delivery batch.
    let audit = ctx.audit_begin_signal(engine, ds.target).await;
    let target_gone = matches!(
        engine.status(&ds.target).await?,
        None | Some(InvocationStatus::Completed(_)) | Some(InvocationStatus::Failed { .. })
    );
    if target_gone {
        super::audit_end(engine, ctx, audit, &ds.target, "noop").await?;
        match source {
            Source::SignalOutbox { key } => engine.drop_signal(key).await?,
            Source::Inbound { key } => engine.drop_inbound(key).await?,
            _ => {}
        }
        duty.ack();
        return Ok(());
    }
    drive(
        engine,
        ctx,
        ds.target,
        Some((Wake::Signal(cairn_engine::signal_notification(&ds)), source)),
        &mut guard,
        lc::Turn::PLAIN,
        duty,
    )
    .await?;
    // The delivery batch did not run (the turn was throttled, the SDK failed before getting
    // there): the signal is not in the journal — write nothing; if it was delivered, the row
    // went with that batch.
    drop(audit);
    Ok(())
}

/// Write a signal into the target invocation **in place**, then hand it to the scheduler —
/// the drainer's path, where no SDK turn may run.
///
/// The target's lock is already held (`try_invocation`). If the target has stopped for good
/// the signal has nobody left to receive it: drop the source row — the same rule as for the
/// timers of a cancelled invocation. A duplicate (the machine has already seen the identity)
/// is skipped by the machine itself, and the source row is still cleaned up in the same
/// commit.
pub(super) async fn settle_signal_local<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    guard: InvGuard<'c>,
    ds: cairn_engine::DeliverSignal,
    source: Source,
) -> Result<()> {
    // A target in a different tenant from the sender ⇒ treated like a missing target: drop
    // the source row, do not log the id (it would leak across tenants). Checked before the
    // core: the core does not know tenants.
    let visible = match cairn_engine::tenant_on_wire(ds.tenant.as_deref()) {
        Some(t) => engine.visible_to(&t, &ds.target).await?,
        None => false,
    };
    let plan = match visible {
        true => Some(
            arrived(engine, ctx, ds.target, lc::WakeKind::Signal, None)
                .await?
                .plan,
        ),
        false => None,
    };
    let Some(plan) = plan.filter(|p| p.wake != lc::WakeWrite::Discard) else {
        match source {
            Source::SignalOutbox { key } => engine.drop_signal(key).await?,
            Source::Inbound { key } => engine.drop_inbound(key).await?,
            _ => {}
        }
        return Ok(());
    };
    let mut inv = engine.load(ds.target, params_for(&ds.target)).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .deliver_signal(&mut inv, cairn_engine::signal_notification(&ds), source)
        .await?;
    drop(guard);
    if plan.ram.handout.is_some() {
        ctx.runner.queue(ds.target);
    }
    Ok(())
}

/// The drainer's "write, then hand over" (lifecycle core, `Event::CompletionArrived`) for an
/// `id` whose lock the caller **already holds**: stopped for good ⇒ `Discard` (drop the
/// source row — waking a terminal invocation is rerunning it); otherwise ⇒ `Deliver`, and a
/// turn is handed over only when there is no handed-over turn that has not run yet (FL-20 —
/// that turn carries this wake-up too) **and**, for a completion to a `suspended`
/// invocation, the wait tree is satisfied or unknown.
///
/// `completion` = id of the completion being delivered (`None` for a signal).
pub(super) async fn arrived(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    wake: lc::WakeKind,
    completion: Option<u32>,
) -> Result<Arrival> {
    let now = engine.clock().wall_ms();
    let mut asker = life::Asker::new(now, ctx, life::attendance_holding(ctx.runner, &id));
    if let Some(c) = completion {
        asker = asker.arriving(c);
    }
    let plan = asker
        .ask(engine, ctx, id, lc::Event::CompletionArrived { wake })
        .await?;
    // Stay suspended: the core writes the wake-up without changing `suspended` (the tree is
    // not satisfied).
    let stays = asker.view.status == Some(lc::Status::Suspended)
        && matches!(plan.wake, lc::WakeWrite::Deliver(_))
        && plan.status.is_none();
    Ok(Arrival {
        plan,
        stays,
        machine: asker.machine.take(),
    })
}

/// The core's answer to the drainer, with the caller's machine if reading the wait tree
/// loaded it.
pub(super) struct Arrival {
    pub(super) plan: lc::Plan,
    /// Write the wake-up while **keeping** `suspended` (`Engine::deliver_staying_suspended`).
    pub(super) stays: bool,
    machine: Option<std::result::Result<Invocation, cairn_engine::EngineError>>,
}

impl Arrival {
    /// The caller's machine for the delivery: the one reading the wait tree loaded,
    /// otherwise [`replayed`].
    pub(super) async fn machine(
        &mut self,
        engine: &mut Lease,
        id: InvocationId,
    ) -> Result<std::result::Result<Invocation, cairn_engine::EngineError>> {
        match self.machine.take() {
            Some(m) => Ok(m),
            None => replayed(engine, id).await,
        }
    }
}

/// Number of signal rows read per round. A cap on the cost of one scan, not a cap on
/// visibility: the cursor carries on in the next round.
pub(super) const SIGNAL_BATCH: usize = 64;

/// A row of an ordered queue that the drainer reads through the ledger ([`RowLedger`]): the
/// key carries `seq`, and there is a read from a given `seq`.
pub(super) trait QueueRow: Copy {
    const QUEUE: PoisonQueue;
    fn seq(&self) -> u64;
    /// Live rows from `from`, up to `limit` rows, by `seq` — not counted in metrics.
    async fn read_raw(engine: &mut Lease, from: u64, limit: usize) -> Result<Vec<(Self, Bytes)>>;
    /// Like [`Self::read_raw`], counted in `cairnflow_{inbound,signal}_rows_read_total`.
    async fn read(engine: &mut Lease, from: u64, limit: usize) -> Result<Vec<(Self, Bytes)>> {
        let rows = Self::read_raw(engine, from, limit).await?;
        let counter = match Self::QUEUE {
            PoisonQueue::Inbound => &drain_metrics::INBOUND_ROWS_READ,
            PoisonQueue::Signal => &drain_metrics::SIGNAL_ROWS_READ,
        };
        drain_metrics::add(counter, rows.len() as u64);
        Ok(rows)
    }
}

impl QueueRow for cairn_engine::InboundKey {
    const QUEUE: PoisonQueue = PoisonQueue::Inbound;
    fn seq(&self) -> u64 {
        self.seq
    }
    async fn read_raw(engine: &mut Lease, from: u64, limit: usize) -> Result<Vec<(Self, Bytes)>> {
        Ok(engine.inbound_from(from, limit).await?)
    }
}

impl QueueRow for cairn_engine::SignalRef {
    const QUEUE: PoisonQueue = PoisonQueue::Signal;
    fn seq(&self) -> u64 {
        self.seq
    }
    async fn read_raw(engine: &mut Lease, from: u64, limit: usize) -> Result<Vec<(Self, Bytes)>> {
        Ok(engine.signals_from(from, limit).await?)
    }
}

/// Old rows of the ledger to reconsider in a drain turn, by `seq` (same pattern as the
/// outbox's `gather_rows`). Each row is reread from its own `seq`, one row: the read returns
/// **the first live row** from there. Returning a different row (or nothing) means every
/// ledger row in between is gone — they leave the ledger at no extra read, and the row just
/// read is used if it is the next old row.
pub(super) struct Reread<K> {
    again: std::collections::VecDeque<(u64, Park)>,
    /// The latest read: which `seq` was asked from, and the first live row from there.
    probe: Option<(u64, Option<(K, Bytes)>)>,
}

impl<K: QueueRow> Reread<K> {
    pub(super) fn new(again: Vec<(u64, Park)>) -> Self {
        Self {
            again: again.into(),
            probe: None,
        }
    }

    /// Releasing the engine between two chunks ([`between_chunks`]): the latest read does
    /// not span the release — its row may have changed, and "no live row left from `from`"
    /// no longer holds once new rows arrive.
    pub(super) async fn release(&mut self, engine: &mut Lease) {
        self.probe = None;
        between_chunks(engine).await;
    }

    /// The next old row still alive that `skip` does not wave aside, or `None` when done.
    /// Rows skipped by `skip` stay in the ledger, unread.
    pub(super) async fn next(
        &mut self,
        engine: &mut Lease,
        ledger: &mut RowLedger,
        mut skip: impl FnMut(Park) -> bool,
    ) -> Result<Option<(K, Bytes)>> {
        loop {
            let Some(&(seq, park)) = self.again.front() else {
                return Ok(None);
            };
            if skip(park) {
                self.again.pop_front();
                continue;
            }
            match &self.probe {
                Some((from, first)) if *from <= seq => match first {
                    // No live row left from `from` ⇒ this row is gone.
                    None => {
                        ledger.forget(seq);
                        self.again.pop_front();
                    }
                    Some((k, _)) if k.seq() > seq => {
                        ledger.forget(seq);
                        self.again.pop_front();
                    }
                    Some((k, _)) if k.seq() == seq => {
                        let (_, first) = self.probe.take().expect("probe just matched");
                        self.again.pop_front();
                        return Ok(first);
                    }
                    // The first live row is one we skip — ask again from `seq`.
                    Some(_) => self.probe = None,
                },
                _ => {
                    let mut got = K::read(engine, seq, 1).await?;
                    self.probe = Some((seq, got.pop()));
                }
            }
        }
    }
}

/// An old row waiting for a reason to change that this turn would leave anyway — not read.
///
/// A row waiting for a lock: the lock holder is busy again (or was busy within the turn); the
/// missed lock attempt wrote the id into `busy` and the "someone is waiting" mark. A
/// throttled row: the turn has deferred `StartCall` (a deferral defers the whole rest of the
/// turn) — the callee goes into `busy` as when the row is read and then deferred, so later
/// messages for it wait too.
fn skip_parked(ctx: &Ctx<'_>, busy: &mut Busy, park: Park) -> bool {
    match park {
        Park::Lock(id) => {
            busy.contains(&id) || (ctx.locks.is_busy(&id) && try_lock(ctx, busy, id).is_none())
        }
        Park::Shed(callee) if ctx.l0.deferring_inbound() => {
            busy.insert(callee);
            true
        }
        _ => false,
    }
}

/// The ledger of `K` for this turn and the list of old rows to consider. Rows waiting for
/// the lock of a still-busy invocation are not in the list — the id goes into `busy` via a
/// missed lock attempt.
fn ledger_for<K: QueueRow>(ctx: &Ctx<'_>, busy: &mut Busy) -> (RowLedger, Reread<K>) {
    let ledger = ctx.drainer.take_ledger(K::QUEUE);
    let again = ledger.due(ctx.drainer.catalog_gen(), |id| {
        if !ctx.locks.is_busy(&id) {
            return true;
        }
        let lock = try_lock(ctx, busy, id);
        debug_assert!(
            lock.is_none(),
            "lock was just busy and nothing awaited in between"
        );
        false
    });
    (ledger, Reread::new(again))
}

/// Record the outcome of row `seq` in the ledger; return 1 if it was processed. A processed
/// row is still reread in later turns until it is seen to disappear ([`Park::Again`]).
fn note_row(ledger: &mut RowLedger, seq: u64, row: Row) -> usize {
    match row {
        Row::Handled => {
            ledger.note(seq, Park::Again);
            1
        }
        Row::Left(park) => {
            ledger.note(seq, park);
            0
        }
    }
}

/// Debug build: the invariant of [`RowLedger`] by a full scan at the end of the turn —
/// every live row below the scan cursor must be in the ledger. A mismatch is a row the
/// drainer would not reread until the next leader change: panic. Not counted in the
/// rows-read metric.
#[cfg(debug_assertions)]
async fn check_ledger<K: QueueRow>(engine: &mut Lease, ledger: &RowLedger) -> Result<()> {
    let mut from = 0u64;
    while from < ledger.scanned() {
        let rows = K::read_raw(engine, from, 256).await?;
        let Some((last, _)) = rows.last() else {
            break;
        };
        from = last.seq() + 1;
        for (k, _) in &rows {
            assert!(
                k.seq() >= ledger.scanned() || ledger.contains(k.seq()),
                "{:?} row seq {} is live below scan cursor {} but missing from the drainer's ledger",
                K::QUEUE,
                k.seq(),
                ledger.scanned()
            );
        }
    }
    Ok(())
}

/// Return the turn's ledger (checked in debug builds).
async fn keep_ledger<K: QueueRow>(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    ledger: RowLedger,
) -> Result<()> {
    #[cfg(debug_assertions)]
    if ledger.has_parked() {
        check_ledger::<K>(engine, &ledger).await?;
    }
    #[cfg(not(debug_assertions))]
    let _ = engine;
    ctx.drainer.keep_ledger(K::QUEUE, ledger);
    Ok(())
}

/// Send the pending signals: a target in the same partition is delivered in place, a
/// target in another partition gets the message forwarded and the row is only forgotten
/// after the other side confirms it is durable — the same contract as `drain_outbox`.
///
/// **A cursor by `seq`, not a fixed window.** The earlier version read the first 64 rows of
/// the table each turn: 64 rows to a dead target filled the window, and row 65 was never
/// seen. Now a turn goes through the whole index; a broken target only holds back its own
/// rows (`down`), like `drain_outbox`.
///
/// **A ledger per term, not rereading from 0**: a row left behind because the target's lock
/// was busy is only reread when the lock is released ([`RowLedger`]); the open scan carries
/// on from the previous turn's cursor.
///
/// **Same gathering and batching path as the outbox**: rows to another range go into
/// [`Sender`] — one `sync` per round, one sequential queue per target sending in batches,
/// targets in parallel — and are only deleted once the batch carrying them has been acked.
pub async fn drain_signals<'c>(engine: &mut Lease, ctx: &Ctx<'c>) -> Result<usize> {
    // One reader (the partition's drainer), so no drain lock — see [`crate::drainer`]. If a
    // target is busy, skip its rows in this turn.
    let mut busy: Busy = Busy::default();
    // Like `drain_outbox`: a target whose send failed only holds back its own rows.
    let mut down = Down::default();
    // No signal row holds the caller's lock across the send; `Sender` only needs somewhere
    // to release (nothing).
    let mut held = Held::new(ctx.runner);
    let mut sender = Sender::new(ctx.transport);
    // A mark **at the start** of the turn: rows produced after it (processing a signal can
    // spawn new signals) are left for the next turn. That way a turn always ends.
    let high = engine.outbox_high_water().await?;
    let (mut ledger, mut reread) = ledger_for::<cairn_engine::SignalRef>(ctx, &mut busy);
    let mut handled = 0;
    // Rows considered since the previous engine release: a full chunk ⇒ one durable ticket
    // for the chunk's send rows, then release the engine.
    let mut in_chunk = 0;
    // 1. Old rows of the ledger still to consider, by `seq`.
    while let Some((key, raw)) = reread
        .next(engine, &mut ledger, |park| {
            skip_parked(ctx, &mut busy, park)
        })
        .await?
    {
        let row = signal_row(
            engine,
            ctx,
            &mut busy,
            &mut sender,
            &mut held,
            &down,
            key,
            raw,
        )
        .await?;
        handled += note_row(&mut ledger, key.seq, row);
        in_chunk += 1;
        if chunk_full(engine, in_chunk, SIGNAL_BATCH) {
            handled += sender.flush(engine, ctx, &mut held, &mut down).await?;
            reread.release(engine).await;
            in_chunk = 0;
        }
    }
    // 2. The open scan from the previous turn's cursor.
    let mut cursor = ledger.scanned();
    'pass: loop {
        let pending = cairn_engine::SignalRef::read(engine, cursor, SIGNAL_BATCH).await?;
        if pending.is_empty() {
            break;
        }
        for (key, raw) in pending {
            if key.seq > high {
                break 'pass;
            }
            let row = signal_row(
                engine,
                ctx,
                &mut busy,
                &mut sender,
                &mut held,
                &down,
                key,
                raw,
            )
            .await?;
            handled += note_row(&mut ledger, key.seq, row);
            cursor = key.seq + 1;
            ledger.scanned_to(cursor);
            in_chunk += 1;
            if chunk_full(engine, in_chunk, SIGNAL_BATCH) {
                // Full chunk: durable ticket and hand over the send rows, then release the
                // engine. The rest of the read does not span the release — reread from the
                // cursor.
                handled += sender.flush(engine, ctx, &mut held, &mut down).await?;
                between_chunks(engine).await;
                in_chunk = 0;
                continue 'pass;
            }
        }
        // One durable ticket for every send row of the round, then hand them to the send task.
        handled += sender.flush(engine, ctx, &mut held, &mut down).await?;
    }
    handled += sender.finish(engine, ctx, &mut held, &mut down).await?;
    keep_ledger::<cairn_engine::SignalRef>(engine, ctx, ledger).await?;
    Ok(handled)
}

/// One row of a signal drain turn. A row sent to another range goes into `sender` and into
/// the ledger as a row to reread (until the batch carrying it is acked and it disappears).
#[allow(clippy::too_many_arguments)]
async fn signal_row<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    busy: &mut Busy,
    sender: &mut Sender,
    held: &mut Held<'c>,
    down: &Down,
    key: cairn_engine::SignalRef,
    raw: Bytes,
) -> Result<Row> {
    // A row that cannot be decoded (or is not a signal) is **kept**, not deleted — see
    // [`poison_row`]. The sender does not wait for signals, the target is in the corrupt
    // part: there is nobody to tell.
    let ds = match InterPartition::decode(raw) {
        Ok(InterPartition::Signal(ds)) => ds,
        Ok(_) => {
            poison_row(
                engine,
                ctx,
                PoisonQueue::Signal,
                key.seq,
                None,
                &"not a signal",
            )
            .await?;
            return Ok(Row::Left(Park::Again));
        }
        Err(e) => {
            poison_row(engine, ctx, PoisonQueue::Signal, key.seq, None, &e).await?;
            return Ok(Row::Left(Park::Again));
        }
    };
    if !engine.owns_id(&ds.target) {
        // The row stays intact until the batch carrying it is acked; a target that failed
        // within the turn stays.
        let p = Box::new(Pending {
            msg: InterPartition::Signal(ds),
            row: SourceRow::Signal(key),
            after: After::Forget,
        });
        if let Err(p) = sender.queue(engine, down, p) {
            left(held, p);
        }
        return Ok(Row::Left(Park::Again));
    }
    let Some(g) = try_lock(ctx, busy, ds.target) else {
        return Ok(Row::Left(Park::Lock(ds.target)));
    };
    settle_signal_local(engine, ctx, g, ds, Source::SignalOutbox { key }).await?;
    Ok(Row::Handled)
}

/// A signal/inbound row that **cannot be decoded** (TigerBeetle's "assert the negative
/// space").
///
/// Not deleted: deleting a message abandons its waiter without anyone knowing, and the row
/// may have been written by a **newer** binary (a rolling upgrade) — that version can read
/// it. Keeping the row blocks no other row (the drainer goes past it as past a busy
/// target); it only costs one read per drain turn. The first time it is seen in a term,
/// say so loudly and count it (`cairnflow_poison_rows_total`), then tell the waiter — if it
/// is known who, and it is in this range — with `paused` code
/// [`cairn_engine::INFRA_STUCK`]: a visible outcome, cleared with `resume` after fixing the
/// binary or the store, and the row is still there to deliver.
///
/// A busy waiter (mid-turn) is left for a later drain turn. An error writing `paused` is
/// only a `warn` (except losing write authority): one row's error does not block the rows
/// after it.
pub(super) async fn poison_row(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    queue: PoisonQueue,
    seq: u64,
    waiter: Option<(InvocationId, u32)>,
    err: &dyn std::fmt::Display,
) -> Result<()> {
    match ctx.drainer.note_poison(queue, seq) {
        PoisonSight::Settled => return Ok(()),
        PoisonSight::Pending => {}
        PoisonSight::New => {
            crate::metrics::flow::bump(&crate::metrics::flow::POISON_ROWS);
            tracing::error!(
                row = ?queue,
                seq,
                error = %err,
                waiter = waiter.map(|(id, _)| hex(&id)).as_deref(),
                "undecodable row — kept, not deleted"
            );
        }
    }
    let Some((id, completion_id)) = waiter.filter(|(id, _)| engine.owns_id(id)) else {
        ctx.drainer.settle_poison(queue, seq);
        return Ok(());
    };
    let (Some(guard), _) = life::claim(ctx.locks, ctx.runner, id) else {
        return Ok(());
    };
    let message = format!(
        "INFRA_STUCK: completion {completion_id} is in an undecodable {queue:?} row \
         (seq {seq}): {err} — the row is kept; fix the binary or the store, then resume"
    );
    let paused = life::pause_infra(engine, ctx, id, &message).await;
    drop(guard);
    match paused {
        Ok(_) => ctx.drainer.settle_poison(queue, seq),
        Err(e) => {
            if e.is_fenced() {
                return Err(e);
            }
            tracing::warn!(invocation = %hex(&id), error = %e, "could not record paused for the waiter of a corrupt row; the next drain retries");
        }
    }
    Ok(())
}

/// Number of cross-partition messages read per round — and the row cap of a **chunk**: a
/// drain turn releases the engine after at most that many rows ([`between_chunks`]).
///
/// No longer the cap of the whole turn: a turn goes through the whole index **up to the
/// mark at its start** — messages produced while processing are left for the next turn, so
/// a background tick still does not become a never-ending loop.
pub(super) const INBOUND_BATCH: usize = 64;

/// Process messages received from other partitions.
///
/// Runs on the background tick rather than on receipt, and that is what keeps this scheme
/// from deadlocking: the sender is acked as soon as the message is durable, so it does not
/// wait for us to finish running. A call cycle A→B→A is therefore just two queues, not two
/// threads locking each other.
///
/// **A ledger per term, not rereading from 0**: rows left behind go into [`RowLedger`] with
/// a reason ([`Park`]) — the target's lock busy, L0 throttle, service not yet in the
/// deployment registry — and are only reread when the reason changes; the open scan carries
/// on from the previous turn's cursor. The order of consideration is as before: the
/// ledger's old rows (every `seq` below the cursor) first, by `seq`, then the open scan;
/// the `busy` rule is unchanged — a row not reread still puts its target into `busy` where
/// the old path did.
///
/// **In chunks**: release the engine after every [`INBOUND_BATCH`] rows or when a hold
/// exceeds [`DRAIN_HOLD_BUDGET`], in both phases — one hold no longer grows with the backlog
/// (measured before chunking: ~770 rows, 0.5–0.6 s per hold under a backlog of 3·10⁵). The
/// release point lies between two rows; `busy`, the ledger, the cursor and the `high` mark
/// span it, rows read but not yet considered do not ([`between_chunks`]). Rows of a target
/// that becomes busy after the release point stay like every row of a busy target — the
/// per-target order holds (`try_lock`).
pub async fn drain_inbound<'c>(engine: &mut Lease, ctx: &Ctx<'c>) -> Result<usize> {
    // One reader (the partition's drainer), so no drain lock. The invocation lock is only
    // *tried*: a target in the middle of a turn has its row skipped (the row stays where it
    // is), rather than waiting out someone else's whole SDK turn.
    let mut busy: Busy = Busy::default();
    // A mark **at the start** of the turn: rows produced after it (processing a message can
    // spawn new messages) are left for the next turn. That way a turn always ends.
    let high = engine.outbox_high_water().await?;
    let (mut ledger, mut reread) = ledger_for::<cairn_engine::InboundKey>(ctx, &mut busy);
    let mut handled = 0;
    // Rows considered since the previous engine release.
    let mut in_chunk = 0;
    // 1. Old rows of the ledger still to consider, by `seq`.
    while let Some((key, raw)) = reread
        .next(engine, &mut ledger, |park| {
            skip_parked(ctx, &mut busy, park)
        })
        .await?
    {
        let row = inbound_row(engine, ctx, &mut busy, key, raw).await?;
        handled += note_row(&mut ledger, key.seq, row);
        in_chunk += 1;
        if chunk_full(engine, in_chunk, INBOUND_BATCH) {
            reread.release(engine).await;
            in_chunk = 0;
        }
    }
    // 2. The open scan from the previous turn's cursor.
    let mut cursor = ledger.scanned();
    'pass: loop {
        let pending = cairn_engine::InboundKey::read(engine, cursor, INBOUND_BATCH).await?;
        if pending.is_empty() {
            break;
        }
        for (key, raw) in pending {
            if key.seq > high {
                break 'pass;
            }
            let row = inbound_row(engine, ctx, &mut busy, key, raw).await?;
            handled += note_row(&mut ledger, key.seq, row);
            cursor = key.seq + 1;
            ledger.scanned_to(cursor);
            in_chunk += 1;
            if chunk_full(engine, in_chunk, INBOUND_BATCH) {
                // The rest of the read does not span the release: the next chunk rereads
                // from the cursor.
                between_chunks(engine).await;
                in_chunk = 0;
                continue 'pass;
            }
        }
    }
    keep_ledger::<cairn_engine::InboundKey>(engine, ctx, ledger).await?;
    Ok(handled)
}

/// One row of an inbound drain turn: decode it (keep a corrupt row), then process it by
/// message type.
async fn inbound_row<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    busy: &mut Busy,
    key: cairn_engine::InboundKey,
    raw: Bytes,
) -> Result<Row> {
    // A corrupt row: a test replaces the bytes of a `Completion` row with garbage — the shape
    // of a row written by a newer binary, or corrupted on the store.
    let raw = if key.kind == cairn_store::InboundKind::Completion
        && cairn_store::failpoint::fires("turn::drain_inbound::corrupt")
    {
        Bytes::from_static(&[0xff, 0xff])
    } else {
        raw
    };
    let msg = match InterPartition::decode(raw) {
        Ok(m) => m,
        Err(e) => {
            // **Keep, do not delete**: see [`poison_row`]. Keeping blocks no other row — the
            // drainer goes past it as past a busy target's row. Only a `Completion` row
            // tells who the waiter is (the target, in the same range as the row); other
            // types carry the waiter in the corrupt part or in the sending range.
            let waiter = (key.kind == cairn_store::InboundKind::Completion)
                .then_some((key.id, key.completion_id));
            poison_row(engine, ctx, PoisonQueue::Inbound, key.seq, waiter, &e).await?;
            return Ok(Row::Left(Park::Again));
        }
    };
    match msg {
        InterPartition::StartCall(sc) => inbound_start_call(engine, ctx, busy, key, sc).await,
        InterPartition::Completion(dc) => {
            let Some(g) = try_lock(ctx, busy, dc.target) else {
                return Ok(Row::Left(Park::Lock(dc.target)));
            };
            deliver_inbound_completion(engine, ctx, g, key, dc).await?;
            Ok(Row::Handled)
        }
        // A signal from another partition: the same delivery path as a local signal, only
        // the source row to clean up differs (inbound instead of signal-outbox).
        InterPartition::Signal(ds) => {
            let Some(g) = try_lock(ctx, busy, ds.target) else {
                return Ok(Row::Left(Park::Lock(ds.target)));
            };
            settle_signal_local(engine, ctx, g, ds, Source::Inbound { key }).await?;
            Ok(Row::Handled)
        }
        // A parent in another partition was just cancelled. Cancel our child, then drop the
        // row — dropping first would lose the cancel if we died in between.
        InterPartition::Cancel(dc) => {
            let Some(g) = try_lock(ctx, busy, dc.target) else {
                return Ok(Row::Left(Park::Lock(dc.target)));
            };
            cancel_child_locked(engine, ctx, g, dc.target).await?;
            engine.drop_inbound(key).await?;
            Ok(Row::Handled)
        }
        // Someone in another partition wants to wait for the result of one of our invocations.
        InterPartition::Attach(w) => {
            inbound_attach(engine, key, w).await?;
            Ok(Row::Handled)
        }
        // Peek without waiting: answer RIGHT AWAY with a Completion message — even "not done
        // yet" (`Void`). Not through the outbox, because there is nothing to remember: the
        // inbound row stays until the answer can be sent.
        InterPartition::PeekOutput(w) => inbound_peek(engine, ctx, key, w).await,
    }
}

/// A `StartCall` message: create the callee (or attach to an existing run), idempotently.
///
/// L0 throttle, idempotency attach, the workflow duplicate check and creation: [`admit`]
/// with source [`admit::Source::Inbound`]. The message is already durable in `inbound` and
/// the sender has been acked — throttled or not decidable yet, leave the row as is, a later
/// drain turn does it (the serve loop's throttle tick nudges it again). **Never** refused
/// at `/internal/inbound`: the sender would consider this whole range `down` and
/// completions coming here would get stuck with it.
async fn inbound_start_call<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    busy: &mut Busy,
    key: cairn_engine::InboundKey,
    sc: StartCall,
) -> Result<Row> {
    let sc_scope = parse_ident(sc.scope.as_deref(), "scope", &sc.callee, Scope::new);
    let sc_limit = parse_ident(
        sc.limit_key.as_deref(),
        "limit key",
        &sc.callee,
        LimitKey::parse,
    );
    // Corrupt tenant ⇒ create nothing (see `tenant_of_wire`): the message stays in
    // `inbound`, the sender is not wrongly acked.
    let Some(sc_tenant) = tenant_of_wire(sc.tenant.as_deref(), &sc.callee) else {
        return Ok(Row::Left(Park::Again));
    };
    let sc_target = Target {
        tenant: sc_tenant,
        scope: sc_scope,
        service: sc.service.clone(),
        handler: sc.handler.clone(),
        object_key: sc.object_key.clone(),
    };
    let reply_to = sc.reply_to.map(|(caller, completion_id)| ReplyTo {
        caller,
        completion_id,
    });
    let admission = admit::admit(
        engine,
        ctx,
        admit::Source::Inbound { reply_to, busy },
        admit::Request {
            id: sc.callee,
            target: &sc_target,
            limit_key: sc_limit.as_ref(),
            idempotency_key: sc.idempotency_key.as_deref(),
            input: sc.parameter.clone(),
            log_level: None,
            tags: &sc.lineage.tags,
            // The sender is the parent — except for a schedule firing, where `sender` is the
            // schedule id.
            sender: sc.schedule.is_none().then_some(sc.sender.0),
            root_id: sc.lineage.root_id,
            // A cross-range firing of a schedule: the header goes into entry 0
            // (`ctx.schedule`); the schedule row is in the sending range, not here.
            schedule: sc
                .schedule
                .as_deref()
                .map(|header| cairn_engine::ScheduleStamp { header, row: None }),
        },
    )
    .await?;
    use admit::Admission as A;
    match admission {
        // L0 throttle: reread in a turn that has not deferred. Not decidable yet: the
        // callee's lock is busy (the missed lock attempt wrote it into `busy`) ⇒ wait for
        // the lock; otherwise the service is not yet in the deployment registry ⇒ wait for
        // the registry to change.
        A::Shed => Ok(Row::Left(Park::Shed(sc.callee))),
        A::NotYet if busy.contains(&sc.callee) => Ok(Row::Left(Park::Lock(sc.callee))),
        A::NotYet => Ok(Row::Left(Park::Catalog(ctx.drainer.catalog_gen()))),
        // Creation is **idempotent**: if it already exists it is not created again. So
        // deleting the inbound row in a separate commit afterwards is safe — dying in between
        // only leads to a reprocessing with no side effects.
        A::Attached { guard, .. } => {
            engine.drop_inbound(key).await?;
            drop(guard);
            Ok(Row::Handled)
        }
        A::Created { guard, .. } => {
            engine.drop_inbound(key).await?;
            // **Emit, do not run**: the callee's first turn is the job of a task waiting for
            // its own slot, not of the drainer.
            drop(guard);
            ctx.runner.queue(sc.callee);
            Ok(Row::Handled)
        }
        A::Answered(_) | A::Rejected(_) | A::Refused(_) | A::Admitted => {
            unreachable!("an Inbound source never gets an ingress/restart reply")
        }
    }
}

/// The target state of a wait/peek message, seen from the caller's tenant carried on the
/// message: a different tenant ⇒ `None`, as if it did not exist.
async fn watch_status(engine: &Lease, w: &Watch) -> Result<Option<InvocationStatus>> {
    Ok(match cairn_engine::tenant_on_wire(w.tenant.as_deref()) {
        Some(t) => engine.visible_status(&t, &w.target).await?,
        None => None,
    })
}

/// An `Attach` message: queue the result if already done, otherwise record the waiter.
async fn inbound_attach(engine: &mut Lease, key: cairn_engine::InboundKey, w: Watch) -> Result<()> {
    match watch_status(engine, &w).await? {
        Some(InvocationStatus::Completed(v)) => {
            engine
                .enqueue_reply(&w.target, &w.caller, w.completion_id, v.into_reply())
                .await?;
        }
        Some(InvocationStatus::Failed { code, message }) => {
            engine
                .enqueue_reply(
                    &w.target,
                    &w.caller,
                    w.completion_id,
                    ReplyResult::Failure { code, message },
                )
                .await?;
        }
        Some(_) => engine.attach(&w.target, &w.caller, w.completion_id).await?,
        None => {
            engine
                .enqueue_reply(
                    &w.target,
                    &w.caller,
                    w.completion_id,
                    ReplyResult::Failure {
                        code: 404,
                        message: format!("invocation {} not found", hex(&w.target)),
                    },
                )
                .await?;
        }
    }
    engine.drop_inbound(key).await?;
    Ok(())
}

/// A `PeekOutput` message: answer right away with a `Completion` message.
async fn inbound_peek(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    key: cairn_engine::InboundKey,
    w: Watch,
) -> Result<Row> {
    let result = match watch_status(engine, &w).await? {
        // Segmented ⇒ ref + source owner; the shell of the waiter's range assembles or COPYs.
        Some(InvocationStatus::Completed(v)) => v.into_reply().into(),
        Some(InvocationStatus::Failed { code, message }) => {
            CompletionPayload::Failure { code, message }
        }
        Some(_) => CompletionPayload::Void,
        None => CompletionPayload::Failure {
            code: 404,
            message: format!("invocation {} not found", hex(&w.target)),
        },
    };
    let msg = InterPartition::Completion(DeliverCompletion {
        target: w.caller,
        completion_id: w.completion_id,
        result,
    });
    // A **full** ticket: the answer reads the output of another invocation, it is not a
    // send row.
    let durable = cairn_store::SendTicket::from(&engine.sync().await?);
    // Per-target circuit breaker: the inbound drainer does not pay a full `SEND_TIMEOUT` for
    // a target already known to hang — this send is a probe batch.
    let dest = engine.cluster().partition_of_id(&w.caller);
    let version = engine.cluster().version();
    let timeout = if ctx.drainer.tripped(dest, version) {
        crate::transport::PROBE_TIMEOUT
    } else {
        crate::transport::SEND_TIMEOUT
    };
    let sent = engine
        .released(async |r: &Released| ctx.transport.send_within(&msg, &durable, r, timeout).await)
        .await;
    if let Err(e) = sent {
        if e.is_timeout() {
            ctx.drainer.trip(dest, version);
        }
        tracing::warn!(error = %e, "could not answer a cross-partition peek yet");
        return Ok(Row::Left(Park::Again)); // keep the inbound row, retry on the next tick
    }
    ctx.drainer.untrip(dest);
    engine.drop_inbound(key).await?;
    Ok(Row::Handled)
}

/// The result of an invocation that has stopped for good, as a completion for the waiter.
pub(super) fn status_completion(s: InvocationStatus) -> CompletionResult {
    match s {
        InvocationStatus::Completed(cairn_engine::InvocationOutput::Inline(v)) => {
            CompletionResult::CallValue(v)
        }
        // The caller delivers segmented output through `values::deliver_output` before
        // getting here; this branch is unreachable.
        InvocationStatus::Completed(cairn_engine::InvocationOutput::Segmented(_)) => {
            CompletionResult::CallFailure(pb::Failure {
                code: 500,
                message: "segmented output must be delivered by the shell".into(),
                metadata: vec![],
            })
        }
        InvocationStatus::Failed { code, message } => CompletionResult::CallFailure(pb::Failure {
            code,
            message,
            metadata: vec![],
        }),
        other => CompletionResult::CallFailure(pb::Failure {
            code: 500,
            message: format!("status is not terminal: {other:?}"),
            metadata: vec![],
        }),
    }
}

pub(super) fn watch_not_found(target: &InvocationId) -> CompletionResult {
    CompletionResult::CallFailure(pb::Failure {
        code: 404,
        message: format!("invocation {} not found", hex(target)),
        metadata: vec![],
    })
}

/// Deliver a completion coming from another partition.
///
/// This is why the `inbound` table must deduplicate by key: the sender keeps the outbox row
/// until it gets the ack, so it **will** resend after dying at exactly the wrong spot. The
/// content key takes care of "no two rows"; the rest — a row fully processed while the
/// sender has not learnt it yet — is the `settled` check of `Engine::deliver`: if the
/// completion already has a notification, the commit only has to delete the inbound row.
///
/// Delivering the same completion id twice violates exactly-once delivery and **ends the
/// invocation with an error**. So that check is not an optimisation.
///
/// **One journal load**: this function used to load the journal once to check `settled`
/// and then `settle_completion` loaded it again. The first check was empty — a freshly
/// loaded machine has not replayed so its `settled` holds nothing yet (the set only fills
/// at `Event::Start`) — so only the load in the delivery path remains, after the core.
pub(super) async fn deliver_inbound_completion<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    guard: InvGuard<'c>,
    key: cairn_engine::InboundKey,
    dc: DeliverCompletion,
) -> Result<()> {
    // Already ended or missing ⇒ a late wake-up; waking a terminal invocation is rerunning
    // it.
    let mut arrival = arrived(
        engine,
        ctx,
        dc.target,
        lc::WakeKind::Completion,
        Some(dc.completion_id),
    )
    .await?;
    if arrival.plan.wake == lc::WakeWrite::Discard {
        engine.drop_inbound(key).await?;
        return Ok(());
    }
    let inv = match arrival.machine(engine, dc.target).await? {
        Ok(inv) => inv,
        Err(e) => {
            // No journal ⇒ the invocation does not exist in this partition. Misrouted, or
            // the data has been cleaned up. Do not keep the row waiting for an invocation
            // that will never appear.
            tracing::error!(error = %e, "cross-partition completion has no target, skipped");
            engine.drop_inbound(key).await?;
            return Ok(());
        }
    };

    let result = match dc.result {
        CompletionPayload::InvocationId(id) => CompletionResult::CallInvocationId(id),
        CompletionPayload::Value(v) => CompletionResult::CallValue(v),
        CompletionPayload::Failure { code, message } => {
            CompletionResult::CallFailure(pb::Failure {
                code,
                message,
                metadata: vec![],
            })
        }
        CompletionPayload::Void => CompletionResult::Void,
        // Segmented output from another range: assemble inline or COPY to the receiver's
        // owner, outside the engine. On failure ⇒ the inbound row stays, retry next time.
        CompletionPayload::Segmented(o) => {
            super::values::deliver_output(engine, &dc.target, o).await?
        }
    };
    // **Write, then hand over — do not run.** Delete the inbound row in the same commit as
    // the notification it produces: split apart, a death between the two commits would
    // replay it, and replaying is exactly what exactly-once delivery forbids.
    settle_into(
        engine,
        inv,
        Completion {
            id: dc.completion_id,
            result,
        },
        Source::Inbound { key },
        !arrival.stays,
    )
    .await?;
    // Release the lock **before** handing over: the task running it will ask for this very lock.
    drop(guard);
    if arrival.plan.ram.handout.is_some() {
        ctx.runner.queue(dc.target);
    }
    Ok(())
}
