// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Outbox draining: `drain_outbox` (gather, then send per destination),
//! `sync_outbox*`, `cancel_child_locked`.

use {
    super::*,
    crate::{drainer::Backlog, lease::IoSet, metrics::interrange::drain as drain_metrics},
};

/// The invocation that **local** processing of an outbox row must lock, if any.
///
/// `Call` always writes `CallInvocationId` into the caller (even when the
/// target is on another partition); other rows only lock when the local part of
/// the work is on this partition — through the transport, the other side takes
/// care of itself.
pub(super) fn local_lock_target(entry: &OutboxEntry, engine: &Engine) -> Option<InvocationId> {
    let local = |id: &InvocationId| engine.owns_id(id);
    match entry {
        OutboxEntry::Call(c) => Some(c.caller),
        OutboxEntry::Attach(w) | OutboxEntry::PeekOutput(w) => local(&w.target).then_some(w.caller),
        OutboxEntry::Reply(r) => local(&r.caller).then_some(r.caller),
        OutboxEntry::Cancel(c) => local(&c.target).then_some(c.target),
        // A schedule firing writes into no invocation on this range.
        OutboxEntry::Fire(_) => None,
    }
}

/// The **other** partition an outbox row will be sent to, if it can be known
/// without doing anything. `Call` returns `None`: its target is only known after
/// `dispatch_call` (it needs the deployment registry), and the caller checks it there.
pub(super) fn remote_destination(
    entry: &OutboxEntry,
    engine: &Engine,
) -> Option<cairn_store::PartitionId> {
    let target = match entry {
        OutboxEntry::Call(_) | OutboxEntry::Fire(_) => return None,
        OutboxEntry::Attach(w) | OutboxEntry::PeekOutput(w) => &w.target,
        OutboxEntry::Reply(r) => &r.caller,
        OutboxEntry::Cancel(c) => &c.target,
    };
    // "Ours" by the engine's range; "whose" by the current table.
    (!engine.owns_id(target)).then(|| engine.cluster().partition_of_id(target))
}

/// Sends the messages of one destination queue from its send task — a single
/// place for the failpoint `turn::drain_outbox::before_send` (the gap between
/// the durable ticket and the send).
async fn send_queue(
    transport: &Transport,
    msgs: &[InterPartition],
    durable: &cairn_store::SendTicket,
    released: &Released,
    timeout: std::time::Duration,
) -> crate::transport::Sent {
    cairn_store::failpoint!("turn::drain_outbox::before_send");
    transport
        .send_batch_within(msgs, durable, released, timeout)
        .await
}

/// Destination ranges whose send failed within one drain pass — see
/// [`drain_outbox`]. Lives for one pass; a **hung** destination (past
/// `SEND_TIMEOUT`) also goes into the drainer's circuit breaker, which lives
/// across passes ([`crate::drainer::Drainer::tripped`]).
pub(super) type Down = cairn_store::entropy::HashSet<cairn_store::RangeId>;

/// Maximum number of destination ranges sent to in parallel within one drain pass.
const MAX_DEST_CONCURRENCY: usize = 8;

/// A row (outbox, or signal) that has been decided "send through the
/// transport", waiting for its turn to be sent. The row stays intact in the
/// store until `after`.
pub(super) struct Pending {
    pub(super) msg: InterPartition,
    pub(super) row: SourceRow,
    pub(super) after: After,
}

/// The source row of a [`Pending`].
#[derive(Clone, Copy)]
pub(super) enum SourceRow {
    Outbox(OutboxRef),
    Signal(cairn_engine::SignalRef),
}

/// Local work **after the receiver acks** — runs once the engine is taken
/// back, in exactly the send order within each destination.
pub(super) enum After {
    /// A `Call` row: hand the id to the caller and delete the row in the same
    /// commit, under the caller lock the drain pass holds across the send
    /// ([`Held`]), then count the call (`cairnflow_calls_total`, counted only
    /// once the id has been
    /// delivered).
    Deliver {
        caller: InvocationId,
        completion: Completion,
        dispatch: Dispatch,
    },
    /// Any other row: the other side is durable ⇒ it may be forgotten.
    Forget,
}

/// Outcome of the gather phase for one row.
enum Gathered {
    /// Done locally (usually deleted).
    Handled,
    /// Stays where it is; the next pass retries.
    Left,
    /// Waiting to be sent.
    Send(Box<Pending>),
}

/// The local lock of a row: held by itself, or borrowed from [`Held`] (the
/// same invocation has a row awaiting ack in this pass).
enum Lock<'c> {
    Own(InvGuard<'c>),
    Shared(InvocationId),
}

impl<'c> Lock<'c> {
    fn guard<'a>(&'a self, held: &'a Held<'c>) -> &'a InvGuard<'c> {
        match self {
            Lock::Own(g) => g,
            Lock::Shared(id) => held.guard(id),
        }
    }
}

/// The caller lock the drain pass holds **across the send**.
///
/// A `Call` row is sent first and the id delivered afterwards; while waiting
/// for the ack the caller lock stays held, as it did before sends were
/// batched — only longer by at most one send round. It is what keeps
/// `Cancel(c)` behind `StartCall(c)`: the caller's `/cancel` needs exactly this
/// lock, and only produces a `Cancel` row once the caller has `c`'s id
///
/// Several rows of the **same** caller (fan-out) share one lock: a later row
/// borrows the earlier row's lock instead of being skipped as "busy". The lock
/// is released when the caller's last row awaiting ack has been applied or
/// left; the caller is handed to the scheduler (if any row asked for it)
/// **after** the release ([`deliver_to_caller`]).
pub(super) struct Held<'c> {
    runner: &'c crate::drainer::Runner,
    map: cairn_store::entropy::HashMap<InvocationId, HeldLock<'c>>,
}

struct HeldLock<'c> {
    guard: InvGuard<'c>,
    /// Number of rows awaiting ack that rely on this lock.
    pending: usize,
    /// Whether some completion delivery asked for the caller to be handed to the scheduler.
    handout: bool,
}

impl<'c> Held<'c> {
    pub(super) fn new(runner: &'c crate::drainer::Runner) -> Self {
        Self {
            runner,
            map: Default::default(),
        }
    }

    fn holds(&self, id: &InvocationId) -> bool {
        self.map.contains_key(id)
    }

    fn guard(&self, id: &InvocationId) -> &InvGuard<'c> {
        &self
            .map
            .get(id)
            .expect("a borrowed lock must be held")
            .guard
    }

    /// A row of `id` awaiting ack relies on `lock` from now until [`Self::release`].
    fn adopt(&mut self, id: InvocationId, lock: Lock<'c>) {
        match lock {
            Lock::Own(guard) => {
                let old = self.map.insert(
                    id,
                    HeldLock {
                        guard,
                        pending: 1,
                        handout: false,
                    },
                );
                debug_assert!(old.is_none(), "a self-held lock cannot already be in Held");
            }
            Lock::Shared(id) => {
                self.map
                    .get_mut(&id)
                    .expect("a borrowed lock must be held")
                    .pending += 1;
            }
        }
    }

    fn mark_handout(&mut self, id: InvocationId) {
        if let Some(l) = self.map.get_mut(&id) {
            l.handout = true;
        }
    }

    /// The row of `id` awaiting ack is done (applied, or left).
    fn release(&mut self, id: InvocationId) {
        let Some(l) = self.map.get_mut(&id) else {
            return;
        };
        l.pending -= 1;
        if l.pending == 0 {
            let l = self.map.remove(&id).expect("just seen");
            Self::let_go(self.runner, id, l);
        }
    }

    fn let_go(runner: &crate::drainer::Runner, id: InvocationId, l: HeldLock<'_>) {
        let handout = l.handout;
        drop(l.guard);
        if handout {
            runner.queue(id);
        }
    }
}

impl Drop for Held<'_> {
    /// A drain pass that failed midway: release every lock, and still hand out
    /// the callers that received a completion — the completion is durable,
    /// nobody may forget it.
    fn drop(&mut self) {
        for (id, l) in std::mem::take(&mut self.map) {
            Self::let_go(self.runner, id, l);
        }
    }
}

/// The send result of one destination: one [`crate::transport::Delivery`] per
/// message, in exactly the send order.
type Flown = (cairn_store::RangeId, crate::transport::Sent);

/// The sending part of a drain pass.
///
/// - **Per-destination queues, each destination sequential** (IR-3): messages
///   to the same range go in exactly their creation order. Each destination's
///   send task carries **one batch** — the head of the queue that fits the cap
///   of 64 messages / 1 MiB ([`crate::transport::chunk_len`]) — through
///   [`Transport::send_batch`] (one commit on the receiver; re-split by the new
///   table when the table changes); the next batch is only handed over once the
///   previous task has returned and been applied. When a message fails, it and
///   every later message to the same destination stay (`down`), including
///   messages gathered in later rounds
///   (`dead_destination.rs::oversized_queue_goes_as_consecutive_batches_…`).
/// - **Parallel across destinations**, at most [`MAX_DEST_CONCURRENCY`], on
///   separate tasks ([`IoSet`]) — the engine is never held by anyone waiting on
///   the network (D16, IR-8).
/// - **The destination that finishes first is applied first**: the drainer
///   applies one destination's results as soon as they return — between two
///   gather rounds, or while waiting at the end of the pass — without waiting
///   for other destinations; a hung destination only holds its own rows until
///   `SEND_TIMEOUT`.
/// - **Per-destination circuit breaker**: a send that hits `SEND_TIMEOUT`
///   puts the destination into the drainer's circuit breaker
///   ([`crate::drainer::Drainer::trip`]); in later passes that destination only
///   receives **one** probe batch capped by
///   [`crate::transport::PROBE_TIMEOUT`] — on failure it and every later row to
///   the same destination stay like any failure (`down`), on a full ack the
///   breaker is cleared and the queue carries on with the normal cap. The order
///   between rows to the same destination does not change: the probe batch is
///   the head of the queue, like every batch.
/// - **One send ticket per round**: rows gathered in a round are only handed to
///   a send task after that round's [`Lease::sync_sends`] — every row sent was
///   read as durable (LF-1, IR-1), without waiting for commits that hold no
///   send rows (turn endings, other turns).
pub(super) struct Sender {
    transport: Transport,
    io: IoSet<Flown>,
    /// Gathered rows not yet handed to any task, per destination, in exactly creation order.
    queued: cairn_store::entropy::HashMap<
        cairn_store::RangeId,
        std::collections::VecDeque<Box<Pending>>,
    >,
    /// Rows handed to the destination's send task, in exactly send order.
    flying: cairn_store::entropy::HashMap<
        cairn_store::RangeId,
        std::collections::VecDeque<Box<Pending>>,
    >,
    /// Destinations with waiting rows but no task yet, in order (FIFO waiting for a slot).
    waiting: std::collections::VecDeque<cairn_store::RangeId>,
    /// The destination of each target key **within the pass**: every message
    /// with the same target key (e.g. `StartCall(c)` and `Cancel(c)`) sits in
    /// the same queue even when the range table changes mid-pass. The transport
    /// still routes each message by the table at send time
    /// (IR-6).
    pinned: cairn_store::entropy::HashMap<cairn_store::PartitionKey, cairn_store::RangeId>,
    /// Send ticket of the latest `sync_sends`.
    durable: Option<cairn_store::SendTicket>,
    /// Rows gathered since the latest `sync`.
    unsynced: bool,
}

impl Sender {
    pub(super) fn new(transport: &Transport) -> Self {
        Self {
            transport: transport.clone(),
            io: IoSet::default(),
            queued: Default::default(),
            flying: Default::default(),
            waiting: Default::default(),
            pinned: Default::default(),
            durable: None,
            unsynced: false,
        }
    }

    /// Queues `p` for its destination. `Err(p)` = the destination already failed
    /// in this pass — the row stays.
    pub(super) fn queue(
        &mut self,
        engine: &Engine,
        down: &Down,
        p: Box<Pending>,
    ) -> std::result::Result<(), Box<Pending>> {
        let pk = cairn_store::partition_key_of(p.msg.destination_id());
        let dest = *self
            .pinned
            .entry(pk)
            .or_insert_with(|| engine.cluster().partition_of_key(pk));
        if down.contains(&dest) {
            return Err(p);
        }
        let queue = self.queued.entry(dest).or_default();
        if queue.is_empty() && !self.flying.contains_key(&dest) {
            self.waiting.push_back(dest);
        }
        queue.push_back(p);
        self.unsynced = true;
        Ok(())
    }

    /// Hands queues to send tasks, up to the parallel-destination cap. Only
    /// after `sync`. A destination in the circuit breaker (range table
    /// `version`) goes with the cap [`crate::transport::PROBE_TIMEOUT`] — one
    /// probe batch.
    fn launch(&mut self, breaker: &crate::drainer::Drainer, version: u64) {
        if self.unsynced {
            debug_assert!(false, "row handed to the send task before a durable ticket");
            return;
        }
        let Some(durable) = self.durable else {
            return;
        };
        while self.io.len() < MAX_DEST_CONCURRENCY {
            let Some(dest) = self.waiting.pop_front() else {
                return;
            };
            let Some(queue) = self.queued.get_mut(&dest).filter(|q| !q.is_empty()) else {
                continue;
            };
            // **One batch at a time**: the head of the queue that fits the batch
            // cap; the rest waits until this task returns (`settle` re-queues the
            // destination), so a later batch to the same destination only goes
            // once the earlier batch is acked — and an acked batch is applied at
            // once, without waiting for the next one.
            let take = crate::transport::chunk_len(queue.iter().map(|p| p.msg.encode().len()));
            let batch: std::collections::VecDeque<Box<Pending>> = queue.drain(..take).collect();
            let msgs: Vec<InterPartition> = batch.iter().map(|p| p.msg.clone()).collect();
            self.flying.insert(dest, batch);
            let transport = self.transport.clone();
            let timeout = if breaker.tripped(dest, version) {
                crate::transport::PROBE_TIMEOUT
            } else {
                crate::transport::SEND_TIMEOUT
            };
            self.io.spawn(async move |r: &Released| {
                use crate::metrics::interrange::SEND;
                let started = tokio::time::Instant::now();
                cairn_store::probe::mark("drain.send", msgs.len() as u64);
                let sent = send_queue(&transport, &msgs, &durable, r, timeout).await;
                cairn_store::probe::mark("drain.send_done", u64::from(dest.0));
                SEND.observe(started.elapsed());
                (dest, sent)
            });
        }
    }

    /// End of a gather round: one `sync` for every new row, hand rows to send
    /// tasks, then apply destinations that have finished — waiting for none.
    pub(super) async fn flush<'c>(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'c>,
        held: &mut Held<'c>,
        down: &mut Down,
    ) -> Result<usize> {
        if self.unsynced {
            use crate::metrics::interrange::SYNC;
            let synced = tokio::time::Instant::now();
            cairn_store::probe::mark("drain.sync", u64::from(engine.partition().0));
            // Holding the engine: tests only count here, never pause.
            cairn_store::failpoint!("turn::drain_outbox::before_sync");
            // Send-ticket watermark **at gather time** (after every read of the
            // round) — enables a cheap check once the ticket is in hand.
            let mark = engine.writer().latest_send_seq();
            crate::metrics::floor::send_wait(
                if mark.is_none_or(|m| engine.writer().durable_seq() >= m) {
                    crate::metrics::floor::Ticket::Ready
                } else {
                    crate::metrics::floor::Ticket::Wait
                },
            );
            // Outbox rows must be durable **before** the other partition sees
            // the message: that side receives and acts, and if this side dies
            // it does not remember who it asked to do what. The send ticket is
            // taken at the time of asking: the latest write holding send rows —
            // every row gathered so far (LF-1).
            self.durable = Some(engine.sync_sends().await?);
            self.check_ir1(engine, mark).await?;
            SYNC.observe(synced.elapsed());
            cairn_store::probe::mark("drain.sync_done", u64::from(engine.partition().0));
            self.unsynced = false;
        }
        self.launch(ctx.drainer, engine.cluster().version());
        let mut handled = 0;
        while let Some(done) = self.io.try_next() {
            handled += self.settle(engine, ctx, held, down, done).await?;
            self.launch(ctx.drainer, engine.cluster().version());
        }
        Ok(handled)
    }

    /// The IR-1 tripwire (LF-1), right after the send ticket is in hand and
    /// before rows are handed to send tasks. Two layers, independent of each other:
    ///
    /// - **every build**: `durable_seq` is not below the send-ticket watermark
    ///   at gather time — a cheap check on the writer's own ledger;
    /// - **debug/`testkit` builds**: re-read every row about to be sent at the
    ///   **durable** level (`PartitionWriter::get_durable`, `Remote` filter) — it
    ///   asks the store itself, not the writer's ledger, so it still catches an
    ///   error that coincides with a wrong watermark.
    ///
    /// A mismatch ⇒ failpoint `turn::outbox::undurable_send` (tests turn on
    /// `panic`), an `error` log, metric
    /// `cairnflow_invariant_violations_total{check="ir1"}`, and **nothing is
    /// sent** this round: the error fails the drain pass, rows stay, the next
    /// pass re-reads them.
    async fn check_ir1(&self, engine: &mut Lease, mark: Option<u64>) -> Result<()> {
        let durable_seq = engine.writer().durable_seq();
        if let Some(m) = mark.filter(|m| durable_seq < *m) {
            return Err(ir1_violation(format!(
                "send ticket issued with durable_seq {durable_seq} below the gather mark {m}"
            )));
        }
        #[cfg(any(debug_assertions, feature = "testkit"))]
        for q in self.queued.values() {
            for p in q {
                let key = match p.row {
                    SourceRow::Outbox(r) => cairn_store::keys::outbox(r.owner, &r.key.0, r.key.1),
                    SourceRow::Signal(s) => {
                        cairn_store::keys::signal_outbox(&s.sender, s.entry_index)
                    }
                };
                let seen = engine.writer().get(&key).await?;
                let durable = engine.writer().get_durable(&key).await?;
                // A row that is already gone (destination handled locally,
                // another pass deleted it) leaves nothing to compare.
                if seen.is_some() && durable != seen {
                    return Err(ir1_violation(format!(
                        "send row seq {} not durable when handed to the send task (durable: {}, visible: yes)",
                        match p.row {
                            SourceRow::Outbox(r) => r.seq,
                            SourceRow::Signal(s) => s.seq,
                        },
                        if durable.is_some() {
                            "different bytes"
                        } else {
                            "absent"
                        },
                    )));
                }
            }
        }
        Ok(())
    }

    /// End of the pass: wait for every destination still in flight (releasing
    /// the engine while waiting), applying each destination as it returns.
    pub(super) async fn finish<'c>(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'c>,
        held: &mut Held<'c>,
        down: &mut Down,
    ) -> Result<usize> {
        let mut handled = self.flush(engine, ctx, held, down).await?;
        while let Some(done) = engine.next_io(&mut self.io).await {
            handled += self.settle(engine, ctx, held, down, done).await?;
            self.launch(ctx.drainer, engine.cluster().version());
        }
        // The task was cancelled (runtime shutting down): its rows stay.
        for (_, q) in self.flying.drain().chain(self.queued.drain()) {
            for p in q {
                left(held, p);
            }
        }
        Ok(handled)
    }

    /// Applies one destination's send results, in exactly the send order.
    async fn settle<'c>(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'c>,
        held: &mut Held<'c>,
        down: &mut Down,
        (dest, sent): Flown,
    ) -> Result<usize> {
        use crate::transport::Delivery;
        let batch = self.flying.remove(&dest).unwrap_or_default();
        for (first, e) in &sent.errors {
            // The row is intact ⇒ nothing lost, only delayed. Only this
            // destination stops for the pass; other rows carry on.
            tracing::warn!(
                error = %e,
                dest = dest.0,
                "could not send cross-partition {} yet",
                batch.get(*first).map_or("message", |p| what(&p.msg))
            );
        }
        if !sent.errors.is_empty() {
            down.insert(dest);
        }
        // Circuit breaker: only a send that **hit the cap** opens it — a
        // destination that refuses at once costs the drainer nothing; a batch
        // acked in full clears it.
        if sent.errors.iter().any(|(_, e)| e.is_timeout()) {
            ctx.drainer.trip(dest, engine.cluster().version());
        } else if sent.errors.is_empty() && !batch.is_empty() {
            ctx.drainer.untrip(dest);
        }
        let mut results = sent.delivery.into_iter();
        let mut handled = 0;
        // Apply in exactly the send order. A batch re-split by a new table may
        // leave acked messages after a failed one — they went to another range,
        // and no message with the same target key as the failed one is sent
        // after it (`Delivery::NotSent`).
        for p in batch {
            match results.next() {
                Some(Delivery::Acked) => {
                    apply_after(engine, ctx, held, p).await?;
                    handled += 1;
                }
                // Failed, or not sent: stays.
                Some(Delivery::Failed | Delivery::NotSent) | None => left(held, p),
            }
        }
        if down.contains(&dest) {
            // Every later row to the same destination, including those gathered
            // in later rounds, stays — their relative order is kept, the next
            // pass scans again from the start.
            for p in self.queued.remove(&dest).into_iter().flatten() {
                left(held, p);
            }
        } else if self.queued.get(&dest).is_some_and(|q| !q.is_empty()) {
            self.waiting.push_back(dest);
        }
        Ok(handled)
    }
}

/// An IR-1 break caught by the tripwire: report it on every channel, then turn
/// it into an error of the drain pass (nothing is sent).
fn ir1_violation(what: String) -> ActorError {
    cairn_store::failpoint!("turn::outbox::undurable_send");
    crate::metrics::floor::invariant_violation("ir1");
    tracing::error!(detail = %what, "IR-1: the drainer was about to send a non-durable row — not sent this round");
    ActorError::Engine(cairn_engine::EngineError::Store(
        cairn_store::StoreError::Io(format!("IR-1 broken: {what}")),
    ))
}

/// Message kind name for logs.
fn what(msg: &InterPartition) -> &'static str {
    match msg {
        InterPartition::StartCall(_) => "call",
        InterPartition::Completion(_) => "result",
        InterPartition::Attach(_) => "attach",
        InterPartition::PeekOutput(_) => "peek",
        InterPartition::Cancel(_) => "cancel",
        InterPartition::Signal(_) => "signal",
    }
}

/// A row waiting to be sent stays (send failed, or its turn never came).
pub(super) fn left(held: &mut Held<'_>, p: Box<Pending>) {
    if let After::Deliver { caller, .. } = p.after {
        held.release(caller);
    }
}

/// Local work after a row's ack.
async fn apply_after<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    held: &mut Held<'c>,
    p: Box<Pending>,
) -> Result<()> {
    let p = *p;
    match p.after {
        After::Deliver {
            caller,
            completion,
            dispatch,
        } => {
            let SourceRow::Outbox(row) = p.row else {
                unreachable!("only outbox Call rows hand out ids");
            };
            let handout =
                deliver_to_caller(engine, ctx, held.guard(&caller), caller, completion, row)
                    .await?;
            if handout {
                held.mark_handout(caller);
            }
            dispatch.count();
            held.release(caller);
        }
        // Durable on the other side ⇒ it may be forgotten.
        After::Forget => match p.row {
            SourceRow::Outbox(row) => engine.drop_outbox(row).await?,
            SourceRow::Signal(key) => engine.drop_signal(key).await?,
        },
    }
    Ok(())
}

/// Delivers a completion **locally** under lock `lock` ([`deliver_to_caller`]),
/// then releases the lock and hands the caller to the scheduler — or, with a
/// borrowed lock, lets [`Held`] do that on release.
async fn deliver_local<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    held: &mut Held<'c>,
    lock: Lock<'c>,
    caller: InvocationId,
    completion: Completion,
    row: OutboxRef,
) -> Result<()> {
    let handout = deliver_to_caller(engine, ctx, lock.guard(held), caller, completion, row).await?;
    match lock {
        Lock::Own(guard) => {
            drop(guard);
            if handout {
                ctx.runner.queue(caller);
            }
        }
        Lock::Shared(id) => {
            if handout {
                held.mark_handout(id);
            }
        }
    }
    Ok(())
}

/// Tries to lock `id` for a row: borrow it if the pass holds it across a send,
/// otherwise [`try_lock`].
fn take_lock<'c>(
    ctx: &Ctx<'c>,
    busy: &mut Busy,
    held: &Held<'c>,
    id: InvocationId,
) -> Option<Lock<'c>> {
    if held.holds(&id) {
        return Some(Lock::Shared(id));
    }
    let known_busy = busy.contains(&id);
    let lock = try_lock(ctx, busy, id);
    if lock.is_none() && !known_busy {
        drain_metrics::add(&drain_metrics::LOCK_BUSY, 1);
    }
    lock.map(Lock::Own)
}

/// One **drain pass**: goes over every outbox row still to be considered, then
/// returns the watermark it passed.
///
/// # Who calls it
///
/// Exactly one place: the partition's drainer task ([`crate::drainer`]). There
/// is no drain lock here and **none may be added** — what keeps two drain
/// passes from picking up the same row is that there is only one reader, not a
/// lock. Adding a second call site reopens exactly the class of bugs that the
/// single reader closes.
///
/// # A cursor, not a snapshot
///
/// The old path read `pending_outbox(32)` — a snapshot of 32 rows in key order
/// — and went back to re-read from the start in the next round. Two flaws: key
/// order is not creation order, and row 33 was not seen until the next round. A
/// cursor advances, and advances in exactly creation order; a thousand-branch
/// fan-out goes through in `ceil(1000/32)` rounds rather than a thousand
/// re-reads.
///
/// # A per-term ledger, no re-read from 0
///
/// The cursor carries across passes in the drainer's [`Backlog`]: the next
/// pass only scans onward from where the previous pass stopped, plus **re-reads
/// row by row** the old rows still to be considered — rows left for a reason
/// other than a lock (*stuck* rows: target not yet in this node's deployment
/// registry; destination failed in the pass), rows processed but not yet seen
/// to disappear, and rows waiting for the lock of an invocation **that has been
/// released**. Rows waiting for the lock of an invocation that is still busy
/// are **not read**: this pass would leave them anyway, exactly as a read
/// followed by a failed lock attempt — and the id is still recorded as "someone
/// is waiting" (the failed lock attempt), so the guard holding it pokes the
/// drainer on release. Before this, a fan-out parent running a turn made every
/// drain pass re-read all of its thousand rows — quadratic in the number of
/// children.
///
/// The ledger is an in-RAM hint: a pass that fails midway does not return the
/// ledger, and a leader change means a new ledger — the next pass scans again
/// from 0, the old path. No row is missed, thanks to the invariant of
/// [`Backlog`]; debug builds re-check it at the end of every pass with a full
/// scan ([`check_backlog`]).
///
/// # The consideration order is unchanged
///
/// Every pass considers rows in exactly `seq` order: the ledger's old rows
/// (every `seq` below the scan cursor) first, then the onward scan. The skip
/// rules do not change: once an id is busy in the pass, every later row of it
/// stays (`busy`); once a destination has failed, every later row to it stays
/// (`down`) — their relative order is kept for the next pass.
///
/// # Gather, then send per destination
///
/// Each round of 32 rows is one **gather phase** (holding the engine) and one
/// **chunk** of the pass: when the round is full or the hold exceeds
/// `DRAIN_HOLD_BUDGET`, the drainer releases the engine before the next round,
/// so a single hold does not grow with the backlog; local rows are processed
/// at once;
/// rows that must go through the transport become a [`Pending`] with their
/// `after` work (deliver the id, or forget the row) and go into the
/// [`Sender`]'s per-destination queue. The end of the round is **one** send
/// ticket for the whole round (`engine.sync_sends()`, IR-1/LF-1), then the queues are handed to
/// send tasks — each destination sequential, destinations in parallel — and
/// the drainer goes on gathering the next round while messages are in flight.
/// Whichever destination returns first has its `after` applied first, in
/// exactly the send order within the destination; a row is only deleted after
/// `200` (IR-2). At the end of the pass the drainer waits for every destination
/// still in flight before returning the watermark. The per-row decision
/// (`lc::drain::decide_row`/`decide_call`) does not change — only where `Send`
/// runs
/// changes.
///
/// # The returned watermark means "passed", not "done"
///
/// A stuck row stays, but the watermark still moves past it. That is **on
/// purpose**: if the watermark had to wait for stuck rows, a target not yet
/// deployed would pin every waiter behind it indefinitely — whereas the old
/// path, on meeting a stuck row, just skipped it and answered. The semantics
/// here are the old semantics, expressed as a number. Rows not re-read because
/// their lock is still busy were already passed by an earlier pass.
///
/// # A dead destination only holds back **its own** rows
///
/// When a send to a failed partition fails, that partition goes into the pass's
/// `down` ledger: that row and every later row **to the same destination** stay
/// in place (their relative order kept — the next pass re-reads them), while
/// rows to other destinations and local rows carry on. The previous version
/// abandoned the whole pass on the first send error, and since the next pass
/// started again from 0, a partition that lost its leader for good stalled
/// **every** row behind it from that sender — an hour of chaos lost 7 acked
/// invocations down exactly that path, with both caller and callee on healthy
/// partitions.
pub async fn drain_outbox<'c>(engine: &mut Lease, ctx: &Ctx<'c>) -> Result<u64> {
    /// Rows read per round. A cap on the cost of one scan, not on visibility:
    /// the cursor carries on in the next round, so row 33 is no longer forgotten.
    const BATCH: usize = 32;

    drain_metrics::add(&drain_metrics::PASSES, 1);
    // The per-term ledger. Only returned when the pass has no error: every `?`
    // below drops the ledger, and the next pass scans again from 0. A ledger
    // with no row waiting for a lock saves nothing: drop it, and this pass scans
    // again from 0 — exactly the old path (a scan from 0 is cheap thanks to the
    // index watermark), whereas re-reading every processed row to see it
    // disappear is not.
    let mut seen = ctx.drainer.take_backlog();
    if !seen.has_parked() {
        seen = Backlog::default();
    }
    let mut handled = 0usize;
    // Invocations with a row skipped in this drain pass because their lock is
    // busy. Every later row of the same id is skipped too, so their relative
    // order is kept: the next pass meets them again in exactly creation order.
    let mut busy: Busy = Busy::default();
    // Destination ranges whose send failed in this pass. Later rows to the same
    // destination are skipped outright: that both keeps their relative order and
    // avoids paying another `SEND_TIMEOUT` wait for each row to a destination
    // already known not to answer. Keyed by range id: if the transport learns a
    // new table mid-pass, later rows compute their destination from the new
    // table, and a new range does not suffer because an old range failed.
    let mut down: Down = Down::default();
    // Declared **before** `sender`: drops run in reverse declaration order, so a
    // pass that fails midway cancels the send tasks first and only then
    // releases the caller locks.
    let mut held = Held::new(ctx.runner);
    let mut sender = Sender::new(ctx.transport);
    // The ledger's old rows to reconsider in the pass. When the lock holder is
    // still busy ⇒ its row does not go into the list, and the id goes into
    // `busy` through a failed lock attempt — exactly the "someone is waiting"
    // mark that the old read-then-try-lock left behind.
    let mut again: std::collections::VecDeque<(u64, Option<InvocationId>)> = seen
        .due(|id| {
            if !ctx.locks.is_busy(&id) {
                return true;
            }
            let lock = take_lock(ctx, &mut busy, &held, id);
            debug_assert!(
                lock.is_none(),
                "lock was just seen busy, no await in between"
            );
            false
        })
        .into();
    let mut drained = None;
    for _ in 0..MAX_OUTBOX_ROUNDS {
        // Read the upper bound **before** the read. Any row assigned a `seq`
        // after this point carries a number above `high`, so returning `high`
        // as the watermark never claims a row we have not seen.
        let high = engine.outbox_high_water().await?;
        let (pending, cursor) =
            gather_rows(engine, ctx, &mut seen, &mut again, &mut busy, &held, BATCH).await?;
        if pending.is_empty() {
            drained = Some(high);
            break;
        }
        let round = pending.len();
        for (row, entry) in pending {
            let seq = row.seq;
            let target = local_lock_target(&entry, engine);
            let blocked_by =
                match outbox_row(engine, ctx, &mut busy, &mut down, &mut held, row, entry).await? {
                    // Processed — still re-read on the next pass, until it is
                    // seen to be gone (the ledger does not trust "done", [`Backlog`]).
                    Gathered::Handled => {
                        handled += 1;
                        None
                    }
                    // Left because the lock was busy ⇒ wait for the holder to
                    // release it; any other reason ⇒ re-read on the next pass,
                    // as on the old path.
                    Gathered::Left => target.filter(|t| busy.contains(t)),
                    Gathered::Send(p) => {
                        if let Err(p) = sender.queue(engine, &down, p) {
                            left(&mut held, p);
                        }
                        None
                    }
                };
            seen.note(seq, blocked_by);
        }
        // Every live row below `cursor` went into the ledger above.
        seen.scanned_to(cursor);
        handled += sender.flush(engine, ctx, &mut held, &mut down).await?;
        // One gather round is one chunk: when the round is full or the hold
        // exceeds the budget ⇒ release the engine before the next round. The
        // round's rows are in the ledger or in the `Sender` — no row that was
        // read crosses the release point without being considered.
        if chunk_full(engine, round, BATCH) {
            between_chunks(engine).await;
        }
    }
    // Out of rows (or out of rounds): wait for every destination still in
    // flight before returning the watermark — a "passed" watermark must not
    // move past a row whose outcome is not yet known.
    handled += sender.finish(engine, ctx, &mut held, &mut down).await?;
    drain_metrics::add(&drain_metrics::ROWS_HANDLED, handled as u64);
    // The ledger is only used on the next pass when rows are still waiting for
    // a lock — only then does it need checking (a pass with no row waiting for a
    // lock makes the next pass scan again from 0).
    #[cfg(debug_assertions)]
    if seen.has_parked() {
        check_backlog(engine, &seen).await?;
    }
    let scanned = seen.scanned();
    ctx.drainer.keep_backlog(seen);
    if let Some(high) = drained {
        return Ok(high);
    }
    // Out of rounds. The old path returned an error here ("calls looping on each
    // other?") because it re-read the same snapshot over and over, so not
    // draining after 64 rounds really did mean stuck. A cursor advances
    // steadily, so running out of rounds only means **a lot of work** — a
    // fan-out of more than 2048 rows is valid. Poke ourselves again and return
    // the watermark: the next pass carries on, and meanwhile nobody is held at
    // the door. Every row below the scan cursor has been passed by some pass
    // (this one or an earlier one).
    tracing::debug!(
        handled = handled,
        "outbox still has rows after {MAX_OUTBOX_ROUNDS} rounds — draining continues next turn"
    );
    ctx.drainer.nudge();
    Ok(scanned.saturating_sub(1))
}

/// Reads one round of outbox rows (up to `limit` rows), in exactly `seq`
/// order: first the ledger's old rows still to be considered (`again`, every
/// `seq` below the scan cursor), each re-read from the store; once the old
/// rows are exhausted, the onward scan from the cursor. Returns the rows read
/// and the new scan cursor — every live row below it is either already in the
/// ledger or in what is returned.
///
/// An old row is read with [`Engine::outbox_from`] from its own `seq`, one row:
/// the read returns **the first live row** from that `seq`. Getting a different
/// row back (or nothing) means every ledger row in between is gone — they leave
/// the ledger at no extra read, and the row just read is used if it is the next
/// old row. Rows read are used in the same round, before the engine is released
/// — like the old path's 32-row scan.
async fn gather_rows<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    seen: &mut Backlog,
    again: &mut std::collections::VecDeque<(u64, Option<InvocationId>)>,
    busy: &mut Busy,
    held: &Held<'c>,
    limit: usize,
) -> Result<(Vec<(OutboxRef, OutboxEntry)>, u64)> {
    let mut out = Vec::new();
    // The latest read: from which `seq` it asked, and the first live row from there (if any).
    let mut probe: Option<(u64, Option<(OutboxRef, OutboxEntry)>)> = None;
    while out.len() < limit {
        let Some(&(seq, blocked_by)) = again.front() else {
            break;
        };
        // The lock holder is busy again in this pass (or just retook the lock
        // between two rounds): the row stays, unread — the failed lock attempt
        // already recorded the id in `busy` and the "someone is waiting" mark.
        if let Some(id) = blocked_by {
            if busy.contains(&id)
                || (!held.holds(&id)
                    && ctx.locks.is_busy(&id)
                    && take_lock(ctx, busy, held, id).is_none())
            {
                again.pop_front();
                continue;
            }
        }
        match &probe {
            Some((from, first)) if *from <= seq => match first {
                // No live row left from `from` ⇒ this row is gone.
                None => {
                    seen.forget(seq);
                    again.pop_front();
                }
                Some((row, _)) if row.seq > seq => {
                    seen.forget(seq);
                    again.pop_front();
                }
                Some((row, _)) if row.seq == seq => {
                    let (_, first) = probe.take().expect("just matched");
                    out.push(first.expect("just matched"));
                    again.pop_front();
                }
                // The first live row is a row we are skipping — ask again from `seq`.
                Some(_) => probe = None,
            },
            _ => {
                let mut got = engine.outbox_from(seq, 1).await?;
                drain_metrics::add(&drain_metrics::ROWS_READ, got.len() as u64);
                probe = Some((seq, got.pop()));
            }
        }
    }
    let mut cursor = seen.scanned();
    if out.len() < limit && again.is_empty() {
        let got = engine.outbox_from(cursor, limit - out.len()).await?;
        drain_metrics::add(&drain_metrics::ROWS_READ, got.len() as u64);
        if let Some((last, _)) = got.last() {
            cursor = last.seq + 1;
        }
        out.extend(got);
    }
    Ok((out, cursor))
}

/// Debug builds: checks the invariant of [`Backlog`] with a full scan at the end
/// of the pass — every outbox row still live below the scan cursor must be in
/// the ledger. A mismatch is a row the drainer would never read again until the
/// next leader change: panic, do not quietly carry on. This read does not count
/// toward
/// `cairnflow_outbox_rows_read_total`.
#[cfg(debug_assertions)]
async fn check_backlog(engine: &mut Lease, seen: &Backlog) -> Result<()> {
    let mut from = 0u64;
    while from < seen.scanned() {
        let rows = engine.outbox_from(from, 256).await?;
        let Some((last, _)) = rows.last() else {
            break;
        };
        from = last.seq + 1;
        for (row, _) in &rows {
            assert!(
                row.seq >= seen.scanned() || seen.contains(row.seq),
                "outbox row seq {} is live below scan cursor {} but missing from the drainer's ledger",
                row.seq,
                seen.scanned()
            );
        }
    }
    Ok(())
}

/// The gather phase of one outbox row.
async fn outbox_row<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    busy: &mut Busy,
    down: &mut Down,
    held: &mut Held<'c>,
    row: OutboxRef,
    entry: OutboxEntry,
) -> Result<Gathered> {
    // **Never wait for the lock of a turn in flight**. A row that must write
    // into a local invocation tries to lock it *before* any side effect; if it
    // is busy, the row is skipped (it stays in place, the watermark still moves
    // past it as with a stuck row) — the guard holding it will poke the drainer
    // on release. Waiting here means waiting for someone else's whole SDK turn,
    // and meanwhile no row of the whole partition is dispatched.
    // A schedule firing to another range does not go through the lifecycle
    // core: it belongs to no invocation of this range and locks no one.
    if let OutboxEntry::Fire(f) = entry {
        return outbox_fire(engine, ctx, down, row, f).await;
    }
    let remote = remote_destination(&entry, engine);
    let lock_target = local_lock_target(&entry, engine);
    let mut view = lc::drain::RowView {
        kind: match &entry {
            OutboxEntry::Call(_) => lc::drain::RowKind::Call,
            OutboxEntry::Attach(_) => lc::drain::RowKind::Attach,
            OutboxEntry::PeekOutput(_) => lc::drain::RowKind::Peek,
            OutboxEntry::Reply(_) => lc::drain::RowKind::Reply,
            OutboxEntry::Cancel(_) => lc::drain::RowKind::Cancel,
            OutboxEntry::Fire(_) => unreachable!("Fire rows branch off above"),
        },
        remote: remote.is_some(),
        dest_down: remote.is_some_and(|d| down.contains(&d)),
        lock: match lock_target {
            Some(_) => lc::drain::RowLock::Untried,
            None => lc::drain::RowLock::Unneeded,
        },
    };
    let mut lock = None;
    let send = loop {
        match lc::drain::decide_row(&view) {
            lc::drain::RowAction::Leave => return Ok(Gathered::Left),
            lc::drain::RowAction::TryLock => {
                lock = lock_target.and_then(|t| take_lock(ctx, busy, held, t));
                view.lock = match lock {
                    Some(_) => lc::drain::RowLock::Held,
                    None => lc::drain::RowLock::Busy,
                };
            }
            lc::drain::RowAction::HandleCall | lc::drain::RowAction::Local => break false,
            lc::drain::RowAction::Send => break true,
        }
    };
    if send {
        // Send through the transport: the message is built now, the local work after the ack is forgetting the row.
        let msg = match entry {
            OutboxEntry::Call(_) => unreachable!("Call rows go through decide_call"),
            OutboxEntry::Fire(_) => unreachable!("Fire rows branch off above"),
            // Waiting on / watching the result of another invocation on another
            // partition: forward the message, and only forget the outbox row once
            // the other side confirms it is durable. The message carries the
            // waiter's tenant: the target range only answers a target of the same
            // tenant. One point read of metadata — a rare row.
            OutboxEntry::Attach(w) => InterPartition::Attach(Watch {
                target: w.target,
                caller: w.caller,
                completion_id: w.completion_id,
                tenant: cairn_engine::wire_tenant(engine.tenant_of(&w.caller).await?.as_ref()),
            }),
            OutboxEntry::PeekOutput(w) => InterPartition::PeekOutput(Watch {
                target: w.target,
                caller: w.caller,
                completion_id: w.completion_id,
                tenant: cairn_engine::wire_tenant(engine.tenant_of(&w.caller).await?.as_ref()),
            }),
            OutboxEntry::Reply(r) => InterPartition::Completion(DeliverCompletion {
                target: r.caller,
                completion_id: r.completion_id,
                result: r.result.into(),
            }),
            // A child on another partition: the same mould as `Reply` — the row
            // may only be forgotten once the other side has written it durably.
            OutboxEntry::Cancel(c) => InterPartition::Cancel(cairn_engine::DeliverCancel {
                target: c.target,
                parent: c.caller,
                id_completion: c.id_completion,
            }),
        };
        return Ok(Gathered::Send(Box::new(Pending {
            msg,
            row: SourceRow::Outbox(row),
            after: After::Forget,
        })));
    }
    match entry {
        OutboxEntry::Fire(_) => unreachable!("Fire rows branch off above"),
        OutboxEntry::Call(c) => outbox_call(engine, ctx, down, held, row, c, lock).await,
        // A target on the same partition is resolved locally.
        OutboxEntry::Attach(w) => {
            outbox_attach(
                engine,
                ctx,
                held,
                row,
                w,
                lock.expect("local attach holds the waiter lock"),
            )
            .await
        }
        OutboxEntry::PeekOutput(w) => {
            outbox_peek(
                engine,
                ctx,
                held,
                row,
                w,
                lock.expect("local peek holds the waiter lock"),
            )
            .await
        }
        OutboxEntry::Reply(r) => {
            outbox_reply(
                engine,
                ctx,
                held,
                row,
                r,
                lock.expect("local reply holds the caller lock"),
            )
            .await
        }
        OutboxEntry::Cancel(c) => {
            let lock = lock.expect("local cancel holds the child lock");
            cancel_child_held(engine, ctx, lock.guard(held), c.target).await?;
            engine.drop_outbox(row).await?;
            Ok(Gathered::Handled)
        }
    }
}

/// The `Fire` row of a schedule: creates the run on the target's range with the
/// row's idempotency key — locally if the target is on this range, otherwise
/// sends a `StartCall` (`reply_to: None`, `sender = (schedule id, fire_seq)`)
/// and only forgets the row once the target range has written the message
/// durably. The target range deduplicates by key, so redelivery after a crash
/// does not create a second run (CG-10).
async fn outbox_fire<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    down: &mut Down,
    row: OutboxRef,
    f: cairn_engine::OutboundFire,
) -> Result<Gathered> {
    // Broken tenant ⇒ create nothing: the row stays (like `dispatch_call`).
    let Some(tenant) = tenant_of_wire(f.tenant.as_deref(), &f.schedule) else {
        return Ok(Gathered::Left);
    };
    let scope = parse_ident(f.scope.as_deref(), "scope", &f.schedule, Scope::new);
    let limit_key = parse_ident(
        f.limit_key.as_deref(),
        "limit key",
        &f.schedule,
        LimitKey::parse,
    );
    let target = Target {
        tenant,
        scope,
        service: f.service.clone(),
        handler: f.handler.clone(),
        object_key: f.object_key.clone(),
    };
    // The registry does not have the service yet ⇒ `EXCLUSIVE` or not is unknown: wait for it to catch up.
    if deployment_for(ctx.deployments, &target).is_none() {
        return Ok(Gathered::Left);
    }
    let Some(key) = cairn_store::partition_key_for_target(
        &target.tenant,
        target.scope(),
        &f.service,
        &f.object_key,
        Some(&f.idempotency_key),
    ) else {
        unreachable!("an idempotency key always yields a partition key");
    };
    let callee = cairn_store::idempotent_invocation_id(
        key,
        &target.tenant,
        target.scope(),
        &f.service,
        &f.handler,
        &f.object_key,
        &f.idempotency_key,
    );
    let header = cairn_engine::schedule::header_value(&f.name, f.at);
    if !engine.owns_key(key) {
        let dest = engine.cluster().partition_of_key(key);
        if down.contains(&dest) {
            return Ok(Gathered::Left);
        }
        let msg = InterPartition::StartCall(StartCall {
            callee,
            service: f.service,
            handler: f.handler,
            object_key: f.object_key,
            parameter: f.parameter,
            reply_to: None,
            idempotency_key: Some(f.idempotency_key),
            scope: f.scope,
            limit_key: f.limit_key,
            tenant: f.tenant,
            sender: (f.schedule, f.fire_seq),
            schedule: Some(header),
            lineage: Default::default(),
        });
        return Ok(Gathered::Send(Box::new(Pending {
            msg,
            row: SourceRow::Outbox(row),
            after: After::Forget,
        })));
    }
    // Target on the same range: create locally (L0 throttle like a local child
    // call), then forget the row. Two commits: a crash in between makes the next
    // drain meet the existing idempotency key ⇒ it attaches, without creating again.
    let admitted = admit::admit(
        engine,
        ctx,
        admit::Source::Call { reply_to: None },
        admit::Request {
            id: callee,
            target: &target,
            limit_key: limit_key.as_ref(),
            idempotency_key: Some(&f.idempotency_key),
            input: f.parameter.clone(),
            log_level: None,
            tags: &[],
            sender: None,
            root_id: None,
            schedule: Some(cairn_engine::ScheduleStamp {
                header: &header,
                row: None,
            }),
        },
    )
    .await;
    // A firing that can **never** be created (target key too long, broken
    // identifier — a schedule written before the `PUT` cap existed): drop the
    // row, without returning an error for the whole drain pass — `?` here used
    // to abandon the whole pass, and the next pass met exactly this row again.
    // Transient errors (store, lease) still propagate as before.
    let admission = match admitted {
        Ok(a) => a,
        Err(ActorError::Engine(cairn_engine::EngineError::Store(
            e @ (cairn_store::StoreError::KeyTooLong { .. } | cairn_store::StoreError::Ident(_)),
        ))) => {
            tracing::warn!(schedule = %hex(&f.schedule), error = %e, "dropping a schedule fire that can never be created");
            crate::metrics::schedules::fire("refused");
            engine.drop_outbox(row).await?;
            return Ok(Gathered::Handled);
        }
        Err(e) => return Err(e),
    };
    use admit::Admission as A;
    match admission {
        A::Created { held, .. } => {
            engine.drop_outbox(row).await?;
            if held.is_none() {
                ctx.runner.queue(callee);
            }
            Ok(Gathered::Handled)
        }
        A::Attached { .. } => {
            engine.drop_outbox(row).await?;
            Ok(Gathered::Handled)
        }
        A::Shed | A::NotYet => Ok(Gathered::Left),
        A::Answered(_) | A::Rejected(_) | A::Refused(_) | A::Admitted => {
            unreachable!("a Call source never gets an ingress/restart reply")
        }
    }
}

/// A `Call` row: create (or find) the callee, deliver the id to the caller —
/// locally at once, or after the target range's ack.
async fn outbox_call<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    down: &mut Down,
    held: &mut Held<'c>,
    row: OutboxRef,
    c: cairn_engine::OutboundCall,
    mut lock: Option<Lock<'c>>,
) -> Result<Gathered> {
    let mut view = lc::drain::CallView {
        awaits_result: c.result_completion.is_some(),
        caller: None,
        dispatch: None,
    };
    let mut dispatched = None;
    loop {
        match lc::drain::decide_call(&view) {
            lc::drain::CallAction::ReadCaller => {
                view.caller = Some(life::status_of(engine.status(&c.caller).await?.as_ref()));
            }
            // **Do not start work whose result nobody will read.**
            //
            // When the caller has stopped for good (cancelled, mostly), a
            // `ctx.call` still sitting in the outbox is only a model call with
            // no recipient — and in an agent workload, that is a bill with no
            // recipient.
            //
            // `ctx.send` **still runs**: it declares no result completion, so
            // the caller never waited for it. It is detached work, along exactly
            // the boundary that `Engine::live_children` cuts.
            lc::drain::CallAction::DropStale => {
                engine.drop_outbox(row).await?;
                return Ok(Gathered::Handled);
            }
            lc::drain::CallAction::Dispatch => {
                let d = dispatch_call(engine, ctx, &c).await?;
                view.dispatch = Some(d.as_ref().map(|d| lc::drain::CallDispatch {
                    remote_down: d.elsewhere.map(|dest| down.contains(&dest)),
                    created: d.created,
                }));
                dispatched = d;
            }
            // Cannot be created yet (the service is not yet in this node's
            // deployment registry), or the target is on a range whose send
            // failed in this pass. The row stays where it was; the next tick
            // retries.
            lc::drain::CallAction::Leave => return Ok(Gathered::Left),
            // Return the id to the caller and delete the outbox row in the same commit.
            lc::drain::CallAction::DeliverLocal { queue_callee } => {
                let dispatch = dispatched.expect("dispatched");
                let callee = dispatch.callee;
                deliver_local(
                    engine,
                    ctx,
                    held,
                    lock.take()
                        .expect("a Call row always holds the caller lock"),
                    c.caller,
                    Completion {
                        id: c.id_completion,
                        result: CompletionResult::CallInvocationId(hex(&callee)),
                    },
                    row,
                )
                .await?;
                dispatch.count();
                if queue_callee {
                    // **Dispatch, do not run.** The drain loop creates the
                    // callee and hands it to the scheduler; running it is the
                    // job of a task waiting for its own slot. This is where the
                    // child's SDK turn leaves the drain path.
                    //
                    // Hand it out only if we are the one who created it. If we
                    // attached to an invocation that is already running, do not
                    // touch it — it has its own pace.
                    ctx.runner.queue(callee);
                }
                return Ok(Gathered::Handled);
            }
            // Target on another partition: the order is reversed — send the
            // message, and **only once the receiver confirms it is durably
            // written** deliver the id to the caller and delete the outbox row.
            // Acking earlier would forget a call that nobody has received yet.
            // The caller lock travels with the row until then ([`Held`]).
            lc::drain::CallAction::SendRemote => {
                let dispatch = dispatched.expect("dispatched");
                let msg = InterPartition::StartCall(StartCall {
                    callee: dispatch.callee,
                    service: c.service.clone(),
                    handler: c.handler.clone(),
                    object_key: c.object_key.clone(),
                    parameter: c.parameter.clone(),
                    reply_to: c.result_completion.map(|cid| (c.caller, cid)),
                    idempotency_key: c.idempotency_key.clone(),
                    // The scope must travel along: it is **part of the
                    // identity**, and the target partition writes
                    // state/promise/idempotency by it. Forgetting it means
                    // writing into someone else's cell.
                    scope: c.scope.clone(),
                    limit_key: c.limit_key.clone(),
                    // The tenant too — the callee is in the caller's tenant.
                    tenant: c.tenant.clone(),
                    sender: (c.caller, c.id_completion),
                    schedule: None,
                    // Lineage travels along: a callee on another range receives the root and tags.
                    lineage: Box::new(c.lineage.clone()),
                });
                held.adopt(
                    c.caller,
                    lock.take()
                        .expect("a Call row always holds the caller lock"),
                );
                return Ok(Gathered::Send(Box::new(Pending {
                    msg,
                    row: SourceRow::Outbox(row),
                    after: After::Deliver {
                        caller: c.caller,
                        completion: Completion {
                            id: c.id_completion,
                            result: CompletionResult::CallInvocationId(hex(&dispatch.callee)),
                        },
                        dispatch,
                    },
                })));
            }
        }
    }
}

/// The target's status for a local wait/watch row, seen from the waiter's
/// tenant: a different tenant ⇒ `None`, as if it did not exist.
async fn watched_status(
    engine: &Lease,
    w: &cairn_engine::OutboundWatch,
) -> Result<Option<InvocationStatus>> {
    Ok(match engine.tenant_of(&w.caller).await? {
        Some(t) => engine.visible_status(&t, &w.target).await?,
        None => None,
    })
}

/// A local `Attach` row.
async fn outbox_attach<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    held: &mut Held<'c>,
    row: OutboxRef,
    w: cairn_engine::OutboundWatch,
    lock: Lock<'c>,
) -> Result<Gathered> {
    match watched_status(engine, &w).await? {
        Some(s) if s.is_terminal() => {
            let Some(result) = terminal_result(engine, &w.caller, s).await else {
                return Ok(Gathered::Left);
            };
            deliver_local(
                engine,
                ctx,
                held,
                lock,
                w.caller,
                Completion {
                    id: w.completion_id,
                    result,
                },
                row,
            )
            .await?;
        }
        Some(_) => {
            engine.attach(&w.target, &w.caller, w.completion_id).await?;
            engine.drop_outbox(row).await?;
        }
        None => {
            deliver_local(
                engine,
                ctx,
                held,
                lock,
                w.caller,
                Completion {
                    id: w.completion_id,
                    result: watch_not_found(&w.target),
                },
                row,
            )
            .await?;
        }
    }
    Ok(Gathered::Handled)
}

/// A local `PeekOutput` row.
async fn outbox_peek<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    held: &mut Held<'c>,
    row: OutboxRef,
    w: cairn_engine::OutboundWatch,
    lock: Lock<'c>,
) -> Result<Gathered> {
    let result = match watched_status(engine, &w).await? {
        Some(s) if s.is_terminal() => match terminal_result(engine, &w.caller, s).await {
            Some(r) => r,
            None => return Ok(Gathered::Left),
        },
        Some(_) => CompletionResult::Void,
        None => watch_not_found(&w.target),
    };
    deliver_local(
        engine,
        ctx,
        held,
        lock,
        w.caller,
        Completion {
            id: w.completion_id,
            result,
        },
        row,
    )
    .await?;
    Ok(Gathered::Handled)
}

/// The result of terminal status `s` for waiter `caller` on this range: a
/// segmented output goes through [`super::values::deliver_output`].
/// `None` = could not be delivered yet (store error) — the row stays, the next
/// tick retries.
async fn terminal_result(
    engine: &mut Lease,
    caller: &InvocationId,
    s: InvocationStatus,
) -> Option<CompletionResult> {
    match s {
        InvocationStatus::Completed(cairn_engine::InvocationOutput::Segmented(o)) => {
            match super::values::deliver_output(engine, caller, o).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "segmented output not delivered yet");
                    None
                }
            }
        }
        s => Some(status_completion(s)),
    }
}

/// A local `Reply` row: the result for a caller on the same partition.
async fn outbox_reply<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    held: &mut Held<'c>,
    row: OutboxRef,
    r: cairn_engine::OutboundReply,
    lock: Lock<'c>,
) -> Result<Gathered> {
    let result = match r.result {
        ReplyResult::Value(v) => CompletionResult::CallValue(v),
        // Segmented output: assembled inline or COPYed under the caller's
        // owner, outside the engine. Failure ⇒ the row stays, the next tick retries.
        ReplyResult::Segmented(o) => {
            match super::values::deliver_output(engine, &r.caller, o).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "segmented reply not delivered yet");
                    return Ok(Gathered::Left);
                }
            }
        }
        ReplyResult::Failure { code, message } => CompletionResult::CallFailure(pb::Failure {
            code,
            message,
            metadata: vec![],
        }),
    };
    deliver_local(
        engine,
        ctx,
        held,
        lock,
        r.caller,
        Completion {
            id: r.completion_id,
            result,
        },
        row,
    )
    .await?;
    Ok(Gathered::Handled)
}

/// If signals are still waiting to be dispatched, wait for **one complete drain pass** of the drainer.
///
/// Signal rows have no `seq`, so they cannot be waited on by watermark like the
/// outbox; but a pass that starts after the time of asking has certainly seen
/// every row created before it. With no signal waiting (nearly always),
/// nothing is waited for.
pub(super) async fn wait_spawned_signals(engine: &mut Lease, ctx: &Ctx<'_>) -> Result<()> {
    if !engine.signals_from(0, 1).await?.is_empty() {
        ctx.drainer.wait_full_pass(engine).await;
    }
    Ok(())
}

/// Waits for the drainer to finish **the work spawned by `id`'s turn**.
///
/// This is the synchronous shape that `/invoke`, `/resume` and the signal door
/// need: they answer someone who is waiting, so the work spawned by that call
/// must finish before the answer. They used to drain by themselves — and so
/// every call with a `ctx.call` closed the whole partition's entry door while
/// it drained.
///
/// **Ask exactly your own question.** The first version of this function waited
/// for the whole partition's `seq` ceiling, and it was wrong in a way only a
/// measurement showed: a handler that issues no command — a read door, a health
/// check, a `Ledger/spend` — **spawns no row**, so it has nothing to wait for.
/// Making it wait for the shared ceiling makes it queue behind an unrelated
/// fan-out. Measured on the `#[ignore]` bench: 56 ms when idle versus 1.41 s
/// while a fan-out was draining, and all of those 1.41 s were in this wait.
///
/// What changes is **who does the work**, not who waits: here we compare a
/// number we hold with a published watermark, and [`Lease::released`] releases
/// the engine during the comparison — so the drainer has the engine to make
/// progress. Holding the engine while waiting for someone who needs the engine
/// is a deadlock, not slowness.
///
/// Hitting the cap is not an error: the caller re-reads the durable state and
/// answers by it — the same answer the old path gave when it met a stuck row.
pub(super) async fn sync_outbox_of(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
) -> Result<()> {
    let drained = ctx.drainer.mark();
    match engine.spawned_after(&id, drained) {
        Some(mine) => {
            ctx.drainer.wait_for(engine, mine).await;
        }
        // This turn spawned no row. Still poke — there may be someone else's
        // rows — but there is no reason to stand by and watch whether it finished.
        None => ctx.drainer.nudge(),
    }
    Ok(())
}

/// Like [`sync_outbox_of`], but waits for **the whole partition** to drain up to now.
///
/// For places that create outbox rows **outside a turn** — `Engine::cancel`,
/// `complete_promise_external` — so there is no turn to credit. Wider than
/// needed, and acceptable because they are operational paths, not hot paths:
/// a cancel command waiting one extra drain tick does not change the shape of
/// the system.
pub async fn sync_outbox(engine: &mut Lease, ctx: &Ctx<'_>) -> Result<()> {
    let mine = engine.outbox_high_water().await?;
    ctx.drainer.wait_for(engine, mine).await;
    Ok(())
}

/// Cancels a running child on **this partition**.
///
/// Locks the invocation first, exactly the rule of `deliver_to_caller`: if
/// another turn slips in between "seen still alive" and "write Failed", the two
/// overwrite each other.
///
/// The cancellation keeps propagating down without recursion here:
/// `Engine::cancel` queues cancel commands for *its children* in the outbox,
/// and this `drain_outbox` loop picks them up next. It stops because
/// cancelling an invocation that is already terminal is a no-op — it creates
/// no new rows.
///
/// `target`'s lock is already held: the drainer only *tries* the lock without
/// waiting, so the caller takes it with `try_invocation` first.
pub(super) async fn cancel_child_locked<'c>(
    engine: &mut Lease,
    ctx: &Ctx<'c>,
    guard: InvGuard<'c>,
    target: InvocationId,
) -> Result<()> {
    cancel_child_held(engine, ctx, &guard, target).await
}

/// Like [`cancel_child_locked`], with `target`'s lock held and released by the
/// caller (the outbox drainer may be holding it across a send — [`Held`]).
async fn cancel_child_held(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    _guard: &InvGuard<'_>,
    target: InvocationId,
) -> Result<()> {
    // Every outcome other than `Cancelled` — already done, no longer there — is
    // normal: the child finished just before the cancel arrived. Nothing to do.
    //
    // The same path as cancelling through the admin API (core
    // `Event::AdminCancel`): leave the queue, then return the VO lock. Without
    // this step the lock of an `EXCLUSIVE` child stays in RAM until the next
    // leader change, and every other call on the same key waits on a dead
    // invocation (`tests/stale_state.rs`).
    if let cairn_engine::CancelOutcome::Cancelled =
        life::cancel(engine, ctx, target, "parent cancelled").await?
    {
        tracing::debug!(child = %hex(&target), "cancel propagated to child");
    }
    Ok(())
}
