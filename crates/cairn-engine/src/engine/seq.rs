// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Issuing queue/outbox/message seqs, and reading/cleaning the outbox.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// The next seq — the place in the queue of an invocation about to be created.
    ///
    /// **One counter for the whole partition, not one per queue.** `seq` only
    /// needs two properties: unique within a queue, and increasing in queue
    /// order. One shared counter gives both, and it gives something more
    /// important: issuing a number does not touch the store.
    ///
    /// The first version derived it from the queue itself (`max(seq) + 1`) so as
    /// to keep nothing in RAM. That was right for the old `inbox` — the queue
    /// there was *one object key*, so it was short. It is plainly wrong for
    /// vqueues: a SHARED queue gathers **every** invocation of a `(scope, limit
    /// key, service)`, so that scan is O(pending work) **for every creation** —
    /// exactly the hot path.
    ///
    /// Keeping it in RAM does not break INV-3 because it is **not pending
    /// work**: it is a rebuildable number, and [`Self::all_vqueues`] — the scan
    /// that runs once anyway on becoming leader — is where it is rebuilt.
    /// Seeded *lazily* here rather than making the call site remember to call
    /// an initialiser: forgetting would issue a duplicate `seq`, i.e. overwrite
    /// a live entry, and it would only show at the leader change after next.
    ///
    /// **Seeded from a durable watermark**: the writer knows for sure the
    /// largest `seq` of the queue index (`floors::QUEUE_HIGH` — the mark
    /// matches, the derived zone was rebuilt, or the lock rebuild has run to
    /// the end, [`Self::finish_restore`]), so creations during the chunked lock
    /// rebuild wait for no scan. When it does not know for sure, the shell
    /// rebuilds the locks **before** the first command
    /// ([`Self::queue_seq_primed`]), and the scanning branch below remains only
    /// for tests and tools that open a bare engine.
    pub async fn next_queue_seq(&mut self, _queue: &QueueName) -> Result<u64> {
        let next = match (self.next_seq, self.writer.queue_seq_high()) {
            (Some(n), _) => n,
            (None, Some(high)) => high + 1,
            (None, None) => self
                .writer
                .all_vqueues_unbounded()
                .await?
                .iter()
                .map(|(_, _, _, seq, _)| *seq)
                .max()
                .map_or(1, |m| m + 1),
        };
        self.next_seq = Some(next + 1);
        Ok(next)
    }

    /// The queue `seq` counter is available without any scan — creations can
    /// be served while the VO lock rebuild is still running.
    pub fn queue_seq_primed(&self) -> bool {
        self.next_seq.is_some() || self.writer.queue_seq_high().is_some()
    }

    /// The `seq` for an outbox row — **the old one if the row exists**, a new
    /// one otherwise.
    ///
    /// The "old one if it exists" part is D-O2, and it is not an optimisation:
    /// the state machine replays tasks for every completion still pending on
    /// *every* replay, so `push_outbox` hitting an existing row is the everyday
    /// path. Issuing a new `seq` there leaves two index rows pointing at one
    /// main row, and the drainer — which reads the index — sends that call
    /// **twice**. The main table's deduplication cannot save it, because it
    /// deduplicates by *identity*, not by the number of times it is seen.
    ///
    /// Keeping the counter and the map in RAM does not break INV-3, exactly the
    /// argument of [`Self::next_queue_seq`]: they are numbers **rebuildable**
    /// from a scan, not pending work. The pending work stays intact in the two
    /// tables.
    ///
    /// Seeded **lazily** here rather than making the call site remember to call
    /// an initialiser — the same reason, and the same consequence if forgotten:
    /// a duplicate `seq`, i.e. one index row overwriting a live index row, i.e.
    /// a call that vanishes; and it would only show at the leader change after
    /// next.
    async fn next_outbox_seq(&mut self, owner: PartitionKey, key: OutboxKey) -> Result<u64> {
        self.prime_outbox().await?;
        if let Some(seq) = self.outbox.of.get(&(owner, key)) {
            return Ok(*seq);
        }
        let seq = self.next_message_seq().await?;
        self.outbox.of.insert((owner, key), seq);
        Ok(seq)
    }

    /// A new `seq` for a row with an ordering index — outbox, signal and
    /// inbound **share** one partition counter.
    ///
    /// Shared rather than separate because nobody needs separate ones: `seq`
    /// only needs to be unique within an index and increase in production
    /// order; one counter for all three is seeded once. The
    /// [`Self::outbox_high_water`] watermark therefore also counts signals and
    /// inbound — harmless: it is the ceiling "every row produced is ≤ this
    /// number", and that ceiling still holds.
    ///
    /// Signals and inbound do **not** reuse the old `seq` when rewriting the
    /// same row (there is no map like the outbox's D-O2): the index row of the
    /// earlier write becomes a stale row, and the drainer recognises it because
    /// the `seq` in the entity row has changed.
    pub(super) async fn next_message_seq(&mut self) -> Result<u64> {
        self.prime_outbox().await?;
        let seq = self
            .outbox
            .next
            .expect("prime_outbox set it on the line above");
        self.outbox.next = Some(seq + 1);
        Ok(seq)
    }

    /// Rebuilds the counter and the map from the three ordering indexes. Runs
    /// once, reads only the derived zone, with one scan covering all three
    /// indexes.
    ///
    /// There is no migration path: a bucket of an old binary is rejected by
    /// `DATA_FORMAT` before it gets here (D19).
    async fn prime_outbox(&mut self) -> Result<()> {
        if self.outbox.next.is_some() {
            return Ok(());
        }
        let heads = self.writer.seq_heads_unbounded().await?;
        let max = heads.max;
        self.outbox.of = heads
            .outbox
            .into_iter()
            .map(|(seq, owner, key)| ((owner, key), seq))
            .collect();
        self.outbox.next = Some(max + 1);
        Ok(())
    }

    /// Stages one outbox row into `muts` — index and main row, same commit.
    ///
    /// The **only** path by which the engine writes the outbox, and it exists
    /// so that `seq` cannot be forgotten: [`Mutations::push_outbox`] requires a
    /// `seq`, and the only place that issues a `seq` under the D-O2 rule is
    /// [`Self::next_outbox_seq`].
    ///
    /// `owner_pk` is the pk of the entity that owns the row — the call site
    /// states it.
    pub(super) async fn stage_outbox(
        &mut self,
        muts: &mut Mutations,
        owner_pk: PartitionKey,
        key: OutboxKey,
        payload: &[u8],
    ) -> Result<u64> {
        let seq = self.next_outbox_seq(owner_pk, key).await?;
        muts.push_outbox(owner_pk, seq, &key.0, key.1, payload);
        Ok(seq)
    }

    /// [`Self::stage_outbox`], and **credits it to `spawner`'s turn**.
    ///
    /// Every row produced in a turn is a consequence of that turn — child
    /// calls, results returned up to the parent, results for waiters. That is
    /// exactly the set the promise "`/invoke` answers after the work it spawned
    /// has finished" refers to, so it is the set worth crediting.
    pub(super) async fn stage_outbox_for(
        &mut self,
        spawner: InvocationId,
        muts: &mut Mutations,
        owner_pk: PartitionKey,
        key: OutboxKey,
        payload: &[u8],
    ) -> Result<()> {
        let seq = self.stage_outbox(muts, owner_pk, key, payload).await?;
        let slot = self.spawned.entry(spawner).or_insert(0);
        *slot = (*slot).max(seq);
        Ok(())
    }

    /// The highest `seq` produced by `id`'s turn **that the drain watermark has
    /// not passed yet**.
    ///
    /// Why ask separately instead of waiting for the whole partition's
    /// ceiling: the promise is about the work *this invocation* spawned, not
    /// about other people's work. A handler that issues no command — exactly
    /// the shape of a read endpoint, and exactly the shape of a `Ledger/spend`
    /// call in the benchmark workload — spawns no row, so it **has nothing to
    /// wait for**. Making it wait for the shared ceiling means queueing it
    /// behind an unrelated fan-out; that is exactly the 14× slowdown that was
    /// measured, and removing it needs nothing more elaborate.
    ///
    /// **Asking does not delete.** The earlier version forgot the entry as soon
    /// as it answered, so when two paths waited for the same invocation
    /// (`/resume` and a signal arriving at the same time), the later asker got
    /// `None` and answered before the work finished. An entry is only forgotten
    /// once the drain watermark has passed it — at that point nobody needs to
    /// wait for it anymore.
    pub fn spawned_after(&mut self, id: &InvocationId, drained: u64) -> Option<u64> {
        let mine = self.spawned.get(id).copied();
        if mine.is_some_and(|seq| seq <= drained) {
            self.spawned.remove(id);
        }
        // A **soft** cap: clean up when the map has grown, not on every ask.
        // The cleanup is O(n) and this question is on the `/invoke` hot path.
        const SOFT_CAP: usize = 4096;
        if self.spawned.len() > SOFT_CAP {
            self.spawned.retain(|_, seq| *seq > drained);
        }
        mine.filter(|seq| *seq > drained)
    }

    /// Forgets an outbox row in `muts`, and removes it from the map.
    ///
    /// Two things in one place because forgetting the second is a slow memory
    /// leak: the map must shrink with the number of pending rows, not follow
    /// history.
    pub(super) fn stage_outbox_removal(&mut self, muts: &mut Mutations, row: OutboxRef) {
        muts.pop_outbox(row.owner, row.seq, &row.key.0, row.key.1);
        self.outbox.of.remove(&(row.owner, row.key));
    }

    /// Pending work in the outbox **from `from_seq` on**, in production order.
    ///
    /// Reads by cursor, not by snapshot: the main table is ordered by
    /// invocation id, so its "first 32 rows" have no relation to production
    /// order, and row 33 would not be seen until the next round. The index
    /// advances steadily — the drainer keeps the last `seq` it processed and
    /// reads on from there.
    ///
    /// Stale index rows met on the way are cleaned up right away (a separate commit).
    pub async fn outbox_from(
        &mut self,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(OutboxRef, OutboxEntry)>> {
        let got = self.writer.outbox_from(from_seq, limit).await?;
        self.forget_stale(got.stale).await?;
        let mut out = Vec::new();
        for (seq, owner, key, raw) in got.rows {
            out.push((OutboxRef { seq, owner, key }, OutboxEntry::decode(raw)?));
        }
        Ok(out)
    }

    /// Pending work in the outbox from the start — [`Self::outbox_from`] with cursor 0.
    pub async fn pending_outbox(&mut self, limit: usize) -> Result<Vec<(OutboxRef, OutboxEntry)>> {
        self.outbox_from(0, limit).await
    }

    /// Cleans up stale index rows a read just met. Nothing to clean ⇒ commits
    /// nothing.
    pub(super) async fn forget_stale(&mut self, stale: Vec<Vec<u8>>) -> Result<()> {
        if stale.is_empty() {
            return Ok(());
        }
        tracing::debug!(rows = stale.len(), "removed stale index rows");
        let mut muts = Mutations::new();
        for key in stale {
            muts.forget_index_row(key);
        }
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// The highest `seq` **issued** so far. Every row produced is ≤ this number.
    ///
    /// This is the number someone who needs to synchronise keeps: waiting for
    /// the drained watermark to pass it is waiting for exactly "everything I
    /// just spawned has finished". Comparing a number you hold against a
    /// published watermark **cannot miss a wake-up** — unlike registering to
    /// wait for an event, which always has a window between observing and
    /// registering.
    pub async fn outbox_high_water(&mut self) -> Result<u64> {
        self.prime_outbox().await?;
        Ok(self.outbox.next.expect("prime_outbox just set it") - 1)
    }
}
