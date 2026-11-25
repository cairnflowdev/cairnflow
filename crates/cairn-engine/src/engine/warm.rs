// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The warm machine: the caller's state machine kept in RAM between drainer
//! deliveries of completions, instead of reloading and replaying the whole
//! journal every time.
//!
//! **The warm machine is only a cache; the source of truth is the journal in
//! the store.** What makes it correct is three things, none of which relies on
//! "remembering to evict in the right place":
//!
//! 1. **A pure lemma** (`cairn-runtime/tests/warm_lemma.rs`): a machine that
//!    replayed `J` and then received completion `c` equals a machine that
//!    replays `J ++ notif(c)` — in the part of the state the journal decides
//!    (`InvocationMachine::divergence`).
//! 2. **Checking the tail before use** ([`Engine::settle_machine`]): entry `L`
//!    (`L` = the number of entries the machine knows) is **absent** from the
//!    store, entry `L − 1` and entry 0 **match byte for byte**, the hash-chain
//!    head matches. The engine is the only writer of the range Db (INV-4) and
//!    the journal is append-only, so a journal in the store that differs from
//!    the machine is either longer (a turn has appended — entry `L` is
//!    present), or lacks a tail the machine has (an earlier delivery commit
//!    did not reach the store — entry `L − 1` absent or different), or is a
//!    different journal with the same id (purged and recreated — entry 0
//!    differs; and every place that writes entry 0, `create`/`restart`, as
//!    well as `purge`, evicts that id's machine). No match ⇒ drop the warm
//!    machine, load cold.
//! 3. **Cross-check mode** in every test build (`debug_assertions`) and in the
//!    `warm-check` build (`cairn-server --features testkit`): every use of the
//!    warm machine also loads cold and compares, a mismatch panics — a
//!    tripwire for the very argument in (2).
//!
//! The warm machine lives in the range's engine: a leader change, a split or a
//! restart means a new engine with an empty cache. Not shared between ranges,
//! no lock (the engine is `!Send`, one thread). A byte cap per range
//! ([`WARM_CAP_BYTES`]); going over the cap only costs speed (LRU drops the
//! oldest machine).
//!
//! **Only the drainer's delivery path stores machines** ([`Engine::keep_warm`],
//! after a successful `deliver`) and only that path uses them
//! ([`Engine::settle_machine`]). Turns always load cold (`load`, evicting the
//! id's machine). Getting a machine **takes it out**: any error between take
//! and store (a failed or ambiguous commit) leaves the cache without that id's
//! machine.

use super::*;

/// Byte cap of the warm machines per range (32 MiB) — the default of every
/// engine.
pub const WARM_CAP_BYTES: usize = 32 << 20;

/// Estimated overhead per journal entry (`JournalEntry` + its slot in the `Vec`).
const ENTRY_OVERHEAD: usize = 48;
/// Estimated overhead per machine (`Invocation`, the completion ledger).
const MACHINE_OVERHEAD: usize = 512;

/// Where a delivery's machine came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarmUse {
    /// A warm machine, durable tail matches.
    Hit,
    /// No warm machine: loaded cold.
    Cold,
    /// A warm machine whose durable tail did not match: dropped, loaded cold.
    Stale,
}

/// Cross-check mode on/off at run time — only meaningful in builds that have it
/// (`debug_assertions` or the `warm-check` feature). Benchmarks running the
/// `testkit` binary on an S3 profile turn it off (`CAIRNFLOW_WARM_CHECK=0`,
/// `cairn-server`) to measure the real product path.
static CHECK_OFF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Turns cross-check mode off (or back on) for the whole process.
pub fn set_warm_check(on: bool) {
    CHECK_OFF.store(!on, std::sync::atomic::Ordering::Relaxed);
}

/// This build has cross-check mode, and it is on.
pub fn warm_check_on() -> bool {
    cfg!(any(debug_assertions, feature = "warm-check"))
        && !CHECK_OFF.load(std::sync::atomic::Ordering::Relaxed)
}

/// Cap on the number of remembered wait trees per range. Full ⇒ remember no
/// more: that invocation is "unknown" ⇒ woken on every completion as before —
/// only costs speed.
const TREES_MAX: usize = 65_536;

/// One engine's warm-machine cache: LRU by bytes; plus the wait tree of each
/// invocation's last suspension.
pub(super) struct Warm {
    cap: usize,
    used: usize,
    tick: u64,
    slots: HashMap<InvocationId, Slot>,
    /// `tick` at store time → id; the smallest head is the oldest machine.
    order: std::collections::BTreeMap<u64, InvocationId>,
    /// The `Suspension` tree of the last suspension (the `suspended` commit of
    /// `Engine::step`), until a turn loads the journal (`load`) or the id is
    /// evicted. A rebuildable hint: lost ⇒ the drainer "does not know" and wakes.
    trees: HashMap<InvocationId, pb::SuspensionMessage>,
}

struct Slot {
    tick: u64,
    bytes: usize,
    inv: Invocation,
}

impl Warm {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            cap,
            used: 0,
            tick: 0,
            slots: HashMap::default(),
            order: Default::default(),
            trees: HashMap::default(),
        }
    }

    /// Remembers the wait tree of the suspension just committed.
    pub(super) fn wait_on(&mut self, id: InvocationId, tree: pb::SuspensionMessage) {
        if self.trees.len() < TREES_MAX || self.trees.contains_key(&id) {
            self.trees.insert(id, tree);
        }
    }

    /// Takes `id`'s machine out of the cache (if present).
    fn take(&mut self, id: &InvocationId) -> Option<Invocation> {
        let s = self.slots.remove(id)?;
        self.order.remove(&s.tick);
        self.used -= s.bytes;
        Some(s.inv)
    }

    /// Forgets the machine and wait tree of `id` — every path that changes the
    /// id's journal outside the delivery path, and every turn.
    pub(super) fn forget(&mut self, id: &InvocationId) {
        self.take(id);
        self.trees.remove(id);
    }

    /// Stores `inv`; returns how many old machines had to be dropped to fit the cap.
    fn put(&mut self, inv: Invocation) -> u64 {
        self.take(&inv.id);
        let bytes = size_of(&inv);
        if bytes > self.cap {
            return 0;
        }
        let mut evicted = 0;
        while self.used + bytes > self.cap {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some(s) = self.slots.remove(&oldest) {
                self.used -= s.bytes;
                evicted += 1;
            }
        }
        self.tick += 1;
        self.order.insert(self.tick, inv.id);
        self.used += bytes;
        self.slots.insert(
            inv.id,
            Slot {
                tick: self.tick,
                bytes,
                inv,
            },
        );
        evicted
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.slots.len()
    }
}

/// The estimated bytes of a machine: the payload of its journal entries plus overhead.
fn size_of(inv: &Invocation) -> usize {
    MACHINE_OVERHEAD
        + inv
            .machine
            .journal()
            .entries()
            .iter()
            .map(|e| e.payload.len() + ENTRY_OVERHEAD)
            .sum::<usize>()
}

impl<S: RangeStore> Engine<S> {
    /// The wait tree of `id`'s last suspension, if the engine still remembers
    /// it: written after the `suspended` commit, removed when a turn loads the
    /// journal, lost when the engine is lost. `None` = unknown.
    pub fn waiting_tree(&self, id: &InvocationId) -> Option<&pb::SuspensionMessage> {
        self.warm.trees.get(id)
    }

    /// Byte cap of the warm machines (default [`WARM_CAP_BYTES`]); `0` = off.
    pub fn with_warm_cap(mut self, bytes: usize) -> Self {
        self.warm = Warm::new(bytes);
        self
    }

    /// `id`'s machine for one drainer delivery: the warm machine if the journal
    /// tail in the store matches it, otherwise load cold and **replay without
    /// applying tasks**.
    ///
    /// Replaying without applying tasks: the delivery path does not rewrite the
    /// outbox/timer rows of pending completions, does not re-read
    /// promises/state — no commit. That recovery sweep is the next turn's job,
    /// which always starts with `load` + `Event::Start` through
    /// [`Engine::step`]. Every pending completion of an invocation already has
    /// a durable source (an outbox row, a timer, a promise/attach waiter)
    /// written in the same commit as the command that declared it; the tasks
    /// re-applied at `Start` are idempotent (D-O2) and only make up for a turn
    /// that died between two commits — i.e. a turn that has not ended, and
    /// that turn reruns on its own.
    ///
    /// An error of the **cold load** is returned as `Ok(Err)` (the inbound call
    /// site treats it as "no target"); a read error of the tail check is an
    /// ordinary error (`Err`).
    pub async fn settle_machine(
        &mut self,
        id: InvocationId,
        params: StartParams,
    ) -> Result<std::result::Result<(Invocation, WarmUse), EngineError>> {
        let mut how = WarmUse::Cold;
        if let Some(mut inv) = self.warm.take(&id) {
            if self.still_current(&mut inv).await? {
                if warm_check_on() {
                    self.cross_check(&inv, params).await;
                }
                return Ok(Ok((inv, WarmUse::Hit)));
            }
            how = WarmUse::Stale;
        }
        Ok(self.replay_cold(id, params).await.map(|inv| (inv, how)))
    }

    /// Loads cold, then replays the journal into the machine without applying
    /// tasks: `Start`'s effects (the greeting, the replayed part, tasks) are
    /// dropped — nobody is listening.
    async fn replay_cold(&mut self, id: InvocationId, params: StartParams) -> Result<Invocation> {
        // Not through `load`: it evicts the id's wait tree, and the tree
        // belongs to the last suspended turn — the delivery path only reads it.
        let mut inv = self.load_journal(id, params).await?;
        inv.machine.handle(Event::Start)?;
        Ok(inv)
    }

    /// Stores the machine that just finished a delivery (`deliver` has
    /// committed). A closed machine (violation, ended) is not stored. Returns
    /// how many old machines were dropped because of the cap.
    pub fn keep_warm(&mut self, inv: Invocation) -> u64 {
        if inv.outcome.is_some() || inv.machine.phase() != cairn_runtime::Phase::Processing {
            self.warm.forget(&inv.id);
            return 0;
        }
        self.warm.put(inv)
    }

    /// Whether the journal tail in the store really is the machine's journal
    /// (see the module doc). Refreshes the machine's pinned deployment from the
    /// store (`migrate` changes it without touching the journal).
    async fn still_current(&self, inv: &mut Invocation) -> Result<bool> {
        let j = inv.machine.journal();
        let len = j.len();
        if len == 0 {
            return Ok(false);
        }
        if self
            .writer
            .get(&cairn_store::keys::journal_entry(&inv.id, len))
            .await?
            .is_some()
        {
            return Ok(false);
        }
        for i in [len - 1, 0] {
            let stored = self
                .writer
                .get(&cairn_store::keys::journal_entry(&inv.id, i))
                .await?;
            let ours = j.get(i).expect("i < len").encode();
            if stored.as_deref() != Some(ours.as_ref()) {
                return Ok(false);
            }
        }
        let chain = self
            .writer
            .get(&cairn_store::keys::chain(&inv.id))
            .await?
            .and_then(|raw| cairn_store::keys::decode_chain(&raw));
        if chain != inv.chain {
            return Ok(false);
        }
        let first = j.get(0).expect("len > 0").clone();
        inv.deployment = self.pinned_of(&inv.id, &first).await?;
        Ok(true)
    }

    /// Cross-check mode: load the same id cold and compare with the warm machine; a mismatch panics.
    async fn cross_check(&mut self, warm: &Invocation, params: StartParams) {
        let cold = match self.replay_cold(warm.id, params).await {
            Ok(c) => c,
            Err(e) => panic!(
                "warm machine of {} matched the tail but the cold load failed: {e}",
                hex(&warm.id)
            ),
        };
        if let Some(d) = divergence(warm, &cold) {
            panic!(
                "warm machine of {} diverges from the cold-loaded machine: {d}",
                hex(&warm.id)
            );
        }
    }
}

/// The first place where two `Invocation`s differ in the part rebuildable from the store.
fn divergence(a: &Invocation, b: &Invocation) -> Option<String> {
    if let Some(d) = a.machine.divergence(&b.machine) {
        return Some(format!("machine: {d}"));
    }
    if a.target != b.target {
        return Some("target".into());
    }
    if a.reply_to != b.reply_to {
        return Some("reply_to".into());
    }
    if a.kind != b.kind {
        return Some("kind".into());
    }
    if a.deployment != b.deployment {
        return Some(format!(
            "deployment: {:?} ↔ {:?}",
            a.deployment, b.deployment
        ));
    }
    if a.chain != b.chain {
        return Some("hash chain head".into());
    }
    if a.calls != b.calls {
        return Some("call rule marks".into());
    }
    if a.outcome != b.outcome {
        return Some("outcome".into());
    }
    None
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        cairn_protocol::{encode_message, FrameDecoder},
        cairn_runtime::{Completion, CompletionResult},
        cairn_store::{LeadershipGrant, PartitionId},
        slatedb::{object_store::memory::InMemory, Db},
    };

    const ID: InvocationId = [9; 24];
    const OTHER: InvocationId = [10; 24];

    async fn engine() -> Engine {
        let db = Db::builder("partition-0", Arc::new(InMemory::new()))
            .build()
            .await
            .expect("open db");
        let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
            .await
            .expect("open writer");
        Engine::open(w)
            .await
            .expect("open engine")
            .with_warm_cap(WARM_CAP_BYTES)
    }

    fn target() -> Target {
        Target {
            tenant: Default::default(),
            scope: None,
            service: "Svc".into(),
            handler: "run".into(),
            object_key: String::new(),
        }
    }

    fn call(i: u32) -> cairn_protocol::Frame {
        let wire = encode_message(
            cairn_protocol::MessageType::CALL,
            cairn_protocol::Flags::NONE,
            &pb::CallCommandMessage {
                service_name: "Far".into(),
                handler_name: "h".into(),
                parameter: Bytes::from(format!("{i}")),
                invocation_id_notification_idx: 2 * i + 1,
                result_completion_id: 2 * i + 2,
                ..Default::default()
            },
        );
        let mut buf = bytes::BytesMut::from(&wire[..]);
        FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
    }

    /// A caller that issued `n` `ctx.call`s (completions `1..=2n` pending).
    async fn caller(e: &mut Engine, id: InvocationId, n: u32) {
        let mut inv = e
            .create(
                id,
                &target(),
                CreateOptions::default(),
                StartParams::default(),
                Bytes::new(),
            )
            .await
            .expect("create");
        e.step(&mut inv, Event::Start).await.expect("start");
        for i in 0..n {
            e.step(&mut inv, Event::FromSdk(call(i)))
                .await
                .expect("command");
        }
    }

    fn id_of(i: u32) -> Completion {
        Completion {
            id: 2 * i + 1,
            result: CompletionResult::CallInvocationId(format!("callee-{i}")),
        }
    }

    /// One drainer delivery, exactly the sequence of `turn::dispatch`
    /// (`settle_machine` → `deliver` → `keep_warm`).
    async fn settle(e: &mut Engine, id: InvocationId, c: Completion) -> WarmUse {
        let (mut inv, how) = e
            .settle_machine(id, StartParams::default())
            .await
            .expect("read")
            .expect("load");
        e.deliver(&mut inv, c, Source::None).await.expect("deliver");
        e.keep_warm(inv);
        how
    }

    async fn journal(e: &mut Engine, id: InvocationId) -> Vec<JournalEntry> {
        e.load(id, StartParams::default())
            .await
            .expect("load")
            .machine
            .journal()
            .entries()
            .to_vec()
    }

    /// From the second delivery on no turn interferes with the warm machine; the
    /// final journal is the same as delivering through the cold path (cross-check
    /// mode runs on every use).
    #[tokio::test]
    async fn later_settles_use_the_warm_machine() {
        let mut e = engine().await;
        caller(&mut e, ID, 3).await;
        let mut hows = Vec::new();
        for i in 0..3 {
            hows.push(settle(&mut e, ID, id_of(i)).await);
        }
        assert_eq!(hows, [WarmUse::Cold, WarmUse::Hit, WarmUse::Hit]);
        let j = journal(&mut e, ID).await;
        assert_eq!(j.len(), 1 + 3 + 3, "entry 0, ba Call, ba notification");
    }

    /// The delivery path loads cold and then only **replays**: it does not
    /// re-apply the tasks of pending completions — it does not rewrite the
    /// outbox rows of calls waiting for an id, no commit. That recovery sweep
    /// is the next turn's job (always
    /// `load` + `Event::Start` through `Engine::step`).
    ///
    /// Before the warm machine: the delivery path's `Event::Start` went through
    /// `Engine::step`, rewriting three `Call` outbox rows (D-O2, same `seq`) in
    /// a separate commit.
    #[tokio::test]
    async fn a_cold_settle_machine_writes_nothing() {
        let mut e = engine().await;
        caller(&mut e, ID, 3).await;
        let before = e.writer.health().bytes_written();
        let (inv, how) = e
            .settle_machine(ID, StartParams::default())
            .await
            .expect("read")
            .expect("load");
        assert_eq!(how, WarmUse::Cold);
        assert_eq!(inv.machine.pending_completions().len(), 6);
        assert_eq!(
            e.writer.health().bytes_written(),
            before,
            "replay on the delivery path must write nothing"
        );
    }

    /// An engine opened with the default cap [`WARM_CAP_BYTES`]: the second
    /// delivery uses the warm machine without anyone having to enable it.
    #[tokio::test]
    async fn the_default_engine_keeps_warm_machines() {
        let db = Db::builder("partition-0", Arc::new(InMemory::new()))
            .build()
            .await
            .expect("open db");
        let w = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
            .await
            .expect("open writer");
        let mut e = Engine::open(w).await.expect("open engine");
        caller(&mut e, ID, 2).await;
        assert_eq!(settle(&mut e, ID, id_of(0)).await, WarmUse::Cold);
        assert_eq!(settle(&mut e, ID, id_of(1)).await, WarmUse::Hit);
    }

    /// A cold-loading turn (`load`) evicts the id's warm machine.
    #[tokio::test]
    async fn a_load_forgets_the_warm_machine() {
        let mut e = engine().await;
        caller(&mut e, ID, 2).await;
        settle(&mut e, ID, id_of(0)).await;
        e.load(ID, StartParams::default()).await.expect("turn");
        assert_eq!(settle(&mut e, ID, id_of(1)).await, WarmUse::Cold);
    }

    /// The journal grew outside the delivery path (entry `L` present) ⇒ the warm machine is stale, dropped.
    #[tokio::test]
    async fn a_longer_journal_makes_the_warm_machine_stale() {
        let mut e = engine().await;
        caller(&mut e, ID, 3).await;
        settle(&mut e, ID, id_of(0)).await;
        // Another turn appends entry `L` without going through this engine's
        // `load` — the shape of every journal write path the warm machine
        // does not see.
        let len = e.warm.slots[&ID].inv.known_entries();
        let mut muts = Mutations::new();
        let n = Message::CallInvocationIdCompletion(
            pb::CallInvocationIdCompletionNotificationMessage {
                completion_id: 3,
                invocation_id: "callee-1".into(),
            },
        );
        let raw = JournalEntry::new(n.message_type(), {
            let w = n.encode(cairn_protocol::Flags::NONE);
            w.slice(cairn_protocol::HEADER_LEN..)
        })
        .encode();
        muts.append_journal(&ID, len, &raw);
        e.writer.commit(muts).await.expect("write by hand");
        assert_eq!(settle(&mut e, ID, id_of(2)).await, WarmUse::Stale);
    }

    /// A delivery commit that did not reach the store (error, ambiguous): the
    /// warm machine knows an entry the store does not have (entry `L − 1`
    /// absent) ⇒ dropped, even if someone mistakenly stored it.
    #[tokio::test]
    async fn a_delivery_that_never_reached_the_store_is_stale() {
        let mut e = engine().await;
        caller(&mut e, ID, 3).await;
        settle(&mut e, ID, id_of(0)).await;
        let mut inv = e.warm.take(&ID).expect("warm machine");
        // A completion goes into the machine, without a commit — like a
        // `deliver` failing after the machine received it.
        inv.machine
            .handle(Event::Completion(id_of(1)))
            .expect("machine");
        e.keep_warm(inv);
        assert_eq!(settle(&mut e, ID, id_of(1)).await, WarmUse::Stale);
        let j = journal(&mut e, ID).await;
        assert_eq!(j.len(), 1 + 3 + 2, "completion 3 delivered exactly once");
    }

    /// Every place that writes entry 0 (`create`) and `purge` evict the id's
    /// machine: purging and recreating the same id is a different journal.
    #[tokio::test]
    async fn purge_and_create_forget_the_warm_machine() {
        let mut e = engine().await;
        caller(&mut e, ID, 2).await;
        settle(&mut e, ID, id_of(0)).await;
        e.fail_with(&ID, 500, "cancelled").await.expect("end");
        e.purge_now(&ID).await.expect("purge");
        assert_eq!(e.warm.len(), 0, "purge evicts the machine");

        caller(&mut e, ID, 2).await;
        settle(&mut e, ID, id_of(0)).await;
        assert_eq!(e.warm.len(), 1);
        e.create(
            ID,
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await
        .expect("create over");
        assert_eq!(e.warm.len(), 0, "create evicts the machine");
    }

    /// A closed machine (protocol violation) is not stored.
    #[tokio::test]
    async fn a_closed_machine_is_not_kept() {
        let mut e = engine().await;
        caller(&mut e, ID, 1).await;
        let (mut inv, _) = e
            .settle_machine(ID, StartParams::default())
            .await
            .expect("read")
            .expect("load");
        e.deliver(
            &mut inv,
            Completion {
                id: 77,
                result: CompletionResult::Timer,
            },
            Source::None,
        )
        .await
        .expect("deliver");
        e.keep_warm(inv);
        assert_eq!(e.warm.len(), 0);
    }

    /// Byte cap: storing a new machine drops the oldest one.
    #[tokio::test]
    async fn the_byte_cap_evicts_the_oldest_machine() {
        let mut e = engine().await;
        caller(&mut e, ID, 2).await;
        caller(&mut e, OTHER, 2).await;
        settle(&mut e, ID, id_of(0)).await;
        let one = e.warm.used;
        e.warm.cap = one + one / 2;
        let (mut inv, _) = e
            .settle_machine(OTHER, StartParams::default())
            .await
            .expect("read")
            .expect("load");
        e.deliver(&mut inv, id_of(0), Source::None)
            .await
            .expect("deliver");
        assert_eq!(e.keep_warm(inv), 1, "ID's machine was dropped");
        assert!(e.warm.slots.contains_key(&OTHER) && !e.warm.slots.contains_key(&ID));
    }

    /// Cross-check mode catches what the tail check cannot see: an entry **in
    /// the middle** of the journal changed (entry 0 and `L − 1` match, `L`
    /// absent) ⇒ panic.
    #[tokio::test]
    #[should_panic(expected = "diverges from the cold-loaded machine")]
    async fn cross_check_catches_what_the_tail_cannot_see() {
        let mut e = engine().await;
        caller(&mut e, ID, 2).await;
        settle(&mut e, ID, id_of(0)).await;
        let mut muts = Mutations::new();
        let raw = JournalEntry::new(cairn_protocol::MessageType::CALL, call(9).payload).encode();
        muts.append_journal(&ID, 1, &raw);
        e.writer.commit(muts).await.expect("write by hand");
        settle(&mut e, ID, id_of(1)).await;
    }
}
