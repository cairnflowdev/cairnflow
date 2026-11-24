// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level log on
// the turn's ordinary path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The local write right: [`LeadershipGrant`], `Validity` (the lease deadline as an
//! atomic tick), [`ValidityHandle`].

use super::*;

/// Proof that we hold the lease. Consume-once, not `Clone`, not `Send`.
///
/// This is **the only way** to open a [`PartitionWriter`]. There is no other public
/// constructor than [`LeadershipGrant::single_node`] (single-node deployments).
#[must_use = "a grant must be used to open a writer, or dropped to yield leadership"]
pub struct LeadershipGrant {
    pub(super) partition: PartitionId,
    /// The term of the lease that issued this grant. **Not the fence** — the fence is the
    /// `writer_epoch` in SlateDB's manifest. Here it is only for logs and introspection:
    /// "at which change of owner did this node become leader".
    term: u64,
    pub(super) validity: Arc<Validity>,
    _not_send_sync: PhantomData<*const ()>,
}

impl LeadershipGrant {
    /// A grant for a **single-node** deployment: there is no lease layer, this process is
    /// the leader by definition.
    ///
    /// In a cluster, real grants are issued by [`crate::Session::claim`]. The call-site
    /// signature is the same — that is why this type existed before there was any
    /// distribution.
    ///
    /// Safety still holds even if someone runs two single-node processes: SlateDB's
    /// `writer_epoch` fences the old process (verified experimentally).
    pub fn single_node(partition: PartitionId) -> Self {
        Self {
            partition,
            term: 0,
            validity: Arc::new(Validity::new(TICKS_NEVER)),
            _not_send_sync: PhantomData,
        }
    }

    /// A grant issued **under a node session** (D20): valid as long as it has not been
    /// revoked on its own **and** the parent session is still valid.
    ///
    /// A grant has no deadline of its own — the deadline is the session's, renewed once
    /// for every partition of the node.
    ///
    /// `pub(crate)`: this is the only place besides [`Self::single_node`] that can create a
    /// grant, and it lives in the same crate as [`crate::Session`]. Opening it up would open
    /// a detour around the lease layer — opening a writer with no proof at all that one is
    /// the leader. Thanks to the nesting, a grant **cannot outlive its session**: not even
    /// when the write of partition ownership succeeds while the session renewal is hanging.
    pub(crate) fn from_session(partition: PartitionId, term: u64, parent: &ValidityHandle) -> Self {
        Self {
            partition,
            term,
            validity: Arc::new(Validity::child(Arc::clone(&parent.0))),
            _not_send_sync: PhantomData,
        }
    }

    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Which change of owner this is. See the note on the `term` field: **not** a fence.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// A handle for the lease layer to report "lease lost" from elsewhere.
    ///
    /// This is the part that *may* be `Send` — it can only lower the flag, never write.
    pub fn validity_handle(&self) -> ValidityHandle {
        ValidityHandle(Arc::clone(&self.validity))
    }
}

/// The validity of a lease: a revocation flag **and a deadline on the clock**.
///
/// The deadline is the whole reason this type is no longer a bare `AtomicBool`. The earlier
/// version only lowered the flag when the renewal loop *got to* its deadline check — so a
/// hanging renewal PUT, or a blocked thread, left the grant valid well past `2/3 TTL`. Now
/// the deadline is part of the read: [`Self::is_valid`] compares with the clock, so
/// revocation cannot be delayed by some starved task.
///
/// `valid_until` is stored as a tick (nanoseconds around a process-local origin) so it can
/// be read with one atomic load on the commit path.
///
/// **Suspension**: past `valid_until` the grant is no longer valid — no acks, no reads
/// (LS-2) — but not yet **lost**: for `pause` ticks after that, a renewal winning the CAS
/// from exactly the previous version can still extend the deadline
/// ([`ValidityHandle::extend_until`]) and the grant runs again, nobody has to drop the range
/// or replay. Only past that cap too (`valid_until + pause`) or when revoked is it lost
/// ([`ValidityHandle::lost`]). `pause` = 0 (every validity that is not a session root) is
/// the old behaviour: expired means lost.
pub(super) struct Validity {
    revoked: AtomicBool,
    valid_until: AtomicU64,
    /// Suspension ticks allowed after `valid_until` — see above.
    pause: u64,
    /// The incarnation's read fence (only on the session root) — see [`ReadFence`].
    fence: Option<ReadFence>,
    /// Wakes whoever waits on [`ValidityHandle::lost`] when revoked or when the deadline changes.
    changed: tokio::sync::watch::Sender<()>,
    /// The parent validity: a partition's grant lives under the node's session. When the
    /// parent ends the child ends, nobody needs to revoke each child.
    parent: Option<Arc<Validity>>,
    /// Pinned by a fault-injection tool: ignores both its own deadline and the parent.
    pinned: AtomicBool,
}

/// The leader-side read fence of a session incarnation: each call issues (through the
/// session's background loop, batched) a conditional GET on the session record and returns
/// `true` when it is still our `live` one. Attached to the **root** validity; child grants
/// find it through the parent chain.
pub(crate) type ReadFence = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync,
>;

/// A grant without a deadline: [`LeadershipGrant::single_node`], or a grant pinned by a
/// fault-injection tool.
pub(super) const TICKS_NEVER: u64 = u64::MAX;

/// The tick origin's offset: the local origin is initialised lazily, so an `Instant`
/// before it (e.g. when a lease takeover was issued) must be representable without being
/// clamped to 0. 2^62 ns ≈ 146 years each way.
pub(super) const TICKS_BIAS: u64 = 1 << 62;

pub(super) fn tick_epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

pub(super) fn to_ticks(at: Instant) -> u64 {
    let epoch = tick_epoch();
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    if at >= epoch {
        TICKS_BIAS
            .saturating_add(nanos(at - epoch))
            .min(TICKS_NEVER - 1)
    } else {
        TICKS_BIAS.saturating_sub(nanos(epoch - at))
    }
}

pub(super) fn from_ticks(ticks: u64) -> Instant {
    let epoch = tick_epoch();
    if ticks >= TICKS_BIAS {
        epoch + Duration::from_nanos(ticks - TICKS_BIAS)
    } else {
        epoch - Duration::from_nanos(TICKS_BIAS - ticks)
    }
}

impl Validity {
    pub(super) fn new(valid_until: u64) -> Self {
        Self {
            revoked: AtomicBool::new(false),
            valid_until: AtomicU64::new(valid_until),
            pause: 0,
            fence: None,
            changed: tokio::sync::watch::Sender::new(()),
            parent: None,
            pinned: AtomicBool::new(false),
        }
    }

    /// A child has no deadline of its own: the deadline is the parent's.
    pub(super) fn child(parent: Arc<Validity>) -> Self {
        Self {
            parent: Some(parent),
            ..Self::new(TICKS_NEVER)
        }
    }

    pub(super) fn is_valid(&self) -> bool {
        if self.revoked.load(Ordering::Acquire) {
            return false;
        }
        if self.pinned.load(Ordering::Acquire) {
            return true;
        }
        let until = self.valid_until.load(Ordering::Acquire);
        let own = until == TICKS_NEVER || to_ticks(Instant::now()) < until;
        own && self.parent.as_ref().is_none_or(|p| p.is_valid())
    }

    /// **Lost** for good: revoked, or past the suspension cap too — on itself or on the
    /// parent chain. Differs from `!is_valid()` during the suspension window.
    pub(super) fn is_lost(&self) -> bool {
        if self.revoked.load(Ordering::Acquire) {
            return true;
        }
        if self.pinned.load(Ordering::Acquire) {
            return false;
        }
        let until = self.valid_until.load(Ordering::Acquire);
        let own =
            until != TICKS_NEVER && to_ticks(Instant::now()) >= until.saturating_add(self.pause);
        own || self.parent.as_ref().is_some_and(|p| p.is_lost())
    }

    /// The nearest **loss** mark over the whole parent chain (deadline plus suspension cap)
    /// — the mark at which `lost()` must wake up and check again.
    fn nearest_loss(&self) -> u64 {
        if self.pinned.load(Ordering::Acquire) {
            return TICKS_NEVER;
        }
        let until = self.valid_until.load(Ordering::Acquire);
        let own = if until == TICKS_NEVER {
            TICKS_NEVER
        } else {
            until.saturating_add(self.pause).min(TICKS_NEVER - 1)
        };
        match &self.parent {
            Some(p) => own.min(p.nearest_loss()),
            None => own,
        }
    }

    /// The nearest read fence on the parent chain (usually the session root's). `None` = a
    /// grant under no session (single node).
    pub(super) fn read_fence(&self) -> Option<&ReadFence> {
        self.fence
            .as_ref()
            .or_else(|| self.parent.as_ref().and_then(|p| p.read_fence()))
    }

    /// Suspended: no longer valid but not lost.
    pub(super) fn is_paused(&self) -> bool {
        !self.is_valid() && !self.is_lost()
    }

    /// Waits until **no longer suspended**: valid again (`true` — a renewal winning the CAS
    /// moved the deadline) or lost for good (`false`). Returns at once if not suspended.
    /// Only runs on the rare (suspended) branch, so registering a `watch` here does not
    /// touch the hot path.
    pub(super) async fn settled(&self) -> bool {
        let mut watches = Vec::new();
        let mut at: Option<&Validity> = Some(self);
        while let Some(v) = at {
            watches.push(v.changed.subscribe());
            at = v.parent.as_deref();
        }
        loop {
            if self.is_valid() {
                return true;
            }
            if self.is_lost() {
                return false;
            }
            let any_change =
                futures::future::select_all(watches.iter_mut().map(|w| Box::pin(w.changed())));
            let until = self.nearest_loss();
            // `biased`: every arm leads to a recheck.
            tokio::select! {
                biased;
                _ = any_change => {}
                _ = tokio::time::sleep_until(from_ticks(until)) => {}
            }
        }
    }

    pub(super) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        self.changed.send_replace(());
    }
}

/// The validity of a lease, seen from elsewhere. `Send + Sync` on purpose: the lease layer
/// runs on another task — possibly on another runtime — and must be able to lower the
/// validity. It does **not** grant write rights.
#[derive(Clone)]
pub struct ValidityHandle(pub(super) Arc<Validity>);

impl ValidityHandle {
    /// Declares the lease lost. The next commit fails before spending any I/O.
    pub fn revoke(&self) {
        self.0.revoke();
    }

    /// Not revoked **and** not past the deadline that lowers it by itself.
    pub fn is_valid(&self) -> bool {
        self.0.is_valid()
    }

    /// Lost for good: revoked, or past the suspension cap too. Differs from `!is_valid()`
    /// during the suspension window — the grant is no longer valid but can still be
    /// revived.
    pub fn is_lost(&self) -> bool {
        self.0.is_lost()
    }

    /// Moves the deadline to `until` after a renewal **winning the CAS from exactly the
    /// previous version**.
    ///
    /// Returns `false` — and moves nothing — if the grant is already **lost**: revoked, or
    /// past the suspension cap too (past the cap it is revoked at once so that state is
    /// final). Within the suspension window it can be moved: the winning CAS proves nobody
    /// has marked the session expired or overwritten it (an observer only kills a session
    /// with an `expired` CAS or an overwrite — both change the version), so nobody can have
    /// taken its ranges yet; the new deadline counts from when that renewal was **issued**,
    /// the same proof as a timely renewal (top of `session/mod.rs`). A root without a
    /// suspension cap (`pause` = 0) keeps the old rule: expired means lost.
    ///
    /// `true` = not lost — even when the new deadline is still in the past (the CAS was
    /// issued too long before it returned): the grant then stays suspended, waiting for the
    /// next renewal.
    pub(crate) fn extend_until(&self, until: Instant) -> bool {
        let v = &self.0;
        let new = to_ticks(until);
        let mut cur = v.valid_until.load(Ordering::Acquire);
        loop {
            if v.revoked.load(Ordering::Acquire) {
                return false;
            }
            if cur == TICKS_NEVER || new <= cur {
                // No deadline to move, or the new deadline is not later: keep it.
                return !v.is_lost();
            }
            if to_ticks(Instant::now()) >= cur.saturating_add(v.pause) {
                v.revoke();
                return false;
            }
            match v
                .valid_until
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    v.changed.send_replace(());
                    return true;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Drops the deadline, keeping the grant valid forever — **only** for fault-injection
    /// tools.
    ///
    /// Simulates the worst case of a frozen process: it passes the validity check right
    /// before freezing, then wakes up and keeps writing without checking again. The clock
    /// cannot save that case; only layer 2 (`writer_epoch`) does — and that is exactly what
    /// the fault-injection tests need to check.
    pub(crate) fn pin_for_fault_injection(&self) {
        self.0.valid_until.store(TICKS_NEVER, Ordering::Release);
        self.0.pinned.store(true, Ordering::Release);
        self.0.changed.send_replace(());
    }

    /// The root validity of a node session, valid until `valid_until`, with no suspension:
    /// expired means lost.
    #[cfg(test)]
    pub(crate) fn root(valid_until: Instant) -> ValidityHandle {
        Self::root_with_pause(valid_until, Duration::ZERO, None)
    }

    /// The root validity of a node session, valid until `valid_until`, suspendable for an
    /// extra `pause` after the deadline — see [`Validity`] — and carrying the incarnation's
    /// read fence `fence`.
    pub(crate) fn root_with_pause(
        valid_until: Instant,
        pause: Duration,
        fence: Option<ReadFence>,
    ) -> ValidityHandle {
        ValidityHandle(Arc::new(Validity {
            pause: u64::try_from(pause.as_nanos()).unwrap_or(u64::MAX),
            fence,
            ..Validity::new(to_ticks(valid_until))
        }))
    }

    /// The read fence on this grant's parent chain — see [`ReadFence`].
    #[cfg(test)]
    pub(crate) fn read_fence(&self) -> Option<ReadFence> {
        self.0.read_fence().cloned()
    }

    /// The **loss** mark by the clock: deadline plus suspension cap, over the whole parent
    /// chain. `None` = no deadline (a single-node grant, or a pinned one).
    pub(crate) fn lost_at(&self) -> Option<Instant> {
        let at = self.0.nearest_loss();
        (at != TICKS_NEVER).then(|| from_ticks(at))
    }

    /// Waits until **lost**: revoked, or past the suspension cap too (for a root without
    /// suspension: past the deadline). Returns at once if already lost. A suspension does
    /// not wake it: the engine does not step down for a late renewal that can still be
    /// revived.
    pub(crate) async fn lost(&self) {
        // Register before checking — on **every** level of the parent chain: a revocation
        // between the check and the wait, at any level, still wakes us.
        let mut watches = Vec::new();
        let mut at: Option<&Validity> = Some(&self.0);
        while let Some(v) = at {
            watches.push(v.changed.subscribe());
            at = v.parent.as_deref();
        }
        loop {
            if self.0.is_lost() {
                return;
            }
            let any_change =
                futures::future::select_all(watches.iter_mut().map(|w| Box::pin(w.changed())));
            let until = self.0.nearest_loss();
            if until == TICKS_NEVER {
                let _ = any_change.await;
            } else {
                // `biased`: both arms lead to the same recomputation of the deadline — the
                // order only exists to keep the simulation deterministic.
                tokio::select! {
                    biased;
                    _ = any_change => {}
                    _ = tokio::time::sleep_until(from_ticks(until)) => {}
                }
            }
        }
    }
}
