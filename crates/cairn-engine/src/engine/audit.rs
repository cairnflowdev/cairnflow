// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Audit rows **in the same commit** as a person's action.
//!
//! The shell (server) calls [`Engine::audit_next`] **after** taking the lock of
//! the target entity and **right before** the engine call that performs the
//! action: the record sits in a pending slot matched by **entity**
//! (invocation, schedule). The engine's first commit touching exactly that
//! entity (`stage_status`, `stage_lifecycle`, `purge`, `step`, schedules)
//! carries the [`cairn_store::keyspace::Table::Audit`] row in the same batch —
//! no other commit can "ride along" (unlike stream riders: a rider attaches to
//! whatever commit comes next). An action that changes nothing (no such id,
//! already ended) has no commit touching the entity: the shell calls
//! [`Engine::audit_flush`] to commit the record on its own, before answering
//! the caller — still durable before the `2xx`.
//!
//! The pending slot is dropped when the shell's task is cancelled midway:
//! [`AuditGuard`] removes the record on drop, so there is no record for an
//! action that never ran.

use {
    super::*,
    std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

struct Pending {
    token: u64,
    entity: crate::InvocationId,
    /// Only travels with the signal-delivery batch (`deliver_signal`) — the
    /// `Running` commit of the turn carrying the signal comes before that
    /// batch, and an external signal exists only in RAM until that batch is
    /// durable.
    signal: bool,
    tenant: String,
    ts_ms: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// The pending slot of one engine. `Arc` because [`AuditGuard`] must remove the
/// record on drop without having the engine at hand.
#[derive(Default)]
pub(super) struct AuditSlots {
    any: AtomicBool,
    /// There may be rows waiting for export (set on write and on becoming
    /// leader; the export loop clears it when a scan finds nothing).
    dirty: AtomicBool,
    next_token: AtomicU64,
    pending: Mutex<Vec<Pending>>,
}

impl AuditSlots {
    pub(super) fn dirty_at_start() -> Self {
        let s = Self::default();
        s.dirty.store(true, Ordering::Relaxed);
        s
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Pending>> {
        self.pending.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn refresh(&self, v: &[Pending]) {
        self.any.store(!v.is_empty(), Ordering::Release);
    }

    fn remove(&self, token: u64) -> Option<Pending> {
        let mut v = self.lock();
        let i = v.iter().position(|p| p.token == token)?;
        let p = v.swap_remove(i);
        self.refresh(&v);
        Some(p)
    }
}

/// A record waiting for a commit that touches its entity. Drop ⇒ removed if
/// nobody used it.
pub struct AuditGuard {
    slots: Arc<AuditSlots>,
    token: u64,
}

impl Drop for AuditGuard {
    fn drop(&mut self) {
        self.slots.remove(self.token);
    }
}

impl<S: RangeStore> Engine<S> {
    /// The `audit/table` gate as it was when the range opened. Off ⇒ no audit
    /// row is written (old binaries do not know the table) —
    /// [`Self::audit_next`] returns `None`.
    pub fn with_audit(mut self, on: bool) -> Self {
        self.audit = on;
        self
    }

    pub fn audit_on(&self) -> bool {
        self.audit
    }

    /// The cluster's store and prefix for the audit-row export loop.
    pub fn with_audit_store(mut self, store: Option<cairn_store::audit::AuditStore>) -> Self {
        self.audit_store = store;
        self
    }

    pub fn audit_store(&self) -> Option<&cairn_store::audit::AuditStore> {
        self.audit_store.as_ref()
    }

    /// Places audit record `record` (JSON) of tenant `tenant`, at `ts_ms`, for
    /// an action on entity `entity` — see the module doc. The row key is
    /// `(entity, writer_epoch, count)`: unique across every leader change.
    pub fn audit_next(
        &mut self,
        entity: crate::InvocationId,
        tenant: &str,
        ts_ms: u64,
        record: &[u8],
    ) -> Option<AuditGuard> {
        self.audit_next_at(entity, tenant, ts_ms, record, false)
    }

    /// Like [`Self::audit_next`], for an external signal: the record only
    /// travels with the signal-delivery batch ([`Self::deliver_signal`]), not
    /// with any other commit touching the entity.
    pub fn audit_next_signal(
        &mut self,
        entity: crate::InvocationId,
        tenant: &str,
        ts_ms: u64,
        record: &[u8],
    ) -> Option<AuditGuard> {
        self.audit_next_at(entity, tenant, ts_ms, record, true)
    }

    fn audit_next_at(
        &mut self,
        entity: crate::InvocationId,
        tenant: &str,
        ts_ms: u64,
        record: &[u8],
        signal: bool,
    ) -> Option<AuditGuard> {
        if !self.audit {
            return None;
        }
        self.audit_seq += 1;
        let key = cairn_store::keys::audit(&entity, self.writer.writer_epoch(), self.audit_seq);
        let value = cairn_store::audit::encode_row(tenant, ts_ms, record);
        let token = self.audit_slots.next_token.fetch_add(1, Ordering::Relaxed);
        let mut v = self.audit_slots.lock();
        v.push(Pending {
            token,
            entity,
            signal,
            tenant: tenant.to_string(),
            ts_ms,
            key,
            value,
        });
        self.audit_slots.refresh(&v);
        Some(AuditGuard {
            slots: Arc::clone(&self.audit_slots),
            token,
        })
    }

    /// Stages every pending record of `entity` into `muts` (the batch about to
    /// commit touching that entity). Nobody waiting ⇒ only one atomic read.
    pub(super) fn take_audit_for(&self, entity: &crate::InvocationId, muts: &mut Mutations) {
        self.take_audit(entity, muts, false);
    }

    /// Like [`Self::take_audit_for`], for the signal-delivery batch: also the
    /// records reserved for that batch.
    pub(super) fn take_audit_for_signal(&self, entity: &crate::InvocationId, muts: &mut Mutations) {
        self.take_audit(entity, muts, true);
    }

    fn take_audit(&self, entity: &crate::InvocationId, muts: &mut Mutations, signal: bool) {
        if !self.audit_slots.any.load(Ordering::Acquire) {
            return;
        }
        let mut v = self.audit_slots.lock();
        let mut i = 0;
        while i < v.len() {
            if v[i].entity == *entity && (signal || !v[i].signal) {
                let p = v.swap_remove(i);
                muts.put_audit(p.key, &p.value);
                self.audit_slots.dirty.store(true, Ordering::Release);
            } else {
                i += 1;
            }
        }
        self.audit_slots.refresh(&v);
    }

    /// `guard`'s record has not travelled with any commit (the action changed
    /// nothing): commit it on its own right away — durable before the shell
    /// answers — with `record` replacing the record placed earlier (a different
    /// outcome: not found, nothing changed). If it already travelled with the
    /// action's commit, do nothing.
    pub async fn audit_flush(&mut self, guard: AuditGuard, record: Option<&[u8]>) -> Result<()> {
        let Some(p) = self.audit_slots.remove(guard.token) else {
            return Ok(());
        };
        let value = match record {
            Some(r) => cairn_store::audit::encode_row(&p.tenant, p.ts_ms, r),
            None => p.value,
        };
        let mut muts = Mutations::new();
        muts.put_audit(p.key, &value);
        self.writer.commit(muts).await?;
        self.audit_slots.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Drops `guard`'s record if it has not travelled with any commit (no-op
    /// outcomes are aggregated outside the range, not turned into rows).
    /// `true` = dropped; `false` = the row already went with the action's
    /// commit.
    pub fn audit_discard(&mut self, guard: AuditGuard) -> bool {
        self.audit_slots.remove(guard.token).is_some()
    }

    /// There may be audit rows waiting for export (set on write and on
    /// becoming leader; the export loop clears it when a scan finds nothing).
    pub fn audit_dirty(&self) -> bool {
        self.audit && self.audit_slots.dirty.load(Ordering::Acquire)
    }

    pub fn set_audit_dirty(&self, dirty: bool) {
        self.audit_slots.dirty.store(dirty, Ordering::Release);
    }

    /// At most `limit` rows waiting for export: `(key, value)`.
    pub async fn audit_rows(&self, limit: usize) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self.writer.audit_rows(limit).await?)
    }

    /// Drops rows already exported — one commit.
    pub async fn audit_remove(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut muts = Mutations::new();
        for k in keys {
            muts.remove_audit(k);
        }
        self.writer.commit(muts).await?;
        Ok(())
    }
}
