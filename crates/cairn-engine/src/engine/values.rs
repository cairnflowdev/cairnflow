// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Segmented values in the engine.
//!
//! This part keeps two invariants for segmented-value data, and reads fail
//! closed:
//!
//! 1. **One of two rows.** Every `(object, user_key)` has at most one row,
//!    `State` or `SegmentedState`. Writing or clearing a state key
//!    (`SetState`, `ClearState`, `ClearAllState`, workflow cleanup) deletes
//!    the `SegmentedState` row of the same key **and** removes every holder
//!    `State(user_key)` — in the same batch.
//! 2. **Holders follow the journal.** `Engine::purge` removes every holder
//!    `Invocation(id)` in the same commit as the journal.
//! 3. **Fail-closed reads.** A key with only a `SegmentedState` row ⇒ a lazy
//!    `GetState` returns an error ([`StoreError::Segmented`]), eager state
//!    marks `partial_state` (the SDK reads lazily), `GetStateKeys` and
//!    `max-state-keys=` see the key. The tail is never read as the whole
//!    value.
//!
//! **Only when the gate is on at range open** ([`Engine::with_values`]): rows
//! in the two tables exist only in ranges whose leader opened them with the
//! `values/segmented` gate on (only the leader writes, and the gate is
//! one-way), so gate off ⇒ the tables are empty by construction, and the turn
//! path pays no extra read.

use {
    super::*,
    crate::output::output_owner,
    cairn_protocol::value::Logical,
    cairn_store::value::{self, Holder, SegRef, SegmentedRow},
};

impl<S: RangeStore> Engine<S> {
    /// The `values/segmented` gate as it was at range open — see the module head.
    pub fn with_values(mut self, on: bool) -> Self {
        self.values = on;
        self
    }

    pub fn values_on(&self) -> bool {
        self.values
    }

    /// The segment store; the shell uses it outside the engine (D16).
    pub fn with_value_store(mut self, store: Option<value::ValueStore>) -> Self {
        self.value_store = store;
        self
    }

    pub fn value_store(&self) -> Option<&value::ValueStore> {
        self.value_store.as_ref()
    }

    /// `step` of a frame the **shell has sealed**: `fresh` is the `(sha, gen)`
    /// the shell just PUT for this frame (durable before the commit, AP-1).
    /// Every other ref of the frame must be in the key's current value —
    /// checked under the engine, in the same commit step
    /// ([`StoreError::ForeignSegment`]).
    pub async fn step_sealed(
        &mut self,
        inv: &mut Invocation,
        frame: cairn_protocol::Frame,
        fresh: Vec<([u8; 32], u64)>,
    ) -> Result<Vec<Message>> {
        self.step_sealed_with(inv, frame, fresh, 0).await
    }

    /// Like [`Self::step_sealed`]; `old_bytes` is the number of bytes in
    /// `fresh`'s segments that the shell **merged** from old segments — not new
    /// customer bytes, so they do not count units.
    pub async fn step_sealed_with(
        &mut self,
        inv: &mut Invocation,
        frame: cairn_protocol::Frame,
        fresh: Vec<([u8; 32], u64)>,
        old_bytes: u64,
    ) -> Result<Vec<Message>> {
        self.sealed_fresh = fresh;
        self.sealed_old_bytes = old_bytes;
        let r = self.step(inv, Event::Sealed(frame)).await;
        self.sealed_fresh.clear();
        self.sealed_old_bytes = 0;
        r
    }

    /// Refs the shell just **COPYed** to the recipient's owner for the
    /// completion about to be delivered: a `CallSegmented` completion may carry
    /// only these refs. Cleared after the delivery step ([`Self::deliver`]).
    pub fn stage_copied(&mut self, fresh: Vec<([u8; 32], u64)>) {
        self.sealed_fresh = fresh;
    }

    pub(super) fn clear_copied(&mut self) {
        self.sealed_fresh.clear();
        self.sealed_old_bytes = 0;
    }

    /// An `Output`/`ProposeRunCompletion` frame the shell has sealed: every ref
    /// must be a segment the shell just PUT for this frame, or already be in
    /// the holder `Invocation(id)` of **this very** invocation under its output
    /// owner (a ref of an entry it has journaled, e.g. `GetStateCompletion`).
    /// Anything else ⇒ [`StoreError::ForeignSegment`], nothing is written.
    pub(super) async fn check_sealed_output(
        &self,
        inv: &Invocation,
        frame: &cairn_protocol::Frame,
    ) -> Result<()> {
        use cairn_protocol::MessageType as T;
        if !(frame.ty == T::OUTPUT || frame.ty == T::PROPOSE_RUN_COMPLETION) {
            return Ok(());
        }
        let Ok(m) = cairn_protocol::decode(frame) else {
            return Ok(());
        };
        let foreign = || StoreError::ForeignSegment {
            key: format!("{} of {}", frame.ty, hex(&inv.id)),
        };
        let refs = cairn_protocol::value::refs_in(&m).map_err(|_| foreign())?;
        if refs.is_empty() {
            return Ok(());
        }
        if !self.values {
            return Err(foreign().into());
        }
        let mut held: Option<Vec<SegRef>> = None;
        for r in refs {
            if self.sealed_fresh.contains(&(r.sha256, r.gen)) {
                continue;
            }
            if held.is_none() {
                held = Some(self.invocation_held(&inv.target, &inv.id).await?);
            }
            let ok = held.as_deref().is_some_and(|h| {
                h.iter()
                    .any(|h| h.sha == r.sha256 && h.gen == r.gen && h.len == r.len)
            });
            if !ok {
                return Err(foreign().into());
            }
        }
        Ok(())
    }

    /// A completion carrying a segmented value (`CallSegmented`) may only carry
    /// refs the shell just COPYed ([`Self::stage_copied`]) — a ref outside the
    /// list is rejected like a made-up ref.
    pub(super) fn check_copied(&self, c: &cairn_runtime::Completion) -> Result<()> {
        let cairn_runtime::CompletionResult::CallSegmented(v) = &c.result else {
            return Ok(());
        };
        let ok = self.values
            && v.refs()
                .iter()
                .all(|r| self.sealed_fresh.contains(&(r.sha256, r.gen)));
        if ok {
            Ok(())
        } else {
            Err(StoreError::ForeignSegment {
                key: format!("completion {}", c.id),
            }
            .into())
        }
    }

    /// The segments the holder `Invocation(id)` holds under its output owner.
    pub(super) async fn invocation_held(
        &self,
        t: &Target,
        id: &InvocationId,
    ) -> Result<Vec<SegRef>> {
        let (pk, owner) = output_owner(t, id);
        let prefix = cairn_store::keys::value_refs_holder_prefix(
            pk,
            &t.tenant,
            &owner,
            &Holder::Invocation(*id),
        );
        let mut out = Vec::new();
        for (k, v) in self.writer.value_refs_under_unbounded(&prefix).await? {
            if let (Some(k), Some(len)) = (
                cairn_store::keys::parse_value_ref(&k),
                value::decode_ref_row(&v),
            ) {
                out.push(SegRef {
                    sha: k.sha,
                    gen: k.gen,
                    len,
                });
            }
        }
        Ok(out)
    }

    /// Every ref in `refs` is a `SetState` ref of `inv` for key `user_key` that
    /// may be reused — the same set `WriteState` accepts: the key's current
    /// `SegmentedState` row, then the holder `Invocation(id)` of the invocation
    /// itself, matching `(sha, gen, len)` exactly. The shell calls this before
    /// reading a ref's bytes to merge them: a ref outside this set belongs to
    /// another owner or is made up — no GET, no merge, so the engine rejects it
    /// like a dangling ref.
    pub async fn refs_reusable(
        &self,
        inv: &Invocation,
        user_key: &[u8],
        refs: &[cairn_protocol::value::SegmentRef],
    ) -> Result<bool> {
        let matches = |held: &[SegRef], r: &cairn_protocol::value::SegmentRef| {
            held.iter()
                .any(|h| h.sha == r.sha256 && h.gen == r.gen && h.len == r.len)
        };
        let mut held = self.segmented_refs_of(&inv.target, user_key).await?;
        if refs.iter().all(|r| matches(&held, r)) {
            return Ok(true);
        }
        held.extend(self.invocation_held(&inv.target, &inv.id).await?);
        Ok(refs.iter().all(|r| matches(&held, r)))
    }

    /// The refs of the key's current `SegmentedState` row (gate off, absent, or
    /// broken ⇒ empty — a broken row lets no ref be reused).
    pub async fn segmented_refs_of(&self, target: &Target, user_key: &[u8]) -> Result<Vec<SegRef>> {
        if !self.values {
            return Ok(Vec::new());
        }
        let Some(pk) = target.object_owner() else {
            return Ok(Vec::new());
        };
        let raw = self
            .writer
            .segmented_state(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                user_key,
            )
            .await?;
        Ok(raw
            .and_then(|r| SegmentedRow::decode(&r))
            .map(|r| r.refs)
            .unwrap_or_default())
    }

    /// Writes segmented value `v` for the key (after
    /// [`Self::stage_drop_segmented`]): deletes the `State` row, writes the
    /// `SegmentedState` row and a holder `State(k)` for every ref. A ref not
    /// just PUT must be in `held` (the key's old value).
    pub(super) fn stage_write_segmented(
        &self,
        muts: &mut Mutations,
        target: &Target,
        user_key: &[u8],
        v: &Logical,
        held: &[SegRef],
    ) -> Result<()> {
        let Logical::Segmented { refs, tail, len } = v else {
            return Ok(());
        };
        let foreign = || StoreError::ForeignSegment {
            key: String::from_utf8_lossy(user_key).into_owned(),
        };
        let (Some((pk, owner)), true) = (
            value::object_owner(
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            ),
            self.values,
        ) else {
            return Err(foreign().into());
        };
        let mut row = SegmentedRow {
            len: *len,
            refs: Vec::with_capacity(refs.len()),
            tail: tail.clone(),
        };
        let holder = Holder::State(Bytes::copy_from_slice(user_key));
        for r in refs {
            let known = self.sealed_fresh.contains(&(r.sha256, r.gen))
                || held
                    .iter()
                    .any(|h| h.sha == r.sha256 && h.gen == r.gen && h.len == r.len);
            if !known {
                return Err(foreign().into());
            }
            row.refs.push(SegRef {
                sha: r.sha256,
                gen: r.gen,
                len: r.len,
            });
            muts.put_value_ref(pk, &target.tenant, &owner, &holder, &r.sha256, r.gen, r.len);
        }
        muts.clear_state(
            pk,
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
            user_key,
        )
        .put_segmented_state(
            pk,
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
            user_key,
            &row,
        );
        Ok(())
    }

    /// A holder `Invocation(id)` for every ref of the entry just persisted.
    /// Owner as [`Self::stage_release_invocation`] removes it: a keyed object ⇒
    /// the object's owner, otherwise ⇒ the id's owner. Returns the total bytes
    /// of the refs **just sealed** in the entry (in `fresh`), minus the part
    /// the shell merged from old segments — the part that counts units by new
    /// bytes.
    pub(super) fn stage_entry_holders(
        &self,
        t: &Target,
        id: &InvocationId,
        entry: &cairn_runtime::JournalEntry,
        muts: &mut Mutations,
    ) -> Result<usize> {
        if !self.values {
            return Ok(0);
        }
        let frame = cairn_protocol::Frame::new(
            entry.ty,
            cairn_protocol::Flags::NONE,
            entry.payload.clone(),
        );
        let Ok(m) = cairn_protocol::decode(&frame) else {
            return Ok(0);
        };
        let refs = cairn_protocol::value::refs_in(&m)
            .map_err(|e| StoreError::CorruptSegmented { key: e.to_string() })?;
        if refs.is_empty() {
            return Ok(0);
        }
        let (pk, owner) = output_owner(t, id);
        let holder = Holder::Invocation(*id);
        let mut fresh = 0u64;
        let mut counted: Vec<([u8; 32], u64)> = Vec::new();
        for r in refs {
            muts.put_value_ref(pk, &t.tenant, &owner, &holder, &r.sha256, r.gen, r.len);
            let k = (r.sha256, r.gen);
            if self.sealed_fresh.contains(&k) && !counted.contains(&k) {
                counted.push(k);
                fresh += r.len;
            }
        }
        Ok(fresh.saturating_sub(self.sealed_old_bytes) as usize)
    }

    /// Invariant 1 for one key: a `SegmentedState` row exists ⇒ delete it and
    /// stage the removal of every holder `State(user_key)` into `muts`. Removes
    /// by holder **prefix**, not by the refs in the row: a broken row still
    /// removes everything.
    pub(super) async fn stage_drop_segmented(
        &self,
        muts: &mut Mutations,
        target: &Target,
        user_key: &[u8],
    ) -> Result<()> {
        if !self.values {
            return Ok(());
        }
        let Some((pk, owner)) = value::object_owner(
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
        ) else {
            return Ok(());
        };
        let row = self
            .writer
            .segmented_state(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                user_key,
            )
            .await?;
        if row.is_none() {
            return Ok(());
        }
        muts.clear_segmented_state(
            pk,
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
            user_key,
        );
        let holder = Holder::State(Bytes::copy_from_slice(user_key));
        let prefix =
            cairn_store::keys::value_refs_holder_prefix(pk, &target.tenant, &owner, &holder);
        for (k, _) in self.writer.value_refs_under_unbounded(&prefix).await? {
            muts.remove_value_ref(&k);
        }
        Ok(())
    }

    /// Whether object `target` has any segmented state key (gate off ⇒ no) —
    /// reads at most one row.
    pub(super) async fn has_segmented_keys(&self, target: &Target) -> Result<bool> {
        if !self.values {
            return Ok(false);
        }
        let Some(pk) = target.object_owner() else {
            return Ok(false);
        };
        Ok(self
            .writer
            .has_segmented_state(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            )
            .await?)
    }

    /// The segmented state keys of object `target` (gate off ⇒ empty).
    pub(super) async fn segmented_keys(&self, target: &Target) -> Result<Vec<Bytes>> {
        if !self.values {
            return Ok(Vec::new());
        }
        let Some(pk) = target.object_owner() else {
            return Ok(Vec::new());
        };
        Ok(self
            .writer
            .segmented_state_entries_unbounded(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            )
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    /// The key's segmented value (ref + tail) — gate off or absent ⇒ `None`; a
    /// broken row ⇒ [`StoreError::CorruptSegmented`] (strict read).
    pub(super) async fn segmented_value(
        &self,
        target: &Target,
        user_key: &[u8],
    ) -> Result<Option<Logical>> {
        if !self.values {
            return Ok(None);
        }
        let Some(pk) = target.object_owner() else {
            return Ok(None);
        };
        let Some(raw) = self
            .writer
            .segmented_state(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                user_key,
            )
            .await?
        else {
            return Ok(None);
        };
        let row = SegmentedRow::decode(&raw).ok_or_else(|| StoreError::CorruptSegmented {
            key: String::from_utf8_lossy(user_key).into_owned(),
        })?;
        Ok(Some(Logical::Segmented {
            refs: row
                .refs
                .iter()
                .map(|r| cairn_protocol::value::SegmentRef {
                    sha256: r.sha,
                    len: r.len,
                    gen: r.gen,
                })
                .collect(),
            tail: row.tail,
            len: row.len,
        }))
    }

    /// Key `user_key` of `target` has only a `SegmentedState` row (gate off ⇒
    /// `false`).
    pub(super) async fn is_segmented(&self, target: &Target, user_key: &[u8]) -> Result<bool> {
        if !self.values {
            return Ok(false);
        }
        let Some(pk) = target.object_owner() else {
            return Ok(false);
        };
        Ok(self
            .writer
            .segmented_state(
                pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                user_key,
            )
            .await?
            .is_some())
    }

    /// Invariant 2: stages the removal of every holder `Invocation(id)` into
    /// `muts`. The owner is derived from entry 0's target: a keyed/scoped
    /// object ⇒ the object's owner; otherwise ⇒ the owner of the id itself
    /// (both are scanned: the writer picks one of the two by the same rule). An
    /// unreadable target ⇒ removes nothing: the rows leak (`verify` sees them),
    /// no guessed deletion (delete only when provable).
    pub(super) async fn stage_release_invocation(
        &self,
        muts: &mut Mutations,
        id: &InvocationId,
        target: Option<&Target>,
    ) -> Result<()> {
        if !self.values {
            return Ok(());
        }
        let holder = Holder::Invocation(*id);
        let mut owners = Vec::with_capacity(2);
        if let Some(t) = target {
            if let Some(o) = value::object_owner(&t.tenant, t.scope(), &t.service, &t.object_key) {
                owners.push((t.tenant.clone(), o));
            }
            owners.push((t.tenant.clone(), value::invocation_owner(id)));
        }
        for (tenant, (pk, owner)) in owners {
            let prefix = cairn_store::keys::value_refs_holder_prefix(pk, &tenant, &owner, &holder);
            for (k, _) in self.writer.value_refs_under_unbounded(&prefix).await? {
                muts.remove_value_ref(&k);
            }
        }
        Ok(())
    }
}
