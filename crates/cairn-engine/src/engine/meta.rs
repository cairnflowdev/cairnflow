// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Invocation metadata and the engine's single status-write path.
//!
//! Every write of a status row by the engine goes through
//! [`Engine::stage_status`] (or [`Engine::stage_wake_status`] for waking), and
//! the caller **declares the old class** ([`Was`]) — the type forces all eight
//! write sites to declare it. From the old and new class, this function
//! decides the `t` stamp written into the payload (the time it entered the
//! class; the old `t` is kept when the class does not change) and the
//! query-index operation.

use {
    super::*,
    cairn_store::query_index::{Class, InvocationMeta, Placement},
};

/// The old class of a status write — declared by the caller.
pub(super) enum Was<'a> {
    /// Creation: there is no row yet. The metadata goes in the same commit.
    New(&'a InvocationMeta),
    /// Already exists, currently in this class; `at` = the old class's `t`
    /// stamp (read from the status row — `None` for `active`).
    In(Class, Option<u64>),
}

impl Was<'_> {
    /// The old class from a status row already read ([`Engine::status_at`]).
    pub(super) fn of(status: &InvocationStatus, at: Option<u64>) -> Self {
        Was::In(status.class(), at)
    }
}

impl InvocationKind {
    /// The kind code in [`InvocationMeta::kind`].
    pub fn code(self) -> u8 {
        match self {
            Self::Plain => 0,
            Self::WorkflowRun => 1,
            Self::WorkflowShared => 2,
        }
    }

    /// Inverse of [`Self::code`]; unknown code ⇒ `Plain` (like [`Self::from_header`]).
    pub fn from_code(code: u8) -> Self {
        match code {
            1 => Self::WorkflowRun,
            2 => Self::WorkflowShared,
            _ => Self::Plain,
        }
    }

    /// The name in the API and SQL.
    pub fn name(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::WorkflowRun => "workflow-run",
            Self::WorkflowShared => "workflow-shared",
        }
    }
}

/// The metadata of a creation (without `created_ms` — the engine sets it).
pub(super) fn meta_of(target: &Target, opts: &CreateOptions<'_>) -> InvocationMeta {
    InvocationMeta {
        created_ms: 0,
        kind: opts.kind.code(),
        // Parent: whoever waits for the result, or the one-way sender.
        root: opts.reply_to.is_none() && opts.sender.is_none(),
        tenant: target.tenant.as_str().to_string(),
        scope: target
            .scope()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        service: target.service.clone(),
        handler: target.handler.clone(),
        key: target.object_key.clone(),
        parent: opts.reply_to.map(|r| r.caller).or(opts.sender),
        schedule: opts
            .schedule
            .as_ref()
            .map(|s| crate::schedule::stamp_name(s.header).to_string()),
        idempotency_key: opts.idempotency_key.map(str::to_string),
        deployment: opts.deployment.map(str::to_string),
        tags: opts
            .tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        root_id: opts.root_id,
        restarted_from: None,
        limit_key: opts
            .queue
            .and_then(|(q, _)| q.limit.as_ref())
            .map(|l| l.to_string()),
        unknown: Vec::new(),
    }
}

/// The target of an invocation, from its metadata. `None` if the tenant/scope
/// is no longer valid (a broken row).
pub fn target_of_meta(m: &InvocationMeta) -> Option<Target> {
    Some(Target {
        tenant: TenantId::new(&m.tenant).ok()?,
        scope: if m.scope.is_empty() {
            None
        } else {
            Some(Scope::new(&m.scope).ok()?)
        },
        service: m.service.clone(),
        handler: m.handler.clone(),
        object_key: m.key.clone(),
    })
}

/// A wake-up (`suspended → running`) that has been allowed: **can only be
/// built** from a status that was read as `suspended` ([`Self::observed`]).
/// That is how the wake path declares the old class `active` by construction:
/// the engine's wake queue only accepts this type, so no path writes `running`
/// blindly over a `paused` (losing `t`, the index row in the wrong class).
/// Same class ⇒ reads nothing, touches no index, needs no metadata.
pub(super) struct Woken(InvocationId);

impl Woken {
    pub(super) fn observed(id: InvocationId, status: Option<&InvocationStatus>) -> Option<Self> {
        matches!(status, Some(InvocationStatus::Suspended)).then_some(Self(id))
    }

    pub(super) fn stage(self, muts: &mut Mutations) {
        // `running` carries no `t` (class `active`): the stamp is ignored.
        muts.set_invocation_status(&self.0, &InvocationStatus::Running.encode(0));
    }
}

impl<S: RangeStore> Engine<S> {
    /// The status with the class's `t` stamp (format 10) — one GET.
    pub async fn status_at(
        &self,
        id: &InvocationId,
    ) -> Result<Option<(InvocationStatus, Option<u64>)>> {
        let raw = self
            .writer
            .get(&cairn_store::keys::invocation_status(id))
            .await?;
        Ok(raw.and_then(|raw| {
            let at = InvocationStatus::decode_at(&raw);
            InvocationStatus::decode(raw).map(|s| (s, at))
        }))
    }

    /// The metadata of `id`.
    pub async fn meta(&self, id: &InvocationId) -> Result<Option<InvocationMeta>> {
        Ok(self.writer.invocation_meta(id).await?)
    }

    /// Writes status `new` of `id`, together with everything derived from the
    /// class: the `t` stamp, the metadata row (on creation), and the
    /// query-index operation (`query_event`). Class unchanged ⇒ keep the old
    /// `t`, read nothing, touch no index. Class changed ⇒ one metadata point
    /// read (hot zone).
    pub(super) async fn stage_status(
        &self,
        muts: &mut Mutations,
        id: &InvocationId,
        was: Was<'_>,
        new: &InvocationStatus,
    ) -> Result<()> {
        // The audit row of the action on this entity.
        self.take_audit_for(id, muts);
        let now = self.clock.wall_ms();
        // Billing units: 1 at creation, 1 on entering an ending status — every
        // creating/ending path writes its status through here, in the same batch.
        let terminal = |c: Class| matches!(c, Class::Completed | Class::Failed);
        match was {
            Was::New(meta) => {
                muts.meter_units(&meta.tenant, 1 + u64::from(terminal(new.class())));
                let p = Placement::new(new.class(), now, meta);
                muts.put_invocation_meta(id, meta)
                    .set_invocation_status(id, &new.encode(now))
                    .query_event_with(self.query_indexes, id, meta, None, Some(p));
            }
            Was::In(old, old_at) => {
                // Same class (pausing a `paused` again, `running ↔ suspended`):
                // `t` is the time it **entered** the class, unchanged.
                let at = if old == new.class() {
                    old_at.unwrap_or(now)
                } else {
                    now
                };
                muts.set_invocation_status(id, &new.encode(at));
                if old != new.class() {
                    // Absent metadata only happens for hand-built rows (tests)
                    // — nothing to index; `verify` reports any orphan row.
                    if let Some(meta) = self.meta(id).await? {
                        if terminal(new.class()) && !terminal(old) {
                            muts.meter_units(&meta.tenant, 1);
                        }
                        let before = Placement::new(old, old_at.unwrap_or(0), &meta);
                        let after = Placement::new(new.class(), at, &meta);
                        muts.query_event_with(
                            self.query_indexes,
                            id,
                            &meta,
                            Some(before),
                            Some(after),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// The metadata of a `restart-as-new` copy: copies `old`'s row, changes
    /// `created_ms`, keeps the parent and tags, writes `restarted_from`. `old`
    /// has no row (impossible in format 10) ⇒ built from the target.
    pub(super) async fn restart_meta(
        &self,
        old: &InvocationId,
        target: &Target,
        deployment: Option<&str>,
    ) -> Result<InvocationMeta> {
        let mut m = match self.meta(old).await? {
            Some(m) => m,
            None => meta_of(target, &CreateOptions::default()),
        };
        m.created_ms = self.clock.wall_ms();
        m.restarted_from = Some(*old);
        m.deployment = deployment.map(str::to_string);
        Ok(m)
    }
}
