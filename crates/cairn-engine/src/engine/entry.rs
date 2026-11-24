// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Input-entry headers: building (`input_entry`, `with_key`) and reading (`target_of`,
//! `kind_of`, …).

use super::*;

pub(super) const HDR_SERVICE: &str = "cairnflow-service";
/// The invocation's scope. Durable in entry 0 because **the routing target is
/// not self-sufficient without it**: reloading an invocation after failover
/// must read the right state table, and reading the wrong scope is reading
/// another tenant's state. The same reason as `HDR_KEY`.
pub(super) const HDR_SCOPE: &str = "cairnflow-scope";
/// The invocation's tenant. Only written when not `default`, like scope.
/// Durable in entry 0 for the same reason: reloading after failover must read
/// the right state keys of the right tenant.
pub(super) const HDR_TENANT: &str = "cairnflow-tenant";
pub(super) const HDR_HANDLER: &str = "cairnflow-handler";
pub(super) const HDR_KEY: &str = "cairnflow-key";
pub(super) const HDR_REPLY_TO: &str = "cairnflow-reply-to";
pub(super) const HDR_REPLY_COMPLETION: &str = "cairnflow-reply-completion";
/// The invocation's idempotency key, if any. Lives in entry 0 because purge
/// needs it: cleaning up an invocation must also clean up the `idempotency`
/// row pointing at it, and at that point entry 0 is the only self-sufficient
/// source — purge has only the id in hand.
pub(super) const HDR_IDEMPOTENCY: &str = "cairnflow-idempotency-key";
/// The minimum `ctx.log` level requested at creation — read by the SDK.
pub const HDR_LOG_LEVEL: &str = "cairnflow-log-level";
/// The invocation's contract ([`InvocationKind`]). Absent = `Plain`.
pub(super) const HDR_KIND: &str = "cairnflow-kind";
/// The deployment the invocation is pinned to. Durable in entry 0 for the same
/// reason as `HDR_SERVICE`: reloading after failover must know to call *the
/// right* code that produced this journal, not the service's newest code.
pub(super) const HDR_DEPLOYMENT: &str = "cairnflow-deployment";

/// The invocation lives under the rule "a virtual object's `SHARED` handler
/// does not write state" (D-W5). Every entry 0 written by a binary that knows
/// the rule carries it (`input_entry`, and `Engine::restart_in` adds it for
/// the next copy if the old copy lacks it). Absent ⇒ an entry 0 from an old
/// binary: the old rule, forever — an invocation in flight during a binary
/// upgrade does not change behaviour midway.
///
/// The engine does not read the deployment registry, so the mark enforces
/// nothing by itself: the turn layer decides whether this turn runs a VO's
/// `SHARED` handler (from the pinned deployment) and then calls
/// [`Invocation::mark_shared_object_handler`]. Old binaries ignore unknown
/// header keys — a marked invocation running on an old node is simply not
/// restricted.
pub const HDR_STATE_LOCK: &str = "cairnflow-state-lock";

/// Entry-0 header of a new copy: which copy it was rebuilt from.
pub(super) const HDR_RESTARTED_FROM: &str = "cairnflow-restarted-from";

/// The invocation was created once the `calls/prefix` gate was on in the range
/// that created it: every unkeyed `ctx.call`/`ctx.send` **of its own** follows
/// the prefix-grouping rule, forever. Absent ⇒ the old rule, forever.
///
/// The rule belongs to the **caller**, fixed once in the commit that creates
/// it, because that is the only durable and immutable thing every rewrite of
/// its `Call` rows can read: the state machine replays `Task::Invoke` on every
/// replay, and that rewrite must not look at the gate state at that moment, at
/// the outbox row (possibly already deleted), or at the D-O2 map in RAM
/// (possibly off after a failed commit).
pub(super) const HDR_CALL_PREFIX: &str = "cairnflow-call-prefix";

/// The invocation was created once the `calls/placement` gate was on in the
/// range that created it: every unkeyed `ctx.call`/`ctx.send` **of its own**
/// carries `placement: "spread"` under the old rule `H(caller, cid)`, forever.
/// Absent ⇒ `spread` is ignored (grouped like a default call), forever.
///
/// The same reason as [`HDR_CALL_PREFIX`]: the rule must be something durable
/// and immutable that every rewrite of a `Call` row can read. One more reason:
/// a binary that knows up to gate step 4 ignores `placement` — unmarked
/// invocations are exactly those for which that binary's rule and this
/// binary's rule agree, and the gate (`min_binary=5`) guarantees no step-4
/// binary is still running once there are marked invocations.
pub(super) const HDR_CALL_PLACEMENT: &str = "cairnflow-call-placement";

/// The two call-rule marks of entry 0 ([`HDR_CALL_PREFIX`], [`HDR_CALL_PLACEMENT`]),
/// fixed in the commit that creates the invocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CallRules {
    /// Unkeyed calls group by the caller's pk prefix.
    pub(super) prefix: bool,
    /// Unkeyed calls `spread` under the old rule.
    pub(super) placement: bool,
}

/// `StartMessage.key` is always the invocation's object key.
///
/// Filled in here rather than at the call site, because callers **can
/// forget** — and when they forget, `ctx.key` on the SDK side silently becomes
/// an empty string. No error goes off; the virtual object simply does not know
/// who it is, and the bug shows up in the business layer.
pub(super) fn with_key(params: StartParams, target: &Target, kind: InvocationKind) -> StartParams {
    StartParams {
        key: target.object_key.clone(),
        // The right to write state is fixed **here**, from entry 0's durable
        // contract — not from the deployment registry at run time. See
        // [`InvocationKind`].
        state_read_only: matches!(kind, InvocationKind::WorkflowShared),
        // `StartMessage` does **not** carry the scope as a separate field: the
        // SDK does not need it (the scope does not change how the handler
        // runs, only where and with whom). But someone *reading logs* does — a
        // 48-character hex id does not say which tenant this invocation
        // belongs to, and that is exactly the first question in a multi-tenant
        // incident.
        //
        // So the scope goes into `debug_id` as a prefix. No new field, and it
        // can still be traced back: the part after the `/` is still the full
        // id.
        debug_id: match &target.scope {
            Some(s) => format!("{s}/{}", params.debug_id),
            None => params.debug_id,
        },
        ..params
    }
}

// Each parameter is an entry-0 header — grouping them into a struct would only dodge the lint.
#[allow(clippy::too_many_arguments)]
pub(super) fn input_entry(
    target: &Target,
    reply_to: Option<&ReplyTo>,
    idempotency_key: Option<&str>,
    kind: InvocationKind,
    deployment: Option<&str>,
    calls: CallRules,
    log_level: Option<&str>,
    schedule: Option<&str>,
    input: Bytes,
) -> JournalEntry {
    use cairn_protocol::{pb, Flags, MessageType};
    let hdr = |k: &str, v: &str| pb::Header {
        key: k.to_string(),
        value: v.to_string(),
    };
    let mut headers = vec![
        hdr(HDR_SERVICE, &target.service),
        hdr(HDR_HANDLER, &target.handler),
        hdr(HDR_KEY, &target.object_key),
        // The state-write rule for a VO's `SHARED` handlers is fixed here (D-W5).
        hdr(HDR_STATE_LOCK, "on"),
    ];
    // Only written when present: an empty header and an absent header read
    // back the same in `target_of`, so do not make entry 0 longer for every
    // unscoped invocation — which is nearly all of them.
    if let Some(s) = &target.scope {
        headers.push(hdr(HDR_SCOPE, s.as_str()));
    }
    if !target.tenant.is_default() {
        headers.push(hdr(HDR_TENANT, target.tenant.as_str()));
    }
    if let Some(r) = reply_to {
        headers.push(hdr(HDR_REPLY_TO, &hex(&r.caller)));
        headers.push(hdr(HDR_REPLY_COMPLETION, &r.completion_id.to_string()));
    }
    if let Some(idem) = idempotency_key {
        headers.push(hdr(HDR_IDEMPOTENCY, idem));
    }
    if let Some(k) = kind.as_header() {
        headers.push(hdr(HDR_KIND, k));
    }
    if let Some(d) = deployment {
        headers.push(hdr(HDR_DEPLOYMENT, d));
    }
    if calls.prefix {
        headers.push(hdr(HDR_CALL_PREFIX, "on"));
    }
    if calls.placement {
        headers.push(hdr(HDR_CALL_PLACEMENT, "on"));
    }
    if let Some(l) = log_level {
        headers.push(hdr(HDR_LOG_LEVEL, l));
    }
    if let Some(s) = schedule {
        headers.push(hdr(crate::schedule::HDR_SCHEDULE, s));
    }
    let msg = Message::Input(pb::InputCommandMessage {
        headers,
        value: Some(cairn_protocol::value::inline(input)),
        name: String::new(),
    });
    let wire = msg.encode(Flags::NONE);
    JournalEntry::new(MessageType::INPUT, wire.slice(cairn_protocol::HEADER_LEN..))
}

pub(super) fn input_headers(entry: &JournalEntry) -> Option<Vec<cairn_protocol::pb::Header>> {
    use cairn_protocol::{Flags, Frame, MessageType};
    if entry.ty != MessageType::INPUT {
        return None;
    }
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    match cairn_protocol::decode(&frame) {
        Ok(Message::Input(input)) => Some(input.headers),
        _ => None,
    }
}

/// Reads back where to return the result from the `Input` entry. `None` = nobody waits.
pub(super) fn reply_to_of(entry: &JournalEntry) -> Option<ReplyTo> {
    let headers = input_headers(entry)?;
    let get = |k: &str| headers.iter().find(|h| h.key == k).map(|h| h.value.clone());
    let caller = unhex(&get(HDR_REPLY_TO)?)?;
    let completion_id = get(HDR_REPLY_COMPLETION)?.parse().ok()?;
    Some(ReplyTo {
        caller,
        completion_id,
    })
}

/// The idempotency key recorded in entry 0, if the invocation was created with one.
///
/// `None` for an invocation without a key **and** for an invocation created by
/// an old binary (before this header existed) — for the latter, purge cleans
/// up everything except the `idempotency` row, and that orphan row is treated
/// by the idempotency paths as "expired": a mapping pointing at an invocation
/// that does not exist ⇒ create a new one, overwrite.
/// The contract recorded in entry 0. Absent ⇒ [`InvocationKind::Plain`], so
/// every invocation created by a binary from before workflows reads back
/// exactly as before.
pub(super) fn kind_of(entry: &JournalEntry) -> InvocationKind {
    let Some(headers) = input_headers(entry) else {
        return InvocationKind::Plain;
    };
    headers
        .iter()
        .find(|h| h.key == HDR_KIND)
        .map_or(InvocationKind::Plain, |h| {
            InvocationKind::from_header(&h.value)
        })
}

/// The target and deployment read from entry 0 (`Input`) — for journal readers
/// outside the engine (trial replay).
pub fn input_head(entry: &JournalEntry) -> Option<(Target, Option<String>)> {
    target_of(entry).map(|t| (t, deployment_of(entry)))
}

/// The deployment recorded in entry 0, if the invocation was created while pinned.
pub(super) fn deployment_of(entry: &JournalEntry) -> Option<String> {
    input_headers(entry)?
        .into_iter()
        .find(|h| h.key == HDR_DEPLOYMENT)
        .map(|h| h.value)
}

/// The [`HDR_CALL_PREFIX`] and [`HDR_CALL_PLACEMENT`] marks of entry 0. Absent
/// (or an unknown value) ⇒ `false`: the old rule, exactly the rule of every
/// invocation created before the gate.
pub(super) fn call_rules_of(entry: &JournalEntry) -> CallRules {
    let Some(hs) = input_headers(entry) else {
        return CallRules::default();
    };
    let on = |key: &str| hs.iter().any(|h| h.key == key && h.value == "on");
    CallRules {
        prefix: on(HDR_CALL_PREFIX),
        placement: on(HDR_CALL_PLACEMENT),
    }
}

/// The [`HDR_STATE_LOCK`] mark of entry 0. Absent (or an unknown value) ⇒
/// `false`: the old rule, exactly the rule of every invocation created before
/// D-W5.
pub(super) fn state_lock_of(entry: &JournalEntry) -> bool {
    input_headers(entry).is_some_and(|hs| {
        hs.iter()
            .any(|h| h.key == HDR_STATE_LOCK && h.value == "on")
    })
}

pub(super) fn idempotency_of(entry: &JournalEntry) -> Option<String> {
    let headers = input_headers(entry)?;
    headers
        .iter()
        .find(|h| h.key == HDR_IDEMPOTENCY)
        .map(|h| h.value.clone())
}

pub(super) fn unhex(s: &str) -> Option<InvocationId> {
    cairn_store::id_from_hex(s)
}

/// Reads the routing target back from the `Input` entry.
pub(super) fn target_of(entry: &JournalEntry) -> Option<Target> {
    use cairn_protocol::{Flags, Frame, MessageType};
    if entry.ty != MessageType::INPUT {
        return None;
    }
    let frame = Frame::new(entry.ty, Flags::NONE, entry.payload.clone());
    let Ok(Message::Input(input)) = cairn_protocol::decode(&frame) else {
        return None;
    };
    let get = |k: &str| {
        input
            .headers
            .iter()
            .find(|h| h.key == k)
            .map(|h| h.value.clone())
    };
    // Tenant: header absent ⇒ `default` (every invocation from before tenants,
    // and every invocation of tenant `default`). Header **present but broken**
    // ⇒ cannot be loaded — the opposite of scope below: a broken tenant that
    // falls back to `default` would read and write the data of a different,
    // real tenant.
    let tenant = match get(HDR_TENANT) {
        None => TenantId::default(),
        Some(t) => TenantId::new(&t).ok()?,
    };
    Some(Target {
        tenant,
        // Header absent ⇒ no scope. Header **present but invalid** also gives
        // `None`, and that is the safer choice: reading the root (unscoped)
        // state of an invocation with a broken header finds it empty and the
        // invocation dies cleanly; guessing a scope means writing into the
        // state of a real tenant.
        scope: get(HDR_SCOPE).and_then(|s| Scope::new(&s).ok()),
        service: get(HDR_SERVICE)?,
        handler: get(HDR_HANDLER)?,
        object_key: get(HDR_KEY)?,
    })
}
