// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Messages sent between two partitions.
//!
//! Two partitions are two different writers, possibly on two different nodes,
//! so partition A **cannot write** to partition B's state. Everything that
//! crosses that boundary goes through exactly the two kinds of message here.
//!
//! ## Why only two kinds
//!
//! A `ctx.call` across partitions produces exactly two directions of data:
//!
//! ```text
//!  caller partition                        target partition
//!  ────────────────────                    ──────────────
//!  ctx.call ──► outbox
//!               └──► StartCall ─────────►  create invocation, run
//!                                             │
//!  deliver to SDK ◄── Completion ◄───────────┘  done
//! ```
//!
//! There is no third kind, and that is on purpose: every message kind is a
//! surface that must stay backward compatible forever.
//!
//! ## Deduplication lives in the *key*, not here
//!
//! The receiver writes a message into the `inbound` table under a key
//! **derived from its content** (see `cairn_store::keys::inbound`), so
//! resending the same message overwrites exactly the old row. The sender is
//! therefore free to resend — and it *will* resend, because it only deletes
//! the outbox row after receiving the ack.
//!
//! That is a requirement, not a convenience: delivering the same completion
//! twice is a **protocol violation** under the service protocol, and the
//! invocation is ended with an error. An at-least-once mechanism without
//! deduplication would kill exactly the invocations it is trying to save.

use {
    crate::outbox::{OutboxError, ReplyResult},
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_store::{InboundKind, InvocationId},
};

/// The version we write. Same discipline as `outbox`: older versions are readable.
///
/// v2 appends `scope` and `limit_key` to `StartCall`. v3 appends `sender` — the
/// identity of the *call* on the sending side, which is the deduplication key
/// (see [`InterPartition::inbound_key`]).
///
/// v3 **cannot** read older messages: the 2026-10 refactor broke the format on
/// purpose (the bucket carries a `DATA_FORMAT` mark), and a `StartCall`
/// without `sender` has no correct deduplication key to use.
///
/// v5 appends `tenant` to `StartCall`; a v4 message reads as tenant
/// `default`.
const VERSION: u8 = 5;
const VERSION_MIN: u8 = 4;

/// v6 appends `schedule` to `StartCall` — **only** when the message carries
/// it: a cross-range schedule fire, which only exists once the
/// `schedules/table` gate is on (every binary `knows ≥ 7`). Every other
/// message keeps the v5 layout, readable by old binaries.
const VERSION_SCHEDULE: u8 = 6;

/// v7 appends the **caller's** `tenant` to `Attach`/`PeekOutput`
/// ([`Watch::tenant`]) and `Signal` ([`DeliverSignal::tenant`]): the target
/// range only answers/delivers to someone in the same tenant as the target.
/// Only those kinds write v7; another kind at v7 is a broken message. A v5
/// message of those kinds reads as tenant `default` — closed to every tenant
/// other than `default`.
const VERSION_TENANT: u8 = 7;

const KIND_START: u8 = 1;
const KIND_COMPLETION: u8 = 2;
const KIND_ATTACH: u8 = 3;
const KIND_PEEK_OUTPUT: u8 = 4;
const KIND_SIGNAL: u8 = 5;
/// Cancellation propagated down to a child. Same discipline as `KIND_SIGNAL`:
/// a new kind does **not** bump `VERSION` (the version describes the field
/// layout of an existing kind), and an old binary rejects it at
/// `/internal/inbound` so the sender keeps the row and retries — the cancel is
/// not lost in a half-old half-new cluster, it only arrives late.
const KIND_CANCEL: u8 = 6;

/// A cross-partition message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InterPartition {
    /// "Create this invocation and run it."
    StartCall(StartCall),
    /// "Deliver this completion to that invocation."
    Completion(DeliverCompletion),
    /// "Add `caller` to the list of waiters for `target`'s result" — attach.
    /// The result comes back later as a `Completion` message.
    Attach(Watch),
    /// "Answer right away with `target`'s current result" — void if not done yet.
    PeekOutput(Watch),
    /// "Deliver this signal to that invocation" (`SendSignal`, 0x0410). An old
    /// binary that does not know this kind rejects it at `/internal/inbound` —
    /// the sender keeps the row and retries, the signal is not lost in a
    /// half-old half-new cluster.
    Signal(DeliverSignal),
    /// "Cancel this invocation — its parent was just cancelled."
    Cancel(DeliverCancel),
}

/// A cancel command on its way to a child in another partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliverCancel {
    /// The invocation to cancel. Its partition is in the id.
    pub target: InvocationId,
    /// The parent + the `Call` completion used to receive the child's id — the
    /// deterministic identity of the cancel, used as the deduplication key (a
    /// cancel has no completion id of its own to key on like `Completion`).
    pub parent: InvocationId,
    pub id_completion: u32,
}

/// The identity of a signal: numbered (built-ins such as CANCEL = 1) or named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalId {
    Idx(u32),
    Name(String),
}

/// A signal on its way to the target invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliverSignal {
    /// The invocation receiving the signal. Its partition is in the id.
    pub target: InvocationId,
    /// The sender + the entry index of the `SendSignal` command in its journal
    /// — the deterministic identity of the SEND, used as the deduplication key
    /// (a signal has no completion id to key on like `Completion`).
    pub sender: InvocationId,
    pub entry_index: u32,
    pub signal: SignalId,
    pub result: CompletionPayload,
    /// The **sender's** tenant (v7, [`wire_tenant`]). A target in another
    /// tenant ⇒ the signal is dropped as if sent to a target that does not
    /// exist. `None` (a v5 message) = `default`.
    pub tenant: Option<String>,
}

/// A request to wait for/look at the result of an invocation in another partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Watch {
    pub target: InvocationId,
    pub caller: InvocationId,
    pub completion_id: u32,
    /// The **caller's** tenant (v7, [`wire_tenant`]). A target in another
    /// tenant ⇒ answered as if it did not exist. The target range trusts this
    /// field because `/internal/inbound` is authenticated with the cluster
    /// secret. `None` (a v5 message) = `default`.
    pub tenant: Option<String>,
}

/// The on-wire tenant of a `Watch`/`DeliverSignal`: `None` (an old message) ⇒
/// `default`, an invalid string ⇒ `None` — matches no tenant, i.e. the target
/// "does not exist". Not logged: the sender has decided.
pub fn tenant_on_wire(raw: Option<&str>) -> Option<cairn_store::TenantId> {
    match raw {
        None => Some(cairn_store::TenantId::default()),
        Some(t) => cairn_store::TenantId::new(t).ok(),
    }
}

/// The tenant the sender writes on the wire: the caller's tenant, or an empty
/// string when it cannot be determined (matches no tenant ⇒ closed).
pub fn wire_tenant(t: Option<&cairn_store::TenantId>) -> Option<String> {
    Some(t.map(|t| t.as_str().to_string()).unwrap_or_default())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartCall {
    /// The callee's id. Deterministic, and it **already carries the partition
    /// key** in its first 8 bytes — so it is both identity and address (through
    /// the cluster's key-range table).
    pub callee: InvocationId,
    pub service: String,
    pub handler: String,
    pub object_key: String,
    pub parameter: Bytes,
    /// Who waits for the result. `None` is a one-way `ctx.send`.
    pub reply_to: Option<(InvocationId, u32)>,
    pub idempotency_key: Option<String>,
    /// The scope of the invocation to create. It must travel with the message
    /// because it is **part of the identity**: the target partition writes
    /// state, promises and the idempotency table by it.
    pub scope: Option<String>,
    /// Limit key — only affects queueing in the target partition.
    pub limit_key: Option<String>,
    /// The tenant of the invocation to create = the caller's tenant. Part of
    /// the identity like `scope`, and an input to the callee's partition-key
    /// hash.
    /// `None` = `default`.
    pub tenant: Option<String>,
    /// The identity of the **call** on the sending side: `(caller, completion
    /// receiving CallInvocationId)` — unique per the service protocol, even
    /// for `ctx.send`.
    ///
    /// This is the message's deduplication key, **not** `callee`. A callee can
    /// be deterministic — a workflow's `run` turn, a call with an idempotency
    /// key — so two *different* calls to the same callee used to share a key,
    /// and the later message overwrote the earlier one in the `inbound` table
    /// before anyone got to process it. A `ctx.send` followed by a `ctx.call`
    /// with the same workflow key is enough: the send vanishes silently, the
    /// call creates the `run` turn and waits for it forever.
    pub sender: (InvocationId, u32),
    /// Which schedule fire this is: `<name>@<instant>` — the
    /// `cairnflow-schedule` header of entry 0 on the target side
    /// (`ctx.schedule`). `None` for every ordinary call.
    pub schedule: Option<String>,
    /// The call-tree root and the callee's tags — a leniently read tail block
    /// ([`crate::lineage`]), no version bump. Boxed: keeps `InterPartition`
    /// small.
    pub lineage: Box<crate::lineage::Lineage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliverCompletion {
    /// The invocation receiving the completion. Its partition is in the id.
    pub target: InvocationId,
    pub completion_id: u32,
    pub result: CompletionPayload,
}

/// The content of a completion crossing partitions.
///
/// Only three forms, because only three forms can cross the partition
/// boundary: the id of the invocation just created, a return value, and a
/// failure. State, sleep and run are all work **within** one partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompletionPayload {
    InvocationId(String),
    Value(Bytes),
    Failure {
        code: u32,
        message: String,
    },
    /// "Nothing" — `GetInvocationOutput` when the target is not done yet.
    Void,
    /// A segmented output with its source owner: the target range's shell
    /// assembles it inline or COPYs it to the recipient's owner before
    /// persisting. Only `Call`/attach/get-output results — signals never carry
    /// it.
    Segmented(crate::SegmentedOutput),
}

impl From<ReplyResult> for CompletionPayload {
    fn from(r: ReplyResult) -> Self {
        match r {
            ReplyResult::Value(v) => Self::Value(v),
            ReplyResult::Segmented(o) => Self::Segmented(o),
            ReplyResult::Failure { code, message } => Self::Failure { code, message },
        }
    }
}

impl InterPartition {
    /// The message's key in the receiver's `inbound` table.
    ///
    /// **This is the deduplication mechanism.** The key must be a function of
    /// the *identity of the work to do*, not of the send: two sends of the same
    /// work must give the same key, otherwise the second row is processed as
    /// new work.
    pub fn inbound_key(&self) -> (InboundKind, InvocationId, u32) {
        match self {
            // The identity of the *call*, not of the callee — see
            // [`StartCall::sender`]. A resend still overwrites exactly the old row.
            Self::StartCall(s) => (InboundKind::StartCall, s.sender.0, s.sender.1),
            Self::Completion(c) => (InboundKind::Completion, c.target, c.completion_id),
            // The identity of a wait request is **the waiter**, not the
            // target: the protocol guarantees `(caller, completion)` is unique,
            // while one target can have many waiters.
            Self::Attach(w) => (InboundKind::Attach, w.caller, w.completion_id),
            Self::PeekOutput(w) => (InboundKind::PeekOutput, w.caller, w.completion_id),
            // The identity of a send is `(sender, entry index)` — see the
            // comment on [`DeliverSignal`]. The target is in the value, not the key.
            Self::Signal(s) => (InboundKind::Signal, s.sender, s.entry_index),
            Self::Cancel(c) => (InboundKind::Cancel, c.parent, c.id_completion),
        }
    }

    /// The partition that must receive this message: partition key read from the id, looked up in the cluster table.
    pub fn destination(&self, cluster: &cairn_store::ClusterConfig) -> cairn_store::PartitionId {
        cluster.partition_of_id(self.destination_id())
    }

    /// The invocation this message goes to — its partition key decides the
    /// receiving range, and it is the **owner** of the inbound row on the
    /// receiving side: not the id in the deduplication key, which is the sender
    /// in five of the six kinds.
    pub fn destination_id(&self) -> &InvocationId {
        match self {
            Self::StartCall(s) => &s.callee,
            Self::Completion(c) => &c.target,
            Self::Attach(w) | Self::PeekOutput(w) => &w.target,
            Self::Signal(s) => &s.target,
            Self::Cancel(c) => &c.target,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(256);
        b.put_u8(match self {
            Self::StartCall(StartCall {
                schedule: Some(_), ..
            }) => VERSION_SCHEDULE,
            Self::Attach(_) | Self::PeekOutput(_) | Self::Signal(_) => VERSION_TENANT,
            _ => VERSION,
        });
        match self {
            Self::StartCall(s) => {
                b.put_u8(KIND_START);
                b.put_slice(&s.callee);
                put_str(&mut b, &s.service);
                put_str(&mut b, &s.handler);
                put_str(&mut b, &s.object_key);
                put_bytes(&mut b, &s.parameter);
                match &s.reply_to {
                    Some((caller, cid)) => {
                        b.put_u8(1);
                        b.put_slice(caller);
                        b.put_u32(*cid);
                    }
                    None => b.put_u8(0),
                }
                put_opt_str(&mut b, s.idempotency_key.as_deref());
                put_opt_str(&mut b, s.scope.as_deref());
                put_opt_str(&mut b, s.limit_key.as_deref());
                b.put_slice(&s.sender.0);
                b.put_u32(s.sender.1);
                put_opt_str(&mut b, s.tenant.as_deref());
                // v6: only when present — a message without it stops here, exactly the v5 layout.
                if let Some(h) = &s.schedule {
                    put_str(&mut b, h);
                }
                // The lineage tail block: an old binary stops at the field above.
                s.lineage.encode_tail(&mut b);
            }
            Self::Completion(c) => {
                b.put_u8(KIND_COMPLETION);
                b.put_slice(&c.target);
                b.put_u32(c.completion_id);
                match &c.result {
                    CompletionPayload::InvocationId(id) => {
                        b.put_u8(1);
                        put_str(&mut b, id);
                    }
                    CompletionPayload::Value(v) => {
                        b.put_u8(2);
                        put_bytes(&mut b, v);
                    }
                    CompletionPayload::Failure { code, message } => {
                        b.put_u8(3);
                        b.put_u32(*code);
                        put_str(&mut b, message);
                    }
                    CompletionPayload::Void => b.put_u8(4),
                    CompletionPayload::Segmented(o) => {
                        b.put_u8(5);
                        o.encode_into(&mut b);
                    }
                }
            }
            Self::Attach(w) | Self::PeekOutput(w) => {
                b.put_u8(match self {
                    Self::Attach(_) => KIND_ATTACH,
                    _ => KIND_PEEK_OUTPUT,
                });
                b.put_slice(&w.target);
                b.put_slice(&w.caller);
                b.put_u32(w.completion_id);
                put_opt_str(&mut b, w.tenant.as_deref());
            }
            Self::Signal(s) => {
                b.put_u8(KIND_SIGNAL);
                b.put_slice(&s.target);
                b.put_slice(&s.sender);
                b.put_u32(s.entry_index);
                match &s.signal {
                    SignalId::Idx(i) => {
                        b.put_u8(1);
                        b.put_u32(*i);
                    }
                    SignalId::Name(n) => {
                        b.put_u8(2);
                        put_str(&mut b, n);
                    }
                }
                match &s.result {
                    CompletionPayload::InvocationId(id) => {
                        b.put_u8(1);
                        put_str(&mut b, id);
                    }
                    CompletionPayload::Value(v) => {
                        b.put_u8(2);
                        put_bytes(&mut b, v);
                    }
                    CompletionPayload::Failure { code, message } => {
                        b.put_u8(3);
                        b.put_u32(*code);
                        put_str(&mut b, message);
                    }
                    CompletionPayload::Void => b.put_u8(4),
                    CompletionPayload::Segmented(o) => {
                        b.put_u8(5);
                        o.encode_into(&mut b);
                    }
                }
                put_opt_str(&mut b, s.tenant.as_deref());
            }
            Self::Cancel(c) => {
                b.put_u8(KIND_CANCEL);
                b.put_slice(&c.target);
                b.put_slice(&c.parent);
                b.put_u32(c.id_completion);
            }
        }
        b.freeze()
    }

    pub fn decode(raw: Bytes) -> Result<Self, OutboxError> {
        let mut b = raw;
        if b.remaining() < 2 {
            return Err(OutboxError::Empty);
        }
        let version = b.get_u8();
        if !(VERSION_MIN..=VERSION_TENANT).contains(&version) {
            return Err(OutboxError::UnknownVersion(version));
        }
        let kind = b.get_u8();
        // v7 only exists for the two kinds carrying the caller's tenant.
        if version == VERSION_TENANT
            && !matches!(kind, KIND_ATTACH | KIND_PEEK_OUTPUT | KIND_SIGNAL)
        {
            return Err(OutboxError::UnknownVersion(version));
        }
        let tenant_tail = |b: &mut Bytes| -> Result<Option<String>, OutboxError> {
            if version >= VERSION_TENANT {
                get_opt_str(b)
            } else {
                Ok(None)
            }
        };
        match kind {
            KIND_START => {
                let callee = get_id(&mut b)?;
                let service = get_str(&mut b)?;
                let handler = get_str(&mut b)?;
                let object_key = get_str(&mut b)?;
                let parameter = get_bytes(&mut b)?;
                let reply_to = {
                    if b.remaining() < 1 {
                        return Err(OutboxError::Truncated);
                    }
                    match b.get_u8() {
                        0 => None,
                        _ => {
                            let caller = get_id(&mut b)?;
                            Some((caller, get_u32(&mut b)?))
                        }
                    }
                };
                let idempotency_key = get_opt_str(&mut b)?;
                let scope = get_opt_str(&mut b)?;
                let limit_key = get_opt_str(&mut b)?;
                let sender = (get_id(&mut b)?, get_u32(&mut b)?);
                let tenant = if version >= 5 {
                    get_opt_str(&mut b)?
                } else {
                    None
                };
                let schedule = if version >= VERSION_SCHEDULE {
                    Some(get_str(&mut b)?)
                } else {
                    None
                };
                let lineage = Box::new(crate::lineage::Lineage::decode_tail(&mut b));
                Ok(Self::StartCall(StartCall {
                    tenant,
                    schedule,
                    lineage,
                    callee,
                    service,
                    handler,
                    object_key,
                    parameter,
                    reply_to,
                    idempotency_key,
                    scope,
                    limit_key,
                    sender,
                }))
            }
            KIND_COMPLETION => {
                let target = get_id(&mut b)?;
                let completion_id = get_u32(&mut b)?;
                if b.remaining() < 1 {
                    return Err(OutboxError::Truncated);
                }
                let result = match b.get_u8() {
                    1 => CompletionPayload::InvocationId(get_str(&mut b)?),
                    2 => CompletionPayload::Value(get_bytes(&mut b)?),
                    3 => CompletionPayload::Failure {
                        code: get_u32(&mut b)?,
                        message: get_str(&mut b)?,
                    },
                    4 => CompletionPayload::Void,
                    5 => CompletionPayload::Segmented(
                        crate::SegmentedOutput::decode_from(&mut b)
                            .ok_or(OutboxError::Truncated)?,
                    ),
                    other => return Err(OutboxError::UnknownKind(other)),
                };
                Ok(Self::Completion(DeliverCompletion {
                    target,
                    completion_id,
                    result,
                }))
            }
            kind @ (KIND_ATTACH | KIND_PEEK_OUTPUT) => {
                let w = Watch {
                    target: get_id(&mut b)?,
                    caller: get_id(&mut b)?,
                    completion_id: get_u32(&mut b)?,
                    tenant: tenant_tail(&mut b)?,
                };
                Ok(match kind {
                    KIND_ATTACH => Self::Attach(w),
                    _ => Self::PeekOutput(w),
                })
            }
            KIND_SIGNAL => {
                let target = get_id(&mut b)?;
                let sender = get_id(&mut b)?;
                let entry_index = get_u32(&mut b)?;
                if b.remaining() < 1 {
                    return Err(OutboxError::Truncated);
                }
                let signal = match b.get_u8() {
                    1 => SignalId::Idx(get_u32(&mut b)?),
                    2 => SignalId::Name(get_str(&mut b)?),
                    other => return Err(OutboxError::UnknownKind(other)),
                };
                if b.remaining() < 1 {
                    return Err(OutboxError::Truncated);
                }
                let result = match b.get_u8() {
                    1 => CompletionPayload::InvocationId(get_str(&mut b)?),
                    2 => CompletionPayload::Value(get_bytes(&mut b)?),
                    3 => CompletionPayload::Failure {
                        code: get_u32(&mut b)?,
                        message: get_str(&mut b)?,
                    },
                    4 => CompletionPayload::Void,
                    other => return Err(OutboxError::UnknownKind(other)),
                };
                Ok(Self::Signal(DeliverSignal {
                    target,
                    sender,
                    entry_index,
                    signal,
                    result,
                    tenant: tenant_tail(&mut b)?,
                }))
            }
            KIND_CANCEL => Ok(Self::Cancel(DeliverCancel {
                target: get_id(&mut b)?,
                parent: get_id(&mut b)?,
                id_completion: get_u32(&mut b)?,
            })),
            other => Err(OutboxError::UnknownKind(other)),
        }
    }
}

// ───────────────────────────────────────────────────────── codec helpers

fn put_str(b: &mut BytesMut, s: &str) {
    b.put_u32(s.len() as u32);
    b.put_slice(s.as_bytes());
}

fn put_bytes(b: &mut BytesMut, v: &[u8]) {
    b.put_u32(v.len() as u32);
    b.put_slice(v);
}

fn put_opt_str(b: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => {
            b.put_u8(1);
            put_str(b, s);
        }
        None => b.put_u8(0),
    }
}

fn get_len(b: &mut Bytes) -> Result<usize, OutboxError> {
    if b.remaining() < 4 {
        return Err(OutboxError::Truncated);
    }
    let n = b.get_u32() as usize;
    if b.remaining() < n {
        return Err(OutboxError::Truncated);
    }
    Ok(n)
}

/// The same handling as `outbox`: broken bytes become replacement characters,
/// not an error. A service name with odd bytes still routes somewhere and
/// fails where it can be seen; rejecting the whole message would leave that
/// row stuck forever.
fn get_str(b: &mut Bytes) -> Result<String, OutboxError> {
    let n = get_len(b)?;
    let raw = b.split_to(n);
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

fn get_bytes(b: &mut Bytes) -> Result<Bytes, OutboxError> {
    let n = get_len(b)?;
    Ok(b.split_to(n))
}

fn get_opt_str(b: &mut Bytes) -> Result<Option<String>, OutboxError> {
    if b.remaining() < 1 {
        return Err(OutboxError::Truncated);
    }
    match b.get_u8() {
        0 => Ok(None),
        _ => Ok(Some(get_str(b)?)),
    }
}

fn get_u32(b: &mut Bytes) -> Result<u32, OutboxError> {
    if b.remaining() < 4 {
        return Err(OutboxError::Truncated);
    }
    Ok(b.get_u32())
}

impl InterPartition {
    /// The body of a **batch** of messages to one range (route
    /// `POST /internal/inbound/batch`): a `u32` message count, then for each
    /// message a `u32` length and exactly the bytes of
    /// [`InterPartition::encode`]. The frame has no version of its own: each
    /// message carries its own version, the frame is only a concatenation.
    ///
    /// Takes **already encoded** messages so the sender can measure the batch
    /// size (the byte cap) without encoding twice.
    pub fn encode_batch(encoded: &[Bytes]) -> Bytes {
        let len = Self::BATCH_HEADER
            + encoded
                .iter()
                .map(|e| Self::BATCH_FRAME + e.len())
                .sum::<usize>();
        let mut b = BytesMut::with_capacity(len);
        b.put_u32(encoded.len() as u32);
        for e in encoded {
            b.put_u32(e.len() as u32);
            b.put_slice(e);
        }
        b.freeze()
    }

    /// Inverse of [`Self::encode_batch`]. Any broken message ⇒ the whole batch is
    /// broken: the receiver rejects the whole batch (all or nothing), it does
    /// not accept the readable part.
    pub fn decode_batch(raw: Bytes) -> Result<Vec<InterPartition>, OutboxError> {
        let mut b = raw;
        let count = get_u32(&mut b)? as usize;
        // Every message has at least one length frame: stops a made-up count before allocating.
        if count > b.remaining() / Self::BATCH_FRAME {
            return Err(OutboxError::Truncated);
        }
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let len = get_u32(&mut b)? as usize;
            if b.remaining() < len {
                return Err(OutboxError::Truncated);
            }
            out.push(Self::decode(b.split_to(len))?);
        }
        if b.has_remaining() {
            return Err(OutboxError::Truncated);
        }
        Ok(out)
    }

    /// Bytes of a batch header ([`Self::encode_batch`]): the message count.
    pub const BATCH_HEADER: usize = 4;
    /// Frame bytes of each message in a batch, besides its body: the length.
    pub const BATCH_FRAME: usize = 4;
}

fn get_id(b: &mut Bytes) -> Result<InvocationId, OutboxError> {
    if b.remaining() < cairn_store::ID_LEN {
        return Err(OutboxError::Truncated);
    }
    let mut id = [0u8; cairn_store::ID_LEN];
    b.copy_to_slice(&mut id);
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_store::{make_invocation_id, ClusterConfig, PartitionId, PartitionKey};

    /// A key that falls into partition `p` of a cluster of `n` evenly split partitions.
    fn key_in(n: u16, p: usize) -> PartitionKey {
        PartitionKey(ClusterConfig::equal(n).table().ranges()[p].start)
    }

    /// A `Completion` message carrying a segmented output (code 5): the round
    /// trip keeps ref, tail and source owner; a cut anywhere ⇒ error.
    #[test]
    fn segmented_completion_round_trips() {
        let m = InterPartition::Completion(DeliverCompletion {
            target: [0x33; 24],
            completion_id: 2,
            result: CompletionPayload::Segmented(crate::SegmentedOutput {
                tenant: cairn_store::TenantId::new("acme").unwrap(),
                pk: PartitionKey(5),
                owner: cairn_store::value::Owner([6; 32]),
                value: cairn_protocol::value::Logical::Segmented {
                    refs: vec![cairn_protocol::value::SegmentRef {
                        sha256: [1; 32],
                        len: 300_000,
                        gen: 4,
                    }],
                    tail: Bytes::from_static(b"\""),
                    len: 300_001,
                },
            }),
        });
        let raw = m.encode();
        assert_eq!(InterPartition::decode(raw.clone()).unwrap(), m);
        for n in 0..raw.len() {
            assert!(InterPartition::decode(raw.slice(..n)).is_err(), "cut {n}");
        }
    }

    /// A batch round trip keeps exactly order and content; an empty batch is
    /// valid; a cut anywhere, trailing bytes or a made-up count break the
    /// whole batch.
    #[test]
    fn batch_round_trips_in_order_and_rejects_any_damage() {
        let msgs = [
            start(),
            InterPartition::Cancel(DeliverCancel {
                target: make_invocation_id(key_in(8, 7), [1; 16]),
                parent: make_invocation_id(key_in(8, 2), [9; 16]),
                id_completion: 3,
            }),
            start(),
        ];
        let encoded: Vec<Bytes> = msgs.iter().map(InterPartition::encode).collect();
        let body = InterPartition::encode_batch(&encoded);
        let back = InterPartition::decode_batch(body.clone()).expect("intact batch");
        assert_eq!(back.len(), msgs.len());
        for (a, b) in back.iter().zip(&encoded) {
            assert_eq!(&a.encode(), b, "order and content kept");
        }
        assert!(
            InterPartition::decode_batch(InterPartition::encode_batch(&[]))
                .expect("empty batch")
                .is_empty()
        );
        for cut in 0..body.len() {
            assert!(
                InterPartition::decode_batch(body.slice(..cut)).is_err(),
                "a batch cut at byte {cut} must break the whole batch"
            );
        }
        let mut longer = body.to_vec();
        longer.push(0);
        assert!(
            InterPartition::decode_batch(Bytes::from(longer)).is_err(),
            "trailing bytes"
        );
        let mut bogus = BytesMut::new();
        bogus.put_u32(u32::MAX);
        assert!(
            InterPartition::decode_batch(bogus.freeze()).is_err(),
            "made-up count"
        );
    }

    fn start() -> InterPartition {
        InterPartition::StartCall(StartCall {
            callee: make_invocation_id(key_in(8, 7), [1; 16]),
            service: "cart".into(),
            handler: "add".into(),
            object_key: "user-1".into(),
            parameter: Bytes::from_static(b"{}"),
            reply_to: Some((make_invocation_id(key_in(8, 2), [9; 16]), 4)),
            idempotency_key: Some("idem".into()),
            scope: Some("acme".into()),
            limit_key: Some("growth/alice".into()),
            sender: (make_invocation_id(key_in(8, 2), [9; 16]), 3),
            // The round trip carries the tenant too (v5).
            tenant: Some("acme-corp".into()),
            schedule: None,
            lineage: Default::default(),
        })
    }

    /// **Lineage is a tail block**: a message with lineage = exactly the bytes
    /// of the message without it, plus the tail — a reader that stops at the
    /// last field it knows (an old binary) reads the same message; a new reader
    /// round-trips it fully.
    #[test]
    fn lineage_is_a_tail() {
        for schedule in [None, Some("nightly@1".to_string())] {
            let InterPartition::StartCall(mut s) = start() else {
                unreachable!()
            };
            s.schedule = schedule;
            let plain = InterPartition::StartCall(s.clone()).encode();
            *s.lineage = crate::lineage::Lineage {
                root_id: Some([5; 24]),
                tags: vec![("team".into(), "pay".into())],
            };
            let with = InterPartition::StartCall(s.clone());
            let bytes = with.encode();
            assert!(bytes.starts_with(&plain), "tail after the old layout");
            assert_eq!(bytes[0], plain[0], "version unchanged");
            assert_eq!(InterPartition::decode(bytes).unwrap(), with);
        }
    }

    /// **v6**: `schedule` round-trips; a message with it is written at v6, one
    /// without it at v5 — the same bytes as before, readable by old binaries.
    #[test]
    fn schedule_roundtrip_only_fire_bumps_version() {
        let InterPartition::StartCall(mut s) = start() else {
            unreachable!()
        };
        let v5 = InterPartition::StartCall(s.clone()).encode();
        assert_eq!(v5[0], 5);
        s.schedule = Some("nightly@1700000000000".into());
        let m = InterPartition::StartCall(s);
        let raw = m.encode();
        assert_eq!(raw[0], 6);
        assert_eq!(&raw[1..v5.len()], &v5[1..], "v6 = v5 + tail");
        assert_eq!(InterPartition::decode(raw.clone()).unwrap(), m);
        for n in 0..raw.len() {
            let _ = InterPartition::decode(raw.slice(..n));
        }
    }

    fn completion() -> InterPartition {
        InterPartition::Completion(DeliverCompletion {
            target: make_invocation_id(key_in(8, 2), [9; 16]),
            completion_id: 4,
            result: CompletionPayload::Value(Bytes::from_static(b"ok")),
        })
    }

    #[test]
    fn there_and_back() {
        for m in [start(), completion()] {
            assert_eq!(InterPartition::decode(m.encode()).unwrap(), m);
        }
        // Including the variants that pass through least often.
        let one_way = InterPartition::StartCall(StartCall {
            reply_to: None,
            idempotency_key: None,
            ..match start() {
                InterPartition::StartCall(s) => s,
                _ => unreachable!(),
            }
        });
        assert_eq!(InterPartition::decode(one_way.encode()).unwrap(), one_way);

        for payload in [
            CompletionPayload::InvocationId("abcd".into()),
            CompletionPayload::Failure {
                code: 500,
                message: "broken".into(),
            },
        ] {
            let m = InterPartition::Completion(DeliverCompletion {
                target: [3; 24],
                completion_id: 1,
                result: payload,
            });
            assert_eq!(InterPartition::decode(m.encode()).unwrap(), m);
        }
    }

    /// Two **different calls** to the same deterministic callee (a workflow's
    /// `run` turn: `ctx.send` then `ctx.call` with the same key) must have two
    /// inbound keys — with a shared key the later message overwrites the
    /// earlier one, and the send vanishes silently. Resending the *same* call
    /// still shares the key.
    #[test]
    fn two_calls_same_callee_different_keys() {
        let base = match start() {
            InterPartition::StartCall(s) => s,
            _ => unreachable!(),
        };
        let send = InterPartition::StartCall(StartCall {
            reply_to: None,
            sender: (base.sender.0, 1),
            ..base.clone()
        });
        let call = InterPartition::StartCall(StartCall {
            reply_to: Some((base.sender.0, 2)),
            sender: (base.sender.0, 2),
            ..base.clone()
        });
        assert_ne!(send.inbound_key(), call.inbound_key());
        assert_eq!(call.inbound_key(), call.clone().inbound_key());
    }

    /// The target is read straight from the id — no lookup, no configuration.
    #[test]
    fn target_read_from_id() {
        let c = ClusterConfig::equal(8);
        assert_eq!(start().destination(&c), PartitionId(7));
        assert_eq!(completion().destination(&c), PartitionId(2));
    }

    /// **The key must be a function of the work to do, not of the send.**
    ///
    /// Resending the same message must give the same key, otherwise the second
    /// row is processed as new work — and for `Completion`, "processing twice"
    /// means delivering the same completion id twice, which kills the
    /// invocation under the service protocol.
    #[test]
    fn resend_same_message_same_key() {
        assert_eq!(start().inbound_key(), start().inbound_key());
        assert_eq!(completion().inbound_key(), completion().inbound_key());

        // And two different completions of the same invocation have different keys.
        let InterPartition::Completion(mut c) = completion() else {
            unreachable!()
        };
        c.completion_id = 5;
        assert_ne!(
            InterPartition::Completion(c).inbound_key(),
            completion().inbound_key()
        );
    }

    /// **v7**: `Attach`/`PeekOutput`/`Signal` carry the caller's tenant and
    /// round-trip; an old v5 message (no tail) decodes as `None` — `default`;
    /// another kind at v7 is a broken message.
    #[test]
    fn watch_and_signal_carry_tenant() {
        let w = Watch {
            target: make_invocation_id(key_in(8, 2), [9; 16]),
            caller: make_invocation_id(key_in(8, 5), [4; 16]),
            completion_id: 3,
            tenant: Some("alpha".into()),
        };
        let s = DeliverSignal {
            target: w.target,
            sender: w.caller,
            entry_index: 7,
            signal: SignalId::Idx(17),
            result: CompletionPayload::Value(Bytes::from_static(b"v")),
            tenant: Some("alpha".into()),
        };
        for m in [
            InterPartition::Attach(w.clone()),
            InterPartition::PeekOutput(w.clone()),
            InterPartition::Signal(s.clone()),
        ] {
            let raw = m.encode();
            assert_eq!(raw[0], VERSION_TENANT);
            assert_eq!(InterPartition::decode(raw.clone()).unwrap(), m);
            for n in 0..raw.len() {
                let _ = InterPartition::decode(raw.slice(..n));
            }
            // A v5 message: same layout, no tenant tail.
            let mut old = m.clone();
            match &mut old {
                InterPartition::Attach(w) | InterPartition::PeekOutput(w) => w.tenant = None,
                InterPartition::Signal(s) => s.tenant = None,
                _ => unreachable!(),
            }
            let none = old.encode();
            let mut v5 = BytesMut::from(&none[..none.len() - 1]);
            v5[0] = 5;
            let back = InterPartition::decode(v5.freeze()).unwrap();
            assert_eq!(back, old, "a v5 message decodes as tenant None");
            let tenant = match &back {
                InterPartition::Attach(w) | InterPartition::PeekOutput(w) => w.tenant.clone(),
                InterPartition::Signal(s) => s.tenant.clone(),
                _ => unreachable!(),
            };
            assert_eq!(
                tenant_on_wire(tenant.as_deref()),
                Some(cairn_store::TenantId::default())
            );
        }
        assert_eq!(tenant_on_wire(Some("")), None, "empty matches no tenant");
        assert_eq!(wire_tenant(None).as_deref(), Some(""));
        // Other kinds have no v7.
        let mut c = BytesMut::from(&completion().encode()[..]);
        c[0] = VERSION_TENANT;
        assert!(matches!(
            InterPartition::decode(c.freeze()),
            Err(OutboxError::UnknownVersion(7))
        ));
    }

    #[test]
    fn truncated_does_not_panic() {
        for m in [start(), completion()] {
            let full = m.encode();
            for n in 0..full.len() {
                let _ = InterPartition::decode(full.slice(..n));
            }
        }
    }
}
