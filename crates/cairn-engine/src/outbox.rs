// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `outbox` table — work that must be sent out.
//!
//! **This is the table that enforces INV-3 most visibly.** An outgoing call
//! that exists only in the leader's RAM is lost on failover. Written to the
//! outbox in the *same* commit as the journal entry that produced it, it
//! survives, and the new leader picks it up.
//!
//! The format has a **version byte first**. This is durable data that will be
//! read back after a binary upgrade, so "can read what it once wrote" must be a
//! checkable property, not a hope.

use {
    crate::engine::InvocationId,
    bytes::{Buf, BufMut, Bytes, BytesMut},
    cairn_store::PartitionKey,
};

/// The version we **write**.
///
/// v2 changes exactly one thing from v1: `result_completion` carries an
/// explicit "present" flag instead of using 0 as the absence marker. See
/// [`OutboxEntry::decode`].
///
/// v3 **appends** `scope` and `limit_key` — the two new fields of
/// `CallCommandMessage` (7, 8). Appending rather than inserting in the middle
/// is what lets the read path branch on exactly one version comparison, and
/// lets a v2 row (a call in flight during a binary upgrade) read as `None`
/// instead of garbage.
///
/// v4: 24-byte ids (partition key + 16 bytes) — the 2026-10 refactor.
///
/// v5 **appends** the call's `tenant`. A v4 row reads as tenant `default` —
/// the right tenant for everything written before tenants existed.
///
/// v6 **appends** `callee_pk` to the `Call` row: the callee's placement under
/// the prefix-grouping rule. A binary that knows up to migration step 3 reads
/// `VERSION_MIN..=5` strictly and returns `UnknownVersion(6)`, so v6 is
/// written **only** when the row really carries `callee_pk` — which the engine
/// sets only for callers created once the `calls/prefix` gate is on, i.e. when
/// every binary of the cluster already `knows ≥ 4`. Every other row (including
/// a `Call` without `callee_pk`) is still written at [`VERSION_NO_CALLEE_PK`]:
/// the same bytes as before `callee_pk` existed, readable by old binaries.
const VERSION: u8 = 6;

/// The version of every row that does not carry `callee_pk` — the v5 layout, unchanged.
const VERSION_NO_CALLEE_PK: u8 = 5;

/// v7: the schedule's `Fire` row — a new kind, written at its own version so a
/// binary that knows up to v6 rejects it with `UnknownVersion(7)` (the row
/// stays, like every unreadable row). Only written once the `schedules/table`
/// gate is on, i.e. every binary of the cluster already `knows ≥ 7`. Every
/// other kind keeps its old version.
const VERSION_FIRE: u8 = 7;

/// v8: a `Reply` row carrying a segmented output — written at its own version
/// so a binary that knows up to v7 rejects it (`UnknownVersion(8)`, the row
/// stays) instead of reading an unknown result code as `Failure`. Only written
/// once the `values/segmented` gate is on. Every other row keeps its old
/// version.
const VERSION_VALUES: u8 = 8;

/// The highest version that **can be read**.
const VERSION_MAX: u8 = VERSION_VALUES;

/// The oldest version that can still **be read**.
///
/// v4 cannot read v1–v3 rows: the id length changed, so an old row has no way
/// to decode correctly. The 2026-10 refactor broke the format on purpose — the
/// bucket carries a `DATA_FORMAT` mark and this binary refuses to open an old
/// bucket, so an old row never gets here. The v1–v3 read branches below remain
/// only because branching on version is still the right shape for the next
/// appended field.
const VERSION_MIN: u8 = 4;

const KIND_CALL: u8 = 1;
const KIND_REPLY: u8 = 2;
const KIND_ATTACH: u8 = 3;
const KIND_PEEK_OUTPUT: u8 = 4;
/// Cancellation propagated down to a child — see [`OutboundCancel`].
///
/// **A new kind, no `VERSION` bump.** The version describes the *field layout*
/// of an existing kind; the kind is a different axis. An old binary that reads
/// this row returns `UnknownKind(5)` and leaves it in the table — exactly the
/// failure mode we want, because a row that stays can be picked up by the new
/// version. The same reasoning as `KIND_SIGNAL` of cross-partition messages.
const KIND_CANCEL: u8 = 5;
/// A cross-range fire of a schedule — see [`OutboundFire`].
const KIND_FIRE: u8 = 6;

/// The target of a call, and where to return the result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundCall {
    pub caller: InvocationId,
    /// The completion that receives the called invocation's id (`0x800E`), arrives immediately.
    pub id_completion: u32,
    /// The completion that receives the result (`0x800D`). `None` for `OneWayCall` — it does not wait.
    pub result_completion: Option<u32>,
    pub service: String,
    pub handler: String,
    pub object_key: String,
    pub parameter: Bytes,
    /// The absolute time to run; 0 = as soon as possible.
    pub invoke_time: u64,
    pub idempotency_key: Option<String>,
    /// The call's scope (`CallCommandMessage.scope`, field 7).
    ///
    /// A `String` rather than a `Scope`: this is durable bytes, and it must
    /// read back even when the value is no longer valid under the current
    /// rules. The check lives at the entry boundary (ingress, and where
    /// `Task::Invoke` is translated), not here.
    pub scope: Option<String>,
    /// Limit key (`CallCommandMessage.limit_key`, field 8). Only affects
    /// queueing — it is **not** part of the called invocation's identity.
    pub limit_key: Option<String>,
    /// The call's tenant = the **caller's** tenant: there are no cross-tenant
    /// calls. `None` = `default`. A `String` for the same reason as `scope`.
    pub tenant: Option<String>,
    /// The callee's partition key under the prefix-grouping rule (v6 row):
    /// `pk = (caller pk & high 32 bits) | hash`.
    ///
    /// Only present on calls without a scope, object key or idempotency key,
    /// from a caller carrying the `cairnflow-call-prefix` mark in entry 0
    /// (created once the `calls/prefix` gate was on). `None` (a v5 row, or a
    /// keyed tier) ⇒ `dispatch_call` uses the existing rule. Both are functions
    /// of the call's content and the caller's immutable mark, so every rewrite
    /// of the row (replaying `Task::Invoke`) and every re-drain yields the same
    /// id even if the gate has changed since (IR-9).
    pub callee_pk: Option<PartitionKey>,
    /// The call-tree root and the callee's tags — a leniently read tail block
    /// ([`crate::lineage`]), no version bump.
    pub lineage: crate::lineage::Lineage,
}

/// The result of a called invocation, on its way back to the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundReply {
    pub caller: InvocationId,
    pub completion_id: u32,
    pub result: ReplyResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplyResult {
    Value(Bytes),
    /// A segmented output with its source owner: the shell delivers it —
    /// assembled inline or COPYed to the recipient's owner — never the engine.
    Segmented(crate::SegmentedOutput),
    Failure {
        code: u32,
        message: String,
    },
}

/// A request to wait for (attach) or look at (peek) another invocation's result.
///
/// Goes through the outbox because `target` may be on another partition — the
/// same reason as `Call`: the request must be durable before anyone carries it
/// off.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundWatch {
    pub caller: InvocationId,
    pub completion_id: u32,
    pub target: InvocationId,
}

/// "Cancel this child too."
///
/// Cancelling an invocation while leaving the ones it called untouched abandons
/// running work — and in a workload where every running invocation costs money,
/// that is abandoning a bill. Once the caller is dead nobody reads the results
/// anymore, so they only spend money.
///
/// Goes through the outbox rather than a direct call because `target` may be on
/// another partition — the same reason as [`OutboundCall`]. And because it is
/// durable, a death midway does not lose the cancel command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundCancel {
    /// The parent — only for keying the row and for reading logs backwards.
    pub caller: InvocationId,
    /// The completion the parent's `Call` uses to receive the child's id. It is
    /// deterministic and unique in the parent's journal, so it is the natural
    /// row key: a replay overwrites, it does not spawn a second cancel command.
    pub id_completion: u32,
    pub target: InvocationId,
}

/// A schedule fire whose run is on **another range** (a virtual-object or
/// scoped target). Written in the same commit as the schedule row that moved
/// `next_at` (CG-12); the drainer sends it as a `StartCall` with idempotency
/// key `schedule/<name>/<instant>` — the target range deduplicates (CG-10).
///
/// Row key: `(schedule id, fire_seq)` in the `(caller, completion)` columns —
/// the schedule id is 24 bytes like an invocation id, the owner is the
/// schedule's pk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundFire {
    pub schedule: InvocationId,
    pub fire_seq: u32,
    /// The schedule name and nominal instant — the run's `cairnflow-schedule`
    /// header and the idempotency key.
    pub name: String,
    pub at: u64,
    pub tenant: Option<String>,
    pub scope: Option<String>,
    pub service: String,
    pub handler: String,
    pub object_key: String,
    pub parameter: Bytes,
    pub limit_key: Option<String>,
    /// The idempotency key the target range deduplicates on:
    /// `schedule/<name>/<instant>` for a schedule instant,
    /// `schedule/<name>/manual/<request id>` for a manual fire.
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxEntry {
    Call(OutboundCall),
    Reply(OutboundReply),
    /// `AttachInvocation` — wait until the target finishes.
    Attach(OutboundWatch),
    /// `GetInvocationOutput` — answer right away: the result, or "not done yet".
    PeekOutput(OutboundWatch),
    /// Cancellation propagated down to a child.
    Cancel(OutboundCancel),
    /// A cross-range fire of a schedule.
    Fire(OutboundFire),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OutboxError {
    #[error("empty outbox entry")]
    Empty,
    #[error("outbox format version {0}; this binary reads only {VERSION_MIN}..={VERSION_MAX}")]
    UnknownVersion(u8),
    #[error("unknown outbox kind {0}")]
    UnknownKind(u8),
    #[error("truncated outbox entry")]
    Truncated,
}

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
        Some(v) => {
            b.put_u8(1);
            put_str(b, v);
        }
        None => b.put_u8(0),
    }
}

/// A "present" flag first, then the value — **never** use some value as a marker.
///
/// The temptation is to use 0 as "absent", because the TypeScript SDK issues
/// completion ids from 1. But the protocol only requires ids to be **unique
/// within an invocation**; nothing says they start at 1, and the runtime has
/// no right to dictate which numbers an SDK may not use — the same argument
/// that separated the `retries` table from `timers`.
///
/// With 0 as the marker: an SDK that issues ids from 0 would have its
/// `ctx.call` silently turn into `ctx.send` — the callee finishes without
/// knowing where to return the result, the caller hangs forever, and there is
/// no error to see.
fn put_opt_u32(b: &mut BytesMut, v: Option<u32>) {
    match v {
        Some(n) => {
            b.put_u8(1);
            b.put_u32(n);
        }
        None => b.put_u8(0),
    }
}

fn get_opt_u32(b: &mut Bytes) -> Result<Option<u32>, OutboxError> {
    if b.remaining() < 1 {
        return Err(OutboxError::Truncated);
    }
    match b.get_u8() {
        0 => Ok(None),
        _ => Ok(Some(get_u32(b)?)),
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

fn get_str(b: &mut Bytes) -> Result<String, OutboxError> {
    let n = get_len(b)?;
    let raw = b.copy_to_bytes(n);
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

fn get_bytes(b: &mut Bytes) -> Result<Bytes, OutboxError> {
    let n = get_len(b)?;
    Ok(b.copy_to_bytes(n))
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

fn get_u64(b: &mut Bytes) -> Result<u64, OutboxError> {
    if b.remaining() < 8 {
        return Err(OutboxError::Truncated);
    }
    Ok(b.get_u64())
}

fn get_id(b: &mut Bytes) -> Result<InvocationId, OutboxError> {
    if b.remaining() < cairn_store::ID_LEN {
        return Err(OutboxError::Truncated);
    }
    let mut id = [0u8; cairn_store::ID_LEN];
    b.copy_to_slice(&mut id);
    Ok(id)
}

impl OutboxEntry {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(128);
        b.put_u8(match self {
            Self::Call(OutboundCall {
                callee_pk: Some(_), ..
            }) => VERSION,
            Self::Fire(_) => VERSION_FIRE,
            Self::Reply(OutboundReply {
                result: ReplyResult::Segmented(_),
                ..
            }) => VERSION_VALUES,
            _ => VERSION_NO_CALLEE_PK,
        });
        match self {
            Self::Call(c) => {
                b.put_u8(KIND_CALL);
                b.put_slice(&c.caller);
                b.put_u32(c.id_completion);
                put_opt_u32(&mut b, c.result_completion);
                put_str(&mut b, &c.service);
                put_str(&mut b, &c.handler);
                put_str(&mut b, &c.object_key);
                put_bytes(&mut b, &c.parameter);
                b.put_u64(c.invoke_time);
                put_opt_str(&mut b, c.idempotency_key.as_deref());
                put_opt_str(&mut b, c.scope.as_deref());
                put_opt_str(&mut b, c.limit_key.as_deref());
                put_opt_str(&mut b, c.tenant.as_deref());
                // v6: only when present — a row without it stops here, exactly the v5 layout.
                if let Some(pk) = c.callee_pk {
                    b.put_u8(1);
                    b.put_u64(pk.0);
                }
                // The lineage tail block: an old binary stops at the field above.
                c.lineage.encode_tail(&mut b);
            }
            Self::Reply(r) => {
                b.put_u8(KIND_REPLY);
                b.put_slice(&r.caller);
                b.put_u32(r.completion_id);
                match &r.result {
                    ReplyResult::Value(v) => {
                        b.put_u8(0);
                        put_bytes(&mut b, v);
                    }
                    ReplyResult::Failure { code, message } => {
                        b.put_u8(1);
                        b.put_u32(*code);
                        put_str(&mut b, message);
                    }
                    ReplyResult::Segmented(o) => {
                        b.put_u8(2);
                        o.encode_into(&mut b);
                    }
                }
            }
            Self::Attach(w) | Self::PeekOutput(w) => {
                b.put_u8(match self {
                    Self::Attach(_) => KIND_ATTACH,
                    _ => KIND_PEEK_OUTPUT,
                });
                b.put_slice(&w.caller);
                b.put_u32(w.completion_id);
                b.put_slice(&w.target);
            }
            Self::Cancel(c) => {
                b.put_u8(KIND_CANCEL);
                b.put_slice(&c.caller);
                b.put_u32(c.id_completion);
                b.put_slice(&c.target);
            }
            Self::Fire(f) => {
                b.put_u8(KIND_FIRE);
                b.put_slice(&f.schedule);
                b.put_u32(f.fire_seq);
                put_str(&mut b, &f.name);
                b.put_u64(f.at);
                put_opt_str(&mut b, f.tenant.as_deref());
                put_opt_str(&mut b, f.scope.as_deref());
                put_str(&mut b, &f.service);
                put_str(&mut b, &f.handler);
                put_str(&mut b, &f.object_key);
                put_bytes(&mut b, &f.parameter);
                put_opt_str(&mut b, f.limit_key.as_deref());
                put_str(&mut b, &f.idempotency_key);
            }
        }
        b.freeze()
    }

    pub fn decode(mut b: Bytes) -> Result<Self, OutboxError> {
        if b.remaining() < 2 {
            return Err(OutboxError::Empty);
        }
        let version = b.get_u8();
        if !(VERSION_MIN..=VERSION_MAX).contains(&version) {
            return Err(OutboxError::UnknownVersion(version));
        }
        match b.get_u8() {
            KIND_FIRE if version >= VERSION_FIRE => Ok(Self::Fire(OutboundFire {
                schedule: get_id(&mut b)?,
                fire_seq: get_u32(&mut b)?,
                name: get_str(&mut b)?,
                at: get_u64(&mut b)?,
                tenant: get_opt_str(&mut b)?,
                scope: get_opt_str(&mut b)?,
                service: get_str(&mut b)?,
                handler: get_str(&mut b)?,
                object_key: get_str(&mut b)?,
                parameter: get_bytes(&mut b)?,
                limit_key: get_opt_str(&mut b)?,
                idempotency_key: get_str(&mut b)?,
            })),
            KIND_CALL => {
                let caller = get_id(&mut b)?;
                let id_completion = get_u32(&mut b)?;
                let result_completion = if version == 1 {
                    // v1 used 0 as the absence marker. Keep the old reading
                    // for old rows — the mistake here would be to *read* a v1
                    // row with v2 rules, turning the 4 id bytes into a flag
                    // and three bytes of garbage.
                    match get_u32(&mut b)? {
                        0 => None,
                        n => Some(n),
                    }
                } else {
                    get_opt_u32(&mut b)?
                };
                let service = get_str(&mut b)?;
                let handler = get_str(&mut b)?;
                let object_key = get_str(&mut b)?;
                let parameter = get_bytes(&mut b)?;
                let invoke_time = get_u64(&mut b)?;
                let idempotency_key = get_opt_str(&mut b)?;
                // v1/v2 rows stop here. Reading further reads garbage — and a
                // garbage scope is a call routed to the wrong partition, i.e. a
                // hang forever.
                let (scope, limit_key) = if version >= 3 {
                    (get_opt_str(&mut b)?, get_opt_str(&mut b)?)
                } else {
                    (None, None)
                };
                let tenant = if version >= 5 {
                    get_opt_str(&mut b)?
                } else {
                    None
                };
                // A v5 row stops above: the old rule, forever (IR-9).
                let callee_pk = if version >= 6 {
                    if b.remaining() < 1 {
                        return Err(OutboxError::Truncated);
                    }
                    match b.get_u8() {
                        0 => None,
                        _ => Some(PartitionKey(get_u64(&mut b)?)),
                    }
                } else {
                    None
                };
                let lineage = crate::lineage::Lineage::decode_tail(&mut b);
                Ok(Self::Call(OutboundCall {
                    caller,
                    id_completion,
                    result_completion,
                    service,
                    handler,
                    object_key,
                    parameter,
                    invoke_time,
                    idempotency_key,
                    scope,
                    limit_key,
                    tenant,
                    callee_pk,
                    lineage,
                }))
            }
            KIND_REPLY => {
                let caller = get_id(&mut b)?;
                let completion_id = get_u32(&mut b)?;
                if b.remaining() < 1 {
                    return Err(OutboxError::Truncated);
                }
                let result = match b.get_u8() {
                    0 => ReplyResult::Value(get_bytes(&mut b)?),
                    2 if version >= VERSION_VALUES => ReplyResult::Segmented(
                        crate::SegmentedOutput::decode_from(&mut b)
                            .ok_or(OutboxError::Truncated)?,
                    ),
                    _ => ReplyResult::Failure {
                        code: get_u32(&mut b)?,
                        message: get_str(&mut b)?,
                    },
                };
                Ok(Self::Reply(OutboundReply {
                    caller,
                    completion_id,
                    result,
                }))
            }
            kind @ (KIND_ATTACH | KIND_PEEK_OUTPUT) => {
                let w = OutboundWatch {
                    caller: get_id(&mut b)?,
                    completion_id: get_u32(&mut b)?,
                    target: get_id(&mut b)?,
                };
                Ok(match kind {
                    KIND_ATTACH => Self::Attach(w),
                    _ => Self::PeekOutput(w),
                })
            }
            KIND_CANCEL => Ok(Self::Cancel(OutboundCancel {
                caller: get_id(&mut b)?,
                id_completion: get_u32(&mut b)?,
                target: get_id(&mut b)?,
            })),
            other => Err(OutboxError::UnknownKind(other)),
        }
    }

    pub fn caller(&self) -> InvocationId {
        match self {
            Self::Call(c) => c.caller,
            Self::Reply(r) => r.caller,
            Self::Attach(w) | Self::PeekOutput(w) => w.caller,
            Self::Cancel(c) => c.caller,
            Self::Fire(f) => f.schedule,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: InvocationId = [0xAA; 24];

    fn call() -> OutboxEntry {
        OutboxEntry::Call(OutboundCall {
            caller: A,
            id_completion: 3,
            result_completion: Some(4),
            service: "PaymentService".into(),
            handler: "charge".into(),
            object_key: "acct-1".into(),
            parameter: Bytes::from_static(b"{\"amount\":100}"),
            invoke_time: 1_700_000_000_000,
            idempotency_key: Some("idem-1".into()),
            scope: Some("acme".into()),
            limit_key: Some("growth/alice".into()),
            // The round trip carries the tenant too (v5).
            tenant: Some("acme-corp".into()),
            callee_pk: None,
            lineage: Default::default(),
        })
    }

    /// **Lineage is a tail block**: a `Call` row with lineage = exactly the
    /// bytes of the row without it, plus the tail, at the same version — v5 and v6.
    fn segmented() -> crate::SegmentedOutput {
        crate::SegmentedOutput {
            tenant: cairn_store::TenantId::new("acme").unwrap(),
            pk: PartitionKey(77),
            owner: cairn_store::value::Owner([4; 32]),
            value: cairn_protocol::value::Logical::Segmented {
                refs: vec![cairn_protocol::value::SegmentRef {
                    sha256: [8; 32],
                    len: 1 << 20,
                    gen: 9,
                }],
                tail: Bytes::from_static(b"]"),
                len: (1 << 20) + 1,
            },
        }
    }

    /// A segmented `Reply` row: its own version (8) so that old binaries
    /// reject it instead of reading it as `Failure`; the round trip keeps the
    /// source owner.
    #[test]
    fn segmented_reply_round_trips_at_its_own_version() {
        let e = OutboxEntry::Reply(OutboundReply {
            caller: A,
            completion_id: 3,
            result: ReplyResult::Segmented(segmented()),
        });
        let raw = e.encode();
        assert_eq!(raw[0], VERSION_VALUES);
        assert_eq!(OutboxEntry::decode(raw.clone()).unwrap(), e);
        for n in 0..raw.len() {
            assert!(OutboxEntry::decode(raw.slice(..n)).is_err(), "cut at {n}");
        }
        // Result code 2 at an old version: not read as a ref (only v8 carries it).
        let mut old = raw.to_vec();
        old[0] = VERSION_NO_CALLEE_PK;
        assert_ne!(OutboxEntry::decode(Bytes::from(old)).ok(), Some(e));
    }

    #[test]
    fn call_lineage_is_a_tail() {
        for callee_pk in [None, Some(PartitionKey(42))] {
            let OutboxEntry::Call(mut c) = call() else {
                unreachable!()
            };
            c.callee_pk = callee_pk;
            let plain = OutboxEntry::Call(c.clone()).encode();
            c.lineage = crate::lineage::Lineage {
                root_id: Some([5; 24]),
                tags: vec![("team".into(), "pay".into())],
            };
            let with = OutboxEntry::Call(c);
            let bytes = with.encode();
            assert!(bytes.starts_with(&plain), "tail after the old layout");
            assert_eq!(bytes[0], plain[0], "version unchanged");
            assert_eq!(OutboxEntry::decode(bytes).unwrap(), with);
        }
    }

    /// **v7**: a `Fire` row round-trips, written at v7; every other kind keeps
    /// its old version. A `Fire` row at an older version ⇒ unknown kind.
    #[test]
    fn fire_roundtrips_at_v7() {
        let f = OutboxEntry::Fire(OutboundFire {
            schedule: A,
            fire_seq: 9,
            name: "nightly".into(),
            at: 1_700_000_000_000,
            tenant: Some("acme".into()),
            scope: None,
            service: "Cart".into(),
            handler: "sweep".into(),
            object_key: "u1".into(),
            parameter: Bytes::from_static(b"{}"),
            limit_key: Some("growth".into()),
            idempotency_key: "schedule/nightly/1700000000000".into(),
        });
        let raw = f.encode();
        assert_eq!(raw[0], 7);
        assert_eq!(OutboxEntry::decode(raw.clone()).unwrap(), f);
        assert_eq!(f.caller(), A);
        for n in 0..raw.len() {
            let _ = OutboxEntry::decode(raw.slice(..n));
        }
        let mut older = raw.to_vec();
        older[0] = 6;
        assert_eq!(
            OutboxEntry::decode(Bytes::from(older)),
            Err(OutboxError::UnknownKind(6))
        );
        assert_eq!(call().encode()[0], 5, "Call without callee_pk is still v5");
    }

    #[test]
    fn call_roundtrip() {
        assert_eq!(OutboxEntry::decode(call().encode()).unwrap(), call());
    }

    /// **v6**: `callee_pk` round-trips; a row with it is written at v6, a row
    /// without it (including `Call`) at v5 — **the same bytes** as before
    /// `callee_pk` existed, so old binaries read every row without `callee_pk`.
    #[test]
    fn callee_pk_round_trips_and_only_it_bumps_the_version() {
        let OutboxEntry::Call(mut c) = call() else {
            unreachable!()
        };
        let v5 = OutboxEntry::Call(c.clone()).encode();
        assert_eq!(v5[0], 5);
        assert_eq!(
            v5,
            encode_v5(&c),
            "a row without callee_pk must be exactly the v5 layout"
        );
        c.callee_pk = Some(PartitionKey(0x1234_5678_9abc_def0));
        let e = OutboxEntry::Call(c.clone());
        let raw = e.encode();
        assert_eq!(raw[0], 6);
        assert_eq!(OutboxEntry::decode(raw.clone()).unwrap(), e);
        assert_eq!(&raw[..v5.len()][1..], &v5[1..], "v6 = v5 + tail");
        for n in 0..raw.len() {
            let _ = OutboxEntry::decode(raw.slice(..n));
        }
        // A version newer than any the binary knows ⇒ an explicit rejection,
        // not read as garbage — exactly what a binary that knows up to v5 does
        // with a v6 row (hence the gate).
        let mut newer = raw.to_vec();
        newer[0] = VERSION_MAX + 1;
        assert_eq!(
            OutboxEntry::decode(Bytes::from(newer)),
            Err(OutboxError::UnknownVersion(VERSION_MAX + 1))
        );
        // Other kinds never write v6.
        let reply = OutboxEntry::Reply(OutboundReply {
            caller: A,
            completion_id: 1,
            result: ReplyResult::Value(Bytes::new()),
        });
        assert_eq!(reply.encode()[0], 5);
    }

    /// The **v5** layout written by hand — a row in the store from before `callee_pk` existed.
    fn encode_v5(c: &OutboundCall) -> Bytes {
        let mut b = BytesMut::with_capacity(128);
        b.put_u8(5);
        b.put_u8(KIND_CALL);
        b.put_slice(&c.caller);
        b.put_u32(c.id_completion);
        put_opt_u32(&mut b, c.result_completion);
        put_str(&mut b, &c.service);
        put_str(&mut b, &c.handler);
        put_str(&mut b, &c.object_key);
        put_bytes(&mut b, &c.parameter);
        b.put_u64(c.invoke_time);
        put_opt_str(&mut b, c.idempotency_key.as_deref());
        put_opt_str(&mut b, c.scope.as_deref());
        put_opt_str(&mut b, c.limit_key.as_deref());
        put_opt_str(&mut b, c.tenant.as_deref());
        b.freeze()
    }

    #[test]
    fn one_way_roundtrip_awaits_no_result() {
        let OutboxEntry::Call(mut c) = call() else {
            unreachable!()
        };
        c.result_completion = None;
        c.idempotency_key = None;
        let e = OutboxEntry::Call(c);
        assert_eq!(OutboxEntry::decode(e.encode()).unwrap(), e);
    }

    #[test]
    fn reply_roundtrip() {
        for result in [
            ReplyResult::Value(Bytes::from_static(b"result")),
            ReplyResult::Value(Bytes::new()),
            ReplyResult::Failure {
                code: 500,
                message: "boom".into(),
            },
        ] {
            let e = OutboxEntry::Reply(OutboundReply {
                caller: A,
                completion_id: 7,
                result,
            });
            assert_eq!(OutboxEntry::decode(e.encode()).unwrap(), e);
        }
    }

    #[test]
    fn unknown_version_is_rejected_explicitly() {
        // This is durable data: an old binary that reads a new format must
        // report an error saying why, not guess and then break in a higher
        // layer.
        let mut raw = call().encode().to_vec();
        raw[0] = 99;
        assert_eq!(
            OutboxEntry::decode(Bytes::from(raw)),
            Err(OutboxError::UnknownVersion(99))
        );
    }

    #[test]
    fn completion_id_0_is_still_a_real_id() {
        // The protocol only requires completion ids to be **unique within an
        // invocation** — nothing says they start at 1. An SDK that issues ids
        // from 0 must be able to wait for results like any other SDK.
        //
        // If 0 were taken as the absence marker, `ctx.call` would silently
        // become `ctx.send`: the callee finishes without knowing where to
        // return, the caller hangs forever, and there is no error to see.
        let OutboxEntry::Call(mut c) = call() else {
            unreachable!()
        };
        c.id_completion = 0;
        c.result_completion = Some(0);
        let e = OutboxEntry::Call(c);

        let OutboxEntry::Call(back) = OutboxEntry::decode(e.encode()).unwrap() else {
            unreachable!()
        };
        assert_eq!(
            back.result_completion,
            Some(0),
            "id 0 swallowed as 'do not wait for the result'"
        );
        assert_eq!(back.id_completion, 0);
    }

    /// Encodes in **format v1** — a row already in the store before the upgrade.
    ///
    /// Written by hand rather than calling `encode()`: what we need to check is
    /// that the new binary reads the old bytes, and a shared encode function
    /// would automatically "agree" with every future change, i.e. check
    /// nothing at all.
    fn encode_v1(c: &OutboundCall) -> Bytes {
        let mut b = BytesMut::with_capacity(128);
        b.put_u8(1);
        b.put_u8(KIND_CALL);
        b.put_slice(&c.caller);
        b.put_u32(c.id_completion);
        b.put_u32(c.result_completion.unwrap_or(0)); // old rule: 0 = absent
        put_str(&mut b, &c.service);
        put_str(&mut b, &c.handler);
        put_str(&mut b, &c.object_key);
        put_bytes(&mut b, &c.parameter);
        b.put_u64(c.invoke_time);
        put_opt_str(&mut b, c.idempotency_key.as_deref());
        b.freeze()
    }

    /// A row of an old format is **rejected**, not read as garbage (v4, the
    /// 2026-10 refactor): the id length changed, so v1–v3 rows cannot decode
    /// correctly. An old bucket is stopped at the `DATA_FORMAT` mark before
    /// getting here anyway; this check is the second net.
    #[test]
    fn old_format_row_is_rejected() {
        let OutboxEntry::Call(c) = call() else {
            unreachable!()
        };
        assert!(matches!(
            OutboxEntry::decode(encode_v1(&c)),
            Err(OutboxError::UnknownVersion(1))
        ));
    }

    #[test]
    fn truncated_v1_row_does_not_panic() {
        let OutboxEntry::Call(c) = call() else {
            unreachable!()
        };
        let full = encode_v1(&c);
        for n in 0..full.len() {
            let _ = OutboxEntry::decode(full.slice(..n));
        }
    }

    #[test]
    fn truncated_entry_does_not_panic() {
        let full = call().encode();
        for n in 0..full.len() {
            // Must not panic at any length — broken data does happen, and a
            // panic here kills the engine thread with it.
            let _ = OutboxEntry::decode(full.slice(..n));
        }
    }
}
