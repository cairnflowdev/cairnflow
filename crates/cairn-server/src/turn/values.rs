// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Segmented values in the shell (wire ≤ v5).
//!
//! - [`seal_frame`]: a `SetState`/`GetEagerState` carrying a large value ⇒
//!   compare the prefix **by sha** with the segments of the key's current value
//!   (no GET at all), reuse the matching part, seal the whole remainder when it
//!   is ≥ `seal_bytes`, PUT it **before** the commit (AP-1) outside the engine
//!   (D16), then rewrite the frame as refs + tail. The engine receives it through
//!   `step_sealed` and checks the refs under the engine.
//! - [`assemble`]: every segmented `Value` in a message down to an SDK ≤ v5 is
//!   assembled inline **before** the frame is encoded — the bytes sent are
//!   exactly those of an inline journal, so the replay digest (wire v5) does not
//!   change. The engine never assembles.
//!
//! - [`prepare_down`] (wire v6): when the deployment negotiated v6 and the node
//!   has a `direct`/`proxy` read path ⇒ **no** assembly; the `Value` goes down
//!   exactly as in the journal, and each ref's read URL travels outside the
//!   entry (`Start.segment_urls`, the `SegmentUrls` frame). Otherwise ⇒
//!   [`assemble`].
//!
//! - [`seal_frame`] also seals `Output` and `ctx.run` results;
//!   [`deliver_output`] delivers a segmented output to the caller — assembled
//!   inline or COPYed under its owner.
//!
//! Only when the `values/segmented` gate is on when the range opens (`Engine::values_on`).

use {
    super::*,
    cairn_protocol::value::{self, Logical, SegmentRef},
    cairn_store::value::{Segment, ValueStore},
};

/// Default sealing threshold (break-even ≈ 0.38 MiB on `s3-standard`, matching
/// the eager-state cap). To be derived from the price profile later.
pub(super) const SEAL_BYTES: usize = 256 << 10;
/// Lower clamp: never seal below 64 KiB.
pub(super) const MIN_SEAL_BYTES: usize = 64 << 10;
/// The last bytes **not** sealed, kept as the tail: an appended value (chat
/// history `[…]`, a JSON string) changes its last few bytes every turn — the
/// closing `]`/`"`. Sealing them too would mean a segment is never a prefix of
/// the next value, and every turn would reseal O(H).
pub(super) const TAIL_RESERVE: usize = 4 << 10;
/// Maximum PUTs while exceeding `T_put` (like artifacts).
const PUT_ROUNDS: usize = 3;
/// The tier base and the number of segments merged at a time.
pub(super) const MERGE_FANOUT: usize = 4;
/// Size cap of one merged segment: never GET/PUT more than this within one turn.
pub(super) const MERGE_MAX: u64 = 16 << 20;

/// The tier of a segment: `⌊log_W(max(len, seal) / seal)⌋`, `W` = [`MERGE_FANOUT`].
fn tier(len: u64, seal: u64) -> u32 {
    let w = MERGE_FANOUT as u64;
    let mut t = 0;
    let mut bound = seal.max(1).saturating_mul(w);
    while len >= bound && bound < u64::MAX {
        t += 1;
        bound = bound.saturating_mul(w);
    }
    t
}

/// The merge rule on the size sequence `lens` (the last element is the segment
/// about to be PUT): `Some(start)` ⇔ the **last** [`MERGE_FANOUT`] elements all
/// have a tier ≤ the last element's tier and a total ≤ [`MERGE_MAX`] — merge
/// `lens[start..]` into one. Evenly sized seals ⇒ a base-`W` counter.
pub(super) fn merge_window(lens: &[u64], seal: u64) -> Option<usize> {
    let n = lens.len();
    if n < MERGE_FANOUT {
        return None;
    }
    let start = n - MERGE_FANOUT;
    let top = tier(lens[n - 1], seal);
    let window = &lens[start..];
    let total: u64 = window.iter().fold(0u64, |a, &l| a.saturating_add(l));
    (window.iter().all(|&l| tier(l, seal) <= top) && total <= MERGE_MAX).then_some(start)
}

fn store_err(e: cairn_store::StoreError) -> ActorError {
    ActorError::from(cairn_engine::EngineError::from(e))
}

/// The tenant's `seal_bytes`: `min(SEAL_BYTES, max-payload / 4)`, clamped below at
/// [`MIN_SEAL_BYTES`].
pub(super) fn seal_bytes(max_payload: Option<u64>) -> usize {
    let cap = max_payload.map_or(SEAL_BYTES, |m| (m / 4) as usize);
    SEAL_BYTES.min(cap).max(MIN_SEAL_BYTES)
}

/// `(pk, owner)` of `target`'s state value — `None` = no key (D-a).
fn owner_of(target: &Target) -> Option<(cairn_store::PartitionKey, cairn_store::value::Owner)> {
    cairn_store::value::object_owner(
        &target.tenant,
        target.scope(),
        &target.service,
        &target.object_key,
    )
}

/// The state key and inline value of a sealable frame; `None` = not that kind
/// of frame.
fn sealable(m: &Message) -> Option<(Bytes, &pb::Value)> {
    match m {
        Message::SetState(s) => Some((s.key.clone(), s.value.as_ref()?)),
        Message::GetEagerState(g) => match &g.result {
            Some(pb::get_eager_state_command_message::Result::Value(v)) => Some((g.key.clone(), v)),
            _ => None,
        },
        _ => None,
    }
}

/// A frame sealed by the shell, for `engine.step_sealed_with`.
pub(super) struct Sealed {
    pub frame: cairn_protocol::Frame,
    /// `(sha, gen)` just PUT for the frame (durable before the commit, AP-1).
    pub fresh: Vec<([u8; 32], u64)>,
    /// Bytes in `fresh` merged from old segments — not counted as units.
    pub old_bytes: u64,
}

/// Seals frame `f` if it should — see the module header. `None` ⇒ the old path
/// (`Event::FromSdk`). `Some(sealed)` ⇒ `engine.step_sealed_with`.
///
/// `v6`: the deployment negotiated wire v6 — `SetState.value` may carry
/// **reused** refs; the shell keeps them, seals the tail if it is large
/// enough, and the frame always goes `Event::Sealed` (the engine checks the
/// refs under the engine).
pub(super) async fn seal_frame(
    engine: &mut Lease,
    inv: &Invocation,
    f: &cairn_protocol::Frame,
    v6: bool,
) -> Result<Option<Sealed>> {
    use cairn_protocol::MessageType as T;
    if !engine.values_on() {
        return Ok(None);
    }
    if f.ty == T::OUTPUT || f.ty == T::PROPOSE_RUN_COMPLETION {
        return seal_output(engine, inv, f, v6).await;
    }
    if !(f.ty == T::SET_STATE || f.ty == T::GET_EAGER_STATE) {
        return Ok(None);
    }
    if f.ty == T::SET_STATE && inv.state_writes_forbidden() {
        return Ok(None);
    }
    let (Some(store), Some((pk, owner))) = (engine.value_store().cloned(), owner_of(&inv.target))
    else {
        return Ok(None);
    };
    let Ok(mut m) = cairn_protocol::decode(f) else {
        return Ok(None);
    };
    let Some((key, v)) = sealable(&m) else {
        return Ok(None);
    };
    // Below v6, only inline values from the SDK: `segments` are left for the
    // machine to answer 571. From v6, the SDK's reused refs — only in `SetState`.
    let reuse_ok = v6 && f.ty == T::SET_STATE;
    let limit = engine.tenant_limit(&inv.target.tenant);
    let threshold = seal_bytes(limit.max_payload);
    // The value's whole bytes when the SDK sends inline (≤ v5): merging takes
    // its bytes from here, no GET.
    let mut whole: Option<Bytes> = None;
    let (mut refs, rest, total) = match Logical::from_sdk(Some(v), reuse_ok) {
        Ok(Logical::Inline(full)) => {
            if full.len() < threshold {
                return Ok(None);
            }
            // Over `max-value-bytes=`: do not PUT what the engine will refuse —
            // the frame takes the old path, the engine ends the invocation.
            if over_value_limit(full.len() as u64, limit.max_value_bytes) {
                return Ok(None);
            }
            whole = Some(full.clone());
            // Prefix: the current value's segments matched chunk by chunk by sha.
            let old = engine
                .segmented_refs_of(&inv.target, &key)
                .await
                .map_err(ActorError::from)?;
            let mut refs: Vec<SegmentRef> = Vec::new();
            let mut off = 0usize;
            for r in &old {
                let end = off.saturating_add(r.len as usize);
                if end > full.len() || cairn_store::artifact::sha256(&full[off..end]) != r.sha {
                    break;
                }
                refs.push(SegmentRef {
                    sha256: r.sha,
                    len: r.len,
                    gen: r.gen,
                });
                off = end;
            }
            let total = full.len() as u64;
            (refs, full.slice(off..), total)
        }
        // A v6 SDK's refs: kept as is, the engine checks that they are in the
        // key's current row (in the same commit step). A small tail means
        // nothing is sealed — the frame still goes `Sealed` because it carries refs.
        Ok(Logical::Segmented { refs, tail, len }) => {
            // Over `max-value-bytes=`: the frame goes `Sealed` as is (it carries
            // refs — `FromSdk` would be 571), the engine ends the invocation.
            if over_value_limit(len, limit.max_value_bytes) {
                return Ok(Some(Sealed {
                    frame: f.clone(),
                    fresh: Vec::new(),
                    old_bytes: 0,
                }));
            }
            // The SDK's refs are only read (merged) when the engine will accept
            // them: the same check as `WriteState` (the key's current row, holder
            // `Invocation(id)`), **before** any GET or PUT (from a security
            // review). Foreign refs — of another owner or tenant on the same
            // range, or made up — ⇒ the frame goes `Sealed` as is, the engine
            // refuses them as dangling refs; the shell reads no byte and PUTs
            // nothing for that turn.
            if !engine
                .refs_reusable(inv, &key, &refs)
                .await
                .map_err(ActorError::from)?
            {
                tracing::debug!(
                    "value: SetState carries refs outside the key — left to the engine"
                );
                return Ok(Some(Sealed {
                    frame: f.clone(),
                    fresh: Vec::new(),
                    old_bytes: 0,
                }));
            }
            (refs, tail, len)
        }
        Err(_) => return Ok(None),
    };
    let mut fresh = Vec::new();
    let mut old_bytes = 0u64;
    let tail = if rest.len() >= threshold {
        let (sealed_part, kept) = rest.split_at(rest.len() - TAIL_RESERVE.min(rest.len()));
        let kept = rest.slice_ref(kept);
        let mut pending = rest.slice_ref(sealed_part);
        // Merging only for state: every state sealing adds one ref. Merging goes
        // **into** the part about to be PUT, so the turn still makes only one PUT.
        if f.ty == T::SET_STATE {
            let (p, old) = merge_into(
                engine,
                &store,
                inv,
                (pk, owner),
                &mut refs,
                pending,
                whole.as_ref(),
                threshold as u64,
            )
            .await;
            pending = p;
            old_bytes = old;
        }
        let r = put_sealed(engine, &store, inv, (pk, owner), pending).await?;
        refs.push(r);
        fresh.push((r.sha256, r.gen));
        kept
    } else {
        rest
    };
    if refs.is_empty() {
        return Ok(None);
    }
    let sealed = Logical::Segmented {
        refs,
        tail,
        len: total,
    }
    .encode();
    match &mut m {
        Message::SetState(s) => s.value = Some(sealed),
        Message::GetEagerState(g) => {
            g.result = Some(pb::get_eager_state_command_message::Result::Value(sealed))
        }
        _ => unreachable!("sealable only takes SetState and GetEagerState"),
    }
    let wire = m.encode(f.flags);
    let frame = cairn_protocol::Frame::new(f.ty, f.flags, wire.slice(cairn_protocol::HEADER_LEN..));
    Ok(Some(Sealed {
        frame,
        fresh,
        old_bytes,
    }))
}

/// `len` exceeds the tenant's `max-value-bytes=`.
fn over_value_limit(len: u64, max: Option<u64>) -> bool {
    max.is_some_and(|m| len > m)
}

/// Merges the tail of `refs` into `pending` (the bytes about to be PUT) per
/// [`merge_window`] — returns the new `pending` and the number of old bytes
/// merged into it. The old refs' bytes come from `whole` (the inline value of
/// an SDK ≤ v5) or a GET through the store (the range's cache), checking length
/// and sha. A failed GET ⇒ stop merging: merging is only an optimisation, the
/// value is still correct.
///
/// `refs` only holds refs confirmed to belong to the key: a prefix matching the
/// current row (SDK ≤ v5) or a v6 SDK's refs that passed
/// [`cairn_engine::Engine::refs_reusable`] in [`seal_frame`]. Never called with
/// unchecked SDK refs.
#[allow(clippy::too_many_arguments)]
async fn merge_into(
    engine: &mut Lease,
    store: &ValueStore,
    inv: &Invocation,
    (pk, owner): (cairn_store::PartitionKey, cairn_store::value::Owner),
    refs: &mut Vec<SegmentRef>,
    mut pending: Bytes,
    whole: Option<&Bytes>,
    seal: u64,
) -> (Bytes, u64) {
    let mut old = 0u64;
    loop {
        let mut lens: Vec<u64> = refs.iter().map(|r| r.len).collect();
        lens.push(pending.len() as u64);
        let Some(start) = merge_window(&lens, seal) else {
            break;
        };
        let mut buf = bytes::BytesMut::with_capacity(lens[start..].iter().sum::<u64>() as usize);
        let mut off: usize = refs[..start].iter().map(|r| r.len as usize).sum();
        for r in &refs[start..] {
            let end = off + r.len as usize;
            let got = match whole {
                Some(w) if end <= w.len() => Some(w.slice(off..end)),
                _ => {
                    let seg = Segment {
                        tenant: inv.target.tenant.as_str().to_string(),
                        pk,
                        owner,
                        sha: r.sha256,
                        gen: r.gen,
                    };
                    let st: &ValueStore = store;
                    engine
                        .released(async |_: &Released| st.get(&seg).await)
                        .await
                        .ok()
                        .flatten()
                        .filter(|b| {
                            b.len() as u64 == r.len && cairn_store::artifact::sha256(b) == r.sha256
                        })
                }
            };
            let Some(b) = got else {
                tracing::debug!("value: segment for a merge not readable — merge skipped");
                return (pending, old);
            };
            buf.extend_from_slice(&b);
            off = end;
        }
        old += refs[start..].iter().map(|r| r.len).sum::<u64>();
        buf.extend_from_slice(&pending);
        refs.truncate(start);
        pending = buf.freeze();
        crate::metrics::agent::bump(&crate::metrics::agent::VALUE_MERGES);
    }
    (pending, old)
}

/// PUTs `bytes` as a new segment under `(pk, owner)` of `inv`'s tenant, outside
/// the engine (D16), **before** the commit (AP-1). Past `T_put` between PUT and
/// commit ⇒ abandon that object (an orphan) and PUT again under a new `gen`, at
/// most [`PUT_ROUNDS`] times.
async fn put_sealed(
    engine: &mut Lease,
    store: &ValueStore,
    inv: &Invocation,
    (pk, owner): (cairn_store::PartitionKey, cairn_store::value::Owner),
    bytes: Bytes,
) -> Result<SegmentRef> {
    let sha = cairn_store::artifact::sha256(&bytes);
    let deadline = engine.artifact_put_deadline();
    for round in 0..PUT_ROUNDS {
        let seg = Segment {
            tenant: inv.target.tenant.as_str().to_string(),
            pk,
            owner,
            sha,
            gen: fresh_gen(),
        };
        let started = engine.clock().mono();
        let b = bytes.clone();
        engine
            .released(async |_: &Released| store.put(&seg, b).await)
            .await
            .map_err(store_err)?;
        // The "segment durable, holder row not yet committed" gap (AP-1): a crash
        // here leaves an orphan — the values sweep holds and then deletes it.
        cairn_store::failpoint!("value::after_put", err);
        if engine.clock().mono().saturating_duration_since(started) <= deadline {
            crate::metrics::agent::bump(&crate::metrics::agent::VALUE_SEALED);
            crate::metrics::agent::add(
                &crate::metrics::agent::VALUE_SEALED_BYTES,
                bytes.len() as u64,
            );
            return Ok(SegmentRef {
                sha256: seg.sha,
                len: bytes.len() as u64,
                gen: seg.gen,
            });
        }
        // Past `T_put`: the object just PUT may be older than `min_age` before the
        // commit — abandon it (an orphan) and PUT again under a new `gen`.
        tracing::debug!(
            round,
            "value: T_put exceeded between PUT and commit — new gen"
        );
    }
    Err(store_err(cairn_store::StoreError::Io(format!(
        "value: all {PUT_ROUNDS} PUTs exceeded T_put {deadline:?} — next attempt"
    ))))
}

/// `(pk, owner)` of the values in `inv`'s journal and output: a keyed object ⇒
/// the object's owner, otherwise the id itself — the same rule as the holder
/// `Invocation(id)` in the engine.
fn value_owner(inv: &Invocation) -> (cairn_store::PartitionKey, cairn_store::value::Owner) {
    cairn_engine::output_owner(&inv.target, &inv.id)
}

/// Seals a large `Output.value` and `ctx.run` result (`ProposeRunCompletion`).
/// Written once, so **no** prefix comparison: an inline part ≥ `seal_bytes`
/// becomes one whole segment (no tail kept). A v6 SDK may send reused refs —
/// the engine checks that they are in the invocation's own `Invocation(id)`
/// holder. A `Failure` is never sealed.
async fn seal_output(
    engine: &mut Lease,
    inv: &Invocation,
    f: &cairn_protocol::Frame,
    v6: bool,
) -> Result<Option<Sealed>> {
    use pb::{output_command_message::Result as O, propose_run_completion_message::Result as P};
    let Some(store) = engine.value_store().cloned() else {
        return Ok(None);
    };
    let Ok(mut m) = cairn_protocol::decode(f) else {
        return Ok(None);
    };
    let logical = match &m {
        Message::Output(o) => match &o.result {
            Some(O::Value(v)) => Logical::from_sdk(Some(v), v6),
            _ => return Ok(None),
        },
        Message::ProposeRunCompletion(p) => match &p.result {
            Some(P::Value(b)) => Ok(Logical::Inline(b.clone())),
            Some(P::SegmentedValue(v)) => Logical::from_sdk(Some(v), v6),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    // A broken layout or refs from an SDK below v6: leave it for the machine to answer 571.
    let Ok(logical) = logical else {
        return Ok(None);
    };
    let limit = engine.tenant_limit(&inv.target.tenant);
    let threshold = seal_bytes(limit.max_payload);
    // Over `max-value-bytes=`: no PUT; an inline value takes the old path, one
    // with refs goes `Sealed` as is — the engine ends the invocation.
    if over_value_limit(logical.len(), limit.max_value_bytes) {
        return Ok(match logical {
            Logical::Inline(_) => None,
            Logical::Segmented { .. } => Some(Sealed {
                frame: f.clone(),
                fresh: Vec::new(),
                old_bytes: 0,
            }),
        });
    }
    let (mut refs, rest, total) = match logical {
        Logical::Inline(full) if full.len() < threshold => return Ok(None),
        Logical::Inline(full) => (Vec::new(), full.clone(), full.len() as u64),
        Logical::Segmented { refs, tail, len } => (refs, tail, len),
    };
    let mut fresh = Vec::new();
    let tail = if rest.len() >= threshold {
        let r = put_sealed(engine, &store, inv, value_owner(inv), rest).await?;
        refs.push(r);
        fresh.push((r.sha256, r.gen));
        Bytes::new()
    } else {
        rest
    };
    if refs.is_empty() {
        return Ok(None);
    }
    let sealed = Logical::Segmented {
        refs,
        tail,
        len: total,
    }
    .encode();
    match &mut m {
        Message::Output(o) => o.result = Some(O::Value(sealed)),
        Message::ProposeRunCompletion(p) => p.result = Some(P::SegmentedValue(sealed)),
        _ => unreachable!("seal_output only takes Output and ProposeRunCompletion"),
    }
    let wire = m.encode(f.flags);
    let frame = cairn_protocol::Frame::new(f.ty, f.flags, wire.slice(cairn_protocol::HEADER_LEN..));
    Ok(Some(Sealed {
        frame,
        fresh,
        old_bytes: 0,
    }))
}

/// Cap on inline assembly when delivering a segmented output to a caller whose
/// tenant has no `max-payload=` (`len ≤ max-payload` ⇒ inline): keeps the
/// caller's completion under a reasonable frame size, COPY above it.
pub(super) const INLINE_DELIVERY_CAP: u64 = 4 << 20;

/// The result of segmented output `o` delivered to `caller` on this range, **outside** the
/// engine (D16):
///
/// - `len ≤ max-payload` of the receiving tenant (no cap ⇒
///   [`INLINE_DELIVERY_CAP`]) ⇒ GET, check sha, assemble ⇒ `CallValue` — the
///   bytes on the wire and in the caller's journal are exactly as with an
///   inline value;
/// - larger ⇒ COPY each segment under the caller's owner with a new `gen`, PUT
///   **before** the commit ⇒ `CallSegmented` with the new refs; the engine
///   receives the list through `stage_copied` and refuses refs outside it.
///   Call `deliver` **right after** this function.
///
/// The caller's target cannot be read ⇒ an error (the source row stays, retried later).
pub(super) async fn deliver_output(
    engine: &mut Lease,
    caller: &InvocationId,
    o: cairn_engine::SegmentedOutput,
) -> Result<CompletionResult> {
    let Some(store) = engine.value_store().cloned() else {
        return Err(store_err(cairn_store::StoreError::Io(
            "value: segmented output but no value store".into(),
        )));
    };
    let target = engine
        .target_of_id(caller)
        .await
        .map_err(ActorError::from)?
        .ok_or_else(|| {
            store_err(cairn_store::StoreError::Io(
                "value: caller of a segmented output has no target".into(),
            ))
        })?;
    let max = engine
        .tenant_limit(&target.tenant)
        .max_payload
        .unwrap_or(INLINE_DELIVERY_CAP);
    if o.len() <= max {
        let st: &ValueStore = &store;
        let full = engine
            .released(async |_: &Released| crate::value_output::assemble(st, &o, max).await)
            .await
            .map_err(|e| store_err(e.into()))?;
        return Ok(CompletionResult::CallValue(full));
    }
    let (pk, owner) = cairn_engine::output_owner(&target, caller);
    let st: &ValueStore = &store;
    let tenant = target.tenant.clone();
    let (value, fresh) = engine
        .released(async |_: &Released| {
            crate::value_output::copy(st, &o, (&tenant, pk, owner)).await
        })
        .await
        .map_err(|e| store_err(e.into()))?;
    engine.stage_copied(fresh);
    Ok(CompletionResult::CallSegmented(value))
}

/// The SDK-bound direction of [`prepare_down`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Down {
    /// The turn's opening batch: `msgs[0]` is `Start`, the URLs go into `Start.segment_urls`.
    Opening,
    /// A mid-turn batch (BIDI): the URLs travel in a `SegmentUrls` frame at the head of the batch.
    Mid,
}

/// Prepares `msgs` for the SDK: when the deployment negotiated v6 and the node
/// has a read path that can send refs ⇒ attach the read URL of every ref;
/// otherwise ⇒ [`assemble`].
pub(super) async fn prepare_down(
    engine: &mut Lease,
    urls: &crate::value_urls::ValueUrls,
    wire_version: u32,
    inv: &Invocation,
    msgs: &mut Vec<Message>,
    down: Down,
) -> Result<()> {
    if wire_version < crate::discovery::WIRE_SEGMENTED || !urls.sends_refs() {
        return assemble(engine, inv, msgs).await;
    }
    if !msgs
        .iter()
        .any(|m| value::values(m).into_iter().any(|v| !v.segments.is_empty()))
    {
        return Ok(());
    }
    // The owner is the invocation's own (its target, or the id when unkeyed) —
    // there is no path that signs a key chosen by the SDK.
    let (pk, owner) = value_owner(inv);
    let tenant = inv.target.tenant.as_str().to_string();
    let now = engine.clock().wall_ms();
    let batch: &[Message] = msgs;
    // Signing may need credentials from the network (IMDS) ⇒ outside the engine (D16).
    let signed = engine
        .released(async |_: &Released| urls.for_messages(&tenant, pk, owner, batch, now).await)
        .await
        .map_err(|e| store_err(cairn_store::StoreError::Io(e)))?;
    match down {
        Down::Opening => match msgs.first_mut() {
            Some(Message::Start(s)) => s.segment_urls = signed,
            _ => unreachable!("an opening batch starts with Start"),
        },
        Down::Mid => msgs.insert(
            0,
            Message::SegmentUrls(pb::SegmentUrlsMessage { urls: signed }),
        ),
    }
    Ok(())
}

fn fresh_gen() -> u64 {
    let mut b = [0u8; 8];
    cairn_store::entropy::fill(&mut b);
    u64::from_be_bytes(b)
}

/// Assembles every segmented `Value` of `msgs` inline (SDK ≤ v5) — see the
/// module header. Segments are fetched outside the engine (D16); a missing
/// segment or a wrong sha/length ⇒ a turn error (retried), nothing wrong is
/// sent down to the SDK.
pub(super) async fn assemble(
    engine: &mut Lease,
    inv: &Invocation,
    msgs: &mut [Message],
) -> Result<()> {
    let segmented = msgs
        .iter()
        .any(|m| value::values(m).into_iter().any(|v| !v.segments.is_empty()));
    if !segmented {
        return Ok(());
    }
    let Some(store) = engine.value_store().cloned() else {
        return Err(store_err(cairn_store::StoreError::Io(
            "value: segmented value but no value store".into(),
        )));
    };
    // Every segmented value in `inv`'s journal (state, `Output`, `ctx.run`
    // results, COPYs of completions) belongs to its output owner.
    let (pk, owner) = value_owner(inv);
    let tenant = inv.target.tenant.as_str().to_string();
    for m in msgs.iter_mut() {
        for v in value::values_mut(m) {
            let l = Logical::of(Some(v)).map_err(|e| {
                store_err(cairn_store::StoreError::CorruptSegmented { key: e.to_string() })
            })?;
            let Logical::Segmented { refs, tail, len } = l else {
                continue;
            };
            let mut full = bytes::BytesMut::with_capacity(len as usize);
            for r in &refs {
                let seg = Segment {
                    tenant: tenant.clone(),
                    pk,
                    owner,
                    sha: r.sha256,
                    gen: r.gen,
                };
                let st: &ValueStore = &store;
                let got = engine
                    .released(async |_: &Released| st.get(&seg).await)
                    .await
                    .map_err(store_err)?;
                let Some(b) = got.filter(|b| {
                    b.len() as u64 == r.len && cairn_store::artifact::sha256(b) == r.sha256
                }) else {
                    return Err(store_err(cairn_store::StoreError::Io(format!(
                        "value: segment {} missing or corrupt",
                        st.path(&seg)
                    ))));
                };
                full.extend_from_slice(&b);
            }
            full.extend_from_slice(&tail);
            *v = value::inline(full.freeze());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 256 << 10;

    /// Row-by-row table of the merge rule.
    #[test]
    fn merge_window_rows() {
        let m = MERGE_MAX;
        let rows: &[(&[u64], Option<usize>, &str)] = &[
            (&[S, S, S], None, "fewer than W elements"),
            (&[S, S, S, S], Some(0), "W elements of the same tier 0"),
            (
                &[4 * S, S, S, S],
                None,
                "a tier-1 element before three tier-0 elements",
            ),
            (&[4 * S, S, S, S, S], Some(1), "last four share a tier"),
            (
                &[S, S, S, 4 * S],
                Some(0),
                "a large last element pulls in the smaller ones before it",
            ),
            (&[16 * S, 4 * S, 4 * S, 4 * S, 4 * S], Some(1), "4 × tier 1"),
            (
                &[S, 3 * S, 2 * S, 3 * S],
                Some(0),
                "different sizes within tier 0",
            ),
            (&[m, m, m, m], None, "total over MERGE_MAX"),
            (
                &[m / 4, m / 4, m / 4, m / 4],
                Some(0),
                "total exactly MERGE_MAX",
            ),
            (&[1, 1, 1, 1], Some(0), "below seal counts as tier 0"),
        ];
        for (lens, want, why) in rows {
            assert_eq!(merge_window(lens, S), *want, "{why}: {lens:?}");
        }
        assert_eq!(tier(S, S), 0);
        assert_eq!(tier(4 * S - 1, S), 0);
        assert_eq!(tier(4 * S, S), 1);
        assert_eq!(tier(16 * S, S), 2);
    }

    /// Simulates `seal_frame` with the merge rule: returns `(ref count, bytes PUT)`.
    fn simulate(seals: &[u64]) -> (Vec<u64>, u64) {
        let mut refs: Vec<u64> = Vec::new();
        let mut put = 0u64;
        for &n in seals {
            let mut pending = n;
            loop {
                let mut lens = refs.clone();
                lens.push(pending);
                let Some(start) = merge_window(&lens, S) else {
                    break;
                };
                pending += refs[start..].iter().sum::<u64>();
                refs.truncate(start);
            }
            put += pending;
            refs.push(pending);
        }
        (refs, put)
    }

    /// Evenly sized seals = a base-4 counter: the ref count is the digit sum,
    /// and each byte is copied again ≤ `log_4(MERGE_MAX / seal)` times.
    #[test]
    fn equal_seals_count_in_base_four() {
        for n in 1..=256u64 {
            let (refs, put) = simulate(&vec![S; n as usize]);
            let mut digits = 0;
            let mut x = n;
            while x > 0 {
                digits += x % 4;
                x /= 4;
            }
            // From 64 seals (16 MiB = MERGE_MAX) tier 3 merges no further.
            let top = n / 64;
            let low = n % 64;
            let mut low_digits = 0;
            let mut x = low;
            while x > 0 {
                low_digits += x % 4;
                x /= 4;
            }
            let want = if n < 64 { digits } else { top + low_digits };
            assert_eq!(refs.len() as u64, want, "{n} seal: {refs:?}");
            assert_eq!(refs.iter().sum::<u64>(), n * S);
            assert!(put <= 4 * n * S, "{n} seals: {put} bytes copied again");
        }
        // 8 MiB ≈ 32 seal ⇒ ≤ 7 ref; 64 MiB = 256 seal ⇒ ≤ 13.
        assert!((1..=33).all(|n| simulate(&vec![S; n]).0.len() <= 7));
        assert!((1..=256).all(|n| simulate(&vec![S; n]).0.len() <= 13));
    }

    /// Unevenly sized seals (an SDK ≤ v5 appending many bytes per turn): bounds
    /// checked by a randomized test — values up to 64 MiB keep ≤ 16 refs, each
    /// byte is copied again ≤ 4 times on average, and the total size is unchanged.
    #[test]
    fn random_seals_stay_bounded() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..200 {
            let mut seals = Vec::new();
            let mut total = 0u64;
            while total < 64 << 20 {
                // Usually a 256 KiB–1 MiB chunk; now and then a chunk up to 4 MiB.
                let n = if next() % 10 == 0 {
                    S + next() % (4 << 20)
                } else {
                    S + next() % (3 * S)
                };
                total += n;
                seals.push(n);
            }
            let (refs, put) = simulate(&seals);
            assert_eq!(refs.iter().sum::<u64>(), total);
            assert!(refs.len() <= 16, "{} ref: {refs:?}", refs.len());
            assert!(put <= 4 * total, "copied again {put} / {total}");
        }
    }

    #[test]
    fn seal_bytes_follows_max_payload_with_a_floor() {
        assert_eq!(seal_bytes(None), SEAL_BYTES);
        assert_eq!(seal_bytes(Some(1 << 20)), 256 << 10);
        assert_eq!(seal_bytes(Some(512 << 10)), 128 << 10);
        // The lower clamp wins.
        assert_eq!(seal_bytes(Some(1024)), MIN_SEAL_BYTES);
    }
}
