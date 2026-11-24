// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! One step of the invocation state machine: `load`, `step`, `read_promise`, and the
//! completion builders `step` uses.

use {
    super::*,
    cairn_lifecycle::codec::{Payload, RetryRow, RowKind, Width},
};

impl<S: RangeStore> Engine<S> {
    /// Reloads an invocation from its durable journal — the recovery path after
    /// a process death or a leader change.
    /// The routing target is taken back from entry 0, not from a parameter —
    /// reloading must be self-sufficient, because the caller (the timer loop)
    /// has only the id.
    pub async fn load(&mut self, id: InvocationId, params: StartParams) -> Result<Invocation> {
        // A turn (or any other path) loading cold ⇒ the id's warm machine and
        // wait tree no longer belong to the delivery path: the turn will
        // suspend again with a new tree, or finish.
        self.warm.forget(&id);
        self.load_journal(id, params).await
    }

    /// [`Self::load`] without evicting anything — the warm machine's cold path (`warm.rs`).
    pub(super) async fn load_journal(
        &mut self,
        id: InvocationId,
        params: StartParams,
    ) -> Result<Invocation> {
        let rows = self.writer.journal_unbounded(&id).await?;
        let mut entries = Vec::with_capacity(rows.len());
        let journal_bytes = rows.iter().map(|(_, b)| b.len() as u64).sum();
        for (expected, (index, bytes)) in rows.into_iter().enumerate() {
            // The journal is append-only with contiguous indexes. A hole means
            // broken data, not "temporarily missing" — replaying on would
            // number entries wrongly and every later command index would point
            // at the wrong place.
            if index as usize != expected {
                return Err(EngineError::JournalGap(expected as u32));
            }
            entries.push(JournalEntry::decode(bytes)?);
        }
        let first = entries.first().ok_or(EngineError::NoJournal)?;
        let target = target_of(first).ok_or(EngineError::MissingTarget)?;
        let reply_to = reply_to_of(first);
        let kind = kind_of(first);
        let calls = call_rules_of(first);
        let state_lock = state_lock_of(first);
        let deployment = self.pinned_of(&id, first).await?;
        let params = self
            .with_eager_state(with_key(params, &target, kind), &target)
            .await?;
        let chain = self
            .writer
            .get(&cairn_store::keys::chain(&id))
            .await?
            .and_then(|raw| cairn_store::keys::decode_chain(&raw));
        Ok(Invocation {
            id,
            target,
            reply_to,
            kind,
            deployment,
            machine: InvocationMachine::resume(id, params, Journal::from_entries(entries)),
            outcome: None,
            chain,
            retention_ms: None,
            calls,
            state_lock,
            journal_bytes,
            inline_values: false,
        })
    }

    /// Snapshots the object's state into `StartParams` — eager state.
    ///
    /// Capped: an object whose state is larger than the cap is sent PARTIALLY
    /// (`partial_state = true`), the SDK reads the rest lazily. Without a cap,
    /// an object that accumulates a lot of state would turn every one of its
    /// turns into a load of the whole table — exactly the hot path that must
    /// stay cheap.
    pub(super) async fn with_eager_state(
        &self,
        params: StartParams,
        target: &Target,
    ) -> Result<StartParams> {
        const MAX_ENTRIES: usize = 64;
        const MAX_BYTES: usize = 256 * 1024;

        // An unkeyed, unscoped object (D-a): there is no state to snapshot —
        // its first state command will fail the invocation for good.
        let Some(owner) = target.object_owner() else {
            return Ok(params);
        };
        // The scan stops at the cap: it reads at most `MAX_ENTRIES + 1` rows,
        // rather than reading everything and then cutting.
        let (state_map, capped) = self
            .writer
            .state_entries_capped(
                owner,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                (MAX_ENTRIES, MAX_BYTES),
            )
            .await?;
        // Segmented keys do not go into `state_map`: the SDK must read them
        // lazily.
        let partial_state = capped || self.has_segmented_keys(target).await?;
        Ok(StartParams {
            state_map,
            partial_state,
            ..params
        })
    }

    /// Pushes an event into an invocation; returns the messages to send down to the SDK.
    ///
    /// **Why everything goes into one commit.** The state machine does *not*
    /// replay `WriteState` on replay — it only replays tasks for pending
    /// completions. So if `SetState` became durable in a different commit than
    /// its journal entry, a death exactly between the two commits would lose
    /// that mutation for good: the journal says "set", the store says "not
    /// yet". In the same commit it is either both or neither. That applies to
    /// all pending work: timers, outbox, invocation status.
    /// Whether SDK frame `frame` exceeds the `max-payload=` of `inv`'s tenant —
    /// for the shell before it handles a frame itself without going through
    /// [`Self::step`] (`ContinueAsNew`): when it does, the shell feeds the frame
    /// into `step`, where the invocation ends with `payload_too_large`.
    pub fn frame_oversized(&mut self, inv: &Invocation, frame: &cairn_protocol::Frame) -> bool {
        self.refresh_tenant_limits();
        let max = self.tenant_limit(&inv.target.tenant).max_payload;
        max.and_then(|m| oversized_frame(frame, m)).is_some()
    }

    pub async fn step(&mut self, inv: &mut Invocation, event: Event) -> Result<Vec<Message>> {
        let mut queue = VecDeque::from([event]);
        let mut out = Vec::new();

        // The owner of the state/promises of the object this invocation touches.
        // `None` (D-a) ⇒ every task touching the object is blocked right below.
        let object = inv.target.object_owner();
        // The tenant's limits from the range's snapshot: once per `step`, no
        // shared lock while the source is unchanged.
        self.refresh_tenant_limits();
        let limit = self.tenant_limit(&inv.target.tenant);
        while let Some(ev) = queue.pop_front() {
            // `max-payload=`: a frame written to the journal that is larger than
            // the limit does not reach the machine — the invocation ends with a
            // stable code in this very commit.
            // `max-artifact-bytes=`: the shell does not PUT artifacts of a
            // tenant that has reached its limit but lets the frame come here —
            // it ends with a stable code instead of the machine's 571 "unknown
            // code".
            // A completion/signal **from outside**: its source row (timer,
            // outbox, inbound, signal outbox) is deleted in this very commit,
            // and an external signal has been acked once the commit is durable
            // — its entry must not be dropped.
            let external = matches!(&ev, Event::Completion(_) | Event::Signal(_));
            let artifacts_full = matches!(&ev, Event::FromSdk(f) | Event::Sealed(f)
                if f.ty == cairn_protocol::MessageType::ARTIFACT_PUT)
                && self
                    .tenant_limits
                    .artifacts_full(&inv.target.tenant)
                    .is_some();
            // `max-value-bytes=`: the logical limit of a value, more specific
            // than `max-payload=`, so it is checked first.
            let over_value = value_over(&ev, limit.max_value_bytes);
            let mut effects = match oversized(&ev, limit.max_payload) {
                _ if over_value.is_some() => {
                    let (len, max) = over_value.expect("checked is_some");
                    vec![inv.machine.fail(
                        crate::TENANT_LIMIT,
                        &format!(
                            "{}: value of {len} bytes exceeds the tenant limit of {max} bytes",
                            crate::VALUE_BYTES_LIMIT_CODE
                        ),
                    )]
                }
                Some((len, max)) => vec![inv.machine.fail(
                    crate::PAYLOAD_TOO_LARGE,
                    &format!(
                        "{}: frame of {len} bytes exceeds the tenant limit of {max} bytes — \
                         large values (files, images, long histories) go through ctx.artifact, \
                         the journal keeps only a reference",
                        crate::PAYLOAD_TOO_LARGE_CODE
                    ),
                )],
                None if artifacts_full => vec![inv.machine.fail(
                    crate::TENANT_LIMIT,
                    &format!(
                        "{}: tenant artifacts reached the limit of {} bytes",
                        crate::ARTIFACTS_LIMIT_CODE,
                        limit.max_artifact_bytes.unwrap_or_default()
                    ),
                )],
                None => {
                    // The refs of a sealed `Output`/`ProposeRunCompletion` frame
                    // and of a completion the shell just COPYed: checked under
                    // the engine, in the same commit step — a made-up ref never
                    // reaches the machine.
                    match &ev {
                        Event::Sealed(f) => self.check_sealed_output(inv, f).await?,
                        Event::Completion(c) => self.check_copied(c)?,
                        _ => {}
                    }
                    inv.machine.handle(ev)?
                }
            };
            // `max-timers=`: the invocation's pending timers — the machine has
            // already recorded the pending completion of the command that just
            // arrived. The same pattern as `max-journal=` right below: keep the
            // entry, end with a stable code.
            if let Some(max) = limit.max_timers {
                let timers = inv
                    .machine
                    .pending_of(cairn_protocol::MessageType::SLEEP_COMPLETION);
                if timers > max as usize
                    && effects
                        .iter()
                        .any(|fx| matches!(fx, Effect::Task(Task::ScheduleTimer { .. })))
                {
                    effects.retain(|fx| matches!(fx, Effect::Persist { .. }));
                    effects.push(inv.machine.fail(
                        crate::TENANT_LIMIT,
                        &format!(
                            "{}: {timers} pending timers exceed the tenant limit of {max}",
                            crate::TIMERS_LIMIT_CODE
                        ),
                    ));
                }
            }
            // `max-state-keys=`: a **new** state key of an object over the
            // limit ⇒ end before writing. Only tenants with a limit pay for the
            // reads (whether the key exists, counting keys up to the limit + 1).
            if let (Some(max), Some(owner)) = (limit.max_state_keys, object) {
                if let Some(n) = self.state_keys_over(inv, owner, &effects, max).await? {
                    effects.retain(|fx| matches!(fx, Effect::Persist { .. }));
                    effects.push(inv.machine.fail(
                        crate::TENANT_LIMIT,
                        &format!(
                            "{}: object has {n} state keys, exceeding the tenant limit of {max}",
                            crate::STATE_KEYS_LIMIT_CODE
                        ),
                    ));
                }
            }
            // The SDK reads values inline (wire before v6): the shell assembles
            // every segmented value going down — up to the tenant's payload
            // limit. Over the limit ⇒ end with the payload limit's stable code,
            // the same pattern as `max-timers=` above: keep the entry that
            // happened (the completion just written), drop what is sent out.
            // Otherwise the SDK would get a frame larger than the limit that it
            // can never write back (the turn hangs or retries forever).
            if let (true, Some(max)) = (inv.inline_values, limit.max_payload) {
                let over = effects.iter().find_map(|fx| match fx {
                    Effect::Send(m) => cairn_protocol::value::values(m)
                        .into_iter()
                        .find(|v| !v.segments.is_empty() && v.len > max)
                        .map(|v| v.len),
                    _ => None,
                });
                if let Some(len) = over {
                    effects.retain(|fx| matches!(fx, Effect::Persist { .. }));
                    effects.push(inv.machine.fail(
                        crate::PAYLOAD_TOO_LARGE,
                        &format!(
                            "{}: value of {len} bytes exceeds the tenant limit of {max} bytes \
                             for an SDK that receives values inline (wire before v6) — upgrade \
                             the SDK to read segmented values",
                            crate::PAYLOAD_TOO_LARGE_CODE
                        ),
                    ));
                }
            }
            // `max-journal=`: an entry at index ≥ the limit is not written.
            // Keep the entries below the limit (they happened), drop everything
            // else of the step, end with a stable code — the same pattern as
            // D-a right below.
            if let Some(max) = limit.max_journal {
                let over = |fx: &Effect| matches!(fx, Effect::Persist { entry_index, .. } if *entry_index >= max);
                if effects.iter().any(over) {
                    effects.retain(|fx| matches!(fx, Effect::Persist { .. }) && !over(fx));
                    effects.push(inv.machine.fail(
                        crate::JOURNAL_TOO_LONG,
                        &format!(
                            "{}: journal reached the tenant limit of {max} entries",
                            crate::JOURNAL_TOO_LONG_CODE
                        ),
                    ));
                }
            }
            // `max-journal-bytes=`: an entry that would push the journal's total
            // bytes over the limit is not written — the durable journal never
            // exceeds the limit, so a later cold-loading turn reads at most that
            // much into RAM. Keep the entries before it, drop everything else of
            // the step, and the invocation is **`paused`** (not `failed`):
            // raising the limit and then `resume` continues it, the journal
            // loses nothing.
            //
            // Except entries **from outside** (`external`): always written,
            // even over the limit — dropping one would lose a completion whose
            // source row has been consumed or a signal already acked, and a
            // delivery step outside a turn has nobody to record a `Pause`
            // outcome. Each such entry is bounded by `max-payload=`; the next
            // entry **of the invocation itself** (the turn that runs after the
            // completion woke it) hits the limit and stops it there.
            if let Some(max) = limit.max_journal_bytes.filter(|_| !external) {
                let mut total = inv.journal_bytes;
                let cut = effects.iter().find_map(|fx| match fx {
                    Effect::Persist { entry_index, entry } => {
                        total += entry_len(entry);
                        (total > max).then_some(*entry_index)
                    }
                    _ => None,
                });
                if let Some(cut) = cut {
                    effects.retain(|fx| {
                        matches!(fx, Effect::Persist { entry_index, .. } if *entry_index < cut)
                    });
                    effects.push(inv.machine.pause(
                        crate::JOURNAL_BYTES_LIMIT,
                        &format!(
                            "{}: journal would exceed the tenant limit of {max} bytes; \
                             pass large values through ctx.artifact, raise the limit \
                             and resume to continue",
                            crate::JOURNAL_BYTES_LIMIT_CODE
                        ),
                    ));
                }
            }
            // **D-a.** An unkeyed, unscoped object touching state or a promise ⇒
            // end the invocation with a terminal error, in this very commit.
            // Keep the journal entry (it happened), drop everything else of
            // the step.
            if object.is_none() && effects.iter().any(touches_object) {
                effects.retain(|fx| matches!(fx, Effect::Persist { .. }));
                effects.push(
                    inv.machine
                        .fail(NO_OBJECT_KEY, &no_owner(&inv.target).to_string()),
                );
            }

            let mut batch = StepBatch::new();
            self.stage_pending_removals(&mut batch.muts);
            if std::mem::take(&mut self.audit_signal_armed) {
                self.take_audit_for_signal(&inv.id, &mut batch.muts);
            } else {
                self.take_audit_for(&inv.id, &mut batch.muts);
            }
            for fx in effects {
                self.apply_effect(inv, object, fx, &mut batch).await?;
            }
            if let Some(result) = batch.finished.take() {
                self.stage_attachment_replies(inv, result, &mut batch.muts)
                    .await?;
            }

            // ── Durable FIRST. This is the contract, not an ordering optimisation.
            if !batch.muts.is_empty() {
                self.writer.commit(batch.muts).await?;
            }
            // The suspension is committed: remember the wait tree for the drainer.
            if let Some(s) = batch.suspended.take() {
                self.warm.wait_on(inv.id, s);
            }
            // ── Only then send.
            out.extend(batch.sends);

            for task in batch.reads {
                if let Some(c) = self.answer_read(inv, object, task).await? {
                    queue.push_back(Event::Completion(c));
                }
            }
            for c in batch.ready {
                queue.push_back(Event::Completion(c));
            }
        }
        Ok(out)
    }

    /// Reads and decodes a durable promise of the object.
    pub(super) async fn read_promise(
        &self,
        owner_pk: PartitionKey,
        target: &Target,
        name: &str,
    ) -> Result<Option<PromiseRecord>> {
        match self
            .writer
            .promise(
                owner_pk,
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
                name,
            )
            .await?
        {
            Some(raw) => Ok(Some(PromiseRecord::decode(raw)?)),
            None => Ok(None),
        }
    }
}

impl<S: RangeStore> Engine<S> {
    /// The number of state keys of the object after the write commands in
    /// `effects`, if it exceeds `max`. Rewriting an existing key does not
    /// count. Earlier batches of the same `step` are committed (visible to
    /// reads), so only this batch's new keys add up. Counts at most `max + 1`.
    async fn state_keys_over(
        &self,
        inv: &Invocation,
        owner: PartitionKey,
        effects: &[Effect],
        max: u32,
    ) -> Result<Option<usize>> {
        let t = &inv.target;
        let mut fresh = Vec::new();
        for fx in effects {
            let Effect::Task(Task::WriteState {
                key,
                value: Some(_),
            }) = fx
            else {
                continue;
            };
            if fresh.contains(key) {
                continue;
            }
            let exists = self
                .writer
                .state(owner, &t.tenant, t.scope(), &t.service, &t.object_key, key)
                .await?
                .is_some()
                || self.is_segmented(t, key).await?;
            if !exists {
                fresh.push(key.clone());
            }
        }
        if fresh.is_empty() {
            return Ok(None);
        }
        let have = self
            .writer
            .state_key_count(
                owner,
                &t.tenant,
                t.scope(),
                &t.service,
                &t.object_key,
                max as usize + 1,
            )
            .await?
            + self.segmented_keys(t).await?.len();
        let n = have + fresh.len();
        Ok((n > max as usize).then_some(n))
    }
}

/// The `ReplyResult` of a **promise** → a completion result (value/failure). A
/// promise value is always inline; `Segmented` cannot get here — returns a
/// failure instead of panicking on the engine thread.
pub(super) fn reply_completion(r: ReplyResult) -> CompletionResult {
    match r {
        ReplyResult::Value(v) => CompletionResult::CallValue(v),
        ReplyResult::Segmented(_) => {
            // Tripwire: the engine meets a segmented value where it needs the
            // whole bytes. Tests enable `panic`.
            cairn_store::failpoint!("value::engine_needs_bytes");
            CompletionResult::CallFailure(pb::Failure {
                code: 500,
                message: "segmented promise value".into(),
                metadata: vec![],
            })
        }
        ReplyResult::Failure { code, message } => CompletionResult::CallFailure(pb::Failure {
            code,
            message,
            metadata: vec![],
        }),
    }
}

/// The result of an invocation that has finished for good, as a completion. A
/// segmented output is not delivered directly under the engine: the call site
/// queues a `Reply` outbox row for it before getting here; that branch
/// returns a failure instead of panicking.
pub(super) fn terminal_completion(s: InvocationStatus) -> CompletionResult {
    match s {
        InvocationStatus::Completed(crate::InvocationOutput::Inline(v)) => {
            CompletionResult::CallValue(v)
        }
        InvocationStatus::Completed(crate::InvocationOutput::Segmented(_)) => {
            // Tripwire: a segmented output here means the call site forgot to
            // queue a `Reply` row for the shell — the engine would have to assemble.
            cairn_store::failpoint!("value::engine_needs_bytes");
            CompletionResult::CallFailure(pb::Failure {
                code: 500,
                message: "segmented output must be delivered through the outbox".into(),
                metadata: vec![],
            })
        }
        InvocationStatus::Failed { code, message } => CompletionResult::CallFailure(pb::Failure {
            code,
            message,
            metadata: vec![],
        }),
        // The call site has filtered with `is_terminal()`; this branch is
        // unreachable but returning a failure is still better than panicking
        // on the engine thread.
        other => CompletionResult::CallFailure(pb::Failure {
            code: 500,
            message: format!("status is not terminal: {other:?}"),
            metadata: vec![],
        }),
    }
}

pub(super) fn bad_id(completion_id: u32, raw: &str) -> Completion {
    Completion {
        id: completion_id,
        result: CompletionResult::CallFailure(pb::Failure {
            code: 400,
            message: format!("invalid invocation id {raw:?}"),
            metadata: vec![],
        }),
    }
}

pub(super) fn not_found(completion_id: u32, target: &InvocationId) -> Completion {
    Completion {
        id: completion_id,
        result: CompletionResult::CallFailure(pb::Failure {
            code: 404,
            message: format!("invocation {} not found", hex(target)),
            metadata: vec![],
        }),
    }
}

/// Byte 9 of a `retries` row value: a watchdog row — see
/// [`Engine::begin_turn`]. Absent (an 8-byte row) = an ordinary retry.
pub const WATCH_FLAG: u8 = cairn_lifecycle::codec::WATCH_FLAG;

/// A watchdog row: `attempt` 0, journal length "unknown" (`u32::MAX`, like old
/// rows — the progress comparison does not reset the count), flag
/// [`WATCH_FLAG`].
///
/// Every encoding here goes through `cairn_lifecycle::codec`: one place for
/// the 4/8/9/13-byte layouts; `tests/retry_codec.rs` pins every byte.
pub(super) fn encode_watch() -> Payload {
    RetryRow::legacy_watch(0)
        .encode_as(Width::W9)
        .expect("guard row without fires fits in 9 bytes")
}

/// The retry row decoded from the payload of deadline `due`.
///
/// The codec reads unknown lengths strictly; no binary writes those lengths,
/// so the engine reads them **leniently** as before the codec — missing bytes
/// ⇒ default values (`attempt` 0, `journal_len` unknown, no flag, `fires` 0)
/// — so that one broken row does not block the background tick forever (a
/// deliberate deviation from the codec's strictness).
pub(crate) fn decode_row(due: u64, payload: &[u8]) -> RetryRow {
    RetryRow::decode(due, payload).unwrap_or_else(|_| {
        let u32_at = |i: usize| {
            payload
                .get(i..i + 4)
                .and_then(|b| b.try_into().ok())
                .map(u32::from_be_bytes)
        };
        RetryRow {
            due,
            attempt: u32_at(0).unwrap_or(0),
            journal_len: u32_at(4).unwrap_or(u32::MAX),
            fires: u32_at(9).unwrap_or(0),
            kind: if payload.get(8) == Some(&WATCH_FLAG) {
                RowKind::Watch
            } else {
                RowKind::Retry
            },
        }
    })
}

/// Whether a `retries` row value (after `due`) is a watchdog row.
pub(super) fn is_watch(payload: &[u8]) -> bool {
    decode_row(0, payload).is_watch()
}

/// A `retries` row: `attempt | journal length at scheduling time`, both big-endian.
pub(super) fn encode_retry(attempt: u32, journal_len: u32) -> Payload {
    RetryRow::retry(0, attempt, journal_len, 0)
        .encode_as(Width::W8)
        .expect("plain row without fires fits in 8 bytes")
}

/// A `retries` row with the **uncounted fires** (FL-21): `attempt | journal
/// length | flag 0 | fires`. `fires` = the number of times the background
/// tick handed over a turn since the last counted attempt (a turn entering
/// `Running`) — the backoff step is `attempt + fires`, so a turn that is
/// throttled or cancelled before `Running` uses no attempt but still pushes
/// the next fire further out.
///
/// Read leniently like `journal_len` before it: an old binary reads the first
/// 8 bytes and the flag byte (0 = not a watchdog row), ignoring the tail. A row
/// without the tail means `fires` 0. No `DATA_FORMAT` change.
pub(super) fn encode_retry_fired(attempt: u32, journal_len: u32, fires: u32) -> Payload {
    RetryRow::retry(0, attempt, journal_len, fires)
        .encode_as(Width::W13)
        .expect("13 bytes fit every row")
}

/// The result the user passes to `CompletePromise`, in durable form.
pub(super) fn promise_reply(c: &pb::complete_promise_command_message::Completion) -> ReplyResult {
    use pb::complete_promise_command_message::Completion as C;
    match c {
        // The machine checked inline-ness on receipt (`value::sdk_values_inline`);
        // a mismatch fails with a code, the tail is never read as the whole value.
        C::CompletionValue(v) => match cairn_protocol::value::inline_content(Some(v)) {
            Ok(b) => ReplyResult::Value(b),
            Err(e) => ReplyResult::Failure {
                code: cairn_protocol::PROTOCOL_VIOLATION,
                message: e.to_string(),
            },
        },
        C::CompletionFailure(f) => ReplyResult::Failure {
            code: f.code,
            message: f.message.clone(),
        },
    }
}

pub(super) fn signal_id_of(s: &pb::send_signal_command_message::SignalId) -> SignalId {
    use pb::send_signal_command_message::SignalId as S;
    match s {
        S::Idx(i) => SignalId::Idx(*i),
        S::Name(n) => SignalId::Name(n.clone()),
    }
}

pub(super) fn signal_payload_of(
    r: Option<&pb::send_signal_command_message::Result>,
) -> CompletionPayload {
    use pb::send_signal_command_message::Result as R;
    match r {
        None | Some(R::Void(_)) => CompletionPayload::Void,
        Some(R::Value(v)) => match cairn_protocol::value::inline_content(Some(v)) {
            Ok(b) => CompletionPayload::Value(b),
            Err(e) => CompletionPayload::Failure {
                code: cairn_protocol::PROTOCOL_VIOLATION,
                message: e.to_string(),
            },
        },
        Some(R::Failure(f)) => CompletionPayload::Failure {
            code: f.code,
            message: f.message.clone(),
        },
    }
}

/// An SDK frame carrying a user value (`Output`, `SetState`) larger than
/// `max`: `(frame size, limit)`. Measures the whole frame payload — a few
/// bytes more than the value (key, name), leaning towards strict.
/// An SDK frame over `max-payload=`: every frame the runtime **writes to the
/// journal** — commands (`Output`, `SetState`, `Call`/`OneWayCall`,
/// `SendSignal`, `CompletePromise`, …), `ctx.run` results
/// (`ProposeRunCompletion`) and the new input of `ContinueAsNew`. The side
/// channel (`ArtifactPut` has its own limit `max-artifact-bytes=`,
/// `StreamAppend` has the stream's limit) and control frames (`End`,
/// `Suspension`, `Error`, ack) are not considered. The journal is what gets
/// resent on every REQUEST_RESPONSE turn: blobs must go through
/// `ctx.artifact`.
fn oversized(ev: &Event, max: Option<u64>) -> Option<(usize, u64)> {
    // A frame the shell has sealed is subject to the same limit: only ref +
    // tail remain, so the limit applies to exactly the inline part.
    let (Event::FromSdk(f) | Event::Sealed(f), Some(max)) = (ev, max) else {
        return None;
    };
    oversized_frame(f, max)
}

/// The largest logical value of an SDK frame that **writes** a value
/// (`SetState`, `Output`, `ProposeRunCompletion`) when it exceeds `max` —
/// `(len, max)`. A frame that does not decode or of another kind ⇒ `None` (the
/// machine handles broken frames itself).
fn value_over(ev: &Event, max: Option<u64>) -> Option<(u64, u64)> {
    use cairn_protocol::MessageType as T;
    let (Event::FromSdk(f) | Event::Sealed(f), Some(max)) = (ev, max) else {
        return None;
    };
    if !(f.ty == T::SET_STATE || f.ty == T::OUTPUT || f.ty == T::PROPOSE_RUN_COMPLETION) {
        return None;
    }
    let m = cairn_protocol::decode(f).ok()?;
    let len = match &m {
        // An inline `ctx.run` result (`bytes value = 14`) is not a `Value`.
        cairn_protocol::Message::ProposeRunCompletion(p) => match &p.result {
            Some(pb::propose_run_completion_message::Result::Value(b)) => b.len() as u64,
            _ => max_logical(&m),
        },
        _ => max_logical(&m),
    };
    (len > max).then_some((len, max))
}

/// The largest logical length among all `Value`s of `m` (broken layout ⇒ encoded size).
fn max_logical(m: &cairn_protocol::Message) -> u64 {
    use cairn_protocol::value::{encoded_len, Logical};
    cairn_protocol::value::values(m)
        .into_iter()
        .map(|v| Logical::of(Some(v)).map_or(encoded_len(v) as u64, |l| l.len()))
        .max()
        .unwrap_or(0)
}

fn oversized_frame(f: &cairn_protocol::Frame, max: u64) -> Option<(usize, u64)> {
    use cairn_protocol::{MessageKind, MessageType as T};
    let journaled = f.ty.kind() == MessageKind::Command
        || f.ty == T::PROPOSE_RUN_COMPLETION
        || f.ty == T::CONTINUE_AS_NEW;
    (journaled && f.payload.len() as u64 > max).then_some((f.payload.len(), max))
}

/// Builds the `SignalNotification` (0xFBFF) delivered to the target SDK from a cross-partition message.
pub fn signal_notification(ds: &DeliverSignal) -> pb::SignalNotificationMessage {
    use pb::signal_notification_message::{Result as R, SignalId as S};
    pb::SignalNotificationMessage {
        signal_id: Some(match &ds.signal {
            SignalId::Idx(i) => S::Idx(*i),
            SignalId::Name(n) => S::Name(n.clone()),
        }),
        result: Some(match &ds.result {
            CompletionPayload::Value(v) => R::Value(cairn_protocol::value::inline(v.clone())),
            CompletionPayload::Failure { code, message } => R::Failure(pb::Failure {
                code: *code,
                message: message.clone(),
                metadata: vec![],
            }),
            // `InvocationId` (and segmented output) mean nothing for a signal;
            // sending one here is a bug where the message was built, but the
            // target is innocent — deliver Void instead of killing it.
            CompletionPayload::Segmented(_) => {
                // Tripwire: a signal carrying a segmented output — the engine
                // would have to assemble it.
                cairn_store::failpoint!("value::engine_needs_bytes");
                R::Void(pb::Void {})
            }
            CompletionPayload::InvocationId(_) | CompletionPayload::Void => R::Void(pb::Void {}),
        }),
    }
}

pub(super) fn status_of(o: &Outcome, inv: &Invocation) -> InvocationStatus {
    use cairn_runtime::FailureAction as A;
    match o {
        // An `Output` carrying `failure` is the final result, and it is a
        // failure. Folding it into `Completed` with an empty payload would lose
        // the error code and message entirely — the client sees "done" with an
        // empty output, without knowing why.
        Outcome::Completed(out) => match &out.result {
            // Segmented ⇒ ref + tail with the source owner.
            Some(cairn_protocol::pb::output_command_message::Result::Value(v)) => {
                match crate::output::output_of(v, &inv.target, &inv.id) {
                    Ok(b) => InvocationStatus::Completed(b),
                    Err(e) => InvocationStatus::Failed {
                        code: cairn_protocol::PROTOCOL_VIOLATION,
                        message: e.to_string(),
                    },
                }
            }
            Some(cairn_protocol::pb::output_command_message::Result::Failure(f)) => {
                InvocationStatus::Failed {
                    code: f.code,
                    message: f.message.clone(),
                }
            }
            None => InvocationStatus::Completed(Bytes::new().into()),
        },
        Outcome::Suspended(_) => InvocationStatus::Suspended,
        Outcome::Failed {
            code,
            message,
            action,
        } => match action {
            // Retry: the invocation will run again, so the durable status is still Running.
            A::Retry { .. } => InvocationStatus::Running,
            A::Pause => InvocationStatus::Paused {
                code: *code,
                message: message.clone(),
            },
            A::Fail => InvocationStatus::Failed {
                code: *code,
                message: message.clone(),
            },
        },
    }
}

pub(super) fn reply_result(o: &Outcome, inv: &Invocation) -> Option<ReplyResult> {
    use cairn_runtime::FailureAction as A;
    match o {
        Outcome::Completed(out) => Some(match &out.result {
            Some(cairn_protocol::pb::output_command_message::Result::Value(v)) => {
                match crate::output::output_of(v, &inv.target, &inv.id) {
                    Ok(b) => b.into_reply(),
                    Err(e) => ReplyResult::Failure {
                        code: cairn_protocol::PROTOCOL_VIOLATION,
                        message: e.to_string(),
                    },
                }
            }
            Some(cairn_protocol::pb::output_command_message::Result::Failure(f)) => {
                ReplyResult::Failure {
                    code: f.code,
                    message: f.message.clone(),
                }
            }
            None => ReplyResult::Value(Bytes::new()),
        }),
        Outcome::Failed {
            code,
            message,
            action,
        } => match action {
            // A pending retry is not the final result yet; do not return early.
            A::Retry { .. } => None,
            _ => Some(ReplyResult::Failure {
                code: *code,
                message: message.clone(),
            }),
        },
        // Suspended means not done yet.
        Outcome::Suspended(_) => None,
    }
}

/// The bytes an entry takes in the durable journal (`JournalEntry::encode`: two
/// type bytes + payload) — what `max-journal-bytes=` counts.
pub(super) fn entry_len(entry: &cairn_runtime::JournalEntry) -> u64 {
    2 + entry.payload.len() as u64
}
