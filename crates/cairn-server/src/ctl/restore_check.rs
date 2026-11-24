// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Detecting inter-range messages lost after a restore — the `restore.lost_call` /
//! `restore.lost_reply` checks of `verify`.
//!
//! A backup takes checkpoints **one range at a time** (`ctl/backup.rs`), so it is not a
//! consistent cut of the whole cluster. A call between two ranges loses its message
//! when:
//!
//! - **case A, the call message** — the caller's range is captured **after** it received
//!   the callee id (the `Call` outbox row deleted), the callee's range is captured
//!   **before** the `Inbound` commit of the `start_call` message: the caller waits for
//!   the result of a callee that does not exist;
//! - **case B, the reply** — the callee's range is captured after the callee finished
//!   and its `Reply` outbox row was deleted (the caller received it), the caller's range
//!   is captured **before** the `Inbound` commit of the completion: the caller waits
//!   forever for a result already delivered.
//!
//! **What is inspected** (built from the five SQL queries of step 5 of
//! `docs/runbooks/restore-from-backup.md`, plus the tables they do not look at):
//! - `InvocationStatus` of every invocation; the journal of **unfinished** invocations
//!   (`running`, `suspended`, `paused`): `Call` entries waiting for a result, the callee
//!   id taken from their `CallInvocationIdCompletion` notification;
//! - `Outbox` on both sides: the caller's `Call` rows (messages not sent yet), `Reply`
//!   rows on the callee's range (replies not sent yet);
//! - `Inbound` on both sides: `start_call` on the callee's range (keyed by caller),
//!   completions on the caller's range (replies received, not yet in the journal);
//! - the caller's `Delayed`; the callee's `Attachments` (extra waiters);
//! - calls batched within one range (`DeliverLocal`, one commit) and `Idempotency` rows
//!   need no separate inspection: both sides live in **one** Db, one checkpoint.
//!
//! If any row is still "in flight" (outbox, inbound, delayed, attachment), the call is
//! **not** lost yet: skip it, no guessing. Missing a table means false positives.
//!
//! **A running cluster:** a call in progress looks lost when the two ranges are read at
//! two different moments. A candidate only becomes a finding when it is still intact on
//! the second read, re-read **in causal order** (case A: the caller's range, then the
//! callee's; case B: the callee's range, then the caller's — the caller only learns the
//! callee id after the callee's range is durable, and the callee's range only deletes
//! the reply after the caller is durable), each reader waiting until it has replayed the
//! shared WAL tail (`settle`). On a freshly restored bucket (the cluster stopped,
//! `cairn-ctl backup check-restore`) the two reads are the same.
//!
//! There is a row cap per range ([`RESTORE_MAX_ROWS`]); over ⇒ one `heals("not
//! checked")` line — "not fully scanned", never reported as lost.

use {
    super::{settle, Ctl, Finding},
    cairn_store::{
        keys::{self as keyspace, table_of, Table},
        InvocationId, PartitionReader,
    },
    std::collections::{BTreeMap, BTreeSet},
};

/// Check name: a lost call message (case A).
pub const LOST_CALL: &str = "restore.lost_call";
/// Check name: a lost reply (case B).
pub const LOST_REPLY: &str = "restore.lost_reply";

/// Cap on the rows per range the check considers (status, outbox, inbound, delayed,
/// attachment combined) — each row keeps only its key and one flag. Over ⇒ that range is
/// not checked.
const RESTORE_MAX_ROWS: usize = 1_000_000;

/// Cap on the unfinished invocations per range whose journal the check reads.
const RESTORE_MAX_LIVE: usize = 100_000;

/// Cap on the candidates re-read per `verify`; over ⇒ one `heals("not checked")` line.
const RESTORE_MAX_CANDIDATES: usize = 1_000;

/// What a range tells about its calls.
#[derive(Debug, Default)]
pub(super) struct RangeFacts {
    /// Invocations of the range ⇒ whether finished (`completed`/`failed`).
    status: BTreeMap<InvocationId, bool>,
    /// Calls waiting for a result, by invocations of the range.
    waiting: Vec<Waiting>,
    /// The `(id, completion)` keys of every outbox row in the range.
    outbox: BTreeSet<(InvocationId, u32)>,
    /// The `id` in the key of every inbound row in the range.
    inbound: BTreeSet<InvocationId>,
    /// Callers with a `Delayed` row in the range.
    delayed: BTreeSet<InvocationId>,
    /// The waiters (`caller`) in every `Attachments` row of the range.
    attached: BTreeSet<InvocationId>,
    /// Over the cap: no conclusion about this range.
    truncated: bool,
}

/// A `Call` entry waiting for a result.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Waiting {
    caller: InvocationId,
    callee: InvocationId,
    /// The completion carrying the result.
    result: u32,
}

/// A lost-message candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Lost {
    Call(Waiting),
    Reply(Waiting),
}

fn hex(id: &InvocationId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<InvocationId> {
    let b = s.as_bytes();
    let mut out = [0u8; cairn_store::ID_LEN];
    if b.len() != 2 * out.len() {
        return None;
    }
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(std::str::from_utf8(&b[2 * i..2 * i + 2]).ok()?, 16).ok()?;
    }
    Some(out)
}

/// Reads the facts of a range. A read error ⇒ `Err` (the caller reports it as a Db error).
pub(super) async fn collect(reader: &PartitionReader) -> cairn_store::Result<RangeFacts> {
    use cairn_engine::InvocationStatus as S;
    let mut f = RangeFacts::default();
    // Only the key and one flag per status are kept — the values (customer payloads) do
    // not stay in RAM.
    let mut rows = 0usize;
    let mut live = Vec::new();
    reader
        .scan_fold(&[], |k, v| {
            let t = table_of(k);
            if !matches!(
                t,
                Some(
                    Table::InvocationStatus
                        | Table::Outbox
                        | Table::Inbound
                        | Table::Delayed
                        | Table::Attachments
                )
            ) {
                return true;
            }
            rows += 1;
            if rows >= RESTORE_MAX_ROWS {
                f.truncated = true;
                return false;
            }
            match t {
                Some(Table::InvocationStatus) => {
                    if let Some(id) = keyspace::parse_invocation_status(k) {
                        // A status that cannot be decoded: treated as unfinished — never
                        // leads to "callee finished but reply lost".
                        let done = matches!(
                            S::decode(bytes::Bytes::copy_from_slice(v)),
                            Some(S::Completed(_) | S::Failed { .. })
                        );
                        f.status.insert(id, done);
                        if !done {
                            live.push(id);
                        }
                    }
                }
                Some(Table::Outbox) => {
                    if let Some(k) = keyspace::parse_outbox(k) {
                        f.outbox.insert(k);
                    }
                }
                Some(Table::Inbound) => {
                    if let Some((_, id, _)) = keyspace::parse_inbound(k) {
                        f.inbound.insert(id);
                    }
                }
                Some(Table::Delayed) => {
                    if let Some((id, _)) = keyspace::parse_delayed(k) {
                        f.delayed.insert(id);
                    }
                }
                Some(Table::Attachments) => {
                    if let Some((caller, _)) = keyspace::parse_attachment(k) {
                        f.attached.insert(caller);
                    }
                }
                _ => {}
            }
            true
        })
        .await?;
    if f.truncated {
        return Ok(f);
    }
    if live.len() > RESTORE_MAX_LIVE {
        f.truncated = true;
        return Ok(f);
    }
    for caller in live {
        f.waiting.extend(waiting_calls(reader, caller).await?);
    }
    Ok(f)
}

/// The `Call` entries of `caller` still waiting for a result with the callee id known.
async fn waiting_calls(
    reader: &PartitionReader,
    caller: InvocationId,
) -> cairn_store::Result<Vec<Waiting>> {
    use cairn_protocol::journal_view::{pending, view};
    let views: Vec<_> = reader
        .journal(&caller)
        .await?
        .into_iter()
        .filter_map(|(_, raw)| cairn_runtime::journal::JournalEntry::decode(raw).ok())
        .map(|e| view(e.ty, &e.payload))
        .collect();
    // id completion ⇒ id callee (notification `CallInvocationIdCompletion`).
    let callee_of: BTreeMap<u32, InvocationId> = views
        .iter()
        .filter_map(|v| Some((v.lite.completes?, unhex(v.lite.invocation_id.as_deref()?)?)))
        .collect();
    Ok(pending(&views)
        .into_iter()
        .filter_map(|i| {
            let l = &views[i].lite;
            (l.kind == Some("Call")).then_some(())?;
            Some(Waiting {
                caller,
                callee: *callee_of.get(&l.awaits_invocation_id?)?,
                result: l.awaits?,
            })
        })
        .collect())
}

/// The lost-message candidate of call `w`, given the facts of the caller's range `a` and
/// the callee's range `b`. `None` = not lost, or no conclusion possible.
fn judge(w: &Waiting, a: &RangeFacts, b: &RangeFacts) -> Option<Lost> {
    if a.truncated || b.truncated {
        return None;
    }
    match b.status.get(&w.callee) {
        // The callee is still running: the call is alive.
        Some(false) => None,
        // The callee does not exist: if the call message is still in flight (the caller's
        // outbox/delayed, the callee range's inbound), it is not lost yet.
        None => {
            let in_flight = a.outbox.iter().any(|(id, _)| *id == w.caller)
                || a.delayed.contains(&w.caller)
                || b.inbound.contains(&w.caller);
            (!in_flight).then(|| Lost::Call(w.clone()))
        }
        // The callee is finished: if the reply is still in flight (the reply outbox on
        // the callee's range, the completion inbound on the caller's range, extra waiters
        // of the callee), it is not lost yet.
        Some(true) => {
            let in_flight = b.outbox.contains(&(w.caller, w.result))
                || a.inbound.contains(&w.caller)
                || b.attached.contains(&w.caller);
            (!in_flight).then(|| Lost::Reply(w.clone()))
        }
    }
}

/// Every candidate of the read `facts`. Calls within one range are considered too — one
/// Db, one checkpoint, so they never become candidates.
fn candidates(
    table: &cairn_store::PartitionTable,
    facts: &BTreeMap<u32, RangeFacts>,
) -> Vec<(u32, u32, Lost)> {
    let mut out = Vec::new();
    for (ra, a) in facts {
        for w in &a.waiting {
            let rb = table
                .partition_of_key(cairn_store::partition_key_of(&w.callee))
                .0;
            if let Some(b) = facts.get(&rb) {
                if let Some(l) = judge(w, a, b) {
                    out.push((*ra, rb, l));
                }
            }
        }
    }
    out
}

/// The finding of a confirmed candidate.
fn finding(ra: u32, rb: u32, lost: Lost) -> Finding {
    match lost {
        Lost::Call(w) => Finding::new(
            LOST_CALL,
            Some(ra),
            format!(
                "invocation {} waits for the result (completion {}) of a call to {} in range {rb}, \
                 but range {rb} has neither the callee nor a start_call message, and the caller no \
                 longer has an outbox/delayed row — the call message is lost (backup captured \
                 range {rb} before it received the message?)",
                hex(&w.caller),
                w.result,
                hex(&w.callee),
            ),
        ),
        Lost::Reply(w) => Finding::new(
            LOST_REPLY,
            Some(ra),
            format!(
                "invocation {} waits for the result (completion {}) of {} in range {rb}, the \
                 callee has finished but range {rb} no longer has an outbox reply row and range \
                 {ra} has no completion in inbound — the reply is lost (backup captured range {ra} \
                 before it received the reply?)",
                hex(&w.caller),
                w.result,
                hex(&w.callee),
            ),
        ),
    }
}

impl Ctl {
    /// Re-reads (with `settle`) each range of `ranges`; a range that fails to read becomes
    /// a `db` finding and is absent from the result (its candidates reach no conclusion).
    async fn reread(
        &self,
        ranges: &BTreeSet<u32>,
        out: &mut Vec<Finding>,
    ) -> BTreeMap<u32, RangeFacts> {
        let mut got = BTreeMap::new();
        for r in ranges {
            match self.settled_facts(*r).await {
                Ok(f) => {
                    got.insert(*r, f);
                }
                Err(e) => out.push(Finding::new(
                    "db",
                    Some(*r),
                    format!("cannot reread the Db for the cross-range message check: {e}"),
                )),
            }
        }
        got
    }

    async fn settled_facts(&self, range: u32) -> super::CtlResult<RangeFacts> {
        let (reader, _tailers) = self.open_reader(range).await?;
        settle(&reader).await;
        let f = collect(&reader).await;
        let _ = reader.close().await;
        Ok(f?)
    }

    /// Findings from the facts of the first read `facts`: each candidate is re-read in
    /// causal order (see the module head) and only becomes a finding if still intact.
    pub(super) async fn restore_findings(
        &self,
        table: &cairn_store::PartitionTable,
        facts: &BTreeMap<u32, RangeFacts>,
    ) -> Vec<Finding> {
        let mut out: Vec<Finding> = facts
            .iter()
            .filter(|(_, f)| f.truncated)
            .map(|(r, _)| {
                Finding::new(
                    LOST_CALL,
                    Some(*r),
                    format!(
                        "range too large for the cross-range message check (> {RESTORE_MAX_ROWS} \
                         rows or > {RESTORE_MAX_LIVE} unfinished invocations) — not fully scanned"
                    ),
                )
                .heals("not checked")
            })
            .collect();
        let mut all = candidates(table, facts);
        if all.len() > RESTORE_MAX_CANDIDATES {
            out.push(
                Finding::new(
                    LOST_CALL,
                    None,
                    format!(
                        "{} candidate lost cross-range messages (> {RESTORE_MAX_CANDIDATES}) — \
                         only rereading the first {RESTORE_MAX_CANDIDATES}",
                        all.len()
                    ),
                )
                .heals("not checked"),
            );
            all.truncate(RESTORE_MAX_CANDIDATES);
        }
        let (calls, replies): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|(_, _, l)| matches!(l, Lost::Call(_)));
        // Re-read in batches, in causal order: case A reads every caller range **then**
        // every callee range; case B the other way round. Each range at most twice per case.
        for (batch, caller_first) in [(calls, true), (replies, false)] {
            if batch.is_empty() {
                continue;
            }
            let callers: BTreeSet<u32> = batch.iter().map(|(ra, _, _)| *ra).collect();
            let callees: BTreeSet<u32> = batch.iter().map(|(_, rb, _)| *rb).collect();
            let (first, second) = if caller_first {
                (callers, callees)
            } else {
                (callees, callers)
            };
            let first = self.reread(&first, &mut out).await;
            let second = self.reread(&second, &mut out).await;
            let (at_caller, at_callee) = if caller_first {
                (&first, &second)
            } else {
                (&second, &first)
            };
            for (ra, rb, lost) in batch {
                let (Some(a), Some(b)) = (at_caller.get(&ra), at_callee.get(&rb)) else {
                    continue;
                };
                let w = match &lost {
                    Lost::Call(w) | Lost::Reply(w) => w,
                };
                // Exactly that candidate still stands (the caller still waits for that very call).
                if !a.waiting.contains(w) || judge(w, a, b).as_ref() != Some(&lost) {
                    continue;
                }
                out.push(finding(ra, rb, lost));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> InvocationId {
        [b; cairn_store::ID_LEN]
    }

    fn call() -> Waiting {
        Waiting {
            caller: id(1),
            callee: id(2),
            result: 3,
        }
    }

    #[test]
    fn hex_round_trips() {
        let i = id(0xab);
        assert_eq!(unhex(&hex(&i)), Some(i));
        assert_eq!(unhex("xyz"), None);
    }

    /// The table of `judge`: each "in flight" row makes the call **not** lost.
    #[test]
    fn in_flight_rows_are_not_losses() {
        let w = call();
        let (a, mut b) = (RangeFacts::default(), RangeFacts::default());
        assert_eq!(judge(&w, &a, &b), Some(Lost::Call(w.clone())));
        b.inbound.insert(w.caller);
        assert_eq!(judge(&w, &a, &b), None, "start_call not processed yet");
        let mut a = RangeFacts::default();
        let b = RangeFacts::default();
        a.outbox.insert((w.caller, 9));
        assert_eq!(judge(&w, &a, &b), None, "call message not sent yet");
        let mut a = RangeFacts::default();
        a.delayed.insert(w.caller);
        assert_eq!(judge(&w, &a, &b), None, "delayed call");

        let a = RangeFacts::default();
        let mut b = RangeFacts::default();
        b.status.insert(w.callee, false);
        assert_eq!(judge(&w, &a, &b), None, "callee still running");
        b.status.insert(w.callee, true);
        assert_eq!(judge(&w, &a, &b), Some(Lost::Reply(w.clone())));
        b.outbox.insert((w.caller, w.result));
        assert_eq!(judge(&w, &a, &b), None, "reply not sent yet");
        let mut b = RangeFacts::default();
        b.status.insert(w.callee, true);
        let mut a = RangeFacts::default();
        a.inbound.insert(w.caller);
        assert_eq!(judge(&w, &a, &b), None, "reply received, not journaled");
        let a = RangeFacts::default();
        b.attached.insert(w.caller);
        assert_eq!(judge(&w, &a, &b), None, "extra waiter of the callee");
        b.truncated = true;
        assert_eq!(judge(&w, &a, &b), None, "range not fully scanned");
    }
}
