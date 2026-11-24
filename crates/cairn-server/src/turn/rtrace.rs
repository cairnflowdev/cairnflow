// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Runtime** spans in `$trace`: when a handler declares `trace: "runtime"`,
//! each turn gets a `cairnflow.turn` span, and each `ctx.run` a child span —
//! from the `Run` command to its `ProposeRunCompletion`. Off by default: the
//! `tracing` spans `cairnflow.run`/`cairnflow.turn` still go to OTLP as before,
//! not into `$trace`.
//!
//! Across a retry: a `ctx.run` span carries `attrs.entry` — the index of the
//! `Run` entry in the journal; a turn that does not reach `Completed`/`Waiting`
//! closes with `error`, and a run still open at that point closes with `error`
//! plus `abandoned`; a `ProposeRunCompletion` of a `Run` already in the journal
//! from an earlier attempt (no open span) records a whole span with `start` =
//! the turn's start time and `start_estimated`.
//!
//! Records go through `RangeStreams::append_runtime` — they take no part in
//! the SDK's `(step, attempt, k)` deduplication. `span_id` is hashed from the
//! invocation id, the attempt, the turn's start time and the order within the
//! turn: it does not need to be deterministic across replay (the runtime never
//! replays its spans — every turn is a real turn).

use {
    super::*,
    sha2::{Digest, Sha256},
};

pub(super) struct RuntimeTrace {
    on: bool,
    id: InvocationId,
    /// The invocation's tenant — the per-tenant stream RAM share of `$trace`.
    tenant: cairn_store::TenantId,
    attempt: u32,
    started_ms: u64,
    n: u64,
    turn: Option<String>,
    /// Open `ctx.run`s: completion id → span id.
    runs: std::collections::BTreeMap<u32, String>,
}

impl RuntimeTrace {
    pub(super) fn new(
        on: bool,
        id: InvocationId,
        tenant: cairn_store::TenantId,
        attempt: u32,
        now_ms: u64,
    ) -> Self {
        Self {
            on,
            id,
            tenant,
            attempt,
            started_ms: now_ms,
            n: 0,
            turn: None,
            runs: Default::default(),
        }
    }

    fn next_id(&mut self) -> String {
        self.n += 1;
        let mut h = Sha256::new();
        h.update(self.id);
        h.update(self.attempt.to_be_bytes());
        h.update(self.started_ms.to_be_bytes());
        h.update(self.n.to_be_bytes());
        h.update(b"cairnflow.runtime");
        let d = h.finalize();
        d[..8].iter().map(|b| format!("{b:02x}")).collect()
    }

    async fn emit(&self, engine: &mut Lease, ctx: &Ctx<'_>, v: serde_json::Value) {
        let name = crate::streams::TRACE;
        // The tenant's stream RAM share (from a security review).
        ctx.streams.bind_tenant(
            &self.id,
            &self.tenant,
            engine.tenant_limit(&self.tenant).max_stream_ram,
        );
        if ctx.streams.needs_seed(&self.id, name, true) {
            match engine.writer().stream_tail_unbounded(&self.id, name).await {
                Ok(tail) => ctx.streams.seed(&self.id, name, tail),
                Err(e) => {
                    tracing::debug!(error = %e, "cannot read the tail of $trace — runtime span dropped");
                    return;
                }
            }
        }
        ctx.streams
            .append_runtime(&self.id, name, self.attempt, Bytes::from(v.to_string()));
    }

    fn now(engine: &Lease) -> u64 {
        engine.clock().wall_ms()
    }

    /// The turn starts.
    pub(super) async fn begin(&mut self, engine: &mut Lease, ctx: &Ctx<'_>) {
        if !self.on {
            return;
        }
        let sid = self.next_id();
        let v = serde_json::json!({
            "kind": "start", "span_id": sid, "parent_id": null, "name": "cairnflow.turn",
            "ts": Self::now(engine), "attrs": { "cairnflow.runtime": true, "attempt": self.attempt },
        });
        self.turn = Some(sid);
        self.emit(engine, ctx, v).await;
    }

    /// One frame sent up by the SDK, before it enters the machine: `Run` opens a
    /// child span, `ProposeRunCompletion` closes it. `inv` is the state **before**
    /// the frame: the incoming `Run` command will get index `inv.known_entries()`.
    pub(super) async fn frame(
        &mut self,
        engine: &mut Lease,
        ctx: &Ctx<'_>,
        inv: &Invocation,
        f: &cairn_protocol::Frame,
    ) {
        if !self.on {
            return;
        }
        use cairn_protocol::MessageType;
        if f.ty == MessageType::RUN {
            if let Ok(Message::Run(m)) = cairn_protocol::decode(f) {
                let sid = self.next_id();
                let v = serde_json::json!({
                    "kind": "start", "span_id": sid, "parent_id": self.turn,
                    "name": if m.name.is_empty() { "run".to_string() } else { m.name.clone() },
                    "ts": Self::now(engine),
                    "attrs": { "cairnflow.runtime": true, "entry": inv.known_entries() },
                });
                self.runs.insert(m.result_completion_id, sid);
                self.emit(engine, ctx, v).await;
            }
        } else if f.ty == MessageType::PROPOSE_RUN_COMPLETION {
            if let Ok(Message::ProposeRunCompletion(m)) = cairn_protocol::decode(f) {
                let failed = matches!(
                    m.result,
                    Some(pb::propose_run_completion_message::Result::Failure(_))
                );
                let status = if failed { "error" } else { "ok" };
                let sid = match self.runs.remove(&m.result_completion_id) {
                    Some(sid) => sid,
                    // The `Run` was already in the journal from an earlier attempt:
                    // the closure reruns in this turn without the runtime seeing it start.
                    None => {
                        let Some(entry) = inv.run_entry(m.result_completion_id) else {
                            return;
                        };
                        let sid = self.next_id();
                        let name = entry_name(inv, entry);
                        let v = serde_json::json!({
                            "kind": "start", "span_id": sid, "parent_id": self.turn,
                            "name": name, "ts": self.started_ms,
                            "attrs": {
                                "cairnflow.runtime": true, "entry": entry, "start_estimated": true,
                            },
                        });
                        self.emit(engine, ctx, v).await;
                        sid
                    }
                };
                let v = serde_json::json!({
                    "kind": "end", "span_id": sid, "ts": Self::now(engine), "status": status,
                });
                self.emit(engine, ctx, v).await;
            }
        }
    }

    /// The turn ends: `ok` = the turn reached `Completed` or `Waiting` — a
    /// transient error, a stop, a failure or the turn's own error is `error`. A
    /// run still open (closure not finished when the turn ended) closes with
    /// `error` plus `abandoned` before the turn span.
    pub(super) async fn end(&mut self, engine: &mut Lease, ctx: &Ctx<'_>, ok: bool) {
        let Some(sid) = self.turn.take().filter(|_| self.on) else {
            return;
        };
        for (_, run) in std::mem::take(&mut self.runs) {
            let v = serde_json::json!({
                "kind": "end", "span_id": run, "ts": Self::now(engine), "status": "error",
                "attrs": { "abandoned": true },
            });
            self.emit(engine, ctx, v).await;
        }
        let v = serde_json::json!({
            "kind": "end", "span_id": sid, "ts": Self::now(engine),
            "status": if ok { "ok" } else { "error" },
        });
        self.emit(engine, ctx, v).await;
    }
}

/// The name of the `ctx.run` at the `entry`-th `Run` entry (empty ⇒ `run`, like a span opened by
/// frame `Run`).
fn entry_name(inv: &Invocation, entry: u32) -> String {
    inv.run_name(entry)
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| "run".to_string())
}
