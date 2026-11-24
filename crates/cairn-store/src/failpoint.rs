// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Named failpoints (inspired by TiDB's `testfailpoint`).
//!
//! `FaultyStore` intercepts at the object layer: "die at the k-th write". Failpoints cover
//! the rest — the gap between two steps **in RAM**, injected logic errors ("the transport
//! returns 409 exactly once"), and the real process of the chaos harness (through the
//! server's `/admin/failpoints`).
//!
//! **Off by default.** The `fail` crate (tikv/fail-rs) only generates code when the
//! `failpoints` feature is on (this crate's; `cairn-server/failpoints` turns it on too).
//! When off, [`failpoint!`] disappears and the release binary carries nothing — not even
//! the point name strings.
//!
//! **Global registry.** `fail` keeps its configuration in a process-wide static, so tests
//! that use failpoints run sequentially in a separate binary (`tests/failpoints.rs`), each
//! test starting with `testkit::StepController::new()` (which holds `fail`'s scenario
//! lock).
//!
//! **Place them at boundaries**, before or after a step, not scattered; and **not inside
//! the region that holds the engine mutex** unless that is exactly what is being tested —
//! a callback that blocks there changes the execution order of every other turn on the
//! range. List of placed points: [`NAMES`].

/// Every named failpoint in the repo, in the form `<module>::<point>`. The server's
/// `/admin/failpoints` refuses names outside this list — a typo in the chaos harness must
/// not silently become "enable a point nobody calls".
///
/// Adding a new point means adding its name here.
pub const NAMES: &[&str] = &[
    // ── Scheduler tripwires (AGENTS §6.4): `secure` grants a permit to an invocation that
    // already has a live permit; `release_all` releases the VO lock while the invocation
    // still has another live permit. Must never be reached — chaos enables `panic`.
    "flow::second_live_permit",
    "flow::lock_released_by_non_holder",
    // ── Incremental replay of REQUEST_RESPONSE: the SDK has returned a `409` miss, the
    // runtime has not yet resent the full replay. `return` ⇒ the turn fails like a
    // transport error.
    "rr_replay::after_miss",
    // ── Compressing/decompressing REQUEST_RESPONSE bodies: the start of
    // `invoker::compress` (bodies from the compression threshold up) and
    // `invoker::gunzip_capped` — tests stop here to check that compression does not run
    // on the engine thread.
    "invoker::rr_gzip",
    "invoker::rr_gunzip",
    // ── Commit path (cairn-store `writer`, cairn-server `lease`).
    "writer::commit::before_write",
    "writer::commit::before_durable",
    // A batch **with outbox rows** has been submitted to SlateDB (it will be applied), the
    // handle not yet stored: `return` ⇒ `commit` returns an error as if the future had
    // been cancelled after submission — `latest` does not take that write.
    "writer::commit::after_submit",
    "writer::commit::after_durable",
    "writer::seal::before_flush",
    // Send-ticket barrier LF-3: the start of `PartitionWriter::send_barrier` — tests count
    // (callback).
    "writer::barrier",
    "lease::sync::wait",
    "lease::released::io",
    // ── Session and range owner.
    "session::renew::before_put",
    "session::renew::after_put",
    "session::claim",
    // Right after the owner-record CAS wins in `claim_with`, before the check "the session
    // was lost during the write" — tests stop here.
    "session::claim::after_write",
    "session::retire::before_write",
    // Solo node: right before the CAS that answers a challenge (renewal loop), and right
    // before the challenge CAS (tracker).
    "session::before_answer_challenge",
    "session::before_challenge_cas",
    // Delegated renewal: right before the CAS that falls back to `direct` and the CAS that
    // enters the mode / delegates again (the node's renewal loop).
    "session::before_fallback_cas",
    "session::before_delegate_cas",
    // Coordinator: right before the `expired` CAS by its rule (tracker), and between the
    // batch barrier and the reply (`control.rs`). Tripwire of ack rule 5:
    // `coordinator::ack_after_expire` sits right on the branch "about to ack a session it
    // has already issued an `expired` CAS for" — tests enable `panic`.
    "coordinator::before_expire",
    "coordinator::before_ack",
    "coordinator::ack_after_expire",
    "owner::write",
    // ── Range table.
    "cluster::update::before_cas",
    // Between the winning table CAS and the write of `cluster-history/<version>`:
    // `return` ⇒ the history lacks that version, like a crash in this gap.
    "cluster::history::before_write",
    // ── Split/merge: boundaries after each phase (`clock.lap`), plus the roll-back step.
    "split::precondition",
    "split::intentCas",
    "split::flush",
    "split::quiesce",
    "split::cloneLeft",
    "split::cloneRight",
    "split::ownerClaim",
    "split::commitCas",
    "split::open",
    "split::retire",
    "merge::precondition",
    "merge::intentCas",
    "merge::gather",
    "merge::flush",
    "merge::quiesce",
    "merge::union",
    "merge::ownerClaim",
    "merge::commitCas",
    "merge::open",
    "merge::retire",
    "split::roll_back",
    // The gap between the CAS that drops the intent and the roll-back's removal of the
    // close mark: tests stop here so a later intent closes the same source again.
    "split::roll_back::before_reopen",
    // The same gap in `cairn-ctl intent clear`: between the CAS that drops the intent and
    // the removal of the close mark.
    "ctl::intent_clear::before_reopen",
    // `cairn-ctl backup`: after the first object, before everything is copied — a partial
    // copy has no backup manifest.
    "ctl::backup::mid_copy",
    // Check (i) of `cairn-ctl verify`: during the scan, at the boundary from the hot area
    // (metadata, status) to the query index area. No early exit; tests stop here so the
    // cluster writes in between the two areas.
    "ctl::verify::between_query_scans",
    // `cairn-ctl backup` after the checkpoint of **one** range, before the next range:
    // tests slip a step of a cross-range call into the gap to build a copy that is
    // inconsistent across ranges.
    "ctl::backup::after_range",
    // `cairn-ctl backup`: renewing the GC fence on the source — tests make every renewal
    // fail so the fence lapses while the copy runs.
    "ctl::backup::renew_fence",
    // `cairn-ctl fork`: after the clone of one range, before the next range — tests split
    // a range in the gap so the table changes mid-run.
    "ctl::fork::after_range",
    // Lineage loop (`lineage.rs::detach_pending`): the list of led ranges has been read,
    // right before `detach_now` of one range — tests slip a split that quiesces that range
    // into the gap.
    "lineage::before_detach",
    // The background build of the query index (the `max-live=` counter) waits, **outside**
    // the engine, without blocking a thread: `return` ⇒ the loop rests one tick and asks
    // again instead of building.
    "query::build_hold",
    // One chunk of building the query counter (`count_step`, a `ByCreated` scan), with the
    // engine held — tests count chunks (it is built only when someone asks). Callback
    // only.
    "query::count_step",
    // Hint-row sweep loop (`hint_sweep`): `now` — `return` ⇒ do not wait for
    // `HINT_SWEEP_DELAY`; `step` — every chunk, before taking the engine (callback only;
    // tests count chunks and ticks).
    "query::hint_sweep_now",
    "query::hint_sweep_step",
    // The gap between the `min_binary` CAS and the gate-enabling CAS of `cairn-ctl cluster
    // enable-call-prefix`: die here, run it again and it completes.
    "ctl::enable_call_prefix::raised",
    // The same gap in `cairn-ctl cluster enable-streams`.
    "ctl::enable_streams::raised",
    // The same gap in `cairn-ctl cluster enable-schedules`.
    "ctl::enable_schedules::raised",
    // The same gap in `cairn-ctl cluster enable-key-roles`.
    "ctl::enable_key_roles::raised",
    // The same gap in `cairn-ctl cluster enable-tenant-state`.
    "ctl::enable_tenant_state::raised",
    // `POST /internal/registry/apply` cannot re-read `tenants`: the ACK of a tenant state
    // change must come out as `202`, not a false `204`.
    "ingress::registry_apply::tenants",
    // `DELETE /deployments/{id}`: the catalog has checked "no longer the current
    // revision", the deployment object is not yet deleted — the gap that
    // `PUT /services/{s}/revision` slips into.
    "ingress::delete_deployment::before_remove",
    // Between the GET and the apply of a re-read of a cluster-level registry into RAM (the
    // same bug class as a node acting on a stale tenant state): tests stop an **older**
    // read here so a newer read (an apply request) goes through first. No early exit.
    "tenants::refresh::loaded",
    "auth::keys::refresh::loaded",
    // A starting node has opened its session and read the registries (`auth/keys`,
    // `tenants`) but does not serve HTTP yet (the window in which a booting node could
    // miss a tenant state change). No early exit.
    "node::start::after_registry_read",
    // The same gap in `cairn-ctl cluster enable-audit`.
    "ctl::enable_audit::raised",
    // The same gap in `cairn-ctl cluster enable-tenant-purge`.
    "ctl::enable_tenant_purge::raised",
    // The same gap in `cairn-ctl cluster enable-values`.
    "ctl::enable_values::raised",
    // Tenant purge: tripwire — a row carrying another tenant slipped into the engine's
    // row-delete batch.
    "tenant_purge::foreign_row",
    // The runner's purge loop: after holding a prefix in place (phase A), after the CAS of
    // the `purging-ms=` mark (phase B starts), between two row-delete batches of a range,
    // before deleting the objects of a prefix.
    "tenant_purge::after_hold",
    "tenant_purge::after_purging_mark",
    "tenant_purge::between_batches",
    "tenant_purge::before_prefix_delete",
    // Right before the final check of the `purged` tombstone: every range has reported
    // clean in this round, the prefix is empty. Tests split/merge ranges in this gap.
    "tenant_purge::before_final_check",
    // The "range clean" proof of one range has just become durable, later ranges not yet
    // checked; and every range has a proof, the progress entry is not yet deleted, the
    // tombstone not yet written.
    "tenant_purge::after_range_proof",
    // A range's full compaction is done, the `compacted` proof is about to be written
    // (another runner dirties the range again in this gap).
    "tenant_purge::before_range_proof",
    "tenant_purge::before_progress_delete",
    // A range's audit row export loop: before writing the object (rows still intact),
    // after writing but before deleting the rows (a later leader writes the same object
    // again — it matches — then deletes).
    "audit::before_export",
    "audit::after_export",
    // The two gaps around a schedule fire commit: before ⇒ nothing is durable yet, it fires
    // again at the same mark; after ⇒ both the run and the moved schedule row are durable.
    "turn::schedule::before_fire",
    "turn::schedule::after_fire",
    // Tripwire (CG-12): writing a live schedule with no next fire time — that schedule dies
    // silently. Tests enable `panic` here.
    "schedule::live_without_next",
    // The gap "artifact object PUT, row not yet committed" (AP-1): die here to leave an
    // orphan for the sweep loop.
    "artifact::after_put",
    // Tripwire (AGENTS §6.4): the sweep loop sees a hold-in-place entry whose incarnation
    // still has rows — the hold was wrong (AP-8).
    "artifact::delete_live",
    // Tripwire of the values sweep loop: a hold-in-place entry for a segment that still has
    // a holder, or that cannot be proven not to.
    "value::delete_live",
    // The shell's gap "segment PUT, holder row not yet committed" (AP-1): die here to leave
    // an orphan segment for the sweep loop.
    "value::after_put",
    // The gap "COPY PUT under the recipient owner, completion not yet committed" when
    // delivering a segmented output (AP-1): die here to leave an orphan for the sweep
    // loop; the source row stays, the next delivery COPYs again.
    "value::copy_after_put",
    // Tripwire (AGENTS §6.4): the engine meets a segmented value where it needs the whole
    // bytes (promise/signal completion, output delivered directly under the engine, promise
    // record). The engine never assembles: the correct path goes through the shell
    // (`turn/values.rs`, `value_output.rs`). Tests enable `panic`.
    "value::engine_needs_bytes",
    // The gap between quiesce and the close mark on the source Db (found by M1): tests stop
    // here so the source is reopened before it is closed.
    "split::before_close",
    "merge::before_close",
    // Tripwire of SM-12: a split/merge roll-back is about to delete a checkpoint that a Db
    // in the lineage still mentions. Tests enable `panic`.
    "split::discard::referenced_checkpoint",
    // Deleting a checkpoint of an orphan child on a broken ancestor: `return` replaces the
    // result of the delete with an error.
    "split::discard::delete_checkpoint",
    // Intent reconciliation loop (SM-15): `held` — the loop sees a coordinator still
    // holding the intent (tests count with a callback); `roll_back` — tripwire right on the
    // branch that rolls back an intent nobody has held for two rounds.
    "split::reconcile::held",
    "split::reconcile::roll_back",
    // ── Outbox.
    "turn::drain_outbox::before_send",
    // The drainer has gathered the rows to send in this round, right before waiting for the
    // durable ticket: the engine is held — count only (callback), do not stop.
    "turn::drain_outbox::before_sync",
    // Tripwire IR-1 (LF-1): right on the branch "the drainer has a send ticket while a row
    // about to be sent is not durable" — re-reading the row at the durable level (test/
    // debug builds) or `durable_seq` below the mark at gathering time (every build). Tests
    // enable `panic`.
    "turn::outbox::undurable_send",
    "transport::send_once",
    // Cross-range batch: the start of `Transport::send_batch`, the batch gathered and its
    // destinations pinned, not yet routed — tests stop here so a split slips in. Stop/count
    // only (no `return`).
    "transport::send_batch",
    // The receiver stages each message of the batch into a commit
    // (`Engine::accept_messages`): `return` ⇒ fails mid-batch, before the commit.
    "engine::accept_messages::stage",
    // Delivering `CallInvocationId` to the caller (`deliver_to_caller`) fails: the callee
    // exists while the `Call` row stays. Error `return` only.
    "turn::deliver_to_caller::call_id",
    // ── Shared WAL.
    "nodewal::stripe::cut",
    "nodewal::stripe::put",
    "nodewal::stripe::ack",
    // ── Leadership and retry.
    "actor::step_down",
    "actor::recover",
    // `recover`'s scan of unfinished invocations fails (`return` ⇒ I/O error): if the scan
    // fails, nobody scans again.
    "actor::recover::scan",
    // `restore_flow` fails (`return` ⇒ I/O error) — the range must step down rather than
    // serve before the VO locks are rebuilt (LS-9).
    "actor::restore_flow",
    // Chunked rebuild of the VO locks waits **outside** the engine before each chunk,
    // without blocking a thread: `return` ⇒ the next chunk has not run, the range still
    // serves commands — tests slip commands in between two chunks.
    "actor::restore_hold",
    // The rescue net reads the status of an id marked as failed — it must keep the mark and
    // back off one more step, not drop it (FL-16). Error `return` only.
    "actor::rescue_round::status",
    // An invocation list scan (`ListInvocations`) is about to run, with the engine held —
    // tests count the scans that actually run (a command whose asker has gone away must not
    // be scanned). Callback only.
    "actor::list::scan",
    // One chunk of an invocation list scan, with the engine held — tests count how many
    // times a scan takes the engine (it releases between chunks). Callback only.
    "actor::list::chunk",
    "turn::fire_retries",
    // Un-sticking pulls wake-up rows back: `step` on each pull tick that has rows, `row` on
    // each row fired — tests count how many rows one tick holds the engine for. Callback
    // only.
    "turn::wake_lifted::step",
    "turn::wake_lifted::row",
    // ── Error paths of a turn: the turn fails before reaching the reschedule point, or the
    // reschedule itself fails. All three points only `return` an I/O error (no stop), so
    // they can be placed under the engine.
    "turn::ensure_queued",
    "turn::drive_one",
    "turn::schedule_after_error",
    // The remaining `?`s before the reschedule point (FL-16): checking the durable status
    // before the turn, reading `journal_len` to schedule a retry, forcing durability in
    // `drive_client` when it has to wait for a permit. Error `return` only.
    "turn::gate_turn",
    "turn::retry_if",
    "turn::client_sync",
    // Admin cancel: the commit that removes from the queue after a failed terminal commit —
    // error `return` only.
    "turn::release_queue_slot",
    // The drainer reads an inbound row of kind `Completion` as garbage bytes: the row does
    // not decode. Branch only (`fires`), no early exit.
    "turn::drain_inbound::corrupt",
    // The L0 throttle defers every incoming `StartCall` from the start of the drain round,
    // as if L0 were at the edge. Branch only (`fires`).
    "l0::defer_inbound",
    // The turn exits with `?` right after committing `Running`, without rescheduling and
    // without marking the net: the shape of a newly forgotten error path.
    "turn::after_running",
    // An admin cancel slips in between the SDK's `ContinueAsNew` and the turn's final
    // commit: through the server it cannot be reached today — cancel holds the invocation
    // lock — so tests build it here. Branch only (`fires`), no early exit.
    "turn::continue_as_new::cancelled_mid_turn",
    // Right before a call out to the tenant's endpoint that opens a new SDK turn — sending
    // a `REQUEST_RESPONSE` turn, opening a BIDI stream, sending the tail when resuming in
    // place —, with the engine released, after every wait for durability and for the
    // engine of the turn: tests stop here to change the tenant state in the gap. No early
    // exit.
    "turn::sdk::before_call",
    // ── Migration framework.
    "migrate::cluster::locked",
    "migrate::cluster::after_step",
    "migrate::range_db::after_step",
    // Step `wal/shared`: the original WAL has been flushed into SSTs, the `wal_name` mark
    // not yet changed; the mark changed, the owner record not yet.
    "migrate::wal_shared::drained",
    "migrate::wal_shared::marked",
    "migrate::range::after_step",
    "migrate::range::before_mark",
    // ── GC (D-d).
    "gc::between_check_and_delete",
    // The D-d plan after the table snapshot, before reading tombstones.
    "gc::plan::after_snapshot",
    // Trash: `before_delete` — the gap "copied into the trash, the original not yet
    // deleted"; `still_needed` — tripwire: the purge loop sees an expired entry that is
    // still needed (the delete of the original was wrong). Tests enable `panic`.
    "trash::move::before_delete",
    "trash::purge::still_needed",
    // Fault injection for chaos: when enabled, the D-d plan **skips** the "still
    // referenced" gate — a deliberately broken GC, to prove that `cairn-ctl verify` catches
    // a Db that is still referenced being deleted. Never enable outside that proof run.
    "gc::skip_reference_check",
    // As above, for D-d's second barrier: skip the gate "the Db's own manifest still has a
    // named final checkpoint". The `verify` proof run enables both — with
    // `skip_reference_check` alone, the checkpoint barrier holds.
    "gc::skip_checkpoint_check",
    // Tripwire of the checkpoint barrier: the scan says no live range stands on the retired
    // Db while a **live** Db still has a named final checkpoint on it — the two proofs
    // disagree (G6: detach removes the checkpoint before dropping the entry). Negative tests
    // enable `panic`; in the proof run with `skip_reference_check`, reaching here is
    // correct.
    "gc::retired_db::proofs_disagree",
    // Tripwire of the tombstone-by-proxy loop (SM-11): right on the branch "about to CAS the
    // tombstone". Negative tests enable `panic` — reaching here with a range not yet proven
    // is a bug.
    "gc::retire_orphan::before_cas",
    // Tripwire of the orphan-child discard loop (SM-14): about to discard the Db of an
    // off-table range that a live range stands on or that has no owner record. Tests enable
    // `panic` for the child of an in-flight split/merge.
    "gc::discard_orphan::before_discard",
    // The orphan-child discard loop (SM-14 (3)) has finished checking proofs, references
    // and barriers, and has not yet re-read the table: tests stop here to change the table
    // version (mutation testing found that no test observed the version recheck).
    "gc::discard_orphan::before_recheck",
    // Tripwire (SM-14): an off-table range with a "never entered the table" proof while a
    // live range still stands on it — the loop keeps it. On a bucket created after the
    // range-layer migration framework this must never be reached; tests enable `panic`.
    "gc::discard_orphan::referenced",
    // ── Slowing the store at run time ([`DelayStore`], chaos `--slow-node`,
    // `--l0-pressure`): `return(<ms>[:<string>])`.
    "node::put_delay",
    "compactor::io_delay",
    // ── Engine open. Tripwire of SM-13: about to open the Db of a range that this node's
    // split/merge coordinator has quiesced and not finished. Tests enable `panic` or count
    // with a callback.
    "node::open::quiesced_source",
    // Test knob: `return` ⇒ opening the Db skips the old barriers (SM-13, fresh table),
    // leaving only the close mark on the Db itself.
    "node::open::bypass_old_fences",
    // Opening the Db meets a close mark on the Db itself (SM-17). Tests count with a
    // callback.
    "node::open::closed_db",
    // Opening the Db **hangs**: `return` (every range) or `return(<range id>)` ⇒ SlateDB's
    // `build`, or the flush of replayed rows in `acquire_shared`, waits forever — the shape
    // of SlateDB retrying forever when the store is broken for a long time. Tests check the
    // bound of the Db open path ([`stall_if`]).
    "store::open_db::stall",
    "nodewal::acquire::flush_stall",
    // ── The node's refresh loop (deployment, key, tenant, snapshot registries).
    "node::refresh_loop::before_tick",
    // ── Cluster-level registries: the registry write request has finished its main write
    // but not yet sealed the `control/registry-gen` generation. `return` ⇒ "dies" in this
    // gap — no seal, no peer notification.
    "registry::between_write_and_seal",
];

#[cfg(feature = "failpoints")]
mod delay;
#[cfg(feature = "failpoints")]
pub use delay::{DelayOps, DelayStore};

/// Whether this build generates failpoint code (feature `failpoints`).
pub const fn enabled() -> bool {
    cfg!(feature = "failpoints")
}

/// The error a failpoint `return(<arg>)` returns through [`failpoint!`]`(name, err)`.
#[doc(hidden)]
pub fn error(name: &str, arg: Option<String>) -> crate::StoreError {
    crate::StoreError::Io(match arg {
        Some(a) if !a.is_empty() => format!("failpoint {name}: {a}"),
        _ => format!("failpoint {name}"),
    })
}

/// Asks point `name` **without** `return`ing from the function: `Some(arg)` if it is
/// configured `return(arg)` and has just fired. For places that need to inject an error
/// into one attempt (a retry loop) instead of the whole function. With the feature off,
/// always `None`.
#[doc(hidden)]
#[inline(always)]
pub fn hit(name: &str) -> Option<Option<String>> {
    #[cfg(feature = "failpoints")]
    {
        fail::eval(name, |arg| arg)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        None
    }
}

/// Hangs (waits forever, cancellable) if point `name` fires for range `range`: `return`
/// for every range, `return(<id>)` for exactly that range. For testing the bound on a
/// SlateDB operation that retries forever. With the feature off, does nothing.
#[doc(hidden)]
pub async fn stall_if(name: &str, range: u32) {
    if !node_allows(name) {
        return;
    }
    if let Some(arg) = hit(name) {
        if arg
            .as_deref()
            .is_none_or(|a| a.is_empty() || a == range.to_string())
        {
            std::future::pending::<()>().await;
        }
    }
}

/// Whether point `name` is enabled and has just fired (node limits included) — for places
/// that only need to branch, without an argument or an early exit. With the feature off,
/// always `false`.
#[doc(hidden)]
#[inline(always)]
pub fn fires(name: &str) -> bool {
    node_allows(name) && hit(name).is_some()
}

// ───────────────────────────────────────── node conditions
//
// The test harness runs **several nodes in one process**, while `fail`'s registry is
// global: a point enabled without a condition acts on every node. Two pieces allow
// limiting it:
//
// - **Current node**: task-local [`in_node`] (the node's ingress/internal gate, the
//   session loop) or thread-local [`set_thread_node`] (engine threads — each node has its
//   own thread pool). [`current_node`] reads the task-local first.
// - **Per-point limit**: [`only_on`] — point `name` only fires when the current node is in
//   the given set. Every form of [`failpoint!`] checks it, so no placement site has to
//   change. A limited point does **not** fire when the code runs outside every node (no
//   task-local, no thread-local).
//
// Not covered yet: the shared WAL's background tasks (`nodewal`) and the node's other
// `tokio::spawn`s — they do not know their node. If needed, wrap the spawn site in
// [`in_node`].
//
// With the feature off, every function below is a cheap no-op (`current_node` = `None`,
// `node_allows` = `true`).

#[cfg(feature = "failpoints")]
tokio::task_local! {
    static NODE: std::sync::Arc<str>;
}

#[cfg(feature = "failpoints")]
thread_local! {
    static THREAD_NODE: std::cell::RefCell<Option<std::sync::Arc<str>>> =
        const { std::cell::RefCell::new(None) };
}

/// The node running this code, if someone has set it (see the start of this section).
pub fn current_node() -> Option<std::sync::Arc<str>> {
    #[cfg(feature = "failpoints")]
    {
        NODE.try_with(|n| n.clone())
            .ok()
            .or_else(|| THREAD_NODE.with(|n| n.borrow().clone()))
    }
    #[cfg(not(feature = "failpoints"))]
    {
        None
    }
}

/// Runs `fut` with `node` as the current node (task-local; child tasks spawned from inside
/// it do **not** inherit it).
pub async fn in_node<F: std::future::Future>(node: &str, fut: F) -> F::Output {
    #[cfg(feature = "failpoints")]
    {
        NODE.scope(std::sync::Arc::from(node), fut).await
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = node;
        fut.await
    }
}

/// Sets the current node for **this thread** — for threads that serve only one node (the
/// engine threads of `EnginePool`).
pub fn set_thread_node(node: &str) {
    #[cfg(feature = "failpoints")]
    THREAD_NODE.with(|n| *n.borrow_mut() = Some(std::sync::Arc::from(node)));
    #[cfg(not(feature = "failpoints"))]
    let _ = node;
}

#[cfg(feature = "failpoints")]
static ONLY: std::sync::Mutex<Vec<(u64, String, Vec<String>)>> = std::sync::Mutex::new(Vec::new());
#[cfg(feature = "failpoints")]
static ONLY_ANY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(feature = "failpoints")]
static ONLY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether point `name` may fire on the current node. Nobody limits `name` ⇒ `true`.
/// Several limits on the same point ⇒ the union of their sets.
#[doc(hidden)]
#[inline]
pub fn node_allows(name: &str) -> bool {
    #[cfg(feature = "failpoints")]
    {
        use std::sync::atomic::Ordering;
        if !ONLY_ANY.load(Ordering::Acquire) {
            return true;
        }
        let only = ONLY.lock().unwrap_or_else(|e| e.into_inner());
        let mut sets = only.iter().filter(|(_, n, _)| n == name).peekable();
        if sets.peek().is_none() {
            return true;
        }
        let Some(me) = current_node() else {
            return false;
        };
        sets.any(|(_, _, nodes)| nodes.iter().any(|n| **n == *me))
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        true
    }
}

/// Limits point `name` to `nodes` until the guard is dropped.
#[cfg(feature = "failpoints")]
#[must_use = "the limit is removed as soon as the guard is dropped"]
pub fn only_on(name: &str, nodes: &[&str]) -> NodeOnly {
    use std::sync::atomic::Ordering;
    let id = ONLY_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut only = ONLY.lock().unwrap_or_else(|e| e.into_inner());
    only.push((
        id,
        name.to_string(),
        nodes.iter().map(|n| n.to_string()).collect(),
    ));
    ONLY_ANY.store(true, Ordering::Release);
    NodeOnly(id)
}

/// Removes every node limit — for the start of a test scenario (an earlier test may have
/// panicked midway).
#[cfg(feature = "failpoints")]
pub fn clear_node_limits() {
    let mut only = ONLY.lock().unwrap_or_else(|e| e.into_inner());
    only.clear();
    ONLY_ANY.store(false, std::sync::atomic::Ordering::Release);
}

/// Guard of [`only_on`].
#[cfg(feature = "failpoints")]
#[derive(Debug)]
pub struct NodeOnly(u64);

#[cfg(feature = "failpoints")]
impl Drop for NodeOnly {
    fn drop(&mut self) {
        let mut only = ONLY.lock().unwrap_or_else(|e| e.into_inner());
        only.retain(|(id, _, _)| *id != self.0);
        ONLY_ANY.store(!only.is_empty(), std::sync::atomic::Ordering::Release);
    }
}

/// Places a named failpoint. Five forms:
///
/// - `failpoint!(name)` — stop (`pause`, a `StepController` callback), sleep, panic;
///   `return` panics (the point has no early exit).
/// - `failpoint!(name, err)` — as above, plus `return(<arg>)` ⇒ the function returns
///   `Err(failpoint::error(name, arg).into())`: the function's error type must accept
///   `From<StoreError>`.
/// - `failpoint!(name, |arg: Option<String>| <return value>)` — an arbitrary early
///   exit.
/// - `failpoint!(name, cond, err)` and `failpoint!(name, cond, |arg| …)` — like the two
///   forms above, but the point is only evaluated when `cond` is true (inspired by TiKV's
///   `fsm/apply.rs`). `cond` is only evaluated when the feature is on.
///
/// Every form also checks the point's node limit ([`only_on`]): a limited point only fires
/// on nodes in the set.
///
/// With the `failpoints` feature off, all five disappear (including the `name` and `cond`
/// expressions).
#[cfg(feature = "failpoints")]
#[macro_export]
macro_rules! failpoint {
    ($name:expr, $cond:expr, err) => {{
        let __fp_name: &str = $name;
        if $crate::failpoint::node_allows(__fp_name) && $cond {
            if let ::std::option::Option::Some(res) = $crate::__fail::eval(
                __fp_name,
                |arg: ::std::option::Option<::std::string::String>| {
                    ::std::result::Result::Err($crate::failpoint::error(__fp_name, arg).into())
                },
            ) {
                return res;
            }
        }
    }};
    ($name:expr, $cond:expr, $e:expr) => {{
        let __fp_name: &str = $name;
        if $crate::failpoint::node_allows(__fp_name) && $cond {
            if let ::std::option::Option::Some(res) = $crate::__fail::eval(__fp_name, $e) {
                return res;
            }
        }
    }};
    ($name:expr, err) => {
        $crate::failpoint!($name, true, err)
    };
    ($name:expr, $e:expr) => {
        $crate::failpoint!($name, true, $e)
    };
    ($name:expr) => {{
        let __fp_name: &str = $name;
        if $crate::failpoint::node_allows(__fp_name) {
            $crate::__fail::eval(__fp_name, |_| {
                panic!(
                    "failpoint {} has no early exit: `return` cannot be used here",
                    __fp_name
                )
            });
        }
    }};
}

/// The disabled version of [`failpoint!`]: generates nothing.
#[cfg(not(feature = "failpoints"))]
#[macro_export]
macro_rules! failpoint {
    ($($t:tt)*) => {{}};
}

#[cfg(test)]
mod tests {
    #[test]
    fn names_unique_and_well_formed() {
        let mut seen = crate::entropy::HashSet::default();
        for n in super::NAMES {
            assert!(seen.insert(n), "duplicate name: {n}");
            assert!(
                n.contains("::"),
                "name must have the form <module>::<point>: {n}"
            );
        }
    }
}
