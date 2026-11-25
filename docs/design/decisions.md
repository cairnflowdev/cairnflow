# Decision and invariant registry

Cairnflow's code, tests and guides cite short codes — `D11`, `INV-5`, `SM-9`, `NW-2`, `G18` — for
the decisions and invariants that keep a diskless engine correct. This file is the public registry
of every code that is still in use: what it states, why, and which tests prove it. When a comment
says "(SM-9)", this is where it points.

## Format

Each entry is one level-3 heading of the form `### <CODE> — <title>`, followed by four labelled
paragraphs:

```
**Statement.** The rule as the code enforces it today.
**Why.** The reason for the rule.
**Proven by.** `path/to/file.rs::test_name`, … (tests that fail if the rule breaks)
**See.** Where the subsystem is documented.
```

- `<CODE>` is the text between `### ` and the first ` — ` (space, em dash U+2014, space). It
  matches `[A-Z][A-Za-z0-9′-]*` — for example `D27`, `INV-5`, `LS-16a`, `C3′`, `D-d`, `S3-2`.
  A heading whose code is a single letter (`### X — …`) registers a whole family of inventory rows
  that live in another document.
- Library-semantics entries add **Relied on by.** before **Proven by.**
- Tests are named `file::function` (for unit tests inside a crate, `file::tests::function`); a
  doctest or a model matrix row is named in words. Some test names are not yet in English; they
  will be renamed together with this file.
- A code is never reused for a different meaning. Superseded entries stay, marked as such.
- New codes are added here in the same change that first cites them. Prefer naming a rule in prose
  over inventing a code; a code earns its place when several files must point at the same rule.

## Families

| Family | Meaning | Defined in |
|---|---|---|
| `D<n>` | Core design decisions | [here](#d--core-design-decisions) |
| `INV-<n>` | Top-level safety invariants | [here](#inv--top-level-safety-invariants) |
| `N3` | Design principle: refuse immediately | [here](#n--design-principles) |
| `M1`–`M4` | Protocol models in `crates/cairn-model` | [here](#m--protocol-models), [`docs/testing.md`](../testing.md) |
| `LS-<n>` | Leases, sessions, range leadership | [here](#ls--leases-sessions-and-range-leadership), [`docs/guides/lease-session.md`](../guides/lease-session.md) |
| `NW-<n>`, `C<n>` | Per-node shared WAL | [here](#nw--the-per-node-shared-wal), [`docs/guides/nodewal.md`](../guides/nodewal.md) |
| `SM-<n>` | Split/merge, range table, retired-DB GC | [here](#sm--splitmerge-the-range-table-and-retired-db-gc), [`docs/guides/split-merge.md`](../guides/split-merge.md) |
| `D-a`…`D-j`, `G-a`…`G-c` | Range-partition decisions and GC hazards | [here](#d-x--range-partition-design-decisions) |
| `FL-<n>`, `LF-<n>`, `IR-<n>`, `AP-<n>`, `CG-<n>`, `AU-1`, `D-O1`, `D-O2`, `D-F1`, `D-F4`, `D-W5` | Flow control, lifecycle, inter-range messaging, agent primitives, schedules, audit, outbox ordering | [here](#fl--flow-control-scheduling-and-invocation-lifecycle), [`docs/guides/flow.md`](../guides/flow.md) |
| `Q-<n>` | Query layer | [here](#q--query-layer), [`docs/guides/query.md`](../guides/query.md) |
| `G<n>`, `DC<n>`, `OS<n>`, `S3-<n>` | Assumptions about SlateDB, the disk cache, the object_store client and S3 | [here](#g--slatedb-and-object-store-assumptions), [`docs/reliability/irreversible.md`](../reliability/irreversible.md) §0.3 |
| `F<n>` | Patches in the SlateDB fork | [here](#f--patches-in-the-slatedb-fork) |
| `RD<n>` | Historical regressions pinned by the models | [here](#rd--historical-regressions-pinned-by-the-models) |
| `I4` | Follower stale-tail rule | [here](#i4--a-follower-may-lack-the-unexposed-tail-of-a-quiet-log) |
| `X`, `L`, `S`, `M`, `O`, `A`, `R`, `N` rows | Inventory of irreversible actions | [here](#inventory-rows-of-irreversiblemd-x-l-s-m-o-a-r-n), [`docs/reliability/irreversible.md`](../reliability/irreversible.md) §0.1 |

## D — core design decisions

The numbered decisions below fix the shape of the system: what state lives where, who may write it,
and what a client acknowledgement means. Numbers are stable and never reused; gaps are decisions that
no longer appear in the code or were folded into others. D1–D6 of the original list ("SlateDB, not
RocksDB", "no Raft", "redo, not command replication", "no Postgres", "own protocol", "keep Restate's
command numbers where the meaning matches") are architecture background described in
[`docs/architecture.md`](../architecture.md); they are not registered here because the code uses the
labels `D1`–`D6` for something else today (see [Collisions](#collisions-to-resolve-during-translation)).

### D7 — Two store tiers: WAL and SST

**Statement.** A node takes two object-store locations: `--main-store` (SSTs, manifests, owner records, the range table and every other cairn object) and an optional `--wal-store` that holds only WAL objects. Without `--wal-store` both tiers share the main store under different prefixes.
**Why.** The two workloads are opposite (many small appends versus few large immutable files), and a deployment may want a low-latency store for the WAL only. Both tiers must honour conditional PUT (D12).
**Proven by.** `crates/cairn-store/tests/store.rs::wal_and_sst_live_in_different_stores`
**See.** `docs/architecture.md` §2.

### D9 — Protocol before distribution

**Statement.** The wire protocol between the server and SDKs is defined in its own crate (`cairn-protocol`), which knows nothing about runtime, storage or HTTP, and changes to it are pinned by recorded conformance traces.
**Why.** A protocol cannot be fixed after SDKs ship; storage and clustering can.
**Proven by.** `crates/cairn-runtime/tests/conformance_gen.rs::traces_match_runtime`
**See.** `docs/protocol-v1.md`, `conformance/README.md`.

### D10 — Single writer enforced by the type system

**Statement.** A range's `Engine` is `!Send` and lives on exactly one thread; the only handle to write a range's database is the `PartitionWriter` that engine owns. Code that tries to use a writer from elsewhere does not compile.
**Why.** The store is shared between nodes and there is no replica to absorb a mistake; a second writer corrupts the only copy (INV-4).
**Proven by.** the `compile_fail` doctests on `PartitionWriter` (not `Clone`, not `Send`) in `crates/cairn-store/src/writer/mod.rs`; `crates/cairn-store/tests/store.rs::old_writer_fenced_when_new_writer_starts`
**See.** INV-4, LS-4.

### D11 — Acknowledge only after the write is durable

**Statement.** A client, an SDK or a peer range is told that something happened only after the write that records it is durable (`await_durable`, carried as a `Committed` ticket), never after `put`. With the shared WAL "durable" means the WAL object holding the frame was PUT successfully and every earlier frame of the stream is acknowledged (NW-1).
**Why.** A fenced zombie writer still sees `put` succeed; only the durability wait reports the fence. Acknowledging after `put` acknowledges writes that will never exist.
**Proven by.** `crates/cairn-server/tests/durability.rs::reply_waits_for_durability`, `crates/cairn-store/tests/shared_wal.rs::shared_wal_ack_only_after_put`, `crates/cairn-server/tests/failpoints.rs::remote_peer_never_sees_a_row_before_it_is_durable`
**See.** INV-5, FL-2, NW-1.

### D12 — Stores without conditional PUT are refused, on both tiers

**Statement.** At start-up each store tier is probed: a store that does not honour `PutMode::Create` (and, for a cluster, `PutMode::Update` on an etag) is refused with a clear error before anything is written. The CAS probe is separate from the create probe, so a single-process node on `file://` (create but no update) still starts.
**Why.** The real fence is the conditional PUT, not `writer_epoch`; a store that silently ignores the condition would let two writers win.
**Proven by.** `crates/cairn-store/tests/gates.rs::conditional_put_gate_on_real_store`, `crates/cairn-store/tests/gates.rs::cas_gate_on_real_store`, `crates/cairn-store/src/config/mod.rs::tests::store_swallowing_conditions_is_rejected`, `crates/cairn-server/tests/liveness.rs::boot_still_rejects_store_without_cas_quickly`
**See.** OS2, `docs/architecture.md` §3.

### D16 — No network call while holding the engine

**Statement.** Every function that performs network I/O takes a `&Released`, which only `Lease::released` can create while the engine lock is released. Calling the network with the engine held is a compile error.
**Why.** The receiving side of an inter-range message must take its own engine; holding ours across the send lets two ranges wait on each other forever.
**Proven by.** the `compile_fail` doctest on `Released` in `crates/cairn-server/src/lease.rs`; `crates/cairn-server/tests/transport_engine.rs::transport_send_does_not_hold_engine`
**See.** FL-1, IR-8.

### D18 — One reader per range; background paths only enqueue wake-ups

**Statement.** Each range has one reader of its queues. Background paths (timers, drainers, retries) record a wake-up and hand it to the scheduler; no SDK turn runs on a background path and nobody waits on another turn's lock.
**Why.** Removes drain locks and head-of-line waits between unrelated turns.
**Proven by.** `crates/cairn-server/tests/lifecycle.rs::two_timers_on_one_tick_deliver_one_turn`, `crates/cairn-server/tests/lifecycle.rs::drainer_does_not_redeliver_delivered_turn`
**See.** FL-20, `docs/guides/flow.md`.

### D19 — Formats may break, but never silently (superseded by D27)

**Statement.** Historical rule from before the first frozen format: a bucket carries a format marker and a binary refuses a format it does not know. Since D27 the stronger rule applies; the marker check remains.
**Why.** A binary must never misread older data.
**Proven by.** `crates/cairn-store/tests/format.rs::other_format_version_refused`, `crates/cairn-store/tests/format.rs::newer_headers_stop_loudly`
**See.** D27.

### D20 — Leases are per node

**Statement.** Each node holds one session object that it renews for all of its ranges; a range's owner record (`owners/range-<id>`) is written only when ownership changes. Session expiry is explicit and permanent (LS-1).
**Why.** Ranges on a node live and die with it; per-range leases would make idle cost grow with ranges × nodes.
**Proven by.** `crates/cairn-server/tests/idle_cost.rs::idle_cluster_lease_writes_do_not_scale_with_partitions`, `crates/cairn-server/tests/idle_cost.rs::idle_cluster_reads_leases_once_per_ttl`
**See.** `docs/guides/lease-session.md`, LS-1, LS-3.

### D21 — Keyspace ordered by the owner's partition key, in three zones

**Statement.** Every row key starts with a zone byte: `0x00 ‖ owner_pk ‖ table ‖ …` for hot entity rows, `0x01 ‖ index ‖ …` for derived indexes that sit outside every range and are rebuilt, and `0x02 ‖ owner_pk ‖ …` for bulk entity data (journal, state). A range is therefore two contiguous byte ranges, and split/merge is a projected clone rather than a rewrite.
**Why.** Splitting must not copy data; derived indexes must be rebuildable from the owned rows.
**Proven by.** `crates/cairn-engine/tests/keyspace_layout.rs::projection_keeps_every_owned_row`, `crates/cairn-engine/tests/keyspace_layout.rs::recover_reads_only_derived_region`, `crates/cairn-store/src/keyspace.rs::tests::row_owner_classifies_every_kind_of_row`
**See.** `docs/key-design.md`, F4.

### D22 — Range identity

**Statement.** A `RangeId` is never reused; generations follow the usual rule (children are `gen + 1`); the intent is written to the table before any data step; the table changes only by CAS; a range's DB lives at `ranges/<id>` and its owner record at `owners/range-<id>`, which records the WAL kind; an engine serves one fixed range for its whole life.
**Why.** A reused id would alias a DB or owner record onto a different range; the intent tells whoever takes a range after a crash what to undo.
**Proven by.** `crates/cairn-store/src/cluster.rs::tests::update_enforces_no_id_reuse`, `crates/cairn-store/src/cluster.rs::tests::update_forbids_new_intent_promising_issued_id`
**See.** SM-1, SM-3.

### D23 — The `WrongRange` contract

**Statement.** Every inter-range message carries the target range and table version. A receiver whose table disagrees answers `409 wrong_range` with its own table, checked again inside the actor; the sender adopts the table if newer and resends within the same turn, never looping on the same version; ingress reloads the table and retries exactly once.
**Why.** During a table change two nodes may hold two versions; this converges without a retry storm.
**Proven by.** `crates/cairn-server/tests/wrong_range.rs::inbound_rejected_while_range_retiring`, `crates/cairn-server/tests/wrong_range.rs::stale_sender_recovers_after_table_change`, `crates/cairn-server/tests/wrong_range.rs::invoke_during_table_change_returns_result_not_503`
**See.** SM-6.

### D24 — Split and merge follow an intent protocol

**Statement.** A split or merge runs: intent CAS on the table (the decision point) → flush while still serving → quiesce (lease kept, DB really closed) → projected clone/union → claim the children in the node's session → commit CAS (sources leave the table, children appear with `gen + 1`) → open children and rebuild derived indexes → retire the sources with a tombstone. Whoever finds a range carrying an intent no live coordinator holds rolls it back or, when the op record proves it ready, rolls it forward.
**Why.** Every crash point leaves either the sources or the children authoritative, never both.
**Proven by.** `crates/cairn-server/tests/split.rs::split_crash_at_every_step`, `crates/cairn-model/tests/m2.rs::production_is_safe`
**See.** `docs/guides/split-merge.md`, SM-3, SM-19.

### D27 — The on-bucket format is frozen at `DATA_FORMAT` 10

**Statement.** `DATA_FORMAT` is 10 and is not bumped. Every later change to the bucket layout goes through the migration framework (`crates/cairn-store/src/migrate.rs`): changing an existing object is a `MIGRATIONS` step behind a cluster gate; a new loosely read object whose absence means "default" only needs an `OBJECT_POLICIES` line; strictly read objects change their header. A committed format-10 fixture bucket must keep opening.
**Why.** Users' buckets must survive upgrades; "delete the bucket" is never an answer.
**Proven by.** `crates/cairn-server/tests/format_fixture.rs::format_10_fixture_still_opens`, `crates/cairn-server/tests/format_fixture.rs::format_10_cluster_fixture_migrates`, `crates/cairn-store/tests/format.rs::format_errors_never_say_delete_the_bucket`
**See.** `docs/architecture.md` §8.

## INV — top-level safety invariants

The original design states five invariants. `T` is the highest durable WAL position, `D` the highest
position compacted into SSTs, and `D'ᵢ` the position the manifest held by node *i* covers.

### INV-1 — Compaction never runs ahead of the WAL

**Statement.** `D ≤ T`: nothing is compacted into SSTs that is not already durable in the WAL.
**Why.** Otherwise a crash could expose rows that were never durably logged.
**Proven by.** SlateDB's own write path; cairn pins the related flush ordering in `crates/cairn-store/tests/library_contracts.rs::g2_memtable_flush_returns_after_the_manifest_is_stored`.
**See.** G1, G2, NW-7.

### INV-2 — A reader overlays everything its own manifest does not cover

**Statement.** Node *i* keeps an overlay covering all of `(D'ᵢ, T]`, where `D'ᵢ` is the manifest it holds itself, not the newest `D`.
**Why.** Truncating at `D` loses data whenever the manifest poll is late.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::gc_floor_never_passes_frames_the_stored_manifest_does_not_cover`
**See.** NW-7, NW-10.

### INV-3 — All pending work lives in state, not in leader RAM

**Statement.** Timers, outbox rows, inbound messages, retries and locks are rows in the range's database; a new leader rebuilds every in-memory structure from them (`restore_flow`, recovery).
**Why.** Leadership can move at any moment; work held only in RAM would be lost.
**Proven by.** `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`, `crates/cairn-model/tests/m4.rs::core_holds_every_property`
**See.** FL-8, FL-16, LS-9.

### INV-4 — At most one valid writer per range

**Statement.** At any time at most one `PartitionWriter` can make writes durable for a range. Two independent layers enforce it: the type system (D10) against programming errors and SlateDB's `writer_epoch` plus the owner-record CAS against timing errors.
**Why.** There is no replica; two writers corrupt the only copy.
**Proven by.** `crates/cairn-store/tests/store.rs::old_writer_fenced_when_new_writer_starts`, `crates/cairn-model/tests/m1.rs::without_owner_cas_and_writer_epoch_two_writers_meet`
**See.** D10, LS-3, LS-4.

### INV-5 — Acknowledge only after `await_durable`

**Statement.** Same rule as D11, stated as an invariant: no acknowledgement leaves the node before the write it reports is durable and the writer still holds its lease.
**Why.** See D11.
**Proven by.** `crates/cairn-server/tests/durability.rs::status_does_not_reply_before_creation_is_durable`, `crates/cairn-store/tests/shared_wal.rs::shared_wal_ack_only_after_put`
**See.** D11, FL-2, LS-5.

## N — design principles

### N3 — Refuse immediately instead of accepting and filtering later

**Statement.** While a range has no leader, or its leader is stepping down, requests are answered at once with a retryable `NotLeader` (`503`) instead of being queued or accepted and cleaned up later. Silence is never allowed: a node always answers.
**Why.** Simpler and semantically correct; the cost is a retryable error that is part of the API contract.
**Proven by.** `crates/cairn-server/tests/liveness.rs::commands_answered_while_seal_hangs`, `crates/cairn-server/tests/liveness.rs::partition_steps_down_when_db_closes`
**See.** LS-7.

## M — protocol models

`crates/cairn-model` checks the distributed protocols exhaustively with `stateright`. Each model
names its actions after the real functions and turns every fence into a flag; the acceptance test
of a model is that switching a historical fix off makes the checker find a counterexample, and with
every fence on the checker explores the whole space with every invariant holding. See
[`docs/testing.md`](../testing.md).

### M1 — Leases, sessions, the range table and opening a DB

**Statement.** Models session renewal and expiry, owner-record CAS, `writer_epoch`, delegated renewal and solo renewal, the range-table read before opening a DB, and the quiesce/closed-flag fences around split and merge. Properties: at most one writer acknowledges per range, no acknowledgement lands in a retired DB, reads are never stale under a lying clock when the read fence is on.
**Why.** Lease protocols fail only in rare interleavings that tests cannot reach by chance.
**Proven by.** `crates/cairn-model/tests/m1.rs::all_fences_hold_for_split`, `crates/cairn-model/tests/m1.rs::d1_swallowed_table_read_error_acks_into_retired_parent`, `crates/cairn-model/tests/m1.rs::grant_outliving_ttl_breaks_ls3`
**See.** LS-1 … LS-16, SM-13, SM-17.

### M2 — Split, merge, GC and fork

**Statement.** Runs the real decision core `cairn_split::step` against a modelled environment (table, owner records, DBs, checkpoints, barriers) and checks that no DB is deleted while a live DB lists it, that no acknowledgement lands in a quiesced or retired source, and that no checkpoint or owner record is stranded. Every historical bug is a `Bugs` flag and a row of `crates/cairn-model/tests/m2.rs::matrix`.
**Why.** Every delete path around an operation must be safe against every crash and race.
**Proven by.** `crates/cairn-model/tests/m2.rs::production_is_safe`, `crates/cairn-model/tests/m2.rs::the_core_holds_for_split`, `crates/cairn-model/tests/m2.rs::the_core_holds_for_merge`
**See.** SM-*, D-d.

### M3 — The per-node shared WAL

**Statement.** Models sealing, range acquisition, the GC floor, dead-log sweeping and forgetting, stripe rotation and follower exposure. Property: no acknowledged frame is lost and no zombie acknowledges into a sealed log.
**Why.** The shared WAL multiplexes many ranges into one log; its fences interact.
**Proven by.** `crates/cairn-model/tests/m3.rs::all_fences_hold`, `crates/cairn-model/tests/m3.rs::matrix_one_fence_off`
**See.** NW-*, C1 … C7.

### M4 — The invocation lifecycle on the real core

**Statement.** Unlike M1–M3, the transition function is the production lifecycle core (`cairn_lifecycle::step_mutated`); the model supplies only the shell's event order and the environment (SDK, throttling, leader changes, cancellation). Property: in every quiescent state every invocation has reached an outcome or is `paused` with a reason, and attempts and permits are counted exactly.
**Why.** Lifecycle bugs were the largest class of historical defects; the model runs the code that ships.
**Proven by.** `crates/cairn-model/tests/m4.rs::core_holds_every_property`, `crates/cairn-model/tests/m4.rs::production_is_the_core`, `crates/cairn-model/tests/m4.rs::every_historical_bug_turns_m4_red`
**See.** FL-16 … FL-21.


## LS — leases, sessions and range leadership

Leadership is decided in three layers. A **session** (`sessions/<node>/<inc>`) says a node incarnation is alive and is renewed once for all of the node's ranges (D20). An **owner record** (`owners/range-<id>`) says who leads a range and is written only when ownership changes. The real write fence is SlateDB's `writer_epoch` plus the object store's conditional PUT; the two upper layers only decide who *should* lead. Delegated renewal (via a coordinator) and solo renewal (a lone node renewing by conditional GET) change how often the bucket is written, never the fencing argument. The model checker M1 (`crates/cairn-model/src/m1.rs`) pins the core rules.

### LS-1 — An ended session stays ended, and only the bucket declares a session dead

**Statement.** `expired` and `closed` are terminal; a node that loses its session opens a new incarnation and never revives the old one. An observer CASes `expired` on a `direct` record only after one TTL of silence counted from when it *received* the highest beat; `solo` records must be challenged first (LS-16) and `delegated` records are expired only by their coordinator (LS-15). `SessionTracker::is_dead` is true only after reading a terminal state or winning the `expired` CAS, never from the observer's clock alone, and an incarnation number is claimed with a `Create` marker under `incarnations/<node>/` so it is never reused, even after the clock steps back.
**Why.** Every irreversible action (claim, migration lock takeover, tombstoning on behalf of a dead owner) is gated on `is_dead`, so death must be a fact on the bucket; reusing an `inc` would reopen an old shared-WAL log name.
**Proven by.** `crates/cairn-store/src/session/tests.rs::stalled_session_expired_after_ttl_not_before`, `crates/cairn-store/src/session/tests.rs::ambiguous_open_does_not_leave_a_ghost_incarnation`, `crates/cairn-store/src/session/tests.rs::restarted_node_never_reuses_incarnation_after_session_gc`, `crates/cairn-model/tests/m1.rs::unfenced_unexpirable_judgment_breaks_ls3`
**See.** `docs/guides/lease-session.md` §1.

### LS-2 — A grant expires by the holder's clock and only a winning CAS revives it

**Statement.** A leadership grant is valid until `t + ⅔·TTL`, where `t` is when the winning renewal (or acknowledged delegated heartbeat) was *issued*. Past that it is paused: no acks and no read replies, but the range is not yet released; a later renewal that wins its CAS from the previous version revives it, while a lost CAS or passing the pause cap (last issue + TTL + TTL/3) loses it for good. Range grants are children of the session grant, renewal runs off the engine thread and never waits on side work, and a node whose monotonic clock drifts more than ±10% from the store's `Date` header steps down.
**Why.** An observer expires a session only one TTL after the last beat it received, so a grant that ends at ⅔·TTL is gone before anyone else can claim; the clock-rate check turns the "clocks run at similar rates" assumption into a checked condition.
**Proven by.** `crates/cairn-store/src/session/tests.rs::late_renewal_that_wins_cas_revives_the_session`, `crates/cairn-store/src/session/tests.rs::paused_session_is_lost_at_the_pause_cap`, `crates/cairn-store/src/session/tests.rs::hung_renewal_put_is_hedged_not_waited_out`, `crates/cairn-model/tests/m1.rs::grant_outliving_ttl_breaks_ls3`
**See.** `docs/guides/lease-session.md` §1; `crates/cairn-store/src/session/mod.rs` (module doc).

### LS-3 — Ownership changes only by CAS on the owner record

**Statement.** When two nodes claim one range, exactly one CAS on `owners/range-<id>` wins; a session never seen before is not presumed dead, and a frozen node's ranges move no sooner than the lease silence period. Re-claiming a record this session already holds rewrites it with a new term, so older keepers of the same session go stale. An ambiguous owner-record CAS is resolved by re-reading and comparing (session + term), never treated as a loss.
**Why.** The owner record is the single source of who leads; peer-to-peer networking is never a source of truth about leases.
**Proven by.** `crates/cairn-store/src/session/tests.rs::two_claimers_one_wins`, `crates/cairn-store/src/session/tests.rs::ambiguous_owner_cas_is_resolved_by_reading_back`, `crates/cairn-server/tests/cluster_lease.rs::frozen_node_partitions_move_after_ttl_not_before`, `crates/cairn-model/tests/m1.rs::without_owner_cas_two_nodes_believe_they_lead`
**See.** `docs/guides/lease-session.md` §1.

### LS-4 — The write fence is `writer_epoch`, not the term

**Statement.** Opening a range writer bumps SlateDB's `writer_epoch` and fences the previous writer, including a frozen process that does not know it lost leadership. A zombie is stopped when it waits for durability, not at `put`.
**Why.** Lease and term only say who should lead; the storage-level epoch is what physically prevents two writers on one Db.
**Proven by.** `crates/cairn-store/tests/store.rs::old_writer_fenced_when_new_writer_starts`, `crates/cairn-store/tests/store.rs::frozen_old_leader_cannot_write_after_b_starts`, `crates/cairn-model/tests/m1.rs::without_owner_cas_and_writer_epoch_two_writers_meet`
**See.** `docs/guides/lease-session.md` §1.

### LS-5 — No lease, no ticket

**Statement.** Every `Committed` ticket, including one issued in place for a read-only answer, requires a valid grant; a paused grant makes `commit` and `Lease::sync` wait until revival or loss. Every command reply at the leader also passes a read fence: a conditional GET on the incarnation's session record, issued *after* the read and batched once per node, and anything but our own `live` record yields `LeaseLost`. The early `LeaseLost` check in `commit` is an optimisation, not the safety mechanism (INV-5, D11).
**Why.** The grant trusts only the local clock; a leader whose clock stalled (suspended VM) could otherwise serve a stale read after its range moved.
**Proven by.** `crates/cairn-store/tests/durability.rs::no_ticket_after_lease_lost`, `crates/cairn-server/tests/failover.rs::leader_waking_after_losing_its_range_never_serves_a_stale_read`, `crates/cairn-store/src/session/tests.rs::read_fence_batches_and_refuses_after_expiry`, `crates/cairn-model/tests/m1.rs::lying_clock_without_read_fence_serves_a_stale_read`
**See.** `docs/guides/lease-session.md` §1.

### LS-6 — Lost leadership is detected in three layers, each of which steps the range down

**Statement.** Layer 1 is `keeper.lost()`, which fires when the grant is lost (not merely paused). Layer 2 is `Fenced`/`health.closed()` from the store plus a stalled-`durable_seq` watchdog. Layer 3 is the owner watch: a claimant's notice, the coordinator's view, or a periodic conditional GET of every held owner record (default every minute) triggers a re-read, and only a bucket read showing the record is no longer ours revokes the grant.
**Why.** Layer 3 is liveness only — writes are blocked by LS-4 and reads by LS-5 — so hints may be lost without risk, but a deposed leader must stop promptly.
**Proven by.** `crates/cairn-server/tests/cluster_lease.rs::old_owner_steps_down_at_once_when_the_claimant_tells_it`, `crates/cairn-server/tests/liveness.rs::partition_steps_down_when_db_closes`, `crates/cairn-server/tests/liveness.rs::partition_steps_down_on_durability_stall`, `crates/cairn-server/tests/cluster_lease.rs::default_owner_watch_grid_is_one_minute`
**See.** `docs/guides/lease-session.md` §1.

### LS-7 — A non-leader answers at once

**Statement.** A range actor that is not leading replies `NotLeader` immediately, even while a seal hangs, because `step_down` keeps reading the command channel. `Shutdown`, `Resign` and `Quiesce` reply only after the seal has finished.
**Why.** Clients must be redirected without waiting on storage, but a caller that asked for a hand-off must know the writer is sealed.
**Proven by.** `crates/cairn-server/tests/liveness.rs::commands_answered_while_seal_hangs`, `crates/cairn-server/tests/quiesce.rs::shutdown_and_resign_reply_after_seal`
**See.** `docs/guides/lease-session.md` §1.

### LS-8 — Quiesce keeps the lease; a retired range is never claimed again

**Statement.** Sealing a quiesced range lowers the writer's validity, not the lease's, and a split/merge coordinator's quiesce holds the lease only while that coordinator lives. A tombstone (`retired`) makes `claim` return `RangeRetired` for everyone, including its own node; if the leader died before writing it, a background pass writes it by CAS under exactly the same "owner is dead" rule as `claim` (SM-11).
**Why.** A retired range's data now lives in its children; re-opening it would accept writes that are then lost.
**Proven by.** `crates/cairn-server/tests/quiesce.rs::quiesced_range_keeps_lease_and_stays_closed`, `crates/cairn-server/tests/quiesce.rs::retired_range_is_never_reacquired`, `crates/cairn-server/tests/failpoints.rs::parent_left_without_tombstone_is_retired_after_its_leader_dies`, `crates/cairn-server/tests/failpoints.rs::coordinator_panic_after_quiesce_releases_and_rolls_back_the_source`
**See.** `docs/guides/lease-session.md` §1; `docs/guides/split-merge.md` §1.

### LS-9 — Every term starts with recovery

**Statement.** On becoming leader, `restore_flow` rebuilds virtual-object locks before the first execution slot is granted (FL-8); if it fails the range steps down. `recover` then runs as a task of the serving loop, scanning the invocation index in bounded chunks and releasing the engine between chunks; a failed `recover` scan does not step down but is retried within the term (FL-16).
**Why.** A new leader must not grant work against stale locks, yet recovery over the whole history must not block serving.
**Proven by.** `crates/cairn-server/tests/single_reader.rs::recover_does_not_block_serving`, `crates/cairn-server/tests/recovery_storm.rs::recover_scans_in_chunks_whatever_the_history`, `crates/cairn-server/tests/failpoints.rs::failed_restore_flow_steps_down_instead_of_serving`, `crates/cairn-server/tests/failpoints.rs::failed_recover_scan_is_retried_within_the_term`
**See.** `docs/guides/lease-session.md` §1; `docs/guides/flow.md` §1.

### LS-10 — The balancer decides who tries for a lease, not who writes

**Statement.** Each node contends up to its fair share ⌈ranges / live nodes⌉, counting in-flight contests, and a newly started node counts only after it settles. At most one range is released per cycle; when a stable node sits below the floor, the single node at the cap with the lowest id yields one range, backing off exponentially if the yield does not land.
**Why.** Balancing is policy layered on top of LS-3/LS-4; it must converge without ever weakening the write fence.
**Proven by.** `crates/cairn-server/tests/cluster_lease.rs::rebalance_converges_and_stays`, `crates/cairn-server/tests/cluster_lease.rs::balancer_releases_at_most_one_range_per_cycle`, `crates/cairn-server/tests/cluster_lease.rs::idle_node_gets_a_range_when_ranges_do_not_divide_evenly`, `crates/cairn-server/src/balance.rs::tests::failed_yield_doubles_backoff`
**See.** `docs/guides/lease-session.md` §1.

### LS-11 — A slow node sheds its ranges, one node at a time, never when alone or when everyone is slow

**Statement.** A slowness score is computed only from existing PUTs (session renewals, shared-WAL attempts); a node sheds after two rounds at the maximum score and a p99 PUT above twice the median of the other live nodes, and only after winning the `shedding` marker by CAS. A draining node (`draining=1` on its session) refuses ranges on every acquire path and is not counted in others' floor or cap.
**Why.** Like LS-10 this is only policy about who should try to lead; one shedder at a time prevents a uniformly slow cluster from emptying itself.
**Proven by.** `crates/cairn-server/tests/slow_shed.rs::slow_node_sheds_everything_then_takes_ranges_back`, `crates/cairn-server/tests/slow_shed.rs::uniformly_slow_cluster_never_sheds`, `crates/cairn-server/tests/slow_shed.rs::lone_node_never_sheds`, `crates/cairn-store/src/health.rs::tests::one_shedder_at_a_time`
**See.** `docs/guides/lease-session.md` §1.

### LS-12 — Opening an engine does not re-claim the lease over a transient I/O error

**Statement.** After `claim`, the open's reads (table reload, `cluster-version`, pre-existing owner record, WAL-kind marker) retry I/O errors in place with backoff and jitter while keeping the keeper, bounded (8 attempts, ≤ 10 s each, ≤ 30 s total) and stopping once the lease is lost. A table reload that exhausts its retries fails the open rather than proceeding on the in-memory table; a failed open is retried by the leadership loop with backoff while still answering commands.
**Why.** Re-claiming on every error amplifies throttling; opening by a stale table could reopen a parent that already left the table and ack writes that are later lost (SM-13).
**Proven by.** `crates/cairn-server/tests/cluster_lease.rs::engine_open_rides_out_transient_get_errors_without_reclaiming`, `crates/cairn-server/tests/cluster_lease.rs::engine_open_rides_out_transient_table_reload_errors_without_reclaiming`, `crates/cairn-server/tests/failpoints.rs::stale_table_never_reopens_a_parent_that_left_the_table`, `crates/cairn-model/tests/m1.rs::d1_swallowed_table_read_error_acks_into_retired_parent`
**See.** `docs/guides/lease-session.md` §1.

### LS-13 — A local data directory belongs to exactly one node, and its old sessions expire at once

**Statement.** A node on `file://` (`--data-dir`) takes an exclusive `flock` on the store directory at startup and refuses to start if it is held; the store is then wrapped with in-process emulated CAS. Under the lock every still-`live` session belongs to a dead run, so `expire_sessions_under_lock` CASes them `expired` before the new session is established.
**Why.** A local filesystem has no etag CAS; the OS lock (released even on `kill -9`) is what makes single-writer hold, and it lets a restart take its ranges without waiting a TTL.
**Proven by.** `crates/cairn-server/tests/local_dir.rs::local_dir_node_leads_and_restarts_without_waiting_ttl`, `crates/cairn-store/src/session/tests.rs::sessions_expire_at_once_under_the_local_lock`
**See.** `docs/guides/lease-session.md` §1.

### LS-14 — A delegated node renews by coordinator ack only after its delegation CAS is confirmed

**Statement.** On a coordinator's `Grant`, the keeper CASes `mode=delegated coord=<C>:<term>` with `beat+1`; only once that CAS is confirmed (won, or ambiguous and read back as ours) may acks extend the grant, each to `t_sent + ⅔·TTL` of the acknowledged heartbeat. After `lose` the grant is revoked permanently and no ack revives it. The node self-checks its own record by conditional GET each interval (seeing it ended or changed ⇒ `lose`) and falls back to `direct` by CAS when acks stop.
**Why.** Delegation removes per-node session PUTs, but must never let an ack extend a grant the bucket has already ended (LS-3).
**Proven by.** `crates/cairn-server/tests/delegation.rs::delegated_node_sends_no_session_put_while_acked`, `crates/cairn-server/tests/delegation.rs::node_cut_from_bucket_but_not_coordinator_loses_its_ranges`, `crates/cairn-store/src/session/tests.rs::delegation::ack_after_pause_cap_never_revives`, `crates/cairn-model/tests/m1.rs::ack_before_delegation_confirmed_breaks_ls3`
**See.** `docs/guides/lease-session.md` §1; `crates/cairn-store/src/session/delegate.rs` (module doc).

### LS-15 — The coordinator fences after receiving, never acks a session it expired, and others defer to it

**Statement.** The coordinator role is bound to one incarnation and term. Before acking a heartbeat batch it checks, by reads issued after receipt, that it still holds the role and its own session is `live`, that the sender's record is delegated to it at the reported version, and that it has never issued an `expired` CAS for that session (even an ambiguous one). It expires a delegated record only by CAS `If-Match` one TTL after the last acknowledged heartbeat; any other observer may expire it only after one TTL since it saw the role lost and one TTL since it saw that record version.
**Why.** These fences keep delegation within the same "grant ends before anyone can claim" argument as LS-2.
**Proven by.** `crates/cairn-server/tests/delegation.rs::coordinator_killed_ranges_move_within_two_ttl`, `crates/cairn-server/tests/failpoints.rs::zombie_coordinator_ack_never_extends_an_expired_grant`, `crates/cairn-store/src/session/tests.rs::delegation::delegating_to_a_lost_coordinator_is_not_expired_early`, `crates/cairn-model/tests/m1.rs::ack_after_expire_breaks_ls3`
**See.** `docs/guides/lease-session.md` §1.

### LS-16 — A lone node renews by unchallenged GET; a newcomer challenges through the bucket, then waits a TTL

**Statement.** With the quiet-lease gate on and no other unended session (or as the coordinator every peer delegates to), a node CASes `mode=solo` and, once that is confirmed, renews by a conditional GET of its own record: an unchanged record extends the grant to `t + ⅔·TTL`, a failed GET does not. A newcomer that finds a `solo` record CASes a `challenged=` line onto it and may expire it only after one TTL without a beat increase; the owner answers by CASing back to `direct`.
**Why.** An idle single node then writes nothing to the bucket, while any second process is still guaranteed to see the first node's grant end before it claims.
**Proven by.** `crates/cairn-store/src/session/tests.rs::solo::solo_node_sends_no_session_put_while_alone`, `crates/cairn-store/src/session/tests.rs::solo::restarted_incarnation_takes_over_after_challenge_and_ttl`, `crates/cairn-server/tests/cluster_lease.rs::frozen_solo_node_ranges_move_after_challenge_and_ttl`, `crates/cairn-model/tests/m1.rs::solo_extend_on_challenged_read_breaks_ls3`
**See.** `docs/guides/lease-session.md` §1.

### LS-16a — Solo and delegated renewal stay off until the quiet-lease gate is on

**Statement.** No node enters `solo` (and no coordinator grants delegation) unless `control/quiet-lease` is on; a missing or unreadable gate counts as off. A genuinely empty bucket gets the gate (with delegation) by `Create` at bootstrap, after the store passes the G18 conditional-GET probe; an existing bucket is switched only by `cairn-ctl cluster enable-quiet-lease`, which probes the store, raises `min_binary` so older binaries cannot start, waits out the boot bound and re-checks every session before writing the gate.
**Why.** An older binary applies the plain silence rule of LS-1 to a `solo` record and would expire a live owner; a store serving stale `304`s would break the solo self-check.
**Proven by.** `crates/cairn-store/tests/migrate.rs::fresh_bucket_gets_the_quiet_lease_gate_with_delegation`, `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_checks_knows_then_raises_min_binary_and_opens_the_gate`, `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_refuses_a_store_that_serves_a_stale_304`, `crates/cairn-server/tests/upgrade.rs::old_binary_cannot_start_when_quiet_lease_enabled`
**See.** `docs/guides/lease-session.md` §1.

## NW — the per-node shared WAL

Each session incarnation writes the WAL of every range it holds into `S` log streams (stripes), `nodewal/<node>/<inc>/<stripe>/<seq>`, while each Db still sees a private WAL through SlateDB's `wal` trait. A range's WAL index lives in its owner record (`wal_kind`, `wal_segments`, `wal_floor_id`). The shared WAL is the only WAL of every range, including single-node deployments. Every rule below answers a counterexample of the design model (the C claims) and is pinned by the model checker M3 (`crates/cairn-model/src/m3.rs`).

### NW-1 — Ack only after the PUT, and in stream order

**Statement.** `WalFlushed` (and therefore `await_durable`) is delivered only after the object's `Create` has succeeded. With several PUTs in flight, object `n+1` is acked only after object `n` (`FuturesOrdered`). When an object is cut is a free choice of `Stripe::run`, not part of the invariant.
**Why.** D11: a commit is acknowledged only once durable, and an out-of-order ack would let a client see `w2` while `w1` can still be lost.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::shared_wal_ack_only_after_put`, `crates/cairn-store/tests/shared_wal.rs::ack_in_order_within_stripe`, `crates/cairn-store/tests/shared_wal.rs::ack_in_order_with_two_puts_in_flight`, `crates/cairn-model/tests/m3.rs::nw1_ack_before_put_or_out_of_order_loses_an_ack`
**See.** `docs/guides/nodewal.md` §1.

### NW-2 — A log incarnation is written only by itself

**Statement.** Only the owning stream writes data objects to its log, always with `PutMode::Create`, and nothing is ever acked unless known durable. On a store declaring `atomic_put` (`s3://`, `memory://`, `file://`, or `--wal-atomic-put true`) an `AlreadyExists` on any attempt is resolved by reading the object back and comparing bytes: our bytes ⇒ ack, a seal or foreign object ⇒ `Fenced`, absent ⇒ retry; on an `atomic_put = false` store `AlreadyExists` is a fence. `Fenced` kills the stream and fail-stops the whole incarnation, whereas a PUT that does not complete within `stall_max` retires and rotates only that stream to a fresh stripe id, leaving the node alive.
**Why.** Only the stream and a sealer ever write at that name, so a byte comparison is meaningful on atomic stores; a torn object on a non-atomic store could read as ours while the sealer saw it empty.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::zombie_node_cannot_write_after_seal`, `crates/cairn-store/tests/shared_wal.rs::own_ambiguous_log_put_is_recognized_and_acked`, `crates/cairn-store/tests/shared_wal.rs::ambiguous_log_put_fails_stop_without_ack`, `crates/cairn-store/tests/shared_wal.rs::hung_put_bounds_stall_then_rotates_its_stripe`
**See.** `docs/guides/nodewal.md` §1; `crates/cairn-store/src/nodewal/stripe.rs` (module doc).

### NW-3 — Seal at the first gap above the durable GC floor, then re-read the floor

**Statement.** A sealer writes the seal with `Create` at the first missing `seq` counted from the durable GC floor, then re-reads the floor; a seal that landed on a name GC just freed does not count (C2), and a seal sits *at* the gap of an in-flight PUT, never above it (C3′). Objects above the seal are outside the canonical log: nobody replays or acks them. Each I/O of the seal retries with backoff, and the outer race loop is bounded so an inconsistent store yields `Io` instead of spinning while holding the lease.
**Why.** Sealing at "tail + 1" either lands on a freed name or above an in-flight object, losing an acked write or breaking the prefix property.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::seal_on_gc_freed_name_is_detected`, `crates/cairn-store/tests/shared_wal.rs::seal_lands_on_inflight_gap_not_above_it`, `crates/cairn-store/tests/shared_wal.rs::seal_gives_up_on_an_inconsistent_store`, `crates/cairn-model/tests/m3.rs::c2_seal_on_a_name_gc_just_freed_loses_an_ack`
**See.** `docs/guides/nodewal.md` §1.

### NW-4 — Range takeover order

**Statement.** `acquire_shared` seals and reads the old logs, opens the Db (replay), CASes our own segment into the owner record before the first frame (C4), flushes the replayed rows to L0 through the last frame read (C6), then drops the old segments (C7), and only then hands the Db to the engine. Old segments are pruned by the replay mark read from the stored manifest (`proof::DurableReplayMark`), never by the in-memory manifest.
**Why.** Each step closes a counterexample: an unlisted segment hides acked frames, and serving unflushed or still-listed replayed rows lets a later taker reach a different history.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::owner_segment_written_before_first_frame`, `crates/cairn-store/tests/shared_wal.rs::takeover_flushes_replayed_rows_before_serving`, `crates/cairn-store/tests/shared_wal.rs::takeover_prunes_old_segments_before_serving`, `crates/cairn-model/tests/m3.rs::c7_pruning_by_the_in_memory_mark_loses_an_ack`
**See.** `docs/guides/nodewal.md` §1; `crates/cairn-store/src/nodewal/acquire.rs`.

### NW-5 — Never replay one's own live log

**Statement.** If a range's owner record lists a segment in this incarnation's log and that log already holds frames of the range, takeover fail-stops (C1), except for a log whose stream has been retired and rotated, which is sealed and replayed like anyone else's. A failed owner-record CAS during `acquire_shared` also fail-stops, while an ambiguous one is resolved by re-reading. A dirty yield rotates the stream before writing the owner record, and a self-reclaim applies a pending release only over the exact record it was made for, so no release path leaves a record pointing at a live log.
**Why.** Replaying one's own log while its PUTs are still in flight loses writes that are acked afterwards.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::own_live_log_listed_without_a_release_still_fails_stop`, `crates/cairn-store/tests/shared_wal.rs::range_lost_while_alive_reacquires_from_the_rotated_log`, `crates/cairn-store/tests/shared_wal.rs::self_reclaim_after_a_lost_clean_owner_cas_does_not_fail_stop`, `crates/cairn-model/tests/m3.rs::self_claim_without_the_pending_release_meets_its_own_live_log`
**See.** `docs/guides/nodewal.md` §1.

### NW-6 — A release clears WAL segments only after a clean seal

**Statement.** A release removes the range's `wal_segments` only when the Db sealed cleanly (`LeaseKeeper::writer_sealed`, recorded by an immediate owner-record CAS); a dirty release keeps the segments and the taker seals and replays them (C4′). Before writing the owner record, a dirty yield retires and rotates the stream so the kept segment points at a log that no longer acks, and a lost or hung clean CAS is retried by the release with the union of segments, never dropping a listed one except an unserved segment of our own incarnation.
**Why.** Clearing segments while rows are still only in the memtable loses acked writes; sealing a live log would fail-stop the whole node.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::cooperative_release_flushes_before_clearing_segments`, `crates/cairn-store/tests/shared_wal.rs::cooperative_handoff_replays_nothing`, `crates/cairn-store/tests/shared_wal.rs::dirty_yield_taken_by_another_node_does_not_fail_stop_the_node`, `crates/cairn-model/tests/m3.rs::nw6_unclean_release_loses_an_ack`
**See.** `docs/guides/nodewal.md` §1.

### NW-7 — The GC floor advances only past objects no reference still needs

**Statement.** GC writes the floor first and deletes below it afterwards; a failed delete is retried on a later round without moving the floor back. A range references `(replay_after_wal_id, ∞)` of its *stored* manifest; checkpoints reference no frames, and tombstoned ranges need none. An unreadable reference makes the round keep everything (deletion only when provable), and the stored-manifest read is bounded so one hung read cannot stall the GC loop.
**Why.** SlateDB advances its in-memory replay mark before writing the manifest; trusting it lets GC delete frames that a crash would still need.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::gc_keeps_object_until_all_ranges_flushed`, `crates/cairn-store/tests/shared_wal.rs::gc_floor_never_passes_frames_the_stored_manifest_does_not_cover`, `crates/cairn-store/tests/shared_wal.rs::gc_survives_failing_reference_read`, `crates/cairn-model/tests/m3.rs::nw7_gc_trusting_the_in_memory_mark_loses_an_ack`
**See.** `docs/guides/nodewal.md` §1.

### NW-8 — Never seal a live incarnation's log; dead logs are forgotten by a pure function of the bucket

**Statement.** A dead log (ended incarnation, or a retired stream's log) has no GC floor and no in-memory progress: it stays untouched until no owner record, tombstones included, lists it, and is then sealed if needed and forgotten once (`LogStore::forget`) under `DeleteProof::dead_log` or `DeleteProof::rotated_log`, whose snapshot of owner records is taken after the death (or retirement) and after the seal is on the bucket. Forgetting moves data, floor and index to the trash but keeps the seal and writes a `forgotten` marker first, so an interrupted forget is finished without resealing.
**Why.** Sealing a live log kills that node; keeping the seal is a clock-free fence against a zombie writing at the old seal position.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::orphan_sweep_leaves_live_logs_alone`, `crates/cairn-store/tests/shared_wal.rs::dead_log_seal_and_floor_are_removed_when_unreferenced`, `crates/cairn-store/tests/shared_wal.rs::zombie_cannot_ack_after_its_sealed_log_is_forgotten`, `crates/cairn-model/tests/m3.rs::without_the_clock_forgetting_a_dead_log_lets_its_zombie_ack`
**See.** `docs/guides/nodewal.md` §1; `crates/cairn-store/src/nodewal/sweep.rs`.

### NW-9 — Every range opens on the shared WAL; the WAL kind comes from the owner record

**Statement.** New ranges are always `shared`. An unknown `wal_kind` refuses the open and leaves the record unchanged, and a mismatched `wal_name` marker in the manifest refuses the open. A legacy `slatedb` range is opened only inside the `wal/shared` migration step, which drains the native WAL (SlateDB refuses the marker change while frames remain) and then CASes `wal_kind=shared`; outside that step the node returns `StoreError::NativeWal`.
**Why.** Mixing WAL kinds on one Db would let frames be replayed from the wrong place or not at all.
**Proven by.** `crates/cairn-store/src/session/tests.rs::unknown_wal_kind_refuses_open`, `crates/cairn-store/tests/shared_wal.rs::wal_kind_mismatch_refuses_open`, `crates/cairn-store/tests/shared_wal.rs::child_of_native_parent_adopts_shared_wal`, `crates/cairn-store/tests/migrate.rs::native_wal_range_moves_to_shared_wal`
**See.** `docs/guides/nodewal.md` §1.

### NW-10 — Followers expose contiguous objects on atomic stores, only acked objects otherwise

**Statement.** The tailer reads sequentially from the floor and stops at the first non-data object. On an `atomic_put` store every readable object is exposed at once; on an `atomic_put = false` store exposure stops at the highest `acked_upto` seen (C5), and a follower does not serve in-progress state for a range whose unexposed tail is older than `max(4 polls, 1 s)`. A reader accepts only frames contiguous by WAL id and otherwise reports `WalTruncated` and reloads the manifest.
**Why.** On an atomic store a sealer sees the same object, so it will be replayed; a torn object may read as present to a follower but empty to the sealer.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::follower_exposes_contiguous_objects_on_an_atomic_store`, `crates/cairn-store/tests/shared_wal.rs::follower_exposes_only_acked_objects`, `crates/cairn-server/tests/follower_read.rs::follower_does_not_serve_a_stale_status_when_the_leader_is_idle`, `crates/cairn-store/src/nodewal/tailer.rs::tests::reader_does_not_apply_frames_past_objects_the_tailer_skipped`
**See.** `docs/guides/nodewal.md` §1; `crates/cairn-model/tests/m3.rs::follower_matrix`.

### NW-11 — The log index states only facts about acked objects and only saves GETs

**Statement.** `<log>/index` is rewritten by the writer from acked objects only (on GC ticks with new objects and every 64 newly acked objects), so every `seq` in `[floor, upto]` is data and any seal lies above `upto`. Sealers probe for the gap from `max(floor, upto + 1)`, and takers GET only objects the index lists with frames of their range above the manifest's replay mark. The index is read leniently: missing, garbled or unreadable ⇒ scan from the floor.
**Why.** It turns takeover and sealing from O(log) reads into O(range) reads without being needed for correctness.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::takeover_reads_only_objects_of_its_range`, `crates/cairn-store/tests/shared_wal.rs::later_taker_reads_index_left_by_the_sealer`, `crates/cairn-store/tests/shared_wal.rs::garbled_log_index_falls_back_to_reading_from_the_floor`
**See.** `docs/guides/nodewal.md` §1.

### NW-12 — Forgetting a dead log can be undone

**Statement.** `LogStore::forget` moves every object of a dead log except its seal (data, floor, index) into the trash in chunks, and the trash purge deletes for real only when no owner record lists the log any more. Frames that `gc_log` deletes below the floor of a *live* log are not covered; they rely on bucket versioning.
**Why.** A mistaken forget must be recoverable; deleting customer data happens only when provable.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::dead_log_forgotten_by_mistake_is_restored`
**See.** `docs/guides/nodewal.md` §1.

## C — shared-WAL design claims

The shared-WAL design was model-checked before it was built; each naive variant the checker broke became a counterexample, and each fix became a lasting design claim with its own regression test. The NW invariants above are how the code enforces these claims; the C codes name the failure each rule exists to prevent, and `Cause::C1` is also a fail-stop cause label in code.

### C1 — A node never takes a range back by replaying its own live log

**Statement.** Within one incarnation a node must not reopen a range by replaying a log its stream may still write to; if the owner record lists such a segment with frames of the range, takeover fail-stops with cause `C1`. A log whose stream has been retired (rotated) no longer counts as live and may be sealed and replayed.
**Why.** An in-flight PUT of the old Db can land and be acked after the replay, so the reopened Db lacks an acked write.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::own_live_log_listed_without_a_release_still_fails_stop`, `crates/cairn-store/tests/shared_wal.rs::range_lost_while_alive_reacquires_from_the_rotated_log`
**See.** NW-5; `docs/guides/nodewal.md` §1.

### C2 — A seal must not land on a name GC just freed

**Statement.** GC writes a durable floor before deleting, and a sealer re-reads the floor after its `Create`; a seal below the floor is discarded and sealing restarts.
**Why.** Otherwise a seal can succeed on a freed name below an acked object, and replay stops before that acked write.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::seal_on_gc_freed_name_is_detected`, `crates/cairn-model/tests/m3.rs::c2_seal_on_a_name_gc_just_freed_loses_an_ack`
**See.** NW-3.

### C3′ — A seal lands at an in-flight gap, never above it

**Statement.** The seal goes at the first gap counted from the durable floor, so an in-flight PUT's slot is sealed rather than skipped.
**Why.** A seal above an in-flight object lets the taker serve `w2` without `w1`, after which the zombie's late PUT lands and is acked.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::seal_lands_on_inflight_gap_not_above_it`, `crates/cairn-model/tests/m3.rs::c3_seal_above_an_inflight_put_loses_a_zombie_ack`
**See.** NW-3.

### C4 — A taker lists its segment before writing its first frame

**Statement.** The new owner CASes its own segment into the owner record before any frame of the range reaches its log.
**Why.** A frame written before the segment is listed is invisible to the next taker, which then neither seals nor reads that log and loses an acked write.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::owner_segment_written_before_first_frame`, `crates/cairn-model/tests/m3.rs::c4_frame_before_segment_loses_an_ack`
**See.** NW-4.

### C4′ — A release clears segments only after the data is flushed

**Statement.** A cooperative release flushes to L0 and seals cleanly before it removes `wal_segments`; a dirty release keeps them.
**Why.** Releasing with rows still in the memtable lets the next owner read nothing while the old log still acks those rows.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::cooperative_release_flushes_before_clearing_segments`, `crates/cairn-model/tests/m3.rs::nw6_unclean_release_loses_an_ack`
**See.** NW-6.

### C5 — A follower never exposes an object a sealer might read as absent

**Statement.** On stores that may tear objects (`atomic_put = false`) a follower exposes object `k` only after seeing `acked_upto ≥ k`; on atomic stores contiguous exposure from the floor is safe.
**Why.** A torn object can read as present at the follower and empty at the sealer, exposing a write that never becomes part of history.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::follower_exposes_only_acked_objects`, `crates/cairn-store/tests/shared_wal.rs::follower_exposes_contiguous_objects_on_an_atomic_store`
**See.** NW-10.

### C6 — Replayed rows reach L0 before the taker serves

**Statement.** After replay the taker flushes through the last frame it read (writing a barrier if the memtable is already empty) before serving.
**Why.** Serving replayed rows that exist only in memory lets a crash and a second replay of a torn object reach a different answer.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::takeover_flushes_replayed_rows_before_serving`, `crates/cairn-store/tests/shared_wal.rs::takeover_covers_replayed_frames_already_in_l0`
**See.** NW-4.

### C7 — Old segments are dropped before the taker serves

**Statement.** After the flush the taker removes the previous owners' segments from the owner record, using the replay mark of the stored manifest.
**Why.** A later taker re-reading the old log could see a torn object differently and resurrect a write the previous taker decided did not exist.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::takeover_prunes_old_segments_before_serving`, `crates/cairn-model/tests/m3.rs::c7_pruning_by_the_in_memory_mark_loses_an_ack`
**See.** NW-4.

## SM — split/merge, the range table and retired-DB GC

The range table (`cluster.conf`) maps contiguous partition-key ranges to range ids; each range has its own SlateDB database under `ranges/<id>/` and an owner record under `owners/`. Ranges are split and merged by a coordinator through an intent CAS and a commit CAS on that table; children are clones or unions of their sources. Ranges that leave the table are retired (tombstoned) and their databases are later deleted by the retired-DB GC pass (D-d) — only behind typed delete proofs and a trash. These invariants are the contract of `crates/cairn-store/src/{cluster,partition_table,proof,trash,ledger}.rs`, `crates/cairn-store/src/config/range_db.rs`, `crates/cairn-server/src/{split,gc}*` and the decision core `crates/cairn-split/`; the protocol models M1 and M2 check many of them exhaustively.

### SM-1 — The range table always partitions the key space and never reuses a range id

**Statement.** Ranges cover `0..=u64::MAX` with no gap and no overlap, every id names exactly one range, and an id is never reused (D22). Each table change raises the version by one and never lowers `next_range_id`; a new intent may only promise ids allocated in that same CAS, so an id that left the table (or never entered it) unpromised stays out forever. This is enforced in `PartitionTable::from_entries` and `check_successor`, not by callers.
**Why.** SM-11 and SM-14 rely on "out of the table, unpromised, already allocated" being permanent; a reused id would alias a new range onto a DB that is live or retired.
**Proven by.** `crates/cairn-store/src/partition_table.rs::tests::decode_rejects_gap_and_overlap`, `crates/cairn-store/src/partition_table.rs::tests::equal_split_covers_u64_without_gaps`, `crates/cairn-store/src/cluster.rs::tests::update_enforces_no_id_reuse`, `crates/cairn-store/src/cluster.rs::tests::update_forbids_new_intent_promising_issued_id`
**See.** `docs/guides/split-merge.md` §1.

### SM-2 — The range table changes only by CAS, and an ambiguous CAS is resolved by reading back

**Statement.** `ClusterConfig::update` writes the table with `PutMode::Update` on its etag: of two writers starting from the same version exactly one wins, and the loser gets `TableUpdate::Conflict` carrying the winner's table. An ambiguous outcome (`Precondition` or `Err` after client retries) is resolved by rereading (`cas::put_resolving`): the intended table is there ⇒ `Applied`, another table ⇒ `Conflict`, unreadable ⇒ `Err`. Lines a newer binary wrote and this binary does not understand are copied verbatim.
**Why.** The object-store client retries 5xx responses even for conditional PUTs (OS1), so a write that looks lost may have landed; misreading it would strand an intent on the table.
**Proven by.** `crates/cairn-store/src/cluster.rs::tests::table_update_is_cas`, `crates/cairn-store/tests/failpoints.rs::paused_update_loses_cas_to_writer_in_the_gap`, `crates/cairn-store/src/cluster.rs::tests::ambiguous_cas_resolves_to_applied_when_our_table_landed`, `crates/cairn-server/tests/split.rs::ambiguous_intent_cas_never_strands_the_intent`
**See.** `docs/guides/split-merge.md` §1; AGENTS.md rule 7.

### SM-3 — A split or merge has two CAS points, and exactly one of them wins

**Statement.** The intent CAS is the decision point and the commit CAS is the commit point. Rolling back first CASes the intent away and only then discards the children's DBs and owner records. A node that claims a source carrying an intent no local coordinator runs rolls it back before opening the DB — unless the op ledger (SM-19) is at `Prepared` and a direct check (`split::forward::prepared`) finds every child ready and every source still closed at the expected epoch, in which case it commits on the coordinator's behalf (`split::roll_forward`). A coordinator rolled back by someone else re-checks that the intent is still its own (`ensure_intent_ours`) before every child-creating step, so it never recreates children; every roll-back/roll-forward decision is made by `cairn_split::step`.
**Why.** Both CASes target the same table version, so either the children replace the sources or the sources stay — never both, never neither.
**Proven by.** `crates/cairn-server/tests/split.rs::split_crash_at_every_step`, `crates/cairn-server/tests/split.rs::split_crash_before_commit_cas_rolls_forward`, `crates/cairn-server/tests/failpoints.rs::coordinator_rolled_back_by_another_does_not_recreate_children`, `crates/cairn-model/tests/m2.rs::d5_coordinator_rolled_back_by_another_strands_an_owner_record`
**See.** `docs/guides/split-merge.md` §1; M2.

### SM-4 — Children are cloned only from a source DB that is really closed

**Statement.** `quiesce` answers only after the source DB is sealed (WAL drained, memtable flushed, DB closed), and the coordinator keeps the source's lease so nobody reopens it. Before clone/union the coordinator checks that no source owner record still lists a `wal_segment` (`ensure_wal_drained`); otherwise the operation fails with a retryable `503` and rolls back. The SlateDB fork's clone additionally refuses a source whose WAL is not covered by its manifest (G10).
**Why.** A clone taken from a DB with unflushed WAL would silently miss acknowledged rows.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::clone_refuses_unclosed_parent`, `crates/cairn-store/tests/range_clone.rs::sealed_parent_clones_with_every_acked_row`, `crates/cairn-server/tests/quiesce.rs::quiesce_waits_for_inflight_turn_then_closes`, `crates/cairn-store/tests/shared_wal.rs::open_shared_parent_refuses_clone`
**See.** `docs/guides/split-merge.md` §1.

### SM-5 — A child carries only the entity rows of its key span

**Statement.** Clone and union project every source onto the two byte ranges that hold entity rows of the child's partition-key span (`keyspace::owned_ranges`). Derived regions (secondary indexes) lie outside every range and are rebuilt when the child opens; `union_ranges` merges sources that do not overlap.
**Why.** A child must not inherit rows it does not own, and derived state must be rebuildable from entity rows alone.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::clone_union_and_discard_follow_the_pk_span`
**See.** `docs/guides/split-merge.md` §1.

### SM-6 — `WrongRange` is returned before the engine does anything

**Statement.** The range check (D23) runs at HTTP ingress and again in the actor, against the table version and range the sender named and any pending intent. A batch is checked once as a whole: if any message does not belong to the addressed range the whole batch gets `409` and nothing is written. The sender learns the newer table from the `409` body, regroups every unacknowledged message by the new table (keeping per-destination order) and resends; a sender whose destination has no leader probes the node that just released it and keeps only a `409` carrying a newer table.
**Why.** No write may land in a range that no longer owns its key, and stale senders must converge without a restart or a readable `cluster.conf`.
**Proven by.** `crates/cairn-server/tests/wrong_range.rs::inbound_rejected_while_range_retiring`, `crates/cairn-server/tests/wrong_range.rs::stale_sender_recovers_after_table_change`, `crates/cairn-server/tests/failpoints.rs::batch_split_between_gather_and_send_is_regrouped_by_the_new_table`, `crates/cairn-server/tests/failpoints.rs::after_split_a_stale_node_learns_the_table_from_the_releaser`
**See.** `docs/guides/split-merge.md` §1.

### SM-7 — Acknowledged messages and calls are neither lost nor duplicated across a split or merge

**Statement.** During a table change clients see only retryable `503`s. A batch is one commit at the receiver, so a split lands before or after it, never inside it; a rejected batch keeps every row at the sender, which resends and is deduplicated by key. Transient coordinator failures — lost leadership, a changed table, a seal that fails or times out, store or lease errors, and the bounded waits while holding the engine, intent or quiesced source (`FLUSH_MEMTABLE_BOUND`, `CLONE_BOUND`, `CLAIM_BOUND`) — return `503` and roll back.
**Why.** Splits happen under load and automatically; they must be invisible to exactly-once delivery.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::inbound_during_split_quiesce_is_neither_lost_nor_duplicated`, `crates/cairn-server/tests/failpoints.rs::batch_rejected_503_keeps_every_row`, `crates/cairn-server/tests/split.rs::split_under_load_in_process`, `crates/cairn-server/src/split/tests.rs::store_and_lease_errors_are_retryable`
**See.** `docs/guides/split-merge.md` §1.

### SM-8 — A range that leaves the table is retired, not merely stopped

**Statement.** `reconcile` calls `mark_retiring` and then `retire` only when the table read from the bucket (`ClusterConfig::read_stored`) also lacks the range; a read failure retires nothing that round. Retiring writes a tombstone (`retired`) on the owner record, so no node holding an old table can reclaim the parent DB; if the leader dies before writing it, SM-11 writes it instead. Conversely, a range still in the table whose engine job died is rebuilt.
**Why.** An in-memory table may come from a peer's `409` body that nobody has confirmed on the bucket, and an untombstoned parent could be reopened by a stale node.
**Proven by.** `crates/cairn-server/tests/range_table.rs::node_follows_table_change_without_restart`, `crates/cairn-server/tests/quiesce.rs::retired_range_is_never_reacquired`, `crates/cairn-server/tests/split.rs::range_is_not_retired_by_a_table_that_is_not_on_the_bucket`, `crates/cairn-server/tests/failpoints.rs::range_job_that_panics_is_rebuilt`
**See.** `docs/guides/split-merge.md` §1.

### SM-9 — Retired-DB GC deletes a DB only when it proves nobody references it

**Statement.** D-d considers only tombstones older than `min_age`, and right before each delete it rebuilds `DeleteProof::retired_db` on one fresh table snapshot: the range is out of the table, unpromised, tombstoned, covered by no GC barrier, listed in the `external_dbs` of no live range or in-flight child, and its own manifest holds no named final checkpoint of an untombstoned range; the delete re-reads the table version. Every ambiguity keeps the DB (unreadable manifest, live DB without a manifest, foreign prefix, unnamed checkpoint), and a round deletes nothing if the runner's clock and the store's `last_modified` differ by more than `min_age/4`. The reference set is read once per table version per round, which is sound because at a fixed version the live ranges' `external_dbs` only shrink (irreversible.md N4); only the leader of the lowest live range runs the pass, and the manifest is deleted last.
**Why.** AGENTS.md rule 5: a leak only costs space, a wrong delete loses customer data.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::retired_parent_db_kept_while_children_reference_it`, `crates/cairn-server/tests/failpoints.rs::gc_round_reproves_when_split_commits_between_check_and_delete`, `crates/cairn-server/tests/failpoints.rs::dd_keeps_a_parent_its_children_name_when_the_reference_scan_is_blind`, `crates/cairn-server/tests/gc.rs::gc_rounds_delete_nothing_when_the_node_clock_runs_ahead_of_the_store`, `crates/cairn-store/tests/range_clone.rs::n4_live_refs_only_shrink_at_a_fixed_version`
**See.** `docs/guides/split-merge.md` §1; M2 (`NO_DELETE_UNDER_LIVE`).

### SM-10 — Discarding an orphan child of a split keeps the live parent's ancestry

**Statement.** When a split is rolled back, discarding the orphan child (or grandchild) never removes a checkpoint that the live parent's ancestry still stands on. This is the split-shaped case of SM-12, which states the general rule for splits and merges.
**Why.** A removed ancestor checkpoint lets SlateDB GC delete SSTs a live range still reads.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::discarding_an_orphan_grandchild_keeps_the_live_parents_ancestry`
**See.** `docs/guides/split-merge.md` §1.

### SM-11 — A range that left the table without a tombstone is tombstoned by proxy, only when provable

**Statement.** The orphan retirer (`gc::OrphanRetirer`, run by the D-d runner; nudged when an intent settles, a session ends or a node becomes runner, with a `--retire-orphans-every-s` safety net, default 3600 s) tombstones range `r` iff: `r` is not in the freshly reloaded table; no intent promises `r`; `r < next_range_id`; and the owner record was released or its session is dead by exactly the rule of `Session::claim`. The tombstone is a CAS on the version just read, copies the record but drops its `wal_segment`, and stamps `retired_ms`. Every ambiguity keeps (unreadable record or session, unknown WAL kind, owner alive, CAS lost); the pass respects the upgrade gate but not GC barriers, since it deletes nothing.
**Why.** A leader that dies between the commit and its own tombstone would leave the parent DB orphaned forever and its WAL references would pin the shared-WAL floor (NW-7).
**Proven by.** `crates/cairn-server/tests/failpoints.rs::parent_left_without_tombstone_is_retired_after_its_leader_dies`, `crates/cairn-server/tests/failpoints.rs::inflight_split_children_are_never_tombstoned`, `crates/cairn-server/tests/gc.rs::orphan_retirer_writes_only_what_it_can_prove`, `crates/cairn-server/tests/failpoints.rs::orphan_tombstone_whose_cas_loses_is_kept_and_retried`
**See.** `docs/guides/split-merge.md` §1.

### SM-12 — Rolling back a split or merge deletes only the checkpoints the orphan child created

**Statement.** `discard_range_db(child, sources)` deletes only the `final_checkpoint_id` of each `external_dbs` entry in the orphan's manifest, never a `source_checkpoint_id`, and keeps any checkpoint still mentioned by a manifest in the lineage (tripwire `split::discard::referenced_checkpoint`). If reading the lineage or deleting a checkpoint fails, it returns an error without deleting the child's DB, and the caller keeps the child's owner record so SM-14 can finish later. An entry whose checkpoint is gone but that stands on no parent SST in any live view is reported as a pending detach, not a loss.
**Why.** The orphan's manifest is the only record of the checkpoints it placed on its ancestors (G4); losing it leaves a non-expiring checkpoint pinning a live range's SSTs forever.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::discarding_an_orphan_merge_keeps_the_sources_ancestry`, `crates/cairn-store/tests/range_clone.rs::discard_keeps_the_orphan_until_its_checkpoints_are_gone`, `crates/cairn-store/tests/failpoints.rs::discard_tripwire_fires_and_keeps_a_referenced_checkpoint`, `crates/cairn-server/tests/failpoints.rs::merge_roll_back_keeps_checkpoints_the_sources_stand_on`
**See.** `docs/guides/split-merge.md` §1; M2 (`NO_ORPHAN_CHECKPOINT_ON_LIVE`).

### SM-13 — Nobody opens the source of an in-flight intent for writing

**Statement.** From quiesce until the commit or roll-back completes, the source DB is not opened for writing. A node that claims the source while another node coordinates rolls the intent back (SM-3), which makes the commit CAS fail; on the coordinating node itself the coordinator marks the source quiesced (`ActiveGuard::mark_quiesced`) just before the quiesce command, and `factory` then waits holding the lease (`ActiveIntents::wait_unquiesced`) until the coordinator's guard drops. The tripwire `node::open::quiesced_source` sits right before the DB open.
**Why.** Writes into a source after the clone are missing from the children while the commit CAS still wins; a frozen source is also what keeps it from compacting or detaching under in-flight children (G5).
**Proven by.** `crates/cairn-server/tests/failpoints.rs::quiesced_split_source_never_reopens_while_coordinator_lives`, `crates/cairn-server/tests/failpoints.rs::quiesced_merge_sources_never_reopen_while_coordinator_lives`, `crates/cairn-model/tests/m1.rs::all_fences_hold_for_split`, `crates/cairn-model/tests/m2.rs::n2_in_flight_children_rest_on_four_layers`
**See.** `docs/guides/split-merge.md` §1; M1 (`SM13_QUIESCED_SOURCE_CLOSED`).

### SM-14 — Orphan children of a roll-back are discarded, only when provable

**Statement.** The orphan pass discards range `r` (DB first, then owner record) iff `r` is permanently out of the table (conditions 1–3 of SM-11) and there is evidence it never entered the table — its op ledger is `Cleaned`, or, without a ledger, its DB is listed in the `external_dbs` of a range in the table, or it has a manifest but no owner record — and, checked directly: no live range lists `r` in `external_dbs`, no GC barrier covers `r`, and the table version is unchanged right before the delete. A ledger at `Prepared` or `Committed` is counter-evidence. Any new path that deletes an owner record must preserve the premise of the second inference: the owner record of a range that ever entered the table outlives its DB's manifest.
**Why.** Roll-backs can be left half done (SM-12 failure, ambiguous roll-back CAS, a coordinator waking up after being rolled back), and the leftovers would otherwise leak forever.
**Proven by.** `crates/cairn-server/tests/gc.rs::orphan_round_keeps_retired_parent_without_owner_that_children_stand_on`, `crates/cairn-server/tests/gc.rs::orphan_round_still_discards_true_orphans_nobody_stands_on`, `crates/cairn-server/tests/split.rs::half_done_roll_back_is_finished_by_a_later_round`, `crates/cairn-server/tests/split.rs::claimed_orphan_is_discarded_by_the_ledger_after_its_source_retires`
**See.** `docs/guides/split-merge.md` §1; M2 (`SM14_OWNER_OUTLIVES_DB`).

### SM-15 — Split/merge operations belong to the node, and no intent outlives its coordinator

**Statement.** `RangeOps::start` runs each operation in a node task: the HTTP handler only waits on a ticket, a client disconnect does not stop the operation, and a panic is recorded as `failed`. The quiesce command carries a hold wire owned by the coordinator's guard, so if the guard drops while the engine is quiesced the engine releases its lease and the source is rolled back through SM-3. Roll-backs touch only the coordinator's own intent and retry in the background for up to `SETTLE_FOR` (10 min); the intent reconciler (D-d runner, `--reconcile-intents-every-s`, default 15 s) asks each source's leader who coordinates and, for an intent unheld across two consecutive rounds, rolls it forward if prepared, otherwise back (an unanswered query counts as held).
**Why.** A stranded intent blocks auto-split for the whole cluster; safety still rests only on CAS, since roll-back and commit are mutually exclusive.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::client_disconnect_mid_split_never_strands_the_source`, `crates/cairn-server/tests/failpoints.rs::coordinator_panic_after_quiesce_releases_and_rolls_back_the_source`, `crates/cairn-server/tests/split.rs::stale_intent_without_coordinator_is_rolled_back_by_the_runner`, `crates/cairn-server/tests/failpoints.rs::reconciler_never_rolls_back_an_intent_a_live_coordinator_holds`
**See.** `docs/guides/split-merge.md` §1.

### SM-16 — Deleting a range DB, owner record or op ledger can be undone from the trash

**Statement.** D-d (`delete_retired_db`), roll-back discards (`discard_range_db`), `delete_owner` and ledger reaping (`ledger::delete_op`) move objects into `<prefix>/trash/<date>/<id>/` (copy, then delete, manifest last) instead of deleting them; to readers of the original location the result is a delete. The trash pass of the D-d runner (`gc::purge_trash_once`) really deletes only entries older than `--trash-keep-s` (default 7 days) and only after re-checking the reloaded table and `external_dbs`; entries still needed are kept (tripwire `trash::purge::still_needed`). Restoring a child DB restores its owner record first; checkpoints removed on ancestors and SlateDB's internal GC are not covered.
**Why.** Every proof can be wrong; a trash with a keep window turns a wrong delete into an operator-recoverable event.
**Proven by.** `crates/cairn-store/tests/trash.rs::retired_db_deleted_by_mistake_is_restored`, `crates/cairn-store/tests/trash.rs::discarded_child_db_is_restored`, `crates/cairn-store/src/trash.rs::tests::purge_rechecks_before_deleting`, `crates/cairn-server/tests/ctl.rs::trash_restore_by_range_brings_back_db_and_owner`
**See.** `docs/guides/split-merge.md` §1; `docs/runbooks/restore-from-trash.md`.

### SM-17 — A split/merge source carries a close mark on its own DB from before the clone

**Statement.** After quiesce and `ensure_intent_ours`, before clone/union, the coordinator sets the `closed` flag in the source manifest only if `writer_epoch` is still the epoch of the writer it quiesced (`ObjectStores::close_range_db_at`, fork patch `Admin::close_for_writes_at_epoch`); a mismatch fails with `Fenced` and the operation rolls back. From then on SlateDB refuses every writer on that DB, checked atomically with the epoch bump. A commit leaves the mark forever; a roll-back removes it only after the intent-removal CAS and only if the epoch read before that CAS still holds (`reopen_range_db_at`), and a stale mark on a range that is in the table with no intent is healed on the next open (`heal_stale_close`).
**Why.** It fences reopening a quiesced or retired source on the DB itself, even if the table read and SM-13 both fail.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_a_stale_table_from_reopening_the_parent`, `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_a_quiesced_split_source_closed`, `crates/cairn-server/tests/split.rs::stale_close_on_a_live_range_heals_on_next_open`, `crates/cairn-store/tests/range_clone.rs::close_at_epoch_refuses_after_a_reopen`
**See.** `docs/guides/split-merge.md` §1; G14.

### SM-18 — Every deletion of customer data goes through a typed proof gate

**Statement.** `delete_retired_db`, `discard_range_db`, `delete_owner` and `LogStore::delete_all` require a `cairn_store::proof::DeleteProof` for exactly the subject they delete. A proof can only be built by a function that durably reads the facts it needs: `retired_db` (SM-9), `abandoned_child_db` (SM-14's direct checks inside the constructor), `abandoned_child_owner` (only once the child's DB has no manifest), `dead_log` (the death is read before the owner-record snapshot) and `settled_op` (SM-19). `scripts/lint-sources.sh` check 4 rejects new direct object deletes and `DeleteProof::assume*` outside tests.
**Why.** It moves "prove before delete" from reviewer discipline into the type system.
**Proven by.** `crates/cairn-store/tests/proof.rs::retired_db_proof_reads_the_facts_it_needs`, `crates/cairn-store/tests/proof.rs::abandoned_child_proof_checks_refs_barriers_and_version`, `crates/cairn-store/tests/proof.rs::proof_for_another_subject_is_refused`, `crates/cairn-store/src/proof.rs::tests::dead_log_needs_death_and_seal_before_snapshot`
**See.** `docs/guides/split-merge.md` §1.

### SM-19 — Every split or merge keeps a durable op ledger

**Statement.** The coordinator creates `ops/<first child id>` (`cairn_store::ledger`) at `Intent` right after the intent CAS wins and advances it before each side effect: `Closed` (with the sealed epoch), `Cloned`, `Prepared` (every child has a DB and owner record, before the commit CAS), `Committed`. A roll-back writes `Aborting` before the intent-removal CAS and `Cleaned` after it wins. The ledger changes only by `cairn_split::Phase::advance`, as a CAS on the version just read (`put_resolving`), and terminal phases are never overwritten; a ledger whose op has settled and that is older than `min_age` is moved to the trash by D-d through `DeleteProof::settled_op`.
**Why.** It gives roll-forward (SM-3) and orphan discard (SM-14) positive evidence of how far an operation got, instead of inference from bucket history.
**Proven by.** `crates/cairn-store/src/ledger.rs::tests::phases_follow_the_transition_rule`, `crates/cairn-server/tests/split.rs::every_op_leaves_a_ledger_ending_committed_or_cleaned`, `crates/cairn-server/tests/split.rs::settled_ledgers_are_reaped_by_the_dd_round`, `crates/cairn-store/tests/proof.rs::settled_op_proof_waits_for_every_range_to_settle`
**See.** `docs/guides/split-merge.md` §1.

### SM-20 — Auto-split can cut a call tree grouped by prefix, inside that prefix

**Statement.** With the `calls/prefix` gate on, an unkeyed callee of `ctx.call`/`ctx.send` keeps the caller's top 32 partition-key bits (`partition_key_for_callee_prefix`), so a call tree stays inside one 2³² window — almost always one range, but not one key. Auto-split samples `owner_pk` and cuts at a weighted median inside that window (`choose_key`). The rule belongs to the caller (a mark in entry 0, written at creation) and does not depend on the range table, the gate's current state or outbox rows; callers with `placement: "spread"` keep the old spread placement.
**Why.** Co-locating call trees makes inter-range calls rare without giving up the ability to split a hot tree.
**Proven by.** `crates/cairn-server/tests/auto_split.rs::on_mode_splits_a_hot_call_tree_inside_its_prefix`, `crates/cairn-server/tests/interrange.rs::unkeyed_call_tree_stays_on_one_range_with_the_gate_on`, `crates/cairn-store/src/partition_table.rs::tests::callee_prefix_tree_stays_in_one_window_and_spreads_inside_it`, `crates/cairn-server/tests/failpoints.rs::call_row_written_before_the_gate_keeps_its_callee_after_the_gate`
**See.** `docs/guides/split-merge.md` §1.

### SM-21 — The range table is created only on an empty bucket, and a vanished table stops deletions

**Statement.** When `cluster.conf` is absent, `ClusterConfig::open` creates the initial table only if a delimiter LIST at the prefix root (`format::has_cluster_data`) finds no `ranges/`, `partition-*` or `owners/`; otherwise the node refuses to start with `StoreError::MissingClusterConfig`, and a failed LIST is an error. While the cluster runs, `reload` and `read_stored` return `MissingClusterConfig` if the table disappears, and every proof gate, D-d, the trash pass, the orphan pass and tenant purge skip their round.
**Why.** Owner records do not carry key spans, so the table cannot be rebuilt from them, and a fresh table would reallocate ids of DBs that still exist (SM-1, D22).
**Proven by.** `crates/cairn-server/tests/split.rs::lost_cluster_conf_on_a_bucket_with_ranges_refuses_to_start`, `crates/cairn-store/src/cluster.rs::tests::table_is_not_created_over_cluster_data`, `crates/cairn-store/src/cluster.rs::tests::failed_list_never_creates_the_table`, `crates/cairn-server/tests/gc.rs::rounds_delete_nothing_while_the_range_table_is_missing`
**See.** `docs/guides/split-merge.md` §1; `docs/runbooks/cluster-conf-lost.md`.

### SM-22 — Every winning table leaves a history copy, written after the CAS

**Statement.** The initial `Create` and every winning CAS of `update` (including a win established by read-back) write `cluster-history/<version>` with exactly the bytes of `cluster.conf` at that version (`cluster_history::after_won`) — after the CAS, never before. The write is a `Create` resolved by read-back, an existing copy with different bytes is kept, and a failure never fails the table change (it is logged and counted). The newest 16 copies are kept; older ones go to the trash through `DeleteProof::stale_table_history` once the table on the bucket is at least 16 versions ahead.
**Why.** A copy written before the CAS could record a table that never won, and `cairn-ctl table restore` (SM-23) needs the exact last winning table.
**Proven by.** `crates/cairn-store/src/cluster.rs::tests::history_holds_the_bytes_of_every_won_table`, `crates/cairn-store/src/cluster.rs::tests::history_keeps_the_newest_16_and_moves_older_ones_to_the_trash`, `crates/cairn-store/src/cluster_history.rs::tests::failed_history_write_never_fails_the_table_change`
**See.** `docs/guides/split-merge.md` §1.

### SM-23 — `table restore` rebuilds the newest history copy only when it proves that copy was the last table

**Statement.** `cairn-ctl table restore` refuses when any trace of a later table exists: an owner record, `ranges/<id>/` or op ledger with an id at or above `next_range_id` of the candidate version V; a live range of V without an owner record or with a tombstone; a tombstoned range promised by an intent of V; a `committed` ledger whose source is still in V, or a `cleaned` one whose intent V still carries. Any unreadable item or an existing `cluster.conf` also refuses; the write is a `Create`, `--apply` requires every session to have ended unless `--force`, and the default is a dry run. A ledger at `Prepared` whose intent is still in V is resolved in place: if the roll-forward check passes the committed table (V + 1) is written, otherwise the intent is kept.
**Why.** Restoring an older table than the last one that won would resurrect retired parents under live children and hand children's DBs to the trash.
**Proven by.** `crates/cairn-server/tests/split.rs::table_restore_rebuilds_the_last_table_after_random_splits_and_merges`, `crates/cairn-server/tests/failpoints.rs::table_restore_refuses_when_history_lags_a_won_cas`, `crates/cairn-server/tests/failpoints.rs::table_restore_rolls_forward_a_prepared_split_whose_commit_won`, `crates/cairn-model/tests/m2.rs::ctl_refusing_a_prepared_op_keeps_the_core`
**See.** `docs/guides/split-merge.md` §1; `docs/runbooks/cluster-conf-lost.md`.

### SM-24 — Children do not stand on their ancestors forever

**Statement.** A background pass on every node (`lineage.rs`, `--lineage-compact-*`) requests one full compaction (`submit_full_compaction`) for a led range that stands on two or more ancestors, or on an ancestor retired for more than 24 h, wakes the compactor, and then runs SlateDB's own detach task right away (`ObjectStores::detach_now`) on each tick for up to `lineage::DETACH_FOR` (30 min) — changing when detach runs, never its condition. Detach never runs on a range carrying a split/merge intent (the table is re-read right before each `detach_now`). The pass deletes nothing; auto-split refuses a range three or more ancestors deep.
**Why.** SlateDB's own compaction rewrites only L0, so a cold child would keep inherited sorted runs — and every ancestor's DB — forever, and D-d could never reclaim them.
**Proven by.** `crates/cairn-server/tests/split.rs::cold_children_stop_standing_on_ancestors_after_nested_splits`, `crates/cairn-server/tests/split.rs::lineage_detaches_right_after_its_full_compaction`, `crates/cairn-server/tests/failpoints.rs::lineage_never_detaches_a_quiesced_split_source`, `crates/cairn-server/tests/auto_split.rs::auto_split_refuses_a_range_three_ancestors_deep_by_default`
**See.** `docs/guides/split-merge.md` §1; G29.

### SM-25 — The cost of D-d does not grow with the number of swept tombstones

**Statement.** The `gc-swept` ledger (`cairn_store::gc_swept`, read leniently, absent ≡ empty) lists range ids for which a D-d round itself read a tombstoned owner record and found no manifest; later rounds skip those ids entirely. The ledger can only remove candidates, never add a delete, and every `gc_swept::FULL_PASS_EVERY` (24 h, stamped on the ledger) a round ignores it, re-reads every tombstone and rebuilds it. Writes go through `cas::put_resolving`; a lost CAS or error drops the update.
**Why.** Tombstones are permanent, so without the ledger every round would GET and LIST every tombstone ever written.
**Proven by.** `crates/cairn-server/tests/gc.rs::dd_round_cost_does_not_grow_with_swept_tombstones`, `crates/cairn-server/tests/gc.rs::swept_ledger_never_hides_a_db_past_a_full_pass`
**See.** `docs/guides/split-merge.md` §1.


## D-x — range-partition design decisions

Lettered decisions made when the engine moved from fixed partitions to splittable ranges.

### D-a — State and promises need an object key or a scope

**Statement.** `ctx.get/set/promise` in a handler invoked without an object key and without a scope fail terminally with `NO_OBJECT_KEY` (574): there is no partition key that could own the state.
**Why.** Per-partition state for unkeyed services was silently wrong (same service, different partition, different state).
**Proven by.** `crates/cairn-engine/tests/keyspace_layout.rs::unkeyed_unscoped_state_fails_terminally`, `crates/cairn-engine/tests/keyspace_layout.rs::unkeyed_unscoped_promise_is_refused_from_outside`, `crates/cairn-engine/tests/keyspace_layout.rs::scoped_unkeyed_state_is_allowed`
**See.** `docs/key-design.md`.

### D-c — The concurrent-turn cap belongs to the node

**Statement.** `--max-concurrent-turns` caps turns per node and is shared among the engines the node leads; an idle range's share can be borrowed.
**Why.** A per-range cap would grow the total after every split — exactly what the cap exists to prevent.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::busy_range_borrows_idle_node_permits`
**See.** FL-7.

### D-d — Retired range DBs are deleted only by the proof-gated GC pass

**Statement.** "D-d" names the background pass (`gc_retired_dbs_once`) that deletes the database of a retired range once it can prove that no live database, in-flight child, fork barrier or trash entry still references it; deletion goes through `DeleteProof::retired_db` and the trash. It is disabled unless `--gc-retired-dbs` is set.
**Why.** Children reference their parents' SSTs until compaction replaces them; deleting early loses acknowledged data.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::retired_parent_db_kept_while_children_reference_it`, `crates/cairn-store/tests/range_clone.rs::retired_parent_db_deleted_after_children_compact_away_from_it`, `crates/cairn-model/tests/m2.rs::dd_reaps_in_a_round_whose_table_moved`
**See.** SM-9, SM-16, SM-18, SM-25.

### D-e — A merge gathers its sources onto one node by asking for a release

**Statement.** Merge needs all sources led by the coordinator's node: the coordinator asks the other leader to release the range (a `released` tombstone) and then claims it itself; the leadership balancer skips ranges that carry an intent.
**Why.** Reuses the existing leadership-balancing mechanism instead of a new transfer protocol.
**Proven by.** `crates/cairn-server/tests/split.rs::merge_gathers_range_from_other_node`, `crates/cairn-server/tests/failpoints.rs::merge_whose_source_is_taken_and_rolled_back_mid_gather_is_503`
**See.** SM-15.

### D-f — The owner record carries the WAL kind

**Statement.** `owners/range-<id>` records which WAL a range's DB uses; a node claims a range only if it can read that WAL kind, and a DB is never opened with the wrong kind. Every range now uses the shared WAL (NW-9).
**Why.** Opening a DB with the wrong WAL silently drops rows still in the other log.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::wal_kind_mismatch_refuses_open`, `crates/cairn-store/src/session/tests.rs::unknown_wal_kind_refuses_open`
**See.** NW-9, G12.

### D-g — The WAL tier is designed for a quorum but ships with one store

**Statement.** The shared-WAL log type is written so that a Q-of-M quorum of stores could replace it; only M = 1 is implemented. Durability is therefore that of the configured WAL store.
**Why.** Keeps the door open for multi-zone WAL durability without paying for it now.
**Proven by.** none needed — a design constraint on the log type, with no behaviour of its own.
**See.** `docs/architecture.md` §4.

### D-h — The batching window is per node, with an optional per-range override

**Statement.** `--wal-window-ms` sets the minimum gap between two WAL object cuts of a stripe; a range may carry its own window (set through the admin API, stored in `range-windows`), and a frame waits for the window of the ranges it shares a cut with.
**Why.** Latency/cost trade-off per range without one log per range.
**Proven by.** `crates/cairn-server/tests/range_window.rs::range_window_set_by_api_slows_that_range`, `crates/cairn-store/tests/shared_wal.rs::range_window_spaces_cuts_for_that_range`
**See.** NW-1.

### D-i — Stripes per node, ranges assigned by id

**Statement.** Each node writes `--wal-stripes` independent log streams (default 2); a range is assigned to a stripe by its id. A stalled PUT fences and rotates only its own stripe.
**Why.** One stream reaches about one PUT per round trip; a second stream isolates a noisy neighbour.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::hung_put_fences_only_its_stripe`, `crates/cairn-store/tests/shared_wal.rs::hung_put_bounds_stall_then_rotates_its_stripe`
**See.** NW-2.

### D-j — Idle ranges are flushed by frame age

**Statement.** A range whose oldest unflushed WAL frame is older than `--wal-age-flush-ms` (default 60 s) has its memtable flushed to L0, oldest first and with a bounded number of concurrent forced flushes, so the shared WAL's GC floor can advance.
**Why.** Bounds replay time after failover and the length of log a node must keep.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::forced_flush_is_paced_oldest_first`, `crates/cairn-store/tests/shared_wal.rs::forced_flush_covers_frame_already_in_l0`
**See.** NW-7.

## G-x — retired-DB GC hazards (lettered)

### G-a — DB identity compares range ids, never raw path strings

**Statement.** A child's `external_dbs` records its parent as the path string at clone time. GC parses the `RangeId` from the end of the path after normalising and checks that the prefix matches; a prefix it cannot match counts as "still referenced", never as "unused".
**Why.** A bucket moved to another prefix would otherwise make every parent look unreferenced.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::retired_db_kept_when_reference_was_written_under_another_prefix`
**See.** SM-9.

### G-b — A DB is deleted WAL first, manifest last

**Statement.** Deleting a range's DB lists its objects and deletes `wal/` first, then everything else, and the manifest last; a deletion interrupted half-way still has its manifest and is finished by a later round.
**Why.** The manifest is the evidence that the DB exists; removing it first would leak the rest forever.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::partial_retired_db_delete_is_finished_next_round`
**See.** SM-9, SM-16.

### G-c — The shared-WAL floor advances past frames of a deleted range

**Statement.** When a node's log still holds frames of a range whose DB the GC has already deleted, the GC floor treats those frames as no longer referenced instead of waiting forever.
**Why.** Without it the floor sticks and the log leaks (no data loss, unbounded growth).
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::gc_floor_advances_past_frames_of_deleted_retired_range`
**See.** NW-7.


## FL — flow control, scheduling and invocation lifecycle

`FL-*` codes are the invariants of the per-range turn scheduler (`crates/cairn-server/src/flow/`),
the turn shell (`crates/cairn-server/src/turn/`) and the invocation lifecycle core
(`crates/cairn-lifecycle`). FL-16 … FL-21 are rows of one transition table,
`cairn_lifecycle::step(view, event) -> Plan`, which the shell executes and the M4 model checks under
interleavings, crashes and leader changes. Each entry names the tests that pin it; the full map of
files, review checklist and failure modes lives in `docs/guides/flow.md`.

### FL-1 — No network I/O while holding the engine

**Statement.** Every function that touches the network takes a `&Released`, which only `Lease::released` (inside an I/O closure) and `IoSet::spawn` can create, so a network call while the engine is held is a compile error. The outbox drainer gathers a round of rows under the engine, takes one send ticket for the round, and hands the rows to per-destination send tasks (one ordered queue per destination range, at most 8 destinations in parallel) that run with the engine released; it reacquires the engine only to apply acknowledgements.
**Why.** A range's engine serialises every turn; a slow peer must never stall the range that is talking to it (D16).
**Proven by.** `crates/cairn-server/tests/transport_engine.rs::transport_send_does_not_hold_engine`, `crates/cairn-server/tests/dead_destination.rs::hung_destination_does_not_hold_other_destinations`, `crates/cairn-server/tests/interrange.rs::remote_rows_of_one_round_go_in_one_batch_per_destination`, the `compile_fail` doctest on `Released` in `crates/cairn-server/src/lease.rs`.
**See.** `docs/guides/flow.md` §1; D16.

### FL-2 — No external effect before a durable ticket

**Statement.** Sending to the SDK and answering a client require a `&Committed` ticket, a mark of the latest write's `seq` taken when requested and granted only once that write is durable. `Transport::send`/`send_batch` require the narrower `&SendTicket` (LF-1); a `Committed` converts to a `SendTicket`, never the reverse, and neither can be constructed elsewhere.
**Why.** Nothing outside the range may observe state that a crash could still lose (INV-5, D11).
**Proven by.** `crates/cairn-store/tests/durability.rs::ticket_waits_for_others_inflight_writes`, `crates/cairn-server/tests/durability.rs::status_does_not_reply_before_creation_is_durable`, `crates/cairn-server/tests/failpoints.rs::remote_peer_never_sees_a_row_before_it_is_durable`.
**See.** `docs/guides/flow.md` §1; INV-5, D11.

### FL-3 — Permits are acquired in two phases

**Statement.** `ResourceManager::poll_acquire` has no side effects; `Provisional::secure` is the only place that increments counters and takes locks. The effective limit is the tightest of all tiers, and any sequence of acquires and releases returns every counter to zero.
**Why.** A side-effect-free probe can be retried and abandoned freely; a single mutation point makes leaks impossible to hide.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::no_leak_over_ten_thousand_acquire_release`, `crates/cairn-server/src/flow/tests/mod.rs::three_counter_tiers_all_block`, `crates/cairn-server/tests/flow_control.rs::scope_concurrency_cap_is_real`, `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`.
**See.** `docs/guides/flow.md` §1.

### FL-4 — A virtual-object lock is released only when the invocation ends

**Statement.** Of the turn-ending paths, only a terminal outcome releases the virtual-object (VO) lock (`release_all`); suspension, infrastructure failure, `Paused` and a turn cut short (`TurnPermit::drop`) call `release_running`, which keeps it. When it is unknown where a turn stopped, the lock is kept and a durable wake is left, or the rescue net is marked if that wake cannot be written (FL-16). A turn that failed after `Running` first reads the status back: a finishing commit that landed but reported an error leaves a terminal status, and the turn then ends like any terminal one (`release_all`).
**Why.** Releasing the lock of an invocation that may still resume would let a second invocation run on the same object concurrently; keeping the lock of one that has ended stalls the object's next call until a leader change.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::suspend_releases_permit_but_keeps_lock`, `crates/cairn-lifecycle/src/tests/turn.rs::failed_turn_whose_finish_landed_releases_the_lock`, `crates/cairn-server/tests/failpoints.rs::finished_turn_with_a_failed_commit_releases_the_object_lock`, `crates/cairn-server/tests/flow_control.rs::turn_that_read_state_then_died_midway_keeps_object_lock`, `crates/cairn-server/src/flow/tests/mod.rs::lock_holder_abandoning_just_as_granted_keeps_lock`.
**See.** `docs/guides/flow.md` §1.

### FL-5 — Service order follows durable seq, not arrival

**Statement.** Within a queue, requests are granted in order of their durable `seq`, and the current lock holder stands at the head; nobody overtakes it, which gives users FIFO per object key. A nacked EXCLUSIVE ticket returns its permit but keeps the VO lock and its `seq` position, and tickets waiting at the grant gate enter the core in `(queue, seq)` order.
**Why.** Arrival order differs between nodes and across failover; durable `seq` is the only order every leader agrees on.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::queue_orders_by_seq_not_arrival_order`, `crates/cairn-server/tests/flow_control.rs::same_object_key_runs_in_arrival_order`, `crates/cairn-server/src/flow/tests/ticket.rs::a_nacked_exclusive_ticket_keeps_its_object_lock`, `crates/cairn-server/src/flow/sim/mod.rs::swarm_with_nacks_keeps_seq_order`.
**See.** `docs/guides/flow.md` §1.

### FL-6 — Cancelling a wait never strands a permit

**Statement.** `Claim::drop` drains a permit already in its channel, or else marks its queue entry dead; the core never grants a dead entry and sweeps dead entries when they reach the head or when there are more than 32 and they outnumber live ones, and an emptied queue leaves the table at once. A permit taken from the channel but not yet started carries its own release path until `Permit::begin`, so a task cancelled in that gap returns it; a turn whose SDK makes no progress is abandoned after `sdk_timeout` and its permit returned.
**Why.** Cancellation is routine (client disconnects, leadership loss); a leaked permit is a slot lost until restart.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::abandoned_request_leaves_no_dead_entry`, `crates/cairn-server/src/lease.rs::tests::wait_cancelled_while_relocking_engine_returns_the_turn`, `crates/cairn-server/tests/concurrency.rs::stalled_sdk_abandons_turn_and_returns_permit`, `crates/cairn-server/src/flow/tests/wake.rs::queue_emptied_by_cancellations_leaves_the_table`.
**See.** `docs/guides/flow.md` §1.

### FL-7 — The turn cap belongs to the node

**Statement.** `--max-concurrent-turns` is per node: each range is guaranteed an equal share and may borrow free slots while the node has room and no range is starving (subject to FL-13). Node totals are the sum of the live per-range cells, kept as two lock-free atomics; a range's starving flag stands only while it still has waiters for a turn slot.
**Why.** A fixed per-range split wastes capacity on idle ranges, while unbounded borrowing would let one hot range starve the others.
**Proven by.** `crates/cairn-server/tests/split.rs::max_concurrent_turns_is_per_node`, `crates/cairn-server/src/flow/tests/mod.rs::busy_range_borrows_idle_node_permits`, `crates/cairn-server/src/flow/tests/rate.rs::starvation_flag_does_not_outlive_its_permit_waiter`, `crates/cairn-server/src/flow/sim/mod.rs::seed_1473_starving_flag_outlives_its_waiter`.
**See.** `docs/guides/flow.md` §1.

### FL-8 — A new leader rebuilds VO locks before its first grant

**Statement.** On leader change, counters start at zero and the VO lock book is rebuilt from the store in chunks (at most `RESTORE_CHUNK` = 1 024 index rows per engine hold, releasing between chunks) while the range already serves reads, creates, cancels and draining. The scheduler's grant gate stays closed, and the core grants nothing, until the last chunk is applied. Queue entries of finished invocations never become locks, and an admin cancel writes `failed`, drops the retry row and removes the queue entry in one commit.
**Why.** Every lock-holding entry at restore time belongs to the previous term; granting before the book is complete could hand a held object to a second invocation.
**Proven by.** `crates/cairn-server/tests/recovery_storm.rs::restore_rebuilds_locks_in_chunks_while_serving`, `crates/cairn-server/tests/failpoints.rs::commands_are_served_but_nothing_is_granted_while_locks_are_rebuilt`, `crates/cairn-server/src/flow/tests/ticket.rs::a_closed_gate_grants_nothing`, `crates/cairn-server/tests/flow_control.rs::orphan_entry_does_not_become_permanent_lock`.
**See.** `docs/guides/flow.md` §1; LS-9.

### FL-9 — Releasing a permit is O(1)

**Statement.** `wake` only records free capacity on the released counter or turn cap; `dispatch` then calls waiters one at a time once the eligible round is drained. Releasing a lock wakes the waiters of that lock only.
**Why.** With tens of thousands of waiters, a release that rescans every queue would turn each turn end into a stall.
**Proven by.** `crates/cairn-server/src/flow/tests/wake.rs::releasing_a_turn_does_bounded_work_with_20k_waiting`, `crates/cairn-server/src/flow/tests/mod.rs::wakes_by_specific_counter_not_by_rule`, `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`.
**See.** `docs/guides/flow.md` §1.

### FL-10 — Durable state is re-checked after every engine release

**Statement.** Before a turn runs, the lifecycle core re-reads durable state twice: `Event::Drive` before the permit and `Event::PermitGranted` after it. A terminal invocation only has its source row dropped, and a `Paused` one records the completion without running.
**Why.** Anything may have happened while the engine was released; acting on a stale view would rerun finished work.
**Proven by.** `crates/cairn-server/tests/stale_state.rs::stale_inbound_completion_does_not_rerun_completed_invocation`.
**See.** `docs/guides/flow.md` §1.

### FL-11 — The L0 throttle sheds only new work

**Statement.** From the soft L0 view count (default 5), a range refuses new work with a probability rising to 0.9, at exactly three sources of `turn::admit::Source`: ingress `Invoke`/`Submit` (`429 range_overloaded`, nothing created), `StartCall` in the inbound drain (row kept, never an error on `/internal/inbound`), and local child calls (outbox row kept). One deferral makes the rest of that drain pass defer, preserving creation order; completions, timers, retries, signals and replies are never throttled.
**Why.** Shedding admitted work would strand in-flight invocations; shedding new work lets compaction catch up without losing anything.
**Proven by.** `crates/cairn-server/tests/l0_throttle.rs::l0_throttle_sheds_new_work_but_not_in_flight`, `crates/cairn-server/src/l0.rs::tests::deferral_is_sticky_within_a_pass_and_wakes_the_loop`, `crates/cairn-server/src/l0.rs::tests::hold_rises_at_once_and_falls_only_after_cooldown`.
**See.** `docs/guides/flow.md` §1.

### FL-12 — Rate tier: charge 1 CU up front, settle after the turn

**Statement.** `poll_acquire` checks rate tokens after the concurrency counters and before the turn cap; `secure` charges 1 CU, and the end of the turn charges the remaining `CU − 1` (refunding 1 when the turn used none) before release. A tenant's bucket is node-wide (GCRA on one atomic, no lock on the turn path) with the rate split across the nodes that lead at least one range; an empty bucket answers `429` with `Retry-After`, and a per-term rate loop wakes token waiters.
**Why.** Nobody "releases" tokens, so waiters must be woken by time; charging before the turn bounds overshoot to one turn.
**Proven by.** `crates/cairn-server/src/flow/tests/rate.rs::out_of_tokens_blocks_at_rate_tier_and_clock_wakes`, `crates/cairn-server/src/rate.rs::tests::bucket_starts_full_goes_negative_refills_by_rate_split_by_node`, `crates/cairn-server/tests/tenant_rate.rs::tenant_over_rate_is_throttled_to_rate_and_other_tenant_unaffected`.
**See.** `docs/guides/flow.md` §1.

### FL-13 — Under saturation, a free slot goes to the tenant with the lowest virtual time

**Statement.** The turn wait list is a fair queue with one FIFO per tenant, dequeued by `(priority, vt)` evaluated at dequeue time; `vt` grows by `CU × w` (`w = max_rate / rate`, 1 without a rate), with an extra `0.2 × w` from an invocation's ninth turn on. Cross-range borrowing is allowed only while the tenant's `vt` is within a slack of the lowest `vt` among waiting tenants on the node; new tenants start at that minimum and idle tenants are pulled up, so sleeping earns no credit.
**Why.** A loud tenant spread over many objects must not queue ahead of a quiet one.
**Proven by.** `crates/cairn-server/src/flow/tests/rate.rs::free_permit_goes_to_tenant_with_lowest_vt`, `crates/cairn-server/src/rate.rs::tests::vt_by_weight_new_tenant_at_min_vt_and_pulled_back`, `crates/cairn-server/tests/tenant_rate.rs::loud_tenant_on_many_objects_does_not_queue_ahead_of_quiet_one`, `crates/cairn-server/tests/tenant_rate.rs::sleeping_tenant_wakes_without_starving_others`.
**See.** `docs/guides/flow.md` §1.

### FL-14 — Quarantine never cancels or fails, and only gates new work

**Statement.** A poison-handler identity is `(tenant, service, handler, deployment)`. The default mode `observe` only logs, counts and reports; `hold` creates new invocations directly in `paused` (code 576) in the creating commit while in-flight ones continue; `cooldown` caps the identity to one concurrent turn per range, as a tier after the rate tier and before the turn cap. Quarantine lifts on expiry, a new deployment or an operator `DELETE`, and the turn path reads only a per-range copy without shared locks.
**Why.** Automatic detection can be wrong; it may slow or park new work but must never destroy anything.
**Proven by.** `crates/cairn-server/tests/quarantine.rs::observe_is_the_default_and_changes_nothing`, `crates/cairn-server/tests/quarantine.rs::poison_handler_is_held_across_two_nodes_without_hurting_others`, `crates/cairn-server/tests/quarantine.rs::cooldown_caps_handler_to_one_turn_per_range`, `crates/cairn-server/tests/quarantine.rs::operator_delete_releases_and_does_not_flap_back`.
**See.** `docs/guides/flow.md` §1.

### FL-15 — A drain pass costs what is pending, and skips no row

**Statement.** The outbox, signal and inbound drainers resume from a per-index floor (`SeqFloor`) plus a set of live rows below it, so they never rescan tombstones of drained rows; every live index row with `seq` below the floor is in that set, and every commit that writes below the floor adds to it. Rows left behind are kept in a per-term ledger with a wake reason (lock busy, L0 shed, unknown service, retry later) and reread only when that reason clears. Undecodable rows are never deleted (their waiter is paused with 577), and a destination that times out trips a per-destination breaker so a hung peer cannot hold the sender's inbound.
**Why.** Drain cost proportional to history makes every range slower with age; deleting a row the drainer cannot read loses customer data.
**Proven by.** `crates/cairn-engine/tests/outbox_seq.rs::empty_drain_pass_skips_tombstones_of_drained_rows`, `crates/cairn-engine/tests/outbox_seq.rs::late_commit_row_below_floor_is_still_seen`, `crates/cairn-server/tests/failpoints.rs::undecodable_inbound_row_is_kept_and_pauses_its_waiter`, `crates/cairn-server/tests/dead_destination.rs::hung_destination_does_not_hold_the_senders_inbound`.
**See.** `docs/guides/flow.md` §1.

### FL-16 — Each term runs a state-reconciling rescue net

**Statement.** Any path that may drop the wake of an in-flight invocation marks `drainer::Rescue` in RAM; the per-term `rescue_loop` re-dispatches an id that is still `running`, has no scheduled retry and that nobody is working on. Every turn task holds a typed `WakeDuty` that is acked only once a durable wake exists; dropping it unacked (an error, a panic, a cancelled task) marks the net. Marks back off per id from 0.5 s doubling to 30 s, and an id marked 50 times in a row or for an hour is paused with 577 (`INFRA_STUCK`).
**Why.** A forgotten error path must degrade into a delayed retry, never into an invocation that silently stops.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::panicking_turn_is_rescued`, `crates/cairn-server/tests/failpoints.rs::resume_turn_that_panics_is_rescued`, `crates/cairn-server/tests/failpoints.rs::rescue_backs_off_per_invocation`, `crates/cairn-server/tests/failpoints.rs::invocation_stuck_on_infra_is_paused_577`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-17 — Every invocation that entered a turn has a durable wake

**Statement.** The commit that moves an invocation to `Running` also writes a watch row in the retry index (due `now + sdk_timeout + WATCH_MARGIN`, carrying the counted attempt) unless another durable wake already exists. Ending the turn is one commit that removes the watch row and writes the next retry if any, and keeps the watch row if no retry could be scheduled; a turn that dies anywhere is woken when its watch row fires.
**Why.** The process, the task or a single `?` may vanish mid-turn; a durable row is the only wake that survives all of them.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::turn_lost_after_running_is_woken_by_its_watch`, `crates/cairn-server/tests/lifecycle.rs::turn_dying_after_running_still_counts_attempt`, `crates/cairn-engine/tests/outbox_seq.rs::watch_row_fires_like_retry_and_clears_with_final_commit`, `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-18 — A durable progress floor never passes an unprocessed row

**Statement.** The scan floors of the due-time indexes (timer, retry, purge, delayed) and the ordered indexes are stored in one derived-region key, written inside an existing commit and stamped with `writer_epoch`; a floor is trusted only by the writer immediately following. No due-time index row on the store is below its floor and no live ordered row has a `seq` below its floor, because a commit that writes below a floor lowers it atomically and floors advance only past rows that earlier commits deleted. The per-index `seq` counter never restarts below the highest `seq` ever assigned.
**Why.** Floors let a new leader skip tombstones instead of scanning history, but a floor above a live row would hide it forever.
**Proven by.** `crates/cairn-engine/tests/progress_floors.rs::durable_floor_never_passes_a_row_under_random_crash`, `crates/cairn-engine/tests/progress_floors.rs::row_written_below_floor_then_crash_still_seen`, `crates/cairn-engine/tests/progress_floors.rs::floor_dropped_when_unaware_writer_interleaves`, `crates/cairn-engine/tests/progress_floors.rs::dropped_floor_does_not_pull_seq_below_old_tombstone`.
**See.** `docs/guides/flow.md` §1.

### FL-19 — An attempt is counted only when the turn enters Running

**Statement.** `fire_retries` moves a retry row forward before running it but keeps `attempt`; the `Running` commit rewrites it with `attempt + 1`, or with `1` at the current journal length if the journal grew since the row was written. A turn that stops before `Running` (throttled, a parked ticket lost on leader change, a failed enqueue, or a preflight finding the deployment not yet loaded) spends no attempt.
**Why.** Otherwise throttling or repeated leader changes could exhaust `maxAttempts` without the SDK ever being called.
**Proven by.** `crates/cairn-server/tests/flow_control.rs::background_turn_parked_for_budget_does_not_spend_attempt`, `crates/cairn-server/tests/lifecycle.rs::parked_first_turn_does_not_spend_attempt`, `crates/cairn-server/tests/lifecycle.rs::preset_schedule_recounted_when_journal_grows`, `crates/cairn-server/tests/lifecycle.rs::unloaded_deployment_does_not_spend_attempt`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-20 — Background work never dispatches a duplicate turn

**Statement.** An invocation is busy when its lock is held or a dispatched turn has not finished (`Runner::is_queued`). Retry firing, `recover`, the rescue net, timer delivery and the drainer's deliver-and-dispatch paths all ask the same `Attendance::busy()` of the lifecycle core; for a busy invocation they only defer the retry row or record the timer or completion, without dispatching a second turn.
**Why.** A turn may wait for the engine longer than the retry backoff; without the second half of the check the SDK would be called twice.
**Proven by.** `crates/cairn-server/tests/flow_control.rs::delivered_turn_without_lock_is_not_delivered_twice`, `crates/cairn-server/tests/lifecycle.rs::two_timers_on_one_tick_deliver_one_turn`, `crates/cairn-server/tests/lifecycle.rs::drainer_does_not_redeliver_delivered_turn`, `crates/cairn-lifecycle/src/tests/retry.rs::tick_timer_table`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-21 — Backoff counts every fire, not only counted attempts

**Statement.** A retry row carries `fires`, the number of times it fired since the last counted attempt (encoded only by `cairn_lifecycle::codec::RetryRow`, absent means 0), and the next fire is scheduled at `backoff(attempt + fires)` using the handler's retry policy when it declares one. The `Running` commit resets `fires` to 0; `maxAttempts` still counts attempts only (FL-19).
**Why.** Turns throttled or cancelled before `Running` would otherwise refire at the base backoff forever, which under continuous leader changes collapses throughput.
**Proven by.** `crates/cairn-server/tests/flow_control.rs::fire_not_counted_as_attempt_still_backs_off`, `crates/cairn-server/tests/lifecycle.rs::retry_after_error_follows_handler_policy`, `crates/cairn-lifecycle/src/tests/retry.rs::fire_retries_table`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-22 — A warm state machine is a cache; the stored journal is the truth

**Statement.** The drainer may settle a completion on a state machine kept in the range engine's RAM, but only when the stored journal tail matches it (entry `L` absent, entries `L − 1` and 0 byte-equal, same hash-chain head); otherwise it reloads cold. Taking a machine removes it, so any error or ambiguous commit leaves no cached machine, and load, create, restart, purge and termination evict the id; the cache is per range, LRU, capped at 32 MiB. Debug and `testkit` builds cross-check every warm use against a cold reload.
**Why.** Replaying the caller's whole journal per completion is quadratic for fan-out, but a stale cache must never decide anything.
**Proven by.** `crates/cairn-server/tests/settle.rs::settles_of_one_drain_round_replay_the_callers_journal_once`, `crates/cairn-runtime/tests/warm_lemma.rs::completion_on_a_replayed_machine_equals_replaying_the_longer_journal`, `crates/cairn-engine/src/engine/warm.rs::tests::a_longer_journal_makes_the_warm_machine_stale`, `crates/cairn-engine/src/engine/warm.rs::tests::cross_check_catches_what_the_tail_cannot_see`.
**See.** `docs/guides/flow.md` §1.

### FL-23 — The drainer wakes a suspended caller only when its wait tree is satisfied or unknown

**Statement.** The engine remembers in RAM the `Suspension` tree of an invocation's last suspend. When a completion arrives for a `suspended` invocation whose tree is known to remain unsatisfied, the completion is recorded and its source row removed in one commit and the invocation stays `suspended`; if the tree is satisfied or unknown, the invocation is woken. Timers and signals always wake, and the tree is only a hint: neither the wire nor the bucket layout changes.
**Why.** A parent waiting on many children should not run one turn per child completion.
**Proven by.** `crates/cairn-server/tests/settle.rs::fanout_parent_runs_at_most_three_turns`, `crates/cairn-server/tests/settle.rs::parent_finishes_after_losing_its_waiting_tree_between_two_completions`, `crates/cairn-lifecycle/src/tests/retry.rs::drainer_wakes_only_when_wait_tree_satisfied`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-24 — Suspended tenants get no permits; limited tenants get no new work

**Statement.** A tenant's `state=` reaches each range as an atomic snapshot. For `limited` and `suspended`, `turn::admit` refuses ingress, `restart` and schedule sources (`429 tenant_limited` / `403 tenant_suspended`; schedule fires are skipped) while child calls and `StartCall` are still created. `suspended` also blocks the first scheduler tier: background turns park until the tenant is lifted, and a turn re-checks right before every SDK call so a suspended tenant's endpoint is never called; `deleting`/`purged` behave like `suspended` and also refuse the tenant's own credentials (`403 tenant_deleting`). An unknown value means `suspended`.
**Why.** Suspension must stop all outbound calls to a tenant's endpoint without losing or failing its work.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::suspended_tenant_gets_no_permit`, `crates/cairn-server/tests/tenant_state.rs::unsuspend_runs_parked_turns_at_once`, `crates/cairn-server/tests/failpoints.rs::suspended_tenant_turn_granted_before_suspension_never_calls_the_sdk`, `crates/cairn-server/tests/tenant_state.rs::deleting_refuses_access_and_reverts_until_purging`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-25 — SDK commands by id touch only invocations of the same tenant

**Statement.** `attach`, `getOutput` and `signal` (including cancel and awakeables) carry ids supplied by the SDK, so the engine checks the target's tenant (`Engine::visible_status`/`visible_to`); a different or unreadable tenant takes the "does not exist" path byte for byte (404 for attach and peek, a silent drop for signals). Cross-range `Watch`/`DeliverSignal` messages carry the caller's tenant for the destination range to check.
**Why.** Invocation ids are not secrets; an SDK must not be able to read or signal another tenant's invocation by guessing one.
**Proven by.** `crates/cairn-server/tests/tenant_isolation.rs::alpha_sdk_cannot_reach_bravo_by_id`, `crates/cairn-server/tests/interrange.rs::foreign_tenant_watch_and_signal_across_nodes_answer_not_found`, `crates/cairn-engine/src/interpartition.rs::tests::watch_and_signal_carry_tenant`.
**See.** `docs/guides/flow.md` §1.

### FL-26 — A tenant never holds more than its turn cap on a node

**Statement.** `--max-turns-per-tenant` caps the running turns of one tenant across all ranges of a node (`--profile saas`: a quarter of `--max-concurrent-turns`, at least 1). A node-wide meter increments in `secure` and decrements in `release_running`, and each range returns exactly what it added on reset and on drop. The tier sits after the concurrency counters and before the rate tier, is not a budget (`202`, background turns keep waiting), and is soft across engine threads.
**Why.** A tenant with a hung endpoint or long model steps must not occupy every turn slot of a shared node.
**Proven by.** `crates/cairn-server/src/flow/tests/mod.rs::tenant_turn_cap_blocks_across_ranges_and_wakes`, `crates/cairn-server/src/flow/tests/mod.rs::dropped_range_returns_tenant_turns`, `crates/cairn-server/tests/tenant_turns.rs::one_tenant_never_holds_more_than_its_turn_share`.
**See.** `docs/guides/flow.md` §1.

### FL-27 — Journal memory held by SDK turns is bounded

**Statement.** With `--replay-ram-budget-bytes` (default 512 MiB, `0` disables), a turn holds its journal's size from the cold load to the end of the turn; when the budget is full, the turn is rescheduled after 1 s without calling the SDK or spending an attempt. One tenant holds at most a quarter, while small journals (at most `min(1 MiB, cap/64)`) and a tenant's first turn always run. Per invocation, `max-journal-bytes=` pauses with 580 an invocation whose own entry would exceed it, while entries from outside (completions, signals) are always written.
**Why.** Replaying large journals concurrently can exhaust node memory; external entries were already acknowledged, so dropping them would lose data.
**Proven by.** `crates/cairn-server/src/replay_budget.rs::tests::holds_and_refuses`, `crates/cairn-server/src/replay_budget.rs::tests::two_tenants_do_not_starve_a_third`, `crates/cairn-server/tests/replay_budget.rs::full_budget_reschedules_without_calling_sdk`, `crates/cairn-server/tests/tenant_limits.rs::journal_bytes_cut_keeps_external_signal`.
**See.** `docs/guides/flow.md` §1.

### FL-28 — A task waiting for a permit is small

**Statement.** In a dispatched turn future only the permit wait sits inline; the pre-permit gate, the throttled path and the post-permit turn are boxed and allocated when reached. Background permit waiters are parked tickets rather than tasks (FL-30), so the per-waiter cost is the ticket.
**Why.** Futures are allocated at their largest state; with hundreds of thousands of waiters an inline turn body costs gigabytes.
**Proven by.** `crates/cairn-server/src/actor/background.rs::tests::waiting_turn_task_is_small`, `crates/cairn-server/src/actor/background.rs::tests::parked_waiter_is_data`.
**See.** `docs/guides/flow.md` §1.

### FL-29 — A drainer's engine hold is bounded, not proportional to backlog

**Statement.** A drain pass releases the engine between chunks: after at most 64 inbound or signal rows, after each full outbox round (32 rows), and whenever the current hold exceeds `DRAIN_HOLD_BUDGET` (20 ms). Release points lie between rows: each row asks the lifecycle core, reads state and commits within a single hold.
**Why.** A backlog must not freeze commands and turns of the range behind one long drain hold.
**Proven by.** `crates/cairn-server/tests/recovery_storm.rs::drain_releases_the_engine_between_chunks_whatever_the_backlog`, `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-30 — Background permit waiters are data, not tasks

**Statement.** A dispatched background turn passes a bounded admission gate (64 concurrent tasks), takes the invocation lock, runs the pre-permit gate and asks for a permit synchronously; if it is not granted at once, it parks a `flow::Ticket` in the scheduler core and the task ends, releasing the invocation lock. A granted ticket's turn claims the invocation lock on first poll or nacks (permit returned, VO lock kept, same `seq`). There is at most one live ticket per invocation, adopted by any newer request, and tickets are term RAM, re-dispatched by the next term's `recover`.
**Why.** Tens of thousands of waiters must cost neither memory per task nor periodic engine holds.
**Proven by.** `crates/cairn-server/tests/flow_control.rs::permit_waiter_is_not_a_task`, `crates/cairn-server/src/flow/tests/ticket.rs::a_nacked_ticket_waits_for_the_lock_then_asks_again`, `crates/cairn-server/src/flow/tests/ticket.rs::a_new_request_adopts_the_parked_ticket`, `crates/cairn-server/src/flow/tests/ticket.rs::close_leaves_no_ticket_behind`.
**See.** `docs/guides/flow.md` §1; M4.

### FL-31 — At term start, commands go before queued background work, with a bound

**Statement.** During a term's opening window (until lock restore and the first `recover` finish), background tasks (`tick`, `drain`, `query_build`, `hint_sweep`, `recover`, `rescue`, `restore`) yield the engine lock to waiting commands, each at most `YIELD_MAX` (16) times; scheduler-dispatched turns stay FIFO, and outside the window the lock is plain FIFO. A final reply consumes its `Lease` and never reacquires the engine just to release it.
**Why.** A cold leader would otherwise answer its first commands only after every startup chore, while unbounded yielding would starve lock restore.
**Proven by.** `crates/cairn-server/src/lease.rs::tests::command_goes_before_queued_background_at_term_start`, `crates/cairn-server/src/lease.rs::tests::background_yields_to_at_most_yield_max_commands`, `crates/cairn-server/src/lease.rs::tests::final_reply_does_not_wait_to_reacquire_the_engine`, `crates/cairn-server/src/lease.rs::tests::runner_turn_does_not_jump_the_restore`.
**See.** `docs/guides/flow.md` §1.

### FL-32 — At most one live permit per invocation; only its holder releases the VO lock

**Statement.** Every permit request of an invocation runs under its invocation lock, except granted parked tickets, which keep their permit in the scheduler's ledger until their turn claims the lock. A new request for the same invocation in that window adopts the ticket and takes that very permit instead of acquiring a second one. Tripwires `flow::second_live_permit` and `flow::lock_released_by_non_holder` guard the branches that must never run.
**Why.** A second permit's release would free the VO lock the first one still uses, letting the next waiter run concurrently on an EXCLUSIVE object.
**Proven by.** `crates/cairn-server/src/flow/tests/ticket.rs::a_signal_adopting_a_granted_ticket_takes_its_permit`, `crates/cairn-server/src/flow/tests/ticket.rs::a_new_ticket_adopting_a_granted_ticket_takes_its_permit`, `crates/cairn-model/tests/m4.rs::signal_to_a_granted_ticket_holds_every_property`.
**See.** `docs/guides/flow.md` §1; M4.

## LF — send tickets and the latency floor

`LF-*` codes narrow the durability ticket so that cross-range sends, inbound acknowledgements and
`/invoke` answers wait only for what they depend on. They refine FL-2 and IR-1; the shared
mechanism is `SendTicket` and `send_doubt` in `crates/cairn-store/src/writer/mod.rs`.

### LF-1 — A send ticket covers exactly the durable send rows

**Statement.** Every outbox or signal row handed to the transport was committed at or below the round's send-ticket mark, and the ticket is issued only once that mark is durable; the mark is the latest commit that wrote a send row (`keyspace::is_send_key`, tracked by `PartitionWriter::commit`). The drainer does not wait for a turn-ending commit or other turns' commits. The tripwire `Sender::check_ir1` refuses to send a round whose rows are not durable and counts `cairnflow_invariant_violations_total{check="ir1"}`.
**Why.** Waiting for the full write tip would add every unrelated commit's latency to each cross-range message.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::outbox_send_does_not_wait_for_the_turn_end_commit`, `crates/cairn-store/tests/range_store.rs::send_ticket_covers_exactly_the_commits_with_sends`, `crates/cairn-server/tests/failpoints.rs::remote_peer_never_sees_a_row_before_it_is_durable`.
**See.** `docs/guides/flow.md` §1 (FL-2); IR-1.

### LF-2 — Only the transport accepts a send ticket

**Statement.** A `SendTicket` is accepted only by `Transport::send`/`send_batch`; client replies and SDK sends still require a full `Committed`. A `Committed` converts to a `SendTicket`, and the reverse is a compile error.
**Why.** The narrow ticket proves only that send rows are durable, not the rest of the range's state.
**Proven by.** The `compile_fail` doctest on `SendTicket` in `crates/cairn-store/src/writer/mod.rs`.
**See.** `docs/guides/flow.md` §1 (FL-2).

### LF-3 — A doubtful send commit forces a barrier

**Statement.** If a commit carrying send rows returns `Err` or is cancelled after submission, `send_doubt` is set and no send ticket is issued until a barrier submitted after it (`PartitionWriter::send_barrier`) is durable.
**Why.** A submitted batch may still be applied after its handle is lost; only a later durable barrier proves the prefix including it is durable.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::cancelled_commit_with_an_outbox_row_forces_a_barrier`, `crates/cairn-store/tests/failpoints.rs::submitted_outbox_batch_with_lost_handle_is_applied_but_not_waited_for`, `crates/cairn-server/tests/failpoints.rs::cancelled_outbox_commit_is_not_sent_before_it_is_durable`.
**See.** `docs/guides/flow.md` §1 (FL-2).

### LF-4 — /internal/inbound answers 200 only after the receiving commit is durable

**Statement.** `/internal/inbound` returns `200` only once the commit that accepted the messages is durable, even though the drainer is poked as soon as that commit is submitted and may already have acted on the messages at memory level. The `200` waits only for that commit, not for processing (IR-7).
**Why.** The sender deletes its outbox row on `200`; acknowledging a message that a crash could lose would drop it.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::early_drained_inbound_survives_split_quiesce`, `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`.
**See.** `docs/guides/flow.md` §1; M4.

### LF-5 — Effects derived from a received message follow its durable commit

**Statement.** Every external effect derived from a just-received message comes after the receiving commit is durable: calling the SDK requires a durable `Running` commit later in the WAL, answering a client requires `Committed`, and forwarding requires a `SendTicket` covering a later commit. If the receiving commit is lost, every later write is lost with it, and the sender, having no `200`, resends (IR-2).
**Why.** Acting early on undurable input is safe only if every visible consequence shares its fate.
**Proven by.** `crates/cairn-server/tests/interrange.rs::inbound_message_and_first_running_share_one_wal_object`, `crates/cairn-server/tests/failpoints.rs::drained_inbound_on_a_lost_leader_runs_once`, `crates/cairn-server/tests/failpoints.rs::early_drained_inbound_survives_split_quiesce`.
**See.** `docs/guides/flow.md` §1; M4.

### LF-7 — The /invoke completion notifier is only a hint

**Statement.** A synchronous `/invoke` registers in the range's waiter book before its first status read and wakes on a notification or on a safety poll grid, up to `--invoke-wait-ms`. The writer's status hook notifies on every terminal path at submit time, but the final answer is still produced only through the durable ticket and read barrier; a lost or spurious notification costs one extra poll, and every waiter entry is removed when its request ends.
**Why.** Notification lowers latency without becoming a second source of truth or a leak.
**Proven by.** `crates/cairn-server/tests/invoke_wait.rs::final_answer_arrives_without_waiting_for_the_poll_grid`, `crates/cairn-server/tests/invoke_wait.rs::every_ending_wakes_the_waiter`, `crates/cairn-server/tests/invoke_wait.rs::waiters_are_released_on_every_exit`, `crates/cairn-server/tests/invoke_wait.rs::waiter_wakes_and_reasks_when_its_leader_steps_down`.
**See.** `docs/guides/flow.md` §1; D11.

## IR — inter-range messaging

`IR-*` codes govern messages between ranges: calls, replies, signals and cancels written to a
range's outbox and delivered to the destination range's inbound over `/internal/inbound`
(`crates/cairn-server/src/turn/outbox.rs`, `crates/cairn-server/src/transport.rs`,
`crates/cairn-engine/src/interpartition.rs`).

### IR-1 — An outbox row is durable before the peer sees it

**Statement.** A row gathered by the outbox drainer reaches a send task only after the round's send ticket (LF-1), so the destination never sees a row that is not durable at the sender, including a `Reply` row whose turn is still waiting for durability.
**Why.** A peer acting on a row the sender later loses would create effects with no cause.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::remote_peer_never_sees_a_row_before_it_is_durable`, `crates/cairn-server/tests/transport_engine.rs::transport_send_does_not_hold_engine`.
**See.** `docs/guides/flow.md` §1 (FL-2); LF-1.

### IR-2 — Outbox rows are deleted only after a durable 200

**Statement.** The sender deletes an outbox row only after `200`, and `200` means the message is durable at the receiver (LF-4). A batch request is all-or-nothing: if the receiver fails anywhere, no message of the batch is written and the sender keeps every row.
**Why.** At-least-once delivery with content-keyed deduplication (IR-5) needs the sender to keep a row until the receiver can no longer lose it.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::inbound_during_split_quiesce_is_neither_lost_nor_duplicated`, `crates/cairn-server/tests/failpoints.rs::receiver_failing_mid_batch_writes_no_message_of_it`.
**See.** `crates/cairn-server/src/transport.rs`; LF-4.

### IR-3 — Messages to one destination keep their order

**Statement.** Rows to the same destination range are sent in generation order through one sequential queue per destination, and an oversized queue goes as consecutive batches. If a send fails, that row and every later row to the same destination stay behind.
**Why.** A `Cancel` for a callee must never arrive before the `StartCall` that created it.
**Proven by.** `crates/cairn-server/tests/dead_destination.rs::failed_send_keeps_later_rows_to_the_same_destination_in_order`, `crates/cairn-server/tests/dead_destination.rs::cancel_reaches_the_callee_after_its_start_call`, `crates/cairn-server/tests/dead_destination.rs::oversized_queue_goes_as_consecutive_batches_and_a_failed_one_keeps_its_rows`.
**See.** `crates/cairn-server/src/turn/outbox.rs`.

### IR-4 — A dead destination holds back only its own rows

**Statement.** A destination that fails or hangs keeps only the rows addressed to it; rows and signals to other destinations keep flowing.
**Why.** One unreachable range must not stall all cross-range traffic of its senders.
**Proven by.** `crates/cairn-server/tests/dead_destination.rs::dead_destination_does_not_stall_other_outbox_rows`, `crates/cairn-server/tests/dead_destination.rs::signal_rows_to_dead_destination_do_not_block_others`, `crates/cairn-server/tests/dead_destination.rs::hung_destination_does_not_hold_other_destinations`.
**See.** `crates/cairn-server/src/transport.rs`.

### IR-5 — Deduplication rests on a key derived from message content

**Statement.** The receiver stores each inbound message under a key derived from its content (`InterPartition::inbound_key`), so a resent message lands on the same key and is accepted at most once, while different calls to the same callee get different keys.
**Why.** Resends after a lost `200` are expected (IR-2); deduplication must not depend on sender-side state that a crash can lose.
**Proven by.** `crates/cairn-engine/src/interpartition.rs::tests::resend_same_message_same_key`, `crates/cairn-engine/src/interpartition.rs::tests::two_calls_same_callee_different_keys`, `crates/cairn-server/tests/failpoints.rs::inbound_during_split_quiesce_is_neither_lost_nor_duplicated`.
**See.** `crates/cairn-engine/src/interpartition.rs`; M4.

### IR-6 — The receiver checks range and table version before writing

**Statement.** Before writing, the receiver checks that each message belongs to its range under the current partition table and rejects it with `409 wrong_range` otherwise (D23); the sender applies the newer table from the response and resends within the same send.
**Why.** After a split or merge, writing a message into a range that no longer owns its key would strand it.
**Proven by.** `crates/cairn-server/tests/wrong_range.rs::stale_sender_recovers_after_table_change`, `crates/cairn-server/tests/wrong_range.rs::inbound_rejected_while_range_retiring`.
**See.** `crates/cairn-server/src/transport.rs`; D23.

### IR-7 — The receiver does not process messages while the sender waits

**Statement.** `/internal/inbound` only commits the messages and answers; processing happens in the receiver's drainer, never synchronously inside the request. The drainer may read a message before it is durable, but the `200` does not wait for it.
**Why.** Synchronous processing would chain one range's latency into another's and couple their engines.
**Proven by.** `crates/cairn-server/tests/single_reader.rs::inbound_processed_without_tick`, `crates/cairn-server/tests/single_reader.rs::invoke_reply_not_blocked_by_inbound_processing`.
**See.** `crates/cairn-server/src/ingress/internal.rs`; LF-4.

### IR-8 — Inter-range sends never hold the engine

**Statement.** Inter-range sends run in `IoSet` tasks with the engine released; this is D16 and FL-1 applied to the outbox, enforced by the `Released` type.
**Why.** A slow destination must not block the sending range's turns.
**Proven by.** `crates/cairn-server/tests/transport_engine.rs::transport_send_does_not_hold_engine`, the `compile_fail` doctest on `Released` in `crates/cairn-server/src/lease.rs`.
**See.** `docs/guides/flow.md` §1 (FL-1); D16.

### IR-9 — Callee ids are deterministic

**Statement.** Reprocessing the same outbox row yields the same callee id on every binary of the cluster, independent of the partition count and of later changes to call-placement gates. The placement rule is a pure function of the call's content and a durable marker written in the caller's entry 0; v5 rows keep the old derivation forever.
**Why.** A different id on redrain would create a second callee for one logical call.
**Proven by.** `crates/cairn-store/src/cluster.rs::tests::callee_id_independent_of_partition_count`, `crates/cairn-store/src/cluster.rs::tests::callee_ids_with_the_gate_off_are_pinned`, `crates/cairn-engine/tests/call_prefix.rs::rows_written_before_the_gate_keep_the_old_rule_on_replay`, `crates/cairn-engine/tests/call_placement.rs::spread_calls_of_a_marked_caller_keep_the_old_rule`.
**See.** `crates/cairn-engine/src/engine/effects.rs`.

### IR-10 — Keyed entity rows live under the owner's partition key

**Statement.** State, promise and idempotency rows of keyed calls (scope, object key or idempotency key) live under `owner_pk = partition_key_for_target(…)`; only unkeyed calls may be colocated by the caller-prefix rule.
**Why.** Changing where keyed rows live would require moving existing customer data.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::clone_union_and_discard_follow_the_pk_span`, `crates/cairn-engine/tests/keyspace_layout.rs::projection_keeps_every_owned_row`.
**See.** `crates/cairn-engine/src/engine/effects.rs`.

## AP — agent primitives: side channel, streams and artifacts

`AP-*` codes govern the agent-facing primitives carried on the side channel of the SDK wire: live
and durable streams (`$log`, `$trace`, user streams, read over SSE) and artifacts (immutable blobs
referenced from the journal). Code lives in `crates/cairn-server/src/streams.rs`,
`crates/cairn-server/src/turn/sdk.rs` and `crates/cairn-store/src/artifact.rs`.

### AP-1 — An artifact is durable before anything points to it

**Statement.** `ArtifactPut` stops at the turn layer: the object `artifacts/<tenant>/<id>/<epoch>/<sha>` is written with the engine released (`Create`, `cas::put_resolving`), then the `Artifacts` row is committed, before the next frame is processed; journal entries that reference the artifact come later on the same writer.
**Why.** A reference to a missing object would be a permanent read failure.
**Proven by.** `crates/cairn-server/tests/streams.rs::artifact_put_lands_before_the_output_and_reads_back`, `crates/cairn-server/tests/failpoints.rs::artifact_orphaned_between_put_and_commit_is_swept`.
**See.** `docs/guides/flow.md` §1.

### AP-2 — Artifacts are deleted only through the trash with proof

**Statement.** Purging an invocation deletes its rows, not its objects; the artifact sweep holds unreferenced `(id, epoch)` directories in place in the trash and deletes them only after the retention period, re-checking `proof::artifacts_unreferenced` under the engine and `DeleteProof::purged_invocation`.
**Why.** Deleting customer data is irreversible; it requires proof and a recovery window.
**Proven by.** `crates/cairn-server/tests/streams.rs::purged_artifacts_are_held_in_place`, `crates/cairn-store/tests/trash.rs::hold_in_place_keeps_objects_until_purged_with_proof`, `crates/cairn-store/tests/proof.rs::purged_invocation_proof_needs_the_owning_range_no_row_and_age`.
**See.** `docs/guides/flow.md` §1.

### AP-3 — A durable stream's acknowledged prefix survives until purge

**Statement.** For a durable stream, every record at or below `durableOffset` can be read back, in order, until the invocation is purged, including after the leader is killed.
**Why.** `durableOffset` is a promise to readers that resuming from it never skips data.
**Proven by.** `crates/cairn-server/tests/streams.rs::durable_prefix_survives_leader_kill`.
**See.** `crates/cairn-server/src/streams.rs`.

### AP-4 — Side-channel frames add no durability wait to a turn

**Statement.** `StreamAppend` frames stop at the turn layer and go into the range's `StreamHub`; they never call `engine.step` or become journal entries, and a batch of only side-channel frames triggers neither `sync` nor `send`. The hub is per range and per term with a node-wide RAM cap on one atomic, and streams are read only at the leader.
**Why.** Logging and streaming must be free on the latency path of a turn.
**Proven by.** `crates/cairn-server/tests/streams.rs::appends_add_no_commit_to_the_turn`, `crates/cairn-server/tests/streams.rs::bidi_records_reach_readers_mid_run_and_resume_by_offset`, `crates/cairn-server/tests/streams.rs::leader_change_sends_moved_then_reset`.
**See.** `docs/guides/flow.md` §1.

### AP-5 — Replay does not re-emit records of completed steps

**Statement.** The SDK knows when it is replaying: logs, spans and stream records around a step that is already in the journal are not emitted again.
**Why.** Re-emitting on every replay would duplicate logs and traces once per turn.
**Proven by.** the replay-awareness test of `sdk/typescript/test/logs.test.ts` (logs and spans around steps already in the journal are not emitted again).
**See.** `sdk/typescript/src/handler.ts`.

### AP-7 — Streams and artifacts never return another tenant's data

**Statement.** Stream, log and artifact reads check the invocation's tenant against the caller; another tenant's invocation answers `404`, indistinguishable from a missing one, including through forwarding.
**Why.** Existence of another tenant's invocation must not leak.
**Proven by.** `crates/cairn-server/tests/tenant_isolation.rs::sse_and_forwarding_keep_tenants_apart`.
**See.** `crates/cairn-server/src/ingress/streams.rs`, `crates/cairn-server/src/ingress/artifacts.rs`.

### AP-8 — An artifact directory is deleted only when its incarnation is dead

**Statement.** `<id>/<epoch>` is removed only when the range leader proves that the invocation row is absent or carries a different epoch and the directory is older than `min_age`. Epochs are drawn lazily from `entropy` at the first artifact of each incarnation, so an id recreated after purge has a new epoch.
**Why.** Ids are deterministic and reused after retention; without epochs, cleaning the old incarnation would delete the new one's artifacts.
**Proven by.** `crates/cairn-server/tests/streams.rs::recreated_id_keeps_its_artifacts`, `crates/cairn-store/tests/proof.rs::purged_invocation_proof_needs_the_owning_range_no_row_and_age`.
**See.** `docs/guides/flow.md` §1.

### AP-9 — The artifact row commits within T_put of the PUT

**Statement.** The `Artifacts` row commits within `T_PUT` (5 minutes) of the start of the PUT; if that deadline passes, the frame re-PUTs the object by overwrite (refreshing its age) before committing. The sweep only touches directories older than `min_age` ≥ 2 × `T_PUT`.
**Why.** The sweep must never see an object old enough to delete while its row is still on its way to commit.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::slow_commit_reputs_artifact`.
**See.** `docs/guides/flow.md` §1.

### AP-10 — restart-as-new copies artifacts before the restart commit

**Statement.** `turn/life.rs::copy_artifacts` copies referenced artifacts server-side to the new id with the engine released and commits their rows before `Engine::restart_in`; `continueAsNew` copies only artifacts whose sha appears in the new input.
**Why.** The new invocation must be able to read every artifact it references even after the old one is purged.
**Proven by.** `crates/cairn-server/tests/streams.rs::purging_old_after_restart_keeps_new_readable`.
**See.** `docs/guides/flow.md` §1.

### AP-11 — Stream records are identified by (step, attempt, k) and deduplicated

**Statement.** Within one stream, `(step, attempt, k)` is unique and ordered; the server drops a record that does not advance past the last one of its step, and offsets are assigned by the runtime and stay dense.
**Why.** Replays and retries resend records; identity by position lets the server drop duplicates without losing new records.
**Proven by.** `crates/cairn-server/src/streams.rs::tests::offsets_are_dense_and_duplicates_are_dropped_not_lost`, `crates/cairn-server/tests/streams.rs::resumed_step_records_are_not_dropped_as_duplicates`.
**See.** `crates/cairn-server/src/streams.rs`.

### AP-12 — durable: true is reported only once a record is durable

**Statement.** Durable-stream records ride the range's next batch, and SSE reports `durable: true` for a record carried in batch `seq` only when `durable_seq ≥ seq`. A new term seeds offsets from the last chunk in the database before its first record.
**Why.** A reader may checkpoint on `durable: true`; reporting it early would break AP-3.
**Proven by.** `crates/cairn-server/tests/streams.rs::durable_records_ride_the_next_commit_and_never_commit_alone`.
**See.** `docs/guides/flow.md` §1.

### AP-13 — Stream and query reads never open a follower reader

**Statement.** Streams, logs and stream-backed queries are read only at the range leader; another node answers `503` with the leader and opens no reader, unless the request asks to be forwarded, in which case it still runs at the leader.
**Why.** Follower readers on every range would multiply object-store reads and keep readers alive indefinitely.
**Proven by.** `crates/cairn-server/tests/streams.rs::non_leader_answers_503_with_leader_and_opens_no_reader`, `crates/cairn-server/tests/forward.rs::sse_follows_through_forwarding`.
**See.** `crates/cairn-server/src/streams.rs`.

### AP-14 — A failing step keeps the records sent before it failed

**Statement.** When a step fails while the node is alive, the commit that records the failure carries every pending durable-stream record (`$log`, `$trace`) sent before the failure.
**Why.** The logs of a failing step are exactly the ones needed to debug it.
**Proven by.** `crates/cairn-server/tests/streams.rs::failing_step_commit_carries_pending_records`.
**See.** `crates/cairn-server/src/streams.rs`.

### AP-15 — Quiesce loses no accepted durable-stream record

**Statement.** When a range quiesces (split, lease handover), pending durable-stream records ride the barrier batch written by `seal`, adding no extra PUT.
**Why.** Splits and handovers are routine and must not drop records already accepted.
**Proven by.** `crates/cairn-server/tests/streams.rs::quiesce_keeps_pending_durable_records`.
**See.** `crates/cairn-server/src/streams.rs`.

### AP-16 — No commit exists only to make a stream durable

**Statement.** Durable-stream records ride commits that happen anyway (at most one `Streams` row per stream per batch); the only exceptions are an empty commit when more than 256 KiB is pending on the range and, when the handler opts in, a `logFlushMs` timer commit.
**Why.** An idle range with buffered logs must not generate object-store writes on its own.
**Proven by.** `crates/cairn-server/tests/streams.rs::durable_records_ride_the_next_commit_and_never_commit_alone`, `crates/cairn-server/tests/streams.rs::pending_over_256_kib_flushes_without_a_turn_commit`, `crates/cairn-server/tests/streams.rs::log_flush_ms_makes_waiting_records_durable`.
**See.** `docs/guides/flow.md` §1.

## CG — client calls, waits, timestamps and server-side schedules

`CG-*` codes cover client-call idempotency, combinator semantics shared by SDK and runtime,
invocation timestamps and server-side schedules (`crates/cairn-schedule`,
`crates/cairn-server/src/turn/schedule.rs`, the `Schedules` table and `ScheduleDue` index).

### CG-1 — One logical client call creates at most one invocation

**Statement.** When a call sets no idempotency key, the TypeScript client generates a random one and keeps it across every retry of that call (`null` opts out; workflow `run` is already keyed), so a call whose response is lost after the server created the invocation still creates only one.
**Why.** Retrying on a lost response is the client's job; without a stable key it would duplicate work.
**Proven by.** the "CG-1" test of `sdk/typescript/test/client.e2e.test.ts` (a lost `/invoke` response is retried with the same key and yields one invocation), the `invoke: 202` test of `sdk/typescript/test/connect.test.ts` (attach by id until done; the generated idempotency key survives retries).
**See.** `sdk/typescript/src/client.ts`.

### CG-2 — A client never sees 202 from invoke

**Statement.** When `/invoke` answers `202`, the client attaches by id until the invocation ends, and attach returns exactly the durable result; the node caps how long one attach request waits.
**Why.** Callers expect a typed result, not a polling protocol.
**Proven by.** `crates/cairn-server/tests/invoke_wait.rs::attach_by_id_waits_for_the_durable_result`, `crates/cairn-server/tests/invoke_wait.rs::attach_wait_is_capped_by_the_node`.
**See.** `crates/cairn-server/src/ingress/invoke.rs`.

### CG-3 — The same journal yields the same race winner

**Statement.** `ctx.race` picks the winner among ready leaves by their order in the journal, whether the leaves are completions, signals or a mix, so every replay of the same journal picks the same winner.
**Why.** Determinism under replay is the core contract of durable execution.
**Proven by.** `crates/cairn-runtime/tests/conformance_gen.rs::traces_match_runtime`, the `race(signal, sleep)` test of `sdk/typescript/test/handler.test.ts` (the winner follows journal order and is stable across replay).
**See.** `conformance/README.md`.

### CG-4 — SDK and runtime agree on when a suspension is satisfied

**Statement.** The SDK's `suspensionSatisfied` and the runtime's `suspension_satisfied_by` give the same answer on every suspension tree the SDK emits; both are checked against the shared vectors in `conformance/suspension-vectors.json`.
**Why.** Disagreement would make the SDK wait forever or the runtime wake it for nothing.
**Proven by.** `crates/cairn-runtime/tests/suspension_vectors.rs::runtime_and_sdk_agree_on_every_vector`, the "CG-4" test of `sdk/typescript/test/conformance.test.ts` (the SDK encodes suspension trees byte-exactly and satisfies them by the runtime's rule).
**See.** `conformance/README.md`.

### CG-5 — A single-leaf wait encodes exactly as before

**Statement.** Waiting on one leaf encodes the `Suspension` exactly as before combinator trees existed; trees only add encodings, and the committed conformance traces do not drift.
**Why.** Older SDKs and recorded traces must keep working byte for byte.
**Proven by.** `crates/cairn-runtime/tests/conformance_gen.rs::traces_match_runtime`, the single-leaf `awaitSignal` test of `sdk/typescript/test/handler.test.ts` (suspension uses field 2 or 3, nothing else).
**See.** `conformance/README.md`.

### CG-6 — createdAt never moves

**Statement.** An invocation's `createdAt` is fixed for its whole life, and no timestamp is written into the journal.
**Why.** Clients filter and sort by creation time; a moving value breaks pagination and replay determinism.
**Proven by.** `crates/cairn-server/tests/invocation_times.rs::created_never_moves`.
**See.** `crates/cairn-engine/src/status.rs`.

### CG-7 — SlateDB create_ts survives every storage operation

**Statement.** A row's `create_ts` is preserved across flush, compaction, shared-WAL replay, clone, union and `DbReader`, and moves on overwrite.
**Why.** Invocation timestamps are read from `create_ts` (CG-9), so this library behaviour is a pinned assumption.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_reopen_from_wal_and_moves_on_overwrite`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_compaction`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_clone_union_and_reader`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_shared_wal_replay_after_crash`.
**See.** `docs/reliability/irreversible.md` §0.3.

### CG-8 — get_meta behaves the same on Db and on the model

**Statement.** `RangeStore::get_meta` returns the value with its `create_ts` in milliseconds, with the same contract on SlateDB and on the in-memory range model.
**Why.** Engine tests run on the model; a contract gap would hide bugs.
**Proven by.** `crates/cairn-store/tests/range_store.rs::get_meta_carries_create_ts`.
**See.** `crates/cairn-store/src/testkit/range_model.rs`.

### CG-9 — Invocation timestamps add no writes

**Statement.** `createdAt` is the SlateDB `create_ts` of entry 0 and `modifiedAt` the `create_ts` of the status row, so these timestamps add no bytes to any commit; reading them and filtering by time commits nothing and writes no WAL, manifest or SST object.
**Why.** Timestamps must not make every call more expensive, and reads must never cause writes.
**Proven by.** `crates/cairn-server/tests/invocation_times.rs::no_extra_writes`.
**See.** `crates/cairn-server/tests/invocation_times.rs`.

### CG-10 — A schedule mark produces at most one run

**Statement.** A `(schedule, mark)` pair creates at most one run: on the same range the run id is `schedule_run_id(schedule, mark)` and an existing id only advances the schedule; across ranges the `Fire` row sends a `StartCall` with key `schedule/<name>/<mark>` that the destination deduplicates. Missed marks fire once for the latest and are recorded as `missed`.
**Why.** Resends, retries and manual fires must not multiply scheduled work.
**Proven by.** `crates/cairn-server/tests/schedules.rs::local_fire_creates_the_run_and_advances_in_one_commit`, `crates/cairn-server/tests/schedules.rs::remote_fire_crosses_ranges_once`, `crates/cairn-server/tests/schedules.rs::missed_marks_fire_once_for_the_latest`, `crates/cairn-server/tests/failpoints.rs::schedule_fire_dies_around_the_commit`.
**See.** `docs/guides/flow.md` §1.

### CG-11 — Time filters do not hold the engine and are bounded

**Statement.** Listing invocations filtered by creation or end time runs as a bounded scan with both a row cap and a time cap, and does not queue behind a slow turn.
**Why.** Introspection must stay responsive and cheap on large ranges.
**Proven by.** `crates/cairn-engine/tests/introspection.rs::time_filter_has_scan_cap_and_sorts_descending`, `crates/cairn-server/tests/concurrency.rs::introspection_does_not_queue_behind_a_slow_turn`.
**See.** `crates/cairn-engine/src/engine/types.rs`.

### CG-12 — A fire and the next due time commit together

**Statement.** A schedule fire and its new `next_at` are written in one commit, so no durable state has a live schedule without a next fire; the tripwire `schedule::live_without_next` checks every write of a live schedule.
**Why.** A schedule without a pending fire dies silently.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::schedule_fire_dies_around_the_commit`, `crates/cairn-engine/tests/schedules_table.rs::create_with_schedule_mark_writes_header_and_row_in_one_commit`, `crates/cairn-engine/tests/schedules_table.rs::cross_range_fire_writes_fire_and_schedule_row_in_one_commit`.
**See.** `docs/guides/flow.md` §1.

### CG-13 — The schedules gate protects old binaries

**Statement.** While the `schedules/table` gate is off, `PUT /schedules` is refused with `409`; once it is on, a binary that does not know the `Schedules` table cannot lead a range. `cairn-ctl cluster enable-schedules` raises `min_binary` before opening the gate.
**Why.** An old binary leading a range with schedules would fire nothing, with no error to notice.
**Proven by.** `crates/cairn-server/tests/upgrade.rs::schedules_gate_off_refuses_put`, `crates/cairn-server/tests/upgrade.rs::schedules_gate_stops_the_streams_binary`, `crates/cairn-server/tests/ctl.rs::enable_schedules_raises_min_binary_and_lower_keeps_the_floor`.
**See.** `crates/cairn-store/src/migrate.rs`.

### CG-14 — Schedules follow split and merge

**Statement.** Schedule rows and their runs move with their partition key through split and merge, and the `ScheduleDue` index is rebuilt with the derived region; no mark is lost or fired twice.
**Why.** Range topology changes must be invisible to schedule semantics.
**Proven by.** `crates/cairn-server/tests/split.rs::schedule_due_during_split`, `crates/cairn-engine/tests/schedules_table.rs::derived_rebuild_restores_schedule_index`.
**See.** `crates/cairn-engine/tests/schedules_table.rs`.

### CG-15 — overlap: skip never runs two runs of a schedule at once

**Statement.** With `overlap: skip`, a fire whose previous run is still active creates nothing; the previous run's state is read from the same database as the fire.
**Why.** Users choose `skip` precisely to forbid concurrent runs.
**Proven by.** `crates/cairn-server/tests/schedules.rs::skip_never_overlaps`.
**See.** `docs/guides/flow.md` §1.

### CG-16 — Every leader computes the same next_at

**Statement.** `next_at` is a pure function of the schedule spec using an embedded, pinned time-zone database (`tzdb-bundle-always`, pinned `jiff`), so every leader computes the same marks regardless of host configuration or when the schedule was created.
**Why.** Leaders on different hosts or versions must not disagree on when a schedule is due.
**Proven by.** `crates/cairn-schedule/src/tests.rs::dst_tables`, `crates/cairn-schedule/src/tests.rs::croner_oracle_utc`, `crates/cairn-schedule/src/tests.rs::embedded_tzdb_and_pinned_jiff`.
**See.** `crates/cairn-schedule/src/lib.rs`.

## Q — query layer

`Q-*` codes govern the query layer: declared secondary indexes over invocation metadata
(`QUERY_INDEXES`), the read handles that serve durable data without the engine, and the SQL surface
built on DataFusion (`crates/cairn-query`, `crates/cairn-server/src/query/`). The full map lives in
`docs/guides/query.md`.

### Q-1 — Every index is derived from metadata and status through its descriptor

**Statement.** After every commit, the index rows of a `readable` descriptor are exactly `rows(descriptor, id, meta, Placement::new(class, t, meta))` over all live invocations. The only write path is `Mutations::query_event`, called from `Engine::stage_status`, `purge`, and `create` over an existing id.
**Why.** One derivation point keeps indexes from drifting and makes drift checkable by `verify`.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::class_indexes_follow_transitions`, `crates/cairn-engine/tests/query_meta.rs::purge_leaves_no_query_rows`, `crates/cairn-engine/tests/query_meta.rs::create_over_existing_id_replaces_query_rows`, `crates/cairn-server/tests/ctl.rs::verify_flags_query_index_drift`.
**See.** `docs/guides/query.md` §1.

### Q-2 — running ↔ suspended touches no index

**Statement.** Moving between `running` and `suspended` writes no index row and reads nothing; a wake takes a `Woken` value that can only be built from a status read as `suspended`.
**Why.** These transitions are the hot path of every turn.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::running_suspended_writes_no_index`, `crates/cairn-engine/tests/query_meta.rs::wake_only_suspended`.
**See.** `docs/guides/query.md` §1.

### Q-3 — A class timestamp has one source

**Statement.** For `active` the class time is `created_ms` from metadata; for `paused`, `completed` and `failed` it is the timestamp in the status payload, unchanged while the class does not change. `Placement::new` is the only function that derives it, for both live writes and background builds.
**Why.** Two sources would produce index rows that rebuilds cannot reproduce.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::created_at_is_stable_across_transitions`, `crates/cairn-engine/tests/derived_rebuild.rs::derived_region_rebuilds_to_same_bytes`.
**See.** `docs/guides/query.md` §1.

### Q-4 — The planner uses only descriptors readable on that range

**Statement.** A per-range marker records each descriptor's name and version; a missing or different marker means `building`, and that range falls back to a metadata scan with an `indexing` warning.
**Why.** A half-built index would silently return incomplete results.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::adding_an_index_builds_in_background`, `crates/cairn-engine/tests/query_meta.rs::dropped_index_rows_are_swept`, `crates/cairn-engine/tests/query_meta.rs::query_index_rebuilds_after_split`.
**See.** `docs/guides/query.md` §1.

### Q-5 — Background builds only put, one chunk per engine hold, marker last

**Statement.** An index build only writes rows, one chunk per engine hold, and writes its readable marker last; a crash midway restarts the build from the beginning.
**Why.** The marker is the only signal readers trust, so it must never precede the rows.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::query_index_rebuild_races_live_writes`, `crates/cairn-engine/tests/query_meta.rs::query_build_resumes_after_death_mid_chunk`.
**See.** `docs/guides/query.md` §1.

### Q-6 — Counts are exact per commit within a range

**Statement.** Class counters are updated after each batch is submitted; while being built, only changes to invocations at or below the build cursor are applied. On nodes the counters are built on demand: the scan starts after the first reader of the term, earlier readers get `partial` with `indexing`, and nothing is scanned if nobody asks.
**Why.** Exact counts answer `COUNT(*)` without reading, but building them must not cost every leader a full scan.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::counts_match_scan`, `crates/cairn-query/tests/sql.rs::counts_read_nothing`, `crates/cairn-server/tests/failpoints.rs::query_counts_are_built_on_demand`.
**See.** `docs/guides/query.md` §1.

### Q-7 — Exact pushdown means re-applied at the leaf

**Statement.** Every filter shape that `filter::supported` accepts has a matching condition in `Row::admits`; any other shape is reported as `Unsupported` to DataFusion.
**Why.** Claiming an exact filter that the leaf does not apply would return wrong rows.
**Proven by.** `crates/cairn-query/tests/sql.rs::query_matches_oracle`.
**See.** `docs/guides/query.md` §1.

### Q-8 — Declared order is the real order

**Statement.** A leaf that declares `[created_at DESC, id ASC]` (or another axis and direction through `try_pushdown_sort`) always returns rows sorted that way; within one timestamp, ascending id bytes equal ascending hex.
**Why.** DataFusion skips its own sort when the leaf declares an order.
**Proven by.** `crates/cairn-query/tests/sql.rs::scan_order_matches_declared`, `crates/cairn-query/tests/sql.rs::plan_shape_list_page`, `crates/cairn-store/src/query_index.rs::tests::same_time_orders_by_id_hex`.
**See.** `docs/guides/query.md` §1.

### Q-9 — Tenant is structural

**Statement.** Every index scan starts at the caller's tenant prefix, and point reads and counters filter by tenant.
**Why.** Isolation enforced by key layout cannot be bypassed by a crafted predicate.
**Proven by.** `crates/cairn-query/tests/sql.rs::tenant_is_structural`, `crates/cairn-server/tests/query_api.rs::query_roles`.
**See.** `docs/guides/query.md` §1.

### Q-10 — Reads see only durable rows and never wait for the engine

**Statement.** Query reads go through read handles that see only durable data and never acquire the range engine.
**Why.** Queries must neither expose undurable state nor slow down turns.
**Proven by.** `crates/cairn-store/tests/range_read.rs::query_sees_only_durable_rows`, `crates/cairn-server/tests/failpoints.rs::query_does_not_wait_for_engine`.
**See.** `docs/guides/query.md` §1.

### Q-11 — The SQL surface is locked and budgeted

**Statement.** No DDL, DML, statements, table functions, file access or scalar array-generating functions; memory, read, row and time limits apply, with four concurrent queries per node. Results are consumed batch by batch: the row cap stops the query at row `max_rows + 1`, results count against the query's memory pool, and each batch checks the deadline and yields.
**Why.** SQL from tenants is untrusted input to a shared node.
**Proven by.** `crates/cairn-query/tests/sql.rs::sql_surface_is_locked`, `crates/cairn-query/tests/sql.rs::memory_cap_is_enforced`, `crates/cairn-query/tests/sql.rs::rows_cap_stops_before_materialising`, `crates/cairn-server/tests/query_api.rs::query_budget_is_enforced`.
**See.** `docs/guides/query.md` §1.

### Q-12 — Remote ranges run the same leaf code, without duplicates or silent gaps

**Statement.** The coordinating node reads the ranges it has open (only handles matching both the id and the bounds of the current table) and sends the rest to their leaders over `POST /internal/query/scan`, which runs the same leaf code; it merges and applies `fetch`. An unreachable node makes the answer `partial` with the range named, a different descriptor version remotely means a metadata scan with `indexing`, and a deposed leader's handle never returns an old snapshot as a complete answer.
**Why.** A cluster-wide query must not double-count a split range or quietly omit a dead one.
**Proven by.** `crates/cairn-query/tests/sql.rs::query_spans_nodes_in_process`, `crates/cairn-query/tests/sql.rs::remote_down_is_partial`, `crates/cairn-server/tests/query_cluster.rs::query_during_split`, `crates/cairn-server/tests/query_cluster.rs::query_on_deposed_leader`.
**See.** `docs/guides/query.md` §1.

### Q-13 — Keyed tables never scan a whole range

**Statement.** `sys_journal`, `sys_state`, `sys_promise`, `sys_artifact` and `sys_log` require equality on a full key set, otherwise planning fails with a caller error; tables keyed by `id` read metadata to check the tenant. Tables with an empty key set scan an ordered index under a read cap, and `sys_inbound`/`sys_outbox` without `id` are cluster-admin only.
**Why.** An unkeyed scan of journals or state is unbounded in cost.
**Proven by.** `crates/cairn-query/tests/sql.rs::bulk_table_without_key_is_refused`, `crates/cairn-query/tests/sql.rs::journal_matches_engine`, `crates/cairn-query/tests/sql.rs::state_reads_one_object`.
**See.** `docs/guides/query.md` §1.

### Q-14 — View tables are REST, not copies

**Statement.** RAM- and bucket-backed tables (`cairn_query::view::VIEWS`) run the matching REST `GET` on the node's router with the caller's credentials, and a REST 4xx is a caller error. Parameter columns require equality and accept only `[A-Za-z0-9_.:-]` before entering a path.
**Why.** One implementation per read keeps authorization and results identical across REST and SQL.
**Proven by.** `crates/cairn-server/tests/query_api.rs::views_match_rest`, `crates/cairn-server/tests/query_api.rs::views_follow_rest_roles`, `crates/cairn-server/tests/query_api.rs::ops_tables_are_cluster_admin`.
**See.** `docs/guides/query.md` §1.

### Q-15 — Coverage is checked

**Statement.** Every `keyspace::Table` has a query table or a recorded reason for exclusion, every ingress `GET` route has a query table or a reason, and `docs/api/query-tables.md` is generated from the declarations.
**Why.** New data must not silently become invisible to SQL, and the docs must not drift.
**Proven by.** `crates/cairn-query/tests/catalog.rs::every_table_is_queryable_or_excluded`, `crates/cairn-query/tests/catalog.rs::query_tables_doc_is_current`, `crates/cairn-server/tests/query_api.rs::every_read_endpoint_has_a_query_table`.
**See.** `docs/guides/query.md` §1.

### Q-16 — Hint rows are never deleted on the write path

**Statement.** Per-object hint rows (`ObjectKeys`) carry no invocation id and are never deleted by index events; `sys_object` checks that the object still has state or an invocation before returning it. A paced background sweep deletes hints with nothing left, reading and deleting under the same engine hold and keeping the row when in doubt.
**Why.** Deleting a hint on the write path would need a read per commit; a stale hint costs only a filtered row.
**Proven by.** `crates/cairn-store/src/query_index.rs::tests::object_keys_is_a_hint_never_deleted`, `crates/cairn-engine/tests/query_meta.rs::object_hints_survive_purge_and_are_swept`, `crates/cairn-server/tests/failpoints.rs::hint_sweep_is_paced_and_resumes_across_terms`.
**See.** `docs/guides/query.md` §1.

### Q-17 — Lineage follows calls without breaking the wire

**Statement.** A callee receives `parent_id`, `root_id` and the SDK's tags in a trailing block of the `Call` row and `StartCall` message; a message without the block is byte-identical to the old format.
**Why.** Lineage queries must work without a wire version bump for older peers.
**Proven by.** `crates/cairn-server/tests/lineage.rs::call_lineage_reaches_the_callee`, `crates/cairn-engine/src/outbox.rs::tests::call_lineage_is_a_tail`, `crates/cairn-engine/src/interpartition.rs::tests::lineage_is_a_tail`.
**See.** `docs/guides/query.md` §1.

### Q-18 — Handler history caches only days that can no longer change

**Statement.** The drainer writes only days at or after yesterday by its own clock; readers treat a day starting at `s` as closed only when `s + 2 days + HISTORY_SKEW_MS ≤ now`, and only closed days are cached beyond 30 s (at most 1 h). A tenant in `deleting` or `purged` bypasses the cache.
**Why.** Caching an open day would serve stale numbers indefinitely.
**Proven by.** `crates/cairn-server/src/stats_history.rs::tests::closed_days_are_never_rewritten`, `crates/cairn-server/src/stats_history.rs::tests::cache_lists_open_days_only`, `crates/cairn-server/src/stats_history.rs::tests::forget_drops_closed_days`.
**See.** `docs/guides/query.md` §1.

### Q-19 — sys_artifact_bytes is exact per commit within a range

**Statement.** `sys_artifact_bytes` is the sum of `size` over counted `Artifacts` rows (decodable and with metadata, attributed to the metadata's tenant). Every path that changes whether a row counts (SDK put, restart copy, purge, tenant purge) emits a delta computed under the same engine hold as its commit; rebuilds scan in chunks, report `indexing` until done, and never read the object store.
**Why.** The artifact-bytes quota is enforced from this number.
**Proven by.** `crates/cairn-engine/tests/query_meta.rs::artifact_bytes_match_scan`, `crates/cairn-engine/tests/tenant_purge.rs::tenant_purge_drops_artifact_bytes`, `crates/cairn-server/tests/tenant_limits.rs::artifact_bytes_across_split_and_merge`, `crates/cairn-server/tests/tenant_limits.rs::restart_copies_count_once`.
**See.** `docs/guides/query.md` §1.

### Q-20 — Query shape is bounded before DataFusion; object reads spend the tenant's budget

**Statement.** `cairn_query::check_sql` bounds text size, operators, relations and blocks on tokens before parsing, then walks the AST by allow-list and bounds blocks, relations, expressions, schema width, cells and grouping sets after CTE inlining. `GET /archive/{id}`, `GET /audit` and schedule listing spend store-read units from the tenant's bucket before reading, answering `429` or `partial` when it runs dry.
**Why.** Planning itself can be exponential or deeply recursive; bounding shape first keeps one query from taking down a node.
**Proven by.** `crates/cairn-query/tests/sql.rs::long_operator_chain_is_refused`, `crates/cairn-query/tests/sql.rs::cte_inlining_is_counted`, `crates/cairn-query/tests/sql.rs::adversarial_queries_plan_fast`, `crates/cairn-server/tests/archive.rs::missing_id_costs_bounded_store_reads`.
**See.** `docs/guides/query.md` §1.

### Q-21 — User-written SQL runs in a child process, not in the node

**Statement.** In `process` mode (default when the `cairn-query-worker` binary is present, mandatory under `--profile saas`) `POST /query` runs in a sandboxed child that has no range handles, cluster secret, environment or caller identity and treats the node as remote for every range. The node treats the child's messages as untrusted: it sets tenant, ranges and read budgets itself, bounds frame sizes, and kills the child on any violation, memory overrun or deadline with `422`.
**Why.** DataFusion planning and constant folding allocate outside the pool and run synchronously; only a process boundary contains them.
**Proven by.** `crates/cairn-server/tests/query_isolation.rs::adversarial_queries_do_not_kill_the_node`, `crates/cairn-server/tests/query_isolation.rs::worker_death_is_reported_and_replaced`, `crates/cairn-server/tests/query_isolation.rs::isolation_flag_is_honoured`, `crates/cairn-query/src/view.rs::tests::only_view_paths_are_accepted`.
**See.** `docs/guides/query.md` §1.

## Other codes

### D-O1 — The outbox `seq` is an index, not the primary key

**Statement.** Outbox rows keep their natural key; a separate index orders them by a per-range `seq` that the drainer walks with a durable cursor.
**Why.** Changing the outbox key to `seq` would break replay de-duplication.
**Proven by.** `crates/cairn-engine/tests/outbox_seq.rs::cursor_reads_in_creation_order`, `crates/cairn-engine/tests/outbox_seq.rs::read_continues_from_cursor`
**See.** FL-15, FL-18.

### D-O2 — Replay reuses the original `seq`

**Statement.** Replaying a turn that re-emits an outbox row for the same `(invocation, completion)` reuses the `seq` first assigned to it, so the index holds one entry per row; removing a row (`Engine::stage_outbox_removal`) removes its index entry too.
**Why.** A new `seq` would duplicate the call.
**Proven by.** `crates/cairn-engine/tests/outbox_seq.rs::replayed_task_adds_no_index_row`, `crates/cairn-engine/tests/outbox_seq.rs::trimming_a_row_deletes_its_index`
**See.** FL-15.

### AU-1 — An audit row commits with the action it records

**Statement.** `turn::Ctx::audit_begin` parks an audit record after taking the entity lock; the first commit that touches that entity carries the `Audit` row and index entry, and if none does, `audit_flush` commits it before the response. The export loop writes each row to an object with `Create` plus read-back and only then deletes the row, so a crash at any point exports exactly once.
**Why.** An audit trail that can miss or duplicate actions is not evidence.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::audit_export_dies_between_put_and_delete_writes_once`, `crates/cairn-server/tests/failpoints.rs::audit_rows_follow_a_split_and_export_once`, `crates/cairn-store/src/audit.rs::tests::writes_exactly_once_through_ambiguity`
**See.** `docs/guides/flow.md` §1.

### D-F1 — A scope decides the partition key

**Statement.** An invocation with a scope gets its partition key from the scope alone (`H(scope)`), whatever its service and object key, so every target of a scope lives in one range for any range table. A scope's concurrency counter is therefore a plain integer in the memory of one engine.
**Why.** Admission needs no consensus and no network step. The price is that one scope cannot be spread over several ranges.
**Proven by.** `crates/cairn-store/src/partition_table.rs::tests::same_scope_same_key_for_any_table`, `crates/cairn-store/src/cluster.rs::tests::same_scope_same_partition_for_every_n`
**See.** `docs/key-design.md` §1, FL-5.

### D-F4 — Every invocation goes through a virtual queue

**Statement.** Every invocation — exclusive or shared, keyed or not — is admitted through a virtual queue of its range; there is no legacy path that bypasses the queue and no feature flag that selects one. Exclusive and shared differ only in which resources the queue entry needs.
**Why.** One admission path is one path to test and to reason about.
**Proven by.** `crates/cairn-model/tests/m4.rs::core_holds_every_property`, `crates/cairn-server/tests/flow_control.rs::same_object_key_runs_in_arrival_order`
**See.** `docs/guides/flow.md`.

### D-W5 — Shared handlers of a virtual object cannot write state

**Statement.** A turn whose effective handler type is `SHARED` in a `VIRTUAL_OBJECT` service (per the deployment the invocation is bound to) has `SetState`, `ClearState` and `ClearAllState` refused with a protocol violation (571). The rule applies to invocations whose entry 0 carries the state-lock mark; older invocations keep the old behaviour.
**Why.** Shared handlers run without the object lock, concurrently with exclusive ones; a write from them is a lost update.
**Proven by.** `crates/cairn-engine/tests/state_lock.rs::marked_shared_turn_refuses_writes_and_exclusive_turn_writes`, `crates/cairn-server/tests/object_shared_state.rs::shared_object_handler_cannot_write_state`
**See.** `sdk/typescript/README.md`.

### I4 — A follower may lack the unexposed tail of a quiet log

**Statement.** A follower reads a range's WAL only as far as objects have been exposed; the tail of a quiet leader's log may not be exposed yet. A follower that sees an unexposed tail older than its bound does not serve possibly stale in-progress state and defers to the leader instead.
**Why.** Follower reads must never present stale state as current.
**Proven by.** `crates/cairn-server/tests/follower_read.rs::follower_does_not_serve_a_stale_status_when_the_leader_is_idle`, `crates/cairn-server/tests/follower_read.rs::follower_sees_an_idle_leaders_tail_without_redirecting`
**See.** NW-10.

## G — SlateDB and object-store assumptions

Cairn's correctness depends on specific semantics of SlateDB (including patches carried in the fork under `vendor/slatedb`) and of the object store underneath. Each assumption is pinned by a test so that a library upgrade that changes it turns the tree red; most pins live in `crates/cairn-store/tests/library_contracts.rs`. Changing SlateDB, object_store or the fork, or adding a new assumption, requires a new pin and a row here (AGENTS.md §2).

### G1 — SlateDB updates its in-memory manifest before the manifest is stored

**Statement.** SlateDB applies an uploaded flush to its in-memory manifest (`apply_uploaded_state`) and notifies waiters before the new manifest is durably written, so in-memory WAL marks can run ahead of the stored manifest.
**Why.** Anything that trims or deletes WAL by an in-memory mark could drop acknowledged rows after a crash.
**Relied on by.** NW-7 (the shared-WAL GC floor trusts only the stored manifest); irreversible.md M2, M6, X8.
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::gc_floor_never_passes_frames_the_stored_manifest_does_not_cover`, `crates/cairn-server/tests/sim_cluster.rs::swarm` (the `slot` fault: crash after an SST PUT, before the manifest)
**See.** `docs/reliability/irreversible.md` §0.3.

### G2 — A memtable flush returns only after the manifest is stored

**Statement.** `flush_with_options(MemTable)` resolves only after the manifest update that publishes the flushed SST has been written (`resolve_pending_flushes` runs after `write_manifest_update`).
**Why.** Seal and take-over treat a returned flush as durable state they can build on.
**Relied on by.** `flush_through`, `PartitionWriter::seal`, irreversible.md M2 and M3.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g2_memtable_flush_returns_after_the_manifest_is_stored`
**See.** `docs/reliability/irreversible.md` §0.3.

### G3 — `Db::close` flushes and returns only once the data is durable

**Statement.** Closing a SlateDB `Db` flushes outstanding writes and returns only after they are durable.
**Why.** Seal relies on close to leave nothing acknowledged outside the stored DB.
**Relied on by.** SM-4, NW-6.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::sealed_parent_clones_with_every_acked_row` (indirect)
**See.** `docs/reliability/irreversible.md` §0.3.

### G4 — Clone writes the child manifest before it places checkpoints on ancestors

**Statement.** A SlateDB clone writes the child's manifest, with fresh checkpoint ids, before it creates the corresponding final checkpoints on the ancestors; a clone that dies in between leaves a child DB and no ancestor checkpoint.
**Why.** The child's manifest is then a complete before-image of every checkpoint the clone may have placed, so a roll-back can remove exactly those.
**Relied on by.** SM-12 (`discard_range_db` uses the child manifest as evidence).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g4_clone_writes_the_child_manifest_before_ancestor_checkpoints`, `crates/cairn-store/tests/library_contracts.rs::g4_clone_dying_between_child_manifest_and_ancestor_checkpoint_is_discarded`, `crates/cairn-store/tests/range_clone.rs::discard_keeps_the_orphan_until_its_checkpoints_are_gone`
**See.** `docs/reliability/irreversible.md` §0.3.

### G5 — Compaction and GC of a DB run only in the process that has it open; a quiesced source does not change

**Statement.** SlateDB's compactor and GC for a DB run only inside the process that holds it open for writing, so a sealed and closed source gains no objects and its manifest does not change while in-flight children stand on it.
**Why.** D-d and the 300 s temporary clone checkpoint assume a quiesced source cannot detach or compact away what an in-flight child needs.
**Relied on by.** SM-9, SM-13, SM-24; the `quiesced_source_frozen` assumption of M2.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g5_quiesced_closed_db_gains_no_objects_under_compactor_and_gc`
**See.** `docs/reliability/irreversible.md` §0.3.

### G6 — Detach deletes the parent checkpoint first and drops the entry second, only when nothing stands on the parent

**Statement.** SlateDB's detach removes an `external_dbs` entry only when its `sst_ids` are empty in the current manifest and in every manifest pinned by a live checkpoint, and it deletes the final checkpoint on the parent before dropping the entry from the child.
**Why.** The window between the two steps is a legitimate state that `cairn-ctl verify` must report as a pending detach, not a lost checkpoint.
**Relied on by.** SM-9, SM-12 (`ExternalStatus::Detaching`).
**Proven by.** `crates/cairn-store/tests/range_clone.rs::missing_checkpoint_of_an_entry_without_parent_ssts_is_a_pending_detach`
**See.** `docs/reliability/irreversible.md` §0.3.

### G7 — `external_dbs` lists every ancestor, and an entry disappears only through detach

**Statement.** A clone's `external_dbs` copies every ancestor of its sources (direct and inherited), and an entry leaves the manifest only through detach (G6).
**Why.** The union of live ranges' `external_dbs` is then the complete set of DBs a live range can read.
**Relied on by.** SM-9, SM-14.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::retired_parent_db_kept_while_children_reference_it`, `crates/cairn-store/tests/range_clone.rs::retired_parent_db_deleted_after_children_compact_away_from_it`
**See.** `docs/reliability/irreversible.md` §0.3.

### G8 — SST GC cuts at the oldest of min-age, compaction mark and newest L0, reading stored and checkpoint manifests

**Statement.** SlateDB's SST garbage collector deletes an SST only below min(`min_age`, the lowest compaction mark, the newest L0), and decides from the latest stored manifest plus the manifests pinned by live checkpoints.
**Why.** A parent may compact and GC after a clone without removing any SST its child still reads.
**Relied on by.** irreversible.md S1; G27.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g8_parent_compaction_and_gc_keep_what_the_child_reads`
**See.** `docs/reliability/irreversible.md` §0.3.

### G9 — Checkpoint expiry and manifest GC follow the clock of the node running them

**Statement.** SlateDB expires checkpoints and trusts its manifest boundary window (`gc_min_age/2`) by the clock of the process running GC. On cairn's side, D-d, the orphan pass and the trash pass measure skew against the store's `last_modified` and delete nothing when it exceeds `min_age/4`; the SlateDB-side dependency on the writer node's clock is not pinned.
**Why.** A fast clock could expire a checkpoint or age a tombstone early and delete data still needed.
**Relied on by.** irreversible.md S2 and S5; the age checks of tombstones, barriers and trash entries.
**Proven by.** `crates/cairn-server/tests/gc.rs::gc_rounds_delete_nothing_when_the_node_clock_runs_ahead_of_the_store` (cairn side only)
**See.** `docs/reliability/irreversible.md` §0.3.

### G10 — Clone refuses a source whose WAL is not covered by its manifest

**Statement.** The fork's clone refuses a source DB that still has WAL not covered by its manifest; with the shared WAL it asks the source owner record (`SharedWalAdmin::is_empty`).
**Why.** It is the library-side fence behind SM-4: no child may be cut from a source with unflushed rows.
**Relied on by.** SM-4.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::clone_refuses_unclosed_parent`, `crates/cairn-store/tests/shared_wal.rs::open_shared_parent_refuses_clone`
**See.** `docs/reliability/irreversible.md` §0.3.

### G11 — Opening a writer bumps `writer_epoch` and fences the previous writer

**Statement.** Opening a `Db` for writing advances `writer_epoch`, after which the previous writer can no longer write. Cairn uses the default open path (retry until the open wins), not `DbBuilder::with_expected_epoch`.
**Why.** Lease handover relies on the store, not only the lease, to stop a stale leader.
**Relied on by.** LS-4.
**Proven by.** `crates/cairn-store/tests/store.rs::old_writer_fenced_when_new_writer_starts`, `crates/cairn-store/tests/store.rs::frozen_old_leader_cannot_write_after_b_starts`
**See.** `docs/reliability/irreversible.md` §0.3.

### G12 — Changing the WAL kind is refused while the WAL has rows

**Statement.** `set_wal_name` refuses to change a DB's WAL kind (native vs. shared) while its WAL still holds rows.
**Why.** Switching WAL kind with unreplayed rows would orphan them.
**Relied on by.** NW-9; irreversible.md S7.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g12_wal_kind_change_refuses_a_db_whose_wal_has_rows`, `crates/cairn-store/tests/shared_wal.rs::wal_kind_mismatch_refuses_open`, `crates/cairn-store/tests/shared_wal.rs::child_of_native_parent_adopts_shared_wal`
**See.** `docs/reliability/irreversible.md` §0.3.

### G13 — SlateDB `Admin` operations retry forever

**Statement.** SlateDB's `Admin` operations (manifest reads, WAL references, checkpoints, `set_wal_name`), memtable flushes and clones retry indefinitely, so cairn wraps every such await in a bound (`config::admin_io` or `retry::deadline`); `scripts/lint-sources.sh` check 3 rejects new unbounded awaits.
**Why.** An unbounded wait while holding a lease or engine would stall a range past its lease TTL.
**Relied on by.** LS-12; AGENTS.md §6 item 5.
**Proven by.** `crates/cairn-server/src/node/tests.rs::open_io_bounds_slatedb_retrying_forever`
**See.** `docs/reliability/irreversible.md` §0.3.

### G14 — Reopen-at-epoch removes a close mark only if the epoch the caller read still holds

**Statement.** The fork's `reopen_for_writes_at_epoch` clears the `closed` flag only when `writer_epoch` is still the value the caller read, and re-evaluates that condition on every retry of the manifest CAS. The retry branch itself is not pinned.
**Why.** A slow roll-back must not clear the close mark that a later intent placed on the same source.
**Relied on by.** SM-17 (`split::reopen_sources`, `heal_stale_close`).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g14_reopen_at_epoch_keeps_a_newer_close`
**See.** `docs/reliability/irreversible.md` §0.3.

### G15 — Clone and union name their final checkpoint on every ancestor after the child path

**Statement.** Clone and union place exactly one non-expiring final checkpoint, named with the child DB's path, on every `external_dbs` entry of the child (direct sources and inherited ancestors); it disappears only through the child's detach (G6) or `discard_range_db`. Checkpoints created before the fork patch are unnamed.
**Why.** A retired DB can then prove from its own manifest whether any untombstoned descendant still stands on it.
**Relied on by.** SM-9 (second fence of `DeleteProof::retired_db`); `cairn-ctl verify` check (f).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g15_clone_and_union_name_their_final_checkpoint_on_every_ancestor`
**See.** `docs/reliability/irreversible.md` §0.3.

### G16 — Cairn never takes a checkpoint through `Db`, because the shared WAL returns an upper bound

**Statement.** SlateDB uses the `wal_id` returned by `WalWriter::flush` only as the WAL mark of `Db::create_checkpoint`/`begin_checkpoint`; the shared WAL returns an upper bound there, so such a checkpoint could include rows beyond its mark or trip a manifest-writer assertion. Cairn therefore takes checkpoints only through `Admin` (`create_detached_checkpoint`) and clone, and `scripts/lint-sources.sh` check 8 rejects `create_checkpoint(`/`begin_checkpoint(` in `crates/*/src`.
**Why.** A checkpoint with a wrong WAL mark breaks the "rows after the mark are not in the checkpoint" contract.
**Relied on by.** NW-7 (the `next_wal_sst_id` half, as in G1).
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::gc_floor_never_passes_frames_the_stored_manifest_does_not_cover` (plus lint check 8)
**See.** `docs/reliability/irreversible.md` §0.3.

### G17 — SlateDB randomness comes from the seed the builder was given

**Statement.** All randomness of `Admin`, `Db` and `DbReader` (retry jitter, checkpoint ids, ULIDs) is drawn from the RNG the builder seeds with `with_seed`; unseeded, it comes from the OS. Every builder is seeded from `entropy::slatedb_seed` (lint check 9). This is for deterministic simulation only, not for safety.
**Why.** Deterministic simulation needs every source of nondeterminism under the seed.
**Relied on by.** The deterministic simulation harness (`tests/sim_cluster.rs`).
**Proven by.** `crates/cairn-store/src/config/mod.rs::tests::admin_and_reader_retry_by_source_seed`, `crates/cairn-server/tests/sim_cluster.rs::swarm`
**See.** `docs/reliability/irreversible.md` §0.3.

### G18 — A conditional GET never serves a stale `304`

**Statement.** After a winning write to an object, a GET with `If-None-Match` on the old etag returns `200` with the new body, a GET on the new etag returns `304`, a read right after a write (same or another client) sees the new body, and every write changes the etag. Caching proxies or weakly consistent stores violate this; `cairn-ctl cluster enable-quiet-lease --apply` runs the same probe against the cluster's bucket and refuses on failure.
**Why.** Quiet leases and fences treat `304` as proof that nobody challenged an object.
**Relied on by.** LS-16, LS-5, LS-6.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g18_conditional_get_never_serves_a_stale_304`, `crates/cairn-server/tests/s3_client_contract.rs::conditional_get_reaches_the_store_and_a_stale_304_is_caught`, `crates/cairn-store/tests/s3_store_contract.rs::s3_5_conditional_get_never_serves_a_stale_304`, `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_refuses_a_store_that_serves_a_stale_304`
**See.** `docs/reliability/irreversible.md` §0.3; S3-5.

### G19 — A `Remote` read sees exactly the durable prefix, also with the shared WAL

**Statement.** A read with `DurabilityLevel::Remote` sees exactly the writes with `seq ≤ last_remote_persisted_seq`, and with the shared WAL that mark is the `durable_seq` pushed by `SharedWriterInit` — the same mark the writer uses to issue commit tickets.
**Why.** The drainer's debug tripwire re-reads every row it is about to send at durable level, which is meaningful only if `Remote` means durable.
**Relied on by.** IR-1 (`PartitionWriter::get_durable`).
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::remote_read_sees_exactly_the_durable_prefix_with_a_shared_wal`
**See.** `docs/reliability/irreversible.md` §0.3.

### G20 — Row `create_ts` survives replay, compaction, clone, union and readers

**Statement.** SlateDB stamps every row with `create_ts` (ms, from the DB's clock, one stamp per batch) and keeps it unchanged through native WAL replay, compaction, clone, union, `DbReader` and shared-WAL replay; an overwrite gets a new stamp.
**Why.** Invocation timestamps (`createdAt`, `modifiedAt`, `endedAt`, and the `created_*`/`ended_*` filters) are read from it at zero storage cost.
**Relied on by.** Invocation timestamps in the query layer.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_reopen_from_wal_and_moves_on_overwrite`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_compaction`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_clone_union_and_reader`, `crates/cairn-store/tests/library_contracts.rs::create_ts_survives_shared_wal_replay_after_crash`
**See.** `docs/reliability/irreversible.md` §0.3.

### G21 — `Db::snapshot()` is a stable snapshot

**Statement.** A scan through `Db::snapshot()` sees no write made after the snapshot, even across a memtable flush, and a `Remote` durability filter applies to the snapshot as to the DB (G19).
**Why.** Each range answers a query from one consistent, durable-only view.
**Relied on by.** The read contract of the query layer (`range_read.rs`).
**Proven by.** `crates/cairn-store/tests/range_read.rs::snapshot_holds_across_memtable_flush`, `crates/cairn-store/tests/range_read.rs::query_sees_only_durable_rows`
**See.** `docs/reliability/irreversible.md` §0.3.

### G22 — Reading through a closed `Db` fails instead of serving stale data

**Statement.** A read through a clone or snapshot of a `Db` that has been closed returns an error; it never returns the old view as if current.
**Why.** A query whose range is sealed mid-flight must report `partial`, not wrong rows.
**Relied on by.** Query read handles across a seal (`range_read.rs`).
**Proven by.** `crates/cairn-store/tests/range_read.rs::read_handle_gone_after_seal`
**See.** `docs/reliability/irreversible.md` §0.3.

### G23 — A full compaction into the last sorted run drops deleted bytes

**Statement.** A tiered compaction whose sources are all L0s and all sorted runs and whose destination is the last run drops trailing tombstones and the values they shadow when no snapshot needs them; a compaction submitted through `Admin::submit_compaction` runs in the compactor of the process that has the DB open (G5). Source SSTs stay pinned by the compactor's own checkpoint until it expires, then SlateDB GC deletes them after `gc_min_age`.
**Why.** Tenant purge promises that deleted bytes leave the store, not only become unreadable.
**Relied on by.** Tenant purge (irreversible.md L4, `ObjectStores::submit_full_compaction`).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g23_full_compaction_into_the_last_run_drops_deleted_bytes`
**See.** `docs/reliability/irreversible.md` §0.3.

### G24 — `wake_compactor` runs a submitted compaction without waiting for a poll

**Statement.** The fork's `Db::wake_compactor` raises the in-process compactor's flush signal, so the coordinator re-reads the manifest and compaction log and runs a compaction submitted through `Admin::submit_compaction` immediately instead of at the next poll (1 h by default); without a compactor it does nothing. If it breaks, purges are only slower; no delete depends on it.
**Why.** Tenant purge should finish promptly on an idle cell.
**Relied on by.** Tenant purge (`tenant_purge::compact_range`).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g24_wake_compactor_runs_a_submitted_compaction_without_a_poll`, `crates/cairn-server/tests/tenant_purge.rs::idle_cell_purge_does_not_wait_for_the_compactor_poll`
**See.** `docs/reliability/irreversible.md` §0.3.

### G25 — Dropping shadowed versions at memtable flush is safe with the shared WAL

**Statement.** SlateDB drops older versions of a key at memtable flush. This is safe for cairn because (a) a new L0 is published only after the shared WAL is durable up to the memtable's last sequence number, (b) followers reading through `DbReader` never see an overwritten key go back or vanish across flushes, and (c) an open snapshot keeps a version its memtable later shadows.
**Why.** No reader — follower, query snapshot or post-ack read — may see an acknowledged row disappear.
**Relied on by.** Follower reads, query snapshots, ack-after-durable (INV-5).
**Proven by.** `crates/cairn-store/tests/shared_wal.rs::g25a_l0_is_published_only_after_the_shared_wal_is_durable`, `crates/cairn-store/tests/shared_wal.rs::g25b_follower_never_sees_an_overwritten_key_go_back_or_vanish`, `crates/cairn-store/tests/shared_wal.rs::g25c_open_snapshot_keeps_a_version_its_memtable_shadows`
**See.** `docs/reliability/irreversible.md` §0.3.

### G26 — Every manifest cairn writes keeps sequence-tracker format v1

**Statement.** SlateDB writes a v2 sequence tracker only when a delta of sequence numbers or timestamps overflows `i32`; cairn never sets sequence numbers by hand, so every manifest it produces keeps format v1.
**Why.** Older binaries panic when decoding a v2 tracker, which would break rolling upgrades and downgrades.
**Relied on by.** Mixed-version clusters and downgrades.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g26_every_manifest_keeps_sequence_tracker_format_v1`
**See.** `docs/reliability/irreversible.md` §0.3.

### G27 — Incremental compaction stays off

**Statement.** Cairn never enables SlateDB's experimental incremental compaction (`enable_experimental_incremental_compaction` stays `false` for every range DB's compactor), so no partial compaction is committed to the manifest and source SSTs leave the manifest only at the final commit.
**Why.** G8 and downgrades assume no partial commits exist.
**Relied on by.** G8; downgrades.
**Proven by.** `crates/cairn-store/src/config/open.rs::tests::g27_incremental_compaction_stays_off`
**See.** `docs/reliability/irreversible.md` §0.3.

### G28 — A clone taken after a full compaction references no deleted bytes

**Statement.** Cloning or unioning a DB after its full compaction (G23) has finished takes the compacted manifest, so the child references no SST still holding deleted rows, even while the source's old SSTs await GC.
**Why.** Children of a split or merge whose intent follows every source's compaction can inherit the source's "purged" proof instead of being rescanned.
**Relied on by.** Tenant purge (`tenant_purge::inherits`).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g28_clone_after_full_compaction_references_no_deleted_bytes`
**See.** `docs/reliability/irreversible.md` §0.3.

### G29 — A standalone GC run performs exactly the detach task on a DB with an open writer

**Statement.** A `GarbageCollector` built separately (public `GarbageCollectorBuilder`, detach options only, bounded retries) and run once on a DB with an open writer does exactly the detach work of that DB's own GC (G6); the writer rereads the changed manifest as it would after its own GC. Per-task errors are swallowed, so callers re-read the result. If it breaks, detach is only slower; no delete depends on it.
**Why.** It lets a child stop standing on its ancestors right after its full compaction instead of an hour later.
**Relied on by.** SM-24 (`ObjectStores::detach_now`).
**Proven by.** `crates/cairn-server/tests/split.rs::lineage_detaches_right_after_its_full_compaction`
**See.** `docs/reliability/irreversible.md` §0.3.

### G30 — The fork's compactor runs one job per destination sorted run

**Statement.** A compaction requested through `Admin::submit_compaction` skips the coordinator's destination check, so two requests of the same full compaction name one destination run. The fork keeps the second request `Submitted` until the first job ends, then validates it again; a reclaimed job whose destination another job owns goes back to `Submitted`; a worker never claims two jobs with one destination. If it breaks, the range stops serving (every open fails); no delete depends on it.
**Why.** The executor runs one task per destination and panics on a second one. Before the fork fix, that panic closed the Db and both jobs stayed in the ledger, so the range failed at every later open.
**Relied on by.** Tenant purge phase B and the lineage round (`ObjectStores::submit_full_compaction`), which two rounds or two nodes can call for the same range.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g30_two_full_compactions_with_one_destination_keep_the_db_open`
**See.** `docs/reliability/irreversible.md` §0.3.

### G31 — Only a stored `Completed` status proves a compaction finished

**Statement.** The compaction ledger (`.compactions`) keeps the active jobs and the finished job with the highest id; a finished job with a lower id leaves at a later write. A job that the newest version no longer holds proves nothing about how it ended. `Compacted` is not finished either: the coordinator can still fail it when it validates the output against the manifest. The fork writes every job that finished after the last stored version once with its final status before the trim drops it, so the outcome of every job is in some stored version.
**Why.** Tenant purge phase B read "absent" and `Compacted` as "done". A failed full compaction that a later job pushed out of the ledger then gave a clean range proof while the deleted rows were still in the SSTs.
**Relied on by.** `ObjectStores::full_compaction_done` (tenant purge phase B, the lineage round).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g31_a_job_that_left_the_ledger_is_not_done`
**See.** `docs/reliability/irreversible.md` §0.3.

### G32 — A full compaction keeps what an open snapshot needs

**Statement.** A compaction keeps every version that an open snapshot needs, also a compaction into the last run. The job's retention bound is the `recent_snapshot_min_seq` of the manifest that the worker reads after it claims the job; the flusher writes it (the oldest open snapshot, or `last_l0_seq` when none is open), and it does not go down. Cairn requests a full compaction for a purge only when, right after its flush, `recent_snapshot_min_seq >= last_l0_seq` in that manifest; otherwise it asks again on a later round.
**Why.** A query snapshot opened before a delete and still open when the compaction runs keeps the deleted bytes in a live SST, under a clean proof.
**Relied on by.** `ObjectStores::submit_full_compaction` (tenant purge phase B), `PartitionWriter::flush_memtable_fresh` (it writes a barrier key so that the flush stores a fresh manifest).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g32_a_full_compaction_keeps_what_an_open_snapshot_needs`
**See.** `docs/reliability/irreversible.md` §0.3.

### G33 — Cairn does not turn on trivial moves

**Statement.** The compactor options of every range Db set `enable_trivial_move: false` (SlateDB's default, set explicitly). A trivial move completes a compaction of non-overlapping SSTs by moving them into the destination run without rewriting a row, so it drops no tombstone.
**Why.** Tenant purge phase B treats a `Completed` full compaction as proof that the deleted rows left the SSTs.
**Relied on by.** `ObjectStores::compactor_options`; tenant purge phase B.
**Proven by.** `crates/cairn-store/src/config/open.rs::tests::g33_trivial_move_stays_off`
**See.** `docs/reliability/irreversible.md` §0.3.

## DC — disk-cache contracts

The optional node-local SST disk cache (`--disk-cache-*`, the fork's `CachedObjectStore`) may only make reads faster or slower, never wrong. These five contracts are pinned in `crates/cairn-store/tests/library_contracts.rs`; the code and the tests currently call them `D1`…`D5`, which collides with the roadmap decisions `D1`…`D27`, so the registry names them `DC1`…`DC5`.

### DC1 — The disk cache holds only compacted SSTs

**Statement.** Only compacted SSTs read or written with a DB's or reader's label are cached; WAL, manifests, unlabelled reads (all of cairn's own I/O) and compactor/GC reads go straight to the object store. Called `D1` in `library_contracts.rs` today.
**Why.** Nothing that decides durability or fencing (WAL, manifest, owner records, leases) may be read from node disk.
**Relied on by.** The disk cache as a whole; lease and fencing correctness.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::d1_disk_cache_holds_only_compacted_ssts`, `crates/cairn-store/tests/library_contracts.rs::d_on_real_s3`
**See.** `docs/reliability/irreversible.md` §0.3; `docs/runbooks/disk-cache.md`.

### DC2 — A corrupt cached part is refetched

**Statement.** A cached part that is corrupt (written by `rename` without fsync, torn by a crash) fails its block checksum, and SlateDB re-reads it once with `Refetch`, discarding the bad part. Called `D2` in `library_contracts.rs` today.
**Why.** Node disk is never a source of truth.
**Relied on by.** The disk cache.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::d2_corrupt_part_is_refetched`, `crates/cairn-store/tests/library_contracts.rs::d_on_real_s3`
**See.** `docs/reliability/irreversible.md` §0.3.

### DC3 — Cache keys are object paths, so ranges sharing one cache never read each other's data

**Statement.** The cache is keyed by object path, not by user key, so many range DBs sharing one node cache never read each other's entries. Called `D3` in `library_contracts.rs` today.
**Why.** One cache per node is shared by all its ranges (`ObjectStores::with_disk_cache`).
**Relied on by.** The per-node shared cache.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::d3_two_ranges_share_one_cache`, `crates/cairn-store/tests/library_contracts.rs::d_on_real_s3`
**See.** `docs/reliability/irreversible.md` §0.3.

### DC4 — Cache I/O errors fall back to the object store

**Statement.** A cache I/O error (full disk, permissions) means the part is not stored and the read falls back to the object store. Called `D4` in `library_contracts.rs` today.
**Why.** A broken local disk must cost latency, not availability.
**Relied on by.** The disk cache.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::d4_unwritable_cache_falls_back_to_the_store`
**See.** `docs/reliability/irreversible.md` §0.3.

### DC5 — The cache never needs invalidation

**Statement.** SST names are never-reused ULIDs and a DB reads only SSTs listed in its manifest, so a DB deleted by D-d and a new DB at the same path never read the old cached copy. Called `D5` in `library_contracts.rs` today.
**Why.** D-d and SlateDB GC then need no cache cleanup.
**Relied on by.** SM-9 and SlateDB GC (no cache invalidation step).
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::d5_gc_deleted_db_is_never_served_from_cache`
**See.** `docs/reliability/irreversible.md` §0.3.

## OS — object_store client assumptions

Cairn talks to the bucket through the `object_store` crate (0.14). These assumptions about that client decide how conditional writes, listing and errors are interpreted. The inventory currently calls them `O1`…`O6`, which collides with the inventory rows `O1`…`O4` (DB opens for write), so the registry names them `OS1`…`OS6`.

### OS1 — The client retries every 5xx, including conditional PUTs

**Statement.** object_store retries every 5xx response, including conditional PUTs, so `Precondition`/`AlreadyExists` may report this caller's own landed write and `Err` may hide a write that landed. Called `O1` in `irreversible.md` §0.3 today.
**Why.** Without read-back, a CAS winner could believe it lost or a loser that it may retry blindly.
**Relied on by.** AGENTS.md rule 7 (`cas::put_resolving`); SM-2, SM-22, SM-25.
**Proven by.** `crates/cairn-server/tests/s3_client_contract.rs::create_that_landed_then_5xx_is_reported_as_already_exists`, `crates/cairn-server/tests/s3_client_contract.rs::update_that_landed_then_5xx_is_reported_as_precondition`, `crates/cairn-store/src/cas.rs::tests::ambiguous_write_that_landed_is_written`, `crates/cairn-store/src/session/tests.rs::ambiguous_owner_cas_is_resolved_by_reading_back`
**See.** `docs/reliability/irreversible.md` §0.3.

### OS2 — `Create` and `Update(etag)` are real conditional writes

**Statement.** `PutMode::Create` fails with `AlreadyExists` when the object exists and `PutMode::Update(etag)` fails with `Precondition` when the etag moved; the store gate (D12) refuses a store that silently ignores the condition. Called `O2` in `irreversible.md` §0.3 today.
**Why.** Every CAS in cairn (table, owner records, sessions, ledgers) depends on it.
**Relied on by.** Every CAS; D12.
**Proven by.** `crates/cairn-store/src/config/mod.rs::tests::store_swallowing_conditions_is_rejected`, `crates/cairn-store/src/config/mod.rs::tests::wal_tier_failing_gate_still_blocks`, `crates/cairn-store/src/config/mod.rs::tests::memory_and_local_both_pass_gate`
**See.** `docs/reliability/irreversible.md` §0.3.

### OS3 — Client retries are bounded in time

**Statement.** The S3 client cairn builds from `s3://…` carries cairn's retry budget (`S3_CLIENT_RETRY_TIMEOUT`, 30 s) instead of object_store's default. Called `O3` in `irreversible.md` §0.3 today.
**Why.** A direct GET/PUT on an owner record, `cluster.conf` or session must not hang longer than lease and session budgets allow.
**Relied on by.** Lease and session deadlines; AGENTS.md §6 item 5.
**Proven by.** `crates/cairn-store/tests/store.rs::s3_client_carries_cairn_retry_budget`
**See.** `docs/reliability/irreversible.md` §0.3.

### OS4 — Deleting a missing object is success

**Statement.** DELETE of an absent object returns `Ok` or `NotFound`, and cairn treats both as done. Called `O4` in `irreversible.md` §0.3 today.
**Why.** Every delete path must be idempotent so a crashed round can simply be repeated.
**Relied on by.** Every delete path; SM-9, SM-16.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::partial_retired_db_delete_is_finished_next_round` (indirect)
**See.** `docs/reliability/irreversible.md` §0.3.

### OS5 — `list(prefix)` matches whole path segments

**Statement.** `list(prefix)` matches by path segment, so `ranges/1` is not a prefix of `ranges/10/…`. Pinned for `memory://`, `file://` and the fault-injecting store; not yet on real S3. Called `O5` in `irreversible.md` §0.3 today (the related risk is irreversible.md N5).
**Why.** Prefix deletes of a range DB or a dead WAL log would otherwise delete ranges 10–19 together with range 1.
**Relied on by.** irreversible.md X1, X2, X10, X17.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::o5_list_matches_whole_path_segments`
**See.** `docs/reliability/irreversible.md` §0.3.

### OS6 — Exhausted 5xx and connection errors surface as `Error::Generic`

**Statement.** After exhausting retries on a 5xx, or on a network/connection error, object_store returns `Error::Generic`; meaningful store answers (`412`, `409`, `404`, `NotImplemented`) are distinct variants. `retry::object_store_verdict` retries `Generic` on boot paths and concludes "the store ignores conditions" only from the other variants. Called `O6` in `irreversible.md` §0.3 today.
**Why.** A transient outage at boot must not be mistaken for a store that cannot do CAS.
**Relied on by.** The boot-time store gate (D12) and session open.
**Proven by.** `crates/cairn-server/tests/s3_client_contract.rs::exhausted_5xx_and_refused_connection_are_generic_and_transient`, `crates/cairn-store/src/retry.rs::tests::classifies_object_store_errors`
**See.** `docs/reliability/irreversible.md` §0.3.

## S3 — S3 store contracts

What cairn requires of the S3-compatible store itself, independent of the client library. The tests live in `crates/cairn-store/tests/s3_store_contract.rs` and are `#[ignore]` by default; they run against a real store named by `CAIRNFLOW_STORE_CONTRACT_URI`. SeaweedFS has passed all five; other stores are listed in `docs/self-hosting.md` §3 only after passing them or by provider documentation, as stated there.

### S3-1 — Reads and LISTs are strongly consistent after writes and deletes

**Statement.** A GET or LIST issued after a completed PUT or DELETE observes it.
**Why.** Several passes decide from a LIST that must not miss or resurrect objects.
**Relied on by.** LISTs of `owners/` (irreversible.md X10, R3), `ranges/` (X6), `nodewal/` (X9), and D-d.
**Proven by.** `crates/cairn-store/tests/s3_store_contract.rs::s3_1_read_and_list_after_write_and_delete`
**See.** `docs/reliability/irreversible.md` §0.3.

### S3-2 — PUT is atomic

**Statement.** A PUT, including multipart, leaves the whole object or nothing, even if the client is killed mid-upload. A WAL store not on the verified list must declare `--wal-atomic-put false`, and an `s3://` store without that declaration starts only if it is recognised as verified (`store_check.rs`, by endpoint or `Server` header).
**Why.** Resolving an ambiguous shared-WAL PUT by reading back is safe only if objects cannot be torn.
**Relied on by.** NW-2; seal objects and leases.
**Proven by.** `crates/cairn-store/tests/s3_store_contract.rs::s3_2_kill_mid_put_leaves_whole_or_nothing`, `crates/cairn-server/tests/wal_atomic_put.rs::unverified_s3_endpoint_without_declaration_refuses_to_start`, `crates/cairn-store/src/config/location.rs::atomic_put_tests::verified_s3_stores_by_endpoint_and_server`
**See.** `docs/reliability/irreversible.md` §0.3; `docs/self-hosting.md` §3.

### S3-3 — Conditional PUTs hold with versioning enabled

**Statement.** `If-None-Match`/`If-Match` keep their semantics on a versioned bucket; a delete marker reads as `NotFound`, and a later `Create` wins.
**Why.** Operators may enable bucket versioning as the last line of recovery without breaking CAS.
**Relied on by.** Every CAS on a versioned bucket.
**Proven by.** `crates/cairn-store/tests/s3_store_contract.rs::s3_3_conditional_put_holds_with_versioning`
**See.** `docs/reliability/irreversible.md` §0.3.

### S3-4 — Object `last_modified` tracks the node clock

**Statement.** An object's `last_modified` is close to the writing node's clock; the GC clock-skew gate re-checks this on every deleting round and deletes nothing when it fails.
**Why.** D-d falls back to `last_modified` to age tombstones without `retired_ms`, and the skew gate uses it as the reference clock.
**Relied on by.** SM-9 (clock gate), G9.
**Proven by.** `crates/cairn-store/tests/s3_store_contract.rs::s3_4_last_modified_tracks_the_node_clock`, `crates/cairn-server/tests/gc.rs::gc_rounds_delete_nothing_when_the_node_clock_runs_ahead_of_the_store`
**See.** `docs/reliability/irreversible.md` §0.3.

### S3-5 — A real store never serves a stale `304`

**Statement.** G18 holds on the real store: a conditional GET never returns a stale `304`, including from a second client.
**Why.** Quiet leases and fences read `304` as "unchallenged".
**Relied on by.** LS-16, LS-5.
**Proven by.** `crates/cairn-store/tests/s3_store_contract.rs::s3_5_conditional_get_never_serves_a_stale_304`
**See.** `docs/reliability/irreversible.md` §0.3; G18.


## F — patches in the SlateDB fork

Cairn builds on a fork of SlateDB 0.18 (`vendor/slatedb`). The patches are listed with their
upstream status in [`docs/architecture.md`](../architecture.md) §9; the codes below are what the
cairn code cites.

### F1 — Named WALs, a shared-WAL GC contract and a streaming reader

**Statement.** The fork records a `wal_name` in the manifest, lets a group of DBs in one process share a WAL through a trait contract (writer, reader, GC, admin aware of many DBs), and lets `DbReader` stream from a `WalReader`. Opening a DB with a different WAL kind is refused while its WAL holds rows.
**Why.** The per-node shared WAL (NW-*) needs SlateDB to treat the WAL as an external, named log.
**Proven by.** `crates/cairn-store/tests/library_contracts.rs::g12_wal_kind_change_refuses_a_db_whose_wal_has_rows`, `crates/cairn-store/tests/shared_wal.rs::wal_kind_mismatch_refuses_open`
**See.** NW-9, G12.

### F2 — Compaction and manifest refresh driven by signals

**Statement.** When the compactor runs in the writer's process, the writer signals new L0 and the compactor signals manifest changes, so polling is only a sparse safety net; `Db::wake_compactor` runs a submitted compaction without waiting for a poll (G24).
**Why.** Idle cost per DB drops to near zero, which is what makes many small ranges affordable.
**Proven by.** `crates/cairn-server/tests/idle_cost.rs::idle_single_process_partition_sends_almost_nothing`, `crates/cairn-server/tests/idle_cost.rs::idle_one_node_cluster_range_sends_almost_nothing`
**See.** G24.

### F3 — Clone probes the real WAL tail of its source

**Statement.** Before accepting a source, clone checks the source's actual WAL tail instead of trusting `next_wal_sst_id` from the stored manifest, and refuses a source with WAL not covered by its manifest; a clone of the latest state copies the WAL tail.
**Why.** Cloning an open DB could otherwise silently miss acknowledged rows.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::clone_refuses_unclosed_parent`, `crates/cairn-store/tests/range_clone.rs::unprojected_clone_of_open_parent_keeps_acked_rows`
**See.** SM-4, G10.

### F4 — A clone source can be projected onto several key ranges

**Statement.** Clone takes one view per intersection of a source SST with each target key range, so a child holds only its own key ranges (two per range under D21) without rewriting data.
**Why.** Split/merge stay metadata operations.
**Proven by.** `crates/cairn-engine/tests/keyspace_layout.rs::projection_keeps_every_owned_row`, `crates/cairn-store/tests/range_clone.rs::l0_headroom_counts_projected_views`
**See.** D21, SM-5.

### F5 — The WAL flush timer runs only while the WAL holds entries

**Statement.** The fork's periodic WAL flush does nothing while the current WAL is empty; block cache and background runtimes are shared across DBs through SlateDB's existing API.
**Why.** Memory, threads and idle requests must not scale with the number of ranges.
**Proven by.** `crates/cairn-server/tests/idle_cost.rs::idle_single_process_partition_sends_almost_nothing`
**See.** F2.

### F6 — Visible size of projected views

**Statement.** The fork exports `estimate_visible_size` for SST views and sorted runs, so a child that projects a view onto its parent's SST counts only the visible part.
**Why.** Without it a fresh child looks as large as its parent and the auto-splitter misjudges it.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::range_size_counts_only_the_visible_part_of_projected_views`
**See.** SM-20.

### F7 — Latest-metadata reads trust the GC boundary for half of `gc_min_age`

**Statement.** A reader's latest-manifest read skips re-reading the GC boundary object within a trust window of half `gc_min_age` (150 s with the default 5 minutes).
**Why.** Halves idle reads per reader; safe while clock skew stays below the window (G9).
**Proven by.** `crates/cairn-store/tests/store.rs::reader_poll_skips_boundary_get_each_tick`
**See.** G9.

### F8 — A closed-for-writes flag in the manifest

**Statement.** The fork adds a `closed` flag to the manifest, set by `Admin::close_for_writes_at_epoch` and cleared only by `Admin::reopen_for_writes_at_epoch` while `writer_epoch` is still the value the caller read; a closed DB refuses every writer.
**Why.** A split/merge source must stay closed even if a stale node tries to reopen it.
**Proven by.** `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_a_quiesced_split_source_closed`, `crates/cairn-model/tests/m1.rs::closed_flag_with_epoch_check_keeps_acks_for_split`
**See.** SM-17, G14.

## RD — historical regressions pinned by the models

Defects found and fixed during development; each is now a `Bugs` flag in a model and a
counterexample test. The code currently calls them `D1`–`D7` (see
[Collisions](#collisions-to-resolve-during-translation)).

### RD1 — A swallowed table-read error reopened a parent that had left the table

**Statement.** If reloading the range table failed and the error was swallowed, a node could reopen a parent that had already been split away and acknowledge writes into it before it retired. Opening a DB now requires a fresh table read; a failed read means "do not open".
**Why.** Those writes were lost when the parent retired.
**Proven by.** `crates/cairn-model/tests/m1.rs::d1_swallowed_table_read_error_acks_into_retired_parent`, `crates/cairn-model/tests/m2.rs::d1_stale_table_opens_a_source_whose_children_never_see_the_write`, `crates/cairn-server/tests/failpoints.rs::stale_table_never_reopens_a_parent_that_left_the_table`
**See.** LS-12.

### RD2 — A quiesced split/merge source was reopened while its coordinator lived

**Statement.** The DB-open path did not know a source was quiesced; a node that took its lease could reopen it, and writes made there were lost when the operation committed. Opening now waits for the quiesce mark and the closed flag (F8) refuses writers.
**Why.** Acknowledged writes were lost.
**Proven by.** `crates/cairn-model/tests/m1.rs::d2_open_path_unaware_of_quiesce_acks_into_retired_source`, `crates/cairn-server/tests/failpoints.rs::quiesced_split_source_never_reopens_while_coordinator_lives`
**See.** SM-13, SM-17.

### RD3 — Ambiguous conditional writes were treated as lost

**Statement.** The object-store client retries 5xx even for conditional PUTs, so a `Precondition` could be our own earlier success. Every conditional write now resolves ambiguity by reading back (`cas::put_resolving`).
**Why.** Misread outcomes stranded intents and caused false lease loss.
**Proven by.** `crates/cairn-server/tests/split.rs::ambiguous_intent_cas_never_strands_the_intent`, `crates/cairn-server/tests/split.rs::ambiguous_roll_back_cas_still_cleans_the_children`
**See.** SM-2, OS1.

### RD4 — Rolling back deleted the evidence before cleaning up

**Statement.** A roll-back removed a child's manifest and owner record even when reading its lineage or deleting its checkpoint on an ancestor failed, leaving a checkpoint pinning SSTs forever. The child DB is now kept until its checkpoints are gone.
**Why.** Leak, and loss of the only evidence a later round could act on.
**Proven by.** `crates/cairn-store/tests/range_clone.rs::discard_keeps_the_orphan_until_its_checkpoints_are_gone`
**See.** SM-12.

### RD5 — A coordinator rolled back by another node recreated its children

**Statement.** A coordinator that kept running after another node rolled its intent back could recreate child DBs and owner records. It now re-checks that the intent is still its own before every child-creating step.
**Why.** Stranded owner records and DBs.
**Proven by.** `crates/cairn-model/tests/m2.rs::d5_coordinator_rolled_back_by_another_strands_an_owner_record`, `crates/cairn-server/tests/failpoints.rs::coordinator_rolled_back_by_another_does_not_recreate_children`
**See.** SM-3.

### RD6 — `fork` did not protect the DBs its copy stands on

**Statement.** A forked cluster references its source cluster's ancestor DBs; the source's GC could delete them. `cairn-ctl fork` now sets a GC barrier on the source.
**Why.** The fork would lose data.
**Proven by.** `crates/cairn-server/tests/ctl.rs::source_gc_keeps_dbs_that_a_fork_stands_on`
**See.** SM-9.

### RD7 — Archive treated an existing object as "already archived"

**Statement.** A deterministic id that ran again and was purged on the same day found the archive slot taken and skipped archiving, then deleted the journal. Archive now reads back and compares, and writes the next slot `<id>.<n>` for a different run.
**Why.** Lost history.
**Proven by.** `crates/cairn-server/tests/archive.rs::rerun_of_idempotent_id_purged_the_same_day_is_archived`
**See.** `docs/reliability/irreversible.md` L1.


## Inventory rows of irreversible.md (X, L, S, M, O, A, R, N)

`docs/reliability/irreversible.md` §0.1 is an inventory of every action cairn takes that cannot be undone, with where it happens, what durable facts it decides on, the gap between read and act, and what happens on an unreadable or ambiguous read; §0.2 maps each row to the invariant, tests and tripwire that cover it, and lists open suspicions. Row codes are positions in that document, not invariants: the invariants are the SM, NW, LS, FL and G entries they point to. Code and docs refer to a row as "irreversible.md X8" (never a bare `X8`), because several letters collide with other code families (`L0` is SlateDB's level 0, `M1`–`M4` are protocol models, `S3-n` are store contracts, `A`/`R`/`L`-numbered tokens are also used for internal work items).

### X — irreversible deletes of cairn objects (inventory rows)

**Statement.** Rows X1–X17 list every delete of a cairn-owned object: D-d and its delete order (X1, X2), roll-back and orphan discards (X3–X6), owner-record deletes (X7), shared-WAL floor GC, sealing and forgetting dead logs (X8–X10), session, barrier and gate-probe deletes (X11–X13, X16), deployment and admin deletes (X14, X15) and tenant purge (X17). Each row records its durable inputs, its read-to-act gap and its behaviour on unreadable input.
**Why.** A complete list is what lets reviewers check that every delete path goes through a proof gate (SM-18) and the trash (SM-16).
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### L — logical deletes of customer data inside a DB (inventory rows)

**Statement.** Rows L1–L4 list logical deletes inside range DBs: retention purge after archiving (L1), removing outbox/inbound rows after acknowledgement (L2), schedule deletion (L3, not trashed by design) and tenant purge of rows (L4).
**Why.** These are not covered by the object trash, so each needs its own justification.
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### S — SlateDB deletes that cairn triggers (inventory rows)

**Statement.** Rows S1–S7 list deletes performed by SlateDB itself under cairn's configuration: SST GC, manifest GC, native WAL GC, detach, checkpoint expiry, removal of an orphan's checkpoint on an ancestor, and overwriting the WAL-kind mark. Cairn cannot gate them; their safety rests on the G assumptions and, as a last resort, bucket versioning.
**Why.** They show which irreversible actions are outside cairn's proof gates.
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2 (via §0.3).
**See.** `docs/reliability/irreversible.md` §0.1, §0.3.

### M — floor and watermark advances (inventory rows)

**Statement.** Rows M1–M6 list monotonic marks whose advance discards information: the shared-WAL GC floor, pruning old WAL segments from an owner record on take-over, clean and dirty release of segments, scan floors of in-DB indexes, and the confirmed replay mark.
**Why.** A mark advanced past durable state loses acknowledged rows even though nothing is "deleted".
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### O — opening a DB for write (inventory rows)

**Statement.** Rows O1–O4 list every place a range DB is opened for writing or created: becoming leader, sealing old logs on take-over, creating children by clone/union, and `cairn-ctl fork`.
**Why.** Opening the wrong DB for write is how acknowledged writes get lost behind a split; these rows are where SM-13, SM-17 and LS-12 apply.
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### A — external acknowledgements (inventory rows)

**Statement.** Rows A1–A2 list the points where cairn tells the outside world something is done: the `Committed` ticket behind every reply, SDK message and inter-range send (A1), and shared-WAL frame acks (A2).
**Why.** An ack cannot be taken back; INV-5 (ack only after `await_durable`) governs both.
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### R — range retirement and table CAS (inventory rows)

**Statement.** Rows R1–R7 list the commit CAS of a split/merge, the leader's tombstone, proxy tombstones, the manual `cairn-ctl range force-retire`, intent roll-back CAS, and marking a session expired. (Row R4, single-process tombstones, describes a path that no longer exists: since every node runs a session, SM-8 and SM-11 cover it.)
**Why.** These are the transitions that make a range or session permanently gone.
**Proven by.** Per row, in the coverage table of `docs/reliability/irreversible.md` §0.2.
**See.** `docs/reliability/irreversible.md` §0.1.

### N — suspicions recorded against the inventory

**Statement.** N1–N5 and N7–N12 (there is no N6) of `docs/reliability/irreversible.md` are suspicions raised while auditing the inventory, each with its current status in the document; they are history and open questions, not invariants. Some have since become pinned assumptions (N4: at a fixed table version, live ranges' `external_dbs` only shrink — now part of SM-9 and SM-14; N5: segment-wise LIST — now OS5).
**Why.** Keeping the reasoning next to the rows shows which gaps were closed and which remain by design. Refer to them as "irreversible.md N2"; a bare `N3` is the design principle registered above.
**Proven by.** Per item, in `docs/reliability/irreversible.md` §0.2 ("Suspicions").
**See.** `docs/reliability/irreversible.md` §0.2.

## Collisions to resolve during translation

The same short label means different things in different parts of today's code. Until the
translation pass rewrites these sites, read them as follows. After it, only the registered meaning
of a code may appear bare.

| Label as written today | Where | Registered as |
|---|---|---|
| `D1`, `D2`, `D4`, `D5`, `D6` | `crates/cairn-model`, `crates/cairn-split`, `crates/cairn-server/src/{split,node,ctl}`, `crates/cairn-store/src/{proof,failpoint,inspect}.rs`, split/GC tests, `docs/guides/split-merge.md`, `scripts/mutants.py` | `RD1`, `RD2`, `RD4`, `RD5`, `RD6` |
| `D3` | `crates/cairn-store/src/{cas,cluster}.rs`, `crates/cairn-store/src/session/tests.rs`, `crates/cairn-server/tests/split.rs`, `docs/guides/split-merge.md` | `RD3` |
| `D7` | `crates/cairn-store/src/archive.rs`, `crates/cairn-server/src/turn/timers.rs`, `crates/cairn-server/tests/archive.rs` | `RD7` (elsewhere `D7` is the two-tier decision) |
| `D1`–`D5` | `crates/cairn-store/tests/library_contracts.rs`, `docs/runbooks/disk-cache.md`, `deploy/prometheus/alerts.yml`, `AGENTS.md` | `DC1`–`DC5` |
| `O1`–`O6` | `docs/reliability/irreversible.md` §0.3, `crates/cairn-server/tests/s3_client_contract.rs`, `crates/cairn-store/tests/library_contracts.rs` | `OS1`–`OS6` (the §0.1 rows `O1`–`O4` keep their names as "irreversible.md O1") |
| `M1`–`M6` as inventory rows | `docs/reliability/irreversible.md` §0.1–0.2 | "irreversible.md M1" etc.; bare `M1`–`M4` are the models |
| `M5`, `M6` | `docs/runbooks/README.md` | monitoring items of that page; reword |
| `N1`–`N20` | `crates/cairn-server/src/{split,actor,session}`, `crates/cairn-store/src/nodewal`, `crates/cairn-model/src/m3.rs`, `crates/cairn-server/tests/{failpoints,sim_cluster}.rs`, `docs/guides/nodewal.md`, `docs/runbooks/node-fail-stop.md` | findings of an internal liveness audit and the irreversible.md suspicions; reword as prose. Bare `N3` is the design principle |
| `G1`, `G2` | `crates/cairn-server/src/node/http.rs`, `crates/cairn-server/tests/http_limits.rs` | HTTP accept-loop limits (header timeout, slowloris); reword. Bare `G1`, `G2` are SlateDB assumptions |
| `F4`, `F5`, `F8`, `F10`, `F12` | `crates/cairn-query/src/invocation.rs` | column groups of the query layer; reword. Bare `F<n>` are fork patches |
| `I1`–`I6` | `harness/chaos/workloads/billing.ts`, `harness/chaos/chaos.ts`, `scripts/check.sh` | invariants local to the billing workload, defined at the top of that file |
| `I7` | `crates/cairn-server/src/main.rs`, `crates/cairn-server/tests/profile_saas.rs` | planning history; drop |
| Model test names `d1_…`, `d2_…`, `d5_…`, `r1i_…`, `r1f_…`, `r631_…`, `r636_…`, `l1_…`, `l3_l4_…`, `l13_…`, `before_3g_c_…`, `before_w5r_…`, `before_w6q_…`, `f023_…` | `crates/cairn-model/tests/{m1,m2,m4}.rs` | rename after the regression they pin (`rd1_…` or a descriptive name) |
