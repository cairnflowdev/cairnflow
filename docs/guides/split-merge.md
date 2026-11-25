# Guide: split/merge, the range table and retired-range GC

*This is a map, and a hypothesis: it drifts faster than the code. Check in the code before
trusting it. Search by function name; the guide deliberately records no line numbers.*

The range table (`cluster.conf`) divides the `u64` partition-key space into contiguous ranges.
Each range is one Db and one engine. The partition key lives permanently in the first 8 bytes of
the id, so changing the table changes no id. Split/merge follow the intent protocol (D24): intent,
quiesce, clone, claim the children, commit, retire.

## 1. Invariants

- **SM-1. The table is always valid.** The ranges cover `0..=u64::MAX` with no gap and no
  overlap. Each id belongs to exactly one range. Ids are never reused (D22). A table change bumps
  the version by `+1` and `next_range_id` never decreases. A **new** intent only promises ids issued
  in that very CAS (`≥ next_range_id` of the previous table), so an issued id that left the table
  (or never entered it) and that no intent promises stays outside **forever** — SM-11 relies on
  this. Enforced in `PartitionTable::from_entries` and `check_successor`, not by callers.
  Tests: `crates/cairn-store/src/partition_table.rs::tests::decode_rejects_gap_and_overlap`,
  `crates/cairn-store/src/partition_table.rs::tests::equal_split_covers_u64_without_gaps`,
  `crates/cairn-store/src/cluster.rs::tests::update_enforces_no_id_reuse`,
  `crates/cairn-store/src/cluster.rs::tests::update_forbids_new_intent_promising_issued_id`.
- **SM-2. The table changes only by CAS** (`ClusterConfig::update`, `PutMode::Update` on the
  etag). Of two editors starting from the same version, exactly one wins. The loser receives
  `TableUpdate::Conflict` carrying the winner's table. Unknown lines from a newer binary are copied
  verbatim.
  Tests: `crates/cairn-store/src/cluster.rs::tests::table_update_is_cas`,
  `crates/cairn-store/tests/failpoints.rs::paused_update_loses_cas_to_writer_in_the_gap`,
  `crates/cairn-store/src/cluster.rs::tests::update_keeps_unknown_lines`.
  **An ambiguous CAS outcome is resolved by reading back** (RD3): object_store retries a 5xx
  itself, conditional PUTs included, so a write that landed may show the caller `Precondition`, or
  `Err`. `update` reads back and compares with the table it meant to write (`cas::put_resolving`):
  equal ⇒ `Applied`, different ⇒ `Conflict`, unreadable ⇒ `Err` (unknown). Consequence: two
  writers writing **identical** tables from the same version both see `Applied` — the bucket holds
  exactly the table each of them wanted; two roll-backs of the same intent both clean up the
  children (safe, because cleaning up after the CAS that dropped the intent is always correct). A
  coordinator whose intent CAS did not clearly win reads the table again and rolls the intent back
  if the range still carries **exactly** its intent (`undo_stray_intent`) — otherwise the intent
  stays stuck until the next leader change and auto-split stops cluster-wide. The content
  comparison only means something when nobody else can write the same bytes: child ids are issued
  from `next_range_id`, and a node never runs two coordinators on the same range
  (`ActiveIntents::hold` returns `None`). The same refusal makes the `ActiveIntents` set a counter
  capped at 1: a failed `hold` produces no guard that could remove the current holder's mark
  (`crates/cairn-server/src/split/tests.rs::failed_hold_never_releases_the_holders_ranges`).
  Tests: `crates/cairn-store/src/cluster.rs::tests::ambiguous_cas_resolves_to_applied_when_our_table_landed`,
  `crates/cairn-store/src/cluster.rs::tests::cas_that_really_lost_stays_conflict`,
  `crates/cairn-server/tests/split.rs::ambiguous_intent_cas_never_strands_the_intent`.
- **SM-3. There are two CAS points, and exactly one of them wins.** The intent CAS is the
  decision point, the commit CAS is the commit point. Rolling back (`roll_back`) does **the CAS
  that drops the intent first**, and only then cleans up the children's Dbs and owner records. A
  node that takes a range carrying an intent that none of its own coordinators is working on
  (`ActiveIntents`) rolls back before opening the Db. A split that sees the `upgrading` lock after
  its intent also rolls back.
  **Rolling forward from `Prepared`**: the taker of a source and the reconciliation loop (SM-15) do
  not roll back an op whose op ledger (SM-19) is at `Prepared` **and** whose direct check
  (`split::forward::prepared`) finds that every child has a Db and an owner record without a
  tombstone, and every source still carries a close mark at exactly `sealed + 1` (nobody reopened
  the source after it was closed, so the children have every acked write): they do the commit CAS
  in the coordinator's place (`split::roll_forward`, with the same table transformation
  `committed_table` — a slow coordinator that does its CAS later sees through `put_resolving` exactly
  the table it meant to write). The children carry owner records of the coordinator's session and
  are claimable once that session dies; the sources retire as on every commit. A missing ledger, a
  phase before `Prepared`, a failed read or a failed check ⇒ roll back as before (rolling back is a
  CAS — always safe). `cairn-ctl intent clear` never rolls forward. Every roll-back/roll-forward
  decision is `cairn_split::step` (`Event::Found`).
  Test: `crates/cairn-server/tests/split.rs::split_crash_before_commit_cas_rolls_forward`.
  Tests: `crates/cairn-server/tests/split.rs::split_crash_at_every_step`,
  `crates/cairn-server/tests/failpoints.rs::admin_failpoints_drive_a_real_split_failure`,
  `crates/cairn-server/tests/failpoints.rs::split_backs_off_when_upgrade_starts_after_its_intent`,
  `crates/cairn-server/tests/net_partition.rs::merge_rolls_back_cleanly_when_resign_is_cut`
  (the merge coordinator cut off from the other side's leader: a clean roll-back, no stranded
  intent — `--features testkit`).
  An ambiguous roll-back CAS still cleans up every child (SM-2):
  `crates/cairn-server/tests/split.rs::ambiguous_roll_back_cas_still_cleans_the_children`.
  **A coordinator rolled back by another node does not recreate children** (RD5): the node that
  takes the source rolls back when the coordinator hangs or loses its session for more than 2×TTL
  (`ActiveIntents` is local to that node). A coordinator that wakes up must not create a child Db or
  owner record afterwards — they would leak, with the record pointing to its **live** session. So
  before every step that creates children (clone/union, claiming the children) it reads the table
  on the bucket and demands that the intent is still **exactly** its own (`ensure_intent_ours`,
  `503` otherwise); and when rolling back, `roll_back_here` cleans up the children of its own intent
  that are permanently out of the table (`discard_own_orphans`, by the SM-14 rule) — a net for the
  window between the check and the creating step.
  Test: `crates/cairn-server/tests/failpoints.rs::coordinator_rolled_back_by_another_does_not_recreate_children`.
  Model M1 (`crates/cairn-model/src/m1.rs`): another node taking the source without rolling the
  intent back can open the quiescing source, the commit still wins, and acks are lost —
  `crates/cairn-model/tests/m1.rs::without_foreign_roll_back_another_node_opens_the_quiesced_source`.
  Model M2 (`crates/cairn-model/src/m2.rs`, which runs the `cairn_split::step` core **itself**): a
  roll-back that cleans up the children **before** the CAS that drops the intent (flag
  `sm3_discard_first`) no longer breaks anything — the core refuses to clean up children that are
  still promised before any proof (the "SM-3 reversed" row of
  `crates/cairn-model/tests/m2.rs::matrix`); planting RD5 back (flag `d5_recreates_children`) makes
  the owner record of a child without a Db point back at the coordinator's live session, and that
  child leaks —
  `crates/cairn-model/tests/m2.rs::d5_coordinator_rolled_back_by_another_strands_an_owner_record`.
- **SM-4. Clone only from a Db that is really closed.** `quiesce` answers after the Db has been
  sealed (WAL flushed, memtable flushed, closed), and the lease is still held so that nobody
  reopens the parent Db. F3 in the fork refuses a source that still has WAL the manifest does not
  cover. With the shared WAL, a source that still has frames not yet in L0 is refused too. So
  before clone/union the coordinator checks that no source's owner record still has a
  `wal_segment` (`ensure_wal_drained`): if one does ⇒ `Busy` (`503`) and roll back, rather than
  letting SlateDB return `InvalidCloneSourceWithWal` as a `500`. Gathering ranges (D-e) uses
  `Session::claim_from_others`: an owner record that already belongs to this session is not
  rewritten, so the keeper held by this node's own engine does not go stale.
  Tests: `crates/cairn-store/tests/range_clone.rs::clone_refuses_unclosed_parent`,
  `crates/cairn-store/tests/range_clone.rs::sealed_parent_clones_with_every_acked_row`,
  `crates/cairn-server/tests/quiesce.rs::quiesce_waits_for_inflight_turn_then_closes`,
  `crates/cairn-store/tests/shared_wal.rs::open_shared_parent_refuses_clone`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::split_when_clean_owner_cas_fails_after_seal_returns_503`,
  `crates/cairn-server/tests/net_partition.rs::merge_right_after_source_leader_dies_never_returns_500`
  (`--features testkit`).
- **SM-5. A child carries only the entity zone of its pk span.** The projection takes two byte
  ranges per source (`keyspace::owned_ranges`). The derived zone lies outside every range and is
  rebuilt when the child opens. `union_ranges` (merge) joins sources that do not overlap.
  Test: `crates/cairn-store/tests/range_clone.rs::clone_union_and_discard_follow_the_pk_span`.
- **SM-6. `WrongRange` (D23) is returned before the engine does anything.** The check runs twice:
  at HTTP (`ingress/internal.rs::{inbound, inbound_batch}` through `owning_engine`,
  `ingress/mod.rs::ask_range`) and in the actor (`dispatch::on_command`, by `Command::within` and
  the `retiring` flag). The sender learns the table from the `409` body and resends at once
  (`Transport::send`/`send_batch`). Ingress reloads the table and retries **exactly once**.
  **For batches**: the check runs **once for the whole batch** — table version, the range the
  sender names, intent — and **every** message must belong to exactly that range (at HTTP and again
  in the actor); one stray message is a `409` for the whole batch, and no message is written. The
  sender learns the table and then **regroups** every un-acked message by the new table (a stable
  partition by destination, keeping input order within each new destination) and keeps sending in
  the same send. **A sender holding an old table whose destination range no longer has a leader**
  (the `owns` hint is gone, the owner record was released by another session that is not dead — a
  tombstone after a split/merge included) probes the owner that just released it
  (`RoutingTable::releaser_of`): only a `409` carrying a newer table is kept to learn from, every
  other outcome is `NoLeader` as if there were nobody — the only way to learn the table when
  `cluster.conf` cannot be read.
  Tests: `crates/cairn-server/tests/wrong_range.rs::inbound_rejected_while_range_retiring`,
  `crates/cairn-server/tests/wrong_range.rs::stale_sender_recovers_after_table_change`,
  `crates/cairn-server/tests/wrong_range.rs::invoke_during_table_change_returns_result_not_503`;
  batches: `crates/cairn-server/tests/failpoints.rs::batch_split_between_gather_and_send_is_regrouped_by_the_new_table`,
  `crates/cairn-server/src/transport.rs::tests::regrouping_keeps_input_order_within_each_destination`;
  the owner that just released: `crates/cairn-server/tests/failpoints.rs::after_split_a_stale_node_learns_the_table_from_the_releaser`,
  `crates/cairn-server/src/transport.rs::tests::only_a_newer_wrong_range_table_survives_a_probe`.
- **SM-7. Acked messages and calls are neither lost nor duplicated across a split/merge.**
  Clients only see a retryable `503`. A batch is **one** commit on the receiving side: a split
  cuts before or after that commit, never in the middle — the child receives the whole batch or no
  message of it; a refused batch means the sender keeps all of its rows and resends them,
  deduplicated by key (IR-5).
  Tests: `crates/cairn-server/tests/failpoints.rs::inbound_during_split_quiesce_is_neither_lost_nor_duplicated`,
  `crates/cairn-server/tests/failpoints.rs::batch_split_between_gather_and_send_is_regrouped_by_the_new_table`,
  `crates/cairn-server/tests/failpoints.rs::batch_rejected_503_keeps_every_row`,
  `crates/cairn-server/tests/split.rs::split_under_load_in_process`,
  `crates/cairn-server/tests/split.rs::merge_gathers_range_from_other_node`,
  `crates/cairn-server/tests/split.rs::sync_invoke_waits_through_split`.
  The destination range's drainer acts on an **undurable** message (it is poked as soon as the
  receiving commit is submitted, LF-4) and then the range quiesces: quiesce cancels the receiving
  task (the sender gets no `200` and resends to the child) and the unfinished turn; `seal` (the
  barrier) covers every submitted write, cancelled writes included — the child has the message and
  the invocation already created, and the resend is deduplicated by id; if `seal` fails, the split
  rolls back and the range keeps running, still exactly once:
  `crates/cairn-server/tests/failpoints.rs::early_drained_inbound_survives_split_quiesce`,
  `crates/cairn-server/tests/failpoints.rs::early_drained_inbound_survives_a_failed_seal`.
  Messages arriving late or duplicated across a split (`--features testkit`):
  `crates/cairn-server/tests/net_partition.rs::stale_inbound_after_split_is_processed_once_in_right_child`,
  `crates/cairn-server/tests/net_partition.rs::duplicate_inbound_after_lost_ack_and_split_is_deduplicated`.
  Transient coordinator errors — lost leadership, a table change, **the source range's seal timing
  out or failing during quiesce** (a slow store) — are `503`, not `500`:
  `crates/cairn-server/src/split/tests.rs::leadership_errors_are_retryable`,
  `crates/cairn-server/src/split/tests.rs::seal_failure_during_quiesce_is_retryable`.
  The same rule applies to transient store I/O errors and to losing the lease / being fenced midway
  (directly or through the engine), and to **gathering the other range of a merge**
  (`gather_here`): the range still in someone else's hands after `settle`, or our intent rolled
  back by the node that took the range (SM-3 — the bucket is asked again every `INTENT_RECHECK`), is
  a `503`:
  `crates/cairn-server/src/split/tests.rs::store_and_lease_errors_are_retryable`,
  `crates/cairn-server/tests/failpoints.rs::merge_whose_source_is_taken_and_rolled_back_mid_gather_is_503`.
  The same rule applies to **claiming the child ranges** (`claim_children`): the node's session
  lost or suspended between the CAS of a child's owner record and the check after it
  (`Claim::Held(None)`) is a `503`; the child's owner record, pointing to an incarnation that was
  abandoned or is no longer valid, goes away with the child Db on the roll-back's cleanup path. Only
  `Held(Some(..))` — another live session holding a brand new child — is still a `500`:
  `crates/cairn-server/tests/failpoints.rs::merge_whose_session_pauses_while_claiming_the_child_is_503`,
  `crates/cairn-server/tests/failpoints.rs::merge_whose_session_is_lost_while_claiming_the_child_is_503`,
  `crates/cairn-server/tests/failpoints.rs::split_whose_session_pauses_while_claiming_a_child_is_503`.
  The coordinator never waits without a bound while holding the engine, an intent or a quiesced
  source: flushing the memtable before quiesce (`FlushMemtable` holds the engine) is bounded by
  `FLUSH_MEMTABLE_BOUND`, clone/union by `CLONE_BOUND`, claiming the children by `CLAIM_BOUND`;
  past the bound it is a `503` and a roll-back like every failed step.
  Tests: `crates/cairn-server/tests/split.rs::split_gives_up_when_memtable_flush_hangs`,
  `crates/cairn-server/tests/split.rs::split_gives_up_when_child_clone_hangs`,
  `crates/cairn-server/tests/split.rs::split_gives_up_when_child_claim_hangs`.
- **SM-8. A range that leaves the table is retired, not merely stopped.** `reconcile` calls
  `mark_retiring` and then `retire` — only when the table **read from the bucket**
  (`ClusterConfig::read_stored`) no longer has the range either: the RAM table may have come from a
  peer's `409` body without anyone reading the bucket (D23). A failed read ⇒ no range retires in
  that round. The tombstone (`retired`) stops every node holding an old table from claiming the
  parent Db again. Nodes follow the new table without a restart. If the leader dies before writing
  the tombstone, SM-11 writes it in its place. There are no session-less nodes any more: a node
  without `--cluster` is a one-node cluster, every engine has a keeper that writes the tombstone,
  and the SM-11 loop runs on every node (previously a single-process node wrote tombstones through
  a separate path, `gc::retire_alone`, which was its own class of bug). The other direction: a
  range **still** in the table whose engine has died (a job panic) makes `reconcile` remove the dead
  handle and rebuild the engine.
  Tests: `crates/cairn-server/tests/range_table.rs::node_follows_table_change_without_restart`,
  `crates/cairn-server/tests/failpoints.rs::range_job_that_panics_is_rebuilt`,
  `crates/cairn-server/tests/quiesce.rs::retired_range_is_never_reacquired`,
  `crates/cairn-server/tests/gc.rs::single_process_split_and_merge_leave_tombstones_that_gc_reaps`,
  `crates/cairn-server/tests/split.rs::range_is_not_retired_by_a_table_that_is_not_on_the_bucket`.
- **SM-9. D-d deletes a retired Db only when it proves nobody references it** (AGENTS.md rule 5).
  When in doubt, keep: a failed manifest read, a live Db without a manifest yet, a mismatched
  prefix. A **promised** child without a manifest yet is not a doubt: it can only stand on what
  its sources reference (G7), and the sources are live ranges that were read
  (`referenced_ranges_with`; previously every split/merge blocked the whole D-d round). Right before
  deleting, the round rebuilds the proof (`DeleteProof::retired_db`: table, tombstone, barrier,
  references, checkpoints) on **one** fresh table snapshot; the proof carries that snapshot's
  version and the delete reads the version again. A table that changed **since the plan** does not
  make the round give up: the plan is only a list of candidates, and the fresh proof does not rely
  on it — before this, under constant auto-splitting (the table changed on average every ~31 s, and
  plan plus one proof took ~30–40 s) nearly every round gave up at its first candidate and D-d
  deleted nothing for hours (found in a 4-hour soak; core: `Keep(TableMoved)` for that range only,
  the flag `dd_abandons_round` pins the old behaviour; M2:
  `::dd_reaps_in_a_round_whose_table_moved`). The reference-set scan reads live ranges' manifests
  in parallel (16), and **once per table version within a round**: the plan and the proofs of every
  candidate share `proof::RoundRefs` (`DeleteProof::retired_db_with`); a changed table version or a
  set with unknown entries ⇒ read again. This is safe because at a fixed version the
  `external_dbs` of live ranges only **shrink** (a live range's Db is created once before it
  enters the table; compaction and detach only remove) — an old set only makes us keep more; the
  core still demands that the set's version equals the proof's version, and the delete still
  reads the version again. Before this, each candidate's proof re-read the manifests of every live
  range: L × (1 + N) GETs per round. Tests:
  `crates/cairn-store/tests/range_clone.rs::n4_live_refs_only_shrink_at_a_fixed_version`,
  `crates/cairn-server/tests/gc.rs::dd_reads_live_manifests_once_per_table_version`; M2: the
  `refs` slot of `Gc::Planned`, `SOMETIMES_REAPED_ON_ROUND_REFS`. There is a `min_age`. The
  manifest is deleted last. Only the leader of the range with the smallest id runs this round; a
  node asks about that every `gc::RUNNER_CHECK_EVERY` (5 s) and rests a full `every` only after a
  round it actually ran — previously the first round waited a full `every` from node start, so a
  node living shorter than `every` never ran D-d
  (`crates/cairn-server/tests/gc.rs::first_dd_round_runs_soon_after_the_node_starts`). A failed read
  or delete of **one** range keeps that range (`unreadable`/`failed`, `cairnflow_gc_skipped_total`)
  and the round goes on with the other ranges — the delete conditions do not change. **Ages can be
  trusted only when the runner's clock agrees with the store**: before the first delete of the D-d
  round, of the proxy-tombstone / orphan-child round and of the trash round, the runner PUTs a
  `gc-clock-probe` object, HEADs it and compares `last_modified` with its own clock
  (`gc::measure_clock_skew`); a skew over `min_age/4` (floor 2 s; the orphan round uses the default
  `min_age`, the trash round `keep`) or a failed measurement ⇒ every delete of the round is dropped
  (`cairnflow_gc_clock_blocked_total{round}`, `cairnflow_gc_clock_skew_ms`). A round with nothing to
  delete does not measure. Only the **runner's** clock is measured: `retired_ms`, written by another
  leader at retirement, and SlateDB checkpoint expiry inside the writer process (G9) trust that
  node's clock.
  Test: `crates/cairn-server/tests/gc.rs::gc_rounds_delete_nothing_when_the_node_clock_runs_ahead_of_the_store`.
  Tests: `crates/cairn-store/tests/range_clone.rs::retired_parent_db_kept_while_children_reference_it`,
  `crates/cairn-server/tests/gc.rs::dd_round_survives_per_item_errors`,
  `crates/cairn-store/tests/range_clone.rs::retired_db_kept_when_reference_was_written_under_another_prefix`,
  `crates/cairn-server/tests/failpoints.rs::gc_round_reproves_when_split_commits_between_check_and_delete`,
  `::gc_keeps_reaping_when_the_table_moves_every_round` (the table changes in every round),
  `crates/cairn-server/tests/gc.rs::retired_db_younger_than_min_age_is_kept`,
  `crates/cairn-store/tests/range_clone.rs::partial_retired_db_delete_is_finished_next_round`,
  `crates/cairn-server/tests/gc.rs::gc_runs_only_on_leader_of_smallest_live_range` (`is_runner`),
  `crates/cairn-server/tests/failpoints.rs::gc_plan_uses_one_table_snapshot` (the plan uses **one**
  table snapshot; the reference set includes children in flight — a finding of M2),
  `crates/cairn-server/tests/gc.rs::promised_child_without_manifest_does_not_block_gc`.
  **A second fence: positive evidence from the retired Db itself.** The reference-set scan (table
  + `external_dbs` of live ranges) is **AND**ed with the Db's own manifest: no final checkpoint
  (without expiry) may remain that is named after a range **without a tombstone** whose Db still
  has a manifest (`ObjectStores::checkpoint_pins`; G15: clone/union name it after the child Db's
  path). Checkpoints of tombstoned ranges (retired: nobody opens or reads them as a Db) or of Dbs
  whose manifest is gone are skipped — **live** descendants have checkpoints of their own (G7 +
  G15, G6: only detach removes them); a name outside the prefix (`fork`) counts as still present;
  **no name** (checkpoints written before checkpoint naming) ⇒ unknown, keep
  (`cairnflow_gc_checkpoint_held{kind="unnamed"}`: how many an old bucket still has); a failed read
  ⇒ keep. Checkpoints with an expiry are not considered. The plan blocks with
  `BlockedBy::Checkpoint`, and the `retired_db` proof checks again. The scan says nobody, yet a
  **live** Db still has a named checkpoint ⇒ keep + tripwire `gc::retired_db::proofs_disagree`
  (`cairnflow_gc_proofs_disagree_total{direction="scan_clear"}`); the other direction (the scan
  sees a reference, the manifest has no checkpoint left) is a legitimate window of G4/G6 and is
  only counted (`direction="scan_referenced"`). Cost: one owner-record GET (and one manifest read
  when there is no tombstone) per named checkpoint, only for candidates that passed the scan.
  Tests: `crates/cairn-server/tests/failpoints.rs::dd_keeps_a_parent_its_children_name_when_the_reference_scan_is_blind`,
  `crates/cairn-store/tests/proof.rs::retired_db_proof_ignores_checkpoints_of_retired_ranges`,
  `crates/cairn-store/tests/library_contracts.rs::g15_clone_and_union_name_their_final_checkpoint_on_every_ancestor`;
  M2: the environment flag `dd_scan_sees` (off = a blind scan), `M2::unnamed_history`, the core
  flags `dd_no_checkpoint_fence`/`dd_unnamed_as_clear` — the "blind scan …" rows of `::matrix`.
  References from **outside** the table (a `cairn-ctl fork` copy under another prefix) are invisible
  to D-d: they go through GC barriers — `fork` puts a barrier on every range its copy's manifest
  lists (RD6), `crates/cairn-server/tests/ctl.rs::source_gc_keeps_dbs_that_a_fork_stands_on`.
  Model M2 (`crates/cairn-model/src/m2.rs`, invariant `NO_DELETE_UNDER_LIVE`: no Db is deleted
  while a live Db — in the table, a child in flight, an unexpired fork copy — still lists it;
  exhaustive): the core without flags holds for split, merge and a pre-migration bucket
  (`crates/cairn-model/tests/m2.rs::the_core_holds_for_split`, `::the_core_holds_for_merge`,
  `::the_core_holds_on_a_pre_migration_bucket`). Dropping `fork`'s barrier (planting RD6 back)
  **together with** the checkpoint fence (the copy puts final checkpoints on every Db it stands on)
  ⇒ the "fork without barrier (environment) + no checkpoint fence" row of `::matrix`; RD6 alone is
  caught by the checkpoint fence. A commit slipped in between `plan`'s table load and its candidate
  filter (the parent just retired, the children still standing on it) is blocked by **four**
  layers, each sufficient on its own at the model level: the core demands that candidates and the
  reference set come from the same table version (flag `dd_mixed_snapshots`), the version is
  checked again (against the snapshot the gate's facts stand on — the proof's, or the plan's when
  nothing is rebuilt), the `DeleteProof::retired_db` proof is rebuilt on a reloaded table, and the
  named-checkpoint fence; drop all four ⇒ `::dd_mixed_snapshots_need_all_four_layers_off`. Treating
  an "unknown" set as "unreferenced" **and** dropping the checkpoint fence ⇒ the "rule 5 + no
  checkpoint fence" row. Each M2 **environment** flag is anchored to a real name in the code
  (`m2::ANCHORS`, drift check `::anchors_exist_in_the_source`); the decision part is the core
  itself and needs no anchor.
- **SM-10. Discarding an orphan child does not touch the checkpoints of a live range.** The
  roll-back path of a **split** (found by a one-hour chaos run). SM-12 now holds the general rule
  for both split and merge; this test keeps the split shape.
  Test: `crates/cairn-store/tests/range_clone.rs::discarding_an_orphan_grandchild_keeps_the_live_parents_ancestry`.

- **SM-11. A range that left the table, whose owner died before writing the tombstone, is
  tombstoned by proxy — only when provable.** The tombstone is written by the leader when the
  engine retires (SM-8). If the leader dies between the table commit and that moment, nobody ever
  claims the range again, because it has left the table: the parent Db is orphaned forever (D-d
  only deletes tombstoned Dbs) and its WAL reference pins the log floor (NW-7). The proxy-tombstone
  round (`gc::OrphanRetirer`) runs on the D-d runner (`is_runner`), independently of the D-d flag
  because it deletes nothing. The round runs on **nudges** (`node/setup.rs`): (a) the table just
  finished a split/merge — commit or roll-back (`gc::intent_settled`); (b) the tracker saw another
  session just end (`SessionTracker::ended`) — exactly when (4) below has just become true; (c) the
  node just became the runner. A nudge reaching a node that is not the runner is dropped (the real
  runner receives it too; whoever becomes the runner later runs at once by (c)). A round left with
  unfinished work because of possibly transient errors (`OrphanRound::needs_retry`) retries with
  exponential backoff from 1 s; the safety net `--retire-orphans-every-s` (default 3600 s;
  previously 15 s) catches what no nudge reports (`cairn-ctl`, a missed nudge). Rule 5: slow
  cleanup only costs space. Nudge tests:
  `crates/cairn-server/tests/gc.rs::orphan_whose_owner_just_died_is_tombstoned_by_a_nudge` (b),
  `crates/cairn-server/tests/gc.rs::orphan_is_discarded_as_soon_as_a_node_becomes_the_runner`
  (c), `crates/cairn-server/tests/split.rs::orphan_of_a_half_done_roll_back_is_discarded_by_a_nudge`
  (a). It writes a tombstone for a range `r` whose owner record has no tombstone if and only if
  **all four** hold:
  1. `r` is **not** in the table at the version just reloaded (`cluster.reload`, committed);
  2. no intent of that table promises `r` (`PartitionTable::promised`). The children of a split/
     merge in flight have a Db and an owner record **before** the table commits; tombstoning one
     loses data if the commit wins;
  3. `r < next_range_id` (the id has been issued).
     The three above together are **permanent** (SM-1: an id that left the table with nobody
     promising it never comes back, not even through a new intent — `check_successor`). So the
     round does not have to hold the table still until its CAS, and does not need to re-check the
     table version as D-d does;
  4. the owner record has been released, or the owner's session is dead by **exactly the rule of
     `Session::claim`** (`is_session_dead`: marked by the tracker, or a GET that finds it gone /
     ended; a session never seen or unreadable is **not** dead). Whoever may claim the range again
     may also write its tombstone, and no stricter rule is applied.

  The tombstone is written by **CAS on exactly the version** of the record read in (4), as
  `retire` does: if the owner or a claimer changes the record in between, the CAS misses and the
  next round reads again. The tombstone copies every other field (session, term, `data`), **drops**
  `wal_segment` (`gc::tomb_segments`: only a claimer of the range reads it, and a tombstoned range
  is never claimed again — copying the dead owner's segment would mean that log could never be
  removed for good; the same rule applies to `cairn-ctl range force-retire`) and adds `released`,
  `retired`, `retired_ms` = the time of writing — exactly what the leader would write, so D-d's
  `min_age` counts from here. Every doubt means **keep**, with a reason (`OrphanKept`): a failed read
  of the record or the session (`unreadable`), an unknown `wal_kind` from a newer binary
  (`unknownWalKind` — rewriting the record would delete its line), an owner not proven dead
  (`ownerLive`, including the session of the node running the round), a missed CAS (`casLost`). A
  LIST/table-load error fails the whole round, writing nothing. Right on the "about to CAS the
  tombstone" branch sits the tripwire `gc::retire_orphan::before_cas`.

  **The upgrade gate: honoured.** `upgrading` ⇒ the whole round is skipped, as for D-d. Not a
  safety condition (the CAS is the arbiter), but so as not to write over `owners/` while a
  migration step may be walking through it; waiting is cheap. **GC barriers: not honoured.** A
  barrier keeps a Db from being *deleted*; a tombstone deletes nothing, and D-d still checks
  barriers right before deleting. Waiting for barriers here would only prolong the time the WAL
  floor is pinned. **No `min_age` of its own**: (4) is already a quiet period of about a lease TTL,
  and the retired Db still goes through D-d's `min_age` before it is deleted.

  The same round also cleans up orphan children of a roll-back (SM-14), and so LISTs all of
  `ranges/` — then one LIST per directory to know whether it still holds objects (the filer keeps
  empty directories), but **only for directories of ranges outside the RAM table and not yet known
  to be tombstoned** (`range_dirs_where`): other ranges are filtered out of the candidate set right
  after anyway, so the candidate set is exactly as before.
  Not covered (manual path `cairn-ctl range force-retire`): a `ranges/<id>` directory with neither
  an owner record nor a manifest (a directory with a manifest but no owner record is cleaned up by
  SM-14), and a record pointing to a session that stays alive forever while no engine holds the
  range. The known path to the latter — a coordinator waking up after being rolled back by another
  node — is closed by SM-3 (RD5).
  Tests: `crates/cairn-server/tests/failpoints.rs::parent_left_without_tombstone_is_retired_after_its_leader_dies`,
  `crates/cairn-server/tests/failpoints.rs::inflight_split_children_are_never_tombstoned`
  (`panic` tripwire), `crates/cairn-server/tests/gc.rs::orphan_retirer_writes_only_what_it_can_prove`,
  `crates/cairn-server/tests/failpoints.rs::orphan_tombstone_whose_cas_loses_is_kept_and_retried`
  (`casLost`: the record changed between the read and the CAS).
  Model M2: dropping (2) in the cleanup/tombstone round (flag `sm11_ignores_promised`) ⇒ a child in
  flight whose ledger is at `Prepared` (the coordinator died before the commit CAS) takes the
  tombstone branch, gets a tombstone, then D-d deletes its Db and its final checkpoint stays on the
  live source (the "SM-11 (2)" row of `tests/m2.rs::matrix`, `NO_ORPHAN_CHECKPOINT_ON_LIVE`). The
  child-cleanup path is still blocked by the `abandoned_child_db` gate (it checks "not promised").
  With nobody writing proxy tombstones (flag `sm11_no_proxy_tomb`) ⇒ a committed source stays
  without a tombstone forever —
  `crates/cairn-model/tests/m2.rs::sm11_without_proxy_tombstones_a_committed_parent_is_never_retired`.

- **SM-12. Rolling back a split/merge deletes only the checkpoints the orphan child itself
  created.** `discard_range_db(child, sources)` only deletes the `final_checkpoint_id` of each
  `external_dbs` entry in the orphan child's manifest. SlateDB issues new ids for every entry on
  each clone/union and writes them into the child's manifest **before** creating the checkpoints
  on the ancestors, so that manifest is the write-ahead record of exactly what the clone placed on
  the ancestors (the same rule as SlateDB's `Admin::delete_db` and detach). It **never** deletes
  `source_checkpoint_id`: in an inherited entry, that is the final checkpoint of a **live source**;
  in a direct source's entry, it is the clone's temporary checkpoint, which expires by itself after
  300 s. Before deleting, it reads the manifest of every Db in the lineage (the paths in
  `external_dbs` plus the intent's live sources) and **keeps** any checkpoint still mentioned,
  through the tripwire `split::discard::referenced_checkpoint`. A failed read deletes nothing.
  Why not derive it from the child's manifest: a union merges the inherited entries of the two
  sources that share ancestors, and only adds an entry for a direct source while it still owns SSTs
  after the projection. Two ranges that were just split and have written nothing are entirely
  absent from the merged Db's manifest (the `seal_barrier` key lies outside every range). The old
  SM-10 only protected checkpoints mentioned by the manifests of the paths present, so rolling back
  a merge of 40+41 deleted the checkpoint that 40 stood on (found in a chaos run).
  `cairn-ctl verify` reports this consequence as `external_db` (`source Db lost checkpoint … that
  this range stands on`). **Except for SlateDB's detach window**: a child's GC `detach` removes the
  final checkpoint on the parent **first** and only then drops the entry from the child's manifest,
  and only does so for entries that stand on none of the parent's SSTs. An entry that lost its
  checkpoint while standing on no SST — in the current manifest and in every manifest the child's
  live checkpoints pin (`inspect::detach_pending`, exactly the `find_detachable` condition) — is
  `ExternalStatus::Detaching`, not lost. Still standing on SSTs in any view means it is still
  `CheckpointMissing`.
  Test: `crates/cairn-store/tests/range_clone.rs::missing_checkpoint_of_an_entry_without_parent_ssts_is_a_pending_detach`
  (with a mutation that really deletes the checkpoint).
  **Keep the evidence until the cleanup is done** (RD4; Neon deletes the index last for the same
  reason): the orphan child's manifest is the only record of the checkpoints it placed on its
  ancestors — when rolling back a split, that ancestor is a **live** parent range, and a checkpoint
  without expiry pins its SSTs forever. So a failed lineage read or a failed checkpoint delete ⇒
  `discard_range_db` returns an error **without deleting the child's Db**, and the roll-back
  (`split::discard_orphan`, `cairn-ctl intent clear`) keeps the child's owner record too; the
  orphan-child round (SM-14) finishes the job. An ancestor without a manifest (D-d already deleted
  the whole Db) counts as having had its checkpoint removed.
  The fork sets the final checkpoint's `CheckpointOptions.name` to the child Db's path, so from the
  ancestor's side it is known which clone a checkpoint belongs to (`RangeCheckpoint::created_by`);
  `cairn-ctl verify` reports `orphan_checkpoint` (f) for a final checkpoint on a **live** range
  whose creating child is permanently out of the table. Checkpoints created before that fork version
  have no name — they cannot be judged.
  Tests: `crates/cairn-store/tests/range_clone.rs::discard_keeps_the_orphan_until_its_checkpoints_are_gone`,
  `crates/cairn-store/tests/failpoints.rs::discard_keeps_the_orphan_when_deleting_its_checkpoint_fails`.
  Model M2 (invariant `NO_ORPHAN_CHECKPOINT_ON_LIVE`, i.e. `verify`'s (f)): a failed cleanup that
  still deletes the child Db (planting RD4 back, environment flag `keep_db_until_checkpoints_gone`)
  so the checkpoint stays on the live source — the "half Db (environment)" row of
  `crates/cairn-model/tests/m2.rs::matrix`. The "keep the owner record" half alone breaks no safety
  invariant (the Db remains, no owner record ⇒ the second SM-14 evidence sees it). M2 does not model
  `source_checkpoint_id` or a union dropping a source that no longer has SSTs (the shape of that
  chaos run).
  Tests: `crates/cairn-store/tests/range_clone.rs::discarding_an_orphan_merge_keeps_the_sources_ancestry`,
  `crates/cairn-server/tests/failpoints.rs::merge_roll_back_keeps_checkpoints_the_sources_stand_on`
  (`panic` tripwire, the merge failing at `merge::ownerClaim` on one node only),
  `crates/cairn-store/tests/failpoints.rs::discard_tripwire_fires_and_keeps_a_referenced_checkpoint`.

- **SM-13. Nobody opens the Db of a range that is the source of an in-flight intent for
  writing** (RD2). From the moment the coordinator quiesces the source until it commits or finishes
  rolling back, writes to the source are not in the cloned child (or the union Db), yet the commit
  CAS still wins because the table has not changed. Two fences:
  - **another node** taking the source sees an intent that none of *its* coordinators is working on
    ⇒ it rolls back (SM-3) — the CAS that drops the intent makes the commit CAS miss;
  - **the coordinating node itself** takes the source again (the engine in `quiesced()` lost its
    lease, or the session was lost and a new incarnation came up): `ActiveIntents` only says "do
    not roll back", so the coordinator also marks the source **as quiesced**
    (`ActiveGuard::mark_quiesced`) right **before** the quiesce command — no earlier, because the
    other range of a merge is gathered and opened on this node (`gather_here`) before that.
    `factory` meeting that mark takes the lease and then **waits**
    (`ActiveIntents::wait_unquiesced`, holding the lease, re-taking it if lost) until the mark is
    removed (the coordinator's guard drops: after retirement, or after the roll-back has CASed the
    intent away), then reads a fresh table as usual. Right before the place that opens the Db sits
    the tripwire `node::open::quiesced_source`: a range that still has the mark there is not
    opened.
  Consequence: the source neither compacts nor detaches while children are in flight — the
  assumption D-d relies on. Model M2 shows that D-d has **four** layers here, each sufficient on its
  own: a quiesced source is opened by nobody (SM-13 together with the SM-17 close mark — M2 models
  the Db opener itself), SlateDB's G6 (detach does not drop an entry whose final checkpoint the child
  still pins on the source), D-d reading the `external_dbs` of children in flight, and the named
  checkpoint fence (a child in flight has already placed a named final checkpoint on its
  ancestors); drop all four and D-d deletes an ancestor under a child in flight —
  `crates/cairn-model/tests/m2.rs::n2_in_flight_children_rest_on_four_layers`. The
  `abandoned_child_db` gate also counts children in flight, so the orphan-child round is no longer a
  hole on pre-migration buckets. The "a closed source is compacted by nobody" half of SM-13 is
  pinned in
  `crates/cairn-store/tests/library_contracts.rs::g5_quiesced_closed_db_gains_no_objects_under_compactor_and_gc`.
  Tests: `crates/cairn-server/tests/failpoints.rs::quiesced_split_source_never_reopens_while_coordinator_lives`,
  `crates/cairn-server/tests/failpoints.rs::quiesced_merge_sources_never_reopen_while_coordinator_lives`
  (the coordinator stops after clone/union, A loses its session, the source is taken again; the
  tripwire counts through a callback).
  Model M1 (`crates/cairn-model/src/m1.rs`, invariants `SM13_QUIESCED_SOURCE_CLOSED` and
  `NO_ACK_INTO_RETIRED_DB`, exhaustive): with every fence in place it holds for split
  (`crates/cairn-model/tests/m1.rs::all_fences_hold_for_split`) and merge
  (`all_fences_hold_for_merge`, `#[ignore]`, run by hand); dropping `wait_unquiesced` and the
  tripwire (planting RD2 back) makes the counterexample the coordinating node's own path:
  `crates/cairn-model/tests/m1.rs::d2_open_path_unaware_of_quiesce_acks_into_retired_source`,
  `crates/cairn-model/tests/m1.rs::d2_also_breaks_merge`. The fence on other nodes (SM-3) relies on
  LS-12's fresh table: planting RD1 back also breaks SM-13.

- **SM-14. Orphan children of a roll-back are cleaned up for good — only when provable.** A
  roll-back cleans up the children at once (SM-3), but may stop halfway: a failed Db cleanup
  (SM-12), a roll-back CAS with an unclear outcome (SM-2), a coordinator waking up after being
  rolled back by another node (RD5). The proxy-tombstone round (`gc::OrphanRetirer`, on the D-d
  runner) LISTs both `owners/` and `ranges/`, and cleans up a range `r` (`discard_range_db`, then
  the owner record) if and only if: (1)–(3) of SM-11 hold (outside the table just reloaded, promised
  by no intent, id issued — permanently out of the table) **and** one of two pieces of evidence
  shows that `r` never entered the table:
  - `r`'s Db is listed in the `external_dbs` of a range **that is in that table**. Committing a
    split/merge takes every source out of the table and sources never come back (ids are not
    reused);
  - `r`'s Db has a manifest while `r` **has no owner record**. The coordinator claims every child
    (`claim_children`) **before** the commit CAS, and the owner record of a range that ever entered
    the table is never deleted while its Db still has a manifest: tombstones are permanent, D-d
    only deletes Dbs, and every other `delete_owner` path only cleans up children of an intent the
    CAS dropped or ranges whose Db is gone. This evidence holds even when every source has retired
    — the gap in the first evidence: the roll-back died in the middle of cleaning up the children,
    then a later merge took the sources out of the table (`ranges/28` in a chaos run). **Whoever
    adds a path that deletes owner records must keep this condition** (delete only for ranges that
    never entered the table, or whose Db has no manifest left).

  **Evidence from the op ledger** (SM-19): the ledger of the op that created `r` (`ops/r` or
  `ops/r-1`) at `Cleaned` is **positive** evidence that `r` never entered the table (the CAS that
  dropped the intent won) — it replaces the two inferences above, so a child that **already claimed
  its owner record** and stands only on retired sources is cleaned up too (formerly "not
  covered"); a ledger at `Prepared`/`Committed` is **counter**-evidence — the round leaves `r` to
  the tombstone branch (SM-11) and D-d, and the `abandoned_child_db` gate refuses (every child
  cleanup path). No ledger, an unfinished ledger or a failed read ⇒ the two inferences as above (the
  gate: failed read ⇒ refuse). Every decision is `cairn_split::step` (`Event::Discard`,
  `Event::Gate`).
  Test: `crates/cairn-server/tests/split.rs::claimed_orphan_is_discarded_by_the_ledger_after_its_source_retires`.

  **Then check directly what the delete needs**, because the two pieces of evidence above are
  inferences from the bucket's history, and the second one is **wrong** on a bucket that a
  single-process node split before the range-level migration framework existed: a retired parent
  there has no owner record while its children still stand on it. Clean up only when **all three**
  hold, for both pieces of evidence:
  1. no range in the table (same version as (1)–(3)) lists `r` in `external_dbs` — exactly D-d's
     set (`ObjectStores::referenced_ranges`); a live range's manifest unreadable or missing, an
     unknown prefix ⇒ keep (`referenced`). A real reference found ⇒ keep, `warn`, through the
     tripwire `gc::discard_orphan::referenced`;
  2. no GC barrier covers `r` (`barrier`) — references from outside the table (`fork`) are only
     visible through barriers (SM-9);
  3. the table version read again right before deleting is still that version (`tableMoved`), as
     for D-d.
  With the first evidence, (1) can never see anything (nobody clones a child that has not
  committed); it is applied there too because a bucket built by an old binary or by an operator's
  hand may break the history the inference relies on, and the reference set is shared. The set is
  read lazily (only once a candidate has evidence) and kept across rounds until the table version
  changes (`gc::LiveRefs`: at a fixed version the `external_dbs` of live ranges only shrink), unless
  something could not be proven.

  Never entered the table and nobody stands on it ⇒ nobody reads it. Not provable (no manifest, an
  owner record while it only stands on ranges that left the table, a mismatched prefix) ⇒ left to
  SM-11/D-d; a failed owner-record read or a failed cleanup ⇒ keep (`discardFailed`), the next round
  continues. The owner need not be dead: the table is the arbiter, not the lease. Not behind the D-d
  flag (this is the roll-back path's job), no `min_age` (no reader opens a child that has not
  committed). Tombstoning and then letting D-d delete the Db would leave the child's final
  checkpoint on the live source forever — so cleanup comes **before** tombstoning. The tripwire
  right on the cleanup branch: `gc::discard_orphan::before_discard`.
  Not covered: a child that **already claimed its owner record** and stands only on ranges that left
  the table, whose op has **no ledger** (a binary from before ledgers) or whose ledger has not
  reached `Cleaned` (the roll-back died between the CAS and the ledger write); it is tombstoned
  (SM-11) once its owner dies and D-d deletes its Db, while its checkpoint on the ancestor goes with
  the ancestor when D-d deletes the ancestor. A `ranges/<r>` directory without a manifest (a cleanup
  stopped after the manifest was gone — does not happen with the current delete order) is kept. A
  retired parent without an owner record on a pre-migration bucket: kept as long as children stand
  on it; once the children have all `detach`ed, the round cleans it up (nobody stands on it, but it
  does not go through D-d's `min_age`) — having no tombstone, D-d never sees it.
  Tests: `crates/cairn-server/tests/gc.rs::orphan_round_keeps_retired_parent_without_owner_that_children_stand_on`
  (a parent without an owner record, children standing on it: kept as `referenced`; the node is
  rebuilt and everything is read again), `crates/cairn-server/tests/gc.rs::orphan_round_still_discards_true_orphans_nobody_stands_on`
  (the other direction, second evidence),
  `crates/cairn-server/tests/failpoints.rs::orphan_round_trips_the_referenced_wire_on_a_pre_migration_parent`
  (the tripwire on the right branch),
  `crates/cairn-server/tests/split.rs::half_done_roll_back_is_finished_by_a_later_round`
  (`verify` too sees `orphan_checkpoint` before the round and is clean after it),
  `crates/cairn-server/tests/failpoints.rs::orphan_of_half_rolled_back_merge_is_discarded_after_its_sources_retire`
  (second evidence; the round does not clean up tombstoned retired sources; tripwire `referenced` =
  `panic`),
  `crates/cairn-server/tests/failpoints.rs::inflight_split_children_are_never_tombstoned`
  (`panic` tripwire: a child in flight is never cleaned up).
  Model M2 (`crates/cairn-model/src/m2.rs`): the premise of the second evidence is the invariant
  `SM14_OWNER_OUTLIVES_DB` (the owner record of a range that ever entered the table remains as long
  as its Db has a manifest), which holds under every existing fence. Dropping the direct check is
  red only on a bucket whose history breaks that premise (a single-process node before the
  migration framework): the round cleans up an ancestor or a parent without an owner record under a
  live range —
  `crates/cairn-model/tests/m2.rs::r1i_orphan_round_trusting_history_discards_a_parent_in_use`.
  `LiveRefs` is keyed by version: the core refuses a set of another version; dropping that check
  (flag `n4_any_refs_version`) is still caught by the gate (which reads a fresh set) — the matrix
  row for it in `tests/m2.rs::matrix`. A second op on a source retired after a roll-back that died
  halfway: the second evidence covers it while there is no ledger; a `Cleaned` ledger is positive
  evidence —
  `crates/cairn-model/tests/m2.rs::r1f_orphan_of_a_half_done_roll_back_after_its_sources_retire`.

- **SM-15. Split/merge operations belong to the node, not to a request; no intent stays behind
  once nobody coordinates it.** The intent on the table is the source of truth. SM-3 only rolls
  back when **the source's leader changes** (`factory`); while the leader is alive, each way a
  coordinator can disappear has a cleanup layer:
  1. **No dying with the request.** `RangeOps::start` runs the operation in a task of the node and
     records it in the op book (`OpBook`: `GET /ranges/ops`, `/ranges/ops/{id}`, RAM only). The
     HTTP handler only waits for a ticket: when the client disconnects hyper drops the handler, and
     the task goes on. A supervising task wraps the operation: a panic becomes `failed` in the book.
  2. **A quiesced source is tied to the coordinator's lifetime.** The quiesce command carries a
     hold wire (`EngineHandle::quiesce_held`) whose sending end lives in the `ActiveGuard`. The guard
     drops while the engine is still in `quiesced()` (a panic, a cancelled task) ⇒ the engine
     releases the lease and takes it again as for `Resign` ⇒ `factory` sees an intent none of the
     node's coordinators is working on ⇒ roll back (SM-3). The `select!` of `quiesced()` is
     `biased`, commands first: the coordinator sends `Retire`/`Resign` and waits for the answer
     **before** dropping the guard, so the ordinary path never sees the wire cut.
  3. **A roll-back does not give up.** A coordinator (and `undo_stray_intent`) only rolls back
     **exactly** its own intent (`roll_back_if`/`roll_back_matching`: another intent on the range
     belongs to a later attempt and is not touched). A failed roll-back ⇒ retried in the background
     with exponential backoff (`settle_in_background`, limit `SETTLE_FOR` = 10 minutes), not just
     one `error!` line.
  4. **The intent reconciliation round** (`IntentReconciler`, on the D-d runner — `is_runner`,
     tick `--reconcile-intents-every-s`, default 15 s; it used to share the
     `--retire-orphans-every-s` tick): each round reloads the table; for each intent it asks the node
     leading each source (`GET /internal/ranges/{id}/coordinator`) — a coordinator always leads its
     sources, and a node that took a source from it has already rolled back in `factory`. Nobody
     holding it for **two rounds in a row** with the **same** intent ⇒ the core decides: an op that
     finished preparing ⇒ roll forward (SM-3), otherwise `roll_back_if`. Cannot ask ⇒ treated as
     held. When the commit wins the sources have left the table and there is no intent left to see
     (the part after the commit is SM-8/SM-11).
  Safety does not rest on round 4's answer: a roll-back is a CAS, the counterweight of the commit
  CAS (SM-3); wrongly rolling back an operation that is still alive (an old routing table) only
  makes it fail, and it does not recreate children (RD5). Tripwire `split::reconcile::roll_back`
  right on the roll-back branch; `split::reconcile::held` when the round sees a holder.
  Not covered: a coordinator that **hangs** (does not die) midway — clone/union or an unbounded wait
  — keeps the source quiesced until it returns; round 4 sees it as still holding.
  Tests: `crates/cairn-server/tests/failpoints.rs::client_disconnect_mid_split_never_strands_the_source`,
  `crates/cairn-server/tests/failpoints.rs::client_disconnect_mid_merge_never_strands_the_sources`
  (the client closes its socket while the coordinator is stopped after clone/union),
  `crates/cairn-server/tests/failpoints.rs::coordinator_panic_after_quiesce_releases_and_rolls_back_the_source`
  (the hold wire),
  `crates/cairn-server/tests/failpoints.rs::merge_failing_before_quiesce_with_failed_roll_back_leaves_no_intent`,
  `crates/cairn-server/tests/split.rs::torn_intent_cas_with_failed_rereads_is_rolled_back_later`
  (roll-back in the background),
  `crates/cairn-server/tests/split.rs::stale_intent_without_coordinator_is_rolled_back_by_the_runner`,
  `crates/cairn-server/tests/failpoints.rs::reconciler_never_rolls_back_an_intent_a_live_coordinator_holds`
  (the reconciliation round; `panic` tripwire, the coordinator on another node).
- **SM-18. Every deletion of customer data goes through a typed proof gate.**
  `delete_retired_db`, `discard_range_db`, `delete_owner` and `LogStore::delete_all` demand a
  `cairn_store::proof::DeleteProof` for **exactly** the thing they delete; a proof can only be built
  by a function that itself reads durably exactly the facts needed: D-d (`retired_db`: reloaded
  table, out of the table, not promised, tombstoned, no barrier, referenced by no live range or
  child in flight — based on the version, and the delete reads the version again), an abandoned
  child (`abandoned_child_db`: out of the table, not promised, id issued, **and** (1)–(3) of SM-14
  right inside the builder — referenced by no live range or child in flight, no barrier, based on
  the version — because a retired parent satisfies the first three conditions too; `_owner`: the
  owner record only once the Db has no manifest left — RD4 turned into a type), a dead log
  (`dead_log`: the death and the seal are read **before** the owner-record snapshot, the type
  enforces the order), an op ledger (`settled_op`: the ledger at an outcome, old enough, every range
  of the op settled — permanently). Lint (`scripts/lint-sources.sh` check 4) blocks new direct object
  deletes and `DeleteProof::assume*` outside tests.
  Tests: `crates/cairn-store/tests/proof.rs::retired_db_proof_reads_the_facts_it_needs`,
  `crates/cairn-store/tests/proof.rs::abandoned_child_owner_waits_for_its_db`,
  `crates/cairn-store/tests/proof.rs::abandoned_child_proof_checks_refs_barriers_and_version`,
  `crates/cairn-store/tests/proof.rs::proof_for_another_subject_is_refused`,
  `crates/cairn-store/src/proof.rs::tests::dead_log_needs_death_and_seal_before_snapshot`.
- **SM-17. A split/merge source carries a close mark on its own Db from before the clone.** After
  quiesce and `ensure_intent_ours`, **before** clone/union, the coordinator writes the `closed` flag
  into the source Db's manifest **only if** `writer_epoch` is still the epoch of the writer that
  just quiesced (`EngineHandle::sealed_writer_epoch` → `ObjectStores::close_range_db_at`, the fork
  patch `Admin::close_for_writes_at_epoch`: flag + `writer_epoch` bump). A mismatch ⇒ someone
  reopened the source between quiesce and the close command — that writer can still write until the
  next manifest update — so the close fails (`Fenced`) and the operation rolls back (a finding of
  model M1; before, only F3 + "opening a writer always writes WAL" implicitly blocked this window).
  From then on SlateDB refuses **every** writer opening that Db — checked atomically with the CAS
  that bumps the epoch, relying on no RAM table and no table read — so RD1 (a stale table) and RD2
  (reopening a quiescing source) are blocked even if SM-13 and the fresh table fail. Commit ⇒ the
  flag stays forever. Roll-back ⇒ the flag is removed **after** the CAS that drops the intent
  (`split::reopen_sources`, `cairn-ctl intent clear` too): removing it before, the CAS could lose to
  a commit and a retired source would lose its flag. A roll-back dying between the two steps ⇒ a
  live range with a closed Db: the next open that meets the flag removes it (`heal_stale_close`)
  **only if** a successful table read shows the range in the table with no intent. Every removal
  is **conditional on the epoch** (fork patch `Admin::reopen_for_writes_at_epoch` →
  `ObjectStores::reopen_range_db_at`): the roll-back reads the source's close mark
  (`range_db_close_mark`) **before** the CAS that drops the intent, `heal_stale_close` reads it
  **before** its table read; a later intent only closes the source again after that CAS / is on the
  table at that read, so a late removal is fenced instead of removing a later intent's mark. A close
  mark taken after the mark read that still belongs to the intent being rolled back is left for
  `heal_stale_close`. `cairn-ctl intent clear` used to be an exception — an unconditional removal
  (`reopen_range_db`), without reading the mark (a finding of M2); now it reads the mark before the
  CAS and removes by the core's `Reopen` like every roll-back. How to remove is decided by
  `cairn_split::step` (`Event::Cleared`, `Event::StaleClose`).
  Test: `crates/cairn-server/tests/failpoints.rs::ctl_intent_clear_keeps_the_close_of_a_later_intent`.
  The `wal/shared` migration step opens the native-WAL Db **before** that open: a failed step also
  goes through `heal_stale_close`, and then the leadership loop retries.
  Counting wire `node::open::closed_db`. A clone does not inherit the flag; readers, checkpoints,
  the compactor and GC still run on a closed Db. Binaries from before the F8 patch do not know the
  flag: a mixed-version cluster has only the older fences.
  Tests: `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_a_stale_table_from_reopening_the_parent`,
  `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_a_quiesced_split_source_closed`,
  `crates/cairn-server/tests/failpoints.rs::closed_flag_alone_keeps_quiesced_merge_sources_closed`
  (the older fences turned off with the `node::open::bypass_old_fences` knob),
  `crates/cairn-server/tests/failpoints.rs::coordinator_rolled_back_by_another_does_not_recreate_children`
  (the flag is set while stopped after the close, and the roll-back removes it),
  `crates/cairn-server/tests/split.rs::stale_close_on_a_live_range_heals_on_next_open`,
  `crates/cairn-server/tests/failpoints.rs::source_reopened_before_the_close_loses_no_ack`,
  `crates/cairn-server/tests/failpoints.rs::merge_source_reopened_before_the_close_loses_no_ack`
  (the source reopened before the close command; the coordinator rolls back at the close command),
  `crates/cairn-store/tests/range_clone.rs::close_at_epoch_refuses_after_a_reopen`,
  `crates/cairn-server/tests/failpoints.rs::slow_roll_back_keeps_the_close_of_a_later_intent`
  (a slow roll-back, a later intent), `crates/cairn-store/tests/library_contracts.rs::g14_reopen_at_epoch_keeps_a_newer_close`,
  the model `crates/cairn-model/tests/m1.rs` (epoch-conditional close), the fork:
  `vendor/slatedb/slatedb/tests/closed_for_writes.rs`.
- **SM-19. Every split/merge keeps a durable op ledger**: `ops/<first child>`
  (`cairn_store::ledger`; keyed by the first child because child ids are issued in the very intent
  CAS that wins and are never reused — `cluster.conf` does not change). The coordinator **creates**
  the ledger at `Intent` right after the intent CAS wins (before that the ids do not belong to the
  op: two nodes splitting two ranges at the same version pick the same `next_range_id`), then moves
  the phase **before** the side effects of each step: `Closed` (with `sealed` = the epoch of the
  writer that just quiesced) before the close mark, `Cloned` before the first clone/union,
  `Prepared` once every child has a Db and an owner record, before the commit CAS; `Committed` after
  the commit wins. A roll-back (`roll_back_matching`, `cairn-ctl intent clear`) writes `Aborting`
  **before** the CAS that drops the intent and `Cleaned` after that CAS wins, before removing the
  close mark and cleaning up the children. The ledger only changes by `cairn_split::Phase::advance`,
  by CAS on the version just read (`put_resolving`, the `by` line carries a nonce): once the ledger
  is `Aborting` the coordinator cannot write `Prepared` and rolls back; outcomes (`Committed`,
  `Cleaned`) are never overwritten. Read loosely: an op without a ledger (a binary from before
  ledgers) behaves as intent-only; an unknown phase ⇒ a failed read. A roll-back meeting a ledger it
  cannot change (none, already done) goes on — the table is the arbiter; a failed `Aborting` write
  ⇒ the roll-back fails (retried), a failed `Cleaned` write ⇒ only logged. Cost: 5 PUTs (+ a
  read-back when a CAS loses) per winning op, 2 PUTs + 2 GETs per roll-back.
  **Ledgers of settled ops are reaped**: the D-d round (`gc::reap_settled_ops`, after the Db
  deletes, same runner, upgrade gate and clock skew check) LISTs `ops/`, and a ledger older than
  D-d's `min_age` (counted from the ledger's last write) goes through the
  `DeleteProof::settled_op` gate (core: `Event::ReapLedger`) and into the trash (`ledger::delete_op`,
  `Subject::Op`). Settled means: the ledger is at an outcome; no intent mentions the op (a source
  carrying the op's intent, a promised child); the table matches the phase (`Committed` ⇒ the op
  committed or the ranges moved on; `Cleaned` ⇒ the intent is gone); `Committed` ⇒ every source is
  tombstoned; `Cleaned` ⇒ every child has no Db, and its owner record is absent or a tombstone. A
  mismatch or a failed read ⇒ keep. The grounds are permanent (tombstones, D22, outcomes never
  overwritten). A missing ledger afterwards reads as "no ledger" (the behaviour from before ledgers)
  — M2's `LedgerReap` action (no age) proves that no path misreads it, and the liveness
  `M2::stuck` demands that ledgers are reaped. Cost: one LIST per D-d round; a few GETs per ledger
  old enough; four trash requests per reaped ledger, once in the op's life. Metrics
  `cairnflow_gc_ops_reaped_total{outcome}`, `cairnflow_gc_ops_ledgers`.
  Tests: `crates/cairn-store/src/ledger.rs::tests::phases_follow_the_transition_rule`,
  `crates/cairn-store/src/ledger.rs::tests::ambiguous_writes_resolve_by_reading_back`,
  `crates/cairn-server/tests/split.rs::every_op_leaves_a_ledger_ending_committed_or_cleaned`,
  `crates/cairn-server/tests/split.rs::settled_ledgers_are_reaped_by_the_dd_round`,
  `crates/cairn-store/tests/proof.rs::settled_op_proof_waits_for_every_range_to_settle`,
  `crates/cairn-store/tests/trash.rs::op_ledger_deleted_by_mistake_is_restored`.
- **SM-16. Deleting one of cairn's Dbs or owner records can be undone.** D-d
  (`delete_retired_db`), roll-backs (`discard_range_db`), `delete_owner` and op-ledger reaping
  (`ledger::delete_op`) **move** objects into `<prefix>/trash/<date>/<id>/` (copy then delete, in
  G-b order, manifest last) instead of deleting for real; for readers of the original location the
  result is exactly like a delete. The trash round of the D-d runner (`gc::purge_trash_once`,
  enabled independently of the D-d flag) only deletes for real entries older than `--trash-keep-s`
  (7 days) after checking again, against the reloaded table and `external_dbs`, that nobody needs
  them; still needed ⇒ keep and fire the tripwire `trash::purge::still_needed`. Restoring a child Db
  restores its owner record **at the same time**, the owner record first
  (`cairn-ctl trash restore --range`): SM-14 treats "a Db with a manifest but no owner record" as
  evidence of an orphan child. Not covered: checkpoints that `discard_range_db` removes on
  ancestors, and SlateDB's internal GC — those rely on bucket versioning.
  Tests: `crates/cairn-store/tests/trash.rs::retired_db_deleted_by_mistake_is_restored`,
  `crates/cairn-store/tests/trash.rs::discarded_child_db_is_restored`,
  `crates/cairn-store/tests/trash.rs::deleted_owner_record_is_restored`,
  `crates/cairn-store/src/trash.rs::tests::purge_rechecks_before_deleting`,
  `crates/cairn-server/tests/gc.rs::trash_round_purges_only_what_nobody_needs`,
  `crates/cairn-server/tests/ctl.rs::trash_restore_by_range_brings_back_db_and_owner`.
  Runbook: `docs/runbooks/restore-from-trash.md`.
- **SM-20. Auto-split can cut a call tree grouped by prefix, inside that prefix.** With the
  `calls/prefix` gate on (`cluster-version`: `call_prefix=on`, `min_binary ≥ 4`; on by default in
  new clusters), an **unkeyed** callee of `ctx.call`/`ctx.send` keeps the caller's top 32 pk bits
  (`partition_key_for_callee_prefix`), so the whole tree lies in a 2³² window — nearly always
  within one range, but not **one** pk. Auto-split sees the hot range's `owner_pk` samples
  concentrated in that window and cuts at the weighted median **inside** it (`choose_key`);
  `split_blocker` only demands that the split point lies inside the range. No other split/merge
  rule knows about prefixes: ids do not change across a split (the pk is in the id), and the rule
  belongs to the **caller** — the `cairnflow-call-prefix` mark in entry 0, written when it was
  created (v6 rows carry `callee_pk`; a caller without the mark writes v5 rows, drained by the old
  rule forever, IR-9) — independent of the range table, the gate state or the outbox row at rewrite
  or drain time. A `placement: "spread"` call of a caller with the `cairnflow-call-placement` mark
  (gate `calls/placement`) goes out as a v5 row — spread, outside the tree's window; callers without
  the mark ignore `spread`. Auto-split defaults to `suggest`: a hot tree stays in one range until an
  operator splits it or turns on `on`.
  Tests: `crates/cairn-server/tests/auto_split.rs::on_mode_splits_a_hot_call_tree_inside_its_prefix`,
  `crates/cairn-server/tests/interrange.rs::unkeyed_call_tree_stays_on_one_range_with_the_gate_on`,
  `crates/cairn-store/src/partition_table.rs::tests::callee_prefix_tree_stays_in_one_window_and_spreads_inside_it`,
  `crates/cairn-server/tests/failpoints.rs::call_row_written_before_the_gate_keeps_its_callee_after_the_gate`,
  `crates/cairn-engine/tests/call_placement.rs`,
  `crates/cairn-server/tests/failpoints.rs::spread_call_keeps_its_callee_across_failover`.

- **SM-21. The range table is created only on an empty bucket.** When `ClusterConfig::open` finds
  `cluster.conf` missing, it only `Create`s an `equal` table when a delimiter LIST at the root of the
  prefix (`format::has_cluster_data`) sees no `ranges/`, `partition-*` or `owners/`. If it sees
  them, the object was deleted from outside: owner records carry no key span so the table cannot be
  rebuilt, and a new table would reissue ids that collide with live Dbs (breaking SM-1/D22) — the
  node refuses to start with `StoreError::MissingClusterConfig` (runbook
  `docs/runbooks/cluster-conf-lost.md`). `cluster-version`, `format` and `sessions/` are not
  evidence: a node writes them before opening the table. A failed LIST ⇒ `Err` (rule 5). The
  bootstrap race is unchanged: on seeing data, read the table again — if the `Create` winner is
  already there, follow it.
  **A table that vanishes while the cluster runs**: `ClusterConfig::reload` and `read_stored` return
  `Err(MissingClusterConfig)` when the cluster has a table on the bucket and a GET finds it missing
  — no longer "unchanged". Every "reload the table" proof gate (`proof.rs`), D-d, the trash round,
  the orphan round and tenant purge skip their round; opening new ranges waits; ranges already open
  keep serving. Test:
  `crates/cairn-server/tests/gc.rs::rounds_delete_nothing_while_the_range_table_is_missing`.
  Tests: `crates/cairn-server/tests/split.rs::lost_cluster_conf_on_a_bucket_with_ranges_refuses_to_start`,
  `crates/cairn-store/src/cluster.rs::tests::table_is_not_created_over_cluster_data`,
  `crates/cairn-store/src/cluster.rs::tests::boot_objects_are_not_cluster_data`,
  `crates/cairn-store/src/cluster.rs::tests::bootstrap_race_follows_the_winner_that_already_has_ranges`,
  `crates/cairn-store/src/cluster.rs::tests::failed_list_never_creates_the_table`.

- **SM-22. Every winning table leaves a history copy, written after the CAS.** The first `Create`
  in `ClusterConfig::open` and every winning CAS of `update` (including wins found by a read-back
  that resolved an ambiguity) write `cluster-history/<version>` = exactly the bytes of `cluster.conf`
  at that version (`cluster_history::after_won`). **After** the CAS, not before: a copy written
  before the CAS might be a table that never won. The name is the content: `Create` through
  `cas::put_resolving`; different bytes at the same version ⇒ keep the existing copy (`conflict`).
  A failed write does not fail the table change (a log line +
  `cairnflow_cluster_history_failures_total{step}`) — the history is then one version behind, and
  `cairn-ctl table restore` (SM-23) refuses rather than rebuilding an old table. The newest 16 copies
  are kept; older ones go to the trash through `DeleteProof::stale_table_history` (the table **on
  the bucket** is ≥ 16 versions ahead — permanent; table missing ⇒ keep). Never read at run time;
  backups do not copy it (a copy writes its own history).
  Tests: `crates/cairn-store/src/cluster.rs::tests::history_holds_the_bytes_of_every_won_table`,
  `crates/cairn-store/src/cluster.rs::tests::history_keeps_the_newest_16_and_moves_older_ones_to_the_trash`,
  `crates/cairn-store/src/cluster.rs::tests::ambiguous_history_write_is_resolved_by_reread`,
  `crates/cairn-store/src/cluster_history.rs::tests::failed_history_write_never_fails_the_table_change`,
  `crates/cairn-store/src/cluster_history.rs::tests::pruned_history_restores_from_the_trash`.

- **SM-23. `table restore` rebuilds the newest history copy only when it proves that copy was the
  last table** (`ctl/table_restore.rs`). Every table after V leaves marks on the bucket, so the
  command refuses when: there is an `owners/range-<id>`, a `ranges/<id>/` or an `ops/<key>` ledger
  with an id ≥ V's `next_range_id`; a live range of V has no owner record or is tombstoned; a
  tombstoned range is promised by an intent of V; a `committed` ledger whose sources are still in V,
  or a `cleaned` one while V still carries the intent (an `aborting` ledger is written **before** the
  roll-back CAS, so it is not evidence). A failed read of one entry ⇒ refuse (rule 5). `cluster.conf`
  already exists ⇒ refuse; the write is a `Create` (`Lost` ⇒ refuse). `--apply` demands that no
  session is unfinished, unless `--force`. Dry run by default; `--apply` leaves `audit/ctl/<ts>`.
  **A prepared op whose intent is still in V**: a ledger at `Prepared` (or later) means the commit
  CAS may have won after V and the children may have taken writes; writing V back with the intent
  would let a later failed ledger read (the core rolls back) or `intent clear` move the children's
  Dbs into the trash. Every node is stopped, so `table restore` resolves the op on the spot with the
  roll-forward check (`split::forward::prepared`): complete ⇒ write **the committed table**
  (`committed_table`, version V + 1), its history copy and a `Committed` ledger; incomplete ⇒ the
  commit cannot have won, keep the intent. New evidence lines: `ops/<key> is prepared and every
  child … restoring the committed table` / `… keeping the intent`. `cairn-ctl intent clear` without
  `--force` reads the ledger (core, `Found(Ctl)`): `Prepared` with a complete check, or a failed
  read ⇒ refuse (historical flag `Bugs::ctl_clears_prepared`).
  Tests: `crates/cairn-server/tests/split.rs::table_restore_rebuilds_the_last_table_after_random_splits_and_merges`,
  `crates/cairn-server/tests/failpoints.rs::table_restore_refuses_when_history_lags_a_won_cas`,
  `::table_restore_rolls_forward_a_prepared_split_whose_commit_won`,
  `crates/cairn-split/src/tests/finder.rs::ctl_reads_the_ledger_before_clearing`,
  `crates/cairn-model/tests/m2.rs::ctl_refusing_a_prepared_op_keeps_the_core`.

- **SM-24. Children do not stand on their ancestors forever.** SlateDB's own compaction only
  rewrites L0, so a **cold** child keeps its inherited sorted runs — and the `external_dbs` entries
  of every ancestor — forever; D-d cannot delete any ancestor. A background loop on each node
  (`lineage.rs`, `--lineage-compact-*`): a range the node leads that stands on ≥ 2 ancestors, or on
  an ancestor retired > 24 h ago, gets one **full** compaction requested
  (`submit_full_compaction`) and the compactor woken. Once compacted, the loop tries to **detach at
  once** on every tick up to `lineage::DETACH_FOR` (30 minutes, long enough for the compactor's
  10-minute checkpoint to expire): `ObjectStores::detach_now` runs exactly SlateDB GC's `detach`
  task through the public API (`GarbageCollectorBuilder` + `run_gc_once`, every other task off,
  bounded retries) — it only changes **when**, not the condition; before, it had to wait for the
  Db's own next GC tick (1 h from the time the Db was opened; measured at ~55 minutes). Detach
  **does not** run on a range carrying a split/merge intent: the table is read again right before
  every `detach_now` — the list of led ranges read at the start of the tick may be older than the
  source's close mark, and a quiescing source's manifest must stand still (G5,
  `quiesced_source_frozen` in M2).
  Test: `crates/cairn-server/tests/failpoints.rs::lineage_never_detaches_a_quiesced_split_source`.
  Depth: the writer's in-RAM manifest only changes at a manifest poll (1 h), so the loop reads fresh
  `external_dbs` on the bucket (bounded per tick) before deciding to compact and after each detach,
  and `WriterHealth::external_paths` (a gauge, a merge suggestion input) is the **intersection** of
  the RAM copy with the fresh one — correct because a Db's `external_dbs` only shrink.
  `split_blocker` already reads the manifest on the bucket. One compaction at a time per node, the
  deepest range first; it yields while one of the node's ranges is under the L0 throttle; skips
  ranges with an intent; a compacted range is not requested again before `retry_after` (6 h). The
  loop **deletes nothing** and changes no delete condition — D-d (SM-9) is still the only one that
  deletes ancestors. Auto-split refuses a range standing on ≥ 3 ancestors (`split_blocker` ⇒
  `blockedBy: depth`, `split_exclusive` ⇒ `Busy`); a manual split only warns. The decision is
  `LineageConfig::due` + `lineage::pick`. Metrics: `cairnflow_range_external_depth`,
  `cairnflow_lineage_compactions_submitted_total{reason}` (`depth` | `ancestor_age`),
  `cairnflow_lineage_compactions_total`.
  Tests: `crates/cairn-server/tests/split.rs::cold_children_stop_standing_on_ancestors_after_nested_splits`
  (the old-ancestor rule), `::depth_rule_alone_compacts_a_child_two_ancestors_deep` (the depth rule
  alone: `ancestor_age` of one year, the child starting at depth 2, compacted because of `depth`,
  back to 0), `::lineage_detaches_right_after_its_full_compaction` (SlateDB GC and the 1 h manifest
  poll: the bucket and the gauge back to 0 in ~1.5 s),
  `crates/cairn-server/tests/auto_split.rs::auto_split_refuses_a_range_three_ancestors_deep_by_default`,
  `::auto_split_refuses_a_range_past_the_depth_cap`,
  `crates/cairn-server/src/lineage.rs::tests::{lineage_defaults_are_pinned, depth_two_is_due_for_depth_alone}`;
  M2: `M2::deep` (a lineage two deep, `::the_core_holds_on_a_two_deep_lineage`).
- **SM-25. The cost of D-d does not grow with the number of swept tombstones.** Tombstones are
  permanent (the fence `retired_range_is_never_reacquired`), so before the ledger every D-d round
  GET the owner record and LISTed the manifest of every tombstone, including tombstones whose Db
  was deleted long ago (measured at ≈ 2 requests per tombstone per round). The `gc-swept` ledger
  (`cairn_store::gc_swept`, read loosely, missing ≡ empty) lists ids for which **a D-d round
  itself** has just read a tombstoned owner record **and** read the manifest as missing — after
  deleting (reading the manifest again) or because the Db was already deleted; a failed read or an
  ambiguity ⇒ not added. Later rounds skip those ids entirely (no GET, no LIST). The ledger only
  **removes** candidates and never adds a delete: the only error it can cause is keeping (rule 5) —
  a Db that reappears by hand (`cairn-ctl trash restore`, a bucket copy) under an id already in the
  ledger. The bound on that error: every `gc_swept::FULL_PASS_EVERY` (24 h, the mark
  `full-pass-ms=` lives in the ledger itself, so it does not reset when the runner changes) the round
  ignores the ledger, reads every tombstone again and rebuilds the ledger from that very read (ids
  whose read failed are kept). `verify` (h) only skips ledger ids whose `ranges/<id>` has no object
  left — no finding changes. Written with `cas::put_resolving` (`by=` carries a nonce); a lost CAS
  or an error ⇒ dropped, the next round reads again. Capped at `MAX_RUNS` consecutive ranges;
  backups do not copy the ledger. M2 does not change: in the model a deleted Db never reappears, so
  the candidate set is exactly as before. Tests:
  `crates/cairn-server/tests/gc.rs::dd_round_cost_does_not_grow_with_swept_tombstones`
  (the second round GETs no owner record, and the total request count is equal at 10 and 40
  tombstones),
  `::swept_ledger_never_hides_a_db_past_a_full_pass`,
  `crates/cairn-store/src/gc_swept.rs::tests` (encoding, rebuild, cap, ambiguous writes).

- **CG-14. Schedules follow split/merge, without losing or duplicating a mark.** Schedule rows
  (`Schedules`, the hot zone) and their outbox `Fire` rows (keyed `(schedule id, fire_seq)`) live
  under the schedule's pk, so they move with it to a child; the `ScheduleDue` index is in the
  derived zone and is rebuilt when the child Db opens (`Index::REBUILT`). A mark that comes due in
  the middle of a split fires in the child exactly once (deterministic run ids). No split/merge
  rule knows about schedules. Tests:
  `crates/cairn-server/tests/split.rs::schedule_due_during_split`,
  `crates/cairn-engine/tests/schedules_table.rs::derived_rebuild_restores_schedule_index`.

- **Automatic splitting in `on` mode converges.** Three rules keep a low
  `--auto-split-hot-share` from splitting until the nodes saturate (a 24 h soak with 0.005
  grew from 8 to 528 ranges): (1) a split **for load** is blocked (`max_ranges`) once the table
  has `--auto-split-max-ranges` ranges (default 128; splits for size are not capped) — the
  count comes from the table on the bucket, so it holds across restarts; (2) the hot signal is
  the time turns spend in the handler, not their wall time, so engine and durability waits
  that grow with the range count do not make ranges look hotter; (3) the split cooldown and
  the node's split interval are restored from the op ledger (`ops/<key>` `last_modified`)
  after a restart or on a node that did not do the split. There is no automatic merge: a
  cluster at the cap stays there, merges are suggested. Tests:
  `crates/cairn-server/src/autosplit.rs::tests::on_mode_blocks_load_splits_at_max_ranges`,
  `::engine_and_commit_waits_do_not_heat_a_range`,
  `crates/cairn-server/tests/auto_split.rs::on_mode_keeps_the_cooldown_across_a_restart`,
  `::on_mode_keeps_the_node_interval_across_a_restart`.
## 2. Files to read, in order

1. `crates/cairn-store/src/partition_table.rs`: `PartitionKey`, `RangeEntry`, `RangeIntent`,
   `PartitionTable::from_entries` (every table invariant).
2. `crates/cairn-store/src/cluster.rs`: `ClusterConfig::{update, reload, request_reload,
   follow, apply}`, `check_successor`.
3. `crates/cairn-split/`: the pure decision core `step(view, event) -> Plan` — `step.rs` lists
   who asks for each event; `op.rs` — `Phase` and the op ledger's transition rule; `bugs.rs` —
   historical bugs; `src/tests/` — the row-by-row table. M2 (`crates/cairn-model/src/m2.rs`) runs
   this very core.
4. `crates/cairn-server/src/split/`: `mod.rs` — the header doc (9 steps), `RangeOps`; `ops.rs` —
   `RangeOps::{start, split, split_after_intent}`; `merge.rs` — `merge, merge_after_intent`;
   `book.rs` — `OpBook`; `active.rs` — `ActiveIntents`; `steps.rs` — `claim_children`,
   `gather_here`, `ensure_intent_ours`, `roll_back_here`/`settle_in_background`,
   `undo_stray_intent`; `rollback.rs` — `roll_back`/`roll_back_if`, `discard_own_orphans`;
   `reconcile.rs` — `IntentReconciler` (SM-15).
5. `crates/cairn-store/src/config/range_db.rs`: `clone_range`, `union_ranges`,
   `discard_range_db`, `delete_range_objects`, `referenced_ranges`, `adopt_shared_wal`;
   `config/sizing.rs`: `projected_l0_views`.
6. `crates/cairn-server/src/actor/`: `handle.rs` (`EngineHandle::{quiesce, quiesce_held,
   flush_memtable, retire, mark_retiring}`), `leadership.rs::quiesced` (the hold wire — SM-15), the
   D23 check in `dispatch.rs::on_command`.
7. `crates/cairn-server/src/node/spawner.rs`: `RangeSpawner::factory` (waiting for a quiesced
   source — SM-13, fresh table, rolling back orphan intents), `follow_table`, `reconcile`,
   `gc_retired_dbs_once`.
8. `crates/cairn-server/src/ingress/`: `internal.rs::inbound`, `mod.rs::ask_range`,
   `error.rs::ApiError::wrong_range`.
9. `crates/cairn-server/src/transport.rs`: `Transport::send`/`send_batch` (learning the table from
   a `409`, regrouping a batch by the new table — `group_by_destination`), `TableWire`.
10. `crates/cairn-server/src/gc.rs`: `plan`, `run_once`, `is_runner`, `OrphanRetirer::run_once`
   (SM-11), `discard_if_orphan_child` (SM-14).
11. Background: the SM, D-x and G-x entries of
   [`docs/design/decisions.md`](../design/decisions.md), and
   [`docs/architecture.md`](../architecture.md).

## 3. Impact matrix

| Change | Then look at / run |
|---|---|
| `split/`, `partition_table.rs` | The `cluster-4p+split` e2e row + a 10-minute chaos run (`CHAOS_SPLIT_EVERY_S=60`), per AGENTS.md §2. Plus `cargo test -p cairn-server --test split --test quiesce --test wrong_range --test range_table` |
| The `cluster.conf` layout (encode/decode, new keys) | Rule 2: a strictly read object, changing its layout changes the header. `cargo test -p cairn-server --test format_fixture` + `cargo test -p cairn-store --lib -- cluster:: partition_table::` |
| Step order or `clock.lap` points | Point names are `<op>::<phase>` in `failpoint::NAMES`. `cargo test -p cairn-store -p cairn-server --features failpoints --test failpoints` |
| `roll_back`, `ActiveIntents`, the roll-back part and the wait-for-quiesced-source part of `factory` | SM-3, SM-13. `split_crash_at_every_step` (run `cargo test -p cairn-server --test split`) + `cargo test -p cairn-server --features failpoints --test failpoints` (`quiesced_*`) |
| `RangeOps::start`/`OpBook`, the quiesce hold wire (`quiesce_held`, `quiesced`), background roll-backs, `IntentReconciler`, `/internal/ranges/{id}/coordinator` | SM-15. `cargo test -p cairn-server --test split` + `cargo test -p cairn-server --features failpoints --test failpoints` (`client_disconnect_*`, `coordinator_panic_*`, `reconciler_*`, `merge_failing_*`) + `cargo test -p cairn-server --features testkit --test net_partition` (node-to-node paths) |
| Clone/union/discard in `config/range_db.rs` | SM-4, SM-5, SM-10, SM-12. `cargo test -p cairn-store --test range_clone --test shared_wal` + the failpoint row (`split::discard::referenced_checkpoint`) |
| D23 in `ingress/`/`transport.rs`/`serve` | SM-6. `cargo test -p cairn-server --test wrong_range --test split` |
| GC (`gc.rs`, `delete_range_objects`, `referenced_ranges`, `refs()`) | `cargo test -p cairn-store --test range_clone --test shared_wal` + `cargo test -p cairn-server --test split --test gc`, plus the failpoint row (`gc::between_check_and_delete`) |
| `follow_table`/`reconcile` | SM-8. `cargo test -p cairn-server --test range_table --test quiesce` |
| The proxy-tombstone / orphan-child round (`OrphanRetirer`, `discard_if_orphan_child`), `PartitionTable::promised`, `check_successor` | SM-1, SM-11, SM-14. `cargo test -p cairn-server --test gc` + `cargo test -p cairn-server --features failpoints --test failpoints` + `cargo test -p cairn-store --lib -- cluster::` |
| Quiesce/seal (`actor/`, `writer/mod.rs::seal`) | `docs/guides/lease-session.md` LS-7, LS-8 |
| Paths that delete a Db or an owner record, D-d's check/delete order, the orphan-child round, roll-back paths, GC barriers, `fork` | Decisions live in the `crates/cairn-split/` core (`step`, the `src/tests/` table, `Bugs` flags); the environment lives in M2 (mapping table in the header doc of `crates/cairn-model/src/m2.rs`; a new `Bugs` flag for a historical bug, with a row in the `tests/m2.rs::matrix` matrix), then `cargo test -p cairn-split -p cairn-model` |
| Node-to-node paths (`transport.rs`, `/internal/*`, `ClusterSecret::send`) | `cargo test -p cairn-server --features testkit --test net_partition` |
| Deriving the callee pk (`partition_key_for_callee*`, `turn/dispatch.rs::callee_key`), outbox `Call` rows (`cairn-engine/src/outbox.rs`, `Engine::callee_pk_for`), the `calls/prefix` and `calls/placement` gates | SM-20, IR-9. `cargo test -p cairn-engine --test call_prefix --test call_placement` + `cargo test -p cairn-store --lib -- cluster:: partition_table::` + `cargo test -p cairn-server --test interrange --test auto_split --test format_fixture` + the failpoint row (`call_row_written_before_the_gate_*`, `spread_call_keeps_its_callee_*`) + `scripts/mixed-binary.sh` |

## 4. Review checklist

- [ ] Every table change goes through `ClusterConfig::update`; nothing writes `cluster.conf`
  directly.
- [ ] A new step before the commit has a roll-back path. A new step after the commit returns no
  error (only `lap_committed`). A new step that **creates** something for a child (Db, owner record)
  stands behind an `ensure_intent_ours` (SM-3, RD5).
- [ ] A roll-back still does the CAS that drops the intent **first**, and the cleanup **after**.
- [ ] A new path that starts a split/merge from a request goes through `RangeOps::start` (a node
  task), and does not `await` `split`/`merge` directly in the handler's future (SM-15).
- [ ] A new quiesce command from a coordinator uses `quiesce_held` with the wire from its guard;
  rolling back on someone else's behalf uses `roll_back_if` (exactly that intent), not a bare
  `roll_back`.
- [ ] A new quiesce command from a coordinator comes after `mark_quiesced` (SM-13); a new path
  that opens a range's Db goes through `factory` (which waits for the mark) and never opens it
  directly.
- [ ] A new CAS/`Create` goes through `cas::put_resolving`; never treat `Precondition` as lost or
  `Err` as not written (AGENTS.md §1 rule 7).
- [ ] No Db is cloned before `quiesce` returned `Ok`. `Err(SealFailed)` means no clone. `Ok` is not
  enough with the shared WAL: `ensure_wal_drained` still stands between quiesce and clone/union.
- [ ] A coordinator does not `claim` a range whose owner record already belongs to its own session
  (use `claim_from_others`).
- [ ] A new coordinator error path caused by a legitimate race (the range changed owner, the intent
  was rolled back by another node, a wait past its bound) or by a flaky store returns
  `RangeOpError::Busy` (`503`), not `Failed` (`500`); `Failed` is only for abnormal errors (SM-7).
- [ ] A new command aimed at a key has a `target_key()`, so the D23 check in the actor covers it.
- [ ] A new send path between nodes carries `range` and `table` in the `/internal/inbound` query.
- [ ] Every "not sure" in GC comes out as **keep**, with a `warn`, never as delete.
- [ ] A cleanup that fails halfway keeps the evidence (manifest, owner record) for the next round to
  see: never delete a child's owner record while its Db is not gone (SM-12, SM-14). A new path
  that deletes owner records does so only for ranges that never entered the table or whose
  manifest is gone — SM-14 cleans up Dbs with a manifest but no owner record.
- [ ] A new path that deletes the Db of a range outside the table checks **directly** that "no
  range in the table stands on it" with D-d's set (`referenced_ranges`), GC barriers and the table
  version right before deleting — never inferring only from history ("never entered the table")
  (SM-9, SM-14).
- [ ] A cleanup path only deletes checkpoints it created itself, read from its own record (the
  orphan child's manifest), and never infers "nobody uses it" from what it happens to see (SM-12).
- [ ] Whoever writes the owner record of a range it does not hold (proxy tombstone,
  `force-retire`) uses a CAS on the version just read, and never loosens the "owner is dead" rule
  compared with `Session::claim`.
- [ ] A new path that creates a Db/owner record for an id before that id enters the table keeps
  the id promised by an intent for that whole time (SM-11 (2)).
- [ ] New range ids are taken only from the `next_range_id` of the table being CASed.
- [ ] Split/merge futures stay `Send` (`_futures_are_send`): no `Claim`/grant held across an
  `await`.
- [ ] A new half-done state that a background round heals (proxy tombstone, orphan-child cleanup,
  D-d) makes `verify` attach `heals` to its finding (`ctl.rs`): cluster tests only go red on cleanup
  with findings no round heals (`common::Bucket`), and the background scrubber (`scrub.rs`) only
  counts what is still intact after two rounds.
- [ ] A new decision (roll back, roll forward, delete, remove the close mark, open the Db) lives in
  the `cairn_split::step` core, with a row in the `crates/cairn-split/src/tests/` table; the shell
  only executes the `Plan` — intent CASes, child cleanup paths and close-mark removal outside
  `split/` FAIL in `scripts/lint-sources.sh` (check 7, list `scripts/lint-split.allow`); a new
  historical bug is a `Bugs` flag and an M2 matrix row. A new delete path, or a changed check/delete
  order of an existing one, has a matching action in M2's environment, and
  `cargo test -p cairn-split -p cairn-model` is green.
- [ ] Changing the callee placement rule: the rule is a function of the call's content and of the
  **caller's** immutable mark (entry 0, written at creation) — not of the gate state, the outbox row
  or the D-O2 map at rewrite time (the state machine replays `Task::Invoke` on every replay) or at
  drain time; rows without `callee_pk` drain by the old rule forever (SM-20, IR-9).
- [ ] Changing an SM-x invariant means fixing this guide in the same commit.

## 5. Common failure modes

- **A batch only half regrouped after learning the table.** One batch send may carry several
  destination groups; learning the new table at the first group but still sending the later groups
  by the old table snapshot makes them get a `409` with **the same version** just learned — not
  "newer" — and the send ends with `wrong_range` (the rows stay until the next drain pass).
  `Transport::deliver` stops the group loop as soon as it learns a table (`regroup`) and reroutes
  **every** un-acked message. The regrouping must be a stable partition (`group_by_destination`):
  re-sorting by destination in a way that reorders messages within a destination reorders
  `StartCall`/`Cancel` of the same key (IR-3).

- **An old table, nobody left at the destination range, no refusal to learn from.** A node that
  cannot read `cluster.conf` only learns the table from `409 wrong_range`. The destination range
  left the table ⇒ no hint, and the owner record is a released tombstone ⇒ `NoLeader` forever if the
  routing table only knows leaders. In `direct` mode the hole was masked by the old tracker hint (up
  to TTL/3); delegation's `view` reports a released hold at once, so it showed deterministically.
  Fix: probe the owner that just released it (SM-6). Still open: the releasing owner has died too and
  `cluster.conf` still cannot be read.

- **A client disconnects mid split/merge, and the source range answers `WrongRange` forever.**
  Split/merge used to live in the handler's future: hyper dropped it at the first `await` after the
  client closed the socket, the roll-back path never ran, and the source stayed in `quiesced()`
  holding the lease. If the range with the smallest id is stuck, D-d and proxy tombstoning stop
  cluster-wide (`is_runner`). Fixed: SM-15.

- **A recovery storm after a split.** The write pause was only 0.3–0.7 s, but the first call to a
  child took 10 s on `file://` and 70 s on SeaweedFS, because `recover` re-dispatched every
  `suspended` invocation (since fixed). Test:
  `crates/cairn-server/tests/recovery_storm.rs::sleeping_invocations_are_not_redriven_on_leader_change`.
- **A permanent `500 checkpoint missing` on one range.** Rolling back a split deleted a checkpoint a
  live range stood on, because SlateDB copies the `external_dbs` of every ancestor into the child
  (found by a one-hour chaos run). Fixed, see SM-10. The same shape on the **merge** roll-back path
  (a chaos run: a merge of 40+41 rolled back, then a permanent `500`): two sources that had just been
  split and had written nothing were absent from the merged Db's manifest, and SM-10's "keep the
  checkpoints the sources mention" did not see them. Fixed, see SM-12.
- **Deleting a parent Db under its children.** Two shapes have occurred: a mismatched prefix made
  the string comparison fail (G-a); deleting in LIST order left an orphan `wal/` when it failed
  midway (G-b). See the tests under SM-9.
- **Hitting the L0 cap inside the window.** Flushing stalled until one compactor poll tick (~40 s
  measured). Hence the `ensure_l0_headroom` precondition (counting *views*, F4). Test:
  `range_clone.rs::l0_headroom_counts_projected_views`.
- **`500 "Projection and/or union with WAL is not supported"` when merging right after
  `kill -9`** (shared WAL). The old owner of the source range died; the balancer of the
  coordinating node **itself** took the range while the coordinator was asking the old owner to
  release it. The coordinator then `claim`ed the range: the record was already ours ⇒ rewritten
  with a new term ⇒ the keeper's `owner_version` in the old engine went stale ⇒ the CAS dropping the
  `writer_sealed` segment missed (only a `warn`), `wal_segment` stayed, and the union refused the
  source. Same outcome when that CAS failed because of a flaky store. See SM-4.
- **A `500` saying the other range could not be gathered onto this node, when merging** (a
  `--slow-node` chaos run). A node that had just come back took the other range after the rescue
  grace period and rolled back the intent of a coordinator that was still alive (SM-3, by design);
  `gather_here` waited out `settle` and then returned `Failed`. Fixed: `503`, and it stops as soon
  as it sees the intent rolled back. A legitimate race or a flaky store must come out as `Busy`, not
  `Failed` — see SM-7.
- **A `500` saying a child range could not be taken, with the holder `None`, when merging/
  splitting** (a chaos run with the failpoint `session::renew::before_put`). The coordinator's
  session became invalid (a missed renewal) exactly between the `Create` CAS of the child's owner
  record and the check after it ⇒ `Claim::Held(None)`; `claim_children` mapped every `Held` to
  `Failed`. Fixed: `Held(None)` ⇒ `Busy`. The roll-back cleans up the child Db and the owner record
  pointing to the old incarnation — nothing leaks. See SM-7.
- **Commands losing a beat inside the window.** `/admin/tick` used to answer
  `200 {"advanced":0}` while the parent had quiesced and the children had not opened yet. A
  synchronous `/invoke` used to answer `202` early. Tests:
  `split.rs::admin_tick_during_split_is_not_lost`, `split.rs::sync_invoke_waits_through_split`,
  `split.rs::id_routed_admin_calls_survive_split`.
- **A sender holding an old table gets `409` forever.** Senders used to not learn the table from a
  refusal. Now `Transport::send` applies the newer table and resends, at most `MAX_TABLE_LEARNS`
  times.
- **An old binary deletes a newer binary's fields during a split.** `encode` from the struct lost
  unknown lines. Fixed with `extra`.
- **A parent range leaves the table without a tombstone** (a chaos run with `--profile all`,
  `verify` reported `orphan_db`). The leader died between the table commit and `retire`. The parent
  Db leaked forever and the WAL floor was pinned. Fixed: SM-11.
- **An orphan child stays forever after its sources retire** (a `--slow-node` chaos run, `verify`
  reported `orphan_db` for `ranges/28`). The merge coordinator died after the union, the roll-back
  died in the middle of cleaning up the children, then a later merge took the sources out of the
  table: the child no longer stood on a live range and had no owner record to tombstone. Fixed: the
  second evidence of SM-14.
- **The orphan-child round deletes the Db of a parent its children still stand on.** A bucket that
  a single-process node split before the range-level migration framework: the retired parent has no
  owner record, so SM-14's second evidence treated it as an orphan child. The root has no
  `external_dbs`, so `discard_range_db` had no checkpoint to cross-check. Fixed: SM-14 (1)–(3);
  (1)–(3) now live inside `DeleteProof::abandoned_child_db` itself, so every child cleanup path
  (roll-back, `cairn-ctl intent clear`) is blocked too, not only the orphan-child round.
- **`verify` reports `external_db` "source Db lost checkpoint" right after a split, and then it
  goes away by itself** (`gc.rs::single_process_split_and_merge_leave_tombstones_that_gc_reaps` red
  3–9 times in 60). Not a loss: the source had no SST in the child's half (`sst_ids` empty), so the
  child's detach ran at its first GC tick, and `verify` read exactly the window between its two
  steps. Fixed in `verify` (SM-12, `Detaching`). Do not "fix" it by changing the detach order in the
  fork: parent-first is so that a crash midway does not leak a checkpoint on the parent.
- **Two nodes both run D-d for one tick during a leader change.** Still safe, because deletes are
  idempotent and each runner checks again before deleting (`is_runner`). Do not "fix" it with a new
  lease.
- **A hot call tree sits entirely in one range, and nobody cuts it.** With the `calls/prefix` gate,
  an unkeyed `ctx.call` tree gathers in its root's range. Auto-split in `suggest` (the default) only
  suggests; the tree stays there until an operator splits by hand or turns on `on`. Not a split
  bug: SM-20 says a split **can** cut it, not that it cuts by itself. Do not "fix" it with a spread
  rule for trees: the deliberate way to spread is the SDK's `placement: "spread"` option (gate
  `calls/placement`).
