# Guide: the per-node shared WAL (nodewal)

*This is a map, and a hypothesis: it drifts faster than the code. Check in the code before
trusting it. Search by function name; the guide deliberately records no line numbers.*

Each session incarnation writes the WAL of every range it holds into `S` log streams,
`nodewal/<node>/<inc>/<stripe>/<seq>`. Each DB still sees a WAL of its own through the
`slatedb::wal` trait (RFC 0030 + F1). A range's WAL index lives in the owner record: `wal_kind`,
`wal_segments`, `wal_floor_id`. Every rule here has a matching counterexample C1–C7 (registered in
[`docs/design/decisions.md`](../design/decisions.md)). The shared WAL is the **only** WAL of every
range (SlateDB's native WAL was removed; native-WAL ranges of old buckets moved to the shared WAL
in the `wal/shared` migration step) — including nodes without `--cluster`, which are one-node
clusters.

## 1. Invariants

- **NW-1. Ack only after the PUT, and in stream order** (D11). `WalFlushed`, i.e.
  `await_durable`, arrives only after the object's `Create` has finished. With W > 1, object n+1
  becoming durable first still waits for n (`FuturesOrdered`).
  Tests: `crates/cairn-store/tests/shared_wal.rs::shared_wal_ack_only_after_put`,
  `crates/cairn-store/tests/shared_wal.rs::ack_in_order_within_stripe`,
  `crates/cairn-store/tests/shared_wal.rs::ack_in_order_with_two_puts_in_flight`,
  `crates/cairn-model/tests/m3.rs::nw1_ack_before_put_or_out_of_order_loses_an_ack`.
  **When to cut is not an invariant.** The cut rule (`Stripe::run`: deadline `max(head of queue +
  L, previous deadline + window)`, at once on `flush()` or the size limit) only chooses *when* an
  object is built; content, seq, ack after the PUT and in order do not depend on it, and M3 treats
  `Action::Cut` as a free action. Linger L defaults to 1 ms since 2026-10-21
  (`NodeWalConfig::linger`, `--wal-linger-us`). Tests:
  `shared_wal.rs::default_linger_is_1ms_and_halves_puts_of_a_sparse_burst`,
  `linger_coalesces_commits_arriving_close_after_idle`,
  `criterion6_saturated_node_puts_per_window_per_stripe` (L never cuts more densely than the
  window).
  **M3's assumption `ack_needs_live_grant`** (`crates/cairn-model/src/m3.rs`): the `commit` of a
  node whose session is dead on the bucket cannot ack — M3 imports this from M1 (LS-2, LS-5). In
  delegated mode (LS-14/LS-15 of `lease-session.md`) a grant's validity is also extended by the
  coordinator's **ack**, not only by a renewal CAS; the assumption still holds because (a) the
  coordinator only CASes `expired` one TTL after the last heartbeat it acked (τ), and an ack only
  extends the deadline to `t_send + ⅔·TTL` with `t_send` ≤ τ; (b) other observers defer one TTL
  after seeing the role lost **and** after the last version; (c) after `lose` no ack revives the
  grant. M1 checks all three (`delegation_holds_*`, each fence one row breaking LS-3). If the
  assumption breaks (clocks off by more than 1.5×, an ack reviving a grant), sealing the log still
  blocks the zombie's ack (M3 `NO_CLOCK_KEEP_SEAL`, NW-8 "forgetting keeps the seal") — as in
  `direct`.
- **NW-2. An incarnation's log is written only by itself**, with `PutMode::Create`. Meeting a seal
  at its own `seq` means it is sealed: `Fenced`, the stream `die`s, and the whole incarnation
  fail-stops (`NodeWals::get` calls `Session::fail_stop`). A PUT that does not finish (or whose
  outcome is unknown) after the `stall_max` = `put_timeout × put_attempts` budget makes **only that
  stream rotate** (the "poison one shard" pattern of Lance): `Stripe::retire` stops acking and
  closes every range of the stream with `Fenced` **before** asking to rotate, then `rotate_loop`
  claims a new stripe id in the same incarnation (`LogStore::claim`: `Create` floor 0 of the new
  log, then LIST — a directory holding other objects means the id is dropped; new ids are ≥ S and
  never equal an old log), builds a new stream for that slot, and seals the old log itself in the
  background (`rotate_seal_policy`). The old stream's ranges step down, yield dirty, and are
  claimed again — the takeover seals the old log before replaying (NW-5), so late PUTs of the old
  stream are fenced. The node, the session and the other streams live on;
  `cairnflow_nodewal_stripe_rotations_total{cause}` (`stall`, `unknown`; `dirty_yield` — the
  rotation of a dirty yield, NW-6). **A failed id claim does not fail-stop**: the claim's `Create`
  of floor 0 has a limit of 2 × `stall_max` (a `Create` as slow as the PUT that just stalled the
  stream can still finish); on failure the slot **stays rotated** — the dead old stream stays in
  the slot, `NodeWal::slot_ready` is false, `acquire_shared` refuses to open a range mapped to the
  slot before any seal — and `rotate_until_claimed` retries in the background with backoff
  (`retry::ops::ROTATE_RETRY`, at most 10 minutes, then fail-stop `wal_stall`). From the moment the
  stream stalls (`Stripe::stalled`, **before** closing the ranges) until every slot has a stream
  again, the node **drains** (`SlowMeter::set_wal_draining`): the balancer refuses to take ranges
  (`Balancer::with_wal_gate`), the session record publishes `draining=1`, and the ranges just
  yielded go to other nodes. The net (`NodeWalConfig::rotation_net`, asked after each failed claim
  round): no other node can take the ranges — we are the only live node, or every other node is
  slow/draining (`cairn-server/src/slow.rs:peers_can_serve`), or a bare `NodeWal` without a net ⇒
  fail-stop `wal_stall` as before: a new incarnation builds all its initial streams without
  claiming an id (`crates/cairn-store/tests/shared_wal.rs::fail_stop_cause_names_each_wal_path`,
  case 3).
  Tests: `crates/cairn-store/tests/shared_wal.rs::failed_rotation_claim_keeps_the_slot_rotated_and_retries`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::wholly_slow_wal_puts_move_ranges_without_fail_stop`.
  Never ack an object that is not surely durable. Within that budget `stripe::put` **does not
  cancel a slow PUT** (as upstream SlateDB does): an attempt past `put_timeout` keeps running, a
  parallel `Create` runs as well, and whichever finishes first wins — the outcome of every attempt
  is resolved by reading back (below). Fast errors (503) are retried after a jittered backoff
  (through `cairn_store::entropy`, the `tokio::time` clock), instead of burning all attempts within
  a millisecond. `inc` is never reused.
  **`AlreadyExists` is read back on `atomic_put` stores**: object_store retries every 5xx itself,
  `Create` included, and an attempt that `put_timeout` stopped waiting for can still land, so
  `AlreadyExists` on **any** attempt (the first included) may be our own write.
  `Stripe::resolve` GETs the object at `seq` and compares **all its bytes**: exactly our bytes ⇒
  ack (`ours`); a seal or a foreign object ⇒ `Fenced` (`theirs`; the labels
  `wal_fenced`/`wal_ambiguous` now only mean a real seal); absent ⇒ `Create` again (`absent`); GET
  failing until the budget runs out ⇒ no ack, `wal_stall` (`unknown`) —
  `cairnflow_nodewal_put_resolved_total{outcome}`. The comparison is meaningful because only this
  stream writes data objects at `<node>/<inc>/<stripe>/<seq>` and every attempt sends the same
  bytes; sealers only write seals. Acks still go in order (NW-1), so an object "of ours" lying
  above a seal is never acked: the object below it meets the seal first. **The capability is
  declared, not guessed** (`StoreLocation::atomic_put`: `s3://`, `memory://`, `file://` ⇒ yes;
  other schemes ⇒ no; `--wal-atomic-put true|false` overrides). A store with torn objects (MinIO
  EC, multi-AZ quorums) must declare `false`: the stream's read could see the half with data while
  the sealer sees nothing and puts a seal on the other half — a separate model produced that
  counterexample. Then `AlreadyExists` is a fence as before (the deliberate exception to rule 7,
  AGENTS §1), at the price of one node fail-stopping needlessly per ambiguous log PUT. Measured:
  every abandoned incarnation is counted in `cairnflow_failstop_total{cause}`; the profile
  `s3-standard-faults-wal.json` and the `check.sh` row `each-once+nodewal` (on by default) inject
  three ambiguous cases into `nodewal/` — landed but reported an error, landed with the first
  attempt seeing `AlreadyExists`, landed late after `put_timeout` (`testkit::Action::LandLate`).
  The read-back fence in M3: the action `LandAmbiguous`, the flags `resolve_put_by_read`,
  `resolve_checks_owner`.
  Tests: `crates/cairn-store/tests/shared_wal.rs::own_ambiguous_log_put_is_recognized_and_acked`,
  `crates/cairn-store/tests/shared_wal.rs::late_landing_log_put_is_recognized_and_acked`,
  `crates/cairn-store/tests/shared_wal.rs::late_log_put_racing_a_seal_at_the_same_seq_is_fenced`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::ambiguous_wal_put_does_not_fail_stop_the_node`,
  `crates/cairn-store/tests/shared_wal.rs::ambiguous_log_put_fails_stop_without_ack`
  (`atomic_put = false`),
  `crates/cairn-model/tests/m3.rs::reading_back_without_checking_the_owner_lets_a_zombie_ack_into_a_seal`,
  `crates/cairn-store/tests/shared_wal.rs::hung_put_fences_only_its_stripe`,
  `crates/cairn-store/tests/shared_wal.rs::slow_put_is_not_cancelled_at_put_timeout`,
  `crates/cairn-model/tests/m3.rs::rotation_needs_one_of_its_two_fences`. Labels:
  `crates/cairn-store/tests/shared_wal.rs::fail_stop_cause_names_each_wal_path`,
  `crates/cairn-store/tests/shared_wal.rs::node_wals_carry_the_fail_stop_cause_to_the_session`.
  Tests: `crates/cairn-store/tests/shared_wal.rs::zombie_node_cannot_write_after_seal`,
  `crates/cairn-store/tests/shared_wal.rs::hung_put_bounds_stall_then_rotates_its_stripe`,
  `crates/cairn-store/tests/shared_wal.rs::fast_put_errors_back_off_within_stall_budget`,
  `crates/cairn-store/tests/shared_wal.rs::persistent_fast_put_errors_fail_stop_after_stall_budget`,
  `crates/cairn-store/tests/shared_wal.rs::restarted_node_never_reuses_incarnation_after_session_gc`.
  **Only stream errors fail-stop.** `NodeWal::shutdown` (the incarnation has ended, or the last
  `NodeWals` copy was dropped) also makes the stream `die`, but `NodeWals` cancels that WAL's
  watcher task first: dropping `NodeWals` while the incarnation is alive — a graceful shutdown,
  the last engine exiting before `Session::close` — must not abandon the incarnation (`lose`
  would open a new incarnation racing `close()`, leaving a ghost `live` session that peers wait a
  whole TTL for). Test:
  `crates/cairn-store/tests/shared_wal.rs::dropping_the_last_node_wals_does_not_fail_stop_a_live_session`
  (with the reverse direction: `NodeWal::fail_stop` still abandons the incarnation).
- **NW-3. Seal at the first gap above the durable GC floor, then re-read the floor.** Objects
  above the seal are outside the canonical part: nobody replays them, nobody acks them. A seal
  that lands on a name GC just freed does not count (C2). The seal lands **at** the gap of an
  in-flight PUT, not above it (C3′). Every I/O of a seal (LIST floor, GET, `Create` seal) retries
  I/O errors with backoff (`retry_io`): retrying only changes when the read happens, which the
  seal loop already tolerates; one 503 GET no longer fails a whole takeover (the
  `cairn_store::retry` primitive: 8 attempts, 25 ms → 1 s). The outer loop (real races: a zombie
  writing, GC deleting) **is bounded** at 16 rounds: on a contradictory store — `Create` misses but
  GET sees nothing — the seal returns `Io` instead of looping forever while holding the lease.
  Tests: `crates/cairn-store/tests/shared_wal.rs::seal_below_landed_object_discards_it`,
  `crates/cairn-store/tests/shared_wal.rs::seal_gives_up_on_an_inconsistent_store`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_seal_rides_out_transient_get_errors`,
  `crates/cairn-store/tests/shared_wal.rs::seal_on_gc_freed_name_is_detected`,
  `crates/cairn-store/tests/shared_wal.rs::seal_lands_on_inflight_gap_not_above_it`,
  `crates/cairn-model/tests/m3.rs::c2_seal_on_a_name_gc_just_freed_loses_an_ack`,
  `crates/cairn-model/tests/m3.rs::c3_seal_above_an_inflight_put_loses_a_zombie_ack`.
  The M3 model shows these two rules differ in their assumptions: C2 is needed **even when** the
  clocks are right (the ack happens before the session dies, the zombie only runs GC); C3′ is only
  needed when the clocks are broken (a zombie acks into an object landing after the seal).
- **NW-4. Range takeover order** (`acquire_shared`). Seal and read the old logs, then open the DB
  (replay), then CAS our segment into the owner record **before** the first frame (C4), then flush
  the replayed rows to L0 (C6), then drop the old segments (C7). Only then is the DB handed to the
  engine. The C6 flush must **cover up to** the last frame read (`flush_through`): when the
  replayed rows are already in L0 they are filtered out, the memtable is empty and a plain flush is
  a no-op — then a fence row is written and flushed again.
  Dropping the old segments (C7) follows the **stored** mark (`proof::DurableReplayMark`, one
  manifest GET after `flush_through`; `LeaseKeeper::prune_wal_segments` requires this type), not
  the in-RAM `db.manifest()` — SlateDB changes the RAM mark before writing the manifest (the NW-7
  class).
  Tests: `crates/cairn-store/tests/shared_wal.rs::owner_segment_written_before_first_frame`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_flushes_replayed_rows_before_serving`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_covers_replayed_frames_already_in_l0`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_prunes_old_segments_before_serving`,
  `crates/cairn-store/tests/shared_wal.rs::failover_replays_frames_of_dead_node`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::failover_replays_from_dead_node_log`,
  `crates/cairn-model/tests/m3.rs::c4_frame_before_segment_loses_an_ack`,
  `crates/cairn-model/tests/m3.rs::c7_pruning_by_the_in_memory_mark_loses_an_ack`.
- **NW-5. Never replay one's own live log** (C1). A `wal_segment` pointing at this incarnation's
  log while that log already holds frames of the range ⇒ fail-stop. **Except the log of a rotated
  stream** (`Inner::is_retired`): that stream stopped acking before rotating, so its log is no
  longer "live" — `take_over` seals it and then replays it like another node's log (the same
  `sealed` slot as the background self-seal). M3: two fences, `retired_stripe_stops_acking` and
  `seal_rotated_log`, each sufficient alone; turning both off is C1. A missed owner-record CAS in
  `acquire_shared` also fail-stops. An ambiguous CAS re-reads the record instead of treating it as
  a miss. An abandoned takeover detaches the old DB from the stream by generation (`register`).
  **No owner record of a yield still points at a live log.** Two paths that used to bring a node to
  C1 (and to `wal_fenced` when the taker is another node, NW-6) are blocked before reaching
  `take_over`: (A) a dirty yield rotates the stream before writing the owner record (NW-6) — the
  segment left behind points at a rotated log, the "except" branch above; (B) the seal succeeded
  but the clean record CAS and the yield both missed (the yield sits in `pending_release`), then
  the same node reclaims the range before the background loop: `Session::reclaim_wal` lets the
  claim CAS write the pending yield's WAL part, **only when** the record still lists exactly the
  segments the keeper knows (`PendingRelease::held`) — the result is as if the yield came first
  and the claim after, an interleaving that already exists; a foreign segment (an ambiguous segment
  add of its own) ⇒ the bucket's WAL part is kept as before. C1 still stands for a record pointing
  at a live log without going through any yield (a keeper abandoned without writing anything). M3:
  the flags `rotate_before_dirty_yield`, `self_claim_applies_pending_release`, the invariant
  `NO_LIVE_LOG_FENCED` — turning one flag off brings the live-log seal / C1 back, and no flag is
  propping up safety (`ACKED_RECOVERABLE` holds with them off).
  Tests: `crates/cairn-store/tests/shared_wal.rs::range_lost_while_alive_reacquires_from_the_rotated_log`,
  `crates/cairn-store/tests/shared_wal.rs::own_live_log_listed_without_a_release_still_fails_stop`,
  `crates/cairn-store/tests/shared_wal.rs::self_reclaim_after_a_lost_clean_owner_cas_does_not_fail_stop`,
  `crates/cairn-store/src/session/tests.rs::reclaim_applies_the_pending_release_only_over_the_record_it_was_made_for`,
  `crates/cairn-model/tests/m3.rs::self_claim_without_the_pending_release_meets_its_own_live_log`,
  `crates/cairn-store/tests/shared_wal.rs::torn_owner_cas_during_acquire_is_resolved_by_reread`,
  `crates/cairn-store/tests/shared_wal.rs::reacquire_after_abandoned_open_on_same_incarnation`,
  `crates/cairn-store/tests/shared_wal.rs::hung_put_fences_only_its_stripe` (step 5).
- **NW-6. A release clears `wal_segment` only after the DB sealed cleanly** (C4′). The clean flag
  is set at `LeaseKeeper::writer_sealed`, and the owner record is CASed right then. A dirty yield
  keeps the segments; the taker seals and replays. **A dirty yield rotates the stream before
  writing the owner record** (form A; `LeaseKeeper::rotate_live_logs` in `release` and `Drop` →
  `NodeWal::rotate_for_dirty_yield` → `Stripe::rotate_for_dirty_yield`): the segment left behind
  points at a log **of our own** whose stream has stopped acking and closed all its ranges
  (`Stripe::retire`, exactly once — racing a stall rotation, only one request goes out), and then
  `rotate_loop` builds a new stream as for a stall rotation
  (`cairnflow_nodewal_stripe_rotations_total{cause="dirty_yield"}`). The taker seals a log that no
  longer acks: harmless, the node lives. Before this, the taker sealed a **live** log — the next
  PUT of every other range on the stream met the seal and the whole node fail-stopped (≥ 34
  `wal_fenced`/`wal_ambiguous` in chaos). The price: every other range of the stream gets
  `Fenced`, steps down and is reclaimed on the new stream (the same price as a stalled stream),
  instead of the whole node. In-flight PUTs of the rotated stream are not acked, and do not
  fail-stop when they meet the taker's seal (`Stripe::run`, `Stripe::ack`). Writing a dirty
  tombstone (`LeaseKeeper::retire`) does not rotate: nobody takes a range carrying a tombstone, so
  nobody seals the logs the tombstone lists. "Not yet served" segments are dropped. If that CAS
  fails (a flaky store, or a stale keeper because someone rewrote the record) the seal is still
  `Ok` but the segments stay: split/merge checks the record before cloning and answers `503`
  (SM-4), and the next release cleans the segments. That CAS is bounded by `RELEASE_WRITE_BOUND`
  like the release: if it hangs, waiting stops, the clean flag stays and the release cleans the
  segments — a sealed range is not left without a leader for up to 180 s. If the abandoned CAS did
  write after all, the release misses on our own record: it recognises it (same session + term,
  not yet released) and CASes again with the **union** of the record's segments and the
  keeper's — it never drops a segment the record lists — **except** a segment pointing at the
  incarnation's own log that the keeper does not know: a record with the same session + term is
  written only by that keeper, so such a foreign segment can only be its own ambiguous segment
  add, and the takeover failed right after the CAS — the log holds no frame of the range under that
  segment. Keeping it would turn the release dirty, pointing at a **live** log that the keeper has
  not `attach_shared` and so does not rotate: the taker would seal a live log and the whole node
  would fail-stop. Foreign segments of other incarnations are still kept — an ambiguous segment
  add therefore does not wedge a range on our live session. For the same reason,
  `add_wal_segment` meeting our very segment already as the last one (the claim inherits the
  previous attempt's ambiguous segment; `take_over` skipped it because the stream had no frame of
  the range) also marks it "not yet served", so a takeover failing later drops it. A dirty yield's
  rotation request when the rotation loop has stopped (the incarnation is shutting down)
  fail-stops like a stalled stream (`Stripe::rotate_for_dirty_yield`) rather than silently leaving
  the slot without a stream.
  Tests: `crates/cairn-store/tests/shared_wal.rs::cooperative_release_flushes_before_clearing_segments`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::hung_clean_owner_cas_does_not_hold_resigned_range`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::late_reply_of_clean_owner_cas_does_not_strand_resigned_range`,
  `crates/cairn-store/src/session/tests.rs::release_after_unseen_own_write_still_releases`,
  `crates/cairn-store/src/session/tests.rs::release_after_ambiguous_segment_add_still_releases_and_keeps_the_segment`,
  `crates/cairn-store/tests/shared_wal.rs::cooperative_handoff_replays_nothing`,
  `crates/cairn-store/tests/shared_wal.rs::quiesced_shared_range_clones_while_lease_held`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::graceful_shutdown_hands_off_without_replay`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::split_when_clean_owner_cas_fails_after_seal_returns_503`,
  `crates/cairn-model/tests/m3.rs::nw6_unclean_release_loses_an_ack`,
  `crates/cairn-store/tests/shared_wal.rs::dirty_yield_taken_by_another_node_does_not_fail_stop_the_node`,
  `crates/cairn-server/tests/failpoints.rs::failed_seal_on_resign_does_not_fail_stop_the_node`,
  `crates/cairn-model/tests/m3.rs::dirty_yield_without_rotation_seals_a_live_log`,
  `crates/cairn-store/tests/failpoints.rs::unserved_segment::ambiguous_segment_cas_then_release_does_not_list_the_live_log`,
  `crates/cairn-store/tests/failpoints.rs::unserved_segment::reclaim_meeting_its_own_unserved_segment_does_not_list_the_live_log`,
  `crates/cairn-store/src/nodewal/mod.rs::tests::dirty_yield_rotation_without_a_rotation_loop_fails_stop`,
  `crates/cairn-model/tests/m3.rs::abandoned_acquire_keeping_its_own_segment_seals_a_live_log`.
- **NW-7. The GC floor advances only past objects all of whose frames are outside every
  reference.** The floor is written **first**, deletes come after. A durable floor is the round's
  progress even if a delete fails: objects below the floor whose DELETE failed are retried on
  their own in the next round (`delete_below_floor`), instead of walking the whole batch again
  from the old floor. A range's reference is `(replay_after_wal_id, ∞)` of the **current stored**
  manifest (`Inner::refs`, `stored_refs`). For a DB open on the node: the in-memory mark is only
  trusted up to the mark seen in the stored manifest (`durable_replay_after`) — SlateDB advances
  the in-memory mark (`apply_uploaded_state`) **before** writing the manifest, and GC landing in
  that gap followed by the node dying loses acked rows (a long-standing bug, found by a Ready
  run); **checkpoints reference no frame**, because with the shared WAL nobody replays from a
  checkpoint (takeovers replay from the current manifest, clones require an empty WAL, `fork` only
  takes what is in L0, followers write no checkpoints) — and the compactor's 15-minute checkpoint
  often captures a manifest with a gap. Cold ranges are force-flushed by frame age T by
  `age_flush_loop` (D-j), and the forced flush **covers up to** the overdue frame
  (`flush_through`, a fence row when the memtable is empty), so as not to pin the floor forever.
  A range carrying a tombstone — written by the leader, or by the proxy tombstone loop when the
  leader died before writing it (SM-11 of `split-merge.md`) — needs no frame (D22: nobody replays
  it), whether its DB was already deleted by D-d (G-c) or **still exists** because child ranges
  still reference it.
  Tests: `crates/cairn-store/tests/shared_wal.rs::gc_keeps_object_until_all_ranges_flushed`,
  `crates/cairn-store/tests/shared_wal.rs::gc_floor_never_passes_frames_the_stored_manifest_does_not_cover`,
  `crates/cairn-store/tests/shared_wal.rs::forced_flush_covers_frame_already_in_l0`,
  `crates/cairn-store/tests/shared_wal.rs::compactor_checkpoint_does_not_pin_shared_wal_frames`,
  `crates/cairn-store/tests/shared_wal.rs::gc_floor_advances_past_frames_of_deleted_retired_range`,
  `crates/cairn-store/tests/shared_wal.rs::gc_floor_advances_past_frames_of_retired_range_whose_db_remains`,
  `crates/cairn-model/tests/m3.rs::nw7_gc_trusting_the_in_memory_mark_loses_an_ack`,
  `crates/cairn-store/tests/shared_wal.rs::failed_gc_delete_keeps_the_durable_floor`.
  When a reference cannot be read (the owner record, the stored manifest) that round **keeps**
  (rule 5), and reading the stored manifest is bounded (`config::admin_io`): one read hanging or
  failing forever only keeps its own log, it does not stall the whole `gc_loop`. Tests:
  `crates/cairn-store/tests/shared_wal.rs::gc_survives_hung_reference_read`,
  `crates/cairn-store/tests/shared_wal.rs::gc_survives_failing_reference_read`.
- **NW-8. Never seal a live incarnation's log; dead logs are forgotten by a round that is a pure
  function of the bucket.** Sealing a live log kills that node. A **dead** log — of an incarnation
  that has ended or been cleaned up, including the log of a stream that was rotated when its
  incarnation died — **has no floor GC** and no progress in RAM: it stays whole until no owner
  record, tombstones included, lists it any more (usually when the last range listing it is
  claimed again and the taker drops the old segments, C7), and is then sealed (if not yet) and
  forgotten once (`LogStore::forget`). The gate is `DeleteProof::dead_log`: the death is read
  **first**, the owner-record snapshot (`ReferencedLogs`: LIST `owners/` + one GET per range) is
  taken **after**. While a log is still listed, the sweep only waits — no seal, no read.
  **The log of a stream rotated while its incarnation is alive** goes the same way, cleaned up by
  that incarnation itself: the ground is "the stream has stopped acking"
  (`DeleteProof::rotated_log`, with `proof::StreamRetired` read before the snapshot like the death
  — M3 `retire_before_snapshot`: redundant on its own like `death_before_snapshot`, but turned off
  together with `snapshot_after_seal` it breaks even under the clock assumption) in place of the
  death; the full sweep only takes our own logs once their stream has rotated
  (`Inner::is_retired`). Before this, such a log stayed for the node's whole life — in a 4 h soak,
  728 + 709 objects sat for 1 677 s
  (`crates/cairn-store/tests/shared_wal.rs::rotated_log_is_forgotten_while_its_incarnation_lives`,
  `crates/cairn-model/tests/m3.rs::forgetting_a_rotated_log_by_a_snapshot_older_than_the_rotation_loses_an_ack`).
  Two loops share one core (`sweep.rs::forget_unlisted`): the **full sweep** (`sweep_dead_logs`;
  candidates from **one recursive LIST** of `nodewal/`, first after `FIRST_SWEEP_AFTER` (1 s) so
  that incarnations shorter-lived than the period still sweep once, then every `SWEEP_EVERY_GC` GC
  ticks; while the previous round still had work or failed, again after `SWEEP_AGAIN_FIRST` (1 s),
  doubling up to one tick; at most `SWEEP_BATCH` logs per round, starting at a random position,
  purged `FORGET_LOGS_IN_FLIGHT` at a time) and the **hint loop** (`hint_loop`: logs a takeover on the node
  just dropped from an owner record, gathered over `HINT_GATHER`, one directory LIST per log). No
  walking directories with a delimiter LIST: a store that keeps empty directories (SeaweedFS)
  lists vanished logs forever, and sealing a log that has no objects re-creates it (seen in a
  soak). Dying midway, the next round redoes it correctly: the `forgotten` mark (carrying the seal
  position) is written before anything moves, and a log carrying the mark that still has objects
  outside the mark and a seal is an unfinished forget — finish it, do not seal again
  (`LogStore::forgotten_seals`). The reads a round rests on are retried one by one (the owner
  snapshot's LIST and GETs, op `owner.snapshot_read`; session records, op
  `nodewal.sweep_session_read`): with ~450 owner records read twice per round, one injected GET
  fault per snapshot failed most rounds of a 24 h soak and dead logs stood unpurged for up to 73
  minutes (`crates/cairn-store/tests/shared_wal.rs::dead_log_sweep_survives_one_failed_owner_read`,
  `::dead_log_sweep_retries_soon_after_a_failed_round`). The in-RAM seal copy (`Inner::sealed`) is only a buffer for the
  taker and is dropped once the log is no longer listed. The incarnation's own **live** logs still
  have floor GC (NW-7). The price: a dead log keeps all its objects until it is forgotten, and a
  log still listed by a tombstone (the tombstone of a keeper retired dirty) keeps them forever. A
  failed seal (out of `retry_io`) leaves no "known" mark. A dirty yield of a live node used to be
  a path to sealing a live log; it now rotates the stream before writing the owner record (NW-6),
  so only the price of one rotation remains (every range of the stream steps down and is
  reclaimed). The seal limit at a yield with the shared WAL is still `stall_max` +
  `SEAL_FLUSH_BUDGET` (`actor::seal_timeout`, including when stepping down because the store
  fails): a seal only exceeds the limit when the log stream has rotated itself (or fail-stopped),
  or the memtable flush is slower than its budget. An engine open failing **after**
  `acquire_shared` (the range-level migration step, `set_data`, `Engine::open_in_range`) also
  seals the DB and calls `writer_sealed` before dropping the keeper (`node::seal_failed_open`,
  same limit) — dropping it directly would be a dirty yield: before rotation on dirty yields, one
  deterministic error made the nodes kill each other in turn; now each time is one rotation.
  Tests: `crates/cairn-store/tests/shared_wal.rs::orphan_sweep_leaves_live_logs_alone`,
  `crates/cairn-server/tests/failpoints.rs::failed_open_after_acquire_shared_does_not_kill_the_node`,
  `crates/cairn-server/tests/shared_wal_cluster.rs::slow_wal_put_within_budget_does_not_turn_resign_into_fail_stop`,
  `crates/cairn-server/src/actor/leadership.rs::seal_timeout_tests::seal_outlasts_shared_wal_stall_budget_plus_flush`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_seal_and_floor_are_removed_when_unreferenced`,
  `crates/cairn-store/tests/shared_wal.rs::orphan_log_of_cleanly_closed_node_is_collected`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_sweep_progresses_across_short_lived_incarnations`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_sweep_is_not_starved_by_logs_already_swept`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_sweep_runs_within_a_life_shorter_than_gc_interval`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_sweep_retries_a_failed_seal_in_the_same_incarnation`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_backlog_is_forgotten_across_lives_shorter_than_a_sweep`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_sealed_log_is_collected_without_waiting_for_gc_interval`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_sealed_log_is_collected_before_a_long_dead_log_sweep`,
  `crates/cairn-store/tests/shared_wal.rs::forget_cut_short_is_finished_by_a_later_round`,
  `crates/cairn-store/tests/shared_wal.rs::dead_log_collected_while_listed_is_forgotten_once_unlisted`,
  `crates/cairn-store/tests/shared_wal.rs::forgotten_dead_log_is_not_resealed_on_a_store_that_keeps_empty_dirs`.
  **Forgetting keeps the seal** (M3's `forget_keeps_seal`):
  `LogStore::forget` moves the floor, the index and leftover data into the trash but **keeps the
  seal** and writes the `forgotten` mark (before moving anything). Moving the seal away as well
  would let a zombie whose stream has not yet touched the seal PUT into exactly the old seal's
  place and ack if its grant is still valid after the TTL — the broken-clock assumption (M3,
  `crates/cairn-model/tests/m3.rs::without_the_clock_forgetting_a_dead_log_lets_its_zombie_ack`);
  an owner-record snapshot taken after the death does not cover a segment the zombie CASes in
  afterwards either. The seal is a fence on the bucket and needs no clock. For the same reason, the
  snapshot used as proof is taken **after** the seal is on the bucket (`proof::SealSeen`): every
  frame a zombie could ack lies below the seal, and its segment is CASed before its first frame,
  i.e. before the seal. Before dead logs lost their floor GC, the condition "the GC floor reached
  the seal" covered this implicitly; dropping floor GC without snapshotting after the seal lets a
  zombie CAS a segment after the snapshot and then ack, and the forget moves that object away (M3
  `snapshot_after_seal`,
  `crates/cairn-model/tests/m3.rs::without_the_clock_a_snapshot_before_the_seal_lets_forget_lose_a_zombie_ack`).
  The forget loop does not reseal a log carrying the mark (it only finishes an unfinished forget),
  and `verify` (g) does not count it. The price: two objects per dead log, forever.
  Test: `crates/cairn-store/tests/shared_wal.rs::zombie_cannot_ack_after_its_sealed_log_is_forgotten`.
- **NW-9. Every range opens on the shared WAL; the WAL kind comes from the owner record** (D-f).
  New ranges are always `shared`. An unknown kind is refused, and the record is left as is. A
  `slatedb` record (old buckets) is opened only **inside** the `wal/shared` migration step
  (`Run::RangeDb`, before the DB opens to serve): open the DB with the native WAL, write a fence,
  flush the WAL and then the memtable, close; change the `wal_name` mark — SlateDB refuses while
  the native WAL still has frames the manifest does not cover (`WalNotEmpty`, G12 in
  `library_contracts.rs`), so "flushed" is a fence of the library, not our own claim; then CAS
  `wal_kind=shared` into the owner record. Outside that step the node does not open `slatedb`
  ranges (`StoreError::NativeWal`). A mismatching `wal_name` mark in the manifest refuses the open.
  The child of a native-WAL parent changes its mark when the DB's WAL is empty
  (`adopt_shared_wal`). A DB that **already exists** without an owner record (a bucket built by a
  single-process node before every node became a cluster): before the claim,
  `ObjectStores::adopt_unowned_db` writes a released record carrying the manifest's kind, so the
  migration step sees it.
  Tests: `crates/cairn-store/src/session/tests.rs::unknown_wal_kind_refuses_open`,
  `crates/cairn-store/tests/shared_wal.rs::wal_kind_mismatch_refuses_open`,
  `crates/cairn-store/tests/shared_wal.rs::child_of_native_parent_adopts_shared_wal`,
  `crates/cairn-store/tests/range_clone.rs::adopt_unowned_db_records_the_db_wal_kind`,
  `crates/cairn-store/tests/migrate.rs::native_wal_range_moves_to_shared_wal`.
- **NW-10. Followers expose contiguous objects from the floor on atomic stores; only acked objects
  on stores that can tear** (C5). The tailer reads sequentially from the floor and stops at the
  first place that is not data. An `atomic_put` store: a readable object is exposed at once
  (`wm = max(acked_upto, seq)`) — an object that was seen is one whole write, the sealer (strong
  read-after-write, S3-1) sees it too so a seal `Create` there misses, and every object below the
  seal is replayed by the taker. An `atomic_put = false` store: `wm =` the largest `acked_upto`
  seen (C5 — a torn object can read as present at the follower and empty at the sealer).
  The model: M3 `expose_contiguous` (`TailRead`, the invariant `EXPOSED_RECOVERABLE`; exposure does
  not go through grant validity, so `seal_at_first_gap`, `snapshot_after_seal`,
  `forget_keeps_seal`, `seal_rotated_log` are required even when the clocks are right —
  `crates/cairn-model/tests/m3.rs::follower_matrix`), and a separate model with
  `--expose-contiguous` (clean; `--torn` brings C5 back). The overall "lose 0, wrong 0" under death
  at every step is held by the deterministic simulation.
  Tests: `crates/cairn-store/tests/shared_wal.rs::follower_exposes_contiguous_objects_on_an_atomic_store`,
  `crates/cairn-store/tests/shared_wal.rs::follower_exposes_only_acked_objects` (torn store),
  `crates/cairn-server/tests/follower_read.rs::follower_sees_an_idle_leaders_tail_without_redirecting`,
  `crates/cairn-store/tests/shared_wal_sim.rs::shared_wal_survives_crash_at_every_step`.
  The net for tearing stores (rule I4): the **last** object of a quiet log is covered by no header,
  so a reader only sees it when the writer writes the next object, or when the range force-flushes
  by frame age (T = 60 s, checked every T/4) and the reader loads the manifest (≥ 10 s) — measured
  at 60–80 s. So the tailer remembers when it read each **unexposed** object
  (`SharedWalReader::unexposed_since`, always `None` on an atomic store), and a follower **does not
  serve an unfinished state** (or a journal) of a range whose unexposed tail has been there longer
  than `max(4 poll ticks, 1 s)`: it returns `None` ⇒ `503` with the leader, as for "not seen".
  Outcomes (`completed`/`failed`) are still served:
  `crates/cairn-server/tests/follower_read.rs::follower_does_not_serve_a_stale_status_when_the_leader_is_idle`
  (store `atomic_put = false`).
  The tailer stops when it meets a seal, when the log vanishes (no floor, and the last object read
  is gone too — a dead log was forgotten), or when no reader follows the log any more; a stopped
  tailer nobody holds is pruned:
  `crates/cairn-store/tests/shared_wal.rs::tailer_of_a_log_nobody_follows_is_retired`,
  `crates/cairn-store/tests/shared_wal.rs::tailer_stops_when_its_log_vanishes`.
  A tailer that has caught up backs off its GETs (doubling up to `BACKOFF_POLLS` poll ticks), back
  to the poll tick when it reads an object or when someone reads from the follower
  (`NodeTailers::nudge`); the floor LIST is by time (at most once every `FLOOR_EVERY_POLLS` poll
  ticks). When the node becomes the range's leader, the range's follower reader closes — nobody
  follows their own log:
  `crates/cairn-store/tests/shared_wal.rs::caught_up_tailer_backs_off_and_still_sees_new_writes`,
  `crates/cairn-server/tests/read_cost.rs::follower_reader_of_a_range_this_node_now_leads_stops_tailing`.
  **A reader only takes frames contiguous in wal id**: the range's next frame having an id larger
  than the id the reader needs (a gap), or GC having deleted frames ≥ that id before the tailer
  emitted them (`dropped`) ⇒ `WalTruncated` at that id, and the reader loads the manifest; the
  bounded iterator (the manifest-load path) returns the frames before the gap and then
  `WalTruncated` (SlateDB keeps the replayed rows). A slow tailer across one GC tick jumps the floor
  over objects it **never read** — they do not go into `dropped` — and before the fix the reader
  applied frames above the gap: the follower served a state that was not a prefix (seeing a later
  write while missing an earlier one) until the next manifest load. The same rule blocks the case
  of an owner record dropping the segment of an old log the reader has not finished reading
  (`maybe_refresh` replacing the log list). A real gap (`register` taking a first id higher than
  the last id) also stops the reader until the stored manifest covers it — a takeover flushes L0
  before serving, so it costs one manifest load, at worst one manifest poll tick: slow, not wrong:
  `crates/cairn-store/src/nodewal/tailer.rs::tests::reader_does_not_apply_frames_past_objects_the_tailer_skipped`.

- **NW-11. The log index states only facts about acked objects, and only saves GETs**
  (`<log>/index`). The writer overwrites it at every GC tick with new objects and every
  `INDEX_EVERY_OBJECTS` (64) new acked objects (`gc::write_index`, `gc::index_loop`), from
  `Stripe::index` — only acked objects, and acks go in order, so every seq in `[floor, upto]` is
  data and the seal (if any) lies above `upto`. The sealer probes for the gap from
  `max(floor, upto + 1)` instead of from the floor; entries below the floor are dropped (GC proved
  all their frames are outside every reference). A range's taker only GETs objects that the index
  says hold frames of the range with a wal id above the `replay_after_wal_id` of the manifest read
  before the takeover (`acquire_shared_with_hint`; the hint comes from the manifest read of
  `adopt_shared_wal`): the mark only grows, SlateDB does not replay frames below it, and our first
  id is already above it (`SealedLog::frames_of`); an object in the index that is absent can only
  be skipped when the floor has passed it, otherwise it is an error. The sealer rewrites the index
  up to the seal once it has read the tail, for whoever comes next. **Read leniently**: absent,
  corrupt, GET error ⇒ probe from the floor as before. A new object, `DATA_FORMAT` unchanged; floor
  GC of a sealed log runs on the index, with no objects needed in memory.
  Tests: `crates/cairn-store/tests/shared_wal.rs::takeover_reads_only_objects_of_its_range`,
  `crates/cairn-store/tests/shared_wal.rs::takeover_skips_objects_already_in_l0`,
  `crates/cairn-store/tests/shared_wal.rs::young_log_is_indexed_without_waiting_for_gc_interval`,
  `crates/cairn-store/tests/shared_wal.rs::later_taker_reads_index_left_by_the_sealer`,
  `crates/cairn-store/tests/shared_wal.rs::garbled_log_index_falls_back_to_reading_from_the_floor`,
  `crates/cairn-store/tests/shared_wal_sim.rs::shared_wal_survives_crash_at_every_step`.
- **NW-12. Forgetting a dead log can be undone.** `LogStore::forget` moves every object of the
  dead log — data (dead logs have no floor GC), floor, index; the seal stays (NW-8) — into
  `<prefix>/trash/<date>/<id>/`, one entry per `FORGET_CHUNK` objects, the entries moved in
  parallel; the trash purge loop only really deletes when no owner record lists the log any more.
  **Not covered**: frames that `gc_log` deletes below the floor of a **live** log — their volume
  equals all the WAL writes, and that part relies on bucket versioning.
  Test: `crates/cairn-store/tests/shared_wal.rs::dead_log_forgotten_by_mistake_is_restored`.

## 2. Files to read, in order

1. The NW and C entries of [`docs/design/decisions.md`](../design/decisions.md) (the
   counterexamples). Read them before the code: every odd branch in the code is a counterexample.
2. `crates/cairn-store/src/nodewal/format.rs`: the object layout (data, seal, `acked_upto`) and
   the log index (`LogIndex`, NW-11).
3. `crates/cairn-store/src/nodewal/log.rs`: the four operations on a log (`create`, `get`,
   floor).
4. `crates/cairn-store/src/nodewal/stripe.rs`: `Stripe::{run, cut, put, ack, die, register}`
   and `RangeWal`.
5. `crates/cairn-store/src/nodewal/mod.rs`: `NodeWal::{start, take_over, seal}`, `rotate_loop`
   (stream rotation); then `acquire.rs::acquire_shared`, `gc.rs` (`Inner::{refs, gc_log}`,
   `gc_loop` — live logs only), `sweep.rs` (`forget_unlisted`, `sweep_loop`, `hint_loop` —
   forgetting dead logs), `flush.rs::age_flush_loop`.
6. `crates/cairn-store/src/nodewal/wals.rs`: `NodeWals::{follow, get}` (a fail-stop leads to
   abandoning the incarnation).
7. `crates/cairn-store/src/session/keeper.rs`: the WAL part of `LeaseKeeper` (`add_wal_segment`,
   `prune_wal_segments`, `writer_sealed`, `pending`).
8. `crates/cairn-server/src/node/`: `NodeWals::follow`, the open path in
   `spawner.rs::RangeSpawner::factory` (the range-DB migration step, the `wal_kind` check,
   `adopt_shared_wal`, `acquire_shared_with_hint`), applying `range-windows` to the streams. The
   `wal/shared` step: `crates/cairn-store/src/migrate.rs::wal_shared`,
   `crates/cairn-store/src/config/range_db.rs::native_wal_to_shared`.
9. `crates/cairn-store/src/nodewal/tailer.rs`: followers, when touching follower reads.

## 3. Impact matrix

| Change | Then look at/run |
|---|---|
| Anything in `nodewal/` | `cargo test -p cairn-store --test shared_wal --test shared_wal_sim` + cluster e2e `--wal-kind shared` (AGENTS.md §2) |
| `format.rs`, log object names/paths | Rule 2: do not bump `DATA_FORMAT`. `cargo test -p cairn-server --test format_fixture` (the cluster fixture has `nodewal/`) |
| `stripe.rs` (cut, PUT, ack) | NW-1, NW-2. Add `cargo test -p cairn-store --test shared_wal -- ack_in_order criterion6 noisy_range` and the failpoint row (`nodewal::stripe::*`) |
| `take_over`, `seal`, `acquire_shared` | NW-3 → NW-5. `shared_wal_sim` with more seeds: `CAIRN_SIM_SEEDS=64 cargo test -p cairn-store --test shared_wal_sim` |
| GC (`refs()`, `gc_log`), forgetting (`sweep.rs`, `LogStore::forget`) | NW-7, NW-8, NW-12, rule 5 of AGENTS.md. `cargo test -p cairn-store --test range_clone --test shared_wal` + `cargo test -p cairn-server --test split` |
| The WAL part of `LeaseKeeper` | NW-4, NW-6. `docs/guides/lease-session.md`. `cargo test --workspace` |
| The order or fences of sealing, takeover, floor GC, sweeping/forgetting dead logs | Change the M3 model (`crates/cairn-model/src/m3.rs`, mapping table in the header doc) and its row in `tests/m3.rs::matrix`; `cargo test -p cairn-model --test m3` |
| `NodeWalConfig` defaults (W, S, window, L, T) | A user decision. Measure again `criterion6_saturated_node_puts_per_window_per_stripe`, `commit_latency_on_memory_not_worse_than_native`, and GETs/PUTs per step with `harness/chaos/chaos.ts --cost` (several runs — one run varies ±25%). T = 30 s was measured: takeover replay drops, but SST/manifest reads rise to compensate and L0 flush PUTs nearly double — keep 60 s |
| A turn/commit path touching durability | `shared_wal_survives_crash_at_every_step`, `split_crash_at_every_step`, `tests/durability.rs` (AGENTS.md §2) |

Chaos with the shared WAL runs on SeaweedFS, not on MinIO (AGENTS.md §5).

## 4. Review checklist

- [ ] No path sends `WalFlushed` or releases a `flush` waiter before the PUT has finished.
- [ ] The listener receives `WalFlushed` **before** the `flush` waiter is released.
- [ ] Every PUT error with an unknown outcome leads to `die` + fail-stop or `retire` + stream
  rotation (stop acking **before** asking to rotate), never "retry then ack" — except exactly one
  path: `AlreadyExists` whose read-back finds **exactly our bytes** on an `atomic_put` store
  (NW-2). No path infers "ours" from a header, from the attempt, or on an `atomic_put = false`
  store. `put`'s retry loop counts against the `stall_max` time budget, with backoff between
  attempts — not by number of attempts.
- [ ] The owner record gets the segment CAS **before** the DB is handed to the engine. No frame of
  ours exists without a segment pointing at our log.
- [ ] GC: write the floor before deleting; any error reading a reference skips the round, deletes
  nothing.
- [ ] No log is sealed while its session is not surely dead (`read_session` returns terminal or
  absent) — except the logs of the incarnation's own rotated streams (`Inner::is_retired`).
- [ ] A new path writing a "released" owner record that still lists segments (a dirty yield)
  rotates every live log of ours in it **before** the write (`LeaseKeeper::rotate_live_logs`) —
  otherwise the taker seals a live log and the whole node fail-stops. A keeper without `shared`
  yet (a failed takeover) cannot rotate: every segment of our own it leaves behind must be a served
  segment — unserved segments, inherited or ambiguous ones included, are dropped.
- [ ] The dead-log sweep reads no progress from RAM: every decision comes from the LIST, the
  session records and the owner-record snapshot of that very round; no floor GC on dead or rotated
  logs.
- [ ] No new global lock in `stripe.rs`: the lock is the `Mutex<State>` of **one** stream.
- [ ] The path delivering frames to a follower reader (`SharedWalReader::contiguous`) never
  delivers a frame with an id larger than the id the reader needs: every gap is a `WalTruncated`
  (NW-10).
- [ ] The log index (NW-11) is only built from acked or read objects; every path using it has an
  "absent/corrupt ⇒ as before" branch, and nothing deletes or skips an object only because the
  index says so while the floor has not passed it.
- [ ] Changing an NW-x invariant updates this guide in the same commit.

## 5. Common failure modes

Four real bugs found by the deterministic simulation, each with a regression test:

- **Reusing an incarnation after the session was cleaned up** (seed 12). The new log writes below
  the old log's GC floor, the sealer misses it, acked rows are lost. See NW-2.
- **A torn owner-record CAS while adding a segment** (seed 2). The record points at our log while
  we drop the range, and the range is stuck. See NW-5.
- **A takeover failing midway** (seed 48). A range "still open" on the stream makes every later
  attach be refused. See NW-5.
- **One range failing at yield takes the whole node down** (≥ 59 fail-stops in chaos/soak, each
  ≤ 30 s after a fault injected during the seal or the owner-record CAS). Form A: the seal fails
  or exceeds its limit ⇒ a dirty yield ⇒ the taker seals a live log (`wal_fenced`) or the same
  node takes it back (`c1`). Form B (18/27 of the `c1` cases): the seal succeeds but the clean
  record CAS and the yield both miss ⇒ the same node reclaims before the background loop ⇒ `c1`.
  Fixed: rotate the stream before a dirty yield; a self-reclaim applies the pending yield. See
  NW-5, NW-6.
- **L0 full during a takeover** (seed 33). Flushing the replayed rows (C6) stalls on L0
  backpressure until the compactor's poll tick. This is a liveness issue, not safety. Not fixed in
  cairn: that is the job of F2 in the fork.
- **Orphan logs living forever.** One hour of chaos left 38 248 objects from nodes that released
  cleanly and shut down. Fixed with `sweep_dead_logs`. Test:
  `orphan_log_of_cleanly_closed_node_is_collected`.
- **Forgetting dead logs does not finish within one incarnation's life** (chaos with
  `--s3-profile s3-standard`, a kill every 10 s). The sweep sealed **every** dead log before GC
  (~11 s per 512-object log, read sequentially); the node died before the GC phase, and the next
  incarnation started over. Fixed: GC each log right after sealing it, in batches; the seal reads
  ahead up to 16 GETs in parallel, GC deletes 16 objects in parallel (the floor is durable, so the
  delete order does not matter). The initial batch also counted logs with only a seal left that an
  owner record still listed (impossible to forget), so the same 16 first logs by name ate the batch
  of every incarnation — now only logs with data objects count, starting at a random position.
  Since dead logs lost their floor GC, the batch only counts logs being forgotten, and logs still
  listed cost nothing. See NW-8.
- **Incarnations shorter-lived than `gc_interval` never sweep** (a smoke run with `--profile all`,
  a kill every 10 s: 12/88 processes ever swept, dead logs of 500–2 700 objects sat at floor 0 for
  over 600 s without being pinned). `gc_loop` slept a full `gc_interval` (30 s) before its first
  round. Fixed: the first sweep after 1 s. See NW-8.
- **Seal-only logs pile up, and a sweep lasts longer than a process lives** (a soak: 255 seal-only
  logs nobody listed, the floor of one log with data stuck for 639 s). Forgetting only ran once, at
  the end of `sweep_dead_logs`; under a kill every 10 s no life got there, so every new life
  resealed hundreds of finished logs. At the same time, logs sealed at a takeover were only GCed in
  the early round (after the sweep) and at the `gc_interval` tick: both takers died before GCing
  them. Fixed: forget each log as soon as its GC finishes; GC sealed logs before the sweep and
  every second while they are young. Since dead logs lost their floor GC there is no GC of sealed
  logs (`young_sealed_gc_loop` was removed): a takeover hints the forget loop (`hint_loop`) as
  soon as it drops the old segments. See NW-8.
- **Forgotten logs coming back in every life** (a 4-hour soak, SeaweedFS). The sweep walked
  directories with a delimiter LIST; SeaweedFS still lists empty directories, so every node life
  resealed (writing a seal at 0) and then forgot every log that ever died: 1 422 logs were
  forgotten 5 928 times. The number grew until the sweep no longer reached logs with data within
  a node's ~48 s life (27 logs with the floor stuck for over 600 s after more than two hours).
  Fixed: candidates from a recursive LIST. See NW-8.
- **One failed seal abandons the log for the incarnation's whole life.** `sealed` left an empty
  slot in the map when the seal failed, and the sweep treated every log with a slot as "known".
  Fixed: remove the slot on failure; only an initialised slot is "known". Now the forget loop asks
  RAM nothing at all. See NW-8.
- **A dead log GCed while still listed stays until the node dies.** GC of sealed logs recorded the
  log in an in-RAM `swept` set when the floor reached the seal; if another range still listed the
  log at that moment it was not forgotten, and that incarnation's sweep skipped every log in the
  set. Same class: a forget failing after writing the `forgotten` mark left the floor and the index
  forever (the sweep skipped every marked log). Fixed: drop floor GC of dead logs together with
  every in-RAM set; each round is a pure function of the bucket, and an unfinished forget is
  finished. See NW-8.
- **Tailers living forever.** A tailer only stopped at a seal; a log forgotten while the tailer was
  still behind made GET see `None` forever, and `NodeTailers` never pruned tailers even after
  their reader was evicted — GETs and CPU grew with the node's life. Fixed: see NW-10. Backing off
  once caught up, a floor LIST by time and no longer following one's own log: see NW-10.
- **Every taker re-reads the whole dead log** (a `chaos.ts --cost` run: ~46% of all GETs). The
  taker GOT every object from the floor to the gap even though its range had frames in only part
  of them, and two surviving nodes taking over a dead node's ranges both re-read it. Fixed: the
  log index (NW-11).
- **The frame-age forced flush chases its own fence** (found in a Ready run). A write meeting an
  overdue frame froze the memtable at once with the mark before its frame (a gap);
  `flush_through` wrote a fence, but writing the fence also froze at once because the frame in the
  gap was overdue ⇒ the fence went into the gap, the next tick force-flushed it with a new fence,
  forever: one log PUT + one L0 SST + one manifest per tick for an idle range, with the log floor
  stuck at the newest fence. Fixed: the fence is written under `Stripe::hold_age_freeze` (no
  age-based freezing until the explicit flush after it is durable):
  `crates/cairn-store/tests/shared_wal.rs::forced_flush_does_not_chase_its_own_barrier`.
- **GC trusting the in-memory manifest** (a long-standing bug, exposed by `shared_wal_sim` seed 9
  under load in a Ready run). SlateDB advances `replay_after_wal_id` in memory before writing the
  manifest; a GC tick in that gap deleted frames the stored manifest did not cover yet, the node
  died, the taker replayed from the old mark: acked rows lost. Fixed: see NW-7.
- **One failed DELETE makes the GC round drop a durable floor** (a 4 h soak). `gc_log` returned
  `Err` when one DELETE of the batch failed — **after** the floor was written — so `gc_loop` did
  not record the floor, did not cut the in-RAM index, did not report the floor; the next round
  walked again from the old floor and deleted the whole batch again. The `s3-standard-faults*`
  profiles fail 0.2 % of DELETEs, so from ~2 000 objects almost every round failed: the batch grew
  with the incarnation's life, the GC round took minutes (the floor on the bucket stood still
  between two rounds for > 600 s in an incarnation living 4 hours), the index grew, the node's
  floor alert never fired, and the stream of DELETEs pressed on the shared store. It only showed
  in long-lived incarnations (chaos kills each node every few tens of seconds). Fixed: see NW-7.
- **One hung manifest read stalls the whole floor GC loop.** `stored_refs` `.await`ed
  `Admin::referenced_wal_ranges` directly, and `Admin`'s store retries forever in production: one
  manifest read hanging or failing forever kept `gc_loop` at that log, and every log after it on
  the node stopped being cleaned. Fixed: `admin_io` (limit `ADMIN_IO_BOUND`, op `slatedb.admin`),
  check 3 of `lint-sources.sh` names the operation. See NW-7.
- **A floor stuck because D-d deleted the range's DB** (G-c). A leak, no data loss. See NW-7.
- **A dead log's floor stuck for tens of minutes because a retired range still has its DB** (a
  chaos run with the invariant "the floor never stands still for more than 10 minutes"). A
  checkpoint in the manifest of the retired DB pointed at frames that were replayed but not yet
  flushed when it was taken; the DB lives until the child ranges stop referencing it. Fixed: a
  range carrying a tombstone references no frame. See NW-7.
- **The memtable flush gap pins the floor** (a chaos run, the floor stuck for 605 s, a live range
  changing leader > 20 times). SlateDB freezes the memtable at `last_flushed_wal_id` at freeze
  time: frames not yet durable then have rows in the L0 SST while the mark stops below them. Then
  (a) a takeover replays that frame, the rows are filtered, the memtable is empty, the C6 flush is
  a no-op; (b) the age-forced flush of an idle range is a no-op too; (c) the compactor's 15-minute
  checkpoint captures a manifest with a gap. Fixed: `flush_through` (a fence when needed) for
  (a)(b), checkpoints referencing no frame for (c). See NW-4, NW-7.
- **Idle ranges pinning objects forever.** GC kept the `replay_after_wal_id` file (`Included`) as
  with the native WAL. `refs()` turns that mark into `Excluded`, and `age_flush_loop` nudges ranges
  that are not writing.
- **Noisy neighbours.** One range writing objects of the limit's size made other ranges wait. The
  `object_cap` limit bounds that; a stream's window is the smallest among the ranges with frames
  waiting. Tests: `noisy_range_bounded_latency`, `range_window_spaces_cuts_for_that_range`.
- **A seal that cannot survive one failed GET** (exposed after fixing the bug below,
  `s3_slowdown`). Incarnations no longer fail-stopped continually, so logs were long; one 503 GET
  failed the whole seal and the takeover started over — the range had no leader. Fixed:
  `retry_io`. See NW-3.
- **Retrying PUTs without pausing** (`s3_slowdown`). Under 503s returned at once, the
  `put_attempts` attempts were spent within ~0.1 ms and then the whole incarnation fail-stopped.
  Fixed: the budget is time (`stall_max`), with backoff + jitter. See NW-2.
- **One ambiguous or slow log PUT takes the whole node down** (a soak: 4/7 fail-stops caused by a
  PUT exceeding `stall_max`; the each-once+nodewal row: 83/114 cluster tests red). The
  `AlreadyExists` of an object_store retry of a 5xx was treated as a seal, and every stream shared
  one `FailStop`. Fixed: read back on `atomic_put` stores, do not cancel slow PUTs, and rotate
  exactly the stream instead of taking the node down. See NW-2, NW-5.
- **A node with all its WAL PUTs slow still takes the incarnation down** (chaos: 18 `wal_stall`
  in 15 minutes). The stream rotated correctly, but the new log's floor-0 `Create` was slow too
  and the claim cut each attempt at `put_timeout` ⇒ "rotation failed" ⇒ fail-stop. Fixed: a limit
  of 2 × `stall_max`, the slot stays rotated and retries in the background, the node drains. See
  NW-2.
- **The window drifting with timer latency.** The cut deadline used to be "head of queue +
  window": 14–26 % slower than the native WAL (comment in `Stripe::run`). Now the deadline sits on
  a grid spaced one window apart.
- **Cutting too early (Nagle).** An idle stream cut the first append at once, and the second append
  of the same batch of SDK frames, arriving 0.2–0.5 ms later, had to wait for that whole PUT
  (W = 1): on S3 each durability wait cost ~2 PUTs. Fixed: linger L = 1 ms by default
  (2026-10-21). Test: `default_linger_is_1ms_and_halves_puts_of_a_sparse_burst`.

**The `// #[hot-path]` mark** (AGENTS.md §6). `nodewal/stripe.rs` is one of the files carrying the
mark. In a marked file: no logging at level `info` on the per-turn/commit path (`append`, `cut`,
`ack`); no global locks (only the stream's own `State` lock, held briefly, never across an
`await`); no synchronous I/O (the PUT runs inside `run`'s future). `warn`/`error` on failure paths
(`put` missing, `die`, `register` detaching an old DB) are fine.
