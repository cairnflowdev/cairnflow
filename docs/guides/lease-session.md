# Guide: leases, sessions and range leadership

*This is a map, and a hypothesis: it drifts faster than the code. Check in the code before
trusting it. Search by function name; the guide deliberately records no line numbers.*

Three layers, each with one job. The **session** (`sessions/<node>/<inc>`) says the node is
alive, and is renewed once for every range of the node (D20). The **owner record**
(`owners/range-<id>`) says who leads a range, and is written only when the owner changes.
SlateDB's **`writer_epoch`** together with the object store's conditional PUT is the real write
fence. The two layers above only decide who *should* lead.

Objects of this layer on the bucket (main store):

| Object | Who writes | Who reads | Who deletes, when |
|---|---|---|---|
| `sessions/<node>/<inc>` | the owner (`Create`, renewal CAS, mode-change CAS `mode=solo`/`mode=delegated coord=` — LS-14/LS-16, `closed`); observers (`expired` CAS, challenge CAS `challenged=` on a `solo` record — LS-16); the coordinator (`expired` CAS of a record `delegated` to its role — LS-15) | the tracker, `claim`, the shared-WAL sweep, the self-check GET of a `solo`/`delegated` owner | the tracker: sessions ended ≥ `GC_AFTER_TTLS` TTLs ago |
| `control/quiet-lease` | whoever builds a new bucket (`migrate.rs:bootstrap_fresh`, `Create`, on with delegation by default — LS-16a); `cairn-ctl cluster enable-/disable-quiet-lease`, `record-evidence` | the owner: the first time in an incarnation and every `quiet_lease_poll` while alone; the coordinator: every `quiet_lease_poll` (`Session::quiet_lease_on`, the gate's run-time rule — LS-15) (GET `If-None-Match`); missing or broken ≡ off | nobody |
| `incarnations/<node>/<inc>` | `open_incarnation` (`Create`, nonce) | `open_incarnation` (LIST, the lower bound of `inc`); missing = the old lower bound | `open_incarnation`: markers smaller than the one just taken, after the new session has opened |
| `owners/range-<id>` | `claim` (CAS), the keeper (release, tombstone, WAL segment), the proxy-tombstone round | everyone | tombstones stay; only records of orphan children that never entered the table are cleaned up (`split-merge.md`) |
| `control/coordinator` | a node taking the role (CAS `term + 1`, `cas::put_resolving`), the coordinator releasing it (CAS `released`) | every node's control loop (`cairn-server/src/control.rs`), the coordinator fence | nobody deletes it; missing ≡ no coordinator (every node observes the bucket itself) |
| `control/registry-gen` — not part of the lease, but the coordinator has one job on it | every registry write through `RegistrySync::write` (odd CAS + an `open=` token before the main write, even CAS after); the **coordinator** removes a token standing longer than `registry_stuck` (`RegistryGen::seal_stuck`, every tick of its loop, no I/O when even) | `RefreshLoop`/the quarantine loop (GET `If-None-Match`, or the generation in the `view`); odd ⇒ full loop every tick, nothing recorded | nobody; missing ≡ full loop every tick. The freshness of the key registry follows these reads: a node cut off from both the bucket and the coordinator for more than 2·`deploy_refresh` closes requests authenticated with keys from the registry (`registry_gen.rs`, test `tests/registry_gen.rs`). It also follows the node's own session (`registry_gen::SessionGate`): the copy counts as fresh only while the incarnation's grant is valid and only once a **full** refresh round (keys, tenants and deployments all re-read — not a key-only read, not a generation hint from `view`) confirmed it after that incarnation opened, and the balancer takes no range for an incarnation before that (`RegistryGen::session_ready`; a won claim is re-checked, a range handed to a not-ready incarnation is given away after the rescue grace; a catalog pointer that cannot be resolved does not count as a failed read) — so a peer whose record an observer ended (a full TTL of silence, after the grant lapsed at ⅔ TTL, LS-2) no longer serves registry keys, and a synchronous registry write may count it as applied (`RegistrySync::confirm_peers_at`); an incarnation opened in-process re-reads before serving (tests `tests/registry_gen.rs::bucket_cut_node_refuses_a_revoked_key_once_the_revocation_is_acked`, `::reincarnated_node_takes_no_range_before_a_full_registry_round`, `::dangling_catalog_pointer_does_not_keep_a_reincarnated_node_out`, `registry_gen::tests::session_changed_during_boot_needs_a_full_round`, `registry_gen::tests::freshness_needs_a_valid_grant`, `::new_incarnation_is_stale_until_the_registry_is_read_again`, `::key_only_confirmation_does_not_reopen_a_new_incarnation`). The in-RAM copies of `tenants`, `auth/keys`, `auth/oidc`+`auth/revoked` are applied **in read order** (`registry_sync::ReadOrder`: a ticket is taken before the GET, a node's own write takes a ticket after its CAS, a ticket older than the copy being applied is dropped) — an old GET of the refresh loop (an odd generation triggers it right at write time) arriving late never undoes a copy that `/internal/registry/apply` already acked (tests `tests/failpoints.rs::stale_tenants_read_never_undoes_an_acked_suspension`, `::stale_keys_read_never_undoes_an_acked_revocation`, `::booting_node_that_read_the_registry_blocks_the_ack`, `tenants::tests::stale_refresh_never_undoes_a_newer_book`, `auth::tests::stale_refresh_never_resurrects_a_revoked_key`, `auth::oidc::tests::stale_refresh_never_brings_back_a_removed_client`) |

## 1. Invariants

- **LS-1. An ended session stays ended.** `expired` and `closed` are permanent states. Losing the
  session means opening a new incarnation, never reviving the old one. An observer only CASes
  `expired` after one TTL of silence counted from when it *received* the largest beat (proved at
  the top of `session/mod.rs`). **The silence rule only applies to `direct` records** (no `mode=`
  line): for a `mode=solo` record (LS-16) the observer challenges and then waits one TTL from the
  challenge, without counting silence from the last beat — a `solo` owner renews without writing;
  for a `mode=delegated coord=X:t` record (LS-14) only the coordinator of exactly the `X:t` role
  counts silence (from the latest proof of life), and other observers **yield** (LS-15); a mode
  this binary does not know (`SessionMode::Other`) is neither expired nor challenged
  (`tracker.rs:observe`). In a cluster that has a coordinator, a record that **cannot be decoded**
  is not handled by LS-1 either (it may be a torn delegation CAS) — only the coordinator expires
  it (LS-15). Tests:
  `crates/cairn-store/src/session/tests.rs::solo::restarted_incarnation_takes_over_after_challenge_and_ttl`,
  `crates/cairn-server/tests/net_partition.rs::node_falling_back_never_expires_a_peer_still_delegated`,
  `crates/cairn-store/src/session/tests.rs::delegation::undecodable_record_is_not_expired_by_a_direct_observer`.
  A failed LIST of `sessions/` (even a prolonged one) does not skip the observation round: known
  sessions are still GET and marked; the LIST is retried next round. The observation round runs
  every `LeaseConfig::observe_interval` — cluster nodes set **TTL/3**, sparser than the TTL/5
  renewal tick (each round is one GET per peer, most of the read cost when idle). A sparser tick
  only makes the "beat received" mark later, i.e. `expired` later — safety is unchanged; a dead
  node is detected after the TTL plus at most one observation tick: ≤ 4/3 TTL (13.3 s at a TTL of
  10 s; 12 s before).
  Tick test: `crates/cairn-server/tests/read_cost.rs::session_tracker_observes_every_third_of_ttl`.
  **Observation modes** (`tracker.rs::Observing`) only change the *tick*, not the rule: `Full` (as
  above — also the fallback path), `Coordinator` (the coordinator: like `Full`, plus new nodes
  reporting themselves through heartbeats — `SessionTracker::adopt`, equivalent to a LIST that saw
  it; the LIST still runs every TTL because sessions that send no heartbeat — old binaries, nodes
  cut off from the coordinator — only reach the table through a LIST), `Sparse` (a node listening to
  a fresh `view` from the coordinator: each session every `SPARSE_AFTER_TTLS` = 2 TTLs, sessions the
  `view` marks as suspect every tick, a safety-net LIST every 10 minutes; `request_refresh` forces a
  full round). In every mode the `expired` CAS still only comes after one TTL of silence counted from
  when the largest beat was *received* — a sparse tick only makes `expired` later (the sparse tracker
  catches a dead node after ~4 TTL + a tick even when the suspect mark is not raised — the
  coordinator can neither read that node nor know it went silent; previously 12 TTL; the suspect
  mark brings it to ≈ 2 TTL + a tick + TTL). A `view` older than TTL/3 (counted from when the acked
  heartbeat was *sent*) ⇒ the node goes back to `Full` at once. Tests:
  `crates/cairn-server/tests/coordinator.rs::nodes_listening_to_the_view_stop_polling_the_bucket`,
  `crates/cairn-server/tests/net_partition.rs::stale_view_puts_nodes_back_on_their_own_tracker`,
  `crates/cairn-server/tests/net_partition.rs::dead_node_is_expired_when_the_coordinator_cannot_read_it`,
  `crates/cairn-server/tests/net_partition.rs::dead_node_is_expired_by_the_sparse_tracker_without_suspects`,
  `crates/cairn-server/tests/coordinator.rs::coordinator_learns_a_joining_node_from_its_heartbeat_not_a_list`.
  Tests: `crates/cairn-store/src/session/tests.rs::stalled_session_expired_after_ttl_not_before`,
  `crates/cairn-store/src/session/tests.rs::paused_owner_learns_expiry_on_next_renew`,
  `crates/cairn-store/src/session/tests.rs::failed_list_still_observes_known_sessions`.
  **"Dead" only comes from the bucket**: `SessionTracker::is_dead` is an ended state that was read,
  or an `expired` CAS of ours that won — never the observer's clock alone. When a session stands
  still for more than `UNEXPIRABLE_AFTER_TTLS` TTLs and the `expired` CAS keeps missing (a torn
  object, stale reads), the observer **overwrites** the record (a `live` copy carrying the largest
  beat it saw — the next renewal of a still-living owner misses ⇒ `lose`), counts silence again
  from when the answer was received, and then CASes `expired` as usual; the proof at the top of
  `session/mod.rs` applies with that mark. A failed overwrite (ambiguous ones included) ⇒ only a
  warning + `cairnflow_session_rearm_total{outcome="failed"}`, and the count still restarts. Every
  irreversible action based on `is_dead`/`is_session_dead` — `claim` (LS-3), the migration lock
  (`migrate.rs:holder_is_dead`), proxy tombstones (`gc.rs:OrphanRetirer`) — therefore stands behind
  a fence on the bucket; the shared-WAL sweep reads the record directly
  (`proof::SessionDeath::observed`). Routing and balancing also read `is_dead`/`alive`, but only as
  a hint, through `Membership::looks_dead`: `Membership` is the hint-only membership view for every
  user outside the lease layer, and is never the source of `is_session_dead`. `Membership` takes
  the coordinator's `view` when the `view` is fresh (ack sent within the last TTL/3), otherwise the
  tracker; "ended" in the `view` comes from the coordinator's tracker (the bucket) but over the
  network, so it only feeds `looks_dead` (merged with our own tracker) and `Membership::ended`
  (nudging the proxy-tombstone round — that round decides "dead" itself through
  `is_session_dead`). Model M1: `Action::JudgeUnexpirable`, flag `unexpirable_rearm` —
  `crates/cairn-model/tests/m1.rs::torn_views_hold_with_rearm_for_split` (holds),
  `crates/cairn-model/tests/m1.rs::unfenced_unexpirable_judgment_breaks_ls3` (flag off ⇒ two
  leaders).
  An ambiguous `Create` opening an incarnation is resolved by reading back (`cas::put_resolving`;
  only that node writes `sessions/<node>/…`): before this, an `AlreadyExists` from object_store's own
  retry opened `<inc + 1>` and left `<inc>` as a ghost `live` session nobody renewed — peers counted
  it as a live node until the TTL ran out.
  Test: `crates/cairn-store/src/session/tests.rs::ambiguous_open_does_not_leave_a_ghost_incarnation`.
  The open at boot (`Session::establish_within`) retries transient I/O errors within the boot limit
  (`boot::BOOT_IO_DEADLINE`, op `boot.session`) with the **same** `<inc>` and the same content — no
  new LIST: if the previous attempt landed and the read-back also failed, a new LIST would open
  `<inc + 1>` and leave exactly the ghost session above. A reopen inside the renewal loop is still
  a single attempt (the loop backs off by itself). Test:
  `crates/cairn-store/src/session/tests.rs::boot_open_retries_the_same_incarnation`.
  **`inc` is never reused, and is not taken from the clock**: before using an `inc`,
  `open_incarnation` takes it with a `Create` of the marker `incarnations/<node>/<inc>` (the content
  carries the open's nonce — ambiguity is resolved by reading back); an `AlreadyExists` that is not
  ours ⇒ `inc + 1`. The lower bound is the largest marker on the bucket (LIST
  `incarnations/<node>/`), so a clock that went backwards after the old session was cleaned up does
  not reopen an old shared-WAL log name. A bucket without markers keeps the old lower bound
  `max(listed + 1, now_ms)`. Markers smaller than the one just taken are cleaned up after the new
  session opens — the largest marker is never deleted.
  Tests: `crates/cairn-store/src/session/tests.rs::restarted_node_never_reuses_incarnation_after_session_gc`
  (a clock going backwards through `cairn_store::entropy`),
  `crates/cairn-store/src/session/tests.rs::incarnation_markers_bound_inc_and_only_the_newest_stays`,
  `crates/cairn-store/src/session/tests.rs::ambiguous_incarnation_marker_is_resolved_by_reading_back`.
- **LS-2. A grant expires by its own clock; only a winning CAS revives it.** It is valid until
  `t_k + ⅔·TTL`. Past that with no renewal answer, the grant is **suspended**: no longer valid —
  no acks, no read answers, `commit` and `Lease::sync` wait instead of failing — but not lost:
  `lost()` does not fire, the engine does not step down, nobody releases ranges. A renewal that
  wins its CAS **from exactly the previous version** moves the deadline to `t_j + ⅔·TTL` of that
  very issue (`extend_until`) and the grant runs again — a winning CAS proves nobody CASed
  `expired` or overwrote the record, i.e. nobody has taken a range. In delegated mode (LS-14) the
  deadline is also moved by the coordinator's **ack**: to `t_sent + ⅔·TTL` of the acked heartbeat —
  and only that; an ack cannot move a revoked root or one past the cap. A missed CAS (the re-read
  finds the session ended, a foreign beat, the record gone) or passing **the suspension cap** (the
  last validity deadline + ⅔·TTL, i.e. the last issue + TTL + TTL/3; a win whose new deadline is
  still in the past does not move the cap) means `lose` as before. Metric
  `cairnflow_session_revived_total`.
  **The clock assumption is checked at run time**: LS-1/LS-2 hold when the nodes' monotonic clocks
  run within 1.5× of each other. Each `s3://` node compares the drift of its monotonic clock with
  the drift by the store's `Date` header (samples from existing requests —
  `store_clock.rs:DateConnector`, no extra request; a 5-minute window, a certain interval
  `[lo, hi]`). The whole interval outside ±10% (`clock_rate.rs:TOLERANCE`; two nodes within the
  threshold differ by ≤ 1.22×) ⇒ the node **stops leading**: it releases the coordinator role and
  does not take it back (`Control::set_clock_trusted`), sets `SlowMeter::clock_draining` (the
  balancer refuses ranges, the record publishes `draining=1`) and yields every range; the whole
  interval within ±5% ⇒ it takes ranges again. No other node able to take the ranges ⇒ it keeps them
  (as for LS-11), only logging an `error`. Metrics `cairnflow_clock_rate`,
  `cairnflow_clock_rate_untrusted`, `cairnflow_clock_rate_stepdowns_total`. Safety is unchanged
  (LS-4/LS-5 do not use clocks): this only turns a silent assumption into a checked condition.
  Tests: `crates/cairn-server/tests/clock_rate.rs::node_with_a_skewed_clock_steps_down_and_comes_back`,
  `crates/cairn-server/tests/clock_rate.rs::clock_within_tolerance_keeps_leadership`,
  `crates/cairn-store/src/store_clock.rs::tests::rate_bounds_hold_the_true_rate`,
  `crates/cairn-store/src/store_clock.rs::tests::date_connector_samples_real_responses`.
  A range's grant is a child of the session's validity (`LeadershipGrant::from_session`), so it
  never outlives the session. The renewal loop runs on the multi-threaded runtime, not on the
  engine thread, and **waits for no side work**: background release retries run in a loop of their
  own (`release_loop`), each write bounded by one TTL — a hung release PUT does not make it miss
  ⅔ TTL. The bound on one renewal is **the remaining deadline**, not one tick; ticks are measured
  from when the previous one was *issued*; for every tick without an answer one more identical CAS
  is issued in parallel, without cancelling the previous one — a node whose PUTs are slower than
  the tick but under ⅓ TTL keeps its session, and a hung connection does not eat the whole deadline.
  An ambiguous renewal (`Err` that may have written — AGENTS.md rule 7) is adopted when a read finds our beat,
  with the issue mark of the **oldest** ambiguous attempt with that beat: an observer may have seen
  that beat since that attempt wrote it.
  Tests: `crates/cairn-store/src/session/tests.rs::renewal_slower_than_pace_keeps_session`,
  `crates/cairn-store/src/session/tests.rs::renewal_slower_than_deadline_loses_session`,
  `crates/cairn-store/src/session/tests.rs::hung_renewal_put_is_hedged_not_waited_out`,
  `crates/cairn-store/src/session/tests.rs::ambiguous_renewal_counts_from_its_own_issue_time`,
  `crates/cairn-store/src/session/tests.rs::revokes_by_deadline_when_renew_hangs`,
  `crates/cairn-store/src/session/tests.rs::hung_background_release_does_not_block_renewal`,
  `crates/cairn-store/src/session/tests.rs::grant_bounded_by_session_when_owner_put_succeeds_but_renew_hangs`,
  `crates/cairn-store/src/writer/mod.rs::tests::validity_expires_by_clock_and_late_extension_cannot_revive_it`,
  `crates/cairn-store/src/session/tests.rs::renews_while_partition_thread_blocked`.
  Suspension and revival:
  `crates/cairn-store/src/session/tests.rs::late_renewal_that_wins_cas_revives_the_session`,
  `crates/cairn-store/src/session/tests.rs::late_renewal_that_loses_cas_loses_the_session`,
  `crates/cairn-store/src/session/tests.rs::paused_session_is_lost_at_the_pause_cap`,
  `crates/cairn-store/src/writer/mod.rs::tests::paused_root_revives_on_extension_and_is_lost_past_the_cap`,
  `crates/cairn-store/src/writer/mod.rs::tests::settled_waits_out_the_pause`,
  `crates/cairn-server/tests/cluster_lease.rs::late_renewal_that_wins_cas_keeps_every_range`
  (the node keeps every range at the same term, and read commands during the suspension are only
  answered after the revival).
  Model M1 (`crates/cairn-model/src/m1.rs`, exhaustive): a validity longer than the TTL lets an
  observer mark `expired` while the owner still believes it leads —
  `crates/cairn-model/tests/m1.rs::grant_outliving_ttl_breaks_ls3`; revival (`RenewLate`) with the
  CAS fence keeps every invariant (`all_fences_hold_*`), and without the fence LS-3 breaks —
  `crates/cairn-model/tests/m1.rs::revival_without_cas_breaks_ls3`.
- **LS-3. Ownership changes only by CAS on the owner record.** Of two claimers of a range,
  exactly one wins. A session *never seen* is not considered dead. The ranges of a frozen node move
  away no earlier than the lease's silence period. A `claim` on a record that already belongs to
  this session rewrites it with a new term, so every older keeper of this session for that range
  goes stale (its next CAS misses). Whoever only wants "the range must be ours" without stealing
  its own engine's keeper uses `claim_from_others`.
  Tests: `crates/cairn-store/src/session/tests.rs::two_claimers_one_wins`,
  `crates/cairn-store/src/session/tests.rs::claim_matrix`,
  `crates/cairn-server/tests/cluster_lease.rs::frozen_node_partitions_move_after_ttl_not_before`.
  An ambiguous owner-record CAS (object_store retries a 5xx itself: the first attempt wrote, the
  retry sees `Precondition`/`AlreadyExists`; or `Err` after the write) is resolved by reading back
  and comparing content (`write_owner` → `cas::put_resolving`; the record carries session + term,
  so a match is ours) — it never becomes a false `Held(None)` or `LeaseLost`, which with the shared
  WAL would make `acquire_shared` fail-stop the whole node.
  Test: `crates/cairn-store/src/session/tests.rs::ambiguous_owner_cas_is_resolved_by_reading_back`.
  The network between nodes is not a source of truth about leases: a node cut off from every peer
  that still sees the bucket remains a valid leader, no range gets two leaders, and acked work is
  not lost (`--features testkit`):
  `crates/cairn-server/tests/net_partition.rs::isolated_node_still_on_bucket_loses_no_acked_work`.
  The other direction: a node cut off from the bucket but still reachable by peers loses its grant
  within ⅔·TTL by its own clock (without waiting for anyone to tell it), stops answering as a
  leader, and its ranges move after the lease's silence period — peers hearing it do not make it
  any more "alive":
  `crates/cairn-server/tests/cluster_lease.rs::bucket_cut_node_loses_leadership_within_two_thirds_ttl_and_ranges_move`.
  A coordinator gathering ranges does not steal its own engine's keeper:
  `crates/cairn-server/tests/net_partition.rs::merge_right_after_source_leader_dies_never_returns_500`.
  Model M1 (invariant `LS3_LEADER_IS_RECORDED_OWNER`: an engine whose grant is still valid is the
  one the owner record points to) holds with every fence —
  `crates/cairn-model/tests/m1.rs::all_fences_hold_for_split` — and breaks when `claim` drops the
  live-owner rule: `crates/cairn-model/tests/m1.rs::without_owner_cas_two_nodes_believe_they_lead`.
- **LS-4. The write fence is `writer_epoch`, not `term`.** `PartitionWriter::open` bumps the
  epoch and fences the old writer, including a frozen process that does not know it has lost its
  right. A zombie is stopped at the durability wait, not at `put`.
  Tests: `crates/cairn-store/tests/store.rs::old_writer_fenced_when_new_writer_starts`,
  `crates/cairn-store/tests/store.rs::frozen_old_leader_cannot_write_after_b_starts`.
  Model M1 (invariant `SINGLE_WRITER`: at most one writer per Db can write): dropping the
  owner-record CAS while keeping `writer_epoch` still holds
  (`crates/cairn-model/tests/m1.rs::without_owner_cas_writer_epoch_still_keeps_one_writer`,
  `#[ignore]`, run by hand), dropping both breaks it
  (`crates/cairn-model/tests/m1.rs::without_owner_cas_and_writer_epoch_two_writers_meet`).
- **LS-5. No lease, no ticket** (INV-5, D11). `alive()` runs for every `Committed` ticket,
  including tickets issued on the spot, so a leader that lost its right cannot answer even a
  read-only query. `commit` stops early with `LeaseLost`; that is an optimisation, not the safety
  mechanism. While the grant is **suspended** (LS-2), `commit`, `Pending::durable` and
  `Lease::sync` wait until it revives (a ticket issued under the new deadline) or is lost
  (`LeaseLost`) — a turn in flight takes no error branch, so the engine's in-RAM state is still
  right when it continues.
  **The read fence at the leader**: `alive()` only trusts the node's clock, so a node whose clock
  stood still (a sleeping machine, a paused VM) wakes up and still answers reads while the range
  has moved to someone else. Every command answer at the leader (`Shared::finish` →
  `Lease::sync_reply`) goes through a conditional GET (`If-None-Match` on the etag of the last
  renewal) of the incarnation's session record, issued **after** the read, **one GET per batch for
  the whole node** (`session/fence.rs`: a channel + a background loop; whoever arrives while a GET is
  in flight waits for the next batch). Still our `live` ⇒ answer; ended, gone, a GET failing past
  its limit or older than TTL/3 ⇒ `LeaseLost` (the client gets `NotLeader`). A transient GET failure
  is **retried** within the TTL/3 limit (`retry::ops::SESSION_READ_FENCE`): every attempt is still
  issued after the read, so it is evidence just like the first; without retries one 5xx refused the
  whole batch, and `/invocations?scope=cluster` (one fence per range) lacked a range on nearly every
  query at a few hundred ranges
  (`crates/cairn-server/tests/observability.rs::transient_read_fence_failure_does_not_drop_a_range_from_the_answer`).
  For tickets that must wait for durability the fence runs in parallel with that wait (cost
  `max(PUT, GET)`). The fence loop stands outside `tasks`, like the owner-record watch loop. Metrics
  `cairnflow_read_fence_waiters_total`, `cairnflow_read_fence_total{outcome}`.
  Outside the fence: followers, the early notice of `invoke` within a turn
  (`turn::finish_invocation_notifying`), nodes without a session, and
  `Shared::finish_unexposed` — a durable ticket + `alive`, no GET — only for answers **nobody
  sees** ("not yet" from `Command::Settled`, the blocking `/invoke` question loop: the "done"
  answer goes through the fence) or answers that **only assert durable history**
  (`Command::Inbound`: our write reached the store before we were fenced — LS-4 — and the new owner
  replays it). An answer reading state never takes that path.
  **The evidence must be issued after the read**: a fence GET (or a winning renewal CAS) issued
  after the last read of an answer covers **every** earlier read in the same incarnation — so a
  batch gathers every request in the channel **before** the GET is issued, and a turn reading
  several things answers through one fence (`Command::View` of `GET /invocations/{id}`); evidence
  issued **before** the read does not count (M1 `proof_after_read`).
  Tests: `crates/cairn-server/tests/failover.rs::leader_waking_after_losing_its_range_never_serves_a_stale_read`,
  `crates/cairn-store/src/session/tests.rs::read_fence_batches_and_refuses_after_expiry`;
  cost: `crates/cairn-server/tests/read_cost.rs::blocking_invoke_pays_the_read_fence_once_not_per_poll`,
  `::status_read_pays_one_read_fence`.
  Model M1 (`NO_STALE_READ`): a lying clock (`validity > ttl`) without the fence reads stale data —
  `crates/cairn-model/tests/m1.rs::lying_clock_without_read_fence_serves_a_stale_read`; with the
  fence it is clean — `::read_fence_keeps_reads_fresh_with_a_lying_clock`; two-phase reads
  (`split_reads`) are clean — `::proof_issued_after_the_read_keeps_reads_fresh_with_a_lying_clock`
  —, and reusing existing evidence reads stale data — `::proof_issued_before_the_read_serves_a_stale_read`.
  Tests: `crates/cairn-store/tests/durability.rs::no_ticket_after_lease_lost`,
  `crates/cairn-store/tests/store.rs::lost_lease_blocks_before_write`.
- **LS-6. Lost leadership is detected in three layers, and each layer steps the range down.**
  Layer 1 is `keeper.lost()` — it fires when the grant is **lost** (revoked, past the suspension
  cap), not when it is only suspended (LS-2). In delegated mode (LS-14) layer 1 also fires when the
  self-check GET finds our record ended or changed by someone else (`delegate.rs:delegated_check` ⇒
  `lose`), and when the coordinator answers `expired` and the fallback CAS then misses. Layer 2 is
  `Fenced`/`health.closed()` from the store itself, plus the watchdog on a stalled `durable_seq`.
  Layer 3 is `owner_watch_loop`, driven by **the claimant's notice**: a winning claim CAS on the
  record of another session that had not released it ⇒ `Session::on_owner_taken` ⇒ the node pushes
  `POST /internal/owner/changed` to the `address=` of the old record (`owner_notice.rs`, through
  `ClusterSecret::send`, a background task — no network call on the `claim` path, D16) ⇒ the old
  owner's `Session::owner_changed` GETs exactly that record. The notice is not trusted (LS-3): only
  a bucket read finding the record no longer ours revokes the grant. **A second path: the
  coordinator's `view`** (`control.rs::check_owners`): a node receiving a `view` (a heartbeat ack;
  for the coordinator, the `view` it builds every batch) in which another, unended session holds a
  range we hold with a term ≥ ours ⇒ the same `Session::owner_changed` (at most once per TTL for
  each range–holder pair); the `view` is also only a hint — the owner-record GET decides. A lost
  notice that still reached the coordinator: the old owner sees it within ~`renew_interval` + one
  GET. The safety net: every `LeaseConfig::owner_watch` (default `DEFAULT_OWNER_WATCH` = **1
  minute**; it was 10 minutes) one GET `If-None-Match` per range held, compared with the version mark
  it remembers — the keeper's own writes (`LeaseKeeper::note_written`) update that mark, so idle
  ranges get `304`. No LIST (previously one LIST of `owners/` per TTL for the whole node, and before
  that also one GET per range after every claim). When the notice **and** the `view` both miss (cut
  off from both the claimant and the coordinator, the claimant dying right after its CAS, no
  coordinator), the old owner finds out at the safety net (≤ 1 minute) — this layer only takes care
  of **liveness**: its writes are stopped by LS-4, its reads by LS-5. Layer 3 stands outside
  `tasks`, so `abort`/`CrashHandle` do not kill it.
  Tests: `crates/cairn-server/tests/failover.rs::frozen_leader_other_node_takes_over_with_data_intact`
  (the notice is wired in-process; the probe uses `flow`, not the read fence — a probe through the
  read fence used to hide whether layer 3 ran at all),
  `crates/cairn-server/tests/cluster_lease.rs::old_owner_steps_down_at_once_when_the_claimant_tells_it`
  (the notice through the internal port, a 10-minute safety net),
  `crates/cairn-server/tests/net_partition.rs::old_owner_steps_down_from_the_coordinator_view_when_the_notice_is_lost`
  (the notice blocked, a 10-minute safety net: steps down within ≤ 2·`renew_interval` thanks to the
  `view`),
  `crates/cairn-server/tests/net_partition.rs::old_owner_cut_from_notice_and_coordinator_steps_down_at_the_grid`
  (cut off from every peer: the safety net),
  `crates/cairn-server/tests/cluster_lease.rs::default_owner_watch_grid_is_one_minute`,
  `crates/cairn-server/tests/liveness.rs::partition_steps_down_when_db_closes`,
  `crates/cairn-server/tests/liveness.rs::partition_steps_down_on_durability_stall`,
  `crates/cairn-server/tests/idle_cost.rs::idle_one_node_cluster_range_sends_almost_nothing`.
- **LS-7. A non-leader answers at once** (`NotLeader`), even while a seal hangs
  (`reject_until_leader`; `step_down` still reads the command channel). `Shutdown`, `Resign` and
  `Quiesce` answer **after** the seal is done.
  Tests: `crates/cairn-server/tests/failover.rs::stopping_a_leader_remaining_node_returns_notleader`,
  `crates/cairn-server/tests/liveness.rs::commands_answered_while_seal_hangs`,
  `crates/cairn-server/tests/quiesce.rs::shutdown_and_resign_reply_after_seal`.
- **LS-8. Quiesce keeps the lease. A retired range is never claimed again.** A seal lowers the
  *writer's* validity, not the lease's. The tombstone (`retired`) makes `claim` return
  `RangeRetired` for everyone, ourselves included (D22). A coordinator's quiesce keeps the lease
  **as long as the coordinator lives**: when the `quiesce_held` wire is cut the engine releases as
  for `Resign` (SM-15 in `split-merge.md`,
  `crates/cairn-server/tests/failpoints.rs::coordinator_panic_after_quiesce_releases_and_rolls_back_the_source`).
  The tombstone is written by the leader on retirement; for a range that has left the table whose
  owner died before that, the proxy-tombstone round writes it instead, by CAS and by **exactly** the
  "owner is dead" rule of `claim` (SM-11 in `split-merge.md`) — that rule is not loosened anywhere
  else.
  Tests: `crates/cairn-server/tests/quiesce.rs::quiesced_range_keeps_lease_and_stays_closed`,
  `crates/cairn-server/tests/quiesce.rs::retired_range_is_never_reacquired`,
  `crates/cairn-server/tests/failpoints.rs::parent_left_without_tombstone_is_retired_after_its_leader_dies`.
  Model M1 (invariant `LS8_RETIRED_NEVER_REACQUIRED`, including SM-11's proxy tombstones):
  `crates/cairn-model/tests/m1.rs::all_fences_hold_for_split`.
- **LS-9. Every term starts with recovery**: `restore_flow` rebuilds the VO locks **before the
  first permit grant** — in chunks, as a task of the serving loop, with the grant gate closed until
  the last chunk (FL-8); only when the queue `seq` counter could not be seeded from the durable mark
  does it run before the serving loop (a phase that reads no commands; a lost lease makes it give
  up at once). A failed `restore_flow` ⇒ step down, and that term grants no permit (it may already
  have answered reads). Then `recover` runs **as a task of the serving loop** — the loop answers
  commands while it scans. `recover` only *dispatches* work to the scheduler, runs no turns, and
  skips invocations that have a scheduled retry and invocations someone is working on (lock busy, a
  dispatched turn not yet done).
  **The scan goes in chunks**: at most `RECOVER_CHUNK` (1 024) rows of the `Invocations` index per
  engine hold, releasing the engine between two chunks, dispatching the chunk's turns within that
  same hold — the index includes finished invocations until they are purged, so one hold over the
  whole index is O(history). This is correct because turns were already allowed to interleave: each
  id is asked "is anyone working on it" and "does it have a scheduled wake" in the same hold as its
  dispatch; an id that moved to `running` in an earlier chunk is dispatched by the path that moved
  it. `end_opening` comes after the last chunk; the rescue net does its full scans with the same
  scan. `DeploymentsChanged` answers at once and then runs the same reconciliation (merged if one is
  already waiting; when a new one starts, the scan in progress stops at its next chunk — the new one
  starts again from the beginning). The node's background loops wait for the engine with a bound:
  catalog-change notices (`NOTIFY_WAIT`, all ranges in parallel), the balancer's `resign` and
  self-release (`RESIGN_WAIT`). A failed `recover` scan (503) does **not** step down (a failed
  `restore_flow` does): it marks the term's rescue net (`drainer::Rescue::mark_all`), and
  `rescue_loop` rescans with backoff within the same term (FL-16 in `flow.md`).
  Tests: `crates/cairn-server/tests/single_reader.rs::recover_does_not_block_serving`,
  `crates/cairn-server/tests/recovery_storm.rs::recover_scans_in_chunks_whatever_the_history`,
  `crates/cairn-server/tests/failpoints.rs::key_revocation_reaches_a_node_whose_engine_is_stuck_in_recover`,
  `crates/cairn-server/tests/stale_state.rs::deployments_changed_does_not_rerun_an_invocation_mid_turn`,
  `crates/cairn-server/tests/failpoints.rs::failed_recover_scan_is_retried_within_the_term`,
  `crates/cairn-server/tests/failpoints.rs::failed_restore_flow_steps_down_instead_of_serving`,
  `crates/cairn-server/tests/recovery_storm.rs::sleeping_invocations_are_not_redriven_on_leader_change`,
  `crates/cairn-server/tests/flow_control.rs::killing_leader_mid_turn_neither_leaks_nor_wedges`.
- **LS-10. The balancer only decides who *tries* to hold a lease, not who writes.** The cap is the
  fair share ⌈ranges / live nodes⌉; `ContendPermit` also counts contests in progress; a node that
  just came up is only counted after `settle_for`. Each cycle releases at most one range. When a
  stable node sits below the floor ⌊ranges / nodes⌋ (by the `owns` hint), every node only contends
  up to the floor, and **one** node at the cap (the smallest id) yields one range after two cycles;
  a yield that does not arrive doubles the backoff next time. No stable live node stays empty while
  there are ranges to share.
  Tests: `crates/cairn-server/src/balance.rs::tests::permit_blocks_over_grab_under_concurrent_contention`,
  `crates/cairn-server/src/balance.rs::tests::failed_yield_doubles_backoff`,
  `crates/cairn-server/tests/cluster_lease.rs::idle_node_gets_a_range_when_ranges_do_not_divide_evenly`,
  `crates/cairn-server/src/balance.rs::tests::node_joining_midway_not_counted_in_fair_share_until_stable`,
  `crates/cairn-server/tests/cluster_lease.rs::rebalance_converges_and_stays`,
  `crates/cairn-server/tests/cluster_lease.rs::balancer_releases_at_most_one_range_per_cycle`.
- **LS-11. A slow node sheds its ranges, one node at a time, and never when it is alone or when the
  whole cluster is uniformly slow.** A slowness score of 1–100 comes only from existing PUTs (session
  renewals, shared-WAL PUT attempts); a node sheds when its score is 100 for two rounds **and** its
  p99 PUT is > 2× the median of the other live nodes, after taking the `shedding` marker by CAS. A
  draining node (`draining=1` in its session) refuses ranges on every path of `Balancer::acquire`,
  and other nodes leave it out of the floor/cap. As with LS-10, this is only a policy of *who should
  try* to hold a lease; the write fence does not change. In delegated mode (LS-14): a node does not
  write its record every time its score or p99 changes — those travel in the heartbeat into the
  `view` —, only when `draining` changes or the score **crosses the cap**; a score round without
  samples (an idle delegated node) publishes no p99, so the median leaves that node out instead of
  using a stale number; a draining node goes back to `direct` and does not delegate again while
  draining (renewing by PUT is the measurement it needs to recover). Tests:
  `crates/cairn-server/tests/slow_shed.rs::idle_delegated_peers_publish_no_stale_p99`,
  `crates/cairn-store/src/health.rs::tests::round_without_samples_publishes_no_p99`,
  `crates/cairn-store/src/session/tests.rs::delegation::delegated_node_writes_only_on_slow_threshold_and_falls_back_when_draining`.
  Tests: `crates/cairn-server/tests/slow_shed.rs::slow_node_sheds_everything_then_takes_ranges_back`,
  `crates/cairn-server/tests/slow_shed.rs::uniformly_slow_cluster_never_sheds`,
  `crates/cairn-server/tests/slow_shed.rs::lone_node_never_sheds`,
  `crates/cairn-store/src/health.rs::tests::one_shedder_at_a_time`,
  `crates/cairn-server/src/slow.rs::tests::never_sheds_when_alone`.

- **LS-12. Opening an engine does not re-claim the lease over a transient I/O error.** After
  `claim`, the reads of the open (reloading the table, `cluster-version`, the owner record of a Db
  that existed before the claim, the WAL-kind mark) retry I/O errors in place with backoff + jitter
  (`node::open_io`), **keeping** the keeper, until the lease is lost. Previously each error dropped
  the keeper (a release PUT), slept 0.5 s and claimed again (a claim PUT, term + 1) — under
  prolonged 503s the open itself added to the throttling and the range had no leader. Taking the
  range on the shared WAL and opening the Db are **not** retried with the same keeper (they change
  the keeper's state midway; C1) — they have retries of their own per operation. "I/O error" means
  every kind the lower layer wraps store errors into (`cairn_store::retry::store_verdict`): `Io`
  (`cluster-version`), `LeaseIo` (`cluster.conf`, owner records), SlateDB's `Unavailable` (the
  WAL-kind mark). Previously only `Io` was retried, so a table reload was never retried. **It is
  bounded** (the `cairn_store::retry` primitive): 8 attempts, each ≤ 10 s, the whole operation
  ≤ 30 s — SlateDB's `Admin` retries object-store errors **forever** by itself, so without a bound a
  prolonged store failure would hang the open while holding the lease. A failed open makes the
  leadership loop claim again **with backoff** (0.5 s doubling up to 8 s, jitter; back to 0.5 s
  after every successful open) and keep answering commands while it rests (`NotLeader`; `Shutdown`
  does not wait out the rest).
  **A failed table reload (out of attempts) means no open** — the error goes up, and the leadership
  loop retries the whole open. The RAM table may be older than a split/merge that has committed;
  opening by it means reopening a parent Db that has left the table and whose tombstone is not yet
  written, acking writes into it that are then lost when the parent retires (RD1).
  Tests: `crates/cairn-server/tests/cluster_lease.rs::engine_open_rides_out_transient_get_errors_without_reclaiming`,
  `crates/cairn-server/tests/cluster_lease.rs::engine_open_rides_out_transient_table_reload_errors_without_reclaiming`,
  `crates/cairn-server/src/node/tests.rs` (one test per read, plus non-I/O errors not being retried,
  plus `open_io_stops_retrying_once_the_lease_is_lost` and `open_io_bounds_slatedb_retrying_forever`),
  `crates/cairn-server/tests/liveness.rs::failing_factory_backs_off_and_keeps_answering`,
  `crates/cairn-server/tests/failpoints.rs::stale_table_never_reopens_a_parent_that_left_the_table`.
  Model M1 (invariant `NO_ACK_INTO_RETIRED_DB`): holds with every fence
  (`crates/cairn-model/tests/m1.rs::all_fences_hold_for_split`); planting RD1 back (a swallowed
  table-read error) makes the checker find a counterexample, and SM-13 breaks with it — another node
  can only roll back the intent (SM-3) when it reads a fresh table:
  `crates/cairn-model/tests/m1.rs::d1_swallowed_table_read_error_acks_into_retired_parent`.

- **LS-13. A local data directory belongs to exactly one node, and its old sessions expire at
  once.** `LocalFileSystem` has no CAS on etags; a node on `file://` (`--data-dir`) takes an
  exclusive lock (`flock`) on the store directory at the start of `node::start`
  (`ObjectStores::exclusive_local`, `cairn_store::local::DirLock`) — if it is held, the node does
  not start — and then wraps the store in an in-process emulated CAS (`local::LocalCas`), correct
  because the lock keeps every other process out. The operating system releases the lock when the
  process dies (`kill -9` included), so under the lock every session still `live` belongs to a run
  that has died: `expire_sessions_under_lock` CASes them `expired` **before** `Session::establish`,
  and their ranges can be taken at once instead of after a TTL. The premise of LS-1/LS-2 (the
  owner's grant expires before the `expired` CAS) holds because the owner is no longer running, not
  because of the clock. A missed CAS is dropped — the tracker takes care of it after a TTL as
  always. `cairn-ctl` does not take the lock.
  Tests: `crates/cairn-server/tests/local_dir.rs::local_dir_node_leads_and_restarts_without_waiting_ttl`,
  `crates/cairn-store/src/session/tests.rs::sessions_expire_at_once_under_the_local_lock`.
  Model M1: `Action::ExpireUnderLock` (premise: the owner is dead) — every invariant holds with one
  crash (`crates/cairn-model/tests/m1.rs::all_fences_hold_for_split_with_a_crash`).

- **LS-14. A delegated node renews by the coordinator's ack, only after its delegation CAS is
  confirmed; it proves by itself that it still reaches the bucket, and falls back to `direct` when
  acks stop** (model M1 `M1::delegation`; code: the header doc of
  `crates/cairn-store/src/session/delegate.rs`). *Entering the mode:* a `Grant` from coordinator
  `C` (term `t`) — **permission** (`Grant`) and **renewal** (`Ack`) are two different messages on
  the wire (`control.rs:Lease`) — wakes the keeper; the keeper (every mode-changing CAS goes through
  it, sequentially) CASes `mode=delegated`, `coord=<C>:<t>`, `beat+1` — an LS-2 renewal. Only once
  that CAS is **confirmed** (won, or ambiguous and the re-read finds ours — `adopt`) does
  `Current::delegated` exist. It does not enter while `solo`, while the grant is suspended, or
  within `REDELEGATE_AFTER_TTLS` (2) TTLs after the previous fallback. *Renewal:* an ack for a
  heartbeat sent at `t_sent` (before the first attempt) moves the deadline to `t_sent + ⅔·TTL`
  (`Session::on_lease_answer`) while we still delegate to exactly `C:t` and the heartbeat reports a
  beat ≥ the beat of the CAS that entered the mode (heartbeats sent before the delegation was
  confirmed do not count). **After `lose` the grant is revoked for good** — `extend_until` refuses a
  revoked root or one past the cap (in delegated mode the suspension cap is a safety fence, not only
  a liveness one). *Self-check*: every tick one GET `If-None-Match` of our own record
  (`delegated_check`; merged with the read fence loop — `fence.rs:confirm` → `delegate::touch`):
  it touches the bucket (`Current::touched_at`, whose age travels in the heartbeat —
  `touched_ms`); finding it ended or changed ⇒ `lose`. That GET does **not** renew — unless it finds
  our record carrying the beat of our own ambiguous CAS (AGENTS.md rule 7, `unconfirmed`): then it
  adopts it like `renew_once`'s re-read branch (`adopt`, with the earliest ambiguous issue as the
  mark). This path exists because an ack arriving while a fallback CAS is in flight moves the
  fallback mark, so the next round self-checks instead of CASing the fallback again. Finding it
  ended **while the grant is still valid** is a protocol bug (it never happens if the yield rule
  and the suspension cap hold): a **release-build tripwire** revokes at once, counts
  `cairnflow_lease_delegated_revoked_while_valid_total` and logs an `error`; chaos runs treat > 0 as
  FAIL (`session/tests.rs::delegation::revoked_while_valid_tripwire_fires_and_loses`). *Fallback:*
  the mark is the `t_sent` of the last ack (or when the CAS entering the mode was issued); past
  mark + TTL/3 + jitter (≤ TTL/10), or a `Fallback`/`Expired` answer ⇒ CAS `direct`, `beat+1` — an
  LS-2 renewal (a CAS that misses because we were expired ⇒ `lose`). CASes while delegated (`owns`,
  `draining`) keep the mode, `beat+1`, and do **not** renew (redundant for safety given the yield
  rule that recounts by version). Every self-write bumps the beat. A coordinator change: a `Grant`
  from the new role ⇒ one re-delegation CAS (an LS-2 renewal). A delegated node does not take the
  coordinator role. Tests:
  `crates/cairn-server/tests/delegation.rs::delegated_node_sends_no_session_put_while_acked`,
  `::node_cut_from_bucket_but_not_coordinator_loses_its_ranges` (self-check),
  `::frozen_delegated_node_ranges_move_after_ttl_not_before`,
  `crates/cairn-server/tests/net_partition.rs::node_cut_from_coordinator_but_on_bucket_keeps_leadership`,
  `::isolated_delegated_node_still_on_bucket_loses_no_acked_work`,
  `crates/cairn-store/src/session/tests.rs::delegation::ack_after_pause_cap_never_revives` (the
  suspension cap),
  `::ack_before_delegation_cas_lands_does_not_extend` (only a confirmed delegation renews),
  `::owns_aba_rewrite_still_resets_suspicion` (every self-write bumps the beat); rule 7:
  `::ambiguous_delegate_cas_is_resolved_by_reading_back`,
  `::ambiguous_fallback_cas_is_resolved_by_reading_back`,
  `::ambiguous_fallback_cas_then_ack_is_recognised_by_self_check`; failpoint:
  `crates/cairn-server/tests/failpoints.rs::stuck_fallback_loses_to_the_coordinator_after_a_ttl`
  (`session::before_fallback_cas`); cost:
  `crates/cairn-server/tests/idle_cost.rs::idle_three_node_cluster_stays_under_per_source_caps`
  (only the coordinator PUTs renewals). Model M1: `crates/cairn-model/tests/m1.rs::delegation_holds_*`
  (holds), `::ack_before_delegation_confirmed_breaks_ls3`, `::ack_after_own_lose_breaks_ls3`,
  `::extending_delegated_write_is_safe_with_yield_restart` (not renewing on our own CASes is
  redundant), `::self_check_gate_is_liveness_only` (the self-check is liveness only).
- **LS-15. The coordinator: its role is tied to its incarnation, it fences after receiving, it
  never acks a session it has issued an `expired` CAS for, it expires only by CAS If-Match; others
  yield to it** (code: `crates/cairn-server/src/control.rs:Control::answer`,
  `crates/cairn-store/src/session/tracker.rs:delegated_due`). *Ack* (`lease_for`): a batch of
  heartbeats passes a **two-part fence** (`control/coordinator` at exactly the **current**
  incarnation and term of the role, not released, and C's session record still `live` — a GET issued
  **after** receiving the batch; the term in the heartbeat equals the current term), the sender's
  record is `delegated` to exactly C's role at **exactly the version** the sender reports (beat,
  etag — ack rule 4; a heartbeat reporting a version the tracker does not have yet makes C GET it in
  parallel with the fence: `SessionTracker::confirm`), C has **never issued** an `expired` CAS for
  that session, ambiguous ones included (`Seen::expire_issued`, ack rule 5; tripwire
  `coordinator::ack_after_expire`), and the sender's bucket-touch age is < TTL/3 (ack rule 6, the
  self-check). C's own `alive()` is not demanded (ack rule 1 is merged into the fence). *The gate*:
  `Grant` only when `control/quiet-lease` is on **with `delegation=on`** (written by whoever builds a
  new bucket — LS-16a; or `cairn-ctl cluster enable-quiet-lease --delegation` — without that flag
  the gate only enables the lone-node mode; the command refuses while `evidence=` lacks any of the
  five items (a)–(e) recorded with `cluster record-evidence`, unless the override flag
  `--accept-missing-evidence` is given, which leaves a `delegation_unproven=` mark; test
  `ctl.rs::delegation_refuses_without_evidence_and_writes_nothing`; nodes do not read the evidence)
  and every session has `knows ≥ migrate::QUIET_LEASE`; a session with a low or missing `knows` ⇒
  `Fallback` for every heartbeat. *τ(N)* = `max(Seen::since, Seen::heard)` — when the newest version
  was received, or when an **acked** heartbeat was received (a heartbeat that was not acked renews
  nothing; counting it would mean a node cut off from the bucket that still sends heartbeats is
  never expired). *Expire:* a record `delegated` to exactly `(C, the term C holds)`, τ one TTL old, C
  not "weak" (grant suspended or slowness score at the cap; deferral is bounded by one TTL, past
  which it releases the role) ⇒ an `expired` CAS If-Match the version just read (failed GET: the
  version held — `expire_held`); a miss ⇒ read again, the beat grew ⇒ a new τ; torn ⇒ `rearm`
  **keeping the `mode` line**. C's tracker does not GET sessions delegated to it while heartbeats
  keep arriving regularly and report exactly the beat already read.
  **The yield rule**: an observer other than the coordinator of the `X:t` role only expires a
  `delegated coord=X:t` record once ≥ TTL has passed since **it** saw that role lost (a later term,
  `released`, X's session ended on the bucket) **and** ≥ TTL since it saw this version of the record
  (`since` is reset every time the beat grows — a delegation CAS is a renewal). An undecodable record
  in a cluster with a coordinator: only C expires it, once C's fence has passed **after** it saw the
  breakage, with τ ≥ when C held the role. *The role is sticky:* it does not move to a new
  incarnation; it is released on a graceful shutdown or when weak for more than one TTL; it is not
  taken back within `RETAKE_AFTER_TTLS`.
  *C's passive renewal*: C enters LS-16's `solo` — verbatim, no new line — when the gate has the
  delegation flag, C holds the role under its current incarnation, every other unended readable
  session is `delegated coord=C:<term>` (`SessionTracker::all_delegated_to`) and C is not draining
  (`renew.rs:quiet_coordinator`); it leaves when any condition is lost. In `solo`, C's fence also
  demands that its renewal loop has been awake within one TTL (`Session::keeper_alive_if_solo`):
  only that loop answers challenges, and nodes delegating to C defer their challenges as long as
  they are acked — a dead renewal loop with C still acking would leave C's ranges stuck. C's tracker
  still LISTs `sessions/` every TTL in `solo` (the run-time rule that forces a fallback for old
  binaries relies on it). **Deferring challenges:** the tracker does not challenge the `solo`
  record of a session our own record is delegating to (confirmed — `TrackerInner::leaning_on`);
  falling back to `direct` ends the deferral. Safety is unchanged: C's ranges follow LS-16, the
  delegated nodes' ranges follow the yield rule — how C's session ends does not enter the argument;
  the two rules above only decide when there are PUTs and challenges.
  Tests: `crates/cairn-server/tests/delegation.rs::gate_without_delegation_flag_keeps_the_cluster_direct`,
  `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_checks_knows_then_raises_min_binary_and_opens_the_gate`
  (the `--delegation` flag), `crates/cairn-server/tests/delegation.rs::coordinator_killed_ranges_move_within_two_ttl`,
  `::coordinator_and_node_die_together_ranges_move_within_three_ttl`,
  `::coordinator_reopening_incarnation_drops_the_role` (the role is sticky to the incarnation),
  `::coordinator_renewal_pause_does_not_fallback_the_cluster` (ack rule 1 merged into the fence),
  `::slow_coordinator_releases_and_is_not_retaken_at_once` (sticky role),
  `::old_binary_session_forces_cluster_fallback` (the run-time rule; also when C is in `solo`: a
  LIST every TTL),
  `::coordinator_sends_no_session_put_while_every_peer_delegates` (passive renewal, deferred
  challenges),
  `::coordinator_leaves_solo_for_a_joining_node_and_returns_after_it_delegates`,
  `::solo_coordinator_frozen_ranges_move_after_challenge_within_two_ttl`,
  `::solo_coordinator_with_a_dead_keeper_stops_acking_and_loses_its_ranges`,
  `crates/cairn-server/tests/cluster_lease.rs::old_owner_steps_down_at_once_when_the_claimant_tells_it`
  (C's renewal loop dead while its control loop lives),
  `crates/cairn-server/tests/net_partition.rs::coordinator_cut_from_all_peers_expires_nobody`,
  `::expired_node_waking_gets_no_ack_and_loses` (no ack after an `expired` CAS),
  `::delegated_dead_node_is_expired_when_the_coordinator_cannot_read_it`,
  `crates/cairn-server/tests/coordinator.rs::coordinator_ignores_heartbeat_of_an_old_term` (the
  two-part fence),
  `crates/cairn-store/src/session/tests.rs::delegation::delegating_to_a_lost_coordinator_is_not_expired_early`
  (the yield rule),
  `::ambiguous_expire_cas_blocks_further_acks` (no ack after an `expired` CAS),
  `::torn_delegated_record_is_rearmed_not_wedged`, `::undecodable_record_is_not_expired_by_a_direct_observer`
  (torn and undecodable records); failpoint:
  `crates/cairn-server/tests/failpoints.rs::zombie_coordinator_ack_never_extends_an_expired_grant`
  (`coordinator::before_ack`, tripwire `coordinator::ack_after_expire`). Model M1:
  `crates/cairn-model/tests/m1.rs::delegation_holds_*`, with one row per fence breaking LS-3 —
  `::ack_after_expire_breaks_ls3`, `::blind_expire_breaks_ls3`,
  `::zombie_coordinator_without_fence_breaks_ls3`,
  `::reopened_coordinator_without_incarnation_check_breaks_ls3`, `::ack_for_an_old_term_breaks_ls3`,
  `::fallback_observer_expiring_delegated_breaks_ls3`, `::old_observer_without_gate_breaks_ls3`,
  `::delegating_to_a_lost_coordinator_breaks_ls3_with_literal_yield` (the yield rule). The
  coordinator as a `solo` node (`M1::hub_node`): `::coordinator_solo_holds_with_all_fences` (holds;
  with every fault, (2, 2): `::coordinator_solo_holds_with_all_faults`, `--ignored`), breaking rows
  `::coordinator_solo_extend_on_challenged_read_breaks_ls3`,
  `::coordinator_solo_without_fence_breaks_ls3`,
  `::coordinator_solo_observer_not_deferring_breaks_ls3`.

- **LS-16. A lone node renews by an unchallenged GET; a newcomer challenges through the bucket,
  then waits one TTL** (model M1 `M1::solo`). *Entering the mode:* the `control/quiet-lease` gate
  is on (LS-16a below) **and** we are the only unended session according to the tracker after a
  LIST since the incarnation opened (`SessionTracker::alone`) — or we are the coordinator every
  other session delegates to (LS-15) ⇒ the next renewal CASes `mode=solo`, `beat+1` (an LS-2
  renewal); only once that CAS is **confirmed** (won, or ambiguous and the re-read finds exactly our
  `solo` copy, with no `challenged` line) does the renewal loop stop PUTting (`Current::solo`,
  `renew.rs:adopt`). *Renewal:* every `renew_interval` one GET `If-None-Match` of our own record
  (`renew.rs:self_check`; merged with the read fence loop — `fence.rs:confirm` → `solo_extend`),
  issued at `t`: a `304` against the etag of the **confirmed** `solo` copy (not the etag from before
  the CAS that entered the mode) or a `200` with exactly the content we last wrote ⇒ deadline
  `t + ⅔·TTL`. A failed GET ⇒ no renewal (suspended, then lost at the cap as in LS-2). After `lose`
  the grant is revoked for good — no GET revives it. *Challenge:* the tracker of a newcomer (a joining
  node, a new incarnation, a second process) meeting an unended `solo` record of another session
  CASes it If-Match with the verbatim content + `challenged=<us>` (`SessionTracker::challenge`,
  `cas::put_resolving`) — unless our own record is delegating to that very session (deferred
  challenges — LS-15); τ = when the confirmed answer was received — or the first read that finds a
  new `solo` version (the `challenged` line changed). At τ + TTL with the beat not grown ⇒ an
  `expired` CAS If-Match, then `claim` as usual. A missed challenge CAS ⇒ read again next round (a
  grown beat = the owner is alive). *Answer:* the self-check GET finding a `200` different from what
  we wrote (a `challenged` line, an overwrite, undecodable) — no renewal — ⇒ a CAS If-Match on the
  new etag back to `direct`, `beat+1`, dropping the line (`renew.rs:answer`): winning it is an LS-2
  renewal; the named challenger goes into the tracker (`SessionTracker::learn`), so we do not enter
  `solo` again while it lives. Our own CAS (keeping `solo` because `owns` changed, or leaving the
  mode) missing because a challenge landed before it is an answer too, not a `lose`: the re-read
  finds our `live` record, the old beat, carrying a `challenged` line ⇒ CAS back to `direct` from the
  version just read (`renew.rs:renew_once`, the `Contested` branch; same failpoint
  `session::before_answer_challenge`) — found by a chaos run. *Leaving the mode* (one CAS back to
  `direct`) when another session is known or the gate is off; `owns`/`draining` changing while alone
  ⇒ one CAS that keeps `solo`. While alone the tracker only LISTs `sessions/` every
  `quiet_list_every` (10 minutes) as a safety net (a coordinator in `solo` still LISTs every TTL —
  LS-15); `owner_watch` keeps the GET safety net (1 minute). A torn `solo` record (the challenge or
  `expired` CAS keeps missing with the beat not growing, or it cannot be decoded) ⇒ overwrite it
  **with** our `challenged` line, then continue as a challenge (a bare overwrite would give back
  exactly the content the owner wrote, and the owner's self-check GET would renew from it).
  Tests (`crates/cairn-store/src/session/tests.rs::solo::…`): `solo_node_sends_no_session_put_while_alone`,
  `solo_node_sends_no_session_list_while_alone`, `restarted_incarnation_takes_over_after_challenge_and_ttl`,
  `second_process_same_node_challenges_and_both_go_direct_without_pingpong`,
  `joining_node_challenges_and_cluster_forms`, `frozen_solo_node_ranges_move_after_ttl_not_before`,
  `solo_node_cut_from_bucket_pauses_then_loses`, `challenge_racing_owns_write_is_retried`,
  `owns_write_racing_challenge_answers_instead_of_losing`; rule 7:
  `ambiguous_enter_solo_cas_is_resolved_by_reading_back`, `ambiguous_challenge_cas_resolves_by_reading_back`,
  `ambiguous_answer_cas_is_resolved_by_reading_back`. Cluster:
  `crates/cairn-server/tests/cluster_lease.rs::solo_node_joined_by_another_forms_a_cluster_without_losing_a_range`,
  `crates/cairn-server/tests/cluster_lease.rs::frozen_solo_node_ranges_move_after_challenge_and_ttl`
  (both in the `each-once` row); failpoints:
  `crates/cairn-server/tests/failpoints.rs::solo_owner_stuck_before_answering_loses_to_the_challenger`
  (`session::before_answer_challenge`),
  `crates/cairn-server/tests/failpoints.rs::challenger_counts_ttl_from_its_confirmed_cas_not_its_read`
  (`session::before_challenge_cas`); cost:
  `crates/cairn-server/tests/idle_cost.rs::idle_single_node_stays_under_per_source_caps`
  (0 `session.renew` PUTs, 0 `session.tracker` LISTs). Model M1:
  `crates/cairn-model/tests/m1.rs::solo_holds_with_all_fences` (holds), and one row per fence
  breaking LS-3 — `::solo_extend_on_challenged_read_breaks_ls3`, `::expire_solo_without_challenge_breaks_ls3`,
  `::old_observer_vs_solo_breaks_ls3`.
  **LS-16a. The gate.** No node enters `solo` while `control/quiet-lease` is not on. There are two
  ways to turn it on. *(1) A new bucket (the default, `--quiet-lease delegation`):* the cluster-level
  runner **creates** `cluster-version` on a truly empty bucket (`migrate.rs:fresh_bucket`) with
  `min_binary ≥ migrate::QUIET_LEASE`, then under the `upgrading` lock writes the gate `state=on` +
  `delegation=on` + the mark `delegation_unproven=` (all five items, `by=bootstrap`) with a `Create`
  (rule 7) if the gate is still missing (`migrate.rs:bootstrap_fresh`); on `s3://` the G18 probe
  must pass first, and a failure ⇒ nothing is written. No `knows` check, no `BOOT_WAIT`: an empty
  bucket has no binary yet, and `min_binary` is on `cluster-version` before the gate exists. A
  bucket that already holds a cluster is never changed; `--quiet-lease off|solo` chooses otherwise
  at bootstrap. Tests:
  `crates/cairn-store/tests/migrate.rs::fresh_bucket_gets_the_quiet_lease_gate_with_delegation`,
  `::existing_bucket_keeps_its_quiet_lease_state`,
  `::concurrent_bootstrap_of_one_empty_bucket_gives_one_gate`,
  `::ambiguous_gate_create_resolves_by_reading_back`,
  `::bootstrap_skips_the_gate_on_a_store_that_serves_a_stale_304`,
  `::takeover_of_a_dead_bootstrapper_still_writes_the_gate`,
  `crates/cairn-server/tests/upgrade.rs::fresh_bucket_boots_with_the_delegation_gate_and_a_lone_node_goes_solo`,
  `::data_dir_node_on_an_empty_directory_goes_solo`. *(2) An existing bucket:* `cairn-ctl cluster
  enable-quiet-lease --apply`: the command **probes the store** first (the G18 conditional-GET probe
  of `irreversible.md` §0.3, `store_probe::conditional_get` on `control/probe/<nonce>`, two clients
  — a store serving a stale `304` or a stale read ⇒ refuse, write nothing: LS-16's self-check GET
  relies on exactly that), checks that every unended session has `knows ≥ migrate::QUIET_LEASE`,
  CASes `min_binary` up to that number (old binaries — which would apply LS-1 to a `solo` record —
  can no longer start), waits `ctl::BOOT_WAIT` (a node's boot bound: an old binary that passed the
  `min_binary` check before the CAS and is still booting has either opened its session or stopped
  by then), checks again, and then writes `control/quiet-lease` = `on`. The gate step is a
  `Run::Gate` entry of the migration registry: the cluster-level runner skips it, so booting a new
  binary does not raise `version`, and downgrading is possible — `disable-quiet-lease`, wait for
  every session to go back to `direct` (`cluster status`), then `lower-min-binary`. Nodes read the
  gate the first time in each incarnation and every `quiet_lease_poll` (60 s) while alone; a failed
  read ≡ off. Tests:
  `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_checks_knows_then_raises_min_binary_and_opens_the_gate`,
  `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_waits_out_the_boot_bound_before_the_last_check`,
  `crates/cairn-server/tests/ctl.rs::quiet_lease_downgrade_needs_gate_off_and_every_session_direct`,
  `crates/cairn-server/tests/ctl.rs::enable_quiet_lease_refuses_a_store_that_serves_a_stale_304`,
  `crates/cairn-store/tests/library_contracts.rs::g18_conditional_get_never_serves_a_stale_304`,
  `crates/cairn-server/tests/upgrade.rs::old_binary_cannot_start_when_quiet_lease_enabled`,
  `crates/cairn-store/tests/migrate.rs::gate_steps_are_skipped_by_the_cluster_runner`.

## 2. Files to read, in order

1. `crates/cairn-store/src/owner.rs`: the owner record, `WalKind`, `SessionId`. Short, and the
   source of truth.
2. `crates/cairn-store/src/session/`: the header doc of `mod.rs` (why it is correct), `claim.rs`
   (`Session::establish`, `claim_with`), `renew.rs` (`keeper_loop`/`renew_once`,
   `owner_watch_loop`), `keeper.rs` (`LeaseKeeper`: `release`, `retire`, `forget`, `Drop`),
   `tracker.rs` (`SessionTracker`), `membership.rs` (`Membership`: membership hints for routing,
   balancing, slow-shed, registry notices, graceful shutdown — the file's header doc says why "dead"
   does not go through it), `coordinator.rs` (the coordinator role: taking it, stickiness, the
   two-part fence, releasing it; the schedule lives in `crates/cairn-server/src/control.rs`).
3. `crates/cairn-store/src/writer/`: `validity.rs` (`LeadershipGrant`, `Validity`); `mod.rs`
   (`Committed`, `Pending::durable`, `alive`, `PartitionWriter::{open, commit, durability, seal}`).
4. `crates/cairn-server/src/lease.rs`: `Lease`, `Released`, `Locks`, the four rules and the table of
   engine release points.
5. `crates/cairn-server/src/node/`: `start.rs::start` and its phases in order (`setup.rs`:
   `pass_gates` — the D12 gate, `bind_listeners`, `lease_setup`, `Session::establish` before the
   engines, `migrate_cluster`, `spawn_engines`), `spawner.rs::RangeSpawner::factory` (claims the
   lease **before** `Db::open`; `open_io` — LS-12), `running.rs::RunningNode::shutdown`.
6. `crates/cairn-server/src/actor/`: `leadership.rs` (`leadership_loop`, `step_down`, `quiesced`,
   `reject_until_leader`), `serve.rs` (`serve`, the `select!` branches), `dispatch.rs`
   (`on_command` — the D23 check, then one `on_*` function per command), `background.rs`
   (`restore_flow`, `recover`).
7. `crates/cairn-server/src/balance.rs`: `Balancer::acquire`, `spawn_release_loop`. Then
   `crates/cairn-store/src/health.rs` (`SlowMeter`, `SlowScore`, `ShedMarker`) and
   `crates/cairn-server/src/slow.rs` (`Decider`, the slowness score loop).
8. The D10, D20, LS and INV-4 entries of [`docs/design/decisions.md`](../design/decisions.md), and
   [`docs/architecture.md`](../architecture.md), for the original reasons behind the two fence
   layers.

## 3. Impact matrix

| Change | Then look at / run |
|---|---|
| `session/` (`record.rs`), `owner.rs` (object layout) | AGENTS.md rule 2: do not bump `DATA_FORMAT`. `cargo test -p cairn-server --test format_fixture`, `cargo test -p cairn-store --lib session::` |
| Grant validity (`Validity`, `extend_until`, TTL, renewal tick) | LS-2, LS-6. `cargo test -p cairn-store --lib` + `cargo test -p cairn-server --test cluster_lease --test failover --test liveness` |
| The observation tick (`observe_interval`, the `SessionTracker` loop), the sources of `Membership` | LS-1 (time to detect a dead node). `cargo test -p cairn-server --test read_cost --test failover --test liveness --test cluster_lease` + `sim_cluster` (changing the tick changes the trace) |
| Lone-node mode (`renew.rs:self_check`/`answer`/`solo_extend`/`want_solo`/`quiet_coordinator`, `tracker.rs:challenge`/`alone`/`all_delegated_to`/`lean_on`, `coordinator.rs:keeper_alive_if_solo`, the `mode=`/`challenged=` lines), the `control/quiet-lease` gate, `Run::Gate` | LS-1, LS-15, LS-16. `cargo test -p cairn-store --lib session::` + `cargo test -p cairn-server --test cluster_lease --test delegation --test idle_cost --test ctl --test upgrade` + `--features failpoints --test failpoints` + `CAIRNFLOW_TEST_FAULTS=each-once … --test cluster_lease` + `cargo test -p cairn-model --test m1` (mapping table `EnterSolo`/`SoloCheck`/`SoloWrite`/`Challenge`/`AnswerChallenge`/`ExpireChallenged`) + `sim_cluster` (the `quiet` swarm dimension) |
| Delegated mode (`delegate.rs`, `renew.rs:delegated_round`/`adopt`, the `mode=delegated`/`coord=` lines), the coordinator (`control.rs:answer`/`lease_for`, `tracker.rs:delegated_due`/`lease_view`/`expire_held`, `coordinator.rs` → `CoordKnowledge`) | LS-1, LS-2, LS-14, LS-15. `cargo test -p cairn-store --lib session::` + `cargo test -p cairn-server --test delegation --test coordinator --test idle_cost --test read_cost` + `--features testkit --test net_partition` + `--features failpoints --test failpoints` + `CAIRNFLOW_TEST_FAULTS=each-once … --test delegation` + `cargo test -p cairn-model --test m1` (mapping table `Delegate`/`HeartbeatAck`/`Fallback`/`SelfCheck`/`RecordWrite`/`CoordinatorExpire`/`CoordinatorRead`; when changing M1, also `-- --ignored`) + `sim_cluster` (the `quiet` swarm dimension) |
| `claim_with`, `session_is_dead` | LS-3, LS-8. See `split/steps.rs::claim_children` and `gather_here` (which call `claim_from_others`). `cargo test -p cairn-server --features testkit --test net_partition` + `cargo test -p cairn-server --test quiesce --test balance_leadership` |
| `lease.rs` | `docs/guides/flow.md` (D16). `cargo test -p cairn-server --test transport_engine --test concurrency --test durability` |
| `actor/` (`serve`, `step_down`, `quiesced`) | `cargo test --workspace` + 4-partition cluster e2e, per AGENTS.md §2 |
| `node/` (`spawner.rs::factory`, `running.rs::shutdown`) | The order claim → reload the table → roll back intents → migrate → open the Db. `cargo test -p cairn-server --test range_table --test upgrade` + `--test graceful_shutdown -- --include-ignored` on a real S3 |
| `balance.rs` | `cargo test -p cairn-server --lib balance::` + `--test cluster_lease --test balance_leadership --test slow_shed` |
| `health.rs`, `slow.rs`, the `slow=`/`p99_put_ms=`/`draining=` fields | LS-11. `cargo test -p cairn-store --lib health::` + `cargo test -p cairn-server --lib slow:: --test slow_shed --test format_fixture` |
| The WAL part of `LeaseKeeper` (`writer_sealed`, `pending`) | `docs/guides/nodewal.md`. `cargo test -p cairn-store --test shared_wal --test shared_wal_sim` |
| The lease protocol or the Db open path (`claim_with`, `session_is_dead`, `RangeSpawner::factory`, grant validity), or the `crates/cairn-model/` models | LS-2/3/4/8/12, SM-3/13: fix model M1 (`crates/cairn-model/src/m1.rs`, mapping table in the file's header doc) to match, then `cargo test -p cairn-model` (when changing M1, also `-- --ignored`) |
| `failpoint!` points (`session::claim`, `actor::step_down`, `actor::recover`, `actor::recover::scan`, `actor::restore_flow`, `lease::*`) | `cargo test -p cairn-store -p cairn-server --features failpoints --test failpoints` |

## 4. Review checklist

- [ ] No path opens a `PartitionWriter` except through a `LeadershipGrant` issued by `claim` (or
  `single_node`).
- [ ] Claiming the lease still comes **before** `Db::open`. Reversing it means every node that
  starts fences a healthy leader.
- [ ] Every new external effect demands `&Committed`. Tickets come from `Lease::sync`, never from
  `wait_durable` while holding the engine.
- [ ] No new branch in `serve` swallows `Fenced` or a `CloseReason` and keeps serving.
- [ ] Commands that answer "closed" (`Shutdown`, `Resign`, `Quiesce`, `Retire`) still go through
  `AfterSeal`, answering after the seal.
- [ ] `owner_watch_loop` still lives outside `tasks`, and does not keep `Inner` alive while waiting.
- [ ] A new claim path (other than `claim_with`) that may take the range of another live session
  calls the `on_owner_taken` listener as `claim_with` does (the notice to the old owner).
- [ ] A new answer at the leader goes through `Shared::finish` (the read fence). `finish_unexposed`
  only for answers nobody sees or that only assert durable history (LS-5) — with the reason written
  in place.
- [ ] No reuse of evidence (a fence GET, a renewal) issued **before** the answer's read.
- [ ] Code outside the lease layer reads membership through `Session::membership()`, not
  `Session::tracker()`; no irreversible decision (claim, delete, tombstone) is based on
  `Membership`.
- [ ] No clock comparison between two machines. Only local `Instant`s are measured.
- [ ] `solo` mode (LS-16): a renewal by GET only when compared with the etag of the **confirmed**
  `solo` copy (`Current::solo`), and only for the current incarnation (`solo_extend`). Every new
  self-write bumps `beat+1`. A new observer never expires a non-`direct` record by the silence rule.
- [ ] A coordinator in `solo`: no acks while its renewal loop has not been awake for more than one
  TTL; its tracker still LISTs every TTL; the deferred-challenge rule only applies to sessions our
  own record has **confirmed** delegating to. No new path relies on "the coordinator is alive" to
  expire or claim — still only challenge + TTL (LS-16).
- [ ] Delegated mode (LS-14/LS-15): an ack only moves the deadline of the current incarnation that
  has a **confirmed** delegation to exactly that role; no path moves the deadline of a revoked root;
  the coordinator never acks a session it has issued an `expired` CAS for; τ only counts acked
  heartbeats; observers that are not the role's coordinator never apply LS-1 to a `delegated` record
  (the yield rule, recounting when the beat grows).
- [ ] An owner-record write with an unclear outcome is **read back** (`write_owner` through
  `cas::put_resolving`, then `write_record`), never treated as missed — `Precondition` included.
- [ ] No new lease-release path drops `pending_release`: a failed release must be retried by the
  session in the background. Any place that removes an entry from `pending_release` after an I/O
  wait compares the current entry with the copy it took (`forget_pending_release`), never deleting
  by range.
- [ ] Changing an LS-x invariant means fixing this guide in the same commit.

## 5. Common failure modes

- **A frozen node still believes it is the leader.** Layer 1 is pinned (`CrashHandle`), layer 2
  only fires when it writes. Before the per-node layer 3 existed, the detection bound was each Db's
  manifest poll tick (60 s). Now it is a few TTLs. Reproduced in
  `failover.rs::frozen_leader_other_node_takes_over_with_data_intact`.
- **The store dies underneath while the lease keeps renewing.** A WAL flush error past its retry
  count is not `Fenced`, so ranges used to get stuck as leaders that could write nothing. The fix is
  the `health.closed()` branches and the `durable_seq` watchdog (`liveness.rs`).
- **A hung seal makes the node silent.** Stepping down without reading the command channel leaves
  clients not knowing where to go. It is bounded (`actor::seal_timeout`); past the bound the writer
  is dropped, and the next leader fences it through `writer_epoch`. With the shared WAL that bound
  is derived from the log stream's `stall_max` (NW-8 in `nodewal.md`): a shorter bound turns a burst
  of slow PUTs into a dirty yield and then a fail-stop.
- **Quiesce believes it lost the lease.** A seal used to lower the lease's validity too, so the
  quiesce loop took the range back and reopened the parent Db while the coordinator was cloning. The
  fix is the writer's own validity (`Validity::child`). Test:
  `quiesce.rs::quiesced_range_keeps_lease_and_stays_closed`. A **real** lease loss during quiesce
  (the session lost) still makes the engine take the range back, but `factory` waits for the node's
  coordinator to finish instead of opening the Db (SM-13 in `split-merge.md`).
- **Torn sessions on MinIO.** A client `kill -9`ed in the middle of a CAS leaves an object whose GET
  flips etags back and forth and on which conditional PUTs always miss. There is the
  `UNEXPIRABLE_AFTER_TTLS` threshold; past it the observer **overwrites** the record and only then
  marks it as usual (LS-1) — before, it considered the node dead by its clock alone, and a node still
  alive that the observer read stale lost its ranges (two leaders). A failed overwrite only raises a
  warning (`CairnflowSessionUnexpirable`). Tests:
  `session/tests.rs::torn_session_of_dead_node_is_rearmed_then_expired`,
  `session/tests.rs::torn_session_that_cannot_be_rearmed_is_never_declared_dead`,
  `session/tests.rs::torn_view_of_a_live_owner_never_leads_to_claim`,
  `cluster_lease.rs::torn_session_of_dead_node_does_not_strand_partitions`. Chaos runs on SeaweedFS
  because of this (AGENTS.md §5).
- **Reusing `inc` after the session was cleaned up.** Found by a shared-WAL soak: a new log had the
  same name as an old one, leading to lost acked rows. The first fix was `inc ≥ unix millis` — still
  open when the clock goes backwards; now the lower bound is the `incarnations/<node>/<inc>` marker
  (LS-1).
  Tests: `crates/cairn-store/tests/shared_wal.rs::restarted_node_never_reuses_incarnation_after_session_gc`,
  `crates/cairn-store/src/session/tests.rs::restarted_node_never_reuses_incarnation_after_session_gc`.
- **An ownership-change storm from a node dying and coming back.** Every return means two owner
  changes and one re-dispatch of every unfinished invocation. Measured under a billing workload: 326
  leader elections in 10 minutes. The fix is `settle_for`.
- **A slow node loses its session regularly.** The bound on each renewal used to equal the tick
  (TTL/5): a node with PUTs slower than the tick (a `--slow-node` chaos run, +1.4 s, TTL 6 s)
  cancelled every renewal before it wrote and lost its session every ~5.5 s. Now the bound is the
  remaining deadline, plus a parallel attempt every tick. Test:
  `session/tests.rs::renewal_slower_than_pace_keeps_session`.
- **A failed release orphans a range.** A release write past `RELEASE_WRITE_BOUND` is handed to the
  session to retry in the background. Test: `cluster_lease.rs::failed_release_does_not_strand_partition`.
  A background attempt can be in flight for up to a TTL; meanwhile the node may claim the range
  again itself and fail to release it in a new term — a late old attempt only removes the pending
  entry if the entry is still exactly the copy it took (`session`, `term`, `version`); otherwise it
  would delete the newer release and leave the owner record pointing to a live session forever:
  `crates/cairn-store/src/session/tests.rs::stale_release_retry_keeps_the_newer_pending_release`.
- **A grey node holds its ranges forever.** The node still renews in time but every PUT takes a
  whole second: the lease never expires, and all its ranges suffer that latency. The fix is LS-11.
  Shedding wrongly causes cascading owner changes — there are three safety nets: the
  one-node-at-a-time marker, the comparison with the median, and the recovery time.
- **Ranges without a leader under prolonged 503s** (`s3_slowdown`). The engine open dropped the whole
  keeper at the first failed GET and claimed again after 0.5 s: the term climbed into the hundreds,
  two PUTs per round. See LS-12.
- **SIGTERM waits out the TTL.** B used to wait 20 s. Now `shutdown` seals in parallel, closes the
  session and then tells its peers. The test `graceful_shutdown.rs::sigterm_hands_ranges_over_before_ttl`
  needs a real S3 store, so it is `#[ignore]`; the "real S3" row of `scripts/check.sh` runs it (it
  finds a local SeaweedFS by itself), and without a store that row records `SKIP`.
  **A red run under load:** the last engine exited and dropped the last `NodeWals` copy **before**
  `session.close()`; `Drop` stopped the WAL, and the fail-stop watch task (running on the engine
  thread's runtime, still alive in the binary) called `Session::fail_stop` ⇒ `lose` ⇒ the renewal
  loop opened a new incarnation while `close()` was cancelling it. The `Create` PUT reached the store
  but `current` was never written ⇒ a ghost `live` session, and B believed A was alive until the TTL
  ran out. Every graceful shutdown goes through `lose` (the log line "session lost — dropping every
  partition" appears in green runs too); it is red only when it hits the cancellation window. Fixed
  in `NodeWals` (NW-2): dropping the handle does not fail-stop. Inside a test process the engine
  thread shuts down together with its runtime and the watch task is cancelled with it — the
  deterministic test lives in `shared_wal.rs`.
