# Guide: flow control, turn scheduling and the engine-release rules

*This is a map, and a hypothesis: it drifts faster than the code. Check in the code before
trusting it. Search by function name; the guide deliberately records no line numbers.*

Everything here lives **on the engine thread** of one range. A scope decides the partition (D-F1),
so a scope's counter is a `u32` in the RAM of exactly one process, and acquiring a permit needs no
network step. There are four resources: the EXCLUSIVE lock of a virtual object (held across a
suspension), three-tier concurrency counters, rate tokens (CU, per tenant or per counter), and the
turn cap (counters and the turn cap are released on suspension; tokens are not "released" — they
come back with time). Decisions live in the synchronous core `flow::Core`: `ResourceManager` plus
the queues, the eligible ring and a `WaitList` per resource; every event takes `now` and returns
`Actions` (permits just granted, a rate deadline moved earlier, room on the node) — no `Waker`,
channel or tokio inside. `flow::Scheduler` is the async shell around `Rc<RefCell<Core>>`: it hands
permits to the requester's `oneshot`, wakes `rate_loop`, tells the node. The tokio-free sim
(`flow/sim/`) applies random event sequences straight to the core and checks FL-3/4/5/6/7/9/13
after every step. The turn cap belongs to the node (`engine_pool::TurnBudget`, D-c). The rule above
all others: **no network call while holding the engine** (D16).

## 1. Invariants

- **FL-1. No network call while holding the engine** (D16). Every function that touches the
  network demands `&Released`, and only two places can create one: `Lease::released` (inside the
  I/O closure) and `IoSet::spawn` (for a task of its own on the engine thread, holding no `Lease`).
  A violation is a compile error. Waiting for something that is not the network (a permit, the
  invocation lock, durability, an `IoSet` result through `Lease::next_io`) goes through
  `Lease::wait`/`Lease::sync`.
  **The outbox drainer sends per destination**: each round of 32 rows is a gathering phase that
  holds the engine, then **one send ticket** (`Lease::sync_sends`, LF-1) for the whole round, then
  the sendable rows are handed to the `IoSet` send task — one sequential queue per destination
  range, at most `MAX_DEST_CONCURRENCY` (8) destinations in parallel. The drainer does not wait on
  the network: it gathers the next round while messages are in flight, and applies the post-ack
  work (delivering the id, deleting the row) of whichever destination answers first, in send order
  within the destination, once it has the engine back. An outgoing `Call` row keeps the **caller
  lock** until it has been applied (at most one send round longer): the caller's `/cancel` needs
  exactly that lock, so `Cancel(c)` cannot be produced before the caller has the id of `c`. Several
  rows of the same caller borrow one shared lock (`turn/outbox.rs::Held`).
  **Batches per destination**: each send task of a destination receives **one batch** — the head
  of that destination's queue up to the limit (≤ 64 messages, ≤ 1 MiB) — and sends it with
  `Transport::send_batch`: one `POST /internal/inbound/batch` request, **one** commit on the
  receiving side, `200` only after that commit is durable. The next batch to the same destination
  is handed over only once the previous task has returned (and been applied), so an acked batch
  does not wait for the next one. The send runs in an `IoSet` task (engine released); the engine
  is taken back only to apply the result. The receiver holds **its** engine for exactly one commit
  for the whole batch, as for a single message. Signals (`drain_signals`) go through the same
  `Sender`: one ticket per round instead of per row.
  Batch tests: `crates/cairn-server/tests/interrange.rs::remote_rows_of_one_round_go_in_one_batch_per_destination`,
  `crates/cairn-server/tests/interrange.rs::signals_of_one_round_go_in_one_batch`,
  `crates/cairn-server/tests/failpoints.rs::receiver_failing_mid_batch_writes_no_message_of_it`,
  `crates/cairn-server/tests/transport_engine.rs::batches_to_an_old_binary_fall_back_to_single_messages`,
  `crates/cairn-server/tests/dead_destination.rs::oversized_queue_goes_as_consecutive_batches_and_a_failed_one_keeps_its_rows`.
  Tests: `crates/cairn-server/tests/transport_engine.rs::transport_send_does_not_hold_engine`,
  `crates/cairn-server/tests/concurrency.rs::introspection_does_not_queue_behind_a_slow_turn`,
  plus the `compile_fail` doctest on `Released` in `crates/cairn-server/src/lease.rs`;
  per-destination sending: `crates/cairn-server/tests/interrange.rs::remote_rows_of_one_round_share_one_durability_wait`,
  `crates/cairn-server/tests/dead_destination.rs::hung_destination_does_not_hold_other_destinations`,
  `crates/cairn-server/tests/dead_destination.rs::failed_send_keeps_later_rows_to_the_same_destination_in_order`,
  `crates/cairn-server/tests/dead_destination.rs::cancel_reaches_the_callee_after_its_start_call`.
- **FL-2. No external effect before a durable ticket** (INV-5, D11). Sending down to the SDK
  (`awaiting_sdk`) and answering a client demand `&Committed`: the ticket is a **mark** taken when
  asked, the seq of the latest write. It cannot be taken away. `Transport::send`/`send_batch`
  demand `&SendTicket` — the **send ticket**: its mark is the latest write **that has send rows**
  (outbox, signal outbox). A `Committed` converts into a `SendTicket`; the other direction is a
  compile error (LF-2, `compile_fail` doctest on `SendTicket`).
  **IR-1 at the drainer:** outbox rows gathered in a round reach the send task only after the
  round's send ticket, so the other side never sees a row that is not durable — including a
  `Reply` row whose turn is still waiting for durability (no lock anywhere blocks the drainer).
  **LF-1.** Every outbox/signal row handed to the transport has a commit ≤ the mark of the round's
  send ticket, and the ticket is issued only once the mark is durable. The `sends` bit is set in
  `Mutations::put` (the only write path, `keyspace::is_send_key`), `PartitionWriter::commit` keeps
  `latest_send`; debug builds cross-check it against the batch's `seq` index. The drainer does
  **not** wait for the turn-end commit or for other turns' commits. Two-layer tripwire
  (`Sender::check_ir1`, failpoint `turn::outbox::undurable_send`): every build — `durable_seq` is
  not below the mark taken when gathering; debug/`testkit` builds — re-read every row about to be
  sent at the durable level (`get_durable`, G19). A mismatch ⇒ that round is not sent,
  `cairnflow_invariant_violations_total{check="ir1"}`. `PeekOutput` keeps the full ticket.
  **LF-2.** Only `Transport::send`/`send_batch` accept a send ticket.
  **LF-3.** A commit with send rows that returns `Err` or is cancelled after submission (the guard
  in `commit`) sets `send_doubt`: no send ticket is issued until a **barrier** submitted after it
  (`PartitionWriter::send_barrier`, the `seal_barrier` lock) is durable. **The drainer reads at
  the `Memory` level**, so it can *act* on undurable data — delete inbound rows, create callees,
  dispatch turns — in the window where `on_inbound_batch` releases the engine to wait for the
  receiving commit to become durable; what stays true is that every external effect after that
  still demands a ticket, and a lost tail is lost together.
  **A write with a lost handle** (`commit` fails or is cancelled **after** the batch was submitted:
  SlateDB still applies it, `latest` does not take it) is not waited for by any full ticket; every
  place that cancels a `commit` belongs to the end of a term (doc of `PartitionWriter::commit`), and
  `seal` covers it with a barrier. For send tickets, LF-3 above applies.
  Tests: `crates/cairn-store/tests/durability.rs::ticket_waits_for_others_inflight_writes`,
  `crates/cairn-server/tests/durability.rs::status_does_not_reply_before_creation_is_durable`,
  `crates/cairn-server/tests/durability.rs::list_does_not_reply_before_creation_is_durable`;
  IR-1: `crates/cairn-server/tests/failpoints.rs::remote_peer_never_sees_a_row_before_it_is_durable`;
  LF-1: `crates/cairn-server/tests/failpoints.rs::outbox_send_does_not_wait_for_the_turn_end_commit`;
  LF-3: `crates/cairn-server/tests/failpoints.rs::cancelled_commit_with_an_outbox_row_forces_a_barrier`;
  G19: `crates/cairn-store/tests/shared_wal.rs::remote_read_sees_exactly_the_durable_prefix_with_a_shared_wal`;
  send tickets on both `Db` and the model: `crates/cairn-store/tests/range_store.rs::{slate,model}::send_ticket_covers_exactly_the_commits_with_sends`,
  `::durable_reads_see_only_flushed_writes`; a crash keeps a prefix:
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (rows handed over after a send ticket lie in the durable prefix after every crash);
  lost-handle writes: `crates/cairn-store/tests/failpoints.rs::submitted_outbox_batch_with_lost_handle_is_applied_but_not_waited_for`,
  `crates/cairn-server/tests/failpoints.rs::cancelled_outbox_commit_is_not_sent_before_it_is_durable`;
  a drainer acting on undurable messages, then losing the lease: `crates/cairn-server/tests/failpoints.rs::drained_inbound_on_a_lost_leader_runs_once`,
  M4 `SOMETIMES_EARLY_DRAIN`, `LIVE`, `ATTEMPTS`, `ONE_DELIVERY` (`crates/cairn-model/src/m4.rs`:
  undurable tail, receiving messages, `200`).
- **FL-3. Two-phase permits.** `ResourceManager::poll_acquire` has no side effects.
  `Provisional::secure` is the **only** place that increments counters and takes locks. The
  effective cap is the tightest of the three tiers. However many times permits are acquired and
  released, the counters return to 0.
  Tests: `crates/cairn-server/src/flow/tests/mod.rs::no_leak_over_ten_thousand_acquire_release`,
  `crates/cairn-server/src/flow/tests/mod.rs::three_counter_tiers_all_block`,
  `crates/cairn-server/tests/flow_control.rs::scope_concurrency_cap_is_real`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (after every step: counters, cooldown, turn slots = the sum of live permits —
  `check_fl3_accounting`; never over a cap at grant time — `on_grant`; asking `poll_acquire` twice
  changes nothing — `probe`; at the end of the liveness phase every counter is back to 0 —
  `check_quiescent`).
- **FL-4. A VO lock is released only when the invocation ends.** The core's turn-end plan
  (`Plan::ram.release`; shell `turn::{end_turn, end_turn_with, begin_turn}`) has four paths: only a
  terminal one does `release_all`. Suspension, an infrastructure failure, `Paused` and a turn cut
  off midway (`TurnPermit::drop`) all do `release_running`, i.e. keep the lock. When it is unknown
  where a turn stopped, keep the lock, and leave a scheduled wake — if the wake cannot be written,
  mark the rescue net (FL-16). A turn that failed after `Running` first reads the status back
  (rule 7): its finishing commit may have landed and reported an error; a terminal status ⇒ the
  turn ended, `release_all`.
  Tests: `crates/cairn-server/src/flow/tests/mod.rs::suspend_releases_permit_but_keeps_lock`,
  `crates/cairn-lifecycle/src/tests/turn.rs::failed_turn_whose_finish_landed_releases_the_lock`,
  `crates/cairn-server/tests/failpoints.rs::finished_turn_with_a_failed_commit_releases_the_object_lock`,
  `crates/cairn-server/tests/flow_control.rs::turn_that_read_state_then_died_midway_keeps_object_lock`,
  `crates/cairn-server/src/flow/tests/mod.rs::lock_holder_abandoning_just_as_granted_keeps_lock`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (the core's lock book = the model, no two invocations hold the same lock; `give_back` by a
  holder that already held the lock keeps it).
- **FL-5. Service order follows durable `seq`, not arrival order.** The current holder of a
  queue's lock stands at the head of the line; nobody jumps ahead of it. For users this means FIFO
  on one object key. **Order survives a nack**: a granted ticket of an EXCLUSIVE queue whose turn
  failed to win the invocation lock gives the permit back but **keeps the VO lock**
  (`Core::nack`: `release_running`, refunding the 1 CU charged up front) even when that grant is
  what took the lock — the ticket keeps waiting at its `seq` and re-asks at the head of the line
  once the invocation lock is released; giving the lock back would hand the object to a later
  `seq` ahead of it. Tickets waiting at the grant gate (FL-8) enter the core by `(queue, seq)`
  (`Scheduler::ungate`), not by parking order (`recover` dispatches by id).
  Tests: `crates/cairn-server/src/flow/tests/mod.rs::queue_orders_by_seq_not_arrival_order`,
  `crates/cairn-server/src/flow/tests/mod.rs::late_arrival_with_lower_seq_does_not_jump_lock_holder`,
  `crates/cairn-server/tests/flow_control.rs::same_object_key_runs_in_arrival_order`,
  `crates/cairn-server/src/flow/tests/ticket.rs::{a_nacked_exclusive_ticket_keeps_its_object_lock,
  ungate_serves_an_object_in_seq_order}`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (at grant time, no request with a smaller `seq` still waits in the same queue, unless the
  grantee held the lock when it asked; drop the lock-holder rule and the liveness phase wedges),
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_with_nacks_keeps_seq_order`
  (a nacked request of an EXCLUSIVE queue still counts as a waiter at its `seq`).
- **FL-6. Cancelling midway never strands a permit.** `Claim::drop` sweeps a permit already in
  the channel, and leaves no dead entry in the wait queue: an empty channel reports `Core::cancel`
  — the entry becomes dead (`QueueState::dead`), the core never grants to it, and it is cleaned up
  when it reaches the head or when dead entries outnumber live ones (> 32). A queue left with no
  waiters (after the last grant, or after a cleanup that empties it) leaves the table at once. A
  permit that has left the channel but **not yet entered a turn** carries that return path itself
  (`Permit::unstarted`) up to `Permit::begin` (`TurnPermit::new`): this window still has one
  `await` point — `Lease::wait` takes the permit and only then wins the engine back — and a task
  cancelled there (`JoinSet::shutdown` on lost leadership) releases like a `Claim` (`release_all`,
  or `release_running` if it already held the VO lock). From `begin` on, dropping a live permit is
  still a `debug_assert`. When the SDK makes no progress the turn is abandoned at `sdk_timeout`,
  and the permit is returned.
  Tests: `crates/cairn-server/src/flow/tests/mod.rs::abandoned_request_leaves_no_dead_entry`,
  `crates/cairn-server/src/lease.rs::tests::wait_cancelled_while_relocking_engine_returns_the_turn`,
  `crates/cairn-server/tests/concurrency.rs::stalled_sdk_abandons_turn_and_returns_permit`,
  `crates/cairn-server/src/flow/tests/wake.rs::queue_emptied_by_a_grant_leaves_the_table`,
  `crates/cairn-server/src/flow/tests/wake.rs::queue_emptied_by_cancellations_leaves_the_table`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (no grant to a requester that left; dead entries are bounded — `check_fl6_dead`; after every
  event the eligible ring is drained, every queue with a live waiter is blocked on one resource
  **and** has a live entry in that resource's wait list, no empty queue stays behind —
  `check_fl6_structure`; liveness phase: every request is granted within a bounded number of
  rounds).
- **FL-7. The turn cap belongs to the node** (D-c). The even split is the **guaranteed** share: a
  range may borrow while the node has room and no range is starving (`turn_room`) — and the tenant
  at the head of the line is not far ahead of the slowest waiting tenant (FL-13). The turn total is
  the sum of the live cells (`NodeTurns`), not an accumulated counter: two node atomics to which
  each cell adds/subtracts exactly its own share, and a dying cell gives back exactly what it still
  holds — read without a lock. **The starving flag only stands while the range still has a permit
  waiter** (`Core::fed_unless_waiting`, sim seed 1473): when the `Turns` wait list empties — the
  waiter left (`forget_waiter`), was called out (`pop_waiter`), or moved to blocking elsewhere
  after a re-queue (`set_rules`, `set_refused` when a tenant is suspended, `thaw`) — the flag is
  cleared at once; before this it stood until the next grant in the range or the next "node
  changed" message, and with the node not full nobody sends one, so every other range stopped
  borrowing. Tests: `crates/cairn-server/src/flow/tests/rate.rs::starvation_flag_does_not_outlive_its_permit_waiter`,
  `crates/cairn-server/src/flow/sim/mod.rs::seed_1473_starving_flag_outlives_its_waiter`.
  Tests: `crates/cairn-server/tests/split.rs::max_concurrent_turns_is_per_node`,
  `crates/cairn-server/src/flow/tests/mod.rs::busy_range_borrows_idle_node_permits`,
  `crates/cairn-server/src/flow/tests/mod.rs::dropped_range_does_not_keep_node_permits`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (the node's counter = the sum of the cells, the starving flag matches, total ≤ cap except for
  guaranteed shares granted while the node leads too many ranges — `check_fl7_node`; the
  guaranteed share / borrowing rule at the moment of the grant — `check_grant_turns`; a starving
  range must still have a permit waiter, a pending node message, or a full node).
- **FL-8. On a leader change counters return to 0, and VO locks are rebuilt** from the store
  (`restore_flow`) **before the first permit grant** (previously "before the first command": one
  engine hold walked the queue tombstones of the whole history — 1.1–1.8 s at 10⁶ finished
  invocations). The rebuild goes **in chunks** like `recover`: each chunk is one engine hold — at
  most `restore_chunk` (1 024) rows of the `Invocations` index after the cursor, the queue entries
  of the unfinished invocations among them (`Engine::restore_chunk`; the queue index keyed
  `(queue, stage, seq, id)` cannot cut tombstones), core `Event::LeaderUp` per entry, the chunk's
  stage changes in one commit, the RAM locks rebuilt — **in the same hold**; the engine is
  released between two chunks. While rebuilding, the range serves reads, creation (`Inbox`),
  cancellation, the drainers, background ticks and `recover` — but the scheduler's **grant gate**
  is closed (`Scheduler::{close_gate, open_gate}`): requesters wait at the gate **before** entering
  the core, so every decision of the core (the lock holder at the head of the line, `held_lock` of
  `Claim`) sees a complete lock book, and every operation that needs to know the locks (choosing
  who runs) waits for the last chunk; releasing a lock not yet rebuilt does nothing. Parked tickets
  of background turns (FL-30) wait at the same gate (`Tickets::gated`) and enter the core when it
  opens (`Scheduler::open_gate` → `ungate`). **With the gate closed the core grants nothing**
  (`Core::frozen`): `ungate` puts every ticket waiting at the gate into the core while it is still
  frozen, then `Core::thaw` grants once in `seq` order — not one `dispatch` per ticket;
  `restore_flow` opens the gate **after** releasing the engine. Test:
  `crates/cairn-server/src/flow/tests/ticket.rs::a_closed_gate_grants_nothing`. This is correct
  because nothing moves an entry **into** a lock-holding stage without going through a permit
  grant: every entry holding a lock during the rebuild belongs to the previous term; a command
  interleaved in between only changes it in the releasing direction (cancel, withdraw), and a lock
  already rebuilt is released as usual. A creation command during the rebuild needs the queue
  `seq` counter (FL-5): it is seeded from the durable mark `floors::QUEUE_HIGH` (matching marker,
  derived zone rebuilt, or the previous rebuild ran to the end); without one the rebuild runs to
  the end **before** the serving loop, as it did originally. The rebuild lives with the term: a
  lease lost midway means the next term rebuilds from scratch on `Shared` with a new scheduler.
  Failure ⇒ step down, the gate never opens (LS-9). An orphan queue entry of a finished invocation
  does not become a lock — the rebuild does not read entries of finished invocations (every ending
  path withdraws the entry in the same commit); orphan entries in old data are only a leak, healed
  by cancelling again. An admin cancel (`turn::cancel`, core `Event::AdminCancel`) writes the
  `failed` status, drops the retry row and withdraws the queue entry in **one** commit
  (`Engine::cancel`; previously the withdrawal was a second commit, and a crash in between left an
  `Inbox` entry nobody cleaned up), then releases the RAM lock. Cancelling an invocation that has
  already stopped for good (core `Event::Dequeue`) heals orphan entries of old data and releases
  the RAM lock on every exit path, including when the withdrawal commit fails.
  Tests: `crates/cairn-server/tests/flow_control.rs::killing_leader_mid_turn_neither_leaks_nor_wedges`,
  `crates/cairn-server/tests/recovery_storm.rs::restore_rebuilds_locks_in_chunks_while_serving`,
  `crates/cairn-server/tests/failpoints.rs::commands_are_served_but_nothing_is_granted_while_locks_are_rebuilt`,
  `crates/cairn-server/tests/failpoints.rs::cancelling_holders_on_both_sides_of_a_restore_chunk_frees_every_lock`,
  `crates/cairn-server/tests/failpoints.rs::restore_cut_by_a_lost_term_is_redone_by_the_next_term`,
  `crates/cairn-store/src/writer/floors.rs::tests::queue_seq_floor_used_only_when_head_matches`,
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (creation commands interleaved between two chunks, a new `seq` on every live entry),
  M4 `VO_EXCLUSIVE`/`LIVE` with the `RESTORE` configuration (planted flags
  `grant_before_restored`, `restore_lock_after_release`; `BEFORE_W5R` — the old behaviour —
  correct but slow),
  `crates/cairn-server/tests/failpoints.rs::admin_cancel_failing_at_dequeue_frees_the_object`,
  `crates/cairn-server/tests/lifecycle.rs::admin_cancel_dying_between_two_commits_leaves_no_inbox_entry`,
  `crates/cairn-server/src/flow/tests/mod.rs::lock_rebuilt_after_failover_still_blocks`,
  `crates/cairn-server/tests/flow_control.rs::orphan_entry_does_not_become_permanent_lock`.
- **FL-9. Releasing a permit is O(1).** `wake` only records the free room of a counter/turn cap.
  `dispatch` calls the waiters one at a time once the eligible ring is drained. A lock wakes every
  waiter of that lock.
  Tests: `crates/cairn-server/src/flow/tests/wake.rs::releasing_a_turn_does_bounded_work_with_20k_waiting`,
  `crates/cairn-server/src/flow/tests/mod.rs::wakes_by_specific_counter_not_by_rule`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (each step makes ≤ 1 wasted call — a waiter called out of the wait list of a counted resource R
  blocks again at R itself; the number of live waiters of each list = the number of queues blocked
  there).
- **FL-10. After every engine release point, re-check durable state before running a turn**
  (the core's gates: `Event::Drive` before the permit, `Event::PermitGranted` after it; shell
  `turn::write_gate_wake`). An invocation that is already terminal only drops the source row.
  `Paused` writes the completion without running.
  Test: `crates/cairn-server/tests/stale_state.rs::stale_inbound_completion_does_not_rerun_completed_invocation`.
- **FL-11. The L0 throttle sheds only new work.** From the `soft` level of L0 views (5 under a
  cap of 8) the range refuses with a probability, at most 0.9, at exactly the three sources of
  `turn::admit::Source` (one place asks the throttle — `turn/admit.rs`): `Invoke`/`Submit`
  commands at ingress (`ActorError::Overloaded` ⇒ `429 range_overloaded`, **nothing created, no
  id**; an invocation that already exists is answered as usual), `StartCall` in `drain_inbound`
  (the row is already durable and stays; **never** an error at `/internal/inbound`), and a local
  child call in `dispatch_call` (`Ok(None)`, the outbox row stays). Once deferred, every later
  attempt **in the same drain pass** is deferred too — creation order is kept (FL-5). Completions,
  timers, retries, signals and replies are never throttled. The throttle state is a `Cell` on the
  engine thread (`l0::Throttle`); the view count is read from the manifest watch in RAM, no GET.
  Tests: `crates/cairn-server/tests/l0_throttle.rs::l0_throttle_sheds_new_work_but_not_in_flight`,
  `crates/cairn-server/src/l0.rs::tests::deferral_is_sticky_within_a_pass_and_wakes_the_loop`,
  `crates/cairn-server/src/l0.rs::tests::hold_rises_at_once_and_falls_only_after_cooldown`.
- **FL-12. Rate tier: charge 1 CU up front, settle after the turn.** `poll_acquire` checks tokens
  **after** the counters and **before** the turn cap; `secure` charges 1 CU (and the bucket of a
  counter that has a `rate`); the turn end (`turn::end_turn*`) charges the remaining `CU − 1`
  (`stats::take_run_cu`; 0 ⇒ refund 1) **before** releasing. A tenant's bucket belongs to the node
  (`rate::TenantBook`, GCRA on one atomic, no lock on the turn path), split as `rate / number of
  nodes leading ≥ 1 range`. Running out of tokens is "out of budget" like a counter: `429` +
  `Retry-After` = when the bucket turns positive again, and a background turn schedules itself for
  exactly that moment. Nobody releases tokens, so `Scheduler::rate_loop` (a task of the term,
  stopped together with the drainer on quiesce) wakes token waiters.
  Tests: `crates/cairn-server/src/flow/tests/rate.rs::out_of_tokens_blocks_at_rate_tier_and_clock_wakes`,
  `crates/cairn-server/src/flow/tests/rate.rs::scope_rate_one_bucket_per_tenant`,
  `crates/cairn-server/src/rate.rs::tests::bucket_starts_full_goes_negative_refills_by_rate_split_by_node`,
  `crates/cairn-server/tests/tenant_rate.rs::tenant_over_rate_is_throttled_to_rate_and_other_tenant_unaffected`.
- **FL-13. Under saturation, a free permit goes to the tenant with the lowest current `vt`.** The
  `Turns` wait list is a `FairQueue`: one FIFO per tenant, taken out by `(priority, vt **at
  take-out time**)` — not by a snapshot taken at registration, lest a noisy tenant's old queue
  stand ahead of a quiet tenant. `vt += CU × w` (`w = max_rate / rate`, no `rate` ⇒ 1), and turn
  k > 8 of one invocation pays an extra `0.2 × w`. Cross-range borrowing (the part above the
  guaranteed share) happens only when the tenant's `vt` ≤ the node's `min_vt` (of **waiting**
  tenants) + slack. A new tenant starts at `min_vt`; every second, a tenant that fell behind is
  pulled up by `vt += (max − vt) / 2` — a tenant that sleeps long accrues no credit. The guaranteed
  share does not change (FL-7).
  Tests: `crates/cairn-server/src/flow/tests/rate.rs::free_permit_goes_to_tenant_with_lowest_vt`,
  `crates/cairn-server/src/flow/tests/rate.rs::cross_range_permit_borrowing_by_vt`,
  `crates/cairn-server/src/rate.rs::tests::vt_by_weight_new_tenant_at_min_vt_and_pulled_back`,
  `crates/cairn-server/tests/tenant_rate.rs::loud_tenant_on_many_objects_does_not_queue_ahead_of_quiet_one`,
  `crates/cairn-server/tests/tenant_rate.rs::sleeping_tenant_wakes_without_starving_others`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (a permit called from the `Turns` wait list goes to the tenant with the lowest `vt` among the
  tenants that still have a live waiter; borrowing only when `vt ≤ min_vt + slack` —
  `check_grant_turns`).

- **FL-14. Quarantining a poison handler never cancels or fails anything, and only gates new
  work.** The identity is `(tenant, service, handler, deployment)`. The default is `observe`: a
  `warn` log on detection, a metric, the `quarantine` column of `/stats/handlers` — turns change
  nothing. `hold` (enabled per tenant): **new** invocations are created directly in `paused` code
  576 in the **same commit** that creates them (`CreateOptions.paused`), with the queue entry in
  `Inbox` — no turn runs, no VO lock is held; invocations already in progress keep running on
  their retries; only `resume` lets a held one run. `cooldown`: the fifth tier of `poll_acquire` —
  after rate (FL-12), before the turn cap — a cap of `COOLDOWN_CAP = 1` concurrent turn **per
  range** for that identity; `secure` adds `cool_usage`, `release_running` subtracts it; waiting
  here takes no turn slot and is not "out of budget" (`202`, a background turn keeps waiting). The
  identity is read (entry 0, one GET under the engine) only when the queue's `(tenant, service)`
  has a `cooldown` item. Lifting: `expires_ms` passes, a new deployment (the identity changes),
  `DELETE /admin/quarantine/…` (leaves `cleared` for one rule window + one summary window so the
  detector does not put it back on the old numbers). The turn path only reads the range's copy
  (`quarantine::RangeView`): one atomic compares the version, an empty list means no hashing and no
  clock read — no shared lock. Detection and all list I/O run in a background task of the node.
  Tests: `crates/cairn-server/tests/quarantine.rs::poison_handler_is_held_across_two_nodes_without_hurting_others`,
  `crates/cairn-server/tests/quarantine.rs::observe_is_the_default_and_changes_nothing`,
  `crates/cairn-server/tests/quarantine.rs::operator_delete_releases_and_does_not_flap_back`,
  `crates/cairn-server/tests/quarantine.rs::new_deployment_and_expiry_release_hold`,
  `crates/cairn-server/tests/quarantine.rs::cooldown_caps_handler_to_one_turn_per_range`,
  `crates/cairn-server/src/flow/tests/mod.rs::cooldown_one_turn_per_identity_on_range`,
  `crates/cairn-server/src/quarantine.rs::tests::detection_writes_cas_once_and_manual_clear_does_not_reenter`,
  `crates/cairn-server/src/quarantine.rs::tests::cleared_outlives_summary_window_containing_clear`.

- **FL-15. A drain pass costs what is still pending, not what was ever drained; and it skips no
  row.** The three drainers (`drain_outbox`, `drain_signals`, `drain_inbound`) open a pass with a
  read from `seq` 0 when their ledger holds no row waiting for any reason (below), but
  `PartitionWriter::seq_rows_from` does not rescan a stretch already walked: each ordered index has
  a floor (`SeqFloor`, in the writer's RAM; the value `min(floor, below)` is written durably with a
  commit and the next writer opens from it — FL-18). Below the floor, the rows still alive are read
  by point reads from `below`, and the open scan starts at the floor — it does not walk the
  tombstones of drained rows. The floor's invariant: every live index row with `seq < floor` is in
  `below`. The floor only advances over a stretch a scan has just walked to the end, and `commit`
  adds to `below` **every** index row written under the floor (a seq issued before a late commit,
  a rewrite with the old seq, D-O2) — it does not assume that `seq` issue order equals commit order.
  Past `BELOW_CAP` live rows under the floor, the floor stops advancing (reads as before). **A row
  that cannot be decoded is not deleted** (`turn::poison_row`): the drainer walks past it, counts
  `cairnflow_poison_rows_total` once per term, and the waiter of a `Completion` row goes to `paused`
  577 — after a fix, `resume` finds the row still there to deliver.
  **The outbox drainer does not re-read rows waiting for a lock**: `drain_outbox` only opens with
  a read from `seq` 0 when the term's ledger (`drainer::Backlog`, RAM) holds no row waiting for a
  lock; otherwise it re-reads each old row still to be considered (left behind for another reason,
  processed but not yet seen gone, waiting for the lock of an invocation that has since released)
  and then scans open from the previous pass's cursor — lock-waiting rows of an invocation **still
  busy** are not read (this pass would leave them anyway; the id still goes into `busy` and the
  "someone is waiting" mark of `Locks`). The ledger's invariant: every live outbox row with a `seq`
  below the scan cursor is in the ledger; a row leaves the ledger only when a read sees it gone
  (not when the drainer believes it is done — `Engine::deliver` has a path that does not commit); a
  row that is gone does not come back at the same `seq` because the only delete
  (`stage_outbox_removal`) removes the D-O2 entry as well. Losing the ledger (a failed pass, a new
  term) ⇒ rescan from 0. Debug builds check the invariant at the end of each pass with a full scan
  (`turn::outbox::check_backlog`; a mismatch panics).
  **The inbound and signal drainers keep a ledger too**: `drainer::RowLedger` (one ledger per
  queue, term RAM) records each row left behind with its **wake reason** (`drainer::Park`): the
  destination lock is busy (`Lock`, re-read when the lock is released), the L0 throttle (`Shed`,
  re-read in a pass that has not deferred — once the throttle defers, the rest of the pass is
  deferred too, so a deferring pass reads no throttled row; the callee still goes into `busy`), a
  service not yet in the deployment catalog (`Catalog`, re-read when the catalog changes —
  `Drainer::observe_catalog`), otherwise `Again` (processed but not yet seen gone, a failed send
  destination, a broken row — re-read next pass). A pass re-reads the old rows by `seq`
  (`turn::Reread`) and then scans open from the previous pass's cursor, not from 0; a ledger with
  no row waiting for any reason ⇒ read from 0 as before. Same invariant, same rule for leaving the
  ledger, same debug check (`check_ledger`) as the outbox ledger. Measurement:
  `cairnflow_{inbound,signal}_rows_read_total`.
  Test: `crates/cairn-server/tests/failpoints.rs::shed_inbound_rows_are_not_reread_every_pass`.
  **A hung destination does not hold back the sending range's inbound**: a drain pass goes
  **inbound → signal → outbox** — the last two end by waiting for every destination still in
  flight (`Sender::finish`), so the inbound that went first only waits until the next pass. A send
  that hits `SEND_TIMEOUT` (`TransportError::is_timeout`) puts the destination range into the
  drainer's **circuit breaker** (`Drainer::{trip, tripped, untrip}`, keyed by range id **and**
  table version, lives across passes, lost on a term change): the next pass sends it only **one**
  probe batch with a limit of `PROBE_TIMEOUT` (2 s) — on failure that batch and every later row to
  the same destination stay behind as for `down`; a full ack untrips it. A destination that
  refuses at once (503, `NoLeader`) does not trip the breaker. The inbound drainer's `PeekOutput`
  replies go through the same breaker.
  Test: `crates/cairn-server/tests/dead_destination.rs::hung_destination_does_not_hold_the_senders_inbound`.
  Tests: `crates/cairn-server/tests/interrange.rs::busy_callers_rows_are_not_reread_every_pass`,
  `crates/cairn-server/src/drainer.rs::tests::row_ledger_returns_rows_to_recheck_by_seq`,
  `crates/cairn-server/src/drainer.rs::tests::row_ledger_records_and_forgets_keeping_index_consistent`,
  `crates/cairn-engine/tests/outbox_seq.rs::empty_drain_pass_skips_tombstones_of_drained_rows`,
  `crates/cairn-server/tests/failpoints.rs::undecodable_inbound_row_is_kept_and_pauses_its_waiter`,
  `crates/cairn-engine/tests/outbox_seq.rs::skipped_row_below_floor_is_still_reread`,
  `crates/cairn-engine/tests/outbox_seq.rs::late_commit_row_below_floor_is_still_seen`.

FL-16…FL-21 are rules of the **invocation lifecycle**. They are rows of **one** state-transition
table — `cairn_lifecycle::step(view, event) -> Plan` (`crates/cairn-lifecycle`, row-by-row table in
`src/tests/`) — and the shell (`crates/cairn-server/src/turn/life.rs`) only reads the `View`,
executes the `Plan` and reports the next event; source check 5 blocks status changes outside the
shell. M4 (`crates/cairn-model/src/m4.rs`) checks `step` itself under interleaving, throttling,
crashes and leader changes — commits are atomic but **not** durable at once (an undurable tail is
lost on a leader change; receiving a message, `200` and calling the SDK demand durability as in
the real shell); the "Core" line of each rule says which table rows and which M4 property keep it.
Changing a rule ⇒ change the table and M4 in the same commit (AGENTS.md §2).

- **FL-16. Each term has a rescue net that reconciles by state.** A path that may drop the wake of
  an unfinished invocation without being able to write a replacement event **marks**
  `drainer::Rescue` — in RAM, no I/O, no `info` log. `rescue_loop` (a task of the term, stopped
  together with the drainer on quiesce) sleeps until there is a mark, then at the due time of each
  mark (backed off per id, see below) looks at the durable state: still `running`, no scheduled
  retry, **nobody working on it** (the invocation lock is free and no dispatched turn is
  unfinished — `Runner::is_queued`) ⇒ dispatch again through `Runner`. A failed read backs that id
  off one more step; a failed full scan rescans, with double the backoff. It is bounded: a ledger
  of `RESCUE_CAP` ids (overflow ⇒ full scan like `recover`), the tick, and the work per round.
  Places that mark: a failed `recover` scan (full scan); and the **default nack** (a lesson taken
  from Temporal): every task that runs a turn of an invocation (a turn dispatched by the
  scheduler — guard `Dispatched` —, `/invoke`, `/send`, `/resume`, an external signal, cooperative
  cancel) holds a `drainer::WakeDuty` for the id, and `turn::drive*` demands it **by type**. A turn
  calls `ack()` only once the invocation has a durable wake or no longer needs one: the gate sees
  it ended/`paused`, the turn-end commit is durable (with the scheduled wake if there is one), the
  core's `TurnFailed`/`Throttled` wrote the scheduled wake, a turn with a preset schedule
  (`Turn::planned`: the next scheduled wake is durable before the turn — **except a parked-ticket
  turn**: while the ticket was parked, the background tick may have moved that row far out into a
  one-day watch row, so `drive_granted` does not ack early, and a status-read error at the
  post-permit gate of a `planned` turn reports core `TurnFailed { BeforeRunning }` so the core
  rewrites a near scheduled wake), a fence error (`marks_rescue`), a creation that is not this
  turn's (`Existing`/`Held`, the L0 throttle). Dropping without an ack — any `?` (including a new
  `?` nobody thought of yet), a panic anywhere in the task, a task cancelled mid-term — makes
  `Drop` mark the net (`Rescue::nack`): RAM only, no I/O, no wait, no second panic (ledger already
  borrowed ⇒ full scan). When the term ends (`Rescue::close` before `tasks.shutdown`) the marks are
  dropped: turns cancelled while stepping down are the next term's `recover`'s job. This replaces
  `turn::lost_wake` sprinkled at each `?` and `actor::rescued_on_panic` wrapping each task — a
  `/resume` task someone forgot to wrap was a real gap (test below). The decision of one
  reconciliation round (busy, mark, status, scheduled wake, threshold) belongs to the lifecycle core
  (`cairn_lifecycle::step`, `Event::RescueRound`), in the same table as `recover`
  (`Event::Recover`). **Backoff per id**: the `n`th consecutive mark of an id waits
  `min(0.5 s × 2ⁿ⁻¹, 30 s)` (jittered), other ids are not dragged along; a turn that gets through
  (`drive_with_permit` has an outcome) breaks the streak (`Rescue::clear`). Past `InfraStuck` (50
  consecutive marks or 1 hour) the reconciliation round puts it into `paused` code 577
  (`INFRA_STUCK`) instead of dispatching again. If a term-wide task dies (drain, rate, rescue net),
  the serving loop steps down to rebuild.
  Metrics: `cairnflow_rescue_{marked,nacked,requeued,round_failed,infra_stuck}_total`
  (`nacked` is the part of `marked` that comes from the default nack).
  Core: `Event::RescueRound`/`Event::Recover` (busy ⇒ stop or `Mark::Later`; failed read ⇒
  `Mark::Strike`/`Abort`; a scheduled retry ⇒ stop; past the threshold ⇒ `paused` 577), `Duty::Ack`
  of every turn-ending Plan; M4 `LIVE` (quiescent state ⇒ every invocation reaches an outcome or a
  `paused` with a reason — including the rescue net's 577).
  Tests: `crates/cairn-server/tests/failpoints.rs::failed_recover_scan_is_retried_within_the_term`,
  `crates/cairn-server/tests/failpoints.rs::send_turn_failing_before_reschedule_is_rescued`,
  `crates/cairn-server/tests/failpoints.rs::turn_whose_reschedule_also_fails_is_rescued`,
  `crates/cairn-server/tests/failpoints.rs::rescue_backs_off_per_invocation`,
  `crates/cairn-server/tests/failpoints.rs::invocation_stuck_on_infra_is_paused_577`,
  `crates/cairn-server/tests/failpoints.rs::panicking_turn_is_rescued`,
  `crates/cairn-server/tests/failpoints.rs::turn_failing_at_gate_is_rescued` (the gate),
  `crates/cairn-server/tests/failpoints.rs::retry_that_cannot_be_scheduled_is_rescued`
  (the scheduled wake after `Retry`),
  `crates/cairn-server/tests/failpoints.rs::client_turn_whose_early_reply_sync_fails_is_rescued`
  (the `sync` of `drive_client`),
  `crates/cairn-server/tests/failpoints.rs::rescue_keeps_its_mark_when_the_status_read_fails`,
  `crates/cairn-server/tests/failpoints.rs::resume_turn_that_panics_is_rescued` (default nack: a
  task nobody wrapped),
  `crates/cairn-server/src/drainer.rs::tests::wake_duty_nacks_by_default`,
  `crates/cairn-lifecycle/src/tests/restore.rs::rescue_net_and_recover_table`,
  `crates/cairn-server/src/turn/mod.rs::tests::rescue_skips_fenced_errors_and_planned_retries`,
  `crates/cairn-server/src/drainer.rs::tests::rescue_net_backs_off_per_id_and_knows_when_stuck`,
  `crates/cairn-server/src/drainer.rs::tests::rescue_net_is_capped_and_take_drains_due_marks`,
  `crates/cairn-server/src/drainer.rs::tests::runner_counts_delivered_turns_until_done`.

- **FL-17. Every invocation that entered a turn has at least one durable wake.** The commit that
  moves the stage to `Running` (core `Event::PermitGranted`, `turn::begin_turn`) carries a **watch
  row** — a `retries` row with the `WATCH_FLAG` flag (byte 9), due at
  `now + sdk_timeout + WATCH_MARGIN`, carrying the **counted attempt** (`attempt + 1`) and the
  attempt's journal length (after the wake the turn carries; previously `0 ‖ u32::MAX`, so a turn
  that died after `Running` was not counted) — unless there is already another durable wake (a turn
  run by a scheduled retry: the schedule was preset). The `ctx.attempt` of a turn with no preset
  schedule is read from the durable row (`cairn_lifecycle::step::position`): when a watch row fires
  after a turn died, the next turn carries the counted attempt (FL-19). An old binary reads a watch
  row as a retry of that `attempt` — one extra turn when it fires, not wrong. A turn ends in **one**
  commit (core `Event::TurnEnded`/`TurnFailed`, `turn::end_turn*` → `Engine::apply_lifecycle`): the
  stage or the row withdrawal, removing the watch row, and the retry if there is one; if no retry
  can be scheduled after an error, the watch row is **kept**. When a turn ends for good, the
  engine's terminal commit (`Effect::Finish`, `fail_with`/`cancel`, `restart` and `continue_as_new`
  for old versions) drops the retry entity row (`Mutations::drop_retry`) — removing it separately
  would add one more durability wait for the caller (the `durability.test.ts` test that counts the
  durability waits of one `ctx.run`). A turn that dies anywhere (process, panic, an un-acked `?` —
  FL-16) has its watch row fire through `fire_retries` like a retry. `pending_retry` does not count
  watch rows: recovery on becoming leader and the rescue net do not wait for them. The retry index
  has a scan floor (`due_floors`, lowered when a commit writes under it, written durably with the
  commit — FL-18) so each background tick does not walk again over the tombstones of every removed
  watch row; there is **one** floor, with the same meaning in RAM and on the store: no retry index
  row (live, stray, or watch) has a due time under it. A watch row due under the floor lowers the
  floor in the `begin_turn` commit itself. **Stray** index rows (the entity row moved and the write
  did not delete the old index row) count toward the `limit` cap of a `due_retries` scan — a
  cluster of strays with the same due time is not read and cleaned in full within one hold
  (`crates/cairn-engine/tests/outbox_seq.rs::stray_index_rows_count_toward_scan_cap`). The
  `Running` commit of a turn with a preset schedule rewrites the row at its **real** due time
  (`begin_turn` reads the row before the commit): when the background tick moved the row (busy
  holder, parked ticket — one day), the index row at the moved due time is deleted, leaving no
  cluster of stray rows that all fire one day later
  (`crates/cairn-server/tests/flow_control.rs::index_row_of_postponed_retry_deleted_on_running`).
  **Not covered:** an invocation just created in `Inbox` that has not entered a turn — its wakes
  are the turn dispatched in RAM, `recover` (at term start and every time the deployment catalog
  changes) and the rescue net. When a chunked `recover` is superseded (the catalog changed in the
  middle of the scan), the new scan **starts where the old one stood** (`Shared::recover_at`,
  written after every chunk) and then wraps around to the beginning (`ScanRing`): when the catalog
  changes faster than one full scan, the tail of the index is still reached (previously the new
  scan restarted from the beginning and the tail was never reached). Test:
  `crates/cairn-server/src/actor/background.rs::tests::superseded_recover_inherits_its_cursor`.
  Tests: `crates/cairn-server/tests/failpoints.rs::turn_lost_after_running_is_woken_by_its_watch`,
  `crates/cairn-server/tests/lifecycle.rs::turn_dying_after_running_still_counts_attempt`,
  `crates/cairn-engine/tests/outbox_seq.rs::watch_row_fires_like_retry_and_clears_with_final_commit`,
  `crates/cairn-engine/tests/outbox_seq.rs::cleared_retry_not_rescanned_and_row_below_floor_still_seen`,
  `crates/cairn-engine/tests/progress_floors.rs::watch_row_survives_reopen_and_recovery_reads_retry_from_floor`,
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (the `Driver` calls the core itself: turns, background ticks and recovery on the store model, a
  crash keeps a random prefix of the undurable tail; checks FL-17 and `cairn_lifecycle::valid`
  after every step, and heals until every invocation is done).
  Core: `Event::PermitGranted` (the `Set` row of the `Running` commit), `valid` ✘
  `RunningWithoutWake`; M4 `VALID` (after every step) and `LIVE`.

- **FL-18. A durable progress floor never passes an unprocessed row** (Temporal's "ack level").
  The scan floors of the four due-time indexes (timer, retry, purge, delayed — `due_floors`) and
  the three ordered indexes (`SeqFloor`) are written durably as **one** key of the derived zone
  (`0x01 ‖ 0xFD`, `Index::Floors`), **in the batch of an existing commit** when the value changes —
  no commit of its own, no PUT of its own; the fence is `writer_epoch`. On becoming leader, the
  first scan of each index starts at its floor, not from 0 across tombstones. Invariant: no
  due-time index row still on the store has a due time under its floor, and no **live** ordered row
  has a `seq` under its floor. It holds thanks to three places (`writer/floors.rs`): a commit that
  writes a row under the floor **lowers the floor before building the written value** (atomically
  with the row); the floor only advances in a scan, over rows that **earlier** commits deleted, and
  the advanced value goes out in a **later** commit (the WAL is sequential); the value carries a
  `writer_epoch` mark and is used only when it belongs to the writer **immediately before** — an old
  binary or a tool that opened the Db for writing in between makes the floor be dropped (scan from
  0). Missing, broken, unknown version ⇒ 0. Rebuilding the derived zone (reissuing consecutive
  `seq`s from above every known `seq` — see below) resets the floors to 0; a child Db after a
  split/merge does not see the key (the derived zone is outside every projection).
  `recover` used to read scheduled retries with one scan of the retry index from the floor
  (`Engine::pending_retries_among`) instead of one GET per `running` invocation. Now `recover`'s
  scan goes in chunks and turns interleave between two chunks (and may schedule new wakes), so a
  set captured earlier is no longer right for a later chunk: `recover` reads the retry row of each
  `running` id **that nobody is working on** in the same hold as the dispatch (core `Need::Retry`,
  like the rescue net). `pending_retries_among` remains for FL-18's tests.
  There is no "never" branch on the running path to place a tripwire on (a floor written wrong
  puts a row outside the scan, and nobody sees it): the check is
  `PartitionWriter::rows_below_floors` (testkit), run after every open in the random-crash tests.
  **Timer scans are capped**: one tick reads at most `MAX_TIMERS_PER_TICK` (256) timer index rows,
  as for retries; hitting the cap ⇒ the next due time is that of the next due row (the background
  loop wakes again at once) and the key of the last row read becomes a RAM cursor
  (`timer_resume`) — the next batch reads on **after** it, so rows that cannot be deleted yet (an
  invocation mid-turn) do not take up the cap of every tick. A scan from the cursor **does not
  touch the floor**; the floor only advances in a scan that starts at the floor, up to the first
  row it meets. A row written under the cursor in the middle of a round waits for the next round.
  `rebuild_derived` drops the cursor together with the floor. `/admin/tick` also dispatches at most
  that many per range. Tests:
  `crates/cairn-engine/tests/progress_floors.rs::capped_timer_scan_resumes_after_its_cursor`,
  `crates/cairn-server/tests/lifecycle.rs::tick_delivers_at_most_the_cap_per_hold_in_due_order`.
  Tests: `crates/cairn-engine/tests/progress_floors.rs::progress_floor_survives_reopen`,
  `crates/cairn-engine/tests/progress_floors.rs::durable_floor_never_passes_a_row_under_random_crash`,
  `crates/cairn-engine/tests/progress_floors.rs::row_written_below_floor_then_crash_still_seen`,
  `crates/cairn-engine/tests/progress_floors.rs::floor_dropped_when_unaware_writer_interleaves`,
  `crates/cairn-engine/tests/progress_floors.rs::derived_rebuild_resets_floor_to_0`,
  `crates/cairn-engine/tests/progress_floors.rs::watch_row_survives_reopen_and_recovery_reads_retry_from_floor`,
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (`rows_below_floors` after every open; every inbound message drains while healing),
  `crates/cairn-store/src/writer/floors.rs::tests::floor_reads_back_exactly_and_loosely`.
  **The `seq` counter never goes back below an issued `seq`**: the same floor key carries a `high`
  pair (code `0xF0`, `floors::SEQ_HIGH` — outside every index code, old binaries skip it) = the
  largest `seq` ever written into an ordered index; `seq_heads` seeds the counter no lower than it,
  and it is read **regardless of the mark** (a lower bound that only grows — a higher counter is
  always safe; the scan floor is still used only when the mark is right). Before this, a writer
  that opened and died before its first commit made the next writer drop the floor, and once every
  row was drained the counter went back to 1: new rows sat below the whole old tombstone stretch,
  the floor only advanced to the last live row, so every drain pass of the range walked that
  stretch again until compaction cleaned it up (a 60-minute chaos run: that range read 5–10 times
  its volume per commit until the end of the run). Test:
  `crates/cairn-engine/tests/progress_floors.rs::dropped_floor_does_not_pull_seq_below_old_tombstone`.
  **Seeding uses each index's own floor**: `seq_heads` scans outbox, signal and inbound, each index
  from its own durable floor — not in one sweep from the outbox floor, which walked the tombstones
  of every inbound message ever drained during the first engine hold of the term (0.5 s at 10⁶).
  Test: `crates/cairn-engine/tests/progress_floors.rs::term_start_seq_never_walks_tombstones_below_floor`.
  The same rule in `rebuild_derived`: ordered index rows are reissued from `max(seq` of the index
  row just deleted, the `seq` in the entity row, the floor row's `high` regardless of the mark`) +
  1`, not from 1. Test:
  `crates/cairn-engine/tests/progress_floors.rs::derived_rebuild_issues_no_seq_below_tombstone`.
- **FL-19. An attempt is counted only when the turn enters `Running`** (**every** turn, not only
  turns dispatched by a scheduled wake that fired). `fire_retries` moves the scheduled row on to
  the next wake **before** running (a crash midway still leaves a wake) but keeps `attempt` as is;
  the turn's `Running` commit (core `Event::PermitGranted`, `turn::begin_turn`) rewrites that row
  with `attempt + 1` — unless the journal has grown since the fire (a wake was written while the
  turn was waiting, FL-20): then the turn is the first attempt at the new position, and the row
  records `1` at the current journal length (M4 found this; before, it carried the number and
  position of the fire). A turn that stops before that point spends no attempt: running out of the
  permit wait limit because of budget (`Throttled` — now only for turns with a caller: `resume`,
  external signals; background turns park a ticket, FL-30), a parked ticket or a permit-waiting
  turn lost when the range changes owner (stepping down does not wait for turns without a permit),
  a failure in `ensure_queued` — for a turn with no preset schedule, the scheduled row it leaves
  keeps the counted attempts of the durable row and only adds one fire (`fires + 1`, FL-21; core
  `Event::Throttled`/`TurnFailed { at: BeforeRunning }`). A crash after that point means the
  attempt has been counted — also for a turn with no preset schedule: the watch row of the
  `Running` commit carries it (FL-17). When a turn **with** a preset schedule fails (before or
  after `Running`), that schedule is its wake — unless the row is already a watch row moved far
  out because of a parked ticket (FL-30): the core rewrites `Retry` at the backoff step given by
  `position`, keeping `attempt`/`journal_len`/`fires` (table
  `cairn-lifecycle/src/tests/turn.rs::broken_ticket_turn_rewrites_deferred_watch_row`, flag
  `planned_keeps_watch_row`; M4 `WAKE_BOUND`, configuration `PARK_FAIL`) — except for one path
  that knows for sure the SDK was not called: the preflight `RetrySoon` (the invocation's
  deployment is not loaded on the node yet) gives that attempt back in the turn-end commit (core
  `Event::TurnStarted`, only adds one fire). Without this rule a burst of throttling after a
  failover, or a chain of owner changes of the range, used up `maxAttempts`, and the preflight
  produced `paused` 575 without the SDK having been called again even once, and without a single
  log line. The first fix only gave the attempt back to turns that *returned* `Throttled`; a
  cancelled turn returns nothing; a first turn that was throttled (`park_for_retry`) still recorded
  `attempt 1` until the lifecycle core took over this decision.
  Core: `position` and `Event::{PermitGranted, Throttled, TurnFailed, TurnStarted}` (table
  `src/tests/turn.rs`: the rows marked as changed also pin the behaviour from before the lifecycle
  core as a historical bug); M4 `ATTEMPTS` (an attempt is counted ⇔ the SDK was called, both
  directions; allowing a turn to die between the `Running` commit and the call) and `NEW_ATTEMPT`;
  `range_model.rs` checks the same rule on the store model (`check_attempt`). In the heavy variant
  of the billing chaos workload, the remaining missing attempts (jumps, "(b) real attempts") were
  all turns dying in exactly that window under `kill -9`.
  Tests: `crates/cairn-server/tests/flow_control.rs::{background_turn_parked_for_budget_does_not_spend_attempt,
  retry_cancelled_while_waiting_for_permit_does_not_spend_attempt}`,
  `crates/cairn-server/tests/lifecycle.rs::{parked_first_turn_does_not_spend_attempt,
  preset_schedule_recounted_when_journal_grows, unloaded_deployment_does_not_spend_attempt}`.
- **FL-20. Background ticks never dispatch a duplicate turn for an invocation someone is working
  on.** `fire_retries` treats an invocation as busy when its lock is held **or** a dispatched turn
  is unfinished (`Runner::is_queued` — the task may still be waiting for the engine, without the
  lock yet): it moves the scheduled row as in FL-5, and dispatches nothing more. It is the **same**
  question as `recover` and the rescue net ask: `turn::attendance` builds a
  `cairn_lifecycle::Attendance` in one place (`turn/life.rs`), and the core decides `busy()` from
  it; places that must write under the lock use `turn::claim` (take it if free). Without the second
  half, an engine congested for longer than the backoff was enough for the next tick to dispatch a
  second turn, and the SDK was called needlessly. The `Deliver` branch of `tick` (lock free) asks
  that half too (core `Event::TimerDue`): when a dispatched turn has not run yet, the timer is only
  **written** (completion + deleting the timer row, one commit), not dispatched again — the other
  turn takes the lock after the tick and reads a journal that already has the completion. The most
  common case is two timers of one invocation coming due in the same tick. The drainer's three
  "write then dispatch" places (`deliver_to_caller`, `settle_signal_local`,
  `deliver_inbound_completion`; core `Event::CompletionArrived`) ask the same half: two rows of one
  invocation in one drain pass dispatch only one turn.
  Tests: `crates/cairn-server/tests/flow_control.rs::delivered_turn_without_lock_is_not_delivered_twice`,
  `crates/cairn-server/tests/lifecycle.rs::{two_timers_on_one_tick_deliver_one_turn,
  drainer_does_not_redeliver_delivered_turn}`,
  table `crates/cairn-lifecycle/src/tests/retry.rs::tick_timer_table`.
  Core: `Attendance::busy()` in `Event::{RetryDue, TimerDue, CompletionArrived, Recover,
  RescueRound}`; M4 `NO_DUPLICATE` (planted flags `fl20_lock_only`, `a_timer_ignores_queued`,
  `drain_ignores_queued` are all red).
- **FL-21. The backoff step counts every fire, not only counted attempts** (a regression of FL-19
  found by a 60-minute billing chaos run). The scheduled row also carries `fires` — the number of
  times `fire_retries` dispatched a turn since the last counted attempt (the only encoding is
  `cairn_lifecycle::codec::RetryRow`: `attempt | journal_len | flag 0 | fires`, read loosely,
  missing = 0) — and the next fire is scheduled at `backoff(attempt + fires)`. `backoff` is the
  handler's policy (`retryPolicy*`) when it declares one, otherwise the default table — at
  **every** scheduling: a fire, a `Retry` that carries no due time from the SDK, errors before and
  after `Running` (core `Need::Policy`; before the lifecycle core the two error paths ignored the
  policy). Attempts are still counted only at `Running` (FL-19, `maxAttempts` unchanged); the
  `Running` commit rewrites the row as 8 bytes, so `fires` goes back to 0, and journal progress
  resets both. Without this rule, turns throttled or cancelled before `Running` could not push the
  next fire out: under constant owner changes, every invocation's scheduled row was fired again at
  the base step every term, the permit wait list swelled to ~97k on one range, every waiting turn
  grabbed the engine again on every wait slice, and throughput collapsed. Since then, a
  permit-waiting turn **does not** grab the engine on every slice: the "give up because of budget"
  check (`blocked_on(..).is_budget()`) reads the scheduler's RAM while the engine is released, and
  the task takes the engine only when granted a permit or when giving up (if a permit arrives just
  after giving up, it takes it); there is still **one** permit request for the whole wait. Test:
  `crates/cairn-server/tests/flow_control.rs::permit_waiter_does_not_keep_grabbing_engine`.
  Background turns no longer wait inside a task at all: they park a ticket (FL-30) — also when
  blocked by budget, so there is no giving up, and no scheduled row is produced by the permit
  request. A fire of an existing schedule whose turn parked a ticket and was then lost to an owner
  change still adds a fire here: `fire_not_counted_as_attempt_still_backs_off` builds the
  scheduled row with a turn failing in a retrying way.
  Tests: `crates/cairn-server/tests/flow_control.rs::fire_not_counted_as_attempt_still_backs_off`,
  `crates/cairn-server/tests/lifecycle.rs::retry_after_error_follows_handler_policy`,
  table `crates/cairn-lifecycle/src/tests/retry.rs::fire_retries_table`.
  Core: `Event::RetryDue` (`fires + 1`, step `attempt + fires`), `Need::Policy`; M4 `BACKOFF`
  (planted flag `fl21_no_fires` is red).
- **FL-22. A warm machine is only a cache; the journal on the store is the source of truth.** The
  drainer delivers completions to a caller using the machine kept in the range engine's RAM
  (`Engine::settle_machine`/`keep_warm`, `cairn-engine/src/engine/warm.rs`) instead of reloading and
  replaying the whole journal every time. A warm machine is used only when **the tail of the
  journal on the store matches it** (entry `L` absent; entry `L − 1` and entry 0 equal byte for
  byte; the head of the hash chain equal; the deployment binding re-read); a mismatch ⇒ drop it and
  load cold. Only the delivery path stores machines, and only after a successful `deliver`; taking
  one removes it, so any error or ambiguous commit leaves the cache without a machine for that id.
  `load` (every turn), `create`, `restart`, `purge` and endings remove the id's machine. The cap is
  32 MiB per range, LRU; lost when the engine is lost (leader change, split, restart). A delivery
  path that loads cold **only replays and applies no task**: the recovery scan is the next turn's
  job (always `load` + `Event::Start` through `Engine::step`). Debug builds and `warm-check`
  (`--features testkit`) compare each use of a warm machine against a cold-loaded one, and a
  mismatch panics (`CAIRNFLOW_WARM_CHECK=0` turns it off in benchmarks). Metrics
  `cairnflow_warm_machine_total{outcome}`, `cairnflow_settle_replays_total`.
  Tests: the lemma `crates/cairn-runtime/tests/warm_lemma.rs`, `crates/cairn-engine/src/engine/warm.rs`
  (`tests`: growth, a commit that never reached the store, `purge`/`create`, the cap, the
  comparison catching a mismatch mid-journal),
  `crates/cairn-server/tests/settle.rs::settles_of_one_drain_round_replay_the_callers_journal_once`.
- **FL-23. The drainer wakes a `suspended` caller only when its wait tree is satisfied or
  unknown.** The engine remembers the `Suspension` tree of the last suspension in RAM
  (`Engine::waiting_tree`: written after the `suspended` commit of `Engine::step`, removed when a
  turn loads the journal — `load` — or when the id is removed as for warm machines, lost when the
  engine is lost). A drainer completion (`deliver_to_caller`, `deliver_inbound_completion`) for a
  `suspended` invocation: the core asks `Need::Waiting`; the shell
  (`turn/life.rs::Asker::waiting`) evaluates the tree over the caller machine's ledger of
  delivered completions before and **after** the completion that just arrived. Known not to be
  satisfied ⇒ write the notification and delete the source row in one commit, **stay**
  `suspended`, dispatch no turn (`Engine::deliver_staying_suspended`); satisfied or unknown ⇒ as
  before. Liveness: the tree is monotone in the set of delivered completions, and every pending
  completion has a durable source, so the completion that satisfies the tree reaches the drainer
  and sees `after` right, or sees no tree and wakes. Timers (`TimerDue`) and signals always wake.
  No change to the SDK wire, no change to the bucket: the tree is a hint. Metric
  `cairnflow_settle_held_total`.
  Tests: `crates/cairn-server/tests/settle.rs::fanout_parent_runs_at_most_three_turns`,
  `crates/cairn-server/tests/settle.rs::parent_finishes_after_losing_its_waiting_tree_between_two_completions`,
  table `crates/cairn-lifecycle/src/tests/retry.rs::drainer_wakes_only_when_wait_tree_satisfied`.
  Core: `Event::CompletionArrived` + `Need::Waiting`; M4 configuration `TWO` (waiting for two
  completions, with a leader change in between), the planted flags `waiting_unknown_unsatisfied`
  and `waiting_without_new` break `LIVE`; `Bugs::BEFORE_3G_C` (wake on every completion) keeps
  every property.
- **LF-4. `/internal/inbound`'s `200` goes out only after the receiving commit is durable, even
  when the drainer has already acted on the message.** `on_inbound_batch` pokes the drainer as
  soon as `accept_messages` submits its commit (before `finish_unexposed_then`); the drainer runs
  when `Lease::sync` releases the engine — reads inbound rows at the `Memory` level, deletes the
  row, creates the invocation or delivers the completion, dispatches the turn — and with the WAL
  linger, the receiving commit, the row deletion and the next turn's `Running` land in the same
  object. The poke after `200` is still there (the net). IR-7 holds: `200` does not wait for the
  processing, only for the receiving commit to be durable; no task holds the engine while waiting
  for the network or for durability.
- **LF-5. Every external effect derived from a message just received follows the durable
  receiving commit.** Calling the SDK demands a durable `Running` (a seq greater than the receiving
  commit — a prefix), answering a client demands `Committed` (taken after the message was read),
  forwarding demands a `SendTicket` (the send rows produced from the message sit in a commit after
  the receiving commit). A lost tail is lost together: receiving commit not durable ⇒ every write
  after it is not durable either, and a sender without a `200` sends again (IR-2).
  Tests: `crates/cairn-server/tests/interrange.rs::inbound_message_and_first_running_share_one_wal_object`,
  `crates/cairn-server/tests/failpoints.rs::drained_inbound_on_a_lost_leader_runs_once` (lease
  lost before the message was durable: the SDK is not called, the callee runs exactly once),
  `crates/cairn-server/tests/failpoints.rs::early_drained_inbound_survives_split_quiesce`,
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (the ack duty: an acked message still has a durable row or was durably deleted after every
  crash; an un-acked message is sent again). M4: `LIVE` + flag `ack_before_durable`, `ATTEMPTS` +
  flag `effect_before_durable`, `ONE_DELIVERY` + flag `accept_checks_durable_only`,
  `SOMETIMES_EARLY_DRAIN`.
- **LF-7. The `/invoke` completion notifier is only a hint.** `wait_for_settled` registers in the
  range's waiter book (`actor::Waiters`, next to `EngineHandle`) **before** the first question
  (`Router::submit`, the status query of an attach — `Waiters::watch_armed`) and wakes on a
  notification **or** a tick of the poll grid, up to the `--invoke-wait-ms` limit. The grid is a
  safety net: 1 s while the book is fed (the leader on this node has hooked in, `Waiters::fed`),
  5 → 50 ms when it is not (nobody leads the range yet); when serving ends, the leader drops the
  flag and wakes every waiter (`Feeding::drop`) so they ask at once and see that the range changed.
  The notifier is the writer's hook (`PartitionWriter::set_status_hook`, re-hooked by the leader
  every term): every P1 write goes through `Mutations::set_invocation_status` and every batch
  through `commit`, so **every** ending path (a turn, `fail`/`pause`/`restart`, an admin or cascaded
  cancel, `Kill`, `continueAsNew`) notifies in exactly one place, at the moment the commit is
  submitted (not yet durable). The final answer is still produced only in `on_settled` → `finish`
  (durable ticket + read barrier): a notification carries no state. A spurious notification ⇒ one
  "not yet" question; losing the book (leader change, a range split — the wait loop registers in
  the new book and asks once at once —, the range on another node) ⇒ fall back to the grid. An id's
  entry lives exactly as long as its guards (the guard lives in the request's future): done, limit
  reached, client disconnected all remove the entry — gauge `cairnflow_invoke_waiters`. The lock is
  per range and held only for one map lookup. Metrics `cairnflow_invoke_wakeups_total{by}`,
  `cairnflow_invoke_polls_total{answer}`.
  Tests: `crates/cairn-server/tests/invoke_wait.rs::final_answer_arrives_without_waiting_for_the_poll_grid`,
  `crates/cairn-server/tests/invoke_wait.rs::waiters_are_released_on_every_exit` (done, limit
  reached, client disconnected, range split while waiting — the gauge and the book return to 0),
  `crates/cairn-server/tests/invoke_wait.rs::every_ending_wakes_the_waiter`,
  `crates/cairn-server/tests/invoke_wait.rs::workflow_attach_wakes_on_completion`,
  `crates/cairn-server/tests/split.rs::sync_invoke_waits_through_split`,
  `crates/cairn-server/tests/store_profile.rs::invoke_polls_at_most_twice_per_call_on_s3_standard`
  (≤ 2 questions per call on `s3-standard`),
  `crates/cairn-server/tests/invoke_wait.rs::waiter_wakes_and_reasks_when_its_leader_steps_down`
  (leadership lost while waiting: the answer comes before the sparse grid tick, the book returns to
  0), `crates/cairn-server/src/actor/waiters.rs::tests`.
- **AP-4. Side-channel frames add no durability wait to a turn.** `StreamAppend` (wire v4) stops
  at the turn layer (`turn/sdk.rs::side_frame`): it goes into the range's `StreamHub`, no
  `engine.step`, it does not become an entry. In `drive_bidi`, a batch holding only side-channel
  frames (`Keepalive`, `StreamAppend`) does no `sync` and no `send` — it reads the next frame at
  once. The hub is **per range and per term** (`RangeStreams`, a `Mutex` never held across
  `.await`), the node's RAM cap is one atomic (`streams::NodeRam`); no shared lock. Sealing goes
  through `life::side` (`Side::Seal`); an SDK outcome (engine `apply_finish`, core `Event::Finished`)
  seals in `end_turn` (`life::finished_side`). A turn that ends suspended on an external wait ⇒
  `RangeStreams::waiting` (`drive::after_run`); the range's timer tick (`RangeStreams::tick`)
  releases the bytes of a suspended invocation idle for longer than `idle_release` (keeping the
  ledger: offsets, deduplication) and cleans up streams sealed longer than `STREAM_LINGER` ago. End
  of term ⇒ `end_term`: SSE readers receive `moved` (+ `reset` for ephemeral streams). Reads happen
  only at the leader (`Command::OpenStream`), never on a follower reader (AP-13). A request asking
  to be forwarded (`x-cairn-forward: leader`, the console) to another node is run **at the leader**
  through `ingress/forward.rs` and the SSE body is copied line by line — still only the leader
  reads (`crates/cairn-server/tests/forward.rs::sse_follows_through_forwarding`).
  Tests: `crates/cairn-server/tests/streams.rs::appends_add_no_commit_to_the_turn`,
  `::idle_waiting_invocation_releases_ephemeral_records`,
  `streams.rs::tests::waiting_invocation_keeps_only_its_ledger`,
  `::bidi_records_reach_readers_mid_run_and_resume_by_offset`,
  `::non_leader_answers_503_with_leader_and_opens_no_reader`,
  `::leader_change_sends_moved_then_reset`, `crates/cairn-server/src/streams.rs::tests`, traces
  `conformance/traces/stream-side-{bidi,rr}-v4.json`.
- **AP-12/AP-16. Durable streams ride existing batches, and report durable only once durable.**
  `PartitionWriter` has one `Rider` per term (`streams::StreamRider`): **every** batch — a commit
  of any invocation, `seal`'s barrier (AP-15) — carries at most one extra `Streams` row per stream,
  holding the records from `carried`. No commit of its own on a timer; two exceptions: a flush when
  full (`FLUSH_BYTES`, 256 KiB not yet carried on the range ⇒ `turn/sdk.rs::side_frame` commits an
  empty batch, without waiting), and the handler's `logFlushMs` (`side_frame` schedules a mark at
  the next multiple on the node's clock, `turn/timers.rs::tick` commits an empty batch when it is
  due and records are still uncarried). A record carried in batch `seq` is reported by SSE as
  `durable: true` only when `durable_seq ≥ seq`. A new term seeds offsets from the last chunk in the
  Db before the first record (`RangeStreams::seed`) — reissuing from 0 would overwrite a durable
  chunk. Tests: `crates/cairn-server/tests/streams.rs::durable_records_ride_the_next_commit_and_never_commit_alone`,
  `::failing_step_commit_carries_pending_records`,
  `::pending_over_256_kib_flushes_without_a_turn_commit`,
  `::log_flush_ms_makes_waiting_records_durable`, `streams.rs::tests`.

- **AP-1/AP-2/AP-8. An artifact is durable before anything points to it; it is deleted only
  through the trash once its incarnation is dead.** `ArtifactPut` (wire v4) stops at the turn layer
  (`turn/sdk.rs::artifact_frame`): it PUTs the object `artifacts/<tenant>/<id>/<epoch>/<sha>` with
  the engine released (`Create`, `cas::put_resolving`), then commits the `Artifacts` row
  (`id ‖ sha → epoch`), before processing the next frame — an entry pointing to the artifact comes
  later in the same writer. The epoch is issued lazily from `entropy` at the first artifact of an
  incarnation (an id recreated after a purge has a different epoch). A purge deletes rows and does
  not touch objects; `artifact_sweep` (on each node, only for ids of the ranges it leads) holds in
  place the `(id, epoch)` directories that no longer have a row and are older than `min_age`, then
  deletes them for real after the retention period, checking again
  (`proof::artifacts_unreferenced` under the engine + `DeleteProof::purged_invocation`).
  Tests: `crates/cairn-server/tests/streams.rs::artifact_put_lands_before_the_output_and_reads_back`,
  `::purged_artifacts_are_held_in_place`, `::recreated_id_keeps_its_artifacts`,
  `crates/cairn-store/tests/proof.rs::purged_invocation_proof_needs_the_owning_range_no_row_and_age`,
  `crates/cairn-store/tests/trash.rs::hold_in_place_keeps_objects_until_purged_with_proof`,
  `crates/cairn-server/tests/failpoints.rs::artifact_orphaned_between_put_and_commit_is_swept`
  (a crash between the PUT and the row commit: the retry issues a new epoch, and the orphan is
  swept). The artifact sweep, the value sweep and tenant purge find in-place holds through
  `Trash::held_entries`: a LIST with a delimiter, and a GET of `entry` only for entries not yet
  classified (`entry` is immutable per id), so an idle round does not pay by the size of the trash;
  unclassified entries are read in parallel (32). A node's classification memory may still remember
  a hold that **another node** removed (the store keeps an empty directory): the artifact/value
  sweeps re-read `entry` before deleting, so this only costs work; tenant purge (the loop, the
  check before the tombstone, the API) uses `Trash::held_entries_checked` — it HEADs the known holds
  again, with an optional time limit, and reports the unclassified part in `HeldScan::unchecked`
  (`crates/cairn-store/tests/trash.rs::held_entries_checked_forgets_holds_another_node_removed`,
  `::held_entries_checked_stops_at_its_budget_and_resumes`). Tests:
  `crates/cairn-server/tests/gc.rs::trash_loops_do_not_reread_every_entry_each_round`,
  `crates/cairn-store/src/trash.rs::tests::held_entries_lists_in_place_entries_and_reads_each_once`.
- **AP-9. The `Artifacts` row commits within `T_put` of the start of the PUT.** Past that limit,
  `artifact_frame` PUTs again by overwriting (refreshing the age) before committing; the sweep only
  touches directories older than `min_age` ≥ 2 × `T_put`, so it never sees an object old enough
  while its row is still on its way to commit. Test:
  `crates/cairn-server/tests/failpoints.rs::slow_commit_reputs_artifact`.
- **AP-10. `restart-as-new` copies artifacts to the new id before the restart commit.**
  `turn/life.rs::copy_artifacts`: a server-side copy with the engine released, commit the rows of
  the new id, then `Engine::restart_in`; `continueAsNew` copies only artifacts whose sha is in the
  new input. Test: `crates/cairn-server/tests/streams.rs::purging_old_after_restart_keeps_new_readable`.

- **CG-10/CG-12/CG-15. Server-side schedules: a mark produces at most one run; a fire and the new
  `next_at` commit together; `overlap: skip` never overlaps.** The range holding a schedule's pk
  keeps the `Schedules` row (`0x1A`, keyed by schedule id) and the `ScheduleDue` index (the real
  fire time, `u64::MAX` while paused). `turn/timers.rs::tick` calls `turn/schedule.rs::fire_due`
  (a hot path, behind the `schedules/table` gate): an unkeyed target ⇒ `admit(Source::Schedule)`
  creates the run with id `schedule_run_id(schedule, mark)` **and** moves the schedule row in the
  same commit (`CreateOptions.schedule`), and an id that already exists ⇒ only move; a target with
  a key or a scope ⇒ an outbox `Fire` row (v7) + the schedule row in one commit, and the drainer
  (`turn/outbox.rs::outbox_fire`, outside the lifecycle core) sends a `StartCall` keyed
  `schedule/<name>/<mark>`, which the destination range deduplicates. Missed marks: fire once for
  the latest mark, the rest go into `missed`. Skip reads the state of the previous run from the same
  Db. Tripwire `schedule::live_without_next` on every write of a live schedule without a next due
  time. Tests:
  `crates/cairn-server/tests/schedules.rs::local_fire_creates_the_run_and_advances_in_one_commit`,
  `::remote_fire_crosses_ranges_once`, `::skip_never_overlaps`,
  `::missed_marks_fire_once_for_the_latest`, `::jitter_delays_the_fire_but_not_the_mark`,
  `crates/cairn-server/tests/failpoints.rs::schedule_fire_dies_around_the_commit`,
  `crates/cairn-engine/tests/schedules_table.rs`.

- **AU-1. The audit row of a human action rides the action's commit.** `turn::Ctx::audit_begin`
  puts the record into the engine's pending slot **after** taking the entity lock; the first commit
  touching exactly that entity carries the `Table::Audit` row + the `Audit` index (`take_audit_for`
  in `stage_status`/`stage_lifecycle`/`purge`/`migrate`/schedules/the `step` batch; an external
  signal only in the signal-delivery batch); no commit touches it ⇒ `audit_flush` commits on its own
  before answering. The export loop (`audit::export`, a background tick) reads rows under the
  engine, releases the engine to write the object `r-<hex key>.json` with `Create` + a read-back
  byte comparison, then deletes the rows — a crash in any window makes the next round write exactly
  that object again. Tests: `crates/cairn-server/tests/audit.rs`,
  `crates/cairn-server/tests/failpoints.rs::audit_export_dies_between_put_and_delete_writes_once`,
  `::audit_rows_follow_a_split_and_export_once`,
  `crates/cairn-store/src/audit.rs::tests::writes_exactly_once_through_ambiguity`.

- **FL-24. Suspended tenants get no permits; limited tenants get no new work.** The state is the
  `state=` field of the `tenants` registry, carried into each range's `TenantLimits` snapshot
  through `TenantLimitsSource` (an atomic generation, no shared lock in the steady state).
  `limited`/`suspended`: clause 0 of `turn::admit` returns `Admission::Refused` for the ingress,
  `restart` and schedule sources (nothing created, nothing read — `429 tenant_limited`/`403
  tenant_suspended`); schedules skip the fire right at the schedule's range (`LastKind::Skipped`,
  both same-range and cross-range) and manual fires are refused; child calls and `StartCall` are
  created as usual. `suspended` adds: the **first** tier of `poll_acquire` is
  `ResourceKind::Tenant` — counted as budget, with `throttle_after` = `TurnLimits::suspended_retry`
  (15 minutes). Background turns **park a ticket** (FL-30): no giving up, no rescheduling, no
  engine hold at all until the suspension is lifted — lifting it (`set_refused`) puts every queue
  back on the eligible ring and tickets are granted like any other waiter. Waiters of a suspended
  tenant **do not count toward its `demand`** (`Pending::demand` = `None`, removed/re-added in
  `Core::set_refused`): its frozen `vt` is not "the slowest waiting tenant", and counting it would
  pin the node's `min_vt` so that other tenants could not borrow free permits for the whole
  suspension (FL-13). Test: `crates/cairn-server/src/flow/tests/rate.rs::suspended_tenant_does_not_pin_min_vt`.
  Turns with a caller (`resume`, an external signal) and turns that **already have a permit** when
  the preflight sees the tenant suspended (the refusal at call time, below) still give up at once
  and reschedule through `Event::Throttled`; the core rule `due = max(row.due, now + after_ms)`
  keeps this to ≤ 4 commits/hour per invocation. The shell (`Scheduler::sync_tenants`) compares the
  generation at every permit request and every background tick; a changed set ⇒
  `Core::set_refused` puts every queue back on the eligible ring (as for a rule change) — lifting
  the suspension wakes the waiters. An unknown value ⇒ `suspended`.
  **Lifting a suspension runs scheduled turns at once**: a row scheduled while suspended lies
  `suspended_retry` in the future, so `Scheduler::set_refused` records the tenant that just left
  the set (`take_lifted`) and the next background tick (`turn::wake_lifted`) fires every scheduled
  row of that tenant on the range whose due time is in `(now, now + suspended_retry + 1 s]`, through
  the path of a due row (`Event::RetryDue`, core unchanged). Batched sweeping: each background tick
  reads ≤ 1 024 index rows and fires ≤ `MAX_RETRIES_PER_TICK` (256) rows, like `fire_retries`; the
  `(due, id)` cursor and the set of ids already fired live in `Shared::lift` (term RAM), and the
  background loop wakes again at once until the sweep is done. This includes ordinary retries whose
  backoff step falls into the window: lifting a suspension counts as ending one backoff. **A leader
  that comes up after the lift**: it never saw the tenant suspended, so `set_refused` sees nothing
  change; `TenantFlags::change_state` records in the registry `refused-ms=` (entering a
  permit-refusing state — keeping the old mark while rows of the previous suspension may still be
  waiting) and `lifted-ms=` (leaving it), and `sync_tenants` puts a non-refused tenant whose
  `lifted-ms=` mark has not been seen in this term into `take_lifted` with the mark
  (`flow::Lifted`). `LiftSweep::add` sweeps the due-time range the suspension may have scheduled,
  `[refused + suspended_retry − 60 s, lifted + suspended_retry + 60 s]` (`flow::LIFT_SLACK`, cut
  below at `now`; upper bound already past ⇒ dropped); it is merged with the range above when both
  exist. An old binary writing the registry does not update the marks: that lift then takes effect
  only at the leader that saw it. Tests:
  `crates/cairn-server/tests/tenant_state.rs::unsuspend_runs_parked_turns_at_once`,
  `::unsuspend_across_leader_change_runs_parked_turns` (the product's 15-minute reschedule tick,
  lifted through a node that leads no range), `::repeated_suspensions_in_one_window_are_swept_once_by_a_new_leader`
  (lift/suspend/lift spaced from the scheduled time by more than `LIFT_SLACK`; ordinary retries
  outside the range do not fire early),
  `crates/cairn-server/tests/failpoints.rs::leader_change_while_suspended_runs_every_parked_turn`
  (with parked tickets: tickets parked while suspended are lost with the old leader, the new leader
  dispatches again, and lifting runs each one exactly once — a background turn refused a permit no
  longer produces a 15-minute scheduled row, FL-30),
  `crates/cairn-server/tests/upgrade.rs::old_binary_keeps_lift_marks_and_new_binary_does_not_misfire`,
  `crates/cairn-server/tests/failpoints.rs::unsuspend_grants_parked_tickets_without_sweeping_rows`
  (lifting grants permits to parked tickets, no row goes through the sweep — the batched sweep ≤ 256
  remains for scheduled rows of refusals at call time and for throttled turns with a caller;
  `lift_tests` pins the swept range),
  `crates/cairn-server/src/tenants.rs::tests::lift_marks_follow_state_changes`,
  `crates/cairn-server/src/turn/retry.rs::lift_tests`.
  **Ask again right before the call**: a permit is only the right to run, not a promise that the
  call may go out — a run may stand waiting for durability, or for the engine for tens of seconds
  (in a soak run, permits were granted 7 s and 36 s before the suspend command and the SDK was
  called after the `204`), and one permit covers every sub-turn of the run. So the preflight
  (`TurnStarted`) reads `View::tenant_refused_ms` (`turn/permit.rs::refused_ms`), and the closures
  that call out to the endpoint — a `REQUEST_RESPONSE` request (including the resend after a cache
  miss), opening a BIDI stream, an in-place reattach tail, an ack batch while the handler still
  runs — ask `refused_now` after every wait point: refused ⇒ `Step::Refused` ⇒ preflight again ⇒
  core `Reply::Throttled`: `Suspended`, keep the VO lock, retry row at `now + suspended_retry`,
  attempt unchanged (given back when the journal has not grown since the row's position, back to 0
  when it has). A turn that is in the SDK when the command arrives goes on to the next step boundary
  and stops there. Counter: `cairnflow_flow_refused_at_call_total`. Tests:
  `crates/cairn-server/tests/failpoints.rs::suspended_tenant_turn_granted_before_suspension_never_calls_the_sdk`,
  `::suspended_tenant_continuation_in_the_same_run_never_calls_the_sdk`,
  `crates/cairn-lifecycle/src/tests/turn.rs::refused_tenant_does_not_call_sdk`, M4 `NO_CALL_REFUSED`
  (configuration `REFUSED`, matrix row `refused_calls_sdk`).
  `deleting`/`purged` (tenant purge) act like `suspended`, plus every key, cookie and SSO session
  **of** the tenant ⇒ `403 tenant_deleting` in `auth::authenticate` (keys that can choose a tenant —
  `cluster-admin`, `provision` — are not affected). Tests:
  `crates/cairn-engine/src/tenant_limits.rs::tests::deleting_and_purged_refuse_access`,
  `crates/cairn-server/tests/tenant_state.rs::deleting_refuses_access_and_reverts_until_purging`. Tests:
  `crates/cairn-server/src/flow/tests/mod.rs::suspended_tenant_gets_no_permit`,
  `crates/cairn-server/src/flow/sim/mod.rs::swarm_holds_flow_invariants_after_every_step`
  (no grant to a suspended tenant; the liveness phase lifts suspensions first),
  `crates/cairn-lifecycle/src/tests/turn.rs::throttle_pushes_due_of_planned_retry`,
  `crates/cairn-server/tests/tenant_state.rs` (every source of work, the SDK receives no turn while
  suspended, the number of refusals is bounded by the reschedule tick, lifting continues with
  `attempt` unchanged, unknown values, ACK).
- **FL-26. A tenant never holds more than its turn cap on a node.** `--max-turns-per-tenant` (SaaS
  profile: ¼ of `--max-concurrent-turns`, floor 1) is the cap on **running** turns of one tenant
  across every range of the node. `TenantMeter::turns` (node-wide, in `TenantBook`) is incremented
  in `Provisional::secure` and decremented in `release_running`; each range keeps the share it added
  (`ResourceManager::tenant_turns`) and gives back exactly that in `reset` and in `Drop` (a range
  dropped while turns are unfinished) — otherwise the tenant would be stuck at the cap forever. The
  `ResourceKind::TenantTurns` tier of `poll_acquire` comes after the counters and before the rate
  (blocking earlier spends no token); it is not budget (`202`, a background turn keeps waiting). A
  tenant dropping below the cap ⇒ the node's `announce` ⇒ every range re-asks its `TenantTurns`
  waiters (`Core::node_turns_changed`). The cap is **soft** across engine threads, like the node's
  turn cap (two ranges on two threads granting at the same moment can overshoot slightly); the
  sequential sim checks the exact cap. Tests:
  `crates/cairn-server/src/flow/tests/mod.rs::tenant_turn_cap_blocks_across_ranges_and_wakes`,
  `::dropped_range_returns_tenant_turns`, `flow/sim` (FL-26, parameter `tenant_cap`),
  `crates/cairn-server/tests/tenant_turns.rs::one_tenant_never_holds_more_than_its_turn_share`.
- **FL-27. The total journal held in RAM by the node's SDK turns is bounded.**
  `--replay-ram-budget-bytes` (default 512 MiB, `0` = off): after a cold load, a turn holds a
  `replay_budget::ReplayHold` of `Invocation::journal_bytes()` until the turn ends (`drive_one`).
  Not enough room ⇒ the same "refuse before calling" path as a suspended tenant
  (`View::tenant_refused_ms` ⇒ `Reply::Throttled`): the SDK is not called, the turn is rescheduled
  1 s later, the attempt is unchanged. With an empty budget a turn always runs (nobody is blocked
  forever). Split per tenant (a finding of a security review): one tenant holds ≤ ¼ (its first turn
  while it holds nothing is always admitted), and a turn with a journal ≤ `min(1 MiB, cap/64)` is
  always admitted and does not count toward the tenant's share — two tenants cannot starve a third.
  A process-wide atomic counter (the tenant's share lives in one of 256 FNV-hashed cells), no lock.
  In the same family: `max-journal-bytes=` of the `tenants` registry caps **one** journal — an entry
  **of the invocation itself** that pushes the total over is not written, and the invocation goes to
  `paused` 580 (`engine/step.rs`), so the next load reads at most one entry beyond the cap per wake.
  Entries **coming from outside** (completions, signals) are always written, even over the cap
  (each ≤ `max-payload=`): its source row is deleted in the same commit and an external signal was
  already acked with `200` — dropping it would lose data (same security review); `GET
  /invocations/{id}/journal` reads by page (`PartitionWriter::journal_page`, 4 MiB cap per page).
  Tests: `crates/cairn-server/src/replay_budget.rs::tests::{holds_and_refuses,
  two_tenants_do_not_starve_a_third, small_turns_and_first_turn_always_run}`,
  `crates/cairn-server/tests/replay_budget.rs::full_budget_reschedules_without_calling_sdk`,
  `crates/cairn-engine/tests/tenant_quota.rs::journal_bytes_per_invocation`,
  `crates/cairn-server/tests/tenant_limits.rs::{max_journal_bytes_pauses_invocation, journal_is_paginated,
  journal_bytes_cut_keeps_external_signal}`.
- **FL-25. SDK commands by id touch only invocations of the same tenant** (a security-review
  finding). `attach`, `getOutput` and `signal` (including idx 1 CANCEL and awakeables with idx ≥ 17)
  carry an id supplied by the SDK, which does not go through the ingress's `ensure_visible`. The
  shared choke point is `Engine::visible_status`/`visible_to`: the target's tenant is read from the
  metadata (when missing, from the header of entry 0); a tenant other than the caller's, or no
  readable tenant ⇒ take exactly the "does not exist" branch — `404 invocation <id> not found` for
  attach/peek (no `Attachments` row written), silently drop the source row for signals. The answer
  is byte for byte the answer for a random id. Same range: `task_attach`/`task_peek_output` (the
  turn's tenant), in-place outbox rows after a merge (`outbox_attach`/`outbox_peek`, tenant from the
  waiter's metadata), `settle_signal_local` (before the core). Cross-range: `Watch`/`DeliverSignal`
  v7 messages carry the caller's tenant (the drainer reads the waiter's metadata when sending;
  `task_send_signal` writes the turn's tenant), and the destination range trusts that field because
  `/internal/inbound` is authenticated with the cluster secret; a v5 message without the field ⇒
  `default`. Human signals (`/invocations/{id}/signal`) take another path
  (`deliver_signal_and_drive`), already checked at ingress. Tests:
  `crates/cairn-server/tests/tenant_isolation.rs::alpha_sdk_cannot_reach_bravo_by_id`
  (same range and cross-range), `crates/cairn-server/tests/interrange.rs::foreign_tenant_watch_and_signal_across_nodes_answer_not_found`
  (cross-node), `crates/cairn-engine/src/interpartition.rs::tests::watch_and_signal_carry_tenant`.
- **FL-28. A task waiting for a permit is small.** Each invocation the drainer emits is one
  `run_dispatched` task living through the wait for the engine and then for a permit, and its future
  is allocated in full at spawn with the size of its **largest** state. So in `drive_untraced` only
  the permit wait sits inline; the pre-permit gate (`gate_before_permit`), the reschedule when the
  limit runs out (`throttled`) and the post-permit turn (`drive_with_permit`) live in boxes of their
  own, allocated when that step is reached (≤ the turn cap at once). `stats::traced` is a plain
  function returning a future (an `async fn` holds `fut` twice). Before: 61 784 B per task
  (release) — a backlog of 3·10⁵ was ~20 GB, the process was compressed/swapped out, engine holds
  took 7–19 s. After: 8 440 B. A new large await before the permit wait goes into a box. Test:
  `crates/cairn-server/src/actor/background.rs::tests::waiting_turn_task_is_small`
  (≤ 16 KiB in debug builds). Now that task only lives through the gate — the waiter is a ticket
  (FL-30) —, and the admission gate is bounded (`drainer::ADMIT_CAP`); this number is now only the
  size of a task passing the gate and of a granted ticket's turn (`run_granted`). Test:
  `crates/cairn-server/src/actor/background.rs::tests::parked_waiter_is_data` (ticket ≤ 512 B).
- **FL-29. A drainer's engine hold is bounded, not proportional to the backlog.** A drain pass
  releases the engine between two **chunks**: inbound and signal after at most
  `INBOUND_BATCH`/`SIGNAL_BATCH` (64) rows, outbox after each full gathering round (32), all three
  when the current hold exceeds `turn::DRAIN_HOLD_BUDGET` (20 ms, `Lease::held_for`); `drain_loop`
  releases between two queues once over budget. The release point lies **between two rows**: each
  row asks the core (`Attendance`), reads the state and commits within one hold — asking before the
  release point and dispatching after it would dispatch duplicate turns (M4
  `drain_attendance_across_release` makes `NO_DUPLICATE` red). Rows read but not yet considered do
  not cross the release point (the next chunk re-reads from the cursor; `Reread::release` drops the
  probe read); `busy`/`down`, the ledgers (FL-15), the cursors, the `high` mark taken at the start of
  the pass and rows already handed to the `Sender` do cross it — as at the release points that
  already existed (send tickets, waiting for destinations). This is correct because each queue has
  one reader: turns, commands and background ticks that slip into the release point dispatch none of
  its rows; rows of a destination that becomes busy after the release point stay behind like every
  row of a busy destination (per-destination order is kept); newly arrived rows carry a `seq` above
  the mark and are left for the next pass. Before: one hold for the whole pass — measured at ~770
  rows, 0.5–0.6 s under a backlog of 3·10⁵.
  Tests: `crates/cairn-server/tests/recovery_storm.rs::drain_releases_the_engine_between_chunks_whatever_the_backlog`
  (5 000 rows: 2 holds, 644 ms → 81 holds, 24 ms in debug builds), M4 (`Action::Drain` is one step
  per row, with matrix rows for it), `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (`Driver::drain_chunk`: receiving messages, turns and crashes slipped in between two chunks).
- **FL-30. Background permit waiters are data, not tasks** (`crates/cairn-server/src/flow/ticket.rs`).
  A `Runner` turn is dispatched through a bounded **admission gate** (`drainer::ADMIT_CAP` = 64
  tasks at once; excess requests sit in the channel as data; a task waiting for the invocation lock
  of a turn in flight gives up its place before waiting): it takes the invocation lock, runs the
  pre-permit gate (`Event::Drive`), then requests a permit **synchronously within the same engine
  hold** (`turn::drive_dispatched` → `Scheduler::park`). Granted at once ⇒ the turn runs in that
  task; otherwise it **parks a ticket** (`flow::Ticket`: the `RunRequest` turn, the `Dispatched`
  guard — the "turn dispatched" mark of FL-20 and the wake duty of FL-16 —, the `(queue, seq)`
  place, `cool`) in the `Core`'s queue, and the task ends, releasing the invocation lock. Whatever it
  is blocked on, the ticket just sits there — VO lock, counter, rate, suspended tenant (FL-24), turn
  cap: no `queue_wait` slices, no `Event::Throttled`, no engine hold at all until it is granted; the
  grant order is the core's (seq in the queue, the lock holder first, WFQ by `vt` — FL-13). A grant
  to a ticket (`Scheduler::deliver`) hands ticket + permit (the unused return path, by `held_before`
  **at grant time**, not at request time — a cancel may have released the VO lock while the ticket
  was parked) to the serving loop; the turn task (`actor::run_granted`) **takes the invocation lock
  synchronously at its first poll**: busy ⇒ **nack** (`Scheduler::nack`): the permit goes back — the
  VO lock stays with the ticket, FL-5 —, the ticket waits for the lock to be released (`Locks` pokes
  `Scheduler::lock_freed`) and then asks again with the same `seq`. No task holds a permit while
  waiting for the invocation lock — the "invocation lock first, permit second" order of every other
  path holds, and no two tasks wait for each other. The turn continues from the post-permit gate
  (`Event::PermitGranted`): an invocation that finished, was cancelled or went `paused` while the
  ticket was parked does not run. **One entry per invocation in the queue**: every new permit
  request (a background turn that just passed the gate, a client turn, `resume`, an external signal
  — `Scheduler::{park, acquire_cooled}`) withdraws the live ticket of the same invocation wherever it
  is (`Scheduler::adopt`: in the core — `Core::withdraw` —, waiting at the gate, waiting for the
  lock, granted but not yet holding the lock — the ticket's `token` number no longer lives; a
  granted ticket hands **its permit** to the request, FL-32); the new turn takes over the wake duty,
  and the withdrawn ticket leaves no rescue-net mark. M4 found why: without the withdrawal, a turn
  carrying a timer dispatched while the ticket's gate still held the lock ran before the ticket, and
  then the ticket (a preset schedule) counted the attempt again at the position the other turn had
  already counted. A ticket holds no invocation lock, so cancel, the drainer and signals do not wait
  for its turn (before tickets, cancelling a VO lock waiter waited until that waiter was granted).
  Tickets are term RAM: `Scheduler::reset`/`close` drop them (the rescue net is already closed), and
  the next term's `recover` dispatches again (INV-3). `close` withdraws the core's queues **first**
  and only then drops granted tickets: returning a granted ticket's permit grants it again to the
  next ticket, and a granted ticket holds the scheduler inside its return path — the reverse order is
  a reference cycle keeping the core and every ticket alive after the term (a benchmark caught this,
  `close_leaves_no_ticket_behind`). Quiesce **closes the grant gate first** and then returns the
  permits of tickets granted but not yet turned into a turn (`retire_ready`): a returned permit does
  not cascade into grants for the next ticket
  (`flow/tests/ticket.rs::quiesce_does_not_cascade_grants`). Benchmark: W = 2·10⁴ waiters blocked
  on budget — 6 076 → 38 engine holds/s while waiting (only `drain`/`tick`), granted turns 34 →
  100/s, the same as at W = 10³. Gauges `cairnflow_flow_parked` (tickets),
  `cairnflow_flow_turn_tasks` (background turn tasks).
  **A parked ticket's retry row is moved far out once**: a ticket usually carries a scheduled wake
  (the preset schedule of a fired turn, the watch row of a turn that died midway) that comes due
  while the ticket is parked. The background tick asks the core with `Attendance::parked`
  (`Scheduler::is_parked`); the core (`Event::RetryDue`, busy branch) moves the row by
  **`Knobs::parked_defer_ms`** (`PARKED_DEFER_MS`, one day) and turns it into a **watch row**,
  keeping `attempt`/`journal_len`/`fires` (`position` does not read the kind — FL-19, FL-21 intact):
  the ticket's turn rewrites the row at `PermitGranted` when granted, and a turn that withdraws the
  ticket rewrites it when it runs; if the ticket's turn fails **before** rewriting (a status read at
  the post-permit gate, a `Running` commit that failed before writing), the core's `TurnFailed`
  rewrites a near `Retry` (before this, a `running` invocation was woken by nobody for a day or
  until the next leader change); a ticket lost with the term means the next term's `recover` does
  not wait for the watch row (`pending` only counts the `Retry` kind) and dispatches a turn at once.
  Except for an `Inbox` entry of an EXCLUSIVE queue (a watch row there is the mark of having entered
  `Running`, `valid` ✘ `EnteredButUnlocked`): moved like an ordinary busy holder. Before: moved by
  `busy_defer_ms` (10 s) like an ordinary busy holder — W/10 row writes per second, and `tick`'s hold
  time grew with the number of tickets (benchmark `permit_queue_retry_rows`: 2 000 moves/s, 202 ms
  held/s at W = 2·10⁴). Core: table `cairn-lifecycle/src/tests/retry.rs::parked_ticket_row_deferred_into_watch_row`
  (flag `parked_defer_short` pins the old behaviour); M4 planted flag `parked_defer_resets_fires` →
  `BACKOFF` (configuration `PARK_RETRY`). Test:
  `crates/cairn-server/tests/flow_control.rs::parked_ticket_with_schedule_not_rescheduled_every_tick`.
  Tests: `crates/cairn-server/tests/flow_control.rs::{budget_waiter_neither_grabs_engine_nor_reschedules,
  permit_waiter_is_not_a_task, cancelled_object_lock_waiter_does_not_wait_for_its_turn}`,
  `crates/cairn-server/src/actor/background.rs::tests::parked_waiter_is_data`,
  `crates/cairn-server/src/flow/tests/ticket.rs::{close_leaves_no_ticket_behind,
  a_nacked_ticket_waits_for_the_lock_then_asks_again, a_new_request_adopts_the_parked_ticket}`,
  `crates/cairn-server/tests/flow_control.rs::turn_with_caller_replaces_parked_ticket_of_same_invocation`,
  `crates/cairn-server/tests/quiesce.rs::quiesce_does_not_wait_for_a_ticket_granted_meanwhile`, M4
  (`Pc::Parked`, planted flags `adopt_keeps_ticket` → `ATTEMPTS`, `parked_grant_ignores_lock` →
  `ONE_TURN`, `nack_drops_ticket` → `LIVE`, `parked_granted_twice` → `NO_DUPLICATE`,
  `parked_grant_skips_gate` → `VALID`; the old shell is `Bugs::BEFORE_W6Q`),
  `crates/cairn-engine/tests/range_model.rs::turns_and_recovery_on_model_with_random_crashes`
  (`Driver::run_parked`: parked tickets, other steps slipping in, crashes), the `flow/` sim
  (`Core::withdraw` on half of the cancellations).
- **FL-31. At term start, commands go before queued background work, with a bound.** The engine
  lock (`lease::EngineLock`) is a tokio FIFO mutex plus a `Precedence` part, active only in the
  **opening window of a term**: opened when the lock is built, closed when `restore_flow` (if it
  runs inside the serving loop) and the term's first `recover` are done (`EngineLock::opening_job`;
  the guard drops even on failure or cancellation; no such job ⇒ closed at once). Inside the window,
  a **background** task (`lease::BACKGROUND`: `tick`, `drain`, `query_build`, `hint_sweep`,
  `recover`, `rescue`, `restore` — by the `holding_as` label) that asks for the engine while a
  command is waiting stands aside; one already in the mutex queue gives up its turn when a command
  arrived after it. **The bound**: each background request yields to at most `lease::YIELD_MAX` (16)
  command acquisitions and then queues as usual — a flood of commands does not starve the lock
  rebuild (FL-8: the grant gate) or the drainers. Background work already holding the engine
  releases early at its **existing** release points when a command is waiting
  (`Lease::command_waiting`): the drainer treats the chunk as full (`turn::chunk_full`, once the
  chunk has at least one row — FL-29) and releases between two queues. The lock rebuild is **not**
  cut into smaller chunks: its first hold is opening a cold iterator (41 ms for 128 rows at
  H = 10⁶ — chunk size cannot cut that), while sub-chunks of 128 rows made the whole rebuild 4×
  slower (2.0 → 8.0 s at 10⁶; measured and then dropped). No new kind of release point is added and
  nothing a hold does changes: only which of the waiting tasks takes the engine next — M4 picks
  every enabled action at each step, so every such order has already been explored (the "pick the
  next action" row of M4's shell table). A "command" is every task that is neither background work
  nor a turn dispatched by the scheduler; `Runner` turns (`run_dispatched`/`run_granted`,
  `lease::runner_turn`) acquire FIFO — they neither yield nor are yielded to: at term start
  `recover` dispatches all R turns, the grant gate is still closed so they only park tickets, and
  counting them as commands would slow down the lock rebuild (the thing that opens the gate)
  (benchmark at H = 10⁶: "dispatch again" 8.8 → 9.8 s). Outside the window: FIFO as before. **The
  final reply does not reacquire the engine**: `Shared::finish`/`finish_unexposed*` consume the
  `Lease` (`Lease::sync_reply_owned`/`sync_owned`): the durable mark and the read barrier are taken
  while holding, then the engine is released and the wait happens without reacquiring — the ticket
  promises exactly what it did before; a task with work left after the reply (`/send`) goes through
  `finish_held`. Before: it reacquired only to release, queued behind holds that slipped in while it
  waited (benchmark at H = 10⁶ cold: 34 ms after the first drain pass). The `cairnflow_schedules`
  gauge is counted in the term's first `query_build` task (counted and set within one hold — schedule
  commands `adjust` it within their own hold), not before the serving loop. Before: the first command
  queued behind the first `restore` chunk, the first drain pass, `recover` and the first background
  tick — at H = 10⁶ cold it waited 102 ms for the engine. Tests:
  `crates/cairn-server/src/lease.rs::tests::{command_goes_before_queued_background_at_term_start,
  background_is_fifo_after_the_opening, background_yields_to_at_most_yield_max_commands,
  final_reply_does_not_wait_to_reacquire_the_engine, runner_turn_does_not_jump_the_restore}`.
  **Consequence for probes**: "the range answers commands" no longer implies "the lock rebuild is
  done" — whoever needs both waits for the second one separately. Split/merge (`wait_led_here`)
  probes the leader with a command and then only reads the node's `restore_ms` (it does not probe
  with a command every 2 ms — the rebuild would yield to each probe); test
  `failpoints.rs::split_reports_after_children_restore_their_queue`.
- **A pinned limit, not an invariant: the `StartCall` deduplication window is exactly as long as
  the age of the callee's invocation row.** The destination range deduplicates resent messages with
  the invocation row itself (`turn/admit.rs`, the `Source::Inbound` branch: the row exists ⇒
  `Attached`). A purge (retention expired or `POST /invocations/{id}/purge`) closes the window: a
  resend that arrives after it **runs the callee again**. A sender only resends outbox rows that are
  not yet deleted, and rows are deleted right after the ack, so this case needs the sender to stall
  (or lose the ack) for longer than the retention. Whether to change or keep this is an open design
  decision; changing it flips the test's assertion. Test:
  `crates/cairn-server/tests/start_call_window.rs::start_call_resent_after_callee_purge_runs_callee_again`.
- **FL-32. An invocation has at most one live permit; a VO lock is released only by the permit that
  holds it.** Every permit request of an invocation runs under its invocation lock — except parked
  tickets (FL-30), which are granted **without** holding the invocation lock. So a granted ticket
  keeps its **permit in the scheduler's ledger** (`Tickets::handed`) until its turn takes the lock
  (`Scheduler::claim`); the turn task only holds a handle (`flow::Granted`). A new request of the
  same invocation in that window — an external signal, cooperative cancel, a timer-carrying turn the
  background tick dispatched while the lock was busy, a turn dispatched by the drainer — withdraws
  the ticket **and takes that very permit** (`Scheduler::adopt` returns the `Permit`, with the unused
  return path given by `held_before` at grant time), without requesting another; a nack or `claim`
  of a handle that was withdrawn has nothing left to return. Before this, the permit lived in the
  future of the spawned task, a new request was granted a second permit ("already holds the lock",
  `Core::request`), and returning the ticket's permit — `release_all` compares locks by **invocation
  id**, not by permit — released the VO lock the other permit was using: the next one in the queue
  ran in parallel on the same EXCLUSIVE object. No external signal is needed: M4 found the
  timer-carrying turn case. Tripwires (AGENTS.md §6.4): `flow::second_live_permit` (`secure` for an
  invocation that already has a live permit), `flow::lock_released_by_non_holder` (`release_all`
  releasing a VO lock while the invocation still has another live permit) — the
  `ResourceManager::live_permits` ledger; chaos runs enable `panic`.
  Core: none (the scheduler's shell); M4 `ONE_PERMIT`, `VO_EXCLUSIVE` (flag `granted_out_of_ledger`,
  configuration `SIGNAL`; `ONE_PERMIT` is red even in `WAKES` without signals).
  Tests: `crates/cairn-server/src/flow/tests/ticket.rs::{a_signal_adopting_a_granted_ticket_takes_its_permit,
  a_new_ticket_adopting_a_granted_ticket_takes_its_permit}`,
  `crates/cairn-model/tests/m4.rs::{signal_to_a_granted_ticket_holds_every_property,
  granted_out_of_ledger_breaks_one_permit_without_signals}`.

## 2. Files to read, in order

0. `crates/cairn-lifecycle` — **the lifecycle core**: `step(view, event) -> Plan` is the only
   state-transition table of an invocation (retry/timer ticks, the drainer's "write then
   dispatch", the pre/post-permit gates, the `Running` commit, preflight, turn end, errors
   before/after `Running`, leader change, `recover`, the rescue net, admin); `valid(view)` is the
   set of valid durable states; `codec` is the only encoding of retry rows; `drain` holds the
   outbox drainer's decisions and 202/429. Row-by-row tables live in `src/tests/`; M4
   (`crates/cairn-model/src/m4.rs`) checks `step` itself under interleaving, throttling, crashes
   and leader changes. Shell: `crates/cairn-server/src/turn/life.rs` — `Asker` (reads the `View`
   by `Need`), `commit` (`Engine::apply_lifecycle`: stage + retry row in one commit), `side` (the
   `Side` effects: sealing the invocation's streams), `attendance`/`claim` (the **one** "busy"
   question).
1. `crates/cairn-server/src/lease.rs`: the four rules, the table of engine release points,
   `Released`, `Lease::{released, wait, sync, next_io}`, `IoSet`, `Locks::try_invocation`.
2. `crates/cairn-server/src/flow/`: `mod.rs` — the header doc (three resources, why two phases,
   core and shell), `ResourceKind`, `Counter`; `resource.rs` — `Permit`,
   `ResourceManager::{poll_acquire, turn_room, release_running, release_all}`, `WaitList`;
   `decide.rs` — `Core::{request, cancel, wake, wake_next, dispatch, serve_queue}`, `Actions`,
   `Claim`, `Core::withdraw`; `scheduler.rs` — `Scheduler::{acquire, deliver}`; `ticket.rs` —
   parked tickets of background turns (FL-30): `Scheduler::{park, adopt, claim, nack, lock_freed}`.
   Tests: `tests/` (`mod.rs`, `wake.rs`, `rate.rs`). Sim: `crates/cairn-server/src/flow/sim/`
   (`mod.rs` — types, tests; `events.rs`; `invariants.rs`).
3. `crates/cairn-server/src/engine_pool.rs`: `TurnBudget`, `NodeTurns`, `Leading`.
   `crates/cairn-server/src/rate.rs`: `TenantBook`, `TenantMeter` (bucket + `vt`), `Demand`,
   `spawn_peer_count`.
4. `crates/cairn-server/src/turn/`: `permit.rs` (`acquire_turn`, `acquire_turn_background`,
   `begin_turn`, `end_turn`, `end_turn_with`, `park_for_retry`, `queued_slot`), `drive.rs`
   (`TurnPermit`, `drive_untraced` — the `Event::Drive` gate, `drive_with_permit`,
   `write_gate_wake`; background turns: `drive_dispatched` — gate then park a ticket,
   `drive_granted`), `retry.rs` (`fire_retries`, `failed_before_running`, `failed_after_running`),
   `timers.rs` (`tick`), `mod.rs` (`awaiting_sdk`).
5. `crates/cairn-server/src/actor/`: building the `Scheduler` in `leadership.rs::leadership_loop`,
   `background.rs::{restore_flow, recover, rescue_round}` (core `LeaderUp`, `Recover`,
   `RescueRound`), the `budget_changes` branch of `serve.rs::serve`.
6. `crates/cairn-server/src/drainer.rs`: one drainer per range; `Runner` dispatches work rather
   than running turns — a bounded admission gate (`ADMIT_CAP`, `Admit`), the serving loop spawns the
   tasks passing the gate (`actor::run_dispatched`) and the turn tasks of granted tickets
   (`actor::run_granted`); `Rescue` — the term's rescue net (FL-16), whose loop is
   `actor::{rescue_loop, rescue_round}`; `WakeDuty` — the default nack of every turn.
7. `crates/cairn-server/src/l0.rs`: the L0 throttle — `reject_probability`, `Hold` (hysteresis on
   the way down), `Throttle::{admit_ingress, defer_inbound, defer_dispatch, begin_pass, tick}`; the
   three throttle checks are asked only in `turn/admit.rs` (the shared admission path —
   `admit::Source` says which throttle applies, before or after looking up existing work), plus the
   `l0_tick` branch of `serve`.
8. `crates/cairn-server/src/quarantine.rs`: poison-handler quarantine — the object
   `quarantine/<tenant>`, `Quarantine::{detect, clear, refresh}` (a background task of the node),
   `RangeView::{held, cooled}` (the engine thread); called from `turn::admit` (`start_invocation`,
   `hold`) and `turn::cool_key` → `Scheduler::acquire_cooled` → the `ResourceKind::Cooldown` tier
   of `poll_acquire` (`cooldown`).
9. Background: [`docs/architecture.md`](../architecture.md) for where flow control sits in the
   turn path.

## 3. Impact matrix

| Change | Then look at / run |
|---|---|
| `flow/` (including `flow/sim/`) | `cargo test -p cairn-server --lib flow::` (includes the sim, 400 seeds) + `cargo test -p cairn-server --test flow_control --test concurrency --test stale_state`. When changing the core, add one long run `CAIRN_FLOW_SIM_SEEDS=10000 cargo test -p cairn-server --lib flow::sim` (~1.5 minutes); one seed: `CAIRN_FLOW_SIM_SEED=<s>` |
| `engine_pool.rs` (`TurnBudget`, `NodeTurns`) | FL-7. `cargo test -p cairn-server --lib -- flow:: engine_pool::` + `--test split` (`max_concurrent_turns_is_per_node`) |
| `rate.rs`, the rate tier of `poll_acquire`, `settle` | FL-12. `cargo test -p cairn-server --lib -- flow:: rate::` + `--test tenant_rate` |
| `FairQueue`, `vt`, the `vt` borrowing rule, the sweep tick | FL-13, FL-7. `cargo test -p cairn-server --lib -- flow:: rate:: engine_pool::` + `--test tenant_rate --test split` |
| `turn/` (the commit path, `begin_turn`/`end_turn*`, `acquire_turn*`) | Per AGENTS.md §2: `split_crash_at_every_step`, `shared_wal_survives_crash_at_every_step`, `tests/durability.rs`. Plus `--test flow_control --test concurrency` |
| `lease.rs` (release points, `sync`, `IoSet`), the outbox drainer (`turn/outbox.rs`: `Sender`, `Held`; the `drainer::Backlog` ledger), the release point between two chunks of a drain pass (`turn::{between_chunks, chunk_full, DRAIN_HOLD_BUDGET}`, `Reread::release`) | FL-1, FL-2, FL-15, FL-29. A new release point: `--test recovery_storm` + the M4 shell (`Action::Drain`) and `Driver::drain_chunk` of `range_model.rs`. `cargo test -p cairn-server --features testkit --test dead_destination --test net_partition` + `cargo test -p cairn-server --test interrange --test wrong_range --test single_reader` + `cargo test -p cairn-server --test transport_engine --test concurrency --test durability --test single_reader` + `cargo test --workspace` + 4-partition cluster e2e with both WALs |
| `begin_turn`/`end_turn`, watch rows, `due_floors` | FL-17, FL-18. `cargo test -p cairn-engine --test outbox_seq` + `cargo test -p cairn-server --features failpoints --test failpoints` + the `turn/` row of AGENTS.md §2 |
| `drainer::Rescue`, `WakeDuty`, `rescue_loop`/`rescue_round`, the `ack()` sites of the wake duty | FL-16. `cargo test -p cairn-server --lib drainer::` + `cargo test -p cairn-server --features failpoints --test failpoints` + the `turn/` row of AGENTS.md §2 |
| `crates/cairn-lifecycle` (the core), M4, `turn/life.rs` (the shell: `Asker`, `commit`, `attendance`), source check 5 (`scripts/lint-lifecycle.allow`) | Per AGENTS.md §2 (the lifecycle core row): `cargo test -p cairn-lifecycle` + `cargo test -p cairn-model --test m4` + `cargo test -p cairn-engine --test retry_codec --test range_model` — every new branch gets a table row; mutating a branch must turn the table red. The shell reads by the core's `Need`: adding a "not read" slot must not change the order or number of store reads of an existing branch. Plus the `turn/` row of AGENTS.md §2 |
| the engine lock `lease::EngineLock` (`Precedence`, `YIELD_MAX`, `BACKGROUND`, `opening_job`, `command_waiting`) | FL-31, FL-8, FL-29. `cargo test -p cairn-server --lib lease:: --test recovery_storm --test flow_control --test failover --test liveness` + the `actor/` row of AGENTS.md §2 (`cargo test --workspace`, 4-partition cluster e2e) + the takeover benchmark (`recovery_storm.rs::takeover_with_large_history`, release). A new `holding_as` label for a background task ⇒ decide whether it belongs in `BACKGROUND` |
| parked tickets (`flow/ticket.rs`, `Core::withdraw`), `turn::{drive_dispatched, drive_granted}`, `actor::{run_dispatched, run_granted}`, the admission gate (`drainer::ADMIT_CAP`), the ticket/gate branches of `serve` | FL-30. The lifecycle core row of AGENTS.md §2 (M4 `Pc::Parked`; changing the event order means fixing the M4 shell and `Driver::run_parked` of `range_model.rs`) + `cargo test -p cairn-server --lib flow:: actor:: --test flow_control --test lifecycle --test tenant_state --test tenant_rate` + the permit-queue benchmark (`arch_bench.rs::permit_queue_churn`, release) |
| `restore_flow`, `reset`, `restore_lock`, the grant gate (`Scheduler::{close_gate, open_gate}`), `Engine::{restore_chunk, finish_restore}`, the `floors::QUEUE_HIGH` mark | FL-8. `--test flow_control` (the three recovery tests of FL-8 and FL-4) + `--test recovery_storm` + `cargo test -p cairn-server --features failpoints --test failpoints restore` + `cargo test -p cairn-model --test m4` (the M4 shell: `RestoreOne`, the gate) + `cargo test -p cairn-engine --test range_model --test progress_floors` |
| The rule book (`set_rules`, the `rules` object) | A strictly read object (AGENTS.md rule 2): changing its layout changes the header. `flow_control.rs::changing_rule_under_load` + `--test format_fixture` |
| Adding a function that touches the network | It must take `&Released`; an external effect also takes `&Committed`. No test can replace this rule: the compiler is the test |
| `l0.rs`, the `ctx.l0` call sites, `ActorError::Overloaded` | FL-11. `cargo test -p cairn-server --lib l0:: --test l0_throttle` |
| `quarantine.rs`, `hold` in `turn::admit` (`start_invocation`), `CreateOptions.paused`, the `Cooldown` tier of `poll_acquire` | FL-14, FL-3. `cargo test -p cairn-server --lib -- quarantine:: flow:: --test quarantine` + `--test format_fixture` (new object) |
| SDK commands by id (`task_attach`, `task_peek_output`, `task_send_signal`, `outbox_attach`/`outbox_peek`, `inbound_attach`/`inbound_peek`, `settle_signal_local`), `Watch`/`DeliverSignal` of `interpartition.rs` | FL-25. `cargo test -p cairn-server --test tenant_isolation --test interrange --test transport_engine` + `cargo test -p cairn-engine --lib interpartition` + the `turn/` row of AGENTS.md §2 |
| tenant state: `state=` in `tenants.rs`, `Admission::Refused`, the `Tenant` tier of `poll_acquire`, `Scheduler::sync_tenants`, skipped schedule fires | FL-24. `cargo test -p cairn-server --lib -- flow:: tenants:: --test tenant_state --test schedules` + the lifecycle core row (`throttle_pushes_due_of_planned_retry`) |
| `seq_rows_from`, `SeqFloor`, `Mutations::{put, delete}`, `commit` (cairn-store `writer/`: `read.rs`, `scan.rs`, `mutations.rs`, `mod.rs`) | FL-15. `cargo test -p cairn-engine --test outbox_seq --test derived_rebuild` + `cargo test -p cairn-store` + the `turn/` row of AGENTS.md §2 |
| Durable progress floors (`writer/floors.rs`, `due_floors`, `commit`, `load_floors`, `Index::Floors`), due-time scans, `seq_heads`, `pending_retries_among` | FL-18, FL-15, FL-17. `cargo test -p cairn-engine --test progress_floors --test outbox_seq --test derived_rebuild` + `cargo test -p cairn-store --lib writer::` + `cargo test -p cairn-server --test format_fixture --test upgrade` + the `turn/` row of AGENTS.md §2 |
| The become-leader path (`restore_flow`, `recover`, `scan_open`, `end_opening`) | `cargo test -p cairn-server --test open_cost --test recovery_storm --test retry_attempts` |
| `RangeStore` (`writer/store.rs`), the `testkit::range_model` model, the `Driver` of `range_model.rs` (a thin shell over the lifecycle core itself: throttling, busy, infrastructure errors, `maxAttempts`, the attempt-count check, `valid` after every step) | FL-17, FL-18, FL-19. `cargo test -p cairn-store --test range_store` + `cargo test -p cairn-engine --test range_model` (more seeds: `CAIRN_MODEL_SEEDS=3000`). The `Driver` calls `cairn_lifecycle::step` as the `turn/life.rs` shell does — changing the shell's event order means fixing the `Driver` to match |
| Adding a `failpoint!` on the turn path | Never inside an engine-holding region (AGENTS.md §5). `cargo test -p cairn-store -p cairn-server --features failpoints --test failpoints` |

## 4. Review checklist

- [ ] Every new wait (permit, lock, durability, SDK, network) goes through
  `Lease::wait`/`released`/`sync` (or `next_io` for `IoSet` work), with no bare `.await` while
  holding a `Lease`.
- [ ] No `borrow_mut()` of `Scheduler`/`Locks` is held across an `await`.
- [ ] A new path that requests a permit for an invocation goes through
  `Scheduler::{park, acquire_cooled}` (which withdraws its parked ticket first — one entry per
  invocation, FL-30), from a task that **already holds** the invocation lock; no task holds a permit
  and then waits for the invocation lock.
- [ ] A new cross-range send path goes through `Sender` (one `sync` per round, one task per
  destination) and `Transport::send_batch`; a row is deleted only on `Delivery::Acked`. Do not send
  messages one by one inside the drain loop (except `drain_inbound`'s `PeekOutput` replies, not yet
  batched).
- [ ] Every exit path of a turn consumes the permit explicitly through
  `turn::end_turn*`/`begin_turn` (following `Plan::ram.release`) or `TurnPermit`.
- [ ] A permit just received goes straight into `TurnPermit::new` (i.e. `Permit::begin`). A place
  that receives a permit without running the turn at once lets it release itself by dropping it
  before `begin`, rather than calling `begin` and then abandoning it.
- [ ] The run's CU is settled (`TurnPermit::settle_cu`) **before** the permit is released. No live
  `Permit` is dropped (`debug_assert` red, `error!` in production).
- [ ] Only terminal paths call `release_all`.
- [ ] A new task that runs turns holds a `WakeDuty` for the id (created **before** the spawn);
  `ack()` only where the invocation already has a durable wake or no longer needs one — never ack
  "to be safe" on an error path (FL-16). The `Drop` of a new guard on the turn path touches RAM
  only.
- [ ] A new branch on lifecycle state goes into the core (`cairn_lifecycle::step`), with a table row
  in `cairn-lifecycle/src/tests/` — never branch in the shell. Writing status, stage or retry rows
  goes only through the executors of `turn/life.rs` (source check 5); a new "busy" question asks
  `turn::attendance`/`claim`, not `Locks`/`Runner` directly.
- [ ] `poll_acquire` still has no side effects; counters only increase in `secure`.
- [ ] No loop over the whole wait queue on the permit release or request path.
- [ ] Background paths only `try_invocation`, never stand waiting for another turn's invocation
  lock (rule 4 of `lease.rs`).
- [ ] A background turn gives up only when blocked **by a counter** (`acquire_turn_background`).
- [ ] Any decision that crosses an engine release point is re-checked (the core's gates:
  `Event::Drive`, `Event::PermitGranted`).
- [ ] A new drainer loop (or background work walking a backlog) releases the engine between two
  chunks (FL-29): the release point lies between two rows, and rows read but not yet considered do
  not cross it.
- [ ] A new path that creates work goes through `turn::admit::admit` with a `Source` (a new variant
  for a new source — the doc of `Source` says how the L0 throttle applies); `start_invocation` and
  `Engine::create` outside `turn/admit.rs` FAIL in `scripts/lint-sources.sh` check 6. Paths that
  drain existing work (completion, timer, retry, signal, reply) do **not**.
- [ ] Quarantine (FL-14) adds no path that fails/cancels; a new work-creating path goes through
  `admit` (so `hold` applies), and the turn path does not read the node's `Quarantine` directly
  (a lock) but through `RangeView`.
- [ ] A new path that deletes outbox rows goes through `Engine::stage_outbox_removal` (which also
  removes the D-O2 entry), and a new path issuing outbox `seq`s goes through `next_message_seq`: the
  drainer's ledger (`drainer::Backlog`, FL-15) relies on a vanished row never coming back at the
  same `seq` and on new rows always carrying a `seq` greater than every `seq` issued.
- [ ] A new path writing ordered index rows (outbox, signal, inbound) or due-time rows (timer,
  retry, purge, delayed) goes through `Mutations` and `commit` — never straight into the Db — so
  the scan floor sees it and the durable floor is lowered in the same commit (FL-15, FL-18). A new
  due-time index must have a key starting with `due` and be listed in `floors::DUE_INDEXES`, or
  have no floor.
- [ ] The `Core` calls no `Waker`, channel or tokio: a new event is a `Core` function that takes
  `now` and returns `Actions`; work with external effects (sending into a channel, `Notify`, the
  node's watch) is done in `Scheduler::deliver`. A new event is added to the sim (`flow/sim/`,
  `Ev`) together with checks for the invariants it touches.
- [ ] Time enters the core as a parameter: `ResourceManager`/`Core` take `now`, the `Scheduler`
  shell reads the clock once per call; a turn's wall clock is read from `engine.clock()`, never by
  calling `entropy::now`/`SystemTime` directly. Timed waits go through `cairn_store::pace` (no bare
  `sleep`); a new `select!` is `biased` with the reason for its order.
- [ ] A new path that changes an invocation's journal **outside** `Engine::step`/`deliver` (writing
  entry 0, deleting the journal, copying the journal) removes that id's warm machine
  (`Warm::forget`, FL-22); a new path that uses a caller's machine without running a turn goes
  through `settle_machine` + `keep_warm`, and does not keep an `Invocation` across an engine release
  point.
- [ ] No new unbounded Db scan while holding the engine: use the reader's bounded variants
  (`limit`, cursors, `partial`) and release the engine between two chunks when a whole O(history)
  index must be walked (`Engine::runnable_chunk`). Calling `…_unbounded(` in `turn/`, `actor/`,
  `flow/`, `cairn-engine/src/engine/` FAILs in `scripts/lint-sources.sh` check 11 except for
  `scripts/lint-scan.allow` — a new line states what the scan's size is bounded by.
- [ ] Changing an FL-x invariant means fixing this guide in the same commit.

## 5. Common failure modes

- **Two ranges send to each other at the same time and wait for each other forever.** Sending an
  inter-partition message while holding the engine: `xcall` stuck at a concurrency of 2. Now a
  compile error (D16). Test: `transport_engine.rs`.
- **A ticket granted wrongly.** The old version *took away* the pending handle; a task arriving
  later saw an empty slot and got a ticket, so `status` answered after 1 ms on undurable data. See
  FL-2.
- **A wait queue full of dead entries** (found by a billing workload). A hot object with about
  6 000 commands, tasks leaving and requesting again every `queue_wait`: 300 000 entries, every
  commit of the range waited 11–15 s. Background turns giving up on the VO lock produced 100–180
  commits/s. Tests:
  `flow/tests/mod.rs::abandoned_request_leaves_no_dead_entry`,
  `flow_control.rs::background_turn_waiting_for_object_lock_does_not_give_up_after_wait_cap`.
- **Lost updates from releasing the VO lock early.** Two `recordUsage` calls both read
  `apiCalls = 5030` under chaos. See FL-4.
- **A wedged queue.** `recover` dispatched by id, not by `seq`; a later arrival with a small `seq`
  jumped ahead of the lock holder, and 3 864 commands stood still. See FL-5.
- **Throughput collapsed after the fix that counts attempts only at `Running`** (a 60-minute chaos
  run of a billing workload). Attempts were counted only at `Running` but the backoff step still
  read the attempt count, so a permit-waiting turn cancelled on an owner change did not push the
  next fire further out; the wait queue grew linearly until no term could drain it before the next
  kill. A scheduler snapshot looked like a lock held with nobody running, but the range was still
  progressing — each permit-waiting turn took ~17 minutes. See FL-21.
- **Attempts exhausted without the handler running again** (the same billing workload). After a
  failover, a tenant with a `rate` was throttled; 19 scheduled fires were all `Throttled`, each one
  spending an attempt, then `paused` 575 "all 20 attempts used". The payment gateway received no
  call, and the logs were silent. The first fix only covered turns returning `Throttled`; in a later
  run (split and balancing moving ranges between owners constantly) a retry turn cancelled while
  waiting for a permit still spent one attempt per owner change. See FL-19.
- **A busy range crawls while the node is idle.** A hard even split: 93 996 commands waiting, ~3
  turns/s. The first fix used an accumulating counter that leaked after every owner change. See
  FL-7.
- **A noisy tenant on many objects jumps ahead of a quiet tenant.** The `Turns` wait list was FIFO
  per queue: tenant A spread over 20 objects is 20 queues, tenant B one queue — B waited ~10 turns
  (1.1–1.3 s in the acceptance test). See FL-13.
- **One core at 100% when releasing permits.** Every `Turns` waiter was woken: about 10⁹
  "blocked" results after 11 minutes at 100k invocations. See FL-9.
- **A late completion reruns a finished invocation.** See FL-10.
- **The queue table grows with the number of objects ever run** (found by the flow sim). A quantum
  of 1 made `serve_queue` return after granting the last waiter, before the "empty head, drop it
  from the table" branch: every object key ever run stayed until the end of the term. Same shape: 33
  waiters on one object all leave, and the cleanup that empties the queue leaves the queue behind.
  See FL-6.
- **The throttle wedges itself.** Blocking completions or timers while L0 is high means unfinished
  work never finishes, nobody drains the backlog, and L0 never drops. Returning 429 at
  `/internal/inbound` makes the sender treat the whole range as `down`, and every completion to it
  gets stuck too. See FL-11.
- **`cooldown` blocks the head of a shared queue.** The queue of a SHARED handler groups every
  handler with the same `(tenant, scope, limit key, service)`: a turn of a `cooldown` identity at the
  head of the queue blocks other handlers of the same service behind it until that identity's
  running turn is done (≤ `sdk_timeout`). That is the price of a tier in `poll_acquire` (which only
  looks at the head of the queue); `hold` does not have it. See FL-14.
- **Quarantine causing its own incident.** Quarantining a healthy handler by mistake blocks real
  customer work, so the default is only `observe`, the rule demands 0 successes, and `hold` never
  cancels. A manual clear that the detector puts right back within a second (the numbers in the
  window have not changed yet) is why `cleared` exists. See FL-14.
- **A hung handler holds a permit forever.** Without `sdk_timeout` the cap drains away and the
  range stands still. See FL-6.
- **A permit dropped while stepping down** (found under `s3_slowdown`). `Lease::wait` took the
  permit from the inner future and only then waited to win the engine back; `JoinSet::shutdown` cut
  the task there and the live permit was dropped — a `debug_assert` panic on the engine thread. See
  FL-6.
- **The drainer re-reads the rows of a busy parent.** Every drain pass read the outbox from `seq`
  0; a fan-out parent's `Call` rows must take the parent lock, so while the parent runs a turn all
  its rows stay behind, and the next pass reads the thousand rows again — 21–450 thousand reads for
  ~2 000 rows at a spread `fanout` of 1 000. See FL-15 (the `drainer::Backlog` ledger).
- **A hung destination slows down the sending range's own inbound.** A drainer went outbox →
  signal → inbound, waited for destinations still in flight up to `SEND_TIMEOUT` at the end of the
  outbox, and the `down` set lived for one pass only: every pass gave the same hung destination
  another 10 s, and completions and `StartCall`s to the sending range waited too (10.06 s measured
  in the test). See FL-15 (pass order, circuit breaker).

**The `// #[hot-path]` mark** (AGENTS.md §6). Files carrying the mark: every file of `turn/`, every
file of `flow/`, every file of `writer/` (cairn-store), `nodewal/stripe.rs`. In a marked file: no
`info` log on the ordinary path of a turn; no global lock; no synchronous I/O. Two known sites were
left in place when the mark was added: `tracing::info!` "run captured" (a rare event triggered by an
operator — allowed) and `tracing::info!` "the invocation's deployment is not loaded on this node
yet — rescheduled" (once per 1 s retry of an invocation while the deployment catalog has not
arrived — should be lowered to `debug`). A third — `NodeTurns` locking a node-wide `Mutex` every
time `turn_room` was called — has since been replaced by two atomics (FL-7). The node's tenant book
(`rate::TenantBook`) has a `Mutex`, but the turn path does not touch it: the scheduler keeps an
`Arc` copy of the meter and looks the book up only the first time it meets a tenant.
