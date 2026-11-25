// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! # M1 — lease, session, range table, opening a Db
//!
//! Extended for the control plane: `solo` mode ([`M1::solo`]), renewal
//! delegated to a coordinator ([`M1::delegation`], [`Hub`]), and a coordinator
//! that is itself a node with ranges, renewing passively with `solo`
//! ([`M1::hub_node`]). Configurations that check only leases switch split/merge
//! off ([`M1::lease`]). The protocol holes the model found are pinned by the
//! rows of `tests/m1.rs`.
//!
//! *The model is a hypothesis about the code, like a guide: it drifts. Changing
//! the lease protocol or the Db-open path means fixing the model in the same
//! commit (AGENTS.md §2).*
//!
//! N nodes (2 by default) compete to lead the ranges; one coordinator splits
//! (or merges) **once**: the source range leaves the table and retires, the
//! child ranges enter the table. The checker explores every interleaving by BFS
//! with small parameters (a saturating discrete clock, bounded incarnation and
//! death counts), so the state space is finite.
//!
//! ## State ↔ real objects
//!
//! | Model | Real |
//! |---|---|
//! | [`State::table`] ([`Table`]) | `cluster.conf` on the bucket (`cairn-store/src/cluster.rs`, `partition_table.rs`); version 1 → 2 (intent) → 3 (commit or roll-back) |
//! | [`Node::ram`] | the node's RAM table (`ClusterConfig::table`), only moves forward (`ClusterConfig::apply`) — may be stale |
//! | [`State::owners`] ([`Owner`]) | `owners/range-<id>` (`cairn-store/src/owner.rs`): session (node + incarnation), `released`, `retired` (tombstone). `term` dropped: a CAS comparing (node, inc) is enough at this level |
//! | [`State::sessions`] ([`Session`]) | `sessions/<node>/<inc>`: `inc`, `age` = clock ticks since the last renewal, `ended` = `expired`/`closed` (permanent, LS-1) |
//! | [`Session::quiet`] | ticks since the owner last wrote the record (beat increased) — what the tracker counts under LS-1 (`tracker.rs:observe`). Differs from `age` only when a renewal protocol that does not write is on ([`M1::solo`]) |
//! | [`Session::belief`] ([`Belief`]), [`Session::lost`] | what the owner **knows** (as opposed to the truth on the bucket): whether the `mode=delegated` CAS has been confirmed; whether it has called `lose` (past the pause cap — writes `closed` —, or a self-check GET saw the session ended) |
//! | [`Session::hb`] ([`Heartbeat`]) | a `POST /internal/heartbeat` in flight: age since its first send, the `term` the owner wrote, bucket-touch age < TTL/3. Only with [`M1::heartbeat_in_flight`] |
//! | [`Session::pending_expire`] | the coordinator's If-Match `expired` PUT whose outcome is ambiguous, which may still land (rule 7) until the record's version changes |
//! | [`Hub`] | the coordinator process: its session (`inc`, `ended` — always `direct`; with [`M1::hub_node`] a copy of node 0's session, synchronised after every step), its role in RAM (`role`, may be stale), `control/coordinator` (`term`, `released`; incarnation via [`Hub::terms`]) |
//! | [`Term::lost_age`] | when a term's role was lost (session ended, a later term, `released`) by the clock of the fastest observer — the reference point of the yield rule |
//! | [`View`] | the coordinator tracking one session: τ(N) (`tau`), the delegated version a GET confirmed (`held`; `Stale` = the record changed without it knowing — If-Match will miss), the `expired` CAS issued (`issued`, ambiguous ones too) |
//! | [`Session::mode`] ([`Mode`]) | the record's `mode=` line (read leniently; absent = `direct`); `Solo::challenge` = the `challenged=<node>/<inc>` line and its age counted from when the challenge CAS was confirmed (the challenger's τ) |
//! | [`Engine`] | one leadership of a range engine on a node (`actor/leadership.rs`): has won the lease, has opened the Db (writer), has quiesced (Db sealed, lease still held), has exited for good |
//! | [`Engine::Open::fenced`] | SlateDB's `writer_epoch` has been bumped by a later open (LS-4) |
//! | [`State::closed`] | the `closed` flag in the range Db's SlateDB manifest (SM-17; fork: `Admin::close_for_writes`/`reopen_for_writes`, `ManifestCore::closed`) |
//! | [`Node::reopen_pending`] | the node's `factory` has just CASed away a foreign intent in `split/rollback.rs:roll_back_matching` and has not yet run `reopen_sources` |
//! | [`Coord`] | the split/merge coordinator (`cairn-server/src/split/`, `RangeOps`) and its node's `ActiveIntents` (`active` = holds a guard, `marked` = SM-13's quiesced mark) |
//! | [`Ghost`] | ghost variables to state invariants (acks lost, child acked, …) — not in the code |
//!
//! ## Actions ↔ real functions
//!
//! | Action | `file:function` |
//! |---|---|
//! | [`Action::Tick`] | the shared clock advances one tick; grant validity = `validity` ticks, TTL = `ttl` ticks (1 and 2 by default: only `validity < ttl` matters) |
//! | [`Action::Renew`] | `cairn-store/src/session/renew.rs:keeper_loop`/`renew_once`; in time ⇒ renew; past due it steps itself down (paused), and past the pause cap or on a missed CAS ⇒ `lose` (writes `closed`); session already ended ⇒ `open_incarnation` |
//! | [`Action::RenewLate`] | `renew.rs:renew_once` → `adopt` → `writer/validity.rs:ValidityHandle::extend_until` while the grant is **paused** (past due, not past the cap): a CAS that wins from exactly the previous version ⇒ the deadline moves, the grant runs again, the engine does not step down. Fenced (`revive_by_cas`): only if the record has not been `expired`/overwritten (the CAS wins). The pause cap is only liveness: the model allows revival at any age — an over-approximation |
//! | [`Action::ObserveExpired`] | `cairn-store/src/session/tracker.rs:SessionTracker::observe` (CAS `expired` after a TTL of silence — [`Session::quiet`]). Only for `direct` records; `solo` records only when [`Fences::quiet_lease_gate`] is off (an observer running an old binary); `delegated coord=X:t` records by the **yield rule** (`tracker.rs:delegated_due`, branch `Due::ByYield`: the moment we see role `(X, t)` lost — `CoordKnowledge::{terms, released, ended}` from every read of `control/coordinator` (`coordinator.rs`) and every ack (`Session::heard_coordinator`) — is one TTL old **and** `Seen::since` is one TTL old) |
//! | [`Action::JudgeUnexpirable`] | `cairn-store/src/session/tracker.rs:SessionTracker::observe`, branch `expire_failures ≥ 2` after `UNEXPIRABLE_AFTER_TTLS` TTLs (the `expired` CAS keeps missing: a torn object or a stale read). Generated only with [`M1::torn_views`]: the observer sees the session through a torn/stale object, so it can fire **at any time** (it sees no beat). Fenced (`rearm`): overwrite the record unconditionally — the owner's next renewal misses its CAS ⇒ `lose` —, then wait one TTL and CAS `expired` as in [`Action::ObserveExpired`]. Unfenced (the historical behaviour): `dead_unexpirable` in RAM, and `claim` trusts it |
//! | [`Action::ExpireUnderLock`] | `cairn-store/src/session/under_lock.rs:expire_sessions_under_lock`, called from `node/start.rs:start` when `ObjectStores::exclusive_local` holds the local directory lock (`local.rs:DirLock`): CAS `expired` at once on every `live` session — the premise is that the owner is dead (the lock was released), not a TTL |
//! | [`Action::Crash`] | the process dies (`kill -9`): every engine, coordinator and `ActiveIntents` in RAM is lost; the session stops renewing |
//! | [`Action::FollowTable`] | `cairn-store/src/cluster.rs:ClusterConfig::follow` → a successful `reload` |
//! | [`Action::Claim`] | `cairn-server/src/node/spawner.rs:RangeSpawner::factory` (`cluster.contains` on the RAM table; `ObjectStores::adopt_unowned_db` — an existing Db with no owner record yet ⇒ `Create` a released record, which in the model ≡ an empty record) → `balance.rs:Balancer::acquire` → `cairn-store/src/session/claim.rs:Session::claim_with` (tombstone ⇒ `RangeRetired`; live owner ⇒ `Held`; CAS the owner record) |
//! | [`Action::ReloadForOpen`] | `node/spawner.rs:RangeSpawner::factory`: `ActiveIntents::wait_unquiesced` (SM-13) then `open_io(…, cluster.reload())`; `Failed` = the read failed after every retry |
//! | [`Action::OpenDb`] | `node/spawner.rs:RangeSpawner::factory`: `cluster.contains` a second time, `split/rollback.rs:roll_back` (an intent this node did not make — SM-3; a winning CAS stops at [`Node::reopen_pending`]), the tripwire `node::open::quiesced_source` (SM-13), `acquire_shared_with_hint` + `cairn-store/src/writer/mod.rs:PartitionWriter::open` (bumps `writer_epoch`; every range opens with the shared WAL). On a closed mark: SlateDB's `fence.rs` returns `ClosedForWrites` (checked in the same CAS as the epoch) ⇒ the open fails, then `node/spawner.rs:heal_stale_close` (`range_db_closed`, `cluster.reload()` — `Read` is the outcome of this read —, removes the mark with `reopen_range_db_at` against the mark read before the table, only if the range is in the table and there is no intent) |
//! | [`Action::WriterFenced`] | SlateDB: the writer's next manifest update (memtable flush, manifest poller) sees the `writer_epoch` that `close_for_writes` bumped ⇒ `Fenced`; the engine exits, the leadership loop wins it back |
//! | [`Action::Ack`] | `writer/mod.rs:PartitionWriter::commit` → `Pending::durable` → `alive` ⇒ `Committed` (LS-5, INV-5): one ack going out |
//! | [`Action::ServeRead`] | `cairn-server/src/actor/serve.rs:Shared::finish` → `lease.rs:Lease::sync_reply`: `alive` (the node's clock) then, with `read_fence`, the read fence (`cairn-store/src/session/fence.rs:confirm` — GET the incarnation's session record: answer only if it is still `live`). A **stale** read = answering while the range's owner record no longer belongs to the engine (someone else has claimed it, and may have written and acked) |
//! | [`Action::ReadStart`], [`Action::ReadTicket`], [`Action::ProbeSession`], [`Action::ReadReply`] | only with [`M1::split_reads`]: the same path as [`Action::ServeRead`] split into steps. `ReadStart` = the turn's reads (`actor/dispatch.rs:on_*` — `on_view` reads three things in one turn; the **first** read is the oldest, so the model needs only that one); `ReadTicket` = `lease.rs:Lease::sync_with` (`durability()` ⇒ `alive`, then the fence future is polled for the first time ⇒ the request enters the channel); `ProbeSession` = the GET of `session/fence.rs:read_fence_loop`/`confirm` — the loop drains **every** request in the channel **before** issuing the GET, so a GET covers only requests that arrived before it (one arriving while the GET is in flight waits for the next batch); `ReadReply` = `reply.send`. A winning renewal (`Renew`/`RenewLate`) is also proof in the model — the code does **not** use it yet (measured: ≤ 2.5 % of fence GETs had a renewal issued within 50 ms after them); the model allows more answers than the code, so it is still an over-approximation for safety |
//! | [`Action::Reconcile`] | `node/spawner.rs:reconcile` (the range left the RAM table **and** the table read from the bucket — `ClusterConfig::read_stored`; the model has only the table read from the bucket, so nothing changes) → `EngineHandle::retire` → `cairn-store/src/session/keeper.rs:LeaseKeeper::retire` (CAS the tombstone) |
//! | [`Action::RetireOrphan`] | `cairn-server/src/gc.rs:OrphanRetirer::run_once` (SM-11: out of the table, owner released or dead ⇒ CAS the tombstone on its behalf) |
//! | [`Action::IntentCas`] | `split/ops.rs:RangeOps::split_with`/`split/merge.rs:merge`: `ActiveIntents::hold` + `ClusterConfig::update` (the intent CAS — the decision point) |
//! | [`Action::MarkQuiesced`] | `split/active.rs:ActiveGuard::mark_quiesced` (SM-13, right before the quiesce command) |
//! | [`Action::Quiesce`] | `cairn-server/src/actor/handle.rs:EngineHandle::quiesce_held` (Db sealed, lease still held — LS-8); if the engine no longer leads it cannot fire, and the coordinator can only roll back |
//! | [`Action::CloseSources`] | `split/ops.rs:split_with`/`split/merge.rs:merge` after `ensure_intent_ours`, before clone/union: `cairn-store/src/config/range_db.rs:ObjectStores::close_range_db` (merge: both sources) — SM-17. Does not look at the table (the intent check comes earlier and is not atomic with the close) |
//! | [`Action::Clone`] | `split/steps.rs:ensure_intent_ours` + `ObjectStores::clone_range`/`union_ranges` (snapshot of the sources) |
//! | [`Action::ClaimChildren`] | `split/steps.rs:ensure_intent_ours` + `claim_children` (the children's owner records point at the node's current session) |
//! | [`Action::CommitCas`] | `split/ops.rs:split_after_intent`/`split/merge.rs:merge_after_intent`: `ClusterConfig::update(marked_version, …)` (the commit point) |
//! | [`Action::Retire`] | the coordinator's post-commit part: `handle.retire()` (tombstone) then the guard drops (removes `active`/`marked`) |
//! | [`Action::RollBack`] | `split/steps.rs:roll_back_here` → `roll_back_matching` (CAS dropping the intent); CAS won ⇒ [`Phase::RollingBack`] (closed marks still to remove); CAS lost ⇒ `Resign` the quiesced sources, the guard drops, the marks are **not** removed |
//! | [`Action::ReopenSources`] | `split/rollback.rs:reopen_sources` (`ObjectStores::reopen_range_db_at` for each source against the mark read before the CAS; failure or being fenced is only logged) after a winning roll-back CAS — the coordinator's (then `Resign`, the guard drops) or `factory`'s (then the open continues). A node that dies between the CAS and this step = it never runs |
//! | [`Action::EnterSolo`] | `cairn-store/src/session/renew.rs:keeper_loop` → `want_solo` (the tracker's `SessionTracker::alone` after a LIST since the incarnation opened — or `quiet_coordinator`: holds the role, `SessionTracker::all_delegated_to`, not draining, the gate with the delegation flag —, the `quiet_lease::Watch` gate) → `renew_once(…, SessionMode::Solo)` → `adopt` (`Current::solo` only when the record confirms it is exactly our `solo` version, with no `challenged` line — also through the re-read after an ambiguous CAS): CAS `mode=solo`, `beat+1` — a winning `direct` renewal (LS-2), after which renewal PUTs stop. The gate and "being the only session not yet ended" are only cost: the model allows entry at any time, on every node — an over-approximation |
//! | [`Action::SoloCheck`] | `renew.rs:self_check` every `renew_interval` (TTL/5), together with `fence.rs:confirm` (the read-fence loop can batch with it, via `renew.rs:solo_extend`): GET `If-None-Match` = the etag of the **confirmed** `solo` version, `304` or a `200` equal to `Current::written` ⇒ `extend_until(issue time + ⅔·TTL)`. Issue and serve are merged into one step (serving after the issue time only shortens the deadline); also proof for the read fence (`proof`). A revoked grant (after `lose`) or one past the cap does not move under `extend_until` — model: `own_cas_wins` |
//! | [`Action::SoloWrite`] | `renew.rs:keeper_loop` when `needs_write` (`owns`/`draining` changed while alone) → `renew_once(…, SessionMode::Solo)` from the etag of the confirmed `solo` version |
//! | [`Action::Challenge`] | `cairn-store/src/session/tracker.rs:SessionTracker::observe` (a `solo` record not yet ended, with no `challenged` line yet; unless our own record delegates to that very session — `TrackerInner::leaning_on`, which is only liveness: the model challenges at any time) → `SessionTracker::challenge`: `cas::put_resolving` If-Match the etag just read, content = the record verbatim + `challenged=<us>`; τ (`Seen::since`) = when the confirmed answer arrived (or the first read that sees a `challenged` line — ours or someone else's: a new version). The beat does not change. It is also `rearm`'s overwrite of a `solo` record (torn, CAS keeps missing): the overwrite **carries** our `challenged` line |
//! | [`Action::AnswerChallenge`] | `renew.rs:self_check` sees a `200` different from `Current::written` (a `challenged` line, an overwrite, undecodable — no renewal) → `answer` (failpoint `session::before_answer_challenge`) → `renew_once(…, SessionMode::Direct)` If-Match the new etag, `beat+1` on the highest beat seen: won = an LS-2 renewal (even while paused — a CAS from exactly the previous version, as in `RenewLate`); missed and the re-read sees the session ended ⇒ `lose`. It is also the way out after a [`Action::SoloWrite`] (or a CAS leaving the mode) misses because a challenge landed before it: `renew_once`, branch `Contested`, re-reads our record with the old beat carrying a `challenged` line ⇒ the same failpoint, CAS back to `direct` from the version just read on the next keeper round (before a chaos run found this, the code called `lose` there — a deviation from the model on liveness, not on safety). It is also "leaving solo mode" (`keeper_loop`: in `solo` while `want_solo` is false — it learned of another session via `SessionTracker::learn`/the safety-net LIST, or the gate is off) |
//! | [`Action::ExpireChallenged`] | `tracker.rs:SessionTracker::observe`, branch `SessionMode::Solo` with a `challenged` line: `Seen::since` (when the version with that line arrived) is ≥ TTL ago and the beat did not increase ⇒ CAS `expired` If-Match, then `claim` takes the range as today |
//! | [`Action::Heartbeat`] | `cairn-server/src/control.rs:Control::beat` — the node sends `POST /internal/heartbeat` via `ClusterSecret::send` every tick, `sent` taken **before** the first attempt; carries the beat/etag, `touched_ms`, `delegated`. Only with [`M1::heartbeat_in_flight`]; otherwise send-and-receive is merged into [`Action::HeartbeatAck`] |
//! | [`Action::HeartbeatAck`] | the protocol's `Ack(n)`, together with `ZombieAck` (an ack from a lost role: gets through only with the coordinator fence off). In code (`cairn-server/src/control.rs:Control::answer` → `lease_for`; fence `coordinator.rs:Session::coordinator_fence`; ack rules 4–5 `tracker.rs:SessionTracker::lease_view`; tripwire `coordinator::ack_after_expire`; the node applies the ack in `delegate.rs:Session::on_lease_answer`): the coordinator receives a heartbeat (τ(N) = when an **acked** heartbeat arrived — `cairn-server/src/control.rs:answer` → `SessionTracker::heard` only for an `Ack` answer), GETs to confirm the delegated version if it does not hold it, fences (ack rules 2–3, [`Fences::coordinator_fence`]), ack rules 5–6, then returns `granted`; the node applies the ack only if its delegation CAS has been confirmed and it has not called `lose`, deadline = heartbeat issue time + validity. Ack rule 1 ("C has not lost") is folded into the fence; ack rule 4 is not modelled (see below) |
//! | [`Action::Delegate`] | `cairn-store/src/session/renew.rs:keeper_loop` (every CAS that changes the mode goes through the keeper, sequentially) → `delegate::want_delegate` (a `Grant` answer fresh within TTL/3 from a coordinator other than us, not `solo`, outside the re-delegation delay, grant valid) → `delegate_once` (failpoint `session::before_delegate_cas`) → `renew_once(…, SessionMode::Delegated)`, `beat+1`, with the term from the grant — which may already have released/lost the role (a late answer: an over-approximation). Won = a `direct` renewal, `adopt` sets `Current::delegated`; ambiguous ⇒ [`Belief::Unsure`] until a re-read (`adopt` with the record read). It is also "re-delegating" to a new term (`renew.rs:delegated_round`) |
//! | [`Action::Fallback`] | the "fallback path": `renew.rs:delegated_round` — the reference `Delegation::base` (the CAS into the mode, or the send time of the last ack) + TTL/3 + jitter, or a `Fallback`/`Expired` answer — failpoint `session::before_fallback_cas`, `renew_once(…, Direct)`, `beat+1`: a winning `direct` renewal. The jitter, the TTL/3 reference and the `REDELEGATE_AFTER_TTLS` delay before re-delegating are liveness: the model falls back at any time |
//! | [`Action::SelfCheck`] | `cairn-store/src/session/delegate.rs:delegated_check` every tick, together with `fence.rs:confirm` (the read-fence loop can batch with it, via `delegate::touch`): GET `If-None-Match` on the node's own record — touches the bucket (`Current::touched_at`), sees it ended or changed by someone else ⇒ `lose`. An ambiguous delegation CAS is resolved by `renew_once`'s re-read; `delegated_check` also resolves our own ambiguous CAS (fallback, mode keep, re-delegation — `unconfirmed`, same beat) when the GET sees it landed: `adopt` as in the re-read branch (a chaos run: an ack moved the fallback reference while an ambiguous fallback CAS was in flight — before that the code called `lose` here, a deviation from the model on liveness, not on safety: the model merges an ambiguous fallback CAS that landed into a winning [`Action::Fallback`], and `SelfCheck` takes the record read as [`Belief::Unsure`]). No renewal (unless it adopts an ambiguous fallback/re-delegation CAS — that is the LS-2 renewal of that CAS itself, referenced at its earliest issue). The protocol's `BucketCut(n)` = a node that can take no bucket step (no `Renew`/`Delegate`/`Fallback`/`SelfCheck`) — BFS already covers every such schedule, no separate action needed |
//! | [`Action::RecordWrite`] | `renew.rs:delegated_round` when `needs_write` (`owns`, `draining`) → `renew_once(…, Delegated)` with the same role, `beat+1`; `adopt` sees the same role and does **not** renew the grant |
//! | [`Action::CoordinatorExpire`] | expire rules 1–4: `tracker.rs:SessionTracker::observe` on the coordinator, `delegated_due` branch `Due::ByCoordinator` — a `delegated` record for exactly `(us, the term we hold)` (`CoordKnowledge::mine`), τ = `max(Seen::since, Seen::heard)` one TTL old, not currently weak (`weak_since`, deferral capped at one TTL) ⇒ `expire_issued` then (failpoint `coordinator::before_expire`) CAS `expired` If-Match the version just read; a miss ⇒ GET again next round (beat increased ⇒ a new `since`). GET failed ⇒ `expire_held` CAS If-Match the version held (`Seen::version`). Ambiguous outcome ⇒ [`Action::ExpireLands`]. The model's "old role" (zombie) ⊇ the code: the code applies this rule only while it believes it holds the role |
//! | [`Action::CoordinatorRead`] | `tracker.rs:SessionTracker::observe` on the coordinator reads a `delegated coord=<us>:<our term>` record (the tracker round when heartbeats stopped arriving, or `SessionTracker::confirm` when a heartbeat reports a version the tracker does not have): that version becomes the delegated version (`Seen::etag`/`best_beat`); a new beat ⇒ τ = when it arrived (`Seen::since`) |
//! | [`Action::ExpireLands`] | an ambiguous conditional PUT landing late (object_store retries 5xx — rule 7) |
//! | [`Action::CoordinatorTakeover`] | `cairn-store/src/session/coordinator.rs:Session::try_coordinate` (`cas::put_resolving`, term + 1) when the record is released or points at an ended session; a node that is delegating does not take over (`control.rs:run`). "A node that just released does not take back within 3·TTL" is liveness |
//! | [`Action::CoordinatorRelease`] | `coordinator.rs:Session::release_coordinator` — CAS `control/coordinator` to `released` (graceful shutdown in `Session::close`; weak for over a TTL in `control.rs:run`; a clock running at the wrong rate — `clock_rate.rs` and `control.rs:run`) and drop the role. The model lets it fire at any time, so the release for the clock needs no separate action; yielding the ranges then is the balancer's ordinary yield path |
//! | [`Action::CoordinatorLose`], [`Action::CoordinatorReopen`] | the coordinator's session ends (`lose`, or an observer expires it after every node fell back to `direct`) and a new incarnation opens; the role in RAM is **not** lost by itself — a zombie. Not generated with [`M1::hub_node`]: node 0's session takes the node's path (`Renew`, challenger, observer) |
//!
//! ## Fences ([`Fences`] flags)
//!
//! - `fresh_table_before_open` — LS-12, RD1: if loading the table fails, **do
//!   not open**. Off = bug RD1: the read error is swallowed, the open follows
//!   the stale RAM table.
//! - `wait_unquiesced` — SM-13, RD2: `factory` waits for the quiesce mark to be
//!   removed, and a tripwire sits right before the open. Off = bug RD2: the
//!   open path does not know the source is quiescing.
//! - `owner_cas` — LS-3: `claim` wins only if the owner record is empty,
//!   released, this node's, or the owner's session is dead. Off = anyone can
//!   claim (the tombstone still blocks — LS-8 is part of `claim`, not a flag).
//! - `roll_back_foreign_intent` — SM-3: an opener that sees an intent its own
//!   node's coordinator did not make ⇒ CASes the intent away before opening.
//! - `writer_epoch` — LS-4: opening the Db fences the old writer; a fenced
//!   writer cannot ack.
//! - `closed_flag` — SM-17: the coordinator writes a closed mark into the
//!   source's manifest before the clone; SlateDB refuses every writer opening a
//!   Db that carries it, whatever the opener's table. Commit ⇒ the mark stays;
//!   roll-back ⇒ CAS then remove it (two steps). Off = no mark
//!   ([`Action::CloseSources`] only changes the phase).
//! - `unexpirable_rearm` — judging a session "dead" after
//!   `UNEXPIRABLE_AFTER_TTLS` TTLs without being able to CAS its record leads to
//!   `claim` only through a fence on the bucket — overwrite the record (if the
//!   owner is still alive its next renewal misses ⇒ `lose`), wait one TTL
//!   counted from the overwrite, then win the `expired` CAS. Off = the historical
//!   bug: the judgment lives only in the observer's RAM, the owner can still
//!   renew; `claim` trusts it ⇒ two leaders (LS-3). Meaningful only with
//!   [`M1::torn_views`] on.
//! - `read_fence` — an answer at the leader goes out only when a GET of the
//!   session record (issued after the read) sees the incarnation still `live`.
//!   With `validity < ttl` it is redundant (with a correct clock `alive` is
//!   enough); it is the fence when the node's clock lies (`validity > ttl`: a
//!   sleeping machine, a paused VM, a slow clock). Off + `validity > ttl` =
//!   stale reads.
//! - `proof_after_read` — with [`M1::split_reads`], proof (a fence GET, a
//!   winning renewal CAS) counts for an answer only if it was **issued after**
//!   that answer's ticket (the ticket is taken after the read). Off = reuse
//!   proof that already existed — issued before the read — and with a lying
//!   clock that is a stale read
//!   (`proof_issued_before_the_read_serves_a_stale_read`). In the code this is
//!   the batching rule of `fence.rs`: a request that arrives while a GET is in
//!   flight is not answered by that GET.
//! - `revive_by_cas` — a paused grant revives only when a renewal CAS wins
//!   from exactly the previous version — that is, the session record has not
//!   been CASed to `expired` or overwritten by an observer. Off = revival by an
//!   unconditional write (or by trusting the clock): a session already marked
//!   comes back to life while another node has taken the range ⇒ LS-3 breaks.
//! - `solo_extend_needs_unchallenged_read`, `expire_solo_needs_challenge_then_ttl`,
//!   `quiet_lease_gate` — `solo` mode (LS-16, LS-16a): the self-check GET
//!   renews only while unchallenged; a newcomer expires only after a
//!   challenge plus one TTL counted from the challenge; no old binary applies LS-1 to a
//!   `solo` record. Each flag has a row breaking LS-3 in `tests/m1.rs`
//!   (`solo_extend_on_challenged_read_breaks_ls3`,
//!   `expire_solo_without_challenge_breaks_ls3`, `old_observer_vs_solo_breaks_ls3`);
//!   `SINGLE_WRITER` and `NO_STALE_READ` hold in all three.
//! - `ack_needs_confirmed_delegation`, `no_ack_after_expire`,
//!   `expire_if_match_delegated`, `coordinator_fence` (with its two sub-parts
//!   `fence_checks_incarnation`, `ack_checks_heartbeat_term`),
//!   `observer_defers_to_coordinator`, `delegation_gate` — delegated renewal
//!   (LS-14, LS-15): each flag has a row breaking LS-3 in `tests/m1.rs`,
//!   `SINGLE_WRITER` and `NO_STALE_READ` hold in every row.
//! - `ack_respects_pause_cap` — "an ack does not revive a grant past the pause
//!   cap". A finding of the model: in `direct` the cap is only liveness, in
//!   delegated mode it is a safety fence (`lose` writes `closed`, someone else
//!   takes the range, then a late ack revives the grant).
//! - `yield_restarts_on_write` — **proposed by the model**: the yield rule
//!   counts the TTL from `max(when the role was seen lost, when this version of
//!   the record was seen)`. Counting literally only from when the role was seen
//!   lost, a delegation CAS — itself a renewal — onto a lost role lets the
//!   observer expire at once
//!   (`delegating_to_a_lost_coordinator_breaks_ls3_with_literal_yield`).
//! - `delegated_write_no_extend` and `self_check_gates_ack`: switching them off
//!   breaks nothing. The first is redundant for safety given
//!   `yield_restarts_on_write`; the second is a liveness fence, as predicted.
//! - `heal_stale_close` — not a safety fence but a liveness path: an open that
//!   meets a mark on a live range with no intent (table read successfully)
//!   removes it.
//! - `close_checks_epoch` — close only if nobody has opened the source since the
//!   quiesce (the epoch is still the quiesced writer's):
//!   `EngineHandle::sealed_writer_epoch` → `ObjectStores::close_range_db_at`
//!   (fork `Admin::close_for_writes_at_epoch`). Proposed by this model itself,
//!   now in the code; [`Fences::ALL`] has it on. The model shows that the closed
//!   mark alone is not enough — see below.
//!
//! [`Fences::CLOSED_FLAG_ONLY`] switches off every older fence of the open path
//! (fresh table, SM-13, rolling back a foreign intent). **Finding:** with it,
//! "no ack into a retired Db" breaks for both split and merge — a writer that
//! reopens the source **between the quiesce and the close** is not blocked by
//! the mark (the mark blocks only opens after it; an already open writer is
//! fenced only lazily, at its next manifest update), and acks after the clone.
//! Adding `close_checks_epoch` makes it hold. **Second finding:** a stale mark
//! on a live range happens even when nobody dies — the coordinator closes late,
//! after another node rolled the intent back and reopened the source; its own
//! roll-back CAS loses, so it does not remove the mark. Only
//! `heal_stale_close` removes it ([`SOMETIMES_LATE_CLOSE`]).
//!
//! ## Invariants
//!
//! - [`SINGLE_WRITER`]: each Db has at most one writer able to write (Db open,
//!   grant valid, not fenced) — LS-2/3/4/5.
//! - [`NO_ACK_INTO_RETIRED_DB`]: no ack into a Db that is or later becomes
//!   retired — an ack into a range that has left the table, or into a source
//!   after the clone snapshot when the later commit CAS wins (RD1, RD2). A lost
//!   ack is a dead end (nothing is generated after it).
//! - [`SM13_QUIESCED_SOURCE_CLOSED`]: source quiesced, intent still on the
//!   table ⇒ nobody can write to the source.
//! - [`LS3_LEADER_IS_RECORDED_OWNER`]: an engine with a valid grant is exactly
//!   the one the owner record points at.
//! - [`LS8_RETIRED_NEVER_REACQUIRED`]: no engine holds a valid grant on a range
//!   that carries a tombstone.
//! - [`SM17_COMMITTED_SOURCE_STAYS_CLOSED`]: commit won ⇒ the closed mark of
//!   every source stays (no remover touches a retired source).
//! - [`NO_STALE_READ`]: no engine answers a read when the range's owner record
//!   no longer belonged to it **at the time of the read** (two-phase reads: at
//!   `ReadStart`; if the record changes after the read, linearising at the read
//!   is valid).
//! - `sometimes` (the model is not vacuous): the commit completes and a child
//!   is acked; another node rolls the intent back; a source carries a tombstone
//!   after the commit; a stale mark is removed by itself; a late close.
//! - Liveness of the closed mark (`AG(stale mark on a live range ⇒ EF
//!   removed)`) cannot be stated with stateright's `Property` (`eventually`
//!   looks only at terminal states, and the model has cycles): the test
//!   `stale_close_can_always_be_healed` walks the graph itself.
//!
//! ## Large runs and reduction
//!
//! Configurations above a few tens of millions of states are explored with
//! stateright's parallel DFS (it keeps only the set of fingerprints, ~25 bytes
//! per state, instead of BFS's ~200 bytes). Configurations symmetric under the
//! permutation of the two nodes ([`M1::node_symmetric`]: both nodes have the
//! same role) can be reduced with [`node_symmetry`] (π = [`State::swapped`],
//! [`Action::swapped`]; the soundness argument is in its doc, checked directly
//! by `tests/m1.rs::node_symmetry_commutes_with_every_step` and
//! `node_symmetry_keeps_every_delegation_break`). The reduction is only a
//! function handed to the checker: states, actions and `step` are unchanged, so
//! every older configuration has exactly the same number of states as before.
//!
//! ## Deliberately left out of the model
//!
//! - Data contents, the WAL (own or shared, NW-*), memtable/L0, `ensure_wal_drained`,
//!   `flush_memtable`, GC (D-d, SM-14, GC barriers), checkpoints: those belong to
//!   M2/M3. Consequence: SlateDB's F3 (clone refuses a source that still has a
//!   WAL — SM-4) is absent, so `Clone` can fire even after the source has been
//!   reopened; RD2's counterexample may clone after the reopen.
//! - Several splits/merges (a single operation, no retry after a roll-back); the
//!   second child of a split (symmetric with the first, only one child by
//!   default). Consequence for SM-17: `heal_stale_close` (read the table, then
//!   remove) and `reopen_sources` are not atomic with the read/CAS before them —
//!   with one operation, the rolled-back table is final, so this is harmless;
//!   with a **later** operation on the same range, a slow remover could remove
//!   the new mark of the later close. Outside the model; blocked in the code by
//!   removing conditionally on the epoch read before the CAS/table read
//!   (`ObjectStores::reopen_range_db_at`) and pinned by
//!   `failpoints.rs::slow_roll_back_keeps_the_close_of_a_later_intent`.
//! - The two closes of a merge are one step ([`Action::CloseSources`]); dying
//!   between them leaves only one source with a mark — a subset of "close both,
//!   then die".
//! - Roll-backers other than `factory` and the coordinator (the intent
//!   reconcile loop SM-15, `cairn-ctl intent clear`): the same shape, CAS then
//!   `reopen_sources`.
//! - The lazy fence of the closed mark is nondeterministic: [`Action::WriterFenced`]
//!   can come at any time (or never) — an over-approximation for safety.
//! - The owner record's `term` and an old keeper of the same session
//!   (`claim_from_others`): each (node, range) has only one engine.
//! - Ambiguous CAS outcomes (rule 7, `cas::put_resolving`): every CAS is atomic
//!   and has a clear outcome.
//! - A background retry of a release (`session/renew.rs:retry_releases`)
//!   interleaving with a re-claim and the next release of the same range: the
//!   release CAS is atomic, so the model has no "late attempt from an old
//!   term". The code drops the pending entry only while it is still exactly the
//!   snapshotted one (`session`, `term`, `version` — `forget_pending_release`);
//!   pinned by `session/tests.rs::stale_release_retry_keeps_the_newer_pending_release`.
//! - Failed reads other than `factory`'s table load (`cluster-version`, the
//!   owner record, the WAL-kind mark): they only make the open fail — the
//!   coordinator's nondeterministic `RollBack` and the `Failed` branch already
//!   cover the shape "a step fails ⇒ roll back".
//! - Upgrade gates, range-level migrations — including the `wal/shared` step,
//!   which opens and then closes the own-WAL Db once (one bump of
//!   `writer_epoch` by the range's own owner, under the same lease, before
//!   `OpenDb`; on a stale closed mark it fails and then `heal_stale_close` runs
//!   as for `OpenDb`) —, the balancer (fairness, self-shedding — LS-10/11),
//!   voluntary lease release, `owner_watch_loop`, the `quiesce_held` guard
//!   (SM-15), a node coming back after death (a dead node stays dead; a new
//!   incarnation comes only from losing the session — a node that comes back is
//!   another node). There is no single-process mode any more: a node without
//!   `--cluster` is a one-node cluster.
//! - Clock skew between machines (the assumption of LS-1/LS-2: skew below
//!   1.5×) — one shared clock, `validity < ttl` is a parameter.
//! - `solo` mode: ambiguous outcomes of the CAS into the mode, of a challenge
//!   and of an answer. An ambiguous entry needs no separate branch:
//!   [`Action::SoloCheck`] renews only when the read **sees** the owner's own
//!   `solo` record — that read itself confirms the CAS. A constraint on the
//!   code: a `304` counts only against the etag of the **confirmed** `solo`
//!   record, not the etag from before the CAS. An ambiguous challenge whose
//!   re-read does not show it as ours leaves the newcomer without a τ, so it
//!   does not expire; a late PUT is blocked by If-Match if the record changed
//!   (if it does land, the next read sees the `challenged` line as a new
//!   version — τ = when it arrived). The 10-minute safety-net LIST
//!   (`LeaseConfig::quiet_list_every`) and `owner_watch`'s 10-minute GET net:
//!   only cost and liveness. A `solo` record that cannot be decoded
//!   (`tracker.rs`, the `Err` branch when the last readable mode is `solo`): the
//!   reference is when it was **seen** broken, and one TTL later it is
//!   overwritten as a challenge — the model has no torn objects in `solo` mode
//!   (`torn_views` is only for `direct`).
//! - Delegation: the coordinator does not lead ranges (its ranges take the
//!   `direct` protocol of today, already covered) — except in the
//!   [`M1::hub_node`] configuration, where it is node 0: it leads ranges, enters
//!   `solo`, gets challenged, and only it enters `solo` (a delegating node is in
//!   `solo` only once it is alone — the plain `solo` protocol); one node
//!   delegates ([`M1::delegators`]), the other is always `direct` (observer,
//!   claimer — an `--ignored` test covers two). Heartbeat send-and-receive is
//!   one step (argument in [`M1::heartbeat_in_flight`]); an ack applies as soon
//!   as it is returned (a late ack carries an older send age: it only extends
//!   less). Ack rule 4 ("C knows N is `delegated` to it at exactly the version N
//!   reports") is not modelled: `granted` is both the permission to delegate
//!   (first step, returned to a `direct` node) and the ack, so the node decides
//!   itself whether to use it to renew; the rest of rule 4 (the record has been
//!   expired) is rule 5, the fence and the yield rule. Gate: the runtime rule
//!   `knows ≥ K` and `min_binary` are merged into one flag
//!   ([`Fences::delegation_gate`]: whether or not an observer runs an old
//!   binary). An ambiguous delegation CAS landing late (the PUT arrives after
//!   the node's own next CAS): the keeper serialises and uses If-Match, so it is
//!   equivalent to "landed when the node re-read". Clock: as everywhere else,
//!   one shared clock — τ is measured at the receiver, deadlines at the sender,
//!   at the same rate (assumed skew < 1.5×).
//! - The owner's renewal missing its CAS because the record was overwritten
//!   ([`Action::JudgeUnexpirable`] fenced): the code calls `lose` without writing
//!   anything, and the record stays `live` until an observer CASes `expired`;
//!   the model sets `ended` at once (together with revoking the grant) — letting
//!   others `claim` **earlier** than the code, i.e. an over-approximation for
//!   safety.

use stateright::{Model, Property};

pub type NodeId = u8;
pub type RangeId = u8;

/// Property names (used in tests and reports).
pub const SINGLE_WRITER: &str = "single writer per Db (LS-3, LS-4)";
pub const NO_ACK_INTO_RETIRED_DB: &str = "no ack into a retired Db (RD1, RD2)";
pub const SM13_QUIESCED_SOURCE_CLOSED: &str =
    "SM-13: source of an in-flight intent is not writable after quiesce";
pub const LS3_LEADER_IS_RECORDED_OWNER: &str = "LS-3: a live leader is the recorded owner";
pub const LS8_RETIRED_NEVER_REACQUIRED: &str = "LS-8: a retired range is never led again";
pub const SM17_COMMITTED_SOURCE_STAYS_CLOSED: &str =
    "SM-17: a source of a committed op keeps its closed flag";
pub const NO_STALE_READ: &str = "LS-5: a leader never answers a read after its range changed hands";
pub const SOMETIMES_CHILD_ACKED: &str = "sometimes: op commits and a child acks";
pub const SOMETIMES_FOREIGN_ROLL_BACK: &str = "sometimes: another node rolls back the intent";
pub const SOMETIMES_SOURCE_TOMBSTONED: &str = "sometimes: a source is tombstoned after commit";
pub const SOMETIMES_STALE_CLOSE_HEALED: &str =
    "sometimes: an open heals the stale closed flag of a live range";
pub const SOMETIMES_LATE_CLOSE: &str =
    "sometimes: the coordinator closes a source whose intent another node rolled back";
pub const SOMETIMES_SOLO_EXPIRED: &str = "sometimes: a challenger expires a solo session";
pub const SOMETIMES_CHALLENGE_ANSWERED: &str = "sometimes: a solo owner answers a challenge";
pub const SOMETIMES_ACK_EXTENDS: &str = "sometimes: an ack extends a delegated grant";
pub const SOMETIMES_COORDINATOR_EXPIRES: &str =
    "sometimes: the coordinator expires a delegated session";
pub const SOMETIMES_YIELD_EXPIRES: &str =
    "sometimes: an observer expires a session delegated to a coordinator that lost its role";
pub const SOMETIMES_SOLO_HUB_ACKS: &str =
    "sometimes: a coordinator in solo mode extends a delegated grant";
pub const SOMETIMES_SOLO_HUB_EXPIRED: &str =
    "sometimes: a challenger expires the solo coordinator while a node is delegated to it";

/// The existing fences, each switchable (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fences {
    pub fresh_table_before_open: bool,
    pub wait_unquiesced: bool,
    pub owner_cas: bool,
    pub roll_back_foreign_intent: bool,
    pub writer_epoch: bool,
    /// SM-17: the closed mark on the Db itself (`close_range_db`).
    pub closed_flag: bool,
    /// `factory`'s step that removes a stale closed mark by itself
    /// (`heal_stale_close`). Not a safety fence but a liveness path: off ⇒ a
    /// live range may stay stuck closed.
    pub heal_stale_close: bool,
    /// The coordinator closes a source only while the Db's `writer_epoch` is
    /// still the epoch of the writer it quiesced — nobody has opened the Db
    /// since the quiesce. Mismatch ⇒ no close, only a roll-back is left.
    /// Proposed by M1, now in the code (`ObjectStores::close_range_db_at`);
    /// `ALL` has it on.
    pub close_checks_epoch: bool,
    /// Judging "dead" without being able to CAS leads to `claim` only after an
    /// overwrite + one TTL + CAS `expired` (see the module doc). `ALL` has it
    /// on.
    pub unexpirable_rearm: bool,
    /// A paused grant revives only when a renewal CAS wins (the record is not
    /// `expired` and has not been overwritten). `ALL` has it on.
    pub revive_by_cas: bool,
    /// The read fence at the leader — GET the session record before answering.
    /// `ALL` has it on.
    pub read_fence: bool,
    /// Proof for the read fence (a fence GET, a winning renewal CAS) counts for
    /// an answer only when it was **issued after** the answer's ticket — the
    /// ticket is taken after the read (`fence.rs:ProofClock`). Off = reuse
    /// proof issued before the read. Meaningful only with [`M1::split_reads`].
    /// `ALL` has it on.
    pub proof_after_read: bool,
    /// The `solo` node's self-check GET renews the grant only when it sees its
    /// own `solo` record **unchallenged**. Off = it renews even when the read
    /// shows a `challenged` line. Meaningful only with [`M1::solo`].
    pub solo_extend_needs_unchallenged_read: bool,
    /// A newcomer CASes `expired` on a `solo` record only after it has
    /// challenged and the record has stood unchanged for one TTL **counted from
    /// the challenge**. Off = expire on silence (TTL since the last beat),
    /// without a challenge.
    pub expire_solo_needs_challenge_then_ttl: bool,
    /// The `control/quiet-lease` gate (with `min_binary`): while a `solo` record
    /// exists, no old binary is left that applies LS-1's silence rule to it.
    /// Off = an observer running an old binary expires a `solo` record after a
    /// TTL of silence.
    pub quiet_lease_gate: bool,
    /// The node uses an ack to renew only once its `mode=delegated` CAS is
    /// **confirmed** (won, or ambiguous and then re-read as ours). Off = use
    /// acks as soon as the delegation CAS has been issued. Meaningful only with
    /// [`M1::delegation`] (as is every flag below).
    pub ack_needs_confirmed_delegation: bool,
    /// Ack rule 5: the coordinator does not ack a session for which it has
    /// **ever issued** an `expired` CAS, ambiguous ones included (a late PUT
    /// still lands). Off = remember only expiries known to have won; if
    /// ambiguous and the re-read shows the record unchanged, keep acking.
    pub no_ack_after_expire: bool,
    /// Expire rule 4: the coordinator's `expired` CAS is If-Match on the
    /// **delegated version** it holds. Off = a blind overwrite.
    pub expire_if_match_delegated: bool,
    /// Ack rules 2–3: before acking a batch, and after receiving it, GET
    /// `control/coordinator` (exactly the current incarnation, the right term,
    /// not released) and the coordinator's own session record (still `live`);
    /// the term in the heartbeat equals the current term. Off = ack by the role
    /// in RAM.
    pub coordinator_fence: bool,
    /// The incarnation part of the coordinator fence:
    /// `control/coordinator.session` must equal the coordinator's **current**
    /// incarnation. Off = compare with the incarnation of the role in RAM, and
    /// "session still `live`" reads the current incarnation's record — an old
    /// incarnation's role passes the fence thanks to the new record.
    pub fence_checks_incarnation: bool,
    /// The ack-rule-3 part of the fence: the term in the heartbeat equals the
    /// current term. Off = also ack heartbeats from a node still delegated to an
    /// old term.
    pub ack_checks_heartbeat_term: bool,
    /// The yield rule: an observer other than the coordinator expires a
    /// `delegated coord=X:t` record only when X has lost its role (session
    /// ended, term > t, or `released`) **and** it has been ≥ TTL since it saw
    /// that. Off = once X is seen to have lost its role, LS-1's silence rule
    /// applies at once (no TTL wait).
    pub observer_defers_to_coordinator: bool,
    /// Proposed by the model: the yield rule's wait restarts from **every new
    /// version** of the record (beat increased): TTL since `max(when X was seen
    /// to lose its role, when this version was seen)`. Off = the literal rule:
    /// count only from when X was seen to lose its role.
    pub yield_restarts_on_write: bool,
    /// The node's CASes while delegated (`owns`, `draining`) do not renew the
    /// grant; only acks renew.
    pub delegated_write_no_extend: bool,
    /// The `control/quiet-lease` gate + the coordinator's `knows ≥ K` rule:
    /// while a `delegated` record exists, no old binary applies LS-1 to it.
    /// Off = an observer running an old binary expires a `delegated` record
    /// after a TTL of silence.
    pub delegation_gate: bool,
    /// Ack rule 6: the coordinator acks only when the heartbeat reports its
    /// last successful bucket touch < TTL/3 ago. The model predicts this is a
    /// **liveness** fence (a node cut off from the bucket does not keep its
    /// session forever), not a safety one.
    pub self_check_gates_ack: bool,
    /// "An ack does not revive a grant past the pause cap": a node that has
    /// called `lose` (past the cap, writes `closed`; or a self-check GET saw the
    /// session ended) applies no more acks. In `direct` the pause cap is only
    /// liveness; in delegated mode it is a safety fence — a finding of the
    /// model. Off = an ack revives the grant after `lose`.
    pub ack_respects_pause_cap: bool,
}

impl Fences {
    /// Every fence of the current code.
    pub const ALL: Fences = Fences {
        fresh_table_before_open: true,
        wait_unquiesced: true,
        owner_cas: true,
        roll_back_foreign_intent: true,
        writer_epoch: true,
        closed_flag: true,
        heal_stale_close: true,
        close_checks_epoch: true,
        unexpirable_rearm: true,
        revive_by_cas: true,
        read_fence: true,
        proof_after_read: true,
        solo_extend_needs_unchallenged_read: true,
        expire_solo_needs_challenge_then_ttl: true,
        quiet_lease_gate: true,
        ack_needs_confirmed_delegation: true,
        no_ack_after_expire: true,
        expire_if_match_delegated: true,
        coordinator_fence: true,
        fence_checks_incarnation: true,
        ack_checks_heartbeat_term: true,
        observer_defers_to_coordinator: true,
        yield_restarts_on_write: true,
        delegated_write_no_extend: true,
        delegation_gate: true,
        self_check_gates_ack: true,
        ack_respects_pause_cap: true,
    };

    /// Only the fences on the object itself: the closed mark (with the
    /// self-healing step), the owner record and `writer_epoch`; every older
    /// fence of the Db-open path (fresh table, SM-13, rolling back a foreign
    /// intent) is off.
    pub const CLOSED_FLAG_ONLY: Fences = Fences {
        fresh_table_before_open: false,
        wait_unquiesced: false,
        roll_back_foreign_intent: false,
        // The closed mark **alone**: no epoch check at close time (one test
        // keeps the finding that this leaks; another switches it back on).
        close_checks_epoch: false,
        ..Fences::ALL
    };
}

/// The shape of the single operation: split `0 → {1}` or merge `{0, 1} → {2}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shape {
    Split,
    Merge,
}

/// Model configuration.
#[derive(Clone, Debug)]
pub struct M1 {
    pub nodes: u8,
    pub shape: Shape,
    /// Ticks of silence before an observer CASes `expired` (TTL).
    pub ttl: u8,
    /// Ticks a grant is valid after the last renewal (⅔ TTL in the real code).
    /// `validity > ttl` is a wrong configuration (LS-2) — tests use it to see
    /// fences break.
    pub validity: u8,
    pub max_incarnations: u8,
    pub max_crashes: u8,
    /// An observer may see another node's session through a torn object or a
    /// stale read (no beat increase seen, the `expired` CAS misses) — enables
    /// [`Action::JudgeUnexpirable`]. Off by default: the rest of the model does
    /// not need it, and it multiplies the state space.
    pub torn_views: bool,
    /// **Two-phase** reads: the read, the ticket (after `alive`), the proof (a
    /// fence GET or a winning renewal CAS) and the answer are separate steps
    /// that can interleave with everything else — instead of the atomic
    /// [`Action::ServeRead`]. Off by default: more states, and only the read
    /// fence's tests need it.
    pub split_reads: bool,
    /// Whether the single split/merge operation runs. Off = only the lease
    /// protocol on the source range is left (every node competes for range 0)
    /// — for configurations that check only leases (`solo`, delegation), many
    /// times smaller. On by default.
    pub split_op: bool,
    /// A node can enter `solo` mode (renewing by self-check GET), others
    /// challenge through the bucket. Off by default.
    pub solo: bool,
    /// Renewal delegated to a coordinator ([`Hub`]) via heartbeat/ack over the
    /// network. Off by default.
    pub delegation: bool,
    /// Number of nodes (counted from the last node) that delegate; the other
    /// nodes are always `direct` (observer, claimer). One is enough for every
    /// breaking row.
    pub delegators: u8,
    /// Incarnation cap of the coordinator process.
    pub hub_incarnations: u8,
    /// Term cap of `control/coordinator` (one term per role take-over).
    pub coordinator_terms: u8,
    /// An in-flight heartbeat has its own send age ([`Action::Heartbeat`] then
    /// [`Action::HeartbeatAck`]). Off (default) = send and receive in one step,
    /// send age 0: since an ack renews until **send** time + validity and τ =
    /// **receive** time, a heartbeat sent later (received at the same time)
    /// renews more and sets the same τ, so the one-step version dominates every
    /// delayed one for safety — it only lacks heartbeats carrying an old `term`
    /// or bucket-touch age, and those only make the coordinator refuse more. On
    /// to check that very argument (an `--ignored` test).
    pub heartbeat_in_flight: bool,
    /// Faults specific to the delegation protocol: ambiguous CASes (delegation,
    /// `expired`), CASes while delegated ([`Action::RecordWrite`]), the
    /// coordinator losing its role, releasing, taking back, opening a new
    /// incarnation. Off = a healthy coordinator — only to check delegation
    /// **together with** split/merge quickly by BFS (those faults are already
    /// checked in the lease configurations, where they can break LS-3; split
    /// with every fault runs by DFS in
    /// `delegation_holds_for_split_with_all_faults`). On by default.
    pub delegation_faults: bool,
    /// The coordinator **is node 0** — [`Hub`]'s session is node 0's session
    /// (it has ranges, can enter `solo`, gets challenged, calls `lose`, opens a
    /// new incarnation along every node's path), instead of a separate process
    /// that is always `direct` and whose session can end at any time
    /// ([`Action::CoordinatorLose`]). Needs [`M1::delegation`]; node 0 never
    /// delegates. The coordinator's incarnation cap is
    /// [`M1::max_incarnations`]. Off by default.
    pub hub_node: bool,
    pub fences: Fences,
}

impl M1 {
    /// Split, two nodes, TTL 2 ticks, validity 1 tick (`validity < ttl` is
    /// enough for LS-2; as small as possible to keep the state space small), at
    /// most two incarnations per node, no deaths.
    ///
    /// No deaths is enough for safety: a dead node is a node that is never
    /// scheduled again, and BFS already explores every such interleaving (its
    /// session expires via `Tick` + `ObserveExpired` like any silent node). The
    /// only difference — the `ActiveIntents` in RAM being lost — affects only the
    /// dead node itself. `max_crashes = 1` has its own test.
    pub fn split(fences: Fences) -> Self {
        M1 {
            nodes: 2,
            shape: Shape::Split,
            ttl: 2,
            validity: 1,
            max_incarnations: 2,
            max_crashes: 0,
            torn_views: false,
            split_reads: false,
            split_op: true,
            solo: false,
            delegation: false,
            delegators: 1,
            hub_incarnations: 2,
            coordinator_terms: 2,
            heartbeat_in_flight: false,
            delegation_faults: true,
            hub_node: false,
            fences,
        }
    }

    /// Only the lease protocol (no split/merge): two nodes compete for range 0.
    /// The base of the `solo` and delegation configurations.
    pub fn lease(fences: Fences) -> Self {
        M1 {
            split_op: false,
            ..Self::split(fences)
        }
    }

    /// The ranges a node's actions can touch: without split/merge the children
    /// never enter the table, so they are left out of the action list.
    fn acted_ranges(&self) -> std::ops::Range<RangeId> {
        if self.split_op {
            0..self.range_count() as RangeId
        } else {
            0..self.sources().len() as RangeId
        }
    }

    /// Whether any protocol renews the grant without writing the session record
    /// (self-check GET, ack) — then the observer's "silence"
    /// ([`Session::quiet`]) diverges from the grant's age.
    fn quiet_lease(&self) -> bool {
        self.solo || self.delegation
    }

    /// Track `n`'s bucket touches (ack rule 6): only when the fence is on — the
    /// fence only **removes** acks, so the fence-off configuration has every
    /// behaviour of the fence-on one.
    fn tracks_touch(&self, n: NodeId) -> bool {
        self.delegator(n) && self.fences.self_check_gates_ack
    }

    /// Node `n` delegates ([`M1::delegators`]). The coordinator
    /// ([`M1::hub_node`]) does not delegate to itself.
    fn delegator(&self, n: NodeId) -> bool {
        self.delegation && n + self.delegators >= self.nodes && !self.hub_at(n)
    }

    /// Node `n` is the coordinator process ([`M1::hub_node`]).
    fn hub_at(&self, n: NodeId) -> bool {
        self.hub_node && n == 0
    }

    /// The coordinator process is still running: always, when it is a separate
    /// process (its session ends via [`Action::CoordinatorLose`]); when it is
    /// node 0, as long as that node has not died.
    fn hub_running(&self, s: &State) -> bool {
        !self.hub_node || !s.nodes[0].dead
    }

    /// Ticks the observer has seen the record stand still (beat not increased).
    fn quiet(&self, ss: &Session) -> u8 {
        if self.quiet_lease() {
            ss.quiet
        } else {
            ss.age
        }
    }

    /// The owner has just written its own record (beat + 1): the version changes.
    fn wrote(&self, s: &mut State, n: NodeId) {
        let ss = &mut s.sessions[n as usize];
        if self.quiet_lease() {
            ss.quiet = 0;
        }
        if self.tracks_touch(n) {
            ss.touched = true;
        }
        Self::version_changed(s, n);
    }

    /// `n`'s session record changes version: an If-Match CAS on the old version
    /// (the delegated version the coordinator holds, an ambiguous `expired` PUT
    /// not yet landed) misses from now on.
    fn version_changed(s: &mut State, n: NodeId) {
        s.sessions[n as usize].pending_expire = false;
        if let Some(hub) = &mut s.hub {
            let v = &mut hub.views[n as usize];
            if v.held == Held::Current {
                v.held = Held::Stale;
            }
        }
    }

    /// Someone else CASes `expired` on `n`'s record.
    fn expire_by_other(s: &mut State, n: NodeId) {
        s.sessions[n as usize].ended = true;
        Self::version_changed(s, n);
    }

    /// Like [`Self::split`] but merging two ranges.
    pub fn merge(fences: Fences) -> Self {
        M1 {
            shape: Shape::Merge,
            ..Self::split(fences)
        }
    }

    pub fn sources(&self) -> &'static [RangeId] {
        match self.shape {
            Shape::Split => &[0],
            Shape::Merge => &[0, 1],
        }
    }

    pub fn children(&self) -> &'static [RangeId] {
        match self.shape {
            Shape::Split => &[1],
            Shape::Merge => &[2],
        }
    }

    fn range_count(&self) -> usize {
        self.sources().len() + self.children().len()
    }

    /// Range `r` is in table `t`.
    pub fn contains(&self, t: Table, r: RangeId) -> bool {
        match t {
            Table::Committed => self.children().contains(&r),
            _ => self.sources().contains(&r),
        }
    }

    fn intent_on(&self, t: Table, r: RangeId) -> bool {
        t == Table::Intent && self.sources().contains(&r)
    }

    fn is_source(&self, r: RangeId) -> bool {
        self.sources().contains(&r)
    }

    fn age_cap(&self) -> u8 {
        self.ttl.max(self.validity)
    }
}

/// `cluster.conf` — only the four tables reachable with a single operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    /// v1: sources in the table, no intent.
    Initial,
    /// v2: sources carry the intent (the intent CAS has won).
    Intent,
    /// v3: sources left the table, children entered it (the commit CAS has won).
    Committed,
    /// v3: the intent was CASed away (rolled back); the sources stay.
    RolledBack,
}

impl Table {
    pub fn version(self) -> u8 {
        match self {
            Table::Initial => 1,
            Table::Intent => 2,
            Table::Committed | Table::RolledBack => 3,
        }
    }
}

/// `owners/range-<id>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Owner {
    pub node: NodeId,
    pub inc: u8,
    pub released: bool,
    /// Tombstone (`retired`): nobody takes it again, not even the owner itself
    /// (LS-8, D22).
    pub retired: bool,
}

/// `sessions/<node>/<inc>` of the node's current incarnation, plus its local
/// clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Session {
    pub inc: u8,
    /// Ticks since the last successful renewal (saturates at `max(ttl, validity)`).
    pub age: u8,
    /// `expired` or `closed` on the bucket — permanent (LS-1). The owner does
    /// **not** see an observer's `expired` until its next renewal; it trusts
    /// only its own clock (`age < validity`).
    pub ended: bool,
    /// Ticks since an observer **overwrote** the record (saturates at `ttl`);
    /// `None` = not overwritten. The owner's next renewal misses its CAS.
    pub rearm_age: Option<u8>,
    /// An observer has judged it dead **unfenced** (the historical behaviour,
    /// `dead_unexpirable` in RAM) — only with `unexpirable_rearm` off.
    pub judged: bool,
    /// Ticks since the owner last **wrote** the record (beat increased) — what
    /// the observer counts under LS-1 (saturates like `age`). Tracked only when
    /// a renewal protocol that does not write is on (`solo`, delegation);
    /// otherwise always 0 and LS-1 reads `age`.
    pub quiet: u8,
    /// The `mode` line of the record on the bucket.
    pub mode: Mode,
    /// What the owner believes about its delegation CAS.
    pub belief: Belief,
    /// The owner's last successful bucket touch is recent (< TTL/3: within this
    /// tick) — what the heartbeat reports for ack rule 6.
    pub touched: bool,
    /// A heartbeat in flight to the coordinator (one slot: a later heartbeat
    /// replaces the earlier one — the earlier one counts as lost).
    pub hb: Option<Heartbeat>,
    /// An ambiguous `expired` CAS of the coordinator (If-Match the current
    /// version) may still land late (rule 7) — until the version changes.
    pub pending_expire: bool,
    /// The owner has called `lose` on this session (past the pause cap — writes
    /// `closed` —, or a self-check GET saw it ended): the grant is revoked for
    /// good. Tracked only with [`M1::delegation`]; unlike `ended`, which is the
    /// truth on the bucket that the owner does not see (expired by someone else).
    pub lost: bool,
}

/// What the owner believes about its `mode=delegated` CAS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Belief {
    /// No unresolved delegation CAS: the owner knows the record is `direct`.
    Direct,
    /// A delegation CAS issued, outcome ambiguous, not yet re-read.
    Unsure,
    /// The delegation CAS is confirmed.
    Delegated,
}

/// A heartbeat in flight: `age` = ticks since its first send (the owner's
/// clock — an ack renews until send time + validity; saturates at `validity`:
/// older than that, an ack renews nothing), `term` the owner wrote in the
/// heartbeat, `touched` = bucket-touch age < TTL/3 at send time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Heartbeat {
    pub age: u8,
    pub term: u8,
    pub touched: bool,
}

impl Session {
    /// A freshly opened record (`open_incarnation`): `live`, `direct`, a new beat.
    pub const FRESH: Session = Session {
        inc: 1,
        age: 0,
        ended: false,
        rearm_age: None,
        judged: false,
        quiet: 0,
        mode: Mode::Direct,
        belief: Belief::Direct,
        touched: false,
        hb: None,
        pending_expire: false,
        lost: false,
    };
}

/// The `mode=` line of the session record (read leniently; absent = `direct`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mode {
    /// As today: the owner PUTs renewals, observers apply LS-1.
    Direct,
    /// `mode=solo`: the owner renews by self-check GET. `challenge` = ticks
    /// since the confirmed challenge CAS (the `challenged=` line, saturates at
    /// `ttl`); `None` = not challenged.
    Solo { challenge: Option<u8> },
    /// `mode=delegated coord=<hub>/<inc>:<term>`: the term identifies the
    /// coordinator (each term exactly one incarnation, [`Hub::terms`]).
    Delegated { term: u8 },
}

/// The control plane's coordinator — not the split/merge coordinator
/// ([`Coord`]). A process that is always `direct` and does not lead ranges in
/// the model (its ranges take today's protocol, already covered by the rest of
/// M1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Hub {
    /// Current incarnation of the coordinator's session.
    pub inc: u8,
    /// That session has ended on the bucket (`lose`, or expired).
    pub ended: bool,
    /// The role in RAM: (incarnation, term) the process believes it holds. May
    /// be stale (zombie): `lose` has not released it yet, the clock stalled.
    pub role: Option<(u8, u8)>,
    /// `control/coordinator.term`.
    pub term: u8,
    /// `control/coordinator` is `released` in the current term.
    pub released: bool,
    /// Every term so far (index = term − 1).
    pub terms: Vec<Term>,
    /// The coordinator's view of each node.
    pub views: Vec<View>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Term {
    /// The coordinator incarnation holding this term.
    pub inc: u8,
    /// Ticks since this term's role was lost (session ended, a later term, or
    /// `released`) — the fastest observer sees it at once (saturates at `ttl`).
    pub lost_age: Option<u8>,
}

/// The coordinator tracking one session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct View {
    /// Ticks since the latest proof of life τ(N): a heartbeat received, or a GET
    /// that saw a new version (saturates at `ttl`).
    pub tau: u8,
    /// The delegated version the coordinator holds (confirmed by GET).
    pub held: Held,
    /// The coordinator has issued an `expired` CAS for this session.
    pub issued: Issued,
}

impl Hub {
    /// Term `t`'s role is lost: a later term exists, `released` in that very
    /// term, or the session of the incarnation holding it has ended on the
    /// bucket.
    pub fn lost(&self, t: u8) -> bool {
        self.term > t
            || (self.term == t && self.released)
            || self.inc != self.terms[t as usize - 1].inc
            || self.ended
    }
}

impl View {
    pub const FRESH: View = View {
        tau: 0,
        held: Held::None,
        issued: Issued::None,
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Held {
    None,
    /// Exactly the record's current version (If-Match will win).
    Current,
    /// The record has changed since the GET (the coordinator does not know).
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Issued {
    None,
    /// An ambiguous `expired` CAS (the re-read shows the record unchanged — the PUT may land late).
    Ambiguous,
    /// The `expired` CAS has won.
    Won,
}

/// One leadership of range `r`'s engine on a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Engine {
    /// The lease is not won (or was dropped); the leadership loop will retry.
    Idle,
    /// `claim` won at incarnation `inc`; `fresh` = past the table-load step.
    Claimed { inc: u8, fresh: bool },
    /// The Db is open for writing (writer). `fenced` = a later open bumped `writer_epoch`.
    Open { inc: u8, fenced: bool },
    /// Quiesced for the coordinator: Db sealed, lease still held.
    Quiesced { inc: u8 },
    /// The range has left the table, the engine exited for good (the id is never reused).
    Gone,
}

impl Engine {
    fn claim_inc(self) -> Option<u8> {
        match self {
            Engine::Claimed { inc, .. } | Engine::Open { inc, .. } | Engine::Quiesced { inc } => {
                Some(inc)
            }
            Engine::Idle | Engine::Gone => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub dead: bool,
    /// The RAM table — may be older than the bucket.
    pub ram: Table,
    /// One engine per range (index = range id).
    pub engines: Vec<Engine>,
    /// This range's `factory` has just CASed away a foreign intent (SM-3) and
    /// has not yet removed the closed marks from the sources
    /// (`reopen_sources`); that range's open waits.
    pub reopen_pending: Option<RangeId>,
    /// A read answer passing through the fence ([`M1::split_reads`]). Only
    /// **stale** reads are tracked (at read time the owner record no longer
    /// belonged to the engine): a fresh read never becomes a violation and does
    /// not change the protocol state.
    pub read: Option<PendingRead>,
    /// Incarnation of the latest proof (a fence GET or renewal CAS that saw our
    /// session still live). Recorded only with [`Fences::proof_after_read`] off
    /// — that is what the faulty configuration reuses.
    pub last_proof: Option<u8>,
}

/// A read answer passing through the fence ([`M1::split_reads`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PendingRead {
    /// Read done (by the engine of incarnation `inc`), not yet past `alive`/the ticket.
    Started { inc: u8 },
    /// Past `alive` with a ticket taken; `proven` = a proof issued after the
    /// ticket has seen `inc`'s session record still live and ours.
    Waiting { inc: u8, proven: bool },
}

/// The coordinator's phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    NotStarted,
    Intent,
    Quiesced,
    /// The sources carry the closed mark (SM-17), not yet cloned.
    Closed,
    Cloned,
    ChildrenClaimed,
    Committed,
    Done,
    /// The roll-back CAS has won; the closed marks are not yet removed from the
    /// sources (`reopen_sources`), the guard is still held.
    RollingBack,
    RolledBack,
    /// The coordinator's node died midway.
    Crashed,
}

impl Phase {
    /// The `ActiveIntents` guard is alive.
    fn holds_guard(self) -> bool {
        matches!(
            self,
            Phase::Intent
                | Phase::Quiesced
                | Phase::Closed
                | Phase::Cloned
                | Phase::ChildrenClaimed
                | Phase::Committed
                | Phase::RollingBack
        )
    }

    /// Not committed, can still roll back.
    fn before_commit(self) -> bool {
        matches!(
            self,
            Phase::Intent
                | Phase::Quiesced
                | Phase::Closed
                | Phase::Cloned
                | Phase::ChildrenClaimed
        )
    }

    /// Cloned (the source snapshot is taken), not committed.
    fn cloned(self) -> bool {
        matches!(self, Phase::Cloned | Phase::ChildrenClaimed)
    }

    /// The sources are quiesced and the coordinator is still working (SM-13).
    fn source_quiesced(self) -> bool {
        matches!(
            self,
            Phase::Quiesced | Phase::Closed | Phase::Cloned | Phase::ChildrenClaimed
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Coord {
    pub node: NodeId,
    pub phase: Phase,
    /// `ActiveIntents`' "quiesced" mark (SM-13).
    pub marked: bool,
    /// A successful open of the source Db after the quiesce (`writer_epoch` has
    /// moved past the epoch of the writer the coordinator quiesced). Tracked
    /// only with [`Fences::close_checks_epoch`] on.
    pub reopened: bool,
}

/// Ghost variables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ghost {
    /// An ack was lost because its Db retired: an ack into a range that has
    /// left the table, or into a source after the clone snapshot when the later
    /// commit wins.
    pub lost_ack: bool,
    /// An ack into a source after the clone while the intent is still on the table.
    pub source_acked_after_clone: bool,
    pub child_acked: bool,
    pub foreign_roll_back: bool,
    /// `heal_stale_close` has removed a stale closed mark.
    pub healed: bool,
    /// The coordinator closes a source after someone else rolled the intent
    /// back (a late close: `ensure_intent_ours` and `close_range_db` are not
    /// atomic) — a stale mark that needs no death.
    pub late_close: bool,
    /// An engine answered a read while the range's owner record no longer
    /// belonged to it.
    pub stale_read: bool,
    /// A `solo` record was CASed to `expired` by a challenger.
    pub solo_expired: bool,
    /// A `solo` owner answered a challenge (CAS back to `direct`).
    pub challenge_answered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub table: Table,
    /// The `closed` flag in each range's Db manifest (SM-17).
    pub closed: Vec<bool>,
    pub owners: Vec<Option<Owner>>,
    pub sessions: Vec<Session>,
    pub nodes: Vec<Node>,
    pub coord: Coord,
    /// The control plane's coordinator — only with [`M1::delegation`].
    pub hub: Option<Hub>,
    pub crashes: u8,
    pub ghost: Ghost,
}

/// The outcome of one `cluster.conf` read by `factory`: the load before opening
/// ([`Action::ReloadForOpen`]) or `heal_stale_close`'s read
/// ([`Action::OpenDb`] meeting a closed mark).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Read {
    Ok,
    /// Failed after every attempt of `open_io`.
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Renew(NodeId),
    /// A renewal landing **after** the self-step-down deadline, while the grant
    /// is paused, that wins its CAS — a revival.
    RenewLate(NodeId),
    ObserveExpired(NodeId),
    /// An observer judges the node's session dead after `UNEXPIRABLE_AFTER_TTLS`
    /// TTLs without being able to CAS it (torn/stale object) — see the module doc.
    JudgeUnexpirable(NodeId),
    /// A dead node's session expires **at once**, without waiting a TTL: the
    /// exclusive lock on the local directory proves the owner is dead.
    ExpireUnderLock(NodeId),
    Crash(NodeId),
    FollowTable(NodeId),
    Claim(NodeId, RangeId),
    ReloadForOpen(NodeId, RangeId, Read),
    /// `Read` matters only when the open meets a closed mark (`heal_stale_close`'s
    /// table read); every other branch is generated only with `Read::Ok`.
    OpenDb(NodeId, RangeId, Read),
    Ack(NodeId, RangeId),
    /// The engine answers a read command — see the mapping table.
    ServeRead(NodeId, RangeId),
    /// Two-phase read ([`M1::split_reads`]), step 1: the turn reads the range state.
    ReadStart(NodeId, RangeId),
    /// Step 2: `Lease::sync_reply` passes `alive`, then takes a fence ticket.
    ReadTicket(NodeId),
    /// A fence GET (`fence.rs:confirm`) issued after the ticket: still live and
    /// ours ⇒ proof; otherwise ⇒ the answer is refused.
    ProbeSession(NodeId),
    /// Last step: the answer goes out.
    ReadReply(NodeId),
    /// A writer opened before the closed mark reaches its next manifest update:
    /// sees `writer_epoch` changed ⇒ `Fenced`, the engine exits.
    WriterFenced(NodeId, RangeId),
    Reconcile(NodeId, RangeId),
    RetireOrphan(NodeId, RangeId),
    IntentCas(NodeId),
    MarkQuiesced,
    Quiesce,
    CloseSources,
    Clone,
    ClaimChildren,
    CommitCas,
    Retire,
    RollBack,
    /// `reopen_sources` by a roll-backer on the node: the coordinator (after its
    /// roll-back CAS) or `factory` (after CASing away a foreign intent).
    /// `Io::Failed` = the removal failed (only a `warn`, carries on).
    ReopenSources(NodeId, Io),
    /// The node CASes its record to `mode=solo` (a winning `direct` renewal),
    /// after which it stops PUTting renewals.
    EnterSolo(NodeId),
    /// The `solo` node's self-check GET — unchallenged ⇒ renew the grant until
    /// issue time + validity.
    SoloCheck(NodeId),
    /// The `solo` node CASes its record **keeping** `mode=solo`, `beat+1`
    /// (`owns`/`draining` changed while alone), If-Match the confirmed `solo`
    /// version: won = an LS-2 renewal; if challenged, the etag has changed so it
    /// misses — the way out is [`Action::AnswerChallenge`].
    SoloWrite(NodeId),
    /// Someone else CASes a `challenged=` line into the node's `solo` record.
    Challenge(NodeId),
    /// The `solo` node CASes its record back to `direct` (dropping the
    /// `challenged` line) — answering a challenge, or leaving solo mode by itself
    /// once it learns of another session.
    AnswerChallenge(NodeId),
    /// The challenger CASes `expired` on a `solo` record that stood unchanged for
    /// one TTL after the challenge.
    ExpireChallenged(NodeId),
    /// The node sends a heartbeat to the coordinator (while delegated or delegating).
    Heartbeat(NodeId),
    /// The coordinator receives the node's heartbeat (new τ), passes the fence,
    /// then returns `granted` and the node applies the ack — one step (the
    /// protocol's `Ack(n)`; a different name because [`Action::Ack`] is the ack
    /// to clients); a lost role that still acks is the protocol's `ZombieAck`
    /// (gets through only with [`Fences::coordinator_fence`] off).
    HeartbeatAck(NodeId),
    /// The keeper CASes the record to `mode=delegated coord=C:t` (or
    /// re-delegates to a new term).
    Delegate(NodeId, Cas),
    /// The node CASes the record back to `direct`, `beat+1` (acks lost,
    /// `fallback`, term changed) — a `direct` renewal.
    Fallback(NodeId),
    /// Self-check GET of the node's own record: touches the bucket, resolves an
    /// ambiguous delegation CAS, sees it ended ⇒ `lose`.
    SelfCheck(NodeId),
    /// A delegating node's CAS keeping the `mode` (changes `owns`, `draining`).
    RecordWrite(NodeId),
    /// The coordinator CASes `expired` on a delegated session silent for a TTL.
    CoordinatorExpire(NodeId, Expiry),
    /// The coordinator's tracker GETs a record delegated to its role **without**
    /// a heartbeat — confirming the delegated version (a node that just
    /// delegated and died before its first heartbeat, or every heartbeat lost).
    /// A version not held (`Held::None`, `Held::Stale`) is a new version: τ resets.
    CoordinatorRead(NodeId),
    /// The coordinator's ambiguous `expired` PUT lands late (rule 7).
    ExpireLands(NodeId),
    /// The coordinator takes `control/coordinator` (term + 1) when the record
    /// points at a lost role.
    CoordinatorTakeover,
    /// The coordinator CASes `control/coordinator` to `released` and drops the role.
    CoordinatorRelease,
    /// The coordinator's session ends (`lose`, or expired) — the role in RAM is
    /// **not** lost by itself (zombie).
    CoordinatorLose,
    /// The coordinator opens a new incarnation.
    CoordinatorReopen,
}

/// Outcome of the node's delegation CAS ([`Action::Delegate`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Cas {
    Won,
    /// Ambiguous, landed.
    AmbiguousApplied,
    /// Ambiguous, did not land (the record stays as it was).
    AmbiguousLost,
}

/// Outcome of the coordinator's `expired` CAS ([`Action::CoordinatorExpire`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Expiry {
    Won,
    /// Ambiguous: the re-read shows the record unchanged; the PUT may land late
    /// ([`Action::ExpireLands`]).
    Ambiguous,
}

/// Outcome of one admin write to the manifest (`reopen_range_db`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Io {
    Ok,
    /// Failed after every attempt of `admin_io`.
    Failed,
}

impl M1 {
    /// The grant issued at node `n`'s incarnation `inc` is still valid (LS-2):
    /// exactly the current incarnation, no more than `validity` ticks since the
    /// last renewal — by the node's own clock, not by `ended` on the bucket.
    /// Past due it is **paused**: no acks, no writes; [`Action::RenewLate`]
    /// winning its CAS makes it valid again, [`Action::Renew`] calls `lose`.
    /// With `validity < ttl`, an observer CASes `expired` only after the grant
    /// has run out (proved at the top of `session/mod.rs`).
    fn alive(&self, s: &State, n: NodeId, inc: u8) -> bool {
        let ss = &s.sessions[n as usize];
        !s.nodes[n as usize].dead && ss.inc == inc && ss.age < self.validity
    }

    /// An observation of `n`'s session record that sees its current incarnation
    /// still live (a winning renewal CAS, a fence GET `304`/`200` live): proof
    /// for every answer of that incarnation that **has taken its ticket**.
    fn proof(&self, s: &mut State, n: NodeId) {
        let inc = s.sessions[n as usize].inc;
        let node = &mut s.nodes[n as usize];
        if let Some(PendingRead::Waiting { inc: i, proven }) = &mut node.read {
            if *i == inc {
                *proven = true;
            }
        }
        if !self.fences.proof_after_read {
            node.last_proof = Some(inc);
        }
    }

    fn session_live(&self, s: &State, n: NodeId) -> bool {
        self.alive(s, n, s.sessions[n as usize].inc)
    }

    /// The owner is delegated or delegating: a `delegated` record or an unresolved delegation CAS.
    fn delegating(ss: &Session) -> bool {
        ss.belief != Belief::Direct || matches!(ss.mode, Mode::Delegated { .. })
    }

    /// An observer is left (another node still alive) to CAS `n`'s record.
    fn observer(&self, s: &State, n: NodeId) -> bool {
        (0..self.nodes).any(|m| m != n && !s.nodes[m as usize].dead)
    }

    /// The owner's CAS on its own record wins (from exactly the previous
    /// version: not ended, not overwritten) — a `direct` renewal under LS-2,
    /// even while the grant is paused.
    fn renewed(&self, s: &mut State, n: NodeId) {
        s.sessions[n as usize].age = 0;
        self.wrote(s, n);
        self.proof(s, n);
    }

    /// `n`'s record can be written by the owner's own CAS: not ended, not
    /// overwritten, the node still alive.
    fn own_cas_wins(s: &State, n: NodeId) -> bool {
        let ss = &s.sessions[n as usize];
        !s.nodes[n as usize].dead && !ss.ended && ss.rearm_age.is_none()
    }

    /// Engine `(n, r)` has a writer that can write: Db open, grant still valid
    /// (LS-5), not fenced (LS-4).
    pub fn can_write(&self, s: &State, n: NodeId, r: RangeId) -> bool {
        match s.nodes[n as usize].engines[r as usize] {
            Engine::Open { inc, fenced } => {
                self.alive(s, n, inc) && !(self.fences.writer_epoch && fenced)
            }
            _ => false,
        }
    }

    /// `Session::session_is_dead`: an older incarnation (ended before the later
    /// incarnation opened), or the current incarnation itself has ended.
    fn session_dead(&self, s: &State, o: &Owner) -> bool {
        let ss = &s.sessions[o.node as usize];
        o.inc < ss.inc || (o.inc == ss.inc && (ss.ended || ss.judged))
    }

    fn owner_is(s: &State, r: RangeId, n: NodeId, inc: u8) -> bool {
        matches!(s.owners[r as usize],
            Some(o) if o.node == n && o.inc == inc && !o.released && !o.retired)
    }

    /// The keeper is dropped (a failed open, `Resign`): release if the record is still ours.
    fn release(s: &mut State, r: RangeId, n: NodeId, inc: u8) {
        if Self::owner_is(s, r, n, inc) {
            if let Some(o) = &mut s.owners[r as usize] {
                o.released = true;
            }
        }
    }

    fn tombstone(s: &mut State, r: RangeId) {
        if let Some(o) = &mut s.owners[r as usize] {
            o.released = true;
            o.retired = true;
        }
    }

    /// A failed open: the engine drops its keeper (releases) and the leadership loop retries.
    fn fail_open(s: &mut State, n: NodeId, r: RangeId, inc: u8) {
        s.nodes[n as usize].engines[r as usize] = Engine::Idle;
        Self::release(s, r, n, inc);
    }

    fn active(&self, s: &State, n: NodeId, r: RangeId) -> bool {
        s.coord.node == n && s.coord.phase.holds_guard() && self.is_source(r)
    }

    fn marked(&self, s: &State, n: NodeId, r: RangeId) -> bool {
        s.coord.node == n && s.coord.marked && self.is_source(r)
    }

    fn claimable(&self, s: &State, n: NodeId, r: RangeId) -> bool {
        match s.owners[r as usize] {
            None => true,
            Some(o) => {
                !o.retired
                    && (!self.fences.owner_cas
                        || o.released
                        || o.node == n
                        || self.session_dead(s, &o))
            }
        }
    }

    /// The "single writer" invariant: each Db has at most one engine able to
    /// write ([`Self::can_write`]) — LS-2 + LS-3 + LS-4 + LS-5 together.
    pub fn single_writer(&self, s: &State) -> bool {
        (0..self.range_count() as RangeId)
            .all(|r| (0..self.nodes).filter(|&n| self.can_write(s, n, r)).count() <= 1)
    }

    /// SM-13: from when the coordinator quiesces the sources until commit or
    /// roll-back — intent still on the table — no engine can write to a source.
    pub fn quiesced_source_closed(&self, s: &State) -> bool {
        !(s.coord.phase.source_quiesced() && s.table == Table::Intent)
            || self
                .sources()
                .iter()
                .all(|&r| (0..self.nodes).all(|n| !self.can_write(s, n, r)))
    }

    /// Every engine still holding a valid lease on range `r`, with `r` satisfying `want`.
    fn leaders(&self, s: &State, want: impl Fn(RangeId, NodeId, u8) -> bool) -> bool {
        (0..self.nodes).all(|n| {
            s.nodes[n as usize]
                .engines
                .iter()
                .enumerate()
                .all(|(r, e)| match e.claim_inc() {
                    Some(inc) if self.alive(s, n, inc) => want(r as RangeId, n, inc),
                    _ => true,
                })
        })
    }

    /// LS-3: an engine with a valid grant is exactly the one the owner record
    /// points at (not released) — of two contenders exactly one is the owner.
    pub fn leader_is_recorded_owner(&self, s: &State) -> bool {
        self.leaders(s, |r, n, inc| Self::owner_is(s, r, n, inc))
    }

    /// SM-17: commit won ⇒ the closed mark of every source stays (no remover
    /// — `reopen_sources`, `heal_stale_close` — touches a retired source).
    pub fn committed_source_stays_closed(&self, s: &State) -> bool {
        !self.fences.closed_flag
            || s.table != Table::Committed
            || self.sources().iter().all(|&r| s.closed[r as usize])
    }

    /// Range `r` is live (in the `cluster.conf` table, no intent) while its Db
    /// carries the closed mark — left only by a roll-back that died midway (or a
    /// failed removal).
    pub fn stale_close(&self, s: &State, r: RangeId) -> bool {
        s.closed[r as usize] && self.contains(s.table, r) && !self.intent_on(s.table, r)
    }

    /// LS-8: no engine holds a valid grant on a range carrying a tombstone.
    pub fn retired_never_led(&self, s: &State) -> bool {
        self.leaders(s, |r, _, _| {
            !s.owners[r as usize].is_some_and(|o| o.retired)
        })
    }

    fn step(&self, s: &State, a: Action) -> Option<State> {
        let mut s = s.clone();
        let f = self.fences;
        match a {
            Action::Tick => {
                let cap = self.age_cap();
                let mut changed = false;
                for ss in &mut s.sessions {
                    // An ended session: its age matters only for `alive`, so it
                    // stops at `validity` (does not inflate the state space).
                    let cap = if ss.ended { self.validity } else { cap };
                    if ss.age < cap {
                        ss.age += 1;
                        changed = true;
                    }
                    if self.quiet_lease() && !ss.ended && ss.quiet < self.ttl {
                        ss.quiet += 1;
                        changed = true;
                    }
                    if let Mode::Solo { challenge: Some(a) } = &mut ss.mode {
                        if *a < self.ttl && !ss.ended {
                            *a += 1;
                            changed = true;
                        }
                    }
                    if let Some(a) = &mut ss.rearm_age {
                        if *a < self.ttl && !ss.ended {
                            *a += 1;
                            changed = true;
                        }
                    }
                    if ss.touched {
                        ss.touched = false;
                        changed = true;
                    }
                    if let Some(hb) = &mut ss.hb {
                        if hb.age < self.validity {
                            hb.age += 1;
                            changed = true;
                        }
                    }
                }
                if let Some(hub) = &mut s.hub {
                    for t in &mut hub.terms {
                        if let Some(a) = &mut t.lost_age {
                            if *a < self.ttl {
                                *a += 1;
                                changed = true;
                            }
                        }
                    }
                    for (m, v) in hub.views.iter_mut().enumerate() {
                        if v.tau < self.ttl && self.delegator(m as NodeId) {
                            v.tau += 1;
                            changed = true;
                        }
                    }
                }
                if !changed {
                    return None;
                }
            }
            Action::Renew(n) => {
                if s.nodes[n as usize].dead {
                    return None;
                }
                let max_inc = self.max_incarnations;
                let ss = &mut s.sessions[n as usize];
                // A mode that does not PUT renewals (`solo`): the renewal loop is
                // left with only the `lose` branch (paused past the cap) and
                // opening a new incarnation.
                let puts = ss.mode == Mode::Direct && ss.belief != Belief::Delegated;
                if !ss.ended && ss.rearm_age.is_some() {
                    // The record was overwritten by an observer ⇒ the renewal CAS
                    // misses, the re-read sees a foreign beat ⇒ `lose` (the grant
                    // is revoked). See "deliberately left out of the model" about
                    // `ended`.
                    ss.ended = true;
                    ss.age = ss.age.max(self.validity);
                    ss.lost = self.delegation;
                } else if !ss.ended && ss.age < self.validity {
                    if ss.age == 0 || !puts {
                        return None;
                    }
                    self.renewed(&mut s, n);
                } else if !ss.ended {
                    // `lose`: paused past the cap, or the renewal CAS missed —
                    // the grant is revoked, `closed` is written. Revival is
                    // `RenewLate`.
                    ss.ended = true;
                    ss.lost = self.delegation;
                } else if ss.inc < max_inc {
                    // `open_incarnation`.
                    *ss = Session {
                        inc: ss.inc + 1,
                        ..Session::FRESH
                    };
                    if let Some(hub) = &mut s.hub {
                        hub.views[n as usize] = View::FRESH;
                    }
                    // Answers of the old incarnation never pass `alive` or the
                    // fence again.
                    s.nodes[n as usize].read = None;
                } else {
                    return None;
                }
            }
            Action::RenewLate(n) => {
                if s.nodes[n as usize].dead {
                    return None;
                }
                let ss = &mut s.sessions[n as usize];
                // Only while paused: past validity (in time it is `Renew`); only
                // in a mode that PUTs renewals.
                if ss.age < self.validity
                    || ss.mode != Mode::Direct
                    || ss.belief == Belief::Delegated
                {
                    return None;
                }
                if f.revive_by_cas {
                    // A CAS from the version of the last renewal: misses when an
                    // observer has CASed `expired` or overwritten — the `lose`
                    // branch of [`Action::Renew`].
                    if ss.ended || ss.rearm_age.is_some() {
                        return None;
                    }
                } else {
                    // Unfenced: writes `live` over everything (even `expired`).
                    ss.ended = false;
                    ss.rearm_age = None;
                }
                self.renewed(&mut s, n);
            }
            Action::ObserveExpired(n) => {
                // The coordinator is an observer too (its tracker) — when it is
                // node 0 ([`M1::hub_node`]) it is already counted in
                // `observer`.
                let observer = self.observer(&s, n) || (self.delegation && !self.hub_node);
                let ss = s.sessions[n as usize];
                let quiet = self.quiet(&ss) >= self.ttl;
                // After an overwrite the observer counts silence again from there.
                let rearm_quiet = ss.rearm_age.is_none_or(|a| a >= self.ttl);
                // LS-1 applies only to `direct` records. For `solo`/`delegated`
                // records only an observer running an old binary (gate off)
                // counts silence; for `delegated` records the coordinator's
                // yield rule applies.
                let due = match ss.mode {
                    Mode::Direct => quiet,
                    Mode::Solo { .. } => !f.quiet_lease_gate && quiet,
                    Mode::Delegated { term } => {
                        let hub = s.hub.as_ref().expect("a delegated record needs a hub");
                        let lost_age = hub.terms[term as usize - 1].lost_age.unwrap_or(0);
                        let yielded = hub.lost(term)
                            && if f.observer_defers_to_coordinator {
                                lost_age >= self.ttl && (!f.yield_restarts_on_write || quiet)
                            } else {
                                quiet
                            };
                        (!f.delegation_gate && quiet) || yielded
                    }
                };
                if ss.ended || !due || !rearm_quiet || !observer {
                    return None;
                }
                Self::expire_by_other(&mut s, n);
            }
            Action::JudgeUnexpirable(n) => {
                let observer = (0..self.nodes).any(|m| m != n && !s.nodes[m as usize].dead);
                let ss = &mut s.sessions[n as usize];
                if !self.torn_views || ss.ended || !observer {
                    return None;
                }
                if f.unexpirable_rearm {
                    // One overwrite is enough: later ones (the code overwrites
                    // again while the `expired` CAS keeps missing) only move the
                    // silence reference — later, never earlier.
                    if ss.rearm_age.is_some() {
                        return None;
                    }
                    ss.rearm_age = Some(0);
                } else {
                    if ss.judged {
                        return None;
                    }
                    ss.judged = true;
                }
            }
            Action::ExpireUnderLock(n) => {
                // The lock's premise: the owner is no longer running. The real
                // lock is stronger — **every** other process has died — so this
                // weaker premise covers it.
                let observer = (0..self.nodes).any(|m| m != n && !s.nodes[m as usize].dead);
                let ss = &mut s.sessions[n as usize];
                if ss.ended || !s.nodes[n as usize].dead || !observer {
                    return None;
                }
                Self::expire_by_other(&mut s, n);
            }
            Action::Crash(n) => {
                if s.crashes >= self.max_crashes || s.nodes[n as usize].dead {
                    return None;
                }
                s.crashes += 1;
                let node = &mut s.nodes[n as usize];
                node.dead = true;
                node.reopen_pending = None;
                node.read = None;
                for e in &mut node.engines {
                    if *e != Engine::Gone {
                        *e = Engine::Idle;
                    }
                }
                if s.coord.node == n && s.coord.phase.holds_guard() {
                    s.coord.phase = Phase::Crashed;
                    s.coord.marked = false;
                }
            }
            Action::FollowTable(n) => {
                let node = &mut s.nodes[n as usize];
                if node.dead || node.ram == s.table {
                    return None;
                }
                node.ram = s.table;
            }
            Action::Claim(n, r) => {
                let node = &s.nodes[n as usize];
                let e = node.engines[r as usize];
                let may = match e {
                    Engine::Idle => true,
                    Engine::Gone => false,
                    // Lease lost (grant no longer valid) ⇒ the leadership loop takes it back.
                    _ => !e.claim_inc().is_some_and(|inc| self.alive(&s, n, inc)),
                };
                if node.dead
                    || !may
                    || !self.contains(node.ram, r)
                    || !self.session_live(&s, n)
                    || !self.claimable(&s, n, r)
                {
                    return None;
                }
                let inc = s.sessions[n as usize].inc;
                s.owners[r as usize] = Some(Owner {
                    node: n,
                    inc,
                    released: false,
                    retired: false,
                });
                s.nodes[n as usize].engines[r as usize] = Engine::Claimed { inc, fresh: false };
            }
            Action::ReloadForOpen(n, r, read) => {
                let Engine::Claimed { inc, fresh: false } = s.nodes[n as usize].engines[r as usize]
                else {
                    return None;
                };
                if !self.alive(&s, n, inc) {
                    return None;
                }
                // SM-13: `wait_unquiesced` — wait, keeping the lease.
                if f.wait_unquiesced && self.marked(&s, n, r) {
                    return None;
                }
                match read {
                    Read::Ok => {
                        let table = s.table;
                        let node = &mut s.nodes[n as usize];
                        node.ram = table;
                        node.engines[r as usize] = Engine::Claimed { inc, fresh: true };
                    }
                    Read::Failed if f.fresh_table_before_open => Self::fail_open(&mut s, n, r, inc),
                    // RD1: the error is swallowed, the open carries on with the RAM table.
                    Read::Failed => {
                        s.nodes[n as usize].engines[r as usize] =
                            Engine::Claimed { inc, fresh: true };
                    }
                }
            }
            Action::OpenDb(n, r, read) => {
                let Engine::Claimed { inc, fresh: true } = s.nodes[n as usize].engines[r as usize]
                else {
                    return None;
                };
                // The open is in the middle of removing the closed mark of its own roll-back.
                if !self.alive(&s, n, inc) || s.nodes[n as usize].reopen_pending == Some(r) {
                    return None;
                }
                // `read` matters only on the closed-mark branch: every other
                // branch is generated only once (with `Read::Ok`).
                let hits_close = f.closed_flag && s.closed[r as usize];
                if !self.contains(s.nodes[n as usize].ram, r) {
                    if read == Read::Failed {
                        return None;
                    }
                    Self::fail_open(&mut s, n, r, inc);
                    return Some(s);
                }
                // SM-3: an intent this node's coordinator did not make ⇒ roll back
                // (CAS dropping the intent; on a loss, apply the winner's table and
                // look again).
                if f.roll_back_foreign_intent
                    && self.intent_on(s.nodes[n as usize].ram, r)
                    && !self.active(&s, n, r)
                {
                    if read == Read::Failed {
                        return None;
                    }
                    if s.table == Table::Intent {
                        s.table = Table::RolledBack;
                        s.ghost.foreign_roll_back = true;
                        s.nodes[n as usize].ram = s.table;
                        if f.closed_flag {
                            // `roll_back_matching`: CAS won ⇒ `reopen_sources`
                            // is a separate step ([`Action::ReopenSources`]); the
                            // node may die between the two steps.
                            s.nodes[n as usize].reopen_pending = Some(r);
                            return Some(s);
                        }
                    }
                    s.nodes[n as usize].ram = s.table;
                    if !self.contains(s.table, r) {
                        Self::fail_open(&mut s, n, r, inc);
                        return Some(s);
                    }
                }
                // SM-13's tripwire (`node::open::quiesced_source`).
                if f.wait_unquiesced && self.marked(&s, n, r) {
                    if read == Read::Failed {
                        return None;
                    }
                    Self::fail_open(&mut s, n, r, inc);
                    return Some(s);
                }
                // SM-17: SlateDB refuses a writer opening a Db that carries the
                // closed mark — checked in the same CAS that bumps `writer_epoch`,
                // looking at no table. The open fails, then `heal_stale_close`
                // removes the mark only when a **successful** table read shows the
                // range still in the table with no intent.
                if hits_close {
                    Self::fail_open(&mut s, n, r, inc);
                    if f.heal_stale_close && read == Read::Ok {
                        let table = s.table;
                        s.nodes[n as usize].ram = table;
                        if self.contains(table, r) && !self.intent_on(table, r) {
                            s.closed[r as usize] = false;
                            s.ghost.healed = true;
                        }
                    }
                    return Some(s);
                }
                if read == Read::Failed {
                    return None;
                }
                if f.close_checks_epoch && self.is_source(r) && s.coord.phase == Phase::Quiesced {
                    s.coord.reopened = true;
                }
                // `PartitionWriter::open`: bumps `writer_epoch`, fences the old writer.
                for (m, node) in s.nodes.iter_mut().enumerate() {
                    if m != n as usize {
                        if let Engine::Open { inc, .. } = node.engines[r as usize] {
                            node.engines[r as usize] = Engine::Open { inc, fenced: true };
                        }
                    }
                }
                s.nodes[n as usize].engines[r as usize] = Engine::Open { inc, fenced: false };
            }
            Action::WriterFenced(n, r) => {
                // The lazy fence of `close_for_writes`: until now the writer could
                // still ack (see `CloseSources`). The engine exits, the keeper
                // drops (releases), the leadership loop takes it back — and the
                // next open meets the closed mark.
                let Engine::Open { inc, .. } = s.nodes[n as usize].engines[r as usize] else {
                    return None;
                };
                if !(f.closed_flag && s.closed[r as usize]) {
                    return None;
                }
                Self::fail_open(&mut s, n, r, inc);
            }
            Action::Ack(n, r) => {
                if !self.can_write(&s, n, r) {
                    return None;
                }
                let before = s.ghost;
                if !self.contains(s.table, r) {
                    // The range has left the table: an ack into a Db that is retiring or retired (RD1).
                    s.ghost.lost_ack = true;
                }
                if self.is_source(r) && s.table == Table::Intent && s.coord.phase.cloned() {
                    // After the clone snapshot: the child lacks this write (RD2 if the commit wins).
                    s.ghost.source_acked_after_clone = true;
                }
                if self.children().contains(&r) {
                    s.ghost.child_acked = true;
                }
                if s.ghost == before {
                    return None;
                }
            }
            Action::ServeRead(n, r) => {
                // Only an engine with an open Db answers (not when quiesced/sealed).
                // A writer fenced by `writer_epoch` **can still** read — that fence
                // only blocks writes; this is exactly where stale reads happen.
                let Engine::Open { inc, .. } = s.nodes[n as usize].engines[r as usize] else {
                    return None;
                };
                if !self.alive(&s, n, inc) {
                    return None;
                }
                let ss = s.sessions[n as usize];
                if f.read_fence && (ss.inc != inc || ss.ended) {
                    return None;
                }
                if Self::owner_is(&s, r, n, inc) {
                    // A fresh read: no state change (keeps the space small).
                    return None;
                }
                s.ghost.stale_read = true;
            }
            Action::ReadStart(n, r) => {
                let node = &s.nodes[n as usize];
                if !self.split_reads || node.dead || node.read.is_some() {
                    return None;
                }
                let Engine::Open { inc, .. } = node.engines[r as usize] else {
                    return None;
                };
                // Only stale reads are tracked (see `Node::read`).
                if Self::owner_is(&s, r, n, inc) {
                    return None;
                }
                s.nodes[n as usize].read = Some(PendingRead::Started { inc });
            }
            Action::ReadTicket(n) => {
                let Some(PendingRead::Started { inc }) = s.nodes[n as usize].read else {
                    return None;
                };
                // `Lease::sync_with`: if paused, wait (revival is `RenewLate`
                // before this step), then `durability()` ⇒ `alive`.
                if !self.alive(&s, n, inc) {
                    return None;
                }
                let node = &mut s.nodes[n as usize];
                // The faulty configuration: proof that **already existed** (issued
                // before the ticket, so possibly before the read) counts as well.
                let proven = !f.proof_after_read && node.last_proof == Some(inc);
                node.read = Some(PendingRead::Waiting { inc, proven });
            }
            Action::ProbeSession(n) => {
                let Some(PendingRead::Waiting { inc, proven: false }) = s.nodes[n as usize].read
                else {
                    return None;
                };
                let ss = s.sessions[n as usize];
                // `confirm`: `304`, or a `200` that is `live` + our node (an
                // observer's overwrite too — still live). The path carries the
                // incarnation.
                if ss.inc == inc && !ss.ended {
                    self.proof(&mut s, n);
                } else {
                    // Ended / incarnation abandoned ⇒ `LeaseLost`.
                    s.nodes[n as usize].read = None;
                }
            }
            Action::ReadReply(n) => {
                let Some(PendingRead::Waiting { proven, .. }) = s.nodes[n as usize].read else {
                    return None;
                };
                if f.read_fence && !proven {
                    return None;
                }
                // Every tracked read is stale.
                s.nodes[n as usize].read = None;
                s.ghost.stale_read = true;
            }
            Action::Reconcile(n, r) => {
                let node = &s.nodes[n as usize];
                let e = node.engines[r as usize];
                if node.dead
                    || !self.is_source(r)
                    || self.contains(node.ram, r)
                    || e == Engine::Gone
                {
                    return None;
                }
                if let Some(inc) = e.claim_inc() {
                    if self.alive(&s, n, inc) && Self::owner_is(&s, r, n, inc) {
                        Self::tombstone(&mut s, r);
                    }
                }
                s.nodes[n as usize].engines[r as usize] = Engine::Gone;
            }
            Action::RetireOrphan(n, r) => {
                if s.nodes[n as usize].dead || !self.is_source(r) || s.table != Table::Committed {
                    return None;
                }
                let o = s.owners[r as usize]?;
                if o.retired || !(o.released || self.session_dead(&s, &o)) {
                    return None;
                }
                Self::tombstone(&mut s, r);
            }
            Action::IntentCas(n) => {
                if s.coord.phase != Phase::NotStarted
                    || s.nodes[n as usize].dead
                    || s.table != Table::Initial
                    || s.nodes[n as usize].ram != Table::Initial
                    || !self.sources().iter().all(|&r| self.can_write(&s, n, r))
                {
                    return None;
                }
                s.table = Table::Intent;
                s.nodes[n as usize].ram = Table::Intent;
                s.coord = Coord {
                    node: n,
                    phase: Phase::Intent,
                    marked: false,
                    reopened: false,
                };
            }
            Action::MarkQuiesced => {
                if s.coord.phase != Phase::Intent || s.coord.marked {
                    return None;
                }
                s.coord.marked = true;
            }
            Action::Quiesce => {
                let n = s.coord.node;
                if s.coord.phase != Phase::Intent
                    || !s.coord.marked
                    || !self.sources().iter().all(|&r| self.can_write(&s, n, r))
                {
                    return None;
                }
                for &r in self.sources() {
                    let e = &mut s.nodes[n as usize].engines[r as usize];
                    if let Engine::Open { inc, .. } = *e {
                        *e = Engine::Quiesced { inc };
                    }
                }
                s.coord.phase = Phase::Quiesced;
            }
            Action::CloseSources => {
                // `ensure_intent_ours` then `close_range_db`: two non-atomic
                // steps, so the close here does **not** look at the table (another
                // roll-backer may cut in between) — `Clone` re-checks the intent as
                // the code does. Closing bumps `writer_epoch`, but an open writer is
                // fenced only at its **next** manifest update (an ack goes through
                // the WAL, not touching the manifest): the model lets the open
                // writer keep writing — an over-approximation, sound for safety.
                if s.coord.phase != Phase::Quiesced {
                    return None;
                }
                if f.close_checks_epoch && s.coord.reopened {
                    // `close_range_db_at`: the epoch has moved past the quiesced
                    // writer ⇒ `Fenced`, no close.
                    return None;
                }
                if f.closed_flag {
                    for &r in self.sources() {
                        s.closed[r as usize] = true;
                    }
                    if s.table != Table::Intent {
                        s.ghost.late_close = true;
                    }
                }
                s.coord.phase = Phase::Closed;
            }
            Action::Clone => {
                if s.coord.phase != Phase::Closed || s.table != Table::Intent {
                    return None;
                }
                s.nodes[s.coord.node as usize].ram = s.table;
                s.coord.phase = Phase::Cloned;
            }
            Action::ClaimChildren => {
                let n = s.coord.node;
                if s.coord.phase != Phase::Cloned
                    || s.table != Table::Intent
                    || !self.session_live(&s, n)
                {
                    return None;
                }
                let inc = s.sessions[n as usize].inc;
                for &c in self.children() {
                    s.owners[c as usize] = Some(Owner {
                        node: n,
                        inc,
                        released: false,
                        retired: false,
                    });
                }
                s.nodes[n as usize].ram = s.table;
                s.coord.phase = Phase::ChildrenClaimed;
            }
            Action::CommitCas => {
                if s.coord.phase != Phase::ChildrenClaimed || s.table != Table::Intent {
                    return None;
                }
                s.table = Table::Committed;
                s.nodes[s.coord.node as usize].ram = Table::Committed;
                s.coord.phase = Phase::Committed;
                if s.ghost.source_acked_after_clone {
                    s.ghost.lost_ack = true;
                }
            }
            Action::Retire => {
                let n = s.coord.node;
                if s.coord.phase != Phase::Committed {
                    return None;
                }
                for &r in self.sources() {
                    let e = s.nodes[n as usize].engines[r as usize];
                    if let Engine::Quiesced { inc } = e {
                        if self.alive(&s, n, inc) && Self::owner_is(&s, r, n, inc) {
                            Self::tombstone(&mut s, r);
                            s.nodes[n as usize].engines[r as usize] = Engine::Gone;
                        }
                    }
                }
                s.coord.marked = false;
                s.coord.phase = Phase::Done;
            }
            Action::RollBack => {
                let n = s.coord.node;
                if !s.coord.phase.before_commit() {
                    return None;
                }
                let won = s.table == Table::Intent;
                if won {
                    s.table = Table::RolledBack;
                }
                s.nodes[n as usize].ram = s.table;
                if won && f.closed_flag {
                    // `roll_back_matching`: CAS won ⇒ `reopen_sources` is a
                    // separate step; the guard is held until it is done.
                    s.coord.phase = Phase::RollingBack;
                    return Some(s);
                }
                // CAS lost (someone else rolled back): do not remove the closed
                // marks — that is the winner's job (or `heal_stale_close`'s).
                self.finish_roll_back(&mut s);
            }
            Action::ReopenSources(n, io) => {
                if s.nodes[n as usize].dead {
                    return None;
                }
                let coordinator = s.coord.node == n && s.coord.phase == Phase::RollingBack;
                if !coordinator && s.nodes[n as usize].reopen_pending.is_none() {
                    return None;
                }
                let closed = self.sources().iter().any(|&r| s.closed[r as usize]);
                if io == Io::Failed && !closed {
                    return None;
                }
                if io == Io::Ok {
                    for &r in self.sources() {
                        s.closed[r as usize] = false;
                    }
                }
                if coordinator {
                    self.finish_roll_back(&mut s);
                } else {
                    s.nodes[n as usize].reopen_pending = None;
                }
            }
            Action::EnterSolo(n) => {
                // The gate and "being the only session" are only cost: the model
                // allows entering the mode at any time — an over-approximation.
                // A coordinator that is a node ([`M1::hub_node`]): only it enters
                // `solo` — a delegating node is in `solo` only once alone, i.e.
                // the plain `solo` protocol, already checked by the `solo`
                // configurations.
                if !self.solo
                    || (self.hub_node && !self.hub_at(n))
                    || !Self::own_cas_wins(&s, n)
                    || s.sessions[n as usize].mode != Mode::Direct
                    || s.sessions[n as usize].belief != Belief::Direct
                {
                    return None;
                }
                s.sessions[n as usize].mode = Mode::Solo { challenge: None };
                self.renewed(&mut s, n);
            }
            Action::SoloCheck(n) => {
                let ss = &s.sessions[n as usize];
                let Mode::Solo { challenge } = ss.mode else {
                    return None;
                };
                // The GET sees exactly the `solo` record the owner last wrote
                // (`304`, or a `200` with the same content): not ended, not
                // overwritten, and — fenced — with no `challenged` line yet.
                // Issued at t ⇒ deadline t + validity: the model merges issue and
                // serve (serving after the issue time only shortens the deadline).
                if !Self::own_cas_wins(&s, n)
                    || (f.solo_extend_needs_unchallenged_read && challenge.is_some())
                    || ss.age == 0
                {
                    return None;
                }
                s.sessions[n as usize].age = 0;
                self.proof(&mut s, n);
            }
            Action::SoloWrite(n) => {
                // CAS If-Match the etag of the confirmed `solo` version: misses
                // when the record has ended, been overwritten or carries a
                // `challenged` line.
                if !Self::own_cas_wins(&s, n)
                    || s.sessions[n as usize].mode != (Mode::Solo { challenge: None })
                    || s.sessions[n as usize].age == 0
                {
                    return None;
                }
                self.renewed(&mut s, n);
            }
            Action::Challenge(n) => {
                let observer = self.observer(&s, n);
                let ss = &mut s.sessions[n as usize];
                if !observer || ss.ended || ss.mode != (Mode::Solo { challenge: None }) {
                    return None;
                }
                // CAS If-Match the etag just read; τ = when the CAS was confirmed.
                // The beat does not change (the `challenged` line is not the
                // owner's write).
                ss.mode = Mode::Solo { challenge: Some(0) };
                Self::version_changed(&mut s, n);
            }
            Action::AnswerChallenge(n) => {
                if !Self::own_cas_wins(&s, n) {
                    return None;
                }
                let Mode::Solo { challenge } = s.sessions[n as usize].mode else {
                    return None;
                };
                // CAS If-Match the new etag (with the `challenged` line), back to
                // `direct`, beat + 1: won ⇒ a renewal under LS-2. If the challenger
                // has CASed `expired`, this CAS misses — `Renew`'s `lose` branch.
                s.sessions[n as usize].mode = Mode::Direct;
                if challenge.is_some() {
                    s.ghost.challenge_answered = true;
                }
                self.renewed(&mut s, n);
            }
            Action::ExpireChallenged(n) => {
                let observer = self.observer(&s, n);
                let quiet = self.quiet(&s.sessions[n as usize]);
                let ss = &mut s.sessions[n as usize];
                let Mode::Solo { challenge } = ss.mode else {
                    return None;
                };
                let due = if f.expire_solo_needs_challenge_then_ttl {
                    // The record has stayed exactly as after the challenge (CAS If-Match) for one TTL.
                    challenge.is_some_and(|a| a >= self.ttl)
                } else {
                    quiet >= self.ttl
                };
                if !observer || ss.ended || !due {
                    return None;
                }
                Self::expire_by_other(&mut s, n);
                s.ghost.solo_expired = true;
            }
            Action::Heartbeat(n) => {
                let ss = &s.sessions[n as usize];
                let hub = s.hub.as_ref()?;
                // Only a delegated or delegating node needs acks (a `direct` node's
                // heartbeat only refreshes τ — it can never break safety).
                if !self.heartbeat_in_flight
                    || !self.delegator(n)
                    || s.nodes[n as usize].dead
                    || !Self::delegating(ss)
                {
                    return None;
                }
                let term = match ss.mode {
                    Mode::Delegated { term } => term,
                    _ => hub.term,
                };
                let hb = Heartbeat {
                    age: 0,
                    term,
                    touched: ss.touched,
                };
                if ss.hb == Some(hb) {
                    return None;
                }
                s.sessions[n as usize].hb = Some(hb);
            }
            Action::HeartbeatAck(n) => {
                let ss = s.sessions[n as usize];
                let hb = if self.heartbeat_in_flight {
                    ss.hb?
                } else {
                    // Send and receive in one step (see [`M1::heartbeat_in_flight`]).
                    if !Self::delegating(&ss) || s.nodes[n as usize].dead {
                        return None;
                    }
                    Heartbeat {
                        age: 0,
                        term: match ss.mode {
                            Mode::Delegated { term } => term,
                            _ => s.hub.as_ref()?.term,
                        },
                        touched: ss.touched,
                    }
                };
                if !self.hub_running(&s) {
                    return None;
                }
                let hub = s.hub.as_mut()?;
                let (ri, rt) = hub.role?;
                s.sessions[n as usize].hb = None;
                let ours = hub.terms[rt as usize - 1].inc == ri;
                // The coordinator fence (ack rules 2–3), issued after receiving the
                // batch: `control/coordinator` has the current incarnation and the
                // role's term, not released; its own session is still `live`; the
                // heartbeat has the same term.
                let fenced = !f.coordinator_fence
                    || (ours
                        && hub.term == rt
                        && !hub.released
                        && (!f.fence_checks_incarnation || hub.inc == ri)
                        && !hub.ended
                        && (!f.ack_checks_heartbeat_term || hb.term == rt));
                let v = &mut hub.views[n as usize];
                // GET to confirm the delegated version (once) — as in
                // [`Action::CoordinatorRead`]: a version not held is a new version,
                // τ resets.
                if v.held == Held::None
                    && ss.mode == (Mode::Delegated { term: rt })
                    && !ss.ended
                    && ours
                {
                    v.held = Held::Current;
                    v.tau = 0;
                }
                let not_expired = match v.issued {
                    Issued::None => true,
                    Issued::Ambiguous => !f.no_ack_after_expire,
                    Issued::Won => false,
                };
                let touched = !f.self_check_gates_ack || hb.touched;
                // τ(N) = when a heartbeat the coordinator **acks** arrived: a
                // heartbeat that is not acked renews no grant, so it need not be
                // counted. Counting it too (the model's first version) means a node
                // cut off from the bucket that still sends heartbeats is never
                // expired — ack rule 6 loses its liveness. A heartbeat not acked in
                // the code (ack rule 4 fails) ≡ a lost heartbeat here.
                let acks = fenced && not_expired && touched;
                if acks {
                    v.tau = 0;
                }
                // The node applies the ack: deadline = heartbeat issue time + validity.
                let uses = if f.ack_needs_confirmed_delegation {
                    ss.belief == Belief::Delegated
                } else {
                    ss.belief != Belief::Direct
                };
                if acks
                    && uses
                    && !(f.ack_respects_pause_cap && ss.lost)
                    && !s.nodes[n as usize].dead
                    && hb.age < self.validity
                    && hb.age < ss.age
                {
                    s.sessions[n as usize].age = hb.age;
                }
            }
            Action::Delegate(n, cas) => {
                let hub = s.hub.as_ref()?;
                let ss = &s.sessions[n as usize];
                // Target: `control/coordinator` as the node knows it — it may have
                // released or lost the role (a late `granted`, a stale `view`): an
                // over-approximation.
                let term = hub.term;
                let from = match ss.mode {
                    Mode::Direct => true,
                    // Re-delegating to a new term (one CAS, "changing coordinator").
                    Mode::Delegated { term: t } => t != term,
                    Mode::Solo { .. } => false,
                };
                if !self.delegator(n)
                    || !Self::own_cas_wins(&s, n)
                    || !from
                    || ss.belief == Belief::Unsure
                {
                    return None;
                }
                let ss = &mut s.sessions[n as usize];
                match cas {
                    Cas::Won => {
                        // A winning `direct` renewal (LS-2).
                        ss.mode = Mode::Delegated { term };
                        ss.belief = Belief::Delegated;
                        self.renewed(&mut s, n);
                    }
                    Cas::AmbiguousApplied => {
                        ss.mode = Mode::Delegated { term };
                        ss.belief = Belief::Unsure;
                        self.wrote(&mut s, n);
                    }
                    Cas::AmbiguousLost => ss.belief = Belief::Unsure,
                }
            }
            Action::Fallback(n) => {
                if !self.delegator(n)
                    || !Self::own_cas_wins(&s, n)
                    || !Self::delegating(&s.sessions[n as usize])
                {
                    return None;
                }
                let ss = &mut s.sessions[n as usize];
                ss.mode = Mode::Direct;
                ss.belief = Belief::Direct;
                self.renewed(&mut s, n);
            }
            Action::SelfCheck(n) => {
                let validity = self.validity;
                let touch = self.tracks_touch(n);
                let ss = &mut s.sessions[n as usize];
                if !self.delegator(n) || s.nodes[n as usize].dead || !Self::delegating(ss) {
                    return None;
                }
                let before = *ss;
                if ss.ended {
                    // Sees it ended ⇒ `lose`.
                    ss.age = ss.age.max(validity);
                    ss.lost = true;
                } else {
                    ss.touched = touch;
                    if ss.belief == Belief::Unsure {
                        ss.belief = match ss.mode {
                            Mode::Delegated { .. } => Belief::Delegated,
                            _ => Belief::Direct,
                        };
                    }
                }
                if *ss == before {
                    return None;
                }
                if !s.sessions[n as usize].ended {
                    self.proof(&mut s, n);
                }
            }
            Action::RecordWrite(n) => {
                let ss = &s.sessions[n as usize];
                if !self.delegator(n)
                    || !Self::own_cas_wins(&s, n)
                    || ss.belief != Belief::Delegated
                    || !matches!(ss.mode, Mode::Delegated { .. })
                {
                    return None;
                }
                if f.delegated_write_no_extend {
                    self.wrote(&mut s, n);
                } else {
                    self.renewed(&mut s, n);
                }
            }
            Action::CoordinatorExpire(n, e) => {
                if !self.hub_running(&s) {
                    return None;
                }
                let ss = s.sessions[n as usize];
                let hub = s.hub.as_mut()?;
                // An old role (zombie) can expire too: the expire rules rest on τ,
                // not on the role.
                let (ri, rt) = hub.role?;
                let ours = hub.terms[rt as usize - 1].inc == ri;
                let v = &mut hub.views[n as usize];
                if v.held == Held::None || v.tau < self.ttl || v.issued != Issued::None {
                    return None;
                }
                if f.expire_if_match_delegated {
                    if v.held == Held::Stale {
                        // CAS missed ⇒ GET again: beat increased ⇒ a new τ; if it is
                        // still delegated to us, hold the new version.
                        v.tau = 0;
                        v.held = if ss.mode == (Mode::Delegated { term: rt }) && !ss.ended && ours {
                            Held::Current
                        } else {
                            Held::None
                        };
                        return Some(s);
                    }
                } else if e == Expiry::Ambiguous {
                    // A blind write has no "did not land": ambiguous ≡ winning later.
                    return None;
                }
                if ss.ended {
                    return None;
                }
                match e {
                    Expiry::Won => {
                        v.issued = Issued::Won;
                        Self::expire_by_other(&mut s, n);
                    }
                    Expiry::Ambiguous => {
                        v.issued = Issued::Ambiguous;
                        s.sessions[n as usize].pending_expire = true;
                    }
                }
            }
            Action::CoordinatorRead(n) => {
                if !self.hub_running(&s) {
                    return None;
                }
                let ss = s.sessions[n as usize];
                let hub = s.hub.as_mut()?;
                let (ri, rt) = hub.role?;
                let ours = hub.terms[rt as usize - 1].inc == ri;
                let v = &mut hub.views[n as usize];
                if v.held == Held::Current
                    || ss.ended
                    || !ours
                    || ss.mode != (Mode::Delegated { term: rt })
                {
                    return None;
                }
                // A version delegated to the current role that the coordinator
                // does not hold is a version it has not read: the record becomes
                // `delegated` for this term only by an owner write **after** the
                // term exists (taking the role resets every `held` to `None`).
                // Code: a beat greater than every beat the tracker has seen ⇒
                // `since` = when it arrived.
                v.tau = 0;
                v.held = Held::Current;
            }
            Action::ExpireLands(n) => {
                let ss = &s.sessions[n as usize];
                if !ss.pending_expire || ss.ended {
                    return None;
                }
                Self::expire_by_other(&mut s, n);
            }
            Action::CoordinatorTakeover => {
                if !self.hub_running(&s) {
                    return None;
                }
                let terms = self.coordinator_terms;
                let hub = s.hub.as_mut()?;
                if hub.ended || !hub.lost(hub.term) || hub.term >= terms {
                    return None;
                }
                hub.term += 1;
                hub.released = false;
                hub.terms.push(Term {
                    inc: hub.inc,
                    lost_age: None,
                });
                hub.role = Some((hub.inc, hub.term));
                // A version delegated to an old term is not "ours in the current
                // term": that record goes by the yield rule. Expiries already
                // issued are remembered.
                for v in &mut hub.views {
                    v.held = Held::None;
                }
            }
            Action::CoordinatorRelease => {
                if !self.hub_running(&s) {
                    return None;
                }
                let hub = s.hub.as_mut()?;
                let (ri, rt) = hub.role?;
                if hub.term == rt && hub.terms[rt as usize - 1].inc == ri && !hub.released {
                    hub.released = true;
                }
                hub.role = None;
            }
            Action::CoordinatorLose => {
                let hub = s.hub.as_mut()?;
                // The coordinator is node 0: its session ends along the node's path
                // (`Renew` → `lose`, challenger, observer).
                if self.hub_node || hub.ended {
                    return None;
                }
                hub.ended = true;
            }
            Action::CoordinatorReopen => {
                let max = self.hub_incarnations;
                let hub = s.hub.as_mut()?;
                if self.hub_node || !hub.ended || hub.inc >= max {
                    return None;
                }
                hub.inc += 1;
                hub.ended = false;
            }
        }
        if let (true, Some(hub)) = (self.hub_node, &mut s.hub) {
            // The coordinator's session is node 0's session.
            hub.inc = s.sessions[0].inc;
            hub.ended = s.sessions[0].ended;
        }
        if let Some(hub) = &mut s.hub {
            // The fastest observer sees the role lost as soon as it is lost.
            for t in 1..=hub.terms.len() as u8 {
                if hub.terms[t as usize - 1].lost_age.is_none() && hub.lost(t) {
                    hub.terms[t as usize - 1].lost_age = Some(0);
                }
            }
        }
        Some(s)
    }

    /// The tail of `roll_back_here`: `Resign` the quiesced sources (release; the
    /// leadership loop takes them back later), the guard drops.
    fn finish_roll_back(&self, s: &mut State) {
        let n = s.coord.node;
        for &r in self.sources() {
            if let Engine::Quiesced { inc } = s.nodes[n as usize].engines[r as usize] {
                Self::fail_open(s, n, r, inc);
            }
        }
        s.coord.marked = false;
        s.coord.phase = Phase::RolledBack;
    }

    /// A configuration symmetric under the permutation of the two nodes: two
    /// nodes, and every node has the same role — no delegation, or every node
    /// delegates. Then [`node_symmetry`] is a sound reduction (argument there).
    /// One delegating node (node 1) next to an always-`direct` node (node 0) is
    /// **not** symmetric.
    pub fn node_symmetric(&self) -> bool {
        self.nodes == 2 && !self.hub_node && (!self.delegation || self.delegators >= self.nodes)
    }
}

/// Symmetry reduction: the representative of the class {s, π(s)} with π
/// swapping nodes 0 ↔ 1 — the state with the smaller fingerprint. Used only
/// when [`M1::node_symmetric`] (tests assert it before switching it on).
///
/// **Why it loses no counterexample.** With a symmetric configuration:
///
/// 1. the initial state is fixed under π (every node, session and view alike;
///    owner records empty; `Coord::node` in [`Phase::NotStarted`] is a dead field
///    — `IntentCas` overwrites it, and every read of it requires another phase —
///    so π does not touch it in that phase);
/// 2. `step(π(s), π(a)) = π(step(s, a))`: [`M1::step`] has no node constant
///    (only `delegator(n)`, true for every node when every node delegates), every
///    quantifier over nodes is `all`/`any`, and π swaps exactly every field that
///    carries a node id — `sessions`, `nodes`, `Hub::views` by index,
///    `Owner::node`, `Coord::node` (outside the initial phase). No other field
///    carries a node id (`Engine` is indexed by range, `Term::inc`/`Hub::role` are
///    the coordinator's incarnation);
/// 3. every property is invariant under π (quantified over every node/range).
///
/// From (1)–(2), the set of reachable states is closed under π and π maps a path
/// to a path; from (3), s violates ⇔ π(s) violates. So exploring one
/// representative per class finds exactly the violations of the full
/// exploration. Two different states with the same fingerprint only lose
/// reduction (each is its own representative), never a state. Stateright
/// continues from the **real** state (only the representative's fingerprint
/// enters the visited set), so a counterexample trace is still a valid path.
///
/// Empirical evidence (`tests/m1.rs`): every breaking row of the delegation
/// matrix, run in the configuration with two delegating nodes (⊇ the behaviour
/// of one delegating node: delegating is an action, not mandatory) with the
/// reduction on, **still breaks** LS-3; and the state count of the earlier
/// symmetric configuration (`delegation_holds_with_two_delegators`) with the
/// reduction is about half of the unreduced count.
pub fn node_symmetry(s: &State) -> State {
    let p = s.swapped();
    if fingerprint(&p) < fingerprint(s) {
        p
    } else {
        s.clone()
    }
}

fn fingerprint(s: &State) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    // `DefaultHasher::new()` has fixed keys: same state, same fingerprint on every thread.
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

impl Action {
    /// π on actions: swaps node ids 0 ↔ 1 (tests check `step(π(s), π(a)) =
    /// π(step(s, a))` on reachable states — item (2) of [`node_symmetry`]).
    pub fn swapped(self) -> Action {
        use Action::*;
        let w = |n: NodeId| 1 - n;
        match self {
            Renew(n) => Renew(w(n)),
            RenewLate(n) => RenewLate(w(n)),
            ObserveExpired(n) => ObserveExpired(w(n)),
            JudgeUnexpirable(n) => JudgeUnexpirable(w(n)),
            ExpireUnderLock(n) => ExpireUnderLock(w(n)),
            Crash(n) => Crash(w(n)),
            FollowTable(n) => FollowTable(w(n)),
            Claim(n, r) => Claim(w(n), r),
            ReloadForOpen(n, r, x) => ReloadForOpen(w(n), r, x),
            OpenDb(n, r, x) => OpenDb(w(n), r, x),
            Ack(n, r) => Ack(w(n), r),
            ServeRead(n, r) => ServeRead(w(n), r),
            ReadStart(n, r) => ReadStart(w(n), r),
            ReadTicket(n) => ReadTicket(w(n)),
            ProbeSession(n) => ProbeSession(w(n)),
            ReadReply(n) => ReadReply(w(n)),
            WriterFenced(n, r) => WriterFenced(w(n), r),
            Reconcile(n, r) => Reconcile(w(n), r),
            RetireOrphan(n, r) => RetireOrphan(w(n), r),
            IntentCas(n) => IntentCas(w(n)),
            ReopenSources(n, io) => ReopenSources(w(n), io),
            EnterSolo(n) => EnterSolo(w(n)),
            SoloCheck(n) => SoloCheck(w(n)),
            SoloWrite(n) => SoloWrite(w(n)),
            Challenge(n) => Challenge(w(n)),
            AnswerChallenge(n) => AnswerChallenge(w(n)),
            ExpireChallenged(n) => ExpireChallenged(w(n)),
            Heartbeat(n) => Heartbeat(w(n)),
            HeartbeatAck(n) => HeartbeatAck(w(n)),
            Delegate(n, c) => Delegate(w(n), c),
            Fallback(n) => Fallback(w(n)),
            SelfCheck(n) => SelfCheck(w(n)),
            RecordWrite(n) => RecordWrite(w(n)),
            CoordinatorExpire(n, e) => CoordinatorExpire(w(n), e),
            CoordinatorRead(n) => CoordinatorRead(w(n)),
            ExpireLands(n) => ExpireLands(w(n)),
            Tick | MarkQuiesced | Quiesce | CloseSources | Clone | ClaimChildren | CommitCas
            | Retire | RollBack | CoordinatorTakeover | CoordinatorRelease | CoordinatorLose
            | CoordinatorReopen => self,
        }
    }
}

impl State {
    /// π: swaps nodes 0 ↔ 1 (see [`node_symmetry`]).
    pub fn swapped(&self) -> State {
        let mut p = self.clone();
        let swap = |n: NodeId| 1 - n;
        p.sessions.swap(0, 1);
        p.nodes.swap(0, 1);
        for o in p.owners.iter_mut().flatten() {
            o.node = swap(o.node);
        }
        if p.coord.phase != Phase::NotStarted {
            p.coord.node = swap(p.coord.node);
        }
        if let Some(hub) = &mut p.hub {
            hub.views.swap(0, 1);
        }
        p
    }
}

impl Model for M1 {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let ranges = self.range_count();
        vec![State {
            table: Table::Initial,
            closed: vec![false; ranges],
            owners: vec![None; ranges],
            sessions: vec![Session::FRESH; self.nodes as usize],
            nodes: vec![
                Node {
                    dead: false,
                    ram: Table::Initial,
                    engines: vec![Engine::Idle; ranges],
                    reopen_pending: None,
                    read: None,
                    last_proof: None,
                };
                self.nodes as usize
            ],
            coord: Coord {
                node: 0,
                phase: Phase::NotStarted,
                marked: false,
                reopened: false,
            },
            hub: self.delegation.then(|| Hub {
                inc: 1,
                ended: false,
                role: Some((1, 1)),
                term: 1,
                released: false,
                terms: vec![Term {
                    inc: 1,
                    lost_age: None,
                }],
                views: vec![View::FRESH; self.nodes as usize],
            }),
            crashes: 0,
            ghost: Ghost {
                lost_ack: false,
                source_acked_after_clone: false,
                child_acked: false,
                foreign_roll_back: false,
                healed: false,
                late_close: false,
                stale_read: false,
                solo_expired: false,
                challenge_answered: false,
            },
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        // A lost ack is a **dead end**: stateright's BFS overwrites the finding
        // on every visit to a violating state, and this ghost variable sticks —
        // continuing only makes the counterexample longer. A fully fenced
        // configuration never gets here. (The other invariants are predicates on
        // the state, not sticky; stopping at them would hide a later violation
        // of another invariant.)
        if s.ghost.lost_ack {
            return;
        }
        out.push(Action::Tick);
        for n in 0..self.nodes {
            out.push(Action::Renew(n));
            out.push(Action::RenewLate(n));
            out.push(Action::ObserveExpired(n));
            if self.torn_views {
                out.push(Action::JudgeUnexpirable(n));
            }
            out.push(Action::ExpireUnderLock(n));
            out.push(Action::Crash(n));
            out.push(Action::FollowTable(n));
            if self.split_op {
                out.push(Action::IntentCas(n));
                out.push(Action::ReopenSources(n, Io::Ok));
                out.push(Action::ReopenSources(n, Io::Failed));
            }
            if self.delegator(n) && self.heartbeat_in_flight {
                out.push(Action::Heartbeat(n));
            }
            if self.delegator(n) {
                out.extend([
                    Action::HeartbeatAck(n),
                    Action::Delegate(n, Cas::Won),
                    Action::Fallback(n),
                    Action::SelfCheck(n),
                    Action::CoordinatorExpire(n, Expiry::Won),
                    Action::CoordinatorRead(n),
                ]);
                if self.delegation_faults {
                    out.extend([
                        Action::Delegate(n, Cas::AmbiguousApplied),
                        Action::Delegate(n, Cas::AmbiguousLost),
                        Action::RecordWrite(n),
                        Action::CoordinatorExpire(n, Expiry::Ambiguous),
                        Action::ExpireLands(n),
                    ]);
                }
            }
            if self.solo {
                out.extend([
                    Action::EnterSolo(n),
                    Action::SoloCheck(n),
                    Action::SoloWrite(n),
                    Action::Challenge(n),
                    Action::AnswerChallenge(n),
                    Action::ExpireChallenged(n),
                ]);
            }
            if self.split_reads {
                out.push(Action::ReadTicket(n));
                out.push(Action::ProbeSession(n));
                out.push(Action::ReadReply(n));
            }
            for r in self.acted_ranges() {
                out.push(Action::Claim(n, r));
                out.push(Action::ReloadForOpen(n, r, Read::Ok));
                out.push(Action::ReloadForOpen(n, r, Read::Failed));
                out.push(Action::OpenDb(n, r, Read::Ok));
                out.push(Action::OpenDb(n, r, Read::Failed));
                out.push(Action::Ack(n, r));
                if self.split_reads {
                    out.push(Action::ReadStart(n, r));
                } else {
                    out.push(Action::ServeRead(n, r));
                }
                out.push(Action::WriterFenced(n, r));
                out.push(Action::Reconcile(n, r));
                out.push(Action::RetireOrphan(n, r));
            }
        }
        if self.delegation && self.delegation_faults {
            out.extend([Action::CoordinatorTakeover, Action::CoordinatorRelease]);
            if !self.hub_node {
                out.extend([Action::CoordinatorLose, Action::CoordinatorReopen]);
            }
        }
        if !self.split_op {
            return;
        }
        out.extend([
            Action::MarkQuiesced,
            Action::Quiesce,
            Action::CloseSources,
            Action::Clone,
            Action::ClaimChildren,
            Action::CommitCas,
            Action::Retire,
            Action::RollBack,
        ]);
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        self.step(s, a)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut out = vec![
            Property::always(SINGLE_WRITER, |m: &M1, s: &State| m.single_writer(s)),
            Property::always(NO_ACK_INTO_RETIRED_DB, |_, s: &State| !s.ghost.lost_ack),
            Property::always(SM13_QUIESCED_SOURCE_CLOSED, |m: &M1, s: &State| {
                m.quiesced_source_closed(s)
            }),
            Property::always(LS3_LEADER_IS_RECORDED_OWNER, |m: &M1, s: &State| {
                m.leader_is_recorded_owner(s)
            }),
            Property::always(LS8_RETIRED_NEVER_REACQUIRED, |m: &M1, s: &State| {
                m.retired_never_led(s)
            }),
            Property::always(SM17_COMMITTED_SOURCE_STAYS_CLOSED, |m: &M1, s: &State| {
                m.committed_source_stays_closed(s)
            }),
            Property::always(NO_STALE_READ, |_, s: &State| !s.ghost.stale_read),
            Property::sometimes(SOMETIMES_CHILD_ACKED, |_, s: &State| {
                s.table == Table::Committed && s.ghost.child_acked
            }),
            Property::sometimes(SOMETIMES_FOREIGN_ROLL_BACK, |_, s: &State| {
                s.ghost.foreign_roll_back
            }),
            Property::sometimes(SOMETIMES_SOURCE_TOMBSTONED, |m: &M1, s: &State| {
                s.table == Table::Committed
                    && m.sources()
                        .iter()
                        .all(|&r| s.owners[r as usize].is_some_and(|o| o.retired))
            }),
            Property::sometimes(SOMETIMES_STALE_CLOSE_HEALED, |_, s: &State| s.ghost.healed),
            Property::sometimes(SOMETIMES_LATE_CLOSE, |_, s: &State| s.ghost.late_close),
        ];
        if !self.split_op {
            // No split/merge: the operation's `sometimes` have no example.
            out.retain(|p| !p.name.starts_with("sometimes"));
        }
        if self.solo {
            out.push(Property::sometimes(
                SOMETIMES_SOLO_EXPIRED,
                |_, s: &State| s.ghost.solo_expired,
            ));
            out.push(Property::sometimes(
                SOMETIMES_CHALLENGE_ANSWERED,
                |_, s: &State| s.ghost.challenge_answered,
            ));
        }
        if self.delegation {
            // State predicates instead of ghost variables (each ghost bit doubles
            // the state space).
            out.push(Property::sometimes(
                SOMETIMES_ACK_EXTENDS,
                |m: &M1, s: &State| {
                    // A grant newer than the last write of a `delegated` record:
                    // only an ack can do that.
                    (0..m.nodes).any(|n| {
                        let ss = &s.sessions[n as usize];
                        matches!(ss.mode, Mode::Delegated { .. })
                            && m.session_live(s, n)
                            && ss.quiet > ss.age
                    })
                },
            ));
            out.push(Property::sometimes(
                SOMETIMES_COORDINATOR_EXPIRES,
                |m: &M1, s: &State| {
                    (0..m.nodes).any(|n| {
                        s.sessions[n as usize].ended
                            && s.hub
                                .as_ref()
                                .is_some_and(|h| h.views[n as usize].issued != Issued::None)
                    })
                },
            ));
            out.push(Property::sometimes(
                SOMETIMES_YIELD_EXPIRES,
                |m: &M1, s: &State| {
                    // A `delegated` record ended while the coordinator has not
                    // issued an `expired` CAS and the node has not died: only the
                    // yield rule (or an old binary, with the gate off) can do that.
                    (0..m.nodes).any(|n| {
                        let ss = &s.sessions[n as usize];
                        ss.ended
                            && matches!(ss.mode, Mode::Delegated { .. })
                            && !s.nodes[n as usize].dead
                            && s.hub
                                .as_ref()
                                .is_some_and(|h| h.views[n as usize].issued == Issued::None)
                    })
                },
            ));
        }
        if self.hub_node && self.solo {
            // The coordinator (node 0) is in `solo` while its acks renew a
            // delegating node's grant — both mechanisms at once.
            out.push(Property::sometimes(
                SOMETIMES_SOLO_HUB_ACKS,
                |m: &M1, s: &State| {
                    let hub = &s.sessions[0];
                    !hub.ended
                        && matches!(hub.mode, Mode::Solo { .. })
                        && (1..m.nodes).any(|n| {
                            let ss = &s.sessions[n as usize];
                            matches!(ss.mode, Mode::Delegated { .. })
                                && m.session_live(s, n)
                                && ss.quiet > ss.age
                        })
                },
            ));
            // The coordinator's `solo` record ends (only a challenger can do that
            // with the gate on) while a node is still delegated to it.
            out.push(Property::sometimes(
                SOMETIMES_SOLO_HUB_EXPIRED,
                |m: &M1, s: &State| {
                    let hub = &s.sessions[0];
                    hub.ended
                        && s.ghost.solo_expired
                        && matches!(hub.mode, Mode::Solo { challenge: Some(_) })
                        && (1..m.nodes).any(|n| {
                            let ss = &s.sessions[n as usize];
                            !ss.ended && matches!(ss.mode, Mode::Delegated { .. })
                        })
                },
            ));
        }
        out
    }
}
