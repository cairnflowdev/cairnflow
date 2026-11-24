// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! # M3 — the per-node shared WAL
//!
//! *The model is a hypothesis about the code, like a guide: it drifts. Changing
//! `nodewal/` (sealing, taking over ranges, floor GC, scanning/finally removing
//! dead logs) or the WAL part of `LeaseKeeper` means fixing the model in the
//! same commit (AGENTS.md §2).*
//!
//! One range, two incarnations: `A` holds the range at first and writes its WAL
//! into `A`'s log; `B` (another node) takes the range over — after `A`'s
//! session dies, or after `A` yields — and also runs the dead-log scan. The data
//! are **wal ids** (one write = one frame = one id, issued from a shared
//! counter). The model tracks where each acked id lives: an L0 SST listed by the
//! **stored** manifest, or an object of a log the owner record still lists. The
//! state space is small (up to ~3.7 million states from `LandAmbiguous`, ~4.6
//! million from dirty yields and self-reclaims): every configuration is
//! exhausted in about ten seconds with `--release`, the whole matrix in about
//! ten minutes.
//!
//! ## State ↔ real objects
//!
//! | Model | Real |
//! |---|---|
//! | [`Log::objects`] | `nodewal/<node>/<inc>/<stripe>/<seq>` (data object, `format.rs`); [`Obj::acked`] = the object is in `Stripe::index` (acked) |
//! | [`Log::seals`] | seal object at `<seq>` (`format::encode_seal`) — there may be several, one below the floor does not count (C2) |
//! | [`Log::floor`] | durable GC floor `<log>/floor/<f>` (`LogStore::write_floor`) |
//! | [`Log::forgotten`] | `sweep.rs:forget_unlisted` → `LogStore::forget`: data, floor, index leave the log (into the trash, NW-12); the seal stays, the `forgotten` mark is written first |
//! | [`Db`] | the **stored** manifest of the range Db: `writer_epoch`, `replay_after_wal_id`, the L0 SSTs (by flushed id) |
//! | [`Writer::mem_mark`] | `db.manifest().replay_after_wal_id()` in RAM — SlateDB advances it in `apply_uploaded_state`, **before** writing the manifest |
//! | [`Writer::pending`] | L0 SSTs uploaded, manifest not written (the "changed in memory, not written out" gap) |
//! | [`Owner`] | `owners/range-<id>`: owner (`session`), `wal_segments` (in order: `A`'s log, then `B`'s), yield (`released`) |
//! | [`Taker`] | `B`'s part of `acquire_shared_with_hint` (mark hint, segments captured at claim, `max_seen`, first id) and of `sweep.rs:forget_unlisted` (owner-record snapshot) |
//! | [`State::a_session_dead`] | `A`'s session is past its TTL and someone observed it (`SessionTracker::observe`) |
//! | [`State::acked`] | ghost variable: ids acked to the outside (`WalFlushed` → `Committed`) |
//! | [`State::a_pending_release`] | the range's `PendingRelease` in `A`'s session `pending_release`: a clean yield whose CAS has not reached the bucket (`LeaseKeeper::release` → `hand_off_release`, background loop `retry_releases`) |
//! | [`State::live_fenced`] | ghost variable: an incarnation whose session is alive had its live log sealed (by another taker), or met its own live log when reclaiming the range (C1) — both are a whole-node fail-stop |
//!
//! ## Actions ↔ real functions
//!
//! | Action | `file:function` |
//! |---|---|
//! | [`Action::Write`] | `PartitionWriter::commit`: rows into the memtable, frame into the stream's buffer (`RangeWal::append`) |
//! | [`Action::Cut`] | `nodewal/stripe.rs:Stripe::cut` — one new object for every pending frame, at most [`IN_FLIGHT`] PUTs in flight. **When** to cut (window, linger L — 1 ms by default —, size cap, `flush()`) is not in the model: `Cut` is enabled in every state with pending frames (room in flight, segment added) and the model explores every interleaving that defers it (more `Write`s, a `Crash` before the cut) — every timing rule lies inside the explored space |
//! | [`Action::Land`] | `Stripe::put` → `LogStore::create` (`PutMode::Create`); `AlreadyExists` ⇒ read back (`Stripe::resolve`, flag `resolve_put_by_read`): our object ⇒ as success, a seal ⇒ `die` + fail-stop (NW-2) |
//! | [`Action::Rotate`] | `Stripe::stalled` → `Stripe::retire` (stop acking, close every range with `Fenced`) + `rotate_loop` (new stream, new id); the range steps down and **yields dirty** (`LeaseKeeper::release`, the segment stays). The next taker — `B`, or that same node on its new stream, the same shape on the bucket — seals the old log before replay (`take_over`: a rotated log is not C1). `rotate` also seals the old log itself in the background (`nodewal.rotate_seal`): [`Action::SealCreate`] is allowed as soon as `A` has rotated |
//! | [`Action::DirtyYield`] | `actor/leadership.rs:step_down`: seal failed or over the cap ⇒ `LeaseKeeper::release` leaves the segment (a **dirty** yield). Flag `rotate_before_dirty_yield` (form A): before writing the owner record, the keeper rotates the stream the range writes to (`LeaseKeeper::rotate_live_logs` → `NodeWal::rotate_for_dirty_yield` → `Stripe::retire` + `rotate_loop`) — the same shape as [`Action::Rotate`]. Off: the stream lives on (Db dropped, no ack reaches a client), uncut rows of the dropped Db still get cut — state [`Stage::Yielded`] |
//! | [`Action::ReleaseLost`] | form B: the seal is done (the range is clean), but the clean owner-record CAS (`LeaseKeeper::writer_sealed`) **and** the yield both missed — the record on the bucket is still `A`'s and still lists `A`'s segment; the clean yield sits in `pending_release` |
//! | [`Action::ReleaseRetry`] | `session/renew.rs:retry_releases`: the pending yield's CAS reaches the bucket (drops `A`'s segment) |
//! | [`Action::SelfClaim`] | `A` itself reclaims the range in the same incarnation (`Session::claim_with`, record `mine`), then `acquire_shared`: `take_over` meets a segment pointing at its own **live** log whose stream already has frames ⇒ C1, fail-stop. Flag `self_claim_applies_pending_release` (form B): the owner record still lists exactly the segments the keeper knows and a yield is pending ⇒ the claim CAS writes that yield's WAL part (`claim.rs:reclaim_wal`) — as if the yield had landed before the claim |
//! | [`Action::LandAmbiguous`] | an attempt of `Stripe::put` reaches the bucket without the writer knowing: an error/`put_timeout` after the write, or object_store retrying a 5xx by itself (`testkit::Action::{Ambiguous, LandLate}`); the next attempt meets `AlreadyExists` |
//! | [`Action::Ack`] | `Stripe::ack` (in order, `FuturesOrdered`) → `WalFlushed` → `PartitionWriter::commit` demands a still-valid grant (`writer/mod.rs:alive`) |
//! | [`Action::Flush1`] | SlateDB flushes the memtable: L0 SST uploaded, `apply_uploaded_state` advances the mark in RAM |
//! | [`Action::Flush2`] | SlateDB writes the manifest (CAS on `writer_epoch`; epoch mismatch ⇒ the writer is fenced) |
//! | [`Action::Gc`] | `nodewal/gc.rs:Inner::gc_log` from `gc_loop`, **only on the incarnation's own live log**: advance the floor past acked objects that every outside frame references (`Inner::refs` / `stored_refs`), write the floor, then delete (flag `gc_delete_after_floor`: only write the floor — deleting is [`Action::GcDelete`]). Dead logs and rotated logs have no floor GC |
//! | [`Action::GcDelete`] | (flag `gc_delete_after_floor`) `nodewal/gc.rs:delete_below_floor` from `gc_log` and from the start of each `gc_loop` round (the failed deletes of the previous round): delete objects **below the durable floor** of the incarnation's own live log — however many rounds late (a failed DELETE is retried in the next round); once dead, the leftovers go with the final removal |
//! | [`Action::AddSegment`] | `LeaseKeeper::add_wal_segment` (owner-record CAS; a miss ⇒ fail-stop, C4) |
//! | [`Action::Release`] | `LeaseKeeper::writer_sealed` (+ yield): CAS dropping the `wal_segment` (NW-6) |
//! | [`Action::SessionDeath`] | `A`'s TTL passed, someone observed it — `A` may still be running (a zombie) |
//! | [`Action::Crash`] | the process dies (`kill -9`); PUTs already sent may still reach the bucket ([`Action::Land`]) |
//! | [`Action::Claim`] | `Session::claim_with` (owner CAS) + `acquire_shared`: read the mark hint (`manifest_head`), capture `wal_segments` |
//! | [`Action::SealReadFloor`] | `NodeWal::seal`: `store.floor()` |
//! | [`Action::SealCreate`] | `NodeWal::seal`: find the first hole from the floor, `create(seal)`, read the floor again (C2); read the frames below the seal (`SealedLog`) |
//! | [`Action::Open`] | `acquire.rs:open_db`: SlateDB opens the Db (bumps `writer_epoch`, replays frames above the **current** manifest's `replay_after_wal_id`), `SharedWriterInit` picks the first id |
//! | [`Action::Prune`] | `acquire_shared_with_hint`: `DurableReplayMark::read` → `covered` → `LeaseKeeper::prune_wal_segments` (C7), then serve |
//! | [`Action::SweepSnapshot`] | `sweep.rs:forget_unlisted` (a full scan round `sweep_dead_logs` or a hinted round after taking over a range, `forget_hinted`): the **second** owner-record snapshot (`Inner::referenced_logs`, `ReferencedLogs`), after reading the death of every candidate (the incarnation's own rotated logs: `proof::StreamRetired`) and after sealing the log. The first snapshot only filters (logs still listed are not sealed), it is no proof — the model does not have it |
//! | [`Action::Forget`] | `sweep.rs:forget_unlisted`: seal (`NodeWal::sealed`), then `LogStore::forget` with `DeleteProof::dead_log` — without waiting for the floor to reach the seal (no floor advances on a dead log any more). A **rotated** log of a live incarnation (flag `forget_rotated_live`): the same path, proof `DeleteProof::rotated_log` (the stream that stopped acking stands in for death); that incarnation itself removes it — here `B` plays the remover, the same shape on the bucket |
//! | [`Action::AbandonAcquire`] | `acquire.rs:acquire_shared_with_hint` fails **after** `A`'s segment is in the owner record but **before** the first frame (flushing replayed rows over the cap, a failed durable-mark read, a failed CAS dropping old segments; or the segment CAS itself was ambiguous and the read-back failed), then `LeaseKeeper::release` (the keeper has not done `attach_shared` yet, so `rotate_live_logs` does not rotate). The segment reached the record by a visible CAS, an ambiguous one, or was inherited from the previous takeover (`add_wal_segment` returns early). Flag `drop_unserved_own_segment`: the yield drops that segment (`unserved_segment` also on the early-return branch; `write_release` does not merge in unknown segments of its own session) |
//! | [`Action::TailRead`] | (only [`M3::with_follower`]) `nodewal/tailer.rs:LogTailer::follow` on `A`'s log: GET `next`; data ⇒ exposed (flag `expose_contiguous`: at once — an `atomic_put` store; off: only acked objects — the C5 gate `acked_upto`), then `next + 1`; a seal ⇒ stop; a miss below the floor ⇒ jump to the floor; no floor and the previous object gone too ⇒ the log was finally removed, stop |
//!
//! ## Fences ([`Fences`] flags)
//!
//! - `ack_after_put` — NW-1: ack only after `Create` finishes.
//! - `ack_in_order` — NW-1: object n+1 durable first still waits for n.
//! - `segment_before_first_frame` — C4 (NW-4): CAS the segment into the owner
//!   record before the log's first frame.
//! - `gc_durable_mark` — NW-7: GC trusts the RAM mark of an open Db only up to
//!   the mark of the stored manifest. Off = the historical bug caught by
//!   `shared_wal_sim` seed 9.
//! - `prune_by_durable_mark` — C7 (N3): drop old segments by the stored mark
//!   (`DurableReplayMark`), not by the RAM mark.
//! - `seal_at_first_gap` — C3′ (NW-3): seal at the first hole counted from the
//!   floor, not above the highest object seen.
//! - `reread_floor_after_seal` — C2 (NW-3): a seal that lands on a name GC has
//!   just deleted (the floor has passed it) does not count; seal again.
//! - `clean_release_only` — NW-6: a yield deletes the `wal_segment` only when the
//!   Db is sealed clean (every frame is down in L0).
//! - `death_before_snapshot` — NW-8: the scan round's owner-record snapshot is
//!   taken **after** the session's death is confirmed.
//! - `snapshot_after_seal` — the snapshot used as proof for the final removal
//!   is taken **after** the seal is on the bucket (`sweep.rs:forget_unlisted`:
//!   the second snapshot, after `NodeWal::sealed`). Earlier the removal
//!   demanded that the GC floor had reached the seal — i.e. every frame below
//!   the seal was in L0 —, and that condition silently covered this spot.
//!   Without it (and with broken clocks): the zombie CASes its segment **after**
//!   the snapshot, cuts, acks; the seal is placed above that object; the removal
//!   moves an acked object away while the owner record still lists the log.
//! - `resolve_checks_owner` — NW-2: the read-back after `AlreadyExists` acks
//!   only when the object at `seq` really is ours (`Stripe::resolve` compares
//!   the bytes too); a seal ⇒ `Fenced`. Off = treat every `AlreadyExists` as
//!   "ours" — the zombie acks into the sealer's seal slot.
//! - `retired_stripe_stops_acking` — a rotated stream stops acking
//!   (`Stripe::retire` before asking to rotate; the `run` loop exits).
//! - `seal_rotated_log` — the log of a rotated stream is sealed before anyone
//!   replays it, even when the taker is that same incarnation
//!   (`NodeWal::take_over` treats it as another node's log; `rotate` seals it
//!   itself in the background, no floor GC afterwards). Off = reading the old
//!   log unsealed — the shape of C1. Each of these two fences **is enough on
//!   its own** (matrix: switching one off does not break); switching both off
//!   breaks (`tests/m3.rs::rotation_needs_one_of_its_two_fences`).
//! - `retire_before_snapshot` — the snapshot used as proof for the final removal
//!   of a **rotated** log of a live incarnation is taken **after** the stream
//!   stopped acking (`proof::StreamRetired` is read before every snapshot, like
//!   death with `death_before_snapshot`). Without it: the snapshot is taken
//!   before `A` adds its segment, `A` adds the segment, writes, acks, rotates;
//!   the removal by the old snapshot moves an acked object away while the owner
//!   record still lists the log — it breaks even under the clock assumption,
//!   but only when `snapshot_after_seal` is off too: `A`'s log is only sealed
//!   after the rotation, so a snapshot after the seal is a snapshot after the
//!   rotation (like `death_before_snapshot`).
//! - `forget_keeps_seal` — the final removal of a dead log keeps the seal and
//!   leaves a `forgotten` mark (`nodewal/log.rs:LogStore::forget`; a scan round
//!   does not re-seal a marked log, it only finishes the part not yet moved).
//!   Needed only when the clock assumption breaks: without it, a zombie whose
//!   stream has not touched the seal can write into the old seal's very slot
//!   after the seal left the log, and ack (`tests/m3.rs`).
//!
//! - `rotate_before_dirty_yield` — form A: before writing the owner record of a
//!   dirty yield, rotate the stream the range writes to — the segment the owner
//!   record keeps points at a log of ours that **has stopped acking**, and a
//!   taker sealing it is harmless. Off = the code before this change: a taker
//!   seals a live log ⇒ the node fail-stops ([`NO_LIVE_LOG_FENCED`]). Not a
//!   safety fence: the seal still fences the live stream, [`ACKED_RECOVERABLE`]
//!   holds even when off.
//! - `self_claim_applies_pending_release` — form B: a self-reclaim (same
//!   incarnation) where the owner record still lists exactly the segments the
//!   keeper knows applies the WAL part of the pending yield (clean: drop every
//!   segment). Off = the code before this change: `take_over` meets its own
//!   live log ⇒ C1, fail-stop ([`NO_LIVE_LOG_FENCED`]).
//! - `drop_unserved_own_segment` — the yield after a takeover that failed before
//!   the first frame ([`Action::AbandonAcquire`]) drops the segment pointing at
//!   `A`'s own log — that log holds no frame of the range. Off = the code before
//!   this change: an inherited segment (`add_wal_segment` returning early
//!   without marking "not served") or an ambiguous segment the keeper does not
//!   know (the merge of `write_release`) stays, the keeper has no `shared` yet
//!   so it does not rotate ⇒ a taker seals a live log
//!   ([`NO_LIVE_LOG_FENCED`]). Like `rotate_before_dirty_yield`: node liveness,
//!   [`ACKED_RECOVERABLE`] holds even when off.
//!
//! Behaviours (not fences):
//!
//! - `expose_contiguous` — the follower exposes every contiguous data object
//!   from the floor as soon as it can read it (an `atomic_put` store), without
//!   waiting for the `acked_upto` header. Off = the C5 gate (store
//!   `atomic_put = false`). Meaningful only with a follower
//!   ([`M3::with_follower`]). Exposure does **not** go through grant validity,
//!   so under it every fence on the bucket must stand alone as when clocks
//!   break: `seal_at_first_gap`, `snapshot_after_seal`, `forget_keeps_seal`
//!   become mandatory even with correct clocks, and `seal_rotated_log` can no
//!   longer be replaced by `retired_stripe_stops_acking`
//!   (`tests/m3.rs::follower_matrix`). Torn objects together with contiguous
//!   exposure bring C5 back in a separate model (`--torn --expose-contiguous`)
//!   — why `atomic_put = false` keeps the gate.
//! - `forget_rotated_live` — a rotated log of a **live** incarnation is finally
//!   removed when no owner record lists it any more (before: only when the
//!   incarnation died — each rotation leaked the whole old log for the node's
//!   lifetime, as a 4 h soak showed). Off = the behaviour before this change.
//! - `gc_delete_after_floor` — GC writes the floor, then deletes the objects
//!   below the floor in a separate step, possibly many rounds late (one failed
//!   DELETE does not make the round drop a durable floor; the next round retries
//!   only the failed part). While an object below the floor is still there, a
//!   tailer can read it, and a sealer with a stale floor read probes a higher
//!   hole. Off = floor and deletion atomic (an abstraction of the earlier code).
//! - `resolve_put_by_read` — a log PUT's `AlreadyExists` is resolved by
//!   reading back (an `atomic_put` store) instead of failing stop at once. Off =
//!   the behaviour of an `atomic_put = false` store (and of the earlier code):
//!   safe, only less live. Objects here are atomic; **torn objects** together
//!   with read-back give a counterexample in the separate model
//!   `verify/m1-shared-wal-model` (`--torn --resolve-by-read`) — why
//!   `atomic_put = false` exists.
//!
//! Imported assumptions:
//!
//! - `ack_needs_live_grant` — M1 (LS-2, LS-5): `commit` demands a still-valid
//!   grant by the node's own clock, and the grant expires before anyone else
//!   sees the session dead (`validity < ttl`). This is the **clock**
//!   assumption. Off = a zombie can still ack after its session is considered
//!   dead — only the seal (a fence on the bucket, needing no clock) stops it.
//!   Most of the sealing fences only have work to do when this assumption breaks.
//!
//! Which invariant breaks when each flag is switched off alone: the matrix is
//! pinned in `tests/m3.rs::matrix`.
//!
//! ## Invariants
//!
//! - [`EXPOSED_RECOVERABLE`] (only with a follower): every id the follower has
//!   exposed to a client can be read back like an acked id — including ids `A`
//!   never acked (NW-10).
//! - [`ACKED_RECOVERABLE`]: every acked id is in an L0 SST the stored manifest
//!   lists, or in the **canonical** part of a log the owner record still lists
//!   — the objects from the floor to the first seal not below the floor (an
//!   unsealed log: to the first hole). That is exactly what a later taker of
//!   the range can read (NW-1, NW-3, NW-4, NW-6, NW-7, NW-8).
//! - [`NO_LIVE_LOG_FENCED`] (node liveness, not safety): no taker seals a live
//!   log of a live incarnation, and no incarnation meets its own live log when
//!   reclaiming the range. Holds when `rotate_before_dirty_yield`,
//!   `self_claim_applies_pending_release` and `drop_unserved_own_segment` are
//!   all on.
//! - `sometimes` (the model is not empty): `B` takes the range over after `A`
//!   has acked twice and drops `A`'s segment; `A`'s dead log is finally removed;
//!   `A` yields clean, then `B` serves; an ambiguous log PUT is read back as
//!   "ours", then acked; `A`'s stream rotates, then `B` serves; `A`'s rotated
//!   log is finally removed while `A` is alive; `A` reclaims the range after a
//!   clean yield missed its CAS and serves on (form B).
//!
//! ## Deliberately left out of the model
//!
//! - One range, one stream per incarnation, no third taker (the invariant is
//!   stated statically on the bucket, so it is not needed: it states exactly
//!   what a later taker can read). `A` writes at most [`A_WRITES`] times, `B`
//!   [`B_WRITES`].
//! - Leases, grant validity: M1 (only the `ack_needs_live_grant` assumption is
//!   left here). Tombstones, retired ranges, split/merge (D22, G-c): M2. The log
//!   index (NW-11, it only saves GETs), `age_flush_loop` (liveness). There is
//!   only one follower, on `A`'s log (nobody takes `B`'s log over after `B`),
//!   and only with [`M3::with_follower`]; a reader reloading the manifest when
//!   it fell behind (`WalTruncated`) is not modelled — ids below the floor are
//!   already in L0. The reader's delivery rule (only frames contiguous by wal
//!   id; a gap ⇒ `WalTruncated`) is also outside: [`Tail::exposed`] is the set
//!   of exposed ids, not the state the reader applies — its prefix property is
//!   held by the unit tests of `nodewal/tailer.rs`.
//! - The new stream claiming its id on rotation (`rotate_until_claimed`): a
//!   failure or delay only postpones when `A` has a new stream, it changes
//!   nothing on the bucket — a slot without a stream opens no range
//!   (`NodeWal::slot_ready`), so the shape on the bucket is still
//!   [`Action::Rotate`] followed by another taker.
//! - The **other** ranges on a stream rotated because of a dirty yield: the old
//!   stream closes all its ranges with `Fenced`, they step down, yield dirty on
//!   the rotated log and get claimed again — the same shape as
//!   [`Action::Rotate`] followed by a taker. A self-reclaim after a rotation is
//!   exactly the `B` role here (the taker seals the rotated log, then replays),
//!   so [`Action::SelfClaim`] only starts from [`Stage::Released`] (form B) and
//!   [`Stage::Yielded`] (a dirty yield without rotation). `A` reclaims itself at
//!   most once.
//! - Reusing an incarnation (seed 12, NW-2): `inc` is never reused, so no action
//!   goes back to an old log.
//! - Ambiguous CAS (rule 7 of AGENTS.md): every CAS is atomic with a clear
//!   result. Ambiguous log PUTs exist ([`Action::LandAmbiguous`]); past
//!   `stall_max` without a clear result ⇒ fail-stop, equivalent to
//!   [`Action::Crash`]. Torn objects: a separate model (above).
//! - The sealer's hole probe and its frame read are atomic with creating the
//!   seal; a GC round is atomic (plan, write the floor, delete). A GC round
//!   based on stale references is safe because the stored mark only grows.
//! - The final removal is atomic. In reality: the `forgotten` mark (carrying the
//!   seal position) is written **before** anything moves; dying half-way, a
//!   later round finishes the rest without re-sealing
//!   (`tests/shared_wal.rs::forget_cut_short_is_finished_by_a_later_round`).
//! - Liveness (the floor does not stand still forever, dead logs get removed):
//!   the BFS only checks safety.

use stateright::{Model, Property};

/// A set of wal ids, bit `i` = id `i` (ids start at 1).
pub type Ids = u16;

/// Number of writes by `A`.
pub const A_WRITES: u8 = 2;
/// Number of writes by `B` after taking over the range.
pub const B_WRITES: u8 = 1;
/// Maximum PUTs in flight per stream (W of `NodeWalConfig`).
pub const IN_FLIGHT: usize = 2;
/// Number of `seq` slots of one log: `A` cuts at most [`A_WRITES`] objects, the
/// seal sits right above.
pub const SEQS: usize = A_WRITES as usize + 1;

pub const ACKED_RECOVERABLE: &str =
    "every acked wal id is in stored L0 or in the canonical part of a listed log (NW-1..NW-8)";
pub const SOMETIMES_HANDOFF: &str =
    "sometimes: B serves after A acked twice, and A's segment is pruned";
pub const SOMETIMES_DEAD_LOG_FORGOTTEN: &str = "sometimes: A's dead log is forgotten";
pub const SOMETIMES_CLEAN_RELEASE: &str = "sometimes: A releases cleanly and B serves";
pub const SOMETIMES_RESOLVED_OURS: &str =
    "sometimes: an ambiguous log PUT is read back as ours and acked";
pub const SOMETIMES_ROTATED: &str = "sometimes: A's stripe rotates and B serves A's acked writes";
pub const SOMETIMES_ROTATED_LOG_FORGOTTEN: &str =
    "sometimes: A's rotated log is forgotten while A's session lives";
pub const NO_LIVE_LOG_FENCED: &str =
    "no taker seals the live log of a live incarnation, and no incarnation meets its own live log at take-over (N11, C1)";
pub const SOMETIMES_PENDING_RELEASE_APPLIED: &str =
    "sometimes: A reclaims after its clean release missed the CAS, and serves again";
pub const EXPOSED_RECOVERABLE: &str =
    "every wal id a follower exposed is in stored L0 or in the canonical part of a listed log (NW-10)";
pub const SOMETIMES_EXPOSED_UNACKED: &str =
    "sometimes: the follower exposes a write A has not acked, and B serves";
pub const SOMETIMES_ABANDONED_ACQUIRE: &str =
    "sometimes: A abandons an acquisition before its first frame, and B serves";
pub const SOMETIMES_TAIL_STOPPED: &str =
    "sometimes: the follower's tailer stops (seal or forgotten log)";

fn max_id(ids: Ids) -> u8 {
    if ids == 0 {
        0
    } else {
        15 - ids.leading_zeros() as u8
    }
}

/// Every id in `ids` is ≤ `mark`.
fn all_le(ids: Ids, mark: u8) -> bool {
    max_id(ids) <= mark
}

/// The part of `ids` above `mark`.
fn above(ids: Ids, mark: u8) -> Ids {
    ids & !(((1u32 << (mark + 1)) - 1) as Ids)
}

/// Fences, each switched on/off (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fences {
    pub ack_after_put: bool,
    pub ack_in_order: bool,
    pub segment_before_first_frame: bool,
    pub gc_durable_mark: bool,
    pub prune_by_durable_mark: bool,
    pub seal_at_first_gap: bool,
    pub reread_floor_after_seal: bool,
    pub clean_release_only: bool,
    pub death_before_snapshot: bool,
    pub snapshot_after_seal: bool,
    /// Imported assumption (clocks, M1).
    pub ack_needs_live_grant: bool,
    pub forget_keeps_seal: bool,
    /// Behaviour: resolve `AlreadyExists` by reading back.
    pub resolve_put_by_read: bool,
    pub resolve_checks_owner: bool,
    pub retired_stripe_stops_acking: bool,
    pub seal_rotated_log: bool,
    pub retire_before_snapshot: bool,
    /// Behaviour: finally remove a rotated log of a live incarnation.
    pub forget_rotated_live: bool,
    /// Behaviour: deleting objects below the floor is a separate step, possibly
    /// late ([`Action::GcDelete`]).
    pub gc_delete_after_floor: bool,
    /// Behaviour: the tailer exposes every contiguous data object from the floor
    /// (an `atomic_put` store). Off = the C5 gate (only acked objects are
    /// exposed). Meaningful only when the model has a follower ([`M3::with_follower`]).
    pub expose_contiguous: bool,
    /// Behaviour (form A): a dirty yield rotates the stream before writing the
    /// owner record ([`Action::DirtyYield`]).
    pub rotate_before_dirty_yield: bool,
    /// Behaviour (form B): a self-reclaim applies the pending yield
    /// ([`Action::SelfClaim`]).
    pub self_claim_applies_pending_release: bool,
    /// Behaviour: the yield after a takeover that failed before the first frame
    /// drops `A`'s own segment ([`Action::AbandonAcquire`]).
    pub drop_unserved_own_segment: bool,
}

impl Fences {
    /// Every fence of the current code, with M1's clock assumption.
    pub const ALL: Fences = Fences {
        ack_after_put: true,
        ack_in_order: true,
        segment_before_first_frame: true,
        gc_durable_mark: true,
        prune_by_durable_mark: true,
        seal_at_first_gap: true,
        reread_floor_after_seal: true,
        clean_release_only: true,
        death_before_snapshot: true,
        snapshot_after_seal: true,
        ack_needs_live_grant: true,
        forget_keeps_seal: true,
        resolve_put_by_read: true,
        resolve_checks_owner: true,
        retired_stripe_stops_acking: true,
        seal_rotated_log: true,
        retire_before_snapshot: true,
        forget_rotated_live: true,
        gc_delete_after_floor: true,
        expose_contiguous: true,
        rotate_before_dirty_yield: true,
        self_claim_applies_pending_release: true,
        drop_unserved_own_segment: true,
    };

    /// Like [`Self::ALL`] with broken clocks: a zombie can ack after its
    /// session is considered dead.
    pub const NO_CLOCK: Fences = Fences {
        ack_needs_live_grant: false,
        ..Self::ALL
    };
}

/// Model configuration.
#[derive(Clone, Debug)]
pub struct M3 {
    pub fences: Fences,
    /// A follower tails `A`'s log ([`Action::TailRead`], NW-10).
    pub follower: bool,
}

impl M3 {
    pub fn new(fences: Fences) -> Self {
        M3 {
            fences,
            follower: false,
        }
    }

    /// Like [`Self::new`], plus a follower (another node's tailer) tailing
    /// `A`'s log: every id it exposes to a client must be readable back like an
    /// acked id ([`EXPOSED_RECOVERABLE`]).
    pub fn with_follower(fences: Fences) -> Self {
        M3 {
            fences,
            follower: true,
        }
    }
}

/// Incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum W {
    A,
    B,
}

impl W {
    fn ix(self) -> usize {
        match self {
            W::A => 0,
            W::B => 1,
        }
    }

    fn bit(self) -> u8 {
        1 << self.ix()
    }
}

/// One data object on the bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Obj {
    pub ids: Ids,
    /// The writer has acked it (it is in `Stripe::index`): the writer's own GC
    /// only goes over objects of this kind.
    pub acked: bool,
}

/// The log of one incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Log {
    pub objects: [Option<Obj>; SEQS],
    /// Bit `s` = there is a seal at `seq` `s`.
    pub seals: u8,
    pub floor: u8,
    pub forgotten: bool,
}

impl Log {
    fn sealed_at(&self, seq: usize) -> bool {
        self.seals & (1 << seq) != 0
    }

    /// End of the canonical part: the first seal not below the floor,
    /// otherwise the first hole from the floor.
    pub fn canonical_end(&self) -> usize {
        (self.floor as usize..SEQS)
            .find(|&s| self.sealed_at(s) || self.objects[s].is_none())
            .unwrap_or(SEQS)
    }
}

/// One PUT in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Put {
    pub seq: u8,
    pub ids: Ids,
    pub landed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    /// `B` has not claimed the range yet.
    Idle,
    /// `B` has done the owner CAS, is sealing/reading the old log.
    Claimed,
    /// `B` has opened the Db, not serving yet.
    Opened,
    /// Serving: accepts writes.
    Serving,
    /// `A` has yielded.
    Released,
    /// `A`'s stream has rotated: Db closed, the range yielded dirty; PUTs in
    /// flight may still reach the bucket.
    Rotated,
    /// `A` yielded dirty **without rotating** (the earlier code): Db dropped, the
    /// stream lives on — PUTs in flight reach the bucket, uncut rows of the
    /// dropped Db still get cut, no ack reaches a client.
    Yielded,
    /// The process died or failed stop.
    Dead,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Writer {
    pub stage: Stage,
    pub writes: u8,
    pub memtable: Ids,
    /// Frames written, not yet cut into an object.
    pub buffer: Ids,
    pub inflight: Vec<Put>,
    pub next_seq: u8,
    pub mem_mark: u8,
    /// SST uploaded + RAM mark advanced, manifest not written.
    pub pending: Option<(Ids, u8)>,
    pub epoch: u8,
    pub seg_added: bool,
}

impl Writer {
    fn new(stage: Stage) -> Self {
        Writer {
            stage,
            writes: 0,
            memtable: 0,
            buffer: 0,
            inflight: Vec::new(),
            next_seq: 0,
            mem_mark: 0,
            pending: None,
            epoch: 0,
            seg_added: false,
        }
    }

    fn alive(&self) -> bool {
        self.stage != Stage::Dead
    }
}

/// `A`'s log that `B` has sealed (`SealedLog`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Sealed {
    pub seal: u8,
    /// Frames readable below the seal.
    pub read: Ids,
}

/// The follower's tailer on `A`'s log (`nodewal/tailer.rs:LogTailer`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Tail {
    /// Next `seq` to GET (`TailState::next`).
    pub next: u8,
    /// Ids exposed to clients (a ghost variable of the follower's reader).
    pub exposed: Ids,
    /// The `follow` loop has stopped: it met a seal, or the log was finally removed.
    pub stopped: bool,
}

/// Owner-record snapshot of the dead-log scan round.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Snapshot {
    pub a_listed: bool,
    /// Taken after `A`'s session was seen dead (`SessionDeath` before the snapshot).
    pub after_death: bool,
    /// Taken after `A`'s stream stopped acking (`StreamRetired` before the snapshot).
    pub after_retire: bool,
}

/// `B`'s takeover and dead-log scan part.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Taker {
    /// Replay-mark hint read when claiming the range.
    pub hint: u8,
    /// `A`'s segment was in `wal_segments` at claim time.
    pub a_listed: bool,
    pub floor_read: Option<u8>,
    pub sealed: Option<Sealed>,
    pub max_seen: u8,
    pub first: u8,
    pub snapshot: Option<Snapshot>,
}

/// The stored manifest of the range Db.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Db {
    pub epoch: u8,
    pub stored_mark: u8,
    pub l0: Ids,
}

/// `owners/range-<id>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Owner {
    /// `None` = yielded.
    pub holder: Option<W>,
    /// Bit `W::bit()` = a segment points at that incarnation's log.
    pub segs: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub a: Writer,
    pub b: Writer,
    pub t: Taker,
    pub logs: [Log; 2],
    pub db: Db,
    pub owner: Owner,
    pub a_session_dead: bool,
    pub acked: Ids,
    pub next_id: u8,
    /// Ghost variable: id of an ambiguous log PUT that was read back as "ours".
    pub resolved: Ids,
    /// Follower on `A`'s log (runs only with [`M3::follower`]).
    pub tail: Tail,
    /// `A` has a clean yield that has not reached the bucket (`pending_release`).
    pub a_pending_release: bool,
    /// `A` has reclaimed the range itself once ([`Action::SelfClaim`]).
    pub self_claimed: bool,
    /// Ghost variable: [`NO_LIVE_LOG_FENCED`] broke.
    pub live_fenced: bool,
    /// Ghost variable: a self-reclaim applied the pending yield (form B).
    pub applied_pending: bool,
    /// Ghost variable: `A` abandoned a takeover ([`Action::AbandonAcquire`]).
    pub abandoned: bool,
}

impl State {
    fn w(&self, w: W) -> &Writer {
        match w {
            W::A => &self.a,
            W::B => &self.b,
        }
    }

    fn w_mut(&mut self, w: W) -> &mut Writer {
        match w {
            W::A => &mut self.a,
            W::B => &mut self.b,
        }
    }

    /// Writer `w` has an open Db (its RAM mark means something).
    fn db_open(&self, w: W) -> bool {
        match w {
            W::A => self.a.stage == Stage::Serving,
            W::B => matches!(self.b.stage, Stage::Opened | Stage::Serving),
        }
    }

    /// Id `id` can be read back: in the stored L0, or in the canonical part of
    /// a log the owner record lists.
    pub fn recoverable(&self, id: u8) -> bool {
        let bit = 1u16 << id;
        if self.db.l0 & bit != 0 {
            return true;
        }
        [W::A, W::B].into_iter().any(|w| {
            let log = &self.logs[w.ix()];
            self.owner.segs & w.bit() != 0
                && (log.floor as usize..log.canonical_end())
                    .any(|s| log.objects[s].is_some_and(|o| o.ids & bit != 0))
        })
    }

    /// `A`'s log is live: process `A` runs, its session is not considered dead,
    /// the stream has not rotated — sealing it kills the whole node.
    fn a_log_live(&self) -> bool {
        self.a.alive() && !self.a_session_dead && self.a.stage != Stage::Rotated
    }

    pub fn acks_recoverable(&self) -> bool {
        (1..16).all(|id| self.acked & (1 << id) == 0 || self.recoverable(id))
    }

    pub fn exposed_recoverable(&self) -> bool {
        (1..16).all(|id| self.tail.exposed & (1 << id) == 0 || self.recoverable(id))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Write(W),
    Cut(W),
    /// PUT number `i` in the in-flight queue reaches the bucket (an attempt with an answer).
    Land(W, usize),
    /// An attempt of PUT number `i` reaches the bucket without the writer
    /// knowing; the PUT is still in flight (the next attempt is [`Action::Land`]).
    LandAmbiguous(W, usize),
    /// Ack PUT number `i`.
    Ack(W, usize),
    Flush1(W),
    Flush2(W),
    /// One GC round of an incarnation on **its own** live log. There is no
    /// floor GC on another's log (dead logs, rotated logs): they stay whole
    /// until finally removed.
    Gc(W),
    /// (flag `gc_delete_after_floor`) The incarnation deletes objects below the
    /// durable floor of its own live log — separate from [`Action::Gc`], arbitrarily late.
    GcDelete(W),
    AddSegment(W),
    Release,
    SessionDeath,
    Crash(W),
    Claim,
    SealReadFloor,
    SealCreate,
    Open,
    Prune,
    SweepSnapshot,
    Forget,
    /// `A`'s stream rotates.
    Rotate,
    /// `A` yields dirty (seal failed or over the cap) — form A.
    DirtyYield,
    /// `A` yields clean but the CAS has not reached the bucket — form B.
    ReleaseLost,
    /// `A`'s pending yield reaches the bucket (background loop).
    ReleaseRetry,
    /// `A` reclaims the range itself in the same incarnation.
    SelfClaim,
    /// `A`'s takeover fails after its segment is in the owner record, before
    /// the first frame; the keeper yields.
    AbandonAcquire,
    /// One GET of the follower's tailer on `A`'s log (`LogTailer::follow`).
    TailRead,
}

impl M3 {
    /// The range's reference mark as seen by `w` (`Inner::refs`): a Db open on
    /// `w` ⇒ the RAM mark (bounded by the stored mark with the NW-7 fence);
    /// otherwise the stored manifest's mark (`stored_refs`).
    fn refs_mark(&self, s: &State, w: W) -> u8 {
        if s.db_open(w) {
            let mem = s.w(w).mem_mark;
            if self.fences.gc_durable_mark {
                mem.min(s.db.stored_mark)
            } else {
                mem
            }
        } else {
            s.db.stored_mark
        }
    }

    /// Whether it may ack (the clock assumption).
    fn may_ack(&self, s: &State, w: W) -> bool {
        let wr = s.w(w);
        wr.alive()
            && (matches!(wr.stage, Stage::Serving | Stage::Opened)
                || (wr.stage == Stage::Rotated && !self.fences.retired_stripe_stops_acking))
            && !(self.fences.ack_needs_live_grant && w == W::A && s.a_session_dead)
    }

    /// `B` may seal `A`'s log: `A`'s session is dead (dead-log scan, or a
    /// takeover after `A` died), or `B` has claimed the range while the owner
    /// record still lists `A`'s log (a dirty yield).
    fn may_seal(&self, s: &State) -> bool {
        s.b.alive()
            && s.t.sealed.is_none()
            && !s.logs[0].forgotten
            && (s.a_session_dead
                || (s.b.stage == Stage::Claimed && s.t.a_listed)
                || s.a.stage == Stage::Rotated)
    }

    /// Final removal of `A`'s log by a snapshot: `A`'s session is dead (proof
    /// `dead_log`), or — flag `forget_rotated_live` — `A`'s stream has rotated
    /// while `A` is alive (`rotated_log`). Each ground demands a snapshot taken
    /// after it, unless the matching fence is off.
    fn may_forget(&self, s: &State, sn: Snapshot) -> bool {
        let f = &self.fences;
        let dead = s.a_session_dead && (sn.after_death || !f.death_before_snapshot);
        let rotated = f.forget_rotated_live
            && s.a.stage == Stage::Rotated
            && (sn.after_retire || !f.retire_before_snapshot);
        dead || rotated
    }

    fn step(&self, s: &State, a: Action) -> Option<State> {
        let f = &self.fences;
        let mut n = s.clone();
        match a {
            Action::Write(w) => {
                let max = if w == W::A { A_WRITES } else { B_WRITES };
                let wr = n.w(w);
                if wr.stage != Stage::Serving || wr.writes >= max {
                    return None;
                }
                let id = n.next_id;
                n.next_id += 1;
                let wr = n.w_mut(w);
                wr.writes += 1;
                wr.memtable |= 1 << id;
                wr.buffer |= 1 << id;
            }
            Action::Cut(w) => {
                let wr = n.w_mut(w);
                // `Yielded`: the live stream still cuts uncut rows of the dropped Db.
                if !matches!(wr.stage, Stage::Serving | Stage::Opened | Stage::Yielded)
                    || wr.buffer == 0
                    || wr.inflight.len() >= IN_FLIGHT
                    || (f.segment_before_first_frame && !wr.seg_added)
                {
                    return None;
                }
                let put = Put {
                    seq: wr.next_seq,
                    ids: wr.buffer,
                    landed: false,
                };
                wr.next_seq += 1;
                wr.buffer = 0;
                wr.inflight.push(put);
            }
            Action::Land(w, i) => {
                let put = *n.w(w).inflight.get(i)?;
                if put.landed {
                    return None;
                }
                let seq = put.seq as usize;
                let log = &mut n.logs[w.ix()];
                if log.sealed_at(seq) || log.objects[seq].is_some() {
                    // `AlreadyExists`. Read back: the data object at `seq` is
                    // ours (only this stream writes data here, same bytes)
                    // ⇒ as success. A seal, or no read-back ⇒ sealed: the
                    // stream `die`s, fail-stop.
                    let ours =
                        log.objects[seq].is_some_and(|o| o.ids == put.ids) && !log.sealed_at(seq);
                    if f.resolve_put_by_read && (ours || !f.resolve_checks_owner) {
                        n.w_mut(w).inflight[i].landed = true;
                        n.resolved |= put.ids;
                    } else {
                        let wr = n.w_mut(w);
                        wr.inflight.remove(i);
                        wr.stage = Stage::Dead;
                    }
                } else {
                    log.objects[seq] = Some(Obj {
                        ids: put.ids,
                        acked: false,
                    });
                    n.w_mut(w).inflight[i].landed = true;
                }
            }
            Action::LandAmbiguous(w, i) => {
                let put = *n.w(w).inflight.get(i)?;
                let seq = put.seq as usize;
                let log = &mut n.logs[w.ix()];
                if put.landed || log.sealed_at(seq) || log.objects[seq].is_some() {
                    return None;
                }
                log.objects[seq] = Some(Obj {
                    ids: put.ids,
                    acked: false,
                });
            }
            Action::Ack(w, i) => {
                if !self.may_ack(s, w) {
                    return None;
                }
                if f.ack_in_order && i != 0 {
                    return None;
                }
                let put = *n.w(w).inflight.get(i)?;
                if f.ack_after_put && !put.landed {
                    return None;
                }
                n.acked |= put.ids;
                if put.landed {
                    if let Some(o) = &mut n.logs[w.ix()].objects[put.seq as usize] {
                        o.acked = true;
                    }
                }
                n.w_mut(w).inflight.remove(i);
            }
            Action::Flush1(w) => {
                if !s.db_open(w) {
                    return None;
                }
                let wr = n.w_mut(w);
                if wr.memtable == 0 || wr.pending.is_some() {
                    return None;
                }
                let mark = wr.mem_mark.max(max_id(wr.memtable));
                wr.pending = Some((wr.memtable, mark));
                wr.mem_mark = mark;
                wr.memtable = 0;
            }
            Action::Flush2(w) => {
                let (ids, mark) = s.w(w).pending?;
                if !s.w(w).alive() {
                    return None;
                }
                if s.w(w).epoch == s.db.epoch {
                    n.db.l0 |= ids;
                    n.db.stored_mark = n.db.stored_mark.max(mark);
                    n.w_mut(w).pending = None;
                } else {
                    // The manifest CAS missed: the writer is fenced.
                    let wr = n.w_mut(w);
                    wr.pending = None;
                    wr.stage = Stage::Dead;
                }
            }
            Action::Gc(w) => {
                if !s.w(w).alive() {
                    return None;
                }
                if w == W::B && s.b.stage == Stage::Idle {
                    return None;
                }
                // A rotated stream leaves its own log's GC loop (`gc_loop` only
                // walks the current streams); the old log has no floor GC any more.
                if w == W::A && s.a.stage == Stage::Rotated {
                    return None;
                }
                let mark = self.refs_mark(s, w);
                let log = &mut n.logs[w.ix()];
                let mut at = log.floor as usize;
                // Our own log: only acked objects (the index).
                while at < SEQS && log.objects[at].is_some_and(|o| o.acked && all_le(o.ids, mark)) {
                    at += 1;
                }
                if at as u8 <= log.floor {
                    return None;
                }
                if !f.gc_delete_after_floor {
                    for seq in log.floor as usize..at {
                        log.objects[seq] = None;
                    }
                }
                log.floor = at as u8;
            }
            Action::GcDelete(w) => {
                if !f.gc_delete_after_floor || !s.w(w).alive() {
                    return None;
                }
                // Same condition as the GC round: a rotated log leaves the loop,
                // its leftovers below the floor go with the final removal.
                if w == W::A && s.a.stage == Stage::Rotated {
                    return None;
                }
                let log = &mut n.logs[w.ix()];
                let below = log.floor as usize;
                if log.objects[..below].iter().all(Option::is_none) {
                    return None;
                }
                for seq in 0..below {
                    log.objects[seq] = None;
                }
            }
            Action::AddSegment(w) => {
                let ok_stage = match w {
                    W::A => s.a.stage == Stage::Serving,
                    W::B => s.b.stage == Stage::Opened,
                };
                if !ok_stage || s.w(w).seg_added {
                    return None;
                }
                if s.owner.holder == Some(w) {
                    n.owner.segs |= w.bit();
                    n.w_mut(w).seg_added = true;
                } else {
                    // The CAS missed ⇒ the range was lost behind our back ⇒ fail-stop (C1).
                    n.w_mut(w).stage = Stage::Dead;
                }
            }
            Action::Release => {
                let a = &s.a;
                if a.stage != Stage::Serving || s.owner.holder != Some(W::A) || s.a_session_dead {
                    return None;
                }
                if f.clean_release_only
                    && (a.memtable != 0
                        || a.buffer != 0
                        || !a.inflight.is_empty()
                        || a.pending.is_some())
                {
                    return None;
                }
                n.owner.holder = None;
                n.owner.segs &= !W::A.bit();
                n.a.stage = Stage::Released;
            }
            Action::SessionDeath => {
                if s.a_session_dead || (s.a.stage == Stage::Released && !s.a_pending_release) {
                    return None;
                }
                n.a_session_dead = true;
            }
            Action::Crash(w) => {
                if !s.w(w).alive() {
                    return None;
                }
                n.w_mut(w).stage = Stage::Dead;
            }
            Action::Claim => {
                if s.b.stage != Stage::Idle || !(s.a_session_dead || s.owner.holder.is_none()) {
                    return None;
                }
                n.owner.holder = Some(W::B);
                n.t.a_listed = s.owner.segs & W::A.bit() != 0;
                n.t.hint = s.db.stored_mark;
                n.b.stage = Stage::Claimed;
            }
            Action::SealReadFloor => {
                if !self.may_seal(s) || s.t.floor_read.is_some() {
                    return None;
                }
                n.t.floor_read = Some(s.logs[0].floor);
            }
            Action::SealCreate => {
                if !self.may_seal(s) {
                    return None;
                }
                let floor = s.t.floor_read? as usize;
                let log = &s.logs[0];
                let at = if f.seal_at_first_gap {
                    (floor..SEQS)
                        .find(|&q| log.objects[q].is_none())
                        .unwrap_or(SEQS)
                } else {
                    (floor..SEQS)
                        .rev()
                        .find(|&q| log.objects[q].is_some())
                        .map_or(floor, |q| q + 1)
                };
                n.t.floor_read = None;
                if at >= SEQS {
                    return None;
                }
                if log.sealed_at(at) || log.objects[at].is_some() {
                    // `AlreadyExists`: read again from the start.
                    return Some(n);
                }
                n.logs[0].seals |= 1 << at;
                if s.a_log_live() {
                    // The live stream's next PUT meets the seal ⇒ the whole node fail-stops.
                    n.live_fenced = true;
                }
                if f.reread_floor_after_seal && s.logs[0].floor as usize > at {
                    // A seal on a name GC just deleted: does not count, seal again.
                    return Some(n);
                }
                let read = (floor..at)
                    .filter_map(|q| log.objects[q].map(|o| o.ids))
                    .fold(0, |a, b| a | b);
                n.t.sealed = Some(Sealed {
                    seal: at as u8,
                    read,
                });
            }
            Action::Open => {
                // `seal_rotated_log` off: the rotated stream's log is read without
                // sealing — the canonical part at read time.
                let unsealed_read = s.a.stage == Stage::Rotated && !f.seal_rotated_log;
                if s.b.stage != Stage::Claimed
                    || (s.t.a_listed && s.t.sealed.is_none() && !unsealed_read)
                {
                    return None;
                }
                let read = if !s.t.a_listed {
                    0
                } else if let Some(sl) = s.t.sealed {
                    above(sl.read, s.t.hint)
                } else {
                    let log = &s.logs[0];
                    let ids = (log.floor as usize..log.canonical_end())
                        .filter_map(|q| log.objects[q].map(|o| o.ids))
                        .fold(0, |a, b| a | b);
                    above(ids, s.t.hint)
                };
                n.db.epoch += 1;
                n.b.epoch = n.db.epoch;
                n.b.memtable = above(read, s.db.stored_mark);
                n.b.mem_mark = s.db.stored_mark;
                n.t.max_seen = max_id(read);
                n.t.first = s.next_id;
                n.b.stage = Stage::Opened;
            }
            Action::Prune => {
                if s.b.stage != Stage::Opened || !s.b.seg_added {
                    return None;
                }
                let mark = if f.prune_by_durable_mark {
                    s.db.stored_mark
                } else {
                    s.b.mem_mark
                };
                let covered = if mark >= s.t.max_seen {
                    mark.max(s.t.first.saturating_sub(1))
                } else {
                    mark
                };
                let both = W::A.bit() | W::B.bit();
                if s.owner.segs & both == both && s.t.first.saturating_sub(1) <= covered {
                    if s.owner.holder != Some(W::B) {
                        n.b.stage = Stage::Dead;
                        return Some(n);
                    }
                    n.owner.segs &= !W::A.bit();
                }
                n.b.stage = Stage::Serving;
            }
            Action::SweepSnapshot => {
                let rotated = s.a.stage == Stage::Rotated;
                let for_dead = !f.death_before_snapshot || s.a_session_dead;
                let for_rotated = f.forget_rotated_live && (!f.retire_before_snapshot || rotated);
                if !s.b.alive()
                    || s.t.snapshot.is_some()
                    || !(for_dead || for_rotated)
                    || (f.snapshot_after_seal && s.t.sealed.is_none())
                {
                    return None;
                }
                n.t.snapshot = Some(Snapshot {
                    a_listed: s.owner.segs & W::A.bit() != 0,
                    after_death: s.a_session_dead,
                    after_retire: rotated,
                });
            }
            Action::Rotate => {
                if s.a.stage != Stage::Serving {
                    return None;
                }
                n.a.stage = Stage::Rotated;
                n.a.buffer = 0;
                n.a.memtable = 0;
                // Dirty yield: CAS on our version — if someone else has claimed,
                // it does not land.
                if s.owner.holder == Some(W::A) {
                    n.owner.holder = None;
                }
            }
            Action::DirtyYield => {
                if s.a.stage != Stage::Serving {
                    return None;
                }
                n.a.memtable = 0;
                // Dirty yield: CAS on our version, the segment stays.
                if s.owner.holder == Some(W::A) {
                    n.owner.holder = None;
                }
                if f.rotate_before_dirty_yield {
                    // As in [`Action::Rotate`] — the stream stops acking, pending rows dropped.
                    n.a.stage = Stage::Rotated;
                    n.a.buffer = 0;
                } else {
                    n.a.stage = Stage::Yielded;
                }
            }
            Action::ReleaseLost => {
                let a = &s.a;
                if a.stage != Stage::Serving
                    || s.owner.holder != Some(W::A)
                    || s.a_session_dead
                    || a.memtable != 0
                    || a.buffer != 0
                    || !a.inflight.is_empty()
                    || a.pending.is_some()
                {
                    return None;
                }
                // Seal done (clean), both CASes missed: the bucket is unchanged.
                n.a.stage = Stage::Released;
                n.a_pending_release = true;
            }
            Action::ReleaseRetry => {
                if !s.a_pending_release || !s.a.alive() || s.owner.holder != Some(W::A) {
                    return None;
                }
                n.owner.holder = None;
                n.owner.segs &= !W::A.bit();
                n.a_pending_release = false;
            }
            Action::SelfClaim => {
                if s.self_claimed || !matches!(s.a.stage, Stage::Released | Stage::Yielded) {
                    return None;
                }
                // Claim CAS: the record is still ours (`mine`) or was yielded.
                let ours = s.owner.holder == Some(W::A);
                if !(ours || s.owner.holder.is_none()) {
                    return None;
                }
                let mut segs = s.owner.segs;
                if ours && s.a_pending_release && f.self_claim_applies_pending_release {
                    // The record lists exactly the segments the keeper knows: apply the clean yield.
                    segs &= !W::A.bit();
                    n.applied_pending = true;
                }
                n.a_pending_release = false;
                n.self_claimed = true;
                n.owner.holder = Some(W::A);
                n.owner.segs = segs;
                if segs & W::A.bit() != 0 && s.a.next_seq > 0 {
                    // C1 (`take_over`): a segment points at our own live log
                    // whose stream already has frames ⇒ fail-stop.
                    n.a.stage = Stage::Dead;
                    n.live_fenced = true;
                    return Some(n);
                }
                // Open a new Db (nothing to replay: the range is clean, or the stream
                // has no frames yet — `skipped_own`); the old Db's pending rows are dropped (`register`).
                n.db.epoch += 1;
                n.a.epoch = n.db.epoch;
                n.a.memtable = 0;
                n.a.buffer = 0;
                n.a.pending = None;
                n.a.mem_mark = s.db.stored_mark;
                n.a.seg_added = false;
                n.a.stage = Stage::Serving;
                // `A`'s second life writes nothing more: it has the same shape as
                // the first (same log, a new segment before the first frame), it
                // would only multiply the state space. What needs checking — whether
                // dropping `A`'s segment at claim time loses any acked id — is
                // already checked right at this state.
                n.a.writes = A_WRITES;
            }
            Action::AbandonAcquire => {
                let a = &s.a;
                // The takeover is half-way: the Db is open (`Serving` here also
                // covers the stretch before the first serve), `A`'s log has no
                // frame yet — nothing cut, nothing waiting to be cut or flushed.
                if a.stage != Stage::Serving
                    || s.owner.holder != Some(W::A)
                    || s.a_session_dead
                    || a.next_seq != 0
                    || a.buffer != 0
                    || a.memtable != 0
                    || !a.inflight.is_empty()
                    || a.pending.is_some()
                {
                    return None;
                }
                n.owner.holder = None;
                if f.drop_unserved_own_segment {
                    n.owner.segs &= !W::A.bit();
                } else {
                    // The segment reached the record (an ambiguous CAS, or
                    // inherited) without the keeper marking it "not served": the
                    // dirty yield keeps it, no rotation (the keeper has no `shared` yet).
                    n.owner.segs |= W::A.bit();
                }
                n.a.stage = Stage::Released;
                n.abandoned = true;
            }
            Action::Forget => {
                // Seal first (the seal stays as a fence), then remove for good
                // once a snapshot taken after the death no longer lists the log —
                // without waiting for any floor (dead logs have no floor GC).
                s.t.sealed?;
                let sn = s.t.snapshot?;
                if !s.b.alive() || s.logs[0].forgotten || sn.a_listed || !self.may_forget(s, sn) {
                    return None;
                }
                let log = &mut n.logs[0];
                log.forgotten = true;
                log.objects = [None; SEQS];
                log.floor = 0;
                if !f.forget_keeps_seal {
                    log.seals = 0;
                }
            }
            Action::TailRead => {
                if !self.follower || s.tail.stopped {
                    return None;
                }
                let at = s.tail.next as usize;
                if at >= SEQS {
                    return None;
                }
                let log = &s.logs[0];
                if log.sealed_at(at) {
                    // A seal: the log has nothing new (whatever the follower has
                    // not seen reaches it through the L0 the new owner flushes before serving).
                    n.tail.stopped = true;
                } else if let Some(o) = log.objects[at] {
                    // A data object at `next`: every `seq` below it was read (or
                    // is below the floor). Exposed at once (atomic store), or
                    // only when an `acked_upto` header covers it (C5) — here:
                    // when the writer has acked it, the upper bound of any header that can cover it.
                    if !(f.expose_contiguous || o.acked) {
                        return None;
                    }
                    n.tail.exposed |= o.ids;
                    n.tail.next += 1;
                } else if log.floor > s.tail.next {
                    // A miss below the floor: GC has passed (`WalTruncated`) — jump to the floor.
                    n.tail.next = log.floor;
                } else if log.floor == 0 && at > 0 && log.objects[at - 1].is_none() {
                    // No floor and the last object read is gone too: the log was
                    // finally removed — treat it as sealed, stop.
                    n.tail.stopped = true;
                } else {
                    return None;
                }
            }
        }
        Some(n)
    }
}

impl Model for M3 {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            a: Writer::new(Stage::Serving),
            b: Writer::new(Stage::Idle),
            t: Taker::default(),
            logs: [Log::default(), Log::default()],
            db: Db::default(),
            owner: Owner {
                holder: Some(W::A),
                segs: 0,
            },
            a_session_dead: false,
            acked: 0,
            next_id: 1,
            resolved: 0,
            tail: Tail::default(),
            a_pending_release: false,
            self_claimed: false,
            live_fenced: false,
            applied_pending: false,
            abandoned: false,
        }]
    }

    fn actions(&self, s: &State, out: &mut Vec<Action>) {
        // A lost ack is the end of the road (as in M1): going on only makes the counterexample longer.
        if !s.acks_recoverable() || !s.exposed_recoverable() || s.live_fenced {
            return;
        }
        for w in [W::A, W::B] {
            out.extend([
                Action::Write(w),
                Action::Cut(w),
                Action::Flush1(w),
                Action::Flush2(w),
                Action::AddSegment(w),
                Action::Crash(w),
                Action::Gc(w),
                Action::GcDelete(w),
            ]);
            for i in 0..s.w(w).inflight.len() {
                out.push(Action::Land(w, i));
                out.push(Action::LandAmbiguous(w, i));
                out.push(Action::Ack(w, i));
            }
        }
        out.extend([
            Action::Release,
            Action::SessionDeath,
            Action::Claim,
            Action::SealReadFloor,
            Action::SealCreate,
            Action::Open,
            Action::Prune,
            Action::SweepSnapshot,
            Action::Forget,
            Action::Rotate,
            Action::DirtyYield,
            Action::ReleaseLost,
            Action::ReleaseRetry,
            Action::SelfClaim,
            Action::AbandonAcquire,
            Action::TailRead,
        ]);
        out.retain(|&a| self.step(s, a).is_some_and(|n| n != *s));
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        self.step(s, a)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always(ACKED_RECOVERABLE, |_, s: &State| s.acks_recoverable()),
            Property::always(NO_LIVE_LOG_FENCED, |_, s: &State| !s.live_fenced),
            Property::sometimes(SOMETIMES_HANDOFF, |_, s: &State| {
                s.b.stage == Stage::Serving
                    && s.owner.segs & W::A.bit() == 0
                    && s.acked.count_ones() >= 2
            }),
            Property::sometimes(SOMETIMES_DEAD_LOG_FORGOTTEN, |_, s: &State| {
                s.logs[0].forgotten && s.acked != 0
            }),
            Property::sometimes(SOMETIMES_CLEAN_RELEASE, |_, s: &State| {
                s.a.stage == Stage::Released && s.b.stage == Stage::Serving && s.acked != 0
            }),
            Property::sometimes(SOMETIMES_RESOLVED_OURS, |_, s: &State| {
                s.resolved & s.acked != 0
            }),
            Property::sometimes(SOMETIMES_ROTATED, |_, s: &State| {
                s.a.stage == Stage::Rotated && s.b.stage == Stage::Serving && s.acked != 0
            }),
        ];
        props.push(Property::sometimes(
            SOMETIMES_ROTATED_LOG_FORGOTTEN,
            |_, s: &State| {
                s.logs[0].forgotten
                    && s.a.stage == Stage::Rotated
                    && !s.a_session_dead
                    && s.acked != 0
            },
        ));
        if self.fences.drop_unserved_own_segment {
            props.push(Property::sometimes(
                SOMETIMES_ABANDONED_ACQUIRE,
                |_, s: &State| s.abandoned && s.b.stage == Stage::Serving,
            ));
        }
        if self.fences.self_claim_applies_pending_release {
            props.push(Property::sometimes(
                SOMETIMES_PENDING_RELEASE_APPLIED,
                |_, s: &State| s.applied_pending && s.a.stage == Stage::Serving && s.acked != 0,
            ));
        }
        if self.follower {
            props.push(Property::always(EXPOSED_RECOVERABLE, |_, s: &State| {
                s.exposed_recoverable()
            }));
            props.push(Property::sometimes(
                SOMETIMES_TAIL_STOPPED,
                |_, s: &State| s.tail.stopped && s.tail.exposed != 0,
            ));
            if self.fences.expose_contiguous {
                props.push(Property::sometimes(
                    SOMETIMES_EXPOSED_UNACKED,
                    |_, s: &State| s.tail.exposed & !s.acked != 0 && s.b.stage == Stage::Serving,
                ));
            }
        }
        props
    }
}
