// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Acceptance tests of model M3: all fences on ⇒ exhaustive search, invariants
//! hold; re-injecting NW-7 and the counterexamples C2, C3′, C4, C7 of the
//! shared-WAL design ⇒ the checker finds a counterexample; the "switch exactly
//! one fence off" matrix pins which fence holds up the invariant — under M1's
//! clock assumption ([`Fences::ALL`]) and when that assumption breaks
//! ([`Fences::NO_CLOCK`]; both with the code's `forget_keeps_seal`).
//!
//! The counterexamples printed (`--nocapture`) are the **shortest** (as in `tests/m2.rs`).

use {
    cairn_model::m3::{self, Action, Fences, M3, W},
    stateright::{Checker, HasDiscoveries, Model},
};

fn report(name: &str, c: &impl Checker<M3>) {
    println!(
        "{name}: {} unique states, {} generated, depth {}, done = {}",
        c.unique_state_count(),
        c.state_count(),
        c.max_depth(),
        c.is_done()
    );
}

/// BFS over the **whole** state space.
fn check_all(name: &str, fences: Fences) -> impl Checker<M3> {
    let c = M3::new(fences)
        .checker()
        .finish_when(HasDiscoveries::AllFailures)
        .spawn_bfs()
        .join();
    report(name, &c);
    assert!(c.is_done(), "{name}: the BFS must be exhaustive");
    c
}

/// Whether an `always` invariant has a counterexample (exhaustive): an acked id
/// is lost ([`m3::ACKED_RECOVERABLE`]), or — node liveness — a live incarnation
/// had its live log sealed or met its own live log
/// ([`m3::NO_LIVE_LOG_FENCED`]).
fn breaks(name: &str, fences: Fences) -> bool {
    let c = check_all(name, fences);
    c.discovery(m3::ACKED_RECOVERABLE).is_some() || c.discovery(m3::NO_LIVE_LOG_FENCED).is_some()
}

/// Shortest counterexample: depth-bounded BFS with growing depth (see `tests/m2.rs`).
fn first_counterexample(name: &str, fences: Fences) -> Vec<Action> {
    first_counterexample_of(name, fences, m3::ACKED_RECOVERABLE)
}

/// Like [`first_counterexample`], for invariant `property`.
fn first_counterexample_of(name: &str, fences: Fences, property: &'static str) -> Vec<Action> {
    let actions = (2..=64)
        .find_map(|depth| {
            let c = M3::new(fences)
                .checker()
                .target_max_depth(depth)
                .finish_when(HasDiscoveries::AnyOf([property].into()))
                .spawn_bfs()
                .join();
            let found = c.discovery(property)?;
            report(name, &c);
            Some(found.into_actions())
        })
        .unwrap_or_else(|| panic!("{name}: no counterexample found"));
    println!("{name}: counterexample ({} steps):", actions.len());
    for (i, a) in actions.iter().enumerate() {
        println!("  {:>2}. {a:?}", i + 1);
    }
    actions
}

fn position(actions: &[Action], want: Action) -> usize {
    actions
        .iter()
        .position(|&a| a == want)
        .unwrap_or_else(|| panic!("missing {want:?} in {actions:?}"))
}

/// No clock assumption; the final removal of a dead log keeps the seal (the code, `Fences::ALL`).
const NO_CLOCK_KEEP_SEAL: Fences = Fences::NO_CLOCK;

/// Like [`NO_CLOCK_KEEP_SEAL`], but the final removal moves the seal away too —
/// the code before the fix that keeps the seal.
const NO_CLOCK_FORGET_SEAL: Fences = Fences {
    forget_keeps_seal: false,
    ..Fences::NO_CLOCK
};

// ───────────────────────────────────────── (a) all fences

/// (a) Every fence of the code, with M1's clock assumption: exhaustive, no
/// counterexample, and every `sometimes` has an example (takeover after two
/// acks; a dead log finally removed; a clean yield).
#[test]
fn all_fences_hold() {
    check_all("all fences", Fences::ALL).assert_properties();
}

/// (a') Broken clocks (a zombie can ack after its session is considered dead),
/// with the code's `forget_keeps_seal`: the seal is a fence on the bucket, enough on its own.
#[test]
fn without_the_clock_the_seal_holds_if_forget_keeps_it() {
    check_all("no clock, keep seal", NO_CLOCK_KEEP_SEAL).assert_properties();
}

// ───────────────────────────────────────── (b) historical bugs

/// **NW-7** (`shared_wal_sim` seed 9): GC trusts the RAM mark. A memtable flush
/// uploads the SST and advances the RAM mark (`apply_uploaded_state`), the
/// manifest is not written yet; GC deletes objects the stored manifest does not
/// cover. The node dying before writing the manifest loses acked rows.
#[test]
fn nw7_gc_trusting_the_in_memory_mark_loses_an_ack() {
    let ce = first_counterexample(
        "NW-7",
        Fences {
            gc_durable_mark: false,
            ..Fences::ALL
        },
    );
    assert!(position(&ce, Action::Flush1(W::A)) < position(&ce, Action::Gc(W::A)));
    assert!(
        !ce.contains(&Action::Flush2(W::A)),
        "manifest not written: {ce:?}"
    );
}

/// **C7 / N3**: the taker drops the old log's segment by the RAM mark after
/// flushing the replayed rows, before the manifest is written.
#[test]
fn c7_pruning_by_the_in_memory_mark_loses_an_ack() {
    let ce = first_counterexample(
        "C7",
        Fences {
            prune_by_durable_mark: false,
            ..Fences::ALL
        },
    );
    assert!(position(&ce, Action::Flush1(W::B)) < position(&ce, Action::Prune));
    assert!(!ce.contains(&Action::Flush2(W::B)), "{ce:?}");
}

/// **C2**: a seal lands on a name GC has just deleted. The taker reads the floor,
/// GC (of the old incarnation itself, still running as a zombie) advances the
/// floor and deletes the first object, the taker probes for a hole from the old
/// floor and places the seal **below** the new floor — reads no object at all,
/// then drops the old segment. Breaks **even under the clock assumption**: the
/// ack here happens before the session dies; the zombie only GCs, it does not ack.
#[test]
fn c2_seal_on_a_name_gc_just_freed_loses_an_ack() {
    let ce = first_counterexample(
        "C2",
        Fences {
            reread_floor_after_seal: false,
            ..Fences::ALL
        },
    );
    let read = position(&ce, Action::SealReadFloor);
    let gc = position(&ce, Action::Gc(W::A));
    let create = position(&ce, Action::SealCreate);
    assert!(read < gc && gc < create, "{ce:?}");
    assert!(position(&ce, Action::SessionDeath) < gc);
}

/// **C3′**: the seal is placed above the highest object seen instead of at the
/// hole of a PUT in flight. That PUT lands after the seal, below the seal, and
/// is acked — the taker finished reading without seeing it, its mark passed
/// that frame. Needs a zombie ack: under the clock assumption this fence is
/// redundant (matrix).
#[test]
fn c3_seal_above_an_inflight_put_loses_a_zombie_ack() {
    let ce = first_counterexample(
        "C3′",
        Fences {
            seal_at_first_gap: false,
            ..NO_CLOCK_KEEP_SEAL
        },
    );
    let create = position(&ce, Action::SealCreate);
    let land = ce
        .iter()
        .rposition(|&a| a == Action::Land(W::A, 0))
        .expect("the PUT lands");
    assert!(create < land, "{ce:?}");
    assert!(ce[land..].contains(&Action::Ack(W::A, 0)), "{ce:?}");
}

/// **C4**: the log's first frame before the owner record has a segment pointing at the log.
#[test]
fn c4_frame_before_segment_loses_an_ack() {
    let ce = first_counterexample(
        "C4",
        Fences {
            segment_before_first_frame: false,
            ..Fences::ALL
        },
    );
    assert!(!ce.contains(&Action::AddSegment(W::A)), "{ce:?}");
}

/// **NW-6**: a yield deletes the `wal_segment` while the Db is not sealed clean.
#[test]
fn nw6_unclean_release_loses_an_ack() {
    let ce = first_counterexample(
        "NW-6",
        Fences {
            clean_release_only: false,
            ..Fences::ALL
        },
    );
    assert_eq!(ce.last(), Some(&Action::Release));
}

/// **NW-1**: ack before the PUT, and out-of-order acks within a stream.
#[test]
fn nw1_ack_before_put_or_out_of_order_loses_an_ack() {
    first_counterexample(
        "NW-1 ack before PUT",
        Fences {
            ack_after_put: false,
            ..Fences::ALL
        },
    );
    let ce = first_counterexample(
        "NW-1 ack out of order",
        Fences {
            ack_in_order: false,
            ..Fences::ALL
        },
    );
    assert!(ce.contains(&Action::Ack(W::A, 1)), "{ce:?}");
}

// ───────────────────────────────────────── (c) findings

/// **The final removal of a dead log relied on the clock assumption** (the code
/// before the fix that keeps the seal, `NO_CLOCK_FORGET_SEAL`). `forget_log`
/// moved the seal into the trash too. A zombie whose stream has not touched the
/// seal (no PUT since it was sealed) can PUT into the old seal's very slot, then
/// ack — while its other objects left the log with the removal. The owner-record
/// snapshot "after the death" cannot cover a segment the zombie CASes **after**
/// the death either. Under the clock assumption (the grant expires before the
/// TTL) the zombie cannot ack, so the current code is right; keeping the seal
/// makes this part need no clock (test (a')).
#[test]
fn without_the_clock_forgetting_a_dead_log_lets_its_zombie_ack() {
    let ce = first_counterexample("no clock, drop seal", NO_CLOCK_FORGET_SEAL);
    let forget = position(&ce, Action::Forget);
    assert!(
        ce[forget..]
            .iter()
            .any(|a| matches!(a, Action::Ack(W::A, _))),
        "{ce:?}"
    );
    // The zombie's PUT lands **after** the removal, into the old seal's slot.
    // (The order of `AddSegment(A)` relative to the death is not pinned: there are
    // two shortest traces of the same length, the exploration order picks one —
    // since `LandAmbiguous` was added the chosen one changed.)
    assert!(
        ce[forget..]
            .iter()
            .any(|a| matches!(a, Action::Land(W::A, _))),
        "{ce:?}"
    );
}

/// **The owner-record snapshot taken before the seal**. Without floor GC on
/// dead logs, the final removal no longer demands that the floor reached the
/// seal — the condition that used to cover this spot silently (every frame
/// below the seal was in L0). The snapshot is taken after the death but
/// **before** the seal: zombie `A` (broken clocks) CASes its segment **after**
/// the snapshot, cuts, acks; `B` places the seal above that object and removes
/// by the old snapshot — an acked object leaves the log while the owner record
/// still lists the log. The code takes the second snapshot after sealing
/// (`sweep.rs:forget_unlisted`, `proof::SealSeen`).
#[test]
fn without_the_clock_a_snapshot_before_the_seal_lets_forget_lose_a_zombie_ack() {
    let ce = first_counterexample(
        "snapshot before seal",
        Fences {
            snapshot_after_seal: false,
            ..NO_CLOCK_KEEP_SEAL
        },
    );
    let snap = position(&ce, Action::SweepSnapshot);
    let seg = position(&ce, Action::AddSegment(W::A));
    let seal = position(&ce, Action::SealCreate);
    assert!(snap < seg && seg < seal, "{ce:?}");
    assert_eq!(ce.last(), Some(&Action::Forget), "{ce:?}");
}

/// **Reading an ambiguous log PUT back without telling whose it is**: treat
/// every `AlreadyExists` as "our earlier write". Zombie `A` (broken clocks)
/// PUTs into `B`'s seal slot, "reads back" and acks — `B` has finished reading
/// the log without that id. `Stripe::resolve` compares the bytes too: a seal is
/// never ours (`resolve_checks_owner`). Under the clock assumption the zombie
/// cannot ack, so the fence is redundant there (matrix) — like `seal_at_first_gap`.
#[test]
fn reading_back_without_checking_the_owner_lets_a_zombie_ack_into_a_seal() {
    let ce = first_counterexample(
        "read-back without owner check",
        Fences {
            resolve_checks_owner: false,
            ..NO_CLOCK_KEEP_SEAL
        },
    );
    let seal = position(&ce, Action::SealCreate);
    let land = ce
        .iter()
        .rposition(|a| matches!(a, Action::Land(W::A, _)))
        .expect("A's PUT");
    assert!(seal < land, "{ce:?}");
    assert!(
        ce[land..].iter().any(|a| matches!(a, Action::Ack(W::A, _))),
        "{ce:?}"
    );
}

/// No read-back (store `atomic_put = false`, or the earlier code): an ambiguous
/// log PUT is a fail-stop — safe, only less live. Exhaustive, no counterexample.
#[test]
fn without_reading_back_an_ambiguous_put_only_fails_stop() {
    let c = check_all(
        "no read-back",
        Fences {
            resolve_put_by_read: false,
            ..Fences::ALL
        },
    );
    assert!(c.discovery(m3::ACKED_RECOVERABLE).is_none());
    assert!(
        c.discovery(m3::SOMETIMES_RESOLVED_OURS).is_none(),
        "no read-back yet an ambiguous PUT was acked"
    );
}

/// **Stream rotation needs at least one of its two fences**. Both off: `A`'s
/// stream rotates yet still acks, and the taker reads the old log unsealed —
/// exactly the shape of C1: `A`'s PUT in flight lands **after** the read, `A`
/// acks it, the taker does not have it. Each fence alone is enough (matrix); the
/// code has both.
#[test]
fn rotation_needs_one_of_its_two_fences() {
    let ce = first_counterexample(
        "rotation without fences",
        Fences {
            retired_stripe_stops_acking: false,
            seal_rotated_log: false,
            ..Fences::ALL
        },
    );
    let rotate = position(&ce, Action::Rotate);
    let open = position(&ce, Action::Open);
    assert!(rotate < open, "{ce:?}");
    assert!(
        ce[open..].iter().any(|a| matches!(a, Action::Ack(W::A, _))),
        "{ce:?}"
    );
    assert!(!ce.contains(&Action::SealCreate), "{ce:?}");
}

/// **Removing a rotated log by a snapshot older than the rotation**. A rotated
/// log of a live incarnation is finally removed when no owner record lists it;
/// the ground is "the stream has stopped acking" (`proof::StreamRetired`), so
/// the snapshot must be taken **after** the rotation. Taken before: `A` has not
/// added its segment yet so the snapshot does not see the log; `A` adds the
/// segment, writes, acks, rotates; the removal by the old snapshot moves an
/// acked object away while the owner record still lists the log. Breaks **even
/// under the clock assumption**: the ack here comes before the rotation, not from a zombie.
///
/// `snapshot_after_seal` must be off too: `A`'s log is only sealed once `A` has
/// rotated (or died, or the range went to someone else after the rotation), so a
/// snapshot after the seal is a snapshot after the rotation — like
/// `death_before_snapshot` (matrix).
#[test]
fn forgetting_a_rotated_log_by_a_snapshot_older_than_the_rotation_loses_an_ack() {
    let ce = first_counterexample(
        "snapshot before rotation",
        Fences {
            retire_before_snapshot: false,
            snapshot_after_seal: false,
            ..Fences::ALL
        },
    );
    let snap = position(&ce, Action::SweepSnapshot);
    let seg = position(&ce, Action::AddSegment(W::A));
    let rotate = position(&ce, Action::Rotate);
    assert!(snap < seg && seg < rotate, "{ce:?}");
    assert!(!ce.contains(&Action::SessionDeath), "{ce:?}");
    assert_eq!(ce.last(), Some(&Action::Forget), "{ce:?}");
}

/// Without removing rotated logs of live incarnations (`forget_rotated_live`
/// off): safe, only leaking — a rotated log is never removed while `A` is alive.
#[test]
fn without_forgetting_rotated_logs_nothing_is_lost_but_nothing_is_forgotten_alive() {
    let c = check_all(
        "without forget_rotated_live",
        Fences {
            forget_rotated_live: false,
            ..Fences::ALL
        },
    );
    assert!(c.discovery(m3::ACKED_RECOVERABLE).is_none());
    assert!(
        c.discovery(m3::SOMETIMES_ROTATED_LOG_FORGOTTEN).is_none(),
        "without forget_rotated_live, yet a rotated log was removed while A was alive"
    );
}

/// **A live log fenced, form A**: a dirty yield without rotating the stream.
/// `A`'s seal fails or goes over the cap while stepping down, the owner record
/// keeps a segment pointing at `A`'s **live** log, a taker seals that log — the
/// stream's next PUT (carrying frames of every other range of the node) meets the
/// seal ⇒ the whole node fail-stops. Nothing is lost ([`m3::ACKED_RECOVERABLE`]
/// holds — the seal fences the live stream), it only takes a node down for one
/// range. Seen ≥ 34 times as `wal_fenced`/`wal_ambiguous` and 7 times as `c1` in
/// chaos/soak runs. The code rotates the stream before writing the owner record
/// (`rotate_before_dirty_yield`): the taker seals a log that has stopped acking.
#[test]
fn dirty_yield_without_rotation_seals_a_live_log() {
    let fences = Fences {
        rotate_before_dirty_yield: false,
        ..Fences::ALL
    };
    let c = check_all("dirty yield, no rotation", fences);
    assert!(
        c.discovery(m3::ACKED_RECOVERABLE).is_none(),
        "a dirty yield without rotation lost an ack: the seal must fence the live stream"
    );
    let ce = first_counterexample_of("dirty yield, no rotation", fences, m3::NO_LIVE_LOG_FENCED);
    let yielded = position(&ce, Action::DirtyYield);
    let claim = position(&ce, Action::Claim);
    assert!(yielded < claim, "{ce:?}");
    assert_eq!(ce.last(), Some(&Action::SealCreate), "{ce:?}");
    assert!(!ce.contains(&Action::SessionDeath), "{ce:?}");
}

/// **C1, form B** (18 of 27 `c1` occurrences in chaos/soak runs): the seal is
/// done — every frame of the range is down in L0 — but the clean owner-record
/// CAS and the yield both miss (a flaky store), the yield sits in
/// `pending_release`. `A` itself reclaims the range before the background loop:
/// the record is still `A`'s and still lists `A`'s live log ⇒ `take_over` meets
/// C1, fail-stop. Rotating the stream does not cure this case (there is no dirty
/// yield). The code: the claim applies the WAL part of the pending yield when
/// the record still lists exactly the segments the keeper knows
/// (`self_claim_applies_pending_release`) — as if the yield had landed before
/// the claim, an interleaving that exists anyway.
#[test]
fn self_claim_without_the_pending_release_meets_its_own_live_log() {
    let fences = Fences {
        self_claim_applies_pending_release: false,
        ..Fences::ALL
    };
    let c = check_all("self-reclaim without applying the yield", fences);
    assert!(c.discovery(m3::ACKED_RECOVERABLE).is_none());
    let ce = first_counterexample_of(
        "self-reclaim without applying the yield",
        fences,
        m3::NO_LIVE_LOG_FENCED,
    );
    let lost = position(&ce, Action::ReleaseLost);
    assert!(position(&ce, Action::Cut(W::A)) < lost, "{ce:?}");
    assert_eq!(ce.last(), Some(&Action::SelfClaim), "{ce:?}");
    assert!(!ce.contains(&Action::DirtyYield), "{ce:?}");
}

/// **A failed takeover keeping its own segment**: `A`'s takeover fails after
/// its segment is in the owner record, before the first frame — an ambiguous
/// segment CAS whose read-back failed, or a reclaim inheriting exactly that
/// segment (`add_wal_segment` returns early) that then flushes over the cap. The
/// keeper has not done `attach_shared` so `rotate_live_logs` does not rotate;
/// the segment stays pointing at `A`'s **live** log, a taker seals it ⇒ the
/// whole node fail-stops. The code drops that segment
/// (`drop_unserved_own_segment`): the log has no frame of the range under it.
#[test]
fn abandoned_acquire_keeping_its_own_segment_seals_a_live_log() {
    let fences = Fences {
        drop_unserved_own_segment: false,
        ..Fences::ALL
    };
    let c = check_all("failed takeover keeps its own segment", fences);
    assert!(
        c.discovery(m3::ACKED_RECOVERABLE).is_none(),
        "a takeover failing before the first frame lost an ack"
    );
    let ce = first_counterexample_of(
        "failed takeover keeps its own segment",
        fences,
        m3::NO_LIVE_LOG_FENCED,
    );
    let abandoned = position(&ce, Action::AbandonAcquire);
    assert!(abandoned < position(&ce, Action::Claim), "{ce:?}");
    assert_eq!(ce.last(), Some(&Action::SealCreate), "{ce:?}");
    assert!(!ce.contains(&Action::SessionDeath), "{ce:?}");
}

// ───────────────────────────────────────── (d) matrix

type Knock = fn(&mut Fences);

/// Switch exactly one fence off ⇒ does an invariant break, under the clock
/// assumption and when it breaks (the seal stays on final removal). `false` = the
/// fence is redundant at model level:
///
/// - `seal_at_first_gap` is redundant under the clock assumption: only a zombie
///   can ack into an object that lands after the seal.
/// - `death_before_snapshot` is redundant in both: the snapshot is taken after
///   the seal (`snapshot_after_seal`), and sealing is only allowed after the
///   death (or once the range has been claimed) — a snapshot after the seal is a
///   snapshot after the death. Before that it was redundant because the removal
///   demanded the GC floor to reach the seal. Not proposed for removal — it is a
///   net for the spots the model makes atomic (sealing and removal).
/// - `snapshot_after_seal` is redundant under the clock assumption: a zombie
///   cannot ack after the death, so every acked frame has a segment from before the death.
/// - `retire_before_snapshot` is redundant in both, for the same reason as
///   `death_before_snapshot`: the snapshot is taken after the seal, and `A`'s log
///   is only sealed after the rotation. Off together with `snapshot_after_seal`
///   it breaks even under the clock assumption
///   (`forgetting_a_rotated_log_by_a_snapshot_older_than_the_rotation_loses_an_ack`).
/// - `rotate_before_dirty_yield`, `self_claim_applies_pending_release`,
///   `drop_unserved_own_segment`: "breaks" here is [`m3::NO_LIVE_LOG_FENCED`] —
///   a live node fail-stops (C1) — not a lost ack; breaks in both
///   (`dirty_yield_without_rotation_seals_a_live_log`,
///   `self_claim_without_the_pending_release_meets_its_own_live_log`,
///   `abandoned_acquire_keeping_its_own_segment_seals_a_live_log`).
fn matrix() -> Vec<(&'static str, Knock, bool, bool)> {
    vec![
        ("ack_after_put", |f| f.ack_after_put = false, true, true),
        ("ack_in_order", |f| f.ack_in_order = false, true, true),
        (
            "segment_before_first_frame",
            |f| f.segment_before_first_frame = false,
            true,
            true,
        ),
        ("gc_durable_mark", |f| f.gc_durable_mark = false, true, true),
        (
            "prune_by_durable_mark",
            |f| f.prune_by_durable_mark = false,
            true,
            true,
        ),
        (
            "seal_at_first_gap",
            |f| f.seal_at_first_gap = false,
            false,
            true,
        ),
        (
            "reread_floor_after_seal",
            |f| f.reread_floor_after_seal = false,
            true,
            true,
        ),
        (
            "clean_release_only",
            |f| f.clean_release_only = false,
            true,
            true,
        ),
        (
            "death_before_snapshot",
            |f| f.death_before_snapshot = false,
            false,
            false,
        ),
        // Only needed when the clock assumption breaks.
        (
            "snapshot_after_seal",
            |f| f.snapshot_after_seal = false,
            false,
            true,
        ),
        // Only needed when the clock assumption breaks.
        (
            "forget_keeps_seal",
            |f| f.forget_keeps_seal = false,
            false,
            true,
        ),
        // Read-back off is the behaviour of an `atomic_put = false` store — safe
        // in both (only more fail-stops).
        (
            "resolve_put_by_read",
            |f| f.resolve_put_by_read = false,
            false,
            false,
        ),
        // Each rotation fence alone is enough — switching one off does not
        // break; switching both off breaks (`rotation_needs_one_of_its_two_fences`).
        (
            "retired_stripe_stops_acking",
            |f| f.retired_stripe_stops_acking = false,
            false,
            false,
        ),
        (
            "seal_rotated_log",
            |f| f.seal_rotated_log = false,
            false,
            false,
        ),
        // The read-back must tell a seal from our object — only a zombie can ack
        // into the seal slot, so it is only needed when the clock assumption breaks.
        (
            "resolve_checks_owner",
            |f| f.resolve_checks_owner = false,
            false,
            true,
        ),
        // Redundant alone (a snapshot after the seal is already after the
        // rotation); the behaviour off is the earlier behaviour — safe, only leaking.
        (
            "retire_before_snapshot",
            |f| f.retire_before_snapshot = false,
            false,
            false,
        ),
        (
            "forget_rotated_live",
            |f| f.forget_rotated_live = false,
            false,
            false,
        ),
        // Off = floor and deletion atomic (an abstraction of the earlier code).
        // Late deletion below a durable floor breaks nothing in both.
        (
            "gc_delete_after_floor",
            |f| f.gc_delete_after_floor = false,
            false,
            false,
        ),
        // Node liveness — breaks NO_LIVE_LOG_FENCED in both.
        (
            "rotate_before_dirty_yield",
            |f| f.rotate_before_dirty_yield = false,
            true,
            true,
        ),
        (
            "self_claim_applies_pending_release",
            |f| f.self_claim_applies_pending_release = false,
            true,
            true,
        ),
        // Node liveness — breaks NO_LIVE_LOG_FENCED in both.
        (
            "drop_unserved_own_segment",
            |f| f.drop_unserved_own_segment = false,
            true,
            true,
        ),
    ]
}

#[test]
fn matrix_one_fence_off() {
    let mut wrong = Vec::new();
    for (name, knock, with_clock, without_clock) in matrix() {
        for (base, fences, want) in [
            ("clock", Fences::ALL, with_clock),
            ("no clock, keep seal", NO_CLOCK_KEEP_SEAL, without_clock),
        ] {
            let mut f = fences;
            knock(&mut f);
            let got = breaks(&format!("{base} - {name}"), f);
            if got != want {
                wrong.push(format!("{base} - {name}: breaks = {got}, pinned = {want}"));
            }
        }
    }
    assert!(wrong.is_empty(), "matrix mismatch:\n{}", wrong.join("\n"));
}

// ───────────────────────────────────────── (e) follower (4.3)

/// Exhaustive search with a follower tailing `A`'s log.
fn check_follower(name: &str, fences: Fences) -> impl Checker<M3> {
    let c = M3::with_follower(fences)
        .checker()
        .finish_when(HasDiscoveries::AllFailures)
        .spawn_bfs()
        .join();
    report(name, &c);
    assert!(c.is_done(), "{name}: the BFS must be exhaustive");
    c
}

/// With a follower: an id that was acked **or exposed** is lost.
fn follower_breaks(name: &str, fences: Fences) -> bool {
    let c = check_follower(name, fences);
    c.discovery(m3::ACKED_RECOVERABLE).is_some()
        || c.discovery(m3::EXPOSED_RECOVERABLE).is_some()
        || c.discovery(m3::NO_LIVE_LOG_FENCED).is_some()
}

/// Shortest counterexample for [`m3::EXPOSED_RECOVERABLE`].
fn first_exposure_counterexample(name: &str, fences: Fences) -> Vec<Action> {
    let actions = (2..=64)
        .find_map(|depth| {
            let c = M3::with_follower(fences)
                .checker()
                .target_max_depth(depth)
                .finish_when(HasDiscoveries::AnyOf([m3::EXPOSED_RECOVERABLE].into()))
                .spawn_bfs()
                .join();
            let found = c.discovery(m3::EXPOSED_RECOVERABLE)?;
            report(name, &c);
            Some(found.into_actions())
        })
        .unwrap_or_else(|| panic!("{name}: no counterexample found"));
    println!("{name}: counterexample ({} steps):", actions.len());
    for (i, a) in actions.iter().enumerate() {
        println!("  {:>2}. {a:?}", i + 1);
    }
    actions
}

/// (e) The follower exposes every contiguous data object from the floor (an
/// `atomic_put` store), with every fence of the code and the clock assumption:
/// exhaustive, every exposed id — including ids `A` never acked — can be read
/// back; the tailer sometimes stops (a seal, a finally removed log) after exposing something.
#[test]
fn follower_exposing_contiguous_objects_holds() {
    let c = check_follower("follower exposes contiguously", Fences::ALL);
    c.assert_properties();
}

/// (e') As above, with broken clocks: exposure does not demand a valid grant,
/// so this is the configuration where contiguous exposure **really** relies on the bucket fences.
#[test]
fn follower_exposing_contiguous_objects_holds_without_the_clock() {
    check_follower("contiguous follower, no clock", NO_CLOCK_KEEP_SEAL).assert_properties();
}

/// (e'') The C5 gate (store `atomic_put = false`): only acked objects are exposed — clean.
#[test]
fn follower_behind_the_ack_gate_holds() {
    let c = check_follower(
        "follower behind the C5 gate",
        Fences {
            expose_contiguous: false,
            ..Fences::ALL
        },
    );
    assert!(c.discovery(m3::ACKED_RECOVERABLE).is_none());
    assert!(c.discovery(m3::EXPOSED_RECOVERABLE).is_none());
}

/// **Contiguous exposure needs the bucket fences even when clocks are right.**
/// Exposure does not go through grant validity (`ack_needs_live_grant` only blocks
/// acks), so an object a zombie or a rotated stream wrote late — never acked —
/// can be exposed like a zombie's ack. Four fences the non-follower matrix marks
/// "only needed when clocks break" (or redundant) become mandatory; the code has
/// all four:
///
/// - `seal_at_first_gap`: a seal placed above the highest object instead of the
///   first hole from the floor ⇒ an orphan object lands **below** the seal after
///   the taker's read;
/// - `snapshot_after_seal`: the proof snapshot taken before the seal ⇒ an
///   exposed object leaves the log on final removal;
/// - `forget_keeps_seal`: final removal moves the seal too ⇒ a late PUT lands in
///   the old seal's slot and is exposed;
/// - `seal_rotated_log`: the taker reads the rotated stream's log without sealing
///   ⇒ the old stream's PUT in flight lands after the read, is not acked (the
///   stream has stopped acking) but is exposed — the two rotation fences can no
///   longer stand in for each other.
#[test]
fn exposing_contiguous_objects_needs_the_bucket_fences_even_with_the_clock() {
    for (name, knock) in [
        (
            "seal_at_first_gap",
            (|f| f.seal_at_first_gap = false) as Knock,
        ),
        ("snapshot_after_seal", |f| f.snapshot_after_seal = false),
        ("forget_keeps_seal", |f| f.forget_keeps_seal = false),
        ("seal_rotated_log", |f| f.seal_rotated_log = false),
    ] {
        let mut f = Fences::ALL;
        knock(&mut f);
        let ce = first_exposure_counterexample(&format!("contiguous exposure - {name}"), f);
        assert!(ce.contains(&Action::TailRead), "{name}: {ce:?}");
    }
}

/// The "switch exactly one fence off" matrix **with a contiguously exposing
/// follower** — differs from the non-follower matrix ([`matrix`]) in five rows (see
/// [`exposing_contiguous_objects_needs_the_bucket_fences_even_with_the_clock`]):
/// under the clock assumption `seal_at_first_gap`, `snapshot_after_seal`,
/// `forget_keeps_seal`, `seal_rotated_log` break; `expose_contiguous` (off = the
/// C5 gate) breaks in neither.
fn follower_matrix() -> Vec<(&'static str, Knock, bool, bool)> {
    let mut m: Vec<_> = matrix()
        .into_iter()
        .map(|(name, knock, with_clock, without_clock)| {
            let with_clock = with_clock
                || matches!(
                    name,
                    "seal_at_first_gap"
                        | "snapshot_after_seal"
                        | "forget_keeps_seal"
                        | "seal_rotated_log"
                );
            let without_clock = without_clock || name == "seal_rotated_log";
            (name, knock, with_clock, without_clock)
        })
        .collect();
    m.push((
        "expose_contiguous",
        |f| f.expose_contiguous = false,
        false,
        false,
    ));
    m
}

fn run_follower_matrix(base: &str, fences: Fences, pick: fn(bool, bool) -> bool) {
    let mut wrong = Vec::new();
    for (name, knock, with_clock, without_clock) in follower_matrix() {
        let mut f = fences;
        knock(&mut f);
        let want = pick(with_clock, without_clock);
        let got = follower_breaks(&format!("follower {base} - {name}"), f);
        if got != want {
            wrong.push(format!("{base} - {name}: breaks = {got}, pinned = {want}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "follower matrix mismatch:\n{}",
        wrong.join("\n")
    );
}

/// The "clock" half of the follower matrix. ~5 minutes in release, hence `#[ignore]`:
/// `cargo test --release -p cairn-model --test m3 -- --ignored follower_matrix`.
#[test]
#[ignore = "follower matrix: ~5 minutes per half in release"]
fn follower_matrix_one_fence_off() {
    run_follower_matrix("clock", Fences::ALL, |w, _| w);
}

/// The "no clock" half of the follower matrix (see above).
#[test]
#[ignore = "follower matrix: ~5 minutes per half in release"]
fn follower_matrix_one_fence_off_without_the_clock() {
    run_follower_matrix("no clock", NO_CLOCK_KEEP_SEAL, |_, wo| wo);
}
