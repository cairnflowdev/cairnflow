// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The four child cleanup paths, the `abandoned_child_db` gate, proxy tombstones.

use super::{check, row};
use crate::{
    Child, Event, Gate, Keep, Need, Orphan, Owner, Phase, Plan, ReadErr, Refs, Remover, View,
};

/// A typical orphan child: permanently out of the table, has a Db, has an owner
/// record, stands on a live source.
const ORPHAN: Child = Child {
    in_table: false,
    promised: false,
    allocated: true,
    db: true,
    owner: true,
    stands_on_live: true,
};

const CLEAR: Gate = Gate {
    table_version: 7,
    refs_version: 7,
    refs: Refs::Clear,
    refs_in_flight: true,
    barrier: false,
    moved: false,
};

fn v(child: Child, ledger: Option<Phase>, gate: Option<Gate>) -> View {
    View {
        child: Some(child),
        ledger: Some(Ok(ledger)),
        gate: gate.map(Ok),
        ..View::default()
    }
}

const ROUND: Event = Event::Discard(Remover::OrphanRound);

#[test]
fn permanence_comes_first() {
    check(vec![
        row(
            "child in the table (commit won) ⇒ keep",
            v(
                Child {
                    in_table: true,
                    ..ORPHAN
                },
                None,
                None,
            ),
            Event::Discard(Remover::RollBack),
            Plan::Keep(Keep::InTable),
        ),
        row(
            "SM-11 (2): child promised (op in flight) ⇒ keep",
            v(
                Child {
                    promised: true,
                    ..ORPHAN
                },
                None,
                Some(CLEAR),
            ),
            ROUND,
            Plan::Keep(Keep::Promised),
        )
        .with(|b| b.sm11_ignores_promised = true, Plan::DiscardDb),
        row(
            "id not allocated yet ⇒ keep",
            v(
                Child {
                    allocated: false,
                    ..ORPHAN
                },
                None,
                None,
            ),
            ROUND,
            Plan::Keep(Keep::Unallocated),
        ),
    ]);
}

#[test]
fn proofs_of_never_in_table() {
    let retired_sources = Child {
        stands_on_live: false,
        ..ORPHAN
    };
    check(vec![
        row(
            "the roller-back just won this op's CAS ⇒ clean up (the gate checks)",
            v(retired_sources, None, None),
            Event::Discard(Remover::RollBack),
            Plan::DiscardDb,
        ),
        row(
            "ctl after its CAS ⇒ clean up",
            v(retired_sources, None, None),
            Event::Discard(Remover::Ctl),
            Plan::DiscardDb,
        ),
        row(
            "the cleanup round reads the ledger first",
            View {
                child: Some(ORPHAN),
                ..View::default()
            },
            ROUND,
            Plan::Need(Need::Ledger),
        )
        .with(|b| b.ledger_not_proof = true, Plan::Need(Need::Gate)),
        row(
            "ledger Cleaned ⇒ positive proof (an orphan child with an owner, the sources retired)",
            v(retired_sources, Some(Phase::Cleaned), Some(CLEAR)),
            ROUND,
            Plan::DiscardDb,
        )
        .with(|b| b.ledger_not_proof = true, Plan::Keep(Keep::NotProven)),
        row(
            "ledger Prepared ⇒ the child may have entered the table — not the business of a child cleanup path",
            v(ORPHAN, Some(Phase::Prepared), Some(CLEAR)),
            ROUND,
            Plan::Keep(Keep::MayHaveCommitted),
        )
        .with(|b| b.ledger_not_proof = true, Plan::DiscardDb),
        row(
            "ledger Committed ⇒ never through a child cleanup path",
            v(
                Child {
                    db: false,
                    ..ORPHAN
                },
                Some(Phase::Committed),
                None,
            ),
            Event::Discard(Remover::OwnOrphans),
            Plan::Keep(Keep::MayHaveCommitted),
        )
        .with(|b| b.ledger_not_proof = true, Plan::DeleteOwner),
        row(
            "SM-14 proof 1: stands on a live range",
            v(ORPHAN, None, Some(CLEAR)),
            ROUND,
            Plan::DiscardDb,
        ),
        row(
            "R1-f: proof 2 — has a Db, no owner record, the sources retired",
            v(
                Child {
                    owner: false,
                    ..retired_sources
                },
                Some(Phase::Aborting),
                Some(CLEAR),
            ),
            ROUND,
            Plan::DiscardDb,
        )
        .with(
            |b| b.r1f_first_proof_only = true,
            Plan::Keep(Keep::NotProven),
        ),
        row(
            "no proof ⇒ left to SM-11/D-d",
            v(retired_sources, None, Some(CLEAR)),
            ROUND,
            Plan::Keep(Keep::NotProven),
        ),
        row(
            "reading the ledger failed ⇒ infer as before the ledger",
            View {
                child: Some(ORPHAN),
                ledger: Some(Err(ReadErr)),
                gate: Some(Ok(CLEAR)),
                ..View::default()
            },
            ROUND,
            Plan::DiscardDb,
        ),
        row(
            "no Db: the cleanup round leaves it to the tombstone branch",
            v(
                Child {
                    db: false,
                    ..ORPHAN
                },
                None,
                None,
            ),
            ROUND,
            Plan::Keep(Keep::NotProven),
        ),
        row(
            "no Db: the coordinator deletes the owner record (D5)",
            v(
                Child {
                    db: false,
                    ..ORPHAN
                },
                None,
                None,
            ),
            Event::Discard(Remover::OwnOrphans),
            Plan::DeleteOwner,
        ),
        row(
            "no Db, no owner record: nothing",
            v(
                Child {
                    db: false,
                    owner: false,
                    ..ORPHAN
                },
                None,
                None,
            ),
            Event::Discard(Remover::RollBack),
            Plan::Ignore,
        ),
    ]);
}

#[test]
fn direct_checks_of_the_orphan_round() {
    // A retired parent without an owner record, on a bucket from before the
    // migration (R1-i): proof 2 says "never entered the table" — wrong.
    let parent = Child {
        owner: false,
        stands_on_live: false,
        ..ORPHAN
    };
    check(vec![
        row(
            "R1-i: a live range stands on it ⇒ keep",
            v(
                parent,
                None,
                Some(Gate {
                    refs: Refs::Referenced,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::Keep(Keep::Referenced),
        )
        .with(|b| b.r1i_trusts_history = true, Plan::DiscardDb),
        row(
            "unknown means keep",
            v(
                parent,
                None,
                Some(Gate {
                    refs: Refs::Unknown,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::Keep(Keep::Referenced),
        )
        .with(|b| b.unknown_refs_as_clear = true, Plan::DiscardDb),
        row(
            "GC barrier ⇒ keep",
            v(
                parent,
                None,
                Some(Gate {
                    barrier: true,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::Keep(Keep::Barrier),
        ),
        row(
            "the table changed before deleting ⇒ keep",
            v(
                parent,
                None,
                Some(Gate {
                    moved: true,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::Keep(Keep::TableMoved),
        ),
        row(
            "N4: a reference set of another version ⇒ keep",
            v(
                parent,
                None,
                Some(Gate {
                    refs_version: 6,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::Keep(Keep::StaleRefs),
        )
        .with(|b| b.n4_any_refs_version = true, Plan::DiscardDb),
        row(
            "the cleanup round does not need children in flight in the set (the gate adds them)",
            v(
                parent,
                None,
                Some(Gate {
                    refs_in_flight: false,
                    ..CLEAR
                }),
            ),
            ROUND,
            Plan::DiscardDb,
        ),
        row(
            "reading the set failed ⇒ keep",
            View {
                child: Some(parent),
                ledger: Some(Ok(None)),
                gate: Some(Err(ReadErr)),
                ..View::default()
            },
            ROUND,
            Plan::Keep(Keep::Unreadable),
        ),
        row(
            "D4: Db cleanup done ⇒ delete the owner record",
            View::default(),
            Event::Discarded { ok: true },
            Plan::DeleteOwner,
        ),
        row(
            "D4: Db cleanup failed ⇒ keep the owner record (the Db is still proof)",
            View::default(),
            Event::Discarded { ok: false },
            Plan::Keep(Keep::DiscardFailed),
        )
        .with(|b| b.d4_owner_before_db = true, Plan::DeleteOwner),
    ]);
}

#[test]
fn proof_gate() {
    let gate = |c: Child, g: Option<Gate>| View {
        child: Some(c),
        ledger: Some(Ok(None)),
        gate: g.map(Ok),
        ..View::default()
    };
    check(vec![
        row(
            "the gate reads the ledger of the op that created the child",
            View {
                child: Some(ORPHAN),
                ..View::default()
            },
            Event::Gate,
            Plan::Need(Need::Ledger),
        )
        .with(|b| b.ledger_not_proof = true, Plan::Need(Need::Gate)),
        row(
            "ledger Prepared ⇒ the child may have entered the table — the gate refuses",
            View {
                ledger: Some(Ok(Some(Phase::Prepared))),
                ..gate(ORPHAN, Some(CLEAR))
            },
            Event::Gate,
            Plan::Keep(Keep::MayHaveCommitted),
        )
        .with(|b| b.ledger_not_proof = true, Plan::Prove),
        row(
            "reading the ledger failed ⇒ the gate refuses (unknown means keep)",
            View {
                ledger: Some(Err(ReadErr)),
                ..gate(ORPHAN, Some(CLEAR))
            },
            Event::Gate,
            Plan::Keep(Keep::Unreadable),
        ),
        row(
            "permanently out of the table ⇒ read the set",
            gate(ORPHAN, None),
            Event::Gate,
            Plan::Need(Need::Gate),
        )
        .with(|b| b.r1i_trusts_history = true, Plan::Prove),
        row(
            "nobody stands on it ⇒ proven",
            gate(ORPHAN, Some(CLEAR)),
            Event::Gate,
            Plan::Prove,
        ),
        row(
            "a retired parent a child stands on ⇒ refuse",
            gate(
                ORPHAN,
                Some(Gate {
                    refs: Refs::Referenced,
                    ..CLEAR
                }),
            ),
            Event::Gate,
            Plan::Keep(Keep::Referenced),
        )
        .with(|b| b.r1i_trusts_history = true, Plan::Prove),
        row(
            "N2: a set not adding children in flight ⇒ refuse",
            gate(
                ORPHAN,
                Some(Gate {
                    refs_in_flight: false,
                    ..CLEAR
                }),
            ),
            Event::Gate,
            Plan::Keep(Keep::StaleRefs),
        )
        .with(|b| b.dd_ignores_in_flight = true, Plan::Prove),
        row(
            "child in the table ⇒ refuse",
            gate(
                Child {
                    in_table: true,
                    ..ORPHAN
                },
                Some(CLEAR),
            ),
            Event::Gate,
            Plan::Keep(Keep::InTable),
        ),
        row(
            "child promised ⇒ refuse, even for the lenient gate",
            gate(
                Child {
                    promised: true,
                    ..ORPHAN
                },
                Some(CLEAR),
            ),
            Event::Gate,
            Plan::Keep(Keep::Promised),
        )
        .with(|b| b.r1i_trusts_history = true, Plan::Keep(Keep::Promised)),
    ]);
}

#[test]
fn proxy_tombstone() {
    let o = |owner: Result<Owner, ReadErr>| View {
        orphan: Some(Orphan {
            in_table: false,
            promised: false,
            allocated: true,
            owner,
        }),
        ..View::default()
    };
    check(vec![
        row(
            "SM-11: the owner yielded ⇒ write a tombstone",
            o(Ok(Owner::Released)),
            Event::Tomb,
            Plan::Tombstone,
        )
        .with(|b| b.sm11_no_proxy_tomb = true, Plan::Ignore),
        row(
            "SM-11: the owner died per claim's rules ⇒ write a tombstone",
            o(Ok(Owner::Held { dead: Ok(true) })),
            Event::Tomb,
            Plan::Tombstone,
        ),
        row(
            "SM-11: the owner is not proven dead ⇒ keep",
            o(Ok(Owner::Held { dead: Ok(false) })),
            Event::Tomb,
            Plan::Keep(Keep::OwnerLive),
        ),
        row(
            "SM-11: reading the session failed ⇒ keep",
            o(Ok(Owner::Held { dead: Err(ReadErr) })),
            Event::Tomb,
            Plan::Keep(Keep::Unreadable),
        ),
        row(
            "SM-11: unknown wal_kind ⇒ keep",
            o(Ok(Owner::UnknownWal)),
            Event::Tomb,
            Plan::Keep(Keep::UnknownWal),
        ),
        row(
            "already tombstoned ⇒ stop",
            o(Ok(Owner::Tomb)),
            Event::Tomb,
            Plan::Ignore,
        ),
        row(
            "SM-11 (2): child promised ⇒ keep",
            View {
                orphan: Some(Orphan {
                    in_table: false,
                    promised: true,
                    allocated: true,
                    owner: Ok(Owner::Released),
                }),
                ..View::default()
            },
            Event::Tomb,
            Plan::Keep(Keep::Promised),
        )
        .with(|b| b.sm11_ignores_promised = true, Plan::Tombstone),
    ]);
}
