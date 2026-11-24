// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `ops/<key>` ledger cleanup round: the ledger of a settled op leaves the bucket.

use super::{check, row};
use crate::{Event, Keep, Need, Phase, Plan, ReadErr, Remains, Standing, View};

const SETTLED: Remains = Remains {
    old: true,
    mentioned: false,
    sources_tombed: true,
    children_gone: true,
};

fn v(phase: Option<Phase>, standing: Standing, r: Remains) -> View {
    View {
        ledger: Some(Ok(phase)),
        standing: Some(Ok(standing)),
        remains: Some(Ok(r)),
        ..View::default()
    }
}

const FREE: Standing = Standing::Free { other: false };

#[test]
fn reap() {
    check(vec![
        row(
            "read the ledger",
            View::default(),
            Event::ReapLedger,
            Plan::Need(Need::Ledger),
        ),
        row(
            "read the table",
            View {
                ledger: Some(Ok(Some(Phase::Committed))),
                ..View::default()
            },
            Event::ReapLedger,
            Plan::Need(Need::Standing),
        ),
        row(
            "read the remains",
            View {
                remains: None,
                ..v(Some(Phase::Committed), Standing::Committed, SETTLED)
            },
            Event::ReapLedger,
            Plan::Need(Need::Remains),
        ),
        row(
            "Committed, the sources tombstoned, old enough ⇒ delete the ledger",
            v(Some(Phase::Committed), Standing::Committed, SETTLED),
            Event::ReapLedger,
            Plan::Prove,
        )
        .with(
            |b| b.ledger_kept_forever = true,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "Committed, the children moved on ⇒ delete the ledger",
            v(Some(Phase::Committed), Standing::Elsewhere, SETTLED),
            Event::ReapLedger,
            Plan::Prove,
        ),
        row(
            "Cleaned, the children have no Db and no owner record ⇒ delete the ledger",
            v(Some(Phase::Cleaned), FREE, SETTLED),
            Event::ReapLedger,
            Plan::Prove,
        )
        .with(
            |b| b.ledger_kept_forever = true,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "Committed, a source not tombstoned yet ⇒ keep",
            v(
                Some(Phase::Committed),
                Standing::Committed,
                Remains {
                    sources_tombed: false,
                    ..SETTLED
                },
            ),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        )
        .with(|b| b.ledger_reaps_unsettled = true, Plan::Prove),
        row(
            "Cleaned, a child still has a Db or owner record ⇒ keep",
            v(
                Some(Phase::Cleaned),
                FREE,
                Remains {
                    children_gone: false,
                    ..SETTLED
                },
            ),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        )
        .with(|b| b.ledger_reaps_unsettled = true, Plan::Prove),
        row(
            "the ledger just reached an outcome ⇒ keep (a late roller-forward/back)",
            v(
                Some(Phase::Cleaned),
                FREE,
                Remains {
                    old: false,
                    ..SETTLED
                },
            ),
            Event::ReapLedger,
            Plan::Keep(Keep::Young),
        ),
        row(
            "an intent still mentions the op ⇒ keep",
            v(
                Some(Phase::Cleaned),
                FREE,
                Remains {
                    mentioned: true,
                    ..SETTLED
                },
            ),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "the ledger is not at an outcome ⇒ keep",
            v(Some(Phase::Prepared), Standing::Pending, SETTLED),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        )
        .with(
            |b| b.ledger_reaps_unsettled = true,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "ledger Committed but the table says the intent is gone ⇒ disagreement, keep",
            v(Some(Phase::Committed), FREE, SETTLED),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "ledger Cleaned but the table says the children are in the table ⇒ disagreement, keep",
            v(Some(Phase::Cleaned), Standing::Committed, SETTLED),
            Event::ReapLedger,
            Plan::Keep(Keep::NotSettled),
        ),
        row(
            "no ledger ⇒ nothing",
            v(None, FREE, SETTLED),
            Event::ReapLedger,
            Plan::Ignore,
        ),
        row(
            "reading the ledger failed ⇒ keep",
            View {
                ledger: Some(Err(ReadErr)),
                ..View::default()
            },
            Event::ReapLedger,
            Plan::Keep(Keep::Unreadable),
        ),
        row(
            "reading the table failed ⇒ keep",
            View {
                standing: Some(Err(ReadErr)),
                ..v(Some(Phase::Committed), Standing::Committed, SETTLED)
            },
            Event::ReapLedger,
            Plan::Keep(Keep::Unreadable),
        ),
        row(
            "reading the remains failed ⇒ keep",
            View {
                remains: Some(Err(ReadErr)),
                ..v(Some(Phase::Cleaned), FREE, SETTLED)
            },
            Event::ReapLedger,
            Plan::Keep(Keep::Unreadable),
        ),
    ]);
}
