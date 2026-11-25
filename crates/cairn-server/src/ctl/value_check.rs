// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The value checks of `verify`:
//!
//! - **(m) `value_orphan`**: a `values/…` object with no holder, older than
//!   [`MIN_AGE`], with no trash entry yet. The value sweep heals it
//!   (`heals = "value sweep"`): it is the "PUT done, commit failed" gap or a holder just
//!   removed.
//! - **(n) `value_dangling`**: a holder row whose object is missing; a ref in a
//!   `SegmentedState` row with no matching `State(k)` holder row; a `ValueRefs`/
//!   `SegmentedState` row that cannot be decoded. No round heals it: the customer's value
//!   cannot be read (or would be deleted by mistake) — except for a tenant in state
//!   `deleting`, whose purge round deletes its segments before its rows (`heals`).
//!
//! A ref in the **journal** (a `SetState`/`GetEagerState`/`GetStateCompletion` entry
//! carrying `Value.segments`) must have an `Invocation(id)` holder of that very
//! invocation — without one it is (n).

use {
    super::{Finding, VerifyReport},
    cairn_store::{
        keys::{self, Table},
        trash::{Subject, Trash},
        value::{decode_ref_row, Holder, Owner, Segment, SegmentedRow, ValueStore},
        PartitionReader,
    },
    slatedb::object_store::{path::Path, ObjectStore},
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    },
};

pub const ORPHAN: &str = "value_orphan";
pub const DANGLING: &str = "value_dangling";

/// The age below which a segment with no holder is not yet an orphan (like
/// `value_sweep::DEFAULT_MIN_AGE`).
pub const MIN_AGE_MS: u64 = 3_600_000;

/// Holder rows seen, by segment.
#[derive(Default)]
pub(super) struct Facts {
    /// Segment → holders.
    held: BTreeMap<Segment, Vec<Holder>>,
    /// Refs of `SegmentedState` rows: (segment, state key).
    state_refs: Vec<(Segment, bytes::Bytes, u32)>,
    /// Refs in the journal: `(sha, gen)`, the invocation id, the range.
    journal_refs: Vec<(([u8; 32], u64), cairn_store::InvocationId, u32)>,
    /// Broken rows: (range, description).
    bad: Vec<(u32, String)>,
}

/// Reads two tables of a range's Db into `facts` (one scan).
pub(super) async fn collect(
    reader: &PartitionReader,
    range: u32,
    facts: &mut Facts,
) -> cairn_store::Result<()> {
    reader
        .scan_fold(&[], |k, v| {
            match keys::table_of(k) {
                Some(Table::ValueRefs) => {
                    let (Some(r), Some(pk)) = (keys::parse_value_ref(k), keys::owner_of(k)) else {
                        facts
                            .bad
                            .push((range, "ValueRefs key is unreadable".into()));
                        return true;
                    };
                    if decode_ref_row(v).is_none() {
                        facts
                            .bad
                            .push((range, "ValueRefs row is unreadable".into()));
                    }
                    facts
                        .held
                        .entry(Segment {
                            tenant: r.tenant.to_string(),
                            pk,
                            owner: r.owner,
                            sha: r.sha,
                            gen: r.gen,
                        })
                        .or_default()
                        .push(r.holder);
                }
                Some(Table::Journal) => {
                    // Only three entry kinds can carry segmented values: do not decode
                    // every entry.
                    let Ok(entry) =
                        cairn_runtime::JournalEntry::decode(bytes::Bytes::copy_from_slice(v))
                    else {
                        return true;
                    };
                    use cairn_protocol::MessageType as T;
                    if !matches!(
                        entry.ty,
                        T::SET_STATE | T::GET_EAGER_STATE | T::GET_STATE_COMPLETION
                    ) {
                        return true;
                    }
                    let keys::RowOwner::Id { id, .. } = keys::row_owner(k) else {
                        return true;
                    };
                    let frame = cairn_protocol::Frame::new(
                        entry.ty,
                        cairn_protocol::Flags::NONE,
                        entry.payload,
                    );
                    let Ok(m) = cairn_protocol::decode(&frame) else {
                        return true;
                    };
                    match cairn_protocol::value::refs_in(&m) {
                        Ok(refs) => facts
                            .journal_refs
                            .extend(refs.into_iter().map(|r| ((r.sha256, r.gen), id, range))),
                        Err(e) => facts
                            .bad
                            .push((range, format!("journal entry value is unreadable: {e}"))),
                    }
                }
                Some(Table::SegmentedState) => {
                    let (Some((tenant, rest, user)), Some(pk)) =
                        (keys::parse_segmented_state(k), keys::owner_of(k))
                    else {
                        facts
                            .bad
                            .push((range, "SegmentedState key is unreadable".into()));
                        return true;
                    };
                    let Some(row) = SegmentedRow::decode(&bytes::Bytes::copy_from_slice(v)) else {
                        facts
                            .bad
                            .push((range, "SegmentedState row is unreadable".into()));
                        return true;
                    };
                    let owner = Owner::of_object_rest(rest);
                    for r in row.refs {
                        facts.state_refs.push((
                            Segment {
                                tenant: tenant.to_string(),
                                pk,
                                owner,
                                sha: r.sha,
                                gen: r.gen,
                            },
                            bytes::Bytes::copy_from_slice(user),
                            range,
                        ));
                    }
                }
                _ => {}
            }
            true
        })
        .await
}

/// Compares `facts` with the `values/` objects and the trash entries.
pub(super) async fn findings(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    facts: &Facts,
    deleting: &BTreeSet<String>,
    now_ms: u64,
    rep: &mut VerifyReport,
) -> cairn_store::Result<()> {
    let values = ValueStore::new(Arc::clone(store), prefix);
    let mut objects: BTreeMap<Segment, u64> = BTreeMap::new();
    let mut after = None;
    loop {
        let (dirs, next) = values.list_page(after.as_ref(), 1000).await?;
        for d in dirs {
            for (sha, gen, ms) in d.segments {
                objects.insert(
                    Segment {
                        tenant: d.tenant.clone(),
                        pk: d.pk,
                        owner: d.owner,
                        sha,
                        gen,
                    },
                    ms,
                );
            }
        }
        match next {
            Some(p) => after = Some(p),
            None => break,
        }
    }
    rep.items += objects.len();
    let in_trash: BTreeSet<Segment> = Trash::new(Arc::clone(store), prefix)
        .entries()
        .await?
        .into_iter()
        .filter_map(|e| match e.subject {
            Some(Subject::Value(s)) => Some(s),
            _ => None,
        })
        .collect();
    compare(&values, &objects, &in_trash, facts, deleting, now_ms, rep);
    Ok(())
}

/// The pure comparison part of [`findings`]: `objects` (segment → `last_modified` ms) and
/// `in_trash` already read.
/// `deleting`: tenants in state `deleting`. Their purge round deletes the `values/<t>/`
/// prefix every round, also while ranges still hold the tenant's holder and
/// `SegmentedState` rows, so a Db read half way (a backup copied during the purge, verify
/// during the purge) holds refs whose object or holder is gone. Those gaps are reported
/// with `heals`: the round deletes the rows when it resumes, and check (l) reports any row
/// left after `purged`.
fn compare(
    values: &ValueStore,
    objects: &BTreeMap<Segment, u64>,
    in_trash: &BTreeSet<Segment>,
    facts: &Facts,
    deleting: &BTreeSet<String>,
    now_ms: u64,
    rep: &mut VerifyReport,
) {
    for (range, what) in &facts.bad {
        rep.findings
            .push(Finding::new(DANGLING, Some(*range), what.clone()));
    }
    for (seg, ms) in objects {
        if facts.held.contains_key(seg)
            || in_trash.contains(seg)
            || ms.saturating_add(MIN_AGE_MS) > now_ms
        {
            continue;
        }
        rep.findings.push(
            Finding::new(
                ORPHAN,
                None,
                format!("{}: segment has no holder", values.path(seg)),
            )
            .heals("value sweep"),
        );
    }
    let purging = |seg: &Segment| deleting.contains(seg.tenant.as_str());
    let dangling = |seg: &Segment, range: Option<u32>, detail: String| {
        let f = Finding::new(DANGLING, range, detail);
        if purging(seg) {
            f.heals("tenant purge round")
        } else {
            f
        }
    };
    for (seg, holders) in &facts.held {
        if !objects.contains_key(seg) {
            rep.findings.push(dangling(
                seg,
                None,
                format!(
                    "{}: {} holder(s) but the object is missing",
                    values.path(seg),
                    holders.len()
                ),
            ));
        }
    }
    // Refs in the journal: an `Invocation(id)` holder of the same `(sha, gen)`, under any
    // owner (the owner is derived from entry 0's target, not available here).
    let mut by_ref: BTreeMap<([u8; 32], u64), Vec<&Holder>> = BTreeMap::new();
    for (seg, holders) in &facts.held {
        by_ref
            .entry((seg.sha, seg.gen))
            .or_default()
            .extend(holders);
    }
    for (r, id, range) in &facts.journal_refs {
        let held = by_ref.get(r).is_some_and(|h| {
            h.iter()
                .any(|h| matches!(h, Holder::Invocation(x) if x == id))
        });
        if !held {
            rep.findings.push(Finding::new(
                DANGLING,
                Some(*range),
                format!(
                    "journal of {} references segment {}-{:016x} without a holder row",
                    cairn_store::keys::id_hex(id),
                    cairn_store::artifact::sha_hex(&r.0),
                    r.1
                ),
            ));
        }
    }
    for (seg, user, range) in &facts.state_refs {
        let held = facts
            .held
            .get(seg)
            .is_some_and(|h| h.iter().any(|h| matches!(h, Holder::State(k) if k == user)));
        if !held {
            rep.findings.push(dangling(
                seg,
                Some(*range),
                format!(
                    "{}: referenced by state key {:?} without a holder row",
                    values.path(seg),
                    String::from_utf8_lossy(user)
                ),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, bytes::Bytes, cairn_store::PartitionKey, slatedb::object_store::memory::InMemory,
    };

    fn seg(n: u8) -> Segment {
        Segment {
            tenant: "default".into(),
            pk: PartitionKey(7),
            owner: Owner([n; 32]),
            sha: [n; 32],
            gen: 1,
        }
    }

    fn run(
        objects: &BTreeMap<Segment, u64>,
        in_trash: &BTreeSet<Segment>,
        facts: &Facts,
        now_ms: u64,
    ) -> Vec<Finding> {
        run_deleting(objects, in_trash, facts, &BTreeSet::new(), now_ms)
    }

    fn run_deleting(
        objects: &BTreeMap<Segment, u64>,
        in_trash: &BTreeSet<Segment>,
        facts: &Facts,
        deleting: &BTreeSet<String>,
        now_ms: u64,
    ) -> Vec<Finding> {
        let values = ValueStore::new(Arc::new(InMemory::new()), &Path::default());
        let mut rep = VerifyReport::default();
        compare(
            &values, objects, in_trash, facts, deleting, now_ms, &mut rep,
        );
        rep.findings
    }

    /// **A tenant purge in progress read as lost values.** Phase B deletes the tenant's
    /// `values/<t>/` prefix every round, also while ranges still hold its rows (the
    /// holder and `SegmentedState` rows go with the range scans). A Db read in between — a
    /// backup copied during the purge, verify during the purge — holds holder rows whose
    /// object is gone: the purge round's unfinished work, not a lost value.
    ///
    /// Must see: for tenant `gone` (deleting) the missing object and the state ref without
    /// a holder carry `heals`; the same gaps of tenant `default` do not.
    #[test]
    fn dangling_refs_of_a_tenant_being_purged_heal() {
        let gone = |n: u8| Segment {
            tenant: "gone".into(),
            ..seg(n)
        };
        let objects = BTreeMap::new();
        let mut facts = Facts::default();
        facts
            .held
            .insert(gone(1), vec![Holder::State(Bytes::from_static(b"k"))]);
        facts
            .state_refs
            .push((gone(2), Bytes::from_static(b"j"), 0));
        facts
            .held
            .insert(seg(3), vec![Holder::State(Bytes::from_static(b"k"))]);
        let deleting = BTreeSet::from(["gone".to_string()]);
        let f = run_deleting(&objects, &BTreeSet::new(), &facts, &deleting, 0);
        let real: Vec<_> = f.iter().filter(|f| f.heals.is_none()).collect();
        assert_eq!(
            real.len(),
            1,
            "only default's missing object is an error: {f:?}"
        );
        assert!(real[0].detail.contains(&seg(3).owner.hex()), "{f:?}");
        assert!(
            f.iter()
                .filter(|f| f.heals == Some("tenant purge round"))
                .count()
                >= 1,
            "gone's gaps heal: {f:?}"
        );
    }

    /// (m): an object with no holder, older than `MIN_AGE_MS`, no entry ⇒ orphan
    /// (healable); young, held, or with an entry ⇒ not. (n): a holder with the object
    /// missing, a `SegmentedState` ref with no `State(k)` holder, a broken row ⇒ dangling
    /// (not healable).
    #[test]
    fn orphans_heal_and_dangling_refs_do_not() {
        let now = 10 * MIN_AGE_MS;
        let objects: BTreeMap<_, _> = [1, 2, 3].map(|n| (seg(n), now)).into_iter().collect();
        let in_trash: BTreeSet<_> = [seg(3)].into_iter().collect();
        let later = now + MIN_AGE_MS + 1;
        let mut facts = Facts::default();
        facts
            .held
            .insert(seg(2), vec![Holder::State(Bytes::from_static(b"k"))]);

        assert!(
            run(&objects, &in_trash, &facts, now).is_empty(),
            "still young: nothing"
        );
        let f = run(&objects, &in_trash, &facts, later);
        assert_eq!(f.len(), 1, "{f:?}");
        assert_eq!((f[0].check, f[0].heals), (ORPHAN, Some("value sweep")));
        assert!(f[0].detail.contains(&seg(1).owner.hex()), "{f:?}");

        // A holder with the object missing; a State ref with no holder; a broken row.
        facts.held.insert(seg(9), vec![Holder::Invocation([0; 24])]);
        facts
            .state_refs
            .push((seg(2), Bytes::from_static(b"other"), 0));
        facts
            .bad
            .push((0, "SegmentedState row is unreadable".into()));
        let f: Vec<_> = run(&objects, &in_trash, &facts, now)
            .into_iter()
            .map(|f| (f.check, f.heals.is_none()))
            .collect();
        assert_eq!(f, vec![(DANGLING, true); 3], "{f:?}");
    }
}
