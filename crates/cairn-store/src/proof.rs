// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The typed proof gate.
//!
//! Every deletion of customer data in cairn — a range's Db
//! ([`crate::ObjectStores::delete_retired_db`],
//! [`crate::ObjectStores::discard_range_db`]), an owner record
//! ([`crate::delete_owner`]), a dead shared WAL log (`LogStore::delete_all`), the operation
//! ledger of a settled op ([`crate::ledger::delete_op`]) — requires a [`DeleteProof`] for
//! **exactly** what it deletes. A proof can only be created by the functions here, and each
//! function **reads durably by itself** exactly the facts the deletion needs — it takes no
//! RAM table and no conclusion from the caller. Callers keep their own policy (age, tick,
//! soft barriers…); the gate only keeps the safety part, and the compiler keeps any path
//! from going around it (like D16 with `Lease::released`). A lint
//! (`scripts/lint-sources.sh`) blocks [`DeleteProof::assume`] outside tests.
//!
//! Two kinds of grounds:
//! - **Table version** (D-d): the proof holds at version `V`; the deletion re-reads the
//!   table right before deleting and refuses if the version has changed (the re-check lives
//!   inside the gate).
//! - **Permanent**: a fact that cannot be reversed — a child of an intent that was CASed
//!   away (ids are never reused, D22), the log of a dead incarnation that no owner record
//!   lists (nobody adds it back), the log of a stream that has stopped acking that no owner
//!   record lists (a rotated stream never writes again).
//!
//! **Progress marks**: a mark used to drop data — cutting shared WAL segments from the
//! owner record — is only taken from **written-out** state ([`DurableReplayMark`]), never
//! from SlateDB's in-memory manifest (bug class NW-7: SlateDB changes the mark in RAM
//! before writing the manifest).
//!
//! Opening a Db for writing has no type gate here: its fence lives on the Db itself (the
//! closed mark, SM-17) — stronger than a type, because SlateDB checks it for every writer.

use {
    crate::{
        cluster::ClusterConfig,
        error::{Result, StoreError},
        owner::{LogId, SessionId},
        partition_table::RangeId,
        trash::Subject,
        ObjectStores,
    },
    std::sync::atomic::{AtomicU64, Ordering},
};

/// The first half of [`DeleteProof::purged_invocation`]: the leader has read the Db of the
/// range holding `owner.id` and found no artifact row carrying `owner.epoch`. Can only be
/// built by [`artifacts_unreferenced`].
#[derive(Debug)]
#[must_use = "only meaningful when passed to DeleteProof::purged_invocation"]
pub struct ArtifactsUnreferenced {
    owner: crate::artifact::Owner,
}

impl ArtifactsUnreferenced {
    pub fn owner(&self) -> &crate::artifact::Owner {
        &self.owner
    }
}

/// Reads under the range's engine (no network I/O outside the Db): the table (in RAM) says
/// `writer`'s range holds `owner.id` and no split/merge intent is in flight; every artifact
/// row of the id is readable and none carries `owner.epoch`. A damaged row, another range,
/// an intent ⇒ cannot be proven (keep).
pub async fn artifacts_unreferenced<S: crate::RangeStore>(
    writer: &crate::PartitionWriter<S>,
    cluster: &ClusterConfig,
    owner: &crate::artifact::Owner,
) -> Result<std::result::Result<ArtifactsUnreferenced, String>> {
    let table = cluster.table();
    let entry = table.range_of_key(crate::cluster::partition_key_of(&owner.id));
    if entry.id.0 != writer.partition().0 {
        return Ok(Err(format!(
            "range {} does not hold the id (the table says range {})",
            writer.partition().0,
            entry.id.0
        )));
    }
    if entry.intent.is_some() {
        return Ok(Err(format!("range {} is splitting/merging", entry.id.0)));
    }
    for (_, row) in writer.artifacts_of_unbounded(&owner.id).await? {
        match row {
            None => return Ok(Err("unreadable artifact row".into())),
            Some(r) if r.epoch == owner.epoch => {
                return Ok(Err(format!("artifact row epoch {:016x} remains", r.epoch)))
            }
            Some(_) => {}
        }
    }
    Ok(Ok(ArtifactsUnreferenced {
        owner: owner.clone(),
    }))
}

/// The first half of [`DeleteProof::value_unreferenced`]: the leader has read the Db of
/// the range holding `pk` and knows the **complete** set of `(sha, gen)` the owner still
/// has holders for. Can only be built by [`values_referenced`].
#[derive(Debug)]
#[must_use = "only meaningful when passed to DeleteProof::value_unreferenced"]
pub struct ValuesReferenced {
    tenant: String,
    pk: crate::PartitionKey,
    owner: crate::value::Owner,
    held: std::collections::BTreeSet<([u8; 32], u64)>,
}

impl ValuesReferenced {
    /// The `(sha, gen)` that still have holders.
    pub fn held(&self) -> &std::collections::BTreeSet<([u8; 32], u64)> {
        &self.held
    }

    pub fn owner(&self) -> (&str, crate::PartitionKey, &crate::value::Owner) {
        (&self.tenant, self.pk, &self.owner)
    }
}

/// Reads under the range's engine (no network I/O outside the Db): the table (in RAM) says
/// `writer`'s range holds `pk` and no split/merge intent is in flight; every
/// [`crate::keys::Table::ValueRefs`] row of `(tenant, owner)` is readable. A damaged key or
/// value, an unknown tenant, another range, an intent ⇒ cannot be proven (keep the whole
/// directory).
pub async fn values_referenced<S: crate::RangeStore>(
    writer: &crate::PartitionWriter<S>,
    cluster: &ClusterConfig,
    tenant: &str,
    pk: crate::PartitionKey,
    owner: &crate::value::Owner,
) -> Result<std::result::Result<ValuesReferenced, String>> {
    let table = cluster.table();
    let entry = table.range_of_key(pk);
    if entry.id.0 != writer.partition().0 {
        return Ok(Err(format!(
            "range {} does not hold pk {:016x} (the table says range {})",
            writer.partition().0,
            pk.0,
            entry.id.0
        )));
    }
    if entry.intent.is_some() {
        return Ok(Err(format!("range {} is splitting/merging", entry.id.0)));
    }
    let Ok(tid) = crate::scope::TenantId::new(tenant) else {
        return Ok(Err(format!("tenant {tenant:?} is not valid")));
    };
    let prefix = crate::keys::value_refs_prefix(pk, &tid, owner);
    let mut held = std::collections::BTreeSet::new();
    for (k, v) in writer.value_refs_under_unbounded(&prefix).await? {
        let Some(r) = crate::keys::parse_value_ref(&k) else {
            return Ok(Err("unreadable holder key".into()));
        };
        if crate::value::decode_ref_row(&v).is_none() {
            return Ok(Err("unreadable holder row".into()));
        }
        if r.tenant != tid || r.owner != *owner {
            return Ok(Err("holder row off its prefix".into()));
        }
        held.insert((r.sha, r.gen));
    }
    Ok(Ok(ValuesReferenced {
        tenant: tenant.to_string(),
        pk,
        owner: *owner,
        held,
    }))
}

/// The `tenants/purge` gate is on (`cluster-version` read fresh) — condition 1 of every
/// tenant purge proof.
async fn purge_gate(
    store: &std::sync::Arc<dyn slatedb::object_store::ObjectStore>,
    root: &slatedb::object_store::path::Path,
) -> Result<std::result::Result<(), String>> {
    let on = crate::migrate::read_cluster_version(store, root)
        .await?
        .is_some_and(|(cv, _)| cv.tenant_purge_on());
    Ok(if on {
        Ok(())
    } else {
        Err("the tenants/purge gate is not on".into())
    })
}

/// The right to logically delete a tenant's **rows** in a range Db: the half outside the
/// engine, built by [`tenant_purge_grant`] (reads the bucket — network I/O, not holding the
/// engine, D16); the half inside the engine consumes it (`Engine::purge_tenant_rows`). Not
/// `Clone`.
#[derive(Debug)]
#[must_use = "only meaningful when passed to the tenant row delete command"]
pub struct TenantPurgeGrant {
    tenant: crate::scope::TenantId,
}

impl TenantPurgeGrant {
    pub fn tenant(&self) -> &crate::scope::TenantId {
        &self.tenant
    }

    /// A grant that checks nothing — **test only** for the engine.
    #[cfg(any(test, feature = "testkit"))]
    pub fn assume(tenant: crate::scope::TenantId) -> Self {
        Self { tenant }
    }
}

/// Builds a [`TenantPurgeGrant`]: the same conditions as [`DeleteProof::tenant_purged`]
/// (gate on, `deleting` + `purging-ms=` + `keep` elapsed), read fresh at the time of the
/// call.
pub async fn tenant_purge_grant(
    stores: &ObjectStores,
    tenant: &crate::scope::TenantId,
    keep: std::time::Duration,
    now_ms: u64,
) -> Result<std::result::Result<TenantPurgeGrant, String>> {
    let main = stores.main();
    if let Err(why) = purge_gate(main.store(), main.prefix()).await? {
        return Ok(Err(why));
    }
    let line = crate::tenant_purge::read_line(main.store(), main.prefix(), tenant).await?;
    Ok(
        crate::tenant_purge::purge_due(tenant, line.as_ref(), keep, now_ms).map(|_| {
            TenantPurgeGrant {
                tenant: tenant.clone(),
            }
        }),
    )
}

/// The result of a proof builder: `Ok(Err(reason))` = cannot be proven (keep); `Err` = a
/// failed read (keep, delete only when provable).
pub type Proven = std::result::Result<DeleteProof, String>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Basis {
    TableVersion(u64),
    Permanent,
    /// Only enough to **hold in place** ([`crate::trash::Trash::hold_in_place`]) — the
    /// deletion promise of phase A of a tenant purge ([`DeleteProof::tenant_deleting`]).
    /// `purge_in_place` refuses.
    HoldOnly,
}

/// Proof that `subject` may be deleted. Not `Clone`: each proof serves one deletion.
#[derive(Debug)]
#[must_use = "a proof only means something when passed to a deletion"]
pub struct DeleteProof {
    subject: Subject,
    basis: Basis,
}

impl DeleteProof {
    pub fn subject(&self) -> &Subject {
        &self.subject
    }

    /// The table version the proof rests on; `None` = permanent.
    pub fn table_version(&self) -> Option<u64> {
        match self.basis {
            Basis::TableVersion(v) => Some(v),
            Basis::Permanent | Basis::HoldOnly => None,
        }
    }

    /// The proof is enough to really delete (not just to hold in place).
    pub fn may_purge(&self) -> bool {
        self.basis != Basis::HoldOnly
    }

    /// **D-d** (SM-9): the Db of retired range `r` is no longer needed by anyone, at
    /// `now_ms`. Reload the table, then on **exactly** that snapshot: `r` is not in the
    /// table and promised by no intent; `r`'s owner record is a tombstone; no GC barrier
    /// (an unreadable barrier included) covers `r`; no live range — children in flight
    /// included — lists `r` in `external_dbs` (doubt ⇒ keep).
    ///
    /// **And** (the second barrier) `r`'s **own** manifest has no final checkpoint left of
    /// a Db that still exists ([`ObjectStores::checkpoint_pins`]): positive evidence, read
    /// from the ancestor's side, independent of the scan. The two disagreeing (the scan says
    /// nobody, while a live range still has a named checkpoint on `r`) ⇒ keep + tripwire
    /// `gc::retired_db::proofs_disagree` + [`stats::PROOFS_DISAGREE`]; unnamed checkpoints
    /// (from before checkpoint naming) ⇒ keep; a failed read ⇒ keep.
    ///
    /// Reads the reference set fresh. A D-d round looking at many candidates uses
    /// [`Self::retired_db_with`] to read it once per table version.
    pub async fn retired_db(
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        r: RangeId,
        now_ms: u64,
    ) -> Result<Proven> {
        Self::retired_db_with(stores, cluster, r, now_ms, &mut RoundRefs::default()).await
    }

    /// Like [`Self::retired_db`], with the reference set taken from `refs`: reused when it
    /// was read at **exactly** the version of the table just reloaded, read again (and kept
    /// in `refs`) otherwise. The core still requires the set's version to equal the proof's
    /// table version, and the deletion re-reads that version ([`Self::check`]) as before.
    pub async fn retired_db_with(
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        r: RangeId,
        now_ms: u64,
        round: &mut RoundRefs,
    ) -> Result<Proven> {
        cluster.reload().await?;
        let t = cluster.table();
        if t.contains(r) {
            return Ok(Err(format!("range {} is still in the table", r.0)));
        }
        let promised = t.promised();
        if promised.contains(&r) {
            return Ok(Err(format!("range {} is promised by an intent", r.0)));
        }
        let main = stores.main();
        let tomb = matches!(
            crate::owner::read_owner(main.store(), main.prefix(), r).await?,
            Some(rec) if rec.retired
        );
        if !tomb {
            return Ok(Err(format!("range {} carries no tombstone", r.0)));
        }
        let barriers = crate::gc_barrier::list_barriers(main.store(), main.prefix()).await?;
        let barrier = barriers.iter().find(|b| b.blocks(r, now_ms));
        let mut live: Vec<RangeId> = t.partitions().collect();
        // Chaos fault injection: GC broken on purpose — only to prove that
        // `cairn-ctl verify` catches the consequences.
        let (refs, refs_version, why) = if crate::failpoint::fires("gc::skip_reference_check") {
            (cairn_split::Refs::Clear, t.version(), None)
        } else {
            let (version, refs) = round.get(stores, &t).await?;
            (refs.refs_of(r), version, refs.blocker(r))
        };
        live.extend(promised.iter().copied());
        // The core decides (`cairn_split::Event::ReapProve`) on **exactly** snapshot `t`:
        // the proof carries its version, and the deletion re-reads that version
        // ([`Self::check`]). No comparison with the caller's plan: the table changing since
        // the plan does not make this proof wrong.
        let mut view = cairn_split::View {
            retired: Some(cairn_split::Retired {
                in_table: false,
                promised: false,
                tomb,
                db: true,
            }),
            gate: Some(Ok(cairn_split::Gate {
                table_version: t.version(),
                refs_version,
                refs,
                refs_in_flight: true,
                barrier: barrier.is_some(),
                moved: false,
            })),
            ..cairn_split::View::default()
        };
        let mut pins = None;
        loop {
            match cairn_split::step(&view, &cairn_split::Event::ReapProve) {
                cairn_split::Plan::Need(cairn_split::Need::Pins) => {
                    let read = read_pins(stores, r, &live).await;
                    view.pins = Some(match &read {
                        Ok(p) => Ok(p.core()),
                        Err(_) => Err(cairn_split::ReadErr),
                    });
                    pins = Some(read);
                }
                cairn_split::Plan::Delete => {
                    return Ok(Ok(DeleteProof {
                        subject: Subject::RangeDb(r.0),
                        basis: Basis::TableVersion(t.version()),
                    }))
                }
                cairn_split::Plan::Keep(k) => {
                    let pins_why = match &pins {
                        Some(Ok(p)) => p.blocker(),
                        Some(Err(e)) => Some(format!("cannot read checkpoints: {e}")),
                        None => None,
                    };
                    match k {
                        cairn_split::Keep::ProofsDisagree => {
                            stats::PROOFS_DISAGREE.fetch_add(1, Ordering::Relaxed);
                            // Tripwire (AGENTS.md §6): the scan says deletable while a live
                            // Db still stands on `r`.
                            crate::failpoint!("gc::retired_db::proofs_disagree");
                            tracing::warn!(
                                range = r.0,
                                reason = ?pins_why,
                                version = t.version(),
                                "D-d: the reference-set scan says nobody stands on the retired Db, but a live Db still has a named final checkpoint on it — keeping"
                            );
                        }
                        cairn_split::Keep::Referenced => scan_only(stores, r, &live).await,
                        _ => {}
                    }
                    return Ok(Err(barrier
                        .map(|b| format!("GC barrier {} covers range {}", b.id, r.0))
                        .or(why)
                        .or(pins_why)
                        .unwrap_or_else(|| format!("core holds range {}: {k:?}", r.0))));
                }
                other => unreachable!("the core returned {other:?} for ReapProve"),
            }
        }
    }

    /// The Db of **abandoned child** `c` — a child of a split/merge whose intent was CASed
    /// away (rollback, the orphan-child cleanup loop, `cairn-ctl intent clear`), at
    /// `now_ms`. Reload the table, then on **exactly** that snapshot: `c` is not in the
    /// table, promised by no intent, and its id has been allocated (`c < next_range_id`) —
    /// the three together are permanent (D22).
    ///
    /// Those three also hold for a **retired parent range** that children still stand on,
    /// so the gate itself also checks what the deletion needs — (1)–(3) of SM-14, which used
    /// to be checked only by the caller (`gc::discard_if_orphan_child`), exactly the shape of
    /// an earlier bug:
    ///
    /// 1. no range in the table — children in flight included — lists `c` in
    ///    `external_dbs` ([`ObjectStores::referenced_ranges_with`]; a failed read, a live Db
    ///    without a manifest yet, an unknown prefix ⇒ keep);
    /// 2. no GC barrier (an unreadable barrier included) covers `c` — references from
    ///    outside the table (`cairn-ctl fork`) are only visible through barriers;
    /// 3. the grounds are the table version: the deletion re-reads the table and refuses if
    ///    the version has changed (as for D-d).
    pub async fn abandoned_child_db(
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        c: RangeId,
        now_ms: u64,
    ) -> Result<Proven> {
        cluster.reload().await?;
        let t = cluster.table();
        // The core decides (`cairn_split::Event::Gate`).
        let mut view = cairn_split::View {
            child: Some(cairn_split::Child {
                in_table: t.contains(c),
                promised: t.promised().contains(&c),
                allocated: c.0 < t.next_range_id(),
                db: true,
                owner: true,
                stands_on_live: false,
            }),
            ..cairn_split::View::default()
        };
        let mut why = None;
        loop {
            match cairn_split::step(&view, &cairn_split::Event::Gate) {
                // The ledger of the op that created `c` — `Prepared`/`Committed` ⇒ the
                // child may already have entered the table, not a job for child cleanup.
                cairn_split::Plan::Need(cairn_split::Need::Ledger) => {
                    let main = stores.main();
                    view.ledger = Some(
                        match crate::ledger::read_op_of_child(main.store(), main.prefix(), c.0)
                            .await
                        {
                            Ok(o) => Ok(o.map(|o| o.record.phase)),
                            Err(e) => {
                                why = Some(format!(
                                    "cannot read the operation ledger of range {}: {e}",
                                    c.0
                                ));
                                Err(cairn_split::ReadErr)
                            }
                        },
                    );
                }
                cairn_split::Plan::Need(cairn_split::Need::Gate) => {
                    let main = stores.main();
                    let barriers =
                        crate::gc_barrier::list_barriers(main.store(), main.prefix()).await?;
                    let barrier = barriers.iter().find(|b| b.blocks(c, now_ms));
                    let live: Vec<RangeId> = t.partitions().collect();
                    let refs = stores.referenced_ranges_with(&live, &t.promised()).await?;
                    why = barrier
                        .map(|b| format!("GC barrier {} covers range {}", b.id, c.0))
                        .or_else(|| refs.blocker(c));
                    view.gate = Some(Ok(cairn_split::Gate {
                        table_version: t.version(),
                        refs_version: t.version(),
                        refs: refs.refs_of(c),
                        refs_in_flight: true,
                        barrier: barrier.is_some(),
                        moved: false,
                    }));
                }
                cairn_split::Plan::Prove => {
                    return Ok(Ok(DeleteProof {
                        subject: Subject::RangeDb(c.0),
                        basis: Basis::TableVersion(t.version()),
                    }))
                }
                cairn_split::Plan::Keep(k) => {
                    return Ok(Err(match abandoned(&t, c) {
                        Err(permanence) => permanence,
                        Ok(()) if k == cairn_split::Keep::MayHaveCommitted => format!(
                            "the operation ledger says the op creating range {} may have committed — not an abandoned child",
                            c.0
                        ),
                        Ok(()) => why.unwrap_or_else(|| format!("core holds range {}: {k:?}", c.0)),
                    }))
                }
                other => unreachable!("the core returned {other:?} for Gate"),
            }
        }
    }

    /// The owner record of abandoned child `c`: out of the table, not promised, id
    /// allocated (permanent, D22), **and** `c`'s Db has no manifest left — the Db is still
    /// the evidence of the checkpoint it placed on the source range (RD4), so the owner
    /// record goes after it. No reference check needed: nobody stands on an owner record,
    /// and the Db has already gone through [`Self::abandoned_child_db`] or D-d.
    pub async fn abandoned_child_owner(
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        c: RangeId,
    ) -> Result<Proven> {
        cluster.reload().await?;
        if let Err(why) = abandoned(&cluster.table(), c) {
            return Ok(Err(why));
        }
        if stores.range_db_exists(c).await? {
            return Ok(Err(format!("range {} Db still has a manifest (RD4)", c.0)));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::Owner(c.0),
            basis: Basis::Permanent,
        }))
    }

    /// The `ops/<key>` operation ledger of a **settled** op, at `now_ms` with minimum age
    /// `min_age_ms`. Reads by itself: the ledger (phase, `last_modified`), the reloaded
    /// table (where the op stands, which intents mention the op), then by phase —
    /// `Committed`: the owner record of every source is a tombstone; `Cleaned`: every child
    /// has no Db (manifest) left and its owner record is absent or a tombstone. The core
    /// decides (`cairn_split::Event::ReapLedger`).
    ///
    /// **Permanent** grounds: a tombstone is never removed, a retired range never returns to
    /// the table, child ids are never reused (D22) so no later intent mentions exactly this
    /// op again, and nobody recreates the Db/owner record of a child of a `Cleaned` op (only
    /// an operator restoring from the trash). After the outcome nobody writes the ledger any
    /// more (`Phase::advance`: `Committed`/`Cleaned` are not overwritten), so the ledger that
    /// was read is the ledger deleted. An absent ledger is the behaviour from before ledgers
    /// existed (every ledger reader treats `Ok(None)` as an op of an old binary) — M2 proves
    /// it is still safe once the op has settled (`LedgerReap`).
    pub async fn settled_op(
        stores: &ObjectStores,
        cluster: &ClusterConfig,
        key: u32,
        min_age_ms: u64,
        now_ms: u64,
    ) -> Result<Proven> {
        let main = stores.main();
        let (store, prefix) = (main.store(), main.prefix());
        let Some((rec, modified_ms)) = crate::ledger::read_op_aged(store, prefix, key).await?
        else {
            return Ok(Err(format!("no ledger ops/{key}")));
        };
        let op = rec.op;
        if op.key() != key {
            return Ok(Err(format!(
                "ledger ops/{key} carries an op of key {} — damaged bucket, keeping",
                op.key()
            )));
        }
        cluster.reload().await?;
        let t = cluster.table();
        let mut view = cairn_split::View {
            ledger: Some(Ok(Some(rec.phase))),
            standing: Some(Ok(crate::ledger::standing(&t, op))),
            ..cairn_split::View::default()
        };
        loop {
            match cairn_split::step(&view, &cairn_split::Event::ReapLedger) {
                cairn_split::Plan::Need(cairn_split::Need::Remains) => {
                    let promised = t.promised();
                    let mentioned = op
                        .sources()
                        .iter()
                        .any(|s| crate::ledger::carries(&t, crate::ids::PartitionId(s), op))
                        || op
                            .children()
                            .iter()
                            .any(|c| promised.contains(&crate::ids::PartitionId(c)));
                    let tombed = |r: u32| async move {
                        Ok::<_, StoreError>(matches!(
                            crate::owner::read_owner(store, prefix, crate::ids::PartitionId(r)).await?,
                            Some(rec) if rec.retired
                        ))
                    };
                    // Only read what the ledger's phase needs; leave the other cell `false`.
                    let mut sources_tombed = false;
                    let mut children_gone = false;
                    if rec.phase == cairn_split::Phase::Committed {
                        sources_tombed = true;
                        for s in op.sources().iter() {
                            sources_tombed &= tombed(s).await?;
                        }
                    } else {
                        children_gone = true;
                        for c in op.children().iter() {
                            let db = stores.range_db_exists(crate::ids::PartitionId(c)).await?;
                            let owner =
                                crate::owner::read_owner(store, prefix, crate::ids::PartitionId(c))
                                    .await?;
                            children_gone &= !db && owner.is_none_or(|o| o.retired);
                        }
                    }
                    view.remains = Some(Ok(cairn_split::Remains {
                        old: now_ms.saturating_sub(modified_ms) >= min_age_ms,
                        mentioned,
                        sources_tombed,
                        children_gone,
                    }));
                }
                cairn_split::Plan::Prove => {
                    return Ok(Ok(DeleteProof {
                        subject: Subject::Op(key),
                        basis: Basis::Permanent,
                    }))
                }
                cairn_split::Plan::Keep(k) => {
                    return Ok(Err(format!(
                        "ledger ops/{key} ({:?}) not settled: {k:?}",
                        rec.phase
                    )))
                }
                cairn_split::Plan::Ignore => return Ok(Err(format!("no ledger ops/{key}"))),
                other => unreachable!("the core returned {other:?} for ReapLedger"),
            }
        }
    }

    /// The shared WAL log `log` of a **dead** incarnation that no owner record — tombstones
    /// included — still lists. The death **and** the log's seal must be seen **before** the
    /// owner-record snapshot (`nodewal/sweep.rs:forget_unlisted`): a snapshot after the death
    /// covers every segment that incarnation added while alive, and a snapshot after the
    /// seal covers every segment a zombie of it (broken clock) CASed before the last frame
    /// it could still ack — frames above the seal cannot be acked (M3
    /// `snapshot_after_seal`). The type enforces that order.
    pub(crate) fn dead_log(
        log: &LogId,
        death: &SessionDeath,
        seal: &SealSeen,
        referenced: &ReferencedLogs,
    ) -> Proven {
        if death.session != log.session {
            return Err(format!(
                "the death of {} is not that of log {log}",
                death.session
            ));
        }
        if seal.log != *log {
            return Err(format!("the seal of {} is not that of log {log}", seal.log));
        }
        if referenced.seq <= death.seq {
            return Err(format!(
                "the owner-record snapshot is older than seeing {} dead",
                death.session
            ));
        }
        if referenced.seq <= seal.seq {
            return Err(format!("owner snapshot older than seal of log {log}"));
        }
        if referenced.logs.contains(log) {
            return Err(format!("an owner record still lists log {log}"));
        }
        Ok(DeleteProof {
            subject: Subject::WalLog(log.clone()),
            basis: Basis::Permanent,
        })
    }

    /// The shared WAL log `log` of a **rotated** stream of a live incarnation that no owner
    /// record — tombstones included — still lists. The same reasoning as [`Self::dead_log`],
    /// with "the stream has stopped acking" as the grounds instead of death: the rotation
    /// **and** the seal must be seen before the owner-record snapshot. The stream stops
    /// acking (`Stripe::retire`) before asking to rotate, and the segment pointing at the log
    /// is CASed before the first frame (C4), so every acked frame has its segment from
    /// before the rotation — a snapshot after the rotation that sees no segment means every
    /// such frame is already in L0 (C7). Nobody adds a segment of this log back: a range's
    /// `register` always goes to the slot's current stream (M3 `retire_before_snapshot`).
    pub(crate) fn rotated_log(
        log: &LogId,
        retired: &StreamRetired,
        seal: &SealSeen,
        referenced: &ReferencedLogs,
    ) -> Proven {
        if retired.log != *log {
            return Err(format!(
                "the rotation of {} is not that of log {log}",
                retired.log
            ));
        }
        if seal.log != *log {
            return Err(format!("the seal of {} is not that of log {log}", seal.log));
        }
        if referenced.seq <= retired.seq {
            return Err(format!(
                "the owner-record snapshot is older than seeing log {log} rotated"
            ));
        }
        if referenced.seq <= seal.seq {
            return Err(format!("owner snapshot older than seal of log {log}"));
        }
        if referenced.logs.contains(log) {
            return Err(format!("an owner record still lists log {log}"));
        }
        Ok(DeleteProof {
            subject: Subject::WalLog(log.clone()),
            basis: Basis::Permanent,
        })
    }

    /// The history copy `cluster-history/<version>` has fallen out of the
    /// [`crate::cluster_history::KEEP`] newest copies: the table **read from the bucket** is
    /// at version ≥ `version + KEEP`. **Permanent**: the table version only grows — even
    /// with `cairn-ctl table restore`, which rebuilds exactly the highest history copy (≥
    /// this table). No table ⇒ cannot be proven: that is exactly when history is needed most
    /// (delete only when provable). A failed read ⇒ `Err`.
    pub async fn stale_table_history(
        store: &std::sync::Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: &slatedb::object_store::path::Path,
        version: u64,
    ) -> Result<Proven> {
        let keep = crate::cluster_history::KEEP;
        let Some(now) = crate::cluster::stored_version(store, prefix).await? else {
            return Ok(Err(
                "no cluster.conf — keeping every table history copy".to_string()
            ));
        };
        if version.saturating_add(keep) > now {
            return Ok(Err(format!(
                "table at version {now}: history copy {version} is still among the {keep} newest"
            )));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::TableHistory(version),
            basis: Basis::Permanent,
        }))
    }

    /// **Artifacts of a dead life** (AP-2, AP-8): the directory
    /// `artifacts/<tenant>/<id>/<epoch>/` is held in place, then deleted. Two halves, because
    /// the first reads the Db under the engine while the second is network I/O (D16):
    ///
    /// 1. [`artifacts_unreferenced`] — on the leader of the range holding the id: no
    ///    [`crate::keys::ArtifactRow`] of the id carries this `epoch`.
    /// 2. this function — every object in the directory is older than `min_age_ms` (≥ 2 ×
    ///    `T_put`): an object just PUT whose row has not committed is still young.
    ///
    /// **Permanent**: the epoch is allocated from `entropy` at the first artifact of a life
    /// and never reused; once the life has no rows left, nobody writes rows with that epoch
    /// again (a retry after a failed commit also allocates a new epoch).
    pub async fn purged_invocation(
        unreferenced: ArtifactsUnreferenced,
        artifacts: &crate::artifact::ArtifactStore,
        min_age_ms: u64,
        now_ms: u64,
    ) -> Result<Proven> {
        let owner = unreferenced.owner;
        let objects = artifacts.objects(&owner).await?;
        if let Some(young) = objects.iter().find(|m| {
            (m.last_modified.timestamp_millis().max(0) as u64).saturating_add(min_age_ms) > now_ms
        }) {
            return Ok(Err(format!(
                "{} is younger than {} ms",
                young.location, min_age_ms
            )));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::Artifacts(owner),
            basis: Basis::Permanent,
        }))
    }

    /// **A segment with no holder left**: the object `values/<t>/<pk>/<owner>/<sha>-<gen>`
    /// is held in place, then deleted. Two halves, because the first reads the Db under the
    /// engine while the second is network I/O (D16):
    ///
    /// 1. [`values_referenced`] — on the leader of the range holding `pk`: no holder row of
    ///    the owner carries `(sha, gen)`.
    /// 2. this function — `seg`'s object exists and is older than `min_age_ms` (≥ 2 ×
    ///    `T_put`): a segment just PUT whose holder row has not committed is still young.
    ///
    /// **Permanent**: a new `gen` is allocated on each seal; a ref is only reused (`reuse`,
    /// COPY at a range boundary) while a holder row still exists, checked under the engine
    /// in the same commit step. A `(sha, gen)` with no holder left never gets one again.
    pub async fn value_unreferenced(
        referenced: &ValuesReferenced,
        values: &crate::value::ValueStore,
        seg: &crate::value::Segment,
        min_age_ms: u64,
        now_ms: u64,
    ) -> Result<Proven> {
        if seg.tenant != referenced.tenant
            || seg.pk != referenced.pk
            || seg.owner != referenced.owner
        {
            return Ok(Err("segment is not under the owner that was read".into()));
        }
        if referenced.held.contains(&(seg.sha, seg.gen)) {
            return Ok(Err("the segment still has holders".into()));
        }
        let Some(meta) = values.head(seg).await? else {
            return Ok(Err("the segment object is absent".into()));
        };
        if (meta.last_modified.timestamp_millis().max(0) as u64).saturating_add(min_age_ms) > now_ms
        {
            return Ok(Err(format!(
                "{} is younger than {} ms",
                meta.location, min_age_ms
            )));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::Value(seg.clone()),
            basis: Basis::Permanent,
        }))
    }

    /// **Phase A of a tenant purge**: the `kind` prefix of `tenant` is **held in place** in
    /// the trash. Reads fresh by itself: the `tenants/purge` gate is on (`cluster-version`),
    /// the tenant is not `default`, the `tenants` book line carries `state=deleting`
    /// verbatim. No age or `purging-ms=` required: the hold entry is only a deletion
    /// promise — this proof is **not** enough to really delete ([`Self::may_purge`] is
    /// false), a real deletion always requires [`Self::tenant_purged`] built at that time.
    pub async fn tenant_deleting(
        stores: &ObjectStores,
        tenant: &crate::scope::TenantId,
        kind: crate::tenant_purge::PrefixKind,
    ) -> Result<Proven> {
        let main = stores.main();
        if let Err(why) = purge_gate(main.store(), main.prefix()).await? {
            return Ok(Err(why));
        }
        let line = crate::tenant_purge::read_line(main.store(), main.prefix(), tenant).await?;
        if let Err(why) = crate::tenant_purge::deleting(tenant, line.as_ref()) {
            return Ok(Err(why));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::TenantPrefix {
                tenant: tenant.clone(),
                kind,
            },
            basis: Basis::HoldOnly,
        }))
    }

    /// **Phase B of a tenant purge**: the `kind` prefix of `tenant` is really deleted.
    /// Reads fresh by itself: like [`Self::tenant_deleting`], **plus** a book line carrying a
    /// readable `purging-ms=` and a readable `deleting-ms=` at least `keep` before `now_ms`
    /// ([`crate::tenant_purge::purge_due`]). A damaged mark, an absent line, an unreadable
    /// book ⇒ cannot be proven (keep).
    ///
    /// **Permanent**: after `purging-ms=` there is no way back (`PUT …/state` returns
    /// `409 tenant_purging`), and the path is built by
    /// [`crate::tenant_purge::PrefixKind::path`] from the tenant id.
    pub async fn tenant_purged(
        stores: &ObjectStores,
        tenant: &crate::scope::TenantId,
        kind: crate::tenant_purge::PrefixKind,
        keep: std::time::Duration,
        now_ms: u64,
    ) -> Result<Proven> {
        let main = stores.main();
        if let Err(why) = purge_gate(main.store(), main.prefix()).await? {
            return Ok(Err(why));
        }
        let line = crate::tenant_purge::read_line(main.store(), main.prefix(), tenant).await?;
        if let Err(why) = crate::tenant_purge::purge_due(tenant, line.as_ref(), keep, now_ms) {
            return Ok(Err(why));
        }
        Ok(Ok(DeleteProof {
            subject: Subject::TenantPrefix {
                tenant: tenant.clone(),
                kind,
            },
            basis: Basis::Permanent,
        }))
    }

    /// **A progress entry left after the tombstone**: the `tenant-purge/<t>/` prefix
    /// ([`crate::tenant_purge::PrefixKind::Progress`] — **only** this kind, carrying no
    /// customer data) of a tenant already `purged` is held in place, then deleted. The
    /// leftover appears when two runners overlap during a leader change (the loser writes
    /// progress after the tombstone) or when an older binary wrote the tombstone. Reads fresh
    /// by itself: the `tenants/purge` gate is on, the tenant is not `default`, the book line
    /// carries `state=purged` verbatim and a readable `purged-ms=`.
    ///
    /// **Permanent**: the `purged` tombstone has no way back.
    pub async fn purged_tenant_progress(
        stores: &ObjectStores,
        tenant: &crate::scope::TenantId,
    ) -> Result<Proven> {
        let main = stores.main();
        if let Err(why) = purge_gate(main.store(), main.prefix()).await? {
            return Ok(Err(why));
        }
        if tenant.is_default() {
            return Ok(Err("the default tenant cannot be deleted".into()));
        }
        let line = crate::tenant_purge::read_line(main.store(), main.prefix(), tenant).await?;
        match line {
            Some(l) if l.state.as_deref() == Some("purged") && l.purged.at().is_some() => {}
            other => {
                return Ok(Err(format!(
                    "tenant {tenant} does not carry a readable purged tombstone yet: {other:?}"
                )))
            }
        }
        Ok(Ok(DeleteProof {
            subject: Subject::TenantPrefix {
                tenant: tenant.clone(),
                kind: crate::tenant_purge::PrefixKind::Progress,
            },
            basis: Basis::Permanent,
        }))
    }

    /// A proof that checks nothing — **test only**, to set up deletion scenarios
    /// (`testkit`). A lint blocks calling it in `crates/*/src`.
    #[cfg(any(test, feature = "testkit"))]
    pub fn assume(subject: Subject) -> DeleteProof {
        DeleteProof {
            subject,
            basis: Basis::Permanent,
        }
    }

    /// [`Self::assume`] for the Db of range `r` — test only.
    #[cfg(any(test, feature = "testkit"))]
    pub fn assume_db(r: RangeId) -> DeleteProof {
        Self::assume(Subject::RangeDb(r.0))
    }

    /// [`Self::assume`] for the owner record of range `r` — test only.
    #[cfg(any(test, feature = "testkit"))]
    pub fn assume_owner(r: RangeId) -> DeleteProof {
        Self::assume(Subject::Owner(r.0))
    }

    /// The deletion's check: the proof is for exactly `expected`, and (for table-version
    /// grounds) the reloaded table is still at that version.
    pub(crate) async fn check(
        &self,
        expected: &Subject,
        cluster: Option<&ClusterConfig>,
    ) -> Result<()> {
        if &self.subject != expected {
            return Err(StoreError::Io(format!(
                "proof for {} but the deletion is {expected}",
                self.subject
            )));
        }
        if !self.may_purge() {
            return Err(StoreError::Io(format!(
                "the proof for {expected} is only enough to hold, not to delete"
            )));
        }
        if let Basis::TableVersion(v) = self.basis {
            let cluster = cluster.ok_or_else(|| {
                StoreError::Io(format!(
                    "the proof for {expected} rests on version {v} but the deletion has no table to re-check"
                ))
            })?;
            cluster.reload().await?;
            let now = cluster.version();
            if now != v {
                return Err(StoreError::Io(format!(
                    "the range table changed from version {v} to {now} after the proof — not deleting {expected}"
                )));
            }
        }
        Ok(())
    }
}

/// `replay_after_wal_id` in the **stored** manifest of range Db `range` — the mark below
/// which every frame is in L0 and the manifest on the bucket says so. Can only be built by
/// [`Self::read`] (one manifest GET); SlateDB's in-RAM mark may run ahead of the stored copy
/// (NW-7), so there is no other way to create this type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableReplayMark {
    range: u32,
    value: u64,
}

impl DurableReplayMark {
    /// Reads the stored manifest. `Ok(None)` = the Db has no manifest yet.
    pub async fn read(stores: &ObjectStores, range: u32) -> Result<Option<DurableReplayMark>> {
        Ok(stores
            .manifest_head(range)
            .await?
            .map(|h| DurableReplayMark {
                range,
                value: h.replay_after_wal_id,
            }))
    }

    pub fn range(&self) -> u32 {
        self.range
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

/// `c` is permanently out of table `t` (D22): not in the table, not promised, id
/// allocated.
fn abandoned(t: &crate::PartitionTable, c: RangeId) -> std::result::Result<(), String> {
    if t.contains(c) {
        return Err(format!("range {} is in the table", c.0));
    }
    if t.promised().contains(&c) {
        return Err(format!("range {} is promised by an intent", c.0));
    }
    if c.0 >= t.next_range_id() {
        return Err(format!(
            "range {} has never been allocated (next_range_id={})",
            c.0,
            t.next_range_id()
        ));
    }
    Ok(())
}

/// The D-d reference set (live ranges **plus** children in flight,
/// [`ObjectStores::referenced_ranges_with`]) at one table version, shared by the plan and
/// every proof of **one** round: one manifest read per live range per version, not per
/// candidate.
///
/// Safe because at a fixed table version the `external_dbs` of live ranges only
/// **shrink** (row N4 of `docs/reliability/irreversible.md`: a live range's Db is created
/// once, before it enters the table; compaction collects SSTs, GC `detach` drops entries) —
/// a set read early is a superset of the real set later, so it can only cause extra
/// **keeping**. A child in flight without a manifest at read time can only stand on its
/// source and what the source lists, which are already in the set. Pinned by:
/// `range_clone.rs::n4_live_refs_only_shrink_at_a_fixed_version`.
///
/// The table changes version ⇒ read again. A set with unprovable spots (`unknown`) is not
/// reused: it may heal by itself without the table changing. Not kept across rounds: the
/// next round reads fresh.
#[derive(Debug, Default)]
pub struct RoundRefs(Option<(u64, crate::RangeRefs)>);

impl RoundRefs {
    /// The set of version `t.version()` — read if not yet there for that version. Returns
    /// the version the set was read at (always `t.version()`).
    pub async fn get(
        &mut self,
        stores: &ObjectStores,
        t: &crate::PartitionTable,
    ) -> Result<(u64, &crate::RangeRefs)> {
        let version = t.version();
        if self
            .0
            .as_ref()
            .is_none_or(|(v, refs)| *v != version || refs.unknown.is_some())
        {
            let live: Vec<RangeId> = t.partitions().collect();
            let refs = stores.referenced_ranges_with(&live, &t.promised()).await?;
            self.0 = Some((version, refs));
        }
        let (v, refs) = self.0.as_ref().expect("just set");
        Ok((*v, refs))
    }
}

/// The final checkpoints on retired Db `r` for the checkpoint barrier — through the fault
/// injection `gc::skip_checkpoint_check` (the `verify` proof run: the barrier is blinded).
pub async fn read_pins(
    stores: &ObjectStores,
    r: RangeId,
    live: &[RangeId],
) -> Result<crate::RangePins> {
    if crate::failpoint::fires("gc::skip_checkpoint_check") {
        return Ok(crate::RangePins::default());
    }
    stores.checkpoint_pins(r, live).await
}

/// The reverse disagreement of the checkpoint barrier: the scan sees a live range standing
/// on `r` while `r`'s manifest has no final checkpoint left. A legitimate window (G4: the
/// child manifest is written before the checkpoint on the ancestor; G6: detach removes the
/// checkpoint before dropping the entry), so not a tripwire — only counted
/// ([`stats::SCAN_ONLY`]). One extra read, only at the proof step (the candidate has passed
/// the plan).
async fn scan_only(stores: &ObjectStores, r: RangeId, live: &[RangeId]) {
    match stores.checkpoint_pins(r, live).await {
        Ok(p) if p.core() == cairn_split::Pins::default() => {
            stats::SCAN_ONLY.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                range = r.0,
                "D-d: the scan sees a reference while the retired Db has no final checkpoint left (G4/G6 window) — keeping"
            );
        }
        _ => {}
    }
}

/// Counters of the checkpoint barrier in this process, cumulative (`/metrics`:
/// `cairnflow_gc_proofs_disagree_total{direction}`).
pub mod stats {
    use std::sync::atomic::AtomicU64;

    /// The scan says nobody stands on the retired Db while a live Db still has a named final
    /// checkpoint on it (tripwire `gc::retired_db::proofs_disagree`).
    pub static PROOFS_DISAGREE: AtomicU64 = AtomicU64::new(0);
    /// The scan sees a reference while the retired Db has no final checkpoint left (G4/G6
    /// window, legitimate).
    pub static SCAN_ONLY: AtomicU64 = AtomicU64::new(0);
}

/// Global order between the reads of [`SessionDeath`] and [`ReferencedLogs`] in the
/// process.
static READS: AtomicU64 = AtomicU64::new(0);

/// Incarnation `session` has been seen dead: the session record is absent or ended.
#[derive(Clone, Debug)]
pub(crate) struct SessionDeath {
    session: SessionId,
    seq: u64,
}

impl SessionDeath {
    /// Built from **the read just made** of the session record (`read_session`): `Some`
    /// when absent or ended; an unreadable record ⇒ `None` (alive).
    pub(crate) fn observed(
        session: &SessionId,
        record: Option<std::result::Result<&crate::SessionRecord, ()>>,
    ) -> Option<SessionDeath> {
        let dead = match record {
            None => true,
            Some(Ok(rec)) => rec.state.is_terminal(),
            Some(Err(())) => false,
        };
        dead.then(|| SessionDeath {
            session: session.clone(),
            seq: READS.fetch_add(1, Ordering::SeqCst) + 1,
        })
    }
}

/// The stream of `log` has been seen **rotated** — no longer acking, never writing again —
/// within its own incarnation. Only `nodewal` can build it, from the incarnation's book of
/// rotated streams (`Inner::is_retired`).
#[derive(Clone, Debug)]
pub(crate) struct StreamRetired {
    log: LogId,
    seq: u64,
}

impl StreamRetired {
    /// Built **right after** seeing `log` in the book of rotated streams.
    pub(crate) fn observed(log: &LogId) -> StreamRetired {
        StreamRetired {
            log: log.clone(),
            seq: READS.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
}

/// The seal of `log` has been seen on the bucket: a seal just made (or the node's existing
/// seal copy), or read from the `forgotten` mark of an unfinished purge.
#[derive(Clone, Debug)]
pub(crate) struct SealSeen {
    log: LogId,
    seq: u64,
}

impl SealSeen {
    /// Built **right after** the seal of `log` is certain on the bucket.
    pub(crate) fn observed(log: &LogId) -> SealSeen {
        SealSeen {
            log: log.clone(),
            seq: READS.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
}

/// A snapshot of every log that an owner record — tombstones included — still lists.
#[derive(Clone, Debug)]
pub(crate) struct ReferencedLogs {
    logs: crate::entropy::HashSet<LogId>,
    seq: u64,
}

impl ReferencedLogs {
    /// One LIST of `owners/` and one GET per range; an unreadable record ⇒ error.
    pub(crate) async fn read(stores: &ObjectStores, in_flight: usize) -> Result<ReferencedLogs> {
        let seq = READS.fetch_add(1, Ordering::SeqCst) + 1;
        let main = stores.main();
        let logs = crate::owner::referenced_logs(main.store(), main.prefix(), in_flight).await?;
        Ok(ReferencedLogs { logs, seq })
    }

    /// The snapshot still lists `log`.
    pub(crate) fn lists(&self, log: &LogId) -> bool {
        self.logs.contains(log)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(node: &str) -> LogId {
        LogId::parse(&format!("{node}/1:0")).unwrap()
    }

    fn snapshot(logs: &[LogId]) -> ReferencedLogs {
        ReferencedLogs {
            logs: logs.iter().cloned().collect(),
            seq: READS.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }

    /// The owner-record snapshot must come **after** seeing the death and after the seal;
    /// a log still listed, or the death or seal of another log ⇒ no proof.
    #[test]
    fn dead_log_needs_death_and_seal_before_snapshot() {
        let a = log("a");
        let before = snapshot(&[]);
        let death = SessionDeath::observed(&a.session, None).expect("absent ⇒ dead");
        let seal = SealSeen::observed(&a);
        assert!(DeleteProof::dead_log(&a, &death, &seal, &before).is_err());
        let after = snapshot(&[]);
        assert!(DeleteProof::dead_log(&a, &death, &seal, &after).is_ok());
        let listed = snapshot(std::slice::from_ref(&a));
        assert!(DeleteProof::dead_log(&a, &death, &seal, &listed).is_err());
        let other = SessionDeath::observed(&log("b").session, None).unwrap();
        let later = snapshot(&[]);
        assert!(DeleteProof::dead_log(&a, &other, &seal, &later).is_err());
        // Seal seen after the snapshot (M3 `snapshot_after_seal`).
        let death = SessionDeath::observed(&a.session, None).unwrap();
        let between = snapshot(&[]);
        let late_seal = SealSeen::observed(&a);
        assert!(DeleteProof::dead_log(&a, &death, &late_seal, &between).is_err());
        let b = log("b");
        let b_seal = SealSeen::observed(&b);
        let last = snapshot(&[]);
        assert!(DeleteProof::dead_log(&a, &death, &b_seal, &last).is_err());
        // An unreadable session record ⇒ treated as alive.
        assert!(SessionDeath::observed(&a.session, Some(Err(()))).is_none());
    }

    /// Like [`dead_log_needs_death_and_seal_before_snapshot`], with the rotation of exactly
    /// that log as the grounds.
    #[test]
    fn rotated_log_needs_retirement_and_seal_before_snapshot() {
        let a = log("a");
        let before = snapshot(&[]);
        let retired = StreamRetired::observed(&a);
        let seal = SealSeen::observed(&a);
        assert!(DeleteProof::rotated_log(&a, &retired, &seal, &before).is_err());
        let after = snapshot(&[]);
        assert!(DeleteProof::rotated_log(&a, &retired, &seal, &after).is_ok());
        let listed = snapshot(std::slice::from_ref(&a));
        assert!(DeleteProof::rotated_log(&a, &retired, &seal, &listed).is_err());
        // The rotation of another log (the other stream of the same incarnation).
        let b = LogId::parse("a/1:1").unwrap();
        let other = StreamRetired::observed(&b);
        let later = snapshot(&[]);
        assert!(DeleteProof::rotated_log(&a, &other, &seal, &later).is_err());
        // Rotation seen after the snapshot (M3 `retire_before_snapshot`).
        let seal = SealSeen::observed(&a);
        let between = snapshot(&[]);
        let late = StreamRetired::observed(&a);
        assert!(DeleteProof::rotated_log(&a, &late, &seal, &between).is_err());
        // Seal seen after the snapshot.
        let retired = StreamRetired::observed(&a);
        let between = snapshot(&[]);
        let late_seal = SealSeen::observed(&a);
        assert!(DeleteProof::rotated_log(&a, &retired, &late_seal, &between).is_err());
    }
}
