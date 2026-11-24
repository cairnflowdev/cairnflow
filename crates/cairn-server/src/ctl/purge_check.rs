// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Check (l) `tenant-residue` of `verify`.
//!
//! - A `purged` tenant (tombstone) that still has rows in a range's Db or objects
//!   under one of its prefixes ⇒ a finding no round heals: the purge promised it was
//!   done and it is not. A hold-in-place entry of it ⇒ a healable finding: an old runner
//!   wrote the entry after the tombstone (two rounds overlapping when the runner role
//!   moves node), the purge round removes the entry on a later tick, deleting nothing;
//!   objects under the prefix (if any) are still reported separately.
//! - A `TenantPrefix` hold entry of a tenant that is not `deleting`/`purged` ⇒ a
//!   healable finding: the tenant was reversed, the purge round removes the entry on a
//!   later tick.
//! - A `deleting` tenant past `purging-ms=` ⇒ an information line (purging).
//! - Exception: the progress entries `tenant-purge/<t>/` of a `purged` tenant (object or
//!   hold entry) carry no customer data and the purge round cleans them
//!   (`Purger::sweep_progress_residue`) ⇒ a healable finding.
//!
//! The tenant's rows in the Db: the keys carrying the tenant (`State`, `Promises`,
//! `PromiseWaiters`, `Idempotency`, query indexes) and its `InvocationMeta` rows —
//! enough to see residue; rows keyed by id with no meta left are the job of the purge
//! round's full scan.
//!
//! Runs with `open_dbs` (`cairn-ctl verify`, `Bucket::drop`), like (k).

use {
    super::{CtlResult, Finding, VerifyOptions, VerifyReport},
    cairn_store::{
        keys::{self as keyspace, RowOwner, Table},
        tenant_purge::{self as tp, PrefixKind, TenantLine},
        trash::{Subject, Trash},
        TenantId,
    },
    slatedb::object_store::{path::Path, ObjectStore},
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    },
};

/// The check name.
pub const CHECK: &str = "tenant-residue";

/// The `tenants` registry read fresh; broken ⇒ one finding and an empty registry (nothing checked).
pub(super) async fn book(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    rep: &mut VerifyReport,
) -> BTreeMap<TenantId, TenantLine> {
    match tp::read_book(store, prefix).await {
        Ok(b) => b,
        // A registry of a newer binary or broken: do not check residue — an information
        // line, not a finding (a broken registry has its own node warning).
        Err(e) => {
            rep.notes.push(Finding::new(
                CHECK,
                None,
                format!("cannot read the tenants registry — tenant residue not checked: {e}"),
            ));
            BTreeMap::new()
        }
    }
}

/// Tenant mang bia `purged`.
pub(super) fn purged(book: &BTreeMap<TenantId, TenantLine>) -> BTreeSet<TenantId> {
    book.iter()
        .filter(|(_, l)| l.state.as_deref() == Some("purged"))
        .map(|(t, _)| t.clone())
        .collect()
}

/// Counts the rows of `purged` tenants in a range's Db.
pub(super) async fn rows(
    reader: &cairn_store::PartitionReader,
    purged: &BTreeSet<TenantId>,
) -> cairn_store::Result<BTreeMap<TenantId, u64>> {
    let mut out: BTreeMap<TenantId, u64> = BTreeMap::new();
    if purged.is_empty() {
        return Ok(out);
    }
    reader
        .scan_fold(&[], |k, v| {
            let t = match keyspace::row_owner(k) {
                RowOwner::Tenant(t) => Some(t),
                _ if keyspace::table_of(k) == Some(Table::InvocationMeta) => {
                    cairn_store::query_index::InvocationMeta::decode(v)
                        .and_then(|m| TenantId::new(&m.tenant).ok())
                }
                _ => None,
            };
            if let Some(t) = t.filter(|t| purged.contains(t)) {
                *out.entry(t).or_default() += 1;
            }
            true
        })
        .await?;
    Ok(out)
}

/// Findings about objects and hold entries (no Db needed).
pub(super) async fn findings(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    book: &BTreeMap<TenantId, TenantLine>,
    _opts: &VerifyOptions,
    rep: &mut VerifyReport,
) -> CtlResult<()> {
    rep.items += 1;
    for t in purged(book) {
        for kind in PrefixKind::ALL {
            if !tp::objects(store, prefix, &t, kind, 0).await?.is_empty() {
                let f = Finding::new(
                    CHECK,
                    None,
                    format!(
                        "{}: tenant {t} is purged but objects remain",
                        kind.path(prefix, &t)
                    ),
                );
                rep.findings.push(if kind == PrefixKind::Progress {
                    f.heals("tenant purge loop removes leftover purge progress")
                } else {
                    f
                });
            }
        }
    }
    for (t, l) in book {
        if l.state.as_deref() == Some("deleting") && l.purging.is_present() {
            rep.notes.push(
                Finding::new(CHECK, None, format!("tenant {t} is being purged (phase B)"))
                    .heals("tenant purge loop"),
            );
        }
    }
    for e in Trash::new(Arc::clone(store), prefix).entries().await? {
        let Some(Subject::TenantPrefix { tenant, kind }) = e.subject else {
            continue;
        };
        let state = book.get(&tenant).and_then(|l| l.state.as_deref());
        match state {
            Some("deleting") => {}
            Some("purged") => {
                let f = Finding::new(
                    CHECK,
                    None,
                    format!(
                        "trash entry {}: tenant {tenant} is purged but the entry still holds \
                         prefix {kind}",
                        e.id
                    ),
                );
                rep.findings.push(if kind == PrefixKind::Progress {
                    f.heals("tenant purge loop removes leftover purge progress")
                } else {
                    f.heals("tenant purge loop releases the hold (deletes nothing)")
                });
            }
            _ => rep.findings.push(
                Finding::new(
                    CHECK,
                    None,
                    format!(
                        "trash entry {}: holds prefix {kind} of tenant {tenant} that is not in \
                         deleting (reversed?)",
                        e.id
                    ),
                )
                .heals("tenant purge loop removes the hold"),
            ),
        }
    }
    Ok(())
}
