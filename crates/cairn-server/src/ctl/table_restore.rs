// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl table restore` (SM-23): rebuilds a `cluster.conf` deleted from outside
//! from the highest history copy (`cluster-history/<V>`, SM-22) — **only when that copy
//! can be proven to be the last one**. Dry run by default; only `--apply` writes, and it
//! writes with `Create`.
//!
//! History is written **after** the CAS, so dying between the CAS and the write leaves
//! history one version behind: the highest copy V may not be the last table. The proof
//! "V is the last" reads the rest of the bucket — every table after V leaves traces
//! there:
//!
//! 1. **Ids**: no `owners/range-<id>`, `ranges/<id>/` or `ops/<key>` log carries an id ≥
//!    V's `next_range_id` — every intent after V assigns ids from there, and every step
//!    after an intent CAS (the log, clone, child claim) leaves an object carrying that id.
//! 2. **Live ranges**: every range in V has an owner record **without** a tombstone — the
//!    tombstone is only written after the commit taking the range out of the table; a
//!    missing record (D-d cleaned it after retirement, or it was never opened) cannot be
//!    told apart ⇒ refused (when in doubt, refuse).
//! 3. **Tombstones**: every range carrying a tombstone is not in V, and is not a child
//!    promised by V's intent (a child committed then retired ⇒ V is two table changes
//!    old).
//! 4. **Operation log**: an op the log says is `committed` has no source left in V; an
//!    op the log says is `cleaned` has no intent left in V (the log records the phase
//!    after its CAS — `aborting` is written before the rollback CAS, so it is acceptable).
//!
//! **A prepared op whose commit may have won**: an intent of V whose `ops/<key>` log is
//! at `Prepared` (or later) — the coordinator may have CASed the commit after V and died
//! before the history write, and the children have taken writes. Writing V back with the
//! intent would let a later failed log read (or `intent clear`) **roll back** the op:
//! the children's Dbs go to the trash, losing acked writes. Every node is stopped at
//! `--apply`, so the forward runner's direct check (`split::forward::prepared`: every
//! child has a Db and an owner record, every source closed at the recorded epoch) is
//! the truth: satisfied ⇒ write **the committed table** (`committed_table`, the same
//! transformation as the coordinator) and the log `Committed`; not satisfied ⇒ the
//! commit cannot have won, keep the intent (rolling back later is safe).
//!
//! Cannot be proven ⇒ refused, printing every mismatch; the operator takes the bucket
//! versioning path (runbook `cluster-conf-lost.md`). `--apply` also requires no
//! unfinished session (`sessions/`) — a running node holds the table in RAM; `--force`
//! skips this when the operator is sure every node has stopped (a node killed with
//! `kill -9` leaves a `live` session).

use {
    super::{Ctl, CtlError, CtlResult, RepairOptions, RepairReport},
    cairn_store::{
        cas::{put_resolving, Cas},
        cluster_history, inspect, ledger, ClusterConfig, PartitionId,
    },
    serde::Serialize,
    slatedb::object_store::{path::Path, PutMode},
    std::collections::BTreeSet,
};

/// The result of `table restore`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableRestoreReport {
    pub applied: bool,
    /// The version of the history copy rebuilt (the highest one).
    pub version: u64,
    pub next_range_id: u32,
    /// Every history version seen in the bucket.
    pub history: Vec<u64>,
    /// The table to be written (already written) — exactly the bytes of
    /// `cluster-history/<version>`, or the committed table of the ops in `rolled_forward`.
    pub table: String,
    /// The `ops/<key>` keys of ops for which the table moves forward (commit) instead of keeping
    /// the intent.
    pub rolled_forward: Vec<u32>,
    /// The proofs checked, line by line.
    pub checked: Vec<String>,
    pub warnings: Vec<String>,
    pub audit: Option<String>,
}

fn config_path(prefix: &Path) -> Path {
    let p = prefix.as_ref();
    if p.is_empty() {
        Path::from("cluster.conf")
    } else {
        Path::from(format!("{p}/cluster.conf"))
    }
}

impl Ctl {
    /// `table restore` — see the module doc.
    pub async fn table_restore(&self, opts: &RepairOptions) -> CtlResult<TableRestoreReport> {
        let main = self.stores.main();
        let (store, prefix) = (main.store(), main.prefix());
        if let Some(c) = ClusterConfig::read_only(store, prefix).await? {
            return Err(CtlError::Refused(format!(
                "cluster.conf exists at {} (table version {}) — nothing to restore",
                main.uri(),
                c.version()
            )));
        }
        let history = cluster_history::versions(store, prefix).await?;
        let Some(&v) = history.last() else {
            return Err(CtlError::Refused(format!(
                "no range table history under {}/{} — the table cannot be rebuilt from this \
                 bucket; restore cluster.conf from bucket versioning (see \
                 docs/runbooks/cluster-conf-lost.md)",
                main.uri(),
                cluster_history::DIR
            )));
        };
        let raw = cluster_history::read(store, prefix, v)
            .await?
            .ok_or_else(|| {
                CtlError::Refused(format!(
                    "{}/{v} disappeared while reading — run again",
                    cluster_history::DIR
                ))
            })?;
        let t = cairn_store::decode_table(&raw).map_err(|e| {
            CtlError::Refused(format!(
                "{}/{v} cannot be decoded: {e}",
                cluster_history::DIR
            ))
        })?;
        if t.version() != v {
            return Err(CtlError::Refused(format!(
                "{}/{v} holds table version {} — the history object is not ours",
                cluster_history::DIR,
                t.version()
            )));
        }
        let next = t.next_range_id();
        let promised: BTreeSet<u32> = t.promised().iter().map(|r| r.0).collect();
        let mut wrong = Vec::new();
        let mut checked = Vec::new();

        // (1) id ≥ next_range_id, and (2), (3) — owner records.
        let owner_ids = inspect::owner_ranges(store, prefix).await?;
        let mut live_owner = BTreeSet::new();
        let mut tombed = BTreeSet::new();
        for r in &owner_ids {
            if r.0 >= next {
                wrong.push(format!(
                    "owners/range-{} exists but table version {v} has next_range_id {next} — \
                     the table changed after version {v}",
                    r.0
                ));
            }
            match inspect::owner_observed(store, prefix, *r).await {
                Ok(Some(o)) if o.record.retired => {
                    tombed.insert(r.0);
                }
                Ok(Some(_)) => {
                    live_owner.insert(r.0);
                }
                Ok(None) => {}
                Err(e) => wrong.push(format!("owners/range-{} is unreadable: {e}", r.0)),
            }
        }
        checked.push(format!(
            "{} owner records: none with id >= next_range_id {next}",
            owner_ids.len()
        ));
        for r in t.ranges() {
            let id = r.id.0;
            if tombed.contains(&id) {
                wrong.push(format!(
                    "range {id} is live in table version {v} but its owner record is a \
                     tombstone — a later split/merge retired it"
                ));
            } else if !live_owner.contains(&id) {
                wrong.push(format!(
                    "range {id} is live in table version {v} but has no owner record — it may \
                     have been retired and reclaimed after version {v}"
                ));
            }
        }
        checked.push(format!(
            "{} live ranges of version {v}: each has an owner record without a tombstone",
            t.ranges().len()
        ));
        for id in &tombed {
            if promised.contains(id) {
                wrong.push(format!(
                    "range {id} is promised by an intent of version {v} but its owner record is a \
                     tombstone — it was committed and retired after version {v}"
                ));
            }
        }
        checked.push(format!("{} tombstones: none in version {v}", tombed.len()));

        // (1) — Db directories.
        let (dirs, skips) = self.stores.range_dirs_scan().await?;
        for s in &skips {
            wrong.push(format!("{} is unreadable: {}", s.item, s.error));
        }
        for d in &dirs {
            if d.0 >= next {
                wrong.push(format!(
                    "ranges/{} exists but table version {v} has next_range_id {next} — the table \
                     changed after version {v}",
                    d.0
                ));
            }
        }
        checked.push(format!(
            "{} ranges/ directories: none with id >= next_range_id {next}",
            dirs.len()
        ));

        // (1), (4) — the operation log.
        let ops = ledger::list_ops(store, prefix).await?;
        for (key, _) in &ops {
            let rec = match ledger::read_op(store, prefix, *key).await {
                Ok(Some(o)) => o.record,
                Ok(None) => continue,
                Err(e) => {
                    wrong.push(format!("ops/{key} is unreadable: {e}"));
                    continue;
                }
            };
            let op = rec.op;
            if op.children().iter().any(|c| c >= next) {
                wrong.push(format!(
                    "ops/{key} records an operation whose ranges are >= next_range_id {next} of \
                     version {v} — the table changed after version {v}"
                ));
                continue;
            }
            match rec.phase {
                cairn_split::Phase::Committed
                    if op.sources().iter().any(|s| t.contains(PartitionId(s))) =>
                {
                    wrong.push(format!(
                        "ops/{key} is committed but its source ranges are still live in version \
                         {v} — the commit came after version {v}"
                    ));
                }
                cairn_split::Phase::Cleaned
                    if op
                        .sources()
                        .iter()
                        .any(|s| ledger::carries(&t, PartitionId(s), op)) =>
                {
                    wrong.push(format!(
                        "ops/{key} is cleaned up but version {v} still carries its intent — the \
                         roll-back came after version {v}"
                    ));
                }
                _ => {}
            }
        }
        checked.push(format!(
            "{} split/merge ledgers: consistent with version {v}",
            ops.len()
        ));

        if !wrong.is_empty() {
            return Err(CtlError::Refused(format!(
                "cannot prove that {}/{v} is the last range table — not restoring:\n  {}\n\
                 The history may lag the lost table (a crash between a table change and its \
                 history write). Restore cluster.conf from bucket versioning instead (see \
                 docs/runbooks/cluster-conf-lost.md)",
                cluster_history::DIR,
                wrong.join("\n  ")
            )));
        }

        // A prepared op whose intent is still in V — resolved in place.
        let mut table = t.clone();
        let mut rolled_forward = Vec::new();
        let mut seen = BTreeSet::new();
        for r in t.ranges() {
            let Some(intent) = r.intent else {
                continue;
            };
            let op = crate::split::core::op_of(r.id, intent);
            if !seen.insert(op.key()) {
                continue;
            }
            let key = op.key();
            let Some(o) = ledger::read_op(store, prefix, key).await? else {
                continue;
            };
            if !o.record.phase.may_have_committed() {
                continue;
            }
            if crate::split::forward::prepared(&self.stores, op, &o.record).await? {
                table = crate::split::forward::committed_table(&table, r.id, intent)?
                    .with_extra(t.extra());
                rolled_forward.push(key);
                checked.push(format!(
                    "ops/{key} is prepared and every child has a Db and an owner record, every \
                     source is sealed — its commit may have won after version {v}: restoring the \
                     committed table (roll forward), not the intent"
                ));
            } else {
                checked.push(format!(
                    "ops/{key} is prepared but a child or a source seal is missing — its commit \
                     cannot have won: keeping the intent of version {v}"
                ));
            }
        }
        let body: bytes::Bytes = if rolled_forward.is_empty() {
            raw
        } else {
            cairn_store::encode_table(&table).into()
        };

        let mut warnings = Vec::new();
        let unfinished: Vec<String> = self
            .sessions(opts.now_ms)
            .await?
            .into_iter()
            .filter(|s| s.state != "expired" && s.state != "closed")
            .map(|s| format!("{} ({}, idle {} ms)", s.session, s.state, s.idle_ms))
            .collect();
        if !unfinished.is_empty() {
            let line = format!(
                "sessions not ended: {} — every node must be stopped before --apply (a node \
                 killed with kill -9 leaves its session live; add --force once you are sure)",
                unfinished.join(", ")
            );
            if opts.apply && !opts.force {
                return Err(CtlError::Refused(line));
            }
            warnings.push(if opts.apply {
                format!("--force: {line}")
            } else {
                line
            });
        }

        let mut rep = TableRestoreReport {
            applied: false,
            version: v,
            next_range_id: next,
            history,
            table: table.encode(),
            rolled_forward,
            checked,
            warnings,
            audit: None,
        };
        if !opts.apply {
            return Ok(rep);
        }
        match put_resolving(store, &config_path(prefix), body.clone(), PutMode::Create).await {
            Ok(Cas::Written(_)) => {}
            Ok(Cas::Lost) => {
                return Err(CtlError::Refused(
                    "cluster.conf appeared while restoring — someone else wrote it; run `table \
                     show` and verify before doing anything else"
                        .into(),
                ))
            }
            // I/O error: whether it was written is unknown (ambiguous outcome) — the next
            // run sees whether `cluster.conf` exists.
            Err(e) => {
                return Err(CtlError::Store(cairn_store::StoreError::Io(format!(
                    "writing cluster.conf failed (it may or may not have been written — run \
                     `table show`): {e}"
                ))))
            }
        }
        rep.applied = true;
        if !rep.rolled_forward.is_empty() {
            // History of the committed table, and the log `Committed` (like the forward
            // runner): a failure is only a warning — the written table is the arbiter.
            if let Err(e) = cluster_history::write(store, prefix, table.version(), body).await {
                rep.warnings.push(format!(
                    "cannot write {}/{}: {e}",
                    cluster_history::DIR,
                    table.version()
                ));
            }
            for &key in &rep.rolled_forward {
                let Some(o) = ledger::read_op(store, prefix, key).await.ok().flatten() else {
                    continue;
                };
                if let Err(e) = crate::split::mark_ledger(
                    &self.stores,
                    o.record.op,
                    cairn_split::Phase::Committed,
                )
                .await
                {
                    rep.warnings.push(format!(
                        "table restored as committed but ops/{key} could not be marked committed: \
                         {e}"
                    ));
                }
            }
        }
        let audit = RepairReport {
            applied: true,
            object: "cluster.conf".into(),
            before: String::new(),
            after: rep.table.clone(),
            cleanup: Vec::new(),
            warnings: rep.warnings.clone(),
            audit: None,
        };
        rep.audit = Some(self.audit(opts, &audit).await?);
        Ok(rep)
    }
}
