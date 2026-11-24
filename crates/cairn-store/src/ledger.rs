// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **durable op ledger** of split/merge: one object per op, `<prefix>/ops/<key>` — `key`
//! is the first child (`cairn_split::Op::key`). Child ids are allocated by the winning intent
//! CAS itself and never reused (D22), so each op has its own key and `cluster.conf` does not
//! change at all.
//!
//! ```text
//! cairnflow-op/1
//! op=split:<source>:<left>:<right> | merge:<low>:<high>:<into>
//! phase=intent|closed|cloned|prepared|committed|aborting|cleaned
//! coordinator=<the coordinator's session>
//! sealed=<source>:<writer_epoch>,…      (from `closed`: epoch of the writer just quiesced)
//! by=<the writer of this version>#<nonce>
//! ```
//!
//! **Each phase is written before its side effects** (table in `cairn_split::Phase`), and
//! the phase only changes by the transition rule `cairn_split::Phase::advance`, by CAS on
//! the version just read (`cas::put_resolving`, rule 7): a coordinator that sees the ledger
//! has moved to `Aborting` cannot write `Prepared`; a roll-back that sees `Committed` does
//! not overwrite it. The `by` line carries a nonce on each write, so the content never
//! coincides between two writers — `put_resolving`'s comparison is meaningful.
//!
//! **Lenient read, absence means intent-only behaviour** (rule 2): ops of binaries that
//! predate the ledger have no ledger; readers treat them as they do today. Unknown keys are
//! copied verbatim when writing again. An unknown header or phase ⇒ a read error: the
//! caller **keeps** (rule 5) or rolls back (always safe — CAS), never treats it as
//! `Cleaned`.
//!
//! **The ledger of a settled op is cleaned up**: the D-d loop moves it into the trash
//! through the gate [`crate::proof::DeleteProof::settled_op`] ([`delete_op`]). A missing
//! ledger after that is exactly the "no ledger" behaviour above.

use {
    crate::{
        cas::{put_resolving, Cas},
        error::{Result, StoreError},
        ids::PartitionId,
        partition_table::{PartitionTable, RangeIntent},
    },
    cairn_split::{Advance, Op, Phase, Standing},
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::sync::Arc,
};

pub const OP_HEADER: &str = "cairnflow-op/1";

/// Number of re-reads when the ledger's CAS loses (someone else just changed the phase).
const ADVANCE_TRIES: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpRecord {
    pub op: Op,
    pub phase: Phase,
    /// The coordinator's session (`<node>/<inc>`).
    pub coordinator: String,
    /// `writer_epoch` of the writer just quiesced on each source — the close mark is
    /// conditional on this epoch, so the source is still closed by **exactly** that close
    /// when its epoch is `+1`.
    pub sealed: Vec<(u32, u64)>,
    /// The writer of this version, with a nonce.
    pub by: String,
    /// Unknown lines from a newer binary.
    pub extra: Vec<String>,
}

fn phase_name(p: Phase) -> &'static str {
    match p {
        Phase::Intent => "intent",
        Phase::Closed => "closed",
        Phase::Cloned => "cloned",
        Phase::Prepared => "prepared",
        Phase::Committed => "committed",
        Phase::Aborting => "aborting",
        Phase::Cleaned => "cleaned",
    }
}

fn parse_phase(s: &str) -> Option<Phase> {
    Some(match s {
        "intent" => Phase::Intent,
        "closed" => Phase::Closed,
        "cloned" => Phase::Cloned,
        "prepared" => Phase::Prepared,
        "committed" => Phase::Committed,
        "aborting" => Phase::Aborting,
        "cleaned" => Phase::Cleaned,
        _ => return None,
    })
}

fn op_text(op: &Op) -> String {
    match *op {
        Op::Split {
            source,
            left,
            right,
        } => format!("split:{source}:{left}:{right}"),
        Op::Merge { lo, hi, into } => format!("merge:{lo}:{hi}:{into}"),
    }
}

fn parse_op(s: &str) -> Option<Op> {
    let mut it = s.split(':');
    let kind = it.next()?;
    let mut n = || it.next()?.parse::<u32>().ok();
    let (a, b, c) = (n()?, n()?, n()?);
    match kind {
        "split" => Some(Op::Split {
            source: a,
            left: b,
            right: c,
        }),
        "merge" => Some(Op::Merge {
            lo: a,
            hi: b,
            into: c,
        }),
        _ => None,
    }
}

impl OpRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut s = format!(
            "{OP_HEADER}\nop={}\nphase={}\ncoordinator={}\n",
            op_text(&self.op),
            phase_name(self.phase),
            self.coordinator
        );
        if !self.sealed.is_empty() {
            let v: Vec<String> = self
                .sealed
                .iter()
                .map(|(r, e)| format!("{r}:{e}"))
                .collect();
            s.push_str(&format!("sealed={}\n", v.join(",")));
        }
        s.push_str(&format!("by={}\n", self.by));
        for l in &self.extra {
            s.push_str(l);
            s.push('\n');
        }
        s.into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<OpRecord> {
        let bad = |why: &str| StoreError::Io(format!("broken op ledger: {why}"));
        let text = std::str::from_utf8(raw).map_err(|_| bad("not UTF-8"))?;
        let mut lines = text.lines();
        if lines.next() != Some(OP_HEADER) {
            return Err(bad("unknown header"));
        }
        let (mut op, mut phase, mut coordinator, mut by) = (None, None, None, None);
        let mut sealed = Vec::new();
        let mut extra = Vec::new();
        for l in lines.filter(|l| !l.is_empty()) {
            match l.split_once('=') {
                Some(("op", v)) => op = Some(parse_op(v).ok_or_else(|| bad("op"))?),
                Some(("phase", v)) => phase = Some(parse_phase(v).ok_or_else(|| bad("phase"))?),
                Some(("coordinator", v)) => coordinator = Some(v.to_string()),
                Some(("by", v)) => by = Some(v.to_string()),
                Some(("sealed", v)) => {
                    for item in v.split(',').filter(|i| !i.is_empty()) {
                        let (r, e) = item.split_once(':').ok_or_else(|| bad("sealed"))?;
                        sealed.push((
                            r.parse().map_err(|_| bad("sealed"))?,
                            e.parse().map_err(|_| bad("sealed"))?,
                        ));
                    }
                }
                _ => extra.push(l.to_string()),
            }
        }
        Ok(OpRecord {
            op: op.ok_or_else(|| bad("missing op"))?,
            phase: phase.ok_or_else(|| bad("missing phase"))?,
            coordinator: coordinator.unwrap_or_default(),
            sealed,
            by: by.unwrap_or_default(),
            extra,
        })
    }
}

/// `<prefix>/ops/<key>`.
pub fn op_path(prefix: &Path, key: u32) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(format!("ops/{key}"))
    } else {
        Path::from(format!("{}/ops/{key}", prefix.as_ref()))
    }
}

/// One read of the ledger, with the version for the next CAS.
#[derive(Clone, Debug)]
pub struct ObservedOp {
    pub record: OpRecord,
    pub version: UpdateVersion,
}

/// Reads the ledger `ops/<key>`. `Ok(None)` = no ledger (an op of a binary that predates the
/// ledger, or the intent CAS has not won yet).
pub async fn read_op(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    key: u32,
) -> Result<Option<ObservedOp>> {
    let got = match store.get(&op_path(prefix, key)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::Io(e.to_string())),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got
        .bytes()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?;
    Ok(Some(ObservedOp {
        record: OpRecord::decode(&raw)?,
        version,
    }))
}

/// The phase in the ledger of the op that created child `c` (tries `ops/c` then `ops/c-1`,
/// keeping the one with `c` in `children()`). `Ok(None)` = no ledger.
pub async fn read_op_of_child(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    c: u32,
) -> Result<Option<ObservedOp>> {
    for key in Op::keys_of_child(c).iter() {
        if let Some(o) = read_op(store, prefix, key).await? {
            if o.record.op.children().contains(c) {
                return Ok(Some(o));
            }
        }
    }
    Ok(None)
}

/// Like [`read_op`], plus the object's `last_modified` (ms) — the age of the last write,
/// i.e. when the ledger reached its current phase (the ledger cleanup loop).
pub async fn read_op_aged(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    key: u32,
) -> Result<Option<(OpRecord, u64)>> {
    let got = match store.get(&op_path(prefix, key)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(StoreError::Io(e.to_string())),
    };
    let modified = got.meta.last_modified.timestamp_millis().max(0) as u64;
    let raw = got
        .bytes()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?;
    Ok(Some((OpRecord::decode(&raw)?, modified)))
}

/// When the ledger of the op that created child range `c` was last written (store clock,
/// ms) — at most one GET per candidate key. `None` = no ledger lists `c` (a range of the
/// initial table, or its ledger already reaped by the ledger cleanup loop).
pub async fn op_of_child_written_ms(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    c: u32,
) -> Result<Option<u64>> {
    for key in Op::keys_of_child(c).iter() {
        if let Some((record, ms)) = read_op_aged(store, prefix, key).await? {
            if record.op.children().contains(c) {
                return Ok(Some(ms));
            }
        }
    }
    Ok(None)
}

/// Every ledger under `ops/`: `(key, last_modified ms)` — one LIST. Non-numeric keys are
/// skipped (not a ledger this binary wrote).
pub async fn list_ops(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<(u32, u64)>> {
    let dir = if prefix.as_ref().is_empty() {
        Path::from("ops")
    } else {
        Path::from(format!("{}/ops", prefix.as_ref()))
    };
    let listed: Vec<_> = store
        .list(Some(&dir))
        .try_collect()
        .await
        .map_err(|e| StoreError::Io(e.to_string()))?;
    let mut out: Vec<(u32, u64)> = listed
        .into_iter()
        .filter_map(|m| {
            let key = m.location.filename()?.parse().ok()?;
            (op_path(prefix, key) == m.location)
                .then(|| (key, m.last_modified.timestamp_millis().max(0) as u64))
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// **Moves** the ledger `ops/<key>` of a settled op into the trash (SM-16). Gate: `proof`
/// must be [`crate::proof::DeleteProof::settled_op`] for exactly this key. A ledger already
/// absent ⇒ `Ok` (idempotent).
pub async fn delete_op(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    proof: crate::proof::DeleteProof,
) -> Result<()> {
    let key = match proof.subject() {
        crate::trash::Subject::Op(k) => *k,
        other => {
            return Err(StoreError::Io(format!(
                "delete_op needs a proof for an op ledger, got {other}"
            )))
        }
    };
    proof.check(&crate::trash::Subject::Op(key), None).await?;
    crate::trash::Trash::new(Arc::clone(store), prefix)
        .move_into(
            &crate::trash::EntryId::new(crate::entropy::now_ms()),
            &crate::trash::Subject::Op(key),
            "op ledger of a settled op",
            &[op_path(prefix, key)],
        )
        .await
        .map(|_| ())
}

/// The core's op for the `intent` carried on range `p`.
pub fn op_of(p: PartitionId, intent: RangeIntent) -> Op {
    match intent {
        RangeIntent::Splitting { left, right, .. } => Op::Split {
            source: p.0,
            left: left.0,
            right: right.0,
        },
        RangeIntent::Merging { with, into } => Op::Merge {
            lo: p.0.min(with.0),
            hi: p.0.max(with.0),
            into: into.0,
        },
    }
}

/// How the op stands on `t`.
pub fn standing(t: &PartitionTable, op: Op) -> Standing {
    if op.children().iter().any(|c| t.contains(PartitionId(c))) {
        return Standing::Committed;
    }
    let sources: Vec<_> = op.sources().iter().map(PartitionId).collect();
    if !sources.iter().all(|s| t.contains(*s)) {
        return Standing::Elsewhere;
    }
    if sources.iter().all(|s| carries(t, *s, op)) {
        Standing::Pending
    } else {
        Standing::Free {
            other: sources
                .iter()
                .any(|s| t.range(*s).is_some_and(|r| r.intent.is_some()) && !carries(t, *s, op)),
        }
    }
}

/// Whether range `s` of `t` carries the intent of exactly `op`.
pub fn carries(t: &PartitionTable, s: PartitionId, op: Op) -> bool {
    t.range(s)
        .and_then(|r| r.intent)
        .is_some_and(|i| op_of(s, i) == op)
}

/// The writer's identity with a fresh nonce.
pub fn writer_tag(who: &str) -> String {
    let mut n = [0u8; 8];
    crate::entropy::fill(&mut n);
    format!("{who}#{:016x}", u64::from_le_bytes(n))
}

/// **Creates** the ledger in phase `Intent` — right after the intent CAS wins. An ambiguous
/// create is resolved by reading back; a ledger that already exists and is not ours
/// (impossible with a freshly allocated id, unless the bucket is broken) ⇒ error.
pub async fn create_op(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    op: Op,
    coordinator: &str,
) -> Result<ObservedOp> {
    let record = OpRecord {
        op,
        phase: Phase::Intent,
        coordinator: coordinator.to_string(),
        sealed: Vec::new(),
        by: writer_tag(coordinator),
        extra: Vec::new(),
    };
    match put_resolving(
        store,
        &op_path(prefix, op.key()),
        record.encode().into(),
        PutMode::Create,
    )
    .await
    {
        Ok(Cas::Written(version)) => Ok(ObservedOp { record, version }),
        Ok(Cas::Lost) => Err(StoreError::Io(format!(
            "op ledger ops/{} already exists — child id not fresh?",
            op.key()
        ))),
        Err(e) => Err(StoreError::Io(e.to_string())),
    }
}

/// Moves the op's ledger to `to` by the `Phase::advance` rule, editing the record with
/// `edit` (e.g. writing `sealed`). `seen` = the latest read the caller holds (the
/// coordinator keeps the copy of its previous write, saving a GET); `None` ⇒ read. A lost
/// CAS ⇒ re-read and reconsider, at most [`ADVANCE_TRIES`] times.
///
/// Returns `Write` (written; the new copy is in `seen`), `Already` (the ledger is already in
/// that phase) or `Refused` (the ledger went down another branch, or there is no ledger and
/// `to` is not `Intent`).
pub async fn advance_op(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    key: u32,
    to: Phase,
    who: &str,
    seen: &mut Option<ObservedOp>,
    edit: impl Fn(&mut OpRecord),
) -> Result<Advance> {
    for _ in 0..ADVANCE_TRIES {
        if seen.is_none() {
            *seen = read_op(store, prefix, key).await?;
        }
        let Some(cur) = seen.clone() else {
            return Ok(Advance::Refused);
        };
        match Phase::advance(Some(cur.record.phase), to) {
            Advance::Write => {}
            other => return Ok(other),
        }
        let mut next = OpRecord {
            phase: to,
            by: writer_tag(who),
            ..cur.record.clone()
        };
        edit(&mut next);
        match put_resolving(
            store,
            &op_path(prefix, key),
            next.encode().into(),
            PutMode::Update(cur.version.clone()),
        )
        .await
        {
            Ok(Cas::Written(version)) => {
                *seen = Some(ObservedOp {
                    record: next,
                    version,
                });
                return Ok(Advance::Write);
            }
            // Someone else just changed the ledger: re-read, reconsider.
            Ok(Cas::Lost) => *seen = None,
            Err(e) => return Err(StoreError::Io(e.to_string())),
        }
    }
    Err(StoreError::Io(format!(
        "op ledger ops/{key} keeps changing — could not write {to:?}"
    )))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Ambiguity, FaultyStore, Rule},
        slatedb::object_store::memory::InMemory,
    };

    const OP: Op = Op::Split {
        source: 4,
        left: 9,
        right: 10,
    };

    fn root() -> Path {
        Path::from("t")
    }

    #[test]
    fn record_roundtrips_and_keeps_unknown_lines() {
        let r = OpRecord {
            op: Op::Merge {
                lo: 1,
                hi: 2,
                into: 7,
            },
            phase: Phase::Closed,
            coordinator: "node-a/3".into(),
            sealed: vec![(1, 5), (2, 9)],
            by: "node-a/3#00ff".into(),
            extra: vec!["future=1".into()],
        };
        assert_eq!(OpRecord::decode(&r.encode()).unwrap(), r);
        assert!(OpRecord::decode(b"cairnflow-op/9\nop=split:1:2:3\nphase=intent\n").is_err());
        assert!(
            OpRecord::decode(b"cairnflow-op/1\nop=split:1:2:3\nphase=someday\n").is_err(),
            "an unknown phase never becomes a known phase"
        );
    }

    #[tokio::test]
    async fn phases_follow_the_transition_rule() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let created = create_op(&store, &root(), OP, "a/1").await.unwrap();
        let mut seen = Some(created);
        for p in [Phase::Closed, Phase::Cloned] {
            let got = advance_op(&store, &root(), 9, p, "a/1", &mut seen, |_| {})
                .await
                .unwrap();
            assert_eq!(got, Advance::Write, "{p:?}");
        }
        // A roll-back (with its own read) moves to Aborting.
        let mut theirs = None;
        assert_eq!(
            advance_op(
                &store,
                &root(),
                9,
                Phase::Aborting,
                "x/1",
                &mut theirs,
                |_| {}
            )
            .await
            .unwrap(),
            Advance::Write
        );
        // The coordinator holds the old copy: the CAS loses, it re-reads, sees Aborting ⇒
        // Refused.
        assert_eq!(
            advance_op(
                &store,
                &root(),
                9,
                Phase::Prepared,
                "a/1",
                &mut seen,
                |_| {}
            )
            .await
            .unwrap(),
            Advance::Refused
        );
        assert_eq!(
            advance_op(
                &store,
                &root(),
                9,
                Phase::Aborting,
                "y/1",
                &mut None,
                |_| {}
            )
            .await
            .unwrap(),
            Advance::Already
        );
        let r = read_op_of_child(&store, &root(), 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.record.phase, Phase::Aborting);
        assert_eq!(r.record.op, OP);
        // No ledger: only creating at Intent.
        assert_eq!(
            advance_op(
                &store,
                &root(),
                40,
                Phase::Aborting,
                "y/1",
                &mut None,
                |_| {}
            )
            .await
            .unwrap(),
            Advance::Refused
        );
        assert!(read_op_of_child(&store, &root(), 41)
            .await
            .unwrap()
            .is_none());
    }

    /// Rule 7: an ambiguous create and an ambiguous phase change (object_store retrying a 5xx
    /// on its own, or an error after the write) are resolved by reading back — the ledger is
    /// ours, not "already exists" or "lost".
    #[tokio::test]
    async fn ambiguous_writes_resolve_by_reading_back() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            faulty.add_rule(Rule::ambiguous_put("ops/9", reply));
            let created = create_op(&store, &root(), OP, "a/1")
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: ambiguous ledger create: {e}"));
            let mut seen = Some(created);
            faulty.add_rule(Rule::ambiguous_put("ops/9", reply));
            let got = advance_op(&store, &root(), 9, Phase::Closed, "a/1", &mut seen, |r| {
                r.sealed = vec![(4, 2)]
            })
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: ambiguous phase change: {e}"));
            assert_eq!(got, Advance::Write, "{reply:?}");
            let r = read_op(&store, &root(), 9).await.unwrap().unwrap();
            assert_eq!(
                (r.record.phase, r.record.sealed.clone()),
                (Phase::Closed, vec![(4, 2)])
            );
        }
    }
}
