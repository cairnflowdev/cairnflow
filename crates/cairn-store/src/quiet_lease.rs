// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **quiet renewal** gate: the object `control/quiet-lease`.
//!
//! Solo mode (`mode=solo`) and delegation change the observers' rule: records in those
//! modes are **not** expired by the silence rule LS-1. An old binary that does not know the
//! new rule would expire them while their owner still holds a grant, so no node may enter
//! those modes before every binary in the cluster knows the gate step
//! [`crate::migrate::QUIET_LEASE`]. The order of enabling:
//!
//! 1. every node runs a binary with `knows ≥ K`;
//! 2. `cairn-ctl cluster enable-quiet-lease`: tests the store itself (a conditional GET
//!    never returns a stale `304` — [`crate::store_probe`]), checks that every unfinished
//!    session has `knows ≥ K`, sets `min_binary = K` in `cluster-version` (old binaries can
//!    no longer start), checks again, then writes this object with `state=on`;
//! 3. nodes read the gate on their own tick ([`Watch`]) and only from then on enter the
//!    quiet modes.
//!
//! Downgrade: `disable-quiet-lease` (writes `state=off`), wait for every session to go back
//! to `direct` (`cairn-ctl cluster status`), then `lower-min-binary`.
//!
//! # Object
//!
//! ```text
//! cairnflow-quiet-lease/1
//! state=on                 on | off
//! knows=<K>                the gate step the enabling command checked
//! by=<operator> at_ms=<ts>
//! delegation=on            (only when on) delegated renewal
//! evidence=v1 <item>…      (optional) evidence that delegation's gate is done (items (a)–(e))
//! delegation_unproven=<at_ms>,<by>,<missing items>
//!                          (optional) delegation enabled with the override flag while
//!                          evidence was missing
//! ```
//!
//! **Evidence** ([`Evidence`]): the `evidence=` line starts with `v1`, then one word per
//! item `<k>=<at_ms>,<by>,<note>` with `k` ∈ `a`…`e` ([`EVIDENCE_ITEMS`]); `by` and the note
//! escape `%`, `,`, whitespace and control characters as `%XX` (non-ASCII letters stay as
//! they are). Read leniently: a damaged or unknown item is skipped (counts as missing)
//! without breaking the object; a line not starting with `v1` (free text from before this
//! form, or a newer form) reads as **no items** — missing is the safe path: `cairn-ctl
//! cluster enable-quiet-lease --delegation` refuses. A binary from before this form reads
//! the line verbatim and writes it back verbatim.
//!
//! The `delegation_unproven=` line deliberately has no whitespace: a binary from before it
//! splits every unknown line on whitespace to look for `by=`/`at_ms=`, so a `by=` word in
//! that line would overwrite the gate's enabler when an old binary reads it.
//!
//! **Delegation is enabled separately**: `state=on` alone only enables solo mode; the
//! coordinator only delegates when there is also `delegation=on` (`cairn-ctl cluster
//! enable-quiet-lease --delegation`). The line absent ≡ off — read leniently like every
//! other line, and an object written by a binary from before this line has delegation off.
//!
//! **Read leniently, absence ≡ off** (rule 2 of AGENTS.md). Anything that is not exactly
//! this header with `state=on` — absent, damaged, an unknown or newer header — reads as off:
//! off is the safe path (today's protocol). Written with
//! [`crate::cas::put_resolving_or_overwrite`] (rule 7; a `--data-dir` without CAS is
//! overwritten — one process): the only writer is `cairn-ctl`, and two runs writing the same
//! state have the same outcome.

use {
    crate::error::{Result, StoreError},
    slatedb::object_store::{
        path::Path, Error as OsError, GetOptions, ObjectStore, ObjectStoreExt, PutMode,
        UpdateVersion,
    },
    std::sync::{Arc, Mutex},
};

/// The header this binary writes.
pub const HEADER: &str = "cairnflow-quiet-lease/1";

/// Path under the cluster prefix.
pub const OBJECT: &str = "control/quiet-lease";

pub fn path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(OBJECT)
    } else {
        Path::from(format!("{}/{OBJECT}", prefix.as_ref()))
    }
}

/// Content of the gate.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuietLease {
    pub on: bool,
    /// The gate step the enabling command checked (`knows=`).
    pub knows: u32,
    pub by: String,
    pub at_ms: u64,
    /// Delegated renewal enabled (`delegation=on`) — only meaningful when `on`.
    pub delegation: bool,
    /// Evidence that delegation's gate is done (`evidence=`, items (a)–(e)), one line.
    pub evidence: Evidence,
    /// Delegation was enabled with the override flag `--accept-missing-evidence` while
    /// these items were missing (`delegation_unproven=`) — a mark for readers of
    /// `cluster status`.
    pub delegation_unproven: Option<Unproven>,
}

/// The five evidence items of delegation's gate.
pub const EVIDENCE_ITEMS: [char; 5] = ['a', 'b', 'c', 'd', 'e'];

/// Short title of item `k`, for `cairn-ctl`'s printed line.
pub fn evidence_title(k: char) -> &'static str {
    match k {
        'a' => "wider model (M1: split/merge with the delegation bugs, two nodes, two terms)",
        'b' => "tripwire in release builds (cairnflow_lease_delegated_revoked_while_valid_total)",
        'c' => "chaos ≥ 30 minutes with --kill-coordinator-every-s and delegation on",
        'd' => "clean 24-hour soak",
        'e' => "store contract suite green on the target store",
        _ => "unknown item",
    }
}

/// One evidence item: who recorded it, when, and a note (commit, command or log, …).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceEntry {
    pub at_ms: u64,
    pub by: String,
    pub note: String,
}

/// The evidence items recorded (the `evidence=` line). See the module header.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Evidence {
    entries: std::collections::BTreeMap<char, EvidenceEntry>,
}

/// Mark of this form of the `evidence=` line.
const EVIDENCE_V1: &str = "v1";

impl Evidence {
    /// Reads the value of the `evidence=` line leniently.
    pub fn parse(value: &str) -> Evidence {
        let mut out = Evidence::default();
        let mut words = value.split(' ').filter(|w| !w.is_empty());
        if words.next() != Some(EVIDENCE_V1) {
            return out;
        }
        for w in words {
            let Some((k, v)) = w.split_once('=') else {
                continue;
            };
            let mut ks = k.chars();
            let (Some(k), None) = (ks.next(), ks.next()) else {
                continue;
            };
            if !EVIDENCE_ITEMS.contains(&k) {
                continue;
            }
            let mut parts = v.splitn(3, ',');
            let (Some(at), Some(by), Some(note)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let (Ok(at_ms), Some(by), Some(note)) = (at.parse(), unescape(by), unescape(note))
            else {
                continue;
            };
            if note.trim().is_empty() {
                continue;
            }
            out.entries.insert(k, EvidenceEntry { at_ms, by, note });
        }
        out
    }

    /// Value of the `evidence=` line; `None` when there is no item yet (no line written).
    pub fn line(&self) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        let mut out = String::from(EVIDENCE_V1);
        for (k, e) in &self.entries {
            out.push_str(&format!(
                " {k}={},{},{}",
                e.at_ms,
                escape(&e.by),
                escape(&e.note)
            ));
        }
        Some(out)
    }

    pub fn get(&self, k: char) -> Option<&EvidenceEntry> {
        self.entries.get(&k)
    }

    /// Records (or overwrites) item `k`. A `k` outside [`EVIDENCE_ITEMS`] is ignored.
    pub fn record(&mut self, k: char, entry: EvidenceEntry) {
        if EVIDENCE_ITEMS.contains(&k) {
            self.entries.insert(k, entry);
        }
    }

    /// Items not yet present, in `a`…`e` order.
    pub fn missing(&self) -> Vec<char> {
        EVIDENCE_ITEMS
            .into_iter()
            .filter(|k| !self.entries.contains_key(k))
            .collect()
    }

    /// All five items present.
    pub fn complete(&self) -> bool {
        self.missing().is_empty()
    }
}

/// The "delegation enabled while evidence was missing" mark (the `delegation_unproven=`
/// line).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unproven {
    pub at_ms: u64,
    pub by: String,
    /// Items missing at enabling time, in `a`…`e` order.
    pub missing: Vec<char>,
}

impl Unproven {
    fn line(&self) -> String {
        format!(
            "{},{},{}",
            self.at_ms,
            escape(&self.by),
            self.missing.iter().collect::<String>()
        )
    }

    /// Read leniently: damaged ⇒ `None`.
    fn parse(value: &str) -> Option<Unproven> {
        let mut parts = value.splitn(3, ',');
        let at_ms = parts.next()?.parse().ok()?;
        let by = unescape(parts.next()?)?;
        let missing: Vec<char> = parts.next()?.chars().collect();
        if missing.is_empty() || missing.iter().any(|k| !EVIDENCE_ITEMS.contains(k)) {
            return None;
        }
        Some(Unproven { at_ms, by, missing })
    }
}

/// Escapes `%`, `,`, whitespace and control characters as `%XX`.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '%' || c == ',' || c.is_whitespace() || c.is_control() {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The inverse of [`escape`]; a damaged `%` or bytes that are not UTF-8 ⇒ `None`.
fn unescape(s: &str) -> Option<String> {
    let raw = s.as_bytes();
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'%' {
            let hex = std::str::from_utf8(raw.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

impl QuietLease {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "{HEADER}\nstate={}\nknows={}\nby={} at_ms={}\n",
            if self.on { "on" } else { "off" },
            self.knows,
            self.by.replace(['\n', ' '], "_"),
            self.at_ms
        );
        if self.delegation {
            out.push_str("delegation=on\n");
        }
        if let Some(e) = self.evidence.line() {
            out.push_str(&format!("evidence={e}\n"));
        }
        if let Some(u) = &self.delegation_unproven {
            out.push_str(&format!("delegation_unproven={}\n", u.line()));
        }
        out.into_bytes()
    }

    /// `None` ⇔ unreadable (unknown header, not UTF-8, missing `state`).
    pub fn decode(raw: &[u8]) -> Option<QuietLease> {
        let text = std::str::from_utf8(raw).ok()?;
        let mut lines = text.lines();
        if lines.next()? != HEADER {
            return None;
        }
        let (mut on, mut knows, mut by, mut at_ms) = (None, 0, String::new(), 0);
        let (mut delegation, mut evidence, mut delegation_unproven) =
            (false, Evidence::default(), None);
        for line in lines {
            if let Some(v) = line.strip_prefix("evidence=") {
                evidence = Evidence::parse(v);
                continue;
            }
            if let Some(v) = line.strip_prefix("delegation_unproven=") {
                delegation_unproven = Unproven::parse(v);
                continue;
            }
            for kv in line.split(' ') {
                match kv.split_once('=') {
                    Some(("state", "on")) => on = Some(true),
                    Some(("state", _)) => on = Some(false),
                    Some(("knows", v)) => knows = v.parse().unwrap_or(0),
                    Some(("by", v)) => by = v.to_string(),
                    Some(("at_ms", v)) => at_ms = v.parse().unwrap_or(0),
                    Some(("delegation", v)) => delegation = v == "on",
                    _ => {}
                }
            }
        }
        Some(QuietLease {
            on: on?,
            knows,
            by,
            at_ms,
            delegation,
            evidence,
            delegation_unproven,
        })
    }
}

/// The gate of a **new bucket**: a node building an empty bucket
/// ([`crate::migrate::run_cluster_with`]) writes `control/quiet-lease` up front according
/// to this choice, at the same time as it creates `cluster-version`. A bucket that already
/// has a cluster is skipped — changing the gate after that is `cairn-ctl`'s job. Node flag
/// `--quiet-lease <off|solo|delegation>`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Bootstrap {
    /// No gate written: a new cluster in `direct`, as before this flag existed.
    Off,
    /// `state=on`, no `delegation=on`: solo mode only.
    Solo,
    /// `state=on` + `delegation=on`, with the `delegation_unproven=` mark (all five evidence
    /// items missing, `by=`[`BOOTSTRAP_BY`]) — the default.
    #[default]
    Delegation,
}

/// The writer of the `delegation_unproven=` mark when a new bucket enables delegation up
/// front.
pub const BOOTSTRAP_BY: &str = "bootstrap";

impl Bootstrap {
    pub fn parse(v: &str) -> Option<Bootstrap> {
        match v {
            "off" => Some(Bootstrap::Off),
            "solo" => Some(Bootstrap::Solo),
            "delegation" => Some(Bootstrap::Delegation),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Bootstrap::Off => "off",
            Bootstrap::Solo => "solo",
            Bootstrap::Delegation => "delegation",
        }
    }

    /// Content of the gate the builder of a new bucket writes; `None` for
    /// [`Bootstrap::Off`]. `writer` is the builder's session (`by=bootstrap@<writer>` —
    /// unique content for rule 7's comparison).
    pub fn gate(&self, writer: &str, at_ms: u64) -> Option<QuietLease> {
        let delegation = match self {
            Bootstrap::Off => return None,
            Bootstrap::Solo => false,
            Bootstrap::Delegation => true,
        };
        Some(QuietLease {
            on: true,
            knows: crate::migrate::QUIET_LEASE,
            by: format!("{BOOTSTRAP_BY}@{writer}"),
            at_ms,
            delegation,
            evidence: Evidence::default(),
            delegation_unproven: delegation.then(|| Unproven {
                at_ms,
                by: BOOTSTRAP_BY.into(),
                missing: EVIDENCE_ITEMS.to_vec(),
            }),
        })
    }

    /// The matching state of a gate that was read (absent, damaged or `state=off` ≡
    /// [`Bootstrap::Off`]) — to compare the node flag with an existing bucket.
    pub fn of(gate: Option<&QuietLease>) -> Bootstrap {
        match gate {
            Some(q) if q.on && q.delegation => Bootstrap::Delegation,
            Some(q) if q.on => Bootstrap::Solo,
            _ => Bootstrap::Off,
        }
    }
}

/// Raw bytes → whether the gate is on. Unreadable ≡ off.
pub fn is_on(raw: &[u8]) -> bool {
    QuietLease::decode(raw).is_some_and(|q| q.on)
}

/// Raw bytes → the gate is on **and** delegation is on. Unreadable ≡ off.
pub fn delegation_on(raw: &[u8]) -> bool {
    QuietLease::decode(raw).is_some_and(|q| q.on && q.delegation)
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Reads the gate with the object's version. `Ok(None)` = absent (≡ off); `Some((None, _))`
/// = an object exists but is unreadable (≡ off).
pub async fn read(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Option<(Option<QuietLease>, UpdateVersion)>> {
    let got = match store.get(&path(prefix)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got.bytes().await.map_err(io)?;
    Ok(Some((QuietLease::decode(&raw), version)))
}

/// Writes the gate by CAS on `expected` (`None` ⇒ create). `Ok(false)` = the CAS missed:
/// someone changed the object before us.
pub async fn write(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    q: &QuietLease,
    expected: Option<UpdateVersion>,
) -> Result<bool> {
    let mode = match expected {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    match crate::cas::put_resolving_or_overwrite(store, &path(prefix), q.encode().into(), mode)
        .await
    {
        Ok(crate::cas::Cas::Written(_)) => Ok(true),
        Ok(crate::cas::Cas::Lost) => Ok(false),
        Err(e) => Err(io(e)),
    }
}

/// The node's gate reader: a GET with `If-None-Match` and the previous etag — almost always
/// `304` (a Class B request). A read error keeps the old conclusion **only when** it is off:
/// an error never turns the gate on, and a gate that was on but fails to read counts as off
/// (the safe path).
#[derive(Default)]
pub struct Watch {
    /// Etag of the previous read, gate on, delegation on.
    last: Mutex<Option<(Option<String>, bool, bool)>>,
}

impl Watch {
    /// Whether the gate is on, by one conditional GET.
    pub async fn poll(&self, store: &Arc<dyn ObjectStore>, prefix: &Path) -> bool {
        self.poll_state(store, prefix).await.0
    }

    /// (gate on, delegation on) by one conditional GET.
    pub async fn poll_state(&self, store: &Arc<dyn ObjectStore>, prefix: &Path) -> (bool, bool) {
        let (etag, was, was_delegation) = self
            .last
            .lock()
            .expect("mutex")
            .clone()
            .unwrap_or((None, false, false));
        let opts = GetOptions {
            if_none_match: etag.clone(),
            ..Default::default()
        };
        let now = match store.get_opts(&path(prefix), opts).await {
            Err(OsError::NotModified { .. }) if etag.is_some() => return (was, was_delegation),
            Err(OsError::NotFound { .. }) => (None, false, false),
            Err(e) => {
                tracing::debug!(error = %e, "reading the quiet renewal gate failed — treating it as off");
                return (false, false);
            }
            Ok(got) => {
                let etag = got.meta.e_tag.clone();
                match got.bytes().await {
                    Ok(raw) => (etag, is_on(&raw), delegation_on(&raw)),
                    Err(_) => return (false, false),
                }
            }
        };
        let state = (now.1, now.2);
        *self.last.lock().expect("mutex") = Some(now);
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips_and_reads_leniently() {
        let q = QuietLease {
            on: true,
            knows: 3,
            by: "op@host".into(),
            at_ms: 7,
            ..Default::default()
        };
        assert_eq!(QuietLease::decode(&q.encode()), Some(q.clone()));
        assert!(is_on(&q.encode()));
        // Delegation is enabled separately: the line absent ≡ off; evidence round-trips.
        assert!(!delegation_on(&q.encode()));
        let mut evidence = Evidence::default();
        evidence.record('a', entry("m1 ok"));
        let d = QuietLease {
            delegation: true,
            evidence,
            delegation_unproven: Some(Unproven {
                at_ms: 9,
                by: "op@host".into(),
                missing: vec!['b', 'c', 'd', 'e'],
            }),
            ..q.clone()
        };
        assert_eq!(QuietLease::decode(&d.encode()), Some(d.clone()));
        assert!(delegation_on(&d.encode()));
        assert!(!delegation_on(
            &QuietLease {
                on: false,
                ..d.clone()
            }
            .encode()
        ));
        let off = QuietLease { on: false, ..q };
        assert!(!is_on(&off.encode()));
        // Read leniently: unknown lines are skipped; anything else than the right header +
        // `state=on` is off.
        assert!(is_on(b"cairnflow-quiet-lease/1\nstate=on\nfuture=1\n"));
        for raw in [
            &b""[..],
            b"cairnflow-quiet-lease/2\nstate=on\n",
            b"cairnflow-quiet-lease/1\n",
            b"cairnflow-quiet-lease/1\nstate=yes\n",
            b"\xff\xfe",
        ] {
            assert!(!is_on(raw), "{raw:?}");
        }
    }

    fn entry(note: &str) -> EvidenceEntry {
        EvidenceEntry {
            at_ms: 1_800_000_000_000,
            by: "op@host".into(),
            note: note.into(),
        }
    }

    /// The five evidence items: missing ones are counted in `a`…`e` order; a note with
    /// spaces, commas, `%`, newlines and non-ASCII letters round-trips through one
    /// `evidence=` line; the gate's `by=`/`at_ms=` readers are not overwritten.
    #[test]
    fn evidence_records_items_and_round_trips_on_one_line() {
        let mut ev = Evidence::default();
        assert_eq!(ev.missing(), EVIDENCE_ITEMS.to_vec());
        assert!(!ev.complete() && ev.line().is_none());
        let tricky = "commit 1a2b, chaos 30 min 100% green π≈日本\nlog: target/c.log by=x at_ms=1";
        ev.record('c', entry(tricky));
        ev.record('z', entry("unknown"));
        assert_eq!(ev.missing(), vec!['a', 'b', 'd', 'e']);
        let line = ev.line().expect("has items");
        assert!(!line.contains('\n') && !line.contains(" by="), "{line}");
        assert_eq!(Evidence::parse(&line), ev);
        let q = QuietLease {
            on: true,
            knows: 3,
            by: "boss".into(),
            at_ms: 7,
            delegation: true,
            evidence: ev.clone(),
            delegation_unproven: None,
        };
        let back = QuietLease::decode(&q.encode()).expect("readable");
        assert_eq!(back, q);
        assert_eq!(
            back.evidence.get('c').map(|e| e.note.as_str()),
            Some(tricky)
        );
        for k in EVIDENCE_ITEMS {
            ev.record(k, entry(&format!("item {k}")));
        }
        assert!(ev.complete());
        assert_eq!(Evidence::parse(&ev.line().unwrap_or_default()), ev);
    }

    /// **Read leniently.** A damaged or unknown item is skipped (counts as missing), the
    /// healthy item next to it stays, the object still reads; free text from before the
    /// `v1` form reads as no items; a damaged override mark ⇒ absent.
    #[test]
    fn evidence_reads_leniently() {
        let good = "a=5,op,m1%20ok";
        for (bad, why) in [
            ("b=x,op,note", "at_ms is not a number"),
            ("b=5,op", "missing note"),
            ("b=5,op,", "empty note"),
            ("b=5,op,%ZZ", "bad escape"),
            ("b=5,op,%FF", "not UTF-8"),
            ("bb=5,op,note", "two-letter key"),
            ("f=5,op,note", "unknown item"),
            ("b", "no ="),
        ] {
            let ev = Evidence::parse(&format!("v1 {good} {bad}"));
            assert_eq!(ev.missing(), vec!['b', 'c', 'd', 'e'], "{why}");
            assert_eq!(ev.get('a').map(|e| e.note.as_str()), Some("m1 ok"), "{why}");
            let raw = format!(
                "cairnflow-quiet-lease/1\nstate=on\nknows=3\nby=op at_ms=1\ndelegation=on\nevidence=v1 {good} {bad}\n"
            );
            let q = QuietLease::decode(raw.as_bytes()).expect("damaged item keeps the object");
            assert!(
                q.on && q.delegation && q.by == "op" && q.at_ms == 1,
                "{why}"
            );
            assert_eq!(q.evidence.missing(), vec!['b', 'c', 'd', 'e'], "{why}");
        }
        for legacy in ["m1 ok; chaos 30m ok", "a=5,op,note", "v2 a=5,op,note", ""] {
            assert_eq!(Evidence::parse(legacy), Evidence::default(), "{legacy:?}");
        }
        for bad in ["", "x,op,cd", "5,op,", "5,op,cz", "5,%ZZ,cd", "5,op"] {
            let raw = format!("cairnflow-quiet-lease/1\nstate=on\ndelegation_unproven={bad}\n");
            let q = QuietLease::decode(raw.as_bytes()).expect("damaged mark keeps the object");
            assert!(q.on && q.delegation_unproven.is_none(), "{bad:?}");
        }
    }

    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy(OBJECT).map(|p| p.header),
            Some(HEADER)
        );
    }
}
