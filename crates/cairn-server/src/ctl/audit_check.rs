// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Check (k) `audit` of `verify`: the objects under `audit/tenant/<tenant>/<day>/` are
//! correctly named, readable, and whether each intent has an outcome.
//!
//! - Valid names: `r-<hex row key>.json` (records inside a range,
//!   `cairn_store::audit::row_name`) or `<ts>-<node>-<boot 16 hex>-<seq>.{intent,done,denied}`
//!   (`crate::audit::Sink`). Any other name, or a day directory not shaped `yyyy-mm-dd`
//!   ⇒ a finding no round heals.
//! - The body must be JSON (both sources write server-built JSON). Unreadable ⇒ a
//!   finding.
//! - An intent without a `.done`: **a valid case** — death between the intent and the
//!   outcome, or an outcome PUT that kept failing. The D-d runner's reconcile round
//!   (`crate::audit::reconcile_intents`, every 10 min) writes the outcome `unknown` once the
//!   intent is an hour old. Below [`STALE_INTENT_MS`] it is an information line
//!   ([`VerifyReport::notes`] — the report stays clean); beyond that it is a finding: the
//!   round has not run for most of a day.
//!
//! Runs with `open_dbs` (`cairn-ctl verify`, `Bucket::drop`), not in the background
//! scrubber (`open_dbs: false`): the audit tree grows over time, and a LIST + GET of
//! every object on every scrub round is too expensive.

use {
    super::{CtlResult, Finding, Skipped, VerifyOptions, VerifyReport},
    futures::{StreamExt, TryStreamExt},
    slatedb::object_store::{path::Path, ObjectStore, ObjectStoreExt},
    std::collections::BTreeSet,
};

/// An intent without an outcome for longer than this becomes a warning.
pub const STALE_INTENT_MS: u64 = 24 * 3_600_000;

/// Cap on audit objects per `verify`; over ⇒ one `heals("not checked")` line.
const AUDIT_VERIFY_MAX: usize = 200_000;

/// Maximum size of an audit object `verify` downloads to check the JSON — server-built
/// records are a few hundred bytes; larger is odd, reported without downloading.
const AUDIT_OBJECT_MAX: u64 = 1 << 20;

/// Audit object kind by name.
#[derive(Debug, PartialEq, Eq)]
enum Kind {
    Row,
    /// Intent / outcome / denial, with the original name (no suffix) and the `ts` mark.
    Note {
        base: String,
        ext: &'static str,
        ts: u64,
    },
}

fn is_day(d: &str) -> bool {
    d.len() == 10
        && d.bytes().enumerate().all(|(i, b)| {
            if i == 4 || i == 7 {
                b == b'-'
            } else {
                b.is_ascii_digit()
            }
        })
}

fn is_hex(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn kind_of(name: &str) -> Option<Kind> {
    if let Some(hex) = name
        .strip_prefix("r-")
        .and_then(|n| n.strip_suffix(".json"))
    {
        return (is_hex(hex) && hex.len() % 2 == 0).then_some(Kind::Row);
    }
    let (base, ext) = name.rsplit_once('.')?;
    let ext = ["intent", "done", "denied", "noop"]
        .into_iter()
        .find(|e| *e == ext)?;
    let parts: Vec<&str> = base.split('-').collect();
    let [ts, node @ .., boot, seq] = parts.as_slice() else {
        return None;
    };
    let ok = !node.is_empty()
        && node.iter().all(|p| !p.is_empty())
        && boot.len() == 16
        && is_hex(boot)
        && !seq.is_empty()
        && seq.bytes().all(|b| b.is_ascii_digit());
    let ts = ts.parse().ok().filter(|_| ok)?;
    Some(Kind::Note {
        base: base.to_string(),
        ext,
        ts,
    })
}

/// (k) on `store` under `prefix`.
pub(super) async fn audit_findings(
    store: &std::sync::Arc<dyn ObjectStore>,
    prefix: &Path,
    opts: &VerifyOptions,
    rep: &mut VerifyReport,
) -> CtlResult<()> {
    let root = if prefix.as_ref().is_empty() {
        Path::from(cairn_store::audit::DIR)
    } else {
        Path::from(format!("{}/{}", prefix.as_ref(), cairn_store::audit::DIR))
    };
    rep.items += 1;
    // Streamed, stopping at cap + 1: do not load the whole audit tree into RAM before
    // knowing it is too large.
    let metas = match store
        .list(Some(&root))
        .take(AUDIT_VERIFY_MAX + 1)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(m) => m,
        Err(e) if opts.skip_unreadable => {
            rep.skipped
                .push(Skipped::new("audit", None, root.to_string(), e));
            return Ok(());
        }
        Err(e) => return Err(cairn_store::StoreError::Io(e.to_string()).into()),
    };
    if metas.len() > AUDIT_VERIFY_MAX {
        rep.notes.push(
            Finding::new(
                "audit",
                None,
                format!("more than {AUDIT_VERIFY_MAX} audit objects — not checked"),
            )
            .heals("not checked"),
        );
        return Ok(());
    }
    let skip = root.as_ref().len() + 1;
    let mut intents = Vec::new();
    let mut done: BTreeSet<String> = BTreeSet::new();
    for m in metas {
        let path = m.location.to_string();
        let rel = path.get(skip..).unwrap_or_default();
        let parts: Vec<&str> = rel.split('/').collect();
        let kind = match parts.as_slice() {
            [_tenant, day, name] if is_day(day) => kind_of(name),
            _ => None,
        };
        let Some(kind) = kind else {
            rep.findings.push(Finding::new(
                "audit",
                None,
                format!(
                    "{path}: unknown name (not r-<hex>.json or <ts>-<node>-<boot>-<seq>.<kind>)"
                ),
            ));
            continue;
        };
        if m.size > AUDIT_OBJECT_MAX {
            rep.findings.push(Finding::new(
                "audit",
                None,
                format!("{path}: {} bytes — too large for an audit record", m.size),
            ));
            continue;
        }
        let body = match store.get(&m.location).await {
            Ok(g) => g.bytes().await,
            Err(e) => Err(e),
        };
        match body {
            Ok(b) if serde_json::from_slice::<serde_json::Value>(&b).is_ok() => {}
            Ok(_) => rep.findings.push(Finding::new(
                "audit",
                None,
                format!("{path}: body is not JSON"),
            )),
            // Deleted between LIST and GET: nothing left to check.
            Err(slatedb::object_store::Error::NotFound { .. }) => continue,
            Err(e) if opts.skip_unreadable => {
                rep.skipped
                    .push(Skipped::new("audit", None, path.clone(), e));
                continue;
            }
            Err(e) => rep.findings.push(Finding::new(
                "audit",
                None,
                format!("{path}: cannot read: {e}"),
            )),
        }
        match kind {
            Kind::Note {
                base,
                ext: "intent",
                ts,
            } => intents.push((path, base, ts)),
            Kind::Note {
                base, ext: "done", ..
            } => {
                done.insert(base);
            }
            _ => {}
        }
    }
    for (path, base, ts) in intents {
        if done.contains(&base) {
            continue;
        }
        let age = opts.now_ms.saturating_sub(ts);
        let f = Finding::new(
            "audit",
            None,
            format!(
                "{path}: intent without an outcome, {} minutes old",
                age / 60_000
            ),
        );
        if age < STALE_INTENT_MS {
            // Death between intent and outcome is a valid case; the reconcile round
            // records `unknown` after an hour.
            rep.notes.push(f.heals("audit reconcile round"));
        } else {
            rep.findings.push(f);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(kind_of("r-00ab.json"), Some(Kind::Row));
        assert_eq!(kind_of("r-0ab.json"), None, "odd hex length");
        assert_eq!(
            kind_of("1793620000000-node-a-00000000000000ab-7.intent"),
            Some(Kind::Note {
                base: "1793620000000-node-a-00000000000000ab-7".into(),
                ext: "intent",
                ts: 1_793_620_000_000,
            })
        );
        assert!(kind_of("1-n-00000000000000ab-0.done").is_some());
        assert!(kind_of("1-n-00000000000000ab-0.denied").is_some());
        for bad in [
            "by-hand.txt",
            "1-n-00000000000000ab-0.json",
            "x-n-00000000000000ab-0.intent",
            "1-n-00ab-0.intent",
            "1--00000000000000ab-0.intent",
            "1-n-00000000000000ab-.intent",
        ] {
            assert_eq!(kind_of(bad), None, "{bad}");
        }
        assert!(is_day("2026-11-02") && !is_day("2026-1-02"));
    }
}
