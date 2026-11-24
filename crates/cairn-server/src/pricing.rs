// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Token price table: object `<prefix>/pricing`, **read leniently**, written by
//! `cairn-ctl pricing set` (CAS, rule 7).
//!
//! ```text
//! cairnflow-pricing/1
//! version=<n>                       bumped on every write — makes the content unique for CAS's comparison
//! by=<who>                          the last writer (read leniently)
//! <model>\t<micro-USD per 1 M input tokens>\t<micro-USD per 1 M output tokens>
//! ```
//!
//! Corrupt or a newer header ⇒ no prices at all (`cost_micros` = 0) — metering
//! never breaks because of the price table. The node reloads at every metering
//! flush tick; an absent object keeps the table already loaded.
//! A new object, no `DATA_FORMAT` bump (AGENTS §1.2).

use {
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
    std::{collections::BTreeMap, sync::Arc},
};

const HEADER: &str = "cairnflow-pricing/1";

/// Price of one model: micro-USD per 1 000 000 tokens.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Price {
    pub input: u64,
    pub output: u64,
}

impl Price {
    /// Cost (micro-USD) of `input`/`output` tokens.
    pub fn cost(&self, input: u64, output: u64) -> u64 {
        let c = u128::from(input) * u128::from(self.input)
            + u128::from(output) * u128::from(self.output);
        (c / 1_000_000).min(u128::from(u64::MAX)) as u64
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Pricing {
    pub version: u64,
    pub by: String,
    pub models: BTreeMap<String, Price>,
}

impl Pricing {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("{HEADER}\nversion={}\n", self.version);
        if !self.by.is_empty() {
            out.push_str(&format!("by={}\n", self.by.replace(['\n', '\t'], " ")));
        }
        for (m, p) in &self.models {
            out.push_str(&format!("{m}\t{}\t{}\n", p.input, p.output));
        }
        out.into_bytes()
    }

    /// Lenient read: unknown header ⇒ `None`; a corrupt price line ⇒ that line is skipped.
    pub fn decode(raw: &[u8]) -> Option<Pricing> {
        let text = std::str::from_utf8(raw).ok()?;
        let mut lines = text.lines();
        if lines.next()? != HEADER {
            return None;
        }
        let mut out = Pricing::default();
        for line in lines.filter(|l| !l.is_empty()) {
            if let Some(v) = line.strip_prefix("version=") {
                out.version = v.parse().unwrap_or(0);
            } else if let Some(v) = line.strip_prefix("by=") {
                out.by = v.to_string();
            } else {
                let mut f = line.split('\t');
                let (Some(m), Some(i), Some(o)) = (f.next(), f.next(), f.next()) else {
                    continue;
                };
                let (Ok(input), Ok(output)) = (i.parse(), o.parse()) else {
                    continue;
                };
                if !m.is_empty() {
                    out.models.insert(m.to_string(), Price { input, output });
                }
            }
        }
        Some(out)
    }
}

pub fn path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from("pricing")
    } else {
        Path::from(format!("{}/pricing", prefix.as_ref()))
    }
}

/// Price table together with the object version to CAS on. `Ok(None)` = none yet.
pub async fn read(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<Option<(Pricing, slatedb::object_store::UpdateVersion)>, String> {
    let got = match store.get(&path(prefix)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let tag = slatedb::object_store::UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got.bytes().await.map_err(|e| e.to_string())?;
    Ok(Some((Pricing::decode(&raw).unwrap_or_default(), tag)))
}

/// Write by CAS on `expected` (`None` ⇒ create). `Ok(false)` = CAS lost.
/// An ambiguous outcome is resolved by reading back (rule 7): the bumped
/// `version` and `by` make the content unique.
pub async fn write(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    p: &Pricing,
    expected: Option<slatedb::object_store::UpdateVersion>,
) -> Result<bool, String> {
    let mode = match expected {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    match cairn_store::cas::put_resolving_or_overwrite(
        store,
        &path(prefix),
        p.encode().into(),
        mode,
    )
    .await
    {
        Ok(cairn_store::cas::Cas::Written(_)) => Ok(true),
        Ok(cairn_store::cas::Cas::Lost) => Ok(false),
        Err(e) => Err(e.to_string()),
    }
}

/// Load the price table into the process's metering. A failed read or an absent
/// object ⇒ keep the old table: removing a price is `pricing set <model> 0 0`, not
/// deleting the object (and the test harness runs many nodes, many buckets in one
/// process — a bucket without a price table must not wipe another bucket's table).
pub async fn refresh(store: &Arc<dyn ObjectStore>, prefix: &Path) {
    match read(store, prefix).await {
        Ok(Some((p, _))) => crate::metering::set_prices(p.models),
        Ok(None) => {}
        Err(e) => tracing::debug!(error = %e, "cannot read the price table — keeping the old one"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pricing_round_trips_and_reads_lax() {
        let mut p = Pricing {
            version: 3,
            by: "ops@a".into(),
            ..Default::default()
        };
        p.models.insert(
            "m-1".into(),
            Price {
                input: 3_000_000,
                output: 15_000_000,
            },
        );
        assert_eq!(Pricing::decode(&p.encode()), Some(p.clone()));
        assert_eq!(p.models["m-1"].cost(1_000_000, 100_000), 4_500_000);
        assert_eq!(Pricing::decode(b"cairnflow-pricing/2\n"), None);
        let lax =
            Pricing::decode(b"cairnflow-pricing/1\nversion=1\nbad\nm\tx\t1\nok\t1\t2\n").unwrap();
        assert_eq!(lax.models.len(), 1);
        assert_eq!(
            lax.models["ok"],
            Price {
                input: 1,
                output: 2
            }
        );
    }
}
