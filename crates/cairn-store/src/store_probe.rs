// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The store's conditional-GET probe** (`docs/reliability/irreversible.md` §0.3 row
//! G18).
//!
//! Solo mode (LS-16) extends the grant from an `If-None-Match` GET on the node's own
//! session record: `304` means "nobody has challenged yet". The read fence (LS-5) and the
//! coordinator's fence rely on the same thing. All three are correct only when the store
//! guarantees:
//!
//! 1. after a **winning** write to `X`, a GET of `X` with `If-None-Match` = the **old**
//!    etag returns `200` with the new body — **never `304`**;
//! 2. a GET with the **new** etag returns `304`;
//! 3. a read right after a write, from the same client or **another client**, sees the
//!    new version;
//! 4. every write changes the etag.
//!
//! S3 is strongly consistent so it keeps all four; a caching proxy in front of the bucket,
//! or a weakly consistent store, does not — and then a lone node could renew from a stale
//! `304` while a newcomer has already finished challenging and taken the range. Contract
//! tests (`library_contracts.rs`, `s3_store_contract.rs`) pin the four points on
//! `memory://`, `file://` and real S3; `cairn-ctl cluster enable-quiet-lease` runs
//! [`conditional_get`] on the cluster's own bucket before turning the gate on, and
//! refuses on failure.
//!
//! **Probe objects** live under [`DIR`] (`control/probe/<nonce>`); the key carries the
//! run's nonce so two runs never touch each other. This is not customer data and nobody
//! references it: it is deleted directly when done, like the probe key of the D12 gate
//! (`config/gate.rs`; `scripts/lint-delete.allow`) — only when the last read saw exactly
//! our content. A failed delete leaves it behind (a few dozen bytes).
//!
//! **The probe's own** writes resolve ambiguity by reading back (rule 7,
//! [`crate::cas::put_resolving_or_overwrite`]: `--data-dir` has no CAS, so it overwrites —
//! one process). What is measured are the GETs.

use {
    crate::{
        cas::{self, Cas},
        error::{Result, StoreError},
        retry::{self, Verdict},
    },
    bytes::Bytes,
    slatedb::object_store::{
        path::Path, Error as OsError, GetOptions, ObjectStore, ObjectStoreExt, PutMode,
        UpdateVersion,
    },
    std::{sync::Arc, time::Duration},
};

/// Directory of the probe objects, under the cluster prefix.
pub const DIR: &str = "control/probe";

/// Default number of rounds of `cairn-ctl` — enough for a weakly consistent store to show
/// itself in the first few rounds, while still only ~`ROUNDS` PUTs on a real bucket.
pub const ROUNDS: u32 = 32;

/// The probe passed.
#[derive(Clone, Debug)]
pub struct Passed {
    pub rounds: u32,
    pub elapsed: Duration,
}

/// `Ok(Ok(_))` = the store keeps the contract; `Ok(Err(reason))` = the store **answered**
/// wrongly (a verdict); `Err` = a transient I/O error, no verdict yet.
pub type Outcome = std::result::Result<Passed, String>;

/// Path of probe object `nonce` under `prefix`.
pub fn path(prefix: &Path, nonce: &str) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(format!("{DIR}/{nonce}"))
    } else {
        Path::from(format!("{}/{DIR}/{nonce}", prefix.as_ref()))
    }
}

/// A fresh nonce for one run: process + time + random (through [`crate::entropy`],
/// deterministic under simulation).
pub fn nonce() -> String {
    let mut r = [0u8; 4];
    crate::entropy::fill(&mut r);
    format!(
        "{}-{}-{:08x}",
        crate::entropy::process_id(),
        crate::entropy::now_ms(),
        u32::from_le_bytes(r)
    )
}

/// Header of the probe object — see `crate::format::OBJECT_POLICIES`.
pub const HEADER: &str = "cairnflow-store-probe/1";

fn body(nonce: &str, round: u32) -> Bytes {
    Bytes::from(format!("{HEADER}\nprobe={nonce}\nround={round}\n"))
}

/// I/O error of one step: transient ⇒ no verdict (`Err`), anything else ⇒ the store
/// answered wrongly (`Ok(Err)`).
fn classify(step: &str, e: &OsError) -> std::result::Result<String, StoreError> {
    if retry::object_store_verdict(e) == Verdict::Transient {
        Err(StoreError::Io(format!(
            "conditional GET probe: {step}: transient I/O error, no verdict yet: {e}"
        )))
    } else {
        Ok(format!("{step}: {e}"))
    }
}

/// One GET of the probe: `Some(body)` or `None` = `304`.
async fn get(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    if_none_match: Option<&str>,
    step: &str,
) -> Result<std::result::Result<Option<Bytes>, String>> {
    let opts = GetOptions {
        if_none_match: if_none_match.map(str::to_string),
        ..Default::default()
    };
    match store.get_opts(path, opts).await {
        Err(OsError::NotModified { .. }) => Ok(Ok(None)),
        Err(e) => classify(step, &e).map(Err),
        Ok(got) => match got.bytes().await {
            Ok(b) => Ok(Ok(Some(b))),
            Err(e) => classify(step, &e).map(Err),
        },
    }
}

/// Runs the probe for `rounds` rounds on object `control/probe/<nonce>` under `prefix`.
/// `writer` writes and reads; `reader` is **another client** pointing at the same bucket
/// (read-after-write from another client; pass the same store if there is none).
///
/// Each round `i`: write `v_i` (CAS If-Match etag `e_{i-1}`, `Create` in the first
/// round) → etag `e_i`; then `e_i ≠ e_{i-1}`; GET If-None-Match `e_{i-1}` on both clients
/// ⇒ `200` + `v_i` (`304` is a blocking failure: "stale 304"); GET If-None-Match `e_i` on
/// both clients ⇒ `304`; a plain GET on `reader` ⇒ `v_i`.
pub async fn conditional_get(
    writer: &Arc<dyn ObjectStore>,
    reader: &Arc<dyn ObjectStore>,
    prefix: &Path,
    nonce: &str,
    rounds: u32,
) -> Result<Outcome> {
    let t0 = tokio::time::Instant::now();
    let path = path(prefix, nonce);
    let mut prev: Option<UpdateVersion> = None;
    let mut last_ours = false;
    let outcome = 'probe: {
        for i in 0..rounds.max(1) {
            let want = body(nonce, i);
            let mode = match &prev {
                None => PutMode::Create,
                Some(v) => PutMode::Update(v.clone()),
            };
            let now = match cas::put_resolving_or_overwrite(writer, &path, want.clone(), mode).await
            {
                Ok(Cas::Written(v)) => v,
                Ok(Cas::Lost) => {
                    break 'probe Ok(Err(format!(
                        "round {i}: CAS on the probe object lost — someone wrote this run's own key"
                    )))
                }
                Err(e) => match classify(&format!("round {i}: write"), &e) {
                    Ok(why) => break 'probe Ok(Err(why)),
                    Err(io) => break 'probe Err(io),
                },
            };
            last_ours = true;
            let Some(etag) = now.e_tag.clone() else {
                break 'probe Ok(Err(format!(
                    "round {i}: the store returned no etag for the write — conditional GET is unusable"
                )));
            };
            let old = prev.as_ref().and_then(|v| v.e_tag.clone());
            if old.as_deref() == Some(etag.as_str()) {
                break 'probe Ok(Err(format!(
                    "round {i}: etag unchanged after the write ({etag}) — `304` cannot tell old from new"
                )));
            }
            for (who, store) in [("writing client", writer), ("other client", reader)] {
                if let Some(old) = old.as_deref() {
                    let step = format!("round {i}: {who} GET If-None-Match old etag");
                    match get(store, &path, Some(old), &step).await {
                        Err(io) => break 'probe Err(io),
                        Ok(Err(why)) => break 'probe Ok(Err(why)),
                        Ok(Ok(None)) => {
                            break 'probe Ok(Err(format!(
                                "{step}: store returned a STALE 304 after a winning write — a lone \
                                 node would renew from a record that was already challenged \
                                 (caching proxy or weakly consistent store?)"
                            )))
                        }
                        Ok(Ok(Some(b))) if b != want => {
                            break 'probe Ok(Err(format!(
                                "{step}: read the old version ({:?}) after a winning write",
                                String::from_utf8_lossy(&b)
                            )))
                        }
                        Ok(Ok(Some(_))) => {}
                    }
                }
                let step = format!("round {i}: {who} GET If-None-Match new etag");
                match get(store, &path, Some(&etag), &step).await {
                    Err(io) => break 'probe Err(io),
                    Ok(Err(why)) => break 'probe Ok(Err(why)),
                    Ok(Ok(None)) => {}
                    Ok(Ok(Some(b))) => {
                        break 'probe Ok(Err(format!(
                            "{step}: store returned 200 ({} bytes) instead of 304 — ignores \
                             If-None-Match or the etag is unstable",
                            b.len()
                        )))
                    }
                }
            }
            let step = format!("round {i}: other client GET");
            match get(reader, &path, None, &step).await {
                Err(io) => break 'probe Err(io),
                Ok(Err(why)) => break 'probe Ok(Err(why)),
                Ok(Ok(Some(b))) if b == want => {}
                Ok(Ok(got)) => {
                    break 'probe Ok(Err(format!(
                        "{step}: did not see the version just written (saw {:?}) — read-after-write is inconsistent",
                        got.map(|b| String::from_utf8_lossy(&b).into_owned())
                    )))
                }
            }
            prev = Some(now);
        }
        Ok(Ok(Passed {
            rounds: rounds.max(1),
            elapsed: t0.elapsed(),
        }))
    };
    // Clean up: only when the last write is ours (the key carries the run's nonce) and the
    // probe did not conclude the store reads wrongly — if it does, "the last read saw
    // ours" is not trustworthy, so the object stays (a few dozen bytes).
    if last_ours && matches!(outcome, Ok(Ok(_))) {
        if let Err(e) = writer.delete(&path).await {
            tracing::debug!(path = %path, error = %e, "could not delete the probe object — leaving it");
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    fn mem() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[test]
    fn every_header_is_in_policy() {
        assert_eq!(
            crate::format::object_policy("control/probe/<nonce>").map(|p| p.header),
            Some(HEADER)
        );
    }

    #[tokio::test]
    async fn in_memory_store_passes_and_leaves_nothing() -> Result<()> {
        let store = mem();
        let got = conditional_get(&store, &store, &Path::from("c"), "n", 8).await?;
        assert!(got.is_ok(), "{got:?}");
        let left = store.head(&path(&Path::from("c"), "n")).await;
        assert!(
            matches!(left, Err(OsError::NotFound { .. })),
            "probe object left behind: {left:?}"
        );
        Ok(())
    }

    /// A caching layer returns `304` for every conditional GET: verdict "stale 304".
    #[tokio::test]
    async fn stale_not_modified_is_refused() -> Result<()> {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains(DIR.into()),
            Action::StaleNotModified,
        ));
        let store: Arc<dyn ObjectStore> = faulty;
        let got = conditional_get(&store, &store, &Path::default(), "n", 4).await?;
        let why = got.expect_err("a stale 304 must be refused");
        assert!(why.contains("STALE 304"), "{why}");
        Ok(())
    }

    /// The probe's write is ambiguous (applied, the caller gets an error / a retry-style
    /// `Precondition`): resolved by reading back (rule 7), the probe still passes.
    #[tokio::test]
    async fn ambiguous_probe_write_resolves_by_reading_back() -> Result<()> {
        for reply in [Ambiguity::Error, Ambiguity::Retried] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            faulty.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(DIR.into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(2),
                },
            ));
            let store: Arc<dyn ObjectStore> = faulty;
            let got = conditional_get(&store, &store, &Path::default(), "n", 4).await?;
            assert!(got.is_ok(), "{reply:?}: {got:?}");
        }
        Ok(())
    }

    /// A transient error is not a verdict.
    #[tokio::test]
    async fn transient_failure_is_not_a_verdict() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains(DIR.into()),
            Action::Fail { times: None },
        ));
        let store: Arc<dyn ObjectStore> = faulty;
        let got = conditional_get(&store, &store, &Path::default(), "n", 2).await;
        assert!(matches!(got, Err(StoreError::Io(_))), "{got:?}");
    }
}
