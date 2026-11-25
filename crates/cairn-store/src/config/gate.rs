// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The bucket's conditional PUT / CAS gate (D12) and the `format` check.

use {
    super::*,
    crate::retry::{self, Policy, Verdict},
};

/// A single attempt: only for callers with no boot bound (tests, tools).
const ONCE: Policy = Policy::new(
    std::time::Duration::from_millis(100),
    std::time::Duration::from_secs(1),
    1,
);

impl ObjectStores {
    /// **The D12 gate.** Runs before opening the database, on *both* tiers.
    ///
    /// Failing ⇒ return an error, and the caller must stop startup. This is not a
    /// warning: latency is performance, this is correctness.
    ///
    /// When both tiers point at the same store the probe runs only once — probing the same
    /// place twice adds no information. A single attempt: a transient I/O error comes out
    /// as [`StoreError::Io`] — see [`Self::assert_conditional_put_within`].
    pub async fn assert_conditional_put(&self) -> Result<()> {
        self.assert_conditional_put_within(&ONCE).await
    }

    /// Like [`Self::assert_conditional_put`], retrying transient I/O errors per `policy`
    /// (the boot path). It concludes "the store does not honor the condition" only from
    /// the store's **answer**; a transient error (5xx, network) proves nothing, so it is
    /// a [`StoreError::Io`] and gets retried — with the same probe key across all
    /// attempts.
    pub async fn assert_conditional_put_within(&self, policy: &Policy) -> Result<()> {
        let nonce = probe_nonce();
        gate_retrying(policy, || {
            conditional_put_gate(
                self.main.store(),
                self.main.prefix(),
                "main (SST + manifest)",
                &nonce,
            )
        })
        .await?;
        if let Some(wal) = &self.wal {
            gate_retrying(policy, || {
                conditional_put_gate(wal.store(), wal.prefix(), "WAL", &nonce)
            })
            .await?;
        }
        Ok(())
    }

    /// **The lease-tier gate.** Applies only to multi-node deployments.
    ///
    /// [`crate::Session`] renews the session with a CAS on the etag (`PutMode::Update`)
    /// — stronger than the `PutMode::Create` the D12 gate requires, and **not every store
    /// that has one has the other**. `LocalFileSystem` is an in-house example: it accepts
    /// `Create` but returns `not yet implemented` for `Update`.
    ///
    /// So this is a separate gate, run separately, and it does **not** run for a
    /// single-node deployment: a single node has no leader election, it uses
    /// [`crate::LeadershipGrant::single_node`] and never touches leases. Merging the two
    /// gates into one would block the `Dev / single node` deployment shape and gain
    /// nothing.
    ///
    /// Only the main store is checked — leases live next to the manifest, not in the WAL
    /// tier.
    pub async fn assert_compare_and_swap(&self) -> Result<()> {
        self.assert_compare_and_swap_within(&ONCE).await
    }

    /// Like [`Self::assert_compare_and_swap`], retrying transient I/O errors per `policy`
    /// — see [`Self::assert_conditional_put_within`].
    pub async fn assert_compare_and_swap_within(&self, policy: &Policy) -> Result<()> {
        let nonce = probe_nonce();
        gate_retrying(policy, || {
            compare_and_swap_gate(self.main.store(), self.main.prefix(), &nonce)
        })
        .await
    }

    /// Check the bucket's data format marker, writing the marker if the bucket is still
    /// empty — see [`crate::DATA_FORMAT`]. `open_db` calls it too; it is called early at
    /// startup so the node refuses **before** writing anything else to the bucket.
    pub async fn ensure_format(&self) -> Result<()> {
        crate::format::ensure(self.main.store(), self.main.prefix(), true).await
    }
}

/// Retry a gate: only [`StoreError::Io`] (a transient error of the probe) is retryable;
/// a gate error (`*Unsupported`) is a conclusion, returned at once.
async fn gate_retrying<F, Fut>(policy: &Policy, op: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    retry::run_classified(
        &retry::ops::BOOT_GATE,
        policy,
        retry::Never,
        |e: &StoreError| match e {
            StoreError::Io(_) => Verdict::Transient,
            _ => Verdict::Permanent,
        },
        op,
    )
    .await
}

/// A transient I/O error of the probe: it proves nothing about the store — try again.
fn probe_io(tier: &str, step: &str, e: &slatedb::object_store::Error) -> StoreError {
    StoreError::Io(format!(
        "gate probe of tier {tier}: {step}: transient I/O error, cannot yet tell whether the store honors the condition: {e}"
    ))
}

/// A transient error per [`retry::object_store_verdict`].
fn transient(e: &slatedb::object_store::Error) -> bool {
    retry::object_store_verdict(e) == Verdict::Transient
}

/// Clear the probe key before an attempt. A transient error ⇒ retry the whole probe: if
/// the key still holds the content of an earlier attempt (same nonce), the first `Create`
/// misses and the gate concludes wrongly. Other errors are ignored as before — the write
/// below will tell the truth.
async fn clear_probe(store: &Arc<dyn ObjectStore>, path: &Path, tier: &str) -> Result<()> {
    match store.delete(path).await {
        Err(e) if transient(&e) => Err(probe_io(tier, "clearing the probe key", &e)),
        _ => Ok(()),
    }
}

/// Probe content. Only meaningful to a human reading the store's logs.
pub(super) const FIRST: &[u8] = b"cairnflow-conditional-put-probe/first";
pub(super) const ZOMBIE: &[u8] = b"cairnflow-conditional-put-probe/zombie";

/// Probe one tier: PUT `Create` twice to the same key.
///
/// Three outcomes, only one passes:
///
/// | Second write | Content afterwards | Conclusion |
/// |---|---|---|
/// | `AlreadyExists` | intact | ✅ the fence is usable |
/// | `AlreadyExists` | **overwritten** | ❌ worse than accepting outright: the error is reported correctly yet the data is still lost |
/// | `Ok` | overwritten | ❌ the fence has a hole |
///
/// The middle column is why this probe reads the content back instead of trusting the
/// error code. Garage v1.0.1 showed that the two can come apart.
pub(super) async fn conditional_put_gate(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    tier: &str,
    nonce: &str,
) -> Result<()> {
    // A per-process key: two nodes starting together must not make each other fail the
    // gate. What we measure is the store's behaviour, not a race.
    let p = prefix.as_ref();
    let path = Path::from(if p.is_empty() {
        format!(".cairnflow-fence-probe/{nonce}")
    } else {
        format!("{p}/.cairnflow-fence-probe/{nonce}")
    });

    let fail = |reason: String| StoreError::ConditionalPutUnsupported {
        tier: tier.to_string(),
        store: store.to_string(),
        reason,
    };

    // Clear first: an earlier run killed midway may have left garbage at exactly this
    // key, and then the **first** PUT misses — a completely false report.
    clear_probe(store, &path, tier).await?;

    // The **probe's own** write resolves ambiguity by reading back (rule 7): the key carries
    // the process nonce, so "exactly our content" means our write. The **test** (the second
    // write below) goes bare — that is what is being measured.
    match crate::cas::put_resolving(store, &path, Bytes::from_static(FIRST), PutMode::Create).await
    {
        Ok(crate::cas::Cas::Written(_)) => {}
        Ok(crate::cas::Cas::Lost) => {
            return Err(fail(
                "first conditional PUT failed: the probe key already holds other content".into(),
            ))
        }
        Err(e) if transient(&e) => return Err(probe_io(tier, "first conditional PUT", &e)),
        Err(e) => return Err(fail(format!("first conditional PUT failed: {e}"))),
    }

    let second = store
        .put_opts(
            &path,
            PutPayload::from_static(ZOMBIE),
            PutOptions::from(PutMode::Create),
        )
        .await;

    // Read back **before** concluding: the error code and the actual damage are two
    // different things. A transient read failure cannot conclude anything — try again.
    let content = match store.get(&path).await {
        Ok(g) => match g.bytes().await {
            Ok(b) => Some(b),
            Err(e) if transient(&e) => return Err(probe_io(tier, "rereading probe key", &e)),
            Err(_) => None,
        },
        Err(e) if transient(&e) => return Err(probe_io(tier, "rereading probe key", &e)),
        Err(_) => None,
    };
    let _ = store.delete(&path).await;

    let clobbered = content.as_deref() == Some(ZOMBIE);
    match second {
        // The second write failed transiently (503 before reaching the store): nothing was
        // measured. A store that took the write but still reported 5xx was caught by the
        // read-back above seeing ZOMBIE.
        Err(e) if transient(&e) && !clobbered => {
            Err(probe_io(tier, "second conditional PUT", &e))
        }
        Ok(_) => Err(fail(
            "second conditional PUT was accepted — a zombie writer can overwrite the new leader's WAL"
                .into(),
        )),
        Err(e) if !matches!(e, slatedb::object_store::Error::AlreadyExists { .. }) => Err(fail(
            format!("second conditional PUT returned an unexpected error, not AlreadyExists: {e}"),
        )),
        Err(_) if clobbered => Err(fail(
            "store reported AlreadyExists but still overwrote the content — the error is reported correctly yet the data is still lost"
                .into(),
        )),
        Err(_) => {
            tracing::info!(tier, store = %store, "conditional PUT gate: passed");
            Ok(())
        }
    }
}

/// CAS probe: write, update with the right etag, then update again with the **old** etag.
///
/// Only the last step means anything. A store that accepts all three is a store that
/// ignores the condition — and a lease built on it would grant leadership to two nodes at
/// once without either knowing. (Still no data loss, thanks to the `Create` fence; but it
/// turns failover into a continuous tug of war instead of a single handover.)
pub(super) async fn compare_and_swap_gate(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    nonce: &str,
) -> Result<()> {
    const TIER: &str = "lease";
    let p = prefix.as_ref();
    let path = Path::from(if p.is_empty() {
        format!(".cairnflow-cas-probe/{nonce}")
    } else {
        format!("{p}/.cairnflow-cas-probe/{nonce}")
    });

    let fail = |reason: String| StoreError::CompareAndSwapUnsupported {
        store: store.to_string(),
        reason,
    };
    let cleanup = |store: Arc<dyn ObjectStore>, path: Path| async move {
        let _ = store.delete(&path).await;
    };

    clear_probe(store, &path, TIER).await?;
    // The **probe's own** two writes resolve ambiguity by reading back (rule 7) — the key
    // carries the process nonce. The attempt with the old etag below goes bare.
    let stale =
        match crate::cas::put_resolving(store, &path, Bytes::from_static(b"v1"), PutMode::Create)
            .await
        {
            Ok(crate::cas::Cas::Written(v)) => v,
            Ok(crate::cas::Cas::Lost) => {
                return Err(fail(
                    "create PUT failed: the probe key already holds other content".into(),
                ))
            }
            Err(e) if transient(&e) => return Err(probe_io(TIER, "create PUT", &e)),
            Err(e) => return Err(fail(format!("create PUT failed: {e}"))),
        };

    match crate::cas::put_resolving(
        store,
        &path,
        Bytes::from_static(b"v2"),
        PutMode::Update(stale.clone()),
    )
    .await
    {
        Ok(crate::cas::Cas::Written(_)) => {}
        Ok(crate::cas::Cas::Lost) => {
            cleanup(Arc::clone(store), path).await;
            return Err(fail("CAS with the right etag was refused".into()));
        }
        Err(e) if transient(&e) => {
            cleanup(Arc::clone(store), path).await;
            return Err(probe_io(TIER, "CAS with the right etag", &e));
        }
        Err(e) => {
            cleanup(Arc::clone(store), path).await;
            return Err(fail(format!("CAS with the right etag was refused: {e}")));
        }
    }

    // Old etag: this is what a lease holder that has been robbed does.
    let third = store
        .put_opts(
            &path,
            PutPayload::from_static(b"zombie"),
            PutOptions::from(PutMode::Update(stale)),
        )
        .await;
    cleanup(Arc::clone(store), path).await;

    match third {
        Ok(_) => Err(fail(
            "CAS with the OLD etag was still accepted — a robbed lease holder can still overwrite the lease".into(),
        )),
        Err(slatedb::object_store::Error::Precondition { .. }) => {
            tracing::info!(store = %store, "CAS gate (lease tier): passed");
            Ok(())
        }
        // Transient failure before reaching the store: nothing was measured.
        Err(e) if transient(&e) => Err(probe_io(TIER, "CAS with the old etag", &e)),
        Err(e) => Err(fail(format!(
            "CAS with the old etag returned an unexpected error, not Precondition: {e}"
        ))),
    }
}

/// A per-process probe key: two nodes starting together must not make each other fail
/// the gate. What we measure is the store's behaviour, not a race.
pub(super) fn probe_nonce() -> String {
    format!(
        "{}-{}",
        crate::entropy::process_id(),
        crate::entropy::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    /// Rule 7: the **probe's own** write reached the store but the reply says otherwise
    /// (object_store retries 5xx on its own). Before the fix: the gate took that as a broken
    /// store — the first `Create` "failed", the CAS with the right etag "was refused" — and
    /// the node refused to start on a healthy S3.
    ///
    /// Must see: both gates pass. The gate's tests (the second `Create`, the CAS with the
    /// old etag) still go bare, so a store that swallows the condition still fails.
    #[tokio::test]
    async fn ambiguous_probe_write_does_not_fail_the_gate() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            faulty.add_rule(Rule::ambiguous_put(".cairnflow-fence-probe/", reply));
            conditional_put_gate(&store, &Path::from("p"), "main", "n")
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: fence gate failed, ambiguous probe: {e}"));
            // The `Create`, then the CAS with the right etag, one rule each.
            for skip in [0, 1] {
                faulty.add_rule(Rule::new(
                    OpSet::PUT,
                    PathMatch::Contains(".cairnflow-cas-probe/".into()),
                    Action::Ambiguous {
                        reply,
                        skip,
                        times: Some(1),
                    },
                ));
            }
            compare_and_swap_gate(&store, &Path::from("p"), "n")
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: CAS gate failed, ambiguous probe: {e}"));
        }
    }

    /// A 503 (before reaching the store) on any request of the probe says nothing about
    /// the store. Must see: a single attempt ⇒ `StoreError::Io`, **not** `*Unsupported`;
    /// with a retry policy ⇒ the gate passes.
    #[tokio::test]
    async fn transient_probe_error_is_io_and_retried() {
        let policy = Policy::new(
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
            4,
        );
        for (probe, ops, skip) in [
            (".cairnflow-fence-probe/", OpSet::DELETE, 0),
            (".cairnflow-fence-probe/", OpSet::PUT, 0),
            (".cairnflow-fence-probe/", OpSet::PUT, 1),
            (".cairnflow-fence-probe/", OpSet::GET, 0),
            (".cairnflow-cas-probe/", OpSet::DELETE, 0),
            (".cairnflow-cas-probe/", OpSet::PUT, 0),
            (".cairnflow-cas-probe/", OpSet::PUT, 1),
            (".cairnflow-cas-probe/", OpSet::PUT, 2),
        ] {
            let gate = |retrying: bool| {
                let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
                faulty.add_rule(Rule::new(
                    ops,
                    PathMatch::Contains(probe.into()),
                    Action::FailNth { skip },
                ));
                let stores = ObjectStores::single(super::super::StoreLocation::from_store(
                    faulty,
                    "p",
                    "test://faulty",
                ));
                let p = if retrying { policy } else { ONCE };
                async move {
                    if probe.contains("fence") {
                        stores.assert_conditional_put_within(&p).await
                    } else {
                        stores.assert_compare_and_swap_within(&p).await
                    }
                }
            };
            let what = format!("{probe} {ops:?} skip {skip}");
            match gate(false).await {
                Ok(()) | Err(StoreError::Io(_)) => {}
                Err(e) => panic!("{what}: transient error reported as a broken store: {e}"),
            }
            gate(true)
                .await
                .unwrap_or_else(|e| panic!("{what}: still fails after retrying: {e}"));
        }
    }
}
