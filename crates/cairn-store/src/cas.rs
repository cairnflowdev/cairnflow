// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Resolves the **ambiguous outcome** of a conditional write (AGENTS.md section 1, rule 7; for
//! the range table this is SM-2).
//!
//! An `Update`/`Create` PUT may **already have been applied by the store** without the caller
//! knowing:
//!
//! - object_store 0.14 (`client/retry.rs`) retries every 5xx by itself, conditional PUTs
//!   included. The first attempt may already have written (S3 says so explicitly: a 500 "may or may not
//!   have succeeded"), the retry sends the identical request and gets `412`/`409` — the caller
//!   sees `Precondition`/`AlreadyExists` while the object on the store **is its own**;
//! - an error after the write landed (timeout, connection dropped while reading the reply):
//!   `Err`.
//!
//! Treating both as "lost" leaves the coordinator's own intent stuck on the table, and makes the
//! keeper fail-stop the whole node over a spurious `LeaseLost`. The rule: an ambiguous outcome
//! is resolved by **reading back** and comparing the content with what we meant to write — no
//! guessing.
//!
//! **Limit of the content comparison:** it proves "the object carries exactly what we meant to
//! write", not "we are the ones who wrote it". The caller must guarantee that nobody else can
//! write **exactly the same** content in the same slot: an owner record carries session + term
//! (unique per claim), a range-table intent carries the child id allocated from
//! `next_range_id`, and a node's coordinator never runs two intents on the same range
//! (`ActiveIntents`).

use {
    bytes::Bytes,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
        UpdateVersion,
    },
    std::sync::Arc,
};

/// Result of [`put_resolving`].
#[derive(Clone, Debug)]
pub enum Cas {
    /// The object on the store is what we meant to write; the version is for the next CAS.
    Written(UpdateVersion),
    /// Someone changed the object before us (or it vanished between the read and the write).
    Lost,
}

/// Conditional PUT with `mode`, resolving ambiguity by reading back:
///
/// - `Ok` ⇒ [`Cas::Written`];
/// - `NotFound` (an `Update` on an object that has vanished) ⇒ [`Cas::Lost`]: none of our
///   attempts could have created that object;
/// - `Precondition`/`AlreadyExists` ⇒ read back: same content ⇒ `Written` (the first attempt
///   wrote, the retry saw itself), different ⇒ `Lost`;
/// - any other error ⇒ read back: same content ⇒ `Written`; otherwise return the **original
///   error** — we do not know whether the write landed (if the object is still at the old
///   version we may still win on the next attempt). A failed read-back also returns the
///   original error.
pub async fn put_resolving(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    body: Bytes,
    mode: PutMode,
) -> std::result::Result<Cas, OsError> {
    resolve(store, path, body, mode, false).await
}

/// Like [`put_resolving`], for the cluster's **read-modify-write** objects (rule book, catalog,
/// keys, tenants, …): a store that cannot do CAS on an etag (`LocalFileSystem`, the
/// `--data-dir` path) ⇒ overwrite and treat it as `Written`.
///
/// Overwriting is safe **precisely because** the lack of CAS already prevented forming a
/// cluster (D12): there is no second node to race with. Not for objects whose overwrite could
/// break a fence (owner record, session, shared WAL log) — those only exist in a cluster.
pub async fn put_resolving_or_overwrite(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    body: Bytes,
    mode: PutMode,
) -> std::result::Result<Cas, OsError> {
    resolve(store, path, body, mode, true).await
}

async fn resolve(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    body: Bytes,
    mode: PutMode,
    overwrite: bool,
) -> std::result::Result<Cas, OsError> {
    let err = match store
        .put_opts(path, PutPayload::from(body.clone()), PutOptions::from(mode))
        .await
    {
        Ok(res) => return Ok(Cas::Written(res.into())),
        Err(OsError::NotFound { .. }) => return Ok(Cas::Lost),
        Err(OsError::NotImplemented { .. } | OsError::NotSupported { .. }) if overwrite => {
            tracing::debug!(path = %path, "store cannot do CAS — overwriting (single process)");
            let res = store
                .put_opts(
                    path,
                    PutPayload::from(body),
                    PutOptions::from(PutMode::Overwrite),
                )
                .await?;
            return Ok(Cas::Written(res.into()));
        }
        Err(e) => e,
    };
    let lost_unless_ours = matches!(
        err,
        OsError::Precondition { .. } | OsError::AlreadyExists { .. }
    );
    match read_back(store, path).await {
        Ok(Some((now, version))) if now == body => {
            tracing::debug!(path = %path, error = %err, "ambiguous CAS: read-back shows what we wrote — we won");
            Ok(Cas::Written(version))
        }
        Ok(_) if lost_unless_ours => Ok(Cas::Lost),
        _ => Err(err),
    }
}

async fn read_back(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
) -> std::result::Result<Option<(Bytes, UpdateVersion)>, OsError> {
    let got = match store.get(path).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    Ok(Some((got.bytes().await?, version)))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::testkit::{Action, Ambiguity, FaultyStore, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    fn rule(reply: Ambiguity) -> Rule {
        Rule::new(
            OpSet::PUT,
            PathMatch::Any,
            Action::Ambiguous {
                reply,
                skip: 0,
                times: Some(1),
            },
        )
    }

    /// The write reached the store but the reply says otherwise: reading back ⇒ `Written`, and
    /// the returned version is usable for the next CAS.
    #[tokio::test]
    async fn ambiguous_write_that_landed_is_written() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            let p = Path::from("x");
            faulty.add_rule(rule(reply));
            let Cas::Written(v) =
                put_resolving(&store, &p, Bytes::from_static(b"a"), PutMode::Create)
                    .await
                    .unwrap()
            else {
                panic!("{reply:?}: Create wrote but came out Lost");
            };
            faulty.add_rule(rule(reply));
            let Cas::Written(v) =
                put_resolving(&store, &p, Bytes::from_static(b"b"), PutMode::Update(v))
                    .await
                    .unwrap()
            else {
                panic!("{reply:?}: Update wrote but came out Lost");
            };
            let Cas::Written(_) =
                put_resolving(&store, &p, Bytes::from_static(b"c"), PutMode::Update(v))
                    .await
                    .unwrap()
            else {
                panic!("{reply:?}: version after resolving the ambiguity is unusable");
            };
        }
    }

    /// A real loss is still `Lost`; an error while the object is still at the old version is an
    /// error (unknown outcome).
    #[tokio::test]
    async fn real_loss_is_lost_and_unknown_stays_an_error() {
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let p = Path::from("x");
        let Cas::Written(v) = put_resolving(&store, &p, Bytes::from_static(b"a"), PutMode::Create)
            .await
            .unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            put_resolving(&store, &p, Bytes::from_static(b"z"), PutMode::Create)
                .await
                .unwrap(),
            Cas::Lost
        ));
        store
            .put(&p, PutPayload::from_static(b"other"))
            .await
            .unwrap();
        assert!(matches!(
            put_resolving(
                &store,
                &p,
                Bytes::from_static(b"b"),
                PutMode::Update(v.clone())
            )
            .await
            .unwrap(),
            Cas::Lost
        ));
        // Error before reaching the store: the object is unchanged ⇒ the original error.
        faulty.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Any,
            Action::Fail { times: Some(1) },
        ));
        assert!(
            put_resolving(&store, &p, Bytes::from_static(b"b"), PutMode::Update(v))
                .await
                .is_err()
        );
        // `Update` on an object that has vanished.
        let gone = Path::from("gone");
        let v = UpdateVersion {
            e_tag: Some("x".into()),
            version: None,
        };
        assert!(matches!(
            put_resolving(&store, &gone, Bytes::from_static(b"b"), PutMode::Update(v))
                .await
                .unwrap(),
            Cas::Lost
        ));
    }
}
