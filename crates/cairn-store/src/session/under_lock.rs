// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Expiring old sessions **by proof of the lock**.

use super::*;

/// Marks `expired` every session still `live` on the bucket, at once — without waiting
/// out a silent TTL like [`SessionTracker`].
///
/// Callable only with a [`crate::local::LocalLock`] in hand: the exclusive lock on the
/// store directory proves no other process is running on the bucket, so every session
/// still `live` belongs to an earlier run that **has died** (the operating system releases
/// the lock when the process dies, `kill -9` included). The premise of expiry — every
/// grant of the owner has lapsed before the `expired` CAS — holds because the owner no
/// longer runs, rather than because of the clock. Call it **before**
/// [`Session::establish`]: no session of this very process is touched.
///
/// Without it a node on `--data-dir` restarting after `kill -9` would wait one TTL (10 s
/// by default) before it could claim its own ranges again — whereas a single-process node
/// used to wait for nothing.
///
/// CAS on the version just read, through [`crate::cas::put_resolving`] (rule 7). An
/// unreadable record ⇒ write a minimal `expired` record (like the tracker). A lost CAS ⇒
/// skip that session: the tracker handles it after one TTL as usual. Returns the sessions
/// marked.
pub async fn expire_sessions_under_lock(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    _proof: &crate::local::LocalLock,
) -> Result<Vec<SessionId>> {
    let dir = Path::from(sessions_dir(prefix));
    let listed: Vec<_> = store.list(Some(&dir)).try_collect().await.map_err(io)?;
    let mut ids: Vec<SessionId> = listed
        .iter()
        .filter_map(|m| session_id_of(prefix, &m.location))
        .collect();
    ids.sort_by(|a, b| (&a.node, a.inc).cmp(&(&b.node, b.inc)));
    let mut expired = Vec::new();
    for id in ids {
        let Some((rec, version)) = read_session(store, prefix, &id).await? else {
            continue;
        };
        let rec = match rec {
            Ok(r) if r.state.is_terminal() => continue,
            Ok(r) => SessionRecord {
                state: SessionState::Expired,
                ..r
            },
            Err(_) => SessionRecord {
                node: id.node.clone(),
                beat: 0,
                address: None,
                ingress: None,
                knows: None,
                slow: None,
                p99_put_ms: None,
                draining: false,
                owns: BTreeMap::new(),
                state: SessionState::Expired,
                mode: SessionMode::Direct,
                challenged: None,
            },
        };
        match crate::cas::put_resolving(
            store,
            &session_path(prefix, &id),
            rec.encode().into(),
            PutMode::Update(version),
        )
        .await
        {
            Ok(crate::cas::Cas::Written(_)) => {
                tracing::info!(session = %id, "session of an earlier run (the directory lock proves it died) — marking it expired");
                expired.push(id);
            }
            Ok(crate::cas::Cas::Lost) => {
                tracing::warn!(session = %id, "could not mark the old session expired (CAS lost) — waiting for the tracker after one TTL");
            }
            Err(e) => return Err(io(e)),
        }
    }
    Ok(expired)
}
