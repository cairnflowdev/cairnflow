// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Range owner change notices** (layer 3 of LS-6).
//!
//! The old owner of a range must notice by itself that the range has passed into someone
//! else's hands when both its clock (layer 1) and its renewal loop (layer 2) fall short —
//! a node that froze and woke up, a clock that stood still. It used to notice through a
//! LIST of `owners/` every TTL on every node holding ranges. Now **the taker** tells it:
//! after every winning take CAS on the record of another session that did not hand over
//! ([`cairn_store::Session::on_owner_taken`]), the node pushes a
//! `POST /internal/owner/changed` (signed with the cluster secret, through
//! [`crate::auth::ClusterSecret::send`]) to the internal port the old record advertises
//! (`address=`). The receiver GETs exactly that record
//! ([`cairn_store::Session::owner_changed`]) and revokes its grant if it is no longer its own.
//! If the notice gets lost, the coordinator's `view` ([`crate::control`]) leads to the same
//! GET; the net is one conditional GET per range every [`cairn_store::DEFAULT_OWNER_WATCH`].
//!
//! Best-effort: the notice goes out in a background task, a failure is only logged at debug
//! (if the old owner is really dead nobody listens). No network call on the path of `claim`
//! — it runs in the engine factory (D16) — only a `spawn`. The notice is not trusted: the
//! receiver reads the bucket itself (LS-3), so a forged notice only costs one GET.

use {
    serde::{Deserialize, Serialize},
    std::time::Duration,
};

/// Limit on one notice: if the old owner hangs, give up, the net takes care of it.
const NOTICE_TIMEOUT: Duration = Duration::from_millis(500);

/// Body of `POST /internal/owner/changed`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct OwnerChanged {
    /// The range that just changed owner.
    pub range: u32,
    /// Term of the new owner record — only for logging.
    pub term: u64,
}

/// Install the listener for range takes of `session`: each take from another session
/// notifies the old owner (if the old record advertises an address) on the current runtime.
pub fn install(session: &cairn_store::Session, secret: &crate::auth::ClusterSecret) {
    let secret = secret.clone();
    let client = reqwest::Client::new();
    let runtime = tokio::runtime::Handle::current();
    let me = session.node_id().to_string();
    session.on_owner_taken(move |taken| {
        let Some(addr) = taken.address.clone() else {
            return;
        };
        let (secret, client, me) = (secret.clone(), client.clone(), me.clone());
        let body = OwnerChanged {
            range: taken.range.0,
            term: taken.term,
        };
        runtime.spawn(async move {
            let req = client
                .post(format!(
                    "{}/internal/owner/changed",
                    addr.trim_end_matches('/')
                ))
                .timeout(NOTICE_TIMEOUT)
                .json(&body);
            match secret.send(&client, req).await {
                Ok(r) if r.status().is_success() => {
                    tracing::debug!(range = body.range, old_owner = %taken.from, node = %me, "old owner notified: range changed owner")
                }
                Ok(r) => {
                    tracing::debug!(range = body.range, old_owner = %taken.from, status = %r.status(), "old owner did not accept the owner change notice")
                }
                Err(e) => {
                    tracing::debug!(range = body.range, old_owner = %taken.from, error = %e, "cannot notify the old owner — the owner record watch net takes care of it")
                }
            }
        });
    });
}
