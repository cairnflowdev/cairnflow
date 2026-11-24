// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The incarnation marker `incarnations/<node>/<inc>`.
//!
//! `inc` is the identity of one run of a session: owner records and the shared WAL log
//! (`nodewal/<node>/<inc>/…`) carry it, so it must **never** be reused for the same node.
//! The record `sessions/<node>/<inc>` is not enough as proof of "used": an ended session is
//! cleaned up after `GC_AFTER_TTLS` TTLs, while the log's seal/floor stay. Before markers
//! existed, the remaining lower bound was the wall clock (`inc ≥ unix millis`) — a clock
//! going backwards meant a log name collision and lost acked rows.
//!
//! The marker is a small object, created with `Create` **before** `inc` is used:
//!
//! - Claiming: `Create` wins (or is ambiguous and the re-read finds exactly our content —
//!   rule 7; the content carries a nonce of this open) ⇒ `inc` is ours. `AlreadyExists`
//!   and not ours ⇒ try `inc + 1`.
//! - Lower bound: LIST `incarnations/<node>/` ⇒ the new `inc` is greater than every marker
//!   still on the bucket, whatever the clock.
//! - Read leniently: on an old bucket with no markers the old lower bound
//!   `max(listed + 1, now_ms)` still applies — it only grows, nothing of an old bucket
//!   changes.
//! - Cleanup: once the session of the new `inc` is open, the **smaller** markers of the same
//!   node are deleted. Provable: every later open LISTs and sees the new marker (S3-1), so it
//!   picks an `inc` greater than it, hence greater than every deleted marker; the new
//!   marker is only deleted once an even greater marker exists. No deletion by age, the
//!   largest marker is never deleted. A marker is not customer data (it does not go through
//!   the trash); a failed delete only leaves small garbage that the next open cleans up.

use super::*;

/// Header of the marker.
const MARKER_HEADER: &str = "cairnflow-incarnation/1";

pub(super) fn incarnations_dir(prefix: &Path, node: &str) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from(format!("incarnations/{node}"))
    } else {
        Path::from(format!("{}/incarnations/{node}", prefix.as_ref()))
    }
}

pub(super) fn marker_path(prefix: &Path, id: &SessionId) -> Path {
    Path::from(format!(
        "{}/{}",
        incarnations_dir(prefix, &id.node).as_ref(),
        id.inc
    ))
}

/// The `inc` of a marker of `node`, if `location` is indeed a marker of that node (node
/// ids may contain `/`: `incarnations/a/…` also lists the markers of node `a/1/2` — a
/// remainder that is not a number is skipped).
pub(super) fn marker_inc(prefix: &Path, node: &str, location: &Path) -> Option<u64> {
    let dir = format!("{}/", incarnations_dir(prefix, node).as_ref());
    location.as_ref().strip_prefix(&dir)?.parse().ok()
}

/// Marker content: the node and a nonce of this open — unique enough for `put_resolving`'s
/// comparison to be meaningful (two opens of the same node never write the same thing).
pub(super) fn marker_body(node: &str) -> bytes::Bytes {
    let mut nonce = [0u8; 8];
    crate::entropy::fill(&mut nonce);
    bytes::Bytes::from(format!(
        "{MARKER_HEADER}\nnode={node}\nnonce={:016x}\n",
        u64::from_le_bytes(nonce)
    ))
}

/// Deletes the markers `older` (all smaller than `kept`, the marker just claimed) — best
/// effort, bounded. See the module header: the `kept` marker holds the lower bound for
/// every later open.
pub(super) async fn forget_older(inner: &Inner, kept: &SessionId, older: Vec<u64>) {
    for inc in older {
        debug_assert!(inc < kept.inc);
        let id = SessionId {
            node: kept.node.clone(),
            inc,
        };
        let path = marker_path(&inner.prefix, &id);
        match crate::retry::deadline(
            &crate::retry::ops::SESSION_MARKER,
            RELEASE_WRITE_BOUND,
            inner.store.delete(&path),
        )
        .await
        {
            Ok(Ok(()) | Err(OsError::NotFound { .. })) => {}
            Ok(Err(e)) => {
                tracing::debug!(marker = %id, error = %e, "could not clean up an old incarnation marker")
            }
            Err(_) => tracing::debug!(marker = %id, "old incarnation marker cleanup timed out"),
        }
    }
}
