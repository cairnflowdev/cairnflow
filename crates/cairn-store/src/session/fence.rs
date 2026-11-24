// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The read fence on the leader.**
//!
//! A writer's `alive()` trusts the node's clock (`Instant`): the grant is valid until
//! `t_k + ⅔·TTL`. That clock lies when the machine sleeps or the VM is paused
//! (`CLOCK_MONOTONIC` does not run), or runs slower than 1.5× the observer's clock: the
//! node wakes up still believing it leads while the session has already been CASed to
//! `expired` and the range has gone to someone else — its read answers are stale
//! (`failover.rs::leader_waking_after_losing_its_range_never_serves_a_stale_read`). Writes
//! do not need this fence: a zombie's write is stopped by `writer_epoch`/the log seal at
//! the durability wait (LS-4).
//!
//! The fence: before returning an answer on the leader, a conditional GET
//! (`If-None-Match` = the etag of the last renewal) of the session record of **this very**
//! incarnation, issued **after** the read. A range only changes hands through a session
//! record that has ended (`expired`/`closed`, or gone) or through our own release (LS-3),
//! so a GET that sees our record still `live` at a time `G` after the read proves nobody
//! took the range before `G` — the read has not missed any acked write of a new owner.
//! `304` = unchanged since the last renewal; `200` ⇒ look at the content (our renewal has
//! just come back before the renewal loop received it, or an observer's overwrite — still
//! `live`): `live` + our node ⇒ pass. A store ignoring `If-None-Match` always returns `200`
//! — still correct, it only costs bytes.
//!
//! **One request per batch for the whole node.** Every answer waiting to be sent goes into
//! a channel; the background loop takes everything currently in the channel and issues one
//! GET for the whole batch. Those arriving while a GET is in flight wait for the next one —
//! their GET must be issued **after** their read. The turn's path only sends into the
//! channel and waits on a `oneshot`: no shared lock, no synchronous I/O (AGENTS §6).

use {
    super::*,
    slatedb::object_store::GetOptions,
    tokio::sync::{mpsc, oneshot},
};

/// An answer waiting for the fence: the grant's incarnation and where to report the result.
pub(super) struct Request {
    id: SessionId,
    reply: oneshot::Sender<bool>,
}

/// Read fence counters — metrics `cairnflow_read_fence_*`.
pub mod stats {
    use std::sync::atomic::AtomicU64;

    /// Answers that went through the fence (each answer once).
    pub static WAITERS: AtomicU64 = AtomicU64::new(0);
    /// Batches whose GET saw our record still `live`.
    pub static CONFIRMED: AtomicU64 = AtomicU64::new(0);
    /// Refused batches: the record has ended, is gone, is no longer ours, or the grant's
    /// incarnation is no longer the current incarnation.
    pub static REFUSED: AtomicU64 = AtomicU64::new(0);
    /// Undecided batches: the GET failed or timed out — the answers are refused.
    pub static FAILED: AtomicU64 = AtomicU64::new(0);
}

/// The fence of incarnation `id`, tied to its root validity: each call sends a request into
/// the background loop's channel and returns a future waiting for the result. The background
/// loop has stopped (session closed) ⇒ `false`.
pub(super) fn handle(tx: &mpsc::UnboundedSender<Request>, id: &SessionId) -> ReadFence {
    let tx = tx.clone();
    let id = id.clone();
    Arc::new(move || {
        let (reply, rx) = oneshot::channel();
        let sent = tx
            .send(Request {
                id: id.clone(),
                reply,
            })
            .is_ok();
        Box::pin(async move { sent && rx.await.unwrap_or(false) })
    })
}

/// The node's read fence loop — see the module header. It stands outside `tasks` like the
/// owner-record watch loop: `abort`/`CrashHandle` simulate the renewal loop dying, while the
/// fence must keep running when the node wakes up.
pub(super) async fn read_fence_loop(
    inner: std::sync::Weak<Inner>,
    mut rx: mpsc::UnboundedReceiver<Request>,
) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(more) = rx.try_recv() {
            batch.push(more);
        }
        let Some(inner) = inner.upgrade() else { return };
        stats::WAITERS.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        // Usually only one incarnation per batch; grants of a dropped incarnation (rare:
        // only when pinned) are split off.
        while let Some(id) = batch.first().map(|r| r.id.clone()) {
            let (mine, rest): (Vec<_>, Vec<_>) = batch.into_iter().partition(|r| r.id == id);
            batch = rest;
            let ok = confirm(&inner, &id).await;
            for r in mine {
                let _ = r.reply.send(ok);
            }
        }
    }
}

/// One GET for one batch: whether session record `id` is still our `live` record.
async fn confirm(inner: &Inner, id: &SessionId) -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    // In `solo` this GET is also a self-check GET: `304`, or `200` with exactly the content
    // we wrote ⇒ renewed from when it was issued (the **first** attempt — the earliest, so
    // safe even when only a later attempt gets an answer). In delegated mode this GET is the
    // self-check GET: it touches the bucket (no renewal — only the renewal ack), and if it
    // sees the record changed the keeper looks at it at once.
    let (etag, solo, delegated) = {
        let st = inner.state.lock().expect("mutex");
        match st.current.as_ref() {
            Some(c) if &c.id == id => (
                c.version.e_tag.clone(),
                c.solo.then(|| c.written.clone()),
                c.delegated.is_some().then(|| c.written.clone()),
            ),
            // A dropped incarnation: its grants answer nothing any more.
            _ => {
                stats::REFUSED.fetch_add(1, Relaxed);
                return false;
            }
        }
    };
    let path = session_path(&inner.prefix, id);
    let issued_at = Instant::now();
    let get = || {
        let opts = GetOptions {
            if_none_match: etag.clone(),
            ..Default::default()
        };
        let path = &path;
        async move {
            match inner.store.get_opts(path, opts).await {
                // Unchanged since our last renewal.
                Err(OsError::NotModified { .. }) => Ok(None),
                Ok(r) => r.bytes().await.map(Some),
                Err(e) => Err(e),
            }
        }
    };
    // Bound: one third of the TTL — any longer and the answer would be late anyway.
    // Within that bound a transient GET failure is **retried**: each attempt is issued
    // after the read of every answer in the batch, so any attempt that sees our `live` is
    // as much proof as the first. Without retries one 5xx (the `s3-standard-faults*`
    // profiles: ~0.3 % of GETs) refused the whole batch — and `/invocations?scope=cluster`,
    // one fence per range, almost always missed one range at a few hundred ranges (seen in
    // a 4 h soak). `NotFound` is an answer, not retried.
    let bound = inner.cfg.ttl / 3;
    let policy = crate::retry::Policy::for_duration(
        Duration::from_millis(20),
        Duration::from_millis(200),
        bound,
    )
    .with_attempt_bound(bound / 2);
    let verdict = match crate::retry::run(
        &crate::retry::ops::SESSION_READ_FENCE,
        &policy,
        crate::retry::Never,
        get,
    )
    .await
    {
        Ok(None) => {
            if solo.is_some() {
                super::renew::solo_extend(inner, id, etag.as_deref(), issued_at);
            }
            super::delegate::touch(inner, id, etag.as_deref(), issued_at);
            Some(true)
        }
        Ok(Some(raw)) => {
            let rec = SessionRecord::decode(&raw);
            if let Some(written) = &delegated {
                if !rec.as_ref().is_ok_and(|r| r == written) {
                    *inner.solo_poke.lock().expect("mutex") = true;
                    inner.wake.notify_one();
                }
            }
            if let Some(written) = &solo {
                match &rec {
                    // The etag changed but the content is ours: the renewal loop rechecks.
                    Ok(r) if r == written => {}
                    // Challenged, overwritten, damaged: the renewal loop answers at once.
                    _ => {
                        *inner.solo_poke.lock().expect("mutex") = true;
                        inner.wake.notify_one();
                    }
                }
            }
            Some(rec.is_ok_and(|r| r.state == SessionState::Live && r.node == inner.cfg.node_id))
        }
        Err(OsError::NotFound { .. }) => Some(false),
        Err(e) => {
            tracing::debug!(session = %id, error = %e, "read fence: session GET failed");
            None
        }
    };
    match verdict {
        Some(true) => {
            stats::CONFIRMED.fetch_add(1, Relaxed);
            true
        }
        Some(false) => {
            stats::REFUSED.fetch_add(1, Relaxed);
            tracing::warn!(session = %id, "read fence: session no longer live — refusing the answer, nudging the renewal loop");
            // The renewal loop will miss its CAS and `lose`.
            inner.wake.notify_one();
            false
        }
        None => {
            stats::FAILED.fetch_add(1, Relaxed);
            false
        }
    }
}
