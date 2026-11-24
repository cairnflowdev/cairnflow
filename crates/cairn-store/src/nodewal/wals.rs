// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The shared WAL per **incarnation** of the node session: one [`NodeWal`] with its own
//! log per incarnation; when an incarnation ends its WAL stops, and when the WAL
//! fail-stops the incarnation is dropped ([`Session::fail_stop`]) — the node opens a new
//! incarnation, a new log, and takes its ranges back as usual.

use {
    super::{NodeWal, NodeWalConfig},
    crate::{
        config::ObjectStores,
        error::{Result, StoreError},
        owner::SessionId,
        session::Session,
    },
    std::sync::{Arc, Mutex},
};

struct Inner {
    stores: ObjectStores,
    cfg: NodeWalConfig,
    session: Session,
    current: Mutex<Option<NodeWal>>,
    follow: Mutex<Option<tokio::task::AbortHandle>>,
    /// Fail-stop watch task of the `current` WAL (see [`NodeWals::get`]).
    watch: Mutex<Option<tokio::task::AbortHandle>>,
}

impl Drop for Inner {
    /// Nobody holds `NodeWals` any more (the last engine has exited): stop the WAL so its
    /// background tasks do not live forever — but **do not** fail-stop the incarnation.
    /// Abort the watch task before stopping: a stream dying from `shutdown` is not the
    /// incarnation's fault, and the incarnation may still be alive (a graceful shutdown
    /// releases the last copy once every range has yielded, **before** `Session::close`).
    /// The earlier version let the watch task call `Session::fail_stop` ⇒ `lose` dropped the
    /// live incarnation and the renewal loop opened a new incarnation, racing with the
    /// `close()` that was cancelling it — a `Create` PUT reaching the store before
    /// `current` was written left a `live` session nobody renewed, and peers waited out its
    /// TTL.
    fn drop(&mut self) {
        if let Some(t) = self.follow.lock().expect("mutex").take() {
            t.abort();
        }
        if let Some(t) = self.watch.lock().expect("mutex").take() {
            t.abort();
        }
        if let Some(w) = self.current.lock().expect("mutex").take() {
            w.shutdown();
        }
    }
}

/// The [`NodeWal`] of the current incarnation of a [`Session`].
#[derive(Clone)]
pub struct NodeWals(Arc<Inner>);

impl NodeWals {
    /// Follows `session`: an incarnation lost ⇒ stop its WAL.
    pub fn follow(stores: &ObjectStores, session: &Session, cfg: NodeWalConfig) -> NodeWals {
        let inner = Arc::new(Inner {
            stores: stores.clone(),
            cfg,
            session: session.clone(),
            current: Mutex::new(None),
            follow: Mutex::new(None),
            watch: Mutex::new(None),
        });
        let weak = Arc::downgrade(&inner);
        let mut changes = session.live_changes();
        let task = tokio::spawn(crate::io_source::tagged("nodewal.watch", async move {
            loop {
                let live = changes
                    .borrow_and_update()
                    .as_ref()
                    .map(|(id, _)| id.clone());
                let Some(inner) = weak.upgrade() else { return };
                // The incarnation has ended: the watch task's `fail_stop` has nothing left
                // to drop — abort it, sparing a false "dropping incarnation" line. Take the
                // watch task **under the `current` lock** (lock order as in `get`): outside
                // the lock, `get` for a new incarnation could slip in and we would abort its
                // watch task by mistake.
                let stale = {
                    let mut cur = inner.current.lock().expect("mutex");
                    match cur.as_ref() {
                        Some(w) if Some(w.session()) != live.as_ref() => {
                            let watch = inner.watch.lock().expect("mutex").take();
                            cur.take().map(|w| (w, watch))
                        }
                        _ => None,
                    }
                };
                drop(inner);
                if let Some((w, watch)) = stale {
                    if let Some(t) = watch {
                        t.abort();
                    }
                    tracing::info!(session = %w.session(), "incarnation ended — stopping its shared WAL");
                    w.shutdown();
                }
                if changes.changed().await.is_err() {
                    return;
                }
            }
        }));
        *inner.follow.lock().expect("mutex") = Some(task.abort_handle());
        NodeWals(inner)
    }

    pub fn config(&self) -> &NodeWalConfig {
        &self.0.cfg
    }

    /// The WAL of incarnation `id` — built the first time it is asked for. `id` no longer
    /// the current incarnation ⇒ [`StoreError::LeaseLost`].
    pub fn get(&self, id: &SessionId) -> Result<NodeWal> {
        let mut cur = self.0.current.lock().expect("mutex");
        if let Some(w) = cur.as_ref() {
            if w.session() == id {
                return Ok(w.clone());
            }
        }
        if self.0.session.current().as_ref() != Some(id) {
            return Err(StoreError::LeaseLost);
        }
        if let Some(old) = cur.take() {
            // The old incarnation is gone: its watch task has nothing left to drop.
            if let Some(t) = self.0.watch.lock().expect("mutex").take() {
                t.abort();
            }
            old.shutdown();
        }
        let wal = NodeWal::start(&self.0.stores, id.clone(), self.0.cfg.clone());
        // WAL fail-stop ⇒ drop the incarnation. The task only holds the receiver: when the
        // WAL is dropped the sender goes with it and the task exits.
        let mut stopped = wal.fail_stopped();
        let session = self.0.session.clone();
        let sid = id.clone();
        // Only the cause cell, not the `watch` sender (sender dropped ⇒ task exits).
        let cause_cell = Arc::clone(&wal.0.failstop.1);
        let watch = tokio::spawn(async move {
            let reason = match stopped.wait_for(|v| v.is_some()).await {
                Ok(v) => v.clone().unwrap_or_default(),
                Err(_) => return,
            };
            // The cause is set before the reason (`FailStop::trigger`), so it is already
            // here; `Other` is only a safety net.
            let cause = cause_cell
                .get()
                .copied()
                .unwrap_or(crate::failstop::Cause::Other);
            tracing::error!(session = %sid, cause = %cause, reason = %reason, "shared WAL fail-stop — dropping the incarnation");
            session.fail_stop(&sid, cause, "shared WAL fail-stop").await;
        });
        *self.0.watch.lock().expect("mutex") = Some(watch.abort_handle());
        *cur = Some(wal.clone());
        Ok(wal)
    }
}
