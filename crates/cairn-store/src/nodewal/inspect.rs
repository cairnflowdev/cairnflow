// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! GC floors of every shared WAL log, read **offline** from the bucket — for `cairn-ctl
//! wal floors`. GET/LIST only; seals nothing, cleans nothing. Same rule for "which range
//! pins the floor" as the node's combined GC loop ([`super::stored_refs`] +
//! [`super::contains`]), but the references are read from the **stored** manifest — an open
//! range may already have flushed further than that.

use {
    super::{contains, format::Decoded, list_dirs, log::LogStore, stored_refs, Refs},
    crate::entropy::HashMap,
    crate::{
        config::ObjectStores,
        error::Result,
        owner::{LogId, SessionId},
        session::SessionState,
    },
};

/// One shared WAL log as the bucket sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OfflineLog {
    pub log: LogId,
    /// Session state of the incarnation writing the log; `None` = the session record has
    /// been cleaned up (or is damaged).
    pub session_state: Option<SessionState>,
    /// The recorded GC floor (`floor/<f>`).
    pub floor: u64,
    /// Number of contiguous data objects from the floor up.
    pub objects: u64,
    /// Seal position, if the log has been sealed.
    pub sealed_at: Option<u64>,
    /// The first range whose reference holds an object from the floor up — where the next
    /// GC round will stop — with the frame's wal id.
    pub pinned_by: Option<(u32, u64)>,
    /// The log has been purged (`forgotten` mark, `LogStore::forget`): only the seal and the
    /// mark remain, so it still shows up in the list with floor 0 — do not read its floor as
    /// a stuck floor.
    pub forgotten: bool,
}

/// Every log under `nodewal/`, by node, incarnation, stream.
pub async fn log_floors(stores: &ObjectStores) -> Result<Vec<OfflineLog>> {
    let wal_store = stores.wal().store();
    let wal_prefix = stores.main().prefix().clone();
    let root = super::log::nodewal_root(&wal_prefix);
    let main = stores.main();
    let mut refs: HashMap<u32, Refs> = HashMap::default();
    let mut out = Vec::new();
    for node in list_dirs(wal_store, &root).await? {
        for inc in list_dirs(wal_store, &format!("{root}/{node}")).await? {
            let Ok(inc) = inc.parse::<u64>() else {
                continue;
            };
            let session = SessionId {
                node: node.clone(),
                inc,
            };
            let session_state =
                match crate::session::read_session(main.store(), main.prefix(), &session).await? {
                    Some((Ok(rec), _)) => Some(rec.state),
                    _ => None,
                };
            for stripe in list_dirs(wal_store, &format!("{root}/{node}/{inc}")).await? {
                let Ok(stripe) = stripe.parse::<u16>() else {
                    continue;
                };
                let log = LogId {
                    session: session.clone(),
                    stripe,
                };
                let store = LogStore::new(std::sync::Arc::clone(wal_store), &wal_prefix, &log);
                let floor = store.floor().await?;
                let mut at = floor;
                let mut sealed_at = None;
                let mut pinned_by = None;
                loop {
                    match store.get(at).await? {
                        Some(Decoded::Data(d)) => {
                            // The first object still holding a referenced frame is where
                            // the next GC round will stop.
                            if pinned_by.is_none() {
                                for f in &d.frames {
                                    if let std::collections::hash_map::Entry::Vacant(slot) =
                                        refs.entry(f.range)
                                    {
                                        // A reference that cannot be read counts as the
                                        // range holding everything (conservative).
                                        let r = stored_refs(stores, f.range).await.unwrap_or_else(
                                            |_| {
                                                vec![slatedb::wal::WalFileRange(
                                                    std::ops::Bound::Unbounded,
                                                    std::ops::Bound::Unbounded,
                                                )]
                                            },
                                        );
                                        slot.insert(r);
                                    }
                                    if refs[&f.range].iter().any(|r| contains(r, f.wal_file_id)) {
                                        pinned_by = Some((f.range, f.wal_file_id));
                                        break;
                                    }
                                }
                            }
                            at += 1;
                        }
                        Some(Decoded::Seal { .. }) => {
                            sealed_at = Some(at);
                            break;
                        }
                        None => break,
                    }
                }
                let forgotten = store.forgotten().await?;
                out.push(OfflineLog {
                    log,
                    session_state,
                    floor,
                    objects: at - floor,
                    sealed_at,
                    pinned_by,
                    forgotten,
                });
            }
        }
    }
    Ok(out)
}

/// An incarnation with a shared WAL log directory on the bucket, as the scanner sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IncarnationLogs {
    pub session: SessionId,
    /// `Ok(None)` = the session record has been cleaned up (only ended records are cleaned
    /// up); `Err` = present but unreadable.
    pub state: std::result::Result<Option<SessionState>, ()>,
    /// Number of objects under `nodewal/<node>/<inc>/` (data, seal, floor), **excluding**
    /// purged logs (carrying the `forgotten` mark: only the seal and the mark remain — see
    /// `LogStore::forget`). Only counted for incarnations that are **not** `live` — a live
    /// log always has objects, counting them costs useless LISTs; `None` for a live
    /// incarnation.
    pub objects: Option<u64>,
}

/// Result of [`incarnation_logs`]: the logs that were read, and the skipped entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IncarnationScan {
    pub logs: Vec<IncarnationLogs>,
    /// LIST `nodewal/<node>`, GET of a session record or LIST of an incarnation's logs that
    /// failed after [`crate::inspect::SCAN_RETRY`].
    pub skipped: Vec<crate::inspect::ScanSkip>,
}

/// Every incarnation with logs under `nodewal/` — for the state scanner (the "shared WAL
/// log nobody holds" check). Cheaper than [`log_floors`]: one LIST per node, one GET of the
/// session record per incarnation, and one LIST (count) for each dead incarnation; no GET
/// of any log object.
///
/// **Tolerates per-entry failures**: every read retries with a bound
/// ([`crate::inspect::scan_read`]); a failed LIST of `nodewal/` is an error, a failed node
/// or incarnation goes into [`IncarnationScan::skipped`] and the scan goes on.
pub async fn incarnation_logs(stores: &ObjectStores) -> Result<IncarnationScan> {
    use {
        crate::inspect::{scan_read, ScanSkip},
        futures::TryStreamExt,
    };
    let wal_store = stores.wal().store();
    let root = super::log::nodewal_root(stores.main().prefix());
    let main = stores.main();
    let mut out = IncarnationScan::default();
    let skip = |item: String, session: Option<SessionId>, error: String| ScanSkip {
        item,
        range: None,
        session,
        error,
    };
    for node in scan_read(|| list_dirs(wal_store, &root)).await? {
        let node_dir = format!("{root}/{node}");
        let incs = match scan_read(|| list_dirs(wal_store, &node_dir)).await {
            Ok(i) => i,
            Err(e) => {
                out.skipped.push(skip(node_dir, None, e.to_string()));
                continue;
            }
        };
        for inc in incs {
            let Ok(inc) = inc.parse::<u64>() else {
                continue;
            };
            let session = SessionId {
                node: node.clone(),
                inc,
            };
            let read =
                scan_read(|| crate::session::read_session(main.store(), main.prefix(), &session))
                    .await;
            let state = match read {
                Ok(Some((Ok(rec), _))) => Ok(Some(rec.state)),
                Ok(Some((Err(_), _))) => Err(()),
                Ok(None) => Ok(None),
                Err(e) => {
                    out.skipped.push(skip(
                        format!("sessions/{node}/{inc}"),
                        Some(session),
                        e.to_string(),
                    ));
                    continue;
                }
            };
            let objects = if state == Ok(Some(SessionState::Live)) {
                None
            } else {
                let dir = slatedb::object_store::path::Path::from(format!("{root}/{node}/{inc}"));
                let listed: Vec<_> =
                    match scan_read(|| wal_store.list(Some(&dir)).try_collect::<Vec<_>>()).await {
                        Ok(l) => l,
                        Err(e) => {
                            out.skipped
                                .push(skip(dir.to_string(), Some(session), e.to_string()));
                            continue;
                        }
                    };
                // `<stripe>/…` of each object, relative to the incarnation directory.
                let stripe_of = |m: &slatedb::object_store::ObjectMeta| {
                    m.location
                        .as_ref()
                        .strip_prefix(dir.as_ref())
                        .and_then(|r| r.strip_prefix('/'))
                        .and_then(|r| r.split_once('/'))
                        .map(|(stripe, name)| (stripe.to_string(), name.to_string()))
                };
                let forgotten: std::collections::BTreeSet<String> = listed
                    .iter()
                    .filter_map(stripe_of)
                    .filter(|(_, name)| name == super::log::FORGOTTEN)
                    .map(|(stripe, _)| stripe)
                    .collect();
                Some(
                    listed
                        .iter()
                        .filter(|m| stripe_of(m).is_none_or(|(st, _)| !forgotten.contains(&st)))
                        .count() as u64,
                )
            };
            out.logs.push(IncarnationLogs {
                session,
                state,
                objects,
            });
        }
    }
    Ok(out)
}
