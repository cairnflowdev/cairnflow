// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `GET /invocations/{id}/streams/{name}` — reading a stream over SSE, **only on the
//! leader**.
//!
//! A node not leading the range returns `503` with `leader` and does **not** open a
//! follower reader (AP-13): a reader kept alive by SSE is ~10 requests per poll cycle
//! times nodes × ranges. A request asking for forwarding (`x-cairn-forward: leader` —
//! the console) makes the receiving node open the SSE **on the leader** through
//! [`super::forward`] and copy the body verbatim: still one reader, on the leader;
//! `moved`, `Last-Event-ID` and keepalives pass through as when called directly.
//!
//! Events:
//!
//! | `event` | `data` |
//! |---|---|
//! | `record` | `{offset, durable, step, attempt, k, data}` — `id:` is the offset |
//! | `gap` | `{offset, dropped}` — records dropped by the cap, or overwritten for a slow reader |
//! | `superseded` | `{offset, step, attempt}` — that attempt was replaced by a new attempt |
//! | `end` | `{next}` — the stream is sealed and fully read; the server closes |
//! | `moved` | `{leader}` — the leader's term ended; the client asks again at `leader` |
//! | `reset` | `{}` — after `moved`, transient streams only: what was shown is gone everywhere |
//!
//! The `durable` of a `record` belongs **to the record**, not to the stream: `true` only
//! once the record is durable (AP-12). Transient streams are always `false`.
//!
//! **Durable streams.** The part that has left the hub's RAM — durable, so it can be read
//! again — and streams this term has never seen (a leader change, an invocation finished
//! earlier) are read from the Db **on the leader** (`Command::ReadStream`), then
//! continued from the hub. A durable stream lives until purge: no `410` while the Db
//! still has its chunks.
//!
//! Resuming: `?from=<offset>` or the `Last-Event-ID` header (continues from id + 1).
//! Each connection lives at most `StreamConfig::max_life`; the node has a connection cap
//! (`429` + `Retry-After`), and a `:keepalive` every `StreamConfig::keepalive`.

use {
    super::*,
    crate::streams::{Body, RangeStreams},
    axum::response::sse::{Event, KeepAlive, Sse},
    std::{
        collections::{BTreeMap, VecDeque},
        convert::Infallible,
        sync::atomic::Ordering,
    },
};

/// Maximum records copied out in one hold of the hub's lock.
const CHUNK: usize = 256;
/// Maximum chunk value bytes per Db read for one reader.
const DB_PAGE: usize = 256 << 10;
/// A reader of a stream that exists only in the Db (the hub does not see it) asks again
/// after this long once it has read everything and the invocation is not finished —
/// the hub signals nothing for a stream it does not have.
const DB_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// Holds one slot in the node's SSE cap; returned when the connection closes.
struct SseSlot(Arc<std::sync::atomic::AtomicUsize>);

impl SseSlot {
    fn take(st: &AppState) -> Option<Self> {
        let max = st.streams.sse_max;
        st.sse_open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()?;
        crate::metrics::agent::bump(&crate::metrics::agent::SSE_OPEN);
        Some(Self(Arc::clone(&st.sse_open)))
    }
}

impl Drop for SseSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
        crate::metrics::agent::sub(&crate::metrics::agent::SSE_OPEN, 1);
    }
}

/// The number of long-lived connections open per tenant on the node.
///
/// A per-tenant cap instead of a global accept cap: behind the platform's proxy every
/// connection comes from the same few addresses, so a shared cap is a shared bucket — a
/// tenant opening many SSEs would make the whole cell stop accepting. The mutex is held
/// only while adding to/subtracting from one cell, when a connection opens and closes —
/// never on the turn path.
#[derive(Default)]
pub(super) struct TenantOpen(std::sync::Mutex<BTreeMap<cairn_store::TenantId, usize>>);

/// Holds one slot in a tenant's long-lived connection cap; returned on drop (the SSE
/// connection closes, attach answers, or the client goes away).
pub(super) struct TenantSlot {
    open: Arc<TenantOpen>,
    tenant: cairn_store::TenantId,
}

impl TenantSlot {
    /// Takes a slot for `tenant`; out of slots ⇒ `None` (the caller returns
    /// [`too_many_open_streams`]). Cap `0` ⇒ no cap.
    pub(super) fn take(st: &AppState, tenant: &cairn_store::TenantId) -> Option<Self> {
        Self::take_in(&st.tenant_open, tenant, st.streams.tenant_max)
    }

    /// Like [`Self::take`], in the `open` book with cap `max` (`0` ⇒ no cap) — for other
    /// counts of the same shape (`proxy` segment reads).
    pub(super) fn take_in(
        open_by: &Arc<TenantOpen>,
        tenant: &cairn_store::TenantId,
        max: usize,
    ) -> Option<Self> {
        {
            let mut open = open_by.0.lock().unwrap_or_else(|p| p.into_inner());
            let n = open.get(tenant).copied().unwrap_or(0);
            if max > 0 && n >= max {
                return None;
            }
            open.insert(tenant.clone(), n + 1);
        }
        Some(Self {
            open: Arc::clone(open_by),
            tenant: tenant.clone(),
        })
    }
}

/// `429 too_many_open_streams` with `Retry-After`: the tenant is out of long-lived
/// connection slots on the node.
pub(super) fn too_many_open_streams(st: &AppState) -> Response {
    let mut resp = ApiError::coded(
        StatusCode::TOO_MANY_REQUESTS,
        super::error::Code::TooManyOpenStreams,
        format!(
            "tenant already has {} long connections (SSE, attach) open on this node — retry later",
            st.streams.tenant_max
        ),
    )
    .into_response();
    if let Some(v) = retry_after_header(1000) {
        resp.headers_mut()
            .insert(axum::http::header::RETRY_AFTER, v);
    }
    resp
}

impl Drop for TenantSlot {
    fn drop(&mut self) {
        let mut open = self.open.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(n) = open.get_mut(&self.tenant) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                open.remove(&self.tenant);
            }
        }
    }
}

/// The start offset: `?from=`, or `Last-Event-ID` + 1, or 0.
fn start_offset(
    q: &HashMap<String, String>,
    headers: &axum::http::HeaderMap,
) -> Result<u64, ApiError> {
    if let Some(v) = q.get("from") {
        return v
            .parse()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "from is not a number".into()));
    }
    match headers.get("last-event-id") {
        Some(v) => v
            .to_str()
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|n| n + 1)
            .ok_or_else(|| {
                ApiError(
                    StatusCode::BAD_REQUEST,
                    "Last-Event-ID is not a number".into(),
                )
            }),
        None => Ok(0),
    }
}

pub(super) async fn stream_sse(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path((id, name)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    ext: axum::http::Extensions,
) -> ApiResult<Response> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    if !crate::streams::valid_name(&name) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("invalid stream name: {name:?}"),
        ));
    }
    let from = start_offset(&q, &headers)?;
    // The tenant's slot before the node's slot: a tenant that is full does not take
    // someone else's place in the node cap.
    let Some(tenant_slot) = TenantSlot::take(&st, &who.tenant) else {
        return Ok(too_many_open_streams(&st));
    };
    let Some(slot) = SseSlot::take(&st) else {
        crate::metrics::agent::bump(&crate::metrics::agent::SSE_REJECTED);
        let mut resp = ApiError::coded(
            StatusCode::TOO_MANY_REQUESTS,
            super::error::Code::SseLimit,
            format!(
                "node already has {} SSE connections open — retry later",
                st.streams.sse_max
            ),
        )
        .into_response();
        if let Some(v) = retry_after_header(1000) {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, v);
        }
        return Ok(resp);
    };
    // Leader only (AP-13): a node not leading returns `503` + `leader` — it does not
    // fall back to the follower path like `/invocations/{id}`.
    let (_, access) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.open_stream(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    let access = access.map_err(|e| {
        ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
    })?;
    let not_found = || ApiError(StatusCode::NOT_FOUND, "no such invocation".into());
    let target = access.target.ok_or_else(not_found)?;
    // Another tenant ⇒ 404 (AP-7): do not reveal that the invocation exists.
    if !who.all_tenants() && target.tenant != who.tenant {
        return Err(not_found());
    }
    let status = access.status.ok_or_else(not_found)?;
    // The hub has not seen the stream: a durable stream of an earlier term is in the Db.
    let mut in_db = false;
    if access.hub.read(&raw, &name, from, 0).is_none() {
        let page = st
            .router
            .ask_range(cairn_store::partition_key_of(&raw), |e| {
                let name = name.clone();
                async move { e.read_stream(raw, name, 0, 1).await }
            })
            .await
            .ok_or_else(range_settling)?;
        in_db = page
            .1
            .map_err(|e| {
                ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, &st, &raw)
            })?
            .exists;
    }
    if !in_db
        && access.hub.read(&raw, &name, from, 0).is_none()
        && matches!(
            status,
            InvocationStatus::Completed(_)
                | InvocationStatus::Failed { .. }
                | InvocationStatus::Paused { .. }
        )
    {
        return Err(ApiError::coded(
            StatusCode::GONE,
            super::error::Code::StreamExpired,
            format!("stream {name:?} is gone (sealed and expired)"),
        ));
    }
    let reader = Reader {
        rx: access.hub.subscribe(),
        hub: access.hub,
        id: raw,
        name,
        next: from,
        pending: VecDeque::new(),
        deadline: tokio::time::Instant::now() + st.streams.max_life,
        done: false,
        seen: in_db.then_some(true),
        in_db,
        recheck_at: tokio::time::Instant::now() + st.streams.recheck,
        creds: (headers.clone(), ext),
        st: st.clone(),
        _slot: slot,
        _tenant_slot: tenant_slot,
    };
    let events = futures::stream::unfold(reader, |mut r| async move {
        r.next_event().await.map(|e| (Ok::<_, Infallible>(e), r))
    });
    Ok(Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(st.streams.keepalive)
                .text("keepalive"),
        )
        .into_response())
}

/// One SSE reader: copies chunks from the hub, waits on `tick` once everything is read.
struct Reader {
    hub: Arc<RangeStreams>,
    rx: tokio::sync::watch::Receiver<u64>,
    id: InvocationId,
    name: String,
    next: u64,
    pending: VecDeque<Event>,
    deadline: tokio::time::Instant,
    done: bool,
    /// The stream has been seen in the hub (or in the Db) — and whether it is durable or transient.
    seen: Option<bool>,
    /// A durable stream with chunks in the Db that the hub does not (or no longer) hold.
    in_db: bool,
    /// The next permission recheck and what to check with: headers + extensions at open.
    recheck_at: tokio::time::Instant,
    creds: (axum::http::HeaderMap, axum::http::Extensions),
    st: AppState,
    _slot: SseSlot,
    _tenant_slot: TenantSlot,
}

fn json_event(kind: &str, v: serde_json::Value) -> Event {
    Event::default().event(kind).data(v.to_string())
}

impl Reader {
    async fn next_event(&mut self) -> Option<Event> {
        loop {
            if let Some(e) = self.pending.pop_front() {
                return Some(e);
            }
            if self.done || tokio::time::Instant::now() >= self.deadline {
                return None;
            }
            // Recheck permissions periodically: a revoked key, a logged-out session, a
            // tenant being deleted ⇒ close the stream.
            if tokio::time::Instant::now() >= self.recheck_at {
                if !self.st.auth().still_valid(&self.creds.0, &self.creds.1) {
                    return None;
                }
                self.recheck_at = tokio::time::Instant::now() + self.st.streams.recheck;
            }
            // Mark as seen **before** reading: a change arriving after this read wakes
            // the wait below at once.
            self.rx.borrow_and_update();
            if self.hub.ended() {
                self.moved();
                continue;
            }
            match self.hub.read(&self.id, &self.name, self.next, CHUNK) {
                // The head of a durable stream has left RAM (it is durable): read from the
                // Db up to where the hub starts.
                Some(slice) if slice.durable && slice.missed > 0 => {
                    self.seen = Some(true);
                    let upto = self.next + slice.missed;
                    let before = self.next;
                    if self.pull_db(Some(upto)).await.is_none() {
                        continue;
                    }
                    // One read takes only one Db page: if it still progresses, read on in
                    // the next round. Only when a page brings nothing and the hub is still
                    // not reached is that stretch really missing from the Db.
                    if self.next < upto && self.next == before {
                        // The Db does not have that stretch (should not happen): report the loss,
                        // move on.
                        self.pending.push_back(json_event(
                            "gap",
                            serde_json::json!({ "offset": self.next, "dropped": upto - self.next }),
                        ));
                        self.next = upto;
                    }
                    continue;
                }
                Some(slice) => {
                    self.seen = Some(slice.durable);
                    if slice.missed > 0 {
                        self.pending.push_back(json_event(
                            "gap",
                            serde_json::json!({ "offset": self.next, "dropped": slice.missed }),
                        ));
                    }
                    for r in &slice.records {
                        let durable = slice.durable && r.offset < slice.durable_upto;
                        self.pending.push_back(record_event(r, durable));
                    }
                    self.next = slice.next;
                    if slice.ended {
                        self.pending
                            .push_back(json_event("end", serde_json::json!({ "next": self.next })));
                        self.done = true;
                    }
                    if !self.pending.is_empty() {
                        continue;
                    }
                }
                // A durable stream that is only in the Db (a new term, or cleared from the
                // hub after sealing): read on from the Db; done once the invocation is finished.
                None if self.in_db || self.seen == Some(true) => {
                    self.in_db = true;
                    let Some(terminal) = self.pull_db(None).await else {
                        continue;
                    };
                    if !self.pending.is_empty() {
                        continue;
                    }
                    if terminal {
                        self.pending
                            .push_back(json_event("end", serde_json::json!({ "next": self.next })));
                        self.done = true;
                        continue;
                    }
                    tokio::select! {
                        changed = self.rx.changed() => {
                            if changed.is_err() {
                                self.moved();
                            }
                        }
                        () = tokio::time::sleep(DB_POLL) => {}
                        () = tokio::time::sleep_until(self.deadline.min(self.recheck_at)) => {}
                    }
                    continue;
                }
                // Seen before and now gone: the sealed stream has expired and been cleaned up.
                None if self.seen.is_some() => {
                    self.pending
                        .push_back(json_event("end", serde_json::json!({ "next": self.next })));
                    self.done = true;
                    continue;
                }
                None => {}
            }
            tokio::select! {
                changed = self.rx.changed() => {
                    if changed.is_err() {
                        // The hub has been dropped: the term has ended.
                        self.moved();
                    }
                }
                () = tokio::time::sleep_until(self.deadline.min(self.recheck_at)) => {}
            }
        }
    }

    /// Reads durable chunks from the Db on the leader, from `self.next` up to before
    /// `upto`, pushing them as events (`durable: true`: the leader's Db only holds durable
    /// chunks once the hub no longer holds them). Returns whether the invocation has
    /// finished; `None` when the leader has changed (`moved` already pushed) or the range
    /// is moving (retry later).
    async fn pull_db(&mut self, upto: Option<u64>) -> Option<bool> {
        let (id, name, from) = (self.id, self.name.clone(), self.next);
        let asked = self
            .st
            .router
            .ask_range(cairn_store::partition_key_of(&id), |e| {
                let name = name.clone();
                async move { e.read_stream(id, name, from, DB_PAGE).await }
            })
            .await;
        let page = match asked {
            Some((_, Ok(page))) => page,
            Some((_, Err(_))) => {
                self.moved();
                return None;
            }
            // The range is moving: rest one tick, then ask again.
            None => {
                cairn_store::pace::poll_gap(DB_POLL).await;
                return None;
            }
        };
        let mut chunks = page.chunks.into_iter().peekable();
        while let Some((first, chunk)) = chunks.next() {
            if upto.is_some_and(|u| first >= u) {
                break;
            }
            match chunk {
                Some(c) => {
                    for r in c.records {
                        if r.offset < self.next || upto.is_some_and(|u| r.offset >= u) {
                            continue;
                        }
                        let rec = crate::streams::Record::from_chunk(r);
                        self.next = rec.offset + 1;
                        self.pending.push_back(record_event(&rec, true));
                    }
                }
                // A corrupt chunk: skip to the next chunk, report the lost part.
                None => {
                    let to = chunks.peek().map(|(f, _)| *f).or(upto).unwrap_or(self.next);
                    if to > self.next {
                        self.pending.push_back(json_event(
                            "gap",
                            serde_json::json!({ "offset": self.next, "dropped": to - self.next }),
                        ));
                        self.next = to;
                    }
                }
            }
        }
        Some(matches!(
            page.status,
            None | Some(
                InvocationStatus::Completed(_)
                    | InvocationStatus::Failed { .. }
                    | InvocationStatus::Paused { .. }
            )
        ))
    }

    /// The term ended: `moved` (+ `reset` for transient streams), then close.
    fn moved(&mut self) {
        let leader = self.st.router.not_leader(&self.id).leader;
        self.pending
            .push_back(json_event("moved", serde_json::json!({ "leader": leader })));
        if self.seen != Some(true) {
            self.pending
                .push_back(json_event("reset", serde_json::json!({})));
        }
        self.done = true;
    }
}

fn record_event(r: &crate::streams::Record, durable: bool) -> Event {
    let base = |kind: &str, mut v: serde_json::Value| {
        v["offset"] = r.offset.into();
        Event::default()
            .event(kind)
            .id(r.offset.to_string())
            .data(v.to_string())
    };
    match &r.body {
        Body::Data(p) => {
            let data = serde_json::from_slice::<serde_json::Value>(p)
                .unwrap_or_else(|_| String::from_utf8_lossy(p).into_owned().into());
            base(
                "record",
                serde_json::json!({
                    // Only durable records (AP-12); transient streams are always `false`.
                    "durable": durable,
                    "step": r.ident.step,
                    "attempt": r.ident.attempt,
                    "k": r.ident.k,
                    "data": data,
                }),
            )
        }
        Body::Gap { dropped } => base("gap", serde_json::json!({ "dropped": dropped })),
        Body::Superseded { step, attempt } => base(
            "superseded",
            serde_json::json!({ "step": step, "attempt": attempt }),
        ),
    }
}

// ─────────────────────────────── `$log` and `$trace`

/// The default / maximum record cap of one `GET /invocations/{id}/logs` page.
const LOGS_LIMIT: usize = 1000;
const LOGS_LIMIT_MAX: usize = 10_000;
/// Cap on the `$trace` records read for one tree build — `TRACE_MAX_BYTES` (1 MiB)
/// divided by the smallest records of a few dozen bytes.
const SPANS_RECORDS_MAX: usize = 50_000;

/// Asks the leader of `raw`'s range (AP-13), checks the tenant (AP-7).
pub(super) async fn leader_access(
    st: &AppState,
    who: &crate::auth::Identity,
    raw: InvocationId,
) -> Result<crate::actor::StreamAccess, ApiError> {
    let (_, access) = st
        .router
        .ask_range(cairn_store::partition_key_of(&raw), |e| async move {
            e.open_stream(raw).await
        })
        .await
        .ok_or_else(range_settling)?;
    let access = access
        .map_err(|e| ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, st, &raw))?;
    let not_found = || ApiError(StatusCode::NOT_FOUND, "no such invocation".into());
    let target = access.target.as_ref().ok_or_else(not_found)?;
    if !who.all_tenants() && target.tenant != who.tenant {
        return Err(not_found());
    }
    Ok(access)
}

/// The records of durable stream `name` from `from`, at most `max`: the part that has
/// left the hub is read from the leader's Db, the tail from the hub. `(record, durable)`
/// in offset order, and the next offset.
async fn collect(
    st: &AppState,
    access: &crate::actor::StreamAccess,
    raw: InvocationId,
    name: &str,
    from: u64,
    max: usize,
) -> Result<(Vec<(crate::streams::Record, bool)>, u64), ApiError> {
    let mut out = Vec::new();
    let mut next = from;
    let hub_slice = access.hub.read(&raw, name, from, 0);
    // The part before the hub (or the whole stream when the hub does not hold it) is in the Db.
    let need_db = hub_slice.as_ref().is_none_or(|s| s.durable && s.missed > 0);
    if need_db {
        let upto = hub_slice.as_ref().map(|s| from + s.missed);
        loop {
            let page = st
                .router
                .ask_range(cairn_store::partition_key_of(&raw), |e| {
                    let name = name.to_string();
                    async move { e.read_stream(raw, name, next, DB_PAGE).await }
                })
                .await
                .ok_or_else(range_settling)?
                .1
                .map_err(|e| {
                    ApiError::from_actor_routed(e, StatusCode::INTERNAL_SERVER_ERROR, st, &raw)
                })?;
            let before = next;
            for (_, chunk) in page.chunks {
                let Some(c) = chunk else { continue };
                for r in c.records {
                    if r.offset < next || upto.is_some_and(|u| r.offset >= u) || out.len() >= max {
                        continue;
                    }
                    next = r.offset + 1;
                    out.push((crate::streams::Record::from_chunk(r), true));
                }
            }
            if next == before || out.len() >= max || upto.is_some_and(|u| next >= u) {
                break;
            }
        }
        // Reached where the hub starts and the Db has no more stretches (a corrupt
        // chunk): continue from the hub. A full page stops exactly where it read —
        // jumping would skip records.
        if let Some(u) = upto.filter(|_| out.len() < max) {
            next = next.max(u);
        }
    }
    if out.len() < max {
        if let Some(s) = access.hub.read(&raw, name, next, max - out.len()) {
            for r in s.records {
                let durable = s.durable && r.offset < s.durable_upto;
                next = r.offset + 1;
                out.push((r, durable));
            }
        }
    }
    Ok((out, next))
}

/// The JSON payload of a record (the SDK writes JSON); not JSON ⇒ a string.
fn payload_json(p: &[u8]) -> serde_json::Value {
    serde_json::from_slice(p).unwrap_or_else(|_| String::from_utf8_lossy(p).into_owned().into())
}

const LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];

fn level_rank(l: &str) -> Option<usize> {
    LEVELS.iter().position(|x| *x == l)
}

/// `GET /invocations/{id}/logs?level=&attempt=&from=&limit=` — a JSON page of `$log`
/// (`Admin` permission; leader only). `?follow=1` ⇒ SSE of `$log`, like
/// `/streams/$log`.
pub(super) async fn logs(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
    ext: axum::http::Extensions,
) -> ApiResult<Response> {
    if q.get("follow").is_some_and(|v| v == "1" || v == "true") {
        return stream_sse(
            State(st),
            axum::Extension(who),
            Path((id, crate::streams::LOG.to_string())),
            Query(q),
            headers,
            ext,
        )
        .await;
    }
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let bad = |m: &str| ApiError(StatusCode::BAD_REQUEST, m.into());
    let min = match q.get("level") {
        Some(l) => Some(level_rank(l).ok_or_else(|| bad("level: debug | info | warn | error"))?),
        None => None,
    };
    let attempt = match q.get("attempt") {
        Some(a) => Some(
            a.parse::<u32>()
                .map_err(|_| bad("attempt is not a number"))?,
        ),
        None => None,
    };
    let from = start_offset(&q, &headers)?;
    let limit = match q.get("limit") {
        Some(l) => l
            .parse::<usize>()
            .map_err(|_| bad("limit is not a number"))?,
        None => LOGS_LIMIT,
    }
    .clamp(1, LOGS_LIMIT_MAX);
    let access = leader_access(&st, &who, raw).await?;
    let (records, next) = collect(&st, &access, raw, crate::streams::LOG, from, limit).await?;
    let mut page = Vec::with_capacity(records.len());
    for (r, durable) in records {
        let mut v = serde_json::json!({
            "offset": r.offset,
            "durable": durable,
            "step": r.ident.step,
            "attempt": r.ident.attempt,
            "k": r.ident.k,
        });
        match &r.body {
            Body::Data(p) => {
                let data = payload_json(p);
                let rank = data["level"].as_str().and_then(level_rank);
                if min.is_some_and(|m| rank.is_some_and(|r| r < m)) {
                    continue;
                }
                if let serde_json::Value::Object(fields) = data {
                    for (k, val) in fields {
                        v[k] = val;
                    }
                } else {
                    v["data"] = data;
                }
            }
            Body::Gap { dropped } => v["gap"] = serde_json::json!({ "dropped": dropped }),
            Body::Superseded { step, attempt } => {
                v["superseded"] = serde_json::json!({ "step": step, "attempt": attempt })
            }
        }
        if attempt.is_some_and(|a| a != r.ident.attempt) {
            continue;
        }
        page.push(v);
    }
    Ok(Json(serde_json::json!({ "records": page, "next": next })).into_response())
}

/// A span being built.
#[derive(Default)]
struct SpanNode {
    name: String,
    parent: Option<String>,
    start_ts: serde_json::Value,
    end_ts: serde_json::Value,
    status: serde_json::Value,
    error: Option<serde_json::Value>,
    attrs: serde_json::Map<String, serde_json::Value>,
    step: u32,
    attempt: u32,
    /// `start` has been seen. Not ⇒ a node built from the `end` alone (`start_lost`).
    started: bool,
    ended: bool,
    children: Vec<String>,
}

/// `GET /invocations/{id}/spans` — the span tree built from `$trace` (`Admin`
/// permission; leader only): each span is `start` ∪ `end`; a span with `start` and no
/// `end` is **abandoned** (`unfinished: true` — the node died or the attempt was
/// replaced); a span with `end` and no `start` is built from the `name`/`parent_id` of
/// the `end` (`start_lost: true` — a `start` not yet durable is lost when the runtime
/// dies); plus the total `input_tokens`/`output_tokens` of every span.
pub(super) async fn spans(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let raw = unhex(&id).ok_or_else(|| {
        ApiError(
            StatusCode::BAD_REQUEST,
            "id is not 48 hex characters".into(),
        )
    })?;
    let access = leader_access(&st, &who, raw).await?;
    let (records, _) = collect(
        &st,
        &access,
        raw,
        crate::streams::TRACE,
        0,
        SPANS_RECORDS_MAX,
    )
    .await?;
    let mut nodes: BTreeMap<String, SpanNode> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut placed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let (mut input, mut output) = (0u64, 0u64);
    let merge = |into: &mut serde_json::Map<String, serde_json::Value>, v: &serde_json::Value| {
        if let Some(m) = v.as_object() {
            for (k, val) in m {
                into.insert(k.clone(), val.clone());
            }
        }
    };
    for (r, _) in records {
        let Body::Data(p) = &r.body else { continue };
        let d = payload_json(p);
        let Some(span_id) = d["span_id"].as_str().map(str::to_string) else {
            continue;
        };
        match d["kind"].as_str() {
            Some("start") => {
                // A retry sends `start` again with the same id (`span_id` does not contain
                // `attempt`): merge into the existing node, one place in the order.
                if placed.insert(span_id.clone()) {
                    order.push(span_id.clone());
                }
                let n = nodes.entry(span_id).or_default();
                n.name = d["name"].as_str().unwrap_or_default().to_string();
                n.parent = d["parent_id"].as_str().map(str::to_string);
                n.start_ts = d["ts"].clone();
                n.step = r.ident.step;
                n.attempt = r.ident.attempt;
                n.started = true;
                merge(&mut n.attrs, &d["attrs"]);
            }
            Some("end") => {
                for (key, sum) in [("input_tokens", &mut input), ("output_tokens", &mut output)] {
                    *sum += d["attrs"][key].as_u64().unwrap_or(0);
                }
                if placed.insert(span_id.clone()) {
                    order.push(span_id.clone());
                }
                let n = nodes.entry(span_id).or_default();
                if !n.started {
                    // `start` was lost: the SDK repeats `name` and `parent_id` in
                    // `end`.
                    n.name = d["name"].as_str().unwrap_or_default().to_string();
                    n.parent = d["parent_id"].as_str().map(str::to_string);
                    n.step = r.ident.step;
                    n.attempt = r.ident.attempt;
                }
                n.end_ts = d["ts"].clone();
                n.status = d["status"].clone();
                n.error = d.get("error").cloned();
                n.ended = true;
                merge(&mut n.attrs, &d["attrs"]);
            }
            _ => {}
        }
    }
    // Children in the order of each span's first record (`start`, or `end` when `start`
    // was lost); a missing parent (an earlier attempt already cleared from the stream,
    // or a replay's parent span) ⇒ a root.
    let mut roots = Vec::new();
    for sid in &order {
        let parent = nodes.get(sid).and_then(|n| n.parent.clone());
        match parent.filter(|p| nodes.contains_key(p)) {
            Some(p) => {
                if let Some(pn) = nodes.get_mut(&p) {
                    pn.children.push(sid.clone());
                }
            }
            None => roots.push(sid.clone()),
        }
    }
    // Each span is drawn **once** (`seen`): the tree is built by whoever writes `$trace`,
    // so a parent cycle (a parent that is its own child) does not loop and does not
    // blow any node up.
    fn render(
        id: &str,
        nodes: &BTreeMap<String, SpanNode>,
        seen: &mut std::collections::BTreeSet<String>,
        depth: usize,
    ) -> serde_json::Value {
        let Some(n) = nodes.get(id) else {
            return serde_json::Value::Null;
        };
        seen.insert(id.to_string());
        let mut children = Vec::new();
        if depth < 64 {
            for c in &n.children {
                if !seen.contains(c) {
                    children.push(render(c, nodes, seen, depth + 1));
                }
            }
        }
        let mut v = serde_json::json!({
            "span_id": id,
            "parent_id": n.parent,
            "name": n.name,
            "start_ts": n.start_ts,
            "end_ts": n.end_ts,
            "status": n.status,
            "unfinished": !n.ended,
            "start_lost": !n.started,
            "attrs": n.attrs,
            "step": n.step,
            "attempt": n.attempt,
            "children": children,
        });
        if let Some(e) = &n.error {
            v["error"] = e.clone();
        }
        v
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut spans: Vec<_> = roots
        .iter()
        .map(|r| render(r, &nodes, &mut seen, 0))
        .collect();
    // Spans not reachable from any root (a parent cycle, or deeper than the cap): drawn
    // as roots in order, so that no span disappears.
    for sid in &order {
        if !seen.contains(sid) {
            spans.push(render(sid, &nodes, &mut seen, 0));
        }
    }
    Ok(Json(serde_json::json!({
        "spans": spans,
        "tokens": { "input": input, "output": output },
    }))
    .into_response())
}
