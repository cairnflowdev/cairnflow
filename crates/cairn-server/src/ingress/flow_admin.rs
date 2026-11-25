// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Admin flow control: rules, limits, vqueues, scheduler.

use super::*;

// ─────────────────────────────────────────────── flow control (admin API)

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleView {
    pub pattern: String,
    /// `"unlimited"` or a number; **absent** = not set (the counter takes the cap of a
    /// less specific rule). A string rather than `Option<u32>`: "no limit" is an
    /// *explicit choice* of the operator, entirely different from "not set" — and a
    /// `null` read back cannot tell the two apart.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<String>,
    /// Rate cap, CU/s, for each `(tenant, counter)` matching the rule: `"unlimited"`
    /// or a number; absent = not set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<String>,
    /// The maximum CU that can accumulate; absent = equal to `rate`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
    pub description: String,
    pub disabled: bool,
    pub modified_ms: u64,
    /// The number of counter levels this rule applies to: 1 = scope, 2 = L1, 3 = L2.
    pub level: usize,
}

/// One rate cap line for a whole tenant.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantRuleView {
    pub tenant: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
    pub description: String,
    pub disabled: bool,
    pub modified_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RulesResponse {
    pub version: u64,
    pub rules: Vec<RuleView>,
    /// Per-tenant rate caps (the `tenant` lines of the `/2` rule book).
    pub tenants: Vec<TenantRuleView>,
    /// A warning about `*`, printed **right where people type the command**.
    ///
    /// This is the number one misunderstanding about flow control and it fails in the
    /// expensive direction, so it must not live only in the docs. Only appears when the
    /// rule book really has a pattern containing `*` — a warning that always shows is a
    /// warning nobody reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<&'static str>,
}

pub(super) fn rule_view(r: &cairn_store::Rule) -> RuleView {
    RuleView {
        pattern: r.pattern.to_string(),
        concurrency: r.concurrency.map(|c| c.to_string()),
        rate: r.rate.map(|c| c.to_string()),
        burst: r.burst,
        description: r.description.clone(),
        disabled: r.disabled,
        modified_ms: r.modified_ms,
        level: r.pattern.depth(),
    }
}

pub(super) fn rules_response(book: &cairn_store::RuleBook) -> RulesResponse {
    rules_response_for(book, true)
}

/// Like [`rules_response`]; `patterns = false` (a key without the `Cluster` permission
/// — the control plane) ⇒ tenant lines only: patterns carry the customer's service
/// names, which are none of the provisioner's business.
pub(super) fn rules_response_for(book: &cairn_store::RuleBook, patterns: bool) -> RulesResponse {
    let rules: Vec<RuleView> = if patterns {
        book.iter().map(rule_view).collect()
    } else {
        Vec::new()
    };
    RulesResponse {
        version: book.version,
        warning: rules
            .iter()
            .any(|r| r.pattern.contains('*'))
            .then_some(STAR_WARNING),
        rules,
        tenants: book
            .tenants()
            .map(|t| TenantRuleView {
                tenant: t.tenant.to_string(),
                rate: t.rate.map(|c| c.to_string()),
                burst: t.burst,
                description: t.description.clone(),
                disabled: t.disabled,
                modified_ms: t.modified_ms,
            })
            .collect(),
    }
}

pub(super) fn rule_store(st: &AppState) -> ApiResult<&RuleStore> {
    st.rules.as_ref().ok_or_else(|| {
        ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "this node runs without a rule store (no object store configured for the cluster)"
                .into(),
        )
    })
}

/// `GET /rules` — the whole rule book, with `version`. A `provision` key only sees the
/// tenant lines ([`rules_response_for`]).
///
/// `version` is not decoration: it is what a reader compares to know whether the copy
/// they see is newer than the previous one. An etag cannot do that — an S3 etag is a
/// content hash, so a rule book rewritten identically keeps its etag.
pub(super) async fn list_rules(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
) -> ApiResult<Json<RulesResponse>> {
    let book = rule_store(&st)?
        .load()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .book;
    Ok(Json(rules_response_for(&book, who.all_tenants())))
}

pub(super) async fn get_rule(
    State(st): State<AppState>,
    Path(pattern): Path<String>,
) -> ApiResult<Json<RuleView>> {
    let book = rule_store(&st)?
        .load()
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .book;
    book.get(&pattern)
        .map(|r| Json(rule_view(r)))
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no rule {pattern:?}")))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutRuleRequest {
    /// The rule's pattern. Absent when editing a tenant's line (`tenant`).
    #[serde(default)]
    pub pattern: Option<String>,
    /// Edit the rate cap of **this whole tenant** instead of a pattern rule.
    #[serde(default)]
    pub tenant: Option<String>,
    /// A number, or `"unlimited"`. Absent = **leave** the current cap **alone**;
    /// `null` = **remove** it back to not set — outer `Some`: present in the JSON,
    /// inner: the value (`None` = `null`).
    #[serde(default, deserialize_with = "present")]
    pub concurrency: Option<Option<serde_json::Value>>,
    /// Rate cap, CU/s: a number, or `"unlimited"`. Absent = leave alone;
    /// `null` = remove.
    #[serde(default, deserialize_with = "present")]
    pub rate: Option<Option<serde_json::Value>>,
    /// The maximum CU that can accumulate (≥ 1). Absent = leave alone; `null` = remove (equal to
    /// `rate`).
    #[serde(default, deserialize_with = "present")]
    pub burst: Option<Option<u32>>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: Option<bool>,
}

/// A field **present** in the JSON, including `null` ⇒ `Some(..)`; absent ⇒ `None`
/// (through `#[serde(default)]`). serde reads the `null` of an `Option<T>` as `None`, so
/// without the outer layer "remove" and "leave alone" would be the same.
fn present<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

/// `"unlimited"` or a `u32`.
pub(super) fn limit_field(name: &str, v: &Option<serde_json::Value>) -> ApiResult<Option<Limit>> {
    match v {
        None => Ok(None),
        Some(serde_json::Value::String(s)) if s == "unlimited" => Ok(Some(Limit::Unlimited)),
        Some(serde_json::Value::Number(n)) => {
            let v = n
                .as_u64()
                .filter(|v| *v <= u32::MAX as u64)
                .ok_or_else(|| {
                    ApiError(StatusCode::BAD_REQUEST, format!("{name} must be a u32"))
                })?;
            Ok(Some(Limit::Max(v as u32)))
        }
        Some(_) => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("{name} must be a number or the string \"unlimited\""),
        )),
    }
}

/// `POST /rules` — adds or edits a rule (or a tenant's line).
/// **Idempotent, and a merge**. A key without the `Cluster` permission (the control
/// plane) only edits tenant lines: a pattern ⇒ `403`.
///
/// An absent field means *leave alone*, not *delete*: `set --disabled` must not lose
/// the `description` explaining why this rule exists, and that is often the most
/// expensive part of a rule.
pub(super) async fn put_rule(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Json(req): Json<PutRuleRequest>,
) -> ApiResult<Json<RulesResponse>> {
    let patterns = who.all_tenants();
    if !patterns && req.pattern.is_some() {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "pattern rules are a cluster matter — this key only edits the tenant row".into(),
        ));
    }
    let clear = cairn_store::RuleClear {
        concurrency: req.concurrency == Some(None),
        rate: req.rate == Some(None),
        burst: req.burst == Some(None),
    };
    let concurrency = limit_field("concurrency", &req.concurrency.clone().flatten())?;
    let rate = limit_field("rate", &req.rate.clone().flatten())?;
    let burst = req.burst.flatten();
    if burst == Some(0) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "burst must be ≥ 1".into(),
        ));
    }
    let target = match (&req.pattern, &req.tenant) {
        (Some(p), None) => Ok(p.clone()),
        (None, Some(t)) => {
            if concurrency.is_some() || clear.concurrency {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "a tenant row only has rate/burst: the concurrency limit is set per pattern"
                        .into(),
                ));
            }
            Err(t.clone())
        }
        _ => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "need exactly one of: pattern or tenant".into(),
            ))
        }
    };
    let now = now_ms();
    let book = rule_store(&st)?
        .update(|b| {
            let edit = cairn_store::RuleEdit {
                concurrency,
                rate,
                burst,
                description: req.description.clone(),
                disabled: req.disabled,
                clear,
            };
            match &target {
                Ok(pattern) => b.apply(pattern, edit, now).map(|_| ()),
                Err(tenant) => b.apply_tenant(tenant, edit, now).map(|_| ()),
            }
            .map_err(cairn_store::StoreError::from)
        })
        .await
        .map_err(|e| match e {
            cairn_store::StoreError::Ident(_) => ApiError(StatusCode::BAD_REQUEST, e.to_string()),
            other => ApiError(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    push_rules(&st, &book).await;
    Ok(Json(rules_response_for(&book, patterns)))
}

/// `DELETE /rules?tenant=<t>` — deletes a tenant's rate cap line. `404` if there is
/// none.
pub(super) async fn delete_tenant_rule(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<RulesResponse>> {
    let tenant = q
        .get("tenant")
        .cloned()
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, "need ?tenant=".into()))?;
    let mut existed = false;
    let book = rule_store(&st)?
        .update(|b| {
            existed = b
                .remove_tenant(&tenant)
                .map_err(cairn_store::StoreError::from)?;
            Ok(())
        })
        .await
        .map_err(|e| match e {
            cairn_store::StoreError::Ident(_) => ApiError(StatusCode::BAD_REQUEST, e.to_string()),
            other => ApiError(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    if !existed {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no row for tenant {tenant:?}"),
        ));
    }
    push_rules(&st, &book).await;
    Ok(Json(rules_response_for(&book, who.all_tenants())))
}

pub(super) async fn delete_rule(
    State(st): State<AppState>,
    Path(pattern): Path<String>,
) -> ApiResult<Json<RulesResponse>> {
    let mut existed = false;
    let book = rule_store(&st)?
        .update(|b| {
            existed = b.remove(&pattern).map_err(cairn_store::StoreError::from)?;
            Ok(())
        })
        .await
        .map_err(|e| match e {
            cairn_store::StoreError::Ident(_) => ApiError(StatusCode::BAD_REQUEST, e.to_string()),
            other => ApiError(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;
    if !existed {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("no rule {pattern:?}"),
        ));
    }
    push_rules(&st, &book).await;
    Ok(Json(rules_response(&book)))
}

/// Pushes the rule book just written into the scheduler of **the partitions this node
/// leads**, immediately.
///
/// It does not replace the poll tick — other nodes only learn through their own poll,
/// and that is the latency written into the API contract. This only cuts the latency
/// for the node *where the command is typed*, so that `POST /rules` followed at once by
/// `GET /limits` does not return an answer older than the command just run.
pub(super) async fn push_rules(st: &AppState, book: &cairn_store::RuleBook) {
    for (_, engine) in st.router.partitions.iter() {
        let _ = engine.set_rules(book.clone()).await;
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitView {
    pub partition: u32,
    pub counter: String,
    /// 1 = scope, 2 = L1, 3 = L2.
    pub level: u8,
    pub used: u32,
    /// `null` = no limit (no rule matches, or the rule says `unlimited`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available: Option<u32>,
    /// The rule currently winning at this counter. Here because "the queue is blocked by
    /// rule R" is **not enough**: what blocks is the *pair* (rule, specific counter).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
}

/// `GET /limits` — usage/limit/available per level, for the partitions this node
/// leads.
///
/// **Only counts counters that are non-zero.** A counter back at 0 is removed from the
/// table: keeping every scope that ever ran forever is a slow memory leak no test sees.
/// To learn the cap of a scope that has not run anything, ask `/rules`.
pub(super) async fn list_limits(State(st): State<AppState>) -> ApiResult<Json<Vec<LimitView>>> {
    let mut out = Vec::new();
    for (p, engine) in st.router.partitions.iter() {
        let Some(snap) = flow_of(&engine).await else {
            continue;
        };
        for r in snap.limits {
            out.push(LimitView {
                partition: p,
                counter: r.counter,
                level: r.level,
                used: r.used,
                limit: r.limit,
                available: r.available,
                rule: r.rule,
            });
        }
    }
    Ok(Json(out))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VQueueView {
    pub partition: u32,
    pub queue: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_key: Option<String>,
    pub service: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    pub exclusive: bool,
    /// Requests waiting for a slot.
    pub waiting: usize,
    pub granted: u64,
    pub blocked_times: u64,
    /// The resource kind that is blocking, if blocked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_on: Option<String>,
    /// The invocation holding the object's VO lock, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_holder: Option<String>,
}

/// `GET /vqueues` — queues that have work.
///
/// The queue name is read straight from the key, with no reverse lookup table: that is
/// the dividend of using composite keys instead of hashing.
///
/// **The statistics here are not persisted** — `granted`/`blockedTimes` go back to 0
/// after every leader change. That is a choice: INV-3 requires *pending work* to live
/// in the state table, statistics are not pending work, and in cairn every increment
/// of a number is a commit through the object store.
pub(super) async fn list_vqueues(State(st): State<AppState>) -> ApiResult<Json<Vec<VQueueView>>> {
    let mut out = Vec::new();
    for (p, engine) in st.router.partitions.iter() {
        let Some(snap) = flow_of(&engine).await else {
            continue;
        };
        for q in snap.queues {
            out.push(VQueueView {
                partition: p,
                queue: q.queue,
                scope: q.scope,
                limit_key: q.limit_key,
                service: q.service,
                object_key: q.object_key,
                exclusive: q.exclusive,
                waiting: q.waiting,
                granted: q.granted,
                blocked_times: q.blocked_times,
                blocked_on: q.blocked_on,
                lock_holder: q.lock_holder,
            });
        }
    }
    Ok(Json(out))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerView {
    pub partition: u32,
    /// Queues in the eligible ring.
    pub eligible: usize,
    pub queues: usize,
    pub waiting: usize,
    pub turns_in_use: usize,
    pub max_turns: usize,
    pub locks_held: usize,
    /// The rule book version this partition **is actually using**.
    ///
    /// May lag `/rules` by one poll cycle, and that is the contract, not a bug —
    /// comparing these two numbers is how the operator sees that lag.
    pub rules_version: u64,
    pub granted: u64,
    pub blocked: Vec<BlockedCount>,
    /// Rate buckets (CU) and `vt` of each tenant that has a cap or is waiting — of the
    /// **node** (shared by all its ranges), repeated on each range.
    pub tenants: Vec<crate::rate::TenantRateRow>,
}

#[derive(Serialize)]
pub struct BlockedCount {
    pub resource: String,
    pub count: u64,
}

/// `GET /scheduler` — queue depth, where things are blocked.
pub(super) async fn scheduler_status(
    State(st): State<AppState>,
) -> ApiResult<Json<Vec<SchedulerView>>> {
    let mut out = Vec::new();
    for (p, engine) in st.router.partitions.iter() {
        let Some(snap) = flow_of(&engine).await else {
            continue;
        };
        let s = snap.scheduler;
        out.push(SchedulerView {
            partition: p,
            eligible: s.eligible,
            queues: s.queues,
            waiting: s.waiting,
            turns_in_use: s.turns_in_use,
            max_turns: s.max_turns,
            locks_held: s.locks_held,
            rules_version: s.rules_version,
            granted: s.granted,
            blocked: s
                .blocked
                .into_iter()
                .map(|(resource, count)| BlockedCount { resource, count })
                .collect(),
            tenants: s.tenants,
        });
    }
    Ok(Json(out))
}

/// The flow control snapshot of a partition. `None` = this node does not lead it —
/// out of scope, the same rule as `/invocations`.
pub(super) async fn flow_of(engine: &EngineHandle) -> Option<crate::flow::FlowSnapshot> {
    engine.flow().await.ok()
}

pub(super) fn now_ms() -> u64 {
    cairn_store::entropy::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
