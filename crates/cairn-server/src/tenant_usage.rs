// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant usage counting for soft limits.
//!
//! One background round per node, tick [`DEFAULT_EVERY`]; **it only works when some tenant
//! carries a limit that needs it** — self-hosting costs nothing.
//!
//! - `max-live=`: invocations of class `active` + `paused` from the query counters
//!   (`QueryCounts`) of **every** range — local ranges read RAM, ranges on other nodes go
//!   through the query layer's remote scan (`sys_invocation_counts`, still RAM, no store
//!   read). A range rebuilding its counters (after `kill -9`, a leader change, a split/merge)
//!   is missing from the total ⇒ the soft limit lets it through; the overshoot is at most ≈
//!   creation rate × tick.
//! - `max-artifact-bytes=`: bytes of **live** artifacts (committed `Artifacts` rows) from
//!   every range's RAM counter (`cairn_store::artifact_bytes`), the same path as `max-live=`
//!   (`sys_artifact_bytes`) — **0 store requests** (previously one LIST of
//!   `artifacts/<tenant>/` per tick per node). Uncommitted, orphan or trash-held objects do
//!   not count. A range rebuilding is missing from the total ⇒ let through, as above.
//!
//! The result goes into `TenantLimitsSource::replace_usage`: the engine (`admit`, `step`) and
//! the scheduler read it through a generation-tagged snapshot, no shared lock on the turn
//! path. A failed count ⇒ keep the old numbers (never reset to 0: resetting would lift the
//! limit exactly when nobody is looking).

use {
    cairn_engine::TenantUsage,
    cairn_store::TenantId,
    std::{collections::BTreeMap, time::Duration},
};

/// Default counting tick.
pub const DEFAULT_EVERY: Duration = Duration::from_secs(5);

/// Time limit of one counting query.
const QUERY_TIMEOUT_MS: u64 = 10_000;

/// The query counting live invocations per tenant.
const LIVE_SQL: &str = "SELECT tenant, sum(count) FROM sys_invocation_counts \
     WHERE class IN ('active', 'paused') GROUP BY tenant";

/// The query counting live artifact bytes per tenant.
const ARTIFACT_SQL: &str = "SELECT tenant, bytes FROM sys_artifact_bytes";

/// Run the counting round until the task is cancelled (node stop).
pub async fn run(st: crate::ingress::AppState, every: Duration) {
    let Some(settings) = st.tenant_settings().cloned() else {
        return;
    };
    loop {
        cairn_store::pace::beat(every).await;
        let snap = settings.limits().snapshot();
        let live: Vec<TenantId> = snap
            .limited()
            .filter(|(_, l)| l.max_live.is_some())
            .map(|(t, _)| t.clone())
            .collect();
        let bytes: Vec<TenantId> = snap
            .limited()
            .filter(|(_, l)| l.max_artifact_bytes.is_some())
            .map(|(t, _)| t.clone())
            .collect();
        let scheds: Vec<TenantId> = snap
            .limited()
            .filter(|(_, l)| l.max_schedules.is_some())
            .map(|(t, _)| t.clone())
            .collect();
        if live.is_empty() && bytes.is_empty() && scheds.is_empty() {
            settings.limits().replace_usage(BTreeMap::new());
            continue;
        }
        let mut usage: BTreeMap<TenantId, TenantUsage> = BTreeMap::new();
        if !live.is_empty() {
            match count(&st, LIVE_SQL).await {
                Ok(counts) => {
                    for t in &live {
                        usage.entry(t.clone()).or_default().live =
                            counts.get(t).copied().unwrap_or(0);
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "cannot count live invocations — keeping the old numbers");
                    for t in &live {
                        usage.entry(t.clone()).or_default().live = snap.usage(t).live;
                    }
                }
            }
        }
        if !bytes.is_empty() {
            match count(&st, ARTIFACT_SQL).await {
                Ok(sums) => {
                    for t in &bytes {
                        usage.entry(t.clone()).or_default().artifact_bytes =
                            sums.get(t).copied().unwrap_or(0);
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "cannot count artifact bytes — keeping the old numbers");
                    for t in &bytes {
                        usage.entry(t.clone()).or_default().artifact_bytes =
                            snap.usage(t).artifact_bytes;
                    }
                }
            }
        }
        let mut counted_schedules = false;
        if !scheds.is_empty() {
            // Schedules per tenant: counted chunk by chunk in every range, once per tick
            // for every tenant carrying a limit.
            match crate::ingress::schedule_counts(&st).await {
                Ok(counts) => {
                    for t in &scheds {
                        usage.entry(t.clone()).or_default().schedules =
                            counts.get(t).copied().unwrap_or(0);
                    }
                    counted_schedules = true;
                }
                Err(e) => {
                    tracing::debug!(error = %e, "cannot count schedules — keeping the old numbers");
                    for t in &scheds {
                        usage.entry(t.clone()).or_default().schedules = snap.usage(t).schedules;
                    }
                }
            }
        }
        settings.limits().replace_usage(usage);
        if counted_schedules {
            st.reset_schedule_pending();
        }
    }
}

/// `tenant ⇒ number` from a `SELECT tenant, <number>` query through the node's query layer
/// (every tenant). A `partial` answer (a range rebuilding, a remote node not answering) is
/// still used: the missing part is not added — the soft limit lets it through.
async fn count(
    st: &crate::ingress::AppState,
    sql: &str,
) -> Result<BTreeMap<TenantId, u64>, String> {
    let svc = st
        .query()
        .ok_or_else(|| "query layer not enabled on this node".to_string())?;
    let answer = svc
        .execute(
            st,
            crate::query::http::Author::Server,
            cairn_query::TenantScope::All,
            crate::query::views::Credentials::default(),
            sql,
            &[],
            Some(QUERY_TIMEOUT_MS),
        )
        .await
        .map_err(|(code, msg)| format!("{code}: {msg}"))?;
    let rows = answer.rows()?;
    let mut out = BTreeMap::new();
    for r in rows {
        let (Some(t), Some(n)) = (
            r[0].as_str().and_then(|t| TenantId::new(t).ok()),
            r[1].as_u64()
                .or_else(|| r[1].as_str().and_then(|s| s.parse().ok())),
        ) else {
            continue;
        };
        out.insert(t, n);
    }
    Ok(out)
}
