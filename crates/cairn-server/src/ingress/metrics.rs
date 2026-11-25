// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `/metrics` and the metering view.

use super::*;

// ─────────────────────────────────────────────────── metering

/// `GET /metering?hour=yyyy-mm-ddThh[&tenant=]` — the numbers of one `(tenant, hour)`,
/// summed over every node, read from the bucket (each node flushes its numbers
/// periodically). Default: the current hour, the key's tenant; cluster-admin picks the
/// tenant with `?tenant=`.
pub(super) async fn metering_view(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    let tenant = acting_tenant(&who, &q)?;
    let hour = q
        .get("hour")
        .cloned()
        .unwrap_or_else(crate::metering::hour_now);
    // `yyyy-mm-ddThh`: it is a segment of an object path — block everything else.
    let ok_hour = hour.len() == 13
        && hour.bytes().enumerate().all(|(i, b)| match i {
            4 | 7 => b == b'-',
            10 => b == b'T',
            _ => b.is_ascii_digit(),
        });
    if !ok_hour {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "hour must be yyyy-mm-ddThh (UTC)".into(),
        ));
    }
    let Some((store, prefix)) = &st.metering else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node has no store for metering".into(),
        ));
    };
    let usage = crate::metering::read(store, prefix, tenant.as_str(), &hour)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let mut v = usage_json(&hour, &usage);
    v["tenant"] = tenant.as_str().into();
    Ok(Json(v))
}

/// One metering hour on the wire (`/metering`, `/metering/hours`).
fn usage_json(hour: &str, u: &crate::metering::Usage) -> serde_json::Value {
    serde_json::json!({
        "hour": hour,
        "invocations": u.invocations,
        "steps": u.steps,
        "runMs": u.run_ms,
        "inputTokens": u.input_tokens,
        "outputTokens": u.output_tokens,
        "costMicros": u.cost_micros,
        "models": u.models,
        "sdkBytesOut": u.sdk_bytes_out,
        "sdkBytesIn": u.sdk_bytes_in,
        "units": u.units,
    })
}

/// `GET /metering/hours?from=yyyy-mm-ddThh&to=yyyy-mm-ddThh[&tenant=]` — every hour
/// **with numbers** in `[from, to]` (≤ 31 days), summed over every node, ordered by
/// hour: the token/cost chart of the Usage screen. No `to` ⇒ the current hour.
pub(super) async fn metering_hours(
    State(st): State<AppState>,
    axum::Extension(who): axum::Extension<crate::auth::Identity>,
    Query(q): Query<HashMap<String, String>>,
) -> ApiResult<Json<serde_json::Value>> {
    use crate::metering::{hour_index, hour_now, RANGE_HOURS_MAX};
    let tenant = acting_tenant(&who, &q)?;
    let bad = |m: String| ApiError(StatusCode::BAD_REQUEST, m);
    let from = q
        .get("from")
        .cloned()
        .ok_or_else(|| bad("missing from (yyyy-mm-ddThh, UTC)".into()))?;
    let to = q.get("to").cloned().unwrap_or_else(hour_now);
    let (Some(a), Some(b)) = (hour_index(&from), hour_index(&to)) else {
        return Err(bad("from/to must be yyyy-mm-ddThh (UTC)".into()));
    };
    if b < a {
        return Err(bad("to must not be before from".into()));
    }
    if b - a >= RANGE_HOURS_MAX {
        return Err(bad(format!(
            "window is at most {RANGE_HOURS_MAX} hours (31 days), asked for {}",
            b - a + 1
        )));
    }
    let Some((store, prefix)) = &st.metering else {
        return Err(ApiError(
            StatusCode::NOT_IMPLEMENTED,
            "node has no store for metering".into(),
        ));
    };
    let hours = crate::metering::read_range(store, prefix, tenant.as_str(), &from, &to)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "from": from,
        "to": to,
        "hours": hours.iter().map(|(h, u)| usage_json(h, u)).collect::<Vec<_>>(),
    })))
}

// ─────────────────────────────────────────────────────────── metrics

/// Prometheus text — counters from [`crate::metrics::Metrics`], gauges snapshotted at
/// scrape time from the same sources `/cluster` uses.
pub(super) async fn metrics_text(State(st): State<AppState>) -> Response {
    use crate::metrics::{render, Gauges, PartitionGauge};
    let table = st.router.routing.snapshot();
    let mut gauges = Gauges {
        partition_count: st.router.cluster.partition_count() as u64,
        table_version: st.router.cluster.version(),
        partitions_with_leader: st
            .router
            .cluster
            .partitions()
            .into_iter()
            .filter(|p| table.contains_key(&p.0))
            .count() as u64,
        deployments: st.registry.snapshot().deployments().len() as u64,
        reader_healthy: Vec::new(),
        reader_durable_seq: Vec::new(),
        reader_manifest_seq: Vec::new(),
        range_turn_ms: st
            .auto_split
            .as_ref()
            .map(|a| {
                a.book()
                    .turn_ms_totals()
                    .into_iter()
                    .map(|(partition, value)| PartitionGauge { partition, value })
                    .collect()
            })
            .unwrap_or_default(),
        range_l0: st
            .auto_split
            .as_ref()
            .map(|a| a.book().l0_rows())
            .unwrap_or_default(),
        nodewal: cairn_store::nodewal::floor_reports(st.router.node_id.as_deref()),
        slow: st
            .router
            .node_id
            .as_deref()
            .and_then(cairn_store::health::meter_of),
        coordinator_term: st
            .router
            .node_id
            .as_deref()
            .and_then(cairn_store::coordinator_roles::term_of),
        // Ranges this node holds per the routing table; a single node (no id) holds
        // every range.
        schedules: crate::metrics::schedules::total(|range| match st.router.node_id.as_deref() {
            None => true,
            Some(me) => table.get(&range).is_some_and(|l| l.node_id == me),
        }),
        query_index: crate::metrics::query_index::snapshot(|range| {
            match st.router.node_id.as_deref() {
                None => true,
                Some(me) => table.get(&range).is_some_and(|l| l.node_id == me),
            }
        }),
        disk_cache: st
            .gc
            .as_ref()
            .and_then(|g| g.stores.disk_cache())
            .map(|c| c.snapshot()),
        tenants_book_rejected: st
            .tenant_settings
            .as_ref()
            .is_some_and(|t| t.book_rejected()) as u64,
        host: None,
    };
    // The machine's RAM and disk: the data directory (`file://` store) and the SST disk
    // cache, when the node uses them. Reads `/proc` and the mount list ⇒ off the async
    // threads.
    let dirs: Vec<(&'static str, std::path::PathBuf)> = st
        .gc
        .as_ref()
        .map(|g| {
            let mut d = Vec::new();
            if let Some(dir) = g.stores.data_dir() {
                d.push(("data", dir.to_path_buf()));
            }
            if let Some(c) = g.stores.disk_cache() {
                d.push(("disk_cache", c.config().dir.clone()));
            }
            d
        })
        .unwrap_or_default();
    gauges.host = tokio::task::spawn_blocking(move || crate::host::snapshot(&dirs))
        .await
        .ok();
    if let Some(f) = &st.followers {
        for (partition, s) in f.statuses().await {
            gauges.reader_healthy.push(PartitionGauge {
                partition,
                value: s.healthy as u64,
            });
            gauges.reader_durable_seq.push(PartitionGauge {
                partition,
                value: s.freshness.durable_seq,
            });
            gauges.reader_manifest_seq.push(PartitionGauge {
                partition,
                value: s.freshness.manifest_seq,
            });
        }
    }
    #[allow(unused_mut)]
    let mut text = render(&st.metrics, &gauges);
    // Counters of the store carrying an S3 simulation profile — `testkit` builds only.
    #[cfg(feature = "testkit")]
    crate::store_profile::render(&mut text);
    // The query layer.
    crate::query::http::metrics::render(&mut text);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        text,
    )
        .into_response()
}
