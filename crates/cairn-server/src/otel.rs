// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-invocation traces.
//!
//! Spans are **always** there (the cost of a `tracing` span nobody listens to is next to
//! nothing): one `cairnflow.run` span per run of an invocation, one `cairnflow.turn` span
//! per SDK turn inside it. Exported to OpenTelemetry only when built with
//! `--features otel` and `--otel-endpoint` is set — the default binary does not pull in the
//! OTLP dependency tree.
//!
//! **One trace per invocation.** An invocation runs many times (suspensions, wake-ups,
//! retries, leader changes), each with its own root span. The trace id is **derived from
//! the invocation id** (its first 16 bytes) so every run of it — on every node — lands in
//! the same trace without carrying any context through the journal.

use cairn_store::InvocationId;

/// Span of one run of invocation `id`.
pub fn run_span(id: &InvocationId) -> tracing::Span {
    let hex = cairn_store::id_hex(id);
    let span = tracing::info_span!("cairnflow.run", invocation = %hex);
    #[cfg(feature = "otel")]
    attach_trace(&span, id);
    span
}

/// Span of one SDK turn.
pub fn turn_span(service: &str, handler: &str, mode: &str) -> tracing::Span {
    tracing::info_span!("cairnflow.turn", service, handler, mode)
}

#[cfg(feature = "otel")]
fn attach_trace(span: &tracing::Span, id: &InvocationId) {
    use {
        opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
        },
        tracing_opentelemetry::OpenTelemetrySpanExt,
    };
    let mut t = [0u8; 16];
    t.copy_from_slice(&id[..16]);
    let mut s = [0u8; 8];
    s.copy_from_slice(&id[16..24]);
    let parent = SpanContext::new(
        TraceId::from_bytes(t),
        SpanId::from_bytes(s),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    let _ = span.set_parent(opentelemetry::Context::new().with_remote_span_context(parent));
}

#[cfg(feature = "otel")]
static PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Flush every span still in the exporter's buffer — call at graceful shutdown, otherwise
/// the last run of every invocation is missing from its trace. Without `otel` ⇒ no-op.
pub fn shutdown() {
    #[cfg(feature = "otel")]
    if let Some(p) = PROVIDER.get() {
        let _ = p.force_flush();
        let _ = p.shutdown();
    }
}

/// Layer exporting spans over OTLP/HTTP to `endpoint` (e.g. `http://collector:4318`).
#[cfg(feature = "otel")]
pub fn layer<S>(
    endpoint: &str,
) -> Result<
    (
        tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
        opentelemetry_sdk::trace::SdkTracerProvider,
    ),
    String,
>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use {opentelemetry::trace::TracerProvider, opentelemetry_otlp::WithExportConfig};
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()
        .map_err(|e| e.to_string())?;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cairnflow")
                .build(),
        )
        .build();
    let tracer = provider.tracer("cairnflow");
    let _ = PROVIDER.set(provider.clone());
    Ok((tracing_opentelemetry::layer().with_tracer(tracer), provider))
}

// ─────────────── user spans from `$trace`
//
// `ctx.trace.span` writes `start`/`end` onto the durable stream `$trace`. With OTLP export
// on, the hub hands each `$trace` record just received to a bounded channel
// ([`offer_trace`]) — without waiting for durability: telemetry tolerates loss, and the
// SDK's deterministic `span_id` lets the backend merge duplicates after a replay or retry.
// A separate OS thread pairs `start` with `end` ([`SpanPairing`]) and exports over
// OTLP/HTTP. A full channel ⇒ dropped (never blocks the engine thread). The trace id is
// derived from the invocation id as in [`run_span`]; the user's root spans sit under the
// same remote parent (`id[16..24]`) as `cairnflow.run`.

/// Whether **user** spans and logs (`$trace`, `$log`) are exported to the operator's OTLP.
/// A shared cell turns it off (`--no-user-otlp`, SaaS profile): a shared collector would
/// receive every tenant's data mixed together, and the attributes in `$trace`/`$log` are
/// written by the customer. Per-tenant export is future work. System spans
/// (`cairnflow.run`, `cairnflow.turn`) are unchanged; customers still read their own spans
/// in the console (`sys_span`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UserExport {
    #[default]
    On,
    Off,
}

/// Turn on exporting user spans and `$log` to `endpoint` according to `user`; returns the
/// error of each part (one failing part does not block the other). `Off` ⇒ builds nothing:
/// [`offer_trace`]/[`offer_log`] drop every record on the spot.
#[cfg(feature = "otel")]
pub fn export_user_data(endpoint: &str, user: UserExport) -> Vec<String> {
    if user == UserExport::Off {
        return Vec::new();
    }
    let mut errors = Vec::new();
    // User spans from `$trace`.
    if let Err(e) = export_user_spans(endpoint) {
        errors.push(format!("cannot enable exporting user spans: {e}"));
    }
    // `$log` (3.9b): `<endpoint>/v1/logs`.
    if let Err(e) = export_user_logs(endpoint) {
        errors.push(format!("cannot enable exporting $log: {e}"));
    }
    errors
}

/// Whether any user span or `$log` export channel is open.
pub fn user_export_active() -> bool {
    TRACE_TX.get().is_some() || LOG_TX.get().is_some()
}

/// Maximum `$trace` records waiting for export in the channel; when full they are dropped.
pub const TRACE_QUEUE: usize = 4096;
/// `start`s without an `end` are kept at most this many; beyond it the oldest is dropped
/// (abandoned spans are not exported — read them at `GET /invocations/{id}/spans`).
pub const OPEN_SPANS_MAX: usize = 10_000;

/// A user span ready for export — independent of opentelemetry, so the pairing and
/// mapping can be tested in the default build.
#[derive(Clone, Debug, PartialEq)]
pub struct UserSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// Parent: a user span, or the invocation's shared remote parent.
    pub parent_id: [u8; 8],
    pub name: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub ok: bool,
    pub error: Option<String>,
    /// Already renamed to `gen_ai.*` names where they exist.
    pub attrs: Vec<(String, serde_json::Value)>,
}

/// Attribute names at export: cairn's names stored in `$trace`, mapped onto OpenTelemetry's
/// GenAI semantic conventions where they exist. Other names are kept as they are.
pub fn otel_attr_name(k: &str) -> &str {
    match k {
        "model" => "gen_ai.request.model",
        "response_model" => "gen_ai.response.model",
        "provider" | "system" => "gen_ai.system",
        "input_tokens" => "gen_ai.usage.input_tokens",
        "output_tokens" => "gen_ai.usage.output_tokens",
        "tool" => "gen_ai.tool.name",
        "operation" => "gen_ai.operation.name",
        other => other,
    }
}

fn span_bytes(hex: &str) -> Option<[u8; 8]> {
    let mut out = [0u8; 8];
    if hex.len() != 16 {
        return None;
    }
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Pairs `$trace` `start` with `end` (bounded by [`OPEN_SPANS_MAX`]).
#[derive(Default)]
pub struct SpanPairing {
    open: std::collections::BTreeMap<(InvocationId, [u8; 8]), serde_json::Value>,
    order: std::collections::VecDeque<(InvocationId, [u8; 8])>,
}

impl SpanPairing {
    /// One `$trace` record of `id`. `Some` when it is an `end`: of a `start` already seen,
    /// or — when the `start` is lost (not durable when the runtime died, a leader change
    /// between the two records, pushed out by the limit) — built from the `end`'s own
    /// `name`/`parent_id`, with the start time equal to the end time and the attribute
    /// `cairn.start_lost`. A corrupt payload, a `span_id` that is not 16 hex digits, an
    /// orphan `end` without a `name` (older SDKs) ⇒ dropped.
    pub fn feed(&mut self, id: &InvocationId, payload: &[u8]) -> Option<UserSpan> {
        let d: serde_json::Value = serde_json::from_slice(payload).ok()?;
        let sid = span_bytes(d["span_id"].as_str()?)?;
        match d["kind"].as_str()? {
            "start" => {
                if self.open.len() >= OPEN_SPANS_MAX {
                    if let Some(old) = self.order.pop_front() {
                        self.open.remove(&old);
                    }
                }
                if self.open.insert((*id, sid), d).is_none() {
                    self.order.push_back((*id, sid));
                }
                None
            }
            "end" => {
                let (start, lost) = match self.open.remove(&(*id, sid)) {
                    Some(start) => {
                        self.order.retain(|k| *k != (*id, sid));
                        (start, false)
                    }
                    None => {
                        d["name"].as_str()?;
                        let start = serde_json::json!({
                            "name": d["name"],
                            "parent_id": d["parent_id"],
                            "ts": d["ts"],
                        });
                        (start, true)
                    }
                };
                let mut trace_id = [0u8; 16];
                trace_id.copy_from_slice(&id[..16]);
                let mut root = [0u8; 8];
                root.copy_from_slice(&id[16..24]);
                let parent_id = start["parent_id"]
                    .as_str()
                    .and_then(span_bytes)
                    .unwrap_or(root);
                let mut attrs = Vec::new();
                for v in [&start["attrs"], &d["attrs"]] {
                    if let Some(m) = v.as_object() {
                        for (k, val) in m {
                            attrs.retain(|(n, _): &(String, _)| n != otel_attr_name(k));
                            attrs.push((otel_attr_name(k).to_string(), val.clone()));
                        }
                    }
                }
                if lost {
                    attrs.push(("cairn.start_lost".to_string(), serde_json::json!(true)));
                }
                Some(UserSpan {
                    trace_id,
                    span_id: sid,
                    parent_id,
                    name: start["name"].as_str().unwrap_or("span").to_string(),
                    start_ms: start["ts"].as_u64().unwrap_or(0),
                    end_ms: d["ts"].as_u64().unwrap_or(0),
                    ok: d["status"].as_str() != Some("error"),
                    error: d["error"].as_str().map(str::to_string),
                    attrs,
                })
            }
            _ => None,
        }
    }
}

/// One `$log` line ready for export — independent of
/// opentelemetry, like [`UserSpan`].
#[derive(Clone, Debug, PartialEq)]
pub struct UserLog {
    pub trace_id: [u8; 16],
    /// The invocation's shared remote parent (`id[16..24]`) — log lines do not carry
    /// a span of their own.
    pub span_id: [u8; 8],
    pub ts_ms: u64,
    /// `debug|info|warn|error` as the SDK wrote it.
    pub level: String,
    pub msg: String,
    /// Already renamed to `gen_ai.*` names where they exist.
    pub attrs: Vec<(String, serde_json::Value)>,
}

/// A `$log` record of `id` as a [`UserLog`]. A corrupt payload or a missing `msg`
/// ⇒ `None`.
pub fn user_log(id: &InvocationId, payload: &[u8]) -> Option<UserLog> {
    let d: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let msg = d["msg"].as_str()?.to_string();
    let mut trace_id = [0u8; 16];
    trace_id.copy_from_slice(&id[..16]);
    let mut span_id = [0u8; 8];
    span_id.copy_from_slice(&id[16..24]);
    let attrs = d["attrs"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (otel_attr_name(k).to_string(), v.clone()))
                .collect()
        })
        .unwrap_or_default();
    Some(UserLog {
        trace_id,
        span_id,
        ts_ms: d["ts"].as_u64().unwrap_or(0),
        level: d["level"].as_str().unwrap_or("info").to_string(),
        msg,
        attrs,
    })
}

/// Maximum `$log` records waiting for export in the channel; when full they are dropped.
pub const LOG_QUEUE: usize = 4096;

static LOG_TX: std::sync::OnceLock<std::sync::mpsc::SyncSender<TraceItem>> =
    std::sync::OnceLock::new();

/// Like [`offer_trace`], for a `$log` record just received.
pub fn offer_log(id: &InvocationId, payload: &bytes::Bytes) {
    if let Some(tx) = LOG_TX.get() {
        if tx.try_send((*id, payload.clone())).is_err() {
            crate::metrics::agent::bump(&crate::metrics::agent::OTEL_LOGS_DROPPED);
        }
    }
}

/// Turn on exporting `$log` over OTLP/HTTP to `endpoint` (`<endpoint>/v1/logs`): a
/// separate OS thread reads the channel and hands records to opentelemetry's batch
/// processor (its own thread, a bounded queue).
#[cfg(feature = "otel")]
pub fn export_user_logs(endpoint: &str) -> Result<(), String> {
    use opentelemetry_otlp::WithExportConfig;
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/logs", endpoint.trim_end_matches('/')))
        .build()
        .map_err(|e| e.to_string())?;
    let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("cairnflow")
                .build(),
        )
        .with_batch_exporter(exporter)
        .build();
    let (tx, rx) = std::sync::mpsc::sync_channel::<TraceItem>(LOG_QUEUE);
    LOG_TX
        .set(tx)
        .map_err(|_| "$log export already enabled".to_string())?;
    std::thread::Builder::new()
        .name("cairnflow-otel-user-logs".into())
        .spawn(move || user_log_loop(provider, rx))
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(feature = "otel")]
fn user_log_loop(
    provider: opentelemetry_sdk::logs::SdkLoggerProvider,
    rx: std::sync::mpsc::Receiver<TraceItem>,
) {
    use opentelemetry::{
        logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity},
        trace::{SpanId, TraceFlags, TraceId},
    };
    let logger = provider.logger("cairnflow.user");
    // Channel closed (every sender dropped) ⇒ exit.
    while let Ok((id, payload)) = rx.recv() {
        let Some(l) = user_log(&id, &payload) else {
            continue;
        };
        let (severity, text) = match l.level.as_str() {
            "debug" => (Severity::Debug, "DEBUG"),
            "warn" => (Severity::Warn, "WARN"),
            "error" => (Severity::Error, "ERROR"),
            _ => (Severity::Info, "INFO"),
        };
        let mut rec = logger.create_log_record();
        rec.set_timestamp(std::time::UNIX_EPOCH + std::time::Duration::from_millis(l.ts_ms));
        rec.set_severity_number(severity);
        rec.set_severity_text(text);
        rec.set_body(AnyValue::String(l.msg.into()));
        for (k, v) in l.attrs {
            rec.add_attribute(k, any_value(v));
        }
        rec.set_trace_context(
            TraceId::from_bytes(l.trace_id),
            SpanId::from_bytes(l.span_id),
            Some(TraceFlags::SAMPLED),
        );
        logger.emit(rec);
    }
    let _ = provider.shutdown();
}

#[cfg(feature = "otel")]
fn any_value(v: serde_json::Value) -> opentelemetry::logs::AnyValue {
    use opentelemetry::logs::AnyValue;
    match v {
        serde_json::Value::Bool(b) => AnyValue::Boolean(b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => AnyValue::Int(i),
            None => AnyValue::Double(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(t) => AnyValue::String(t.into()),
        other => AnyValue::String(other.to_string().into()),
    }
}

type TraceItem = (InvocationId, bytes::Bytes);

static TRACE_TX: std::sync::OnceLock<std::sync::mpsc::SyncSender<TraceItem>> =
    std::sync::OnceLock::new();

/// Hand a `$trace` record just received to the OTLP exporter, if any. No waiting,
/// no lock: a full channel or export not enabled ⇒ dropped.
pub fn offer_trace(id: &InvocationId, payload: &bytes::Bytes) {
    if let Some(tx) = TRACE_TX.get() {
        if tx.try_send((*id, payload.clone())).is_err() {
            crate::metrics::agent::bump(&crate::metrics::agent::OTEL_SPANS_DROPPED);
        }
    }
}

/// Turn on exporting user spans over OTLP/HTTP to `endpoint` (e.g.
/// `http://collector:4318`): a separate OS thread pairs them and exports in batches.
#[cfg(feature = "otel")]
pub fn export_user_spans(endpoint: &str) -> Result<(), String> {
    use opentelemetry_otlp::WithExportConfig;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()
        .map_err(|e| e.to_string())?;
    let (tx, rx) = std::sync::mpsc::sync_channel::<TraceItem>(TRACE_QUEUE);
    TRACE_TX
        .set(tx)
        .map_err(|_| "user span export already enabled".to_string())?;
    std::thread::Builder::new()
        .name("cairnflow-otel-user-spans".into())
        .spawn(move || user_span_loop(exporter, rx))
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(feature = "otel")]
fn user_span_loop(
    exporter: opentelemetry_otlp::SpanExporter,
    rx: std::sync::mpsc::Receiver<TraceItem>,
) {
    use opentelemetry_sdk::trace::SpanExporter as _;
    let mut pairing = SpanPairing::default();
    let scope = opentelemetry::InstrumentationScope::builder("cairnflow.user").build();
    // Channel closed (every sender dropped) ⇒ exit.
    while let Ok(first) = rx.recv() {
        let mut batch = Vec::new();
        for (id, payload) in std::iter::once(first).chain(rx.try_iter().take(511)) {
            if let Some(s) = pairing.feed(&id, &payload) {
                batch.push(span_data(s, &scope));
            }
        }
        if !batch.is_empty() {
            let _ = futures::executor::block_on(exporter.export(batch));
        }
    }
}

#[cfg(feature = "otel")]
fn span_data(
    s: UserSpan,
    scope: &opentelemetry::InstrumentationScope,
) -> opentelemetry_sdk::trace::SpanData {
    use opentelemetry::{
        trace::{SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState},
        KeyValue, Value,
    };
    let at = |ms: u64| std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms);
    let attributes = s
        .attrs
        .into_iter()
        .map(|(k, v)| {
            let v = match v {
                serde_json::Value::Bool(b) => Value::Bool(b),
                serde_json::Value::Number(n) => match n.as_i64() {
                    Some(i) => Value::I64(i),
                    None => Value::F64(n.as_f64().unwrap_or(0.0)),
                },
                serde_json::Value::String(t) => Value::String(t.into()),
                other => Value::String(other.to_string().into()),
            };
            KeyValue::new(k, v)
        })
        .collect();
    opentelemetry_sdk::trace::SpanData {
        span_context: SpanContext::new(
            TraceId::from_bytes(s.trace_id),
            SpanId::from_bytes(s.span_id),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        ),
        parent_span_id: SpanId::from_bytes(s.parent_id),
        parent_span_is_remote: true,
        span_kind: SpanKind::Internal,
        name: s.name.into(),
        start_time: at(s.start_ms),
        end_time: at(s.end_ms.max(s.start_ms)),
        attributes,
        dropped_attributes_count: 0,
        events: Default::default(),
        links: Default::default(),
        status: if s.ok {
            Status::Ok
        } else {
            Status::error(s.error.unwrap_or_default())
        },
        instrumentation_scope: scope.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: InvocationId = [7; 24];

    fn feed(p: &mut SpanPairing, v: serde_json::Value) -> Option<UserSpan> {
        p.feed(&ID, v.to_string().as_bytes())
    }

    /// `start` + `end` ⇒ one span: the id from the SDK, the trace id from the invocation id,
    /// the root under the invocation's remote parent, attributes merged and renamed to `gen_ai.*`.
    #[test]
    fn start_and_end_pair_into_one_span_with_gen_ai_names() {
        use serde_json::json;
        let mut p = SpanPairing::default();
        assert!(feed(
            &mut p,
            json!({"kind":"start","span_id":"00000000000000aa","parent_id":null,"name":"llm","ts":5,"attrs":{"model":"m"}})
        )
        .is_none());
        let child = json!({"kind":"start","span_id":"00000000000000bb","parent_id":"00000000000000aa","name":"tool","ts":6});
        assert!(feed(&mut p, child).is_none());
        let s = feed(
            &mut p,
            json!({"kind":"end","span_id":"00000000000000aa","ts":9,"status":"error","error":"boom","attrs":{"input_tokens":12,"x":1}}),
        )
        .expect("span");
        assert_eq!(s.trace_id, [7; 16]);
        assert_eq!(s.span_id, [0, 0, 0, 0, 0, 0, 0, 0xaa]);
        assert_eq!(
            s.parent_id, [7; 8],
            "root under the remote parent id[16..24]"
        );
        assert_eq!((s.name.as_str(), s.start_ms, s.end_ms), ("llm", 5, 9));
        assert!(!s.ok);
        assert_eq!(s.error.as_deref(), Some("boom"));
        let names: Vec<_> = s.attrs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            ["gen_ai.request.model", "gen_ai.usage.input_tokens", "x"]
        );
        let c = feed(
            &mut p,
            json!({"kind":"end","span_id":"00000000000000bb","ts":7,"status":"ok"}),
        )
        .expect("child");
        assert_eq!(c.parent_id, [0, 0, 0, 0, 0, 0, 0, 0xaa]);
        assert!(c.ok);
        // An `end` with neither a `start` nor a `name` (older SDKs), a corrupt payload: dropped.
        assert!(feed(
            &mut p,
            json!({"kind":"end","span_id":"00000000000000cc","ts":1})
        )
        .is_none());
        assert!(p.feed(&ID, b"{").is_none());
        assert!(feed(&mut p, json!({"kind":"start","span_id":"xyz"})).is_none());
    }

    /// An `end` whose `start` was lost (the runtime died while the `start` was not durable,
    /// or a leader change between the two records): the SDK writes
    /// `name`/`parent_id` into the `end`, so the span can still be exported — the start time
    /// equal to the end time, the attribute `cairn.start_lost`. The old bug: dropped, so the
    /// span and its tokens never reached the backend.
    #[test]
    fn end_without_start_is_exported_from_its_own_name_and_parent() {
        use serde_json::json;
        let mut p = SpanPairing::default();
        let s = feed(
            &mut p,
            json!({"kind":"end","span_id":"00000000000000cc","parent_id":"00000000000000aa","name":"llm","ts":9,"status":"ok","attrs":{"input_tokens":3}}),
        )
        .expect("span with a lost start still exported");
        assert_eq!(s.span_id, [0, 0, 0, 0, 0, 0, 0, 0xcc]);
        assert_eq!(s.parent_id, [0, 0, 0, 0, 0, 0, 0, 0xaa]);
        assert_eq!((s.name.as_str(), s.start_ms, s.end_ms), ("llm", 9, 9));
        assert!(s.ok);
        assert!(
            s.attrs
                .iter()
                .any(|(k, v)| k == "cairn.start_lost" && *v == json!(true)),
            "{:?}",
            s.attrs
        );
        assert!(s
            .attrs
            .iter()
            .any(|(k, _)| k == "gen_ai.usage.input_tokens"));
        // Root: no parent ⇒ under the invocation's remote parent.
        let r = feed(
            &mut p,
            json!({"kind":"end","span_id":"00000000000000dd","parent_id":null,"name":"late","ts":4,"status":"ok"}),
        )
        .expect("root with a lost start");
        assert_eq!(r.parent_id, [7; 8]);
    }

    /// A `$log` line ⇒ an OTLP log record: the trace id and the remote parent from the
    /// invocation id, the level, the message, attributes renamed to `gen_ai.*`.
    #[test]
    fn log_record_maps_to_an_otlp_log_under_the_invocation_trace() {
        let l = user_log(
            &ID,
            serde_json::json!({"ts":42,"level":"warn","msg":"slow","attrs":{"model":"m","n":1}})
                .to_string()
                .as_bytes(),
        )
        .expect("log");
        assert_eq!(l.trace_id, [7; 16]);
        assert_eq!(l.span_id, [7; 8]);
        assert_eq!(
            (l.ts_ms, l.level.as_str(), l.msg.as_str()),
            (42, "warn", "slow")
        );
        let names: Vec<_> = l.attrs.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, ["gen_ai.request.model", "n"]);
        // No `msg` or a corrupt payload: dropped.
        assert!(user_log(&ID, b"{").is_none());
        assert!(user_log(&ID, br#"{"ts":1,"level":"info"}"#).is_none());
    }

    /// `UserExport::Off` opens no channel, so a customer's
    /// `$trace`/`$log` never reaches the operator's collector;
    /// `On` with the same collector does — proving the fake collector really catches them, so
    /// the "not seen" check above means something. A single test: the channel
    /// is a process-wide `OnceLock`, once on it cannot be turned off.
    ///
    /// 1. A fake OTLP/HTTP collector records the path and body of every request.
    /// 2. `Off`: no channel open; offer a span and a log carrying the mark `offmark`.
    /// 3. `On`: channel open; offer a span and a log carrying the mark `onmark`.
    ///
    /// Must see: the collector receives `/v1/traces` and `/v1/logs` containing `onmark`, and
    /// no request contains `offmark`.
    #[cfg(feature = "otel")]
    #[test]
    fn user_export_off_sends_nothing_and_on_reaches_the_collector() {
        use {
            bytes::Bytes,
            std::{
                io::{BufRead, BufReader, Read, Write},
                net::TcpListener,
                sync::{Arc, Mutex},
                time::Duration,
            },
        };
        /// `(path, body)` of every request the collector receives.
        type Seen = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
        let seen: Seen = Arc::default();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("addr"));
        {
            let seen = Arc::clone(&seen);
            std::thread::spawn(move || {
                for conn in listener.incoming() {
                    let Ok(mut conn) = conn else { continue };
                    let seen = Arc::clone(&seen);
                    std::thread::spawn(move || {
                        let mut r = BufReader::new(conn.try_clone().expect("clone"));
                        // Keep-alive: many requests on one connection.
                        loop {
                            let mut line = String::new();
                            if r.read_line(&mut line).unwrap_or(0) == 0 {
                                return;
                            }
                            let path = line.split(' ').nth(1).unwrap_or("").to_string();
                            let mut len = 0usize;
                            loop {
                                let mut h = String::new();
                                if r.read_line(&mut h).unwrap_or(0) == 0 {
                                    return;
                                }
                                if h == "\r\n" {
                                    break;
                                }
                                if let Some((k, v)) = h.split_once(':') {
                                    if k.eq_ignore_ascii_case("content-length") {
                                        len = v.trim().parse().unwrap_or(0);
                                    }
                                }
                            }
                            let mut body = vec![0; len];
                            if r.read_exact(&mut body).is_err() {
                                return;
                            }
                            seen.lock().unwrap().push((path, body));
                            let _ = conn.write_all(
                                b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\n\r\n",
                            );
                        }
                    });
                }
            });
        }
        let offer = |mark: &str| {
            let span = |v: serde_json::Value| Bytes::from(v.to_string());
            offer_trace(
                &ID,
                &span(
                    serde_json::json!({"kind":"start","span_id":"00000000000000aa","name":format!("span-{mark}"),"ts":1}),
                ),
            );
            offer_trace(
                &ID,
                &span(
                    serde_json::json!({"kind":"end","span_id":"00000000000000aa","ts":2,"status":"ok"}),
                ),
            );
            offer_log(
                &ID,
                &span(serde_json::json!({"ts":3,"level":"info","msg":format!("log-{mark}")})),
            );
        };
        let has =
            |body: &[u8], needle: &str| body.windows(needle.len()).any(|w| w == needle.as_bytes());

        assert!(export_user_data(&endpoint, UserExport::Off).is_empty());
        assert!(!user_export_active(), "Off must open no channel");
        offer("offmark");

        assert_eq!(
            export_user_data(&endpoint, UserExport::On),
            Vec::<String>::new()
        );
        assert!(user_export_active());
        offer("onmark");

        // A 20 s limit, polling every 50 ms.
        for round in 0.. {
            let got = seen.lock().unwrap().clone();
            let traces = got
                .iter()
                .any(|(p, b)| p == "/v1/traces" && has(b, "span-onmark"));
            let logs = got
                .iter()
                .any(|(p, b)| p == "/v1/logs" && has(b, "log-onmark"));
            if traces && logs {
                assert!(
                    !got.iter().any(|(_, b)| has(b, "offmark")),
                    "records offered while Off must not reach the collector"
                );
                break;
            }
            assert!(
                round < 400,
                "collector did not receive user spans/logs after enabling: {:?}",
                got.iter().map(|(p, _)| p).collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn open_spans_are_bounded() {
        let mut p = SpanPairing::default();
        for i in 0..(OPEN_SPANS_MAX + 5) {
            let sid = format!("{i:016x}");
            feed(
                &mut p,
                serde_json::json!({"kind":"start","span_id":sid,"name":"s","ts":1}),
            );
        }
        assert_eq!(p.open.len(), OPEN_SPANS_MAX);
        // The oldest one has been dropped.
        let oldest = format!("{:016x}", 0);
        assert!(feed(
            &mut p,
            serde_json::json!({"kind":"end","span_id":oldest,"ts":2})
        )
        .is_none());
    }
}
