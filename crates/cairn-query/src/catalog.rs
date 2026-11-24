// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Table catalogue**: column docs generated from the declarations
//! (`docs/api/query-tables.md`, test `query_tables_doc_is_current`) and the
//! coverage check: every keyspace table either has a query table that reads it
//! or is on the exclusion list **with a reason** (`every_table_is_queryable_or_excluded`).

use {
    crate::{invocation, keyed, view},
    cairn_store::keys::Table,
    std::fmt::Write,
};

/// How a keyspace table is queried.
#[derive(Clone, Copy, Debug)]
pub enum Covered {
    /// The query table that reads it.
    By(&'static [&'static str]),
    /// Not (yet) queryable, and why.
    Excluded(&'static str),
}

/// Every table of `keyspace::Table`.
pub const TABLE_COVERAGE: &[(Table, Covered)] = &[
    (Table::InvocationStatus, Covered::By(&["sys_invocation"])),
    (Table::InvocationMeta, Covered::By(&["sys_invocation"])),
    (Table::Journal, Covered::By(&["sys_journal"])),
    (Table::State, Covered::By(&["sys_state"])),
    (Table::Idempotency, Covered::By(&["sys_invocation"])),
    (Table::Promises, Covered::By(&["sys_promise"])),
    (
        Table::VQueueIndex,
        Covered::By(&["sys_vqueue", "sys_vqueue_entry"]),
    ),
    (
        Table::Streams,
        Covered::By(&["sys_log", "sys_stream", "sys_stream_record"]),
    ),
    (
        Table::Artifacts,
        Covered::By(&["sys_artifact", "sys_artifact_bytes"]),
    ),
    (Table::Schedules, Covered::By(&["sys_schedule"])),
    // Audit rows waiting for export: read from the exported object.
    (Table::Audit, Covered::By(&["sys_audit"])),
    (Table::Timers, Covered::By(&["sys_timer"])),
    (Table::Outbox, Covered::By(&["sys_outbox"])),
    (Table::SignalOutbox, Covered::By(&["sys_outbox"])),
    (Table::Inbound, Covered::By(&["sys_inbound"])),
    (Table::Attachments, Covered::By(&["sys_attachment"])),
    (Table::PromiseWaiters, Covered::By(&["sys_promise_waiter"])),
    (Table::Retries, Covered::By(&["sys_invocation"])),
    (Table::LastFailure, Covered::By(&["sys_invocation"])),
    (Table::Purges, Covered::By(&["sys_invocation"])),
    (Table::Delayed, Covered::By(&["sys_delayed"])),
    (
        Table::Pins,
        Covered::By(&["sys_invocation", "sys_deployment"]),
    ),
    (Table::Chain, Covered::By(&["sys_invocation"])),
    (Table::SegmentedState, Covered::By(&["sys_state"])),
    // One row per (holder, segment) of the owner.
    (Table::ValueRefs, Covered::By(&["sys_value"])),
];

/// Column docs of `sys_invocation`, in schema order.
pub const INVOCATION_DOCS: &[(&str, &str)] = &[
    ("id", "id invocation (48 hex)"),
    ("tenant", "tenant"),
    ("service", "service"),
    ("handler", "handler"),
    ("key", "object key (empty for unkeyed services)"),
    ("kind", "`call`, `send`, `workflow`, …"),
    ("class", "`active`, `paused`, `completed`, `failed`"),
    (
        "status",
        "detailed status (`running`, `suspended`, … — one read when projected)",
    ),
    ("root", "root (not a child call)"),
    ("created_at", "creation time"),
    (
        "transitioned_at",
        "time the current class was entered (`active`: creation time)",
    ),
    ("ended_at", "end time"),
    ("duration_ms", "`ended_at − created_at`"),
    ("scope", "scope (metadata — one read when projected)"),
    ("parent_id", "caller id (metadata)"),
    ("schedule", "schedule that fired it (metadata)"),
    ("idempotency_key", "idempotency key (metadata)"),
    ("deployment", "deployment at creation (metadata)"),
    ("restarted_from", "previous run of restart-as-new (metadata)"),
    ("tags", "user tags, JSON (metadata)"),
    ("range", "range that holds it"),
    (
        "failure_code",
        "error code of class `failed`/`paused` (572 cancelled, 575 out of attempts, 576 quarantined, 577 stuck on infrastructure); one point read per row when projected",
    ),
    ("failure_message", "error message that goes with `failure_code`"),
    (
        "attempt",
        "attempts counted by the pending retry (`Retries`); one point read per row when projected",
    ),
    ("retry_at", "due time of the pending retry (or watchdog)"),
    (
        "last_failure_code",
        "code of the latest failed attempt (`LastFailure`); one point read per row when projected",
    ),
    ("last_failure_message", "message of the latest failed attempt (cut at 1 KiB)"),
    ("last_failure_at", "time the latest failed attempt ended"),
    (
        "purge_at",
        "purge (retention) due time of a finished invocation; one point read per row when projected",
    ),
    (
        "hash_chain_len",
        "number of entries in the tamper-evident hash chain (tenant with `hashChain` on); one point read per row when projected",
    ),
    (
        "pinned_deployment",
        "currently pinned deployment (after `migrate`; `deployment` is the one at creation) — unfinished invocations only; one point read per row when projected",
    ),
    (
        "root_id",
        "root of the call tree (NULL at the root) — `WHERE root_id = …` is one `ByAttr[$root]` seek per range: the whole tree of one root",
    ),
    (
        "journal_len",
        "number of journal entries (console question 32: `WHERE journal_len >= N`) — one reverse scan for the last entry per row when projected; filtering is a residual after the scan path of another filter",
    ),
    (
        "limit_key",
        "limit key at creation (`?limit-key=`, metadata; NULL for invocations created before this column existed) — together with `scope` it is the identity of its queue in `sys_vqueue_entry`",
    ),
];

const COUNTS_DOCS: &[(&str, &str)] = &[
    ("tenant", "tenant"),
    ("service", "service"),
    ("handler", "handler"),
    ("class", "class"),
    ("root", "root"),
    (
        "count",
        "invocation count (RAM, exact as of the commits in the range)",
    ),
];

const ARTIFACT_BYTES_DOCS: &[(&str, &str)] = &[
    ("tenant", "tenant"),
    (
        "bytes",
        "live artifact bytes (committed `Artifacts` rows; RAM, exact as of the commits in the range)",
    ),
];

fn ty_name(t: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::DataType as D;
    match t {
        D::Utf8 => "text".into(),
        D::Int64 => "int".into(),
        D::UInt32 | D::UInt64 => "uint".into(),
        D::Float64 => "float".into(),
        D::Boolean => "bool".into(),
        D::Timestamp(..) => "timestamp".into(),
        other => other.to_string(),
    }
}

fn section(
    out: &mut String,
    name: &str,
    doc: &str,
    key: Option<String>,
    schema: &datafusion::arrow::datatypes::Schema,
    docs: &dyn Fn(&str) -> String,
) {
    let _ = writeln!(out, "## `{name}`\n\n{doc}\n");
    if let Some(k) = key {
        let _ = writeln!(out, "**Required key:** {k}\n");
    }
    let _ = writeln!(out, "| Column | Type | Meaning |\n|---|---|---|");
    for f in schema.fields() {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} |",
            f.name(),
            ty_name(f.data_type()),
            docs(f.name())
        );
    }
    out.push('\n');
}

/// `docs/api/query-tables.md`.
pub fn markdown() -> String {
    let mut out = String::from(
        "# Query tables (`POST /query`)\n\n\
         *Generated from `cairn_query::catalog::markdown` — do not edit by hand; \
         `CAIRNFLOW_REGENERATE_QUERY_DOCS=1 cargo test -p cairn-query --test catalog`.*\n\n\
         Adding columns is fine, changing/removing one breaks the API.\n\n",
    );
    let pick = |docs: &'static [(&str, &str)]| {
        move |n: &str| {
            docs.iter()
                .find(|(c, _)| *c == n)
                .map_or_else(String::new, |(_, d)| d.to_string())
        }
    };
    section(
        &mut out,
        "sys_invocation",
        "Invocations, by the declared index. Declared order `[created_at DESC, id ASC]`.",
        None,
        &invocation::schema(),
        &pick(INVOCATION_DOCS),
    );
    section(
        &mut out,
        "sys_invocation_counts",
        "RAM counters by group; does not read the store.",
        None,
        &crate::counts::schema(),
        &pick(COUNTS_DOCS),
    );
    section(
        &mut out,
        crate::artifact_bytes::NAME,
        "Live artifact bytes per tenant (RAM counters, does not read the store); source of the \
         `maxArtifactBytes` cap. A range rebuilding its counters ⇒ warning `indexing`.",
        None,
        &crate::artifact_bytes::schema(),
        &pick(ARTIFACT_BYTES_DOCS),
    );
    for s in keyed::TABLES {
        let key = s
            .keys
            .iter()
            .map(|k| {
                k.iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(" + ")
            })
            .collect::<Vec<_>>()
            .join(" or ");
        let docs = |n: &str| {
            s.columns
                .iter()
                .find(|c| c.name == n)
                .map_or_else(String::new, |c| c.doc.to_string())
        };
        section(&mut out, s.name, s.doc, Some(key), &s.schema(), &docs);
    }
    for s in view::VIEWS {
        let docs = |n: &str| {
            s.columns
                .iter()
                .find(|c| c.name == n)
                .map_or_else(String::new, |c| c.doc.to_string())
        };
        section(&mut out, s.name, s.doc, None, &s.schema(), &docs);
    }
    let _ = writeln!(
        out,
        "## Keyspace coverage\n\n| Keyspace table | Query |\n|---|---|"
    );
    for (t, c) in TABLE_COVERAGE {
        let how = match c {
            Covered::By(names) => names
                .iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(", "),
            Covered::Excluded(why) => format!("— {why}"),
        };
        let _ = writeln!(out, "| `{t:?}` | {how} |");
    }
    let _ = writeln!(
        out,
        "\n## Feature coverage matrix\n\n\
         Every cairn feature → how to see it through `POST /query`. **REST** = an action or raw \
         bytes, already served by an endpoint; **—** = deliberately not queryable.\n\n\
         | # | Feature | Table / column |\n|---|---|---|"
    );
    for f in FEATURES {
        let how = match f.tables {
            [] => format!("— {}", f.note),
            names => {
                let t = names
                    .iter()
                    .map(|n| format!("`{n}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if f.note.is_empty() {
                    t
                } else {
                    format!("{t} — {}", f.note)
                }
            }
        };
        let _ = writeln!(out, "| {} | {} | {how} |", f.id, f.name);
    }
    out
}

/// One row of the feature coverage matrix.
pub struct Feature {
    pub id: &'static str,
    pub name: &'static str,
    /// Query tables that show it (must exist); empty = not queryable, the
    /// reason is in `note`.
    pub tables: &'static [&'static str],
    pub note: &'static str,
}

const fn f(
    id: &'static str,
    name: &'static str,
    tables: &'static [&'static str],
    note: &'static str,
) -> Feature {
    Feature {
        id,
        name,
        tables,
        note,
    }
}

/// The coverage matrix — every feature cairn has today.
pub const FEATURES: &[Feature] = &[
    f(
        "F1",
        "Calls: `/invoke`, `/send`, scope, workflow, delayed",
        &["sys_invocation", "sys_delayed"],
        "columns `kind`, `scope`, `tenant`",
    ),
    f(
        "F2",
        "Idempotency key",
        &["sys_invocation"],
        "column `idempotency_key`",
    ),
    f(
        "F3",
        "Status, class, timestamps",
        &["sys_invocation"],
        "`status`, `class`, `created_at`, `transitioned_at`, `ended_at`, `duration_ms`",
    ),
    f(
        "F4",
        "Retries, backoff, latest error",
        &["sys_invocation"],
        "`attempt`, `retry_at`, `last_failure_*`",
    ),
    f(
        "F5",
        "Pause (575/576/577), resume",
        &["sys_invocation"],
        "class `paused`, `failure_code`, `failure_message`; resume: REST",
    ),
    f(
        "F6",
        "Hard / cooperative cancel, propagated down the child tree",
        &["sys_invocation"],
        "`failure_code = 572`; children: `parent_id`",
    ),
    f(
        "F7",
        "`restart-as-new`",
        &["sys_invocation"],
        "`restarted_from`",
    ),
    f(
        "F8",
        "`migrate` to another deployment",
        &["sys_invocation"],
        "`pinned_deployment` (current), `deployment` (at creation)",
    ),
    f(
        "F9",
        "Trial replay, replay-check",
        &[],
        "REST: read-only action, the result is not stored",
    ),
    f(
        "F10",
        "Retention, purge",
        &["sys_invocation"],
        "`purge_at`; purge: REST",
    ),
    f(
        "F11",
        "Archive",
        &["sys_archive"],
        "by day; details: REST `/archive/{id}`",
    ),
    f(
        "F12",
        "Tamper-evident hash chain",
        &["sys_invocation", "sys_tenant"],
        "`hash_chain_len`; check: REST `/archive/{id}?verify=true`",
    ),
    f(
        "F13",
        "Attach (wait for the result by id / workflow)",
        &["sys_attachment"],
        "",
    ),
    f("F14", "Journal", &["sys_journal"], ""),
    f("F15", "`ctx.sleep`, timer", &["sys_timer"], ""),
    f(
        "F16",
        "Awakeable",
        &["sys_journal"],
        "uncompleted entries; resolve/reject: REST",
    ),
    f(
        "F17",
        "Signal (wait / send)",
        &["sys_journal", "sys_outbox"],
        "undelivered: `kind = 'signal'`",
    ),
    f(
        "F18",
        "Workflow promises, waiters",
        &["sys_promise", "sys_promise_waiter"],
        "",
    ),
    f("F19", "Combinators, future tree", &["sys_journal"], ""),
    f("F20", "Undelivered calls / replies", &["sys_outbox"], ""),
    f("F21", "Unapplied inbound messages", &["sys_inbound"], ""),
    f("F22", "Delayed sends", &["sys_delayed"], ""),
    f(
        "F23",
        "State",
        &["sys_state", "sys_value"],
        "bytes/segments of segmented values: `sys_value`",
    ),
    f(
        "F24",
        "`EXCLUSIVE` lock, the object's waiting queue",
        &["sys_vqueue_entry"],
        "",
    ),
    f(
        "F25",
        "Object list of a service",
        &["sys_object"],
        "`ObjectKeys` hint index, checked on read",
    ),
    f(
        "F26",
        "Workflow: `run` turn, `SHARED` handlers, output",
        &["sys_invocation", "sys_promise"],
        "output: REST",
    ),
    f(
        "F27",
        "Scope, limit key, rule book",
        &["sys_rule", "sys_limit"],
        "",
    ),
    f(
        "F28",
        "Virtual queues, scheduler, tenant fairness, throttle",
        &["sys_vqueue", "sys_scheduler"],
        "",
    ),
    f("F29", "Poison-handler quarantine", &["sys_quarantine"], ""),
    f(
        "F30",
        "Deployments, revisions, pinned invocation count",
        &["sys_deployment"],
        "`pinned`",
    ),
    f(
        "F31",
        "Services, handlers, handler options",
        &["sys_service", "sys_handler"],
        "",
    ),
    f(
        "F32",
        "Schedules",
        &["sys_schedule", "sys_invocation"],
        "runs: column `schedule`",
    ),
    f(
        "F33",
        "Durable streams (`ctx.stream`)",
        &["sys_stream", "sys_stream_record"],
        "ephemeral streams (RAM only): REST SSE",
    ),
    f("F34", "Log (`$log`)", &["sys_log"], ""),
    f(
        "F35",
        "Span, token (`$trace`)",
        &["sys_span"],
        "tree: REST `/spans`",
    ),
    f(
        "F36",
        "Artifact",
        &["sys_artifact", "sys_artifact_bytes"],
        "byte: REST",
    ),
    f(
        "F37",
        "Metering, CU, tokens, cost",
        &["sys_metering", "sys_pricing"],
        "",
    ),
    f(
        "F38",
        "Per-handler statistics",
        &["sys_handler_stats", "sys_handler_history"],
        "hourly, 30 days: `sys_handler_history`",
    ),
    f("F39", "Slow log", &["sys_slow_run"], ""),
    f(
        "F40",
        "Capture of the next run",
        &["sys_capture"],
        "bundle bytes: REST",
    ),
    f(
        "F41",
        "Metric Prometheus, OTel",
        &[],
        "already served by `/metrics`, the OTLP exporter",
    ),
    f("F42", "Tenant settings", &["sys_tenant"], ""),
    f("F43", "API keys", &["sys_key"], "no secrets"),
    f(
        "F44",
        "Ranges, load, L0, throttle, flush windows",
        &["sys_range", "sys_range_load", "sys_range_window"],
        "",
    ),
    f(
        "F45",
        "Split/merge operations, split suggestions",
        &["sys_range_op", "sys_range_suggestion"],
        "",
    ),
    f(
        "F46",
        "Nodes, slow nodes",
        &["sys_node", "sys_cluster_version"],
        "which step each binary knows: `sys_cluster_version`",
    ),
    f("F47", "Version, gates", &["sys_cluster_version"], ""),
    f("F48", "Shared WAL", &["sys_nodewal"], ""),
    f("F49", "Trash (GC)", &["sys_trash"], ""),
    f("F50", "Scrubber / `verify`", &["sys_scrub"], ""),
    f(
        "F51",
        "Data-plane audit log",
        &["sys_audit"],
        "last 24 hours; other ranges: `GET /audit?from&to`",
    ),
];
