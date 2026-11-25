// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The `cairn` CLI — thin, calls the admin API over HTTP.
//!
//! Thin on purpose: every capability lives in the API so anyone can call it with
//! `curl`; the CLI only saves keystrokes and prints readably. No argument parsing
//! library — the same hand-written style as `main.rs`, and an eight-command CLI does
//! not need more yet.
//!
//! ```sh
//! cairn --server http://127.0.0.1:8080 services
//! cairn invoke OrderService submit --key customer-1 --input '{"dish":"pho"}'
//! cairn invocations --status running
//! cairn cancel 00016f0a...
//! ```

// The CLI runs outside the cluster, calling the admin API over a real network — it never
// runs in the simulation, so requests go out directly rather than through `egress`.
#![allow(
    clippy::disallowed_methods,
    reason = "CLI outside the cluster, never runs in the simulation"
)]

use serde_json::Value;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

fn usage() -> ! {
    eprintln!(
        "cairn [--server URL] [--token KEY] [--json] <command>\n\
         \n\
         Commands:\n\
         \x20 register <uri>                       register an SDK deployment\n\
         \x20 deployments                          registered deployments (id, revision)\n\
         \x20 deployments rm <id>                  remove a deployment that is no longer a revision\n\
         \x20 revision <service> <id> [--expect ID] move a service's revision (rollback)\n\
         \x20 services                             callable services and handlers\n\
         \x20 invoke <service> <handler>           call a service\n\
         \x20        [--key K] [--input DATA]      DATA is the body; `-` reads stdin\n\
         \x20        [--scope S] [--limit-key L]   flow control; a limit key needs a scope\n\
         \x20 status <id>                          status of one invocation\n\
         \x20 journal <id>                         journal of an invocation\n\
         \x20 invocations [--status S] [--limit N] list invocations\n\
         \x20             [--all-nodes]            every node in the cluster (?scope=cluster)\n\
         \x20             [--cursor C] [--root]    keyset pages from the query layer (any\n\
         \x20             [--tag k=v] [--class L]  of these flags selects it; --cursor ''\n\
         \x20             [--service S]            is the first page)\n\
         \x20 sql <query>                          read-only SQL (POST /query), e.g.\n\
         \x20                                      \"SELECT class, sum(count) FROM\n\
         \x20                                      sys_invocation_counts GROUP BY class\"\n\
         \x20 stats [--window 5m|15m|1h]           per-handler summary: turns, outcomes,\n\
         \x20       [--tenant T] [--all-nodes]     failed attempts, p50/p99/max\n\
         \x20 capture <service> <handler>          capture the handler's next runs\n\
         \x20         [--tenant T] [--deployment D] [--count N] [--ttl SECONDS]\n\
         \x20 capture ls [--tenant T]              live capture requests + stored bundles\n\
         \x20 capture get <tenant> <id> [--out F]  download a capture bundle (default <id>.bundle)\n\
         \x20 replay --bundle <file>               replay a capture bundle locally, no cluster needed\n\
         \x20        [--endpoint URL]              SDK endpoint other than the one in the bundle\n\
         \x20 cancel <id>                          cancel an invocation\n\
         \x20 purge <id>                           purge a finished invocation\n\
         \x20 resume <id>                          resume a paused invocation\n\
         \x20 signal <id> <name|number> [--value JSON] wake an invocation from outside\n\
         \x20 resolve <svc> <key> <name> [--value J] resolve a promise from outside\n\
         \x20 reject <svc> <key> <name> <message>  reject a promise\n\
         \x20 promise <svc> <key> <name>           whether a promise has a result yet\n\
         \x20 workflow <svc> <key> [--attach]      a workflow's result; --attach waits for it\n\
         \x20          [--scope S]                 (no id needed: it is derived from svc+key)\n\
         \x20 cluster                              who leads which partition\n\
         \x20 ranges                               range table: version, key span, gen, intent\n\
         \x20 ranges split <id> [--at HEX16]       split a range (default: middle of its span)\n\
         \x20 ranges merge <left> <right>          merge two adjacent ranges\n\
         \x20 rules                                rule book (concurrency limits)\n\
         \x20 rules set <pattern>                  add or change a rule (merge)\n\
         \x20       [--concurrency N|unlimited]\n\
         \x20       [--description TXT] [--disabled|--enabled]\n\
         \x20 rules rm <pattern>                   remove a rule\n\
         \x20 limits                               usage/limit/available per tier\n\
         \x20 vqueues                              queues that have work\n\
         \x20 scheduler                            queue depth and what is blocking\n\
         \x20 keys                                 the cluster's API keys (secrets not shown)\n\
         \x20 keys create <tenant> <role>          create a key; role: invoke | view | admin |\n\
         \x20                                      provision | cluster-admin (view and provision\n\
         \x20                                      need `cairn-ctl cluster enable-key-roles` once)\n\
         \x20 keys rm <id>                         delete a key\n\
         \x20 keys check <id>                      whether any node still accepts a key (after rm returns 202)\n\
         \n\
         --server defaults to CAIRNFLOW_SERVER, then {DEFAULT_SERVER}\n\
         --token API key (sent as `Authorization: Bearer`), defaults to CAIRNFLOW_TOKEN\n\
         --json prints the API's raw JSON instead of a table"
    );
    std::process::exit(2);
}

struct Cli {
    server: String,
    json: bool,
    client: reqwest::Client,
    /// API key — every route except `/health` requires it when the cluster has auth on.
    token: Option<String>,
}

#[derive(Debug)]
struct CliError(String);

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<E: std::error::Error> From<E> for CliError {
    fn from(e: E) -> Self {
        Self(e.to_string())
    }
}

type Result<T> = std::result::Result<T, CliError>;

impl Cli {
    /// Attaches the API key if there is one.
    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    /// Sends the request, **following the `leader` field of a `503`** — the API's
    /// machine-readable contract (see `ErrorBody`). Once is enough: if the new leader also
    /// returns 503 the cluster is electing, just tell the user the truth.
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<(Vec<u8>, &str)>,
    ) -> Result<(reqwest::StatusCode, String)> {
        let mut base = self.server.clone();
        for hop in 0..2 {
            let url = format!("{base}{path}");
            let mut req = self.authed(self.client.request(method.clone(), &url));
            if let Some((b, content_type)) = &body {
                req = req
                    .header(reqwest::header::CONTENT_TYPE, *content_type)
                    .body(b.clone());
            }
            let resp = req.send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE && hop == 0 {
                if let Some(leader) = serde_json::from_str::<Value>(&text)
                    .ok()
                    .and_then(|v| v.get("leader")?.as_str().map(String::from))
                {
                    base = leader;
                    continue;
                }
            }
            return Ok((status, text));
        }
        unreachable!("the loop above always returns on hop 1");
    }

    async fn get(&self, path: &str) -> Result<(reqwest::StatusCode, String)> {
        self.request(reqwest::Method::GET, path, None).await
    }

    /// POST an arbitrary body — for `/invoke` (the payload belongs to the service, not to us).
    async fn post(&self, path: &str, body: Vec<u8>) -> Result<(reqwest::StatusCode, String)> {
        self.request(
            reqwest::Method::POST,
            path,
            Some((body, "application/octet-stream")),
        )
        .await
    }

    /// POST JSON — for admin endpoints using the `Json` extractor (it requires the content type).
    async fn post_json(&self, path: &str, body: Vec<u8>) -> Result<(reqwest::StatusCode, String)> {
        self.request(
            reqwest::Method::POST,
            path,
            Some((body, "application/json")),
        )
        .await
    }

    async fn put_json(&self, path: &str, body: Vec<u8>) -> Result<(reqwest::StatusCode, String)> {
        self.request(reqwest::Method::PUT, path, Some((body, "application/json")))
            .await
    }

    async fn delete(&self, path: &str) -> Result<(reqwest::StatusCode, String)> {
        self.request(reqwest::Method::DELETE, path, None).await
    }
}

/// Prints the result: `--json` prints it verbatim; otherwise `render` turns it into a
/// table. HTTP errors (4xx/5xx) print the API's `error` to stderr and exit 1.
fn emit(
    cli: &Cli,
    status: reqwest::StatusCode,
    text: &str,
    render: impl Fn(&Value) -> String,
) -> Result<()> {
    if !status.is_success() {
        let msg = serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|v| v.get("error")?.as_str().map(String::from))
            .unwrap_or_else(|| text.to_string());
        return Err(CliError(format!("{}: {msg}", status.as_u16())));
    }
    if cli.json {
        println!("{text}");
        return Ok(());
    }
    // `204` has no body.
    let v: Value = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(text)?
    };
    println!("{}", render(&v));
    Ok(())
}

/// The number in a field, `0` if absent. In the same spirit as [`s`]: the diagnostic
/// table still prints when the API drops a field because it is empty.
fn n(v: &Value, key: &str) -> u64 {
    v.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// The "not yet certain" part of a key revocation: `pending` nodes, or sessions that
/// could not be listed (`listed: false`).
fn unconfirmed(v: &Value, id: &str) -> String {
    let nodes: Vec<&str> = v
        .get("pending")
        .and_then(Value::as_array)
        .map(|p| p.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut why = Vec::new();
    if !nodes.is_empty() {
        why.push(format!("NOT yet confirmed by: {}", nodes.join(", ")));
    }
    if v.get("listed").and_then(Value::as_bool) == Some(false) {
        why.push("could not LIST sessions, so the set of live nodes is unknown".to_string());
    }
    if why.is_empty() {
        why.push("not yet confirmed by every node".to_string());
    }
    format!(
        "{} (they pick it up within one key-registry refresh, or refuse authenticated requests while unsure the registry is fresh; check again: `cairn keys check {id}`)",
        why.join("; ")
    )
}

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("-")
}

// ─────────────────────────────────────────────────────────── table rendering

fn render_deployments(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        return "(no deployments registered)".into();
    }
    rows.iter()
        .map(|d| {
            let services = d
                .get("services")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            // The transport shows right on the first line: if `BIDI_STREAM` is declared
            // in the manifest and h2c cannot pass the proxy, every invocation of this
            // deployment fails at the transport, and the on-call person's first question
            // is "which mode is it running in".
            let current = d
                .get("currentFor")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(
                "{}  {}  (wire v{}, {})  {services}{}",
                s(d, "id"),
                s(d, "endpoint"),
                d.get("wireVersion").and_then(Value::as_u64).unwrap_or(0),
                d.get("protocolMode").and_then(Value::as_str).unwrap_or("?"),
                if current.is_empty() {
                    "  [not a revision of any service]".to_string()
                } else {
                    format!("  [revision: {current}]")
                },
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_services(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        return "(no services yet: register a deployment first)".into();
    }
    rows.iter()
        .map(|svc| {
            let handlers = svc
                .get("handlers")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|h| match h.get("ty").and_then(Value::as_str) {
                            Some(t) => format!("{} ({t})", s(h, "name")),
                            None => s(h, "name").to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            format!(
                "{}  [{}]  @ {}\n  handlers: {handlers}",
                s(svc, "name"),
                s(svc, "ty"),
                s(svc, "deployment"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_invoke(v: &Value) -> String {
    let id = s(v, "id");
    match s(v, "status") {
        "completed" => format!("completed  {id}\n{}", s(v, "output")),
        "waiting" => {
            format!("waiting    {id}\n(waiting on a timer or call: see `cairn status {id}`)")
        }
        other => format!(
            "{other}  {id}\ncode {}: {}",
            v.get("code").and_then(Value::as_u64).unwrap_or(0),
            s(v, "message")
        ),
    }
}

fn render_rules(v: &Value) -> String {
    let mut out = format!("rule book v{}", n(v, "version"));
    let rules = v
        .get("rules")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if rules.is_empty() {
        out.push_str("\n(no rules: nothing is limited)");
    }
    for r in &rules {
        out.push_str(&format!(
            "\n{:<24} {:>9}  L{}{}{}",
            s(r, "pattern"),
            s(r, "concurrency"),
            n(r, "level"),
            if r.get("disabled").and_then(Value::as_bool) == Some(true) {
                "  [disabled]"
            } else {
                ""
            },
            match s(r, "description") {
                "" => String::new(),
                d => format!("  — {d}"),
            }
        ));
    }
    // The warning about `*` is printed **right here**, not only in the docs: this is where
    // people just typed the command, and this is the misunderstanding that fails in the
    // most expensive direction.
    if let Some(w) = v.get("warning").and_then(Value::as_str) {
        out.push_str(&format!("\n\nwarning: {w}"));
    }
    out
}

fn render_limits(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        return "(no counter is non-zero)".into();
    }
    rows.iter()
        .map(|r| {
            let limit = r
                .get("limit")
                .and_then(Value::as_u64)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "∞".into());
            format!(
                "p{} {:<48} {}/{}{}",
                n(r, "partition"),
                s(r, "counter"),
                n(r, "used"),
                limit,
                match r.get("rule").and_then(Value::as_str) {
                    Some(rule) => format!("   (rule {rule})"),
                    None => String::new(),
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_vqueues(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    if rows.is_empty() {
        return "(no queue has work)".into();
    }
    rows.iter()
        .map(|q| {
            format!(
                "p{} {:<40} waiting {:<4} granted {:<6}{}",
                n(q, "partition"),
                s(q, "queue"),
                n(q, "waiting"),
                n(q, "granted"),
                match q.get("blockedOn").and_then(Value::as_str) {
                    Some(b) => format!(" blocked on {b}"),
                    None => String::new(),
                }
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_scheduler(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    rows.iter()
        .map(|p| {
            let blocked = p
                .get("blocked")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|b| format!("{}={}", s(b, "resource"), n(b, "count")))
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            format!(
                "partition {}\n  turns {}/{}  locks {}  queues {} (waiting {}, eligible {})\n  \
                 rules v{}  granted {}  blocked: {}",
                n(p, "partition"),
                n(p, "turnsInUse"),
                n(p, "maxTurns"),
                n(p, "locksHeld"),
                n(p, "queues"),
                n(p, "waiting"),
                n(p, "eligible"),
                n(p, "rulesVersion"),
                n(p, "granted"),
                if blocked.is_empty() {
                    "—".into()
                } else {
                    blocked
                },
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_status(v: &Value) -> String {
    let mut out = format!("{}  {}", s(v, "status"), s(v, "id"));
    if let Some(o) = v.get("output").and_then(Value::as_str) {
        out.push('\n');
        out.push_str(o);
    }
    out
}

fn render_journal(v: &Value) -> String {
    let rows = v.as_array().cloned().unwrap_or_default();
    rows.iter()
        .map(|e| {
            let name = e
                .get("name")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| s(e, "ty").to_string());
            let payload = s(e, "payload");
            let short = if payload.len() > 64 {
                format!("{}… ({} bytes)", &payload[..64], payload.len() / 2)
            } else {
                payload.to_string()
            };
            format!(
                "{:>3}  {:<28} {short}",
                e.get("index").and_then(Value::as_u64).unwrap_or(0),
                name,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A page of `GET /invocations?cursor=`.
fn render_page(v: &Value) -> String {
    let rows = v
        .get("invocations")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = if rows.is_empty() {
        "(no invocations)".to_string()
    } else {
        rows.iter()
            .map(|r| {
                let key = r
                    .get("key")
                    .and_then(Value::as_str)
                    .map(|k| format!(" key={k}"))
                    .unwrap_or_default();
                format!(
                    "{}  {:<9} {:<10} {}/{}{key}",
                    s(r, "id"),
                    s(r, "class"),
                    s(r, "status"),
                    s(r, "service"),
                    s(r, "handler"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    if v.get("partial").and_then(Value::as_bool).unwrap_or(false) {
        out.push_str(&format!("\n(PARTIAL: {})", v["warnings"]));
    }
    if let Some(c) = v.get("nextCursor").and_then(Value::as_str) {
        out.push_str(&format!("\n(next page: --cursor {c})"));
    }
    out
}

/// The result of `POST /query`: column names, then one line per row, cells separated by tabs.
fn render_sql(v: &Value) -> String {
    let cols: Vec<String> = v["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| c["name"].as_str().unwrap_or_default().to_string())
        .collect();
    let mut out = vec![cols.join("\t")];
    for r in v["rows"].as_array().into_iter().flatten() {
        out.push(
            r.as_array()
                .into_iter()
                .flatten()
                .map(|c| match c {
                    Value::String(s) => s.clone(),
                    Value::Null => "NULL".into(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join("\t"),
        );
    }
    if v["partial"].as_bool().unwrap_or(false) {
        out.push(format!("(PARTIAL: {})", v["warnings"]));
    }
    out.join("\n")
}

fn render_invocations(v: &Value) -> String {
    // `?scope=cluster` returns an object; local returns an array.
    let rows = v
        .as_array()
        .or_else(|| v.get("invocations").and_then(Value::as_array))
        .cloned()
        .unwrap_or_default();
    let mut note = String::new();
    if v.get("partial").and_then(Value::as_bool).unwrap_or(false) {
        note = format!(
            "\n(PARTIAL: nodes not answering: {}; ranges nobody covers: {})",
            v.get("missing").map(Value::to_string).unwrap_or_default(),
            v.get("missingRanges")
                .map(Value::to_string)
                .unwrap_or_default()
        );
    }
    if rows.is_empty() {
        return format!("(no invocations){note}");
    }
    let body = rows
        .iter()
        .map(|r| {
            let target = match (
                r.get("service").and_then(Value::as_str),
                r.get("handler").and_then(Value::as_str),
            ) {
                (Some(sv), Some(h)) => match r.get("key").and_then(Value::as_str) {
                    Some(k) => format!("{sv}/{h} key={k}"),
                    None => format!("{sv}/{h}"),
                },
                _ => "-".into(),
            };
            let attempt = match n(r, "attempt") {
                0 => String::new(),
                a => format!("  attempt {a}"),
            };
            let node = r
                .get("node")
                .and_then(Value::as_str)
                .map(|x| format!("  @{x}"))
                .unwrap_or_default();
            format!(
                "{}  p{:<3} {:<10} {target}  {}{attempt}{node}",
                s(r, "id"),
                r.get("partition").and_then(Value::as_u64).unwrap_or(0),
                s(r, "status"),
                s(r, "deployment"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{body}{note}")
}

fn render_stats(v: &Value) -> String {
    let rows = v
        .get("handlers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = format!(
        "{:<12} {:<28} {:<16} {:>6} {:>5} {:>5} {:>5} {:>6} {:>6} {:>8} {:>8} {:>8}",
        "node",
        "tenant/service/handler",
        "deployment",
        "turns",
        "done",
        "failed",
        "paused",
        "retry✗",
        "susp",
        "p50ms",
        "p99ms",
        "maxms"
    );
    for r in &rows {
        let f = |k: &str| r.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        out.push_str(&format!(
            "\n{:<12} {:<28} {:<16} {:>6} {:>5} {:>5} {:>5} {:>6} {:>6} {:>8.1} {:>8.1} {:>8.1}",
            s(r, "node"),
            format!("{}/{}/{}", s(r, "tenant"), s(r, "service"), s(r, "handler")),
            s(r, "deployment"),
            n(r, "turns"),
            n(r, "completed"),
            n(r, "failed"),
            n(r, "paused"),
            n(r, "failedAttempts"),
            n(r, "suspensions"),
            f("p50Ms"),
            f("p99Ms"),
            f("maxMs"),
        ));
    }
    if rows.is_empty() {
        out.push_str("\n(no turns in this window)");
    }
    if v.get("partial").and_then(Value::as_bool).unwrap_or(false) {
        out.push_str(&format!(
            "\n(PARTIAL: nodes not answering: {})",
            v.get("missing").map(Value::to_string).unwrap_or_default()
        ));
    }
    out
}

fn render_captures(v: &Value) -> String {
    let specs = v
        .get("captures")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let bundles = v
        .get("bundles")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = format!("tenant {}", s(v, "tenant"));
    for c in &specs {
        out.push_str(&format!(
            "\n  {}  {}/{}  deployment={}  remaining {}  expires {}",
            s(c, "id"),
            s(c, "service"),
            s(c, "handler"),
            c.get("deployment").and_then(Value::as_str).unwrap_or("*"),
            n(c, "remaining"),
            n(c, "expiresMs"),
        ));
    }
    if specs.is_empty() {
        out.push_str("\n  (no live capture requests)");
    }
    out.push_str(&format!("\nbundles ({}):", bundles.len()));
    for b in &bundles {
        out.push_str(&format!("\n  {}", b.as_str().unwrap_or("-")));
    }
    out
}

fn render_replay(v: &Value) -> String {
    if v.get("matches").and_then(Value::as_bool).unwrap_or(false) {
        format!("match  {} commands  {}", n(v, "upto"), s(v, "id"))
    } else {
        format!(
            "MISMATCH at command {}  journal {}  new code {}  {}",
            v.get("mismatchAt")
                .map(Value::to_string)
                .unwrap_or_else(|| "-".into()),
            s(v, "expected"),
            s(v, "got"),
            s(v, "message"),
        )
    }
}

fn render_cancel(v: &Value) -> String {
    if v.get("cancelled").and_then(Value::as_bool).unwrap_or(false) {
        format!("cancelled  {}", s(v, "id"))
    } else {
        format!(
            "nothing changed: the invocation was already {}  {}",
            s(v, "status"),
            s(v, "id")
        )
    }
}

fn render_cluster(v: &Value) -> String {
    let mut out = format!(
        "{} partition",
        v.get("partitionCount").and_then(Value::as_u64).unwrap_or(0)
    );
    if let Some(parts) = v.get("partitions").and_then(Value::as_array) {
        for p in parts {
            let n = p.get("partition").and_then(Value::as_u64).unwrap_or(0);
            match p.get("leader").and_then(Value::as_str) {
                Some(l) => out.push_str(&format!(
                    "\n  p{n:<3} leader {l} @ {}  (term {})",
                    s(p, "address"),
                    p.get("term").and_then(Value::as_u64).unwrap_or(0)
                )),
                None => out.push_str(&format!("\n  p{n:<3} (no leader yet)")),
            }
        }
    }
    if let Some(readers) = v.get("readers").and_then(Value::as_array) {
        for r in readers {
            out.push_str(&format!(
                "\n  reader p{} durable={} manifest={} {}",
                r.get("partition").and_then(Value::as_u64).unwrap_or(0),
                r.get("durableSeq").and_then(Value::as_u64).unwrap_or(0),
                r.get("manifestSeq").and_then(Value::as_u64).unwrap_or(0),
                if r.get("healthy").and_then(Value::as_bool).unwrap_or(false) {
                    "healthy"
                } else {
                    "WEDGED"
                }
            ));
        }
    }
    out
}

/// The range table of `/cluster`: one line per range, in key order.
fn render_ranges(v: &Value) -> String {
    let mut out = format!(
        "range table version {}, next id {}",
        n(v, "version"),
        n(v, "nextRangeId")
    );
    let Some(ranges) = v.get("ranges").and_then(Value::as_array) else {
        out.push_str("\n  (the server returned no range table: is it older than step 2.5?)");
        return out;
    };
    out.push_str(&format!(
        "\n  {:<6} {:<33} {:<4} {:<28} leader",
        "range", "key span", "gen", "intent"
    ));
    for r in ranges {
        let intent = match r.get("intent") {
            Some(i) if s(i, "kind") == "splitting" => {
                format!("split@{}→{},{}", s(i, "at"), n(i, "left"), n(i, "right"))
            }
            Some(i) if s(i, "kind") == "merging" => {
                format!("merge+{}→{}", n(i, "with"), n(i, "into"))
            }
            _ => "-".into(),
        };
        let leader = match r.get("leader").and_then(Value::as_str) {
            Some(l) => format!("{l} @ {} (term {})", s(r, "address"), n(r, "term")),
            None => "(no leader yet)".into(),
        };
        out.push_str(&format!(
            "\n  {:<6} {}-{} {:<4} {:<28} {leader}",
            n(r, "range"),
            s(r, "start"),
            s(r, "end"),
            n(r, "gen"),
            intent
        ));
    }
    out
}

// ─────────────────────────────────────────────────────────── commands

/// Takes `--flag value` out of `args`, returning the value if present.
fn take_flag(args: &mut Vec<String>, flag: &str) -> Result<Option<String>> {
    if let Some(i) = args.iter().position(|a| a == flag) {
        if i + 1 >= args.len() {
            return Err(CliError(format!("{flag} needs a value")));
        }
        let v = args.remove(i + 1);
        args.remove(i);
        return Ok(Some(v));
    }
    Ok(None)
}

/// `--value` takes JSON. If it does not parse, it is taken as a **string** — someone
/// typing `--value approved` almost always wants a string, and making them type
/// `'"approved"'` just to get past the shell makes them pay for our implementation detail.
fn parse_json_flag(raw: &str) -> Result<Value> {
    Ok(serde_json::from_str(raw).unwrap_or_else(|_| Value::from(raw)))
}

fn take_switch(args: &mut Vec<String>, flag: &str) -> bool {
    if let Some(i) = args.iter().position(|a| a == flag) {
        args.remove(i);
        return true;
    }
    false
}

async fn run() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(args.first().map(String::as_str), Some("--version" | "-V")) {
        println!("{}", cairn_server::version_line("cairn"));
        return Ok(());
    }
    let server = match take_flag(&mut args, "--server")? {
        Some(u) => u,
        None => std::env::var("CAIRNFLOW_SERVER").unwrap_or_else(|_| DEFAULT_SERVER.into()),
    };
    let json = take_switch(&mut args, "--json");
    let token = match take_flag(&mut args, "--token")? {
        Some(t) => Some(t),
        None => std::env::var("CAIRNFLOW_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    };
    let cli = Cli {
        server: server.trim_end_matches('/').to_string(),
        json,
        client: reqwest::Client::new(),
        token,
    };

    if args.is_empty() {
        usage();
    }
    let cmd = args.remove(0);
    match cmd.as_str() {
        "register" => {
            let uri = args.first().cloned().unwrap_or_else(|| usage());
            let body = serde_json::to_vec(&serde_json::json!({ "uri": uri }))?;
            let (st, text) = cli.post_json("/deployments", body).await?;
            emit(&cli, st, &text, |v| {
                format!(
                    "registered {} ({})\nservices: {}",
                    s(v, "endpoint"),
                    s(v, "id"),
                    v.get("services")
                        .and_then(Value::as_array)
                        .map(|a| a
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(", "))
                        .unwrap_or_default()
                )
            })
        }
        "deployments" if args.first().map(String::as_str) == Some("rm") => {
            let id = args.get(1).cloned().unwrap_or_else(|| usage());
            let (st, text) = cli.delete(&format!("/deployments/{id}")).await?;
            emit(&cli, st, &text, |_| format!("removed {id}"))
        }
        "deployments" => {
            let (st, text) = cli.get("/deployments").await?;
            emit(&cli, st, &text, render_deployments)
        }
        "revision" => {
            if args.len() < 2 {
                usage();
            }
            let (service, id) = (args[0].clone(), args[1].clone());
            let expect = match args.get(2).map(String::as_str) {
                Some("--expect") => Some(args.get(3).cloned().unwrap_or_else(|| usage())),
                Some(_) => usage(),
                None => None,
            };
            let body =
                serde_json::to_vec(&serde_json::json!({ "deployment": id, "expect": expect }))?;
            let (st, text) = cli
                .put_json(&format!("/services/{service}/revision"), body)
                .await?;
            emit(&cli, st, &text, |_| format!("{service} → {id}"))
        }
        "services" => {
            let (st, text) = cli.get("/services").await?;
            emit(&cli, st, &text, render_services)
        }
        "invoke" => {
            if args.len() < 2 {
                usage();
            }
            let mut rest = args.split_off(2);
            let (service, handler) = (args[0].clone(), args[1].clone());
            let key = take_flag(&mut rest, "--key")?;
            let input = match take_flag(&mut rest, "--input")? {
                Some(v) if v == "-" => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    std::io::stdin().read_to_end(&mut buf)?;
                    buf
                }
                Some(v) => v.into_bytes(),
                None => Vec::new(),
            };
            let scope = take_flag(&mut rest, "--scope")?;
            let limit_key = take_flag(&mut rest, "--limit-key")?;
            // Caught here instead of letting the ingress return `400`: the person typing
            // the command has the screen in front of them, and a round trip to learn they
            // forgot `--scope` is a wasted round trip.
            if limit_key.is_some() && scope.is_none() {
                return Err(CliError(
                    "--limit-key needs --scope: without a scope there is no \
                     counter to limit"
                        .into(),
                ));
            }
            let base = match &scope {
                Some(sc) => format!("/scope/{sc}/invoke/{service}/{handler}"),
                None => format!("/invoke/{service}/{handler}"),
            };
            let mut qs = Vec::new();
            if let Some(k) = &key {
                qs.push(format!("key={k}"));
            }
            if let Some(l) = &limit_key {
                qs.push(format!("limit-key={l}"));
            }
            let path = if qs.is_empty() {
                base
            } else {
                format!("{base}?{}", qs.join("&"))
            };
            let (st, text) = cli.post(&path, input).await?;
            emit(&cli, st, &text, render_invoke)
        }
        "status" => {
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let (st, text) = cli.get(&format!("/invocations/{id}")).await?;
            emit(&cli, st, &text, render_status)
        }
        "journal" => {
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let (st, text) = cli.get(&format!("/invocations/{id}/journal")).await?;
            emit(&cli, st, &text, render_journal)
        }
        "sql" => {
            if args.is_empty() {
                usage();
            }
            let body = serde_json::json!({ "sql": args.join(" ") });
            let (st, text) = cli
                .post_json("/query", body.to_string().into_bytes())
                .await?;
            emit(&cli, st, &text, render_sql)
        }
        "invocations" => {
            // Keyset paging: any one of these flags means taking the query layer path.
            let cursor = take_flag(&mut args, "--cursor")?;
            let tag = take_flag(&mut args, "--tag")?;
            let class = take_flag(&mut args, "--class")?;
            let service = take_flag(&mut args, "--service")?;
            let root = take_switch(&mut args, "--root");
            if cursor.is_some() || tag.is_some() || class.is_some() || service.is_some() || root {
                let enc = |v: &str| {
                    v.bytes()
                        .map(|b| match b {
                            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                                (b as char).to_string()
                            }
                            _ => format!("%{b:02X}"),
                        })
                        .collect::<String>()
                };
                let mut qs = vec![format!("cursor={}", enc(cursor.as_deref().unwrap_or("")))];
                for (k, v) in [("tag", &tag), ("class", &class), ("service", &service)] {
                    if let Some(v) = v {
                        qs.push(format!("{k}={}", enc(v)));
                    }
                }
                if let Some(v) = take_flag(&mut args, "--limit")? {
                    qs.push(format!("limit={}", enc(&v)));
                }
                if root {
                    qs.push("root=true".into());
                }
                let (st, text) = cli.get(&format!("/invocations?{}", qs.join("&"))).await?;
                return emit(&cli, st, &text, render_page);
            }
            let status = take_flag(&mut args, "--status")?;
            let limit = take_flag(&mut args, "--limit")?;
            let all_nodes = take_switch(&mut args, "--all-nodes");
            let mut qs = Vec::new();
            if let Some(v) = &status {
                qs.push(format!("status={v}"));
            }
            if let Some(v) = &limit {
                qs.push(format!("limit={v}"));
            }
            // `--all-nodes`: the node asked fans out itself through the internal port,
            // and tells the truth when a node is missing (`partial`).
            if all_nodes {
                qs.push("scope=cluster".into());
            }
            let path = if qs.is_empty() {
                "/invocations".to_string()
            } else {
                format!("/invocations?{}", qs.join("&"))
            };
            let (st, text) = cli.get(&path).await?;
            emit(&cli, st, &text, render_invocations)
        }
        "stats" => {
            let window = take_flag(&mut args, "--window")?;
            let tenant = take_flag(&mut args, "--tenant")?;
            let mut qs = Vec::new();
            if let Some(w) = window {
                qs.push(format!("window={w}"));
            }
            if let Some(t) = tenant {
                qs.push(format!("tenant={t}"));
            }
            if take_switch(&mut args, "--all-nodes") {
                qs.push("scope=cluster".into());
            }
            let path = if qs.is_empty() {
                "/stats/handlers".to_string()
            } else {
                format!("/stats/handlers?{}", qs.join("&"))
            };
            let (st, text) = cli.get(&path).await?;
            emit(&cli, st, &text, render_stats)
        }
        "capture" => match args.first().map(String::as_str) {
            Some("ls") => {
                args.remove(0);
                let path = match take_flag(&mut args, "--tenant")? {
                    Some(t) => format!("/admin/capture?tenant={t}"),
                    None => "/admin/capture".into(),
                };
                let (st, text) = cli.get(&path).await?;
                emit(&cli, st, &text, render_captures)
            }
            Some("get") => {
                args.remove(0);
                let out = take_flag(&mut args, "--out")?;
                let (Some(tenant), Some(id)) = (args.first().cloned(), args.get(1).cloned()) else {
                    usage()
                };
                let url = format!("{}/admin/capture/{tenant}/{id}", cli.server);
                let resp = cli.authed(cli.client.get(&url)).send().await?;
                let st = resp.status();
                let bytes = resp.bytes().await?;
                if !st.is_success() {
                    return emit(&cli, st, &String::from_utf8_lossy(&bytes), |_| {
                        String::new()
                    });
                }
                let file = out.unwrap_or_else(|| format!("{id}.bundle"));
                std::fs::write(&file, &bytes)?;
                println!("wrote {file} ({} bytes)", bytes.len());
                Ok(())
            }
            Some(_) => {
                let tenant = take_flag(&mut args, "--tenant")?;
                let deployment = take_flag(&mut args, "--deployment")?;
                let count = take_flag(&mut args, "--count")?
                    .map(|c| c.parse::<u32>())
                    .transpose()
                    .map_err(|_| CliError("--count is not a number".into()))?;
                let ttl = take_flag(&mut args, "--ttl")?
                    .map(|c| c.parse::<u64>())
                    .transpose()
                    .map_err(|_| CliError("--ttl is not a number (seconds)".into()))?;
                let (Some(service), Some(handler)) = (args.first().cloned(), args.get(1).cloned())
                else {
                    usage()
                };
                let mut body = serde_json::json!({ "service": service, "handler": handler });
                if let Some(t) = tenant {
                    body["tenant"] = t.into();
                }
                if let Some(d) = deployment {
                    body["deployment"] = d.into();
                }
                if let Some(c) = count {
                    body["count"] = c.into();
                }
                if let Some(t) = ttl {
                    body["ttl"] = t.into();
                }
                let (st, text) = cli
                    .post_json("/admin/capture", serde_json::to_vec(&body)?)
                    .await?;
                emit(&cli, st, &text, |v| {
                    format!(
                        "capture set {}  {}/{}  remaining {}  expires {}",
                        s(v, "id"),
                        s(v, "service"),
                        s(v, "handler"),
                        n(v, "remaining"),
                        n(v, "expiresMs")
                    )
                })
            }
            None => usage(),
        },
        "replay" => {
            // No cluster needed: the bundle carries the journal + deployment.
            let file = take_flag(&mut args, "--bundle")?.unwrap_or_else(|| usage());
            let endpoint = take_flag(&mut args, "--endpoint")?;
            let raw = std::fs::read(&file)?;
            let bundle = cairn_server::capture::Bundle::decode(&raw).map_err(CliError)?;
            // The CLI runs on the operator's machine, calling their own SDK: a tenant client
            // without a policy (no redirects, no proxy env).
            let sdk = cairn_server::egress::TenantHttp::h1(Default::default());
            let report = cairn_server::replay::replay_bundle(&sdk, &bundle, endpoint.as_deref())
                .await
                .map_err(|e| CliError(e.to_string()))?;
            let v = serde_json::to_value(&report)?;
            if cli.json {
                println!("{v}");
            } else {
                println!(
                    "bundle {file}: invocation {} on {} ({}), status when captured {}",
                    report.id,
                    bundle.meta.deployment.id,
                    endpoint
                        .as_deref()
                        .unwrap_or(&bundle.meta.deployment.endpoint),
                    bundle.meta.status
                );
                println!("{}", render_replay(&v));
            }
            if !report.matches {
                std::process::exit(3);
            }
            Ok(())
        }
        "cancel" => {
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let (st, text) = cli
                .post(&format!("/invocations/{id}/cancel"), Vec::new())
                .await?;
            emit(&cli, st, &text, render_cancel)
        }
        "purge" => {
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let (st, text) = cli
                .post(&format!("/invocations/{id}/purge"), Vec::new())
                .await?;
            emit(&cli, st, &text, |v| {
                format!(
                    "purged  {}",
                    v.get("id").and_then(Value::as_str).unwrap_or("?")
                )
            })
        }
        "resume" => {
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let (st, text) = cli
                .post(&format!("/invocations/{id}/resume"), Vec::new())
                .await?;
            emit(&cli, st, &text, |v| {
                format!(
                    "resumed  {}  → {}",
                    v.get("id").and_then(Value::as_str).unwrap_or("?"),
                    v.get("status").and_then(Value::as_str).unwrap_or("?")
                )
            })
        }
        // The four commands below are the door for **external systems**: approval forms,
        // webhooks, callbacks. Before them, waking a waiting workflow had to go round
        // through a `SHARED` handler written just for that.
        "signal" => {
            let value = take_flag(&mut args, "--value")?;
            let id = args.first().cloned().unwrap_or_else(|| usage());
            let name = args.get(1).cloned().unwrap_or_else(|| usage());
            let signal = match name.parse::<u32>() {
                Ok(n) => Value::from(n),
                Err(_) => Value::from(name),
            };
            let mut body = serde_json::Map::new();
            body.insert("signal".into(), signal);
            if let Some(v) = value {
                body.insert("value".into(), parse_json_flag(&v)?);
            }
            let (st, text) = cli
                .post_json(
                    &format!("/invocations/{id}/signal"),
                    serde_json::to_vec(&Value::Object(body)).expect("a JSON map always encodes"),
                )
                .await?;
            emit(&cli, st, &text, |v| {
                format!(
                    "sent  {}  → {}",
                    v.get("id").and_then(Value::as_str).unwrap_or("?"),
                    v.get("status").and_then(Value::as_str).unwrap_or("?")
                )
            })
        }
        "resolve" | "reject" => {
            let raw_value = take_flag(&mut args, "--value")?;
            let svc = args.first().cloned().unwrap_or_else(|| usage());
            let key = args.get(1).cloned().unwrap_or_else(|| usage());
            let name = args.get(2).cloned().unwrap_or_else(|| usage());
            let (path, body) = if cmd == "resolve" {
                let v = match raw_value {
                    Some(v) => parse_json_flag(&v)?,
                    None => Value::Null,
                };
                (format!("/promises/{svc}/{key}/{name}/resolve"), v)
            } else {
                let msg = args.get(3).cloned().unwrap_or_else(|| usage());
                let mut m = serde_json::Map::new();
                m.insert("message".into(), Value::from(msg));
                (
                    format!("/promises/{svc}/{key}/{name}/reject"),
                    Value::Object(m),
                )
            };
            let (st, text) = cli
                .post_json(
                    &path,
                    serde_json::to_vec(&body).expect("JSON always encodes"),
                )
                .await?;
            emit(&cli, st, &text, |v| {
                if v.get("completed").and_then(Value::as_bool) == Some(true) {
                    format!(
                        "completed, woke {} waiters",
                        v.get("woken").and_then(Value::as_u64).unwrap_or(0)
                    )
                } else {
                    "the promise already had a result: nothing changed".into()
                }
            })
        }
        "promise" => {
            let svc = args.first().cloned().unwrap_or_else(|| usage());
            let key = args.get(1).cloned().unwrap_or_else(|| usage());
            let name = args.get(2).cloned().unwrap_or_else(|| usage());
            let (st, text) = cli.get(&format!("/promises/{svc}/{key}/{name}")).await?;
            emit(&cli, st, &text, |v| {
                if v.get("completed").and_then(Value::as_bool) != Some(true) {
                    return "no result yet".into();
                }
                match (
                    v.get("value").and_then(Value::as_str),
                    v.get("failure").and_then(Value::as_str),
                ) {
                    (Some(x), _) => format!("resolved  {x}"),
                    (_, Some(f)) => format!("rejected  {f}"),
                    _ => "has a result".into(),
                }
            })
        }
        // Takes `(svc, key)` rather than `<id>`: the run id is deterministic from that
        // pair, so operators can look it up by what they *know* — nobody still holds the
        // id of a workflow that has been waiting for three days.
        "workflow" => {
            let svc = args.first().cloned().unwrap_or_else(|| usage());
            let key = args.get(1).cloned().unwrap_or_else(|| usage());
            let mut rest: Vec<String> = args[2..].to_vec();
            let scope = take_flag(&mut rest, "--scope")?;
            let attach = take_switch(&mut rest, "--attach");
            let base = match &scope {
                Some(s) => format!("/scope/{s}/workflow/{svc}/{key}"),
                None => format!("/workflow/{svc}/{key}"),
            };
            let (st, text) = if attach {
                cli.post(&format!("{base}/attach"), Vec::new()).await?
            } else {
                cli.get(&format!("{base}/output")).await?
            };
            emit(&cli, st, &text, render_status)
        }
        "rules" => {
            let sub = args.first().cloned().unwrap_or_else(|| "list".into());
            match sub.as_str() {
                "list" => {
                    let (st, text) = cli.get("/rules").await?;
                    emit(&cli, st, &text, render_rules)
                }
                "set" => {
                    if args.len() < 2 {
                        usage();
                    }
                    let pattern = args[1].clone();
                    let mut rest = args.split_off(2);
                    let concurrency = take_flag(&mut rest, "--concurrency")?;
                    let description = take_flag(&mut rest, "--description")?;
                    let disabled = take_switch(&mut rest, "--disabled");
                    let enabled = take_switch(&mut rest, "--enabled");
                    let mut body = serde_json::json!({ "pattern": pattern });
                    if let Some(c) = concurrency {
                        body["concurrency"] = match c.as_str() {
                            "unlimited" => Value::String("unlimited".into()),
                            n => Value::Number(
                                n.parse::<u64>()
                                    .map_err(|_| {
                                        CliError(
                                            "--concurrency must be a number or `unlimited`".into(),
                                        )
                                    })?
                                    .into(),
                            ),
                        };
                    }
                    if let Some(d) = description {
                        body["description"] = Value::String(d);
                    }
                    if disabled {
                        body["disabled"] = Value::Bool(true);
                    } else if enabled {
                        body["disabled"] = Value::Bool(false);
                    }
                    let (st, text) = cli.post_json("/rules", serde_json::to_vec(&body)?).await?;
                    emit(&cli, st, &text, render_rules)
                }
                "rm" => {
                    let pattern = args.get(1).cloned().unwrap_or_else(|| usage());
                    // `*` and `/` in the pattern must survive the URL path.
                    let enc = pattern.replace('*', "%2A").replace('/', "%2F");
                    let (st, text) = cli.delete(&format!("/rules/{enc}")).await?;
                    emit(&cli, st, &text, render_rules)
                }
                _ => usage(),
            }
        }
        "limits" => {
            let (st, text) = cli.get("/limits").await?;
            emit(&cli, st, &text, render_limits)
        }
        "vqueues" => {
            let (st, text) = cli.get("/vqueues").await?;
            emit(&cli, st, &text, render_vqueues)
        }
        "scheduler" => {
            let (st, text) = cli.get("/scheduler").await?;
            emit(&cli, st, &text, render_scheduler)
        }
        "keys" => match args.first().map(String::as_str) {
            None => {
                let (st, text) = cli.get("/admin/keys").await?;
                emit(&cli, st, &text, |v| {
                    let rows = v.as_array().cloned().unwrap_or_default();
                    if rows.is_empty() {
                        return "(no keys besides the bootstrap key)".into();
                    }
                    rows.iter()
                        .map(|k| format!("{}  {}  {}", s(k, "id"), s(k, "tenant"), s(k, "role")))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            Some("create") => {
                let (tenant, role) = match (args.get(1), args.get(2)) {
                    (Some(t), Some(r)) => (t.clone(), r.clone()),
                    _ => usage(),
                };
                let body =
                    serde_json::to_vec(&serde_json::json!({ "tenant": tenant, "role": role }))?;
                let (st, text) = cli.post_json("/admin/keys", body).await?;
                emit(&cli, st, &text, |v| {
                    format!(
                        "key {} ({} / {})\nsecret (shown ONCE, store it now):\n{}",
                        s(v, "id"),
                        s(v, "tenant"),
                        s(v, "role"),
                        s(v, "secret")
                    )
                })
            }
            Some("rm") => {
                let id = args.get(1).cloned().unwrap_or_else(|| usage());
                let (st, text) = cli.delete(&format!("/admin/keys/{id}")).await?;
                // `202`: written, but the nodes in `pending` have not confirmed they
                // applied it, or the sessions could not be listed (`listed: false`) —
                // report it clearly, exit 0.
                emit(&cli, st, &text, |v| {
                    if st == reqwest::StatusCode::ACCEPTED {
                        format!("deleted key {id}; {}", unconfirmed(v, &id))
                    } else {
                        format!("deleted key {id}")
                    }
                })
            }
            // Asking again after a `202` or an `rm` with an unclear outcome:
            // `GET /admin/keys/{id}/revocation`.
            Some("check") => {
                let id = args.get(1).cloned().unwrap_or_else(|| usage());
                let (st, text) = cli.get(&format!("/admin/keys/{id}/revocation")).await?;
                emit(&cli, st, &text, |v| {
                    if v.get("revoked").and_then(Value::as_bool) != Some(true) {
                        format!(
                            "key {id} is still in the registry: not revoked (`cairn keys rm {id}`)"
                        )
                    } else if v.get("confirmed").and_then(Value::as_bool) == Some(true) {
                        format!("key {id} is no longer accepted by any live node")
                    } else {
                        format!("key {id} is out of the registry; {}", unconfirmed(v, &id))
                    }
                })
            }
            Some(_) => usage(),
        },
        "cluster" => {
            let (st, text) = cli.get("/cluster").await?;
            emit(&cli, st, &text, render_cluster)
        }
        "ranges" => match args.first().map(String::as_str) {
            None => {
                let (st, text) = cli.get("/cluster").await?;
                emit(&cli, st, &text, render_ranges)
            }
            Some("split") => {
                let at = take_flag(&mut args, "--at")?;
                let id = args.get(1).cloned().unwrap_or_else(|| usage());
                let query = at.map(|a| format!("?at={a}")).unwrap_or_default();
                let (st, text) = cli
                    .post(&format!("/ranges/{id}/split{query}"), Vec::new())
                    .await?;
                emit(&cli, st, &text, render_ranges)
            }
            Some("merge") => {
                let (Some(left), Some(right)) = (args.get(1), args.get(2)) else {
                    usage()
                };
                let (st, text) = cli
                    .post(
                        &format!("/ranges/merge?left={left}&right={right}"),
                        Vec::new(),
                    )
                    .await?;
                emit(&cli, st, &text, render_ranges)
            }
            Some(_) => usage(),
        },
        _ => usage(),
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("cairn: {e}");
        std::process::exit(1);
    }
}
