// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The operations pack: the Grafana dashboard
//! (`deploy/grafana/cairnflow.json`), the alert rules
//! (`deploy/prometheus/alerts.yml`) and the runbooks (`docs/runbooks/`) may only mention
//! metrics and labels the node **really prints** on `/metrics`.
//!
//! The source of truth is [`render`] with every optional part filled in (per-range lines,
//! the shared-WAL floor, slow points), not a hand-copied list: renaming a
//! metric in code and forgetting the dashboard turns this test red.
//!
//! In addition:
//! - every alert rule has a `runbook_url` pointing at a real file, and the rule name
//!   appears in `docs/runbooks/README.md`;
//! - the README's "missing metrics" section (**proposed** names, not yet in code)
//!   is not checked — everything before it is.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    cairn_server::{
        l0::RangeL0Row,
        metrics::{render, sdk, Gauges, Metrics, PartitionGauge},
    },
    cairn_store::{
        dbstats::DbStatsSnapshot,
        health::SlowMeter,
        nodewal::{FloorReport, LogFloor},
        LogId, WriteStallTotals,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        path::{Path, PathBuf},
        time::Duration,
    },
};

/// Labels Prometheus attaches at scrape time, not printed by the node.
const TARGET_LABELS: [&str; 2] = ["instance", "job"];

/// The README section heading after which come proposed metric names.
const MISSING_HEADING: &str = "## Missing metrics";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The metric families the node prints, with their type and the label sets seen.
struct Families {
    kind: BTreeMap<String, String>,
    labels: BTreeMap<String, BTreeSet<String>>,
}

impl Families {
    /// The `/metrics` of a cluster node with every optional part.
    fn rendered() -> Self {
        for (_, _, c) in sdk::all() {
            c.bump(0);
        }
        // Lineage is printed only after a D-d plan.
        cairn_server::gc::lineage_stats::OBSERVED.store(true, std::sync::atomic::Ordering::Relaxed);
        // The scrub round age is printed only on a node that has been the runner.
        cairn_server::scrub::stats::note_runner(1);
        let slow = SlowMeter::new("ops-pack", Duration::from_secs(1));
        slow.renewed(Duration::from_millis(3));
        let one = |value| {
            vec![PartitionGauge {
                partition: 0,
                value,
            }]
        };
        let g = Gauges {
            partition_count: 1,
            table_version: 1,
            partitions_with_leader: 1,
            deployments: 1,
            reader_healthy: one(1),
            reader_durable_seq: one(1),
            reader_manifest_seq: one(1),
            range_turn_ms: one(5),
            schedules: 1,
            range_l0: vec![RangeL0Row {
                range: 0,
                l0_views: 1,
                write_stall: WriteStallTotals {
                    count: 1,
                    micros: 1,
                },
                db: Some(DbStatsSnapshot::default()),
                reject_ratio: 0.0,
                shed: [0; 3],
                external_depth: Some(1),
            }],
            nodewal: vec![FloorReport {
                node: "ops-pack".into(),
                logs: vec![LogFloor {
                    log: LogId::parse("ops-pack/1:0").expect("log id"),
                    floor: 1,
                    pinned_by: Some((0, 1)),
                    pinned_for: Duration::from_secs(1),
                }],
            }],
            slow: Some(slow),
            coordinator_term: Some(1),
            disk_cache: Some(cairn_store::disk_cache::DiskCacheSnapshot::default()),
            query_index: vec![("ByCreated", "readable", 1), ("ByCreated", "building", 0)],
            tenants_book_rejected: 1,
            host: Some(cairn_server::host::HostStats {
                rss_bytes: Some(1),
                memory_limit_bytes: Some(2),
                disks: vec![("data", 1, 2), ("disk_cache", 1, 2)],
            }),
        };
        Self::parse(&render(&Metrics::default(), &g))
    }

    fn parse(text: &str) -> Self {
        let mut kind = BTreeMap::new();
        let mut labels: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut it = rest.split_whitespace();
                let (Some(n), Some(k)) = (it.next(), it.next()) else {
                    panic!("broken TYPE line: {line}");
                };
                kind.insert(n.to_string(), k.to_string());
                continue;
            }
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let Some(open) = line.find('{') else { continue };
            let sample = &line[..open];
            let close = line.rfind('}').expect("missing '}'");
            let family = strip_histogram_suffix(sample)
                .filter(|base| kind.get(*base).is_some_and(|k| k == "histogram"))
                .unwrap_or(sample);
            let set = labels.entry(family.to_string()).or_default();
            for pair in line[open + 1..close].split(',') {
                if let Some((k, _)) = pair.split_once('=') {
                    set.insert(k.to_string());
                }
            }
        }
        Self { kind, labels }
    }

    /// The family of a referenced name (a histogram's `_bucket`/`_sum`/`_count` sample
    /// names are accepted), or `None` if the node does not print that name.
    fn family_of<'a>(&self, name: &'a str) -> Option<&'a str> {
        if self.kind.contains_key(name) {
            return Some(name);
        }
        strip_histogram_suffix(name).filter(|b| self.kind.get(*b).is_some_and(|k| k == "histogram"))
    }

    /// The valid labels for a referenced name.
    fn labels_of(&self, name: &str) -> BTreeSet<String> {
        let fam = self.family_of(name).unwrap_or(name);
        let mut out: BTreeSet<String> = self.labels.get(fam).cloned().unwrap_or_default();
        out.extend(TARGET_LABELS.iter().map(|s| s.to_string()));
        if name.ends_with("_bucket") {
            out.insert("le".into());
        }
        out
    }
}

fn strip_histogram_suffix(name: &str) -> Option<&str> {
    ["_bucket", "_sum", "_count"]
        .iter()
        .find_map(|s| name.strip_suffix(s))
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Every `cairnflow_…` in a text, skipping abbreviated names with `*` (e.g.
/// `cairnflow_sdk_*_total`) and names inside a path or a longer word.
fn names_in(text: &str) -> Vec<(String, usize)> {
    let mut out = Vec::new();
    let b = text.as_bytes();
    let mut i = 0;
    while let Some(off) = text[i..].find("cairnflow_") {
        let start = i + off;
        let mut end = start;
        while end < b.len() && is_ident(b[end] as char) {
            end += 1;
        }
        let prev_ok = start == 0 || !(is_ident(b[start - 1] as char) || b[start - 1] == b'/');
        let next = b.get(end).copied();
        if prev_ok && next != Some(b'*') && next != Some(b'/') {
            out.push((text[start..end].to_string(), start));
        }
        i = end.max(start + 1);
    }
    out
}

/// Checks a PromQL expression: metric names, the labels in the `{…}` following a name, and
/// the labels in `by (…)`/`without (…)`.
fn check_expr(f: &Families, where_: &str, expr: &str, errors: &mut Vec<String>) {
    let mut allowed_by: BTreeSet<String> = TARGET_LABELS.iter().map(|s| s.to_string()).collect();
    for (name, at) in names_in(expr) {
        if f.family_of(&name).is_none() {
            errors.push(format!(
                "{where_}: metric '{name}' is not on /metrics — `{expr}`"
            ));
            continue;
        }
        let ok = f.labels_of(&name);
        allowed_by.extend(ok.iter().cloned());
        let rest = &expr[at + name.len()..];
        if let Some(sel) = rest.strip_prefix('{') {
            let sel = &sel[..sel.find('}').unwrap_or(sel.len())];
            for m in sel.split(',') {
                let label = m.split(['=', '!', '~']).next().unwrap_or("").trim();
                if !label.is_empty() && !ok.contains(label) {
                    errors.push(format!(
                        "{where_}: label '{label}' of '{name}' is not on /metrics (have: {ok:?})"
                    ));
                }
            }
        }
    }
    for kw in ["by (", "by(", "without (", "without("] {
        let mut s = expr;
        while let Some(p) = s.find(kw) {
            let after = &s[p + kw.len()..];
            let list = &after[..after.find(')').unwrap_or(after.len())];
            for l in list.split(',').map(str::trim).filter(|l| !l.is_empty()) {
                if !allowed_by.contains(l) {
                    errors.push(format!(
                        "{where_}: grouping by label '{l}' that no metric of `{expr}` has"
                    ));
                }
            }
            s = after;
        }
    }
}

fn check_prose(f: &Families, where_: &str, text: &str, errors: &mut Vec<String>) {
    for (name, _) in names_in(text) {
        if f.family_of(&name).is_none() {
            errors.push(format!(
                "{where_}: mentions metric '{name}' that the node does not print"
            ));
        }
    }
}

fn walk_json(v: &serde_json::Value, path: &str, out: &mut Vec<(String, String, String)>) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, x) in m {
                let p = format!("{path}.{k}");
                if let serde_json::Value::String(s) = x {
                    out.push((p.clone(), k.clone(), s.clone()));
                }
                walk_json(x, &p, out);
            }
        }
        serde_json::Value::Array(a) => {
            for (i, x) in a.iter().enumerate() {
                walk_json(x, &format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

#[test]
fn rendered_metrics_cover_every_family() {
    // An anchor for the test itself: the full render must have the families that appear only
    // with optional parts — otherwise the checks below run on an incomplete set.
    let f = Families::rendered();
    for must in [
        "cairnflow_range_l0_views",
        "cairnflow_range_slatedb_l0_ssts",
        "cairnflow_nodewal_floor_pinned",
        "cairnflow_slow_score",
        "cairnflow_object_put_seconds",
        "cairnflow_reader_healthy",
        "cairnflow_sdk_rr_turns_by_range_total",
        "cairnflow_sdk_bytes_total",
        "cairnflow_range_turn_ms_total",
        "cairnflow_calls_total",
        "cairnflow_interrange_messages_total",
        "cairnflow_interrange_sync_seconds",
        "cairnflow_interrange_send_seconds",
        "cairnflow_outbox_rows_read_total",
        "cairnflow_schedules",
        "cairnflow_schedule_fires_total",
        "cairnflow_schedule_fire_lag_seconds",
        "cairnflow_range_engine_hold_max_ms",
        "cairnflow_range_engine_holds_total",
        "cairnflow_range_external_depth",
        "cairnflow_owner_tombstones",
        "cairnflow_retired_dbs",
        "cairnflow_dirty_yields_total",
        "cairnflow_scrub_last_complete_age_seconds",
    ] {
        assert!(
            f.kind.contains_key(must),
            "render lacks {must}: {:?}",
            f.kind.keys()
        );
    }
    assert!(f.labels["cairnflow_range_l0_shed_total"].contains("kind"));
    assert!(f.labels["cairnflow_range_engine_hold_max_ms"].contains("holder"));
    assert!(f.labels["cairnflow_failstop_total"].contains("cause"));
    assert!(f.labels["cairnflow_object_put_seconds"].contains("kind"));
    assert!(f.labels["cairnflow_nodewal_floor_age_seconds"].contains("node"));
    assert_eq!(
        f.labels["cairnflow_schedule_fires_total"],
        BTreeSet::from(["outcome".to_string()])
    );
    // Bytes to the SDK: exactly two bounded labels, no service name.
    assert_eq!(
        f.labels["cairnflow_sdk_bytes_total"],
        BTreeSet::from(["mode".to_string(), "dir".to_string()])
    );
    // Cross-range labels: bounded, no range id, no service name.
    assert_eq!(
        f.labels["cairnflow_calls_total"],
        BTreeSet::from(["placement".to_string(), "key_class".to_string()])
    );
    assert_eq!(
        f.labels["cairnflow_interrange_messages_total"],
        BTreeSet::from(["kind".to_string(), "outcome".to_string()])
    );
}

#[test]
fn grafana_dashboard_names_only_real_metrics() {
    let f = Families::rendered();
    let v: serde_json::Value = serde_json::from_str(&read("deploy/grafana/cairnflow.json"))
        .expect("the dashboard is valid JSON");
    let mut strings = Vec::new();
    walk_json(&v, "$", &mut strings);
    let mut errors = Vec::new();
    let mut exprs = 0;
    for (path, key, s) in &strings {
        match key.as_str() {
            "expr" => {
                exprs += 1;
                check_expr(&f, path, s, &mut errors);
            }
            "query" | "definition" => check_expr(&f, path, s, &mut errors),
            _ => check_prose(&f, path, s, &mut errors),
        }
    }
    assert!(
        exprs > 20,
        "the dashboard has only {exprs} expressions — wrong file read?"
    );
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn alert_rules_name_only_real_metrics_and_link_runbooks() {
    let f = Families::rendered();
    let text = read("deploy/prometheus/alerts.yml");
    let readme = read("docs/runbooks/README.md");
    let mut errors = Vec::new();
    let mut alerts = Vec::new();
    let mut current: Option<String> = None;
    let mut with_runbook = BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        let where_ = format!("alerts.yml:{}", n + 1);
        let t = line.trim_start();
        if let Some(name) = t.strip_prefix("- alert:") {
            let name = name.trim().to_string();
            alerts.push(name.clone());
            current = Some(name);
        } else if let Some(expr) = t.strip_prefix("expr:") {
            check_expr(&f, &where_, expr.trim(), &mut errors);
        } else if let Some(url) = t.strip_prefix("runbook_url:") {
            let url = url.trim();
            if !repo_root().join(url).is_file() {
                errors.push(format!(
                    "{where_}: runbook_url {url} does not point at any file"
                ));
            }
            if let Some(a) = &current {
                with_runbook.insert(a.clone());
            }
        } else {
            check_prose(&f, &where_, line, &mut errors);
        }
    }
    assert!(alerts.len() >= 5, "only {} rules read", alerts.len());
    for a in &alerts {
        if !with_runbook.contains(a) {
            errors.push(format!("rule {a} has no runbook_url"));
        }
        if !readme.contains(a.as_str()) {
            errors.push(format!("rule {a} is not in docs/runbooks/README.md"));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn runbooks_name_only_real_metrics() {
    let f = Families::rendered();
    let dir = repo_root().join("docs/runbooks");
    let mut errors = Vec::new();
    let mut files = 0;
    for e in std::fs::read_dir(&dir).expect("docs/runbooks") {
        let p = e.expect("entry").path();
        if p.extension().is_none_or(|x| x != "md") {
            continue;
        }
        files += 1;
        let text = std::fs::read_to_string(&p).expect("read runbook");
        // After the missing-metrics heading come proposed names — not there yet, on purpose.
        let checked = text.split(MISSING_HEADING).next().unwrap_or("");
        let name = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        for (n, line) in checked.lines().enumerate() {
            check_prose(&f, &format!("{name}:{}", n + 1), line, &mut errors);
        }
    }
    assert!(files >= 6, "only {files} runbooks");
    assert!(
        read("docs/runbooks/README.md").contains(MISSING_HEADING),
        "the README must have the {MISSING_HEADING} section"
    );
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// The deployment samples (`deploy/docker-compose.yml`, the Helm chart `deploy/helm/cairnflow`)
/// pass only `cairnflow` flags that exist — renaming a flag and forgetting the samples means the
/// node in the sample does not start. Checked against `main.rs`'s flag table (`name: "--…"`).
#[test]
fn deploy_templates_pass_only_real_flags() {
    let main = read("crates/cairn-server/src/main.rs");
    let mut errors = Vec::new();
    let mut seen = 0;
    for file in [
        "deploy/docker-compose.yml",
        "deploy/helm/cairnflow/templates/statefulset.yaml",
    ] {
        let text = read(file);
        for (n, line) in text.lines().enumerate() {
            let Some(flag) = line.trim().strip_prefix("- --") else {
                continue;
            };
            let flag = flag.split_whitespace().next().unwrap_or_default();
            seen += 1;
            if !main.contains(&format!("name: \"--{flag}\"")) {
                errors.push(format!(
                    "{file}:{}: flag --{flag} does not exist in main.rs",
                    n + 1
                ));
            }
        }
    }
    assert!(seen >= 10, "only {seen} flags read");
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// **The sample compose does not expose the cluster to the network
/// with fixed keys.** Formerly every `ports:` bound `0.0.0.0` (Docker goes around the
/// host's firewall), the cluster-admin key was a fixed sample string
/// `cf_compose_demo_key_change_me` (29 bytes), and the cluster secret lay in the very bucket
/// the sample S3 key opens.
///
/// Must see: every published port starts with `127.0.0.1:`; `CAIRNFLOW_ADMIN_KEY`,
/// `CAIRNFLOW_TOKEN`, `CAIRNFLOW_CLUSTER_SECRET` come from compose's required variables
/// (`${…:?…}`), no default value in the file; the remaining sample credential is
/// marked "trial use only".
#[test]
fn compose_sample_is_loopback_only() {
    let compose = read("deploy/docker-compose.yml");
    let mut in_ports = false;
    for (n, line) in compose.lines().enumerate() {
        let t = line.trim();
        if t.starts_with('#') || t.is_empty() {
            continue;
        }
        if t == "ports:" {
            in_ports = true;
            continue;
        }
        if in_ports {
            if let Some(item) = t.strip_prefix("- ") {
                let v = item.trim_matches('"');
                assert!(
                    v.starts_with("127.0.0.1:"),
                    "line {}: published port not on loopback only: {t}",
                    n + 1
                );
                continue;
            }
            in_ports = false;
        }
    }
    for var in [
        "CAIRNFLOW_ADMIN_KEY",
        "CAIRNFLOW_TOKEN",
        "CAIRNFLOW_CLUSTER_SECRET",
    ] {
        let line = compose
            .lines()
            .find(|l| l.trim_start().starts_with(&format!("{var}:")))
            .unwrap_or_else(|| panic!("compose does not set {var}"));
        let value = line.split_once(':').map(|(_, v)| v.trim()).unwrap_or("");
        assert!(
            value.starts_with("${CAIRNFLOW_") && value.contains(":?"),
            "{var} must be a required compose variable, not a sample value: {line}"
        );
    }
    assert!(
        !compose.contains("cf_compose_demo_key_change_me"),
        "the fixed sample admin key is still in compose"
    );
    assert!(
        compose.contains("trial use only"),
        "the remaining sample credential (SeaweedFS's S3 key) must carry the trial-only note"
    );
}

/// `docs/self-hosting-security.md` tells self-hosters which flags turn each protection
/// on. Every backticked `--flag` in it must be a flag that `cairnflow --help` lists, so
/// the page cannot drift from the binary (a renamed or removed flag turns this red).
#[test]
fn self_hosting_security_flags_exist() {
    let doc = read("docs/self-hosting-security.md");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_cairnflow"))
        .arg("--help")
        .output()
        .expect("run cairnflow --help");
    let help = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let is_flag_char = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
    let known: BTreeSet<&str> = help
        .split(|c: char| !is_flag_char(c))
        .filter(|w| w.starts_with("--") && w.len() > 2)
        .collect();
    let mut flags = BTreeSet::new();
    for piece in doc.split('`').skip(1).step_by(2) {
        if let Some(rest) = piece.strip_prefix("--") {
            let name: String = rest.chars().take_while(|&c| is_flag_char(c)).collect();
            if !name.is_empty() {
                flags.insert(format!("--{name}"));
            }
        }
    }
    assert!(
        flags.len() >= 10,
        "found only {} flags in the page",
        flags.len()
    );
    let missing: Vec<_> = flags
        .iter()
        .filter(|f| !known.contains(f.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "flags in docs/self-hosting-security.md that `cairnflow --help` does not list: {missing:?}"
    );
}
