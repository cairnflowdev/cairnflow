// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operations reports: `cairn-ctl … --report-to-bucket
//! <id>` running next to a cell writes its result to `ops-reports/<id>`, and a control plane —
//! holding only a `provision` key — reads it through `GET /admin/ops-reports[/{id}]`.
//!
//! Runs the real **binaries** on both sides: what needs checking is the exit code, `cairn-ctl`'s
//! "reserve, then run, then write" order, and that the node's route reads exactly what it wrote.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{ensure, Context, Result},
    common::binary::Binary,
    reqwest::{Client, StatusCode},
    serde_json::Value,
    sha2::{Digest, Sha256},
};

const PROVISION_KEY: &str = "cf_test_ops_reports_provision_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_ops_reports_32_bytes";

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

/// `cairn-ctl --data-dir <dir> <args…>`, with a clean environment.
fn ctl(dir: &str, args: &[&str]) -> Result<Run> {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_cairn-ctl"))
        .env_clear()
        .args(["--data-dir", dir, "--who", "e7-test"])
        .args(args)
        .output()
        .context("run cairn-ctl")?;
    Ok(Run {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

fn report_files(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("ops-reports"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// **From the command to the `provision` key.**
///
/// Order:
/// 1. A node binary on `--data-dir`, holding only the hash of the `provision` key.
/// 2. `cairn-ctl --report-to-bucket op-1 cluster status` on the same directory.
/// 3. Run again with `op-1`.
/// 4. `--report-to-bucket op-2 backup --to <a target inside the source>` (the command
///    fails on its arguments).
/// 5. The flag on a command that does not take reports (`keys create`), and a malformed id.
/// 6. The `provision` key reads the list and each report; a tenant's `admin` key
///    tries the same routes.
///
/// Must see: (2) code 0, report `op-1` has `command = "cluster status"`, `ok`,
/// the binary's `knows` and the cluster's `result.version`; (3) code 3, the command does not
/// run (empty stdout), `op-1` unchanged; (4) code 2 and still a report `op-2`,
/// `ok = false`, `exitCode = 2`, `error` giving the reason, `result = null`; (5) code 2,
/// no new object; (6) the list is `op-2` then `op-1`, `404` for an unused id,
/// `400` for a malformed id, `403` for the tenant key.
#[tokio::test]
async fn ctl_reports_reach_the_provision_key() -> Result<()> {
    let root = std::env::temp_dir().join(format!(
        "cairn-ops-reports-{}-{}",
        std::process::id(),
        cairn_store::entropy::now_ms()
    ));
    std::fs::create_dir_all(&root)?;
    let dir = root.to_string_lossy().into_owned();
    let r = async {
        let prov_hash: String = Sha256::digest(PROVISION_KEY.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let n = Binary::spawn(
            &["--data-dir", &dir, "--port", "0", "--internal-port", "0"],
            &[
                ("CAIRNFLOW_PROVISION_KEY_SHA256", prov_hash.as_str()),
                ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
            ],
        )?;
        let http = Client::new();
        let get = |path: String, key: String| {
            let http = http.clone();
            let url = format!("{}{path}", n.url);
            async move {
                let r = http.get(url).bearer_auth(key).send().await?;
                let code = r.status();
                let text = r.text().await?;
                Ok::<_, anyhow::Error>((code, text))
            }
        };

        // ── 2.
        let r = ctl(&dir, &["--report-to-bucket", "op-1", "cluster", "status"])?;
        ensure!(r.code == 0, "cluster status: {} {}", r.code, r.stderr);
        ensure!(r.stderr.contains("ops-reports/op-1"), "{}", r.stderr);
        let (code, text) = get("/admin/ops-reports/op-1".into(), PROVISION_KEY.into()).await?;
        ensure!(code == StatusCode::OK, "{code} {text}");
        let v: Value = serde_json::from_str(&text)?;
        let rep = &v["report"];
        ensure!(
            v["id"] == "op-1"
                && v["header"] == cairn_store::ops_report::OPS_REPORT_HEADER
                && rep["command"] == "cluster status"
                && rep["ok"] == true
                && rep["exitCode"] == 0
                && rep["knows"] == cairn_store::migrate::KNOWS
                && rep["binary"] == cairn_server::version_line("cairn-ctl")
                && rep["who"] == "e7-test"
                && rep["result"]["version"].is_u64(),
            "report op-1: {v}"
        );
        let first = text;

        // ── 3.
        let r = ctl(&dir, &["--report-to-bucket", "op-1", "cluster", "status"])?;
        ensure!(
            r.code == 3 && r.stdout.trim().is_empty(),
            "id already used: {} {} {}",
            r.code,
            r.stdout,
            r.stderr
        );
        let (_, again) = get("/admin/ops-reports/op-1".into(), PROVISION_KEY.into()).await?;
        ensure!(again == first, "op-1 was overwritten");

        // ── 4.
        let inside = format!("file://{dir}/bk");
        let r = ctl(
            &dir,
            &["--report-to-bucket", "op-2", "backup", "--to", &inside],
        )?;
        ensure!(
            r.code == 2,
            "backup into the source: {} {}",
            r.code,
            r.stderr
        );
        let (code, text) = get("/admin/ops-reports/op-2".into(), PROVISION_KEY.into()).await?;
        ensure!(code == StatusCode::OK, "{code} {text}");
        let v: Value = serde_json::from_str(&text)?;
        let rep = &v["report"];
        ensure!(
            rep["command"] == "backup"
                && rep["ok"] == false
                && rep["exitCode"] == 2
                && rep["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("is inside"))
                && rep["result"].is_null(),
            "report op-2: {v}"
        );

        // ── 5.
        let before = report_files(&root);
        for args in [
            &[
                "--report-to-bucket",
                "op-3",
                "keys",
                "create",
                "default",
                "admin",
                "--apply",
            ][..],
            &["--report-to-bucket", "op-3", "verify"],
            &["--report-to-bucket", "../op-3", "cluster", "status"],
            &["--report-to-bucket", ".op-3", "cluster", "status"],
        ] {
            let r = ctl(&dir, args)?;
            ensure!(
                r.code == 2,
                "{args:?}: {} {} {}",
                r.code,
                r.stdout,
                r.stderr
            );
            ensure!(
                !r.stdout.contains("cf_"),
                "{args:?} prints secrets: {}",
                r.stdout
            );
        }
        ensure!(report_files(&root) == before, "{:?}", report_files(&root));
        ensure!(before == ["op-1", "op-2"], "{before:?}");

        // ── 6.
        let (code, text) = get("/admin/ops-reports".into(), PROVISION_KEY.into()).await?;
        ensure!(code == StatusCode::OK, "{code} {text}");
        let v: Value = serde_json::from_str(&text)?;
        let ids: Vec<&str> = v["reports"]
            .as_array()
            .map(|a| a.iter().filter_map(|r| r["id"].as_str()).collect())
            .unwrap_or_default();
        ensure!(
            ids == ["op-2", "op-1"] && v["truncated"] == false,
            "list: {v}"
        );
        for (path, want) in [
            ("/admin/ops-reports/op-9", StatusCode::NOT_FOUND),
            ("/admin/ops-reports/-x", StatusCode::BAD_REQUEST),
        ] {
            let (code, text) = get(path.into(), PROVISION_KEY.into()).await?;
            ensure!(code == want, "{path}: {code} {text}");
        }
        let made: Value = http
            .post(format!("{}/admin/keys", n.url))
            .bearer_auth(PROVISION_KEY)
            .json(&serde_json::json!({ "tenant": "acme", "role": "admin" }))
            .send()
            .await?
            .json()
            .await?;
        let tenant_key = made["secret"].as_str().context("admin key")?.to_string();
        for path in ["/admin/ops-reports", "/admin/ops-reports/op-1"] {
            let (code, _) = get(path.into(), tenant_key.clone()).await?;
            ensure!(code == StatusCode::FORBIDDEN, "tenant key {path}: {code}");
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&root);
    r
}
