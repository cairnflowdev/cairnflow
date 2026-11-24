// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The `cairn` CLI's human-facing output is English** (the English public docs quote
//! the CLI's printed lines verbatim).
//!
//! `cairn` is only a thin shell over the HTTP API, so the test builds an axum server
//! returning canned JSON (the shape of the real API) instead of a node: what is
//! checked is what the **shell** prints — tables, status lines, flag errors, help —
//! not the node.
//!
//! | Action | Must see |
//! |---|---|
//! | `cairn` with no arguments | exit 2, English help, key roles listing all of `invoke \| view \| admin \| provision \| cluster-admin` |
//! | every read command on an empty list / a sample status | exit 0, the expected English sentence, no Vietnamese text |
//! | bad flags (`--count x`, a flag missing its value, `--limit-key` without `--scope`) | exit 1, an English error |
//! | `replay --bundle` with a file that is not a bundle | exit 1, an English error |
//!
//! "Vietnamese text" = an accented Latin letter (U+00C0–U+024F, U+1EA0–U+1EF9) or a
//! combining mark (U+0300–U+036F). Arrows, `…`, `—`, `∞`, `✗` are fine.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    axum::{
        routing::{get, post},
        Json, Router,
    },
    serde_json::json,
    std::process::Output,
};

fn vietnamese(s: &str) -> Option<char> {
    s.chars().find(|c| {
        ('\u{00C0}'..='\u{024F}').contains(c)
            || ('\u{1EA0}'..='\u{1EF9}').contains(c)
            || ('\u{0300}'..='\u{036F}').contains(c)
    })
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn cairn(server: Option<&str>, args: &[&str]) -> Output {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_cairn"));
    cmd.env_clear();
    if let Some(s) = server {
        cmd.args(["--server", s]);
    }
    cmd.args(args).output().expect("run cairn")
}

/// Exits with `code`, prints `want`, no Vietnamese text.
#[track_caller]
fn expect(out: &Output, code: i32, want: &str, what: &str) {
    let t = text(out);
    assert_eq!(out.status.code(), Some(code), "{what}: {t}");
    assert!(t.contains(want), "{what}: no {want:?} in:\n{t}");
    if let Some(c) = vietnamese(&t) {
        panic!("{what}: Vietnamese text {c:?} left in:\n{t}");
    }
}

/// A fake axum server returning canned JSON, in the shape of the real API.
async fn fake_api() -> String {
    let empty = || async { Json(json!([])) };
    let app = Router::new()
        .route("/deployments", get(empty))
        .route("/services", get(empty))
        .route(
            "/invocations",
            get(
                |q: axum::extract::RawQuery| async move {
                    match q.0 {
                        Some(q) if q.contains("cursor=") => Json(json!({"invocations": []})),
                        _ => Json(json!([])),
                    }
                },
            ),
        )
        .route("/rules", get(|| async { Json(json!({"version": 1, "rules": []})) }))
        .route("/limits", get(empty))
        .route("/vqueues", get(empty))
        .route("/admin/keys", get(empty))
        .route(
            "/stats/handlers",
            get(|| async { Json(json!({"handlers": []})) }),
        )
        .route(
            "/admin/capture",
            get(|| async { Json(json!({"tenant": "default", "captures": [], "bundles": []})) }),
        )
        .route(
            "/cluster",
            get(|| async {
                Json(json!({
                    "partitionCount": 1,
                    "partitions": [{"partition": 0}],
                    "readers": [{"partition": 0, "durableSeq": 1, "manifestSeq": 1, "healthy": true}],
                    "version": 3,
                    "nextRangeId": 1,
                    "ranges": [{"range": 0, "start": "0000000000000000", "end": "ffffffffffffffff", "gen": 0}],
                }))
            }),
        )
        .route(
            "/invocations/{id}/cancel",
            post(|| async { Json(json!({"cancelled": false, "status": "completed", "id": "x"})) }),
        )
        .route(
            "/promises/{svc}/{key}/{name}",
            get(|| async { Json(json!({"completed": false})) }),
        )
        .route(
            "/admin/keys/{id}/revocation",
            get(|| async { Json(json!({"revoked": true, "confirmed": false, "pending": ["n2"]})) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

#[test]
fn usage_is_english_and_lists_every_key_role() {
    let out = cairn(None, &[]);
    expect(&out, 2, "Commands:", "usage");
    expect(&out, 2, "invoke | view | admin |", "usage: key roles");
    expect(&out, 2, "provision | cluster-admin", "usage: key roles");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_commands_print_english() {
    let api = fake_api().await;
    let cases: &[(&[&str], &str)] = &[
        (&["deployments"], "(no deployments registered)"),
        (&["services"], "(no services yet"),
        (&["invocations"], "(no invocations)"),
        (&["invocations", "--cursor", ""], "(no invocations)"),
        (&["rules"], "(no rules: nothing is limited)"),
        (&["limits"], "(no counter is non-zero)"),
        (&["vqueues"], "(no queue has work)"),
        (&["keys"], "(no keys besides the bootstrap key)"),
        (&["stats"], "(no turns in this window)"),
        (&["capture", "ls"], "(no live capture requests)"),
        (&["cluster"], "(no leader yet)"),
        (&["ranges"], "range table version 3"),
        (&["cancel", "x"], "nothing changed"),
        (&["promise", "s", "k", "n"], "no result yet"),
        (&["keys", "check", "k1"], "NOT yet confirmed by: n2"),
    ];
    for (args, want) in cases {
        let api = api.clone();
        let what = format!("cairn {}", args.join(" "));
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let out = tokio::task::spawn_blocking(move || {
            let a: Vec<&str> = args.iter().map(String::as_str).collect();
            cairn(Some(&api), &a)
        })
        .await
        .expect("join");
        expect(&out, 0, want, &what);
    }
}

#[test]
fn flag_errors_are_english() {
    // No server is listening: every error below is caught before a request.
    let none = Some("http://127.0.0.1:9");
    expect(
        &cairn(none, &["capture", "S", "h", "--count", "x"]),
        1,
        "--count is not a number",
        "--count",
    );
    expect(
        &cairn(none, &["capture", "S", "h", "--ttl", "x"]),
        1,
        "--ttl is not a number",
        "--ttl",
    );
    expect(
        &cairn(none, &["invoke", "S", "h", "--limit-key", "k"]),
        1,
        "--limit-key needs --scope",
        "--limit-key",
    );
    expect(
        &cairn(none, &["invoke", "S", "h", "--key"]),
        1,
        "--key needs a value",
        "flag missing its value",
    );
    expect(
        &cairn(none, &["rules", "set", "x", "--concurrency", "many"]),
        1,
        "--concurrency must be a number",
        "--concurrency",
    );
}

#[test]
fn replay_of_a_non_bundle_is_english() {
    let dir = std::env::temp_dir().join(format!("cairn-cli-text-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let file = dir.join("junk.bundle");
    std::fs::write(&file, b"not a bundle").expect("write");
    let out = cairn(None, &["replay", "--bundle", file.to_str().expect("utf8")]);
    let _ = std::fs::remove_dir_all(&dir);
    expect(&out, 1, "not a capture bundle", "replay");
}
