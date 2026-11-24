// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Graceful shutdown: `SIGTERM` ⇒ the node hands its ranges to a peer **at
//! once**, instead of making the peer wait for the lease to expire.
//!
//! This is the only test that runs the **real binary**: what it checks is how the
//! process reacts to a signal — `node::start` inside the test process has no
//! signal to receive. Two processes cannot share `memory://`, and `file://` does
//! not go through the cluster's CAS gate, so the test needs a real S3 store; it is
//! `#[ignore]` and runs only when called explicitly (the "real S3" row of
//! `scripts/check.sh` detects a local SeaweedFS) — same environment variables as
//! `failover.rs`:
//!
//! ```sh
//! scripts/dev-store/up.sh
//! cargo build --bin cairnflow
//! CAIRN_TEST_S3_ENDPOINT=http://localhost:18333 CAIRN_TEST_S3_URI=s3://walbench/sigterm \
//! CAIRN_TEST_S3_KEY=dev-only-access-key CAIRN_TEST_S3_SECRET=dev-only-not-a-secret CAIRN_TEST_S3_REGION=us-east-1 \
//!   cargo test -p cairn-server --test graceful_shutdown -- --include-ignored --nocapture
//! ```
//!
//! SeaweedFS, not MinIO: a local MinIO tears a lease object in two when the client
//! is killed mid-CAS — this test does not kill with `-9`, but its cleanup at the
//! end does.

// The test runs on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{anyhow, bail, ensure, Context, Result},
    common::{
        fake_sdk::{FakeSdk, Step},
        invocation, invoke, register,
    },
    std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
        sync::{mpsc, Arc, Mutex},
        time::{Duration, Instant},
    },
};

const PARTITIONS: usize = 4;
/// The binary's default TTL. Graceful shutdown must be far faster — that is the whole point.
const LEASE_TTL: Duration = Duration::from_secs(10);
/// The handover target: the ranges of a node hit by `kill -TERM` have a new owner
/// in < 2 s.
const HANDOVER: Duration = Duration::from_secs(2);

/// The store URI for this run, or `None` when there is no S3 store.
fn store_uri(what: &str) -> Option<String> {
    let uri = std::env::var("CAIRN_TEST_S3_URI").ok()?;
    // A fresh prefix per run: the bucket is shared, and a session or owner record
    // left over from an earlier run skews the scenario in ways that are hard to see.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    Some(format!("{uri}/{what}-{}-{nonce}", std::process::id()))
}

/// S3 environment variables for the child process, read from `CAIRN_TEST_S3_*`.
fn s3_env() -> Vec<(&'static str, String)> {
    let mut env = vec![
        ("AWS_ALLOW_HTTP", "true".to_string()),
        ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false".to_string()),
    ];
    for (k, v) in [
        ("AWS_ENDPOINT", "CAIRN_TEST_S3_ENDPOINT"),
        ("AWS_ACCESS_KEY_ID", "CAIRN_TEST_S3_KEY"),
        ("AWS_SECRET_ACCESS_KEY", "CAIRN_TEST_S3_SECRET"),
        ("AWS_REGION", "CAIRN_TEST_S3_REGION"),
    ] {
        if let Ok(val) = std::env::var(v) {
            env.push((k, val));
        }
    }
    env
}

/// One `cairnflow` process. Drop ⇒ `kill -9` — a test that fails halfway must not
/// leave an orphan node holding leases on the bucket.
struct Proc {
    child: Child,
    url: String,
    node_id: String,
    /// All of stdout + stderr, to print when the test fails.
    log: Arc<Mutex<Vec<String>>>,
}

impl Proc {
    fn spawn(store: &str, node_id: &str) -> Result<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_cairnflow"))
            .args([
                "--main-store",
                store,
                "--cluster",
                "--partitions",
                &PARTITIONS.to_string(),
                "--node-id",
                node_id,
                "--port",
                "0",
                "--lease-ttl-ms",
                &LEASE_TTL.as_millis().to_string(),
                "--deploy-refresh-ms",
                "200",
                // This test measures the handover, not API keys; messages
                // between the two nodes are still signed with the cluster secret in the bucket.
                "--no-auth",
            ])
            .envs(s3_env())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("could not run the cairnflow binary")?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::channel::<String>();
        // Read both pipes **continuously**: tracing logs go to stdout, and a full pipe
        // is a child process frozen in the middle of the test measuring it.
        for pipe in [
            Box::new(child.stdout.take().expect("stdout")) as Box<dyn std::io::Read + Send>,
            Box::new(child.stderr.take().expect("stderr")),
        ] {
            let log = Arc::clone(&log);
            let tx = tx.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines().map_while(|l| l.ok()) {
                    if let Some(url) = line.strip_prefix("cairnflow listening on ") {
                        let _ = tx.send(url.trim().to_string());
                    }
                    log.lock().unwrap().push(line);
                }
            });
        }
        let url = match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(url) => url,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                bail!(
                    "node {node_id} failed to start:\n{}",
                    log.lock().unwrap().join("\n")
                );
            }
        };
        Ok(Self {
            child,
            url,
            node_id: node_id.to_string(),
            log,
        })
    }

    fn sigterm(&self) -> Result<()> {
        let st = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        if !st.success() {
            bail!("kill -TERM returned {st}");
        }
        Ok(())
    }

    /// Waits for the process to exit, at most `limit`.
    fn wait_exit(&mut self, limit: Duration) -> Result<std::process::ExitStatus> {
        let started = Instant::now();
        loop {
            if let Some(st) = self.child.try_wait()? {
                return Ok(st);
            }
            if started.elapsed() > limit {
                bail!("node {} did not exit after {limit:?}", self.node_id);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn dump(&self) -> String {
        format!(
            "── log of {} ──\n{}",
            self.node_id,
            self.log.lock().unwrap().join("\n")
        )
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Range → leading node, per `GET /cluster` of `node_url`.
async fn leaders(node_url: &str) -> Result<Vec<(u64, Option<String>)>> {
    let v: serde_json::Value = reqwest::get(format!("{node_url}/cluster"))
        .await?
        .json()
        .await
        .context("/cluster is not JSON")?;
    v["ranges"]
        .as_array()
        .ok_or_else(|| anyhow!("/cluster lacks `ranges`: {v}"))?
        .iter()
        .map(|r| {
            let id = r["range"]
                .as_u64()
                .ok_or_else(|| anyhow!("range lacks id: {r}"))?;
            Ok((id, r["leader"].as_str().map(str::to_string)))
        })
        .collect()
}

/// Waits until `node_url` sees `want` hold, at most `limit`. Returns the wait time.
async fn wait_view(
    node_url: &str,
    limit: Duration,
    what: &str,
    want: impl Fn(&[(u64, Option<String>)]) -> bool,
) -> Result<Duration> {
    let started = Instant::now();
    loop {
        if let Ok(v) = leaders(node_url).await {
            if want(&v) {
                return Ok(started.elapsed());
            }
        }
        if started.elapsed() > limit {
            bail!("over {limit:?} and still not seen: {what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A cluster of two real `cairnflow` nodes. `kill -TERM` node A — which leads all
/// four ranges — and B leads every range A held within < 2 s, A exits with code 0,
/// and every invocation completed on A is still readable on B.
///
/// Before graceful shutdown existed, A died by the signal's default: no seal, no
/// session close, and B had to wait for A's lease to expire — measured at 20 s (the
/// 2×TTL rescue grace). With a graceful shutdown that does not notify peers, B still
/// only sees `closed` at its next observation round (TTL/5 = 2 s); with the notice, ~0.4 s.
///
/// `#[ignore]`: the `--workspace` row has no S3 store, and a test that skips itself
/// and reports PASS is lying. The "real S3" row of `scripts/check.sh`
/// runs it with `--include-ignored`; without `CAIRN_TEST_S3_URI` it fails.
#[tokio::test]
#[ignore = "needs a real S3 store — the S3 row of check.sh, see the top of the file"]
async fn sigterm_hands_ranges_over_before_ttl() -> Result<()> {
    let Some(store) = store_uri("sigterm") else {
        bail!("needs CAIRN_TEST_S3_URI (see the doc at the top of the file)");
    };
    let pid = std::process::id();
    let mut a = Proc::spawn(&store, &format!("sigterm-a-{pid}"))?;
    let r = async {
        // A lone cluster process becomes leader of all four ranges.
        let a_id = a.node_id.clone();
        wait_view(
            &a.url,
            Duration::from_secs(30),
            "A leads every range",
            |v| v.len() == PARTITIONS && v.iter().all(|(_, l)| l.as_deref() == Some(a_id.as_str())),
        )
        .await?;

        // Work completed on A: must survive intact after A leaves.
        let sdk = FakeSdk::builder()
            .object("Counter", "add", vec![Step::output("ok")])
            .spawn()
            .await?;
        register(&a.url, &sdk.url).await?;
        let mut ids = Vec::new();
        for i in 0..8 {
            let (code, body) =
                invoke(&a.url, "Counter", "add", Some(&format!("k{i}")), "1").await?;
            if code != 200 || body["output"] != "ok" {
                bail!("invoke on A returned {code}: {body}");
            }
            ids.push(
                body["id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("missing id: {body}"))?
                    .to_string(),
            );
        }

        let b = Proc::spawn(&store, &format!("sigterm-b-{pid}"))?;
        let r = async {
            // B has seen A lead every range — the mark from which to time the handover.
            wait_view(
                &b.url,
                Duration::from_secs(30),
                "B sees A lead every range",
                |v| {
                    v.len() == PARTITIONS
                        && v.iter().all(|(_, l)| l.as_deref() == Some(a_id.as_str()))
                },
            )
            .await?;

            a.sigterm()?;
            let b_id = b.node_id.clone();
            let took = wait_view(
                &b.url,
                LEASE_TTL * 3,
                "B leads every range of A",
                |v| {
                    v.len() == PARTITIONS
                        && v.iter().all(|(_, l)| l.as_deref() == Some(b_id.as_str()))
                },
            )
            .await?;
            eprintln!("B leads every range of A after {took:?}");

            let exit = a.wait_exit(Duration::from_secs(30))?;
            ensure!(
                exit.success(),
                "A must exit with code 0 after a graceful shutdown, exited with {exit}"
            );
            ensure!(
                took < HANDOVER,
                "B leads every range of A after {took:?} — must be < {HANDOVER:?} (TTL {LEASE_TTL:?})"
            );

            // Zero loss: every invocation completed on A is readable on B, still completed.
            for id in &ids {
                let (code, st) = invocation(&b.url, id).await?;
                ensure!(code == 200, "B cannot read {id}: {st}");
                ensure!(st["status"] == "completed", "{id} on B: {st}");
            }
            // And B serves new work on every range it just took.
            for i in 0..8 {
                let (code, body) =
                    invoke(&b.url, "Counter", "add", Some(&format!("after-{i}")), "1").await?;
                ensure!(code == 200, "invoke on B: {body}");
                ensure!(body["output"] == "ok", "invoke on B: {body}");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if r.is_err() {
            eprintln!("{}", b.dump());
        }
        r
    }
    .await;
    if r.is_err() {
        eprintln!("{}", a.dump());
    }
    r
}
