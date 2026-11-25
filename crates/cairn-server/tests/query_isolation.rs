// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Isolating `/query` in a child process** (security review of the query engine).
//!
//! The referee runs real hostile queries through `POST /query` of the `cairnflow` **binary**
//! (a real port, the binary's defaults — no flag choosing the mode). Must see for each query:
//! it ends within the deadline with a clear error (`400`/`422`) or a result; the node stays
//! alive; the node's RSS does not grow with the hostile query; the next ordinary query
//! answers correctly.
//!
//! `CAIRN_ISOLATION_CASES=s30,s32` runs only those cases (to measure red on the baseline
//! without burning the machine's RAM). A watchdog thread kills the node when its RSS
//! exceeds [`WATCHDOG_RSS`].

// The test runs on real time and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "a test outside the deterministic simulation may use real nondeterministic sources"
)]

mod common;

use {
    anyhow::{bail, ensure, Context, Result},
    common::binary::Binary,
    serde_json::{json, Value},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
};

const ADMIN_KEY: &str = "cf_test_query_isolation_admin_key_0123456789";
const SECRET: &str = "cf_test_cluster_secret_query_isolation_0123456789";

/// The deadline each query asks for in its body (`timeout_ms`).
const DEADLINE: Duration = Duration::from_secs(5);
/// A query must be answered within this (the deadline + a margin).
const ANSWER_WITHIN: Duration = Duration::from_secs(12);
/// The most the node's RSS may grow across all hostile queries.
const RSS_GROWTH: u64 = 256 << 20;
/// The watchdog kills the node when its RSS exceeds this (protects the machine when running red on the baseline).
const WATCHDOG_RSS: u64 = 2 << 30;

/// The RSS (bytes) of `pid`, `None` once the process is gone. Linux: `/proc` (a minimal
/// image has no `ps`); other systems: `ps`.
pub fn rss(pid: u32) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        let kib = status
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))?
            .split_whitespace()
            .next()?
            .parse::<u64>()
            .ok()?;
        Some(kib << 10)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse::<u64>()
            .ok()
            .map(|kib| kib << 10)
    }
}

/// `kill -9 pid`.
fn kill9(pid: u32) {
    // SAFETY: `kill` with the pid of a process the test spawned itself.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

fn node() -> Result<Binary> {
    Binary::spawn(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
        ],
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )
}

/// `POST /query`: code, body, time.
async fn query(http: &reqwest::Client, url: &str, sql: &str) -> Result<(u16, Value, Duration)> {
    let t0 = Instant::now();
    let resp = http
        .post(format!("{url}/query"))
        .bearer_auth(ADMIN_KEY)
        .json(&json!({ "sql": sql, "timeout_ms": DEADLINE.as_millis() as u64 }))
        .send()
        .await
        .context("POST /query")?;
    let code = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    Ok((code, body, t0.elapsed()))
}

/// The hostile queries of the review rounds (each case is named after its finding).
pub fn adversarial(case: &str) -> String {
    let list = |n: usize, f: &dyn Fn(usize) -> String, sep: &str| {
        (0..n).map(f).collect::<Vec<_>>().join(sep)
    };
    match case {
        // s3: a chain of set operations filling ~64 KiB.
        "s3" => format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(3_400)),
        // s10: a CTE reading the previous CTE twice, 40 levels deep.
        "s10" => format!(
            "WITH a0 AS (SELECT 1 AS x){} SELECT count(*) FROM a40",
            list(
                40,
                &|i| format!(
                    ",a{} AS (SELECT p.x FROM a{i} p JOIN a{i} q ON true)",
                    i + 1
                ),
                ""
            )
        ),
        // s20: `CUBE` over 30 columns, and 36 columns mixed into one column.
        "s20" => format!(
            "SELECT count(*) FROM (VALUES ({})) GROUP BY CUBE({})",
            list(30, &|i| i.to_string(), ","),
            list(30, &|i| format!("column{}", i + 1), ", ")
        ),
        "s20_mixed" => format!(
            "SELECT count(*) FROM (VALUES ({})) GROUP BY CUBE({}), column37",
            list(37, &|i| i.to_string(), ","),
            list(36, &|i| format!("column{}", i + 1), ", ")
        ),
        // s21 (a): a CTE without `AS`.
        "s21" => format!(
            "WITH a0 (SELECT 1 AS x){} SELECT count(*) FROM a40",
            list(
                40,
                &|i| format!(",a{} (SELECT p.x FROM a{i} p JOIN a{i} q ON true)", i + 1),
                ""
            )
        ),
        // s22: a `VALUES` CTE of 20 000 columns cross-joined 62 times.
        "s22" => format!(
            "WITH w AS (VALUES ({})) SELECT count(*) FROM {}",
            list(20_000, &|_| "1".into(), ","),
            list(62, &|i| format!("w a{i}"), ", ")
        ),
        // s30: expressions of `REPLACE` must not be carried along.
        "s30" => format!(
            "WITH c0 AS (SELECT 1 AS id){} SELECT count(*) FROM c30",
            list(
                30,
                &|i| {
                    format!(
                    ",c{} AS (SELECT * REPLACE ((SELECT max(p.id) FROM c{i} p JOIN c{i} q ON true) AS id) FROM sys_invocation)",
                    i + 1
                )
                },
                ""
            )
        ),
        // s31: copying expressions by `GROUP BY` position, by named window.
        "s31" => format!(
            "SELECT coalesce({}) AS a FROM (VALUES (1),(2)) t(x) GROUP BY {}",
            list(9_000, &|_| "x".into(), ", "),
            list(9_000, &|_| "1".into(), ", ")
        ),
        "s31_window" => format!(
            "SELECT greatest({}) FROM (VALUES (1),(2)) t(x) WINDOW w AS (PARTITION BY {})",
            list(2_000, &|_| "rank() OVER w".into(), ", "),
            list(10_000, &|_| "x".into(), ", ")
        ),
        // s32: chained `BETWEEN` — the optimizer doubles at every level.
        "s32" => format!(
            "SELECT count(*) FROM (VALUES (true),(false)) t(c) WHERE c{}",
            " BETWEEN false AND true".repeat(40)
        ),
        // s33: string values amplified outside the pool.
        "s33_concat" => format!(
            "WITH c0 AS (SELECT '{}' AS s){} SELECT length(s) FROM c8",
            "x".repeat(1_000),
            list(
                8,
                &|i| format!(
                    ", c{} AS (SELECT concat(s,s,s,s,s,s,s,s) AS s FROM c{i})",
                    i + 1
                ),
                ""
            )
        ),
        "s33_replace" => {
            let a = "a".repeat(20_000);
            format!("SELECT length(replace(replace('{a}', 'a', '{a}'), 'a', '{a}'))")
        }
        other => panic!("unknown case {other}"),
    }
}

/// Every case, cheap queries (refused by the checker) first.
pub const CASES: &[&str] = &[
    "s3",
    "s10",
    "s20",
    "s20_mixed",
    "s21",
    "s22",
    "s30",
    "s31",
    "s31_window",
    "s32",
    "s33_concat",
    "s33_replace",
];

/// An ordinary query after each hostile one: the node answers correctly.
async fn healthy(http: &reqwest::Client, url: &str) -> Result<()> {
    let (code, body, _) = query(http, url, "SELECT 1 + 1 AS x").await?;
    ensure!(code == 200, "the ordinary query returned {code}: {body}");
    ensure!(body["rows"] == json!([[2]]), "wrong plain query: {body}");
    let (code, body, _) = query(http, url, "SELECT count(*) AS n FROM sys_invocation").await?;
    ensure!(code == 200, "sys_invocation returned {code}: {body}");
    ensure!(body["rows"] == json!([[0]]), "wrong sys_invocation: {body}");
    Ok(())
}

/// **The referee for every hostile case over real HTTP.** For each hostile query: an answer
/// within [`ANSWER_WITHIN`] with `400`/`422` (or `200`), the node alive, the next ordinary
/// query correct; the node's RSS grows ≤ [`RSS_GROWTH`] across the whole series.
///
/// On a build that runs queries inside the node process (before isolation): `s30` plans
/// 2³⁰ copies, `s32` 2⁴⁰ nodes, `s33_*` tens of GB outside the pool — the node hangs
/// past the deadline, its RSS exceeds the watchdog limit, or it dies.
#[tokio::test(flavor = "multi_thread")]
async fn adversarial_queries_do_not_kill_the_node() -> Result<()> {
    let mut n = node()?;
    let pid = n.pid();
    let url = n.url.clone();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    let killed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let (killed, stop) = (killed.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if rss(pid).is_some_and(|r| r > WATCHDOG_RSS) {
                    killed.store(true, Ordering::Relaxed);
                    kill9(pid);
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    let only = std::env::var("CAIRN_ISOLATION_CASES").ok();
    let cases: Vec<&str> = CASES
        .iter()
        .copied()
        .filter(|c| {
            only.as_deref()
                .is_none_or(|o| o.split(',').any(|x| x == *c))
        })
        .collect();
    let result = async {
        healthy(&http, &url).await?;
        let base = rss(pid).context("the node's RSS")?;
        let mut peak = base;
        for case in &cases {
            let sql = adversarial(case);
            let (code, body, took) = match query(&http, &url, &sql).await {
                Ok(x) => x,
                Err(e) => bail!(
                    "{case}: no answer ({e:#}); killed by the watchdog: {}",
                    killed.load(Ordering::Relaxed)
                ),
            };
            eprintln!("{case}: {code} after {took:?}: {}", body["error"]);
            ensure!(
                took <= ANSWER_WITHIN,
                "{case}: answered after {took:?} (> {ANSWER_WITHIN:?})"
            );
            ensure!([200, 400, 422].contains(&code), "{case}: {code} {body}");
            if code != 200 {
                ensure!(
                    body["error"].as_str().is_some_and(|e| !e.is_empty()),
                    "{case}: an error without a message: {body}"
                );
            }
            ensure!(n.alive(), "{case}: the node died\n{}", n.dump());
            healthy(&http, &url)
                .await
                .with_context(|| format!("after {case}"))?;
            let now = rss(pid).context("the node's RSS")?;
            peak = peak.max(now);
        }
        ensure!(
            peak.saturating_sub(base) <= RSS_GROWTH,
            "the node's RSS grew {} MiB (from {} MiB)",
            (peak - base) >> 20,
            base >> 20
        );
        eprintln!("RSS: base {} MiB, peak {} MiB", base >> 20, peak >> 20);
        Ok(())
    }
    .await;
    stop.store(true, Ordering::Relaxed);
    let _ = watchdog.join();
    ensure!(
        !killed.load(Ordering::Relaxed),
        "the watchdog killed the node: RSS > {} MiB",
        WATCHDOG_RSS >> 20
    );
    result
}

/// The pids of `parent`'s `cairn-query-worker` children. Linux: `/proc`; other systems:
/// `pgrep`.
fn workers(parent: u32) -> Vec<u32> {
    #[cfg(target_os = "linux")]
    {
        let Ok(dir) = std::fs::read_dir("/proc") else {
            return Vec::new();
        };
        dir.flatten()
            .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
            .filter(|pid| {
                let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                    return false;
                };
                // `pid (comm) state ppid …` — comm may contain spaces.
                let Some((head, tail)) = stat.rsplit_once(") ") else {
                    return false;
                };
                let ppid = tail
                    .split_whitespace()
                    .nth(1)
                    .and_then(|p| p.parse::<u32>().ok());
                ppid == Some(parent) && head.contains("(cairn-query-wor")
            })
            .collect()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let out = std::process::Command::new("pgrep")
            .args(["-P", &parent.to_string(), "-f", "cairn-query-worker"])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        out.lines().filter_map(|l| l.trim().parse().ok()).collect()
    }
}

/// A long-running but memory-light query: filtering a 200⁴-row cross product (the filter
/// defeats statistics-based counting).
fn long_query() -> String {
    let v = (1..=200)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT count(*) FROM (VALUES {v}) a(x), (VALUES {v}) b(x), (VALUES {v}) c(x), (VALUES {v}) d(x) WHERE a.x + b.x + c.x + d.x > 3"
    )
}

async fn query_with(
    http: &reqwest::Client,
    url: &str,
    sql: &str,
    timeout_ms: u64,
) -> Result<(u16, Value, Duration)> {
    let t0 = Instant::now();
    let resp = http
        .post(format!("{url}/query"))
        .bearer_auth(ADMIN_KEY)
        .json(&json!({ "sql": sql, "timeout_ms": timeout_ms }))
        .send()
        .await?;
    let code = resp.status().as_u16();
    Ok((code, resp.json().await.unwrap_or(Value::Null), t0.elapsed()))
}

/// **The child lifecycle**, the real binary in its default mode:
///
/// 1. the node starts with a warm pool of 4 children (the `query:` line says child process);
/// 2. a long query; the test `kill -9`s every child of the node midway ⇒ the query returns
///    `422` naming the signal, the node lives, the next query is correct (a new child replaces it);
/// 3. a query past its deadline ⇒ `422` within the deadline + a margin (the parent kills the child);
/// 4. a query over the child's memory limit ⇒ `422` naming the limit;
/// 5. the warm pool returns to 4 children.
///
/// Prints the RSS of each waiting child.
#[tokio::test(flavor = "multi_thread")]
async fn worker_death_is_reported_and_replaced() -> Result<()> {
    let mut n = node()?;
    let pid = n.pid();
    let url = n.url.clone();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    ensure!(
        n.dump().contains("cairn-query-worker processes"),
        "the node does not run queries in a child process:\n{}",
        n.dump()
    );
    healthy(&http, &url).await?;
    let t0 = Instant::now();
    while workers(pid).len() < 4 && t0.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let idle = workers(pid);
    ensure!(idle.len() == 4, "warm pool: {idle:?}");
    let rss_each: Vec<u64> = idle
        .iter()
        .filter_map(|p| rss(*p))
        .map(|b| b >> 20)
        .collect();
    eprintln!(
        "waiting child RSS (MiB): {rss_each:?}; node {} MiB",
        rss(pid).unwrap_or(0) >> 20
    );

    // 2. Kill midway.
    let long = long_query();
    let running = {
        let (http, url, long) = (http.clone(), url.clone(), long.clone());
        tokio::spawn(async move { query_with(&http, &url, &long, 20_000).await })
    };
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for w in workers(pid) {
        kill9(w);
    }
    let (code, body, took) = running.await??;
    ensure!(code == 422, "a query whose child was killed: {code} {body}");
    ensure!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("signal 9")),
        "the message does not name the signal: {body}"
    );
    ensure!(took < Duration::from_secs(10), "answered after {took:?}");
    ensure!(n.alive(), "the node died");
    healthy(&http, &url).await.context("after child kill")?;

    // 3. Past the deadline.
    let (code, body, took) = query_with(&http, &url, &long, 1_000).await?;
    ensure!(code == 422, "a query past its deadline: {code} {body}");
    ensure!(
        took < Duration::from_secs(5),
        "a query past its deadline answered after {took:?}"
    );
    healthy(&http, &url).await.context("after a timeout")?;

    // 4. The memory limit.
    let (code, body, _) = query_with(&http, &url, &adversarial("s33_concat"), 10_000).await?;
    ensure!(code == 422, "a query over memory: {code} {body}");
    ensure!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("MiB of memory")),
        "the message does not name the memory limit: {body}"
    );
    ensure!(n.alive(), "the node died");
    healthy(&http, &url).await.context("after the OOM query")?;

    // 5. The warm pool recovers.
    let t0 = Instant::now();
    while workers(pid).len() != 4 && t0.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ensure!(workers(pid).len() == 4, "pool after: {:?}", workers(pid));
    let metrics = http
        .get(format!("{url}/metrics"))
        .bearer_auth(ADMIN_KEY)
        .send()
        .await?
        .text()
        .await?;
    let stopped = metrics
        .lines()
        .find_map(|l| l.strip_prefix("cairnflow_query_worker_stopped_total "))
        .and_then(|v| v.trim().parse::<u64>().ok());
    // Killed midway and the memory limit; a query past its deadline is usually stopped by the
    // child itself (the plan yields the thread) before the parent has to kill it.
    ensure!(stopped >= Some(2), "child-stopped metric: {stopped:?}");
    Ok(())
}

/// `--query-isolation inline` runs queries in the node; `process` with the binary missing ⇒
/// refuses to start.
#[test]
fn isolation_flag_is_honoured() -> Result<()> {
    let n = Binary::spawn(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
            "--query-isolation",
            "inline",
        ],
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )?;
    let t0 = Instant::now();
    while !n.dump().contains("  query:") && t0.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(20));
    }
    ensure!(
        n.dump()
            .contains("SQL runs inside the node process (--query-isolation inline)"),
        "{}",
        n.dump()
    );
    ensure!(workers(n.pid()).is_empty(), "inline yet there are children");
    drop(n);
    let (st, out) = Binary::run_to_exit(
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--query-isolation",
            "process",
            "--query-worker",
            "/nonexistent/cairn-query-worker",
        ],
        &[("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY)],
        Duration::from_secs(20),
    )?;
    ensure!(!st.success(), "started with the worker missing:\n{out}");
    ensure!(
        out.contains("cannot find the cairn-query-worker binary"),
        "{out}"
    );
    Ok(())
}

/// **The node as PID 1** (a Docker image with `ENTRYPOINT ["cairnflow"]` and no init,
/// compose, a hosted deployment). Linux only.
///
/// 1. `unshare --user --map-root-user --pid --fork cairnflow …` (without
///    `--mount-proc`: a container cannot mount `/proc`; `/proc/self` is still right) —
///    the node is PID 1 of a new PID namespace (no real root needed);
/// 2. the node spawns `/query` children as usual; a child sees `getppid() == 1`.
///
/// Must see: `SELECT 1 + 1` returns `200` `[[2]]` (before the fix: the child exited by itself,
/// believing its parent dead ⇒ `422`). Needs a user namespace: the container runs with
/// `--security-opt seccomp=unconfined` (Docker's default seccomp blocks it).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn node_as_pid_1_answers_queries() -> Result<()> {
    let n = Binary::spawn_wrapped(
        &[
            "unshare",
            "--user",
            "--map-root-user",
            "--pid",
            "--fork",
            "--kill-child",
        ],
        &[
            "--main-store",
            "memory://",
            "--port",
            "0",
            "--internal-port",
            "0",
            "--query-isolation",
            "process",
        ],
        &[
            ("CAIRNFLOW_ADMIN_KEY", ADMIN_KEY),
            ("CAIRNFLOW_CLUSTER_SECRET", SECRET),
        ],
    )
    .context("unshare (needs a user namespace)")?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    for _ in 0..6 {
        let (code, body, _) = query(&http, &n.url, "SELECT 1 + 1 AS x").await?;
        ensure!(code == 200, "node as PID 1: {code} {body}\n{}", n.dump());
        ensure!(body["rows"] == json!([[2]]), "{body}");
    }
    Ok(())
}

/// **The `/query` children are locked down like strangers** (Linux). A warm child waiting
/// for work is:
///
/// 1. in its own session (`setsid` — the node can kill the whole group);
/// 2. not dumpable — a process of the same user (a sibling child of another tenant, this
///    test) cannot open `/proc/<child>/fd/*`, `environ`, `mem`;
/// 3. limited by the kernel: address space (`RLIMIT_AS`, not just its own
///    allocator), no file writes (`RLIMIT_FSIZE 0`), few fds, no core;
/// 4. running under seccomp (`Seccomp: 2`, `NoNewPrivs: 1`) — each blocked call
///    has its own test in `cairn-query` (`worker::tests`).
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn workers_are_sandboxed() -> Result<()> {
    let n = node()?;
    let t0 = Instant::now();
    while workers(n.pid()).len() < 4 && t0.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // The children are past initialisation (seccomp is set before reading work).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ws = workers(n.pid());
    ensure!(!ws.is_empty(), "no children");
    for w in ws {
        let status = std::fs::read_to_string(format!("/proc/{w}/status"))?;
        let field = |name: &str| {
            status
                .lines()
                .find_map(|l| l.strip_prefix(name))
                .map(str::trim)
                .unwrap_or_default()
                .to_string()
        };
        ensure!(
            field("Seccomp:") == "2",
            "con {w}: Seccomp {}",
            field("Seccomp:")
        );
        ensure!(field("NoNewPrivs:") == "1", "con {w}: NoNewPrivs");
        // SAFETY: `getsid` has no preconditions.
        let sid = unsafe { libc::getsid(w as libc::pid_t) };
        ensure!(sid == w as libc::pid_t, "child {w}: session {sid}");
        ensure!(
            std::fs::read_link(format!("/proc/{w}/fd/0")).is_err(),
            "child {w}: /proc/<child>/fd is readable (dumpable)"
        );
        let limits = std::fs::read_to_string(format!("/proc/{w}/limits"))?;
        let limit = |name: &str| {
            limits
                .lines()
                .find(|l| l.starts_with(name))
                .map(|l| {
                    l[name.len()..]
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_string()
                })
                .unwrap_or_default()
        };
        ensure!(
            limit("Max address space") != "unlimited",
            "con {w}: RLIMIT_AS"
        );
        ensure!(
            limit("Max file size") == "0",
            "con {w}: RLIMIT_FSIZE {}",
            limit("Max file size")
        );
        ensure!(limit("Max core file size") == "0", "con {w}: RLIMIT_CORE");
        let files: u64 = limit("Max open files").parse().unwrap_or(u64::MAX);
        ensure!(files <= 256, "con {w}: RLIMIT_NOFILE {files}");
    }
    // The children still run queries.
    let http = reqwest::Client::new();
    healthy(&http, &n.url).await
}

/// **A large answer is refused by the child** (the answer byte limit), and the node does not
/// hold it on the child's behalf: 400 rows × 50 KB ≈ 20 MB.
#[tokio::test(flavor = "multi_thread")]
async fn large_answers_are_refused_by_the_worker() -> Result<()> {
    let n = node()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    healthy(&http, &n.url).await?;
    let base = rss(n.pid()).context("RSS")?;
    let rows = (1..=400)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT s FROM (VALUES ('{}')) t(s), (VALUES {rows}) u(x)",
        "x".repeat(50_000)
    );
    let (code, body, _) = query_with(&http, &n.url, &sql, 10_000).await?;
    ensure!(code == 422, "a 20 MB answer: {code} {}", body["error"]);
    ensure!(
        body["error"].as_str().is_some_and(|e| e.contains("MiB")),
        "the message does not name the limit: {body}"
    );
    let grew = rss(n.pid()).unwrap_or(0).saturating_sub(base);
    ensure!(grew < 32 << 20, "the node's RSS grew {} MiB", grew >> 20);
    healthy(&http, &n.url).await
}

/// **A configuration requiring a child process while the node has no query runtime to
/// run it** (`query_threads = 0`) ⇒ the node **does not start**, rather than quietly
/// running queries in place.
#[tokio::test(flavor = "multi_thread")]
async fn worker_config_without_query_runtime_refuses_to_start() -> Result<()> {
    let mut cfg = common::solo_config(1);
    cfg.query_threads = 0;
    cfg.query_worker = Some(common::query_worker());
    match cairn_server::node::start(cfg).await {
        Ok(n) => {
            n.shutdown().await;
            bail!("the node started and ran queries in place")
        }
        Err(e) => {
            ensure!(e.to_string().contains("query worker"), "{e}");
            Ok(())
        }
    }
}
