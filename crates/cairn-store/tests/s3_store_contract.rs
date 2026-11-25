// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **S3 store** contract (`docs/reliability/irreversible.md` §0.3, rows
//! S3-1 to S3-5): what cairn assumes about the object store itself, not about
//! the client or SlateDB. Every test is `#[ignore]` and runs only against a real
//! store:
//!
//! ```sh
//! CAIRNFLOW_STORE_CONTRACT_URI=s3://bucket/prefix \
//! CAIRNFLOW_STORE_CONTRACT_VERSIONED_URI=s3://bucket-versioned/prefix \
//! AWS_ENDPOINT=http://localhost:18333 AWS_ALLOW_HTTP=true \
//! AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-east-1 \
//!   cargo test -p cairn-store --test s3_store_contract -- --ignored --nocapture --test-threads 1
//! ```
//!
//! Without a URI each test prints "skipped" and passes. `…_VERSIONED_URI` points
//! at a bucket **with versioning enabled** (S3-3); without it S3-3 runs only on
//! an ordinary bucket. Each run writes under `<prefix>/contract-<pid>-<nanos>/`
//! and cleans up when done (a versioned bucket keeps the old versions — use a
//! separate bucket). Credentials and endpoint are read from the environment as
//! the binary does (`StoreLocation::parse`).

// Tests run on real time, network and processes.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "real store contract: real clock, network and child processes"
)]

use {
    anyhow::{bail, ensure, Context, Result},
    bytes::Bytes,
    cairn_store::StoreLocation,
    futures::TryStreamExt,
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
        UpdateVersion,
    },
    std::{
        io::{BufRead, BufReader},
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
};

const URI: &str = "CAIRNFLOW_STORE_CONTRACT_URI";
const VERSIONED_URI: &str = "CAIRNFLOW_STORE_CONTRACT_VERSIONED_URI";
/// The S3-2 child-process mode: `<put|multipart>:<path>:<bytes>`.
const CHILD: &str = "CAIRNFLOW_STORE_CONTRACT_CHILD";

/// The store and private directory of one run, from environment variable `var`.
struct Run {
    store: Arc<dyn ObjectStore>,
    root: String,
}

impl Run {
    fn open(var: &str, tag: &str) -> Result<Option<Run>> {
        let Ok(uri) = std::env::var(var) else {
            eprintln!("{tag}: skipped — {var} not set");
            return Ok(None);
        };
        let loc = StoreLocation::parse(&uri)?;
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();
        let base = loc.prefix().as_ref().trim_matches('/').to_string();
        let run = format!("contract-{}-{nanos}/{tag}", std::process::id());
        let root = if base.is_empty() {
            run
        } else {
            format!("{base}/{run}")
        };
        eprintln!("{tag}: {uri} → {root}/");
        Ok(Some(Run {
            store: Arc::clone(loc.store()),
            root,
        }))
    }

    fn path(&self, rel: &str) -> Path {
        Path::from(format!("{}/{rel}", self.root))
    }

    async fn list(&self, rel: &str) -> Result<Vec<String>> {
        Ok(self
            .store
            .list(Some(&self.path(rel)))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await?)
    }

    /// Delete every object of the run.
    async fn clean(&self) -> Result<()> {
        let all: Vec<_> = self
            .store
            .list(Some(&Path::from(self.root.clone())))
            .map_ok(|m| m.location)
            .try_collect()
            .await?;
        for p in all {
            match self.store.delete(&p).await {
                Ok(()) | Err(OsError::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

/// `Some(bytes)`, or `None` when the object is absent.
async fn get(store: &dyn ObjectStore, p: &Path) -> Result<Option<Bytes>> {
    match store.get(p).await {
        Ok(r) => Ok(Some(r.bytes().await?)),
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

// ─────────────────────────────────────────────── S3-1

/// **S3-1.** Strong read-after-write and LIST-after-write/delete consistency:
/// LIST of `owners/` (irreversible.md X10, R3), `ranges/` (X6), `nodewal/` (X9)
/// and D-d rely on it. 1 000 rounds, a new key each round: PUT → GET sees the
/// exact bytes → LIST sees the key → overwriting PUT → GET sees the new bytes →
/// DELETE → GET absent → LIST does not see it.
///
/// Must see: 0 mismatches at every step (prints mismatches per step and the
/// time).
#[tokio::test]
#[ignore = "needs a real S3 store: CAIRNFLOW_STORE_CONTRACT_URI"]
async fn s3_1_read_and_list_after_write_and_delete() -> Result<()> {
    let Some(run) = Run::open(URI, "s3-1")? else {
        return Ok(());
    };
    const ROUNDS: usize = 1000;
    let mut misses = [0usize; 6];
    let names = [
        "GET after PUT",
        "LIST after PUT",
        "GET after overwrite",
        "GET after DELETE",
        "LIST after DELETE",
        "LIST still has the old key",
    ];
    let started = Instant::now();
    for i in 0..ROUNDS {
        let rel = format!("k/{i:05}");
        let p = run.path(&rel);
        let v1 = Bytes::from(format!("v1-{i}-{:?}", Instant::now()));
        run.store.put(&p, v1.clone().into()).await?;
        if get(run.store.as_ref(), &p).await?.as_ref() != Some(&v1) {
            misses[0] += 1;
        }
        let listed = run.list("k").await?;
        if !listed.contains(&p.to_string()) {
            misses[1] += 1;
        }
        if listed.len() != 1 {
            misses[5] += 1;
        }
        let v2 = Bytes::from(format!("v2-{i}"));
        run.store.put(&p, v2.clone().into()).await?;
        if get(run.store.as_ref(), &p).await?.as_ref() != Some(&v2) {
            misses[2] += 1;
        }
        run.store.delete(&p).await?;
        if get(run.store.as_ref(), &p).await?.is_some() {
            misses[3] += 1;
        }
        if run.list("k").await?.contains(&p.to_string()) {
            misses[4] += 1;
        }
    }
    let took = started.elapsed();
    println!(
        "S3-1: {ROUNDS} rounds in {:.1} s ({:.1} ms/round)",
        took.as_secs_f64(),
        took.as_secs_f64() * 1000.0 / ROUNDS as f64
    );
    for (n, m) in names.iter().zip(misses) {
        println!("  {n}: {m} mismatches");
    }
    run.clean().await?;
    ensure!(
        misses.iter().all(|&m| m == 0),
        "the store is not strongly consistent: {misses:?}"
    );
    Ok(())
}

// ─────────────────────────────────────────────── S3-2

/// Deterministic bytes of object number `seed`: a read-back compares byte by byte.
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 8);
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(len);
    out
}

const PART: usize = 5 * 1024 * 1024;

async fn upload(store: &dyn ObjectStore, p: &Path, mode: &str, body: Vec<u8>) -> Result<()> {
    match mode {
        "put" => {
            store.put(p, Bytes::from(body).into()).await?;
        }
        "multipart" => {
            let mut up = store.put_multipart(p).await?;
            for chunk in body.chunks(PART) {
                up.put_part(Bytes::copy_from_slice(chunk).into()).await?;
            }
            up.complete().await?;
        }
        other => bail!("unknown mode: {other}"),
    }
    Ok(())
}

/// The S3-2 child process: does nothing without [`CHILD`]. Prints `READY`
/// right before the PUT, `DONE` once the store has answered (at the end of the
/// line: libtest prints the test name first, on the same line).
#[tokio::test]
#[ignore = "child process of s3_2_kill_mid_put_leaves_whole_or_nothing"]
async fn s3_2_child_put() -> Result<()> {
    use std::io::Write;
    let Ok(spec) = std::env::var(CHILD) else {
        return Ok(());
    };
    let mut it = spec.splitn(3, ':');
    let (mode, rel, len) = (
        it.next().context("mode")?,
        it.next().context("path")?,
        it.next().context("bytes")?.parse::<usize>()?,
    );
    let run = Run::open(URI, "s3-2-child")?.context("the child needs a URI")?;
    let p = Path::from(rel);
    let seed = seed_of(rel);
    let body = pattern(seed, len);
    println!("READY");
    std::io::stdout().flush()?;
    upload(run.store.as_ref(), &p, mode, body).await?;
    println!("DONE");
    std::io::stdout().flush()?;
    Ok(())
}

fn seed_of(rel: &str) -> u64 {
    rel.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
    })
}

/// The outcome of an object after its writer is `kill -9`ed.
#[derive(Debug, Default)]
struct Outcomes {
    absent: usize,
    whole: usize,
    torn: Vec<String>,
    /// The child printed `DONE` (the store had answered) yet the object is absent/torn.
    lost_after_done: Vec<String>,
}

/// **S3-2.** PUT is atomic: a writer dying (`kill -9`) mid-PUT leaves the
/// object **either whole or absent** — never torn (NW-2, seal, lease). MinIO
/// with erasure coding violates it (AGENTS.md §5). Each mode (`put`, one 32 MiB
/// request; `multipart`, 5 MiB parts) times one full write, then kills the
/// child process 12 times at points spread evenly from 0 to 1.2× that time.
///
/// Must see: 0 torn objects; an object the child reported `DONE` for is whole.
/// Prints absent/whole counts per mode (both must be > 0 for the probe to cover
/// the gap in between).
#[tokio::test]
#[ignore = "needs a real S3 store: CAIRNFLOW_STORE_CONTRACT_URI"]
async fn s3_2_kill_mid_put_leaves_whole_or_nothing() -> Result<()> {
    let Some(run) = Run::open(URI, "s3-2")? else {
        return Ok(());
    };
    const LEN: usize = 32 * 1024 * 1024;
    const KILLS: usize = 12;
    let exe = std::env::current_exe()?;
    let mut bad = Vec::new();
    for mode in ["put", "multipart"] {
        // The time of one full write, in this process.
        let probe = run.path(&format!("{mode}/probe"));
        let body = pattern(seed_of(probe.as_ref()), LEN);
        let t0 = Instant::now();
        upload(run.store.as_ref(), &probe, mode, body).await?;
        let full = t0.elapsed();
        let mut out = Outcomes::default();
        for k in 0..KILLS {
            let p = run.path(&format!("{mode}/{k:02}"));
            let mut child = std::process::Command::new(&exe)
                .args([
                    "--ignored",
                    "--exact",
                    "s3_2_child_put",
                    "--nocapture",
                    "--test-threads",
                    "1",
                ])
                .env(CHILD, format!("{mode}:{p}:{LEN}"))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            let stdout = child.stdout.take().context("the child's stdout")?;
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(|l| l.ok()) {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
            });
            // Wait for READY (the child has built its payload and is about to PUT).
            loop {
                match rx.recv_timeout(Duration::from_secs(60)) {
                    Ok(l) if l.trim_end().ends_with("READY") => break,
                    Ok(_) => {}
                    Err(e) => {
                        let _ = child.kill();
                        bail!("the child is not READY: {e}");
                    }
                }
            }
            let delay = full.mul_f64(1.2 * k as f64 / (KILLS - 1) as f64);
            std::thread::sleep(delay);
            let _ = child.kill(); // SIGKILL
            let _ = child.wait();
            let done = rx.try_iter().any(|l| l.trim_end().ends_with("DONE"));
            let want = pattern(seed_of(p.as_ref()), LEN);
            let got = get(run.store.as_ref(), &p).await?;
            let label = format!(
                "{mode}/{k:02} (killed after {} ms, DONE = {done})",
                delay.as_millis()
            );
            match got {
                None if done => out.lost_after_done.push(label),
                None => out.absent += 1,
                Some(b) if b[..] == want[..] => out.whole += 1,
                Some(b) => out.torn.push(format!("{label}: {} / {LEN} byte", b.len())),
            }
        }
        println!(
            "S3-2 {mode}: one full write {} ms; {KILLS} kills → absent {}, whole {}, torn {:?}, lost after DONE {:?}",
            full.as_millis(),
            out.absent,
            out.whole,
            out.torn,
            out.lost_after_done
        );
        if out.absent == 0 || out.whole == 0 {
            println!("  (warning: gap not covered on both sides — raise LEN or move the points)");
        }
        bad.extend(out.torn);
        bad.extend(out.lost_after_done);
    }
    // A half-done multipart upload (if the store keeps one) is not an object: LIST does not see it.
    run.clean().await?;
    ensure!(bad.is_empty(), "PUT is not atomic: {bad:#?}");
    Ok(())
}

// ─────────────────────────────────────────────── S3-3

fn etag_of(r: &slatedb::object_store::PutResult) -> UpdateVersion {
    UpdateVersion {
        e_tag: r.e_tag.clone(),
        version: r.version.clone(),
    }
}

fn kind(r: &std::result::Result<slatedb::object_store::PutResult, OsError>) -> &'static str {
    match r {
        Ok(_) => "Ok",
        Err(OsError::AlreadyExists { .. }) => "AlreadyExists",
        Err(OsError::Precondition { .. }) => "Precondition",
        Err(OsError::NotFound { .. }) => "NotFound",
        Err(OsError::NotImplemented { .. }) => "NotImplemented",
        Err(_) => "other Err",
    }
}

/// One S3-3 pass over `run`: returns the lines `step: got (want)` and the number of wrong steps.
async fn conditional_puts(run: &Run) -> Result<(Vec<String>, usize)> {
    let s = run.store.as_ref();
    let p = run.path("cas");
    let mut lines = Vec::new();
    let mut wrong = 0;
    let mut check = |step: &str, got: &'static str, want: &[&str]| {
        let ok = want.contains(&got);
        if !ok {
            wrong += 1;
        }
        lines.push(format!(
            "{} {step}: {got} (want {})",
            if ok { "✓" } else { "✗" },
            want.join(" | ")
        ));
    };
    let create = PutOptions::from(PutMode::Create);
    let r1 = s
        .put_opts(&p, Bytes::from_static(b"1").into(), create.clone())
        .await;
    check("Create on an absent key", kind(&r1), &["Ok"]);
    let v1 = etag_of(&r1?);
    let r = s
        .put_opts(&p, Bytes::from_static(b"x").into(), create.clone())
        .await;
    check("Create on an existing key", kind(&r), &["AlreadyExists"]);
    let r2 = s
        .put_opts(
            &p,
            Bytes::from_static(b"2").into(),
            PutMode::Update(v1.clone()).into(),
        )
        .await;
    check("Update(current etag)", kind(&r2), &["Ok"]);
    let v2 = etag_of(&r2?);
    let r = s
        .put_opts(
            &p,
            Bytes::from_static(b"x").into(),
            PutMode::Update(v1).into(),
        )
        .await;
    check("Update(old etag)", kind(&r), &["Precondition"]);
    let now = get(s, &p).await?;
    check(
        "content after two misses",
        if now.as_deref() == Some(&b"2"[..]) {
            "Ok"
        } else {
            "other Err"
        },
        &["Ok"],
    );
    s.delete(&p).await?;
    let gone = match s.get(&p).await {
        Ok(_) => "Ok",
        Err(OsError::NotFound { .. }) => "NotFound",
        Err(_) => "other Err",
    };
    check("GET after DELETE (delete marker)", gone, &["NotFound"]);
    let head = match s.head(&p).await {
        Ok(_) => "Ok",
        Err(OsError::NotFound { .. }) => "NotFound",
        Err(_) => "other Err",
    };
    check("HEAD after DELETE", head, &["NotFound"]);
    let r = s
        .put_opts(
            &p,
            Bytes::from_static(b"x").into(),
            PutMode::Update(v2).into(),
        )
        .await;
    // AWS: If-Match on an absent key ⇒ 404 (object_store: NotFound) or 412.
    check(
        "Update(etag from before DELETE)",
        kind(&r),
        &["Precondition", "NotFound"],
    );
    let r3 = s
        .put_opts(&p, Bytes::from_static(b"3").into(), create.clone())
        .await;
    check("Create after DELETE", kind(&r3), &["Ok"]);
    let r = s
        .put_opts(&p, Bytes::from_static(b"x").into(), create)
        .await;
    check("Create again", kind(&r), &["AlreadyExists"]);
    let last = get(s, &p).await?;
    check(
        "final content",
        if last.as_deref() == Some(&b"3"[..]) {
            "Ok"
        } else {
            "other Err"
        },
        &["Ok"],
    );
    Ok((lines, wrong))
}

/// **S3-3.** Conditional PUTs (`If-None-Match: *` for `Create`, `If-Match` for
/// `Update`) hold when the bucket has **versioning enabled** (restoring objects
/// deleted by mistake relies on versioning): a delete marker ⇒ GET/HEAD
/// `NotFound`, and a later `Create` **wins** (is not blocked by an old
/// version); an old etag ⇒ `Precondition`. Runs on an ordinary bucket as the
/// baseline, and on a versioned bucket when [`VERSIONED_URI`] is set.
///
/// Must see: every step ✓ on both buckets.
#[tokio::test]
#[ignore = "needs a real S3 store: CAIRNFLOW_STORE_CONTRACT_URI (+ _VERSIONED_URI)"]
async fn s3_3_conditional_put_holds_with_versioning() -> Result<()> {
    let mut wrong = 0;
    let mut ran = 0;
    for (var, label) in [
        (URI, "ordinary bucket"),
        (VERSIONED_URI, "versioned bucket"),
    ] {
        let Some(run) = Run::open(var, "s3-3")? else {
            continue;
        };
        ran += 1;
        let (lines, w) = conditional_puts(&run).await?;
        println!("S3-3 {label}:");
        for l in lines {
            println!("  {l}");
        }
        wrong += w;
        run.clean().await?;
    }
    if ran > 0 && std::env::var(VERSIONED_URI).is_err() {
        println!("S3-3: {VERSIONED_URI} not set — the versioning part is not checked");
    }
    ensure!(wrong == 0, "{wrong} conditional-PUT steps wrong");
    Ok(())
}

// ─────────────────────────────────────────────── S3-4

/// **S3-4.** An object's `last_modified` is close to the node clock: D-d
/// computes a tombstone's age from it when the tombstone lacks `retired_ms`
/// (`gc.rs`), and a clock-skew gate would compare exactly these two values. Ten
/// samples: the node clock before and after the PUT, then a HEAD.
///
/// Must see: |skew| (against the PUT's midpoint, S3's one-second resolution)
/// below the default `min_age/4` (≈ 75 s for a `min_age` of 300 s); prints each
/// sample.
#[tokio::test]
#[ignore = "needs a real S3 store: CAIRNFLOW_STORE_CONTRACT_URI"]
async fn s3_4_last_modified_tracks_the_node_clock() -> Result<()> {
    let Some(run) = Run::open(URI, "s3-4")? else {
        return Ok(());
    };
    let mut worst = 0f64;
    for i in 0..10 {
        let p = run.path(&format!("clock/{i}"));
        let before = SystemTime::now();
        run.store.put(&p, Bytes::from_static(b"t").into()).await?;
        let after = SystemTime::now();
        let meta = run.store.head(&p).await?;
        let mid = before + after.duration_since(before)? / 2;
        let lm: SystemTime = meta.last_modified.into();
        let skew = match lm.duration_since(mid) {
            Ok(d) => d.as_secs_f64(),
            Err(e) => -e.duration().as_secs_f64(),
        };
        println!(
            "S3-4 sample {i}: last_modified − node clock = {skew:+.3} s (PUT {} ms)",
            after.duration_since(before)?.as_millis()
        );
        worst = worst.max(skew.abs());
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("S3-4: largest skew {worst:.3} s");
    run.clean().await?;
    ensure!(
        worst < 75.0,
        "last_modified is {worst:.1} s off the node clock"
    );
    Ok(())
}

// ─────────────────────────────────────────────── S3-5

/// **S3-5 (G18 on a real store).** A conditional GET never returns a stale
/// `304`. `store_probe::conditional_get` — the probe that `cairn-ctl cluster
/// enable-quiet-lease` runs before enabling the gate — for 200 rounds: each
/// round wins a CAS on an object, a GET If-None-Match with the old etag on the
/// writing client **and on a second client** (its own `StoreLocation`, its own
/// connection pool) must return `200` with the new version, the new etag must
/// give `304`, and a plain GET on the second client sees the new version. Solo
/// mode (LS-16) and the read fence (LS-5) rely on exactly this.
///
/// Must see: the probe passes (prints the time per round).
#[tokio::test]
#[ignore = "needs a real S3 store: CAIRNFLOW_STORE_CONTRACT_URI"]
async fn s3_5_conditional_get_never_serves_a_stale_304() -> Result<()> {
    let Some(run) = Run::open(URI, "s3-5")? else {
        return Ok(());
    };
    const ROUNDS: u32 = 200;
    let uri = std::env::var(URI)?;
    let other = Arc::clone(StoreLocation::parse(&uri)?.store());
    let nonce = cairn_store::store_probe::nonce();
    let started = Instant::now();
    let got = cairn_store::store_probe::conditional_get(
        &run.store,
        &other,
        &Path::from(run.root.clone()),
        &nonce,
        ROUNDS,
    )
    .await;
    let took = started.elapsed();
    println!(
        "S3-5: {ROUNDS} rounds in {:.1} s ({:.1} ms/round) → {got:?}",
        took.as_secs_f64(),
        took.as_secs_f64() * 1000.0 / f64::from(ROUNDS)
    );
    run.clean().await?;
    match got? {
        Ok(_) => Ok(()),
        Err(why) => bail!("the store does not hold the conditional-GET contract: {why}"),
    }
}
