// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The parent-node side of the `/query` child processes**
//! (`cairn_query::worker`).
//!
//! A SQL statement written by the caller runs in a `cairn-query-worker`
//! process; the node only does linear work:
//!
//! - keeps a pool of warm children ([`Pool`], size `MAX_CONCURRENT`), each child
//!   runs **one** statement and is then killed; a child that dies of anything
//!   is replaced;
//! - serves the child's requests ([`relay`]): scans local ranges or asks remote
//!   nodes, view tables through the router — **enforcing itself** the
//!   statement's tenant, ranges and read budget; caps on frame size, request
//!   count, filter size; a view-table path must be a declared path;
//! - kills the child when the statement's deadline passes (cutting
//!   DataFusion's synchronous CPU work, which `timeout_at` does not cut), and
//!   reads the exit status when the child dies (memory cap, stack overflow).
//!
//! The child's messages are a **trust boundary**: the child is a stranger — its
//! statement may have taken it over. It has no cluster secret, environment
//! variables or range handles to use.

use {
    cairn_query::{
        ctx::{QueryCtx, TenantScope, Warning},
        remote::{RemoteScan, Scan, ScanRequest, ScanResponse, WIRE_VERSION},
        view::ViewSource,
        worker::{
            DoneRef, FromWorker, Job, Target, ToWorker, WireError, EXIT_MEMORY, MAX_ANSWER_FRAME,
            MAX_IN_FLIGHT, MAX_KEYED_ARGS, MAX_PRED_ENTRIES, MAX_REQUESTS, MAX_REQUEST_FRAME,
            TAG_DONE, TAG_REQUEST,
        },
        QueryError,
    },
    cairn_store::range_read::RangeReadHandle,
    futures::{stream::FuturesUnordered, StreamExt},
    std::{
        path::PathBuf,
        process::Stdio,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        process::{Child, Command},
    },
};

/// Configuration of the `/query` child processes.
#[derive(Clone, Debug)]
pub struct QueryWorkerConfig {
    /// Binary `cairn-query-worker`.
    pub path: PathBuf,
    /// Allocation byte cap of each child (counting allocator, exceeded ⇒ the child exits).
    pub memory_bytes: u64,
}

/// Default memory cap per child (`--query-worker-memory-mb`): the statement's
/// 128 MiB DataFusion pool, plus the plan, the scan rows received from the
/// parent and the JSON answer. Sized by measurement.
pub const DEFAULT_WORKER_MEMORY_MB: u64 = 512;

/// Extra grace after the statement's deadline before the child is killed: the
/// child returns `query_budget` itself when the plan yields the thread; the
/// parent only kills it when it does not yield.
const KILL_GRACE: Duration = Duration::from_secs(1);

/// Cap on waiting for a child to exit after a kill / after its pipe closes.
const REAP_WAIT: Duration = Duration::from_secs(2);

/// The part of a child's stderr kept (first line goes to the `debug` log when it dies).
const STDERR_KEEP: usize = 512;

/// Number of children stopped before answering (deadline, memory cap, death, wire violation) —
/// metric `cairnflow_query_worker_stopped_total`.
pub static STOPPED: AtomicU64 = AtomicU64::new(0);

/// The pool of warm children.
pub struct Pool {
    cfg: QueryWorkerConfig,
    warm: usize,
    idle: Mutex<Vec<Child>>,
    /// The query runtime: children are spawned on its reactor (their pipes are
    /// used there), and the refill loop runs there.
    handle: tokio::runtime::Handle,
}

impl Pool {
    pub fn new(cfg: QueryWorkerConfig, warm: usize, handle: tokio::runtime::Handle) -> Arc<Self> {
        let pool = Arc::new(Self {
            cfg,
            warm,
            idle: Mutex::default(),
            handle,
        });
        pool.refill();
        pool
    }

    pub fn config(&self) -> &QueryWorkerConfig {
        &self.cfg
    }

    /// Spawns a child. Called on a fixed thread of the query runtime (the
    /// refill loop, or `take` inside a statement) — `PR_SET_PDEATHSIG` fires
    /// when the **thread** that spawned the child exits, so children are never
    /// spawned on `spawn_blocking` (a thread that expires when idle would kill
    /// a warm child).
    fn spawn(&self) -> std::io::Result<Child> {
        let _rt = self.handle.enter();
        // SAFETY: `getpid` has no preconditions.
        let parent = unsafe { libc::getpid() };
        let memory = self.cfg.memory_bytes;
        let mut cmd = Command::new(&self.cfg.path);
        cmd.arg("--memory-bytes")
            .arg(self.cfg.memory_bytes.to_string())
            // No environment variable of the node (cluster secret, S3 keys,
            // admin keys) reaches the child. The node's fds are opened with
            // `CLOEXEC`, so none leak.
            .env_clear()
            // Fewer malloc reserve arenas ⇒ `RLIMIT_AS` sits closer to the memory cap.
            .env("MALLOC_ARENA_MAX", "2")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // SAFETY: `before_exec` only makes syscalls and does not allocate (safe
        // between `fork` and `exec`).
        unsafe {
            cmd.pre_exec(move || cairn_query::worker::before_exec(parent, memory));
        }
        cmd.spawn()
    }

    /// A child without a job: a live warm child, or a freshly spawned one.
    fn take(&self) -> std::io::Result<Child> {
        loop {
            let next = self.idle.lock().unwrap_or_else(|p| p.into_inner()).pop();
            match next {
                Some(mut c) => {
                    if matches!(c.try_wait(), Ok(None)) {
                        return Ok(c);
                    }
                }
                None => return self.spawn(),
            }
        }
    }

    /// Refills the warm pool up to `warm` children, in the background.
    pub fn refill(self: &Arc<Self>) {
        let me = Arc::clone(self);
        self.handle.spawn(async move {
            loop {
                {
                    let mut idle = me.idle.lock().unwrap_or_else(|p| p.into_inner());
                    // A warm child that died (OOM killer, `kill`) does not count toward the pool.
                    idle.retain_mut(|c| matches!(c.try_wait(), Ok(None)));
                    if idle.len() >= me.warm {
                        return;
                    }
                }
                match me.spawn() {
                    Ok(c) => me.idle.lock().unwrap_or_else(|p| p.into_inner()).push(c),
                    Err(e) => {
                        tracing::warn!(error = %e, path = %me.cfg.path.display(), "cannot start a query worker");
                        return;
                    }
                }
            }
        });
    }
}

/// A group of ranges the child asks about through the parent.
pub(crate) enum Serve {
    /// Ranges open on this node.
    Local(Vec<RangeReadHandle>),
    /// Ranges of a remote node.
    Far(Arc<dyn RemoteScan>),
}

/// The job of one statement running in a child.
pub(crate) struct Relay {
    pub tenant: TenantScope,
    /// Local range counts for the child's path chooser.
    pub class_counts: Option<Vec<cairn_query::invocation::ClassCount>>,
    pub targets: Vec<(Target, Serve)>,
    pub views: Option<Arc<dyn ViewSource>>,
    pub sql: String,
    pub params: Vec<cairn_query::Param>,
    pub limits: cairn_query::Limits,
}

/// Outcome: the answer or an error, plus the reads **counted by the parent**
/// (charged to the tenant's bucket) and the warnings the parent saw.
pub(crate) struct Outcome {
    pub result: Result<Answered, QueryError>,
    pub read: u64,
    pub warnings: Vec<Warning>,
}

fn stopped(why: impl Into<String>) -> QueryError {
    STOPPED.fetch_add(1, Ordering::Relaxed);
    QueryError::Budget(why.into())
}

fn violation(why: &str) -> QueryError {
    tracing::warn!(why, "query worker broke the protocol; killed it");
    stopped(format!("the query worker sent an invalid request ({why})"))
}

async fn write_frame(
    w: &mut (impl tokio::io::AsyncWrite + Unpin),
    body: &[u8],
) -> std::io::Result<()> {
    let len = u32::try_from(body.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame too large"))?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

/// A child's answer: `columns`/`rows` are slices of the frame body (not
/// copied), written as is into the HTTP body.
pub(crate) struct Answered {
    pub columns: bytes::Bytes,
    pub rows: bytes::Bytes,
    pub row_count: u64,
    pub warnings: Vec<Warning>,
}

/// A parsed frame from the child.
enum Msg {
    /// `Scan`/`View`.
    Req(FromWorker),
    Done(Result<Answered, WireError>),
}

/// One frame from the child; `None` = the pipe closed.
type Frame = Option<Result<Msg, &'static str>>;

/// Reads one frame: kind (1 byte), length (`u32`), body. The per-kind cap is
/// checked **before** allocating a buffer or waiting for the body.
async fn read_frame(r: &mut (impl tokio::io::AsyncRead + Unpin)) -> Frame {
    let mut head = [0u8; 5];
    r.read_exact(&mut head).await.ok()?;
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    match head[0] {
        TAG_REQUEST if len > MAX_REQUEST_FRAME => return Some(Err("request frame too large")),
        TAG_DONE if len > MAX_ANSWER_FRAME => return Some(Err("answer frame too large")),
        TAG_REQUEST | TAG_DONE => {}
        _ => return Some(Err("unknown frame tag")),
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await.ok()?;
    if head[0] == TAG_REQUEST {
        return Some(match serde_json::from_slice::<FromWorker>(&body) {
            Ok(m @ (FromWorker::Scan { .. } | FromWorker::View { .. })) => Ok(Msg::Req(m)),
            Ok(FromWorker::Done(_)) => Err("frame tag mismatch"),
            Err(_) => Err("unreadable frame"),
        });
    }
    let body = bytes::Bytes::from(body);
    let parsed = match serde_json::from_slice::<DoneRef<'_>>(&body) {
        Ok(DoneRef::Done(r)) => r,
        Err(_) => return Some(Err("unreadable frame")),
    };
    // Slice of the frame at the raw value's position in the body.
    let slice = |raw: &serde_json::value::RawValue| {
        let start = raw.get().as_ptr() as usize - body.as_ptr() as usize;
        body.slice(start..start + raw.get().len())
    };
    Some(Ok(Msg::Done(parsed.map(|a| Answered {
        columns: slice(a.columns),
        rows: slice(a.rows),
        row_count: a.row_count,
        warnings: a.warnings,
    }))))
}

/// The message for the caller when the child exits before answering.
fn exit_reason(
    status: Option<std::process::ExitStatus>,
    memory_bytes: u64,
    tail: &str,
) -> QueryError {
    // The child's stderr is never logged at `warn`: a DataFusion panic may
    // carry fragments of the tenant's SQL or values. `warn` carries only the
    // exit status; the first stderr line (cut at 200 bytes) goes to `debug`.
    tracing::warn!(?status, "query worker stopped before answering");
    let first: String = tail
        .lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(200)
        .collect();
    tracing::debug!(stderr = %first, "query worker stderr");
    let Some(status) = status else {
        return stopped("the query worker stopped before answering");
    };
    if status.code() == Some(EXIT_MEMORY) {
        return stopped(format!(
            "the query needed more than {} MiB of memory — narrow it (fewer rows, columns or nested WITH queries)",
            memory_bytes >> 20
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(sig) = status.signal() {
            return stopped(format!(
                "the query stopped its worker process (signal {sig}) — narrow it"
            ));
        }
    }
    stopped(format!(
        "the query worker exited with {status} before answering"
    ))
}

/// A child request that has been served but not yet answered.
enum Pending {
    Scan {
        node: String,
        ranges: Vec<u32>,
        /// Read budget reserved for this scan.
        grant: u64,
        r: Result<ScanResponse, QueryError>,
    },
    View(Result<serde_json::Value, QueryError>),
}

/// The reply to the child from a finished request: read units go into the
/// **parent's** counter, warnings into the parent's ledger; error messages from
/// the store / a remote node do not reach the child (log only).
fn settle(
    id: u64,
    done: Pending,
    spent: &AtomicU64,
    budget: u64,
    over_budget: &mut bool,
    warn: &QueryCtx,
) -> ToWorker {
    let over = |over_budget: &mut bool| {
        *over_budget = true;
        WireError::Budget(format!("the query read more than {budget} units"))
    };
    match done {
        Pending::Scan { r: Ok(resp), .. } => {
            let now = spent
                .fetch_add(resp.read, Ordering::Relaxed)
                .saturating_add(resp.read);
            for w in &resp.warnings {
                warn.warn(w.range, w.reason.clone());
            }
            if now > budget {
                ToWorker::Scan {
                    id,
                    reply: Err(over(over_budget)),
                }
            } else {
                ToWorker::Scan {
                    id,
                    reply: Ok(resp),
                }
            }
        }
        Pending::Scan {
            r: Err(QueryError::Budget(_)),
            ..
        } => ToWorker::Scan {
            id,
            reply: Err(over(over_budget)),
        },
        Pending::Scan {
            node,
            ranges,
            r: Err(e),
            ..
        } => {
            for r in &ranges {
                warn.warn_partial(Some(*r), &format_args!("node {node}: {e}"));
            }
            ToWorker::Scan {
                id,
                reply: Err(WireError::Internal(format!(
                    "ranges of node {node} could not be read"
                ))),
            }
        }
        Pending::View(r) => ToWorker::View {
            id,
            reply: r.map_err(|e| match e {
                // The caller's errors at REST (role, parameters) pass through verbatim.
                QueryError::Sql(m) => WireError::Sql(m),
                QueryError::Budget(m) => WireError::Budget(m),
                QueryError::Internal(m) => {
                    tracing::warn!(detail = %m, "view table read failed");
                    WireError::Internal("the view could not be read".into())
                }
            }),
        },
    }
}

/// Cap on total scan-answer bytes the node sends to one child: the size of the
/// statement's pool in in-place mode — the node does not hold more than that on
/// the child's behalf.
const MAX_RELAYED_BYTES: usize = 128 << 20;

/// Cap on the message size of one child warning.
const MAX_WARNING_BYTES: usize = 256;
/// Cap on the size of a child's error message to the caller.
const MAX_ERROR_BYTES: usize = 2 << 10;

/// A child's answer within the contract: two JSON arrays; row count ≤ the row
/// cap; warnings ≤ 4 × range count + 8, each belonging to one of the
/// statement's ranges with a message ≤ [`MAX_WARNING_BYTES`] — so that all
/// later processing (deduplication, JSON) is bounded.
fn check_answer(a: &Answered, job: &Relay) -> Result<(), &'static str> {
    let arrays = [&a.columns, &a.rows]
        .iter()
        .all(|s| s.trim_ascii_start().starts_with(b"["));
    if !arrays {
        return Err("answer is not two arrays");
    }
    if a.row_count > job.limits.max_rows as u64 {
        return Err("answer has more rows than the limit");
    }
    let ranges: std::collections::BTreeSet<u32> = job
        .targets
        .iter()
        .flat_map(|(t, _)| t.ranges.iter().copied())
        .collect();
    if a.warnings.len() > 4 * ranges.len() + 8 {
        return Err("too many answer warnings");
    }
    let fits = |w: &Warning| {
        w.reason.len() <= MAX_WARNING_BYTES && w.range.is_none_or(|r| ranges.contains(&r))
    };
    if !a.warnings.iter().all(fits) {
        return Err("answer warnings out of bounds");
    }
    Ok(())
}

/// A child's error message cut at [`MAX_ERROR_BYTES`] (on a character boundary).
fn clip_error(e: QueryError) -> QueryError {
    let clip = |m: String| {
        if m.len() <= MAX_ERROR_BYTES {
            return m;
        }
        let mut end = MAX_ERROR_BYTES;
        while !m.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &m[..end])
    };
    match e {
        QueryError::Sql(m) => QueryError::Sql(clip(m)),
        QueryError::Budget(m) => QueryError::Budget(clip(m)),
        QueryError::Internal(m) => QueryError::Internal(clip(m)),
    }
}

/// The filters and parameters of a scan request are within caps (the child is a stranger).
fn check_scan(scan: &Scan) -> Result<(), &'static str> {
    match scan {
        Scan::Invocation { pred, .. } if pred.entries() > MAX_PRED_ENTRIES => {
            Err("filter too large")
        }
        Scan::Keyed { args, .. } if args.len() > MAX_KEYED_ARGS => Err("too many keyed arguments"),
        _ => Ok(()),
    }
}

/// Runs `job` in a child of `pool`. Every exit path kills the child.
pub(crate) async fn relay(pool: &Arc<Pool>, job: Relay) -> Outcome {
    let spent = AtomicU64::new(0);
    let warn = QueryCtx::new(job.tenant.clone(), Vec::new());
    let result = drive(pool, &job, &spent, &warn).await;
    pool.refill();
    Outcome {
        result,
        read: spent.load(Ordering::Relaxed),
        warnings: warn.warnings(),
    }
}

async fn drive(
    pool: &Arc<Pool>,
    job: &Relay,
    spent: &AtomicU64,
    warn: &QueryCtx,
) -> Result<Answered, QueryError> {
    let mut child = pool
        .take()
        .map_err(|e| QueryError::Internal(format!("cannot start a query worker: {e}")))?;
    let (Some(stdin), Some(stdout), Some(mut stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        return Err(QueryError::Internal("query worker without pipes".into()));
    };
    // stderr: keep the head for the log, keep reading so the pipe never fills.
    let tail = Arc::new(Mutex::new(String::new()));
    let drain = {
        let tail = tail.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            while let Ok(n) = stderr.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let mut t = tail.lock().unwrap_or_else(|p| p.into_inner());
                if t.len() < STDERR_KEEP {
                    t.push_str(&String::from_utf8_lossy(&buf[..n]));
                }
            }
        })
    };
    let finished = match converse(stdin, stdout, job, spent, warn).await {
        Ok(a) => Ok(a),
        Err(Ended::Failed(e)) => Err(e),
        Err(Ended::Closed) => {
            // The pipe closed before `Done`: the child has died.
            let status = tokio::time::timeout(REAP_WAIT, child.wait())
                .await
                .ok()
                .and_then(Result::ok);
            let _ = tokio::time::timeout(REAP_WAIT, drain).await;
            let t = tail.lock().unwrap_or_else(|p| p.into_inner()).clone();
            Err(exit_reason(status, pool.cfg.memory_bytes, &t))
        }
    };
    // One child, one statement: kill it, even when it has answered — the whole
    // group (the child runs in its own session). `kill_on_drop` + tokio's
    // reaper clean up the dead process.
    if let Some(pid) = child.id().and_then(|p| libc::pid_t::try_from(p).ok()) {
        // SAFETY: `kill` on this child's own group.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    drop(child);
    finished
}

/// How a statement ends when there is no `Done`.
enum Ended {
    /// The child's pipe closed: the child has died — the caller reads the exit status.
    Closed,
    Failed(QueryError),
}

/// The conversation with a child over its `stdin`/`stdout`: send the `Job`,
/// serve requests, wait for `Done` within the deadline. Separate from the
/// process so a malicious child can be tested with in-memory pipes.
async fn converse(
    mut stdin: impl tokio::io::AsyncWrite + Unpin + Send + 'static,
    mut stdout: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    job: &Relay,
    spent: &AtomicU64,
    warn: &QueryCtx,
) -> Result<Answered, Ended> {
    let tenant = match &job.tenant {
        TenantScope::All => None,
        TenantScope::One(t) => Some(t.clone()),
    };
    let first = ToWorker::Job(Job {
        sql: job.sql.clone(),
        params: job.params.clone(),
        tenant: tenant.clone(),
        targets: job.targets.iter().map(|(t, _)| t.clone()).collect(),
        class_counts: job.class_counts.clone(),
        views: job.views.is_some(),
        memory_bytes: job.limits.memory_bytes as u64,
        deadline_ms: job.limits.deadline.as_millis() as u64,
        max_rows: job.limits.max_rows as u64,
        read_budget: job.limits.read_budget,
    });
    // The write pipe gets its own task: if the child does not read, the parent does not stall.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = out_rx.recv().await {
            if write_frame(&mut stdin, &frame).await.is_err() {
                return;
            }
        }
    });
    let send = |msg: &ToWorker| -> usize {
        match serde_json::to_vec(msg) {
            Ok(b) => {
                let n = b.len();
                let _ = out_tx.send(b);
                n
            }
            Err(e) => {
                tracing::warn!(error = %e, "cannot encode a message for the query worker");
                0
            }
        }
    };
    send(&first);
    // The read pipe gets its own task (reading a frame cannot be cancelled
    // midway); the channel is bounded ⇒ a child sending faster than the parent
    // serves has to wait.
    let (in_tx, mut in_rx) = tokio::sync::mpsc::channel::<Frame>(1);
    let reader = tokio::spawn(async move {
        loop {
            let f = read_frame(&mut stdout).await;
            let end = !matches!(f, Some(Ok(_)));
            if in_tx.send(f).await.is_err() || end {
                return;
            }
        }
    });
    let deadline = tokio::time::sleep(job.limits.deadline + KILL_GRACE);
    tokio::pin!(deadline);
    let budget = job.limits.read_budget;
    let mut over_budget = false;
    let mut asked = 0usize;
    // Read budget reserved for scans in flight; a scan without a reservation
    // waits in the queue (in arrival order) until a finished one returns its
    // remainder.
    let mut reserved = 0u64;
    let mut deferred: std::collections::VecDeque<(u64, usize, Scan)> =
        std::collections::VecDeque::new();
    // Scan-answer bytes sent to the child: cap on the rows the node holds for the statement.
    let mut relayed = 0usize;
    type Fut<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = (u64, Pending)> + Send + 'a>>;
    let mut inflight: FuturesUnordered<Fut<'_>> = FuturesUnordered::new();
    // Grant reservations to waiting scans: each one gets **all** of the budget
    // nobody holds yet (the meaning of the statement's read cap does not
    // change); when nothing is left but scans are still in flight, wait for
    // them to return their remainder; when nothing is left at all ⇒ a budget
    // error for the child.
    macro_rules! pump {
        () => {
            while let Some(&(id, _, _)) = deferred.front() {
                let left = budget
                    .saturating_sub(spent.load(Ordering::Relaxed))
                    .saturating_sub(reserved);
                if left == 0 && !over_budget && reserved > 0 {
                    break;
                }
                let Some((_, target, scan)) = deferred.pop_front() else {
                    break;
                };
                if left == 0 || over_budget {
                    over_budget = true;
                    send(&ToWorker::Scan {
                        id,
                        reply: Err(WireError::Budget(format!(
                            "the query read more than {budget} units"
                        ))),
                    });
                    continue;
                }
                let Some((t, serve)) = job.targets.get(target) else {
                    continue;
                };
                reserved += left;
                // The parent rebuilds the request: tenant, ranges, budget are the parent's.
                let req = ScanRequest {
                    v: WIRE_VERSION,
                    tenant: tenant.clone(),
                    ranges: t.ranges.clone(),
                    read_budget: left,
                    scan,
                };
                let (node, ranges) = (t.node.clone(), t.ranges.clone());
                inflight.push(Box::pin(async move {
                    let r = match serve {
                        Serve::Local(h) => cairn_query::remote::serve(h.clone(), req).await,
                        Serve::Far(r) => r.scan(req).await,
                    };
                    (
                        id,
                        Pending::Scan {
                            node,
                            ranges,
                            grant: left,
                            r,
                        },
                    )
                }));
            }
        };
    }
    let finished: Result<Answered, Ended> = loop {
        tokio::select! {
            () = &mut deadline => {
                break Err(Ended::Failed(stopped(format!(
                    "query ran over {:?} (the worker was stopped)",
                    job.limits.deadline
                ))));
            }
            Some((id, done)) = inflight.next(), if !inflight.is_empty() => {
                if let Pending::Scan { grant, .. } = &done {
                    reserved = reserved.saturating_sub(*grant);
                }
                relayed = relayed.saturating_add(send(&settle(id, done, spent, budget, &mut over_budget, warn)));
                if relayed > MAX_RELAYED_BYTES {
                    break Err(Ended::Failed(QueryError::Budget(format!(
                        "the query's scans returned more than {} MiB — narrow it",
                        MAX_RELAYED_BYTES >> 20
                    ))));
                }
                pump!();
            }
            frame = in_rx.recv(), if inflight.len() + deferred.len() < MAX_IN_FLIGHT => {
                let msg = match frame.flatten() {
                    None => break Err(Ended::Closed),
                    Some(Err(why)) => break Err(Ended::Failed(violation(why))),
                    Some(Ok(m)) => m,
                };
                match msg {
                    Msg::Done(r) => {
                        break match r {
                            Ok(_) if over_budget => Err(Ended::Failed(QueryError::Budget(format!(
                                "the query read more than {budget} units (index rows + point reads)"
                            )))),
                            Ok(a) => match check_answer(&a, job) {
                                Ok(()) => Ok(a),
                                Err(why) => Err(Ended::Failed(violation(why))),
                            },
                            Err(e) => Err(Ended::Failed(clip_error(QueryError::from(e)))),
                        };
                    }
                    Msg::Req(FromWorker::Scan { id, target, req }) => {
                        asked += 1;
                        if asked > MAX_REQUESTS {
                            break Err(Ended::Failed(violation("too many requests")));
                        }
                        if job.targets.get(target as usize).is_none() {
                            break Err(Ended::Failed(violation("unknown target")));
                        }
                        if let Err(why) = check_scan(&req.scan) {
                            break Err(Ended::Failed(violation(why)));
                        }
                        deferred.push_back((id, target as usize, req.scan));
                        pump!();
                    }
                    Msg::Req(FromWorker::View { id, path }) => {
                        asked += 1;
                        if asked > MAX_REQUESTS {
                            break Err(Ended::Failed(violation("too many requests")));
                        }
                        if !cairn_query::view::is_view_path(&path) {
                            break Err(Ended::Failed(violation("not a view path")));
                        }
                        let views = job.views.clone();
                        inflight.push(Box::pin(async move {
                            let r = match views {
                                Some(v) => v.get(&path).await,
                                None => Err(QueryError::Internal(
                                    "view tables are not available".into(),
                                )),
                            };
                            (id, Pending::View(r))
                        }));
                    }
                    Msg::Req(FromWorker::Done(_)) => break Err(Ended::Failed(violation("frame tag mismatch"))),
                }
            }
        }
    };
    reader.abort();
    writer.abort();
    finished
}

#[cfg(test)]
mod tests {
    //! A malicious child talks to [`converse`] over in-memory pipes: the parent
    //! must enforce tenant/ranges/budget itself, and block every request outside
    //! the contract.

    use {
        super::*,
        cairn_query::{filter::Pred, invocation::Order, remote::WirePath, worker::WorkerAnswer},
        tokio::io::{AsyncRead, AsyncWrite, DuplexStream},
    };

    /// Fake remote node: records the requests it receives, returns `read` units each time.
    struct Recorder {
        got: Mutex<Vec<ScanRequest>>,
        read: u64,
    }

    #[async_trait::async_trait]
    impl RemoteScan for Recorder {
        fn node(&self) -> &str {
            "n2"
        }
        fn ranges(&self) -> &[u32] {
            &[]
        }
        async fn scan(&self, req: ScanRequest) -> Result<ScanResponse, QueryError> {
            self.got.lock().unwrap().push(req);
            Ok(ScanResponse {
                v: WIRE_VERSION,
                read: self.read,
                ..Default::default()
            })
        }
    }

    struct Views(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl ViewSource for Views {
        async fn get(&self, path: &str) -> Result<serde_json::Value, QueryError> {
            self.0.lock().unwrap().push(path.to_string());
            Ok(serde_json::json!([]))
        }
    }

    fn job(remote: Arc<dyn RemoteScan>, views: Arc<Views>, deadline: Duration) -> Relay {
        Relay {
            tenant: TenantScope::One("acme".into()),
            class_counts: Some(Vec::new()),
            targets: vec![(
                Target {
                    node: "n2".into(),
                    ranges: vec![3, 4],
                },
                Serve::Far(remote),
            )],
            views: Some(views),
            sql: "SELECT 1".into(),
            params: Vec::new(),
            limits: cairn_query::Limits {
                deadline,
                read_budget: 100,
                ..Default::default()
            },
        }
    }

    /// Fake child: the read end (the parent's messages) and the write end (the child's messages).
    struct Evil {
        from_parent: DuplexStream,
        to_parent: DuplexStream,
    }

    impl Evil {
        async fn read(&mut self) -> ToWorker {
            let mut len = [0u8; 4];
            self.from_parent.read_exact(&mut len).await.unwrap();
            let mut b = vec![0u8; u32::from_be_bytes(len) as usize];
            self.from_parent.read_exact(&mut b).await.unwrap();
            serde_json::from_slice(&b).unwrap()
        }
        async fn raw(&mut self, tag: u8, bytes: &[u8]) {
            self.to_parent.write_all(&[tag]).await.unwrap();
            write_frame(&mut self.to_parent, bytes).await.unwrap();
        }
        async fn send(&mut self, m: &FromWorker) {
            let tag = match m {
                FromWorker::Done(_) => TAG_DONE,
                _ => TAG_REQUEST,
            };
            self.raw(tag, &serde_json::to_vec(m).unwrap()).await;
        }
    }

    /// Pipes for [`converse`]: (the child's stdin, the child's stdout, the fake child).
    fn pipes() -> (
        impl AsyncWrite + Unpin + Send,
        impl AsyncRead + Unpin + Send,
        Evil,
    ) {
        let (stdin, from_parent) = tokio::io::duplex(1 << 20);
        let (to_parent, stdout) = tokio::io::duplex(1 << 20);
        (
            stdin,
            stdout,
            Evil {
                from_parent,
                to_parent,
            },
        )
    }

    fn scan(target: u32, pred: Pred) -> FromWorker {
        FromWorker::Scan {
            id: 7,
            target,
            req: ScanRequest {
                v: WIRE_VERSION,
                tenant: Some("evil".into()),
                ranges: vec![99, 100],
                read_budget: u64::MAX,
                scan: Scan::Invocation {
                    pred: Box::new(pred),
                    order: Order::CreatedDesc,
                    path: WirePath::Meta,
                    fetch: None,
                    projection: Vec::new(),
                },
            },
        }
    }

    fn done() -> FromWorker {
        FromWorker::Done(Ok(WorkerAnswer {
            columns: serde_json::value::to_raw_value(&serde_json::json!([])).unwrap(),
            rows: serde_json::value::to_raw_value(&serde_json::json!([[1]])).unwrap(),
            row_count: 1,
            warnings: Vec::new(),
            read: 0,
        }))
    }

    /// Runs [`converse`] with the fake child `play`.
    async fn run<F, Fut>(
        remote_read: u64,
        play: F,
    ) -> (Result<Answered, Ended>, Arc<Recorder>, Arc<Views>, u64)
    where
        F: FnOnce(Evil) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let remote = Arc::new(Recorder {
            got: Mutex::default(),
            read: remote_read,
        });
        let views = Arc::new(Views(Mutex::default()));
        let j = job(remote.clone(), views.clone(), Duration::from_secs(5));
        let (stdin, stdout, evil) = pipes();
        let child = tokio::spawn(play(evil));
        let spent = AtomicU64::new(0);
        let warn = QueryCtx::new(j.tenant.clone(), Vec::new());
        let r = converse(stdin, stdout, &j, &spent, &warn).await;
        child.abort();
        (r, remote, views, spent.load(Ordering::Relaxed))
    }

    fn failed(r: &Result<Answered, Ended>) -> String {
        match r {
            Err(Ended::Failed(e)) => e.to_string(),
            Err(Ended::Closed) => "closed".into(),
            Ok(_) => "ok".into(),
        }
    }

    /// The parent ignores the tenant, ranges and budget the child declares: it uses its own.
    #[tokio::test]
    async fn parent_overrides_tenant_ranges_and_budget() {
        let (r, remote, _, spent) = run(10, |mut c| async move {
            let ToWorker::Job(j) = c.read().await else {
                panic!("job first")
            };
            assert_eq!(j.tenant.as_deref(), Some("acme"));
            c.send(&scan(0, Pred::default())).await;
            let ToWorker::Scan {
                id: 7,
                reply: Ok(_),
            } = c.read().await
            else {
                panic!("scan answer")
            };
            c.send(&done()).await;
        })
        .await;
        assert!(r.is_ok(), "{}", failed(&r));
        let got = remote.got.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].tenant.as_deref(), Some("acme"));
        assert_eq!(got[0].ranges, vec![3, 4]);
        assert_eq!(got[0].read_budget, 100);
        assert_eq!(spent, 10);
    }

    /// The read budget is counted by the parent: exceeded ⇒ the child gets an
    /// error, the statement is `query_budget` even though the child still sends `Done`.
    #[tokio::test]
    async fn parent_counts_the_read_budget() {
        let (r, _, _, spent) = run(150, |mut c| async move {
            c.read().await;
            c.send(&scan(0, Pred::default())).await;
            let ToWorker::Scan {
                reply: Err(WireError::Budget(_)),
                ..
            } = c.read().await
            else {
                panic!("must be a budget error")
            };
            // Ask again: the parent does not scan any more.
            c.send(&scan(0, Pred::default())).await;
            let ToWorker::Scan {
                reply: Err(WireError::Budget(_)),
                ..
            } = c.read().await
            else {
                panic!("still a budget error")
            };
            c.send(&done()).await;
        })
        .await;
        assert!(failed(&r).contains("query_budget"), "{}", failed(&r));
        assert_eq!(spent, 150);
    }

    /// Every contract violation ⇒ the statement stops with a clear error, the request is not served.
    #[tokio::test]
    async fn malformed_requests_stop_the_query() {
        let big = Pred {
            services: Some(vec!["a".into(); MAX_PRED_ENTRIES + 1]),
            ..Default::default()
        };
        type Play = Box<
            dyn FnOnce(Evil) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                + Send,
        >;
        let cases: Vec<(&str, Play)> = vec![
            (
                "unknown target",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.send(&scan(5, Pred::default())).await;
                    })
                }),
            ),
            (
                "filter too large",
                Box::new(move |mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.send(&scan(0, big)).await;
                    })
                }),
            ),
            (
                "not a view path",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.send(&FromWorker::View {
                            id: 1,
                            path: "/admin/failpoints".into(),
                        })
                        .await;
                    })
                }),
            ),
            (
                "unreadable frame",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.raw(TAG_REQUEST, b"{\"nope\":1}").await;
                    })
                }),
            ),
            (
                "frame too large",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.to_parent.write_all(&[TAG_DONE]).await.unwrap();
                        c.to_parent
                            .write_all(&u32::MAX.to_be_bytes())
                            .await
                            .unwrap();
                    })
                }),
            ),
            (
                "request frame too large",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        let path = format!("/services{}", " ".repeat(MAX_REQUEST_FRAME));
                        c.send(&FromWorker::View { id: 1, path }).await;
                    })
                }),
            ),
            (
                "answer is not two arrays",
                Box::new(|mut c| {
                    Box::pin(async move {
                        c.read().await;
                        c.send(&FromWorker::Done(Ok(WorkerAnswer {
                            columns: serde_json::value::to_raw_value(&serde_json::json!({}))
                                .unwrap(),
                            rows: serde_json::value::to_raw_value(&serde_json::json!([])).unwrap(),
                            row_count: 0,
                            warnings: Vec::new(),
                            read: 0,
                        })))
                        .await;
                    })
                }),
            ),
        ];
        for (why, play) in cases {
            let (r, remote, views, _) = run(1, play).await;
            let msg = failed(&r);
            assert!(
                msg.contains(why) && msg.contains("query_budget"),
                "{why}: {msg}"
            );
            assert!(remote.got.lock().unwrap().is_empty(), "{why}: scanned");
            assert!(views.0.lock().unwrap().is_empty(), "{why}: view called");
        }
    }

    /// A child's answer carries only warnings within caps (entry count, the
    /// statement's ranges, message size) and a row count ≤ the row cap:
    /// otherwise it is a violation, handled within the statement's deadline
    /// (previously the parent deduplicated 200 000 warnings in O(n²) on the main
    /// runtime after the deadline).
    #[tokio::test]
    async fn answer_lists_are_capped() {
        let answer = |warnings: Vec<Warning>, rows: u64| {
            FromWorker::Done(Ok(WorkerAnswer {
                columns: serde_json::value::to_raw_value(&serde_json::json!([])).unwrap(),
                rows: serde_json::value::to_raw_value(&serde_json::json!([])).unwrap(),
                row_count: rows,
                warnings,
                read: 0,
            }))
        };
        let many: Vec<Warning> = (0..200_000)
            .map(|i| Warning {
                range: Some(i),
                reason: String::new(),
            })
            .collect();
        let foreign = vec![Warning {
            range: Some(99),
            reason: "partial".into(),
        }];
        let long = vec![Warning {
            range: Some(3),
            reason: "x".repeat(10_000),
        }];
        for (why, done) in [
            ("warnings", answer(many, 0)),
            ("warnings", answer(foreign, 0)),
            ("warnings", answer(long, 0)),
            ("rows", answer(Vec::new(), 10_001)),
        ] {
            let t0 = tokio::time::Instant::now();
            let (r, ..) = run(0, move |mut c| async move {
                c.read().await;
                c.send(&done).await;
            })
            .await;
            let msg = failed(&r);
            assert!(
                msg.contains(why) && msg.contains("query_budget"),
                "{why}: {msg}"
            );
            assert!(
                t0.elapsed() < Duration::from_secs(2),
                "{why}: {:?}",
                t0.elapsed()
            );
        }
        // A valid warning (the statement's range, short message) goes through.
        let ok = vec![Warning {
            range: Some(3),
            reason: "partial: the range could not be read — retry later".into(),
        }];
        let (r, ..) = run(0, move |mut c| async move {
            c.read().await;
            c.send(&answer(ok, 1)).await;
        })
        .await;
        assert!(r.is_ok(), "{}", failed(&r));
    }

    /// Fake remote node that waits for `want` concurrent requests (at most 300 ms)
    /// before answering; measures the total read budget granted to scans in flight.
    struct Gated {
        want: u64,
        seen: AtomicU64,
        open: tokio::sync::Notify,
        now: AtomicU64,
        max: AtomicU64,
    }

    #[async_trait::async_trait]
    impl RemoteScan for Gated {
        fn node(&self) -> &str {
            "n2"
        }
        fn ranges(&self) -> &[u32] {
            &[]
        }
        async fn scan(&self, req: ScanRequest) -> Result<ScanResponse, QueryError> {
            let g = req.read_budget;
            let now = self.now.fetch_add(g, Ordering::SeqCst) + g;
            self.max.fetch_max(now, Ordering::SeqCst);
            if self.seen.fetch_add(1, Ordering::SeqCst) + 1 >= self.want {
                self.open.notify_waiters();
            } else {
                let _ =
                    tokio::time::timeout(Duration::from_millis(300), self.open.notified()).await;
            }
            self.now.fetch_sub(g, Ordering::SeqCst);
            Ok(ScanResponse {
                v: WIRE_VERSION,
                read: 10,
                ..Default::default()
            })
        }
    }

    /// Concurrent scans of one statement **share** the read budget: the total
    /// budget granted to scans in flight ≤ the statement's budget (previously
    /// each one got the whole remainder ⇒ 16 × the budget).
    #[tokio::test]
    async fn parallel_scans_share_one_read_budget() {
        let gated = Arc::new(Gated {
            want: 16,
            seen: AtomicU64::new(0),
            open: tokio::sync::Notify::new(),
            now: AtomicU64::new(0),
            max: AtomicU64::new(0),
        });
        let views = Arc::new(Views(Mutex::default()));
        let j = job(gated.clone(), views, Duration::from_secs(10));
        let (stdin, stdout, mut c) = pipes();
        let child = tokio::spawn(async move {
            c.read().await;
            for id in 0..16u64 {
                let FromWorker::Scan { target, req, .. } = scan(0, Pred::default()) else {
                    unreachable!()
                };
                c.send(&FromWorker::Scan { id, target, req }).await;
            }
            for _ in 0..16 {
                c.read().await;
            }
            c.send(&done()).await;
        });
        let spent = AtomicU64::new(0);
        let warn = QueryCtx::new(j.tenant.clone(), Vec::new());
        let _ = converse(stdin, stdout, &j, &spent, &warn).await;
        child.abort();
        let max = gated.max.load(Ordering::SeqCst);
        assert!(
            max <= 100,
            "concurrent scans were granted {max} units (budget 100)"
        );
    }

    /// A request frame declaring a size above the cap is refused **before** the
    /// node allocates a buffer or waits for the body: the frame kind comes before the length.
    #[tokio::test]
    async fn oversized_request_frames_are_refused_before_reading() {
        let t0 = tokio::time::Instant::now();
        let (r, ..) = run(0, |mut c| async move {
            c.read().await;
            let mut head = vec![b'R'];
            head.extend_from_slice(&(32u32 << 20).to_be_bytes());
            c.to_parent.write_all(&head).await.unwrap();
            // No body sent: the node must not wait for it.
            std::future::pending::<()>().await;
        })
        .await;
        let msg = failed(&r);
        assert!(msg.contains("request frame too large"), "{msg}");
        assert!(t0.elapsed() < Duration::from_secs(1), "{:?}", t0.elapsed());
    }

    /// More than [`MAX_REQUESTS`] requests ⇒ stop.
    #[tokio::test]
    async fn request_count_is_capped() {
        let (r, _, views, _) = run(0, |mut c| async move {
            c.read().await;
            for id in 0..=MAX_REQUESTS as u64 {
                c.send(&FromWorker::View {
                    id,
                    path: "/services".into(),
                })
                .await;
                if !matches!(c.read().await, ToWorker::View { .. }) {
                    return;
                }
            }
        })
        .await;
        assert!(failed(&r).contains("too many requests"), "{}", failed(&r));
        assert_eq!(views.0.lock().unwrap().len(), MAX_REQUESTS);
    }

    /// The child closes its pipe before `Done` ⇒ `Closed` (the caller reads the
    /// exit status); a silent child ⇒ deadline plus grace.
    #[tokio::test]
    async fn closed_and_silent_workers_end_the_query() {
        let (r, ..) = run(0, |mut c| async move {
            c.read().await;
            drop(c);
        })
        .await;
        assert!(matches!(r, Err(Ended::Closed)), "{}", failed(&r));
        let remote = Arc::new(Recorder {
            got: Mutex::default(),
            read: 0,
        });
        let views = Arc::new(Views(Mutex::default()));
        let j = job(remote, views, Duration::from_millis(100));
        let (stdin, stdout, evil) = pipes();
        let t0 = tokio::time::Instant::now();
        let r = converse(
            stdin,
            stdout,
            &j,
            &AtomicU64::new(0),
            &QueryCtx::new(j.tenant.clone(), Vec::new()),
        )
        .await;
        drop(evil);
        assert!(failed(&r).contains("ran over"), "{}", failed(&r));
        assert!(t0.elapsed() < Duration::from_millis(100) + KILL_GRACE + Duration::from_secs(1));
    }
}
