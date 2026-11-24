// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The child process of `/query`**.
//!
//! DataFusion's planning, optimisation and constant folding run synchronously,
//! with no memory or time cap (security review): one query of ≤ 64 KiB can
//! kill the process that runs it. So a query written by the asker runs entirely
//! in a separate process (binary `cairn-query-worker`): parsing, the AST
//! checker ([`crate::sql_check`]), planning, execution. That process
//!
//! - holds no range handle, cluster secret, environment variable or asker
//!   identity: the parent node is the "remote node" of every range
//!   ([`crate::remote`]) — the child sends [`ScanRequest`], the parent enforces
//!   tenant, ranges and read budget itself and then serves; view tables go
//!   through the parent (only the paths of [`crate::view::VIEWS`]);
//! - counts allocations in its own global allocator ([`CappedAlloc`]): over the
//!   cap ⇒ exits at once with [`EXIT_MEMORY`] — only the child dies;
//! - is killed by the parent when the query deadline passes (cutting the
//!   synchronous CPU that `timeout_at` cannot cut);
//! - runs **one** query and exits; the parent keeps a few warm children ready.
//!
//! Wire: stdin/stdout pipes, each frame = a big-endian `u32` length + JSON. The
//! parent sends [`ToWorker::Job`] and then the replies; the child sends
//! [`FromWorker`]. The child's messages are a **trust boundary**: the parent
//! treats the child as a stranger (caps on frame size, request count, filter
//! size; trusts no tenant/range/budget field the child sends).

use {
    crate::{
        ctx::{QueryCtx, TenantScope, Warning},
        remote::{RemoteScan, ScanRequest, ScanResponse},
        session::{Limits, Param},
        view::ViewSource,
        QueryError,
    },
    serde::{Deserialize, Serialize},
    serde_json::value::RawValue,
    std::{
        alloc::{GlobalAlloc, Layout, System},
        collections::BTreeMap,
        io::{Read, Write},
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
};

/// Binary name of the child process (looked up next to the `cairnflow` binary).
pub const BINARY: &str = "cairn-query-worker";

/// Exit code when the child goes over the memory cap ([`CappedAlloc`]).
pub const EXIT_MEMORY: i32 = 86;
/// Exit code when the wire breaks (unknown frame, missing `Job`).
pub const EXIT_PROTOCOL: i32 = 87;

/// Cap on one request frame from the child (scan, view table).
pub const MAX_REQUEST_FRAME: usize = 1 << 20;
/// Cap on the size of the JSON answer (`columns` + `rows`) the child sends:
/// larger ⇒ the child answers `query_budget` instead of sending it. The node
/// moves the bytes unchanged into the HTTP body.
pub const MAX_ANSWER_BYTES: usize = 16 << 20;
/// Cap on the child's answer frame (`Done`): the answer plus warnings.
pub const MAX_ANSWER_FRAME: usize = MAX_ANSWER_BYTES + (1 << 20);
/// Child → parent frame kind, sent **before** the length: the node knows the
/// frame's cap before allocating a buffer. `R` = request
/// (≤ [`MAX_REQUEST_FRAME`]), `D` = answer (≤ [`MAX_ANSWER_FRAME`]).
pub const TAG_REQUEST: u8 = b'R';
pub const TAG_DONE: u8 = b'D';
/// Cap on the `Job` frame the child receives (SQL ≤ 64 KiB, parameters by the request body cap).
pub const MAX_JOB_FRAME: usize = 16 << 20;
/// Cap on the number of requests (scans + view tables) of one query.
pub const MAX_REQUESTS: usize = 1_024;
/// Cap on the number of requests one child has in service at once.
pub const MAX_IN_FLIGHT: usize = 16;
/// Cap on the element count of a pushed-down filter ([`crate::filter::Pred::entries`]):
/// a query of ≤ 64 KiB cannot write a longer list (`'a',` is 4 bytes).
pub const MAX_PRED_ENTRIES: usize = crate::MAX_SQL_BYTES / 4;
/// Cap on the number of parameter pairs of a table by key.
pub const MAX_KEYED_ARGS: usize = 16;

/// A group of ranges the child asks about through the parent: the parent's local ranges, or a remote node's.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Target {
    pub node: String,
    pub ranges: Vec<u32>,
}

/// The query the child must run.
#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub sql: String,
    pub params: Vec<Param>,
    /// `None` = every tenant (`cluster-admin`). Only so the child filters like
    /// the coordinating node; the parent enforces the tenant on every scan and
    /// does not trust the child.
    pub tenant: Option<String>,
    pub targets: Vec<Target>,
    /// Counters of the node's local ranges for the path chooser
    /// ([`crate::invocation::class_counts`]); `None` = some range is not built yet.
    pub class_counts: Option<Vec<crate::invocation::ClassCount>>,
    /// Whether view tables are registered.
    pub views: bool,
    pub memory_bytes: u64,
    pub deadline_ms: u64,
    pub max_rows: u64,
    pub read_budget: u64,
}

/// An error on the wire — keeps the class of [`QueryError`].
#[derive(Debug, Serialize, Deserialize)]
pub enum WireError {
    Sql(String),
    Budget(String),
    Internal(String),
}

impl From<&QueryError> for WireError {
    fn from(e: &QueryError) -> Self {
        match e {
            QueryError::Sql(m) => Self::Sql(m.clone()),
            QueryError::Budget(_) => Self::Budget(e.to_string()),
            QueryError::Internal(m) => Self::Internal(m.clone()),
        }
    }
}

impl From<WireError> for QueryError {
    fn from(e: WireError) -> Self {
        match e {
            WireError::Sql(m) => Self::Sql(m),
            // `Budget` already carries the `query_budget:` prefix once turned into
            // a string on the sending side; do not add it twice.
            WireError::Budget(m) => Self::Budget(
                m.strip_prefix("query_budget: ")
                    .map(str::to_string)
                    .unwrap_or(m),
            ),
            WireError::Internal(m) => Self::Internal(m),
        }
    }
}

/// Cha → con.
#[derive(Debug, Serialize, Deserialize)]
pub enum ToWorker {
    Job(Job),
    Scan {
        id: u64,
        reply: Result<ScanResponse, WireError>,
    },
    View {
        id: u64,
        reply: Result<serde_json::Value, WireError>,
    },
}

/// The child's answer: `columns`/`rows` as raw JSON bytes (the parent passes them straight on).
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerAnswer {
    pub columns: Box<RawValue>,
    pub rows: Box<RawValue>,
    pub row_count: u64,
    pub warnings: Vec<Warning>,
    pub read: u64,
}

/// The child's answer, borrowed from the frame body (node side): `columns`/`rows`
/// are slices of the frame, not copies.
#[derive(Debug, Deserialize)]
pub struct WorkerAnswerRef<'a> {
    #[serde(borrow)]
    pub columns: &'a RawValue,
    #[serde(borrow)]
    pub rows: &'a RawValue,
    pub row_count: u64,
    pub warnings: Vec<Warning>,
    pub read: u64,
}

/// A `D` frame on the node side: only the `Done` variant of [`FromWorker`].
#[derive(Debug, Deserialize)]
pub enum DoneRef<'a> {
    #[serde(borrow)]
    Done(Result<WorkerAnswerRef<'a>, WireError>),
}

/// Child → parent. Externally tagged enum (serde's default): `RawValue` does not
/// go through the buffer of an internally tagged enum.
#[derive(Debug, Serialize, Deserialize)]
pub enum FromWorker {
    Scan {
        id: u64,
        target: u32,
        req: ScanRequest,
    },
    View {
        id: u64,
        path: String,
    },
    Done(Result<WorkerAnswer, WireError>),
}

/// Write one frame.
pub fn write_frame(w: &mut impl Write, body: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(body.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame too large"))?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(body)
}

/// Read one frame of ≤ `max` bytes. `Ok(None)` = the stream ended right at a frame start.
pub fn read_frame(r: &mut impl Read, max: usize) -> std::io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {len} bytes is over {max}"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok(Some(body))
}

// ---------------------------------------------------------------- allocator

/// Global allocator counting the bytes the child process has allocated; over
/// the cap ⇒ prints one line to stderr (without allocating), then
/// `_exit(EXIT_MEMORY)`. Cap `0` = no cap. Only the `cairn-query-worker` binary
/// declares it — the node's hot path counts nothing.
pub struct CappedAlloc {
    used: AtomicUsize,
    limit: AtomicUsize,
}

impl CappedAlloc {
    pub const fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(0),
        }
    }

    pub fn set_limit(&self, bytes: usize) {
        self.limit.store(bytes, Ordering::Relaxed);
    }

    /// Bytes currently allocated.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn grow(&self, n: usize) {
        let now = self.used.fetch_add(n, Ordering::Relaxed).saturating_add(n);
        let limit = self.limit.load(Ordering::Relaxed);
        if limit != 0 && now > limit {
            over_limit();
        }
    }

    fn shrink(&self, n: usize) {
        self.used.fetch_sub(n, Ordering::Relaxed);
    }
}

impl Default for CappedAlloc {
    fn default() -> Self {
        Self::new()
    }
}

fn over_limit() -> ! {
    const MSG: &[u8] = b"cairn-query-worker: memory limit reached\n";
    // SAFETY: `write` and `_exit` do not allocate; callable from inside the allocator.
    unsafe {
        libc::write(2, MSG.as_ptr().cast(), MSG.len());
        libc::_exit(EXIT_MEMORY)
    }
}

// SAFETY: every operation goes straight to `System`; only adds to / subtracts from the counter.
unsafe impl GlobalAlloc for CappedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.grow(layout.size());
        // SAFETY: the contract of `GlobalAlloc::alloc` is passed through unchanged.
        let p = unsafe { System.alloc(layout) };
        if p.is_null() {
            self.shrink(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.grow(layout.size());
        // SAFETY: as above.
        let p = unsafe { System.alloc_zeroed(layout) };
        if p.is_null() {
            self.shrink(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as above.
        unsafe { System.dealloc(ptr, layout) };
        self.shrink(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old = layout.size();
        if new_size > old {
            self.grow(new_size - old);
        }
        // SAFETY: as above.
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if p.is_null() {
            if new_size > old {
                self.shrink(new_size - old);
            }
        } else if new_size < old {
            self.shrink(old - new_size);
        }
        p
    }
}

/// Exit at once, running no destructor (another thread may be holding a lock).
fn exit_now(code: i32) -> ! {
    // SAFETY: `_exit` is always safe to call.
    unsafe { libc::_exit(code) }
}

/// Linux: die with the parent (`PR_SET_PDEATHSIG`), be the first candidate for
/// the OOM killer, write no core. macOS: parent dies ⇒ stdin ends ⇒ the reader thread exits.
fn harden() {
    // SAFETY: `setrlimit` with a valid struct.
    unsafe {
        let none = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &none);
    }
    #[cfg(target_os = "linux")]
    {
        // Not dumpable: `execve` resets the flag to 1, so set it again here — a
        // process of the same user (a sibling child of another tenant) cannot
        // open `/proc/<child>/{fd,environ,mem}`, cannot `ptrace`. If it cannot
        // be set ⇒ take no work.
        // SAFETY: `prctl` with constant arguments.
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0) } != 0 {
            exit_now(EXIT_PROTOCOL);
        }
        let _ = std::fs::write("/proc/self/oom_score_adj", "1000");
    }
}

/// Address-space cap of the child (`RLIMIT_AS`) added on top of the memory cap:
/// the binary's code, the runtime's 2 × 64 MiB stacks, malloc's reserved areas
/// (`MALLOC_ARENA_MAX=2`). The kernel enforces it — a hijacked child cannot
/// bypass it the way it could bypass its own counting allocator.
pub const ADDRESS_SPACE_MARGIN: u64 = 1 << 30;

/// Runs in the child **between `fork` and `exec`** (the node's `pre_exec`,
/// [`Command`](std::process::Command)): syscalls only, no allocation, no lock
/// — async-signal-safe.
///
/// Linux: the child dies with the parent (`PR_SET_PDEATHSIG`, kept across the
/// `execve` of a non-setuid binary), then compares `getppid()` with `parent` —
/// the node's pid captured **before** `fork` — to catch the case where the
/// parent died before the flag was set. Does not compare with `1`: when the node
/// is PID 1 (Docker without init) that is the real parent. `PDEATHSIG` fires
/// when the **thread** that spawned the child exits: the node spawns children on
/// a fixed thread of the query runtime, not on `spawn_blocking`.
///
/// # Safety
/// Call only between `fork` and `exec` of a child process.
pub unsafe fn before_exec(parent: libc::pid_t, memory_bytes: u64) -> std::io::Result<()> {
    let fail = || Err(std::io::Error::last_os_error());
    #[cfg(target_os = "linux")]
    // SAFETY: `prctl`/`getppid` are plain syscalls.
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
            return fail();
        }
        if libc::getppid() != parent {
            return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = parent;
    // SAFETY: plain syscalls with constant or on-stack arguments.
    unsafe {
        // Own session: the node kills the whole group (`kill(-pid)`).
        if libc::setsid() == -1 {
            return fail();
        }
        let set = |which, n: u64| {
            let r = libc::rlimit {
                rlim_cur: n as libc::rlim_t,
                rlim_max: n as libc::rlim_t,
            };
            libc::setrlimit(which, &r)
        };
        // No core, no file writes (pipes do not count), few fds.
        if set(libc::RLIMIT_CORE, 0) != 0
            || set(libc::RLIMIT_FSIZE, 0) != 0
            || set(libc::RLIMIT_NOFILE, 64) != 0
        {
            return fail();
        }
        #[cfg(target_os = "linux")]
        {
            if set(
                libc::RLIMIT_AS,
                memory_bytes.saturating_add(ADDRESS_SPACE_MARGIN),
            ) != 0
            {
                return fail();
            }
            // Stray fds from the C library or a launcher missing `CLOEXEC`.
            // Kernels < 5.9 have no `close_range`: ignore it (Rust's fds have
            // `CLOEXEC`).
            libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = memory_bytes;
        if libc::chdir(c"/".as_ptr()) != 0 {
            return fail();
        }
    }
    Ok(())
}

/// **The child's seccomp** (Linux): after the child has built its runtime,
/// before it reads work. A deny list (`EPERM`) — the child only needs to read/write
/// pipes, allocate memory, threads, clocks:
///
/// - network: `socket`, `socketpair`, `connect`;
/// - processes: `fork`/`vfork`, `clone` **without** `CLONE_THREAD`, `clone3`
///   (`ENOSYS` — glibc falls back to `clone`), `execve`/`execveat`;
/// - signals to other processes: `kill`/`tgkill` only with its own pid;
///   `tkill`, `rt_(tg)sigqueueinfo`, `pidfd_*`;
/// - inspecting other processes: `ptrace`, `process_vm_{readv,writev}`, `kcmp`;
/// - files: `open`/`openat`/`openat2`/`creat`, `open_by_handle_at`, and every call
///   that changes a file by path (`unlink(at)`, `rename(at,at2)`, `truncate`,
///   `link`/`symlink`/`mkdir`/`mknod`/`rmdir`, `chmod`/`chown`/`utime*` forms);
/// - limits of other processes: `prlimit64` only with pid 0 or its own pid;
/// - x86_64: any number with the x32 bit;
/// - bypassing the filter or escalating: `io_uring_*`, `bpf`, `userfaultfd`,
///   `perf_event_open`, `memfd_create`, `unshare`/`setns`/`mount`/`umount2`/
///   `pivot_root`/`chroot`, `keyctl`/`add_key`/`request_key`, module loading,
///   `process_madvise`, `fanotify_init`, `quotactl`, `swapon`/`swapoff`, `reboot`,
///   `kexec_*`.
///
/// An architecture other than x86_64/aarch64 ⇒ kill the process. Applies to every
/// thread (`SECCOMP_FILTER_FLAG_TSYNC`). Systems other than Linux: does nothing.
pub fn sandbox() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::install()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use libc::{c_long, sock_filter};

    const LD_W_ABS: u16 = 0x20; // BPF_LD | BPF_W | BPF_ABS
    const JEQ_K: u16 = 0x15; // BPF_JMP | BPF_JEQ | BPF_K
    const JSET_K: u16 = 0x45; // BPF_JMP | BPF_JSET | BPF_K
    const RET_K: u16 = 0x06; // BPF_RET | BPF_K
    const RET_ALLOW: u32 = 0x7fff_0000;
    const RET_KILL_PROCESS: u32 = 0x8000_0000;
    const RET_ERRNO: u32 = 0x0005_0000;
    // `struct seccomp_data`.
    const OFF_NR: u32 = 0;
    const OFF_ARCH: u32 = 4;
    const OFF_ARG0: u32 = 16; // low half (little-endian)
    #[cfg(target_arch = "x86_64")]
    const ARCH: u32 = 0xC000_003E; // AUDIT_ARCH_X86_64
    #[cfg(target_arch = "x86_64")]
    const X32_BIT: u32 = 0x4000_0000; // __X32_SYSCALL_BIT
    #[cfg(target_arch = "aarch64")]
    const ARCH: u32 = 0xC000_00B7; // AUDIT_ARCH_AARCH64

    fn op(code: u16, jt: u8, jf: u8, k: u32) -> sock_filter {
        sock_filter { code, jt, jf, k }
    }

    fn denied() -> Vec<c_long> {
        let v = vec![
            libc::SYS_socket,
            libc::SYS_socketpair,
            libc::SYS_connect,
            libc::SYS_execve,
            libc::SYS_execveat,
            libc::SYS_ptrace,
            libc::SYS_process_vm_readv,
            libc::SYS_process_vm_writev,
            libc::SYS_kcmp,
            libc::SYS_tkill,
            libc::SYS_rt_sigqueueinfo,
            libc::SYS_rt_tgsigqueueinfo,
            libc::SYS_pidfd_open,
            libc::SYS_pidfd_send_signal,
            libc::SYS_pidfd_getfd,
            libc::SYS_openat,
            libc::SYS_openat2,
            libc::SYS_open_by_handle_at,
            libc::SYS_io_uring_setup,
            libc::SYS_io_uring_enter,
            libc::SYS_io_uring_register,
            libc::SYS_bpf,
            libc::SYS_userfaultfd,
            libc::SYS_perf_event_open,
            libc::SYS_memfd_create,
            libc::SYS_unshare,
            libc::SYS_setns,
            libc::SYS_mount,
            libc::SYS_umount2,
            libc::SYS_pivot_root,
            libc::SYS_chroot,
            libc::SYS_keyctl,
            libc::SYS_add_key,
            libc::SYS_request_key,
            libc::SYS_init_module,
            libc::SYS_finit_module,
            libc::SYS_delete_module,
            // Changing files by path needs no open file.
            libc::SYS_unlinkat,
            libc::SYS_renameat2,
            libc::SYS_truncate,
            libc::SYS_linkat,
            libc::SYS_symlinkat,
            libc::SYS_mkdirat,
            libc::SYS_mknodat,
            libc::SYS_fchmodat,
            libc::SYS_fchownat,
            libc::SYS_utimensat,
            libc::SYS_name_to_handle_at,
            // Other processes' memory, the machine.
            libc::SYS_process_madvise,
            libc::SYS_fanotify_init,
            libc::SYS_quotactl,
            libc::SYS_swapon,
            libc::SYS_swapoff,
            libc::SYS_reboot,
            libc::SYS_kexec_load,
        ];
        // Old syscalls only x86_64 has (aarch64 has only `clone`/`openat` and the `*at` forms).
        #[cfg(target_arch = "x86_64")]
        let v = {
            let mut v = v;
            v.extend([
                libc::SYS_fork,
                libc::SYS_vfork,
                libc::SYS_open,
                libc::SYS_creat,
                libc::SYS_unlink,
                libc::SYS_rename,
                libc::SYS_renameat,
                libc::SYS_rmdir,
                libc::SYS_mkdir,
                libc::SYS_link,
                libc::SYS_symlink,
                libc::SYS_mknod,
                libc::SYS_chmod,
                libc::SYS_chown,
                libc::SYS_lchown,
                libc::SYS_utime,
                libc::SYS_utimes,
                libc::SYS_futimesat,
                libc::SYS_kexec_file_load,
            ]);
            v
        };
        v
    }

    /// The BPF program of [`super::sandbox`].
    pub(super) fn program(pid: u32) -> Vec<sock_filter> {
        let errno = |e: i32| RET_ERRNO | (e as u32 & 0xffff);
        let mut p = vec![
            op(LD_W_ABS, 0, 0, OFF_ARCH),
            op(JEQ_K, 1, 0, ARCH),
            op(RET_K, 0, 0, RET_KILL_PROCESS),
            op(LD_W_ABS, 0, 0, OFF_NR),
        ];
        // x86_64: a number with bit 30 set is the x32 ABI under the same
        // `AUDIT_ARCH_X86_64`; it would miss every exact match below.
        #[cfg(target_arch = "x86_64")]
        p.extend([
            op(JSET_K, 0, 1, X32_BIT),
            op(RET_K, 0, 0, errno(libc::EPERM)),
        ]);
        for nr in denied() {
            p.push(op(JEQ_K, 0, 1, nr as u32));
            p.push(op(RET_K, 0, 0, errno(libc::EPERM)));
        }
        p.push(op(JEQ_K, 0, 1, libc::SYS_clone3 as u32));
        p.push(op(RET_K, 0, 0, errno(libc::ENOSYS)));
        // `kill`/`tgkill`: only to itself (argument 0 = pid).
        for nr in [libc::SYS_kill, libc::SYS_tgkill] {
            p.extend([
                op(JEQ_K, 0, 4, nr as u32),
                op(LD_W_ABS, 0, 0, OFF_ARG0),
                op(JEQ_K, 0, 1, pid),
                op(RET_K, 0, 0, RET_ALLOW),
                op(RET_K, 0, 0, errno(libc::EPERM)),
                op(LD_W_ABS, 0, 0, OFF_NR),
            ]);
        }
        // `prlimit64`: only its own limits (argument 0 = 0 or its own pid).
        p.extend([
            op(JEQ_K, 0, 5, libc::SYS_prlimit64 as u32),
            op(LD_W_ABS, 0, 0, OFF_ARG0),
            op(JEQ_K, 2, 0, 0),
            op(JEQ_K, 1, 0, pid),
            op(RET_K, 0, 0, errno(libc::EPERM)),
            op(RET_K, 0, 0, RET_ALLOW),
            op(LD_W_ABS, 0, 0, OFF_NR),
        ]);
        // `clone`: only creating threads (flags in argument 0 on x86_64 and aarch64).
        p.extend([
            op(JEQ_K, 0, 4, libc::SYS_clone as u32),
            op(LD_W_ABS, 0, 0, OFF_ARG0),
            op(JSET_K, 1, 0, libc::CLONE_THREAD as u32),
            op(RET_K, 0, 0, errno(libc::EPERM)),
            op(RET_K, 0, 0, RET_ALLOW),
            op(LD_W_ABS, 0, 0, OFF_NR),
        ]);
        p.push(op(RET_K, 0, 0, RET_ALLOW));
        p
    }

    pub(super) fn install() -> std::io::Result<()> {
        // SAFETY: `getpid` has no precondition.
        let pid = unsafe { libc::getpid() } as u32;
        let mut prog = program(pid);
        let fprog = libc::sock_fprog {
            len: prog.len() as u16,
            filter: prog.as_mut_ptr(),
        };
        // SAFETY: `prctl`/`seccomp` with a program that lives until the call returns.
        unsafe {
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::syscall(
                libc::SYS_seccomp,
                libc::SECCOMP_SET_MODE_FILTER,
                libc::SECCOMP_FILTER_FLAG_TSYNC,
                &fprog as *const libc::sock_fprog,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------- child side

enum Reply {
    Scan(Result<ScanResponse, WireError>),
    View(Result<serde_json::Value, WireError>),
}

/// The child's channel to the parent: numbered requests, replies routed to the right waiter.
struct Link {
    next: AtomicU64,
    pending: Mutex<BTreeMap<u64, tokio::sync::oneshot::Sender<Reply>>>,
    out: std::sync::mpsc::Sender<Vec<u8>>,
}

impl Link {
    fn send(&self, msg: &FromWorker) {
        let tag = match msg {
            FromWorker::Done(_) => TAG_DONE,
            _ => TAG_REQUEST,
        };
        let mut frame = vec![tag];
        match serde_json::to_writer(&mut frame, msg) {
            Ok(()) => {
                let _ = self.out.send(frame);
            }
            Err(_) => exit_now(EXIT_PROTOCOL),
        }
    }

    async fn ask(&self, make: impl FnOnce(u64) -> FromWorker) -> Option<Reply> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id, tx);
        self.send(&make(id));
        rx.await.ok()
    }

    fn deliver(&self, frame: &[u8]) {
        let (id, reply) = match serde_json::from_slice::<ToWorker>(frame) {
            Ok(ToWorker::Scan { id, reply }) => (id, Reply::Scan(reply)),
            Ok(ToWorker::View { id, reply }) => (id, Reply::View(reply)),
            _ => exit_now(EXIT_PROTOCOL),
        };
        let tx = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
        if let Some(tx) = tx {
            let _ = tx.send(reply);
        }
    }
}

fn lost() -> QueryError {
    QueryError::Internal("the query worker lost its parent".into())
}

/// A group of ranges, asked about through the parent.
struct ParentRemote {
    link: Arc<Link>,
    target: u32,
    node: String,
    ranges: Vec<u32>,
}

#[async_trait::async_trait]
impl RemoteScan for ParentRemote {
    fn node(&self) -> &str {
        &self.node
    }

    fn ranges(&self) -> &[u32] {
        &self.ranges
    }

    async fn scan(&self, req: ScanRequest) -> Result<ScanResponse, QueryError> {
        let target = self.target;
        match self
            .link
            .ask(|id| FromWorker::Scan { id, target, req })
            .await
        {
            Some(Reply::Scan(r)) => r.map_err(QueryError::from),
            _ => Err(lost()),
        }
    }
}

/// View tables, asked about through the parent.
struct ParentViews {
    link: Arc<Link>,
}

#[async_trait::async_trait]
impl ViewSource for ParentViews {
    async fn get(&self, path: &str) -> Result<serde_json::Value, QueryError> {
        let path = path.to_string();
        match self.link.ask(|id| FromWorker::View { id, path }).await {
            Some(Reply::View(r)) => r.map_err(QueryError::from),
            _ => Err(lost()),
        }
    }
}

/// Run `job` (already received) with the parent at the other end of `link`.
async fn answer(job: Job, link: Arc<Link>) -> Result<WorkerAnswer, QueryError> {
    let tenant = match job.tenant {
        None => TenantScope::All,
        Some(t) => TenantScope::One(t),
    };
    let remotes = job
        .targets
        .into_iter()
        .enumerate()
        .map(|(i, t)| {
            Arc::new(ParentRemote {
                link: link.clone(),
                target: i as u32,
                node: t.node,
                ranges: t.ranges,
            }) as Arc<dyn RemoteScan>
        })
        .collect();
    let mut ctx = QueryCtx::new(tenant, Vec::new()).with_remotes(remotes);
    ctx.class_counts = Some(job.class_counts);
    if job.views {
        ctx = ctx.with_views(Arc::new(ParentViews { link }));
    }
    let limits = Limits {
        memory_bytes: job.memory_bytes as usize,
        deadline: Duration::from_millis(job.deadline_ms),
        max_rows: job.max_rows as usize,
        read_budget: job.read_budget,
    };
    let out = crate::session::run_with_params(ctx, &job.sql, &job.params, limits).await?;
    let rows = crate::answer::rows_json(&out.batches).map_err(QueryError::Internal)?;
    let too_big = || {
        QueryError::Budget(format!(
            "the answer is larger than {} MiB — add LIMIT or select fewer columns",
            MAX_ANSWER_BYTES >> 20
        ))
    };
    let columns = crate::answer::columns_json(&out.schema, &out.batches);
    let raw = |r: serde_json::Result<Box<RawValue>>| {
        r.map_err(|e| QueryError::Internal(format!("cannot encode the answer: {e}")))
    };
    let columns = raw(serde_json::value::to_raw_value(&columns))?;
    let row_count = rows.len() as u64;
    let rows = raw(serde_json::value::to_raw_value(&rows))?;
    if columns.get().len() + rows.get().len() > MAX_ANSWER_BYTES {
        return Err(too_big());
    }
    Ok(WorkerAnswer {
        columns,
        row_count,
        rows,
        warnings: out.warnings,
        read: out.read,
    })
}

/// `main` of the [`BINARY`] binary: read one `Job` from stdin, run it, write
/// `Done`, exit. Returns the exit code.
pub fn child_main() -> i32 {
    harden();
    // Build the runtime before taking work: a warm child pays this cost while
    // waiting, not on the query's path.
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(64 << 20)
        .thread_name("cairn-query-worker")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return EXIT_PROTOCOL,
    };
    // Seccomp after initialisation (runtime and threads exist), before taking work.
    if sandbox().is_err() {
        return EXIT_PROTOCOL;
    }
    let job = {
        let mut stdin = std::io::stdin().lock();
        match read_frame(&mut stdin, MAX_JOB_FRAME) {
            Ok(Some(b)) => match serde_json::from_slice::<ToWorker>(&b) {
                Ok(ToWorker::Job(j)) => j,
                _ => return EXIT_PROTOCOL,
            },
            // The parent closed the pipe before handing over work (node stopping, warm child dropped).
            Ok(None) => return 0,
            Err(_) => return EXIT_PROTOCOL,
        }
    };
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let writer = std::thread::spawn(move || {
        let mut out = std::io::stdout().lock();
        for frame in rx {
            // An empty frame = close (JSON is never empty).
            if frame.is_empty() {
                return;
            }
            // The first byte is the frame kind, sent before the length.
            if out
                .write_all(&frame[..1])
                .and_then(|()| write_frame(&mut out, &frame[1..]))
                .and_then(|()| out.flush())
                .is_err()
            {
                exit_now(0);
            }
        }
    });
    let link = Arc::new(Link {
        next: AtomicU64::new(0),
        pending: Mutex::default(),
        out: tx,
    });
    {
        let link = link.clone();
        std::thread::spawn(move || {
            let mut stdin = std::io::stdin().lock();
            loop {
                match read_frame(&mut stdin, usize::MAX) {
                    Ok(Some(frame)) => link.deliver(&frame),
                    // The parent closed the pipe or died: nobody reads the answer any more.
                    _ => exit_now(0),
                }
            }
        });
    }
    let result = rt.block_on(answer(job, link.clone()));
    link.send(&FromWorker::Done(result.map_err(|e| WireError::from(&e))));
    // The writer thread flushes every frame first, then stops.
    let _ = link.out.send(Vec::new());
    let _ = writer.join();
    // Do not wait for the runtime to shut down cleanly: its threads may be stuck in synchronous CPU.
    exit_now(0)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    //! Each block of [`super::sandbox`] on real Linux: a child process (the test
    //! binary itself, case `sandbox_child`) installs the filter, then tries each
    //! call and prints the result; the parent reads and asserts.

    const CHILD: &str = "CAIRN_SANDBOX_CHILD";

    fn errno() -> i32 {
        std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
    }

    /// Runs in the child process (the parent sets [`CHILD`]).
    #[test]
    #[ignore = "run only by sandbox_blocks_escapes"]
    fn sandbox_child() {
        if std::env::var_os(CHILD).is_none() {
            return;
        }
        // A file made before the filter: the path calls below must not touch it.
        let dir = std::env::temp_dir().join(format!(
            "cairn-sandbox-{}",
            cairn_store::entropy::process_id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("victim");
        std::fs::write(&file, b"keep").expect("victim file");
        let c = |p: &std::path::Path| {
            std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).expect("path")
        };
        let (victim, moved) = (c(&file), c(&dir.join("moved")));
        super::sandbox().expect("install seccomp");
        let say = |name: &str, r: i64, e: i32| println!("SANDBOX {name} {r} {e}");
        // SAFETY: syscalls with constant arguments; the child tries and prints, relying on no result.
        unsafe {
            let r = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
            say("socket", r.into(), errno());
            let r = libc::kill(libc::getppid(), 0);
            say("kill_parent", r.into(), errno());
            let r = libc::kill(libc::getpid(), 0);
            say("kill_self", r.into(), errno());
            let r = libc::syscall(libc::SYS_tgkill, libc::getpid(), libc::gettid(), 0);
            say("tgkill_self", r, errno());
            let r = libc::fork();
            if r == 0 {
                libc::_exit(0);
            }
            say("fork", r.into(), errno());
            let path = c"/bin/true";
            let argv = [path.as_ptr(), std::ptr::null()];
            let envp = [std::ptr::null()];
            let r = libc::execve(path.as_ptr(), argv.as_ptr(), envp.as_ptr());
            say("execve", r.into(), errno());
            let r = libc::ptrace(libc::PTRACE_ATTACH, libc::getppid(), 0, 0);
            say("ptrace", r, errno());
            let r = libc::open(c"/etc/hostname".as_ptr(), libc::O_RDONLY);
            say("open", r.into(), errno());
        }
        // SAFETY: syscalls on paths and limits; the child tries and prints, relying on no result.
        unsafe {
            let r = libc::syscall(libc::SYS_unlinkat, libc::AT_FDCWD, victim.as_ptr(), 0);
            say("unlinkat", r, errno());
            let r = libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                victim.as_ptr(),
                libc::AT_FDCWD,
                moved.as_ptr(),
                0,
            );
            say("renameat2", r, errno());
            let r = libc::syscall(libc::SYS_truncate, victim.as_ptr(), 0);
            say("truncate", r, errno());
            let r = libc::syscall(
                libc::SYS_fchmodat,
                libc::AT_FDCWD,
                victim.as_ptr(),
                0o777,
                0,
            );
            say("fchmodat", r, errno());
            #[cfg(target_arch = "x86_64")]
            {
                let r = libc::syscall(libc::SYS_unlink, victim.as_ptr());
                say("unlink", r, errno());
                let r = libc::syscall(libc::SYS_rename, victim.as_ptr(), moved.as_ptr());
                say("rename", r, errno());
                // The same call through the x32 ABI (bit 30 of the number, same
                // `AUDIT_ARCH_X86_64`): a filter matching plain numbers misses it.
                let r = libc::syscall(0x4000_0000 | libc::SYS_unlink, victim.as_ptr());
                say("x32_unlink", r, errno());
            }
            let mut lim: libc::rlimit = std::mem::zeroed();
            let r = libc::syscall(
                libc::SYS_prlimit64,
                libc::getppid(),
                libc::RLIMIT_NOFILE,
                std::ptr::null::<libc::rlimit>(),
                &mut lim as *mut libc::rlimit,
            );
            say("prlimit_parent", r, errno());
            let r = libc::syscall(
                libc::SYS_prlimit64,
                0,
                libc::RLIMIT_NOFILE,
                std::ptr::null::<libc::rlimit>(),
                &mut lim as *mut libc::rlimit,
            );
            say("prlimit_self", r, errno());
        }
        let kept = std::fs::metadata(&file)
            .map(|m| m.len() as i64)
            .unwrap_or(-1);
        say("victim_len", kept, 0);
        let thread = std::thread::spawn(|| 7).join().unwrap_or(0);
        say("thread", thread, 0);
    }

    /// **The child's seccomp blocks each escape** (Linux). Must see: `socket`,
    /// `kill` to the parent, `fork`, `execve`, `ptrace`, `open`, the path calls
    /// that need no open file (`unlink(at)`, `rename(at2)`, `truncate`,
    /// `fchmodat`), the x32 form of a call on x86_64, and `prlimit64` on another
    /// process ⇒ `-1` with `EPERM`, and the file the child aimed at is unchanged;
    /// `kill`/`tgkill`/`prlimit64` on itself and creating threads still work.
    #[test]
    fn sandbox_blocks_escapes() {
        let out = std::process::Command::new(std::env::current_exe().expect("exe"))
            .args([
                "--exact",
                "worker::tests::sandbox_child",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run child");
        let text = String::from_utf8_lossy(&out.stdout);
        let got = |name: &str| -> (i64, i32) {
            let line = text
                .lines()
                .flat_map(|l| l.split("SANDBOX ").skip(1))
                .find(|l| l.starts_with(&format!("{name} ")))
                .unwrap_or_else(|| {
                    panic!(
                        "missing {name}:\n{text}\n{}",
                        String::from_utf8_lossy(&out.stderr)
                    )
                });
            let mut it = line.split_whitespace().skip(1);
            let r = it.next().and_then(|x| x.parse().ok()).unwrap_or(i64::MIN);
            let e = it.next().and_then(|x| x.parse().ok()).unwrap_or(-1);
            (r, e)
        };
        let mut blocked = vec![
            "socket",
            "kill_parent",
            "fork",
            "execve",
            "ptrace",
            "open",
            "unlinkat",
            "renameat2",
            "truncate",
            "fchmodat",
            "prlimit_parent",
        ];
        if cfg!(target_arch = "x86_64") {
            blocked.extend(["unlink", "rename", "x32_unlink"]);
        }
        for name in blocked {
            assert_eq!(
                got(name),
                (-1, libc::EPERM),
                "{name} is not blocked:\n{text}"
            );
        }
        assert_eq!(got("kill_self").0, 0, "{text}");
        assert_eq!(got("tgkill_self").0, 0, "{text}");
        assert_eq!(got("prlimit_self").0, 0, "{text}");
        assert_eq!(
            got("victim_len").0,
            4,
            "the victim file was changed:\n{text}"
        );
        assert_eq!(got("thread").0, 7, "{text}");
    }
}
