// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Run the real `cairnflow` **binary** from a test — for what only a process sees:
//! command-line flags, environment variables, real ports, exit codes.
//!
//! `memory://` is private to each process, so each [`Binary`] has its own bucket.

use {
    anyhow::{bail, Context, Result},
    std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
        sync::{mpsc, Arc, Mutex},
        time::{Duration, Instant},
    },
};

/// The line the binary prints once it listens on the ingress port.
const INGRESS_LINE: &str = "cairnflow listening on ";
/// The line the binary prints for the internal port.
const INTERNAL_LINE: &str = "  internal:    ";

/// One `cairnflow` process. Drop ⇒ kill it.
pub struct Binary {
    child: Child,
    /// The ingress + admin port.
    pub url: String,
    /// The port only for other nodes (`/internal/*`).
    pub internal_url: String,
    log: Arc<Mutex<Vec<String>>>,
}

fn pump(child: &mut Child, log: &Arc<Mutex<Vec<String>>>) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel::<String>();
    for pipe in [
        Box::new(child.stdout.take().expect("stdout")) as Box<dyn std::io::Read + Send>,
        Box::new(child.stderr.take().expect("stderr")),
    ] {
        let log = Arc::clone(log);
        let tx = tx.clone();
        // Read continuously: a full pipe freezes the child process.
        std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines().map_while(|l| l.ok()) {
                let _ = tx.send(line.clone());
                log.lock().unwrap().push(line);
            }
        });
    }
    rx
}

fn command(args: &[&str], envs: &[(&str, &str)]) -> Command {
    command_wrapped(&[], args, envs)
}

/// `wrapper… cairnflow args…` (e.g. `unshare --pid --fork` so that the node is PID 1).
fn command_wrapped(wrapper: &[&str], args: &[&str], envs: &[(&str, &str)]) -> Command {
    let bin = env!("CARGO_BIN_EXE_cairnflow");
    let mut c = match wrapper.split_first() {
        Some((prog, rest)) => {
            let mut c = Command::new(prog);
            c.args(rest).arg(bin);
            c
        }
        None => Command::new(bin),
    };
    c.args(args)
        // Do not let the environment of whoever runs the tests leak in: a
        // CAIRNFLOW_ADMIN_KEY left in the shell changes the meaning of the whole test.
        .env_remove("CAIRNFLOW_ADMIN_KEY")
        .env_remove("CAIRNFLOW_ADMIN_KEY_SHA256")
        .env_remove("CAIRNFLOW_PROVISION_KEY_SHA256")
        .env_remove("CAIRNFLOW_CLUSTER_SECRET")
        .env_remove("CAIRNFLOW_IDENTITY_MASTER")
        .env_remove("CAIRNFLOW_PROFILE")
        .envs(envs.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

impl Binary {
    /// Run and wait until it has printed both ports.
    pub fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Result<Self> {
        Self::spawn_wrapped(&[], args, envs)
    }

    /// Like [`Self::spawn`], run through `wrapper` (program + flags, `cairnflow`
    /// appended).
    pub fn spawn_wrapped(wrapper: &[&str], args: &[&str], envs: &[(&str, &str)]) -> Result<Self> {
        let mut child = command_wrapped(wrapper, args, envs)
            .spawn()
            .context("could not run the cairnflow binary")?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let rx = pump(&mut child, &log);
        let deadline = Instant::now() + Duration::from_secs(30);
        let (mut url, mut internal_url) = (None, None);
        while url.is_none() || internal_url.is_none() {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok(line) => {
                    if let Some(u) = line.strip_prefix(INGRESS_LINE) {
                        url = Some(u.trim().to_string());
                    }
                    if let Some(u) = line.strip_prefix(INTERNAL_LINE) {
                        internal_url = Some(u.trim().to_string());
                    }
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    bail!(
                        "cairnflow {args:?} did not print both ports:\n{}",
                        log.lock().unwrap().join("\n")
                    );
                }
            }
        }
        Ok(Self {
            child,
            url: url.expect("loop above"),
            internal_url: internal_url.expect("loop above"),
            log,
        })
    }

    /// Run and wait for it to **exit on its own** (at most `limit`): exit code + all output.
    pub fn run_to_exit(
        args: &[&str],
        envs: &[(&str, &str)],
        limit: Duration,
    ) -> Result<(std::process::ExitStatus, String)> {
        let mut child = command(args, envs).spawn()?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let _rx = pump(&mut child, &log);
        let t0 = Instant::now();
        loop {
            if let Some(st) = child.try_wait()? {
                // Let the pipe reader threads catch the last line.
                std::thread::sleep(Duration::from_millis(50));
                return Ok((st, log.lock().unwrap().join("\n")));
            }
            if t0.elapsed() > limit {
                let _ = child.kill();
                let _ = child.wait();
                bail!(
                    "cairnflow {args:?} did not exit on its own after {limit:?}:\n{}",
                    log.lock().unwrap().join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn dump(&self) -> String {
        self.log.lock().unwrap().join("\n")
    }

    /// The process's pid (to measure RSS from outside, `ps`).
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Whether the process is still running (not exited, not killed).
    pub fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for Binary {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
