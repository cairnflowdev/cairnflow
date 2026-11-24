// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! RAM and disk of the machine running the node: a small hosted cell is a
//! small machine (1 GB); running out of RAM makes the platform restart the
//! machine — every tenant dies together — and previously no metric showed it
//! approaching.
//!
//! Sampled when `/metrics` is scraped, no background loop. The RAM cap is the
//! cgroup limit when there is one (container), otherwise the machine's RAM (a
//! Firecracker VM, for instance). Disk: the data directory
//! (`--data-dir`/`file://`) and the SST disk-cache directory, when the node
//! uses them.

use std::path::{Path, PathBuf};

/// One sample.
#[derive(Clone, Debug, Default)]
pub struct HostStats {
    /// RSS of the node process, bytes. `None` = the platform does not expose it.
    pub rss_bytes: Option<u64>,
    /// RAM cap of the node (cgroup, otherwise the whole machine), bytes.
    pub memory_limit_bytes: Option<u64>,
    /// `(dir, bytes free for the process, bytes total)` per `dir` label.
    pub disks: Vec<(&'static str, u64, u64)>,
}

/// Samples RSS, the RAM cap, and the disk holding each directory in `dirs`
/// (`(label, path)`). Synchronous I/O (reads `/proc`, the mount list): the
/// caller runs it in `spawn_blocking`.
pub fn snapshot(dirs: &[(&'static str, PathBuf)]) -> HostStats {
    use sysinfo::{Disks, ProcessRefreshKind, ProcessesToUpdate, System};
    let mut sys = System::new();
    let pid = sysinfo::get_current_pid().ok();
    if let Some(pid) = pid {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory(),
        );
    }
    sys.refresh_memory();
    let rss_bytes = pid.and_then(|p| sys.process(p)).map(|p| p.memory());
    let total = sys.total_memory();
    let memory_limit_bytes = match sys.cgroup_limits() {
        // A cgroup without a limit reports the machine size; take the smaller.
        Some(c) if c.total_memory > 0 && (total == 0 || c.total_memory < total) => {
            Some(c.total_memory)
        }
        _ => (total > 0).then_some(total),
    };
    let mut disks = Vec::new();
    if !dirs.is_empty() {
        let list = Disks::new_with_refreshed_list();
        for (label, dir) in dirs {
            if let Some((free, all)) = disk_of(&list, dir) {
                disks.push((*label, free, all));
            }
        }
    }
    HostStats {
        rss_bytes,
        memory_limit_bytes,
        disks,
    }
}

/// The disk holding `dir`: the longest mount point that prefixes the absolute path.
fn disk_of(list: &sysinfo::Disks, dir: &Path) -> Option<(u64, u64)> {
    let abs = std::fs::canonicalize(dir).ok()?;
    list.list()
        .iter()
        .filter(|d| abs.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| (d.available_space(), d.total_space()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On the test machine: RSS and the RAM cap are readable, the disk of the
    /// temp directory has total > 0 and free ≤ total.
    #[test]
    fn reads_this_process_and_the_temp_disk() {
        let tmp = std::env::temp_dir();
        let s = snapshot(&[("data", tmp)]);
        let rss = s.rss_bytes.expect("process RSS");
        let limit = s.memory_limit_bytes.expect("RAM cap");
        assert!(rss > 0 && rss < limit, "rss {rss}, cap {limit}");
        let [(label, free, total)] = s.disks[..] else {
            panic!("disk of the temp directory: {:?}", s.disks);
        };
        assert_eq!(label, "data");
        assert!(total > 0 && free <= total, "{free}/{total}");
    }
}
