# Runbook — machine RAM and disk

**Alerts:** `CairnflowMemoryHigh`, `CairnflowDiskLow`.

## Mechanism

Code: `crates/cairn-server/src/host.rs` (sampled when `/metrics` is scraped).

- `cairnflow_process_resident_bytes`: the RSS of the node process. `cairnflow_memory_limit_bytes`:
  the cgroup limit when there is one (a container), otherwise the machine's RAM. Out of RAM, the
  operating system kills the node (and the platform restarts the machine): every tenant on the node
  loses its running turns, and acknowledged work runs again once the node is up.
- Most RAM under load is the journal of open turns: about 3–5 times the journal size per slot. Caps:
  `--replay-ram-budget-bytes` (default 512 MiB, each tenant ≤ ¼), the slot count
  `--max-concurrent-turns`, and a tenant's journal byte cap `max-journal-bytes=`.
- `cairnflow_disk_free_bytes{dir}`/`cairnflow_disk_total_bytes{dir}`: the disk holding the data
  directory (`data`, a `file://` store) and the SST disk cache (`disk_cache`), when the node uses
  them. A node on S3 without a disk cache has no such line. A full disk: a `file://` store fails its
  writes (the node fail-stops), the disk cache stops taking new entries.

## Diagnosis

```sh
# RAM and disk ratios (PromQL)
cairnflow_process_resident_bytes / cairnflow_memory_limit_bytes
cairnflow_disk_free_bytes / cairnflow_disk_total_bytes
```

- RAM grows with `cairnflow_replay_budget_bytes` and `cairnflow_rr_journal_bytes`: open turns carry
  large journals. `/query`: `SELECT id, tenant, journal_len FROM sys_invocation ORDER BY journal_len DESC LIMIT 20`
  (cluster-admin key) finds invocations with long journals.
- RAM grows while turns do not: `cairnflow_stream_ram_bytes` (temporary streams),
  `cairnflow_artifact_cache_bytes`.
- The `disk_cache` disk is nearly full while `cairnflow_disk_cache_bytes` is small: something else
  writes to the same disk (logs).

## Handling

1. RAM: lower `--max-concurrent-turns` or `--replay-ram-budget-bytes`; lower `max-journal-bytes=` of
   the tenant taking the room; move to a bigger machine. A restart only relieves it for a while.
2. Disk: lower `--disk-cache-bytes` (the cache evicts down below the cap), clean up logs, grow the
   disk. A full `file://` store holds customer data: grow the disk first, do not delete anything in
   the data directory.
