// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl` — checks and repairs the cluster's bucket **without a running node**.
//! It reads the bucket directly with `--main-store`/`--wal-store` like a node, so it
//! needs access to the bucket: a separate binary, not a subcommand of `cairn` (which
//! only needs an API key).
//!
//! ```sh
//! cairn-ctl --main-store s3://my-bucket/prod verify
//! cairn-ctl --main-store s3://my-bucket/prod table show
//! cairn-ctl --main-store s3://my-bucket/prod intent clear 4            # dry run
//! cairn-ctl --main-store s3://my-bucket/prod intent clear 4 --apply
//! cairn-ctl --main-store s3://my-bucket/prod trash restore --range 7 --apply
//! ```
//!
//! The logic lives in `cairn_server::ctl`; this only parses arguments and prints.

use {
    cairn_server::ctl::{Ctl, CtlError, RepairOptions, RepairReport, TrashTarget, VerifyOptions},
    cairn_store::{ObjectStores, StoreLocation},
    serde::Serialize,
};

fn usage() -> ! {
    eprintln!(
        "cairn-ctl (--data-dir DIR | --main-store URI [--wal-store URI]) [--prefix P] [--json] <command>\n\
         \n\
         Read-only:\n\
         \x20 table show                          range table: version, key span, gen, intent\n\
         \x20 owners                              owner record of every range + session state\n\
         \x20 ranges retired                      tombstoned ranges, whether the Db remains, who still holds it\n\
         \x20 range dump <id> [--table T] [--limit N]\n\
         \x20                                     raw rows of a range Db (default 100 rows)\n\
         \x20 wal floors                          shared WAL logs: GC floor, range pinning the floor\n\
         \x20 verify [--no-open] [--wal-logs] [--retired-after-s N] [--no-restore-checks] [--deep]\n\
         \x20                                     check bucket invariants; exit code 1 on findings.\n\
         \x20                                     By default also checks lost cross-range messages (j: a call\n\
         \x20                                     waiting on a missing callee / a reply that is nowhere);\n\
         \x20                                     --no-restore-checks turns it off.\n\
         \x20                                     --wal-logs: also shared WAL logs nobody holds (g);\n\
         \x20                                     --retired-after-s: also retired Dbs D-d should have deleted (h);\n\
         \x20                                     --deep: also every SST the manifest and live checkpoints of a\n\
         \x20                                     range name is on the bucket (sst_missing; one LIST per Db)\n\
         \x20 trash list                          trash entries: id, what was deleted, reason, age, size\n\
         \x20 cluster status                      cluster-version, quiet-renewal gate, call gate, every session\n\
         \x20                                     (knows, mode, challenge)\n\
         \n\
         Repair (DRY RUN by default — prints the change, writes nothing; only --apply writes):\n\
         \x20 intent clear <range> [--expect-version N] [--apply]\n\
         \x20                                     roll back an uncommitted split/merge of the range\n\
         \x20 range force-retire <id> [--apply]   write a tombstone for a range that left the table\n\
         \x20 table restore [--force] [--apply]  rebuild a lost cluster.conf from the newest cluster-history/<version>,\n\
         \x20                                     only once the rest of the bucket proves it is the last table (no\n\
         \x20                                     owner, ranges/ or ops/ id >= its next_range_id; every live range has\n\
         \x20                                     an owner record without a tombstone; ledgers agree); written with\n\
         \x20                                     Create. --apply refuses while a session has not ended (every node\n\
         \x20                                     must be stopped); --force once you are sure they are\n\
         \x20 trash restore (<id> | --range <r>) [--force] [--apply]\n\
         \x20                                     copy a trash entry's objects back to where they were (no overwrite);\n\
         \x20                                     --range: the range's Db and owner record, owner record first\n\
         \x20 cluster enable-quiet-lease [--delegation [--accept-missing-evidence]] [--apply]\n\
         \x20                                     enable quiet renewal: tests the store itself (conditional GET does\n\
         \x20                                     not return a stale 304, control/probe/), checks every session knows\n\
         \x20                                     the gate step, sets min_binary (old binaries stop starting), writes\n\
         \x20                                     control/quiet-lease; --delegation also enables renewal delegation\n\
         \x20                                     (step 3) — refused until all evidence items (a)–(e) of the 3.9\n\
         \x20                                     done-gate are recorded; --accept-missing-evidence enables anyway\n\
         \x20                                     (staging/chaos/test, NOT for production) and records the missing\n\
         \x20                                     items in the object\n\
         \x20 cluster record-evidence <a|b|c|d|e> --note \"…\" [--apply]\n\
         \x20                                     record one done-gate evidence item of delegation (3.9) in\n\
         \x20                                     control/quiet-lease: (a) wider model, (b) release tripwire,\n\
         \x20                                     (c) ≥ 30 min chaos killing the coordinator, (d) 24 h soak,\n\
         \x20                                     (e) contract 5.6 on the target store; enables nothing\n\
         \x20 cluster disable-quiet-lease [--apply]\n\
         \x20                                     disable the gate; nodes go back to direct on the next gate read\n\
         \x20 cluster lower-min-binary [--apply]  lower min_binary to version — after the gate is off and every session\n\
         \x20                                     is back to direct (downgrade runbook); with the call gate on, not\n\
         \x20                                     below that gate step (4 or 5)\n\
         \x20 cluster enable-call-prefix [--apply]\n\
         \x20                                     enable the call gates (one-way): group call trees by pk prefix\n\
         \x20                                     (step 4) and placement spreading: \"spread\" (step 5); checks every\n\
         \x20                                     session knows step 5, sets min_binary=5, writes call_prefix=on,\n\
         \x20                                     call_placement=on to cluster-version; ranges pick up the gate at\n\
         \x20                                     their next open\n\
         \x20 cluster enable-streams [--apply]    enable the durable stream gate (one-way, step 6): checks every session\n\
         \x20                                     knows step 6, sets min_binary=6, writes streams=on; ranges write\n\
         \x20                                     Streams rows ($log, $trace) from their next open\n\
         \x20 cluster enable-schedules [--apply]  enable the server-side schedule gate (one-way, step 7): checks every\n\
         \x20                                     session knows step 7, sets min_binary=7, writes schedules=on;\n\
         \x20                                     ranges fire schedules and the API accepts PUT /schedules from their next open\n\
         \x20 cluster enable-key-roles [--apply]  enable the key registry /2 gate (one-way, step 8):\n\
         \x20                                     checks every session knows step 8, sets min_binary=8, writes\n\
         \x20                                     key_roles=on; view/provision keys can be created from then on\n\
         \x20 cluster enable-tenant-state [--apply]\n\
         \x20                                     enable the tenant state gate (one-way, step 9):\n\
         \x20                                     checks every session knows step 9, sets min_binary=9, writes\n\
         \x20                                     tenant_state=on; from then on PUT /admin/tenants/{{t}}/state is accepted\n\
         \x20 cluster enable-audit [--apply]       enable the in-range audit gate (one-way, step 10):\n\
         \x20                                     checks every session knows step 10, sets min_binary=10, writes\n\
         \x20                                     audit=on; ranges write audit rows from their next open\n\
         \x20 cluster enable-tenant-purge [--apply]\n\
         \x20                                     enable the tenant purge gate (one-way, step 11):\n\
         \x20                                     checks every session knows step 11, sets min_binary=11, writes\n\
         \x20                                     tenant_purge=on; from then on PUT …/state accepts \"deleting\"\n\
         \x20 cluster enable-values [--apply]      enable the segmented values gate (one-way, step 12):\n\
         \x20                                     checks every session knows step 12, sets\n\
         \x20                                     min_binary=12, writes values=on; ranges seal large values from\n\
         \x20                                     their next open\n\
         \x20 keys create <tenant> <role> [--apply]\n\
         \x20                                     create an API key directly in the auth/keys registry (role: invoke | view |\n\
         \x20                                     admin | provision | cluster-admin; view/provision need\n\
         \x20                                     enable-key-roles) — bootstraps the provision key for the control\n\
         \x20                                     plane; the secret is printed ONCE and never goes into the audit record\n\
         \x20 pricing [list]                      token price table (micro-USD per 1 M input/output tokens)\n\
         \x20 pricing set <model> <input> <output> [--apply]\n\
         \x20                                     set the price of a model (0 0 = remove); nodes load it at the metering flush tick\n\
         \x20 identity publish|activate|retire <tenant> [--apply]\n\
         \x20                                     rotate the tenant's request identity key:\n\
         \x20                                     publish announces g+1 (still signs with g) → customer adds the new kid →\n\
         \x20                                     activate signs with g+1 → retire removes the old key\n\
         \x20 fork --to <prefix>                  copy of the cluster under another prefix (writes a checkpoint and a 24 h\n\
         \x20                                     GC fence `fork-<prefix>` on the source)\n\
         \x20   [--artifacts live|all|none]       artifacts copied along (default live: except directories held in the trash)\n\
         \n\
         Self-contained backup — target store from a different provider than the source:\n\
         \x20 backup --to <uri>                   checkpoint every range Db (temporary GC fence), copy every object they\n\
         \x20                                     reference and every cluster object to <uri>/<id>/data, manifest\n\
         \x20                                     <uri>/<id>/BACKUP written last; only the part of each range already in L0\n\
         \x20 backup list --to <uri>              backups at the target: id, complete or partial, object count, bytes\n\
         \x20 backup prune --to <uri> [--keep-daily 7] [--keep-weekly 4] [--apply]\n\
         \x20                                     delete backups outside the retention schedule (and partial ones > 24 h);\n\
         \x20                                     writes audit/ctl on the cluster\n\
         \x20 restore --from <uri> [--id ID]      copy a backup (latest complete, or ID) to an empty --main-store, checking\n\
         \x20                                     size + hash of every object; --main-store must have the source's prefix\n\
         \x20 backup check-restore                on a freshly restored --main-store, BEFORE starting nodes: verify opens\n\
         \x20                                     every Db and looks for cross-range calls that lost a message\n\
         \x20                                     (restore.lost_call/lost_reply) because the backup captured ranges at\n\
         \x20                                     different points; exit code 1 if any\n\
         \x20 --backup-env-prefix P               the backup store (--to of backup/list/prune, --from of restore)\n\
         \x20                                     takes credentials/endpoint/region ONLY from P+AWS_* variables (e.g.\n\
         \x20                                     CAIRN_BACKUP_AWS_ACCESS_KEY_ID, …_SECRET_ACCESS_KEY, …_ENDPOINT,\n\
         \x20                                     …_REGION=auto); a missing key is an error, the source's AWS_* is not borrowed.\n\
         \x20                                     Absent: the same AWS_* as --main-store\n\
         \n\
         --report-to-bucket ID\n\
         \x20            write the result (JSON as with --json, exit code, error; no secrets) to the object\n\
         \x20            ops-reports/<ID> of the cluster (--main-store) — read with a provision key via\n\
         \x20            GET /admin/ops-reports/<ID>. Only for backup, backup list,\n\
         \x20            backup prune, restore, cluster status, cluster enable-*, table restore. ID: 1–128 characters\n\
         \x20            A-Z a-z 0-9 - _ ., new each run (already exists ⇒ refused, code 3, command not run);\n\
         \x20            written even when the command fails; keeps the newest 256 reports, ≤ 30 days\n\
         --who NAME   who is running it, written to audit/ctl/<ts> (default $USER@$HOST)\n\
         --json       print JSON\n\
         --version    version, data=<format>, knows=<migration step count> — use the same knows= as the cluster\n\
         \n\
         Exit codes: 0 done/clean, 1 verify found problems, 2 bad arguments, 3 refused, 4 store error."
    );
    std::process::exit(2);
}

struct Args {
    main_store: Option<String>,
    wal_store: Option<String>,
    prefix: Option<String>,
    json: bool,
    apply: bool,
    force: bool,
    no_open: bool,
    /// `verify --no-restore-checks`: skip check (j), lost inter-range messages.
    no_restore_checks: bool,
    /// `verify --deep`: the `sst_missing` check.
    deep: bool,
    /// `cluster enable-quiet-lease --delegation`.
    delegation: bool,
    /// `--accept-missing-evidence`: enable delegation when evidence is missing.
    accept_missing_evidence: bool,
    /// `cluster record-evidence … --note`.
    note: Option<String>,
    wal_logs: bool,
    retired_after_s: Option<u64>,
    who: Option<String>,
    table: Option<String>,
    limit: usize,
    expect_version: Option<u64>,
    to: Option<String>,
    /// `fork --artifacts=live|all|none`.
    artifacts: Option<String>,
    /// `restore --from <uri>`, `--id`.
    from: Option<String>,
    id: Option<String>,
    /// `backup prune --keep-daily/--keep-weekly`.
    keep_daily: usize,
    keep_weekly: usize,
    /// `--backup-env-prefix P`: the backup store reads `P` + `AWS_*`.
    backup_env_prefix: Option<String>,
    /// `--report-to-bucket <id>`: the report goes into `ops-reports/<id>`.
    report_to_bucket: Option<String>,
    range: Option<u32>,
    rest: Vec<String>,
}

/// `--data-dir DIR` ⇒ `file://<absolute DIR>`, like a node.
fn data_dir_uri(dir: &str) -> String {
    let p = std::path::Path::new(dir);
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(p)
    };
    format!("file://{}", abs.display())
}

fn parse(argv: &[String]) -> Args {
    let mut a = Args {
        // The same environment variables as a node.
        main_store: std::env::var("CAIRNFLOW_MAIN_STORE").ok().or_else(|| {
            std::env::var("CAIRNFLOW_DATA_DIR")
                .ok()
                .map(|d| data_dir_uri(&d))
        }),
        wal_store: std::env::var("CAIRNFLOW_WAL_STORE").ok(),
        prefix: None,
        json: false,
        apply: false,
        force: false,
        no_open: false,
        no_restore_checks: false,
        deep: false,
        delegation: false,
        accept_missing_evidence: false,
        note: None,
        wal_logs: false,
        retired_after_s: None,
        who: None,
        table: None,
        limit: 100,
        expect_version: None,
        to: None,
        artifacts: None,
        from: None,
        id: None,
        keep_daily: 7,
        keep_weekly: 4,
        backup_env_prefix: None,
        report_to_bucket: None,
        range: None,
        rest: Vec::new(),
    };
    let mut i = 0;
    let take = |i: &mut usize| -> String {
        *i += 1;
        argv.get(*i).cloned().unwrap_or_else(|| usage())
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "--data-dir" => a.main_store = Some(data_dir_uri(&take(&mut i))),
            "--main-store" => a.main_store = Some(take(&mut i)),
            "--wal-store" => a.wal_store = Some(take(&mut i)),
            "--prefix" => a.prefix = Some(take(&mut i)),
            "--json" => a.json = true,
            "--apply" => a.apply = true,
            "--force" => a.force = true,
            "--no-open" => a.no_open = true,
            "--no-restore-checks" => a.no_restore_checks = true,
            "--deep" => a.deep = true,
            "--delegation" => a.delegation = true,
            "--accept-missing-evidence" => a.accept_missing_evidence = true,
            "--note" => a.note = Some(take(&mut i)),
            "--wal-logs" => a.wal_logs = true,
            "--retired-after-s" => {
                a.retired_after_s = Some(take(&mut i).parse().unwrap_or_else(|_| usage()))
            }
            "--who" => a.who = Some(take(&mut i)),
            "--table" => a.table = Some(take(&mut i)),
            "--limit" => a.limit = take(&mut i).parse().unwrap_or_else(|_| usage()),
            "--expect-version" => {
                a.expect_version = Some(take(&mut i).parse().unwrap_or_else(|_| usage()))
            }
            "--to" => a.to = Some(take(&mut i)),
            "--from" => a.from = Some(take(&mut i)),
            "--id" => a.id = Some(take(&mut i)),
            "--keep-daily" => a.keep_daily = take(&mut i).parse().unwrap_or_else(|_| usage()),
            "--keep-weekly" => a.keep_weekly = take(&mut i).parse().unwrap_or_else(|_| usage()),
            "--backup-env-prefix" => a.backup_env_prefix = Some(take(&mut i)),
            "--report-to-bucket" => a.report_to_bucket = Some(take(&mut i)),
            "--artifacts" => a.artifacts = Some(take(&mut i)),
            s if s.starts_with("--artifacts=") => {
                a.artifacts = Some(s["--artifacts=".len()..].to_string())
            }
            "--range" => a.range = Some(take(&mut i).parse().unwrap_or_else(|_| usage())),
            "-h" | "--help" => usage(),
            "-V" | "--version" => {
                println!("{}", cairn_server::version_line("cairn-ctl"));
                std::process::exit(0);
            }
            s if s.starts_with("--") => {
                eprintln!("unknown argument: {s}");
                usage()
            }
            s => a.rest.push(s.to_string()),
        }
        i += 1;
    }
    a
}

fn stores(a: &Args) -> Result<ObjectStores, CtlError> {
    let uri = a
        .main_store
        .as_deref()
        .ok_or_else(|| CtlError::Bad("missing --main-store or --data-dir".into()))?;
    let main = StoreLocation::parse(uri)?;
    let s = match &a.wal_store {
        Some(w) => ObjectStores::split(main, StoreLocation::parse(w)?)?,
        None => ObjectStores::single(main),
    };
    Ok(match &a.prefix {
        Some(p) => s.at_prefix(p)?,
        None => s,
    })
}

fn who(a: &Args) -> String {
    a.who.clone().unwrap_or_else(|| {
        let user = std::env::var("USER").unwrap_or_else(|_| "?".into());
        let host = std::env::var("HOST")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_else(|_| "?".into());
        format!("{user}@{host}")
    })
}

fn print_json<T: Serialize>(v: &T) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

fn id_arg(a: &Args, at: usize) -> u32 {
    a.rest
        .get(at)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| usage())
}

fn print_repair(json: bool, r: &RepairReport) {
    if json {
        return print_json(r);
    }
    println!(
        "{} {}",
        if r.applied {
            "APPLIED"
        } else {
            "DRY RUN — nothing written; add --apply to write"
        },
        r.object
    );
    println!("── before\n{}", r.before.trim_end());
    println!("── after\n{}", r.after.trim_end());
    for c in &r.cleanup {
        println!("cleanup: {c}");
    }
    for w in &r.warnings {
        println!("warning: {w}");
    }
    if let Some(a) = &r.audit {
        println!("audit: {a}");
    }
}

/// Runs the command. `result` receives the JSON of the result (as `--json` prints it) for
/// commands accepting `--report-to-bucket` ([`cairn_server::ctl::REPORTABLE`]).
async fn run(a: &Args, result: &mut Option<serde_json::Value>) -> Result<i32, CtlError> {
    let ctl = Ctl::new(stores(a)?);
    let mut keep = |v: serde_json::Result<serde_json::Value>| *result = v.ok();
    let cmd: Vec<&str> = a.rest.iter().map(String::as_str).collect();
    // A separate credential set only makes sense for the backup store; another command
    // with the flag means the caller thinks it applies somewhere — say so, do not
    // silently drop it.
    if a.backup_env_prefix.is_some()
        && !matches!(
            cmd.as_slice(),
            ["backup"] | ["backup", "list"] | ["backup", "prune"] | ["restore"]
        )
    {
        return Err(CtlError::Bad(
            "--backup-env-prefix only works with backup, backup list, backup prune, restore".into(),
        ));
    }
    let backup_store = |uri: Option<&str>| {
        cairn_server::ctl::location(
            uri.unwrap_or_else(|| usage()),
            a.backup_env_prefix.as_deref(),
        )
    };
    let command = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let repair = RepairOptions {
        apply: a.apply,
        force: a.force,
        who: who(a),
        command: command.clone(),
        expect_version: a.expect_version,
        now_ms: cairn_store::entropy::now_ms(),
    };
    match cmd.as_slice() {
        ["table", "show"] | ["table"] => {
            let t = ctl.table_show().await?;
            if a.json {
                print_json(&t);
            } else {
                println!(
                    "table_version={} next_range_id={}",
                    t.version, t.next_range_id
                );
                for r in &t.ranges {
                    println!(
                        "range {:>5}  {}-{}  gen {}{}",
                        r.id,
                        r.start,
                        r.end,
                        r.gen,
                        r.intent
                            .as_ref()
                            .map(|i| format!("  {i}"))
                            .unwrap_or_default()
                    );
                }
                for x in &t.extra {
                    println!("(unknown line) {x}");
                }
            }
        }
        ["owners"] => {
            let v = ctl.owners().await?;
            if a.json {
                print_json(&v);
            } else {
                for o in &v {
                    if let Some(e) = &o.error {
                        println!("range {:>5}  CORRUPT: {e}", o.range);
                        continue;
                    }
                    println!(
                        "range {:>5}  {}  term {}  {} ({})  {}{}{}  segments {}",
                        o.range,
                        if o.in_table { "table" } else { "    " },
                        o.term.unwrap_or(0),
                        o.session.as_deref().unwrap_or("-"),
                        o.session_state.as_deref().unwrap_or("-"),
                        o.wal_kind.as_deref().unwrap_or("-"),
                        if o.released { " released" } else { "" },
                        if o.retired { " retired" } else { "" },
                        o.wal_segments.len()
                    );
                }
            }
        }
        ["ranges", "retired"] => {
            let v = ctl.retired().await?;
            if a.json {
                print_json(&v);
            } else {
                for r in &v {
                    println!(
                        "range {:>5}  since {} ms  Db {}  {}",
                        r.range,
                        r.since_ms,
                        if r.db_exists { "present" } else { "deleted" },
                        r.referenced_by.as_deref().unwrap_or("")
                    );
                }
            }
        }
        ["range", "dump", _] => {
            let rows = ctl
                .range_dump(id_arg(a, 2), a.table.as_deref(), a.limit)
                .await?;
            if a.json {
                print_json(&rows);
            } else {
                for r in &rows {
                    println!(
                        "{}  {}  pk {}  {} B  {}",
                        r.key,
                        r.table.as_deref().unwrap_or("-"),
                        r.owner_pk.as_deref().unwrap_or("-"),
                        r.value_len,
                        r.value
                    );
                }
            }
        }
        ["wal", "floors"] => {
            let v = ctl.wal_floors().await?;
            if a.json {
                print_json(&v);
            } else {
                for l in &v {
                    println!(
                        "{}  ({})  floor {}  {} object{}{}",
                        l.log,
                        l.session_state.as_deref().unwrap_or("no session"),
                        l.floor,
                        l.objects,
                        l.sealed_at
                            .map(|s| format!("  seal@{s}"))
                            .unwrap_or_default(),
                        l.pinned_by_range
                            .map(|r| format!(
                                "  pinned by range {r} (wal id {})",
                                l.pinned_wal_id.unwrap_or(0)
                            ))
                            .unwrap_or_default()
                    );
                }
            }
        }
        ["verify"] | ["backup", "check-restore"] => {
            // `backup check-restore`: on a freshly restored bucket, **before** starting
            // nodes — every Db opens and no inter-range call lost its message because
            // the copy captured the ranges at different points.
            let restoring = cmd.as_slice() == ["backup", "check-restore"];
            let rep = ctl
                .verify(VerifyOptions {
                    open_dbs: restoring || !a.no_open,
                    wal_logs: a.wal_logs,
                    retired_after: a.retired_after_s.map(std::time::Duration::from_secs),
                    restore_checks: restoring || !a.no_restore_checks,
                    deep: a.deep,
                    ..VerifyOptions::default()
                })
                .await?;
            if a.json {
                print_json(&rep);
            } else {
                for f in &rep.notes {
                    match f.range {
                        Some(r) => println!("[info {}] range {r}: {}", f.check, f.detail),
                        None => println!("[info {}] {}", f.check, f.detail),
                    }
                }
                for f in &rep.findings {
                    let heals = f
                        .heals
                        .map(|h| format!(" (healed by: {h})"))
                        .unwrap_or_default();
                    match f.range {
                        Some(r) => println!("[{}] range {r}: {}{heals}", f.check, f.detail),
                        None => println!("[{}] {}{heals}", f.check, f.detail),
                    }
                }
                println!(
                    "{}: {} ranges, {} external_dbs references, {} ranges/ directories, {} \
                     findings",
                    if rep.clean() { "CLEAN" } else { "FINDINGS" },
                    rep.ranges_checked,
                    rep.external_refs_checked,
                    rep.range_dirs,
                    rep.findings.len()
                );
            }
            return Ok(if rep.clean() { 0 } else { 1 });
        }
        ["intent", "clear", _] => {
            print_repair(a.json, &ctl.intent_clear(id_arg(a, 2), &repair).await?)
        }
        ["range", "force-retire", _] => {
            print_repair(a.json, &ctl.force_retire(id_arg(a, 2), &repair).await?)
        }
        ["table", "restore"] => {
            let r = ctl.table_restore(&repair).await?;
            keep(serde_json::to_value(&r));
            if a.json {
                print_json(&r);
            } else {
                println!(
                    "{} cluster.conf from cluster-history/{} (history: {})",
                    if r.applied {
                        "APPLIED"
                    } else {
                        "DRY RUN — nothing written; add --apply to write"
                    },
                    r.version,
                    r.history
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                for c in &r.checked {
                    println!("proof: {c}");
                }
                println!("── table\n{}", r.table.trim_end());
                for w in &r.warnings {
                    println!("warning: {w}");
                }
                if let Some(a) = &r.audit {
                    println!("audit: {a}");
                }
            }
        }
        ["trash", "list"] | ["trash"] => {
            let v = ctl.trash_list(cairn_store::entropy::now_ms()).await?;
            if a.json {
                print_json(&v);
            } else {
                for e in &v {
                    println!(
                        "{}  [{}]  {}  {} object  {} B  {} h  {}{}",
                        e.id,
                        e.store,
                        e.subject.as_deref().unwrap_or("?"),
                        e.objects,
                        e.bytes,
                        e.age_ms / 3_600_000,
                        e.reason,
                        e.unreadable
                            .as_ref()
                            .map(|u| format!("  UNREADABLE: {u}"))
                            .unwrap_or_default()
                    );
                }
            }
        }
        ["trash", "restore", rest @ ..] => {
            let target = match (rest, a.range) {
                ([id], None) => TrashTarget::Entry((*id).to_string()),
                ([], Some(r)) => TrashTarget::Range(r),
                _ => usage(),
            };
            print_repair(a.json, &ctl.trash_restore(&target, &repair).await?)
        }
        ["cluster", "status"] => {
            let st = ctl.cluster_status(cairn_store::entropy::now_ms()).await?;
            keep(serde_json::to_value(&st));
            if a.json {
                print_json(&st);
            } else {
                println!(
                    "version={} min_binary={}{}  quiet-lease {} (gate step {})  call-prefix {} \
                     (gate step {})  call-placement {} (gate step {})  streams {} (gate step {})  \
                     schedules {} (gate step {})  key-roles {} (gate step {})  tenant-state {} \
                     (gate step {})  audit {} (gate step {})",
                    st.version,
                    st.min_binary,
                    st.upgrading
                        .as_ref()
                        .map(|u| format!("  upgrading={u}"))
                        .unwrap_or_default(),
                    if st.quiet_lease { "ON" } else { "off" },
                    st.quiet_lease_knows,
                    if st.call_prefix { "ON" } else { "off" },
                    st.call_prefix_knows,
                    if st.call_placement { "ON" } else { "off" },
                    st.call_placement_knows,
                    if st.streams { "ON" } else { "off" },
                    st.streams_knows,
                    if st.schedules { "ON" } else { "off" },
                    st.schedules_knows,
                    if st.key_roles { "ON" } else { "off" },
                    st.key_roles_knows,
                    if st.tenant_state { "ON" } else { "off" },
                    st.tenant_state_knows,
                    if st.audit { "ON" } else { "off" },
                    st.audit_knows
                );
                println!(
                    "renewal delegation {}  done-gate evidence: {}",
                    if st.delegation { "ON" } else { "off" },
                    if st.evidence_missing.is_empty() {
                        "complete (a)–(e)".to_string()
                    } else {
                        format!(
                            "missing {}",
                            st.evidence_missing
                                .iter()
                                .map(|k| format!("({k})"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    }
                );
                for e in &st.evidence {
                    match &e.note {
                        Some(n) => println!(
                            "  ({}) {}: {}  — {} at at_ms={}",
                            e.item,
                            e.title,
                            n,
                            e.by.as_deref().unwrap_or("?"),
                            e.at_ms.unwrap_or(0)
                        ),
                        None => println!("  ({}) {}: MISSING", e.item, e.title),
                    }
                }
                if let Some(u) = &st.delegation_unproven {
                    println!("  WARNING: {u}");
                }
                for s in &st.sessions {
                    println!(
                        "{}  {}  knows={}  mode={}{}  idle {} ms",
                        s.session,
                        s.state,
                        s.knows.map_or("-".into(), |k| k.to_string()),
                        s.mode,
                        s.challenged
                            .as_ref()
                            .map(|c| format!("  challenged={c}"))
                            .unwrap_or_default(),
                        s.idle_ms
                    );
                }
            }
        }
        ["cluster", "enable-quiet-lease"] => {
            if a.accept_missing_evidence && !a.delegation {
                return Err(CtlError::Bad(
                    "--accept-missing-evidence only goes with --delegation".into(),
                ));
            }
            let r = ctl
                .enable_quiet_lease_with(&repair, a.delegation, a.accept_missing_evidence)
                .await?;
            keep(serde_json::to_value(&r));
            print_repair(a.json, &r)
        }
        ["cluster", "record-evidence", item] => {
            let mut chars = item.chars();
            let k = match (chars.next(), chars.next()) {
                (Some(k), None) => k,
                _ => usage(),
            };
            let note = a.note.as_deref().unwrap_or_else(|| usage());
            print_repair(a.json, &ctl.record_evidence(k, note, &repair).await?)
        }
        ["cluster", "disable-quiet-lease"] => {
            print_repair(a.json, &ctl.disable_quiet_lease(&repair).await?)
        }
        ["cluster", "lower-min-binary"] => {
            print_repair(a.json, &ctl.lower_min_binary(&repair).await?)
        }
        ["cluster", gate @ ("enable-call-prefix"
        | "enable-streams"
        | "enable-schedules"
        | "enable-key-roles"
        | "enable-tenant-state"
        | "enable-audit"
        | "enable-tenant-purge"
        | "enable-values")] => {
            let r = match *gate {
                "enable-call-prefix" => ctl.enable_call_prefix(&repair).await?,
                "enable-streams" => ctl.enable_streams(&repair).await?,
                "enable-schedules" => ctl.enable_schedules(&repair).await?,
                "enable-tenant-state" => ctl.enable_tenant_state(&repair).await?,
                "enable-audit" => ctl.enable_audit(&repair).await?,
                "enable-tenant-purge" => ctl.enable_tenant_purge(&repair).await?,
                "enable-values" => ctl.enable_values(&repair).await?,
                _ => ctl.enable_key_roles(&repair).await?,
            };
            keep(serde_json::to_value(&r));
            print_repair(a.json, &r)
        }
        ["keys", "create", tenant, role] => {
            let made = ctl.create_key(tenant, role, &repair).await?;
            print_repair(a.json, &made.report);
            if made.report.applied {
                // The secret goes to stdout once, separate from the report (the report
                // goes into the audit record, the secret does not).
                if a.json {
                    print_json(&serde_json::json!({
                        "id": made.id,
                        "tenant": made.tenant,
                        "role": made.role,
                        "secret": made.secret,
                    }));
                } else {
                    println!("id={} tenant={} role={}", made.id, made.tenant, made.role);
                    println!("secret (shown only once): {}", made.secret);
                }
            }
        }
        ["pricing", "list"] | ["pricing"] => {
            let p = ctl.pricing_show().await?;
            if a.json {
                print_json(&p);
            } else {
                println!("version={} by={}", p.version, p.by);
                for (m, price) in &p.models {
                    println!(
                        "{m}\tinput {} µUSD/Mtok\toutput {} µUSD/Mtok",
                        price.input, price.output
                    );
                }
            }
        }
        ["pricing", "set", model, input, output] => {
            let num = |v: &str| {
                v.parse::<u64>()
                    .map_err(|_| CtlError::Bad(format!("not a number: {v:?}")))
            };
            print_repair(
                a.json,
                &ctl.pricing_set(model, num(input)?, num(output)?, &repair)
                    .await?,
            )
        }
        ["identity", step, tenant] => {
            let step = cairn_server::identity::IdentityStep::parse(step).unwrap_or_else(|| usage());
            print_repair(a.json, &ctl.identity(tenant, step, &repair).await?)
        }
        ["fork"] => {
            let to = a.to.as_deref().unwrap_or_else(|| usage());
            let mode = a.artifacts.as_deref().unwrap_or("live").parse()?;
            let rep = ctl.fork_with(to, &repair.who, &command, mode).await?;
            if a.json {
                print_json(&rep);
            } else {
                println!(
                    "copy at prefix {:?}: ranges {:?}, owner records {:?}",
                    rep.to, rep.ranges, rep.owners
                );
                println!("copied: {}", rep.copied.join(", "));
                if let Some(b) = &rep.barrier {
                    println!(
                        "GC fence on the source: gc-barriers/{} protects={} expires {} ms — extend \
                         with PUT /admin/gc/barriers/{} {{\"holder\": …, \"protects\": \"{}\", \
                         \"ttlMs\": 86400000}}, remove with {{\"ttlMs\": 0}} when deleting the \
                         copy",
                        b.id, b.protects, b.expires_ms, b.id, b.protects
                    );
                }
                for w in &rep.warnings {
                    println!("warning: {w}");
                }
                if let Some(a) = &rep.audit {
                    println!("audit (on the source): {a}");
                }
            }
        }
        ["backup"] => {
            let to = backup_store(a.to.as_deref())?;
            let rep = ctl.backup(&to, &repair.who, &command).await?;
            keep(serde_json::to_value(&rep));
            if a.json {
                print_json(&rep);
            } else {
                println!(
                    "backup {} at {}: {} objects, {} bytes, ranges {:?}",
                    rep.id, rep.to, rep.objects, rep.bytes, rep.ranges
                );
                for w in &rep.warnings {
                    println!("warning: {w}");
                }
                if let Some(a) = &rep.audit {
                    println!("audit (on the source): {a}");
                }
            }
        }
        ["backup", "list"] => {
            let to = backup_store(a.to.as_deref())?;
            let all = ctl.backup_list(&to).await?;
            keep(serde_json::to_value(&all));
            if a.json {
                print_json(&all);
            } else {
                for b in &all {
                    match (b.complete, b.objects, b.bytes) {
                        (true, Some(n), Some(bytes)) => {
                            println!("{}  complete  {n} objects  {bytes} bytes", b.id)
                        }
                        _ => println!("{}  PARTIAL (no manifest — unusable)", b.id),
                    }
                }
            }
        }
        ["backup", "prune"] => {
            let to = backup_store(a.to.as_deref())?;
            let rep = ctl
                .backup_prune(
                    &to,
                    a.keep_daily,
                    a.keep_weekly,
                    a.apply,
                    &repair.who,
                    &command,
                )
                .await?;
            keep(serde_json::to_value(&rep));
            if a.json {
                print_json(&rep);
            } else {
                println!(
                    "{} keep {:?}, delete {:?}",
                    if rep.applied {
                        "DELETED —"
                    } else {
                        "DRY RUN — nothing deleted; add --apply to delete —"
                    },
                    rep.keep,
                    rep.delete
                );
                if let Some(a) = &rep.audit {
                    println!("audit (on the cluster): {a}");
                }
            }
        }
        ["restore"] => {
            let from = backup_store(a.from.as_deref())?;
            let rep = ctl
                .restore(&from, a.id.as_deref(), &repair.who, &command)
                .await?;
            keep(serde_json::to_value(&rep));
            if a.json {
                print_json(&rep);
            } else {
                println!(
                    "copied backup {} from {}: {} objects, {} bytes — run `cairn-ctl verify`, then \
                     start the nodes (cluster secret and bootstrap key come from the secret store)",
                    rep.id, rep.from, rep.objects, rep.bytes
                );
            }
        }
        _ => usage(),
    }
    Ok(0)
}

/// Prints the error, returns the exit code.
fn exit_code(r: Result<i32, CtlError>) -> i32 {
    match r {
        Ok(c) => c,
        Err(e @ CtlError::Refused(_)) => {
            eprintln!("{e}");
            3
        }
        Err(e @ CtlError::Bad(_)) => {
            eprintln!("{e}");
            2
        }
        Err(e) => {
            eprintln!("store error: {e}");
            4
        }
    }
}

/// `--report-to-bucket <id>`: checks the command and the id, reserves the slot (the id
/// has no report yet) **before** running; runs; writes the report — even when the
/// command fails. A command that finished but whose report could not be written ⇒ a
/// non-zero exit code: whoever triggered it reads the report, not stdout.
async fn run_reported(a: &Args, id: &str) -> i32 {
    let cmd: Vec<&str> = a.rest.iter().map(String::as_str).collect();
    if !cairn_server::ctl::reportable(&cmd) {
        eprintln!(
            "--report-to-bucket only works with backup, backup list, backup prune, restore, \
             cluster status, cluster enable-*"
        );
        return 2;
    }
    let ctl = match stores(a) {
        Ok(s) => Ctl::new(s),
        Err(e) => return exit_code(Err(e)),
    };
    if let Err(e) = ctl.report_reserve(id).await {
        return exit_code(Err(e));
    }
    let started_ms = cairn_store::entropy::now_ms();
    let mut result = None;
    let r = run(a, &mut result).await;
    let error = r.as_ref().err().map(|e| match e {
        CtlError::Store(_) => format!("store error: {e}"),
        _ => e.to_string(),
    });
    let code = exit_code(r);
    let report = cairn_server::ctl::OpsReport::new(
        id,
        &cmd,
        a.apply,
        &who(a),
        started_ms,
        code,
        error,
        result.filter(|_| code == 0),
    );
    match ctl.report_write(&report).await {
        Ok(w) => {
            eprintln!("report: {}", w.path);
            if let Some(why) = &w.pruned.skipped {
                eprintln!("warning: old reports not pruned: {why}");
            }
            for (id, e) in &w.pruned.failed {
                eprintln!("warning: cannot delete old report {id}: {e}");
            }
            code
        }
        Err(e) => {
            eprintln!("cannot write report ops-reports/{id}:");
            match exit_code(Err(e)) {
                _ if code != 0 => code,
                c => c,
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let a = parse(&argv);
    let code = match a.report_to_bucket.clone() {
        Some(id) => run_reported(&a, &id).await,
        None => exit_code(run(&a, &mut None).await),
    };
    std::process::exit(code);
}
