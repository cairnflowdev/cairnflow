#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Mutation testing of the safety checks.

Each mutant breaks **one** safety check — a fence, a version re-check, the
delete-proof gate, a `verify` check — with a single string replacement at exactly
one site, then runs the tests expected to catch it. A mutant is **killed** when at
least one test fails; **survived** means no test sees that check — a coverage hole,
recorded in the report and in the defect log.

    scripts/mutants.py               # every mutant
    scripts/mutants.py nw7 c2        # only these ids
    scripts/mutants.py --list        # list them

The file is restored after every mutant (even on interrupt). Report:
`target/mutants/report.md`. Exits 1 when a mutant survives.

Runs like the WIP row of AGENTS.md §2: plain `cargo test`, each mutant rebuilds
the changed crate. Set `CARGO_INCREMENTAL=0` on a machine short on disk.
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TIMEOUT_S = int(os.environ.get("MUTANT_TIMEOUT_S", "1800"))


@dataclass
class Mutant:
    id: str
    what: str
    file: str
    old: str
    new: str
    cmds: list
    # An **equivalent** mutant: no reachable path makes it differ from the original
    # code (reason recorded here). If it survives it is reported as "equivalent", not
    # as a coverage hole; if it is killed the reason is wrong — look again.
    equivalent: str | None = None


SERVER_FP = "cargo test -p cairn-server --features failpoints --test failpoints"

MUTANTS = [
    Mutant(
        "dd-recheck-version",
        "D-d skips re-checking the table version right before deleting (SM-9)",
        "crates/cairn-server/src/gc.rs",
        "if cluster.version() != round.plan.table_version {",
        "if false && cluster.version() != round.plan.table_version {",
        ["cargo test -p cairn-server --test gc --test split", SERVER_FP],
    ),
    Mutant(
        "orphan-recheck-version",
        "the orphan child cleanup round skips re-checking the table version (SM-14 (3))",
        "crates/cairn-server/src/gc.rs",
        "    cluster.reload().await?;\n    if cluster.version() != table.version() {",
        "    cluster.reload().await?;\n    if false && cluster.version() != table.version() {",
        ["cargo test -p cairn-server --test gc --test split", SERVER_FP],
    ),
    Mutant(
        "refs-unknown-keeps",
        "a reference set that failed to read is treated as referencing nothing (rule 5, D-d)",
        "crates/cairn-store/src/config/range_db.rs",
        "        if let Some(why) = &self.unknown {\n            return Some(why.clone());",
        "        if let Some(why) = None::<&String> {\n            return Some(why.clone());",
        ["cargo test -p cairn-store --test range_clone", "cargo test -p cairn-server --test gc"],
    ),
    Mutant(
        "nw7-durable-mark",
        "shared WAL floor GC trusts the in-memory mark of an open Db (NW-7)",
        "crates/cairn-store/src/nodewal/gc.rs",
        "in_memory.min(stored)",
        "in_memory",
        ["cargo test -p cairn-store --test shared_wal --test shared_wal_sim"],
    ),
    Mutant(
        "c2-reread-floor",
        "sealing does not reread the floor: a seal on a name GC just deleted still counts (NW-3, C2)",
        "crates/cairn-store/src/nodewal/mod.rs",
        "if retry_io(log, || store.floor()).await? > at {",
        "if false && retry_io(log, || store.floor()).await? > at {",
        ["cargo test -p cairn-store --test shared_wal --test shared_wal_sim"],
    ),
    Mutant(
        "c7-prune-durable",
        "range adoption drops every old segment without the stored mark covering it (NW-4, C7)",
        "crates/cairn-store/src/nodewal/acquire.rs",
        "keeper.prune_wal_segments(mark, covered)",
        "keeper.prune_wal_segments(mark, u64::MAX)",
        ["cargo test -p cairn-store --test shared_wal --test shared_wal_sim"],
        equivalent=(
            "`flush_through` returns only once the stored manifest covers `max_seen` (G2, "
            "`library_contracts.rs::g2_memtable_flush_returns_after_the_manifest_is_stored`), "
            "so the stored mark ≥ `max_seen` and `covered` ≥ first id − 1: it prunes like `u64::MAX`. "
            "The check is a safety net for when G2 breaks — the G2 contract test guards that"
        ),
    ),
    Mutant(
        "ack-needs-grant",
        "a commit acks after the grant has expired (LS-5)",
        "crates/cairn-store/src/writer/mod.rs",
        "    if !validity.is_valid() {\n        return Err(StoreError::LeaseLost);",
        "    if false && !validity.is_valid() {\n        return Err(StoreError::LeaseLost);",
        ["cargo test -p cairn-store --test durability --test store"],
    ),
    Mutant(
        "sm17-close-epoch",
        "closes the source without conditioning on the quiesced writer's epoch (SM-17)",
        "crates/cairn-store/src/config/range_db.rs",
        "admin_io(self.admin(range.0).close_for_writes_at_epoch(writer_epoch)).await",
        "{ let _ = writer_epoch; admin_io(self.admin(range.0).close_for_writes()).await }",
        [SERVER_FP, "cargo test -p cairn-server --test split"],
    ),
    Mutant(
        "heal-stale-close",
        "an open removes the close mark even while the range has an intent (SM-17, heal_stale_close)",
        "crates/cairn-server/src/node/spawner.rs",
        "if cluster.range(partition).is_some_and(|r| r.intent.is_none()) {",
        "if cluster.range(partition).is_some() {",
        [SERVER_FP, "cargo test -p cairn-server --test split"],
    ),
    Mutant(
        "ensure-intent-ours",
        "the coordinator does not require the intent to still be its own before creating children",
        "crates/cairn-server/src/split/steps.rs",
        "if self.cluster.range(p).and_then(|r| r.intent) == Some(intent) {",
        "if true || self.cluster.range(p).and_then(|r| r.intent) == Some(intent) {",
        [SERVER_FP, "cargo test -p cairn-server --test split"],
    ),
    Mutant(
        "verify-external-db",
        "`cairn-ctl verify` does not report external_dbs pointing at a lost Db",
        "crates/cairn-server/src/ctl.rs",
        "                rep.findings.push(Finding::new(\n                    \"external_db\",",
        "                let _ = (Finding::new(\n                    \"external_db\",",
        ["cargo test -p cairn-server --test ctl --test gc"],
    ),
    Mutant(
        "trash-recheck-live",
        "the trash purge round really deletes the Db/owner record of a range still in the table (SM-16)",
        "crates/cairn-store/src/trash.rs",
        "Subject::RangeDb(r) | Subject::Owner(r) if live(*r) => {",
        "Subject::RangeDb(r) | Subject::Owner(r) if false && live(*r) => {",
        ["cargo test -p cairn-store --lib trash --test trash", "cargo test -p cairn-server --test gc"],
    ),
    Mutant(
        "dead-log-listed",
        "the delete-proof gate allows purging a dead log that an owner record still lists (NW-8)",
        "crates/cairn-store/src/proof.rs",
        "if referenced.logs.contains(log) {",
        "if false && referenced.logs.contains(log) {",
        ["cargo test -p cairn-store --lib proof --test shared_wal"],
    ),
    Mutant(
        "dead-log-order",
        "the delete-proof gate accepts an owner-record snapshot older than the death (NW-8)",
        "crates/cairn-store/src/proof.rs",
        "if referenced.seq <= death.seq {",
        "if false && referenced.seq <= death.seq {",
        ["cargo test -p cairn-store --lib proof --test shared_wal"],
    ),
    Mutant(
        "cas-read-back",
        "an ambiguous CAS reads back exactly our content and still treats it as a loss (rule 7)",
        "crates/cairn-store/src/cas.rs",
        "Ok(Some((now, version))) if now == body => {",
        "Ok(Some((now, version))) if false && now == body => {",
        ["cargo test -p cairn-store --lib cas", "cargo test -p cairn-store --lib session"],
    ),
]


def run(cmd: str) -> tuple[bool, float, str]:
    """`(red, seconds, log tail)`. A timeout counts as red (a hang is also a detection)."""
    t0 = time.monotonic()
    try:
        p = subprocess.run(
            cmd, shell=True, cwd=ROOT, capture_output=True, text=True, timeout=TIMEOUT_S
        )
        out = p.stdout + p.stderr
        red = p.returncode != 0
    except subprocess.TimeoutExpired as e:
        out = f"timed out after {TIMEOUT_S}s\n{(e.stdout or b'')!s}"
        red = True
    tail = "\n".join(l for l in out.splitlines() if "FAILED" in l or "panicked" in l or "error" in l)[-1500:]
    return red, time.monotonic() - t0, tail


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if "--list" in sys.argv:
        for m in MUTANTS:
            print(f"{m.id:24} {m.what}")
        return 0
    chosen = [m for m in MUTANTS if not args or m.id in args]
    out_dir = ROOT / "target" / "mutants"
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for m in chosen:
        path = ROOT / m.file
        original = path.read_text()
        n = original.count(m.old)
        if n != 1:
            print(f"✗ {m.id}: the original string occurs {n} times in {m.file} — fix the list")
            rows.append((m, "list error", 0.0, f"the original string occurs {n} times"))
            continue
        path.write_text(original.replace(m.old, m.new, 1))
        try:
            killed_by = None
            spent = 0.0
            tail = ""
            for cmd in m.cmds:
                red, secs, tail = run(cmd)
                spent += secs
                if red:
                    killed_by = cmd
                    break
        finally:
            path.write_text(original)
        verdict = "killed" if killed_by else ("equivalent" if m.equivalent else "SURVIVED")
        mark = "✓" if killed_by or m.equivalent else "✗"
        print(f"{mark} {m.id:24} {verdict:10} {spent:6.0f}s  {killed_by or m.equivalent or ''}")
        rows.append((m, verdict, spent, killed_by or tail))
    with open(out_dir / "report.md", "w") as f:
        f.write("| Mutant | Check broken | Result | Seconds | Caught by |\n|---|---|---|---|---|\n")
        for m, verdict, spent, by in rows:
            note = by if verdict == "killed" else (m.equivalent or "") if verdict == "equivalent" else ""
            f.write(f"| `{m.id}` | {m.what} | {verdict} | {spent:.0f} | {note} |\n")
    survived = [m.id for m, v, _, _ in rows if v not in ("killed", "equivalent")]
    print(f"\n{len(rows) - len(survived)}/{len(rows)} killed or equivalent; survived/errors: {survived or 'none'}")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
