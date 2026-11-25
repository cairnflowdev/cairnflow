#!/usr/bin/env bash
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1
# Source checks that clippy cannot do. A FAIL step of `scripts/check.sh`;
# runs on its own too: `scripts/lint-sources.sh`.
#
# 1. **Bare `tokio::time::sleep(…).await` in `crates/*/src`.** Clippy cannot
#    tell a background loop sleeping a fixed time from a `select!` branch that
#    waits together with the stop signal, so this is a grep: a call
#    `sleep(…).await` (the `… = sleep(…) => …` branch of a `select!` has no
#    `.await` so it does not count; neither does `std::thread::sleep`). The
#    existing sites are frozen in `scripts/lint-sources.allow` (count per
#    file); going above that count is a FAIL. Not counted: comment lines,
#    `tests.rs`/`tests/`, `testkit.rs`/`testkit/`, and everything from a
#    `#[cfg(test)]` + `mod …` at column 0 to the end of the file (by repo
#    convention the test module sits at the end of the file). Background-loop
#    waits go through `cairn_store::pace` — the only seam in the list.
# 2. **The `// #[hot-path]` mark** (AGENTS.md §6): the four hot-path roots
#    (`turn/`, `flow/`, cairn-store's `writer/`, `nodewal/stripe.rs`) carry the
#    mark on line 1; when a marked file is `X/mod.rs` or `X.rs`, every `.rs`
#    file under `X/` must carry it too; the mark is allowed only on line 1.
# 3. **A direct `.await` on a SlateDB operation that retries forever** (the
#    rule "every I/O `.await` in a background loop is bounded"). In production
#    SlateDB retries object store errors without a bound
#    (`object_store_max_retries: None`), so `read_manifest`,
#    `list_checkpoints`, `delete_checkpoint`, `create_detached_checkpoint`,
#    `refresh_checkpoint`, `set_wal_name`, `flush_memtable`,
#    `flush_with_options`, `create_clone`, `build` (opening a Db, reader,
#    clone), `close`, `await_durable` — and the shared WAL's
#    `flush_through_unbounded` — can hang forever when `.await`ed directly.
#    They must go through `cairn_store::config::admin_io` or
#    `cairn_store::retry::deadline` (the future passed directly as an argument
#    — the closing parenthesis of the call is not followed by `.await`).
#    Frozen list: `scripts/lint-unbounded.allow`. Direct object_store calls do
#    not need it: the S3 client has its own bound (`S3_CLIENT_RETRY_TIMEOUT` +
#    a per-request limit).
#
# 4. **Direct deletes outside the delete-proof gate.** Paths that delete
#    customer data go through `DeleteProof` (`crates/cairn-store/src/proof.rs`);
#    a new direct object delete call (`.delete(&…)`, `store.delete(…)`,
#    `.delete_stream(…)`) in `crates/*/src` is a FAIL when it goes above the
#    file's count in the frozen list `scripts/lint-delete.allow`: a new site
#    must explain why it is not customer data, or go through the gate. And
#    `DeleteProof::assume*` (a proof that checks nothing, for tests) must not
#    appear in `crates/*/src` outside `proof.rs`.
#
# 5. **Lifecycle state changes outside the core's shell.** Every change of an
#    invocation's status, queue stage and retry row is an output of
#    `cairn_lifecycle::step`, executed in **one** place —
#    `crates/cairn-server/src/turn/life.rs`. Calls to the engine primitives
#    (`schedule_retry`, `reschedule_retry`, `cancel_retry`, `defer_retries`,
#    `undefer_retry`, `set_retry_rows`, `apply_lifecycle`, `apply_lifecycle_noting`, `apply_stages`,
#    `begin_turn`, `end_turn`, `set_stage`, `requeue_running`,
#    `recover_vqueues`, `enqueue_existing`, `dequeue`, `pause_with`, `pause_in`,
#    `fail_with`, `fail_in`, `cancel`, `resume`, `restart`, `restart_in` on
#    `engine`; `set_invocation_status`, `drop_retry`, `add_retry`,
#    `remove_retry`, `move_vqueue`, `pop_vqueue`, `push_vqueue` on `muts`)
#    in `crates/cairn-server/src` outside that file are a FAIL when they go
#    above the file's count in the frozen list `scripts/lint-lifecycle.allow`
#    (target 0). A receiver split across lines (`engine` at the end of a line,
#    `.op(` on the next) counts too. `Engine::create` (the `Submitted` core)
#    belongs to check 6.
# 6. **Creating an invocation outside the common admission path.** All new
#    work goes through `turn::admit::admit` (L0 throttle, idempotency,
#    isolation, rules); `start_invocation` and `Engine::create`
#    (`engine.create(`, including a receiver split across lines) may only be
#    called in `crates/cairn-server/src/turn/admit.rs`. No allow list.
#
# 7. **Irreversible split/merge actions outside the core's shell.** Every
#    intent CAS (writing or dropping `intent` on a `RangeEntry`), every child
#    cleanup path (`discard_range_db`, `discard_child_now`,
#    `discard_abandoned_child`, `discard_orphan`) and every removal of a close
#    mark (`reopen_range_db`, `reopen_range_db_at`) is an output of
#    `cairn_split::step`, executed in the shell — `crates/cairn-server/src/split/`
#    and a few other executors that ask the core (`factory`/`heal_stale_close`,
#    the orphan round, `cairn-ctl intent clear`). Calls in
#    `crates/cairn-server/src` and `crates/cairn-store/src` outside `split/`
#    and outside test code are a FAIL when they go above the file's count in
#    `scripts/lint-split.allow` (each line has a reason). Definition lines
#    (`fn …`) do not count.
# 8. **`Db` checkpoints** (G16, `docs/reliability/irreversible.md` §0.3).
#    `Db::create_checkpoint`/`Db::begin_checkpoint` take their WAL mark from
#    `WalWriter::flush` (SlateDB #2084); the shared WAL's `wal_id` is only an
#    upper bound (`nodewal/stripe.rs:Stripe::flush`), so such a checkpoint can
#    pick up rows beyond the mark or hit an assert of the manifest writer.
#    Cairn takes checkpoints through `Admin` (`create_detached_checkpoint`)
#    and clones. A call `create_checkpoint(` or `begin_checkpoint(` in
#    `crates/*/src` outside test code is a FAIL. No allow list.
# 9. **Unseeded SlateDB builders** (deterministic simulation,
#    `docs/testing.md` §4). SlateDB's `Admin`, `Db`, `DbReader`, compactor and GC each own an
#    RNG — object store retry jitter (upstream #2141), checkpoint ids, ULIDs —
#    drawn from the operating system when the builder has no `with_seed`; the
#    simulation needs the seed and clock from `cairn_store::entropy`. Calls
#    `AdminBuilder::new(`, `Db::builder(`, `DbReader::builder(`,
#    `CompactorBuilder::new(`, `GarbageCollectorBuilder::new(`, `Db::open(`,
#    `DbReader::open(`, `Admin::builder(` in `crates/*/src` outside test code
#    are a FAIL when they go above the file's count in
#    `scripts/lint-slatedb-seed.allow`: a new site seeds from `entropy` (or
#    uses `config::admin_builder`) and only then goes into the list.
# 10. **The `Dockerfile` base images are pinned by digest:** every `FROM` line
#    carries `@sha256:…` (or `@${X_DIGEST}` with `ARG X_DIGEST=sha256:…`) — a
#    tag (`debian:bookworm-slim`) changes content under our feet. To change a
#    version, fetch the new digest (`docker buildx imagetools inspect <image>`)
#    and update `ARG …_DIGEST` in the same commit. No allow list.
# 11. **Unbounded scans on paths that hold the engine.** The reader's scan API
#    (`PartitionWriter`, `crates/cairn-store/src/writer/read.rs`) without a row
#    bound carries the `_unbounded` suffix; the bounded variants take
#    `limit`/`(max_entries, max_bytes)` and return a cursor (last key) or
#    `partial`. Calls `…_unbounded(` in `crates/cairn-server/src/{turn,actor,flow}/`
#    and `crates/cairn-engine/src/engine/` outside test code are a FAIL when
#    they go above the file's count in the frozen list `scripts/lint-scan.allow`
#    (one reason per line: what the cost scales with — O(live), O(one
#    invocation), Db open path, tests only). Definition lines (`fn …`) do not
#    count. The shared WAL's `flush_through_unbounded` belongs to check 3.
# 12. **Publication lints** (`scripts/oss/`). `oss_lang.py`: no Vietnamese --
#    with diacritics, or undiacritic words in identifiers, literals and
#    generated data -- in exported files beyond the frozen per-file counts of
#    `scripts/lint-lang.allow` (shrink-only; `keep` lines are permanent).
#    `manifest.py check`: every tracked path is classified in
#    `scripts/oss/manifest.toml`. `oss_refs.py`: no new reference to an
#    internal document or internal code beyond `scripts/lint-refs.allow`. The
#    last two exist only in the private repository and are skipped when
#    `scripts/oss/manifest.toml` is absent.
#
# Written for bash 3.2 (macOS): no associative arrays.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ALLOW=scripts/lint-sources.allow
ALLOW_UNBOUNDED=scripts/lint-unbounded.allow
ALLOW_DELETE=scripts/lint-delete.allow
ALLOW_LIFECYCLE=scripts/lint-lifecycle.allow
ALLOW_SPLIT=scripts/lint-split.allow
ALLOW_SEED=scripts/lint-slatedb-seed.allow
ALLOW_SCAN=scripts/lint-scan.allow
SCAN_ROOTS="crates/cairn-server/src/turn crates/cairn-server/src/actor crates/cairn-server/src/flow
crates/cairn-engine/src/engine"
SPLIT_SHELL=crates/cairn-server/src/split/
LIFECYCLE_SHELL=crates/cairn-server/src/turn/life.rs
ADMIT=crates/cairn-server/src/turn/admit.rs
MARK='// #[hot-path]'
HOT_ROOTS="crates/cairn-server/src/turn/mod.rs crates/cairn-server/src/flow/mod.rs
crates/cairn-store/src/writer/mod.rs crates/cairn-store/src/nodewal/stripe.rs"
fail=0

# ───────────────────────────────────────────── 1. bare sleep

# Prints "<count> <file>" for every file with a bare `sleep(…).await` outside test code.
count_sleeps() {
  find crates/*/src -name '*.rs' ! -name tests.rs ! -path '*/tests/*' ! -name testkit.rs ! -path '*/testkit/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        function check(s) {
          if (s ~ /(^|[^_[:alnum:]])sleep\(.*\)[[:space:]]*\.await/ && s !~ /thread::sleep/)
            n++
        }
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        buf != "" {
          buf = buf $0
          held++
          if ($0 ~ /\.await|;/ || held >= 8) { check(buf); buf = "" }
          next
        }
        # rustfmt splits long calls: `sleep(` at the end of a line, `.await` on the next.
        /(^|[^_[:alnum:]])sleep\([[:space:]]*$/ { buf = $0; held = 0; next }
        { check($0) }
        END { if (n) print n, F }
      ' "$f"
    done
}

# Allowed count for file $1 in list $2 (default $ALLOW).
allowed_for() {
  awk -v F="$1" '!/^[[:space:]]*(#|$)/ && $2 == F { print $1; found = 1 } END { if (!found) print 0 }' "${2:-$ALLOW}"
}

found="$(count_sleeps)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL bare sleep: $f has $n \`sleep(…).await\` sites, the allow list says $max."
    echo "     Use \`cairn_store::pace\` (beat / poll_gap / retry_pause / Backoff), wait in a"
    echo "     \`select!\` together with the stop signal, or update $ALLOW with a reason."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n bare sleep sites left (the list says $max) — lower the count in $ALLOW."
  fi
done <<EOF
$found
EOF
# List entries whose file has no site left (or no longer exists).
awk '!/^[[:space:]]*(#|$)/ { print $2 }' "$ALLOW" | while IFS= read -r f; do
  if ! printf '%s\n' "$found" | awk -v F="$f" '$2 == F { ok = 1 } END { exit !ok }'; then
    echo "note: $f has no bare sleep left — remove its line from $ALLOW."
  fi
done

# ───────────────────────────────────────────── 2. hot-path mark

has_mark() { head -n 1 "$1" | grep -qF -- "$MARK"; }

for f in $HOT_ROOTS; do
  if [ ! -f "$f" ]; then
    echo "FAIL hot-path: hot-path root $f no longer exists — update HOT_ROOTS in $0 and AGENTS.md §6."
    fail=1
  elif ! has_mark "$f"; then
    echo "FAIL hot-path: $f is a hot-path root but line 1 does not start with \`$MARK\`."
    fail=1
  fi
done

while IFS= read -r f; do
  [ -n "$f" ] || continue
  line="$(grep -nF -- "$MARK" "$f" | head -n 1 | cut -d: -f1)"
  if [ "$line" != 1 ]; then
    echo "FAIL hot-path: $f carries the mark on line $line — the mark must be line 1."
    fail=1
    continue
  fi
  case "$f" in
    */mod.rs) dir="$(dirname "$f")" ;;
    *) dir="${f%.rs}" ;;
  esac
  [ -d "$dir" ] || continue
  while IFS= read -r child; do
    if ! has_mark "$child"; then
      echo "FAIL hot-path: $child is a child file of $f (hot path) but lacks \`$MARK\` on line 1."
      fail=1
    fi
  done <<EOF
$(find "$dir" -name '*.rs' | LC_ALL=C sort)
EOF
done <<EOF
$(grep -rlF -- "$MARK" crates/*/src | LC_ALL=C sort)
EOF

# ───────────────────────────────────────────── 3. unbounded SlateDB await

# Prints "<count> <file>" for every file with a direct `.<op>(…).await` (a SlateDB
# operation that retries forever) outside test code. Joins the whole file into one
# string (rustfmt splits calls over several lines), then balances parentheses to
# find the call's closing one.
count_unbounded() {
  find crates/*/src -name '*.rs' ! -name tests.rs ! -path '*/tests/*' ! -name testkit.rs ! -path '*/testkit/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        { line = $0; sub(/^[[:space:]]+/, "", line); text = text line " " }
        END {
          re = "(\\.(read_manifest|list_checkpoints|delete_checkpoint|create_detached_checkpoint|refresh_checkpoint|set_wal_name|referenced_wal_ranges|flush_memtable|flush_with_options|create_clone|build|close|await_durable)|[^A-Za-z0-9_]flush_through_unbounded)\\("
          while (match(text, re)) {
            i = RSTART + RLENGTH; depth = 1; L = length(text)
            while (i <= L && depth > 0) {
              c = substr(text, i, 1)
              if (c == "(") depth++; else if (c == ")") depth--
              i++
            }
            if (substr(text, i) ~ /^[[:space:]]*\.await/) n++
            text = substr(text, i)
          }
          if (n) print n, F
        }
      ' "$f"
    done
}

unbounded="$(count_unbounded)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_UNBOUNDED")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL unbounded await: $f has $n direct \`.await\` sites on SlateDB operations that retry forever,"
    echo "     the allow list says $max. Wrap them in \`cairn_store::config::admin_io\` or"
    echo "     \`cairn_store::retry::deadline\`, or update $ALLOW_UNBOUNDED with a reason (bound at the call site)."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n unbounded SlateDB awaits left (the list says $max) — lower the count in $ALLOW_UNBOUNDED."
  fi
done <<EOF
$unbounded
EOF
awk '!/^[[:space:]]*(#|$)/ { print $2 }' "$ALLOW_UNBOUNDED" | while IFS= read -r f; do
  if ! printf '%s\n' "$unbounded" | awk -v F="$f" '$2 == F { ok = 1 } END { exit !ok }'; then
    echo "note: $f has no unbounded SlateDB await left — remove its line from $ALLOW_UNBOUNDED."
  fi
done

# ───────────────────────────────────────────── 4. deletes outside the proof gate

# Prints "<count> <file>" for every file with a direct object delete call outside test code.
count_deletes() {
  find crates/*/src -name '*.rs' ! -name tests.rs ! -path '*/tests/*' ! -name testkit.rs ! -path '*/testkit/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /\.delete\(&|store(\(\))?\.delete\(|\.delete_stream\(/ { n++ }
        END { if (n) print n, F }
      ' "$f"
    done
}

deletes="$(count_deletes)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_DELETE")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL delete outside the gate: $f has $n direct object delete calls, the allow list says $max."
    echo "     Customer data goes through \`cairn_store::proof::DeleteProof\`;"
    echo "     anything else: update $ALLOW_DELETE with a reason."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n direct delete calls left (the list says $max) — lower the count in $ALLOW_DELETE."
  fi
done <<EOS
$deletes
EOS
assumed="$(grep -rn 'DeleteProof::assume' crates/*/src --include='*.rs' | grep -v '^crates/cairn-store/src/proof.rs:' || true)"
if [ -n "$assumed" ]; then
  echo "FAIL \`DeleteProof::assume*\` outside tests (a proof that checks nothing):"
  printf '%s\n' "$assumed"
  fail=1
fi

# ───────────────────────────────────── 5. state changes outside the core's shell

# Prints "<count> <file>" for every cairn-server file with a primitive call that
# changes lifecycle state outside the shell (`turn/life.rs`) and outside test code.
count_lifecycle() {
  find crates/cairn-server/src -name '*.rs' ! -name tests.rs ! -path '*/tests/*' ! -name testkit.rs ! -path '*/testkit/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      [ "$f" = "$LIFECYCLE_SHELL" ] && continue
      awk -v F="$f" '
        BEGIN {
          eng = "(schedule_retry|reschedule_retry|cancel_retry|defer_retries|undefer_retry|set_retry_rows|apply_lifecycle|apply_lifecycle_noting|apply_stages|begin_turn|end_turn|set_stage|requeue_running|recover_vqueues|enqueue_existing|dequeue|pause_with|pause_in|fail_with|fail_in|cancel|resume|restart|restart_in)"
          mut = "(set_invocation_status|drop_retry|add_retry|remove_retry|move_vqueue|pop_vqueue|push_vqueue)"
        }
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /^[[:space:]]*$/ { next }
        {
          if ($0 ~ ("(^|[^_[:alnum:]])engine[[:space:]]*\\.[[:space:]]*" eng "\\(") ||
              $0 ~ ("(^|[^_[:alnum:]])muts[[:space:]]*\\.[[:space:]]*" mut "\\(") ||
              (prev ~ /(^|[^_[:alnum:]])engine[[:space:]]*$/ && $0 ~ ("^[[:space:]]*\\." eng "\\(")) ||
              (prev ~ /(^|[^_[:alnum:]])muts[[:space:]]*$/ && $0 ~ ("^[[:space:]]*\\." mut "\\(")))
            n++
          prev = $0
        }
        END { if (n) print n, F }
      ' "$f"
    done
}

lifecycle="$(count_lifecycle)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_LIFECYCLE")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL state change outside the shell: $f has $n primitive calls that change lifecycle state,"
    echo "     the allow list says $max. Ask the core (\`cairn_lifecycle::step\`) and execute the Plan"
    echo "     through \`$LIFECYCLE_SHELL\`, or update $ALLOW_LIFECYCLE with a reason."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n state-change calls outside the shell left (the list says $max) — lower the count in $ALLOW_LIFECYCLE."
  fi
done <<EOL
$lifecycle
EOL

# ───────────────────────────────────── 6. invocation creation outside `admit`

creates="$(
  find crates/cairn-server/src -name '*.rs' ! -name tests.rs ! -name testkit.rs ! -path '*/testkit/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      [ "$f" = "$ADMIT" ] && continue
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /^[[:space:]]*$/ { next }
        {
          if ($0 ~ /(^|[^_[:alnum:]])start_invocation[[:space:]]*\(/ ||
              $0 ~ /(^|[^_[:alnum:]])engine[[:space:]]*\.[[:space:]]*create[[:space:]]*\(/ ||
              (prev ~ /(^|[^_[:alnum:]])engine[[:space:]]*$/ && $0 ~ /^[[:space:]]*\.create[[:space:]]*\(/))
            print F ":" FNR ": " $0
          prev = $0
        }
      ' "$f"
    done
)"
if [ -n "$creates" ]; then
  echo "FAIL invocation created outside \`admit\`: \`start_invocation\`/\`Engine::create\` are called only in $ADMIT"
  echo "     (new work goes through \`turn::admit::admit\`):"
  printf '%s\n' "$creates"
  fail=1
fi

# ───────────────────────────────────── 7. split/merge outside the core's shell

count_split() {
  find crates/cairn-server/src crates/cairn-store/src -name '*.rs' ! -name tests.rs ! -name testkit.rs \
    ! -path '*/testkit/*' ! -path '*/tests/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      case "$f" in "$SPLIT_SHELL"*) continue ;; esac
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /(^|[^_[:alnum:]])fn[[:space:]]/ { next }
        /(^|[^_[:alnum:]])(reopen_range_db|reopen_range_db_at|discard_range_db|discard_child_now|discard_abandoned_child|discard_orphan)[[:space:]]*\(/ { n++; next }
        /RangeEntry[[:space:]]*\{[[:space:]]*intent:/ { n++; next }
        /^[[:space:]]*intent:[[:space:]]*Some\((intent|RangeIntent)/ { n++; next }
        END { if (n) print n, F }
      ' "$f"
    done
}

splits="$(count_split)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_SPLIT")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL split/merge outside the shell: $f has $n intent CAS / child cleanup / close-mark removal sites,"
    echo "     the allow list says $max. Ask the core (\`cairn_split::step\`) and execute the Plan in"
    echo "     \`$SPLIT_SHELL\`, or update $ALLOW_SPLIT with a reason."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n split/merge sites outside the shell left (the list says $max) — lower the count in $ALLOW_SPLIT."
  fi
done <<EOS
$splits
EOS

# ───────────────────────────────────── 8. `Db` checkpoints

checkpoints="$(
  find crates/*/src -name '*.rs' ! -name tests.rs ! -name testkit.rs ! -path '*/testkit/*' ! -path '*/tests/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /(^|[^_[:alnum:]])(create_checkpoint|begin_checkpoint)[[:space:]]*\(/ { print F ":" FNR ": " $0 }
      ' "$f"
    done
)"
if [ -n "$checkpoints" ]; then
  echo "FAIL \`Db\` checkpoint (G16): its WAL mark relies on the \`wal_id\` of \`WalWriter::flush\`,"
  echo "     and the shared WAL returns only an upper bound. Take checkpoints through \`Admin\` (\`create_detached_checkpoint\`):"
  printf '%s\n' "$checkpoints"
  fail=1
fi

# ───────────────────────────────────── 9. unseeded SlateDB builders

count_seeded() {
  find crates/*/src -name '*.rs' ! -name tests.rs ! -name testkit.rs ! -path '*/testkit/*' ! -path '*/tests/*' |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /(^|[^_[:alnum:]])(AdminBuilder::new|Db::builder|DbReader::builder|CompactorBuilder::new|GarbageCollectorBuilder::new|Db::open|DbReader::open|Admin::builder)[[:space:]]*\(/ { n++ }
        END { if (n) print n, F }
      ' "$f"
    done
}

seeded="$(count_seeded)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_SEED")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL SlateDB builder: $f has $n sites building Admin/Db/DbReader/compactor/GC, the allow list says $max."
    echo "     Seed the RNG and clock from \`cairn_store::entropy\` (\`slatedb_seed\`, \`slatedb_clock\`;"
    echo "     \`Admin\` through \`config::admin_builder\`), then update $ALLOW_SEED with a reason."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n SlateDB builder sites left (the list says $max) — lower the count in $ALLOW_SEED."
  fi
done <<EOS
$seeded
EOS

unpinned="$( (grep -nE '^FROM ' Dockerfile | grep -vE '@(sha256:[0-9a-f]{64}|\$\{[A-Z_]+_DIGEST\})( |$)';
  grep -nE '^ARG [A-Z_]+_DIGEST=' Dockerfile | grep -vE '=sha256:[0-9a-f]{64}$') || true)"
if [ -n "$unpinned" ]; then
  echo "FAIL Dockerfile: base image not pinned by digest:"
  printf '%s\n' "$unpinned" | sed 's/^/     /'
  echo "     Get the digest with \`docker buildx imagetools inspect <image>\`, then pin \`FROM <image>:<tag>@sha256:…\`."
  fail=1
fi

# ───────────────────────────────────── 11. unbounded scans while holding the engine

# Prints "<count> <file>" for every file under $SCAN_ROOTS with a `…_unbounded(` call
# outside test code and outside definition lines. rustfmt may move `.x_unbounded(` to
# the line after the receiver, but the name sits right next to the opening
# parenthesis, so counting per line is enough.
count_scans() {
  for d in $SCAN_ROOTS; do
    find "$d" -name '*.rs' ! -name tests.rs ! -path '*/tests/*' ! -name testkit.rs ! -path '*/testkit/*'
  done |
    LC_ALL=C sort |
    while IFS= read -r f; do
      awk -v F="$f" '
        pending && /^mod / { exit }
        { pending = 0 }
        /^#\[cfg\(test\)\]/ { pending = 1; next }
        /^[[:space:]]*\/\// { next }
        /(^|[^_[:alnum:]])fn[[:space:]]/ { next }
        {
          line = $0
          while (match(line, /[_[:alnum:]]+_unbounded[[:space:]]*\(/)) {
            n++
            line = substr(line, RSTART + RLENGTH)
          }
        }
        END { if (n) print n, F }
      ' "$f"
    done
}

scans="$(count_scans)"
while read -r n f; do
  [ -n "${n:-}" ] || continue
  max="$(allowed_for "$f" "$ALLOW_SCAN")"
  if [ "$n" -gt "$max" ]; then
    echo "FAIL unbounded scan while holding the engine: $f has $n \`…_unbounded(\` calls, the allow list says $max."
    echo "     Use the reader's bounded variant (\`limit\`, a cursor, releasing the engine between two chunks),"
    echo "     or update $ALLOW_SCAN with a reason (what the cost scales with)."
    fail=1
  elif [ "$n" -lt "$max" ]; then
    echo "note: $f has $n unbounded scan calls left (the list says $max) — lower the count in $ALLOW_SCAN."
  fi
done <<EOS
$scans
EOS
awk '!/^[[:space:]]*(#|$)/ { print $2 }' "$ALLOW_SCAN" | while IFS= read -r f; do
  if ! printf '%s\n' "$scans" | awk -v F="$f" '$2 == F { ok = 1 } END { exit !ok }'; then
    echo "note: $f has no unbounded scan call left — remove its line from $ALLOW_SCAN."
  fi
done

# ─────────────────────────────────────── 12. publication lints (scripts/oss)

if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL publication lints: python3 not found (scripts/oss needs Python 3.9+)."
  fail=1
else
  if [ -f scripts/oss/manifest.toml ]; then
    python3 scripts/oss/manifest.py check || fail=1
    python3 scripts/oss/oss_refs.py || fail=1
  fi
  python3 scripts/oss/oss_lang.py || fail=1
fi

total() { printf '%s\n' "$1" | awk 'NF { s += $1 } END { print s + 0 }'; }
if [ "$fail" = 0 ]; then
  echo "lint-sources: clean ($(total "$found") bare sleeps, $(total "$unbounded") unbounded SlateDB awaits, $(total "$deletes") direct object deletes, $(total "$lifecycle") lifecycle state changes outside the shell, $(total "$splits") split/merge sites outside \`split/\`, $(total "$seeded") SlateDB builder sites, $(total "$scans") unbounded scans while holding the engine — all within the frozen lists; 0 invocations created outside \`admit\`, 0 \`Db\` checkpoints)."
fi
exit "$fail"
