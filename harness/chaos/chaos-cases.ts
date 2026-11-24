// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * The pure part of the chaos cases (`chaos.ts`): the mutual-exclusion watch on
 * hot objects, the watch for `running` invocations standing still, the expected
 * output of the parallel-branch handler. Split out of `chaos.ts` so it can be unit
 * tested (`chaos-cases.test.ts`) — the harness only runs on a real cluster.
 */

// ───────────────────────────── (a) mutual exclusion on hot objects

/** One hold of object `key` by invocation `inv`, seen in the handler. */
type Hold = { inv: string; at: number };

/**
 * The watch for "no two concurrent SDK turns on one object key". The `EXCLUSIVE`
 * handler reports **enter** in its first `ctx.run` closure (the lock is held) and
 * **leave** in its last closure — the hold span covers the time suspended waiting
 * for a signal, because a VO lock is held across a suspension. Another invocation
 * entering within that span means two live instances on one lock.
 *
 * Not counted:
 * - the same invocation entering again (the turn died before the closure's result
 *   was durable ⇒ the closure reruns — exactly the `ctx.run` contract);
 * - a holder the harness has sent a cancel to (`excuse` returns the reason): a hard
 *   cancel ends the invocation inside the hold span, the leave closure never runs.
 *
 * A holder that has ended (the harness sees its final status) is removed with
 * [`forget`] — in case the leave closure ran in a turn the harness did not see.
 */
export class ExclusiveWatch {
  private readonly holds = new Map<string, Hold[]>();
  /** Number of enters examined. */
  enters = 0;
  /** Overlaps excused, by reason. */
  readonly excused = new Map<string, number>();
  /** Violations: description lines. */
  readonly violations: string[] = [];
  /** Largest number of overlapping holders seen on one key (printed only). */
  maxHolders = 0;
  /** Pairs (key, entering, holding) already reported — one line per pair. */
  private readonly reported = new Set<string>();

  /**
   * `inv` enters the hold span of `key` at `now`. `excuse(other)` returns the reason
   * to excuse it when `other` is holding, or `undefined`.
   */
  enter(key: string, inv: string, now: number, excuse: (other: string) => string | undefined): void {
    this.enters++;
    const list = this.holds.get(key) ?? [];
    const kept: Hold[] = [];
    for (const h of list) {
      if (h.inv === inv) continue;
      const why = excuse(h.inv);
      if (why !== undefined) {
        this.excused.set(why, (this.excused.get(why) ?? 0) + 1);
        continue;
      }
      const pair = `${key}|${inv}|${h.inv}`;
      if (!this.reported.has(pair) && this.violations.length < 1_000) {
        this.reported.add(pair);
        this.violations.push(
          `${key}: ${inv} entered at ${new Date(now).toISOString()} while ${h.inv} still held it (entered at ${new Date(h.at).toISOString()})`,
        );
      }
      kept.push(h);
    }
    kept.push({ inv, at: list.find((h) => h.inv === inv)?.at ?? now });
    this.maxHolders = Math.max(this.maxHolders, kept.length);
    this.holds.set(key, kept);
  }

  /**
   * `inv` leaves the hold span of `key` (the last closure): only removes, does not
   * judge. The leave closure of a zombie turn (a node that lost its lease after
   * delivering a pending result) may finish after a new turn of the same invocation
   * has completed and the next holder has entered — judging here would be a false
   * alarm; a later entrant is caught by its own [`enter`].
   */
  leave(key: string, inv: string): void {
    this.forget(inv, key);
  }

  /** Removes every hold of `inv` (it has ended). */
  forget(inv: string, key?: string): void {
    for (const [k, list] of this.holds) {
      if (key !== undefined && k !== key) continue;
      const rest = list.filter((h) => h.inv !== inv);
      if (rest.length === 0) this.holds.delete(k);
      else this.holds.set(k, rest);
    }
  }

  /** Number of holders of `key` according to the watch. */
  holders(key: string): number {
    return this.holds.get(key)?.length ?? 0;
  }
}

// ───────────────────────────── (b) `running` standing still while the cluster is healthy

/** One row of `GET /invocations?scope=cluster&status=running`. */
export type RunningRow = {
  id: string;
  status: string;
  service?: string;
  handler?: string;
  key?: string;
  attempt?: number;
  modifiedAt?: number;
  createdAt?: number;
  tenant?: string;
};

/**
 * The watch for "no `running` invocation standing still for more than N minutes
 * while the cluster is healthy" (a turn failing before `Running` once left a
 * far-off deadline row). A row's clock starts at its last change (`modifiedAt`) or
 * at the most recent time the cluster became healthy again (`healthySince` — after
 * a kill, a node exiting on its own, a revive, a failpoint set/cleared, a slow
 * node, a broken store), whichever is later. Over `limitMs` ⇒ a violation (once per id).
 *
 * A row without `modifiedAt` (an old node) uses the time it was first seen.
 */
export class StuckWatch {
  private readonly firstSeen = new Map<string, number>();
  readonly flagged = new Map<string, string>();
  /** Number of observations, rows examined (printed only). */
  rounds = 0;
  rows = 0;
  /** Largest age (ms) of a `running` row counted from the healthy mark — printed only. */
  maxAgeMs = 0;

  readonly limitMs: number;

  constructor(limitMs: number) {
    this.limitMs = limitMs;
  }

  observe(rows: readonly RunningRow[], now: number, healthySince: number): void {
    this.rounds++;
    for (const r of rows) {
      if (r.status !== "running") continue;
      this.rows++;
      if (!this.firstSeen.has(r.id)) this.firstSeen.set(r.id, now);
      const changed = r.modifiedAt ?? this.firstSeen.get(r.id)!;
      const since = Math.max(changed, healthySince);
      const age = now - since;
      this.maxAgeMs = Math.max(this.maxAgeMs, age);
      if (age > this.limitMs && !this.flagged.has(r.id)) {
        this.flagged.set(
          r.id,
          `${r.id} ${r.service ?? "?"}/${r.handler ?? "?"}${r.key ? `[${r.key}]` : ""}` +
            `${r.tenant ? ` (${r.tenant})` : ""} running, attempt ${r.attempt ?? 0}, last change ` +
            `${new Date(changed).toISOString()}, cluster healthy since ${new Date(healthySince).toISOString()} — ` +
            `standing still ${Math.round(age / 1000)}s > ${Math.round(this.limitMs / 1000)}s`,
        );
      }
    }
  }
}

// ───────────────────────────── (e) parallel branches

/** Step `i` of branch `b` is a `ctx.run` (even) or a `ctx.call` (odd). */
export const parIsRun = (i: number) => i % 2 === 0;

/**
 * The `ctx.run` step throws a `TerminalError` (a durable failure, the branch catches
 * it and continues) — deterministic by `seq`: one call in four, at step 0 of one branch.
 */
export function parTerminalAt(seq: number, b: number, i: number, branches: number): boolean {
  return seq % 4 === 0 && b === seq % branches && i === 0;
}

/** The value of the `ChaosEcho.ping` call of odd step `i`, branch `b`. */
export const parEcho = (b: number, i: number) => b * 100 + i;

/** The expected output of `ChaosPar.fan` for `seq`: one array of step values per branch. */
export function parExpected(seq: number, branches: number, steps: number): string[][] {
  return Array.from({ length: branches }, (_, b) =>
    Array.from({ length: steps }, (_, i) =>
      parIsRun(i) ? (parTerminalAt(seq, b, i, branches) ? `T${b}.${i}` : `r${b}.${i}`) : `e${parEcho(b, i)}`,
    ),
  );
}

/**
 * Compares the real output with the expected one, branch by branch. Returns the list of mismatches (empty = correct).
 */
export function parDiff(seq: number, branches: number, steps: number, output: unknown): string[] {
  const want = parExpected(seq, branches, steps);
  const got = (output as { seq?: unknown; branches?: unknown } | null | undefined) ?? {};
  const out: string[] = [];
  if (got.seq !== seq) out.push(`seq ${String(got.seq)} ≠ ${seq}`);
  if (!Array.isArray(got.branches)) return [...out, `branches is not an array: ${JSON.stringify(got.branches)?.slice(0, 120)}`];
  if (got.branches.length !== branches) out.push(`${got.branches.length} branches ≠ ${branches}`);
  for (let b = 0; b < branches; b++) {
    const g = JSON.stringify(got.branches[b]);
    const w = JSON.stringify(want[b]);
    if (g !== w) out.push(`branch ${b}: ${g?.slice(0, 200)} ≠ ${w}`);
  }
  return out;
}

// ───────────────────────────── (d) findings on a restored copy

/**
 * Findings of `verify`/`backup check-restore` that are a **documented limit** of
 * backups, not a bug: the copy is not consistent across ranges (each range is
 * captured at its own L0 point), so cross-range messages in flight can be lost —
 * `check-restore` must detect them, the operator handles them (runbook
 * `docs/runbooks/restore-from-backup.md`, the item on the missing common cut across
 * ranges, and step 5). The names match `ctl/restore_check.rs`
 * (`LOST_CALL`, `LOST_REPLY`).
 */
export const RESTORE_KNOWN_LIMITS: ReadonlySet<string> = new Set(["restore.lost_call", "restore.lost_reply"]);

/** One run of `cairn-ctl --json verify|backup check-restore`. */
export type CtlRun = { code: number | null; out: string; err: string };

/** Classification of the findings of one check of a restored copy. */
export type RestoreJudgement = {
  /** Violations: findings no round heals that are not a documented limit, or a failed run. */
  violations: string[];
  /** Documented limits (`restore.lost_*`) — correct findings, counted by name. */
  known: Map<string, number>;
  /** Findings with a healing round (`heals`) — counted by `name (round)`. */
  healed: Map<string, number>;
};

/**
 * Judges one `verify`/`backup check-restore` run on a restored copy.
 * Exit code 1 means "findings present" (the output is still JSON); a code other than 0/1 is a failed run.
 */
export function judgeRestoreCheck(what: string, run: CtlRun): RestoreJudgement {
  const out: RestoreJudgement = { violations: [], known: new Map(), healed: new Map() };
  const bump = (m: Map<string, number>, k: string) => m.set(k, (m.get(k) ?? 0) + 1);
  if (run.code !== 0 && run.code !== 1) {
    out.violations.push(`${what} exited ${run.code}: ${run.err.trim().slice(0, 300)}`);
    return out;
  }
  let rep: { findings?: Array<{ check: string; range: number | null; detail: string; heals?: string | null }> };
  try {
    rep = JSON.parse(run.out) as typeof rep;
  } catch {
    out.violations.push(`${what}: output is not JSON: ${run.out.slice(0, 200)}`);
    return out;
  }
  const findings = rep.findings ?? [];
  for (const f of findings) {
    if (RESTORE_KNOWN_LIMITS.has(f.check)) bump(out.known, f.check);
    else if (f.heals) bump(out.healed, `${f.check} (${f.heals})`);
    else out.violations.push(`${what}: ${f.check} range ${f.range}: ${f.detail.slice(0, 300)}`);
  }
  if (run.code === 1 && findings.length === 0) out.violations.push(`${what} exited 1 without any finding`);
  return out;
}

// ───────────────────────────── (c) judging a purge by the `tenants` book

/** The header of the `<prefix>/tenants` book (`cairn-store/src/tenant_purge.rs::HEADER`). */
export const TENANT_BOOK_HEADER = "cairnflow-tenants/1";

/** A tenant's line in the book — the purge fields, verbatim. */
export type BookLine = { state?: string; deletingMs?: string; purgingMs?: string; purgedMs?: string };
/** Result of reading the book: the tenant's line (`null` = no line — tenant `active`), or the reason it could not be read. */
export type BookRead = { ok: true; line: BookLine | null } | { ok: false; why: string };

/** Reads `tenant`'s line in the `tenants` book (the same splitting rules as `decode_book`). */
export function parseTenantBook(text: string, tenant: string): BookRead {
  const lines = text.split("\n");
  if ((lines[0] ?? "").trim() !== TENANT_BOOK_HEADER) {
    return { ok: false, why: `header ${JSON.stringify((lines[0] ?? "").slice(0, 60))}, expected ${TENANT_BOOK_HEADER}` };
  }
  for (const raw of lines.slice(1)) {
    const f = raw.split(" ").filter((x) => x !== "");
    if (f[0] !== tenant) continue;
    const line: BookLine = {};
    for (const x of f.slice(1)) {
      if (x.startsWith("state=")) line.state = x.slice(6);
      else if (x.startsWith("deleting-ms=")) line.deletingMs = x.slice(12);
      else if (x.startsWith("purging-ms=")) line.purgingMs = x.slice(11);
      else if (x.startsWith("purged-ms=")) line.purgedMs = x.slice(10);
    }
    return { ok: true, line };
  }
  return { ok: true, line: null };
}

/** What the harness sees through `GET /admin/tenants/{t}/purge`. */
export type PurgeApiView = {
  /** `state` of the most recent **fresh** answer (absent = none yet). */
  state?: string;
  /** Total failed polls (errors, non-2xx codes, over the time cap). */
  failures: number;
  /** Number of consecutive failed polls up to the last one. */
  failStreak: number;
  /** Age (ms) of the most recent fresh answer at judgement time. */
  lastFreshAgeMs?: number;
};

export type PurgeJudgement = {
  purged: boolean;
  /** What the verdict rests on: a fresh API answer, the book on the bucket, or undecidable. */
  source: "api" | "book" | "undecidable";
  /** Reason for failing (absent when `purged`). */
  problem?: string;
  /** One line printing both sources. */
  note: string;
};

const bookText = (b: BookRead | undefined) =>
  b === undefined
    ? "not read"
    : !b.ok
      ? `unreadable (${b.why})`
      : b.line === null
        ? "no line (active)"
        : `state=${b.line.state ?? "active"}${b.line.deletingMs ? ` deleting-ms=${b.line.deletingMs}` : ""}` +
          `${b.line.purgingMs ? ` purging-ms=${b.line.purgingMs}` : ""}${b.line.purgedMs ? ` purged-ms=${b.line.purgedMs}` : ""}`;

/**
 * Judges case (c): a fresh API answer says `purged` ⇒ done. Otherwise **the book is
 * the arbiter**: book `purged` ⇒ done (the API is stale — the poll path over its
 * cap); book not yet `purged` ⇒ stuck per the book. Book unreadable: only rule
 * "stuck per the API" while the API answer is still fresh (fewer than `staleAfter`
 * consecutive failed polls) — never rule stuck on a stale answer.
 */
export function judgePurge(api: PurgeApiView, book: BookRead | undefined, staleAfter: number): PurgeJudgement {
  const stale = api.failStreak >= staleAfter;
  const apiText =
    `API ${api.state ?? "no fresh answer yet"}` +
    `${api.lastFreshAgeMs !== undefined ? ` (fresh answer ${Math.round(api.lastFreshAgeMs / 1000)}s ago)` : ""}` +
    `, ${api.failures} failed polls, ${api.failStreak} in a row${stale ? " — STALE" : ""}`;
  const note = `${apiText}; tenants book: ${bookText(book)}`;
  if (api.state === "purged") return { purged: true, source: "api", note };
  if (book?.ok) {
    if (book.line?.state === "purged") return { purged: true, source: "book", note };
    return { purged: false, source: "book", problem: `stuck per the tenants book (${bookText(book)})`, note };
  }
  if (stale) {
    return {
      purged: false,
      source: "undecidable",
      problem: `undecidable: API over its cap/failing ${api.failStreak} times in a row and tenants book ${bookText(book)}`,
      note,
    };
  }
  return { purged: false, source: "api", problem: `stuck per the API (fresh answer: ${api.state ?? "?"}), tenants book ${bookText(book)}`, note };
}

// ───────────────────────────── masked 500 errors (split/merge verdict)

/**
 * One node log line "internal error returned as 500" (`ingress/error.rs`) → its
 * `[code, detail]`. A 500 body carries only `code <code>`; the detail (e.g.
 * `failpoint split::flush: chaos`) is only in the log. The split/merge verdict
 * uses this to recognise errors the harness injected itself. Handles tracing's
 * coloured text log (ANSI codes around field names) and JSON.
 */
export function maskedErrorOf(line: string): [code: string, detail: string] | undefined {
  if (!line.includes("internal error returned as 500")) return undefined;
  // eslint-disable-next-line no-control-regex
  const plain = line.replace(/\x1b\[[0-9;]*m/g, "");
  const code = /\bcode"?[=:]"?([0-9a-f]{12})/.exec(plain)?.[1];
  if (!code) return undefined;
  const at = plain.search(/\bdetail"?[=:]/);
  return [code, at < 0 ? plain : plain.slice(at)];
}

// ───────────────────────────── uncaught errors the run survives

/**
 * An uncaught error that must not end the run, with the reason to print; `undefined` = end
 * the run as before. Only one today: Node 24.18's undici calls `socket.setTypeOfService` on
 * every HTTP/1 write, and on macOS a socket the peer already reset throws `EINVAL`
 * synchronously from a socket callback — no listener can catch it. The request on that socket
 * fails on its own timeout; the cluster under test is not involved. A 24 h soak died in its 8th
 * hour this way, without a verdict.
 */
export function survivableUncaught(e: unknown): string | undefined {
  const err = e as NodeJS.ErrnoException | undefined;
  if (err instanceof Error && err.code === "EINVAL" && err.syscall === "setTypeOfService") {
    return "undici setTypeOfService EINVAL on a broken socket (Node bug, the request times out on its own)";
  }
  return undefined;
}

/**
 * How a `cairn-ctl` run ended, for a log line: the exit code and the first line of stderr, or
 * "timed out" when the harness killed it at its own timeout, or the signal that killed it.
 * A 24 h soak printed "backup refused/failed (null) — " for 25 cycles that each ran the full
 * 900 s and were SIGKILLed — it read like an instant exit.
 */
export function describeCtlExit(
  r: { code: number | null; signal: string | null; timedOut: boolean; err: string },
  timeoutMs: number,
): string {
  const err = r.err.trim().slice(0, 200);
  const tail = err === "" ? "" : `: ${err}`;
  if (r.timedOut) return `timed out after ${Math.round(timeoutMs / 1000)}s (killed by the harness)${tail}`;
  if (r.code === null) return `killed by ${r.signal ?? "an unknown signal"}${tail}`;
  return `exited ${r.code}${tail}`;
}

/**
 * The backup ids an `aws s3 ls <root>/` listing shows (its `PRE <id>/` lines). The harness
 * removes those without a `BACKUP` manifest after a failed or refused cycle: they are partial
 * copies `restore` never uses, and 46 of them filled the disk of a 24 h soak.
 */
export function backupIdsIn(ls: string): string[] {
  return ls
    .split("\n")
    .map((l) => /^\s*PRE (.+)\/$/.exec(l)?.[1])
    .filter((id): id is string => id !== undefined);
}

/**
 * A setup call of the harness (keys, deployments, rules, tenant settings) under the S3 fault
 * profile: a `5xx` or a network error is retried, up to `attempts` calls with `pause` between
 * them; any other answer (`2xx`, `4xx`) is returned at once. The last `5xx` is returned, the
 * last network error thrown. A short soak died while setting up because one injected GET
 * failure of `auth/keys` turned `POST /admin/keys` into a `500`.
 */
export async function retryServerErrors(
  call: () => Promise<Response>,
  attempts: number,
  pause: (ms: number) => Promise<void>,
): Promise<Response> {
  for (let i = 1; ; i++) {
    try {
      const r = await call();
      if (r.status < 500 || i >= attempts) return r;
      await r.body?.cancel().catch(() => undefined);
    } catch (e) {
      if (i >= attempts) throw e;
    }
    await pause(1_000);
  }
}
