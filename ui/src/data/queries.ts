// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Named `/query` statements of the console. Rules:
//
// - **Every statement declares the question number(s) n** of the statement shape
//   it uses, and each number must have a server test `ui_question_<n>` that pins
//   the cost of that shape (`queries.test.ts` — `ui_queries_are_catalogued`). A
//   new screen that needs a new statement ⇒ add `ui_question_<n>` on the server
//   first.
// - **User values only go through bind parameters** (`$1…$n`): `sql` is a fixed
//   string, values are never concatenated in. The part that varies with the
//   filters (WHERE) is assembled from the fixed fragments of `invocationList`;
//   values are still parameters.
// - The UI never derives `status`/`class` itself: it shows exactly what the
//   server returns.

export type Param = string | number | boolean | null;

export type Query<P extends Param[] = Param[]> = {
  /** Question numbers of the statement shape; each number has a `ui_question_<n>`. */
  questions: number[];
  sql: string;
  params: P;
};

function q<P extends Param[]>(questions: number[], sql: string, params: P): Query<P> {
  return { questions, sql: sql.replace(/\s+/g, " ").trim(), params };
}

/** Classes of `sys_invocation.class`. */
export type InvocationClass = "active" | "paused" | "completed" | "failed";

/** Fixed statements (no arbitrary filters). */
/** Scope and limit key of a queue (`QueueName`); `null` = none. */
export type QueueIdent = { scope: string | null; limitKey: string | null };

/** `tenant = $1 [AND scope = $n] [AND limit_key = $n] AND service = $n` — same parameter order. */
function queueWhere(tenant: string, queue: QueueIdent, service: string) {
  const params: (string | number)[] = [tenant];
  const parts = ["tenant = $1"];
  if (queue.scope !== null) {
    params.push(queue.scope);
    parts.push(`scope = $${params.length}`);
  }
  if (queue.limitKey !== null) {
    params.push(queue.limitKey);
    parts.push(`limit_key = $${params.length}`);
  }
  params.push(service);
  parts.push(`service = $${params.length}`);
  return { sql: parts.join(" AND "), params };
}

export const queries = {
  /** Counts by service × class — in RAM, does not read the store (Overview). */
  invocationCounts: () =>
    q(
      [1],
      "SELECT service, class, sum(count) AS n FROM sys_invocation_counts GROUP BY service, class",
      [],
    ),
  /** Per-handler statistics, default window (Overview, Services). */
  handlerStats: () =>
    q(
      [2],
      `SELECT service, handler, sum(turns) AS turns, sum(completed) AS completed, sum(failed) AS failed,
              sum(paused) AS paused, sum(failed_attempts) AS failed_attempts, max(p99_ms) AS p99_ms
       FROM sys_handler_stats GROUP BY service, handler`,
      [],
    ),
  /** Handlers failing the most (Overview). */
  topFailing: (limit: number) =>
    q(
      [4],
      `SELECT service, handler, sum(count) AS n FROM sys_invocation_counts WHERE class = 'failed'
       GROUP BY service, handler ORDER BY n DESC, service, handler LIMIT $1`,
      [limit],
    ),
  /** Health per service: failed/total and attempts (Overview "Needs attention"). */
  serviceHealth: () =>
    q(
      [5],
      `SELECT c.service,
              sum(CASE WHEN c.class = 'failed' THEN c.count ELSE 0 END) AS failed,
              sum(c.count) AS total,
              max(s.turns) AS turns
       FROM sys_invocation_counts c
       LEFT JOIN (SELECT service, sum(turns) AS turns FROM sys_handler_stats GROUP BY service) s
         ON s.service = c.service
       GROUP BY c.service`,
      [],
    ),
  /** Deployments and the number of invocations still pinned to them (Overview, Deployments). */
  deploymentsPinned: () => q([8], "SELECT id, pinned FROM sys_deployment", []),
  /**
   * Hourly since a point in time (Usage): completed/failed/CU. The view reads 30
   * days and the filter applies afterwards (`ui_question_3`); the server caches
   * closed days, so a warm query only LISTs today and yesterday. Refreshed with
   * `HISTORY_REFRESH`.
   */
  history: (sinceMs: number) =>
    q(
      [3],
      `SELECT hour_ms, sum(completed) AS completed, sum(failed) AS failed, sum(cu) AS cu
       FROM sys_handler_history WHERE hour_ms >= $1 GROUP BY hour_ms ORDER BY hour_ms`,
      [sinceMs],
    ),
  /** Like `history`, grouped by handler over the window — the Usage table. */
  historyByHandler: (sinceMs: number) =>
    q(
      [3],
      `SELECT service, handler, sum(turns) AS turns, sum(completed) AS completed, sum(failed) AS failed,
       sum(cu) AS cu, max(p99_ms) AS p99_ms
       FROM sys_handler_history WHERE hour_ms >= $1
       GROUP BY service, handler ORDER BY cu DESC, service, handler`,
      [sinceMs],
    ),
  /** Counters currently non-zero (Flow control) — limit, used, rule currently winning. */
  limits: () =>
    q(
      [44],
      `SELECT range, counter, level, used, "limit", available, rule FROM sys_limit
       ORDER BY used DESC, counter LIMIT 200`,
      [],
    ),
  /** Longest virtual queues and why they are blocked (question 6). */
  vqueues: () =>
    q(
      [6],
      `SELECT range, queue, service, object_key, exclusive, waiting, granted, blocked_on, lock_holder
       FROM sys_vqueue ORDER BY waiting DESC, queue LIMIT 100`,
      [],
    ),
  /**
   * Queues (in RAM) of one service — the "why" banner of the flow-control
   * section: which queue is blocked and why (question 6). Matches the entry on the
   * client (`queueOf`).
   */
  serviceQueues: (service: string) =>
    q(
      [6],
      `SELECT range, scope, limit_key, object_key, exclusive, waiting, blocked_on, lock_holder
       FROM sys_vqueue WHERE service = $1 ORDER BY waiting DESC LIMIT 100`,
      [service],
    ),
  /**
   * The queue of one object, in order (question 41) — the "queued behind whom"
   * banner: who holds the lock (`stage` other than `inbox`), who waits ahead of
   * us. A queue's identity (`QueueName`) includes scope and limit key ⇒
   * invocations that have them filter further (`limit_key` of `sys_invocation`).
   */
  objectQueue: (tenant: string, service: string, key: string, queue: QueueIdent) => {
    const w = queueWhere(tenant, queue, service);
    return q(
      [41],
      `SELECT id, stage, seq, scope, limit_key, range FROM sys_vqueue_entry
       WHERE ${w.sql} AND key = $${w.params.length + 1} ORDER BY seq LIMIT $${w.params.length + 2}`,
      [...w.params, key, 200],
    );
  },
  /** The invocation's own entry in the queue of an unkeyed service (question 41). */
  queueEntry: (tenant: string, service: string, id: string, queue: QueueIdent) => {
    const w = queueWhere(tenant, queue, service);
    return q(
      [41],
      `SELECT id, stage, seq, scope, limit_key, range FROM sys_vqueue_entry
       WHERE ${w.sql} AND id = $${w.params.length + 1}`,
      [...w.params, id],
    );
  },
  /** Per-range scheduler: slots, locks, the `rules_version` actually applied (questions 6, 44). */
  scheduler: () =>
    q(
      [6, 44],
      `SELECT range, eligible, queues, waiting, turns_in_use, max_turns, locks_held, rules_version, blocked
       FROM sys_scheduler ORDER BY range`,
      [],
    ),
  /** Metering for the current hour (`sys_metering` only has this hour) — cluster-admin sums every tenant. */
  meteringNow: () =>
    q(
      [7],
      `SELECT sum(invocations) AS invocations, sum(steps) AS steps, sum(run_ms) AS run_ms,
       sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,
       sum(cost_micros) AS cost_micros, sum(units) AS units FROM sys_metering`,
      [],
    ),
  /** One invocation, point read (detail; peek). */
  invocation: (id: string) =>
    q(
      [35, 18],
      `SELECT id, tenant, service, handler, key, kind, class, status, root, created_at, transitioned_at, ended_at,
              duration_ms, scope, parent_id, schedule, idempotency_key, deployment, tags,
              failure_code, failure_message, attempt, retry_at, last_failure_code, last_failure_message,
              last_failure_at, pinned_deployment, root_id, journal_len, limit_key
       FROM sys_invocation WHERE id = $1`,
      [id],
    ),
  /** Journal headers (no bodies): the list and "what it is waiting for". */
  journalLite: (id: string, from: number) =>
    q(
      [36],
      "SELECT index, name, entry_lite FROM sys_journal WHERE id = $1 AND index >= $2 ORDER BY index",
      [id, from],
    ),
  /** Body of one entry, loaded when opened (two tiers). */
  journalBody: (id: string, index: number) =>
    q([36], "SELECT body FROM sys_journal WHERE id = $1 AND index = $2", [id, index]),
  /** Durable logs after an offset — the polling path when the leader is unreachable. */
  logsAfter: (id: string, offset: number) =>
    q(
      [37],
      "SELECT offset, kind, body FROM sys_log WHERE id = $1 AND offset > $2 ORDER BY offset",
      [id, offset],
    ),
  /** Streams of one invocation (Streams tab) — includes `$log`/`$trace`, the UI filters. */
  streams: (id: string) =>
    q(
      [38],
      `SELECT name, chunks, records, total_bytes, dropped, last_offset FROM sys_stream
       WHERE id = $1 ORDER BY name`,
      [id],
    ),
  /** Durable records of one stream after an offset — polling like `logsAfter`. */
  streamRecords: (id: string, name: string, offset: number) =>
    q(
      [38],
      `SELECT offset, step, attempt, kind, body FROM sys_stream_record
       WHERE id = $1 AND name = $2 AND offset > $3 ORDER BY offset`,
      [id, name, offset],
    ),
  /** Spans of `$trace` (Trace tab) — union of `start` ∪ `end`; the tree is built on the client. */
  spans: (id: string) =>
    q(
      [37],
      `SELECT span_id, parent_id, name, start_ts, end_ts, status, unfinished, start_lost,
              attrs, input_tokens, output_tokens, step, attempt FROM sys_span WHERE id = $1
       ORDER BY span_id`,
      [id],
    ),
  /** The whole tree of a root. */
  tree: (rootId: string) =>
    q(
      [25],
      "SELECT id, parent_id, service, handler, class, status FROM sys_invocation WHERE root_id = $1 ORDER BY id",
      [rootId],
    ),
  /** Direct children. */
  children: (parentId: string) =>
    q(
      [24],
      "SELECT id, service, handler, class, status, created_at FROM sys_invocation WHERE parent_id = $1 ORDER BY created_at DESC, id",
      [parentId],
    ),
  /** Objects of a service (Objects & State) — `ObjectKeys` index, checked on read. */
  objects: (service: string, limit: number) =>
    q(
      [42],
      "SELECT key, scope, has_state, invocations FROM sys_object WHERE service = $1 ORDER BY key LIMIT $2",
      [service, limit],
    ),
  /** State of one object. */
  state: (service: string, key: string) =>
    q(
      [39],
      "SELECT state_key, value FROM sys_state WHERE service = $1 AND key = $2 ORDER BY state_key",
      [service, key],
    ),
  /** Promises of one workflow. */
  promises: (service: string, key: string) =>
    q(
      [40],
      "SELECT name, ok, value, failure_code FROM sys_promise WHERE service = $1 AND key = $2 ORDER BY name",
      [service, key],
    ),
} satisfies Record<string, (...a: never[]) => Query>;

/** Invocation list filters: each field is one fixed WHERE fragment. */
export type InvocationFilter = {
  classes?: InvocationClass[];
  /** Waiting for a retry (has a `retry_at` appointment). */
  retrying?: boolean;
  rootOnly?: boolean;
  service?: string;
  handler?: string;
  key?: string;
  deployment?: string;
  parentId?: string;
  idempotencyKey?: string;
  idPrefix?: string;
  tag?: string;
  createdFromMs?: number;
  createdToMs?: number;
};

/**
 * `LIKE` pattern for a tag: column `tags` is JSON `{"k":"v"}` (question 27), so
 * `k=v` ⇒ `%"k":"v"%`, and a bare `k` ⇒ `%"k":%`. Keys/values go through
 * `JSON.stringify`.
 */
export function tagPattern(tag: string): string {
  const at = tag.indexOf("=");
  if (at < 0) return `%${JSON.stringify(tag)}:%`;
  return `%${JSON.stringify(tag.slice(0, at))}:${JSON.stringify(tag.slice(at + 1))}%`;
}

export type InvocationOrder = "created" | "transitioned" | "heldUp";

const LIST_COLUMNS =
  "id, service, handler, key, class, status, created_at, transitioned_at, attempt, failure_message, last_failure_message";

/**
 * Invocation list by filters. Each fragment declares its question number; values
 * are always parameters. `beforeMs`: page cursor (the timestamp of the last row of
 * the previous page, on the sort column) — the next page reads `<= beforeMs` and
 * drops the ids already seen at that timestamp.
 */
export function invocationList(
  f: InvocationFilter,
  order: InvocationOrder,
  limit: number,
  beforeMs?: number,
): Query {
  const where: string[] = [];
  const params: Param[] = [];
  const questions = new Set<number>([10]);
  const bind = (v: Param) => {
    params.push(v);
    return `$${params.length}`;
  };
  if (f.rootOnly) {
    where.push("root");
    questions.add(11);
  }
  if (f.service) {
    where.push(`service = ${bind(f.service)}`);
    questions.add(12);
  }
  if (f.handler) {
    where.push(`handler = ${bind(f.handler)}`);
    questions.add(12);
  }
  if (f.key !== undefined && f.key !== "") {
    where.push(`key = ${bind(f.key)}`);
    questions.add(20);
  }
  const [only, ...more] = f.classes ?? [];
  if (only !== undefined) {
    where.push(
      more.length === 0
        ? `class = ${bind(only)}`
        : `class IN (${[only, ...more].map((c) => bind(c)).join(", ")})`,
    );
    questions.add(13);
  }
  if (f.retrying) {
    where.push("retry_at IS NOT NULL");
    questions.add(18);
  }
  if (f.deployment) {
    where.push(`deployment = ${bind(f.deployment)}`);
    questions.add(28);
  }
  if (f.parentId) {
    where.push(`parent_id = ${bind(f.parentId)}`);
    questions.add(24);
  }
  if (f.idempotencyKey) {
    where.push(`idempotency_key = ${bind(f.idempotencyKey)}`);
    questions.add(22);
  }
  if (f.idPrefix) {
    // The hex prefix is validated at input; `%` lives in the parameter, not in the SQL.
    where.push(`id LIKE ${bind(`${f.idPrefix.toLowerCase()}%`)}`);
    questions.add(29);
  }
  if (f.tag) {
    where.push(`tags LIKE ${bind(tagPattern(f.tag))}`);
    questions.add(27);
  }
  const col = order === "transitioned" ? "transitioned_at" : "created_at";
  if (f.createdFromMs !== undefined) {
    where.push(`created_at >= to_timestamp_millis(${bind(f.createdFromMs)})`);
    questions.add(30);
  }
  if (f.createdToMs !== undefined) {
    where.push(`created_at < to_timestamp_millis(${bind(f.createdToMs)})`);
    questions.add(30);
  }
  if (beforeMs !== undefined) {
    where.push(
      order === "heldUp"
        ? `${col} >= to_timestamp_millis(${bind(beforeMs)})`
        : `${col} <= to_timestamp_millis(${bind(beforeMs)})`,
    );
    questions.add(30);
  }
  if (order === "transitioned") questions.add(14);
  if (order === "heldUp") questions.add(16);
  // Sort index `[created_at DESC, id ASC]`: "held up" scans backwards ⇒ `ASC, id DESC`.
  const by = order === "heldUp" ? `${col} ASC, id DESC` : `${col} DESC, id`;
  const sql = `SELECT ${LIST_COLUMNS} FROM sys_invocation${where.length ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY ${by} LIMIT ${bind(limit)}`;
  return { questions: [...questions].sort((a, b) => a - b), sql, params };
}

/** Count over the same filters, capped at 10 000 ("≥ 10 000", question 33). */
export function invocationCount(f: InvocationFilter): Query {
  const inner = invocationList(f, "created", COUNT_CAP);
  return {
    questions: [...new Set([...inner.questions, 33])].sort((a, b) => a - b),
    sql: `SELECT count(*) AS n FROM (${inner.sql.replace(/^SELECT .*? FROM/, "SELECT id FROM").replace(/ ORDER BY [^$]*(?= LIMIT)/, "")})`,
    params: inner.params,
  };
}

export const COUNT_CAP = 10_000;
