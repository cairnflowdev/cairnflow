// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Items 4–5 of the "why" banner: whom the invocation is queued behind in the
// virtual queue, and which flow control is blocking that queue. Pure — testable.
//
// - Queue entries (`sys_vqueue_entry`, question 41) by `seq`: `inbox` is waiting,
//   `running`/`suspended`/`paused` hold a slot (for an object: hold the lock).
// - Queues (`sys_vqueue`, question 6, the leader's RAM): `blocked_on` is the label of
//   the resource blocking the head of the queue (`ResourceKind::label` on the
//   server — `lock`, `concurrency_scope|l1|l2`, `turns`, `rate`, `cooldown`).
// - Counters (`sys_limit`, question 44): `[tenant:]scope[/l1[/l2]]`, levels 1–3.

export type QueueEntry = {
  id: string;
  stage: string;
  seq: number;
  scope: string | null;
  limit_key: string | null;
  range: number;
};

export type QueueFacts = {
  /** The invocation's own entry; none ⇒ it is in no queue. */
  mine: QueueEntry;
  /** Holding a slot/lock (not `inbox`), by `seq`. */
  holders: QueueEntry[];
  /** Number of `inbox` entries ahead (only while we are waiting). */
  ahead: number;
  /** Total `inbox` entries of the queue. */
  waiting: number;
};

export function queueFacts(entries: QueueEntry[], id: string): QueueFacts | null {
  const sorted = [...entries].sort((a, b) => Number(a.seq) - Number(b.seq));
  const mine = sorted.find((e) => e.id === id);
  if (!mine) return null;
  const inbox = sorted.filter((e) => e.stage === "inbox");
  return {
    mine,
    holders: sorted.filter((e) => e.stage !== "inbox" && e.id !== id),
    ahead:
      mine.stage === "inbox" ? inbox.filter((e) => Number(e.seq) < Number(mine.seq)).length : 0,
    waiting: inbox.length,
  };
}

export type VQueueRow = {
  range: number;
  scope: string | null;
  limit_key: string | null;
  object_key: string | null;
  exclusive: boolean;
  waiting: number;
  blocked_on: string | null;
  lock_holder: string | null;
};

const same = (a: string | null | undefined, b: string | null | undefined) =>
  (a ?? "") === (b ?? "");

/** The (RAM) queue of an entry: same range, scope, limit key, object key. */
export function queueOf(rows: VQueueRow[], mine: QueueEntry, key: string): VQueueRow | null {
  return (
    rows.find(
      (r) =>
        Number(r.range) === Number(mine.range) &&
        same(r.scope, mine.scope) &&
        same(r.limit_key, mine.limit_key) &&
        same(r.object_key, key),
    ) ?? null
  );
}

export type LimitRow = {
  range: number;
  counter: string;
  level: number;
  used: number;
  limit: number | null;
  available: number | null;
  rule: string | null;
};

const LEVEL: Record<string, number> = {
  concurrency_scope: 1,
  concurrency_l1: 2,
  concurrency_l2: 3,
};

/**
 * The counter blocking the queue when `blocked_on` is a `concurrency_*` level: same
 * range, same level, the queue's `scope[/l1[/l2]]` path cut to that level (counters
 * of tenants other than `default` carry a `tenant:` prefix). Other reasons ⇒ `null`.
 */
export function blockingCounter(q: VQueueRow, limits: LimitRow[]): LimitRow | null {
  const level = q.blocked_on ? LEVEL[q.blocked_on] : undefined;
  if (level === undefined) return null;
  const path = [q.scope ?? "", ...(q.limit_key ? q.limit_key.split("/") : [])]
    .slice(0, level)
    .join("/");
  return (
    limits.find(
      (l) =>
        Number(l.range) === Number(q.range) &&
        Number(l.level) === level &&
        (l.counter === path || l.counter.endsWith(`:${path}`)),
    ) ?? null
  );
}
