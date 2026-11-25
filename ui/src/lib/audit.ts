// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The tenant's audit log: `GET /audit` returns raw objects — rows in the range
// (`row`), intent/outcome pairs of actions on objects (`intent`/`done`, same name
// root), denials aggregated per minute (`denied`, with `count`), no-op outcomes
// aggregated per minute (`noop`, with `count`). No ordering guarantee. Here: pair
// them up, sort by `ts`, text for the reader. Pure — tested in `audit.test.ts`.
import type { components } from "@/data/api.gen";

export type AuditRecord = components["schemas"]["AuditRecord"];

export type Actor = { type?: string; id?: string; role?: string; email?: string; name?: string };

/** One table row: an action (`intent` + `done` paired) or an aggregated denial. */
export type AuditEntry = {
  key: string;
  ts: number;
  tenant: string;
  action: string;
  actor: Actor;
  target: unknown;
  /** The server's outcome; an intent with no `done` yet ⇒ `pending`. */
  outcome: string;
  status?: number | null;
  count?: number | null;
  ip?: string | null;
  userAgent?: string | null;
  requestId?: string | null;
  /** `row` (same commit as the action), `object` (intent + outcome), `denied`, `noop` (aggregated). */
  source: "row" | "object" | "denied" | "noop" | "other";
};

const stem = (name: string) => name.replace(/\.(intent|done)$/, "");

/** Pairs `intent`/`done` with the same root into one row; newest first. */
export function entries(records: readonly AuditRecord[]): AuditEntry[] {
  const pairs = new Map<string, { intent?: AuditRecord; done?: AuditRecord }>();
  const out: AuditEntry[] = [];
  for (const r of records) {
    if (r.kind === "intent" || r.kind === "done") {
      const k = stem(r.name);
      const p = pairs.get(k) ?? {};
      p[r.kind] = r;
      pairs.set(k, p);
    } else {
      const gathered = r.kind === "row" || r.kind === "denied" || r.kind === "noop";
      out.push(entry(r, r.name, gathered ? r.kind : "other"));
    }
  }
  for (const [k, { intent, done }] of pairs) {
    // The intent carries the start time; the outcome carries `outcome`/`status`.
    const base = intent ?? (done as AuditRecord);
    const e = entry(base, k, "object");
    if (done) {
      e.outcome = done.outcome;
      e.status = done.status;
    } else {
      e.outcome = "pending";
    }
    out.push(e);
  }
  return out.sort((a, b) => b.ts - a.ts || a.key.localeCompare(b.key));
}

function entry(r: AuditRecord, key: string, source: AuditEntry["source"]): AuditEntry {
  return {
    key,
    ts: r.ts,
    tenant: r.tenant,
    action: r.action,
    actor: (r.actor ?? {}) as Actor,
    target: r.target,
    outcome: r.outcome,
    status: r.status,
    count: r.count,
    ip: r.ip,
    userAgent: r.userAgent,
    requestId: r.requestId,
    source,
  };
}

/** Actor for the reader: email/name of an SSO user, key id, or kind. */
export function actorLabel(a: Actor): string {
  if (a.type === "user") return a.email || a.name || a.id || "user";
  return a.id || a.type || "—";
}

/** Target: `{k: v}` ⇒ `k=v, …`; strings unchanged; id arrays ⇒ joined with commas. */
export function targetText(t: unknown): string {
  if (t === null || t === undefined) return "";
  if (typeof t === "string" || typeof t === "number" || typeof t === "boolean") return String(t);
  if (Array.isArray(t)) return t.map(targetText).join(", ");
  if (typeof t === "object") {
    return Object.entries(t as Record<string, unknown>)
      .filter(([, v]) => v !== null && v !== undefined)
      .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`)
      .join(", ");
  }
  return "";
}

export type Tone = "ok" | "pending" | "bad" | "neutral";

/** Tone of the outcome — always paired with a text label, never colour alone. */
export function outcomeTone(outcome: string): Tone {
  switch (outcome) {
    case "ok":
      return "ok";
    case "pending":
      return "pending";
    case "error":
    case "denied":
    case "refused":
      return "bad";
    default:
      return "neutral";
  }
}

export const RANGES = ["1h", "24h", "7d", "31d"] as const;
export type Range = (typeof RANGES)[number];

const SPAN: Record<Range, number> = {
  "1h": 3_600_000,
  "24h": 86_400_000,
  "7d": 7 * 86_400_000,
  "31d": 31 * 86_400_000,
};

/** `from`/`to` (ms) of a range counted back from `now`; the server caps it at 31 days. */
export function rangeBounds(range: Range, now: number): { from: number; to: number } {
  return { from: now - SPAN[range], to: now };
}
