// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The Flow control rule form ⇒ the `POST /rules` body. The server's merge
// semantics: an absent field = **leave untouched**; `null` = reset to unset;
// `unlimited` = explicitly no limit; anything else is an integer ≥ 0. The edit
// dialog pre-fills the current values, so a field that **was set** and is left
// empty is reset, and an unset field left empty is left untouched. Pure — testable.
import type { components } from "@/data/api.gen";

export type PutRule = components["schemas"]["PutRuleRequest"];

export type RuleForm = {
  /** Pattern, or the tenant name when `forTenant`. */
  target: string;
  forTenant: boolean;
  concurrency: string;
  rate: string;
  burst: string;
  description: string;
  disabled: boolean;
  /** Which limits were set when the edit dialog opened — such a field left empty ⇒ send `null` (reset). */
  wasSet?: Partial<Record<"concurrency" | "rate" | "burst", boolean>>;
};

/** Errors per field (keyed by field name). */
export type FormErrors = Partial<
  Record<"target" | "concurrency" | "rate" | "burst", "required" | "number" | "tenantConcurrency">
>;

function cap(v: string): number | "unlimited" | null | undefined {
  const t = v.trim().toLowerCase();
  if (t === "") return undefined;
  if (t === "unlimited") return "unlimited";
  return /^\d+$/.test(t) ? Number(t) : null;
}

export function ruleBody(f: RuleForm): { body: PutRule } | { errors: FormErrors } {
  const errors: FormErrors = {};
  if (!f.target.trim()) errors.target = "required";
  const concurrency = cap(f.concurrency);
  const rate = cap(f.rate);
  if (concurrency === null) errors.concurrency = "number";
  if (f.forTenant && concurrency !== undefined) errors.concurrency = "tenantConcurrency";
  if (rate === null) errors.rate = "number";
  const burst =
    f.burst.trim() === ""
      ? undefined
      : /^[1-9]\d*$/.test(f.burst.trim())
        ? Number(f.burst.trim())
        : null;
  if (burst === null) errors.burst = "number";
  if (Object.keys(errors).length > 0) return { errors };
  const unset = (k: "concurrency" | "rate" | "burst", v: unknown) =>
    v === undefined && f.wasSet?.[k] === true;
  const body: PutRule = {
    ...(f.forTenant ? { tenant: f.target.trim() } : { pattern: f.target.trim() }),
    ...(concurrency !== undefined && concurrency !== null ? { concurrency } : {}),
    ...(!f.forTenant && unset("concurrency", concurrency) ? { concurrency: null } : {}),
    ...(rate !== undefined && rate !== null ? { rate } : {}),
    ...(unset("rate", rate) ? { rate: null } : {}),
    ...(burst != null ? { burst } : {}),
    ...(unset("burst", burst) ? { burst: null } : {}),
    description: f.description,
    disabled: f.disabled,
  };
  return { body };
}

/** Whether the rule book has reached every range: the scheduler's `rules_version` ≥ `version`. */
export function propagation(
  version: number,
  rangeVersions: number[],
): { behind: number; total: number } {
  return { behind: rangeVersions.filter((v) => v < version).length, total: rangeVersions.length };
}

/** Scheduler block reason (`ResourceKind::label` on the server). */
export type BlockReason = "lock" | "concurrency" | "turns" | "rate" | "cooldown" | "other";

/**
 * The `blocked` column of `sys_scheduler` (JSON `[{resource, count}]`) ⇒ a readable
 * list, the three `concurrency_*` levels merged into one, largest first. Not valid
 * JSON ⇒ `null` (the table prints the raw text).
 */
export function blockedReasons(
  raw: string | null,
): { reason: BlockReason; count: number }[] | null {
  if (raw === null || raw === "") return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!Array.isArray(parsed)) return null;
  const sum = new Map<BlockReason, number>();
  for (const e of parsed) {
    const r = (e as { resource?: unknown })?.resource;
    const n = Number((e as { count?: unknown })?.count ?? 0);
    if (typeof r !== "string" || !Number.isFinite(n)) return null;
    const reason: BlockReason = r.startsWith("concurrency")
      ? "concurrency"
      : r === "lock" || r === "turns" || r === "rate" || r === "cooldown"
        ? r
        : "other";
    sum.set(reason, (sum.get(reason) ?? 0) + n);
  }
  return [...sum].map(([reason, count]) => ({ reason, count })).sort((a, b) => b.count - a.count);
}
