// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Per-model table of the Usage screen, split out of the route for testing.
import type { components } from "@/data/api.gen";
import { getLocale } from "@/paraglide/runtime";

export type MeterHour = components["schemas"]["MeteringHour"];

type ModelRow = { model: string | null; input: number; output: number; cost: number };

/**
 * Sums `models` over every hour; the `model: null` row is the tokens with an unknown
 * model (the hour's total minus the attributed part) — shown only when non-zero.
 */
export function byModel(hours: MeterHour[]): ModelRow[] {
  const by = new Map<string, ModelRow>();
  const rest = { model: null, input: 0, output: 0, cost: 0 } as ModelRow;
  for (const h of hours) {
    let input = 0;
    let output = 0;
    let cost = 0;
    for (const [model, u] of Object.entries(h.models)) {
      const r = by.get(model) ?? { model, input: 0, output: 0, cost: 0 };
      r.input += u.inputTokens;
      r.output += u.outputTokens;
      r.cost += u.costMicros;
      by.set(model, r);
      input += u.inputTokens;
      output += u.outputTokens;
      cost += u.costMicros;
    }
    rest.input += Math.max(0, h.inputTokens - input);
    rest.output += Math.max(0, h.outputTokens - output);
    rest.cost += Math.max(0, h.costMicros - cost);
  }
  const rows = [...by.values()].sort((a, b) => b.input + b.output - (a.input + a.output));
  if (rest.input + rest.output + rest.cost > 0) rows.push(rest);
  return rows;
}

/**
 * One duration format for every screen: < 1 s ⇒ "60 ms", from 1 s ⇒ "4.4 s" —
 * Overview and Services used to print the same p99 two different ways.
 */
export function formatMs(ms: number, locale = getLocale()): string {
  return ms >= 1000
    ? `${new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(ms / 1000)} s`
    : `${Math.round(ms)} ms`;
}

/**
 * Displayed cost; `null` when no price table is set — "$0.00" next to tens of
 * thousands of tokens would be a guessed number.
 */
export function costLabel(micros: number | null, priced: boolean, locale = getLocale()) {
  if (!priced) return null;
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  }).format((micros ?? 0) / 1_000_000);
}
