// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * Multiplies the counted requests and bytes (`s3-cost.ts`) by a parametric price
 * table (`--pricing file.json`, one entry per provider). Prints markdown, or JSON
 * with `--json`.
 *
 * ```sh
 * node harness/bench/cost.ts bench/s3-model/results/run.jsonl... \
 *   [--pricing file.json]... [--providers a,b] [--json]
 * ```
 *
 * The result is a **formula by workload**, not a single number:
 *
 *   cost/month = 730 · (a_cluster + a_range · R) + Σ_workload (steps/month ÷ 10⁶) · c_workload + GB · p_storage
 *
 * with `a_cluster`, `a_range` (USD/hour) separated from two idle runs with different
 * range counts, `c_workload` (USD/million steps) of each workload shape, and
 * `p_storage` from the price table. Changing the price table changes numbers, not code.
 *
 * The computation is exported as pure functions; the CLI part only runs when the
 * file is invoked directly.
 */

import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** One entry of the price table. Every field except `label` is read leniently: absent ⇒ 0 / default. */
export type Provider = {
  label: string;
  source?: string;
  as_of?: string;
  verify?: boolean;
  /** `store` (default) or `egress` (the transfer fee of the compute platform, not a store). */
  kind?: "store" | "egress";
  /** USD / 1 000 requests, by operation. */
  per_1k?: Record<string, number>;
  /** USD / `storage_unit`-month. */
  storage_gb_month?: number;
  /** `GB` = 10⁹ bytes (default), `GiB` = 2³⁰ bytes. */
  storage_unit?: "GB" | "GiB";
  /** USD / GB (10⁹) of bytes uploaded, every PUT byte (S3 Express). */
  put_gb?: number;
  /** USD / GB (10⁹) of bytes retrieved, every GET byte (S3 Express). */
  get_gb?: number;
  /** USD / GB (10⁹) outbound. In an `egress` entry: the bytes cairn sends to the SDK endpoint. */
  egress_gb?: number;
  /** USD / GB (10⁹) inbound. In an `egress` entry: the bytes the SDK endpoint returns. */
  ingress_gb?: number;
  /** Monthly free allowance of the whole account. */
  free_tier?: { class_a?: number; class_b?: number; storage_gb?: number };
  note?: string;
};
export type Pricing = { note?: string; providers: Record<string, Provider> };

export type Row = {
  scenario: string;
  nodes: number;
  partitions: number;
  seconds: number;
  steps: number;
  invocations: number;
  tenants?: number;
  outcome?: Record<string, number>;
  counts: Record<string, number>;
  storage?: { t: number; bytes: number; objects: number }[];
  /** Idle background measured on the row's own process, before the window (`cost-matrix.ts`) —
   * when present, `marginal` subtracts this instead of an `idle` row of the same size. */
  idle?: { seconds: number; counts: Record<string, number> };
};

/** Billable requests by operation. */
export type Billable = Record<string, number>;
/** Bytes counted in one run. */
export type Bytes = {
  /** Bytes PUT to the store (every tier, every kind of path). */
  put: number;
  /** Bytes GET from the store. */
  get: number;
  /** Bytes cairn sends to the SDK endpoint (every mode). */
  to_sdk: number;
  /** Bytes the SDK endpoint returns (every mode). */
  from_sdk: number;
  /** SDK bytes by transport mode (`BIDI_STREAM`, `REQUEST_RESPONSE`). */
  sdk: Record<string, { to_sdk: number; from_sdk: number }>;
};
/** Requests and bytes of a slice of load (the whole run, the marginal part, or per hour). */
export type Load = { requests: Billable; bytes: Bytes };

export const OPS = ["put", "get", "head", "list", "delete", "copy"] as const;
/** Class A (writes, listings) and Class B (reads). DELETE is free and belongs to no class. */
export const CLASS_A = ["put", "copy", "list"] as const;
export const CLASS_B = ["get", "head"] as const;
export const HOURS_PER_MONTH = 730;
const GB = 1e9;
const GIB = 2 ** 30;

const REQUESTS = "cairnflow_testkit_store_requests_total";
const LIST_PAGES = "cairnflow_testkit_store_list_pages_total";
const PUT_BYTES = "cairnflow_testkit_store_put_bytes_total";
const GET_BYTES = "cairnflow_testkit_store_get_bytes_total";
const SDK_BYTES = "cairnflow_sdk_bytes_total";

/** `name{a="x",b="y"}` → [name, {a: x, b: y}]. */
export function parseKey(key: string): [string, Record<string, string>] {
  const m = /^([a-z_]+)(?:\{(.*)\})?$/.exec(key);
  if (!m) return [key, {}];
  const labels: Record<string, string> = {};
  for (const kv of (m[2] ?? "").matchAll(/(\w+)="([^"]*)"/g)) labels[kv[1]!] = kv[2]!;
  return [m[1]!, labels];
}

/**
 * One result `.jsonl` file → rows. Lines that are not `s3-cost.ts` rows (no
 * `counts` or `seconds`, like the latency phase of `latency-curve`) are skipped.
 */
export function parseRows(text: string): Row[] {
  return text
    .split("\n")
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l) as Partial<Row>)
    .filter((r): r is Row => typeof r.counts === "object" && r.counts !== null && typeof r.seconds === "number" && r.seconds > 0)
    .map((r) => ({ ...r, scenario: r.scenario ?? "?", steps: r.steps ?? 0, invocations: r.invocations ?? 0, nodes: r.nodes ?? 1, partitions: r.partitions ?? 1 }));
}

/**
 * Merges several price tables (a later file overrides same-named entries of an
 * earlier one), then filters by `only` if given. A name in `only` found in no table is an error.
 */
export function mergePricing(tables: Pricing[], only?: string[]): Record<string, Provider> {
  const all: Record<string, Provider> = {};
  for (const t of tables) Object.assign(all, t.providers);
  if (!only || only.length === 0) return all;
  const missing = only.filter((id) => !(id in all));
  if (missing.length > 0) throw new Error(`no such provider: ${missing.join(", ")}`);
  return Object.fromEntries(only.map((id) => [id, all[id]!]));
}

export const isStore = (p: Provider) => (p.kind ?? "store") === "store";
export const isEgress = (p: Provider) => p.kind === "egress";

/** **Billable** requests by operation: LIST by page, rename = copy (+ a free delete). */
export function billable(r: Row): Billable {
  const out: Billable = { put: 0, get: 0, head: 0, list: 0, delete: 0, copy: 0 };
  let listCalls = 0;
  for (const [k, v] of Object.entries(r.counts)) {
    const [name, l] = parseKey(k);
    if (name === REQUESTS) {
      const op = l.op!;
      if (op === "list") listCalls += v;
      else if (op === "put_multipart") out.put! += v;
      else if (op === "rename") out.copy! += v;
      else out[op] = (out[op] ?? 0) + v;
    } else if (name === LIST_PAGES) {
      out.list! += v;
    }
  }
  // An old binary without list_pages: fall back to the number of calls.
  if (out.list === 0) out.list = listCalls;
  return out;
}

/** Bytes of one run. An old row without any family ⇒ 0. */
export function bytes(r: Row): Bytes {
  const out: Bytes = { put: 0, get: 0, to_sdk: 0, from_sdk: 0, sdk: {} };
  for (const [k, v] of Object.entries(r.counts)) {
    const [name, l] = parseKey(k);
    if (name === PUT_BYTES) out.put += v;
    else if (name === GET_BYTES) out.get += v;
    else if (name === SDK_BYTES && (l.dir === "to_sdk" || l.dir === "from_sdk")) {
      out[l.dir] += v;
      const mode = l.mode ?? "?";
      const m = (out.sdk[mode] ??= { to_sdk: 0, from_sdk: 0 });
      m[l.dir] += v;
    }
  }
  return out;
}

export const load = (r: Row): Load => ({ requests: billable(r), bytes: bytes(r) });

/** Applies `fn` to every number of a `Load` (per hour, per step…). */
export function mapLoad(l: Load, fn: (n: number) => number): Load {
  const sdk = Object.fromEntries(
    Object.entries(l.bytes.sdk).map(([m, v]) => [m, { to_sdk: fn(v.to_sdk), from_sdk: fn(v.from_sdk) }]),
  );
  return {
    requests: Object.fromEntries(Object.entries(l.requests).map(([op, n]) => [op, fn(n)])),
    bytes: { put: fn(l.bytes.put), get: fn(l.bytes.get), to_sdk: fn(l.bytes.to_sdk), from_sdk: fn(l.bytes.from_sdk), sdk },
  };
}

/** Multiplies every number of a `Load` by `f`. */
export const scaleLoad = (l: Load, f: number) => mapLoad(l, (n) => n * f);

/** Class A and Class B requests (DELETE not counted). */
export function classes(b: Billable): { a: number; b: number } {
  return {
    a: CLASS_A.reduce((acc, op) => acc + (b[op] ?? 0), 0),
    b: CLASS_B.reduce((acc, op) => acc + (b[op] ?? 0), 0),
  };
}

/**
 * Store request cost (USD): Σ_op requests/1 000 · per_1k, plus
 * `put_gb · PUT bytes/10⁹ + get_gb · GET bytes/10⁹` when `data` is present. An `egress` entry ⇒ 0.
 */
export function requestCost(b: Billable, p: Provider, data?: { put: number; get: number }): number {
  if (!isStore(p)) return 0;
  const req = OPS.reduce((acc, op) => acc + ((b[op] ?? 0) / 1000) * (p.per_1k?.[op] ?? 0), 0);
  return req + dataCost(data, p);
}

/** The store's per-GB fee on PUT/GET bytes (S3 Express). */
export function dataCost(data: { put: number; get: number } | undefined, p: Provider): number {
  if (!data || !isStore(p)) return 0;
  return (data.put / GB) * (p.put_gb ?? 0) + (data.get / GB) * (p.get_gb ?? 0);
}

/** Request + byte cost of a `Load` on a store. */
export const loadCost = (l: Load, p: Provider) => requestCost(l.requests, p, l.bytes);

/** Number of storage units (GB or GiB per `storage_unit`) of `byteCount` bytes. */
export const storageUnits = (byteCount: number, p: Provider) =>
  byteCount / ((p.storage_unit ?? "GB") === "GiB" ? GIB : GB);

/** USD per month to keep `byteCount` bytes (before the free allowance). */
export const storageCost = (byteCount: number, p: Provider) =>
  isStore(p) ? storageUnits(byteCount, p) * (p.storage_gb_month ?? 0) : 0;

/**
 * Transfer fee to the SDK endpoint (USD) of an `egress` entry: bytes cairn sends ×
 * `egress_gb`, bytes the SDK returns × `ingress_gb`. A store ⇒ 0.
 */
export function sdkCost(b: { to_sdk: number; from_sdk: number }, p: Provider): number {
  if (!isEgress(p)) return 0;
  return (b.to_sdk / GB) * (p.egress_gb ?? 0) + (b.from_sdk / GB) * (p.ingress_gb ?? 0);
}

/** Usage over a month: requests, PUT/GET bytes, stored bytes. */
export type Usage = { requests: Billable; put_bytes?: number; get_bytes?: number; storage_bytes?: number };
export type Monthly = {
  /** Before the free allowance. */
  gross: number;
  /** After subtracting `free_tier`. */
  net: number;
  request_gross: number;
  request_net: number;
  /** Per-GB fee on PUT/GET bytes (no free allowance). */
  data: number;
  storage_gross: number;
  storage_net: number;
};

/**
 * One month's bill on one store, before and after the free allowance:
 * billable Class A = max(0, A − class_a), Class B = max(0, B − class_b), storage
 * = max(0, units − storage_gb). Within a class, the free part is split in
 * proportion to each operation's requests (every current price table has one price per class).
 */
export function monthly(u: Usage, p: Provider): Monthly {
  const data = dataCost({ put: u.put_bytes ?? 0, get: u.get_bytes ?? 0 }, p);
  const requestGross = requestCost(u.requests, p);
  const { a, b } = classes(u.requests);
  const free = p.free_tier ?? {};
  const keep = (n: number, f: number | undefined) => (n > 0 ? Math.max(0, n - (f ?? 0)) / n : 0);
  const [ka, kb] = [keep(a, free.class_a), keep(b, free.class_b)];
  const net: Billable = { ...u.requests };
  for (const op of CLASS_A) net[op] = (net[op] ?? 0) * ka;
  for (const op of CLASS_B) net[op] = (net[op] ?? 0) * kb;
  const requestNet = requestCost(net, p);
  const units = isStore(p) ? storageUnits(u.storage_bytes ?? 0, p) : 0;
  const storageGross = units * (p.storage_gb_month ?? 0);
  const storageNet = Math.max(0, units - (free.storage_gb ?? 0)) * (p.storage_gb_month ?? 0);
  return {
    gross: requestGross + data + storageGross,
    net: requestNet + data + storageNet,
    request_gross: requestGross,
    request_net: requestNet,
    data,
    storage_gross: storageGross,
    storage_net: storageNet,
  };
}

/** Stored bytes at the last sample (skipping samples that could not be measured). */
export function storageEnd(r: Row): number | undefined {
  const s = (r.storage ?? []).filter((x) => Number.isFinite(x.bytes));
  return s.at(-1)?.bytes;
}

/** Usage over a month if the row's load ran continuously for 730 hours, stored at the final level. */
export function monthlyUsage(r: Row): Usage {
  const f = (HOURS_PER_MONTH * 3600) / r.seconds;
  const l = scaleLoad(load(r), f);
  return { requests: l.requests, put_bytes: l.bytes.put, get_bytes: l.bytes.get, storage_bytes: storageEnd(r) ?? 0 };
}

/**
 * Idle load per second of the same cluster size: measured directly if there is an
 * idle run with the same node×range, otherwise linearly interpolated by range count from idle runs with the same node count.
 */
export function idlePerSecond(rows: Row[], r: Row): Load | undefined {
  if (r.idle && r.idle.seconds > 0) {
    const own = { ...r, counts: r.idle.counts, seconds: r.idle.seconds };
    return mapLoad(load(own), (n) => n / own.seconds);
  }
  const same = rows.filter((x) => x.scenario === "idle" && x.nodes === r.nodes);
  const exact = same.find((x) => x.partitions === r.partitions);
  const rate = (x: Row) => mapLoad(load(x), (n) => n / x.seconds);
  if (exact) return rate(exact);
  const sorted = [...same].sort((a, b) => a.partitions - b.partitions);
  if (sorted.length < 2) return undefined;
  const [lo, hi] = [rate(sorted[0]!), rate(sorted.at(-1)!)];
  const f = (r.partitions - sorted[0]!.partitions) / (sorted.at(-1)!.partitions - sorted[0]!.partitions);
  return lerp(lo, hi, f);
}

/** lo + f · (hi − lo), number by number. */
function lerp(lo: Load, hi: Load, f: number): Load {
  const n = (a: number, b: number) => a + f * (b - a);
  const modes = new Set([...Object.keys(lo.bytes.sdk), ...Object.keys(hi.bytes.sdk)]);
  const z = { to_sdk: 0, from_sdk: 0 };
  return {
    requests: Object.fromEntries(OPS.map((op) => [op, n(lo.requests[op] ?? 0, hi.requests[op] ?? 0)])),
    bytes: {
      put: n(lo.bytes.put, hi.bytes.put),
      get: n(lo.bytes.get, hi.bytes.get),
      to_sdk: n(lo.bytes.to_sdk, hi.bytes.to_sdk),
      from_sdk: n(lo.bytes.from_sdk, hi.bytes.from_sdk),
      sdk: Object.fromEntries(
        [...modes].map((m) => {
          const [a, b] = [lo.bytes.sdk[m] ?? z, hi.bytes.sdk[m] ?? z];
          return [m, { to_sdk: n(a.to_sdk, b.to_sdk), from_sdk: n(a.from_sdk, b.from_sdk) }];
        }),
      ),
    },
  };
}

/** **Marginal** load: subtract what an idle cluster of the same size emits in the same time (never negative). */
export function marginal(rows: Row[], r: Row): Load | undefined {
  const idle = idlePerSecond(rows, r);
  if (!idle) return undefined;
  const l = load(r);
  const sub = (a: number, b: number) => Math.max(0, a - b * r.seconds);
  return {
    requests: Object.fromEntries(OPS.map((op) => [op, sub(l.requests[op] ?? 0, idle.requests[op] ?? 0)])),
    bytes: {
      put: sub(l.bytes.put, idle.bytes.put),
      get: sub(l.bytes.get, idle.bytes.get),
      // An idle cluster does not call the SDK: the load's SDK bytes are all marginal.
      to_sdk: l.bytes.to_sdk,
      from_sdk: l.bytes.from_sdk,
      sdk: l.bytes.sdk,
    },
  };
}

export const rowLabel = (r: Row) => (r.scenario === "idle" ? `idle ${r.nodes}n×${r.partitions}d` : r.scenario);

/** Machine-readable result of one provider on one row (`--json`). */
export type ProviderResult =
  | {
      kind: "store";
      usd_per_million_steps: number | null;
      usd_per_million_steps_marginal: number | null;
      usd_per_hour: number;
      usd_per_month_gross: number;
      usd_per_month_net: number;
      usd_storage_month_end: number;
    }
  | {
      kind: "egress";
      /** Transfer fee to the SDK endpoint. */
      usd_per_million_steps: number | null;
      usd_per_hour: number;
    };
export type RowResult = {
  file: string;
  scenario: string;
  label: string;
  nodes: number;
  partitions: number;
  seconds: number;
  steps: number;
  invocations: number;
  tenants: number;
  requests: Billable;
  requests_marginal: Billable | null;
  classes: { a: number; b: number };
  bytes: Bytes & { storage_end: number | null };
  providers: Record<string, ProviderResult>;
};

/** Every number of a row for every provider — the `--json` output. */
export function summarize(rows: Row[], providers: Record<string, Provider>, files: string[] = []): RowResult[] {
  return rows.map((r, i) => {
    const l = load(r);
    const m = marginal(rows, r);
    const perStep = (usd: number) => (r.steps > 0 ? (usd / r.steps) * 1e6 : null);
    const perHour = (usd: number) => (usd / r.seconds) * 3600;
    const out: Record<string, ProviderResult> = {};
    for (const [id, p] of Object.entries(providers)) {
      if (isEgress(p)) {
        const c = sdkCost(l.bytes, p);
        out[id] = { kind: "egress", usd_per_million_steps: perStep(c), usd_per_hour: perHour(c) };
      } else {
        const c = loadCost(l, p);
        const mo = monthly(monthlyUsage(r), p);
        out[id] = {
          kind: "store",
          usd_per_million_steps: perStep(c),
          usd_per_million_steps_marginal: m && r.steps > 0 ? perStep(loadCost(m, p)) : null,
          usd_per_hour: perHour(c),
          usd_per_month_gross: mo.gross,
          usd_per_month_net: mo.net,
          usd_storage_month_end: storageCost(storageEnd(r) ?? 0, p),
        };
      }
    }
    return {
      file: files[i] ?? "",
      scenario: r.scenario,
      label: rowLabel(r),
      nodes: r.nodes,
      partitions: r.partitions,
      seconds: r.seconds,
      steps: r.steps,
      invocations: r.invocations,
      tenants: r.tenants ?? 0,
      requests: l.requests,
      requests_marginal: m ? m.requests : null,
      classes: classes(l.requests),
      bytes: { ...l.bytes, storage_end: storageEnd(r) ?? null },
      providers: out,
    };
  });
}

/** Numbers by (label) of a family, the rest grouped. */
function by(r: Row, family: string, label: string, filter: (l: Record<string, string>) => boolean = () => true) {
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(r.counts)) {
    const [name, l] = parseKey(k);
    if (name === family && filter(l)) out[l[label]!] = (out[l[label]!] ?? 0) + v;
  }
  return out;
}

const fmt = (n: number, d = 1) =>
  Number.isFinite(n) ? n.toLocaleString("en-US", { maximumFractionDigits: d, minimumFractionDigits: d }) : "—";
const usd = (n: number) => (n === 0 ? "$0" : n < 0.01 ? `$${n.toPrecision(2)}` : `$${fmt(n, 2)}`);

/** Markdown report, line by line. */
export function report(
  rows: Row[],
  providers: Record<string, Provider>,
  opts: { files: string[]; pricingPaths: string[] },
): string[] {
  const out: string[] = [];
  const log = (s: string) => out.push(s);
  const stores = Object.entries(providers).filter(([, p]) => isStore(p));
  const egress = Object.entries(providers).filter(([, p]) => isEgress(p));

  log(`# Cost model — ${opts.files.join(", ")}\n`);
  log(`Price tables: ${opts.pricingPaths.map((p) => `\`${p}\``).join(", ")}:`);
  for (const [id, p] of Object.entries(providers)) {
    log(`- \`${id}\`: ${p.label} (${p.as_of ?? "?"}${p.verify ? "; **not cross-checked**" : ""})`);
  }

  log("\n## Counted requests\n");
  log("| workload | node×range | seconds | steps | invocation | " + OPS.join(" | ") + " | total/s | MB PUT | MB GET | MB to SDK | MB from SDK |");
  log("|---|---|---|---|---|" + OPS.map(() => "---").join("|") + "|---|---|---|---|---|");
  for (const r of rows) {
    const b = billable(r);
    const y = bytes(r);
    const total = OPS.reduce((a, op) => a + b[op]!, 0);
    log(
      `| ${rowLabel(r)} | ${r.nodes}×${r.partitions} | ${fmt(r.seconds, 0)} | ${r.steps} | ${r.invocations} | ` +
        OPS.map((op) => String(b[op])).join(" | ") +
        ` | ${fmt(total / r.seconds)} | ${fmt(y.put / 1e6, 2)} | ${fmt(y.get / 1e6, 2)} | ${fmt(y.to_sdk / 1e6, 2)} | ${fmt(y.from_sdk / 1e6, 2)} |`,
    );
  }

  log("\n## Requests per million steps (workloads with steps)\n");
  log("| workload | steps/invocation | " + OPS.join(" | ") + " | PUT create | PUT update (CAS) | PUT overwrite | SDK bytes/step |");
  log("|---|---|" + OPS.map(() => "---").join("|") + "|---|---|---|---|");
  for (const r of rows.filter((r) => r.steps > 0)) {
    const b = billable(r);
    const y = bytes(r);
    const k = by(r, "cairnflow_testkit_store_puts_total", "kind");
    const per = (n: number) => fmt((n / r.steps) * 1e6, 0);
    log(
      `| ${rowLabel(r)} | ${fmt(r.steps / Math.max(1, r.invocations), 2)} | ` +
        OPS.map((op) => per(b[op]!)).join(" | ") +
        ` | ${per(k.create ?? 0)} | ${per(k.update ?? 0)} | ${per(k.overwrite ?? 0)} | ${fmt((y.to_sdk + y.from_sdk) / r.steps, 0)} |`,
    );
  }

  log("\n## PUT and GET by path kind (per million steps, or per hour when idle)\n");
  const pathClasses = ["wal", "nodewal", "manifest", "compacted", "lease", "other"];
  log("| workload | op | " + pathClasses.join(" | ") + " |");
  log("|---|---|" + pathClasses.map(() => "---").join("|") + "|");
  for (const r of rows) {
    const scale = r.steps > 0 ? 1e6 / r.steps : 3600 / r.seconds;
    for (const op of ["put", "get", "head", "list", "delete"]) {
      const c = by(r, REQUESTS, "class", (l) => l.op === op);
      if (Object.values(c).every((v) => !v)) continue;
      log(
        `| ${rowLabel(r)}${r.steps > 0 ? "" : " (/hour)"} | ${op} | ` +
          pathClasses.map((cl) => fmt((c[cl] ?? 0) * scale, 0)).join(" | ") +
          " |",
      );
    }
  }

  log("\n## Marginal requests per million steps (idle of the same cluster size subtracted)\n");
  log("| workload | " + OPS.join(" | ") + " | total |");
  log("|---|" + OPS.map(() => "---").join("|") + "|---|");
  for (const r of rows.filter((r) => r.steps > 0)) {
    const m = marginal(rows, r);
    if (!m) continue;
    const per = (n: number) => fmt((n / r.steps) * 1e6, 0);
    log(
      `| ${rowLabel(r)} | ` +
        OPS.map((op) => per(m.requests[op]!)).join(" | ") +
        ` | ${per(OPS.reduce((a, op) => a + m.requests[op]!, 0))} |`,
    );
  }

  log("\n## Request cost\n");
  log("Store includes the per-GB fee on PUT/GET bytes when the price table has one (`put_gb`/`get_gb`, S3 Express). Columns `… egress SDK`: bytes to/from the SDK endpoint × the compute platform's transfer price.\n");
  const egressCols = egress.map(([id]) => `${id} egress SDK`);
  log("| workload | unit | " + [...stores.map(([id]) => id), ...egressCols].join(" | ") + " |");
  log("|---|---|" + [...stores, ...egress].map(() => "---").join("|") + "|");
  for (const r of rows) {
    const l = load(r);
    // Divide first, then multiply (as before) so the rounded numbers do not change.
    const s = (c: number) => usd((c / r.steps) * 1e6);
    const h = (c: number) => usd((c / r.seconds) * 3600);
    if (r.steps > 0) {
      log(
        `| ${rowLabel(r)} | USD / million steps (raw) | ` +
          [...stores.map(([, p]) => s(loadCost(l, p))), ...egress.map(([, p]) => s(sdkCost(l.bytes, p)))].join(" | ") +
          " |",
      );
      const m = marginal(rows, r);
      if (m) {
        log(
          `| ${rowLabel(r)} | USD / million steps (marginal, c_workload) | ` +
            [...stores.map(([, p]) => s(loadCost(m, p))), ...egress.map(([, p]) => s(sdkCost(m.bytes, p)))].join(" | ") +
            " |",
        );
      }
    }
    log(
      `| ${rowLabel(r)} | USD / hour whole cluster | ` +
        [...stores.map(([, p]) => h(loadCost(l, p))), ...egress.map(([, p]) => h(sdkCost(l.bytes, p)))].join(" | ") +
        " |",
    );
  }

  log("\n## Monthly bill if the workload ran continuously (730 hours, stored at the final level)\n");
  log("One account running only this workload. 'after free' subtracts the price table's free allowance (`free_tier`, monthly); for the marginal part of a workload on top of other workloads see the 'before free' column.\n");
  log("| workload | " + stores.map(([id]) => `${id} before free | ${id} after free allowance`).join(" | ") + " |");
  log("|---|" + stores.map(() => "---|---").join("|") + "|");
  for (const r of rows) {
    const u = monthlyUsage(r);
    log(
      `| ${rowLabel(r)} | ` +
        stores
          .map(([, p]) => {
            const mo = monthly(u, p);
            return `${usd(mo.gross)} | ${usd(mo.net)}`;
          })
          .join(" | ") +
        " |",
    );
  }

  // Split the idle cost into a per-cluster and a per-range part from two idle runs with the same node count.
  const idle = rows.filter((r) => r.scenario === "idle").sort((a, b) => a.partitions - b.partitions);
  if (idle.length >= 2 && idle[0]!.nodes === idle.at(-1)!.nodes && idle[0]!.partitions !== idle.at(-1)!.partitions) {
    const [lo, hi] = [idle[0]!, idle.at(-1)!];
    log(`\n## Idle cost: per-cluster part and per-range part (${lo.nodes} node; ${lo.partitions} and ${hi.partitions} ranges)\n`);
    log("| | " + OPS.join(" | ") + " | " + stores.map(([id]) => `${id} USD/month`).join(" | ") + " |");
    log("|---|" + OPS.map(() => "---").join("|") + "|" + stores.map(() => "---").join("|") + "|");
    const perHour = (r: Row) => mapLoad(load(r), (n) => (n / r.seconds) * 3600);
    const [hl, hh] = [perHour(lo), perHour(hi)];
    const dR = hi.partitions - lo.partitions;
    // a_range = (hh − hl) / dR; a_cluster = hl − a_range · R_lo (interpolated to 0 ranges).
    // PUT/GET bytes come along so the per-GB fee (S3 Express) can be split too; an idle
    // cluster has no SDK bytes.
    const diff = (a: Load, b: Load): Load => ({
      requests: Object.fromEntries(OPS.map((op) => [op, (a.requests[op] ?? 0) - (b.requests[op] ?? 0)])),
      bytes: { put: a.bytes.put - b.bytes.put, get: a.bytes.get - b.bytes.get, to_sdk: 0, from_sdk: 0, sdk: {} },
    });
    const rangeRate = mapLoad(diff(hh, hl), (n) => n / dR);
    const base = diff(hl, scaleLoad(rangeRate, lo.partitions));
    for (const [name, v] of [
      [`a_cluster (${lo.nodes} node, requests/hour)`, base],
      ["a_range (each extra range, requests/hour)", rangeRate],
    ] as const) {
      log(
        `| ${name} | ` +
          OPS.map((op) => fmt(v.requests[op]!, 1)).join(" | ") +
          " | " +
          stores.map(([, p]) => usd(loadCost(v, p) * HOURS_PER_MONTH)).join(" | ") +
          " |",
      );
    }
  }

  // The per-tenant part: the `tenants` workload minus idle of the same cluster size, divided by the number
  // of tenants — includes both the per-step part (small, tenants run rarely) and the time part (per-tenant
  // registry refresh). Shown next to the per-step part of the dense workload to tell the two apart.
  const tenantRows = rows.filter((r) => r.scenario === "tenants" && (r.tenants ?? 0) > 0);
  if (tenantRows.length > 0) {
    log("\n## Per-tenant part (`tenants` workload, idle of the same cluster size subtracted)\n");
    log("| tenant | requests/hour per tenant: " + OPS.join(" | ") + " | " + stores.map(([id]) => `${id} USD/month per tenant`).join(" | ") + " |");
    log("|---|" + OPS.map(() => "---").join("|") + "|" + stores.map(() => "---").join("|") + "|");
    for (const r of tenantRows) {
      const m = marginal(rows, r);
      if (!m) continue;
      const per = mapLoad(m, (n) => ((n / r.seconds) * 3600) / r.tenants!);
      log(
        `| ${r.tenants} | ` +
          OPS.map((op) => fmt(per.requests[op]!, 0)).join(" | ") +
          " | " +
          stores.map(([, p]) => usd(loadCost(per, p) * HOURS_PER_MONTH)).join(" | ") +
          " |",
      );
    }
  }

  log("\n## Stored bytes over time\n");
  log("| workload | start (MB, objects) | end (MB, objects) | growth MB/hour | bytes/step | " + stores.map(([id]) => `${id} USD/month at the final level`).join(" | ") + " |");
  log("|---|---|---|---|---|" + stores.map(() => "---").join("|") + "|");
  for (const r of rows) {
    const s = (r.storage ?? []).filter((x) => Number.isFinite(x.bytes));
    if (s.length < 2) continue;
    const [a, z] = [s[0]!, s.at(-1)!];
    const growth = ((z.bytes - a.bytes) / Math.max(1, z.t - a.t)) * 3600;
    log(
      `| ${rowLabel(r)} | ${fmt(a.bytes / 1e6, 2)} MB, ${a.objects} | ${fmt(z.bytes / 1e6, 2)} MB, ${z.objects} | ${fmt(growth / 1e6, 2)} | ` +
        `${r.steps > 0 ? fmt((z.bytes - a.bytes) / r.steps, 0) : "—"} | ` +
        stores.map(([, p]) => usd(storageCost(z.bytes, p))).join(" | ") +
        " |",
    );
  }
  return out;
}

/** CLI: argument parsing. `--pricing` may repeat; `--providers a,b`; `--json`. */
export function parseArgs(argv: string[]): { files: string[]; pricing: string[]; providers: string[]; json: boolean } {
  const files: string[] = [];
  const pricing: string[] = [];
  const providers: string[] = [];
  let json = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === "--pricing") pricing.push(argv[++i]!);
    else if (a === "--providers") providers.push(...argv[++i]!.split(",").map((s) => s.trim()).filter(Boolean));
    else if (a === "--json") json = true;
    else if (a.startsWith("--")) throw new Error(`unknown flag: ${a}`);
    else files.push(a);
  }
  return { files, pricing, providers, json };
}

function main() {
  const here = fileURLToPath(new URL(".", import.meta.url));
  const repoRoot = join(here, "..", "..");
  const args = parseArgs(process.argv.slice(2));
  if (args.files.length === 0) {
    console.error(
      "usage: node harness/bench/cost.ts <result.jsonl>... [--pricing file.json]... [--providers a,b] [--json]",
    );
    process.exit(2);
  }
  const pricingPaths = args.pricing.length > 0 ? args.pricing : [join(repoRoot, "bench", "s3-model", "pricing.json")];
  const providers = mergePricing(
    pricingPaths.map((p) => JSON.parse(readFileSync(p, "utf8")) as Pricing),
    args.providers,
  );
  const perFile = args.files.map((f) => parseRows(readFileSync(f, "utf8")));
  const rows = perFile.flat();
  const rowFiles = args.files.flatMap((f, i) => perFile[i]!.map(() => f));
  const shown = pricingPaths.map((p) => p.replace(repoRoot + "/", ""));
  if (args.json) {
    const meta = Object.fromEntries(
      Object.entries(providers).map(([id, p]) => [
        id,
        { label: p.label, kind: p.kind ?? "store", as_of: p.as_of ?? null, verify: p.verify ?? false },
      ]),
    );
    console.log(JSON.stringify({ pricing: shown, providers: meta, rows: summarize(rows, providers, rowFiles) }, null, 2));
  } else {
    for (const line of report(rows, providers, { files: args.files, pricingPaths: shown })) console.log(line);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
