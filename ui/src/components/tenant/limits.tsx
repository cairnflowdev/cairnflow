// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Tenant limits: `GET /tenant` — limits set by the plan or by the operator in the
// `tenants` registry. Read-only; changing them is the control plane's job
// (`PUT /admin/tenants/{tenant}`).
import { useQuery } from "@tanstack/react-query";
import { QueryError } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

type Limits = components["schemas"]["TenantLimits"];

const DAY = 86_400_000;
const HOUR = 3_600_000;
const MINUTE = 60_000;

/** Retention: the largest unit that divides evenly, text per locale (`Intl`, `unit` style). */
export function retentionLabel(ms: number | null, locale = getLocale()): string {
  if (ms === null) return m.limits_none();
  if (ms === 0) return m.limits_forever();
  const unit = (n: number, u: "day" | "hour" | "minute" | "second") =>
    new Intl.NumberFormat(locale, { style: "unit", unit: u, unitDisplay: "long" }).format(n);
  if (ms % DAY === 0) return unit(ms / DAY, "day");
  if (ms % HOUR === 0) return unit(ms / HOUR, "hour");
  if (ms % MINUTE === 0) return unit(ms / MINUTE, "minute");
  return unit(ms / 1000, "second");
}

/** Bytes in exact multiples of 1024 (a limit must read as the exact number). */
export function bytesLabel(n: number | null, locale = getLocale()): string {
  if (n === null) return m.limits_none();
  const fmt = new Intl.NumberFormat(locale);
  if (n % (1024 * 1024) === 0) return `${fmt.format(n / (1024 * 1024))} MiB`;
  if (n % 1024 === 0) return `${fmt.format(n / 1024)} KiB`;
  return `${fmt.format(n)} B`;
}

/** The registry's `N/S` ⇒ "N per S s". */
export function rateLabel(v: string | null): string {
  if (v === null) return m.limits_none();
  const [n, s] = v.split("/");
  return n && s ? m.limits_rate({ n, seconds: s }) : v;
}

export function LimitTiles({ limits }: { limits: Limits }) {
  const fmt = new Intl.NumberFormat(getLocale());
  const tiles: [string, string, boolean][] = [
    [m.limits_retention(), retentionLabel(limits.retentionMs), limits.retentionMs === null],
    [m.limits_max_payload(), bytesLabel(limits.maxPayload), limits.maxPayload === null],
    [
      m.limits_max_journal(),
      limits.maxJournal === null ? m.limits_none() : fmt.format(limits.maxJournal),
      limits.maxJournal === null,
    ],
    [m.limits_deploy_rate(), rateLabel(limits.deployRate), limits.deployRate === null],
  ];
  return (
    <dl className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
      {tiles.map(([label, value, none]) => (
        <div key={label} className="rounded-md border border-border p-3">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className={none ? "text-lg text-muted-foreground" : "text-lg font-semibold tabular"}>
            {value}
          </dd>
        </div>
      ))}
    </dl>
  );
}

/** The "Tenant limits" section of the Usage screen. */
export function TenantLimits() {
  const q = useQuery({ queryKey: ["tenant"], queryFn: () => ok(rest.GET("/tenant")) });
  return (
    <section aria-labelledby="tenant-limits" className="flex flex-col gap-2">
      <h2 id="tenant-limits" className="text-base font-semibold">
        {m.limits_title({ tenant: q.data?.tenant ?? "…" })}
      </h2>
      <p className="text-sm text-muted-foreground">{m.limits_intro()}</p>
      {q.isError ? (
        <QueryError error={q.error} />
      ) : q.data === undefined ? (
        <Skeleton className="h-20" />
      ) : (
        <LimitTiles limits={q.data.limits} />
      )}
    </section>
  );
}
