// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Audit: who did what to the tenant — `GET /audit`.
// Filter by range (≤ 31 days), actor, action; a `provision`/`cluster` key
// can pick another tenant (`?tenant=`, guarded by the server). The server returns at most 1000 objects and
// `truncated` when it hits the cap — the page must say so, not silently show less.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { Ban, Check, CircleDashed, ScrollText, TriangleAlert, X } from "lucide-react";
import { Fragment, useMemo, useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { Segmented } from "@/components/cairn/segmented";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import {
  type AuditEntry,
  actorLabel,
  entries,
  outcomeTone,
  RANGES,
  type Range,
  rangeBounds,
  targetText,
} from "@/lib/audit";
import { canAny } from "@/lib/roles";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

type Search = { range?: Range; actor?: string; action?: string; tenant?: string };

const text = (v: unknown, max = 200) =>
  typeof v === "string" && v.trim() !== "" ? v.trim().slice(0, max) : undefined;

export const Route = createFileRoute("/_app/audit")({
  validateSearch: (s: Record<string, unknown>): Search => ({
    range: RANGES.includes(s.range as Range) ? (s.range as Range) : undefined,
    actor: text(s.actor),
    action: text(s.action),
    tenant: text(s.tenant, 64),
  }),
  component: Audit,
});

function Audit() {
  const search = Route.useSearch();
  const navigate = useNavigate({ from: "/audit" });
  const caps = useCapabilities();
  const pickTenant = canAny(caps, ["provision", "cluster"]);
  const range = search.range ?? "24h";
  const [draft, setDraft] = useState({
    actor: search.actor ?? "",
    action: search.action ?? "",
    tenant: search.tenant ?? "",
  });
  const set = (s: Partial<Search>) =>
    void navigate({ search: (prev: Search) => ({ ...prev, ...s }) });
  return (
    <div className="flex flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold">{m.nav_audit()}</h1>
      <p className="text-muted-foreground">{m.audit_body()}</p>
      <div className="flex flex-wrap items-end gap-4">
        <div className="flex flex-col gap-1">
          <span className="text-sm font-medium">{m.audit_range()}</span>
          <Segmented
            label={m.audit_range()}
            value={range}
            options={RANGES.map((r) => ({ value: r, label: rangeLabel(r) }))}
            onChange={(r) => set({ range: r })}
          />
        </div>
        <form
          className="flex flex-wrap items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            set({
              actor: text(draft.actor),
              action: text(draft.action),
              tenant: pickTenant ? text(draft.tenant, 64) : undefined,
            });
          }}
        >
          <Field
            id="audit-actor"
            label={m.audit_actor()}
            placeholder={m.audit_actor_hint()}
            value={draft.actor}
            onChange={(actor) => setDraft({ ...draft, actor })}
          />
          <Field
            id="audit-action"
            label={m.audit_action()}
            placeholder="invocation.cancel"
            value={draft.action}
            onChange={(action) => setDraft({ ...draft, action })}
          />
          {pickTenant ? (
            <Field
              id="audit-tenant"
              label={m.audit_tenant()}
              placeholder={m.audit_tenant_own()}
              value={draft.tenant}
              onChange={(tenant) => setDraft({ ...draft, tenant })}
            />
          ) : null}
          <Button type="submit" variant="outline">
            {m.audit_apply()}
          </Button>
        </form>
      </div>
      <Records
        range={range}
        actor={search.actor}
        action={search.action}
        tenant={pickTenant ? search.tenant : undefined}
      />
    </div>
  );
}

function Field({
  id,
  label,
  value,
  placeholder,
  onChange,
}: {
  id: string;
  label: string;
  value: string;
  placeholder?: string;
  onChange: (v: string) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        className="h-8 w-48"
        value={value}
        placeholder={placeholder}
        spellCheck={false}
        onChange={(e) => onChange(e.target.value)}
      />
    </div>
  );
}

function rangeLabel(r: Range): string {
  switch (r) {
    case "1h":
      return m.audit_range_1h();
    case "24h":
      return m.audit_range_24h();
    case "7d":
      return m.audit_range_7d();
    case "31d":
      return m.audit_range_31d();
  }
}

function Records({
  range,
  actor,
  action,
  tenant,
}: {
  range: Range;
  actor?: string;
  action?: string;
  tenant?: string;
}) {
  const q = useQuery({
    queryKey: ["audit", range, actor, action, tenant],
    queryFn: () => {
      const { from, to } = rangeBounds(range, Date.now());
      return ok(rest.GET("/audit", { params: { query: { from, to, actor, action, tenant } } }));
    },
  });
  const rows = useMemo(() => (q.data ? entries(q.data.records) : []), [q.data]);
  if (q.isPending) return <Skeleton className="h-32 w-full" />;
  if (q.isError) return <QueryError error={q.error} />;
  return (
    <section aria-labelledby="audit-records" className="flex flex-col gap-2">
      <h2 id="audit-records" className="text-base font-semibold">
        {m.audit_records({ tenant: q.data.tenant })}
      </h2>
      {q.data.truncated ? (
        <div
          role="status"
          className="flex gap-2 rounded-md border border-input bg-muted px-3 py-2 text-sm"
        >
          <TriangleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
          <div>
            <p className="font-medium">{m.audit_truncated_title()}</p>
            <p className="text-muted-foreground">{m.audit_truncated_body()}</p>
          </div>
        </div>
      ) : null}
      {rows.length === 0 ? (
        <EmptyState icon={<ScrollText />} title={m.audit_empty()} />
      ) : (
        <AuditTable rows={rows} />
      )}
    </section>
  );
}

function AuditTable({ rows }: { rows: AuditEntry[] }) {
  const [open, setOpen] = useState<string | null>(null);
  return (
    <Table>
      <THead>
        <TH>{m.audit_col_time()}</TH>
        <TH>{m.audit_col_actor()}</TH>
        <TH>{m.audit_col_action()}</TH>
        <TH>{m.audit_col_target()}</TH>
        <TH>{m.audit_col_outcome()}</TH>
        <TH>
          <span className="sr-only">{m.audit_details()}</span>
        </TH>
      </THead>
      <TBody>
        {rows.map((e) => {
          const expanded = open === e.key;
          const detailsId = `audit-${e.key}`;
          return (
            <Fragment key={e.key}>
              <TRow className="align-top">
                <TD className="whitespace-nowrap">
                  <Time ms={e.ts} />
                </TD>
                <TD>
                  <span className="font-medium">{actorLabel(e.actor)}</span>
                  {e.actor.role ? (
                    <span className="ml-1 text-xs text-muted-foreground">{e.actor.role}</span>
                  ) : null}
                </TD>
                <TD priority="primary" className="font-mono text-xs">
                  {e.action}
                </TD>
                <TD className="max-w-[28rem] truncate font-mono text-xs">{targetText(e.target)}</TD>
                <TD priority="status">
                  <Outcome entry={e} />
                </TD>
                <TD priority="status" className="py-1 text-right">
                  <Button
                    variant="ghost"
                    size="sm"
                    aria-expanded={expanded}
                    aria-controls={detailsId}
                    onClick={() => setOpen(expanded ? null : e.key)}
                  >
                    {m.audit_details()}
                  </Button>
                </TD>
              </TRow>
              {expanded ? (
                <tr id={detailsId}>
                  <td colSpan={6} className="pb-3">
                    <Details entry={e} />
                  </td>
                </tr>
              ) : null}
            </Fragment>
          );
        })}
      </TBody>
    </Table>
  );
}

const TONE = {
  ok: { icon: Check, cls: "bg-status-completed text-status-completed-foreground" },
  pending: { icon: CircleDashed, cls: "bg-status-suspended text-status-suspended-foreground" },
  bad: { icon: X, cls: "bg-status-failed text-status-failed-foreground" },
  neutral: { icon: null, cls: "bg-muted text-muted-foreground" },
} as const;

/** Outcome: always icon + the server's text; merged records (`denied`, `noop`) with the count. */
function Outcome({ entry: e }: { entry: AuditEntry }) {
  const tone = TONE[outcomeTone(e.outcome)];
  const Icon = e.source === "denied" ? Ban : tone.icon;
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center gap-1 rounded-sm px-1.5 text-xs font-medium whitespace-nowrap",
        "forced-colors:border forced-colors:border-[CanvasText]",
        tone.cls,
      )}
    >
      {Icon ? <Icon aria-hidden className="size-3" /> : null}
      <span>{e.outcome}</span>
      {e.status != null ? <span className="tabular">{e.status}</span> : null}
      {e.count != null ? (
        <span className="tabular">{m.audit_denied_count({ count: String(e.count) })}</span>
      ) : null}
    </span>
  );
}

function Details({ entry: e }: { entry: AuditEntry }) {
  const rows: [string, string][] = [
    [m.audit_detail_source(), sourceLabel(e.source)],
    [m.audit_col_target(), targetText(e.target) || "—"],
    [m.audit_detail_actor(), JSON.stringify(e.actor)],
    [m.audit_detail_ip(), e.ip ?? "—"],
    [m.audit_detail_agent(), e.userAgent ?? "—"],
    [m.audit_detail_request(), e.requestId ?? "—"],
  ];
  return (
    <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 rounded-md bg-muted px-3 py-2 text-xs">
      {rows.map(([k, v]) => (
        <Fragment key={k}>
          <dt className="text-muted-foreground">{k}</dt>
          <dd className="font-mono break-all">{v}</dd>
        </Fragment>
      ))}
    </dl>
  );
}

function sourceLabel(s: AuditEntry["source"]): string {
  switch (s) {
    case "row":
      return m.audit_source_row();
    case "object":
      return m.audit_source_object();
    case "denied":
      return m.audit_source_denied();
    case "noop":
      return m.audit_source_noop();
    default:
      return s;
  }
}
