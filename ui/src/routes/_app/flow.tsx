// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Flow control (cluster-admin): rule book (`/rules` — merge, with
// `version` and the server's `*` warning), per-tenant rate limits, whether the rules have reached
// every range (`version` compared with the scheduler's `rules_version`), counters
// in use, virtual queues and per-range schedulers.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Gauge, TriangleAlert } from "lucide-react";
import { useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryError, QueryState } from "@/components/cairn/query-state";
import { RequireCapability } from "@/components/cairn/role-gate";
import { RowActions, RowButton, RowDelete, rowActionsCell } from "@/components/cairn/row-actions";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Tag } from "@/components/cairn/tag";
import { Time } from "@/components/cairn/time";
import { RuleDialog } from "@/components/flow/rule-dialog";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { useCapabilities } from "@/data/session";
import { type BlockReason, blockedReasons, propagation } from "@/lib/rules";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/flow")({ component: FlowPage });

/** The whole screen needs the `cluster` capability — opened directly by URL, say why. */
function FlowPage() {
  const caps = useCapabilities();
  return (
    <RequireCapability have={caps} need="cluster">
      <Flow />
    </RequireCapability>
  );
}

type RuleView = components["schemas"]["RuleView"];
type TenantRuleView = components["schemas"]["TenantRuleView"];
type Editing = { forTenant: boolean; initial?: RuleView | TenantRuleView };
type Deleting = { kind: "rule"; pattern: string } | { kind: "tenant"; tenant: string };

type Limit = {
  range: number;
  counter: string;
  level: number;
  used: number;
  limit: number;
  available: number;
  rule: string;
};
type Queue = {
  range: number;
  queue: string;
  service: string;
  object_key: string;
  exclusive: boolean;
  waiting: number;
  granted: number;
  blocked_on: string | null;
  lock_holder: string | null;
};
type Sched = {
  range: number;
  eligible: number;
  queues: number;
  waiting: number;
  turns_in_use: number;
  max_turns: number;
  locks_held: number;
  rules_version: number;
  blocked: string | null;
};

const LIVE = { minMs: 5_000, maxMs: 5_000 } as const;

function Flow() {
  const qc = useQueryClient();
  const toast = useToast();
  const rules = useQuery({ queryKey: ["rules"], queryFn: () => ok(rest.GET("/rules")) });
  const sched = useRows<Sched>(queries.scheduler(), { refresh: LIVE });
  const limits = useRows<Limit>(queries.limits(), { refresh: LIVE });
  const queues = useRows<Queue>(queries.vqueues(), { refresh: LIVE });
  const [editing, setEditing] = useState<Editing | null>(null);
  const [deleting, setDeleting] = useState<Deleting | null>(null);
  const fmt = new Intl.NumberFormat(getLocale());

  const toggle = useMutation({
    mutationFn: (r: RuleView) =>
      ok(rest.POST("/rules", { body: { pattern: r.pattern, disabled: !r.disabled } })),
    onSuccess: async (_, r) => {
      toast.add({
        title: r.disabled
          ? m.rule_enabled({ target: r.pattern })
          : m.rule_disabled_toast({ target: r.pattern }),
      });
      await qc.invalidateQueries({ queryKey: ["rules"] });
    },
    onError: (e) =>
      toast.add({ title: m.action_failed(), description: actionError(e), type: "error" }),
  });
  const remove = useMutation({
    mutationFn: (d: Deleting) =>
      d.kind === "rule"
        ? ok(rest.DELETE("/rules/{pattern}", { params: { path: { pattern: d.pattern } } }))
        : ok(rest.DELETE("/rules", { params: { query: { tenant: d.tenant } } })),
    onSuccess: async (_, d) => {
      setDeleting(null);
      toast.add({ title: m.rule_deleted({ target: d.kind === "rule" ? d.pattern : d.tenant }) });
      await qc.invalidateQueries({ queryKey: ["rules"] });
    },
  });

  const versions = sched.data?.rows.map((r) => r.rules_version) ?? [];
  const prop = rules.data ? propagation(rules.data.version, versions) : null;

  return (
    <div className="flex flex-col gap-6 p-6">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">{m.nav_flow()}</h1>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => setEditing({ forTenant: true })}>
            {m.rule_new_tenant()}
          </Button>
          <Button onClick={() => setEditing({ forTenant: false })}>{m.rule_new()}</Button>
        </div>
      </div>
      {rules.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : rules.isError ? (
        <QueryError error={rules.error} />
      ) : (
        <>
          <p role="status" className="text-sm text-muted-foreground">
            {m.rules_version({ version: String(rules.data.version) })}{" "}
            {prop && prop.total > 0
              ? prop.behind === 0
                ? m.rules_applied({ n: String(prop.total) })
                : m.rules_propagating({ behind: String(prop.behind), n: String(prop.total) })
              : null}
          </p>
          {rules.data.warning ? (
            // The server has only one warning (`STAR_WARNING`, fixed text) — print the sentence
            // translated into the console's language, the server's original sentence in `title`.
            <p
              role="note"
              title={rules.data.warning}
              className="flex gap-2 rounded-md border border-input bg-muted px-3 py-2 text-sm"
            >
              <TriangleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
              <span>{m.rules_star_warning()}</span>
            </p>
          ) : null}
          <section aria-labelledby="flow-rules">
            <h2 id="flow-rules" className="mb-2 text-base font-semibold">
              {m.rules_title()}
            </h2>
            {rules.data.rules.length === 0 ? (
              <EmptyState icon={<Gauge />} title={m.rules_empty()}>
                {m.rules_empty_body()}
              </EmptyState>
            ) : (
              <Table>
                <THead>
                  <TH>{m.rule_pattern()}</TH>
                  <TH numeric>{m.rule_concurrency()}</TH>
                  <TH numeric>{m.rule_rate()}</TH>
                  <TH numeric>{m.rule_burst()}</TH>
                  <TH>{m.rule_description()}</TH>
                  <TH>{m.rule_modified()}</TH>
                  <TH>
                    <span className="sr-only">{m.actions()}</span>
                  </TH>
                </THead>
                <TBody>
                  {rules.data.rules.map((r) => (
                    <TRow
                      key={r.pattern}
                      className={r.disabled ? "text-muted-foreground" : undefined}
                    >
                      <TD priority="primary" className="font-mono">
                        {r.pattern}
                        {r.disabled ? (
                          <span className="ml-2 font-sans text-xs">({m.rule_off()})</span>
                        ) : null}
                      </TD>
                      <TD numeric>{r.concurrency ?? "—"}</TD>
                      <TD numeric>{r.rate ?? "—"}</TD>
                      <TD numeric>{r.burst ?? "—"}</TD>
                      <TD priority="hidden">{r.description}</TD>
                      <TD priority="hidden">
                        <Time ms={r.modifiedMs} />
                      </TD>
                      <TD priority="status" className={rowActionsCell}>
                        <RowActions>
                          <RowButton onClick={() => toggle.mutate(r)} disabled={toggle.isPending}>
                            {r.disabled ? m.rule_enable() : m.rule_disable()}
                          </RowButton>
                          <RowButton onClick={() => setEditing({ forTenant: false, initial: r })}>
                            {m.schedule_edit_short()}
                          </RowButton>
                          <RowDelete
                            label={`${m.schedule_delete()} ${r.pattern}`}
                            title={m.schedule_delete()}
                            onClick={() => setDeleting({ kind: "rule", pattern: r.pattern })}
                          />
                        </RowActions>
                      </TD>
                    </TRow>
                  ))}
                </TBody>
              </Table>
            )}
          </section>
          <section aria-labelledby="flow-tenants">
            <h2 id="flow-tenants" className="mb-2 text-base font-semibold">
              {m.rules_tenants()}
            </h2>
            {rules.data.tenants.length === 0 ? (
              <EmptyState variant="inline" title={m.rules_tenants_empty()} />
            ) : (
              <Table>
                <THead>
                  <TH>{m.rule_tenant()}</TH>
                  <TH numeric>{m.rule_rate()}</TH>
                  <TH numeric>{m.rule_burst()}</TH>
                  <TH>{m.rule_description()}</TH>
                  <TH>
                    <span className="sr-only">{m.actions()}</span>
                  </TH>
                </THead>
                <TBody>
                  {rules.data.tenants.map((t) => (
                    <TRow key={t.tenant}>
                      <TD priority="primary" className="font-mono">
                        {t.tenant}
                      </TD>
                      <TD numeric>{t.rate ?? "—"}</TD>
                      <TD numeric>{t.burst ?? "—"}</TD>
                      <TD priority="hidden">{t.description}</TD>
                      <TD priority="status" className={rowActionsCell}>
                        <RowActions>
                          <RowButton onClick={() => setEditing({ forTenant: true, initial: t })}>
                            {m.schedule_edit_short()}
                          </RowButton>
                          <RowDelete
                            label={`${m.schedule_delete()} ${t.tenant}`}
                            title={m.schedule_delete()}
                            onClick={() => setDeleting({ kind: "tenant", tenant: t.tenant })}
                          />
                        </RowActions>
                      </TD>
                    </TRow>
                  ))}
                </TBody>
              </Table>
            )}
          </section>
        </>
      )}
      <section aria-labelledby="flow-limits">
        <h2 id="flow-limits" className="mb-2 text-base font-semibold">
          {m.flow_limits()}
        </h2>
        <QueryState q={limits}>
          {({ rows }) =>
            rows.length === 0 ? (
              <EmptyState variant="inline" title={m.flow_limits_empty()} />
            ) : (
              <Table>
                <THead>
                  <TH>{m.flow_counter()}</TH>
                  <TH numeric>{m.flow_used()}</TH>
                  <TH numeric>{m.flow_limit()}</TH>
                  <TH numeric>{m.flow_available()}</TH>
                  <TH>{m.flow_rule()}</TH>
                </THead>
                <TBody>
                  {rows.map((r) => (
                    <TRow key={`${r.range}-${r.counter}`}>
                      <TD priority="primary" className="font-mono">
                        {r.counter}
                      </TD>
                      <TD numeric>{fmt.format(r.used)}</TD>
                      <TD numeric>{fmt.format(r.limit)}</TD>
                      <TD numeric>{fmt.format(r.available)}</TD>
                      <TD className="font-mono">{r.rule}</TD>
                    </TRow>
                  ))}
                </TBody>
              </Table>
            )
          }
        </QueryState>
      </section>
      <section aria-labelledby="flow-queues">
        <h2 id="flow-queues" className="mb-2 text-base font-semibold">
          {m.flow_queues()}
        </h2>
        <QueryState q={queues}>
          {({ rows }) =>
            rows.length === 0 ? (
              <EmptyState variant="inline" title={m.flow_queues_empty()} />
            ) : (
              <Table>
                <THead>
                  <TH>{m.flow_queue()}</TH>
                  <TH numeric>{m.flow_waiting()}</TH>
                  <TH numeric>{m.flow_granted()}</TH>
                  <TH>{m.flow_blocked_on()}</TH>
                  <TH>{m.flow_lock_holder()}</TH>
                </THead>
                <TBody>
                  {rows.map((r) => (
                    <TRow key={`${r.range}-${r.queue}`}>
                      <TD priority="primary" className="whitespace-nowrap" title={r.queue}>
                        {r.service}
                        {r.object_key ? (
                          <span className="font-mono text-muted-foreground"> · {r.object_key}</span>
                        ) : null}
                        {r.exclusive ? <Tag className="ml-2">{m.flow_exclusive()}</Tag> : null}
                      </TD>
                      <TD numeric>{fmt.format(r.waiting)}</TD>
                      <TD numeric>{fmt.format(r.granted)}</TD>
                      <TD priority="status" className="whitespace-nowrap">
                        {r.blocked_on ? reasonLabel(r.blocked_on) : "—"}
                      </TD>
                      <TD>
                        {r.lock_holder ? (
                          <Id
                            value={r.lock_holder}
                            wrap={(t) => (
                              <Link
                                to="/invocations/$id"
                                params={{ id: r.lock_holder as string }}
                                className="hover:underline"
                              >
                                {t}
                              </Link>
                            )}
                          />
                        ) : (
                          "—"
                        )}
                      </TD>
                    </TRow>
                  ))}
                </TBody>
              </Table>
            )
          }
        </QueryState>
      </section>
      <section aria-labelledby="flow-sched">
        <h2 id="flow-sched" className="mb-2 text-base font-semibold">
          {m.flow_scheduler()}
        </h2>
        <QueryState q={sched}>
          {({ rows }) => (
            <Table>
              <THead>
                <TH>{m.flow_range()}</TH>
                <TH numeric>{m.flow_turns()}</TH>
                <TH numeric>{m.flow_waiting()}</TH>
                <TH numeric>{m.flow_locks()}</TH>
                <TH numeric>{m.flow_rules_version()}</TH>
                <TH>{m.flow_blocked()}</TH>
              </THead>
              <TBody>
                {rows.map((r) => (
                  <TRow key={r.range}>
                    <TD priority="primary">{r.range}</TD>
                    <TD numeric>
                      {fmt.format(r.turns_in_use)} / {fmt.format(r.max_turns)}
                    </TD>
                    <TD numeric>{fmt.format(r.waiting)}</TD>
                    <TD numeric>{fmt.format(r.locks_held)}</TD>
                    <TD priority="hidden" numeric>
                      {r.rules_version}
                    </TD>
                    <TD priority="status">
                      <Blocked raw={r.blocked} />
                    </TD>
                  </TRow>
                ))}
              </TBody>
            </Table>
          )}
        </QueryState>
      </section>
      {editing ? (
        <RuleDialog
          forTenant={editing.forTenant}
          initial={editing.initial}
          onClose={() => setEditing(null)}
        />
      ) : null}
      {deleting ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setDeleting(null);
              remove.reset();
            }
          }}
          title={m.schedule_delete()}
          description={m.rule_delete_body({
            target: deleting.kind === "rule" ? deleting.pattern : deleting.tenant,
          })}
          confirmLabel={m.schedule_delete()}
          destructive
          typeToConfirm={deleting.kind === "rule" ? deleting.pattern : deleting.tenant}
          busy={remove.isPending}
          error={remove.isError ? actionError(remove.error) : null}
          onConfirm={() => remove.mutate(deleting)}
        />
      ) : null}
    </div>
  );
}

const REASON: Record<BlockReason, () => string> = {
  lock: m.flow_reason_lock,
  concurrency: m.flow_reason_concurrency,
  turns: m.flow_reason_turns,
  rate: m.flow_reason_rate,
  cooldown: m.flow_reason_cooldown,
  other: m.flow_reason_other,
};

/** Label of a blocking reason; an unknown reason (newer server) is printed verbatim. */
function reasonLabel(raw: string): string {
  const r = blockedReasons(JSON.stringify([{ resource: raw, count: 0 }]))?.[0]?.reason;
  return r && r !== "other" ? REASON[r]() : raw;
}

function Blocked({ raw }: { raw: string | null }) {
  const reasons = blockedReasons(raw);
  if (reasons === null) return <span className="font-mono text-xs">{raw}</span>;
  if (reasons.length === 0) return <>—</>;
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <ul className="flex flex-wrap gap-x-3 gap-y-0.5">
      {reasons.map((r) => (
        <li key={r.reason} className="whitespace-nowrap">
          {REASON[r.reason]()}{" "}
          <span className="tabular text-muted-foreground">×{fmt.format(r.count)}</span>
        </li>
      ))}
    </ul>
  );
}
