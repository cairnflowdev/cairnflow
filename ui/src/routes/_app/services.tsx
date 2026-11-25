// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Services: handlers, effective type, current revision,
// per-handler stats (the server's default window).
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Boxes } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { PartialBanner } from "@/components/cairn/partial-banner";
import { QueryError } from "@/components/cairn/query-state";
import { RoleGate } from "@/components/cairn/role-gate";
import { RowButton, rowActionsCell } from "@/components/cairn/row-actions";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Tag } from "@/components/cairn/tag";
import { RunHandler } from "@/components/services/run-handler";
import { ok, rest } from "@/data/api";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { useCapabilities } from "@/data/session";
import { formatMs } from "@/lib/usage";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/services")({ component: Services });

type Stat = {
  service: string;
  handler: string;
  turns: number;
  completed: number;
  failed: number;
  paused: number;
  failed_attempts: number;
  p99_ms: number | null;
};

const TY: Record<string, () => string> = {
  service: m.service_ty_service,
  virtualObject: m.service_ty_object,
  workflow: m.service_ty_workflow,
};

function Services() {
  const services = useQuery({ queryKey: ["services"], queryFn: () => ok(rest.GET("/services")) });
  const stats = useRows<Stat>(queries.handlerStats());
  const fmt = new Intl.NumberFormat(getLocale());
  const caps = useCapabilities();
  const [running, setRunning] = useState<{
    service: string;
    handler: string;
    keyed: boolean;
  } | null>(null);
  const stat = (svc: string, h: string) =>
    stats.data?.rows.find((r) => r.service === svc && r.handler === h);
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-xl font-semibold">{m.nav_services()}</h1>
        <p className="text-sm text-muted-foreground">{m.services_turns_note()}</p>
      </div>
      {services.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : services.isError ? (
        <QueryError error={services.error} />
      ) : services.data.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title={m.services_empty()}
          action={
            <Link to="/" className="text-primary underline-offset-4 hover:underline">
              {m.services_empty_action()}
            </Link>
          }
        />
      ) : (
        <>
          {stats.data ? <PartialBanner warnings={stats.data.warnings} /> : null}
          {services.data.map((s) => (
            <section
              key={s.name}
              aria-labelledby={`svc-${s.name}`}
              className="rounded-md border border-border p-4"
            >
              <div className="flex flex-wrap items-baseline gap-3">
                <h2
                  id={`svc-${s.name}`}
                  className="min-w-0 text-base font-semibold [overflow-wrap:anywhere]"
                >
                  <Link to="/invocations" search={{ service: s.name }} className="hover:underline">
                    {s.name}
                  </Link>
                </h2>
                <Tag tone="default">{TY[s.ty]?.() ?? s.ty}</Tag>
                <span className="text-sm text-muted-foreground">
                  {m.service_revision()}{" "}
                  <Id
                    value={s.deploymentId}
                    wrap={(t) => (
                      <Link to="/deployments/$id" params={{ id: s.deploymentId }}>
                        {t}
                      </Link>
                    )}
                  />
                </span>
              </div>
              {/* Fixed column layout: service cards are stacked, their columns
                  must line up (`auto` sizes by each table's content). */}
              <Table fixed className="min-w-[48rem]" frameClassName="mt-2">
                <colgroup>
                  <col className="w-[22%]" />
                  <col className="w-[12%]" />
                  <col span={5} />
                  <col className="w-28" />
                </colgroup>
                {/* Headers wrap inside their cell (`table-fixed` does not widen a column for
                    its text — "Failed attempts" once overlapped the next column). */}
                <THead className="align-bottom leading-tight [&_th]:break-words [&_th]:whitespace-normal">
                  <TH>{m.filter_handler()}</TH>
                  <TH>{m.handler_ty()}</TH>
                  <TH numeric>{m.stat_turns()}</TH>
                  <TH numeric>{m.stat_completed()}</TH>
                  <TH numeric>{m.kpi_failed()}</TH>
                  <TH numeric>{m.stat_failed_attempts()}</TH>
                  <TH numeric>{m.kpi_p99()}</TH>
                  <TH>
                    <span className="sr-only">{m.actions()}</span>
                  </TH>
                </THead>
                <TBody className="tabular">
                  {s.handlers.map((h) => {
                    const st = stat(s.name, h.name);
                    return (
                      <TRow key={h.name}>
                        <TD priority="primary" className="truncate" title={h.name}>
                          <Link
                            to="/invocations"
                            search={{ service: s.name, handler: h.name }}
                            className="hover:underline"
                          >
                            {h.name}
                          </Link>
                        </TD>
                        <TD className="text-muted-foreground">{h.ty ?? "—"}</TD>
                        <TD numeric>{st ? fmt.format(Number(st.turns)) : "—"}</TD>
                        <TD numeric>{st ? fmt.format(Number(st.completed)) : "—"}</TD>
                        <TD numeric>{st ? fmt.format(Number(st.failed)) : "—"}</TD>
                        <TD priority="hidden" numeric>
                          {st ? fmt.format(Number(st.failed_attempts)) : "—"}
                        </TD>
                        <TD numeric>{st?.p99_ms != null ? formatMs(Number(st.p99_ms)) : "—"}</TD>
                        <TD priority="status" className={cn(rowActionsCell, "text-right")}>
                          <RoleGate have={caps} need="invoke">
                            {(blocked) => (
                              <RowButton
                                aria-label={m.run_handler({ handler: h.name })}
                                disabled={!!blocked}
                                title={blocked ?? undefined}
                                onClick={() =>
                                  setRunning({
                                    service: s.name,
                                    handler: h.name,
                                    keyed: s.ty !== "service",
                                  })
                                }
                              >
                                {m.run_short()}
                              </RowButton>
                            )}
                          </RoleGate>
                        </TD>
                      </TRow>
                    );
                  })}
                </TBody>
              </Table>
            </section>
          ))}
        </>
      )}
      {running ? (
        <RunHandler
          service={running.service}
          handler={running.handler}
          keyed={running.keyed}
          onClose={() => setRunning(null)}
        />
      ) : null}
    </div>
  );
}
