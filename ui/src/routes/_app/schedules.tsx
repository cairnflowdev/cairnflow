// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Server-side schedules: list, next fire, last run;
// create/edit, pause/resume (toast), fire now (confirm), delete (type the name).
// Reading: `read` or `invoke`; every change needs `invoke` — a `view`
// key sees the list, buttons disabled with a reason.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { CalendarClock } from "lucide-react";
import { useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { RoleGate } from "@/components/cairn/role-gate";
import { RowActions, RowButton, RowDelete, rowActionsCell } from "@/components/cairn/row-actions";
import { Skeleton } from "@/components/cairn/skeleton";
import { StatusBadge } from "@/components/cairn/status-badge";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { ScheduleForm } from "@/components/schedules/schedule-form";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { useCapabilities } from "@/data/session";
import { describeCron, describeEvery, weekdayName } from "@/lib/cron";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

const fmt = (n: number) => new Intl.NumberFormat(getLocale()).format(n);

export const Route = createFileRoute("/_app/schedules")({ component: Schedules });

type Schedule = components["schemas"]["Schedule"];

/** Most recent run — `last` has at most 10 entries, newest last. */
const latest = (s: Schedule) => s.last.at(-1);

function when(s: Schedule): string {
  if (s.cron)
    return `${describeCron(s.cron, weekdayName(getLocale())) ?? s.cron} (${s.timeZone ?? "UTC"})`;
  return describeEvery(s.everyMs ?? 0);
}

function Schedules() {
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const caps = useCapabilities();
  const list = useQuery({ queryKey: ["schedules"], queryFn: () => ok(rest.GET("/schedules")) });
  const [editing, setEditing] = useState<Schedule | "new" | null>(null);
  const [confirm, setConfirm] = useState<{ kind: "trigger" | "delete"; s: Schedule } | null>(null);
  const done = async () => qc.invalidateQueries({ queryKey: ["schedules"] });
  const pause = useMutation({
    mutationFn: (s: Schedule) =>
      s.paused
        ? ok(rest.POST("/schedules/{name}/resume", { params: { path: { name: s.name } } }))
        : ok(rest.POST("/schedules/{name}/pause", { params: { path: { name: s.name } } })),
    onSuccess: async (s) => {
      toast.add({
        title: s.paused
          ? m.schedule_paused({ name: s.name })
          : m.schedule_resumed({ name: s.name }),
      });
      await done();
    },
    onError: (e) =>
      toast.add({ title: m.action_failed(), description: actionError(e), type: "error" }),
  });
  const act = useMutation({
    mutationFn: async (c: { kind: "trigger" | "delete"; s: Schedule }) =>
      c.kind === "trigger"
        ? ok(rest.POST("/schedules/{name}/trigger", { params: { path: { name: c.s.name } } }))
        : ok(rest.DELETE("/schedules/{name}", { params: { path: { name: c.s.name } } })),
    onSuccess: async (out, c) => {
      setConfirm(null);
      await done();
      if (c.kind === "trigger" && out && "id" in out && "created" in out) {
        toast.add({ title: m.schedule_triggered({ name: c.s.name }) });
        await navigate({ to: "/invocations/$id", params: { id: out.id } });
      } else {
        toast.add({ title: m.schedule_deleted({ name: c.s.name }) });
      }
    },
  });

  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex items-baseline justify-between">
        <h1 className="text-xl font-semibold">{m.nav_schedules()}</h1>
        <RoleGate have={caps} need="invoke">
          {(blocked) => (
            <Button
              disabled={!!blocked}
              title={blocked ?? undefined}
              onClick={() => setEditing("new")}
            >
              {m.schedule_new()}
            </Button>
          )}
        </RoleGate>
      </div>
      {list.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : list.isError ? (
        <QueryError error={list.error} />
      ) : list.data.schedules.length === 0 ? (
        <EmptyState icon={<CalendarClock />} title={m.schedules_empty()}>
          {m.schedules_empty_body()}
        </EmptyState>
      ) : (
        <>
          {list.data.partial ? (
            <p role="status" className="rounded-md border border-input bg-muted px-3 py-2">
              {m.schedules_partial({ nodes: list.data.missing.join(", ") })}
            </p>
          ) : null}
          <Table>
            <THead>
              <TH>{m.schedule_name()}</TH>
              <TH>{m.col_target()}</TH>
              <TH>{m.schedule_when()}</TH>
              <TH>{m.schedule_next()}</TH>
              <TH>{m.schedule_last()}</TH>
              <TH numeric>{m.schedule_fired()}</TH>
              <TH>
                <span className="sr-only">{m.actions()}</span>
              </TH>
            </THead>
            <TBody>
              {list.data.schedules.map((s) => (
                <TRow key={s.name} className={cn(s.paused && "text-muted-foreground")}>
                  <TD priority="primary" className="font-medium whitespace-nowrap">
                    <span className="inline-flex items-center gap-2">
                      {s.name}
                      {s.paused ? <StatusBadge status="paused" /> : null}
                    </span>
                  </TD>
                  <TD className="whitespace-nowrap">
                    <Link
                      to="/invocations"
                      search={{ service: s.target.service, handler: s.target.handler }}
                      className="hover:underline"
                    >
                      {s.target.service}/{s.target.handler}
                    </Link>
                    {s.target.key ? (
                      <span className="text-muted-foreground"> · {s.target.key}</span>
                    ) : null}
                  </TD>
                  <TD>{when(s)}</TD>
                  <TD className="whitespace-nowrap">
                    {s.nextAt ? (
                      <Time ms={s.nextAt} />
                    ) : (
                      <span className="text-muted-foreground">
                        {s.paused ? m.schedule_next_paused() : "—"}
                      </span>
                    )}
                  </TD>
                  <TD priority="hidden">
                    <LastRun s={s} />
                  </TD>
                  <TD priority="hidden" numeric className="whitespace-nowrap">
                    {fmt(s.fired)}
                    {/* Missed / skipped are rare — printed only when present, as a reminder. */}
                    {s.missed > 0 ? (
                      <span className="ml-2 text-xs text-status-paused-foreground">
                        {m.schedule_missed_n({ n: fmt(s.missed) })}
                      </span>
                    ) : null}
                    {s.skipped > 0 ? (
                      <span className="ml-2 text-xs text-muted-foreground">
                        {m.schedule_skipped_n({ n: fmt(s.skipped) })}
                      </span>
                    ) : null}
                  </TD>
                  <TD priority="status" className={rowActionsCell}>
                    <RoleGate have={caps} need="invoke">
                      {(blocked) =>
                        blocked ? (
                          // Read-only key: one line of reason instead of four disabled buttons on every row.
                          <RowActions>
                            <span className="px-2 text-xs text-muted-foreground">{blocked}</span>
                          </RowActions>
                        ) : (
                          <RowActions>
                            <RowButton onClick={() => pause.mutate(s)} disabled={pause.isPending}>
                              {s.paused ? m.schedule_resume() : m.schedule_pause()}
                            </RowButton>
                            <RowButton onClick={() => setConfirm({ kind: "trigger", s })}>
                              {m.schedule_trigger()}
                            </RowButton>
                            <RowButton onClick={() => setEditing(s)}>
                              {m.schedule_edit_short()}
                            </RowButton>
                            <RowDelete
                              label={`${m.schedule_delete()} ${s.name}`}
                              title={m.schedule_delete()}
                              onClick={() => setConfirm({ kind: "delete", s })}
                            />
                          </RowActions>
                        )
                      }
                    </RoleGate>
                  </TD>
                </TRow>
              ))}
            </TBody>
          </Table>
        </>
      )}
      {editing ? (
        <ScheduleForm
          initial={editing === "new" ? undefined : editing}
          onClose={() => setEditing(null)}
        />
      ) : null}
      {confirm ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setConfirm(null);
              act.reset();
            }
          }}
          title={confirm.kind === "trigger" ? m.schedule_trigger() : m.schedule_delete()}
          description={
            confirm.kind === "trigger"
              ? m.schedule_trigger_body({ name: confirm.s.name })
              : m.schedule_delete_body({ name: confirm.s.name })
          }
          confirmLabel={confirm.kind === "trigger" ? m.schedule_trigger() : m.schedule_delete()}
          destructive={confirm.kind === "delete"}
          typeToConfirm={confirm.kind === "delete" ? confirm.s.name : undefined}
          busy={act.isPending}
          error={act.isError ? actionError(act.error) : null}
          onConfirm={() => act.mutate(confirm)}
        />
      ) : null}
    </div>
  );
}

/** Last run: the invocation's status (or "skipped") and the fire time — the fire time is a
 *  link to the invocation, saving an id column. */
function LastRun({ s }: { s: Schedule }) {
  const last = latest(s);
  if (!last) return <span className="text-muted-foreground">—</span>;
  return (
    <span className="flex items-center gap-2 whitespace-nowrap">
      {last.status ? (
        <StatusBadge status={last.status} />
      ) : last.outcome === "skipped" ? (
        <span className="text-xs text-muted-foreground">{m.schedule_outcome_skipped()}</span>
      ) : null}
      <Link to="/invocations/$id" params={{ id: last.id }} className="hover:underline">
        <Time ms={last.at} />
      </Link>
    </span>
  );
}
