// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Workflows: pick a workflow service ⇒ list
// of recent `run` turns (one row per key, click to open) or type a key directly. Opening
// a key ⇒ the `run` turn's id (computed only, `GET /workflow/{s}/{k}/id`), status,
// resolved promises, promises the `run` turn is waiting on (journal), resolve/reject.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { ChevronRight, Workflow } from "lucide-react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id, shorten } from "@/components/cairn/id";
import { QueryError, QueryState } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { StatusBadge } from "@/components/cairn/status-badge";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { type JournalRow, parseRows, waitingOn } from "@/components/invocation/journal";
import { type InvocationDetail, shownStatus } from "@/components/invocations/peek";
import { KeyedToolbar, useKeyedServices, validateKeySearch } from "@/components/keyed/key-form";
import { PromiseForm } from "@/components/keyed/promise-form";
import { Button } from "@/components/ui/button";
import { ok, rest } from "@/data/api";
import { useRows } from "@/data/hooks";
import { useInvocationPages } from "@/data/pages";
import { queries } from "@/data/queries";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/workflows")({
  validateSearch: validateKeySearch,
  component: Workflows,
});

type PromiseRow = { name: string; ok: boolean; value: string | null; failure_code: number | null };

function Workflows() {
  const { service, key } = Route.useSearch();
  const navigate = useNavigate({ from: "/workflows" });
  const { services, list: flows, current, name } = useKeyedServices("workflow", service);
  const runHandler = current?.handlers.find((h) => h.ty === "workflow")?.name;
  const open = (k: string | undefined) => navigate({ search: { service: name, key: k } });
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-xl font-semibold">{m.nav_workflows()}</h1>
        <p className="text-sm text-muted-foreground">{m.workflows_intro()}</p>
      </div>
      {services.isError ? (
        <QueryError error={services.error} />
      ) : services.isPending ? null : flows.length === 0 && !service ? (
        <EmptyState icon={<Workflow aria-hidden />} title={m.workflows_none()}>
          {m.workflows_none_body()}
        </EmptyState>
      ) : (
        <>
          <KeyedToolbar
            list={flows}
            service={name}
            objectKey={key}
            onService={(s) => navigate({ search: { service: s } })}
            onOpen={open}
          />
          {name && key ? (
            <>
              <nav aria-label={m.breadcrumb()} className="flex items-center gap-1.5 text-sm">
                <Link
                  to="/workflows"
                  search={{ service: name }}
                  className="text-muted-foreground hover:text-foreground hover:underline"
                >
                  {m.workflow_all_runs({ service: name })}
                </Link>
                <ChevronRight aria-hidden className="size-4 text-muted-foreground" />
                <span aria-current="page" className="font-mono">
                  {key}
                </span>
              </nav>
              <WorkflowView key={`${name}/${key}`} service={name} objectKey={key} />
            </>
          ) : name && runHandler ? (
            <Runs service={name} handler={runHandler} onOpen={open} />
          ) : null}
        </>
      )}
    </div>
  );
}

/** Recent `run` turns of a workflow service — one row per key, click to open. */
function Runs({
  service,
  handler,
  onOpen,
}: {
  service: string;
  handler: string;
  onOpen: (key: string) => void;
}) {
  const pages = useInvocationPages({ service, handler }, "created");
  if (pages.isPending) return <Skeleton className="h-32 w-full" />;
  if (pages.isError) return <QueryError error={pages.error} />;
  const rows = pages.data.pages.flatMap((p) => p.rows);
  if (rows.length === 0)
    return (
      <EmptyState title={m.workflow_runs_empty({ service })}>
        {m.workflow_runs_empty_body()}
      </EmptyState>
    );
  const last = pages.data.pages.at(-1);
  return (
    <section aria-labelledby="wf-runs" className="flex flex-col gap-2">
      <h2 id="wf-runs" className="text-base font-semibold">
        {m.workflow_runs({ service, handler })}
      </h2>
      <Table>
        <THead>
          <TH>{m.filter_key()}</TH>
          <TH>{m.col_status()}</TH>
          <TH>{m.col_created()}</TH>
          <TH>{m.col_transitioned()}</TH>
          <TH>{m.col_id()}</TH>
        </THead>
        <TBody>
          {rows.map((r) => (
            <TRow
              key={r.id}
              className="cursor-pointer hover:bg-accent"
              onClick={(e) => {
                if ((e.target as HTMLElement).closest("a,button")) return;
                onOpen(r.key);
              }}
            >
              <TD priority="primary" className="font-mono whitespace-nowrap">
                {/* A real link (keyboard, open in new tab); the whole row is clickable too. */}
                <Link to="/workflows" search={{ service, key: r.key }} className="hover:underline">
                  {r.key}
                </Link>
              </TD>
              <TD priority="status">
                <StatusBadge status={shownStatus(r)} />
              </TD>
              <TD>
                <Time ms={Date.parse(r.created_at)} />
              </TD>
              <TD priority="hidden">
                <Time ms={Date.parse(r.transitioned_at)} />
              </TD>
              <TD>
                <Id
                  value={r.id}
                  wrap={(t) => (
                    <Link to="/invocations/$id" params={{ id: r.id }} className="hover:underline">
                      {t}
                    </Link>
                  )}
                />
              </TD>
            </TRow>
          ))}
        </TBody>
      </Table>
      {last?.next ? (
        <Button
          variant="outline"
          className="self-center"
          disabled={pages.isFetchingNextPage}
          onClick={() => void pages.fetchNextPage()}
        >
          {m.load_more()}
        </Button>
      ) : null}
    </section>
  );
}

function WorkflowView({ service, objectKey }: { service: string; objectKey: string }) {
  const wid = useQuery({
    queryKey: ["workflow-id", service, objectKey],
    queryFn: () =>
      ok(
        rest.GET("/workflow/{service}/{key}/id", { params: { path: { service, key: objectKey } } }),
      ),
  });
  const id = wid.data?.id ?? "";
  const run = useRows<InvocationDetail>(queries.invocation(id), { enabled: !!id });
  const journal = useRows<JournalRow>(queries.journalLite(id, 0), { enabled: !!run.data?.rows[0] });
  const done = useRows<PromiseRow>(queries.promises(service, objectKey));
  if (wid.isError) return <QueryError error={wid.error} />;
  const waiting = journal.data
    ? waitingOn(parseRows(journal.data.rows)).filter((e) => e.lite.promise !== undefined)
    : [];
  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-[minmax(0,1fr)_22rem]">
      <div className="flex flex-col gap-4">
        <section aria-labelledby="wf-run">
          <h2 id="wf-run" className="mb-2 text-base font-semibold">
            {m.workflow_run()}
          </h2>
          <QueryState q={run}>
            {({ rows: [r] }) =>
              r ? (
                <p className="flex items-center gap-2">
                  <Id
                    value={r.id}
                    wrap={(t) => (
                      <Link to="/invocations/$id" params={{ id: r.id }}>
                        {t}
                      </Link>
                    )}
                  />
                  <StatusBadge status={shownStatus(r)} />
                </p>
              ) : (
                <EmptyState title={m.workflow_not_started()}>
                  {m.workflow_not_started_body({ id: shorten(id) })}
                </EmptyState>
              )
            }
          </QueryState>
        </section>
        {waiting.length > 0 ? (
          <section aria-labelledby="wf-waiting">
            <h2 id="wf-waiting" className="mb-2 text-base font-semibold">
              {m.workflow_waiting()}
            </h2>
            <ul>
              {waiting.map((e) => (
                <li key={e.index} className="font-mono">
                  {e.lite.promise}
                </li>
              ))}
            </ul>
          </section>
        ) : null}
        <section aria-labelledby="wf-done">
          <h2 id="wf-done" className="mb-2 text-base font-semibold">
            {m.promises_title()}
          </h2>
          <QueryState q={done}>
            {({ rows }) =>
              rows.length === 0 ? (
                <EmptyState variant="inline" title={m.promises_empty()} />
              ) : (
                <ul>
                  {rows.map((p) => (
                    <li key={p.name} className="font-mono">
                      {p.name}: {p.ok ? p.value : `${m.promise_failed()} ${p.failure_code ?? ""}`}
                    </li>
                  ))}
                </ul>
              )
            }
          </QueryState>
        </section>
      </div>
      {/* The key changes when the journal finishes loading ⇒ prefill the name of the pending promise. */}
      <PromiseForm
        key={waiting[0]?.lite.promise ?? ""}
        service={service}
        objectKey={objectKey}
        name={waiting[0]?.lite.promise}
      />
    </div>
  );
}
