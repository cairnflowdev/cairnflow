// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Deployments: read-only. "Drained, removable" = no longer
// the current revision of any service and no invocation still pinned to it.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { Rocket } from "lucide-react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryError } from "@/components/cairn/query-state";
import { RoleGate } from "@/components/cairn/role-gate";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { DeploymentState } from "@/components/deploy/state";
import { Button } from "@/components/ui/button";
import { ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import { deploymentState } from "@/lib/manifest-diff";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/deployments/")({ component: Deployments });

function useDeployments() {
  return useQuery({
    queryKey: ["deployments", "pinned"],
    queryFn: () => ok(rest.GET("/deployments", { params: { query: { pinned: "true" } } })),
  });
}

function Deployments() {
  const q = useDeployments();
  const caps = useCapabilities();
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex items-baseline justify-between">
        <h1 className="text-xl font-semibold">{m.nav_deployments()}</h1>
        <RoleGate have={caps} need="operate">
          {(blocked) =>
            blocked ? (
              <Button disabled title={blocked}>
                {m.deploy_open()}
              </Button>
            ) : (
              <Link
                to="/deployments/deploy"
                className="inline-flex h-8 items-center rounded-lg bg-primary px-3 text-sm font-medium text-primary-foreground hover:bg-primary-hover"
              >
                {m.deploy_open()}
              </Link>
            )
          }
        </RoleGate>
      </div>
      {q.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : q.isError ? (
        <QueryError error={q.error} />
      ) : q.data.length === 0 ? (
        <EmptyState
          icon={<Rocket />}
          title={m.deployments_empty()}
          action={
            <Link to="/" className="text-primary underline-offset-4 hover:underline">
              {m.services_empty_action()}
            </Link>
          }
        />
      ) : (
        <Table>
          <THead>
            <TH>{m.col_id()}</TH>
            <TH>{m.col_status()}</TH>
            <TH>{m.deployment_endpoint()}</TH>
            <TH>{m.deployment_mode()}</TH>
            <TH>{m.nav_services()}</TH>
            <TH numeric>{m.deployment_pinned()}</TH>
            <TH>{m.col_created()}</TH>
          </THead>
          <TBody>
            {[...q.data]
              .sort((a, b) => b.createdAtMs - a.createdAtMs)
              .map((d) => (
                <TRow key={d.id}>
                  <TD priority="primary">
                    <Id
                      value={d.id}
                      full
                      wrap={(t) => (
                        <Link
                          to="/deployments/$id"
                          params={{ id: d.id }}
                          className="hover:underline"
                        >
                          {t}
                        </Link>
                      )}
                    />
                  </TD>
                  <TD priority="status">
                    <DeploymentState state={deploymentState(d)} />
                  </TD>
                  <TD className="max-w-64 truncate font-mono text-xs" title={d.endpoint}>
                    {d.endpoint}
                  </TD>
                  <TD priority="hidden" className="whitespace-nowrap">
                    {d.protocolMode} · v{d.wireVersion}
                  </TD>
                  <TD>
                    {/* Wrap instead of widening the table: the status and pinned-count columns
                          must be visible without horizontal scrolling. */}
                    <span className="flex max-w-80 flex-wrap gap-x-2">
                      {d.services.map((s) => (
                        <span
                          key={s}
                          className={
                            d.currentFor.includes(s) ? "font-medium" : "text-muted-foreground"
                          }
                        >
                          {s}
                        </span>
                      ))}
                    </span>
                  </TD>
                  <TD numeric priority="hidden" className="whitespace-nowrap">
                    {d.pinnedInvocations ?? "—"}
                  </TD>
                  <TD>
                    <Time ms={d.createdAtMs} />
                  </TD>
                </TRow>
              ))}
          </TBody>
        </Table>
      )}
    </div>
  );
}
