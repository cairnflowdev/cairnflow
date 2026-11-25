// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// One deployment: endpoint, transport, services, current revision, number of
// invocations still pinned (counted across the cluster), link to its invocations. Removable
// (no longer current, no longer pinned) ⇒ a remove button right here — the Overview's
// "removable" item links to this page.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { PackageX } from "lucide-react";
import { useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { confirmText, Id } from "@/components/cairn/id";
import { QueryError } from "@/components/cairn/query-state";
import { RoleGate } from "@/components/cairn/role-gate";
import { Skeleton } from "@/components/cairn/skeleton";
import { Time } from "@/components/cairn/time";
import { DeploymentState } from "@/components/deploy/state";
import { Button, buttonVariants } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ApiError, ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import { deploymentState } from "@/lib/manifest-diff";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/deployments/$id")({ component: Deployment });

function Deployment() {
  const { id } = Route.useParams();
  const caps = useCapabilities();
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [removing, setRemoving] = useState(false);
  const q = useQuery({
    queryKey: ["deployment", id],
    queryFn: () =>
      ok(rest.GET("/deployments/{deploymentId}", { params: { path: { deploymentId: id } } })),
  });
  const remove = useMutation({
    mutationFn: () =>
      ok(rest.DELETE("/deployments/{deploymentId}", { params: { path: { deploymentId: id } } })),
    onSuccess: async () => {
      toast.add({ title: m.deploy_deleted({ id }) });
      await qc.invalidateQueries({ queryKey: ["deployments"] });
      await navigate({ to: "/deployments" });
    },
  });
  const state = q.data ? deploymentState(q.data) : null;
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="flex min-w-0 flex-wrap items-center gap-x-2 text-xl font-semibold">
          {m.deployment_title()}{" "}
          <Id value={id} full className="min-w-0 whitespace-normal break-all" />
        </h1>
        {state ? <DeploymentState state={state} /> : null}
        {state?.kind === "removable" ? (
          <RoleGate have={caps} need="operate">
            {(blocked) => (
              <Button
                variant="destructive"
                className="ml-auto"
                disabled={!!blocked}
                title={blocked ?? undefined}
                onClick={() => setRemoving(true)}
              >
                {m.deployment_remove()}
              </Button>
            )}
          </RoleGate>
        ) : null}
      </div>
      {removing ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setRemoving(false);
              remove.reset();
            }
          }}
          title={m.deployment_remove()}
          description={m.deployment_remove_body({ id })}
          confirmLabel={m.deployment_remove()}
          destructive
          typeToConfirm={confirmText(id)}
          busy={remove.isPending}
          error={remove.isError ? actionError(remove.error) : null}
          onConfirm={() => remove.mutate()}
        />
      ) : null}
      {q.isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : q.isError && q.error instanceof ApiError && q.error.status === 404 ? (
        <EmptyState
          icon={<PackageX />}
          title={m.deployment_missing_title()}
          action={
            <Link to="/deployments" className={buttonVariants({ variant: "outline" })}>
              {m.deployment_missing_back()}
            </Link>
          }
        >
          {m.deployment_missing_body()}
        </EmptyState>
      ) : q.isError ? (
        <QueryError error={q.error} />
      ) : (
        <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2">
          <dt className="text-muted-foreground">{m.deployment_endpoint()}</dt>
          <dd className="font-mono break-all">{q.data.endpoint}</dd>
          <dt className="text-muted-foreground">{m.deployment_mode()}</dt>
          <dd>
            {q.data.protocolMode} · v{q.data.wireVersion}
          </dd>
          <dt className="text-muted-foreground">{m.col_created()}</dt>
          <dd>
            <Time ms={q.data.createdAtMs} />
          </dd>
          <dt className="text-muted-foreground">{m.nav_services()}</dt>
          <dd>{q.data.services.join(", ")}</dd>
          <dt className="text-muted-foreground">{m.deployment_current_for()}</dt>
          <dd>
            {q.data.currentFor.length ? q.data.currentFor.join(", ") : m.deployment_current_none()}
          </dd>
          <dt className="text-muted-foreground">{m.deployment_pinned()}</dt>
          <dd className="tabular">
            {q.data.pinnedInvocations ?? m.deployment_pinned_unknown()}{" "}
            <Link
              to="/invocations"
              search={{ deployment: id, root: 0 }}
              className="ml-2 text-primary hover:underline"
            >
              {m.deployment_invocations()}
            </Link>
          </dd>
        </dl>
      )}
    </div>
  );
}
