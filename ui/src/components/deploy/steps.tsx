// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Steps of "Safe deploy". Each step only reports results the server has confirmed;
// moving a revision carries `expect` (when two people move it at once, one wins —
// the loser sees the server's message, with the id currently holding the pointer).
import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { confirmText, Id } from "@/components/cairn/id";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import type { InvocationRow } from "@/data/pages";
import { invocationList } from "@/data/queries";
import { run } from "@/data/run";
import { BulkRun, type ItemResult, type Progress } from "@/lib/bulk";
import { diffService, type ServiceDiff, unchanged } from "@/lib/manifest-diff";
import { m } from "@/paraglide/messages";

type Deployment = components["schemas"]["Deployment"];
type ReplayCheck = components["schemas"]["ReplayCheck"];

export const deploymentQuery = (id: string) => ({
  queryKey: ["deployment", id] as const,
  queryFn: () =>
    ok(rest.GET("/deployments/{deploymentId}", { params: { path: { deploymentId: id } } })),
});

function Step({ n, title, children }: { n: number; title: string; children: React.ReactNode }) {
  return (
    <section
      aria-labelledby={`step-${n}`}
      className="flex flex-col gap-2 rounded-md border border-border p-4"
    >
      <h2 id={`step-${n}`} className="text-base font-semibold">
        {n}. {title}
      </h2>
      {children}
    </section>
  );
}

const show = (v: unknown) => (v === undefined ? "—" : JSON.stringify(v));

/** Step 1: manifest differences against each service's current revision. */
export function DiffStep({ next, current }: { next: Deployment; current: Map<string, string> }) {
  const olds = [...new Set(current.values())].filter((id) => id !== next.id);
  // Deployments are immutable ⇒ a manifest is never stale.
  const loaded = useQueries({
    queries: olds.map((id) => ({ ...deploymentQuery(id), staleTime: Number.POSITIVE_INFINITY })),
  });
  const manifests = new Map(
    loaded.flatMap((q) => (q.data ? [[q.data.id, q.data.manifest ?? []] as const] : [])),
  );
  const ready = loaded.every((q) => q.isSuccess);
  const diffs: ServiceDiff[] = (next.manifest ?? []).map((s) => {
    const cur = current.get(s.name);
    return diffService(cur ? manifests.get(cur)?.find((o) => o.name === s.name) : undefined, s);
  });
  return (
    <Step n={1} title={m.deploy_step_diff()}>
      {!ready ? (
        <p aria-busy="true">{m.loading()}</p>
      ) : (
        <ul className="flex flex-col gap-3">
          {diffs.map((d) => (
            <li key={d.service}>
              <h3 className="font-medium">
                {d.service}{" "}
                <span className="text-sm font-normal text-muted-foreground">
                  {d.isNew
                    ? m.deploy_service_new()
                    : unchanged(d)
                      ? m.deploy_service_same()
                      : m.deploy_service_from({ id: current.get(d.service) ?? "" })}
                </span>
              </h3>
              {d.handlersAdded.length > 0 ? (
                <p>{m.deploy_handlers_added({ names: d.handlersAdded.join(", ") })}</p>
              ) : null}
              {d.handlersRemoved.length > 0 ? (
                <p role="alert" className="text-destructive">
                  {m.deploy_handlers_removed({ names: d.handlersRemoved.join(", ") })}
                </p>
              ) : null}
              {d.changed.length > 0 ? (
                <Table dense className="w-auto" frameClassName="mt-1">
                  <THead>
                    <TH>{m.deploy_col_where()}</TH>
                    <TH>{m.deploy_col_field()}</TH>
                    <TH>{m.deploy_col_from()}</TH>
                    <TH>{m.deploy_col_to()}</TH>
                  </THead>
                  <TBody className="font-mono">
                    {d.changed.map((c) => (
                      <TRow key={`${c.handler}-${c.field}`}>
                        <TD priority="primary">{c.handler ?? m.deploy_service_level()}</TD>
                        <TD priority="primary">{c.field}</TD>
                        <TD>{show(c.from)}</TD>
                        <TD>{show(c.to)}</TD>
                      </TRow>
                    ))}
                  </TBody>
                </Table>
              ) : null}
            </li>
          ))}
        </ul>
      )}
    </Step>
  );
}

/** Step 2: trial replay of a sample of unfinished invocations of each service on the new version. */
export function ReplayStep({ next, services }: { next: Deployment; services: string[] }) {
  const [reports, setReports] = useState<Map<string, ReplayCheck>>(new Map());
  const check = useMutation({
    mutationFn: async () => {
      const out = new Map<string, ReplayCheck>();
      for (const service of services) {
        out.set(
          service,
          await ok(
            rest.POST("/deployments/{deploymentId}/replay-check", {
              // Every node: each node replays the sample on the ranges it leads.
              params: {
                path: { deploymentId: next.id },
                query: { service, sample: 20, scope: "cluster" },
              },
            }),
          ),
        );
      }
      return out;
    },
    onSuccess: setReports,
  });
  return (
    <Step n={2} title={m.deploy_step_replay()}>
      <p className="text-sm text-muted-foreground">{m.deploy_replay_body()}</p>
      <Button
        size="sm"
        variant="outline"
        className="self-start"
        onClick={() => check.mutate()}
        disabled={check.isPending}
      >
        {m.deploy_replay_run()}
      </Button>
      {check.isError ? (
        <p role="alert" className="text-destructive">
          {actionError(check.error)}
        </p>
      ) : null}
      {reports.size > 0 ? (
        <ul className="flex flex-col gap-2" aria-live="polite">
          {[...reports].map(([service, r]) => (
            <li key={service}>
              <span className="font-medium">{service}</span>:{" "}
              {m.deploy_replay_result({ matched: String(r.matched), checked: String(r.checked) })}{" "}
              <span className="text-xs text-muted-foreground">
                {m.deploy_replay_ranges({ n: String(r.ranges.length) })}
              </span>
              {r.failures.length > 0 ? (
                <ul className="ml-4 text-sm">
                  {r.failures.map((f) => (
                    <li key={f.id} className="flex flex-wrap items-center gap-2">
                      <Id
                        value={f.id}
                        wrap={(t) => (
                          <Link to="/invocations/$id" params={{ id: f.id }}>
                            {t}
                          </Link>
                        )}
                      />
                      <span className="text-destructive">
                        {typeof f.error === "string"
                          ? f.error
                          : m.deploy_replay_mismatch({
                              at: String(f.mismatchAt ?? "?"),
                              message: String(f.message ?? ""),
                            })}
                      </span>
                    </li>
                  ))}
                </ul>
              ) : null}
            </li>
          ))}
        </ul>
      ) : null}
    </Step>
  );
}

export type Moved = Map<string, string>;

/**
 * Step 3: move each service's revision, `expect` = the current id seen. A new
 * service (no revision yet) is moved unconditionally.
 */
export function MoveStep({
  next,
  current,
  onMoved,
}: {
  next: Deployment;
  current: Map<string, string>;
  /** service ⇒ old revision id, for rollback and drain tracking. */
  onMoved: (moved: Moved) => void;
}) {
  const qc = useQueryClient();
  const toast = useToast();
  const todo = next.services.filter((s) => current.get(s) !== next.id);
  const [open, setOpen] = useState(false);
  const [results, setResults] = useState<{ service: string; error?: string }[]>([]);
  const move = useMutation({
    mutationFn: async () => {
      const out: { service: string; error?: string }[] = [];
      const moved: Moved = new Map();
      for (const service of todo) {
        const expect = current.get(service);
        try {
          await ok(
            rest.PUT("/services/{service}/revision", {
              params: { path: { service } },
              body: { deployment: next.id, ...(expect ? { expect } : {}) },
            }),
          );
          out.push({ service });
          if (expect) moved.set(service, expect);
        } catch (e) {
          out.push({ service, error: actionError(e) });
        }
      }
      return { out, moved };
    },
    onSuccess: async ({ out, moved }) => {
      setResults(out);
      setOpen(false);
      await qc.invalidateQueries();
      if (out.every((r) => !r.error))
        toast.add({ title: m.deploy_moved({ n: String(out.length) }) });
      onMoved(moved);
    },
  });
  return (
    <Step n={3} title={m.deploy_step_move()}>
      {todo.length === 0 ? (
        <p>{m.deploy_move_done()}</p>
      ) : (
        <Button size="sm" className="self-start" onClick={() => setOpen(true)}>
          {m.deploy_move_button({ n: String(todo.length) })}
        </Button>
      )}
      {results.length > 0 ? (
        <ul aria-live="polite">
          {results.map((r) => (
            <li key={r.service} className={r.error ? "text-destructive" : undefined}>
              {r.service}: {r.error ?? m.deploy_moved_one()}
            </li>
          ))}
        </ul>
      ) : null}
      <ConfirmDialog
        open={open}
        onOpenChange={setOpen}
        title={m.deploy_step_move()}
        description={m.deploy_move_confirm({ services: todo.join(", ") })}
        confirmLabel={m.deploy_move_button({ n: String(todo.length) })}
        busy={move.isPending}
        onConfirm={() => move.mutate()}
      />
    </Step>
  );
}

async function migrateOne(id: string, to: string): Promise<ItemResult> {
  const { response, error } = await rest.POST("/invocations/{id}/migrate", {
    params: { path: { id }, query: { deployment: to } },
  });
  return {
    id,
    ok: response.ok,
    status: response.status,
    error: response.ok
      ? undefined
      : ((error as { error?: string } | undefined)?.error ?? response.statusText),
  };
}

/**
 * A page of ids still pinned to `old`, ≤ 50 per round. Migrated ones leave the
 * filter; mismatched ones (`409`) stay — skip the ones already tried so it does not
 * loop forever.
 */
function pinnedPages(old: string) {
  const tried = new Set<string>();
  return async () => {
    const out = await run<InvocationRow>(invocationList({ deployment: old }, "created", 50));
    const ids = out.rows
      .filter((r) => r.class !== "completed" && r.class !== "failed" && !tried.has(r.id))
      .map((r) => r.id);
    for (const id of ids) tried.add(id);
    return ids;
  };
}

/** Steps 4–6: track the drain, migrate the ones whose replay matched, remove the old version, rollback. */
export function DrainStep({ next, moved }: { next: Deployment; moved: Moved }) {
  const olds = [...new Set(moved.values())];
  return (
    <Step n={4} title={m.deploy_step_drain()}>
      {olds.length === 0 ? (
        <EmptyState variant="inline" title={m.deploy_drain_none()} />
      ) : (
        <ul className="flex flex-col gap-4">
          {olds.map((old) => (
            <OldDeployment
              key={old}
              old={old}
              next={next}
              services={[...moved].filter(([, v]) => v === old).map(([k]) => k)}
            />
          ))}
        </ul>
      )}
    </Step>
  );
}

function OldDeployment({
  old,
  next,
  services,
}: {
  old: string;
  next: Deployment;
  services: string[];
}) {
  const qc = useQueryClient();
  const toast = useToast();
  // Already removed ⇒ stop asking (otherwise every round is a `404`).
  const [gone, setGone] = useState(false);
  const q = useQuery({ ...deploymentQuery(old), refetchInterval: 5_000, enabled: !gone });
  const [progress, setProgress] = useState<Progress | null>(null);
  const [confirm, setConfirm] = useState<"delete" | "rollback" | null>(null);
  const pinned = q.data?.pinnedInvocations;
  const stillCurrent = (q.data?.currentFor.length ?? 0) > 0;
  const migrate = async () => {
    await new BulkRun({
      next: pinnedPages(old),
      send: async (ids) => {
        const out: ItemResult[] = [];
        for (let i = 0; i < ids.length; i += 5) {
          out.push(
            ...(await Promise.all(ids.slice(i, i + 5).map((id) => migrateOne(id, next.id)))),
          );
        }
        return out;
      },
      onProgress: setProgress,
    }).run();
    await qc.invalidateQueries({ queryKey: ["deployment", old] });
  };
  const act = useMutation({
    mutationFn: async (kind: "delete" | "rollback") => {
      if (kind === "delete") {
        await ok(
          rest.DELETE("/deployments/{deploymentId}", { params: { path: { deploymentId: old } } }),
        );
        return;
      }
      for (const service of services) {
        await ok(
          rest.PUT("/services/{service}/revision", {
            params: { path: { service } },
            body: { deployment: old, expect: next.id },
          }),
        );
      }
    },
    onSuccess: async (_, kind) => {
      setConfirm(null);
      toast.add({
        title: kind === "delete" ? m.deploy_deleted({ id: old }) : m.deploy_rolled_back(),
      });
      if (kind === "delete") setGone(true);
      await qc.invalidateQueries({
        predicate: (x) =>
          !(kind === "delete" && x.queryKey[0] === "deployment" && x.queryKey[1] === old),
      });
    },
  });
  const deleted = gone;
  return (
    <li className="flex flex-col gap-2">
      <p className="flex flex-wrap items-center gap-2">
        <Id
          value={old}
          full
          wrap={(t) => (
            <Link to="/deployments/$id" params={{ id: old }}>
              {t}
            </Link>
          )}
        />
        <span className="text-muted-foreground">({services.join(", ")})</span>
      </p>
      {deleted ? (
        <p>{m.deploy_deleted({ id: old })}</p>
      ) : (
        <>
          <p role="status" aria-live="polite">
            {pinned === undefined
              ? m.deployment_pinned_unknown()
              : pinned === 0
                ? m.deploy_drained()
                : m.deploy_pinned_left({ n: String(pinned) })}
          </p>
          <div className="flex flex-wrap gap-2">
            {pinned !== 0 && !stillCurrent ? (
              <Button
                size="sm"
                variant="outline"
                onClick={() => void migrate()}
                disabled={progress?.state === "running"}
              >
                {m.deploy_migrate_matching()}
              </Button>
            ) : null}
            <Button size="sm" variant="outline" onClick={() => setConfirm("rollback")}>
              {m.deploy_rollback()}
            </Button>
            {pinned === 0 && !stillCurrent ? (
              <Button size="sm" variant="destructive" onClick={() => setConfirm("delete")}>
                {m.deploy_delete_old()}
              </Button>
            ) : null}
          </div>
          {progress ? (
            <p role="status">
              {m.deploy_migrate_progress({
                ok: String(progress.ok),
                failed: String(progress.failed),
              })}
            </p>
          ) : null}
        </>
      )}
      {confirm ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setConfirm(null);
              act.reset();
            }
          }}
          title={confirm === "delete" ? m.deploy_delete_old() : m.deploy_rollback()}
          description={
            confirm === "delete"
              ? m.deploy_delete_body({ id: old })
              : m.deploy_rollback_body({ services: services.join(", "), id: old })
          }
          confirmLabel={confirm === "delete" ? m.deploy_delete_old() : m.deploy_rollback()}
          destructive={confirm === "delete"}
          typeToConfirm={confirm === "delete" ? confirmText(old) : undefined}
          busy={act.isPending}
          error={act.isError ? actionError(act.error) : null}
          onConfirm={() => act.mutate(confirm)}
        />
      ) : null}
    </li>
  );
}
