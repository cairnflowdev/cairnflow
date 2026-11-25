// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// "Safe deploy": register without moving the revision ⇒
// compare manifests ⇒ trial replay ⇒ move with `expect` ⇒ watch the drain, migrate the ones whose
// replay matches ⇒ remove the old version; one-button rollback. State lives in the URL (`d`, `prev`)
// — reloading the page can still continue, including rollback.
import { useMutation, useQuery } from "@tanstack/react-query";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { actionError } from "@/components/cairn/confirm";
import { Id } from "@/components/cairn/id";
import { QueryError } from "@/components/cairn/query-state";
import { RequireCapability } from "@/components/cairn/role-gate";
import { Skeleton } from "@/components/cairn/skeleton";
import {
  DiffStep,
  DrainStep,
  deploymentQuery,
  type Moved,
  MoveStep,
  ReplayStep,
} from "@/components/deploy/steps";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import { m } from "@/paraglide/messages";

type Search = { d?: string; prev?: string };

export const Route = createFileRoute("/_app/deployments/deploy")({
  validateSearch: (s: Record<string, unknown>): Search => ({
    d: typeof s.d === "string" && /^dp_[A-Za-z0-9_-]+$/.test(s.d) ? s.d : undefined,
    prev: typeof s.prev === "string" && s.prev ? s.prev : undefined,
  }),
  component: SafeDeploy,
});

/** `Shop:dp_a,Cart:dp_b` ⇔ Map service ⇒ old revision. */
export const encodeMoved = (m: Moved) => [...m].map(([k, v]) => `${k}:${v}`).join(",");
export const decodeMoved = (s: string | undefined): Moved =>
  new Map(
    (s ?? "")
      .split(",")
      .map((p) => p.split(":"))
      .filter((p): p is [string, string] => p.length === 2 && !!p[0] && !!p[1]?.startsWith("dp_")),
  );

function SafeDeploy() {
  const { d, prev } = Route.useSearch();
  const navigate = useNavigate({ from: "/deployments/deploy" });
  const caps = useCapabilities();
  return (
    <div className="flex max-w-4xl flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold">{m.deploy_title()}</h1>
      {/* Every step (register, move revision, migrate, remove) needs `operate`. */}
      <RequireCapability have={caps} need="operate">
        {d ? (
          <Flow
            id={d}
            moved={decodeMoved(prev)}
            onMoved={(mv) => navigate({ search: { d, prev: encodeMoved(mv) } })}
          />
        ) : (
          <Stage onStaged={(id) => navigate({ search: { d: id } })} />
        )}
      </RequireCapability>
    </div>
  );
}

function Stage({ onStaged }: { onStaged: (id: string) => void }) {
  const [uri, setUri] = useState("");
  const stage = useMutation({
    mutationFn: () =>
      ok(rest.POST("/deployments", { params: { query: { activate: "false" } }, body: { uri } })),
    onSuccess: (dep) => onStaged(dep.id),
  });
  return (
    <form
      className="flex flex-col gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        stage.mutate();
      }}
    >
      <p className="text-muted-foreground">{m.deploy_intro()}</p>
      <Label htmlFor="deploy-uri">{m.deploy_uri()}</Label>
      <Input
        id="deploy-uri"
        type="url"
        className="font-mono"
        placeholder="http://my-service:9080"
        value={uri}
        onChange={(e) => setUri(e.target.value)}
        required
      />
      {stage.isError ? (
        <p role="alert" className="text-destructive">
          {actionError(stage.error)}
        </p>
      ) : null}
      <Button type="submit" className="self-start" disabled={stage.isPending || uri === ""}>
        {m.deploy_stage()}
      </Button>
    </form>
  );
}

function Flow({ id, moved, onMoved }: { id: string; moved: Moved; onMoved: (m: Moved) => void }) {
  const next = useQuery(deploymentQuery(id));
  const services = useQuery({ queryKey: ["services"], queryFn: () => ok(rest.GET("/services")) });
  if (next.isPending || services.isPending) return <Skeleton className="h-40 w-full" />;
  if (next.isError) return <QueryError error={next.error} />;
  if (services.isError) return <QueryError error={services.error} />;
  // Current revision of each service the new version declares. Once moved it is `id` —
  // the comparison uses the old version remembered in the URL.
  const current = new Map<string, string>();
  for (const s of next.data.services) {
    const now = services.data.find((x) => x.name === s)?.deploymentId;
    const was = moved.get(s);
    if (now && now !== id) current.set(s, now);
    else if (was) current.set(s, was);
  }
  const live = new Map(
    next.data.services.flatMap((s) => {
      const now = services.data.find((x) => x.name === s)?.deploymentId;
      return now ? [[s, now] as const] : [];
    }),
  );
  return (
    <>
      <p className="flex flex-wrap items-center gap-2">
        {m.deploy_staged()} <Id value={id} full /> ·{" "}
        <span className="font-mono">{next.data.endpoint}</span>
      </p>
      <DiffStep next={next.data} current={current} />
      <ReplayStep next={next.data} services={next.data.services.filter((s) => current.has(s))} />
      <MoveStep
        next={next.data}
        current={live}
        onMoved={(mv) => onMoved(new Map([...moved, ...mv]))}
      />
      <DrainStep next={next.data} moved={moved} />
    </>
  );
}
