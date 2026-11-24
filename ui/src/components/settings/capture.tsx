// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Invocation capture: request a capture of the next matching run of (tenant, service,
// handler[, deployment]) within a time limit (`ttl`) — a bundle for reproducing a
// failure outside the cluster — and download existing bundles
// (`GET /admin/capture/{tenant}/{id}`, raw bytes).
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { actionError } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { Time } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import { m } from "@/paraglide/messages";

/** Selectable time limits, in seconds (server: 1 s … 7 days, default 1 hour). */
const TTLS: [number, () => string][] = [
  [900, m.capture_ttl_15m],
  [3600, m.capture_ttl_1h],
  [6 * 3600, m.capture_ttl_6h],
  [24 * 3600, m.capture_ttl_1d],
  [7 * 24 * 3600, m.capture_ttl_7d],
];

export function Capture() {
  const qc = useQueryClient();
  const toast = useToast();
  const [tenant, setTenant] = useState("default");
  const [service, setService] = useState("");
  const [handler, setHandler] = useState("");
  const [count, setCount] = useState("1");
  const [deployment, setDeployment] = useState("");
  const [ttl, setTtl] = useState(3600);
  const list = useQuery({
    queryKey: ["captures", tenant],
    queryFn: () => ok(rest.GET("/admin/capture", { params: { query: { tenant } } })),
    enabled: tenant.trim() !== "",
  });
  const create = useMutation({
    mutationFn: () =>
      ok(
        rest.POST("/admin/capture", {
          body: {
            tenant,
            service,
            handler,
            ...(deployment.trim() ? { deployment: deployment.trim() } : {}),
            count: Number(count) || 1,
            ttl,
          },
        }),
      ),
    onSuccess: async (c) => {
      toast.add({
        title: m.capture_created({ target: `${c.service}/${c.handler}`, n: String(c.remaining) }),
      });
      await qc.invalidateQueries({ queryKey: ["captures", tenant] });
    },
  });
  return (
    <section aria-labelledby="set-capture" className="flex flex-col gap-3">
      <h2 id="set-capture" className="text-base font-semibold">
        {m.capture_title()}
      </h2>
      <p className="text-sm text-muted-foreground">{m.capture_body()}</p>
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          create.mutate();
        }}
      >
        {(
          [
            ["cap-tenant", m.rule_tenant(), tenant, setTenant, "w-32"],
            ["cap-service", m.col_service(), service, setService, "w-40"],
            ["cap-handler", m.filter_handler(), handler, setHandler, "w-40"],
            ["cap-count", m.capture_count(), count, setCount, "w-20"],
          ] as const
        ).map(([id, label, v, set, w]) => (
          <div key={id} className="flex flex-col gap-1">
            <Label htmlFor={id}>{label}</Label>
            <Input
              id={id}
              className={`h-8 ${w}`}
              value={v}
              onChange={(e) => set(e.target.value)}
              required
              spellCheck={false}
            />
          </div>
        ))}
        <div className="flex flex-col gap-1">
          <Label htmlFor="cap-deployment">{m.capture_deployment()}</Label>
          <Input
            id="cap-deployment"
            className="h-8 w-44 font-mono"
            placeholder={m.capture_deployment_any()}
            value={deployment}
            onChange={(e) => setDeployment(e.target.value)}
            spellCheck={false}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="cap-ttl">{m.capture_ttl()}</Label>
          <NativeSelect
            id="cap-ttl"
            value={String(ttl)}
            onChange={(e) => setTtl(Number(e.target.value))}
          >
            {TTLS.map(([s, label]) => (
              <option key={s} value={s}>
                {label()}
              </option>
            ))}
          </NativeSelect>
        </div>
        <Button type="submit" disabled={create.isPending || !service || !handler}>
          {m.capture_create()}
        </Button>
      </form>
      {create.isError ? (
        <p role="alert" className="text-destructive">
          {actionError(create.error)}
        </p>
      ) : null}
      {list.isError ? (
        <QueryError error={list.error} />
      ) : list.data ? (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <div>
            <h3 className="mb-1 text-sm font-medium">{m.capture_live()}</h3>
            {list.data.captures.length === 0 ? (
              <EmptyState variant="inline" title={m.capture_none()} />
            ) : (
              <ul className="text-sm">
                {list.data.captures.map((c) => (
                  <li key={c.id}>
                    {c.service}/{c.handler}
                    {c.deployment ? (
                      <span className="font-mono text-muted-foreground"> @ {c.deployment}</span>
                    ) : null}{" "}
                    · {m.capture_remaining({ n: String(c.remaining) })} · {m.capture_expires()}{" "}
                    <Time ms={c.expiresMs} />
                  </li>
                ))}
              </ul>
            )}
          </div>
          <div>
            <h3 className="mb-1 text-sm font-medium">{m.capture_bundles()}</h3>
            {list.data.bundles.length === 0 ? (
              <EmptyState variant="inline" title={m.capture_no_bundles()} />
            ) : (
              <ul className="font-mono text-xs">
                {list.data.bundles.map((id) => (
                  <li key={id}>
                    <a
                      href={`/admin/capture/${encodeURIComponent(list.data.tenant)}/${encodeURIComponent(id)}`}
                      download={`capture-${id}.bin`}
                      className="text-primary hover:underline"
                    >
                      {id}
                    </a>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      ) : null}
    </section>
  );
}
