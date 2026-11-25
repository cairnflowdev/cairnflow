// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Per-tenant settings: hash chain (on/off, applies to invocations created
// afterwards), slow-run threshold (0 = the node's default), isolation mode and rule
// (`default`/empty = back to the node's default; the server checks the syntax, a
// `400` shows its message verbatim). The list = tenants with settings ∪ tenants
// with keys.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Checkbox } from "@/components/cairn/checkbox";
import { actionError } from "@/components/cairn/confirm";
import { QueryError } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { m } from "@/paraglide/messages";

type Put = components["schemas"]["PutTenantSettings"];

export function Tenants() {
  const qc = useQueryClient();
  const toast = useToast();
  const settings = useQuery({
    queryKey: ["tenant-settings"],
    queryFn: () => ok(rest.GET("/admin/tenants")),
  });
  const keys = useQuery({ queryKey: ["keys"], queryFn: () => ok(rest.GET("/admin/keys")) });
  const put = useMutation({
    mutationFn: ({ tenant, body }: { tenant: string; body: Put }) =>
      ok(rest.PUT("/admin/tenants/{tenant}", { params: { path: { tenant } }, body })),
    onSuccess: async (out) => {
      toast.add({ title: m.tenant_saved({ tenant: out.tenant }) });
      await qc.invalidateQueries({ queryKey: ["tenant-settings"] });
    },
    onError: (e) =>
      toast.add({ title: m.action_failed(), description: actionError(e), type: "error" }),
  });
  if (settings.isPending) return <Skeleton className="h-20 w-full" />;
  if (settings.isError) return <QueryError error={settings.error} />;
  const known = new Map(settings.data.tenants.map((t) => [t.tenant, t]));
  const names = [
    ...new Set([...known.keys(), ...(keys.data ?? []).map((k) => k.tenant), "default"]),
  ].sort();
  return (
    <section aria-labelledby="set-tenants" className="flex flex-col gap-2">
      <h2 id="set-tenants" className="text-base font-semibold">
        {m.tenants_title()}
      </h2>
      <Table>
        <THead>
          <TH>{m.rule_tenant()}</TH>
          <TH>{m.tenant_hash_chain()}</TH>
          <TH>{m.tenant_slow_ms()}</TH>
          <TH>{m.tenant_quarantine()}</TH>
        </THead>
        <TBody>
          {names.map((t) => {
            const s = known.get(t);
            return (
              <TRow key={t} className="align-middle">
                <TD priority="primary" className="font-mono">
                  {t}
                </TD>
                <TD>
                  <Checkbox
                    aria-label={m.tenant_hash_chain_for({ tenant: t })}
                    checked={s?.hashChain ?? false}
                    disabled={put.isPending}
                    onCheckedChange={(c) => put.mutate({ tenant: t, body: { hashChain: c } })}
                  />
                </TD>
                <TD>
                  <SlowMs
                    tenant={t}
                    value={s?.slowMs ?? null}
                    onSave={(ms) => put.mutate({ tenant: t, body: { slowMs: ms } })}
                  />
                </TD>
                <TD>
                  <Quarantine
                    // Change the React key when the server changes the value ⇒ the field shows the saved value.
                    key={`${s?.quarantineMode ?? ""}|${s?.quarantineRule ?? ""}`}
                    tenant={t}
                    mode={s?.quarantineMode ?? null}
                    rule={s?.quarantineRule ?? null}
                  />
                </TD>
              </TRow>
            );
          })}
        </TBody>
      </Table>
      <p className="text-xs text-muted-foreground">{m.tenant_hash_chain_hint()}</p>
      <div className="text-xs text-muted-foreground">
        <p>{m.tenant_quarantine_hint()}</p>
        <details>
          <summary className="min-h-6 cursor-pointer">{m.hint_more()}</summary>
          <p>{m.tenant_quarantine_detail()}</p>
        </details>
      </div>
    </section>
  );
}

function SlowMs({
  tenant,
  value,
  onSave,
}: {
  tenant: string;
  value: number | null;
  onSave: (ms: number) => void;
}) {
  const [v, setV] = useState(value === null ? "" : String(value));
  const valid = /^\d+$/.test(v.trim()) || v.trim() === "";
  return (
    <form
      className="flex items-center gap-2"
      onSubmit={(e) => {
        e.preventDefault();
        if (valid) onSave(v.trim() === "" ? 0 : Number(v.trim()));
      }}
    >
      <Input
        aria-label={m.tenant_slow_ms_for({ tenant })}
        className="h-7 w-28"
        inputMode="numeric"
        placeholder={m.tenant_slow_default()}
        value={v}
        onChange={(e) => setV(e.target.value)}
        aria-invalid={valid ? undefined : true}
      />
      {/* Form in one row: small buttons, as tall as the h-7 inputs. */}
      <Button type="submit" variant="outline" size="sm" disabled={!valid}>
        {m.schedule_save()}
      </Button>
    </form>
  );
}

const MODES = ["off", "observe", "cooldown", "hold"] as const;
const MODE_LABEL: Record<(typeof MODES)[number], () => string> = {
  off: m.tenant_quarantine_off,
  observe: m.tenant_quarantine_observe,
  cooldown: m.tenant_quarantine_cooldown,
  hold: m.tenant_quarantine_hold,
};

/**
 * Isolation mode + rule of a tenant. Both are sent on every save: empty ⇒
 * `"default"` (back to the node's default). The server's error (`400` when the rule
 * has a syntax error) shows verbatim right below the field, not only in a toast.
 */
function Quarantine({
  tenant,
  mode,
  rule,
}: {
  tenant: string;
  mode: string | null;
  rule: string | null;
}) {
  const qc = useQueryClient();
  const toast = useToast();
  const [modeV, setModeV] = useState(mode ?? "");
  const [ruleV, setRuleV] = useState(rule ?? "");
  const save = useMutation({
    mutationFn: () =>
      ok(
        rest.PUT("/admin/tenants/{tenant}", {
          params: { path: { tenant } },
          body: { quarantine: modeV || "default", quarantineRule: ruleV.trim() || "default" },
        }),
      ),
    onSuccess: async (out) => {
      toast.add({ title: m.tenant_saved({ tenant: out.tenant }) });
      await qc.invalidateQueries({ queryKey: ["tenant-settings"] });
    },
  });
  const changed = modeV !== (mode ?? "") || ruleV.trim() !== (rule ?? "");
  return (
    <form
      className="flex flex-col gap-1"
      onSubmit={(e) => {
        e.preventDefault();
        save.mutate();
      }}
    >
      <div className="flex items-center gap-2">
        <NativeSelect
          aria-label={m.tenant_quarantine_for({ tenant })}
          className="h-7"
          value={modeV}
          onChange={(e) => setModeV(e.target.value)}
        >
          <option value="">{m.tenant_quarantine_default()}</option>
          {MODES.map((v) => (
            <option key={v} value={v}>
              {MODE_LABEL[v]()}
            </option>
          ))}
        </NativeSelect>
        <Input
          aria-label={m.tenant_quarantine_rule_for({ tenant })}
          className="h-7 w-28 font-mono"
          placeholder="20/5/300"
          spellCheck={false}
          value={ruleV}
          onChange={(e) => setRuleV(e.target.value)}
          aria-invalid={save.isError ? true : undefined}
        />
        {/* The row already has the slow threshold's "Save" button — a distinct name tells them apart. */}
        <Button
          type="submit"
          variant="outline"
          size="sm"
          aria-label={m.tenant_quarantine_save({ tenant })}
          disabled={!changed || save.isPending}
        >
          {m.schedule_save()}
        </Button>
      </div>
      {save.isError ? (
        <p role="alert" className="text-xs text-destructive">
          {actionError(save.error)}
        </p>
      ) : null}
    </form>
  );
}
