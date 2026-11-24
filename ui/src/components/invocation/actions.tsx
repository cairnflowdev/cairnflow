// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Actions on one invocation, by friction level: resume — toast; cooperative
// cancel — confirm; hard kill, purge, migrate `force` — type to confirm. Restart
// from step n, trial replay, migrate with a replay report, send a signal by
// name. Success is only reported once the server has returned `2xx` — never
// optimistic. Capabilities: cooperative cancel and signal need `invoke`; resume,
// restart, replay, migrate, hard kill, purge need `operate`. Items the key has no
// capability for are left out of the menu; with no items left the button is
// disabled with a reason.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { ChevronDown } from "lucide-react";
import { useMemo, useState } from "react";
import { Checkbox } from "@/components/cairn/checkbox";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { confirmText } from "@/components/cairn/id";
import { RoleGate } from "@/components/cairn/role-gate";
import { type Entry, failedStep, restartChoices } from "@/components/invocation/journal";
import type { InvocationDetail } from "@/components/invocations/peek";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { type PaletteAction, usePaletteActions } from "@/lib/palette";
import { type Capability, can } from "@/lib/roles";
import { m } from "@/paraglide/messages";

type Kind = "cancelSoft" | "cancelHard" | "purge" | "restart" | "replay" | "migrate" | "signal";
type Report = components["schemas"]["ReplayReport"];

export function Actions({
  r,
  caps,
  entries,
  restartFrom,
  onRestartFrom,
}: {
  r: InvocationDetail;
  /** Capability set of the key (`whoami.capabilities`). */
  caps: readonly Capability[] | undefined;
  /** Journal after `settle` — the choices of the restart dialog. */
  entries: Entry[];
  /** Opens the restart dialog with a given `from` (clicking "Restart from here" in the journal). */
  restartFrom: number | null;
  onRestartFrom: (n: number | null) => void;
}) {
  const qc = useQueryClient();
  const toast = useToast();
  const navigate = useNavigate();
  const [open, setOpen] = useState<Kind | null>(null);
  const kind: Kind | null = restartFrom !== null ? "restart" : open;
  const close = () => {
    setOpen(null);
    onRestartFrom(null);
    act.reset();
  };
  const ended = r.class === "completed" || r.class === "failed";
  const operate = can(caps, "operate");
  const invoke = can(caps, "invoke");
  const path = { path: { id: r.id } };
  const refresh = () => qc.invalidateQueries();

  const resume = useMutation({
    mutationFn: () => ok(rest.POST("/invocations/{id}/resume", { params: path })),
    onSuccess: async () => {
      toast.add({ title: m.action_resumed() });
      await refresh();
    },
    onError: (e) =>
      toast.add({ title: m.action_failed(), description: actionError(e), type: "error" }),
  });

  const act = useMutation({
    mutationFn: async (v: {
      kind: Kind;
      from?: number;
      deployment?: string;
      force?: boolean;
      signal?: string;
      /** Absent ⇒ the signal carries no value (Void, unlike `null`). */
      value?: unknown;
    }) => {
      switch (v.kind) {
        case "cancelSoft":
          return ok(
            rest.POST("/invocations/{id}/cancel", {
              params: { ...path, query: { mode: "signal" } },
            }),
          );
        case "cancelHard":
          return ok(rest.POST("/invocations/{id}/cancel", { params: path }));
        case "purge":
          return ok(rest.POST("/invocations/{id}/purge", { params: path }));
        case "restart":
          return ok(
            rest.POST("/invocations/{id}/restart", {
              params: { ...path, query: { from: v.from, deployment: v.deployment || undefined } },
            }),
          );
        case "replay":
          return ok(
            rest.POST("/invocations/{id}/replay", {
              params: { ...path, query: { deployment: v.deployment || undefined } },
            }),
          );
        case "signal":
          return ok(
            rest.POST("/invocations/{id}/signal", {
              params: path,
              body: {
                signal: v.signal ?? "",
                ...(v.value !== undefined ? { value: v.value } : {}),
              },
            }),
          );
        case "migrate":
          return ok(
            rest.POST("/invocations/{id}/migrate", {
              params: {
                ...path,
                query: { deployment: v.deployment ?? "", force: v.force ? "true" : undefined },
              },
            }),
          );
      }
    },
    onSuccess: async (out, v) => {
      if (v.kind === "replay") return; // the report shows in the dialog
      close();
      if (v.kind === "restart" && out && "new" in out) {
        toast.add({ title: m.action_restarted() });
        await navigate({ to: "/invocations/$id", params: { id: out.new } });
        return;
      }
      if (v.kind === "purge") {
        toast.add({ title: m.action_purged() });
        await navigate({ to: "/invocations" });
        return;
      }
      if (v.kind === "signal") {
        const status = out && "status" in out ? String(out.status) : "";
        toast.add({ title: m.action_signal_sent({ signal: v.signal ?? "", status }) });
        await refresh();
        return;
      }
      toast.add({
        title:
          v.kind === "migrate"
            ? m.action_migrated()
            : out && "cancelled" in out && "status" in out && !out.cancelled
              ? m.action_already_stopped({ status: String(out.status) })
              : m.action_cancelled(),
      });
      await refresh();
    },
  });

  // The same actions in Cmd+K while this page is open.
  // biome-ignore lint/correctness/useExhaustiveDependencies: rebuilt when the class changes
  const palette: PaletteAction[] = useMemo(
    () => [
      ...(r.class === "paused" && operate
        ? [{ id: "resume", label: m.action_resume(), run: () => resume.mutate() }]
        : []),
      ...(!ended && invoke
        ? [
            {
              id: "cancel-soft",
              label: m.action_cancel_soft(),
              run: () => setOpen("cancelSoft"),
            },
          ]
        : []),
      ...(operate
        ? [{ id: "restart", label: m.action_restart(), run: () => setOpen("restart") }]
        : []),
    ],
    [r.class, ended, operate, invoke],
  );
  usePaletteActions(palette);

  const id8 = confirmText(r.id);
  const error = act.isError ? actionError(act.error) : null;
  const first = [
    operate ? (
      <DropdownMenuItem key="restart" onClick={() => setOpen("restart")}>
        {m.action_restart()}
      </DropdownMenuItem>
    ) : null,
    operate ? (
      <DropdownMenuItem key="replay" onClick={() => setOpen("replay")}>
        {m.action_replay()}
      </DropdownMenuItem>
    ) : null,
    !ended && operate ? (
      <DropdownMenuItem key="migrate" onClick={() => setOpen("migrate")}>
        {m.action_migrate()}
      </DropdownMenuItem>
    ) : null,
    !ended && invoke ? (
      <DropdownMenuItem key="signal" onClick={() => setOpen("signal")}>
        {m.action_signal()}
      </DropdownMenuItem>
    ) : null,
  ].filter((x) => x !== null);
  const second = [
    !ended && invoke ? (
      <DropdownMenuItem key="cancel-soft" onClick={() => setOpen("cancelSoft")}>
        {m.action_cancel_soft()}
      </DropdownMenuItem>
    ) : null,
    !ended && operate ? (
      <DropdownMenuItem
        key="cancel-hard"
        variant="destructive"
        onClick={() => setOpen("cancelHard")}
      >
        {m.action_cancel_hard()}
      </DropdownMenuItem>
    ) : null,
    ended && operate ? (
      <DropdownMenuItem key="purge" variant="destructive" onClick={() => setOpen("purge")}>
        {m.action_purge()}
      </DropdownMenuItem>
    ) : null,
  ].filter((x) => x !== null);
  // No usable item (a `view` key): the button still shows, disabled with a reason.
  const blocked = first.length + second.length === 0 ? m.cap_needs({ cap: "operate" }) : null;
  return (
    <>
      <div className="flex gap-2">
        {r.class === "paused" ? (
          <RoleGate have={caps} need="operate">
            {(why) => (
              <Button
                onClick={() => resume.mutate()}
                disabled={!!why || resume.isPending}
                title={why ?? undefined}
              >
                {m.action_resume()}
              </Button>
            )}
          </RoleGate>
        ) : null}
        {blocked ? (
          <Button variant="outline" disabled title={blocked}>
            {m.actions()}
            <ChevronDown aria-hidden />
          </Button>
        ) : (
          <DropdownMenu>
            <DropdownMenuTrigger render={<Button variant="outline" />}>
              {m.actions()}
              <ChevronDown aria-hidden />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="min-w-56">
              {first.length > 0 ? <DropdownMenuGroup>{first}</DropdownMenuGroup> : null}
              {first.length > 0 && second.length > 0 ? <DropdownMenuSeparator /> : null}
              {second.length > 0 ? <DropdownMenuGroup>{second}</DropdownMenuGroup> : null}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>

      <ConfirmDialog
        open={kind === "cancelSoft"}
        onOpenChange={(o) => !o && close()}
        title={m.action_cancel_soft()}
        description={m.action_cancel_soft_body()}
        confirmLabel={m.action_cancel_soft()}
        busy={act.isPending}
        error={error}
        onConfirm={() => act.mutate({ kind: "cancelSoft" })}
      />
      <ConfirmDialog
        open={kind === "cancelHard"}
        onOpenChange={(o) => !o && close()}
        title={m.action_cancel_hard()}
        description={m.action_cancel_hard_body()}
        confirmLabel={m.action_cancel_hard()}
        destructive
        typeToConfirm={id8}
        busy={act.isPending}
        error={error}
        onConfirm={() => act.mutate({ kind: "cancelHard" })}
      />
      <ConfirmDialog
        open={kind === "purge"}
        onOpenChange={(o) => !o && close()}
        title={m.action_purge()}
        description={m.action_purge_body()}
        confirmLabel={m.action_purge()}
        destructive
        typeToConfirm={id8}
        busy={act.isPending}
        error={error}
        onConfirm={() => act.mutate({ kind: "purge" })}
      />
      {kind === "restart" ? (
        <RestartDialog
          entries={entries}
          initialFrom={restartFrom}
          busy={act.isPending}
          error={error}
          onClose={close}
          onConfirm={(from, deployment) => act.mutate({ kind: "restart", from, deployment })}
        />
      ) : null}
      {kind === "signal" ? (
        <SignalDialog
          busy={act.isPending}
          error={error}
          onClose={close}
          onConfirm={(signal, value) => act.mutate({ kind: "signal", signal, value })}
        />
      ) : null}
      {kind === "replay" || kind === "migrate" ? (
        <DeploymentDialog
          kind={kind}
          r={r}
          busy={act.isPending}
          error={error}
          report={kind === "replay" && act.data && "matches" in act.data ? act.data : null}
          onClose={close}
          onConfirm={(deployment, force) => act.mutate({ kind, deployment, force })}
        />
      ) : null}
    </>
  );
}

function useDeploymentIds() {
  const q = useQuery({ queryKey: ["deployments"], queryFn: () => ok(rest.GET("/deployments")) });
  return (q.data ?? []).map((d) => d.id);
}

/**
 * Restart from a step: pick a **command** by name — keep the journal before it,
 * run again from it (`from` = the command's entry index). A command is never split
 * from its completion. Default: the entry clicked in the journal, else the failing
 * step, else from the start.
 */
function RestartDialog({
  entries,
  initialFrom,
  busy,
  error,
  onClose,
  onConfirm,
}: {
  entries: Entry[];
  initialFrom: number | null;
  busy: boolean;
  error: string | null;
  onClose: () => void;
  onConfirm: (from: number, deployment: string) => void;
}) {
  const choices = restartChoices(entries).filter((e) => e.index > 1);
  const failed = failedStep(entries, null);
  const pick = [initialFrom, failed?.index].find(
    (n) => n !== null && n !== undefined && choices.some((c) => c.index === n),
  );
  const [from, setFrom] = useState(String(pick ?? 1));
  const [deployment, setDeployment] = useState("");
  const ids = useDeploymentIds();
  const n = Number(from);
  return (
    <ConfirmDialog
      open
      onOpenChange={(o) => !o && onClose()}
      title={m.restart_title()}
      description={m.action_restart_body()}
      confirmLabel={m.restart_go()}
      busy={busy}
      error={error}
      onConfirm={() => onConfirm(n, deployment)}
    >
      <Label htmlFor="restart-from">{m.restart_step()}</Label>
      <NativeSelect id="restart-from" value={from} onChange={(e) => setFrom(e.target.value)}>
        <option value="1">{m.restart_from_start()}</option>
        {choices.map((e) => (
          <option key={e.index} value={e.index}>
            {`#${e.index} ${e.lite.kind ?? e.name ?? "?"}${e.lite.name ? ` “${e.lite.name}”` : ""}`}
            {e.index === failed?.index && failed.at ? ` — ${m.restart_failed_here()}` : ""}
          </option>
        ))}
      </NativeSelect>
      <DeploymentSelect
        id="restart-deployment"
        ids={ids}
        value={deployment}
        onChange={setDeployment}
        optional
      />
    </ConfirmDialog>
  );
}

/**
 * Send a signal by name (`POST /invocations/{id}/signal`): the name is required,
 * the JSON value optional — empty ⇒ the signal carries no value. Delivering a
 * signal again with the same name after it was delivered is a no-op in the state
 * machine (the server dedupes).
 */
function SignalDialog({
  busy,
  error,
  onClose,
  onConfirm,
}: {
  busy: boolean;
  error: string | null;
  onClose: () => void;
  onConfirm: (signal: string, value: unknown) => void;
}) {
  const [name, setName] = useState("");
  const [raw, setRaw] = useState("");
  const parsed = parseJson(raw);
  const ready = name.trim() !== "" && parsed.ok;
  return (
    <ConfirmDialog
      open
      onOpenChange={(o) => !o && onClose()}
      title={m.action_signal()}
      description={m.action_signal_body()}
      confirmLabel={m.action_signal_send()}
      busy={busy || !ready}
      error={error}
      onConfirm={() => onConfirm(name.trim(), parsed.ok ? parsed.value : undefined)}
    >
      <Label htmlFor="signal-name">{m.signal_name()}</Label>
      <Input
        id="signal-name"
        autoComplete="off"
        spellCheck={false}
        className="font-mono"
        value={name}
        onChange={(e) => setName(e.target.value)}
      />
      <Label htmlFor="signal-value">{m.signal_value()}</Label>
      <Textarea
        id="signal-value"
        spellCheck={false}
        className="font-mono"
        placeholder='{"approved": true}'
        aria-describedby="signal-value-hint"
        aria-invalid={parsed.ok ? undefined : true}
        value={raw}
        onChange={(e) => setRaw(e.target.value)}
      />
      <p id="signal-value-hint" className="text-xs text-muted-foreground">
        {parsed.ok ? m.signal_value_hint() : m.signal_value_bad()}
      </p>
    </ConfirmDialog>
  );
}

/** Empty ⇒ no value (`undefined`); does not parse ⇒ `ok: false`. */
function parseJson(raw: string): { ok: true; value: unknown } | { ok: false } {
  if (raw.trim() === "") return { ok: true, value: undefined };
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch {
    return { ok: false };
  }
}

function DeploymentDialog({
  kind,
  r,
  busy,
  error,
  report,
  onClose,
  onConfirm,
}: {
  kind: "replay" | "migrate";
  r: InvocationDetail;
  busy: boolean;
  error: string | null;
  report: Report | null;
  onClose: () => void;
  onConfirm: (deployment: string, force: boolean) => void;
}) {
  const ids = useDeploymentIds();
  const [deployment, setDeployment] = useState(kind === "replay" ? "" : "");
  const [force, setForce] = useState(false);
  const migrate = kind === "migrate";
  return (
    <ConfirmDialog
      open
      onOpenChange={(o) => !o && onClose()}
      title={migrate ? m.action_migrate() : m.action_replay()}
      description={migrate ? m.action_migrate_body() : m.action_replay_body()}
      confirmLabel={migrate ? m.action_migrate() : m.action_replay()}
      destructive={migrate && force}
      typeToConfirm={migrate && force ? confirmText(r.id) : undefined}
      busy={busy || (migrate && deployment === "")}
      error={error}
      onConfirm={() => onConfirm(deployment, force)}
    >
      <DeploymentSelect
        id={`${kind}-deployment`}
        ids={ids}
        value={deployment}
        onChange={setDeployment}
        optional={!migrate}
      />
      {migrate ? (
        <Checkbox checked={force} onCheckedChange={setForce} className="whitespace-normal">
          {m.migrate_force()}
        </Checkbox>
      ) : null}
      {report ? (
        <div role="status" className="rounded-md border border-border p-2">
          {report.matches ? (
            <p>{m.replay_matches({ upto: String(report.upto) })}</p>
          ) : (
            <>
              <p className="font-medium text-destructive">
                {m.replay_mismatch({ at: String(report.mismatchAt ?? report.upto) })}
              </p>
              {report.expected ? <p className="font-mono text-xs">− {report.expected}</p> : null}
              {report.got ? <p className="font-mono text-xs">+ {report.got}</p> : null}
              {report.message ? <p className="text-muted-foreground">{report.message}</p> : null}
            </>
          )}
        </div>
      ) : null}
    </ConfirmDialog>
  );
}

function DeploymentSelect({
  id,
  ids,
  value,
  onChange,
  optional,
}: {
  id: string;
  ids: string[];
  value: string;
  onChange: (v: string) => void;
  optional?: boolean;
}) {
  return (
    <>
      <Label htmlFor={id}>{m.col_deployment()}</Label>
      <NativeSelect id={id} value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="">{optional ? m.deployment_current() : m.deployment_choose()}</option>
        {ids.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </NativeSelect>
    </>
  );
}
