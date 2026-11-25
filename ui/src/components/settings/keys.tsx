// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// API keys: the list (no secrets), create ⇒ the `cf_…` secret shown **once** with a
// copy button and a warning, revoke by typing the id. The secret only lives in the
// dialog's state; closing the dialog ⇒ clears the state and `reset()`s the mutation
// (so it does not linger in the TanStack Query cache). Never in the URL, never
// logged. The `view`/`provision` roles need the cluster's `auth/keys-v2` gate: not
// enabled ⇒ `409 key_roles_disabled`, naming the command that enables it. A caller
// with `provision` but without `cluster` can only grant `invoke`/`view`/`admin` —
// the server checks too.
import { useMutation, useQuery, useQueryClient, useSuspenseQuery } from "@tanstack/react-query";
import { KeyRound, TriangleAlert } from "lucide-react";
import { useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { CopyButton } from "@/components/cairn/copy-button";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { RowActions, RowButton, rowActionsCell } from "@/components/cairn/row-actions";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Tag } from "@/components/cairn/tag";
import { Time } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import { useToast } from "@/components/ui/toast";
import { ApiError, ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { useCapabilities, whoamiQuery } from "@/data/session";
import { can } from "@/lib/roles";
import { m } from "@/paraglide/messages";

type Role = components["schemas"]["ApiKey"]["role"];
const ROLES: Role[] = ["invoke", "view", "admin", "provision", "cluster-admin"];
/** Roles within a tenant — a caller without `cluster` can only grant these. */
const TENANT_ROLES: Role[] = ["invoke", "view", "admin"];

function createError(e: unknown): string {
  if (e instanceof ApiError && e.status === 409 && e.code === "key_roles_disabled") {
    return m.key_roles_disabled();
  }
  return actionError(e);
}

export function Keys() {
  const { data: me } = useSuspenseQuery(whoamiQuery);
  const qc = useQueryClient();
  const toast = useToast();
  const list = useQuery({ queryKey: ["keys"], queryFn: () => ok(rest.GET("/admin/keys")) });
  const [creating, setCreating] = useState(false);
  const [revoking, setRevoking] = useState<string | null>(null);
  const revoke = useMutation({
    mutationFn: async (id: string) => {
      const { response, data, error } = await rest.DELETE("/admin/keys/{id}", {
        params: { path: { id } },
      });
      if (!response.ok) return ok(Promise.resolve({ response, data, error }));
      return response.status === 202 ? (data as { pending?: string[] } | undefined) : undefined;
    },
    onSuccess: async (out, id) => {
      setRevoking(null);
      toast.add({
        title: out?.pending?.length
          ? m.key_revoked_pending({ id, nodes: out.pending.join(", ") })
          : m.key_revoked({ id }),
      });
      await qc.invalidateQueries({ queryKey: ["keys"] });
    },
  });
  return (
    <section aria-labelledby="set-keys" className="flex flex-col gap-2">
      <div className="flex items-baseline justify-between">
        <h2 id="set-keys" className="text-base font-semibold">
          {m.keys_title()}
        </h2>
        <Button onClick={() => setCreating(true)}>{m.key_new()}</Button>
      </div>
      {list.isPending ? (
        <Skeleton className="h-20 w-full" />
      ) : list.isError ? (
        <QueryError error={list.error} />
      ) : list.data.length === 0 ? (
        <EmptyState icon={<KeyRound />} title={m.keys_empty()}>
          {m.keys_empty_body()}
        </EmptyState>
      ) : (
        <Table>
          <THead>
            <TH>{m.key_id()}</TH>
            <TH>{m.rule_tenant()}</TH>
            <TH>{m.key_role()}</TH>
            <TH>{m.col_created()}</TH>
            <TH>
              <span className="sr-only">{m.actions()}</span>
            </TH>
          </THead>
          <TBody>
            {list.data.map((k) => (
              <TRow key={k.id}>
                <TD priority="primary" className="font-mono">
                  {k.id}
                  {k.id === me.keyId ? (
                    // The key of this session: revoking it signs you out.
                    <Tag className="ml-2 font-sans">{m.key_this_session()}</Tag>
                  ) : null}
                </TD>
                <TD>{k.tenant}</TD>
                <TD>{k.role}</TD>
                <TD>
                  <Time ms={k.createdAtMs} />
                </TD>
                <TD priority="status" className={rowActionsCell}>
                  <RowActions>
                    <RowButton
                      destructive
                      aria-label={`${m.key_revoke()} ${k.id}`}
                      onClick={() => setRevoking(k.id)}
                    >
                      {m.key_revoke()}
                    </RowButton>
                  </RowActions>
                </TD>
              </TRow>
            ))}
          </TBody>
        </Table>
      )}
      {creating ? <CreateKey onClose={() => setCreating(false)} /> : null}
      {revoking ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setRevoking(null);
              revoke.reset();
            }
          }}
          title={m.key_revoke()}
          description={
            revoking === me.keyId ? (
              <>
                {m.key_revoke_body({ id: revoking })} <strong>{m.key_revoke_self()}</strong>
              </>
            ) : (
              m.key_revoke_body({ id: revoking })
            )
          }
          confirmLabel={m.key_revoke()}
          destructive
          typeToConfirm={revoking}
          busy={revoke.isPending}
          error={revoke.isError ? actionError(revoke.error) : null}
          onConfirm={() => revoke.mutate(revoking)}
        />
      ) : null}
    </section>
  );
}

function CreateKey({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient();
  const caps = useCapabilities();
  const roles = can(caps, "cluster") ? ROLES : TENANT_ROLES;
  const [tenant, setTenant] = useState("");
  const [role, setRole] = useState<Role>("invoke");
  const create = useMutation({
    mutationFn: () => ok(rest.POST("/admin/keys", { body: { tenant, role } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["keys"] }),
  });
  const close = () => {
    // The secret must not linger anywhere after the dialog closes.
    create.reset();
    onClose();
  };
  const secret = create.data?.secret;
  return (
    <Dialog open onOpenChange={(o) => !o && close()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{secret ? m.key_created() : m.key_new()}</DialogTitle>
          {secret ? null : <DialogDescription>{m.key_new_body()}</DialogDescription>}
        </DialogHeader>
        {secret ? (
          <div className="flex flex-col gap-3">
            <p
              role="alert"
              className="flex gap-2 rounded-md border border-input bg-muted px-3 py-2 text-sm"
            >
              <TriangleAlert aria-hidden className="mt-0.5 size-4 shrink-0" />
              {m.key_secret_once()}
            </p>
            <Label htmlFor="key-secret">{m.key_secret()}</Label>
            <Input
              id="key-secret"
              readOnly
              value={secret}
              className="font-mono"
              autoComplete="off"
              spellCheck={false}
              onFocus={(e) => e.target.select()}
            />
            <DialogFooter>
              <CopyButton
                value={secret}
                label={m.key_secret()}
                text={m.key_copy()}
                showText
                variant="outline"
                size="default"
              />
              <Button onClick={close}>{m.key_done()}</Button>
            </DialogFooter>
          </div>
        ) : (
          <form
            className="grid grid-cols-[6rem_1fr] items-center gap-x-3 gap-y-2"
            onSubmit={(e) => {
              e.preventDefault();
              create.mutate();
            }}
          >
            <Label htmlFor="key-tenant">{m.rule_tenant()}</Label>
            <Input
              id="key-tenant"
              value={tenant}
              onChange={(e) => setTenant(e.target.value)}
              required
              spellCheck={false}
            />
            <Label htmlFor="key-role">{m.key_role()}</Label>
            <NativeSelect
              id="key-role"
              value={role}
              onChange={(e) => setRole(e.target.value as Role)}
            >
              {roles.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </NativeSelect>
            {create.isError ? (
              <p role="alert" className="col-span-2 text-destructive">
                {createError(create.error)}
              </p>
            ) : null}
            <DialogFooter className="col-span-2">
              <Button type="button" variant="outline" onClick={close}>
                {m.confirm_cancel()}
              </Button>
              <Button type="submit" disabled={create.isPending || tenant.trim() === ""}>
                {m.key_create()}
              </Button>
            </DialogFooter>
          </form>
        )}
      </DialogContent>
    </Dialog>
  );
}
