// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Resolve / reject a workflow promise: `POST
// /promises/{service}/{key}/{name}/{resolve|reject}`. The result the server confirms
// (`completed: false` = it already had a result) is reported with a toast. Needs
// `invoke`: a `view` key sees the form but the button is disabled with a reason.
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { actionError } from "@/components/cairn/confirm";
import { RoleGate } from "@/components/cairn/role-gate";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import { m } from "@/paraglide/messages";

export function PromiseForm({
  service,
  objectKey,
  name: initial,
}: {
  service: string;
  objectKey: string;
  name?: string;
}) {
  const qc = useQueryClient();
  const toast = useToast();
  const caps = useCapabilities();
  const [name, setName] = useState(initial ?? "");
  const [value, setValue] = useState('""');
  const [reject, setReject] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const path = { service, key: objectKey, name };
  const send = useMutation({
    mutationFn: async () => {
      if (reject) {
        return ok(
          rest.POST("/promises/{service}/{key}/{name}/reject", {
            params: { path },
            body: { message: value },
          }),
        );
      }
      return ok(
        rest.POST("/promises/{service}/{key}/{name}/resolve", {
          params: { path },
          body: JSON.parse(value),
        }),
      );
    },
    onSuccess: async (out) => {
      toast.add({
        title: out.completed ? m.promise_completed({ name }) : m.promise_already({ name }),
      });
      await qc.invalidateQueries();
    },
    onError: (e) =>
      setError(
        e instanceof SyntaxError
          ? m.schedule_input_invalid({ message: e.message })
          : actionError(e),
      ),
  });
  return (
    <form
      aria-label={m.promise_complete_title()}
      className="flex flex-col gap-2 rounded-md border border-border p-3"
      onSubmit={(e) => {
        e.preventDefault();
        setError(null);
        send.mutate();
      }}
    >
      <h3 className="font-semibold">{m.promise_complete_title()}</h3>
      <Label htmlFor="pf-name">{m.promise_name()}</Label>
      <Input
        id="pf-name"
        value={name}
        onChange={(e) => setName(e.target.value)}
        required
        spellCheck={false}
      />
      <div className="flex gap-4" role="radiogroup" aria-label={m.promise_outcome()}>
        <label className="flex items-center gap-1.5">
          <input
            type="radio"
            className="accent-primary"
            name="pf-kind"
            checked={!reject}
            onChange={() => setReject(false)}
          />
          {m.promise_resolve()}
        </label>
        <label className="flex items-center gap-1.5">
          <input
            type="radio"
            className="accent-primary"
            name="pf-kind"
            checked={reject}
            onChange={() => setReject(true)}
          />
          {m.promise_reject()}
        </label>
      </div>
      <Label htmlFor="pf-value">{reject ? m.promise_reject_message() : m.promise_value()}</Label>
      <Textarea
        id="pf-value"
        className="font-mono text-xs"
        rows={2}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        spellCheck={false}
      />
      {error ? (
        <p role="alert" className="text-destructive">
          {error}
        </p>
      ) : null}
      <RoleGate have={caps} need="invoke">
        {(blocked) => (
          <div className="flex items-center gap-3">
            <Button
              type="submit"
              disabled={!!blocked || send.isPending || name === ""}
              title={blocked ?? undefined}
            >
              {reject ? m.promise_reject() : m.promise_resolve()}
            </Button>
            {blocked ? <span className="text-sm text-muted-foreground">{blocked}</span> : null}
          </div>
        )}
      </RoleGate>
    </form>
  );
}
