// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Awakeable: resolve (JSON) or reject (code, message) — `POST
// /awakeables/{id}/{resolve|reject}`. Pasting `awk_…` into Cmd+K lands here. Needs
// `invoke`: a `view` key sees the form, buttons disabled with a reason.
import { useMutation } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
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
import { invocationOfAwakeable } from "@/lib/jump";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/awakeables/$id")({ component: Awakeable });

function Awakeable() {
  const { id } = Route.useParams();
  const toast = useToast();
  const caps = useCapabilities();
  const [reject, setReject] = useState(false);
  const [value, setValue] = useState('""');
  const [code, setCode] = useState("500");
  const [error, setError] = useState<string | null>(null);
  const owner = invocationOfAwakeable(id);
  const send = useMutation({
    mutationFn: async () =>
      reject
        ? ok(
            rest.POST("/awakeables/{id}/reject", {
              params: { path: { id } },
              body: { code: Number(code), message: value },
            }),
          )
        : ok(
            rest.POST("/awakeables/{id}/resolve", {
              params: { path: { id } },
              body: JSON.parse(value),
            }),
          ),
    onSuccess: () => toast.add({ title: reject ? m.awakeable_rejected() : m.awakeable_resolved() }),
    onError: (e) =>
      setError(
        e instanceof SyntaxError
          ? m.schedule_input_invalid({ message: e.message })
          : actionError(e),
      ),
  });
  return (
    <div className="flex max-w-xl flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold break-all">{m.awakeable_title({ id })}</h1>
      {owner ? (
        <Link
          to="/invocations/$id"
          params={{ id: owner }}
          className="text-primary underline-offset-4 hover:underline"
        >
          {m.awakeable_owner()}
        </Link>
      ) : null}
      <form
        className="flex flex-col gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setError(null);
          send.mutate();
        }}
      >
        <div className="flex gap-4" role="radiogroup" aria-label={m.promise_outcome()}>
          <label className="flex items-center gap-1.5">
            <input
              type="radio"
              className="accent-primary"
              name="awk-kind"
              checked={!reject}
              onChange={() => setReject(false)}
            />
            {m.promise_resolve()}
          </label>
          <label className="flex items-center gap-1.5">
            <input
              type="radio"
              className="accent-primary"
              name="awk-kind"
              checked={reject}
              onChange={() => setReject(true)}
            />
            {m.promise_reject()}
          </label>
        </div>
        {reject ? (
          <>
            <Label htmlFor="awk-code">{m.awakeable_code()}</Label>
            <Input
              id="awk-code"
              type="number"
              min={0}
              value={code}
              onChange={(e) => setCode(e.target.value)}
            />
          </>
        ) : null}
        <Label htmlFor="awk-value">{reject ? m.promise_reject_message() : m.promise_value()}</Label>
        <Textarea
          id="awk-value"
          className="font-mono text-xs"
          rows={3}
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
                disabled={!!blocked || send.isPending}
                title={blocked ?? undefined}
              >
                {reject ? m.promise_reject() : m.promise_resolve()}
              </Button>
              {blocked ? <span className="text-sm text-muted-foreground">{blocked}</span> : null}
            </div>
          )}
        </RoleGate>
      </form>
    </div>
  );
}
