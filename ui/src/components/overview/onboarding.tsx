// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Onboarding when there is no deployment yet: the command that runs the sample SDK with
// a copy button, a field to paste a URI to register (`POST /deployments`).
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Rocket } from "lucide-react";
import { useState } from "react";
import { CopyButton } from "@/components/cairn/copy-button";
import { RoleGate } from "@/components/cairn/role-gate";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ok, rest } from "@/data/api";
import { useCapabilities } from "@/data/session";
import { m } from "@/paraglide/messages";

const COMMAND = "cd sdk/typescript && npm run example";

export function Onboarding() {
  const qc = useQueryClient();
  const caps = useCapabilities();
  const [uri, setUri] = useState("http://127.0.0.1:9080");
  const register = useMutation({
    mutationFn: (u: string) => ok(rest.POST("/deployments", { body: { uri: u } })),
    onSuccess: () => qc.invalidateQueries(),
  });
  return (
    <section
      aria-labelledby="onboarding"
      className="flex max-w-2xl flex-col gap-4 rounded-md border border-border p-6"
    >
      <div className="flex items-center gap-2">
        <Rocket aria-hidden className="size-5" />
        <h2 id="onboarding" className="text-base font-semibold">
          {m.onboarding_title()}
        </h2>
      </div>
      <ol className="flex list-decimal flex-col gap-4 pl-5">
        <li>
          <p>{m.onboarding_step_run()}</p>
          <div className="mt-2 flex items-center gap-2">
            <code className="rounded-sm bg-muted px-2 py-1 font-mono text-xs">{COMMAND}</code>
            <CopyButton value={COMMAND} label={m.onboarding_what()} />
          </div>
        </li>
        <li>
          <form
            className="flex flex-col gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              register.mutate(uri.trim());
            }}
          >
            <Label htmlFor="deployment-uri">{m.onboarding_step_register()}</Label>
            <div className="flex gap-2">
              <Input
                id="deployment-uri"
                value={uri}
                onChange={(e) => setUri(e.target.value)}
                spellCheck={false}
                aria-invalid={register.isError ? true : undefined}
                aria-describedby={register.isError ? "deployment-error" : undefined}
              />
              {/* Registering a deployment needs `operate` (a `view` key only looks). */}
              <RoleGate have={caps} need="operate">
                {(blocked) => (
                  <Button
                    type="submit"
                    title={blocked ?? undefined}
                    disabled={!!blocked || register.isPending || uri.trim() === ""}
                  >
                    {m.onboarding_register()}
                  </Button>
                )}
              </RoleGate>
            </div>
            {register.isError ? (
              <p id="deployment-error" role="alert" className="text-destructive">
                {register.error.message}
              </p>
            ) : null}
          </form>
        </li>
        <li>{m.onboarding_step_docs()}</li>
      </ol>
    </section>
  );
}
