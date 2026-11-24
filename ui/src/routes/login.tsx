// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Sign-in: a hostname with SSO ⇒ the "Sign in with …" button is the main path,
// the API key always remains below. Paste an API key once, exchange it for a
// session cookie. Pasting is allowed (WCAG 3.3.8) — no paste blocking, no
// retyping. SSO errors come back here as `?sso_error=<code>` (server, `ui.rs`).
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute, useRouter } from "@tanstack/react-router";
import { LogIn } from "lucide-react";
import { useState } from "react";
import { Button, buttonVariants } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError } from "@/data/api";
import { login, loginOptionsQuery, ssoHref, whoamiQuery } from "@/data/session";
import { ssoErrorText } from "@/lib/sso";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

type Search = { redirect?: string; sso_error?: string };

export const Route = createFileRoute("/login")({
  validateSearch: (s: Record<string, unknown>): Search => ({
    redirect: typeof s.redirect === "string" && s.redirect.startsWith("/") ? s.redirect : undefined,
    sso_error:
      typeof s.sso_error === "string" && /^[a-z_]{1,40}$/.test(s.sso_error)
        ? s.sso_error
        : undefined,
  }),
  component: Login,
});

function Login() {
  const { redirect, sso_error } = Route.useSearch();
  const router = useRouter();
  const qc = useQueryClient();
  const options = useQuery(loginOptionsQuery);
  const sso = options.data?.sso === true;
  const [key, setKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const shown = error ?? (sso_error ? ssoErrorText(sso_error) : null);
  return (
    <main id="main" className="mx-auto flex min-h-dvh max-w-sm flex-col justify-center gap-6 p-6">
      <h1 className="text-xl font-semibold">{m.login_title()}</h1>
      {shown ? (
        <p
          role="alert"
          className="rounded-md border border-destructive/40 px-3 py-2 text-destructive"
        >
          {shown}
        </p>
      ) : null}
      {sso ? (
        <div className="flex flex-col gap-2">
          <a
            href={ssoHref(redirect)}
            className={cn(buttonVariants({ size: "lg" }), "h-10 w-full text-base")}
          >
            <LogIn aria-hidden />
            {m.login_sso({ name: options.data?.displayName ?? m.login_sso_default() })}
          </a>
          <p className="text-sm text-muted-foreground">{m.login_sso_help()}</p>
          <div className="flex items-center gap-3 pt-2 text-sm text-muted-foreground">
            <hr aria-hidden className="flex-1 border-border" />
            <span>{m.login_or_key()}</span>
            <hr aria-hidden className="flex-1 border-border" />
          </div>
        </div>
      ) : null}
      <form
        className="flex flex-col gap-3"
        onSubmit={async (e) => {
          e.preventDefault();
          setBusy(true);
          setError(null);
          try {
            await login(key.trim());
            setKey("");
            await qc.invalidateQueries({ queryKey: whoamiQuery.queryKey });
            await router.navigate({ to: (redirect ?? "/") as "/" });
          } catch (err) {
            setError(
              err instanceof ApiError && err.status === 401
                ? m.login_invalid()
                : m.login_failed({ message: err instanceof Error ? err.message : String(err) }),
            );
          } finally {
            setBusy(false);
          }
        }}
      >
        <Label htmlFor="api-key">{m.login_key()}</Label>
        <Input
          id="api-key"
          type="password"
          autoComplete="off"
          spellCheck={false}
          value={key}
          onChange={(e) => setKey(e.target.value)}
          aria-describedby="api-key-help"
          aria-invalid={error ? true : undefined}
          required
        />
        <p id="api-key-help" className="text-muted-foreground">
          {m.login_help()}
        </p>
        {/* With SSO the SSO button is the main path: one primary button per screen. */}
        <Button
          type="submit"
          variant={sso ? "outline" : "default"}
          disabled={busy || key.trim() === ""}
        >
          {m.login_submit()}
        </Button>
      </form>
    </main>
  );
}
