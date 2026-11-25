// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Trial run of a handler (as in Temporal #1525): JSON input, object key, idempotency
// key, one tag ⇒ `POST /invoke/{service}/{handler}/send` ⇒ straight to the invocation
// page. Does not wait for the result — the detail page updates itself.
import { useMutation } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import { actionError } from "@/components/cairn/confirm";
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
import { Textarea } from "@/components/ui/textarea";
import { rest } from "@/data/api";
import { m } from "@/paraglide/messages";

export function RunHandler({
  service,
  handler,
  keyed,
  onClose,
}: {
  service: string;
  handler: string;
  /** Virtual object / workflow ⇒ needs an object key. */
  keyed: boolean;
  onClose: () => void;
}) {
  const navigate = useNavigate();
  const [input, setInput] = useState("{}");
  const [key, setKey] = useState("");
  const [idem, setIdem] = useState("");
  const [tag, setTag] = useState("");
  const [error, setError] = useState<string | null>(null);
  const run = useMutation({
    mutationFn: async () => {
      const body = input.trim() === "" ? null : JSON.parse(input);
      const { response } = await rest.POST("/invoke/{service}/{handler}/send", {
        params: {
          path: { service, handler },
          query: key ? { key } : undefined,
          header: {
            ...(idem ? { "Idempotency-Key": idem } : {}),
            ...(tag ? { "cairnflow-tag": tag } : {}),
          },
        },
        body,
      });
      const id = response.headers.get("x-cairn-invocation-id");
      if (!response.ok || !id) {
        let message = response.statusText;
        try {
          message = ((await response.clone().json()) as { error?: string }).error ?? message;
        } catch {}
        throw new Error(`${response.status}: ${message}`);
      }
      return id;
    },
    onSuccess: (id) => navigate({ to: "/invocations/$id", params: { id } }),
    onError: (e) =>
      setError(
        e instanceof SyntaxError
          ? m.schedule_input_invalid({ message: e.message })
          : actionError(e),
      ),
  });
  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{m.run_title({ target: `${service}/${handler}` })}</DialogTitle>
          <DialogDescription>{m.run_body()}</DialogDescription>
        </DialogHeader>
        <form
          className="flex flex-col gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            setError(null);
            run.mutate();
          }}
        >
          <Label htmlFor="run-input">{m.run_input()}</Label>
          <Textarea
            id="run-input"
            className="font-mono text-xs"
            rows={5}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            spellCheck={false}
          />
          {keyed ? (
            <>
              <Label htmlFor="run-key">{m.filter_key()}</Label>
              <Input
                id="run-key"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                required
                spellCheck={false}
              />
            </>
          ) : null}
          <Label htmlFor="run-idem">{m.filter_idem()}</Label>
          <Input
            id="run-idem"
            value={idem}
            onChange={(e) => setIdem(e.target.value)}
            spellCheck={false}
          />
          <Label htmlFor="run-tag">{m.run_tag()}</Label>
          <Input
            id="run-tag"
            value={tag}
            onChange={(e) => setTag(e.target.value)}
            placeholder="env=dev"
            spellCheck={false}
          />
          {error ? (
            <p role="alert" className="text-destructive">
              {error}
            </p>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              {m.confirm_cancel()}
            </Button>
            <Button type="submit" disabled={run.isPending || (keyed && key === "")}>
              {m.run_submit()}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
