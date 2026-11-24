// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Confirm dialog by friction level: `confirm` — click to confirm; `type` — the button
// is enabled only after typing the exact string (the id's last 8 characters). Server
// errors show inside the dialog without closing it; it only closes once the server
// has returned `2xx`.
import { type ReactNode, useState } from "react";
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
import { ApiError } from "@/data/api";
import { m } from "@/paraglide/messages";

/** Message of an action error: codes meaningful to an operator come before the server's message. */
/**
 * `503` on a path by id/key: the range lives on another node (or has no leader yet).
 * The console asks for forwarding on every request, so a `503` with `leader` means
 * the node serving the console **cannot reach** that leader. `504`: forwarded to the
 * leader but no response — the action may have run. Say so plainly instead of
 * printing the server's raw message.
 */
export function leaderError(e: unknown): string | null {
  if (!(e instanceof ApiError)) return null;
  if (e.status === 504) return m.action_ambiguous();
  if (e.status !== 503) return null;
  return e.leader ? m.action_not_leader({ leader: e.leader }) : m.action_no_leader();
}

export function actionError(e: unknown): string {
  const leader = leaderError(e);
  if (leader) return leader;
  if (e instanceof ApiError) {
    if (e.status === 403) return m.action_forbidden();
    if (e.status === 429) return m.action_throttled();
    if (e.status === 409) return m.action_conflict({ message: e.message });
    if (e.status === 404) return m.action_not_found();
    return `${e.status}: ${e.message}`;
  }
  return e instanceof Error ? e.message : String(e);
}

export function ConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel,
  destructive,
  typeToConfirm,
  busy,
  error,
  onConfirm,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: ReactNode;
  confirmLabel: string;
  destructive?: boolean;
  /** String the user must type exactly; absent ⇒ just click to confirm. */
  typeToConfirm?: string;
  busy?: boolean;
  error?: string | null;
  onConfirm: () => void;
  children?: ReactNode;
}) {
  const [typed, setTyped] = useState("");
  const ready = !typeToConfirm || typed.trim() === typeToConfirm;
  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (!o) setTyped("");
        onOpenChange(o);
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <form
          className="flex flex-col gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            if (ready && !busy) onConfirm();
          }}
        >
          {children}
          {typeToConfirm ? (
            <>
              <Label htmlFor="type-to-confirm">{m.confirm_type({ text: typeToConfirm })}</Label>
              <Input
                id="type-to-confirm"
                autoComplete="off"
                spellCheck={false}
                className="font-mono"
                value={typed}
                onChange={(e) => setTyped(e.target.value)}
              />
            </>
          ) : null}
          {error ? (
            <p role="alert" className="text-destructive">
              {error}
            </p>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              {m.confirm_cancel()}
            </Button>
            <Button
              type="submit"
              variant={destructive ? "destructive" : "default"}
              disabled={!ready || busy}
            >
              {confirmLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
