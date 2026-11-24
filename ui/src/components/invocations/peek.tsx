// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Side panel of the list: clicking a row ⇒ quick look; Enter or ⌘-click on the id
// ⇒ the full page.
import { Link } from "@tanstack/react-router";
import { X } from "lucide-react";
import { Id } from "@/components/cairn/id";
import { QueryState } from "@/components/cairn/query-state";
import { StatusBadge } from "@/components/cairn/status-badge";
import { Time } from "@/components/cairn/time";
import { splitError } from "@/components/invocation/journal";
import { useQueue } from "@/components/invocation/why";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { useMedia, WIDE } from "@/lib/media";
import { m } from "@/paraglide/messages";

export type InvocationDetail = {
  id: string;
  tenant: string;
  service: string;
  handler: string;
  key: string;
  kind: string;
  class: string;
  status: string | null;
  root: boolean;
  created_at: string;
  transitioned_at: string;
  ended_at: string | null;
  duration_ms: number | null;
  scope: string | null;
  parent_id: string | null;
  schedule: string | null;
  idempotency_key: string | null;
  deployment: string | null;
  tags: string | null;
  failure_code: number | null;
  failure_message: string | null;
  attempt: number | null;
  retry_at: string | null;
  last_failure_code: number | null;
  last_failure_message: string | null;
  last_failure_at: string | null;
  pinned_deployment: string | null;
  root_id: string | null;
  journal_len: number | null;
  /** `null` for invocations created before the column existed (or on an old server). */
  limit_key?: string | null;
};

/**
 * Displayed status: the server's detailed `status`, falling back to `class`.
 * `inLine`: its own queue entry is still `inbox` (`sys_vqueue_entry`, see
 * `useQueue`) ⇒ "queued" instead of "running" — a server fact from another table,
 * not a guess.
 */
export const shownStatus = (r: { status: string | null; class: string }, inLine = false) =>
  inLine && r.class === "active" ? "queued" : (r.status ?? r.class);

export function Peek({ id, onClose }: { id: string; onClose: () => void }) {
  const wide = useMedia(WIDE);
  // Below `lg`: a sheet covering the right side ("side panel becomes a sheet"); the
  // panel used to be hidden and clicking a row seemed to do nothing.
  if (!wide) {
    return (
      <Dialog open onOpenChange={(o) => !o && onClose()}>
        <DialogContent className="top-0 right-0 left-auto h-dvh max-w-sm translate-x-0 translate-y-0 content-start rounded-none sm:max-w-sm">
          <DialogTitle>{m.peek_title()}</DialogTitle>
          <PeekBody id={id} />
        </DialogContent>
      </Dialog>
    );
  }
  return (
    <aside
      aria-label={m.peek_title()}
      className="flex w-96 shrink-0 flex-col gap-3 overflow-y-auto border-l border-border p-4"
    >
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold">{m.peek_title()}</h2>
        <Button variant="ghost" size="icon" aria-label={m.peek_close()} onClick={onClose}>
          <X aria-hidden />
        </Button>
      </div>
      <PeekBody id={id} />
    </aside>
  );
}

function PeekBody({ id }: { id: string }) {
  const q = useRows<InvocationDetail>(queries.invocation(id));
  const queue = useQueue(q.data?.rows[0], false);
  return (
    <>
      <QueryState q={q}>
        {({ rows: [r] }) =>
          r ? (
            <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5">
              <dt className="text-muted-foreground">{m.col_id()}</dt>
              <dd>
                <Id value={r.id} />
              </dd>
              <dt className="text-muted-foreground">{m.col_target()}</dt>
              <dd>
                {r.service}/{r.handler}
                {r.key ? ` · ${r.key}` : ""}
              </dd>
              <dt className="text-muted-foreground">{m.col_status()}</dt>
              <dd>
                <StatusBadge status={shownStatus(r, queue.inLine)} />
              </dd>
              <dt className="text-muted-foreground">{m.col_created()}</dt>
              <dd>
                <Time ms={Date.parse(r.created_at)} />
              </dd>
              {r.last_failure_message || r.failure_message ? (
                <>
                  <dt className="text-muted-foreground">{m.col_last_error()}</dt>
                  <dd className="break-words">
                    {splitError(r.failure_message ?? r.last_failure_message ?? "").head}
                  </dd>
                </>
              ) : null}
            </dl>
          ) : (
            <p>{m.not_found_body()}</p>
          )
        }
      </QueryState>
      <Link
        to="/invocations/$id"
        params={{ id }}
        className="text-primary underline-offset-4 hover:underline"
      >
        {m.peek_open()}
      </Link>
    </>
  );
}
