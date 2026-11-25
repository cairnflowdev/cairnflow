// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The "why" banner of the invocation detail, in priority order: the final error /
// the latest attempt's error, paused (575, out of attempts), "what it is waiting
// for" — entries with no completion, "queued behind whom" — position in the
// object's queue, who holds the lock, waiting since when (question 41), then flow
// control — which limit is blocking the queue (questions 6, 44; only cluster-admin
// can read those RAM tables). Actions (resume…) come later.
import { Link } from "@tanstack/react-router";
import { CircleAlert, Gauge, Hourglass, ListOrdered, Lock, Pause } from "lucide-react";
import { shorten } from "@/components/cairn/id";
import { Time } from "@/components/cairn/time";
import type { InvocationDetail } from "@/components/invocations/peek";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import {
  blockingCounter,
  type LimitRow,
  type QueueEntry,
  type QueueFacts,
  queueFacts,
  queueOf,
  type VQueueRow,
} from "@/lib/queue";
import { m } from "@/paraglide/messages";
import { type Entry, failedStep, splitError, waitingOn } from "./journal";

function waitLabel(e: Entry) {
  const l = e.lite;
  switch (l.kind) {
    case "Sleep":
      return l.wakeUpAtMs ? (
        <>
          {m.why_sleep()} <Time ms={l.wakeUpAtMs} />
        </>
      ) : (
        m.why_sleep()
      );
    case "Call":
      return l.invocationId ? (
        <>
          {m.why_call({ target: `${l.service}/${l.handler}` })}{" "}
          <Link
            to="/invocations/$id"
            params={{ id: l.invocationId }}
            className="underline underline-offset-4"
          >
            {shorten(l.invocationId)}
          </Link>
        </>
      ) : (
        m.why_call({ target: `${l.service}/${l.handler}` })
      );
    case "GetPromise":
    case "PeekPromise":
      return m.why_promise({ name: l.promise ?? "" });
    case "Run":
      return m.why_run({ name: l.name ?? "" });
    case "AttachInvocation":
    case "GetInvocationOutput":
      return m.why_attach({ id: shorten(l.invocationId ?? "") });
    default:
      return m.why_entry({ kind: l.kind ?? "?", index: String(e.index) });
  }
}

/**
 * " · at entry N" (unfinished command) or " · after entry N" — links to the exact
 * journal entry (never to the `Output` entry that carries the error itself).
 */
function After({ r, entries }: { r: InvocationDetail; entries: Entry[] }) {
  const step = failedStep(entries, r.journal_len);
  if (step === null) return null;
  const n = String(step.index);
  const name = entries.find((e) => e.index === step.index)?.lite.name;
  return (
    <>
      {" · "}
      <Link
        to="/invocations/$id"
        params={{ id: r.id }}
        search={{ entry: step.index }}
        className="underline-offset-4 hover:underline"
      >
        {step.at ? m.why_at_entry({ n }) : m.why_after_entry({ n })}
        {step.at && name ? ` (${name})` : null}
      </Link>
    </>
  );
}

const LIVE = { minMs: 2000, maxMs: 2000 } as const;

/** Block reason (label `ResourceKind::label`) ⇒ readable text; `lock` already has the queue line. */
function flowReason(raw: string): string {
  if (raw.startsWith("concurrency")) return m.flow_reason_concurrency();
  if (raw === "turns") return m.flow_reason_turns();
  if (raw === "rate") return m.flow_reason_rate();
  if (raw === "cooldown") return m.flow_reason_cooldown();
  return raw;
}

/**
 * Items 4–5 of the banner: the object's queue (or its own entry for an unkeyed
 * service) and the leader's RAM queue. Only read while the invocation is `active`.
 */
export function useQueue(r: InvocationDetail | undefined, clusterAdmin: boolean) {
  const active = r?.class === "active";
  const keyed = !!r?.key;
  const ident = { scope: r?.scope ?? null, limitKey: r?.limit_key ?? null };
  const obj = useRows<QueueEntry>(
    queries.objectQueue(r?.tenant ?? "", r?.service ?? "", r?.key ?? "", ident),
    { enabled: active && keyed, refresh: LIVE },
  );
  const own = useRows<QueueEntry>(
    queries.queueEntry(r?.tenant ?? "", r?.service ?? "", r?.id ?? "", ident),
    { enabled: active && !keyed, refresh: LIVE },
  );
  const facts = r && active ? queueFacts((keyed ? obj : own).data?.rows ?? [], r.id) : null;
  const inLine = facts?.mine.stage === "inbox";
  const vq = useRows<VQueueRow>(queries.serviceQueues(r?.service ?? ""), {
    enabled: clusterAdmin && inLine,
    refresh: LIVE,
  });
  const queue = facts && vq.data ? queueOf(vq.data.rows, facts.mine, r?.key ?? "") : null;
  const flow = inLine && queue?.blocked_on && queue.blocked_on !== "lock" ? queue : null;
  const limits = useRows<LimitRow>(queries.limits(), {
    enabled: !!flow?.blocked_on?.startsWith("concurrency"),
    refresh: LIVE,
  });
  const counter = flow && limits.data ? blockingCounter(flow, limits.data.rows) : null;
  return { facts, flow, counter, inLine };
}

export type Queue = ReturnType<typeof useQueue>;

function HolderLink({ id }: { id: string }) {
  return (
    <Link to="/invocations/$id" params={{ id }} className="font-mono underline underline-offset-4">
      {shorten(id)}
    </Link>
  );
}

function QueueLine({ r, f }: { r: InvocationDetail; f: QueueFacts }) {
  const object = `${r.service} · ${r.key}`;
  if (f.mine.stage !== "inbox") {
    return f.waiting > 0 ? (
      <Line icon={<Lock />}>{m.why_holds_lock({ object, n: String(f.waiting) })}</Line>
    ) : null;
  }
  const pos = { object, pos: String(f.ahead + 1), n: String(f.waiting) };
  const [holder, ...more] = f.holders;
  return (
    <Line icon={<ListOrdered />}>
      {holder ? (
        <>
          {m.why_queued_behind(pos)} <HolderLink id={holder.id} />
          {more.length > 0 ? ` ${m.why_queued_more({ n: String(more.length) })}` : null}
        </>
      ) : (
        m.why_queued(pos)
      )}
      {" · "}
      {m.why_queued_since()} <Time ms={Date.parse(r.created_at)} />
    </Line>
  );
}

export function Why({
  r,
  entries,
  queue,
}: {
  r: InvocationDetail;
  /** Journal after `settle` — an ended invocation has no "waiting" entries left. */
  entries: Entry[];
  /**
   * Its queue (`useQueue`, shared with the page's "Queued" badge). The flow-control
   * RAM tables (`sys_vqueue`, `sys_limit`) are only readable by cluster-admin.
   */
  queue: Queue;
}) {
  const waiting = waitingOn(entries);
  const items: React.ReactNode[] = [];
  if (r.class === "failed" && (r.failure_code !== null || r.failure_message)) {
    items.push(
      <Line key="fail" icon={<CircleAlert className="text-destructive" />} tone="destructive">
        <strong>{m.why_failed({ code: String(r.failure_code ?? "") })}</strong>{" "}
        <ErrorText message={r.failure_message ?? ""}>
          <After r={r} entries={entries} />
        </ErrorText>
      </Line>,
    );
  } else if (r.last_failure_message) {
    items.push(
      <Line key="last" icon={<CircleAlert className="text-destructive" />} tone="destructive">
        <strong>
          {r.attempt
            ? m.why_last_failure_attempt({ attempt: String(r.attempt) })
            : m.why_last_failure()}
        </strong>{" "}
        {r.last_failure_code !== null ? `${r.last_failure_code}: ` : ""}
        <ErrorText message={r.last_failure_message}>
          <After r={r} entries={entries} />
          {r.last_failure_at ? (
            <>
              {" — "}
              <Time ms={Date.parse(r.last_failure_at)} />
            </>
          ) : null}
          {r.retry_at ? (
            <>
              {" · "}
              {m.why_retry_at()} <Time ms={Date.parse(r.retry_at)} />
            </>
          ) : null}
        </ErrorText>
      </Line>,
    );
  }
  if (r.class === "paused") {
    items.push(
      <Line key="paused" icon={<Pause />}>
        {r.failure_code === 575 ? m.why_paused_attempts() : m.why_paused()}
      </Line>,
    );
  }
  if (r.class === "active") {
    for (const e of waiting) {
      items.push(
        <Line key={`w${e.index}`} icon={<Hourglass />}>
          {waitLabel(e)}
        </Line>,
      );
    }
    // The object's queue: only for keyed invocations (exclusive queue); an unkeyed
    // service has many slots at once — "behind whom" has no meaning, only item 5 is left.
    if (queue.facts && r.key) {
      const line = <QueueLine key="queue" r={r} f={queue.facts} />;
      if (queue.facts.mine.stage === "inbox" || queue.facts.waiting > 0) items.push(line);
    }
    if (queue.flow?.blocked_on) {
      const c = queue.counter;
      items.push(
        <Line key="flow" icon={<Gauge />}>
          {m.why_flow({ reason: flowReason(queue.flow.blocked_on) })}
          {c
            ? ` — ${m.why_flow_counter({
                counter: c.counter,
                used: String(c.used),
                limit: c.limit === null ? "∞" : String(c.limit),
              })}${c.rule ? ` ${m.why_flow_rule({ rule: c.rule })}` : ""}`
            : null}
        </Line>,
      );
    }
  }
  if (items.length === 0) return null;
  return (
    <section
      aria-label={m.why_title()}
      className="flex flex-col gap-1 rounded-md border border-border bg-muted px-3 py-2"
    >
      {items}
    </section>
  );
}

/**
 * Error message: the first line reads at a glance, the stack is collapsed — the
 * whole stack is not dumped into one paragraph of the banner. `children` (step,
 * timestamp) come after the first line.
 */
function ErrorText({ message, children }: { message: string; children?: React.ReactNode }) {
  const { head, stack } = splitError(message);
  return (
    <>
      {head}
      {children}
      {stack ? (
        <details className="mt-1">
          <summary className="cursor-pointer text-xs text-muted-foreground">
            {m.why_stack()}
          </summary>
          <pre className="mt-1 max-h-64 overflow-auto rounded-sm bg-background p-2 font-mono text-xs whitespace-pre-wrap break-words">
            {stack}
          </pre>
        </details>
      ) : null}
    </>
  );
}

function Line({
  icon,
  children,
  tone,
}: {
  icon: React.ReactNode;
  children: React.ReactNode;
  tone?: "destructive";
}) {
  return (
    <div className="flex gap-2" data-tone={tone}>
      <span aria-hidden className="mt-0.5 shrink-0 [&_svg]:size-4">
        {icon}
      </span>
      <span className="break-words">{children}</span>
    </div>
  );
}
