// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Streams tab of the invocation detail: the invocation's user streams
// (`ctx.stream`) — `$log` and `$trace` have their own tabs. The stream list is the
// **durable** streams (`sys_stream`, question 38). Records: the invocation is still
// running ⇒ SSE `/streams/{name}` (runs on the leader through forwarding, records
// arrive at once even before they are durable); SSE fails or it has finished ⇒ poll
// the durable records (`sys_stream_record`) like the Log tab.
import { useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryState } from "@/components/cairn/query-state";
import { Segmented } from "@/components/cairn/segmented";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { type SseEvent, useSse } from "@/data/sse";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export type StreamRow = {
  name: string;
  chunks: number;
  records: number;
  total_bytes: number;
  dropped: number;
  last_offset: number | null;
};

type RecordRow = {
  offset: number;
  step: number;
  attempt: number;
  kind: string;
  body: string | null;
};

const LIVE = { minMs: 2000, maxMs: 2000 } as const;

/** User streams: drops system streams (`$log`, `$trace` — names starting with `$`). */
export function userStreams(rows: StreamRow[]): StreamRow[] {
  return rows.filter((r) => !r.name.startsWith("$"));
}

export function StreamsTab({ id, live }: { id: string; live: boolean }) {
  const q = useRows<StreamRow>(queries.streams(id), { refresh: live ? LIVE : false });
  const [picked, setPicked] = useState<string | null>(null);
  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs text-muted-foreground">
        {live ? m.streams_live_hint() : m.streams_durable_only()}
      </p>
      <QueryState q={q}>
        {({ rows }) => {
          const list = userStreams(rows);
          if (list.length === 0)
            return <EmptyState title={m.streams_empty()}>{m.streams_empty_body()}</EmptyState>;
          const name = list.some((s) => s.name === picked) ? (picked as string) : list[0]?.name;
          const s = list.find((x) => x.name === name);
          if (!s || !name) return null;
          return (
            <>
              {list.length > 1 ? (
                <Segmented
                  label={m.streams_pick()}
                  value={name}
                  onChange={setPicked}
                  options={list.map((x) => ({ value: x.name, label: x.name }))}
                />
              ) : null}
              <Summary s={s} />
              <Records id={id} name={name} live={live} />
            </>
          );
        }}
      </QueryState>
    </div>
  );
}

function Summary({ s }: { s: StreamRow }) {
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <p className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
      <span className="font-mono text-foreground">{s.name}</span>
      <span>{m.streams_records({ n: fmt.format(Number(s.records)) })}</span>
      <span>{m.bytes({ n: fmt.format(Number(s.total_bytes)) })}</span>
      {Number(s.dropped) > 0 ? (
        <span className="text-destructive">
          {m.streams_dropped({ n: fmt.format(Number(s.dropped)) })}
        </span>
      ) : null}
    </p>
  );
}

/** SSE events of a stream ⇒ rows like `sys_stream_record`. Pure — testable. */
export function sseRecordRows(events: SseEvent[]): RecordRow[] {
  const out: RecordRow[] = [];
  for (const e of events) {
    if (e.event !== "record" && e.event !== "gap" && e.event !== "superseded") continue;
    let v: { offset?: number; step?: number; attempt?: number; data?: unknown } = {};
    try {
      v = JSON.parse(e.data);
    } catch {
      continue;
    }
    if (typeof v.offset !== "number") continue;
    // Like `sys_stream_record.body`: the record's JSON, strings included — live and
    // durable records have the same shape.
    const body = e.event !== "record" ? e.data : JSON.stringify(v.data ?? null);
    out.push({
      offset: v.offset,
      step: v.step ?? 0,
      attempt: v.attempt ?? 0,
      kind: e.event === "record" ? "data" : e.event,
      body,
    });
  }
  return out;
}

function Records({ id, name, live }: { id: string; name: string; live: boolean }) {
  const sse = useSse(live ? `/invocations/${id}/streams/${encodeURIComponent(name)}` : null);
  const streaming = live && sse.state !== "failed";
  const q = useRows<RecordRow>(queries.streamRecords(id, name, -1), {
    refresh: live && !streaming ? LIVE : false,
    enabled: !streaming,
  });
  if (streaming) {
    const rows = sseRecordRows(sse.events);
    return (
      <>
        <p role="status" className="text-xs text-muted-foreground">
          {sse.state === "ended" ? m.streams_sealed() : m.streams_live()}
        </p>
        {rows.length === 0 ? (
          <EmptyState variant="inline" title={m.streams_no_records()} />
        ) : (
          <RecordList name={name} rows={rows} />
        )}
      </>
    );
  }
  return (
    <QueryState q={q}>
      {({ rows }) =>
        rows.length === 0 ? (
          <EmptyState variant="inline" title={m.streams_no_records()} />
        ) : (
          <RecordList name={name} rows={rows} />
        )
      }
    </QueryState>
  );
}

function RecordList({ name, rows }: { name: string; rows: RecordRow[] }) {
  return (
    <ol aria-label={m.streams_records_of({ name })} className="font-mono text-xs">
      {rows.map((r) => (
        <li
          key={r.offset}
          className="flex gap-3 border-t border-border py-1 whitespace-pre-wrap break-words"
        >
          <span className="w-12 shrink-0 text-right text-muted-foreground">{r.offset}</span>
          <span
            className="w-16 shrink-0 text-muted-foreground"
            title={m.streams_step_attempt({
              step: String(r.step),
              attempt: String(r.attempt),
            })}
          >
            {r.step}·{r.attempt}
          </span>
          {r.kind === "data" ? (
            <span className="min-w-0">{r.body}</span>
          ) : (
            <span className="min-w-0 text-muted-foreground">
              [{r.kind}] {r.body}
            </span>
          )}
        </li>
      ))}
    </ol>
  );
}
