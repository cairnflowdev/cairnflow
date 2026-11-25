// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Stream SSE: `GET /invocations/{id}/logs?follow=1` and `/streams/{name}` only run
// on the range leader; the console asks for forwarding (`x-cairn-forward: leader`),
// so it reads with a streaming `fetch` instead of `EventSource` (which cannot set
// headers). Reconnects with `Last-Event-ID` when the server reports `moved` (the
// leader's term ended) or closes the connection at the end of its life
// (`max_life`); `end` means the stream is sealed and fully read.
import { useEffect, useState } from "react";
import { FORWARD_HEADERS } from "./api";

export type SseEvent = { event: string; data: string; id?: string };

/**
 * Splits the **complete** events off `buf` (a frame ends with an empty line); the
 * unfinished part is returned in `rest`. `:` lines are comments (keepalive) — dropped. Pure.
 */
export function parseSse(buf: string): { events: SseEvent[]; rest: string } {
  const events: SseEvent[] = [];
  const text = buf.replace(/\r\n?/g, "\n");
  const frames = text.split("\n\n");
  const rest = frames.pop() ?? "";
  for (const frame of frames) {
    let event = "message";
    let id: string | undefined;
    const data: string[] = [];
    for (const line of frame.split("\n")) {
      if (line === "" || line.startsWith(":")) continue;
      const i = line.indexOf(":");
      const field = i < 0 ? line : line.slice(0, i);
      const value = i < 0 ? "" : line.slice(i + 1).replace(/^ /, "");
      if (field === "event") event = value;
      else if (field === "data") data.push(value);
      else if (field === "id") id = value;
    }
    if (data.length > 0 || event !== "message") events.push({ event, data: data.join("\n"), id });
  }
  return { events, rest };
}

export type SseState = "connecting" | "open" | "ended" | "failed";

/** Maximum reconnects (after `moved` or a closed connection) before giving up for polling. */
const RECONNECTS = 5;

/**
 * Reads SSE at `path` (`null` = do not read). `failed` ⇒ the caller falls back to
 * polling (`status` is the HTTP code when the server refuses, e.g. `410` stream gone).
 */
export function useSse(path: string | null): {
  events: SseEvent[];
  state: SseState;
  status?: number;
} {
  const [events, setEvents] = useState<SseEvent[]>([]);
  const [state, setState] = useState<SseState>("connecting");
  const [status, setStatus] = useState<number | undefined>(undefined);
  useEffect(() => {
    if (path === null) return;
    setEvents([]);
    setState("connecting");
    setStatus(undefined);
    const abort = new AbortController();
    let last: string | undefined;
    (async () => {
      for (let attempt = 0; attempt <= RECONNECTS; attempt++) {
        let res: Response;
        try {
          res = await fetch(path, {
            credentials: "same-origin",
            signal: abort.signal,
            headers: {
              accept: "text/event-stream",
              ...FORWARD_HEADERS,
              ...(last !== undefined ? { "last-event-id": last } : {}),
            },
          });
        } catch {
          if (abort.signal.aborted) return;
          setState("failed");
          return;
        }
        if (!res.ok || !res.body) {
          setStatus(res.status);
          setState("failed");
          return;
        }
        setState("open");
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        let moved = false;
        for (;;) {
          let chunk: ReadableStreamReadResult<Uint8Array>;
          try {
            chunk = await reader.read();
          } catch {
            if (abort.signal.aborted) return;
            break;
          }
          if (chunk.done) break;
          buf += decoder.decode(chunk.value, { stream: true });
          const { events: got, rest } = parseSse(buf);
          buf = rest;
          if (got.length === 0) continue;
          for (const e of got) if (e.id !== undefined) last = e.id;
          const kept = got.filter((e) => e.event !== "moved" && e.event !== "reset");
          if (kept.length > 0) setEvents((xs) => [...xs, ...kept]);
          if (got.some((e) => e.event === "end")) {
            setState("ended");
            abort.abort();
            return;
          }
          if (got.some((e) => e.event === "moved")) {
            moved = true;
            break;
          }
        }
        if (abort.signal.aborted) return;
        // `moved`, or closed before `end` (connection end of life): reconnect from the last id.
        if (moved) setState("connecting");
      }
      if (!abort.signal.aborted) setState("failed");
    })();
    return () => abort.abort();
  }, [path]);
  return { events, state, status };
}
