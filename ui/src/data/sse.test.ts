// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { sseRecordRows } from "@/components/invocation/streams-tab";
import { sseLogRows } from "@/components/invocation/tabs";
import { parseSse } from "./sse";

describe("parseSse", () => {
  it("splits complete frames, keeps the unfinished part; drops keepalives; id and multi-line data", () => {
    const { events, rest } = parseSse(
      ':keepalive\n\nevent: record\nid: 3\ndata: {"offset":3}\n\ndata: a\ndata: b\n\nevent: rec',
    );
    expect(events).toEqual([
      { event: "record", data: '{"offset":3}', id: "3" },
      { event: "message", data: "a\nb", id: undefined },
    ]);
    expect(rest).toBe("event: rec");
  });

  it("CRLF like LF; an event with no data (end) is still an event", () => {
    const { events } = parseSse('event: end\r\ndata: {"next":4}\r\n\r\nevent: moved\r\n\r\n');
    expect(events.map((e) => e.event)).toEqual(["end", "moved"]);
  });
});

describe("sseLogRows", () => {
  it("record ⇒ a log row (data is the record), gap keeps its type; other events dropped", () => {
    const rows = sseLogRows([
      {
        event: "record",
        id: "0",
        data: JSON.stringify({ offset: 0, durable: false, data: { level: "info", msg: "x" } }),
      },
      { event: "gap", data: JSON.stringify({ offset: 1, dropped: 2 }) },
      { event: "end", data: "{}" },
      { event: "record", data: "not json" },
    ]);
    expect(rows).toEqual([
      { offset: 0, kind: "data", body: '{"level":"info","msg":"x"}', durable: false },
      { offset: 1, kind: "gap", body: '{"offset":1,"dropped":2}', durable: undefined },
    ]);
  });
});

describe("sseRecordRows", () => {
  it("record ⇒ a row like sys_stream_record: body is always JSON, strings included", () => {
    const rows = sseRecordRows([
      { event: "record", data: JSON.stringify({ offset: 0, step: 2, attempt: 1, data: "chunk" }) },
      { event: "record", data: JSON.stringify({ offset: 1, step: 2, attempt: 1, data: { a: 1 } }) },
      { event: "end", data: "{}" },
    ]);
    expect(rows).toEqual([
      { offset: 0, step: 2, attempt: 1, kind: "data", body: '"chunk"' },
      { offset: 1, step: 2, attempt: 1, kind: "data", body: '{"a":1}' },
    ]);
  });
});
