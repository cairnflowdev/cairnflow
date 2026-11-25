// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import {
  failedAfter,
  failedStep,
  pairEntries,
  parseRows,
  restartChoices,
  settle,
  showBody,
  splitError,
  waitingOn,
} from "./journal";

describe("two-tier journal", () => {
  it("waiting = commands with completed false", () => {
    const e = parseRows([
      { index: 0, name: "Input", entry_lite: '{"kind":"Input","hasBody":true}' },
      {
        index: 1,
        name: "Sleep",
        entry_lite: '{"kind":"Sleep","awaits":1,"completed":false,"wakeUpAtMs":9,"hasBody":false}',
      },
      {
        index: 2,
        name: "Run",
        entry_lite: '{"kind":"Run","awaits":2,"completed":true,"hasBody":false}',
      },
    ]);
    expect(waitingOn(e).map((x) => x.index)).toEqual([1]);
  });

  it("body: JSON reformatted, utf8 verbatim, base64 verbatim", () => {
    expect(showBody({ encoding: "json", size: 7, truncated: false, text: '{"a":1}' }).text).toBe(
      '{\n  "a": 1\n}',
    );
    expect(showBody({ encoding: "utf8", size: 2, truncated: false, text: "ok" })).toEqual({
      text: "ok",
      mono: false,
    });
    expect(showBody({ encoding: "base64", size: 3, truncated: false, base64: "/wAQ" }).text).toBe(
      "/wAQ",
    );
  });
});

describe("failedAfter", () => {
  it("last entry written; unknown ⇒ null", () => {
    expect(failedAfter(4)).toBe(3);
    expect(failedAfter(1)).toBe(0);
    expect(failedAfter(0)).toBeNull();
    expect(failedAfter(null)).toBeNull();
  });
});

// The demo's Fulfillment/run fails at `quality-check` — entry 13 has no
// completion, entry 14 is the `Output` carrying the error.
const failedRun = parseRows([
  { index: 0, name: "Input", entry_lite: '{"kind":"Input","hasBody":true}' },
  {
    index: 11,
    name: "Run",
    entry_lite: '{"kind":"Run","name":"pick-item-4","awaits":6,"completed":true,"hasBody":false}',
  },
  {
    index: 12,
    name: "RunCompletion",
    entry_lite: '{"kind":"RunCompletion","completes":6,"hasBody":true}',
  },
  {
    index: 13,
    name: "Run",
    entry_lite:
      '{"kind":"Run","name":"quality-check","awaits":7,"completed":false,"hasBody":false}',
  },
  {
    index: 14,
    name: "Output",
    entry_lite:
      '{"kind":"Output","name":"output","failure":{"code":422,"message":"item damaged in QC"},"hasBody":false}',
  },
]);

describe("failing step", () => {
  it("ended invocation: no entry is 'waiting', the unfinished command 'did not finish'", () => {
    const s = settle(failedRun, true);
    expect(waitingOn(s)).toEqual([]);
    expect(s.find((e) => e.index === 13)?.unfinished).toBe(true);
    // Still running ⇒ still waiting.
    expect(waitingOn(settle(failedRun, false)).map((e) => e.index)).toEqual([13]);
  });

  it("fails at the unfinished command, not at the last Output entry", () => {
    expect(failedStep(failedRun, 15)).toEqual({ index: 13, at: true });
    // No unfinished command: after the last entry that is not Output.
    expect(failedStep(failedRun.slice(0, 3), 3)).toEqual({ index: 12, at: false });
    // Journal not loaded: falls back to journal_len − 1 as before.
    expect(failedStep([], 4)).toEqual({ index: 3, at: false });
  });
});

// Error messages, merged rows, restart choices.

describe("V4 — banner, journal, restart", () => {
  it("error: the first line is the message, the stack is separate — even when the stack is folded into one line", () => {
    expect(
      splitError(
        "payment gateway timeout\nError: payment gateway timeout\n    at capture (file:///s.ts:88:37)",
      ),
    ).toEqual({
      head: "payment gateway timeout",
      stack: "Error: payment gateway timeout\n    at capture (file:///s.ts:88:37)",
    });
    expect(
      splitError(
        "payment gateway timeout Error: payment gateway timeout at capture (file:///s.ts:88:37) at async runOnce (file:///h.ts:2311:35)",
      ).head,
    ).toBe("payment gateway timeout");
    expect(splitError("item damaged in QC")).toEqual({
      head: "item damaged in QC",
      stack: null,
    });
  });

  it("a completion is merged into the row of the command it completes", () => {
    const rows = pairEntries(failedRun);
    expect(rows.map((r) => [r.entry.index, r.completion?.index ?? null])).toEqual([
      [0, null],
      [11, 12],
      [13, null],
      [14, null],
    ]);
  });

  it("restart picks by command: no Input, no completion, no Output", () => {
    expect(restartChoices(failedRun).map((c) => c.index)).toEqual([11, 13]);
  });
});
