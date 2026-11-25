// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Two-tier journal: headers from `sys_journal.entry_lite`, the body is loaded
// when an entry is opened. Types match `JournalEntryLite`/`JournalBody` of
// openapi.json (same JSON as REST `?decode=1`).
import type { components } from "@/data/api.gen";

export type EntryLite = components["schemas"]["JournalEntryLite"];
export type Body = components["schemas"]["JournalBody"];

export type JournalRow = { index: number; name: string | null; entry_lite: string };

export type Entry = {
  index: number;
  name: string | null;
  lite: EntryLite;
  /**
   * A command with no completion in an invocation that **has ended**: nobody
   * waits for it any more — it "did not finish". `settle` sets this flag and drops
   * `completed: false`, so `waitingOn` no longer sees it.
   */
  unfinished?: boolean;
};

export function parseRows(rows: JournalRow[]): Entry[] {
  return rows.map((r) => ({
    index: Number(r.index),
    name: r.name,
    lite: JSON.parse(r.entry_lite) as EntryLite,
  }));
}

/** Commands waiting for a result — source of the "what it is waiting for" banner (question 36). */
export function waitingOn(entries: Entry[]): Entry[] {
  return entries.filter((e) => e.lite.completed === false);
}

/** Displayed body: JSON is reformatted, UTF-8 verbatim, anything else base64. */
export function showBody(b: Body): { text: string; mono: boolean } {
  if (b.encoding === "json" && b.text !== undefined) {
    try {
      return { text: JSON.stringify(JSON.parse(b.text), null, 2), mono: true };
    } catch {
      return { text: b.text, mono: true };
    }
  }
  if (b.encoding === "utf8") return { text: b.text ?? "", mono: false };
  return { text: b.base64 ?? "", mono: true };
}

/**
 * Invocation has ended ⇒ commands with no completion become "did not finish".
 * Still running ⇒ unchanged.
 */
export function settle(entries: Entry[], ended: boolean): Entry[] {
  if (!ended) return entries;
  return entries.map((e) =>
    e.lite.completed === false
      ? { ...e, lite: { ...e.lite, completed: undefined }, unfinished: true }
      : e,
  );
}

/**
 * Failing step of the last attempt: the last command with no completion ⇒ it
 * failed **at** that command; none ⇒ **after** the last entry that is not `Output`
 * (the `Output` entry carries the error itself, it is not where it failed). Journal
 * not loaded ⇒ `journal_len − 1` like `failedAfter`.
 */
export function failedStep(
  entries: Entry[],
  journalLen: number | null | undefined,
): { index: number; at: boolean } | null {
  if (entries.length === 0) {
    const n = failedAfter(journalLen);
    return n === null ? null : { index: n, at: false };
  }
  const open = entries.findLast((e) => e.unfinished || e.lite.completed === false);
  if (open) return { index: open.index, at: true };
  const last = entries.findLast((e) => e.lite.kind !== "Output");
  return last ? { index: last.index, at: false } : null;
}

/**
 * Last entry written when the attempt failed: `journal_len − 1` (before errors
 * carried a command index, the banner said "failed after entry N"). Unknown ⇒ `null`.
 */
export function failedAfter(journalLen: number | null | undefined): number | null {
  return journalLen != null && journalLen > 0 ? journalLen - 1 : null;
}

/**
 * Error message of an attempt: the first line is the message, the rest is the
 * stack. SDKs sometimes send the stack folded into one line ("msg Error: msg at f
 * (file:…)") — cut at the first `…Error: ` or the first frame
 * `at f (file:|node:|http…)`.
 */
export function splitError(msg: string): { head: string; stack: string | null } {
  const nl = msg.indexOf("\n");
  if (nl >= 0) {
    const stack = msg.slice(nl + 1).replace(/\s+$/, "");
    return { head: msg.slice(0, nl).trim(), stack: stack.trim() === "" ? null : stack };
  }
  const cut = /\s(?=\w*Error: |at (?:async )?\S+ \((?:file:|node:|https?:|\/))/.exec(msg);
  if (!cut || cut.index === 0) return { head: msg.trim(), stack: null };
  return { head: msg.slice(0, cut.index).trim(), stack: msg.slice(cut.index + 1).trim() };
}

/** One journal row: a command, with the notification that completes it, if any. */
export type JournalLine = { entry: Entry; completion?: Entry };

/**
 * Merges completions into the row of the command they complete (`completes` =
 * the command's `awaits`) — the reader does not have to pair `Run` with
 * `RunCompletion` by hand. Notifications that match no command (invocation id of
 * a call, signal) stay as their own rows.
 */
export function pairEntries(entries: Entry[]): JournalLine[] {
  const out: JournalLine[] = [];
  const awaiting = new Map<number, JournalLine>();
  for (const e of entries) {
    const done = e.lite.completes;
    const line = done === undefined ? undefined : awaiting.get(done);
    if (done !== undefined && line) {
      line.completion = e;
      awaiting.delete(done);
      continue;
    }
    const own: JournalLine = { entry: e };
    out.push(own);
    if (e.lite.awaits !== undefined) awaiting.set(e.lite.awaits, own);
  }
  return out;
}

/** Entries a restart can run from: commands (not Input, not notifications, not Output/End). */
export function isRestartPoint(e: Entry): boolean {
  return (
    e.index >= 1 &&
    e.lite.completes === undefined &&
    e.lite.signal === undefined &&
    e.lite.kind !== "Output" &&
    e.lite.kind !== "End"
  );
}

/**
 * Selectable steps in the restart dialog: one option per command — keep the
 * journal before it, run again from it. A command is never split from its
 * completion.
 */
export function restartChoices(entries: Entry[]): Entry[] {
  return entries.filter(isRestartPoint);
}
