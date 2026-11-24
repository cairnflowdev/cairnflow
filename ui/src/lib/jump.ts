// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Pasting an id into Cmd+K jumps straight to it: 48 hex ⇒ invocation, `dp_…` ⇒
// deployment, `awk_…` ⇒ awakeable, 6–47 hex ⇒ a prefix (the list filtered by id
// prefix). No match ⇒ `null` (the palette searches by text as usual).
export type Jump =
  | { kind: "invocation"; id: string }
  /** 6–47 hex: the invocation list filtered by id prefix (question 29). */
  | { kind: "prefix"; id: string }
  | { kind: "deployment"; id: string }
  | { kind: "awakeable"; id: string };

/** `awk_<48 hex invocation id><8 hex index>` ⇒ the id of the invocation waiting on it. */
export function invocationOfAwakeable(awk: string): string | null {
  return /^awk_([0-9a-f]{48})[0-9a-f]{8}$/.exec(awk)?.[1] ?? null;
}

export function parseJump(text: string): Jump | null {
  const t = text.trim();
  if (/^[0-9a-fA-F]{48}$/.test(t)) return { kind: "invocation", id: t.toLowerCase() };
  if (/^[0-9a-fA-F]{6,47}$/.test(t)) return { kind: "prefix", id: t.toLowerCase() };
  if (/^dp_[A-Za-z0-9_-]+$/.test(t)) return { kind: "deployment", id: t };
  if (invocationOfAwakeable(t)) return { kind: "awakeable", id: t };
  return null;
}
