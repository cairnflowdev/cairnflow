// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Central shortcut registry: one listener for the whole app, no stray listeners —
// the Supabase approach. The `?` sheet and the palette read the same registry.
//
// Keys: a single key (`/`, `?`), a combination `mod+k` (⌘ on macOS, Ctrl elsewhere),
// or a two-key sequence (`g o`) within 1 second. Ignored while typing in an input.
import { useEffect } from "react";

export type Shortcut = { keys: string; label: () => string; run: () => void };

const SEQ_MS = 1000;

/** Whether a key event comes from a place where text is being typed. */
function typing(e: KeyboardEvent): boolean {
  const t = e.target as HTMLElement | null;
  if (!t) return false;
  return t.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(t.tagName);
}

/** Canonical form of one press: `mod+k`, `?`, `g`. */
export function chord(e: Pick<KeyboardEvent, "key" | "metaKey" | "ctrlKey" | "altKey">): string {
  const key = e.key.length === 1 ? e.key.toLowerCase() : e.key;
  if (e.metaKey || e.ctrlKey) return `mod+${key}`;
  if (e.altKey) return `alt+${key}`;
  return e.key === "?" ? "?" : key;
}

/** Matcher: takes each press, returns the matching shortcut (sequences like `g o` included). */
export function matcher(source: Shortcut[] | (() => Shortcut[]), now: () => number = Date.now) {
  let prev: { key: string; at: number } | null = null;
  return (c: string): Shortcut | null => {
    const shortcuts = typeof source === "function" ? source() : source;
    const seq = prev && now() - prev.at <= SEQ_MS ? `${prev.key} ${c}` : null;
    const hit =
      (seq && shortcuts.find((s) => s.keys === seq)) || shortcuts.find((s) => s.keys === c) || null;
    // The first key of a sequence is remembered; cleared once a sequence matches.
    prev = hit && seq && hit.keys === seq ? null : { key: c, at: now() };
    return hit;
  };
}

/**
 * Keys of the open page (J/K/Enter/Esc of a list) — still go through the single
 * listener of `install`; pages declare them with `usePageShortcuts`.
 */
const page = new Set<Shortcut[]>();

export function pageShortcuts(): Shortcut[] {
  return [...page].flat();
}

/** Attach a page's keys to the registry while the page is open, remove them on leave. */
export function usePageShortcuts(list: Shortcut[]) {
  useEffect(() => {
    page.add(list);
    return () => {
      page.delete(list);
    };
  }, [list]);
}

/** Attach the registry to `window` (plus the open page's keys). Returns the detach function. */
export function install(shortcuts: Shortcut[]): () => void {
  const match = matcher(() => [...shortcuts, ...pageShortcuts()]);
  const on = (e: KeyboardEvent) => {
    const c = chord(e);
    // `mod+k` works even while typing (opens the palette from a search box); plain keys do not.
    if (typing(e) && !c.startsWith("mod+")) return;
    const hit = match(c);
    if (hit) {
      e.preventDefault();
      hit.run();
    }
  };
  window.addEventListener("keydown", on);
  return () => window.removeEventListener("keydown", on);
}

/** Display label of a key: `mod` ⇒ ⌘/Ctrl. */
export function display(keys: string): string[] {
  const mac = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform);
  return keys.split(" ").map((k) => k.replace("mod+", mac ? "⌘" : "Ctrl+").toUpperCase());
}
