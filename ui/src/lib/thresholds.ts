// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Thresholds of "Needs attention" on the Overview: the defaults are constants; the
// user adjusts them in Settings, stored in this browser's `localStorage`. The server
// keeps no thresholds. Storage may be missing/throw (private window, blocked site
// data) ⇒ always falls back to the defaults, never breaks the screen.
import { useSyncExternalStore } from "react";

export type Thresholds = {
  /** A `failed / total` ratio above this ⇒ the service needs attention… */
  failedRate: number;
  /** …once it has at least this many runs. */
  minRuns: number;
};

export const DEFAULTS: Thresholds = { failedRate: 0.02, minRuns: 20 };
export const STORAGE_KEY = "cairn.thresholds";

/** Stored value ⇒ valid thresholds; a broken/missing field ⇒ that field's default. */
export function parseThresholds(raw: string | null): Thresholds {
  let v: Partial<Record<keyof Thresholds, unknown>> = {};
  try {
    v = raw ? (JSON.parse(raw) as typeof v) : {};
  } catch {
    v = {};
  }
  const rate =
    typeof v.failedRate === "number" && v.failedRate >= 0 && v.failedRate <= 1
      ? v.failedRate
      : null;
  const runs =
    typeof v.minRuns === "number" && Number.isInteger(v.minRuns) && v.minRuns >= 0
      ? v.minRuns
      : null;
  return { failedRate: rate ?? DEFAULTS.failedRate, minRuns: runs ?? DEFAULTS.minRuns };
}

function read(): string | null {
  try {
    return globalThis.localStorage?.getItem(STORAGE_KEY) ?? null;
  } catch {
    return null;
  }
}

const listeners = new Set<() => void>();
let cachedRaw: string | null | undefined;
let cached: Thresholds = DEFAULTS;

function snapshot(): Thresholds {
  const raw = read();
  if (raw !== cachedRaw) {
    cachedRaw = raw;
    cached = parseThresholds(raw);
  }
  return cached;
}

/** Save (or `null` ⇒ back to defaults). `false` when storage cannot be written. */
export function saveThresholds(t: Thresholds | null): boolean {
  let ok = true;
  try {
    if (t) globalThis.localStorage?.setItem(STORAGE_KEY, JSON.stringify(t));
    else globalThis.localStorage?.removeItem(STORAGE_KEY);
  } catch {
    ok = false;
  }
  for (const l of listeners) l();
  return ok;
}

export function useThresholds(): Thresholds {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    snapshot,
    () => DEFAULTS,
  );
}
