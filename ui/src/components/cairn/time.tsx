// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Relative time, absolute on hover. Copying always yields ISO UTC (`dateTime`). The
// display time zone is selectable — the browser's or UTC — in the user menu,
// remembered in `localStorage`. Relative text updates itself on one shared 30 s tick.
import { useSyncExternalStore } from "react";
import { getLocale } from "@/paraglide/runtime";

export type TimeZonePref = "local" | "utc";
const ZONE_KEY = "cairn-time-zone";

function storedZone(): TimeZonePref {
  try {
    return localStorage.getItem(ZONE_KEY) === "utc" ? "utc" : "local";
  } catch {
    return "local";
  }
}

let zone: TimeZonePref = typeof window === "undefined" ? "local" : storedZone();
const zoneListeners = new Set<() => void>();

export function setTimeZonePref(z: TimeZonePref) {
  zone = z;
  try {
    localStorage.setItem(ZONE_KEY, z);
  } catch {}
  for (const l of zoneListeners) l();
}

/** The selected time zone; components reading it redraw when the user changes it. */
export function useTimeZonePref(): TimeZonePref {
  return useSyncExternalStore(
    (l) => {
      zoneListeners.add(l);
      return () => zoneListeners.delete(l);
    },
    () => zone,
  );
}

/** `Intl` `timeZone` for the current choice (`undefined` = the browser's). */
const intlZone = () => (zone === "utc" ? "UTC" : undefined);

// One shared tick for every `Time` on the page, not one timer per cell.
const TICK_MS = 30_000;
let now = Date.now();
const tickListeners = new Set<() => void>();
let ticker: ReturnType<typeof setInterval> | null = null;

function useNow(): number {
  return useSyncExternalStore(
    (l) => {
      tickListeners.add(l);
      ticker ??= setInterval(() => {
        now = Date.now();
        for (const f of tickListeners) f();
      }, TICK_MS);
      return () => {
        tickListeners.delete(l);
        if (tickListeners.size === 0 && ticker) {
          clearInterval(ticker);
          ticker = null;
        }
      };
    },
    () => now,
  );
}

const UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
  ["year", 365 * 24 * 3600_000],
  ["month", 30 * 24 * 3600_000],
  ["day", 24 * 3600_000],
  ["hour", 3600_000],
  ["minute", 60_000],
  ["second", 1000],
];

/** "3 minutes ago" (or its vi equivalent) — the largest unit with |Δ| ≥ 1. */
export function relative(ms: number, now: number, locale = getLocale()): string {
  const delta = ms - now;
  const fmt = new Intl.RelativeTimeFormat(locale, { numeric: "auto", style: "short" });
  for (const [unit, size] of UNITS) {
    if (Math.abs(delta) >= size || unit === "second") {
      return fmt.format(Math.round(delta / size), unit);
    }
  }
  return fmt.format(0, "second");
}

/** Full date and time with the time-zone name ("… 7:05:03 AM UTC"). */
export function absolute(ms: number, locale = getLocale(), timeZone = intlZone()): string {
  return new Intl.DateTimeFormat(locale, {
    dateStyle: "medium",
    timeStyle: "long",
    timeZone,
  }).format(ms);
}

/**
 * Clock time with milliseconds, `07:05:03.042` — for log lines, where the order and
 * spacing between lines is the information; "3 minutes ago" on every line would be
 * unreadable.
 */
export function clock(ms: number, timeZone = intlZone()): string {
  const parts = new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    timeZone,
  }).formatToParts(ms);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "00";
  const milli = String(((ms % 1000) + 1000) % 1000).padStart(3, "0");
  return `${get("hour")}:${get("minute")}:${get("second")}.${milli}`;
}

export function Time({
  ms,
  now: fixed,
  timeZone,
}: {
  ms: number;
  /** Fixed "now" (stories, tests); absent ⇒ the shared tick. */
  now?: number;
  timeZone?: string;
}) {
  // Subscribe to the tick and the time zone to redraw; "now" is read at render time.
  useNow();
  useTimeZonePref();
  const at = fixed ?? Date.now();
  return (
    <time
      dateTime={new Date(ms).toISOString()}
      title={absolute(ms, undefined, timeZone ?? intlZone())}
      className="whitespace-nowrap tabular"
    >
      {relative(ms, at)}
    </time>
  );
}
