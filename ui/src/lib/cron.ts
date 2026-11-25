// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Plain-language description of a 5-field cron (Schedules). Only the common shapes;
// anything else returns `null` — the screen shows the raw expression, no guessing. The
// server checks the syntax (`PUT /schedules/{name}` ⇒ 400 with a reason).
import { m } from "@/paraglide/messages";

const DAY_NAMES = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"] as const;

function int(s: string, lo: number, hi: number): number | null {
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return n >= lo && n <= hi ? n : null;
}

const pad = (n: number) => String(n).padStart(2, "0");

/** Weekday lists `1-5`, `1,3,5`, `mon-fri` ⇒ indexes 0..6; other shapes ⇒ null. */
function days(s: string): number[] | null {
  const one = (t: string): number | null => {
    const i = DAY_NAMES.indexOf(t.toLowerCase() as (typeof DAY_NAMES)[number]);
    return i >= 0 ? i : int(t, 0, 7) === 7 ? 0 : int(t, 0, 6);
  };
  const out: number[] = [];
  for (const part of s.split(",")) {
    const [a, b] = part.split("-");
    const x = a === undefined ? null : one(a);
    if (x === null) return null;
    if (b === undefined) out.push(x);
    else {
      const y = one(b);
      if (y === null || y < x) return null;
      for (let i = x; i <= y; i++) out.push(i);
    }
  }
  return out;
}

export function describeCron(expr: string, dayName: (d: number) => string): string | null {
  const f = expr.trim().split(/\s+/);
  if (f.length !== 5) return null;
  const [min, hour, dom, mon, dow] = f as [string, string, string, string, string];
  if (dom !== "*" || mon !== "*") return null;
  const every = /^\*\/(\d+)$/;
  if (hour === "*" && dow === "*") {
    if (min === "*") return m.cron_every_minute();
    const e = every.exec(min);
    if (e) return m.cron_every_n_minutes({ n: e[1] ?? "" });
    const mm = int(min, 0, 59);
    return mm === null ? null : m.cron_hourly_at({ m: pad(mm) });
  }
  const mm = int(min, 0, 59);
  if (mm === null) return null;
  const eh = every.exec(hour);
  if (eh && dow === "*") return m.cron_every_n_hours({ n: eh[1] ?? "", m: pad(mm) });
  const hh = int(hour, 0, 23);
  if (hh === null) return null;
  const at = `${pad(hh)}:${pad(mm)}`;
  if (dow === "*") return m.cron_daily_at({ at });
  const d = days(dow);
  if (!d) return null;
  if (d.join() === "1,2,3,4,5") return m.cron_weekdays_at({ at });
  return m.cron_days_at({ at, days: d.map(dayName).join(", ") });
}

/** Weekday names per locale (0 = Sunday). */
export function weekdayName(locale: string) {
  const f = new Intl.DateTimeFormat(locale, { weekday: "short", timeZone: "UTC" });
  // 2023-01-01 is a Sunday.
  return (d: number) => f.format(Date.UTC(2023, 0, 1 + d));
}

/** Period in readable units: "Every minute", "Every 5 min", "Every 2 h". */
export function describeEvery(ms: number): string {
  const sec = ms / 1000;
  if (sec % 3600 === 0)
    return sec === 3600 ? m.schedule_every_hour() : m.schedule_every_n_h({ n: String(sec / 3600) });
  if (sec % 60 === 0)
    return sec === 60 ? m.schedule_every_minute() : m.schedule_every_n_min({ n: String(sec / 60) });
  return m.schedule_every_n_s({ n: String(sec) });
}
