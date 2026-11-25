// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Pure schedule arithmetic: 5-field cron in an IANA time zone and
//! `every_ms`, `next(after)`.
//!
//! No I/O, no clock reads: `after`/`now` are always supplied by the caller (the
//! engine's wall time, `engine.clock().wall_ms()`). Every instant is Unix
//! milliseconds.
//!
//! **Cron.** 5 fields `minute hour day month weekday`; `*`, `,`, `-`, `/`;
//! month names (`JAN`–`DEC`) and weekday names (`SUN`–`SAT`, `7` = Sunday); no
//! `L` `W` `#`, no seconds field. Day and weekday are **OR**ed Vixie-style: when
//! both are restricted (the field does not start with `*`) a day matches when
//! either matches; when one of them is `*…` only the other decides.
//!
//! **DST taken literally, like Temporal.** The instants are the UTC times at
//! which the zone's wall clock matches the spec. A wall time that does not
//! exist (the spring gap) ⇒ no fire; a repeated wall time (the autumn fold) ⇒
//! two fires. One rule for fixed times and `*/15` alike.
//!
//! **`every_ms`** is anchored at the Unix epoch: the instants are multiples of
//! the period — every leader computes the same instants regardless of when the
//! schedule was created (CG-16), and there is no DST.
//!
//! `next` jumps field by field (month → day → hour → minute) on the wall clock
//! and then converts to UTC; it never walks every minute of a day that does not
//! match.

#![forbid(unsafe_code)]

use std::fmt;

use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{AmbiguousOffset, TimeZone};
use jiff::{Timestamp, ToSpan};

/// One minute, in milliseconds.
const MINUTE_MS: i64 = 60_000;
/// The largest wall-clock difference between two adjacent offsets of a zone
/// (DST, historical zone changes). Scanning this far around an instant misses
/// no fire in an autumn fold.
const FOLD_MAX_MS: i64 = 3 * 60 * MINUTE_MS;
/// The horizon of `next`: no match within this many years ⇒ `None`. Enough for
/// `29 2` (leap years, including the 8-year gap around 2100).
pub const HORIZON_YEARS: i16 = 9;
/// `PUT` requires the spec to match within this many years — otherwise an error.
pub const VALIDATE_YEARS: i64 = 5;

/// A spec error — the caller returns `400` with this message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecError(pub String);

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SpecError {}

fn err<T>(msg: impl Into<String>) -> Result<T, SpecError> {
    Err(SpecError(msg.into()))
}

/// A checked schedule spec.
#[derive(Clone, Debug)]
pub enum Spec {
    Cron(Cron),
    /// The period, ms, anchored at the epoch.
    Every(u64),
}

/// A parsed cron expression, with its time zone.
#[derive(Clone, Debug)]
pub struct Cron {
    expr: String,
    tz_name: String,
    tz: TimeZone,
    minutes: u64,
    hours: u32,
    /// Bits 1..=31.
    doms: u32,
    /// Bits 1..=12.
    months: u16,
    /// Bits 0..=6, 0 = Sunday.
    dows: u8,
    dom_star: bool,
    dow_star: bool,
}

impl Spec {
    /// A 5-field cron in zone `tz` (IANA; `UTC` is the default at the API layer).
    pub fn cron(expr: &str, tz: &str) -> Result<Spec, SpecError> {
        Ok(Spec::Cron(Cron::parse(expr, tz)?))
    }

    /// The period `every_ms`, no less than `min_ms` (`--schedule-min-period-ms`).
    pub fn every(every_ms: u64, min_ms: u64) -> Result<Spec, SpecError> {
        if every_ms == 0 || every_ms < min_ms {
            return err(format!("everyMs must be ≥ {min_ms}"));
        }
        if every_ms > i64::MAX as u64 / 4 {
            return err("everyMs is too large");
        }
        Ok(Spec::Every(every_ms))
    }

    /// The first instant **strictly after** `after` (ms), or `None` if there is
    /// none within [`HORIZON_YEARS`] years.
    pub fn next(&self, after: i64) -> Option<i64> {
        match self {
            Spec::Every(p) => {
                let p = *p as i64;
                after.div_euclid(p).checked_add(1)?.checked_mul(p)
            }
            Spec::Cron(c) => c.next(after),
        }
    }

    /// Checks that the spec has an instant within [`VALIDATE_YEARS`] years from
    /// `now` — `0 0 30 2 *` does not (`PUT` returns `400`).
    pub fn validate(&self, now: i64) -> Result<(), SpecError> {
        let limit = now.saturating_add(VALIDATE_YEARS * 366 * 24 * 60 * MINUTE_MS);
        match self.next(now) {
            Some(t) if t <= limit => Ok(()),
            _ => err(format!(
                "the schedule has no fire time in the next {VALIDATE_YEARS} years"
            )),
        }
    }

    /// The next `n` instants after `after` (for `GET /schedules/{name}`).
    pub fn upcoming(&self, after: i64, n: usize) -> Vec<i64> {
        let mut out = Vec::with_capacity(n);
        let mut at = after;
        while out.len() < n {
            match self.next(at) {
                Some(t) => {
                    out.push(t);
                    at = t;
                }
                None => break,
            }
        }
        out
    }

    /// The number of instants in `(after, until]`, counted up to `cap` (missed instants).
    pub fn count_between(&self, after: i64, until: i64, cap: u64) -> u64 {
        if until <= after {
            return 0;
        }
        if let Spec::Every(p) = self {
            let p = *p as i64;
            let n = until.div_euclid(p) - after.div_euclid(p);
            return (n.max(0) as u64).min(cap);
        }
        let mut n = 0;
        let mut at = after;
        while n < cap {
            match self.next(at) {
                Some(t) if t <= until => {
                    n += 1;
                    at = t;
                }
                _ => break,
            }
        }
        n
    }

    /// The **last** instant in `(after, until]` — the most recent miss, fired once.
    pub fn last_between(&self, after: i64, until: i64) -> Option<i64> {
        if until <= after {
            return None;
        }
        if let Spec::Every(p) = self {
            let p = *p as i64;
            let t = until.div_euclid(p) * p;
            return (t > after).then_some(t);
        }
        // Cron: step back block by block, scanning forward in each block — do
        // not walk from `after` when `after` is very far back (a schedule paused
        // for a long time).
        let mut span = 2 * 60 * MINUTE_MS;
        loop {
            let from = until.saturating_sub(span).max(after);
            let mut last = None;
            let mut at = from;
            while let Some(t) = self.next(at) {
                if t > until {
                    break;
                }
                last = Some(t);
                at = t;
            }
            if last.is_some() || from == after {
                return last;
            }
            span = span.saturating_mul(4);
        }
    }

    /// The printed form (`cron@tz` or `every:ms`) — for error messages and logs.
    pub fn describe(&self) -> String {
        match self {
            Spec::Every(p) => format!("every:{p}"),
            Spec::Cron(c) => format!("{}@{}", c.expr, c.tz_name),
        }
    }
}

/// The `jitter` delay of one fire: `hash mod jitter_ms`, clamped below the next
/// instant so the fire does not slide past the next instant. `hash` is a hash
/// of `(schedule id, at)` computed by the caller — same input, same result on
/// every leader.
pub fn jittered(at: i64, next: Option<i64>, jitter_ms: u64, hash: u64) -> i64 {
    if jitter_ms == 0 {
        return at;
    }
    // Unsigned addition: `(hash % jitter_ms) as i64` is negative when
    // `jitter_ms > i64::MAX` ⇒ the fire would land before the instant. The
    // caller clamps `jitter_ms` when accepting the schedule; the arithmetic must
    // still never flip the sign.
    let t = at.saturating_add_unsigned(hash % jitter_ms);
    match next {
        Some(n) if t >= n => (n - 1).max(at),
        _ => t,
    }
}

impl Cron {
    fn parse(expr: &str, tz_name: &str) -> Result<Cron, SpecError> {
        let tz = match TimeZone::get(tz_name) {
            Ok(tz) => tz,
            Err(_) => return err(format!("unknown timeZone: {tz_name}")),
        };
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return err(format!(
                "cron needs exactly 5 fields (minute hour day-of-month month day-of-week), got {}",
                fields.len()
            ));
        }
        let minutes = field(fields[0], 0, 59, &[], "minute")?;
        let hours = field(fields[1], 0, 23, &[], "hour")?;
        let doms = field(fields[2], 1, 31, &[], "day-of-month")?;
        let months = field(fields[3], 1, 12, &MONTHS, "month")?;
        let mut dows = field(fields[4], 0, 7, &DAYS, "day-of-week")?;
        // `7` is Sunday, like `0`.
        if dows & (1 << 7) != 0 {
            dows = (dows | 1) & !(1 << 7);
        }
        Ok(Cron {
            expr: fields.join(" "),
            tz_name: tz_name.to_string(),
            tz,
            minutes,
            hours: hours as u32,
            doms: doms as u32,
            months: months as u16,
            dows: dows as u8,
            dom_star: fields[2].starts_with('*'),
            dow_star: fields[4].starts_with('*'),
        })
    }

    /// Whether wall time `c` matches the spec (seconds and below must be 0).
    /// The spec's direct definition — tests use it as the oracle for
    /// [`Cron::next`].
    pub fn matches(&self, c: DateTime) -> bool {
        c.second() == 0
            && c.subsec_nanosecond() == 0
            && self.minutes & (1 << c.minute()) != 0
            && self.hours & (1 << c.hour()) != 0
            && self.months & (1 << c.month()) != 0
            && self.day_matches(c.date())
    }

    /// The spec's time zone.
    pub fn time_zone(&self) -> &TimeZone {
        &self.tz
    }

    fn day_matches(&self, d: Date) -> bool {
        let dom = self.doms & (1 << d.day()) != 0;
        let dow = self.dows & (1 << d.weekday().to_sunday_zero_offset()) != 0;
        match (self.dom_star, self.dow_star) {
            (true, true) => true,
            (true, false) => dow,
            (false, true) => dom,
            (false, false) => dom || dow,
        }
    }

    /// The first matching wall time **≥** `start` (truncated to the minute), up to `end`.
    fn next_civil(&self, start: DateTime, end: Date) -> Option<DateTime> {
        let mut d = start.date();
        let mut h = start.hour() as u32;
        let mut m = start.minute() as u32;
        loop {
            if d > end {
                return None;
            }
            if self.months & (1 << d.month()) == 0 {
                // Go to day 1 of the next month.
                d = d.first_of_month().checked_add(1.month()).ok()?;
                h = 0;
                m = 0;
                continue;
            }
            if !self.day_matches(d) {
                d = d.tomorrow().ok()?;
                h = 0;
                m = 0;
                continue;
            }
            let Some(hh) = next_bit(self.hours as u64, h, 23) else {
                d = d.tomorrow().ok()?;
                h = 0;
                m = 0;
                continue;
            };
            if hh != h {
                h = hh;
                m = 0;
            }
            let Some(mm) = next_bit(self.minutes, m, 59) else {
                if h == 23 {
                    d = d.tomorrow().ok()?;
                    h = 0;
                } else {
                    h += 1;
                }
                m = 0;
                continue;
            };
            return Some(d.to_datetime(Time::constant(h as i8, mm as i8, 0, 0)));
        }
    }

    fn next(&self, after: i64) -> Option<i64> {
        // Scan from [`FOLD_MAX_MS`] before `after`: `after` may lie in the
        // second pass of an autumn fold, where the wall clock has gone back.
        let from = Timestamp::from_millisecond(after.saturating_sub(FOLD_MAX_MS)).ok()?;
        let start = self.tz.to_datetime(from);
        let start = start
            .date()
            .to_datetime(Time::constant(start.hour(), start.minute(), 0, 0));
        let end = start
            .date()
            .checked_add((HORIZON_YEARS as i32).years())
            .ok()?;
        let mut cursor = start;
        let mut best: Option<i64> = None;
        loop {
            let c = self.next_civil(cursor, end)?;
            if let Some(b) = best {
                // An instant earlier than `b` must have a wall time within ±FOLD_MAX of
                // `b`'s wall time; beyond that, stop.
                let bc = self.tz.to_datetime(Timestamp::from_millisecond(b).ok()?);
                if c > bc
                    .checked_add(jiff::Span::new().milliseconds(FOLD_MAX_MS))
                    .ok()?
                {
                    return best;
                }
            }
            for t in instants(&self.tz, c) {
                if t > after && best.is_none_or(|b| t < b) {
                    best = Some(t);
                }
            }
            cursor = c.checked_add(1.minute()).ok()?;
        }
    }
}

/// The instants (ms) that wall time `c` in zone `tz` maps to: 0 (spring gap),
/// 1, or 2 (autumn fold).
fn instants(tz: &TimeZone, c: DateTime) -> Vec<i64> {
    let amb = tz.to_ambiguous_timestamp(c);
    let at = |off: jiff::tz::Offset| -> Option<i64> {
        off.to_timestamp(c).ok().map(|t| t.as_millisecond())
    };
    match amb.offset() {
        AmbiguousOffset::Unambiguous { offset } => at(offset).into_iter().collect(),
        AmbiguousOffset::Gap { .. } => Vec::new(),
        AmbiguousOffset::Fold { before, after } => {
            let mut v: Vec<i64> = [at(before), at(after)].into_iter().flatten().collect();
            v.sort_unstable();
            v
        }
    }
}

/// The first set bit of `set` in `[from, max]`.
fn next_bit(set: u64, from: u32, max: u32) -> Option<u32> {
    if from > max {
        return None;
    }
    let masked = set >> from;
    if masked == 0 {
        return None;
    }
    let b = from + masked.trailing_zeros();
    (b <= max).then_some(b)
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const DAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

/// A cron field ⇒ a bit set in `[lo, hi]`. `names[i]` stands for the value
/// `lo + i` (months start at 1, weekdays at 0).
fn field(s: &str, lo: u32, hi: u32, names: &[&str], what: &str) -> Result<u64, SpecError> {
    let value = |v: &str| -> Result<u32, SpecError> {
        let up = v.to_ascii_uppercase();
        if let Some(i) = names.iter().position(|n| *n == up) {
            return Ok(lo + i as u32);
        }
        match v.parse::<u32>() {
            Ok(n) if (lo..=hi).contains(&n) => Ok(n),
            _ => err(format!("{what}: '{v}' is outside [{lo}, {hi}]")),
        }
    };
    let mut set = 0u64;
    for part in s.split(',') {
        if part.is_empty() {
            return err(format!("{what}: empty part in '{s}'"));
        }
        let (range, step) = match part.split_once('/') {
            Some((r, st)) => match st.parse::<u32>() {
                Ok(n) if n > 0 => (r, n),
                _ => return err(format!("{what}: invalid step '{st}'")),
            },
            None => (part, 1),
        };
        let (a, b) = if range == "*" {
            (lo, hi)
        } else if let Some((x, y)) = range.split_once('-') {
            let (x, y) = (value(x)?, value(y)?);
            if x > y {
                return err(format!("{what}: range '{range}' is reversed"));
            }
            (x, y)
        } else {
            let x = value(range)?;
            // `a/n` = from `a` to the end of the field, step `n` (Vixie).
            (
                x,
                if step > 1 || part.contains('/') {
                    hi
                } else {
                    x
                },
            )
        };
        let mut v = a;
        while v <= b {
            set |= 1 << v;
            v += step;
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests;
