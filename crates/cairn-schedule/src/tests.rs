// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The row-by-row tables of the schedule arithmetic (CG-16).

use super::*;

/// `2026-03-08T07:00:00Z` ⇒ ms.
fn ms(s: &str) -> i64 {
    s.parse::<Timestamp>().unwrap().as_millisecond()
}

fn iso(t: i64) -> String {
    Timestamp::from_millisecond(t).unwrap().to_string()
}

fn next_iso(spec: &Spec, after: &str) -> Option<String> {
    spec.next(ms(after)).map(iso)
}

#[test]
fn parse_and_errors() {
    for bad in [
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * 32 * *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "5-1 * * * *",
        "1,,2 * * * *",
        "L * * * *",
        "* * * * MON#2",
        "x * * * *",
    ] {
        assert!(Spec::cron(bad, "UTC").is_err(), "{bad} must fail");
    }
    assert!(Spec::cron("* * * * *", "Mars/Olympus").is_err());
    assert!(Spec::cron("0 9 * JAN-MAR mon-fri", "Europe/Berlin").is_ok());
    assert!(Spec::every(9_999, 10_000).is_err());
    assert!(Spec::every(0, 0).is_err());
    assert!(Spec::every(10_000, 10_000).is_ok());
}

#[test]
fn utc_hand_table() {
    let rows: &[(&str, &str, Option<&str>)] = &[
        (
            "* * * * *",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T00:01:00Z"),
        ),
        (
            "* * * * *",
            "2026-01-01T00:00:30.5Z",
            Some("2026-01-01T00:01:00Z"),
        ),
        (
            "0 * * * *",
            "2026-01-01T00:00:00Z",
            Some("2026-01-01T01:00:00Z"),
        ),
        (
            "*/15 * * * *",
            "2026-01-01T00:16:00Z",
            Some("2026-01-01T00:30:00Z"),
        ),
        (
            "5/20 * * * *",
            "2026-01-01T00:26:00Z",
            Some("2026-01-01T00:45:00Z"),
        ),
        (
            "0 9 * * MON-FRI",
            "2026-01-02T09:00:00Z",
            Some("2026-01-05T09:00:00Z"),
        ),
        (
            "0 0 1 * *",
            "2026-01-31T12:00:00Z",
            Some("2026-02-01T00:00:00Z"),
        ),
        (
            "0 0 31 * *",
            "2026-02-01T00:00:00Z",
            Some("2026-03-31T00:00:00Z"),
        ),
        (
            "0 0 29 2 *",
            "2026-01-01T00:00:00Z",
            Some("2028-02-29T00:00:00Z"),
        ),
        // Vixie OR: day 13 **or** Friday.
        (
            "0 0 13 * FRI",
            "2026-02-01T00:00:00Z",
            Some("2026-02-06T00:00:00Z"),
        ),
        (
            "0 0 13 * FRI",
            "2026-02-06T00:00:00Z",
            Some("2026-02-13T00:00:00Z"),
        ),
        // `*/2` in the day field starts with `*` ⇒ unrestricted ⇒ only the weekday decides.
        (
            "0 0 */2 * MON",
            "2026-02-01T00:00:00Z",
            Some("2026-02-02T00:00:00Z"),
        ),
        (
            "0 0 * * 7",
            "2026-02-01T00:00:00Z",
            Some("2026-02-08T00:00:00Z"),
        ),
        (
            "0 0 * * 0",
            "2026-01-31T00:00:00Z",
            Some("2026-02-01T00:00:00Z"),
        ),
        (
            "30 23 31 12 *",
            "2026-12-31T23:30:00Z",
            Some("2027-12-31T23:30:00Z"),
        ),
        // Never matches.
        ("0 0 30 2 *", "2026-01-01T00:00:00Z", None),
        ("0 0 31 4 *", "2026-01-01T00:00:00Z", None),
    ];
    for (expr, after, want) in rows {
        let s = Spec::cron(expr, "UTC").unwrap();
        assert_eq!(next_iso(&s, after).as_deref(), *want, "{expr} > {after}");
    }
    let never = Spec::cron("0 0 30 2 *", "UTC").unwrap();
    assert!(never.validate(ms("2026-01-01T00:00:00Z")).is_err());
    assert!(Spec::cron("0 0 29 2 *", "UTC")
        .unwrap()
        .validate(ms("2026-01-01T00:00:00Z"))
        .is_ok());
}

#[test]
fn every_anchored_at_epoch() {
    let s = Spec::every(60_000, 1).unwrap();
    assert_eq!(s.next(0), Some(60_000));
    assert_eq!(s.next(59_999), Some(60_000));
    assert_eq!(s.next(60_000), Some(120_000));
    assert_eq!(s.next(-1), Some(0));
    assert_eq!(s.upcoming(0, 3), vec![60_000, 120_000, 180_000]);
    assert_eq!(s.count_between(0, 600_000, 100), 10);
    assert_eq!(s.count_between(0, 600_000, 4), 4);
    assert_eq!(s.count_between(10, 10, 4), 0);
    assert_eq!(s.last_between(0, 659_999), Some(600_000));
    assert_eq!(s.last_between(600_000, 659_999), None);
}

#[test]
fn cron_batch_instants() {
    let s = Spec::cron("*/15 * * * *", "UTC").unwrap();
    let a = ms("2026-01-01T00:00:00Z");
    let b = ms("2026-01-03T00:07:00Z");
    // `(a, b]`: day 1 except the 00:00 instant (95), day 2 (96), 00:00 of day 3.
    assert_eq!(s.count_between(a, b, 10_000), 95 + 96 + 1);
    assert_eq!(
        s.last_between(a, b).map(iso).as_deref(),
        Some("2026-01-03T00:00:00Z")
    );
    assert_eq!(s.last_between(b, b + 60_000), None);
    // A schedule paused for a long time: the last instant is found without walking from the start.
    let far = Spec::cron("0 0 1 1 *", "UTC").unwrap();
    assert_eq!(
        far.last_between(ms("2020-06-01T00:00:00Z"), ms("2026-06-01T00:00:00Z"))
            .map(iso)
            .as_deref(),
        Some("2026-01-01T00:00:00Z")
    );
}

#[test]
fn jitter_clamped_below_next_instant() {
    assert_eq!(jittered(1_000, Some(2_000), 0, 77), 1_000);
    assert_eq!(jittered(1_000, Some(2_000), 500, 777), 1_277);
    assert_eq!(jittered(1_000, Some(1_100), 500, 777), 1_099);
    assert_eq!(jittered(1_000, None, 500, 777), 1_277);
}

/// **A huge jitter does not move the fire before the instant.**
/// `(hash % jitter_ms) as i64` is negative when `jitter_ms > i64::MAX` and the
/// remainder is ≥ 2^63 ⇒ the fire is due before the instant, clamped to 0 in
/// the engine ⇒ the schedule fires on every background tick.
#[test]
fn jitter_never_lands_before_the_mark() {
    for hash in [0, 1 << 63, u64::MAX, (1 << 63) + 12_345, u64::MAX - 1] {
        let t = jittered(1_000, Some(2_000), u64::MAX, hash);
        assert!((1_000..2_000).contains(&t), "hash {hash}: {t}");
        let t = jittered(1_000, None, u64::MAX, hash);
        assert!(t >= 1_000, "hash {hash}, no next instant: {t}");
    }
}

/// An exhaustive oracle: every UTC minute in `[from, to]` at which the zone's
/// wall clock matches the spec (the spec's direct definition — not through
/// `next`'s field-jumping algorithm).
fn brute(c: &Cron, from: i64, to: i64) -> Vec<i64> {
    let mut out = Vec::new();
    let mut t = from - from.rem_euclid(MINUTE_MS);
    while t <= to {
        let civil = c
            .time_zone()
            .to_datetime(Timestamp::from_millisecond(t).unwrap());
        if c.matches(civil) {
            out.push(t);
        }
        t += MINUTE_MS;
    }
    out
}

/// Three zones around the 2026 DST transitions: for every minute of a ±1 day
/// window around a transition, `next(minute)` and `next(minute + 0.5 s)` must
/// be the next exhaustive instant. Plus a few hand rows pinning the rule
/// "the spring gap does not fire, the autumn fold fires twice".
#[test]
fn dst_tables() {
    let zones = [
        // (zone, spring transition, autumn transition) — the UTC time of the transition day.
        (
            "America/New_York",
            "2026-03-08T07:00:00Z",
            "2026-11-01T06:00:00Z",
        ),
        (
            "Europe/Berlin",
            "2026-03-29T01:00:00Z",
            "2026-10-25T01:00:00Z",
        ),
        // 30-minute DST; southern hemisphere: autumn (back) in April, spring (forward) in October.
        (
            "Australia/Lord_Howe",
            "2026-10-03T15:30:00Z",
            "2026-04-04T15:00:00Z",
        ),
    ];
    let specs = [
        "30 2 * * *",
        "0 2 * * *",
        "30 1 * * *",
        "45 1 * * *",
        "*/15 * * * *",
        "0 * * * *",
        "15,45 0-4 * * *",
        "0 3 * * SUN",
        "59 1 * * *",
    ];
    for (zone, spring, fall) in zones {
        for expr in specs {
            let s = Spec::cron(expr, zone).unwrap();
            let Spec::Cron(c) = &s else { unreachable!() };
            for edge in [spring, fall] {
                let from = ms(edge) - 26 * 3_600_000;
                let to = ms(edge) + 26 * 3_600_000;
                // Up to 8 days after the window: weekly specs need to see the next instant.
                let truth = brute(c, from, to + 8 * 86_400_000);
                let mut p = from;
                while p <= to {
                    for probe in [p, p + 500] {
                        let want = truth.iter().copied().find(|&m| m > probe);
                        assert_eq!(s.next(probe), want, "{expr} @ {zone}, after {}", iso(probe));
                    }
                    p += MINUTE_MS;
                }
            }
        }
    }

    let ny = |e: &str| Spec::cron(e, "America/New_York").unwrap();
    // Spring gap 2026-03-08: 02:00–02:59 does not exist ⇒ 02:30 does not fire that day.
    assert_eq!(
        next_iso(&ny("30 2 * * *"), "2026-03-08T05:00:00Z").as_deref(),
        Some("2026-03-09T06:30:00Z")
    );
    // Autumn fold 2026-11-01: 01:30 happens twice (EDT then EST) ⇒ fires twice.
    let s = ny("30 1 * * *");
    assert_eq!(
        next_iso(&s, "2026-11-01T04:00:00Z").as_deref(),
        Some("2026-11-01T05:30:00Z")
    );
    assert_eq!(
        next_iso(&s, "2026-11-01T05:30:00Z").as_deref(),
        Some("2026-11-01T06:30:00Z")
    );
    assert_eq!(
        next_iso(&s, "2026-11-01T06:30:00Z").as_deref(),
        Some("2026-11-02T06:30:00Z")
    );
    // `*/15` across the autumn fold: 8 instants in wall time 01:00–01:59 (two passes).
    let q = ny("*/15 * * * *");
    let n = q.count_between(ms("2026-11-01T04:59:00Z"), ms("2026-11-01T06:59:00Z"), 100);
    assert_eq!(n, 8);
    // Lord Howe: autumn 2026-04-05 02:00 goes back to 01:30 ⇒ 01:45 twice, 30 minutes apart.
    let lh = Spec::cron("45 1 * * *", "Australia/Lord_Howe").unwrap();
    let a = lh.next(ms("2026-04-04T14:00:00Z")).unwrap();
    let b = lh.next(a).unwrap();
    assert_eq!(
        (iso(a).as_str(), b - a),
        ("2026-04-04T14:45:00Z", 30 * 60_000)
    );
    // Lord Howe spring 2026-10-04 02:00 jumps to 02:30 ⇒ 02:15 does not exist.
    let gap = Spec::cron("15 2 4 10 *", "Australia/Lord_Howe").unwrap();
    assert!(gap.next(ms("2026-10-03T00:00:00Z")).unwrap() > ms("2027-01-01T00:00:00Z"));
}

/// A deterministic xorshift — no extra dependency for the oracle's random part.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One random field. `star_step` = `*/n` may be used (not for the day/weekday
/// fields: croner treats only a bare `*` as unrestricted, Vixie treats every
/// field starting with `*` that way — the two implementations differ only
/// there).
fn gen_field(r: &mut Rng, lo: u64, hi: u64, star_step: bool) -> String {
    match r.below(6) {
        0 => "*".into(),
        1 if star_step => format!("*/{}", 1 + r.below(hi - lo + 1).min(14)),
        2 => {
            let a = lo + r.below(hi - lo + 1);
            let b = a + r.below(hi - a + 1);
            format!("{a}-{b}")
        }
        3 => {
            let n = 1 + r.below(3);
            let mut v: Vec<u64> = (0..n).map(|_| lo + r.below(hi - lo + 1)).collect();
            v.sort_unstable();
            v.dedup();
            v.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        }
        4 => {
            let a = lo + r.below(hi - lo + 1);
            format!("{a}-{}/{}", hi, 1 + r.below(5))
        }
        _ => (lo + r.below(hi - lo + 1)).to_string(),
    }
}

/// The `croner` oracle for UTC: 10 000 `(spec, after)` pairs.
#[test]
fn croner_oracle_utc() {
    use chrono::{TimeZone as _, Utc};
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    let base = ms("2020-01-01T00:00:00Z");
    let span = ms("2030-01-01T00:00:00Z") - base;
    let mut compared = 0;
    for _ in 0..10_000 {
        let expr = format!(
            "{} {} {} {} {}",
            gen_field(&mut r, 0, 59, true),
            gen_field(&mut r, 0, 23, true),
            gen_field(&mut r, 1, 28, false),
            gen_field(&mut r, 1, 12, true),
            gen_field(&mut r, 0, 6, false),
        );
        let after = base + (r.below(span as u64) as i64);
        let ours = Spec::cron(&expr, "UTC").unwrap().next(after);
        let theirs = croner::Cron::new(&expr)
            .parse()
            .unwrap_or_else(|e| panic!("croner cannot parse {expr}: {e:?}"))
            .find_next_occurrence(&Utc.timestamp_millis_opt(after).unwrap(), false)
            .ok()
            .map(|d| d.timestamp_millis());
        // croner searches without a horizon; an instant beyond our horizon is skipped.
        if let (None, Some(t)) = (ours, theirs) {
            assert!(
                t > after + (HORIZON_YEARS as i64 - 1) * 365 * 86_400_000,
                "{expr} after {}",
                iso(after)
            );
            continue;
        }
        assert_eq!(ours, theirs, "{expr} after {}", iso(after));
        compared += 1;
    }
    assert!(compared > 9_000, "only {compared} compared");
}

/// A cap of ≤ 1 ms per `next` in the worst case: a spec that never matches
/// (scans the whole horizon), `29 2` from a non-leap year, a wall time in the
/// autumn fold.
#[test]
#[allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "measures the real CPU time of `next` — not a product source of nondeterminism"
)]
fn time_ceiling() {
    let cases = [
        ("0 0 30 2 *", "UTC", "2026-01-01T00:00:00Z"),
        ("0 0 29 2 *", "UTC", "2097-03-01T00:00:00Z"),
        (
            "59 23 31 12 SAT",
            "America/New_York",
            "2026-01-01T00:00:00Z",
        ),
        ("30 1 * * *", "America/New_York", "2026-11-01T05:31:00Z"),
        (
            "0 0 31 4,6,9,11 *",
            "Australia/Lord_Howe",
            "2026-01-01T00:00:00Z",
        ),
    ];
    for (expr, tz, after) in cases {
        let s = Spec::cron(expr, tz).unwrap();
        let a = ms(after);
        let n = 200;
        let t0 = std::time::Instant::now();
        for i in 0..n {
            std::hint::black_box(s.next(a + i));
        }
        let each = t0.elapsed() / n as u32;
        // A debug build is ~10–30 times slower than release; the tight cap is release-only.
        let cap = if cfg!(debug_assertions) { 20 } else { 1 };
        assert!(each.as_millis() < cap, "{expr} @ {tz}: {each:?} per next");
    }
}

/// CG-16: every leader uses the same embedded tzdb — the `tzdb-bundle-always`
/// flag and the `jiff` version pinned exactly in the crate's `Cargo.toml`.
#[test]
fn embedded_tzdb_and_pinned_jiff() {
    let toml = include_str!("../Cargo.toml");
    let line = toml
        .lines()
        .find(|l| l.starts_with("jiff ="))
        .expect("jiff line");
    assert!(line.contains("tzdb-bundle-always"), "{line}");
    assert!(
        line.contains("version = \"="),
        "jiff must be pinned exactly: {line}"
    );
    assert!(line.contains("default-features = false"), "{line}");
}
