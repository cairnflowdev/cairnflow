// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Distribution-based delays for [`super::Action::Latency`].
//!
//! Two forms: **lognormal**, given by its median and p99 (two numbers people can read off
//! a benchmark page, rather than `mu`/`sigma`), and a **quantile table**
//! p50/p90/p99/p999 when real measurements exist and should not be forced into a
//! parametric form.
//!
//! **Deterministic:** each rule keeps a [`SimRng`] seeded from its own `seed`; samples are
//! drawn under the rule table's lock, in the order of matching requests. Same seed and
//! same request order ⇒ same sequence of delays — which is what deterministic simulation
//! needs. Two rules with the same seed would draw two identical sequences; profiles
//! ([`super::profile`]) mix the seed with the rule index to avoid that.

use std::time::Duration;

/// A small, deterministic PRNG with no external crate: SplitMix64.
///
/// Not cryptographic, no need — it only needs a sequence reproducible from the seed and a
/// distribution even enough for drawing delays and jitter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimRng(u64);

impl SimRng {
    pub fn new(seed: u64) -> SimRng {
        SimRng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A uniform real in `[0, 1)`, 53 bits.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Standard normal N(0, 1) by Box–Muller (dropping the second half — simpler than
    /// keeping state, and speed does not matter here).
    pub fn normal(&mut self) -> f64 {
        // `1 - unit()` ∈ (0, 1]: ln never meets 0.
        let u1 = 1.0 - self.unit();
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// z of the 0.99 quantile of the standard normal distribution.
const Z_P99: f64 = 2.326_347_874;

/// The delay distribution of one rule.
#[derive(Clone, Debug, PartialEq)]
pub enum LatencyDist {
    /// Lognormal with median `p50` and 0.99 quantile `p99`:
    /// `mu = ln p50`, `sigma = ln(p99 / p50) / 2.326`. `p99 ≤ p50` ⇒ the constant `p50`.
    LogNormal { p50: Duration, p99: Duration },
    /// A quantile table, interpolated linearly **in log space** between the marks
    /// (0.5 → p50, 0.9 → p90, 0.99 → p99, 0.999 → p999). Both ends are clamped: below the
    /// 0.1 mark it takes `p50² / p90` (the mirror of p90 around p50 in log space, like a
    /// lognormal), above 0.999 it takes `p999` — the table says nothing about the farther
    /// tail, so the model does not invent one.
    Quantiles {
        p50: Duration,
        p90: Duration,
        p99: Duration,
        p999: Duration,
    },
}

impl LatencyDist {
    /// A constant — handy for tests and for profiles wanting a fixed delay.
    pub fn fixed(d: Duration) -> LatencyDist {
        LatencyDist::LogNormal { p50: d, p99: d }
    }

    /// Draws one sample.
    pub fn sample(&self, rng: &mut SimRng) -> Duration {
        match self {
            LatencyDist::LogNormal { p50, p99 } => {
                let m = p50.as_secs_f64();
                if *p99 <= *p50 || m <= 0.0 {
                    return *p50;
                }
                let sigma = (p99.as_secs_f64() / m).ln() / Z_P99;
                secs(m * (sigma * rng.normal()).exp())
            }
            LatencyDist::Quantiles {
                p50,
                p90,
                p99,
                p999,
            } => {
                let (p50, p90, p99, p999) = (
                    p50.as_secs_f64(),
                    p90.as_secs_f64(),
                    p99.as_secs_f64(),
                    p999.as_secs_f64(),
                );
                let p10 = if p90 > 0.0 { p50 * p50 / p90 } else { p50 };
                let knots = [
                    (0.1, p10),
                    (0.5, p50),
                    (0.9, p90),
                    (0.99, p99),
                    (0.999, p999),
                ];
                secs(interpolate_log(&knots, rng.unit()))
            }
        }
    }

    /// Quantile `q` of the distribution itself (no sampling) — for tests and for printing
    /// the profile for people to read.
    pub fn quantile(&self, q: f64) -> Duration {
        match self {
            LatencyDist::LogNormal { p50, p99 } => {
                let m = p50.as_secs_f64();
                if *p99 <= *p50 || m <= 0.0 {
                    return *p50;
                }
                let sigma = (p99.as_secs_f64() / m).ln() / Z_P99;
                secs(m * (sigma * inverse_normal(q)).exp())
            }
            LatencyDist::Quantiles {
                p50,
                p90,
                p99,
                p999,
            } => {
                let (p50, p90) = (p50.as_secs_f64(), p90.as_secs_f64());
                let p10 = if p90 > 0.0 { p50 * p50 / p90 } else { p50 };
                let knots = [
                    (0.1, p10),
                    (0.5, p50),
                    (0.9, p90),
                    (0.99, p99.as_secs_f64()),
                    (0.999, p999.as_secs_f64()),
                ];
                secs(interpolate_log(&knots, q))
            }
        }
    }
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s.max(0.0))
}

/// Linear interpolation in log space between `(quantile, seconds)` marks, clamped at both ends.
fn interpolate_log(knots: &[(f64, f64)], u: f64) -> f64 {
    let (first, last) = (knots[0], knots[knots.len() - 1]);
    if u <= first.0 {
        return first.1;
    }
    if u >= last.0 {
        return last.1;
    }
    for w in knots.windows(2) {
        let ((q0, v0), (q1, v1)) = (w[0], w[1]);
        if u <= q1 {
            let t = (u - q0) / (q1 - q0);
            if v0 <= 0.0 || v1 <= 0.0 {
                return v0 + t * (v1 - v0);
            }
            return (v0.ln() + t * (v1.ln() - v0.ln())).exp();
        }
    }
    last.1
}

/// Inverse of the standard normal CDF — Acklam's rational approximation (relative error
/// ~1e-9), good enough for printing quantiles.
fn inverse_normal(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    if p < 0.024_25 {
        tail((-2.0 * p.ln()).sqrt())
    } else if p > 1.0 - 0.024_25 {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// The empirical quantile of `n` samples.
    fn empirical(dist: &LatencyDist, seed: u64, n: usize, q: f64) -> f64 {
        let mut rng = SimRng::new(seed);
        let mut v: Vec<f64> = (0..n)
            .map(|_| dist.sample(&mut rng).as_secs_f64())
            .collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[((n as f64) * q) as usize]
    }

    #[test]
    fn same_seed_same_sequence_other_seed_differs() {
        let d = LatencyDist::LogNormal {
            p50: ms(20),
            p99: ms(150),
        };
        let draw = |seed| {
            let mut rng = SimRng::new(seed);
            (0..64).map(|_| d.sample(&mut rng)).collect::<Vec<_>>()
        };
        assert_eq!(draw(7), draw(7));
        assert_ne!(draw(7), draw(8));
    }

    #[test]
    fn lognormal_hits_its_median_and_p99() {
        let d = LatencyDist::LogNormal {
            p50: ms(20),
            p99: ms(150),
        };
        let p50 = empirical(&d, 1, 200_000, 0.5);
        let p99 = empirical(&d, 1, 200_000, 0.99);
        assert!((p50 - 0.020).abs() < 0.001, "p50 {p50}");
        assert!((p99 - 0.150).abs() < 0.010, "p99 {p99}");
        assert!((d.quantile(0.99).as_secs_f64() - 0.150).abs() < 1e-6);
        assert!((d.quantile(0.5).as_secs_f64() - 0.020).abs() < 1e-9);
    }

    #[test]
    fn quantile_table_hits_every_knot_and_clamps_tails() {
        let d = LatencyDist::Quantiles {
            p50: ms(10),
            p90: ms(30),
            p99: ms(80),
            p999: ms(400),
        };
        for (q, want) in [(0.5, 0.010), (0.9, 0.030), (0.99, 0.080)] {
            let got = empirical(&d, 3, 400_000, q);
            assert!((got - want).abs() / want < 0.05, "q{q}: {got} vs {want}");
        }
        // The tail is clamped at p999, the head at p50²/p90.
        let mut rng = SimRng::new(9);
        let all: Vec<_> = (0..100_000).map(|_| d.sample(&mut rng)).collect();
        assert!(all.iter().all(|x| *x <= ms(400)));
        let floor = Duration::from_secs_f64(0.010 * 0.010 / 0.030);
        assert!(all.iter().all(|x| *x + Duration::from_nanos(1) >= floor));
    }

    #[test]
    fn fixed_is_constant() {
        let d = LatencyDist::fixed(ms(5));
        let mut rng = SimRng::new(0);
        assert!((0..100).all(|_| d.sample(&mut rng) == ms(5)));
    }
}
