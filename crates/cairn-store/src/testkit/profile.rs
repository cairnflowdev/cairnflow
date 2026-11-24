// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Store profiles: a JSON file describing delays by operation type and throttling rules,
//! built into a [`FaultyStore`] rule set.
//!
//! For **real processes** — a binary built with the `testkit` feature reads the file from
//! the environment variable [`ENV`] — and for tests wanting the same profile in-process.
//! Sample profiles: `bench/s3-model/profiles/`.
//!
//! ```json
//! {
//!   "name": "s3-standard",
//!   "note": "assumed figures — see the source",
//!   "seed": 1,
//!   "latency": {
//!     "put": { "p50_ms": 30, "p99_ms": 150 },
//!     "get": { "p50_ms": 15, "p90_ms": 30, "p99_ms": 80, "p999_ms": 250 }
//!   },
//!   "slowdown": { "write_rate": 3500, "read_rate": 5500, "split_after_s": 60 }
//! }
//! ```
//!
//! - Keys of `latency`: `put` (covering `put_multipart` unless declared separately),
//!   `put_multipart`, `get`, `head`, `list`, `delete`, `copy`, `rename`,
//!   or `all` (every operation not declared separately).
//! - An entry with only `p50_ms` + `p99_ms` is lognormal; one with all four marks is a
//!   quantile table ([`LatencyDist`]). Anything in between is an error.
//! - `slowdown` absent ⇒ no throttling. `client_retry`: `"object_store"` (the default,
//!   simulating the HTTP client's retry loop) or `"none"`.
//! - An empty profile `{}` only **counts** requests — that is how the cost model counts on
//!   a real process.
//! - `faults`: a list of fault rates, each entry
//!   `{"ops": [...], "classes": [...], "fail": p, "ambiguous": p, "retried": p}`
//!   — `fail` is an error before reaching the store; `ambiguous` **writes and then reports
//!   an error** (Temporal's `ExecuteAndTimeout`); `retried` writes and then returns the
//!   answer of object_store's own retry (`Precondition`/`AlreadyExists`). `ops` are like
//!   the keys of `latency` (plus `all`); `classes` are path classes (`wal`, `nodewal`,
//!   `manifest`, `compacted`, `lease`, `other`), absent = every class. `late` is a PUT
//!   **landing late** by `late_ms` milliseconds — applied even when the caller has stopped
//!   waiting ([`Action::LandLate`]); `late > 0` requires `late_ms`.
//!
//! Unknown fields are an error (`deny_unknown_fields`): silently ignoring a misspelt mark
//! would make a whole measurement run measure the wrong thing.
//!
//! `seed` gives a deterministic sequence **within one process**. The `testkit` binary
//! (`cairn_server::store_profile::wrap_from_env`) mixes it with the process's entropy:
//! with the same seed every restart would replay exactly the same fault sequence from the
//! start, and under chaos a low rate would never reach a hitting draw.

use {
    super::{
        Action, Ambiguity, ClientRetry, FaultyStore, LatencyDist, Op, OpSet, PathClass, PathMatch,
        Rule, RuleId, SlowDown, SlowDownConfig,
    },
    serde::Deserialize,
    std::{collections::BTreeMap, time::Duration},
};

/// The environment variable pointing to the profile file, read by binaries built with `testkit`.
pub const ENV: &str = "CAIRNFLOW_STORE_PROFILE";

/// A profile, as read.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreProfile {
    #[serde(default)]
    pub name: Option<String>,
    /// A note for readers — source of the figures, date, assumptions. Unused.
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub seed: u64,
    #[serde(default)]
    pub latency: BTreeMap<String, LatencySpec>,
    #[serde(default)]
    pub slowdown: Option<SlowDownSpec>,
    #[serde(default)]
    pub faults: Vec<FaultSpec>,
}

/// One fault-rate entry — see the module head.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultSpec {
    pub ops: Vec<String>,
    #[serde(default)]
    pub classes: Vec<String>,
    #[serde(default)]
    pub fail: f64,
    #[serde(default)]
    pub ambiguous: f64,
    #[serde(default)]
    pub retried: f64,
    #[serde(default)]
    pub late: f64,
    #[serde(default)]
    pub late_ms: Option<u64>,
    #[serde(default)]
    pub note: Option<String>,
}

const CLASSES: [(&str, PathClass); 6] = [
    ("wal", PathClass::Wal),
    ("nodewal", PathClass::NodeWal),
    ("manifest", PathClass::Manifest),
    ("compacted", PathClass::Compacted),
    ("lease", PathClass::Lease),
    ("other", PathClass::Other),
];

impl FaultSpec {
    /// The rules of entry number `i`, the seed mixed from the profile's `seed`.
    fn rules(&self, i: usize, seed: u64) -> Result<Vec<Rule>, String> {
        let mut ops = OpSet::NONE;
        for k in &self.ops {
            ops = ops
                | match k.as_str() {
                    "all" => OpSet::ALL,
                    k => match OPS.iter().find(|(n, _)| *n == k) {
                        Some((_, op)) => OpSet::of(*op),
                        None => return Err(format!("faults[{i}].ops: unknown operation {k:?}")),
                    },
                };
        }
        if ops == OpSet::NONE {
            return Err(format!("faults[{i}].ops is empty"));
        }
        let mut paths = Vec::new();
        for c in &self.classes {
            match CLASSES.iter().find(|(n, _)| n == c) {
                Some((_, class)) => paths.push(PathMatch::Class(*class)),
                None => return Err(format!("faults[{i}].classes: unknown class {c:?}")),
            }
        }
        if paths.is_empty() {
            paths.push(PathMatch::Any);
        }
        let mut out = Vec::new();
        let late_ms = match (self.late > 0.0, self.late_ms) {
            (true, None) => return Err(format!("faults[{i}].late > 0 without late_ms")),
            (_, ms) => ms.unwrap_or(0),
        };
        let kinds = [
            ("fail", self.fail, Action::Fail { times: None }),
            (
                "ambiguous",
                self.ambiguous,
                Action::Ambiguous {
                    reply: Ambiguity::Error,
                    skip: 0,
                    times: None,
                },
            ),
            (
                "retried",
                self.retried,
                Action::Ambiguous {
                    reply: Ambiguity::Retried,
                    skip: 0,
                    times: None,
                },
            ),
            (
                "late",
                self.late,
                Action::LandLate(Duration::from_millis(late_ms)),
            ),
        ];
        for (k, (name, p, inner)) in kinds.into_iter().enumerate() {
            if !(0.0..=1.0).contains(&p) {
                return Err(format!("faults[{i}].{name} = {p} outside [0, 1]"));
            }
            if p == 0.0 {
                continue;
            }
            for (j, path) in paths.iter().enumerate() {
                out.push(Rule::new(
                    ops,
                    path.clone(),
                    Action::Chance {
                        p,
                        seed: mix(seed, 1_000 + (i * 100 + k * 10 + j) as u64),
                        inner: Box::new(inner.clone()),
                    },
                ));
            }
        }
        Ok(out)
    }
}

/// One delay entry, in milliseconds.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySpec {
    pub p50_ms: f64,
    #[serde(default)]
    pub p90_ms: Option<f64>,
    pub p99_ms: f64,
    #[serde(default)]
    pub p999_ms: Option<f64>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Throttling rules — the default is S3's figures ([`SlowDownConfig::s3`]).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlowDownSpec {
    #[serde(default)]
    pub write_rate: Option<f64>,
    #[serde(default)]
    pub read_rate: Option<f64>,
    #[serde(default)]
    pub initial_depth: Option<usize>,
    #[serde(default)]
    pub max_depth: Option<usize>,
    #[serde(default)]
    pub split_after_s: Option<f64>,
    #[serde(default)]
    pub client_retry: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// The rules plugged into a store, to remove them or read the throttling rule's figures.
#[derive(Clone, Debug, Default)]
pub struct Installed {
    pub rules: Vec<RuleId>,
    pub slowdown: Option<SlowDown>,
}

fn ms(v: f64) -> Duration {
    Duration::from_secs_f64(v.max(0.0) / 1000.0)
}

const OPS: [(&str, Op); 8] = [
    ("put", Op::Put),
    ("put_multipart", Op::PutMultipart),
    ("get", Op::Get),
    ("head", Op::Head),
    ("list", Op::List),
    ("delete", Op::Delete),
    ("copy", Op::Copy),
    ("rename", Op::Rename),
];

impl LatencySpec {
    fn dist(&self, key: &str) -> Result<LatencyDist, String> {
        match (self.p90_ms, self.p999_ms) {
            (None, None) => Ok(LatencyDist::LogNormal {
                p50: ms(self.p50_ms),
                p99: ms(self.p99_ms),
            }),
            (Some(p90), Some(p999)) => Ok(LatencyDist::Quantiles {
                p50: ms(self.p50_ms),
                p90: ms(p90),
                p99: ms(self.p99_ms),
                p999: ms(p999),
            }),
            _ => Err(format!(
                "latency.{key}: a quantile table needs all of p50_ms, p90_ms, p99_ms, p999_ms \
                 (or only p50_ms + p99_ms for lognormal)"
            )),
        }
    }
}

impl SlowDownSpec {
    fn config(&self, seed: u64) -> Result<SlowDownConfig, String> {
        let base = SlowDownConfig::s3();
        let client_retry = match self.client_retry.as_deref() {
            None | Some("object_store") => Some(ClientRetry::default()),
            Some("none") => None,
            Some(other) => {
                return Err(format!(
                    "slowdown.client_retry '{other}' is unknown — only \"object_store\" or \"none\""
                ))
            }
        };
        Ok(SlowDownConfig {
            write_rate: self.write_rate.unwrap_or(base.write_rate),
            read_rate: self.read_rate.unwrap_or(base.read_rate),
            initial_depth: self.initial_depth.unwrap_or(base.initial_depth),
            max_depth: self.max_depth.unwrap_or(base.max_depth),
            split_after: self
                .split_after_s
                .map_or(base.split_after, Duration::from_secs_f64),
            client_retry,
            seed,
        })
    }
}

/// Mixes the profile's seed with the rule index — two rules never draw the same sequence.
fn mix(seed: u64, i: u64) -> u64 {
    super::SimRng::new(seed ^ i.wrapping_mul(0x9e37_79b9_7f4a_7c15)).next_u64()
}

impl StoreProfile {
    pub fn parse(json: &str) -> Result<StoreProfile, String> {
        let p: StoreProfile =
            serde_json::from_str(json).map_err(|e| format!("store profile: {e}"))?;
        // Check everything right at read time, not at plug-in time.
        p.rules()?;
        Ok(p)
    }

    pub fn load(path: &std::path::Path) -> Result<StoreProfile, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading store profile {}: {e}", path.display()))?;
        StoreProfile::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// The rule set in plug-in order: throttling first (a 503 returns fast, it does not
    /// carry the delay of a successful request), then the per-operation delays.
    pub fn rules(&self) -> Result<(Vec<Rule>, Option<SlowDown>), String> {
        let mut rules = Vec::new();
        let slowdown = match &self.slowdown {
            None => None,
            Some(spec) => {
                let s = SlowDown::new(spec.config(mix(self.seed, 0))?);
                rules.push(Rule::new(
                    OpSet::ALL,
                    PathMatch::Any,
                    Action::SlowDown(s.clone()),
                ));
                Some(s)
            }
        };
        let mut covered = OpSet::NONE;
        for (key, spec) in &self.latency {
            if key == "all" {
                continue;
            }
            let ops = match key.as_str() {
                // `put` covers multipart too, unless multipart is declared separately.
                "put" if !self.latency.contains_key("put_multipart") => OpSet::WRITES,
                k => match OPS.iter().find(|(n, _)| *n == k) {
                    Some((_, op)) => OpSet::of(*op),
                    None => {
                        return Err(format!(
                            "latency.{k}: unknown operation — only all, {}",
                            OPS.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
                        ))
                    }
                },
            };
            covered = covered | ops;
            let seed = mix(self.seed, rules.len() as u64 + 1);
            rules.push(Rule::new(
                ops,
                PathMatch::Any,
                Action::Latency {
                    dist: spec.dist(key)?,
                    seed,
                },
            ));
        }
        if let Some(spec) = self.latency.get("all") {
            let rest = Op::ALL
                .into_iter()
                .filter(|op| !covered.contains(*op))
                .fold(OpSet::NONE, |acc, op| acc | OpSet::of(op));
            if rest != OpSet::NONE {
                let seed = mix(self.seed, rules.len() as u64 + 1);
                rules.push(Rule::new(
                    rest,
                    PathMatch::Any,
                    Action::Latency {
                        dist: spec.dist("all")?,
                        seed,
                    },
                ));
            }
        }
        // Faults after the delay: a real 5xx also costs a round trip.
        for (i, f) in self.faults.iter().enumerate() {
            rules.extend(f.rules(i, self.seed)?);
        }
        Ok((rules, slowdown))
    }

    /// Plugs the rule set into `store`.
    pub fn install(&self, store: &FaultyStore) -> Result<Installed, String> {
        let (rules, slowdown) = self.rules()?;
        Ok(Installed {
            rules: rules.into_iter().map(|r| store.add_rule(r)).collect(),
            slowdown,
        })
    }

    /// A one-line summary for the start-up log.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for (k, s) in &self.latency {
            match s.dist(k) {
                Ok(d) => parts.push(format!(
                    "{k} p50 {:.1}ms p99 {:.1}ms",
                    d.quantile(0.5).as_secs_f64() * 1e3,
                    d.quantile(0.99).as_secs_f64() * 1e3
                )),
                Err(e) => parts.push(e),
            }
        }
        if let Some(s) = &self.slowdown {
            if let Ok(c) = s.config(0) {
                parts.push(format!(
                    "slowdown {}/{} req/s/prefix, split after {:?}, client_retry {}",
                    c.write_rate,
                    c.read_rate,
                    c.split_after,
                    if c.client_retry.is_some() {
                        "object_store"
                    } else {
                        "none"
                    }
                ));
            }
        }
        for f in &self.faults {
            parts.push(format!(
                "faults {} [{}]: fail {} ambiguous {} retried {}",
                f.ops.join(","),
                if f.classes.is_empty() {
                    "every class".to_string()
                } else {
                    f.classes.join(",")
                },
                f.fail,
                f.ambiguous,
                f.retried
            ));
        }
        if parts.is_empty() {
            parts.push("count only".into());
        }
        format!(
            "{} (seed {}): {}",
            self.name.as_deref().unwrap_or("unnamed"),
            self.seed,
            parts.join("; ")
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStoreExt},
        std::sync::Arc,
    };

    #[test]
    fn repo_profiles_parse() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/s3-model/profiles");
        let mut n = 0;
        for e in std::fs::read_dir(&dir).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "json") {
                let prof = StoreProfile::load(&p).unwrap();
                assert!(!prof.summary().is_empty());
                n += 1;
            }
        }
        assert!(n >= 2, "missing sample profiles in {}", dir.display());
    }

    #[test]
    fn rejects_half_quantile_table_unknown_op_and_unknown_field() {
        let half = r#"{"latency": {"get": {"p50_ms": 1, "p90_ms": 2, "p99_ms": 3}}}"#;
        assert!(StoreProfile::parse(half).unwrap_err().contains("p999_ms"));
        let op = r#"{"latency": {"patch": {"p50_ms": 1, "p99_ms": 3}}}"#;
        assert!(StoreProfile::parse(op).unwrap_err().contains("patch"));
        let typo = r#"{"latency": {"get": {"p50_ms": 1, "p99ms": 3}}}"#;
        assert!(StoreProfile::parse(typo).is_err());
        assert!(StoreProfile::parse("{}")
            .unwrap()
            .rules()
            .unwrap()
            .0
            .is_empty());
    }

    /// `faults`: rates by operation and path class, same seed ⇒ same fault sequence; an
    /// `ambiguous` fault has **already written** before reporting the error; undeclared
    /// classes are untouched.
    #[tokio::test]
    async fn faults_hit_at_rate_only_their_class_and_ambiguous_writes_land() {
        let json = r#"{
            "seed": 3,
            "faults": [
                {"ops": ["put"], "classes": ["other"], "ambiguous": 0.5},
                {"ops": ["get"], "fail": 1.0, "classes": ["lease"]}
            ]
        }"#;
        let run = || async {
            let store = FaultyStore::wrap(Arc::new(InMemory::new()));
            StoreProfile::parse(json).unwrap().install(&store).unwrap();
            let mut out = Vec::new();
            for i in 0..200 {
                let p = Path::from(format!("x/{i}"));
                let r = store.put(&p, "v".into()).await;
                // Ambiguous: an error while the object is there.
                assert!(
                    store.inner().head(&p).await.is_ok(),
                    "write {i} did not reach the store"
                );
                out.push(r.is_err());
            }
            // Class `lease`: GETs always fail; class `other` is untouched.
            store
                .put(&Path::from("owners/range-1"), "o".into())
                .await
                .unwrap();
            assert!(store.get(&Path::from("owners/range-1")).await.is_err());
            assert!(store.get(&Path::from("x/0")).await.is_ok());
            let (fail, ambiguous, retried) = store.injected();
            assert_eq!(ambiguous as usize, out.iter().filter(|e| **e).count());
            assert_eq!((fail, retried), (1, 0));
            out
        };
        let (a, b) = (run().await, run().await);
        assert_eq!(a, b, "same seed yet a different fault sequence");
        let hit = a.iter().filter(|e| **e).count();
        assert!(
            (60..=140).contains(&hit),
            "a 0.5 rate over 200 attempts yet {hit} faults"
        );
        assert!(StoreProfile::parse(r#"{"faults": [{"ops": ["put"], "fail": 2}]}"#).is_err());
        assert!(
            StoreProfile::parse(r#"{"faults": [{"ops": ["put"], "classes": ["x"]}]}"#).is_err()
        );
    }

    /// The `s3-standard-faults-wal` profile injects all three ambiguous cases into the
    /// `nodewal` class, and adds **only** that compared with `s3-standard-faults` — the old
    /// profile is kept as a baseline. Case (c) lands on the store after the caller has
    /// stopped waiting.
    #[tokio::test(start_paused = true)]
    async fn wal_faults_profile_adds_all_three_ambiguous_cases_to_nodewal_only() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/s3-model/profiles");
        let base = StoreProfile::load(&dir.join("s3-standard-faults.json")).unwrap();
        let wal = StoreProfile::load(&dir.join("s3-standard-faults-wal.json")).unwrap();
        assert!(
            base.faults
                .iter()
                .all(|f| !f.classes.iter().any(|c| c == "nodewal")),
            "the baseline profile must not touch nodewal"
        );
        let extra: Vec<_> = wal
            .faults
            .iter()
            .filter(|f| f.classes.iter().any(|c| c == "nodewal"))
            .collect();
        assert_eq!(extra.len(), 1);
        let f = extra[0];
        assert!(
            f.ambiguous > 0.0 && f.retried > 0.0 && f.late > 0.0,
            "{f:?}"
        );
        assert!(
            f.late_ms.is_some_and(|ms| ms > 2_000),
            "case (c) must land after the default 2 s put_timeout: {f:?}"
        );
        assert_eq!(wal.faults.len(), base.faults.len() + 1);
        assert_eq!(wal.latency.len(), base.latency.len());

        let late = r#"{"faults": [{"ops": ["put"], "classes": ["nodewal"], "late": 1.0, "late_ms": 3000}]}"#;
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        StoreProfile::parse(late).unwrap().install(&store).unwrap();
        let p = Path::from("nodewal/n/1/0/00000000000000000001");
        let gave_up =
            tokio::time::timeout(std::time::Duration::from_secs(2), store.put(&p, "v".into()))
                .await;
        assert!(gave_up.is_err());
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        assert!(
            store.inner().head(&p).await.is_ok(),
            "the late PUT did not reach the store"
        );
        assert_eq!(store.landed_late(), 1);
        assert!(
            StoreProfile::parse(r#"{"faults": [{"ops": ["put"], "late": 0.5}]}"#)
                .unwrap_err()
                .contains("late_ms")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn installed_profile_delays_per_op_and_is_deterministic() {
        let json = r#"{
            "seed": 5,
            "latency": {
                "put": {"p50_ms": 40, "p99_ms": 40},
                "all": {"p50_ms": 7, "p99_ms": 7}
            }
        }"#;
        let prof = StoreProfile::parse(json).unwrap();
        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        prof.install(&store).unwrap();
        let p = Path::from("a");
        let t = tokio::time::Instant::now();
        store.put(&p, "x".into()).await.unwrap();
        assert_eq!(t.elapsed(), Duration::from_millis(40));
        let t = tokio::time::Instant::now();
        store.head(&p).await.unwrap();
        assert_eq!(t.elapsed(), Duration::from_millis(7));

        // A real lognormal: same seed ⇒ same delay sequence on two stores.
        let json = r#"{"seed": 9, "latency": {"get": {"p50_ms": 20, "p99_ms": 200}}}"#;
        let run = || async {
            let store = FaultyStore::wrap(Arc::new(InMemory::new()));
            store.put(&p, "x".into()).await.unwrap();
            StoreProfile::parse(json).unwrap().install(&store).unwrap();
            let mut out = Vec::new();
            for _ in 0..16 {
                let t = tokio::time::Instant::now();
                store.get(&p).await.unwrap();
                out.push(t.elapsed());
            }
            out
        };
        let (a, b) = (run().await, run().await);
        assert_eq!(a, b);
        assert!(a.iter().any(|d| *d != a[0]), "lognormal but fixed: {a:?}");
    }
}
