// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! S3 emulation store profiles for a **real process**.
//!
//! **Only with the `testkit` feature.** A binary built with that feature reads the
//! environment variable [`ENV`] (`CAIRNFLOW_STORE_PROFILE`); when set, each store tier (main,
//! WAL) is wrapped in a [`FaultyStore`] carrying the profile's rules
//! ([`StoreProfile`]: latency by distribution, S3-style throttling), and its counters show up
//! on `/metrics` as `cairnflow_testkit_store_*`. An empty profile only counts — that is how
//! the cost model counts the requests of a real cluster.
//!
//! Release builds have neither this module, nor that environment variable, nor any of the
//! metric lines above (doctest in [`crate::node::NodeConfig`]).
//!
//! The registry of wrapped stores is **process-wide**: one binary is one node, so there is
//! nothing to mix up. Tests running many nodes in one process plug in `FaultyStore`
//! directly and read `counts()`, without going through here.

pub use cairn_store::testkit::profile::ENV;
use {
    cairn_store::{
        testkit::{
            profile::StoreProfile, FaultyStore, Op, PathClass, PutKind, SlowDown, StoreCounts,
        },
        StoreLocation,
    },
    std::{
        fmt::Write,
        sync::{Arc, Mutex},
    },
};

struct Wrapped {
    tier: &'static str,
    store: Arc<FaultyStore>,
    slowdown: Option<SlowDown>,
}

static WRAPPED: Mutex<Vec<Wrapped>> = Mutex::new(Vec::new());

/// Wrap `loc` per the profile in [`ENV`]. The variable unset (or empty) ⇒ `loc` returned
/// unchanged. A corrupt profile is an error — a measurement run accidentally made without a
/// profile is worse than no run.
pub fn wrap_from_env(tier: &'static str, loc: StoreLocation) -> Result<StoreLocation, String> {
    let Some(path) = std::env::var_os(ENV).filter(|p| !p.is_empty()) else {
        return Ok(loc);
    };
    let mut profile = StoreProfile::load(std::path::Path::new(&path))?;
    // One sequence per process: with the same seed every restart would repeat exactly the
    // same sequence of latencies and errors from the start — under chaos (kills every few
    // dozen seconds) a low error rate would never reach its winning draw. The profile's seed
    // is still mixed in; in-process tests use `wrap`, deterministic as before.
    let mut salt = [0u8; 8];
    cairn_store::entropy::fill(&mut salt);
    profile.seed ^= u64::from_le_bytes(salt);
    let out = wrap(tier, loc, &profile)?;
    tracing::warn!(
        tier = tier,
        profile = %profile.summary(),
        "store carries a simulation profile (testkit) — not a release build"
    );
    Ok(out)
}

/// Wrap `loc` per `profile` and record it in the `/metrics` registry.
pub fn wrap(
    tier: &'static str,
    loc: StoreLocation,
    profile: &StoreProfile,
) -> Result<StoreLocation, String> {
    let store = FaultyStore::wrap(Arc::clone(loc.store()));
    let installed = profile.install(&store)?;
    WRAPPED.lock().unwrap().push(Wrapped {
        tier,
        store: Arc::clone(&store),
        slowdown: installed.slowdown,
    });
    Ok(loc.wrap(store))
}

/// Counters of every wrapped store, added up.
pub fn counts() -> StoreCounts {
    WRAPPED
        .lock()
        .unwrap()
        .iter()
        .fold(StoreCounts::default(), |acc, w| acc.plus(&w.store.counts()))
}

fn op_name(op: Op) -> &'static str {
    match op {
        Op::Put => "put",
        Op::PutMultipart => "put_multipart",
        Op::Get => "get",
        Op::Head => "head",
        Op::List => "list",
        Op::Delete => "delete",
        Op::Copy => "copy",
        Op::Rename => "rename",
    }
}

fn class_name(c: PathClass) -> &'static str {
    match c {
        PathClass::Wal => "wal",
        PathClass::NodeWal => "nodewal",
        PathClass::Manifest => "manifest",
        PathClass::Compacted => "compacted",
        PathClass::Lease => "lease",
        PathClass::Other => "other",
    }
}

fn kind_name(k: PutKind) -> &'static str {
    match k {
        PutKind::Overwrite => "overwrite",
        PutKind::Create => "create",
        PutKind::Update => "update",
    }
}

/// `/metrics` lines of the wrapped stores. Only non-zero cells are printed — readers treat a missing cell as 0.
pub fn render(out: &mut String) {
    let wrapped = WRAPPED.lock().unwrap();
    if wrapped.is_empty() {
        return;
    }
    let family = |out: &mut String, name: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} counter");
    };
    let name = "cairnflow_testkit_store_requests_total";
    family(
        out,
        name,
        "Requests to the object store by operation and path kind (testkit, counted on arrival)",
    );
    for w in wrapped.iter() {
        let c = w.store.counts();
        for op in Op::ALL {
            for class in PathClass::ALL {
                let n = c.get(op, class);
                if n > 0 {
                    let _ = writeln!(
                        out,
                        "{name}{{tier=\"{}\",op=\"{}\",class=\"{}\"}} {n}",
                        w.tier,
                        op_name(op),
                        class_name(class)
                    );
                }
            }
        }
    }
    let name = "cairnflow_testkit_store_requests_by_source_total";
    family(
        out,
        name,
        "Requests to the object store by caller (io_source label of the sending task), operation, path kind and object kind",
    );
    for w in wrapped.iter() {
        for (source, op, class, object, n) in w.store.counts_by_source_object() {
            let _ = writeln!(
                out,
                "{name}{{tier=\"{}\",source=\"{source}\",op=\"{}\",class=\"{}\",object=\"{object}\"}} {n}",
                w.tier,
                op_name(op),
                class_name(class)
            );
        }
    }
    let name = "cairnflow_testkit_store_requests_by_range_total";
    family(
        out,
        name,
        "Requests to the object store labelled with a range (a range's Db and its SlateDB tasks) by caller, operation and range",
    );
    for w in wrapped.iter() {
        for (source, op, range, n) in w.store.counts_by_range() {
            let _ = writeln!(
                out,
                "{name}{{tier=\"{}\",source=\"{source}\",op=\"{}\",range=\"{range}\"}} {n}",
                w.tier,
                op_name(op),
            );
        }
    }
    let name = "cairnflow_testkit_store_puts_total";
    family(
        out,
        name,
        "PUTs by mode: overwrite, create (If-None-Match), update (If-Match)",
    );
    for w in wrapped.iter() {
        let c = w.store.counts();
        for kind in PutKind::ALL {
            for class in PathClass::ALL {
                let n = c.put_kind(kind, class);
                if n > 0 {
                    let _ = writeln!(
                        out,
                        "{name}{{tier=\"{}\",kind=\"{}\",class=\"{}\"}} {n}",
                        w.tier,
                        kind_name(kind),
                        class_name(class)
                    );
                }
            }
        }
    }
    for (name, help, get) in [
        (
            "cairnflow_testkit_store_list_pages_total",
            "LIST pages (1,000 keys per page) — the number of billed LIST requests",
            StoreCounts::list_pages as fn(&StoreCounts, PathClass) -> u64,
        ),
        (
            "cairnflow_testkit_store_put_bytes_total",
            "PUT body bytes",
            StoreCounts::put_bytes,
        ),
        (
            "cairnflow_testkit_store_get_bytes_total",
            "GET body bytes returned",
            StoreCounts::get_bytes,
        ),
    ] {
        family(out, name, help);
        for w in wrapped.iter() {
            let c = w.store.counts();
            for class in PathClass::ALL {
                let n = get(&c, class);
                if n > 0 {
                    let _ = writeln!(
                        out,
                        "{name}{{tier=\"{}\",class=\"{}\"}} {n}",
                        w.tier,
                        class_name(class)
                    );
                }
            }
        }
    }
    let name = "cairnflow_testkit_store_faults_total";
    family(
        out,
        name,
        "Injected profile faults (faults section): fail = error before reaching the store, ambiguous = written then reported as an error, retried = written then got the retry's answer, late = PUT arrived late (still applied after the caller stopped waiting)",
    );
    for w in wrapped.iter() {
        let (fail, ambiguous, retried) = w.store.injected();
        for (kind, n) in [
            ("fail", fail),
            ("ambiguous", ambiguous),
            ("retried", retried),
            ("late", w.store.landed_late()),
        ] {
            if n > 0 {
                let _ = writeln!(out, "{name}{{tier=\"{}\",kind=\"{kind}\"}} {n}", w.tier);
            }
        }
    }
    let name = "cairnflow_testkit_store_slowdown_total";
    family(
        out,
        name,
        "Throttling rule: rejected = 503s issued, retries = simulated client retries, surfaced = 503s reaching cairn, splits = prefix partitions split",
    );
    for w in wrapped.iter() {
        if let Some(s) = &w.slowdown {
            let st = s.stats();
            for (what, n) in [
                ("rejected", st.rejected),
                ("retries", st.retries),
                ("surfaced", st.surfaced),
                ("splits", st.splits),
            ] {
                let _ = writeln!(out, "{name}{{tier=\"{}\",what=\"{what}\"}} {n}", w.tier);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        slatedb::object_store::{memory::InMemory, path::Path, ObjectStoreExt},
    };

    #[tokio::test]
    async fn wrapped_store_counts_show_up_in_metrics() {
        let profile = StoreProfile::parse(
            r#"{"name": "t", "slowdown": {"write_rate": 1000, "read_rate": 1000}}"#,
        )
        .unwrap();
        let loc = StoreLocation::from_store(Arc::new(InMemory::new()), "pre", "memory://pre");
        let loc = wrap("main", loc, &profile).unwrap();
        assert_eq!(loc.prefix().as_ref(), "pre");
        assert_eq!(loc.uri(), "memory://pre");
        let p = Path::from("pre/leases/x");
        loc.store().put(&p, "abc".into()).await.unwrap();
        cairn_store::io_source::tagged("probe", loc.store().get(&p))
            .await
            .unwrap();
        let mut out = String::new();
        render(&mut out);
        for line in [
            "cairnflow_testkit_store_requests_total{tier=\"main\",op=\"put\",class=\"lease\"} 1\n",
            "cairnflow_testkit_store_requests_total{tier=\"main\",op=\"get\",class=\"lease\"} 1\n",
            "cairnflow_testkit_store_requests_by_source_total{tier=\"main\",source=\"untagged\",op=\"put\",class=\"lease\",object=\"leases\"} 1\n",
            "cairnflow_testkit_store_requests_by_source_total{tier=\"main\",source=\"probe\",op=\"get\",class=\"lease\",object=\"leases\"} 1\n",
            "cairnflow_testkit_store_puts_total{tier=\"main\",kind=\"overwrite\",class=\"lease\"} 1\n",
            "cairnflow_testkit_store_put_bytes_total{tier=\"main\",class=\"lease\"} 3\n",
            "cairnflow_testkit_store_slowdown_total{tier=\"main\",what=\"rejected\"} 0\n",
        ] {
            assert!(out.contains(line), "missing {line:?} in\n{out}");
        }
        assert!(counts().op(Op::Put) >= 1);
    }
}
