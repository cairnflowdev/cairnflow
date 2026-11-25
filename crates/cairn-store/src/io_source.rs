// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The **caller** label of object store requests.
//!
//! The path class ([`crate::testkit::PathClass`]) says which object a request touches, not
//! who sent it: `owners/range-<id>` is read by the routing loop, the keeper, the tailer, the
//! WAL floor GC, the tombstone-by-proxy loop… A background loop wraps its future in
//! [`tagged`], and `FaultyStore` (testkit) counts requests by the label in effect for the
//! calling task. With nested labels, the inner label wins.
//!
//! Only has an effect with the `testkit` feature; in the normal build [`tagged`] is the
//! original future and costs nothing. Child tasks (`tokio::spawn`) do **not** inherit the
//! label: their requests are `untagged`, unless the spawn site wraps them itself.
//!
//! SlateDB's internal tasks go through the fork's spawn hook (`slatedb::spawn_hook`) that
//! [`install_slatedb_hook`] installs — `FaultyStore::wrap` calls it, so every process that
//! counts has the hook. Named background tasks (writer, `wal_writer`, `l0_*`, `compactor`,
//! `compaction_worker`, `garbage_collector`, `manifest_poller`…) carry the label
//! `slatedb.<name>`; unnamed helper tasks (block read-ahead, SST uploads) inherit the label
//! of the task that spawned them. Both inherit the **range** ([`in_range`]) of the spawn
//! site: the background tasks of range `r`'s Db are built during the Db open, and the open
//! runs inside `in_range(r, …)`.

#[cfg(any(test, feature = "testkit"))]
tokio::task_local! {
    static SOURCE: &'static str;
    static RANGE: u32;
}

/// The label of a request that is not inside any [`tagged`] region.
pub const UNTAGGED: &str = "untagged";

/// Runs `fut` with source label `source`. A short, fixed name (a metric label).
pub async fn tagged<F: std::future::Future>(source: &'static str, fut: F) -> F::Output {
    #[cfg(any(test, feature = "testkit"))]
    {
        SOURCE.scope(source, fut).await
    }
    #[cfg(not(any(test, feature = "testkit")))]
    {
        let _ = source;
        fut.await
    }
}

/// The label in effect for the current task ([`UNTAGGED`] if none).
pub fn current() -> &'static str {
    #[cfg(any(test, feature = "testkit"))]
    {
        SOURCE.try_with(|s| *s).unwrap_or(UNTAGGED)
    }
    #[cfg(not(any(test, feature = "testkit")))]
    {
        UNTAGGED
    }
}

/// Runs `fut` with range label `range`: its requests, and those of the SlateDB tasks it
/// spawns, are also counted by range (`FaultyStore::counts_by_range`). Does not change the
/// source label.
pub async fn in_range<F: std::future::Future>(range: u32, fut: F) -> F::Output {
    #[cfg(any(test, feature = "testkit"))]
    {
        RANGE.scope(range, fut).await
    }
    #[cfg(not(any(test, feature = "testkit")))]
    {
        let _ = range;
        fut.await
    }
}

/// The range in effect for the current task (`None` if not inside any [`in_range`]).
pub fn current_range() -> Option<u32> {
    #[cfg(any(test, feature = "testkit"))]
    {
        RANGE.try_with(|r| *r).ok()
    }
    #[cfg(not(any(test, feature = "testkit")))]
    {
        None
    }
}

/// Installs SlateDB's spawn hook for the whole process (the first call wins, calling again
/// is harmless). The non-`testkit` build installs nothing: SlateDB spawns as upstream.
pub fn install_slatedb_hook() {
    #[cfg(any(test, feature = "testkit"))]
    hook::install();
}

#[cfg(any(test, feature = "testkit"))]
mod hook {
    use std::sync::{Arc, Mutex, Once};

    /// The context captured at spawn time, rebuilt around each poll of the task.
    struct Scope {
        source: &'static str,
        range: Option<u32>,
    }

    impl slatedb::spawn_hook::TaskScope for Scope {
        fn enter(&self, poll: &mut dyn FnMut()) {
            super::SOURCE.sync_scope(self.source, || match self.range {
                Some(r) => super::RANGE.sync_scope(r, &mut *poll),
                None => poll(),
            })
        }
    }

    /// `slatedb.<name>` as a `&'static str`. SlateDB's task names are a small, fixed set
    /// (job ids have been stripped in the fork), so each name is leaked once.
    fn label(name: &str) -> &'static str {
        static NAMES: Mutex<std::collections::BTreeMap<String, &'static str>> =
            Mutex::new(std::collections::BTreeMap::new());
        let mut names = NAMES.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(l) = names.get(name) {
            return l;
        }
        let l: &'static str = Box::leak(format!("slatedb.{name}").into_boxed_str());
        names.insert(name.to_string(), l);
        l
    }

    pub(super) fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            slatedb::spawn_hook::set_spawn_hook(Box::new(|name: Option<&str>| {
                let source = match name {
                    Some(n) => label(n),
                    None => super::current(),
                };
                let range = super::current_range();
                if source == super::UNTAGGED && range.is_none() {
                    return None;
                }
                Some(Arc::new(Scope { source, range }) as Arc<dyn slatedb::spawn_hook::TaskScope>)
            }));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inner_tag_wins_and_spawned_tasks_are_untagged() {
        assert_eq!(current(), UNTAGGED);
        tagged("outer", async {
            assert_eq!(current(), "outer");
            tagged("inner", async { assert_eq!(current(), "inner") }).await;
            assert_eq!(current(), "outer");
            let child = tokio::spawn(async { current() }).await.unwrap();
            assert_eq!(child, UNTAGGED);
        })
        .await;
    }

    #[tokio::test]
    async fn slatedb_tasks_carry_their_name_and_the_spawner_range() {
        use crate::testkit::{FaultyStore, Op};
        use slatedb::object_store::memory::InMemory;
        use std::sync::Arc;

        let store = FaultyStore::wrap(Arc::new(InMemory::new()));
        let db = in_range(
            7,
            tagged(
                "test.open",
                slatedb::Db::builder(
                    "db",
                    store.clone() as Arc<dyn slatedb::object_store::ObjectStore>,
                )
                .build(),
            ),
        )
        .await
        .unwrap();
        db.put(b"k", b"v").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let by_source = store.counts_by_source();
        // The open carries the caller's label; the Db's background tasks carry their names.
        assert!(by_source.iter().any(|(s, ..)| *s == "test.open"));
        assert!(
            by_source.iter().any(|(s, ..)| s.starts_with("slatedb.")),
            "{by_source:?}"
        );
        let by_range = store.counts_by_range();
        assert!(
            by_range
                .iter()
                .any(|(s, op, r, _)| s.starts_with("slatedb.") && *op == Op::Put && *r == 7),
            "background tasks inherit the range of the open: {by_range:?}"
        );
        assert!(by_range.iter().all(|(_, _, r, _)| *r == 7), "{by_range:?}");
    }
}
