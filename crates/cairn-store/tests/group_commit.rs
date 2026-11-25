// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **Who is responsible for group commit — SlateDB or cairn?**
//!
//! The question deserves a measurement, because the two answers lead to two very different
//! fixes. Reading SlateDB's code shows `WalBufferManager` keeping **one** shared
//! `current_wal`, and `freeze_current_wal` snapshotting the whole batch and flushing it
//! as **one** object — so several in-flight writes *should* be grouped. But "should", read
//! from code, is not "does", measured, so this file measures.
//!
//! Two runs, the same `Db`, the same number of writes, differing only in how they are
//! issued:
//!
//! | | How issued |
//! |---|---|
//! | **sequential** | `write` → `await_durable` → `write` → … |
//! | **concurrent** | issue every `write`, then `await_durable` them all |
//!
//! The conclusion read from the ratio:
//!
//! * concurrent ≈ sequential ⇒ SlateDB does **not** group; grouping would mean changing
//!   SlateDB.
//! * concurrent ≪ sequential ⇒ SlateDB **already** groups, and the obstacle is in cairn:
//!   `EngineLock` held the mutex across `await_durable`, so there were never two writes in
//!   flight to group.
//!
//! **It measured 44.6×, so the second branch** — and that is the measurement that led to
//! the current shape of [`cairn_store::PartitionWriter`]: `commit` only pushes into the
//! WAL, and the wait (`durability()` → `Pending::durable()`) happens once the engine mutex
//! has been released. The "sequential" column here is therefore **a picture of the past**,
//! kept so that whoever next plans to merge the two steps sees the cost at once.
//!
//! Run by hand, printing the numbers:
//!
//! ```sh
//! cargo test --release -p cairn-store --test group_commit -- --nocapture
//! ```
//!
//! No `assert` on timing in CI — a timing measurement on a shared machine is a randomly
//! red test. It `assert`s exactly one thing that does **not** depend on machine speed:
//! every write is durable and reads back right.

// Tests run on real time, network and processes; only `sim_cluster` must be deterministic
// (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    slatedb::{object_store::memory::InMemory, Db, Settings, WriteBatch},
    std::{sync::Arc, time::Duration},
};

const N: usize = 50;
const FLUSH_MS: u64 = 10;

async fn open() -> Db {
    Db::builder("group-commit", Arc::new(InMemory::new()))
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(FLUSH_MS)),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db")
}

fn batch(i: usize, tag: &str) -> WriteBatch {
    let mut b = WriteBatch::new();
    b.put(format!("{tag}/{i:04}").as_bytes(), [7u8; 64]);
    b
}

#[tokio::test(flavor = "multi_thread")]
async fn wal_batches_many_in_flight_writes() {
    let db = open().await;

    // ── Sequential: the shape `PartitionWriter::commit` used to have.
    let t0 = std::time::Instant::now();
    for i in 0..N {
        let h = db.write(batch(i, "seq")).await.expect("write");
        h.await_durable().await.expect("durable");
    }
    let sequential = t0.elapsed();

    // ── Concurrent: issue everything, then wait. This is the shape a group commit
    //    produces — several writes sharing one `current_wal` between two ticks.
    let t0 = std::time::Instant::now();
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        handles.push(db.write(batch(i, "par")).await.expect("write"));
    }
    for h in &handles {
        h.await_durable().await.expect("durable");
    }
    let concurrent = t0.elapsed();

    println!(
        "\n  {N} write, flush_interval = {FLUSH_MS}ms\n\
         \x20   sequential: {:>8.1} ms  ({:.2} ms/write)\n\
         \x20   concurrent: {:>8.1} ms  ({:.2} ms/write)\n\
         \x20   ratio     : {:>8.1}×\n",
        sequential.as_secs_f64() * 1e3,
        sequential.as_secs_f64() * 1e3 / N as f64,
        concurrent.as_secs_f64() * 1e3,
        concurrent.as_secs_f64() * 1e3 / N as f64,
        sequential.as_secs_f64() / concurrent.as_secs_f64(),
    );

    // The only assertion, and it does not measure time: grouped or not, the data must
    // stay intact. A group commit that loses a write is a useless group commit, and that
    // is what is worth pinning in a test.
    for i in 0..N {
        for tag in ["seq", "par"] {
            let key = format!("{tag}/{i:04}");
            let got = db.get(key.as_bytes()).await.expect("get");
            assert_eq!(
                got.as_deref(),
                Some(&[7u8; 64][..]),
                "{key} could not be read back after becoming durable"
            );
        }
    }

    db.close().await.expect("close db");
}
