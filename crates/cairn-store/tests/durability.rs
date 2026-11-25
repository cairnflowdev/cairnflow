// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A durability ticket is a **mark**, not an item.
//!
//! The earlier version kept the latest WAL handle in a slot and took it away when waiting:
//! the second asker found the slot empty and got a ticket at once, while the first asker's
//! write was still in flight. The tests here pin the new semantics at the store layer.

// Tests run on real time, network and processes; only `sim_cluster` must be deterministic
// (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    cairn_store::{
        Durability, LeadershipGrant, Mutations, PartitionId, PartitionWriter, StoreError,
    },
    slatedb::{config::Settings, object_store::memory::InMemory, Db},
    std::{sync::Arc, time::Duration},
};

const INV: [u8; 24] = [0xAA; 24];

async fn writer(flush: Duration) -> PartitionWriter {
    let db = Db::builder("partition-0", Arc::new(InMemory::new()))
        .with_settings(Settings {
            flush_interval: Some(flush),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db)
        .await
        .expect("open writer")
}

/// Two askers, one write in flight: **both** must wait, nobody gets a ticket on the spot.
/// This is exactly the case the old version got wrong.
#[tokio::test]
async fn ticket_waits_for_others_inflight_writes() {
    let mut w = writer(Duration::from_millis(1500)).await;
    let mut m = Mutations::new();
    m.set_invocation_status(&INV, b"COMPLETED");
    w.commit(m).await.unwrap();

    let first = match w.durability().unwrap() {
        Durability::Wait(p) => p,
        Durability::Ready(_) => panic!("a ticket right after writing"),
    };
    // The second asker asks while the first is holding the mark.
    assert!(
        matches!(w.durability().unwrap(), Durability::Wait(_)),
        "the second asker got a ticket on the spot while the write is not yet durable"
    );

    first.durable().await.expect("durable after one flush tick");
    assert!(
        matches!(w.durability().unwrap(), Durability::Ready(_)),
        "the mark has passed yet it still makes us wait"
    );
}

/// Lease lost ⇒ no ticket at all, even with nothing in flight: a deposed leader must no
/// longer answer read-only queries.
#[tokio::test]
async fn no_ticket_after_lease_lost() {
    let grant = LeadershipGrant::single_node(PartitionId(0));
    let validity = grant.validity_handle();
    let db = Db::builder("partition-0", Arc::new(InMemory::new()))
        .build()
        .await
        .unwrap();
    let w = PartitionWriter::open(grant, db).await.unwrap();
    assert!(matches!(w.durability().unwrap(), Durability::Ready(_)));

    validity.revoke();
    assert!(
        matches!(w.durability(), Err(StoreError::LeaseLost)),
        "the lease is lost yet a ticket was issued"
    );
}
