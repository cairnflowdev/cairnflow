// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `/invocations`: the status filter must run **before** `limit`.
//!
//! Filtering after cutting at `limit` turns `?status=failed&limit=5` into
//! "among the *first* 5 invocations, which failed". On a system that has run a
//! few hundred invocations, `cairn invocations --status failed` returns empty
//! while work is failing — and an empty list looks exactly like an answer.
//!
//! The test builds exactly that shape: many `completed` before one `failed` in
//! scan order. Scan order is key order, i.e. id order, so the hand-picked ids
//! here make it deterministic instead of a matter of luck.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterministic sources"
)]

use {
    anyhow::Result,
    bytes::Bytes,
    cairn_engine::{CreateOptions, Engine, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Event, StartParams},
    cairn_store::{
        make_invocation_id, LeadershipGrant, PartitionId, PartitionKey, PartitionWriter,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: String::new(),
    }
}

/// Ids ordered by `n`: 8 bytes of partition key (0) then 16 bytes of `n`. So
/// `inv_id(9)` is sure to come after `inv_id(1..=8)` in scan order.
fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

async fn open_engine(name: &str) -> Result<Engine> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let settings = Settings {
        flush_interval: Some(Duration::from_millis(10)),
        ..Default::default()
    };
    let db = Db::builder("partition-0", store)
        .with_settings(settings)
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    Ok(Engine::open(writer).await?)
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

async fn create(engine: &mut Engine, n: u8) -> Result<cairn_engine::Invocation> {
    Ok(engine
        .create(
            inv_id(n),
            &target(),
            CreateOptions::default(),
            StartParams::default(),
            Bytes::new(),
        )
        .await?)
}

/// Runs to `Output` + `End` — the real path, no back door that sets the status.
async fn completed(engine: &mut Engine, n: u8) -> Result<()> {
    let mut inv = create(engine, n).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(
                MessageType::OUTPUT,
                &pb::OutputCommandMessage {
                    result: Some(pb::output_command_message::Result::Value(
                        cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
                    )),
                    name: String::new(),
                },
            )),
        )
        .await?;
    engine
        .step(
            &mut inv,
            Event::FromSdk(frame(MessageType::END, &pb::EndMessage {})),
        )
        .await?;
    Ok(())
}

/// Cancel — also the real path, and it yields `Failed`.
async fn failed(engine: &mut Engine, n: u8) -> Result<()> {
    create(engine, n).await?;
    engine.cancel(&inv_id(n), "test").await?;
    Ok(())
}

#[tokio::test]
async fn status_filter_runs_before_limit() -> Result<()> {
    let mut engine = open_engine("introspection-filter").await?;

    // Eight finished ones come first, then the failed one.
    for n in 1..=8 {
        completed(&mut engine, n).await?;
    }
    failed(&mut engine, 9).await?;

    // `limit` far smaller than the number of rows before it: this is the case that returned empty.
    let rows = engine.list_invocations(1, Some("failed")).await?;
    assert_eq!(rows.len(), 1, "must see exactly one, the failed one");
    assert_eq!(
        rows[0].1.name(),
        "failed",
        "limit cut before filtering ⇒ eight finished ones hide the failed one"
    );
    assert_eq!(rows[0].0, inv_id(9));

    // And `limit` is still the cap **on the result**, not on the scan.
    for n in 10..=12 {
        failed(&mut engine, n).await?;
    }
    assert_eq!(engine.list_invocations(2, Some("failed")).await?.len(), 2);
    assert_eq!(engine.list_invocations(99, Some("failed")).await?.len(), 4);

    // No filter shows everything, and `limit` still caps.
    assert_eq!(engine.list_invocations(99, None).await?.len(), 12);
    assert_eq!(engine.list_invocations(3, None).await?.len(), 3);

    // A status nobody has ⇒ empty, not an error. (A typo is blocked at the
    // HTTP layer with `400`; the engine only compares names.)
    assert!(engine
        .list_invocations(99, Some("paused"))
        .await?
        .is_empty());
    Ok(())
}

/// **Time filters have a scan cap** (CG-11): a scan by time stops at `scan_max`
/// lines whose status was read and reports `capped`; under the cap it does not.
/// Half-open bounds `[after, before)`, sorted by `createdAt` descending, lines
/// missing a timestamp do not occur here (the Db always stamps) so `untimed` =
/// 0.
#[tokio::test]
async fn time_filter_has_scan_cap_and_sorts_descending() -> Result<()> {
    let mut engine = open_engine("introspection-time").await?;
    for n in 1..=6 {
        completed(&mut engine, n).await?;
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let all = engine
        .list_invocations_where(99, &cairn_engine::ListFilter::default())
        .await?;
    assert_eq!(all.rows.len(), 6);
    assert!(!all.capped, "no capped filter ⇒ not capped");
    let created: Vec<u64> = all
        .rows
        .iter()
        .map(|r| r.3.created_ms.expect("Db stamps"))
        .collect();
    assert!(created.windows(2).all(|w| w[0] < w[1]), "{created:?}");

    let f = cairn_engine::ListFilter {
        created_after: Some(created[2]),
        created_desc: true,
        ..Default::default()
    };
    let got = engine.list_invocations_where(99, &f).await?;
    let ids: Vec<_> = got.rows.iter().map(|r| r.0).collect();
    assert_eq!(ids, vec![inv_id(6), inv_id(5), inv_id(4), inv_id(3)]);
    assert!(!got.capped && got.untimed == 0);
    // `limit` cuts **after** sorting: the two newest.
    let got = engine.list_invocations_where(2, &f).await?;
    assert_eq!(
        got.rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![inv_id(6), inv_id(5)]
    );
    // Scan cap of 3 lines: stops, reports capped, sees only what was scanned.
    let got = engine
        .list_invocations_where(
            99,
            &cairn_engine::ListFilter {
                scan_max: Some(3),
                ..f.clone()
            },
        )
        .await?;
    assert!(got.capped, "hitting the scan cap must report capped");
    assert_eq!(
        got.rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![inv_id(3)]
    );
    // `ended_*` by the status row: half-open.
    let ended: Vec<u64> = all.rows.iter().map(|r| r.3.modified_ms.unwrap()).collect();
    let got = engine
        .list_invocations_where(
            99,
            &cairn_engine::ListFilter {
                ended_before: Some(ended[1]),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        got.rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![inv_id(1)]
    );
    Ok(())
}
