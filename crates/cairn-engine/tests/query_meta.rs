// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Metadata rows and query indexes on real storage.
//!
//! Metadata is the source of truth for `createdAt` and for every query index,
//! so three things must hold on **every** creation path: the row is present in
//! the creating commit, carries the right target/parent/timestamp, and
//! disappears in the purging commit. The class timestamp `t` of
//! `paused`/`completed`/`failed` lives in the status payload (format 10); only
//! paths that know the class write it — waking only writes `running` for a
//! `suspended` invocation.

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
    cairn_engine::{CreateOptions, Engine, InvocationStatus, ReplyTo, Source, Target},
    cairn_protocol::{encode_message, pb, Flags, Frame, FrameDecoder, MessageType},
    cairn_runtime::{Completion, CompletionResult, Event, StartParams},
    cairn_store::{
        clock::ManualClock, keys, make_invocation_id, LeadershipGrant, PartitionId, PartitionKey,
        PartitionWriter, QueueName,
    },
    slatedb::{
        config::Settings,
        object_store::{local::LocalFileSystem, ObjectStore},
        Db,
    },
    std::{path::PathBuf, sync::Arc, time::Duration},
};

const T0: u64 = 1_700_000_000_000;
const WAKE_AT: u64 = T0 + 3_600_000;

fn target() -> Target {
    Target {
        tenant: Default::default(),
        scope: None,
        service: "Svc".into(),
        handler: "h".into(),
        object_key: String::new(),
    }
}

fn inv_id(n: u8) -> [u8; 24] {
    make_invocation_id(PartitionKey(0), [n; 16])
}

fn frame<M: prost::Message>(ty: MessageType, m: &M) -> Frame {
    let wire = encode_message(ty, Flags::NONE, m);
    let mut buf = bytes::BytesMut::from(&wire[..]);
    FrameDecoder::new().decode(&mut buf).unwrap().unwrap()
}

async fn open_engine(name: &str) -> Result<(Engine, Arc<ManualClock>)> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("query-meta-{name}"));
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
    let clock = Arc::new(ManualClock::new(T0));
    let engine = Engine::open(writer).await?.with_clock(clock.clone());
    Ok((engine, clock))
}

/// The shared queue of every invocation in this file: the lifecycle core
/// (feature `lifecycle-check`, on when the whole workspace builds together)
/// considers a live invocation without a queue entry outside the valid set.
static QUEUE: std::sync::LazyLock<QueueName> = std::sync::LazyLock::new(|| QueueName {
    tenant: Default::default(),
    scope: None,
    limit: None,
    exclusive: false,
    service: "Svc".into(),
    object_key: String::new(),
});

async fn queued(engine: &mut Engine) -> Result<Option<(&'static QueueName, u64)>> {
    Ok(Some((&*QUEUE, engine.next_queue_seq(&QUEUE).await?)))
}

async fn create(engine: &mut Engine, n: u8, mut opts: CreateOptions<'_>) -> Result<[u8; 24]> {
    opts.queue = queued(engine).await?;
    engine
        .create(
            inv_id(n),
            &target(),
            opts,
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    Ok(inv_id(n))
}

/// `Start` → `sleep(WAKE_AT)` → `Suspension([1])`.
async fn suspend_on_timer(engine: &mut Engine, id: [u8; 24]) -> Result<()> {
    let mut inv = engine.load(id, StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    for f in [
        frame(
            MessageType::SLEEP,
            &pb::SleepCommandMessage {
                wake_up_time: WAKE_AT,
                result_completion_id: 1,
                name: String::new(),
            },
        ),
        frame(
            MessageType::SUSPENSION,
            &pb::SuspensionMessage {
                waiting_completions: vec![1],
                ..Default::default()
            },
        ),
    ] {
        engine.step(&mut inv, Event::FromSdk(f)).await?;
    }
    Ok(())
}

/// Reload, replay, deliver the timer — the server's path (`settle_completion`).
async fn fire_timer(engine: &mut Engine, id: [u8; 24]) -> Result<()> {
    let mut inv = engine.load(id, StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    engine
        .deliver(
            &mut inv,
            Completion {
                id: 1,
                result: CompletionResult::Timer,
            },
            Source::Timer { due_ms: WAKE_AT },
        )
        .await?;
    Ok(())
}

/// Runs to `Output` + `End` on a loaded invocation.
async fn complete(engine: &mut Engine, id: [u8; 24]) -> Result<()> {
    let mut inv = engine.load(id, StartParams::default()).await?;
    engine.step(&mut inv, Event::Start).await?;
    for f in [
        frame(
            MessageType::OUTPUT,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from_static(b"\"ok\"")),
                )),
                name: String::new(),
            },
        ),
        frame(MessageType::END, &pb::EndMessage {}),
    ] {
        engine.step(&mut inv, Event::FromSdk(f)).await?;
    }
    Ok(())
}

/// The `t` stamp in the raw status payload (format 10: `tag ‖ t(u64) ‖ …` for
/// `completed`/`failed`/`paused`).
async fn raw_t(engine: &Engine, id: &[u8; 24]) -> Result<Option<u64>> {
    let raw = engine
        .writer()
        .get(&keys::invocation_status(id))
        .await?
        .expect("status row present");
    Ok(raw
        .get(1..9)
        .map(|b| u64::from_be_bytes(b.try_into().unwrap())))
}

/// **Every creation path writes metadata in the same commit**, and purge deletes it in the same commit.
///
/// 1. Create a root (no `reply_to`), with an idempotency key and a deployment.
/// 2. Create a child (`reply_to` = the caller).
/// 3. `restart-as-new` from (1) at a later time.
/// 4. End (1), purge it manually.
///
/// Must see: (1) a root, timestamp = the engine clock, idempotency + deployment
/// TLVs; (2) not a root, `parent` = the caller; (3) the target copied,
/// `restarted_from` = (1), a new timestamp; (4) no metadata row of (1) left.
#[tokio::test]
async fn every_invocation_has_meta() -> Result<()> {
    let (mut engine, clock) = open_engine("every").await?;
    let root = create(
        &mut engine,
        1,
        CreateOptions {
            idempotency_key: Some("k-1"),
            deployment: Some("dp_1"),
            ..Default::default()
        },
    )
    .await?;
    let m = engine
        .writer()
        .invocation_meta(&root)
        .await?
        .expect("root has metadata");
    assert_eq!(
        (m.created_ms, m.root, m.parent),
        (T0, true, None),
        "root: {m:?}"
    );
    assert_eq!(
        (m.tenant.as_str(), m.service.as_str(), m.handler.as_str()),
        ("default", "Svc", "h")
    );
    assert_eq!(m.idempotency_key.as_deref(), Some("k-1"));
    assert_eq!(m.deployment.as_deref(), Some("dp_1"));

    clock.advance(Duration::from_secs(1));
    let reply = ReplyTo {
        caller: root,
        completion_id: 7,
    };
    let child = create(
        &mut engine,
        2,
        CreateOptions {
            reply_to: Some(&reply),
            ..Default::default()
        },
    )
    .await?;
    let m = engine
        .writer()
        .invocation_meta(&child)
        .await?
        .expect("child has metadata");
    assert_eq!(
        (m.created_ms, m.root, m.parent),
        (T0 + 1000, false, Some(root)),
        "con: {m:?}"
    );

    clock.advance(Duration::from_secs(1));
    let again = inv_id(3);
    let q = queued(&mut engine).await?;
    engine.restart(&root, &again, 1, None, None, q).await?;
    let m = engine
        .writer()
        .invocation_meta(&again)
        .await?
        .expect("the restart copy has metadata");
    assert_eq!(
        (m.created_ms, m.service.as_str(), m.restarted_from),
        (T0 + 2000, "Svc", Some(root)),
        "restart: {m:?}"
    );

    // `root` has ended (continuedAsNew) — it can be purged.
    engine.purge_now(&root).await?;
    assert_eq!(engine.writer().invocation_meta(&root).await?, None);
    Ok(())
}

/// **The limit key goes into the metadata**: an invocation entering a queue with
/// a `limit` ⇒ `InvocationMeta.limit_key` carries it; restart-as-new keeps it.
#[tokio::test]
async fn limit_key_is_in_meta() -> Result<()> {
    let (mut engine, _clock) = open_engine("limit").await?;
    let queue = QueueName {
        limit: Some(cairn_store::LimitKey::parse("growth/alice")?),
        ..QUEUE.clone()
    };
    let seq = engine.next_queue_seq(&queue).await?;
    let id = inv_id(9);
    engine
        .create(
            id,
            &target(),
            CreateOptions {
                queue: Some((&queue, seq)),
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    let m = engine
        .writer()
        .invocation_meta(&id)
        .await?
        .expect("has metadata");
    assert_eq!(m.limit_key.as_deref(), Some("growth/alice"), "{m:?}");
    let again = inv_id(10);
    let seq = engine.next_queue_seq(&queue).await?;
    engine
        .restart(&id, &again, 1, None, None, Some((&queue, seq)))
        .await?;
    let m = engine
        .writer()
        .invocation_meta(&again)
        .await?
        .expect("restart copy");
    assert_eq!(
        m.limit_key.as_deref(),
        Some("growth/alice"),
        "restart: {m:?}"
    );
    Ok(())
}

/// **`createdAt` is unchanged across every transition**, and comes from the
/// metadata (the engine's clock), not from entry 0's `create_ts`.
///
/// 1. Create at `T0`. 2. Suspend at `T0+1s`. 3. Wake at `T0+2s`. 4. Finish at `T0+3s`.
///
/// Must see: creation time `T0` after every step, both in the metadata and in
/// `target_created` (what `GET /invocations/{id}` reads); `t` of `completed` is
/// `T0+3s`.
#[tokio::test]
async fn created_at_is_stable_across_transitions() -> Result<()> {
    let (mut engine, clock) = open_engine("stable").await?;
    let id = create(&mut engine, 1, CreateOptions::default()).await?;
    async fn check(engine: &Engine, id: &[u8; 24]) -> Result<()> {
        let m = engine.writer().invocation_meta(id).await?.expect("present");
        assert_eq!(m.created_ms, T0);
        let (_, created) = engine.target_created(id).await?;
        assert_eq!(created, Some(T0), "API createdAt");
        Ok(())
    }
    check(&engine, &id).await?;
    clock.advance(Duration::from_secs(1));
    suspend_on_timer(&mut engine, id).await?;
    assert_eq!(engine.status(&id).await?, Some(InvocationStatus::Suspended));
    check(&engine, &id).await?;
    clock.advance(Duration::from_secs(1));
    fire_timer(&mut engine, id).await?;
    assert_eq!(engine.status(&id).await?, Some(InvocationStatus::Running));
    check(&engine, &id).await?;
    clock.advance(Duration::from_secs(1));
    complete(&mut engine, id).await?;
    assert!(matches!(
        engine.status(&id).await?,
        Some(InvocationStatus::Completed(_))
    ));
    check(&engine, &id).await?;
    assert_eq!(raw_t(&engine, &id).await?, Some(T0 + 3000));
    Ok(())
}

/// **Waking only touches `suspended`** (the caller declares the old class):
/// the wake path declares class `active` by construction, so it must never
/// write `running` over a `paused` invocation — doing so loses its `t` and
/// leaves the index row in the wrong class.
///
/// 1. `a`, `b` suspend on timers. 2. Pause `b` at `T0+5s`. 3. At `T0+9s` both
///    timers fire.
///
/// Must see: `a` becomes `running`; `b` stays `paused`, `t` still `T0+5s`.
#[tokio::test]
async fn wake_only_suspended() -> Result<()> {
    let (mut engine, clock) = open_engine("wake").await?;
    let a = create(&mut engine, 1, CreateOptions::default()).await?;
    let b = create(&mut engine, 2, CreateOptions::default()).await?;
    suspend_on_timer(&mut engine, a).await?;
    suspend_on_timer(&mut engine, b).await?;
    clock.set_wall_ms(T0 + 5000);
    assert!(engine.pause_with(&b, 571, "operator").await?);
    assert_eq!(raw_t(&engine, &b).await?, Some(T0 + 5000));
    clock.set_wall_ms(T0 + 9000);
    fire_timer(&mut engine, a).await?;
    fire_timer(&mut engine, b).await?;
    assert_eq!(engine.status(&a).await?, Some(InvocationStatus::Running));
    assert!(
        matches!(
            engine.status(&b).await?,
            Some(InvocationStatus::Paused { .. })
        ),
        "paused is not woken"
    );
    assert_eq!(
        raw_t(&engine, &b).await?,
        Some(T0 + 5000),
        "paused keeps its t"
    );
    Ok(())
}

// ───────────────────────────────────────────── query indexes

/// Every query-index row present (the `QUERY_CODES` code range).
async fn query_rows(engine: &Engine) -> Result<std::collections::BTreeSet<(Vec<u8>, Vec<u8>)>> {
    let lo = vec![0x01, *cairn_store::query_index::QUERY_CODES.start()];
    let hi = vec![0x01, *cairn_store::query_index::QUERY_CODES.end() + 1];
    Ok(engine
        .writer()
        .query_rows(lo, hi, usize::MAX)
        .await?
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect())
}

/// The index rows that **must exist** for the ids in `ids`: derived from the
/// metadata and status rows through the descriptor table itself — exactly what
/// the background build will compute.
async fn expected_rows(
    engine: &Engine,
    ids: &[[u8; 24]],
) -> Result<std::collections::BTreeSet<(Vec<u8>, Vec<u8>)>> {
    use cairn_store::query_index::{rows, Placement, QUERY_INDEXES};
    let mut out = std::collections::BTreeSet::new();
    for id in ids {
        let (Some(meta), Some((status, at))) =
            (engine.meta(id).await?, engine.status_at(id).await?)
        else {
            continue;
        };
        let p = Placement::new(status.class(), at.unwrap_or(0), &meta);
        for ix in QUERY_INDEXES {
            out.extend(rows(ix, id, &meta, p));
        }
    }
    Ok(out)
}

/// The seed of the deterministic generator (xorshift) — enough for a
/// repeatable random walk; prints the seed on failure.
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

/// **The indexes follow every transition sequence.**
///
/// A random walk (deterministic by seed) over 6 id slots: create (plain,
/// `paused`, a child with `reply_to`, with tags), suspend, wake, pause, resume,
/// finish, cancel, `restart-as-new`, purge; the clock jumps a random amount
/// between two steps.
///
/// Must see: after **every** step, the set of query-index rows is exactly the
/// set derived from (metadata, status) of every live invocation — nothing
/// missing, no orphans, no wrong class.
#[tokio::test]
async fn class_indexes_follow_transitions() -> Result<()> {
    for seed in [0x9E37_79B9_7F4A_7C15u64, 7, 4242, 0xDEAD_BEEF] {
        walk(seed)
            .await
            .map_err(|e| anyhow::anyhow!("seed {seed:#x}: {e}"))?;
    }
    Ok(())
}

async fn walk(seed: u64) -> Result<()> {
    let (mut engine, clock) = open_engine(&format!("walk-{seed:x}")).await?;
    let mut rng = Rng(seed);
    let slots: Vec<[u8; 24]> = (1..=6).map(inv_id).collect();
    // A slot that has suspended once: the journal has a `sleep`, so `complete`
    // (whose replay does not resend `sleep`) is no longer valid — end with a
    // cancel.
    let mut slept = [false; 6];
    let caller = ReplyTo {
        caller: inv_id(99),
        completion_id: 1,
    };
    for step in 0..120 {
        clock.advance(Duration::from_millis(1 + rng.below(5_000)));
        let i = rng.below(6) as usize;
        let id = slots[i];
        let status = engine.status(&id).await?;
        let op = rng.below(9);
        let what = match (&status, op) {
            (None, 0..=1) => {
                create(&mut engine, i as u8 + 1, CreateOptions::default()).await?;
                slept[i] = false;
                "create"
            }
            (None, 2) => {
                create(
                    &mut engine,
                    i as u8 + 1,
                    CreateOptions {
                        paused: Some((577, "hold")),
                        ..Default::default()
                    },
                )
                .await?;
                slept[i] = false;
                "create paused"
            }
            (None, _) => {
                create(
                    &mut engine,
                    i as u8 + 1,
                    CreateOptions {
                        reply_to: Some(&caller),
                        tags: &[("env", "prod"), ("team", "a")],
                        ..Default::default()
                    },
                )
                .await?;
                slept[i] = false;
                "create child with tags"
            }
            (Some(InvocationStatus::Running), 0..=1) if !slept[i] => {
                suspend_on_timer(&mut engine, id).await?;
                slept[i] = true;
                "suspend"
            }
            (Some(InvocationStatus::Suspended), 0..=2) => {
                fire_timer(&mut engine, id).await?;
                "wake"
            }
            (Some(InvocationStatus::Running), 2..=3) if !slept[i] => {
                complete(&mut engine, id).await?;
                "done"
            }
            (Some(InvocationStatus::Paused { .. }), 0..=3) => {
                engine.resume(&id).await?;
                "resume"
            }
            (Some(s), 4..=5) if !s.is_terminal() => {
                engine.pause_with(&id, 575, "out of attempts").await?;
                "pause"
            }
            (Some(s), 6) if !s.is_terminal() => {
                engine.cancel(&id, "cancel").await?;
                "cancel"
            }
            (Some(_), 7) => {
                let j = rng.below(6) as usize;
                if engine.status(&slots[j]).await?.is_some() {
                    continue;
                }
                let q = queued(&mut engine).await?;
                engine.restart(&id, &slots[j], 1, None, None, q).await?;
                slept[j] = false;
                "restart"
            }
            (Some(s), _) if s.is_terminal() => {
                engine.purge_now(&id).await?;
                "purge"
            }
            _ => continue,
        };
        let got = query_rows(&engine).await?;
        let want = expected_rows(&engine, &slots).await?;
        anyhow::ensure!(
            got == want,
            "step {step} ({what} slot {i}): extra {:?}, missing {:?}",
            got.difference(&want).count(),
            want.difference(&got).count()
        );
    }
    Ok(())
}

/// **`running ↔ suspended` touches no index**: the most frequent transition,
/// including waking, reads no metadata and writes no index row.
///
/// 1. Create. 2. Suspend. 3. Wake. Between steps wait long enough for the
///    `create_ts` of a rewrite to differ.
///
/// Must see: after (1) there are index rows; after (2) and (3) the same set of
/// rows, each keeping its `create_ts` (not rewritten).
#[tokio::test]
async fn running_suspended_writes_no_index() -> Result<()> {
    let (mut engine, _clock) = open_engine("rs-no-index").await?;
    let id = create(&mut engine, 1, CreateOptions::default()).await?;
    async fn stamped(engine: &Engine) -> Result<Vec<(Vec<u8>, i64)>> {
        let mut out = Vec::new();
        for (k, _) in query_rows(engine).await? {
            let (_, ts) = engine.writer().get_meta(&k).await?.expect("row remains");
            out.push((k, ts));
        }
        Ok(out)
    }
    let before = stamped(&engine).await?;
    anyhow::ensure!(!before.is_empty(), "create must write index rows");
    tokio::time::sleep(Duration::from_millis(20)).await;
    suspend_on_timer(&mut engine, id).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    fire_timer(&mut engine, id).await?;
    assert_eq!(engine.status(&id).await?, Some(InvocationStatus::Running));
    assert_eq!(stamped(&engine).await?, before, "no row was rewritten");
    Ok(())
}

/// **Changing class: an overwriting put in the class-in-value streams, delete +
/// put in `ByTransition`.** Create and then finish an invocation.
///
/// Must see: the `ByCreated`/`ByService` keys stay the same, the values carry the
/// new class; the old `ByTransition` key disappears, the new key is in class
/// `completed`.
#[tokio::test]
async fn class_change_overwrites_without_delete() -> Result<()> {
    use cairn_store::query_index::{decode_row, Class, BY_CREATED, BY_SERVICE, BY_TRANSITION};
    let (mut engine, clock) = open_engine("overwrite").await?;
    let id = create(&mut engine, 1, CreateOptions::default()).await?;
    let of = |rows: &std::collections::BTreeSet<(Vec<u8>, Vec<u8>)>, code: u8| {
        rows.iter()
            .filter(|(k, _)| k.get(1) == Some(&code))
            .cloned()
            .collect::<Vec<_>>()
    };
    let before = query_rows(&engine).await?;
    clock.advance(Duration::from_secs(5));
    complete(&mut engine, id).await?;
    let after = query_rows(&engine).await?;
    for ix in [BY_CREATED, BY_SERVICE] {
        let (b, a) = (of(&before, ix.code), of(&after, ix.code));
        assert_eq!((b.len(), a.len()), (1, 1), "{}", ix.name);
        assert_eq!(b[0].0, a[0].0, "{}: key unchanged", ix.name);
        let row = decode_row(&ix, &a[0].0, &a[0].1).expect("decodes");
        assert_eq!(row.class, Some(Class::Completed), "{}", ix.name);
        assert_eq!(row.t, Some(T0 + 5000), "{}", ix.name);
    }
    let (b, a) = (
        of(&before, BY_TRANSITION.code),
        of(&after, BY_TRANSITION.code),
    );
    assert_eq!((b.len(), a.len()), (1, 1));
    assert_ne!(b[0].0, a[0].0, "ByTransition changes key");
    let row = decode_row(&BY_TRANSITION, &a[0].0, &a[0].1).expect("decodes");
    assert_eq!(
        (row.class, row.t),
        (Some(Class::Completed), Some(T0 + 5000))
    );
    Ok(())
}

/// **Purge leaves no index row behind** (the purge rule): create (with tags),
/// finish, purge.
///
/// Must see: before the purge there are index rows of the id; after the purge
/// no row is left, no metadata.
#[tokio::test]
async fn purge_leaves_no_query_rows() -> Result<()> {
    let (mut engine, _clock) = open_engine("purge").await?;
    let id = create(
        &mut engine,
        1,
        CreateOptions {
            tags: &[("env", "prod")],
            ..Default::default()
        },
    )
    .await?;
    complete(&mut engine, id).await?;
    let rows = query_rows(&engine).await?;
    anyhow::ensure!(rows.len() >= 4, "rows in four indexes: {}", rows.len());
    engine.purge_now(&id).await?;
    assert_eq!(query_rows(&engine).await?.len(), 0, "no orphan rows");
    assert_eq!(engine.meta(&id).await?, None);
    Ok(())
}

/// **`create` over an existing id leaves no rows of the old position** (found
/// by query chaos testing: `verify` (i) reported "4 extra" after a `kill -9`):
///
/// 1. Create id X at T (`active` index rows at `created = T`).
/// 2. The clock jumps 2 s; some creation path creates **the same** X again (new
///    metadata, `created = T + 2 s`).
///
/// Must see: the set of index rows is exactly the set derived from the current
/// (metadata, status) — the four rows of the old position are removed; the
/// counters count X once.
#[tokio::test]
async fn create_over_existing_id_replaces_query_rows() -> Result<()> {
    let (mut engine, clock) = open_engine("create-again").await?;
    let id = create(&mut engine, 1, CreateOptions::default()).await?;
    clock.advance(std::time::Duration::from_secs(2));
    create(&mut engine, 1, CreateOptions::default()).await?;
    let want = expected_rows(&engine, &[id]).await?;
    let have = query_rows(&engine).await?;
    anyhow::ensure!(
        have == want,
        "{} extra rows, {} missing rows",
        have.difference(&want).count(),
        want.difference(&have).count()
    );
    // The counters (once built) count X exactly once.
    let mut c = BuildCursor::default();
    while engine.query_build_step(&mut c, 1024).await? != BuildStep::Idle {}
    let n: u64 = engine
        .query_counts()
        .snapshot()
        .expect("counted")
        .into_iter()
        .map(|(_, n)| n)
        .sum();
    anyhow::ensure!(n == 1, "counters: {n}");
    Ok(())
}

// ───────────────────────────────────────────── background build, status

use cairn_engine::{BuildCursor, BuildStep, IndexState};
use cairn_store::query_index::{
    Applies, Axis, ClassSet, Field, Part, QueryIndex, BY_ATTR, BY_CREATED, BY_SERVICE,
    BY_TRANSITION,
};

/// An experimental descriptor — tests only: class in the key, creation
/// axis.
const BY_CLASS_TEST: QueryIndex = QueryIndex {
    code: 0x11,
    name: "ByClassTest",
    version: 1,
    key: &[
        Part::Tenant,
        Part::Class,
        Part::Time(Axis::Created),
        Part::Id,
    ],
    classes: ClassSet::ALL,
    applies: Applies::Always,
    covers: &[Field::Service, Field::Handler],
};

/// As above, a different layout (also covering `T`) under **the same code**: version 2.
const BY_CLASS_TEST_V2: QueryIndex = QueryIndex {
    version: 2,
    covers: &[Field::Service, Field::Handler, Field::T],
    ..BY_CLASS_TEST
};

static WITH_TEST: [QueryIndex; 5] = [
    BY_CREATED,
    BY_SERVICE,
    BY_TRANSITION,
    BY_ATTR,
    BY_CLASS_TEST,
];
static WITH_TEST_V2: [QueryIndex; 5] = [
    BY_CREATED,
    BY_SERVICE,
    BY_TRANSITION,
    BY_ATTR,
    BY_CLASS_TEST_V2,
];

/// Reopens `name`'s Db (without deleting), with descriptor table `indexes`.
async fn reopen(name: &str, indexes: &'static [QueryIndex]) -> Result<(Engine, Arc<ManualClock>)> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("query-meta-{name}"));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let db = Db::builder("partition-0", store)
        .with_settings(Settings {
            flush_interval: Some(Duration::from_millis(10)),
            ..Default::default()
        })
        .build()
        .await?;
    let writer = PartitionWriter::open(LeadershipGrant::single_node(PartitionId(0)), db).await?;
    let clock = Arc::new(ManualClock::new(T0 + 3_600_000));
    let engine = Engine::open(writer)
        .await?
        .with_clock(clock.clone())
        .with_query_indexes(indexes);
    Ok((engine, clock))
}

async fn build_all(engine: &mut Engine, chunk: usize) -> Result<usize> {
    let mut cursor = BuildCursor::default();
    let mut steps = 0;
    while engine.query_build_step(&mut cursor, chunk).await? != BuildStep::Idle {
        steps += 1;
        anyhow::ensure!(steps < 10_000, "the background build does not stop");
    }
    Ok(steps)
}

/// Like [`expected_rows`] but from the engine's descriptor table.
async fn expected_for(
    engine: &Engine,
    ids: &[[u8; 24]],
) -> Result<std::collections::BTreeSet<(Vec<u8>, Vec<u8>)>> {
    use cairn_store::query_index::{rows, Placement};
    let mut out = std::collections::BTreeSet::new();
    for id in ids {
        let (Some(meta), Some((status, at))) =
            (engine.meta(id).await?, engine.status_at(id).await?)
        else {
            continue;
        };
        let p = Placement::new(status.class(), at.unwrap_or(0), &meta);
        for ix in engine.query_indexes() {
            out.extend(rows(ix, id, &meta, p));
        }
    }
    Ok(out)
}

fn state_of(states: &[(&str, IndexState)], name: &str) -> Option<IndexState> {
    states.iter().find(|(n, _)| *n == name).map(|(_, s)| *s)
}

/// Builds `n` invocations (slots `1..=n`): a third finished, a third paused.
async fn populate(engine: &mut Engine, n: u8) -> Result<Vec<[u8; 24]>> {
    let mut ids = Vec::new();
    for i in 1..=n {
        let id = create(engine, i, CreateOptions::default()).await?;
        match i % 3 {
            0 => complete(engine, id).await?,
            1 => {
                engine.pause_with(&id, 575, "out of attempts").await?;
            }
            _ => {}
        }
        ids.push(id);
    }
    Ok(ids)
}

/// **Adding an index builds in the background without stopping writes.**
///
/// 1. A range with the default table, 20 invocations, built ⇒ four descriptors `readable`.
/// 2. Reopen with a table that adds `ByClassTest` v1.
/// 3. Build in chunks of 3 rows; between chunks one live transition (create,
///    finish, pause, resume, purge).
///
/// Must see: after (2) `ByClassTest` is `building`, the other four descriptors
/// are still `readable` and `readable_query_indexes` does not include it;
/// every live transition between chunks commits; after (3) all five are
/// `readable`, the index rows are exactly the set derived from the status
/// through the new table.
#[tokio::test]
async fn adding_an_index_builds_in_background() -> Result<()> {
    let name = "add-index";
    let (mut engine, _) = open_engine(name).await?;
    let mut ids = populate(&mut engine, 20).await?;
    build_all(&mut engine, 1024).await?;
    let states = engine.query_index_states().await?;
    assert!(
        states.iter().all(|(_, s)| *s == IndexState::Readable),
        "{states:?}"
    );
    engine.seal().await?;

    let (mut engine, clock) = reopen(name, &WITH_TEST).await?;
    let states = engine.query_index_states().await?;
    assert_eq!(state_of(&states, "ByClassTest"), Some(IndexState::Building));
    assert_eq!(state_of(&states, "ByCreated"), Some(IndexState::Readable));
    let readable: Vec<&str> = engine
        .readable_query_indexes()
        .await?
        .iter()
        .map(|ix| ix.name)
        .collect();
    assert!(
        !readable.contains(&"ByClassTest"),
        "the path selector must not use it yet"
    );

    let mut cursor = BuildCursor::default();
    let mut live = 0u8;
    while engine.query_build_step(&mut cursor, 3).await? != BuildStep::Idle {
        clock.advance(Duration::from_millis(250));
        live += 1;
        let id = ids[(live as usize * 7) % ids.len()];
        match (live % 5, engine.status(&id).await?) {
            (0, _) => ids.push(create(&mut engine, 100 + live, CreateOptions::default()).await?),
            (1, Some(InvocationStatus::Running)) => complete(&mut engine, id).await?,
            (2, Some(s)) if !s.is_terminal() => {
                engine.pause_with(&id, 575, "x").await?;
            }
            (3, Some(InvocationStatus::Paused { .. })) => {
                engine.resume(&id).await?;
            }
            (4, Some(s)) if s.is_terminal() => {
                engine.purge_now(&id).await?;
            }
            _ => {}
        }
    }
    let states = engine.query_index_states().await?;
    assert!(
        states.iter().all(|(_, s)| *s == IndexState::Readable),
        "{states:?}"
    );
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    Ok(())
}

/// **A child range after a split rebuilds the query indexes**: the child Db's
/// derived zone is empty (the projection filters it) — here deleted by hand
/// like `derived_rebuild`.
///
/// Must see: reopen ⇒ every descriptor `building`, no rows; built ⇒
/// `readable`, rows exactly the set derived from the status.
#[tokio::test]
async fn query_index_rebuilds_after_split() -> Result<()> {
    let name = "after-split";
    let (mut engine, _) = open_engine(name).await?;
    let ids = populate(&mut engine, 30).await?;
    build_all(&mut engine, 1024).await?;
    let want = query_rows(&engine).await?;
    engine.seal().await?;
    wipe_query_region(name).await?;

    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    let states = engine.query_index_states().await?;
    assert!(
        states.iter().all(|(_, s)| *s == IndexState::Building),
        "{states:?}"
    );
    assert!(query_rows(&engine).await?.is_empty());
    build_all(&mut engine, 4).await?;
    assert_eq!(query_rows(&engine).await?, want);
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    Ok(())
}

/// Deletes the query derived zone (codes `QUERY_CODES` and the `0xFC` mark) — a child Db.
async fn wipe_query_region(name: &str) -> Result<()> {
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("query-meta-{name}"));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
    let db = Db::builder("partition-0", store).build().await?;
    let mut it = db.scan(vec![0x01u8, 0x0D]..vec![0x01u8, 0xFD]).await?;
    let mut keys = vec![];
    while let Some(kv) = it.next().await? {
        keys.push(kv.key);
    }
    for k in &keys {
        db.delete(k).await?;
    }
    db.flush().await?;
    db.close().await?;
    Ok(())
}

/// **The background build races live writes** (a chunk lies entirely within
/// one engine hold; puts only). The query zone is deleted; build in chunks of
/// 2 rows; between two chunks, live transitions on ids **already past** the
/// cursor and ids **not reached yet** (finish, pause, resume, cancel, purge),
/// plus new creations.
///
/// Must see: once built, the index rows are exactly the set derived from the
/// final status — no old-class rows of ids already past, no missing rows of ids
/// created midway.
#[tokio::test]
async fn query_index_rebuild_races_live_writes() -> Result<()> {
    let name = "races";
    let (mut engine, _) = open_engine(name).await?;
    let mut ids = populate(&mut engine, 24).await?;
    engine.seal().await?;
    wipe_query_region(name).await?;
    let (mut engine, clock) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    let mut cursor = BuildCursor::default();
    let mut k = 0usize;
    while engine.query_build_step(&mut cursor, 2).await? != BuildStep::Idle {
        clock.advance(Duration::from_millis(100));
        k += 1;
        // One id at the start (already past), one at the end (not reached yet).
        for id in [ids[k % 6], ids[ids.len() - 1 - (k % 6)]] {
            match engine.status(&id).await? {
                Some(InvocationStatus::Running) if k.is_multiple_of(2) => {
                    complete(&mut engine, id).await?
                }
                Some(InvocationStatus::Running) => {
                    engine.cancel(&id, "cancel").await?;
                }
                Some(InvocationStatus::Paused { .. }) => {
                    engine.resume(&id).await?;
                }
                Some(s) if s.is_terminal() && k.is_multiple_of(3) => {
                    engine.purge_now(&id).await?;
                }
                _ => {}
            }
        }
        if k.is_multiple_of(4) {
            ids.push(create(&mut engine, 200 + k as u8, CreateOptions::default()).await?);
        }
    }
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    Ok(())
}

/// **Removing a descriptor ⇒ its rows are cleaned up; changing the version ⇒
/// rebuilt under the new layout.**
///
/// 1. A table with `ByClassTest` v1, built. 2. Reopen with `ByClassTest` v2.
/// 3. Reopen with the default table.
///
/// Must see: after (2) the code-`0x11` rows have exactly the v2 layout
/// (decodable with v2); after (3) no code-`0x11` row is left, the other four
/// descriptors are unchanged.
#[tokio::test]
async fn dropped_index_rows_are_swept() -> Result<()> {
    use cairn_store::query_index::decode_row;
    let name = "dropped";
    let (engine, _) = open_engine(name).await?;
    engine.seal().await?;
    let (mut engine, _) = reopen(name, &WITH_TEST).await?;
    let ids = populate(&mut engine, 12).await?;
    build_all(&mut engine, 5).await?;
    let v1: Vec<_> = query_rows(&engine)
        .await?
        .into_iter()
        .filter(|(k, _)| k[1] == 0x11)
        .collect();
    assert_eq!(v1.len(), 12);
    engine.seal().await?;

    let (mut engine, _) = reopen(name, &WITH_TEST_V2).await?;
    assert_eq!(
        state_of(&engine.query_index_states().await?, "ByClassTest"),
        Some(IndexState::Building)
    );
    build_all(&mut engine, 5).await?;
    let v2: Vec<_> = query_rows(&engine)
        .await?
        .into_iter()
        .filter(|(k, _)| k[1] == 0x11)
        .collect();
    assert_eq!(v2.len(), ids.len());
    for (k, v) in &v2 {
        assert!(
            decode_row(&BY_CLASS_TEST_V2, k, v).is_some(),
            "v2 layout row"
        );
    }
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    engine.seal().await?;

    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    build_all(&mut engine, 5).await?;
    assert!(
        query_rows(&engine).await?.iter().all(|(k, _)| k[1] != 0x11),
        "rows of a removed descriptor left behind"
    );
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    Ok(())
}

/// **Death mid-build** (the cursor lives only in RAM, the mark is written
/// last): build a few chunks then drop the engine (like a process death),
/// reopen.
///
/// Must see: after the death the mark is not there yet (every descriptor
/// `building`); the next open rebuilds from the start to the end, rows correct.
#[tokio::test]
async fn query_build_resumes_after_death_mid_chunk() -> Result<()> {
    let name = "death";
    let (mut engine, _) = open_engine(name).await?;
    let ids = populate(&mut engine, 15).await?;
    engine.seal().await?;
    wipe_query_region(name).await?;
    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    let mut cursor = BuildCursor::default();
    for _ in 0..3 {
        engine.query_build_step(&mut cursor, 2).await?;
    }
    drop(engine);
    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    let states = engine.query_index_states().await?;
    assert!(
        states.iter().all(|(_, s)| *s == IndexState::Building),
        "{states:?}"
    );
    build_all(&mut engine, 2).await?;
    assert_eq!(
        query_rows(&engine).await?,
        expected_for(&engine, &ids).await?
    );
    Ok(())
}

// ───────────────────────────────────────────── in-RAM counters

/// An exhaustive count from metadata + status: `(tenant, service, handler,
/// class, root) → n`.
async fn exhaustive_counts(
    engine: &Engine,
    ids: &[[u8; 24]],
) -> Result<Vec<(cairn_store::query_index::CountKey, u64)>> {
    let mut m = std::collections::BTreeMap::new();
    for id in ids {
        let (Some(meta), Some(status)) = (engine.meta(id).await?, engine.status(id).await?) else {
            continue;
        };
        *m.entry(cairn_store::query_index::CountKey {
            tenant: meta.tenant,
            service: meta.service,
            handler: meta.handler,
            class: status.class(),
            root: meta.root,
        })
        .or_insert(0u64) += 1;
    }
    Ok(m.into_iter().collect())
}

/// **The counters match an exhaustive count across every transition sequence
/// and rebuild.**
///
/// 1. 18 invocations (children and roots, three classes).
/// 2. Background build + counters in chunks of 2 rows; between chunks one live
///    transition on an id already past the cursor and one not reached yet
///    (finish, pause, resume, cancel, purge, create).
/// 3. A batch of transitions after the build finished.
/// 4. Delete the query zone (like a child Db after a split), reopen, rebuild.
///
/// Must see: before the build finishes `snapshot` is `None`; after (2), (3),
/// (4) it equals the exhaustive count.
#[tokio::test]
async fn counts_match_scan() -> Result<()> {
    let name = "counts";
    let (mut engine, clock) = open_engine(name).await?;
    let mut ids = populate(&mut engine, 18).await?;
    let caller = ReplyTo {
        caller: inv_id(99),
        completion_id: 1,
    };
    for i in 0..4u8 {
        ids.push(
            create(
                &mut engine,
                40 + i,
                CreateOptions {
                    reply_to: Some(&caller),
                    ..Default::default()
                },
            )
            .await?,
        );
    }
    assert_eq!(engine.query_counts().snapshot(), None, "not built");
    let mut cursor = BuildCursor::default();
    let mut k = 0usize;
    while engine.query_build_step(&mut cursor, 2).await? != BuildStep::Idle {
        clock.advance(Duration::from_millis(50));
        k += 1;
        for id in [ids[k % ids.len()], ids[ids.len() - 1 - (k % ids.len())]] {
            match engine.status(&id).await? {
                Some(InvocationStatus::Running) if k.is_multiple_of(2) => {
                    complete(&mut engine, id).await?
                }
                Some(InvocationStatus::Running) => {
                    engine.pause_with(&id, 575, "x").await?;
                }
                Some(InvocationStatus::Paused { .. }) if k.is_multiple_of(3) => {
                    engine.cancel(&id, "cancel").await?;
                }
                Some(InvocationStatus::Paused { .. }) => {
                    engine.resume(&id).await?;
                }
                Some(s) if s.is_terminal() && k.is_multiple_of(5) => {
                    engine.purge_now(&id).await?;
                }
                _ => {}
            }
        }
        if k.is_multiple_of(4) {
            ids.push(create(&mut engine, 100 + k as u8, CreateOptions::default()).await?);
        }
    }
    assert_eq!(
        engine.query_counts().snapshot(),
        Some(exhaustive_counts(&engine, &ids).await?)
    );

    for (n, id) in ids.clone().into_iter().enumerate() {
        match engine.status(&id).await? {
            Some(InvocationStatus::Running) if n % 2 == 0 => complete(&mut engine, id).await?,
            Some(s) if !s.is_terminal() => {
                engine.cancel(&id, "cancel").await?;
            }
            Some(_) => {
                engine.purge_now(&id).await?;
            }
            None => {}
        }
    }
    assert_eq!(
        engine.query_counts().snapshot(),
        Some(exhaustive_counts(&engine, &ids).await?)
    );
    engine.seal().await?;

    wipe_query_region(name).await?;
    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    assert_eq!(engine.query_counts().snapshot(), None);
    build_all(&mut engine, 3).await?;
    assert_eq!(
        engine.query_counts().snapshot(),
        Some(exhaustive_counts(&engine, &ids).await?)
    );
    Ok(())
}

// ───────────────────────────────────────────── restart lineage

/// **The `restart-as-new` lineage is queryable in both directions**: new copy →
/// old copy through the `restarted_from` TLV; old copy → every new copy through
/// `ByAttr[$restarted_from = old id]` (one prefix).
///
/// 1. `a` runs, restarts into `b`; `b` finishes, restart again into `c` (from
///    `a`, which has ended ⇒ `a` stays as it is, `c` is a rerun).
///
/// Must see: `b`, `c` carry `restarted_from = a`; the `ByAttr` prefix scan of
/// `$restarted_from = hex(a)` yields exactly `{b, c}`, newest first.
#[tokio::test]
async fn restart_lineage_is_queryable() -> Result<()> {
    use cairn_store::{
        id_hex,
        query_index::{attr, decode_row, scan_prefix, Fixed, BY_ATTR},
    };
    let (mut engine, clock) = open_engine("lineage").await?;
    let a = create(&mut engine, 1, CreateOptions::default()).await?;
    clock.advance(Duration::from_secs(1));
    let b = inv_id(2);
    let q = queued(&mut engine).await?;
    engine.restart(&a, &b, 1, None, None, q).await?;
    clock.advance(Duration::from_secs(1));
    let c = inv_id(3);
    let q = queued(&mut engine).await?;
    engine.restart(&a, &c, 1, None, None, q).await?;

    for id in [b, c] {
        let m = engine.meta(&id).await?.expect("has metadata");
        assert_eq!(m.restarted_from, Some(a), "new copy → old copy");
    }
    let old = id_hex(&a);
    let prefix = scan_prefix(
        &BY_ATTR,
        &[
            Fixed::Tenant("default"),
            Fixed::Attr(attr::RESTARTED_FROM, &old),
        ],
    )
    .expect("valid prefix");
    let hi = cairn_store::query_index::prefix_end(&prefix);
    let found: Vec<[u8; 24]> = engine
        .writer()
        .query_rows(prefix, hi, 100)
        .await?
        .iter()
        .filter_map(|(k, v)| decode_row(&BY_ATTR, k, v))
        .map(|r| r.id)
        .collect();
    assert_eq!(found, vec![c, b], "old copy → new copies, newest first");
    Ok(())
}

// ──────────────────────────────────── the query layer's write cost

/// Commit bytes (key + value, `WriterHealth::bytes_written`) of one echo —
/// `create` then `Output` + `End` — on an engine with `indexes`.
async fn echo_bytes(name: &str, indexes: &'static [QueryIndex], tags: bool) -> Result<u64> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("query-meta-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let (mut engine, _clock) = reopen(name, indexes).await?;
    let before = engine.writer().health().bytes_written();
    let tag_list: &[(&str, &str)] = if tags {
        &[("env", "prod"), ("team", "pay")]
    } else {
        &[]
    };
    let id = create(
        &mut engine,
        1,
        CreateOptions {
            tags: tag_list,
            ..Default::default()
        },
    )
    .await?;
    complete(&mut engine, id).await?;
    Ok(engine.writer().health().bytes_written() - before)
}

/// **Write cost per echo** (stop threshold "> 1 KB/echo"): how many bytes the
/// index rows of the four descriptors add compared with the same echo without
/// any descriptor (metadata is written in both — it replaces the old list's
/// entry-0 GET, its size is printed separately). Must see: the extra of
/// indexes + metadata < 1 KiB, with and without tags.
#[tokio::test]
async fn query_index_write_cost_per_echo() -> Result<()> {
    static NONE: [QueryIndex; 0] = [];
    for tags in [false, true] {
        let with = echo_bytes(&format!("cost-with-{tags}"), &QUERY_INDEXES_STATIC, tags).await?;
        let without = echo_bytes(&format!("cost-without-{tags}"), &NONE, tags).await?;
        let meta = {
            let name = format!("cost-meta-{tags}");
            let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("query-meta-{name}"));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)?;
            let (mut engine, _) = reopen(&name, &NONE).await?;
            let tag_list: &[(&str, &str)] = if tags {
                &[("env", "prod"), ("team", "pay")]
            } else {
                &[]
            };
            let id = create(
                &mut engine,
                2,
                CreateOptions {
                    tags: tag_list,
                    ..Default::default()
                },
            )
            .await?;
            let m = engine.meta(&id).await?.expect("has metadata");
            (cairn_store::query_index::meta_key(&id).len() + m.encode().len()) as u64
        };
        println!(
            "echo (tags: {tags}): {with} bytes with indexes, {without} bytes without — indexes +{} bytes, metadata {meta} bytes",
            with - without
        );
        anyhow::ensure!(
            with - without + meta < 1024,
            "indexes + metadata {} bytes/echo ≥ 1 KiB",
            with - without + meta
        );
    }
    Ok(())
}

static QUERY_INDEXES_STATIC: [QueryIndex; 4] = [BY_CREATED, BY_SERVICE, BY_TRANSITION, BY_ATTR];

// ─────────────────────────────────────────── ObjectKeys hint rows

/// **Hint rows survive a purge, the term's cleanup loop removes them**:
///
/// 1. Object `o-1` (one invocation) and `o-2` (one invocation + state), ids in
///    the object's owning range; background build finished.
/// 2. Purge both invocations: the two `ObjectKeys` rows are still there (the
///    write path does not delete hints).
/// 3. The hint cleanup loop runs (`hint_sweep_step`, chunks of 1 row).
///
/// Must see: after (3) only `o-2`'s row is left (it still has state).
#[tokio::test]
async fn object_hints_survive_purge_and_are_swept() -> Result<()> {
    let (mut engine, _clock) = open_engine("object-hints").await?;
    let tenant = cairn_store::TenantId::default();
    let mut ids = Vec::new();
    for (n, key) in [(1u8, "o-1"), (2, "o-2")] {
        let pk = cairn_store::keys::object_owner(&tenant, None, "Svc", key).unwrap();
        let id = cairn_store::make_invocation_id(pk, [n; 16]);
        let target = Target {
            object_key: key.into(),
            ..target()
        };
        let opts = CreateOptions {
            queue: queued(&mut engine).await?,
            ..Default::default()
        };
        engine
            .create(id, &target, opts, StartParams::default(), Bytes::new())
            .await?;
        if key == "o-2" {
            let mut m = cairn_store::Mutations::new();
            m.set_state(pk, &tenant, None, "Svc", "o-2", b"k", b"v");
            engine.writer_mut().commit(m).await?;
        }
        ids.push(id);
    }
    build_all(&mut engine, 64).await?;
    let hints = |rows: &std::collections::BTreeSet<(Vec<u8>, Vec<u8>)>| {
        rows.iter()
            .filter(|(k, _)| k[1] == cairn_store::query_index::OBJECT_KEYS.code)
            .filter_map(|(k, v)| {
                cairn_store::query_index::decode_row(&cairn_store::query_index::OBJECT_KEYS, k, v)
                    .and_then(|r| r.key)
            })
            .collect::<Vec<_>>()
    };
    anyhow::ensure!(hints(&query_rows(&engine).await?) == ["o-1", "o-2"]);
    for id in &ids {
        engine.cancel(id, "purge").await?;
        engine.purge_now(id).await?;
    }
    anyhow::ensure!(
        hints(&query_rows(&engine).await?) == ["o-1", "o-2"],
        "purge removed the hint"
    );
    // The cleanup loop (the background loop calls it a while after the build finishes), chunks of 1 row.
    let mut cursor = cairn_engine::HintCursor::default();
    let mut steps = 0;
    while engine.hint_sweep_step(&mut cursor, 1).await?.is_some() {
        steps += 1;
        anyhow::ensure!(steps < 100, "the cleanup loop does not stop");
    }
    let left = hints(&query_rows(&engine).await?);
    anyhow::ensure!(left == ["o-2"], "after the cleanup loop: {left:?}");
    Ok(())
}

// ──────────────────────── artifact byte counter

/// An exhaustive count of live artifact bytes: scan the whole hot zone, the
/// `Artifacts` rows that decode and whose id has metadata, summed per the
/// metadata's tenant.
async fn exhaustive_artifact_bytes(engine: &Engine) -> Result<Vec<(String, u64)>> {
    let mut m = std::collections::BTreeMap::<String, u64>::new();
    for (k, v) in engine
        .writer()
        .rows_under(&[0u8], false, usize::MAX)
        .await?
    {
        let Some((id, _)) = keys::parse_artifact(&k) else {
            continue;
        };
        let (Some(row), Some(meta)) = (keys::ArtifactRow::decode(&v), engine.meta(&id).await?)
        else {
            continue;
        };
        *m.entry(meta.tenant).or_default() += row.size;
    }
    Ok(m.into_iter().filter(|(_, n)| *n > 0).collect())
}

/// One artifact row `(id, sha(n), size)` through the shell's path
/// (`turn/sdk.rs`, `turn/life.rs`): `stage_artifact_put` then commit within the
/// same `&mut`.
async fn put_artifact(engine: &mut Engine, id: [u8; 24], n: u8, size: u64) -> Result<()> {
    let mut muts = cairn_store::Mutations::new();
    engine
        .stage_artifact_put(
            &mut muts,
            &id,
            &[n; 32],
            &keys::ArtifactRow {
                epoch: 7,
                size,
                content_type: "x".into(),
            },
        )
        .await?;
    engine.writer_mut().commit(muts).await?;
    Ok(())
}

/// Creates `inv_id(n)` for tenant `tenant` (that tenant's own queue).
async fn create_for(engine: &mut Engine, n: u8, tenant: &str) -> Result<[u8; 24]> {
    let tenant = cairn_store::TenantId::new(tenant)?;
    let q = QueueName {
        tenant: tenant.clone(),
        ..QUEUE.clone()
    };
    let seq = engine.next_queue_seq(&q).await?;
    engine
        .create(
            inv_id(n),
            &Target { tenant, ..target() },
            CreateOptions {
                queue: Some((&q, seq)),
                ..Default::default()
            },
            StartParams::default(),
            Bytes::new(),
        )
        .await?;
    Ok(inv_id(n))
}

/// **The artifact byte counter matches an exhaustive count across every write
/// path and rebuild** (Q-19).
///
/// 1. 12 invocations of two tenants (`acme`, `beta`), each with 1–3 artifacts;
///    one id **without** metadata carries a row (an orphan copy).
/// 2. Build the counter in chunks of 3 hot-zone rows; between chunks: a new
///    PUT, a PUT again of the same `(id, sha)` (not added a second time), purge
///    of a finished invocation, copying artifacts to a new id then `restart`
///    (the new copy's rows count from the restart commit, not from the copy
///    commit).
/// 3. A batch of changes after the build finished.
/// 4. Seal, reopen (like a `kill -9` / leader change), rebuild in chunks of 5.
///
/// Must see: `snapshot` is `None` before the build finishes; after (2), (3),
/// (4) it equals the exhaustive count.
#[tokio::test]
async fn artifact_bytes_match_scan() -> Result<()> {
    let name = "artifact-bytes";
    let (mut engine, _) = open_engine(name).await?;
    let mut ids = Vec::new();
    for i in 1..=12u8 {
        let tenant = if i % 2 == 0 { "acme" } else { "beta" };
        let id = create_for(&mut engine, i, tenant).await?;
        for a in 0..=(i % 3) {
            put_artifact(&mut engine, id, a, 100 * u64::from(i) + u64::from(a)).await?;
        }
        if i % 3 == 0 {
            complete(&mut engine, id).await?;
        }
        ids.push(id);
    }
    // A row of an id that was never created: does not count, on any path.
    put_artifact(&mut engine, inv_id(250), 1, 9_999).await?;
    assert_eq!(engine.artifact_bytes().snapshot(), None, "not built");

    let mut k = 0u8;
    let mut next = 60u8;
    while engine.artifact_bytes_step(3).await?.is_some() {
        k += 1;
        let id = ids[usize::from(k) % ids.len()];
        // A new PUT and a PUT again of the same `(id, sha)`.
        put_artifact(&mut engine, id, 10 + k % 4, 7).await?;
        put_artifact(&mut engine, id, 0, 7).await?;
        if k.is_multiple_of(3) {
            let done = ids[(usize::from(k) * 5) % ids.len()];
            if matches!(engine.status(&done).await?, Some(s) if s.is_terminal()) {
                engine.purge_now(&done).await?;
            }
        }
        if k.is_multiple_of(4) {
            let old = ids[(usize::from(k) * 7) % ids.len()];
            if engine.status(&old).await?.is_some() {
                let new = inv_id(next);
                next += 1;
                for (sha, row) in engine.writer().artifacts_of_unbounded(&old).await? {
                    if let Some(row) = row {
                        put_artifact(&mut engine, new, sha[0], row.size).await?;
                    }
                }
                let q = queued(&mut engine).await?;
                engine.restart(&old, &new, 1, None, None, q).await?;
                ids.push(new);
            }
        }
        anyhow::ensure!(k < 200, "the build does not stop");
    }
    assert_eq!(
        engine.artifact_bytes().snapshot(),
        Some(exhaustive_artifact_bytes(&engine).await?)
    );

    for (n, id) in ids.clone().into_iter().enumerate() {
        match engine.status(&id).await? {
            Some(s) if s.is_terminal() && n % 2 == 0 => {
                engine.purge_now(&id).await?;
            }
            Some(_) => put_artifact(&mut engine, id, 40, 33).await?,
            None => {}
        }
    }
    assert_eq!(
        engine.artifact_bytes().snapshot(),
        Some(exhaustive_artifact_bytes(&engine).await?)
    );
    let want = exhaustive_artifact_bytes(&engine).await?;
    assert!(want.len() == 2, "two tenants still have bytes: {want:?}");
    engine.seal().await?;

    let (mut engine, _) = reopen(name, cairn_store::query_index::QUERY_INDEXES).await?;
    assert_eq!(engine.artifact_bytes().snapshot(), None);
    while engine.artifact_bytes_step(5).await?.is_some() {}
    assert_eq!(engine.artifact_bytes().snapshot(), Some(want));
    Ok(())
}
