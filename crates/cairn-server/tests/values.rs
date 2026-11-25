// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The values sweep on a real node, and segmented values end to end.
//!
//! The sweep tests PUT `values/…` objects directly (as the shell PUTs before a
//! commit) and build holder rows with `EngineHandle::raw_commit` (under the
//! range's engine, `testkit`).
//!
//! | Case | Must see |
//! |---|---|
//! | orphan segment (no holder), older than `min_age` | held in place (a `Subject::Value` entry), then really deleted after `keep`: both object and entry go |
//! | segment that still has a holder | never gets an entry, the object stays |
//! | wrong hold entry for a segment that still has a holder (SM-16: deleted by mistake) | the entry is removed (restored), the object stays |
//! | segment younger than `min_age` | no entry |

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

mod common;

use {
    anyhow::{ensure, Result},
    bytes::Bytes,
    cairn_store::{
        proof::DeleteProof,
        trash::{EntryId, Subject, Trash},
        value::{Holder, Owner, Segment, ValueStore},
        Mutations, PartitionKey, TenantId,
    },
    slatedb::object_store::{path::Path, ObjectStore},
    std::{sync::Arc, time::Duration},
};

const DEADLINE: Duration = Duration::from_secs(20);

fn fast(keep: Duration, min_age: Duration) -> impl FnOnce(&mut cairn_server::node::NodeConfig) {
    move |cfg| {
        cfg.value_sweep = Some(Duration::from_millis(100));
        cfg.value_min_age = min_age;
        cfg.trash_keep = Some(keep);
    }
}

fn seg(n: u8) -> Segment {
    Segment {
        tenant: "default".into(),
        pk: PartitionKey(0x10),
        owner: Owner([n; 32]),
        sha: [n; 32],
        gen: 1,
    }
}

async fn start(
    bucket: &common::Bucket,
    keep: Duration,
    min_age: Duration,
) -> Result<common::ClusterNode> {
    let n = common::start_cluster_node_tweaked(
        bucket,
        "a",
        1,
        Duration::from_secs(60),
        fast(keep, min_age),
    )
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    Ok(n)
}

fn stores(bucket: &common::Bucket) -> (ValueStore, Trash) {
    let store: Arc<dyn ObjectStore> = (*bucket).clone();
    (
        ValueStore::new(Arc::clone(&store), &Path::default()),
        Trash::new(store, &Path::default()),
    )
}

async fn entry_of(trash: &Trash, s: &Segment) -> Result<Option<EntryId>> {
    Ok(trash
        .entries()
        .await?
        .into_iter()
        .find_map(|e| match e.subject {
            Some(Subject::Value(x)) if &x == s => Some(e.id),
            _ => None,
        }))
}

async fn hold(n: &common::ClusterNode, s: &Segment) -> Result<()> {
    let mut m = Mutations::new();
    m.put_value_ref(
        s.pk,
        &TenantId::default(),
        &s.owner,
        &Holder::State(Bytes::from_static(b"messages")),
        &s.sha,
        s.gen,
        1,
    );
    let range = n.node.cluster.partition_of_key(s.pk);
    let h = n.node.partitions.get(range).expect("range 0 has an engine");
    h.raw_commit(s.pk, m).await?;
    Ok(())
}

/// Wait for `cond` (polled every 50 ms, capped at [`DEADLINE`]).
async fn eventually<F, Fut>(what: &str, mut cond: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    common::within(DEADLINE, what, async {
        loop {
            if cond().await? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
}

/// **Orphan ⇒ hold ⇒ delete.** Hold time 1 s.
///
/// 1. PUT a segment without a holder.
/// 2. The sweep (100 ms, `min_age` 0) writes an in-place hold entry.
/// 3. Past `keep` ⇒ really deleted: the object is gone, the entry is gone.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_segment_is_held_then_purged_after_keep() -> Result<()> {
    let bucket = common::bucket();
    let n = start(&bucket, Duration::from_secs(1), Duration::ZERO).await?;
    let (values, trash) = stores(&bucket);
    let s = seg(1);
    values.put(&s, Bytes::from_static(b"\x01")).await?;
    eventually("in-place hold entry", || async {
        Ok(entry_of(&trash, &s).await?.is_some())
    })
    .await?;
    ensure!(values.head(&s).await?.is_some(), "held: object still there");
    eventually("real delete after keep", || async {
        Ok(values.head(&s).await?.is_none() && entry_of(&trash, &s).await?.is_none())
    })
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// **A holder remains ⇒ no delete; a wrong hold ⇒ the entry is removed
/// (SM-16).** Hold time 0 — any entry that survives a round has been
/// examined.
///
/// 1. PUT segments A and B; a holder row `State(messages)` for A.
/// 2. Write an in-place hold entry for A by hand (a mistaken delete:
///    `DeleteProof::assume`).
/// 3. The sweep: A still has a holder ⇒ the entry is removed (restored), the
///    object A remains. B is an orphan ⇒ deleted (control: the sweep did run).
/// 4. A few more rounds: A never gets an entry again.
#[tokio::test(flavor = "multi_thread")]
async fn held_segment_is_never_deleted_and_a_wrong_hold_is_released() -> Result<()> {
    let bucket = common::bucket();
    let n = start(&bucket, Duration::ZERO, Duration::ZERO).await?;
    let (values, trash) = stores(&bucket);
    let (a, b) = (
        seg(2),
        Segment {
            owner: Owner([2; 32]),
            ..seg(3)
        },
    );
    hold(&n, &a).await?;
    values.put(&a, Bytes::from_static(b"\x02")).await?;
    values.put(&b, Bytes::from_static(b"\x03")).await?;
    trash
        .hold_in_place(
            &EntryId::new(cairn_store::entropy::now_ms()),
            &DeleteProof::assume(Subject::Value(a.clone())),
            "test: mistaken hold of a segment that has a holder",
        )
        .await?;
    eventually("orphan B deleted", || async {
        Ok(values.head(&b).await?.is_none())
    })
    .await?;
    eventually("A's mistaken hold entry removed", || async {
        Ok(entry_of(&trash, &a).await?.is_none())
    })
    .await?;
    ensure!(
        values.head(&a).await?.is_some(),
        "A has a holder: the object remains"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    ensure!(
        entry_of(&trash, &a).await?.is_none() && values.head(&a).await?.is_some(),
        "A is never held again or deleted"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **Younger than `min_age` ⇒ untouched** (the PUT → commit gap, AP-1).
#[tokio::test(flavor = "multi_thread")]
async fn young_segment_is_left_alone() -> Result<()> {
    let bucket = common::bucket();
    let n = start(&bucket, Duration::ZERO, Duration::from_secs(3600)).await?;
    let (values, trash) = stores(&bucket);
    let s = seg(4);
    values.put(&s, Bytes::from_static(b"\x04")).await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    ensure!(
        entry_of(&trash, &s).await?.is_none(),
        "a young segment has no entry"
    );
    ensure!(values.head(&s).await?.is_some(), "young segment remains");
    n.node.shutdown().await;
    // A young segment without a holder: `verify` sees an orphan that is not
    // old enough yet, which it does not report, so there is nothing to allow.
    Ok(())
}

/// **`verify` (n): a holder whose object is missing is dangling, healed by no
/// pass.**
///
/// Must see: `value_dangling` with `heals = None` naming the segment path.
#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_a_holder_whose_object_is_missing() -> Result<()> {
    let bucket = common::bucket();
    let n = start(&bucket, Duration::from_secs(86_400), Duration::ZERO).await?;
    let s = seg(5);
    hold(&n, &s).await?;
    n.node.shutdown().await;
    let found = bucket.verify().await?;
    ensure!(
        found.iter().any(|f| f.check == "value_dangling"
            && f.heals.is_none()
            && f.detail.contains(&s.owner.hex())),
        "verify must see the dangling ref: {found:?}"
    );
    bucket.allow(
        "value_dangling",
        "the test leaves a holder without an object on purpose",
    );
    Ok(())
}

/// **`fork` copies live segments and shares no ref**: the copy has its own
/// objects under its prefix; a segment that already has an in-place hold entry
/// is not copied.
#[tokio::test(flavor = "multi_thread")]
async fn fork_copies_live_segments_and_skips_held_ones() -> Result<()> {
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let (values, trash) = stores(&bucket);
    let (live, held) = (seg(6), seg(7));
    hold(&n, &live).await?;
    values.put(&live, Bytes::from_static(b"\x06")).await?;
    values.put(&held, Bytes::from_static(b"\x07")).await?;
    trash
        .hold_in_place(
            &EntryId::new(cairn_store::entropy::now_ms()),
            &DeleteProof::assume(Subject::Value(held.clone())),
            "test: segment awaiting deletion",
        )
        .await?;
    n.node.shutdown().await;
    let ctl = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store((*bucket).clone(), "", "memory://"),
    ));
    let rep = ctl
        .fork("fork1", "test@values", "cairn-ctl fork --to fork1")
        .await?;
    ensure!(rep.values == 1, "exactly one segment copied: {rep:?}");
    let copy = ValueStore::new((*bucket).clone(), &Path::from("fork1"));
    ensure!(
        copy.head(&live).await?.is_some(),
        "the live segment is in the copy"
    );
    ensure!(
        copy.head(&held).await?.is_none(),
        "a held segment is not copied"
    );
    ensure!(values.head(&live).await?.is_some(), "source untouched");
    bucket.allow(
        "unlisted_object",
        "the fork's copy lives under prefix fork1/ of the same bucket — the source's verify does not know that prefix",
    );
    Ok(())
}

/// A JSON string `"xxxx…"` `n` bytes long (both quotes included).
fn json_blob(n: usize, c: u8) -> Bytes {
    let mut v = vec![c; n];
    v[0] = b'"';
    v[n - 1] = b'"';
    Bytes::from(v)
}

/// Every object under the bucket's `values/`.
async fn value_objects(bucket: &common::Bucket) -> Result<Vec<String>> {
    use futures::TryStreamExt;
    let listed: Vec<_> = bucket
        .list(Some(&Path::from("values")))
        .try_collect()
        .await?;
    Ok(listed.into_iter().map(|m| m.location.to_string()).collect())
}

/// **Chat history takes the segmented path, end to end** (the
/// `values/segmented` gate is on from the start in a new cluster).
///
/// 1. `send1`: `ctx.set("messages", H)`, H = 600 KiB ≥ `seal_bytes` (256 KiB)
///    ⇒ the shell seals all of H into one segment; the journal carries only
///    the ref.
/// 2. `send2`: `ctx.set("messages", H ‖ 10 KiB)` ⇒ the prefix matches the old
///    segment's sha and is reused; the remaining 10 KiB is the tail (below the
///    threshold) ⇒ **no** new segment.
/// 3. `read`: a lazy `ctx.get("messages")` ⇒ the engine returns ref + tail, the
///    shell assembles ⇒ the SDK (fake, wire ≤ v5: a completion carrying
///    `segments` is a 400) receives the whole value.
///
/// Must see: a single segment object; `read` returns exactly `H ‖ 10 KiB`;
/// `sys_state.value_len` = the logical length; `verify` is clean at cleanup.
#[tokio::test(flavor = "multi_thread")]
async fn chat_history_is_sealed_reused_and_read_back() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'a');
    let mut longer = h[..h.len() - 1].to_vec();
    longer.extend(std::iter::repeat_n(b'b', 10 << 10));
    longer.push(b'"');
    let longer = Bytes::from(longer);
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "send1",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: h.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "send2",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: longer.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    for handler in ["send1", "send2"] {
        let (code, v) = common::invoke(&url, "Chat", handler, Some("c1"), "{}").await?;
        ensure!(code == 200, "{handler}: {code} {v}");
        let objects = value_objects(&bucket).await?;
        ensure!(
            objects.len() == 1,
            "{handler}: one segment, reused in send2: {objects:?}"
        );
    }
    let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
    ensure!(code == 200, "read: {code} {v}");
    ensure!(
        v["output"].as_str() == std::str::from_utf8(&longer).ok(),
        "read returns the whole assembled value ({} bytes): {}",
        longer.len(),
        v.to_string().chars().take(200).collect::<String>()
    );
    let resp = reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&serde_json::json!({
            "sql": "SELECT value_len FROM sys_state WHERE service = 'Chat' AND key = 'c1' AND state_key = 'messages'"
        }))
        .send()
        .await?;
    let code = resp.status().as_u16();
    let v: serde_json::Value = resp.json().await?;
    ensure!(
        code == 200 && v["rows"][0][0].as_u64() == Some(longer.len() as u64),
        "sys_state logical length: {code} {v}"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **A wire v6 SDK reads segments through a `proxy` URL and reuses refs** (node
/// `--value-read-path proxy`, base URL = the bound ingress address).
///
/// 1. `send1` (the SDK declares v6): `ctx.set("messages", H)`, H = 600 KiB
///    inline ⇒ the shell seals one segment as in the previous test.
/// 2. `send2`: a lazy `ctx.get` ⇒ the completion carries ref + tail (**not**
///    assembled), the read URL is in `Start.segment_urls`; the SDK GETs through
///    `/values/v1/{token}` and checks the sha; then `ctx.set(H ‖ 10 KiB)` sends
///    the reused ref + tail.
/// 3. `read`: `ctx.get` + return the value ⇒ the whole `H ‖ 10 KiB`.
/// 4. `forge` (another key): a `SetState` carrying an invented ref ⇒ the engine
///    refuses, no state is written.
///
/// Must see: every request of `send2` is under 64 KiB in both directions, and
/// so is the request down to the SDK for `read` (the node does not carry H);
/// still one segment; `read` returns the right value; a fake token ⇒ `403`;
/// the `forge` key has no state.
#[tokio::test(flavor = "multi_thread")]
async fn v6_sdk_reads_segments_through_proxy_and_reuses_refs() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'a');
    let mut longer = h[..h.len() - 1].to_vec();
    longer.extend(std::iter::repeat_n(b'b', 10 << 10));
    longer.push(b'"');
    let longer = Bytes::from(longer);
    let key = || "messages".to_string();
    let sdk = FakeSdk::builder()
        .wire(6)
        .object(
            "Chat",
            "send1",
            vec![
                Step::SetState {
                    key: key(),
                    value: h.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "send2",
            vec![
                Step::GetState { key: key() },
                Step::SetStateReuse {
                    key: key(),
                    value: longer.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![Step::GetState { key: key() }, Step::EchoCallResult],
        )
        .object(
            "Chat",
            "forge",
            vec![
                Step::SetStateForged {
                    key: key(),
                    value: Bytes::from_static(b"\"x\""),
                },
                Step::output("ok"),
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 1, Duration::from_secs(60), |cfg| {
        fast(Duration::from_secs(86_400), Duration::from_secs(3600))(cfg);
        cfg.values.read_path = cairn_server::value_urls::ReadPath::Proxy;
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    for handler in ["send1", "send2"] {
        let (code, v) = common::invoke(&url, "Chat", handler, Some("c1"), "{}").await?;
        ensure!(code == 200, "{handler}: {code} {v}");
    }
    let objects = value_objects(&bucket).await?;
    ensure!(
        objects.len() == 1,
        "one segment, reused in send2: {objects:?}"
    );
    const SMALL: usize = 64 << 10;
    let send2 = sdk.traffic_of("send2");
    ensure!(
        send2.len() >= 2 && send2.iter().all(|&(d, u)| d < SMALL && u < SMALL),
        "send2 does not carry H through the node: {send2:?}"
    );
    let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
    ensure!(code == 200, "read: {code} {v}");
    ensure!(
        v["output"].as_str() == std::str::from_utf8(&longer).ok(),
        "read returns the whole value ({} bytes): {}",
        longer.len(),
        v.to_string().chars().take(200).collect::<String>()
    );
    let read = sdk.traffic_of("read");
    ensure!(
        !read.is_empty() && read.iter().all(|&(d, _)| d < SMALL),
        "the node does not assemble H down to a v6 SDK: {read:?}"
    );

    // A fake token: one body for every error, no API key needed.
    let resp = reqwest::get(format!("{url}/values/v1/bogus")).await?;
    ensure!(resp.status() == 403, "fake token: {}", resp.status());

    // An invented ref: the turn fails (retried forever), nothing written for key `c2`.
    let (code, v) = common::send(&url, "Chat", "forge", Some("c2"), "{}").await?;
    ensure!((200..300).contains(&code), "forge/send: {code} {v}");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let resp = reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&serde_json::json!({
            "sql": "SELECT count(*) FROM sys_state WHERE service = 'Chat' AND key = 'c2'"
        }))
        .send()
        .await?;
    let v: serde_json::Value = resp.json().await?;
    ensure!(
        v["rows"][0][0].as_u64() == Some(0),
        "an invented ref writes no state: {v}"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **A large `Output` goes segmented, ingress assembles.**
///
/// 1. `Chat/messages` (keyed) and `Gen/make` (unkeyed) return 600 KiB ≥
///    `seal_bytes` ⇒ the shell seals the `Output`: under the object's owner,
///    or under the id itself when the service is unkeyed. The `completed`
///    status row carries ref + tail.
/// 2. `/invoke` and `GET /invocations/{id}` assemble outside the engine.
///
/// Must see: both calls `200` with the whole output, `GET` returns the same
/// output, two segment objects; `verify` is clean at cleanup.
#[tokio::test(flavor = "multi_thread")]
async fn large_output_is_sealed_and_assembled_by_ingress() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'o');
    let sdk = FakeSdk::builder()
        .object("Chat", "messages", vec![Step::Output(h.clone())])
        .service("Gen", "make", vec![Step::Output(h.clone())])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let want = std::str::from_utf8(&h)?;
    for (service, handler, key) in [("Chat", "messages", Some("c1")), ("Gen", "make", None)] {
        let (code, v) = common::invoke(&url, service, handler, key, "{}").await?;
        ensure!(
            code == 200 && v["output"].as_str() == Some(want),
            "{service}/{handler}: {code} {}",
            v.to_string().chars().take(200).collect::<String>()
        );
        let id = v["id"].as_str().unwrap_or_default().to_string();
        let (code, s) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && s["output"].as_str() == Some(want),
            "GET {service}/{handler}: {code} {}",
            s.to_string().chars().take(200).collect::<String>()
        );
    }
    let objects = value_objects(&bucket).await?;
    ensure!(objects.len() == 2, "one segment per output: {objects:?}");
    n.node.shutdown().await;
    Ok(())
}

/// **A segmented output reaches its caller, on the same range and on another
/// range** (a tenant without a cap ⇒ delivered inline).
///
/// 1. A lone node holds 2 ranges. `Front/go[f]` calls `Chat/messages[c]` —
///    once with `c` on the same range as `f`, once on another range.
/// 2. `messages` returns 600 KiB ⇒ the `Output` is sealed; the outbox `Reply`
///    row carries the ref + the source owner; the shell delivers (same range:
///    the drainer; other range: a `Completion` message, then the destination
///    range's shell) — `len ≤ max-payload` (no cap) ⇒ assembled inline, the
///    caller's journal holds the bytes as before.
///
/// Must see: both `go`s return the whole 600 KiB (an SDK ≤ v5 answers a
/// completion carrying segments with a 400 — so a right answer means the shell
/// assembled); four segment objects (two outputs of `messages`, two of `go`),
/// no COPY.
#[tokio::test(flavor = "multi_thread")]
async fn segmented_call_output_reaches_the_caller_on_both_ranges() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'c');
    let call = |key: &str| Step::Call {
        service: "Chat".into(),
        handler: "messages".into(),
        key: Some(key.into()),
        idempotency_key: None,
        spread: false,
    };
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 2, Duration::from_secs(60), |cfg| {
        fast(Duration::from_secs(86_400), Duration::from_secs(3600))(cfg)
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads both ranges", |p| {
        [0, 1]
            .iter()
            .all(|r| p.get(r).is_some_and(|v| v == &vec![0]))
    })
    .await?;
    let range_of = |service: &str, key: &str| {
        let (pk, _) =
            cairn_store::value::object_owner(&TenantId::default(), None, service, key).unwrap();
        n.node.cluster.partition_of_key(pk)
    };
    let home = range_of("Front", "f");
    let same = (0..200)
        .map(|i| format!("s{i}"))
        .find(|k| range_of("Chat", k) == home)
        .expect("a key on the same range");
    let other = (0..200)
        .map(|i| format!("o{i}"))
        .find(|k| range_of("Chat", k) != home)
        .expect("a key on another range");
    let sdk = FakeSdk::builder()
        .object("Chat", "messages", vec![Step::Output(h.clone())])
        .object("Front", "same", vec![call(&same), Step::EchoCallResult])
        .object("Front", "other", vec![call(&other), Step::EchoCallResult])
        .spawn()
        .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let want = std::str::from_utf8(&h)?;
    for handler in ["same", "other"] {
        let (code, v) = common::invoke(&url, "Front", handler, Some("f"), "{}").await?;
        ensure!(
            code == 200 && v["output"].as_str() == Some(want),
            "Front/{handler}: {code} {}",
            v.to_string().chars().take(200).collect::<String>()
        );
    }
    // Two outputs of `messages` and two outputs of `go` (600 KiB, sealed under
    // their own owner): inline delivery copies no segment.
    let objects = value_objects(&bucket).await?;
    ensure!(objects.len() == 4, "4 outputs, 4 segments: {objects:?}");
    n.node.shutdown().await;
    Ok(())
}

/// How many segments the `State(messages)` holder of `service[key]` holds,
/// through `sys_value`.
async fn state_segments(url: &str, service: &str, key: &str) -> Result<u64> {
    let resp = reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&serde_json::json!({
            "sql": format!(
                "SELECT count(*) FROM sys_value WHERE service = '{service}' AND key = '{key}' \
                 AND holder_kind = 'state' AND holder = 'messages'"
            )
        }))
        .send()
        .await?;
    let code = resp.status().as_u16();
    let v: serde_json::Value = resp.json().await?;
    ensure!(code == 200, "sys_value: {code} {v}");
    v["rows"][0][0]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("sys_value has no number: {v}"))
}

/// **An appended history keeps few segments: merging on the write path.**
///
/// Two deployments on one node with `--value-read-path proxy`:
/// - `Chat5` (fake SDK, wire ≤ v5): each turn `get`s and then
///   `set(old ‖ 300 KiB)` — the whole value goes up, the shell compares the
///   prefix by sha and then seals the new 300 KiB.
/// - `Chat6` (wire v6): `get` and then **two** appending `set`s in the same
///   turn (300 KiB, then 1 KiB); the second reuses the `refs[0..k]` the SDK
///   remembered after the first — after a merge the row no longer starts with
///   them.
///
/// 20 turns per deployment. Without merging: 20 segments. Merging with base 4:
/// the segment count is the sum of the base-4 digits of the number of seals.
///
/// Must see: every turn `200`; `State(messages)` holds ≤ 6 segments in both;
/// the last read returns exactly the whole history (20 × 300 KiB, plus
/// 20 × 1 KiB for `Chat6`).
#[tokio::test(flavor = "multi_thread")]
async fn appended_history_keeps_few_segments() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    const TURNS: usize = 20;
    let chunk = Bytes::from(vec![b'c'; 300 << 10]);
    let small = Bytes::from(vec![b's'; 1 << 10]);
    let key = || "messages".to_string();
    let read = || vec![Step::GetState { key: key() }, Step::EchoCallResult];
    let v5 = FakeSdk::builder()
        .object(
            "Chat5",
            "append",
            vec![
                Step::GetState { key: key() },
                Step::AppendState {
                    key: key(),
                    chunk: chunk.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object("Chat5", "read", read())
        .spawn()
        .await?;
    let v6 = FakeSdk::builder()
        .wire(6)
        .object(
            "Chat6",
            "append",
            vec![
                Step::GetState { key: key() },
                Step::AppendState {
                    key: key(),
                    chunk: chunk.clone(),
                },
                Step::AppendState {
                    key: key(),
                    chunk: small.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object("Chat6", "read", read())
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 1, Duration::from_secs(60), |cfg| {
        fast(Duration::from_secs(86_400), Duration::from_secs(3600))(cfg);
        cfg.values.read_path = cairn_server::value_urls::ReadPath::Proxy;
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    common::register(&url, &v5.url).await?;
    common::register(&url, &v6.url).await?;
    for (service, per_turn) in [("Chat5", vec![&chunk]), ("Chat6", vec![&chunk, &small])] {
        let mut want: Vec<u8> = Vec::new();
        for i in 0..TURNS {
            let (code, v) = common::invoke(&url, service, "append", Some("c1"), "{}").await?;
            ensure!(code == 200, "{service} turn {i}: {code} {v}");
            for c in &per_turn {
                want.extend_from_slice(c);
            }
        }
        let segs = state_segments(&url, service, "c1").await?;
        ensure!(segs <= 6, "{service}: {segs} segments after {TURNS} turns");
        let (code, v) = common::invoke(&url, service, "read", Some("c1"), "{}").await?;
        ensure!(code == 200, "{service} read: {code}");
        ensure!(
            v["output"].as_str().map(str::as_bytes) == Some(&want[..]),
            "{service}: read back all {} bytes",
            want.len()
        );
    }
    n.node.shutdown().await;
    Ok(())
}

/// `SELECT … FROM /query` returns the first column of the first row as a number.
async fn query_u64(url: &str, sql: &str) -> Result<u64> {
    let resp = reqwest::Client::new()
        .post(format!("{url}/query"))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await?;
    let code = resp.status().as_u16();
    let v: serde_json::Value = resp.json().await?;
    ensure!(code == 200, "{sql}: {code} {v}");
    v["rows"][0][0]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("{sql}: no number: {v}"))
}

/// **Journal bytes per turn do not depend on H.**
///
/// Three chat objects on one node (default tenant, `seal_bytes` = 256 KiB, a
/// fake SDK on wire ≤ v5 sending whole values): histories of H = 1, 3 and
/// 6 MiB built with one `seed` turn, then each object runs the same chain of
/// chat turns — a lazy `ctx.get`, `ctx.set(old ‖ 16 KiB)`. The `seed` turn
/// seals all of H except `TAIL_RESERVE`, so all three start at the same phase
/// of the tail sawtooth.
///
/// The journal bytes of a turn = `sum(payload_len)` of `sys_journal` for that
/// invocation — journal rows read straight from the range's `RangeStore`. Each
/// turn has two entries carrying a value (the `GetState` completion and
/// `SetState`), each entry carries only ref + tail, tail < `seal_bytes` + the
/// new bytes.
///
/// Must see: every turn `200`; journal bytes per turn ≤ 2 × 256 KiB + 16 KiB +
/// 4 KiB (`Input`/`Output` entries, protobuf framing, refs); the per-turn byte
/// series of the three H differ by ≤ 1 KiB on every turn (only the ref indexes
/// differ); the last read returns exactly the whole history.
#[tokio::test(flavor = "multi_thread")]
async fn journal_bytes_per_turn_do_not_grow_with_history() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    const SEAL: u64 = 256 << 10;
    const NEW: usize = 16 << 10;
    const OVERHEAD: u64 = 4 << 10;
    const TURNS: usize = 20;
    let sizes: [usize; 3] = [1 << 20, 3 << 20, 6 << 20];
    let key = || "messages".to_string();
    let chunk = Bytes::from(vec![b'n'; NEW]);
    let mut b = FakeSdk::builder()
        .object(
            "Chat",
            "send",
            vec![
                Step::GetState { key: key() },
                Step::AppendState {
                    key: key(),
                    chunk: chunk.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![Step::GetState { key: key() }, Step::EchoCallResult],
        );
    for h in sizes {
        b = b.object(
            "Chat",
            &format!("seed{}", h >> 20),
            vec![
                Step::AppendState {
                    key: key(),
                    chunk: Bytes::from(vec![b'h'; h]),
                },
                Step::output("ok"),
            ],
        );
    }
    let sdk = b.spawn().await?;
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let mut per_h: Vec<Vec<u64>> = Vec::new();
    for h in sizes {
        let obj = format!("c{}", h >> 20);
        let (code, v) =
            common::invoke(&url, "Chat", &format!("seed{}", h >> 20), Some(&obj), "{}").await?;
        ensure!(code == 200, "seed {h}: {code} {v}");
        let mut turns = Vec::new();
        for i in 0..TURNS {
            let (code, v) = common::invoke(&url, "Chat", "send", Some(&obj), "{}").await?;
            ensure!(code == 200, "H={h} turn {i}: {code} {v}");
            let id = v["id"].as_str().unwrap_or_default().to_string();
            ensure!(!id.is_empty(), "the turn has no id: {v}");
            let bytes = query_u64(
                &url,
                &format!("SELECT sum(payload_len) FROM sys_journal WHERE id = '{id}'"),
            )
            .await?;
            ensure!(
                bytes <= 2 * SEAL + NEW as u64 + OVERHEAD,
                "H={h} turn {i}: {bytes} journal bytes > 2 × {SEAL} + {NEW} + {OVERHEAD}"
            );
            turns.push(bytes);
        }
        let (code, v) = common::invoke(&url, "Chat", "read", Some(&obj), "{}").await?;
        ensure!(code == 200, "read H={h}: {code}");
        let got = v["output"].as_str().map(str::len).unwrap_or_default();
        ensure!(
            got == h + TURNS * NEW,
            "H={h}: read back {got} bytes, want {}",
            h + TURNS * NEW
        );
        per_h.push(turns);
    }
    for i in 0..TURNS {
        let col: Vec<u64> = per_h.iter().map(|t| t[i]).collect();
        let (lo, hi) = (
            *col.iter().min().expect("ba H"),
            *col.iter().max().expect("ba H"),
        );
        ensure!(
            hi - lo <= 1 << 10,
            "turn {i}: journal bytes depend on H: {col:?} (H = {sizes:?})"
        );
    }
    let mean: Vec<u64> = per_h
        .iter()
        .map(|t| t.iter().sum::<u64>() / TURNS as u64)
        .collect();
    eprintln!("journal bytes per turn, mean per H {sizes:?}: {mean:?}; per turn: {per_h:?}");
    n.node.shutdown().await;
    Ok(())
}

/// **SM-16 for segments: a live segment is held by mistake, then restored with
/// `cairn-ctl trash restore`.**
///
/// The values sweep is off (a very long `--value-sweep-every-s`) so that only
/// the operator restores — the sweep removing a mistaken hold entry by itself
/// has its own test (`held_segment_is_never_deleted_and_a_wrong_hold_is_released`).
///
/// 1. `send` (fake SDK ≤ v5): `ctx.set("messages", H)`, H = 600 KiB ⇒ one
///    segment that still has the holder `State(messages)`.
/// 2. Mistaken delete: write an in-place hold entry for that segment
///    (`DeleteProof::assume`, like a broken sweep).
/// 3. While held: a read still returns the whole H (holding in place neither
///    copies nor deletes).
/// 4. `trash list` shows the `value …` entry; `trash restore <id>` as a dry run
///    writes nothing, then for real.
///
/// Must see: the entry disappears, the segment object keeps exactly its bytes
/// (the sha matches), a read after the restore returns the whole H, `verify` is
/// clean at cleanup.
#[tokio::test(flavor = "multi_thread")]
async fn live_segment_held_by_mistake_is_restored_by_ctl() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'a');
    let sdk = FakeSdk::builder()
        .object(
            "Chat",
            "send",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: h.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            "Chat",
            "read",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 1, Duration::from_secs(60), |cfg| {
        cfg.value_sweep = Some(Duration::from_secs(3600));
        cfg.value_min_age = Duration::ZERO;
        cfg.trash_keep = Some(Duration::ZERO);
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, v) = common::invoke(&url, "Chat", "send", Some("c1"), "{}").await?;
    ensure!(code == 200, "send: {code} {v}");
    let objects = value_objects(&bucket).await?;
    ensure!(objects.len() == 1, "one segment: {objects:?}");
    let (values, trash) = stores(&bucket);
    let seg = values
        .segment_of(&Path::from(objects[0].as_str()))
        .ok_or_else(|| anyhow::anyhow!("cannot read a segment from {}", objects[0]))?;
    let before = values.get(&seg).await?.expect("the segment exists");
    // 2.
    trash
        .hold_in_place(
            &EntryId::new(cairn_store::entropy::now_ms()),
            &DeleteProof::assume(Subject::Value(seg.clone())),
            "test: a broken sweep held a segment that still has a holder",
        )
        .await?;
    let id = entry_of(&trash, &seg)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no hold entry yet"))?;
    // 3.
    let read = || async {
        let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
        ensure!(code == 200, "read: {code}");
        ensure!(
            v["output"].as_str().map(str::as_bytes) == Some(&h[..]),
            "read returns all {} bytes",
            h.len()
        );
        Ok::<_, anyhow::Error>(())
    };
    read().await?;
    // 4.
    let store: Arc<dyn ObjectStore> = (*bucket).clone();
    let ctl = cairn_server::ctl::Ctl::new(cairn_store::ObjectStores::single(
        cairn_store::StoreLocation::from_store(store, "", "memory://"),
    ));
    let listed = ctl.trash_list(cairn_store::entropy::now_ms()).await?;
    ensure!(
        listed
            .iter()
            .any(|e| e.id == id.as_str()
                && e.subject.as_deref().is_some_and(|s| s.starts_with("value"))),
        "trash list shows the segment entry: {listed:?}"
    );
    let opts = |apply| cairn_server::ctl::RepairOptions {
        apply,
        force: false,
        who: "test@values".into(),
        command: "cairn-ctl trash restore".into(),
        expect_version: None,
        now_ms: cairn_store::entropy::now_ms(),
    };
    let target = cairn_server::ctl::TrashTarget::Entry(id.as_str().to_string());
    let dry = ctl.trash_restore(&target, &opts(false)).await?;
    ensure!(!dry.applied, "{dry:?}");
    ensure!(
        entry_of(&trash, &seg).await?.is_some(),
        "the dry run removed the entry"
    );
    let done = ctl.trash_restore(&target, &opts(true)).await?;
    ensure!(done.applied, "{done:?}");
    ensure!(
        entry_of(&trash, &seg).await?.is_none(),
        "the entry remains after the restore"
    );
    let after = values.get(&seg).await?.expect("segment after restore");
    ensure!(
        after == before && cairn_store::artifact::sha256(&after) == seg.sha,
        "the segment bytes changed"
    );
    read().await?;
    n.node.shutdown().await;
    Ok(())
}

/// `POST /invocations/{id}/{op}` with body `body`; returns the HTTP code.
async fn post_op(url: &str, id: &str, op: &str, body: serde_json::Value) -> Result<u16> {
    Ok(reqwest::Client::new()
        .post(format!("{url}/invocations/{id}/{op}"))
        .json(&body)
        .send()
        .await?
        .status()
        .as_u16())
}

/// Wakes the fake SDK's `Step::Suspend`.
async fn release(url: &str, id: &str) -> Result<()> {
    let code = post_op(
        url,
        id,
        "signal",
        serde_json::json!({ "signal": common::fake_sdk::HOLD_SIGNAL }),
    )
    .await?;
    ensure!(code == 200, "signal {id}: {code}");
    Ok(())
}

async fn purge(url: &str, id: &str) -> Result<()> {
    let code = post_op(url, id, "purge", serde_json::json!({})).await?;
    ensure!((200..300).contains(&code), "purge {id}: {code}");
    Ok(())
}

/// The segments (by path) under the bucket's `values/`.
async fn segment_set(bucket: &common::Bucket) -> Result<std::collections::BTreeSet<String>> {
    Ok(value_objects(bucket).await?.into_iter().collect())
}

/// **Interleaving: a SHARED handler reads the old copy while an EXCLUSIVE one
/// writes a new copy and the old invocation is purged.**
///
/// Order:
/// 1. `send` (EXCLUSIVE, fake SDK ≤ v5): `ctx.set(messages, A)`, A = 600 KiB
///    of `a` ⇒ segment SA, holders `State(messages)` and `Invocation(send)`.
/// 2. `peek` (SHARED): a lazy `ctx.get` ⇒ the completion journals SA's ref
///    (holder `Invocation(peek)`), then hangs waiting for a signal.
/// 3. The test slips into the gap between read and return: `rewrite`
///    (EXCLUSIVE) writes B = 600 KiB of `b` (no shared prefix ⇒ SA leaves the
///    current row, a new segment SB), then purges `send` — the only remaining
///    holder of SA is `peek`'s journal.
/// 4. The values sweep runs several times (100 ms, `min_age` 1 s).
/// 5. Signal `peek`: the new turn replays the journal — the shell assembles SA
///    from the journaled ref.
/// 6. Purge `peek` ⇒ SA has no holder left.
///
/// Must see: in step 4 SA remains (`peek`'s journal holds it); `peek` is
/// `completed` with the whole A (the copy it read, not B); a `GET` of the key
/// returns B; after step 6 the sweep deletes SA and SB remains; `verify` is
/// clean at cleanup.
#[tokio::test(flavor = "multi_thread")]
async fn shared_reader_keeps_its_segment_while_exclusive_rewrites_and_purges() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let a = json_blob(600 << 10, b'a');
    let b = json_blob(600 << 10, b'b');
    let set = |v: &Bytes| {
        vec![
            Step::SetState {
                key: "messages".into(),
                value: v.clone(),
            },
            Step::output("ok"),
        ]
    };
    let sdk = FakeSdk::builder()
        .object("Chat", "send", set(&a))
        .object("Chat", "rewrite", set(&b))
        .shared_object(
            "Chat",
            "peek",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::Suspend,
                Step::EchoCallResult,
            ],
        )
        .shared_object(
            "Chat",
            "read",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(&bucket, Duration::ZERO, Duration::from_secs(1)).await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    // 1.
    let (code, v) = common::invoke(&url, "Chat", "send", Some("c1"), "{}").await?;
    ensure!(code == 200, "send: {code} {v}");
    let send_id = v["id"].as_str().unwrap_or_default().to_string();
    let sa = segment_set(&bucket).await?;
    ensure!(sa.len() == 1, "one segment SA: {sa:?}");
    // 2.
    let (code, v) = common::send(&url, "Chat", "peek", Some("c1"), "{}").await?;
    ensure!((200..300).contains(&code), "peek: {code} {v}");
    let peek_id = v["id"].as_str().unwrap_or_default().to_string();
    common::wait_status(&url, &peek_id, "suspended").await?;
    // 3.
    let (code, v) = common::invoke(&url, "Chat", "rewrite", Some("c1"), "{}").await?;
    ensure!(code == 200, "rewrite: {code} {v}");
    purge(&url, &send_id).await?;
    let both = segment_set(&bucket).await?;
    ensure!(both.len() == 2 && sa.is_subset(&both), "SA, SB: {both:?}");
    // 4. Past `min_age` and a few sweep rounds (100 ms).
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ensure!(
        sa.is_subset(&segment_set(&bucket).await?),
        "the sweep deleted SA while peek's journal still holds it"
    );
    // 5.
    release(&url, &peek_id).await?;
    let st = common::wait_status(&url, &peek_id, "completed").await?;
    let (code, s) = common::invocation(&url, &peek_id).await?;
    ensure!(
        code == 200 && s["output"].as_str().map(str::as_bytes) == Some(&a[..]),
        "peek returns the copy it read (A): {code} {}",
        st.to_string().chars().take(200).collect::<String>()
    );
    let (code, v) = common::invoke(&url, "Chat", "read", Some("c1"), "{}").await?;
    ensure!(
        code == 200 && v["output"].as_str().map(str::as_bytes) == Some(&b[..]),
        "the key returns B: {code}"
    );
    // 6. Left: SB (state) and the sealed outputs of `read`, `rewrite`.
    purge(&url, &peek_id).await?;
    eventually(
        "SA is deleted after its last remaining holder is purged",
        || async { Ok(sa.is_disjoint(&segment_set(&bucket).await?)) },
    )
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// **`restart --from` keeps the refs of a segmented journal.**
///
/// 1. `seed`: `ctx.set(messages, A)`, A = 600 KiB ⇒ segment SA.
/// 2. `echo`: a lazy `ctx.get`, then returns the value ⇒ `completed` with A;
///    the journal has a completion carrying SA's ref.
/// 3. `rewrite`: writes B (no shared prefix) ⇒ SA leaves the current row.
/// 4. `restart echo --from <entry count − 1>`: keeps every entry except
///    `Output` ⇒ the new invocation replays the journaled completion.
/// 5. Purge `echo` and `seed`; the sweep runs a few times.
///
/// Must see: the new invocation is `completed` with the whole A (the journal is
/// the source of truth, not the current state); SA remains after step 5 (a
/// holder of the new id); purging the new id ⇒ SA is deleted; `verify` is clean
/// at cleanup.
#[tokio::test(flavor = "multi_thread")]
async fn restart_from_keeps_segmented_journal_entries_readable() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let a = json_blob(600 << 10, b'a');
    let b = json_blob(600 << 10, b'b');
    let set = |v: &Bytes| {
        vec![
            Step::SetState {
                key: "messages".into(),
                value: v.clone(),
            },
            Step::output("ok"),
        ]
    };
    let sdk = FakeSdk::builder()
        .object("Chat", "seed", set(&a))
        .object("Chat", "rewrite", set(&b))
        .object(
            "Chat",
            "echo",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::EchoCallResult,
            ],
        )
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(&bucket, Duration::ZERO, Duration::from_secs(1)).await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, v) = common::invoke(&url, "Chat", "seed", Some("c1"), "{}").await?;
    ensure!(code == 200, "seed: {code} {v}");
    let seed_id = v["id"].as_str().unwrap_or_default().to_string();
    let sa = segment_set(&bucket).await?;
    ensure!(sa.len() == 1, "{sa:?}");
    let (code, v) = common::invoke(&url, "Chat", "echo", Some("c1"), "{}").await?;
    ensure!(
        code == 200 && v["output"].as_str().map(str::as_bytes) == Some(&a[..]),
        "echo: {code}"
    );
    let echo_id = v["id"].as_str().unwrap_or_default().to_string();
    let (code, v) = common::invoke(&url, "Chat", "rewrite", Some("c1"), "{}").await?;
    ensure!(code == 200, "rewrite: {code} {v}");
    let entries = query_u64(
        &url,
        &format!("SELECT count(*) FROM sys_journal WHERE id = '{echo_id}'"),
    )
    .await?;
    let resp = reqwest::Client::new()
        .post(format!(
            "{url}/invocations/{echo_id}/restart?from={}",
            entries - 1
        ))
        .send()
        .await?;
    let code = resp.status().as_u16();
    let r: serde_json::Value = resp.json().await?;
    ensure!(code == 200, "restart: {code} {r}");
    let new_id = r["new"].as_str().unwrap_or_default().to_string();
    common::wait_status(&url, &new_id, "completed").await?;
    let (code, s) = common::invocation(&url, &new_id).await?;
    ensure!(
        code == 200 && s["output"].as_str().map(str::as_bytes) == Some(&a[..]),
        "the restarted copy returns A from the journal: {code}"
    );
    purge(&url, &echo_id).await?;
    purge(&url, &seed_id).await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ensure!(
        sa.is_subset(&segment_set(&bucket).await?),
        "SA deleted while the restarted copy's journal still holds it"
    );
    purge(&url, &new_id).await?;
    eventually("SA deleted after the restarted copy was purged", || async {
        Ok(sa.is_disjoint(&segment_set(&bucket).await?))
    })
    .await?;
    n.node.shutdown().await;
    Ok(())
}

/// **replay-check on a segmented journal.**
///
/// Two deployments on one node with `--value-read-path proxy`, the same turn
/// shape: `ctx.set(messages, A)` (A = 600 KiB) in `seed`, then `wait`: a lazy
/// `ctx.get`, hang waiting for a signal, return the value — `suspended` with a
/// ref + tail completion in the journal.
///
/// - `Chat6` (fake SDK wire v6): the replay check sends ref +
///   `Start.segment_urls`, the SDK fetches the segment through the `proxy` URL
///   as in a real turn.
/// - `Chat5` (wire v5 — the replay check needs ≥ v3): the replay check outside
///   the engine does not assemble yet — refused explicitly
///   (`ReplayError::Segmented`), never sending the tail as the whole value.
///
/// Must see: `Chat6` `checked = matched = 1`, no error; `Chat5` `checked = 1`,
/// `matched = 0`, one error mentioning "segmented"; signal ⇒ both `completed`
/// with A.
#[tokio::test(flavor = "multi_thread")]
async fn replay_check_replays_a_segmented_journal() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let a = json_blob(600 << 10, b'a');
    let steps = |svc: &str, b: common::fake_sdk::FakeSdkBuilder| {
        b.object(
            svc,
            "seed",
            vec![
                Step::SetState {
                    key: "messages".into(),
                    value: a.clone(),
                },
                Step::output("ok"),
            ],
        )
        .object(
            svc,
            "wait",
            vec![
                Step::GetState {
                    key: "messages".into(),
                },
                Step::Suspend,
                Step::EchoCallResult,
            ],
        )
    };
    let v6 = steps("Chat6", FakeSdk::builder().wire(6)).spawn().await?;
    let v5 = steps("Chat5", FakeSdk::builder().wire(5)).spawn().await?;
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 1, Duration::from_secs(60), |cfg| {
        fast(Duration::from_secs(86_400), Duration::from_secs(3600))(cfg);
        cfg.values.read_path = cairn_server::value_urls::ReadPath::Proxy;
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let url = n.node.url();
    for (svc, sdk, matched) in [("Chat6", &v6, 1), ("Chat5", &v5, 0)] {
        let dp = common::register(&url, &sdk.url).await?["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let (code, v) = common::invoke(&url, svc, "seed", Some("c1"), "{}").await?;
        ensure!(code == 200, "{svc} seed: {code} {v}");
        let (code, v) = common::send(&url, svc, "wait", Some("c1"), "{}").await?;
        ensure!((200..300).contains(&code), "{svc} wait: {code} {v}");
        let id = v["id"].as_str().unwrap_or_default().to_string();
        common::wait_status(&url, &id, "suspended").await?;
        let resp = reqwest::Client::new()
            .post(format!("{url}/deployments/{dp}/replay-check?service={svc}"))
            .send()
            .await?;
        let code = resp.status().as_u16();
        let r: serde_json::Value = resp.json().await?;
        ensure!(code == 200, "{svc} replay-check: {code} {r}");
        ensure!(
            r["checked"] == 1 && r["matched"] == matched,
            "{svc} replay-check: {r}"
        );
        let failures = r["failures"].as_array().cloned().unwrap_or_default();
        if matched == 1 {
            ensure!(failures.is_empty(), "{svc}: {r}");
        } else {
            ensure!(
                failures.len() == 1 && failures[0].to_string().contains("segmented"),
                "{svc}: explicit refusal: {r}"
            );
        }
        release(&url, &id).await?;
        common::wait_status(&url, &id, "completed").await?;
        let (code, s) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && s["output"].as_str().map(str::as_bytes) == Some(&a[..]),
            "{svc} wait returns A: {code}"
        );
    }
    n.node.shutdown().await;
    Ok(())
}

/// A single-range node with `--value-read-path proxy`, one 1 MiB `default`
/// segment PUT directly (no holder — exempt from verify (m)), and a `proxy`
/// token valid for 60 s for it. Returns `(bucket, node, token, bytes)`.
async fn proxy_node_with_token() -> Result<(common::Bucket, common::ClusterNode, String, Bytes)> {
    proxy_node_with_token_for("default").await
}

/// Like [`proxy_node_with_token`], with the segment and token of `tenant`.
async fn proxy_node_with_token_for(
    tenant: &str,
) -> Result<(common::Bucket, common::ClusterNode, String, Bytes)> {
    proxy_node_with_segment(tenant, 1 << 20).await
}

/// Like [`proxy_node_with_token_for`], with a segment of `len` bytes.
async fn proxy_node_with_segment(
    tenant: &str,
    len: usize,
) -> Result<(common::Bucket, common::ClusterNode, String, Bytes)> {
    let bucket = common::bucket();
    let n = common::start_cluster_node_tweaked(&bucket, "a", 1, Duration::from_secs(60), |cfg| {
        cfg.values.read_path = cairn_server::value_urls::ReadPath::Proxy;
    })
    .await?;
    common::wait_probe(&[&n], DEADLINE, "a leads range 0", |p| {
        p.get(&0).is_some_and(|v| v == &vec![0])
    })
    .await?;
    let bytes = Bytes::from(vec![b'v'; len]);
    let seg = Segment {
        tenant: tenant.into(),
        pk: PartitionKey(0x10),
        owner: Owner([7; 32]),
        sha: cairn_store::artifact::sha256(&bytes),
        gen: 1,
    };
    let store: Arc<dyn ObjectStore> = (*bucket).clone();
    ValueStore::new(store, &Path::from(""))
        .put(&seg, bytes.clone())
        .await?;
    bucket.allow(
        "value_orphan",
        "the test PUTs a segment without a holder directly, to read it through the proxy",
    );
    let key = cairn_server::value_urls::ProxyKey::from_cluster(&n.node.cluster_secret);
    let token =
        cairn_server::value_urls::Token::seal(&key, &seg, cairn_server::gc::wall_ms() + 60_000);
    Ok((bucket, n, token, bytes))
}

/// **The `proxy` read path has a per-tenant concurrency cap.**
///
/// 1. A valid token (tenant `default`), segment GETs on the store held at a
///    gate.
/// 2. 64 parallel `GET /values/v1/{token}`, no API key.
///
/// Before the fix: every request loaded the whole segment into RAM at once —
/// all 64 stood at the store gate (no `TenantSlot`, no cap), and the node held
/// 64 × the segment.
///
/// Must see: store GETs in flight ≤ 8 (the per-tenant concurrent `proxy` read
/// cap on the node) and the excess requests get `429`; once the gate opens all
/// remaining requests get `200` with the right bytes.
#[tokio::test(flavor = "multi_thread")]
async fn proxy_reads_are_bounded() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, OpSet, PathMatch, Rule};
    let (_bucket, n, token, bytes) = proxy_node_with_token().await?;
    let url = n.node.url();
    let gate = Gate::closed();
    n.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Prefix("values/".into()),
        Action::Gate(gate.clone()),
    ));
    let client = reqwest::Client::new();
    let reqs: Vec<_> = (0..64)
        .map(|_| {
            let (client, u) = (client.clone(), format!("{url}/values/v1/{token}"));
            tokio::spawn(async move {
                let r = client.get(u).send().await?;
                let code = r.status().as_u16();
                Ok::<_, anyhow::Error>((code, r.bytes().await?))
            })
        })
        .collect();
    // Wait until every request either stands at the gate or has an answer (429), with a cap.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let answered = reqs.iter().filter(|r| r.is_finished()).count();
        if gate.blocked() + answered >= 64 || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let in_flight = gate.blocked();
    gate.open();
    let mut ok = 0;
    let mut busy = 0;
    for r in reqs {
        let (code, body) = r.await??;
        match code {
            200 => {
                ensure!(body == bytes, "a 200 carries bytes other than the segment");
                ok += 1;
            }
            429 => busy += 1,
            other => anyhow::bail!("unexpected code {other}"),
        }
    }
    let cap = 8;
    ensure!(
        in_flight <= cap,
        "{in_flight} segment GETs in flight at once for one tenant, cap {cap}"
    );
    ensure!(busy > 0 && ok >= 1, "200: {ok}, 429: {busy}");
    n.node.shutdown().await;
    Ok(())
}

/// **A `proxy` read slot is held until the body is fully sent.** The slot
/// (`TenantSlot`, 8 per tenant per node) used to be released when the handler
/// returned, **before** the body was written to the socket: a slow-reading
/// client kept a copy of the segment in the node's RAM outside the cap — "RSS
/// does not grow with the number of requests" held only for the store read.
///
/// 1. A 16 MiB segment (larger than the loopback socket buffer), a valid token.
/// 2. 16 clients send `GET /values/v1/{token}`, receive the headers, and do
///    **not** read the body.
///
/// Must see: at most 8 requests `200` (a body being sent holds its slot), the
/// rest `429`; reading one request's whole body gives its slot back.
#[tokio::test(flavor = "multi_thread")]
async fn proxy_slot_covers_the_body() -> Result<()> {
    let (_bucket, n, token, bytes) = proxy_node_with_segment("default", 16 << 20).await?;
    let url = format!("{}/values/v1/{token}", n.node.url());
    let mut held = Vec::new();
    let (mut ok, mut busy) = (0, 0);
    for _ in 0..16 {
        // One client per request: one connection per request, nobody reads the body.
        let r = reqwest::Client::new().get(&url).send().await?;
        match r.status().as_u16() {
            200 => {
                ok += 1;
                held.push(r);
            }
            429 => busy += 1,
            other => anyhow::bail!("unexpected code {other}"),
        }
    }
    ensure!(
        ok <= 8 && busy >= 8,
        "{ok} bodies being sent at once for one tenant (cap 8), {busy} 429s"
    );
    let first = held.remove(0).bytes().await?;
    ensure!(first == bytes, "the body has the wrong bytes");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = reqwest::Client::new().get(&url).send().await?;
        if r.status() == 200 {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "a body was read fully but its slot was not given back"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(held);
    n.node.shutdown().await;
    Ok(())
}

/// **A store error does not leak into the error body of an unkeyed route.** The
/// store fails a segment GET ⇒ `503` with a fixed body, carrying no text from
/// the S3 client (the object path).
#[tokio::test(flavor = "multi_thread")]
async fn proxy_store_error_is_opaque() -> Result<()> {
    use cairn_store::testkit::{Action, OpSet, PathMatch, Rule};
    let (_bucket, n, token, _) = proxy_node_with_token().await?;
    let url = n.node.url();
    n.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Prefix("values/".into()),
        Action::Fail { times: None },
    ));
    let r = reqwest::get(format!("{url}/values/v1/{token}")).await?;
    let code = r.status().as_u16();
    let body = r.text().await?;
    ensure!(code == 503, "store error ⇒ 503, got {code}: {body}");
    ensure!(
        !body.contains("values/") && !body.to_lowercase().contains("injected"),
        "the error body leaks store text: {body}"
    );
    n.node.shutdown().await;
    Ok(())
}

/// How many store GETs of `values/…` objects `store` has counted.
fn value_gets(store: &cairn_store::testkit::FaultyStore) -> u64 {
    store
        .counts_by_source_object()
        .into_iter()
        .filter(|(_, op, _, kind, _)| *op == cairn_store::testkit::Op::Get && *kind == "values")
        .map(|(.., n)| n)
        .sum()
}

/// **Re-reading a segmented output does not GET the segment again.**
///
/// 1. `Gen/make` returns 600 KiB ⇒ the output is sealed as one segment.
/// 2. 10 `GET /invocations/{id}` with a read key.
///
/// Before the fix every read built a `ValueStore` without a cache and GOT every
/// segment again (outputs up to 64 MiB, up to 256 GETs per request).
///
/// Must see: every read returns the whole output; the total segment GETs of the
/// 10 reads ≤ the segment count (1).
#[tokio::test(flavor = "multi_thread")]
async fn segmented_output_reads_hit_the_cache() -> Result<()> {
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'o');
    let sdk = FakeSdk::builder()
        .service("Gen", "make", vec![Step::Output(h.clone())])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let want = std::str::from_utf8(&h)?;
    let (code, v) = common::invoke(&url, "Gen", "make", None, "{}").await?;
    ensure!(code == 200, "make: {code}");
    let id = v["id"].as_str().unwrap_or_default().to_string();
    let segments = value_objects(&bucket).await?.len() as u64;
    ensure!(segments == 1, "one segment");
    let before = value_gets(&n.store);
    for _ in 0..10 {
        let (code, s) = common::invocation(&url, &id).await?;
        ensure!(
            code == 200 && s["output"].as_str() == Some(want),
            "GET: {code}"
        );
    }
    let gets = value_gets(&n.store) - before;
    ensure!(
        gets <= segments,
        "10 output reads cost {gets} segment GETs ({segments} segments)"
    );
    n.node.shutdown().await;
    Ok(())
}

/// **A `proxy` token of a tenant being deleted no longer opens segments.** A
/// token lives up to 1 h; before the fix the route did not check the tenant's
/// state, so a token issued before `deleting` could still read until it
/// expired.
///
/// Must see: `200` before the state change; after `PUT /admin/tenants/acme/state
/// {deleting}` (within ≤ 5 s, once the node's state book applies it) `403` with
/// the same body as a broken token.
#[tokio::test(flavor = "multi_thread")]
async fn proxy_token_of_deleting_tenant_is_refused() -> Result<()> {
    let (_bucket, n, token, bytes) = proxy_node_with_token_for("acme").await?;
    let url = n.node.url();
    let r = reqwest::get(format!("{url}/values/v1/{token}")).await?;
    ensure!(r.status() == 200, "before the delete: {}", r.status());
    ensure!(r.bytes().await? == bytes, "byte segment");
    let r = reqwest::Client::new()
        .put(format!("{url}/admin/tenants/acme/state"))
        .json(&serde_json::json!({ "state": "deleting" }))
        .send()
        .await?;
    ensure!(r.status() == 204, "set deleting: {}", r.status());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r = reqwest::get(format!("{url}/values/v1/{token}")).await?;
        let code = r.status().as_u16();
        if code == 403 {
            let body = r.text().await?;
            ensure!(body.contains("invalid or expired"), "body: {body}");
            break;
        }
        ensure!(code == 200, "unexpected code {code}");
        ensure!(
            tokio::time::Instant::now() < deadline,
            "a token of a deleting tenant still reads after 5 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    n.node.shutdown().await;
    Ok(())
}

/// **Assembling a segmented output has a per-tenant concurrency cap.** Each
/// assembly allocates a buffer the size of the output (up to `MAX_ASSEMBLE`,
/// 64 MiB) and GETs every segment; a read key repeating
/// `GET /invocations/{id}` in parallel means N buffers.
///
/// 1. `Gen/make` returns 600 KiB (one segment), sent with `send` — nobody
///    assembles; wait for it to finish through `sys_invocation`. The segment
///    cache is still cold.
/// 2. Segment GETs on the store held at a gate; 16 parallel
///    `GET /invocations/{id}`.
///
/// Must see: assemblies standing at the gate ≤ 4 (the cap per tenant per node),
/// the excess requests get `429` with `Retry-After`; once the gate opens all
/// remaining requests get `200` with the right output.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_output_assembly_is_capped_per_tenant() -> Result<()> {
    use cairn_store::testkit::{Action, Gate, OpSet, PathMatch, Rule};
    use common::fake_sdk::{FakeSdk, Step};
    let h = json_blob(600 << 10, b'o');
    let sdk = FakeSdk::builder()
        .service("Gen", "make", vec![Step::Output(h.clone())])
        .spawn()
        .await?;
    let bucket = common::bucket();
    let n = start(
        &bucket,
        Duration::from_secs(86_400),
        Duration::from_secs(3600),
    )
    .await?;
    let url = n.node.url();
    common::register(&url, &sdk.url).await?;
    let (code, v) = common::send(&url, "Gen", "make", None, "{}").await?;
    ensure!((200..300).contains(&code), "send: {code} {v}");
    let id = v["id"].as_str().unwrap_or_default().to_string();
    let done =
        format!("SELECT count(*) FROM sys_invocation WHERE id = '{id}' AND status = 'completed'");
    let deadline = tokio::time::Instant::now() + DEADLINE;
    while query_u64(&url, &done).await? != 1 {
        ensure!(tokio::time::Instant::now() < deadline, "make not done yet");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let gate = Gate::closed();
    n.store.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Prefix("values/".into()),
        Action::Gate(gate.clone()),
    ));
    let reqs: Vec<_> = (0..16)
        .map(|_| {
            let u = format!("{url}/invocations/{id}");
            tokio::spawn(async move {
                let r = reqwest::get(u).await?;
                let code = r.status().as_u16();
                let retry = r.headers().contains_key(reqwest::header::RETRY_AFTER);
                let v: serde_json::Value = r.json().await.unwrap_or_default();
                Ok::<_, anyhow::Error>((code, retry, v))
            })
        })
        .collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let answered = reqs.iter().filter(|r| r.is_finished()).count();
        if gate.blocked() + answered >= 16 || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let in_flight = gate.blocked();
    gate.open();
    let want = std::str::from_utf8(&h)?;
    let (mut ok, mut busy) = (0, 0);
    for r in reqs {
        let (code, retry, v) = r.await??;
        match code {
            200 => {
                ensure!(v["output"].as_str() == Some(want), "200, other output");
                ok += 1;
            }
            429 => {
                ensure!(retry, "429 without Retry-After");
                busy += 1;
            }
            other => anyhow::bail!("unexpected code {other}: {v}"),
        }
    }
    ensure!(
        in_flight <= 4,
        "{in_flight} assemblies standing at the store gate at once for one tenant, cap 4"
    );
    ensure!(ok >= 1 && busy > 0, "200: {ok}, 429: {busy}");
    n.node.shutdown().await;
    Ok(())
}
