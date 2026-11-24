// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A fake h1 SDK speaking wire v5 (incremental `REQUEST_RESPONSE` replay, see the
//! incremental-replay section of `docs/protocol-v1.md`) — shared by `rr_replay.rs`
//! and `failpoints.rs`.
//!
//! One agent handler with `N` `ctx.run`s (each returning `PAYLOAD` bytes), optionally
//! a `ctx.sleep` in the middle. Keeps a journal cache per invocation **like the TS
//! SDK** — the digest chain `cairn_runtime::replay::chain` over exactly the frame
//! bytes received — and can play two replicas behind a load balancer (round-robin,
//! or sticky by cookie).

use {
    super::{invocation, send, within, TEST_DEADLINE},
    anyhow::{anyhow, ensure, Result},
    bytes::Bytes,
    cairn_protocol::{pb, Message},
    cairn_runtime::replay::{chain, Digest},
    cairn_server::{
        discovery::{
            EndpointManifest, HandlerManifest, ProtocolMode, ServiceManifest, ServiceType,
            DISCOVERY_V1,
        },
        invoker::{decode_stream, encode_stream, invocation_content_type},
    },
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    },
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

/// The result bytes of each `ctx.run`.
pub const PAYLOAD: usize = 8 << 10;

/// The load balancer in front of the fake SDK's replicas.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lb {
    /// One replica.
    One,
    /// Two replicas in turn, no affinity.
    RoundRobin,
    /// Two replicas, sticky by cookie `lb` (set on the first response, like an ALB).
    StickyCookie,
    /// Two replicas, chosen at random (xorshift, fixed seed) — like Railway's/Render's
    /// load balancer: no affinity, no fixed alternation.
    Random,
}

/// The part the SDK still holds that it reports with a `409` (`cairn-replay-held`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Held {
    /// Like the TS SDK: `k = min(held, N)` and the real `h_k`.
    Honest,
    /// A v5 SDK from before the held-part report: reports nothing.
    None,
    /// The real `k`, the digest with one bit flipped.
    BadDigest,
    /// `k = N + 1` (any digest).
    TooBig,
    /// `k = 0`.
    Zero,
    /// `k − 1` with the real `h_{k−1}` — the runtime resends from there (tests a
    /// "send-from-k" turn).
    Shorter,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CacheMode {
    /// Keep like the TS SDK.
    Keep,
    /// Declares, returns the header, but keeps nothing (a replica just restarted every turn).
    Forget,
    /// Flip one byte of the kept frames right before the n-th turn (counting from 1).
    CorruptBefore(u64),
    /// Keep like `Keep` but answer a miss to **every** delta turn (with `held`).
    MissAlways,
}

#[derive(Clone, Debug, Default)]
pub struct TurnLog {
    pub id: Vec<u8>,
    pub replay_from: u32,
    pub known: u32,
    /// `Start.retry_count_since_last_stored_entry` — the turn's attempt.
    pub retry: u32,
    pub wire_bytes: usize,
    pub miss: bool,
    /// The `k` the SDK reports with a `409` (0 = no report).
    pub held: u32,
    pub replica: usize,
    pub cookie: Option<String>,
    pub fly: Option<String>,
    pub content_type: String,
}

#[derive(Clone, Copy)]
pub struct Spec {
    pub service: &'static str,
    /// `maxProtocolVersion` khai.
    pub max_wire: u32,
    /// `replayCache` khai.
    pub declare: bool,
    pub mode: CacheMode,
    pub lb: Lb,
    /// The number of `ctx.run`s.
    pub runs: u32,
    /// `ctx.sleep(ms)` after the second `ctx.run` (0 = none).
    pub sleep_ms: u64,
    /// Responses carry `cairn-affinity: fly=<replica id>`.
    pub fly: bool,
    /// How the held part is reported on a miss.
    pub held: Held,
    /// The result bytes of each `ctx.run` (default [`PAYLOAD`]).
    pub payload: usize,
    /// Declares `requestCompression: gzip`: decodes `content-encoding: gzip` request
    /// bodies, and answers with gzip when invited.
    pub gzip: bool,
}

impl Spec {
    pub fn new(service: &'static str) -> Self {
        Self {
            service,
            max_wire: 5,
            declare: true,
            mode: CacheMode::Keep,
            lb: Lb::One,
            runs: 6,
            sleep_ms: 0,
            fly: false,
            held: Held::Honest,
            payload: PAYLOAD,
            gzip: false,
        }
    }
}

pub type Cache = BTreeMap<Vec<u8>, (Vec<Bytes>, Vec<Digest>)>;

pub struct Shared {
    pub spec: Spec,
    /// Each replica's cache.
    caches: Mutex<[Cache; 2]>,
    rr: AtomicU64,
    turns: AtomicU64,
    log: Mutex<Vec<TurnLog>>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

pub struct Sdk {
    pub url: String,
    pub sh: Arc<Shared>,
}

impl Sdk {
    pub async fn spawn(spec: Spec) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let sh = Arc::new(Shared {
            spec,
            caches: Mutex::default(),
            rr: AtomicU64::new(0),
            turns: AtomicU64::new(0),
            log: Mutex::default(),
            tasks: Mutex::default(),
        });
        let s2 = Arc::clone(&sh);
        let accept = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let h = tokio::spawn(serve_h1(socket, Arc::clone(&s2)));
                s2.tasks.lock().unwrap().push(h.abort_handle());
            }
        });
        sh.tasks.lock().unwrap().push(accept.abort_handle());
        Ok(Self { url, sh })
    }

    pub fn log(&self) -> Vec<TurnLog> {
        self.sh.log.lock().unwrap().clone()
    }

    /// The turns (not counting misses) of invocation `id`, in order.
    pub fn turns_of(&self, id: &[u8]) -> Vec<TurnLog> {
        self.log().into_iter().filter(|t| t.id == id).collect()
    }
}

impl Drop for Sdk {
    fn drop(&mut self) {
        for h in self.sh.tasks.lock().unwrap().drain(..) {
            h.abort();
        }
    }
}

pub fn manifest(spec: &Spec) -> String {
    let mut v = serde_json::to_value(EndpointManifest {
        protocol_mode: ProtocolMode::RequestResponse,
        min_protocol_version: 1,
        max_protocol_version: spec.max_wire,
        services: vec![ServiceManifest {
            name: spec.service.into(),
            ty: ServiceType::Service,
            handlers: vec![HandlerManifest {
                name: "run".into(),
                ty: None,
                contract: Default::default(),
            }],
            contract: Default::default(),
        }],
    })
    .expect("manifest");
    if spec.declare {
        v["replayCache"] = true.into();
    }
    if spec.gzip {
        v["requestCompression"] = "gzip".into();
    }
    v.to_string()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// An agent turn over the assembled journal: `runs` results ⇒ `Output`; after the
/// second result a `ctx.sleep` (if any).
pub fn agent_turn(spec: &Spec, msgs: &[Message]) -> Vec<Message> {
    let done = msgs
        .iter()
        .filter(|m| matches!(m, Message::RunCompletion(_)))
        .count() as u32;
    let slept = msgs
        .iter()
        .any(|m| matches!(m, Message::SleepCompletion(_)));
    let sleeping = msgs.iter().any(|m| matches!(m, Message::Sleep(_)));
    let cid = done + 1 + u32::from(sleeping);
    if spec.sleep_ms > 0 && done == 2 && !slept {
        if sleeping {
            return vec![Message::Suspension(pb::SuspensionMessage {
                waiting_completions: vec![3],
                ..Default::default()
            })];
        }
        return vec![
            Message::Sleep(pb::SleepCommandMessage {
                wake_up_time: now_ms() + spec.sleep_ms,
                result_completion_id: 3,
                name: "sleep".into(),
            }),
            Message::Suspension(pb::SuspensionMessage {
                waiting_completions: vec![3],
                ..Default::default()
            }),
        ];
    }
    if done >= spec.runs {
        return vec![
            Message::Output(pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Value(
                    cairn_protocol::value::inline(Bytes::from(format!("\"ok-{done}\""))),
                )),
                name: String::new(),
            }),
            Message::End(pb::EndMessage {}),
        ];
    }
    vec![
        Message::Run(pb::RunCommandMessage {
            result_completion_id: cid,
            name: format!("r{done}"),
        }),
        Message::ProposeRunCompletion(pb::ProposeRunCompletionMessage {
            result_completion_id: cid,
            result: Some(pb::propose_run_completion_message::Result::Value(
                Bytes::from(vec![b'a' + (done % 26) as u8; spec.payload]),
            )),
        }),
        Message::Suspension(pb::SuspensionMessage {
            waiting_completions: vec![cid],
            ..Default::default()
        }),
    ]
}

pub fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// One `/invoke` request: like the TS SDK's `EndpointCore.answer` at wire v5.
pub fn answer(sh: &Shared, head: &str, wire: &[u8]) -> (u16, String, Bytes) {
    let spec = sh.spec;
    let replica = match spec.lb {
        Lb::One => 0,
        Lb::RoundRobin => (sh.rr.fetch_add(1, Ordering::SeqCst) % 2) as usize,
        Lb::Random => {
            // xorshift64 over a shared counter: deterministic in request order.
            let mut x = sh
                .rr
                .fetch_add(1, Ordering::SeqCst)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                | 1;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 32) as usize % 2
        }
        Lb::StickyCookie => match header(head, "cookie")
            .and_then(|c| c.split(';').find_map(|p| p.trim().strip_prefix("lb=")))
        {
            Some("r1") => 1,
            Some(_) => 0,
            None => (sh.rr.fetch_add(1, Ordering::SeqCst) % 2) as usize,
        },
    };
    let ct = header(head, "content-type").unwrap_or("").to_string();
    let wire_v: u32 = ct
        .rsplit(".v")
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let frames = decode_stream(Bytes::copy_from_slice(wire)).expect("valid frames");
    let Ok(Message::Start(start)) = cairn_protocol::decode(&frames[0]) else {
        panic!("first frame is not Start");
    };
    let mut log = TurnLog {
        id: start.id.to_vec(),
        replay_from: start.replay_from,
        known: start.known_entries,
        retry: start.retry_count_since_last_stored_entry,
        wire_bytes: wire.len(),
        replica,
        cookie: header(head, "cookie").map(str::to_string),
        fly: header(head, "fly-force-instance-id").map(str::to_string),
        content_type: ct.clone(),
        ..Default::default()
    };
    let n = start.known_entries as usize;
    let b = start.replay_from as usize;
    let mut caches = sh.caches.lock().unwrap();
    let cache = &mut caches[replica];
    if let CacheMode::CorruptBefore(t) = spec.mode {
        if sh.turns.load(Ordering::SeqCst) + 1 == t {
            if let Some((frames, _)) = cache.get_mut(start.id.as_ref()) {
                let mut f = frames[0].to_vec();
                let last = f.len() - 1;
                f[last] ^= 1;
                frames[0] = Bytes::from(f);
            }
        }
    }
    // Assemble: enough entries, matching digest — over the bytes **being held**.
    let entries: Vec<Bytes> = if b > 0 {
        let held = cache.get(start.id.as_ref()).and_then(|(fs, _)| {
            (spec.mode != CacheMode::MissAlways
                && fs.len() >= b
                && chain(&start.id, &fs[..b])[b][..] == start.prefix_digest[..]
                && frames.len() > n - b)
                .then(|| fs[..b].to_vec())
        });
        let Some(mut held) = held else {
            log.miss = true;
            // The held part: `k = min(held, N)`, `h_k` over the bytes being held.
            let have = cache
                .get(start.id.as_ref())
                .map(|(fs, _)| fs.len().min(n))
                .filter(|k| *k > 0)
                .map(|k| {
                    let fs = &cache[start.id.as_ref()].0;
                    (k as u32, chain(&start.id, &fs[..k])[k])
                });
            let hex = |d: &Digest| d.iter().map(|b| format!("{b:02x}")).collect::<String>();
            let line = match (spec.held, have) {
                (Held::None, _) | (_, None) => String::new(),
                (Held::Honest, Some((k, h))) => format!("{k}:{}", hex(&h)),
                (Held::BadDigest, Some((k, mut h))) => {
                    h[0] ^= 1;
                    format!("{k}:{}", hex(&h))
                }
                (Held::TooBig, Some((_, h))) => format!("{}:{}", n + 1, hex(&h)),
                (Held::Zero, Some((_, h))) => format!("0:{}", hex(&h)),
                (Held::Shorter, Some((k, _))) if k > 1 => {
                    let fs = &cache[start.id.as_ref()].0;
                    let j = k as usize - 1;
                    format!("{j}:{}", hex(&chain(&start.id, &fs[..j])[j]))
                }
                (Held::Shorter, Some(_)) => String::new(),
            };
            let mut extra = "cairn-replay: miss\r\n".to_string();
            if !line.is_empty() {
                log.held = line.split(':').next().unwrap().parse().unwrap_or(0);
                extra.push_str(&format!("cairn-replay-held: {line}\r\n"));
            }
            sh.log.lock().unwrap().push(log);
            return (409, extra, Bytes::new());
        };
        held.extend(
            frames[1..]
                .iter()
                .map(|f| cairn_protocol::encode_frame(f.ty, f.flags, &f.payload)),
        );
        held
    } else {
        frames[1..]
            .iter()
            .map(|f| cairn_protocol::encode_frame(f.ty, f.flags, &f.payload))
            .collect()
    };
    sh.turns.fetch_add(1, Ordering::SeqCst);
    let mut msgs = vec![Message::Start(start.clone())];
    for e in &entries {
        let f = decode_stream(e.clone()).expect("frame")[0].clone();
        msgs.push(cairn_protocol::decode(&f).expect("decodable"));
    }
    let out = agent_turn(&spec, &msgs);
    let ended = out.iter().any(|m| matches!(m, Message::End(_)));
    let mut extra = String::new();
    if wire_v >= 5 && spec.declare {
        if ended {
            cache.remove(start.id.as_ref());
        } else {
            if spec.mode != CacheMode::Forget {
                let kept = entries[..n].to_vec();
                let ch = chain(&start.id, &kept);
                cache.insert(start.id.to_vec(), (kept, ch));
            }
            extra.push_str(&format!("cairn-replay-cache: {n}\r\n"));
        }
        if spec.lb == Lb::StickyCookie && log.cookie.is_none() {
            extra.push_str(&format!(
                "set-cookie: lb=r{replica}; Path=/; HttpOnly\r\nset-cookie: other=1\r\n"
            ));
        }
        if spec.fly {
            extra.push_str(&format!("cairn-affinity: fly=m{replica}abc\r\n"));
        }
    }
    sh.log.lock().unwrap().push(log);
    (200, extra, encode_stream(&out))
}

pub async fn serve_h1(mut socket: tokio::net::TcpStream, sh: Arc<Shared>) {
    let mut buf = Vec::new();
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let mut chunk = [0u8; 8192];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let (status, ctype, extra, body) = if head.starts_with("GET /discover") {
        (
            200,
            DISCOVERY_V1.to_string(),
            String::new(),
            Bytes::from(manifest(&sh.spec)),
        )
    } else if head.starts_with(&format!("POST /invoke/{}/run", sh.spec.service)) {
        let len: usize = header(&head, "content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + len {
            let mut chunk = [0u8; 8192];
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let wire_v = header(&head, "content-type")
            .and_then(|c| c.rsplit(".v").next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(1u32);
        let wire = &buf[head_end..head_end + len];
        let unzipped;
        let wire = if header(&head, "content-encoding") == Some("gzip") {
            use std::io::Read as _;
            let mut out = Vec::new();
            flate2::read::MultiGzDecoder::new(wire)
                .read_to_end(&mut out)
                .expect("valid gzip");
            unzipped = out;
            &unzipped[..]
        } else {
            wire
        };
        let (status, mut extra, body) = answer(&sh, &head, wire);
        let offered = header(&head, "accept-encoding").is_some_and(|v| v.contains("gzip"));
        let body = if sh.spec.gzip && offered && status == 200 {
            use std::io::Write as _;
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(&body).expect("compress into RAM");
            extra.push_str("content-encoding: gzip\r\n");
            Bytes::from(e.finish().expect("compress into RAM"))
        } else {
            body
        };
        (
            status,
            invocation_content_type(wire_v.min(sh.spec.max_wire)).to_string(),
            extra,
            body,
        )
    } else {
        return;
    };
    let reason = if status == 200 { "OK" } else { "Conflict" };
    let reply = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {ctype}\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    let _ = socket.write_all(&body).await;
    let _ = socket.shutdown().await;
}

/// Send one invocation, wait for it to finish; returns (hex id, raw id, output).
pub async fn run_one(node: &str, sdk: &Sdk) -> Result<(String, Vec<u8>, serde_json::Value)> {
    let (code, v) = send(node, sdk.sh.spec.service, "run", None, "{}").await?;
    ensure!(code == 200 || code == 202, "send: {code} {v}");
    let id = v["id"]
        .as_str()
        .or_else(|| v["invocationId"].as_str())
        .ok_or_else(|| anyhow!("no id: {v}"))?
        .to_string();
    let st = within(TEST_DEADLINE, "invocation done", async {
        loop {
            let (_, st) = invocation(node, &id).await?;
            match st["status"].as_str() {
                Some("completed") => return Ok(st),
                Some("failed") => return Err(anyhow!("invocation failed: {st}")),
                _ => tokio::time::sleep(Duration::from_millis(30)).await,
            }
        }
    })
    .await?;
    let raw = (0..id.len() / 2)
        .map(|i| u8::from_str_radix(&id[2 * i..2 * i + 2], 16).unwrap())
        .collect();
    Ok((id, raw, st))
}

/// The invocation's output must be that of a run without cache: `"ok-<runs>"`.
pub fn expect_ok(st: &serde_json::Value, runs: u32) -> Result<()> {
    let out = st.to_string();
    ensure!(out.contains(&format!("ok-{runs}")), "odd output: {st}");
    ensure!(
        st["lastFailure"].is_null() || st.get("lastFailure").is_none(),
        "failed turn: {st}"
    );
    Ok(())
}
