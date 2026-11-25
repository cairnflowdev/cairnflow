// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The cluster-level registry generation: object `control/registry-gen` (two
//! phases).
//!
//! Registry change notifications ([`crate::registry_sync`]) are best-effort: a node
//! does not know which notifications it missed, so before this object every node ran the
//! full registry refresh round (three LISTs, a few GETs) and the quarantine round (one
//! LIST) every poll tick, even when nothing changed. This object gives a cheap way to ask
//! "did the registry change": one `If-None-Match` GET, almost always `304`.
//!
//! # Object
//!
//! ```text
//! cairnflow-registry-gen/1
//! gen=<n> by=<node>/<inc>/<seq> [open=<token>,<token>…]
//! ```
//!
//! **Read leniently, absent means "unknown"** (rule 2 of AGENTS.md): a reader compares
//! the content with the previous read and looks at the parity of `gen=`; unknown keys are
//! skipped. Absent (an old bucket, a cluster where nobody has written a registry yet) ⇒ a
//! full round every tick, as before this object. A different header of the same family
//! belongs to a newer binary: do not overwrite it.
//!
//! # Writers: two phases
//!
//! Every registry write — writes through the API (the `registry_sync::on_change` layer:
//! deployments, revisions, catalogs, keys, tenants, captures, range windows, rules) and
//! quarantine written by the node itself (`Quarantine`) — goes through **one** function,
//! [`crate::registry_sync::RegistrySync::write`]:
//!
//! 1. [`RegistryGen::begin`] **before** the main write: add a token of its own to
//!    `open=` and bring `gen` to **odd** ("writing"). If it cannot open (a failing store
//!    after a few attempts) nothing is written — the registry write request returns `503`.
//! 2. The main write.
//! 3. [`RegistryGen::seal`] **after** it: remove the token; if `open=` is empty `gen` goes
//!    to **even**, otherwise to the next odd value (other writers are still writing).
//!
//! Each step is a CAS through [`put_resolving_or_overwrite`] (rule 7; `--data-dir`
//! without CAS overwrites — a single process), with content unique to each write (`by`
//! carries the session and a sequence number within the process). A lost CAS ⇒ read
//! again and reapply its own change (the token must really go in/out). Each CAS changes
//! the content, so the seal is always a change **after** the main write.
//!
//! **A failed seal is retried in the background** (it used to be "on error, give up"):
//! the notification still goes out at once, the seal is retried through `retry::run`
//! until it wins, or until it sees the token gone and the generation past our first read;
//! the registry write request does not wait.
//!
//! **A stuck odd generation** (a writer died between the main write and the seal): the
//! coordinator ([`RegistryGen::seal_stuck`], called from its round; a lone node is its
//! own coordinator) remembers when it first saw each token; a token standing longer than
//! [`DEFAULT_STUCK`] (`S3_CLIENT_RETRY_TIMEOUT` + one request + a margin — a dead
//! writer's PUT cannot land after that) is removed by CAS. The writer's clock is not
//! needed. While the odd generation stands, readers have been running full rounds every
//! tick, so the change has arrived; the seal only ends those full rounds.
//!
//! Binaries without the two phases bump `gen + 1` and drop `open=` (they do not know that
//! key): one of their writes between a new node's `begin` and `seal` loses the token, and
//! if the new node dies in that gap the change waits for the reconcile — only during a
//! rolling upgrade. Their bump can leave an odd `gen` without a token: new readers treat
//! it as "writing" until the coordinator seals it (correct, it only costs full rounds).
//!
//! # Readers
//!
//! [`GenWatch`]: `RefreshLoop` and the quarantine round GET `If-None-Match` every tick.
//! `304` (or identical content) ⇒ skip the round; changed, absent or a failed GET ⇒ a full
//! round. The generation is read **before** the full round and only recorded once the
//! round completes in full ([`GenWatch::applied`]): a change landing between the GET and
//! the LISTs carries a newer generation, and after an unfinished round the caller forces
//! the next round to run in full. **An odd generation is never recorded**: a write in
//! flight may land after the full round, so while it stays odd every tick runs a full
//! round. Binaries without the two phases record odd ones too (correct, only less fresh
//! until the seal). A full reconcile every [`DEFAULT_RECONCILE`] regardless of the
//! generation: the last layer for binaries older than this object (registry writes that
//! do not bump it).

use {
    bytes::Bytes,
    cairn_store::cas::{put_resolving_or_overwrite, Cas},
    slatedb::object_store::{
        path::Path, Error as OsError, GetOptions, ObjectStore, ObjectStoreExt, PutMode,
        UpdateVersion,
    },
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::time::Instant,
};

/// Header this binary writes.
pub const HEADER: &str = "cairnflow-registry-gen/1";

/// Header family: the same family but different from [`HEADER`] belongs to a newer binary.
const FAMILY: &str = "cairnflow-registry-gen/";

/// Path under the cluster prefix.
pub const OBJECT: &str = "control/registry-gen";

/// Full reconcile interval regardless of the generation (precedent: Quickwit
/// reconciles every minute).
pub const DEFAULT_RECONCILE: Duration = Duration::from_secs(600);

/// A "writing" token standing longer than this (counted from when the coordinator first
/// saw it) is removed by the coordinator:
/// `S3_CLIENT_RETRY_TIMEOUT` (30 s) + one request (30 s) + a 60 s margin — a PUT
/// of a dead writer cannot land after that.
pub const DEFAULT_STUCK: Duration = Duration::from_secs(120);

/// The node is closing requests authenticated by keys in the registry because it cannot
/// confirm the registry is still fresh: `1` while closed. A process gauge.
pub static STALE: AtomicU64 = AtomicU64::new(0);

/// Requests answered `503` because the registry is not certainly fresh.
pub static STALE_REJECTED: AtomicU64 = AtomicU64::new(0);

/// Background retry policy of a failed seal: exponential backoff up to
/// 10 s, limited to one full reconcile — past it, that net catches the rest.
const RETRY: cairn_store::retry::Policy = cairn_store::retry::Policy::for_duration(
    Duration::from_millis(200),
    Duration::from_secs(10),
    DEFAULT_RECONCILE,
);

/// Retry policy of the open (the odd phase): it blocks the registry write request, so it is
/// short — a complete failure writes nothing, the caller returns `503`.
const BEGIN: cairn_store::retry::Policy =
    cairn_store::retry::Policy::new(Duration::from_millis(100), Duration::from_secs(1), 6)
        .with_total(Duration::from_secs(10));

/// This many consecutive CAS losses within one attempt count as a transient error.
const CONTENDED: usize = 8;

struct Inner {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// Identity of the bumper (`<node>/<inc>`); absent (single process, old tests)
    /// ⇒ `-/<boot nonce>`.
    session: Option<cairn_store::Session>,
    seq: AtomicU64,
    /// The latest successful read of any [`GenWatch`] of the node — what the coordinator
    /// returns in `view`.
    latest: std::sync::Mutex<Option<GenHint>>,
    /// The generation carried by the coordinator's `view`, and when it stops being fresh:
    /// while fresh, [`GenWatch::check`] compares with it instead of doing its own GET —
    /// only the coordinator polls the object on a tick.
    remote: std::sync::Mutex<Option<(GenHint, Instant)>>,
    /// The "stuck odd" threshold ([`DEFAULT_STUCK`]; tests lower it).
    stuck_after: std::sync::Mutex<Duration>,
    /// "Writing" token (or `#<gen>` for an odd generation without a token) → when
    /// [`RegistryGen::seal_stuck`] first saw it.
    stuck: std::sync::Mutex<BTreeMap<String, Instant>>,
    /// Freshness of the key registry: the latest confirmation
    /// ([`RegistryGen::confirm`]), the age limit (`None` = never closes),
    /// and the closed state (to log once per transition).
    confirmed: std::sync::Mutex<Instant>,
    fresh_limit: std::sync::Mutex<Option<Duration>>,
    closed: std::sync::atomic::AtomicBool,
    /// The node's own session, for freshness ([`RegistryGen::fresh`]): set with the age
    /// limit when the node has a session (`None` for a lone process without one).
    gate: std::sync::Mutex<Option<SessionGate>>,
    /// The latest **full** confirmation ([`RegistryGen::confirm_full`]): a refresh round
    /// that re-read every registry the node serves from (keys, tenants, deployments) after
    /// this instant. The session gate counts only this one.
    full: std::sync::Mutex<Instant>,
}

/// Freshness follows the node's own session: the registry copy counts as fresh only
/// while the incarnation's grant is valid and the copy was **fully** confirmed (a
/// refresh round that re-read keys, tenants and deployments — not a key-only read, not a
/// generation hint) **after** that incarnation opened.
///
/// Why: a synchronous registry write (key revocation, tenant state) answers `2xx` once
/// every live peer has applied it, and drops a peer whose record has ended on the bucket.
/// An observer writes `expired` only after a full TTL of silence, while the grant lapses
/// at ⅔ TTL on the node's own clock (the lease's clock assumption, LS-2); so a node whose
/// record is ended has already stopped serving registry keys. And a node that opens a new
/// incarnation in-process (it does not rerun the boot sequence) serves nothing from its
/// old copy until it has re-read the registry.
struct SessionGate {
    live:
        tokio::sync::watch::Receiver<Option<(cairn_store::SessionId, cairn_store::ValidityHandle)>>,
    /// The incarnation last seen, and when it was first seen.
    since: Option<(cairn_store::SessionId, Instant)>,
}

impl SessionGate {
    /// When the live incarnation `id` was first seen — stamped now if it is new.
    fn since(&mut self, id: &cairn_store::SessionId) -> Instant {
        match &self.since {
            Some((seen, at)) if seen == id => *at,
            _ => {
                let now = Instant::now();
                self.since = Some((id.clone(), now));
                now
            }
        }
    }

    /// `None` = the session allows freshness; `Some(why)` = it does not. `full` = the
    /// latest full confirmation.
    fn check(&mut self, full: Instant) -> Option<&'static str> {
        let live = self.live.borrow().clone();
        let Some((id, validity)) = live else {
            return Some("no live session");
        };
        let since = self.since(&id);
        if !validity.is_valid() {
            Some("the session grant is not valid")
        } else if full < since {
            Some("the registry has not been fully re-read since the session opened")
        } else {
            None
        }
    }
}

/// The registry generation as one read saw it, sent over the network: a node
/// listening to `view` compares it with the applied generation instead of doing its own GET.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GenHint {
    /// The object is absent.
    Absent,
    /// The object's content.
    Present(String),
}

/// A registry write that has been opened ([`RegistryGen::begin`]); handed to
/// [`RegistryGen::seal`] after the main write. Dropped without a seal (the caller died,
/// the future was cancelled) ⇒ the token stands until the coordinator removes it.
#[derive(Debug)]
#[must_use = "an opened registry write must be sealed"]
pub struct Open {
    /// `None` ⇒ the object carries a newer header: do not touch, do not seal.
    token: Option<String>,
}

/// One read of `gen=`/`open=`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Gen {
    n: u64,
    open: Vec<String>,
}

impl Gen {
    fn odd(&self) -> bool {
        self.n % 2 == 1
    }

    /// The next generation with the set `open`: odd while someone is still writing, even
    /// otherwise — the smallest number greater than `n` with that parity.
    fn next(&self, open: Vec<String>) -> Gen {
        let mut n = self.n.saturating_add(1);
        if (n % 2 == 1) == open.is_empty() {
            n = n.saturating_add(1);
        }
        Gen { n, open }
    }

    fn without(&self, gone: &[String]) -> Vec<String> {
        self.open
            .iter()
            .filter(|t| !gone.contains(t))
            .cloned()
            .collect()
    }
}

/// Result of one read-modify-write ([`RegistryGen::edit`]).
enum Edited {
    Written,
    /// `edit` saw no need to write.
    Unchanged,
    /// The object carries a newer header: do not overwrite.
    Newer,
}

/// A node's bumper and [`GenWatch`] builder. `Clone` is cheap.
#[derive(Clone)]
pub struct RegistryGen(Arc<Inner>);

impl RegistryGen {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
        session: Option<cairn_store::Session>,
    ) -> Self {
        let p = prefix.as_ref();
        let path = Path::from(if p.is_empty() {
            OBJECT.to_string()
        } else {
            format!("{p}/{OBJECT}")
        });
        RegistryGen(Arc::new(Inner {
            store,
            path,
            session,
            seq: AtomicU64::new(0),
            latest: std::sync::Mutex::new(None),
            remote: std::sync::Mutex::new(None),
            stuck_after: std::sync::Mutex::new(DEFAULT_STUCK),
            stuck: std::sync::Mutex::new(BTreeMap::new()),
            confirmed: std::sync::Mutex::new(Instant::now()),
            fresh_limit: std::sync::Mutex::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
            gate: std::sync::Mutex::new(None),
            full: std::sync::Mutex::new(Instant::now()),
        }))
    }

    /// Turn on close-when-uncertain (at node construction): if the key registry has not
    /// been confirmed fresh for longer than `limit` (`2·deploy_refresh`),
    /// [`Self::fresh`] returns `false`. The first mark is construction time (the node has
    /// just loaded the key registry synchronously).
    ///
    /// With a session, freshness also follows it ([`SessionGate`]): `boot` is the
    /// incarnation that was live when the boot sequence read the registry. If it is still
    /// the live one, the boot reads count as a full confirmation; if the session changed
    /// during the boot (lost and reopened in-process), the new incarnation needs a full
    /// refresh round first.
    pub fn set_fresh_limit(&self, limit: Duration, boot: Option<cairn_store::SessionId>) {
        let live = self.0.session.as_ref().map(|s| s.live_changes());
        self.install(limit, live, boot);
    }

    fn install(
        &self,
        limit: Duration,
        live: Option<
            tokio::sync::watch::Receiver<
                Option<(cairn_store::SessionId, cairn_store::ValidityHandle)>,
            >,
        >,
        boot: Option<cairn_store::SessionId>,
    ) {
        let mut boot_still_live = true;
        if let Some(live) = live {
            let current = live.borrow().as_ref().map(|(id, _)| id.clone());
            boot_still_live = boot.is_none() || current == boot;
            let since = current.map(|id| (id, Instant::now()));
            *self.0.gate.lock().expect("mutex") = Some(SessionGate { live, since });
        }
        let now = Instant::now();
        *self.0.confirmed.lock().expect("mutex") = now;
        if boot_still_live {
            *self.0.full.lock().expect("mutex") = now;
        }
        *self.0.fresh_limit.lock().expect("mutex") = Some(limit);
    }

    /// The node's live incarnation may have just changed: stamp when this one was first
    /// seen, **before** the refresh round it nudges takes its start time — otherwise the
    /// first check after that round would stamp a later time and count the round's read as
    /// older than the incarnation.
    pub fn note_session_change(&self) {
        if let Some(g) = self.0.gate.lock().expect("mutex").as_mut() {
            let live = g.live.borrow().as_ref().map(|(id, _)| id.clone());
            if let Some(id) = live {
                g.since(&id);
            }
        }
    }

    /// A refresh round re-read **every** registry the node serves from — keys, tenants,
    /// deployments — after `at` (also a key confirmation, [`Self::confirm`]). Only this
    /// reopens a new incarnation ([`SessionGate`]).
    pub fn confirm_full(&self, at: Instant) {
        self.confirm(at);
        let mut f = self.0.full.lock().expect("mutex");
        if at > *f {
            *f = at;
        }
    }

    /// Whether the node's current incarnation may act on its registry copy: a valid grant,
    /// and a full round since the incarnation opened (no session gate ⇒ `true`). Unlike
    /// [`Self::fresh`] there is no age limit — this gates taking ranges, not requests.
    pub fn session_ready(&self) -> bool {
        let full = *self.0.full.lock().expect("mutex");
        self.0
            .gate
            .lock()
            .expect("mutex")
            .as_mut()
            .is_none_or(|g| g.check(full).is_none())
    }

    /// Like [`Self::set_fresh_limit`] with the session watch given directly (tests).
    #[cfg(test)]
    pub(crate) fn set_fresh_limit_with(
        &self,
        limit: Duration,
        live: tokio::sync::watch::Receiver<
            Option<(cairn_store::SessionId, cairn_store::ValidityHandle)>,
        >,
        boot: Option<cairn_store::SessionId>,
    ) {
        self.install(limit, Some(live), boot);
    }

    /// The node's key registry is exactly as on the bucket **at** `at`: the refresh round
    /// re-read `auth/keys` successfully after `at`, or the generation read at `at` is still
    /// the generation of the latest full round applied, or an ack-requiring notification was just applied.
    pub fn confirm(&self, at: Instant) {
        let mut c = self.0.confirmed.lock().expect("mutex");
        if at > *c {
            *c = at;
        }
    }

    /// Whether the key registry still counts as fresh: confirmed within the age limit and,
    /// with a session, under a valid grant of the current incarnation since it opened
    /// ([`SessionGate`]). `false` ⇒ the caller closes requests authenticated by keys in the
    /// registry. Logs `warn` once per state transition, and sets the gauge [`STALE`].
    pub fn fresh(&self) -> bool {
        let Some(limit) = *self.0.fresh_limit.lock().expect("mutex") else {
            return true;
        };
        let confirmed = *self.0.confirmed.lock().expect("mutex");
        let full = *self.0.full.lock().expect("mutex");
        let age = confirmed.elapsed();
        let gate = self
            .0
            .gate
            .lock()
            .expect("mutex")
            .as_mut()
            .and_then(|g| g.check(full));
        let fresh = age < limit && gate.is_none();
        if self.0.closed.swap(!fresh, Ordering::Relaxed) == fresh {
            STALE.store(u64::from(!fresh), Ordering::Relaxed);
            if fresh {
                tracing::warn!(
                    "key registry confirmed fresh again — reopening authenticated requests"
                );
            } else {
                tracing::warn!(
                    age_ms = age.as_millis() as u64,
                    limit_ms = limit.as_millis() as u64,
                    session = gate.unwrap_or("ok"),
                    "cannot confirm the key registry is still fresh — closing requests authenticated by registry keys (503) until it is confirmed again"
                );
            }
        }
        fresh
    }

    /// The "stuck odd" threshold in force.
    pub fn stuck_after(&self) -> Duration {
        *self.0.stuck_after.lock().expect("mutex")
    }

    /// Set the "stuck odd" threshold (at node construction; [`DEFAULT_STUCK`] if not set).
    pub fn set_stuck_after(&self, after: Duration) {
        *self.0.stuck_after.lock().expect("mutex") = after;
    }

    fn by(&self) -> String {
        let seq = self.0.seq.fetch_add(1, Ordering::Relaxed);
        match self.0.session.as_ref().and_then(|s| s.current()) {
            Some(id) => format!("{id}/{seq}"),
            None => format!("-/{}/{seq}", cairn_store::entropy::boot_nonce()),
        }
    }

    /// Read, then CAS `edit(current generation)` (`None` = no need to write). A lost CAS
    /// ⇒ read again and reapply, at most [`CONTENDED`] times.
    async fn edit(&self, mut edit: impl FnMut(&Gen) -> Option<Gen>) -> Result<Edited, OsError> {
        let store = &self.0.store;
        let path = &self.0.path;
        for _ in 0..CONTENDED {
            let (cur, mode) = match store.get(path).await {
                Ok(got) => {
                    let v = UpdateVersion {
                        e_tag: got.meta.e_tag.clone(),
                        version: got.meta.version.clone(),
                    };
                    let raw = got.bytes().await?;
                    match decode(&raw) {
                        Decoded::Gen(g) => (g, PutMode::Update(v)),
                        Decoded::Newer => {
                            tracing::debug!(path = %path, "registry generation carries a newer header — not overwriting");
                            return Ok(Edited::Newer);
                        }
                        // Garbage: rewrite from scratch, still by CAS on the copy read.
                        Decoded::Garbage => (Gen::default(), PutMode::Update(v)),
                    }
                }
                Err(OsError::NotFound { .. }) => (Gen::default(), PutMode::Create),
                Err(e) => return Err(e),
            };
            let Some(next) = edit(&cur) else {
                return Ok(Edited::Unchanged);
            };
            let body = encode(&next, &self.by());
            match put_resolving_or_overwrite(store, path, body, mode).await? {
                Cas::Written(_) => return Ok(Edited::Written),
                Cas::Lost => continue,
            }
        }
        Err(OsError::Generic {
            store: "registry-gen",
            source: format!("{path} keeps changing underfoot — retry").into(),
        })
    }

    /// The odd phase: open a registry write — call **before** the main write. Retries a
    /// few times ([`retry::ops::REGISTRY_GEN`]); a complete failure ⇒ `Err`, the caller
    /// must not write.
    ///
    /// [`retry::ops::REGISTRY_GEN`]: cairn_store::retry::ops::REGISTRY_GEN
    pub async fn begin(&self) -> Result<Open, OsError> {
        let token = self.by();
        let newer = cairn_store::retry::run(
            &cairn_store::retry::ops::REGISTRY_GEN,
            &BEGIN,
            cairn_store::retry::Never,
            || {
                let token = token.clone();
                async move {
                    // The previous attempt may have written (an error after reaching the store):
                    // if the token is there, done.
                    let edited = self
                        .edit(|g| {
                            (!g.open.contains(&token)).then(|| {
                                let mut open = g.open.clone();
                                open.push(token.clone());
                                g.next(open)
                            })
                        })
                        .await?;
                    Ok::<_, OsError>(matches!(edited, Edited::Newer))
                }
            },
        )
        .await?;
        Ok(Open {
            token: (!newer).then_some(token),
        })
    }

    /// The even phase: seal the write `open` — call **after** the main write. The first
    /// attempt runs at once (the caller notifies peers afterwards). A failure returns `Err`
    /// for the caller to log, and **retries in the background** — without blocking the
    /// caller.
    pub async fn seal(&self, open: Open) -> Result<(), OsError> {
        let Some(token) = open.token else {
            return Ok(());
        };
        self.seal_token(Some(token)).await
    }

    /// Bump the generation one step without opening first — the `seal` of a write without a
    /// token (tests, tools copying straight onto the bucket). An odd generation without a
    /// token (a bump by a binary without the two phases) goes to even.
    pub async fn bump(&self) -> Result<(), OsError> {
        self.seal_token(None).await
    }

    /// **Generation 0 of a new bucket** (the creator calls it through
    /// `migrate::NewCluster::on_fresh`): create the object `gen=0` (even — "nobody is
    /// writing") so readers have something to `304` on, instead of treating absent as
    /// "unknown" and running a full round every tick until the first registry write.
    /// `Create`, an ambiguous outcome resolved by reading back (rule 7; content unique
    /// thanks to `by`). `Ok(true)` ⇔ this call created it; the object already exists
    /// (another creator, the first registry write) ⇒ `Ok(false)`, untouched.
    pub async fn create_initial(&self) -> Result<bool, OsError> {
        let body = encode(&Gen::default(), &self.by());
        match put_resolving_or_overwrite(&self.0.store, &self.0.path, body, PutMode::Create).await?
        {
            Cas::Written(_) => Ok(true),
            Cas::Lost => Ok(false),
        }
    }

    async fn seal_token(&self, token: Option<String>) -> Result<(), OsError> {
        let mut floor = None;
        let err = match self.seal_once(token.as_deref(), &mut floor).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let me = self.clone();
        tokio::spawn(cairn_store::io_source::tagged("registry.gen", async move {
            let floor = std::sync::Mutex::new(floor);
            let res = cairn_store::retry::run(
                &cairn_store::retry::ops::REGISTRY_GEN,
                &RETRY,
                cairn_store::retry::Never,
                || async {
                    let mut f = *floor.lock().expect("mutex");
                    let r = me.seal_once(token.as_deref(), &mut f).await;
                    *floor.lock().expect("mutex") = f;
                    r
                },
            )
            .await;
            if let Err(e) = res {
                tracing::warn!(error = %e, "cannot seal the registry generation after every attempt — the coordinator removes stuck tokens, the reconcile catches the rest");
            }
        }));
        Err(err)
    }

    /// One seal. The token is still there ⇒ remove it. The token is gone (a previous attempt
    /// reached the store but returned an error, or the coordinator removed it as stuck): if the
    /// generation is past `floor` (our first read — after the main write) that is enough,
    /// otherwise bump one step to have a change **after** the main write.
    async fn seal_once(&self, token: Option<&str>, floor: &mut Option<u64>) -> Result<(), OsError> {
        let mut first = *floor;
        self.edit(|g| {
            let f = *first.get_or_insert(g.n);
            match token {
                Some(t) if g.open.iter().any(|x| x == t) => {
                    Some(g.next(g.without(&[t.to_string()])))
                }
                _ if g.n > f => None,
                _ => Some(g.next(g.open.clone())),
            }
        })
        .await?;
        *floor = first;
        Ok(())
    }

    /// The coordinator, every tick: remove "writing" tokens standing past the threshold.
    /// Reads the node's latest read ([`Self::latest`], which the coordinator's registry
    /// refresh round reads every tick) — no I/O while the generation is even or no token
    /// is past the threshold.
    pub async fn seal_stuck(&self) {
        let latest = self.0.latest.lock().expect("mutex").clone();
        let g = match latest {
            Some(GenHint::Present(body)) => match decode(body.as_bytes()) {
                Decoded::Gen(g) => Some(g),
                _ => None,
            },
            _ => None,
        };
        let Some(g) = g.filter(Gen::odd) else {
            self.0.stuck.lock().expect("mutex").clear();
            return;
        };
        let keys = stuck_keys(&g);
        let after = *self.0.stuck_after.lock().expect("mutex");
        let now = Instant::now();
        let stale: Vec<String> = {
            let mut seen = self.0.stuck.lock().expect("mutex");
            seen.retain(|k, _| keys.contains(k));
            for k in &keys {
                seen.entry(k.clone()).or_insert(now);
            }
            seen.iter()
                .filter(|(_, t)| now.saturating_duration_since(**t) >= after)
                .map(|(k, _)| k.clone())
                .collect()
        };
        if stale.is_empty() {
            return;
        }
        let res = self
            .edit(|cur| {
                if !cur.odd() {
                    return None;
                }
                let gone: Vec<String> = cur
                    .open
                    .iter()
                    .filter(|t| stale.contains(t))
                    .cloned()
                    .collect();
                let bare = cur.open.is_empty() && stale.contains(&format!("#{}", cur.n));
                (!gone.is_empty() || bare).then(|| cur.next(cur.without(&gone)))
            })
            .await;
        match res {
            Ok(Edited::Written) => {
                tracing::warn!(
                    token = ?stale,
                    stuck_s = after.as_secs(),
                    "odd registry generation stuck past the threshold — token removed (the writer may have died between the write and the seal)"
                );
                let mut seen = self.0.stuck.lock().expect("mutex");
                seen.retain(|k, _| !stale.contains(k));
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(error = %e, "cannot remove the stuck token of the registry generation — retrying next tick")
            }
        }
    }

    /// The node's latest successful read — see [`GenHint`]. `None` while the node's registry
    /// is not certainly fresh ([`Self::fresh`]): a coordinator cut off from the bucket must
    /// not put an old generation into `view` — listeners stop listening and read for
    /// themselves.
    pub fn latest(&self) -> Option<GenHint> {
        if !self.fresh() {
            return None;
        }
        self.0.latest.lock().expect("mutex").clone()
    }

    /// The generation in the `view` just received, fresh until `until`; `None` ⇒ stop listening
    /// (an old `view`, or we are the coordinator) — [`GenWatch::check`] does its own GET.
    pub fn hear(&self, hint: Option<GenHint>, until: Instant) {
        *self.0.remote.lock().expect("mutex") = hint.map(|h| (h, until));
    }

    fn remote(&self) -> Option<Read> {
        let remote = self.0.remote.lock().expect("mutex");
        let (hint, until) = remote.as_ref()?;
        if Instant::now() >= *until {
            return None;
        }
        Some(match hint {
            GenHint::Absent => Read::Absent,
            GenHint::Present(body) => Read::Present(Seen {
                etag: None,
                body: Bytes::from(body.clone()),
            }),
        })
    }

    fn note(&self, read: &Read) {
        let hint = match read {
            Read::Unknown => return,
            Read::Absent => GenHint::Absent,
            Read::Present(s) => GenHint::Present(String::from_utf8_lossy(&s.body).into_owned()),
        };
        *self.0.latest.lock().expect("mutex") = Some(hint);
    }

    /// Generation tracker for one re-read round, with a full reconcile every `reconcile`.
    pub fn watch(&self, reconcile: Duration) -> GenWatch {
        GenWatch {
            gen: self.clone(),
            reconcile,
            seen: None,
            last_full: None,
        }
    }
}

fn encode(g: &Gen, by: &str) -> Bytes {
    let open = if g.open.is_empty() {
        String::new()
    } else {
        format!(" open={}", g.open.join(","))
    };
    Bytes::from(format!("{HEADER}\ngen={} by={by}{open}\n", g.n))
}

/// "Stuck odd" tracking key of an odd generation: each token, or `#<gen>` when
/// there is no token (a bump by a binary without the two phases).
fn stuck_keys(g: &Gen) -> Vec<String> {
    if g.open.is_empty() {
        vec![format!("#{}", g.n)]
    } else {
        g.open.clone()
    }
}

enum Decoded {
    Gen(Gen),
    Newer,
    Garbage,
}

fn decode(raw: &[u8]) -> Decoded {
    let Ok(text) = std::str::from_utf8(raw) else {
        return Decoded::Garbage;
    };
    let mut lines = text.lines();
    match lines.next() {
        Some(HEADER) => {}
        Some(h) if h.starts_with(FAMILY) => return Decoded::Newer,
        _ => return Decoded::Garbage,
    }
    let mut n = None;
    let mut open = Vec::new();
    for kv in lines.flat_map(str::split_whitespace) {
        if let Some(v) = kv.strip_prefix("gen=") {
            n = n.or(v.parse().ok());
        } else if let Some(v) = kv.strip_prefix("open=") {
            open.extend(v.split(',').filter(|t| !t.is_empty()).map(String::from));
        }
    }
    n.map_or(Decoded::Garbage, |n| Decoded::Gen(Gen { n, open }))
}

/// The content is an odd generation ("writing").
fn odd_body(body: &[u8]) -> bool {
    matches!(decode(body), Decoded::Gen(g) if g.odd())
}

/// Generation content a full round has seen.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Seen {
    etag: Option<String>,
    body: Bytes,
}

/// Generation read **before** a full round; [`GenWatch::applied`] records it once the
/// round completes in full.
#[derive(Debug)]
pub struct Reading(Read);

#[derive(Clone, Debug)]
enum Read {
    /// The GET failed: generation unknown — full round, keep the recorded generation.
    Unknown,
    /// The object is absent.
    Absent,
    Present(Seen),
}

/// Watches `control/registry-gen` for one registry re-read round.
pub struct GenWatch {
    gen: RegistryGen,
    reconcile: Duration,
    /// Generation of the latest full round seen (`None` = absent or no round yet).
    seen: Option<Seen>,
    last_full: Option<Instant>,
}

impl GenWatch {
    /// Poll tick: `None` = the generation is as the latest full round saw it and the
    /// reconcile is not due — skip the round. `Some` = run a full round (the generation
    /// changed, absent, the GET failed, or the reconcile is due).
    ///
    /// Listening to a fresh `view` ([`RegistryGen::hear`]): compares with the generation in
    /// the `view`, no GET.
    pub async fn check(&mut self) -> Option<Reading> {
        let due = self.last_full.is_none_or(|t| t.elapsed() >= self.reconcile);
        let read = match self.gen.remote() {
            Some(r) => r,
            None => self.read().await,
        };
        if !due {
            if let (Read::Present(now), Some(seen)) = (&read, &self.seen) {
                if now.body == seen.body {
                    // The etag may change while the content does not (the store copied it).
                    // A generation from `view` carries no etag: keep the etag seen.
                    if now.etag.is_some() {
                        self.seen = Some(now.clone());
                    }
                    return None;
                }
            }
        }
        Some(Reading(read))
    }

    /// A forced full round (a notification, an unfinished previous round): still read the
    /// generation **before** the round to record it afterwards.
    pub async fn force(&mut self) -> Reading {
        Reading(self.read().await)
    }

    /// A full round that started after `reading` has just completed in full. An odd
    /// generation is not recorded: a write in flight may land after this round, so the
    /// next tick still runs a full round.
    pub fn applied(&mut self, reading: Reading) {
        match reading.0 {
            Read::Unknown => {}
            Read::Absent => self.seen = None,
            Read::Present(s) if odd_body(&s.body) => {}
            Read::Present(s) => self.seen = Some(s),
        }
        self.last_full = Some(Instant::now());
    }

    async fn read(&self) -> Read {
        let read = self.read_store().await;
        self.gen.note(&read);
        read
    }

    async fn read_store(&self) -> Read {
        let opts = GetOptions {
            if_none_match: self.seen.as_ref().and_then(|s| s.etag.clone()),
            ..Default::default()
        };
        match self.gen.0.store.get_opts(&self.gen.0.path, opts).await {
            Err(OsError::NotModified { .. }) => match &self.seen {
                Some(s) => Read::Present(s.clone()),
                None => Read::Unknown,
            },
            Ok(got) => {
                let etag = got.meta.e_tag.clone();
                match got.bytes().await {
                    Ok(body) => Read::Present(Seen { etag, body }),
                    Err(e) => {
                        tracing::debug!(error = %e, "cannot read the registry generation — full round");
                        Read::Unknown
                    }
                }
            }
            Err(OsError::NotFound { .. }) => Read::Absent,
            Err(e) => {
                tracing::debug!(error = %e, "cannot read the registry generation — full round");
                Read::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        cairn_store::testkit::{Action, Ambiguity, FaultyStore, Op, OpSet, PathMatch, Rule},
        slatedb::object_store::memory::InMemory,
    };

    /// A watch of the node's live incarnation, as `Session::live_changes` gives it.
    type Live =
        tokio::sync::watch::Sender<Option<(cairn_store::SessionId, cairn_store::ValidityHandle)>>;

    fn incarnation(
        inc: u64,
        valid_for: Duration,
    ) -> Option<(cairn_store::SessionId, cairn_store::ValidityHandle)> {
        Some((
            cairn_store::SessionId {
                node: "n".into(),
                inc,
            },
            cairn_store::testkit::validity_until(Instant::now() + valid_for),
        ))
    }

    /// **A registry copy is fresh only under a valid grant of the current incarnation.**
    ///
    /// 1. Freshness on (limit 60 s), incarnation 1 live with a valid grant ⇒ fresh.
    /// 2. The grant is revoked (the session was lost) ⇒ not fresh, although the copy was
    ///    confirmed a moment ago — an observer may already have written `expired`, and a
    ///    peer then counts the revocation applied without this node.
    #[tokio::test]
    async fn freshness_needs_a_valid_grant() {
        let (_f, g) = faulty();
        let live: Live = tokio::sync::watch::channel(incarnation(1, Duration::from_secs(60))).0;
        g.set_fresh_limit_with(Duration::from_secs(60), live.subscribe(), None);
        assert!(g.fresh(), "valid grant, copy just confirmed");
        live.borrow().as_ref().expect("live").1.revoke();
        assert!(!g.fresh(), "the grant is revoked");
        live.send_replace(None);
        assert!(!g.fresh(), "no live incarnation");
    }

    /// **A session that changed during the boot needs a full round** (independent review:
    /// the boot sequence reads the registry under incarnation 1; if 1 is lost and 2 opens
    /// before freshness is switched on, the boot reads must not count for 2).
    ///
    /// Must see: incarnation 2 is not ready until a full round; with the boot incarnation
    /// still live it is ready at once.
    #[tokio::test]
    async fn session_changed_during_boot_needs_a_full_round() {
        let (_f, g) = faulty();
        let live: Live = tokio::sync::watch::channel(incarnation(2, Duration::from_secs(60))).0;
        let boot = incarnation(1, Duration::from_secs(60)).map(|(id, _)| id);
        g.set_fresh_limit_with(Duration::from_secs(60), live.subscribe(), boot);
        assert!(
            !g.session_ready(),
            "incarnation 2 counted the boot reads of 1"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
        g.confirm_full(Instant::now());
        assert!(g.session_ready());

        let (_f, g) = faulty();
        let live: Live = tokio::sync::watch::channel(incarnation(1, Duration::from_secs(60))).0;
        let boot = incarnation(1, Duration::from_secs(60)).map(|(id, _)| id);
        g.set_fresh_limit_with(Duration::from_secs(60), live.subscribe(), boot);
        assert!(g.session_ready(), "the boot incarnation is still live");
    }

    /// **A confirmation that re-read only part of the registry does not reopen a new
    /// incarnation** (independent review of the session gate: the refresh round confirms
    /// once `auth/keys` is read even when `tenants` or the deployments fail, and a
    /// generation hint heard from the coordinator confirms without any read — so the new
    /// incarnation could serve tenant state and deployments from the old copy).
    ///
    /// 1. Incarnation 1 live, copy confirmed ⇒ fresh.
    /// 2. Incarnation 2 opens; a key-only confirmation (`confirm`) comes after it.
    ///
    /// Must see: still not fresh — only a full round after the incarnation opened counts.
    #[tokio::test]
    async fn key_only_confirmation_does_not_reopen_a_new_incarnation() {
        let (_f, g) = faulty();
        let live: Live = tokio::sync::watch::channel(incarnation(1, Duration::from_secs(60))).0;
        g.set_fresh_limit_with(Duration::from_secs(60), live.subscribe(), None);
        assert!(g.fresh());
        tokio::time::sleep(Duration::from_millis(2)).await;
        live.send_replace(incarnation(2, Duration::from_secs(60)));
        assert!(!g.fresh());
        tokio::time::sleep(Duration::from_millis(2)).await;
        g.confirm(Instant::now());
        assert!(!g.fresh(), "a key-only confirmation reopened incarnation 2");
        assert!(!g.session_ready(), "incarnation 2 may not take ranges yet");
        g.confirm_full(Instant::now());
        assert!(g.fresh() && g.session_ready(), "a full round reopens it");
    }

    /// **A new incarnation opened in-process serves nothing from the old copy.**
    ///
    /// 1. Incarnation 1 live, copy confirmed ⇒ fresh.
    /// 2. Incarnation 2 opens (the node lost 1 and re-incarnated without the boot
    ///    sequence) ⇒ not fresh: the copy was confirmed under incarnation 1, and a write
    ///    made between the two may have been acked without this node.
    /// 3. The registry is re-read (`confirm`) ⇒ fresh again.
    #[tokio::test]
    async fn new_incarnation_is_stale_until_the_registry_is_read_again() {
        let (_f, g) = faulty();
        let live: Live = tokio::sync::watch::channel(incarnation(1, Duration::from_secs(60))).0;
        g.set_fresh_limit_with(Duration::from_secs(60), live.subscribe(), None);
        assert!(g.fresh());
        tokio::time::sleep(Duration::from_millis(2)).await;
        live.send_replace(incarnation(2, Duration::from_secs(60)));
        assert!(!g.fresh(), "incarnation 2 has not re-read the registry");
        tokio::time::sleep(Duration::from_millis(2)).await;
        g.confirm_full(Instant::now());
        assert!(g.fresh(), "re-read under incarnation 2");
    }

    fn faulty() -> (Arc<FaultyStore>, RegistryGen) {
        let f = FaultyStore::wrap(Arc::new(InMemory::new()));
        let store: Arc<dyn ObjectStore> = f.clone();
        (f, RegistryGen::new(store, &Path::from(""), None))
    }

    async fn gen_of(f: &Arc<FaultyStore>) -> Option<u64> {
        let store: Arc<dyn ObjectStore> = f.clone();
        let raw = store
            .get(&Path::from(OBJECT))
            .await
            .ok()?
            .bytes()
            .await
            .ok()?;
        match decode(&raw) {
            Decoded::Gen(g) => Some(g.n),
            _ => None,
        }
    }

    async fn open_of(f: &Arc<FaultyStore>) -> Vec<String> {
        let store: Arc<dyn ObjectStore> = f.clone();
        let raw = store
            .get(&Path::from(OBJECT))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        match decode(&raw) {
            Decoded::Gen(g) => g.open,
            _ => panic!("cannot decode"),
        }
    }

    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            cairn_store::object_policy(OBJECT).map(|p| p.header),
            Some(HEADER)
        );
    }

    #[test]
    fn decode_is_lenient() {
        assert!(matches!(
            decode(format!("{HEADER}\ngen=7 by=a/1/0 odd=x\njunk\n").as_bytes()),
            Decoded::Gen(Gen { n: 7, ref open }) if open.is_empty()
        ));
        assert!(matches!(
            decode(format!("{HEADER}\ngen=9 by=a/1/3 open=a/1/1,b/2/0,\n").as_bytes()),
            Decoded::Gen(Gen { n: 9, ref open }) if open == &["a/1/1", "b/2/0"]
        ));
        assert!(matches!(
            decode(b"cairnflow-registry-gen/2\ngen=7\n"),
            Decoded::Newer
        ));
        assert!(matches!(decode(b"not a record\n"), Decoded::Garbage));
    }

    #[tokio::test]
    async fn bump_counts_up_and_each_body_is_unique() {
        let (f, gen) = faulty();
        assert_eq!(gen_of(&f).await, None);
        // A bump without opening first: the next even value.
        gen.bump().await.unwrap();
        assert_eq!(gen_of(&f).await, Some(2));
        gen.bump().await.unwrap();
        assert_eq!(gen_of(&f).await, Some(4));
        assert_ne!(
            gen.by(),
            gen.by(),
            "the content of each bump must be unique (rule 7)"
        );
    }

    /// Rule 7: a bump reached the store but the answer is ambiguous — reading back sees
    /// exactly our content ⇒ done, no second bump, no error.
    #[tokio::test]
    async fn ambiguous_bump_is_resolved_by_reread() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let (f, gen) = faulty();
            for want in [2, 4] {
                f.add_rule(Rule::new(
                    OpSet::PUT,
                    PathMatch::Contains(OBJECT.into()),
                    Action::Ambiguous {
                        reply,
                        skip: 0,
                        times: Some(1),
                    },
                ));
                gen.bump()
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
                assert_eq!(gen_of(&f).await, Some(want), "{reply:?}");
            }
        }
    }

    /// **Generation 0 of a new bucket**: the first create writes
    /// `gen=0` (even, no token); a later create — a second creator — leaves it
    /// alone; registry writes after that go on from 0 (odd 1 then even 2). An ambiguous
    /// outcome of the create (`Error`, `Retried` ⇒ `AlreadyExists`) is resolved by reading back.
    #[tokio::test]
    async fn create_initial_writes_generation_zero_once() {
        let (f, gen) = faulty();
        assert!(gen.create_initial().await.unwrap());
        assert_eq!(gen_of(&f).await, Some(0));
        assert!(open_of(&f).await.is_empty());
        let store: Arc<dyn ObjectStore> = f.clone();
        let before = store
            .get(&Path::from(OBJECT))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        // The second create carries a different `by` (the sequence number grows) — like a
        // second creator (another session): the object exists ⇒ `Lost`, untouched.
        assert!(!gen.create_initial().await.unwrap(), "exists ⇒ not created");
        let after = store
            .get(&Path::from(OBJECT))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(before, after, "the second creator overwrote");
        let open = gen.begin().await.unwrap();
        assert_eq!(gen_of(&f).await, Some(1));
        gen.seal(open).await.unwrap();
        assert_eq!(gen_of(&f).await, Some(2));

        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let (f, gen) = faulty();
            f.add_rule(Rule::new(
                OpSet::PUT,
                PathMatch::Contains(OBJECT.into()),
                Action::Ambiguous {
                    reply,
                    skip: 0,
                    times: Some(1),
                },
            ));
            assert!(
                gen.create_initial()
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: {e}")),
                "{reply:?}: our own ambiguous create"
            );
            assert_eq!(gen_of(&f).await, Some(0), "{reply:?}");
        }
    }

    /// A failed bump (a transient store error) does not give up: it retries in the
    /// background until it wins.
    #[tokio::test(start_paused = true)]
    async fn failed_bump_lands_later() {
        let (f, gen) = faulty();
        f.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(OBJECT.into()),
            Action::Fail { times: Some(3) },
        ));
        let _ = gen.bump().await;
        for _ in 0..600 {
            if gen_of(&f).await == Some(2) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the failed bump was not retried within 60 s");
    }

    #[tokio::test]
    async fn newer_header_is_not_overwritten() {
        let (f, gen) = faulty();
        let store: Arc<dyn ObjectStore> = f.clone();
        let newer = Bytes::from_static(b"cairnflow-registry-gen/2\ngen=9\n");
        store
            .put(&Path::from(OBJECT), newer.clone().into())
            .await
            .unwrap();
        gen.bump().await.unwrap();
        let now = store
            .get(&Path::from(OBJECT))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(now, newer);
    }

    /// Absent ⇒ a full round every tick; present ⇒ `304` skips the round; bumped ⇒ full round;
    /// a failed GET ⇒ full round; an unfinished round (no `applied`) ⇒ the next tick is still a full round.
    #[tokio::test]
    async fn watch_skips_only_an_unchanged_generation() {
        let (f, gen) = faulty();
        let mut w = gen.watch(Duration::from_secs(3600));
        let r = w.check().await.expect("no round yet ⇒ full round");
        w.applied(r);
        let r = w.check().await.expect("absent ⇒ full round");
        w.applied(r);

        gen.bump().await.unwrap();
        let r = w.check().await.expect("new generation ⇒ full round");
        // Unfinished round: not recorded ⇒ the next tick still sees a change.
        drop(r);
        let r = w
            .check()
            .await
            .expect("previous round unfinished ⇒ full round");
        w.applied(r);

        let gets = |f: &FaultyStore| f.counts().op(Op::Get);
        let before = gets(&f);
        assert!(
            w.check().await.is_none(),
            "generation unchanged ⇒ skip the round"
        );
        assert!(w.check().await.is_none());
        assert_eq!(gets(&f) - before, 2, "one GET per tick");

        f.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains(OBJECT.into()),
            Action::Fail { times: Some(1) },
        ));
        let r = w.check().await.expect("failed GET ⇒ full round");
        w.applied(r);
        assert!(
            w.check().await.is_none(),
            "a failed GET does not clear the recorded generation"
        );

        gen.bump().await.unwrap();
        assert!(w.check().await.is_some());
    }

    #[tokio::test]
    async fn watch_reconciles_on_schedule_regardless_of_generation() {
        let (_f, gen) = faulty();
        gen.bump().await.unwrap();
        let mut w = gen.watch(Duration::ZERO);
        for _ in 0..3 {
            let r = w.check().await.expect("reconcile due ⇒ full round");
            w.applied(r);
        }
    }

    /// Two phases: open ⇒ odd carrying a token, seal ⇒ even,
    /// token gone. Two overlapping writers: odd until the last one seals.
    #[tokio::test]
    async fn begin_is_odd_and_seal_is_even_even_with_overlapping_writers() {
        let (f, gen) = faulty();
        let a = gen.begin().await.unwrap();
        assert_eq!(gen_of(&f).await, Some(1));
        assert_eq!(open_of(&f).await.len(), 1);
        let b = gen.begin().await.unwrap();
        assert_eq!(
            gen_of(&f).await,
            Some(3),
            "someone still writing ⇒ still odd"
        );
        assert_eq!(open_of(&f).await.len(), 2);
        gen.seal(a).await.unwrap();
        assert_eq!(gen_of(&f).await, Some(5), "b not sealed yet ⇒ still odd");
        gen.seal(b).await.unwrap();
        assert_eq!(gen_of(&f).await, Some(6));
        assert!(open_of(&f).await.is_empty());
    }

    /// Rule 7 for both phases: the open and the seal reach the store but the answer is
    /// ambiguous — reading back resolves it; the token goes in exactly once and out exactly once.
    #[tokio::test]
    async fn ambiguous_begin_and_seal_are_resolved_by_reread() {
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let (f, gen) = faulty();
            let ambiguous = || {
                Rule::new(
                    OpSet::PUT,
                    PathMatch::Contains(OBJECT.into()),
                    Action::Ambiguous {
                        reply,
                        skip: 0,
                        times: Some(1),
                    },
                )
            };
            f.add_rule(ambiguous());
            let open = gen
                .begin()
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
            assert_eq!(gen_of(&f).await, Some(1), "{reply:?}");
            assert_eq!(open_of(&f).await.len(), 1, "{reply:?}");
            f.add_rule(ambiguous());
            gen.seal(open)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: {e}"));
            assert_eq!(gen_of(&f).await, Some(2), "{reply:?}");
            assert!(open_of(&f).await.is_empty(), "{reply:?}");
        }
    }

    /// An open that fails completely ⇒ `Err` (the caller does not write); a few transient
    /// errors are retried and then it opens.
    #[tokio::test(start_paused = true)]
    async fn begin_retries_then_gives_up() {
        let (f, gen) = faulty();
        f.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(OBJECT.into()),
            Action::Fail { times: Some(2) },
        ));
        let open = gen
            .begin()
            .await
            .expect("two transient errors then healthy");
        gen.seal(open).await.unwrap();
        f.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(OBJECT.into()),
            Action::Fail { times: None },
        ));
        assert!(gen.begin().await.is_err());
        assert_eq!(
            gen_of(&f).await,
            Some(2),
            "a failed open leaves nothing behind"
        );
    }

    /// A failed seal is retried in the background until the token is gone.
    #[tokio::test(start_paused = true)]
    async fn failed_seal_lands_later() {
        let (f, gen) = faulty();
        let open = gen.begin().await.unwrap();
        f.add_rule(Rule::new(
            OpSet::PUT,
            PathMatch::Contains(OBJECT.into()),
            Action::Fail { times: Some(3) },
        ));
        assert!(gen.seal(open).await.is_err());
        for _ in 0..600 {
            if gen_of(&f).await == Some(2) {
                assert!(open_of(&f).await.is_empty());
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the failed seal was not retried within 60 s");
    }

    /// Readers never record an odd generation: while odd, a full round every
    /// tick; once sealed even, the round is skipped.
    #[tokio::test]
    async fn watch_never_records_an_odd_generation() {
        let (_f, gen) = faulty();
        gen.bump().await.unwrap();
        let mut w = gen.watch(Duration::from_secs(3600));
        let r = w.check().await.expect("first round");
        w.applied(r);
        assert!(w.check().await.is_none());
        let open = gen.begin().await.unwrap();
        for _ in 0..3 {
            let r = w.check().await.expect("odd ⇒ a full round every tick");
            w.applied(r);
        }
        gen.seal(open).await.unwrap();
        let r = w.check().await.expect("sealed ⇒ full round");
        w.applied(r);
        assert!(w.check().await.is_none(), "even applied ⇒ skip the round");
    }

    /// A stuck odd generation (a writer died between the main write and the seal): the
    /// coordinator removes the token after the threshold counted from when **it** first saw
    /// the token, not earlier; a new token of another writer is kept. An odd generation
    /// without a token (a binary without the two phases) is sealed too.
    #[tokio::test(start_paused = true)]
    async fn coordinator_seals_a_stuck_odd_generation_after_the_threshold() {
        let (f, gen) = faulty();
        gen.set_stuck_after(Duration::from_secs(10));
        let mut w = gen.watch(Duration::from_secs(3600));
        let _dead = gen.begin().await.unwrap();
        let _ = w.check().await; // the coordinator's refresh round reads the generation
        gen.seal_stuck().await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        let alive = gen.begin().await.unwrap();
        let _ = w.check().await;
        gen.seal_stuck().await;
        assert_eq!(open_of(&f).await.len(), 2);
        tokio::time::sleep(Duration::from_secs(5)).await;
        let _ = w.check().await;
        gen.seal_stuck().await;
        assert_eq!(
            open_of(&f).await.len(),
            1,
            "dead token past the threshold ⇒ removed"
        );
        assert_eq!(
            gen_of(&f).await.map(|n| n % 2),
            Some(1),
            "the live writer ⇒ still odd"
        );
        gen.seal(alive).await.unwrap();
        assert_eq!(gen_of(&f).await.map(|n| n % 2), Some(0));

        // An odd generation without a token, from a binary without the two phases.
        let store: Arc<dyn ObjectStore> = f.clone();
        store
            .put(
                &Path::from(OBJECT),
                Bytes::from(format!("{HEADER}\ngen=41 by=cu/1/0\n")).into(),
            )
            .await
            .unwrap();
        let _ = w.check().await;
        gen.seal_stuck().await;
        assert_eq!(gen_of(&f).await, Some(41));
        tokio::time::sleep(Duration::from_secs(11)).await;
        let _ = w.check().await;
        gen.seal_stuck().await;
        assert_eq!(gen_of(&f).await, Some(42));
    }
}
