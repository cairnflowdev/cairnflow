// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant settings: a **tamper-evident hash chain** (off by default), its own
//! **slow log threshold**, and its own **poison-handler quarantine mode + rule**.
//!
//! One cluster-level object `tenants`, text like the rule book and the catalog:
//!
//! ```text
//! cairnflow-tenants/1
//! <tenant> hash-chain slow-ms=2000 quarantine=hold quarantine-rule=20/5/300 identity=2 identity-keys=1,2
//! <tenant> retention-ms=86400000 max-payload=1048576 max-journal=10000 deploy-rate=10/60
//! ```
//!
//! `state=active|limited|suspended`: the tenant's state — written only by
//! `PUT /admin/tenants/{t}/state` (with an ACK from every node, behind the
//! `tenants/state` gate). An unknown value ⇒ `suspended` (not `active`), kept
//! verbatim when written back; absent = `active`.
//!
//! `state=deleting|purged` comes with three ms marks: `deleting-ms=` (entering
//! phase A, reversible for 7 days), `purging-ms=` (the point of no return of
//! phase B), `purged-ms=` (the tombstone). The marks are kept **verbatim** like
//! `state=`: a corrupt value does not corrupt the whole book, but a mark that is
//! present (even corrupt) is still present — `purging-ms=x` still blocks going
//! back, and the proof gate (`cairn_store::tenant_purge`) treats a corrupt mark as
//! "cannot prove".
//!
//! `refused-ms=`/`lifted-ms=`: ms marks of the latest slot-refusal spell
//! (`suspended`, `deleting`) and of when it was lifted — written only by
//! [`TenantFlags::change_state`]. The leader of a range that comes up **after** the
//! suspension was lifted has never seen the tenant suspended, so it does not know to
//! pull back the spell's 15-minute deferred rows; the two marks tell it the due window
//! of those rows (`turn::LiftSweep`). Read leniently: a corrupt number is kept
//! verbatim like an unknown flag and treated as absent — losing the mark only means
//! waiting for the deferral tick as before. Older binaries keep them verbatim and do
//! not update them: a lift written by an older binary only takes effect on the
//! leaders that see it.
//!
//! `identity=`/`identity-keys=`: the identity key generation currently signing and the
//! set currently published; absent = 1 and {1}. Written only by `cairn-ctl identity …`.
//!
//! `retention-ms=`, `max-payload=`, `max-journal=`, `deploy-rate=` are **per-tenant limits** for a shared cell,
//! plus resource limits: `max-live=` (live
//! invocations, a soft limit), `max-deployments=`, `max-schedules=`, `max-timers=` (per
//! invocation), `max-streams=` (per invocation), `max-stream-ram=` (bytes of stream
//! RAM of the tenant on each node; absent = 1/8 of the node's stream
//! RAM), `max-state-keys=` (per object),
//! `max-artifact-bytes=` (whole tenant), `max-journal-bytes=` (total journal bytes
//! per invocation — exceeded ⇒ the invocation is `paused` with code 580),
//! `max-value-bytes=` (logical size of one value —
//! exceeded ⇒ `failed` with code 579 `value_bytes_limit`; older binaries keep it verbatim
//! like an unknown flag): absent = no limit, so self-hosting is unchanged. Unlike the old
//! flags, an unknown value of these (`max-payload=1kb`) **fails the whole decode**: the
//! node keeps the old book — the limits currently in effect — and raises the gauge
//! `cairnflow_tenants_book_rejected` (fail closed); silently skipping it would lift a
//! limit exactly when nobody is looking. `PUT /admin/tenants/{t}` refuses unknown values
//! before writing.
//!
//! Each line is one tenant and its flags. **Read leniently**: unknown flags are skipped on
//! read and **kept verbatim** when written back — a newer binary adds a flag, an older
//! binary can still read it and does not drop that flag when it changes its own. (Binaries
//! before per-tenant slow log thresholds re-encoded from the set of chained tenants, so one
//! of them toggling the chain loses `slow-ms=`; all that is lost is the slow log threshold,
//! which falls back to the node default.)
//!
//! Written by CAS on the etag; every node re-reads it on the deployment registry refresh
//! tick and replaces the [`ChainedTenants`] that the engine asks when it creates an
//! invocation, along with the slow log thresholds. The node that took the change sees it
//! at once; other nodes after one tick — an invocation created on another node in that
//! window follows the old value, and keeps it for life (see `cairn_engine::chain`).

use cairn_store::entropy::{HashMap, HashSet};
use {
    cairn_engine::ChainedTenants,
    cairn_store::{
        cas::{put_resolving_or_overwrite, Cas},
        TenantId,
    },
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    },
};

const HEADER: &str = "cairnflow-tenants/1";
const HASH_CHAIN: &str = "hash-chain";
const SLOW_MS: &str = "slow-ms=";
const QUARANTINE: &str = "quarantine=";
const QUARANTINE_RULE: &str = "quarantine-rule=";
const IDENTITY: &str = "identity=";
const IDENTITY_KEYS: &str = "identity-keys=";
const RETENTION_MS: &str = "retention-ms=";
const MAX_PAYLOAD: &str = "max-payload=";
const MAX_VALUE_BYTES: &str = "max-value-bytes=";
const MAX_JOURNAL: &str = "max-journal=";
const DEPLOY_RATE: &str = "deploy-rate=";
const STATE: &str = "state=";
const MAX_LIVE: &str = "max-live=";
const MAX_DEPLOYMENTS: &str = "max-deployments=";
const MAX_SCHEDULES: &str = "max-schedules=";
const MAX_TIMERS: &str = "max-timers=";
const MAX_STREAMS: &str = "max-streams=";
const MAX_STREAM_RAM: &str = "max-stream-ram=";
const MAX_STATE_KEYS: &str = "max-state-keys=";
const MAX_ARTIFACT_BYTES: &str = "max-artifact-bytes=";
const MAX_JOURNAL_BYTES: &str = "max-journal-bytes=";
const DELETING_MS: &str = "deleting-ms=";
const PURGING_MS: &str = "purging-ms=";
const PURGED_MS: &str = "purged-ms=";
const REFUSED_MS: &str = "refused-ms=";
const LIFTED_MS: &str = "lifted-ms=";

/// The three purge marks of a tenant, **verbatim**. Present but corrupt is still
/// present (see the file's top doc).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PurgeMarks {
    pub deleting: Option<String>,
    pub purging: Option<String>,
    pub purged: Option<String>,
}

impl PurgeMarks {
    fn is_empty(&self) -> bool {
        self.deleting.is_none() && self.purging.is_none() && self.purged.is_none()
    }

    /// `deleting-ms=`, if readable.
    pub fn deleting_ms(&self) -> Option<u64> {
        self.deleting.as_deref().and_then(|v| v.parse().ok())
    }

    /// `purging-ms=`, if readable.
    pub fn purging_ms(&self) -> Option<u64> {
        self.purging.as_deref().and_then(|v| v.parse().ok())
    }

    /// `purged-ms=`, if readable.
    pub fn purged_ms(&self) -> Option<u64> {
        self.purged.as_deref().and_then(|v| v.parse().ok())
    }
}

/// Why a state change was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateRefusal {
    /// The `default` tenant cannot be deleted.
    Default,
    /// Past the `purging-ms=` mark: cannot be reversed.
    Purging,
    /// `purged` tombstone: nothing can change any more.
    Purged,
}

/// Deployment registration rate limit of a tenant: at most `count` registrations in
/// `per_s` seconds (`deploy-rate=N/S`). Each registration is a GET out to a third-party
/// host (discovery), so this is the limit on using cairn as a reflector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeployRate {
    pub count: u32,
    pub per_s: u32,
}

impl DeployRate {
    /// `N/S`, both ≥ 1.
    pub fn parse(v: &str) -> Option<Self> {
        let (n, s) = v.split_once('/')?;
        let (count, per_s) = (n.parse().ok()?, s.parse().ok()?);
        (count >= 1 && per_s >= 1).then_some(Self { count, per_s })
    }

    pub fn encode(&self) -> String {
        format!("{}/{}", self.count, self.per_s)
    }
}

/// `retention-ms=`: a number of ms, `0` = keep forever (like `--retention-ms 0`).
pub fn parse_retention(v: &str) -> Option<u64> {
    v.parse().ok()
}

/// `max-payload=`: a number of bytes ≥ 1.
pub fn parse_max_payload(v: &str) -> Option<u64> {
    v.parse().ok().filter(|n| *n >= 1)
}

/// `max-journal=`: a number of entries ≥ 1 (counting `Input`).
pub fn parse_max_journal(v: &str) -> Option<u32> {
    v.parse().ok().filter(|n| *n >= 1)
}

/// Count limits (`max-live=`, `max-artifact-bytes=`): a number ≥ 1.
pub fn parse_count_u64(v: &str) -> Option<u64> {
    v.parse().ok().filter(|n| *n >= 1)
}

/// Small count limits (`max-deployments=`, `max-schedules=`, `max-timers=`,
/// `max-streams=`, `max-state-keys=`): a number ≥ 1.
pub fn parse_count_u32(v: &str) -> Option<u32> {
    v.parse().ok().filter(|n| *n >= 1)
}

/// Flags of one tenant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantFlags {
    pub hash_chain: bool,
    /// Its own slow log threshold, ms. `None` = the node default.
    pub slow_ms: Option<u64>,
    /// Its own poison-handler quarantine mode (`hold` is enabled
    /// per tenant). `None` = the node default.
    pub quarantine: Option<crate::quarantine::Mode>,
    /// Its own quarantine rule, `attempts/invocations/seconds`. `None` = the node's.
    pub quarantine_rule: Option<crate::quarantine::Rule>,
    /// Identity key generation. `None` = default (1, {1}).
    pub identity: Option<crate::identity::Generations>,
    /// Limits the engine and ingress enforce. Absent = no limit.
    pub limits: cairn_engine::TenantLimit,
    /// Rate limit of `POST /deployments`. Absent = no limit.
    pub deploy_rate: Option<DeployRate>,
    /// The **verbatim** value of `state=`. Absent = `active`.
    /// Kept verbatim so that a newer binary's value (`deleting`) is not lost when
    /// this binary writes the book back; its meaning is [`TenantFlags::state`].
    pub state_raw: Option<String>,
    /// Purge marks.
    pub purge: PurgeMarks,
    /// Unknown flags — kept verbatim when written back.
    extra: Vec<String>,
}

impl TenantFlags {
    fn is_empty(&self) -> bool {
        !self.hash_chain
            && self.slow_ms.is_none()
            && self.quarantine.is_none()
            && self.quarantine_rule.is_none()
            && self.identity.is_none()
            && self.limits.is_empty()
            && self.deploy_rate.is_none()
            && self.state_raw.is_none()
            && self.purge.is_empty()
            && self.extra.is_empty()
    }

    /// Set the state: `Active` removes the field from the book.
    pub fn set_state(&mut self, state: cairn_engine::TenantState) {
        self.state_raw =
            (state != cairn_engine::TenantState::Active).then(|| state.as_str().to_string());
        self.limits.state = state;
    }

    /// Change the state by the rules of `PUT /admin/tenants/{t}/state`: `Ok(true)` =
    /// the book changed, `Ok(false)` = already like that (the same command sent again).
    ///
    /// - `purged` tombstone ⇒ [`StateRefusal::Purged`], whatever the target;
    /// - `deleting`: the `default` tenant ⇒ [`StateRefusal::Default`]; already
    ///   `deleting` ⇒ keep the mark (`Ok(false)`); otherwise ⇒ `deleting-ms=now`;
    /// - any other target once `purging-ms=` exists (even corrupt) ⇒
    ///   [`StateRefusal::Purging`]; leaving `deleting` before that mark removes
    ///   `deleting-ms=` (reverses phase A).
    pub fn change_state(
        &mut self,
        tenant: &TenantId,
        to: cairn_engine::TenantState,
        now_ms: u64,
    ) -> Result<bool, StateRefusal> {
        let was = self.state();
        let changed = self.change_state_only(tenant, to, now_ms)?;
        self.mark_lift(was, now_ms);
        Ok(changed)
    }

    /// Write the suspend/lift marks (`refused-ms=`/`lifted-ms=`) after a state
    /// change from `was`.
    ///
    /// - Entering a slot-refusing state ⇒ `refused-ms=now`, **unless** the previous spell
    ///   may still have deferred rows not yet due (`lifted-ms` + deferral tick + skew
    ///   ≥ `now`): keep the old mark so that a later leader's window covers both spells.
    /// - Leaving a slot-refusing state ⇒ `lifted-ms=now`.
    fn mark_lift(&mut self, was: cairn_engine::TenantState, now_ms: u64) {
        let (before, after) = (was.refuses_turns(), self.state().refuses_turns());
        let l = &mut self.limits;
        if !before && after {
            let pending = l.lifted_ms.is_some_and(|lifted| {
                lifted
                    .saturating_add(crate::flow::SUSPENDED_RETRY.as_millis() as u64)
                    .saturating_add(crate::flow::LIFT_SLACK.as_millis() as u64)
                    >= now_ms
            });
            if l.refused_ms.is_none() || !pending {
                l.refused_ms = Some(now_ms);
            }
        } else if before && !after {
            l.lifted_ms = Some(now_ms);
        }
    }

    /// State change rules of [`Self::change_state`], without writing the suspend marks.
    fn change_state_only(
        &mut self,
        tenant: &TenantId,
        to: cairn_engine::TenantState,
        now_ms: u64,
    ) -> Result<bool, StateRefusal> {
        use cairn_engine::TenantState as S;
        let now = self.state();
        if now == S::Purged {
            return Err(StateRefusal::Purged);
        }
        if to == S::Deleting {
            if *tenant == TenantId::default() {
                return Err(StateRefusal::Default);
            }
            if now == S::Deleting {
                return Ok(false);
            }
            self.set_state(S::Deleting);
            self.purge = PurgeMarks {
                deleting: Some(now_ms.to_string()),
                ..PurgeMarks::default()
            };
            return Ok(true);
        }
        if self.purge.purging.is_some() {
            return Err(StateRefusal::Purging);
        }
        let changed = now != to || self.purge.deleting.is_some();
        self.purge.deleting = None;
        self.set_state(to);
        Ok(changed)
    }

    /// The tenant's state: an unknown value ⇒ `Suspended`.
    pub fn state(&self) -> cairn_engine::TenantState {
        self.state_raw.as_deref().map_or(
            cairn_engine::TenantState::Active,
            cairn_engine::TenantState::parse,
        )
    }
}

type Book = BTreeMap<TenantId, TenantFlags>;

fn decode(raw: &[u8]) -> Result<Book, String> {
    let text = std::str::from_utf8(raw).map_err(|e| format!("tenants is not UTF-8: {e}"))?;
    let mut lines = text.lines();
    let header = lines.next().unwrap_or_default();
    if header != HEADER {
        if let Some(e) = cairn_store::newer_header("tenants", HEADER, header) {
            return Err(e.to_string());
        }
        return Err(format!("tenants lacks header {HEADER}"));
    }
    let mut out = Book::new();
    for line in lines.filter(|l| !l.trim().is_empty()) {
        let mut f = line.split(' ').filter(|x| !x.is_empty());
        let Some(t) = f.next().and_then(|t| TenantId::new(t).ok()) else {
            return Err(format!("tenants: corrupt line {line:?}"));
        };
        let flags = out.entry(t).or_default();
        let (mut active, mut keys) = (None, None);
        for x in f {
            if x == HASH_CHAIN {
                flags.hash_chain = true;
            } else if let Some(ms) = x.strip_prefix(SLOW_MS).and_then(|v| v.parse().ok()) {
                flags.slow_ms = Some(ms);
            } else if let Some(m) = x
                .strip_prefix(QUARANTINE)
                .and_then(crate::quarantine::Mode::parse)
            {
                flags.quarantine = Some(m);
            } else if let Some(r) = x
                .strip_prefix(QUARANTINE_RULE)
                .and_then(crate::quarantine::Rule::parse)
            {
                flags.quarantine_rule = Some(r);
            } else if let Some(g) = x.strip_prefix(IDENTITY).and_then(|v| v.parse::<u64>().ok()) {
                active = Some(g);
            } else if let Some(gs) = x.strip_prefix(IDENTITY_KEYS).and_then(parse_generations) {
                keys = Some(gs);
            } else if let Some(v) = x.strip_prefix(RETENTION_MS) {
                flags.limits.retention_ms =
                    Some(parse_retention(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_PAYLOAD) {
                flags.limits.max_payload =
                    Some(parse_max_payload(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_VALUE_BYTES) {
                flags.limits.max_value_bytes =
                    Some(parse_count_u64(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_JOURNAL) {
                flags.limits.max_journal =
                    Some(parse_max_journal(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(STATE) {
                // Any value is accepted — an unknown one means `suspended`
                // ([`TenantFlags::state`]), it does not corrupt the whole book: one line
                // from a newer binary must not make the node lose every other tenant's limits.
                flags.state_raw = Some(v.to_string());
                flags.limits.state = flags.state();
            } else if let Some(v) = x.strip_prefix(DELETING_MS) {
                flags.purge.deleting = Some(v.to_string());
            } else if let Some(v) = x.strip_prefix(PURGING_MS) {
                flags.purge.purging = Some(v.to_string());
            } else if let Some(v) = x.strip_prefix(PURGED_MS) {
                flags.purge.purged = Some(v.to_string());
            } else if let Some(ms) = x.strip_prefix(REFUSED_MS).and_then(|v| v.parse().ok()) {
                flags.limits.refused_ms = Some(ms);
            } else if let Some(ms) = x.strip_prefix(LIFTED_MS).and_then(|v| v.parse().ok()) {
                flags.limits.lifted_ms = Some(ms);
            } else if let Some(v) = x.strip_prefix(MAX_LIVE) {
                flags.limits.max_live = Some(parse_count_u64(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_DEPLOYMENTS) {
                flags.limits.max_deployments =
                    Some(parse_count_u32(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_SCHEDULES) {
                flags.limits.max_schedules =
                    Some(parse_count_u32(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_TIMERS) {
                flags.limits.max_timers = Some(parse_count_u32(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_STREAM_RAM) {
                flags.limits.max_stream_ram =
                    Some(parse_count_u64(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_STREAMS) {
                flags.limits.max_streams =
                    Some(parse_count_u32(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_STATE_KEYS) {
                flags.limits.max_state_keys =
                    Some(parse_count_u32(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_ARTIFACT_BYTES) {
                flags.limits.max_artifact_bytes =
                    Some(parse_count_u64(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(MAX_JOURNAL_BYTES) {
                flags.limits.max_journal_bytes =
                    Some(parse_count_u64(v).ok_or_else(|| strange(line, x))?);
            } else if let Some(v) = x.strip_prefix(DEPLOY_RATE) {
                flags.deploy_rate = Some(DeployRate::parse(v).ok_or_else(|| strange(line, x))?);
            } else {
                // Unknown flag: a newer binary added it, an older binary can still read.
                flags.extra.push(x.to_string());
            }
        }
        if active.is_some() || keys.is_some() {
            let active = active.unwrap_or(1);
            let mut published = keys.unwrap_or_default();
            // The signing generation is always published: a self-contradicting line
            // does not make the signing key vanish from `GET /identity`.
            published.insert(active);
            flags.identity = Some(crate::identity::Generations { active, published });
        }
    }
    Ok(out)
}

/// `1,2` ⇒ {1, 2}. Empty or a non-numeric element ⇒ `None` (unknown flag).
fn parse_generations(v: &str) -> Option<std::collections::BTreeSet<u64>> {
    let gs: Option<std::collections::BTreeSet<u64>> =
        v.split(',').map(|g| g.parse().ok()).collect();
    gs.filter(|g| !g.is_empty())
}

/// A limit with an unknown value: the whole book is corrupt (see the file's top doc).
fn strange(line: &str, x: &str) -> String {
    format!("tenants: unknown value {x:?} in line {line:?}")
}

fn encode(book: &Book) -> Vec<u8> {
    let mut out = format!("{HEADER}\n");
    for (t, f) in book.iter().filter(|(_, f)| !f.is_empty()) {
        out.push_str(t.as_str());
        if let Some(v) = &f.state_raw {
            out.push_str(&format!(" {STATE}{v}"));
        }
        for (name, v) in [
            (DELETING_MS, &f.purge.deleting),
            (PURGING_MS, &f.purge.purging),
            (PURGED_MS, &f.purge.purged),
        ] {
            if let Some(v) = v {
                out.push_str(&format!(" {name}{v}"));
            }
        }
        for (name, v) in [
            (REFUSED_MS, f.limits.refused_ms),
            (LIFTED_MS, f.limits.lifted_ms),
        ] {
            if let Some(ms) = v {
                out.push_str(&format!(" {name}{ms}"));
            }
        }
        if f.hash_chain {
            out.push(' ');
            out.push_str(HASH_CHAIN);
        }
        if let Some(ms) = f.slow_ms {
            out.push_str(&format!(" {SLOW_MS}{ms}"));
        }
        if let Some(m) = f.quarantine {
            out.push_str(&format!(" {QUARANTINE}{}", m.as_str()));
        }
        if let Some(r) = f.quarantine_rule {
            out.push_str(&format!(" {QUARANTINE_RULE}{}", r.encode()));
        }
        if let Some(g) = &f.identity {
            let keys: Vec<String> = g.published.iter().map(u64::to_string).collect();
            out.push_str(&format!(
                " {IDENTITY}{} {IDENTITY_KEYS}{}",
                g.active,
                keys.join(",")
            ));
        }
        if let Some(ms) = f.limits.retention_ms {
            out.push_str(&format!(" {RETENTION_MS}{ms}"));
        }
        if let Some(n) = f.limits.max_payload {
            out.push_str(&format!(" {MAX_PAYLOAD}{n}"));
        }
        if let Some(n) = f.limits.max_value_bytes {
            out.push_str(&format!(" {MAX_VALUE_BYTES}{n}"));
        }
        if let Some(n) = f.limits.max_journal {
            out.push_str(&format!(" {MAX_JOURNAL}{n}"));
        }
        if let Some(n) = f.limits.max_journal_bytes {
            out.push_str(&format!(" {MAX_JOURNAL_BYTES}{n}"));
        }
        if let Some(r) = f.deploy_rate {
            out.push_str(&format!(" {DEPLOY_RATE}{}", r.encode()));
        }
        let l = &f.limits;
        for (name, v) in [
            (MAX_LIVE, l.max_live),
            (MAX_DEPLOYMENTS, l.max_deployments.map(u64::from)),
            (MAX_SCHEDULES, l.max_schedules.map(u64::from)),
            (MAX_TIMERS, l.max_timers.map(u64::from)),
            (MAX_STREAMS, l.max_streams.map(u64::from)),
            (MAX_STREAM_RAM, l.max_stream_ram),
            (MAX_STATE_KEYS, l.max_state_keys.map(u64::from)),
            (MAX_ARTIFACT_BYTES, l.max_artifact_bytes),
        ] {
            if let Some(n) = v {
                out.push_str(&format!(" {name}{n}"));
            }
        }
        for x in &f.extra {
            out.push(' ');
            out.push_str(x);
        }
        out.push('\n');
    }
    out.into_bytes()
}

/// Tenant settings store + the set of chained tenants shared with the engine + the
/// slow log thresholds shared with the engine threads.
#[derive(Clone)]
pub struct TenantSettings {
    store: Option<(Arc<dyn ObjectStore>, Path)>,
    /// Prefix of the cluster — `cluster-version` for the `tenants/state` gate.
    prefix: Path,
    chained: ChainedTenants,
    slow: Option<crate::slowlog::SlowLog>,
    /// The node's identity signer: takes the per-tenant generations on every
    /// re-read.
    identity: Option<crate::identity::Identity>,
    /// Per-tenant limits shared with every range.
    limits: cairn_engine::TenantLimitsSource,
    /// The latest read saw a corrupt object and kept the old book (gauge
    /// `cairnflow_tenants_book_rejected`).
    rejected: Arc<std::sync::atomic::AtomicBool>,
    /// GCRA of `deploy-rate=`: theoretical arrival time (ms) per tenant, on
    /// this node. Not a hot path (once per `POST /deployments`).
    deploy_tat: Arc<std::sync::Mutex<BTreeMap<TenantId, u64>>>,
    /// The latest read (or the in-RAM copy when there is no store).
    book: Arc<RwLock<Book>>,
    /// Apply order: an old read that arrives late does not overwrite a newer one.
    order: Arc<crate::registry_sync::ReadOrder>,
    /// Default limits for absent fields (SaaS profile).
    defaults: cairn_engine::TenantLimit,
}

impl TenantSettings {
    /// `store` = (object store, cluster prefix); `None` = in RAM only.
    pub fn new(store: Option<(Arc<dyn ObjectStore>, &Path)>) -> Self {
        let path = |p: &Path| {
            let p = p.as_ref();
            Path::from(if p.is_empty() {
                "tenants".to_string()
            } else {
                format!("{p}/tenants")
            })
        };
        Self {
            prefix: store
                .as_ref()
                .map(|(_, p)| (*p).clone())
                .unwrap_or_default(),
            store: store.map(|(s, p)| (s, path(p))),
            chained: ChainedTenants::default(),
            slow: None,
            identity: None,
            limits: cairn_engine::TenantLimitsSource::default(),
            rejected: Arc::default(),
            deploy_tat: Arc::default(),
            book: Arc::new(RwLock::new(Book::new())),
            order: Arc::default(),
            defaults: cairn_engine::TenantLimit::default(),
        }
    }

    /// Default limits for absent fields of every tenant (SaaS profile). Set
    /// before the first book read.
    pub fn with_defaults(mut self, defaults: cairn_engine::TenantLimit) -> Self {
        self.defaults = defaults;
        self
    }

    /// The **effective** limits of `tenant`: the flags in the book, absent fields
    /// taking the node defaults.
    pub fn limits_of(&self, tenant: &TenantId) -> cairn_engine::TenantLimit {
        self.flags(tenant).limits.or_defaults(&self.defaults)
    }

    /// Per-tenant slow log thresholds that go into `slow` on every re-read.
    pub fn with_slow_log(mut self, slow: crate::slowlog::SlowLog) -> Self {
        self.slow = Some(slow);
        self
    }

    /// Per-tenant identity generations that go into `identity` on every re-read.
    pub fn with_identity(mut self, identity: crate::identity::Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Identity generation of `tenant` by the latest read.
    pub fn identity_generations(&self, tenant: &TenantId) -> crate::identity::Generations {
        self.book
            .read()
            .ok()
            .and_then(|b| b.get(tenant).and_then(|f| f.identity.clone()))
            .unwrap_or_default()
    }

    /// The set shared with the engine.
    pub fn chained(&self) -> &ChainedTenants {
        &self.chained
    }

    /// Source of per-tenant limits shared with every range.
    pub fn limits(&self) -> &cairn_engine::TenantLimitsSource {
        &self.limits
    }

    /// Let one deployment registration of `tenant` at `now_ms` through the
    /// `deploy-rate=N/S` limit (GCRA, bursts of at most `N`) — or `Err(ms to wait)`.
    /// An absent field = no limit. The limit is counted on the node that receives the
    /// request: a single-node cell; a multi-node cluster allows at most `N` × the number of nodes.
    pub fn admit_deploy(&self, tenant: &TenantId, now_ms: u64) -> Result<(), u64> {
        let Some(rate) = self.flags(tenant).deploy_rate else {
            return Ok(());
        };
        let window = u64::from(rate.per_s) * 1000;
        let interval = (window / u64::from(rate.count)).max(1);
        let tolerance = window.saturating_sub(interval);
        let Ok(mut tat) = self.deploy_tat.lock() else {
            return Ok(());
        };
        let t = tat.get(tenant).copied().unwrap_or(0).max(now_ms);
        if t.saturating_sub(tolerance) > now_ms {
            return Err(t - tolerance - now_ms);
        }
        tat.insert(tenant.clone(), t + interval);
        Ok(())
    }

    /// Every tenant with at least one limit, by the latest read.
    pub fn limited(&self) -> Vec<(TenantId, TenantFlags)> {
        self.book
            .read()
            .map(|b| {
                b.iter()
                    .filter(|(_, f)| !f.limits.is_empty() || f.deploy_rate.is_some())
                    .map(|(t, f)| (t.clone(), f.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether the `tenants/state` gate is on — reads `cluster-version`
    /// on every ask: only state-change commands (administration, rare) ask. No
    /// bucket ⇒ on.
    pub async fn state_enabled(&self) -> Result<bool, String> {
        let Some((store, _)) = &self.store else {
            return Ok(true);
        };
        cairn_store::migrate::read_cluster_version(store, &self.prefix)
            .await
            .map(|cv| cv.is_some_and(|(cv, _)| cv.tenant_state_on()))
            .map_err(|e| format!("cannot read cluster-version: {e}"))
    }

    /// Whether the `tenants/purge` gate is on — like
    /// [`Self::state_enabled`]. No bucket ⇒ on.
    pub async fn purge_enabled(&self) -> Result<bool, String> {
        let Some((store, _)) = &self.store else {
            return Ok(true);
        };
        cairn_store::migrate::read_cluster_version(store, &self.prefix)
            .await
            .map(|cv| cv.is_some_and(|(cv, _)| cv.tenant_purge_on()))
            .map_err(|e| format!("cannot read cluster-version: {e}"))
    }

    /// Tenants not in `active`: `tenant ⇒ state`, by the latest
    /// read.
    pub fn states(&self) -> BTreeMap<String, &'static str> {
        self.book
            .read()
            .map(|b| {
                b.iter()
                    .filter(|(_, f)| f.state_raw.is_some())
                    .map(|(t, f)| (t.to_string(), f.state().as_str()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The latest read saw a corrupt book (the node is keeping the old one).
    pub fn book_rejected(&self) -> bool {
        self.rejected.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Flags of `tenant` by the latest read (defaults if absent).
    pub fn flags(&self, tenant: &TenantId) -> TenantFlags {
        self.book
            .read()
            .ok()
            .and_then(|b| b.get(tenant).cloned())
            .unwrap_or_default()
    }

    fn apply(&self, book: Book) {
        self.chained.replace(
            book.iter()
                .filter(|(_, f)| f.hash_chain)
                .map(|(t, _)| t.clone())
                .collect::<HashSet<_>>(),
        );
        if let Some(id) = &self.identity {
            id.set_generations(
                book.iter()
                    .filter_map(|(t, f)| f.identity.clone().map(|g| (t.to_string(), g)))
                    .collect(),
            );
        }
        if let Some(slow) = &self.slow {
            slow.set_overrides(
                book.iter()
                    .filter_map(|(t, f)| f.slow_ms.map(|ms| (t.to_string(), ms)))
                    .collect::<HashMap<_, _>>(),
            );
        }
        self.limits.replace(
            cairn_engine::TenantLimits::new(
                book.iter().map(|(t, f)| (t.clone(), f.limits)).collect(),
            )
            .with_defaults(self.defaults),
        );
        if let Ok(mut b) = self.book.write() {
            *b = book;
        }
    }

    async fn load(&self) -> Result<(Book, Option<UpdateVersion>), String> {
        let Some((store, path)) = &self.store else {
            return Ok((
                self.book.read().map(|b| b.clone()).unwrap_or_default(),
                None,
            ));
        };
        match store.get(path).await {
            Ok(got) => {
                let v = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got.bytes().await.map_err(|e| e.to_string())?;
                // Corrupt object (corrupt line, unknown value): the node keeps the old book and
                // reports it on the gauge — fail closed. An I/O error does not: it is transient.
                use crate::metrics::tenant as m;
                match decode(&raw) {
                    Ok(book) => {
                        self.rejected
                            .store(false, std::sync::atomic::Ordering::Relaxed);
                        Ok((book, Some(v)))
                    }
                    Err(e) => {
                        self.rejected
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        m::bump(&m::BOOK_REJECTED_TOTAL);
                        tracing::error!(error = %e, "tenants book corrupt, keeping the one in use");
                        Err(e)
                    }
                }
            }
            Err(OsError::NotFound { .. }) => {
                self.rejected
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                Ok((Book::new(), None))
            }
            Err(e) => Err(format!("cannot read {path}: {e}")),
        }
    }

    /// Re-read from the bucket into the shared set — in read order: the result of a
    /// read that started before the latest apply is dropped
    /// ([`crate::registry_sync::ReadOrder`]).
    pub async fn refresh(&self) -> Result<(), String> {
        if self.store.is_none() {
            return Ok(());
        }
        let ticket = self.order.ticket();
        let (book, _) = self.load().await?;
        cairn_store::failpoint!("tenants::refresh::loaded");
        self.order.apply_if_newer(ticket, || self.apply(book));
        Ok(())
    }

    /// Change the flags of `tenant` — CAS, retried when someone else just wrote.
    pub async fn update(
        &self,
        tenant: &TenantId,
        f: impl Fn(&mut TenantFlags),
    ) -> Result<(), String> {
        self.try_update(tenant, |flags| {
            f(flags);
            Ok::<(), std::convert::Infallible>(())
        })
        .await
        .map(|r| match r {
            Ok(()) => (),
            Err(never) => match never {},
        })
    }

    /// Like [`Self::update`], but `f` may refuse on the copy just read:
    /// `Ok(Err(e))` = nothing written. Each CAS retry calls `f` again on the new
    /// copy.
    pub async fn try_update<E>(
        &self,
        tenant: &TenantId,
        f: impl Fn(&mut TenantFlags) -> Result<(), E>,
    ) -> Result<Result<(), E>, String> {
        let Some((store, path)) = &self.store else {
            let (mut book, _) = self.load().await?;
            if let Err(e) = f(book.entry(tenant.clone()).or_default()) {
                return Ok(Err(e));
            }
            self.apply(book);
            return Ok(Ok(()));
        };
        for _ in 0..8 {
            let (mut book, version) = self.load().await?;
            if let Err(e) = f(book.entry(tenant.clone()).or_default()) {
                return Ok(Err(e));
            }
            let mode = match version {
                Some(v) => PutMode::Update(v),
                None => PutMode::Create,
            };
            // Ambiguity is resolved by reading back (rule 7); a store without CAS (`--data-dir`)
            // overwrites — see `cairn_store::cas::put_resolving_or_overwrite`.
            // Two writers setting the same flag produce the same content: both `Ok` is correct.
            match put_resolving_or_overwrite(store, path, encode(&book).into(), mode).await {
                Ok(Cas::Written(_)) => {
                    // The ticket is taken **after** the write wins: no read that started before
                    // it (possibly older than the write) overwrites this copy.
                    let ticket = self.order.ticket();
                    self.order.apply_if_newer(ticket, || self.apply(book));
                    return Ok(Ok(()));
                }
                Ok(Cas::Lost) => {}
                Err(e) => return Err(format!("cannot write {path}: {e}")),
            }
        }
        Err("tenants keeps changing underfoot — retry".into())
    }

    /// Turn the hash chain on/off for `tenant`.
    pub async fn set_hash_chain(&self, tenant: &TenantId, on: bool) -> Result<(), String> {
        self.update(tenant, |f| f.hash_chain = on).await
    }

    /// Set `tenant`'s own slow log threshold (ms); `None` = back to the default.
    pub async fn set_slow_ms(&self, tenant: &TenantId, ms: Option<u64>) -> Result<(), String> {
        self.update(tenant, |f| f.slow_ms = ms).await
    }

    pub fn hash_chain(&self, tenant: &TenantId) -> bool {
        self.chained.contains(tenant)
    }

    /// `tenant`'s own quarantine mode and rule (`None` = the node's), by the latest
    /// read.
    pub fn quarantine(
        &self,
        tenant: &str,
    ) -> (
        Option<crate::quarantine::Mode>,
        Option<crate::quarantine::Rule>,
    ) {
        let Ok(t) = TenantId::new(tenant) else {
            return (None, None);
        };
        self.book
            .read()
            .ok()
            .and_then(|b| b.get(&t).map(|f| (f.quarantine, f.quarantine_rule)))
            .unwrap_or_default()
    }

    /// Every tenant with its own quarantine settings: `(tenant, mode, rule)`.
    pub fn quarantine_overrides(
        &self,
    ) -> Vec<(
        String,
        Option<crate::quarantine::Mode>,
        Option<crate::quarantine::Rule>,
    )> {
        self.book
            .read()
            .map(|b| {
                b.iter()
                    .filter(|(_, f)| f.quarantine.is_some() || f.quarantine_rule.is_some())
                    .map(|(t, f)| (t.to_string(), f.quarantine, f.quarantine_rule))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Per-tenant slow log thresholds, by the latest read.
    pub fn slow_overrides(&self) -> BTreeMap<String, u64> {
        self.book
            .read()
            .map(|b| {
                b.iter()
                    .filter_map(|(t, f)| f.slow_ms.map(|ms| (t.to_string(), ms)))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 7: the `tenants` CAS was written but the answer is an error.
    /// Must see: the admin command returns `Ok` — not a 500 for a change that has
    /// already taken effect.
    #[tokio::test]
    async fn ambiguous_write_is_resolved_by_reread() {
        use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let t = TenantSettings::new(Some((faulty.clone(), &Path::from("p"))));
            let acme = TenantId::new("acme").unwrap();
            for on in [true, false] {
                faulty.add_rule(Rule::ambiguous_put("tenants", reply));
                t.set_hash_chain(&acme, on)
                    .await
                    .unwrap_or_else(|e| panic!("{reply:?}: written but reported an error: {e}"));
                t.refresh().await.unwrap();
                assert_eq!(t.hash_chain(&acme), on, "{reply:?}");
            }
        }
    }

    /// **Apply order**: two re-reads run in parallel — the refresh round and an
    /// apply request — and finish in any order.
    ///
    /// The interleaving:
    /// 1. Node B reads the book (version 1). Peer A shares the bucket.
    /// 2. B's next first GET reads and then stalls 300 ms before returning
    ///    (`DelayResponse`): B refreshing in the background — version 1.
    /// 3. A writes version 2.
    /// 4. B refreshes once more (the apply request): version 2, applied.
    /// 5. Read (2) arrives.
    ///
    /// Must see: B is still on version 2 — a read from before the write does not overwrite the newer version.
    #[tokio::test]
    async fn stale_refresh_never_undoes_a_newer_book() {
        use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
        let mem = Arc::new(slatedb::object_store::memory::InMemory::new());
        let faulty = FaultyStore::wrap(mem.clone());
        let peer = TenantSettings::new(Some((mem.clone(), &Path::from("p"))));
        let me = TenantSettings::new(Some((faulty.clone(), &Path::from("p"))));
        let acme = TenantId::new("acme").unwrap();
        peer.set_hash_chain(&acme, true).await.unwrap();
        me.refresh().await.unwrap();
        assert!(me.hash_chain(&acme));
        faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("p/tenants".into()),
            Action::EachOnce(Box::new(Action::DelayResponse(
                std::time::Duration::from_millis(300),
            ))),
        ));
        let slow = tokio::spawn({
            let me = me.clone();
            async move { me.refresh().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        peer.set_hash_chain(&acme, false).await.unwrap();
        me.refresh().await.unwrap();
        assert!(
            !me.hash_chain(&acme),
            "the apply request sees the new version"
        );
        slow.await.unwrap().unwrap();
        assert!(
            !me.hash_chain(&acme),
            "a read from before the write overwrote a newer version already applied"
        );
    }

    /// This header is the header of the policy table.
    #[test]
    fn header_is_in_policy() {
        assert_eq!(
            cairn_store::object_policy("tenants").map(|p| p.header),
            Some(HEADER)
        );
    }

    #[test]
    fn round_trip() {
        let mut book = Book::new();
        book.insert(
            TenantId::new("acme").unwrap(),
            TenantFlags {
                hash_chain: true,
                ..Default::default()
            },
        );
        book.insert(
            TenantId::default(),
            TenantFlags {
                slow_ms: Some(250),
                ..Default::default()
            },
        );
        assert_eq!(decode(&encode(&book)).unwrap(), book);
        assert!(decode(b"junk").is_err());
    }

    #[test]
    fn isolation_flag_round_trip() {
        let raw = format!("{HEADER}\nacme quarantine=hold quarantine-rule=3/2/60 color=green\n");
        let book = decode(raw.as_bytes()).unwrap();
        let f = &book[&TenantId::new("acme").unwrap()];
        assert_eq!(f.quarantine, Some(crate::quarantine::Mode::Hold));
        assert_eq!(f.quarantine_rule.map(|r| r.encode()), Some("3/2/60".into()));
        assert_eq!(decode(&encode(&book)).unwrap(), book);
    }

    /// `identity=`/`identity-keys=` round-trip; absent ⇒ defaults; the signing
    /// generation is always in the published set; a corrupt value is an unknown flag, kept verbatim.
    #[test]
    fn identity_generations_roundtrip() {
        let raw = format!(
            "{HEADER}\nacme identity=2 identity-keys=1,2\nbeta identity=3\ngamma identity-keys=x\n"
        );
        let book = decode(raw.as_bytes()).unwrap();
        let g = book[&TenantId::new("acme").unwrap()]
            .identity
            .clone()
            .unwrap();
        assert_eq!((g.active, g.published), (2, [1, 2].into()));
        let g = book[&TenantId::new("beta").unwrap()]
            .identity
            .clone()
            .unwrap();
        assert_eq!((g.active, g.published), (3, [3].into()));
        let gamma = &book[&TenantId::new("gamma").unwrap()];
        assert!(gamma.identity.is_none());
        assert_eq!(gamma.extra, ["identity-keys=x"]);
        assert_eq!(decode(&encode(&book)).unwrap(), book);
    }

    #[test]
    fn unknown_flags_kept_verbatim() {
        let raw = format!("{HEADER}\nacme hash-chain color=green slow-ms=10\n");
        let book = decode(raw.as_bytes()).unwrap();
        let f = &book[&TenantId::new("acme").unwrap()];
        assert_eq!((f.hash_chain, f.slow_ms), (true, Some(10)));
        assert_eq!(
            String::from_utf8(encode(&book)).unwrap(),
            format!("{HEADER}\nacme hash-chain slow-ms=10 color=green\n")
        );
    }

    /// Per-tenant limits: round-trip; an unknown value corrupts the whole book;
    /// a tenant without any field has no limit.
    #[test]
    fn per_tenant_caps() {
        let raw = format!(
            "{HEADER}\nacme retention-ms=0 max-payload=1024 max-journal=50 deploy-rate=10/60\nother hash-chain\n"
        );
        let book = decode(raw.as_bytes()).unwrap();
        let acme = &book[&TenantId::new("acme").unwrap()];
        assert_eq!(
            acme.limits,
            cairn_engine::TenantLimit {
                retention_ms: Some(0),
                max_payload: Some(1024),
                max_journal: Some(50),
                ..Default::default()
            }
        );
        assert_eq!(
            acme.deploy_rate,
            Some(DeployRate {
                count: 10,
                per_s: 60
            })
        );
        assert!(book[&TenantId::new("other").unwrap()].limits.is_empty());
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);
        for bad in [
            "max-payload=1kb",
            "max-payload=0",
            "max-payload=",
            "max-journal=0",
            "max-journal=99999999999",
            "retention-ms=-1",
            "deploy-rate=10",
            "deploy-rate=0/60",
            "deploy-rate=10/0",
            "deploy-rate=a/b",
        ] {
            let raw = format!("{HEADER}\nacme hash-chain\nother {bad}\n");
            assert!(
                decode(raw.as_bytes()).is_err(),
                "{bad} must corrupt the whole book"
            );
        }
    }

    /// `state=`: verbatim round-trip; absent = `active`; an unknown value (a
    /// typo, a newer binary's state) ⇒ `suspended` — does not corrupt the whole book
    /// (other lines are still read), does not become `active`. The state goes into the
    /// engine's snapshot together with the limits.
    #[test]
    fn tenant_state() {
        use cairn_engine::TenantState as S;
        let raw = format!(
            "{HEADER}\nacme state=limited\nbeta state=suspended max-journal=5\ndelta state=Active\neps hash-chain\ngamma state=deleting\n"
        );
        let book = decode(raw.as_bytes()).unwrap();
        let st = |t: &str| book[&TenantId::new(t).unwrap()].state();
        assert_eq!(
            [st("acme"), st("beta"), st("gamma"), st("delta"), st("eps")],
            [
                S::Limited,
                S::Suspended,
                S::Deleting,
                S::Suspended,
                S::Active
            ]
        );
        assert_eq!(
            book[&TenantId::new("gamma").unwrap()].state_raw.as_deref(),
            Some("deleting")
        );
        assert_eq!(
            book[&TenantId::new("beta").unwrap()].limits.state,
            S::Suspended
        );
        assert_eq!(
            book[&TenantId::new("gamma").unwrap()].limits.state,
            S::Deleting
        );
        assert_eq!(book[&TenantId::new("eps").unwrap()].limits.state, S::Active);
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);
    }

    /// Purge marks: verbatim round-trip, including a corrupt value
    /// (does not corrupt the whole book); the state change rules of `change_state`.
    #[test]
    fn purge_marks_and_state_changes() {
        use cairn_engine::TenantState as S;
        let raw = format!(
            "{HEADER}\nacme state=deleting deleting-ms=10 purging-ms=x\nbeta state=purged deleting-ms=1 purging-ms=2 purged-ms=3\n"
        );
        let book = decode(raw.as_bytes()).unwrap();
        let acme = TenantId::new("acme").unwrap();
        let f = &book[&acme];
        assert_eq!(
            (
                f.purge.deleting_ms(),
                f.purge.purging_ms(),
                f.purge.purging.as_deref()
            ),
            (Some(10), None, Some("x"))
        );
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);

        let mut f = f.clone();
        assert_eq!(
            f.change_state(&acme, S::Suspended, 99),
            Err(StateRefusal::Purging),
            "a corrupt purging-ms is still present"
        );
        assert_eq!(f.change_state(&acme, S::Deleting, 99), Ok(false));
        let mut beta = book[&TenantId::new("beta").unwrap()].clone();
        for to in [S::Deleting, S::Active] {
            assert_eq!(
                beta.change_state(&TenantId::new("beta").unwrap(), to, 99),
                Err(StateRefusal::Purged)
            );
        }
        let mut fresh = TenantFlags::default();
        assert_eq!(
            fresh.change_state(&TenantId::default(), S::Deleting, 5),
            Err(StateRefusal::Default)
        );
        assert_eq!(fresh.change_state(&acme, S::Deleting, 5), Ok(true));
        assert_eq!(
            (fresh.state(), fresh.purge.deleting_ms()),
            (S::Deleting, Some(5))
        );
        assert_eq!(fresh.change_state(&acme, S::Deleting, 7), Ok(false));
        assert_eq!(
            fresh.purge.deleting_ms(),
            Some(5),
            "resending keeps the mark"
        );
        assert_eq!(fresh.change_state(&acme, S::Suspended, 8), Ok(true));
        assert_eq!(
            (fresh.state(), fresh.purge.is_empty()),
            (S::Suspended, true)
        );
        assert_eq!(fresh.change_state(&acme, S::Suspended, 9), Ok(false));
    }

    /// Suspend/lift marks: `change_state` writes
    /// `refused-ms=` when entering a slot-refusing state, `lifted-ms=` when leaving
    /// it; switching between two states of the same kind leaves the marks alone; a new
    /// suspension while the previous spell's rows may still be waiting keeps the old mark.
    /// Round-trip; a corrupt number is kept verbatim like an unknown flag, the book is not corrupt.
    #[test]
    fn lift_marks_follow_state_changes() {
        use cairn_engine::TenantState as S;
        let acme = TenantId::new("acme").unwrap();
        let w = (crate::flow::SUSPENDED_RETRY + crate::flow::LIFT_SLACK).as_millis() as u64;
        let marks = |f: &TenantFlags| (f.limits.refused_ms, f.limits.lifted_ms);
        let mut f = TenantFlags::default();
        assert_eq!(f.change_state(&acme, S::Limited, 5), Ok(true));
        assert_eq!(marks(&f), (None, None), "limited does not refuse slots");
        assert_eq!(f.change_state(&acme, S::Suspended, 10), Ok(true));
        assert_eq!(marks(&f), (Some(10), None));
        assert_eq!(f.change_state(&acme, S::Suspended, 11), Ok(false));
        assert_eq!(f.change_state(&acme, S::Deleting, 12), Ok(true));
        assert_eq!(
            marks(&f),
            (Some(10), None),
            "suspended ⇒ deleting does not change the marks"
        );
        assert_eq!(f.change_state(&acme, S::Active, 20), Ok(true));
        assert_eq!(marks(&f), (Some(10), Some(20)));
        // A new suspension while the previous spell's rows may still be waiting: keep the old mark.
        assert_eq!(f.change_state(&acme, S::Suspended, 20 + w), Ok(true));
        assert_eq!(marks(&f), (Some(10), Some(20)));
        assert_eq!(f.change_state(&acme, S::Limited, 30 + w), Ok(true));
        assert_eq!(marks(&f), (Some(10), Some(30 + w)));
        // The previous spell is fully over: a new mark.
        let later = 31 + 2 * w;
        assert_eq!(f.change_state(&acme, S::Suspended, later), Ok(true));
        assert_eq!(marks(&f), (Some(later), Some(30 + w)));

        let book: Book = [(acme.clone(), f.clone())].into_iter().collect();
        let raw = String::from_utf8(encode(&book)).unwrap();
        assert!(
            raw.contains(&format!("refused-ms={later}"))
                && raw.contains(&format!("lifted-ms={}", 30 + w)),
            "{raw}"
        );
        assert_eq!(decode(raw.as_bytes()).unwrap(), book);
        // A corrupt number: an unknown flag, kept verbatim, the book still reads.
        let raw = format!("{HEADER}\nacme state=suspended refused-ms=x lifted-ms=7\n");
        let book = decode(raw.as_bytes()).unwrap();
        assert_eq!(marks(&book[&acme]), (None, Some(7)));
        assert!(String::from_utf8(encode(&book))
            .unwrap()
            .contains("refused-ms=x"));
    }

    /// Resource limits: round-trip; an unknown value or
    /// `0` corrupts the whole book like the other limits.
    #[test]
    fn resource_limits() {
        let raw = format!(
            "{HEADER}\nacme max-live=100 max-deployments=3 max-schedules=5 max-timers=8 max-streams=4 max-stream-ram=65536 max-state-keys=50 max-artifact-bytes=1048576\n"
        );
        let book = decode(raw.as_bytes()).unwrap();
        let l = book[&TenantId::new("acme").unwrap()].limits;
        assert_eq!(
            (
                l.max_live,
                l.max_deployments,
                l.max_schedules,
                l.max_timers,
                l.max_streams,
                l.max_stream_ram,
                l.max_state_keys,
                l.max_artifact_bytes
            ),
            (
                Some(100),
                Some(3),
                Some(5),
                Some(8),
                Some(4),
                Some(65_536),
                Some(50),
                Some(1_048_576)
            )
        );
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);
        for bad in [
            "max-live=0",
            "max-live=x",
            "max-deployments=0",
            "max-schedules=-1",
            "max-timers=",
            "max-streams=0",
            "max-stream-ram=0",
            "max-state-keys=1k",
            "max-artifact-bytes=0",
        ] {
            let raw = format!("{HEADER}\nacme hash-chain\nother {bad}\n");
            assert!(
                decode(raw.as_bytes()).is_err(),
                "{bad} must corrupt the whole book"
            );
        }
    }

    /// `max-value-bytes=`: the logical limit per value, is a limit of the book
    /// (not an unknown field kept verbatim); `0` or an unknown value corrupts the
    /// whole book like the other limits.
    #[test]
    fn max_value_bytes_round_trips() {
        let raw = format!("{HEADER}\nacme max-payload=1048576 max-value-bytes=67108864\n");
        let book = decode(raw.as_bytes()).unwrap();
        let f = &book[&TenantId::new("acme").unwrap()];
        assert_eq!(
            (
                f.limits.max_payload,
                f.limits.max_value_bytes,
                f.extra.len()
            ),
            (Some(1_048_576), Some(64 << 20), 0),
            "max-value-bytes= is not an unknown field"
        );
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);
        for bad in [
            "max-value-bytes=0",
            "max-value-bytes=64m",
            "max-value-bytes=",
        ] {
            let raw = format!("{HEADER}\nacme hash-chain\nother {bad}\n");
            assert!(
                decode(raw.as_bytes()).is_err(),
                "{bad} must corrupt the whole book"
            );
        }
    }

    /// `max-journal-bytes=`: round-trip, next to `max-journal=`
    /// without being confused with it; `0` or an unknown value corrupts the whole book like the other limits.
    #[test]
    fn max_journal_bytes_round_trips() {
        let raw = format!("{HEADER}\nacme max-journal=50 max-journal-bytes=33554432\n");
        let book = decode(raw.as_bytes()).unwrap();
        let f = &book[&TenantId::new("acme").unwrap()];
        assert_eq!(
            (
                f.limits.max_journal,
                f.limits.max_journal_bytes,
                f.extra.len()
            ),
            (Some(50), Some(33_554_432), 0)
        );
        assert_eq!(String::from_utf8(encode(&book)).unwrap(), raw);
        for bad in ["max-journal-bytes=0", "max-journal-bytes=32m"] {
            let raw = format!("{HEADER}\nacme hash-chain\nother {bad}\n");
            assert!(
                decode(raw.as_bytes()).is_err(),
                "{bad} must corrupt the whole book"
            );
        }
    }

    /// Node default limits (SaaS profile): absent fields of a tenant in the book and
    /// tenants absent from the book both have limits — both the ingress copy
    /// (`limits_of`: deployments, schedules, `GET /tenant`) and the engine's snapshot;
    /// a present field wins.
    #[tokio::test]
    async fn defaults_fill_absent_caps() {
        let d = cairn_engine::TenantLimit {
            max_deployments: Some(20),
            max_schedules: Some(50),
            ..Default::default()
        };
        let s = TenantSettings::new(None).with_defaults(d);
        let acme = TenantId::new("acme").unwrap();
        let other = TenantId::new("other").unwrap();
        s.update(&acme, |f| f.limits.max_schedules = Some(3))
            .await
            .unwrap();
        let a = s.limits_of(&acme);
        assert_eq!((a.max_deployments, a.max_schedules), (Some(20), Some(3)));
        assert_eq!(s.limits_of(&other), d);
        let snap = s.limits().snapshot();
        assert_eq!(snap.of(&acme).max_deployments, Some(20));
        assert_eq!(snap.of(&other).max_schedules, Some(50));
    }

    /// `deploy-rate=N/S`: a burst of N, then one every S/N seconds; other tenants and
    /// tenants without the field are not limited.
    #[tokio::test]
    async fn deploy_rate_gcra() {
        let s = TenantSettings::new(None);
        let acme = TenantId::new("acme").unwrap();
        s.update(&acme, |f| f.deploy_rate = DeployRate::parse("2/60"))
            .await
            .unwrap();
        let t0 = 1_000_000;
        assert_eq!(s.admit_deploy(&acme, t0), Ok(()));
        assert_eq!(s.admit_deploy(&acme, t0), Ok(()));
        assert_eq!(s.admit_deploy(&acme, t0), Err(30_000));
        assert_eq!(s.admit_deploy(&acme, t0 + 29_999), Err(1));
        assert_eq!(s.admit_deploy(&acme, t0 + 30_000), Ok(()));
        let other = TenantId::new("other").unwrap();
        for _ in 0..10 {
            assert_eq!(s.admit_deploy(&other, t0), Ok(()));
        }
    }

    #[tokio::test]
    async fn changing_one_flag_leaves_the_other_alone() {
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let slow = crate::slowlog::SlowLog::default();
        let s = TenantSettings::new(Some((store, &Path::from("")))).with_slow_log(slow.clone());
        let acme = TenantId::new("acme").unwrap();
        s.set_hash_chain(&acme, true).await.unwrap();
        s.set_slow_ms(&acme, Some(70)).await.unwrap();
        assert!(s.hash_chain(&acme));
        assert_eq!(slow.threshold("acme"), std::time::Duration::from_millis(70));
        s.set_hash_chain(&acme, false).await.unwrap();
        assert_eq!(s.slow_overrides().get("acme"), Some(&70));
        s.set_slow_ms(&acme, None).await.unwrap();
        assert!(s.slow_overrides().is_empty());
    }
}
