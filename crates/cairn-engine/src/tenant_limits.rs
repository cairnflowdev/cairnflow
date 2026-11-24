// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Per-tenant limits that the engine enforces.
//!
//! The source is the cluster's `tenants` registry
//! (`cairn-server/src/tenants.rs`); the node re-reads it periodically and
//! replaces the snapshot in [`TenantLimitsSource`]. The engine does **not**
//! read the source on the turn path: each range keeps its own snapshot
//! ([`TenantLimits`] behind an `Arc`), taken at open and swapped on the
//! background tick when the source's generation changes — one atomic read per
//! tick, no shared lock on the commit path (the same reason as `set_rules`:
//! the commit path is hot and must not contend on a node-wide lock).
//!
//! Absent tenant or absent field = no limit: self-hosting is unchanged.

use {
    cairn_store::TenantId,
    std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

/// The outcome code of an invocation when an SDK frame written to the journal
/// (a command, a `ctx.run` result, the input of `ContinueAsNew`) exceeds
/// `max-payload=` — the same number as ingress's `413`, the message starts with
/// [`PAYLOAD_TOO_LARGE_CODE`].
pub const PAYLOAD_TOO_LARGE: u32 = 413;
/// Stable string code of the payload limit (ingress `code`, start of the outcome message).
pub const PAYLOAD_TOO_LARGE_CODE: &str = "payload_too_large";
/// The outcome code when an invocation's journal reaches `max-journal=`.
pub const JOURNAL_TOO_LONG: u32 = 578;
/// Stable string code of the journal limit (start of the outcome message).
pub const JOURNAL_TOO_LONG_CODE: &str = "journal_too_long";
/// The outcome code when an SDK command exceeds a tenant resource limit:
/// pending timers per invocation, state keys per object, artifact bytes. The
/// message starts with the limit's string code ([`TIMERS_LIMIT_CODE`],
/// [`STATE_KEYS_LIMIT_CODE`], [`ARTIFACTS_LIMIT_CODE`]).
pub const TENANT_LIMIT: u32 = 579;
/// `max-timers=`: the number of pending timers of one invocation.
pub const TIMERS_LIMIT_CODE: &str = "timers_limit";
/// `max-state-keys=`: the number of state keys of one object.
pub const STATE_KEYS_LIMIT_CODE: &str = "state_keys_limit";
/// `max-artifact-bytes=`: the tenant's total artifact bytes.
pub const ARTIFACTS_LIMIT_CODE: &str = "artifacts_limit";
/// `max-value-bytes=`: the logical size of one value.
pub const VALUE_BYTES_LIMIT_CODE: &str = "value_bytes_limit";
/// The outcome code when an invocation's total journal bytes reach
/// `max-journal-bytes=`. Unlike every other limit: the invocation is
/// **`paused`**, not `failed` — a long journal is the sign of a program that
/// needs fixing (large values go through `ctx.artifact`), not of a broken run;
/// the operator can raise the limit and then `resume`. The message starts with
/// [`JOURNAL_BYTES_LIMIT_CODE`].
pub const JOURNAL_BYTES_LIMIT: u32 = 580;
/// Stable string code of the journal-bytes limit (start of the outcome message).
pub const JOURNAL_BYTES_LIMIT_CODE: &str = "journal_bytes_limit";

/// The state of a tenant.
///
/// - `Limited` (spending cap reached, free tier exhausted): all **new** work is
///   refused (`429 tenant_limited`), schedules stop firing; running work runs to
///   its outcome.
/// - `Suspended` (abuse, overdue payment, operator command): like `Limited`,
///   **plus** the scheduler grants no permit to any invocation of the tenant —
///   the engine does not call out to its endpoints; data and position are kept
///   as they are, and unsuspending lets it continue.
/// - `Deleting`: like `Suspended`, **plus** every key, cookie and SSO session of
///   the tenant is refused ([`Self::refuses_access`]). The first phase (7 days)
///   can be reverted to `suspended`; after the registry's `purging-ms=` instant
///   it cannot.
/// - `Purged`: a permanent tombstone after the purge — like `Deleting`, the id
///   is never reissued.
///
/// An unknown value in the registry (a typo, or a state from a newer binary) is
/// `Suspended` — fail closed ([`TenantState::parse`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum TenantState {
    #[default]
    Active,
    Limited,
    Suspended,
    Deleting,
    Purged,
}

impl TenantState {
    /// The value of the `state=` field. Unknown ⇒ `Suspended`, not `Active`.
    pub fn parse(v: &str) -> Self {
        match v {
            "active" => Self::Active,
            "limited" => Self::Limited,
            "deleting" => Self::Deleting,
            "purged" => Self::Purged,
            _ => Self::Suspended,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Limited => "limited",
            Self::Suspended => "suspended",
            Self::Deleting => "deleting",
            Self::Purged => "purged",
        }
    }

    /// **New** work of the tenant is refused (ingress, schedules, restart).
    pub fn refuses_new_work(self) -> bool {
        self != Self::Active
    }

    /// The scheduler grants no permits to the tenant.
    pub fn refuses_turns(self) -> bool {
        matches!(self, Self::Suspended | Self::Deleting | Self::Purged)
    }

    /// Every key, cookie and SSO session **of** the tenant is refused (`403
    /// tenant_deleting`) — the data can no longer be read while awaiting
    /// deletion. Cluster keys (`cluster-admin`, `provision`) can still read the
    /// state.
    pub fn refuses_access(self) -> bool {
        matches!(self, Self::Deleting | Self::Purged)
    }

    /// The tenant is being or has been deleted (`deleting`, `purged`).
    pub fn is_deleting(self) -> bool {
        self.refuses_access()
    }
}

/// Why new work of a tenant is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantRefusal {
    /// `limited`/`suspended`.
    State(TenantState),
    /// The number of live invocations has reached `max-live=` (a soft limit).
    Live { limit: u64 },
}

/// The limits of one tenant. `None` = no limit (or: per cluster).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TenantLimit {
    /// How long to keep ended invocations — overrides `--retention-ms`, loses
    /// to the handler's `journalRetention`. `Some(0)` = keep forever, like the
    /// cluster flag.
    pub retention_ms: Option<u64>,
    /// Maximum bytes of an `/invoke`/`/send` body and of every SDK frame written
    /// to the journal (`engine/step.rs::oversized_frame`). Applies only to the
    /// **inline** part of a frame (ref + tail when sealed).
    pub max_payload: Option<u64>,
    /// Maximum **logical** bytes of one value the SDK writes to the journal
    /// (`SetState`, `Output`, a `ctx.run` result). Exceeded ⇒ the invocation is
    /// `failed` with code [`TENANT_LIMIT`], the message starting with
    /// [`VALUE_BYTES_LIMIT_CODE`].
    pub max_value_bytes: Option<u64>,
    /// Maximum number of journal entries per invocation, counting `Input`.
    pub max_journal: Option<u32>,
    /// Maximum total journal bytes per invocation (encoded bytes of every
    /// entry, counting `Input`). The entry that would push the total over the
    /// limit is not written; the invocation is `paused` with code
    /// [`JOURNAL_BYTES_LIMIT`] (`engine/step.rs`).
    pub max_journal_bytes: Option<u64>,
    /// The tenant's state. Default `Active`.
    pub state: TenantState,
    /// Maximum live invocations (running, sleeping, waiting, `paused`) of the
    /// tenant — a **soft** limit, counted by the query counters of every range
    /// ([`TenantUsage::live`]). Only blocks new work at ingress, `restart`,
    /// schedules.
    pub max_live: Option<u64>,
    /// Maximum number of deployments of the tenant (`POST /deployments`).
    pub max_deployments: Option<u32>,
    /// Maximum number of schedules of the tenant (`PUT /schedules/{n}` creating a new one).
    pub max_schedules: Option<u32>,
    /// Maximum number of pending timers of one invocation.
    pub max_timers: Option<u32>,
    /// Maximum number of streams of one invocation (a new stream over the limit
    /// is dropped, like the hub's hard limit).
    pub max_streams: Option<u32>,
    /// Maximum number of state keys of one object.
    pub max_state_keys: Option<u32>,
    /// Maximum stream RAM bytes of the tenant on **each node**. `None` = the
    /// node's default (1/8 of stream RAM; `default`: the whole limit).
    pub max_stream_ram: Option<u64>,
    /// Maximum total artifact bytes of the tenant — one counting tick late
    /// ([`TenantUsage::artifact_bytes`]).
    pub max_artifact_bytes: Option<u64>,
    /// The instant of the most recent permit-refusal episode (`refused-ms=`):
    /// when the tenant entered a permit-refusing state — or the instant of the
    /// previous episode, when that episode's timed rows may still be waiting.
    /// Not a limit: the range leader reads it to pull in timed rows that were
    /// held back during the suspension ([`Self::lifted_ms`]).
    pub refused_ms: Option<u64>,
    /// When the tenant last left a permit-refusing state (`lifted-ms=`). A
    /// leader that comes up after the unsuspension — never having seen the
    /// tenant suspended — reads these two instants to fire the suspension
    /// episode's timed rows right away instead of waiting for the 15-minute
    /// timer tick.
    pub lifted_ms: Option<u64>,
}

/// Usage counts of one tenant, recounted periodically by the node's background
/// loop — not durable, not on the commit path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TenantUsage {
    /// `active` + `paused` class invocations over every range that answered (a
    /// range rebuilding its counters is missing — let it pass).
    pub live: u64,
    /// Total bytes under `artifacts/<tenant>/` at the last LIST.
    pub artifact_bytes: u64,
    /// The tenant's number of schedules over every range that answered, at the
    /// node's last count: `PUT /schedules` reads this instead of listing the
    /// whole cluster every time.
    pub schedules: u64,
}

impl TenantLimit {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Absent limit fields are taken from `defaults` (the SaaS profile): a
    /// shared cell has no tenant that is "unlimited" because the registry lacks
    /// a line. The state and the suspend/unsuspend instants are not touched.
    pub fn or_defaults(self, d: &TenantLimit) -> TenantLimit {
        TenantLimit {
            retention_ms: self.retention_ms.or(d.retention_ms),
            max_payload: self.max_payload.or(d.max_payload),
            max_value_bytes: self.max_value_bytes.or(d.max_value_bytes),
            max_journal: self.max_journal.or(d.max_journal),
            max_journal_bytes: self.max_journal_bytes.or(d.max_journal_bytes),
            state: self.state,
            max_live: self.max_live.or(d.max_live),
            max_deployments: self.max_deployments.or(d.max_deployments),
            max_schedules: self.max_schedules.or(d.max_schedules),
            max_timers: self.max_timers.or(d.max_timers),
            max_streams: self.max_streams.or(d.max_streams),
            max_state_keys: self.max_state_keys.or(d.max_state_keys),
            max_stream_ram: self.max_stream_ram.or(d.max_stream_ram),
            max_artifact_bytes: self.max_artifact_bytes.or(d.max_artifact_bytes),
            refused_ms: self.refused_ms,
            lifted_ms: self.lifted_ms,
        }
    }
}

/// A snapshot of the limits of every tenant that has limits, plus the latest usage counts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TenantLimits {
    limits: BTreeMap<TenantId, TenantLimit>,
    usage: BTreeMap<TenantId, TenantUsage>,
    /// Default limits for absent fields (the SaaS profile); empty = absent
    /// means unlimited (self-hosting).
    defaults: TenantLimit,
}

impl TenantLimits {
    pub fn new(limits: BTreeMap<TenantId, TenantLimit>) -> Self {
        Self {
            limits: limits.into_iter().filter(|(_, l)| !l.is_empty()).collect(),
            usage: BTreeMap::new(),
            defaults: TenantLimit::default(),
        }
    }

    /// Like [`Self::new`], with default limits for absent fields (the SaaS profile).
    pub fn with_defaults(mut self, defaults: TenantLimit) -> Self {
        for l in self.limits.values_mut() {
            *l = l.or_defaults(&defaults);
        }
        self.defaults = defaults;
        self
    }

    pub fn of(&self, tenant: &TenantId) -> TenantLimit {
        self.limits.get(tenant).copied().unwrap_or(self.defaults)
    }

    /// Like [`Self::new`], with usage counts (tests, and the source's snapshot).
    pub fn with_usage(mut self, usage: BTreeMap<TenantId, TenantUsage>) -> Self {
        self.usage = usage;
        self
    }

    /// The latest usage counts of `tenant` (none = 0).
    pub fn usage(&self, tenant: &TenantId) -> TenantUsage {
        self.usage.get(tenant).copied().unwrap_or_default()
    }

    /// Every tenant with limits, in order.
    pub fn limited(&self) -> impl Iterator<Item = (&TenantId, &TenantLimit)> {
        self.limits.iter()
    }

    /// Whether **new** work of `tenant` is refused: the state first, then
    /// `max-live=` against the latest count.
    pub fn refusal(&self, tenant: &TenantId) -> Option<TenantRefusal> {
        let l = self.of(tenant);
        if l.state.refuses_new_work() {
            return Some(TenantRefusal::State(l.state));
        }
        l.max_live
            .filter(|max| self.usage(tenant).live >= *max)
            .map(|limit| TenantRefusal::Live { limit })
    }

    /// An `ArtifactPut` of `tenant` is refused: the total bytes have reached
    /// `max-artifact-bytes=` (one counting tick late).
    pub fn artifacts_full(&self, tenant: &TenantId) -> Option<u64> {
        self.of(tenant)
            .max_artifact_bytes
            .filter(|max| self.usage(tenant).artifact_bytes >= *max)
    }

    /// Every tenant the scheduler grants no permits to (`Suspended`, `Deleting`,
    /// `Purged`).
    pub fn turns_refused(&self) -> impl Iterator<Item = &TenantId> {
        self.limits
            .iter()
            .filter(|(_, l)| l.state.refuses_turns())
            .map(|(t, _)| t)
    }

    /// The tenant's retention in the sense of `--retention-ms`: `Some(Some(ttl))`
    /// overrides the cluster, `Some(None)` = keep forever, `None` = per cluster.
    pub fn retention(&self, tenant: &TenantId) -> Option<Option<u64>> {
        self.of(tenant)
            .retention_ms
            .map(|ms| (ms > 0).then_some(ms))
    }
}

/// The source shared between the node (the replacer) and every range (the snapshot takers).
#[derive(Clone, Default)]
pub struct TenantLimitsSource {
    generation: Arc<AtomicU64>,
    current: Arc<RwLock<Arc<TenantLimits>>>,
}

impl TenantLimitsSource {
    /// The current generation: changes every time [`Self::replace`] swaps in a different snapshot.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> Arc<TenantLimits> {
        self.current
            .read()
            .map(|c| Arc::clone(&c))
            .unwrap_or_default()
    }

    /// Replaces the **limits** (keeping the existing usage counts); identical
    /// to the current one ⇒ the generation is not bumped (the refresh loop runs
    /// every 30 s, ranges need not copy what has not changed).
    pub fn replace(&self, limits: TenantLimits) {
        if let Ok(mut c) = self.current.write() {
            let next = TenantLimits {
                limits: limits.limits,
                usage: c.usage.clone(),
                defaults: limits.defaults,
            };
            if **c != next {
                *c = Arc::new(next);
                self.generation.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    /// Replaces the **usage counts** (keeping the limits) — the node's counting
    /// loop. Unchanged ⇒ the generation is not bumped.
    pub fn replace_usage(&self, usage: BTreeMap<TenantId, TenantUsage>) {
        if let Ok(mut c) = self.current.write() {
            if c.usage != usage {
                *c = Arc::new(TenantLimits {
                    limits: c.limits.clone(),
                    usage,
                    defaults: c.defaults,
                });
                self.generation.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> TenantId {
        TenantId::new(s).unwrap()
    }

    #[test]
    fn absent_means_no_limit_and_retention_0_is_forever() {
        let l = TenantLimits::new(BTreeMap::from([
            (
                t("a"),
                TenantLimit {
                    retention_ms: Some(0),
                    ..Default::default()
                },
            ),
            (
                t("b"),
                TenantLimit {
                    retention_ms: Some(5),
                    max_payload: Some(10),
                    max_journal: None,
                    ..Default::default()
                },
            ),
            (t("c"), TenantLimit::default()),
        ]));
        assert_eq!(l.retention(&t("a")), Some(None));
        assert_eq!(l.retention(&t("b")), Some(Some(5)));
        assert_eq!(l.retention(&t("z")), None);
        assert_eq!(l.of(&t("c")), TenantLimit::default());
        assert_eq!(
            l.limited().count(),
            2,
            "an unlimited tenant is not kept in the snapshot"
        );
    }

    /// Default limits (the SaaS profile): absent fields of a tenant in the
    /// registry and every field of a tenant missing from the registry take the
    /// default; present fields win; the state is unchanged; replacing through
    /// the source keeps the defaults.
    #[test]
    fn defaults_fill_absent_caps() {
        let d = TenantLimit {
            max_live: Some(1000),
            max_journal_bytes: Some(32 << 20),
            ..Default::default()
        };
        let l = TenantLimits::new(BTreeMap::from([(
            t("a"),
            TenantLimit {
                max_live: Some(5),
                state: TenantState::Limited,
                ..Default::default()
            },
        )]))
        .with_defaults(d);
        let a = l.of(&t("a"));
        assert_eq!(
            (a.max_live, a.max_journal_bytes, a.state),
            (Some(5), Some(32 << 20), TenantState::Limited)
        );
        assert_eq!(l.of(&t("z")), d, "tenant missing from the registry");
        let (_, la) = l.limited().find(|(x, _)| **x == t("a")).unwrap();
        assert_eq!(la.max_journal_bytes, Some(32 << 20), "counting sees it");
        assert_eq!(
            TenantLimit::default().or_defaults(&d),
            d,
            "or_defaults fills every absent field"
        );
        let s = TenantLimitsSource::default();
        s.replace(l);
        assert_eq!(s.snapshot().of(&t("z")).max_live, Some(1000));
    }

    /// Usage counts and limits are replaced independently: replacing one keeps
    /// the other; counts changed ⇒ the generation changes (ranges copy again),
    /// unchanged ⇒ it does not.
    #[test]
    fn counts_and_limits_change_independently() {
        let s = TenantLimitsSource::default();
        let lim = TenantLimits::new(BTreeMap::from([(
            t("a"),
            TenantLimit {
                max_live: Some(3),
                ..Default::default()
            },
        )]));
        s.replace(lim.clone());
        let g0 = s.generation();
        let usage = BTreeMap::from([(
            t("a"),
            TenantUsage {
                live: 2,
                artifact_bytes: 7,
                schedules: 1,
            },
        )]);
        s.replace_usage(usage.clone());
        assert_eq!(s.generation(), g0 + 1);
        assert_eq!(s.snapshot().usage(&t("a")).live, 2);
        s.replace_usage(usage);
        assert_eq!(s.generation(), g0 + 1, "counts unchanged");
        s.replace(lim);
        assert_eq!(
            s.snapshot().usage(&t("a")).artifact_bytes,
            7,
            "replacing limits keeps the counts"
        );
        assert_eq!(s.snapshot().of(&t("a")).max_live, Some(3));
    }

    /// `deleting`/`purged`: refuse new work, turns and access; `suspended` does
    /// not block access; an unknown value is still `suspended`.
    #[test]
    fn deleting_and_purged_refuse_access() {
        use TenantState as S;
        for (raw, s) in [
            ("deleting", S::Deleting),
            ("purged", S::Purged),
            ("archived", S::Suspended),
        ] {
            assert_eq!(S::parse(raw), s, "{raw}");
        }
        for s in [S::Deleting, S::Purged] {
            assert_eq!(S::parse(s.as_str()), s);
            assert!(s.refuses_new_work() && s.refuses_turns() && s.refuses_access());
        }
        for s in [S::Active, S::Limited, S::Suspended] {
            assert!(!s.refuses_access(), "{s:?}");
        }
        let l = TenantLimits::new(BTreeMap::from([(
            t("a"),
            TenantLimit {
                state: S::Deleting,
                ..Default::default()
            },
        )]));
        assert_eq!(l.turns_refused().collect::<Vec<_>>(), [&t("a")]);
    }

    #[test]
    fn generation_changes_only_when_snapshot_changes() {
        let s = TenantLimitsSource::default();
        let g0 = s.generation();
        s.replace(TenantLimits::default());
        assert_eq!(s.generation(), g0);
        let l = TenantLimits::new(BTreeMap::from([(
            t("a"),
            TenantLimit {
                max_journal: Some(3),
                ..Default::default()
            },
        )]));
        s.replace(l.clone());
        assert_eq!(s.generation(), g0 + 1);
        s.replace(l.clone());
        assert_eq!(s.generation(), g0 + 1);
        assert_eq!(*s.snapshot(), l);
    }
}
