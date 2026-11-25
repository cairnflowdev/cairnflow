// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Rule book: concurrency caps by `(scope, l1, l2)` and **rate** caps (CU/s) by scope or
//! by tenant, living on the object store.
//!
//! # Why there is no metadata store
//!
//! A cluster-wide metadata store is the usual way to distribute a rule book. Cairn has
//! none, and **must not grow one** (every system with its own disk added is one more system
//! the customer has to operate). Instead it uses what cairn already has and what has
//! already passed the D12 gate: an object store with conditional PUT, the same mechanism
//! [`crate::lease`] relies on.
//!
//! The rule book is **one object** `{prefix}/rules`, written by CAS on the etag, and each
//! partition leader re-reads it on its tick.
//!
//! # A consequence that must be in the API contract
//!
//! **Rule changes take effect with a delay**, about one poll period. Not a bug, a choice:
//! the rule book changes a few times a day while the slot-acquisition path runs a few
//! thousand times a second. Forcing immediate consistency here would pay the price in the
//! wrong place.
//!
//! # A lesson from leases, carried over intact
//!
//! An S3 etag is a **content hash**, so a write that does not change any byte is a write
//! that does not change the etag. That is why readers compare [`RuleBook::version`] — a
//! monotonically increasing number *inside* the content — and not the etag.
//!
//! # The `/2` layout
//!
//! ```text
//! cairnflow-rules/2
//! version=<N>
//! rule\t<pattern>\t<concurrency>\t<rate>\t<burst>\t<disabled>\t<modified_ms>\t<description>
//! tenant\t<tenant>\t<rate>\t<burst>\t<disabled>\t<modified_ms>\t<description>
//! ```
//!
//! `concurrency`, `rate`: a number, `unlimited`, or `-` (**unset**). `burst`: a number or
//! `-` (default = `rate`). `rate` is in CU/s ([`crate`] does not know CUs; the definition
//! lives in `cairn-server::metering`).
//!
//! **Each attribute has its own winner.** A counter's concurrency cap belongs to the most
//! specific rule *that sets concurrency*; the rate cap belongs to the most specific rule
//! *that sets a rate*. In `/1` every rule sets concurrency (a rule created without saying
//! anything carries an explicit `unlimited`), so reading `/1` under this rule gives exactly
//! the old meaning — the `rules/2` migration step writes `-` into the two new columns and
//! changes nothing else. A rule newly created in `/2` without a concurrency has concurrency
//! **unset**, not `unlimited`: adding a rate cap for `acme` must not silently remove the
//! concurrency cap that `*` applies to it.
//!
//! A `tenant` line sets a rate cap for **a whole tenant** (every scope, every range). It is
//! the only thing in the rule book about one specific tenant: pattern rules apply to every
//! tenant, each tenant with its own budget.
//!
//! `/1` is still readable (the migration runner reads it; and a bucket that has not been
//! migrated never has a new node running on it before the runner). This binary always
//! writes `/2`. A binary that only knows `/1` stops loudly on `/2` ([`crate::format`]).

use {
    crate::{
        error::{Result, StoreError},
        scope::{CounterPath, IdentError, RulePattern, TenantId},
    },
    slatedb::object_store::{
        path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    },
    std::{collections::BTreeMap, sync::Arc},
};

pub(crate) const HEADER: &str = "cairnflow-rules/2";
/// The layout before the `rules/2` migration step: no `rate`/`burst`/`tenant`.
const HEADER_V1: &str = "cairnflow-rules/1";

/// The cap of a counter (concurrency) or of a bucket (rate, CU/s).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// No limit — the same meaning as "no rule matches", but declared explicitly. It exists
    /// so operators can *turn off* a cap without deleting the rule and losing its
    /// description.
    Unlimited,
    Max(u32),
}

impl Limit {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "unlimited" => Some(Self::Unlimited),
            n => n.parse().ok().map(Self::Max),
        }
    }
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => f.write_str("unlimited"),
            Self::Max(n) => write!(f, "{n}"),
        }
    }
}

/// The "unset" column of the `/2` layout.
const UNSET: &str = "-";

fn opt_limit(s: &str) -> Option<Option<Limit>> {
    if s == UNSET {
        Some(None)
    } else {
        Limit::parse(s).map(Some)
    }
}

fn show_limit(l: Option<Limit>) -> String {
    l.map_or_else(|| UNSET.to_string(), |l| l.to_string())
}

fn opt_u32(s: &str) -> Option<Option<u32>> {
    if s == UNSET {
        Some(None)
    } else {
        s.parse().ok().map(Some)
    }
}

fn show_u32(n: Option<u32>) -> String {
    n.map_or_else(|| UNSET.to_string(), |n| n.to_string())
}

/// The effective rate cap: `per_sec` CU per second, accumulating at most `burst` CU.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    pub per_sec: u32,
    pub burst: u32,
}

impl Rate {
    /// `rate` set + `burst` (absent ⇒ equal to `rate`). `Unlimited` or `0`/s ⇒ `None`: no
    /// bucket at all. (`0`/s literally means "never run" — blocking a tenant has another
    /// path; a rate cap of 0 would only be a way to silently hang all its invocations.)
    fn of(rate: Limit, burst: Option<u32>) -> Option<Rate> {
        match rate {
            Limit::Max(n) if n > 0 => Some(Rate {
                per_sec: n,
                burst: burst.unwrap_or(n).max(1),
            }),
            _ => None,
        }
    }
}

/// A rule. `disabled` is a **soft flag** — off without losing the definition, because the
/// most expensive part of a rule is often the `description` explaining why it exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub pattern: RulePattern,
    /// `None` = unset: counters matching this rule take the cap of a less specific rule
    /// (see the module header).
    pub concurrency: Option<Limit>,
    /// Rate cap, CU/s, for **each** `(tenant, counter)` matching the rule. `None` = unset.
    pub rate: Option<Limit>,
    /// The most CU that can accumulate; `None` = equal to `rate`.
    pub burst: Option<u32>,
    pub description: String,
    pub disabled: bool,
    pub modified_ms: u64,
}

/// The rate cap of **a whole tenant**, over every scope and every range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantRule {
    pub tenant: TenantId,
    pub rate: Option<Limit>,
    pub burst: Option<u32>,
    pub description: String,
    pub disabled: bool,
    pub modified_ms: u64,
}

/// One rule edit: a `None` field is **left untouched**; a field set in [`RuleEdit::clear`]
/// is **reset** to unset.
#[derive(Clone, Debug, Default)]
pub struct RuleEdit {
    pub concurrency: Option<Limit>,
    pub rate: Option<Limit>,
    pub burst: Option<u32>,
    pub description: Option<String>,
    pub disabled: Option<bool>,
    pub clear: RuleClear,
}

/// Caps reset to "unset" — the counter takes the cap of a less specific rule. Different
/// from `Limit::Unlimited` (an **explicitly** removed cap). Applied after the set part of
/// the same edit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuleClear {
    pub concurrency: bool,
    pub rate: bool,
    pub burst: bool,
}

/// A warning that must be printed **right where people type the command**, not only in
/// the docs.
///
/// This is the number one misunderstanding about flow control, and it fails in the
/// expensive direction: an operator sets `*` believing they just bought one shared cap for
/// the whole system.
pub const STAR_WARNING: &str = "`*` is NOT a shared pool: this rule gives EACH scope its own \
budget. Two scopes under the same `*` rule with concurrency=1000 can run 2000 invocations at once.";

/// The whole rule book, keyed by pattern text.
///
/// `BTreeMap` rather than `HashMap`: the serialisation order must be **deterministic**,
/// otherwise two writes of the same logical content would give two different byte
/// streams, and readers comparing the etag (at the lower layer) would see a change where
/// there is none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuleBook {
    /// Increases monotonically on every write. **This** is what readers compare, not the
    /// etag.
    pub version: u64,
    rules: BTreeMap<String, Rule>,
    tenants: BTreeMap<String, TenantRule>,
}

/// Forces tabs and newlines into spaces — they are the layout's separators.
fn clean(d: String) -> String {
    d.replace(['\t', '\n', '\r'], " ")
}

impl RuleBook {
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty() && self.tenants.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Rule> {
        self.rules.values()
    }

    pub fn tenants(&self) -> impl Iterator<Item = &TenantRule> {
        self.tenants.values()
    }

    pub fn get(&self, pattern: &str) -> Option<&Rule> {
        self.rules.get(pattern)
    }

    pub fn get_tenant(&self, tenant: &str) -> Option<&TenantRule> {
        self.tenants.get(tenant)
    }

    /// Adds or edits the concurrency cap / description / disabled flag of a rule — see
    /// [`Self::apply`].
    pub fn set(
        &mut self,
        pattern: &str,
        concurrency: Option<Limit>,
        description: Option<String>,
        disabled: Option<bool>,
        now_ms: u64,
    ) -> std::result::Result<&Rule, IdentError> {
        self.apply(
            pattern,
            RuleEdit {
                concurrency,
                description,
                disabled,
                ..Default::default()
            },
            now_ms,
        )
    }

    /// Adds or edits a rule. **Idempotent, and merging rather than blank overwriting**:
    /// `set --disabled` must not delete the `description`, and `set --description` must not
    /// silently reopen a disabled rule.
    pub fn apply(
        &mut self,
        pattern: &str,
        edit: RuleEdit,
        now_ms: u64,
    ) -> std::result::Result<&Rule, IdentError> {
        let parsed = RulePattern::parse(pattern)?;
        let key = parsed.to_string();
        let entry = self.rules.entry(key.clone()).or_insert_with(|| Rule {
            pattern: parsed,
            concurrency: None,
            rate: None,
            burst: None,
            description: String::new(),
            disabled: false,
            modified_ms: now_ms,
        });
        if let Some(c) = edit.concurrency {
            entry.concurrency = Some(c);
        }
        if let Some(r) = edit.rate {
            entry.rate = Some(r);
        }
        if let Some(b) = edit.burst {
            entry.burst = Some(b);
        }
        if let Some(d) = edit.description {
            // Tabs and newlines are separators of the durable format below. Instead of
            // refusing (a description with a tab is a harmless user mistake), force them
            // into spaces at **the entry point**, once.
            entry.description = clean(d);
        }
        if let Some(x) = edit.disabled {
            entry.disabled = x;
        }
        if edit.clear.concurrency {
            entry.concurrency = None;
        }
        if edit.clear.rate {
            entry.rate = None;
        }
        if edit.clear.burst {
            entry.burst = None;
        }
        entry.modified_ms = now_ms;
        Ok(self.rules.get(&key).expect("just inserted"))
    }

    /// Adds or edits the rate cap of a whole tenant. Merges like [`Self::apply`];
    /// `edit.concurrency` has no meaning here and is ignored (the HTTP layer refuses it
    /// before it gets here).
    pub fn apply_tenant(
        &mut self,
        tenant: &str,
        edit: RuleEdit,
        now_ms: u64,
    ) -> std::result::Result<&TenantRule, IdentError> {
        let id = TenantId::new(tenant)?;
        let key = id.to_string();
        let entry = self
            .tenants
            .entry(key.clone())
            .or_insert_with(|| TenantRule {
                tenant: id,
                rate: None,
                burst: None,
                description: String::new(),
                disabled: false,
                modified_ms: now_ms,
            });
        if let Some(r) = edit.rate {
            entry.rate = Some(r);
        }
        if let Some(b) = edit.burst {
            entry.burst = Some(b);
        }
        if let Some(d) = edit.description {
            entry.description = clean(d);
        }
        if let Some(x) = edit.disabled {
            entry.disabled = x;
        }
        if edit.clear.rate {
            entry.rate = None;
        }
        if edit.clear.burst {
            entry.burst = None;
        }
        entry.modified_ms = now_ms;
        Ok(self.tenants.get(&key).expect("just inserted"))
    }

    /// Deletes. `true` if there was something to delete.
    pub fn remove(&mut self, pattern: &str) -> std::result::Result<bool, IdentError> {
        let key = RulePattern::parse(pattern)?.to_string();
        Ok(self.rules.remove(&key).is_some())
    }

    /// Deletes a tenant's line. `true` if there was something to delete.
    pub fn remove_tenant(&mut self, tenant: &str) -> std::result::Result<bool, IdentError> {
        let key = TenantId::new(tenant)?.to_string();
        Ok(self.tenants.remove(&key).is_some())
    }

    /// The most specific enabled rule matching `path` and satisfying `has`.
    fn most_specific(&self, path: &CounterPath, has: impl Fn(&Rule) -> bool) -> Option<&Rule> {
        self.rules
            .values()
            .filter(|r| !r.disabled && has(r) && r.pattern.matches(path))
            .max_by_key(|r| r.pattern.specificity())
    }

    /// The rule that **wins the concurrency cap** for a specific counter, if any.
    ///
    /// More specific wins, among the rules **that set** concurrency. A `disabled` rule does
    /// not take part — it is absent, not "matching with an infinite cap": if it shadowed a
    /// broader `*` rule, turning it off would **loosen** the cap again, exactly what the
    /// operator would not expect.
    pub fn winner(&self, path: &CounterPath) -> Option<&Rule> {
        self.most_specific(path, |r| r.concurrency.is_some())
    }

    /// The effective cap of a counter. `None` = unlimited.
    pub fn limit_of(&self, path: &CounterPath) -> Option<u32> {
        match self.winner(path).and_then(|r| r.concurrency) {
            Some(Limit::Max(n)) => Some(n),
            Some(Limit::Unlimited) | None => None,
        }
    }

    /// The rate cap of a counter (one bucket per tenant), with the winning rule.
    /// `None` = unlimited.
    pub fn rate_of(&self, path: &CounterPath) -> Option<(Rate, &Rule)> {
        let r = self.most_specific(path, |r| r.rate.is_some())?;
        Rate::of(r.rate?, r.burst).map(|rate| (rate, r))
    }

    /// The rate cap of the whole tenant. `None` = unlimited.
    pub fn tenant_rate(&self, tenant: &TenantId) -> Option<Rate> {
        let t = self.tenants.get(tenant.as_str()).filter(|t| !t.disabled)?;
        Rate::of(t.rate?, t.burst)
    }

    /// Every tenant that has a rate cap.
    pub fn tenant_rates(&self) -> impl Iterator<Item = (&TenantId, Rate)> {
        self.tenants
            .values()
            .filter(|t| !t.disabled)
            .filter_map(|t| Some((&t.tenant, Rate::of(t.rate?, t.burst)?)))
    }

    /// Whether any rule sets a per-counter rate cap — the scheduler skips the per-counter
    /// lookup when none does.
    pub fn has_counter_rates(&self) -> bool {
        self.rules.values().any(|r| !r.disabled && r.rate.is_some())
    }

    // ───────────────────────────────────────────────────── serialisation

    /// A line format, tab-separated. Text rather than binary because this is **an object
    /// an operator will open and read at 3 a.m.**, and when a cluster cannot start because
    /// of a broken rule book, what saves them is `cat`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("{HEADER}\nversion={}\n", self.version);
        for r in self.rules.values() {
            out.push_str(&format!(
                "rule\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                r.pattern,
                show_limit(r.concurrency),
                show_limit(r.rate),
                show_u32(r.burst),
                if r.disabled { 1 } else { 0 },
                r.modified_ms,
                r.description
            ));
        }
        for t in self.tenants.values() {
            out.push_str(&format!(
                "tenant\t{}\t{}\t{}\t{}\t{}\t{}\n",
                t.tenant,
                show_limit(t.rate),
                show_u32(t.burst),
                if t.disabled { 1 } else { 0 },
                t.modified_ms,
                t.description
            ));
        }
        out.into_bytes()
    }

    /// Reads `/2` or `/1`. Strict: one line off the layout refuses the whole object.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        let bad = |reason: String| StoreError::BadRuleBook { reason };
        let text = std::str::from_utf8(raw).map_err(|_| bad("not UTF-8".into()))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        let v1 = match header {
            HEADER => false,
            HEADER_V1 => true,
            _ => {
                if let Some(e) = crate::format::newer_header("rules", HEADER, header) {
                    return Err(e);
                }
                return Err(bad(format!("unknown header, expected {HEADER:?}")));
            }
        };
        let version = lines
            .next()
            .and_then(|l| l.strip_prefix("version=")?.parse::<u64>().ok())
            .ok_or_else(|| bad("missing version".into()))?;

        let mut book = Self {
            version,
            ..Default::default()
        };
        for line in lines {
            if line.is_empty() {
                continue;
            }
            // An unknown line ⇒ refuse the whole object. Skipping a line we do not
            // understand would silently run with fewer caps than the operator thinks —
            // exactly the failure this whole module exists to prevent.
            let wrong = || bad(format!("line does not match the layout: {line:?}"));
            if v1 {
                // `splitn(6, ..)`: the description is **the rest of the line**, so it may
                // contain anything except a newline.
                let f: Vec<&str> = line.splitn(6, '\t').collect();
                if f.len() < 6 || f[0] != "rule" {
                    return Err(wrong());
                }
                let pattern = RulePattern::parse(f[1])
                    .map_err(|e| bad(format!("pattern {:?}: {e}", f[1])))?;
                let concurrency = Limit::parse(f[2])
                    .ok_or_else(|| bad(format!("cap {:?} cannot be read", f[2])))?;
                book.rules.insert(
                    pattern.to_string(),
                    Rule {
                        pattern,
                        concurrency: Some(concurrency),
                        rate: None,
                        burst: None,
                        description: f[5].to_string(),
                        disabled: f[3] == "1",
                        modified_ms: f[4].parse().unwrap_or(0),
                    },
                );
                continue;
            }
            match line.split_once('\t').map(|(k, _)| k) {
                Some("rule") => {
                    let f: Vec<&str> = line.splitn(8, '\t').collect();
                    if f.len() < 8 {
                        return Err(wrong());
                    }
                    let pattern = RulePattern::parse(f[1])
                        .map_err(|e| bad(format!("pattern {:?}: {e}", f[1])))?;
                    let concurrency = opt_limit(f[2])
                        .ok_or_else(|| bad(format!("cap {:?} cannot be read", f[2])))?;
                    let rate = opt_limit(f[3])
                        .ok_or_else(|| bad(format!("rate {:?} cannot be read", f[3])))?;
                    let burst = opt_u32(f[4])
                        .ok_or_else(|| bad(format!("burst {:?} cannot be read", f[4])))?;
                    book.rules.insert(
                        pattern.to_string(),
                        Rule {
                            pattern,
                            concurrency,
                            rate,
                            burst,
                            description: f[7].to_string(),
                            disabled: f[5] == "1",
                            modified_ms: f[6].parse().unwrap_or(0),
                        },
                    );
                }
                Some("tenant") => {
                    let f: Vec<&str> = line.splitn(7, '\t').collect();
                    if f.len() < 7 {
                        return Err(wrong());
                    }
                    let tenant =
                        TenantId::new(f[1]).map_err(|e| bad(format!("tenant {:?}: {e}", f[1])))?;
                    let rate = opt_limit(f[2])
                        .ok_or_else(|| bad(format!("rate {:?} cannot be read", f[2])))?;
                    let burst = opt_u32(f[3])
                        .ok_or_else(|| bad(format!("burst {:?} cannot be read", f[3])))?;
                    book.tenants.insert(
                        tenant.to_string(),
                        TenantRule {
                            tenant,
                            rate,
                            burst,
                            description: f[6].to_string(),
                            disabled: f[4] == "1",
                            modified_ms: f[5].parse().unwrap_or(0),
                        },
                    );
                }
                _ => return Err(wrong()),
            }
        }
        Ok(book)
    }
}

// ────────────────────────────────────────────────────── its place on the object store

/// The rule book on the object store, with CAS on the etag.
///
/// The same mould as [`crate::DeploymentStore`]: one object at **cluster** level, belonging
/// to no partition. The rule book really is cluster configuration — a per-scope cap means
/// nothing if each partition understands it differently.
#[derive(Clone)]
pub struct RuleStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// The process's "the rule book may have just changed" generation, shared by every
    /// clone: bumped when we write ourselves, or when a peer reports a change.
    changed: Arc<std::sync::atomic::AtomicU64>,
    /// Nudges the node-level poll loop every time the generation is bumped
    /// ([`Self::changed`]).
    poke: Arc<tokio::sync::Notify>,
    /// The node's shared copy: the node-level poll loop writes it ([`Self::poll_once`]),
    /// the ranges' engines read it ([`Self::published`]) — one GET per tick per node, not
    /// per range.
    published: Arc<tokio::sync::watch::Sender<Published>>,
}

/// The node's shared copy of the rule book, as the node-level poll loop just read it.
#[derive(Clone, Debug, Default)]
pub struct Published {
    /// Number of finished read rounds, successful or failed; `0` = no round yet. An engine
    /// compares this with the round it has applied to know whether there is something new.
    pub round: u64,
    /// The latest successful read; `None` = no successful read yet. A failed round **keeps**
    /// this copy — falling back to "unlimited" because of one failed read would throw the
    /// caps wide open exactly when nobody is looking.
    pub book: Option<Arc<RuleBook>>,
}

/// The rule book just read, with the object version to CAS the next write against.
pub struct LoadedRules {
    pub book: RuleBook,
    version: Option<UpdateVersion>,
}

/// The outcome of one CAS write.
enum Put {
    Done,
    /// The CAS missed: someone else just wrote.
    Raced,
}

impl RuleStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &Path) -> Self {
        let p = prefix.as_ref();
        let path = if p.is_empty() {
            Path::from("rules")
        } else {
            Path::from(format!("{p}/rules"))
        };
        Self {
            store,
            path,
            changed: Arc::default(),
            poke: Arc::default(),
            published: Arc::new(tokio::sync::watch::channel(Published::default()).0),
        }
    }

    /// The current change generation — see [`Self::mark_changed`].
    pub fn generation(&self) -> u64 {
        self.changed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The rule book may have just changed (we wrote, or a peer reported it): periodic
    /// readers re-read on their next ask.
    pub fn mark_changed(&self) {
        self.changed
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        // `notify_one` keeps a ticket when the poll loop is busy reading: a report arriving
        // in the middle of a GET still makes the loop re-read right after.
        self.poke.notify_one();
    }

    /// Waits for the next change report ([`Self::mark_changed`]) — for the node-level poll
    /// loop.
    pub async fn changed(&self) {
        self.poke.notified().await
    }

    /// The node's current shared copy. No I/O.
    pub fn published(&self) -> Published {
        self.published.borrow().clone()
    }

    /// Waits for the node's first read round to finish (successful or failed), at most
    /// `bound`. For an engine that has just become leader on a node that has just started:
    /// do not serve "uncapped" when the rule book is only one GET away. Past the bound, give
    /// up — the engine applies the shared copy on its next background tick.
    pub async fn first_round(&self, bound: std::time::Duration) {
        let mut rx = self.published.subscribe();
        let _ = crate::retry::deadline(&crate::retry::ops::RULES_POLL, bound, async {
            let _ = rx.wait_for(|p| p.round > 0).await;
        })
        .await;
    }

    /// One round of the node-level poll loop: GET the rule book (bound `bound`), push it into
    /// the shared copy. On failure ⇒ the shared copy keeps the old copy, and it still counts
    /// as a finished round; returns `false` so the loop retries early.
    ///
    /// **A change report arriving during the read ⇒ drop the result**: not published, the
    /// round not counted. A writer on this node pushes the new copy straight into the ranges
    /// (ingress's `push_rules`) right after the report; a GET started before the write and
    /// published after it would bring the ranges back to the old copy — the scheduler only
    /// skips a copy with the same version. The report keeps a ticket
    /// ([`Self::mark_changed`]), so the poll loop re-reads at once. A report arriving
    /// **after** the comparison but before publishing is not seen by the comparison: the
    /// shared copy only moves forward by `version`, an older read is dropped (the round
    /// still counts).
    pub async fn poll_once(&self, bound: std::time::Duration) -> bool {
        let generation = self.generation();
        let read = crate::retry::deadline(&crate::retry::ops::RULES_POLL, bound, self.load()).await;
        if self.generation() != generation {
            tracing::debug!("rule book changed during the read, reading again");
            return true;
        }
        let book = match read {
            Ok(Ok(loaded)) => Some(Arc::new(loaded.book)),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "cannot read the rule book, keeping the current one");
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, "rule book read timed out, keeping the current one");
                None
            }
        };
        let ok = book.is_some();
        self.published.send_modify(|p| {
            p.round += 1;
            // Monotonic by `version`, like [`Self::publish_written`]: a write on the node
            // (another thread) may publish a newer copy **after** the generation comparison
            // above — no `.await` closes that window —, and this GET started before the
            // write. The store reads strongly after writes, so a read older than the
            // published copy can only be a late arrival.
            if let Some(book) = book {
                if p.book
                    .as_ref()
                    .is_none_or(|cur| cur.version <= book.version)
                {
                    p.book = Some(book);
                }
            }
        });
        ok
    }

    /// Publishes the copy this node just wrote into the shared copy, opening a new round.
    /// Ingress pushes that copy straight into the ranges right after [`Self::update`]; the
    /// shared copy must not be older than it, otherwise the next background tick of a range
    /// that has not applied the current round would bring the range back to the old copy.
    ///
    /// Monotonic by `version`: two writes of the node arriving out of order do not
    /// overwrite the new copy with the old one — the versions of CAS writes on the same
    /// object increase strictly.
    pub fn publish_written(&self, book: &RuleBook) {
        self.published.send_if_modified(|p| {
            if p.book
                .as_ref()
                .is_some_and(|cur| cur.version >= book.version)
            {
                return false;
            }
            p.round += 1;
            p.book = Some(Arc::new(book.clone()));
            true
        });
    }

    /// Raw bytes of the rule book with the object version. `None` = none yet.
    async fn load_raw(&self) -> Result<Option<(bytes::Bytes, UpdateVersion)>> {
        match self.store.get(&self.path).await {
            Ok(got) => {
                let version = UpdateVersion {
                    e_tag: got.meta.e_tag.clone(),
                    version: got.meta.version.clone(),
                };
                let raw = got
                    .bytes()
                    .await
                    .map_err(|e| StoreError::LeaseIo(e.to_string()))?;
                Ok(Some((raw, version)))
            }
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Reads the rule book. No object yet ⇒ an empty rule book at version 0 — **not** an
    /// error: a cluster with no rules set is a cluster with no limits at all, and that is
    /// the right default.
    pub async fn load(&self) -> Result<LoadedRules> {
        Ok(match self.load_raw().await? {
            Some((raw, version)) => LoadedRules {
                book: RuleBook::decode(&raw)?,
                version: Some(version),
            },
            None => LoadedRules {
                book: RuleBook::default(),
                version: None,
            },
        })
    }

    /// Writes `raw` by CAS on `expected` (`None` ⇒ create).
    ///
    /// An ambiguous outcome is resolved by re-reading (rule 7): the rule book carries a
    /// `version` bumped on every write, so the content is unique enough — an object_store
    /// retry meeting `Precondition` on our own write is `Done`, not `Raced` (before this,
    /// `edit` ran again on the copy that already had our change, and `remove` found the rule
    /// gone and reported 404).
    ///
    /// **A store without CAS** (`LocalFileSystem`, the `--data-dir` path): overwrite
    /// ([`crate::cas::put_resolving_or_overwrite`]). Safe **precisely because** the lack of
    /// CAS already prevents forming a cluster (D12): there is no second node to race — and
    /// returning an error would mean `--data-dir` can set exactly one rule and no more.
    async fn put_cas(&self, raw: Vec<u8>, expected: Option<UpdateVersion>) -> Result<Put> {
        let mode = match expected {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        match crate::cas::put_resolving_or_overwrite(&self.store, &self.path, raw.into(), mode)
            .await
        {
            Ok(crate::cas::Cas::Written(_)) => Ok(Put::Done),
            // The CAS missed: someone else just wrote. The caller re-reads and redoes the
            // work on their new copy — merging, not overwriting.
            Ok(crate::cas::Cas::Lost) => Ok(Put::Raced),
            Err(e) => Err(StoreError::LeaseIo(e.to_string())),
        }
    }

    /// Read-modify-write with CAS. `edit` may run **several times** — it must be a pure
    /// function on the rule book, with no side effect other than the edit.
    ///
    /// A loop rather than one attempt: two operators typing `rules set` at the same time is
    /// a real thing, and "one wins, the other silently loses their change" is exactly the
    /// kind of failure nobody can trace.
    pub async fn update<F>(&self, mut edit: F) -> Result<RuleBook>
    where
        F: FnMut(&mut RuleBook) -> Result<()>,
    {
        const MAX_TRIES: usize = 8;
        for _ in 0..MAX_TRIES {
            let loaded = self.load().await?;
            let mut book = loaded.book;
            edit(&mut book)?;
            book.version = book.version.saturating_add(1);
            match self.put_cas(book.encode(), loaded.version).await? {
                Put::Done => {
                    // Report first: a poll loop reading (possibly before this write) drops
                    // its result and re-reads ([`Self::poll_once`]).
                    self.mark_changed();
                    self.publish_written(&book);
                    return Ok(book);
                }
                Put::Raced => continue,
            }
        }
        Err(StoreError::BadRuleBook {
            reason: format!("CAS lost {MAX_TRIES} times in a row — someone writing nonstop?"),
        })
    }

    /// **The `rules/2` migration step**: rewrites a `/1` rule book as `/2` — the same
    /// rules, the same `version` (the logical content does not change, so readers have
    /// nothing to apply again), the new columns set to `-`.
    ///
    /// Idempotent: no rule book yet, or already at `/2`, writes nothing. CAS on the etag, so
    /// an old binary writing `/1` between the read and the write makes the write miss, and
    /// the next round rewrites its new copy — no rule is lost.
    pub async fn rewrite_as_v2(&self) -> Result<()> {
        const MAX_TRIES: usize = 8;
        for _ in 0..MAX_TRIES {
            let Some((raw, version)) = self.load_raw().await? else {
                return Ok(());
            };
            if raw.starts_with(format!("{HEADER}\n").as_bytes()) {
                return Ok(());
            }
            // Strict read: a broken `/1` rule book fails the step and the runner stops — no
            // guessing.
            let book = RuleBook::decode(&raw)?;
            match self.put_cas(book.encode(), Some(version)).await? {
                Put::Done => {
                    tracing::info!(rule = book.len(), "rule book rewritten as {HEADER}");
                    return Ok(());
                }
                Put::Raced => continue,
            }
        }
        Err(StoreError::BadRuleBook {
            reason: format!("migrating rules/2: CAS missed {MAX_TRIES} times in a row"),
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::scope::Scope,
        slatedb::object_store::{memory::InMemory, PutPayload},
    };

    fn sc(s: &str) -> Scope {
        Scope::new(s).unwrap()
    }

    fn book(rules: &[(&str, u32)]) -> RuleBook {
        let mut b = RuleBook::default();
        for (p, n) in rules {
            b.set(p, Some(Limit::Max(*n)), None, None, 0).unwrap();
        }
        b
    }

    #[test]
    fn more_specific_wins_on_same_counter() {
        let b = book(&[("*/*", 100), ("checkout/*", 10), ("checkout/premium", 3)]);
        let path = |s: &str, l1: &str| CounterPath::L1(sc(s), l1.into());
        assert_eq!(b.limit_of(&path("checkout", "premium")), Some(3));
        assert_eq!(b.limit_of(&path("checkout", "free")), Some(10));
        assert_eq!(b.limit_of(&path("other", "premium")), Some(100));
    }

    #[test]
    fn rule_applies_only_at_its_depth() {
        let b = book(&[("acme/growth", 5)]);
        assert_eq!(b.limit_of(&CounterPath::Scope(sc("acme"))), None);
        assert_eq!(
            b.limit_of(&CounterPath::L1(sc("acme"), "growth".into())),
            Some(5)
        );
    }

    #[test]
    fn disabled_means_absent_not_unlimited() {
        let mut b = book(&[("*", 100), ("acme", 5)]);
        assert_eq!(b.limit_of(&CounterPath::Scope(sc("acme"))), Some(5));
        b.set("acme", None, None, Some(true), 1).unwrap();
        // Disabling the specific rule ⇒ the broader rule wins, **not** unlimited.
        assert_eq!(b.limit_of(&CounterPath::Scope(sc("acme"))), Some(100));
    }

    #[test]
    fn set_merges_instead_of_blank_overwrite() {
        let mut b = RuleBook::default();
        b.set("acme", Some(Limit::Max(5)), Some("pricing".into()), None, 0)
            .unwrap();
        b.set("acme", None, None, Some(true), 1).unwrap();
        let r = b.get("acme").unwrap();
        assert_eq!(r.concurrency, Some(Limit::Max(5)), "cap must not be lost");
        assert_eq!(r.description, "pricing", "the description must not be lost");
        assert!(r.disabled);
        // And reopening must not touch anything else.
        b.set("acme", None, None, Some(false), 2).unwrap();
        let r = b.get("acme").unwrap();
        assert!(!r.disabled);
        assert_eq!(r.concurrency, Some(Limit::Max(5)));
    }

    #[test]
    fn unlimited_differs_from_no_rule() {
        let mut b = book(&[("*", 100)]);
        b.set("acme", Some(Limit::Unlimited), None, None, 0)
            .unwrap();
        // The more specific rule wins, and it says "unlimited" — so `*` does not apply.
        assert_eq!(b.limit_of(&CounterPath::Scope(sc("acme"))), None);
        assert_eq!(b.limit_of(&CounterPath::Scope(sc("other"))), Some(100));
    }

    #[test]
    fn encoding_roundtrips_and_is_deterministic() {
        let mut b = book(&[("*", 100), ("acme/growth/alice", 1)]);
        b.set(
            "acme",
            Some(Limit::Unlimited),
            Some("VIP customer\twith tab".into()),
            Some(true),
            42,
        )
        .unwrap();
        b.version = 7;
        let raw = b.encode();
        let back = RuleBook::decode(&raw).unwrap();
        assert_eq!(back, b);
        // Deterministic: same content ⇒ same bytes, because readers at the lower layer
        // compare the etag.
        assert_eq!(back.encode(), raw);
        assert!(!String::from_utf8(raw).unwrap().contains("with tab\t"));
    }

    #[test]
    fn corrupt_rule_book_rejected_not_guessed() {
        for raw in [
            &b"junk"[..],
            b"cairnflow-rules/1\n",
            b"cairnflow-rules/3\nversion=1\n",
            b"cairnflow-rules/1\nversion=1\nrule\tacme\n",
            b"cairnflow-rules/1\nversion=1\nrule\tacme\tjunk\t0\t0\t\n",
            // `/1` has no `tenant` line.
            b"cairnflow-rules/1\nversion=1\ntenant\tacme\t5\t-\t0\t0\t\n",
            b"cairnflow-rules/2\nversion=1\nrule\tacme\t5\t0\t0\t\n",
            b"cairnflow-rules/2\nversion=1\nrule\tacme\t5\tjunk\t-\t0\t0\t\n",
            b"cairnflow-rules/2\nversion=1\nrule\tacme\t5\t-\t-1\t0\t0\t\n",
            b"cairnflow-rules/2\nversion=1\ntenant\t.x\t5\t-\t0\t0\t\n",
            b"cairnflow-rules/2\nversion=1\nquota\tacme\t5\t-\t0\t0\t\n",
        ] {
            assert!(RuleBook::decode(raw).is_err(), "must refuse: {raw:?}");
        }
        // A header of the same family with a higher number: stop loudly, not "damaged".
        assert!(matches!(
            RuleBook::decode(b"cairnflow-rules/3\nversion=1\n"),
            Err(StoreError::NewerFormat { .. })
        ));
    }

    /// `/1` reads with exactly the old meaning: every rule sets concurrency explicitly, no
    /// `rate` yet; written back as `/2` with the two new columns as `-`.
    #[test]
    fn v1_reads_with_old_meaning_and_rewrites_as_v2() {
        let v1 = "cairnflow-rules/1\nversion=3\nrule\tacme\tunlimited\t0\t5\tnotes\nrule\t*\t7\t1\t6\t\n";
        let b = RuleBook::decode(v1.as_bytes()).unwrap();
        assert_eq!(b.version, 3);
        let acme = b.get("acme").unwrap();
        assert_eq!(
            (acme.concurrency, acme.rate, acme.burst),
            (Some(Limit::Unlimited), None, None)
        );
        assert!(b.get("*").unwrap().disabled);
        let v2 = String::from_utf8(b.encode()).unwrap();
        assert_eq!(
            v2,
            "cairnflow-rules/2\nversion=3\nrule\t*\t7\t-\t-\t1\t6\t\nrule\tacme\tunlimited\t-\t-\t0\t5\tnotes\n"
        );
        assert_eq!(RuleBook::decode(v2.as_bytes()).unwrap(), b);
    }

    /// `clear` resets a cap to unset, unlike `Unlimited`; untouched fields are kept — both
    /// for pattern rules and for tenant lines.
    #[test]
    fn clear_resets_limit_to_unset() {
        let mut b = RuleBook::default();
        b.apply(
            "acme",
            RuleEdit {
                concurrency: Some(Limit::Max(5)),
                rate: Some(Limit::Max(10)),
                burst: Some(3),
                description: Some("kept".into()),
                ..RuleEdit::default()
            },
            1,
        )
        .unwrap();
        let r = b
            .apply(
                "acme",
                RuleEdit {
                    clear: RuleClear {
                        concurrency: true,
                        burst: true,
                        ..RuleClear::default()
                    },
                    ..RuleEdit::default()
                },
                2,
            )
            .unwrap();
        assert_eq!(
            (r.concurrency, r.rate, r.burst, r.description.as_str()),
            (None, Some(Limit::Max(10)), None, "kept")
        );
        b.apply_tenant(
            "acme",
            RuleEdit {
                rate: Some(Limit::Max(50)),
                burst: Some(5),
                ..RuleEdit::default()
            },
            3,
        )
        .unwrap();
        let t = b
            .apply_tenant(
                "acme",
                RuleEdit {
                    clear: RuleClear {
                        rate: true,
                        ..RuleClear::default()
                    },
                    ..RuleEdit::default()
                },
                4,
            )
            .unwrap();
        assert_eq!((t.rate, t.burst), (None, Some(5)));
    }

    /// Each attribute has its own winner: a more specific rule that only sets `rate` does
    /// not remove the concurrency cap of a broader rule, and vice versa.
    #[test]
    fn rate_and_concurrency_resolve_separately() {
        let mut b = book(&[("*", 100)]);
        b.apply(
            "acme",
            RuleEdit {
                rate: Some(Limit::Max(50)),
                ..Default::default()
            },
            1,
        )
        .unwrap();
        let acme = CounterPath::Scope(sc("acme"));
        assert_eq!(
            b.limit_of(&acme),
            Some(100),
            "a rate-only rule removed the concurrency cap of `*`"
        );
        let (rate, rule) = b.rate_of(&acme).unwrap();
        assert_eq!(
            (rate, rule.pattern.to_string().as_str()),
            (
                Rate {
                    per_sec: 50,
                    burst: 50
                },
                "acme"
            )
        );
        assert!(b.rate_of(&CounterPath::Scope(sc("other"))).is_none());
        // `burst` set on its own; an explicit `unlimited` shadows the broader rule's rate.
        b.apply(
            "acme",
            RuleEdit {
                burst: Some(200),
                ..Default::default()
            },
            2,
        )
        .unwrap();
        assert_eq!(
            b.rate_of(&acme).unwrap().0,
            Rate {
                per_sec: 50,
                burst: 200
            }
        );
        b.apply(
            "*",
            RuleEdit {
                rate: Some(Limit::Max(10)),
                ..Default::default()
            },
            3,
        )
        .unwrap();
        b.apply(
            "acme",
            RuleEdit {
                rate: Some(Limit::Unlimited),
                ..Default::default()
            },
            4,
        )
        .unwrap();
        assert!(b.rate_of(&acme).is_none());
        assert_eq!(
            b.rate_of(&CounterPath::Scope(sc("other")))
                .unwrap()
                .0
                .per_sec,
            10
        );
    }

    #[test]
    fn tenant_line_roundtrips_and_can_be_disabled() {
        let mut b = RuleBook::default();
        b.apply_tenant(
            "acme",
            RuleEdit {
                rate: Some(Limit::Max(50)),
                description: Some("plan\tfree".into()),
                ..Default::default()
            },
            9,
        )
        .unwrap();
        let acme = TenantId::new("acme").unwrap();
        assert_eq!(
            b.tenant_rate(&acme),
            Some(Rate {
                per_sec: 50,
                burst: 50
            })
        );
        assert_eq!(b.tenant_rate(&TenantId::default()), None);
        b.version = 4;
        let back = RuleBook::decode(&b.encode()).unwrap();
        assert_eq!(back, b);
        assert_eq!(back.get_tenant("acme").unwrap().description, "plan free");
        b.apply_tenant(
            "acme",
            RuleEdit {
                disabled: Some(true),
                ..Default::default()
            },
            10,
        )
        .unwrap();
        assert_eq!(b.tenant_rate(&acme), None);
        assert!(b.remove_tenant("acme").unwrap());
        assert!(b.is_empty());
    }

    /// **A change report arriving in the middle of a read round ⇒ that round's result is
    /// not published.** `PUT /rules` writes and then pushes the new copy straight into the
    /// schedulers of this node's ranges (`push_rules`); a poll round that GOT **before** the
    /// write and published **after** it would bring the ranges back to the old copy —
    /// `set_rules` only skips a copy with the same version, it does not block an older one
    /// (the Playwright `why.spec.ts` "flow control by limit key" case failed 2 out of 3 on a
    /// 4-range cluster: the second call ran at once, the cap was gone).
    ///
    /// 1. The first read round publishes v1.
    /// 2. The rule book GET is blocked at a gate; the second round starts and stands at the
    ///    GET.
    /// 3. A change report (`mark_changed`) arrives while the round stands.
    /// 4. Open the gate; the round finishes reading.
    ///
    /// Must see: the second round does not change the shared copy (the round count stays
    /// the same) — the report still holds a ticket so the poll loop re-reads at once; the
    /// third round publishes normally.
    #[tokio::test]
    async fn poll_read_overtaken_by_a_change_is_not_published() {
        use crate::testkit::{Action, FaultyStore, Gate, OpSet, PathMatch, Rule};
        let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
        let rs = RuleStore::new(faulty.clone(), &Path::default());
        rs.update(|b| {
            b.set("acme", Some(Limit::Max(5)), None, None, 0).unwrap();
            Ok(())
        })
        .await
        .unwrap();
        let bound = std::time::Duration::from_secs(10);
        assert!(rs.poll_once(bound).await);
        let first = rs.published();
        assert!(first.round > 0);

        let gate = Gate::closed();
        let rule = faulty.add_rule(Rule::new(
            OpSet::GET,
            PathMatch::Contains("rules".into()),
            Action::Gate(gate.clone()),
        ));
        let poll = tokio::spawn({
            let rs = rs.clone();
            async move { rs.poll_once(bound).await }
        });
        while gate.blocked() == 0 {
            tokio::task::yield_now().await;
        }
        rs.mark_changed();
        faulty.remove_rule(rule);
        gate.open();
        poll.await.unwrap();
        assert_eq!(
            rs.published().round,
            first.round,
            "a read round overtaken by a change report must not be published"
        );

        assert!(rs.poll_once(bound).await);
        assert_eq!(rs.published().round, first.round + 1);
    }

    /// **A successful write on the node publishes the copy it just wrote.** Ingress pushes
    /// the new copy straight into the ranges right after `update`; if the shared copy were
    /// still the previous one at a round some range has not applied, that range's next
    /// background tick would apply an **older** shared copy — the cap just set would vanish
    /// until the next poll round.
    ///
    /// 1. A read round publishes v1 (round 1).
    /// 2. `update` writes v2.
    ///
    /// Must see: right after (2), the shared copy is v2 at a new round; a publish by the
    /// node itself with an older copy (two writes arriving out of order) cannot overwrite
    /// it.
    #[tokio::test]
    async fn local_write_publishes_its_book() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rs = RuleStore::new(store, &Path::default());
        rs.update(|b| {
            b.set("acme", Some(Limit::Max(5)), None, None, 0).unwrap();
            Ok(())
        })
        .await
        .unwrap();
        assert!(rs.poll_once(std::time::Duration::from_secs(10)).await);
        let before = rs.published();
        let v1 = before.book.clone().unwrap();
        let v2 = rs
            .update(|b| {
                b.set("acme", Some(Limit::Max(7)), None, None, 0).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        let after = rs.published();
        assert!(after.round > before.round, "a write must open a new round");
        assert_eq!(
            after.book.as_deref(),
            Some(&v2),
            "the shared copy must be the one just written"
        );
        rs.publish_written(&v1);
        assert_eq!(
            rs.published().book.as_deref(),
            Some(&v2),
            "an older copy must not overwrite the published copy"
        );
    }

    /// **The poll loop does not publish a copy older than the one it holds.**
    ///
    /// The real interleaving: the poll loop reads the generation and GETs v1; another thread
    /// (the `PUT /rules` handler) calls `mark_changed` + `publish_written(v2)` **after** the
    /// poll loop's generation comparison and before its `send_modify` — there is no
    /// `.await` between those two steps, so it is rebuilt from its consequence: the shared
    /// copy is already v2, and the poll loop's GET returns v1 (the read started before the
    /// write).
    ///
    /// Must see: after the poll round, the shared copy is still v2 (the round still
    /// counts). Before the fix: `send_modify` assigned v1, and `refresh_rules` brought the
    /// rule set of every range back to v1 until the next poll round (~100–200 ms).
    #[tokio::test]
    async fn poll_does_not_publish_an_older_book_than_it_holds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rs = RuleStore::new(Arc::clone(&store), &Path::default());
        rs.update(|b| {
            b.set("acme", Some(Limit::Max(5)), None, None, 0).unwrap();
            Ok(())
        })
        .await
        .unwrap();
        let v1_bytes = store
            .get(&Path::from("rules"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let v2 = rs
            .update(|b| {
                b.set("acme", Some(Limit::Max(7)), None, None, 0).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        // The poll loop's GET sees the copy from before the write.
        store
            .put(&Path::from("rules"), PutPayload::from(v1_bytes))
            .await
            .unwrap();
        let before = rs.published();
        assert!(rs.poll_once(std::time::Duration::from_secs(10)).await);
        let after = rs.published();
        assert!(after.round > before.round, "read round must still count");
        assert_eq!(
            after.book.as_deref(),
            Some(&v2),
            "the poll loop brought the shared copy back to an older one"
        );
    }

    /// The `rules/2` migration step is idempotent, and does not create a rule book on a
    /// bucket that has none.
    #[tokio::test]
    async fn v2_rewrite_is_idempotent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rs = RuleStore::new(Arc::clone(&store), &Path::default());
        rs.rewrite_as_v2().await.unwrap();
        assert!(store.get(&Path::from("rules")).await.is_err());
        store
            .put(
                &Path::from("rules"),
                PutPayload::from("cairnflow-rules/1\nversion=2\nrule\tacme\t5\t0\t1\t\n"),
            )
            .await
            .unwrap();
        rs.rewrite_as_v2().await.unwrap();
        let once = store
            .get(&Path::from("rules"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(once.starts_with(b"cairnflow-rules/2\nversion=2\n"));
        rs.rewrite_as_v2().await.unwrap();
        assert_eq!(
            store
                .get(&Path::from("rules"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            once
        );
        assert_eq!(
            rs.load()
                .await
                .unwrap()
                .book
                .limit_of(&CounterPath::Scope(sc("acme"))),
            Some(5)
        );
    }

    #[tokio::test]
    async fn missing_object_reads_as_empty_rule_book() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rs = RuleStore::new(store, &Path::default());
        let loaded = rs.load().await.unwrap();
        assert_eq!(loaded.book.version, 0);
        assert!(loaded.book.is_empty());
    }

    /// A store without CAS (`LocalFileSystem` — the `--data-dir` path) can still **edit the
    /// rule book many times**.
    ///
    /// This is a real failure, not a hypothesis: the first `set` goes through
    /// `PutMode::Create` so it works; every later one goes through `PutMode::Update` and
    /// `LocalFileSystem` returns `NotSupported`. Without a fallback, a single-node
    /// deployment could set exactly one rule and no more.
    #[tokio::test]
    async fn store_without_cas_still_edits_repeatedly() {
        let dir =
            std::env::temp_dir().join(format!("cairn-rules-{}", crate::entropy::process_id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::local::LocalFileSystem::new_with_prefix(&dir).unwrap());
        let rs = RuleStore::new(store, &Path::default());

        for (i, (pattern, limit)) in [("acme", 5u32), ("*", 100), ("acme/growth", 1)]
            .into_iter()
            .enumerate()
        {
            let b = rs
                .update(|b| {
                    b.set(pattern, Some(Limit::Max(limit)), None, None, 0)
                        .unwrap();
                    Ok(())
                })
                .await
                .unwrap_or_else(|e| panic!("write number {} failed: {e}", i + 1));
            assert_eq!(b.version, i as u64 + 1);
            assert_eq!(b.len(), i + 1);
        }
        assert_eq!(rs.load().await.unwrap().book.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Rule 7: a CAS that was written while the answer says otherwise (object_store retries
    /// a 5xx by itself ⇒ `Precondition`; or `Err` after the write). Must see: `remove`
    /// reports **existed** (not a 404 for a rule just deleted), `set` returns no error, and
    /// `version` increases by just one — the write is resolved by re-reading, `edit` is not
    /// redone on the copy that already carries our change.
    #[tokio::test]
    async fn ambiguous_write_is_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty = FaultyStore::wrap(Arc::new(InMemory::new()));
            let rs = RuleStore::new(faulty.clone(), &Path::default());
            rs.update(|b| {
                b.set("acme", Some(Limit::Max(5)), None, None, 0).unwrap();
                Ok(())
            })
            .await
            .unwrap();

            faulty.add_rule(Rule::ambiguous_put("rules", reply));
            let b = rs
                .update(|b| {
                    b.set("*", Some(Limit::Max(100)), None, None, 0).unwrap();
                    Ok(())
                })
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: set wrote yet failed: {e}"));
            assert_eq!(b.version, 2, "{reply:?}: edit reran on the written copy");

            faulty.add_rule(Rule::ambiguous_put("rules", reply));
            let mut existed = false;
            let b = rs
                .update(|b| {
                    existed = b.remove("acme").unwrap();
                    Ok(())
                })
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: remove wrote yet failed: {e}"));
            assert!(existed, "{reply:?}: deleted rule reported absent (404)");
            assert_eq!(b.version, 3, "{reply:?}");
            assert_eq!(rs.load().await.unwrap().book, b, "{reply:?}");
        }
    }

    #[tokio::test]
    async fn write_read_back_and_version_increments() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rs = RuleStore::new(store, &Path::from("cairn"));
        let b1 = rs
            .update(|b| {
                b.set("acme", Some(Limit::Max(5)), None, None, 0).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(b1.version, 1);

        let b2 = rs
            .update(|b| {
                b.set("*", Some(Limit::Max(100)), None, None, 0).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(b2.version, 2);
        assert_eq!(b2.len(), 2);

        let back = rs.load().await.unwrap().book;
        assert_eq!(back, b2);
    }
}
