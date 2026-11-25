// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Flow control identity: scope, limit key, and rule pattern.
//!
//! All three types here are **own types checked in the constructor**, not a `String`
//! checked somewhere. The reason is not aesthetics: a scope goes into *the key of the
//! state table* (see [`crate::keyspace`]), so a scope with an unknown character or longer
//! than the cap is a broken key — and a broken key either panics on the write path or
//! (worse) points into someone else's slot. The only place that can stop that is the place
//! where the value is *created*.
//!
//! # Why 36 bytes
//!
//! The vqueue key combines scope + two levels of limit key + service + object key. The cap
//! of a key is [`crate::keys::MAX_KEY`]; capping each component at 36 bytes caps what flow
//! control adds to the key at 111 bytes, so it is never what pushes a key over the cap.
//! That number is also enough for every real tenant name (a UUID without dashes is 32).

use std::fmt;

/// Length cap of one identity component (scope, each limit key level).
pub const MAX_COMPONENT: usize = 36;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentError {
    #[error("{what} is empty — use no-{what} instead of an empty string")]
    Empty { what: &'static str },
    #[error("{what} is {len} bytes long, the limit is {MAX_COMPONENT}")]
    TooLong { what: &'static str, len: usize },
    #[error("{what} has character {ch:?} — only [a-zA-Z0-9_.-] is allowed")]
    BadChar { what: &'static str, ch: char },
    #[error("limit key {raw:?} has {n} levels — only 1 or 2 allowed (l1 or l1/l2)")]
    BadDepth { raw: String, n: usize },
    #[error("pattern {raw:?} has {n} components — only 1, 2 or 3 allowed")]
    BadPatternDepth { raw: String, n: usize },
}

/// A valid component: non-empty, ≤ [`MAX_COMPONENT`] bytes, `[a-zA-Z0-9_.-]`.
///
/// Forbidding `/` is what lets `l1/l2` split unambiguously, and forbidding every other
/// character keeps scopes printable in logs, URLs and metric names without escaping.
fn check_component(what: &'static str, s: &str) -> Result<(), IdentError> {
    if s.is_empty() {
        return Err(IdentError::Empty { what });
    }
    if s.len() > MAX_COMPONENT {
        return Err(IdentError::TooLong { what, len: s.len() });
    }
    if let Some(ch) = s
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '.' || *c == '-'))
    {
        return Err(IdentError::BadChar { what, ch });
    }
    Ok(())
}

// ───────────────────────────────────────────────────────────────── scope

/// The scope of an invocation — **part of the identity of everything inside it**.
///
/// Two consequences, and both must be kept in mind at once:
///
/// 1. The scope decides the **partition** ([`Self::partition_key`]). Every invocation of
///    the same scope lands in one partition, so the scope's concurrency counter is a `u32`
///    variable in the RAM of exactly one process — no consensus, no network step on the
///    path to obtaining a slot.
/// 2. The scope goes into **the key of the state table**. `Cart/a` in scope `bob` is a
///    different instance from `Cart/a` without a scope: its own state, its own queue, its
///    own promises.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scope(Box<str>);

impl Scope {
    pub fn new(s: &str) -> Result<Self, IdentError> {
        check_component("scope", s)?;
        Ok(Self(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The value used to choose the partition.
    ///
    /// **One function in one place**, on purpose. The escape hatch for a huge scope
    /// (spreading one scope over `k` partitions with the limit split as `limit/k`) is to
    /// change exactly this function — and that stays cheap only as long as there is no
    /// second place hashing scopes its own way.
    pub fn partition_key(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scope({:?})", &*self.0)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ───────────────────────────────────────────────────────────────── tenant

/// Tenant — **the isolation boundary** between customers sharing one cluster.
///
/// Not a scope, and cannot replace a scope. A scope gathers everything of its own into
/// **one** partition (that is how its concurrency counter is a variable in RAM); using a
/// scope as a tenant would cap a large tenant at one thread of one node. A tenant spreads
/// over every partition like any other key, while a scope lives *inside* a tenant and
/// keeps its flow control role.
///
/// It goes into the identity of everything with a user-chosen name: the deployment book
/// (service names belong to the tenant), state/promise/idempotency keys, the partition key
/// hash input. Taken from the API key at ingress, never from the path.
///
/// The character rule is slightly stricter than a scope's: a tenant is also **one object
/// path segment** (`deployments/<tenant>/…`), so it must not start with `.` — `..` is a
/// valid scope but a tenant climbing out of its directory.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(Box<str>);

impl TenantId {
    /// The tenant of a cluster without auth, and of everything that existed before tenants.
    pub const DEFAULT: &'static str = "default";

    pub fn new(s: &str) -> Result<Self, IdentError> {
        check_component("tenant", s)?;
        if s.starts_with('.') {
            return Err(IdentError::BadChar {
                what: "tenant",
                ch: '.',
            });
        }
        Ok(Self(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_default(&self) -> bool {
        &*self.0 == Self::DEFAULT
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self(Self::DEFAULT.into())
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantId({:?})", &*self.0)
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ───────────────────────────────────────────────────────────── limit key

/// `l1` or `l1/l2`. **Must come with a scope** — a limit key without a scope is refused at
/// ingress, because without a scope there is no counter to hang it on.
///
/// A limit key does **not** go into identity: two calls with the same target, same scope
/// and same object key but different limit keys still reach **the same** virtual object.
/// That invariant is enforced at the type level — [`crate::keys::QueueName`] takes a limit
/// key, while [`crate::keys::state`] / [`crate::keys::idempotency`] /
/// [`crate::keys::promise`] have no parameter to take one with.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LimitKey {
    l1: Box<str>,
    l2: Option<Box<str>>,
}

impl LimitKey {
    pub fn parse(raw: &str) -> Result<Self, IdentError> {
        let parts: Vec<&str> = raw.split('/').collect();
        match parts.as_slice() {
            [l1] => {
                check_component("limit key", l1)?;
                Ok(Self {
                    l1: (*l1).into(),
                    l2: None,
                })
            }
            [l1, l2] => {
                check_component("limit key", l1)?;
                check_component("limit key", l2)?;
                Ok(Self {
                    l1: (*l1).into(),
                    l2: Some((*l2).into()),
                })
            }
            other => Err(IdentError::BadDepth {
                raw: raw.to_string(),
                n: other.len(),
            }),
        }
    }

    pub fn l1(&self) -> &str {
        &self.l1
    }

    pub fn l2(&self) -> Option<&str> {
        self.l2.as_deref()
    }
}

impl fmt::Debug for LimitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LimitKey({self})")
    }
}

impl fmt::Display for LimitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.l2 {
            Some(l2) => write!(f, "{}/{}", self.l1, l2),
            None => f.write_str(&self.l1),
        }
    }
}

/// The path of a **specific counter**: `[scope]`, `[scope, l1]` or `[scope, l1, l2]`.
///
/// This is what rules match against — not a queue, not an invocation. One property of the
/// flow control design spells it out: you cannot say "this queue is blocked by rule R", it
/// is blocked by a *pair* (rule, specific counter).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum CounterPath {
    Scope(Scope),
    L1(Scope, Box<str>),
    L2(Scope, Box<str>, Box<str>),
}

impl CounterPath {
    /// The three (or fewer) counters a `(scope, limit_key)` adds to **at once**.
    ///
    /// An invocation carrying `l1/l2` adds to all three, and is only admitted when all
    /// three have room. The effective cap is the tightest one.
    pub fn all_for(scope: &Scope, limit: Option<&LimitKey>) -> Vec<Self> {
        let mut out = vec![Self::Scope(scope.clone())];
        if let Some(lk) = limit {
            out.push(Self::L1(scope.clone(), lk.l1.clone()));
            if let Some(l2) = &lk.l2 {
                out.push(Self::L2(scope.clone(), lk.l1.clone(), l2.clone()));
            }
        }
        out
    }

    pub fn depth(&self) -> usize {
        match self {
            Self::Scope(_) => 1,
            Self::L1(..) => 2,
            Self::L2(..) => 3,
        }
    }

    /// The components, to match against a [`RulePattern`].
    pub fn components(&self) -> Vec<&str> {
        match self {
            Self::Scope(s) => vec![s.as_str()],
            Self::L1(s, l1) => vec![s.as_str(), l1],
            Self::L2(s, l1, l2) => vec![s.as_str(), l1, l2],
        }
    }
}

impl fmt::Display for CounterPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.components().join("/"))
    }
}

// ─────────────────────────────────────────────────────────── rule pattern

/// One component of a pattern: a concrete value, or `*`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Comp {
    Any,
    Exact(Box<str>),
}

/// `comp`, `comp/comp` or `comp/comp/comp`, each `comp` a value or `*`.
///
/// A rule only applies to counters **of exactly its depth**: `scope/l1` limits the L1
/// counter, not the scope. Otherwise a two-level rule would be both a scope cap and an L1
/// cap, and nobody could say what the last `set` changed.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RulePattern {
    comps: Vec<Comp>,
}

impl RulePattern {
    pub fn parse(raw: &str) -> Result<Self, IdentError> {
        let parts: Vec<&str> = raw.split('/').collect();
        if parts.is_empty() || parts.len() > 3 {
            return Err(IdentError::BadPatternDepth {
                raw: raw.to_string(),
                n: parts.len(),
            });
        }
        let mut comps = Vec::with_capacity(parts.len());
        for p in parts {
            if p == "*" {
                comps.push(Comp::Any);
            } else {
                check_component("pattern", p)?;
                comps.push(Comp::Exact(p.into()));
            }
        }
        Ok(Self { comps })
    }

    pub fn depth(&self) -> usize {
        self.comps.len()
    }

    /// Whether this pattern matches that counter. The depths must be equal.
    pub fn matches(&self, path: &CounterPath) -> bool {
        let comps = path.components();
        if comps.len() != self.comps.len() {
            return false;
        }
        self.comps.iter().zip(comps).all(|(pat, val)| match pat {
            Comp::Any => true,
            Comp::Exact(e) => &**e == val,
        })
    }

    /// Specificity, ordered **from the scope down**: `checkout/premium` > `checkout/*` >
    /// `*/premium` > `*/*`.
    ///
    /// Returns a bitmask with the leftmost component in the highest bit. Comparing numbers
    /// compares priority, and it is a **total order on the set of rules matching the same
    /// counter**: two patterns with the same depth and the same bitmask differ only in some
    /// `Exact`, and two different `Exact`s at the same position cannot both match one
    /// counter. So "more specific wins" is never ambiguous — see the `never_ties` test.
    pub fn specificity(&self) -> u32 {
        let mut mask = 0u32;
        for (i, c) in self.comps.iter().enumerate() {
            if matches!(c, Comp::Exact(_)) {
                mask |= 1 << (2 - i);
            }
        }
        mask
    }
}

impl fmt::Display for RulePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<&str> = self
            .comps
            .iter()
            .map(|c| match c {
                Comp::Any => "*",
                Comp::Exact(e) => e,
            })
            .collect();
        f.write_str(&parts.join("/"))
    }
}

impl fmt::Debug for RulePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RulePattern({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sc(s: &str) -> Scope {
        Scope::new(s).unwrap()
    }

    #[test]
    fn scope_checked_in_constructor() {
        assert!(Scope::new("acme").is_ok());
        assert!(Scope::new("acme-1_2.3").is_ok());
        assert!(Scope::new("").is_err(), "empty is not a scope");
        assert!(Scope::new(&"a".repeat(MAX_COMPONENT)).is_ok());
        assert!(Scope::new(&"a".repeat(MAX_COMPONENT + 1)).is_err());
        // `/` must be forbidden: it separates limit keys and patterns.
        assert!(Scope::new("a/b").is_err());
        assert!(Scope::new("a b").is_err());
        assert!(Scope::new("*").is_err());
    }

    #[test]
    fn limit_key_at_most_two_levels() {
        let a = LimitKey::parse("growth").unwrap();
        assert_eq!((a.l1(), a.l2()), ("growth", None));
        let b = LimitKey::parse("growth/alice").unwrap();
        assert_eq!((b.l1(), b.l2()), ("growth", Some("alice")));
        assert_eq!(b.to_string(), "growth/alice");
        assert!(LimitKey::parse("a/b/c").is_err());
        assert!(LimitKey::parse("a/").is_err());
        assert!(LimitKey::parse("").is_err());
    }

    #[test]
    fn counter_path_spans_all_three_levels() {
        let s = sc("acme");
        let lk = LimitKey::parse("growth/alice").unwrap();
        let all = CounterPath::all_for(&s, Some(&lk));
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].to_string(), "acme");
        assert_eq!(all[1].to_string(), "acme/growth");
        assert_eq!(all[2].to_string(), "acme/growth/alice");

        // No limit key ⇒ only the scope counter. A parent counter is larger than the sum of
        // its children, since some invocations call directly at the parent level.
        assert_eq!(CounterPath::all_for(&s, None).len(), 1);

        let one = LimitKey::parse("growth").unwrap();
        assert_eq!(CounterPath::all_for(&s, Some(&one)).len(), 2);
    }

    #[test]
    fn pattern_matches_only_its_depth() {
        let p = RulePattern::parse("acme/growth").unwrap();
        assert_eq!(p.depth(), 2);
        assert!(p.matches(&CounterPath::L1(sc("acme"), "growth".into())));
        // Same prefix but a different depth ⇒ **no** match. A `scope/l1` rule limits the L1
        // counter, not the scope.
        assert!(!p.matches(&CounterPath::Scope(sc("acme"))));
        assert!(!p.matches(&CounterPath::L2(
            sc("acme"),
            "growth".into(),
            "alice".into()
        )));
    }

    #[test]
    fn star_matches_anything_at_same_depth() {
        let p = RulePattern::parse("*/*/tenant1").unwrap();
        assert!(p.matches(&CounterPath::L2(sc("a"), "x".into(), "tenant1".into())));
        assert!(p.matches(&CounterPath::L2(sc("b"), "y".into(), "tenant1".into())));
        assert!(!p.matches(&CounterPath::L2(sc("a"), "x".into(), "tenant2".into())));
    }

    #[test]
    fn more_specific_wins() {
        let rank = |s: &str| RulePattern::parse(s).unwrap().specificity();
        assert!(rank("checkout/premium") > rank("checkout/*"));
        assert!(rank("checkout/*") > rank("*/premium"));
        assert!(rank("*/premium") > rank("*/*"));
    }

    /// **Property test.** For every set of patterns, the rules matching one counter always
    /// have exactly one winner — no ties.
    ///
    /// Proven exhaustively at depth 3: generate every pattern over a small alphabet,
    /// generate every counter, and check that patterns matching the same counter have
    /// pairwise different `specificity`.
    #[test]
    fn never_ties() {
        let alphabet = ["a", "b", "*"];
        let mut patterns = Vec::new();
        for x in alphabet {
            for y in alphabet {
                for z in alphabet {
                    patterns.push(RulePattern::parse(&format!("{x}/{y}/{z}")).unwrap());
                }
            }
        }
        let vals = ["a", "b"];
        for x in vals {
            for y in vals {
                for z in vals {
                    let path = CounterPath::L2(sc(x), y.into(), z.into());
                    let hit: Vec<&RulePattern> =
                        patterns.iter().filter(|p| p.matches(&path)).collect();
                    let mut ranks: Vec<u32> = hit.iter().map(|p| p.specificity()).collect();
                    let total = ranks.len();
                    ranks.sort_unstable();
                    ranks.dedup();
                    assert_eq!(
                        ranks.len(),
                        total,
                        "two rules match {path} with the same specificity: {hit:?}"
                    );
                    // And there is always at least one match (`*/*/*`), so "winning" is
                    // defined for every counter.
                    assert!(total > 0);
                }
            }
        }
    }
}
