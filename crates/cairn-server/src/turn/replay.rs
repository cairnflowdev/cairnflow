// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Incremental replay hints for `REQUEST_RESPONSE` (wire v5).
//!
//! **A hint, not proof.** This ledger remembers, in the range term's RAM, that
//! the endpoint has just kept the first `B` journal entries of an invocation.
//! The next turn sends `Start.replay_from = B` together with a `prefix_digest`
//! **recomputed** over the current journal (`cairn_runtime::replay::Opening`) —
//! the SDK compares with its cache, and on a mismatch it answers with a miss
//! and that turn goes again with a full replay. A wrong or lost hint (leader
//! change, node reopened, split/merge — a new `Shared` every term — LRU, TTL)
//! costs only one round trip, and never makes the SDK run on a wrong prefix.
//!
//! One range, one thread (actor `spawn_local`): `RefCell`, no lock. Not
//! durable: no object on the bucket, does not touch the lifecycle core.

use {
    super::*,
    cairn_runtime::replay::{Digest, Opening},
    std::{cell::RefCell, collections::BTreeMap},
};

/// Cap on hints per range (each entry < 200 bytes + affinity ≤ 4 KiB).
pub const MAX_HINTS: usize = 10_000;
/// A hint unused for longer than this is dropped — the SDK's default cache TTL.
pub const HINT_TTL_MS: u64 = 15 * 60 * 1000;
/// Auto-off: the window (number of delta turns) per deployment per range.
pub const WINDOW_TURNS: u32 = 20;
/// Auto-off **by bytes**: within one window, if the actual bytes sent in delta
/// mode (delta body + every resend, before compression) are ≥ 95 % of the full
/// body bytes of those same turns ⇒ delta no longer saves anything significant
/// yet still costs an HTTP round trip on every miss ⇒ turn it off. Bytes are
/// measured, not the miss rate: at 50 % misses with resend-from-`k`, delta is
/// still almost half the cost of full (an older "> 50 % misses" rule traded
/// egress for latency). The 5 % leaves room for the difference between body
/// bytes before compression and bytes on the wire.
pub const AUTO_OFF_BYTES_PERCENT: u64 = 95;
/// How long delta stays off for a deployment after a window with many misses.
pub const OFF_FOR_MS: u64 = 10 * 60 * 1000;
/// Cap on affinity cookie bytes kept for one invocation.
pub const MAX_COOKIE: usize = 4 << 10;
/// Cap on total affinity cookie bytes kept in one range (from a security review).
pub const MAX_COOKIE_BYTES: usize = 4 << 20;

/// The hint of one invocation.
#[derive(Clone, Debug)]
struct Hint {
    b: u32,
    digest: Digest,
    deployment: String,
    affinity: invoker::Affinity,
    tick: u64,
    at_ms: u64,
}

/// Byte counters of one deployment in the range.
#[derive(Clone, Copy, Debug, Default)]
struct Window {
    turns: u32,
    /// Actual bytes sent in delta mode, including every resend.
    delta_bytes: u64,
    /// Full body bytes of those same turns.
    full_bytes: u64,
    off_until_ms: u64,
}

#[derive(Default)]
struct Inner {
    by_id: BTreeMap<InvocationId, Hint>,
    /// `tick` → id: most-recent-use order, to evict the oldest entry.
    lru: BTreeMap<u64, InvocationId>,
    tick: u64,
    /// Total affinity cookie bytes held (cap [`MAX_COOKIE_BYTES`]).
    cookie_bytes: usize,
    windows: BTreeMap<String, Window>,
    /// Invocations already warned about journal size in this term, capped.
    warned: std::collections::BTreeSet<InvocationId>,
}

/// The hint ledger of one range term.
#[derive(Default)]
pub struct ReplayHints {
    inner: RefCell<Inner>,
    /// `--no-rr-delta-replay`: never send a delta.
    disabled: bool,
}

/// What this turn sends.
#[derive(Debug, Default)]
pub(super) struct Plan {
    /// `B`: `0` = full replay.
    pub from: u32,
    /// Why full — the metric label when `from == 0`.
    pub why_full: Option<&'static str>,
    pub affinity: invoker::Affinity,
}

impl ReplayHints {
    pub fn new(enabled: bool) -> Self {
        Self {
            inner: RefCell::default(),
            disabled: !enabled,
        }
    }

    /// Picks `B` for `id`'s turn on `deployment`: there is a hint for the same
    /// deployment, the deployment negotiated v5 and declares `replayCache`, the
    /// node has not disabled it, the deployment is not currently auto-off,
    /// `B ≤ N`, and `h_B` recomputed over the current journal **equals** `h_B`
    /// at the time the hint was recorded. Any of these false ⇒ full.
    pub(super) fn plan(
        &self,
        id: &InvocationId,
        deployment: &Deployment,
        opening: Option<&Opening>,
        now_ms: u64,
    ) -> Plan {
        let mut inner = self.inner.borrow_mut();
        let Some(hint) = inner.by_id.get(id).cloned() else {
            return Plan {
                why_full: Some("no_hint"),
                ..Plan::default()
            };
        };
        if hint.deployment != deployment.id {
            inner.remove(id);
            return Plan {
                why_full: Some("no_hint"),
                ..Plan::default()
            };
        }
        if now_ms.saturating_sub(hint.at_ms) > HINT_TTL_MS {
            inner.remove(id);
            return Plan {
                why_full: Some("no_hint"),
                ..Plan::default()
            };
        }
        inner.touch(id);
        let affinity = hint.affinity.clone();
        let full = |why| Plan {
            from: 0,
            why_full: Some(why),
            affinity: affinity.clone(),
        };
        if self.disabled
            || !delta_capable(deployment)
            || inner
                .windows
                .get(&deployment.id)
                .is_some_and(|w| w.off_until_ms > now_ms)
        {
            return full("disabled");
        }
        let Some(opening) = opening else {
            return full("no_hint");
        };
        if hint.b == 0 || hint.b > opening.known_entries() {
            return full("no_hint");
        }
        // The journal differs from when the SDK kept it (does not happen on the
        // normal path): sending a delta would only get a miss.
        if opening.digest(hint.b) != Some(&hint.digest) {
            inner.remove(id);
            return full("no_hint");
        }
        Plan {
            from: hint.b,
            why_full: None,
            affinity,
        }
    }

    /// After a delta turn (`from > 0`): the actual bytes sent (`sent`, every
    /// attempt) and the turn's full body bytes (`full`) — the byte-based
    /// auto-off counter.
    pub(super) fn note(&self, deployment: &str, sent: u64, full: u64, now_ms: u64) {
        let mut inner = self.inner.borrow_mut();
        let w = inner.windows.entry(deployment.to_string()).or_default();
        w.turns += 1;
        w.delta_bytes += sent;
        w.full_bytes += full;
        if w.turns >= WINDOW_TURNS {
            if w.delta_bytes * 100 >= w.full_bytes * AUTO_OFF_BYTES_PERCENT {
                w.off_until_ms = now_ms + OFF_FOR_MS;
                crate::metrics::sdk::bump(&crate::metrics::sdk::RR_REPLAY_AUTO_OFF);
            }
            w.turns = 0;
            w.delta_bytes = 0;
            w.full_bytes = 0;
        }
    }

    /// After a successful turn: the SDK kept the first `cache` entries ⇒ hint
    /// `B = min(k, N)`; kept none ⇒ drop the hint (and its affinity).
    pub(super) fn record(
        &self,
        id: &InvocationId,
        deployment: &Deployment,
        opening: Option<&Opening>,
        reply_cache: Option<u32>,
        affinity: invoker::Affinity,
        now_ms: u64,
    ) {
        let mut inner = self.inner.borrow_mut();
        let (Some(k), Some(opening), true) = (reply_cache, opening, deployment.replay_cache) else {
            inner.remove(id);
            return;
        };
        let b = k.min(opening.known_entries());
        let Some(digest) = opening.digest(b).copied().filter(|_| b > 0) else {
            inner.remove(id);
            return;
        };
        // A new affinity wins; when the response carries none, keep the old one
        // (load balancers only set the cookie on the first response).
        let old = inner.by_id.get(id).map(|h| h.affinity.clone());
        let affinity = invoker::Affinity {
            cookie: affinity
                .cookie
                .or_else(|| old.as_ref().and_then(|a| a.cookie.clone())),
            fly: affinity
                .fly
                .or_else(|| old.as_ref().and_then(|a| a.fly.clone())),
        };
        inner.remove(id);
        // The range's cookie byte cap (from a security review): when over it,
        // keep the hint without the cookie — delta still runs, only this
        // invocation's cookie affinity is lost.
        let mut affinity = affinity;
        let add = affinity.cookie.as_ref().map_or(0, String::len);
        if inner.cookie_bytes + add > MAX_COOKIE_BYTES {
            affinity.cookie = None;
        } else {
            inner.cookie_bytes += add;
        }
        inner.tick += 1;
        let tick = inner.tick;
        inner.lru.insert(tick, *id);
        inner.by_id.insert(
            *id,
            Hint {
                b,
                digest,
                deployment: deployment.id.clone(),
                affinity,
                tick,
                at_ms: now_ms,
            },
        );
        while inner.by_id.len() > MAX_HINTS {
            let Some((_, oldest)) = inner.lru.first_key_value().map(|(t, i)| (*t, *i)) else {
                break;
            };
            inner.remove(&oldest);
        }
    }

    /// A failed turn (transport, protocol): drop the hint — the next turn is full.
    pub(super) fn forget(&self, id: &InvocationId) {
        self.inner.borrow_mut().remove(id);
    }

    /// `true` exactly once per `id` in the term (journal size warning).
    pub(super) fn warn_once(&self, id: &InvocationId) -> bool {
        let mut inner = self.inner.borrow_mut();
        if inner.warned.len() >= MAX_HINTS {
            inner.warned.clear();
        }
        inner.warned.insert(*id)
    }

    /// Number of hints held (tests).
    pub fn len(&self) -> usize {
        self.inner.borrow().by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Inner {
    fn remove(&mut self, id: &InvocationId) {
        if let Some(h) = self.by_id.remove(id) {
            self.lru.remove(&h.tick);
            self.cookie_bytes -= h.affinity.cookie.as_ref().map_or(0, String::len);
        }
    }

    fn touch(&mut self, id: &InvocationId) {
        self.tick += 1;
        let tick = self.tick;
        if let Some(h) = self.by_id.get_mut(id) {
            self.lru.remove(&h.tick);
            h.tick = tick;
            self.lru.insert(tick, *id);
        }
    }
}

/// A deployment that can take a delta: negotiated v5, declares `replayCache`, `REQUEST_RESPONSE`.
pub(super) fn delta_capable(d: &Deployment) -> bool {
    d.wire_version >= crate::discovery::WIRE_REPLAY_DELTA
        && d.replay_cache
        && d.protocol_mode == ProtocolMode::RequestResponse
}

/// `name=value` pairs of the `Set-Cookie`s (attributes dropped), joined by
/// `; `, capped at [`MAX_COOKIE`]; a pair whose name/value has odd characters
/// is dropped. `None` = nothing.
pub fn cookie_of<'a>(set_cookies: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut out = String::new();
    for sc in set_cookies {
        let pair = sc.split(';').next().unwrap_or("").trim();
        let Some((name, value)) = pair.split_once('=') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        let token = |s: &str, extra: &[char]| {
            !s.is_empty()
                && s.chars().all(|c| {
                    c.is_ascii_alphanumeric() || "-_.!#$%&'*+^`|~".contains(c) || extra.contains(&c)
                })
        };
        if !token(name, &[]) || !(value.is_empty() || token(value, &['=', '/', ':', '"'])) {
            continue;
        }
        let add = name.len() + 1 + value.len() + if out.is_empty() { 0 } else { 2 };
        if out.len() + add > MAX_COOKIE {
            break;
        }
        if !out.is_empty() {
            out.push_str("; ");
        }
        out.push_str(name);
        out.push('=');
        out.push_str(value);
    }
    (!out.is_empty()).then_some(out)
}

/// `fly=<id>` of the `cairn-affinity` header ⇒ a Fly.io machine id (alphanumeric, ≤ 64 characters).
pub fn fly_of(v: &str) -> Option<String> {
    v.split(',').find_map(|part| {
        let id = part.trim().strip_prefix("fly=")?.trim();
        (!id.is_empty() && id.len() <= 64 && id.chars().all(|c| c.is_ascii_alphanumeric()))
            .then(|| id.to_string())
    })
}

#[cfg(test)]
mod tests {
    use {super::*, cairn_protocol::pb};

    #[test]
    fn cookie_keeps_pairs_drops_attributes_and_junk() {
        let c = cookie_of(
            [
                "AWSALB=abc/def==; Expires=Tue, 10 Nov 2026 00:00:00 GMT; Path=/",
                "bad name=x",
                "evil=a\r\nX-Injected: 1",
                "srv=2; HttpOnly",
            ]
            .into_iter(),
        );
        assert_eq!(c.as_deref(), Some("AWSALB=abc/def==; srv=2"));
        assert_eq!(cookie_of(std::iter::empty()), None);
        let big = "a=".to_string() + &"x".repeat(MAX_COOKIE);
        assert_eq!(
            cookie_of([big.as_str()].into_iter()),
            None,
            "dropped when over the cap"
        );
    }

    fn deployment() -> Deployment {
        serde_json::from_value(serde_json::json!({
            "id": "dp_test",
            "endpoint": "http://sdk",
            "wireVersion": 5,
            "protocolMode": "REQUEST_RESPONSE",
            "services": [],
            "replayCache": true,
        }))
        .expect("deployment")
    }

    fn opening(id: &InvocationId) -> Opening {
        Opening::new(&[
            Message::Start(pb::StartMessage {
                id: bytes::Bytes::copy_from_slice(id),
                known_entries: 1,
                ..Default::default()
            }),
            Message::Input(pb::InputCommandMessage::default()),
        ])
        .expect("turn opening")
    }

    /// Security review: a malicious SDK returning a 4 KiB cookie for every
    /// invocation must not drive the node's RAM up to `MAX_HINTS × MAX_COOKIE`
    /// per range (≈ 40 MiB, times the number of ranges). Must see: the total
    /// cookie bytes held in one range does not exceed `MAX_COOKIE_BYTES`; hints
    /// are still recorded (delta still runs), only the cookie is lost.
    #[test]
    fn cookie_bytes_are_capped_per_range() {
        let h = ReplayHints::new(true);
        let d = deployment();
        let cookie = format!("lb={}", "x".repeat(MAX_COOKIE - 3));
        let n = MAX_COOKIE_BYTES / MAX_COOKIE + 100;
        for i in 0..n {
            let mut id = [0u8; 24];
            id[..8].copy_from_slice(&(i as u64).to_be_bytes());
            h.record(
                &id,
                &d,
                Some(&opening(&id)),
                Some(1),
                invoker::Affinity {
                    cookie: Some(cookie.clone()),
                    fly: None,
                },
                0,
            );
        }
        assert_eq!(h.len(), n, "hints are still recorded");
        let held: usize = h
            .inner
            .borrow()
            .by_id
            .values()
            .filter_map(|x| x.affinity.cookie.as_ref().map(String::len))
            .sum();
        assert!(held <= MAX_COOKIE_BYTES, "{held} byte cookie");
    }

    #[test]
    fn fly_id_is_alphanumeric_and_short() {
        assert_eq!(
            fly_of("fly=0123456789abcd").as_deref(),
            Some("0123456789abcd")
        );
        assert_eq!(fly_of("other=1, fly=abc").as_deref(), Some("abc"));
        assert_eq!(fly_of("fly=a b"), None);
        assert_eq!(fly_of("fly=a\r\nx"), None);
        assert_eq!(fly_of(&format!("fly={}", "a".repeat(65))), None);
        assert_eq!(fly_of("fly="), None);
    }
}
