// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! A ready-made network filter for in-process test clusters (inspired by TiKV's
//! `test_raftstore` transport simulation).
//!
//! Only with cairn-server's `testkit` feature. One [`Net`] is shared by every node of
//! the cluster ([`net_cluster`]): the sending node is identified by `node_id`, the
//! receiving node by its internal port address (registered with [`Net::register`]).
//! Rules ([`Rule`]) are evaluated in the order they were added; the first rule
//! returning something other than `Pass` wins.
//!
//! ```ignore
//! let (_bucket, net, nodes) = net_cluster(&["a", "b", "c"], 4, TTL).await?;
//! let cut = net.add(isolate("a"));          // a cut off from every peer, still sees the bucket
//! // ...
//! net.remove(cut);                          // healed
//! ```
//!
//! **Do not `sleep` to wait for a blocked message** — use [`Net::wait_held`] or
//! [`Net::wait_dropped`] (callback + channel, AGENTS.md §5).

use {
    anyhow::{anyhow, Result},
    cairn_server::{
        netfilter::{Held, NetFilter},
        node::RunningNode,
    },
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc, Mutex, RwLock,
        },
        time::Duration,
    },
    tokio::sync::watch,
};

pub use cairn_server::netfilter::Verdict;

/// A node-to-node request, as a rule sees it.
#[derive(Clone, Copy, Debug)]
pub struct Msg<'a> {
    /// The sending node's `node_id`.
    pub from: &'a str,
    /// The receiving node's `node_id` if the address is registered, otherwise `host:port`.
    pub to: &'a str,
    /// `path?query`.
    pub path: &'a str,
}

impl Msg<'_> {
    /// The range the request is about: `?range=N` (an inter-range message) or
    /// `/internal/ranges/N/…` (a request to yield the range).
    pub fn range(&self) -> Option<u32> {
        let (path, query) = self.path.split_once('?').unwrap_or((self.path, ""));
        query
            .split('&')
            .find_map(|kv| kv.strip_prefix("range="))
            .and_then(|v| v.parse().ok())
            .or_else(|| {
                path.strip_prefix("/internal/ranges/")
                    .and_then(|rest| rest.split('/').next())
                    .and_then(|v| v.parse().ok())
            })
    }
}

/// A filter rule.
pub trait Rule: Send + Sync + 'static {
    fn verdict(&self, msg: &Msg<'_>) -> Verdict;
}

/// A rule's id within a [`Net`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuleId(u64);

/// The filter shared by the whole test cluster.
pub struct Net {
    names: RwLock<HashMap<String, String>>,
    rules: Mutex<Vec<(u64, Arc<dyn Rule>)>>,
    seq: AtomicU64,
    held: Mutex<Vec<Held>>,
    /// The number of messages ever held / dropped — only grows, so that a test can
    /// wait for "a message has been blocked" without `sleep`.
    held_total: watch::Sender<usize>,
    dropped: Mutex<Vec<String>>,
    dropped_total: watch::Sender<usize>,
    /// Every message through the filter (`from→to path`), whatever its fate.
    seen: Mutex<Vec<String>>,
}

impl Net {
    pub fn new() -> Arc<Net> {
        Arc::new(Net {
            names: RwLock::default(),
            rules: Mutex::default(),
            seq: AtomicU64::new(0),
            held: Mutex::default(),
            held_total: watch::Sender::new(0),
            dropped: Mutex::default(),
            dropped_total: watch::Sender::new(0),
            seen: Mutex::default(),
        })
    }

    /// Give the name `name` to `node`'s internal port.
    pub fn register(&self, name: &str, node: &RunningNode) {
        self.names
            .write()
            .unwrap()
            .insert(node.internal_addr.to_string(), name.to_string());
    }

    pub fn add(&self, rule: impl Rule) -> RuleId {
        let id = self.seq.fetch_add(1, Ordering::Relaxed);
        self.rules.lock().unwrap().push((id, Arc::new(rule)));
        RuleId(id)
    }

    pub fn remove(&self, id: RuleId) {
        self.rules.lock().unwrap().retain(|(i, _)| *i != id.0);
    }

    /// Remove every rule — the network heals. Messages being held stay where they are.
    pub fn heal(&self) {
        self.rules.lock().unwrap().clear();
    }

    /// The `path` of every dropped message, in order.
    pub fn dropped(&self) -> Vec<String> {
        self.dropped.lock().unwrap().clone()
    }

    /// Every message that went through the filter (`from→to path`), in order.
    pub fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    /// The number of messages being held.
    pub fn held_len(&self) -> usize {
        self.held.lock().unwrap().len()
    }

    /// Wait until the **total** number of messages ever held reaches `n`.
    pub async fn wait_held(&self, n: usize, deadline: Duration) -> Result<()> {
        wait_count(&self.held_total, n, deadline, "held messages").await
    }

    /// Wait until the total number of messages ever dropped reaches `n`.
    pub async fn wait_dropped(&self, n: usize, deadline: Duration) -> Result<()> {
        wait_count(&self.dropped_total, n, deadline, "dropped messages").await
    }

    /// Take out every message being held (in the order held), without releasing them.
    pub fn take_held(&self) -> Vec<Held> {
        std::mem::take(&mut *self.held.lock().unwrap())
    }

    /// Release every message being held, one by one. Returns `(path, HTTP code or
    /// error)` for each message.
    pub async fn release_held(&self) -> Vec<(String, Result<u16, String>)> {
        let mut out = Vec::new();
        for h in self.take_held() {
            let path = h.path().to_string();
            out.push((path, h.release().await));
        }
        out
    }

    fn name_of(&self, to: &str) -> String {
        self.names
            .read()
            .unwrap()
            .get(to)
            .cloned()
            .unwrap_or_else(|| to.to_string())
    }
}

async fn wait_count(
    w: &watch::Sender<usize>,
    n: usize,
    deadline: Duration,
    what: &str,
) -> Result<()> {
    let mut rx = w.subscribe();
    tokio::time::timeout(deadline, rx.wait_for(|v| *v >= n))
        .await
        .map_err(|_| {
            anyhow!(
                "past {deadline:?} and still not {n} {what} (only {})",
                *w.borrow()
            )
        })?
        .map_err(|_| anyhow!("Net dropped"))?;
    Ok(())
}

impl NetFilter for Net {
    fn verdict(&self, from: &str, to: &str, path: &str) -> Verdict {
        let to = self.name_of(to);
        let msg = Msg {
            from,
            to: &to,
            path,
        };
        self.seen
            .lock()
            .unwrap()
            .push(format!("{from}→{to} {path}"));
        let rules: Vec<Arc<dyn Rule>> = self
            .rules
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r)| Arc::clone(r))
            .collect();
        let v = rules
            .iter()
            .map(|r| r.verdict(&msg))
            .find(|v| *v != Verdict::Pass)
            .unwrap_or(Verdict::Pass);
        if v == Verdict::Drop {
            self.dropped
                .lock()
                .unwrap()
                .push(format!("{from}→{to} {path}"));
            self.dropped_total.send_modify(|n| *n += 1);
        }
        v
    }

    fn hold(&self, held: Held) {
        self.held.lock().unwrap().push(held);
        self.held_total.send_modify(|n| *n += 1);
    }
}

// ───────────────────────────────────────────────────────── ready-made rules

/// A rule from a closure.
pub struct FnRule<F>(pub F);

impl<F: Fn(&Msg<'_>) -> Verdict + Send + Sync + 'static> Rule for FnRule<F> {
    fn verdict(&self, msg: &Msg<'_>) -> Verdict {
        (self.0)(msg)
    }
}

/// Drop every message between group `a` and group `b`, both directions. Messages
/// within one group (and messages a node sends to itself) go through.
pub fn partition(a: &[&str], b: &[&str]) -> impl Rule {
    let a: Vec<String> = a.iter().map(|s| s.to_string()).collect();
    let b: Vec<String> = b.iter().map(|s| s.to_string()).collect();
    FnRule(move |m: &Msg<'_>| {
        let ina = |n: &str| a.iter().any(|x| x == n);
        let inb = |n: &str| b.iter().any(|x| x == n);
        if (ina(m.from) && inb(m.to)) || (inb(m.from) && ina(m.to)) {
            Verdict::Drop
        } else {
            Verdict::Pass
        }
    })
}

/// Node `n` cut off from **every** peer, both directions. Messages `n` sends to itself
/// go through (that is not the network), and `n` still sees the bucket — cutting
/// *node ↔ bucket* is `FaultyStore`'s job.
pub fn isolate(n: &str) -> impl Rule {
    let n = n.to_string();
    FnRule(move |m: &Msg<'_>| {
        if (m.from == n) != (m.to == n) {
            Verdict::Drop
        } else {
            Verdict::Pass
        }
    })
}

/// A rule by path (like TiKV's `RegionPacketFilter`): matches a path prefix, plus
/// optional conditions on range, sending node, receiving node. A matching message
/// gets `verdict` (default `Drop`); a non-matching message goes through.
pub struct PathFilter {
    path_prefix: String,
    range: Option<u32>,
    from: Option<String>,
    to: Option<String>,
    verdict: Verdict,
    /// Let the first `allow` matching messages through before filtering.
    allow: AtomicUsize,
    /// Filter only while the flag is on.
    when: Option<Arc<AtomicBool>>,
    /// Filter at most that many matching messages, then let them through (`None` = no cap).
    limit: Option<AtomicUsize>,
}

impl PathFilter {
    pub fn new(path_prefix: &str) -> PathFilter {
        PathFilter {
            path_prefix: path_prefix.to_string(),
            range: None,
            from: None,
            to: None,
            verdict: Verdict::Drop,
            allow: AtomicUsize::new(0),
            when: None,
            limit: None,
        }
    }

    pub fn range(mut self, range: u32) -> Self {
        self.range = Some(range);
        self
    }

    pub fn from(mut self, node: &str) -> Self {
        self.from = Some(node.to_string());
        self
    }

    pub fn to(mut self, node: &str) -> Self {
        self.to = Some(node.to_string());
        self
    }

    /// The first `n` matching messages go through; filtering starts from message `n + 1`.
    pub fn allow(self, n: usize) -> Self {
        self.allow.store(n, Ordering::Relaxed);
        self
    }

    /// Filter only while `flag` is on.
    pub fn when(mut self, flag: Arc<AtomicBool>) -> Self {
        self.when = Some(flag);
        self
    }

    /// Filter at most `n` matching messages, then let them through.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(AtomicUsize::new(n));
        self
    }

    pub fn verdict(mut self, v: Verdict) -> Self {
        self.verdict = v;
        self
    }

    /// Hold a matching message instead of dropping it — release with [`Net::release_held`].
    pub fn hold(self) -> Self {
        self.verdict(Verdict::Hold)
    }
}

impl Rule for PathFilter {
    fn verdict(&self, m: &Msg<'_>) -> Verdict {
        let matches = m.path.starts_with(&self.path_prefix)
            && self.range.is_none_or(|r| m.range() == Some(r))
            && self.from.as_deref().is_none_or(|f| f == m.from)
            && self.to.as_deref().is_none_or(|t| t == m.to)
            && self.when.as_ref().is_none_or(|f| f.load(Ordering::SeqCst));
        if !matches {
            return Verdict::Pass;
        }
        let allowed = self
            .allow
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if allowed {
            return Verdict::Pass;
        }
        if let Some(limit) = &self.limit {
            if limit
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_err()
            {
                return Verdict::Pass;
            }
        }
        self.verdict
    }
}

/// Hold messages matching `f` (release with [`Net::release_held`]) — the shape of
/// "a message arriving late after the world has changed".
pub fn hold_then_release(f: PathFilter) -> PathFilter {
    f.hold()
}

// ───────────────────────────────────────────────────────── filtered cluster

/// `ids.len()` nodes on a new bucket, sharing one [`Net`] (names registered).
pub async fn net_cluster(
    ids: &[&str],
    partitions: u16,
    ttl: Duration,
) -> Result<(
    Arc<slatedb::object_store::memory::InMemory>,
    Arc<Net>,
    Vec<super::ClusterNode>,
)> {
    net_cluster_with(ids, partitions, ttl, false).await
}

/// Like [`net_cluster`]; `quiet` ⇒ turn on the `control/quiet-lease` gate before the
/// first node starts (like `cairn-ctl cluster enable-quiet-lease --apply`) — nodes
/// other than the coordinator delegate their renewal.
pub async fn net_cluster_with(
    ids: &[&str],
    partitions: u16,
    ttl: Duration,
    quiet: bool,
) -> Result<(
    Arc<slatedb::object_store::memory::InMemory>,
    Arc<Net>,
    Vec<super::ClusterNode>,
)> {
    net_cluster_tweaked(ids, partitions, ttl, quiet, |_| {}).await
}

/// Like [`net_cluster_with`]; `tweak` adjusts every node's configuration (after the
/// network filter is attached).
pub async fn net_cluster_tweaked(
    ids: &[&str],
    partitions: u16,
    ttl: Duration,
    quiet: bool,
    tweak: impl Fn(&mut cairn_server::node::NodeConfig),
) -> Result<(
    Arc<slatedb::object_store::memory::InMemory>,
    Arc<Net>,
    Vec<super::ClusterNode>,
)> {
    let shared = Arc::new(slatedb::object_store::memory::InMemory::new());
    if quiet {
        let store: Arc<dyn slatedb::object_store::ObjectStore> = shared.clone();
        let on = cairn_store::quiet_lease::QuietLease {
            on: true,
            knows: cairn_store::migrate::QUIET_LEASE,
            by: "net_cluster".into(),
            at_ms: 0,
            delegation: true,
            ..Default::default()
        };
        anyhow::ensure!(
            cairn_store::quiet_lease::write(&store, &Default::default(), &on, None).await?,
            "could not write the gate"
        );
    }
    let net = Net::new();
    let mut nodes = Vec::new();
    for id in ids {
        let filter: Arc<dyn NetFilter> = net.clone();
        let n = super::start_cluster_node_tweaked(&shared, id, partitions, ttl, |cfg| {
            cfg.net_filter = Some(filter);
            tweak(cfg);
        })
        .await?;
        net.register(id, &n.node);
        nodes.push(n);
    }
    Ok((shared, net, nodes))
}
