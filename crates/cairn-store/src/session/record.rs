// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The session record on the bucket: [`SessionState`], [`SessionRecord`], paths, reads.

use super::*;

// ───────────────────────────────────────────────────────────── record

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Live,
    Expired,
    Closed,
}

impl SessionState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Expired => "expired",
            Self::Closed => "closed",
        }
    }

    pub fn is_terminal(self) -> bool {
        self != Self::Live
    }
}

/// The record's renewal mode (`mode=`; read leniently, absent = [`SessionMode::Direct`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SessionMode {
    /// Today's protocol: the owner CASes a renewal every TTL/5, observers apply the silence
    /// rule LS-1.
    #[default]
    Direct,
    /// **Solo**: the owner stops PUTting renewals and renews its grant by an unchallenged
    /// self-check GET. Observers do **not** apply LS-1: they challenge (the `challenged=`
    /// line) and then wait a TTL (LS-16).
    Solo,
    /// **Delegated** (LS-14): the owner stops PUTting renewals and renews its grant by the
    /// ack of coordinator `coord` at term `term` (`mode=delegated` plus the line
    /// `coord=<node>/<inc>:<term>`). Only that coordinator expires it, after one silent TTL
    /// counted from the latest proof of life (a heartbeat or a new version); other observers
    /// **defer** (LS-15): they only expire it one TTL after `max(when they saw the role
    /// lost, when they saw this version)`.
    Delegated { coord: SessionId, term: u64 },
    /// A value this binary does not know (a mode of a newer binary, or `delegated` without
    /// a readable `coord=` line). Kept verbatim. Observers neither expire nor challenge it —
    /// without knowing its rules there is nothing to prove.
    Other(String),
}

impl SessionMode {
    fn parse(v: &str, coord: Option<&str>) -> Self {
        match v {
            "direct" => Self::Direct,
            "solo" => Self::Solo,
            "delegated" => coord
                .and_then(|c| {
                    let (id, term) = c.rsplit_once(':')?;
                    Some(Self::Delegated {
                        coord: SessionId::parse(id)?,
                        term: term.parse().ok()?,
                    })
                })
                .unwrap_or_else(|| Self::Other(v.to_string())),
            other => Self::Other(other.to_string()),
        }
    }

    /// Delegated to exactly `coord` at term `term`.
    pub fn delegated_to(&self, coord: &SessionId, term: u64) -> bool {
        matches!(self, Self::Delegated { coord: c, term: t } if c == coord && *t == term)
    }
}

/// Content of `sessions/<node_id>/<inc>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRecord {
    pub node: String,
    /// Bumped on every write — both the measure of life and death (not the etag: the etag
    /// of a torn object flips back and forth) and a way to make the content change so the
    /// etag changes.
    pub beat: u64,
    /// Internal port — see [`LeaseConfig::address`].
    pub address: Option<String>,
    /// Ingress port — see [`LeaseConfig::ingress`]. The `ingress=` line was added later;
    /// older readers skip unknown lines, so writing it breaks nobody.
    pub ingress: Option<String>,
    /// The last migration step the session's binary knows (`knows=`). `None` = a binary
    /// from before the migration framework. Only so operators can see which binaries the
    /// cluster mixes; the decision to run or not lives in `cluster-version`.
    pub knows: Option<u32>,
    /// The node's slowness score 1–100 (`slow=`). `None` = the node does not measure (an
    /// old binary, or the first round has not finished).
    pub slow: Option<u32>,
    /// p99 PUT latency of the latest round, ms (`p99_put_ms=`).
    pub p99_put_ms: Option<u64>,
    /// The node is shedding its leadership because it is slow (`draining=1`): other nodes
    /// do not count it in the fair share.
    pub draining: bool,
    /// Hint: range id (`RangeId.0`) → the term this session holds. The source of truth is
    /// `owners/`; the hint is only for routing and waking waiters.
    pub owns: BTreeMap<u32, u64>,
    pub state: SessionState,
    /// The renewal mode (`mode=`; only written when not `direct`).
    pub mode: SessionMode,
    /// A pending challenge (`challenged=<node>/<inc>`): a newcomer that sees another
    /// session's `solo` record CASes this line in, keeping the other lines (beat
    /// unchanged). The owner, seeing it, answers by CASing back to `direct`, `beat+1`,
    /// dropping the line.
    pub challenged: Option<String>,
}

pub(crate) const SESSION_HEADER: &str = "cairnflow-session/1";

impl SessionRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "{SESSION_HEADER}\nnode={}\nbeat={}\nstate={}\n",
            self.node,
            self.beat,
            self.state.as_str()
        );
        if let Some(a) = &self.address {
            out.push_str(&format!("address={a}\n"));
        }
        if let Some(a) = &self.ingress {
            out.push_str(&format!("ingress={a}\n"));
        }
        if let Some(k) = self.knows {
            out.push_str(&format!("knows={k}\n"));
        }
        if let Some(v) = self.slow {
            out.push_str(&format!("slow={v}\n"));
        }
        if let Some(v) = self.p99_put_ms {
            out.push_str(&format!("p99_put_ms={v}\n"));
        }
        if self.draining {
            out.push_str("draining=1\n");
        }
        if !self.owns.is_empty() {
            let owns: Vec<String> = self.owns.iter().map(|(p, t)| format!("{p}:{t}")).collect();
            out.push_str(&format!("owns={}\n", owns.join(",")));
        }
        match &self.mode {
            SessionMode::Direct => {}
            SessionMode::Solo => out.push_str("mode=solo\n"),
            SessionMode::Delegated { coord, term } => {
                out.push_str(&format!("mode=delegated\ncoord={coord}:{term}\n"))
            }
            SessionMode::Other(m) => out.push_str(&format!("mode={m}\n")),
        }
        if let Some(c) = &self.challenged {
            out.push_str(&format!("challenged={c}\n"));
        }
        out.into_bytes()
    }

    pub fn decode(raw: &[u8]) -> Result<SessionRecord> {
        let bad = |reason: String| StoreError::LeaseCorrupt { reason };
        let text = std::str::from_utf8(raw).map_err(|e| bad(format!("not UTF-8: {e}")))?;
        let mut lines = text.lines();
        let header = lines.next().unwrap_or_default();
        if header != SESSION_HEADER {
            if let Some(e) =
                crate::format::newer_header("sessions/<node>/<inc>", SESSION_HEADER, header)
            {
                return Err(e);
            }
            return Err(bad(format!("missing header {SESSION_HEADER}")));
        }
        let (mut node, mut beat, mut state, mut address) = (None, None, None, None);
        let mut ingress = None;
        let mut knows = None;
        let (mut slow, mut p99_put_ms, mut draining) = (None, None, false);
        let mut owns = BTreeMap::new();
        let (mut mode, mut coord, mut challenged) = (None, None, None);
        for line in lines {
            match line.split_once('=') {
                Some(("node", v)) => node = Some(v.to_string()),
                Some(("beat", v)) => beat = v.parse().ok(),
                Some(("state", v)) => {
                    state = match v {
                        "live" => Some(SessionState::Live),
                        "expired" => Some(SessionState::Expired),
                        "closed" => Some(SessionState::Closed),
                        _ => None,
                    }
                }
                Some(("address", v)) => address = Some(v.to_string()),
                Some(("ingress", v)) => ingress = Some(v.to_string()),
                // Read leniently like every other line: damaged counts as absent.
                Some(("knows", v)) => knows = v.parse().ok(),
                Some(("slow", v)) => slow = v.parse().ok(),
                Some(("p99_put_ms", v)) => p99_put_ms = v.parse().ok(),
                Some(("draining", v)) => draining = v == "1",
                Some(("mode", v)) => mode = Some(v),
                Some(("coord", v)) => coord = Some(v),
                Some(("challenged", v)) => challenged = Some(v.to_string()),
                Some(("owns", v)) => {
                    for pair in v.split(',').filter(|s| !s.is_empty()) {
                        let (p, t) = pair
                            .split_once(':')
                            .ok_or_else(|| bad(format!("bad owns: {pair}")))?;
                        let p = p.parse().map_err(|_| bad(format!("bad owns: {pair}")))?;
                        let t = t.parse().map_err(|_| bad(format!("bad owns: {pair}")))?;
                        owns.insert(p, t);
                    }
                }
                _ => {}
            }
        }
        Ok(SessionRecord {
            node: node.ok_or_else(|| bad("missing node".into()))?,
            beat: beat.ok_or_else(|| bad("missing beat".into()))?,
            address,
            ingress,
            knows,
            slow,
            p99_put_ms,
            draining,
            owns,
            state: state.ok_or_else(|| bad("missing state".into()))?,
            mode: mode.map_or(SessionMode::Direct, |m| SessionMode::parse(m, coord)),
            challenged,
        })
    }
}

pub(super) fn sessions_dir(prefix: &Path) -> String {
    if prefix.as_ref().is_empty() {
        "sessions".into()
    } else {
        format!("{}/sessions", prefix.as_ref())
    }
}

pub(crate) fn session_path(prefix: &Path, id: &SessionId) -> Path {
    Path::from(format!("{}/{}/{}", sessions_dir(prefix), id.node, id.inc))
}

/// [`sessions_dir`] for users outside the module (`inspect`).
pub(crate) fn sessions_dir_of(prefix: &Path) -> String {
    sessions_dir(prefix)
}

/// [`session_id_of`] for users outside the module (`inspect`).
pub(crate) fn session_id_from(prefix: &Path, location: &Path) -> Option<SessionId> {
    session_id_of(prefix, location)
}

/// The session identity from an object path, if it is indeed a session object.
pub(super) fn session_id_of(prefix: &Path, location: &Path) -> Option<SessionId> {
    let dir = format!("{}/", sessions_dir(prefix));
    SessionId::parse(location.as_ref().strip_prefix(&dir)?)
}

/// Like [`read_session`], with the raw bytes — for whoever must write the record back
/// verbatim (a challenge).
pub(crate) async fn read_session_raw(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &SessionId,
) -> Result<Option<(bytes::Bytes, Result<SessionRecord>, UpdateVersion)>> {
    let got = match store.get(&session_path(prefix, id)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got.bytes().await.map_err(io)?;
    let decoded = SessionRecord::decode(&raw);
    Ok(Some((raw, decoded, version)))
}

pub(crate) async fn read_session(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    id: &SessionId,
) -> Result<Option<(Result<SessionRecord>, UpdateVersion)>> {
    let got = match store.get(&session_path(prefix, id)).await {
        Ok(g) => g,
        Err(OsError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io(e)),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let raw = got.bytes().await.map_err(io)?;
    Ok(Some((SessionRecord::decode(&raw), version)))
}
