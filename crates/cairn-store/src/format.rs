// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The bucket's data format marker.
//!
//! In the project's early research phase the data format was **broken on purpose** —
//! there were no users whose compatibility had to be kept. What was never allowed is
//! breaking it *silently*: a new binary reading old data reports no error at all, it just
//! misunderstands every byte, and the incident surfaces somewhere nobody can trace back
//! to the cause.
//!
//! So the bucket carries an object `<prefix>/format` recording the version, and every
//! partition open checks it first:
//!
//! | Bucket | Outcome |
//! |---|---|
//! | empty | write the current version marker (`PutMode::Create`), open normally |
//! | has a marker with the right version | open normally |
//! | has a marker with another version | **refuse**, stating both numbers |
//! | has `ranges/` or `partition-*` but no marker | **refuse** — data from before markers existed |
//!
//! Every change that alters the format on the store had to bump [`DATA_FORMAT`].
//!
//! # Frozen at 10 (D27)
//!
//! From `DATA_FORMAT` 9 on, **every bucket change needs a migration path** — the new
//! binary reads the old format, or there is a conversion tool — and D19 ("formats may
//! break, delete the bucket") no longer applies. **One-time exception:** the query layer
//! (a deliberate decision to break compatibility, buckets included) bumped the format to
//! 10 with no gate and no backfill of old rows — a format 9 bucket is refused like any
//! other format. The D27 rule applies again from 10. The format 10 sample bucket lives in
//! `crates/cairn-server/tests/fixtures/format10/`; the test
//! `format_10_fixture_still_opens` requires every later binary to open it and read it
//! correctly. That sample bucket changes only together with a bump that has a migration
//! path.
//!
//! Added after the freeze at 9, carried over to 10 (migration path = absent means the
//! default): the `range-windows` object — a format 9 bucket that does not have it reads as
//! "every range follows the node's window".
//! The `audit/ctl/<ts>` object — the audit record of `cairn-ctl --apply`; no binary reads
//! it to run, absent means "nobody has edited by hand".
//!
//! # Per-object compatibility policy
//!
//! After the freeze, the bucket changes **object by object**, each object with its own
//! rule. The [`OBJECT_POLICIES`] table is the source of truth; in summary:
//!
//! | Read | Adding a field | Changing the layout |
//! |---|---|---|
//! | **lenient** — unknown lines/keys are skipped (or kept verbatim) | free, old binaries still read it | change the header |
//! | **strict** — an unknown line refuses the whole object | **not allowed**: every change is a header change (`cairnflow-rules/2`) | change the header |
//!
//! Changing an object's header is a **migration step** (registry in `crate::migrate`),
//! not a [`DATA_FORMAT`] bump. A binary that meets a header of the same family with a
//! higher number than the one it writes **stops loudly** with [`StoreError::NewerFormat`]
//! — it does not start, does not overwrite — because reading on means running with half
//! the configuration (for `rules`: fewer caps than the operator thinks), and writing back
//! with the old layout silently erases what the new binary added.

use {
    crate::error::{Result, StoreError},
    slatedb::object_store::{path::Path, Error as OsError, ObjectStore, ObjectStoreExt, PutMode},
    std::sync::Arc,
};

/// The data format version this binary reads and writes.
///
/// Bumped on every change that makes old data read wrong: keys, value encoding, id
/// length. Up to 8: no migration path — old buckets had to be deleted. **From 9: frozen**
/// — see the module head; 10 is the query layer's one-time break.
///
/// | Version | What changed |
/// |---|---|
/// | 1 | first marker |
/// | 2 | cross-partition `StartCall` keyed by call (`sender`), message v3 |
/// | 3 | 24-byte ids carry the partition key; `cluster.conf` v2 carries the key-span table |
/// | 4 | per-node leases: `sessions/` + `owners/` replace `leases/` + `nodes/` (D20) |
/// | 5 | range table (D22): `cluster.conf` v3 — `next_range_id`, `gen`, `intent`; Db at `ranges/<id>`; owner record `owners/range-<id>` carries `wal_kind` |
/// | 6 | three-region keyspace by owner: `0x00`/`0x02 ‖ owner_pk ‖ table ‖ key`, derived region `0x01`; entity rows for timer/retry/purge/delayed; `deployments` table dropped |
/// | 7 | owner record carries the `retired=1` tombstone — an old binary reads it as an ordinary yield and reopens the parent Db |
/// | 8 | `suspended` only means "nothing to wake it yet": loading a completion/signal, or suspending while what it waits for is already in the journal, writes `running`; `recover` only reruns `running` — an old bucket may hold a satisfied `suspended` that this binary never reruns |
/// | 9 | **frozen**: immutable deployments `deployments/<tenant>/<id>` + catalog `catalog/<tenant>`; tenant in the state/promise/idempotency/queue keys and in the partition-key hash; entry 0 carries `cairnflow-deployment`, `cairnflow-tenant`, `cairnflow-restarted-from`; hot table `Pins` (0x15) + index `Deployment` (0x0B), table `Chain` (0x16); outbox v5 / cross-range message v5 carry the tenant; the session record carries `ingress=`; `auth/keys`, `auth/cluster-secret`, `tenants`, `archive/…`, `metering/…` |
/// | 10 | **one-time break** (a deliberate decision): hot row `InvocationMeta` (0x1B); the `paused`/`completed`/`failed` status payload carries its timestamp in the `t` layer; query indexes `ByCreated`/`ByService`/`ByTransition`/`ByAttr` (0x0D–0x10) and the `0x01 ‖ 0xFC` marker in the derived region |
pub const DATA_FORMAT: u32 = 10;

const HEADER: &str = "cairnflow-format";

/// How a binary reads an object of the bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    /// Unknown lines or keys are skipped (or kept verbatim when written back): adding a
    /// field needs no header change.
    Lenient,
    /// Unknown line ⇒ refuse the whole object. Every layout change is a header change.
    Strict,
}

/// The compatibility rule of one object.
#[derive(Clone, Copy, Debug)]
pub struct ObjectPolicy {
    /// The path under the cluster prefix; `<..>` is the varying part.
    pub object: &'static str,
    /// The header this binary writes. Empty = the object has no header (raw content).
    pub header: &'static str,
    pub read: ReadMode,
    /// Why that read mode was chosen.
    pub why: &'static str,
}

/// Every object cairnflow writes to the bucket, with its compatibility rule.
/// Adding a new object means adding a line here (the `every_header_is_in_policy` test
/// of each module compares its header with the table).
pub const OBJECT_POLICIES: &[ObjectPolicy] = &[
    ObjectPolicy {
        object: "format",
        header: HEADER,
        read: ReadMode::Lenient,
        why: "only `version=` is read; other lines are skipped. Written once, never edited in place",
    },
    ObjectPolicy {
        object: "cluster.conf",
        header: "cairnflow-cluster/3",
        read: ReadMode::Lenient,
        why: "unknown `k=v` keys are kept verbatim across every CAS",
    },
    ObjectPolicy {
        object: "cluster-history/<version>",
        header: "cairnflow-cluster/3",
        read: ReadMode::Lenient,
        why: "range-table history: the exact bytes of `cluster.conf` at that version, written \
              with Create **after** the CAS won; absent ≡ no history; only `cairn-ctl table restore` \
              reads it (decoded like `cluster.conf`); the 16 newest are kept, older ones go to the trash",
    },
    ObjectPolicy {
        object: "cluster-version",
        header: "cairnflow-cluster-version/1",
        read: ReadMode::Lenient,
        why: "cluster version and the migration runner's lock; absent ≡ version=0",
    },
    ObjectPolicy {
        object: "owners/range-<id>",
        header: "cairnflow-owner/1",
        read: ReadMode::Lenient,
        why: "`wal_kind`, `retired`, `wal_segment`, `data`, `retired_ms` were added without a header change",
    },
    ObjectPolicy {
        object: "sessions/<node>/<inc>",
        header: "cairnflow-session/1",
        read: ReadMode::Lenient,
        why: "`ingress`, `knows`, `slow`, `p99_put_ms`, `draining`, `mode`, `challenged` were added without a header change",
    },
    ObjectPolicy {
        object: "rules",
        header: "cairnflow-rules/2",
        read: ReadMode::Strict,
        why: "skipping a cap we do not understand means running with fewer caps than the operator thinks. \
              `/2` (migration step `rules/2`) adds `rate`/`burst` and the `tenant` line; `/1` is still read",
    },
    ObjectPolicy {
        object: "catalog/<tenant>",
        header: "cairnflow-catalog v1",
        read: ReadMode::Strict,
        why: "the service → deployment pointer: one misread line means calling the wrong code",
    },
    ObjectPolicy {
        object: "range-windows",
        header: "cairnflow-range-windows/1",
        read: ReadMode::Strict,
        why: "the flush window is a per-range durability commitment; a wrong guess means acking earlier than the plan tier",
    },
    ObjectPolicy {
        object: "auth/keys",
        header: "cairnflow-keys/2",
        read: ReadMode::Strict,
        why: "a key whose role or tenant is misread is a wrongly granted permission. `/2` adds the roles \
              `view`, `provision`; `/1` is still read. The writer keeps `/1` until the `auth/keys-v2` gate is \
              on (old binaries read `/1` strictly), no `Run::Cluster` step",
    },
    ObjectPolicy {
        object: "auth/oidc",
        header: "cairnflow-oidc/1",
        read: ReadMode::Strict,
        why: "OIDC relying-party config: clients by hostname → tenant and role cap — \
              one misread line means the wrong tenant or role. Absent ≡ OIDC off; binaries from before OIDC do \
              not read it (no gate: old binaries refuse v2 cookies, users sign in again)",
    },
    ObjectPolicy {
        object: "auth/revoked-sessions",
        header: "cairnflow-revoked-sessions/1",
        read: ReadMode::Lenient,
        why: "per-session revocation at logout: `sid\\texp\\ttenant` (absent tenant ≡ `\"\"`); unknown lines are skipped; absent ≡ no session revoked; the writer prunes lines past the cookie's expiry, capped at 200 live sessions per tenant, no global cap; a failed read does not close keys, it only closes cookie sessions past the age cap",
    },
    ObjectPolicy {
        object: "auth/revoked",
        header: "cairnflow-revoked/1",
        read: ReadMode::Strict,
        why: "SSO session revocation: missing one line means a revoked session comes back to life. Absent ≡ \
              no revocation; the writer prunes lines past the session TTL cap",
    },
    ObjectPolicy {
        object: "tenants",
        header: "cairnflow-tenants/1",
        read: ReadMode::Lenient,
        why: "unknown flags on a tenant line are skipped",
    },
    ObjectPolicy {
        object: "archive/<tenant>/<day>/<id>",
        header: "cairnflow-archive/1",
        read: ReadMode::Strict,
        why: "compressed binary; each object carries its own header so old and new objects live side by side",
    },
    // Stands **before** the deployment line: that line's `<id>` pattern also matches this name.
    ObjectPolicy {
        object: "deployments/<tenant>/<id>.retiring",
        header: "cairnflow-retiring v1",
        read: ReadMode::Lenient,
        why: "marker of a deployment being removed: only its presence matters, absent = nobody is removing it; the content (node, timestamp, nonce) only serves to resolve ambiguous writes",
    },
    ObjectPolicy {
        object: "deployments/<tenant>/<id>",
        header: "",
        read: ReadMode::Lenient,
        why: "JSON, unknown fields are skipped; immutable — nobody writes it again",
    },
    ObjectPolicy {
        object: "metering/<tenant>/<hour>/<node>",
        header: "",
        read: ReadMode::Lenient,
        why: "each process writes its own object (`<node>.<boot>` today, `<node>` in older binaries); reads merge every object in the hour; unknown keys are skipped",
    },
    ObjectPolicy {
        object: "ops/<key>",
        header: "cairnflow-op/1",
        read: ReadMode::Lenient,
        why: "the operation record of a split/merge: absent ≡ an op from an older binary that wrote only the intent; unknown keys are copied verbatim; unknown phase ⇒ a failed read, the reader keeps or backs off",
    },
    ObjectPolicy {
        object: "gc-barriers/<id>",
        header: "cairnflow-gc-barrier/1",
        read: ReadMode::Lenient,
        why: "unknown keys are skipped; an unreadable barrier blocks every range and is never deleted on its own",
    },
    ObjectPolicy {
        object: "capture-requests/<tenant>",
        header: "cairnflow-captures/1",
        read: ReadMode::Lenient,
        why: "capture command with a TTL; unknown keys and lines are kept verbatim when written back",
    },
    ObjectPolicy {
        object: "tenant-purge/<tenant>/<range>",
        header: crate::tenant_purge::PROGRESS_HEADER,
        read: ReadMode::Lenient,
        why: "per-range \"range is clean\" proof of a tenant purge: written once \
              (`Create`), unknown lines are skipped; absent or unreadable ⇒ the range is scanned and compacted again (it only costs \
              work). Deleted together with the tenant's prefix right before the `purged` tombstone",
    },
    ObjectPolicy {
        object: "captures/<tenant>/<id>",
        header: "cairnflow-capture/1",
        read: ReadMode::Strict,
        why: "binary bundle for `cairn replay --bundle`; each bundle carries its own header",
    },
    ObjectPolicy {
        object: "capture-artifacts/<tenant>/<id>/<sha>",
        header: "",
        read: ReadMode::Lenient,
        why: "raw artifact bytes copied along with a capture bundle (server-side copy); not decoded, named by sha256",
    },
    ObjectPolicy {
        object: "archive/<tenant>/<day>/<id>.artifacts/<sha>",
        header: "",
        read: ReadMode::Lenient,
        why: "raw artifact bytes copied along with the archive (`--archive-artifacts`); \
              not decoded; `list_day` skips the `.artifacts/` directory",
    },
    ObjectPolicy {
        object: "artifacts/<tenant>/<id>/<epoch>/<sha>",
        header: "",
        read: ReadMode::Lenient,
        why: "raw bytes of `ctx.artifact.put`: immutable, named by the sha256 of the \
              content, not decoded; absent ≡ no artifact yet",
    },
    ObjectPolicy {
        object: "values/<tenant>/<pk>/<owner>/<sha>-<gen>",
        header: "",
        read: ReadMode::Lenient,
        why: "a segment of a segmented value: raw bytes, immutable, named by the \
              sha256 of the content plus a `gen` freshly issued at every seal; not decoded; absent ≡ none \
              (a dangling ref is a `verify` finding)",
    },
    ObjectPolicy {
        object: "pricing",
        header: "cairnflow-pricing/1",
        read: ReadMode::Lenient,
        why: "token price table: unknown keys are skipped; corrupt or a newer header ⇒ no \
              prices, metering never fails because of the price table; only `cairn-ctl pricing set` writes it",
    },
    ObjectPolicy {
        object: "stats/<tenant>/<day>/<node>",
        header: "",
        read: ReadMode::Lenient,
        why: "durable per-day statistics (`stats_history`): JSON `{v, rows}`, one object per process \
              (`<node>.<boot>`), reads merge every object of the day; unknown fields are skipped",
    },
    ObjectPolicy {
        object: "incarnations/<node>/<inc>",
        header: "cairnflow-incarnation/1",
        read: ReadMode::Lenient,
        why: "incarnation marker: only LISTed to get a lower bound and compared verbatim with \
              the content of this very open; absent ≡ the old lower bound `now_ms`",
    },
    ObjectPolicy {
        object: "gc-swept",
        header: crate::gc_swept::HEADER,
        read: ReadMode::Lenient,
        why: "registry of swept tombstones: ids of tombstoned ranges whose Db D-d found gone; \
              absent, corrupt or an unknown header ≡ empty (D-d reads every tombstone as before); unknown keys are skipped; a \
              newer header of the same family ⇒ not overwritten; it only makes D-d consider fewer candidates, adds no delete",
    },
    ObjectPolicy {
        object: "gc-clock-probe",
        header: "cairnflow-gc-clock-probe/1",
        read: ReadMode::Lenient,
        why: "GC clock-skew probe: only the HEAD `last_modified` is used, nobody reads \
              the content; overwritten at every measurement; absent means nothing",
    },
    ObjectPolicy {
        object: ".cairnflow-fence-probe/<nonce>",
        header: "",
        read: ReadMode::Lenient,
        why: "conditional-PUT probe of the startup gate (`config/gate.rs`): only the probe itself reads it, compared \
              verbatim; deleted when done, a leftover is harmless",
    },
    ObjectPolicy {
        object: ".cairnflow-cas-probe/<nonce>",
        header: "",
        read: ReadMode::Lenient,
        why: "etag-CAS probe of the startup gate (`config/gate.rs`): like `.cairnflow-fence-probe/`",
    },
    ObjectPolicy {
        object: "audit/ctl/<ts>",
        header: "cairnflow-ctl-audit/1",
        read: ReadMode::Lenient,
        why: "audit record of `cairn-ctl --apply`: read by humans only; unknown keys are skipped",
    },
    ObjectPolicy {
        object: "shedding",
        header: "cairnflow-shedding/1",
        read: ReadMode::Lenient,
        why: "marker \"this node is shedding itself for being slow\": absent ≡ nobody is shedding; unknown keys are skipped",
    },
    ObjectPolicy {
        object: "quarantine/<tenant>",
        header: "cairnflow-quarantine/1",
        read: ReadMode::Lenient,
        why: "quarantine list of poisonous handlers with a TTL: absent ≡ empty; unknown keys and lines are \
              kept verbatim when written back; newer header ⇒ read as empty, not overwritten",
    },
    ObjectPolicy {
        object: "trash/<day>/<id>/entry",
        header: "cairnflow-trash/1",
        read: ReadMode::Lenient,
        why: "describes one deletion moved into the trash; unknown keys are skipped; unreadable \
              (including a newer header) ⇒ the purge loop keeps the entry",
    },
    ObjectPolicy {
        object: "trash/<day>/<id>/obj/<rel>",
        header: "",
        read: ReadMode::Lenient,
        why: "verbatim copy of `<rel>`; only `cairn-ctl trash restore` reads it, copying it back to its original place",
    },
    ObjectPolicy {
        object: "control/registry-gen",
        header: "cairnflow-registry-gen/1",
        read: ReadMode::Lenient,
        why: "cluster-level registry generation: readers only compare the content with the previous read; \
              absent ≡ unknown, every tick rereads the full registry; unknown keys are skipped; newer header ⇒ not overwritten",
    },
    ObjectPolicy {
        object: "control/coordinator",
        header: "cairnflow-coordinator/1",
        read: ReadMode::Lenient,
        why: "the cluster's coordinator: absent ≡ nobody coordinates, every node \
              watches the bucket as it did before the coordinator existed; unknown keys are skipped; garbage ≡ empty (taken by CAS); \
              newer header ⇒ not followed, not overwritten",
    },
    ObjectPolicy {
        object: "control/quiet-lease",
        header: "cairnflow-quiet-lease/1",
        read: ReadMode::Lenient,
        why: "quiet-renewal gate: absent, corrupt or an unknown header ≡ off — the \
              safe path is the current protocol; unknown keys are skipped; only `cairn-ctl cluster` writes it",
    },
    ObjectPolicy {
        object: "control/probe/<nonce>",
        header: "cairnflow-store-probe/1",
        read: ReadMode::Lenient,
        why: "probe object of the conditional-GET test (`store_probe`): only \
              the `cairn-ctl cluster enable-quiet-lease` run itself reads it, compared verbatim; no binary \
              reads it to run; deleted when the test passes, a leftover is harmless",
    },
    ObjectPolicy {
        object: "ops-reports/<id>",
        header: "cairnflow-ops-report/1",
        read: ReadMode::Lenient,
        why: "JSON report of `cairn-ctl … --report-to-bucket <id>`: absent ≡ none yet; \
              written once with Create (id issued by the caller); unknown JSON keys pass through verbatim via \
              `GET /admin/ops-reports/{id}`; no binary reads it to run; `cairn-ctl` cleans up past the count/age \
              caps",
    },
    ObjectPolicy {
        object: "audit/tenant/<tenant>/<day>/<id>",
        header: "",
        read: ReadMode::Lenient,
        why: "data-plane audit log: one small JSON per human action — \
              `r-<row key hex>.json` (in-range action, exported from the `Table::Audit` row), \
              `<ts>-<node>-<boot>-<seq>.intent`/`.done` (action on an object); written once with \
              Create; absent ≡ none yet; unknown JSON keys pass through verbatim via `GET /audit`; no binary \
              reads it to run",
    },
    ObjectPolicy {
        object: "auth/cluster-secret",
        header: "",
        read: ReadMode::Strict,
        why: "raw bytes; written once with Create",
    },
];

/// The rule of `object` (named as in the `object` column of [`OBJECT_POLICIES`]).
pub fn object_policy(object: &str) -> Option<&'static ObjectPolicy> {
    OBJECT_POLICIES.iter().find(|p| p.object == object)
}

/// Layouts owned by a **library**, not by cairnflow: a range's SlateDB Db (manifest,
/// WAL, SST, checkpoint — they change with the `vendor/slatedb` fork, pinned in
/// `library_contracts`) and the shared WAL log (the `nodewal` layout, changes go through
/// M3). Not in [`OBJECT_POLICIES`] because their compatibility rules are the library's
/// rules; [`unlisted_object`] skips them.
pub const LIBRARY_LAYOUTS: &[&str] = &["ranges/<id>/<rel>", "nodewal/<rel>"];

/// A path segment matches a pattern segment: `<..>` matches a non-empty string, the
/// rest matches verbatim (`<id>.artifacts` matches `ab12.artifacts`).
fn segment_matches(pattern: &str, seg: &str) -> bool {
    match pattern.find('<') {
        None => pattern == seg,
        Some(open) => {
            let Some(close) = pattern[open..].find('>').map(|c| open + c) else {
                return pattern == seg;
            };
            let (lit, rest) = (&pattern[..open], &pattern[close + 1..]);
            let Some(tail) = seg.strip_prefix(lit) else {
                return false;
            };
            // `<..>` eats at least one character; try every cut point for the rest.
            (1..=tail.len())
                .filter(|&n| tail.is_char_boundary(n))
                .any(|n| segment_matches(rest, &tail[n..]))
        }
    }
}

/// `rel` matches the pattern `pattern` (the `object` column of [`OBJECT_POLICIES`]): one
/// segment per segment, except that the `<rel>` and `<node>` segments match one or more
/// segments — process-generated node ids contain `/` (`node/<pid>/<ns>`).
pub fn path_matches(pattern: &str, rel: &str) -> bool {
    fn segs(pat: &[&str], got: &[&str]) -> bool {
        match pat.split_first() {
            None => got.is_empty(),
            Some((&("<rel>" | "<node>"), rest)) => (1..=got.len()).any(|n| segs(rest, &got[n..])),
            Some((p, rest)) => got
                .split_first()
                .is_some_and(|(g, more)| segment_matches(p, g) && segs(rest, more)),
        }
    }
    let got: Vec<&str> = rel.split('/').collect();
    !got.iter().any(|s| s.is_empty()) && segs(&pattern.split('/').collect::<Vec<_>>(), &got)
}

/// The [`OBJECT_POLICIES`] line for the object `rel` (path under the cluster
/// prefix).
pub fn policy_of(rel: &str) -> Option<&'static ObjectPolicy> {
    OBJECT_POLICIES.iter().find(|p| path_matches(p.object, rel))
}

/// `rel` is an object cairnflow writes that has **no line** in [`OBJECT_POLICIES`]
/// (and is not under [`LIBRARY_LAYOUTS`]). Cluster tests call it for every object left
/// on the bucket at teardown (`tests/common::Bucket`): a newly written object with a
/// forgotten line turns red, instead of being an object whose read rule nobody knows.
pub fn unlisted_object(rel: &str) -> bool {
    policy_of(rel).is_none() && !LIBRARY_LAYOUTS.iter().any(|p| path_matches(p, rel))
}

/// `cairnflow-rules/2` ⇒ (`cairnflow-rules/`, 2); `cairnflow-catalog v1` ⇒
/// (`cairnflow-catalog v`, 1). No trailing number ⇒ `None`.
fn header_family(h: &str) -> Option<(&str, u32)> {
    let family = h.trim_end_matches(|c: char| c.is_ascii_digit());
    if family.len() == h.len() || family.is_empty() {
        return None;
    }
    Some((family, h[family.len()..].parse().ok()?))
}

/// The header `found` of `object` differs from the header `ours` this binary writes. Same
/// family with a higher number ⇒ a [`StoreError::NewerFormat`] error for the caller to
/// return at once; otherwise (older, other family, garbage) ⇒ `None`, the caller returns
/// its own corruption error.
pub fn newer_header(object: &str, ours: &str, found: &str) -> Option<StoreError> {
    let (fam, ours_n) = header_family(ours)?;
    let (found_fam, found_n) = header_family(found)?;
    (fam == found_fam && found_n > ours_n).then(|| StoreError::NewerFormat {
        object: object.to_string(),
        header: found.to_string(),
    })
}

fn marker_path(prefix: &Path) -> Path {
    if prefix.as_ref().is_empty() {
        Path::from("format")
    } else {
        Path::from(format!("{}/format", prefix.as_ref()))
    }
}

fn encode(version: u32) -> Vec<u8> {
    format!("{HEADER}\nversion={version}\n").into_bytes()
}

fn decode(raw: &[u8]) -> Option<u32> {
    let text = std::str::from_utf8(raw).ok()?;
    let mut lines = text.lines();
    if lines.next()? != HEADER {
        return None;
    }
    lines
        .find_map(|l| l.strip_prefix("version="))
        .and_then(|v| v.trim().parse().ok())
}

fn io(e: OsError) -> StoreError {
    StoreError::Io(e.to_string())
}

/// Read the marker. `Ok(None)` = not there yet. A marker that cannot be decoded counts as
/// version 0 — surely different from [`DATA_FORMAT`], so it goes down the refusal branch
/// instead of being guessed.
async fn read(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<u32>> {
    match store.get(path).await {
        Ok(got) => {
            let raw = got.bytes().await.map_err(io)?;
            Ok(Some(decode(&raw).unwrap_or(0)))
        }
        Err(OsError::NotFound { .. }) => Ok(None),
        Err(e) => Err(io(e)),
    }
}

/// Whether the bucket has any range data yet — `ranges/` of this format, or
/// `partition-*` of the formats before it.
pub(crate) async fn has_partition_data(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
) -> Result<bool> {
    Ok(top_dirs(store, prefix)
        .await?
        .iter()
        .any(|d| is_partition_dir(d)))
}

/// The bucket has had a cluster at `prefix`: range data ([`has_partition_data`]) **or**
/// owner records (`owners/`) — the evidence `ClusterConfig::open` requires before creating
/// a range table. One delimiter LIST at the root of the prefix. `cluster-version`,
/// `format`, `sessions/` do **not** count: a node writes them before opening the table
/// (gate, session, `migrate_cluster`), so a freshly created bucket has them too. LIST
/// fails ⇒ `Err` (never "empty").
pub(crate) async fn has_cluster_data(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<bool> {
    Ok(top_dirs(store, prefix)
        .await?
        .iter()
        .any(|d| is_partition_dir(d) || d == "owners"))
}

fn is_partition_dir(name: &str) -> bool {
    name == "ranges" || name.starts_with("partition-")
}

/// Names of the "directories" right under `prefix` (one delimiter LIST).
async fn top_dirs(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<String>> {
    let listed = if prefix.as_ref().is_empty() {
        store.list_with_delimiter(None).await
    } else {
        store.list_with_delimiter(Some(prefix)).await
    }
    .map_err(io)?;
    Ok(listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().map(str::to_string))
        .collect())
}

fn mismatch(found: Option<u32>) -> StoreError {
    StoreError::DataFormat {
        found,
        expected: DATA_FORMAT,
    }
}

/// Check the format marker; `create` = allowed to write the marker into an empty bucket.
///
/// The read side (follower) calls with `create = false`: a reader writes nothing to the
/// bucket (reader discipline), so on an empty bucket it just goes on.
pub(crate) async fn ensure(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    create: bool,
) -> Result<()> {
    let path = marker_path(prefix);
    match read(store, &path).await? {
        Some(v) if v == DATA_FORMAT => return Ok(()),
        Some(v) => return Err(mismatch(Some(v))),
        None => {}
    }
    if has_partition_data(store, prefix).await? {
        return Err(mismatch(None));
    }
    if !create {
        return Ok(());
    }
    // Ambiguity is resolved by reading back (rule 7): every node writes exactly the same content.
    match crate::cas::put_resolving(store, &path, encode(DATA_FORMAT).into(), PutMode::Create).await
    {
        Ok(crate::cas::Cas::Written(_)) => Ok(()),
        // Several partitions (or several nodes) open an empty bucket together: one side
        // writes, the others read back and compare.
        Ok(crate::cas::Cas::Lost) => match read(store, &path).await? {
            Some(v) if v == DATA_FORMAT => Ok(()),
            other => Err(mismatch(other)),
        },
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rule 7 (ambiguous conditional writes are resolved by reading back): the format
    /// marker was written but the reply is an error. Must see: `ensure` passes (the
    /// read-back sees our own version) instead of the node dying while opening an empty
    /// bucket.
    #[tokio::test]
    async fn ambiguous_marker_write_is_resolved_by_reread() {
        use crate::testkit::{Ambiguity, FaultyStore, Rule};
        for reply in [Ambiguity::Retried, Ambiguity::Error] {
            let faulty =
                FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
            let store: Arc<dyn ObjectStore> = faulty.clone();
            faulty.add_rule(Rule::ambiguous_put("format", reply));
            ensure(&store, &Path::from("p"), true)
                .await
                .unwrap_or_else(|e| panic!("{reply:?}: marker written but an error reported: {e}"));
            assert_eq!(
                read(&store, &marker_path(&Path::from("p"))).await.unwrap(),
                Some(DATA_FORMAT)
            );
        }
    }

    #[test]
    fn newer_header_only_for_same_family_higher_number() {
        let newer = |found| newer_header("rules", "cairnflow-rules/1", found);
        assert!(matches!(
            newer("cairnflow-rules/2"),
            Some(StoreError::NewerFormat { ref object, ref header }) if object == "rules" && header == "cairnflow-rules/2"
        ));
        assert!(newer("cairnflow-rules/12").is_some());
        for not_newer in [
            "cairnflow-rules/1",
            "cairnflow-rules/0",
            "cairnflow-rule/2",
            "cairnflow-rules/",
            "junk",
            "",
        ] {
            assert!(newer(not_newer).is_none(), "{not_newer}");
        }
        assert!(newer_header("catalog", "cairnflow-catalog v1", "cairnflow-catalog v2").is_some());
        assert!(newer_header("catalog", "cairnflow-catalog v1", "cairnflow-catalog/2").is_none());
    }

    #[test]
    fn policy_table_has_unique_objects() {
        let mut seen = std::collections::BTreeSet::new();
        for p in OBJECT_POLICIES {
            assert!(seen.insert(p.object), "{} repeated", p.object);
            // The `format` marker carries its number in the `version=` line, not in the header.
            if !p.header.is_empty() && p.object != "format" {
                assert!(
                    header_family(p.header).is_some(),
                    "{}: header {:?} has no number",
                    p.object,
                    p.header
                );
            }
        }
        assert_eq!(object_policy("format").map(|p| p.header), Some(HEADER));
    }

    /// The header each module writes must be the table's header — the table is the source of truth.
    #[test]
    fn every_header_is_in_policy() {
        let archive = std::str::from_utf8(crate::archive::HEADER)
            .unwrap()
            .trim_end();
        for (object, header) in [
            ("cluster.conf", crate::cluster::HEADER),
            ("cluster-history/<version>", crate::cluster::HEADER),
            ("rules", crate::rules::HEADER),
            ("catalog/<tenant>", crate::deployments::CATALOG_HEADER),
            ("owners/range-<id>", crate::owner::OWNER_HEADER),
            ("sessions/<node>/<inc>", crate::session::SESSION_HEADER),
            ("archive/<tenant>/<day>/<id>", archive),
            ("gc-barriers/<id>", crate::gc_barrier::BARRIER_HEADER),
            ("ops/<key>", crate::ledger::OP_HEADER),
            ("audit/ctl/<ts>", crate::inspect::AUDIT_HEADER),
            ("shedding", crate::health::SHED_HEADER),
            ("ops-reports/<id>", crate::ops_report::OPS_REPORT_HEADER),
            (
                "tenant-purge/<tenant>/<range>",
                crate::tenant_purge::PROGRESS_HEADER,
            ),
        ] {
            assert_eq!(
                object_policy(object).map(|p| p.header),
                Some(header),
                "{object}"
            );
        }
    }

    /// Every kind of object cairnflow writes has a line in the table. The paths here are
    /// the real paths the modules build — modules of this crate call the path-building
    /// function itself, modules of `cairn-server` copy the pattern from its doc. A wider net
    /// runs in every cluster test: `tests/common::Bucket` lists the bucket at teardown and
    /// turns red on an object without a line ([`unlisted_object`]).
    #[test]
    fn every_written_object_has_a_policy() {
        let id = [7u8; crate::ID_LEN];
        let sha = [9u8; 32];
        let arts = crate::artifact::ArtifactStore::new(
            Arc::new(slatedb::object_store::memory::InMemory::new()),
            &Path::from(""),
        );
        let owner = crate::artifact::Owner {
            tenant: "acme".into(),
            id,
            epoch: 3,
        };
        let artifact = arts.path(&owner, &sha);
        let hex = crate::id_hex(&id);
        let sha_hex = crate::artifact::sha_hex(&sha);
        for rel in [
            artifact.as_ref().to_string(),
            // `pricing.rs`: `<prefix>/pricing`.
            "pricing".into(),
            // `stats_history.rs`: `stats/<tenant>/<day>/<node>.<boot>`.
            "stats/acme/2026-10-30/node-a.0123456789abcdef".into(),
            // `capture.rs`: capture requests, bundles, bundle artifacts.
            "capture-requests/acme".into(),
            format!("captures/acme/{hex}"),
            format!("capture-artifacts/acme/{hex}/{sha_hex}"),
            // `archive.rs::copy_artifact`.
            format!("archive/acme/2026-10-30/{hex}.artifacts/{sha_hex}"),
            // `session/incarnation.rs`; process-generated node ids contain `/`.
            "incarnations/node-a/1793313787580".into(),
            "incarnations/node/43300/1793313729937688000/1793313729943".into(),
            "sessions/node/43300/1793313729937688000/1793313729943".into(),
            // `gc.rs::CLOCK_PROBE` (cairn-server).
            "gc-clock-probe".into(),
            // `config/gate.rs`: probe objects of the startup gate.
            ".cairnflow-fence-probe/0123abcd".into(),
            ".cairnflow-cas-probe/0123abcd".into(),
            // `cluster_history.rs`.
            crate::cluster_history::path(&Path::from(""), 17)
                .as_ref()
                .to_string(),
            // `tenant_purge.rs`: proof that a range is clean.
            crate::tenant_purge::proof_path(
                &Path::from(""),
                &crate::scope::TenantId::new("acme").unwrap(),
                7,
            )
            .as_ref()
            .to_string(),
            // `ops_report.rs`.
            crate::ops_report::report_path(&Path::from(""), "op-1")
                .as_ref()
                .to_string(),
            // `audit.rs`: rows in the range, intents/outcomes on objects.
            crate::audit::object_path(&Path::from(""), "acme", 0, "r-00ff.json")
                .as_ref()
                .to_string(),
            crate::audit::object_path(&Path::from(""), "ctl", 0, "17-node-a-1-2.intent")
                .as_ref()
                .to_string(),
        ] {
            assert!(!unlisted_object(&rel), "{rel}: no line in OBJECT_POLICIES");
        }
    }

    /// Pattern matcher: `<..>` is one segment (or part of one), `<rel>` at the end is one
    /// or more segments, an empty segment matches nothing.
    #[test]
    fn path_patterns_match_segments() {
        assert!(path_matches("owners/range-<id>", "owners/range-17"));
        assert!(!path_matches("owners/range-<id>", "owners/range-"));
        assert!(!path_matches("owners/range-<id>", "owners/range-1/x"));
        assert!(path_matches(
            "trash/<day>/<id>/obj/<rel>",
            "trash/d/i/obj/a/b/c"
        ));
        assert!(!path_matches("trash/<day>/<id>/obj/<rel>", "trash/d/i/obj"));
        assert!(path_matches("a/<id>.artifacts/<sha>", "a/ab.artifacts/ff"));
        assert!(!path_matches("a/<id>.artifacts/<sha>", "a/.artifacts/ff"));
        assert!(!path_matches("format", "format/"));
        assert!(path_matches(
            "sessions/<node>/<inc>",
            "sessions/node/4330/17933/17933"
        ));
        assert!(path_matches("sessions/<node>/<inc>", "sessions/a/1"));
        assert!(!path_matches("sessions/<node>/<inc>", "sessions/a"));
        assert!(path_matches(
            "ranges/<id>/<rel>",
            "ranges/3/manifest/00000000000000000001.manifest"
        ));
        assert!(unlisted_object("nobody-writes"));
        assert!(!unlisted_object("cluster.conf"));
    }

    #[test]
    fn marker_roundtrip() {
        assert_eq!(decode(&encode(7)), Some(7));
        assert_eq!(decode(b"something else\nversion=1\n"), None);
    }
}
