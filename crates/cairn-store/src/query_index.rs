// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The query index layer: an immutable metadata row per invocation ([`InvocationMeta`],
//! hot table [`Table::InvocationMeta`]) and the **declarative index framework**
//! ([`QUERY_INDEXES`]).
//!
//! Each index is **one descriptor line** — key parts, time axis, classes, condition,
//! covering columns, version. Three users derive from that same table, none has a path of
//! its own:
//!
//! 1. **The write path** — [`event_ops`] computes the `(key, value)` pairs before and after
//!    a transition for each descriptor; [`crate::Mutations::query_event`] applies them.
//! 2. **Background rebuild** — [`rows`] for one invocation at one placement
//!    ([`Placement`]).
//! 3. **The path chooser** of `cairn-query` — [`QueryIndex::key`], [`decode_row`].
//!
//! Adding an index = adding a line to [`QUERY_INDEXES`] (a new code in [`QUERY_CODES`],
//! never reused). Changing a descriptor's layout = bumping its `version`: ranges rebuild
//! just that index.
//!
//! **Classes** ([`Class`]) — `running ↔ suspended` keeps the class, so the most common
//! transition touches no index. A class's `t` mark: `active` ⇒ creation time (from the
//! metadata), `paused`/`completed`/`failed` ⇒ when it entered the class (in the payload of
//! the status row). The rebuild computes exactly the keys live writes created, because
//! both read `t` from the same place ([`Placement::new`]).

use {
    crate::{
        ids::{id_hex, InvocationId, ID_LEN},
        keyspace::{self, Table},
    },
    bytes::{BufMut, BytesMut},
};

// ─────────────────────────────────────────────────────── classes

/// The class of an invocation in the query index — a condensed status that only changes
/// on rare transitions (creation, entering/leaving `paused`, ending).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Class {
    /// `running` hay `suspended`.
    Active = 0,
    Paused = 1,
    Completed = 2,
    Failed = 3,
}

impl Class {
    pub const ALL: [Class; 4] = [
        Class::Active,
        Class::Paused,
        Class::Completed,
        Class::Failed,
    ];

    pub fn from_u8(b: u8) -> Option<Self> {
        Self::ALL.into_iter().find(|c| *c as u8 == b)
    }

    /// Name in the API and SQL.
    pub fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn from_name(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == s)
    }

    /// `completed` hay `failed`.
    pub fn is_ended(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// An invocation's placement in the index: its class and the class's `t` mark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Placement {
    pub class: Class,
    pub t: u64,
}

impl Placement {
    /// **The only function** deriving `t`: `active` ⇒ the metadata's `created_ms`, other
    /// classes ⇒ `at` (when it entered the class, read from the status row). Live writes and
    /// the rebuild both go through here, so they never produce two different keys.
    pub fn new(class: Class, at: u64, meta: &InvocationMeta) -> Self {
        let t = match class {
            Class::Active => meta.created_ms,
            _ => at,
        };
        Self { class, t }
    }
}

// ─────────────────────────────────────────────────────── metadata

/// Cap on user tags.
pub const MAX_TAGS: usize = 8;
pub const MAX_TAG_KEY: usize = 32;
pub const MAX_TAG_VALUE: usize = 64;

/// Layout version of the [`InvocationMeta`] value.
const META_V1: u8 = 1;

const FLAG_ROOT: u8 = 0b1;

/// TLV codes of [`InvocationMeta`]. `0x09–0xFF` reserved — readers skip unknown tags (and
/// keep them verbatim to copy back on `restart-as-new`).
pub mod tlv {
    pub const PARENT: u8 = 0x01;
    pub const SCHEDULE: u8 = 0x02;
    pub const IDEMPOTENCY: u8 = 0x03;
    pub const DEPLOYMENT: u8 = 0x04;
    pub const ATTR: u8 = 0x05;
    pub const ROOT_ID: u8 = 0x06;
    pub const RESTARTED_FROM: u8 = 0x07;
    /// The limit key. Binaries from before it keep it in `unknown` (read leniently, no
    /// change to `META_V1`).
    pub const LIMIT_KEY: u8 = 0x08;
}

/// System attributes of [`BY_ATTR`] (the `$` prefix is reserved for the system).
pub mod attr {
    pub const PARENT: &str = "$parent";
    pub const SCHEDULE: &str = "$schedule";
    pub const DEPLOYMENT: &str = "$deployment";
    pub const RESTARTED_FROM: &str = "$restarted_from";
    pub const ROOT: &str = "$root";
}

/// The immutable metadata row of an invocation: written in the same commit that creates
/// it, deleted in the same commit that cleans it up.
///
/// ```text
/// key:   0x00 ‖ owner_pk ‖ 0x1B ‖ id
/// value: v(u8)=1 ‖ created_ms(u64) ‖ kind(u8) ‖ flags(u8: bit0 = root) ‖ lp(tenant)
///        ‖ lp(scope) ‖ lp(service) ‖ lp(handler) ‖ lp(key) ‖ TLV*   — TLV: tag(u8) ‖ lp(bytes)
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InvocationMeta {
    /// `createdAt` — the single source of truth (from the engine's `clock`).
    pub created_ms: u64,
    /// The engine's kind code (`plain`/`workflow-run`/`workflow-shared`).
    pub kind: u8,
    /// No caller. A one-way `send` does not carry its sender yet, so it counts as a root.
    pub root: bool,
    pub tenant: String,
    /// Empty = no scope.
    pub scope: String,
    pub service: String,
    pub handler: String,
    /// Empty = no key.
    pub key: String,
    pub parent: Option<InvocationId>,
    pub schedule: Option<String>,
    pub idempotency_key: Option<String>,
    /// Deployment at creation time.
    pub deployment: Option<String>,
    /// User tags, in declaration order.
    pub tags: Vec<(String, String)>,
    pub root_id: Option<InvocationId>,
    pub restarted_from: Option<InvocationId>,
    /// Limit key at creation time (`QueueName.limit`). `None` also for invocations created
    /// before this field existed.
    pub limit_key: Option<String>,
    /// Unknown TLVs (from a newer binary), kept verbatim.
    pub unknown: Vec<(u8, Vec<u8>)>,
}

fn put_lp(b: &mut BytesMut, s: &[u8]) {
    // Every component has already been capped at the entry gate (≤ `MAX_COMPONENT`, tags
    // ≤ 64 B, idempotency key ≤ its own cap), so u16 never wraps.
    let n = s.len().min(u16::MAX as usize);
    b.put_u16(n as u16);
    b.put_slice(&s[..n]);
}

fn put_tlv(b: &mut BytesMut, tag: u8, v: &[u8]) {
    b.put_u8(tag);
    put_lp(b, v);
}

/// Sequential reader; every read returns `None` when bytes are missing (damaged data must
/// not panic the reading thread).
struct Rd<'a>(&'a [u8]);

impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (h, t) = self.0.split_at_checked(n)?;
        self.0 = t;
        Some(h)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }
    fn lp(&mut self) -> Option<&'a [u8]> {
        let n = u16::from_be_bytes(self.take(2)?.try_into().ok()?) as usize;
        self.take(n)
    }
    fn str(&mut self) -> Option<String> {
        String::from_utf8(self.lp()?.to_vec()).ok()
    }
    fn id(&mut self) -> Option<InvocationId> {
        self.take(ID_LEN)?.try_into().ok()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl InvocationMeta {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(96);
        b.put_u8(META_V1);
        b.put_u64(self.created_ms);
        b.put_u8(self.kind);
        b.put_u8(if self.root { FLAG_ROOT } else { 0 });
        for s in [
            &self.tenant,
            &self.scope,
            &self.service,
            &self.handler,
            &self.key,
        ] {
            put_lp(&mut b, s.as_bytes());
        }
        if let Some(p) = &self.parent {
            put_tlv(&mut b, tlv::PARENT, p);
        }
        if let Some(s) = &self.schedule {
            put_tlv(&mut b, tlv::SCHEDULE, s.as_bytes());
        }
        if let Some(s) = &self.idempotency_key {
            put_tlv(&mut b, tlv::IDEMPOTENCY, s.as_bytes());
        }
        if let Some(s) = &self.deployment {
            put_tlv(&mut b, tlv::DEPLOYMENT, s.as_bytes());
        }
        for (k, v) in &self.tags {
            let mut kv = BytesMut::with_capacity(4 + k.len() + v.len());
            put_lp(&mut kv, k.as_bytes());
            put_lp(&mut kv, v.as_bytes());
            put_tlv(&mut b, tlv::ATTR, &kv);
        }
        if let Some(r) = &self.root_id {
            put_tlv(&mut b, tlv::ROOT_ID, r);
        }
        if let Some(r) = &self.restarted_from {
            put_tlv(&mut b, tlv::RESTARTED_FROM, r);
        }
        if let Some(l) = &self.limit_key {
            put_tlv(&mut b, tlv::LIMIT_KEY, l.as_bytes());
        }
        for (tag, v) in &self.unknown {
            put_tlv(&mut b, *tag, v);
        }
        b.to_vec()
    }

    /// `None` = damaged or an unknown version. Unknown TLVs are kept in `unknown`.
    pub fn decode(raw: &[u8]) -> Option<Self> {
        let mut r = Rd(raw);
        if r.u8()? != META_V1 {
            return None;
        }
        let mut m = Self {
            created_ms: r.u64()?,
            kind: r.u8()?,
            root: r.u8()? & FLAG_ROOT != 0,
            tenant: r.str()?,
            scope: r.str()?,
            service: r.str()?,
            handler: r.str()?,
            key: r.str()?,
            ..Self::default()
        };
        while !r.is_empty() {
            let tag = r.u8()?;
            let v = r.lp()?;
            let id = || -> Option<InvocationId> { v.try_into().ok() };
            let s = || String::from_utf8(v.to_vec()).ok();
            match tag {
                tlv::PARENT => m.parent = Some(id()?),
                tlv::SCHEDULE => m.schedule = Some(s()?),
                tlv::IDEMPOTENCY => m.idempotency_key = Some(s()?),
                tlv::DEPLOYMENT => m.deployment = Some(s()?),
                tlv::ATTR => {
                    let mut kv = Rd(v);
                    m.tags.push((kv.str()?, kv.str()?));
                }
                tlv::ROOT_ID => m.root_id = Some(id()?),
                tlv::RESTARTED_FROM => m.restarted_from = Some(id()?),
                tlv::LIMIT_KEY => m.limit_key = Some(s()?),
                other => m.unknown.push((other, v.to_vec())),
            }
        }
        Some(m)
    }

    /// Every attribute of [`BY_ATTR`]: system attributes (`$…`), then user tags. Immutable
    /// for the life of the invocation.
    pub fn attrs(&self) -> Vec<(&'static str, String)> {
        let mut out: Vec<(&'static str, String)> = Vec::new();
        if let Some(p) = &self.parent {
            out.push((attr::PARENT, id_hex(p)));
        }
        if let Some(s) = &self.schedule {
            out.push((attr::SCHEDULE, s.clone()));
        }
        if let Some(d) = &self.deployment {
            out.push((attr::DEPLOYMENT, d.clone()));
        }
        if let Some(r) = &self.restarted_from {
            out.push((attr::RESTARTED_FROM, id_hex(r)));
        }
        if let Some(r) = &self.root_id {
            out.push((attr::ROOT, id_hex(r)));
        }
        out
    }

    /// [`Self::attrs`] plus user tags — `(name, value)`.
    pub fn all_attrs(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .attrs()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        out.extend(self.tags.iter().cloned());
        out
    }
}

/// Checks a user tag: key ≤ 32 B, `[a-z0-9_.-]`, not starting with `$`; value ≤ 64 B. The
/// error is a sentence for the caller (400).
pub fn check_tag(k: &str, v: &str) -> Result<(), String> {
    if k.is_empty() || k.len() > MAX_TAG_KEY {
        return Err(format!(
            "tag key is {} bytes long, must be 1–{MAX_TAG_KEY}",
            k.len()
        ));
    }
    if let Some(c) = k
        .chars()
        .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '_' | '.' | '-'))
    {
        return Err(format!("tag key {k:?} has {c:?} outside [a-z0-9_.-]"));
    }
    if v.len() > MAX_TAG_VALUE {
        return Err(format!(
            "tag value {k:?} is {} bytes long, the cap is {MAX_TAG_VALUE}",
            v.len()
        ));
    }
    Ok(())
}

/// Key of the metadata row of `id`.
pub fn meta_key(id: &InvocationId) -> Vec<u8> {
    keyspace::entity(
        crate::cluster::partition_key_of(id),
        Table::InvocationMeta,
        id,
    )
}

/// The id from a metadata key.
pub fn parse_meta_key(key: &[u8]) -> Option<InvocationId> {
    Rd(keyspace::entity_body(key, Table::InvocationMeta)?).id()
}

// ─────────────────────────────────────────────────────── declarative framework

/// The time axis of an index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Axis {
    /// The metadata's `created_ms`.
    Created,
    /// The `t` of [`Placement`] — when it entered its current class.
    Transition,
}

/// One part of a key. The order in [`QueryIndex::key`] is the byte order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    /// `lp(tenant)`.
    Tenant,
    /// `u8` class.
    Class,
    /// `u8`: 1 = root.
    Root,
    /// `lp(service)`.
    Service,
    /// `lp(name) ‖ lp(value)` — only with [`Applies::PerAttr`].
    Attr,
    /// `!t` = `u64::MAX − t`, big-endian: a forward scan gives newest first.
    Time(Axis),
    /// The 24-byte `id` — byte order = hex order.
    Id,
    /// `lp(object key)` (pha C2).
    ObjectKey,
    /// `lp(scope)` — empty when there is no scope.
    Scope,
}

/// Columns copied into the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    Class,
    Service,
    Handler,
    Key,
    Kind,
    Root,
    Created,
    T,
}

/// Which invocations a descriptor has rows for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applies {
    /// One row per invocation.
    Always,
    /// One row per attribute ([`InvocationMeta::all_attrs`]).
    PerAttr,
    /// One row per **object** (invocations with a key; [`OBJECT_KEYS`]): the key has no
    /// id, many invocations share one row. The row is a **hint**: written in the creating
    /// commit, **never deleted on the write path** (cleanup does not remove it — if the
    /// object still has state or other invocations it must still be visible). Readers check
    /// "does the object still have anything" before answering; `verify` does not treat extra
    /// rows as drift.
    PerObject,
}

/// A set of classes (bitmask by `1 << Class`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassSet(pub u8);

impl ClassSet {
    pub const ALL: ClassSet = ClassSet(0b1111);

    pub const fn contains(self, c: Class) -> bool {
        self.0 & (1 << c as u8) != 0
    }
}

/// One line of the query index table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryIndex {
    /// Index code in the derived region ([`QUERY_CODES`]); never reused.
    pub code: u8,
    /// Name in `EXPLAIN`, `verify`, metrics, the status marker.
    pub name: &'static str,
    /// Bumped ⇒ ranges rebuild just this index.
    pub version: u16,
    pub key: &'static [Part],
    pub classes: ClassSet,
    pub applies: Applies,
    pub covers: &'static [Field],
}

/// The code range reserved for query indexes in the derived region. Codes outside it
/// belong to [`keyspace::Index`]; `0xFC` is the status marker ([`QUERY_READY`]).
pub const QUERY_CODES: std::ops::RangeInclusive<u8> = 0x0D..=0x7F;

/// Code of the per-range index status marker (`0x01 ‖ 0xFC`).
pub const QUERY_READY: u8 = 0xFC;

pub const BY_CREATED: QueryIndex = QueryIndex {
    code: 0x0D,
    name: "ByCreated",
    version: 1,
    key: &[
        Part::Tenant,
        Part::Root,
        Part::Time(Axis::Created),
        Part::Id,
    ],
    classes: ClassSet::ALL,
    applies: Applies::Always,
    covers: &[
        Field::Class,
        Field::Service,
        Field::Handler,
        Field::Key,
        Field::Kind,
        Field::T,
    ],
};

pub const BY_SERVICE: QueryIndex = QueryIndex {
    code: 0x0E,
    name: "ByService",
    version: 1,
    key: &[
        Part::Tenant,
        Part::Service,
        Part::Time(Axis::Created),
        Part::Id,
    ],
    classes: ClassSet::ALL,
    applies: Applies::Always,
    covers: &[
        Field::Class,
        Field::Handler,
        Field::Key,
        Field::Kind,
        Field::Root,
        Field::T,
    ],
};

pub const BY_TRANSITION: QueryIndex = QueryIndex {
    code: 0x0F,
    name: "ByTransition",
    version: 1,
    key: &[
        Part::Tenant,
        Part::Class,
        Part::Root,
        Part::Time(Axis::Transition),
        Part::Id,
    ],
    classes: ClassSet::ALL,
    applies: Applies::Always,
    covers: &[
        Field::Service,
        Field::Handler,
        Field::Key,
        Field::Kind,
        Field::Created,
    ],
};

pub const BY_ATTR: QueryIndex = QueryIndex {
    code: 0x10,
    name: "ByAttr",
    version: 1,
    key: &[
        Part::Tenant,
        Part::Attr,
        Part::Time(Axis::Created),
        Part::Id,
    ],
    classes: ClassSet::ALL,
    applies: Applies::PerAttr,
    covers: &[Field::Service, Field::Handler, Field::Key],
};

/// The query index table — the source of truth for the write path, the rebuild and the
/// path chooser.
pub const QUERY_INDEXES: &[QueryIndex] =
    &[BY_CREATED, BY_SERVICE, BY_TRANSITION, BY_ATTR, OBJECT_KEYS];

/// Objects of a service (UI question 42, `sys_object`): `tenant ‖ service ‖ key ‖ scope`,
/// one row per object that has ever had at least one invocation created. Empty value: it
/// depends on no class or mark, so class transitions write nothing. The first real
/// descriptor added after the initial set — it went the "add an index" way (rebuild,
/// versioned marker).
pub const OBJECT_KEYS: QueryIndex = QueryIndex {
    code: 0x12,
    name: "ObjectKeys",
    version: 1,
    key: &[Part::Tenant, Part::Service, Part::ObjectKey, Part::Scope],
    classes: ClassSet::ALL,
    applies: Applies::PerObject,
    covers: &[],
};

/// Descriptor by name.
pub fn by_name(name: &str) -> Option<&'static QueryIndex> {
    QUERY_INDEXES.iter().find(|ix| ix.name == name)
}

// ─────────────────────────────────────────────────────── key, value

fn inv_time(t: u64) -> [u8; 8] {
    (u64::MAX - t).to_be_bytes()
}

/// An attribute being encoded (only with [`Applies::PerAttr`]).
type AttrRef<'a> = Option<(&'a str, &'a str)>;

impl QueryIndex {
    /// The common key prefix of every row of this descriptor: `0x01 ‖ code`.
    pub fn prefix(&self) -> Vec<u8> {
        keyspace::query_index(self.code, &[])
    }

    /// `[0x01 ‖ code, 0x01 ‖ code+1)`.
    pub fn bounds(&self) -> (Vec<u8>, Vec<u8>) {
        (
            keyspace::query_index(self.code, &[]),
            keyspace::query_index(self.code + 1, &[]),
        )
    }

    fn encode_key(
        &self,
        id: &InvocationId,
        meta: &InvocationMeta,
        at: Placement,
        attr: AttrRef<'_>,
    ) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(64);
        for part in self.key {
            match part {
                Part::Tenant => put_lp(&mut b, meta.tenant.as_bytes()),
                Part::Class => b.put_u8(at.class as u8),
                Part::Root => b.put_u8(meta.root as u8),
                Part::Service => put_lp(&mut b, meta.service.as_bytes()),
                Part::Attr => {
                    let (k, v) = attr.unwrap_or(("", ""));
                    put_lp(&mut b, k.as_bytes());
                    put_lp(&mut b, v.as_bytes());
                }
                Part::Time(Axis::Created) => b.put_slice(&inv_time(meta.created_ms)),
                Part::Time(Axis::Transition) => b.put_slice(&inv_time(at.t)),
                Part::Id => b.put_slice(id),
                Part::ObjectKey => put_lp(&mut b, meta.key.as_bytes()),
                Part::Scope => put_lp(&mut b, meta.scope.as_bytes()),
            }
        }
        keyspace::query_index(self.code, &b)
    }

    fn encode_value(&self, meta: &InvocationMeta, at: Placement) -> Vec<u8> {
        let mut b = BytesMut::with_capacity(48);
        for f in self.covers {
            match f {
                Field::Class => b.put_u8(at.class as u8),
                Field::Service => put_lp(&mut b, meta.service.as_bytes()),
                Field::Handler => put_lp(&mut b, meta.handler.as_bytes()),
                Field::Key => put_lp(&mut b, meta.key.as_bytes()),
                Field::Kind => b.put_u8(meta.kind),
                Field::Root => b.put_u8(meta.root as u8),
                Field::Created => b.put_u64(meta.created_ms),
                Field::T => b.put_u64(at.t),
            }
        }
        b.to_vec()
    }

    /// Whether the class is in the key (class change ⇒ delete + put) or only in the value
    /// (class change ⇒ overwriting put).
    pub fn class_in_key(&self) -> bool {
        self.key.contains(&Part::Class) || self.key.contains(&Part::Time(Axis::Transition))
    }

    /// The key's time axis (`None` if the key has no axis).
    pub fn axis(&self) -> Option<Axis> {
        self.key.iter().find_map(|p| match p {
            Part::Time(a) => Some(*a),
            _ => None,
        })
    }
}

/// Every `(key, value)` row of descriptor `ix` for invocation `id` at placement `at`.
/// Empty if the class is not in `ix.classes`.
pub fn rows(
    ix: &QueryIndex,
    id: &InvocationId,
    meta: &InvocationMeta,
    at: Placement,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    if !ix.classes.contains(at.class) {
        return Vec::new();
    }
    let value = ix.encode_value(meta, at);
    match ix.applies {
        Applies::Always => vec![(ix.encode_key(id, meta, at, None), value)],
        Applies::PerObject if meta.key.is_empty() => Vec::new(),
        Applies::PerObject => vec![(ix.encode_key(id, meta, at, None), value)],
        Applies::PerAttr => meta
            .all_attrs()
            .iter()
            .map(|(k, v)| (ix.encode_key(id, meta, at, Some((k, v))), value.clone()))
            .collect(),
    }
}

/// Index operations of a transition `old → new` (`None` = not yet present / cleaned up).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct EventOps {
    pub deletes: Vec<Vec<u8>>,
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The write path (user 1): for each descriptor in `indexes`, key changed ⇒ delete the old
/// + put the new; key kept, value changed ⇒ overwriting put; both kept ⇒ nothing.
pub fn event_ops(
    indexes: &[QueryIndex],
    id: &InvocationId,
    meta: &InvocationMeta,
    old: Option<Placement>,
    new: Option<Placement>,
) -> EventOps {
    let mut ops = EventOps::default();
    for ix in indexes {
        let before = old.map(|p| rows(ix, id, meta, p)).unwrap_or_default();
        let after = new.map(|p| rows(ix, id, meta, p)).unwrap_or_default();
        for (k, _) in &before {
            // Hint rows are never deleted on the write path (`Applies::PerObject`).
            if ix.applies != Applies::PerObject && !after.iter().any(|(k2, _)| k2 == k) {
                ops.deletes.push(k.clone());
            }
        }
        for (k, v) in after {
            if !before.iter().any(|(k2, v2)| *k2 == k && *v2 == v) {
                ops.puts.push((k, v));
            }
        }
    }
    ops
}

/// The group of a query counter.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CountKey {
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub class: Class,
    pub root: bool,
}

/// The counter change of a transition: `−1` in the old class's group, `+1` in the new
/// class's group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountDelta {
    pub tenant: String,
    pub service: String,
    pub handler: String,
    pub root: bool,
    pub old: Option<Class>,
    pub new: Option<Class>,
    /// The invocation's [`BY_CREATED`] key (independent of class): its place in the scan
    /// that builds the counters ([`QueryCounts`]).
    pub order: Vec<u8>,
}

impl CountDelta {
    pub fn of(
        id: &InvocationId,
        meta: &InvocationMeta,
        old: Option<Placement>,
        new: Option<Placement>,
    ) -> Self {
        Self {
            tenant: meta.tenant.clone(),
            service: meta.service.clone(),
            handler: meta.handler.clone(),
            root: meta.root,
            old: old.map(|p| p.class),
            new: new.map(|p| p.class),
            order: BY_CREATED.encode_key(
                id,
                meta,
                Placement {
                    class: Class::Active,
                    t: 0,
                },
                None,
            ),
        }
    }

    fn key(&self, class: Class) -> CountKey {
        CountKey {
            tenant: self.tenant.clone(),
            service: self.service.clone(),
            handler: self.handler.clone(),
            class,
            root: self.root,
        }
    }

    /// `(group, ±1)` — nothing when the class does not change.
    pub fn changes(&self) -> Vec<(CountKey, i64)> {
        if self.old == self.new {
            return Vec::new();
        }
        let mut out = Vec::new();
        if let Some(c) = self.old {
            out.push((self.key(c), -1));
        }
        if let Some(c) = self.new {
            out.push((self.key(c), 1));
        }
        out
    }
}

/// Cap on the number of counter groups per range; beyond it, groups are merged into the
/// tenant's `(others)` group.
pub const MAX_COUNT_GROUPS: usize = 100_000;

/// Service/handler name of the merged group when [`MAX_COUNT_GROUPS`] is exceeded.
pub const OTHERS: &str = "(others)";

/// Progress of building the counters.
#[derive(Clone, Debug, PartialEq, Eq)]
enum CountScan {
    /// Not started (the `ByCreated` index is not `readable` yet): changes are ignored.
    NotStarted,
    /// `ByCreated` has been scanned up to this key (inclusive): changes of invocations with
    /// a key `≤` the cursor are applied, the rest is left for the scan to see in its final
    /// state.
    At(Vec<u8>),
    Done,
}

/// **In-RAM query counters** of a range: `(tenant, service, handler, class, root) → number
/// of invocations`.
///
/// Counted per commit, exact within the range: every `query_event` in a batch is applied
/// **after** the batch is submitted (a writer hook, like the status hook) — it may run ahead
/// of durable data by at most one flush tick. Rebuilt by a chunked `ByCreated` scan (class
/// in the value), within the same rebuild loop; until it finishes, [`Self::snapshot`]
/// returns `None` (readers report `partial` + `indexing`).
///
/// The lock belongs to the range, not global; only the engine thread writes (the hook runs
/// while holding the engine), and query-layer readers read without the engine.
///
/// **Built on demand**: the `ByCreated` scan is O(invocations of the range) per term, each
/// chunk holding the engine. When the engine enables `with_counts_on_demand`, the rebuild
/// loop only scans once someone has read ([`Self::snapshot`] called before the build has
/// finished marks [`Self::want`]); the first reader of the term gets `None` (`partial` +
/// `indexing`) as at any time the counters are being built.
#[derive(Debug)]
pub struct QueryCounts {
    inner: std::sync::Mutex<CountsInner>,
    /// Someone has asked for the counters in this term.
    wanted: std::sync::atomic::AtomicBool,
    /// Wakes the rebuild loop waiting for someone to ask.
    asked: tokio::sync::Notify,
}

#[derive(Debug)]
struct CountsInner {
    groups: crate::entropy::HashMap<CountKey, i64>,
    scan: CountScan,
}

impl Default for QueryCounts {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(CountsInner {
                groups: Default::default(),
                scan: CountScan::NotStarted,
            }),
            wanted: std::sync::atomic::AtomicBool::new(false),
            asked: tokio::sync::Notify::new(),
        }
    }
}

impl CountsInner {
    fn add(&mut self, mut key: CountKey, d: i64) {
        if !self.groups.contains_key(&key) && self.groups.len() >= MAX_COUNT_GROUPS {
            key.service = OTHERS.into();
            key.handler = OTHERS.into();
        }
        let n = self.groups.entry(key.clone()).or_insert(0);
        *n += d;
        if *n == 0 {
            self.groups.remove(&key);
        }
    }
}

impl QueryCounts {
    fn lock(&self) -> std::sync::MutexGuard<'_, CountsInner> {
        // The hook does not panic midway through a change; a poisoned lock is still readable.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Applies the changes of a submitted batch.
    pub fn apply(&self, deltas: &[CountDelta]) {
        let mut g = self.lock();
        for d in deltas {
            let counted = match &g.scan {
                CountScan::NotStarted => false,
                CountScan::At(cursor) => d.order <= *cursor,
                CountScan::Done => true,
            };
            if counted {
                for (k, n) in d.changes() {
                    g.add(k, n);
                }
            }
        }
    }

    /// Starts (again) building: clears every group, cursor before every key.
    pub fn begin(&self) {
        let mut g = self.lock();
        g.groups.clear();
        g.scan = CountScan::At(Vec::new());
    }

    /// Whether building has started (and not finished).
    pub fn cursor(&self) -> Option<Option<Vec<u8>>> {
        match &self.lock().scan {
            CountScan::NotStarted => None,
            CountScan::At(c) => Some(Some(c.clone())),
            CountScan::Done => Some(None),
        }
    }

    /// Adds a chunk of decoded `ByCreated` rows (`rows` in key order), then moves the cursor
    /// to `last`.
    pub fn absorb(&self, rows: &[IndexRow], last: Vec<u8>) {
        let mut g = self.lock();
        for r in rows {
            if let (Some(tenant), Some(service), Some(handler), Some(class), Some(root)) = (
                r.tenant.clone(),
                r.service.clone(),
                r.handler.clone(),
                r.class,
                r.root,
            ) {
                g.add(
                    CountKey {
                        tenant,
                        service,
                        handler,
                        class,
                        root,
                    },
                    1,
                );
            }
        }
        g.scan = CountScan::At(last);
    }

    /// The scan is done.
    pub fn finish(&self) {
        self.lock().scan = CountScan::Done;
    }

    /// Drops everything (the range changed its derived region): the next build starts over.
    pub fn reset(&self) {
        let mut g = self.lock();
        g.groups.clear();
        g.scan = CountScan::NotStarted;
    }

    /// Someone needs the counters: the rebuild loop (waiting on [`Self::asked`]) scans.
    pub fn want(&self) {
        if !self.wanted.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.asked.notify_waiters();
        }
    }

    /// Whether someone has asked for the counters.
    pub fn wanted(&self) -> bool {
        self.wanted.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Waits until someone asks for the counters (returns at once if someone already has).
    pub async fn asked(&self) {
        loop {
            let notified = self.asked.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.wanted() {
                return;
            }
            notified.await;
        }
    }

    /// Every non-zero group, in key order; `None` when the build has not finished — and
    /// then it marks that someone asked ([`Self::want`]).
    pub fn snapshot(&self) -> Option<Vec<(CountKey, u64)>> {
        let g = self.lock();
        if g.scan != CountScan::Done {
            drop(g);
            self.want();
            return None;
        }
        let mut out: Vec<(CountKey, u64)> = g
            .groups
            .iter()
            .map(|(k, n)| (k.clone(), (*n).max(0) as u64))
            .collect();
        out.sort();
        Some(out)
    }
}

// ─────────────────────────────────────────────────────── reading back

/// A decoded index row: every field the descriptor's key or value carries. Fields the
/// descriptor does not carry are `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexRow {
    pub id: InvocationId,
    pub tenant: Option<String>,
    pub class: Option<Class>,
    pub root: Option<bool>,
    pub service: Option<String>,
    pub handler: Option<String>,
    pub key: Option<String>,
    pub kind: Option<u8>,
    pub created_ms: Option<u64>,
    pub t: Option<u64>,
    pub attr: Option<(String, String)>,
    /// Only descriptors with [`Part::Scope`].
    pub scope: Option<String>,
}

/// Decodes a row of descriptor `ix`. `None` = a key of another descriptor, or damaged (an
/// index row is a hint: readers skip it, `verify` reports it).
pub fn decode_row(ix: &QueryIndex, key: &[u8], value: &[u8]) -> Option<IndexRow> {
    let body = keyspace::query_index_body(key, ix.code)?;
    let mut r = Rd(body);
    let mut row = IndexRow::default();
    for part in ix.key {
        match part {
            Part::Tenant => row.tenant = Some(r.str()?),
            Part::Class => row.class = Some(Class::from_u8(r.u8()?)?),
            Part::Root => row.root = Some(r.u8()? != 0),
            Part::Service => row.service = Some(r.str()?),
            Part::Attr => row.attr = Some((r.str()?, r.str()?)),
            Part::Time(axis) => {
                let t = u64::MAX - r.u64()?;
                match axis {
                    Axis::Created => row.created_ms = Some(t),
                    Axis::Transition => row.t = Some(t),
                }
            }
            Part::Id => row.id = r.id()?,
            Part::ObjectKey => row.key = Some(r.str()?),
            Part::Scope => row.scope = Some(r.str()?),
        }
    }
    if !r.is_empty() {
        return None;
    }
    let mut v = Rd(value);
    for f in ix.covers {
        match f {
            Field::Class => row.class = Some(Class::from_u8(v.u8()?)?),
            Field::Service => row.service = Some(v.str()?),
            Field::Handler => row.handler = Some(v.str()?),
            Field::Key => row.key = Some(v.str()?),
            Field::Kind => row.kind = Some(v.u8()?),
            Field::Root => row.root = Some(v.u8()? != 0),
            Field::Created => row.created_ms = Some(v.u64()?),
            Field::T => row.t = Some(v.u64()?),
        }
    }
    v.is_empty().then_some(row)
}

/// The scan prefix of descriptor `ix` when the leading key parts are bound by `=`.
///
/// `fixed` lists the value of each leading part in [`QueryIndex::key`] order, stopping at
/// the first unbound part. The time and id parts are not bound here (the caller adds the
/// scan bounds).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fixed<'a> {
    Tenant(&'a str),
    Class(Class),
    Root(bool),
    Service(&'a str),
    Attr(&'a str, &'a str),
}

/// `0x01 ‖ code ‖ bound leading parts`. `None` if `fixed` does not match the key order.
pub fn scan_prefix(ix: &QueryIndex, fixed: &[Fixed<'_>]) -> Option<Vec<u8>> {
    let mut b = BytesMut::with_capacity(48);
    for (part, f) in ix.key.iter().zip(fixed) {
        match (part, f) {
            (Part::Tenant, Fixed::Tenant(t)) => put_lp(&mut b, t.as_bytes()),
            (Part::Class, Fixed::Class(c)) => b.put_u8(*c as u8),
            (Part::Root, Fixed::Root(r)) => b.put_u8(*r as u8),
            (Part::Service, Fixed::Service(s)) => put_lp(&mut b, s.as_bytes()),
            (Part::Attr, Fixed::Attr(k, v)) => {
                put_lp(&mut b, k.as_bytes());
                put_lp(&mut b, v.as_bytes());
            }
            _ => return None,
        }
    }
    (fixed.len() <= ix.key.len()).then(|| keyspace::query_index(ix.code, &b))
}

/// Time bounds of a scan within a prefix: rows whose `t` mark is in `[from, to]`
/// (milliseconds, both ends inclusive). Keys carry `!t` so larger marks come first: `lo`
/// from `to`, `hi` from `from`.
pub fn time_bounds(prefix: &[u8], from: Option<u64>, to: Option<u64>) -> (Vec<u8>, Vec<u8>) {
    let mut lo = prefix.to_vec();
    if let Some(to) = to {
        lo.extend_from_slice(&inv_time(to));
    }
    let hi = match from {
        Some(from) => {
            let mut hi = prefix.to_vec();
            hi.extend_from_slice(&inv_time(from));
            // After every id of the `from` mark.
            hi.extend_from_slice(&[0xFF; ID_LEN + 1]);
            hi
        }
        None => prefix_end(prefix),
    };
    (lo, hi)
}

/// The smallest key greater than every key with prefix `p`.
pub fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last < u8::MAX {
            e.push(last + 1);
            return e;
        }
    }
    // All 0xFF: the end of the derived region.
    keyspace::Region::Bulk.bounds().0
}

// ─────────────────────────────────────────────────────── status marker

/// Key of the per-range index status marker.
pub fn ready_key() -> Vec<u8> {
    keyspace::query_index_marker()
}

/// The marker `"query range={id} gen={gen} ByCreated=1 …"` for `indexes`.
pub fn ready_marker(range: u64, gen: u64, indexes: &[QueryIndex]) -> String {
    let mut s = format!("query range={range} gen={gen}");
    for ix in indexes {
        s.push_str(&format!(" {}={}", ix.name, ix.version));
    }
    s
}

/// A decoded status marker: range, generation, and the version of each descriptor whose
/// build has finished.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyMarker {
    pub range: u64,
    pub gen: u64,
    pub versions: Vec<(String, u16)>,
}

impl ReadyMarker {
    /// The finished version of descriptor `name`, if any.
    pub fn version_of(&self, name: &str) -> Option<u16> {
        self.versions
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| *v)
    }
}

/// The inverse of [`ready_marker`]. `None` = damaged (read as "no marker yet" — every
/// index `building`).
pub fn parse_ready_marker(raw: &[u8]) -> Option<ReadyMarker> {
    let s = std::str::from_utf8(raw).ok()?;
    let mut it = s.split(' ');
    if it.next()? != "query" {
        return None;
    }
    let range = it.next()?.strip_prefix("range=")?.parse().ok()?;
    let gen = it.next()?.strip_prefix("gen=")?.parse().ok()?;
    let mut versions = Vec::new();
    for kv in it {
        let (k, v) = kv.split_once('=')?;
        versions.push((k.to_string(), v.parse().ok()?));
    }
    Some(ReadyMarker {
        range,
        gen,
        versions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every descriptor starts with the tenant: whoever deletes a tenant's rows
    /// (`keyspace::row_owner`) reads the tenant at the head of the key.
    #[test]
    fn every_index_leads_with_the_tenant() {
        for ix in QUERY_INDEXES.iter().chain([&OBJECT_KEYS]) {
            assert_eq!(ix.key.first(), Some(&Part::Tenant), "{}", ix.name);
        }
    }

    fn meta() -> InvocationMeta {
        InvocationMeta {
            created_ms: 1_700_000_000_000,
            kind: 0,
            root: true,
            tenant: "acme".into(),
            scope: String::new(),
            service: "Greeter".into(),
            handler: "greet".into(),
            key: String::new(),
            parent: None,
            schedule: Some("nightly".into()),
            idempotency_key: None,
            deployment: Some("dp_1".into()),
            tags: vec![("env".into(), "prod".into())],
            root_id: None,
            restarted_from: None,
            limit_key: None,
            unknown: vec![],
        }
    }

    const ID: InvocationId = [7; ID_LEN];

    #[test]
    fn meta_round_trips() {
        let mut m = meta();
        m.parent = Some([9; ID_LEN]);
        m.restarted_from = Some([3; ID_LEN]);
        assert_eq!(InvocationMeta::decode(&m.encode()), Some(m));
    }

    /// The limit key round-trips through encoding; a row without it reads as `None` (an
    /// invocation created before the limit key was recorded).
    #[test]
    fn meta_round_trips_limit_key() {
        let mut m = meta();
        m.limit_key = Some("growth/alice".into());
        let got = InvocationMeta::decode(&m.encode()).expect("readable");
        assert_eq!(got.limit_key.as_deref(), Some("growth/alice"));
        assert!(got.unknown.is_empty(), "{got:?}");
        assert_eq!(
            InvocationMeta::decode(&meta().encode()).map(|m| m.limit_key),
            Some(None)
        );
    }

    #[test]
    fn meta_ignores_unknown_tlv() {
        let m = meta();
        let mut raw = m.encode();
        raw.extend_from_slice(&[0x42, 0, 3, b'a', b'b', b'c']);
        let got = InvocationMeta::decode(&raw).expect("readable");
        assert_eq!(got.unknown, vec![(0x42, b"abc".to_vec())]);
        // Copying back (restart) keeps unknown TLVs verbatim.
        assert_eq!(got.encode(), raw);
        let mut known = got;
        known.unknown.clear();
        assert_eq!(known, m);
    }

    #[test]
    fn meta_rejects_garbage() {
        assert_eq!(InvocationMeta::decode(&[]), None);
        assert_eq!(InvocationMeta::decode(&[2, 0, 0]), None);
        let mut raw = meta().encode();
        raw.truncate(raw.len() - 1);
        assert_eq!(InvocationMeta::decode(&raw), None);
    }

    #[test]
    fn rows_decode_back() {
        let m = meta();
        let at = Placement::new(Class::Failed, 1_700_000_005_000, &m);
        for ix in QUERY_INDEXES {
            for (k, v) in rows(ix, &ID, &m, at) {
                let row = decode_row(ix, &k, &v).unwrap_or_else(|| panic!("{}", ix.name));
                assert_eq!(row.id, ID);
                assert_eq!(row.tenant.as_deref(), Some("acme"));
                assert_eq!(row.service.as_deref(), Some("Greeter"));
                if ix.name != "ByAttr" {
                    assert_eq!(row.class, Some(Class::Failed), "{}", ix.name);
                    assert_eq!(row.t, Some(1_700_000_005_000));
                }
            }
        }
    }

    #[test]
    fn active_t_is_created() {
        let m = meta();
        assert_eq!(Placement::new(Class::Active, 99, &m).t, m.created_ms);
        assert_eq!(Placement::new(Class::Paused, 99, &m).t, 99);
    }

    #[test]
    fn class_change_overwrites_value_streams() {
        let m = meta();
        let old = Placement::new(Class::Active, 0, &m);
        let new = Placement::new(Class::Completed, m.created_ms + 10, &m);
        let ops = event_ops(&[BY_CREATED, BY_SERVICE], &ID, &m, Some(old), Some(new));
        assert!(ops.deletes.is_empty(), "class in value: put, no delete");
        assert_eq!(ops.puts.len(), 2);
        let ops = event_ops(&[BY_TRANSITION], &ID, &m, Some(old), Some(new));
        assert_eq!((ops.deletes.len(), ops.puts.len()), (1, 1));
        let ops = event_ops(&[BY_ATTR], &ID, &m, Some(old), Some(new));
        assert_eq!(ops, EventOps::default(), "attributes are immutable");
    }

    /// **The `ObjectKeys` hint row**: creation ⇒ one put; class transition ⇒ nothing;
    /// cleanup ⇒ **no** delete; no key ⇒ no row; decodes back to tenant/service/key/scope.
    #[test]
    fn object_keys_is_a_hint_never_deleted() {
        let mut m = meta();
        m.key = "c-1".into();
        m.scope = "eu".into();
        let a = Placement::new(Class::Active, 0, &m);
        let c = Placement::new(Class::Completed, m.created_ms + 10, &m);
        let created = event_ops(&[OBJECT_KEYS], &ID, &m, None, Some(a));
        assert_eq!((created.puts.len(), created.deletes.len()), (1, 0));
        assert_eq!(
            event_ops(&[OBJECT_KEYS], &ID, &m, Some(a), Some(c)),
            EventOps::default()
        );
        assert_eq!(
            event_ops(&[OBJECT_KEYS], &ID, &m, Some(c), None),
            EventOps::default(),
            "cleanup does not remove the hint row"
        );
        let (k, v) = &created.puts[0];
        let row = decode_row(&OBJECT_KEYS, k, v).expect("decodable");
        assert_eq!(
            (
                row.tenant.as_deref(),
                row.service.as_deref(),
                row.key.as_deref(),
                row.scope.as_deref()
            ),
            (
                Some(m.tenant.as_str()),
                Some(m.service.as_str()),
                Some("c-1"),
                Some("eu")
            )
        );
        let mut plain = meta();
        plain.key.clear();
        assert!(rows(&OBJECT_KEYS, &ID, &plain, a).is_empty());
    }

    #[test]
    fn same_placement_is_noop() {
        let m = meta();
        let p = Placement::new(Class::Active, 0, &m);
        assert_eq!(
            event_ops(QUERY_INDEXES, &ID, &m, Some(p), Some(p)),
            EventOps::default()
        );
    }

    #[test]
    fn newest_first_in_scan_order() {
        let mut a = meta();
        let mut b = meta();
        a.created_ms = 10;
        b.created_ms = 20;
        let ka = rows(&BY_CREATED, &ID, &a, Placement::new(Class::Active, 0, &a))[0]
            .0
            .clone();
        let kb = rows(&BY_CREATED, &ID, &b, Placement::new(Class::Active, 0, &b))[0]
            .0
            .clone();
        assert!(kb < ka, "newer comes first");
    }

    #[test]
    fn same_time_orders_by_id_hex() {
        let m = meta();
        let p = Placement::new(Class::Active, 0, &m);
        let mut ids: Vec<InvocationId> = (0u8..=255).step_by(17).map(|b| [b; ID_LEN]).collect();
        ids.reverse();
        let mut keyed: Vec<(Vec<u8>, String)> = ids
            .iter()
            .map(|id| (rows(&BY_CREATED, id, &m, p)[0].0.clone(), id_hex(id)))
            .collect();
        keyed.sort();
        let hexes: Vec<&String> = keyed.iter().map(|(_, h)| h).collect();
        let mut sorted = hexes.clone();
        sorted.sort();
        assert_eq!(hexes, sorted, "same mark: id byte order = hex order");
    }

    #[test]
    fn time_bounds_cover_inclusive_range() {
        let m = meta();
        let pre = scan_prefix(&BY_CREATED, &[Fixed::Tenant("acme"), Fixed::Root(true)]).unwrap();
        let (lo, hi) = time_bounds(&pre, Some(m.created_ms), Some(m.created_ms));
        let k = rows(&BY_CREATED, &ID, &m, Placement::new(Class::Active, 0, &m))[0]
            .0
            .clone();
        assert!(lo <= k && k < hi);
        let (lo, hi) = time_bounds(&pre, Some(m.created_ms + 1), None);
        assert!(!(lo <= k && k < hi));
    }

    #[test]
    fn marker_round_trips() {
        let s = ready_marker(3, 7, QUERY_INDEXES);
        assert_eq!(
            s,
            "query range=3 gen=7 ByCreated=1 ByService=1 ByTransition=1 ByAttr=1 ObjectKeys=1"
        );
        let m = parse_ready_marker(s.as_bytes()).unwrap();
        assert_eq!((m.range, m.gen, m.versions.len()), (3, 7, 5));
        assert_eq!(m.version_of("ByAttr"), Some(1));
    }

    #[test]
    fn codes_are_unique_and_in_range() {
        let mut seen = std::collections::BTreeSet::new();
        for ix in QUERY_INDEXES {
            assert!(QUERY_CODES.contains(&ix.code), "{}", ix.name);
            assert!(seen.insert(ix.code), "duplicate code: {}", ix.name);
        }
    }

    #[test]
    fn codes_do_not_collide_with_derived_indexes() {
        use crate::keyspace::Index;
        let mut taken: Vec<u8> = Index::REBUILT.iter().map(|ix| *ix as u8).collect();
        taken.extend([
            Index::Floors as u8,
            Index::SealBarrier as u8,
            Index::Ready as u8,
        ]);
        for code in taken {
            assert!(!QUERY_CODES.contains(&code), "{code:#x} inside QUERY_CODES");
            assert_ne!(code, QUERY_READY);
        }
    }

    #[test]
    fn tags_are_checked() {
        assert!(check_tag("env", "prod").is_ok());
        assert!(check_tag("$parent", "x").is_err());
        assert!(check_tag("Env", "x").is_err());
        assert!(check_tag(&"k".repeat(33), "x").is_err());
        assert!(check_tag("k", &"v".repeat(65)).is_err());
    }
}
