// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operations ([`Op`]) and the phases of the durable op ledger ([`Phase`]).

/// A range id (`PartitionId.0` of `cairn-store`).
pub type Range = u32;

/// One or two ranges — the sources or children of an operation. `Copy`, no allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ranges {
    len: u8,
    ids: [Range; 2],
}

impl Ranges {
    pub const fn one(a: Range) -> Self {
        Ranges {
            len: 1,
            ids: [a, a],
        }
    }

    pub const fn two(a: Range, b: Range) -> Self {
        Ranges {
            len: 2,
            ids: [a, b],
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = Range> + '_ {
        self.ids[..self.len as usize].iter().copied()
    }

    pub fn contains(&self, r: Range) -> bool {
        self.iter().any(|x| x == r)
    }
}

/// One split or merge — exactly the intent on the table (`RangeIntent` of
/// `cairn-store`, with the ranges it touches). Two ops are equal ⇔ same intent:
/// child ids are allocated from `next_range_id` in the intent CAS itself, so no
/// two ops coincide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// Split `source` into `left`, `right` (`right = left + 1`).
    Split {
        source: Range,
        left: Range,
        right: Range,
    },
    /// Merge `lo` and `hi` (adjacent) into `into`.
    Merge { lo: Range, hi: Range, into: Range },
}

impl Op {
    pub fn sources(&self) -> Ranges {
        match *self {
            Op::Split { source, .. } => Ranges::one(source),
            Op::Merge { lo, hi, .. } => Ranges::two(lo, hi),
        }
    }

    pub fn children(&self) -> Ranges {
        match *self {
            Op::Split { left, right, .. } => Ranges::two(left, right),
            Op::Merge { into, .. } => Ranges::one(into),
        }
    }

    /// The key of the op ledger `ops/<key>`: the first child. Child ids are
    /// allocated by the winning intent CAS itself and never reused (D22), so
    /// each op has its own key — `cluster.conf` needs no change.
    pub fn key(&self) -> Range {
        match *self {
            Op::Split { left, .. } => left,
            Op::Merge { into, .. } => into,
        }
    }

    /// The possible ledger keys of the op that created child `c`: `c` itself,
    /// or `c - 1` (the right child of a split). The shell reads both and keeps
    /// the one with `c` in `children()`.
    pub fn keys_of_child(c: Range) -> Ranges {
        match c.checked_sub(1) {
            Some(p) => Ranges::two(c, p),
            None => Ranges::one(c),
        }
    }
}

/// The phase of the op ledger `ops/<key>`. **Each phase is written before its
/// side effects**, so the phase read is an upper bound on what the op has done:
///
/// | Phase | Written | Side effects that may have happened |
/// |---|---|---|
/// | `Intent` | right after the intent CAS wins (the new child ids belong to the op from then on) | intent on the table, quiesce |
/// | `Closed` | before the close mark on the source | close mark (SM-17) |
/// | `Cloned` | before the first clone/union | child Db (possibly half-made), checkpoints on ancestors |
/// | `Prepared` | after every child has a Db and an owner record, before the commit CAS | commit CAS |
/// | `Committed` | after the commit CAS wins | the source retired |
/// | `Aborting` | before the CAS dropping the intent | the CAS dropping the intent |
/// | `Cleaned` | after the CAS dropping the intent wins, before removing the close mark and cleaning up children | removing the close mark, cleaning up children |
///
/// `Cleaned` is the **durable fact** "the intent was dropped by CAS — the
/// children never enter the table" (replacing SM-14's inference "never entered
/// the table"). `Prepared` and `Committed` are the opposite proof: the children
/// may have entered the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Phase {
    Intent,
    Closed,
    Cloned,
    Prepared,
    Committed,
    Aborting,
    Cleaned,
}

/// The result of the transition rule [`Phase::advance`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Advance {
    /// Write the new phase (CAS on the version just read, `put_resolving`).
    Write,
    /// The ledger is already at that phase (someone else, or our earlier
    /// attempt, wrote it): do not write, go on.
    Already,
    /// Not allowed: the ledger went the other way (rolling back when we want
    /// to prepare, or committed when we want to roll back). The caller treats
    /// it as a failed step.
    Refused,
}

impl Phase {
    /// The phases of the forward branch (`Intent → … → Committed`).
    fn forward(self) -> Option<u8> {
        match self {
            Phase::Intent => Some(0),
            Phase::Closed => Some(1),
            Phase::Cloned => Some(2),
            Phase::Prepared => Some(3),
            Phase::Committed => Some(4),
            Phase::Aborting | Phase::Cleaned => None,
        }
    }

    /// The ledger's transition rule: `from` (`None` = no ledger yet) to `to`.
    ///
    /// - the forward branch only goes up: `Intent → Closed → Cloned → Prepared
    ///   → Committed` (the roller-forward writes `Prepared → Committed`);
    /// - every phase before `Committed` may go to `Aborting`, then `Aborting →
    ///   Cleaned`;
    /// - `Committed` and `Cleaned` are outcomes: nothing overwrites them;
    /// - the ledger is only **created** at `Intent` (after the intent CAS wins)
    ///   — a roller-back meeting an op without a ledger (an old binary, from
    ///   before the ledger) does not create one: intent-only behaviour.
    pub fn advance(from: Option<Phase>, to: Phase) -> Advance {
        let Some(from) = from else {
            return if to == Phase::Intent {
                Advance::Write
            } else {
                Advance::Refused
            };
        };
        if from == to {
            return Advance::Already;
        }
        let ok = match (from.forward(), to.forward()) {
            (Some(a), Some(b)) => b > a,
            (Some(a), None) => to == Phase::Aborting && a < 4,
            (None, None) => from == Phase::Aborting && to == Phase::Cleaned,
            (None, Some(_)) => false,
        };
        if ok {
            Advance::Write
        } else {
            Advance::Refused
        }
    }

    /// An op in this phase **may** have committed (the children entered the table).
    pub fn may_have_committed(self) -> bool {
        matches!(self, Phase::Prepared | Phase::Committed)
    }
}
