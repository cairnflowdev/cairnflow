// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Measurements of one split/merge: `OpReport`, `Clock`.

use super::*;

/// Measurements of one split/merge — returned with the admin response.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpReport {
    /// The coordinator's steps, in order, with how long each took.
    pub phases: Vec<PhaseTiming>,
    /// **Write stop window**: from asking for quiesce until every new range has its leader
    /// on this node and has recovered its queues (FL-31, `wait_led_here`). Commands to the old ranges within the window get `WrongRange`/wait for the table.
    pub window_ms: f64,
    /// Old ranges: drain and seal measured in their engine threads.
    pub parents: Vec<RangeReport>,
    /// New ranges: take the lease, open the Db, rebuild the derived region, recover the queues.
    pub children: Vec<RangeReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PhaseTiming {
    pub phase: &'static str,
    pub ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RangeReport {
    pub range: u32,
    #[serde(flatten)]
    pub timings: RangeTimings,
}

impl RangeReport {
    fn of(stats: &RangeStats, p: PartitionId) -> Self {
        Self {
            range: p.0,
            timings: stats.get(p.0),
        }
    }
}

/// Lap stopwatch: each `lap` records the time since the previous lap.
///
/// Each lap is also a **failpoint** `<op>::<phase>`, placed at the boundary
/// *after* the phase just finished — `split::quiesce` is the gap between quiesce
/// and clone. Before commit (`lap`), `return(msg)` is a real failed step: it takes
/// the roll-back path. After commit (`lap_committed`) it can only pause/sleep — there
/// is no error after commit to inject.
pub(super) struct Clock {
    /// `split` or `merge` — first half of the failpoint name.
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    op: &'static str,
    last: Instant,
    phases: Vec<PhaseTiming>,
    window_from: Option<Instant>,
    window_ms: f64,
}

impl Clock {
    pub(super) fn start(op: &'static str) -> Self {
        Self {
            op,
            last: Instant::now(),
            phases: Vec::new(),
            window_from: None,
            window_ms: 0.0,
        }
    }

    pub(super) fn lap(&mut self, phase: &'static str) -> OpResult<()> {
        self.record(phase);
        cairn_store::failpoint!(&format!("{}::{phase}", self.op), |arg: Option<String>| {
            Err(RangeOpError::Failed(
                cairn_store::failpoint::error(&format!("{}::{phase}", self.op), arg).to_string(),
            ))
        });
        Ok(())
    }

    /// Post-commit lap: the failpoint can only pause/sleep (see [`Clock`]).
    pub(super) fn lap_committed(&mut self, phase: &'static str) {
        self.record(phase);
        cairn_store::failpoint!(&format!("{}::{phase}", self.op));
    }

    fn record(&mut self, phase: &'static str) {
        let now = Instant::now();
        self.phases.push(PhaseTiming {
            phase,
            ms: range_stats::ms(now - self.last),
        });
        self.last = now;
    }

    /// Open the write stop window at the start of the current lap.
    pub(super) fn window_opens(&mut self) {
        self.window_from = Some(self.last);
    }

    /// Close the window at the end of the lap just timed.
    pub(super) fn window_closes(&mut self) {
        if let Some(from) = self.window_from {
            self.window_ms = range_stats::ms(self.last - from);
        }
    }

    pub(super) fn report(
        self,
        stats: &RangeStats,
        parents: &[PartitionId],
        children: &[PartitionId],
    ) -> OpReport {
        OpReport {
            phases: self.phases,
            window_ms: self.window_ms,
            parents: parents.iter().map(|p| RangeReport::of(stats, *p)).collect(),
            children: children
                .iter()
                .map(|p| RangeReport::of(stats, *p))
                .collect(),
        }
    }
}

/// Forget the old drain/seal of `p` before a quiesce — so the report does not read
/// the numbers of an earlier quiesce (one that rolled back) by mistake.
pub(super) fn reset_quiesce_stats(stats: &RangeStats, p: PartitionId) {
    stats.update(p.0, |t| {
        t.drain_ms = None;
        t.seal_ms = None;
    });
}
