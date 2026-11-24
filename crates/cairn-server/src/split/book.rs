// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Operations owned by the node: the `OpBook` book, the `OpTicket` ticket.

use super::*;

// ─────────────────────────────────────── operations owned by the node

/// A table-changing operation the node has accepted.
#[derive(Clone, Copy, Debug)]
pub enum OpKind {
    Split {
        range: PartitionId,
        at: Option<u64>,
    },
    Merge {
        left: PartitionId,
        right: PartitionId,
    },
}

/// State of an operation book entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OpState {
    /// Running in a task of the node.
    Running,
    /// The commit CAS won.
    Committed,
    /// Failed; the intent (if written) has been rolled back, or is being rolled back in the background.
    Failed,
}

/// One entry of `GET /ranges/ops`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpView {
    pub id: String,
    /// `split` hay `merge`.
    pub kind: &'static str,
    pub ranges: Vec<u32>,
    /// Split point the caller asked for (hex 16), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    pub state: OpState,
    /// Running time so far (running) or in total (done).
    pub elapsed_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<OpErrorView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<OpReport>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OpErrorView {
    /// Like [`RangeOpError::label`].
    pub kind: &'static str,
    pub message: String,
}

struct OpRecord {
    view: OpView,
    started: Instant,
}

/// The node's **operation book**: every split/merge that [`RangeOps::start`]
/// accepted, with its state — so a client that disconnected can ask again
/// (`GET /ranges/ops/{id}`). In RAM only, this node's only: **the source of truth
/// is the intent on the table**. If the node dies the book dies with it, and a
/// half-done intent is rolled back by whoever takes the source range (SM-3) or by
/// the reconciliation round (SM-15). Keeps the last [`OpBook::KEEP_DONE`] finished entries.
#[derive(Clone, Default)]
pub struct OpBook(Arc<Mutex<BookInner>>);

#[derive(Default)]
struct BookInner {
    next: u64,
    ops: VecDeque<OpRecord>,
}

impl OpBook {
    pub const KEEP_DONE: usize = 64;

    pub(super) fn begin(&self, node: &str, kind: OpKind) -> String {
        let Ok(mut b) = self.0.lock() else {
            return format!("{node}-?");
        };
        b.next += 1;
        let id = format!("{node}-{}", b.next);
        let (name, ranges, at) = match kind {
            OpKind::Split { range, at } => {
                ("split", vec![range.0], at.map(|a| format!("{a:016x}")))
            }
            OpKind::Merge { left, right } => ("merge", vec![left.0, right.0], None),
        };
        b.ops.push_back(OpRecord {
            view: OpView {
                id: id.clone(),
                kind: name,
                ranges,
                at,
                state: OpState::Running,
                elapsed_ms: 0.0,
                error: None,
                report: None,
            },
            started: Instant::now(),
        });
        let done = b
            .ops
            .iter()
            .filter(|o| o.view.state != OpState::Running)
            .count();
        if done > Self::KEEP_DONE {
            if let Some(i) = b.ops.iter().position(|o| o.view.state != OpState::Running) {
                b.ops.remove(i);
            }
        }
        id
    }

    pub(super) fn finish(&self, id: &str, r: &OpResult<(Arc<PartitionTable>, OpReport)>) {
        let Ok(mut b) = self.0.lock() else { return };
        let Some(o) = b.ops.iter_mut().find(|o| o.view.id == id) else {
            return;
        };
        o.view.elapsed_ms = range_stats::ms(o.started.elapsed());
        match r {
            Ok((_, report)) => {
                o.view.state = OpState::Committed;
                o.view.report = Some(report.clone());
            }
            Err(e) => {
                o.view.state = OpState::Failed;
                o.view.error = Some(OpErrorView {
                    kind: e.label(),
                    message: e.to_string(),
                });
            }
        }
    }

    fn view(o: &OpRecord) -> OpView {
        let mut v = o.view.clone();
        if v.state == OpState::Running {
            v.elapsed_ms = range_stats::ms(o.started.elapsed());
        }
        v
    }

    /// Every entry, oldest first.
    pub fn list(&self) -> Vec<OpView> {
        self.0
            .lock()
            .map(|b| b.ops.iter().map(Self::view).collect())
            .unwrap_or_default()
    }

    pub fn get(&self, id: &str) -> Option<OpView> {
        self.0
            .lock()
            .ok()?
            .ops
            .iter()
            .find(|o| o.view.id == id)
            .map(Self::view)
    }
}

/// Ticket of an accepted operation: its id in the book, and its result once done.
pub struct OpTicket {
    pub id: String,
    pub(super) done: tokio::sync::oneshot::Receiver<OpResult<(Arc<PartitionTable>, OpReport)>>,
}

impl OpTicket {
    /// Wait for the operation to finish. Dropping the ticket midway (client
    /// disconnect) does **not** cancel the operation: it runs in a task of the node.
    pub async fn wait(self) -> OpResult<(Arc<PartitionTable>, OpReport)> {
        let id = self.id;
        self.done.await.unwrap_or_else(|_| {
            Err(RangeOpError::Failed(format!(
                "operation {id} stopped without returning a result — see GET /ranges/ops/{id}"
            )))
        })
    }
}

/// `tokio::spawn` that carries the failpoint's current node (task-local) over to
/// the new task — node-conditioned points still fire on the right node.
pub(super) fn spawn_in_node<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    match cairn_store::failpoint::current_node() {
        Some(n) => tokio::spawn(async move { cairn_store::failpoint::in_node(&n, fut).await }),
        None => tokio::spawn(fut),
    }
}
