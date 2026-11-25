// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Sending messages from one partition to another.
//!
//! Reuses the HTTP server the node already runs — no second channel to build, to
//! configure, and to break on its own. Addresses come from [`crate::RoutingTable`], that
//! is from the lease objects in the bucket.
//!
//! ## Why sending to **ourselves** also goes over HTTP
//!
//! A node usually leads many partitions, so a message's target is very often right here
//! in this process. It still goes over HTTP, and that is a deliberate choice: an
//! in-process shortcut would be a **second code path** for the same job, exercised by
//! different tests, and broken in different ways.
//!
//! That is only safe thanks to a property of the receiving side: it **writes the message
//! down and acks at once**, without processing it synchronously. If it processed inline, a
//! call cycle A→B→A would deadlock two engine threads, and sending to ourselves would
//! deadlock one thread with itself. See `cairn_store::Table::Inbound`.

use {
    crate::{lease::Released, routing::RoutingTable},
    bytes::Bytes,
    cairn_engine::InterPartition,
    cairn_store::{
        ClusterConfig, PartitionId, PartitionTable, RangeEntry, RangeIntent, SendTicket,
    },
    serde::{Deserialize, Serialize},
    std::sync::{Arc, Mutex},
};

/// The range table on the wire — the body of `409 wrong_range` (D23), and the table part of
/// `/cluster`. Key ranges are written as 16-digit hex like `cluster.conf`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TableWire {
    pub version: u64,
    pub next_range_id: u32,
    pub ranges: Vec<RangeWire>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RangeWire {
    pub range: u32,
    pub start: String,
    pub end: String,
    pub gen: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<IntentWire>,
}

/// Unfinished work on a range (split/merge), as the table records it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum IntentWire {
    Splitting { at: String, left: u32, right: u32 },
    Merging { with: u32, into: u32 },
}

impl IntentWire {
    pub fn of(i: &RangeIntent) -> Self {
        match *i {
            RangeIntent::Splitting { at, left, right } => Self::Splitting {
                at: format!("{at:016x}"),
                left: left.0,
                right: right.0,
            },
            RangeIntent::Merging { with, into } => Self::Merging {
                with: with.0,
                into: into.0,
            },
        }
    }

    fn to_intent(&self) -> Option<RangeIntent> {
        Some(match self {
            Self::Splitting { at, left, right } => RangeIntent::Splitting {
                at: hex64(at)?,
                left: PartitionId(*left),
                right: PartitionId(*right),
            },
            Self::Merging { with, into } => RangeIntent::Merging {
                with: PartitionId(*with),
                into: PartitionId(*into),
            },
        })
    }
}

fn hex64(s: &str) -> Option<u64> {
    (s.len() == 16)
        .then(|| u64::from_str_radix(s, 16).ok())
        .flatten()
}

impl From<&PartitionTable> for TableWire {
    fn from(t: &PartitionTable) -> Self {
        Self {
            version: t.version(),
            next_range_id: t.next_range_id(),
            ranges: t
                .ranges()
                .iter()
                .map(|r| RangeWire {
                    range: r.id.0,
                    start: format!("{:016x}", r.start),
                    end: format!("{:016x}", r.end),
                    gen: r.gen,
                    intent: r.intent.as_ref().map(IntentWire::of),
                })
                .collect(),
        }
    }
}

impl TableWire {
    /// Rebuild the table — **checking every invariant** like a table read from the bucket.
    /// `None` if the table is corrupt: a table received over the wire must not be applied unchecked.
    pub fn to_table(&self) -> Option<PartitionTable> {
        let ranges = self
            .ranges
            .iter()
            .map(|r| {
                Some(RangeEntry {
                    start: hex64(&r.start)?,
                    end: hex64(&r.end)?,
                    id: PartitionId(r.range),
                    gen: r.gen,
                    intent: match &r.intent {
                        None => None,
                        Some(i) => Some(i.to_intent()?),
                    },
                })
            })
            .collect::<Option<Vec<_>>>()?;
        PartitionTable::from_entries(self.version, self.next_range_id, ranges).ok()
    }
}

/// The body of `409 wrong_range` — enough for the sender to learn the new table without
/// asking anyone else.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WrongRangeBody {
    pub error: String,
    pub code: String,
    pub table: TableWire,
}

/// The code of the `409` body when the receiver says "the message does not belong to this
/// range by our table".
pub const WRONG_RANGE: &str = "wrong_range";

/// Limit on one inter-partition send, including the receiver's durable write.
///
/// Past the limit the outbox row stays where it is and the next drain retries — nothing is
/// lost, only delayed. Without a limit a hung target partition (or a wait cycle) would
/// keep the sender standing forever.
pub const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Limit on one **probe of the owner that just released** ([`RoutingTable::releaser_of`]).
/// It can only refuse at once (or ack once it has taken the range back); hanging means it
/// cannot answer — do not hold the target's queue up to [`SEND_TIMEOUT`] while a new
/// leader may already exist.
pub const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Who leads the target partition is not known yet.
    ///
    /// **Retryable, and must be retried.** The routing table refreshes periodically, and a
    /// partition changing leader temporarily has nobody. The outbox row is still intact so
    /// nothing is lost — only delayed.
    #[error("leader of partition {0} not known yet")]
    NoLeader(u32),
    /// The leader is known but advertises no address.
    #[error("leader of partition {0} advertises no HTTP address")]
    NoAddress(u32),
    #[error("send to {addr} failed: {source}")]
    Http {
        addr: String,
        source: reqwest::Error,
    },
    /// The receiver returned an error status. The body is kept for logging.
    #[error("{addr} returned {status}: {body}")]
    Rejected {
        addr: String,
        status: u16,
        body: String,
    },
    /// The receiver says the message does not belong to its range by its table **version
    /// `theirs`**, and that table is not newer than ours — the range is retiring, splitting,
    /// or both sides have the same version and still disagree. Resending right away would be
    /// wrong in exactly the same way; the next drain retries, after our table has had a
    /// chance to reload.
    #[error("{addr} refused wrong_range by table version {theirs}: {message}")]
    WrongRange {
        addr: String,
        theirs: u64,
        message: String,
    },
}

impl TransportError {
    /// Whether to retry.
    ///
    /// Almost always `true`, and rightly so: an outbox row is only deleted after an ack, so
    /// "not sent yet" always means "send again later". Except for `4xx` — the receiver is
    /// saying this message is wrong, and sending exactly the same again would be wrong in
    /// exactly the same way.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Rejected { status, .. } => !(400..500).contains(status),
            // A `409` that is not the message's fault: once the table changes it gets through.
            Self::WrongRange { .. } => true,
            _ => true,
        }
    }

    /// The send exhausted its own limit (`SEND_TIMEOUT`/`PROBE_TIMEOUT`) — a hung target,
    /// not a refusing one (only this error costs the drainer its whole limit, so only it
    /// opens the circuit breaker).
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Http { source, .. } if source.is_timeout())
    }
}

/// The `outcome` label of `cairnflow_interrange_messages_total` for a message whose carrying
/// request failed with `e`.
fn outcome_of(e: &TransportError) -> crate::metrics::interrange::Outcome {
    use crate::metrics::interrange::Outcome;
    match e {
        TransportError::Rejected { .. } => Outcome::Rejected,
        TransportError::WrongRange { .. } => Outcome::WrongRange,
        TransportError::NoLeader(_)
        | TransportError::NoAddress(_)
        | TransportError::Http { .. } => Outcome::Unreachable,
    }
}

/// What a refusal teaches us — see [`Transport::learn`].
enum Learn {
    /// A newer table was applied: re-route the unsent part.
    Again,
    /// The error of this send.
    Fail(TransportError),
}

/// Result of sending one batch with [`Transport::post_chunk`]: the first `acked` messages are
/// durable at the receiver; `error` = (number of messages in the failed request, the error).
struct Posted {
    acked: usize,
    error: Option<(usize, TransportError)>,
}

/// Split `idxs` into groups by destination `dest_of`, groups ordered by first appearance,
/// **keeping the order** of `idxs` within each group (a stable partition).
///
/// This is what keeps IR-3 when a batch must be re-split by a new table: every message for
/// the same target key lands in the same range under any table, hence in the same group,
/// and within the group they go in their original order.
fn group_by_destination(
    idxs: &[usize],
    dest_of: impl Fn(usize) -> PartitionId,
) -> Vec<(PartitionId, Vec<usize>)> {
    let mut groups: Vec<(PartitionId, Vec<usize>)> = Vec::new();
    for &i in idxs {
        let d = dest_of(i);
        match groups.iter_mut().find(|(g, _)| *g == d) {
            Some((_, v)) => v.push(i),
            None => groups.push((d, vec![i])),
        }
    }
    groups
}

/// The number of leading messages (encoded sizes `sizes`, in order) that fit in one batch: at
/// most [`MAX_BATCH_MESSAGES`] messages and [`MAX_BATCH_BYTES`] body bytes. Always ≥ 1 while
/// messages remain — a message larger than the limit goes alone.
pub(crate) fn chunk_len(sizes: impl Iterator<Item = usize>) -> usize {
    let mut bytes = InterPartition::BATCH_HEADER;
    let mut n = 0;
    for size in sizes {
        let next = bytes + InterPartition::BATCH_FRAME + size;
        if n == MAX_BATCH_MESSAGES || (n > 0 && next > MAX_BATCH_BYTES) {
            break;
        }
        bytes = next;
        n += 1;
    }
    n
}

/// Limit on messages per batch: twice the `BATCH` of 32 of one outbox read round. The
/// receiver refuses longer batches (`400`).
pub const MAX_BATCH_MESSAGES: usize = 64;

/// Limit on body bytes per batch: keeps one receiver commit below the size of an ordinary
/// WAL object. A single message larger than the limit goes alone (the per-message route).
pub const MAX_BATCH_BYTES: usize = 1 << 20;

/// The outcome of one message in [`Transport::send_batch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// The receiver has durably written this message — in the same commit as every other
    /// message of its carrying request.
    Acked,
    /// The request carrying this message failed; the error is in [`Sent::errors`]. The
    /// receiver wrote no message of that request (all or nothing, IR-2).
    Failed,
    /// Not sent: an earlier message to the **same destination** failed (IR-3).
    NotSent,
}

/// Result of [`Transport::send_batch`]: one [`Delivery`] per message, in input order; one
/// error per failure, with the index of the first message of the failed request.
#[derive(Debug)]
pub struct Sent {
    pub delivery: Vec<Delivery>,
    pub errors: Vec<(usize, TransportError)>,
}

/// Addresses of nodes that answered `404`/`405` on the batch route — old binaries. RAM
/// only, and only valid for **one** generation of the routing table: when the table changes
/// (a leader change, a node restarted with a new binary) everything is forgotten and probed again.
#[derive(Default)]
struct NoBatch {
    generation: u64,
    addrs: std::collections::BTreeSet<String>,
}

/// Number of times a new table may be learned within **one** send. Each learning requires a
/// real version increase, so the loop stops anyway; the limit only keeps a burst of table
/// changes from holding one drain too long.
const MAX_TABLE_LEARNS: u32 = 4;

/// The inter-partition send path.
#[derive(Clone)]
pub struct Transport {
    client: reqwest::Client,
    routing: RoutingTable,
    /// The cluster's key range table: a message's target partition is read from the partition
    /// key in its id, looked up through this table.
    cluster: ClusterConfig,
    /// The cluster secret to sign each send — the receiver's internal port refuses unsigned
    /// requests. `None` = unsigned (tests that build the receiver themselves).
    secret: Option<crate::auth::ClusterSecret>,
    /// Nodes without the batch route ([`NoBatch`]). The lock is only held within one lookup
    /// or one write, never across an `await`.
    no_batch: Arc<Mutex<NoBatch>>,
}

impl Transport {
    pub fn new(client: reqwest::Client, routing: RoutingTable, cluster: ClusterConfig) -> Self {
        Self {
            client,
            routing,
            cluster,
            secret: None,
            no_batch: Arc::default(),
        }
    }

    /// Sign every send with the cluster secret.
    pub fn with_cluster_secret(mut self, secret: crate::auth::ClusterSecret) -> Self {
        self.secret = Some(secret);
        self
    }

    /// A transport that goes nowhere — for a one-partition cluster, where no
    /// inter-partition message exists.
    pub fn disconnected() -> Self {
        Self::new(
            reqwest::Client::new(),
            RoutingTable::new(),
            ClusterConfig::single(),
        )
    }

    /// Send **one** message and **wait for the receiver to confirm the durable write**.
    ///
    /// `Ok(())` is the receiver's promise that the message is in its state (INV-5 applied a
    /// second time, at the boundary between two partitions). Only then may the caller delete
    /// its outbox row.
    ///
    /// **`_durable` is never read, and it is still the most important parameter.** An
    /// inter-partition message is an external effect: the other partition receives it and
    /// acts, and if the outbox row that produced it is not durable yet, a crash here leaves a
    /// system where the other side has done the work and this side does not remember asking
    /// anyone to. Since `commit` stopped waiting for durability itself, what prevents that is
    /// no longer the order — it is this ticket. The ticket is a **send ticket**
    /// ([`SendTicket`]): the message only carries the bytes of the send rows (plus the
    /// routing table), so it is enough that every send row read is durable (LF-1); replying
    /// to a client and sending down to the SDK still require `Committed` (LF-2).
    ///
    /// **The range table changed midway** (D23): the receiver answers `409 wrong_range`
    /// with its table. If that table is newer than ours ⇒ apply it, recompute the target and
    /// resend **within this very send** — without waiting for the table reload round or the
    /// next drain. Not newer ⇒ return [`TransportError::WrongRange`]: resending by the same
    /// version would repeat an already known answer forever.
    pub async fn send(
        &self,
        msg: &InterPartition,
        durable: &SendTicket,
        released: &Released,
    ) -> Result<(), TransportError> {
        self.send_within(msg, durable, released, SEND_TIMEOUT).await
    }

    /// [`Self::send`] with a limit `timeout` per request (the probe batch of the
    /// per-destination circuit breaker: [`PROBE_TIMEOUT`]).
    pub async fn send_within(
        &self,
        msg: &InterPartition,
        durable: &SendTicket,
        released: &Released,
        timeout: std::time::Duration,
    ) -> Result<(), TransportError> {
        let mut sent = self
            .deliver(std::slice::from_ref(msg), durable, released, timeout)
            .await;
        match sent.errors.pop() {
            None => Ok(()),
            Some((_, e)) => Err(e),
        }
    }

    /// Send the messages of **one per-destination queue**, in order, in batches, and wait
    /// for the receiver to write them durably.
    ///
    /// The contract — each point names its test:
    ///
    /// - **Routed at send time, not at gather time.** Each message's destination is computed
    ///   with the current range table; messages are split into groups by destination, each
    ///   group **keeping** its input order ([`group_by_destination`]). The groups are sent one
    ///   after another; one failing group does not block another (IR-4).
    /// - **Batches by limit**: each group is cut into consecutive batches, at most
    ///   [`MAX_BATCH_MESSAGES`] messages and [`MAX_BATCH_BYTES`] bytes ([`chunk_len`]),
    ///   sent **one after another** — the next batch only goes once the previous one is
    ///   acked. A one-message batch (or a message larger than the limit) takes the
    ///   per-message route.
    /// - **All or nothing per request** (IR-2): a batch's `200` means every message of it is
    ///   durable at the receiver in one commit; any error means none of its messages was
    ///   written
    ///   (`failpoints.rs::receiver_failing_mid_batch_writes_no_message_of_it`).
    ///   A failed request ⇒ its messages are [`Delivery::Failed`], every later message of the
    ///   **same group** [`Delivery::NotSent`] (IR-3,
    ///   `failpoints.rs::batch_rejected_{503,409}_keeps_every_row`).
    /// - **`409 wrong_range` with a newer table** (IR-6): apply the table, then every
    ///   unacked message — of the refused request and of every later request — is **re-routed
    ///   and re-grouped** by the new table, and sending continues within the same send.
    ///   Acked messages came before them in the old group, so within each new destination the
    ///   order is still the input order
    ///   (`failpoints.rs::batch_split_between_gather_and_send_is_regrouped_by_the_new_table`).
    /// - **A destination running an old binary**: `404`/`405` on the batch route ⇒ remember the
    ///   address ([`NoBatch`]) and resend that very batch one message at a time, within the send
    ///   (`transport_engine.rs::batches_to_an_old_binary_fall_back_to_single_messages`).
    ///
    /// `_durable` as in [`Self::send`]: every message must be durable at the sender before
    /// the other side sees it (IR-1).
    pub async fn send_batch(
        &self,
        msgs: &[InterPartition],
        durable: &SendTicket,
        released: &Released,
    ) -> Sent {
        self.send_batch_within(msgs, durable, released, SEND_TIMEOUT)
            .await
    }

    /// [`Self::send_batch`] with a limit `timeout` per request — the probe batch of the
    /// per-destination circuit breaker uses [`PROBE_TIMEOUT`].
    pub async fn send_batch_within(
        &self,
        msgs: &[InterPartition],
        durable: &SendTicket,
        released: &Released,
        timeout: std::time::Duration,
    ) -> Sent {
        // The batch is gathered (destinations pinned by the table at gather time), not routed
        // yet — the gap a split can slip into.
        cairn_store::failpoint!("transport::send_batch");
        self.deliver(msgs, durable, released, timeout).await
    }

    async fn deliver(
        &self,
        msgs: &[InterPartition],
        durable: &SendTicket,
        released: &Released,
        timeout: std::time::Duration,
    ) -> Sent {
        let encoded: Vec<Bytes> = msgs.iter().map(InterPartition::encode).collect();
        let mut delivery: Vec<Option<Delivery>> = vec![None; msgs.len()];
        let mut errors = Vec::new();
        let mut learned = 0;
        let mut todo: Vec<usize> = (0..msgs.len()).collect();
        while !todo.is_empty() {
            // One table snapshot for both the destination and the version declared to the receiver.
            let table = self.cluster.table();
            let groups = group_by_destination(&todo, |i| {
                table.partition_of_key(cairn_store::partition_key_of(msgs[i].destination_id()))
            });
            let mut again = Vec::new();
            let mut regroup = false;
            for (dest, idxs) in groups {
                if regroup {
                    // The table just changed: this group was routed by the old table.
                    again.extend(idxs);
                    continue;
                }
                let mut rest = idxs.as_slice();
                while !rest.is_empty() {
                    let take = chunk_len(rest.iter().map(|&i| encoded[i].len()));
                    let chunk = &rest[..take];
                    let posted = self
                        .post_chunk(
                            dest,
                            table.version(),
                            chunk,
                            &encoded,
                            timeout,
                            durable,
                            released,
                        )
                        .await;
                    for &i in &chunk[..posted.acked] {
                        delivery[i] = Some(Delivery::Acked);
                    }
                    let Some((failed, e)) = posted.error else {
                        rest = &rest[take..];
                        continue;
                    };
                    let unsent = &rest[posted.acked..];
                    match self.learn(e, &mut learned) {
                        Learn::Again => {
                            // No message from the failure onwards has been written:
                            // re-route the whole remainder by the new table.
                            again.extend_from_slice(unsent);
                            regroup = true;
                        }
                        Learn::Fail(e) => {
                            for &i in &unsent[..failed] {
                                delivery[i] = Some(Delivery::Failed);
                                crate::metrics::interrange::message(
                                    crate::metrics::interrange::MessageKind::of(&msgs[i]),
                                    outcome_of(&e),
                                );
                            }
                            for &i in &unsent[failed..] {
                                delivery[i] = Some(Delivery::NotSent);
                            }
                            errors.push((unsent[0], e));
                        }
                    }
                    break;
                }
            }
            // Input order: each new destination gets its part in exactly the old order.
            again.sort_unstable();
            todo = again;
        }
        if errors.iter().any(|(_, e)| e.is_retryable()) {
            // A failed send is evidence the routing table may be stale (a leader change, a dead
            // node). Ask for a refresh now instead of waiting for the period — the outbox rows
            // are intact, the next drain sends by the new table. A non-retryable error
            // (the receiver says this very message is wrong) is not the table's fault.
            self.routing.request_refresh();
        }
        let delivery: Vec<Delivery> = delivery
            .into_iter()
            .map(|d| d.unwrap_or(Delivery::NotSent))
            .collect();
        for (msg, d) in msgs.iter().zip(&delivery) {
            use crate::metrics::interrange::{message, MessageKind, Outcome};
            if *d == Delivery::Acked {
                message(MessageKind::of(msg), Outcome::Ok);
            }
        }
        Sent { delivery, errors }
    }

    /// What the receiver's `409 wrong_range` refusal teaches us (D23).
    fn learn(&self, e: TransportError, learned: &mut u32) -> Learn {
        let TransportError::Rejected {
            addr,
            status: 409,
            body,
        } = e
        else {
            return Learn::Fail(e);
        };
        match serde_json::from_str::<WrongRangeBody>(&body) {
            Ok(w) if w.code == WRONG_RANGE => {
                let theirs = w.table.version;
                let newer = *learned < MAX_TABLE_LEARNS
                    && w.table
                        .to_table()
                        .is_some_and(|t| self.cluster.apply(Arc::new(t)));
                if newer {
                    *learned += 1;
                    tracing::info!(
                        from = %addr,
                        version = theirs,
                        "learned a new range table from a refusal — resending by the new table"
                    );
                    return Learn::Again;
                }
                // The receiver's table is not newer than ours: we may be about to go stale
                // too (a range splitting, retiring). Ask for a reload — one
                // GET, cheap.
                self.cluster.request_reload();
                Learn::Fail(TransportError::WrongRange {
                    addr,
                    theirs,
                    message: w.error,
                })
            }
            _ => Learn::Fail(TransportError::Rejected {
                addr,
                status: 409,
                body: body.chars().take(200).collect(),
            }),
        }
    }

    /// Send one batch `chunk` (indexes into `encoded`) to `dest`: one batch request,
    /// or one message at a time when the batch has a single message or the destination has
    /// no batch route.
    ///
    /// `dest` without a leader but with a live **owner that just released**
    /// ([`RoutingTable::releaser_of`]) ⇒ probe it. It can only ack (it has taken the range
    /// back — the receiver checks its own lease) or refuse; only a refusal carrying a table
    /// **newer** than ours is kept for [`Self::learn`] to learn. Every other outcome returns
    /// exactly as when there is nobody to send to: `NoLeader`, one message — the `outcome`
    /// label and the drainer's backoff do not change in the hand-over gap.
    #[allow(clippy::too_many_arguments)]
    async fn post_chunk(
        &self,
        dest: PartitionId,
        version: u64,
        chunk: &[usize],
        encoded: &[Bytes],
        timeout: std::time::Duration,
        durable: &SendTicket,
        released: &Released,
    ) -> Posted {
        let (addr, probe) = match self.address_of(dest) {
            Ok(a) => (a, false),
            Err(TransportError::NoLeader(p)) => {
                match self.routing.releaser_of(dest).and_then(|l| l.address) {
                    Some(a) => (a, true),
                    None => {
                        return Posted {
                            acked: 0,
                            error: Some((1, TransportError::NoLeader(p))),
                        }
                    }
                }
            }
            Err(e) => {
                return Posted {
                    acked: 0,
                    error: Some((1, e)),
                }
            }
        };
        let timeout = if probe {
            PROBE_TIMEOUT.min(timeout)
        } else {
            timeout
        };
        let mut posted = self
            .post_chunk_to(
                &addr, dest, version, chunk, encoded, timeout, durable, released,
            )
            .await;
        if probe {
            posted.error = match posted.error.take() {
                Some((n, e)) if self.teaches_newer_table(&e) => Some((n, e)),
                Some((_, e)) => {
                    tracing::debug!(
                        dest = dest.0,
                        asked = %addr,
                        error = %e,
                        "the owner that just released did not accept and has no newer table"
                    );
                    Some((1, TransportError::NoLeader(dest.0)))
                }
                None => None,
            };
        }
        posted
    }

    /// `e` is a `409 wrong_range` with a table whose version is **greater** than ours.
    fn teaches_newer_table(&self, e: &TransportError) -> bool {
        let TransportError::Rejected {
            status: 409, body, ..
        } = e
        else {
            return false;
        };
        serde_json::from_str::<WrongRangeBody>(body)
            .is_ok_and(|w| w.code == WRONG_RANGE && w.table.version > self.cluster.version())
    }

    /// [`Self::post_chunk`] to an address already chosen.
    #[allow(clippy::too_many_arguments)]
    async fn post_chunk_to(
        &self,
        addr: &str,
        dest: PartitionId,
        version: u64,
        chunk: &[usize],
        encoded: &[Bytes],
        timeout: std::time::Duration,
        durable: &SendTicket,
        released: &Released,
    ) -> Posted {
        if chunk.len() > 1 && !self.lacks_batch(addr) {
            let parts: Vec<Bytes> = chunk.iter().map(|&i| encoded[i].clone()).collect();
            let body = InterPartition::encode_batch(&parts);
            match self
                .post(
                    addr,
                    "/internal/inbound/batch",
                    dest,
                    version,
                    body,
                    timeout,
                    durable,
                    released,
                )
                .await
            {
                Ok(()) => {
                    return Posted {
                        acked: chunk.len(),
                        error: None,
                    }
                }
                // The router of an old binary: no batch route. Our route never returns these
                // two codes (`ingress::inbound_batch`).
                Err(TransportError::Rejected {
                    status: 404 | 405, ..
                }) => {
                    tracing::info!(dest = %addr, "destination has no batch route — sending it one message at a time");
                    self.mark_lacks_batch(addr);
                }
                Err(e) => {
                    return Posted {
                        acked: 0,
                        error: Some((chunk.len(), e)),
                    }
                }
            }
        }
        for (k, &i) in chunk.iter().enumerate() {
            if let Err(e) = self
                .post(
                    addr,
                    "/internal/inbound",
                    dest,
                    version,
                    encoded[i].clone(),
                    timeout,
                    durable,
                    released,
                )
                .await
            {
                return Posted {
                    acked: k,
                    error: Some((1, e)),
                };
            }
        }
        Posted {
            acked: chunk.len(),
            error: None,
        }
    }

    /// One HTTP request to the internal port of `addr`.
    #[allow(clippy::too_many_arguments)]
    async fn post(
        &self,
        addr: &str,
        path: &str,
        dest: PartitionId,
        version: u64,
        body: Bytes,
        timeout: std::time::Duration,
        _durable: &SendTicket,
        _released: &Released,
    ) -> Result<(), TransportError> {
        // `return(409)` / `return(503)` ⇒ as if the receiver returned that code (empty body);
        // no code ⇒ 503. Once per request (batch or single message).
        cairn_store::failpoint!("transport::send_once", |arg: Option<String>| {
            Err(TransportError::Rejected {
                addr: "failpoint transport::send_once".into(),
                status: arg.as_deref().and_then(|a| a.parse().ok()).unwrap_or(503),
                body: String::new(),
            })
        });
        // State the range and table version we routed by: the receiver checks again and
        // refuses (`409 wrong_range`) if the message does not belong to it (D23).
        let url = format!(
            "{}{path}?range={}&table={version}",
            addr.trim_end_matches('/'),
            dest.0,
        );
        let req = self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream")
            // Limit for the whole send ([`SEND_TIMEOUT`], or [`PROBE_TIMEOUT`] when
            // probing). Without it a hung target partition would keep our outbox rows
            // standing forever — the sender would never know to retry.
            .timeout(timeout)
            .body(body);
        let sent = match &self.secret {
            Some(secret) => secret.send(&self.client, req).await,
            None => crate::egress::send(&self.client, req).await,
        };
        let resp = sent.map_err(|source| TransportError::Http {
            addr: addr.to_string(),
            source,
        })?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        // A `409` keeps its body: it may carry a range table to learn (D23) —
        // [`Self::learn`] cuts it itself when it does not.
        let body = if status.as_u16() == 409 {
            body
        } else {
            body.chars().take(200).collect()
        };
        Err(TransportError::Rejected {
            addr: addr.to_string(),
            status: status.as_u16(),
            body,
        })
    }

    /// `addr` answered `404`/`405` on the batch route within the current routing table
    /// generation ([`NoBatch`]).
    fn lacks_batch(&self, addr: &str) -> bool {
        let generation = self.routing.generation();
        let Ok(mut m) = self.no_batch.lock() else {
            return false;
        };
        if m.generation != generation {
            m.generation = generation;
            m.addrs.clear();
        }
        m.addrs.contains(addr)
    }

    fn mark_lacks_batch(&self, addr: &str) {
        let generation = self.routing.generation();
        if let Ok(mut m) = self.no_batch.lock() {
            if m.generation != generation {
                m.generation = generation;
                m.addrs.clear();
            }
            m.addrs.insert(addr.to_string());
        }
    }

    fn address_of(&self, p: PartitionId) -> Result<String, TransportError> {
        let leader = self
            .routing
            .leader_of(p)
            .ok_or(TransportError::NoLeader(p.0))?;
        leader.address.ok_or(TransportError::NoAddress(p.0))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::metrics::interrange::Outcome};

    /// Which send error maps to which `outcome` label. `Http` needs
    /// a real `reqwest::Error` — `dead_destination.rs` checks it through the network
    /// filter.
    #[test]
    fn send_errors_map_to_outcome_labels() {
        let rejected = TransportError::Rejected {
            addr: "a".into(),
            status: 503,
            body: String::new(),
        };
        assert_eq!(outcome_of(&rejected), Outcome::Rejected);
        let wrong = TransportError::WrongRange {
            addr: "a".into(),
            theirs: 3,
            message: String::new(),
        };
        assert_eq!(outcome_of(&wrong), Outcome::WrongRange);
        assert_eq!(
            outcome_of(&TransportError::NoLeader(1)),
            Outcome::Unreachable
        );
        assert_eq!(
            outcome_of(&TransportError::NoAddress(1)),
            Outcome::Unreachable
        );
    }

    /// Re-splitting by a new table is a **stable** partition: each destination gets its part
    /// in exactly the input order, groups ordered by first appearance; every index appears
    /// exactly once.
    #[test]
    fn regrouping_keeps_input_order_within_each_destination() {
        // Even messages to range 7, odd to range 3, multiples of 5 to range 9.
        let dest = |i: usize| {
            PartitionId(if i.is_multiple_of(5) {
                9
            } else if i.is_multiple_of(2) {
                7
            } else {
                3
            })
        };
        let idxs: Vec<usize> = (0..20).collect();
        let groups = group_by_destination(&idxs, dest);
        assert_eq!(
            groups.iter().map(|(d, _)| d.0).collect::<Vec<_>>(),
            [9, 3, 7],
            "groups by first appearance"
        );
        let mut all: Vec<usize> = Vec::new();
        for (d, v) in &groups {
            assert!(v.windows(2).all(|w| w[0] < w[1]), "range {}: {v:?}", d.0);
            assert!(v.iter().all(|&i| dest(i) == *d));
            all.extend(v);
        }
        all.sort_unstable();
        assert_eq!(all, idxs);
        // The remainder after a mid-way failure (non-contiguous indexes) as well.
        let rest = [4, 5, 8, 11, 12, 15];
        let groups = group_by_destination(&rest, dest);
        assert_eq!(
            groups,
            vec![
                (PartitionId(7), vec![4, 8, 12]),
                (PartitionId(9), vec![5, 15]),
                (PartitionId(3), vec![11]),
            ]
        );
    }

    /// The batch limit: 64 messages or 1 MiB of body, whichever comes first; a message over
    /// the limit goes alone; consecutive cuts cover the whole sequence, dropping no message.
    #[test]
    fn chunks_respect_both_caps_and_never_starve_a_large_message() {
        assert_eq!(chunk_len(std::iter::repeat_n(10, 100)), MAX_BATCH_MESSAGES);
        assert_eq!(chunk_len(std::iter::repeat_n(10, 3)), 3);
        assert_eq!(chunk_len(std::iter::empty()), 0);
        // Each message ~300 KiB: three fit in 1 MiB, a fourth does not.
        let big = 300 * 1024;
        assert_eq!(chunk_len(std::iter::repeat_n(big, 10)), 3);
        // A message larger than the limit goes alone — and does not block the next ones.
        assert_eq!(chunk_len([2 * MAX_BATCH_BYTES, 10].into_iter()), 1);
        assert_eq!(chunk_len([10, 2 * MAX_BATCH_BYTES].into_iter()), 1);
        // Consecutive cuts: the batches add up to the message count, each within the limit.
        let sizes: Vec<usize> = (0..500).map(|i| (i * 7919) % 70_000).collect();
        let mut at = 0;
        while at < sizes.len() {
            let n = chunk_len(sizes[at..].iter().copied());
            assert!((1..=MAX_BATCH_MESSAGES).contains(&n));
            let body: usize = InterPartition::BATCH_HEADER
                + sizes[at..at + n]
                    .iter()
                    .map(|s| InterPartition::BATCH_FRAME + s)
                    .sum::<usize>();
            assert!(
                n == 1 || body <= MAX_BATCH_BYTES,
                "batch of {n} messages, {body} bytes"
            );
            at += n;
        }
        assert_eq!(at, sizes.len());
    }

    /// Probing the owner that just released only keeps the error when it is a `409 wrong_range`
    /// with a table **newer** than ours; everything else comes back as `NoLeader`.
    #[test]
    fn only_a_newer_wrong_range_table_survives_a_probe() {
        let t = Transport::new(
            reqwest::Client::new(),
            RoutingTable::new(),
            ClusterConfig::single(),
        );
        let ours = t.cluster.version();
        let body = |code: &str, version: u64| {
            serde_json::json!({
                "error": "x",
                "code": code,
                "table": { "version": version, "nextRangeId": 1, "ranges": [] },
            })
            .to_string()
        };
        let rejected = |status, body| TransportError::Rejected {
            addr: "a".into(),
            status,
            body,
        };
        assert!(t.teaches_newer_table(&rejected(409, body(WRONG_RANGE, ours + 1))));
        assert!(!t.teaches_newer_table(&rejected(409, body(WRONG_RANGE, ours))));
        assert!(!t.teaches_newer_table(&rejected(409, body("other", ours + 1))));
        assert!(!t.teaches_newer_table(&rejected(409, "not json".into())));
        assert!(!t.teaches_newer_table(&rejected(503, body(WRONG_RANGE, ours + 1))));
        assert!(!t.teaches_newer_table(&TransportError::NoLeader(1)));
    }

    /// Old binaries are remembered by address, only within one routing table generation.
    #[test]
    fn old_binary_memory_is_per_address_and_cleared_when_routing_changes() {
        let routing = RoutingTable::new();
        let t = Transport::new(
            reqwest::Client::new(),
            routing.clone(),
            ClusterConfig::single(),
        );
        assert!(!t.lacks_batch("http://a"));
        t.mark_lacks_batch("http://a");
        assert!(t.lacks_batch("http://a"));
        assert!(!t.lacks_batch("http://b"), "only the probed address");
        let leader = |term| crate::routing::Leader {
            node_id: "n".into(),
            address: Some("http://a".into()),
            ingress: None,
            term,
        };
        routing.insert(PartitionId(1), leader(1));
        assert!(
            !t.lacks_batch("http://a"),
            "routing table changed ⇒ forgotten"
        );
        t.mark_lacks_batch("http://a");
        routing.insert(PartitionId(1), leader(1));
        assert!(
            t.lacks_batch("http://a"),
            "rewriting the same old entry is not a change"
        );
    }
}
