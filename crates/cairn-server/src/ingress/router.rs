// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Routing separated from HTTP: [`Router`].
//!
//! Three jobs every external source needs, none of which belong to HTTP:
//!
//! - **generating ids** ([`Router::derive`]): a random id under the destination's
//!   partition key, a deterministic id from the idempotency key (every sender with the
//!   same key computes the same id — that is the dedupe mechanism itself), the
//!   deterministic id of a workflow `run`;
//! - **asking the right range** ([`Router::ask_range`]): the range table changing midway
//!   (`WrongRange`/`Stopped` ⇒ wait for the table to change, then ask again exactly
//!   once), a range nobody leads yet (a bounded wait);
//! - **answering "not the leader"** ([`Router::not_leader`]): which range, and where the
//!   leader is if the routing table knows.
//!
//! The axum handler (`invoke.rs`) is left with parsing the request and serializing the
//! answer. A non-HTTP source (a Kafka ingress behind a feature gate) calls
//! [`Router::submit`] directly — the new work then goes through [`crate::turn::admit`]
//! on the destination range like every other call. `tests/router_source.rs` is a fake
//! source shaped like Kafka.
//!
//! No axum dependency: errors are [`RouteError`]/[`Refused`], not HTTP statuses.

use {
    crate::{
        actor::{ActorError, EngineHandle, InvokeOutcome},
        partitions::Partitions,
        registry::Registry,
        routing::RoutingTable,
    },
    bytes::Bytes,
    cairn_engine::{InvocationKind, Target},
    cairn_store::{
        make_invocation_id, partition_key_for_target, ClusterConfig, InvocationId, LimitKey,
        PartitionId, PartitionKey, Scope, TenantId,
    },
    std::time::Duration,
};

/// Cap on waiting for the range table to change after a `WrongRange` (a split/merge in
/// its write-stop window) before retrying exactly once; also the cap on waiting for a
/// range nobody leads yet.
const TABLE_CHANGE_WAIT: Duration = Duration::from_secs(2);

/// A node's routing path — shared between HTTP ingress and every non-HTTP source.
/// Cheap to clone (every field is an `Arc` inside).
#[derive(Clone)]
pub struct Router {
    pub(super) partitions: Partitions,
    pub(super) registry: Registry,
    pub(super) cluster: ClusterConfig,
    /// Who holds which partition. Empty ⇒ a single-node deployment, nobody to point to.
    pub(super) routing: RoutingTable,
    /// This node's id in the routing table — to tell "the leader is us" (opening the
    /// range, not serving yet) from "the leader is another node".
    pub(super) node_id: Option<String>,
}

/// One handler call, from any source.
#[derive(Clone, Debug)]
pub struct Call {
    pub target: Target,
    /// Only affects queueing — not part of the identity.
    pub limit_key: Option<LimitKey>,
    /// Same key ⇒ same invocation. Kafka source: `(topic, source partition,
    /// offset)`.
    pub idempotency_key: Option<String>,
    pub input: Bytes,
    /// The checked `cairnflow-log-level` header: goes into the header of the same name
    /// on entry 0. `None` for non-HTTP sources.
    pub log_level: Option<String>,
    /// Checked user tags: go into the metadata and `ByAttr`.
    pub tags: Vec<(String, String)>,
}

/// What the caller waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Run the first turn, then return the result (`/invoke`) — or answer early if it
    /// has to queue.
    Wait,
    /// Answer as soon as it is durable (`/send`).
    Send,
}

/// A call that reached the destination range and was answered.
pub struct Accepted {
    pub id: InvocationId,
    /// The engine that answered — for follow-up questions about `id`.
    pub engine: EngineHandle,
    pub outcome: InvokeOutcome,
    /// The waiter registered in `engine`'s book before the question — only with
    /// [`Delivery::Wait`].
    pub watch: Option<crate::actor::Watch>,
}

/// A call refused **before** it had an id — nothing was created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The service or handler is not in the tenant's deployment registry.
    NotFound(String),
    /// A malformed call (a workflow without a key, a workflow with an idempotency key).
    BadRequest(String),
}

/// A call without an answer from the destination range.
pub enum RouteError {
    Refused(Refused),
    /// The table says the id belongs to a range the node has no engine for after a
    /// bounded wait — a range just created, or not part of this cluster.
    NoEngine {
        id: InvocationId,
        partition: PartitionId,
    },
    /// The range answered with an error. `error.is_fenced()` ⇒ ask
    /// [`Router::not_leader`] where to go; `Overloaded` ⇒ nothing was created, send again
    /// after `retry_after_ms`.
    Range {
        id: InvocationId,
        error: ActorError,
    },
}

/// "This node is not the range's leader" — in machine-readable form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeader {
    pub partition: PartitionId,
    /// The leader's **ingress** port, if the routing table knows it.
    pub leader: Option<String>,
}

impl Router {
    /// The id for a `call`, or the reason it is not accepted.
    ///
    /// The id is generated **after** the target is known: its partition is a function of
    /// the object key. With an idempotency key all 16 identifying bytes are a function of
    /// the key tuple too — every client with the same key computes the same id, and that
    /// is the dedupe mechanism itself. For a workflow `run` the workflow key plays exactly
    /// that role.
    pub fn derive(&self, call: &Call) -> Result<InvocationId, Refused> {
        let target = &call.target;
        let kind = self.handler_kind(target.tenant.as_str(), &target.service, &target.handler)?;
        // ── Workflow: two rules at the door, before generating the id.
        if kind == InvocationKind::WorkflowRun {
            if target.object_key.is_empty() {
                return Err(Refused::BadRequest(format!(
                    "{}/{} is a workflow run — `?key=` is required (the workflow key is the identity of the run)",
                    target.service, target.handler
                )));
            }
            // **An idempotency key is refused, not ignored.** The workflow key already
            // is the dedupe key; accepting a second one promises semantics we do not
            // implement (which key wins?). Logging a warning and ignoring it would hide
            // that — cairn says so right where the person who typed it sees it.
            if call.idempotency_key.is_some() {
                return Err(Refused::BadRequest(format!(
                    "do not use Idempotency-Key with {}/{}: the call is already idempotent by workflow key",
                    target.service, target.handler
                )));
            }
            return workflow_id(
                &target.tenant,
                target.scope(),
                &target.service,
                &target.object_key,
            );
        }
        Ok(match call.idempotency_key.as_deref() {
            Some(idem) => {
                let key = partition_key_for_target(
                    &target.tenant,
                    target.scope(),
                    &target.service,
                    &target.object_key,
                    Some(idem),
                )
                .expect("an idempotency key always yields a partition key");
                cairn_store::idempotent_invocation_id(
                    key,
                    &target.tenant,
                    target.scope(),
                    &target.service,
                    &target.handler,
                    &target.object_key,
                    idem,
                )
            }
            None => self.new_invocation_id(target),
        })
    }

    /// The kind of handler `service/handler` in `tenant`'s deployment registry, or
    /// `NotFound` if the registry does not have it.
    ///
    /// The service registry of **the key's tenant**: the same service name in another
    /// tenant is another deployment, and does not exist here.
    pub fn handler_kind(
        &self,
        tenant: &str,
        service: &str,
        handler: &str,
    ) -> Result<InvocationKind, Refused> {
        let deployment = self.registry.find_in(tenant, service).ok_or_else(|| {
            Refused::NotFound(format!("no deployment declares service {service:?} yet"))
        })?;
        if deployment.handler(service, handler).is_none() {
            return Err(Refused::NotFound(format!(
                "service {service:?} has no handler {handler:?}"
            )));
        }
        Ok(deployment.kind(service, handler))
    }

    /// Sends `call` to the destination range: generates the id, asks the right range,
    /// returns its answer. On the destination range the call goes through
    /// [`crate::turn::admit`] (ingress source: the L0 brake, idempotency, quarantine,
    /// rules).
    ///
    /// **Sending again is safe** when the outcome is ambiguous (`Err` after the id
    /// exists, leader lost midway) **if** there is an idempotency key: the resend carries
    /// the same id and meets the existing invocation. Without a key every send is a new id.
    pub async fn submit(&self, call: Call, delivery: Delivery) -> Result<Accepted, RouteError> {
        let id = self.derive(&call).map_err(RouteError::Refused)?;
        let (engine, outcome) = self
            .ask_range(cairn_store::partition_key_of(&id), |engine| {
                let call = call.clone();
                async move {
                    match delivery {
                        Delivery::Wait => {
                            // Register the waiter **before** asking: an outcome submitted
                            // between the answer and the wait loop does not slip past the notifier.
                            let watch = engine.waiters().watch_armed(id);
                            engine
                                .invoke_logged(
                                    id,
                                    call.target,
                                    call.limit_key,
                                    call.idempotency_key,
                                    call.input,
                                    call.log_level,
                                    call.tags,
                                )
                                .await
                                .map(|o| (o, Some(watch)))
                        }
                        Delivery::Send => engine
                            .submit_logged(
                                id,
                                call.target,
                                call.limit_key,
                                call.idempotency_key,
                                call.input,
                                call.log_level,
                                call.tags,
                            )
                            .await
                            .map(|o| (o, None)),
                    }
                }
            })
            .await
            .ok_or(RouteError::NoEngine {
                id,
                partition: self.cluster.partition_of_id(&id),
            })?;
        let (outcome, watch) = outcome.map_err(|error| RouteError::Range { id, error })?;
        Ok(Accepted {
            id,
            engine,
            outcome,
            watch,
        })
    }

    /// The "not the leader" answer for `id`: which range, where the leader is.
    ///
    /// We are having to return `NotLeader`: our table may be pointing the wrong way. Ask
    /// for a refresh only when the table does not know the leader, or points at us — if
    /// the table already knows another leader the caller just hit the wrong node, and
    /// every refresh request is a round of GETs over every session (floor 200 ms): a
    /// caller regularly hitting the wrong node would turn the TTL/5 observation tick into
    /// 5 rounds a second. If that leader has died the tracker sees it on its own tick,
    /// and the node pointed to asks for a refresh itself when it returns `NotLeader`.
    pub fn not_leader(&self, id: &InvocationId) -> NotLeader {
        self.not_leader_at(self.cluster.partition_of_id(id))
    }

    /// Like [`Self::not_leader`], for a call with no id yet — routed by key (an object's
    /// promise: `(service, key)`).
    pub fn not_leader_of_key(&self, key: PartitionKey) -> NotLeader {
        self.not_leader_at(self.cluster.partition_of_key(key))
    }

    fn not_leader_at(&self, p: PartitionId) -> NotLeader {
        let known_elsewhere = self
            .routing
            .leader_of(p)
            .is_some_and(|l| self.node_id.as_deref() != Some(l.node_id.as_str()));
        if !known_elsewhere {
            self.routing.request_refresh();
        }
        NotLeader {
            partition: p,
            // A hint for the caller: the leader's **ingress** port, not the internal
            // port.
            leader: self.routing.leader_of(p).and_then(|l| l.ingress),
        }
    }

    /// Range `p` **cannot be served anywhere yet**: nobody leads it, or this node just
    /// claimed it (the routing table records us as soon as the lease is claimed, before
    /// the Db is opened and the queues recovered). `NotLeader` at that point is
    /// transient — wait, do not point the way back to ourselves.
    pub fn leader_pending(&self, p: PartitionId) -> bool {
        match self.routing.leader_of(p) {
            None => true,
            Some(l) => self.node_id.as_deref() == Some(l.node_id.as_str()),
        }
    }

    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Asks the engine of the range holding `key` — and if the engine says `WrongRange`
    /// (our table is stale, or the range is retiring), **reloads the table and retries
    /// exactly once** at the right engine (D23). The caller sees nothing but latency.
    ///
    /// Exactly once: a second `WrongRange` means the table is mid-change (the range has
    /// an intent, not committed) — return it so the call site reports `503`, the caller
    /// retries. `WrongRange` is returned **before** the engine does anything, so the retry
    /// is safe for every command. Returns the engine that answered too, for follow-up
    /// commands.
    ///
    /// `None` = the table says the key belongs to a range the node has no engine for
    /// after a bounded wait — a range just created.
    pub async fn ask_range<T, F, Fut>(
        &self,
        key: PartitionKey,
        mut ask: F,
    ) -> Option<(EngineHandle, crate::actor::Result<T>)>
    where
        F: FnMut(EngineHandle) -> Fut,
        Fut: std::future::Future<Output = crate::actor::Result<T>>,
    {
        // The table just changed (a split/merge commit) and the `follow_table` loop has
        // not finished building the new range's engine: wait for it (bounded), do not
        // report "not in this cluster" for a key the table clearly says belongs to it.
        let engine = match self.partitions.get(self.cluster.partition_of_key(key)) {
            Some(e) => e,
            None => self.engine_of_key_settled(key).await?,
        };
        let first = self
            .ask_led(&engine, self.cluster.partition_of_key(key), &mut ask)
            .await;
        // Two answers mean "the range is changing": `WrongRange`, and `Stopped` when the
        // command arrives exactly as the range quiesces (its task is cancelled, the reply
        // channel dropped) or retires (the engine exits for good). Retrying is safe:
        // `WrongRange` is returned before the engine does anything; with `Stopped`, the
        // command's id was fixed before asking and every command by id is idempotent.
        let (refusal, stopped) = match &first {
            Err(ActorError::WrongRange) => (true, false),
            Err(ActorError::Stopped) => (true, true),
            _ => (false, false),
        };
        if !refusal {
            return Some((engine, first));
        }
        // Wait (bounded) for **the table to change** before retrying — retrying at once
        // on the same engine of a quiescing range only gets the same answer back. Ask
        // the `follow_table` loop to reload (folding every waiter into one GET) instead
        // of one GET per request.
        let refused_at = self.cluster.version();
        if let Err(e) = self.cluster.reload().await {
            tracing::debug!(error = %e, "cannot reload the range table after WrongRange");
        }
        let deadline = tokio::time::Instant::now() + TABLE_CHANGE_WAIT;
        let mut changes = self.cluster.subscribe();
        while self.cluster.version() == refused_at
            && !engine.is_retiring()
            && tokio::time::Instant::now() < deadline
        {
            self.cluster.request_reload();
            let _ = tokio::time::timeout(Duration::from_millis(200), changes.changed()).await;
        }
        // `Stopped` with the table unchanged and the range not retiring: the engine
        // stopped for another reason (the node is shutting down) — not a range table matter.
        if stopped && self.cluster.version() == refused_at && !engine.is_retiring() {
            return Some((engine, first));
        }
        let engine = self.engine_of_key_settled(key).await.unwrap_or(engine);
        let second = self
            .ask_led(&engine, self.cluster.partition_of_key(key), &mut ask)
            .await;
        Some((engine, second))
    }

    /// Asks `engine`; if it returns `NotLeader` and **nobody** leads the range (per the
    /// routing table) — a range just created by a split/merge, or one that just lost its
    /// owner and is being claimed — waits (bounded) and asks again. `NotLeader` is
    /// returned before the engine does anything, so asking again is safe. A range with a
    /// leader on another node returns at once: the call site points the way there.
    async fn ask_led<T, F, Fut>(
        &self,
        engine: &EngineHandle,
        range: PartitionId,
        ask: &mut F,
    ) -> crate::actor::Result<T>
    where
        F: FnMut(EngineHandle) -> Fut,
        Fut: std::future::Future<Output = crate::actor::Result<T>>,
    {
        let deadline = tokio::time::Instant::now() + TABLE_CHANGE_WAIT;
        loop {
            let r = ask(engine.clone()).await;
            let leaderless = matches!(r, Err(ActorError::NotLeader)) && self.leader_pending(range);
            if !leaderless || tokio::time::Instant::now() >= deadline {
                return r;
            }
            cairn_store::pace::poll_gap(Duration::from_millis(20)).await;
        }
    }

    /// The engine of the range holding `key` per the **current** table, waiting (bounded)
    /// if the node just applied a new table and has not finished building the new
    /// range's engine — the `follow_table` loop builds it as soon as the table changes.
    async fn engine_of_key_settled(&self, key: PartitionKey) -> Option<EngineHandle> {
        const SETTLE: Duration = Duration::from_secs(2);
        let t0 = tokio::time::Instant::now();
        loop {
            let p = self.cluster.partition_of_key(key);
            match self.partitions.get(p) {
                Some(e) if !e.is_retiring() => return Some(e),
                other if t0.elapsed() >= SETTLE => return other,
                _ => cairn_store::pace::poll_gap(Duration::from_millis(10)).await,
            }
        }
    }

    /// A new id for a call **without an idempotency key**: partition key ‖ 128 random
    /// bits.
    ///
    /// With a scope / object key the key is a function of them (`EXCLUSIVE` and D-F1
    /// require that); otherwise a random key spreads calls evenly across every
    /// partition. The identifying part is random — no longer "boot mark + counter", which
    /// used to make every invocation of the same partition and process share the first
    /// 8 bytes.
    fn new_invocation_id(&self, target: &Target) -> InvocationId {
        let key = partition_key_for_target(
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
            None,
        )
        .unwrap_or_else(|| PartitionKey(u64::from_be_bytes(random_bytes())));
        make_invocation_id(key, random_bytes())
    }
}

/// The run id of a workflow — computable from *outside*, without asking anyone.
///
/// That is the whole point of deterministic ids: a client that knows `(scope, service,
/// key)` computes the id itself, so `attach` and `output` need no `workflow key →
/// invocation` lookup table at all. A design with random ids needs such a table; cairn
/// does not, because the id already carries the partition.
///
/// **An empty workflow key ⇒ refused**, never a made-up partition key (D-a): a workflow
/// without a key has no object for its state and promises to belong to. The call sites
/// already block empty keys earlier with their own message; this is the last fence, so
/// that no path computes an id under pk 0 or under the pk of the scope alone.
pub fn workflow_id(
    tenant: &TenantId,
    scope: Option<&Scope>,
    service: &str,
    workflow_key: &str,
) -> Result<InvocationId, Refused> {
    let no_key = || {
        Refused::BadRequest(format!(
            "{service}: a workflow needs a key — a workflow without a key has no owner"
        ))
    };
    if workflow_key.is_empty() {
        return Err(no_key());
    }
    let key =
        partition_key_for_target(tenant, scope, service, workflow_key, None).ok_or_else(no_key)?;
    Ok(cairn_store::workflow_invocation_id(
        key,
        tenant,
        scope,
        service,
        workflow_key,
    ))
}

/// Random bytes from the operating system's source.
fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    cairn_store::entropy::fill(&mut b);
    b
}
