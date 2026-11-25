// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Server-side schedule management: `PUT`/`GET`/`DELETE`/pause/resume/manual
//! fire/list, running on the engine of the range that holds the schedule's pk
//! (actor, [`crate::actor::Command::Schedule`]).
//!
//! The firing round is in `turn/schedule.rs` (hot path); these are the admin commands —
//! rare, user-triggered, not on the turn path.

use {
    crate::{actor::ActorError, lease::Lease, turn},
    bytes::Bytes,
    cairn_engine::{
        schedule::{Overlap, ScheduleRow, SpecRow},
        InvocationId, ScheduleWrite,
    },
    cairn_store::{Scope, TenantId},
};

/// A schedule definition, checked at the API boundary (the spec parses, has a mark
/// within 5 years, a valid target).
#[derive(Clone, Debug)]
pub struct PutSchedule {
    pub tenant: TenantId,
    pub scope: Option<Scope>,
    pub name: String,
    pub spec: SpecRow,
    pub service: String,
    pub handler: String,
    /// Object key of the target; empty = unkeyed.
    pub key: String,
    pub limit_key: Option<String>,
    pub input: Bytes,
    pub overlap: Overlap,
    pub paused: bool,
    pub jitter_ms: u64,
}

/// A schedule management command.
#[derive(Clone, Debug)]
pub enum ScheduleOp {
    Put(Box<PutSchedule>),
    Get(InvocationId),
    Delete(InvocationId),
    Pause(InvocationId),
    Resume(InvocationId),
    /// Fire once right now, by the requested id (resending the same id ⇒ the same run).
    Trigger {
        id: InvocationId,
        request: String,
    },
    /// Every schedule of the range, at most `limit`.
    List {
        limit: usize,
    },
    /// A chunk of the range's schedules after cursor `after`, at most `limit` index
    /// rows, filtered **under the engine** by `tenant` (row form: `None` = `default`) and
    /// `scope`.
    Page {
        after: Option<(u64, InvocationId)>,
        limit: usize,
        tenant: Option<String>,
        scope: Option<String>,
    },
    /// Number of schedules per tenant within an index chunk after `after`.
    Counts {
        after: Option<(u64, InvocationId)>,
        limit: usize,
    },
}

/// A schedule as the API sees it: the row, the real fire time, and the state of recent
/// runs **in the same range** (read when asked, writes nothing).
#[derive(Clone, Debug)]
pub struct ScheduleView {
    pub id: InvocationId,
    /// Real fire time (jitter added); `u64::MAX` = paused.
    pub due: u64,
    pub row: ScheduleRow,
    /// In the order of `row.last`: the run's state if it is in this range.
    pub last_status: Vec<Option<&'static str>>,
}

/// The answer to a [`ScheduleOp`].
#[derive(Debug)]
pub enum ScheduleAnswer {
    /// `created`: a new schedule; `changed`: the definition changed (same hash ⇒ `false`,
    /// `next_at` not moved).
    Put {
        view: ScheduleView,
        created: bool,
        changed: bool,
    },
    View(Option<ScheduleView>),
    Deleted(bool),
    /// `None` = no such schedule; `Some(None)` = L0 throttle, resend later.
    Triggered(Option<Option<(InvocationId, bool)>>),
    List(Vec<ScheduleView>),
    /// One chunk ([`ScheduleOp::Page`]): matching rows and the next cursor (`None` = done).
    Page {
        rows: Vec<ScheduleView>,
        next: Option<(u64, InvocationId)>,
    },
    /// One count chunk ([`ScheduleOp::Counts`]): tenant (row form) → number of schedules.
    Counts {
        by_tenant: std::collections::BTreeMap<Option<String>, u64>,
        next: Option<(u64, InvocationId)>,
    },
    /// Refused, nothing written: HTTP `status`, machine-readable `code`.
    Refused {
        status: u16,
        code: Option<&'static str>,
        message: String,
    },
}

/// `code` of the `409` when the `schedules/table` gate is not on yet (CG-13).
pub const SCHEDULES_DISABLED: &str = "schedules_disabled";

fn refused(status: u16, message: impl Into<String>) -> ScheduleAnswer {
    ScheduleAnswer::Refused {
        status,
        code: None,
        message: message.into(),
    }
}

/// Whether the schedule's target exists in the current revision, and whether it can be
/// scheduled: missing ⇒ `404`; a workflow run (once per id, a schedule firing twice is
/// meaningless) or a virtual object without a key ⇒ `400`.
fn check_target(ctx: &turn::Ctx<'_>, p: &PutSchedule) -> Option<ScheduleAnswer> {
    let Some(d) = ctx.deployments.current(p.tenant.as_str(), &p.service) else {
        return Some(refused(
            404,
            format!("no deployment declares service {:?} yet", p.service),
        ));
    };
    if d.handler(&p.service, &p.handler).is_none() {
        return Some(refused(
            404,
            format!("service {:?} has no handler {:?}", p.service, p.handler),
        ));
    }
    if d.kind(&p.service, &p.handler) == cairn_engine::InvocationKind::WorkflowRun {
        return Some(refused(
            400,
            "target is a workflow run — each workflow id runs once, a schedule cannot target it",
        ));
    }
    let object = d
        .service(&p.service)
        .is_some_and(|s| s.ty == crate::discovery::ServiceType::VirtualObject);
    if object && p.key.is_empty() {
        return Some(refused(400, "virtual object target needs target.key"));
    }
    None
}

/// Execute `op` on the range's engine.
pub(crate) async fn run(
    engine: &mut Lease,
    ctx: &turn::Ctx<'_>,
    op: ScheduleOp,
) -> Result<ScheduleAnswer, ActorError> {
    let now = engine.clock().now().wall_ms();
    let range = engine.partition().0;
    // The `schedules/table` gate (CG-13): a binary that does not know the table may still be
    // leader ⇒ write no schedule rows. Reading is harmless.
    let writes = !matches!(
        op,
        ScheduleOp::Get(_)
            | ScheduleOp::List { .. }
            | ScheduleOp::Page { .. }
            | ScheduleOp::Counts { .. }
    );
    if writes && !engine.schedules() {
        return Ok(ScheduleAnswer::Refused {
            status: 409,
            code: Some(SCHEDULES_DISABLED),
            message: "gate schedules/table is not enabled — `cairn-ctl cluster enable-schedules`"
                .into(),
        });
    }
    // A human command on a schedule: the audit row goes with the commit that writes the
    // schedule row (entity = schedule id); nothing changed ⇒ a separate commit with the outcome.
    let entity = match &op {
        ScheduleOp::Put(p) => Some(cairn_store::schedule_id(
            &p.tenant,
            p.scope.as_ref(),
            &p.name,
        )),
        ScheduleOp::Delete(id)
        | ScheduleOp::Pause(id)
        | ScheduleOp::Resume(id)
        | ScheduleOp::Trigger { id, .. } => Some(*id),
        ScheduleOp::Get(_)
        | ScheduleOp::List { .. }
        | ScheduleOp::Page { .. }
        | ScheduleOp::Counts { .. } => None,
    };
    let audit = match entity {
        Some(e) => ctx.audit_begin(engine, e).await,
        None => None,
    };
    let r = run_op(engine, ctx, op, now, range).await?;
    if let Some(e) = &entity {
        let outcome = match &r {
            ScheduleAnswer::Put { changed: false, .. } => "noop",
            ScheduleAnswer::Deleted(false) | ScheduleAnswer::View(None) => "not_found",
            ScheduleAnswer::Triggered(None) => "not_found",
            ScheduleAnswer::Triggered(Some(None)) => "noop",
            ScheduleAnswer::Refused { .. } => "refused",
            _ => "noop",
        };
        turn::audit_end(engine, ctx, audit, e, outcome).await?;
    }
    Ok(r)
}

async fn run_op(
    engine: &mut Lease,
    ctx: &turn::Ctx<'_>,
    op: ScheduleOp,
    now: u64,
    range: u32,
) -> Result<ScheduleAnswer, ActorError> {
    match op {
        ScheduleOp::Put(p) => {
            if let Some(no) = check_target(ctx, &p) {
                return Ok(no);
            }
            put(engine, *p, now, range).await
        }
        ScheduleOp::Get(id) => Ok(ScheduleAnswer::View(view(engine, &id).await?)),
        ScheduleOp::Delete(id) => {
            let Some((due, _)) = engine.schedule(&id).await? else {
                return Ok(ScheduleAnswer::Deleted(false));
            };
            engine.delete_schedule(&id, due).await?;
            crate::metrics::schedules::adjust(range, -1);
            Ok(ScheduleAnswer::Deleted(true))
        }
        ScheduleOp::Pause(id) | ScheduleOp::Resume(id) => {
            let pause = matches!(op, ScheduleOp::Pause(_));
            let Some((due, mut row)) = engine.schedule(&id).await? else {
                return Ok(ScheduleAnswer::View(None));
            };
            if row.paused != pause {
                row.paused = pause;
                row.updated_ms = now;
                // `resume` counts from now — does not fire the marks already past.
                let new_due = if pause {
                    u64::MAX
                } else {
                    next_due(&id, &mut row, now)
                };
                engine
                    .put_schedule(ScheduleWrite {
                        id,
                        old_due: Some(due),
                        due: new_due,
                        row: &row,
                    })
                    .await?;
            }
            Ok(ScheduleAnswer::View(view(engine, &id).await?))
        }
        ScheduleOp::Trigger { id, request } => {
            let Some((due, row)) = engine.schedule(&id).await? else {
                return Ok(ScheduleAnswer::Triggered(None));
            };
            let r = turn::schedule::trigger(engine, ctx, id, due, row, &request, now).await?;
            Ok(ScheduleAnswer::Triggered(Some(match r {
                turn::schedule::Triggered::Fired { id, created } => Some((id, created)),
                turn::schedule::Triggered::NotYet => None,
                turn::schedule::Triggered::Refused(s) => {
                    return Err(crate::actor::ActorError::TenantRefused(s))
                }
            })))
        }
        ScheduleOp::List { limit } => {
            let mut out = Vec::new();
            for (due, id, row) in engine.list_schedules(limit).await? {
                out.push(ScheduleView {
                    id,
                    due,
                    row,
                    last_status: Vec::new(),
                });
            }
            Ok(ScheduleAnswer::List(out))
        }
        ScheduleOp::Page {
            after,
            limit,
            tenant,
            scope,
        } => {
            let (rows, next) = engine.list_schedules_page(after, limit).await?;
            let rows = rows
                .into_iter()
                .filter(|(_, _, row)| row.tenant == tenant)
                .filter(|(_, _, row)| scope.is_none() || row.scope == scope)
                .map(|(due, id, row)| ScheduleView {
                    id,
                    due,
                    row,
                    last_status: Vec::new(),
                })
                .collect();
            Ok(ScheduleAnswer::Page { rows, next })
        }
        ScheduleOp::Counts { after, limit } => {
            let (rows, next) = engine.list_schedules_page(after, limit).await?;
            let mut by_tenant = std::collections::BTreeMap::new();
            for (_, _, row) in rows {
                *by_tenant.entry(row.tenant).or_insert(0) += 1;
            }
            Ok(ScheduleAnswer::Counts { by_tenant, next })
        }
    }
}

/// The next mark after `now` for `row` (sets `row.next_at`) and the real fire time. A spec with
/// no marks left ⇒ paused.
fn next_due(id: &InvocationId, row: &mut ScheduleRow, now: u64) -> u64 {
    let Ok(spec) = row.spec.parse(0) else {
        row.paused = true;
        return u64::MAX;
    };
    match spec.next(now as i64) {
        Some(n) => {
            let n = n as u64;
            row.next_at = n;
            let after = spec.next(n as i64).map(|t| t as u64);
            row.fire_due(id, n, after)
        }
        None => {
            row.paused = true;
            u64::MAX
        }
    }
}

async fn put(
    engine: &mut Lease,
    p: PutSchedule,
    now: u64,
    range: u32,
) -> Result<ScheduleAnswer, ActorError> {
    let id = cairn_store::schedule_id(&p.tenant, p.scope.as_ref(), &p.name);
    let mut row = ScheduleRow {
        name: p.name,
        tenant: (!p.tenant.is_default()).then(|| p.tenant.as_str().to_string()),
        scope: p.scope.map(|s| s.as_str().to_string()),
        spec: p.spec,
        service: p.service,
        handler: p.handler,
        key: p.key,
        limit_key: p.limit_key,
        input: p.input,
        paused: p.paused,
        overlap: p.overlap,
        jitter_ms: p.jitter_ms,
        next_at: 0,
        fire_seq: 0,
        last: Vec::new(),
        fired: 0,
        missed: 0,
        skipped: 0,
        hash: [0; 16],
        created_ms: now,
        updated_ms: now,
    };
    row.hash = row.definition_hash();
    let old = engine.schedule(&id).await?;
    if let Some((due, cur)) = &old {
        // Same definition, same pause flag ⇒ no-op: an app calling `PUT` at start-up
        // is safe, `next_at` does not move.
        if cur.hash == row.hash && cur.paused == row.paused {
            return Ok(ScheduleAnswer::Put {
                view: ScheduleView {
                    id,
                    due: *due,
                    row: cur.clone(),
                    last_status: Vec::new(),
                },
                created: false,
                changed: false,
            });
        }
        // The definition changed: keep history and counters, the next mark is recomputed from now.
        row.fire_seq = cur.fire_seq;
        row.last = cur.last.clone();
        row.fired = cur.fired;
        row.missed = cur.missed;
        row.skipped = cur.skipped;
        row.created_ms = cur.created_ms;
    }
    let due = if row.paused {
        u64::MAX
    } else {
        next_due(&id, &mut row, now)
    };
    engine
        .put_schedule(ScheduleWrite {
            id,
            old_due: old.as_ref().map(|(d, _)| *d),
            due,
            row: &row,
        })
        .await?;
    if old.is_none() {
        crate::metrics::schedules::adjust(range, 1);
    }
    Ok(ScheduleAnswer::Put {
        view: ScheduleView {
            id,
            due,
            row,
            last_status: Vec::new(),
        },
        created: old.is_none(),
        changed: true,
    })
}

async fn view(engine: &mut Lease, id: &InvocationId) -> Result<Option<ScheduleView>, ActorError> {
    let Some((due, row)) = engine.schedule(id).await? else {
        return Ok(None);
    };
    let mut last_status = Vec::with_capacity(row.last.len());
    for l in &row.last {
        // A run in the same range (unkeyed target): its state is read from the same Db,
        // writing nothing. Another range: only the id.
        let st = if engine.owns_id(&l.id) {
            engine.status(&l.id).await?.map(|s| s.name())
        } else {
            None
        };
        last_status.push(st);
    }
    Ok(Some(ScheduleView {
        id: *id,
        due,
        row,
        last_status,
    }))
}
