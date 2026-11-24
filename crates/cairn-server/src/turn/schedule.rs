// #[hot-path] — a file on every turn's path (AGENTS.md §6): no `info`-level log on the
// turn's normal path, no global lock, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Server-side schedule firing: [`fire_due`] in the background tick,
//! [`trigger`] for a manual firing.
//!
//! Every due schedule is **one commit**: the run (same range) or the `Fire`
//! outbox row (other range) is written together with the schedule row moved to
//! its new `next_at` (CG-12). A crash before the commit ⇒ the next leader fires
//! the same mark again; after ⇒ already durable. No network call while holding
//! the engine (D16): the firing commit only writes the Db, the drainer carries
//! the `Fire` away.

use {
    super::*,
    cairn_engine::{
        schedule::{self as sched, LastFire, LastKind, Overlap, ScheduleRow},
        OutboundFire, ScheduleStamp, ScheduleWrite,
    },
};

/// Maximum schedules fired per tick — like `MAX_DELAYED_PER_TICK`: a backlog
/// after a long downtime does not take the engine thread for a whole tick.
const MAX_FIRES_PER_TICK: usize = 32;

/// Cap on counting missed marks per firing (`missed`): an `every` spec counts
/// by division, cron iterates — a per-minute schedule paused for a year must
/// not iterate half a million marks.
const MISSED_COUNT_CAP: u64 = 100_000;

/// The outcome of one due schedule.
enum Outcome {
    /// The run was created (or the `Fire` row written): the schedule row moves.
    Fired,
    /// `overlap: skip` and the previous run is still running: the schedule row moves, `skipped += 1`.
    Skipped,
    /// Nothing done (L0 throttle, deployment registry lacks the service): the
    /// schedule row stays as is, the next tick retries the same mark.
    Held,
    /// Tenant `limited`/`suspended` or at its `max-live=` cap: the schedule row
    /// moves, `skipped += 1`, a `Skipped` entry in `last` — no run, no `Fire`.
    Refused,
}

/// Fires every schedule due at `now_ms`. The `schedules/table` gate off ⇒ does nothing.
pub(super) async fn fire_due(engine: &mut Lease, ctx: &Ctx<'_>, now_ms: u64) -> Result<usize> {
    if !engine.schedules() {
        return Ok(0);
    }
    let due = engine.due_schedules(now_ms, MAX_FIRES_PER_TICK).await?;
    let mut fired = 0;
    for (due_ms, id, row) in due {
        match fire_one(engine, ctx, now_ms, due_ms, id, row).await {
            Ok(Outcome::Fired) => {
                fired += 1;
                crate::metrics::schedules::fire("fired");
                // The slot right after the firing commit: a crash here, and the
                // next leader sees the schedule row already moved — this mark is
                // not fired again.
                cairn_store::failpoint!("turn::schedule::after_fire", err);
            }
            Ok(Outcome::Skipped) => crate::metrics::schedules::fire("skipped"),
            Ok(Outcome::Refused) => crate::metrics::schedules::fire("refused"),
            Ok(Outcome::Held) => crate::metrics::schedules::fire("throttled"),
            Err(e) if e.is_fenced() => return Err(e),
            // One broken schedule must not block the range's other schedules.
            Err(e) => {
                tracing::warn!(schedule = %hex(&id), error = %e, "schedule fire failed; the next tick retries")
            }
        }
    }
    Ok(fired)
}

/// The next mark after a firing: `(nominal mark, actual fire time)`. `None` =
/// the spec has no mark left within the horizon ⇒ the schedule stops (paused,
/// due `u64::MAX`).
fn next_of(
    spec: &cairn_schedule::Spec,
    row: &ScheduleRow,
    id: &InvocationId,
    after: u64,
) -> Option<(u64, u64)> {
    let next = spec.next(after as i64)? as u64;
    let after_next = spec.next(next as i64).map(|t| t as u64);
    Some((next, row.fire_due(id, next, after_next)))
}

async fn fire_one(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    now_ms: u64,
    due_ms: u64,
    id: InvocationId,
    row: ScheduleRow,
) -> Result<Outcome> {
    // The spec was checked at `PUT`; broken here (another binary's tzdb?) ⇒ leave it as is.
    let spec = row
        .spec
        .parse(0)
        .map_err(|e| ActorError::Rejected(format!("unreadable schedule spec: {e}")))?;
    // **Missed marks**: fire **once** for the latest mark ≤ `now`, the rest go
    // into `missed` (like Kubernetes). The next mark is computed from
    // `max(fired mark, now)`.
    let at0 = row.next_at;
    let lo = at0 as i64 - 1;
    let at = spec
        .last_between(lo, now_ms as i64)
        .map_or(at0, |t| t as u64);
    let lagging = spec
        .count_between(lo, now_ms as i64, MISSED_COUNT_CAP)
        .saturating_sub(1);
    let mut next = row.clone();
    next.fire_seq = row.fire_seq.wrapping_add(1);
    next.missed = row.missed.saturating_add(lagging);
    next.updated_ms = now_ms;
    let new_due = match next_of(&spec, &row, &id, at.max(now_ms)) {
        Some((n, d)) => {
            next.next_at = n;
            d
        }
        None => {
            next.paused = true;
            u64::MAX
        }
    };
    if lagging > 0 {
        crate::metrics::schedules::missed(lagging);
    }
    crate::metrics::schedules::lag(now_ms.saturating_sub(due_ms));
    let write = |row| write_of(id, due_ms, new_due, row);
    let tenant = tenant_of_wire(row.tenant.as_deref(), &id);
    // The slot right before the firing commit: a crash here, nothing is durable
    // yet — the next leader fires exactly this mark again (CG-12).
    cairn_store::failpoint!("turn::schedule::before_fire", err);

    // A tenant that takes no new work: skip the firing **right at the
    // schedule's range**, for both same-range and cross-range targets — recorded
    // with the existing `LastKind::Skipped` (the schedule row is read strictly,
    // so no new variant is added).
    if let Some(t) = &tenant {
        if engine.tenant_refusal(t).is_some() {
            next.skipped = row.skipped.saturating_add(1);
            next.push_last(LastFire {
                at,
                id: cairn_store::schedule_run_id(&id, at),
                kind: LastKind::Skipped,
            });
            engine.put_schedule(write(&next)).await?;
            return Ok(Outcome::Refused);
        }
    }

    if row.keyed() {
        // ── Other range: `Fire` row + schedule row, one commit.
        next.fired = row.fired.saturating_add(1);
        let run = fire_target_id(
            &row,
            tenant.as_ref(),
            &sched::fire_idempotency_key(&row.name, at),
        );
        next.push_last(LastFire {
            at,
            id: run.unwrap_or([0; cairn_store::ID_LEN]),
            kind: LastKind::Fired,
        });
        engine
            .fire_schedule_remote(
                write(&next),
                OutboundFire {
                    schedule: id,
                    fire_seq: next.fire_seq,
                    name: row.name.clone(),
                    at,
                    tenant: row.tenant.clone(),
                    scope: row.scope.clone(),
                    service: row.service.clone(),
                    handler: row.handler.clone(),
                    object_key: row.key.clone(),
                    parameter: row.input.clone(),
                    limit_key: row.limit_key.clone(),
                    idempotency_key: sched::fire_idempotency_key(&row.name, at),
                },
            )
            .await?;
        ctx.drainer.nudge();
        return Ok(Outcome::Fired);
    }

    // ── Same range.
    let run = cairn_store::schedule_run_id(&id, at);
    if row.overlap == Overlap::Skip {
        if let Some(prev) = row.last_fired() {
            // The previous run is still alive (same Db, no network) ⇒ skip this mark (CG-15).
            if engine
                .status(&prev.id)
                .await?
                .is_some_and(|s| !s.is_terminal())
            {
                next.skipped = row.skipped.saturating_add(1);
                next.push_last(LastFire {
                    at,
                    id: run,
                    kind: LastKind::Skipped,
                });
                engine.put_schedule(write(&next)).await?;
                return Ok(Outcome::Skipped);
            }
        }
    }
    next.fired = row.fired.saturating_add(1);
    next.push_last(LastFire {
        at,
        id: run,
        kind: LastKind::Fired,
    });
    let Some(tenant) = tenant else {
        return Ok(Outcome::Held);
    };
    let target = Target {
        tenant,
        scope: None,
        service: row.service.clone(),
        handler: row.handler.clone(),
        object_key: String::new(),
    };
    let limit_key = row
        .limit_key
        .as_deref()
        .and_then(|k| LimitKey::parse(k).ok());
    let header = sched::header_value(&row.name, at);
    let admission = admit::admit(
        engine,
        ctx,
        admit::Source::Schedule,
        admit::Request {
            id: run,
            target: &target,
            limit_key: limit_key.as_ref(),
            idempotency_key: None,
            input: row.input.clone(),
            log_level: None,
            tags: &[],
            sender: None,
            root_id: None,
            schedule: Some(ScheduleStamp {
                header: &header,
                row: Some(write(&next)),
            }),
        },
    )
    .await?;
    use admit::Admission as A;
    match admission {
        A::Created { held, .. } => {
            // Quarantined (`hold`) ⇒ created straight in `Paused`: does not run.
            if held.is_none() {
                ctx.runner.queue(run);
            }
            Ok(Outcome::Fired)
        }
        // The id already exists: a duplicate manual firing, or an old schedule
        // row — only move the schedule row.
        A::Attached { .. } => {
            engine.put_schedule(write(&next)).await?;
            Ok(Outcome::Fired)
        }
        // `Refused` only when the state changed between the check above and
        // `admit`: the next tick skips this mark through the branch above.
        A::Shed | A::NotYet | A::Refused(_) => Ok(Outcome::Held),
        A::Answered(_) | A::Rejected(_) | A::Admitted => {
            unreachable!("a Schedule source never gets an ingress/restart reply")
        }
    }
}

/// The write of schedule row `id` from due time `old` to due time `due`.
fn write_of(id: InvocationId, old: u64, due: u64, row: &ScheduleRow) -> ScheduleWrite<'_> {
    ScheduleWrite {
        id,
        old_due: Some(old),
        due,
        row,
    }
}

/// The id of the cross-range run that `Fire` will create on the target range —
/// only to record in `last` (`GET /schedules/{name}`); the target range
/// recomputes it identically from the idempotency key.
fn fire_target_id(
    row: &ScheduleRow,
    tenant: Option<&cairn_store::TenantId>,
    idem: &str,
) -> Option<InvocationId> {
    let tenant = tenant?;
    let scope = row.scope.as_deref().and_then(|s| Scope::new(s).ok());
    let key = cairn_store::partition_key_for_target(
        tenant,
        scope.as_ref(),
        &row.service,
        &row.key,
        Some(idem),
    )?;
    Some(cairn_store::idempotent_invocation_id(
        key,
        tenant,
        scope.as_ref(),
        &row.service,
        &row.handler,
        &row.key,
        idem,
    ))
}

/// The outcome of [`trigger`].
pub enum Triggered {
    /// The run (same range) or `Fire` row (other range) has been written; `id`
    /// is the run's id, `created` = this call created it.
    Fired { id: InvocationId, created: bool },
    /// L0 throttle, or the registry lacks the service — send again later.
    NotYet,
    /// Tenant `limited`/`suspended` or at its `max-live=` cap: nothing runs.
    Refused(cairn_engine::TenantRefusal),
}

/// A **manual** firing (`POST /schedules/{name}/trigger`): one run right away,
/// ignoring `paused` and `overlap`, without moving `next_at`. The id is derived
/// from `(schedule id, "manual", request id)` — resending the same request
/// yields the same run.
pub async fn trigger(
    engine: &mut Lease,
    ctx: &Ctx<'_>,
    id: InvocationId,
    due: u64,
    row: ScheduleRow,
    request: &str,
    now_ms: u64,
) -> Result<Triggered> {
    let tenant = tenant_of_wire(row.tenant.as_deref(), &id)
        .ok_or_else(|| ActorError::Rejected("corrupt schedule tenant".into()))?;
    // A manual firing is new work — also for a target on another range (not through `admit`).
    if let Some(why) = engine.tenant_refusal(&tenant) {
        return Ok(Triggered::Refused(why));
    }
    let mut next = row.clone();
    next.updated_ms = now_ms;
    next.fired = row.fired.saturating_add(1);
    if row.keyed() {
        let idem = sched::manual_idempotency_key(&row.name, request);
        let run = fire_target_id(&row, Some(&tenant), &idem)
            .ok_or_else(|| ActorError::Rejected("schedule target has no key".into()))?;
        next.fire_seq = row.fire_seq.wrapping_add(1);
        next.push_last(LastFire {
            at: now_ms,
            id: run,
            kind: LastKind::Fired,
        });
        engine
            .fire_schedule_remote(
                ScheduleWrite {
                    id,
                    old_due: Some(due),
                    due,
                    row: &next,
                },
                OutboundFire {
                    schedule: id,
                    fire_seq: next.fire_seq,
                    name: row.name.clone(),
                    at: now_ms,
                    tenant: row.tenant.clone(),
                    scope: row.scope.clone(),
                    service: row.service.clone(),
                    handler: row.handler.clone(),
                    object_key: row.key.clone(),
                    parameter: row.input.clone(),
                    limit_key: row.limit_key.clone(),
                    idempotency_key: idem,
                },
            )
            .await?;
        ctx.drainer.nudge();
        return Ok(Triggered::Fired {
            id: run,
            created: true,
        });
    }
    let run = cairn_store::schedule_manual_run_id(&id, request);
    next.push_last(LastFire {
        at: now_ms,
        id: run,
        kind: LastKind::Fired,
    });
    let target = Target {
        tenant,
        scope: None,
        service: row.service.clone(),
        handler: row.handler.clone(),
        object_key: String::new(),
    };
    let limit_key = row
        .limit_key
        .as_deref()
        .and_then(|k| LimitKey::parse(k).ok());
    let header = sched::header_value(&row.name, now_ms);
    let admission = admit::admit(
        engine,
        ctx,
        admit::Source::Schedule,
        admit::Request {
            id: run,
            target: &target,
            limit_key: limit_key.as_ref(),
            idempotency_key: None,
            input: row.input.clone(),
            log_level: None,
            tags: &[],
            sender: None,
            root_id: None,
            schedule: Some(ScheduleStamp {
                header: &header,
                row: Some(ScheduleWrite {
                    id,
                    old_due: Some(due),
                    due,
                    row: &next,
                }),
            }),
        },
    )
    .await?;
    use admit::Admission as A;
    Ok(match admission {
        A::Created { held, .. } => {
            if held.is_none() {
                ctx.runner.queue(run);
            }
            Triggered::Fired {
                id: run,
                created: true,
            }
        }
        A::Attached { id, .. } => Triggered::Fired { id, created: false },
        A::Shed | A::NotYet => Triggered::NotYet,
        A::Refused(s) => Triggered::Refused(s),
        A::Answered(_) | A::Rejected(_) | A::Admitted => {
            unreachable!("a Schedule source never gets an ingress/restart reply")
        }
    })
}
