// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Parked tickets of background turns (FL-30).

use {
    super::{id, queue, vo},
    crate::{
        drainer::{Rescue, RunRequest, Runner},
        flow::{Parked, Scheduler, Ticket},
    },
    cairn_store::QueueName,
    std::rc::Rc,
};

/// Ticket of `id(n)` at position `seq = n` of an unlocked queue — the turn is handed over
/// through `runner` as the drainer hands it (`Runner::queue`), taken off the channel as the
/// serve loop does.
fn ticket(
    runner: &Runner,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<RunRequest>,
    rescue: &Rc<Rescue>,
    n: u8,
) -> Box<Ticket> {
    ticket_on(runner, rx, rescue, n, queue(None, None))
}

/// Like [`ticket`], in queue `q`.
fn ticket_on(
    runner: &Runner,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<RunRequest>,
    rescue: &Rc<Rescue>,
    n: u8,
    q: QueueName,
) -> Box<Ticket> {
    runner.queue(id(n));
    let run = rx.try_recv().expect("turn just handed over");
    let done = runner.running(&run, rescue);
    Ticket::new(run, done, q, u64::from(n), None)
}

/// Who holds the VO lock of queue `q` (`GET /vqueues`).
fn lock_holder(s: &Scheduler, q: &QueueName) -> Option<String> {
    s.queues_view()
        .into_iter()
        .find(|r| r.queue == q.to_string())
        .and_then(|r| r.lock_holder)
}

fn hex(n: u8) -> String {
    id(n).iter().map(|b| format!("{b:02x}")).collect()
}

/// Shared setup of the two tests below: EXCLUSIVE queue `q` (VO lock `K`), turn cap 8, no
/// rules. W (`1`) is granted right away and holds `K`; A (`2`), B (`3`) park tickets behind
/// `K`. W finishes (`release_all`) ⇒ A is granted; the serve loop takes the granted ticket
/// (`take_ready`, like `spawn_granted`) — A's slot is now inside the task.
fn granted_behind_w() -> (
    Scheduler,
    Runner,
    tokio::sync::mpsc::UnboundedReceiver<RunRequest>,
    Rc<Rescue>,
    QueueName,
    crate::flow::Granted,
) {
    let s = Scheduler::new(8);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let q = vo(None, "k");
    let Parked::Now(tw, mut pw) = s.park(ticket_on(&runner, &mut rx, &rescue, 1, q.clone())) else {
        panic!("W must be granted right away");
    };
    for n in [2, 3] {
        assert!(matches!(
            s.park(ticket_on(&runner, &mut rx, &rescue, n, q.clone())),
            Parked::Queued
        ));
    }
    let (_, mut done) = tw.into_parts();
    done.duty().ack();
    pw.begin();
    s.release_all(pw);
    let mut ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(2)]);
    assert_eq!(lock_holder(&s, &q), Some(hex(2)));
    let g = ready.pop().expect("A's ticket");
    (s, runner, rx, rescue, q, g)
}

/// **External signal adopting a granted ticket.**
///
/// 1. W holds VO lock `K`; A, B park tickets behind `K`.
/// 2. W finishes ⇒ A's ticket is granted (`secure`: `K = A`), the serve loop spawns task
///    `run_granted(A)` (`take_ready`) — the task has not been polled yet.
/// 3. An external signal arrives for A: task S already holds A's invocation lock and asks
///    for a slot (`acquire`) — the ask withdraws A's ticket (`adopt`).
/// 4. The task of A's ticket polls for the first time: the invocation lock is busy (S holds
///    it) ⇒ `nack`.
///
/// Must see: S gets **A's ticket's** slot (there is no second slot), the nack returns
/// nothing, `K` still belongs to A, B is **not** granted. Before the fix: `adopt` did not
/// reach the slot inside the task, S was granted a second slot ("already holds the lock"),
/// the nack returned the ticket's slot by `held_before = false` ⇒ `release_all` cleared `K`
/// (compared by id) ⇒ B was granted while S's slot was still alive: two handlers on one
/// EXCLUSIVE object.
#[tokio::test]
async fn a_signal_adopting_a_granted_ticket_takes_its_permit() {
    let (s, _runner, _rx, _rescue, q, g) = granted_behind_w();
    let mut task = Box::pin(s.acquire(&q, id(2), 2));
    let p2 = futures::FutureExt::now_or_never(&mut task).expect("S granted right away");
    s.nack(g);
    assert!(s.take_ready().is_empty(), "B granted while S runs A");
    assert_eq!(lock_holder(&s, &q), Some(hex(2)), "the VO lock left A");
    assert_eq!(
        s.turns_in_use(),
        1,
        "S got a second slot instead of A's ticket's slot"
    );
    let mut p2 = p2;
    p2.begin();
    s.release_all(p2);
    // A done: only now is it B's turn.
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(3)]);
    drop(ready);
    s.close();
}

/// **The same bug without a signal** (found by the M4 model): like
/// [`a_signal_adopting_a_granted_ticket_takes_its_permit`], but the new ask is a **ticket**
/// of another background turn of A — a turn carrying a timer that the background tick handed
/// over while A's invocation lock was busy — passing the gate and `park` (withdrawing A's
/// ticket) before the task of A's ticket polls. Must see: the new turn is granted right
/// away with A's ticket's slot, the other task's nack returns nothing, `K` still belongs to
/// A, B is not granted.
#[tokio::test]
async fn a_new_ticket_adopting_a_granted_ticket_takes_its_permit() {
    let (s, runner, mut rx, rescue, q, g) = granted_behind_w();
    let Parked::Now(t2, p2) = s.park(ticket_on(&runner, &mut rx, &rescue, 2, q.clone())) else {
        panic!("A's new turn must be granted right away");
    };
    s.nack(g);
    assert!(s.take_ready().is_empty(), "B granted while A runs");
    assert_eq!(lock_holder(&s, &q), Some(hex(2)), "the VO lock left A");
    assert_eq!(s.turns_in_use(), 1, "the new turn got a second slot");
    let (_, mut done) = t2.into_parts();
    done.duty().ack();
    let mut p2 = p2;
    p2.begin();
    s.release_all(p2);
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(3)]);
    drop(ready);
    s.close();
}

/// **Closing the scheduler at the end of a term leaves no ticket behind.**
///
/// 1. Turn cap 1. Ticket `1` is granted right away (the task holds the slot, not yet
///    `begin`); tickets `2`, `3` park.
/// 2. The task of ticket `1` drops its slot (unused): the slot comes back and is granted to
///    ticket `2` — the granted ticket sits in the ready line, carrying the path that returns
///    the slot to the scheduler.
/// 3. `close`.
///
/// Must see: no ticket alive, the core has a single owner left (the `Scheduler` itself),
/// the "turn handed over" mark of all three ids removed. Must not see: returning ticket
/// `2`'s slot on close granting ticket `3`, a reference cycle scheduler → granted ticket →
/// slot → scheduler keeping the core and tickets alive past the term (each turn's tickets
/// would pile up).
#[test]
fn close_leaves_no_ticket_behind() {
    let s = Scheduler::new(1);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let Parked::Now(t1, p1) = s.park(ticket(&runner, &mut rx, &rescue, 1)) else {
        panic!("ticket 1 must be granted right away");
    };
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 2)),
        Parked::Queued
    ));
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 3)),
        Parked::Queued
    ));
    assert_eq!(s.parked(), 2);
    drop(p1);
    drop(t1);
    s.close();
    rescue.close();
    assert_eq!(s.parked(), 0, "tickets still alive after close");
    assert_eq!(
        Rc::strong_count(&s.core),
        1,
        "the core still has other owners after close"
    );
    for n in 1..=3 {
        assert!(
            !runner.is_queued(&id(n)),
            "turn-handed-over mark of {n} still there"
        );
    }
}

/// **Nack**: the turn of a granted ticket does not win the invocation lock ⇒ the slot is
/// returned, the ticket leaves for the line waiting for the lock to be released; the lock
/// is released ⇒ the ticket asks again and is granted. While it waits for the lock, its
/// slot holds nobody back.
#[test]
fn a_nacked_ticket_waits_for_the_lock_then_asks_again() {
    let s = Scheduler::new(1);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let Parked::Now(t1, p1) = s.park(ticket(&runner, &mut rx, &rescue, 1)) else {
        panic!("ticket 1 must be granted right away");
    };
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 2)),
        Parked::Queued
    ));
    drop(p1);
    drop(t1);
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(2)]);
    for g in ready {
        s.nack(g);
    }
    assert!(
        s.take_ready().is_empty(),
        "a ticket waiting for the lock is not granted again"
    );
    assert!(
        s.usage_is_empty(),
        "the nacked ticket's slot was not returned"
    );
    assert_eq!(s.parked(), 1);
    assert!(
        runner.is_queued(&id(2)),
        "a ticket waiting for the lock is still a handed-over turn"
    );
    s.lock_freed(&id(2));
    s.requeue_freed();
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(2)]);
    let mut claimed = s
        .claim(ready.into_iter().next().expect("one ticket"))
        .expect("live ticket");
    assert_eq!(s.parked(), 0);
    claimed.permit.begin();
    claimed.done.duty().ack();
    s.release_all(claimed.permit);
    assert!(s.usage_is_empty());
}

/// **One entry per invocation**: a new ask of the same invocation — a new ticket or a task
/// — withdraws the parked ticket; a withdrawn ticket leaves no safety-net mark.
#[tokio::test]
async fn a_new_request_adopts_the_parked_ticket() {
    let s = Scheduler::new(1);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let Parked::Now(_t1, p1) = s.park(ticket(&runner, &mut rx, &rescue, 1)) else {
        panic!("ticket 1 must be granted right away");
    };
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 2)),
        Parked::Queued
    ));
    // New ticket of the same id: the old ticket leaves the line, the new one takes its place.
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 2)),
        Parked::Queued
    ));
    assert_eq!(s.parked(), 1);
    assert_eq!(s.scheduler_view().waiting, 1);
    // A task asks for a slot for the same id (external signal): the ticket leaves the line,
    // the task waits in its place.
    let q = queue(None, None);
    let mut task = Box::pin(s.acquire(&q, id(2), 2));
    assert!(futures::FutureExt::now_or_never(&mut task).is_none());
    assert_eq!(s.parked(), 0);
    assert_eq!(s.scheduler_view().waiting, 1);
    assert!(
        !runner.is_queued(&id(2)),
        "a withdrawn ticket still has the handed-over mark"
    );
    assert!(
        rescue.strikes(&id(2)).is_none(),
        "a withdrawn ticket marked the safety net"
    );
    let mut p1 = p1;
    p1.begin();
    s.release_all(p1);
    let p2 = task.await;
    s.release_all({
        let mut p = p2;
        p.begin();
        p
    });
    assert!(s.usage_is_empty());
}

/// **Nack**: like [`granted_behind_w`], but the task of A's ticket polls **before** the ask
/// of whoever holds A's invocation lock (external signal, drainer, cancel) — the invocation
/// lock is busy ⇒ nack.
///
/// 1. W holds `K`; A (`seq` 2), B (`seq` 3) park tickets behind `K`.
/// 2. W finishes ⇒ A's ticket is granted (`K = A`), its task is spawned.
/// 3. The task of A's ticket polls: the invocation lock is busy ⇒ `nack`.
///
/// Must see: `K` still belongs to A (A's ticket waits for the invocation lock, then asks
/// again with the same `seq`), B is **not** granted, the slot is returned. Before the fix:
/// the nack returned the slot by `held_before = false` ⇒ `release_all` released `K` ⇒ B
/// (`seq` 3) ran before A (`seq` 2) — FIFO on one object key broken (FL-5).
#[tokio::test]
async fn a_nacked_exclusive_ticket_keeps_its_object_lock() {
    let (s, _runner, _rx, _rescue, q, g) = granted_behind_w();
    s.nack(g);
    assert!(s.take_ready().is_empty(), "B granted before A");
    assert_eq!(lock_holder(&s, &q), Some(hex(2)), "the VO lock left A");
    assert_eq!(
        s.turns_in_use(),
        0,
        "the nacked ticket's slot was not returned"
    );
    // A's invocation lock is released: A's ticket asks again, at the head of the line
    // (holds the lock).
    s.lock_freed(&id(2));
    s.requeue_freed();
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(2)]);
    drop(ready);
    s.close();
}

/// **The gate**: tickets waiting for the slot gate (FL-8) enter the core by `(queue, seq)`,
/// not by parking order — `recover` hands over by id, so a ticket with a larger `seq` may
/// park first. Must see: the gate opens ⇒ the object's smallest-`seq` ticket is granted.
/// Before the fix: the ticket parked first entered the core first and took the free VO lock.
#[tokio::test]
async fn ungate_serves_an_object_in_seq_order() {
    let s = Scheduler::new(8);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let q = vo(None, "k");
    s.close_gate();
    for n in [5, 3] {
        assert!(matches!(
            s.park(ticket_on(&runner, &mut rx, &rescue, n, q.clone())),
            Parked::Queued
        ));
    }
    s.open_gate();
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(3)]);
    drop(ready);
    s.close();
}

/// **A closed gate means the core grants nothing.** A ticket is already in the core; the
/// gate is closed (start of a term — `ungate` puts the tickets waiting at the gate into the
/// core before opening; or quiesce); a slot is returned. Must see: no ticket is granted
/// until the gate opens; the gate opens ⇒ one grant. Before the fix: the gate only blocked
/// those **entering** the core — a returned slot was granted right away to a ticket
/// already in the core, so `ungate` granted tickets one by one while still putting tickets
/// in (one `dispatch` per `enqueue`).
#[tokio::test]
async fn a_closed_gate_grants_nothing() {
    let s = Scheduler::new(1);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let Parked::Now(t1, p1) = s.park(ticket(&runner, &mut rx, &rescue, 1)) else {
        panic!("ticket 1 must be granted right away");
    };
    assert!(matches!(
        s.park(ticket(&runner, &mut rx, &rescue, 2)),
        Parked::Queued
    ));
    s.close_gate();
    drop(p1);
    drop(t1);
    assert!(
        s.take_ready().is_empty(),
        "a ticket granted while the gate is closed"
    );
    assert_eq!(s.turns_in_use(), 0);
    s.open_gate();
    let ready = s.take_ready();
    assert_eq!(ready.iter().map(|g| g.id()).collect::<Vec<_>>(), [id(2)]);
    drop(ready);
    s.close();
}

/// **Quiesce does not return slots in a cascade**: turn cap 1, ticket `1` runs, tickets
/// `2`…`6` park. Quiesce closes the gate, then returns the slots of tickets that were
/// granted but have not become turns (`retire_ready`). Must see: turn `1` releases its slot
/// and no ticket is granted — five tickets stay parked (the term is about to end, `close`
/// drops them, the next term's `recover` hands them over again). Before the fix: each
/// returned slot was granted to the next ticket, `retire_ready` returned it, granted again…
/// — going through **every** ticket, synchronously, on the shared engine thread, each round
/// charging 1 CU.
#[tokio::test]
async fn quiesce_does_not_cascade_grants() {
    let s = Scheduler::new(1);
    let (runner, mut rx) = Runner::new();
    let rescue = Rescue::new();
    let Parked::Now(t1, mut p1) = s.park(ticket(&runner, &mut rx, &rescue, 1)) else {
        panic!("ticket 1 must be granted right away");
    };
    for n in 2..=6 {
        assert!(matches!(
            s.park(ticket(&runner, &mut rx, &rescue, n)),
            Parked::Queued
        ));
    }
    s.close_gate();
    s.retire_ready();
    let (_, mut done) = t1.into_parts();
    done.duty().ack();
    p1.begin();
    s.release_all(p1);
    s.retire_ready();
    assert_eq!(
        s.parked(),
        5,
        "returning slots during quiesce granted in a cascade"
    );
    assert_eq!(s.turns_in_use(), 0);
    s.close();
}
