// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **The slot wait line is data**: parked tickets of background turns in the scheduler's
//! line.
//!
//! Before parked tickets, each background turn waiting for a slot was a task holding the
//! invocation lock (~8.4 KB, FL-28); blocked on budget, it gave up after one `queue_wait`
//! slice and was rescheduled (`Event::Throttled`), the background tick fired the
//! rescheduled row, a new task — each waiter came back to the engine ~0.3 times/s
//! (measured: W = 2·10⁴ waiters ⇒ 6 000 engine holds/s, drain throughput 98 → 33 turns/s).
//!
//! Now a background turn that passes the gate (`turn::drive_dispatched`: state, place in
//! the queue, asking for a slot — one engine hold) and is not granted right away **parks a
//! ticket** ([`Ticket`]) in the [`Core`]'s line and the task ends, releasing the invocation
//! lock. The ticket carries the turn's "turn handed over" mark (`Runner::queued`, FL-20)
//! and its wake-up duty (`WakeDuty`, FL-16). Whatever it is blocked on (VO lock, counter,
//! rate, suspended tenant, turn cap), the ticket just sits there: no slices, no
//! rescheduling, no engine hold at all until it is granted.
//!
//! Lifecycle of a ticket:
//!
//! 1. **Park** ([`Scheduler::park`]): first withdraw the same invocation's old ticket
//!    ([`Scheduler::adopt`] — one entry per invocation in the line; M4
//!    `adopt_keeps_ticket`), then ask for a slot. Granted right away ⇒ the task carries on
//!    with the turn (the ticket never left the task). Slot gate closed (FL-8) ⇒ the ticket
//!    waits at the gate and enters the core when the gate opens.
//! 2. **Grant** ([`Scheduler::deliver`]): ticket + slot stay **in the book** (`handed`),
//!    the serve loop receives a handle ([`Granted`]) and spawns the turn
//!    (`actor::run_granted`). The slot does not leave the book until the turn wins the
//!    invocation lock: a new ask of the same invocation in that gap **takes this very slot**
//!    ([`Scheduler::adopt`]) — there is never a second slot (M4 `ONE_PERMIT`).
//! 3. **Win the invocation lock** on that task's first poll, synchronously
//!    ([`Scheduler::claim`]). Busy ⇒ **nack** ([`Scheduler::nack`]): return the slot (keep
//!    the VO lock if it was held before the grant), the ticket waits for the lock to be
//!    released (`Locks` nudges [`Scheduler::lock_freed`]), then asks again with the same
//!    `seq`. No task waits for the invocation lock while holding a slot — the lock order
//!    "invocation lock first, slot second" holds as on every other path. If the ticket was
//!    withdrawn the handle is empty: no slot to return.
//! 4. The turn continues from the gate after the slot (`Event::PermitGranted`): an
//!    invocation that finished or was cancelled while the ticket was parked does not run
//!    (M4 `parked_grant_skips_gate`).
//!
//! Tickets are the term's RAM: lost with the scheduler when stepping down — the invocation
//! is still durable and in the durable queue, and the next term's `recover` hands it over
//! again (INV-3).

use super::*;

/// Ticket of a background turn in the scheduler's line (see the module head).
pub struct Ticket {
    /// The turn `Runner` handed over (the reschedule set beforehand, timers not yet delivered).
    pub run: crate::drainer::RunRequest,
    /// The turn's "turn handed over" mark and wake-up duty.
    done: crate::drainer::Dispatched,
    queue: QueueName,
    seq: u64,
    cool: Option<CoolKey>,
    /// Number of the parking — once the ticket is withdrawn ([`Scheduler::adopt`]) the
    /// number is no longer alive.
    token: u64,
    parked_at: Instant,
    _mark: Option<TicketMark>,
}

impl Ticket {
    /// Ticket for turn `run` at position `(queue, seq)` of the durable queue.
    pub fn new(
        run: crate::drainer::RunRequest,
        done: crate::drainer::Dispatched,
        queue: QueueName,
        seq: u64,
        cool: Option<CoolKey>,
    ) -> Box<Self> {
        Box::new(Self {
            run,
            done,
            queue,
            seq,
            cool,
            token: 0,
            parked_at: Instant::now(),
            _mark: None,
        })
    }

    /// The turn and its guard — the ticket leaves the scheduler.
    pub fn into_parts(self) -> (crate::drainer::RunRequest, crate::drainer::Dispatched) {
        (self.run, self.done)
    }

    /// A ticket dropped because another turn takes over the wake-up (withdrawn, old ticket):
    /// no safety-net mark.
    fn retire(mut self: Box<Self>) {
        self.done.duty().ack();
    }
}

/// An asker in the [`Core`]'s line: a task waiting on a channel (client turn, `resume`,
/// external signal) or a parked ticket.
pub(crate) enum Waiter {
    Task(oneshot::Sender<Permit>),
    Ticket(Box<Ticket>),
}

/// Outcome of [`Scheduler::park`].
#[allow(
    clippy::large_enum_variant,
    reason = "returned once per background turn and used right away; never sits in a line"
)]
pub enum Parked {
    /// Granted right away: the task carries on with the turn, the ticket never left it.
    Now(Box<Ticket>, Permit),
    /// The ticket sits in the line; the task ends.
    Queued,
}

/// A ticket granted a slot whose turn has not yet won the invocation lock: ticket and slot
/// sit in the book ([`Tickets::handed`]) until [`Scheduler::claim`], [`Scheduler::nack`]
/// or [`Scheduler::adopt`].
struct Handed {
    ticket: Box<Ticket>,
    permit: Permit,
}

/// **Handle** of a ticket just granted a slot, for the turn task
/// ([`Scheduler::take_ready`] → `actor::run_granted`). Carries no slot: the slot stays in
/// the book, so a new ask of the same invocation can take it while the task has not been
/// polled yet. Dropped unused (the task cancelled before its first poll) ⇒ the slot is
/// returned, the ticket dropped — the wake-up duty falls unacked (FL-16), as before the
/// slot stayed in the book.
pub struct Granted {
    sched: Scheduler,
    id: InvocationId,
    token: u64,
    used: bool,
}

impl Granted {
    pub fn id(&self) -> InvocationId {
        self.id
    }
}

impl Drop for Granted {
    fn drop(&mut self) {
        if self.used {
            return;
        }
        if let Some(h) = self.sched.take_handed(&self.id, self.token) {
            drop(h.permit);
            drop(h.ticket);
        }
    }
}

/// The turn of a ticket that has won the invocation lock ([`Scheduler::claim`]).
pub struct Claimed {
    pub run: crate::drainer::RunRequest,
    pub done: crate::drainer::Dispatched,
    pub permit: Permit,
    /// From parking to grant.
    pub waited: Duration,
}

/// A live ticket ([`Scheduler::parked`] of the range, gauge `cairnflow_flow_parked` of the
/// node) — released when the ticket leaves the scheduler.
struct TicketMark {
    count: Rc<std::cell::Cell<usize>>,
}

impl Drop for TicketMark {
    fn drop(&mut self) {
        self.count.set(self.count.get().saturating_sub(1));
        crate::metrics::flow::PARKED.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// An invocation's live ticket: the parking number, and its place in the core (queue,
/// request number) while it sits in the line.
struct Live {
    token: u64,
    at: Option<(QueueKey, u64)>,
}

/// The scheduler's ticket book.
#[derive(Default)]
pub(super) struct Tickets {
    /// One live ticket per invocation, from parking until its turn wins the invocation lock
    /// ([`Scheduler::claim`]) or it is withdrawn ([`Scheduler::adopt`]).
    live: HashMap<InvocationId, Live>,
    next: u64,
    /// Tickets waiting for the slot gate to open (FL-8).
    #[allow(
        clippy::vec_box,
        reason = "tickets move between lines as a `Box` — move the box, do not copy"
    )]
    gated: Vec<Box<Ticket>>,
    /// Granted tickets (in grant order) waiting for the serve loop to spawn the turn: id and
    /// parking number. Ticket + slot are in [`Self::handed`].
    ready: VecDeque<(InvocationId, u64)>,
    /// Granted tickets whose turn has not yet won the invocation lock — **the slot is
    /// here**, not in the task: [`Scheduler::adopt`] can take it.
    handed: HashMap<InvocationId, Handed>,
    /// Nacked tickets (invocation lock busy at grant time), waiting for the lock to be released.
    deferred: HashMap<InvocationId, Box<Ticket>>,
    /// Live tickets of the range ([`TicketMark`]).
    count: Rc<std::cell::Cell<usize>>,
}

impl Tickets {
    fn is_current(&self, id: &InvocationId, token: u64) -> bool {
        self.live.get(id).is_some_and(|l| l.token == token)
    }
}

impl Scheduler {
    /// Park the ticket of background turn `ticket` (see the module head): withdraw the
    /// same invocation's old ticket, then ask for a slot. Called while holding the engine
    /// and the invocation lock, right after the gate before the slot — the ask and an
    /// immediate grant are synchronous.
    pub fn park(&self, mut ticket: Box<Ticket>) -> Parked {
        let id = ticket.run.id;
        if let Some(permit) = self.adopt(&id) {
            // The old ticket was granted but has not become a turn: this turn runs on that
            // very slot — no further ask.
            return Parked::Now(ticket, permit);
        }
        {
            let mut tk = self.tickets.borrow_mut();
            tk.next += 1;
            ticket.token = tk.next;
            tk.live.insert(
                id,
                Live {
                    token: ticket.token,
                    at: None,
                },
            );
            tk.count.set(tk.count.get() + 1);
            ticket._mark = Some(TicketMark {
                count: Rc::clone(&tk.count),
            });
            crate::metrics::flow::PARKED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        ticket.parked_at = Instant::now();
        if !self.gate_open() {
            self.tickets.borrow_mut().gated.push(ticket);
            return Parked::Queued;
        }
        self.sync_tenants();
        match self.enqueue(ticket, true) {
            Some((mut ticket, permit)) => {
                // Granted right away: the ticket never parked — no longer a waiter.
                self.tickets.borrow_mut().live.remove(&id);
                ticket._mark = None;
                Parked::Now(ticket, permit)
            }
            None => Parked::Queued,
        }
    }

    /// Put the ticket into the core. `mine`: an immediate grant to this very ticket does
    /// not enter the ready line but is returned to the caller.
    fn enqueue(&self, ticket: Box<Ticket>, mine: bool) -> Option<(Box<Ticket>, Permit)> {
        let (id, token, seq) = (ticket.run.id, ticket.token, ticket.seq);
        let queue = ticket.queue.clone();
        let cool = ticket.cool.clone();
        let mut st = self.core.borrow_mut();
        let (asked, mut acts) = st.request(
            &queue,
            id,
            seq,
            cool,
            Waiter::Ticket(ticket),
            Instant::now(),
        );
        let now = mine
            .then(|| {
                acts.grants.iter().position(
                    |g| matches!(&g.to, Waiter::Ticket(t) if t.run.id == id && t.token == token),
                )
            })
            .flatten()
            .map(|i| acts.grants.remove(i));
        if now.is_none() {
            // Before `deliver`: a grant within this very ask (a ticket that is not the
            // caller's) removes that place right after.
            if let Some(l) = self.tickets.borrow_mut().live.get_mut(&id) {
                if l.token == token {
                    l.at = Some((queue.encode(), asked.req));
                }
            }
        }
        self.deliver(&mut st, acts);
        drop(st);
        let g = now?;
        let Waiter::Ticket(ticket) = g.to else {
            unreachable!("just picked exactly the ticket's grant")
        };
        let mut permit = g.permit;
        permit.unstarted = Some(GiveBack {
            sched: self.clone(),
            held_lock: g.held_before,
        });
        crate::metrics::flow::bump(&crate::metrics::flow::GRANTED);
        Some((ticket, permit))
    }

    /// A grant to a ticket (from [`Self::deliver`], with the core borrowed): the permit
    /// carries the path that returns an unused slot, the ticket enters the ready line, the
    /// serve loop is notified.
    pub(super) fn granted_ticket(
        &self,
        ticket: Box<Ticket>,
        mut permit: Permit,
        held_before: bool,
    ) {
        permit.unstarted = Some(GiveBack {
            sched: self.clone(),
            held_lock: held_before,
        });
        crate::metrics::flow::bump(&crate::metrics::flow::GRANTED);
        crate::metrics::flow::add(
            &crate::metrics::flow::WAIT_MS_TOTAL,
            ticket.parked_at.elapsed().as_millis() as u64,
        );
        let mut tk = self.tickets.borrow_mut();
        let (id, token) = (ticket.run.id, ticket.token);
        if let Some(l) = tk.live.get_mut(&id) {
            if l.token == token {
                l.at = None;
            }
        }
        debug_assert!(
            !tk.handed.contains_key(&id),
            "two granted tickets for one invocation"
        );
        tk.handed.insert(id, Handed { ticket, permit });
        tk.ready.push_back((id, token));
        drop(tk);
        self.ticket_wake.notify_one();
    }

    /// Take `id`'s granted ticket out of the book if it is still the live ticket number `token`.
    fn take_handed(&self, id: &InvocationId, token: u64) -> Option<Handed> {
        let mut tk = self.tickets.borrow_mut();
        if !tk.is_current(id, token) {
            return None;
        }
        tk.live.remove(id);
        tk.handed.remove(id)
    }

    /// **One entry per invocation in the line**: a new slot ask for `id` — a background
    /// turn that just passed the gate, a client turn, `resume`, an external signal —
    /// withdraws its live ticket, wherever it is (in the core, waiting at the gate, waiting
    /// for the lock, granted but not yet holding the lock). The new turn takes over the
    /// wake-up; a withdrawn ticket leaves no safety-net mark. Without the withdrawal, the
    /// turn carrying the wake-up runs before the ticket, and the ticket — the reschedule set
    /// beforehand — counts the attempt again at the position the other turn already counted
    /// (M4 `adopt_keeps_ticket`, `ATTEMPTS`).
    ///
    /// A ticket **already granted** ⇒ return its slot (the unused-slot return path by
    /// `held_before` at grant time): the withdrawer runs on that very slot and does not ask
    /// for a new one. Asking for another would mean two live slots for one invocation, and
    /// the ticket's slot return — `release_all` compares the VO lock by **id** — would
    /// release the lock the other slot is using (M4 `granted_out_of_ledger`,
    /// `VO_EXCLUSIVE`).
    pub(super) fn adopt(&self, id: &InvocationId) -> Option<Permit> {
        let (at, mut retired, handed) = {
            let mut tk = self.tickets.borrow_mut();
            let live = tk.live.remove(id)?;
            let mut retired: Vec<Box<Ticket>> = tk.deferred.remove(id).into_iter().collect();
            let gated = std::mem::take(&mut tk.gated);
            for t in gated {
                if t.run.id == *id {
                    retired.push(t);
                } else {
                    tk.gated.push(t);
                }
            }
            let handed = tk.handed.remove(id);
            if handed.is_some() {
                tk.ready.retain(|(r, _)| r != id);
            }
            (live.at, retired, handed)
        };
        if let Some((key, req)) = at {
            let mut st = self.core.borrow_mut();
            let (to, acts) = st.withdraw(&key, req, Instant::now());
            self.deliver(&mut st, acts);
            drop(st);
            if let Some(Waiter::Ticket(t)) = to {
                retired.push(t);
            }
        }
        for t in retired {
            t.retire();
        }
        let Handed { ticket, permit } = handed?;
        ticket.retire();
        Some(permit)
    }

    /// The turn of ticket `g` has just won the invocation lock: the ticket leaves the
    /// scheduler. `None` = the ticket was withdrawn while waiting to be spawned (another
    /// turn took over): the slot is returned, the task ends.
    pub fn claim(&self, mut g: Granted) -> Option<Claimed> {
        g.used = true;
        // The ticket was withdrawn: the withdrawer took the slot and removed the ticket —
        // nothing left here.
        let Handed { ticket, permit } = self.take_handed(&g.id, g.token)?;
        let waited = ticket.parked_at.elapsed();
        let (run, done) = ticket.into_parts();
        Some(Claimed {
            run,
            done,
            permit,
            waited,
        })
    }

    /// **Nack**: the turn of ticket `g` did not win the invocation lock (another task — the
    /// drainer, a cancel, a turn with a caller — holds it). Return the unused slot — **keeping
    /// the VO lock** of an EXCLUSIVE queue (`Core::nack`) —, the ticket waits for the lock to
    /// be released ([`Self::lock_freed`]) and then asks again with the same `seq`. No task
    /// holds a slot while waiting for the invocation lock. A withdrawn ticket (the
    /// withdrawer already took its slot) has nothing left to return.
    pub fn nack(&self, mut g: Granted) {
        g.used = true;
        let handed = {
            let mut tk = self.tickets.borrow_mut();
            if !tk.is_current(&g.id, g.token) {
                return;
            }
            tk.handed.remove(&g.id)
        };
        let Some(Handed { ticket, permit }) = handed else {
            return;
        };
        // Return the slot while the book is not borrowed: the return may grant another ticket.
        let mut permit = permit;
        let held_before = permit.unstarted.take().is_some_and(|b| b.held_lock);
        {
            let mut st = self.core.borrow_mut();
            let acts = st.nack(&mut permit, held_before, Instant::now());
            self.deliver(&mut st, acts);
        }
        self.tickets
            .borrow_mut()
            .deferred
            .insert(ticket.run.id, ticket);
    }

    /// `id`'s invocation lock was just released after a missed claim (`Locks`): the ticket
    /// waiting for that lock (if any) asks again on the serve loop's next round.
    pub fn lock_freed(&self, id: &InvocationId) {
        if let Ok(mut f) = self.freed.try_borrow_mut() {
            f.push(*id);
        }
        self.ticket_wake.notify_one();
    }

    /// A ticket was granted or a lock was just released: the serve loop calls
    /// [`Self::requeue_freed`] and [`Self::take_ready`].
    pub async fn tickets_changed(&self) {
        self.ticket_wake.notified().await;
    }

    /// Tickets waiting for a just-released lock ask for a slot again.
    pub fn requeue_freed(&self) {
        let ids = std::mem::take(&mut *self.freed.borrow_mut());
        for id in ids {
            let Some(t) = self.tickets.borrow_mut().deferred.remove(&id) else {
                continue;
            };
            self.enqueue(t, false);
        }
    }

    /// **Quiesce** (the serve loop spawns no more new turns): return the slots of every
    /// ticket granted but not yet spawned and drop the tickets — the invocation is durable
    /// in the durable queue, the next leader hands it over again (like a task waiting for a
    /// slot being cancelled, before parked tickets). A returned slot may be granted on to
    /// another ticket: repeat until none are left. Otherwise the turn cap gets stuck on
    /// slots nobody runs and quiesce waits out its full cap.
    pub fn retire_ready(&self) {
        loop {
            let ready: Vec<(InvocationId, u64)> =
                self.tickets.borrow_mut().ready.drain(..).collect();
            if ready.is_empty() {
                return;
            }
            for (id, token) in ready {
                if let Some(Handed { ticket, permit }) = self.take_handed(&id, token) {
                    drop(permit);
                    ticket.retire();
                }
            }
        }
    }

    /// Handles of the tickets just granted, in grant order.
    pub fn take_ready(&self) -> Vec<Granted> {
        let ready: Vec<(InvocationId, u64)> = self.tickets.borrow_mut().ready.drain(..).collect();
        ready
            .into_iter()
            .map(|(id, token)| Granted {
                sched: self.clone(),
                id,
                token,
                used: false,
            })
            .collect()
    }

    /// The slot gate just opened: tickets waiting at the gate enter the core.
    pub(super) fn ungate(&self) {
        let mut gated = std::mem::take(&mut self.tickets.borrow_mut().gated);
        // By `(queue, seq)`, not by parking order (`recover` hands over by id): each entry
        // into the core is a `dispatch`, so the ticket arriving first would take the free VO
        // lock even though a smaller-`seq` ticket of the same object waits right behind it.
        gated.sort_by_cached_key(|t| (t.queue.encode(), t.seq));
        for t in gated {
            self.enqueue(t, false);
        }
    }

    /// Drop every ticket outside the core (waiting at the gate, granted, waiting for the
    /// lock) — at the start of a term and when stepping down ([`Self::close`]). Granted but
    /// unused slots are returned before the core is reset. Tickets in the core go with
    /// `Core::reset`.
    pub(super) fn drop_tickets(&self) {
        let (gated, handed, deferred) = {
            let mut tk = self.tickets.borrow_mut();
            tk.live.clear();
            tk.ready.clear();
            (
                std::mem::take(&mut tk.gated),
                std::mem::take(&mut tk.handed),
                std::mem::take(&mut tk.deferred),
            )
        };
        self.freed.borrow_mut().clear();
        drop(handed);
        drop(gated);
        drop(deferred);
    }

    /// The term ends: drop **every** ticket — in the core and outside — and every unused
    /// slot. Called after every task of the term has stopped. A granted ticket holds a copy
    /// of the scheduler in its slot return path; returning its slot could in turn grant the
    /// next ticket in the core — so drain the core's line first (nobody left to grant), then
    /// drop the tickets outside the core. Otherwise the reference cycle scheduler → granted
    /// ticket → slot → scheduler keeps the core and all its tickets alive past the term (a
    /// leak on every leader change). Tickets dropped here do not mark the safety net (the
    /// net is closed): the next term's `recover` hands the invocation over again (INV-3).
    pub fn close(&self) {
        let queues = {
            let mut st = self.core.borrow_mut();
            st.ring.clear();
            st.in_ring.clear();
            std::mem::take(&mut st.queues)
        };
        drop(queues);
        self.drop_tickets();
        debug_assert!(
            self.tickets.borrow().handed.is_empty(),
            "returning slots on close must not grant any more tickets"
        );
    }

    /// `id` has a live ticket — in the core, waiting at the gate, granted but not yet
    /// holding the invocation lock, or waiting for the lock (`Attendance::parked` of the
    /// retry tick).
    pub fn is_parked(&self, id: &InvocationId) -> bool {
        self.tickets.borrow().live.contains_key(id)
    }

    /// Number of live tickets of the range.
    pub fn parked(&self) -> usize {
        self.tickets.borrow().count.get()
    }
}
