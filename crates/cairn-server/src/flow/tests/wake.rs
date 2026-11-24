// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tests: releasing a slot is bounded work and wakes the right one.

use super::*;

/// Number of times the scheduler has asked `poll_acquire` so far (granted + blocked).
fn polls(s: &Scheduler) -> u64 {
    let v = s.scheduler_view();
    v.granted + v.blocked.iter().map(|(_, n)| n).sum::<u64>()
}

/// **Releasing a slot is O(1) work, not O(number of waiting queues).**
///
/// 100k live invocations, each its own EXCLUSIVE queue, all waiting on the turn cap. On
/// every slot release the old version woke **every** queue waiting on `Turns`, asked each
/// again, then queued them up to wait again — one core at 100%, ~10⁹ "blocked" results
/// after 11 minutes. A slot that has just freed up can only be granted to **one** waiter:
/// waking exactly the head of the line (and the next one if that one turns out to be blocked
/// elsewhere) is enough.
#[tokio::test]
async fn releasing_a_turn_does_bounded_work_with_20k_waiting() {
    const N: usize = 20_000;
    let s = Scheduler::new(1);
    let holder = s.acquire(&vo(None, "running"), [0xFF; 24], 0).await;

    let queues: Vec<_> = (0..N).map(|i| vo(None, &format!("k{i}"))).collect();
    // `ManuallyDrop`: when the test is red, the old version cancels the 20k waiters in a
    // cascade (each slot not yet taken wakes all 20k again) — O(n²), tens of minutes. Leaking
    // a few futures in a test is fine.
    let mut waiting = std::mem::ManuallyDrop::new(Vec::with_capacity(N));
    waiting.extend(queues.iter().enumerate().map(|(i, q)| {
        let mut b = [0u8; 24];
        b[..8].copy_from_slice(&(i as u64).to_be_bytes());
        Box::pin(s.acquire(q, b, i as u64 + 1))
    }));
    // One poll each is enough to put it in the `Turns` wait line. Poll by hand with an empty
    // waker: `timeout(ZERO)` waits one clock tick each time.
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for f in waiting.iter_mut() {
        assert!(f.as_mut().poll(&mut cx).is_pending());
    }

    let before = polls(&s);
    s.release_running(holder);
    let used = polls(&s) - before;
    assert!(
        used <= 4,
        "releasing one slot re-asked {used} queues ({N} waiting) — must be a few"
    );

    // Exactly the head of the line gets the slot, and the next release goes to the second.
    let first = tokio::time::timeout(Duration::from_millis(200), waiting.remove(0))
        .await
        .expect("the first waiter must get the freed slot");
    let before = polls(&s);
    s.release_running(first);
    assert!(polls(&s) - before <= 4);
    let second = tokio::time::timeout(Duration::from_millis(200), waiting.remove(0))
        .await
        .expect("the wait order must be preserved");
    s.release_running(second);
}

/// A waiter for a VO lock does not eat the slot of a waiter for a slot: the free slot goes to
/// the one that can run, the lock waiter keeps waiting for the lock.
#[tokio::test]
async fn woken_waiter_blocked_elsewhere_passes_the_turn_on() {
    let s = Scheduler::new(1);
    let locked = vo(None, "owned");
    // The lock holder is suspended: holds the lock, holds no slot.
    let owner = s.acquire(&locked, id(1), 1).await;
    s.release_running(owner);
    // Someone else holds the only slot.
    let holder = s.acquire(&vo(None, "other"), id(2), 2).await;
    // First waiter: same object as the lock holder ⇒ will be blocked on the lock. Later
    // waiter: a free object.
    let mut blocked = Box::pin(s.acquire(&locked, id(3), 3));
    let free_q = vo(None, "idle");
    let mut free = Box::pin(s.acquire(&free_q, id(4), 4));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut blocked)
            .await
            .is_err()
    );
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut free)
        .await
        .is_err());

    s.release_running(holder);
    let p = tokio::time::timeout(Duration::from_millis(200), free)
        .await
        .expect("the free slot must go to the waiter not blocked elsewhere");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut blocked)
            .await
            .is_err(),
        "the lock waiter must keep waiting for the lock"
    );
    s.release_all(p);
    s.release_lock(locked.lock_name().unwrap(), id(1));
    let p = tokio::time::timeout(Duration::from_millis(200), blocked)
        .await
        .expect("once the lock is free the lock waiter runs");
    s.release_all(p);
}

/// The waiter at the head of the line leaves (task cancelled — wait cap hit in `turn`, or
/// `JoinSet::shutdown`) while waiting for a slot. The freed slot must go to the **next**
/// waiter: waking "exactly one" who is no longer there leaves the slot idle while someone is
/// still waiting for it — a stall with no error.
#[tokio::test]
async fn turn_passes_on_when_the_woken_waiter_is_gone() {
    let s = Scheduler::new(1);
    let holder = s.acquire(&vo(None, "running"), id(1), 1).await;
    let (qa, qb) = (vo(None, "dropped"), vo(None, "stays"));
    let mut gone = Box::pin(s.acquire(&qa, id(2), 2));
    let mut stays = Box::pin(s.acquire(&qb, id(3), 3));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(gone.as_mut().poll(&mut cx).is_pending());
    assert!(stays.as_mut().poll(&mut cx).is_pending());
    drop(gone);

    s.release_running(holder);
    let p = tokio::time::timeout(Duration::from_millis(200), stays)
        .await
        .expect("the free slot must go to the waiter who stayed");
    s.release_all(p);
    assert!(s.usage_is_empty());
}

/// **Flow sim, seed 1.** A queue with no waiters left must leave the scheduler's table. The
/// old version only dropped it in the "empty head of line" branch of `serve_queue`, but with
/// quantum 1, after the grant to the last one the function returns in the `deficit == 0`
/// branch before reaching it — every queue that ever ran (each object key of a VO) stayed
/// until the end of the term: memory leaking by the number of objects ever touched, and
/// `/vqueues`, `set_rules` walking all of them.
#[tokio::test]
async fn queue_emptied_by_a_grant_leaves_the_table() {
    let s = Scheduler::new(4);
    for i in 0..3u8 {
        let p = s.acquire(&vo(None, &format!("k{i}")), id(i), 1).await;
        s.release_all(p);
    }
    assert_eq!(
        s.scheduler_view().queues,
        0,
        "a finished queue is still in the table"
    );
    assert!(s.queues_view().is_empty());
}

/// **Flow sim, the same bug via the leave path.** 33 waiters for one object's VO lock all
/// leave: the dead-entry sweep (`> 32`) empties the queue, and the queue — with its entry in
/// the lock's wait list — must go too. The old version left an empty queue that nobody
/// could wake to clean up.
#[tokio::test]
async fn queue_emptied_by_cancellations_leaves_the_table() {
    let s = Scheduler::new(64);
    let q = vo(None, "hot");
    s.restore_lock(q.lock_name().unwrap(), id(250));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut waiting: Vec<_> = (0..33u8)
        .map(|i| Box::pin(s.acquire(&q, id(i), u64::from(i) + 1)))
        .collect();
    for f in waiting.iter_mut() {
        assert!(f.as_mut().poll(&mut cx).is_pending());
    }
    drop(waiting);
    assert_eq!(
        s.scheduler_view().queues,
        0,
        "a queue with only dead entries stayed"
    );
    s.release_lock(q.lock_name().unwrap(), id(250));
    assert_eq!(s.scheduler_view().queues, 0);
    let p = s.acquire(&q, id(40), 40).await;
    s.release_all(p);
    assert!(s.usage_is_empty());
}

/// Raising the cap by k slots ⇒ exactly k waiters run; releasing more lets the rest
/// run.
#[tokio::test]
async fn raising_the_ceiling_wakes_as_many_as_it_frees() {
    let s = Scheduler::new(1);
    let holder = s.acquire(&vo(None, "running"), id(1), 1).await;
    let qs: Vec<_> = (0..5).map(|i| vo(None, &format!("k{i}"))).collect();
    let mut fs: Vec<_> = qs
        .iter()
        .enumerate()
        .map(|(i, q)| Box::pin(s.acquire(q, id(10 + i as u8), 10 + i as u64)))
        .collect();
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for f in fs.iter_mut() {
        assert!(f.as_mut().poll(&mut cx).is_pending());
    }
    s.set_max_turns(4);
    assert_eq!(s.turns_in_use(), 4);
    let mut got = Vec::new();
    for f in fs.drain(..3) {
        got.push(
            tokio::time::timeout(Duration::from_millis(200), f)
                .await
                .expect("three new slots go to the first three waiters"),
        );
    }
    for p in got {
        s.release_all(p);
    }
    for f in fs.drain(..) {
        let p = tokio::time::timeout(Duration::from_millis(200), f)
            .await
            .expect("the remaining waiters run");
        s.release_all(p);
    }
    s.release_all(holder);
    assert!(s.usage_is_empty());
}
