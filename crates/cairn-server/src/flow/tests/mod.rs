// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tests of flow control.
use {
    super::*,
    cairn_store::{Limit, LimitKey},
    std::{future::Future, time::Duration},
};

mod rate;
mod ticket;
mod wake;

fn sc(s: &str) -> Scope {
    Scope::new(s).unwrap()
}

fn queue(scope: Option<&str>, limit: Option<&str>) -> QueueName {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: scope.map(sc),
        limit: limit.map(|l| LimitKey::parse(l).unwrap()),
        exclusive: false,
        service: "Svc".into(),
        object_key: String::new(),
    }
}

fn vo(scope: Option<&str>, key: &str) -> QueueName {
    QueueName {
        tenant: cairn_store::TenantId::default(),
        scope: scope.map(sc),
        limit: None,
        exclusive: true,
        service: "Cart".into(),
        object_key: key.into(),
    }
}

fn id(n: u8) -> InvocationId {
    [n; 24]
}

fn book(rules: &[(&str, u32)]) -> RuleBook {
    let mut b = RuleBook::default();
    for (p, n) in rules {
        b.set(p, Some(Limit::Max(*n)), None, None, 0).unwrap();
    }
    b.version = 1;
    b
}

/// **FL-14, `cooldown`.** Two turns of the same identity in `cooldown`, in two different
/// queues: the later turn waits at the `cooldown` tier (taking no turn slot) until the
/// earlier one releases; a turn without that identity goes straight through. Once
/// everything is released no count is left (FL-3).
#[tokio::test]
async fn cooldown_one_turn_per_identity_on_range() {
    let s = Scheduler::new(8);
    let k = CoolKey {
        tenant: "default".into(),
        service: "Svc".into(),
        handler: "boom".into(),
        deployment: "dp_1".into(),
    };
    let (qa, qb, qc) = (
        queue(Some("a"), None),
        queue(Some("b"), None),
        queue(Some("c"), None),
    );
    let first = s.acquire_cooled(&qa, id(1), 1, Some(k.clone())).await;
    let mut second = Box::pin(s.acquire_cooled(&qb, id(2), 2, Some(k.clone())));
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut second)
        .await
        .is_err());
    assert_eq!(s.blocked_on(&qb), Some(ResourceKind::Cooldown(k.clone())));
    assert!(
        !ResourceKind::Cooldown(k.clone()).is_budget(),
        "cooldown is not out of budget"
    );
    assert_eq!(
        s.turns_in_use(),
        1,
        "a turn waiting on cooldown holds no slot"
    );
    let other = s.acquire(&qc, id(3), 3).await;
    s.release_running(first);
    let second = tokio::time::timeout(Duration::from_millis(100), second)
        .await
        .expect("releasing the earlier turn grants the later one");
    s.release_all(second);
    s.release_all(other);
    assert!(s.usage_is_empty());
}

/// An asker that **leaves** (its future cancelled — wait slice over, leadership stopping)
/// must not leave a dead entry in the queue's wait line.
///
/// On a real cluster (a billing workload), a hot object had ~6 000 waiting commands, each
/// task leaving and asking again every `queue_wait`: one queue's wait line grew to
/// **300 000** entries, nearly all dead entries only cleared when they drifted to the
/// head. Inserting by `seq` did a linear scan + `memmove` on that line, on **the range's
/// engine thread** — every commit of the whole range (cold keys included) waited 11–15 s.
#[tokio::test]
async fn abandoned_request_leaves_no_dead_entry() {
    let s = Scheduler::new(8);
    let q = vo(None, "hot");
    let holder = s.acquire(&q, id(0), 0).await;
    // One **live** waiter at the head of the line (smallest seq) — exactly the shape on the
    // cluster: the dead entries behind it never drift to the head to be cleared.
    let mut first = Box::pin(s.acquire(&q, id(1), 1));
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut first)
        .await
        .is_err());

    // 50 lock waiters, each leaving and asking again 20 times.
    for _round in 0..20 {
        for n in 2..=51u8 {
            let fut = s.acquire(&q, id(n), u64::from(n));
            assert!(
                tokio::time::timeout(Duration::from_millis(1), fut)
                    .await
                    .is_err(),
                "the lock is held"
            );
        }
    }
    // Alive: nobody. The wait line must track the real number of waiters, not swell with
    // the number of departures (buggy version: 1 000).
    let waiting = s.scheduler_view().waiting;
    assert!(
        waiting <= 64,
        "the wait line keeps {waiting} dead entries after 1 000 departures"
    );

    s.release_all(holder);
    let p1 = tokio::time::timeout(Duration::from_millis(200), first)
        .await
        .expect("the one at the head is called when the lock is released");
    s.release_all(p1);
    assert!(s.usage_is_empty());
}

/// An asker **holding the lock** that asks for a slot again must be served before those
/// ahead of it in the line.
///
/// H holds an object's lock and then suspends (an infrastructure-failed turn: releases the
/// slot, keeps the lock). X has a smaller `seq` — it joined the line before H, but had no
/// task when H was granted — and now waits at the head, blocked by H's lock. H wakes up,
/// asks for a slot, and is inserted by `seq` **after** X: the scheduler only looks at the
/// head, X waits for H, H waits for X. On a billing workload's cluster: 7 870 commands of
/// one object stood still forever.
#[tokio::test]
async fn lock_holder_reacquiring_permit_is_served_first() {
    let s = Scheduler::new(8);
    let q = vo(None, "hot");
    // H (seq 5) is granted while nobody waits, then suspends: keeps the lock.
    let h = s.acquire(&q, id(5), 5).await;
    s.release_running(h);
    // X (seq 1) gets a task afterwards: at the head of the line, blocked by H's lock.
    let mut x = Box::pin(s.acquire(&q, id(1), 1));
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut x)
        .await
        .is_err());
    // H wakes up: must get a slot, not wait for X.
    let h = tokio::time::timeout(Duration::from_millis(200), s.acquire(&q, id(5), 5))
        .await
        .expect("the lock holder queued behind a waiter for that very lock — deadlock");
    s.release_all(h);
    let x = tokio::time::timeout(Duration::from_millis(200), x)
        .await
        .expect("the lock released, X is called");
    s.release_all(x);
    assert!(s.usage_is_empty());
}

/// An asker that **already holds the lock** (suspended, waiting to retry, or a lock just
/// rebuilt on becoming leader) asks for a slot again and leaves **right when it is
/// granted** — the slot is already in the channel. `Claim` releases that slot; also
/// releasing the lock would hand the object to the next one while the holder's journal is
/// half done — the later turn writes state, the half-done turn reruns, replays the old
/// value and overwrites it (a lost update).
#[tokio::test]
async fn lock_holder_abandoning_just_as_granted_keeps_lock() {
    let s = Scheduler::new(1);
    let q = vo(None, "u1");
    // H suspends: releases the slot, keeps the lock.
    let h = s.acquire(&q, id(1), 1).await;
    s.release_running(h);
    // The only slot is busy on another object ⇒ H's re-ask must wait for a slot.
    let busy = s.acquire(&vo(None, "other"), id(9), 9).await;
    let mut again = Box::pin(s.acquire(&q, id(1), 1));
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut again)
        .await
        .is_err());
    let mut other = Box::pin(s.acquire(&q, id(2), 2));
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut other)
        .await
        .is_err());

    // A free slot ⇒ granted to H (sits in the channel); H leaves before receiving it.
    s.release_all(busy);
    drop(again);
    if let Ok(o) = tokio::time::timeout(Duration::from_millis(20), &mut other).await {
        s.release_all(o);
        panic!(
            "H leaving right when granted also released the VO lock it already held — O jumped in"
        );
    }

    // The lock is still H's: H can ask again, and once it is done for good it is O's turn.
    let h = tokio::time::timeout(Duration::from_millis(200), s.acquire(&q, id(1), 1))
        .await
        .expect("the lock holder must be able to get a slot again");
    s.release_all(h);
    let o = tokio::time::timeout(Duration::from_millis(200), other)
        .await
        .expect("the lock released, O is called");
    s.release_all(o);
    assert!(s.usage_is_empty());
}

/// A lock holder at the head of the line **must not** be overtaken by a later arrival with
/// a smaller `seq`.
///
/// H holds the lock (rebuilt on becoming leader) and asks for a slot while the turn cap is
/// full: it takes position 0 and waits. On becoming leader `recover` hands over thousands
/// of invocations in **id** order, not `seq` order — so X, with a smaller `seq` than H,
/// arrives later. A binary search by `seq` on a line whose position 0 is already out of
/// order puts X **before** H: X at the head waits for H's lock, H waits for X. On a billing
/// workload's cluster: 3 864 commands of `acme:hot` stood still until the end of the run.
#[tokio::test]
async fn late_arrival_with_lower_seq_does_not_jump_lock_holder() {
    let s = Scheduler::new(1);
    let q = vo(None, "hot");
    // H (seq 50) holds the lock, suspended.
    let h = s.acquire(&q, id(50), 50).await;
    s.release_running(h);
    // The only slot is busy on another object ⇒ H's re-ask must wait for a slot.
    let busy = s.acquire(&vo(None, "other"), id(9), 9).await;
    let mut h = Box::pin(s.acquire(&q, id(50), 50));
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut h)
        .await
        .is_err());
    // X (seq 3) arrives after H.
    let mut x = Box::pin(s.acquire(&q, id(3), 3));
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut x)
        .await
        .is_err());

    s.release_all(busy);
    let h = tokio::time::timeout(Duration::from_millis(200), h)
        .await
        .expect("a later arrival overtook the lock holder — deadlock");
    s.release_all(h);
    let x = tokio::time::timeout(Duration::from_millis(200), x)
        .await
        .expect("the lock released, X is called");
    s.release_all(x);
    assert!(s.usage_is_empty());
}

/// A busy range **borrows** slots that other ranges on the same node leave idle.
///
/// The node's turn cap used to be split rigidly and evenly over the ranges the node leads
/// (D-c): a node holding 5 ranges gave each range 6 slots, even if only one range had work.
/// On a billing workload: one range held 93 996 waiting commands, draining ~3 turns/s with
/// 6/6 slots busy, while the node's other ranges left their share idle. The even split must
/// be the **guaranteed share**, and slots the node has free go to whoever is busy — while
/// the total still never exceeds the node cap (the cap protects the customer's SDK
/// deployment).
#[tokio::test]
async fn busy_range_borrows_idle_node_permits() {
    let budget = crate::engine_pool::TurnBudget::new(2);
    let _lead_a = budget.lead();
    let _lead_b = budget.lead();
    assert_eq!(budget.share(), 1);
    let a = Scheduler::new(budget.share());
    a.attach_node_budget(&budget);
    let b = Scheduler::new(budget.share());
    b.attach_node_budget(&budget);
    let quick = Duration::from_millis(20);
    let wait = Duration::from_millis(200);

    // A has work on two objects; B is idle ⇒ A uses both of the node's slots.
    let p1 = a.acquire(&vo(None, "x"), id(1), 1).await;
    let p2 = tokio::time::timeout(wait, a.acquire(&vo(None, "y"), id(2), 2))
        .await
        .expect("a busy range cannot borrow the node's free slots");

    // Node full (2/2): B has no turn yet — it waits, it does **not** exceed the cap.
    let z = vo(None, "z");
    let mut q = Box::pin(b.acquire(&z, id(3), 3));
    assert!(
        tokio::time::timeout(quick, &mut q).await.is_err(),
        "node cap exceeded"
    );
    // A cannot borrow more either.
    let w = vo(None, "w");
    let mut p3 = Box::pin(a.acquire(&w, id(4), 4));
    assert!(
        tokio::time::timeout(quick, &mut p3).await.is_err(),
        "borrowed beyond the node cap"
    );

    // A releases one: that slot goes to **B** (starving for its guaranteed share), not to A.
    a.release_all(p1);
    assert!(
        tokio::time::timeout(quick, &mut p3).await.is_err(),
        "a busy range grabbed the slot of a starving range"
    );
    b.node_turns_changed(budget.share()); // the actor listens to `changes()` and calls exactly this
    let q = tokio::time::timeout(wait, q)
        .await
        .expect("the starving range does not get its guaranteed share");

    // A releases the rest: A's next work runs in A's guaranteed share.
    a.release_all(p2);
    let p3 = tokio::time::timeout(wait, p3)
        .await
        .expect("once A releases, A's next work runs");
    a.release_all(p3);
    b.release_all(q);
    assert!(a.usage_is_empty() && b.usage_is_empty());
}

/// A regression of the first borrowing fix: the scheduler of a range dropped while it
/// still has half-done turns (range retired, owner changed, permits abandoned when stepping
/// down) **must not** hold node slots forever. The first version's cumulative counter
/// leaked like that: node-1 reported full while running 1 turn, the hot object crawled at
/// ~1.7 turns/s, 4 075 commands still waiting after 600 s of draining.
#[tokio::test]
async fn dropped_range_does_not_keep_node_permits() {
    let budget = crate::engine_pool::TurnBudget::new(2);
    let _lead = budget.lead();
    {
        let gone = Scheduler::new(budget.share());
        gone.attach_node_budget(&budget);
        let mut p1 = gone.acquire(&vo(None, "x"), id(1), 1).await;
        let mut p2 = gone.acquire(&vo(None, "y"), id(2), 2).await;
        // Half-done turns (already begun) are never released — exactly what a step-down
        // leaves behind.
        p1.begin();
        p2.begin();
        std::mem::forget(p1);
        std::mem::forget(p2);
    }
    let s = Scheduler::new(budget.share());
    s.attach_node_budget(&budget);
    let a = tokio::time::timeout(
        Duration::from_millis(200),
        s.acquire(&vo(None, "a"), id(3), 3),
    )
    .await
    .expect("the slots of a dropped range are still counted — the node is stuck full");
    let b = tokio::time::timeout(
        Duration::from_millis(200),
        s.acquire(&vo(None, "b"), id(4), 4),
    )
    .await
    .expect("the slots of a dropped range are still counted");
    s.release_all(a);
    s.release_all(b);
}

/// Queue of object `key` of `tenant` (EXCLUSIVE, no scope).
fn vo_of(tenant: &str, key: &str) -> QueueName {
    QueueName {
        tenant: cairn_store::TenantId::new(tenant).unwrap(),
        ..vo(None, key)
    }
}

/// **A tenant's turn cap on the whole node**: two ranges share one node, a cap of 1 turn
/// per tenant. `hog`'s second turn in **another** range waits at the `TenantTurns` tier
/// (not budget), another tenant is still granted; `hog` releases in range A ⇒ the node's
/// announcement wakes the waiter in range B. Once everything is released no count is left.
#[tokio::test]
async fn tenant_turn_cap_blocks_across_ranges_and_wakes() {
    let budget = crate::engine_pool::TurnBudget::new(4);
    budget.tenants().set_turn_cap(Some(1));
    let _lead_a = budget.lead();
    let _lead_b = budget.lead();
    let a = Scheduler::new(budget.share());
    a.attach_node_budget(&budget);
    let b = Scheduler::new(budget.share());
    b.attach_node_budget(&budget);
    let quick = Duration::from_millis(20);
    let wait = Duration::from_millis(200);

    let p1 = a.acquire(&vo_of("hog", "x"), id(1), 1).await;
    let hog_b = vo_of("hog", "y");
    let mut q = Box::pin(b.acquire(&hog_b, id(2), 2));
    assert!(
        tokio::time::timeout(quick, &mut q).await.is_err(),
        "hog holds two turns in two ranges, cap 1"
    );
    let blocked = b.blocked_on(&hog_b);
    assert_eq!(
        blocked,
        Some(ResourceKind::TenantTurns(
            cairn_store::TenantId::new("hog").unwrap()
        ))
    );
    assert!(
        !blocked.unwrap().is_budget(),
        "the tenant cap is not budget"
    );
    let other = tokio::time::timeout(wait, b.acquire(&vo_of("quiet", "z"), id(3), 3))
        .await
        .expect("another tenant blocked by hog's cap");

    a.release_all(p1);
    b.node_turns_changed(budget.share()); // the actor listens to `changes()` and calls exactly this
    let p2 = tokio::time::timeout(wait, q)
        .await
        .expect("hog left the cap in range A but the waiter in range B was not woken");
    b.release_all(p2);
    b.release_all(other);
    assert!(a.usage_is_empty() && b.usage_is_empty());
    assert_eq!(
        budget
            .tenants()
            .meter(&cairn_store::TenantId::new("hog").unwrap())
            .turns(),
        0
    );
}

/// A scheduler dropped or `reset` while a tenant still has half-done turns: those turns
/// leave the node's counter — the tenant does not get stuck at the cap.
#[tokio::test]
async fn dropped_range_returns_tenant_turns() {
    let budget = crate::engine_pool::TurnBudget::new(4);
    budget.tenants().set_turn_cap(Some(1));
    let _lead = budget.lead();
    let hog = cairn_store::TenantId::new("hog").unwrap();
    {
        let gone = Scheduler::new(budget.share());
        gone.attach_node_budget(&budget);
        let mut p = gone.acquire(&vo_of("hog", "x"), id(1), 1).await;
        p.begin();
        std::mem::forget(p);
        assert_eq!(budget.tenants().meter(&hog).turns(), 1);
    }
    assert_eq!(
        budget.tenants().meter(&hog).turns(),
        0,
        "a dropped range keeps turns"
    );
    let s = Scheduler::new(budget.share());
    s.attach_node_budget(&budget);
    let mut p = tokio::time::timeout(
        Duration::from_millis(200),
        s.acquire(&vo_of("hog", "y"), id(2), 2),
    )
    .await
    .expect("the turns of a dropped range still count toward the tenant's cap");
    p.begin();
    std::mem::forget(p);
    s.reset();
    assert_eq!(budget.tenants().meter(&hog).turns(), 0, "reset keeps turns");
}

/// Three counter tiers at once, and **the effective cap is the tightest one**.
#[tokio::test]
async fn three_counter_tiers_all_block() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("acme", 10), ("acme/growth", 2)]));
    let q = queue(Some("acme"), Some("growth/alice"));

    let p1 = s.acquire(&q, id(1), 1).await;
    let p2 = s.acquire(&q, id(2), 2).await;
    // Tier L1 runs out at 2, even though the scope tier still has 8.
    let mut third = Box::pin(s.acquire(&q, id(3), 3));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut third)
            .await
            .is_err(),
        "the effective cap must be the tightest one"
    );
    s.release_all(p1);
    let p3 = tokio::time::timeout(Duration::from_millis(200), third)
        .await
        .expect("releasing a slot at L1 must wake exactly the waiter there");
    s.release_all(p2);
    s.release_all(p3);
    assert!(s.usage_is_empty());
}

/// A parent counter **includes** the child counter but can be larger: some invocations
/// call straight at the parent tier.
#[tokio::test]
async fn parent_counter_covers_children_and_direct_callers() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("acme", 2)]));
    // One at the scope tier, one at tier L2 — both add to `acme`.
    let p1 = s.acquire(&queue(Some("acme"), None), id(1), 1).await;
    let p2 = s
        .acquire(&queue(Some("acme"), Some("growth/alice")), id(2), 2)
        .await;
    let other = queue(Some("acme"), Some("other"));
    let mut third = Box::pin(s.acquire(&other, id(3), 3));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut third)
            .await
            .is_err(),
        "the scope counter must also count invocations called straight at the parent tier"
    );
    s.release_all(p1);
    let p3 = tokio::time::timeout(Duration::from_millis(200), third)
        .await
        .unwrap();
    s.release_all(p2);
    s.release_all(p3);
    assert!(s.usage_is_empty());
}

/// **Wake by concrete counter.** Releasing a slot at `acme/foo/t1` wakes the queue waiting on
/// exactly that counter, and does **not** wake the queue waiting on `acme/bar/t1`, even
/// though both match the same rule `acme/*/t1`.
#[tokio::test]
async fn wakes_by_specific_counter_not_by_rule() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("acme/*/t1", 1)]));
    let foo = queue(Some("acme"), Some("foo/t1"));
    let bar = queue(Some("acme"), Some("bar/t1"));

    let p_foo = s.acquire(&foo, id(1), 1).await;
    let p_bar = s.acquire(&bar, id(2), 2).await;
    let mut foo2 = Box::pin(s.acquire(&foo, id(3), 3));
    let mut bar2 = Box::pin(s.acquire(&bar, id(4), 4));
    for f in [
        &mut foo2 as &mut (dyn std::future::Future<Output = Permit> + Unpin),
        &mut bar2,
    ] {
        assert!(
            tokio::time::timeout(Duration::from_millis(20), f)
                .await
                .is_err(),
            "both must be blocked at their own counter's cap of 1"
        );
    }

    s.release_all(p_foo);
    let p_foo2 = tokio::time::timeout(Duration::from_millis(200), foo2)
        .await
        .expect("the waiter at acme/foo/t1 must be woken");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut bar2)
            .await
            .is_err(),
        "the waiter at acme/bar/t1 must NOT be woken by a slot of foo"
    );

    s.release_all(p_bar);
    let p_bar2 = tokio::time::timeout(Duration::from_millis(200), bar2)
        .await
        .unwrap();
    s.release_all(p_foo2);
    s.release_all(p_bar2);
    assert!(s.usage_is_empty());
}

/// The VO lock is **not** released while suspended; the counter is.
#[tokio::test]
async fn suspend_releases_permit_but_keeps_lock() {
    let s = Scheduler::new(100);
    let q = vo(Some("acme"), "user-1");
    let p1 = s.acquire(&q, id(1), 1).await;

    // Someone else on the same object key must wait.
    let mut other = Box::pin(s.acquire(&q, id(2), 2));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut other)
        .await
        .is_err());

    // Suspend: release the slot, keep the lock. The other one **still** has to wait...
    s.release_running(p1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut other)
            .await
            .is_err(),
        "releasing the VO lock while suspended reopens exactly the race the queue closes"
    );
    assert!(
        s.usage_is_empty(),
        "the slot must be released while suspended"
    );

    // ...and the holder itself can get a slot again right away, since the lock is still its own.
    let p1 = tokio::time::timeout(Duration::from_millis(200), s.acquire(&q, id(1), 1))
        .await
        .expect("an invocation holding the lock must be able to get a slot again");
    s.release_all(p1);
    let p2 = tokio::time::timeout(Duration::from_millis(200), other)
        .await
        .expect("once it is done for good the lock must pass to the next one");
    s.release_all(p2);
}

/// Two different object keys do not lock each other; neither do two different scopes.
#[tokio::test]
async fn object_lock_separates_by_object_key_and_scope() {
    let s = Scheduler::new(100);
    let a = s.acquire(&vo(Some("acme"), "u1"), id(1), 1).await;
    let b = s.acquire(&vo(Some("acme"), "u2"), id(2), 2).await;
    let c = s.acquire(&vo(Some("other"), "u1"), id(3), 3).await;
    let d = s.acquire(&vo(None, "u1"), id(4), 4).await;
    for p in [a, b, c, d] {
        s.release_all(p);
    }
    assert!(s.usage_is_empty());
}

/// The **unscoped** path is subject to no limit except the turn cap.
/// **The wait line is ordered by `seq`, not by arrival order** — the mechanism behind the
/// FIFO guarantee on one object key.
///
/// This is the *only* test that tells the two implementations apart. The tests at the
/// layer above (`tests/flow_control.rs` and the load test's `hot` scenario) pin the
/// **contract** — "send to one key sequentially and it runs sequentially" — but both stay
/// green if the `insert` by seq is changed to `push_back`, because when calls arrive
/// sequentially the arrival order already matches the seq order. The public API cannot
/// build the case where the two orders differ; here it can, and cheaply.
///
/// That case is real: a deferred turn (out of slots, or suspended and then woken) comes
/// back to the wait line carrying its **old** `seq`, i.e. it is behind newcomers in time
/// but must stand ahead of them in order. Ordering by arrival there lets newcomers cut in
/// front of an invocation that has waited longer — and on a busy `EXCLUSIVE` queue,
/// cutting in is starvation.
#[tokio::test]
async fn queue_orders_by_seq_not_arrival_order() {
    let s = Scheduler::new(100);
    let q = vo(None, "u1");

    // A holds the VO lock ⇒ everyone after must queue.
    let a = s.acquire(&q, id(1), 1).await;

    // C arrives **first**, but with a **larger** seq.
    let mut c = Box::pin(s.acquire(&q, id(3), 3));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut c)
            .await
            .is_err(),
        "C got in while the VO lock is held"
    );

    // B arrives **later**, with a **smaller** seq. Arrival order now contradicts seq order —
    // exactly what no check at the layer above can build.
    let mut b = Box::pin(s.acquire(&q, id(2), 2));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut b)
            .await
            .is_err(),
        "B got in while the VO lock is held"
    );

    s.release_all(a);

    let pb = tokio::time::timeout(Duration::from_millis(200), &mut b)
        .await
        .expect("B (seq 2) must run before C (seq 3), though B joined the line later");
    assert_eq!(
        pb.invocation(),
        id(2),
        "wrong one: the wait line is not ordered by seq"
    );
    s.release_all(pb);

    let pc = tokio::time::timeout(Duration::from_millis(200), c)
        .await
        .expect("C must get its turn after B");
    assert_eq!(pc.invocation(), id(3));
    s.release_all(pc);
    assert!(s.usage_is_empty());
}

#[tokio::test]
async fn no_scope_means_no_counter() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("*", 1)]));
    let q = queue(None, None);
    let mut held = Vec::new();
    for i in 0..20 {
        held.push(
            tokio::time::timeout(Duration::from_millis(200), s.acquire(&q, id(i), i as u64))
                .await
                .expect("rule `*` must not touch an unscoped invocation"),
        );
    }
    for p in held {
        s.release_all(p);
    }
}

/// The partition's turn cap is a resource like every other.
#[tokio::test]
async fn turn_cap_blocks_and_wakes() {
    let s = Scheduler::new(2);
    let q = queue(None, None);
    let p1 = s.acquire(&q, id(1), 1).await;
    let p2 = s.acquire(&q, id(2), 2).await;
    let mut p3 = Box::pin(s.acquire(&q, id(3), 3));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut p3)
        .await
        .is_err());
    s.release_running(p1);
    let p3 = tokio::time::timeout(Duration::from_millis(200), p3)
        .await
        .unwrap();
    s.release_all(p2);
    s.release_all(p3);
}

/// **Round-robin between tenants.** Two tenants, one sends 10k — the other does not starve.
///
/// Neither has rules, so they only compete for the **turn cap**. Without round-robin the
/// second tenant would queue behind the first tenant's 10k requests on one shared FIFO
/// line, and wait for turn number 10001.
#[tokio::test]
async fn fairness_noisy_neighbour_does_not_starve_others() {
    let s = Scheduler::new(1);
    let loud = queue(Some("loud"), None);
    let quiet = queue(Some("quiet"), None);

    // The only slot is held, so every request below queues.
    let held = s.acquire(&queue(Some("holder"), None), id(0), 0).await;

    let mut loud_waiters: Vec<_> = (0..10_000)
        .map(|i| Box::pin(s.acquire(&loud, id((i % 250) as u8), i as u64)))
        .collect();
    let mut quiet_waiter = Box::pin(s.acquire(&quiet, id(200), 1));

    // Release one slot: round-robin must rotate to the next queue, not keep serving the
    // noisy one.
    s.release_all(held);
    let first = tokio::time::timeout(Duration::from_millis(200), &mut loud_waiters[0])
        .await
        .expect("the first one of the noisy queue is served first");
    s.release_all(first);
    let p = tokio::time::timeout(Duration::from_millis(200), &mut quiet_waiter)
        .await
        .expect("the quiet tenant must get its turn right after ONE slot of the noisy one");
    s.release_all(p);

    // Cleanup: every remaining waiter is dropped, and the scheduler must cope with that.
    drop(std::mem::take(&mut loud_waiters));
    assert!(s.usage_is_empty());
}

/// **Counters return to 0.** 10k random asks/releases ⇒ counters back to 0.
///
/// A counter leak is not wrong right away — it shows after a few days as "the system slows
/// down on its own and then stops". This test is the only place that catches it early.
#[tokio::test]
async fn no_leak_over_ten_thousand_acquire_release() {
    let s = Scheduler::new(64);
    s.set_rules(book(&[("*", 8), ("*/*", 4), ("*/*/*", 2)]));

    // A deterministic pseudo-random generator: a failing test must be reproducible.
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut held: Vec<(bool, Permit)> = Vec::new();
    for i in 0..10_000u64 {
        let r = next();
        if !held.is_empty() && r % 3 == 0 {
            let idx = (r >> 8) as usize % held.len();
            let (suspend, p) = held.swap_remove(idx);
            if suspend {
                s.release_running(p);
            } else {
                s.release_all(p);
            }
            continue;
        }
        let q = QueueName {
            tenant: cairn_store::TenantId::default(),
            scope: Some(sc(&format!("s{}", r % 5))),
            limit: Some(LimitKey::parse(&format!("l{}/t{}", (r >> 8) % 4, (r >> 16) % 3)).unwrap()),
            exclusive: r % 7 == 0,
            service: "Svc".into(),
            object_key: format!("k{}", (r >> 24) % 6),
        };
        let inv = ((i % 251) as u8).wrapping_add(1);
        // May be blocked — skip, the goal is accounting, not throughput.
        if let Ok(p) =
            tokio::time::timeout(Duration::from_millis(1), s.acquire(&q, id(inv), i)).await
        {
            held.push((r % 2 == 0, p));
        }
    }
    // `release_running` keeps the lock, so finish the cleanup with `release_all`.
    for (_, p) in held {
        s.release_all(p);
    }
    // No `reset()` here: `reset` clears the very counters under test, so an assert after
    // it would always pass.
    assert!(s.usage_is_empty(), "counter leak after 10k asks/releases");
}

/// A rule change ⇒ every waiting queue goes back to the eligibility loop and filters
/// itself again.
#[tokio::test]
async fn raising_cap_lets_waiters_run() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("acme", 1)]));
    let q = queue(Some("acme"), None);
    let p1 = s.acquire(&q, id(1), 1).await;
    let mut waiting = Box::pin(s.acquire(&q, id(2), 2));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );

    let mut b = book(&[("acme", 5)]);
    b.version = 2;
    s.set_rules(b);
    let p2 = tokio::time::timeout(Duration::from_millis(200), waiting)
        .await
        .expect("raising the cap must wake the waiter");
    s.release_all(p1);
    s.release_all(p2);
    assert!(s.usage_is_empty());
}

/// Tightening the cap does **not** evict anyone running — it only blocks later arrivals.
#[tokio::test]
async fn lowering_cap_does_not_evict_running() {
    let s = Scheduler::new(100);
    s.set_rules(book(&[("acme", 5)]));
    let q = queue(Some("acme"), None);
    let held: Vec<Permit> = {
        let mut v = Vec::new();
        for i in 1..=3 {
            v.push(s.acquire(&q, id(i), i as u64).await);
        }
        v
    };
    let mut b = book(&[("acme", 1)]);
    b.version = 2;
    s.set_rules(b);
    // The three running keep running; the fourth is blocked.
    let mut next = Box::pin(s.acquire(&q, id(9), 9));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut next)
        .await
        .is_err());
    for p in held {
        s.release_all(p);
    }
    let p = tokio::time::timeout(Duration::from_millis(200), next)
        .await
        .unwrap();
    s.release_all(p);
    assert!(s.usage_is_empty());
}

/// With a lock rebuilt on becoming leader, others still have to wait.
#[tokio::test]
async fn lock_rebuilt_after_failover_still_blocks() {
    let s = Scheduler::new(100);
    let q = vo(Some("acme"), "u1");
    s.restore_lock(q.lock_name().unwrap(), id(7));
    let mut other = Box::pin(s.acquire(&q, id(8), 8));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut other)
        .await
        .is_err());
    // The old owner gets a slot again right away — it still holds the lock.
    let p = tokio::time::timeout(Duration::from_millis(200), s.acquire(&q, id(7), 7))
        .await
        .unwrap();
    s.release_all(p);
    let p = tokio::time::timeout(Duration::from_millis(200), other)
        .await
        .unwrap();
    s.release_all(p);
}

/// **A leader that comes up after an unsuspend reads the registry's marks**: the term's
/// first sync (suspended set empty ⇒ empty, nothing "changed") puts a non-suspended tenant
/// whose registry entry carries `lifted-ms=` into `take_lifted` with both marks; a mark
/// only once per term, even when the registry changes another field; a suspended tenant
/// does not; a new unsuspend (new mark) does again, along with the `marks: None` line of a
/// leader that sees the set change.
#[test]
fn new_leader_reads_lift_marks_once() {
    use cairn_engine::{TenantLimit, TenantLimits, TenantLimitsSource, TenantState};
    let acme = cairn_store::TenantId::new("acme").unwrap();
    let book = |l: TenantLimit| TenantLimits::new([(acme.clone(), l)].into_iter().collect());
    let lifted = TenantLimit {
        refused_ms: Some(1_000),
        lifted_ms: Some(2_000),
        ..TenantLimit::default()
    };
    let src = TenantLimitsSource::default();
    src.replace(book(lifted));
    let s = Scheduler::new(8);
    s.attach_tenants(src.clone(), SUSPENDED_RETRY);
    // Start of the term: the `reset` right after `attach_tenants` (`actor/leadership.rs`)
    // drops the pending line — the mark must come back on the next sync.
    s.reset();
    s.sync_tenants();
    assert_eq!(
        s.take_lifted(),
        vec![Lifted {
            tenant: acme.clone(),
            marks: Some((Some(1_000), 2_000)),
        }]
    );
    // The registry changes another field, same marks: no second pull.
    src.replace(book(TenantLimit {
        max_live: Some(5),
        ..lifted
    }));
    s.sync_tenants();
    assert!(s.take_lifted().is_empty());
    // Suspended again: no pull (and the suspended set changes).
    src.replace(book(TenantLimit {
        state: TenantState::Suspended,
        ..lifted
    }));
    s.sync_tenants();
    assert!(s.take_lifted().is_empty());
    // Unsuspended once more: the `marks: None` line (set change seen) and the new-mark line.
    src.replace(book(TenantLimit {
        lifted_ms: Some(9_000),
        ..lifted
    }));
    s.sync_tenants();
    assert_eq!(
        s.take_lifted(),
        vec![
            Lifted {
                tenant: acme.clone(),
                marks: None,
            },
            Lifted {
                tenant: acme,
                marks: Some((Some(1_000), 9_000)),
            },
        ]
    );
}

/// **Suspended tenant**: the scheduler grants no slot at all to a tenant in the refused
/// set — even when every other resource has room — and reports exactly the blocking
/// resource (`Tenant`, counted as budget, rescheduled after [`SUSPENDED_RETRY`]). Other
/// tenants are unaffected. Unsuspending wakes exactly the waiter. Suspended while already
/// waiting elsewhere (VO lock) ⇒ releasing the lock does not grant either.
#[tokio::test]
async fn suspended_tenant_gets_no_permit() {
    let s = Scheduler::new(8);
    let acme = cairn_store::TenantId::new("acme").unwrap();
    let of = |mut q: QueueName| {
        q.tenant = acme.clone();
        q
    };
    let (qa, qother) = (of(queue(Some("a"), None)), queue(Some("a"), None));
    s.set_refused([acme.clone()].into_iter().collect());
    let mut a = Box::pin(s.acquire(&qa, id(1), 1));
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut a)
        .await
        .is_err());
    let res = ResourceKind::Tenant(acme.clone());
    assert_eq!(s.blocked_on(&qa), Some(res.clone()));
    assert!(
        res.is_budget(),
        "suspension counts as budget: a background turn gives up right away"
    );
    assert_eq!(s.throttle_after(&res), Some(SUSPENDED_RETRY));
    assert_eq!(s.turns_in_use(), 0);
    // Another tenant, same scope, same range: goes straight through.
    let other = s.acquire(&qother, id(2), 2).await;
    s.release_all(other);
    // Unsuspend: the waiter is granted.
    s.set_refused(Default::default());
    let a = tokio::time::timeout(Duration::from_millis(100), a)
        .await
        .expect("unsuspending grants the waiter");

    // Suspended while waiting for the VO lock: releasing the lock grants no suspended tenant.
    let (v1, v2) = (of(vo(None, "k")), of(vo(None, "k")));
    let holder = s.acquire(&v1, id(3), 3).await;
    let mut waiter = Box::pin(s.acquire(&v2, id(4), 4));
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiter)
        .await
        .is_err());
    s.set_refused([acme.clone()].into_iter().collect());
    s.release_all(holder);
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiter)
        .await
        .is_err());
    assert_eq!(s.blocked_on(&v2), Some(ResourceKind::Tenant(acme.clone())));
    s.set_refused(Default::default());
    let w = tokio::time::timeout(Duration::from_millis(100), waiter)
        .await
        .expect("unsuspending wakes the lock waiter");
    s.release_all(w);
    s.release_all(a);
    assert!(s.usage_is_empty());
}
