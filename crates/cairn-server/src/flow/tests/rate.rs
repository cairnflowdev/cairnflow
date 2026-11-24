// #[hot-path] — a file on the path of every turn (AGENTS.md §6): no `info`-level logging on
// the normal path of a turn, no global locks, no synchronous I/O.
//
// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Tests: the rate tier and WFQ.

use super::*;

// ─────────────────── rate tier and WFQ

fn of_tenant(t: &str, key: &str) -> QueueName {
    QueueName {
        tenant: TenantId::new(t).unwrap(),
        ..vo(None, key)
    }
}

fn tenant_rate(t: &str, rate: u32, burst: u32) -> RuleBook {
    let mut b = RuleBook::default();
    b.apply_tenant(
        t,
        cairn_store::RuleEdit {
            rate: Some(Limit::Max(rate)),
            burst: Some(burst),
            ..Default::default()
        },
        0,
    )
    .unwrap();
    b.version = 1;
    b
}

/// Out of tokens ⇒ blocked on `Rate(tenant)` (the fourth tier, `429` like a counter),
/// `throttle_after` says when the bucket goes positive again; the rate tier's clock wakes
/// the waiter when tokens come back. Settlement of `CU − 1` after the turn; another tenant
/// is not blocked; the counter returns to 0.
#[tokio::test(start_paused = true)]
async fn out_of_tokens_blocks_at_rate_tier_and_clock_wakes() {
    let s = Scheduler::new(10);
    s.set_rules(tenant_rate("a", 10, 1));
    let p = s.acquire(&of_tenant("a", "x"), id(1), 1).await;
    // The turn costs 3 CU: 1 charged up front, 2 settled ⇒ 2 CU of debt (0.2 s at 10 CU/s).
    s.settle(&p, 3);
    s.release_all(p);
    let q = of_tenant("a", "y");
    let mut next = Box::pin(s.acquire(&q, id(2), 2));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(next.as_mut().poll(&mut cx).is_pending());
    let blocked = s.blocked_on(&q).expect("must be waiting");
    assert!(
        matches!(&blocked, ResourceKind::Rate(RateKey::Tenant(t)) if t.as_str() == "a"),
        "{blocked:?}"
    );
    assert!(blocked.is_budget());
    let wait = s.throttle_after(&blocked).unwrap();
    assert!(
        wait > Duration::from_millis(200) && wait <= Duration::from_millis(201),
        "{wait:?}"
    );
    // Another tenant does not share the bucket.
    let other = tokio::time::timeout(
        Duration::from_millis(1),
        s.acquire(&of_tenant("b", "z"), id(3), 3),
    )
    .await
    .expect("a tenant with no cap is blocked by another tenant's bucket");
    s.release_all(other);
    // Clock: before the deadline nobody is called.
    let clock = s.clone();
    let p2 = tokio::task::LocalSet::new()
        .run_until(async move {
            let clock = tokio::task::spawn_local(async move { clock.rate_loop().await });
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(
                next.as_mut().poll(&mut cx).is_pending(),
                "ran before the tokens came back"
            );
            let p2 = tokio::time::timeout(Duration::from_millis(100), next)
                .await
                .expect("tokens came back but nobody was called");
            clock.abort();
            p2
        })
        .await;
    s.settle(&p2, 0); // the turn never reached the SDK: refund 1
    s.release_all(p2);
    assert!(s.usage_is_empty());
}

/// The `ResourceManager` core is a function of `now`: no runtime, no hidden clock — same
/// state and same `now` give the same decision; the caller chooses `now`. Bucket of
/// 10 CU/s: a turn costing 3 CU at `t0` leaves 2 CU of debt, so the next turn is blocked on
/// `Rate` until `t0 + 200 ms`.
#[test]
fn decision_core_is_a_function_of_now() {
    let mut rm = ResourceManager::new(10);
    rm.set_rules(tenant_rate("a", 10, 1));
    let t0 = Instant::now();
    let at = |ms| t0 + Duration::from_millis(ms);
    let AcquireOutcome::Acquired(prov) = rm.poll_acquire(&of_tenant("a", "x"), &id(1), None, t0)
    else {
        panic!("blocked with a full bucket");
    };
    let mut p = prov.secure(&mut rm, t0);
    rm.settle(&p, 3, t0);
    let blocked = |now| {
        matches!(
            rm.poll_acquire(&of_tenant("a", "y"), &id(2), None, now),
            AcquireOutcome::BlockedOn(ResourceKind::Rate(_))
        )
    };
    assert!(blocked(t0));
    assert!(blocked(at(200)));
    assert!(
        !blocked(at(200) + Duration::from_nanos(1)),
        "debt paid off exactly after 2/rate"
    );
    assert!(
        blocked(at(100)),
        "an earlier `now` blocks again — no hidden time-based state"
    );
    let k = RateKey::Tenant(TenantId::new("a").unwrap());
    assert_eq!(rm.rate_wait(&k, at(40)), Duration::from_nanos(160_000_001));
    let _ = rm.release_all(&mut p);
}

/// Per-counter rate cap (a pattern rule with `rate`): one bucket per `(tenant, scope)`,
/// another tenant with the same scope does not share it.
#[tokio::test(start_paused = true)]
async fn scope_rate_one_bucket_per_tenant() {
    let s = Scheduler::new(10);
    let mut b = RuleBook::default();
    b.apply(
        "acme",
        cairn_store::RuleEdit {
            rate: Some(Limit::Max(1)),
            ..Default::default()
        },
        0,
    )
    .unwrap();
    b.version = 1;
    s.set_rules(b);
    let in_scope = |t: &str, k: &str| QueueName {
        tenant: TenantId::new(t).unwrap(),
        ..vo(Some("acme"), k)
    };
    let p = s.acquire(&in_scope("t1", "x"), id(1), 1).await;
    s.release_all(p);
    let q = in_scope("t1", "y");
    let mut next = Box::pin(s.acquire(&q, id(2), 2));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(next.as_mut().poll(&mut cx).is_pending());
    assert!(matches!(
        s.blocked_on(&q),
        Some(ResourceKind::Rate(RateKey::Counter(_)))
    ));
    let p = tokio::time::timeout(
        Duration::from_millis(1),
        s.acquire(&in_scope("t2", "z"), id(3), 3),
    )
    .await
    .expect("another tenant with the same scope is blocked by t1's bucket");
    s.release_all(p);
    drop(next);
}

/// When saturated, the freed slot goes to the tenant with the smallest **current** `vt` —
/// not to the queue that registered to wait first. Tenant A has 3 queues waiting from
/// before and has run a lot; B arrives later with one queue.
#[tokio::test]
async fn free_permit_goes_to_tenant_with_lowest_vt() {
    let s = Scheduler::new(1);
    // A spends a lot: A's `vt` is high.
    for i in 0..5 {
        let p = s
            .acquire(&of_tenant("a", "warm"), id(100 + i), i as u64)
            .await;
        s.settle(&p, 10);
        s.release_all(p);
    }
    let holder = s.acquire(&of_tenant("a", "run"), id(1), 1).await;
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let aq: Vec<_> = (0..3).map(|i| of_tenant("a", &format!("k{i}"))).collect();
    let mut a_waiting: Vec<_> = aq
        .iter()
        .enumerate()
        .map(|(i, q)| Box::pin(s.acquire(q, id(10 + i as u8), 10 + i as u64)))
        .collect();
    for f in a_waiting.iter_mut() {
        assert!(f.as_mut().poll(&mut cx).is_pending());
    }
    let bq = of_tenant("b", "only");
    let mut b = Box::pin(s.acquire(&bq, id(20), 20));
    assert!(b.as_mut().poll(&mut cx).is_pending());
    s.release_all(holder);
    let pb = tokio::time::timeout(Duration::from_millis(100), b)
        .await
        .expect("B must go first");
    for f in a_waiting.iter_mut() {
        assert!(f.as_mut().poll(&mut cx).is_pending(), "A jumped ahead of B");
    }
    s.release_all(pb);
    for f in a_waiting {
        let p = tokio::time::timeout(Duration::from_millis(100), f)
            .await
            .expect("A runs next");
        s.release_all(p);
    }
    assert!(s.usage_is_empty());
}

/// Borrowing slots across ranges by `vt`. A node with 3 slots, two ranges (guaranteed share
/// 1). Range X: tenant A (high `vt`) runs one turn and wants to borrow a free slot. Range
/// Y: tenant B (low `vt`) runs one turn and has one more request waiting (VO lock) — B has
/// demand on the node.
///
/// Must see: A can **not** borrow while B is waiting with a `vt` lower by more than the
/// slack, even though the node has room; once B stops waiting A can borrow. The guaranteed
/// share is unchanged: each range's first turn runs right away.
#[tokio::test]
async fn cross_range_permit_borrowing_by_vt() {
    let budget = crate::engine_pool::TurnBudget::new(3);
    let _l1 = budget.lead();
    let _l2 = budget.lead();
    let x = Scheduler::new(budget.share());
    x.attach_node_budget(&budget);
    let y = Scheduler::new(budget.share());
    y.attach_node_budget(&budget);
    let warm = x.acquire(&of_tenant("a", "warm"), id(1), 1).await;
    x.settle(&warm, 1_000);
    x.release_all(warm);
    let px = x.acquire(&of_tenant("a", "x0"), id(2), 2).await;
    let yq = of_tenant("b", "y0");
    let py = y.acquire(&yq, id(3), 3).await;
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut b_next = Box::pin(y.acquire(&yq, id(4), 4));
    assert!(
        b_next.as_mut().poll(&mut cx).is_pending(),
        "B waits for the VO lock"
    );
    budget.tenants().sweep(Instant::now(), false);
    let aq = of_tenant("a", "x1");
    let mut a_more = Box::pin(x.acquire(&aq, id(5), 5));
    assert!(
        a_more.as_mut().poll(&mut cx).is_pending(),
        "A (high vt) borrowed a free slot while B (low vt) was waiting"
    );
    // B done: B's next request runs in Y's guaranteed share, B stops waiting.
    y.release_all(py);
    let pb = tokio::time::timeout(Duration::from_millis(100), b_next)
        .await
        .expect("B runs next");
    budget.tenants().sweep(Instant::now(), false);
    x.node_turns_changed(budget.share());
    let pa = tokio::time::timeout(Duration::from_millis(100), a_more)
        .await
        .expect("with nobody waiting at a lower vt, A can borrow");
    x.release_all(pa);
    x.release_all(px);
    y.release_all(pb);
    assert!(x.usage_is_empty() && y.usage_is_empty());
}

/// A request of a **suspended** tenant does not pin the node's `min_vt`. A node with
/// 3 slots, two ranges (guaranteed share 1). Range Y: suspended tenant S has one waiting
/// request (blocked on `Tenant` — like the parked tickets of every `running` invocation of S
/// after a leader change). Range X: tenant A (high `vt`, has run a lot) runs one turn in its
/// guaranteed share and wants to borrow a free slot of the node.
///
/// Must see: A can borrow — S cannot run (suspended), so its frozen `vt` is not "the
/// slowest waiting tenant". Before the fix: every `Pending` counted toward `demand`,
/// `min_vt` = S's `vt`, and A was blocked for the whole suspension.
#[tokio::test]
async fn suspended_tenant_does_not_pin_min_vt() {
    let budget = crate::engine_pool::TurnBudget::new(3);
    let _l1 = budget.lead();
    let _l2 = budget.lead();
    let x = Scheduler::new(budget.share());
    x.attach_node_budget(&budget);
    let y = Scheduler::new(budget.share());
    y.attach_node_budget(&budget);
    let s_tenant = TenantId::new("s").unwrap();
    y.set_refused([s_tenant.clone()].into_iter().collect());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let sq = of_tenant("s", "s0");
    let mut s_wait = Box::pin(y.acquire(&sq, id(9), 9));
    assert!(s_wait.as_mut().poll(&mut cx).is_pending(), "S is suspended");
    assert_eq!(y.blocked_on(&sq), Some(ResourceKind::Tenant(s_tenant)));
    let warm = x.acquire(&of_tenant("a", "warm"), id(1), 1).await;
    x.settle(&warm, 1_000);
    x.release_all(warm);
    let px = x.acquire(&of_tenant("a", "x0"), id(2), 2).await;
    budget.tenants().sweep(Instant::now(), false);
    let aq = of_tenant("a", "x1");
    let a_more = x.acquire(&aq, id(5), 5);
    let pa = tokio::time::timeout(Duration::from_millis(100), a_more)
        .await
        .expect("A cannot borrow a free slot because of a suspended tenant's request");
    x.release_all(pa);
    x.release_all(px);
    drop(s_wait);
    assert!(x.usage_is_empty() && y.usage_is_empty());
}

/// **The starving flag does not outlive its slot waiters** (`flow/` sim seed 1473; FL-7).
/// A node with 3 slots, two ranges (guaranteed share 1).
///
/// 1. Range X borrows the whole node: three turns of tenant A (guaranteed 1 + borrowed 2).
/// 2. Range Y: tenant S asks for a slot while the node is full, below its guaranteed share
///    ⇒ blocked on `Turns`, Y **starves** (other ranges stop borrowing).
/// 3. Tenant S is suspended: `set_refused` moves Y's waiter to blocked on `Tenant` — Y has
///    nobody waiting for a slot any more.
/// 4. X releases one turn (the node has room), then asks again.
///
/// Must see: X can borrow again right away. Before the fix: Y's starving flag stood forever
/// — only a grant in Y or a "node changed" message cleared it, and with the node not full
/// nobody sends one — so no other range could borrow any free slot of the node.
#[tokio::test]
async fn starvation_flag_does_not_outlive_its_permit_waiter() {
    let budget = crate::engine_pool::TurnBudget::new(3);
    let _l1 = budget.lead();
    let _l2 = budget.lead();
    let x = Scheduler::new(budget.share());
    x.attach_node_budget(&budget);
    let y = Scheduler::new(budget.share());
    y.attach_node_budget(&budget);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut held = Vec::new();
    for n in 1..=3u8 {
        let q = of_tenant("a", &format!("x{n}"));
        held.push(
            tokio::time::timeout(
                Duration::from_millis(100),
                x.acquire(&q, id(n), u64::from(n)),
            )
            .await
            .expect("X borrows the whole node"),
        );
    }
    let sq = of_tenant("s", "s0");
    let mut s_wait = Box::pin(y.acquire(&sq, id(9), 9));
    assert!(s_wait.as_mut().poll(&mut cx).is_pending(), "node full");
    assert_eq!(y.blocked_on(&sq), Some(ResourceKind::Turns));
    y.set_refused([TenantId::new("s").unwrap()].into_iter().collect());
    assert_eq!(
        y.blocked_on(&sq),
        Some(ResourceKind::Tenant(TenantId::new("s").unwrap()))
    );
    let p = held.pop().expect("one turn of X");
    x.release_all(p);
    let aq = of_tenant("a", "x9");
    let again = tokio::time::timeout(Duration::from_millis(100), x.acquire(&aq, id(8), 8))
        .await
        .expect("X cannot borrow again: Y's starving flag stands while Y has no slot waiters");
    x.release_all(again);
    for p in held {
        x.release_all(p);
    }
    drop(s_wait);
    assert!(x.usage_is_empty() && y.usage_is_empty());
}
