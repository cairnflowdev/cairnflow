# Runbook — a node's clock runs at the wrong rate

**Alert:** `CairnflowClockRateUntrusted` (`cairnflow_clock_rate_untrusted` > 0). Often together
with `CairnflowNodeDrainingLong` (the node drains for a long time) and the `error` log "node
monotonic clock rate is off against the store beyond the threshold".

## Mechanism

Code: `crates/cairn-server/src/clock_rate.rs`, `crates/cairn-store/src/store_clock.rs`; invariant
LS-2 in [`docs/guides/lease-session.md`](../guides/lease-session.md).

1. **Why.** Cairn's leases assume that the nodes' monotonic clocks run within 1.5× of each other:
   the owner's grant is ⅔·TTL by its own clock, an observer expires it after one TTL by its own.
   Read/write safety does not rest on this (`writer_epoch`, log sealing, read fences), but ranges
   moving at the right time and the coordinator's rules do.
2. **Measured without extra requests.** Every S3 answer carries a `Date` header (the store server's
   time). The node samples `Date` from requests it makes anyway (at most one sample every 5 s) and
   compares the elapsed time on its monotonic clock with the elapsed time by `Date`, over a 5-minute
   window. The comparison yields a certain interval: second resolution and request latency only
   widen the interval, they cannot cause a false alarm.
3. **Stepping down.** The whole interval outside ±10% ⇒ the node gives up the coordinator role
   (and does not take it back), refuses to take ranges, publishes `draining=1` and hands over every
   range it holds. `cairnflow_clock_rate_untrusted` = 1, `cairnflow_clock_rate_stepdowns_total`
   grows. ±10% because two nodes both within the threshold differ from each other by at most
   ≈ 1.22×.
4. **Recovery.** The whole interval within ±5% ⇒ the node stops draining and takes back its fair
   share of ranges.
5. **Safety net.** No other node can take ranges ⇒ the node keeps them (releasing would be a
   self-inflicted outage) and only logs an `error` once a minute. Stores without a `Date` header
   (`memory://`, `file://`) are not measured.

## Diagnosis

1. **`cairnflow_clock_rate` of that node** (1.000 = exact). Above 1 = the node's clock runs faster
   than the store; below 1 = slower.
2. **One node or every node?**
   - **Every node** off in the same direction ⇒ the **store's** clock is at fault (the store server
     jumped in time, the store's NTP made a large step). A jump of X seconds at the store skews a
     5-minute window by X/300; it clears after one window. Check NTP on the store servers.
   - **One node** ⇒ that machine: a VM paused/migrated, a broken kernel clock source
     (`clocksource`), heavy CPU steal delaying the loop that reads the clock.
3. **Node log:** the `error` line with `rate` (lower and upper bound of the measured rate).

## Handling

- Fix the machine (NTP, `clocksource`, move the VM). The node takes ranges back by itself when the
  measured rate is back within ±5% — at least one 5-minute window after the clock is healthy.
- There is no flag to turn the measurement off, and you should not look for a way to: a node with a
  skewed rate that keeps leading is exactly the failure the lease assumes cannot happen.
- A single node (a one-node cluster) with a skewed rate keeps serving — fix the machine, then
  restart.
