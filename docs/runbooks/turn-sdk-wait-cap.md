# Runbook — turns cut by the SDK wait cap

**Alert:** `CairnflowSdkTimeouts`.

## Mechanism

Code: `crates/cairn-server/src/turn/mod.rs` (`sdk_timed_out`), `crates/cairn-server/src/turn/sdk.rs`
(`drive_one` picks the cap, `drive_rr`, `drive_bidi`).

- Each turn has an **SDK wait cap**: the `inactivityTimeout` of the handler's contract (the
  TypeScript SDK declares 15 minutes at service level when the user declares nothing), otherwise the
  node's `--inactivity-timeout-ms` (default 60 s). The cap is clamped to `--inactivity-timeout-max-ms`
  (default 1 hour; `--profile saas`: 15 minutes).
- `BIDI_STREAM` restarts the cap at every frame (the SDK sends `Keepalive` while a `ctx.run` is still
  running), so only a truly silent SDK is cut. `REQUEST_RESPONSE` has no signal midway: the cap wraps
  **the whole** turn, so a `ctx.run` longer than the cap (a model call of several minutes) is cut.
- A cut turn is a **retryable** failed attempt: the journal is intact, the next turn reruns the
  unfinished step from its start (a `ctx.run` without a result runs again — for a model call step,
  that means paying for the tokens again). Without `retryPolicy.maxAttempts` the number of attempts is
  unlimited; the SDK's `chatObject` declares 5.
- The node increments `cairnflow_sdk_timeouts_total` and logs `warn` "SDK made no progress within the
  allowed limit — turn dropped, retry scheduled" (with `invocation`, `mode`, `limit_ms`). The
  invocation's `lastFailure` carries code 500 and a matching message.

## Diagnosis

```sh
curl -s -H "authorization: Bearer $CAIRNFLOW_TOKEN" $CAIRNFLOW_SERVER/invocations/<id>
# lastFailure.code 500, "SDK made no progress in N ms (REQUEST_RESPONSE) — turn dropped"; attempt grows
```

Look at `limit_ms` in the log: 60000 means the contract declares no cap and the node uses its default
(an old SDK, or an SDK other than TypeScript); 900000 on a node running `--profile saas` means the
step is longer than 15 minutes.

## Handling

1. **A step longer than the cap is intended** (a slow model call, a slow API): declare an
   `inactivityTimeout` on the service or handler that is enough for the longest step — up to the
   node's maximum. With `--profile saas` the maximum is 15 minutes: longer steps must be split into
   several `ctx.run`s, or run in a separate invocation (`ctx.send`) whose result is awaited through an
   awakeable.
2. **An old SDK** (no default cap declared): upgrade the SDK, or declare `inactivityTimeout` by hand.
3. **A hanging endpoint** (no answer, a proxy buffering the body): the alert comes with
   `cairnflow_sdk_timeouts_total` growing for every handler of one deployment — check the endpoint and
   the proxy in front of it. On a multi-tenant node, a tenant with a hanging endpoint only holds up to
   its share of slots (`--max-turns-per-tenant`, `--profile saas`: ¼ of the node's slots); other tenants
   keep running.
4. An invocation that has rerun too many times: `POST /invocations/{id}/cancel`, or let it reach
   `retryPolicy.maxAttempts` (it goes to `paused`), then fix and `resume`.
