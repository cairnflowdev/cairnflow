# Runbook — spike in authentication failures

**Alert:** `CairnflowAuthFailuresSpike` (`cairnflow_auth_failures_total{kind!="missing"}` above
5 per second throughout 10 minutes).

## Mechanism

Code: `crates/cairn-server/src/metrics.rs` (`auth_failures`), `auth.rs::authenticate`, `ui.rs`
(`session::login`).

- `kind="bearer"`: an API request carrying `Authorization: Bearer` with an invalid key (wrong,
  revoked, of a deleted tenant) ⇒ `401`.
- `kind="cookie"`: a console session cookie that is wrong, expired or revoked ⇒ `401`.
- `kind="missing"`: a route that needs a key, and the request carries none (not counted by the
  alert — browsers and scanners hit this all the time).
- `kind="login"`: `POST <base>/session` with a wrong key. Logging in with a key **is** limited per
  IP and per key prefix (`429`).
- Bearer `401`s are **not** blocked per IP: a 256-bit key cannot be guessed, and behind a platform
  proxy one IP is many clients behind NAT. The node only counts and logs an aggregate once a minute
  (level `warn`, "ingress authentication failures", one column per kind).
- SSO login has its own limits: `/login/oidc` 20 times per client, then once every 3 s; **failed**
  callbacks 10 times per client, then once every 6 s; calls to the issuer's token endpoint 20 times
  per OIDC client, then once per second (message `this SSO client is exchanging too many codes`),
  and from the whole node 200 times, then 10 per second (`node is exchanging too many SSO codes`);
  the flow cookie is single-use. IPv6 is grouped by /64.

## Diagnosis

1. Which kind grows: `sum by (kind) (rate(cairnflow_auth_failures_total[5m]))`.
2. `bearer` grows steadily at one rate: usually an SDK or a job still using a revoked key. Look at
   the access log of the proxy in front (IP, user agent) — the node does not log individual
   requests.
3. `login` grows: someone is probing keys in the console; the per-IP and per-prefix limits are
   blocking it (`429`).

## Fix

- A client's old key: tell the client to rotate the key (`cairn-ctl keys` / the console).
- A probing wave from one network range: block it at the proxy in front (load balancer, CDN), not
  at the node.
