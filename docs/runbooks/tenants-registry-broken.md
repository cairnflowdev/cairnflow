# Runbook — the `tenants` registry is broken (nodes keep the old registry)

**Alert:** `CairnflowTenantsBookRejected` (`cairnflow_tenants_book_rejected` = 1 for more than 2
minutes).

## Mechanism

Code: `crates/cairn-server/src/tenants.rs` (`decode`, `TenantSettings::load`).

- The object `<prefix>/tenants` holds the cluster's settings and **per-tenant limits**:
  `retention-ms=`, `max-payload=`, `max-journal=`, `deploy-rate=N/S` (together with the older flags
  `hash-chain`, `slow-ms=`, `quarantine=`, `quarantine-rule=`).
- One broken line, or a limit with an unknown value (`max-payload=1kb`, `deploy-rate=0/60`), fails
  **the whole** decode. The node **keeps the registry from its last good read**: limits already in
  force still apply, new ones do not yet. Silently skipping an unknown value would remove a tenant's
  limit exactly when nobody is looking — this is how the node fails safe.
- While the registry is broken: every `PUT /admin/tenants/{t}` and `GET /admin/tenants` returns `500`
  (they re-read the registry before writing); a **newly starting** node cannot come up (reading the
  registry is a startup step).
- `cairnflow_tenants_book_rejected_total` counts rejected reads. The node logs at level `error`:
  "tenants book corrupt, keeping the one in use" with the broken line.

`PUT /admin/tenants/{t}` validates values before writing (`400`), so the registry only breaks when
someone writes to the bucket by hand or when a **newer** binary writes a value form this binary does
not understand yet.

## Diagnosis

1. Look for the broken line in the node log, or read the object directly:

   ```sh
   aws s3 cp s3://<bucket>/<prefix>/tenants -
   ```

2. Valid value forms:

   | Field | Form |
   |---|---|
   | `retention-ms=` | a number of ms, `0` = keep forever |
   | `max-payload=` | a number of bytes ≥ 1 |
   | `max-journal=` | a number of entries ≥ 1 |
   | `deploy-rate=` | `N/S`, both ≥ 1 |

3. Is the cluster in a rolling upgrade? An old binary does not understand a limit a new binary just
   wrote: see [upgrade-and-rollback.md](./upgrade-and-rollback.md).

## Fix

Write the object again with valid values (keep the header `cairnflow-tenants/1` on the first line),
then wait one registry refresh tick (`--deploy-refresh-ms`, default 30 s) or call
`GET /admin/tenants` to make the node re-read it at once. The gauge goes back to `0` once it reads.
