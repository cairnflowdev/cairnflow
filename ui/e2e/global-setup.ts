// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// E2E global setup: runs the sample service, registers it on the
// `noauth` node, then creates invocations of every class (table in `fixture/service.ts`) and waits for
// each to reach its class. The `auth` node is left empty — the onboarding screen.
import type { FullConfig } from "@playwright/test";
import { startFakeIssuer } from "./fixture/oidc";
import { startFixtureService } from "./fixture/service";
import { E2E_ADMIN_KEY, E2E_PORT, E2E_PORTAL_URL, E2E_SSO_PORT, E2E_SSO_TENANT } from "./ports";

const BASE = `http://127.0.0.1:${E2E_PORT}`;

/** Extra invocations for measuring INP — together with the sample classes, > 250 root rows. */
export const BULK = 260;

/** Sample invocation ids by role — tests read them back via `process.env.CAIRN_E2E_SEED`. */
export type Seed = Record<
  | "completed"
  | "failed"
  | "paused"
  | "retrying"
  | "sleeping"
  | "parent"
  | "awakeable"
  | "promise"
  | "holder"
  | "queued"
  // Has a per-attempt timeline (failed the first time) / has a durable stream — axe in both themes.
  | "traced"
  | "streamed"
  // URL of the sample service — the "Safe deploy" case registers it on the `auth` node.
  | "sdk",
  string
>;

async function until<T>(what: string, f: () => Promise<T | undefined>, ms = 30_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await f().catch(() => undefined);
    if (v !== undefined) return v;
    if (Date.now() > end) throw new Error(`global setup: timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function send(
  path: string,
  body: unknown = null,
  headers: Record<string, string> = {},
): Promise<string> {
  const r = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  if (r.status >= 300) throw new Error(`POST ${path}: ${r.status} ${await r.text()}`);
  const id = r.headers.get("x-cairn-invocation-id");
  if (!id) throw new Error(`POST ${path}: missing x-cairn-invocation-id`);
  return id;
}

type Status = { status: string; lastFailure?: unknown };

async function status(id: string): Promise<Status> {
  const r = await fetch(`${BASE}/invocations/${id}`);
  return (await r.json()) as Status;
}

const reached = (id: string, ok: (s: Status) => boolean) =>
  until(`${id} reaching the expected class`, async () => {
    const s = await status(id);
    return ok(s) ? s : undefined;
  });

export default async function globalSetup(_: FullConfig) {
  await until(
    "node noauth alive",
    async () => ((await fetch(`${BASE}/health`)).ok ? true : undefined),
    60_000,
  );
  const sdk = await startFixtureService();
  const reg = await fetch(`${BASE}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdk.url }),
  });
  if (!reg.ok) throw new Error(`registering the sample service: ${reg.status} ${await reg.text()}`);

  const tag = { "cairnflow-tag": "env=e2e" };
  const seed: Seed = {
    completed: await send("/invoke/Shop/ok/send", { amount: 42 }, tag),
    failed: await send("/invoke/Shop/decline/send"),
    paused: await send("/invoke/Shop/flaky/send"),
    retrying: await send("/invoke/Shop/retrying/send"),
    sleeping: await send("/invoke/Shop/nap/send"),
    parent: await send("/invoke/Shop/parent/send"),
    awakeable: await send("/invoke/Shop/waitAwk/send"),
    promise: await send("/invoke/Approval/run/send?key=order-7"),
    holder: await send("/invoke/Cart/hold/send?key=cart-1"),
    queued: "",
    traced: await send("/invoke/Shop/traced/send"),
    streamed: await send("/invoke/Shop/streamed/send"),
    sdk: sdk.url,
  };
  // A list long enough to measure INP over 250 rows: BULK calls of `Shop/ok`,
  // sent in batches of 25.
  for (let i = 0; i < BULK; i += 25) {
    await Promise.all(
      Array.from({ length: Math.min(25, BULK - i) }, (_, j) =>
        send("/invoke/Shop/ok/send", { amount: i + j }),
      ),
    );
  }
  await reached(seed.holder, (s) => s.status === "suspended");
  seed.queued = await send("/invoke/Cart/hold/send?key=cart-1");

  await reached(seed.completed, (s) => s.status === "completed");
  await reached(seed.traced, (s) => s.status === "completed");
  await reached(seed.streamed, (s) => s.status === "completed");
  await reached(seed.parent, (s) => s.status === "completed");
  await reached(seed.failed, (s) => s.status === "failed");
  await reached(seed.paused, (s) => s.status === "paused");
  await reached(seed.retrying, (s) => s.lastFailure !== undefined);
  for (const id of [seed.sleeping, seed.awakeable, seed.promise])
    await reached(id, (s) => s.status === "suspended");

  process.env.CAIRN_E2E_SEED = JSON.stringify(seed);

  // The `sso` node: fake issuer + client `localhost` → `acme`, set through
  // the `Provision` API as the control plane does.
  const issuer = await startFakeIssuer();
  const sso = `http://127.0.0.1:${E2E_SSO_PORT}`;
  const admin = async (method: string, path: string, body: unknown) => {
    const r = await fetch(`${sso}${path}`, {
      method,
      headers: { "content-type": "application/json", authorization: `Bearer ${E2E_ADMIN_KEY}` },
      body: JSON.stringify(body),
    });
    if (r.status !== 204) throw new Error(`${method} ${path}: ${r.status} ${await r.text()}`);
  };
  await admin("PUT", "/admin/oidc", { issuer: issuer.url, displayName: "Cairnflow Cloud" });
  await admin("PUT", "/admin/oidc/clients/env_e2e", {
    hostname: "localhost",
    tenant: E2E_SSO_TENANT,
    maxRole: "admin",
    portalUrl: E2E_PORTAL_URL,
  });
  process.env.CAIRN_E2E_ISSUER = issuer.url;
  return async () => {
    await sdk.close();
    await issuer.close();
  };
}
