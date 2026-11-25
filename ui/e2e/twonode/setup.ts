// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Global setup of the two-node row: wait for every range to have a leader, register the
// sample service, create a stuck invocation (`Shop/nap`) **on the node leading its range**
// (sent without asking for forwarding: whichever node accepts it locally is the leader), then record
// for the tests: the id, and that the node serving the console is the **other** one.
import type { FullConfig } from "@playwright/test";
import { startFixtureService } from "../fixture/service";
import { TWONODE_A, TWONODE_B } from "./ports";

/** Invocation tag for the bulk case. */
export const BULK_TAG = "twonode=bulk";

export type TwoNodeSeed = { id: string; console: "a" | "b"; cancelId: string; logId: string };

const url = (n: "a" | "b") => `http://127.0.0.1:${n === "a" ? TWONODE_A : TWONODE_B}`;

async function until<T>(what: string, f: () => Promise<T | undefined>, ms = 60_000): Promise<T> {
  const end = Date.now() + ms;
  for (;;) {
    const v = await f().catch(() => undefined);
    if (v !== undefined) return v;
    if (Date.now() > end) throw new Error(`two-node global setup: timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 200));
  }
}

/** Sends `path` to node `n` **without** asking for forwarding; `undefined` on `503`. */
async function sendLocal(
  n: "a" | "b",
  path: string,
  headers: Record<string, string> = {},
): Promise<string | undefined> {
  const r = await fetch(`${url(n)}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: "null",
  });
  if (r.status === 503) return undefined;
  if (r.status >= 300) throw new Error(`POST ${path} @${n}: ${r.status} ${await r.text()}`);
  return r.headers.get("x-cairn-invocation-id") ?? undefined;
}

async function suspended(n: "a" | "b", id: string) {
  await until(`${id} suspended`, async () => {
    const r = await fetch(`${url(n)}/invocations/${id}`);
    const s = (await r.json()) as { status?: string };
    return s.status === "suspended" ? true : undefined;
  });
}

export default async function setup(_: FullConfig) {
  for (const n of ["a", "b"] as const)
    await until(`node ${n} alive`, async () =>
      (await fetch(`${url(n)}/health`)).ok ? true : undefined,
    );
  await until("every range has a leader", async () => {
    const r = (await (await fetch(`${url("a")}/cluster`)).json()) as {
      partitions: { leader?: string | null }[];
    };
    return r.partitions.length > 0 && r.partitions.every((p) => p.leader) ? true : undefined;
  });
  const sdk = await startFixtureService();
  const reg = await fetch(`${url("a")}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: sdk.url }),
  });
  if (!reg.ok) throw new Error(`registering the sample service: ${reg.status} ${await reg.text()}`);
  for (const n of ["a", "b"] as const)
    await until(`node ${n} sees Shop`, async () => {
      const s = (await (await fetch(`${url(n)}/services`)).json()) as { name?: string }[];
      return s.some((x) => x.name === "Shop") ? true : undefined;
    });

  // Two stuck invocations on the same leading node: one for "kill now", one for
  // "cancel" in the bulk action — the console is opened on the other node.
  const placed = await until("a node accepting Shop/nap locally", async () => {
    for (const n of ["a", "b"] as const) {
      const first = await sendLocal(n, "/invoke/Shop/nap/send");
      if (!first) continue;
      const second = await until(`node ${n} accepting one more`, () =>
        sendLocal(n, "/invoke/Shop/nap/send", { "cairnflow-tag": BULK_TAG }),
      );
      const third = await until(`node ${n} accepting a Shop/napLog`, () =>
        sendLocal(n, "/invoke/Shop/napLog/send"),
      );
      return { leader: n, ids: [first, second, third] };
    }
    return undefined;
  });
  for (const id of placed.ids) await suspended(placed.leader, id);
  const seed: TwoNodeSeed = {
    id: placed.ids[0],
    cancelId: placed.ids[1],
    logId: placed.ids[2],
    console: placed.leader === "a" ? "b" : "a",
  };
  process.env.CAIRN_E2E_TWONODE_SEED = JSON.stringify(seed);
  return async () => {
    await sdk.close();
  };
}
