// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Type tests of `connect()`: this file only has to **pass `tsc`**
 * (`npm run typecheck`, the `tsc` step of `check.sh`) — it is not run. Each
 * `@ts-expect-error` is a mistyped call that `tsc` must catch; once that line stops
 * being an error, `tsc` goes red.
 */

import { connect, type InvocationHandle } from "../src/client.ts";
import { type Context, service, workflow } from "../src/index.ts";

const orders = service("OrderService", {
  async submit(_ctx, input: { amount: number }) {
    return { ok: input.amount > 0 };
  },
  async ping() {
    return "pong";
  },
});

const cart = service(
  "Cart",
  {
    async add(ctx: Context, item: string) {
      ctx.set("last", item);
      return 1;
    },
    async send(_ctx: Context, msg: { text: string }) {
      return msg.text.length;
    },
  },
  { type: "VIRTUAL_OBJECT" },
);

const signup = workflow("Signup", {
  async run(_ctx, user: { email: string }) {
    return user.email.includes("@");
  },
  async click(_ctx, secret: string) {
    return secret;
  },
});

export async function typeChecks(): Promise<void> {
  const c = connect({ url: "http://localhost:8080", apiKey: "cf_x" });
  // The README's two patterns under `exactOptionalPropertyTypes`; passing `string | undefined` directly does not compile.
  connect({ url: "http://localhost:8080", apiKey: process.env.CAIRN_KEY! });
  connect({ url: "http://localhost:8080", ...(process.env.CAIRN_KEY ? { apiKey: process.env.CAIRN_KEY } : {}) });
  // @ts-expect-error `string | undefined` is not assignable to `apiKey?: string`
  connect({ url: "http://localhost:8080", apiKey: process.env.CAIRN_KEY });
  const o = c.service<typeof orders>("OrderService");

  const r: { ok: boolean } = await o.submit({ amount: 5 });
  const p: string = await o.ping();
  // @ts-expect-error wrong input type
  await o.submit({ amount: "5" });
  // @ts-expect-error missing required input
  await o.submit();
  // @ts-expect-error no such handler
  await o.refund({ amount: 1 });
  // @ts-expect-error output is not a string
  const wrong: string = await o.submit({ amount: 1 });

  const h: InvocationHandle<{ ok: boolean }> = await o.send.submit({ amount: 5 }, { idempotencyKey: "k1" });
  const { id } = await o.send.submit({ amount: 5 });
  const same: { ok: boolean } = await c.invocation<{ ok: boolean }>(id).attach({ timeoutMs: 1000 });
  // @ts-expect-error `timeoutMs` does not exist on a send
  await o.send.submit({ amount: 5 }, { timeoutMs: 1 });
  await o.submit({ amount: 5 }, { logLevel: "debug" });
  // @ts-expect-error the runtime only accepts debug|info|warn|error in `cairnflow-log-level` (anything else ⇒ 400)
  await o.submit({ amount: 5 }, { logLevel: "trace" });
  // @ts-expect-error as above, on a send too
  await o.send.submit({ amount: 5 }, { logLevel: "trace" });

  const k = c.object<typeof cart>("Cart", "u1");
  const n: number = await k.add("apple", { idempotencyKey: null });
  // A handler named `send`: called directly, and `send.<h>` is still a send.
  const len: number = await k.send({ text: "hi" });
  await k.send.add("pear");
  // @ts-expect-error wrong input type for the `send` handler
  await k.send({ text: 1 });

  const wf = c.workflow<typeof signup>("Signup", "u1");
  const done: boolean = await wf.run({ email: "a@b" });
  const again: boolean = await wf.attach();
  const out = await wf.output();
  if (out.ready) {
    const v: boolean = out.value;
    void v;
  }
  const s: string = await wf.click("secret");

  const inv = c.invocation(id);
  await inv.signal("approve", { ok: true });
  await inv.cancel({ mode: "signal" });
  // @ts-expect-error unknown mode
  await inv.cancel({ mode: "soft" });
  await c.awakeable("awk_x").resolve({ ok: true });
  await c.promise("Signup", "u1", "email").resolve("x");

  // No type parameter: any handler name, `unknown` input/output (string index —
  // under `noUncheckedIndexedAccess` it needs a `!`).
  const loose: unknown = await c.service("Any").whatever!(1);
  void [r, p, wrong, h, same, n, len, done, again, s, loose];
}
