// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Example: a virtual object using `ctx.get`, `ctx.sleep`, `ctx.run`, `ctx.set`.
 *
 * ```sh
 * node example/order-service.ts                     # SDK on :9080
 * cargo run --bin cairnflow -- --data-dir ./data --no-auth   # runtime on :8080 (dev)
 *
 * # `-H content-type` is required: the runtime rejects bodies not declared as JSON,
 * # and curl sends `application/x-www-form-urlencoded` by default.
 * curl -X POST localhost:8080/deployments -H 'content-type: application/json' \
 *      -d '{"uri":"http://127.0.0.1:9080"}'
 * curl -X POST 'localhost:8080/invoke/OrderService/submit?key=order-42' \
 *      -H 'content-type: application/json' -d '{"amount": 100}'
 * curl -X POST 'localhost:8080/invoke/OrderService/total?key=order-42'
 * curl localhost:8080/invocations/<id>
 * ```
 *
 * `ctx.sleep` fires on its own — the runtime's background tick takes care of it, no
 * need to call `/admin/tick` by hand. (That endpoint exists, and is used to push timers in tests.)
 */

import { type Context, serve, service, TerminalError } from "../src/index.ts";

interface Order {
  readonly amount: number;
}

/** A real side effect: may only run through `ctx.run` (protocol-v1 §8.4). */
async function chargeCard(amount: number): Promise<string> {
  await new Promise((r) => setTimeout(r, 5));
  return `receipt-${amount}`;
}

const orders = service(
  "OrderService",
  {
    async submit(ctx: Context, input: Order) {
      if (!input || typeof input.amount !== "number" || input.amount <= 0) {
        // TerminalError: bad data stays bad no matter how many times it is retried.
        throw new TerminalError("amount must be a positive number", 400);
      }

      const soFar = (await ctx.get<number>("total")) ?? 0;

      // A real wait, really durable: the runtime writes the timer into the state table, not RAM.
      await ctx.sleep(1_000, "await confirmation");

      // Runs exactly once per durable result — even if the process dies and the
      // invocation is rerun on another node.
      const receipt = await ctx.run("charge", () => chargeCard(input.amount));

      ctx.set("total", soFar + input.amount);
      return { receipt, total: soFar + input.amount, order: ctx.key };
    },

    async total(ctx: Context) {
      return (await ctx.get<number>("total")) ?? 0;
    },
  },
  {
    type: "VIRTUAL_OBJECT",
    handlerTypes: { submit: "EXCLUSIVE", total: "SHARED" },
  },
);

const port = Number(process.env.PORT ?? 9080);
const endpoint = await serve({ services: [orders], port });
console.log(`OrderService listening on ${endpoint.url}`);
