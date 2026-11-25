// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0
//
// Regenerates the golden vectors the Go SDK tests compare against, from the TypeScript
// SDK (the reference). Run from this directory: `node golden.ts`.

import { writeFileSync } from "node:fs";

// Escapes every non-ASCII character of a JSON text (the files stay ASCII; a JSON reader
// gets the same strings back).
const ascii = (json: string): string =>
  json.replace(/[\u007f-\uffff]/g, (c) => "\\u" + c.charCodeAt(0).toString(16).padStart(4, "0"));
import { manifest, service, workflow } from "../../typescript/src/endpoint.ts";

// A verbatim copy of `makeRandom` in `sdk/typescript/src/handler.ts` (not exported there).
function makeRandom(seed: bigint): () => number {
  let s = seed === 0n ? 0x9e3779b97f4a7c15n : seed;
  const MASK = (1n << 64n) - 1n;
  return () => {
    s ^= (s >> 12n) & MASK;
    s = (s ^ ((s << 25n) & MASK)) & MASK;
    s ^= (s >> 27n) & MASK;
    const v = (s * 0x2545f4914f6cdd1dn) & MASK;
    return Number(v >> 11n) / 2 ** 53;
  };
}

const seeds = [0n, 1n, 42n, 0xdeadbeefcafebaben, (1n << 64n) - 1n];
const rng = seeds.map((s) => {
  const r = makeRandom(s);
  return { seed: s.toString(), floats: Array.from({ length: 6 }, () => r()) };
});
writeFileSync("../internal/core/testdata/rng.json", ascii(JSON.stringify(rng, null, 1)) + "\n");

// Each case: the value (as JSON the Go test decodes into `any`, or a string) and the bytes
// `JSON.stringify` produces for it.
const values: unknown[] = [
  null, true, 0, -0, 1, -17, 0.1, 1.5, 1e21, 1e-7, 123456789.125, 9007199254740991, 1e300, 5e-324,
  "", "plain", '<a href="x">&amp;</a>', "line\u2028sep\u2029par", "tab\tnl\nret\r", "\b\f\u0001\u001f\u007f",
  "back\\slash \\u2028 literal", "\u00e9 \u4e2d\u6587 \u{1f600}", [], [1, "two", null], { a: 1, b: [true, false], c: { d: "e" } },
];
writeFileSync(
  "../internal/core/testdata/json.json",
  ascii(JSON.stringify(values.map((v) => ({ value: v, negativeZero: Object.is(v, -0), json: JSON.stringify(v) })), null, 1)) + "\n",
);

const h = async () => null;
const greeter = service("Greeter", { greet: h, hello: h }, {
  options: { inactivityTimeout: 60000, retryPolicy: { maxAttempts: 5, onMaxAttempts: "PAUSE" } },
  handlerOptions: {
    greet: {
      journalRetention: 86400000,
      trace: "runtime",
      logFlushMs: 500,
      retryPolicy: { initialInterval: 100, maxInterval: 10000, exponentiationFactor: 1.5 },
    },
  },
});
const counter = service("Counter", { add: h, inc: h, peek: h }, {
  type: "VIRTUAL_OBJECT",
  handlerTypes: { inc: "EXCLUSIVE", peek: "SHARED" },
});
const signup = workflow("Signup", { run: h, click: h }, { options: { journalRetention: 3600000 } });
writeFileSync(
  "manifest.json",
  JSON.stringify({
    rr: JSON.stringify(manifest([greeter, counter, signup])),
    bidi: JSON.stringify(manifest([greeter], "BIDI_STREAM", 6, true, true, true)),
  }, null, 1) + "\n",
);
