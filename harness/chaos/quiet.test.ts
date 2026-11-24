// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Unit tests of the pooled quiet-tenant check (invariant 6 of the chaos harness).
import assert from "node:assert/strict";
import { test } from "node:test";
import { QuietPool, quantile } from "./quiet.ts";

const limitOf = (base: number[]) => Math.max(8_000, Math.round(1.5 * quantile(base, 0.99)));
/** `n` calls of `ms`. */
const calls = (n: number, ms: number) => Array.from({ length: n }, () => ms);

test("quantile: nearest rank, empty list is 0", () => {
  assert.equal(quantile([], 0.99), 0);
  assert.equal(quantile([3, 1, 2], 0.5), 2);
  assert.equal(quantile(calls(99, 10).concat([1_000]), 0.99), 1_000);
});

test("a window with few quiet calls is not judged on its own", () => {
  const pool = new QuietPool(1_000, limitOf);
  // ~160 calls, two slow ones: alone, its p99 (34 s) is over 1.5 × the baseline p99 (20 s).
  const quiet = calls(158, 600).concat([34_000, 60_000]);
  const base = calls(990, 600).concat(calls(10, 20_000));
  assert.ok(quantile(quiet, 0.99) > limitOf(base), "the window alone would fail");
  assert.equal(pool.add(quiet, base), undefined);
});

test("pooled windows with a few slow quiet calls pass", () => {
  const pool = new QuietPool(1_000, limitOf);
  const base = calls(990, 600).concat(calls(10, 20_000));
  let verdict;
  for (let w = 0; w < 7 && verdict === undefined; w++) {
    const quiet = calls(158, 600).concat(w === 0 ? [34_000, 60_000] : calls(2, 900));
    verdict = pool.add(quiet, base);
  }
  assert.ok(verdict, "seven windows of 160 calls reach 1 000");
  assert.equal(verdict.windows, 7);
  assert.equal(verdict.quiet, 1_120);
  assert.equal(verdict.fails, false);
});

test("a quiet tenant slowed for the whole pool fails", () => {
  const pool = new QuietPool(1_000, limitOf);
  const base = calls(990, 600).concat(calls(10, 20_000));
  let verdict;
  for (let w = 0; w < 10 && verdict === undefined; w++) {
    // 5 % of the quiet calls take 40 s: over 1.5 × 20 s at p99.
    verdict = pool.add(calls(152, 600).concat(calls(8, 40_000)), base);
  }
  assert.ok(verdict);
  assert.equal(verdict.fails, true);
  assert.equal(verdict.p99, 40_000);
  assert.equal(verdict.limit, 30_000);
});

test("the pool starts again after each verdict", () => {
  const pool = new QuietPool(200, limitOf);
  const base = calls(1_000, 600);
  assert.equal(pool.add(calls(150, 600), base), undefined);
  const first = pool.add(calls(150, 600), base);
  assert.equal(first?.quiet, 300);
  assert.equal(first?.windows, 2);
  assert.equal(pool.add(calls(150, 600), base), undefined);
});
