// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The harness derives the runtime's identity keys itself; it must agree with `identity.rs`.
import assert from "node:assert/strict";
import { test } from "node:test";
import { kidOf } from "./identity.ts";

test("kidOf matches the runtime's golden vector (identity.rs golden_vector_is_stable)", () => {
  assert.equal(
    kidOf("cf_test_golden_vector_master_key", "acme"),
    "publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr",
  );
  assert.notEqual(kidOf("cf_test_golden_vector_master_key", "acme", 2), kidOf("cf_test_golden_vector_master_key", "acme"));
});
