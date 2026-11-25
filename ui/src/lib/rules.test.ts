// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { blockedReasons, propagation, type RuleForm, ruleBody } from "./rules";

const form = (p: Partial<RuleForm>): RuleForm => ({
  target: "acme/growth",
  forTenant: false,
  concurrency: "",
  rate: "",
  burst: "",
  description: "",
  disabled: false,
  ...p,
});

describe("ruleBody", () => {
  it("empty fields are not sent (merge: left untouched); numbers and unlimited", () => {
    expect(ruleBody(form({ concurrency: "5", rate: " Unlimited " }))).toEqual({
      body: {
        pattern: "acme/growth",
        concurrency: 5,
        rate: "unlimited",
        description: "",
        disabled: false,
      },
    });
    expect(ruleBody(form({ burst: "100", description: "x", disabled: true }))).toEqual({
      body: { pattern: "acme/growth", burst: 100, description: "x", disabled: true },
    });
  });

  it("tenant limit: sends `tenant`, does not accept concurrency", () => {
    expect(ruleBody(form({ target: "acme", forTenant: true, rate: "50" }))).toEqual({
      body: { tenant: "acme", rate: 50, description: "", disabled: false },
    });
    expect(ruleBody(form({ target: "acme", forTenant: true, concurrency: "3" }))).toEqual({
      errors: { concurrency: "tenantConcurrency" },
    });
  });

  it("edit dialog: a field that was set and is left empty ⇒ null (reset to unset)", () => {
    expect(
      ruleBody(form({ rate: "", burst: "", wasSet: { concurrency: true, rate: true } })),
    ).toEqual({
      body: {
        pattern: "acme/growth",
        concurrency: null,
        rate: null,
        description: "",
        disabled: false,
      },
    });
    expect(
      ruleBody(form({ target: "acme", forTenant: true, wasSet: { rate: true, burst: true } })),
    ).toEqual({
      body: { tenant: "acme", rate: null, burst: null, description: "", disabled: false },
    });
  });

  it("errors per field", () => {
    expect(ruleBody(form({ target: " ", concurrency: "-1", rate: "1.5", burst: "0" }))).toEqual({
      errors: { target: "required", concurrency: "number", rate: "number", burst: "number" },
    });
  });
});

describe("propagation", () => {
  it("counts ranges still on the old version", () => {
    expect(propagation(7, [7, 6, 7, 5])).toEqual({ behind: 2, total: 4 });
    expect(propagation(7, [])).toEqual({ behind: 0, total: 0 });
  });
});

describe("blockedReasons", () => {
  it("merges concurrency_*, largest first; empty/null ⇒ []; broken ⇒ null", () => {
    expect(
      blockedReasons(
        '[{"count":4,"resource":"lock"},{"count":662,"resource":"turns"},{"count":1,"resource":"concurrency_l1"},{"count":2,"resource":"concurrency_scope"}]',
      ),
    ).toEqual([
      { reason: "turns", count: 662 },
      { reason: "lock", count: 4 },
      { reason: "concurrency", count: 3 },
    ]);
    expect(blockedReasons(null)).toEqual([]);
    expect(blockedReasons("[]")).toEqual([]);
    expect(blockedReasons("nope")).toBeNull();
    expect(blockedReasons('{"a":1}')).toBeNull();
  });
});
