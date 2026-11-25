// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { diffService, type ManifestService, unchanged } from "./manifest-diff";

const base: ManifestService = {
  name: "Shop",
  ty: "service",
  retryPolicyMaxAttempts: 3,
  handlers: [{ name: "pay", retryPolicyInitialInterval: 100 }, { name: "ok" }],
};

describe("diffService", () => {
  it("new service ⇒ every handler is an addition", () => {
    const d = diffService(undefined, base);
    expect(d.isNew).toBe(true);
    expect(d.handlersAdded).toEqual(["pay", "ok"]);
  });

  it("identical ⇒ unchanged", () => {
    expect(unchanged(diffService(base, structuredClone(base)))).toBe(true);
  });

  it("handlers added/removed, service and handler contract changed", () => {
    const next: ManifestService = {
      ...base,
      retryPolicyMaxAttempts: 5,
      inactivityTimeout: 60_000,
      handlers: [{ name: "pay", retryPolicyInitialInterval: 250 }, { name: "refund" }],
    };
    const d = diffService(base, next);
    expect(d.handlersAdded).toEqual(["refund"]);
    expect(d.handlersRemoved).toEqual(["ok"]);
    expect(d.changed).toEqual([
      { handler: null, field: "inactivityTimeout", from: undefined, to: 60_000 },
      { handler: null, field: "retryPolicyMaxAttempts", from: 3, to: 5 },
      { handler: "pay", field: "retryPolicyInitialInterval", from: 100, to: 250 },
    ]);
    expect(unchanged(d)).toBe(false);
  });

  it("a change of a handler's effective type is a change", () => {
    const old: ManifestService = {
      name: "Cart",
      ty: "virtualObject",
      handlers: [{ name: "get", ty: "exclusive" }],
    };
    const next: ManifestService = { ...old, handlers: [{ name: "get", ty: "shared" }] };
    expect(diffService(old, next).changed).toEqual([
      { handler: "get", field: "ty", from: "exclusive", to: "shared" },
    ]);
  });
});
