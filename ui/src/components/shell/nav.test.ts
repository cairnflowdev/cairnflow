// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import type { Capability } from "@/lib/roles";
import { navFor } from "./nav";

// Sidebar by the capability set of `whoami`. The sets here are sample data as the
// server sends them for each role — the UI does not derive them from the role name.
const pages = (caps: Capability[] | undefined) => navFor(caps).map((n) => n.to);

describe("sidebar by capability", () => {
  it("a `view` key sees the read screens and Schedules, not Flow/Cluster", () => {
    const got = pages(["read"]);
    for (const to of [
      "/",
      "/invocations",
      "/services",
      "/deployments",
      "/query",
      "/schedules",
      "/audit",
    ]) {
      expect(got).toContain(to);
    }
    expect(got).not.toContain("/flow");
    expect(got).not.toContain("/cluster");
  });

  it("an `invoke` key only sees Schedules", () => {
    expect(pages(["invoke"])).toEqual(["/schedules"]);
  });

  it("`admin` sees the same as `view`; `cluster` adds Flow and Cluster", () => {
    expect(pages(["read", "invoke", "operate"])).toEqual(pages(["read"]));
    const all = pages(["read", "invoke", "operate", "provision", "cluster"]);
    expect(all).toContain("/flow");
    expect(all).toContain("/cluster");
  });

  it("no set (old server) ⇒ no items", () => {
    expect(pages(undefined)).toEqual([]);
    expect(pages(["provision"])).toEqual([]);
  });
});
