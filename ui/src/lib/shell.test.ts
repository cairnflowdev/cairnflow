// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { describe, expect, it } from "vitest";
import { parseJump } from "./jump";
import { chord, matcher, type Shortcut } from "./shortcuts";

describe("parseJump", () => {
  it("48 hex ⇒ invocation, dp_ ⇒ deployment, anything else ⇒ null", () => {
    const id = "3FA9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21";
    expect(parseJump(`  ${id} `)).toEqual({ kind: "invocation", id: id.toLowerCase() });
    expect(parseJump("dp_9a12")).toEqual({ kind: "deployment", id: "dp_9a12" });
    expect(parseJump("3fa9")).toBeNull();
    expect(parseJump(`${id}0`)).toBeNull();
    expect(parseJump("invocations")).toBeNull();
    const awk = `awk_${id.toLowerCase()}00000001`;
    expect(parseJump(awk)).toEqual({ kind: "awakeable", id: awk });
    expect(parseJump("awk_123")).toBeNull();
  });
});

describe("shortcut registry", () => {
  const hit: string[] = [];
  const sc = (keys: string): Shortcut => ({ keys, label: () => keys, run: () => hit.push(keys) });
  const all = [sc("mod+k"), sc("/"), sc("?"), sc("g o"), sc("g i")];

  it("normalises keys", () => {
    expect(chord({ key: "k", metaKey: true, ctrlKey: false, altKey: false })).toBe("mod+k");
    expect(chord({ key: "K", metaKey: false, ctrlKey: true, altKey: false })).toBe("mod+k");
    expect(chord({ key: "?", metaKey: false, ctrlKey: false, altKey: false })).toBe("?");
  });

  it("two-key sequence within 1 second; too late ⇒ no match", () => {
    let t = 0;
    const match = matcher(all, () => t);
    expect(match("g")).toBeNull();
    t = 500;
    expect(match("o")?.keys).toBe("g o");
    expect(match("g")).toBeNull();
    t = 2000;
    expect(match("i")).toBeNull();
    expect(match("/")?.keys).toBe("/");
    expect(match("mod+k")?.keys).toBe("mod+k");
  });
});

// A hex id prefix (≥ 6) ⇒ the list filtered by prefix.
describe("Cmd+K accepts id prefixes", () => {
  it("6–47 hex ⇒ a prefix; shorter ⇒ treated as plain text", () => {
    expect(parseJump("819E7f8a2696")).toEqual({ kind: "prefix", id: "819e7f8a2696" });
    expect(parseJump("819e7")).toBeNull();
  });
});
