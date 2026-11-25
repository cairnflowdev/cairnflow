// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULTS, parseThresholds, STORAGE_KEY, saveThresholds } from "./thresholds";

describe("thresholds", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
  });

  it("parse: valid values kept, each broken/missing field ⇒ default", () => {
    expect(parseThresholds(null)).toEqual(DEFAULTS);
    expect(parseThresholds("{bad")).toEqual(DEFAULTS);
    expect(parseThresholds('{"failedRate":0.1,"minRuns":5}')).toEqual({
      failedRate: 0.1,
      minRuns: 5,
    });
    expect(parseThresholds('{"failedRate":2,"minRuns":-1}')).toEqual(DEFAULTS);
    expect(parseThresholds('{"minRuns":3.5}')).toEqual(DEFAULTS);
  });

  it("save and reset to defaults", () => {
    expect(saveThresholds({ failedRate: 0.05, minRuns: 10 })).toBe(true);
    expect(parseThresholds(localStorage.getItem(STORAGE_KEY))).toEqual({
      failedRate: 0.05,
      minRuns: 10,
    });
    saveThresholds(null);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
  });

  it("storage throws ⇒ does not break, reports it could not save", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    });
    expect(saveThresholds({ failedRate: 0.1, minRuns: 1 })).toBe(false);
  });
});
