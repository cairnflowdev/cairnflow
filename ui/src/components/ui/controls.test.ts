// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Token rules for controls: control borders are `--input` (≥ 3:1, WCAG 1.4.11),
// `--border` is only for decorative rules; disabled controls use a `muted` background +
// `muted-foreground` text, never dimmed with opacity; overlays have a border and a shadow.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { buttonVariants } from "./button";

const SRC = new URL("../../", import.meta.url).pathname;
const read = (rel: string) => readFileSync(join(SRC, rel), "utf8");

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((n) => {
    const p = join(dir, n);
    if (statSync(p).isDirectory()) return n === "paraglide" ? [] : sources(p);
    return /\.tsx$/.test(n) && !/\.(test|stories)\.tsx$/.test(n) ? [p] : [];
  });
}

const VARIANTS = ["default", "outline", "secondary", "ghost", "destructive", "link"] as const;

describe("buttons", () => {
  for (const variant of ["outline", "secondary"] as const) {
    it(`${variant}: border is --input`, () => {
      const cls = buttonVariants({ variant }).split(/\s+/);
      expect(cls).toContain("border-input");
      expect(cls).not.toContain("border-border");
    });
  }
  for (const variant of VARIANTS) {
    it(`${variant}: disabled ⇒ muted-foreground text`, () => {
      expect(buttonVariants({ variant }).split(/\s+/)).toContain("disabled:text-muted-foreground");
    });
  }
});

it("no control is disabled through opacity", () => {
  const bad = sources(SRC).flatMap((f) =>
    [...readFileSync(f, "utf8").matchAll(/\S*disabled[^\s"'`:]*:opacity-\d+/g)].map(
      (m) => `${f.slice(SRC.length)}: ${m[0]}`,
    ),
  );
  expect(bad).toEqual([]);
});

describe("overlays have a --border border and a shadow", () => {
  for (const f of ["popover", "dropdown-menu", "dialog"]) {
    it(`${f}`, () => {
      const popup = read(`components/ui/${f}.tsx`)
        .split("\n")
        .find((l) => l.includes("bg-popover"));
      expect(popup).toMatch(/\bborder border-border\b/);
      expect(popup).toMatch(/\bshadow-(?:md|lg|xl)\b/);
    });
  }
});

it("skeleton uses the --skeleton token", () => {
  expect(read("components/cairn/skeleton.tsx")).toContain("bg-skeleton");
});
