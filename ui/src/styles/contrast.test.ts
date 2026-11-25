// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Machine check of the tokens ("measured values" of the theme): reads
// `globals.css`, converts OKLCH to sRGB, asserts every pair still meets the WCAG 2.2 threshold
// in both themes. Changing a token so that contrast slips ⇒ red.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const CSS = readFileSync(new URL("./globals.css", import.meta.url), "utf8");

type Rgb = [number, number, number];

/** The `--x: oklch(L C H)` variables of a `selector { … }` block. */
function tokens(selector: string): Map<string, Rgb> {
  const start = CSS.indexOf(`${selector} {`);
  if (start < 0) throw new Error(`no block ${selector}`);
  const body = CSS.slice(start, CSS.indexOf("\n}", start));
  const out = new Map<string, Rgb>();
  for (const m of body.matchAll(/--([\w-]+):\s*oklch\(([\d.]+)\s+([\d.]+)\s+([\d.]+)\)/g)) {
    out.set(m[1] as string, oklchToSrgb(Number(m[2]), Number(m[3]), Number(m[4])));
  }
  return out;
}

/** OKLCH ⇒ gamma-encoded sRGB, clamped to [0, 1] (CSS Color 4). */
function oklchToSrgb(l: number, c: number, h: number): Rgb {
  const a = c * Math.cos((h * Math.PI) / 180);
  const b = c * Math.sin((h * Math.PI) / 180);
  const l_ = (l + 0.3963377774 * a + 0.2158037573 * b) ** 3;
  const m_ = (l - 0.1055613458 * a - 0.0638541728 * b) ** 3;
  const s_ = (l - 0.0894841775 * a - 1.291485548 * b) ** 3;
  const lin: Rgb = [
    4.0767416621 * l_ - 3.3077115913 * m_ + 0.2309699292 * s_,
    -1.2684380046 * l_ + 2.6097574011 * m_ - 0.3413193965 * s_,
    -0.0041960863 * l_ - 0.7034186147 * m_ + 1.707614701 * s_,
  ];
  return lin.map((x) => {
    const v = Math.min(1, Math.max(0, x));
    return v <= 0.0031308 ? 12.92 * v : 1.055 * v ** (1 / 2.4) - 0.055;
  }) as Rgb;
}

function luminance([r, g, b]: Rgb): number {
  const lin = (v: number) => (v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
  return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);
}

function ratio(a: Rgb, b: Rgb): number {
  const [x, y] = [luminance(a), luminance(b)].sort((p, q) => q - p) as [number, number];
  return (x + 0.05) / (y + 0.05);
}

/** `fg` at alpha `alpha` over `bg` (browsers composite in sRGB). */
function over(fg: Rgb, alpha: number, bg: Rgb): Rgb {
  return fg.map((v, i) => v * alpha + (bg[i] as number) * (1 - alpha)) as Rgb;
}

const THEMES = { light: tokens(":root"), dark: tokens(".dark") } as const;

const STATUSES = ["running", "suspended", "retrying", "paused", "completed", "failed", "scheduled"];

describe.each(Object.entries(THEMES))("contrast — theme %s", (name, t) => {
  const get = (k: string): Rgb => {
    const v = t.get(k);
    if (!v) throw new Error(`${name}: missing --${k}`);
    return v;
  };
  const pair = (fg: string, bg: string, min: number) =>
    it(`${fg} on ${bg} ≥ ${min}`, () => {
      expect(ratio(get(fg), get(bg))).toBeGreaterThanOrEqual(min);
    });

  // Text ≥ 4.5:1 — no text token below the threshold, on background and card.
  for (const fg of ["foreground", "muted-foreground", "tertiary-foreground"]) {
    pair(fg, "background", 4.5);
    pair(fg, "card", 4.5);
  }
  pair("muted-foreground", "muted", 4.5);
  pair("primary-foreground", "primary", 4.5);
  pair("primary-foreground", "primary-hover", 4.5);
  pair("sidebar-foreground", "sidebar", 4.5);
  pair("popover-foreground", "popover", 4.5);
  // Input borders and focus ring ≥ 3:1 (1.4.11).
  pair("input", "background", 3);
  pair("input", "card", 3);
  pair("ring", "background", 3);
  pair("ring", "card", 3);
  // Status badges: text on its own background ≥ 4.5 (always with an icon).
  for (const s of STATUSES) pair(`status-${s}-foreground`, `status-${s}`, 4.5);
  // Status charts ≥ 3:1 against the background.
  pair("chart-ok", "background", 3);
  pair("chart-fail", "background", 3);

  // base-nova style destructive button: `--destructive` text on itself at
  // /10 (light) or /20, hover /25 (dark).
  const alphas = name === "light" ? [0.1, 0.2] : [0.2, 0.25];
  for (const a of alphas) {
    it(`destructive on destructive/${a * 100} (card) ≥ 4.5`, () => {
      const bg = over(get("destructive"), a, get("card"));
      expect(ratio(get("destructive"), bg)).toBeGreaterThanOrEqual(4.5);
    });
    // A delete button on a table row sits on the page background or on `muted` (cards, selected rows),
    // not only on `card`: hover /20 on the light `--background` is 4.41 — axe catches it while
    // the pointer is still on the button.
    for (const under of ["background", "muted"]) {
      it(`destructive on destructive/${a * 100} (${under}) ≥ 4.5`, () => {
        const bg = over(get("destructive"), a, get(under));
        expect(ratio(get("destructive"), bg)).toBeGreaterThanOrEqual(4.5);
      });
    }
  }
});

// An input inside an overlay layer still has a border ≥ 3:1;
// dark theme — softened teal (dark text on the button ≥ 7:1, not more glaring than everything else), overlay layers and
// skeleton set apart from card.
describe.each(Object.entries(THEMES))("controls and overlay layers — theme %s", (name, t) => {
  const get = (k: string): Rgb => {
    const v = t.get(k);
    if (!v) throw new Error(`${name}: missing --${k}`);
    return v;
  };
  it("input on popover ≥ 3", () => {
    expect(ratio(get("input"), get("popover"))).toBeGreaterThanOrEqual(3);
  });
  it("has a --skeleton token", () => {
    get("skeleton");
  });
  if (name !== "dark") return;
  for (const bg of ["primary", "primary-hover"]) {
    it(`primary-foreground on ${bg} ≥ 7`, () => {
      expect(ratio(get("primary-foreground"), get(bg))).toBeGreaterThanOrEqual(7);
    });
  }
  it("primary not glaring: ≤ 8.5 against the background", () => {
    expect(ratio(get("primary"), get("background"))).toBeLessThanOrEqual(8.5);
  });
  it("primary hover is lighter than primary", () => {
    expect(luminance(get("primary-hover"))).toBeGreaterThan(luminance(get("primary")));
  });
  for (const k of ["popover", "skeleton"]) {
    it(`${k} one step lighter than card: ≥ 1.15`, () => {
      expect(luminance(get(k))).toBeGreaterThan(luminance(get("card")));
      expect(ratio(get(k), get("card"))).toBeGreaterThanOrEqual(1.15);
    });
  }
});

it("OKLCH conversion matches the original hex in the comments", () => {
  // light `--foreground`: #1C1B19; light `--primary`: #0B7A6B.
  const hex = (c: Rgb) =>
    `#${c
      .map((v) =>
        Math.round(v * 255)
          .toString(16)
          .padStart(2, "0"),
      )
      .join("")}`.toUpperCase();
  expect(hex(THEMES.light.get("foreground") as Rgb)).toBe("#1C1B19");
  expect(hex(THEMES.light.get("primary") as Rgb)).toBe("#0B7A6B");
});
