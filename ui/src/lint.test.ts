// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Source rules that Biome cannot express (kept in step with the equivalent source
// lint of the hosted web console).
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = new URL(".", import.meta.url).pathname;
const SKIP = new Set(["paraglide"]);

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((n) => {
    if (SKIP.has(n)) return [];
    const p = join(dir, n);
    if (statSync(p).isDirectory()) return sources(p);
    return /\.(ts|tsx)$/.test(n) && !/\.test\.tsx?$/.test(n) && n !== "routeTree.gen.ts" ? [p] : [];
  });
}

/** `[pattern, reason, allowed file]` — the allowed file is a path relative to `src/`. */
const PALETTE =
  /\b(?:bg|text|border|ring|outline|fill|stroke|from|to|via|divide|decoration|shadow)-(?:slate|gray|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|blue|indigo|violet|purple|fuchsia|pink|rose)-\d{2,3}\b/;

const RULES: [RegExp, string, string?][] = [
  [
    /import\.meta\.env\.BASE_URL/,
    "Vite compiles BASE_URL to './' — the base path is read from <meta name=\"cairn-base\"> (src/lib/base.ts)",
  ],
  [/dangerouslySetInnerHTML/, "user payloads are rendered as text only"],
  [
    /from ["']sonner["']|from ["']@radix-ui\/|from ["']radix-ui["']/,
    "the library injects <style> — blocked by the CSP",
  ],
  [/\binnerHTML\s*=/, "never write raw HTML into the DOM"],
  [
    /border-b-2/,
    "underlined tab strips go through components/cairn/tabs.tsx (`Tabs`, or `tabList`/`tabItem` for tabs that are links) — do not rebuild them",
    "components/cairn/tabs.tsx",
  ],
  [
    /<table\b/,
    "tables go through components/cairn/table.tsx (`Table`, `TH`, `TD`…)",
    "components/cairn/table.tsx",
  ],
  [
    /type="checkbox"/,
    "checkboxes go through components/cairn/checkbox.tsx",
    "components/cairn/checkbox.tsx",
  ],
  [/navigator\.clipboard/, "copy via lib/clipboard.ts (`CopyButton`)", "lib/clipboard.ts"],
  [PALETTE, "colours through role tokens (globals.css), not Tailwind's default palette"],
  [/className="[^"]*#[0-9a-fA-F]{3,8}\b/, "no hex in components — use tokens"],
];

/**
 * Hard-coded text in JSX: visible text goes through Paraglide. A text node with
 * letters between `>` and `<` is an error, except `ALLOWED_TEXT` — symbols, units,
 * proper names that are not translated. Stories, tests and `components/ui/` (copied
 * from shadcn, has "Close" for screen readers) are not checked.
 */
// `(?<![=-])`: skips `=>`/`->`; `<(?=[/A-Za-z])`: after the text comes a closing or opening tag, not a generic.
const JSX_TEXT = /(?<![=-])>\s*([A-Za-zÀ-ỹ][^<>{}]*?)\s*<(?=[/A-Za-z])/g;
const ALLOWED_TEXT = new Set<string>([]);

/**
 * Tables become cards on narrow screens: a file that renders `<Table` without
 * `narrow="scroll"` must declare at least one `primary` column — otherwise the card
 * is left with only rows of small text.
 */
const CARD_TABLE_NEEDS_PRIMARY = /<Table\b(?![^>]*narrow="scroll")/;

describe("console source rules", () => {
  for (const file of sources(SRC)) {
    it(file.slice(SRC.length), () => {
      const text = readFileSync(file, "utf8");
      for (const [re, why, allowed] of RULES) {
        if (allowed && file.slice(SRC.length) === allowed) continue;
        expect(re.test(text), `${re}: ${why}`).toBe(false);
      }
      const rel = file.slice(SRC.length);
      if (
        CARD_TABLE_NEEDS_PRIMARY.test(text) &&
        rel !== "components/cairn/table.tsx" &&
        !rel.endsWith(".stories.tsx")
      ) {
        expect(text, 'a card table needs a priority="primary" column').toMatch(/"primary"/);
      }
      if (
        rel.endsWith(".tsx") &&
        !rel.endsWith(".stories.tsx") &&
        !rel.startsWith("components/ui/")
      ) {
        const code = text.replace(/\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");
        const hard = [...code.matchAll(JSX_TEXT)]
          .map((m) => m[1] as string)
          // TS generics (`<T>`), code inside template literals and a comparison before a
          // ternary operator (`n > MAX ? (`) are not JSX.
          .filter((s) => !/^[A-Z]\w*$/.test(s) && !s.includes("=>") && !s.includes(";"))
          .filter((s) => !/\?\s*\($/.test(s))
          .filter((s) => !ALLOWED_TEXT.has(s));
        expect(
          hard,
          "visible text must go through m.*() (Paraglide) — or add it to ALLOWED_TEXT with a reason",
        ).toEqual([]);
      }
    });
  }
});
