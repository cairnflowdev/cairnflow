// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Drift check of the table catalogue: regenerating from `docs/api/query-tables.md`
// must equal the committed file. Red ⇒ `npm run gen:tables`.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { DOC_PATH, generate, OUT_PATH, parse } from "../../scripts/gen-tables";
import { TABLES } from "./tables.gen";

describe("tables.gen.ts", () => {
  it("equals the version regenerated from query-tables.md", () => {
    const fresh = generate(readFileSync(DOC_PATH, "utf8"));
    expect(readFileSync(OUT_PATH, "utf8") === fresh, "run `npm run gen:tables`").toBe(true);
  });

  it("has the tables the console uses, with columns", () => {
    const names = TABLES.map((t) => t.name);
    for (const t of ["sys_invocation", "sys_journal", "sys_handler_stats", "sys_deployment"])
      expect(names).toContain(t);
    const inv = TABLES.find((t) => t.name === "sys_invocation");
    expect(inv?.columns.find((c) => c.name === "created_at")?.type).toBe("timestamp");
  });

  it("reader: heading, description, rows; a table without columns ⇒ error", () => {
    const md =
      "## `sys_x`\n\nDoc **bold** `a`.\n\n| Column | Type | Meaning |\n|---|---|---|\n| `a` | int | num `n` |\n";
    expect(parse(md)).toEqual([
      { name: "sys_x", doc: "Doc bold a.", columns: [{ name: "a", type: "int", doc: "num n" }] },
    ]);
    expect(() => parse("## `sys_y`\n\nEmpty.\n")).toThrow(/has no columns/);
  });
});
