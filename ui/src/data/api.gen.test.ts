// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Drift check of the generated client: regenerating from `docs/api/openapi.json`
// must equal the committed file. Red ⇒ `npm run gen:api`.
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { generate, OUT_PATH, SCHEMA_PATH, tsType } from "../../scripts/gen-api";

describe("api.gen.ts", () => {
  it("equals the version regenerated from openapi.json", () => {
    const fresh = generate(JSON.parse(readFileSync(SCHEMA_PATH, "utf8")));
    expect(readFileSync(OUT_PATH, "utf8") === fresh, "run `npm run gen:api`").toBe(true);
  });

  it("generator: basic types, null, arrays, enum, $ref; an unknown keyword ⇒ error", () => {
    expect(tsType({ type: ["string", "null"] })).toBe("string | null");
    expect(tsType({ type: "array", items: { type: "integer" } })).toBe("(number)[]");
    expect(tsType({ enum: ["a", "b"] })).toBe('"a" | "b"');
    expect(tsType({ $ref: "#/components/schemas/X" })).toBe('components["schemas"]["X"]');
    expect(() => tsType({ type: "string", patternProperties: {} })).toThrow(/unknown/);
  });

  it("every route the console calls is in the schema", () => {
    const doc = JSON.parse(readFileSync(SCHEMA_PATH, "utf8"));
    for (const p of [
      "/auth/whoami",
      "/query",
      "/deployments",
      "/services",
      "/invocations/{id}/journal",
    ]) {
      expect(doc.paths[p], p).toBeDefined();
    }
  });
});
