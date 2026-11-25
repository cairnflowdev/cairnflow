// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Sample statements of the SQL console: each declares a question number that has a
// server test `ui_question_<n>` (like `ui_queries_are_catalogued`), no duplicate
// numbers, and has a title in both locales. Whether a statement actually runs:
// `console_samples_run` (Rust).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { SAMPLES } from "./samples";

const ROOT = join(__dirname, "..", "..", "..");

function catalogued(): Set<number> {
  const out = new Set<number>();
  for (const f of ["crates/cairn-query/tests/sql.rs", "crates/cairn-server/tests/query_api.rs"]) {
    for (const x of readFileSync(join(ROOT, f), "utf8").matchAll(/async fn ui_question_(\d+)\b/g))
      out.add(Number(x[1]));
  }
  return out;
}

describe("sample statements", () => {
  it("each statement has a question number with a ui_question_<n>, no duplicates", () => {
    const have = catalogued();
    const seen = new Set<number>();
    for (const s of SAMPLES) {
      expect(have.has(s.pq), `question ${s.pq} has no ui_question_${s.pq}`).toBe(true);
      expect(seen.has(s.pq), `question ${s.pq} is duplicated`).toBe(false);
      seen.add(s.pq);
    }
    // Covers every statement that has a cost-pinning test.
    expect([...have].filter((n) => !seen.has(n))).toEqual([]);
  });

  it("has a title in both locales", () => {
    for (const loc of ["en", "vi"]) {
      const msgs = JSON.parse(
        readFileSync(join(__dirname, "..", "..", "messages", `${loc}.json`), "utf8"),
      );
      for (const s of SAMPLES)
        expect(msgs[`sample_${s.pq}`], `${loc}: sample_${s.pq}`).toBeTruthy();
    }
  });
});
