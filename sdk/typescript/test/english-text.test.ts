// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0
// Source rule: strings in `src/*.ts` are text users see (thrown errors, errors
// sent to the runtime, `errorText` shown in the UI, warnings), so they must be
// English — public docs quote them verbatim. The test reads only string
// literals through the compiler API, not comments.
//
// "Vietnamese text" = accented Latin (U+00C0–U+024F, U+1EA0–U+1EF9) or
// combining marks (U+0300–U+036F). `—`, `…`, `→` are fine.
import { strict as assert } from "node:assert";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import ts from "typescript";

const SRC = join(import.meta.dirname, "../src");
const VIETNAMESE = /[À-ɏẠ-ỹ̀-ͯ]/u;

function literals(file: string): { line: number; text: string }[] {
  const text = readFileSync(file, "utf8");
  const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
  const out: { line: number; text: string }[] = [];
  const visit = (n: ts.Node): void => {
    if (
      ts.isStringLiteral(n) ||
      ts.isNoSubstitutionTemplateLiteral(n) ||
      ts.isTemplateHead(n) ||
      ts.isTemplateMiddle(n) ||
      ts.isTemplateTail(n)
    ) {
      out.push({ line: sf.getLineAndCharacterOfPosition(n.getStart()).line + 1, text: n.text });
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}

test("user-visible strings in src/ are English", () => {
  const bad: string[] = [];
  for (const f of readdirSync(SRC).filter((f) => f.endsWith(".ts")).sort()) {
    for (const l of literals(join(SRC, f))) {
      if (VIETNAMESE.test(l.text)) bad.push(`${f}:${l.line}: ${JSON.stringify(l.text)}`);
    }
  }
  assert.deepEqual(bad, []);
});
