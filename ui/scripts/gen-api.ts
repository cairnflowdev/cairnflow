// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Generates the TypeScript types of the console's REST client from `docs/api/openapi.json`.
// The output follows the `paths` shape that `openapi-fetch` needs.
//
// Not `openapi-typescript`: its current version requires TypeScript ^5 (the console
// uses 6). The schema uses only a small subset of JSON Schema — exactly the subset the
// TS SDK's strict checker (`sdk/typescript/test/api-schema.ts`) understands — so this generator
// throws on an unknown keyword instead of silently generating `unknown`.
//
// Run: `npm run gen:api`. `src/data/api.gen.test.ts` regenerates and compares with the
// committed file (drift check).
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// biome-ignore lint/suspicious/noExplicitAny: arbitrary JSON Schema
type Schema = { [k: string]: any };

const HERE = dirname(fileURLToPath(import.meta.url));
export const SCHEMA_PATH = join(HERE, "..", "..", "docs", "api", "openapi.json");
export const OUT_PATH = join(HERE, "..", "src", "data", "api.gen.ts");

const ANNOTATIONS = new Set([
  "description",
  "title",
  "x-cairn-reserved",
  "examples",
  "default",
  "deprecated",
  "format",
  // Value constraints — they do not change the TS type.
  "pattern",
  "minimum",
  "maximum",
  "minLength",
  "maxLength",
]);
const KNOWN = new Set([
  ...ANNOTATIONS,
  "type",
  "enum",
  "const",
  "required",
  "properties",
  "additionalProperties",
  "items",
  "oneOf",
  "anyOf",
  "allOf",
  "$ref",
]);
const METHODS = ["get", "put", "post", "delete", "patch", "head", "options"] as const;

const lit = (v: unknown) => JSON.stringify(v);
const key = (k: string) => (/^[A-Za-z_$][\w$]*$/.test(k) ? k : lit(k));
const pad = (n: number) => "  ".repeat(n);

function scalar(t: string): string {
  switch (t) {
    case "string":
      return "string";
    case "integer":
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    default:
      throw new Error(`unknown type: ${t}`);
  }
}

/** TS type of a schema; `depth` for indentation. */
export function tsType(s: Schema, depth = 0): string {
  for (const k of Object.keys(s)) {
    if (!KNOWN.has(k)) throw new Error(`unknown JSON Schema keyword: ${k}`);
  }
  if (s.$ref) {
    const m = /^#\/components\/schemas\/([\w-]+)$/.exec(s.$ref);
    if (!m) throw new Error(`unknown $ref: ${s.$ref}`);
    return `components["schemas"][${lit(m[1] ?? "")}]`;
  }
  if ("const" in s) return lit(s.const);
  if (s.enum) return s.enum.map(lit).join(" | ");
  if (s.oneOf || s.anyOf)
    return (s.oneOf ?? s.anyOf).map((x: Schema) => `(${tsType(x, depth)})`).join(" | ");
  if (s.allOf) return s.allOf.map((x: Schema) => `(${tsType(x, depth)})`).join(" & ");
  const types: string[] = s.type === undefined ? [] : Array.isArray(s.type) ? s.type : [s.type];
  if (types.length === 0) {
    return s.properties || s.additionalProperties !== undefined ? object(s, depth) : "unknown";
  }
  return types
    .map((t) => {
      if (t === "object") return object(s, depth);
      if (t === "array") return `(${s.items ? tsType(s.items, depth) : "unknown"})[]`;
      return scalar(t);
    })
    .join(" | ");
}

function object(s: Schema, depth: number): string {
  const req = new Set<string>(s.required ?? []);
  const lines: string[] = [];
  for (const [k, v] of Object.entries<Schema>(s.properties ?? {})) {
    if (v.description)
      lines.push(`${pad(depth + 1)}/** ${String(v.description).replaceAll("*/", "*\\/")} */`);
    lines.push(`${pad(depth + 1)}${key(k)}${req.has(k) ? "" : "?"}: ${tsType(v, depth + 1)};`);
  }
  // `additionalProperties: true` is the evolution rule for clients (ignore unknown fields)
  // — it does not become an index signature, so a mistyped field is still a compile error.
  const ap = s.additionalProperties;
  if (ap && typeof ap === "object")
    lines.push(`${pad(depth + 1)}[k: string]: ${tsType(ap, depth + 1)};`);
  if (lines.length === 0) return "Record<string, never>";
  return `{\n${lines.join("\n")}\n${pad(depth)}}`;
}

function resolve(doc: Schema, x: Schema, kind: "parameters" | "responses"): Schema {
  if (!x.$ref) return x;
  const m = new RegExp(`^#/components/${kind}/([\\w-]+)$`).exec(x.$ref);
  if (!m) throw new Error(`unknown $ref: ${x.$ref}`);
  const got = m[1] === undefined ? undefined : doc.components?.[kind]?.[m[1]];
  if (!got) throw new Error(`unresolvable $ref: ${x.$ref}`);
  return got;
}

function operation(doc: Schema, op: Schema, inherited: Schema[], depth: number): string {
  const params = [...inherited, ...(op.parameters ?? [])].map((p) => resolve(doc, p, "parameters"));
  const WHERE = ["query", "header", "path", "cookie"] as const;
  const groups: Record<(typeof WHERE)[number], Schema[]> = {
    query: [],
    header: [],
    path: [],
    cookie: [],
  };
  for (const p of params) {
    if (!WHERE.includes(p.in)) throw new Error(`parameter in an unknown place: ${p.in}`);
    groups[p.in as (typeof WHERE)[number]].push(p);
  }
  const out: string[] = [];
  const paramLines: string[] = [];
  for (const where of WHERE) {
    const ps = groups[where];
    if (ps.length === 0) {
      paramLines.push(`${pad(depth + 2)}${where}?: never;`);
      continue;
    }
    const allOptional = ps.every((p) => !p.required);
    const fields = ps
      .map(
        (p) =>
          `${pad(depth + 3)}${key(p.name)}${p.required ? "" : "?"}: ${tsType(p.schema ?? {}, depth + 3)};`,
      )
      .join("\n");
    paramLines.push(
      `${pad(depth + 2)}${where}${allOptional ? "?" : ""}: {\n${fields}\n${pad(depth + 2)}};`,
    );
  }
  out.push(`${pad(depth + 1)}parameters: {\n${paramLines.join("\n")}\n${pad(depth + 1)}};`);
  if (op.requestBody) {
    const rb = op.requestBody;
    const content = Object.entries<Schema>(rb.content ?? {})
      .map(([mime, c]) => `${pad(depth + 3)}${lit(mime)}: ${tsType(c.schema ?? {}, depth + 3)};`)
      .join("\n");
    out.push(
      `${pad(depth + 1)}requestBody${rb.required ? "" : "?"}: {\n${pad(depth + 2)}content: {\n${content}\n${pad(depth + 2)}};\n${pad(depth + 1)}};`,
    );
  } else {
    out.push(`${pad(depth + 1)}requestBody?: never;`);
  }
  const resp: string[] = [];
  for (const [code, r0] of Object.entries<Schema>(op.responses ?? {})) {
    const r = resolve(doc, r0, "responses");
    const entries = Object.entries<Schema>(r.content ?? {});
    const content =
      entries.length === 0
        ? `${pad(depth + 3)}content?: never;`
        : `${pad(depth + 3)}content: {\n${entries
            .map(
              ([mime, c]) => `${pad(depth + 4)}${lit(mime)}: ${tsType(c.schema ?? {}, depth + 4)};`,
            )
            .join("\n")}\n${pad(depth + 3)}};`;
    resp.push(
      `${pad(depth + 2)}${/^\d+$/.test(code) ? code : lit(code)}: {\n${pad(depth + 3)}headers: { [name: string]: unknown };\n${content}\n${pad(depth + 2)}};`,
    );
  }
  out.push(`${pad(depth + 1)}responses: {\n${resp.join("\n")}\n${pad(depth + 1)}};`);
  return `{\n${out.join("\n")}\n${pad(depth)}}`;
}

/** Contents of `api.gen.ts` for an OpenAPI document. */
export function generate(doc: Schema): string {
  const lines: string[] = [
    "// Generated by `npm run gen:api` (ui/scripts/gen-api.ts) from docs/api/openapi.json — do not edit by hand.",
    "",
    "export interface paths {",
  ];
  for (const [path, item] of Object.entries<Schema>(doc.paths ?? {})) {
    const inherited: Schema[] = item.parameters ?? [];
    lines.push(`  ${lit(path)}: {`);
    for (const m of METHODS) {
      lines.push(
        item[m] ? `    ${m}: ${operation(doc, item[m], inherited, 2)};` : `    ${m}?: never;`,
      );
    }
    lines.push("  };");
  }
  lines.push("}", "", "export interface components {", "  schemas: {");
  for (const [name, s] of Object.entries<Schema>(doc.components?.schemas ?? {})) {
    if (s.description) lines.push(`    /** ${String(s.description).replaceAll("*/", "*\\/")} */`);
    lines.push(`    ${key(name)}: ${tsType(s, 2)};`);
  }
  lines.push("  };", "}", "");
  return lines.join("\n");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  writeFileSync(OUT_PATH, generate(JSON.parse(readFileSync(SCHEMA_PATH, "utf8"))));
  console.log(`wrote ${OUT_PATH}`);
}
