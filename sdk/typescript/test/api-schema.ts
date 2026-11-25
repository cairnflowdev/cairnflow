// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Checks cairnflow's HTTP responses against the machine-readable contract
 * `docs/api/openapi.json`. No external package: a JSON Schema checker sufficient
 * for the subset that file uses (`type`, `enum`, `const`, `pattern`, `minimum`,
 * `minLength`, `required`, `properties`, `additionalProperties`, `items`,
 * `oneOf`/`anyOf`/`allOf`, internal `$ref`). Any other keyword ⇒ throws, so the
 * schema file cannot quietly use something the checker ignores.
 *
 * **Strict.** The schema leaves `additionalProperties` open for clients (the
 * evolution rule: only add optional fields, clients ignore unknown fields). The
 * checker here treats an undeclared field as an error — the server cannot add a
 * field without changing the schema.
 *
 * The e2e suite (`e2e.test.ts`) calls [`checkResponse`] on every response of the
 * routes in the schema; `api-schema.test.ts` checks the checker itself.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };
type Schema = { [k: string]: any };

const here = fileURLToPath(new URL(".", import.meta.url));
export const SCHEMA_PATH = join(here, "..", "..", "..", "docs", "api", "openapi.json");

/** Keywords that are only annotations — ignored when checking. */
const ANNOTATIONS = new Set(["description", "title", "x-cairn-reserved", "examples", "default", "deprecated"]);
const KNOWN = new Set([
  ...ANNOTATIONS,
  "type",
  "enum",
  "const",
  "pattern",
  "minimum",
  "minLength",
  "required",
  "properties",
  "additionalProperties",
  "items",
  "oneOf",
  "anyOf",
  "allOf",
  "$ref",
]);

export class ApiSchema {
  readonly doc: Schema;
  private readonly routes: Array<{ method: string; re: RegExp; template: string; op: Schema }>;

  constructor(doc: Schema = JSON.parse(readFileSync(SCHEMA_PATH, "utf8"))) {
    this.doc = doc;
    this.routes = [];
    for (const [template, ops] of Object.entries<Schema>(doc.paths ?? {})) {
      const re = new RegExp(
        "^" + template.replace(/[.*+?^$()|[\]\\]/g, "\\$&").replace(/\{[^}]+\}/g, "[^/]+") + "$",
      );
      for (const [method, op] of Object.entries<Schema>(ops)) {
        this.routes.push({ method: method.toUpperCase(), re, template, op });
      }
    }
  }

  /** The schema operation matching `method path` (path without query), or `undefined`. */
  route(method: string, path: string): { template: string; op: Schema } | undefined {
    const m = method.toUpperCase();
    // Several templates match (`/invocations/batch/{op}` and `/invocations/{id}/cancel`)
    // ⇒ like the server's router: at the first differing segment, a static segment beats a parameter.
    const param = (seg: string | undefined) => seg?.startsWith("{") ?? false;
    const specific = (a: string, b: string) => {
      const [x, y] = [a.split("/"), b.split("/")];
      for (let i = 0; i < Math.max(x.length, y.length); i++) {
        if (param(x[i]) !== param(y[i])) return param(x[i]) ? 1 : -1;
      }
      return 0;
    };
    return this.routes
      .filter((r) => r.method === m && r.re.test(path))
      .sort((a, b) => specific(a.template, b.template))[0];
  }

  /** Internal `$ref` (`#/a/b`) → the document node. */
  resolve(ref: string): Schema {
    if (!ref.startsWith("#/")) throw new Error(`$ref outside the document is not supported: ${ref}`);
    let node: any = this.doc;
    for (const part of ref.slice(2).split("/")) {
      node = node?.[part.replace(/~1/g, "/").replace(/~0/g, "~")];
      if (node === undefined) throw new Error(`dangling $ref: ${ref}`);
    }
    return node;
  }

  private deref(x: Schema): Schema {
    let n = x;
    for (let i = 0; n && typeof n.$ref === "string" && Object.keys(n).length === 1; i++) {
      if (i > 32) throw new Error("$ref cycle");
      n = this.resolve(n.$ref);
    }
    return n;
  }

  /**
   * Checks `value` against `schema`. Returns the list of violations (empty = match),
   * each with its JSON path. `strict`: a field not declared in `properties` is a
   * violation when the schema does not set `additionalProperties`.
   */
  validate(schema: Schema, value: Json, strict = true, at = "$"): string[] {
    for (const k of Object.keys(schema)) {
      if (!KNOWN.has(k)) throw new Error(`the checker does not understand keyword ${JSON.stringify(k)} at ${at}`);
    }
    const errs: string[] = [];
    if (schema.$ref !== undefined) {
      errs.push(...this.validate(this.resolve(schema.$ref), value, strict, at));
    }
    if (schema.type !== undefined) {
      const types: string[] = Array.isArray(schema.type) ? schema.type : [schema.type];
      if (!types.some((t) => typeOk(t, value))) {
        return [...errs, `${at}: expected ${types.join("|")}, got ${describe(value)}`];
      }
    }
    if (schema.const !== undefined && !deepEqual(schema.const, value)) {
      errs.push(`${at}: expected exactly ${JSON.stringify(schema.const)}, got ${JSON.stringify(value)}`);
    }
    if (schema.enum !== undefined && !schema.enum.some((e: Json) => deepEqual(e, value))) {
      errs.push(`${at}: ${JSON.stringify(value)} is not in ${JSON.stringify(schema.enum)}`);
    }
    if (typeof value === "string") {
      if (schema.pattern !== undefined && !new RegExp(schema.pattern, "u").test(value)) {
        errs.push(`${at}: ${JSON.stringify(value.slice(0, 80))} does not match /${schema.pattern}/`);
      }
      if (schema.minLength !== undefined && [...value].length < schema.minLength) {
        errs.push(`${at}: string shorter than ${schema.minLength}`);
      }
    }
    if (typeof value === "number" && schema.minimum !== undefined && value < schema.minimum) {
      errs.push(`${at}: ${value} < ${schema.minimum}`);
    }
    if (Array.isArray(value) && schema.items !== undefined) {
      value.forEach((v, i) => errs.push(...this.validate(schema.items, v, strict, `${at}[${i}]`)));
    }
    if (isObject(value)) {
      for (const k of schema.required ?? []) {
        if (!(k in value)) errs.push(`${at}: missing required field ${JSON.stringify(k)}`);
      }
      const props: Schema = schema.properties ?? {};
      for (const [k, v] of Object.entries(value)) {
        if (k in props) {
          errs.push(...this.validate(props[k], v, strict, `${at}.${k}`));
        } else if (schema.additionalProperties === false) {
          errs.push(`${at}: undeclared field ${JSON.stringify(k)}`);
        } else if (isObject(schema.additionalProperties)) {
          errs.push(...this.validate(schema.additionalProperties, v, strict, `${at}.${k}`));
        } else if (strict && schema.additionalProperties === undefined && schema.properties !== undefined) {
          errs.push(`${at}: undeclared field ${JSON.stringify(k)} (strict — add it to docs/api/openapi.json)`);
        }
      }
    }
    for (const sub of schema.allOf ?? []) errs.push(...this.validate(sub, value, strict, at));
    if (schema.anyOf !== undefined) {
      const all = schema.anyOf.map((s: Schema) => this.validate(s, value, strict, at));
      if (!all.some((e: string[]) => e.length === 0)) errs.push(`${at}: matches no anyOf branch: ${branchSummary(all)}`);
    }
    if (schema.oneOf !== undefined) {
      const all = schema.oneOf.map((s: Schema) => this.validate(s, value, strict, at));
      const ok = all.filter((e: string[]) => e.length === 0).length;
      if (ok === 0) errs.push(`${at}: matches no oneOf branch: ${branchSummary(all)}`);
      else if (ok > 1) errs.push(`${at}: matches ${ok} oneOf branches (must be exactly one)`);
    }
    return errs;
  }

  /**
   * Checks a response already read. `undefined` ⇒ the route is not in the schema (not
   * checked); `[]` ⇒ match; anything else is violations.
   */
  check(
    method: string,
    url: string,
    status: number,
    headers: { get(name: string): string | null },
    text: string,
  ): string[] | undefined {
    const path = new URL(url, "http://x").pathname;
    const r = this.route(method, path);
    if (!r) return undefined;
    const responses: Schema = r.op.responses;
    const spec = responses[String(status)] ?? responses.default;
    const where = `${method.toUpperCase()} ${r.template} → ${status}`;
    if (!spec) return [`${where}: status ${status} is not in the schema`];
    const resp = this.deref(spec);
    const errs: string[] = [];
    for (const [name, h] of Object.entries<Schema>(resp.headers ?? {})) {
      const v = headers.get(name);
      if (v === null) {
        if (h.required) errs.push(`${where}: missing header ${name}`);
      } else {
        errs.push(...this.validate(h.schema ?? {}, v, true, `header ${name}`).map((e) => `${where}: ${e}`));
      }
    }
    const media = resp.content?.["application/json"];
    if (!media) return errs;
    const ctype = headers.get("content-type") ?? "";
    if (!ctype.includes("application/json")) {
      errs.push(`${where}: content-type ${JSON.stringify(ctype)}, expected application/json — body: ${text.slice(0, 200)}`);
      return errs;
    }
    let body: Json;
    try {
      body = JSON.parse(text);
    } catch {
      return [...errs, `${where}: body is not JSON: ${text.slice(0, 200)}`];
    }
    errs.push(...this.validate(media.schema, body).map((e) => `${where}: ${e}`));
    return errs;
  }
}

/**
 * Checks a `Response` (without consuming its body — reads a `clone()`).
 * Throws with every violation when it does not match.
 */
export async function checkResponse(schema: ApiSchema, method: string, url: string, r: Response): Promise<void> {
  const path = new URL(url, "http://x").pathname;
  if (!schema.route(method, path)) return;
  const text = await r.clone().text();
  const errs = schema.check(method, url, r.status, r.headers, text);
  if (errs && errs.length > 0) {
    throw new Error(`response deviates from docs/api/openapi.json:\n  ${errs.join("\n  ")}\n  body: ${text.slice(0, 500)}`);
  }
}

function typeOk(t: string, v: Json): boolean {
  switch (t) {
    case "null":
      return v === null;
    case "boolean":
      return typeof v === "boolean";
    case "string":
      return typeof v === "string";
    case "number":
      return typeof v === "number";
    case "integer":
      return typeof v === "number" && Number.isInteger(v);
    case "array":
      return Array.isArray(v);
    case "object":
      return isObject(v);
    default:
      throw new Error(`unknown type: ${t}`);
  }
}

function isObject(v: unknown): v is { [k: string]: Json } {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

function describe(v: Json): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v === "number" && Number.isInteger(v) ? "integer" : typeof v;
}

function deepEqual(a: Json, b: Json): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function branchSummary(all: string[][]): string {
  return all.map((e, i) => `[${i}] ${e[0] ?? "ok"}`).join("; ");
}
