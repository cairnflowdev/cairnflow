// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// CSV (RFC 4180) for the console's "download" buttons: quoted when a cell contains a
// comma, a quote or a newline; `null` is an empty cell; objects/arrays are written as JSON.
//
// A string cell that starts with `=`, `+`, `-`, `@`, a tab or a carriage return gets a
// leading `'`: a spreadsheet would otherwise run it as a formula, and these strings come
// from tenants' data (inputs, error texts, tags). Numbers are written as they are.

function cell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const raw = typeof v === "object" ? JSON.stringify(v) : String(v);
  const s = typeof v === "string" && /^[=+\-@\t\r]/.test(raw) ? `'${raw}` : raw;
  return /[",\r\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

export function toCsv(header: string[], rows: unknown[][]): string {
  return [header, ...rows].map((r) => r.map(cell).join(",")).join("\n");
}
