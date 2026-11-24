// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// i18n: English is primary; `vi` must have every key, keep the same
// placeholders, and be Unicode NFC — precomposed Vietnamese diacritics, not
// combining ones (NFD skews search and string comparison, and some fonts draw the
// marks in the wrong place).
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const load = (l: string) =>
  JSON.parse(readFileSync(join(__dirname, "..", "messages", `${l}.json`), "utf8")) as Record<
    string,
    string
  >;
const en = load("en");
const vi = load("vi");
const messages = (o: Record<string, string>) =>
  Object.entries(o).filter(([k]) => !k.startsWith("$"));
const params = (s: string) => [...s.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();

describe("i18n en/vi", () => {
  it("vi has exactly the keys of en", () => {
    expect(
      messages(vi)
        .map(([k]) => k)
        .sort(),
    ).toEqual(
      messages(en)
        .map(([k]) => k)
        .sort(),
    );
  });

  it("placeholders are the same in both languages", () => {
    for (const [k, v] of messages(en)) expect(params(vi[k] ?? ""), k).toEqual(params(v));
  });

  it("vi strings are NFC and non-empty", () => {
    for (const [k, v] of messages(vi)) {
      expect(v.length, k).toBeGreaterThan(0);
      expect(v === v.normalize("NFC"), `${k} is not NFC`).toBe(true);
    }
  });

  it("vi is really translated: most strings carry Vietnamese diacritics", () => {
    const marked = messages(vi).filter(([, v]) => /[à-ỹÀ-Ỹđ]/u.test(v)).length;
    expect(marked / messages(vi).length).toBeGreaterThan(0.5);
  });
});

// Terminology (`docs/design/glossary-vi.md`): rules settled by the user — words of the
// code/API/SDK/CLI stay in English, the rest is translated; "khoá" only means locking or
// turning off; hints are at most two sentences. Change the table here in the same commit.
const word = (w: string) => new RegExp(`(?<!\\p{L})${w}(?!\\p{L})`, "iu");

/** Screen names from the glossary. */
const SCREENS: Record<string, string> = {
  nav_overview: "Tổng quan",
  nav_archive: "Lưu trữ",
  nav_usage: "Mức sử dụng",
  nav_flow: "Điều tiết",
  nav_query: "Truy vấn",
  stats_title: "Thống kê truy vấn",
  nav_settings: "Cài đặt",
  nav_audit: "Nhật ký audit",
  nav_objects: "Objects & state",
  nav_schedules: "Schedules",
  nav_cluster: "Cluster",
};

/** `[word not to use, correct word]` — the "Keep in English" table. */
const BANNED: [string, string][] = [
  ["dải", "range"],
  ["không gian khoá", "keyspace"],
  ["khoảng khoá", "keyspace"],
  ["luồng", "stream"],
  ["luật", "rule"],
  ["dịch vụ", "service"],
  ["bộ xử lý", "handler"],
  ["triển khai", "deployment / deploy"],
  ["đối tượng", "object"],
  ["lời hứa", "promise"],
  ["tín hiệu", "signal"],
  ["bên thuê", "tenant"],
  ["cụm", "cluster"],
];

/** Sentence count: sentence-ending punctuation followed by whitespace or end of string (skips "vd.", "e.g.", decimals). */
function sentences(s: string): number {
  const t = s
    .replace(/(?<!\p{L})(vd|e\.g|i\.e|etc|vs)\./giu, "")
    .replace(/\d\.\d/g, "")
    .replace(/…/g, "");
  return [...t.trim().matchAll(/[.!?](?=\s|$)/g)].length;
}

describe("vi terminology", () => {
  it("screen names match the glossary", () => {
    for (const [k, v] of Object.entries(SCREENS)) expect(vi[k], k).toBe(v);
  });

  it("does not use translations of words kept in English", () => {
    const bad = messages(vi).flatMap(([k, v]) =>
      BANNED.filter(([w]) => word(w).test(v)).map(([w, ok]) => `${k}: "${w}" ⇒ ${ok} — ${v}`),
    );
    expect(bad).toEqual([]);
  });

  it("khoá only means lock or turn off; key is key, sign-in is API key", () => {
    const bad = messages(vi)
      .filter(
        ([k, v]) => word("khoá").test(v) && !/\block|disabl|turn(?:ed)? off/i.test(en[k] ?? ""),
      )
      .map(([k, v]) => `${k}: ${v}`);
    expect(bad).toEqual([]);
  });

  it("Vietnamese sentences do not name screens by their translated English names", () => {
    const bad = messages(vi)
      .filter(([, v]) =>
        /(?<!\p{L})(Usage|Archive|Flow control|Query stats|Overview|Settings)(?!\p{L})/u.test(v),
      )
      .map(([k, v]) => `${k}: ${v}`);
    expect(bad).toEqual([]);
  });

  it("nhật ký only for the audit log; journal is journal", () => {
    const bad = messages(vi)
      .filter(([k, v]) => word("nhật ký").test(v) && !/audit/i.test(en[k] ?? ""))
      .map(([k, v]) => `${k}: ${v}`);
    expect(bad).toEqual([]);
  });

  it("hints are at most two sentences in both languages", () => {
    const bad = [en, vi].flatMap((o) =>
      messages(o)
        .filter(([k, v]) => /_(hint|note|body|intro|help|warning)$/.test(k) && sentences(v) > 2)
        .map(([k, v]) => `${k}: ${v}`),
    );
    expect(bad).toEqual([]);
  });

  it("sentence count skips vd./e.g. and decimals", () => {
    expect(sentences("Một. Hai, vd. 1.5 giây. Ba?")).toBe(3);
    expect(sentences("Rule (e.g. 20/5/300) trips.")).toBe(1);
  });
});
