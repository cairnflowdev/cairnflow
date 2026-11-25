// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Base path of the console: the server injects `<meta name="cairn-base">` into
// `index.html` at startup. Vite's BASE_URL variable is not used — it becomes `"./"`
// because the build uses `base: './'`.

/** Base path without a trailing `/`; `""` when served at the root (dev server). */
export function basePath(doc: Document = document): string {
  const v = doc.querySelector('meta[name="cairn-base"]')?.getAttribute("content");
  return v && /^(\/[A-Za-z0-9._~-]+)+$/.test(v) ? v : "";
}

/** Server version, from `<meta name="cairn-version">`. */
export function serverVersion(doc: Document = document): string | null {
  return doc.querySelector('meta[name="cairn-version"]')?.getAttribute("content") ?? null;
}
