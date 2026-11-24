// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Set the theme before the first paint. A static file, not inline:
// CSP `script-src 'self'`. Same key as ThemeProvider (`src/lib/theme.tsx`).
(() => {
  let pref = "system";
  try {
    pref = localStorage.getItem("cairn-theme") || "system";
  } catch {}
  const dark =
    pref === "dark" ||
    (pref === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  const root = document.documentElement;
  root.classList.toggle("dark", dark);
  root.style.colorScheme = dark ? "dark" : "light";
})();
