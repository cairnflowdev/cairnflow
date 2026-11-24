// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Console build.
//
// - `base: './'`: every URL Vite generates is relative to the module's URL, only URLs in
//   `index.html` are relative to the page — the server rewrites them once at startup
//   (`"./` ⇒ `"<base>/`, crate `cairn-ui`). Do not use `import.meta.env.BASE_URL`
//   (compiles to `"./"`): the router reads the base from `<meta name="cairn-base">`.
// - Precompressed: every asset has `.br` and `.gz` versions next to it; the server picks by
//   `Accept-Encoding`, no compression at runtime.
// - `npm run dev` proxies the API to a running node (`CAIRN_DEV_API`, default
//   `http://127.0.0.1:8080`); SSE passes through unbuffered.
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants, gzipSync } from "node:zlib";
import { paraglideVitePlugin } from "@inlang/paraglide-js";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";

const API = process.env.CAIRN_DEV_API ?? "http://127.0.0.1:8080";

/** Node API paths the dev server forwards (everything except the console itself). */
const API_PREFIXES = [
  "/admin",
  "/archive",
  "/auth",
  "/awakeables",
  "/cluster",
  "/deployments",
  "/health",
  "/invocations",
  "/invoke",
  "/limits",
  "/metering",
  "/metrics",
  "/promises",
  "/query",
  "/ranges",
  "/rules",
  "/schedules",
  "/scheduler",
  "/scope",
  "/services",
  "/stats",
  "/ui/session",
  "/vqueues",
  "/workflow",
];

/** Generates `.br` and `.gz` for every compressible asset, after Vite writes `dist`. */
function precompress(): Plugin {
  const compressible = /\.(js|mjs|css|html|svg|json|txt)$/;
  let outDir = "dist";
  return {
    name: "cairn-precompress",
    apply: "build",
    configResolved(c) {
      outDir = c.build.outDir;
    },
    closeBundle() {
      const walk = (dir: string): string[] =>
        readdirSync(dir).flatMap((n) => {
          const p = join(dir, n);
          return statSync(p).isDirectory() ? walk(p) : [p];
        });
      for (const file of walk(outDir)) {
        // The server rewrites index.html at startup: a compressed copy would be stale.
        if (!compressible.test(file) || file.endsWith("index.html")) continue;
        const raw = readFileSync(file);
        if (raw.length < 1024) continue;
        writeFileSync(
          `${file}.br`,
          brotliCompressSync(raw, {
            params: { [constants.BROTLI_PARAM_QUALITY]: 11 },
          }),
        );
        writeFileSync(`${file}.gz`, gzipSync(raw, { level: 9 }));
      }
    },
  };
}

export default defineConfig({
  base: "./",
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  plugins: [
    tanstackRouter({ target: "react", autoCodeSplitting: true }),
    react(),
    tailwindcss(),
    paraglideVitePlugin({
      project: "./project.inlang",
      outdir: "./src/paraglide",
      strategy: ["localStorage", "baseLocale"],
    }),
    precompress(),
  ],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    // No small images/fonts inlined as `data:` into CSS/JS — separate files, cacheable.
    assetsInlineLimit: 0,
  },
  server: {
    port: 5173,
    proxy: Object.fromEntries(API_PREFIXES.map((p) => [p, { target: API, changeOrigin: false }])),
  },
});
