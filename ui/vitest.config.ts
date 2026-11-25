// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  test: {
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    // React Vite plugin: JSX of tests and stories.
    environment: "node",
  },
});
