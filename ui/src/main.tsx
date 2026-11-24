// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import "@fontsource-variable/inter";
import "@fontsource-variable/geist-mono";
import "./styles/globals.css";

import { CSPProvider } from "@base-ui/react/csp-provider";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRouter, RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ToastProvider } from "./components/ui/toast";
import { TooltipProvider } from "./components/ui/tooltip";
import { ApiError } from "./data/api";
import { basePath } from "./lib/base";
import { ThemeProvider } from "./lib/theme";
import { getLocale } from "./paraglide/runtime";
import { routeTree } from "./routeTree.gen";

// Retry once for transient errors (network, 5xx, 429); other 4xx are answers — retrying the `404`
// of a missing id only keeps the screen on its skeleton longer.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: (n, e) =>
        n < 1 && !(e instanceof ApiError && e.status >= 400 && e.status < 500 && e.status !== 429),
    },
  },
});

const router = createRouter({
  routeTree,
  basepath: basePath() || "/",
  context: { queryClient },
  defaultPreload: "intent",
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

// WCAG 3.1.1: the page language follows the active locale (changing locale reloads the page).
document.documentElement.lang = getLocale();

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <StrictMode>
    {/* Base UI does not inject <style> (blocked by the CSP) — the rules it needs live in globals.css. */}
    <CSPProvider disableStyleElements>
      <ThemeProvider>
        <QueryClientProvider client={queryClient}>
          <TooltipProvider>
            <ToastProvider>
              <RouterProvider router={router} />
            </ToastProvider>
          </TooltipProvider>
        </QueryClientProvider>
      </ThemeProvider>
    </CSPProvider>
  </StrictMode>,
);
