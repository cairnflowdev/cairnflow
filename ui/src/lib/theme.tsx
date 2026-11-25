// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Light/dark/system theme. A `.dark` class on `<html>` per the shadcn convention; in
// `system` it listens for the OS changing **after** the choice — shadcn's sample
// ThemeProvider for Vite does not do that.
import { createContext, type ReactNode, useContext, useEffect, useState } from "react";

export type Theme = "light" | "dark" | "system";
const KEY = "cairn-theme";
const QUERY = "(prefers-color-scheme: dark)";

function stored(): Theme {
  try {
    const v = localStorage.getItem(KEY);
    return v === "light" || v === "dark" ? v : "system";
  } catch {
    return "system";
  }
}

function apply(theme: Theme) {
  const dark = theme === "dark" || (theme === "system" && window.matchMedia(QUERY).matches);
  const root = document.documentElement;
  root.classList.toggle("dark", dark);
  root.style.colorScheme = dark ? "dark" : "light";
  // Browser bar colour follows the active theme's `--background`.
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", dark ? "#161514" : "#FAFAF8");
}

const Ctx = createContext<{ theme: Theme; setTheme: (t: Theme) => void } | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(stored);
  useEffect(() => {
    apply(theme);
    try {
      localStorage.setItem(KEY, theme);
    } catch {}
    if (theme !== "system") return;
    const mq = window.matchMedia(QUERY);
    const on = () => apply("system");
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [theme]);
  return <Ctx.Provider value={{ theme, setTheme }}>{children}</Ctx.Provider>;
}

export function useTheme() {
  const c = useContext(Ctx);
  if (!c) throw new Error("useTheme outside ThemeProvider");
  return c;
}
