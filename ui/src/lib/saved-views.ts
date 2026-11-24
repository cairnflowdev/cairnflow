// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Local saved views: `localStorage` per tenant, at most 50, names ≤ 255 characters. A
// view is the URL's query string — shared by link, as in Temporal. Server-side saved
// views may come later.
import type { InvocationSearch } from "./invocation-search";

export type SavedView = { name: string; search: InvocationSearch };

export const MAX_VIEWS = 50;
export const MAX_NAME = 255;

const key = (tenant: string) => `cairn-views:${tenant}`;

export function loadViews(tenant: string): SavedView[] {
  try {
    const raw = JSON.parse(localStorage.getItem(key(tenant)) ?? "[]");
    return Array.isArray(raw)
      ? raw.filter((v) => typeof v?.name === "string" && typeof v?.search === "object")
      : [];
  } catch {
    return [];
  }
}

/** Adds (or replaces the view with the same name); returns the new list. Over 50 ⇒ drops the oldest. */
export function saveView(tenant: string, view: SavedView): SavedView[] {
  const name = view.name.trim().slice(0, MAX_NAME);
  const { peek: _, ...search } = view.search;
  const next = [{ name, search }, ...loadViews(tenant).filter((v) => v.name !== name)].slice(
    0,
    MAX_VIEWS,
  );
  try {
    localStorage.setItem(key(tenant), JSON.stringify(next));
  } catch {}
  return next;
}

export function deleteView(tenant: string, name: string): SavedView[] {
  const next = loadViews(tenant).filter((v) => v.name !== name);
  try {
    localStorage.setItem(key(tenant), JSON.stringify(next));
  } catch {}
  return next;
}
