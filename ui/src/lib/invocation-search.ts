// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// URL state of the Invocations screen: a preset view + filter chips. Everything is in
// the URL ⇒ shareable, back-navigable, a saved view is a query string.
import type { InvocationClass, InvocationFilter, InvocationOrder } from "@/data/queries";

export const VIEWS = ["all", "running", "heldUp", "failed", "paused", "retrying"] as const;
export type View = (typeof VIEWS)[number];

export type InvocationSearch = {
  view?: View;
  /** "Roots only" is on by default; `root=0` turns it off. */
  root?: 0 | 1;
  service?: string;
  handler?: string;
  key?: string;
  deployment?: string;
  tag?: string;
  id?: string;
  idem?: string;
  parent?: string;
  from?: number;
  to?: number;
  /** Id of the row open in the side panel (peek). */
  peek?: string;
};

const str = (v: unknown) =>
  typeof v === "string" && v.length > 0 && v.length <= 512 ? v : undefined;
const num = (v: unknown) => {
  const n = typeof v === "number" ? v : typeof v === "string" ? Number(v) : Number.NaN;
  return Number.isFinite(n) && n >= 0 ? n : undefined;
};

export function validateInvocationSearch(s: Record<string, unknown>): InvocationSearch {
  const view = VIEWS.includes(s.view as View) ? (s.view as View) : undefined;
  const id = str(s.id);
  const peek = str(s.peek);
  return {
    view,
    root: s.root === 0 || s.root === "0" ? 0 : undefined,
    service: str(s.service),
    handler: str(s.handler),
    key: str(s.key),
    deployment: str(s.deployment),
    tag: str(s.tag),
    id: id && /^[0-9a-fA-F]{1,48}$/.test(id) ? id.toLowerCase() : undefined,
    idem: str(s.idem),
    parent: str(s.parent) && /^[0-9a-f]{48}$/.test(String(s.parent)) ? String(s.parent) : undefined,
    from: num(s.from),
    to: num(s.to),
    peek: peek && /^[0-9a-f]{48}$/.test(peek) ? peek : undefined,
  };
}

const VIEW_CLASSES: Record<View, InvocationClass[] | undefined> = {
  all: undefined,
  running: ["active"],
  heldUp: ["active", "paused"],
  failed: ["failed"],
  paused: ["paused"],
  retrying: ["active"],
};

/** Filters and order of the `/query` statement for a URL state. */
export function toFilter(s: InvocationSearch): {
  filter: InvocationFilter;
  order: InvocationOrder;
} {
  const view = s.view ?? "all";
  const filter: InvocationFilter = {
    classes: VIEW_CLASSES[view],
    rootOnly: s.root !== 0,
    service: s.service,
    handler: s.handler,
    key: s.key,
    deployment: s.deployment,
    tag: s.tag,
    idPrefix: s.id,
    idempotencyKey: s.idem,
    parentId: s.parent,
    createdFromMs: s.from,
    createdToMs: s.to,
  };
  if (view === "retrying") filter.retrying = true;
  if (s.parent) filter.rootOnly = false;
  const order: InvocationOrder =
    view === "heldUp"
      ? "heldUp"
      : view === "failed" || view === "paused"
        ? "transitioned"
        : "created";
  return { filter, order };
}
