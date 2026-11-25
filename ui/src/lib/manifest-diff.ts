// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Compares a service's manifest between the current version and the one about to be
// moved to (step 1 of "Safe deploy"). Pure — the screen only displays it.
import type { components } from "@/data/api.gen";

export type ManifestService = components["schemas"]["ManifestService"];
type Handler = components["schemas"]["ManifestHandler"];

export type FieldChange = {
  /** `null` = a field of the service (shared contract, type). */
  handler: string | null;
  field: string;
  from: unknown;
  to: unknown;
};

export type ServiceDiff = {
  service: string;
  /** The service has no revision yet — newly added, nothing to compare. */
  isNew: boolean;
  handlersAdded: string[];
  /** Handlers the old version has and the new one drops — calls to them will fail after the move. */
  handlersRemoved: string[];
  changed: FieldChange[];
};

function fields(o: object, skip: string[]): Map<string, unknown> {
  return new Map(Object.entries(o).filter(([k, v]) => !skip.includes(k) && v !== undefined));
}

function compare(handler: string | null, a: object, b: object, skip: string[]): FieldChange[] {
  const [x, y] = [fields(a, skip), fields(b, skip)];
  const keys = [...new Set([...x.keys(), ...y.keys()])].sort();
  return keys
    .filter((k) => JSON.stringify(x.get(k)) !== JSON.stringify(y.get(k)))
    .map((field) => ({ handler, field, from: x.get(field), to: y.get(field) }));
}

export function diffService(old: ManifestService | undefined, next: ManifestService): ServiceDiff {
  if (!old) {
    return {
      service: next.name,
      isNew: true,
      handlersAdded: next.handlers.map((h) => h.name),
      handlersRemoved: [],
      changed: [],
    };
  }
  const byName = (hs: Handler[]) => new Map(hs.map((h) => [h.name, h]));
  const [o, n] = [byName(old.handlers), byName(next.handlers)];
  const changed = compare(null, old, next, ["name", "handlers"]);
  for (const [name, h] of n) {
    const before = o.get(name);
    if (before) changed.push(...compare(name, before, h, ["name"]));
  }
  return {
    service: next.name,
    isNew: false,
    handlersAdded: [...n.keys()].filter((k) => !o.has(k)),
    handlersRemoved: [...o.keys()].filter((k) => !n.has(k)),
    changed,
  };
}

/** Nothing changed — the move is safe as far as the contract goes (trial replay is another matter). */
export const unchanged = (d: ServiceDiff) =>
  !d.isNew &&
  d.handlersAdded.length === 0 &&
  d.handlersRemoved.length === 0 &&
  d.changed.length === 0;

/**
 * Status of a deployment in the list: still the current revision of some service ⇒
 * current; otherwise draining (invocations still pinned) or removable (none left).
 * Pins not counted yet ⇒ no guess.
 */
export function deploymentState(d: {
  currentFor: string[];
  pinnedInvocations?: number | null;
}):
  | { kind: "current" }
  | { kind: "draining"; pinned: number }
  | { kind: "removable" }
  | { kind: "unknown" } {
  if (d.currentFor.length > 0) return { kind: "current" };
  if (d.pinnedInvocations === null || d.pinnedInvocations === undefined) return { kind: "unknown" };
  return d.pinnedInvocations > 0
    ? { kind: "draining", pinned: d.pinnedInvocations }
    : { kind: "removable" };
}
