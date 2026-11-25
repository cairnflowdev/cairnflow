// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Key permissions: each role is a **capability set** fixed on the server (`invoke`,
// `view`, `admin`, `provision`, `cluster-admin`), no longer nested as
// `invoke ⊂ admin ⊂ cluster-admin`. The server sends that set in `GET /auth/whoami`
// (`capabilities`, in the fixed order of `CAPABILITIES`); the UI only reads it to
// hide/disable things for clarity — the server still decides. The UI does **not**
// derive capabilities from the role name: a new role or a changed set on the server
// is followed by the console at once.
export type Capability = "read" | "invoke" | "operate" | "provision" | "cluster";

/** Canonical order, as the server sends it. */
export const CAPABILITIES: readonly Capability[] = [
  "read",
  "invoke",
  "operate",
  "provision",
  "cluster",
];

/** Whether `have` has capability `need`. No set (old server, not loaded) ⇒ no. */
export function can(have: readonly Capability[] | undefined, need: Capability): boolean {
  return have?.includes(need) ?? false;
}

/** Has at least one of `needs` (a screen usable through several paths, like Schedules). */
export function canAny(
  have: readonly Capability[] | undefined,
  needs: Capability | readonly Capability[],
): boolean {
  return typeof needs === "string" ? can(have, needs) : needs.some((n) => can(have, n));
}
