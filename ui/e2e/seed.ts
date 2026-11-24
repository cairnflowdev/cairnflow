// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import type { Seed } from "./global-setup";

/** Sample invocation ids created by the global setup (each worker reads them from env). */
export function seed(): Seed {
  const raw = process.env.CAIRN_E2E_SEED;
  if (!raw) throw new Error("missing CAIRN_E2E_SEED — the global setup has not run");
  return JSON.parse(raw) as Seed;
}
