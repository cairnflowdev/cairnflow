// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

/**
 * The web console's `/query` statements and refresh cadence, for the simulated
 * browser of `s3-cost.ts` (`console-*`).
 *
 * The SQL is taken **directly** from `ui/src/data/queries.ts` (the file imports
 * nothing, so Node can run it): when the console changes a statement, the
 * measurement changes with it. The adaptive cadence is copied from
 * `ui/src/data/hooks.ts` (`OVERVIEW_REFRESH`) — that file pulls in React Query and
 * cannot be imported; change it there, change it here.
 */

export { queries } from "../../ui/src/data/queries.ts";

/** `OVERVIEW_REFRESH` of `ui/src/data/hooks.ts`: 10 × the statement's duration, clamped. */
export const OVERVIEW_REFRESH_MS = { min: 10_000, max: 60_000 } as const;
/** `HISTORY_REFRESH` of `ui/src/data/hooks.ts`: the hourly statements. */
export const HISTORY_REFRESH_MS = { min: 60_000, max: 300_000 } as const;
