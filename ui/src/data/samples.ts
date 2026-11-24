// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Sample statements of the SQL console: each has a question number. The SQL lives in
// `samples.json` so the server can read it too — `query_api.rs::console_samples_run`
// runs every statement on a real node.
import { m } from "@/paraglide/messages";
import raw from "./samples.json";

export type Sample = { pq: number; sql: string };

export const SAMPLES: Sample[] = raw;

/** Title per locale — key `sample_<pq>` in the messages. */
export function sampleTitle(pq: number): string {
  const f = (m as unknown as Record<string, (() => string) | undefined>)[`sample_${pq}`];
  return f ? f() : `PQ#${pq}`;
}
