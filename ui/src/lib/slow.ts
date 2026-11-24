// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// "Why is it slow" for a slow run: the largest part of the time among code running,
// SDK, waiting for durability, waiting for the engine, waiting for quota.
import type { components } from "@/data/api.gen";
import { m } from "@/paraglide/messages";

type Run = components["schemas"]["SlowRun"];

export function mostly(r: Run): { label: string; ms: number } {
  const parts: [string, number][] = [
    [m.slow_part_run(), r.runMs],
    [m.slow_part_sdk(), r.sdkMs],
    [m.slow_part_durable(), r.durableWaitMs],
    [m.slow_part_engine(), r.engineWaitMs],
    [m.slow_part_permit(), r.permitWaitMs],
  ];
  const [label, ms] = parts.reduce((a, b) => (b[1] > a[1] ? b : a));
  return { label, ms };
}
