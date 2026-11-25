// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Thresholds of "Needs attention" on the Overview: stored in this browser
// (localStorage) — not a cluster setting.
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/components/ui/toast";
import { DEFAULTS, saveThresholds, useThresholds } from "@/lib/thresholds";
import { m } from "@/paraglide/messages";

export function ThresholdsForm() {
  const toast = useToast();
  const t = useThresholds();
  const [rate, setRate] = useState(String(Math.round(t.failedRate * 1000) / 10));
  const [runs, setRuns] = useState(String(t.minRuns));
  const r = Number(rate);
  const n = Number(runs);
  const valid = rate.trim() !== "" && r >= 0 && r <= 100 && /^\d+$/.test(runs.trim());
  return (
    <section aria-labelledby="set-thresholds" className="flex flex-col gap-2">
      <h2 id="set-thresholds" className="text-base font-semibold">
        {m.thresholds_title()}
      </h2>
      <p className="text-sm text-muted-foreground">{m.thresholds_body()}</p>
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          if (!valid) return;
          const saved = saveThresholds({ failedRate: r / 100, minRuns: n });
          toast.add({
            title: saved ? m.thresholds_saved() : m.thresholds_not_saved(),
            type: saved ? undefined : "error",
          });
        }}
      >
        <div className="flex flex-col gap-1">
          <Label htmlFor="thr-rate">{m.thresholds_rate()}</Label>
          <Input
            id="thr-rate"
            className="h-8 w-28"
            inputMode="decimal"
            value={rate}
            onChange={(e) => setRate(e.target.value)}
            aria-invalid={valid ? undefined : true}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="thr-runs">{m.thresholds_runs()}</Label>
          <Input
            id="thr-runs"
            className="h-8 w-28"
            inputMode="numeric"
            value={runs}
            onChange={(e) => setRuns(e.target.value)}
            aria-invalid={valid ? undefined : true}
          />
        </div>
        <Button type="submit" disabled={!valid}>
          {m.schedule_save()}
        </Button>
        <Button
          type="button"
          variant="ghost"
          onClick={() => {
            saveThresholds(null);
            setRate(String(DEFAULTS.failedRate * 100));
            setRuns(String(DEFAULTS.minRuns));
          }}
        >
          {m.thresholds_reset()}
        </Button>
      </form>
    </section>
  );
}
