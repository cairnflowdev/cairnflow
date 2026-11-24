// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Settings: UI thresholds (any key, in this browser); API keys
// and tenant setup need the `provision` capability (`/admin/keys*`, `/admin/tenants*`),
// invocation capture and the price table need `cluster` (`/admin/capture`).
import { createFileRoute } from "@tanstack/react-router";
import { Capture } from "@/components/settings/capture";
import { Keys } from "@/components/settings/keys";
import { Pricing } from "@/components/settings/pricing";
import { Tenants } from "@/components/settings/tenants";
import { ThresholdsForm } from "@/components/settings/thresholds-form";
import { useCapabilities } from "@/data/session";
import { can } from "@/lib/roles";
import { m } from "@/paraglide/messages";

export const Route = createFileRoute("/_app/settings")({ component: Settings });

function Settings() {
  const caps = useCapabilities();
  const provision = can(caps, "provision");
  const cluster = can(caps, "cluster");
  return (
    // Each section (the `section` of each component) is a bordered card — sections are
    // stacked, and without a border one section's form runs into the next section's table.
    <div className="flex max-w-5xl flex-col gap-6 p-6 [&>section]:rounded-lg [&>section]:border [&>section]:border-border [&>section]:p-4">
      <h1 className="text-xl font-semibold">{m.nav_settings()}</h1>
      <ThresholdsForm />
      {provision ? (
        <>
          <Keys />
          <Tenants />
        </>
      ) : null}
      {cluster ? (
        <>
          <Capture />
          <Pricing />
        </>
      ) : (
        <p className="text-muted-foreground">{m.settings_cluster_only()}</p>
      )}
    </div>
  );
}
