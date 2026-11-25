// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Picks (service, key) for Workflows and Objects & State: services come from
// `GET /services` by type (default: the first service), the key is clicked from the
// screen's recent list or typed into the "Open a key…" field. Values go into the URL (shareable).
import { useQuery } from "@tanstack/react-query";
import { Search } from "lucide-react";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { m } from "@/paraglide/messages";

type Service = components["schemas"]["Service"];

/**
 * Services of the same type and the one being viewed: from the URL, else the first
 * service. `name` stays the URL value when it is not (or no longer) in the list — the
 * screen can still open a key from an old link.
 */
export function useKeyedServices(ty: Service["ty"], service: string | undefined) {
  const services = useQuery({ queryKey: ["services"], queryFn: () => ok(rest.GET("/services")) });
  const list = (services.data ?? []).filter((s) => s.ty === ty);
  const current = list.find((s) => s.name === service) ?? (service ? undefined : list[0]);
  return { services, list, current, name: current?.name ?? service };
}

/** One row: pick the service, then a field that opens a known key directly. */
export function KeyedToolbar({
  list,
  service,
  objectKey,
  onService,
  onOpen,
}: {
  list: Service[];
  service?: string;
  objectKey?: string;
  onService: (service: string) => void;
  onOpen: (key: string | undefined) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <label className="sr-only" htmlFor="kf-service">
        {m.col_service()}
      </label>
      <NativeSelect
        id="kf-service"
        wrapperClassName="min-w-48"
        value={service ?? ""}
        onChange={(e) => onService(e.target.value)}
      >
        {list.map((s) => (
          <option key={s.name} value={s.name}>
            {s.name}
          </option>
        ))}
      </NativeSelect>
      <KeyJump key={objectKey ?? ""} value={objectKey} onOpen={onOpen} />
    </div>
  );
}

/** Field that opens a key directly (known, or not yet in the recent list). */
function KeyJump({ value, onOpen }: { value?: string; onOpen: (key: string | undefined) => void }) {
  const [text, setText] = useState(value ?? "");
  return (
    <form
      className="relative"
      onSubmit={(e) => {
        e.preventDefault();
        onOpen(text.trim() || undefined);
      }}
    >
      <label className="sr-only" htmlFor="kf-key">
        {m.filter_key()}
      </label>
      <Search
        aria-hidden
        className="pointer-events-none absolute top-2 left-2 size-4 text-muted-foreground"
      />
      <Input
        id="kf-key"
        className="h-8 w-64 pl-7 font-mono"
        placeholder={m.keyed_open_key()}
        value={text}
        onChange={(e) => setText(e.target.value)}
        spellCheck={false}
      />
    </form>
  );
}

export const validateKeySearch = (
  s: Record<string, unknown>,
): { service?: string; key?: string } => ({
  service: typeof s.service === "string" && s.service ? s.service : undefined,
  key: typeof s.key === "string" && s.key ? s.key : undefined,
});
