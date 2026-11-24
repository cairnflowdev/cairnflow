// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Linear-style filters: the "Filter" button opens a searchable popover — pick a
// field, then pick a value (suggested from `/services`, `/deployments`) or type
// one freely. The button sits in the header row; active filters are "Field is
// value ×" chips on a separate row shown only when there are any, click the value
// to change it. Values go into the URL; the `/query` statement uses bind
// parameters.
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import {
  Box,
  Clock,
  Code,
  Fingerprint,
  GitFork,
  KeyRound,
  ListFilter,
  type LucideIcon,
  Plus,
  Rocket,
  Tag as TagIcon,
} from "lucide-react";
import { type ReactElement, useEffect, useRef, useState } from "react";
import { Tag } from "@/components/cairn/tag";
import { absolute } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { ok, rest } from "@/data/api";
import type { InvocationSearch } from "@/lib/invocation-search";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

export const FIELDS = [
  "service",
  "handler",
  "key",
  "tag",
  "deployment",
  "idem",
  "parent",
  "from",
] as const;
type Field = (typeof FIELDS)[number];

const LABEL: Record<Field, () => string> = {
  service: m.col_service,
  handler: m.filter_handler,
  key: m.filter_key,
  tag: m.filter_tag,
  deployment: m.col_deployment,
  idem: m.filter_idem,
  parent: m.filter_parent,
  from: m.filter_created,
};

const ICON: Record<Field, LucideIcon> = {
  service: Box,
  handler: Code,
  key: KeyRound,
  tag: TagIcon,
  deployment: Rocket,
  idem: Fingerprint,
  parent: GitFork,
  from: Clock,
};

function useSet() {
  const navigate = useNavigate({ from: "/invocations/" });
  return (patch: Partial<InvocationSearch>) =>
    navigate({ search: (s: InvocationSearch) => ({ ...s, ...patch, peek: undefined }) });
}

type Option = { value: string; label?: string; hint?: string };

/** "Created within" ranges of the created-at filter — `from` = now − range. */
const SPANS: [string, number, () => string][] = [
  ["15m", 15 * 60_000, m.filter_last_15m],
  ["1h", 3600_000, m.filter_last_hour],
  ["24h", 24 * 3600_000, m.filter_last_day],
  ["7d", 7 * 24 * 3600_000, m.filter_last_week],
];

/** Chip value of a field: `from` is an ms timestamp ⇒ a readable time. */
function shown(field: Field, v: string | number | undefined): string {
  if (v === undefined) return "";
  return field === "from" ? absolute(Number(v)) : String(v);
}

/** Suggested values of a field; free-form fields (key, tag, idem, parent) have none. */
function useOptions(field: Field | null, search: InvocationSearch): Option[] {
  const services = useQuery({
    queryKey: ["services"],
    queryFn: () => ok(rest.GET("/services")),
    enabled: field === "service" || field === "handler",
  });
  const deployments = useQuery({
    queryKey: ["deployments"],
    queryFn: () => ok(rest.GET("/deployments")),
    enabled: field === "deployment",
  });
  if (field === "service") return (services.data ?? []).map((s) => ({ value: s.name }));
  if (field === "handler") {
    // Filtered by service ⇒ only its handlers; otherwise every handler, with the service name.
    const of = (services.data ?? []).filter((s) => !search.service || s.name === search.service);
    const seen = new Map<string, string[]>();
    for (const s of of)
      for (const h of s.handlers) seen.set(h.name, [...(seen.get(h.name) ?? []), s.name]);
    return [...seen].map(([value, in_]) => ({
      value,
      hint: search.service ? undefined : in_.join(", "),
    }));
  }
  if (field === "deployment")
    return (deployments.data ?? []).map((d) => ({ value: d.id, hint: d.endpoint }));
  if (field === "from") return SPANS.map(([value, , label]) => ({ value, label: label() }));
  return [];
}

/**
 * Filter picker popover. Without `field` it starts at the field list; with one
 * (an active chip) it goes straight to that field's value step.
 */
function FilterMenu({
  search,
  field: fixed,
  trigger,
}: {
  search: InvocationSearch;
  field?: Field;
  trigger: ReactElement;
}) {
  const set = useSet();
  const input = useRef<HTMLInputElement>(null);
  const [open, setOpen] = useState(false);
  const [picked, setPicked] = useState<Field | null>(null);
  const [text, setText] = useState("");
  const field = fixed ?? picked;
  const options = useOptions(open ? field : null, search);
  const typed = text.trim();
  const custom = field !== null && typed !== "" && !options.some((o) => o.value === typed);
  // biome-ignore lint/correctness/useExhaustiveDependencies: refocus when the step changes (Command is rebuilt)
  useEffect(() => {
    if (open) input.current?.focus();
  }, [field]);
  const close = () => {
    setOpen(false);
    setPicked(null);
    setText("");
  };
  const apply = (value: string) => {
    if (field === "from") {
      // A preset range, or a typed timestamp (ISO or text `Date.parse` understands).
      const span = SPANS.find(([v]) => v === value)?.[1];
      const at = span !== undefined ? Date.now() - span : Date.parse(value);
      if (Number.isFinite(at)) set({ from: Math.max(0, Math.round(at)) });
    } else if (field) set({ [field]: value });
    close();
  };
  return (
    <Popover open={open} onOpenChange={(o) => (o ? setOpen(true) : close())}>
      <PopoverTrigger render={trigger} />
      <PopoverContent className="w-80" initialFocus={input}>
        {/* `key` per step: rebuild so cmdk pre-highlights the first item of the new list
            (keeping the old item means Enter selects nothing); the input is refocused below. */}
        {/* The input is named through cmdk's `label`: it attaches `aria-labelledby` to a
            separate `<label>`, so an `aria-label` on the input would be overridden. */}
        <Command loop key={field ?? ""} label={field ? m.filter_value() : m.filter_field()}>
          <CommandInput
            ref={input}
            placeholder={
              field
                ? m.filter_value_placeholder({ field: LABEL[field]().toLowerCase() })
                : m.filter_pick_field()
            }
            value={text}
            onValueChange={setText}
            onKeyDown={(e) => {
              // Like Linear: backspace on an empty input ⇒ back to the field list.
              if (e.key === "Backspace" && text === "" && picked && !fixed) {
                e.preventDefault();
                setPicked(null);
              }
            }}
            spellCheck={false}
          />
          <CommandList>
            {field === null ? (
              <CommandGroup>
                {FIELDS.map((f) => {
                  const Icon = ICON[f];
                  return (
                    <CommandItem
                      key={f}
                      value={LABEL[f]()}
                      onSelect={() => {
                        setPicked(f);
                        setText("");
                      }}
                    >
                      <Icon aria-hidden className="text-muted-foreground" />
                      {LABEL[f]()}
                      {search[f] !== undefined ? (
                        <span className="ml-auto max-w-28 truncate font-mono text-xs text-muted-foreground">
                          {shown(f, search[f])}
                        </span>
                      ) : null}
                    </CommandItem>
                  );
                })}
              </CommandGroup>
            ) : (
              <>
                {options.length > 0 ? (
                  <CommandGroup heading={LABEL[field]()}>
                    {options.map((o) => (
                      <CommandItem
                        key={o.value}
                        value={o.label ?? o.value}
                        keywords={o.hint ? [o.hint] : undefined}
                        data-checked={search[field] === o.value}
                        onSelect={() => apply(o.value)}
                      >
                        {/* Hint (endpoint, service) on the line below: deployment ids are long,
                            on one line both would be truncated. */}
                        <span className="flex min-w-0 flex-col">
                          <span className={cn("truncate", !o.label && "font-mono")}>
                            {o.label ?? o.value}
                          </span>
                          {o.hint ? (
                            <span className="truncate text-xs text-muted-foreground">{o.hint}</span>
                          ) : null}
                        </span>
                      </CommandItem>
                    ))}
                  </CommandGroup>
                ) : null}
                {/* Typed value — free-form fields only have this item; for fields with
                    suggestions it comes last, after the matching suggestions. */}
                {custom ? (
                  <CommandGroup forceMount>
                    <CommandItem forceMount value={`custom ${typed}`} onSelect={() => apply(typed)}>
                      <Plus aria-hidden className="text-muted-foreground" />
                      {m.filter_use({ value: typed })}
                    </CommandItem>
                  </CommandGroup>
                ) : null}
              </>
            )}
            {/* cmdk does not count `forceMount` items ⇒ hide it ourselves once a "Use …" item exists. */}
            {custom ? null : (
              <CommandEmpty>{field ? m.filter_type_value() : m.palette_empty()}</CommandEmpty>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

/**
 * The "Filter" button — always in the header row, does not move when filters are
 * active (like Linear); with filters it shows the number of active filters.
 */
export function FilterButton({ search }: { search: InvocationSearch }) {
  const n = FIELDS.filter((f) => search[f] !== undefined).length;
  return (
    <FilterMenu
      search={search}
      trigger={
        <Button variant="ghost" size="sm">
          <ListFilter aria-hidden />
          {m.filter_button()}
          {n > 0 ? (
            // The number is only visual; screen-reader users have the chip list below.
            <span aria-hidden>
              <Tag tone="filled">{n}</Tag>
            </span>
          ) : null}
        </Button>
      }
    />
  );
}

/**
 * Chip row of the active filters ("Field is value ×", click the value to change it)
 * and a clear-all button. With no filters nothing is rendered — it does not take up a row.
 */
export function ActiveFilters({ search }: { search: InvocationSearch }) {
  const set = useSet();
  const active = FIELDS.filter((f) => search[f] !== undefined);
  if (active.length === 0) return null;
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <ul className="flex flex-wrap gap-1.5" aria-label={m.filter_active()}>
        {active.map((f) => {
          const Icon = ICON[f];
          return (
            <Tag
              key={f}
              as="li"
              size="md"
              tone="default"
              onRemove={() => set({ [f]: undefined })}
              removeLabel={m.filter_remove({ field: LABEL[f]() })}
            >
              <span className="flex items-center gap-1.5 pr-1.5 pl-2 text-muted-foreground">
                <Icon aria-hidden className="size-3.5" />
                {LABEL[f]()}
                <span>{m.filter_is()}</span>
              </span>
              <FilterMenu
                search={search}
                field={f}
                trigger={
                  <button
                    type="button"
                    aria-label={m.filter_change({ field: LABEL[f](), value: shown(f, search[f]) })}
                    title={shown(f, search[f])}
                    className="max-w-48 truncate border-l border-border px-2 font-mono hover:bg-accent focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring"
                  >
                    {shown(f, search[f])}
                  </button>
                }
              />
            </Tag>
          );
        })}
      </ul>
      <Button
        variant="ghost"
        size="sm"
        className="text-muted-foreground"
        onClick={() => set(Object.fromEntries(FIELDS.map((f) => [f, undefined])))}
      >
        {m.filter_clear()}
      </Button>
    </div>
  );
}
