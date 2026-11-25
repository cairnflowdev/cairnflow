// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Invocations: preset views, filter chips, URL state, cursor pagination
// ("Load more"), quick-look panel, local saved views, search box by id/id
// prefix.
import { useSuspenseQuery } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { RefreshCw, Search } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { Checkbox } from "@/components/cairn/checkbox";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id, shorten } from "@/components/cairn/id";
import { PartialBanner } from "@/components/cairn/partial-banner";
import { QueryError, Updated } from "@/components/cairn/query-state";
import { segmentedGroup, segmentedItem } from "@/components/cairn/segmented";
import { Skeleton } from "@/components/cairn/skeleton";
import { StatusBadge } from "@/components/cairn/status-badge";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { splitError } from "@/components/invocation/journal";
import { BulkBar } from "@/components/invocations/bulk";
import { ActiveFilters, FIELDS, FilterButton } from "@/components/invocations/filters";
import { Peek, shownStatus } from "@/components/invocations/peek";
import { SavedViews } from "@/components/invocations/saved-views";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useRows } from "@/data/hooks";
import { useInvocationPages } from "@/data/pages";
import { COUNT_CAP, invocationCount } from "@/data/queries";
import { whoamiQuery } from "@/data/session";
import {
  type InvocationSearch,
  toFilter,
  VIEWS,
  type View,
  validateInvocationSearch,
} from "@/lib/invocation-search";
import { can } from "@/lib/roles";
import { type Shortcut, usePageShortcuts } from "@/lib/shortcuts";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

/** Interval for recounting matches (the "N new" banner). */
const COUNT_REFRESH = { minMs: 10_000, maxMs: 10_000 } as const;

export const Route = createFileRoute("/_app/invocations/")({
  validateSearch: validateInvocationSearch,
  component: Invocations,
});

const VIEW_LABEL: Record<View, () => string> = {
  all: m.view_all,
  running: m.view_running,
  heldUp: m.view_held_up,
  failed: m.view_failed,
  paused: m.view_paused,
  retrying: m.view_retrying,
};

function Invocations() {
  const search = Route.useSearch();
  const navigate = useNavigate({ from: "/invocations/" });
  const { data: me } = useSuspenseQuery(whoamiQuery);
  const { filter, order } = toFilter(search);
  const pages = useInvocationPages(filter, order);
  // Recount on an interval; the list does not jump by itself — new rows show
  // a banner instead.
  const count = useRows<{ n: number }>(invocationCount(filter), { refresh: COUNT_REFRESH });
  const set = (patch: Partial<InvocationSearch>) =>
    navigate({ search: (s: InvocationSearch) => ({ ...s, ...patch }) });
  // Opening/closing peek replaces the current history entry: Back goes to the
  // previous page, not backwards through every row viewed.
  const setPeek = (peek: string | undefined) =>
    navigate({ search: (s: InvocationSearch) => ({ ...s, peek }), replace: true });
  const view = search.view ?? "all";
  const rows = pages.data?.pages.flatMap((p) => p.rows) ?? [];
  const last = pages.data?.pages.at(-1);
  const n = count.data?.rows[0]?.n;
  // Bulk actions (`/invocations/batch/*`) need `operate`; without it there are no checkboxes.
  const canAct = can(me.capabilities, "operate");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // A selection belongs to one filter: changing view/filter/id search clears it —
  // keeping it would let the bulk bar act on rows no longer visible.
  const scope = JSON.stringify([filter, order]);
  const [chosenFor, setChosenFor] = useState(scope);
  // Match count when the visible list was loaded: a later count that is higher ⇒ "N new".
  const [seen, setSeen] = useState<number | undefined>(undefined);
  if (chosenFor !== scope) {
    setChosenFor(scope);
    setSeen(undefined);
    if (selected.size > 0) setSelected(new Set());
  }
  const loadedAt = pages.dataUpdatedAt;
  const refetchCount = count.refetch;
  useEffect(() => {
    if (!loadedAt) return;
    void refetchCount().then((r) => setSeen(r.data?.rows[0]?.n));
  }, [loadedAt, refetchCount]);
  const fresh =
    seen !== undefined && n !== undefined && Number(n) < COUNT_CAP ? Number(n) - Number(seen) : 0;
  const rowRefs = useRef<(HTMLTableRowElement | null)[]>([]);
  const focusRow = (i: number) => {
    const el = rowRefs.current[Math.max(0, Math.min(rows.length - 1, i))];
    el?.focus();
    el?.scrollIntoView({ block: "nearest" });
  };
  const current = () =>
    rowRefs.current.findIndex((el) => el !== null && el === document.activeElement);
  const peekOpen = search.peek !== undefined;
  // biome-ignore lint/correctness/useExhaustiveDependencies: rebuild when the row count or peek changes
  const keys: Shortcut[] = useMemo(
    () => [
      { keys: "j", label: m.shortcut_next_row, run: () => focusRow(current() + 1) },
      { keys: "k", label: m.shortcut_prev_row, run: () => focusRow(Math.max(0, current() - 1)) },
      ...(peekOpen
        ? [{ keys: "Escape", label: m.shortcut_close_peek, run: () => setPeek(undefined) }]
        : []),
    ],
    [rows.length, peekOpen],
  );
  usePageShortcuts(keys);
  const toggle = (id: string) =>
    setSelected((cur) => {
      const next = new Set(cur);
      if (!next.delete(id)) next.add(id);
      return next;
    });

  return (
    <div className="flex h-full min-h-0">
      {/* `[&>*]:shrink-0`: this column scrolls itself (`overflow-auto`), so flex
          children shrink by default — the table frame (`overflow-x-auto`) shrinks
          with them, scrolls on its own inside and the "Load more" button covers rows. */}
      <div className="flex min-w-0 flex-1 flex-col gap-3 overflow-auto p-6 [&>*]:shrink-0">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-2">
          <h1 className="text-xl font-semibold">{m.nav_invocations()}</h1>
          <span className="text-sm text-muted-foreground tabular" aria-live="polite">
            {n === undefined
              ? ""
              : Number(n) >= COUNT_CAP
                ? m.count_at_least({ n: new Intl.NumberFormat(getLocale()).format(COUNT_CAP) })
                : Number(n) === 1
                  ? m.count_one()
                  : m.count_exact({ n: new Intl.NumberFormat(getLocale()).format(Number(n)) })}
          </span>
          {pages.dataUpdatedAt ? (
            <span className="flex items-center gap-1 text-xs text-tertiary-foreground">
              <Updated at={pages.dataUpdatedAt} />
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label={m.list_refresh()}
                title={m.list_refresh()}
                disabled={pages.isFetching}
                onClick={() => void pages.refetch()}
              >
                <RefreshCw
                  aria-hidden
                  className={cn(pages.isFetching && "motion-safe:animate-spin")}
                />
              </Button>
            </span>
          ) : null}
          <div className="ml-auto flex items-center gap-1">
            <FilterButton search={search} />
            <SavedViews tenant={me.tenant} search={search} />
          </div>
        </div>
        {/* Toolbar row: views on the left, lookup (id search, roots only) on the right.
            The filter button is in the title row; active filter chips in the row below, only when present. */}
        <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
          <nav
            aria-label={m.views()}
            // Narrow screen: the view strip scrolls horizontally inside itself, it does not widen the page.
            className={segmentedGroup}
          >
            {VIEWS.map((v) => (
              <Link
                key={v}
                to="/invocations"
                search={(s: InvocationSearch) => ({
                  ...s,
                  view: v === "all" ? undefined : v,
                  peek: undefined,
                })}
                aria-current={v === view ? "page" : undefined}
                className={segmentedItem(v === view)}
              >
                {VIEW_LABEL[v]()}
              </Link>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-3">
            <IdSearch value={search.id} onChange={(id) => set({ id, peek: undefined })} />
            <Checkbox
              checked={search.root !== 0 && !search.parent}
              disabled={!!search.parent}
              onCheckedChange={(c) => set({ root: c ? undefined : 0 })}
            >
              {m.filter_root_only()}
            </Checkbox>
          </div>
        </div>
        <ActiveFilters search={search} />
        {fresh > 0 ? (
          <Button
            variant="outline"
            size="sm"
            className="self-start"
            onClick={() => void pages.refetch()}
          >
            {m.list_new({ n: new Intl.NumberFormat(getLocale()).format(fresh) })}
          </Button>
        ) : null}
        {canAct ? (
          <BulkBar
            key={scope}
            selected={[...selected]}
            total={n === undefined ? undefined : Number(n)}
            filter={filter}
            onClear={() => setSelected(new Set())}
          />
        ) : null}
        {pages.isPending ? (
          <div aria-busy="true" className="flex flex-col gap-2">
            {[0, 1, 2, 3, 4].map((i) => (
              <Skeleton key={i} className="h-8 w-full" />
            ))}
          </div>
        ) : pages.isError ? (
          <QueryError error={pages.error} />
        ) : rows.length === 0 ? (
          FIELDS.some((f) => search[f] !== undefined) ||
          view !== "all" ||
          search.parent ||
          search.id ? (
            <EmptyState title={m.invocations_empty()}>{m.invocations_empty_body()}</EmptyState>
          ) : (
            // Empty with no filter at all: the tenant has no invocation yet — do not say "remove a filter".
            <EmptyState title={m.invocations_none_yet()}>
              {m.invocations_none_yet_body()}
            </EmptyState>
          )
        ) : (
          <>
            <PartialBanner warnings={pages.data.pages.flatMap((p) => p.warnings)} />
            <Table>
              <THead>
                {canAct ? (
                  <TH className="w-8">
                    <Checkbox
                      aria-label={m.select_page()}
                      checked={rows.length > 0 && rows.every((r) => selected.has(r.id))}
                      indeterminate={
                        rows.some((r) => selected.has(r.id)) &&
                        !rows.every((r) => selected.has(r.id))
                      }
                      onCheckedChange={(c) =>
                        setSelected(c ? new Set(rows.map((r) => r.id)) : new Set())
                      }
                    />
                  </TH>
                ) : null}
                <TH>{m.col_id()}</TH>
                <TH>{m.col_target()}</TH>
                <TH>{m.col_status()}</TH>
                <TH>{m.col_created()}</TH>
                <TH>{m.col_transitioned()}</TH>
                <TH numeric>{m.col_attempt()}</TH>
                <TH>{m.col_last_error()}</TH>
              </THead>
              <TBody>
                {rows.map((r, i) => (
                  <TRow
                    key={r.id}
                    ref={(el) => {
                      rowRefs.current[i] = el;
                    }}
                    // Focusable row: J/K move between rows, Space opens/closes peek, Enter opens
                    // the full page.
                    tabIndex={0}
                    aria-selected={search.peek === r.id}
                    className={cn(
                      "cursor-pointer outline-none hover:bg-accent focus-visible:bg-accent focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring",
                      search.peek === r.id && "bg-accent",
                    )}
                    onClick={(e) => {
                      if ((e.target as HTMLElement).closest("a,button,input,[role=checkbox]"))
                        return;
                      setPeek(r.id);
                    }}
                    onKeyDown={(e) => {
                      if (e.target !== e.currentTarget) return;
                      if (e.key === "Enter") {
                        e.preventDefault();
                        void navigate({ to: "/invocations/$id", params: { id: r.id } });
                      } else if (e.key === " ") {
                        e.preventDefault();
                        setPeek(search.peek === r.id ? undefined : r.id);
                      }
                    }}
                  >
                    {canAct ? (
                      <TD priority="primary">
                        <Checkbox
                          aria-label={m.select_row({ id: shorten(r.id) })}
                          checked={selected.has(r.id)}
                          onCheckedChange={() => toggle(r.id)}
                        />
                      </TD>
                    ) : null}
                    <TD>
                      <Id
                        value={r.id}
                        wrap={(t) => (
                          <Link
                            to="/invocations/$id"
                            params={{ id: r.id }}
                            className="hover:underline"
                          >
                            {t}
                          </Link>
                        )}
                      />
                    </TD>
                    <TD priority="primary">
                      {/* Long names are cut with "…" — later columns stay inside the frame. */}
                      <span
                        className="block max-w-[20rem] truncate"
                        title={`${r.service}/${r.handler}${r.key ? ` · ${r.key}` : ""}`}
                      >
                        {r.service}/{r.handler}
                        {r.key ? <span className="text-muted-foreground"> · {r.key}</span> : null}
                      </span>
                    </TD>
                    <TD priority="status">
                      <StatusBadge status={shownStatus(r)} />
                    </TD>
                    <TD>
                      <Time ms={Date.parse(r.created_at)} />
                    </TD>
                    <TD priority="hidden">
                      <Time ms={Date.parse(r.transitioned_at)} />
                    </TD>
                    <TD numeric>{r.attempt ?? ""}</TD>
                    {/* The error column takes the rest of the table and is cut with "…" (full error in `title`):
                        `truncate` on a `<td>` has no effect, the cell grows with the text. */}
                    <TD priority="hidden" className="w-full max-w-0">
                      <span
                        className="block truncate"
                        title={r.failure_message ?? r.last_failure_message ?? undefined}
                      >
                        {splitError(r.failure_message ?? r.last_failure_message ?? "").head}
                      </span>
                    </TD>
                  </TRow>
                ))}
              </TBody>
            </Table>
            {last?.next ? (
              <Button
                variant="outline"
                className="self-center"
                disabled={pages.isFetchingNextPage}
                onClick={() => void pages.fetchNextPage()}
              >
                {m.load_more()}
              </Button>
            ) : null}
          </>
        )}
      </div>
      {search.peek ? <Peek id={search.peek} onClose={() => setPeek(undefined)} /> : null}
    </div>
  );
}

/** Search box by id or id prefix: accepts hex only. */
function IdSearch({
  value,
  onChange,
}: {
  value?: string;
  onChange: (id: string | undefined) => void;
}) {
  const [text, setText] = useState(value ?? "");
  const bad = text !== "" && !/^[0-9a-fA-F]{1,48}$/.test(text.trim());
  return (
    <form
      className="relative"
      onSubmit={(e) => {
        e.preventDefault();
        if (!bad) onChange(text.trim() === "" ? undefined : text.trim().toLowerCase());
      }}
    >
      <label htmlFor="id-search" className="sr-only">
        {m.filter_id()}
      </label>
      <Search
        aria-hidden
        className="pointer-events-none absolute top-2 left-2 size-4 text-muted-foreground"
      />
      <Input
        id="id-search"
        className="h-8 w-56 pl-7 font-mono"
        placeholder={m.filter_id()}
        value={text}
        onChange={(e) => setText(e.target.value)}
        aria-invalid={bad || undefined}
        aria-describedby={bad ? "id-search-error" : undefined}
        spellCheck={false}
      />
      {bad ? (
        // Overlays below the box (does not make the toolbar row taller).
        <span
          id="id-search-error"
          className="absolute top-full left-0 z-10 mt-1 rounded-md border border-border bg-popover px-2 py-0.5 text-xs whitespace-nowrap text-destructive shadow-sm"
        >
          {m.filter_id_invalid()}
        </span>
      ) : null}
    </form>
  );
}
