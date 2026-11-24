// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Invocation detail:
// header, "why" banner, tabs. Running ⇒ refresh every 2 s; stops once it has
// ended or the journal is > 1 000 entries (with the reason).

import { useSuspenseQuery } from "@tanstack/react-query";
import { createFileRoute, Link } from "@tanstack/react-router";
import { useMemo, useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryState } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { StatusBadge } from "@/components/cairn/status-badge";
import { tabItem, tabList } from "@/components/cairn/tabs";
import { Time } from "@/components/cairn/time";
import { Actions } from "@/components/invocation/actions";
import { type JournalRow, parseRows, settle } from "@/components/invocation/journal";
import { StreamsTab } from "@/components/invocation/streams-tab";
import {
  ArtifactsTab,
  JournalTab,
  LogTab,
  StateTab,
  TABS,
  type Tab,
  TreeTab,
} from "@/components/invocation/tabs";
import { TraceTab } from "@/components/invocation/trace-tab";
import { useQueue, Why } from "@/components/invocation/why";
import { type InvocationDetail, shownStatus } from "@/components/invocations/peek";
import { useToast } from "@/components/ui/toast";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { whoamiQuery } from "@/data/session";
import { copyText } from "@/lib/clipboard";
import { type PaletteAction, usePaletteActions } from "@/lib/palette";
import { can } from "@/lib/roles";
import { m } from "@/paraglide/messages";

/** Auto-refresh cap (as Trigger.dev does): a longer journal turns it off, with the reason. */
const LIVE_MAX_ENTRIES = 1000;
const LIVE = { minMs: 2000, maxMs: 2000 } as const;

export const Route = createFileRoute("/_app/invocations/$id")({
  validateSearch: (s: Record<string, unknown>): { tab?: Tab; entry?: number } => ({
    tab: TABS.includes(s.tab as Tab) ? (s.tab as Tab) : undefined,
    entry: Number.isInteger(Number(s.entry)) && s.entry !== undefined ? Number(s.entry) : undefined,
  }),
  component: Detail,
});

const TAB_LABEL: Record<Tab, () => string> = {
  journal: m.tab_journal,
  log: m.tab_log,
  trace: m.tab_trace,
  streams: m.tab_streams,
  tree: m.tab_tree,
  state: m.tab_state,
  artifacts: m.tab_artifacts,
};

function Detail() {
  const { id } = Route.useParams();
  const { tab = "journal", entry } = Route.useSearch();
  const valid = /^[0-9a-f]{48}$/.test(id);
  const { data: me } = useSuspenseQuery(whoamiQuery);
  const caps = me.capabilities;
  // Restart (from step n) is the job of `operate`.
  const canOperate = can(caps, "operate");
  const [restartFrom, setRestartFrom] = useState<number | null>(null);
  const detail = useRows<InvocationDetail>(queries.invocation(id), {
    enabled: valid,
    refresh: LIVE,
  });
  const r = detail.data?.rows[0];
  const ended = r ? r.class === "completed" || r.class === "failed" : false;
  const tooLong = (r?.journal_len ?? 0) > LIVE_MAX_ENTRIES;
  const live = !!r && !ended && !tooLong;
  const journal = useRows<JournalRow>(queries.journalLite(id, 0), {
    enabled: valid && !!r,
    refresh: live ? LIVE : false,
  });
  const entries = journal.data ? settle(parseRows(journal.data.rows), ended) : [];
  const queue = useQueue(r, can(caps, "cluster"));
  const toast = useToast();
  const palette: PaletteAction[] = useMemo(
    () => [
      {
        id: "copy-id",
        label: m.id_copy(),
        run: () =>
          void copyText(id).then((ok) =>
            toast.add(ok ? { title: m.id_copied() } : { title: m.id_copy_failed(), type: "error" }),
          ),
      },
    ],
    [id, toast],
  );
  usePaletteActions(palette);

  if (!valid) {
    return (
      <div className="p-6">
        <EmptyState title={m.not_found_title()}>{m.invocation_bad_id()}</EmptyState>
      </div>
    );
  }
  return (
    <div className="flex flex-col gap-4 p-6">
      <QueryState q={detail} skeleton={<Skeleton className="h-28 w-full" />}>
        {() =>
          r ? (
            <>
              <header className="flex flex-col gap-2">
                <div className="flex flex-wrap items-center gap-3">
                  <h1 className="min-w-0 text-xl font-semibold [overflow-wrap:anywhere]">
                    {r.service}/{r.handler}
                    {r.key ? <span className="text-muted-foreground"> · {r.key}</span> : null}
                  </h1>
                  <StatusBadge
                    status={shownStatus(r, queue.inLine)}
                    attempts={r.attempt ?? undefined}
                  />
                  <div className="ml-auto">
                    <Actions
                      r={r}
                      caps={caps}
                      entries={entries}
                      restartFrom={restartFrom}
                      onRestartFrom={setRestartFrom}
                    />
                  </div>
                </div>
                <dl className="flex flex-wrap gap-x-6 gap-y-1 text-sm">
                  <Meta label={m.col_id()}>
                    <Id value={r.id} />
                  </Meta>
                  {r.deployment ? (
                    <Meta label={m.col_deployment()}>
                      <Id
                        value={r.pinned_deployment ?? r.deployment}
                        full
                        className="min-w-0 whitespace-normal break-all"
                        wrap={(t) => (
                          <Link
                            to="/deployments/$id"
                            params={{ id: r.pinned_deployment ?? r.deployment ?? "" }}
                          >
                            {t}
                          </Link>
                        )}
                      />
                      {r.pinned_deployment && r.pinned_deployment !== r.deployment ? (
                        <span className="text-muted-foreground">
                          {" "}
                          ({m.detail_created_on({ id: r.deployment })})
                        </span>
                      ) : null}
                    </Meta>
                  ) : null}
                  <Meta label={m.col_created()}>
                    <Time ms={Date.parse(r.created_at)} />
                  </Meta>
                  <Meta label={m.col_transitioned()}>
                    <Time ms={Date.parse(r.transitioned_at)} />
                  </Meta>
                  {r.ended_at ? (
                    <Meta label={m.detail_ended()}>
                      <Time ms={Date.parse(r.ended_at)} />
                    </Meta>
                  ) : null}
                  {r.parent_id ? (
                    <Meta label={m.filter_parent()}>
                      <Id
                        value={r.parent_id}
                        wrap={(t) => (
                          <Link to="/invocations/$id" params={{ id: r.parent_id ?? "" }}>
                            {t}
                          </Link>
                        )}
                      />
                    </Meta>
                  ) : null}
                  {r.schedule ? <Meta label={m.detail_schedule()}>{r.schedule}</Meta> : null}
                  {r.tags ? <Meta label={m.detail_tags()}>{tagsText(r.tags)}</Meta> : null}
                </dl>
              </header>
              <Why r={r} entries={entries} queue={queue} />
              {tooLong && !ended ? (
                <p role="status">{m.detail_live_off({ n: String(LIVE_MAX_ENTRIES) })}</p>
              ) : null}
              <nav aria-label={m.detail_tabs()} className={tabList}>
                {TABS.filter((t) => t !== "state" || r.key).map((t) => (
                  <Link
                    key={t}
                    to="/invocations/$id"
                    params={{ id }}
                    search={{ tab: t === "journal" ? undefined : t }}
                    aria-current={t === tab ? "page" : undefined}
                    className={tabItem(t === tab)}
                  >
                    {TAB_LABEL[t]()}
                  </Link>
                ))}
              </nav>
              <section aria-label={TAB_LABEL[tab]()}>
                {tab === "journal" ? (
                  <QueryState q={journal}>
                    {() => (
                      <JournalTab
                        id={id}
                        entries={entries}
                        open={entry}
                        onRestartFrom={canOperate ? setRestartFrom : undefined}
                      />
                    )}
                  </QueryState>
                ) : tab === "log" ? (
                  <LogTab id={id} live={live} />
                ) : tab === "trace" ? (
                  <TraceTab id={id} live={live} r={r} entries={entries} />
                ) : tab === "streams" ? (
                  <StreamsTab id={id} live={live} />
                ) : tab === "tree" ? (
                  <TreeTab id={id} rootId={r.root_id ?? r.id} />
                ) : tab === "state" && r.key ? (
                  <StateTab service={r.service} objectKey={r.key} />
                ) : (
                  <ArtifactsTab id={id} />
                )}
              </section>
            </>
          ) : (
            <EmptyState title={m.not_found_title()}>{m.invocation_missing()}</EmptyState>
          )
        }
      </QueryState>
    </div>
  );
}

function Meta({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex min-w-0 gap-1.5">
      <dt className="shrink-0 text-muted-foreground">{label}</dt>
      <dd className="min-w-0">{children}</dd>
    </div>
  );
}

function tagsText(raw: string): string {
  try {
    const t = JSON.parse(raw) as Record<string, string> | [string, string][];
    const pairs = Array.isArray(t) ? t : Object.entries(t);
    return pairs.map(([k, v]) => `${k}=${v}`).join(", ");
  } catch {
    return raw;
  }
}
