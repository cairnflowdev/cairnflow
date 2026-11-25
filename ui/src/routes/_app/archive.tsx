// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Archive: purged invocations, read back from the archive store. By UTC
// day (`GET /archive?date=`, one LIST) or by id (`GET /archive/{id}`, looking back at most
// 31 days); hash-chain check (`verify=true`) on request.
import { useQuery } from "@tanstack/react-query";
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { ArchiveIcon } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { Id } from "@/components/cairn/id";
import { QueryError } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ok, rest } from "@/data/api";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

type Search = { date?: string; id?: string };

export const Route = createFileRoute("/_app/archive")({
  validateSearch: (s: Record<string, unknown>): Search => ({
    date: typeof s.date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s.date) ? s.date : undefined,
    id: typeof s.id === "string" && /^[0-9a-f]{48}$/.test(s.id) ? s.id : undefined,
  }),
  component: Archive,
});

const today = () => new Date().toISOString().slice(0, 10);

function Archive() {
  const { date = today(), id } = Route.useSearch();
  const navigate = useNavigate({ from: "/archive" });
  const [lookup, setLookup] = useState(id ?? "");
  return (
    <div className="flex flex-col gap-4 p-6">
      <h1 className="text-xl font-semibold">{m.nav_archive()}</h1>
      <p className="text-muted-foreground">{m.archive_body()}</p>
      <div className="flex flex-wrap items-end gap-6">
        <div className="flex flex-col gap-1">
          <Label htmlFor="arch-date">{m.archive_date()}</Label>
          <Input
            id="arch-date"
            type="date"
            className="h-8 w-44"
            value={date}
            onChange={(e) =>
              e.target.value &&
              navigate({ search: (s: Search) => ({ ...s, date: e.target.value }) })
            }
          />
        </div>
        <form
          className="flex w-full max-w-[34rem] items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            const v = lookup.trim().toLowerCase();
            if (/^[0-9a-f]{48}$/.test(v))
              void navigate({ search: (s: Search) => ({ ...s, id: v }) });
          }}
        >
          <div className="flex min-w-0 flex-1 flex-col gap-1">
            <Label htmlFor="arch-id">{m.archive_lookup()}</Label>
            <Input
              id="arch-id"
              className="h-8 w-full font-mono"
              value={lookup}
              onChange={(e) => setLookup(e.target.value)}
              spellCheck={false}
            />
          </div>
          <Button type="submit" disabled={!/^[0-9a-f]{48}$/i.test(lookup.trim())}>
            {m.keyed_open()}
          </Button>
        </form>
      </div>
      {id ? <Archived id={id} /> : null}
      <Day date={date} />
    </div>
  );
}

function Day({ date }: { date: string }) {
  const q = useQuery({
    queryKey: ["archive", date],
    queryFn: () => ok(rest.GET("/archive", { params: { query: { date } } })),
  });
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <section aria-labelledby="arch-day">
      <h2 id="arch-day" className="mb-2 text-base font-semibold">
        {m.archive_day({ date })}
      </h2>
      {q.isPending ? (
        <Skeleton className="h-24 w-full" />
      ) : q.isError ? (
        <QueryError error={q.error} />
      ) : q.data.entries.length === 0 ? (
        <EmptyState icon={<ArchiveIcon />} title={m.archive_empty()} />
      ) : (
        <Table>
          <THead>
            <TH>{m.col_id()}</TH>
            <TH numeric>{m.archive_run()}</TH>
            <TH numeric>{m.archive_bytes()}</TH>
            <TH>{m.archive_written()}</TH>
          </THead>
          <TBody>
            {q.data.entries.map((e) => (
              <TRow key={`${e.id}-${e.run}`}>
                <TD priority="primary">
                  <Id
                    value={e.id}
                    wrap={(t) => (
                      <Link to="/archive" search={{ date, id: e.id }}>
                        {t}
                      </Link>
                    )}
                  />
                </TD>
                <TD numeric>{e.run}</TD>
                <TD numeric>{fmt.format(e.bytes)}</TD>
                <TD>
                  <Time ms={e.writtenAtMs} />
                </TD>
              </TRow>
            ))}
          </TBody>
        </Table>
      )}
    </section>
  );
}

function Archived({ id }: { id: string }) {
  const [verify, setVerify] = useState(false);
  const q = useQuery({
    queryKey: ["archived", id, verify],
    queryFn: () =>
      ok(
        rest.GET("/archive/{id}", {
          params: { path: { id }, query: verify ? { verify: "true" } : {} },
        }),
      ),
  });
  return (
    <section
      aria-labelledby="arch-one"
      className="flex flex-col gap-2 rounded-md border border-border p-4"
    >
      <h2 id="arch-one" className="flex items-center gap-2 text-base font-semibold">
        {m.archive_invocation()} <Id value={id} />
      </h2>
      {q.isPending ? (
        <Skeleton className="h-24 w-full" />
      ) : q.isError ? (
        <QueryError error={q.error} />
      ) : (
        <>
          <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1 text-sm">
            <dt className="text-muted-foreground">{m.archive_status()}</dt>
            <dd>{q.data.status ?? "—"}</dd>
            {q.data.purgedAtMs ? (
              <>
                <dt className="text-muted-foreground">{m.archive_purged()}</dt>
                <dd>
                  <Time ms={q.data.purgedAtMs} />
                </dd>
              </>
            ) : null}
            {q.data.failure ? (
              <>
                <dt className="text-muted-foreground">{m.archive_failure()}</dt>
                <dd className="text-destructive">{q.data.failure}</dd>
              </>
            ) : null}
            {q.data.chain ? (
              <>
                <dt className="text-muted-foreground">{m.archive_chain()}</dt>
                <dd className="font-mono text-xs break-all">
                  {q.data.chain.len} · {q.data.chain.hash}
                </dd>
              </>
            ) : null}
            {verify ? (
              <>
                <dt className="text-muted-foreground">{m.archive_verified()}</dt>
                <dd>
                  <span role="status">
                    {q.data.verified === true
                      ? m.archive_verified_ok()
                      : q.data.verified === false
                        ? m.archive_verified_bad({ error: q.data.error ?? "" })
                        : m.archive_verified_none()}
                  </span>
                </dd>
              </>
            ) : null}
          </dl>
          {q.data.output !== undefined ? (
            <figure>
              <figcaption className="text-xs text-muted-foreground">
                {m.archive_output()}
              </figcaption>
              <pre className="max-h-60 overflow-auto rounded-md bg-muted p-2 text-xs whitespace-pre-wrap break-words">
                {q.data.output}
              </pre>
            </figure>
          ) : null}
          {q.data.journal?.length ? (
            <details>
              <summary className="min-h-6 cursor-pointer">
                {m.archive_journal({ n: String(q.data.journal.length) })}
              </summary>
              <ol className="font-mono text-xs">
                {q.data.journal.map((e) => (
                  <li key={e.index}>
                    {e.index} {e.name ?? e.ty}
                  </li>
                ))}
              </ol>
            </details>
          ) : null}
          {!verify ? (
            <Button
              size="sm"
              variant="outline"
              className="self-start"
              onClick={() => setVerify(true)}
            >
              {m.archive_verify()}
            </Button>
          ) : null}
        </>
      )}
    </section>
  );
}
