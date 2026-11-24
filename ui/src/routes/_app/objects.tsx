// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Objects & State: pick a virtual object service ⇒
// object list (`sys_object`, the `ObjectKeys` index checked at read time: state still present
// or invocations still present), filter by key, click to open; or type a key directly. Opening a key
// ⇒ the object's state and links to its invocations.
import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { ChevronRight, Database } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { Skeleton } from "@/components/cairn/skeleton";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { StateTab } from "@/components/invocation/tabs";
import { KeyedToolbar, useKeyedServices, validateKeySearch } from "@/components/keyed/key-form";
import { Input } from "@/components/ui/input";
import { useRows } from "@/data/hooks";
import { queries } from "@/data/queries";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/objects")({
  validateSearch: validateKeySearch,
  component: Objects,
});

function Objects() {
  const { service, key } = Route.useSearch();
  const navigate = useNavigate({ from: "/objects" });
  const { services, list, name } = useKeyedServices("virtualObject", service);
  const open = (k: string | undefined) => navigate({ search: { service: name, key: k } });
  return (
    <div className="flex flex-col gap-4 p-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-xl font-semibold">{m.nav_objects()}</h1>
        <p className="text-sm text-muted-foreground">{m.objects_intro()}</p>
      </div>
      {services.isError ? (
        <QueryError error={services.error} />
      ) : services.isPending ? null : list.length === 0 && !service ? (
        <EmptyState icon={<Database aria-hidden />} title={m.objects_none()}>
          {m.objects_none_body()}
        </EmptyState>
      ) : (
        <>
          <KeyedToolbar
            list={list}
            service={name}
            objectKey={key}
            onService={(s) => navigate({ search: { service: s } })}
            onOpen={open}
          />
          {name && key ? (
            <>
              <nav aria-label={m.breadcrumb()} className="flex items-center gap-1.5 text-sm">
                <Link
                  to="/objects"
                  search={{ service: name }}
                  className="text-muted-foreground hover:text-foreground hover:underline"
                >
                  {m.objects_all_keys({ service: name })}
                </Link>
                <ChevronRight aria-hidden className="size-4 text-muted-foreground" />
                <span aria-current="page" className="font-mono">
                  {key}
                </span>
              </nav>
              <Link
                to="/invocations"
                search={{ service: name, key, root: 0 }}
                className="self-start text-sm text-primary underline-offset-4 hover:underline"
              >
                {m.object_invocations()}
              </Link>
              <StateTab service={name} objectKey={key} />
            </>
          ) : name ? (
            <ObjectList service={name} onOpen={open} />
          ) : null}
        </>
      )}
    </div>
  );
}

/** Cap on objects per read — more than that, filter by key. */
const OBJECTS_MAX = 500;
/** `invocations` counts up to 16 (16 = at least 16). */
const HELD_CAP = 16;

type ObjectRow = { key: string; scope: string | null; has_state: boolean; invocations: number };

/** Objects of a service (`sys_object`): one row per key, click to open. */
function ObjectList({ service, onOpen }: { service: string; onOpen: (key: string) => void }) {
  const q = useRows<ObjectRow>(queries.objects(service, OBJECTS_MAX));
  const [filter, setFilter] = useState("");
  if (q.isPending) return <Skeleton className="h-32 w-full" />;
  if (q.isError) return <QueryError error={q.error} />;
  const all = q.data.rows;
  if (all.length === 0)
    return (
      <EmptyState title={m.objects_keys_empty({ service })}>
        {m.objects_keys_empty_body()}
      </EmptyState>
    );
  const needle = filter.trim().toLowerCase();
  const rows = needle ? all.filter((r) => r.key.toLowerCase().includes(needle)) : all;
  const scoped = all.some((r) => r.scope);
  const fmt = new Intl.NumberFormat(getLocale());
  return (
    <section aria-labelledby="obj-keys" className="flex flex-col gap-2">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-2">
        <h2 id="obj-keys" className="text-base font-semibold">
          {m.objects_list({ service })}
        </h2>
        <span className="text-sm text-muted-foreground tabular">
          {all.length >= OBJECTS_MAX
            ? m.objects_count_cap({ n: fmt.format(OBJECTS_MAX) })
            : all.length === 1
              ? m.objects_count_one()
              : m.objects_count({ n: fmt.format(all.length) })}
        </span>
        <Input
          aria-label={m.objects_filter()}
          placeholder={m.objects_filter()}
          className="ml-auto h-8 w-56 font-mono"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          spellCheck={false}
        />
      </div>
      {rows.length === 0 ? (
        <EmptyState variant="inline" title={m.query_filter_none()} />
      ) : (
        <Table>
          <THead>
            <TH>{m.filter_key()}</TH>
            {scoped ? <TH>{m.objects_scope()}</TH> : null}
            <TH>{m.objects_state()}</TH>
            <TH numeric>{m.objects_held()}</TH>
          </THead>
          <TBody>
            {rows.map((r) => (
              <TRow
                key={`${r.scope ?? ""}/${r.key}`}
                className="cursor-pointer hover:bg-accent"
                onClick={(e) => {
                  if ((e.target as HTMLElement).closest("a,button")) return;
                  onOpen(r.key);
                }}
              >
                <TD priority="primary" className="font-mono whitespace-nowrap">
                  {/* A real link (keyboard, open in new tab); the whole row is clickable too. */}
                  <Link to="/objects" search={{ service, key: r.key }} className="hover:underline">
                    {r.key}
                  </Link>
                </TD>
                {scoped ? (
                  <TD className="font-mono text-muted-foreground">{r.scope ?? "—"}</TD>
                ) : null}
                <TD priority="status">
                  {r.has_state ? (
                    m.objects_has_state()
                  ) : (
                    <span className="text-muted-foreground">{m.objects_no_state()}</span>
                  )}
                </TD>
                <TD numeric>
                  {Number(r.invocations) === 0 ? (
                    <span className="text-muted-foreground">0</span>
                  ) : Number(r.invocations) >= HELD_CAP ? (
                    `${HELD_CAP}+`
                  ) : (
                    fmt.format(Number(r.invocations))
                  )}
                </TD>
              </TRow>
            ))}
          </TBody>
        </Table>
      )}
    </section>
  );
}
