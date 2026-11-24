// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Cluster screen (cluster-admin): cluster version, nodes, key-space bar +
// range table, split/merge (type to confirm), split suggestions, operation
// log, shared WAL, scrubber, trash, GC plan and barriers, quarantine. Everything
// is read from the cluster REST API (the matching `sys_*` tables run these same GETs).
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { type ReactNode, useState } from "react";
import { actionError, ConfirmDialog } from "@/components/cairn/confirm";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { RequireCapability } from "@/components/cairn/role-gate";
import { RowActions, RowButton } from "@/components/cairn/row-actions";
import { Skeleton } from "@/components/cairn/skeleton";
import { type Priority, Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { Time } from "@/components/cairn/time";
import { KeyspaceBar } from "@/components/cluster/keyspace-bar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { useCapabilities } from "@/data/session";
import { midKey, segments, shortNode, splitKeyProblem } from "@/lib/keyspace";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export const Route = createFileRoute("/_app/cluster")({ component: ClusterPage });

/** The whole screen needs the `cluster` capability — opened directly by URL, say why. */
function ClusterPage() {
  const caps = useCapabilities();
  return (
    <RequireCapability have={caps} need="cluster">
      <Cluster />
    </RequireCapability>
  );
}

type Range = components["schemas"]["RangeView"];
type Op =
  /** `at`: first key of the right half (16 hex) — absent ⇒ the server splits in the middle. */
  { kind: "split"; range: number; at?: string } | { kind: "merge"; left: number; right: number };

const POLL = 5_000;

function Section({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <section aria-labelledby={id} className="flex flex-col gap-2">
      <h2 id={id} className="text-base font-semibold">
        {title}
      </h2>
      {children}
    </section>
  );
}

/** Compact table by column: `cols` is [header, cell]; empty ⇒ `empty` (inline empty message). */
function ColumnTable<T>({
  rows,
  cols,
  rowKey,
  empty,
}: {
  rows: T[];
  /**
   * `[header, cell, numeric column?, narrow-screen slot?]` — a numeric column right-aligns both
   * header and cell (`numeric` of `TH`/`TD`); the narrow-screen slot is the `priority` of `TD`,
   * default first column `primary`, the rest `secondary`.
   */
  cols: [string, (r: T) => ReactNode, boolean?, Priority?][];
  rowKey: (r: T, i: number) => string;
  empty: string;
}) {
  if (rows.length === 0) return <EmptyState variant="inline" title={empty} />;
  return (
    <Table>
      <THead>
        {cols.map(([h, , numeric]) => (
          <TH key={h} numeric={numeric}>
            {h}
          </TH>
        ))}
      </THead>
      <TBody>
        {rows.map((r, i) => (
          <TRow key={rowKey(r, i)} className="align-top">
            {cols.map(([h, cell, numeric, priority], j) => (
              <TD
                key={h}
                numeric={numeric}
                priority={priority ?? (j === 0 ? "primary" : "secondary")}
              >
                {cell(r)}
              </TD>
            ))}
          </TRow>
        ))}
      </TBody>
    </Table>
  );
}

/** Compact duration: `850 ms`, `12 s`, `4 min`, `3 h`. */
function duration(ms: number): string {
  const f = new Intl.NumberFormat(getLocale(), { maximumFractionDigits: 1 });
  if (ms < 1000) return `${f.format(ms)} ms`;
  if (ms < 60_000) return `${f.format(ms / 1000)} s`;
  if (ms < 3_600_000) return `${f.format(ms / 60_000)} min`;
  return `${f.format(ms / 3_600_000)} h`;
}

const json = (v: unknown) => (v == null ? "—" : typeof v === "string" ? v : JSON.stringify(v));

function Cluster() {
  const qc = useQueryClient();
  const toast = useToast();
  const fmt = new Intl.NumberFormat(getLocale());
  const view = useQuery({
    queryKey: ["cluster"],
    queryFn: () => ok(rest.GET("/cluster")),
    refetchInterval: POLL,
  });
  const version = useQuery({
    queryKey: ["cluster-version"],
    queryFn: () => ok(rest.GET("/admin/cluster-version")),
  });
  const nodes = useQuery({
    queryKey: ["cluster-nodes"],
    queryFn: () => ok(rest.GET("/cluster/nodes", { params: { query: { scope: "cluster" } } })),
    refetchInterval: POLL,
  });
  const ops = useQuery({
    queryKey: ["range-ops"],
    // The log of every node — previously only of the node serving the console.
    queryFn: () => ok(rest.GET("/ranges/ops", { params: { query: { scope: "cluster" } } })),
    refetchInterval: POLL,
  });
  const suggestions = useQuery({
    queryKey: ["range-suggestions"],
    queryFn: () => ok(rest.GET("/ranges/suggestions", { params: { query: { scope: "cluster" } } })),
    refetchInterval: POLL,
  });
  const wal = useQuery({
    queryKey: ["nodewal"],
    queryFn: () => ok(rest.GET("/admin/nodewal", { params: { query: { scope: "cluster" } } })),
  });
  const scrub = useQuery({
    queryKey: ["scrub"],
    queryFn: () => ok(rest.GET("/admin/scrub", { params: { query: { scope: "cluster" } } })),
  });
  const trash = useQuery({ queryKey: ["trash"], queryFn: () => ok(rest.GET("/admin/trash")) });
  const gc = useQuery({ queryKey: ["gc-plan"], queryFn: () => ok(rest.GET("/admin/gc/plan")) });
  const barriers = useQuery({
    queryKey: ["gc-barriers"],
    queryFn: () => ok(rest.GET("/admin/gc/barriers")),
  });
  const quarantine = useQuery({
    queryKey: ["quarantine"],
    queryFn: () => ok(rest.GET("/admin/quarantine")),
  });
  const [selected, setSelected] = useState<number | null>(null);
  const [op, setOp] = useState<Op | null>(null);
  // Split dialog: optional split-point box; empty ⇒ middle.
  const [splitAt, setSplitAt] = useState("");
  const [splitTried, setSplitTried] = useState(false);

  const run = useMutation({
    mutationFn: (o: Op) =>
      o.kind === "split"
        ? ok(
            rest.POST("/ranges/{id}/split", {
              params: { path: { id: o.range }, query: o.at ? { at: o.at } : undefined },
            }),
          )
        : ok(rest.POST("/ranges/merge", { params: { query: { left: o.left, right: o.right } } })),
    onSuccess: async (out, o) => {
      setOp(null);
      setSelected(null);
      toast.add({
        title:
          o.kind === "split"
            ? m.cluster_split_done({ range: String(o.range), n: String(out.ranges.length) })
            : m.cluster_merge_done({ left: String(o.left), right: String(o.right) }),
      });
      await qc.invalidateQueries({ queryKey: ["cluster"] });
      await qc.invalidateQueries({ queryKey: ["range-ops"] });
      await qc.invalidateQueries({ queryKey: ["cluster-nodes"] });
    },
  });

  const ranges: Range[] = view.data?.ranges ?? [];
  // Right neighbour of the selected range (by key) — merge only with an adjacent range.
  const ordered = segments(ranges);
  const at = ordered.findIndex((s) => s.range === selected);
  const right = at >= 0 ? ordered[at + 1]?.range : undefined;
  const splitRange = op?.kind === "split" ? ranges.find((r) => r.range === op.range) : undefined;
  const splitProblem = splitRange
    ? splitKeyProblem(splitAt, splitRange.start, splitRange.end)
    : null;
  const splitBad =
    splitTried && splitProblem && splitRange
      ? splitProblem === "format"
        ? m.cluster_split_at_format()
        : m.cluster_split_at_outside({ start: splitRange.start, end: splitRange.end })
      : null;

  return (
    <div className="flex flex-col gap-8 p-6">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-xl font-semibold">{m.nav_cluster()}</h1>
        {version.data ? (
          <p className="text-sm text-muted-foreground">
            {m.cluster_version({
              version: String(version.data.version),
              min: String(version.data.minBinary ?? "—"),
            })}
            {version.data.upgrading ? ` · ${m.cluster_upgrading()}` : ""}
          </p>
        ) : null}
      </div>

      <Section id="cl-nodes" title={m.cluster_nodes()}>
        {nodes.isError ? (
          <QueryError error={nodes.error} />
        ) : nodes.data ? (
          <>
            {nodes.data.partial ? (
              <p
                role="status"
                className="rounded-md border border-input bg-muted px-3 py-2 text-sm"
              >
                {m.cluster_missing({ nodes: nodes.data.missing.join(", ") })}
              </p>
            ) : null}
            <ColumnTable
              rows={nodes.data.rows}
              rowKey={(r) => r.node}
              empty={m.cluster_none()}
              cols={[
                [m.cluster_node(), (r) => <span className="font-mono">{shortNode(r.node)}</span>],
                [
                  m.cluster_leading(),
                  (r) => (r.leading.length ? r.leading.map((n) => `#${n}`).join(", ") : "—"),
                ],
                [
                  m.cluster_put_p99(),
                  (r) => (r.p99PutMs != null ? `${fmt.format(r.p99PutMs)} ms` : "—"),
                  true,
                ],
                // `slow`: the published slowness score (`health::Published::slow`) — print the number itself,
                // `null` = not slow.
                // Narrow screen: unlabelled numbers on row 2 are unreadable — the slowness score and cache
                // only on wide screens; draining is a node state.
                [
                  m.cluster_slow(),
                  (r) => (r.slow != null ? fmt.format(r.slow) : "—"),
                  true,
                  "hidden",
                ],
                [
                  m.cluster_draining(),
                  (r) => (r.draining ? m.cluster_yes() : "—"),
                  false,
                  "status",
                ],
                [
                  m.cluster_disk_cache(),
                  (r) =>
                    r.diskCacheBytes != null
                      ? `${fmt.format(Math.round(r.diskCacheBytes / 1_048_576))} MiB · ${Math.round((r.diskCacheHitRatio ?? 0) * 100)}%`
                      : "—",
                  true,
                  "hidden",
                ],
              ]}
            />
          </>
        ) : (
          <Skeleton className="h-16 w-full" />
        )}
      </Section>

      <Section id="cl-keyspace" title={m.cluster_keyspace()}>
        {view.isError ? (
          <QueryError error={view.error} />
        ) : view.data ? (
          <>
            <KeyspaceBar
              ranges={ranges}
              selected={selected}
              onSelect={(r) => setSelected(selected === r ? null : r)}
            />
            <div className="flex flex-wrap items-center gap-2 text-sm">
              {selected === null ? (
                <span className="text-muted-foreground">{m.cluster_pick_range()}</span>
              ) : (
                <>
                  <span>{m.cluster_selected({ range: String(selected) })}</span>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setOp({ kind: "split", range: selected })}
                  >
                    {m.cluster_split()}
                  </Button>
                  {right !== undefined ? (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => setOp({ kind: "merge", left: selected, right })}
                    >
                      {m.cluster_merge_with({ right: String(right) })}
                    </Button>
                  ) : null}
                </>
              )}
            </div>
            <ColumnTable
              rows={[...ranges].sort((a, b) => (a.start < b.start ? -1 : 1))}
              rowKey={(r) => String(r.range)}
              empty={m.cluster_none()}
              cols={[
                [m.cluster_range(), (r) => <span className="tabular">#{r.range}</span>],
                [
                  m.cluster_keys(),
                  (r) => (
                    <span className="font-mono text-xs whitespace-nowrap">{`${r.start}–${r.end}`}</span>
                  ),
                ],
                [
                  m.cluster_leader(),
                  (r) => <span className="font-mono">{r.leader ? shortNode(r.leader) : "—"}</span>,
                ],
                [m.cluster_term(), (r) => r.term ?? "—", true],
                [m.cluster_intent(), (r) => json(r.intent)],
              ]}
            />
          </>
        ) : (
          <Skeleton className="h-24 w-full" />
        )}
      </Section>

      <Section id="cl-suggest" title={m.cluster_suggestions()}>
        {suggestions.isError ? (
          <QueryError error={suggestions.error} />
        ) : (
          <ColumnTable
            rows={suggestions.data ?? []}
            rowKey={(r) => `${r.range}-${r.at ?? ""}-${r.mergeWith ?? ""}`}
            empty={m.cluster_no_suggestions()}
            cols={[
              [m.cluster_range(), (r) => r.range],
              [m.cluster_reason(), (r) => json(r.reason)],
              [m.cluster_blocked_by(), (r) => r.blockedBy ?? "—"],
              [
                "",
                (r) => {
                  // Merge suggestion: left range `range`, right range
                  // `mergeWith`; still goes through the confirm dialog like a manual merge.
                  const right = r.mergeWith;
                  return (
                    <RowActions>
                      {r.reason === "merge" && right !== undefined ? (
                        <RowButton onClick={() => setOp({ kind: "merge", left: r.range, right })}>
                          {m.cluster_merge_with({ right: String(right) })}
                        </RowButton>
                      ) : (
                        <RowButton
                          onClick={() => {
                            // The suggestion's split point is prefilled; editable.
                            setSplitAt(r.at ?? "");
                            setOp({ kind: "split", range: r.range });
                          }}
                        >
                          {m.cluster_split()}
                        </RowButton>
                      )}
                    </RowActions>
                  );
                },
              ],
            ]}
          />
        )}
      </Section>

      <Section id="cl-ops" title={m.cluster_ops()}>
        {ops.isError ? (
          <QueryError error={ops.error} />
        ) : (
          <ColumnTable
            rows={[...(ops.data?.ops ?? [])].reverse()}
            rowKey={(r) => r.id}
            empty={m.cluster_no_ops()}
            cols={[
              [m.cluster_op_kind(), (r) => r.kind],
              [
                m.cluster_op_ranges(),
                (r) => <span className="whitespace-nowrap">{opRanges(r)}</span>,
              ],
              [
                m.cluster_op_at(),
                (r) => (r.at ? <span className="font-mono text-xs">{r.at}</span> : "—"),
              ],
              [
                m.cluster_node(),
                (r) => <span className="font-mono">{shortNode(opNode(r.id))}</span>,
              ],
              [m.cluster_op_state(), (r) => json(r.state), false, "status"],
              [
                m.cluster_op_elapsed(),
                (r) => (r.elapsedMs != null ? `${fmt.format(Math.round(r.elapsedMs))} ms` : "—"),
                true,
              ],
              [
                m.cluster_op_error(),
                (r) => (r.error ? <span className="text-destructive">{r.error}</span> : "—"),
              ],
            ]}
          />
        )}
      </Section>

      <Section id="cl-wal" title={m.cluster_wal()}>
        {wal.isError ? (
          <QueryError error={wal.error} />
        ) : (
          <ColumnTable
            rows={wal.data?.rows ?? []}
            rowKey={(r, i) => `${r.node}-${r.log}-${i}`}
            empty={m.cluster_no_wal()}
            cols={[
              [m.cluster_node(), (r) => <span className="font-mono">{shortNode(r.node)}</span>],
              [
                m.cluster_wal_log(),
                (r) => <span className="font-mono text-xs whitespace-nowrap">{r.log}</span>,
              ],
              [m.cluster_wal_pinned_by(), (r) => r.pinnedByRange ?? "—"],
              [
                m.cluster_wal_pinned_for(),
                (r) => (r.pinnedForMs != null ? duration(r.pinnedForMs) : "—"),
                true,
              ],
            ]}
          />
        )}
      </Section>
      <Section id="cl-scrub" title={m.cluster_scrub()}>
        {scrub.isError ? (
          <QueryError error={scrub.error} />
        ) : (
          <ColumnTable
            rows={(scrub.data?.rows ?? []).flatMap((n) =>
              (n.findings ?? []).map((f) => ({ node: n.node, ...f })),
            )}
            rowKey={(r, i) => `${r.node}-${i}`}
            empty={m.cluster_scrub_clean()}
            cols={[
              [m.cluster_scrub_check(), (r) => r.check ?? "—"],
              [m.cluster_range(), (r) => r.range ?? "—"],
              [m.cluster_scrub_detail(), (r) => r.detail ?? "—"],
              [
                m.cluster_scrub_heals(),
                (r) => <span className="whitespace-nowrap">{json(r.heals)}</span>,
              ],
            ]}
          />
        )}
      </Section>
      <div className="grid grid-cols-1 gap-8 xl:grid-cols-2">
        <Section id="cl-trash" title={m.cluster_trash()}>
          {trash.isError ? (
            <QueryError error={trash.error} />
          ) : (
            <ColumnTable
              rows={trash.data?.entries ?? []}
              rowKey={(r) => r.entry}
              empty={m.cluster_trash_empty()}
              cols={[
                [
                  m.cluster_trash_subject(),
                  (r) => <span className="font-mono text-xs">{r.subject ?? r.entry}</span>,
                ],
                [m.cluster_reason(), (r) => r.reason ?? "—"],
                [m.archive_bytes(), (r) => fmt.format(r.bytes ?? 0), true],
                [m.col_created(), (r) => (r.createdMs ? <Time ms={r.createdMs} /> : "—")],
              ]}
            />
          )}
          <p className="text-xs text-muted-foreground">{m.cluster_trash_hint()}</p>
        </Section>
        <Section id="cl-gc" title={m.cluster_gc()}>
          {gc.isError ? (
            <QueryError error={gc.error} />
          ) : gc.data ? (
            <p className="text-sm">
              {m.cluster_gc_plan({
                eligible: String(gc.data.eligible.length),
                blocked: String(gc.data.blocked.length),
                bytes: fmt.format(gc.data.bytes ?? 0),
                barriers: String(barriers.data?.barriers.length ?? 0),
              })}
            </p>
          ) : null}
          <p className="text-xs text-muted-foreground">{m.cluster_gc_hint()}</p>
        </Section>
        <Section id="cl-quarantine" title={m.cluster_quarantine()}>
          {quarantine.isError ? (
            <QueryError error={quarantine.error} />
          ) : (
            <ColumnTable
              rows={quarantine.data?.entries ?? []}
              rowKey={(r, i) => `${r.service}/${r.handler}-${i}`}
              empty={m.cluster_quarantine_empty()}
              cols={[
                [m.col_target(), (r) => `${r.service}/${r.handler}`],
                [m.cluster_quarantine_action(), (r) => json(r.action)],
                [m.cluster_reason(), (r) => r.reason ?? "—"],
                [m.cluster_since(), (r) => (r.sinceMs ? <Time ms={r.sinceMs} /> : "—")],
              ]}
            />
          )}
        </Section>
      </div>

      {op ? (
        <ConfirmDialog
          open
          onOpenChange={(o) => {
            if (!o) {
              setOp(null);
              setSplitAt("");
              setSplitTried(false);
              run.reset();
            }
          }}
          title={op.kind === "split" ? m.cluster_split() : m.cluster_merge()}
          description={
            op.kind === "split"
              ? m.cluster_split_body({ range: String(op.range) })
              : m.cluster_merge_body({ left: String(op.left), right: String(op.right) })
          }
          confirmLabel={op.kind === "split" ? m.cluster_split() : m.cluster_merge()}
          destructive
          typeToConfirm={op.kind === "split" ? `split ${op.range}` : `merge ${op.left} ${op.right}`}
          busy={run.isPending}
          error={splitBad ? splitBad : run.isError ? actionError(run.error) : null}
          onConfirm={() => {
            if (
              op.kind === "split" &&
              splitRange &&
              splitKeyProblem(splitAt, splitRange.start, splitRange.end)
            ) {
              setSplitTried(true);
              return;
            }
            run.mutate(
              op.kind === "split" && splitAt.trim()
                ? { ...op, at: splitAt.trim().toLowerCase() }
                : op,
            );
          }}
        >
          {op.kind === "split" && splitRange ? (
            <SplitAt
              range={splitRange}
              value={splitAt}
              invalid={splitBad !== null}
              onChange={(v) => {
                setSplitAt(v);
                setSplitTried(false);
              }}
            />
          ) : null}
        </ConfirmDialog>
      ) : null}
    </div>
  );
}

/** Split-point box of the split dialog: 16 hex, empty ⇒ middle of the key range. */
function SplitAt({
  range,
  value,
  invalid,
  onChange,
}: {
  range: Range;
  value: string;
  invalid: boolean;
  onChange: (v: string) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <Label htmlFor="split-at">{m.cluster_split_at()}</Label>
      <Input
        id="split-at"
        className="font-mono"
        autoComplete="off"
        spellCheck={false}
        maxLength={16}
        placeholder={midKey(range.start, range.end)}
        aria-describedby="split-at-hint"
        aria-invalid={invalid ? true : undefined}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <p id="split-at-hint" className="text-xs text-muted-foreground">
        {m.cluster_split_at_hint({
          start: range.start,
          end: range.end,
          mid: midKey(range.start, range.end),
        })}
      </p>
    </div>
  );
}

/**
 * "#0 → #4, #5": parent ranges (`ranges`) ⇒ child ranges (`report.children`, when present).
 * Previously it printed only the parent number ("0"), which read as "no range".
 */
function opRanges(op: { ranges: number[]; report?: unknown }): string {
  const parents = op.ranges.map((n) => `#${n}`).join(", ");
  const kids = (op.report as { children?: { range?: unknown }[] } | undefined)?.children
    ?.map((c) => c.range)
    .filter((n): n is number => typeof n === "number");
  return kids && kids.length > 0 ? `${parents} → ${kids.map((n) => `#${n}`).join(", ")}` : parents;
}

/** Node coordinating an operation: the id is prefixed with the node that received it (`<node>-<n>`). */
export function opNode(id: string): string {
  const i = id.lastIndexOf("-");
  return i > 0 ? id.slice(0, i) : id;
}
