// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The five states of a data region: loading (skeleton), error, not permitted (403 —
// not a red error), partial (banner), empty (drawn by the screen itself when `data` is
// empty). Plus an "updated at" timestamp.
import type { UseQueryResult } from "@tanstack/react-query";
import { CircleAlert, Lock } from "lucide-react";
import type { ReactNode } from "react";
import { ApiError } from "@/data/api";
import type { Rows } from "@/data/run";
import { m } from "@/paraglide/messages";
import { leaderError } from "./confirm";
import { EmptyState } from "./empty-state";
import { PartialBanner } from "./partial-banner";
import { Skeleton } from "./skeleton";
import { Time } from "./time";

export function QueryError({ error }: { error: unknown }) {
  if (error instanceof ApiError && error.status === 403) {
    return (
      <EmptyState icon={<Lock />} title={m.forbidden_title()}>
        {m.forbidden_body()}
      </EmptyState>
    );
  }
  return (
    <div role="alert" className="flex gap-2 rounded-md border border-destructive/40 px-3 py-2">
      <CircleAlert aria-hidden className="mt-0.5 size-4 shrink-0 text-destructive" />
      <div>
        <p className="font-medium">{m.query_error_title()}</p>
        <p className="text-muted-foreground">
          {leaderError(error) ?? (error instanceof Error ? error.message : String(error))}
        </p>
      </div>
    </div>
  );
}

export function Updated({ at }: { at: number }) {
  return (
    <span className="text-xs text-tertiary-foreground">
      {m.updated_at()} <Time ms={at} />
    </span>
  );
}

/** Renders `children(data)` when there is data; otherwise a skeleton / error. */
export function QueryState<T>({
  q,
  skeleton = <Skeleton className="h-24 w-full" />,
  children,
}: {
  q: UseQueryResult<Rows<T>>;
  skeleton?: ReactNode;
  children: (data: Rows<T>) => ReactNode;
}) {
  if (q.isPending) return <div aria-busy="true">{skeleton}</div>;
  if (q.isError) return <QueryError error={q.error} />;
  return (
    <>
      <PartialBanner warnings={q.data.warnings} />
      {children(q.data)}
    </>
  );
}
