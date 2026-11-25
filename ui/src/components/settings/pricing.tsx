// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Token pricing: read-only — set with `cairn-ctl pricing set`. Prices are stored in
// micro-USD per 1 000 000 tokens; shown as USD per 1 M tokens.
import { useQuery } from "@tanstack/react-query";
import { EmptyState } from "@/components/cairn/empty-state";
import { QueryError } from "@/components/cairn/query-state";
import { Table, TBody, TD, TH, THead, TRow } from "@/components/cairn/table";
import { ok, rest } from "@/data/api";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

export function Pricing() {
  const q = useQuery({ queryKey: ["pricing"], queryFn: () => ok(rest.GET("/metering/pricing")) });
  const usd = new Intl.NumberFormat(getLocale(), {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 4,
  });
  return (
    <section aria-labelledby="set-pricing" className="flex flex-col gap-2">
      <h2 id="set-pricing" className="text-base font-semibold">
        {m.pricing_title()}
      </h2>
      <p className="text-sm text-muted-foreground">{m.pricing_body()}</p>
      {q.isError ? (
        <QueryError error={q.error} />
      ) : q.data ? (
        q.data.models.length === 0 ? (
          <EmptyState variant="inline" title={m.pricing_empty()} />
        ) : (
          <>
            <Table>
              <THead>
                <TH>{m.pricing_model()}</TH>
                <TH numeric>{m.pricing_input()}</TH>
                <TH numeric>{m.pricing_output()}</TH>
              </THead>
              <TBody>
                {q.data.models.map((p) => (
                  <TRow key={p.model}>
                    <TD priority="primary" className="font-mono">
                      {p.model}
                    </TD>
                    <TD numeric>{usd.format(p.input / 1_000_000)}</TD>
                    <TD numeric>{usd.format(p.output / 1_000_000)}</TD>
                  </TRow>
                ))}
              </TBody>
            </Table>
            <p className="text-xs text-muted-foreground">
              {m.pricing_version({ version: String(q.data.version), by: q.data.by })}
            </p>
          </>
        )
      ) : null}
    </section>
  );
}
