// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Add/edit a rule or a tenant rate limit: `POST /rules` (merge — an empty field is
// left untouched, said plainly in the dialog). The server's errors (400 bad pattern…)
// show verbatim.
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Checkbox } from "@/components/cairn/checkbox";
import { actionError } from "@/components/cairn/confirm";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { type FormErrors, type RuleForm, ruleBody } from "@/lib/rules";
import { m } from "@/paraglide/messages";

type RuleView = components["schemas"]["RuleView"];
type TenantRuleView = components["schemas"]["TenantRuleView"];

const ERROR: Record<NonNullable<FormErrors[keyof FormErrors]>, () => string> = {
  required: m.rule_err_required,
  number: m.rule_err_number,
  tenantConcurrency: m.rule_err_tenant_concurrency,
};

export function RuleDialog({
  forTenant,
  initial,
  onClose,
}: {
  forTenant: boolean;
  initial?: RuleView | TenantRuleView;
  onClose: () => void;
}) {
  const qc = useQueryClient();
  const toast = useToast();
  const [f, setF] = useState<RuleForm>({
    target: initial ? ("pattern" in initial ? initial.pattern : initial.tenant) : "",
    forTenant,
    concurrency: initial && "concurrency" in initial ? (initial.concurrency ?? "") : "",
    rate: initial?.rate ?? "",
    burst: initial?.burst != null ? String(initial.burst) : "",
    description: initial?.description ?? "",
    disabled: initial?.disabled ?? false,
    wasSet: {
      concurrency: !!initial && "concurrency" in initial && initial.concurrency != null,
      rate: initial?.rate != null,
      burst: initial?.burst != null,
    },
  });
  const [errors, setErrors] = useState<FormErrors>({});
  const save = useMutation({
    mutationFn: (body: components["schemas"]["PutRuleRequest"]) =>
      ok(rest.POST("/rules", { body })),
    onSuccess: async (book) => {
      toast.add({ title: m.rule_saved({ target: f.target, version: String(book.version) }) });
      await qc.invalidateQueries({ queryKey: ["rules"] });
      onClose();
    },
  });
  const set =
    (k: Exclude<keyof RuleForm, "disabled">) => (e: React.ChangeEvent<HTMLInputElement>) =>
      setF((x) => ({ ...x, [k]: e.target.value }));
  const field = (k: "concurrency" | "rate" | "burst", label: string, hint: string) => (
    <>
      <Label htmlFor={`rule-${k}`}>{label}</Label>
      <div className="flex flex-col gap-0.5">
        <Input
          id={`rule-${k}`}
          value={f[k]}
          onChange={set(k)}
          aria-invalid={errors[k] ? true : undefined}
          aria-describedby={`rule-${k}-hint`}
          placeholder={f.wasSet?.[k] ? m.rule_clear_placeholder() : m.rule_unchanged()}
        />
        <span
          id={`rule-${k}-hint`}
          className={errors[k] ? "text-xs text-destructive" : "text-xs text-muted-foreground"}
        >
          {errors[k] ? ERROR[errors[k]]() : hint}
        </span>
      </div>
    </>
  );
  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>
            {initial
              ? m.rule_edit({ target: f.target })
              : forTenant
                ? m.rule_new_tenant()
                : m.rule_new()}
          </DialogTitle>
          <DialogDescription>{m.rule_merge_hint()}</DialogDescription>
          <details className="text-sm text-muted-foreground">
            <summary className="min-h-6 cursor-pointer">{m.hint_more()}</summary>
            <p>{m.rule_merge_detail()}</p>
          </details>
        </DialogHeader>
        <form
          className="grid grid-cols-[8rem_1fr] items-start gap-x-3 gap-y-2"
          onSubmit={(e) => {
            e.preventDefault();
            const out = ruleBody(f);
            if ("errors" in out) {
              setErrors(out.errors);
              return;
            }
            setErrors({});
            save.mutate(out.body);
          }}
        >
          <Label htmlFor="rule-target">{forTenant ? m.rule_tenant() : m.rule_pattern()}</Label>
          <div className="flex flex-col gap-0.5">
            <Input
              id="rule-target"
              className="font-mono"
              value={f.target}
              onChange={set("target")}
              disabled={!!initial}
              aria-invalid={errors.target ? true : undefined}
              spellCheck={false}
            />
            {!forTenant ? (
              <span className="text-xs text-muted-foreground">{m.rule_pattern_hint()}</span>
            ) : null}
            {errors.target ? (
              <span className="text-xs text-destructive">{ERROR[errors.target]()}</span>
            ) : null}
          </div>
          {!forTenant ? field("concurrency", m.rule_concurrency(), m.rule_cap_hint()) : null}
          {field("rate", m.rule_rate(), m.rule_rate_hint())}
          {field("burst", m.rule_burst(), m.rule_burst_hint())}
          <Label htmlFor="rule-desc">{m.rule_description()}</Label>
          <Input id="rule-desc" value={f.description} onChange={set("description")} />
          <span />
          <Checkbox
            checked={f.disabled}
            onCheckedChange={(disabled) => setF((x) => ({ ...x, disabled }))}
            className="gap-2"
          >
            {m.rule_disabled()}
          </Checkbox>
          {!forTenant && f.target.includes("*") ? (
            <p
              role="note"
              className="col-span-2 rounded-md border border-input bg-muted px-3 py-2 text-sm"
            >
              {m.rule_star_warning()}
            </p>
          ) : null}
          {save.isError ? (
            <p role="alert" className="col-span-2 text-destructive">
              {actionError(save.error)}
            </p>
          ) : null}
          <DialogFooter className="col-span-2">
            <Button type="button" variant="outline" onClick={onClose}>
              {m.confirm_cancel()}
            </Button>
            <Button type="submit" disabled={save.isPending}>
              {m.schedule_save()}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
