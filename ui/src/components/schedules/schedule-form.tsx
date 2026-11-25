// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Create / edit a schedule: `PUT /schedules/{name}` (full overwrite — the same name
// means edit). Service/handler picked from `/services`; key only when the service is
// keyed. Cron with a plain-language description and a few presets, or a period in
// ms; IANA time zone (suggestions, a "use this machine's time zone" button); JSON
// input. The server checks the syntax — a `400` shows its message verbatim in the dialog.
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { actionError } from "@/components/cairn/confirm";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/components/ui/toast";
import { ok, rest } from "@/data/api";
import type { components } from "@/data/api.gen";
import { describeCron, weekdayName } from "@/lib/cron";
import { m } from "@/paraglide/messages";
import { getLocale } from "@/paraglide/runtime";

type Schedule = components["schemas"]["Schedule"];
type Spec = components["schemas"]["ScheduleSpec"];

/** Common expressions; the label is their own `describeCron`. */
const CRON_PRESETS = ["0 * * * *", "0 0 * * *", "0 9 * * 1-5", "0 9 * * 1"];

/** IANA time zones for suggestions (old browsers lack `supportedValuesOf`). */
const TIME_ZONES: string[] =
  typeof Intl.supportedValuesOf === "function" ? Intl.supportedValuesOf("timeZone") : ["UTC"];

export function ScheduleForm({ initial, onClose }: { initial?: Schedule; onClose: () => void }) {
  const qc = useQueryClient();
  const toast = useToast();
  const services = useQuery({
    queryKey: ["services"],
    queryFn: () => ok(rest.GET("/services")),
    retry: false,
  });
  const [name, setName] = useState(initial?.name ?? "");
  const [service, setService] = useState(initial?.target.service ?? "");
  const [handler, setHandler] = useState(initial?.target.handler ?? "");
  const [key, setKey] = useState(initial?.target.key ?? "");
  const [mode, setMode] = useState<"cron" | "every">(initial?.everyMs ? "every" : "cron");
  const [cron, setCron] = useState(initial?.cron ?? "0 * * * *");
  const [everyS, setEveryS] = useState(initial?.everyMs ? String(initial.everyMs / 1000) : "60");
  const [tz, setTz] = useState(initial?.timeZone ?? "UTC");
  const [input, setInput] = useState(
    initial?.input === undefined ? "" : JSON.stringify(initial.input, null, 2),
  );
  const [inputError, setInputError] = useState<string | null>(null);
  const list = services.data ?? [];
  const svc = list.find((s) => s.name === service);
  const handlers = svc?.handlers.map((h) => h.name) ?? [];
  // A key only means something for a virtual object / workflow (required there); for an
  // unkeyed service it is hidden — unless an old schedule already has a key (still shown so it can be edited).
  const keyed = svc ? svc.ty !== "service" : key !== "";
  const dayName = weekdayName(getLocale());
  const described = mode === "cron" ? describeCron(cron, dayName) : null;
  const localTz = Intl.DateTimeFormat().resolvedOptions().timeZone;

  const save = useMutation({
    mutationFn: (spec: Spec) =>
      ok(rest.PUT("/schedules/{name}", { params: { path: { name } }, body: spec })),
    onSuccess: async () => {
      toast.add({ title: m.schedule_saved({ name }) });
      await qc.invalidateQueries({ queryKey: ["schedules"] });
      onClose();
    },
  });

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            {initial ? m.schedule_edit({ name: initial.name }) : m.schedule_new()}
          </DialogTitle>
        </DialogHeader>
        <form
          // Labels align with the first line of the field (the cron field is taller than one line): label as tall as the input.
          className="grid grid-cols-[9.5rem_minmax(0,1fr)] items-start gap-x-3 gap-y-3 [&>label]:min-h-8"
          onSubmit={(e) => {
            e.preventDefault();
            let parsed: unknown;
            if (input.trim() !== "") {
              try {
                parsed = JSON.parse(input);
              } catch (err) {
                setInputError(m.schedule_input_invalid({ message: (err as Error).message }));
                return;
              }
            }
            setInputError(null);
            const spec: Spec = {
              target: { service, handler, ...(key ? { key } : {}) },
              ...(mode === "cron"
                ? { cron, timeZone: tz }
                : { everyMs: Math.round(Number(everyS) * 1000) }),
              ...(parsed === undefined ? {} : { input: parsed }),
            };
            save.mutate(spec);
          }}
        >
          <Label htmlFor="sch-name">{m.schedule_name()}</Label>
          <Input
            id="sch-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={!!initial}
            required
            spellCheck={false}
          />
          <Label htmlFor="sch-service">{m.col_service()}</Label>
          <NativeSelect
            id="sch-service"
            value={service}
            onChange={(e) => {
              setService(e.target.value);
              // Changing the service ⇒ its first handler (the old handler does not belong to the new service).
              setHandler(list.find((x) => x.name === e.target.value)?.handlers[0]?.name ?? "");
            }}
            required
          >
            <option value="" disabled>
              {m.schedule_choose_service()}
            </option>
            {/* An old schedule pointing at a removed service: kept so it can be seen and changed. */}
            {service && !svc ? <option value={service}>{service}</option> : null}
            {list.map((x) => (
              <option key={x.name} value={x.name}>
                {x.name}
              </option>
            ))}
          </NativeSelect>
          <Label htmlFor="sch-handler">{m.filter_handler()}</Label>
          <NativeSelect
            id="sch-handler"
            value={handler}
            onChange={(e) => setHandler(e.target.value)}
            disabled={!service}
            required
          >
            {handler && !handlers.includes(handler) ? (
              <option value={handler}>{handler}</option>
            ) : null}
            {handlers.map((h) => (
              <option key={h} value={h}>
                {h}
              </option>
            ))}
          </NativeSelect>
          {keyed ? (
            <>
              <Label htmlFor="sch-key">{m.schedule_key()}</Label>
              <Input
                id="sch-key"
                className="font-mono"
                value={key}
                onChange={(e) => setKey(e.target.value)}
                required={svc !== undefined}
                spellCheck={false}
              />
            </>
          ) : null}
          <span className="flex min-h-8 items-center text-sm">{m.schedule_when()}</span>
          <div className="flex min-h-8 gap-4" role="radiogroup" aria-label={m.schedule_when()}>
            <label className="flex items-center gap-1.5">
              <input
                type="radio"
                className="accent-primary"
                name="sch-mode"
                checked={mode === "cron"}
                onChange={() => setMode("cron")}
              />
              {m.schedule_cron()}
            </label>
            <label className="flex items-center gap-1.5">
              <input
                type="radio"
                className="accent-primary"
                name="sch-mode"
                checked={mode === "every"}
                onChange={() => setMode("every")}
              />
              {m.schedule_every()}
            </label>
          </div>
          {mode === "cron" ? (
            <>
              <Label htmlFor="sch-cron">{m.schedule_cron()}</Label>
              <div className="flex flex-col gap-1">
                <Input
                  id="sch-cron"
                  className="font-mono"
                  value={cron}
                  onChange={(e) => setCron(e.target.value)}
                  aria-describedby="sch-cron-said"
                  required
                />
                <span id="sch-cron-said" className="text-xs text-muted-foreground">
                  {described ?? m.schedule_cron_raw()}
                </span>
                {/* Common presets — the label is the expression's plain-language description. */}
                <div className="flex flex-wrap gap-1">
                  {CRON_PRESETS.map((c) => (
                    <button
                      key={c}
                      type="button"
                      onClick={() => setCron(c)}
                      aria-pressed={cron.trim() === c}
                      className="rounded-md border border-input px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-accent hover:text-foreground aria-pressed:bg-secondary aria-pressed:text-secondary-foreground"
                    >
                      {describeCron(c, dayName)}
                    </button>
                  ))}
                </div>
              </div>
              <Label htmlFor="sch-tz">{m.schedule_tz()}</Label>
              <div className="flex flex-col items-start gap-1">
                <Input
                  id="sch-tz"
                  list="sch-tzs"
                  value={tz}
                  onChange={(e) => setTz(e.target.value)}
                  spellCheck={false}
                />
                <datalist id="sch-tzs">
                  {TIME_ZONES.map((z) => (
                    <option key={z} value={z} />
                  ))}
                </datalist>
                {localTz && localTz !== tz ? (
                  <button
                    type="button"
                    onClick={() => setTz(localTz)}
                    className="text-xs text-primary underline-offset-4 hover:underline"
                  >
                    {m.schedule_tz_local({ tz: localTz })}
                  </button>
                ) : null}
              </div>
            </>
          ) : (
            <>
              <Label htmlFor="sch-every">{m.schedule_every_s()}</Label>
              <Input
                id="sch-every"
                type="number"
                min={1}
                value={everyS}
                onChange={(e) => setEveryS(e.target.value)}
                required
              />
            </>
          )}
          <Label htmlFor="sch-input">{m.schedule_input()}</Label>
          <Textarea
            id="sch-input"
            className="font-mono text-xs"
            rows={3}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            aria-invalid={inputError ? true : undefined}
            spellCheck={false}
          />
          {inputError || save.isError ? (
            <p role="alert" className="col-span-2 text-destructive">
              {inputError ?? actionError(save.error)}
            </p>
          ) : null}
          <DialogFooter className="col-span-2">
            <Button type="button" variant="outline" onClick={onClose}>
              {m.confirm_cancel()}
            </Button>
            <Button type="submit" disabled={save.isPending || name.trim() === ""}>
              {m.schedule_save()}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
