// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Cmd+K: navigate to every item, paste an id to jump straight to it, an id prefix ⇒
// the list filtered by prefix, search services/handlers, actions of the open page.
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { Box, Code, Zap } from "lucide-react";
import { useState } from "react";
import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command";
import { ok, rest } from "@/data/api";
import { parseJump } from "@/lib/jump";
import { usePageActions } from "@/lib/palette";
import { m } from "@/paraglide/messages";
import { Keys } from "./kbd";
import type { NavItem } from "./nav";

export function CommandPalette({
  open,
  onOpenChange,
  items,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  items: NavItem[];
}) {
  const navigate = useNavigate();
  const [text, setText] = useState("");
  const jump = parseJump(text);
  const actions = usePageActions();
  const services = useQuery({
    queryKey: ["services"],
    queryFn: () => ok(rest.GET("/services")),
    enabled: open,
  });
  // Services and handlers only show once something is typed — an empty palette is the navigation list.
  const typed = text.trim() !== "";
  const go = (fn: () => void) => {
    onOpenChange(false);
    setText("");
    fn();
  };
  return (
    <CommandDialog
      open={open}
      onOpenChange={onOpenChange}
      title={m.palette_title()}
      description={m.palette_description()}
    >
      <Command>
        <CommandInput placeholder={m.shell_search()} value={text} onValueChange={setText} />
        <CommandList>
          <CommandEmpty>{m.palette_empty()}</CommandEmpty>
          {jump ? (
            <CommandGroup forceMount>
              <CommandItem
                forceMount
                value={`jump ${text}`}
                onSelect={() =>
                  go(() =>
                    jump.kind === "invocation"
                      ? navigate({ to: "/invocations/$id", params: { id: jump.id } })
                      : jump.kind === "prefix"
                        ? navigate({ to: "/invocations", search: { id: jump.id, root: 0 } })
                        : jump.kind === "deployment"
                          ? navigate({ to: "/deployments/$id", params: { id: jump.id } })
                          : navigate({ to: "/awakeables/$id", params: { id: jump.id } }),
                  )
                }
              >
                {jump.kind === "invocation"
                  ? m.palette_open_invocation({ id: jump.id })
                  : jump.kind === "prefix"
                    ? m.palette_prefix({ id: jump.id })
                    : jump.kind === "deployment"
                      ? m.palette_open_deployment({ id: jump.id })
                      : m.palette_open_awakeable({ id: jump.id })}
              </CommandItem>
            </CommandGroup>
          ) : null}
          {actions.length > 0 ? (
            <CommandGroup heading={m.palette_actions()}>
              {actions.map((a) => (
                <CommandItem key={a.id} value={a.label} onSelect={() => go(a.run)}>
                  <Zap aria-hidden />
                  {a.label}
                </CommandItem>
              ))}
            </CommandGroup>
          ) : null}
          <CommandGroup heading={m.palette_go()}>
            {items.map((it) => (
              <CommandItem
                key={it.to}
                value={it.label()}
                onSelect={() => go(() => navigate({ to: it.to }))}
              >
                <it.icon aria-hidden />
                {it.label()}
                <CommandShortcut>
                  <Keys keys={it.keys} />
                </CommandShortcut>
              </CommandItem>
            ))}
          </CommandGroup>
          {typed && services.data ? (
            <CommandGroup heading={m.palette_handlers()}>
              {services.data.flatMap((svc) => [
                <CommandItem
                  key={svc.name}
                  value={svc.name}
                  onSelect={() =>
                    go(() => navigate({ to: "/invocations", search: { service: svc.name } }))
                  }
                >
                  <Box aria-hidden />
                  {svc.name}
                </CommandItem>,
                ...svc.handlers.map((h) => (
                  <CommandItem
                    key={`${svc.name}/${h.name}`}
                    value={`${svc.name}/${h.name}`}
                    onSelect={() =>
                      go(() =>
                        navigate({
                          to: "/invocations",
                          search: { service: svc.name, handler: h.name },
                        }),
                      )
                    }
                  >
                    <Code aria-hidden />
                    {svc.name}/{h.name}
                  </CommandItem>
                )),
              ])}
            </CommandGroup>
          ) : null}
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
