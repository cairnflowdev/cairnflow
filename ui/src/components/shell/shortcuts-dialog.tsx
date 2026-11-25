// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The `?` sheet: reads exactly the central shortcut registry.
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import type { Shortcut } from "@/lib/shortcuts";
import { m } from "@/paraglide/messages";
import { Keys } from "./kbd";

export function ShortcutsDialog({
  open,
  onOpenChange,
  shortcuts,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  shortcuts: Shortcut[];
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{m.shortcuts_title()}</DialogTitle>
        </DialogHeader>
        <dl className="grid grid-cols-[1fr_auto] gap-x-6 gap-y-2">
          {shortcuts.map((s) => (
            <div key={s.keys} className="contents">
              <dt>{s.label()}</dt>
              <dd>
                <Keys keys={s.keys} />
              </dd>
            </div>
          ))}
        </dl>
      </DialogContent>
    </Dialog>
  );
}
