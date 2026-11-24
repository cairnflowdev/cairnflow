// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Toast: Base UI Toast — not `sonner` (it injects <style>, blocked by the CSP).
// Resume/signal actions report the server-confirmed result here.
import { Toast } from "@base-ui/react/toast";
import { X } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

export const useToast = Toast.useToastManager;

export function ToastProvider({ children }: { children: ReactNode }) {
  return (
    <Toast.Provider>
      {children}
      <Toast.Portal>
        <Toast.Viewport className="fixed right-4 bottom-4 z-50 flex w-80 flex-col gap-2">
          <Toasts />
        </Toast.Viewport>
      </Toast.Portal>
    </Toast.Provider>
  );
}

function Toasts() {
  const { toasts } = Toast.useToastManager();
  return toasts.map((t) => (
    <Toast.Root
      key={t.id}
      toast={t}
      className={cn(
        "flex items-start gap-2 rounded-md border border-border bg-popover p-3 text-popover-foreground shadow-md",
        t.type === "error" && "border-destructive/50",
      )}
    >
      <Toast.Content className="min-w-0 flex-1">
        <Toast.Title className="font-medium" />
        <Toast.Description className="text-muted-foreground break-words" />
      </Toast.Content>
      <Toast.Close aria-label={m.toast_close()} className="rounded-sm p-0.5 hover:bg-accent">
        <X aria-hidden className="size-4" />
      </Toast.Close>
    </Toast.Root>
  ));
}
