// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Copy button: every copy button of the console (and of the hosted web console that
// shares these components) goes through here. `copyText` has a fallback when the browser
// refuses the clipboard API (console opened at `http://node:8080`); feedback is an icon
// + an `aria-live` region, and a failed copy is reported instead of staying silent.
import { Check, Copy, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { copyText } from "@/lib/clipboard";
import { cn } from "@/lib/utils";
import { m } from "@/paraglide/messages";

const ICON = { xs: "icon-xs", sm: "icon-sm", default: "icon" } as const;

export function CopyButton({
  value,
  label,
  text,
  showText = false,
  variant = "ghost",
  size = "sm",
  className,
}: {
  value: string;
  /** What is copied, for the `aria-label` of an icon-only button: "Copy endpoint". */
  label: string;
  /** Button text when `showText` and not yet clicked (default "Copy"). */
  text?: string;
  showText?: boolean;
  variant?: "ghost" | "outline" | "secondary";
  /** `xs`: a button inside a line of text (next to an id in a table); `default`: next to regular buttons (dialog footer). */
  size?: "xs" | "sm" | "default";
  className?: string;
}) {
  const [state, setState] = useState<"copied" | "failed" | null>(null);
  useEffect(() => {
    if (!state) return;
    const t = setTimeout(() => setState(null), state === "failed" ? 4000 : 1500);
    return () => clearTimeout(t);
  }, [state]);
  const said =
    state === "copied" ? m.copy_done() : state === "failed" ? m.copy_failed() : (text ?? m.copy());
  return (
    <>
      <Button
        type="button"
        variant={variant}
        size={showText ? size : ICON[size]}
        className={cn(!showText && "text-muted-foreground", className)}
        aria-label={showText ? undefined : state ? said : m.copy_what({ what: label })}
        title={state === "failed" ? said : undefined}
        onClick={() => {
          void copyText(value).then((ok) => setState(ok ? "copied" : "failed"));
        }}
      >
        {state === "copied" ? (
          <Check aria-hidden />
        ) : state === "failed" ? (
          <X aria-hidden className="text-destructive" />
        ) : (
          <Copy aria-hidden />
        )}
        {showText ? <span>{said}</span> : null}
      </Button>
      <span aria-live="polite" className="sr-only">
        {state === "copied" ? m.copy_done() : state === "failed" ? m.copy_failed() : ""}
      </span>
    </>
  );
}
