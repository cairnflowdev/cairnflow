// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Empty state: every screen has one — with what can be done next. `inline`: an empty
// message inside a table or panel of a screen — one text size, one padding, no title
// (it does not intrude into the page's heading outline).
import type { ReactNode } from "react";
import { m } from "@/paraglide/messages";

export function EmptyState({
  icon,
  title,
  children,
  action,
  variant = "page",
}: {
  icon?: ReactNode;
  title?: string;
  children?: ReactNode;
  action?: ReactNode;
  variant?: "page" | "inline";
}) {
  if (variant === "inline") {
    return (
      <div className="flex flex-col items-start gap-1 py-2 text-sm text-muted-foreground">
        <p className="flex items-center gap-2">
          {icon ? (
            <span className="text-tertiary-foreground [&_svg]:size-4" aria-hidden>
              {icon}
            </span>
          ) : null}
          {title ?? m.empty_default()}
        </p>
        {children ? <div className="max-w-prose">{children}</div> : null}
        {action ? <div className="mt-1">{action}</div> : null}
      </div>
    );
  }
  return (
    <section className="flex flex-col items-center gap-2 px-6 py-12 text-center">
      {icon ? <div className="text-tertiary-foreground [&_svg]:size-6">{icon}</div> : null}
      <h2 className="text-base font-semibold">{title ?? m.empty_default()}</h2>
      {children ? <div className="max-w-prose text-muted-foreground">{children}</div> : null}
      {action ? <div className="mt-2">{action}</div> : null}
    </section>
  );
}
