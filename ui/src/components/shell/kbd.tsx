// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import { display } from "@/lib/shortcuts";

/** Displayed shortcut: `g o` ⇒ G O, `mod+k` ⇒ ⌘K / Ctrl+K. */
export function Keys({ keys }: { keys: string }) {
  return (
    <span className="inline-flex gap-1">
      {display(keys).map((k) => (
        <kbd
          key={k}
          className="rounded-sm border border-border bg-muted px-1 font-mono text-2xs text-muted-foreground"
        >
          {k}
        </kbd>
      ))}
    </span>
  );
}
