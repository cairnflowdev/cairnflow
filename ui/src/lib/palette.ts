// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Actions of the open page in Cmd+K: the invocation page declares "Resume", "Cancel
// (cooperative)…", "Copy id" — the palette reads them here without knowing which page
// is open.
import { useEffect, useSyncExternalStore } from "react";

export type PaletteAction = { id: string; label: string; run: () => void };

const groups = new Set<PaletteAction[]>();
const listeners = new Set<() => void>();
let snapshot: PaletteAction[] = [];

function changed() {
  snapshot = [...groups].flat();
  for (const l of listeners) l();
}

/** Declares a page's actions while it is open, removes them on leave. */
export function usePaletteActions(list: PaletteAction[]) {
  useEffect(() => {
    groups.add(list);
    changed();
    return () => {
      groups.delete(list);
      changed();
    };
  }, [list]);
}

export function usePageActions(): PaletteAction[] {
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => snapshot,
  );
}
