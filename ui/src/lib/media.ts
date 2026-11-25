// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Matches a media query, tracking resizes (peek is a side panel from `lg` and a
// covering sheet below it — different ARIA roles, so CSS alone cannot do it).
import { useEffect, useState } from "react";

export function useMedia(query: string): boolean {
  const [hit, setHit] = useState(() => window.matchMedia(query).matches);
  useEffect(() => {
    const mq = window.matchMedia(query);
    const on = () => setHit(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return hit;
}

/** From Tailwind's `lg` (1024 px). */
export const WIDE = "(min-width: 1024px)";
