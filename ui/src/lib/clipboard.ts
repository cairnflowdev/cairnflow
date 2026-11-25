// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Copy to the clipboard. `navigator.clipboard` only exists in a secure context (HTTPS
// or localhost) — a console opened at `http://node:8080` does not have it, so it falls
// back to `execCommand("copy")` on a hidden field. Returns `false` when both paths
// fail, so the copy button says so instead of staying silent.
export async function copyText(text: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Refused (permission, tab not focused) — try the old path.
    }
  }
  const area = document.createElement("textarea");
  area.value = text;
  area.setAttribute("readonly", "");
  area.style.position = "fixed";
  area.style.opacity = "0";
  document.body.append(area);
  const back = document.activeElement as HTMLElement | null;
  area.select();
  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch {
    ok = false;
  }
  area.remove();
  back?.focus?.();
  return ok;
}
