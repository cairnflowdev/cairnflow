// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
// The onboarding copy-command button: a failed copy must be reported, like every other
// copy button (`CopyButton`) — the `false` result of `copyText` used to be ignored.
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Suspense } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { whoamiQuery } from "@/data/session";
import { Onboarding } from "./onboarding";

afterEach(cleanup);

function renderOnboarding() {
  const qc = new QueryClient();
  qc.setQueryData(whoamiQuery.queryKey, { capabilities: ["read", "invoke", "operate"] } as never);
  return render(
    <QueryClientProvider client={qc}>
      <Suspense>
        <Onboarding />
      </Suspense>
    </QueryClientProvider>,
  );
}

describe("Onboarding", () => {
  it("a failed command copy ⇒ says so, not silent", async () => {
    Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
    Object.defineProperty(document, "execCommand", {
      value: vi.fn().mockReturnValue(false),
      configurable: true,
    });
    renderOnboarding();
    fireEvent.click(screen.getByRole("button", { name: "Copy command" }));
    expect(await screen.findByRole("button", { name: /Copy failed/ })).toBeTruthy();
  });
});
