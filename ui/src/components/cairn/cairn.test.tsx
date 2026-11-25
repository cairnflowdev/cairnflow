// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { shownStatus } from "@/components/invocations/peek";
import { ApiError } from "@/data/api";
import { CAPABILITIES, type Capability, can, canAny } from "@/lib/roles";
import { Checkbox } from "./checkbox";
import { actionError, leaderError } from "./confirm";
import { CopyButton } from "./copy-button";
import { EmptyState } from "./empty-state";
import { confirmText, Id, shorten } from "./id";
import { RequireCapability, RoleGate } from "./role-gate";
import { isStatus, StatusBadge } from "./status-badge";
import { Table, TBody, TD, TH, THead, TRow } from "./table";
import { TabPanel, Tabs } from "./tabs";
import { Tag } from "./tag";
import { absolute, clock, relative, setTimeZonePref } from "./time";

afterEach(cleanup);

describe("Id", () => {
  it("truncates in the middle, keeps the full id for title and screen readers", () => {
    const id = "3fa9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21";
    expect(shorten(id)).toBe("3fa9…d3ec21");
    expect(shorten("dp_9a")).toBe("dp_9a");
    render(<Id value={id} />);
    expect(screen.getByTitle(id)).toBeTruthy();
    expect(screen.getByText(id).className).toContain("sr-only");
  });

  it("copies the id to the clipboard", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    render(<Id value="abc123" />);
    fireEvent.click(screen.getByRole("button", { name: "Copy ID" }));
    expect(writeText).toHaveBeenCalledWith("abc123");
    expect(await screen.findByRole("button", { name: "Copied" })).toBeTruthy();
  });

  // The first 16 hex of an invocation id are the partition key — shared by every
  // invocation of one key or one call tree. The short form and the type-to-confirm
  // string must come from the distinct part (the tail), not from the shared part.
  // Two ids with the same key: share the first 40 hex, differ in hex 40–44, share the last 3 hex.
  const sameKeyA = `${"e06824607c3e057e".padEnd(40, "0")}aaaaa327`;
  const sameKeyB = `${"e06824607c3e057e".padEnd(40, "0")}bbbbb327`;

  it("ids with the same key shorten to two different strings", () => {
    expect(shorten(sameKeyA)).not.toBe(shorten(sameKeyB));
  });

  it("type-to-confirm strings differ between two invocations of the same key", () => {
    expect(confirmText(sameKeyA)).not.toBe(confirmText(sameKeyB));
    // Deployments created at the same time share the time prefix `dp_01a1881a`.
    expect(confirmText("dp_01a1881a1fd410f07cfeb125719a")).not.toBe(
      confirmText("dp_01a1881a1f12de90688f2ed9542e"),
    );
  });

  it("no navigator.clipboard (http that is not localhost) ⇒ still copies", async () => {
    Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
    const exec = vi.fn().mockReturnValue(true);
    Object.defineProperty(document, "execCommand", { value: exec, configurable: true });
    render(<Id value="abc123" />);
    fireEvent.click(screen.getByRole("button", { name: "Copy ID" }));
    expect(exec).toHaveBeenCalledWith("copy");
    expect(await screen.findByRole("button", { name: "Copied" })).toBeTruthy();
  });

  it("a failed copy ⇒ says so, not silent", async () => {
    Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
    Object.defineProperty(document, "execCommand", {
      value: vi.fn().mockReturnValue(false),
      configurable: true,
    });
    render(<Id value="abc123" />);
    fireEvent.click(screen.getByRole("button", { name: "Copy ID" }));
    expect(await screen.findByRole("button", { name: /Copy failed/ })).toBeTruthy();
  });
});

describe("StatusBadge", () => {
  it("always has an icon and a text label, not colour alone", () => {
    for (const s of [
      "running",
      "suspended",
      "paused",
      "completed",
      "failed",
      "scheduled",
      "retrying",
    ]) {
      const { container, unmount } = render(<StatusBadge status={s} />);
      const badge = container.querySelector(`[data-status="${s}"]`);
      expect(badge?.textContent?.trim().length, s).toBeGreaterThan(0);
      expect(badge?.querySelector("svg, [aria-hidden]"), s).toBeTruthy();
      unmount();
    }
  });

  it("an unknown status shows exactly the string the server returns, no guessing", () => {
    expect(isStatus("nope")).toBe(false);
    render(<StatusBadge status="nope" />);
    expect(screen.getByText("nope")).toBeTruthy();
  });
});

describe("Time", () => {
  const now = Date.UTC(2026, 9, 28, 12);
  it("relative, per locale", () => {
    expect(relative(now - 3 * 60_000, now, "en")).toBe("3 min. ago");
    expect(relative(now - 3 * 60_000, now, "vi")).toBe("3 phút trước");
    expect(relative(now + 2 * 3600_000, now, "en")).toBe("in 2 hr.");
  });
});

describe("RoleGate", () => {
  // Capability sets per role, as the server sends them in `whoami` — the UI does
  // not derive them from the role name; this table is only sample data for the test.
  const view: Capability[] = ["read"];
  const admin: Capability[] = ["read", "invoke", "operate"];

  it("capability sets, not nested", () => {
    expect(can(admin, "operate")).toBe(true);
    expect(can(admin, "cluster")).toBe(false);
    expect(can(view, "invoke")).toBe(false);
    expect(can(["invoke"], "read")).toBe(false);
    expect(can([...CAPABILITIES], "provision")).toBe(true);
    // An old server sends no set ⇒ nothing.
    expect(can(undefined, "read")).toBe(false);
    expect(canAny(view, ["read", "invoke"])).toBe(true);
    expect(canAny(["provision"], ["read", "invoke"])).toBe(false);
    expect(canAny(view, "read")).toBe(true);
  });

  it("hidden or disabled with a reason", () => {
    const { container } = render(
      <RoleGate have={admin} need="cluster" mode="hide">
        {() => <span>x</span>}
      </RoleGate>,
    );
    expect(container.textContent).toBe("");
    render(
      <RoleGate have={view} need="operate">
        {(blocked) => <span>{blocked}</span>}
      </RoleGate>,
    );
    expect(screen.getByText("Requires the operate capability")).toBeTruthy();
  });

  it("a whole screen without the capability ⇒ states the reason instead of drawing the screen", () => {
    render(
      <RequireCapability have={admin} need="cluster">
        <span>split range</span>
      </RequireCapability>,
    );
    expect(screen.queryByText("split range")).toBeNull();
    expect(screen.getByText("Requires the cluster capability")).toBeTruthy();
  });
});

describe("actionError", () => {
  it("`503` with a leader says which node leads, does not print the raw message", () => {
    const raw = "this node is not the leader of partition 3; the leader is at 10.0.0.2:8080";
    const text = actionError(new ApiError(503, raw, null, "10.0.0.2:8080"));
    expect(text).toContain("10.0.0.2:8080");
    expect(text).not.toContain("partition");
  });

  it("`503` without a leader reads 'no leader yet'; other codes keep the old behaviour", () => {
    expect(leaderError(new ApiError(503, "x"))).toBeTruthy();
    expect(leaderError(new ApiError(503, "x"))).not.toBe(
      leaderError(new ApiError(503, "x", null, "h:1")),
    );
    expect(leaderError(new ApiError(500, "boom"))).toBeNull();
    // `504` from forwarding: ambiguous, not a "failure".
    expect(actionError(new ApiError(504, "x"))).toMatch(/may have run/);
    expect(actionError(new ApiError(500, "boom"))).toBe("500: boom");
  });
});

// A call still in the queue (an `inbox` entry of `sys_vqueue_entry`) is not
// "Running" — its journal only has the Input.
describe("V3 — Queued", () => {
  it("Queued badge has an icon and a label; the displayed status uses the queue facts", () => {
    expect(isStatus("queued")).toBe(true);
    render(<StatusBadge status="queued" />);
    expect(screen.getByText("Queued")).toBeTruthy();
    const shown = shownStatus;
    expect(shown({ status: "running", class: "active" }, true)).toBe("queued");
    expect(shown({ status: "running", class: "active" }, false)).toBe("running");
  });
});

describe("log line time", () => {
  it("absolute time with milliseconds, in the given time zone", () => {
    expect(clock(Date.UTC(2026, 9, 29, 7, 5, 3, 42), "UTC")).toBe("07:05:03.042");
  });
});

// Selectable time zone (browser / UTC), remembered in localStorage.
describe("time zone", () => {
  it("UTC selected ⇒ absolute time in UTC", () => {
    setTimeZonePref("utc");
    expect(absolute(Date.UTC(2026, 9, 29, 7, 5, 3), "en")).toContain("7:05:03");
    expect(clock(Date.UTC(2026, 9, 29, 7, 5, 3, 1))).toBe("07:05:03.001");
    setTimeZonePref("local");
  });
});

describe("Tabs", () => {
  const OPTIONS = [
    { value: "a", label: "Alpha" },
    { value: "b", label: "Beta" },
    { value: "c", label: "Gamma" },
  ];

  it("ARIA pattern: the tablist has a name, only the selected tab takes Tab, the panel points back to the tab", () => {
    render(
      <>
        <Tabs id="t" label="Panes" value="b" options={OPTIONS} onChange={() => {}} />
        <TabPanel id="t" value="b">
          content
        </TabPanel>
      </>,
    );
    const tabs = screen.getAllByRole("tab");
    expect(screen.getByRole("tablist", { name: "Panes" })).toBeTruthy();
    expect(tabs.map((t) => t.getAttribute("aria-selected"))).toEqual(["false", "true", "false"]);
    expect(tabs.map((t) => t.tabIndex)).toEqual([-1, 0, -1]);
    const panel = screen.getByRole("tabpanel", { name: "Beta" });
    expect(tabs[1]?.getAttribute("aria-controls")).toBe(panel.id);
  });

  it("arrows wrap, Home/End: select and move focus", () => {
    const onChange = vi.fn();
    const { rerender } = render(
      <Tabs id="t" label="Panes" value="a" options={OPTIONS} onChange={onChange} />,
    );
    const tab = (name: string) => screen.getByRole("tab", { name });
    fireEvent.keyDown(tab("Alpha"), { key: "ArrowLeft" });
    expect(onChange).toHaveBeenLastCalledWith("c");
    expect(document.activeElement).toBe(tab("Gamma"));
    rerender(<Tabs id="t" label="Panes" value="c" options={OPTIONS} onChange={onChange} />);
    fireEvent.keyDown(tab("Gamma"), { key: "ArrowRight" });
    expect(onChange).toHaveBeenLastCalledWith("a");
    fireEvent.keyDown(tab("Gamma"), { key: "Home" });
    expect(onChange).toHaveBeenLastCalledWith("a");
    fireEvent.keyDown(tab("Gamma"), { key: "End" });
    expect(onChange).toHaveBeenLastCalledWith("c");
    onChange.mockClear();
    fireEvent.keyDown(tab("Gamma"), { key: "Enter" });
    expect(onChange).not.toHaveBeenCalled();
  });
});

describe("Table", () => {
  it("table scrolls horizontally in its frame; numeric columns right-aligned; one header style", () => {
    const { container } = render(
      <Table>
        <THead>
          <TH>Service</TH>
          <TH numeric>Turns</TH>
        </THead>
        <TBody>
          <TRow>
            <TD>Shop</TD>
            <TD numeric>12</TD>
          </TRow>
        </TBody>
      </Table>,
    );
    expect(container.firstElementChild?.className).toContain("overflow-x-auto");
    expect(screen.getByRole("columnheader", { name: "Turns" }).className).toContain("text-right");
    expect(screen.getByRole("cell", { name: "12" }).className).toContain("text-right");
    const heads = screen
      .getAllByRole("columnheader")
      .map((h) => h.className.replace("text-right", "").trim());
    expect(new Set(heads).size).toBe(1);
    expect(screen.getByRole("row", { name: /Shop/ }).className).toContain("border-t");
  });

  it("dense is shorter; stickyHeader sticks the header in a bounded frame", () => {
    const { container } = render(
      <Table dense stickyHeader>
        <THead>
          <TH>a</TH>
        </THead>
        <TBody>
          <TRow>
            <TD>1</TD>
          </TRow>
        </TBody>
      </Table>,
    );
    expect(container.firstElementChild?.className).toContain("max-h-[60vh]");
    expect(container.querySelector("thead")?.className).toContain("sticky");
    expect(screen.getByRole("cell").className).toContain("py-1");
    expect(screen.getByRole("cell").className).not.toContain("py-1.5");
  });
});

describe("EmptyState inline", () => {
  it("does not insert a heading; has an action", () => {
    render(
      <EmptyState variant="inline" title="No rows" action={<button type="button">Add</button>} />,
    );
    expect(screen.queryByRole("heading")).toBeNull();
    expect(screen.getByText("No rows")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Add" })).toBeTruthy();
  });
});

describe("Tag", () => {
  it("removable: the remove button has a name, clicking calls onRemove", () => {
    const onRemove = vi.fn();
    render(
      <Tag onRemove={onRemove} removeLabel="Remove filter Service">
        Shop
      </Tag>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Remove filter Service" }));
    expect(onRemove).toHaveBeenCalledOnce();
  });
});

describe("Checkbox", () => {
  it("role checkbox is named by its label; clicking the text toggles the box", () => {
    const onChange = vi.fn();
    render(
      <Checkbox checked={false} onCheckedChange={onChange}>
        Wrap lines
      </Checkbox>,
    );
    const box = screen.getByRole("checkbox", { name: "Wrap lines" });
    expect(box.getAttribute("aria-checked")).toBe("false");
    fireEvent.click(screen.getByText("Wrap lines"));
    expect(onChange).toHaveBeenCalledWith(true);
  });

  it("indeterminate: aria-checked=mixed", () => {
    render(
      <Checkbox
        checked={false}
        indeterminate
        onCheckedChange={() => {}}
        aria-label="Select page"
      />,
    );
    expect(screen.getByRole("checkbox", { name: "Select page" }).getAttribute("aria-checked")).toBe(
      "mixed",
    );
  });
});

describe("CopyButton", () => {
  it("a failed copy ⇒ says so (aria-live), not silent", async () => {
    Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
    Object.defineProperty(document, "execCommand", {
      value: vi.fn().mockReturnValue(false),
      configurable: true,
    });
    render(<CopyButton value="x" label="command" />);
    fireEvent.click(screen.getByRole("button", { name: "Copy command" }));
    expect(await screen.findByRole("button", { name: /Copy failed/ })).toBeTruthy();
    expect(
      screen.getByText(/Copy failed/, { selector: "[aria-live] , [aria-live] *" }),
    ).toBeTruthy();
  });

  it("with text: the button's name is the text, changes to Copied after copying", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    render(<CopyButton value="s3cr3t" label="secret" text="Copy secret" showText />);
    fireEvent.click(screen.getByRole("button", { name: "Copy secret" }));
    expect(writeText).toHaveBeenCalledWith("s3cr3t");
    expect(await screen.findByRole("button", { name: "Copied" })).toBeTruthy();
  });
});

describe("Tabs with step numbers", () => {
  it("step numbers are only visual: the tab name is still the text", () => {
    render(
      <Tabs
        id="t"
        label="Quickstart"
        value="a"
        options={[
          { value: "a", label: "Install", step: 1 },
          { value: "b", label: "Serve", step: 2 },
        ]}
        onChange={() => {}}
      />,
    );
    expect(screen.getByRole("tab", { name: "Serve" })).toBeTruthy();
    expect(screen.getByText("2").getAttribute("aria-hidden")).toBe("true");
  });
});

describe("scrollable Table", () => {
  it("overflowing frame ⇒ takes focus, has a region role and a name; fits the frame ⇒ does not", () => {
    const sizes = { scrollWidth: 900, clientWidth: 375 };
    const spy = vi
      .spyOn(HTMLElement.prototype, "scrollWidth", "get")
      .mockImplementation(() => sizes.scrollWidth);
    const spy2 = vi
      .spyOn(HTMLElement.prototype, "clientWidth", "get")
      .mockImplementation(() => sizes.clientWidth);
    const table = (
      <Table>
        <THead>
          <TH>a</TH>
        </THead>
        <TBody>
          <TRow>
            <TD>1</TD>
          </TRow>
        </TBody>
      </Table>
    );
    render(table);
    const region = screen.getByRole("region", { name: "Table, scrolls" });
    expect(region.getAttribute("tabindex")).toBe("0");
    cleanup();
    sizes.scrollWidth = 375;
    render(table);
    expect(screen.queryByRole("region")).toBeNull();
    spy.mockRestore();
    spy2.mockRestore();
  });
});
