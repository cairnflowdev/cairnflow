// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Stories of the basic cairn components: one story per state.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Inbox } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "./checkbox";
import { CopyButton } from "./copy-button";
import { EmptyState } from "./empty-state";
import { Id } from "./id";
import { PartialBanner } from "./partial-banner";
import { RoleGate } from "./role-gate";
import { RowActions, RowButton, RowDelete } from "./row-actions";
import { Segmented } from "./segmented";
import { StatusBadge } from "./status-badge";
import { Table, TBody, TD, TH, THead, TRow } from "./table";
import { TabPanel, Tabs } from "./tabs";
import { Tag } from "./tag";
import { Time } from "./time";

const meta: Meta = { title: "Cairn/Basics" };
export default meta;
type Story = StoryObj;

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0);

export const Ids: Story = {
  render: () => (
    <div className="flex flex-col gap-2">
      <Id value="3fa9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21" />
      <Id value="dp_9a12" full />
    </div>
  ),
};

export const Times: Story = {
  render: () => (
    <div className="flex flex-col gap-2">
      <Time ms={NOW - 3 * 60_000} now={NOW} />
      <Time ms={NOW - 26 * 3600_000} now={NOW} />
      <Time ms={NOW + 90_000} now={NOW} />
    </div>
  ),
};

export const Statuses: Story = {
  render: () => (
    <div className="flex flex-wrap gap-2">
      {["running", "suspended", "paused", "completed", "failed", "scheduled"].map((s) => (
        <StatusBadge key={s} status={s} />
      ))}
      <StatusBadge status="retrying" attempts={3} />
      <StatusBadge status="weird-new-state" />
    </div>
  ),
};

export const Empty: Story = {
  render: () => (
    <EmptyState
      icon={<Inbox />}
      title="No deployments yet"
      action={<Button>Register a deployment</Button>}
    >
      Run the SDK example, then paste its URL here.
    </EmptyState>
  ),
};

export const Partial: Story = {
  render: () => (
    <PartialBanner
      warnings={[
        { range: 3, reason: "node not responding" },
        { range: null, reason: "index building" },
      ]}
    />
  ),
};

export const RoleGated: Story = {
  render: () => (
    <div className="flex gap-2">
      <RoleGate have={["read", "invoke", "operate"]} need="operate">
        {(blocked) => <Button disabled={!!blocked}>Resume</Button>}
      </RoleGate>
      <RoleGate have={["read"]} need="operate">
        {(blocked) => (
          <Button disabled={!!blocked} title={blocked ?? undefined}>
            Purge
          </Button>
        )}
      </RoleGate>
      <RoleGate have={["read", "invoke", "operate"]} need="cluster" mode="hide">
        {() => <Button>Split range</Button>}
      </RoleGate>
    </div>
  ),
};

export const RowActionButtons: Story = {
  render: () => (
    <Table>
      <THead>
        <TH>Schedule</TH>
        <TH>
          <span className="sr-only">Actions</span>
        </TH>
      </THead>
      <TBody>
        <TRow>
          <TD>nightly-report</TD>
          <TD>
            <RowActions>
              <RowButton>Pause</RowButton>
              <RowButton>Edit</RowButton>
              <RowButton destructive>Revoke</RowButton>
              <RowDelete label="Delete nightly-report" title="Delete" onClick={() => {}} />
            </RowActions>
          </TD>
        </TRow>
      </TBody>
    </Table>
  ),
};

const SERVICES = [
  { name: "Shop", turns: 18_204, failed: 3, p99: "412 ms" },
  { name: "PaymentReconciliationWorkflowWithAVeryLongName", turns: 7, failed: 0, p99: "—" },
  { name: "Cart", turns: 1_204_118, failed: 120, p99: "2.1 s" },
];

const serviceRows = (dense?: boolean) => (
  <Table dense={dense}>
    <THead>
      <TH>Service</TH>
      <TH numeric>Turns</TH>
      <TH numeric>Failed</TH>
      <TH numeric>p99</TH>
    </THead>
    <TBody>
      {SERVICES.map((s) => (
        <TRow key={s.name}>
          <TD className="font-medium">{s.name}</TD>
          <TD numeric>{s.turns.toLocaleString("en")}</TD>
          <TD numeric>{s.failed}</TD>
          <TD numeric>{s.p99}</TD>
        </TRow>
      ))}
    </TBody>
  </Table>
);

export const DataTable: Story = { render: () => serviceRows() };

export const DataTableDense: Story = { render: () => serviceRows(true) };

export const DataTableSticky: Story = {
  render: () => (
    <Table stickyHeader>
      <THead>
        <TH>id</TH>
        <TH numeric>duration_ms</TH>
      </THead>
      <TBody>
        {Array.from(
          { length: 40 },
          (_, i) => `inv_${(i * 7919).toString(16).padStart(6, "0")}`,
        ).map((id, i) => (
          <TRow key={id}>
            <TD className="font-mono">{id}</TD>
            <TD numeric>{(i * 37) % 1000}</TD>
          </TRow>
        ))}
      </TBody>
    </Table>
  ),
};

export const InlineEmpty: Story = {
  render: () => (
    <div className="flex flex-col gap-4">
      <EmptyState variant="inline" title="No turns in the last hour" />
      <EmptyState
        variant="inline"
        icon={<Inbox />}
        title="No keys yet"
        action={
          <Button size="sm" variant="outline">
            Create key
          </Button>
        }
      >
        Keys let SDK endpoints and scripts call this tenant.
      </EmptyState>
    </div>
  ),
};

export const Tags: Story = {
  render: () => (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <Tag>This session</Tag>
        <Tag tone="default">Virtual object</Tag>
        <Tag tone="strong">staging</Tag>
        <Tag tone="danger">prod-eu</Tag>
        <Tag tone="filled">3</Tag>
        <Tag mono onRemove={() => {}} removeLabel="Remove tag team">
          team=payments
        </Tag>
      </div>
      <ul className="flex flex-wrap gap-1.5" aria-label="Active filters">
        <Tag as="li" size="md" onRemove={() => {}} removeLabel="Remove filter Service">
          <span className="flex items-center pr-1.5 pl-2 text-muted-foreground">Service is</span>
          <span className="flex items-center border-l border-border px-2 font-mono">Shop</span>
        </Tag>
      </ul>
    </div>
  ),
};

export const Checkboxes: Story = {
  render: () => (
    <div className="flex flex-col gap-2">
      <Checkbox checked onCheckedChange={() => {}}>
        Wrap lines
      </Checkbox>
      <Checkbox checked={false} onCheckedChange={() => {}}>
        Root invocations only
      </Checkbox>
      <Checkbox checked={false} indeterminate onCheckedChange={() => {}} aria-label="Select page" />
      <Checkbox checked={false} disabled onCheckedChange={() => {}}>
        Force (skip checks)
      </Checkbox>
    </div>
  ),
};

export const CopyButtons: Story = {
  render: () => (
    <div className="flex items-center gap-3">
      <CopyButton value="cd sdk/typescript && npm run example" label="command" />
      <CopyButton value="ck_live_…" label="secret" text="Copy secret" showText variant="outline" />
      <Id value="3fa9c21b0e6d4a7f9be1d2c3a4b5c6d7e8f9a0b1c2d3ec21" />
    </div>
  ),
};

export const SegmentedSwitch: Story = {
  render: () => (
    <Segmented
      label="View"
      value="table"
      options={[
        { value: "table", label: "Table" },
        { value: "chart", label: "Chart" },
        { value: "map", label: "Map", disabled: true },
      ]}
      onChange={() => {}}
    />
  ),
};

export const TabStripSteps: Story = {
  render: () => (
    <div className="flex flex-col gap-2">
      <Tabs
        id="story-steps"
        label="Quickstart"
        value="serve"
        options={[
          { value: "install", label: "Install the SDK", step: 1 },
          { value: "serve", label: "Serve", step: 2 },
          { value: "register", label: "Register", step: 3 },
          { value: "invoke", label: "Invoke", step: 4 },
        ]}
        onChange={() => {}}
      />
      <TabPanel id="story-steps" value="serve">
        <p className="text-muted-foreground">Call serve() with your identity keys.</p>
      </TabPanel>
    </div>
  ),
};

export const TabStrip: Story = {
  render: () => (
    <div className="flex w-72 flex-col gap-2">
      <Tabs
        id="story-tabs"
        label="Samples and tables"
        value="samples"
        options={[
          { value: "samples", label: "Samples" },
          { value: "recent", label: "Recent" },
          { value: "tables", label: "Tables" },
        ]}
        onChange={() => {}}
      />
      <TabPanel id="story-tabs" value="samples">
        <p className="px-2 text-muted-foreground">Handlers that fail most</p>
      </TabPanel>
    </div>
  ),
};
