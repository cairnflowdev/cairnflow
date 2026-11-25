// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Storybook of the cairn components. Every story is also
// rendered by Vitest via `composeStories` and run through axe (`src/components/cairn/stories.test.tsx`),
// so `npm run check` checks them without a browser.
import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.tsx"],
  framework: { name: "@storybook/react-vite", options: {} },
  core: { disableTelemetry: true },
};

export default config;
