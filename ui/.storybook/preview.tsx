// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

import "@fontsource-variable/inter";
import "@fontsource-variable/geist-mono";
import "../src/styles/globals.css";
import type { Preview } from "@storybook/react-vite";

// Theme from the toolbar: adds/removes the `.dark` class like the app's ThemeProvider.
const preview: Preview = {
  globalTypes: {
    theme: {
      description: "Theme",
      toolbar: { items: ["light", "dark"], dynamicTitle: true },
    },
  },
  initialGlobals: { theme: "light" },
  decorators: [
    (Story, ctx) => {
      document.documentElement.classList.toggle("dark", ctx.globals.theme === "dark");
      return <Story />;
    },
  ],
};

export default preview;
