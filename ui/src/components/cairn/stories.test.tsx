// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
// Every story of the cairn components: renders, and axe is clean in both themes.
// `color-contrast` needs real layout — contrast is checked on the tokens by
// `src/styles/contrast.test.ts`.
import { composeStories } from "@storybook/react";
import { cleanup, render } from "@testing-library/react";
import axe from "axe-core";
import { afterEach, describe, expect, it } from "vitest";
import * as stories from "./cairn.stories";

afterEach(cleanup);

const composed = composeStories(stories);

describe.each(["light", "dark"])("story cairn — theme %s", (theme) => {
  for (const [name, Story] of Object.entries(composed)) {
    it(`${name}: renders and axe is clean`, async () => {
      document.documentElement.classList.toggle("dark", theme === "dark");
      const { container } = render(<Story />);
      expect(container.innerHTML.length).toBeGreaterThan(0);
      const res = await axe.run(container, {
        runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"] },
        rules: { "color-contrast": { enabled: false } },
      });
      expect(res.violations.map((v) => `${v.id}: ${v.help}`)).toEqual([]);
    });
  }
});
