// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// @vitest-environment happy-dom
import { describe, expect, it } from "vitest";
import { ssoHref } from "@/data/session";
import { initials, ssoErrorText } from "./sso";

describe("ssoErrorText", () => {
  it("every server code has its own message; unknown codes include the code", () => {
    const codes = [
      "access_denied",
      "revoked",
      "issuer_unavailable",
      "invalid_state",
      "token_rejected",
      "not_configured",
    ];
    const texts = codes.map(ssoErrorText);
    expect(new Set(texts).size).toBe(codes.length);
    expect(ssoErrorText("unavailable")).toBe(ssoErrorText("issuer_unavailable"));
    expect(ssoErrorText("idp_error")).toBe(ssoErrorText("token_rejected"));
    expect(ssoErrorText("wat")).toContain("wat");
  });
});

describe("initials", () => {
  it("multi-part name, one part, email, empty", () => {
    expect(initials("Ada Byron Lovelace")).toBe("AL");
    expect(initials("ada")).toBe("A");
    expect(initials(null, "grace.hopper@example.com")).toBe("GH");
    expect(initials("  ", null)).toBe("");
  });
});

describe("ssoHref", () => {
  it("return_to is a console path under the base path, encoded", () => {
    // No base meta (Vitest) ⇒ `/ui`, like the dev server.
    expect(ssoHref()).toBe("/ui/login/oidc?return_to=%2Fui%2F");
    expect(ssoHref("/invocations?status=failed")).toBe(
      "/ui/login/oidc?return_to=%2Fui%2Finvocations%3Fstatus%3Dfailed",
    );
  });
});
