// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// SSO sign-in: the `sso_error` codes the server (`crates/cairn-server/src/ui.rs`)
// attaches when it sends the browser back to the sign-in page.
import { m } from "@/paraglide/messages";

/** Message for each server `sso_error` code; an unknown code ⇒ a generic message with the code. */
export function ssoErrorText(code: string): string {
  switch (code) {
    case "access_denied":
      return m.login_sso_error_access_denied();
    case "revoked":
      return m.login_sso_error_revoked();
    case "issuer_unavailable":
    case "unavailable":
      return m.login_sso_error_issuer_unavailable();
    case "invalid_state":
      return m.login_sso_error_invalid_state();
    case "token_rejected":
    case "idp_error":
      return m.login_sso_error_token_rejected();
    case "not_configured":
      return m.login_sso_error_not_configured();
    default:
      return m.login_sso_error_other({ code });
  }
}

/** Initials of the name (or email) for the avatar in the top bar. */
export function initials(name: string | null | undefined, email?: string | null): string {
  const src = (name ?? "").trim() || (email ?? "").split("@")[0] || "";
  const parts = src.split(/[\s._-]+/).filter(Boolean);
  const letters = parts.length > 1 ? [parts[0], parts[parts.length - 1]] : parts.slice(0, 1);
  return letters
    .map((p) => Array.from(p ?? "")[0] ?? "")
    .join("")
    .toLocaleUpperCase();
}
