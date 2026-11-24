// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Ports of the three e2e nodes. `CAIRN_E2E_PORT` (default 18480): node
// `noauth` (with sample data); node `auth` on port + 2 (empty — onboarding); node
// `sso` on port + 4 (OIDC sign-in with the fake issuer). Each node also uses
// port + 1 as its internal port.
export const E2E_PORT = Number(process.env.CAIRN_E2E_PORT ?? 18480);
export const E2E_AUTH_PORT = E2E_PORT + 2;
export const E2E_SSO_PORT = E2E_PORT + 4;
/** Tenant the OIDC client of the `sso` node points to (`auth/oidc`). */
export const E2E_SSO_TENANT = "acme";
export const E2E_PORTAL_URL = "https://portal.example.com/environments";
export const E2E_ADMIN_KEY = "cf_test_e2e_console_bootstrap_key_0123456789";
