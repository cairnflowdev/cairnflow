// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Fake OIDC issuer for the console e2e — the same role as the fake issuer
// of `crates/cairn-server/tests/oidc.rs` and the control plane's provider:
// discovery, JWKS (RS256, key generated each run), `/authorize` **auto-consents** for the
// "next user" the test sets through `POST /__next`, `/token` checks `code`,
// `client_id`, `redirect_uri` and PKCE S256. The callback carries `iss` (RFC 9207) like
// `oidc-provider`.
import { createHash, generateKeyPairSync, sign } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

/** Next user: the claims of the `id_token`. `deny` ⇒ `error=access_denied`. */
export type NextUser = {
  sub: string;
  name?: string;
  email?: string;
  role?: string;
  deny?: boolean;
};

type Pending = {
  clientId: string;
  redirectUri: string;
  challenge: string;
  nonce: string;
  user: NextUser;
};

const b64 = (b: Buffer | string) => Buffer.from(b).toString("base64url");

async function body(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

export async function startFakeIssuer(): Promise<{ url: string; close: () => Promise<void> }> {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: "e2e-1", alg: "RS256", use: "sig" };
  const codes = new Map<string, Pending>();
  let next: NextUser = {
    sub: "user-1",
    name: "Ada Lovelace",
    email: "ada@example.com",
    role: "admin",
  };
  let url = "";

  const idToken = (p: Pending) => {
    const now = Math.floor(Date.now() / 1000);
    const claims: Record<string, unknown> = {
      iss: url,
      aud: p.clientId,
      sub: p.user.sub,
      nonce: p.nonce,
      iat: now,
      exp: now + 300,
      email: p.user.email,
      name: p.user.name,
    };
    if (p.user.role) claims.cairn_role = p.user.role;
    const signed = `${b64(JSON.stringify({ alg: "RS256", kid: "e2e-1" }))}.${b64(JSON.stringify(claims))}`;
    return `${signed}.${b64(sign("sha256", Buffer.from(signed), privateKey))}`;
  };

  const json = (res: ServerResponse, status: number, v: unknown) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(v));
  };

  const server = createServer(async (req, res) => {
    const u = new URL(req.url ?? "/", url);
    if (req.method === "GET" && u.pathname === "/.well-known/openid-configuration") {
      return json(res, 200, {
        issuer: url,
        authorization_endpoint: `${url}/authorize`,
        token_endpoint: `${url}/token`,
        jwks_uri: `${url}/jwks`,
      });
    }
    if (req.method === "GET" && u.pathname === "/jwks") return json(res, 200, { keys: [jwk] });
    if (req.method === "POST" && u.pathname === "/__next") {
      next = JSON.parse(await body(req)) as NextUser;
      return json(res, 200, next);
    }
    if (req.method === "GET" && u.pathname === "/authorize") {
      const q = u.searchParams;
      const redirect = q.get("redirect_uri") ?? "";
      const state = q.get("state") ?? "";
      const back = new URL(redirect);
      back.searchParams.set("state", state);
      back.searchParams.set("iss", url);
      if (next.deny) {
        back.searchParams.set("error", "access_denied");
      } else {
        const code = `code-${codes.size + 1}-${Date.now()}`;
        codes.set(code, {
          clientId: q.get("client_id") ?? "",
          redirectUri: redirect,
          challenge: q.get("code_challenge") ?? "",
          nonce: q.get("nonce") ?? "",
          user: next,
        });
        back.searchParams.set("code", code);
      }
      res.writeHead(302, { location: back.toString() });
      return res.end();
    }
    if (req.method === "POST" && u.pathname === "/token") {
      const f = new URLSearchParams(await body(req));
      const p = codes.get(f.get("code") ?? "");
      codes.delete(f.get("code") ?? "");
      const verifier = f.get("code_verifier") ?? "";
      if (
        !p ||
        f.get("client_id") !== p.clientId ||
        f.get("redirect_uri") !== p.redirectUri ||
        createHash("sha256").update(verifier).digest("base64url") !== p.challenge
      ) {
        return json(res, 400, { error: "invalid_grant" });
      }
      return json(res, 200, { access_token: "at", token_type: "Bearer", id_token: idToken(p) });
    }
    res.writeHead(404);
    res.end();
  });
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  return {
    url,
    close: () => new Promise((done) => server.close(() => done())),
  };
}

/** Sets the fake issuer's next user (from a test, over HTTP — the issuer runs in the global setup process). */
export async function setNextUser(issuer: string, user: NextUser): Promise<void> {
  const r = await fetch(`${issuer}/__next`, { method: "POST", body: JSON.stringify(user) });
  if (!r.ok) throw new Error(`fake issuer: ${r.status}`);
}
