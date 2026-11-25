// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Verifying the runtime's request identity.
 *
 * A runtime with an identity master signs every `/discover` and `/invoke` request
 * with the **tenant's** Ed25519 key:
 *
 * - `x-cairn-signature-scheme: v1`;
 * - `x-cairn-jwt-v1: <EdDSA JWT>` — header `{alg: "EdDSA", typ: "JWT", kid}`,
 *   claims `aud` = request path, `sub` = tenant, `iat`, `nbf` = `iat − 60`,
 *   `exp` = `iat + 60` (seconds), `jti` = `<invocation id hex>:<attempt>` (matches
 *   the request's own `Start`; discovery: `discover`), `host` = the `Host` header
 *   the runtime sent, and on `/invoke` `bsh` = base64url(sha256 of the body **before**
 *   compression; BIDI: the turn-opening body — `Start` + the replayed part). The SDK
 *   checks `host`/`bsh` when the claim is present (old runtimes do not sign them).
 *   `host` is compared with `identityHosts` if set, otherwise with the request's own
 *   `Host` (sender-chosen text: it only binds the JWT to an endpoint that routes by host).
 *
 * `kid` = `publickeyv1_<base58 of the public key>`; get it from the runtime's
 * `GET /identity` (`admin` key), or from the console. Pass it to
 * `serve({identityKeys})`: a request with no signature, signed with a key not in
 * the list, with the wrong `aud`, expired, or carrying another invocation's `jti`
 * ⇒ `401`, the handler does not run.
 *
 * Verification uses Web Crypto (`crypto.subtle`, Ed25519): runs on Node ≥ 22,
 * Bun, Deno, Workers — no `node:*` import.
 */

import type { StartInfo } from "./wire.ts";

export const SCHEME_HEADER = "x-cairn-signature-scheme";
export const SCHEME_V1 = "v1";
export const JWT_HEADER = "x-cairn-jwt-v1";
export const KID_PREFIX = "publickeyv1_";
/** Discovery `jti`: there is no invocation to bind to. */
export const DISCOVER_JTI = "discover";

/** The request could not prove it comes from this environment's runtime. */
export class IdentityError extends Error {
  override readonly name = "IdentityError";
}

/** Claims of a verified JWT. */
export interface IdentityClaims {
  readonly aud: string;
  readonly sub: string;
  readonly iat: number;
  readonly nbf: number;
  readonly exp: number;
  readonly jti: string;
  /** The `kid` that signed. */
  readonly kid: string;
  /** Host the runtime sent to — absent on old runtimes. */
  readonly host?: string;
  /** base64url(sha256) of the body before compression — absent on old runtimes and on discovery. */
  readonly bsh?: string;
}

/** base64url(sha256(`bytes`)) — the form of the `bsh` claim. */
export async function bodyHash(bytes: Uint8Array): Promise<string> {
  const d = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>));
  let bin = "";
  for (const b of d) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/** Decodes base58 (Bitcoin alphabet). Unknown character ⇒ `null`. */
export function base58Decode(s: string): Uint8Array<ArrayBuffer> | null {
  let zeros = 0;
  while (zeros < s.length && s[zeros] === "1") zeros++;
  const bytes: number[] = []; // least significant first
  for (const ch of s.slice(zeros)) {
    let carry = ALPHABET.indexOf(ch);
    if (carry < 0) return null;
    for (let i = 0; i < bytes.length; i++) {
      carry += bytes[i]! * 58;
      bytes[i] = carry & 0xff;
      carry >>= 8;
    }
    while (carry > 0) {
      bytes.push(carry & 0xff);
      carry >>= 8;
    }
  }
  const out = new Uint8Array(zeros + bytes.length);
  for (let i = 0; i < bytes.length; i++) out[zeros + i] = bytes[bytes.length - 1 - i]!;
  return out;
}

/**
 * **Strict** base64url: only the base64url alphabet, no padding, no whitespace,
 * zero trailing bits — exactly one spelling per byte sequence. `atob` follows
 * forgiving-base64 and accepts whitespace and arbitrary trailing bits, so one JWT
 * has many different strings that all pass verification; anyone using the JWT
 * string as a key (a one-JWT-one-stream ledger) could be bypassed. Non-canonical ⇒
 * throws (`verify` treats it as a broken JWT).
 */
function b64urlDecode(s: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]*$/.test(s) || s.length % 4 === 1) throw new RangeError("not canonical base64url");
  const pad = s.length % 4 === 0 ? "" : "=".repeat(4 - (s.length % 4));
  const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/") + pad);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  if (b64urlEncode(out) !== s) throw new RangeError("not canonical base64url");
  return out;
}

function b64urlEncode(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function hex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}

/** The `jti` the runtime sets for the turn carrying `start`. */
export function expectedJti(start: Pick<StartInfo, "id" | "retryCount">): string {
  return `${hex(start.id)}:${start.retryCount}`;
}

/** An endpoint's verifier: the list of trusted `kid`s. */
export class IdentityVerifier {
  private readonly keys: Map<string, Promise<CryptoKey>>;

  constructor(kids: readonly string[]) {
    if (kids.length === 0) throw new RangeError("identityKeys is empty");
    this.keys = new Map(
      kids.map((kid) => {
        const raw = kid.startsWith(KID_PREFIX) ? base58Decode(kid.slice(KID_PREFIX.length)) : null;
        if (raw === null || raw.length !== 32) {
          throw new RangeError(`identityKeys: ${JSON.stringify(kid)} is not ${KID_PREFIX}<base58 of 32 bytes>`);
        }
        return [kid, crypto.subtle.importKey("raw", raw, { name: "Ed25519" }, false, ["verify"])];
      }),
    );
  }

  /**
   * Verifies the identity headers of a request to `path`, at time `nowMs`.
   * Throws [`IdentityError`] when it does not pass; `jti` must still be compared
   * with `Start` (an `/invoke` turn) or with [`DISCOVER_JTI`] (discovery) — see [`expectedJti`].
   */
  async verify(
    header: (name: string) => string | undefined,
    path: string,
    nowMs: number = Date.now(),
  ): Promise<IdentityClaims> {
    const scheme = header(SCHEME_HEADER);
    if (scheme !== SCHEME_V1) {
      throw new IdentityError(`missing ${SCHEME_HEADER}: ${SCHEME_V1}`);
    }
    const token = header(JWT_HEADER);
    if (token === undefined) throw new IdentityError(`missing ${JWT_HEADER}`);
    const parts = token.split(".");
    if (parts.length !== 3) throw new IdentityError("JWT does not have three parts");
    let head: { alg?: unknown; kid?: unknown };
    let claims: Record<string, unknown>;
    try {
      head = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[0]!)));
      claims = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[1]!)));
    } catch {
      throw new IdentityError("JWT cannot be decoded");
    }
    if (head.alg !== "EdDSA" || typeof head.kid !== "string") {
      throw new IdentityError("JWT is not EdDSA with a kid");
    }
    const key = this.keys.get(head.kid);
    if (key === undefined) throw new IdentityError(`kid ${head.kid} is not in identityKeys`);
    let sig: Uint8Array<ArrayBuffer>;
    try {
      sig = b64urlDecode(parts[2]!);
    } catch {
      throw new IdentityError("JWT cannot be decoded");
    }
    const ok = await crypto.subtle.verify(
      { name: "Ed25519" },
      await key,
      sig,
      new TextEncoder().encode(`${parts[0]}.${parts[1]}`),
    );
    if (!ok) throw new IdentityError("invalid signature");
    const num = (k: string): number => {
      const v = claims[k];
      if (typeof v !== "number") throw new IdentityError(`claim ${k} is missing`);
      return v;
    };
    const str = (k: string): string => {
      const v = claims[k];
      if (typeof v !== "string") throw new IdentityError(`claim ${k} is missing`);
      return v;
    };
    const opt = (k: string): string | undefined => {
      const v = claims[k];
      if (v === undefined) return undefined;
      if (typeof v !== "string") throw new IdentityError(`claim ${k} is not a string`);
      return v;
    };
    const host = opt("host");
    const bsh = opt("bsh");
    const out: IdentityClaims = {
      aud: str("aud"),
      sub: str("sub"),
      iat: num("iat"),
      nbf: num("nbf"),
      exp: num("exp"),
      jti: str("jti"),
      kid: head.kid,
      ...(host === undefined ? {} : { host }),
      ...(bsh === undefined ? {} : { bsh }),
    };
    if (out.aud !== path) throw new IdentityError(`aud ${out.aud} is not ${path}`);
    const now = Math.floor(nowMs / 1000);
    if (now < out.nbf || now > out.exp) throw new IdentityError("JWT is outside its nbf..exp window");
    return out;
  }
}
