// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A signing key standing in for the runtime's request identity (`identity.rs`), for tests
 * that need an endpoint with `identityKeys`: the replay cache and wire v6 are served only
 * when the endpoint verifies who is calling.
 */

import { DISCOVER_JTI, expectedJti, JWT_HEADER, SCHEME_HEADER, SCHEME_V1 } from "../src/identity.ts";
import { decodeFrames, decodeStart, MT } from "../src/wire.ts";

const enc = new TextEncoder();

function base58Encode(bytes: Uint8Array): string {
  const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const digits: number[] = [];
  for (const b of bytes.subarray(zeros)) {
    let carry = b;
    for (let i = 0; i < digits.length; i++) {
      carry += digits[i]! << 8;
      digits[i] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }
  return "1".repeat(zeros) + digits.reverse().map((d) => alphabet[d]).join("");
}

function b64url(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64url");
}

export interface Signer {
  /** The `kid` to list in `identityKeys`. */
  readonly kid: string;
  /** A JWT over `claims`. */
  sign(claims: Record<string, unknown>): Promise<string>;
  /** Identity headers for a request to `path` (`/discover` or `/invoke/S/h` with its body). */
  headers(path: string, body?: Uint8Array): Promise<Record<string, string>>;
}

/** A fresh Ed25519 key. */
export async function signer(): Promise<Signer> {
  const pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"])) as unknown as {
    publicKey: CryptoKey;
    privateKey: CryptoKey;
  };
  const raw = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
  const kid = `publickeyv1_${base58Encode(raw)}`;
  const sign = async (claims: Record<string, unknown>): Promise<string> => {
    const head = b64url(enc.encode(JSON.stringify({ alg: "EdDSA", typ: "JWT", kid })));
    const body = b64url(enc.encode(JSON.stringify(claims)));
    const sig = new Uint8Array(await crypto.subtle.sign({ name: "Ed25519" }, pair.privateKey, enc.encode(`${head}.${body}`)));
    return `${head}.${body}.${b64url(sig)}`;
  };
  return {
    kid,
    sign,
    headers: async (path, body) => {
      let jti = DISCOVER_JTI;
      if (body !== undefined) {
        const first = decodeFrames(body)[0];
        if (first === undefined || first.type !== MT.START) throw new Error("the body does not open with Start");
        jti = expectedJti(decodeStart(first.payload));
      }
      const now = Math.floor(Date.now() / 1000);
      const jwt = await sign({ aud: path, sub: "acme", iat: now, nbf: now - 60, exp: now + 60, jti });
      return { [SCHEME_HEADER]: SCHEME_V1, [JWT_HEADER]: jwt };
    },
  };
}
