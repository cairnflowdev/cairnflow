// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The runtime's request-identity keys, derived the way `cairn-server/src/identity.rs` does,
// so the harness can give an SDK endpoint the `kid` of the tenant whose deployment points at
// it before any node runs: the SDK serves its replay cache (wire v5) and segmented values
// (wire v6) only to a runtime it verifies.
import { createPrivateKey, createPublicKey, hkdfSync } from "node:crypto";

/** The chaos nodes' `CAIRNFLOW_IDENTITY_MASTER` (≥ 32 bytes); `CHAOS_IDENTITY_MASTER` overrides it. */
export const IDENTITY_MASTER = process.env.CHAOS_IDENTITY_MASTER ?? "chaos-harness-identity-master-longer-than-32-bytes";

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function base58(bytes: Uint8Array): string {
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
  return "1".repeat(zeros) + digits.reverse().map((d) => ALPHABET[d]).join("");
}

/** PKCS#8 prefix of an Ed25519 private key: the 32-byte seed follows. */
const PKCS8_ED25519 = Buffer.from("302e020100300506032b657004220420", "hex");

/**
 * The `kid` the runtime signs `tenant`'s requests with at `generation` (1 until the tenant
 * rotates): seed = HKDF-SHA256(empty salt, master, "cairn-identity/v1" ‖ u32be len ‖ tenant ‖
 * u64be generation), kid = `publickeyv1_` + base58(Ed25519 public key).
 */
export function kidOf(master: string, tenant: string, generation = 1): string {
  const t = Buffer.from(tenant, "utf8");
  const info = Buffer.alloc(17 + 4 + t.length + 8);
  info.write("cairn-identity/v1", 0, "latin1");
  info.writeUInt32BE(t.length, 17);
  t.copy(info, 21);
  info.writeBigUInt64BE(BigInt(generation), 21 + t.length);
  const seed = Buffer.from(hkdfSync("sha256", Buffer.from(master, "utf8"), Buffer.alloc(0), info, 32));
  const key = createPrivateKey({ key: Buffer.concat([PKCS8_ED25519, seed]), format: "der", type: "pkcs8" });
  const raw = createPublicKey(key).export({ format: "der", type: "spki" }).subarray(-32);
  return `publickeyv1_${base58(new Uint8Array(raw))}`;
}
