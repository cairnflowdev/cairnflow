// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Errors shared between the handler (`handler.ts`) and the client (`client.ts`). No
 * `node:*` import — the client runs in browsers and on the edge, and `instanceof
 * TerminalError` holds on both sides.
 */

/**
 * An error code that can travel on the wire (`uint32`): an integer in `[0, 2^32)`,
 * anything else becomes `500` — an invalid code must not break the error path itself.
 */
export function wireCode(code: number): number {
  return Number.isInteger(code) && code >= 0 && code < 2 ** 32 ? code : 500;
}

/** Business error: ends the invocation, NO retry. */
export class TerminalError extends Error {
  readonly code: number;

  constructor(message: string, code = 500) {
    super(message);
    this.name = "TerminalError";
    this.code = wireCode(code);
  }
}
