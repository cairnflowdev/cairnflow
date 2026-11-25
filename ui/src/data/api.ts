// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// HTTP client of the console. The node's API lives at the origin root
// (`/auth/whoami`, `/query`, …) — not under the UI base path. Authentication is the
// `HttpOnly` session cookie: the browser sends it, JS never sees the key.
//
// REST goes through `rest` — `openapi-fetch` with types generated from
// `docs/api/openapi.json` (`api.gen.ts`): calling a path that is not in the schema
// is a compile error. `api()` is left only for `<base>/session`, a UI path rather
// than an API one.
import createClient from "openapi-fetch";
import type { paths } from "./api.gen";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly retryAfterMs: number | null = null,
    /** `503` "not the leader": the leader's ingress port when the node knows it. */
    readonly leader: string | null = null,
    /** Machine-readable code of the error body (`ErrorCode` of the schema), when the server sends one. */
    readonly code: string | null = null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function retryAfter(res: Response): number | null {
  const v = res.headers.get("retry-after");
  if (v === null) return null;
  const s = Number(v);
  return Number.isFinite(s) ? s * 1000 : null;
}

function leaderOf(body: unknown): string | null {
  const l = (body as { leader?: unknown } | undefined)?.leader;
  return typeof l === "string" && l ? l : null;
}

function codeOf(body: unknown): string | null {
  const c = (body as { code?: unknown } | undefined)?.code;
  return typeof c === "string" && c ? c : null;
}

async function errorOf(res: Response): Promise<ApiError> {
  let message = res.statusText;
  let leader: string | null = null;
  let code: string | null = null;
  try {
    const body = (await res.clone().json()) as { error?: string };
    if (body.error) message = body.error;
    leader = leaderOf(body);
    code = codeOf(body);
  } catch {
    // body is not JSON — keep statusText
  }
  return new ApiError(res.status, message, retryAfter(res), leader, code);
}

// `fetch` is bound late: tests replace `globalThis.fetch` after the module has loaded.
//
// `x-cairn-forward: leader`: on a cluster, the node serving the console runs the
// request on the range leader itself instead of returning `503 leader` — the
// browser cannot follow `leader` (CSP `connect-src 'self'`, cookies are per origin).
export const FORWARD_HEADERS = { "x-cairn-forward": "leader" } as const;

export const rest = createClient<paths>({
  credentials: "same-origin",
  headers: FORWARD_HEADERS,
  fetch: (req) => globalThis.fetch(req),
});

/** Result of `rest.*`: the data on `2xx`, otherwise throws `ApiError`. */
export async function ok<T>(
  p: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await p;
  if (!response.ok) {
    // `openapi-fetch` has already read the error body into `error` — reading `response`
    // again hits a used body (it used to fall back to `statusText`, losing the server's message).
    const said = (error as { error?: unknown } | undefined)?.error;
    if (typeof said === "string" && said) {
      throw new ApiError(
        response.status,
        said,
        retryAfter(response),
        leaderOf(error),
        codeOf(error),
      );
    }
    throw await errorOf(response);
  }
  return data as T;
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
      ...FORWARD_HEADERS,
      ...init.headers,
    },
  });
  if (!res.ok) throw await errorOf(res);
  return (res.status === 204 ? undefined : await res.json()) as T;
}
