# Agents with the Vercel AI SDK on cairnflow

Write agents with `streamText` and `useChat` as usual; cairnflow turns every model call and tool
call into a **durable step**. If something dies midway, it continues from the unfinished step and
does not call a model that already answered again. Waiting for a human approval can take as long
as it takes without holding any process. Tokens reach the browser as soon as they are generated,
and reloading the page reconnects.

The adapter lives in `@cairnflow/sdk` (`sdk/typescript/src/vercel.ts`) and matches **AI SDK v7**
(`ai@7`). The SDK does not depend on the `ai` package: you install `ai` (and a provider) in your
own app. A runnable example:
[`sdk/typescript/example/ai-chat/`](../sdk/typescript/example/ai-chat/).

## 1. What there is

| You write | Cairnflow does | Checked in |
|---|---|---|
| `wrapLanguageModel({ model, middleware: cairnMiddleware(ctx) })` | Each model call is a `ctx.run`: the result goes into the journal, replay reads it back from the journal. The span `llm <model>` carries `input_tokens`/`output_tokens` | `test/ai-sdk.test.ts` (the model is called exactly once per step, in both modes) |
| `cairnTools(ctx, tools)` | Each `execute` is a `ctx.run` + a span `tool <name>`; input, result and error reach the UI | as above (the tool runs exactly once) |
| `cairnTools(ctx, tools, { approve: ["refund"] })` | The tool waits on a `ctx.promise` until someone approves; on rejection the tool does not run | `test/ai-sdk.test.ts` (approve, reject) |
| `chatObject("Chat", run)` | A virtual object keyed by `chatId`: the history in state, the id of the running turn, two messages of one chat never run concurrently | `test/ai-sdk.test.ts` (history, `regenerate`) |
| `useChat({ transport: cairnChatTransport(…) })` | Sends messages through `/send`, reads UI chunks over SSE; reloading the page ⇒ reconnects; `stop` ⇒ cooperative cancel: the running model/tool step receives `abortSignal` (BIDI), later steps do not run (section 3) | `test/vercel.test.ts`, `test/ai-sdk.test.ts` |
| `onError: turn.onError` | `streamText` errors go into the invocation's `$log` | — |

## 2. Server side

```ts
import { convertToModelMessages, stepCountIs, streamText, tool, wrapLanguageModel } from "ai";
import { anthropic } from "@ai-sdk/anthropic";
import { z } from "zod";
import { cairnMiddleware, cairnTools, chatObject, serve } from "@cairnflow/sdk";

const tools = {
  lookupOrder: tool({
    description: "Look up an order",
    inputSchema: z.object({ id: z.number() }),
    execute: async ({ id }) => db.orders.get(id),
  }),
  refund: tool({
    description: "Refund — needs a human approval",
    inputSchema: z.object({ id: z.number(), amount: z.number() }),
    execute: async ({ id, amount }) => payments.refund(id, amount),
  }),
};

const chat = chatObject("Chat", async (ctx, turn) =>
  streamText({
    model: wrapLanguageModel({ model: anthropic("claude-sonnet-5-5"), middleware: cairnMiddleware(ctx) }),
    system: "…",
    tools: cairnTools(ctx, tools, { approve: ["refund"] }),
    messages: await convertToModelMessages(turn.messages),
    stopWhen: stepCountIs(8),
    onError: turn.onError,
  }),
);

await serve({ services: [chat], port: 9080 });
```

Then register the deployment: `POST /deployments {"uri": "http://<SDK host>:9080"}` (or
`cairn register`).

**`chatObject` builds three handlers** on the virtual object `Chat`:

| Handler | Kind | What it does |
|---|---|---|
| `send` | EXCLUSIVE | Writes `active` = the invocation id; builds the history from state + the new message (or cuts it at the message to regenerate); calls `run`; drains `toUIMessageStream` (assistant message id = invocation id); stores `messages`; clears `active` |
| `active` | SHARED | The id of the running `send` invocation, or `null` |
| `messages` | SHARED | The history (`UIMessage[]`) |

If you need more (storing the history in your own DB, several agents, …), write your own virtual
object with `service(…, { type: "VIRTUAL_OBJECT" })` and use `cairnMiddleware`/`cairnTools`
directly; the transport only needs handlers `send`, `active`, `messages` with the same contract
as the table above.

**Three rules** (of every cairnflow handler, see `sdk/typescript/README.md`):

- Every external effect goes through `cairnTools` or `ctx.run`. A handler reruns from the start on
  every replay; an unwrapped `execute` calls the external API once more on every replay.
- No `Date.now()`, `Math.random()`, `generateId()` outside `ctx.run`.
- Do not catch and swallow errors around `ctx` commands. Swallowing does not change the outcome
  anyway: a turn with a transient error is still retried. Waiting on something that is not there
  yet throws nothing (the promise hangs until the end of the turn), so there is no `Suspend` left
  to swallow; `turn.onError` still ignores `Suspend` for old SDKs.

**Retries.** A transient error inside `ctx.run` (model 429/5xx, timeout, a tool throwing a plain
`Error`) ⇒ **that turn dies** and the runtime retries it according to the handler's `retryPolicy`
(`chatObject(name, run, { options: { retryPolicy } })`): it replays up to the failed step, reruns
exactly that step, and steps already done do not run again. `streamText` still catches the error
like a tool error, but can no longer do anything with it — every later `ctx` command throws
`TurnFailed`, so the model never answers on top of a transient error. For the same reason the AI
SDK does **not** retry model calls going through `cairnMiddleware` by itself (`maxRetries` has no
effect): a retry inside the same turn would be a new `ctx.run`, and the replay afterwards would
diverge. Provider errors that cannot be retried (`isRetryable: false` — 400 prompt too long, 401
wrong key) go into the journal as the step's failed result: the UI receives `error`, the turn
ends, no retry. A tool error that should not be retried throws `TerminalError`: the error result
goes into the journal (replay does not run the tool again), the model sees a tool error
(`tool-output-error` in the UI, `errorText` kept in the history) and goes on.

## 3. Browser side

```tsx
import { useChat } from "@ai-sdk/react";
import { cairnChatTransport } from "@cairnflow/sdk";

const transport = cairnChatTransport({ baseUrl: "/cairn", service: "Chat" });

function ChatPage({ chatId, initial }: { chatId: string; initial: UIMessage[] }) {
  const { messages, sendMessage, stop, status } = useChat({
    id: chatId,
    transport,
    messages: initial,      // await transport.messages(chatId) when building the page
    resume: true,           // reloading the page mid-generation ⇒ reconnects through `active`
  });
  // a tool part waiting for approval: part.type === "tool-refund" && part.state === "approval-requested"
  const approve = (approvalId: string, approved: boolean) =>
    transport.approve(chatId, approvalId, { approved });
  …
}
```

- **Approve tools with `transport.approve`, not with `addToolApprovalResponse`.** The latter is the
  AI SDK's `needsApproval` path: it ends the turn and sends the whole chat again. Here the
  invocation stays alive, suspended on `ctx.promise("approve-<toolCallId>")`; `approve` resolves
  that promise (`POST /promises/Chat/{chatId}/{approvalId}/resolve`). The approver need not be the
  person chatting — an admin page or a Slack webhook calling the same route works.
- **Pending requests can be read from `messages`.** The `ui` stream is a temporary stream and only
  keeps the running step: a turn suspended waiting for approval that sits idle for more than 60 s
  releases what it emitted (a leader change loses it entirely). While waiting, `cairnTools` keeps
  the request in the object's state (`pending-approval/<approvalId>`), and
  `transport.messages(chatId)` returns one extra assistant message (id = the running invocation's
  id) carrying the `approval-requested` tool part — a reloaded page still has the approve button.
  Once the turn is done, `messages` is the full history as before. Mid-turn, `messages` is the
  history **of the running turn**: it includes the message the user just sent (`send` keeps the
  turn's input in the `turn-input` state as soon as it starts).
- **A page with a stale history is refused.** The transport sends the id of the message right
  before the new one (`after`); when a tab that has not seen another tab's turn (or a page that has
  not loaded `messages`) sends a message, `send` runs no turn, does not change the history, and the
  UI receives an `error` chunk whose `errorText` starts with `STALE_HISTORY:`
  ("STALE_HISTORY: chat history is out of date; reload the chat and send again"). Compare with
  `errorText.startsWith(STALE_HISTORY_CODE)` (both constants are exported from `@cairnflow/sdk`),
  not with the wording. Initialising `useChat` with `messages: await transport.messages(chatId)` is
  enough. An old transport that does not send `after` is not checked.
- `stop()` ⇒ the browser stops reading, and the transport sends
  `POST /invocations/{id}/cancel?mode=signal` (cooperative cancel). The runtime delivers CANCEL
  into the running turn at once: the running model or tool call receives `abortSignal` (the
  provider aborts the request — no more tokens spent), and no later model/tool step runs. An
  aborted model call returns **what it generated so far** as the step's result
  (`finishReason: "other"`, `raw: "cancelled"`), so the turn ends `completed` and the history keeps
  exactly the text the UI showed (like ChatGPT's "Stop"). Measured (a fake model, 150 ms per
  chunk): BIDI stops within ~0.1 s of `stop()`. With `REQUEST_RESPONSE` there is no channel into
  the middle of a turn: the running step runs to its end, the cancel takes effect from the next
  step. A hand-written tool that wants to stop midway reads `options.abortSignal` of `execute`.
- A leader change in the middle of a step (a node dies, the range moves): the temporary stream is
  lost; the UI receives `reset-step` (drops the unfinished part of that step) and the new leader
  reruns the step from the start. Finished steps do not run again.
- The runtime dies and is brought back up (one node, `kill -9`): the transport reconnects, sees that
  the stream is a new life (by comparing with the last record received) ⇒ `reset-step`, the
  unfinished step is generated again from the start. Measured with GLM-5.3: the UI is seamless and
  matches the history. If the turn had finished its last step before dying, the new process emits
  nothing more: the transport closes quietly, the UI keeps what it received, and the full history
  is in `messages`.
- The SDK process dies in the middle of a step (deploy, OOM, `kill -9`): the runtime retries and
  marks the old attempt `superseded` on the stream; the UI receives `reset-step` too and sees the
  step generated again from the start. The model call of the unfinished step **is made again** (no
  result in the journal yet) — that step costs tokens twice, no more. Measured with GLM-5.3: killing
  the app at character 145, the UI shows one seamless answer that matches the history stored in the
  object.

## 4. Proxy and keys: the browser does not call cairnflow directly

Cairnflow **sends no CORS headers**, and an `invoke` key must not live in the browser: whoever has
it can call every service, read every chat and approve every request of that tenant. Put a proxy
on the same origin as the app, where you check the user and attach the key:

```ts
// Sketch — a Next.js route handler at app/cairn/[...path]/route.ts
async function handler(req: Request, { params }: { params: Promise<{ path: string[] }> }) {
  const { path } = await params;
  const user = await auth(req);                                    // your app's session
  if (!(await allowed(user, req.method, path, new URL(req.url))))  // see below
    return new Response("forbidden", { status: 403 });
  const target = `/${path.join("/")}${new URL(req.url).search}`;
  const init = {
    method: req.method,
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${process.env.CAIRNFLOW_INVOKE_KEY}`,
      // The transport sends `Idempotency-Key` with every message; `Last-Event-ID` when resuming SSE.
      ...Object.fromEntries(["idempotency-key", "accept", "last-event-id"].flatMap((h) => {
        const v = req.headers.get(h);
        return v === null ? [] : [[h, v]];
      })),
    },
    body: req.method === "GET" ? undefined : await req.text(),
  };
  // Cluster: the chat's range may live on a node other than the one the proxy calls ⇒ `503` +
  // `leader` (the leader's ingress port, out of the browser's reach) ⇒ the proxy asks there once.
  let res = await fetch(`${process.env.CAIRNFLOW_URL}${target}`, init);
  if (res.status === 503) {
    const { leader } = await res.clone().json().catch(() => ({}));
    if (typeof leader === "string") res = await fetch(`${leader}${target}`, init);
  }
  return res;                                                      // the SSE body passes straight through
}
export { handler as GET, handler as POST };
```

`allowed` lets exactly four paths through, and checks that the user has rights to that chat:

| Path | Where the chat is |
|---|---|
| `POST /invoke/Chat/{send/send,active,messages}` | `?key=<chatId>` |
| `POST /promises/Chat/{chatId}/{approvalId}/resolve` | the path |
| `GET /invocations/{id}/streams/ui`, `POST /invocations/{id}/cancel` | the invocation id — the proxy sees the body returned by `/send` (`{id}`) and by `active`, so it can keep an `id → chatId` table |

Do not buffer SSE (nginx: `X-Accel-Buffering: no`; disable compression for `text/event-stream`).

A Node server running right next to cairnflow (no browser) passes the key straight to the
transport: `cairnChatTransport({ baseUrl, service, headers: { authorization: "Bearer …" } })`. The
transport follows `leader` by itself (once, as in the error contract of `docs/admin-api.md`) and
falls back to `baseUrl` when the leader cannot be reached; every message carries its own
`Idempotency-Key`, so a resend does not create a second turn. Measured on a 3-node cluster: before
this existed, every chat whose range was not held by the client's node failed with `503`.

## 5. What happens under one chat turn

1. `sendMessage` ⇒ `POST /invoke/Chat/send/send?key=<chatId>` with the **last message** (not the
   whole history). Returns `202 {id}` at once; the transport opens the SSE
   `GET /invocations/{id}/streams/ui`.
2. `send` reads the history from state and calls `streamText`. The middleware opens
   `ctx.run("model.stream")`: it calls the provider, every part of the model stream is translated
   into a UI chunk (`start-step`, `text-delta`, `tool-input-available`, …) and appended to the
   temporary `ui` stream as soon as it arrives — the browser sees tokens while the step is still
   running. End of the stream ⇒ propose the result, wait for the runtime to commit.
3. The model calls tools ⇒ `ctx.run("tool:<name>")` per tool (in parallel if the model calls them
   in parallel). A tool needing approval ⇒ one more `ctx.run("approval:<name>")` that emits the
   request, then `ctx.promise`.
4. Repeat until `stopWhen`. Then `ctx.set("messages", …)`, end. The SSE receives `end`, the
   transport emits `finish`.

Replay (after an incident, after every wait for an ack, or every turn under `REQUEST_RESPONSE`):
the handler reruns from the start, `streamText` runs again, but the middleware and the tools
return results from the journal — no provider call, no tool call, nothing emitted to `ui` again.

## 6. Cost

- **The journal of each model step** keeps every part of the model stream (text, tool calls,
  usage). The middleware drops `request` (the request body sent to the provider — the whole
  conversation, one copy per step) and `response.body`.
- **The history lives in state.** Each turn reads it once, and that read writes the value into
  the journal: journal bytes per turn grow with the conversation's length. State and frames have
  the engine's 16 MiB limit, and a tenant can have a lower one: `max-payload=` in the `tenants`
  registry limits every frame written into the journal (one `ctx.set`, a step's result, a call's
  input) — a hosted plan may set it to **1 MiB**. Since the `values/segmented` gate, large values
  go **segmented**: `ctx.set("messages", …)`, the output of `messages` (page reload) and `ctx.run`
  /tool results larger than ~256 KiB are cut by the runtime into segments on object storage; the
  journal and the frames carry only refs, and the limit only applies to the inline part. A history
  longer than `max-payload=` can still be sent with `send` and read back. Remaining limits: ingress
  assembles outputs up to **64 MiB** (`/invoke`, `GET /invocations/{id}`; over it, `413
  value_too_large`), the tenant's `max-value-bytes=` limits the **logical** size of one value (a
  hosted plan: 64 MiB; over it the invocation is `failed` 579 `value_bytes_limit`), and SDKs below
  wire v6 receive the whole assembled value every turn. An appended history keeps few segments (the
  runtime merges them in tiers: ≤ 7 segments at 8 MiB), so a turn that misses the cache costs only
  a few GETs.
- **Do not move the old history into `ctx.artifact.put`.** An artifact is a file of **one** run:
  `ctx.artifact.get` reads by the current invocation's id, so the next `send` turn (a different
  invocation) cannot read the previous turn's artifact — unless it happens to hit the SDK
  process's RAM cache —, and artifacts are deleted together with the invocation that wrote them
  (once retention expires, through the trash). Large values carried every turn are segmented by the
  runtime automatically: the journal per turn no longer depends on the history's length, and
  `chatObject` benefits without code changes.
  `sendHistory: true` (sending the whole history as the AI SDK does by default) adds one more copy
  into entry 0.
- **The `ui` stream is a temporary stream:** the leader's RAM, no extra PUT. SSE is read only at
  the leader.
- **Each approval** adds one journal entry and one wait for a commit.
- Tokens and money: `GET /invocations/{id}/spans` (`tokens`), `GET /metering` (`inputTokens`,
  `outputTokens`, `costMicros` according to the `pricing` table).

## 7. Observing

| You want to know | Ask |
|---|---|
| A chat's running turn | `POST /invoke/Chat/active?key=<chatId>` |
| The span tree and tokens of a turn | `GET /invocations/{id}/spans` |
| `streamText` errors (through `turn.onError`), `ctx.log` | `GET /invocations/{id}/logs` |
| The journal (each model step, tool, approval) | `GET /invocations/{id}/journal` |
| What is waiting for approval | `GET /promises/Chat/{chatId}/{approvalId}` |

## 8. Known limits

- **AI SDK v7 only.** v5/v6 declare middleware and usage differently; the old v5 adapter was
  removed.
- **`REQUEST_RESPONSE` does not stream tokens.** Stream records travel in the response body and
  are applied at the end of the turn — every `ctx.run` is a turn — so a step's text appears **at
  once** when the step finishes, not gradually. A chat that needs flowing text uses `BIDI_STREAM`
  (the default of `serve`); RR only when a proxy between the runtime and the SDK speaks only
  HTTP/1.
- **On a shared cell with the SaaS profile, `serve`'s `BIDI_STREAM` does not happen by itself.** An
  `https://` endpoint for which `POST /deployments` does not state a `transport` is registered as
  `REQUEST_RESPONSE` even when the SDK declares BIDI ([`self-hosting.md`](./self-hosting.md) §8,
  `--deploy-https-request-response`): text appears once per step, and `stop` does not interrupt the
  running step. For BIDI the app must have HTTP/2 all the way to the process (Fly `h2_backend`,
  Cloud Run `--use-http2`) and register `{"uri": …, "transport": "declared"}`. Under
  `REQUEST_RESPONSE`, the SDK wait limit (`--inactivity-timeout-ms`) covers **the whole turn**: a
  model step longer than the limit is abandoned and rerun.
- **Under `REQUEST_RESPONSE`, `active` is only visible after the first durable step** (after the
  first model call): reloading the page during the first model call gives `resumeStream` nothing to
  reconnect to. `BIDI_STREAM` sees it at once.
- **If the runtime dies before a span's `start` is durable**, `/spans` lacks that span (the rerun
  does not send `start` again because the `Run` is already in the journal); its tokens are still
  counted. Not fixed yet.
- **A leader change exactly while a step's `finish-step` is on its way**, when that step has already
  committed: the UI drops the step's text, and the new leader does not emit it again (the step is
  done). The history stored in the object is still complete; reloading the page shows it.
- **Under `REQUEST_RESPONSE`, `stop` cannot interrupt the running step** (section 3): the cancel
  takes effect from the next model/tool step. `BIDI_STREAM` interrupts at once.
- **An invocation's output is assembled up to 64 MiB at ingress** (section 6): with a longer
  history, `messages` returns `413 value_too_large`. A history below that limit is no longer
  blocked by the tenant's `max-payload=`.
- No CORS (section 4).
- **Run against a real provider** once: GLM-5.3 through Z.ai's OpenAI-style API
  (`@ai-sdk/openai-compatible`, `example/ai-chat` with `CHAT_BASE_URL`) — tools, approval with a
  freshly reconnected page, reasoning shown in the UI and kept in the history, tokens in `/spans`,
  `kill -9` of the SDK app mid-generation. After the cooperative-cancel and stop fixes
  (2026-10-26) the whole single-node matrix ran again: `stop()` under BIDI stopped 35 ms after the
  click, the model was called once, the history kept the generated part (72 characters, the UI had
  shown 50). One matrix run hung once at `killAppTool` BIDI (`sendMessage` did not return), and
  three reruns did not reproduce it; cause unknown. Measured again with GLM-5.3-Flash (same day):
  `killAppStream` → `killAppTool` × 12 runs, both modes, 48/48 PASS; the full single-node matrix
  × 2, each run 27/28 (only `reload RR`, the limit above); a 3-node cluster (SeaweedFS, `kill -9` of
  app and runtime, concurrent chats, two tabs, `stop`, a long conversation) 28/28 — `reload RR`
  passes because the reload landed after the first durable step. The hang did not reproduce in 50
  `killAppTool` runs; cause still unknown. Automated tests still use `MockLanguageModelV4`.
- **A provider that refuses permanently but returns a transient code** (Z.ai with an exhausted
  plan returns `429`, code `1113`): the provider SDK retries, then the runtime retries with backoff
  — the UI sees no error, it only waits (measured: ~7 model calls per 120 s). The error shows only
  in `$log`/the invocation's state. Not handled: classifying provider errors is the handler's job.
- **Not checked yet:** React's `useChat` (the tests use `AbstractChat` — the core `useChat` wraps),
  other providers (`@ai-sdk/anthropic`, `@ai-sdk/openai`, AI Gateway).
