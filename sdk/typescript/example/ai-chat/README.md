# ai-chat — a chat agent with the Vercel AI SDK v7

A small chat agent that runs without an API key, to show what cairnflow does underneath `streamText`
and `useChat`. The full guide (proxy, auth, cost, limits): [`docs/ai-sdk.md`](../../../../docs/ai-sdk.md).

```
browser (useChat)              cairnflow                     app.ts (SDK)
  sendMessage ──/send──────▶ Chat.send(chatId) ─────────▶ streamText
                                   │                         ├─ ctx.run("model:…")  ← durable step
  ◀── SSE stream `ui` ─────────────┤◀── UI chunks ───────────┤  tokens to `ui` as they are generated
                                   │                         ├─ ctx.run("tool:weather")
                                   │                         ├─ ctx.promise("approve-…")  ← waits for a human
  transport.approve ──/promises───▶│                         └─ ctx.set("messages")
```

## Run

```sh
cargo build --bin cairnflow                                          # at the repo root
./target/debug/cairnflow --data-dir ./data --port 8080 --no-auth     # dev: no API key

cd sdk/typescript
npm install                                 # pulls `ai`, `zod` (devDependencies of the example)
node example/ai-chat/app.ts                 # SDK on :9081
node example/ai-chat/demo.ts                # the walkthrough, a few seconds
```

The default model is the script in [`scripted.ts`](./scripted.ts): ask about the "weather" and it
calls the `weather` tool, ask for a "refund" and it calls `refund`. To use a real model through the
AI Gateway:

```sh
CHAT_MODEL=anthropic/claude-sonnet-5-5 AI_GATEWAY_API_KEY=… node example/ai-chat/app.ts
```

Or an OpenAI-style API (Z.ai, OpenRouter, vLLM, Ollama, …) through `@ai-sdk/openai-compatible` —
add `CHAT_BASE_URL` (and `CHAT_API_KEY` if needed):

```sh
CHAT_MODEL=glm-5.3 CHAT_BASE_URL=https://api.z.ai/api/coding/paas/v4 CHAT_API_KEY=… \
  node example/ai-chat/app.ts
```

The walkthrough has passed all three scenes with GLM-5.3 this way (2026-10-26); the model's
reasoning goes to the `ui` stream and into the history as text. Other providers (`@ai-sdk/openai`,
`@ai-sdk/anthropic`, …): install that package and replace `model()` in `app.ts`. The middleware does
not care which provider it is.

## What the walkthrough does

`demo.ts` plays the browser: the `AbstractChat` of `ai` (the core that `useChat` wraps) with
`cairnChatTransport`, printing to the terminal instead of rendering.

1. **Tools.** The model calls `weather`, the tool runs inside `ctx.run`, the model answers. The UI
   sees the tool part go through `input-streaming` → `input-available` → `output-available`, then
   the text appears gradually.
2. **Approval.** `refund` is in `approve`: the invocation is suspended on `ctx.promise` — no
   process, connection or RAM holds it, and it can wait three days just as well. While it waits, a
   "new page" (a new transport that remembers no id) calls `resumeStream`: the transport asks the
   object's `active` handler, attaches to that same invocation and sees the same approval request.
   `transport.approve` resolves the promise, the tool runs, and both pages see the result.
3. **History and tokens.** The history is read back from the object's `messages` handler; the
   tokens of the last turn add up at `GET /invocations/{id}/spans`.

Try also: kill `app.ts` (Ctrl-C) while it waits for approval, then run it again — the invocation
is still waiting, and once approved it continues from exactly where it was, without calling the
model again.

## Three places worth reading in `app.ts`

**`chatObject`, not `service`.** It builds a virtual object with `send` (EXCLUSIVE — two messages
of the same chat never run on top of each other), `active` and `messages` (SHARED — readable while
`send` runs). The transport only sends the last message; the history lives in the object's state.

**Everything other than the model goes through `cairnTools`.** An unwrapped tool's `execute` runs
again on every replay (every turn in `REQUEST_RESPONSE`, every wait for an ack in `BIDI_STREAM`)
and its result does not reach the UI.

**`onError: turn.onError`.** By default `streamText` `console.error`s every error, including
`Suspend` — the SDK's "turn suspended waiting for an ack" signal, one line per step.
`turn.onError` drops `Suspend` and writes real errors to the invocation's `$log`
(`GET /invocations/{id}/logs`).
