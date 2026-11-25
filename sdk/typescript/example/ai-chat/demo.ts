// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Walkthrough for `app.ts`, playing the browser: the AI SDK's `AbstractChat` (the core
 * of `useChat`) with `cairnChatTransport`, printing to the terminal instead of rendering React.
 *
 * Three scenes:
 *
 * 1. Ask about the weather — the model calls the `weather` tool, then answers; the text
 *    appears gradually.
 * 2. Ask for a refund — the `refund` tool waits for an approver. While it waits, a "new
 *    page" (a new transport that remembers nothing) reconnects through the object's
 *    `active` and sees the same approval request. Approve ⇒ the tool runs, both pages
 *    see the result.
 * 3. The history read back from the object (`messages`), and tokens added up at `/spans`.
 *
 * Needs `cairnflow --no-auth` at `CAIRNFLOW_SERVER` (default :8080) and `app.ts`
 * at `SDK_URL` (default :9081). The walkthrough registers the deployment itself.
 */
import { AbstractChat, type ChatState, type ChatStatus, type UIMessage } from "ai";
import { cairnChatTransport } from "../../src/index.ts";

const RUNTIME = process.env["CAIRNFLOW_SERVER"] ?? "http://127.0.0.1:8080";
const SDK = process.env["SDK_URL"] ?? "http://127.0.0.1:9081";
const chatId = `demo-${Date.now().toString(36)}`;

/** Whether the terminal cursor is at the start of a line — so tool lines do not land mid-text. */
let atLineStart = true;
function out(text: string): void {
  if (text.length === 0) return;
  process.stdout.write(text);
  atLineStart = text.endsWith("\n");
}

/**
 * `useChat` without React: the state is an array, and each change prints the new part.
 * Only the page with `printText` prints text — two pages printing would double it.
 */
class TerminalChat extends AbstractChat<UIMessage> {
  constructor(label: string, transport: ReturnType<typeof cairnChatTransport>, printText: boolean) {
    const printed = new Map<string, number>();
    const show = (m: UIMessage) => {
      if (m.role !== "assistant") return;
      m.parts.forEach((p, i) => {
        const key = `${m.id}:${i}`;
        if (p.type === "text") {
          const at = printed.get(key) ?? 0;
          if (printText && p.text.length > at) out(p.text.slice(at));
          printed.set(key, p.text.length);
        } else if (p.type.startsWith("tool-")) {
          const t = p as { state: string; input?: unknown; output?: unknown };
          if (printed.get(key + t.state) === undefined) {
            printed.set(key + t.state, 1);
            const detail = t.state === "output-available" ? t.output : t.state === "input-available" ? t.input : undefined;
            out(`${atLineStart ? "" : "\n"}   [${label}] ⚙ ${p.type.slice(5)} ${t.state}${detail === undefined ? "" : ` ${JSON.stringify(detail)}`}\n`);
          }
        }
      });
    };
    const state: ChatState<UIMessage> = {
      status: "ready" as ChatStatus,
      error: undefined,
      messages: [],
      pushMessage: (m) => {
        state.messages.push(m);
        show(m);
      },
      popMessage: () => void state.messages.pop(),
      replaceMessage: (i, m) => {
        state.messages[i] = m;
        show(m);
      },
      snapshot: (t) => structuredClone(t),
    };
    super({ id: chatId, transport, state });
  }
}

async function waitFor(what: string, f: () => boolean, ms = 30_000): Promise<void> {
  for (const end = Date.now() + ms; !f(); ) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

const reg = await fetch(`${RUNTIME}/deployments`, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ uri: SDK }),
});
if (!reg.ok) throw new Error(`POST /deployments → ${reg.status}: ${await reg.text()}`);
console.log(`chat ${chatId} — runtime ${RUNTIME}, SDK ${SDK}\n`);

const transport = cairnChatTransport({ baseUrl: RUNTIME, service: "Chat" });
const page1 = new TerminalChat("page 1", transport, true);

out("1. user: What's the weather like in Hanoi?\n");
await page1.sendMessage({ text: "What's the weather like in Hanoi?" });
out("\n\n");

out("2. user: Please refund order 42, 5 dollars\n");
const sent = page1.sendMessage({ text: "Please refund order 42, 5 dollars" });
const pending = () =>
  page1.lastMessage?.parts.find((p) => p.type === "tool-refund" && (p as { state: string }).state === "approval-requested") as
    | { approval: { id: string } }
    | undefined;
await waitFor("the approval request", () => pending() !== undefined);
out("   … the invocation is suspended on ctx.promise — no process holds it\n");

// "Reload the page": a new transport does not remember the id, it asks the object's `active`.
const page2 = new TerminalChat("page 2", cairnChatTransport({ baseUrl: RUNTIME, service: "Chat" }), false);
page2.messages = (await transport.messages(chatId)) as UIMessage[];
const resumed = page2.resumeStream();
await waitFor("page 2 to see the approval request", () =>
  page2.lastMessage?.parts.some((p) => p.type === "tool-refund" && (p as { state: string }).state === "approval-requested") === true,
);
out("   page 2 reconnects through `active` and sees the same approval request; the approver clicks…\n");
await transport.approve(chatId, pending()!.approval.id, { approved: true });
await sent;
await resumed;
out("\n\n");

console.log("3. history in the object:");
const history = (await transport.messages(chatId)) as UIMessage[];
for (const m of history) {
  const text = m.parts.filter((p) => p.type === "text").map((p) => (p as { text: string }).text).join("");
  const tools = m.parts.filter((p) => p.type.startsWith("tool-")).map((p) => p.type.slice(5));
  console.log(`   ${m.role.padEnd(9)} ${text}${tools.length > 0 ? `  [tool: ${tools.join(", ")}]` : ""}`);
}
const last = history.at(-1)!;
const spans = (await (await fetch(`${RUNTIME}/invocations/${last.id}/spans`)).json()) as {
  tokens: { input: number; output: number };
};
console.log(`\n   last turn (invocation ${last.id}): ${spans.tokens.input} tokens in, ${spans.tokens.output} tokens out`);
