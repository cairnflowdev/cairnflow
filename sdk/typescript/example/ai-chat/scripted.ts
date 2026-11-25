// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A scripted fake model — so the example runs without an API key. Set `CHAT_MODEL`
 * (e.g. `anthropic/claude-sonnet-5-5`) together with `AI_GATEWAY_API_KEY` and
 * `app.ts` uses a real model through the AI Gateway instead of this one.
 *
 * The script reads the **prompt**, it does not count calls: replay does not call the
 * model, so the answer may only depend on what the model receives.
 *
 * - the user's last message mentions "refund" ⇒ call `refund`; mentions "weather" ⇒
 *   call `weather`;
 * - the prompt already has a tool result ⇒ answer in text based on that result;
 * - otherwise ⇒ repeat the question.
 */
import type { LanguageModelV4Prompt, LanguageModelV4StreamPart } from "@ai-sdk/provider";
import { MockLanguageModelV4 } from "ai/test";

const USAGE = (input: number, output: number) => ({
  inputTokens: { total: input, noCache: input, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: output, text: output, reasoning: 0 },
});

function lastUserText(prompt: LanguageModelV4Prompt): string {
  for (let i = prompt.length - 1; i >= 0; i--) {
    const m = prompt[i]!;
    if (m.role === "user") {
      return m.content.map((c) => (c.type === "text" ? c.text : "")).join(" ");
    }
  }
  return "";
}

/** The last tool result in the prompt, if any. */
function lastToolResult(prompt: LanguageModelV4Prompt): { toolName: string; output: unknown } | undefined {
  const last = prompt.at(-1);
  if (last?.role !== "tool") return undefined;
  const r = last.content.findLast((c) => c.type === "tool-result");
  if (r === undefined || r.type !== "tool-result") return undefined;
  const o = r.output as { type: string; value?: unknown };
  // A failed tool (`error-text`/`error-json`) ⇒ pass the error to the model as a result.
  return { toolName: r.toolName, output: o.type.startsWith("error") ? { error: o.value } : o.value };
}

function reply(prompt: LanguageModelV4Prompt): LanguageModelV4StreamPart[] {
  const words = (s: string, id: string): LanguageModelV4StreamPart[] => [
    { type: "text-start", id },
    ...s.split(/(?<= )/).map((w): LanguageModelV4StreamPart => ({ type: "text-delta", id, delta: w })),
    { type: "text-end", id },
  ];
  const done = lastToolResult(prompt);
  if (done !== undefined) {
    const o = done.output as Record<string, unknown>;
    const text =
      o["error"] !== undefined
        ? `Could not do it: ${String(o["error"])}.`
        : done.toolName === "weather"
        ? `In ${o["city"]} it is ${o["sky"]}, ${o["celsius"]}°C.`
        : o["denied"] === true
          ? `The refund request was denied${o["reason"] ? `: ${o["reason"]}` : ""}.`
          : `Refunded ${o["refunded"]} dollars for order ${o["order"]}.`;
    return [...words(text, "t"), { type: "finish", finishReason: { unified: "stop", raw: "stop" }, usage: USAGE(40, 12) }];
  }
  const q = lastUserText(prompt).toLowerCase();
  const call = (toolName: string, input: unknown): LanguageModelV4StreamPart[] => {
    const json = JSON.stringify(input);
    return [
      { type: "tool-input-start", id: "call-1", toolName },
      { type: "tool-input-delta", id: "call-1", delta: json },
      { type: "tool-input-end", id: "call-1" },
      { type: "tool-call", toolCallId: "call-1", toolName, input: json },
      { type: "finish", finishReason: { unified: "tool-calls", raw: "tool_calls" }, usage: USAGE(30, 8) },
    ];
  };
  if (q.includes("refund")) return call("refund", { order: Number(/\d+/.exec(q)?.[0] ?? 0), amount: 5 });
  if (q.includes("weather")) return call("weather", { city: q.includes("hue") ? "Hue" : "Hanoi" });
  return [...words(`You asked: "${q}". Try asking about the weather, or for a refund on an order.`, "t"), {
    type: "finish",
    finishReason: { unified: "stop", raw: "stop" },
    usage: USAGE(20, 10),
  }];
}

export function scriptedModel(delayMs = 40): MockLanguageModelV4 {
  return new MockLanguageModelV4({
    provider: "scripted",
    modelId: "scripted-1",
    doStream: async ({ prompt }) => {
      const parts: LanguageModelV4StreamPart[] = [{ type: "stream-start", warnings: [] }, ...reply(prompt)];
      return {
        stream: new ReadableStream({
          async start(c) {
            for (const p of parts) {
              c.enqueue(p);
              // Emit one word at a time: you can watch tokens go through the `ui` stream while generating.
              if (p.type === "text-delta") await new Promise((r) => setTimeout(r, delayMs));
            }
            c.close();
          },
        }),
      };
    },
  });
}
