// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * A chat agent with the Vercel AI SDK v7 on cairnflow — see README.md next to this
 * file and `docs/ai-sdk.md`.
 *
 * One virtual object `Chat`, key = `chatId`. Each user message is one invocation of
 * `Chat.send`: model and tool calls are durable steps (dying midway continues
 * instead of calling the model again), tokens go to the `ui` stream as soon as they
 * are generated, and the history lives in the object's state.
 */
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import type { LanguageModelV4 } from "@ai-sdk/provider";
import { convertToModelMessages, gateway, stepCountIs, streamText, tool, wrapLanguageModel } from "ai";
import { z } from "zod";
import { cairnMiddleware, cairnTools, chatObject, serve } from "../../src/index.ts";
import { scriptedModel } from "./scripted.ts";

const tools = {
  weather: tool({
    description: "Current weather in a city",
    inputSchema: z.object({ city: z.string() }),
    // Call the external API here. `cairnTools` wraps it in `ctx.run`: the result goes into
    // the journal, and a rerun after a failure does not call it again.
    execute: async ({ city }) => ({ city, sky: "sunny", celsius: 31 }),
  }),
  refund: tool({
    description: "Refund an order — needs an approver",
    inputSchema: z.object({ order: z.number(), amount: z.number() }),
    execute: async ({ order, amount }) => ({ order, refunded: amount }),
  }),
};

/**
 * Nothing set: the scripted model. `CHAT_MODEL` alone: AI Gateway.
 * `CHAT_MODEL` + `CHAT_BASE_URL` (+ `CHAT_API_KEY`): an OpenAI-style API
 * (Z.ai, OpenRouter, vLLM, Ollama, …).
 */
function model(): LanguageModelV4 {
  const id = process.env["CHAT_MODEL"];
  if (id === undefined) return scriptedModel();
  const baseURL = process.env["CHAT_BASE_URL"];
  if (baseURL === undefined) return gateway(id);
  const apiKey = process.env["CHAT_API_KEY"];
  return createOpenAICompatible({ name: "chat", baseURL, ...(apiKey === undefined ? {} : { apiKey }), includeUsage: true })(id);
}

const chat = chatObject("Chat", async (ctx, turn) =>
  streamText({
    model: wrapLanguageModel({
      model: model(),
      middleware: cairnMiddleware(ctx),
    }),
    system: "You are a shop's assistant. Answer briefly, in English.",
    // `refund` waits for an approver via `ctx.promise` — durable, without holding a process.
    tools: cairnTools(ctx, tools, { approve: ["refund"] }),
    messages: await convertToModelMessages(turn.messages),
    stopWhen: stepCountIs(5),
    onError: turn.onError,
  }),
);

const port = Number(process.env["PORT"] ?? 9081);
const endpoint = await serve({ services: [chat], port });
console.log(`ai-chat listening on ${endpoint.url}`);
console.log(`model: ${process.env["CHAT_MODEL"] ?? "scripted (scripted.ts) — set CHAT_MODEL to use a real model"}`);
console.log(`run the walkthrough: node example/ai-chat/demo.ts`);
