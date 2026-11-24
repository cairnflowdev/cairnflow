// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The outside world, simulated.
 *
 * Every function here is a **side effect**: it spends money, it writes to another
 * system, it is slow. No function may be called directly from a handler — they only
 * go through `ctx.run` (protocol-v1 §8.4), and that is the whole reason this file is
 * separate: the export list shows exactly the places that need wrapping.
 *
 * There is no `Math.random()` and no `Date.now()` in this file. A non-deterministic
 * stub breaks the very thing the example wants to prove — and it breaks silently,
 * only showing up on some replay later on.
 */

/** A piece of research, already paid for. */
export interface Note {
  readonly section: string;
  readonly summary: string;
  readonly tokens: number;
  /** Real money, priced by the model. This is what the concurrency cap also protects. */
  readonly usd: number;
}

/** A generated draft, with the bill of the generation. */
export interface Draft {
  readonly html: string;
  readonly tokens: number;
  readonly usd: number;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Simulated latency of one model call, long enough to see the cap holding things back. */
const MODEL_LATENCY_MS = Number(process.env.DEMO_MODEL_LATENCY_MS ?? 250);

/** Simulated price, $/1K tokens. */
const USD_PER_KTOKEN = 0.015;

function price(tokens: number): number {
  return Math.round((tokens / 1000) * USD_PER_KTOKEN * 10_000) / 10_000;
}

/** One model call for one section. Slow, and costly. */
export async function researchSection(topic: string, section: string): Promise<Note> {
  await sleep(MODEL_LATENCY_MS);
  const tokens = 800 + section.length * 40;
  return {
    section,
    summary: `[${topic}] ${section}: three key findings and two sources.`,
    tokens,
    usd: price(tokens),
  };
}

/** Merges the pieces into an article. One more model call, more expensive. */
export async function writeDraft(topic: string, notes: readonly Note[]): Promise<Draft> {
  await sleep(MODEL_LATENCY_MS);
  const tokens = 1_500 + notes.reduce((n, x) => n + x.tokens, 0) / 2;
  const body = notes.map((n) => `<section><h2>${n.section}</h2><p>${n.summary}</p></section>`).join("");
  return { html: `<article><h1>${topic}</h1>${body}</article>`, tokens, usd: price(tokens) };
}

/**
 * Three publishing steps. Each has a compensation step that **takes the same key** —
 * not by accident: the compensation must be registrable *before* the forward step
 * runs, so it must not depend on the value the forward step returns.
 */
export async function publishToCdn(job: string, html: string): Promise<string> {
  await sleep(40);
  return `https://cdn.example/${job}/${html.length}.html`;
}

export async function removeFromCdn(job: string): Promise<void> {
  await sleep(20);
}

export async function indexForSearch(job: string, topic: string): Promise<string> {
  await sleep(30);
  return `idx-${job}-${topic.length}`;
}

export async function removeFromIndex(job: string): Promise<void> {
  await sleep(20);
}

/**
 * The last step, and the one the example makes fail.
 *
 * `down` is turned on by the workflow's input (`chaos: "notify-down"`), not by some
 * random global variable: the reader must be able to point at *why* this run failed,
 * otherwise the saga demo is just a game of chance.
 */
export async function notifySubscribers(topic: string, url: string, down: boolean): Promise<number> {
  await sleep(30);
  if (down) {
    throw new Error(`notification bus returned 410 Gone for topic ${JSON.stringify(topic)}`);
  }
  return 120 + topic.length;
}
