// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * Property test for parallel branches — the generator and the **fake runtime**
 * shared by `parallel.test.ts`.
 *
 * The generator builds a random handler tree from a seed: `Promise.all` of a
 * few branches, each with 1–4 `ctx.run` / `ctx.call` / `ctx.sleep` steps (`ctx.run`
 * step names deliberately repeat across branches), sometimes a nested parallel group. The
 * schedule also comes from the seed (a separate RNG stream): how long each closure run
 * waits (synchronous, a few microtasks, a few `setImmediate`s, under BIDI also real timers so
 * the turn is flushed midway), which runs throw a transient error, when outside completions
 * (`Sleep`, `Call`) arrive, and under BIDI after which frame the SDK process dies.
 *
 * The fake runtime follows exactly the shell rules of `cairn-server/src/turn/sdk.rs`:
 * - SDK commands enter the journal in the order received; `ProposeRunCompletion` becomes a
 *   `RunCompletion` as soon as it is received (BIDI: with an ack);
 * - frames after the outcome are dropped;
 * - a `Suspension` already satisfied by the journal ⇒ next turn at once; otherwise wait for an
 *   outside completion (only arriving **between** two turns — the invocation lock);
 * - an `Error` retry keeps everything written before it; 570 is a violation.
 *
 * The four claims checked:
 * (i)   no `JournalMismatch`;
 * (ii)  replaying **every prefix** of the final journal emits exactly the recorded command
 *       sequence;
 * (iii) a closure with a durable result does not run again;
 * (iv)  each branch gets its own result (and the final output is right).
 *
 * Everything is deterministic per seed under `REQUEST_RESPONSE` (only microtasks and
 * `setImmediate`); BIDI uses real timers so it is only nearly deterministic — the seeds
 * pinned as separate tests are RR seeds.
 */

import {
  type Context,
  type Frame,
  type FrameStream,
  FrameSplitter,
  type Handler,
  runBidi,
  runTurn,
} from "../src/index.ts";
import { eachField, Reader, Writer } from "../src/protobuf.ts";
import {
  decodeCommandName,
  decodeFrames,
  encodeFrame,
  FLAG_REQUESTED_ACK,
  isCommand,
  isNotification,
  JOURNAL_MISMATCH,
  MT,
  typeName,
} from "../src/wire.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

// ─────────────────────────────────────────────────────────── RNG

/** mulberry32 — good enough for random generation, deterministic per seed. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(r: () => number, xs: readonly T[]): T {
  return xs[Math.floor(r() * xs.length)]!;
}

function int(r: () => number, lo: number, hi: number): number {
  return lo + Math.floor(r() * (hi - lo + 1));
}

// ─────────────────────────────────────────────────────────── handler tree

export type Step =
  | { readonly kind: "run"; readonly id: string; readonly name: string }
  | { readonly kind: "call"; readonly id: string }
  | { readonly kind: "sleep"; readonly id: string }
  | { readonly kind: "par"; readonly id: string; readonly branches: readonly (readonly Step[])[] };

export interface Tree {
  readonly branches: readonly (readonly Step[])[];
  /** A sequential step after every branch finishes — a command after the join point. */
  readonly tail: readonly Step[];
}

/** Shared step names — the same name in two branches is the "same names" case of `parallel.test.ts`. */
const SHARED_NAMES = ["fetch", "summarize", "step"] as const;

function genSteps(r: () => number, prefix: string, depth: number, min = 1, max = 4): Step[] {
  const n = int(r, min, max);
  const out: Step[] = [];
  for (let i = 0; i < n; i++) {
    const id = `${prefix}${i}`;
    const k = r();
    if (k < 0.5) {
      out.push({ kind: "run", id, name: r() < 0.6 ? pick(r, SHARED_NAMES) : `run-${id}` });
    } else if (k < 0.7) {
      out.push({ kind: "sleep", id });
    } else if (k < 0.9 || depth >= 1) {
      out.push({ kind: "call", id });
    } else {
      const m = int(r, 2, 3);
      const branches: Step[][] = [];
      for (let b = 0; b < m; b++) branches.push(genSteps(r, `${id}.${b}.`, depth + 1, 1, 2));
      out.push({ kind: "par", id, branches });
    }
  }
  return out;
}

export function genTree(seed: number): Tree {
  const r = rng(seed ^ 0x9e3779b9);
  const n = int(r, 2, 3);
  const branches: Step[][] = [];
  for (let b = 0; b < n; b++) branches.push(genSteps(r, `b${b}.`, 0));
  const tail = r() < 0.5 ? genSteps(r, "t.", 1, 1, 2) : [];
  return { branches, tail };
}

export function describeTree(t: Tree): string {
  const step = (s: Step): string => {
    switch (s.kind) {
      case "run":
        return `run(${JSON.stringify(s.name)})`;
      case "call":
        return "call";
      case "sleep":
        return "sleep";
      case "par":
        return `all[${s.branches.map((b) => b.map(step).join(" → ")).join(" | ")}]`;
    }
  };
  const branches = t.branches.map((b, i) => `  b${i}: ${b.map(step).join(" → ")}`).join("\n");
  return t.tail.length === 0 ? branches : `${branches}\n  tail: ${t.tail.map(step).join(" → ")}`;
}

/** The value the step must receive — `null` for `sleep`. */
function expectedOf(s: Step): unknown {
  switch (s.kind) {
    case "run":
      return `r:${s.id}`;
    case "call":
      return `c:${s.id}`;
    case "sleep":
      return null;
    case "par":
      return s.branches.map((b) => b.map(expectedOf));
  }
}

export function expectedOutput(t: Tree): unknown {
  return { branches: t.branches.map((b) => b.map(expectedOf)), tail: t.tail.map(expectedOf) };
}

// ─────────────────────────────────────────────────────────── schedule

export interface Schedule {
  /** Under BIDI: allow real timers (flush mid-turn), and connection cuts. */
  readonly bidi: boolean;
  readonly r: () => number;
}

/** Observation log of one run — every violation is recorded here. */
export class Ledger {
  readonly violations: string[] = [];
  /** How many times each step's closure **started** running (probe runs not counted). */
  readonly execs = new Map<string, number>();
  /** Transient errors thrown, per step. */
  readonly failures = new Map<string, number>();
  /** Steps whose `RunCompletion` is in the journal. */
  readonly durable = new Set<string>();

  violate(what: string): void {
    if (this.violations.length < 20) this.violations.push(what);
  }
}

/** Waits per the schedule: synchronous, a few microtasks, a few `setImmediate`s, or (BIDI) a timer. */
async function pause(s: Schedule): Promise<void> {
  const k = s.r();
  if (k < 0.15) return;
  if (k < 0.35) {
    const n = int(s.r, 1, 6);
    for (let i = 0; i < n; i++) await Promise.resolve();
    return;
  }
  if (!s.bidi || k < 0.85) {
    const n = int(s.r, 1, 5);
    for (let i = 0; i < n; i++) await new Promise<void>((res) => setImmediate(res));
    return;
  }
  await new Promise<void>((res) => setTimeout(res, int(s.r, 1, 4)));
}

/**
 * Builds the tree's handler. `probe` ⇒ closures return at once, uncounted (the prefix
 * replay of claim ii). `alive` ⇒ under BIDI, a closure that starts after the "process"
 * has died does not really run.
 */
export function makeHandler(
  tree: Tree,
  ledger: Ledger,
  sched: Schedule | null,
  alive: () => boolean = () => true,
): Handler {
  const closure = (s: Step & { kind: "run" }) => async (): Promise<string> => {
    if (sched === null) return `r:${s.id}`;
    if (!alive()) throw new Error("process is dead");
    ledger.execs.set(s.id, (ledger.execs.get(s.id) ?? 0) + 1);
    if (ledger.durable.has(s.id)) {
      ledger.violate(`(iii) closure of ${s.id} (${JSON.stringify(s.name)}) ran again after its result was durable`);
    }
    await pause(sched);
    const failed = ledger.failures.get(s.id) ?? 0;
    if (failed < 2 && sched.r() < 0.25) {
      ledger.failures.set(s.id, failed + 1);
      throw new Error(`transient failure of ${s.id}`);
    }
    return `r:${s.id}`;
  };

  const check = (s: Step, got: unknown): unknown => {
    const want = expectedOf(s);
    if (JSON.stringify(got) !== JSON.stringify(want)) {
      ledger.violate(`(iv) step ${s.id} got ${JSON.stringify(got)}, expected ${JSON.stringify(want)}`);
    }
    return got;
  };

  const branch = async (ctx: Context, steps: readonly Step[]): Promise<unknown[]> => {
    const out: unknown[] = [];
    for (const s of steps) {
      switch (s.kind) {
        case "run":
          out.push(check(s, await ctx.run(s.name, closure(s))));
          break;
        case "call":
          out.push(check(s, await ctx.call("Callee", "work", s.id, { name: `call-${s.id}` })));
          break;
        case "sleep":
          await ctx.sleep(1000, `sleep-${s.id}`);
          out.push(check(s, null));
          break;
        case "par":
          out.push(check(s, await Promise.all(s.branches.map((b) => branch(ctx, b)))));
          break;
      }
    }
    return out;
  };

  return async (ctx: Context) => {
    const branches = await Promise.all(tree.branches.map((b) => branch(ctx, b)));
    const tail = await branch(ctx, tree.tail);
    return { branches, tail };
  };
}

// ─────────────────────────────────────────────────────────── decoding

function field(payload: Uint8Array, want: number): bigint | null {
  let out: bigint | null = null;
  eachField(payload, (f, wire, r) => {
    if (f !== want || wire !== 0) return false;
    out = r.varint();
    return true;
  });
  return out;
}

function bytesField(payload: Uint8Array, want: number): Uint8Array | null {
  let out: Uint8Array | null = null;
  eachField(payload, (f, wire, r) => {
    if (f !== want || wire !== 2) return false;
    out = r.bytes();
    return true;
  });
  return out;
}

/** Completion id of a command (field 11 — `Run`, `Sleep`, `Call`). */
function completionOf(f: Frame): number | null {
  const v = field(f.payload, 11);
  return v === null ? null : Number(v);
}

export interface Tree0 {
  readonly all: boolean;
  readonly completions: number[];
  readonly signals: number[];
  readonly names: string[];
  readonly nested: Tree0[];
}

function packed(r: Reader, wire: number, into: number[]): void {
  if (wire === 0) {
    into.push(Number(r.varint()));
    return;
  }
  const sub = new Reader(r.bytes());
  while (!sub.eof) into.push(Number(sub.varint()));
}

export function decodeSuspension(payload: Uint8Array): Tree0 {
  const t: Tree0 = { all: false, completions: [], signals: [], names: [], nested: [] };
  let all = false;
  eachField(payload, (f, wire, r) => {
    switch (f) {
      case 1:
        packed(r, wire, t.completions);
        return true;
      case 2:
        packed(r, wire, t.signals);
        return true;
      case 3:
        t.names.push(r.string());
        return true;
      case 4:
        t.nested.push(decodeSuspension(r.bytes()));
        return true;
      case 5:
        all = r.varint() === 2n;
        return true;
      default:
        return false;
    }
  });
  return { ...t, all };
}

interface Seen {
  done(id: number): boolean;
  hasIdx(idx: number): boolean;
  hasName(name: string): boolean;
}

function satisfied(t: Tree0, seen: Seen): boolean {
  const leaves = [
    ...t.completions.map((id) => seen.done(id)),
    ...t.signals.map((i) => seen.hasIdx(i)),
    ...t.names.map((n) => seen.hasName(n)),
    ...t.nested.map((n) => satisfied(n, seen)),
  ];
  return t.all ? leaves.every(Boolean) : leaves.some(Boolean);
}

/** Whether the suspension tree is satisfied when the journal has exactly the completions `ids` (no signals). */
export function satisfiedBy(t: Tree0, ids: readonly number[]): boolean {
  return satisfied(t, { done: (id) => ids.includes(id), hasIdx: () => false, hasName: () => false });
}

export function describeSuspension(t: Tree0): string {
  const leaves = [
    ...t.completions.map(String),
    ...t.signals.map((i) => `signal ${i}`),
    ...t.names.map((n) => `signal ${JSON.stringify(n)}`),
    ...t.nested.map(describeSuspension),
  ];
  return `${t.all ? "ALL" : "FIRST"}(${leaves.join(",")})`;
}

function errorOf(payload: Uint8Array): { code: number; message: string } {
  let code = 0;
  let message = "";
  eachField(payload, (f, wire, r) => {
    if (f === 1 && wire === 0) {
      code = Number(r.varint());
      return true;
    }
    if (f === 2 && wire === 2) {
      message = r.string();
      return true;
    }
    return false;
  });
  return { code, message };
}

/** The value of `ProposeRunCompletion`: field 14 (bytes) or 15 (failure). */
function proposal(payload: Uint8Array): { id: number; value: Uint8Array | null } {
  return { id: Number(field(payload, 1) ?? 0n), value: bytesField(payload, 14) };
}

/** Command summary for comparison (claim ii): kind, name, completion id. */
function commandKey(f: Frame): string {
  if (f.type === MT.OUTPUT) return "OUTPUT";
  return `${typeName(f.type)} ${JSON.stringify(decodeCommandName(f.payload))} #${completionOf(f) ?? "-"}`;
}

// ─────────────────────────────────────────────────────────── journal

export function hexOf(b: Uint8Array | null): string {
  return b === null ? "" : Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}

export function startFrame(knownEntries: number, retry: number, keepaliveMs = 0): Uint8Array {
  const w = new Writer()
    .bytes(1, Uint8Array.from([0xab, 0xcd]))
    .uint32(3, knownEntries)
    .uint32(7, retry)
    .uint64(9, 42n);
  if (keepaliveMs > 0) w.uint32(16, keepaliveMs);
  return encodeFrame(MT.START, 0, w.finish());
}

export function frame(type: number, payload: Uint8Array): Frame {
  return { type, flags: 0, payload };
}

export function inputFrame(): Frame {
  return frame(MT.INPUT, new Writer().message(14, new Writer().bytes(1, enc.encode("null"))).finish());
}

function valueNotification(type: number, id: number, value: Uint8Array): Frame {
  return frame(type, new Writer().uint32(1, id).message(5, new Writer().bytes(1, value)).finish());
}

function voidNotification(type: number, id: number): Frame {
  return frame(type, new Writer().uint32(1, id).message(4, new Writer()).finish());
}

export function onWire(frames: readonly Frame[]): Uint8Array {
  const parts = frames.map((f) => encodeFrame(f.type, f.flags, f.payload));
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

/**
 * The runtime-side journal and the outside completions pending. Applies SDK frames by
 * the state machine's rules (the part we need).
 */
export class Runtime {
  readonly journal: Frame[] = [inputFrame()];
  /** completion id ⇒ the notification to write when "the outside world" answers. */
  readonly external = new Map<string, Frame>();
  private readonly idxSeen = new Set<number>();
  /** Object state (key as hex of its wire bytes ⇒ value bytes). */
  private readonly state = new Map<string, Uint8Array>();
  private readonly namesSeen = new Set<string>();
  /** completion ids declared by commands. */
  private readonly declared = new Set<number>();
  /** Journaled Runs that have no completion yet: id ⇒ name. */
  private readonly openRuns = new Map<number, string>();
  private readonly settled = new Set<number>();
  outcome: "suspended" | "retry" | "done" | "paused" | null = null;
  suspension: Tree0 | null = null;
  output: unknown = undefined;
  appended = 0;

  readonly ledger: Ledger;

  constructor(ledger: Ledger) {
    this.ledger = ledger;
  }

  done(id: number): boolean {
    return this.settled.has(id);
  }

  /** One SDK frame. Returns `false` when the frame is dropped (after the outcome). */
  apply(f: Frame): void {
    if (this.outcome !== null && this.outcome !== "done") return;
    if (this.outcome === "done") {
      if (f.type !== MT.END) return;
      return;
    }
    if (f.type === MT.KEEPALIVE || f.type === MT.STREAM_APPEND || f.type === MT.ARTIFACT_PUT) return;
    if (isCommand(f.type)) {
      this.journal.push(frame(f.type, f.payload));
      this.appended++;
      const id = completionOf(f);
      this.onCommand?.(f, id);
      if (id !== null && f.type !== MT.OUTPUT) {
        if (this.declared.has(id)) this.ledger.violate(`571: completion id ${id} declared twice (${commandKey(f)})`);
        this.declared.add(id);
      }
      switch (f.type) {
        case MT.RUN:
          this.openRuns.set(id!, decodeCommandName(f.payload));
          break;
        case MT.SLEEP:
          this.external.set(`c${id}`, voidNotification(MT.SLEEP_COMPLETION, id!));
          break;
        case MT.CALL: {
          const param = JSON.parse(dec.decode(bytesField(f.payload, 3) ?? enc.encode("null"))) as string;
          this.external.set(
            `c${id}`,
            valueNotification(MT.CALL_COMPLETION, id!, enc.encode(JSON.stringify(`c:${param}`))),
          );
          break;
        }
        case MT.SET_STATE: {
          const v = bytesField(f.payload, 3);
          this.state.set(hexOf(bytesField(f.payload, 1)), (v === null ? null : bytesField(v, 1)) ?? new Uint8Array());
          break;
        }
        case MT.CLEAR_STATE:
          this.state.delete(hexOf(bytesField(f.payload, 1)));
          break;
        case MT.GET_STATE: {
          // The runtime answers `GetState` within the turn (the notification is written after the
          // command).
          const v = this.state.get(hexOf(bytesField(f.payload, 1)));
          this.journal.push(
            v === undefined ? voidNotification(MT.GET_STATE_COMPLETION, id!) : valueNotification(MT.GET_STATE_COMPLETION, id!, v),
          );
          this.appended++;
          this.settled.add(id!);
          break;
        }
        case MT.OUTPUT: {
          const v = bytesField(f.payload, 14);
          const inner = v === null ? null : bytesField(v, 1);
          this.output = inner === null ? undefined : JSON.parse(dec.decode(inner));
          this.outcome = "done";
          break;
        }
      }
      return;
    }
    switch (f.type) {
      case MT.PROPOSE_RUN_COMPLETION: {
        const p = proposal(f.payload);
        if (!this.openRuns.has(p.id)) {
          this.ledger.violate(`571: proposal for completion ${p.id} that is not an open Run`);
          return;
        }
        this.openRuns.delete(p.id);
        const failure = bytesField(f.payload, 15);
        if (failure !== null) {
          // A failed result (`RunFailure`, `TerminalError`): durable like any result.
          this.journal.push(frame(MT.RUN_COMPLETION, new Writer().uint32(1, p.id).bytes(6, failure).finish()));
          this.appended++;
          this.settled.add(p.id);
          return;
        }
        const value = p.value ?? enc.encode("null");
        this.journal.push(valueNotification(MT.RUN_COMPLETION, p.id, value));
        this.appended++;
        this.settled.add(p.id);
        const v = JSON.parse(dec.decode(value)) as unknown;
        if (typeof v === "string" && v.startsWith("r:")) this.ledger.durable.add(v.slice(2));
        return;
      }
      case MT.SUSPENSION:
        this.suspension = decodeSuspension(f.payload);
        this.outcome = "suspended";
        return;
      case MT.ERROR: {
        const e = errorOf(f.payload);
        if (e.code === JOURNAL_MISMATCH) {
          this.ledger.violate(`(i) 570: ${e.message}`);
          this.outcome = "paused";
        } else {
          this.outcome = "retry";
        }
        return;
      }
      case MT.END:
        // `Output` was already durable in the previous turn (died between `Output` and `End`): the
        // SDK only resends `End`.
        if (this.journal.some((e) => e.type === MT.OUTPUT)) this.outcome = "done";
        else this.ledger.violate("End without Output");
        return;
      default:
        this.ledger.violate(`unexpected frame ${typeName(f.type)} from the SDK`);
    }
  }

  /** One outside completion arrives (between two turns). */
  deliverExternal(r: () => number): boolean {
    if (this.external.size === 0) return false;
    const key = pick(r, [...this.external.keys()]);
    const n = this.external.get(key)!;
    this.external.delete(key);
    this.journal.push(n);
    if (n.type === MT.SIGNAL) {
      const idx = field(n.payload, 2);
      if (idx !== null) this.idxSeen.add(Number(idx));
      const name = bytesField(n.payload, 3);
      if (name !== null) this.namesSeen.add(dec.decode(name));
    } else {
      this.settled.add(Number(key.slice(1)));
    }
    return true;
  }

  /**
   * Called with each SDK command just written (and its completion id) — scenario tests
   * answer commands the fake runtime does not answer itself (promise, attach…) with
   * [`queueCompletion`].
   */
  onCommand: ((f: Frame, id: number | null) => void) | null = null;

  /** A completion that will arrive as an outside completion (between two turns). */
  queueCompletion(id: number, f: Frame): void {
    this.external.set(`c${id}`, f);
  }

  /** A numbered signal that will arrive as an outside completion (between two turns). */
  queueSignal(idx: number, value: unknown): void {
    const payload = new Writer().uint32(2, idx).message(5, new Writer().bytes(1, enc.encode(JSON.stringify(value))));
    this.external.set(`s${idx}`, frame(MT.SIGNAL, payload.finish()));
  }

  /** The "is this leaf in the journal yet" query for the suspension tree. */
  get seen(): Seen {
    return {
      done: (id) => this.settled.has(id),
      hasIdx: (i) => this.idxSeen.has(i),
      hasName: (n) => this.namesSeen.has(n),
    };
  }

  /** The turn just finished suspended: wait (delivering outside completions) until the tree is satisfied. */
  waitForSuspension(r: () => number): boolean {
    const s = this.suspension!;
    while (!satisfied(s, this.seen)) {
      if (!this.deliverExternal(r)) {
        this.ledger.violate(`stuck: suspension ${describeSuspension(s)} can never be satisfied`);
        return false;
      }
    }
    return true;
  }

  beginTurn(): void {
    this.outcome = null;
    this.suspension = null;
    this.appended = 0;
  }
}

// ─────────────────────────────────────────────────────────── running

export interface Run {
  readonly ledger: Ledger;
  readonly journal: readonly Frame[];
  readonly turns: number;
  readonly completed: boolean;
}

const MAX_TURNS = 300;
/** Time limit of one turn — exceeding it means a hang (both sides waiting on each other). */
const TURN_TIMEOUT_MS = 10_000;

async function within<T>(p: Promise<T>, what: string, ledger: Ledger): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<undefined>((res) => {
    timer = setTimeout(() => {
      ledger.violate(`hang: ${what} did not finish within ${TURN_TIMEOUT_MS} ms`);
      res(undefined);
    }, TURN_TIMEOUT_MS);
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/** One whole invocation under `REQUEST_RESPONSE`. */
export async function simulateRR(seed: number, tree: Tree = genTree(seed)): Promise<Run> {
  const ledger = new Ledger();
  const sched: Schedule = { bidi: false, r: rng(seed) };
  const handler = makeHandler(tree, ledger, sched);
  const d = await driveRR(handler, ledger, sched.r);
  if (d.completed) return finish(ledger, d.runtime, tree, d.turns);
  return { ledger, journal: d.runtime.journal, turns: d.turns, completed: false };
}

/**
 * The fake runtime's `REQUEST_RESPONSE` turn loop for any handler — scenario tests
 * (the pinned counterexamples) use it directly. `r` decides when outside completions arrive.
 */
export async function driveRR(
  handler: Handler,
  ledger: Ledger,
  r: () => number = () => 0.99,
  rt: Runtime = new Runtime(ledger),
): Promise<{ runtime: Runtime; turns: number; completed: boolean; output: unknown }> {
  let retry = 0;
  let turns = 0;
  for (; turns < MAX_TURNS; turns++) {
    // Outside completions arrive at any time between two turns.
    while (r() < 0.3 && rt.deliverExternal(r)) {}
    rt.beginTurn();
    const body = onWire([...rt.journal]);
    const start = startFrame(rt.journal.length, retry);
    const merged = new Uint8Array(start.length + body.length);
    merged.set(start, 0);
    merged.set(body, start.length);
    const reply = await within(runTurn(handler, merged, 4), "REQUEST_RESPONSE turn", ledger);
    if (reply === undefined) break;
    for (const f of decodeFrames(reply)) rt.apply(f);
    if (ledger.violations.length > 0) break;
    if (rt.outcome === "done") return { runtime: rt, turns: turns + 1, completed: true, output: rt.output };
    if (rt.outcome === "paused") break;
    if (rt.outcome === "retry") {
      retry = rt.appended > 0 ? 1 : retry + 1;
      continue;
    }
    if (rt.outcome === "suspended") {
      retry = 0;
      if (!rt.waitForSuspension(r)) break;
      continue;
    }
    ledger.violate("turn ended without Output+End, Suspension or Error");
    break;
  }
  if (turns >= MAX_TURNS) ledger.violate(`no outcome after ${MAX_TURNS} turns`);
  return { runtime: rt, turns, completed: false, output: undefined };
}

function finish(ledger: Ledger, rt: Runtime, tree: Tree, turns: number): Run {
  const want = expectedOutput(tree);
  if (JSON.stringify(rt.output) !== JSON.stringify(want)) {
    ledger.violate(`(iv) output ${JSON.stringify(rt.output)}, expected ${JSON.stringify(want)}`);
  }
  return { ledger, journal: rt.journal, turns, completed: true };
}

/**
 * The other end of the BIDI stream — the fake runtime. Every SDK frame written is applied at once;
 * after `cutAfter` frames "the SDK process dies": the rest does not arrive, the stream closes.
 */
class BidiConn implements FrameStream {
  alive = true;
  ended = false;
  private written = 0;
  private readonly inbox: Frame[] = [];
  private waiter: ((f: Frame | null) => void) | null = null;
  private closed = false;
  readonly finished: Promise<void>;
  private finish!: () => void;

  private readonly rt: Runtime;
  private readonly cutAfter: number;

  constructor(rt: Runtime, cutAfter: number) {
    this.rt = rt;
    this.cutAfter = cutAfter;
    this.finished = new Promise((res) => {
      this.finish = res;
    });
  }

  push(f: Frame): void {
    if (this.closed) return;
    const w = this.waiter;
    if (w !== null) {
      this.waiter = null;
      w(f);
    } else {
      this.inbox.push(f);
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const w = this.waiter;
    if (w !== null && this.inbox.length === 0) {
      this.waiter = null;
      w(null);
    }
    this.finish();
  }

  next(): Promise<Frame | null> {
    const head = this.inbox.shift();
    if (head !== undefined) return Promise.resolve(head);
    if (this.closed) return Promise.resolve(null);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }

  buffered(): Frame[] {
    return this.inbox.splice(0, this.inbox.length);
  }

  write(bytes: Uint8Array): void {
    if (!this.alive) return;
    for (const f of new FrameSplitter().push(bytes)) {
      if (!this.alive) return;
      this.written++;
      if (this.written > this.cutAfter) {
        this.alive = false;
        this.close();
        return;
      }
      this.rt.apply(f);
      if (f.type === MT.PROPOSE_RUN_COMPLETION && (f.flags & FLAG_REQUESTED_ACK) !== 0) {
        const id = proposal(f.payload).id;
        if (this.rt.done(id)) {
          setImmediate(() =>
            this.push({
              type: MT.PROPOSE_RUN_COMPLETION_ACK,
              flags: 0,
              payload: new Writer().uint32(1, id).finish(),
            }),
          );
        }
      }
      const o = this.rt.outcome;
      if (o === "suspended") {
        // Satisfied on the spot ⇒ keep the stream (the SDK reruns by itself after the ack);
        // otherwise close.
        if (satisfied(this.rt.suspension!, this.rt.seen)) {
          this.rt.beginTurn();
        } else {
          this.close();
        }
      } else if (o !== null && o !== "done") {
        this.close();
      }
    }
  }

  end(): void {
    this.ended = true;
    this.close();
  }
}

/** One whole invocation under `BIDI_STREAM`, with connection cuts midway. */
export async function simulateBidi(seed: number, tree: Tree = genTree(seed)): Promise<Run> {
  const ledger = new Ledger();
  const sched: Schedule = { bidi: true, r: rng(seed) };
  const d = await driveBidi((alive) => makeHandler(tree, ledger, sched, alive), ledger, sched.r, true);
  if (d.completed) return finish(ledger, d.runtime, tree, d.turns);
  return { ledger, journal: d.runtime.journal, turns: d.turns, completed: false };
}

/**
 * The fake runtime's `BIDI_STREAM` connection loop. `handlerFor(alive)` builds the handler
 * for one connection (a closure that starts after the process "died" does not really
 * run). `cuts` ⇒ the SDK process dies after a random number of frames.
 */
export async function driveBidi(
  handlerFor: (alive: () => boolean) => Handler,
  ledger: Ledger,
  r: () => number = () => 0.99,
  cuts = false,
  rt: Runtime = new Runtime(ledger),
): Promise<{ runtime: Runtime; turns: number; completed: boolean; output: unknown; connections: number }> {
  let retry = 0;
  let turns = 0;
  for (; turns < MAX_TURNS; turns++) {
    while (r() < 0.3 && rt.deliverExternal(r)) {}
    rt.beginTurn();
    const cutAfter = cuts && r() < 0.35 ? int(r, 1, 12) : Number.POSITIVE_INFINITY;
    const conn = new BidiConn(rt, cutAfter);
    const handler = handlerFor(() => conn.alive);
    const keepaliveMs = r() < 0.7 ? 1 : 0;
    conn.push(decodeFrames(startFrame(rt.journal.length, retry, keepaliveMs))[0]!);
    for (const f of rt.journal) conn.push(f);
    const sdk = runBidi(handler, conn, 4);
    await within(conn.finished, "BIDI stream", ledger);
    if (conn.alive && ledger.violations.length === 0) await within(sdk, "BIDI SDK", ledger);
    if (ledger.violations.length > 0) break;
    const end = { runtime: rt, turns: turns + 1, connections: turns + 1 };
    if (!conn.alive) {
      // The process dies: the journal keeps what was applied. The next turn is a new connection.
      retry++;
      continue;
    }
    if (rt.outcome === "done") return { ...end, completed: true, output: rt.output };
    if (rt.outcome === "paused") break;
    if (rt.outcome === "retry") {
      retry = rt.appended > 0 ? 1 : retry + 1;
      continue;
    }
    if (rt.outcome === "suspended") {
      retry = 0;
      if (!rt.waitForSuspension(r)) break;
      continue;
    }
    ledger.violate("stream ended without Output+End, Suspension or Error");
    break;
  }
  if (turns >= MAX_TURNS) ledger.violate(`no outcome after ${MAX_TURNS} turns`);
  return { runtime: rt, turns, completed: false, output: undefined, connections: turns };
}

/**
 * Claim (ii): replay on **every prefix** of the final journal (closures return at
 * once, uncounted) — no 570, and every emitted command matches exactly the command at the
 * same position of the final journal.
 */
export async function checkPrefixes(tree: Tree, journal: readonly Frame[], ledger: Ledger): Promise<void> {
  const commands = journal.filter((f) => isCommand(f.type) && f.type !== MT.INPUT);
  const probe = makeHandler(tree, new Ledger(), null);
  for (let k = 1; k <= journal.length; k++) {
    const prefix = journal.slice(0, k);
    const known = prefix.filter((f) => isCommand(f.type) && f.type !== MT.INPUT).length;
    const body = onWire(prefix);
    const start = startFrame(prefix.length, 0);
    const merged = new Uint8Array(start.length + body.length);
    merged.set(start, 0);
    merged.set(body, start.length);
    const raw = await within(runTurn(probe, merged, 4), `replay of prefix ${k}`, ledger);
    if (raw === undefined) return;
    const out = decodeFrames(raw);
    let at = known;
    for (const f of out) {
      if (f.type === MT.ERROR && errorOf(f.payload).code === JOURNAL_MISMATCH) {
        ledger.violate(`(ii) replay of prefix ${k}/${journal.length}: 570 ${errorOf(f.payload).message}`);
        return;
      }
      if (!isCommand(f.type)) continue;
      const want = commands[at];
      if (want === undefined || commandKey(want) !== commandKey(f)) {
        ledger.violate(
          `(ii) replay of prefix ${k}/${journal.length} emitted ${commandKey(f)} at command ${at}, ` +
            `the journal has ${want === undefined ? "nothing" : commandKey(want)}`,
        );
        return;
      }
      at++;
    }
  }
}

/** One whole seed: run, then check every prefix. */
export async function checkSeed(seed: number, mode: "rr" | "bidi"): Promise<{ run: Run; tree: Tree }> {
  const tree = genTree(seed);
  const run = mode === "rr" ? await simulateRR(seed, tree) : await simulateBidi(seed, tree);
  if (run.completed && run.ledger.violations.length === 0) await checkPrefixes(tree, run.journal, run.ledger);
  return { run, tree };
}

/** Journal for the reader — to print a counterexample. */
export function describeJournal(journal: readonly Frame[]): string {
  return journal
    .map((f, i) => {
      if (isNotification(f.type)) {
        const id = Number(field(f.payload, 1) ?? 0n);
        return `${i}: ${typeName(f.type)} #${id}`;
      }
      return `${i}: ${commandKey(f)}`;
    })
    .join("\n");
}
