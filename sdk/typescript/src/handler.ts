// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * The SDK-side state machine — sections 6 and 8 of `docs/protocol-v1.md`.
 *
 * A **turn** = one run of user code from the start, each command matching the next
 * entry in the journal. No match ⇒ 570.
 *
 * The user must NOT see the replay/processing boundary (protocol-v1 §6) — and they
 * really do not, because the code runs identically in both phases. The only
 * difference is that commands already in the journal are not emitted again, and
 * values come from the journal instead of from the outside world.
 *
 * The two transports **share** [`runOnce`], and that is deliberate:
 * `REQUEST_RESPONSE` ([`runTurn`]) calls it once per HTTP request, then the runtime
 * resends the whole journal on the next turn. `BIDI_STREAM` ([`runBidi`]) calls it
 * again each time a notification arrives on the open stream, on a **local** journal
 * the SDK keeps itself — the runtime resends nothing, and that is exactly the saving.
 * One kernel means there is no separate "bidi path" whose semantics could drift.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { TerminalError, wireCode } from "./errors.ts";
import {
  type KnownValue,
  readAtMost,
  reusablePrefix,
  SegmentedValueError,
  Segments,
  type ValueOptions,
  WIRE_SEGMENTED,
} from "./values.ts";
// `import type` is required, not style: Node's strip-only mode erases types without
// knowing which names are types, so a type imported with a plain `import` would become
// a runtime reference to an export that does not exist.
import type {
  CallPlacement,
  CompletionResult,
  Frame,
  SegmentRef,
  SignalNotification,
  StartInfo,
  StreamRecordWire,
  SuspensionSpec,
  SuspensionTree,
} from "./wire.ts";
import {
  encodeAttachInvocation,
  encodeCall,
  encodeClearAllState,
  encodeClearState,
  encodeCompletePromise,
  encodeEnd,
  encodeError,
  encodeReplayReport,
  encodeContinueAsNew,
  encodeStreamAppend,
  encodeArtifactPut,
  encodeFrame,
  encodeGetEagerState,
  encodeGetEagerStateKeys,
  encodeGetInvocationOutput,
  encodeGetPromise,
  encodeGetState,
  encodeGetStateKeys,
  encodeOutput,
  encodeOneWayCall,
  encodeOutputFailure,
  encodePeekPromise,
  encodeProposeRunCompletion,
  encodeProposeRunFailure,
  encodeRun,
  encodeRunCompletion,
  encodeRunCompletionFailure,
  encodeSetState,
  encodeSleep,
  encodeSuspensionSpec,
  suspensionTree,
  encodeSendSignal,
  decodeGetEagerStateValue,
  decodeGetEagerStateKeys,
  decodeSegmentUrls,
  decodeProposeRunAck,
  decodeSignal,
  CANCEL_SIGNAL,
  ERROR_FAIL,
  ERROR_PAUSE,
  ERROR_RETRY,
  FLAG_REQUESTED_ACK,
  JOURNAL_MISMATCH,
  MAX_FRAME_LEN,
  MT,
  concatFrames,
  decodeFrames,
  decodeInput,
  decodeInputHeaders,
  decodeNotification,
  decodeStart,
  isCommand,
  isNotification,
  typeName,
  decodeCommandName,
  commandDiff,
} from "./wire.ts";

/**
 * The turn has suspended. A wait on something not yet available **no longer** throws
 * this — the promise stays pending for the rest of the turn; only a ctx command called
 * *after* the turn has suspended throws (user code still running after the outcome,
 * e.g. waiting on something outside the SDK).
 */
export class Suspend extends Error {
  readonly spec: SuspensionSpec;

  constructor(spec: SuspensionSpec) {
    const t = suspensionTree(spec);
    const what = [
      ...(t.completions.length > 0 ? [`completion ${t.completions.join(", ")}`] : []),
      ...(t.signals.length + t.names.length > 0 ? [`signal ${[...t.signals, ...t.names].join(", ")}`] : []),
    ];
    super(`waiting for ${t.combinator === "all" ? "all of" : "one of"}: ${what.join("; ")}`);
    this.name = "Suspend";
    this.spec = spec;
  }
}

/** The journal does not match the current code (protocol-v1 §8.5). Never retried. */
export class JournalMismatch extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JournalMismatch";
  }
}

/**
 * A `replay_only` turn reached the first command **outside** the journal: stop here —
 * before any closure runs. `at` = number of commands matched.
 */
class ReplayStop extends Error {
  readonly at: number;
  constructor(at: number) {
    super(`replay stopped at command ${at}`);
    this.name = "ReplayStop";
    this.at = at;
  }
}

/** `ctx.continueAsNew(input)` — thrown to end the turn. */
class ContinueAsNew extends Error {
  readonly input: unknown;
  constructor(input: unknown) {
    super("continueAsNew");
    this.name = "ContinueAsNew";
    this.input = input;
  }
}

/** Business error: ends the invocation, NO retry — in `errors.ts`, shared with the client. */
export { TerminalError };

/**
 * Suspends the invocation, **waiting for a human** — protocol-v1 §8.5.
 *
 * A third path, between the two existing ones and different from both where it
 * matters most:
 *
 * - An ordinary error ⇒ retried forever with backoff. Right for transient failures,
 *   wrong for things that will not fix themselves: an exhausted quota, missing stock
 *   in the warehouse, a signature that is needed.
 * - [`TerminalError`] ⇒ dead for good. Right when there is nothing left to save, but it
 *   **throws away** the work already done — even after the operator fixes things, it
 *   cannot continue.
 * - `PauseError` ⇒ stop, keep the journal as is, **wait**. The operator fixes the cause,
 *   then `POST /invocations/{id}/resume`, and the invocation continues from exactly
 *   where it stopped.
 *
 * ```ts
 * const stock = await ctx.run("read stock", () => inventory.count(sku));
 * if (stock < input.quantity) {
 *   throw new PauseError(`short of ${input.quantity - stock} ${sku} — restock needed`);
 * }
 * ```
 *
 * A paused invocation still **holds its place in the `EXCLUSIVE` queue**, and that is
 * deliberate: releasing the lock would let another invocation change the state it relies on.
 */
export class PauseError extends Error {
  readonly code: number;

  constructor(message: string, code = 500) {
    super(message);
    this.name = "PauseError";
    this.code = wireCode(code);
  }
}

/**
 * A ctx command after the closure of a `ctx.run` threw a transient error: that turn
 * is already dead, its outcome is Error RETRY whatever the code does next. It is not an
 * `APICallError`, so the AI SDK does not retry it itself.
 */
export class TurnFailed extends Error {
  constructor(cause: unknown) {
    super(`turn failed on a retryable ctx.run error: ${cause instanceof Error ? cause.message : String(cause)}`, {
      cause,
    });
    this.name = "TurnFailed";
  }
}

/** A `ctx.run` call that failed in a way that should not be retried. */
export class RunFailure extends Error {
  readonly code: number;

  constructor(message: string, code = 500) {
    super(message);
    this.name = "RunFailure";
    this.code = wireCode(code);
  }
}

/** The error of every cancellation point — code 409, as in Restate. */
function cancelledError(): TerminalError {
  return new TerminalError("invocation was cancelled (CANCEL signal)", 409);
}

/** A wait point of user code — see [`Turn.wait`]. */
interface Waiter {
  /** Every leaf involved (`c<id>`, `i<idx>`, `n<name>`). */
  readonly keys: readonly string[];
  /** The tree contributed to `Suspension` if it is still pending at the end of the turn. */
  readonly mode: "first" | "all";
  /** Tries to settle (resolve or reject); `true` when done. */
  readonly check: () => boolean;
}

const noop = (): void => {};

class Turn {
  /** Stored command entries (whole frames), **not** counting the runtime's `Input`. */
  private readonly journaledCommands: Frame[] = [];
  private readonly completions = new Map<number, CompletionResult>();
  /** Signals received, in journal order. */
  readonly signals: SignalNotification[] = [];
  /**
   * Journal position of each leaf's **first** appearance — completion notifications
   * (`c<id>`) and signals (`i<idx>` / `n<name>`) on **one** sequence, so `ctx.race`
   * picks the winner among mixed leaves in exactly the recorded order (CG-3).
   */
  private readonly positions = new Map<string, number>();
  /**
   * Delivery schedule: leaves in journal order, each element being the leaves at one
   * position (a signal has both a number and a name).
   *
   * **The command sequence must be a function of the journal.** A result already in
   * the journal is not handed to user code the moment it asks — it is *delivered*, one
   * by one, in exactly the recorded order, with a full drain ([`drive`]) between two
   * deliveries. The previous version answered at once: `ctx.run` knew it woke after one
   * microtask, `ctx.sleep` after three, a real closure ran after an event-loop turn — so
   * in a live turn sibling branches went ahead while a closure was still running, while
   * in a replay turn that closure was already a known result and its branch woke
   * earlier: commands swapped places, a 570, or worse, a branch receiving the other
   * branch's result.
   */
  private readonly deliveries: string[][] = [];
  private nextDelivery = 0;
  /** Delivered leaves — user code may see their results. */
  private readonly delivered = new Set<string>();
  /** Pending waits, by the undelivered leaf they need. */
  private readonly waitersByKey = new Map<string, Set<Waiter>>();
  /** Every pending wait — becomes the `Suspension` tree at the end of the turn. */
  private readonly pending = new Set<Waiter>();
  /**
   * The SDK's async work **outside** closures (`ctx.artifact.get` downloads): the drain
   * waits for it to finish, because user code wakes after it — on replay it may finish
   * at once (cache), and the wake order must be the same.
   */
  private readonly busy = new Set<Promise<unknown>>();
  /** Wakes [`drive`] when it is waiting on user code that awaits something outside the SDK. */
  private poked: (() => void) | null = null;
  /**
   * A CANCEL signal was received. From here on, every wait point (`demand`/`suspend`)
   * throws a 409 `TerminalError` instead of suspending — user code can catch it to clean
   * up (non-durable: `finally`, closing connections...), then the invocation ends as
   * failed. Completions ALREADY in the journal still return normally, so the code
   * before the cancellation point replays identically.
   */
  cancelled = false;
  /**
   * Signals of the **cancellable** `ctx.run` closures currently running — a CANCEL
   * arriving mid-turn ([`cancel`], `BIDI_STREAM`) aborts them.
   */
  readonly running = new Set<AbortController>();
  /**
   * Called when a cancellable closure starts — only from here does `runBidi` read the
   * wire while the turn runs, so a turn without such a closure leaves no read pending
   * after it ends.
   */
  onCancellable: (() => void) | null = null;
  private cursor = 0;
  private nextId = 1;
  /** Frames to send to the runtime, in emission order. */
  readonly out: Frame[] = [];
  /**
   * `ctx.run` results proposed but **not yet confirmed durable**.
   *
   * The key is the completion id, the value the pre-built `RunCompletion` entry. It sits
   * here — not in the local journal — until the runtime sends
   * `ProposeRunCompletionAck`, because only the ack means "written". Putting it in the
   * journal earlier would let the next rerun *trust* a result that may exist nowhere,
   * and if the process dies midway the side effect runs again.
   */
  readonly pendingRuns = new Map<number, Frame>();
  /**
   * `ctx.run` closures of this turn still running (until their proposal is done).
   *
   * The turn **does not end** while anything is here ([`drive`], [`settle`]). Ending the
   * turn early abandons real work — the next turn's replay meets a `Run` with no
   * completion and reruns the closure, while the old closure proposes into a finished
   * turn. Waiting for it makes its result go before the Suspension, in the same turn.
   */
  private readonly inflight = new Set<Promise<unknown>>();
  /** The turn's suspension tree — set when the turn is quiescent ([`drive`]) or still has non-durable proposals. */
  suspendedOn: SuspensionSpec | null = null;
  /**
   * A transient error of a `ctx.run` closure.
   *
   * The result is not proposed, so that the retry reruns the side effect — but the retry
   * is only right if **no step after** that closure made it into the journal: it reruns
   * the closure (a `Run` without completion) at the old position, and the new result
   * (e.g. a success) must be what later steps rely on. User code that swallows the error
   * and carries on — `streamText` treats it as a tool error, and retries the provider
   * itself with a new `ctx.run` — leaves a journal with steps built on the error, then
   * steps out of place on the next attempt. So the error wins like the suspended flag:
   * later ctx commands throw {@link TurnFailed}, records outside the closure are
   * dropped, the outcome is Error RETRY.
   */
  failedWith: unknown = null;
  /**
   * The code currently running is the replay of a finished run: the **next** command is
   * already in the journal. Never inside a running `ctx.run` closure — it only runs when
   * its `Run` has no result yet, i.e. real work, even when its synchronous part runs while
   * the cursor still stands before another journaled command (`Promise.all` rerun after
   * dying between two closures).
   */
  private replaying(): boolean {
    if (this.replayOnly) return true;
    return closureScope.getStore() === undefined && this.cursor < this.journaledCommands.length;
  }
  /** The turn already has an outcome; what keeps running outside closures is a ghost — see [`appendWith`]. */
  private dead(): boolean {
    return this.suspendedOn !== null || this.failedWith !== null || this.mismatch !== null;
  }
  mismatch: string | null = null;
  /**
   * The **mid-turn** write path — only in `BIDI_STREAM` when the runtime speaks v3 and
   * sets a keepalive interval. Without it every frame waits for the end of the turn as before.
   */
  live: { write(frames: readonly Frame[]): void; keepaliveMs: number } | null = null;
  /** Number of leading frames of `out` already on the wire via `live` — runBidi only sends the rest. */
  flushed = 0;
  /** A trial replay turn: a command outside the journal ⇒ [`ReplayStop`]. */
  replayOnly = false;
  /**
   * Set `REQUESTED_ACK` on `ProposeRunCompletion` — only in `BIDI_STREAM`.
   * protocol-v1 §4: in `REQUEST_RESPONSE` no message comes from the runtime after the
   * request starts, so the SDK MUST NOT ask for an ack; the turn ends with a suspension
   * and the next call carries the `RunCompletion` in its replayed part.
   */
  requestAck = true;
  /** Where it diverged, structured — for the replay report. */
  mismatchAt: { at: number; expected: string; got: string } | null = null;
  /**
   * The **mid-turn** write path for the `StreamAppend` side channel: present in
   * `BIDI_STREAM` when the runtime speaks v4, independent of keepalive.
   * Absent ⇒ records sit in `out` and go with the response body (RR: applied at the
   * end of the turn).
   */
  wire: ((frames: readonly Frame[]) => void) | null = null;
  /** The settled wire version — `ctx.stream` only sends from v4. */
  wireVersion = 0;
  /** `retry_count_since_last_stored_entry` of `Start` — the records' `attempt`. */
  attempt = 0;
  /**
   * Minimum level of `ctx.log`: the invocation's `cairnflow-log-level` header, then the
   * handler's `logLevel` option, then `info`. Below the level ⇒ dropped at once, never
   * put on the wire.
   */
  minLogLevel: LogLevel = "info";
  /** The invocation's schedule time (`cairnflow-schedule` header), if any. */
  schedule: ScheduleMark | undefined = undefined;
  /** Appended records not yet on the wire, by stream. */
  private readonly pendingStreams = new Map<
    string,
    { durable: boolean; records: StreamRecordWire[]; close: boolean }
  >();
  /** Next `k` of each stream, per step (`${step}:${name}`). */
  private readonly streamK = new Map<string, number>();
  private pendingStreamBytes = 0;
  private streamTimer: ReturnType<typeof setTimeout> | null = null;
  private warnedStreams = false;

  /** Assembled copies of segmented values (wire v6) — `null` when the turn has none. */
  private readonly segments: Segments | null;

  constructor(entries: readonly Frame[], segments: Segments | null = null) {
    this.segments = segments;
    let lastAt = -1;
    const seen = (key: string, at: number) => {
      if (this.positions.has(key)) return;
      this.positions.set(key, at);
      if (at === lastAt) this.deliveries.at(-1)!.push(key);
      else this.deliveries.push([key]);
      lastAt = at;
    };
    entries.forEach((e, at) => {
      if (e.type === MT.INPUT) return; // the runtime's entry, not user code's
      if (e.type === MT.SIGNAL) {
        // Signals are identified by idx/name, not by completion id — putting them in
        // the completion map would match them in the wrong place (they have no field 1,
        // and every signal would overwrite the others at id 0).
        const s = decodeSignal(e.payload);
        this.signals.push(s);
        if (s.idx !== undefined) seen(`i${s.idx}`, at);
        if (s.name !== undefined) seen(`n${s.name}`, at);
        if (s.idx === CANCEL_SIGNAL) this.cancelled = true;
        return;
      }
      if (isCommand(e.type)) {
        this.journaledCommands.push(e);
      } else {
        const n = decodeNotification(e.payload);
        this.completions.set(n.completionId, this.assembledResult(e, n.result));
        seen(`c${n.completionId}`, at);
      }
    });
  }

  /**
   * The assembled result of a notification: segmented values (wire v6) replaced by the
   * whole bytes [`Segments.admit`] assembled before the turn ran. Not assembled ⇒ a turn
   * error when the code reads it — never read the tail as the whole value.
   */
  private assembledResult(frame: Frame, r: CompletionResult): CompletionResult {
    if (r.kind !== "segmented") return r;
    const a = this.segments?.of(frame);
    // Not assembled (the runtime sent refs to an SDK that has not settled on v6?) ⇒
    // leave it as is: [`demand`] throws a transient error, never hands the tail to user code.
    if (a === undefined) return r;
    return { kind: "value", value: a.bytes, refs: a.refs };
  }

  /**
   * The value embedded in a journaled `GetEagerState` entry — the assembled copy if it
   * is segmented (wire v6), with refs.
   */
  eagerValue(frame: Frame): { value: Uint8Array | null; refs?: readonly SegmentRef[] } {
    const v = decodeGetEagerStateValue(frame.payload);
    if (v === null) return { value: null };
    if (v.segments.length === 0) return { value: v.content };
    const a = this.segments?.of(frame);
    if (a === undefined) throw new SegmentedValueError("segmented value was not assembled before the turn");
    return { value: a.bytes, refs: a.refs };
  }

  /**
   * Allocates a completion id.
   *
   * This counter must run **identically** on every replay, so it is only right when
   * user code is deterministic (protocol-v1 §8.4). That is why determinism is not advice
   * but a requirement: an `if (Math.random() > .5)` branch shifts the ids, and every
   * later notification gets matched to the wrong place.
   */
  alloc(): number {
    return this.nextId++;
  }

  /**
   * Signal index for the next awakeable: from 17 — 1–16 are reserved for built-in
   * signals. Deterministic across replay for the same reason as [`alloc`].
   */
  private nextAwakeable = 17;
  allocAwakeable(): number {
    return this.nextAwakeable++;
  }

  /** Position of the next command in the journal (the cursor). */
  get position(): number {
    return this.cursor;
  }

  /** Kind of the entry journaled at the next position — without moving the cursor. */
  peek(): number | undefined {
    return this.journaledCommands[this.cursor]?.type;
  }

  /**
   * One user-code command. Returns the **journaled frame** if the command is already
   * in the journal (not emitted again), `null` if it was just emitted.
   *
   * `build` is only called when emitting is really needed — so a non-deterministic value
   * like `ctx.sleep`'s wake-up time is computed **once**, as protocol-v1 §8.4 requires.
   *
   * When `name` is present, the journaled entry's name must equal it too (`ctx.run`):
   * same kind but a different name is a different step — the journal is out of place,
   * or the code renamed a step between two deploys — and matching it would rerun the
   * closure under the old name, or hand the other step's result to this one.
   */
  command(type: number, build: () => Uint8Array, name?: string): Frame | null {
    // **A journal mismatch wins** like the suspended flag: user code that swallows a
    // `JournalMismatch` (`streamText` treats it as a tool error) and carries on would build
    // every later command on a journal that no longer matches — no command may be emitted.
    if (this.mismatch !== null) throw new JournalMismatch(this.mismatch);
    if (this.failedWith !== null) throw new TurnFailed(this.failedWith);
    if (this.suspendedOn !== null) throw new Suspend(this.suspendedOn);
    const at = this.cursor++;
    const journaled = this.journaledCommands[at];
    if (journaled !== undefined) {
      if (journaled.type !== type) {
        this.mismatchAt = { at, expected: typeName(journaled.type), got: typeName(type) };
        this.mismatch =
          `entry ${at}: journal has ${typeName(journaled.type)}, ` +
          `current code produced ${typeName(type)}`;
        this.poke();
        throw new JournalMismatch(this.mismatch);
      }
      if (name !== undefined) {
        const was = decodeCommandName(journaled.payload);
        if (was !== name) {
          const expected = `${typeName(type)} ${JSON.stringify(was)}`;
          const got = `${typeName(type)} ${JSON.stringify(name)}`;
          this.mismatchAt = { at, expected, got };
          this.mismatch = `entry ${at}: journal has ${expected}, current code produced ${got}`;
          this.poke();
          throw new JournalMismatch(this.mismatch);
        }
      }
      // Same kind and same name is not enough: target, key, parameters, completion id
      // must match too — otherwise replay hands the old command's result to the new one.
      const current = build();
      const field = commandDiff(type, journaled.payload, current);
      if (field !== null) {
        const expected = `${typeName(type)} ${JSON.stringify(decodeCommandName(journaled.payload))}`;
        const got = `${typeName(type)} ${JSON.stringify(decodeCommandName(current))}`;
        this.mismatchAt = { at, expected, got };
        this.mismatch = `entry ${at}: journal has ${expected}, current code produced ${got} (field ${field} differs)`;
        this.poke();
        throw new JournalMismatch(this.mismatch);
      }
      return journaled;
    }
    // Trial replay: this command has never run. Stop **before** `build()` and before
    // any closure — no side effect of the new code may run.
    if (this.replayOnly) throw new ReplayStop(at);
    const payload = build();
    // Frame cap (protocol-v1 §4): the runtime rejects a larger frame as a transport
    // error — retried forever with the same frame. Throwing here is a terminal error at the right line of code.
    if (payload.length > MAX_FRAME_LEN) throw frameTooLarge(typeName(type), payload.length);
    this.out.push({ type, flags: 0, payload });
    return null;
  }

  emit(type: number, payload: Uint8Array, flags = 0): void {
    this.out.push({ type, flags, payload });
  }

  /** Records a `ctx.run` in [`inflight`] until it finishes. */
  track<T>(p: Promise<T>): Promise<T> {
    this.inflight.add(p);
    this.poke();
    const drop = () => this.inflight.delete(p);
    p.then(drop, drop);
    return p;
  }

  /** Waits for every unfinished `ctx.run` of the turn — see [`inflight`]. */
  async settle(): Promise<void> {
    while (this.inflight.size > 0) await Promise.allSettled([...this.inflight]);
  }

  /**
   * The whole turn is on the wire: from here on none of its write paths may be used.
   * Leftover user code (a timer created in a closure) does not slip frames into the next
   * turn on the same stream.
   */
  close(): void {
    this.flushed = this.out.length;
    this.wire = null;
    this.live = null;
  }

  /**
   * Keeps the turn alive while a `ctx.run` closure runs: if the closure has not
   * finished after [`RUN_EARLY_FLUSH_MS`] (or `keepaliveMs`, whichever is smaller),
   * flush every emitted frame — the `Run` command reaches the runtime before the
   * closure finishes —, then send `KEEPALIVE` every `keepaliveMs` until the returned
   * function is called. The runtime counts its silence cap from the last frame, so a
   * closure longer than the cap is not cancelled midway. A **short** closure that
   * finishes before that interval flushes nothing: `Run` goes with
   * `ProposeRunCompletion` in one write, and the runtime waits for durability **once**
   * for both instead of twice. Stream records written midway still carry the unflushed
   * part of `out` ahead of them ([`flushStreams`]), so the order on the wire does not
   * change. No `live` ⇒ does nothing.
   */
  keepAlive(): () => void {
    const live = this.live;
    if (live === null || live.keepaliveMs <= 0) return () => {};
    const beat: Frame = { type: MT.KEEPALIVE, flags: 0, payload: new Uint8Array() };
    let timer: ReturnType<typeof setInterval> | undefined;
    const early = setTimeout(
      () => {
        if (this.out.length > this.flushed) {
          live.write(this.out.slice(this.flushed));
          this.flushed = this.out.length;
        }
        timer = setInterval(() => live.write([beat]), live.keepaliveMs);
      },
      Math.min(RUN_EARLY_FLUSH_MS, live.keepaliveMs),
    );
    return () => {
      clearTimeout(early);
      if (timer !== undefined) clearInterval(timer);
    };
  }

  /**
   * A stream record (`payload`), or a command to close the stream (`null`).
   *
   * **Replay-aware (AP-5).** A record emitted at a point where the **next** command is
   * already in the journal belongs to a finished run: not sent again. The identity
   * `(step, attempt, k)` — `step` being the current command position — repeats exactly
   * on a rerun at the same point, so a duplicate that gets through (an RR turn rerun
   * after waiting for an ack) is dropped by the runtime (AP-11).
   *
   * Batched by [`STREAM_BATCH_MS`] or [`STREAM_BATCH_BYTES`]. Never throws: a stream
   * must not break the invocation.
   */
  /**
   * Sends an `ArtifactPut` frame (wire v4). Returns `false` when wire < v4 (the caller
   * throws). Replay — a point already in the journal — does not send: the earlier run
   * already PUT it and committed the row before the next entry (AP-1). Sent at once,
   * not batched: the runtime only reads the next frame after the PUT is done, so a
   * later command carrying the ref comes after it on the wire.
   */
  putArtifact(sha: Uint8Array, content: Uint8Array, contentType: string): boolean {
    if (this.wireVersion < STREAMS_VERSION) return false;
    if (this.replaying()) return true;
    // The turn has suspended and we are outside a closure: the rerun PUTs at exactly
    // this point — see [`appendWith`].
    if (this.dead() && closureScope.getStore() === undefined) return true;
    this.flushStreams();
    const frame: Frame = {
      type: MT.ARTIFACT_PUT,
      flags: 0,
      payload: encodeArtifactPut(sha, content, contentType),
    };
    if (this.wire === null) {
      this.out.push(frame);
      return true;
    }
    const head = this.out.slice(this.flushed);
    this.flushed = this.out.length;
    this.wire([...head, frame]);
    return true;
  }

  appendStream(name: string, durable: boolean, payload: Uint8Array | null): void {
    this.appendWith(name, durable, payload === null ? null : () => payload);
  }

  /**
   * Like [`appendStream`], with the payload built from the record's own identity
   * `(step, attempt, k)` — `$trace`'s `span_id` is hashed from it. Returns the identity
   * and whether the record was sent; `null` when wire < v4.
   *
   * **Replay still allocates identities** (without sending or calling `build`): `k`
   * advances exactly as in the real run, so the identity of a record produced later in
   * live mode equals the previous one's — the server drops the duplicate (AP-11) — and
   * a span opened during replay still has the `span_id` of the `start` sent in the
   * previous turn (REQUEST_RESPONSE: the `end` arrives in the turn after the ack).
   */
  appendWith(
    name: string,
    durable: boolean,
    build: ((at: { step: number; attempt: number; k: number }) => Uint8Array) | null,
  ): { step: number; attempt: number; k: number; sent: boolean } | null {
    if (this.wireVersion < STREAMS_VERSION) {
      if (!this.warnedStreams) {
        this.warnedStreams = true;
        process.emitWarning(
          `ctx.stream needs a runtime speaking wire v${STREAMS_VERSION}; the peer speaks v${this.wireVersion}, record dropped`,
        );
      }
      return null;
    }
    if (this.replaying()) {
      if (build === null) return null;
      return { ...this.nextIdent(name), sent: false };
    }
    // **Once the turn has suspended, what keeps running outside closures is a ghost**:
    // the outcome is a suspension whatever the code does next ([`finish`]), and the
    // rerun goes through exactly that stretch again. User code that swallows `Suspend`
    // — e.g. `streamText` treats it as a tool error and keeps running the agent loop —
    // and still sends would let the ghost record take the identity of the real record
    // the rerun will emit, and the server would drop the real one (AP-11). A closure
    // that started before the suspension (parallel tools) is real work: its result is
    // proposed and the rerun does not run it again, so its records still go out.
    if (this.dead() && closureScope.getStore() === undefined) return null;
    let p = this.pendingStreams.get(name);
    if (p === undefined) {
      p = { durable, records: [], close: false };
      this.pendingStreams.set(name, p);
    }
    let ident: { step: number; attempt: number; k: number } | null = null;
    if (build === null) {
      p.close = true;
    } else {
      ident = this.nextIdent(name);
      const payload = build(ident);
      p.records.push({ ...ident, payload });
      this.pendingStreamBytes += payload.length;
    }
    if (this.pendingStreamBytes >= STREAM_BATCH_BYTES || build === null) {
      this.flushStreams();
    } else if (this.wire !== null && this.streamTimer === null) {
      this.streamTimer = setTimeout(() => {
        this.streamTimer = null;
        this.flushStreams();
      }, STREAM_BATCH_MS);
      this.streamTimer.unref?.();
    }
    return ident === null ? null : { ...ident, sent: true };
  }

  /**
   * The next identity of stream `name` (advances `k`). `step` is the position of the
   * running closure's `Run` command, or the current cursor outside a closure: an append
   * right after `ctx.run` returns (the cursor is already past `Run`) does not share a
   * step with the records inside the closure — on a rerun the closure does not run, so
   * that step's `k` does not advance, and formerly the later record got exactly the
   * identity of the first record inside the closure and was dropped by the server.
   * Parallel closures each get their own step, interleaved: the server keeps order
   * **within** each step.
   */
  private nextIdent(name: string): { step: number; attempt: number; k: number } {
    const step = closureScope.getStore() ?? this.cursor;
    const key = `${step}:${name}`;
    const k = this.streamK.get(key) ?? 0;
    this.streamK.set(key, k + 1);
    return { step, attempt: this.attempt, k };
  }

  /**
   * Puts every batched record on the wire (BIDI) or into `out` (RR). Called before every
   * turn-ending frame — `Output`/`End`, `Suspension`, `Error` — because the runtime
   * stops reading after the outcome: a record arriving after it is lost.
   */
  flushStreams(): void {
    if (this.streamTimer !== null) {
      clearTimeout(this.streamTimer);
      this.streamTimer = null;
    }
    if (this.pendingStreams.size === 0) return;
    const frames: Frame[] = [];
    for (const [name, p] of this.pendingStreams) {
      frames.push({
        type: MT.STREAM_APPEND,
        flags: 0,
        payload: encodeStreamAppend(name, p.durable, p.records, p.close),
      });
    }
    this.pendingStreams.clear();
    this.pendingStreamBytes = 0;
    if (this.wire === null) {
      this.out.push(...frames);
      return;
    }
    // Keep program order on the wire: commands already emitted (e.g. the `Run` of a
    // running closure) go before records produced after them — like [`keepAlive`].
    const head = this.out.slice(this.flushed);
    this.flushed = this.out.length;
    this.wire([...head, ...frames]);
  }

  /** Reusable refs for the write-once value `next` — see [`Segments.reuseFor`]. */
  reuseFor(next: Uint8Array): readonly SegmentRef[] {
    return this.segments?.reuseFor(next) ?? [];
  }

  /** Holds a `ctx.run` result back until its ack — see [`pendingRuns`]. */
  proposeRun(id: number, type: number, payload: Uint8Array): void {
    this.pendingRuns.set(id, { type, flags: 0, payload });
  }

  /**
   * The ending command — `Output`.
   *
   * **`Output` is a journal entry, `End` is not** (protocol-v1 §10). So it must be
   * matched against the journal like any other command: re-emitting an already durable
   * `Output` makes the runtime append one more entry, and each recovery makes the
   * journal one entry longer.
   *
   * The window for that is real: the runtime commits frame by frame, so dying exactly
   * between committing `Output` and committing `End` can happen. On the restart after
   * that, the program has run out of steps, and protocol-v1 §10 requires the SDK to
   * resend `End` — but **only** `End`.
   *
   * Unlike [`command`], this function **does not throw**, because it runs in
   * `finish`/`fail`, where there is no exception path left. Returning `false` means the
   * journal has a different entry at this very position — the user code changed, and
   * the caller must report 570.
   */
  output(build: () => Uint8Array): boolean {
    const at = this.cursor++;
    const journaled = this.journaledCommands[at];
    if (journaled?.type === MT.OUTPUT) return true; // already durable — not emitted again
    if (journaled !== undefined) {
      this.mismatch =
        `entry ${at}: journal has ${typeName(journaled.type)}, ` +
        `current code produced ${typeName(MT.OUTPUT)}`;
      return false;
    }
    this.out.push({ type: MT.OUTPUT, flags: 0, payload: build() });
    return true;
  }

  /** The **delivered** result of completion `id` — see [`deliveries`]. */
  known(id: number): CompletionResult | undefined {
    return this.delivered.has(`c${id}`) ? this.completions.get(id) : undefined;
  }

  /** Whether the leaf is in this turn's journal (delivered or not). */
  present(key: string): boolean {
    return this.positions.has(key);
  }

  isDelivered(key: string): boolean {
    return this.delivered.has(key);
  }

  /** The result of completion `id` — only called after its leaf was delivered. */
  demand(id: number): CompletionResult {
    const c = this.known(id);
    if (c?.kind === "segmented") {
      // A transient error (Error RETRY), not a `TerminalError`: a segment that cannot be
      // assembled in this turn is not necessarily broken in the next.
      throw new SegmentedValueError("segmented value was not assembled before the turn");
    }
    if (c === undefined) throw new Error(`internal: completion ${id} read before it was delivered`);
    return c;
  }

  /**
   * A wait point of user code. `check` tries to settle it (resolve or reject) and
   * returns `true` when done; it is called at once, then after each delivery of a leaf
   * in `keys`. Still pending at the end of the turn ⇒ contributes a `mode` tree over the
   * undelivered leaves to the `Suspension`.
   */
  wait(w: Waiter): void {
    if (w.check()) return;
    this.pending.add(w);
    for (const k of w.keys) {
      if (this.delivered.has(k)) continue;
      let set = this.waitersByKey.get(k);
      if (set === undefined) this.waitersByKey.set(k, (set = new Set()));
      set.add(w);
    }
    this.poke();
  }

  /**
   * Waits until every leaf in `keys` is delivered. A leaf **not** in the journal is an
   * external wait: the promise stays pending for the rest of this turn. Once CANCEL was
   * received, an external wait is a cancellation point — throws 409, like every wait
   * point before (see [`Turn.cancelled`]).
   */
  when(keys: readonly string[]): Promise<void> {
    if (keys.every((k) => this.delivered.has(k))) return Promise.resolve();
    if (this.cancelled && keys.some((k) => !this.positions.has(k))) return Promise.reject(cancelledError());
    return new Promise<void>((resolve) => {
      this.wait({
        keys,
        mode: keys.length === 1 ? "first" : "all",
        check: () => {
          if (!keys.every((k) => this.delivered.has(k))) return false;
          resolve();
          return true;
        },
      });
    });
  }

  /**
   * Waits for the **durability** of a `ctx.run` proposal — not a cancellation point.
   *
   * The cancel flag exists so user code stops waiting for things that will never come.
   * The ack of a `ProposeRunCompletion` always comes: the runtime is right at the other
   * end of the stream, the invocation is not terminal, and it only has to finish writing
   * one entry. Making this point throw on the cancel flag would break exactly what was
   * just fixed — the compensation steps in a `catch` block are all `ctx.run`, and they
   * must be able to run after CANCEL arrives.
   *
   * It is an external wait like any other, so the suspension tree at the end of the turn
   * **always** includes the id just proposed: the runtime already wrote the
   * `RunCompletion` when it received the proposal, sees the tree satisfied and calls the
   * SDK again at once — even while another branch waits on the outside world. The
   * result never wakes user code in the same turn: the next turn delivers it in journal order.
   */
  awaitProposal(id: number): Promise<never> {
    return new Promise<never>(() => {
      this.wait({ keys: [`c${id}`], mode: "first", check: () => false });
    });
  }

  /** Records an async SDK task outside closures — see [`busy`]. */
  busyWith<T>(p: Promise<T>): Promise<T> {
    this.busy.add(p);
    const drop = () => this.busy.delete(p);
    p.then(drop, drop);
    this.poke();
    return p;
  }

  private poke(): void {
    const p = this.poked;
    this.poked = null;
    p?.();
  }

  /**
   * Drain: wait for the SDK's internal work outside closures to finish, then one
   * `setImmediate` — after it every microtask has run, i.e. user code that just woke has
   * reached its next wait point.
   */
  private async drain(): Promise<void> {
    for (;;) {
      while (this.busy.size > 0) await Promise.allSettled([...this.busy]);
      await new Promise<void>((r) => setImmediate(r));
      if (this.busy.size === 0) return;
    }
  }

  /**
   * Delivers the next leaves in journal order until a delivery wakes someone.
   * Returns `false` when everything was delivered.
   */
  private deliverNext(): boolean {
    while (this.nextDelivery < this.deliveries.length) {
      let woke = false;
      for (const k of this.deliveries[this.nextDelivery++]!) {
        this.delivered.add(k);
        const ws = this.waitersByKey.get(k);
        if (ws === undefined) continue;
        this.waitersByKey.delete(k);
        for (const w of ws) {
          if (!this.pending.has(w) || !w.check()) continue;
          this.pending.delete(w);
          woke = true;
        }
      }
      if (woke) return true;
    }
    return false;
  }

  /**
   * The loop of a turn: drain, deliver the next leaf, drain, …
   * User code only wakes at a delivery — in journal order, each after a full drain — or
   * synchronously; a real closure finishing only adds a proposal. So the commands
   * emitted are a function of the journal, not of when closures finish.
   *
   * Stops when the handler finishes, when the turn fails (`failedWith`, journal
   * mismatch), or when **quiescent**: no leaves left to deliver, no closure running, the
   * drain done and the handler not finished — then [`suspendedOn`] is the tree of every
   * pending external wait. With no external wait (user code waiting on something outside
   * the SDK) it waits for that, as before.
   */
  async drive(running: Promise<void>): Promise<void> {
    let finished = false;
    void running.then(() => {
      finished = true;
    });
    for (;;) {
      await this.drain();
      if (finished || this.failedWith !== null || this.mismatch !== null) return;
      if (this.deliverNext()) continue;
      if (this.inflight.size > 0) {
        await Promise.race([running, ...[...this.inflight].map((p) => p.then(noop, noop))]);
        continue;
      }
      if (this.pending.size > 0) {
        this.suspendedOn = this.waitTree();
        return;
      }
      await Promise.race([
        running,
        new Promise<void>((r) => {
          this.poked = r;
        }),
      ]);
    }
  }

  /**
   * The `Suspension` tree from every pending wait: one wait ⇒ exactly its tree (same
   * bytes as before); several ⇒ a `FIRST` merging the leaves, a multi-leaf `ALL` wait
   * becoming a child node.
   */
  private waitTree(): SuspensionSpec {
    const node = (w: Waiter): SuspensionTree => {
      const left = w.keys.filter((k) => !this.delivered.has(k));
      return {
        combinator: w.mode === "all" && left.length > 1 ? "all" : "first",
        completions: left.filter((k) => k[0] === "c").map((k) => Number(k.slice(1))),
        signals: left.filter((k) => k[0] === "i").map((k) => Number(k.slice(1))),
        names: left.filter((k) => k[0] === "n").map((k) => k.slice(1)),
      };
    };
    const nodes = [...this.pending].map(node);
    if (nodes.length === 1) return { tree: nodes[0]! };
    const completions = new Set<number>();
    const signals = new Set<number>();
    const names = new Set<string>();
    const nested: SuspensionTree[] = [];
    for (const n of nodes) {
      if (n.combinator === "all") {
        nested.push(n);
        continue;
      }
      for (const c of n.completions) completions.add(c);
      for (const i of n.signals) signals.add(i);
      for (const x of n.names) names.add(x);
    }
    return {
      tree: {
        combinator: "first",
        completions: [...completions].sort((a, b) => a - b),
        signals: [...signals].sort((a, b) => a - b),
        names: [...names].sort(),
        nested,
      },
    };
  }

  /**
   * CANCEL arrives **mid-turn** — the runtime pushes it down the open stream when someone
   * asks to cancel while the turn runs (`BIDI_STREAM`). From here this turn behaves like
   * a turn built on a journal that already has CANCEL: wait points throw, a cancellable
   * `ctx.run` without a result throws; running cancellable closures get the signal. The
   * frame still goes into the local journal in `runBidi`'s wait loop, so a rerun sees
   * exactly this.
   */
  cancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    for (const c of this.running) c.abort(cancelledError());
  }

  /** The signal received with this identity, if any. */
  signalSeen(signal: number | string): SignalNotification | undefined {
    return this.signals.find((s) =>
      typeof signal === "number" ? s.idx === signal : s.name === signal,
    );
  }

  /**
   * The position of a completion in journal order — used by `ctx.race`: the winner is
   * the result that **arrived first**, and that order is decided by the journal, so it
   * is identical on every replay.
   */
  orderOf(id: number): number {
    return this.positions.get(`c${id}`) ?? Number.MAX_SAFE_INTEGER;
  }

  /** Like [`orderOf`], for a signal leaf — the same position sequence as completions. */
  orderOfSignal(signal: number | string): number {
    const key = typeof signal === "number" ? `i${signal}` : `n${signal}`;
    return this.positions.get(key) ?? Number.MAX_SAFE_INTEGER;
  }
}

/**
 * Deterministic RNG, seeded from `StartMessage.random_seed` (protocol-v1 §8.4).
 *
 * xorshift64*: good enough for ids and jitter, and more importantly — **identical on
 * every replay**. User code calling `Math.random()` violates protocol-v1 §8.4.
 */
function makeRandom(seed: bigint): () => number {
  let s = seed === 0n ? 0x9e3779b97f4a7c15n : seed;
  const MASK = (1n << 64n) - 1n;
  return () => {
    s ^= (s >> 12n) & MASK;
    s = (s ^ ((s << 25n) & MASK)) & MASK;
    s ^= (s >> 27n) & MASK;
    const v = (s * 0x2545f4914f6cdd1dn) & MASK;
    // The top 53 bits → [0, 1), exactly Math.random()'s range.
    return Number(v >> 11n) / 2 ** 53;
  };
}

const enc = new TextEncoder();
const dec = new TextDecoder();

/**
 * A string exactly as it will travel on the wire: a lone surrogate half (a slice that
 * cuts an emoji in two) becomes U+FFFD — `TextEncoder` does that when writing the
 * journal, so names/keys must be compared and used as RAM keys in **that same** form,
 * otherwise a replay turn sees a different name (570) or two JS keys share one durable key.
 */
/** A terminal 413 error when a frame would exceed the protocol's 16 MiB cap. */
function frameTooLarge(what: string, bytes: number): TerminalError {
  return new TerminalError(
    `${what} of ${bytes} bytes exceeds the ${MAX_FRAME_LEN}-byte frame limit; keep large data out of the journal (ctx.artifact)`,
    413,
  );
}

function wellFormed(s: string): string {
  return s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "\uFFFD");
}

/**
 * The JSON of a stream record (`ctx.log`, `ctx.trace`, `stream.append`) — **never
 * throws**: a stream must not break the invocation. `BigInt` becomes a decimal string;
 * a value that still cannot be serialized (a cyclic object, a throwing `toJSON`) becomes
 * `{unserializable: <error>}` in place of the record.
 */
function streamJsonBytes(value: unknown): Uint8Array {
  try {
    return enc.encode(
      JSON.stringify(value, (_k, v: unknown) => (typeof v === "bigint" ? v.toString() : v)) ?? "null",
    );
  } catch (e) {
    return enc.encode(JSON.stringify({ unserializable: e instanceof Error ? e.message : String(e) }));
  }
}

function toJsonBytes(value: unknown): Uint8Array {
  return enc.encode(JSON.stringify(value === undefined ? null : value));
}

function fromJsonBytes<T>(bytes: Uint8Array, what: string): T {
  const text = dec.decode(bytes);
  if (text.length === 0) {
    throw new TerminalError(`${what} is empty, not valid JSON`);
  }
  return JSON.parse(text) as T;
}

/**
 * A durable value to come — the result of `ctx.call`, `ctx.sleep`, `ctx.get`...
 *
 * Awaitable like an ordinary Promise. It differs from a Promise in one essential
 * point: **the command is journaled the moment it is created**, while *waiting* only
 * happens on `await` (or when passed to `ctx.all` / `ctx.race`). Separating the two is
 * what makes combinators possible: create three futures, wait for whichever finishes first.
 */
export class DurableFuture<T> implements PromiseLike<T> {
  /** The completion id leaves this future waits on. Empty = value known in place. */
  readonly ids: number[];
  /**
   * Signal leaves (number = numbered, string = named) — `ctx.signal`, awakeables.
   * Signals have no completion id: their leaves go into fields 2/3 of the
   * suspension tree.
   */
  readonly signals: readonly (number | string)[];
  private readonly turn: Turn;
  private readonly take: () => T;

  constructor(turn: Turn, ids: number[], take: () => T, signals: readonly (number | string)[] = []) {
    this.turn = turn;
    this.ids = ids;
    this.take = take;
    this.signals = signals;
  }

  /** The future's leaves (`c<id>`, `i<idx>`, `n<name>`) — empty: value known in place. */
  keys(): string[] {
    return [
      ...this.ids.map((id) => `c${id}`),
      ...this.signals.map((s) => (typeof s === "number" ? `i${s}` : `n${s}`)),
    ];
  }

  /** Whether the result has been **delivered** (or is known in place) — see `Turn.deliveries`. */
  ready(): boolean {
    return this.keys().every((k) => this.turn.isDelivered(k));
  }

  /** Takes the delivered result. Internal to combinators — only call when [`ready`]. */
  resolve(): T {
    return this.take();
  }

  /** Position of the result in journal order — smaller means it arrived first. */
  order(): number {
    return Math.min(
      ...this.ids.map((id) => this.turn.orderOf(id)),
      ...this.signals.map((s) => this.turn.orderOfSignal(s)),
    );
  }

  then<R1 = T, R2 = never>(
    onfulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onrejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2> {
    return this.turn
      .when(this.keys())
      .then(() => this.take())
      .then(onfulfilled, onrejected);
  }
}

/** A workflow's durable promise — see [`Context.promise`]. */
export interface DurablePromise<T> {
  /** Waits until the promise is completed. */
  get(): DurableFuture<T>;
  /** Peeks without waiting: `null` if not completed yet. */
  peek(): DurableFuture<T | null>;
  /** Completes with a value. Throws `TerminalError` if it already has a result. */
  resolve(value: T): Promise<void>;
  /** Completes with a failure. Throws `TerminalError` if it already has a result. */
  reject(message: string, code?: number): Promise<void>;
}


/** Levels of `ctx.log`, in increasing order. */
export type LogLevel = "debug" | "info" | "warn" | "error";
const LOG_ORDER: Readonly<Record<LogLevel, number>> = { debug: 0, info: 1, warn: 2, error: 3 };
const isLogLevel = (v: string | undefined): v is LogLevel => v !== undefined && v in LOG_ORDER;

/** Attributes of a log line or a span. */
export type Attrs = Record<string, unknown>;

/** `ctx.log` — xem {@link Context.log}. */
export interface Log {
  debug(msg: string, attrs?: Attrs): void;
  info(msg: string, attrs?: Attrs): void;
  warn(msg: string, attrs?: Attrs): void;
  error(msg: string, attrs?: Attrs): void;
}

/** An open span — the argument of the function in {@link Trace.span}. */
export interface Span {
  /** Adds attributes; they go with the `end` record. */
  set(attrs: Attrs): void;
}

/** `ctx.trace` — xem {@link Context.trace}. */
export interface Trace {
  span<T>(name: string, fn: (span: Span) => Promise<T> | T, attrs?: Attrs): Promise<T>;
}

/**
 * The span open in the current call scope — a child span gets its parent along the
 * `async` call path, not by time order (two spans running in parallel are not each
 * other's parents).
 */
const spanScope = new AsyncLocalStorage<{ spanId: string | null }>();

/**
 * Inside the closure of a `ctx.run`, and the position of its `Run` command — so that
 * [`Turn.appendWith`] can tell a still-running closure's real work from the ghost part
 * that runs after the turn suspended, and so that the closure's records carry the
 * `step` of that very step ([`Turn.nextIdent`]).
 */
const closureScope = new AsyncLocalStorage<number>();

/** Property key attaching a handler's log level to the handler function — {@link withLogLevel}. */
const LOG_LEVEL = Symbol("cairnflow.logLevel");

/**
 * A handler carrying its minimum log level (`HandlerOptions.logLevel`). The
 * invocation's `cairnflow-log-level` header still overrides it.
 */
export function withLogLevel(h: AnyHandler, level: LogLevel): AnyHandler {
  const wrapped = ((ctx: Context, input: unknown) => h(ctx, input)) as AnyHandler & {
    [LOG_LEVEL]?: LogLevel;
  };
  wrapped[LOG_LEVEL] = level;
  return wrapped;
}

/**
 * Deterministic `span_id`: the first 16 hex of the SHA-256 of `(invocation, step, k)` —
 * the same span has the same id in cairn and in the OTLP backend, and the backend merges
 * resends by itself.
 *
 * **`attempt` is not hashed.** `attempt` counts attempts since the last durable entry,
 * so it changes between when `start` is sent and when the span closes (a span opened on
 * attempt 2 and closed in a later turn with `attempt` 0; dying mid-closure, then
 * attempt 5 closing a span opened on attempt 0). A `start` at a point already in the
 * journal is not resent, so only an id independent of `attempt` lets the `end` match
 * the `start` already sent. The cost: an attempt that dies **before** the next command
 * is durable, followed by one that reopens the span at that same point, merges the two
 * into one span (the later `start` overwrites the earlier).
 */
function spanIdOf(invocation: string, at: { step: number; k: number }): string {
  return createHash("sha256")
    .update(`${invocation}:${at.step}:${at.k}`)
    .digest("hex")
    .slice(0, 16);
}

/** The ref of an artifact — what goes into the journal instead of the bytes. */
export interface ArtifactRef {
  /** sha256 hex of the content. */
  readonly sha256: string;
  readonly size: number;
  readonly contentType: string;
}

/** `ctx.artifact` — see {@link Context.artifact}. */
export interface Artifacts {
  put(bytes: Uint8Array | string, opts?: { contentType?: string }): Promise<ArtifactRef>;
  get(ref: ArtifactRef | string): Promise<Uint8Array>;
}

/**
 * Size cap of one artifact: 16 MiB minus 1 KiB, leaving room for the head of the
 * `ArtifactPut` frame under the 16 MiB frame cap. Matches `cairn_store::artifact::MAX_BYTES`.
 */
export const ARTIFACT_MAX_BYTES = (16 << 20) - 1024;

/** A sha256 in hex, as `put` returns it. */
const SHA256_HEX = /^[0-9a-f]{64}$/;

/** Time cap of one artifact download, ms: the turn fails (and is retried) instead of hanging. */
const ARTIFACT_FETCH_TIMEOUT_MS = 60_000;

/** Source for reading artifacts outside the process's memory — {@link configureArtifacts}. */
let artifactSource: { baseUrl: string; headers: Record<string, string> } | null = null;

/**
 * Configures where `ctx.artifact.get` gets artifacts that are not in the process's
 * memory (an earlier run, another SDK node): the cairnflow ingress port and headers
 * (the tenant's `invoke` key). Reads `GET /invocations/{id}/artifacts/{sha}`.
 */
export function configureArtifacts(o: { baseUrl: string; headers?: Record<string, string> }): void {
  artifactSource = { baseUrl: o.baseUrl.replace(/\/+$/, ""), headers: o.headers ?? {} };
}

/** The process's artifact memory: `sha → bytes`, capped in bytes, oldest evicted first. */
const ARTIFACT_CACHE_MAX = 64 << 20;
const artifactCache = new Map<string, Uint8Array>();
let artifactCacheBytes = 0;
function rememberArtifact(sha: string, bytes: Uint8Array): void {
  if (artifactCache.has(sha) || bytes.length > ARTIFACT_CACHE_MAX) return;
  artifactCache.set(sha, bytes);
  artifactCacheBytes += bytes.length;
  for (const [k, v] of artifactCache) {
    if (artifactCacheBytes <= ARTIFACT_CACHE_MAX) break;
    artifactCache.delete(k);
    artifactCacheBytes -= v.length;
  }
}

/** The writer of a stream — see {@link Context.stream}. */
export interface Stream {
  /** Appends a record (JSON). Does not wait, does not throw; dropped on replay. */
  append(record: unknown): void;
  /** Closes the stream: no record after that. */
  close(): void;
}

/** Options of `ctx.run`. */
export interface RunOptions {
  /**
   * A pass-through step that can stop when the invocation is cancelled — see `ctx.run`.
   * Default `false`: a saga's compensation steps still run after CANCEL.
   */
  readonly cancellable?: boolean;
}

/** The context passed to user code. */
export class Context {
  /** Deterministic RNG. Use this, not `Math.random()` (protocol-v1 §8.4). */
  readonly random: () => number;

  private readonly turn: Turn;
  private readonly start: StartInfo;
  /**
   * The wire version the runtime at the other end **settled on** at discovery, read
   * from the `/invoke` content-type.
   *
   * It is here for exactly one reason: `ctx.scoped(...)` sends `scope` in fields 7/8,
   * and a v1 runtime would **silently ignore unknown fields** — the call runs with no
   * cap at all, and the user sees a bill rather than an error. Knowing the version is
   * the only way to turn that into an error at the offending line of code.
   */
  private readonly wireVersion: number;
  /**
   * Eager state: the object's state snapshot from `StartMessage`.
   * `null` = the runtime sent none (v1, or the object has no state yet) → every read
   * goes the lazy path. `partial` = an incomplete snapshot; absent keys must be read lazily.
   */
  private stateMap: Map<string, Uint8Array> | null;
  private partial: boolean;
  /**
   * Wire v6: the **most recently read** segmented value of each key in this turn (from
   * `GetStateCompletion` — the refs of the state row) — `set` reuses the refs of a
   * matching byte prefix. Any other read (inline, absent, a journaled `GetEagerState`)
   * or any write of that key replaces/deletes the entry.
   */
  private readonly known = new Map<string, KnownValue>();
  /**
   * Counts writes per key (and `clearAll`): a `get` only remembers refs if no write of
   * that key came between the call and the result — refs of an overwritten value are no
   * longer in the current row, and the engine rejects them.
   */
  private readonly writes = new Map<string, number>();
  private clears = 0;

  constructor(turn: Turn, start: StartInfo, wireVersion: number = MAX_PROTOCOL_VERSION) {
    this.turn = turn;
    this.start = start;
    this.wireVersion = wireVersion;
    this.random = makeRandom(start.randomSeed);
    // An empty map + partial=false CANNOT be told apart from a v1 runtime on the wire
    // (proto3 does not write default values), so the map is only trusted when non-empty.
    if (start.stateMap.length > 0 || start.partialState) {
      this.stateMap = new Map(
        start.stateMap.map((e) => [dec.decode(e.key), e.value]),
      );
      this.partial = start.partialState;
    } else {
      this.stateMap = null;
      this.partial = false;
    }
  }

  /** Key of the virtual object. Empty if the service has no key. */
  get key(): string {
    return this.start.key;
  }

  /**
   * The id of this invocation — **exactly the id the runtime accepts** everywhere over
   * HTTP (`/invocations/{id}`, `ctx.attach`, …): the hex of `StartMessage.id`.
   *
   * Treat it as an opaque string. Its length and internal layout are the runtime's
   * business, and have changed once before.
   */
  get invocationId(): string {
    let out = "";
    for (const b of this.start.id) out += b.toString(16).padStart(2, "0");
    return out;
  }

  /**
   * The invocation's label **for people reading logs** — `debug_id` of `StartMessage`.
   *
   * Not an id: for a scoped invocation, the runtime adds a `"{scope}/"` prefix so logs
   * can tell which tenant the invocation belongs to. Do not store it or use it for HTTP
   * lookups — use {@link Context.invocationId}.
   */
  /**
   * The server-side schedule time this run belongs to:
   * `{ name, at }` when the invocation was fired by a schedule (`at` is the nominal
   * time, ms; for a manual fire, the time of firing), `undefined` otherwise. Read from
   * the `cairnflow-schedule` header of entry 0 — `restart` copies headers, so a rerun
   * still belongs to that time.
   */
  get schedule(): ScheduleMark | undefined {
    return this.turn.schedule;
  }

  get debugId(): string {
    return this.start.debugId;
  }

  /**
   * How many times this turn has been retried **since the handler last made progress**.
   *
   * `0` on the first run, and reset to 0 each time the journal grows. So it answers
   * "is this spot stuck", not "how many times has this invocation failed": a ten-step
   * workflow where every step fails transiently once still sees
   * 0, 1, 0, 1, … rather than climbing to 10.
   *
   * Use it to **give up deliberately** — the runtime retries forever with backoff, and
   * that is the right default for transient errors, but wrong for an API that is
   * permanently broken:
   *
   * ```ts
   * const r = await ctx.run("call vendor", async () => {
   *   if (ctx.attempt >= 3) {
   *     throw new TerminalError("vendor not responding after 3 attempts", 503);
   *   }
   *   return callVendor();
   * });
   * ```
   *
   * A `TerminalError` thrown inside the closure is the step's **result**: it goes into
   * the journal as a failed result (the closure does not run again in later turns), then
   * `ctx.run` rethrows it — same `code`, `message` — on every replay. If nobody catches
   * it, the invocation ends **failed** instead of retrying: the waiter gets the error,
   * and it does not sit in the queue forever.
   */
  get attempt(): number {
    return this.start.retryCount;
  }

  /**
   * Ends this invocation and starts a **new** invocation with the same target and
   * `input`, with an empty journal — for long loops (multi-step agents, endless
   * polling): each invocation's journal stays bounded, and replay does not grow
   * quadratically. The new one runs on the service's **current revision** — a natural
   * upgrade point. The caller and the idempotency key move to the new one; the old one
   * ends with `{"continuedAsNew": "<id>"}`.
   *
   * Does not return: the line of code after the call does not run. Needs runtime wire v3.
   *
   * ```ts
   * async loop(ctx, s: { step: number }) {
   *   for (let i = 0; i < 50; i++) s = await ctx.run(`step ${s.step}`, () => next(s));
   *   if (!s.done) ctx.continueAsNew(s);
   *   return s;
   * }
   * ```
   */
  continueAsNew(input: unknown): never {
    if (this.wireVersion < KEEPALIVE_VERSION) {
      throw new TerminalError(
        `ctx.continueAsNew needs a runtime speaking wire v${KEEPALIVE_VERSION}; the peer speaks v${this.wireVersion}`,
      );
    }
    throw new ContinueAsNew(input);
  }

  /**
   * A **stream** of this invocation: tokens, progress — what a UI reads via
   * `GET /invocations/{id}/streams/{name}` (SSE) while the handler is still running.
   *
   * By default it is an **ephemeral stream**: it only lives in the leader's RAM. When the
   * leader changes the running attempt is rerun too, and readers get `reset`. A stream
   * is not a journal — the full output is still the `ctx.run` result.
   *
   * ```ts
   * const ui = ctx.stream("ui");
   * const answer = await ctx.run("llm", async () => {
   *   let text = "";
   *   for await (const tok of model.stream(prompt)) {
   *     ui.append({ tok });
   *     text += tok;
   *   }
   *   return text;
   * });
   * ```
   *
   * `append` does not wait, does not throw, and is not resent on replay. Needs runtime
   * wire v4; below v4 records are dropped (with one warning).
   */
  /**
   * The invocation's log: the durable stream `$log`, tied to the step and attempt, that
   * survives `kill -9` until the invocation is cleaned up. Read it with
   * `GET /invocations/{id}/logs` (admin permission).
   *
   * Levels below the minimum (default `info`; `HandlerOptions.logLevel`; the
   * invocation's `cairnflow-log-level` header) are dropped at once. Does not wait, does
   * not throw, is not resent on replay.
   */
  get log(): Log {
    const turn = this.turn;
    const emit = (level: LogLevel, msg: string, attrs?: Attrs): void => {
      if (LOG_ORDER[level] < LOG_ORDER[turn.minLogLevel]) return;
      turn.appendWith(LOG_STREAM, true, () =>
        streamJsonBytes({ ts: Date.now(), level, msg, ...(attrs === undefined ? {} : { attrs }) }),
      );
    };
    return {
      debug: (m, a) => emit("debug", m, a),
      info: (m, a) => emit("info", m, a),
      warn: (m, a) => emit("warn", m, a),
      error: (m, a) => emit("error", m, a),
    };
  }

  /**
   * The invocation's trace: each span is two records on the durable stream `$trace` —
   * `start{span_id, parent_id, name, ts, attrs}` then
   * `end{span_id, parent_id, name, ts, status, attrs, error?}` — so a node dying mid-span
   * still leaves the `start` behind (an "abandoned" span). `end` repeats `name` and
   * `parent_id` because a non-durable `start` is lost when the runtime dies and the rerun
   * does not resend it: the server builds the span from the `end` alone. Spans nest by
   * call scope; if `fn` throws, the span is `error` and the error propagates.
   *
   * ```ts
   * const r = await ctx.trace.span("plan", async (s) => {
   *   s.set({ model: "…", input_tokens: 1200 });
   *   return ctx.run("llm", () => callModel());
   * });
   * ```
   *
   * Replay-aware: a span opened at a point already in the journal does not emit `start`
   * again; `end` is only emitted when the span closes in live mode (the journal head),
   * with the same `span_id` — a duplicate of an `end` already sent is dropped by the
   * server (AP-11).
   */
  get trace(): Trace {
    const turn = this.turn;
    const invocation = this.invocationId;
    return {
      span: async <T>(name: string, fn: (span: Span) => Promise<T> | T, attrs?: Attrs) => {
        const parent = spanScope.getStore()?.spanId ?? null;
        let spanId: string | null = null;
        // Replay: `start` is not resent, but the identity is still allocated — the same
        // `span_id` as the `start` sent in the real run, so the span's `end` (which may
        // arrive in live mode, e.g. an RR turn after the ack) matches it.
        const opened = turn.appendWith(TRACE_STREAM, true, (at) =>
          streamJsonBytes({
            kind: "start",
            span_id: spanIdOf(invocation, at),
            parent_id: parent,
            name,
            ts: Date.now(),
            ...(attrs === undefined ? {} : { attrs }),
          }),
        );
        if (opened !== null) spanId = spanIdOf(invocation, opened);
        const extra: Attrs = {};
        const span: Span = { set: (a) => void Object.assign(extra, a) };
        const end = (status: "ok" | "error", error?: unknown): void => {
          if (spanId === null) return;
          const id = spanId;
          turn.appendWith(TRACE_STREAM, true, () =>
            streamJsonBytes({
              kind: "end",
              span_id: id,
              parent_id: parent,
              name,
              ts: Date.now(),
              status,
              ...(Object.keys(extra).length === 0 ? {} : { attrs: extra }),
              ...(error === undefined
                ? {}
                : { error: error instanceof Error ? error.message : String(error) }),
            }),
          );
        };
        // Wire < v4 (no identity): the span's children get its parent.
        const scope = { spanId: spanId ?? parent };
        try {
          const v = await spanScope.run(scope, () => fn(span));
          end("ok");
          return v;
        } catch (e) {
          // Suspending (waiting for a completion, for a `ctx.run` ack in REQUEST_RESPONSE)
          // or a journal mismatch is the turn's control flow, not a span error: leave it
          // open — the next turn replays up to here and closes it, with the same
          // `span_id`.
          if (!(e instanceof Suspend) && !(e instanceof JournalMismatch)) end("error", e);
          throw e;
        }
      },
    };
  }

  /**
   * Artifacts (wire v4): large bytes kept out of the journal.
   * `put` sends the bytes to the runtime — the runtime finishes the PUT before
   * processing the next command (AP-1) — and returns a ref computed in place (sha256):
   * return that ref from `ctx.run`, and the journal only carries a few dozen bytes.
   * Replay-aware: a point already in the journal does not resend. `get` reads from the
   * process's memory, otherwise from the {@link configureArtifacts} source. At most
   * {@link ARTIFACT_MAX_BYTES} (16 MiB minus 1 KiB) per artifact.
   */
  get artifact(): Artifacts {
    const turn = this.turn;
    const invocation = this.invocationId;
    return {
      put: async (bytes, opts = {}) => {
        const content = typeof bytes === "string" ? enc.encode(bytes) : bytes;
        if (content.length > ARTIFACT_MAX_BYTES) {
          throw new RangeError(`artifact of ${content.length} bytes exceeds the limit of ${ARTIFACT_MAX_BYTES}`);
        }
        const digest = createHash("sha256").update(content).digest();
        const contentType = opts.contentType ?? "application/octet-stream";
        if (!turn.putArtifact(new Uint8Array(digest), content, contentType)) {
          throw new Error(
            `ctx.artifact needs a runtime speaking wire v${STREAMS_VERSION}; the peer speaks v${this.wireVersion}`,
          );
        }
        const sha256 = digest.toString("hex");
        rememberArtifact(sha256, content);
        return { sha256, size: content.length, contentType };
      },
      get: async (ref) => {
        const sha = typeof ref === "string" ? ref : ref.sha256;
        // The sha goes into the source's URL path, with the tenant's key: only a sha256.
        if (typeof sha !== "string" || !SHA256_HEX.test(sha)) {
          throw new TypeError("artifact ref is not a sha256 (64 lowercase hex digits)");
        }
        const size = typeof ref === "string" ? undefined : ref.size;
        if (size !== undefined && (!Number.isSafeInteger(size) || size < 0 || size > ARTIFACT_MAX_BYTES)) {
          throw new RangeError(`artifact ${sha}: the ref's size ${size} is outside [0, ${ARTIFACT_MAX_BYTES}]`);
        }
        const hit = artifactCache.get(sha);
        if (hit !== undefined && (size === undefined || hit.length === size)) return hit;
        if (artifactSource === null) {
          throw new Error(
            `artifact ${sha} is not in this process's memory; call configureArtifacts({ baseUrl }) to read it from cairnflow`,
          );
        }
        const source = artifactSource;
        // Async SDK work outside closures: the turn's drain waits for it (`Turn.busy`).
        return turn.busyWith(
          (async () => {
            const signal = typeof AbortSignal.timeout === "function" ? AbortSignal.timeout(ARTIFACT_FETCH_TIMEOUT_MS) : undefined;
            // No redirect: the configured headers (the tenant's key) never go anywhere but
            // the source. A `3xx` is not `ok` ⇒ the error below.
            const res = await fetch(`${source.baseUrl}/invocations/${invocation}/artifacts/${sha}`, {
              headers: source.headers,
              redirect: "manual",
              ...(signal === undefined ? {} : { signal }),
            });
            if (!res.ok) {
              await res.body?.cancel().catch(() => {});
              throw new Error(`artifact ${sha}: HTTP ${res.status}`);
            }
            // At most the ref's size (or the artifact limit): a longer body stops being read.
            const body = await readAtMost(res, size ?? ARTIFACT_MAX_BYTES);
            if (body === null) throw new Error(`artifact ${sha}: the download is larger than ${size ?? ARTIFACT_MAX_BYTES} bytes`);
            if (size !== undefined && body.length !== size) {
              throw new Error(`artifact ${sha}: got ${body.length} bytes, the ref says ${size}`);
            }
            if (createHash("sha256").update(body).digest("hex") !== sha) {
              throw new Error(`artifact ${sha}: the download does not match its sha256`);
            }
            rememberArtifact(sha, body);
            return body;
          })(),
        );
      },
    };
  }

  stream(name: string, opts: { durable?: boolean } = {}): Stream {
    if (name.length === 0 || name.length > MAX_STREAM_NAME || name.startsWith("$")) {
      throw new TypeError(
        `invalid stream name: ${JSON.stringify(name)} (1-${MAX_STREAM_NAME} bytes, must not start with "$")`,
      );
    }
    const turn = this.turn;
    const durable = opts.durable === true;
    return {
      append: (record: unknown) => turn.appendStream(name, durable, streamJsonBytes(record)),
      close: () => turn.appendStream(name, durable, null),
    };
  }

  /**
   * Reads state. `null` means the key **does not exist**.
   *
   * With an eager snapshot it answers in place — journaling a `GetEagerState` entry
   * carrying the value read, so replay returns the value *at that time* rather than the
   * current value. Without one (a v1 runtime, or a snapshot missing this key) it emits
   * `GetState` and waits as before.
   *
   * Look at the journal before choosing the path: an invocation that lived through a
   * runtime upgrade may carry lazy entries in its journal even though a snapshot is now
   * available — following the journal, not current capabilities, means replay never diverges.
   */
  get<T>(key: string): DurableFuture<T | null> {
    key = wellFormed(key);
    const journaled = this.turn.peek();
    const eager =
      journaled === MT.GET_EAGER_STATE ||
      (journaled === undefined &&
        this.stateMap !== null &&
        (!this.partial || this.stateMap.has(key)));

    if (eager) {
      const frame = this.turn.command(MT.GET_EAGER_STATE, () =>
        encodeGetEagerState(key, this.stateMap?.get(key) ?? null, key),
      );
      // The refs of a journaled `GetEagerState` are **not** remembered: the runtime seals
      // that entry itself (the value from the SDK's snapshot), so the part beyond the
      // prefix matching the row is new segments, held by the invocation rather than the
      // state row — `set` reusing them would be rejected by the engine, and every retry
      // would repeat exactly that. A read from the snapshot in place (newly emitted) keeps
      // them: that value is the latest `set`'s, and its known refs match it.
      const raw = frame !== null ? this.turn.eagerValue(frame).value : (this.stateMap?.get(key) ?? null);
      if (frame !== null) this.remember(key, this.writeStamp(key), raw);
      return new DurableFuture(this.turn, [], () =>
        raw === null ? null : fromJsonBytes<T>(raw, `state ${JSON.stringify(key)}`),
      );
    }

    const id = this.turn.alloc();
    this.turn.command(MT.GET_STATE, () => encodeGetState(key, id, key));
    const stamp = this.writeStamp(key);
    return new DurableFuture(this.turn, [id], () => {
      const result = this.turn.demand(id);
      switch (result.kind) {
        case "void":
          this.remember(key, stamp, null);
          return null;
        case "value":
          this.remember(key, stamp, result.value, result.refs);
          return fromJsonBytes<T>(result.value, `state ${JSON.stringify(key)}`);
        case "failure":
          throw new TerminalError(result.message, result.code);
        default:
          throw new TerminalError(`result ${result.kind} is not valid for GetState`);
      }
    });
  }

  /** Marker for "nobody wrote this key yet" at the time `get` is called — see [`writes`]. */
  private writeStamp(key: string): string {
    return `${this.writes.get(key) ?? 0}:${this.clears}`;
  }

  private wrote(key: string): void {
    this.writes.set(key, (this.writes.get(key) ?? 0) + 1);
  }

  /** Remembers the read of `key` (keeping refs for a segmented value), if no write came in between. */
  private remember(key: string, stamp: string, value: Uint8Array | null, refs?: readonly SegmentRef[]): void {
    if (stamp !== this.writeStamp(key)) return;
    if (value !== null && refs !== undefined && refs.length > 0) this.known.set(key, { refs, bytes: value });
    else this.known.delete(key);
  }

  /**
   * Writes state. Does not wait — this command has no completion.
   *
   * Wire v6: if the new value starts with the bytes of the segments already read for
   * this same key in the turn ⇒ `SetState` carries the refs of that prefix and only the
   * rest — the whole history is not resent every turn.
   */
  set<T>(key: string, value: T): void {
    key = wellFormed(key);
    const bytes = toJsonBytes(value);
    const known = this.wireVersion >= WIRE_SEGMENTED ? this.known.get(key) : undefined;
    let reuse: readonly SegmentRef[] = [];
    const journaled = this.turn.command(MT.SET_STATE, () => {
      reuse = known === undefined ? [] : known.refs.slice(0, reusablePrefix(known, bytes));
      return encodeSetState(key, bytes, key, reuse);
    });
    this.wrote(key);
    // Replayed: we do not know which refs the earlier run sent ⇒ assume nothing; the next
    // `set` sends inline, and the shell compares the prefix by sha. Newly emitted: the row
    // after commit starts with exactly the refs just sent.
    if (journaled === null && reuse.length > 0) this.known.set(key, { refs: reuse, bytes });
    else this.known.delete(key);
    this.stateMap?.set(key, bytes);
  }

  clear(key: string): void {
    key = wellFormed(key);
    this.turn.command(MT.CLEAR_STATE, () => encodeClearState(key, key));
    this.wrote(key);
    this.known.delete(key);
    this.stateMap?.delete(key);
  }

  /** Deletes ALL of the object's state. */
  clearAll(): void {
    this.turn.command(MT.CLEAR_ALL_STATE, () => encodeClearAllState("clearAll"));
    this.clears++;
    this.known.clear();
    // After deleting everything, the snapshot becomes complete-and-empty — even if it was
    // only partial before: there is nothing left to be missing.
    this.stateMap = new Map();
    this.partial = false;
  }

  /**
   * The object's state keys.
   *
   * A complete eager snapshot answers in place (journaling `GetEagerStateKeys` carrying
   * the list itself); otherwise it asks the runtime (`GetStateKeys`) and waits.
   */
  getKeys(): DurableFuture<string[]> {
    const journaled = this.turn.peek();
    const eager =
      journaled === MT.GET_EAGER_STATE_KEYS ||
      (journaled === undefined && this.stateMap !== null && !this.partial);

    if (eager) {
      const keys = [...(this.stateMap?.keys() ?? [])].sort();
      const frame = this.turn.command(MT.GET_EAGER_STATE_KEYS, () =>
        encodeGetEagerStateKeys(keys.map((k) => enc.encode(k)), "getKeys"),
      );
      const raw = frame !== null
        ? decodeGetEagerStateKeys(frame.payload).map((k) => dec.decode(k))
        : keys;
      return new DurableFuture(this.turn, [], () => raw);
    }

    const id = this.turn.alloc();
    this.turn.command(MT.GET_STATE_KEYS, () => encodeGetStateKeys(id, "getKeys"));
    return new DurableFuture(this.turn, [id], () => {
      const result = this.turn.demand(id);
      switch (result.kind) {
        case "stateKeys":
          return result.keys.map((k) => dec.decode(k));
        case "failure":
          throw new TerminalError(result.message, result.code);
        default:
          throw new TerminalError(`result ${result.kind} is not valid for GetStateKeys`);
      }
    });
  }

  /**
   * Sleeps `ms` milliseconds.
   *
   * The wake-up time is **absolute**, computed exactly once and then journaled
   * (protocol-v1 §8.4). Later replays do not recompute it — if they did, every replay
   * would make the invocation sleep one more interval.
   */
  sleep(ms: number, name = "sleep"): DurableFuture<void> {
    const id = this.turn.alloc();
    this.turn.command(MT.SLEEP, () => encodeSleep(BigInt(Date.now() + ms), id, name));
    return new DurableFuture(this.turn, [id], () => {
      const result = this.turn.demand(id);
      if (result.kind === "failure") throw new TerminalError(result.message, result.code);
    });
  }

  /**
   * Calls another handler and (on `await`) **waits for the result**.
   *
   * This command declares two completions (protocol-v1 §5): one receives the id of the
   * called invocation, one the result. The SDK only waits for the second — the first is
   * still sent by the runtime and stays in the journal, as protocol-v1 §8.1 requires
   * ("the runtime MUST send a notification for every declared completion id").
   *
   * If the callee fails, its error surfaces here as a `TerminalError`: its error is
   * already decided, and retrying on the caller's side cannot change anything.
   */
  call<T>(
    service: string,
    handler: string,
    input: unknown,
    opts: CallOptions = {},
  ): DurableFuture<T> {
    // Check before allocating a completion id: a malformed call must not use up an id.
    const placement = placementField(opts);
    const idCompletion = this.turn.alloc();
    const resultCompletion = this.turn.alloc();
    const fields = {
      service,
      handler,
      parameter: toJsonBytes(input),
      key: opts.key ?? "",
      idempotencyKey: opts.idempotencyKey,
      idCompletion,
      name: opts.name ?? `${service}/${handler}`,
      ...flowFields(opts),
      ...placement,
      tags: opts.tags,
    };
    this.turn.command(MT.CALL, () => encodeCall(fields, resultCompletion));
    return new DurableFuture(this.turn, [resultCompletion], () => {
      const result = this.turn.demand(resultCompletion);
      switch (result.kind) {
        case "value":
          return fromJsonBytes<T>(result.value, `result of ${service}/${handler}`);
        case "failure":
          throw new TerminalError(result.message, result.code);
        case "void":
          return null as T;
        default:
          throw new TerminalError(`result ${result.kind} is not valid for Call`);
      }
    });
  }

  /**
   * Calls another handler **without waiting for the result**.
   *
   * Does not suspend the turn: we `demand` no completion. The id notification still
   * arrives and sits in the journal — harmless, and keeps the protocol-v1 §8.1 contract.
   */
  send(service: string, handler: string, input: unknown, opts: CallOptions = {}): void {
    const placement = placementField(opts);
    const idCompletion = this.turn.alloc();
    const fields = {
      service,
      handler,
      parameter: toJsonBytes(input),
      key: opts.key ?? "",
      idempotencyKey: opts.idempotencyKey,
      idCompletion,
      name: opts.name ?? `${service}/${handler}`,
      ...flowFields(opts),
      ...placement,
      tags: opts.tags,
    };
    // An absolute time, computed in `build` so it is computed ONCE (replay reuses the
    // journaled frame) — the same time handling as `ctx.sleep`.
    this.turn.command(MT.ONE_WAY_CALL, () =>
      encodeOneWayCall(
        fields,
        opts.delay !== undefined && opts.delay > 0
          ? BigInt(Date.now() + Math.ceil(opts.delay))
          : 0n,
      ),
    );
  }

  /**
   * A child `ctx` whose every `call`/`send` carries the scope (and limit key).
   *
   * ```ts
   * ctx.scoped("acme", "growth/alice").call(Svc, "handler", arg);
   * ctx.scoped("acme").send(Svc, "handler", arg);
   * ```
   *
   * **The scope decides the range.** Every target in a scope — every service, every
   * key — shares one partition key, hence one range: calls within a scope are always
   * local, never through outbox → HTTP. The scope beats both `key` and `idempotencyKey`
   * in choosing the range. The callee **and its whole subtree** inherit this scope (a
   * call that declares no scope carries the caller's scope, down every level).
   *
   * The cost: a scope is bounded by the cap of **one** range — one stream, each commit
   * through the object store. Split cannot divide a scope, and a `concurrency` rule set
   * above the range's capacity gives nothing extra. One scope per user / tenant is fine;
   * one per request, or one scope for the whole system, is not. The scope is also
   * identity: objects, idempotency keys and promises in a scope are distinct from their
   * unscoped counterparts.
   * See `docs/key-design.md`.
   *
   * The format is checked **right here**, on the client side, and throws on the spot.
   * Waiting for ingress to report it would make people debug through a network round
   * trip — and worse: the command is already in the journal, so the error would show up
   * as a broken invocation rather than as a wrong line of code.
   *
   * Returns a thin *view* rather than a new `Context`: it shares exactly the same turn,
   * the same journal, the same completion id allocator. A second `Context` would be a
   * second counter, and that is the surest way for two commands to ask for the same
   * completion id.
   */
  scoped(scope: string, limitKey?: string): ScopedContext {
    if (this.wireVersion < FLOW_CONTROL_VERSION) {
      throw new TerminalError(
        `the runtime speaks wire version ${this.wireVersion}; ctx.scoped(...) needs v${FLOW_CONTROL_VERSION}. ` +
          "An older runtime silently IGNORES the scope, so the call would run with no limit at all; " +
          "upgrade the runtime, or remove ctx.scoped from this deployment.",
      );
    }
    checkComponent("scope", scope);
    if (limitKey !== undefined) {
      const parts = limitKey.split("/");
      if (parts.length < 1 || parts.length > 2) {
        throw new TypeError(
          `limit key ${JSON.stringify(limitKey)} has ${parts.length} levels; only 1 or 2 are allowed (l1 or l1/l2)`,
        );
      }
      for (const p of parts) checkComponent("limit key", p);
    }
    return new ScopedContext(this, scope, limitKey);
  }

  /**
   * Sends a signal to another invocation (`SendSignal`, 0x0410). One-way, does not wait,
   * does not suspend — like `ctx.send`. If the target has ended, the signal is a
   * no-op.
   *
   * `signal` is a number (the built-in range numbered by the system) or a name chosen by the application.
   */
  sendSignal(targetInvocationId: string, signal: number | string, value?: unknown): void {
    this.turn.command(MT.SEND_SIGNAL, () =>
      encodeSendSignal({
        target: targetInvocationId,
        signal,
        value: value === undefined ? undefined : toJsonBytes(value),
        name: `signal/${signal}`,
      }),
    );
  }

  /**
   * A signal by identity (idx or name), as a **future**: awaitable, and composable into
   * `ctx.race` / `ctx.all` together with completions — e.g. waiting for an approval with
   * a deadline:
   *
   * ```ts
   * const r = await ctx.race([ctx.signal<Approval>("approve"), ctx.sleep(24 * 3600_000)]);
   * if (r === undefined) throw new TerminalError("approval expired", 408);
   * ```
   *
   * The value is the signal's JSON payload; a signal carrying no value gives `null`.
   * A signal carrying an error (an awakeable reject) throws `TerminalError`.
   *
   * **A signal that has arrived stays satisfied** (protocol-v1 §8.7 rule 4): no command
   * goes into the journal on the call, and the signal is a journal entry. If `sleep`
   * wins a `race`, a signal arriving later is still there; a later `ctx.signal` with the
   * same identity is satisfied at once with the old value — to wait for a new one, use a
   * new name. Each losing `sleep` costs a commit when it fires: use long deadlines, do
   * not loop on short ones.
   */
  signal<T = unknown>(signal: number | string): DurableFuture<T> {
    if (typeof signal === "string") signal = wellFormed(signal);
    const turn: Turn = this.turn;
    return new DurableFuture<T>(
      turn,
      [],
      () => {
        const seen = turn.signalSeen(signal);
        if (seen === undefined) throw new Error(`internal: signal ${signal} read before it was delivered`);
        if (seen.result.kind === "value") {
          return fromJsonBytes<T>(seen.result.value, `signal ${signal}`);
        }
        if (seen.result.kind === "failure") {
          throw new TerminalError(seen.result.message, seen.result.code);
        }
        return null as T;
      },
      [signal],
    );
  }

  /**
   * `await ctx.signal(signal)` — kept for old code. Returns `null` if the signal carries
   * no value.
   */
  async awaitSignal<T = unknown>(signal: number | string): Promise<T | null> {
    return this.signal<T | null>(signal);
  }

  /**
   * Awakeable: a wait point that an **external system** completes by id, without knowing
   * a promise name or object — send the `id` to a webhook (inside `ctx.run`), then
   * `await promise`. The external system calls `POST /awakeables/{id}/resolve` (JSON
   * body) or `/reject`, with an `invoke` key of the same tenant.
   *
   * ```ts
   * const { id, promise } = ctx.awakeable<Approval>();
   * await ctx.run("send approval request", () => mailer.send(approver, id));
   * const approval = await ctx.race([promise, ctx.sleep(7 * 24 * 3600_000)]);
   * ```
   *
   * Built on index-based signals to this very invocation — no new table.
   * `promise` is a [`DurableFuture`] (lazy: only suspends when `await`ed), so it can go
   * into `ctx.race` / `ctx.all`.
   */
  awakeable<T = unknown>(): { id: string; promise: DurableFuture<T> } {
    const idx = this.turn.allocAwakeable();
    const id = `awk_${this.invocationId}${idx.toString(16).padStart(8, "0")}`;
    return { id, promise: this.signal<T>(idx) };
  }

  /**
   * A durable promise by name, belonging to this object — can be completed from ANOTHER
   * handler of the same object (workflow: one step waits for approval, the approval
   * handler resolves it).
   */
  promise<T>(name: string): DurablePromise<T> {
    const turn = this.turn;
    return {
      get: (): DurableFuture<T> => {
        const id = turn.alloc();
        turn.command(MT.GET_PROMISE, () => encodeGetPromise(name, id, `promise/${name}`));
        return new DurableFuture(turn, [id], () => {
          const result = turn.demand(id);
          switch (result.kind) {
            case "value":
              return fromJsonBytes<T>(result.value, `promise ${JSON.stringify(name)}`);
            case "failure":
              throw new TerminalError(result.message, result.code);
            default:
              throw new TerminalError(`result ${result.kind} is not valid for GetPromise`);
          }
        });
      },
      peek: (): DurableFuture<T | null> => {
        const id = turn.alloc();
        turn.command(MT.PEEK_PROMISE, () => encodePeekPromise(name, id, `promise/${name}`));
        return new DurableFuture(turn, [id], () => {
          const result = turn.demand(id);
          switch (result.kind) {
            case "void":
              return null;
            case "value":
              return fromJsonBytes<T>(result.value, `promise ${JSON.stringify(name)}`);
            case "failure":
              throw new TerminalError(result.message, result.code);
            default:
              throw new TerminalError(`result ${result.kind} is not valid for PeekPromise`);
          }
        });
      },
      resolve: async (value: T): Promise<void> => {
        const id = turn.alloc();
        turn.command(MT.COMPLETE_PROMISE, () =>
          encodeCompletePromise(name, { value: toJsonBytes(value) }, id, `promise/${name}`),
        );
        await turn.when([`c${id}`]);
        const result = turn.demand(id);
        if (result.kind === "failure") throw new TerminalError(result.message, result.code);
      },
      reject: async (message: string, code = 500): Promise<void> => {
        const id = turn.alloc();
        turn.command(MT.COMPLETE_PROMISE, () =>
          encodeCompletePromise(name, { code, message }, id, `promise/${name}`),
        );
        await turn.when([`c${id}`]);
        const result = turn.demand(id);
        if (result.kind === "failure") throw new TerminalError(result.message, result.code);
      },
    };
  }

  /**
   * Waits for the result of another invocation by id (the hex form from `ctx.call` or
   * ingress). If the target failed, its error surfaces as a `TerminalError`.
   */
  attach<T>(invocationId: string, name = "attach"): DurableFuture<T> {
    const id = this.turn.alloc();
    this.turn.command(MT.ATTACH_INVOCATION, () =>
      encodeAttachInvocation(invocationId, id, name),
    );
    return new DurableFuture(this.turn, [id], () => {
      const result = this.turn.demand(id);
      switch (result.kind) {
        case "value":
          return fromJsonBytes<T>(result.value, `result of ${invocationId}`);
        case "failure":
          throw new TerminalError(result.message, result.code);
        default:
          throw new TerminalError(`result ${result.kind} is not valid for Attach`);
      }
    });
  }

  /**
   * Peeks at the result of another invocation **without waiting**: `null` if it has not
   * finished. If the target failed, its error surfaces as a `TerminalError`.
   */
  getOutput<T>(invocationId: string, name = "getOutput"): DurableFuture<T | null> {
    const id = this.turn.alloc();
    this.turn.command(MT.GET_INVOCATION_OUTPUT, () =>
      encodeGetInvocationOutput(invocationId, id, name),
    );
    return new DurableFuture(this.turn, [id], () => {
      const result = this.turn.demand(id);
      switch (result.kind) {
        case "void":
          return null;
        case "value":
          return fromJsonBytes<T>(result.value, `result of ${invocationId}`);
        case "failure":
          throw new TerminalError(result.message, result.code);
        default:
          throw new TerminalError(`result ${result.kind} is not valid for GetInvocationOutput`);
      }
    });
  }

  /**
   * Waits for **all** — like `Promise.all`, but able to suspend durably: with results
   * still missing, this turn ends with a suspension carrying an `ALL_COMPLETED` tree,
   * and the runtime only wakes it when everything is done.
   *
   * If one future fails, the outcome is already decided — the error surfaces at once,
   * like Promise.all rejecting on the first rejection. "First" is **in journal order**
   * (delivery order), not by position in the array: a replay after another future also
   * failed still throws exactly the old error.
   */
  all<T extends readonly DurableFuture<unknown>[]>(
    futures: T,
  ): Promise<{ [K in keyof T]: T[K] extends DurableFuture<infer V> ? V : never }> {
    type Out = { [K in keyof T]: T[K] extends DurableFuture<infer V> ? V : never };
    const turn = this.turn;
    const keys = futures.flatMap((f) => f.keys());
    if (turn.cancelled && keys.some((k) => !turn.present(k))) return Promise.reject(cancelledError());
    return new Promise<Out>((resolve, reject) => {
      const got: ({ ok: true; value: unknown } | { ok: false; error: unknown } | undefined)[] = futures.map(() => undefined);
      turn.wait({
        keys,
        mode: "all",
        check: () => {
          futures.forEach((f, i) => {
            if (got[i] !== undefined || !f.ready()) return;
            try {
              got[i] = { ok: true, value: f.resolve() };
            } catch (error) {
              got[i] = { ok: false, error };
            }
          });
          let failed = -1;
          futures.forEach((f, i) => {
            const g = got[i];
            if (g === undefined || g.ok) return;
            if (failed < 0 || f.order() < futures[failed]!.order()) failed = i;
          });
          if (failed >= 0) {
            reject((got[failed] as { error: unknown }).error);
            return true;
          }
          if (got.some((g) => g === undefined)) return false;
          resolve(got.map((g) => (g as { value: unknown }).value) as Out);
          return true;
        },
      });
    });
  }

  /**
   * Waits for **whichever finishes first** — like `Promise.race`. The winner is decided
   * by the order of notifications in the journal, so replay picks the same one again
   * (protocol-v1 §8.4): "finished first" is a recorded event, not a race run again.
   */
  race<T extends readonly DurableFuture<unknown>[]>(
    futures: T,
  ): Promise<T[number] extends DurableFuture<infer V> ? V : never> {
    type Out = T[number] extends DurableFuture<infer V> ? V : never;
    const turn = this.turn;
    const keys = futures.flatMap((f) => f.keys());
    if (turn.cancelled && !futures.some((f) => f.keys().every((k) => turn.present(k)))) {
      return Promise.reject(cancelledError());
    }
    return new Promise<Out>((resolve, reject) => {
      turn.wait({
        keys,
        mode: "first",
        check: () => {
          let winner: DurableFuture<unknown> | null = null;
          for (const f of futures) {
            if (f.ready() && (winner === null || f.order() < winner.order())) winner = f;
          }
          if (winner === null) return false;
          try {
            resolve(winner.resolve() as Out);
          } catch (e) {
            reject(e);
          }
          return true;
        },
      });
    });
  }

  /**
   * Runs a side effect **exactly once** per durable result.
   *
   * Proposes the result, then **waits for it to be durable** — this branch suspends
   * until the next turn while other branches keep going; the result only reaches user
   * code in the next turn, in journal order. `ProposeRunCompletionAck`
   * is the runtime saying "written", and it is only sent *after* the commit
   * (`Effect::Persist` comes before `Effect::Send`, and that is the state machine's
   * contract, not an accidental order). The ack carries no value: the value was just
   * computed here, and the SDK keeps it in `pendingRuns` until the ack comes back.
   *
   * Waiting **is the condition of the promise, not overhead**. The previous version
   * carried on right after proposing, and its failure window was much wider than it
   * looked: it extended to the **next suspension point**, not just to the next command.
   * Three `ctx.run`s in a row in one turn that died at the third reran all three, including
   * two that had long finished. Measurable, and measured.
   *
   * What remains is genuine at-least-once, and unavoidable: dying **while** `fn` is
   * running leaves a command in the journal without a result, so the next attempt runs
   * it again. That is why the work inside `ctx.run` should still be idempotent.
   *
   * In `REQUEST_RESPONSE` the runtime cannot push anything midway, so the turn ends with
   * this very suspension and the runtime calls again — protocol-v1 §4 allows it ("every
   * `ctx.run` MUST end with a suspension or output in the same turn").
   * It costs one more HTTP round trip per `ctx.run`; that is the price of the same
   * promise over a one-way transport.
   *
   * **Cancellation.** By default `ctx.run` is not a cancellation point: a saga's
   * compensation steps in a `catch` block are `ctx.run`, and they must be able to run
   * after CANCEL arrives. `{ cancellable: true }` is for *forward* steps (model calls,
   * tools): if the invocation was cancelled and the result is not in the journal yet, it
   * throws a 409 `TerminalError` instead of running the closure; if CANCEL arrives while
   * the closure runs (`BIDI_STREAM`), `signal` (the closure's parameter) aborts, and the
   * closure throwing after that is also a 409 — no result is proposed. A closure that
   * returns normally despite being aborted still has its result recorded as usual.
   * In `REQUEST_RESPONSE` there is no channel into the middle of a turn: a running
   * closure runs to the end, and the cancel takes effect from the next `ctx.run`.
   */
  run<T>(
    name: string,
    fn: (signal: AbortSignal) => Promise<T> | T,
    opts: RunOptions = {},
  ): Promise<T> {
    return this.runStep(name, fn, opts);
  }

  private async runStep<T>(
    name: string,
    fn: (signal: AbortSignal) => Promise<T> | T,
    opts: RunOptions,
  ): Promise<T> {
    name = wellFormed(name);
    const id = this.turn.alloc();
    const step = this.turn.position;
    this.turn.command(MT.RUN, () => encodeRun(id, name), name);

    // The result is in the journal ⇒ wait for it to be delivered, in journal order — not
    // returned at once (see `Turn.deliveries`), the closure does not run.
    if (this.turn.present(`c${id}`)) {
      await this.turn.when([`c${id}`]);
      const known = this.turn.known(id)!;
      switch (known.kind) {
        case "value":
          return fromJsonBytes<T>(known.value, `result of ctx.run ${JSON.stringify(name)}`);
        case "failure":
          throw new TerminalError(known.message, known.code);
        case "void":
          return null as T;
        default:
          throw new TerminalError(`result ${known.kind} is not valid for Run`);
      }
    }

    // No result yet and the invocation was cancelled: a forward step does not run. The
    // `Run` command was emitted (or matched the journal) above, so a rerun stops at
    // exactly this point, with the same cursor.
    const cancellable = opts.cancellable === true;
    if (cancellable && this.turn.cancelled) throw cancelledError();
    // The closure really runs (a new command, or a replay gap — a journaled `Run` with no
    // result). The turn does not end while it runs ([`Turn.inflight`]); when done it
    // proposes, and the branch **waits externally** for its own durable result.
    await this.turn.track(this.execute(id, step, fn, cancellable));
    return this.turn.awaitProposal(id);
  }

  /** Runs the closure of `ctx.run`, then proposes the result — see [`run`]. */
  private async execute<T>(
    id: number,
    step: number,
    fn: (signal: AbortSignal) => Promise<T> | T,
    cancellable: boolean,
  ): Promise<void> {
    const abort = new AbortController();
    if (cancellable) {
      this.turn.running.add(abort);
      this.turn.onCancellable?.();
    }

    let value: T;
    const stop = this.turn.keepAlive();
    try {
      value = await closureScope.run(step, () => fn(abort.signal));
    } catch (e) {
      // Aborted because of CANCEL: whatever the closure throws is a cancel — no result
      // goes into the journal, the rerun throws above.
      if (cancellable && abort.signal.aborted) throw cancelledError();
      if (e instanceof RunFailure || e instanceof TerminalError) {
        // A failure is a result too, so it must also be durable before anyone relies on
        // it. Proposed exactly like the success path; a rerun reads `failure` out of the
        // journal and throws `TerminalError` at exactly this point, with the same `code`
        // and `message`. Likewise for `TerminalError`: throwing it live into user code
        // would make the closure rerun in every later turn (a `Run` without completion),
        // and would be a wake-up outside journal order.
        this.turn.emit(
          MT.PROPOSE_RUN_COMPLETION,
          encodeProposeRunFailure(id, e.code, e.message),
          this.turn.requestAck ? FLAG_REQUESTED_ACK : 0,
        );
        this.turn.proposeRun(id, MT.RUN_COMPLETION, encodeRunCompletionFailure(id, e.code, e.message));
        return;
      }
      // An ordinary error: do NOT propose a result, so the retry reruns the side effect
      // — and the turn dies here (`failedWith`), even if the caller swallows the error.
      this.turn.failedWith ??= e;
      throw e;
    } finally {
      stop();
      this.turn.running.delete(abort);
    }
    const bytes = toJsonBytes(value);
    const reuse = this.turn.reuseFor(bytes);
    const payload =
      reuse.length === 0 && bytes.length > MAX_FRAME_LEN ? null : encodeProposeRunCompletion(id, bytes, reuse);
    if (payload === null || payload.length > MAX_FRAME_LEN) {
      // The result exceeds the frame cap: a **terminal, durable** failure of the step —
      // do not emit a frame the runtime would reject and retry, rerunning the closure forever.
      const e = frameTooLarge("ctx.run result", bytes.length);
      this.turn.emit(
        MT.PROPOSE_RUN_COMPLETION,
        encodeProposeRunFailure(id, e.code, e.message),
        this.turn.requestAck ? FLAG_REQUESTED_ACK : 0,
      );
      this.turn.proposeRun(id, MT.RUN_COMPLETION, encodeRunCompletionFailure(id, e.code, e.message));
      return;
    }
    this.turn.emit(MT.PROPOSE_RUN_COMPLETION, payload, this.turn.requestAck ? FLAG_REQUESTED_ACK : 0);
    this.turn.proposeRun(id, MT.RUN_COMPLETION, encodeRunCompletion(id, bytes));
  }
}

/**
 * A user handler.
 *
 * The type `I` is **the author's declaration**, not something the runtime checks. On
 * the wire, input is just parsed JSON — the SDK neither knows nor checks its shape.
 * Whoever needs certainty checks it in the handler, and throws `TerminalError` if it is
 * wrong (retrying a wrong payload still comes out wrong).
 */
/**
 * The minimum wire version that understands `scope`/`limit_key` — matches
 * `cairn_server::discovery::WIRE_MAX`.
 */
const FLOW_CONTROL_VERSION = 2;

/**
 * The wire version this SDK speaks. The same number as `MAX_PROTOCOL_VERSION` in
 * `serve.ts`, declared again here so `handler.ts` does not depend back on the HTTP
 * layer — the two modules deliberately do not know each other.
 */
const MAX_PROTOCOL_VERSION = 4;

/**
 * Wire v4: the `StreamAppend` side channel (`ctx.stream`).
 * When the runtime speaks below v4, `ctx.stream` drops records — such a runtime answers
 * 571 to an unknown code.
 */
const STREAMS_VERSION = 4;

/** Batching of stream records: flush after this many ms … */
const STREAM_BATCH_MS = 50;
/** … or when the batch reaches this many bytes. */
const STREAM_BATCH_BYTES = 1024;
/** Matches `cairn_server::streams::MAX_NAME`. */
const MAX_STREAM_NAME = 128;
/** System streams of `ctx.log` / `ctx.trace` — always durable. */
const LOG_STREAM = "$log";
/**
 * A `ctx.run` closure running longer than this (ms) makes the SDK flush `Run` to the
 * wire and start sending `KEEPALIVE` ([`Turn.keepAlive`]); shorter, and `Run` goes with the result.
 */
const RUN_EARLY_FLUSH_MS = 20;
const TRACE_STREAM = "$trace";
/** Invocation header (entry 0) overriding the minimum log level — turn on `debug` when investigating. */
const LOG_LEVEL_HEADER = "cairnflow-log-level";
/** Entry-0 header of a run fired by a schedule: `<name>@<time ms>`. */
const SCHEDULE_HEADER = "cairnflow-schedule";

/** `<name>@<time>` ⇒ `{ name, at }`; malformed ⇒ `undefined` (does not throw — it is the runtime's header). */
function parseScheduleHeader(v: string | undefined): ScheduleMark | undefined {
  if (v === undefined) return undefined;
  const i = v.lastIndexOf("@");
  if (i <= 0) return undefined;
  const at = v.slice(i + 1);
  if (!/^\d+$/.test(at)) return undefined;
  return { name: v.slice(0, i), at: Number(at) };
}

/**
 * Wire v3: `KEEPALIVE` during long closures, the `replay_only` flag. When the runtime
 * speaks v2 the SDK sends nothing new.
 */
const KEEPALIVE_VERSION = 3;

/** Length cap of one identity component — matches `cairn_store::MAX_COMPONENT`. */
const MAX_COMPONENT = 36;
const COMPONENT_RE = /^[A-Za-z0-9_.-]+$/;

/**
 * Checks one scope / limit key component, and throws **at once** if it is wrong.
 *
 * The same rule as the runtime, rewritten rather than called over the wire: what we buy
 * is an error at the wrong line of code, before any command goes into the journal.
 */
function checkComponent(what: string, value: string): void {
  if (value.length === 0) {
    throw new TypeError(`${what} is empty; omit the argument instead of passing an empty string`);
  }
  if (value.length > MAX_COMPONENT) {
    throw new TypeError(`${what} is ${value.length} characters long; the limit is ${MAX_COMPONENT}`);
  }
  if (!COMPONENT_RE.test(value)) {
    throw new TypeError(`${what} ${JSON.stringify(value)} has invalid characters; only [a-zA-Z0-9_.-] is allowed`);
  }
}

/** Merges the scope/limit key of a `ctx.scoped(...)` into a call's options. */
function flowFields(opts: CallOptions): { scope?: string; limitKey?: string } {
  const out: { scope?: string; limitKey?: string } = {};
  if (opts.scope !== undefined) out.scope = opts.scope;
  if (opts.limitKey !== undefined) out.limitKey = opts.limitKey;
  return out;
}

/**
 * The call's `placement`, checked **at once** — an unknown value is the author's error,
 * raised at the wrong line of code before any command goes into the journal (like `checkComponent`).
 */
function placementField(opts: CallOptions): { placement?: CallPlacement } {
  if (opts.placement === undefined) return {};
  if (opts.placement !== "spread") {
    throw new TypeError(
      `invalid placement ${JSON.stringify(opts.placement)}; the only value is "spread" (or omit the argument)`,
    );
  }
  return { placement: opts.placement };
}

/**
 * A `ctx` with a scope attached. See [`Context.scoped`].
 *
 * Only `call` and `send`: those are the only two commands that create a *new*
 * invocation, and a scope only means something for a new invocation. This handler's
 * own `ctx.get`/`ctx.set` stay in **its own** scope — being able to change the scope of
 * running state would mean changing one's own identity midway.
 */
export class ScopedContext {
  readonly #ctx: Context;
  readonly #scope: string;
  readonly #limitKey: string | undefined;

  constructor(ctx: Context, scope: string, limitKey: string | undefined) {
    this.#ctx = ctx;
    this.#scope = scope;
    this.#limitKey = limitKey;
  }

  call<T>(
    service: string,
    handler: string,
    input: unknown,
    opts: CallOptions = {},
  ): DurableFuture<T> {
    return this.#ctx.call<T>(service, handler, input, this.#merge(opts));
  }

  send(service: string, handler: string, input: unknown, opts: CallOptions = {}): void {
    this.#ctx.send(service, handler, input, this.#merge(opts));
  }

  /**
   * An option given in place **beats** the view's scope: `scoped("a").call(..., { scope:
   * "b" })` says plainly that "b" is wanted, and silently ignoring it would be far worse.
   */
  #merge(opts: CallOptions): CallOptions {
    const merged: { scope?: string; limitKey?: string } & CallOptions = { ...opts };
    if (merged.scope === undefined) merged.scope = this.#scope;
    if (merged.limitKey === undefined && this.#limitKey !== undefined) {
      merged.limitKey = this.#limitKey;
    }
    return merged;
  }
}

/** Options for `ctx.call` / `ctx.send`. */
export interface CallOptions {
  /**
   * Key of the target virtual object. Leave empty if the service has no key. Without a
   * scope the key decides the callee's range (`H(service, key)`).
   */
  readonly key?: string;
  /**
   * Guarantees at most one invocation for the same set of keys (protocol-v1 §8.6). With
   * no scope and no key, it decides the callee's range.
   */
  readonly idempotencyKey?: string;
  /** Display name in the journal. */
  readonly name?: string;
  /**
   * The call's scope (flow control). Absent = **inherit the caller's scope**;
   * `ctx.scoped(...)` is the concise way to set it.
   *
   * **The scope decides the range**, ahead of both `key` and `idempotencyKey`: every
   * target in a scope sits in one range, and the callee's whole subtree inherits this
   * scope. The cost is that a scope is bounded by the cap of one range (split cannot
   * divide it). With no scope, no key and no idempotency key, the callee is colocated
   * next to the caller (inheriting the pk prefix, on clusters with the `calls/prefix`
   * gate on) — or deliberately spread with `placement: "spread"`. See
   * [`Context.scoped`] and `docs/key-design.md`.
   */
  readonly scope?: string;
  /**
   * `l1` or `l1/l2`. **Must come with a scope** — without a scope there is no counter
   * to limit, and a limit key with no effect is a parameter that makes the author
   * believe they just set a cap.
   */
  readonly limitKey?: string;
  /**
   * Delays the call by this many milliseconds (`OneWayCall.invoke_time`). Only meaningful
   * for `ctx.send` — `ctx.call` waits for the result, so there is nothing to delay.
   *
   * The absolute time is computed ONCE when the command is emitted (like `ctx.sleep`,
   * protocol-v1 §8.4): replay reuses the journaled time, without recomputing.
   */
  readonly delay?: number;
  /**
   * Placement of an **unkeyed** callee. Absent: the callee inherits the caller's pk
   * prefix — the whole unkeyed call tree is colocated on the root's range (on clusters
   * with the `calls/prefix` gate on). `"spread"`: **deliberately spread** —
   * `pk = H(caller id, completion id)`, so with `R` ranges ≈ (R−1)/R of the calls go to
   * another range. Use it for CPU-heavy child fan-out; children waiting on I/O (LLMs,
   * external APIs) are fine colocated.
   *
   * Only changes **placement**: the callee id stays deterministic, identity does not
   * change, no deduplication, no counters — do not use a separate `key`/`idempotencyKey`
   * per child just to spread.
   *
   * **Ignored** (the callee is colocated as by default, no error) when the call is keyed
   * — `scope` (including a scope inherited from the caller, or `ctx.scoped`), `key`,
   * `idempotencyKey` —; when the cluster has not turned on the `calls/placement` gate;
   * or when the running invocation was created before its range got the gate (the rule
   * is the caller's, settled when it was created). Old runtimes that do not know the
   * field ignore it too. The wire version is not bumped: ignoring only changes
   * placement, not the id. See `docs/key-design.md` §8.
   */
  readonly placement?: CallPlacement;
  /**
   * The callee's user tags: they go into its metadata, and can be filtered in
   * `POST /query` (`sys_invocation.tags`, the `ByAttr` path) like ingress's
   * `cairnflow-tag` header. Keys `[a-z0-9_.-]{1,32}`, values ≤ 64 bytes, at most 8
   * tags; tags breaking the rules are dropped by the runtime (the call still runs). Old
   * runtimes that do not know the field ignore it too. The callee always also gets
   * `$root` (the root of the call tree) and `$parent` — even with `ctx.send`.
   */
  readonly tags?: Readonly<Record<string, string>>;
}

export type Handler<I = any, O = unknown> = (ctx: Context, input: I) => Promise<O> | O;

/**
 * A `Context` **without** `set`/`clear`/`clearAll` — the context of a `SHARED`
 * handler: in a workflow, and in a virtual object (`handlerTypes: { x: "SHARED" }` —
 * declare this type for that handler's `ctx`; `handlerTypes` does not change the type
 * by itself).
 *
 * This is a fence at the type level, and it is *not* the only thing enforcing the rule:
 * the runtime rejects this handler's state writes with a PROTOCOL_VIOLATION
 * (`StartParams.state_read_only`, for virtual objects too). This only makes the error
 * show up when `tsc` runs instead of when the customer's code runs.
 *
 * Why it is forbidden: a `SHARED` handler runs **in parallel** with `run` (workflow) or
 * with an `EXCLUSIVE` handler (virtual object) on the same key, holding no lock. A
 * read-modify-write on both sides is a lost update.
 */
export type SharedContext = Omit<Context, "set" | "clear" | "clearAll">;

export type SharedHandler<I = any, O = unknown> = (
  ctx: SharedContext,
  input: I,
) => Promise<O> | O;

/**
 * A `Context` **without** state and promises — the context of a handler in a plain
 * service (`SERVICE`, not a virtual object or workflow).
 *
 * State and promises belong to an **object**: they sit under that object's partition
 * key, and an unkeyed, unscoped service has no object for them to belong to. The
 * runtime rejects all of its state/promise commands with a terminal error
 * (`NO_OBJECT_KEY`, 574); this type only makes the error show up when `tsc` runs
 * instead of when the customer's service runs.
 *
 * Need state? Declare the service as `{ type: "VIRTUAL_OBJECT" }` and call it with a key.
 */
/** The schedule time of a run — see {@link Context.schedule}. */
export interface ScheduleMark {
  readonly name: string;
  readonly at: number;
}

export type ServiceContext = Omit<
  Context,
  "get" | "set" | "clear" | "clearAll" | "getKeys" | "promise"
>;

export type ServiceHandler<I = any, O = unknown> = (
  ctx: ServiceContext,
  input: I,
) => Promise<O> | O;

/** The type-erased form, used where things are stored and dispatched. */
export type AnyHandler = Handler<any, unknown>;

/**
 * Runs user code **once** on a local journal.
 *
 * This is the kernel shared by both transports: `REQUEST_RESPONSE` calls it exactly
 * once per HTTP request, `BIDI_STREAM` calls it again each time a new notification
 * arrives on the same stream. One function means one replay semantics — there is no
 * "bidi path" to drift.
 */
async function runOnce(
  handler: AnyHandler,
  start: StartInfo,
  entries: readonly Frame[],
  wireVersion: number = MAX_PROTOCOL_VERSION,
  live: ((frames: readonly Frame[]) => void) | null = null,
  requestAck = true,
  attach: (turn: Turn) => void = () => {},
  segments: Segments | null = null,
): Promise<Turn> {
  const turn = new Turn(entries, segments);
  attach(turn);
  turn.requestAck = requestAck;
  turn.replayOnly = wireVersion >= KEEPALIVE_VERSION && start.replayOnly;
  turn.wireVersion = wireVersion;
  turn.attempt = start.retryCount;
  if (live !== null && wireVersion >= STREAMS_VERSION) turn.wire = live;
  if (live !== null && wireVersion >= KEEPALIVE_VERSION && start.keepaliveMs > 0) {
    turn.live = { write: live, keepaliveMs: start.keepaliveMs };
  }

  const inputEntry = entries.find((e) => e.type === MT.INPUT);
  let input: unknown = null;
  const ownLevel = (handler as AnyHandler & { [LOG_LEVEL]?: LogLevel })[LOG_LEVEL];
  if (ownLevel !== undefined) turn.minLogLevel = ownLevel;
  if (inputEntry !== undefined) {
    const raw = decodeInput(inputEntry.payload);
    input = raw.length === 0 ? null : safeJson(raw);
    const headers = decodeInputHeaders(inputEntry.payload);
    const level = headers.get(LOG_LEVEL_HEADER);
    if (isLogLevel(level)) turn.minLogLevel = level;
    turn.schedule = parseScheduleHeader(headers.get(SCHEDULE_HEADER));
  }

  const ctx = new Context(turn, start, wireVersion);
  let outcome: { ok: true; output: unknown } | { ok: false; error: unknown } | undefined;
  const running = (async () => {
    try {
      outcome = { ok: true, output: await handler(ctx, input) };
    } catch (e) {
      outcome = { ok: false, error: e };
    }
  })();
  // Deliver results in journal order until the handler finishes, the turn fails, or it
  // is quiescent — see [`Turn.drive`].
  await turn.drive(running);
  // The handler finished (or the turn failed) while other closures are still running:
  // the turn's outcome is only decided after they finish — see [`Turn.inflight`].
  await turn.settle();
  // If this turn's proposals are not durable yet, the result cannot be trusted yet —
  // suspend on them (even when the handler already returned output: a `ctx.run` nobody
  // awaits, the losing branch of a race).
  if (turn.suspendedOn === null && turn.pendingRuns.size > 0) {
    turn.suspendedOn = { first: [...turn.pendingRuns.keys()].sort((a, b) => a - b) };
  }
  if (outcome === undefined) {
    // The handler is still waiting: the turn stopped on a transient error, a journal mismatch, or quiescence.
    fail(turn, turn.failedWith ?? new Suspend(turn.suspendedOn ?? { first: [] }));
  } else if (outcome.ok) {
    finish(turn, outcome.output);
  } else {
    fail(turn, outcome.error);
  }
  return turn;
}

function onWire(frames: readonly Frame[]): Uint8Array {
  return concatFrames(frames.map((f) => encodeFrame(f.type, f.flags, f.payload)));
}

function badFirstMessage(): Uint8Array {
  return onWire([
    {
      type: MT.ERROR,
      flags: 0,
      payload: encodeError(571, "the first message must be Start", ERROR_FAIL),
    },
  ]);
}

/**
 * Runs one `REQUEST_RESPONSE` turn: takes the runtime's byte stream, returns the bytes
 * of the reply.
 *
 * This function is pure in the sense that matters most — it does not touch the network.
 * That lets tests build every replay situation without a server.
 */
export async function runTurn(
  handler: AnyHandler,
  body: Uint8Array,
  wireVersion: number = MAX_PROTOCOL_VERSION,
  values: ValueOptions = {},
): Promise<Uint8Array> {
  const frames = decodeFrames(body);
  const first = frames[0];
  if (first === undefined || first.type !== MT.START) {
    return badFirstMessage();
  }
  const start = decodeStart(first.payload);
  // Wire v6: assemble every segmented value **before** user code runs.
  // `body` (what the replay cache keeps and hashes) does not change — the assembled
  // copy sits alongside the frame. The runtime only sends refs to deployments settled on
  // v6: below v6 a ref is refused without fetching (an endpoint capped at v5 must not fetch
  // URLs a caller put in the turn), and reading the tail as the whole value, never.
  const segments = new Segments(values, wireVersion);
  segments.addUrls(start.segmentUrls);
  const entries: Frame[] = [];
  try {
    for (const f of frames.slice(1)) {
      if (f.type === MT.SEGMENT_URLS) {
        segments.addUrls(decodeSegmentUrls(f.payload));
        continue;
      }
      await segments.admit(f);
      entries.push(f);
    }
  } catch (e) {
    return onWire([valueFailure(e)]);
  }
  const turn = await runOnce(
    handler,
    start,
    entries,
    wireVersion,
    null,
    false, // protocol-v1 §4: REQUEST_RESPONSE does not ask for acks
    () => {},
    segments,
  );
  return onWire(turn.out);
}

/**
 * An `Error` frame (retried) when assembling a segmented value fails — a missing
 * segment, an expired URL, a wrong sha: a transient turn error, not user code's, not a
 * journal mismatch. The message never carries the URL.
 */
function valueFailure(e: unknown): Frame {
  const message = e instanceof Error ? e.message : String(e);
  return { type: MT.ERROR, flags: 0, payload: encodeError(500, `segmented value: ${message}`, ERROR_RETRY) };
}

/**
 * Both directions on an open stream — the SDK end of `BIDI_STREAM`.
 *
 * A deliberately narrow interface: it knows nothing about HTTP/2 or sockets, only frames
 * in and bytes out. That lets tests build a whole conversation from two arrays, and the
 * only thing `serve.ts` has to get right is connecting it to `node:http2`.
 */
export interface FrameStream {
  /** The next frame from the runtime. `null` = the runtime closed the stream. */
  next(): Promise<Frame | null>;
  /** Frames that have arrived and are waiting — takes them all, without blocking. */
  buffered(): Frame[];
  write(bytes: Uint8Array): void;
  /** Ends the SDK side. The turn is over. */
  end(): void;
}

/**
 * Whether a frame from the runtime takes a slot in the journal.
 *
 * Acks (`CommandAck`, `ProposeRunCompletionAck`) and `0xFC00+` do **not**: they are
 * messages, not entries. Putting them in the local journal shifts the index of every
 * later entry — and a shifted journal index breaks replay at the hardest place to trace.
 */
function isJournalEntry(type: number): boolean {
  return isCommand(type) || isNotification(type);
}

/**
 * A `0xFC00+` arrived but the SDK has no API yet to hand it to user code.
 *
 * Dropping it is right — it is not an entry (see [`isJournalEntry`]) — but dropping it
 * silently would make a third party think their extension is working. The same reasoning
 * as the runtime's warning when `REQUEST_RESPONSE` has no forwarding path: wherever a
 * message is swallowed, it must be said loudly.
 */
function noteCustomDropped(f: Frame): void {
  if (f.type >= 0xfc00) {
    process.emitWarning(
      `custom message 0x${f.type.toString(16)} dropped: the SDK has no API for 0xFC00+ messages yet`,
    );
  }
}

/**
 * Whether what the SDK awaits is already in the local journal — the twin of the
 * runtime's `suspension_satisfied`, and deliberately **the same rule**.
 *
 * With it, the wait loop reads until *enough* has arrived, not until *something* has.
 * Notifications arrive in many pieces with no "one batch" boundary: rerunning the
 * handler with only half of what it needs, just to suspend again at the same spot, is
 * wasted work, and each re-suspension is one more `Suspension` the runtime must examine.
 */
function suspensionSatisfied(spec: SuspensionSpec, entries: readonly Frame[]): boolean {
  const t = suspensionTree(spec);
  const done = new Set<number>();
  const idx = new Set<number>();
  const names = new Set<string>();
  for (const e of entries) {
    if (e.type === MT.SIGNAL) {
      const s = decodeSignal(e.payload);
      if (s.idx !== undefined) idx.add(s.idx);
      if (s.name !== undefined) names.add(s.name);
    } else if (isNotification(e.type)) {
      done.add(decodeNotification(e.payload).completionId);
    }
  }
  return treeSatisfied(t, (id) => done.has(id), (i) => idx.has(i), (n) => names.has(n));
}

/**
 * The rule of the runtime's `suspension_satisfied_by` (`machine.rs`) on a one-level
 * tree: `first` is satisfied when **any** leaf is, `all` when **every** leaf is. The
 * shared vectors `conformance/suspension-vectors.json` check that the two copies agree
 * (CG-4).
 */
export function treeSatisfied(
  t: SuspensionTree,
  done: (completion: number) => boolean,
  hasIdx: (signal: number) => boolean,
  hasName: (name: string) => boolean,
): boolean {
  const leaves = [
    ...t.completions.map(done),
    ...t.signals.map(hasIdx),
    ...t.names.map(hasName),
    ...(t.nested ?? []).map((n) => treeSatisfied(n, done, hasIdx, hasName)),
  ];
  return t.combinator === "all" ? leaves.every(Boolean) : leaves.some(Boolean);
}

/**
 * The reader of `runBidi`: one `io.next()` pending at a time, arriving frames queued in
 * order. While a turn runs, [`watch`] sets a watcher and [`pump`] (when a cancellable
 * closure starts) reads in the background, letting the watcher see each frame as soon
 * as it arrives (the frame still stays queued for the wait loop); outside a turn,
 * [`next`] takes from the queue first, then waits on the wire.
 */
class Inbox {
  private readonly queue: (Frame | null)[] = [];
  private pending: Promise<void> | null = null;
  private watcher: ((f: Frame) => void) | null = null;
  private readonly io: FrameStream;

  constructor(io: FrameStream) {
    this.io = io;
  }

  watch(see: (f: Frame) => void): void {
    this.watcher = see;
  }

  unwatch(): void {
    this.watcher = null;
  }

  async next(): Promise<Frame | null> {
    while (this.queue.length === 0) {
      this.pump();
      await this.pending;
    }
    return this.queue.shift()!;
  }

  pump(): void {
    if (this.pending !== null || this.queue.at(-1) === null) return;
    this.pending = this.io.next().then((f) => {
      this.pending = null;
      this.queue.push(f);
      if (f !== null && this.watcher !== null) {
        this.watcher(f);
        this.pump();
      }
    });
  }
}

/**
 * How long a suspension on a **still open** stream lasts before it is suspicious — see
 * the warning in [`runBidi`]. Sixty seconds is far too wide for any legitimate
 * persist/send latency (a *legitimate* wait longer than that would have made the runtime
 * close the stream), and narrow enough for the on-call person to still see it next to
 * the symptom.
 */
const SUSPEND_STALL_MS = 60_000;

/**
 * The turn loop on a `BIDI_STREAM` connection.
 *
 * ```text
 * runtime                                   SDK
 * ───────                                   ───
 * Start + journal replay      ──────►
 *                                           runs the handler
 *                             ◄──────────   commands, then Suspension
 * resolvable in place? →
 *   push notifications        ──────►
 *                                           reruns the handler from the start, on
 *                                           the local journal — no round trip,
 *                                           no journal received again
 *                             ◄──────────   ... and so on
 * external wait? → close stream             turn over, exactly like REQUEST_RESPONSE
 * ```
 *
 * Rerunning from the start instead of a real `await` is **deliberate for v1**: it uses
 * exactly the existing replay model, so there is no second code path to drift. Turning
 * `demand()` into async-await is a later optimisation, not a requirement.
 */
export async function runBidi(
  handler: AnyHandler,
  io: FrameStream,
  wireVersion: number = MAX_PROTOCOL_VERSION,
  values: ValueOptions = {},
): Promise<void> {
  const first = await io.next();
  if (first === null) return; // the runtime gave up before saying anything
  if (first.type !== MT.START) {
    io.write(badFirstMessage());
    io.end();
    return;
  }
  const start = decodeStart(first.payload);

  // Wire v6: the stream's URL table (opening turn: `Start.segment_urls`; mid-turn:
  // `SegmentUrls` frames ahead of the message carrying refs) and each frame's assembled copy.
  // As in [`runTurn`]: below v6 nothing is fetched and a ref is refused.
  const segments = new Segments(values, wireVersion);
  segments.addUrls(start.segmentUrls);
  /**
   * Takes a frame from the runtime into the local journal: `SegmentUrls` loads the URL
   * table; an entry with a segmented value is assembled **before** it goes into the
   * journal. Assembly fails ⇒ `Error` (retried) and the stream is closed — returns `false`.
   */
  const admit = async (f: Frame): Promise<boolean> => {
    if (f.type === MT.SEGMENT_URLS) {
      segments.addUrls(decodeSegmentUrls(f.payload));
      return true;
    }
    if (!isJournalEntry(f.type)) return true;
    try {
      await segments.admit(f);
    } catch (e) {
      io.write(onWire([valueFailure(e)]));
      io.end();
      return false;
    }
    entries.push(f);
    return true;
  };

  // The local journal. The runtime does **not** resend it in later turns on the same
  // stream — that is exactly the saving — so the SDK must keep it itself, and keep it
  // right: the runtime's replayed entries, the entries we emit, notifications that come later.
  const entries: Frame[] = [];
  for (let i = 0; i < start.knownEntries; i++) {
    const f = await io.next();
    if (f === null) return; // the stream broke during the replay
    // `SegmentUrls` is a side channel, it takes no slot among the replayed entries.
    if (f.type === MT.SEGMENT_URLS) i--;
    if (!(await admit(f))) return;
  }
  for (const f of io.buffered()) {
    noteCustomDropped(f);
    if (!(await admit(f))) return;
  }

  // Frames arriving **while** the turn runs are read at once (queued, keeping order for
  // the wait loop below): a mid-turn CANCEL must reach the running closure, without
  // waiting for the turn to suspend.
  const inbox = new Inbox(io);
  for (;;) {
    let current: Turn | null = null;
    inbox.watch((f) => {
      if (f.type === MT.SIGNAL && decodeSignal(f.payload).idx === CANCEL_SIGNAL) current?.cancel();
    });
    const turn = await runOnce(
      handler,
      start,
      entries,
      wireVersion,
      (f) => io.write(onWire(f)),
      true,
      (t) => {
        current = t;
        t.onCancellable = () => inbox.pump();
      },
      segments,
    );
    inbox.unwatch();
    io.write(onWire(turn.out.slice(turn.flushed)));
    turn.close();
    for (const f of turn.out) if (isJournalEntry(f.type)) entries.push(f);

    // A turn ends with exactly one of three (protocol-v1 §6). Two of the three stop for good.
    const spec = turn.suspendedOn;
    if (spec === null || turn.out[turn.out.length - 1]?.type !== MT.SUSPENSION) {
      io.end();
      return;
    }

    // Suspended — and **not closed**. The runtime either pushes down what we await, or
    // closes the stream because only the outside world can answer it.
    //
    // The clock here plays NO role in the protocol — it is a warning light for the most
    // silent failure of the two-copies-one-rule design: the runtime considers the
    // suspension satisfied (it keeps the stream open, waiting for us to go on) while we
    // do not. Both sides wait for each other, no error, no log. On a *legitimate*
    // external wait the runtime closes the stream, so "stream open + suspended this long"
    // almost certainly means the two copies of `suspension_satisfied` diverged — or the runtime is really stuck.
    const stalled = setTimeout(() => {
      process.emitWarning(
        `suspension has waited over ${SUSPEND_STALL_MS / 1000}s on a stream that is still open: the runtime may ` +
          `consider it satisfied while the SDK does not (do the two suspension_satisfied implementations disagree?)`,
      );
    }, SUSPEND_STALL_MS);
    stalled.unref();
    try {
      for (;;) {
        const woken = await inbox.next();
        if (woken === null) {
          io.end();
          return;
        }
        noteCustomDropped(woken);
        // An ack is not an entry (see [`isJournalEntry`]) — it is the **receipt** for an
        // entry we already built. Only on receiving it does the pre-built entry go into
        // the local journal; from this moment the next rerun may trust it, because the
        // runtime finished writing before sending.
        //
        // The runtime may also echo back the whole `RunCompletion` instead of an ack
        // (when the SDK does not ask for an ack). That path falls into the branch below
        // and is also right — the two copies share a completion key, so they merge into one.
        if (woken.type === MT.PROPOSE_RUN_COMPLETION_ACK) {
          const settled = turn.pendingRuns.get(decodeProposeRunAck(woken.payload));
          if (settled !== undefined) entries.push(settled);
        }
        if (!(await admit(woken))) return;
        // The next turn only runs when **every** proposal of this turn is durable:
        // parallel `ctx.run`s propose several, the suspension point names only one.
        if (
          suspensionSatisfied(spec, entries) &&
          suspensionSatisfied({ all: [...turn.pendingRuns.keys()] }, entries)
        ) {
          break;
        }
      }
    } finally {
      clearTimeout(stalled);
    }
  }
}

function safeJson(raw: Uint8Array): unknown {
  const text = dec.decode(raw);
  try {
    return JSON.parse(text);
  } catch {
    // Input that is not JSON is passed as the raw string — ingress accepts any body.
    return text;
  }
}

function finish(turn: Turn, output: unknown): void {
  turn.flushStreams();
  // A swallowed journal mismatch (see [`Turn.command`]): the outcome is 570, not output.
  if (turn.mismatch !== null) {
    fail(turn, new JournalMismatch(turn.mismatch));
    return;
  }
  // **A closure's transient error wins**, like the suspended flag below.
  if (turn.failedWith !== null) {
    fail(turn, turn.failedWith);
    return;
  }
  // **The suspended flag wins.** If user code accidentally wraps a ctx command in a
  // `try/catch` and swallows `Suspend`, we still suspend correctly — the turn's result is
  // decided by state, not by how exceptions propagate.
  if (turn.suspendedOn !== null) {
    turn.emit(MT.SUSPENSION, encodeSuspensionSpec(turn.suspendedOn));
    return;
  }
  terminate(turn, () => {
    const bytes = toJsonBytes(output);
    const reuse = turn.reuseFor(bytes);
    const payload = reuse.length === 0 && bytes.length > MAX_FRAME_LEN ? null : encodeOutput(bytes, "output", reuse);
    if (payload !== null && payload.length <= MAX_FRAME_LEN) return payload;
    const e = frameTooLarge("handler output", bytes.length);
    return encodeOutputFailure(e.code, e.message, "output");
  });
}

/**
 * Closes the invocation: `Output` (if not durable yet), then `End`.
 *
 * `End` is sent **every time**, even when `Output` is already in the journal —
 * protocol-v1 §10: a process dying after `Output` is durable but before sending `End`
 * would leave the invocation stuck forever at the very last step if we did not resend it.
 */
function terminate(turn: Turn, build: () => Uint8Array): void {
  if (!turn.output(build)) {
    // protocol-v1 §8.5: 570 is never retried — a retry only reproduces the same error. And
    // it is PAUSE, not FAIL: deploy the fixed version, then `resume`, and it carries on.
    turn.emit(MT.ERROR, encodeError(JOURNAL_MISMATCH, turn.mismatch ?? "", ERROR_PAUSE));
    return;
  }
  turn.emit(MT.END, encodeEnd());
}

function fail(turn: Turn, e: unknown): void {
  turn.flushStreams();
  // A closure's transient error wins over every other outcome except a journal mismatch:
  // the caller may have swallowed it and thrown something else (`TurnFailed`, an AI SDK error).
  if (turn.failedWith !== null && turn.mismatch === null && !turn.replayOnly) e = turn.failedWith;
  if (e instanceof ContinueAsNew && turn.mismatch === null && !turn.replayOnly) {
    // Commands of this turn (if any) go first; this frame ends the turn.
    turn.emit(MT.CONTINUE_AS_NEW, encodeContinueAsNew(toJsonBytes(e.input)));
    return;
  }
  if (turn.replayOnly) {
    // The replay report **replaces** every other ending: this turn must leave nothing
    // behind — no Error for the runtime to reschedule, no Output.
    if (turn.mismatch !== null) {
      const m = turn.mismatchAt;
      turn.emit(
        MT.REPLAY_REPORT,
        encodeReplayReport({
          matched: m?.at ?? 0,
          mismatch: true,
          expected: m?.expected,
          got: m?.got,
          message: turn.mismatch,
        }),
      );
      return;
    }
    if (e instanceof ReplayStop) {
      turn.emit(MT.REPLAY_REPORT, encodeReplayReport({ matched: e.at, mismatch: false }));
      return;
    }
  }
  if (turn.mismatch !== null) {
    // protocol-v1 §8.5: 570 is never retried — a retry only reproduces the same error. And
    // it is PAUSE, not FAIL: deploy the fixed version, then `resume`, and it carries on.
    turn.emit(MT.ERROR, encodeError(JOURNAL_MISMATCH, turn.mismatch, ERROR_PAUSE));
    return;
  }
  if (turn.suspendedOn !== null && turn.failedWith === null) {
    turn.emit(MT.SUSPENSION, encodeSuspensionSpec(turn.suspendedOn));
    return;
  }
  if (e instanceof TerminalError) {
    terminate(turn, () => encodeOutputFailure(e.code, e.message, "output"));
    return;
  }
  // Suspended waiting for a human: **not** an `Output`, because the invocation has not
  // ended — the journal stays as is so `/resume` continues from exactly this point.
  if (e instanceof PauseError) {
    turn.emit(MT.ERROR, encodeError(e.code, e.message, ERROR_PAUSE));
    return;
  }
  const message = e instanceof Error ? e.message : String(e);
  const stack = e instanceof Error ? (e.stack ?? "") : "";
  turn.emit(MT.ERROR, encodeError(500, `${message}\n${stack}`.trim(), ERROR_RETRY));
}
