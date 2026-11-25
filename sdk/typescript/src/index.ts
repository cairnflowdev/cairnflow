// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * TypeScript SDK for Cairnflow.
 *
 * ```ts
 * import { service, serve, type Context } from "@cairnflow/sdk";
 *
 * const orders = service("OrderService", {
 *   async submit(ctx: Context, input: { amount: number }) {
 *     const seen = (await ctx.get<number>("count")) ?? 0;
 *     await ctx.sleep(1000);
 *     const receipt = await ctx.run("charge", () => charge(input.amount));
 *     ctx.set("count", seen + 1);
 *     return { receipt };
 *   },
 * }, { type: "VIRTUAL_OBJECT" });
 *
 * await serve({ services: [orders], port: 9080 });
 * ```
 *
 * Three rules for user code (protocol-v1 §8.4), and the SDK can **not** enforce all three:
 *
 * 1. Do not call `Math.random()` — use `ctx.random()`.
 * 2. Do not read `Date.now()` directly — wrap it in `ctx.run`.
 * 3. Do not wrap `ctx` calls in a `try/catch` that swallows the error. The SDK has
 *    a defence (the suspended flag wins, see `handler.ts`) but such wrapping still
 *    makes the code hard to follow.
 */

export {
  type AnyHandler,
  type CallOptions,
  Context,
  DurableFuture,
  type DurablePromise,
  type FrameStream,
  type Handler,
  JournalMismatch,
  PauseError,
  RunFailure,
  type RunOptions,
  runBidi,
  runTurn,
  type ScheduleMark,
  ScopedContext,
  type ServiceContext,
  type Stream,
  type Attrs,
  type Log,
  type LogLevel,
  type Span,
  type Trace,
  type ServiceHandler,
  type SharedContext,
  type SharedHandler,
  Suspend,
  TerminalError,
  TurnFailed,
} from "./handler.ts";

export {
  DISCOVERY_V1,
  type Endpoint,
  type HandlerOptions,
  INVOCATION_V1,
  manifest,
  MAX_PROTOCOL_VERSION,
  MIN_PROTOCOL_VERSION,
  type OnMaxAttempts,
  type ProtocolMode,
  type RetryPolicy,
  serve,
  service,
  type ServeOptions,
  type ServiceDefinition,
  type ServiceType,
  workflow,
} from "./serve.ts";

export { ReplayCache, type ReplayCacheOptions } from "./replay.ts";
export {
  DEFAULT_VALUE_CACHE_BYTES,
  SegmentCache,
  SegmentedValueError,
  WIRE_SEGMENTED,
} from "./values.ts";

export {
  type CallPlacement,
  decodeFrames,
  encodeFrame,
  FLAG_REQUESTED_ACK,
  type Frame,
  FrameError,
  FrameSplitter,
  HEADER_LEN,
  JOURNAL_MISMATCH,
  MAX_FRAME_LEN,
  MT,
  PROTOCOL_VIOLATION,
} from "./wire.ts";
export {
  AlreadyInvoked,
  AttachTimeout,
  type AwakeableHandle,
  CairnHttpError,
  type CallOptions as ClientCallOptions,
  type CancelAck,
  type Client,
  connect,
  type ConnectOptions,
  InvocationFailed,
  type InvocationHandle,
  InvocationPaused,
  type InvocationStatusView,
  type OutputResult,
  type PromiseAck,
  type PromiseHandle,
  type PromiseView,
  readStream,
  type ReadStreamOptions,
  type ScheduleDefinition,
  type ScheduleLast,
  type ScheduleList,
  type SchedulesClient,
  type ScheduleTarget,
  type ScheduleView,
  type SendOptions as ClientSendOptions,
  type ServiceClient,
  type ServiceLike,
  type SignalAck,
  StreamError,
  type StreamEvent,
  type TargetOptions,
  type WorkflowClient,
} from "./client.ts";
export {
  base58Decode,
  DISCOVER_JTI,
  expectedJti,
  type IdentityClaims,
  IdentityError,
  IdentityVerifier,
  JWT_HEADER,
  KID_PREFIX,
  SCHEME_HEADER,
  SCHEME_V1,
} from "./identity.ts";
export { Transport, type TransportOptions } from "./transport.ts";
export {
  type FetchHandler,
  fetchHandler,
  type FetchHandlerOptions,
  type LambdaHttpEvent,
  type LambdaHttpResult,
  lambdaHandler,
} from "./fetch.ts";
export {
  type Approval,
  approvalIdOf,
  cairnChatTransport,
  type CairnChatTransportOptions,
  cairnMiddleware,
  type CairnMiddlewareOptions,
  cairnOnError,
  cairnTools,
  type CairnToolsOptions,
  CHAT_MAX_ATTEMPTS,
  chatObject,
  type ChatObjectOptions,
  type ChatRequest,
  type ChatTurn,
  type Denied,
  STALE_HISTORY,
  STALE_HISTORY_CODE,
  type StreamPart,
  uiChunks,
  type UIMessageStreamSource,
} from "./vercel.ts";
export {
  ARTIFACT_MAX_BYTES,
  type ArtifactRef,
  type Artifacts,
  configureArtifacts,
} from "./handler.ts";
