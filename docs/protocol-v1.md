# Cairnflow Service Protocol v1 — Specification

**Status:** Draft
**Wire version:** 1
**Discovery version:** 1

The protocol between the Cairnflow runtime and the SDKs that run user code.
The design is based on a study of the Restate Service Protocol V7, and its message numbers are
compatible with Restate's service protocol where the semantics match (P5).

The normative keywords **MUST**, **MUST NOT**, **SHOULD**, **MAY** have their RFC 2119 meaning.

---

## 1. Principles

**P1 — The journal is append-only.** An entry once written is never modified. Results arrive
later as notifications carrying an explicit id.

**P2 — Every source of non-determinism must be named in the protocol.** Randomness, time,
external I/O — one mechanism each. Nothing is left to the SDK.

**P3 — Replay order is a contract.** On replay, notifications MUST appear in the same relative
order as on the first run.

**P4 — Leave room before it is needed.** Fields and numbers set aside for v2 are marked
`reserved`. Burning a version will happen; the goal is to be able to burn one without killing
users.

**P5 — Keep Restate-compatible numbers where the semantics match.** It costs nothing, and keeps
the door open to using their SDKs later.

---

## 2. Version negotiation — two independent axes

| Axis | Mechanism | v1 value |
|---|---|---|
| **Discovery** | Content-type when calling `/discover` | `application/vnd.cairnflow.endpointmanifest.v1+json` |
| **Wire** | The manifest declares `minProtocolVersion` / `maxProtocolVersion` | `1` |

The runtime sends an `Accept` header listing every discovery version it supports. The SDK
picks one and returns it in `Content-Type`.

The manifest declares the range of wire versions the SDK supports; the runtime picks **the
largest value inside both ranges**. No overlap ⇒ the deployment is refused at registration,
**not** at invoke time.

---

## 3. Discovery

`GET {endpoint}/discover`

```
Accept: application/vnd.cairnflow.endpointmanifest.v1+json
```

Response — the v1 manifest:

```jsonc
{
  "protocolMode": "BIDI_STREAM",   // or "REQUEST_RESPONSE"
  "minProtocolVersion": 1,
  "maxProtocolVersion": 1,
  "services": [
    {
      "name": "OrderService",
      "ty": "SERVICE",             // SERVICE | VIRTUAL_OBJECT | WORKFLOW
      "handlers": [
        {
          "name": "submit",
          // VIRTUAL_OBJECT: EXCLUSIVE | SHARED (absent ⇒ EXCLUSIVE)
          // WORKFLOW:       WORKFLOW | SHARED (absent ⇒ SHARED)
          "ty": "SHARED",
          "input":  { "contentType": "application/json" },
          "output": { "contentType": "application/json" }
        }
      ]
    }
  ]
}
```

**`WORKFLOW`**: a service of this kind must have **exactly one** handler with
`ty: "WORKFLOW"` — the run's id is `hash(scope, service, key)` and does not carry the handler
name, so two `WORKFLOW` handlers would share one identity. The runtime refuses such a
manifest *at registration*. A handler without `ty` in a workflow is `SHARED`. In a
`VIRTUAL_OBJECT`, a missing `ty` is `EXCLUSIVE` (holds the VO lock, as in Restate; before
2026-10-17 it meant "no kind" and took no lock). In a `SERVICE`, a missing `ty` means "no
kind".

`documentation`/`metadata` are reserved for v2.

---

## 4. Framing

Each message: **an 8-byte big-endian header**, followed by a protobuf payload.

```
 63          48 47                    32 31                  0
┌──────────────┬────────────────────────┬────────────────────┐
│  type (u16)  │         flags          │    length (u32)    │
└──────────────┴────────────────────────┴────────────────────┘

flags:
  bit 47 (0x8000_0000_0000) = REQUESTED_ACK
  bit 46..32                = reserved, MUST be 0
```

`length` is the number of payload bytes, **not** counting the header.

### Length limit

`length` is a `u32`, so a broken or malicious peer can declare 4 GiB. Each side MUST have a
limit and refuse a frame above it **as soon as it reads the header**, before waiting for the
whole payload.

Default limit: **16 MiB**. Exceeded ⇒ `PROTOCOL_VIOLATION` (571).

The exact number is normative, not a suggestion: if the two sides pick different values, a
message that is valid for one is refused by the other, and that error only shows with large
payloads — that is, in production, on exactly the most important invocations.

### The `REQUESTED_ACK` flag

| Set on | Meaning |
|---|---|
| Command | The runtime MUST send back a `CommandAckMessage` once the entry is durable |
| `ProposeRunCompletion` | The runtime sends a `ProposeRunCompletionAckMessage` instead of the full notification |

Only valid on messages with the `ack` column in section 5. Set anywhere else ⇒ `PROTOCOL_VIOLATION`.

### Transport

- `BIDI_STREAM`: HTTP/2, `POST {endpoint}/invoke/{service}/{handler}`.
  Request body = the runtime→SDK message stream. Response body = the SDK→runtime message
  stream. Both directions run concurrently.
- `REQUEST_RESPONSE`: HTTP/1.1 or Lambda. The runtime sends the whole journal in the request
  body, the SDK returns its whole response. No message comes from the runtime after the request
  has started ⇒ the SDK MUST NOT use `REQUESTED_ACK`, and every `ctx.run` MUST end with a
  suspension or an output in the same turn.

Both modes use the content-type **`application/vnd.cairnflow.invocation.v1`** for the request
and response bodies of `/invoke`. The runtime sends it in both `Content-Type` and `Accept`; the
SDK MUST return exactly that value.

### Request identity

The runtime has an identity master that signs every `GET /discover` and `POST /invoke/…` with
the Ed25519 key **of the tenant** that owns the deployment, in HTTP headers — no frame changes:

- `x-cairn-signature-scheme: v1`;
- `x-cairn-jwt-v1`: a JWT, header `{"alg":"EdDSA","typ":"JWT","kid":"publickeyv1_<base58 public key>"}`,
  claims `aud` (the request path, including the endpoint's path prefix), `sub` (tenant), `iat`,
  `nbf` = `iat − 60`, `exp` = `iat + 60` (Unix seconds), `jti`, `host` (the request's `Host`),
  on `/invoke` `bsh` (base64url of the sha256 of the body before compression; `BIDI_STREAM`: the
  turn-opening body), and `nonce`.
- The `jti` of `/invoke` = `<hex id of Start.id>:<Start.retry_count_since_last_stored_entry>` —
  the SDK compares it with the `Start` of **this very** request: a JWT copied onto the request
  of another invocation (or another attempt) does not pass. The `jti` of `/discover` =
  `discover`. So `jti` is **not** unique per request: resuming a paused invocation twice
  within one second, with no journal growth, sends the same `Start` twice.
- `nonce` = base64url of 16 random bytes, fresh for **every** signed request. It is what makes
  two requests' JWTs differ (Ed25519 is deterministic: same claims ⇒ same signature). The SDK
  does not need to read it; a `BIDI_STREAM` SDK lets one JWT open one stream, keyed by the
  JWT's signature, and the `nonce` guarantees the runtime never needs a second stream with the
  same one. Older runtimes do not send it.
- An SDK MUST ignore claims it does not know (the runtime adds claims without a new scheme).

An SDK with verification on (a list of trusted `kid`s) MUST return `401` for a request with a
missing or wrong signature before running the handler; in `BIDI_STREAM` the `jti` can only be
compared once `Start` arrives (after the response headers were sent), so a `jti` mismatch ⇒ the
SDK closes the stream (`RST_STREAM`) without sending any frame. A verifying SDK declares
`"requestIdentity": "v1"` in the manifest (section 3); the runtime records it in the deployment.
Golden vector: `cairn-server/src/identity.rs::golden_vector_is_stable`, re-verified in
`sdk/typescript/test/identity.test.ts`, `sdk/go/internal/identity/identity_test.go` and
`sdk/python/tests/test_endpoint.py`.

In `REQUEST_RESPONSE`, leftover bytes that do not form a frame at the end of the response body
are an **error**, not "wait for more" — the body has ended, so there is nothing left to wait
for. In `BIDI_STREAM` it is the opposite: the body has not ended, so a half frame left over is
normal and only becomes an error when the stream closes.

#### `BIDI_STREAM` is h2c prior-knowledge

HTTP/2 **cleartext, with prior knowledge** — no `Upgrade:`, no ALPN. Two directions over one
HTTP/1.1 connection are not reliable (clients do not guarantee full duplex on h1), so h1 only
serves `REQUEST_RESPONSE`.

`/discover` **always** runs over HTTP/1.1, in both modes: the manifest is read *before* anyone
knows which mode the deployment declares, so it must not depend on its own answer. An SDK
serves both on **one port** by sniffing the 24-byte preface `PRI * HTTP/2.0…`.

The SDK MUST return the response headers **as soon as it receives the request headers**,
before reading any byte of the body. The runtime takes the response headers as the sign that
"the stream is open"; an SDK that waits to read the whole `Start` before returning headers
means both sides wait for each other.

#### Turns on one stream

`BIDI_STREAM` keeps the notion of a **turn**; it only drops the constraint "each turn is one
HTTP request". When the SDK sends `Suspension`:

- The runtime makes exactly the check of section 8.3 — whether what the SDK waits for has a
  notification yet. **Yes** ⇒ it pushes the notification into the stream and **keeps** the
  stream open. **Not yet** (every leaf waits on the outside world) ⇒ it closes the stream; the
  turn ends exactly as in `REQUEST_RESPONSE`.
- After sending `Suspension` the SDK **MUST NOT close**. It keeps reading; when a frame arrives
  it loads it into the local journal and reruns the handler from the start. Stream closed ⇒ the
  turn is over.
- "The runtime closes the stream" means **END_STREAM on the request body**, not `RST_STREAM`.
  When the SDK sees the request body end it MUST close its half at once. The runtime waits for
  that at most `BIDI_CLOSE_GRACE` (2 s, `invoker.rs`) before `RST_STREAM(CANCEL)`. Reason: h2
  servers count the RSTs they receive — nghttp2 (`node:http2`) allows 1 000 per connection,
  refilled at 33/s (rapid-reset protection, CVE-2023-44487), then kills the whole connection —
  so one RST per suspended turn means every open turn on that connection fails, sooner or
  later, above 33 suspended turns/s.

The runtime does **NOT** resend `Start` and the journal replay on later turns of the same
stream — the SDK already has them. It only sends the new entries. The direct consequence for
the SDK: the local journal is the source of truth, and it MUST also record there the entries
the runtime creates on its behalf — namely the `RunCompletion` that comes with each
`ProposeRunCompletion`.

But **when** to record it is what matters. The SDK builds the `RunCompletion` as soon as it
proposes and **keeps it out of the journal** until the `ProposeRunCompletionAck` arrives; after
a proposal, the turn ends with `Suspension`. Putting it into the journal earlier lets the next
rerun trust a result the runtime may not have written yet — and that window does not stop at
the next command, it reaches the **next suspension point**: three `ctx.run`s in a row with the
process dying at the third means all three rerun. The ack is only sent after the commit
(`Effect::Persist` before `Effect::Send`), so waiting for the ack is waiting for exactly the
right thing.

`CommandAck`, `ProposeRunCompletionAck` and `0xFC00+` are **not** journal entries. Recording
them in the local journal shifts the index of every later entry.

The journal of an invocation **does not depend on the mode**: same program ⇒ same entries, same
order, byte for byte. That is what lets an invocation start in one mode and continue on a node
that only speaks the other.

---

## 5. Message type table

### Control — `0x0000`–`0x000F`

| Code | Message | Direction | ack |
|---|---|---|---|
| `0x0000` | `Start` | → SDK | |
| `0x0001` | `Suspension` | → runtime | |
| `0x0002` | `Error` | → runtime | |
| `0x0003` | `End` | → runtime | |
| `0x0004` | `CommandAck` | → SDK | |
| `0x0005` | `ProposeRunCompletion` | → runtime | ✓ |
| `0x0006` | *reserved* (`AwaitingOn`, v2) | | |
| `0x0007` | `ProposeRunCompletionAck` | → SDK | |
| `0x0008` | `Keepalive` *(v3)* | → runtime | |
| `0x0009` | `ReplayReport` *(v3)* | → runtime | |
| `0x000A` | `ContinueAsNew` *(v3)* | → runtime | |
| `0x000B` | `StreamAppend` *(v4, side channel)* | → runtime | |
| `0x000C` | `ArtifactPut` *(v4, side channel)* | → runtime | |
| `0x000D` | `SegmentUrls` *(v6, side channel)* | → SDK | |

### Command — `0x0400`+

| Code | Command | Completion | ack |
|---|---|---|---|
| `0x0400` | `Input` | — | ✓ |
| `0x0401` | `Output` | — | ✓ |
| `0x0402` | `GetState` | `0x8002` | ✓ |
| `0x0403` | `SetState` | — | ✓ |
| `0x0404` | `ClearState` | — | ✓ |
| `0x0405` | `ClearAllState` | — | ✓ |
| `0x0406` | `GetStateKeys` | `0x8006` | ✓ |
| `0x0407` | `GetEagerState` | — (result embedded) | ✓ |
| `0x0408` | `GetEagerStateKeys` | — (result embedded) | ✓ |
| `0x0409` | `GetPromise` | `0x8009` | ✓ |
| `0x040A` | `PeekPromise` | `0x800A` | ✓ |
| `0x040B` | `CompletePromise` | `0x800B` | ✓ |
| `0x040C` | `Sleep` | `0x800C` | ✓ |
| `0x040D` | `Call` | `0x800D` + `0x800E` | ✓ |
| `0x040E` | `OneWayCall` | `0x800E` | ✓ |
| `0x040F` | *reserved* | | |
| `0x0410` | `SendSignal` | — | ✓ |
| `0x0411` | `Run` | `0x8011` | ✓ |
| `0x0412` | `AttachInvocation` | `0x8012` | ✓ |
| `0x0413` | `GetInvocationOutput` | `0x8013` | ✓ |
| `0x0414` | *reserved* | | |

### Notification — `0x8000`+ and `0xFBFF`

| Code | Notification |
|---|---|
| `0x8002` | `GetStateCompletionNotification` |
| `0x8006` | `GetStateKeysCompletionNotification` |
| `0x8009` | `GetPromiseCompletionNotification` |
| `0x800A` | `PeekPromiseCompletionNotification` |
| `0x800B` | `CompletePromiseCompletionNotification` |
| `0x800C` | `SleepCompletionNotification` |
| `0x800D` | `CallCompletionNotification` |
| `0x800E` | `CallInvocationIdCompletionNotification` |
| `0x8011` | `RunCompletionNotification` |
| `0x8012` | `AttachInvocationCompletionNotification` |
| `0x8013` | `GetInvocationOutputCompletionNotification` |
| `0xFBFF` | `SignalNotification` |

**Convention:** the completion of command `0x04XX` has code `0x80XX`. Bit `0x8000` tells
notifications from commands — a parser knows from the header alone.

### Custom — `0xFC00`+

The runtime forwards the bytes as is, without parsing. Reserved for third-party extensions.

---

## 6. Invocation lifecycle

```
runtime                                   SDK
   │  StartMessage                         │
   ├──────────────────────────────────────►│
   │  {known_entries} existing entries      │   ─┐
   ├──────────────────────────────────────►│    │ REPLAY phase
   │  ...                                   │   ─┘
   │                                        │
   │◄──────────────────────────────────────┤   ─┐
   │  new commands                          │    │ PROCESSING phase
   │──────────────────────────────────────►│    │
   │  notifications                         │   ─┘
   │                                        │
   │◄──────────────────────────────────────┤
   │  Output + End   |   Suspension   |   Error
```

**REPLAY phase** — the runtime sends the `known_entries` stored entries. The SDK reruns the
user code, each command matching the next entry: the same entry kind and, for `Run`, the same
`name` (same kind with a different name is a different step — a journal out of place or a
renamed step; matching it would rerun the closure under the old name, or hand one step's result
to another). No match ⇒ `JOURNAL_MISMATCH` (570).
Trace: `conformance/traces/run-renamed-rr-v1.json`.

**PROCESSING phase** — the SDK sends new commands, the runtime stores them and returns
notifications.

The SDK MUST NOT distinguish the two phases towards user code. The boundary is an internal
detail only.

It ends with exactly one of three: `Output`+`End`, `Suspension`, or `Error`.

---

## 7. Protobuf definitions

```protobuf
// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate service protocol, which is
// released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/service-protocol/blob/main/LICENSE
//
// Modifications copyright (c) 2026 Cairnflow, released under the MIT license.
//
// SPDX-License-Identifier: MIT

syntax = "proto3";

package dev.cairnflow.service.protocol;

option java_package  = "dev.cairnflow.generated.service.protocol";
option go_package    = "cairnflow.dev/sdk-go/pb/service/protocol";

enum ProtocolVersion {
  PROTOCOL_VERSION_UNSPECIFIED = 0;
  V1 = 1;
}

// ============ Control ============

// Type: 0x0000
message StartMessage {
  message StateEntry {
    bytes key = 1;
    // If value is an empty byte array,
    // then it means the value is empty and not "missing" (e.g. empty string).
    bytes value = 2;
  }

  // Unique id of the invocation. This id is unique across invocations and won't change when replaying the journal.
  bytes  id = 1;
  // Invocation id that can be used for logging.
  // The user can use this id to address this invocation in admin and status introspection apis.
  string debug_id = 2;
  // Total number of commands + notifications stored.
  uint32 known_entries = 3;

  // Eager state (section 9, now open): the object's whole state, sent along so the SDK
  // answers `ctx.get` locally, without a round trip.
  repeated StateEntry state_map = 4;
  // `true` means `state_map` is only PART of the state — absent keys must be
  // read lazily (`GetState`). `false` + an empty map is indistinguishable from
  // runtime v1 on the wire, so the SDK may treat the map as complete only when
  // it is not empty.
  bool partial_state = 5;

  // If this invocation has a key associated (e.g. for objects and workflows), then this key is filled in. Empty otherwise.
  string key = 6;

  // Retry count since the last stored entry.
  //
  // Please note that this count might not be accurate, as it's not durably stored,
  // thus it might get reset in case Restate crashes/changes leader.
  uint32 retry_count_since_last_stored_entry = 7;
  // Duration since the last stored entry, in milliseconds.
  //
  // Please note this duration might not be accurate,
  // and might change depending on which Restate replica executes the request.
  uint64 duration_since_last_stored_entry = 8;

  // Seed for the SDK's deterministic RNG. STABLE across every replay.
  uint64 random_seed = 9;

  optional string idempotency_key = 10;

  reserved 11 to 15;  // scope, limit_key — v2

  // ── v3 ──
  // How often the SDK sends Keepalive (0x0008) while a ctx.run closure is still
  // running, ms. Absent/0 = do not send (the runtime sets no cap, or speaks v2).
  optional uint64 keepalive_interval_ms = 16;
  // A trial replay: when the SDK meets a command beyond the journal it stops
  // BEFORE running the closure and reports how far it matched — no side effect
  // runs (section 9.1).
  bool replay_only = 17;

  // ── v5 ──
  // Incremental replay for REQUEST_RESPONSE (section 9.3): the request body OMITS
  // the first `replay_from` entries of the journal; the SDK splices them back from
  // its cache when `prefix_digest` (a SHA-256 chain over the frames of entries
  // [0, replay_from)) matches, otherwise it returns a 409 miss. 0/absent = a full
  // replay as in v4 — the bytes of Start stay unchanged.
  uint32 replay_from = 18;
  bytes prefix_digest = 19;
  // v6: the read URLs of every segment ref in the messages of this request/turn
  // opening — sent outside the entries so the entry bytes (and the replay
  // digest) do not change with the URL. Sent only to deployments that settled on
  // v6.
  repeated SegmentUrl segment_urls = 20;
}

// Type: 0x0001
// Implementations MUST send this message when suspending an invocation.
//
// The flat list (v1) means "wake when ANY of them completes". Fields 4-5 (now
// open) express a combinator tree: every node is a child SuspensionMessage. A
// node is satisfied when:
//   FIRST_COMPLETED — any leaf or child node is satisfied;
//   ALL_COMPLETED   — every leaf AND every child node is satisfied.
// COMBINATOR_TYPE_UNKNOWN = 0 is read as FIRST_COMPLETED, so a v1 message is
// still understood correctly without a version bump.
message SuspensionMessage {
  repeated uint32 waiting_completions   = 1;
  repeated uint32 waiting_signals       = 2;
  repeated string waiting_named_signals = 3;

  repeated SuspensionMessage nested_futures = 4;
  CombinatorType combinator_type = 5;
}

enum CombinatorType {
  // Read as FIRST_COMPLETED — keeps compatibility with v1 messages.
  COMBINATOR_TYPE_UNKNOWN = 0;
  FIRST_COMPLETED = 1;
  ALL_COMPLETED   = 2;
}

// Type: 0x0002
message ErrorMessage {
  // The code can be any HTTP status code, as described https://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml.
  // In addition, we define the following error codes that MAY be used by the SDK for better error reporting:
  // * JOURNAL_MISMATCH = 570, that is when the SDK cannot replay a journal due to the mismatch between the journal and the actual code.
  // * PROTOCOL_VIOLATION = 571, that is when the SDK receives an unexpected message or an expected message variant, given its state.
  uint32 code       = 1;
  string message    = 2;
  string stacktrace = 3;

  // Entry that caused the failure. This may be outside the current stored journal size.
  optional uint32 related_command_index = 4;
  optional string related_command_name  = 5;
  optional uint32 related_command_type  = 6;

  reserved 7;

  // Delay before executing the next retry, specified as duration in milliseconds.
  // If provided, it will override the default retry policy used by Restate's invoker ONLY for the next retry attempt.
  // Only meaningful when behavior = RETRY.
  optional uint64 next_retry_delay = 8;

  ErrorBehavior behavior = 9;
}

enum ErrorBehavior {
  // The value 0 is DELIBERATE: an unset field must be read as RETRY.
  RETRY = 0;
  PAUSE = 1;
  FAIL  = 2;
}

// Type: 0x0003
message EndMessage {}

// Type: 0x0004
message CommandAckMessage {
  uint32 command_index = 1;
}

// Type: 0x0005
// Proposes the result of ctx.run to the runtime.
// NOT journaled right away; it appears later as a notification.
// See section 8.2 for the ordering guarantee.
message ProposeRunCompletionMessage {
  uint32 result_completion_id = 1;
  oneof result {
    bytes   value   = 14;
    Failure failure = 15;
    // Wire v6: the result carries reused refs + a tail. The runtime also writes
    // a large result of an SDK ≤ v5 into this field (a sealed frame) before it
    // reaches the state machine. SDKs below v6 must not send it.
    Value   segmented_value = 16;
  }
}

// Type: 0x0007
// Sent only in reply to a ProposeRunCompletionMessage, and ONLY in the PROCESSING phase.
message ProposeRunCompletionAckMessage {
  uint32 completion_id = 1;
}

// Type: 0x0008 (v3) — SDK → runtime, empty payload: a ctx.run closure is still
// running. Does not go into the journal.
message KeepaliveMessage {}

// Type: 0x0009 (v3) — SDK → runtime, ends a replay_only turn. The code matched
// the journal up to command number `matched` (stopping BEFORE the first command
// beyond the journal — no closure ran), or diverged at command `matched`.
message ReplayReportMessage {
  uint32 matched = 1;
  bool mismatch = 2;
  string expected = 3;
  string got = 4;
  string message = 5;
}

// Type: 0x000A (v3) — SDK → runtime, ends the turn: this invocation ends and a
// NEW invocation of the same target starts with `input` and an empty journal — a
// long loop does not bloat the journal (ctx.continueAsNew).
message ContinueAsNewMessage {
  bytes input = 1;
}

// Type: 0x000B (v4) — SDK → runtime, SIDE CHANNEL: a record of a stream. It never
// becomes a journal entry, waits for no durable ack, and shifts no entry index.
// The SDK batches appends per 50 ms or 1 KiB. Sent only once wire v4 is
// settled; below v4 the runtime returns 571 as for an unknown type.
message StreamAppendMessage {
  // The stream name. A leading `$` is reserved for system streams (`$log`, `$trace`).
  string name = 1;
  // A durable stream (chunk rows in the Db, riding on existing commits) or an
  // ephemeral one (the leader's RAM only). A stream keeps one kind for life.
  bool durable = 2;
  repeated StreamRecord records = 3;
  // The writer closes the stream: no record after it.
  bool close = 4;
}

// One stream record. The identity `(step, attempt, k)` is unique within a
// stream (AP-11): `step` = the journal length the SDK knew when appending,
// `attempt` = the turn's `retry_count_since_last_stored_entry`, `k` = the order
// within the step. The offset is assigned by the runtime.
message StreamRecord {
  uint32 step = 1;
  uint32 attempt = 2;
  uint32 k = 3;
  bytes payload = 4;
}

// Type: 0x000C (v4) — SDK → runtime, side channel: an artifact `content` with
// sha256 `sha256`. The runtime finishes the PUT before handling the next frame
// (AP-1); the ref (`sha256`) is usable in later commands.
message ArtifactPutMessage {
  bytes sha256 = 1;
  bytes content = 2;
  string content_type = 3;
}

// Type: 0x000D (v6) — runtime → SDK, BIDI side channel: the read URLs of the
// segment refs in the next message. No entry, no ack.
message SegmentUrlsMessage {
  repeated SegmentUrl urls = 1;
}

// ============ Notification duck-type ============
//
// This message is NEVER sent. It only documents the field layout every
// notification follows, so that a parser can read them all one way.
message NotificationTemplate {
  oneof id {
    uint32 completion_id = 1;
    uint32 signal_id     = 2;
    string signal_name   = 3;
  }
  oneof result {
    Void    void          = 4;
    Value   value         = 5;
    Failure failure       = 6;
    string  invocation_id = 16;
    StateKeys state_keys  = 17;
  }
}

// ============ Input / Output ============

// Type: 0x0400 — Completable: No
message InputCommandMessage {
  repeated Header headers = 1;
  Value  value = 14;
  string name  = 12;
}

// Type: 0x0401 — Completable: No
message OutputCommandMessage {
  oneof result {
    Value   value   = 14;
    Failure failure = 15;
  }
  string name = 12;
}

// ============ State ============

// Type: 0x0402 — Completable: Yes
message GetStateCommandMessage {
  bytes  key = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x8002
message GetStateCompletionNotificationMessage {
  reserved 2, 3, 6, 16;
  uint32 completion_id = 1;
  oneof result {
    Void  void  = 4;   // key does not exist
    Value value = 5;
  }
}

// Type: 0x0403 — Completable: No
message SetStateCommandMessage {
  bytes key   = 1;
  Value value = 3;
  string name = 12;
}

// Type: 0x0404 — Completable: No
message ClearStateCommandMessage {
  bytes  key  = 1;
  string name = 12;
}

// Type: 0x0405 — Completable: No
message ClearAllStateCommandMessage {
  string name = 12;
}

// Type: 0x0406 — Completable: Yes
message GetStateKeysCommandMessage {
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x8006
message GetStateKeysCompletionNotificationMessage {
  reserved 2 to 8, 16;
  uint32 completion_id = 1;
  // Field 17 matches `NotificationTemplate.state_keys` — it is not in the
  // oneof, because this command cannot fail.
  StateKeys state_keys = 17;
}

// Type: 0x0407 — Completable: NO — the result is embedded in the command itself.
// The SDK reads from `StartMessage.state_map` and records what it saw, so a
// replay returns exactly the value it read, not the current value.
message GetEagerStateCommandMessage {
  bytes key = 1;

  oneof result {
    Void  void  = 13;  // key did not exist when read
    Value value = 14;
  }

  string name = 12;
}

// Type: 0x0408 — Completable: No, for the same reason as 0x0407.
message GetEagerStateKeysCommandMessage {
  StateKeys value = 14;
  string name = 12;
}

// ============ Promise (workflow) ============
//
// A durable promise keyed by `(service, object key, promise name)` — it lives in
// the same partition as the object and can be completed from another handler of
// the same object.

// Type: 0x0409 — Completable: Yes. Waits until the promise is completed.
message GetPromiseCommandMessage {
  string key = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x8009
message GetPromiseCompletionNotificationMessage {
  reserved 2, 3, 4, 16, 17;
  uint32 completion_id = 1;
  oneof result {
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0x040A — Completable: Yes. Looks without waiting.
message PeekPromiseCommandMessage {
  string key = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x800A
message PeekPromiseCompletionNotificationMessage {
  reserved 2, 3, 16, 17;
  uint32 completion_id = 1;
  oneof result {
    Void    void    = 4;  // promise not completed yet
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0x040B — Completable: Yes
message CompletePromiseCommandMessage {
  string key = 1;

  oneof completion {
    Value   completion_value   = 2;
    Failure completion_failure = 3;
  }

  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x800B
message CompletePromiseCompletionNotificationMessage {
  reserved 2, 3, 5, 16, 17;
  uint32 completion_id = 1;
  oneof result {
    Void    void    = 4;  // the completion won
    Failure failure = 6;  // the promise already had a result
  }
}

// ============ Syscall ============

// Type: 0x040C — Completable: Yes
message SleepCommandMessage {
  // Wake up time.
  // The time is set as duration since UNIX Epoch.
  // (An ABSOLUTE instant in milliseconds since the epoch, NOT a relative duration.)
  uint64 wake_up_time = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x800C
message SleepCompletionNotificationMessage {
  reserved 2, 3, 5, 6, 16;
  uint32 completion_id = 1;
  Void   void = 4;
}

// Type: 0x040D — Completable: Yes (TWO notifications)
// Notification 1: invocation_id, arrives at once           → invocation_id_notification_idx
// Notification 2: the result, arrives when the callee ends → result_completion_id
message CallCommandMessage {
  string service_name = 1;
  string handler_name = 2;
  bytes  parameter    = 3;
  repeated Header headers = 4;
  // Key of the target virtual object. Empty if there is none.
  string key = 5;
  optional string idempotency_key = 6;

  // Flow control (v2). The field is `optional` so an old SDK and a new runtime
  // can talk — but the other direction is **silently wrong**: a new SDK sending
  // a scope to an old runtime has the field ignored as unknown, and the call
  // runs WITHOUT any cap. That is why this is an axis of the version
  // negotiation at `/discover`, not a field added quietly.
  optional string scope     = 7;
  optional string limit_key = 8;

  uint32 invocation_id_notification_idx = 10;
  uint32 result_completion_id           = 11;
  string name = 12;

  // Placement of an unkeyed callee (section 9, no wire bump). Ignored when there is a key.
  CallPlacement placement = 13;

  // The callee's user tags (section 9, no wire bump): into its metadata, like the
  // `cairnflow-tag` header at ingress. Tags breaking the rules (key
  // `[a-z0-9_.-]{1,32}`, value ≤ 64 B, ≤ 8 tags) are dropped, the call still runs.
  repeated Header tags = 14;
}

enum CallPlacement {
  CALL_PLACEMENT_DEFAULT = 0;  // colocated by the caller's pk prefix (calls/prefix gate)
  CALL_PLACEMENT_SPREAD  = 1;  // spread: H(caller, completion) even with the colocating gate on
}

// Type: 0x800D
message CallCompletionNotificationMessage {
  reserved 2, 3, 4, 16;
  uint32 completion_id = 1;
  oneof result {
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0x800E — used for both Call and OneWayCall
message CallInvocationIdCompletionNotificationMessage {
  reserved 2, 3, 4, 5, 6;
  uint32 completion_id = 1;
  string invocation_id = 16;
}

// Type: 0x040E — Completable: Yes (the invocation_id notification only)
message OneWayCallCommandMessage {
  string service_name = 1;
  string handler_name = 2;
  bytes  parameter    = 3;
  // Time when this BackgroundInvoke should be executed.
  // The time is set as duration since UNIX Epoch.
  // If this value is not set, equal to 0, or past in time,
  // the runtime will execute this BackgroundInvoke as soon as possible.
  uint64 invoke_time = 4;
  repeated Header headers = 5;
  string key = 6;
  optional string idempotency_key = 7;

  // Flow control (v2) — see the comment on `CallCommandMessage`.
  optional string scope     = 8;
  optional string limit_key = 9;

  uint32 invocation_id_notification_idx = 10;
  string name = 12;

  CallPlacement placement = 13;  // see CallCommandMessage
  repeated Header tags = 14;     // see CallCommandMessage
}

// Type: 0x0410 — Completable: No, Fallible: Yes
message SendSignalCommandMessage {
  string target_invocation_id = 1;
  oneof signal_id {
    uint32 idx  = 2;
    string name = 3;
  }
  oneof result {
    Void    void    = 4;
    Value   value   = 5;
    Failure failure = 6;
  }
  // Cannot use the name 'name' because it is already used above.
  string entry_name = 12;
}

// Type: 0x0411 — Completable: Yes
// The result is sent through ProposeRunCompletionMessage, not inside this command.
message RunCommandMessage {
  uint32 result_completion_id = 11;
  string name = 12;
}

// ============ Attach / GetInvocationOutput ============

// Type: 0x0412 — Completable: Yes. Waits for the result of another invocation.
// Field 1 stands alone but is numbered like the future oneof `target`
// (idempotent request, workflow) — adding it later does not change the wire.
message AttachInvocationCommandMessage {
  string invocation_id = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x8012
message AttachInvocationCompletionNotificationMessage {
  reserved 2, 3, 4, 16, 17;
  uint32 completion_id = 1;
  oneof result {
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0x0413 — Completable: Yes. Looks at the result without waiting.
message GetInvocationOutputCommandMessage {
  string invocation_id = 1;
  uint32 result_completion_id = 11;
  string name = 12;
}

// Type: 0x8013
message GetInvocationOutputCompletionNotificationMessage {
  reserved 2, 3, 16, 17;
  uint32 completion_id = 1;
  oneof result {
    Void    void    = 4;  // not finished yet
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0x8011
message RunCompletionNotificationMessage {
  reserved 2, 3, 4, 16;
  uint32 completion_id = 1;
  oneof result {
    Value   value   = 5;
    Failure failure = 6;
  }
}

// Type: 0xFBFF
message SignalNotificationMessage {
  reserved 1, 16;
  oneof signal_id {
    uint32 idx  = 2;
    string name = 3;
  }
  oneof result {
    Void    void    = 4;
    Value   value   = 5;
    Failure failure = 6;
  }
}

// ============ Nested types ============

message StateKeys { repeated bytes keys = 1; }
// Empty `segments` ⇒ inline: `content` is the whole value, `len` = 0 (bytes
// identical to the old layout `{ bytes content = 1; }`). Non-empty `segments` ⇒
// segmented: the value = the segments concatenated, then `content` (the tail),
// `len` = the total. The runtime only sends segmented values to an SDK that
// declares wire v6; an SDK ≤ v5 sending `segments` is a violation (571).
message Value   {
  bytes content = 1;
  repeated SegmentRef segments = 2;
  uint64 len = 3;
}
// An immutable segment; `gen` is new at every seal.
message SegmentRef { bytes sha256 = 1; uint64 len = 2; fixed64 gen = 3; }
// A short-lived GET URL for segment `(sha256, gen)` (v6): presigned by the
// store (`direct`) or a node token (`proxy`).
message SegmentUrl { bytes sha256 = 1; fixed64 gen = 2; string url = 3; }
message Void    {}
message Header  { string key = 1; string value = 2; }

message Failure {
  uint32 code    = 1;
  string message = 2;
  repeated FailureMetadata metadata = 3;
}

message FailureMetadata { string key = 1; string value = 2; }

enum BuiltInSignal {
  SIGNAL_UNKNOWN = 0;
  CANCEL         = 1;
  reserved 2 to 15;   // reserved for future system signals
}
```

---

## 8. Normative semantics

### 8.1 Journal and completion ids

- Command indexes count from 0, in the order entries are written to the journal.
- `result_completion_id` and `invocation_id_notification_idx` are allocated by the **SDK**, and
  MUST be unique within one invocation.
- Notifications MAY arrive out of order. The SDK MUST match them by id and MUST NOT match them
  by position.
- The runtime MUST send a notification for every declared completion id, or end the invocation
  with `Error`.
- The same completion id MUST NOT receive two notifications.

### 8.2 `ctx.run` — the ordering guarantee

This is the subtlest constraint of the whole protocol. Get it wrong and side effects do not
replay stably, and the bug only shows after a crash.

When `REQUESTED_ACK` is set on a `ProposeRunCompletionMessage`, the runtime returns the
`ProposeRunCompletionAckMessage` **at exactly the position in the notification stream** that
the `RunCompletionNotificationMessage` will take on replay.

```
First run (PROCESSING):
    → SleepCompletionNotification      id = 1
    → ProposeRunCompletionAck          id = 3
    → SleepCompletionNotification      id = 2

Every later replay MUST be:
    → SleepCompletionNotification      id = 1
    → RunCompletionNotification        id = 3      ← in place of the ack
    → SleepCompletionNotification      id = 2
```

`ProposeRunCompletionAckMessage` is sent ONLY in the PROCESSING phase, NEVER in the REPLAY
phase.

When `REQUESTED_ACK` is **not** set, the runtime sends the `RunCompletionNotificationMessage`
directly — with the same ordering guarantee.

### 8.3 Suspension

- The SDK SHOULD send `Suspension` when user code is waiting and there is nothing left to do.
- The wait list MUST NOT be empty.
- After `Suspension`, the SDK MUST close its sending side and MUST NOT send further messages.
- The runtime MUST reopen the invocation when any notification in the list is ready.
- The runtime MAY ignore the suspension and keep the connection (for example for a lower
  wake-up latency). The SDK MUST handle both.

### 8.4 Determinism

| Source | Rule |
|---|---|
| Random | The SDK MUST seed its RNG from `StartMessage.random_seed`. No other source of randomness may be used in user code |
| Current time | Only through `ctx.run`, or derived from `random_seed`/`id`. Reading the clock directly is a violation |
| Sleep | An absolute instant, computed once and then journaled |
| External I/O | Must go through `ctx.run` |
| UUID generation | Derived deterministically from `id` + the command index |

The SDK SHOULD detect and warn when user code violates these, if the language allows it.

### 8.5 Errors and retries

| `behavior` | What the runtime does |
|---|---|
| `RETRY` (0) | Retries per policy; `next_retry_delay`, if present, overrides it for **one** attempt |
| `PAUSE` | Suspends the invocation, waiting for manual intervention |
| `FAIL` | Ends the invocation with the error, no retry |

Unset field ⇒ `RETRY`.

The runtime MUST treat `JOURNAL_MISMATCH` (570) as **not retryable** whatever the `behavior` —
a retry only reproduces the same error. It is the sign that the code changed incompatibly with
the old journal, and needs a human.

Cairnflow goes further than the specification here: 570 **always** moves the invocation to
`paused`, even when the SDK sends `FAIL`. The fix is to deploy the right version and then
`POST /invocations/{id}/resume`; `failed` would lose the work forever because of one bad
deploy. The TypeScript, Python and Go SDKs send `PAUSE` for 570.

### 8.6 Idempotency

When `idempotency_key` is present on a `Call`/`OneWayCall`, the runtime MUST guarantee at most
one invocation for the tuple `(service, handler, key, idempotency_key)`. A duplicate call
attaches to the running invocation and receives the same result.

Consequence: an invocation can have **several** waiters for its result. The runtime MUST return
the same result to all of them, including those that attach after it has finished.

The key tuple is compared as **the exact bytes**, not a hash — with a hash two different tuples
can collide, and `idempotency_key` is usually set by an outside party.

**The id of a keyed invocation is a function of the key tuple itself.** The 24-byte id =
partition key ‖ a 16-byte identifier, with identifier =
`SHA-256("cairnflow-ik/2" ‖ len(scope) ‖ scope ‖ len(service) ‖ service ‖
len(handler) ‖ handler ‖ len(key) ‖ key ‖ len(idem) ‖ idem)` truncated to 16 bytes, and
partition key = `H(scope)` if there is a scope, otherwise `H(service, key)` if there is an
object key, otherwise `H(service, idem)` (`H` = the first 8 bytes of a SHA-256 with the domain
tag `cairnflow-pk/2` — see `docs/key-design.md`). The scope is in both: key `k` of tenant `bob`
and of `alice` are two different keys. Thanks to that **every** caller with the same key — an
SDK in whatever partition, or an HTTP client — computes the same id on its own, and the
`CallInvocationId` handed to the caller *is* the real id even when the dedupe happens in another
partition. SHA-256 is required, not an aesthetic choice: keys are set by outside parties, and a
deliberate collision between two key tuples means one party receives the other's result. (Keys
are still compared as the exact bytes through the `idempotency` table — the derived id is only
a naming scheme, not the comparison mechanism.)

**The dedupe window = retention.** A finished invocation is cleaned up after `--retention-ms`
(24h by default); once cleaned, a call with the same key is a **new** invocation — with *the
same id*, because the id is deterministic in the key. This is semantics and must be told to
users: idempotency is not forever unless retention is off.

**The HTTP door is section 8.6 too.** `POST /invoke/{service}/{handler}` accepts an `Idempotency-Key`
header; the semantics are exactly those of a keyed `ctx.call`: a duplicate call points at the
invocation holding the key, and a result that already exists is returned at once.

### 8.7 Signal

`SendSignal` (`0x0410`) delivers a `SignalNotification` (`0xFBFF`) to the target invocation,
at-least-once. Four rules:

1. **Identity, not completion id.** A signal is identified by `idx` or `name`. Receiving **a
   duplicate identity is normal** (at-least-once) and the state machine MUST ignore it
   silently — section 8.1 only talks about completion ids, which the SDK allocates itself; it does
   not apply to signals.
2. **Journaled on delivery.** A signal that arrives is written as a `0xFBFF` entry and replayed
   like every notification — a replay sees the right signal in the right place.
3. **Target already finished ⇒ no-op.** Delivering a notification to a terminal invocation
   would rerun it — the same rule as for the timers of a cancelled invocation.
4. **A signal leaf in a suspension is satisfied once a signal with the same identity is in the
   journal** — both `waiting_signals` (idx) and `waiting_named_signals` (name), in every
   combinator. **Once arrived, satisfied forever:** a signal is a journal entry, it is not
   "consumed" when a leaf waiting on it is satisfied.

**Signal leaves in combinators.** The SDK puts signal leaves next to completion leaves in
**one** single-level tree: `ctx.race([ctx.signal("approve"), ctx.sleep(24h)])` suspends with
`Suspension { waiting_completions: [id of the sleep], waiting_named_signals: ["approve"] }`
(FIRST root); `ctx.all([..])` wraps the leaves in an `ALL_COMPLETED` child node. Waiting on
**one** leaf is encoded exactly as before mixed leaves existed (field 1, 2 or 3 at the root). The
winner of a `race` is the ready leaf that stands **earliest** in the journal — completion
notifications and `0xFBFF` entries on **one** sequence of positions — so a replay picks the
same winner again. A consequence of rule 4:

```ts
const r = await ctx.race([ctx.signal("approve"), ctx.sleep(60_000)]);
// the sleep wins (it expires), then "approve" arrives: it still goes into the journal.
const again = await ctx.signal("approve");   // satisfied AT ONCE, with the value that just arrived
```

To wait for a **new** occurrence, use a new identity (`approve-2`, or a new awakeable). The
cairnflow runtime wakes the invocation on every signal (even when an `all` tree does not have
enough leaves yet): the SDK reruns the handler, sees the tree unsatisfied and suspends again — an
extra turn, still correct. Every `sleep` that loses a `race` still fires when due, costing one
commit on an invocation that has finished: use long deadlines, do not loop on short ones.

`BuiltInSignal.CANCEL = 1` is a **cooperative** cancellation: once the SDK receives it, every
later wait point MUST fail towards user code with a terminal error (code 409), so that
`try/catch/finally` runs its cleanup; the invocation ends with its own failed `Output`.
Completions already in the journal are still returned normally — the code before the
cancellation point replays exactly as before.

The cairnflow runtime delivers `CANCEL` **mid-turn**: in `BIDI_STREAM` a CANCEL
`SignalNotification` can arrive on the open stream at any moment, even when the SDK has not
suspended yet (while running a `ctx.run` closure); in `REQUEST_RESPONSE` it goes into the
journal before the next turn. An SDK that does not read mid-turn frames is still correct: the
frame waits until the next suspension, like every early notification. The TS SDK reads it at
once and aborts `ctx.run` closures declared `cancellable: true` (an `AbortSignal` parameter); a
cancellable `ctx.run` without a result yet, on an invocation that has been cancelled, is a
cancellation point too.

---

## 9. Room left for v2

This list is a contract with ourselves: the things below can be added **without bumping the
wire version**, because their room was reserved in advance.

| Feature | Reserved room | Status |
|---|---|---|
| Future tree for suspensions | `SuspensionMessage` fields 4, 5 | ✅ opened |
| Eager state | `StartMessage` fields 4, 5; commands `0x0407`, `0x0408` | ✅ opened |
| `ClearAllState`, `GetStateKeys` | `0x0405`, `0x0406` / `0x8006` | ✅ opened |
| Promise (workflow) | `0x0409`–`0x040B` | ✅ opened |
| Attach / GetInvocationOutput | `0x0412`, `0x0413` | ✅ opened — only invocation-id targets so far; fields 3-4 (idempotent request, workflow target) still reserved |
| Scope, limit key (rate limiting) | `StartMessage` 11-15; `Call` 7-8; `OneWayCall` 8-9 | ✅ opened on `Call` 7-8 and `OneWayCall` 8-9 in wire v2 (a version bump, section 9.1: a runtime that ignored the fields would run the call without a cap); `StartMessage` 11-15 still reserved |
| `AwaitingOn` (observability) | Control `0x0006` | still reserved |
| Placement of an unkeyed callee (`placement: "spread"`) | `Call`, `OneWayCall` field 13 (new, not reserved in advance) | ✅ opened — **no** wire bump: a placement hint, not semantics. A runtime that does not know the field (or a cluster without the `calls/placement` gate on) ignores it and the callee is colocated by default; the callee id stays deterministic. But a runtime that ignores it **must not** process the same journal as a runtime that understands it — the `calls/placement` migration gate (`min_binary=5`) prevents that, not wire negotiation (the journal keeps the command bytes, and replay re-emits the call before the SDK sees anything) |
| User tags of the callee | `Call`, `OneWayCall` field 14 (new, not reserved in advance) | ✅ opened — **no** wire bump: metadata, not semantics. A runtime that does not know the field ignores it and the callee only lacks its tags |
| New system signals | `BuiltInSignal` reserved 2-15 | still reserved |

Just as the contract promised: the five opened features went in while the wire version stayed
1. Two-way compatibility is built into the SDK: an invocation that lives through an upgrade with
a lazy `GetState` entry in its journal still replays even though the runtime now sends
`state_map` — the SDK looks at the **journal** before choosing the eager or lazy path, not at
the current capabilities.

**What WILL require a version bump:** changing the semantics of an existing command, changing
the header layout, changing the journal model. That is why section 2 has existed since day one.

### 9.1 Wire v2 and v3

| Version | Adds | Negotiation |
|---|---|---|
| v2 | Flow control: `scope`, `limit_key` on `Call`/`OneWayCall` | content-type `invocation.v2` |
| v3 | `Keepalive` (`0x0008`), `ReplayReport` (`0x0009`), `Start.keepalive_interval_ms` (16), `Start.replay_only` (17) | content-type `invocation.v3` |
| v4 | Side channel `StreamAppend` (`0x000B`) — the records of `ctx.stream`; `ArtifactPut` (`0x000C`, artifacts) | content-type `invocation.v4` |
| v5 | Incremental replay for `REQUEST_RESPONSE` (section 9.3): `Start.replay_from` (18), `Start.prefix_digest` (19), headers `cairn-replay-cache`/`cairn-replay`/`cairn-affinity`, a `409` miss response; manifest `replayCache` | content-type `invocation.v5` |
| v6 | Segmented values: a `Value` sent to the SDK carries `segments` (refs) + `content` (tail) + `len`; read URLs in `Start.segment_urls` (20) and the side channel `SegmentUrls` (`0x000D`, BIDI mid-turn); a `SetState.value` from the SDK may carry reused `segments` (refs of the value the SDK just read for that same key); `Output.value` and `ProposeRunCompletion.segmented_value` (field 16, `Value`) too — refs of a segmented value already in the invocation's own journal (`RunCompletion`/`CallCompletion` sent to the SDK may carry refs as well). The runtime only sends refs when the node has a `direct`/`proxy` read path (`--value-read-path`); otherwise it assembles inline as in v5 | content-type `invocation.v6` |

**Keepalive (v3).** The runtime sets `keepalive_interval_ms` = the inactivity limit / 3 when
the deployment settled on v3 and has a limit. In `BIDI_STREAM`, before running the closure of a
new `ctx.run`, the SDK flushes every frame it has emitted (the `Run` command reaches the runtime
**before** the closure) and then sends a `Keepalive` (empty payload) every interval until the
closure is done. The runtime ignores the frame — the inactivity limit is recomputed from every
frame anyway — so a closure longer than the limit is not cancelled. `REQUEST_RESPONSE` has no
mid-turn channel: use the `inactivityTimeout` declared in the manifest.

**Streams (v4).** `StreamAppendMessage { name, durable, records[], close }`, each
`StreamRecord { step, attempt, k, payload }`. The SDK batches appends by 50 ms or 1 KiB into one
frame. In `BIDI_STREAM` the frame goes on the wire even while the SDK is in the middle of a
`ctx.run` closure (the command was emitted before it, keeping program order); in
`REQUEST_RESPONSE` it sits in the response body and the runtime applies it at the end of the
turn. The SDK flushes every batched record **before** the frame that ends the turn
(`Output`/`End`, `Suspension`, `Error`): the runtime stops reading after the outcome. Identity
`(step, attempt, k)`: `step` is the SDK's current command position, `attempt` is
`retry_count_since_last_stored_entry`, `k` is the order within the step; within one stream they
strictly increase. **Replay-aware:** the SDK does not send records emitted at a point where the
next command is already in the journal; the runtime drops duplicates (an identity not greater
than the last record) and inserts `superseded` when a new attempt of the same step starts. The
runtime assigns offsets. This frame never becomes an entry, has no `step`, and waits for no
durability (AP-4); a batch of frames that only holds side-channel frames triggers no durability
wait. Reading: `GET /invocations/{id}/streams/{name}` (SSE, `admin-api.md`).

**Dry-run replay (v3).** `Start.replay_only = true`: the SDK runs the handler on the replayed
journal; at the first command **outside** the journal it stops — before building the command's
payload, before any closure — and ends the turn with `ReplayReport { matched, mismatch,
expected, got, message }`. A journal mismatch also ends with `ReplayReport` (instead of
`Error` 570). Getting through the whole journal without emitting a new command ends the turn as
usual (`Suspension`/`End`) — the runtime takes it as a full match. The runtime does **not** send
`replay_only` to a deployment settled below v3: an old SDK would ignore the flag and run the real
code.

### 9.2 Unknown frames and the side channel

The **side channel** is frames that travel on the connection but **never become journal
entries, never wait for a durable ack, and shift no entry's index wherever they are
interleaved**. Today there are: `Keepalive` (`0x0008`, SDK → runtime), `StreamAppend`
(`0x000B`, SDK → runtime, v4) and the custom range `0xFC00+` (section 5). Conformance traces mark them
with the `channel` field (section 11); `stream-side-{bidi,rr}-v4` covers `StreamAppend`. Every other
frame is on the **journal** channel: its position in the stream is meaningful.

The runtime treats SDK → runtime frames as follows, **at every negotiated wire version** — the
wire version changes what the SDK *is allowed to send*, not how the runtime treats what it does
not understand:

| Frame | What the runtime does |
|---|---|
| `Keepalive` `0x0008` | Ignored after resetting the inactivity limit — even when settled below v3 (lenient: the frame carries no state) |
| Custom `0xFC00+` | Not parsed, not journaled. `BIDI_STREAM` forwards the bytes as is on the open stream; `REQUEST_RESPONSE` has no path for it, so it is dropped with a `warn` log |
| `ReplayReport` `0x0009` outside a `replay_only` turn | `PROTOCOL_VIOLATION` (571), nothing written |
| `ContinueAsNew` `0x000A` | Ends the turn at the turn layer; never enters the journal |
| `StreamAppend` `0x000B` | Settled **v4**: goes into the range's `StreamHub`, not journaled, no durability wait. Settled below v4: 571, nothing written (the SDK sent something it may not send) |
| `SegmentUrls` `0x000D` from the SDK | 571 — this side channel only goes runtime → SDK |
| `SetState` carrying `segments` | Settled **v6**: every ref must be in the key's current row (checked under the engine, in the same commit step); a mismatch ⇒ the turn fails and is retried, nothing written. Settled below v6, or another `Value` carrying `segments`: 571 |
| `Output`/`ProposeRunCompletion` carrying `segments` | Settled **v6**: every ref must be in the invocation's own journal (holder `Invocation(id)`); a mismatch ⇒ the turn fails and is retried, nothing written. Settled below v6: 571 |
| `ArtifactPut` `0x000C` | Settled **v4** with the `streams/table` gate on: PUT the object `artifacts/<tenant>/<id>/<epoch>/<sha>` and then commit the `Artifacts` row, **before** the next frame (AP-1), not journaled. sha not matching the content, more than 16 MiB minus 1 KiB (`MAX_BYTES`, leaving room for the frame overhead under the 16 MiB frame limit), not decodable: 571. Settled below v4 or gate off: 571 |
| Reserved codes (`0x0006`, `0x040F`, `0x0414`), unknown codes (outside every range, or in the command range but not assigned) | 571, nothing written |
| Notifications (`0x8000`–`0xFBFF`) | 571 — notifications only go runtime → SDK |
| `SetState`/`ClearState`/`ClearAllState` from a `SHARED` handler — of a workflow, or of a virtual object when entry 0 carries `cairnflow-state-lock: on` (every invocation created from a version with this rule) | 571 `state writes are not allowed in a SHARED handler…`, nothing written (D-W5) |

Consequence for future features: a new side-channel frame **must carry a new code** and may
only be sent once the version that opens it has been settled. An older runtime sees an unknown
code and returns 571 — failing loudly, never writing the wrong thing — and the negotiation of section 2
never lets an SDK send it to such a runtime. Pinned in
`crates/cairn-runtime/tests/side_channel.rs` (state machine) and
`crates/cairn-server/tests/side_channel.rs` (the `REQUEST_RESPONSE` turn layer, wire 1/2/3; the
`BIDI_STREAM` path uses the same filter in `drive_bidi` but has no test at this layer yet).

Runtime → SDK direction: the SDK MUST ignore custom `0xFC00+` frames it has no API to receive,
and MUST NOT treat them (or `CommandAck`, `ProposeRunCompletionAck`) as local journal entries —
the trace `custom-side-bidi-v3` checks this.

### 9.3 Incremental replay for `REQUEST_RESPONSE` (v5)

In `REQUEST_RESPONSE` the runtime resends the whole journal on every turn (section 4): turn `k`
carries `k − 1` old results, so the bytes sent to the SDK grow quadratically with the number of
steps. v5 lets the SDK keep the journal of a running invocation in RAM and lets the runtime send
only **the part after a mark** — a mark **proven** by a digest, trusting nobody's word. Every
ambiguity falls back to a full replay (exactly as in v4). The semantics of turns, frames and
the durable-then-send order do not change: the SDK runs the handler on exactly the frame
sequence, byte-identical to a full replay.

**Declaration.** The SDK declares `"maxProtocolVersion": 5` and `"replayCache": true` in the
manifest (section 3). The runtime only sends deltas to a deployment settled on v5 **and** declaring
`replayCache`. An old binary ignores the unknown field and settles on ≤ 4.

**Digest.** A SHA-256 hash chain over entries, on **exactly the bytes of the frames** the
runtime sends (8-byte header + payload, before body compression):

```
h₀     = SHA-256("cairn-replay-v1" ‖ u32be(len(id)) ‖ id)        id = Start.id
hᵢ₊₁   = SHA-256(hᵢ ‖ u32be(len(fᵢ)) ‖ fᵢ)                        fᵢ = the frame of journal entry i
```

`h_B` covers entries `[0, B)`. `Start` is **not** part of the chain: it is sent in full on every
turn (including `state_map`).

**Request.** Two fields are added to `StartMessage`:

| Field | Type | Meaning |
|---|---|---|
| `replay_from` (18) | `uint32` | `B`: the request body **omits** the first `B` entries. `0`/absent = a full replay as in v4 |
| `prefix_digest` (19) | `bytes` (32) | `h_B` computed on the runtime's **current** journal. Only present when `B > 0` |

`known_entries` keeps its meaning (`N`, the total of stored entries). Request body = `Start` ‖
entries `[B, N)` ‖ (notifications created when the turn opens, as in v4) — that is, the full
replay body without the `B` frames right after `Start`. The runtime always picks `0 < B ≤ N`.

**The SDK on a turn with `B > 0`.** It has ≥ `B` entries of `id` in its cache **and** its `h_B`
equals `prefix_digest` ⇒ it joins `Start` ‖ cache `[0, B)` ‖ the rest of the body, checks that
the body still has ≥ `N − B` frames, and runs the turn exactly as with a full replay. Otherwise
(no cache entry, fewer than `B` entries, a mismatching digest, missing frames) the SDK answers
**miss**:

- HTTP `409`, header `cairn-replay: miss`, empty body;
- optionally (round 2): header `cairn-replay-held: <k>:<64 hex of h_k>` — the part of the
  journal the SDK still holds for `id`, `k = min(entries held, N)`, `h_k` taken from the stored
  chain. No cache entry ⇒ this header is not sent. A v5 SDK from before round 2 does not send
  it — the runtime then resends everything as before;
- it does **not** run the handler, emits no frame, and leaves the cache entries of other
  invocations alone.

The request identity check (section 4) comes before everything; a miss is only returned after the
request has passed the check.

**Response.** Header `cairn-replay-cache: <k>` = the number of leading journal entries
(`[0, k)`, `k ≤ N`) the SDK keeps after this turn. Absent or `0` = it keeps nothing. The SDK
only keeps entries it **received** from the runtime (the replayed part), not the commands it
just sent — the bytes of those entries are encoded by the runtime, and the SDK does not guess
them. The SDK drops the cache entry when a turn ends the invocation (`End`, a terminal `Error`)
or when its RAM limit/TTL pushes it out.

**Runtime.**

- Keeps a **hint** in RAM for the range's term: `id → (B, h_B, deployment, affinity)`,
  `B = min(k, N)` from the previous turn. The hint is not durable and is never used to **skip**
  the SDK's comparison: `prefix_digest` is always recomputed on the current journal; a stale
  hint (a deployment change, a digest that no longer matches) ⇒ `B = 0`.
- On a miss at a turn with `B > 0` ⇒ resend **at once** in that same turn, with the same
  identity. With `cairn-replay-held: k:h_k` where `0 < k ≤ N`, `k ≠ B` **and** `h_k` computed on
  the current journal equals the `h_k` the SDK reported ⇒ resend **once** with
  `replay_from = k`; otherwise (header absent, malformed, `k` = 0, `k` > `N`, mismatching
  digest), or on a miss of the resend-from-`k` too, ⇒ `B = 0`. No resend counts as an attempt
  or changes durable state. A miss at a turn with `B = 0` (or a `409` without
  `cairn-replay: miss`) is an SDK error: it takes the transport-error path like every non-2xx
  response.
- Turns itself off per deployment **by bytes**: for every window of 20 delta turns (per
  deployment, within one range), if the bytes actually sent in delta mode (every resend, before
  compression) are ≥ 95 % of the full body bytes of those same turns ⇒ no deltas for 10
  minutes. The miss rate is not measured: a miss only costs one round trip of latency, and as
  long as deltas stay cheap they stay on. `--no-rr-delta-replay` turns it off entirely on a
  node.

**Affinity (optional, no effect on correctness — wrong affinity only causes misses).**

- The runtime keeps the `name=value` pairs of the `Set-Cookie` headers in the responses of
  **that invocation** (capped at 4 KiB, attributes dropped) and sends them back as `Cookie` on
  that invocation's next turns, to the same deployment. Never to another invocation,
  deployment or tenant.
- Response header `cairn-affinity: fly=<id>` (`id` alphanumeric, ≤ 64 characters; the SDK
  takes it from `FLY_MACHINE_ID`) ⇒ the runtime sends `fly-force-instance-id: <id>` on the next
  turn.

**Compatibility.**

| | Runtime ≤ v4 | Runtime v5 |
|---|---|---|
| SDK ≤ v4 | as before | settles on ≤ 4 ⇒ fields 18/19 are not sent — as before |
| SDK v5 | the old runtime ignores `replayCache`, settles on ≤ 4 — as before | deltas when `replayCache`; otherwise as before |

`replay.rs` (dry-run replay through `call_detached`) and `BIDI_STREAM` are unchanged. Traces:
`delta-hit-rr-v5`, `delta-miss-rr-v5` (mismatching digest), `delta-after-restart-rr-v5` (the
SDK lost its cache) — the `replay` field of a connection in the trace (`conformance/README.md`).

---

## 10. Implementation checklist

### Runtime

- [x] Parse the 8-byte header, check the reserved bits are 0 — `crates/cairn-protocol`
- [x] Length limit, refused right at the header — `crates/cairn-protocol`
- [x] Dispatch table by message type; `0xFC00`+ forwarded as raw bytes — `crates/cairn-protocol`
- [x] Journal writes, command index allocation — `crates/cairn-runtime`
- [x] Notifications matched by completion id — `crates/cairn-runtime`
- [x] **The section 8.2 ordering guarantee** — `crates/cairn-runtime`, the journal always stores
      `RunCompletion`; only what goes on the wire depends on the phase
- [x] Timers for `Sleep` — `crates/cairn-server`, the `--tick-interval-ms` background loop
- [x] `OneWayCall.invoke_time` — the `Delayed` table, the tick moves it to the outbox when due;
      the precision is the tick, the same as for timers
- [x] `SendSignal` / `SignalNotification` — section 8.7; the `SignalOutbox` table, cross-partition
      through `Signal` messages, deduplicated by identity in the state machine
- [x] `Call`/`OneWayCall` routing, dedup by idempotency key — `crates/cairn-server`
- [x] Waking from suspension — when a timer fires or a `ctx.call` result returns (**signal**:
      not yet)
- [x] Retry policy + the three `ErrorBehavior`s — the `retries` table, exponential backoff,
      `next_retry_delay` overriding once
- [x] `JOURNAL_MISMATCH` not retried — `crates/cairn-runtime`
- [x] Discovery: calling `/discover`, negotiating both version axes — `crates/cairn-server`
- [x] HTTP ingress + admin API — `crates/cairn-server`
- [x] `REQUEST_RESPONSE` invoker — `crates/cairn-server`
- [x] `BIDI_STREAM` invoker (h2c prior-knowledge, turns on one stream) — `crates/cairn-server`

### SDK

`sdk/typescript` — no package dependencies. `sdk/python` implements the same checklist with the
same model (no runtime dependencies; `BIDI_STREAM` needs the optional `h2`). `sdk/go` (module
`github.com/cairnflowdev/cairnflow/sdk/go`) implements it too, with the Go standard library only:
the handler runs on one goroutine, parallel work is futures, and h1 and h2c share one port through
`net/http`.

- [x] Frame encode/decode
- [x] Replay ↔ processing state machine, transparent to the user
- [x] Completion id allocation
- [x] Notifications matched by id, tolerating out-of-order arrival
- [x] Journal mismatch detection → sends code 570
- [x] RNG seeded from `random_seed`
- [x] `ctx.run` → propose, then suspend waiting for durability: `BIDI_STREAM` sets
      `REQUESTED_ACK` and waits for `ProposeRunCompletionAck`; `REQUEST_RESPONSE` asks for no
      ack (section 4) and receives the `RunCompletion` in the replayed part of the next turn
- [x] Deciding to suspend and building the wait list
- [x] Exporting the discovery manifest
- [x] Serving h1 and h2c on **one port** (preface sniffing), the `BIDI_STREAM` turn loop

### `End` is not a journal entry

`EndMessage` is the signal *"user code has returned"*, scoped to **one connection** — the same
kind of thing as the `REQUESTED_ACK` flag, not like `Output`.

The runtime MUST NOT write `End` to the journal, and the SDK MUST send `End` again on the next
replay if the program has run out of steps. Otherwise: a process that dies after `Output` is
durable but before it sent `End` leaves the invocation **stuck forever at its very last step**,
and every replay gets stuck again in the same way.

**`Output` is the opposite.** It *is* a journal entry, so it follows the rules of every
command: the SDK MUST NOT resend an `Output` already in the journal, and MUST match it against
the next entry like every other command — a mismatch ⇒ `JOURNAL_MISMATCH` (570).

The two paragraphs above talk about **the same restart** and reach opposite conclusions, which
makes this the easiest place in all of section 10 to get wrong. The runtime commits frame by frame, so
dying exactly between the `Output` commit and the `End` commit can really happen; on the
recovery that follows the SDK must send **exactly one `End` and no `Output`**. Resending
`Output` makes the runtime append another entry, the journal grows by one after *every*
recovery, and the "bit-exact replay" property of mandatory test 1 no longer holds.

### Mandatory tests before calling it done

1. **Bit-exact replay** — run an invocation, kill it midway at every command index, replay,
   and confirm the journal and the output are identical. — ✅ runtime side:
   `crates/cairn-runtime/tests/mandatory.rs`
2. **Out-of-order notifications** — inject a deliberate permutation and confirm the SDK is
   still correct. — ✅ runtime side
3. **Run/ack ordering (section 8.2)** — the interleaved sleep-run-sleep scenario of section 8.2, run and
   replayed. — ✅ runtime side
4. **Journal mismatch** — change the user code between two runs, confirm 570 is reported and
   there is no endless retry. — ✅ runtime side

What can be checked where, and why:

| Test | Fake SDK (runtime) | Real TypeScript SDK |
|---|---|---|
| 1. Bit-exact replay | ✅ compares **byte by byte**, every index | ✅ kills the whole process at every turn; compares the result + the journal shape. Traces `counter-resume-*` (section 11): resume from **every** durable prefix, byte by byte |
| 2. Out-of-order notifications | ✅ | ✅ traces `out-of-order-bidi-v3` (completion 2 then 1 on an open stream) and `out-of-order-rr-v1` (reversed order in the replayed part) |
| 3. Run/ack ordering (section 8.2) | ✅ | ✅ traces `run-ack-order-bidi-v3` (exactly the section 8.2 scenario: the SDK asks for acks in `BIDI_STREAM`) and `run-no-ack-rr-v1` |
| 4. Journal mismatch | ✅ | ✅ changing user code between two runs; trace `journal-mismatch-rr-v1` |

Cells 2 and 3 used to be empty because the real runtime never reorders notifications and the
SDK did not ask for acks. Now the SDK asks for acks in `BIDI_STREAM`, and both cells are filled
by the **fake runtime** of the conformance suite (section 11) — it emits exactly the frames the
runtime's state machine emits, in the order the scenario chooses.

What `BIDI_STREAM` **does** add, and what has been checked with the real SDK: the same program
running in both modes writes journals that are **identical byte for byte** —
`sdk/typescript/test/journal-parity.test.ts`. That is the condition for an invocation that
started in one mode to continue on a node that only has the other, and it does not follow from
the four tests above.

Tests with the real SDK: `sdk/typescript/test/durability.test.ts` and `e2e.test.ts` — the real
Rust binary talking to the real TypeScript SDK. Two independent implementations of this same
specification, two languages, not one shared line of code. That is the only check that catches
bit-level drift in the header, the message numbers or the protobuf field layout.

**Bit-exactness of a single invocation cannot be observed over HTTP**: `/journal` can be read
*between* two turns, but it cannot give a consistent snapshot *within* a turn — a turn commits
several times, so two consecutive queries see two different journal lengths and both are right.
It is checked at the runtime layer, where journals are compared directly.

*(Before SDK calls were moved off the engine thread the reason was different: `/journal`
queued behind the whole turn and could not answer. Now it answers — what cannot be observed is
only consistency at one point in time, not the ability to ask.)*

---

## 11. Conformance suite

`conformance/` holds language-neutral golden traces: each file is a frame conversation byte by
byte, the wire version, the transport mode, the handler (by name) and the expected final
journal. Format, runner rules and the conformance handlers: `conformance/README.md`; schema:
`conformance/trace.schema.json`. A second SDK proves it speaks the protocol correctly by running
the traces — no Rust, no runtime binary needed.

- **Source.** `crates/cairn-runtime/tests/conformance_gen.rs` generates the traces from the real
  state machine, the transport rules of `turn/sdk.rs` and a reference SDK that encodes with
  `prost`; the drift check (a normal test run) turns red when the runtime's on-the-wire
  behaviour changes.
- **Reference runner.** `sdk/typescript/test/conformance.test.ts` — a fake runtime replays the
  traces into the real TypeScript SDK over h1/h2c; every trace with wire < 3 also runs against an
  SDK pinned with `maxProtocolVersion`. The Python SDK's runner,
  `sdk/python/tests/test_conformance.py`, applies the same rules to `sdk/python`. The Go SDK's
  runner, `sdk/go/conformance_test.go`, applies them to `sdk/go` (`go test ./...`), and pins every
  trace below wire 6, not only those below 3.
- **Canonical encoding.** The SDK MUST encode SDK → runtime frames in proto3 canonical form —
  fields in ascending tag order, numeric `repeated` fields packed, default values not written
  except for `oneof` members — so that the same command produces the same byte sequence in every
  SDK (and the journals of two SDKs can be compared). The runtime still **accepts** every valid
  form; this rule is the SDK's, not the parser's.
