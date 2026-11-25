# cairn-protocol

Cairnflow Service Protocol v1: framing, the message type table, codec.
Implements [`docs/protocol-v1.md`](../../docs/protocol-v1.md) — the document is
the source of truth, and this crate must follow it.

The crate knows **nothing** about the runtime, storage or HTTP. The reason is
D9: the protocol cannot be changed once there are SDKs and customers, so it must
stand alone, be testable alone, and carry no decision of the layers above.

## Requires protoc

`build.rs` generates code from `proto/protocol.proto` with `prost-build`, which
needs `protoc` on PATH (`brew install protobuf`). Checked with libprotoc 35.1.

## Layout

| File | Contents | Spec section |
|---|---|---|
| `frame.rs` | 8-byte header, flags, streaming decoder | 4 |
| `message.rs` | `MessageType`, grouping, the `ack` column, completion mapping | 5 |
| `lib.rs` | `Message`, the `decode()` dispatch table | 5, 10 |
| `proto/protocol.proto` | Protobuf definitions | 7 |

## A few easy mistakes, locked down by tests

- **The custom range is checked before the `0x8000` bit.** `0xFC00+` has that bit
  too; checking in the wrong order parses third-party data as a notification.
- **`0x04XX → 0x80XX` keeps the low byte**, it is not an OR with `0x8000`.
  `0x0402 → 0x8002`, not `0x8402`.
- **An empty `Value` ≠ `Void`.** Void = the key does not exist; an empty Value =
  it exists with an empty value. Merging the two is a classic bug.
- **The length cap is checked at the header**, before waiting for the whole
  payload — otherwise a lying `length` would already have made us hold a huge
  buffer.
- **The decoder consumes nothing when bytes are missing**, and reassembles
  correctly even when the stream arrives one byte at a time.

## Not done yet

`ProtocolError` covers only the framing layer. `JOURNAL_MISMATCH` (570) is a
runtime-layer error — it can only be detected by comparing the journal with the
user's code — so the constant lives here but the detection does not.
