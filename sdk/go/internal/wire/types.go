// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import "fmt"

// MessageType is the 16-bit type code of a frame (docs/protocol-v1.md section 5).
type MessageType uint16

// Control messages, 0x0000-0x000F.
const (
	TypeStart                   MessageType = 0x0000 // runtime → SDK
	TypeSuspension              MessageType = 0x0001 // SDK → runtime
	TypeError                   MessageType = 0x0002 // SDK → runtime
	TypeEnd                     MessageType = 0x0003 // SDK → runtime
	TypeCommandAck              MessageType = 0x0004 // runtime → SDK
	TypeProposeRunCompletion    MessageType = 0x0005 // SDK → runtime, may request an ack
	TypeProposeRunCompletionAck MessageType = 0x0007 // runtime → SDK
	TypeKeepalive               MessageType = 0x0008 // wire v3, SDK → runtime, side channel
	TypeReplayReport            MessageType = 0x0009 // wire v3, SDK → runtime
	TypeContinueAsNew           MessageType = 0x000A // wire v3, SDK → runtime
	TypeStreamAppend            MessageType = 0x000B // wire v4, SDK → runtime, side channel
	TypeArtifactPut             MessageType = 0x000C // wire v4, SDK → runtime, side channel
	TypeSegmentUrls             MessageType = 0x000D // wire v6, runtime → SDK, side channel
)

// Commands, 0x0400+. Each is a journal entry; the completion of command 0x04XX is 0x80XX.
const (
	TypeInput               MessageType = 0x0400
	TypeOutput              MessageType = 0x0401
	TypeGetState            MessageType = 0x0402
	TypeSetState            MessageType = 0x0403
	TypeClearState          MessageType = 0x0404
	TypeClearAllState       MessageType = 0x0405
	TypeGetStateKeys        MessageType = 0x0406
	TypeGetEagerState       MessageType = 0x0407
	TypeGetEagerStateKeys   MessageType = 0x0408
	TypeGetPromise          MessageType = 0x0409
	TypePeekPromise         MessageType = 0x040A
	TypeCompletePromise     MessageType = 0x040B
	TypeSleep               MessageType = 0x040C
	TypeCall                MessageType = 0x040D
	TypeOneWayCall          MessageType = 0x040E
	TypeSendSignal          MessageType = 0x0410
	TypeRun                 MessageType = 0x0411
	TypeAttachInvocation    MessageType = 0x0412
	TypeGetInvocationOutput MessageType = 0x0413
)

// Notifications, 0x8000-0xFBFF. Runtime → SDK only.
const (
	TypeGetStateCompletion            MessageType = 0x8002
	TypeGetStateKeysCompletion        MessageType = 0x8006
	TypeGetPromiseCompletion          MessageType = 0x8009
	TypePeekPromiseCompletion         MessageType = 0x800A
	TypeCompletePromiseCompletion     MessageType = 0x800B
	TypeSleepCompletion               MessageType = 0x800C
	TypeCallCompletion                MessageType = 0x800D
	TypeCallInvocationIDCompletion    MessageType = 0x800E
	TypeRunCompletion                 MessageType = 0x8011
	TypeAttachInvocationCompletion    MessageType = 0x8012
	TypeGetInvocationOutputCompletion MessageType = 0x8013
	TypeSignal                        MessageType = 0xFBFF
)

// CustomBase is the first code of the custom range 0xFC00-0xFFFF: the runtime forwards those
// bytes without parsing them, an SDK ignores the ones it has no API for.
const CustomBase MessageType = 0xFC00

// Error codes the protocol defines on top of HTTP status codes (ErrorMessage.code).
const (
	// JournalMismatch: the code being replayed does not match the journal.
	JournalMismatch = 570
	// ProtocolViolation: a frame that is not allowed here (bad header, bad payload, wrong
	// message for the state, a frame above the length limit).
	ProtocolViolation = 571
)

var typeNames = map[MessageType]string{
	TypeStart:                         "Start",
	TypeSuspension:                    "Suspension",
	TypeError:                         "Error",
	TypeEnd:                           "End",
	TypeCommandAck:                    "CommandAck",
	TypeProposeRunCompletion:          "ProposeRunCompletion",
	TypeProposeRunCompletionAck:       "ProposeRunCompletionAck",
	TypeKeepalive:                     "Keepalive",
	TypeReplayReport:                  "ReplayReport",
	TypeContinueAsNew:                 "ContinueAsNew",
	TypeStreamAppend:                  "StreamAppend",
	TypeArtifactPut:                   "ArtifactPut",
	TypeSegmentUrls:                   "SegmentUrls",
	TypeInput:                         "Input",
	TypeOutput:                        "Output",
	TypeGetState:                      "GetState",
	TypeSetState:                      "SetState",
	TypeClearState:                    "ClearState",
	TypeClearAllState:                 "ClearAllState",
	TypeGetStateKeys:                  "GetStateKeys",
	TypeGetEagerState:                 "GetEagerState",
	TypeGetEagerStateKeys:             "GetEagerStateKeys",
	TypeGetPromise:                    "GetPromise",
	TypePeekPromise:                   "PeekPromise",
	TypeCompletePromise:               "CompletePromise",
	TypeSleep:                         "Sleep",
	TypeCall:                          "Call",
	TypeOneWayCall:                    "OneWayCall",
	TypeSendSignal:                    "SendSignal",
	TypeRun:                           "Run",
	TypeAttachInvocation:              "AttachInvocation",
	TypeGetInvocationOutput:           "GetInvocationOutput",
	TypeGetStateCompletion:            "GetStateCompletion",
	TypeGetStateKeysCompletion:        "GetStateKeysCompletion",
	TypeGetPromiseCompletion:          "GetPromiseCompletion",
	TypePeekPromiseCompletion:         "PeekPromiseCompletion",
	TypeCompletePromiseCompletion:     "CompletePromiseCompletion",
	TypeSleepCompletion:               "SleepCompletion",
	TypeCallCompletion:                "CallCompletion",
	TypeCallInvocationIDCompletion:    "CallInvocationIdCompletion",
	TypeRunCompletion:                 "RunCompletion",
	TypeAttachInvocationCompletion:    "AttachInvocationCompletion",
	TypeGetInvocationOutputCompletion: "GetInvocationOutputCompletion",
	TypeSignal:                        "Signal",
}

// String returns the message name used by the specification and the conformance traces
// ("Start", "GetStateCompletion", ...), or "0xNNNN" for a code without a message (reserved,
// unassigned, custom).
func (t MessageType) String() string {
	if n, ok := typeNames[t]; ok {
		return n
	}
	return fmt.Sprintf("0x%04X", uint16(t))
}

// Known reports whether t is a code this package has a message for. Custom codes are not
// "known": their payload is opaque.
func (t MessageType) Known() bool {
	_, ok := typeNames[t]
	return ok
}

// IsControl reports whether t is below the command range (control codes are 0x0000-0x000F,
// the rest up to 0x03FF is unassigned).
func (t MessageType) IsControl() bool { return t < 0x0400 }

// IsCommand reports whether t is in the command range 0x0400-0x7FFF. Only assigned codes
// are journal entries; the rest of the range is reserved.
func (t MessageType) IsCommand() bool { return t >= 0x0400 && t < 0x8000 }

// IsNotification reports whether t is in the notification range 0x8000-0xFBFF: bit 0x8000
// set, below the custom range.
func (t MessageType) IsNotification() bool { return t >= 0x8000 && t < CustomBase }

// IsCustom reports whether t is in the custom range 0xFC00-0xFFFF.
func (t MessageType) IsCustom() bool { return t >= CustomBase }

// IsSideChannel reports whether a frame of type t travels without becoming a journal entry,
// without a durable ack and without shifting any entry's index (docs/protocol-v1.md section
// 9.2): Keepalive, StreamAppend, ArtifactPut, SegmentUrls and the custom range.
func (t MessageType) IsSideChannel() bool {
	switch t {
	case TypeKeepalive, TypeStreamAppend, TypeArtifactPut, TypeSegmentUrls:
		return true
	}
	return t.IsCustom()
}

// MayRequestAck reports whether the REQUESTED_ACK flag is allowed on a frame of type t: the
// "ack" column of section 5, every assigned command and ProposeRunCompletion. The flag on any
// other frame is a protocol violation.
func (t MessageType) MayRequestAck() bool {
	return t == TypeProposeRunCompletion || (t.IsCommand() && t.Known())
}

// CompletionType returns the notification code that completes command t (0x04XX → 0x80XX).
// It is meaningful only for commands that have a completion.
func (t MessageType) CompletionType() MessageType { return t&0x00FF | 0x8000 }
