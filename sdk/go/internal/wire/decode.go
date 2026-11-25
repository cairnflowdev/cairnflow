// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

import (
	"errors"
	"fmt"
)

// ErrUnknownType is wrapped by Decode for a code that has no message: reserved codes (0x0006,
// 0x040F, 0x0414), unassigned codes of every range. Custom codes (0xFC00+) are not unknown:
// they decode to *Custom.
var ErrUnknownType = errors.New("unknown message type")

// New returns a zero message for t, or nil when t has no message. A custom code returns a
// *Custom with that code.
func New(t MessageType) Message {
	if t.IsCustom() {
		return &Custom{Code: t}
	}
	switch t {
	case TypeStart:
		return new(Start)
	case TypeSuspension:
		return new(Suspension)
	case TypeError:
		return new(ErrorMessage)
	case TypeEnd:
		return new(End)
	case TypeCommandAck:
		return new(CommandAck)
	case TypeProposeRunCompletion:
		return new(ProposeRunCompletion)
	case TypeProposeRunCompletionAck:
		return new(ProposeRunCompletionAck)
	case TypeKeepalive:
		return new(Keepalive)
	case TypeReplayReport:
		return new(ReplayReport)
	case TypeContinueAsNew:
		return new(ContinueAsNew)
	case TypeStreamAppend:
		return new(StreamAppend)
	case TypeArtifactPut:
		return new(ArtifactPut)
	case TypeSegmentUrls:
		return new(SegmentUrls)

	case TypeInput:
		return new(Input)
	case TypeOutput:
		return new(Output)
	case TypeGetState:
		return new(GetState)
	case TypeSetState:
		return new(SetState)
	case TypeClearState:
		return new(ClearState)
	case TypeClearAllState:
		return new(ClearAllState)
	case TypeGetStateKeys:
		return new(GetStateKeys)
	case TypeGetEagerState:
		return new(GetEagerState)
	case TypeGetEagerStateKeys:
		return new(GetEagerStateKeys)
	case TypeGetPromise:
		return new(GetPromise)
	case TypePeekPromise:
		return new(PeekPromise)
	case TypeCompletePromise:
		return new(CompletePromise)
	case TypeSleep:
		return new(Sleep)
	case TypeCall:
		return new(Call)
	case TypeOneWayCall:
		return new(OneWayCall)
	case TypeSendSignal:
		return new(SendSignal)
	case TypeRun:
		return new(Run)
	case TypeAttachInvocation:
		return new(AttachInvocation)
	case TypeGetInvocationOutput:
		return new(GetInvocationOutput)

	case TypeGetStateCompletion:
		return new(GetStateCompletion)
	case TypeGetStateKeysCompletion:
		return new(GetStateKeysCompletion)
	case TypeGetPromiseCompletion:
		return new(GetPromiseCompletion)
	case TypePeekPromiseCompletion:
		return new(PeekPromiseCompletion)
	case TypeCompletePromiseCompletion:
		return new(CompletePromiseCompletion)
	case TypeSleepCompletion:
		return new(SleepCompletion)
	case TypeCallCompletion:
		return new(CallCompletion)
	case TypeCallInvocationIDCompletion:
		return new(CallInvocationIDCompletion)
	case TypeRunCompletion:
		return new(RunCompletion)
	case TypeAttachInvocationCompletion:
		return new(AttachInvocationCompletion)
	case TypeGetInvocationOutputCompletion:
		return new(GetInvocationOutputCompletion)
	case TypeSignal:
		return new(SignalNotification)
	}
	return nil
}

// Decode decodes the payload of a frame of type t. The error wraps ErrUnknownType for a code
// without a message and ErrMalformed for a payload that is not valid protobuf for it.
func Decode(t MessageType, payload []byte) (Message, error) {
	m := New(t)
	if m == nil {
		return nil, fmt.Errorf("%w %s", ErrUnknownType, t)
	}
	if err := m.Unmarshal(payload); err != nil {
		return nil, fmt.Errorf("decode %s: %w", t, err)
	}
	return m, nil
}

// DecodeFrame decodes the payload of f; see Decode.
func DecodeFrame(f Frame) (Message, error) { return Decode(f.Type, f.Payload) }

// UnmarshalValue decodes a standalone Value message (for example a value kept outside a
// frame).
func UnmarshalValue(b []byte) (*Value, error) {
	v := new(Value)
	if err := v.unmarshalProto(b, 0); err != nil {
		return nil, err
	}
	return v, nil
}

// MarshalValue returns the encoding of a standalone Value message.
func MarshalValue(v *Value) []byte { return v.appendProto(nil) }
