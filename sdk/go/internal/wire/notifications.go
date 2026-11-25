// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

// Notifications (0x8000-0xFBFF, runtime → SDK) all follow one field layout
// (NotificationTemplate of section 7): the id in field 1 (completion id) or 2/3 (signal), the
// result in 4 (Void), 5 (Value), 6 (Failure), 16 (invocation id) or 17 (state keys). Each
// message allows only some of the result members; fields outside its definition are skipped
// on decode, as an unknown field would be.

// Notification is implemented by every notification message.
type Notification interface {
	Message
	notification()
}

// Completion is implemented by every notification that completes a command (0x80XX), that is
// every notification but SignalNotification.
type Completion interface {
	Notification
	// CompletionID is the completion id of the command this notification completes.
	CompletionID() uint32
}

func (*GetStateCompletion) notification()            {}
func (*GetStateKeysCompletion) notification()        {}
func (*GetPromiseCompletion) notification()          {}
func (*PeekPromiseCompletion) notification()         {}
func (*CompletePromiseCompletion) notification()     {}
func (*SleepCompletion) notification()               {}
func (*CallCompletion) notification()                {}
func (*CallInvocationIDCompletion) notification()    {}
func (*RunCompletion) notification()                 {}
func (*AttachInvocationCompletion) notification()    {}
func (*GetInvocationOutputCompletion) notification() {}
func (*SignalNotification) notification()            {}

func appendCompletion(b []byte, id uint32, r *Result, t resultTags) []byte {
	b = appendUint32(b, 1, id)
	return appendResult(b, r, t)
}

func unmarshalCompletion(p []byte, id *uint32, r *Result, t resultTags) error {
	return walk(p, 0, func(rd *reader, field, wt int) (bool, error) {
		if field == 1 {
			var err error
			*id, err = rd.uint32Field(field, wt)
			return true, err
		}
		return readResult(rd, field, wt, r, t)
	})
}

var (
	voidValue        = resultTags{4, 5, 0}
	valueFailure     = resultTags{0, 5, 6}
	voidValueFailure = resultTags{4, 5, 6}
	voidFailure      = resultTags{4, 0, 6}
)

// GetStateCompletion (0x8002) answers GetState: Void when the key does not exist, or Value.
type GetStateCompletion struct {
	ID uint32 // 1
	Result
}

func (*GetStateCompletion) Type() MessageType      { return TypeGetStateCompletion }
func (m *GetStateCompletion) CompletionID() uint32 { return m.ID }
func (m *GetStateCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, voidValue)
}
func (m *GetStateCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, voidValue)
}

// GetStateKeysCompletion (0x8006) answers GetStateKeys.
type GetStateKeysCompletion struct {
	ID        uint32     // 1
	StateKeys *StateKeys // 17
}

func (*GetStateKeysCompletion) Type() MessageType      { return TypeGetStateKeysCompletion }
func (m *GetStateKeysCompletion) CompletionID() uint32 { return m.ID }

func (m *GetStateKeysCompletion) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.ID)
	if m.StateKeys != nil {
		b = appendMessage(b, 17, m.StateKeys)
	}
	return b
}

func (m *GetStateKeysCompletion) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ID, err = r.uint32Field(field, wt)
		case 17:
			if m.StateKeys == nil {
				m.StateKeys = new(StateKeys)
			}
			err = r.nested(field, wt, m.StateKeys)
		default:
			return false, nil
		}
		return true, err
	})
}

// GetPromiseCompletion (0x8009) answers GetPromise: Value or Failure.
type GetPromiseCompletion struct {
	ID uint32 // 1
	Result
}

func (*GetPromiseCompletion) Type() MessageType      { return TypeGetPromiseCompletion }
func (m *GetPromiseCompletion) CompletionID() uint32 { return m.ID }
func (m *GetPromiseCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, valueFailure)
}
func (m *GetPromiseCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, valueFailure)
}

// PeekPromiseCompletion (0x800A) answers PeekPromise: Void when the promise is not completed
// yet, Value or Failure.
type PeekPromiseCompletion struct {
	ID uint32 // 1
	Result
}

func (*PeekPromiseCompletion) Type() MessageType      { return TypePeekPromiseCompletion }
func (m *PeekPromiseCompletion) CompletionID() uint32 { return m.ID }
func (m *PeekPromiseCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, voidValueFailure)
}
func (m *PeekPromiseCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, voidValueFailure)
}

// CompletePromiseCompletion (0x800B) answers CompletePromise: Void when this completion won,
// Failure when the promise already had a result.
type CompletePromiseCompletion struct {
	ID uint32 // 1
	Result
}

func (*CompletePromiseCompletion) Type() MessageType      { return TypeCompletePromiseCompletion }
func (m *CompletePromiseCompletion) CompletionID() uint32 { return m.ID }
func (m *CompletePromiseCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, voidFailure)
}
func (m *CompletePromiseCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, voidFailure)
}

// SleepCompletion (0x800C) answers Sleep. Void (4) is a plain field here, not a oneof member.
type SleepCompletion struct {
	ID   uint32 // 1
	Void bool   // 4
}

func (*SleepCompletion) Type() MessageType      { return TypeSleepCompletion }
func (m *SleepCompletion) CompletionID() uint32 { return m.ID }

func (m *SleepCompletion) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.ID)
	return appendVoid(b, 4, m.Void)
}

func (m *SleepCompletion) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ID, err = r.uint32Field(field, wt)
		case 4:
			if err = r.void(field, wt); err == nil {
				m.Void = true
			}
		default:
			return false, nil
		}
		return true, err
	})
}

// CallCompletion (0x800D) carries a call's result: Value or Failure.
type CallCompletion struct {
	ID uint32 // 1
	Result
}

func (*CallCompletion) Type() MessageType      { return TypeCallCompletion }
func (m *CallCompletion) CompletionID() uint32 { return m.ID }
func (m *CallCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, valueFailure)
}
func (m *CallCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, valueFailure)
}

// CallInvocationIDCompletion (0x800E) carries the callee's invocation id, for Call and
// OneWayCall.
type CallInvocationIDCompletion struct {
	ID           uint32 // 1
	InvocationID string // 16
}

func (*CallInvocationIDCompletion) Type() MessageType      { return TypeCallInvocationIDCompletion }
func (m *CallInvocationIDCompletion) CompletionID() uint32 { return m.ID }

func (m *CallInvocationIDCompletion) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.ID)
	return appendString(b, 16, m.InvocationID)
}

func (m *CallInvocationIDCompletion) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ID, err = r.uint32Field(field, wt)
		case 16:
			m.InvocationID, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// RunCompletion (0x8011) carries the durable result of a run: Value or Failure.
type RunCompletion struct {
	ID uint32 // 1
	Result
}

func (*RunCompletion) Type() MessageType      { return TypeRunCompletion }
func (m *RunCompletion) CompletionID() uint32 { return m.ID }
func (m *RunCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, valueFailure)
}
func (m *RunCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, valueFailure)
}

// AttachInvocationCompletion (0x8012) carries the attached invocation's result: Value or
// Failure.
type AttachInvocationCompletion struct {
	ID uint32 // 1
	Result
}

func (*AttachInvocationCompletion) Type() MessageType      { return TypeAttachInvocationCompletion }
func (m *AttachInvocationCompletion) CompletionID() uint32 { return m.ID }
func (m *AttachInvocationCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, valueFailure)
}
func (m *AttachInvocationCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, valueFailure)
}

// GetInvocationOutputCompletion (0x8013) answers GetInvocationOutput: Void when the invocation
// has not finished yet, Value or Failure.
type GetInvocationOutputCompletion struct {
	ID uint32 // 1
	Result
}

func (*GetInvocationOutputCompletion) Type() MessageType      { return TypeGetInvocationOutputCompletion }
func (m *GetInvocationOutputCompletion) CompletionID() uint32 { return m.ID }
func (m *GetInvocationOutputCompletion) AppendPayload(b []byte) []byte {
	return appendCompletion(b, m.ID, &m.Result, voidValueFailure)
}
func (m *GetInvocationOutputCompletion) Unmarshal(p []byte) error {
	return unmarshalCompletion(p, &m.ID, &m.Result, voidValueFailure)
}

// SignalNotification (0xFBFF) delivers a signal: numbered (built-in such as SignalCancel, or
// an awakeable) or named, with Void, Value or Failure.
type SignalNotification struct {
	Signal SignalID // 2 or 3
	Result
}

func (*SignalNotification) Type() MessageType { return TypeSignal }

func (m *SignalNotification) AppendPayload(b []byte) []byte {
	b = m.Signal.appendProto(b)
	return appendResult(b, &m.Result, notificationResult)
}

func (m *SignalNotification) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field == 2 || field == 3 {
			return m.Signal.read(r, field, wt)
		}
		return readResult(r, field, wt, &m.Result, notificationResult)
	})
}
