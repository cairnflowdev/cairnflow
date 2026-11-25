// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

// Commands (0x0400+) are journal entries. Every command carries an optional entry name in
// field 12 (SendSignal calls it EntryName); commands that complete carry the completion id in
// field 11.

// Command is implemented by every command message.
type Command interface {
	Message
	// EntryName is the command's name (field 12), the step name of a run.
	EntryName() string
}

// Completable is implemented by commands that a notification completes. For Call,
// CompletionID is the result's id; its invocation id arrives on InvocationIDNotificationIdx.
type Completable interface {
	Command
	CompletionID() uint32
}

// ─────────────────────────────────────────────────────────── Input / Output

// Input is entry 0 of every journal (0x0400): the request.
type Input struct {
	Headers []Header // 1
	Name    string   // 12
	Value   *Value   // 14
}

func (*Input) Type() MessageType   { return TypeInput }
func (m *Input) EntryName() string { return m.Name }

func (m *Input) AppendPayload(b []byte) []byte {
	for i := range m.Headers {
		b = appendMessage(b, 1, &m.Headers[i])
	}
	b = appendString(b, 12, m.Name)
	if m.Value != nil {
		b = appendMessage(b, 14, m.Value)
	}
	return b
}

func (m *Input) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			var h Header
			if err = r.nested(field, wt, &h); err == nil {
				m.Headers = append(m.Headers, h)
			}
		case 12:
			m.Name, err = r.stringField(field, wt)
		case 14:
			if m.Value == nil {
				m.Value = new(Value)
			}
			err = r.nested(field, wt, m.Value)
		default:
			return false, nil
		}
		return true, err
	})
}

// Output is the invocation's result (0x0401): Value (14) or Failure (15).
type Output struct {
	Name string // 12
	Result
}

var outputResult = resultTags{0, 14, 15}

func (*Output) Type() MessageType   { return TypeOutput }
func (m *Output) EntryName() string { return m.Name }

func (m *Output) AppendPayload(b []byte) []byte {
	b = appendString(b, 12, m.Name)
	return appendResult(b, &m.Result, outputResult)
}

func (m *Output) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field == 12 {
			var err error
			m.Name, err = r.stringField(field, wt)
			return true, err
		}
		return readResult(r, field, wt, &m.Result, outputResult)
	})
}

// ─────────────────────────────────────────────────────────── state

// GetState reads a state key lazily (0x0402); completed by GetStateCompletion.
type GetState struct {
	Key                []byte // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*GetState) Type() MessageType      { return TypeGetState }
func (m *GetState) EntryName() string    { return m.Name }
func (m *GetState) CompletionID() uint32 { return m.ResultCompletionID }

func (m *GetState) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.Key)
	b = appendUint32(b, 11, m.ResultCompletionID)
	return appendString(b, 12, m.Name)
}

func (m *GetState) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.bytesField(field, wt)
		case 11:
			m.ResultCompletionID, err = r.uint32Field(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// SetState writes a state key (0x0403).
type SetState struct {
	Key   []byte // 1
	Value *Value // 3
	Name  string // 12
}

func (*SetState) Type() MessageType   { return TypeSetState }
func (m *SetState) EntryName() string { return m.Name }

func (m *SetState) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.Key)
	if m.Value != nil {
		b = appendMessage(b, 3, m.Value)
	}
	return appendString(b, 12, m.Name)
}

func (m *SetState) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.bytesField(field, wt)
		case 3:
			if m.Value == nil {
				m.Value = new(Value)
			}
			err = r.nested(field, wt, m.Value)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ClearState clears a state key (0x0404).
type ClearState struct {
	Key  []byte // 1
	Name string // 12
}

func (*ClearState) Type() MessageType   { return TypeClearState }
func (m *ClearState) EntryName() string { return m.Name }

func (m *ClearState) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.Key)
	return appendString(b, 12, m.Name)
}

func (m *ClearState) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.bytesField(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ClearAllState clears every state key (0x0405).
type ClearAllState struct {
	Name string // 12
}

func (*ClearAllState) Type() MessageType   { return TypeClearAllState }
func (m *ClearAllState) EntryName() string { return m.Name }

func (m *ClearAllState) AppendPayload(b []byte) []byte { return appendString(b, 12, m.Name) }

func (m *ClearAllState) Unmarshal(p []byte) error { return unmarshalNameOnly(p, &m.Name) }

func unmarshalNameOnly(p []byte, name *string) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 12 {
			return false, nil
		}
		var err error
		*name, err = r.stringField(field, wt)
		return true, err
	})
}

// GetStateKeys lists the state keys lazily (0x0406); completed by GetStateKeysCompletion.
type GetStateKeys struct {
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*GetStateKeys) Type() MessageType      { return TypeGetStateKeys }
func (m *GetStateKeys) EntryName() string    { return m.Name }
func (m *GetStateKeys) CompletionID() uint32 { return m.ResultCompletionID }

func (m *GetStateKeys) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 11, m.ResultCompletionID)
	return appendString(b, 12, m.Name)
}

func (m *GetStateKeys) Unmarshal(p []byte) error {
	return unmarshalIDName(p, &m.ResultCompletionID, &m.Name)
}

func unmarshalIDName(p []byte, id *uint32, name *string) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 11:
			*id, err = r.uint32Field(field, wt)
		case 12:
			*name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// GetEagerState records a state read answered from Start.StateMap (0x0407). Not completable:
// the result is embedded, Void (13) when the key did not exist or Value (14), so a replay
// returns exactly what was read.
type GetEagerState struct {
	Key  []byte // 1
	Name string // 12
	Result
}

var eagerResult = resultTags{13, 14, 0}

func (*GetEagerState) Type() MessageType   { return TypeGetEagerState }
func (m *GetEagerState) EntryName() string { return m.Name }

func (m *GetEagerState) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.Key)
	b = appendString(b, 12, m.Name)
	return appendResult(b, &m.Result, eagerResult)
}

func (m *GetEagerState) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.bytesField(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return readResult(r, field, wt, &m.Result, eagerResult)
		}
		return true, err
	})
}

// GetEagerStateKeys records a key listing answered from Start.StateMap (0x0408). Not
// completable: the keys are embedded.
type GetEagerStateKeys struct {
	Name  string     // 12
	Value *StateKeys // 14
}

func (*GetEagerStateKeys) Type() MessageType   { return TypeGetEagerStateKeys }
func (m *GetEagerStateKeys) EntryName() string { return m.Name }

func (m *GetEagerStateKeys) AppendPayload(b []byte) []byte {
	b = appendString(b, 12, m.Name)
	if m.Value != nil {
		b = appendMessage(b, 14, m.Value)
	}
	return b
}

func (m *GetEagerStateKeys) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 12:
			m.Name, err = r.stringField(field, wt)
		case 14:
			if m.Value == nil {
				m.Value = new(StateKeys)
			}
			err = r.nested(field, wt, m.Value)
		default:
			return false, nil
		}
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── promises

// GetPromise waits for a durable promise (0x0409); completed by GetPromiseCompletion.
type GetPromise struct {
	Key                string // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*GetPromise) Type() MessageType      { return TypeGetPromise }
func (m *GetPromise) EntryName() string    { return m.Name }
func (m *GetPromise) CompletionID() uint32 { return m.ResultCompletionID }

func (m *GetPromise) AppendPayload(b []byte) []byte {
	return appendKeyIDName(b, m.Key, m.ResultCompletionID, m.Name)
}

func (m *GetPromise) Unmarshal(p []byte) error {
	return unmarshalKeyIDName(p, &m.Key, &m.ResultCompletionID, &m.Name)
}

func appendKeyIDName(b []byte, key string, id uint32, name string) []byte {
	b = appendString(b, 1, key)
	b = appendUint32(b, 11, id)
	return appendString(b, 12, name)
}

func unmarshalKeyIDName(p []byte, key *string, id *uint32, name *string) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			*key, err = r.stringField(field, wt)
		case 11:
			*id, err = r.uint32Field(field, wt)
		case 12:
			*name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// PeekPromise looks at a durable promise without waiting (0x040A); completed by
// PeekPromiseCompletion.
type PeekPromise struct {
	Key                string // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*PeekPromise) Type() MessageType      { return TypePeekPromise }
func (m *PeekPromise) EntryName() string    { return m.Name }
func (m *PeekPromise) CompletionID() uint32 { return m.ResultCompletionID }

func (m *PeekPromise) AppendPayload(b []byte) []byte {
	return appendKeyIDName(b, m.Key, m.ResultCompletionID, m.Name)
}

func (m *PeekPromise) Unmarshal(p []byte) error {
	return unmarshalKeyIDName(p, &m.Key, &m.ResultCompletionID, &m.Name)
}

// CompletePromise completes a durable promise (0x040B) with CompletionValue (2) or
// CompletionFailure (3), a oneof; completed by CompletePromiseCompletion.
type CompletePromise struct {
	Key                string   // 1
	CompletionValue    *Value   // 2, oneof completion
	CompletionFailure  *Failure // 3, oneof completion
	ResultCompletionID uint32   // 11
	Name               string   // 12
}

func (*CompletePromise) Type() MessageType      { return TypeCompletePromise }
func (m *CompletePromise) EntryName() string    { return m.Name }
func (m *CompletePromise) CompletionID() uint32 { return m.ResultCompletionID }

var completePromiseResult = resultTags{0, 2, 3}

func (m *CompletePromise) AppendPayload(b []byte) []byte {
	b = appendString(b, 1, m.Key)
	b = appendResult(b, &Result{Value: m.CompletionValue, Failure: m.CompletionFailure}, completePromiseResult)
	b = appendUint32(b, 11, m.ResultCompletionID)
	return appendString(b, 12, m.Name)
}

func (m *CompletePromise) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Key, err = r.stringField(field, wt)
		case 2, 3:
			res := Result{Value: m.CompletionValue, Failure: m.CompletionFailure}
			_, err = readResult(r, field, wt, &res, completePromiseResult)
			if err == nil {
				m.CompletionValue, m.CompletionFailure = res.Value, res.Failure
			}
		case 11:
			m.ResultCompletionID, err = r.uint32Field(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── sleep, calls, signals, run

// Sleep waits until an absolute instant (0x040C); completed by SleepCompletion.
type Sleep struct {
	// WakeUpTime is milliseconds since the Unix epoch, an instant, not a duration.
	WakeUpTime         uint64 // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*Sleep) Type() MessageType      { return TypeSleep }
func (m *Sleep) EntryName() string    { return m.Name }
func (m *Sleep) CompletionID() uint32 { return m.ResultCompletionID }

func (m *Sleep) AppendPayload(b []byte) []byte {
	b = appendUint64(b, 1, m.WakeUpTime)
	b = appendUint32(b, 11, m.ResultCompletionID)
	return appendString(b, 12, m.Name)
}

func (m *Sleep) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.WakeUpTime, err = r.uint64Field(field, wt)
		case 11:
			m.ResultCompletionID, err = r.uint32Field(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// Call invokes another handler and waits for its result (0x040D). Two notifications complete
// it: CallInvocationIDCompletion (InvocationIDNotificationIdx) at once, CallCompletion
// (ResultCompletionID) when the callee ends.
type Call struct {
	ServiceName                 string        // 1
	HandlerName                 string        // 2
	Parameter                   []byte        // 3
	Headers                     []Header      // 4
	Key                         string        // 5, empty when the target has no key
	IdempotencyKey              *string       // 6, optional
	Scope                       *string       // 7, optional, wire v2
	LimitKey                    *string       // 8, optional, wire v2
	InvocationIDNotificationIdx uint32        // 10
	ResultCompletionID          uint32        // 11
	Name                        string        // 12
	Placement                   CallPlacement // 13
	Tags                        []Header      // 14
}

func (*Call) Type() MessageType      { return TypeCall }
func (m *Call) EntryName() string    { return m.Name }
func (m *Call) CompletionID() uint32 { return m.ResultCompletionID }

func (m *Call) AppendPayload(b []byte) []byte {
	b = appendString(b, 1, m.ServiceName)
	b = appendString(b, 2, m.HandlerName)
	b = appendBytes(b, 3, m.Parameter)
	for i := range m.Headers {
		b = appendMessage(b, 4, &m.Headers[i])
	}
	b = appendString(b, 5, m.Key)
	b = appendOptString(b, 6, m.IdempotencyKey)
	b = appendOptString(b, 7, m.Scope)
	b = appendOptString(b, 8, m.LimitKey)
	b = appendUint32(b, 10, m.InvocationIDNotificationIdx)
	b = appendUint32(b, 11, m.ResultCompletionID)
	b = appendString(b, 12, m.Name)
	b = appendEnum(b, 13, int32(m.Placement))
	for i := range m.Tags {
		b = appendMessage(b, 14, &m.Tags[i])
	}
	return b
}

func (m *Call) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ServiceName, err = r.stringField(field, wt)
		case 2:
			m.HandlerName, err = r.stringField(field, wt)
		case 3:
			m.Parameter, err = r.bytesField(field, wt)
		case 4:
			var h Header
			if err = r.nested(field, wt, &h); err == nil {
				m.Headers = append(m.Headers, h)
			}
		case 5:
			m.Key, err = r.stringField(field, wt)
		case 6:
			m.IdempotencyKey, err = optString(r, field, wt)
		case 7:
			m.Scope, err = optString(r, field, wt)
		case 8:
			m.LimitKey, err = optString(r, field, wt)
		case 10:
			m.InvocationIDNotificationIdx, err = r.uint32Field(field, wt)
		case 11:
			m.ResultCompletionID, err = r.uint32Field(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		case 13:
			var v int32
			v, err = r.enumField(field, wt)
			m.Placement = CallPlacement(v)
		case 14:
			var h Header
			if err = r.nested(field, wt, &h); err == nil {
				m.Tags = append(m.Tags, h)
			}
		default:
			return false, nil
		}
		return true, err
	})
}

// OneWayCall invokes another handler without waiting (0x040E); completed only by
// CallInvocationIDCompletion (InvocationIDNotificationIdx).
type OneWayCall struct {
	ServiceName string // 1
	HandlerName string // 2
	Parameter   []byte // 3
	// InvokeTime is when to run, milliseconds since the Unix epoch; 0 or past = now.
	InvokeTime                  uint64        // 4
	Headers                     []Header      // 5
	Key                         string        // 6
	IdempotencyKey              *string       // 7, optional
	Scope                       *string       // 8, optional, wire v2
	LimitKey                    *string       // 9, optional, wire v2
	InvocationIDNotificationIdx uint32        // 10
	Name                        string        // 12
	Placement                   CallPlacement // 13
	Tags                        []Header      // 14
}

func (*OneWayCall) Type() MessageType   { return TypeOneWayCall }
func (m *OneWayCall) EntryName() string { return m.Name }

// CompletionID is InvocationIDNotificationIdx, the only completion of a one-way call.
func (m *OneWayCall) CompletionID() uint32 { return m.InvocationIDNotificationIdx }

func (m *OneWayCall) AppendPayload(b []byte) []byte {
	b = appendString(b, 1, m.ServiceName)
	b = appendString(b, 2, m.HandlerName)
	b = appendBytes(b, 3, m.Parameter)
	b = appendUint64(b, 4, m.InvokeTime)
	for i := range m.Headers {
		b = appendMessage(b, 5, &m.Headers[i])
	}
	b = appendString(b, 6, m.Key)
	b = appendOptString(b, 7, m.IdempotencyKey)
	b = appendOptString(b, 8, m.Scope)
	b = appendOptString(b, 9, m.LimitKey)
	b = appendUint32(b, 10, m.InvocationIDNotificationIdx)
	b = appendString(b, 12, m.Name)
	b = appendEnum(b, 13, int32(m.Placement))
	for i := range m.Tags {
		b = appendMessage(b, 14, &m.Tags[i])
	}
	return b
}

func (m *OneWayCall) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ServiceName, err = r.stringField(field, wt)
		case 2:
			m.HandlerName, err = r.stringField(field, wt)
		case 3:
			m.Parameter, err = r.bytesField(field, wt)
		case 4:
			m.InvokeTime, err = r.uint64Field(field, wt)
		case 5:
			var h Header
			if err = r.nested(field, wt, &h); err == nil {
				m.Headers = append(m.Headers, h)
			}
		case 6:
			m.Key, err = r.stringField(field, wt)
		case 7:
			m.IdempotencyKey, err = optString(r, field, wt)
		case 8:
			m.Scope, err = optString(r, field, wt)
		case 9:
			m.LimitKey, err = optString(r, field, wt)
		case 10:
			m.InvocationIDNotificationIdx, err = r.uint32Field(field, wt)
		case 12:
			m.Name, err = r.stringField(field, wt)
		case 13:
			var v int32
			v, err = r.enumField(field, wt)
			m.Placement = CallPlacement(v)
		case 14:
			var h Header
			if err = r.nested(field, wt, &h); err == nil {
				m.Tags = append(m.Tags, h)
			}
		default:
			return false, nil
		}
		return true, err
	})
}

// SignalID is the signal_id oneof: a numbered signal (Idx, built-in or awakeable) or a named
// one (Name). At most one is set.
type SignalID struct {
	Idx  *uint32 // 2
	Name *string // 3
}

// SignalIdx returns a numbered SignalID.
func SignalIdx(idx uint32) SignalID { return SignalID{Idx: &idx} }

// SignalName returns a named SignalID.
func SignalName(name string) SignalID { return SignalID{Name: &name} }

func (s *SignalID) appendProto(b []byte) []byte {
	if s.Name != nil {
		return appendOptString(b, 3, s.Name)
	}
	return appendOptUint32(b, 2, s.Idx)
}

func (s *SignalID) read(r *reader, field, wt int) (bool, error) {
	switch field {
	case 2:
		v, err := r.uint32Field(field, wt)
		if err == nil {
			*s = SignalID{Idx: &v}
		}
		return true, err
	case 3:
		v, err := r.stringField(field, wt)
		if err == nil {
			*s = SignalID{Name: &v}
		}
		return true, err
	}
	return false, nil
}

// SendSignal sends a signal to another invocation (0x0410). Not completable. The result is
// Void (4), Value (5) or Failure (6).
type SendSignal struct {
	TargetInvocationID string // 1
	Signal             SignalID
	Result
	Name string // 12 (entry_name)
}

func (*SendSignal) Type() MessageType   { return TypeSendSignal }
func (m *SendSignal) EntryName() string { return m.Name }

func (m *SendSignal) AppendPayload(b []byte) []byte {
	b = appendString(b, 1, m.TargetInvocationID)
	b = m.Signal.appendProto(b)
	b = appendResult(b, &m.Result, notificationResult)
	return appendString(b, 12, m.Name)
}

func (m *SendSignal) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		switch field {
		case 1:
			var err error
			m.TargetInvocationID, err = r.stringField(field, wt)
			return true, err
		case 2, 3:
			return m.Signal.read(r, field, wt)
		case 12:
			var err error
			m.Name, err = r.stringField(field, wt)
			return true, err
		}
		return readResult(r, field, wt, &m.Result, notificationResult)
	})
}

// Run journals a run step (0x0411); the closure's result goes through ProposeRunCompletion and
// comes back as RunCompletion.
type Run struct {
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*Run) Type() MessageType      { return TypeRun }
func (m *Run) EntryName() string    { return m.Name }
func (m *Run) CompletionID() uint32 { return m.ResultCompletionID }

func (m *Run) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 11, m.ResultCompletionID)
	return appendString(b, 12, m.Name)
}

func (m *Run) Unmarshal(p []byte) error { return unmarshalIDName(p, &m.ResultCompletionID, &m.Name) }

// ─────────────────────────────────────────────────────────── attach

// AttachInvocation waits for the result of another invocation (0x0412); completed by
// AttachInvocationCompletion.
type AttachInvocation struct {
	InvocationID       string // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*AttachInvocation) Type() MessageType      { return TypeAttachInvocation }
func (m *AttachInvocation) EntryName() string    { return m.Name }
func (m *AttachInvocation) CompletionID() uint32 { return m.ResultCompletionID }

func (m *AttachInvocation) AppendPayload(b []byte) []byte {
	return appendKeyIDName(b, m.InvocationID, m.ResultCompletionID, m.Name)
}

func (m *AttachInvocation) Unmarshal(p []byte) error {
	return unmarshalKeyIDName(p, &m.InvocationID, &m.ResultCompletionID, &m.Name)
}

// GetInvocationOutput looks at the result of another invocation without waiting (0x0413);
// completed by GetInvocationOutputCompletion.
type GetInvocationOutput struct {
	InvocationID       string // 1
	ResultCompletionID uint32 // 11
	Name               string // 12
}

func (*GetInvocationOutput) Type() MessageType      { return TypeGetInvocationOutput }
func (m *GetInvocationOutput) EntryName() string    { return m.Name }
func (m *GetInvocationOutput) CompletionID() uint32 { return m.ResultCompletionID }

func (m *GetInvocationOutput) AppendPayload(b []byte) []byte {
	return appendKeyIDName(b, m.InvocationID, m.ResultCompletionID, m.Name)
}

func (m *GetInvocationOutput) Unmarshal(p []byte) error {
	return unmarshalKeyIDName(p, &m.InvocationID, &m.ResultCompletionID, &m.Name)
}
