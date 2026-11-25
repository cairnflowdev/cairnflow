// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package wire

// Message is a protobuf message that travels as a frame. Every message type of section 5 has
// one implementation in this package (pointer receivers); custom frames are *Custom.
type Message interface {
	// Type is the frame type code of the message.
	Type() MessageType
	// AppendPayload appends the canonical protobuf encoding of the message to b.
	AppendPayload(b []byte) []byte
	// Unmarshal decodes payload into the message. Decoding into a message that already
	// holds values merges, as protobuf does; decode into a zero value for a fresh result.
	Unmarshal(payload []byte) error
}

// Marshal returns the canonical protobuf encoding of m (the frame payload).
func Marshal(m Message) []byte { return m.AppendPayload(nil) }

// EncodeMessage returns m as a whole frame with the given flags.
func EncodeMessage(m Message, flags uint16) []byte {
	return AppendMessage(nil, m, flags)
}

// AppendMessage appends m as a whole frame with the given flags to dst.
func AppendMessage(dst []byte, m Message, flags uint16) []byte {
	at := len(dst)
	dst = AppendHeader(dst, FrameHeader{Type: m.Type(), Flags: flags})
	dst = m.AppendPayload(dst)
	n := len(dst) - at - HeaderLen
	dst[at+4] = byte(n >> 24)
	dst[at+5] = byte(n >> 16)
	dst[at+6] = byte(n >> 8)
	dst[at+7] = byte(n)
	return dst
}

// CombinatorType says how a suspension node is satisfied.
type CombinatorType int32

const (
	// CombinatorUnknown is read as CombinatorFirstCompleted, so a v1 suspension (a flat
	// list) means "wake when any of them completes".
	CombinatorUnknown CombinatorType = 0
	// CombinatorFirstCompleted: any leaf or child node is satisfied.
	CombinatorFirstCompleted CombinatorType = 1
	// CombinatorAllCompleted: every leaf and every child node is satisfied.
	CombinatorAllCompleted CombinatorType = 2
)

// ErrorBehavior says what the runtime does after an ErrorMessage.
type ErrorBehavior int32

const (
	// BehaviorRetry is 0 on purpose: an unset field is read as retry.
	BehaviorRetry ErrorBehavior = 0
	// BehaviorPause pauses the invocation until an operator resumes it.
	BehaviorPause ErrorBehavior = 1
	// BehaviorFail fails the invocation for good.
	BehaviorFail ErrorBehavior = 2
)

// CallPlacement places an unkeyed callee.
type CallPlacement int32

const (
	// PlacementDefault colocates the callee by the caller's partition key prefix.
	PlacementDefault CallPlacement = 0
	// PlacementSpread spreads the callee by hash of (caller, completion).
	PlacementSpread CallPlacement = 1
)

// Built-in signals (BuiltInSignal); 2-15 are reserved for future system signals.
const (
	// SignalCancel is sent by the runtime on a cooperative cancel.
	SignalCancel uint32 = 1
)

// ProtocolVersionV1 is the value of the ProtocolVersion enum.
const ProtocolVersionV1 = 1

// ─────────────────────────────────────────────────────────── Start 0x0000

// Start opens a turn (0x0000, runtime → SDK).
type Start struct {
	// ID is the invocation id; stable across replays.
	ID []byte // 1
	// DebugID is the invocation id for logging and for the admin API.
	DebugID string // 2
	// KnownEntries is the number of stored commands + notifications.
	KnownEntries uint32 // 3
	// StateMap is the eager state: the object's state sent along with the turn.
	StateMap []StateEntry // 4
	// PartialState: StateMap is only part of the state, absent keys must be read lazily.
	// False with an empty map is indistinguishable from a runtime without eager state, so
	// the map is complete only when it is not empty.
	PartialState bool // 5
	// Key is the object or workflow key; empty for a service.
	Key string // 6
	// RetryCountSinceLastStoredEntry counts the retries since the journal last grew. It is
	// part of the request identity's jti.
	RetryCountSinceLastStoredEntry uint32 // 7
	// DurationSinceLastStoredEntry is in milliseconds; not exact.
	DurationSinceLastStoredEntry uint64 // 8
	// RandomSeed seeds the deterministic RNG; stable across replays.
	RandomSeed uint64 // 9
	// IdempotencyKey is the key the invocation was created with, if any.
	IdempotencyKey *string // 10, optional
	// KeepaliveIntervalMs (wire v3): interval of Keepalive frames while a run closure
	// executes; nil or 0 means none.
	KeepaliveIntervalMs *uint64 // 16, optional
	// ReplayOnly (wire v3): a dry-run turn; stop before the first command outside the journal
	// and answer with ReplayReport.
	ReplayOnly bool // 17
	// ReplayFrom (wire v5): the body omits the first ReplayFrom entries; 0 = full replay.
	ReplayFrom uint32 // 18
	// PrefixDigest (wire v5): the hash chain over entries [0, ReplayFrom); 32 bytes.
	PrefixDigest []byte // 19
	// SegmentURLs (wire v6): read URLs of every segment ref of the turn's messages.
	SegmentURLs []SegmentURL // 20
}

func (*Start) Type() MessageType { return TypeStart }

func (m *Start) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.ID)
	b = appendString(b, 2, m.DebugID)
	b = appendUint32(b, 3, m.KnownEntries)
	for i := range m.StateMap {
		b = appendMessage(b, 4, &m.StateMap[i])
	}
	b = appendBool(b, 5, m.PartialState)
	b = appendString(b, 6, m.Key)
	b = appendUint32(b, 7, m.RetryCountSinceLastStoredEntry)
	b = appendUint64(b, 8, m.DurationSinceLastStoredEntry)
	b = appendUint64(b, 9, m.RandomSeed)
	b = appendOptString(b, 10, m.IdempotencyKey)
	b = appendOptUint64(b, 16, m.KeepaliveIntervalMs)
	b = appendBool(b, 17, m.ReplayOnly)
	b = appendUint32(b, 18, m.ReplayFrom)
	b = appendBytes(b, 19, m.PrefixDigest)
	for i := range m.SegmentURLs {
		b = appendMessage(b, 20, &m.SegmentURLs[i])
	}
	return b
}

func (m *Start) Unmarshal(p []byte) error { return m.unmarshalProto(p, 0) }

func (m *Start) unmarshalProto(p []byte, depth int) error {
	return walk(p, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ID, err = r.bytesField(field, wt)
		case 2:
			m.DebugID, err = r.stringField(field, wt)
		case 3:
			m.KnownEntries, err = r.uint32Field(field, wt)
		case 4:
			var e StateEntry
			if err = r.nested(field, wt, &e); err == nil {
				m.StateMap = append(m.StateMap, e)
			}
		case 5:
			m.PartialState, err = r.boolField(field, wt)
		case 6:
			m.Key, err = r.stringField(field, wt)
		case 7:
			m.RetryCountSinceLastStoredEntry, err = r.uint32Field(field, wt)
		case 8:
			m.DurationSinceLastStoredEntry, err = r.uint64Field(field, wt)
		case 9:
			m.RandomSeed, err = r.uint64Field(field, wt)
		case 10:
			var s string
			if s, err = r.stringField(field, wt); err == nil {
				m.IdempotencyKey = &s
			}
		case 16:
			var v uint64
			if v, err = r.uint64Field(field, wt); err == nil {
				m.KeepaliveIntervalMs = &v
			}
		case 17:
			m.ReplayOnly, err = r.boolField(field, wt)
		case 18:
			m.ReplayFrom, err = r.uint32Field(field, wt)
		case 19:
			m.PrefixDigest, err = r.bytesField(field, wt)
		case 20:
			m.SegmentURLs, err = appendSegmentURL(m.SegmentURLs, r, field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── Suspension 0x0001

// Suspension ends a turn waiting on completions and signals (0x0001, SDK → runtime).
//
// It is a tree node: the leaves are completion ids, numbered signals and named signals; the
// children are NestedFutures. A node with CombinatorFirstCompleted (or Unknown) is satisfied
// when any leaf or child is; with CombinatorAllCompleted when every leaf and child is.
type Suspension struct {
	WaitingCompletions  []uint32       // 1, packed
	WaitingSignals      []uint32       // 2, packed
	WaitingNamedSignals []string       // 3
	NestedFutures       []Suspension   // 4
	CombinatorType      CombinatorType // 5
}

func (*Suspension) Type() MessageType { return TypeSuspension }

func (m *Suspension) AppendPayload(b []byte) []byte { return m.appendProto(b) }

func (m *Suspension) appendProto(b []byte) []byte {
	b = appendPacked(b, 1, m.WaitingCompletions)
	b = appendPacked(b, 2, m.WaitingSignals)
	for _, s := range m.WaitingNamedSignals {
		b = appendStringAlways(b, 3, s)
	}
	for i := range m.NestedFutures {
		b = appendMessage(b, 4, &m.NestedFutures[i])
	}
	return appendEnum(b, 5, int32(m.CombinatorType))
}

func (m *Suspension) Unmarshal(p []byte) error { return m.unmarshalProto(p, 0) }

func (m *Suspension) unmarshalProto(p []byte, depth int) error {
	return walk(p, depth, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.WaitingCompletions, err = r.packedUint32(m.WaitingCompletions, field, wt)
		case 2:
			m.WaitingSignals, err = r.packedUint32(m.WaitingSignals, field, wt)
		case 3:
			var s string
			if s, err = r.stringField(field, wt); err == nil {
				m.WaitingNamedSignals = append(m.WaitingNamedSignals, s)
			}
		case 4:
			var n Suspension
			if err = r.nested(field, wt, &n); err == nil {
				m.NestedFutures = append(m.NestedFutures, n)
			}
		case 5:
			var v int32
			v, err = r.enumField(field, wt)
			m.CombinatorType = CombinatorType(v)
		default:
			return false, nil
		}
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── Error 0x0002

// ErrorMessage ends a turn with an error (0x0002, SDK → runtime).
type ErrorMessage struct {
	// Code is an HTTP status code, or JournalMismatch / ProtocolViolation.
	Code       uint32 // 1
	Message    string // 2
	Stacktrace string // 3
	// RelatedCommandIndex, RelatedCommandName, RelatedCommandType point at the entry that
	// caused the failure, which may lie beyond the stored journal.
	RelatedCommandIndex *uint32 // 4, optional
	RelatedCommandName  *string // 5, optional
	RelatedCommandType  *uint32 // 6, optional
	// NextRetryDelay overrides the retry policy for the next attempt only, in milliseconds.
	NextRetryDelay *uint64       // 8, optional
	Behavior       ErrorBehavior // 9
}

func (*ErrorMessage) Type() MessageType { return TypeError }

func (m *ErrorMessage) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.Code)
	b = appendString(b, 2, m.Message)
	b = appendString(b, 3, m.Stacktrace)
	b = appendOptUint32(b, 4, m.RelatedCommandIndex)
	b = appendOptString(b, 5, m.RelatedCommandName)
	b = appendOptUint32(b, 6, m.RelatedCommandType)
	b = appendOptUint64(b, 8, m.NextRetryDelay)
	return appendEnum(b, 9, int32(m.Behavior))
}

func (m *ErrorMessage) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Code, err = r.uint32Field(field, wt)
		case 2:
			m.Message, err = r.stringField(field, wt)
		case 3:
			m.Stacktrace, err = r.stringField(field, wt)
		case 4:
			m.RelatedCommandIndex, err = optUint32(r, field, wt)
		case 5:
			m.RelatedCommandName, err = optString(r, field, wt)
		case 6:
			m.RelatedCommandType, err = optUint32(r, field, wt)
		case 8:
			var v uint64
			if v, err = r.uint64Field(field, wt); err == nil {
				m.NextRetryDelay = &v
			}
		case 9:
			var v int32
			v, err = r.enumField(field, wt)
			m.Behavior = ErrorBehavior(v)
		default:
			return false, nil
		}
		return true, err
	})
}

func optUint32(r *reader, field, wt int) (*uint32, error) {
	v, err := r.uint32Field(field, wt)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func optString(r *reader, field, wt int) (*string, error) {
	v, err := r.stringField(field, wt)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ─────────────────────────────────────────────────────────── empty messages

// End says user code has returned (0x0003, SDK → runtime). It is not a journal entry.
type End struct{}

func (*End) Type() MessageType             { return TypeEnd }
func (*End) AppendPayload(b []byte) []byte { return b }
func (*End) Unmarshal(p []byte) error      { return voidMessage{}.unmarshalProto(p, 0) }

// Keepalive (wire v3, 0x0008, SDK → runtime, side channel) keeps a turn alive while a run
// closure executes. Empty payload.
type Keepalive struct{}

func (*Keepalive) Type() MessageType             { return TypeKeepalive }
func (*Keepalive) AppendPayload(b []byte) []byte { return b }
func (*Keepalive) Unmarshal(p []byte) error      { return voidMessage{}.unmarshalProto(p, 0) }

// ─────────────────────────────────────────────────────────── acks

// CommandAck reports that a command is durable (0x0004, runtime → SDK). Not a journal entry.
type CommandAck struct {
	CommandIndex uint32 // 1
}

func (*CommandAck) Type() MessageType { return TypeCommandAck }

func (m *CommandAck) AppendPayload(b []byte) []byte { return appendUint32(b, 1, m.CommandIndex) }

func (m *CommandAck) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 1 {
			return false, nil
		}
		var err error
		m.CommandIndex, err = r.uint32Field(field, wt)
		return true, err
	})
}

// ProposeRunCompletionAck reports that a proposed run result is durable (0x0007, runtime →
// SDK, processing phase only). Not a journal entry.
type ProposeRunCompletionAck struct {
	CompletionID uint32 // 1
}

func (*ProposeRunCompletionAck) Type() MessageType { return TypeProposeRunCompletionAck }

func (m *ProposeRunCompletionAck) AppendPayload(b []byte) []byte {
	return appendUint32(b, 1, m.CompletionID)
}

func (m *ProposeRunCompletionAck) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 1 {
			return false, nil
		}
		var err error
		m.CompletionID, err = r.uint32Field(field, wt)
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── ProposeRunCompletion 0x0005

// ProposeRunCompletion proposes the result of a run closure (0x0005, SDK → runtime). It is
// not journaled as is: the result comes back as a RunCompletion notification.
//
// The result is a oneof: Value (field 14, set when HasValue), Failure (15) or SegmentedValue
// (16, wire v6: refs of a segmented value already in this invocation's journal plus the rest).
// Use NewProposeValue, NewProposeFailure or NewProposeSegmented to build one.
type ProposeRunCompletion struct {
	ResultCompletionID uint32 // 1
	Value              []byte // 14, oneof result, present when HasValue
	HasValue           bool
	Failure            *Failure // 15, oneof result
	SegmentedValue     *Value   // 16, oneof result
}

// NewProposeValue proposes an inline value.
func NewProposeValue(completionID uint32, value []byte) *ProposeRunCompletion {
	return &ProposeRunCompletion{ResultCompletionID: completionID, Value: value, HasValue: true}
}

// NewProposeFailure proposes a failure.
func NewProposeFailure(completionID uint32, f *Failure) *ProposeRunCompletion {
	return &ProposeRunCompletion{ResultCompletionID: completionID, Failure: f}
}

// NewProposeSegmented proposes a value that reuses segment refs (wire v6).
func NewProposeSegmented(completionID uint32, v *Value) *ProposeRunCompletion {
	return &ProposeRunCompletion{ResultCompletionID: completionID, SegmentedValue: v}
}

func (*ProposeRunCompletion) Type() MessageType { return TypeProposeRunCompletion }

func (m *ProposeRunCompletion) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.ResultCompletionID)
	switch {
	case m.SegmentedValue != nil:
		return appendMessage(b, 16, m.SegmentedValue)
	case m.Failure != nil:
		return appendMessage(b, 15, m.Failure)
	case m.HasValue:
		return appendBytesAlways(b, 14, m.Value)
	}
	return b
}

func (m *ProposeRunCompletion) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.ResultCompletionID, err = r.uint32Field(field, wt)
		case 14:
			var v []byte
			if v, err = r.bytesField(field, wt); err == nil {
				m.Value, m.HasValue, m.Failure, m.SegmentedValue = v, true, nil, nil
			}
		case 15:
			f := m.Failure
			if f == nil {
				f = new(Failure)
			}
			if err = r.nested(field, wt, f); err == nil {
				m.Value, m.HasValue, m.Failure, m.SegmentedValue = nil, false, f, nil
			}
		case 16:
			v := m.SegmentedValue
			if v == nil {
				v = new(Value)
			}
			if err = r.nested(field, wt, v); err == nil {
				m.Value, m.HasValue, m.Failure, m.SegmentedValue = nil, false, nil, v
			}
		default:
			return false, nil
		}
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── wire v3

// ReplayReport ends a dry-run turn (wire v3, 0x0009, SDK → runtime): the code matches the
// journal up to Matched commands, or diverges there.
type ReplayReport struct {
	Matched  uint32 // 1
	Mismatch bool   // 2
	Expected string // 3
	Got      string // 4
	Message  string // 5
}

func (*ReplayReport) Type() MessageType { return TypeReplayReport }

func (m *ReplayReport) AppendPayload(b []byte) []byte {
	b = appendUint32(b, 1, m.Matched)
	b = appendBool(b, 2, m.Mismatch)
	b = appendString(b, 3, m.Expected)
	b = appendString(b, 4, m.Got)
	return appendString(b, 5, m.Message)
}

func (m *ReplayReport) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Matched, err = r.uint32Field(field, wt)
		case 2:
			m.Mismatch, err = r.boolField(field, wt)
		case 3:
			m.Expected, err = r.stringField(field, wt)
		case 4:
			m.Got, err = r.stringField(field, wt)
		case 5:
			m.Message, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ContinueAsNew ends the turn and restarts the invocation with a new input (wire v3, 0x000A,
// SDK → runtime). It never enters the journal.
type ContinueAsNew struct {
	Input []byte // 1
}

func (*ContinueAsNew) Type() MessageType { return TypeContinueAsNew }

func (m *ContinueAsNew) AppendPayload(b []byte) []byte { return appendBytes(b, 1, m.Input) }

func (m *ContinueAsNew) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 1 {
			return false, nil
		}
		var err error
		m.Input, err = r.bytesField(field, wt)
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── wire v4 side channel

// StreamAppend carries records of a stream (wire v4, 0x000B, SDK → runtime, side channel).
type StreamAppend struct {
	Name    string         // 1
	Durable bool           // 2
	Records []StreamRecord // 3
	Close   bool           // 4
}

func (*StreamAppend) Type() MessageType { return TypeStreamAppend }

func (m *StreamAppend) AppendPayload(b []byte) []byte {
	b = appendString(b, 1, m.Name)
	b = appendBool(b, 2, m.Durable)
	for i := range m.Records {
		b = appendMessage(b, 3, &m.Records[i])
	}
	return appendBool(b, 4, m.Close)
}

func (m *StreamAppend) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Name, err = r.stringField(field, wt)
		case 2:
			m.Durable, err = r.boolField(field, wt)
		case 3:
			var rec StreamRecord
			if err = r.nested(field, wt, &rec); err == nil {
				m.Records = append(m.Records, rec)
			}
		case 4:
			m.Close, err = r.boolField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// ArtifactPut uploads an artifact (wire v4, 0x000C, SDK → runtime, side channel). Sha256 must
// be the SHA-256 of Content.
type ArtifactPut struct {
	Sha256      []byte // 1
	Content     []byte // 2
	ContentType string // 3
}

func (*ArtifactPut) Type() MessageType { return TypeArtifactPut }

func (m *ArtifactPut) AppendPayload(b []byte) []byte {
	b = appendBytes(b, 1, m.Sha256)
	b = appendBytes(b, 2, m.Content)
	return appendString(b, 3, m.ContentType)
}

func (m *ArtifactPut) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		var err error
		switch field {
		case 1:
			m.Sha256, err = r.bytesField(field, wt)
		case 2:
			m.Content, err = r.bytesField(field, wt)
		case 3:
			m.ContentType, err = r.stringField(field, wt)
		default:
			return false, nil
		}
		return true, err
	})
}

// SegmentUrls carries read URLs for the segment refs of the messages that follow it (wire v6,
// 0x000D, runtime → SDK, side channel, BIDI_STREAM mid-turn).
type SegmentUrls struct {
	URLs []SegmentURL // 1
}

func (*SegmentUrls) Type() MessageType { return TypeSegmentUrls }

func (m *SegmentUrls) AppendPayload(b []byte) []byte {
	for i := range m.URLs {
		b = appendMessage(b, 1, &m.URLs[i])
	}
	return b
}

func (m *SegmentUrls) Unmarshal(p []byte) error {
	return walk(p, 0, func(r *reader, field, wt int) (bool, error) {
		if field != 1 {
			return false, nil
		}
		var err error
		m.URLs, err = appendSegmentURL(m.URLs, r, field, wt)
		return true, err
	})
}

// ─────────────────────────────────────────────────────────── custom 0xFC00+

// Custom is a frame of the custom range 0xFC00-0xFFFF: its payload is opaque bytes.
type Custom struct {
	Code    MessageType
	Payload []byte
}

func (m *Custom) Type() MessageType             { return m.Code }
func (m *Custom) AppendPayload(b []byte) []byte { return append(b, m.Payload...) }
func (m *Custom) Unmarshal(p []byte) error {
	m.Payload = append([]byte{}, p...)
	return nil
}
