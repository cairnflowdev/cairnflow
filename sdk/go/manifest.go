// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"time"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/core"
)

// defaultInactivityTimeout is the service-level inactivityTimeout the manifest declares
// when a service declares none: 15 minutes (the runtime clamps it to its own cap).
const defaultInactivityTimeout = 15 * time.Minute

// Wire versions this SDK speaks (docs/protocol-v1.md §2).
const (
	minProtocolVersion = 1
	maxProtocolVersion = 6
	// replayDeltaVersion is the first wire version with incremental replay (docs/protocol-v1.md §9.3).
	replayDeltaVersion = 5
)

// manifestOptions are the endpoint-level fields of the manifest.
type manifestOptions struct {
	// bidi declares BIDI_STREAM; otherwise REQUEST_RESPONSE.
	bidi bool
	// maxWire is the highest wire version declared (maxProtocolVersion when 0).
	maxWire int
	// requestIdentity: the endpoint verifies request identity (scheme "v1").
	requestIdentity bool
	// compression: the endpoint accepts gzip-compressed REQUEST_RESPONSE bodies.
	compression bool
	// replayCache: the endpoint keeps journals and accepts deltas (wire v5).
	replayCache bool
}

// manifestJSON is the discovery manifest (docs/protocol-v1.md §3): byte for byte what
// the TypeScript SDK's manifest() produces for the same definitions.
func manifestJSON(o manifestOptions, services ...*ServiceDef) []byte {
	maxWire := o.maxWire
	if maxWire == 0 {
		maxWire = maxProtocolVersion
	}
	var m core.Object
	if o.bidi {
		m.Field("protocolMode", "BIDI_STREAM")
	} else {
		m.Field("protocolMode", "REQUEST_RESPONSE")
	}
	m.Field("minProtocolVersion", minProtocolVersion)
	m.Field("maxProtocolVersion", maxWire)
	if o.requestIdentity {
		m.Field("requestIdentity", "v1")
	}
	if o.compression {
		m.Field("requestCompression", "gzip")
	}
	if o.replayCache && maxWire >= replayDeltaVersion {
		m.Field("replayCache", true)
	}
	list := []byte{'['}
	for i, s := range services {
		if i > 0 {
			list = append(list, ',')
		}
		var so core.Object
		so.Field("name", s.name)
		so.Field("ty", string(s.ty))
		inactivity := defaultInactivityTimeout
		if s.options != nil && s.options.InactivityTimeout > 0 {
			inactivity = s.options.InactivityTimeout
		}
		so.Field("inactivityTimeout", inactivity.Milliseconds())
		contract(&so, s.options, true)
		hs := []byte{'['}
		for j, h := range s.handlers {
			if j > 0 {
				hs = append(hs, ',')
			}
			var ho core.Object
			ho.Field("name", h.name)
			if h.ty != "" {
				ho.Field("ty", string(h.ty))
			}
			contract(&ho, h.options, false)
			hs = append(hs, ho.Bytes()...)
		}
		hs = append(hs, ']')
		so.Raw("handlers", hs)
		list = append(list, so.Bytes()...)
	}
	list = append(list, ']')
	m.Raw("services", list)
	return m.Bytes()
}

// contract writes the manifest fields of a contract, with the names of Restate's
// endpoint manifest. skipInactivity: the service level already wrote inactivityTimeout.
func contract(o *core.Object, h *HandlerOptions, skipInactivity bool) {
	if h == nil {
		return
	}
	if h.InactivityTimeout > 0 && !skipInactivity {
		o.Field("inactivityTimeout", h.InactivityTimeout.Milliseconds())
	}
	if h.JournalRetention > 0 {
		o.Field("journalRetention", h.JournalRetention.Milliseconds())
	}
	if h.RuntimeTrace {
		o.Field("trace", "runtime")
	}
	if h.LogFlush > 0 {
		o.Field("logFlushMs", h.LogFlush.Milliseconds())
	}
	if r := h.RetryPolicy; r != nil {
		if r.InitialInterval > 0 {
			o.Field("retryPolicyInitialInterval", r.InitialInterval.Milliseconds())
		}
		if r.MaxInterval > 0 {
			o.Field("retryPolicyMaxInterval", r.MaxInterval.Milliseconds())
		}
		if r.ExponentiationFactor > 0 {
			o.Field("retryPolicyExponentiationFactor", r.ExponentiationFactor)
		}
		if r.MaxAttempts > 0 {
			o.Field("retryPolicyMaxAttempts", r.MaxAttempts)
		}
		if r.OnMaxAttempts != "" {
			o.Field("retryPolicyOnMaxAttempts", string(r.OnMaxAttempts))
		}
	}
}
