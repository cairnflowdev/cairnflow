// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import "github.com/cairnflowdev/cairnflow/sdk/go/internal/hook"

// The handler lookup of package cairntest, which runs handlers on the turn machine
// without a server.
func init() {
	hook.Lookup = func(service any, name string) (hook.Handler, bool) {
		s, ok := service.(*ServiceDef)
		if !ok || s == nil {
			return hook.Handler{}, false
		}
		h, ok := s.Handler(name)
		if !ok {
			return hook.Handler{}, false
		}
		return hook.Handler{Body: h.body, LogLevel: s.logLevel(h)}, true
	}
}
