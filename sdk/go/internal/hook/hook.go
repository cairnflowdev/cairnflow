// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package hook lets packages of this module that sit outside package cairn (cairntest)
// reach what a declared handler runs, without making it public API. Package cairn sets
// the hooks when it is initialised; a package that imports cairn can use them.
package hook

import "github.com/cairnflowdev/cairnflow/sdk/go/internal/core"

// Handler is what the turn machine needs to run one handler of a service.
type Handler struct {
	// Body is the handler as the core runs it.
	Body core.Body
	// LogLevel is the handler's minimum durable log level ("" = info).
	LogLevel string
}

// Lookup returns the handler named handler of service, a *cairn.ServiceDef. Package cairn
// sets it.
var Lookup func(service any, handler string) (Handler, bool)
