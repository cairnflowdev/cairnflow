// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package e2e runs the Go SDK against the real cairnflow binary: the Rust runtime drives
// Go services over real HTTP, in REQUEST_RESPONSE and BIDI_STREAM.
//
// The tests carry the build tag e2e and need the binary:
//
//	cargo build --bin cairnflow
//	cd sdk/go && go test -tags e2e ./e2e/...
//
// CAIRNFLOW_BINARY points at another binary. CAIRNFLOW_E2E_ARGS runs the same suite on
// another shape, for example a 4-partition cluster:
//
//	CAIRNFLOW_E2E_ARGS="--main-store memory:// --cluster --partitions 4 --tick-interval-ms 100" \
//	  go test -tags e2e ./e2e/...
//
// A missing binary skips the tests, unless CAIRNFLOW_E2E_REQUIRE=1 (the gate sets it),
// which fails them instead.
//
// Every test uses its own object keys and its own runtime, so the tests do not depend on
// the order they run in.
package e2e
