// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !e2e

package e2e

import "testing"

// Without the e2e build tag nothing here runs; say so instead of reporting "no tests".
func TestEndToEnd(t *testing.T) {
	t.Skip("the end-to-end tests need the e2e build tag and the cairnflow binary: " +
		"cargo build --bin cairnflow && go test -tags e2e ./e2e/...")
}
