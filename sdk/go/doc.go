// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package cairn is the Go SDK of Cairnflow, a durable execution engine: handlers whose
// every step is journaled, so a crash, a deploy or a long wait resumes exactly where it
// stopped.
//
// A handler runs on one goroutine with a [Context]. Durable steps are typed functions:
// [Get] and [Set] for state, [Sleep] and [After] for timers, [Run] for side effects,
// [Service], [Object] and [Workflow] for calls, [Signal], [Awakeable] and [Promise] for
// waiting on the outside world. Parallel work is futures, combined with [WaitFirst],
// [Wait], [All] or [Race]; a race is decided by the order of the journal, so a replay
// picks the same winner.
//
//	func submit(ctx cairn.ObjectContext, o Order) (Receipt, error) {
//		total, _, err := cairn.Get[int](ctx, "total")
//		if err != nil {
//			return Receipt{}, err
//		}
//		id, err := cairn.Run(ctx, "charge", func(rc cairn.RunContext) (string, error) {
//			return charge(rc, o.Amount) // rc is a context.Context: I/O, cancellation
//		})
//		if err != nil {
//			return Receipt{}, err
//		}
//		if err := cairn.Set(ctx, "total", total+o.Amount); err != nil {
//			return Receipt{}, err
//		}
//		return Receipt{ID: id, Total: total + o.Amount}, nil
//	}
//
//	var orders = cairn.NewObject("OrderService",
//		cairn.Exclusive("submit", submit),
//		cairn.Shared("total", cairn.NoInput(total)))
//
// Values travel as JSON (encoding/json), with the same bytes the other SDKs write for
// the same value; []byte and json.RawMessage pass through as the value's bytes.
//
// [Serve] runs an HTTP server for a set of services, [HTTPHandler] mounts them in a server of
// your own, [LambdaHandler] serves them on AWS Lambda (REQUEST_RESPONSE only). It serves
// REQUEST_RESPONSE over HTTP/1.1 and, with [Bidi], BIDI_STREAM over h2c (wire v1 to v6). In
// REQUEST_RESPONSE an endpoint that verifies request identity ([IdentityKeys]) keeps the
// journal of each running invocation in memory ([ReplayCache]), so a turn carries only the
// new entries; an unverified endpoint keeps none. Large values (wire v6) arrive as
// refs to segments that the SDK downloads and checks before the handler runs
// ([SegmentOrigins] lists where from); a value written back that starts with a value read
// sends only the new part. An endpoint with neither IdentityKeys nor SegmentOrigins serves
// wire v5 at most and downloads nothing: the runtime sends its large values inline.
package cairn
