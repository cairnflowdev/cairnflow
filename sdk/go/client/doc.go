// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package client calls a Cairnflow cluster from an app or a script through the HTTP ingress
// (docs/api/openapi.json). It uses only the standard library.
//
//	c := client.Connect("https://cairn.example.com", client.WithAPIKey(os.Getenv("CAIRN_KEY")))
//
//	// Call waits for the result; Send returns once the invocation is durable.
//	r, err := client.Call[Receipt](ctx, c.Object("OrderService", "order-42"), "submit", order)
//	h, err := client.Send(ctx, c.Service("Mailer"), "welcome", user)
//	out, err := client.Attach[string](ctx, h, client.Timeout(time.Minute))
//	st, err := h.Status(ctx)
//
//	wf := c.Workflow("Signup", "u1")
//	_, err = client.RunWorkflow[Account](ctx, wf, user)
//	acct, err := client.Attach[Account](ctx, wf)
//
//	_, err = c.Awakeable(id).Resolve(ctx, approval)
//	_, err = c.Promise("Signup", "u1", "email").Resolve(ctx, "a@b.c")
//	_, err = c.Schedules().Put(ctx, "nightly", client.ScheduleDefinition{...})
//
//	for ev, err := range c.ReadStream(ctx, h.ID(), "tokens") { ... }
//
// Rules shared by every call:
//
//   - A 202 never surfaces: the client attaches by id
//     (POST /invocations/{id}/attach?wait_ms=25000) until done or until Timeout runs out
//     (*AttachTimeout; the invocation keeps running).
//   - Every call carries a generated Idempotency-Key, kept across every attempt, so a resend
//     after a 503 or a network error does not create a second invocation. NoIdempotencyKey
//     sends none; a workflow's run sends none by default (it is already once per key).
//   - The client follows the leader of a 503 when it has the same scheme, waits for the
//     Retry-After of a 429 that created nothing, and caps attempts and time per request.
//   - Inputs and values: nil sends no body (the handler sees no input, a signal is Void);
//     json.RawMessage and []byte go out as they are; anything else is JSON from encoding/json
//     without HTML escaping. Outputs decode into the requested type; json.RawMessage and
//     []byte receive the raw bytes.
//   - Every call takes a context.Context; cancelling it stops the call and its waits.
package client
