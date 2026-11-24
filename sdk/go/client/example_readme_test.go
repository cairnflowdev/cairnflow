// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

//lint:file-ignore SA4006 the snippets copy README.md and docs/quickstart.md, which show calls one after
// another without the error check between them; the compiler keeps them honest, not this lint.

package client_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

type Order struct {
	Amount int `json:"amount"`
}

type Receipt struct {
	Receipt string `json:"receipt"`
	Total   int    `json:"total"`
}

type Account struct {
	ID string `json:"id"`
}

// The snippet of the "Client" section of README.md, so it keeps compiling.
func readmeClient(ctx context.Context, awkID string, user any) error {
	c := client.Connect("https://cairn.example.com", client.WithAPIKey(os.Getenv("CAIRN_KEY")))

	r, err := client.Call[Receipt](ctx, c.Object("OrderService", "order-42"), "submit", Order{Amount: 5}) // waits for the result
	h, err := client.Send(ctx, c.Service("Mailer"), "welcome", map[string]string{"to": "a@example.com"})  // fire and forget
	out, err := client.Attach[string](ctx, h, client.Timeout(time.Minute))                                // or client.Output, h.Status
	acct, err := client.RunWorkflow[Account](ctx, c.Workflow("Signup", "u1"), user)
	_, err = c.Awakeable(awkID).Resolve(ctx, map[string]bool{"ok": true})
	_, err = c.Promise("Signup", "u1", "code").Resolve(ctx, "123456")
	for ev, err := range c.ReadStream(ctx, h.ID(), "tokens") { // reconnects on its own
		if err != nil {
			break
		}
		fmt.Println(ev.Kind, string(ev.Data))
	}

	// Not in the snippet: what its prose names.
	_, _, _ = r, out, acct
	var failed *client.InvocationFailed
	if errors.As(err, &failed) && cairn.IsTerminalError(err) {
		return err
	}
	_, _ = client.Send(ctx, c.Service("Mailer"), "welcome", nil, client.IdempotencyKey("k1"))
	_, _ = client.Send(ctx, c.Service("Mailer"), "welcome", nil, client.NoIdempotencyKey())
	_, _, _ = client.Output[string](ctx, h)
	_, _ = h.Status(ctx)
	_, _ = h.Cancel(ctx)
	_, _ = h.Signal(ctx, "approve", "ok")
	_ = c.Schedules()
	var timeout *client.AttachTimeout
	_ = errors.As(err, &timeout)
	return err
}

// The Go client snippet of docs/quickstart.md, section 4.
func quickstartClient(ctx context.Context) error {
	c := client.Connect("http://127.0.0.1:8080", client.WithAPIKey(os.Getenv("CAIRNFLOW_INVOKE_KEY")))
	order := c.Object("OrderService", "order-42")
	r, err := client.Call[Receipt](ctx, order, "submit", Order{Amount: 100})
	h, err := client.Send(ctx, order, "submit", Order{Amount: 5})
	_, err = client.Attach[Receipt](ctx, h)
	_ = r
	return err
}

var _ = []any{readmeClient, quickstartClient}
