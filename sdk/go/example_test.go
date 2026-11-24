// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn_test

import (
	"context"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
)

type Order struct {
	Amount int `json:"amount"`
}

type Receipt struct {
	Receipt string `json:"receipt"`
	Total   int    `json:"total"`
}

type Quote struct {
	Price int `json:"price"`
}

func charge(ctx context.Context, amount int) (string, error) { return "r-1", ctx.Err() }

func submit(ctx cairn.ObjectContext, o Order) (Receipt, error) {
	if o.Amount <= 0 {
		return Receipt{}, cairn.TerminalErrorf(400, "amount must be positive")
	}
	total, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Sleep(ctx, time.Second); err != nil {
		return Receipt{}, err
	}
	r, err := cairn.Run(ctx, "charge", func(rc cairn.RunContext) (string, error) {
		return charge(rc, o.Amount)
	})
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Set(ctx, "total", total+o.Amount); err != nil {
		return Receipt{}, err
	}
	return Receipt{Receipt: r, Total: total + o.Amount}, nil
}

func total(ctx cairn.ObjectSharedContext) (int, error) {
	t, _, err := cairn.Get[int](ctx, "total")
	return t, err
}

// A quote with a deadline: futures and a race decided by the journal.
func quoteOrTimeout(ctx cairn.Context, sku string) (int, error) {
	q := cairn.Service[Quote](ctx, "Pricing", "quote").RequestFuture(sku)
	timeout := cairn.After(ctx, 5*time.Second)
	first, err := cairn.WaitFirst(ctx, q, timeout)
	if err != nil {
		return 0, err
	}
	if first == cairn.Awaitable(timeout) {
		return 0, cairn.TerminalErrorf(504, "no quote in time")
	}
	v, err := q.Result()
	return v.Price, err
}

func Example() {
	orders := cairn.NewObject("OrderService",
		cairn.Exclusive("submit", submit),
		cairn.Shared("total", cairn.NoInput(total)))
	pricing := cairn.NewService("Checkout",
		cairn.Handler("quote", quoteOrTimeout, cairn.WithInactivityTimeout(time.Minute)))
	_, _ = orders, pricing
}

// On AWS Lambda (a Function URL or an API Gateway HTTP API), pass the handler to
// lambda.Start from github.com/aws/aws-lambda-go in your main package; the SDK itself does
// not depend on it.
func ExampleLambdaHandler() {
	orders := cairn.NewObject("OrderService", cairn.Exclusive("submit", submit))
	kid := "publickeyv1_..." // the kid listed at the runtime's GET /identity
	h, err := cairn.LambdaHandler(orders, cairn.IdentityKeys(kid), cairn.SegmentOrigins("https://values.example.com"))
	if err != nil {
		return // log.Fatal(err)
	}
	_ = h // lambda.Start(h)
}
