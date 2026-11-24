// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairntest_test

import (
	"encoding/json"
	"fmt"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/cairntest"
)

type Order struct {
	Amount int `json:"amount"`
}

type Receipt struct {
	Receipt string `json:"receipt"`
	Total   int    `json:"total"`
}

func submit(ctx cairn.ObjectContext, o Order) (Receipt, error) {
	total, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Sleep(ctx, time.Hour); err != nil { // fires by itself in the test
		return Receipt{}, err
	}
	r, err := cairn.Run(ctx, "charge", func(cairn.RunContext) (string, error) {
		return fmt.Sprintf("receipt-%d", o.Amount), nil
	})
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Set(ctx, "total", total+o.Amount); err != nil {
		return Receipt{}, err
	}
	return Receipt{Receipt: r, Total: total + o.Amount}, nil
}

var orders = cairn.NewObject("OrderService", cairn.Exclusive("submit", submit))

// A handler runs to the end; state, timers and Run results are answered by the fake
// runtime, and every turn replays the journal from the start.
func Example() {
	inv := cairntest.New(orders, "submit", Order{Amount: 100},
		cairntest.Key("customer-1"), cairntest.State("total", 50))
	status, err := inv.Run()
	if err != nil {
		panic(err)
	}
	var r Receipt
	if err := inv.Output(&r); err != nil {
		panic(err)
	}
	var total int
	_, _ = inv.Get("total", &total)
	fmt.Println(status, r.Receipt, r.Total, total)
	fmt.Println(inv.Journal())
	// Output:
	// completed receipt-100 150 150
	// [Input GetEagerState Sleep SleepCompletion Run RunCompletion SetState Output]
}

func checkout(ctx cairn.Context, o Order) (string, error) {
	return cairn.Service[string](ctx, "Payments", "pay").Request(o.Amount)
}

var shop = cairn.NewService("Shop", cairn.Handler("checkout", checkout))

// Stub answers a call at once; a failing stub is a failed callee.
func ExampleStub() {
	pay := cairntest.Stub("Payments", "pay", func(in json.RawMessage) (any, error) {
		var amount int
		if err := json.Unmarshal(in, &amount); err != nil {
			return nil, err
		}
		if amount > 1000 {
			return nil, cairn.TerminalErrorf(402, "over quota")
		}
		return fmt.Sprintf("paid-%d", amount), nil
	})

	inv := cairntest.New(shop, "checkout", Order{Amount: 250}, pay)
	status, _ := inv.Run()
	var paid string
	_ = inv.Output(&paid)
	fmt.Println(status, paid)

	inv = cairntest.New(shop, "checkout", Order{Amount: 5000}, pay)
	status, _ = inv.Run()
	err := inv.Output(nil)
	te, _ := cairn.AsTerminalError(err)
	fmt.Println(status, te.Code(), te.Error())
	// Output:
	// completed paid-250
	// failed 402 over quota
}

type Approval struct {
	OK bool `json:"ok"`
}

func approve(ctx cairn.Context, doc string) (string, error) {
	awk := cairn.Awakeable[Approval](ctx)
	if _, err := cairn.Run(ctx, "ask", func(cairn.RunContext) (string, error) {
		return awk.ID(), nil // send the id to a reviewer here
	}); err != nil {
		return "", err
	}
	a, err := awk.Result()
	if err != nil {
		return "", err
	}
	if !a.OK {
		return doc + " rejected", nil
	}
	return doc + " approved", nil
}

var reviews = cairn.NewService("Reviews", cairn.Handler("approve", approve))

// A wait only the outside world can end is Pending: the test resolves it, then runs on.
func ExampleInvocation_Resolve() {
	inv := cairntest.New(reviews, "approve", "contract")
	status, _ := inv.Run()
	p := inv.Pending()[0]
	fmt.Println(status, p.Kind)

	_ = inv.Resolve(p, Approval{OK: true})
	status, _ = inv.Run()
	var out string
	_ = inv.Output(&out)
	fmt.Println(status, out)
	// Output:
	// waiting awakeable
	// completed contract approved
}

func quote(ctx cairn.Context, sku string) (int, error) {
	price := cairn.Service[int](ctx, "Pricing", "quote").RequestFuture(sku)
	timeout := cairn.After(ctx, 5*time.Second)
	first, err := cairn.WaitFirst(ctx, price, timeout)
	if err != nil {
		return 0, err
	}
	if first == cairn.Awaitable(timeout) {
		return 0, cairn.TerminalErrorf(504, "no quote in time")
	}
	return price.Result()
}

var quotes = cairn.NewService("Quotes", cairn.Handler("quote", quote))

// A race between a call and a timer: the timer does not fire by itself while the handler
// also waits for something else; Advance moves the clock.
func ExampleInvocation_Advance() {
	inv := cairntest.New(quotes, "quote", "sku-1")
	status, _ := inv.Run()
	for _, p := range inv.Pending() {
		fmt.Println(status, p.Kind, p.Name)
	}
	inv.Advance(5 * time.Second)
	status, _ = inv.Run()
	err := inv.Output(nil)
	fmt.Println(status, err)
	// Output:
	// waiting call Pricing/quote
	// waiting sleep sleep
	// failed no quote in time
}
