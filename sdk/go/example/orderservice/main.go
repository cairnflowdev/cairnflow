// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Command orderservice is an order service: durable state, a durable timer, a side effect
// that runs once.
//
//	go run ./example/orderservice           # from sdk/go; listens on 127.0.0.1:9080
//	cairn register http://127.0.0.1:9080
//	cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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

// charge is the side effect: a payment provider in real life.
func charge(ctx context.Context, amount int) (string, error) {
	log.Printf("charging %d", amount)
	return fmt.Sprintf("receipt-%d", amount), ctx.Err()
}

func submit(ctx cairn.ObjectContext, order Order) (Receipt, error) {
	if order.Amount <= 0 {
		return Receipt{}, cairn.TerminalErrorf(400, "amount must be positive") // bad data: retrying is useless
	}
	total, _, err := cairn.Get[int](ctx, "total") // false (and 0) = the key does not exist
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Sleep(ctx, time.Second); err != nil { // durable: the timer lives in the store
		return Receipt{}, err
	}
	receipt, err := cairn.Run(ctx, "charge", func(rc cairn.RunContext) (string, error) {
		return charge(rc, order.Amount) // runs once per durable result; rc is a context.Context
	})
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Set(ctx, "total", total+order.Amount); err != nil {
		return Receipt{}, err
	}
	return Receipt{Receipt: receipt, Total: total + order.Amount}, nil
}

// total runs in parallel with other handlers of the same key and only reads state.
func total(ctx cairn.ObjectSharedContext) (int, error) {
	t, _, err := cairn.Get[int](ctx, "total")
	return t, err
}

var orders = cairn.NewObject("OrderService",
	cairn.Exclusive("submit", submit),
	cairn.Shared("total", cairn.NoInput(total)))

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// REQUEST_RESPONSE over HTTP/1.1; add cairn.Bidi() for BIDI_STREAM over h2c, same port.
	if err := cairn.Serve(ctx, "127.0.0.1:9080", orders); err != nil {
		log.Fatal(err)
	}
}
