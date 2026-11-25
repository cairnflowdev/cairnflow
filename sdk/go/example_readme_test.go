// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn_test

// The code snippets of README.md, so they keep compiling. The complete example of its
// "Usage" section is example/orderservice; quoteOrTimeout is in example_test.go.

import (
	"context"
	"errors"
	"log"
	"maps"
	"net/http"
	"slices"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
)

// README "Services".
func ExampleNewWorkflow() {
	greeter := cairn.NewService("Greeter",
		cairn.Handler("greet", func(ctx cairn.Context, name string) (string, error) {
			return "hello " + name, nil
		}, cairn.WithInactivityTimeout(time.Hour)))

	signup := cairn.NewWorkflow("Signup",
		cairn.Main("run", func(ctx cairn.WorkflowContext, email string) (string, error) {
			// Waits, durably, until the shared handler below completes the promise.
			return cairn.Promise[string](ctx, "code").Get()
		}),
		cairn.Shared("verify", func(ctx cairn.WorkflowSharedContext, code string) (cairn.Void, error) {
			return cairn.Void{}, cairn.Promise[string](ctx, "code").Resolve(code)
		}),
	).With(cairn.WithRetryPolicy(cairn.RetryPolicy{
		InitialInterval:      time.Second,
		MaxInterval:          time.Minute,
		ExponentiationFactor: 2,
		MaxAttempts:          10,
		OnMaxAttempts:        cairn.PauseOnMaxAttempts,
	}))
	_, _ = greeter, signup
}

// README "Futures and combinators": fan out, then wait for every result.
func bestPrice(ctx cairn.Context, skus []string) (int, error) {
	futs := make([]cairn.Awaitable, len(skus))
	quotes := make([]*cairn.Future[Quote], len(skus))
	for i, sku := range skus {
		quotes[i] = cairn.Service[Quote](ctx, "Pricing", "quote").RequestFuture(sku)
		futs[i] = quotes[i]
	}
	if err := cairn.All(ctx, futs...); err != nil {
		return 0, err
	}
	best := -1
	for _, f := range quotes {
		q, _ := f.Result()
		if best < 0 || q.Price < best {
			best = q.Price
		}
	}
	return best, nil
}

// README "Rules for handler code", rule 4.
func savePrices(ctx cairn.ObjectContext, prices map[string]int) (cairn.Void, error) {
	for _, k := range slices.Sorted(maps.Keys(prices)) {
		if err := cairn.Set(ctx, k, prices[k]); err != nil {
			return cairn.Void{}, err
		}
	}
	return cairn.Void{}, nil
}

// README "The context": one call of each row, with the types the table names.
func contextTable(ctx cairn.ObjectContext) (cairn.Void, error) {
	v, found, err := cairn.Get[int](ctx, "k")
	_, _, _ = v, found, err
	_ = cairn.Set(ctx, "k", 1)
	_ = cairn.Clear(ctx, "k")
	_ = cairn.ClearAll(ctx)
	_, _ = cairn.StateKeys(ctx)

	_ = cairn.Sleep(ctx, time.Minute)
	var timer *cairn.Future[cairn.Void] = cairn.After(ctx, time.Minute)

	_, _ = cairn.Run(ctx, "step", func(rc cairn.RunContext) (string, error) {
		ref, err := rc.Artifacts().Put([]byte("%PDF"), "application/pdf")
		rc.Log().Info("put", "sha256", ref.SHA256)
		return ref.SHA256, err
	}, cairn.Cancellable())
	run := cairn.RunAsync(ctx, "async", func(rc cairn.RunContext) (int, error) { return rc.Attempt(), rc.Err() })

	_, _ = cairn.Service[Quote](ctx, "Pricing", "quote").Request("sku")
	_ = cairn.Object[int](ctx, "OrderService", "order-42", "total").RequestFuture(nil)
	_ = cairn.Workflow[string](ctx, "Signup", "u1", "verify").Send("123456", cairn.WithDelay(time.Hour))
	_ = cairn.Service[cairn.Void](ctx, "Mailer", "welcome",
		cairn.WithIdempotencyKey("k1"), cairn.WithScope("tenant-1"), cairn.WithLimitKey("l1"),
		cairn.WithSpread(), cairn.WithTags(map[string]string{"team": "a"}), cairn.WithName("mail")).Send(nil)
	if scoped, err := ctx.Scoped("tenant-1", "l1"); err == nil {
		_ = cairn.Service[cairn.Void](scoped, "Mailer", "welcome").Send(nil)
	}

	approve := cairn.Signal[string](ctx, "approve")
	_ = cairn.SendSignal(ctx, "inv_1", "approve", "ok")
	awk := cairn.Awakeable[bool](ctx)
	_ = awk.ID()
	_, _ = awk.Result()

	p := cairn.Promise[string](ctx, "code")
	_, _ = p.Get()
	_ = p.GetFuture()
	_, _, _ = p.Peek()
	_ = p.Resolve("x")
	_ = p.Reject(400, "no")

	_ = cairn.Attach[string](ctx, "inv_1")
	_, _, _ = cairn.GetOutput[string](ctx, "inv_1")

	first, _ := cairn.WaitFirst(ctx, timer, approve)
	for fut, err := range cairn.Wait(ctx, timer, approve) {
		_, _ = fut, err
	}
	it := cairn.WaitIter(ctx, timer, run)
	for it.Next() {
		_ = it.Value()
	}
	_ = it.Err()
	_ = cairn.All(ctx, timer, run)
	_, _ = cairn.Race(ctx, approve, cairn.Signal[string](ctx, "reject"))
	_, _ = first, approve.Done()

	if s, err := ctx.Stream("tokens", cairn.Durable()); err == nil {
		s.Append(map[string]string{"token": "hi"})
		s.Close()
	}
	ctx.Log().Info("order placed", "amount", 5)
	_, _ = cairn.WithSpan(ctx, "fetch", func(sp *cairn.Span) (int, error) {
		sp.Set("rows", 3)
		return 3, nil
	}, "table", "orders")
	_, _ = ctx.Artifacts().Get("sha256-hex")

	_, _, _ = ctx.Rand().IntN(6), ctx.Random(), cairn.UUID(ctx)
	_, _, _ = ctx.Key(), ctx.InvocationID(), ctx.Attempt()
	_, _ = ctx.IdempotencyKey()
	_, _ = ctx.Schedule()

	return cairn.Void{}, cairn.ContinueAsNew(ctx, 1)
}

// README "Errors".
func readmeErrors(err error) []error {
	errs := []error{
		cairn.TerminalErrorf(400, "amount must be positive"),
		cairn.ToTerminalError(err, cairn.WithCode(404)),
		cairn.PauseErrorf("short of %d units", 3),
		cairn.ToPauseError(err),
		cairn.RetryableErrorf(503, "upstream busy"),
		cairn.ToRetryableError(err, cairn.WithRetryAfter(30*time.Second)),
	}
	var se *cairn.SegmentedValueError
	_ = errors.As(err, &se)
	if t, ok := cairn.AsTerminalError(err); ok && t.Code() == cairn.CancelledCode && cairn.IsTerminalError(err) {
		return nil
	}
	return errs
}

// README "Serving".
func readmeServing(ctx context.Context, kid string) {
	orders := cairn.NewObject("OrderService", cairn.Exclusive("submit", submit))
	signup := cairn.NewWorkflow("Signup", cairn.Main("run", func(cairn.WorkflowContext, string) (string, error) { return "", nil }))

	err := cairn.Serve(ctx, ":9080", orders, signup,
		cairn.Bidi(),
		cairn.IdentityKeys(kid),
		cairn.SegmentOrigins("https://values.example.com"))
	_ = err

	mux := http.NewServeMux()
	mux.Handle("/cairn/", http.StripPrefix("/cairn", cairn.HTTPHandler(orders, cairn.IdentityKeys(kid))))
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true) // only needed with cairn.Bidi()
	srv := &http.Server{Addr: ":9080", Handler: mux, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
	_ = srv

	// The endpoint options.
	h, err := cairn.LambdaHandler(orders,
		cairn.IdentityKeys(kid), cairn.IdentityHosts("orders.example.com"),
		cairn.MaxProtocolVersion(6),
		cairn.ReplayCache(cairn.ReplayCacheOptions{MaxBytes: 64 << 20, TTL: 15 * time.Minute}),
		cairn.WithoutReplayCache(),
		cairn.SegmentOrigins("https://values.example.com"), cairn.ValueCacheBytes(64<<20),
		cairn.MaxValueBytes(64<<20), cairn.MaxTurnValueBytes(256<<20),
		cairn.WithSegmentTransport(http.DefaultTransport),
		cairn.WithoutCompression(),
		cairn.WithArtifactSource("https://cairn.example.com", http.Header{"Authorization": {"Bearer cf_..."}}))
	if err != nil {
		log.Fatal(err)
	}
	_ = h
}

// The Go snippet of docs/quickstart.md, section 3 (submit renamed: example_test.go has one).
func chargeCard(ctx context.Context, amount int) (string, error) { return "r-1", ctx.Err() }

func quickstartSubmit(ctx cairn.ObjectContext, order Order) (Receipt, error) {
	soFar, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Sleep(ctx, time.Second); err != nil { // durable timer
		return Receipt{}, err
	}
	receipt, err := cairn.Run(ctx, "charge", func(rc cairn.RunContext) (string, error) {
		return chargeCard(rc, order.Amount) // runs exactly once; rc is a context.Context
	})
	if err != nil {
		return Receipt{}, err
	}
	if err := cairn.Set(ctx, "total", soFar+order.Amount); err != nil {
		return Receipt{}, err
	}
	return Receipt{Receipt: receipt, Total: soFar + order.Amount}, nil
}

func quickstartMain() {
	var orders = cairn.NewObject("OrderService", cairn.Exclusive("submit", quickstartSubmit))
	if err := cairn.Serve(context.Background(), "127.0.0.1:9080", orders); err != nil {
		log.Fatal(err)
	}
}

var _ = []any{bestPrice, savePrices, contextTable, readmeErrors, readmeServing, quickstartMain}
