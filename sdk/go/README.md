# cairn (Go SDK)

The Go SDK for Cairnflow (module `github.com/cairnflowdev/cairnflow/sdk/go`, package `cairn`). It
implements [`docs/protocol-v1.md`](../../docs/protocol-v1.md) on the side of whoever writes the
code, speaks wire versions 1 to 6, and passes every trace of the language-neutral conformance suite
in [`conformance/`](../../conformance) byte for byte, in `REQUEST_RESPONSE` and in `BIDI_STREAM`.

**No dependencies.** `go.mod` has no `require`: the codec, the state machine, the HTTP/1.1 and h2c
server, the AWS Lambda adapter, the client and Ed25519 verification are all standard library.

Go ≥ 1.25.

## Install

The module lives in this repository at `sdk/go`. No release has been tagged yet; until one is, point
your module at a checkout:

```sh
go mod edit -require=github.com/cairnflowdev/cairnflow/sdk/go@v0.0.0 \
            -replace=github.com/cairnflowdev/cairnflow/sdk/go=../cairnflow/sdk/go
```

Once a release is tagged (a Go module in a subdirectory is tagged `sdk/go/vX.Y.Z`), drop the
`replace` and `go get github.com/cairnflowdev/cairnflow/sdk/go@vX.Y.Z`.

```go
import (
	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client" // calling services from an app or a script
)
```

## Usage

```go
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
```

```sh
go run ./example/orderservice           # from sdk/go
cairn register http://127.0.0.1:9080
cairn invoke OrderService submit --key order-42 --input '{"amount": 100}'
```

This is [`example/orderservice/main.go`](example/orderservice/main.go). It listens on the loopback
address only and sets no `IdentityKeys`, which is fine on your own machine; anywhere else, see
[Serving](#serving).

Every durable step is a function of the package that takes the context (`cairn.Get(ctx, …)`,
`cairn.Run(ctx, …)`), so it can be generic in the type of its value. A step that can fail returns
an `error`: return it from the handler as it is. When the step has to wait (a timer, a call, a
result that is not durable yet), the call does not return in this turn at all; see
[How it runs](#how-it-runs).

### Services

- `cairn.NewService(name, handlers...)`: stateless, no key. Handlers are
  `cairn.Handler(name, fn)` with `fn func(cairn.Context, I) (O, error)`; they run in parallel.
- `cairn.NewObject(name, handlers...)`: a virtual object, keyed, with durable state.
  `cairn.Exclusive(name, fn)` takes a `cairn.ObjectContext`: it reads and writes state, and the
  exclusive handlers of one key run one at a time. `cairn.Shared(name, fn)` takes a
  `cairn.ObjectSharedContext`: it runs in parallel with them and reads state; writing state does
  not compile there.
- `cairn.NewWorkflow(name, handlers...)`: exactly one `cairn.Main(name, fn)` taking a
  `cairn.WorkflowContext` (it runs once per workflow id, the key, and reads and writes state) and
  any number of `cairn.Shared(name, fn)` taking a `cairn.WorkflowSharedContext` (they read state,
  signal the run and complete its durable promises). Name the main handler `run`: the clients of
  every SDK start a workflow by calling `run` (`client.RunWorkflow`), and the other SDKs give it
  that name by default.

`cairn.Handler` picks the kind from the type of `fn`'s context, so it can declare any of the above.
A handler without input is wrapped with `cairn.NoInput(fn)`; one without output returns
`cairn.Void`. A mistake in a declaration (no handler, two handlers with one name, a workflow
without one `Main`, a context type the service kind does not run) panics at declaration time.

```go
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
```

Input and output are JSON through `encoding/json`, with the same bytes the other SDKs write for
the same value; `[]byte` and `json.RawMessage` pass through as the value's bytes. An empty input
gives the zero value of `I`; an input that does not decode into `I` fails the invocation with a
terminal error 400 before the handler runs.

The contract in the manifest is declared with `HandlerOption`s, on a handler (trailing arguments)
or on the whole service (`.With(...)`, a handler's own options win): `WithRetryPolicy`,
`WithInactivityTimeout` (a service that sets nothing declares 15 minutes), `WithJournalRetention`,
`WithLogLevel` (the minimum level of the durable log; SDK side only), `WithRuntimeTrace` (the
runtime writes one span per turn and per `Run` into `$trace`) and `WithLogFlush` (`BIDI_STREAM`
only). Durations are sent in whole milliseconds; a zero field is not declared. Changing the
contract means registering a new deployment.

### The context

| | |
|---|---|
| State | `cairn.Get[T](ctx, k)` returns `(value, found, err)`; `cairn.Set(ctx, k, v)`, `cairn.Clear(ctx, k)`, `cairn.ClearAll(ctx)`, `cairn.StateKeys(ctx)` |
| Time | `cairn.Sleep(ctx, d)`, `cairn.After(ctx, d)` (a future); the wake-up time is absolute and journaled once |
| Side effects | `cairn.Run(ctx, name, func(rc cairn.RunContext) (T, error))`, `cairn.RunAsync(...)` (a future); option `cairn.Cancellable()` |
| Calls | `cairn.Service[O](ctx, svc, handler)`, `cairn.Object[O](ctx, svc, key, handler)`, `cairn.Workflow[O](ctx, svc, id, handler)`, then `.Request(in)`, `.RequestFuture(in)` or `.Send(in, cairn.WithDelay(d))`; options `WithIdempotencyKey`, `WithScope`, `WithLimitKey`, `WithSpread`, `WithTags`, `WithName`; `ctx.Scoped(scope, limitKey)` for a view whose calls all carry a scope |
| Signals | `cairn.Signal[T](ctx, name)` (a future), `cairn.SendSignal(ctx, invocationID, name, v)`; `awk := cairn.Awakeable[T](ctx)`, `awk.ID()` for the outside system, `awk.Result()` |
| Promises | `p := cairn.Promise[T](ctx, name)` (objects and workflows): `p.Get()`, `p.GetFuture()`, `p.Peek()`, `p.Resolve(v)`, `p.Reject(code, msg)` |
| Others | `cairn.Attach[T](ctx, invocationID)` (a future), `cairn.GetOutput[T](ctx, invocationID)`, `cairn.ContinueAsNew(ctx, input)` |
| Combinators | `cairn.WaitFirst(ctx, futs...)`, `cairn.Wait(ctx, futs...)` (range over it), `cairn.WaitIter`, `cairn.All(ctx, futs...)`, `cairn.Race(ctx, futs...)`; on a future, `f.Result()` and `f.Done()` |
| Streams | `s, err := ctx.Stream(name)` (`cairn.Durable()` to keep it), `s.Append(record)`, `s.Close()`; `ctx.Log()` is a `*slog.Logger` writing the durable log `$log`; `cairn.WithSpan(ctx, name, fn, attrs...)` writes `$trace` |
| Artifacts | `ref, err := rc.Artifacts().Put(bytes, contentType)` inside a `Run` closure (return the ref, not the bytes), `ctx.Artifacts().Get(ref.SHA256)` |
| Determinism | `ctx.Rand()` (a `*rand.Rand` of `math/rand/v2`), `ctx.Random()`, `cairn.UUID(ctx)`, `ctx.Key()`, `ctx.InvocationID()`, `ctx.Attempt()`, `ctx.IdempotencyKey()`, `ctx.Schedule()` |

A plain service's `cairn.Context` has no state and no promises: they belong to the object or
workflow with that key, so `cairn.Get` on a service context does not compile. `Stream`, `Log` and
`Artifacts` need wire v4; below it, stream and log records are dropped. A `Run` closure gets a
`cairn.RunContext`, which is a `context.Context` (cancelled when a `Cancellable` run is cancelled)
plus `Log`, `Stream`, `Artifacts` and `Attempt`; the closure must not touch the handler's `ctx`.
`ctx.Artifacts().Get` reads artifacts this process put; to download others from the runtime, serve
with `cairn.WithArtifactSource(baseURL, header)`.

### Futures and combinators

A command is journaled when its future is created; waiting only happens in `Result` or in a
combinator. That is how several steps run at once on one goroutine:

```go
// A quote with a deadline: a race decided by the journal.
func quoteOrTimeout(ctx cairn.Context, sku string) (int, error) {
	q := cairn.Service[Quote](ctx, "Pricing", "quote").RequestFuture(sku)
	timeout := cairn.After(ctx, 5*time.Second)
	first, err := cairn.WaitFirst(ctx, q, timeout)
	if err != nil {
		return 0, err // cancelled
	}
	if first == cairn.Awaitable(timeout) {
		return 0, cairn.TerminalErrorf(504, "no quote in time")
	}
	v, err := q.Result()
	return v.Price, err
}

// Fan out, then wait for every result.
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
```

**Results are taken in journal order.** When several futures are ready, `WaitFirst` and `Race`
pick the one whose result stands earliest in the journal, never the one that arrived first in
time; `Wait` yields futures in journal order; `All` returns the first error in journal order. A
replay therefore picks the same winner and takes the same path. The error of `WaitFirst`, `Wait`
and `WaitIter` is the cancellation point's (409), never a future's own error: read each value with
its `Result`. When the turn has to wait, the SDK suspends durably on one tree (`FIRST_COMPLETED`
for `WaitFirst`, `ALL_COMPLETED` for `All`), and the runtime wakes the invocation when the tree is
satisfied.

### Rules for handler code

A handler is rerun from the start on every turn, so it must take the same path every time. Four
rules (see protocol-v1 section 8.4) that the SDK cannot fully enforce:

1. **Stay on the handler's goroutine.** No `go` statements, channels, `sync` or `time.Sleep` in
   handler code to coordinate durable work. Parallel work is futures (`RunAsync`, `RequestFuture`,
   `After`, `Signal`, `Awakeable`, `Attach`, `GetFuture`) plus `WaitFirst`, `Wait`, `All` or
   `Race`. Concurrency inside a `Run` closure is fine: its `RunContext` may be used from
   goroutines the closure starts, until the closure returns.
2. **No randomness or clock of your own.** No `time.Now`, `math/rand` or `crypto/rand` in handler
   code: use `ctx.Rand()`, `ctx.Random()` or `cairn.UUID(ctx)`, and read the clock inside `Run`.
3. **I/O only inside `Run`**: network, files, databases, anything that can change between two
   runs. That is why `cairn.Context` is not a `context.Context`, and why a `Run` closure gets one.
4. **The same commands in the same order.** Ranging over a map visits keys in a random order, so
   commands issued from that loop come out in another order on replay. Sort the keys first:

   ```go
   for _, k := range slices.Sorted(maps.Keys(prices)) {
   	if err := cairn.Set(ctx, k, prices[k]); err != nil {
   		return cairn.Void{}, err
   	}
   }
   ```

What happens when a rule is broken:

- A `ctx` call from another goroutine stops that goroutine (`runtime.Goexit`; its deferred
  functions run) and fails the turn with an error naming the misuse; the invocation is retried
  with backoff until the code is fixed. A `ctx` call inside a `Run` closure panics, which fails the
  attempt the same way.
- Code that issues another command, or the same commands in another order, than the journal holds
  stops with `JOURNAL_MISMATCH` (570): the invocation moves to `paused`. Deploy the fixed code,
  then `cairn resume <id>`.
- A clock or a random number of your own is not detected: a replay may take another branch and
  stop with 570, or carry on with a value the first run never journaled.

### Errors

| Returned from a handler | What happens |
|---|---|
| `cairn.TerminalErrorf(code, format, args...)`, `cairn.ToTerminalError(err, cairn.WithCode(404))` | The invocation fails for good, without retry; the caller gets the error with its code and message |
| `cairn.PauseErrorf(format, args...)`, `cairn.ToPauseError(err)` | The invocation pauses, keeping its journal, waiting for a human: `POST /invocations/{id}/resume` (`cairn resume <id>`) continues from the same point |
| `cairn.RetryableErrorf(code, format, args...)`, `cairn.ToRetryableError(err, cairn.WithRetryAfter(30*time.Second))` | Retried like any other error, with a code, and optionally a delay that overrides the retry policy for the next attempt |
| Any other `error`, or a panic | Retried with backoff per the retry policy (code 500) |

The checks are `errors.As` (wrapped errors count) or the helpers `cairn.IsTerminalError(err)` and
`cairn.AsTerminalError(err)`. Operations whose only failure is a terminal one return a
`*cairn.TerminalError`: a callee that failed, a rejected promise or awakeable, a cancellation point
(code `cairn.CancelledCode`, 409).

Inside a `Run` closure, a returned `*TerminalError` is the step's durable result: the closure does
not run again, and `Run` returns the same error on every replay. A returned `*PauseError` pauses
the invocation without recording anything: after `resume` the closure runs again and sees the
fixed world (as in the TypeScript and Python SDKs). Any other error, or a panic, records nothing
either: the turn fails and the retry reruns the closure.

A large value the SDK cannot read (wire v6: a segment it could not download or check, a value over
a limit) is a `*cairn.SegmentedValueError`; returned from a handler, the attempt is retried.

### How it runs

The same model as the TypeScript and Python SDKs: each turn reruns the handler from the start on
the journal. Commands already journaled are matched (kind, name and every field but the wake-up
time of a timer or a delayed send and the value of a `Set`, or 570) instead of being sent again,
and results are delivered to user code **in journal order**, one at a time, so a replay takes
exactly the same path. A `Run` closure starts on its own goroutine as soon as `RunAsync` is called
(unless its result is already in the journal: then it does not run at all); its result is
proposed to the runtime and returned once durable. Results are proposed in the order of the code,
not in the order closures finish, so the journal does not depend on goroutine timing.

When the turn must end before the handler returns (it has to wait, or the code does not match the
journal), the SDK unwinds the handler's goroutine with `runtime.Goexit`: the call that had to wait
never returns, deferred functions run, `recover()` cannot catch it, and a `ctx` call from a
deferred function exits at once. The next turn reruns the handler from the start. Keep deferred
functions free of durable work, and do not read a deferred function running as "the handler
finished". A turn ends once the handler's goroutine and every closure it started have returned.

## Serving

- `cairn.Serve(ctx, addr, services...)`: the built-in server, until `ctx` ends; then it shuts down
  gracefully (running turns get a grace period). It serves HTTP/1.1 and h2c (HTTP/2 without TLS,
  prior knowledge) on one port. It reads a `REQUEST_RESPONSE` request body with a deadline, and
  sets no write timeout and no limit on the life of a `BIDI_STREAM` stream.
- `cairn.HTTPHandler(services...)`: an `http.Handler` for a server of your own. It panics when
  the options are inconsistent (`Serve` and `LambdaHandler` return the error instead).
- `cairn.Bidi()`: declare `BIDI_STREAM`: each invocation is one h2c stream that stays open across
  turns, so a suspension the runtime can answer at once costs no new request and no journal
  resent. The default is `REQUEST_RESPONSE`, one HTTP/1.1 request per turn: keep it when a proxy
  between the runtime and the endpoint only speaks HTTP/1.1. An HTTP/1.1 request to an endpoint
  declared `Bidi` is still served, as one `REQUEST_RESPONSE` turn.
- `cairn.LambdaHandler(services...)`: AWS Lambda behind a Function URL or an API Gateway HTTP API
  (payload format 2.0), `REQUEST_RESPONSE` only. The SDK does not depend on the AWS libraries:
  pass the handler to `lambda.Start` in your own `main`.

**Set `cairn.IdentityKeys` in production.** Pass it the `kid` listed at the runtime's
`GET /identity`: every request must then carry the runtime's signature. Without it, anyone who can
reach the endpoint can run your handlers on a forged journal, input and state, so the endpoint
turns off what a forged request could abuse: it keeps no replay cache (wire v5) and, unless
`cairn.SegmentOrigins` is set, serves no segmented values (wire v6) and declares wire version 5 at
most. `IdentityKeys` is the setting that unlocks both.

Services and options are both `ServeOption`s, passed in any order:

```go
err := cairn.Serve(ctx, ":9080", orders, signup,
	cairn.Bidi(),
	cairn.IdentityKeys(kid),
	cairn.SegmentOrigins("https://values.example.com"))
```

Mounted in a server of your own (register the deployment as `http://host:9080/cairn`; the request
identity JWT signs the path the runtime sent, prefix included):

```go
mux := http.NewServeMux()
mux.Handle("/cairn/", http.StripPrefix("/cairn", cairn.HTTPHandler(orders, cairn.IdentityKeys(kid))))
var p http.Protocols
p.SetHTTP1(true)
p.SetUnencryptedHTTP2(true) // only needed with cairn.Bidi()
srv := &http.Server{Addr: ":9080", Handler: mux, Protocols: &p, ReadHeaderTimeout: 10 * time.Second}
```

Do not set a `WriteTimeout` shorter than your longest turn (a turn waits for its `Run` closures),
nor any `ReadTimeout` or `WriteTimeout` with `Bidi` (they bound the whole stream).

On AWS Lambda:

```go
import "github.com/aws/aws-lambda-go/lambda"

func main() {
	h, err := cairn.LambdaHandler(orders, cairn.IdentityKeys(kid))
	if err != nil {
		log.Fatal(err)
	}
	lambda.Start(h)
}
```

Register the function's base URL: the runtime calls `<url>/discover` and
`<url>/invoke/{service}/{handler}`, and the handler routes on the end of the raw path the event
carries (`rawPath`), so a mount prefix (an API Gateway stage) works.

Endpoint options:

- `cairn.IdentityKeys(kids...)`: verify the runtime's signature on every request (the `kid` values
  listed at `GET /identity`); an unsigned or misbound request gets 401 before any handler runs.
  Without it the endpoint logs a warning once (`slog`, code `CAIRNFLOW_UNVERIFIED`) and runs with
  the restrictions above. `cairn.IdentityHosts(hosts...)` lists the public hosts a signed `host`
  claim may name, behind a proxy that rewrites `Host`.
- `cairn.MaxProtocolVersion(v)`: pin an older wire version, 1 to 6. The default is the SDK's
  maximum, 6, when `IdentityKeys` or `SegmentOrigins` is set, and 5 otherwise; asking for 6 without
  either is a configuration error.
- `cairn.ReplayCache(cairn.ReplayCacheOptions{MaxBytes, TTL})`, `cairn.WithoutReplayCache()` (wire
  v5, `REQUEST_RESPONSE`): keep the journals of running invocations in memory so the runtime only
  sends what is new; anything the cache cannot prove costs one extra round trip (a `409` miss, then
  the whole journal). On by default (64 MiB, 15 minutes idle) when `IdentityKeys` is set, off
  without it; `ReplayCache` without `IdentityKeys` is a configuration error. Off by default on
  Lambda; no effect with `Bidi` or below wire v5.
- `cairn.SegmentOrigins(origins...)`, `cairn.ValueCacheBytes(n)` (default 64 MiB),
  `cairn.MaxValueBytes(n)` (default 64 MiB, the tenant's default `max-value-bytes`),
  `cairn.MaxTurnValueBytes(n)` (default 256 MiB), `cairn.WithSegmentTransport(rt)` (wire v6
  segmented values: the SDK downloads and checks the segments before the handler runs, never
  follows a redirect, and never puts a segment URL in a log or an error; a failed download is
  reported to the runtime as a generic error). `SegmentOrigins` lists the origins segments may be
  read from (the runtime's value read origins); with `IdentityKeys` and without it, the SDK reads
  the URLs a verified request names. With neither, the endpoint declares wire 5 and the runtime
  sends every value inline.
- `cairn.WithoutCompression()`: stop accepting gzip request bodies and compressing large responses
  (on by default).
- `cairn.WithArtifactSource(baseURL, header)`: let `Artifacts().Get` download artifacts this process
  did not put, from the runtime's ingress with these credentials.

## Client

The `client` package calls a cluster through the HTTP ingress (`docs/api/openapi.json`) from an app
or a script, standard library only:

```go
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
```

A `202` never surfaces: the client attaches by id until the invocation is done or `client.Timeout`
runs out (`*client.AttachTimeout`; it keeps running). Every call carries a generated
`Idempotency-Key` kept across retries (`client.IdempotencyKey(k)` sets one,
`client.NoIdempotencyKey()` turns it off); the client follows the `leader` of a `503` and waits for
the `Retry-After` of a `429`. A handler's terminal error arrives as `*client.InvocationFailed`,
which `errors.As` also finds as a `*cairn.TerminalError` with the same code and message; returned
from a handler or a `Run` closure, it fails that step for good too. `c.Schedules()` manages
server-side schedules; `h.Cancel`, `h.Signal` and `h.Status` act on an invocation by id.

## Testing handlers without a server

`github.com/cairnflowdev/cairnflow/sdk/go/cairntest` runs a handler in the test process over the
SDK's real turn machine, with no runtime: every turn replays the whole journal so far, so a test
exercises replay too.

```go
inv := cairntest.New(orders, "submit", Order{Amount: 100}, cairntest.Key("o-1"))
st, err := inv.Run() // cairntest.Completed, Waiting, Failed, Paused or ContinuedAsNew
if err != nil || st != cairntest.Completed {
	t.Fatal(st, err)
}
var r Receipt
if err := inv.Output(&r); err != nil { // a failed run: *cairn.TerminalError; paused: *cairn.PauseError
	t.Fatal(err)
}
```

- `cairntest.Stub(service, handler, fn)` answers a call; `cairntest.State(key, value)` seeds
  state; `inv.Sends()` lists the sends.
- What the outside world would answer (a call without a stub, an awakeable, a signal, an attach)
  is in `inv.Pending()` until the test calls `inv.Resolve`, `inv.Reject`, `inv.Signal` or
  `inv.Cancel`, then `inv.Run()` again. Running a paused invocation again resumes it.
- Durable timers fire by themselves when the handler waits on timers alone; `inv.Advance(d)`
  moves the clock by hand.
- Code that is not deterministic (the clock or randomness outside `Run`, ranging over a map before
  a durable step) makes a later turn differ from the journal: `inv.Run()` returns that journal
  mismatch as an error.
- `inv.Get`, `inv.StateKeys`, `inv.Stream(name)` (including `$log` and `$trace`), `inv.Journal` and
  `inv.Turns` inspect the result.

## Tests

```sh
cd sdk/go
gofmt -l . && go vet ./... && go test ./...
```

- `conformance_test.go` is the conformance runner: a fake runtime replays every trace of
  `conformance/traces` over real HTTP (h2c for `BIDI_STREAM`) and compares every frame. Every trace
  below wire 6 runs again with the SDK pinned to the trace's version, and every
  `REQUEST_RESPONSE` trace that does not use the replay cache runs once more through
  `LambdaHandler`.
- `internal/wire` has fuzz targets for the frame splitter and the protobuf decoder
  (`go test -fuzz '^FuzzDecode$' ./internal/wire`).

Not ported from the TypeScript SDK: the Vercel AI SDK integration (`vercel.ts`), which is specific
to that library.
