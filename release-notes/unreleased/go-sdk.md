- New Go SDK (`sdk/go`, module `github.com/cairnflowdev/cairnflow/sdk/go`, package `cairn`, Go ≥
  1.25, no dependencies outside the standard library): services, virtual objects and workflows with
  typed durable steps, futures combined in journal order (`WaitFirst`, `Wait`, `All`, `Race`),
  `REQUEST_RESPONSE` (built-in HTTP/1.1 server, an `http.Handler`, AWS Lambda without the AWS
  libraries) and `BIDI_STREAM` (h2c on the same port), wire v1–v6 (the replay cache needs request
  identity; segmented values need request identity or a list of segment origins), request identity,
  and a `client` package for calling services. It passes every conformance trace.
