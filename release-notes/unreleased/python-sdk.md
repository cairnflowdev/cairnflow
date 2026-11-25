- New Python SDK (`sdk/python`, package `cairnflow`, Python ≥ 3.11, no runtime dependencies):
  services, virtual objects and workflows with the whole `ctx` API, `REQUEST_RESPONSE` (built-in
  HTTP/1.1 server, ASGI app, AWS Lambda) and `BIDI_STREAM` (optional `h2` extra), wire v1–v6
  (replay cache, segmented values), request identity, and a client for calling services. It
  passes every conformance trace and runs end to end against the binary.
