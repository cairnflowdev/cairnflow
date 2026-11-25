The TypeScript and Python SDKs no longer serve the replay cache or segmented values to a
runtime they do not verify, refuse an artifact download that does not match its ref, and
return one generic text for a failed segment read.

Breaking: an endpoint without `identityKeys` (Python: `identity_keys`)

- keeps no `REQUEST_RESPONSE` replay cache: `serve()` and the Python `Endpoint` used to turn
  it on by default. Asking for it (`replayCache: true` / options, `replay_cache=True` / a
  `ReplayCache`) without identity keys is now a configuration error. A caller who knew a
  running invocation id could read the held digest from a `409`, splice that invocation's
  journal onto its own turn and run the handler, side effects included, on the victim's
  input.
- declares wire v5 instead of v6 unless `segmentOrigins` (`segment_origins`) is set, and
  pinning `maxProtocolVersion` (`max_protocol_version`) to 6 there is a configuration
  error. Wire v6 makes the endpoint fetch the URLs in the request; the runtime sends values
  inline to a v5 endpoint, so handlers see the same values.

Migration: set `identityKeys` to the `kid` from the runtime's `GET /identity` (the runtime
signs when `CAIRNFLOW_IDENTITY_MASTER` is set). An endpoint that cannot verify identity can
set `segmentOrigins` to the runtime's value read origins to keep wire v6; it cannot keep the
replay cache.

Also: the SDKs keep at most 32 segment fetches in flight per process, refuse a value with
more than `maxValueBytes / 64 KiB` refs, and report every failed segment read after the
request went out as "could not be read, or did not match its ref" (no HTTP status, byte
count or network error). `ctx.artifact.get` refuses a ref that is not a sha256, reads at most the ref's size
(or the artifact limit), checks the size and sha256 before returning or remembering the
bytes, and in TypeScript follows no redirect and gives up after 60 s.
