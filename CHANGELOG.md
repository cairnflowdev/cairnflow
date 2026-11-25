# Changelog

Release notes for each version live in [`release-notes/`](release-notes/); this
file lists the releases.

## Unreleased

First public release in preparation. See
[`release-notes/unreleased/`](release-notes/unreleased/).

- Breaking (TypeScript and Python SDKs): an endpoint without identity keys keeps no replay
  cache and declares wire v5 unless segment origins are set; artifact downloads are checked
  against their ref. See
  [`release-notes/unreleased/sdk-unverified-endpoints.md`](release-notes/unreleased/sdk-unverified-endpoints.md).
