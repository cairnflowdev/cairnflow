# Unreleased changes

Add one file per user-visible change to this directory in the same pull request
as the change: `release-notes/unreleased/<short-name>.md`, with

- one line describing the change for users;
- `Breaking:` and a migration note when it changes an API, a flag, a default, the
  wire or the bucket (see [`docs/compatibility.md`](../../docs/compatibility.md)).

At release time these files are merged into `release-notes/vX.Y.Z.md` (see
[`RELEASING.md`](../../RELEASING.md)).
