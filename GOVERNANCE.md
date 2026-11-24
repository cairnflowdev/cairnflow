# Governance

## Roles

- **Maintainer** — currently a single maintainer, who has the final say on
  design, scope, releases and licensing: Phuong Pham (@congphuong).
- **Committers** — people with merge rights, added by the maintainer.
- **Contributors** — anyone who opens an issue or a discussion. Code
  contributions from outside the project are not open yet (see
  [`CONTRIBUTING.md`](CONTRIBUTING.md)).

## How decisions are made

- Day-to-day changes: a pull request with green CI and one approving review from
  a maintainer or committer.
- Changes to the wire protocol, the on-bucket format, delete paths or an
  invariant: an RFC in `rfcs/` (see [`CONTRIBUTING.md`](CONTRIBUTING.md)),
  discussed in the open and accepted by the maintainer.
- Licensing and trademark decisions belong to the copyright holder (see
  [`LICENSING.md`](LICENSING.md)).

## Becoming a committer

Once code contributions open, committers are invited after a sustained record
of high-quality contributions and reviews — typically several merged non-trivial pull requests that followed
the testing rules in [`AGENTS.md`](AGENTS.md), and helpful reviews of others'
work. Committers are expected to follow the same rules and the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Changes to this document

Through a pull request approved by the maintainer.
