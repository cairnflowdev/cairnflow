# Contributing to Cairnflow

Thanks for your interest. Cairnflow is a durable execution engine whose state
lives entirely on object storage, so most changes touch correctness-critical
code.

Ways to help:

- Code: pull requests are welcome, under the contributor agreement described in
  "License and code contributions" below.
- Questions and ideas: GitHub Discussions (see [`SUPPORT.md`](SUPPORT.md)).
- Bugs: GitHub Issues, using the bug template.
- Security problems: **never** in a public issue — see [`SECURITY.md`](SECURITY.md).
- Design feedback on the wire protocol, the on-bucket format or an invariant:
  open a discussion; see "Design changes" below for how such changes are made.

## License and code contributions

The engine and console are licensed under the Business Source License 1.1; the
SDKs and the conformance suite under Apache-2.0 (see
[`LICENSING.md`](LICENSING.md)).

The engine is offered under both the Business Source License and commercial
licenses, and the Business Source License turns into Apache-2.0 on its change
date. The copyright holder can only do that with the right to license every
contribution under those terms, so a pull request is merged only after each of
its authors has signed the [Contributor License Agreement](CLA.md). It is the
Apache Software Foundation's individual agreement with the maintainer in the
Foundation's place, and it covers companies too. You keep the copyright in your
contribution; the agreement grants the maintainer, and everyone who receives
Cairnflow from the maintainer, a copyright and patent license to it, including
the right to sublicense it.

Signing takes one comment. When you open your first pull request, a bot posts
the sentence to copy into a comment on that pull request; the signature then
covers your later pull requests as well. If your employer owns what you write,
make sure you may contribute it (section 4 of the agreement).

You are responsible for what you submit, whichever tools produced it: the
statements of section 5 of the agreement apply to code written with an AI
assistant exactly as they do to code typed by hand.

The rest of this page describes how changes to Cairnflow are built and
checked.

## Setting up

Requirements:

- Rust — the toolchain pinned in `rust-toolchain.toml` (`rustup` installs it).
- `protoc` (protobuf compiler), `cmake`, a C/C++ compiler.
- Node.js ≥ 22.6 (24 recommended) for the SDK, the console, the end-to-end tests and the
  engine harness (`harness/`: chaos, mixed-binary and benchmark drivers).
- Optional: `cargo-nextest` (faster test runs), `cargo-audit`, `cargo-deny`,
  `cargo-about`, Docker (browser rows of the console tests, local S3 for chaos runs).

```sh
git clone https://github.com/cairnflowdev/cairnflow
cd cairnflow
git submodule update --init          # vendor/slatedb — the build fails without it
(cd ui && npm ci && npm run build)   # the binary embeds the console at compile time
cargo build --bin cairnflow
```

`vendor/slatedb` is a fork of SlateDB used through `[patch.crates-io]`. If it is
empty, cargo stops with "failed to read `vendor/slatedb/…/Cargo.toml`": run the
submodule command above.

## Checking your change

There are two profiles:

- **WIP** — while you work: `cargo test -p <crate>` plus the rows of the table
  "what to run when you change what" in [`AGENTS.md`](AGENTS.md) §2 that your
  change touches. That table is the map of which tests guard which subsystem;
  please follow it.
- **Ready** — before asking for review: `scripts/check.sh`. It runs formatting,
  source lints, clippy with three feature sets, the whole workspace test suite
  (including fault-injection rows), the TypeScript checks, the console checks
  and an end-to-end matrix. Logs land in `target/check/`. With `cargo-nextest`
  installed it takes a few minutes on a warm cache; `--quick` runs only the Rust
  part.

CI runs the same checks on every pull request. Say in the pull request which
profile you ran and paste failures you could not fix.

## Rules for changes

The full list is in [`AGENTS.md`](AGENTS.md) §1 (it is written for both human
and AI contributors). The ones reviewers check first:

1. **Behaviour changes come as a pair of commits**: first `test:` with a test that
   fails on the old code (marked `#[ignore = "red until …"]` so the tree stays
   green), then `fix:` that removes the `ignore` and makes it pass. New
   infrastructure that does not change behaviour (a feature off by default,
   docs) is a single `feat:`/`docs:` commit with its tests.
2. **The on-bucket format is frozen.** Do not bump `DATA_FORMAT`. Layout changes
   go through the migration framework (`crates/cairn-store/src/migrate.rs`) —
   see [`docs/compatibility.md`](docs/compatibility.md).
3. **No network I/O while holding the engine**, and **acknowledge only after the
   write is durable**. These are enforced by types and lints; do not work around
   them.
4. **Delete only what you can prove is unreferenced.** Every deletion of customer
   data goes through the delete-proof gate and the trash; when in doubt, keep.
5. **Resolve ambiguous conditional writes by reading back** (`cas::put_resolving`).
6. Every invariant has a test; if you change an invariant, change the subsystem
   guide in `docs/guides/` in the same commit.

Commit messages use a conventional prefix (`fix:`, `test:`, `feat:`, `docs:`,
`chore:`, `refactor:`) and explain *why*. Code, comments, docs and commit
messages are in English. Text users see at runtime (HTTP error bodies, CLI help,
metric help) is English too and is checked by `english_text`/`cli_text` tests.

## Design changes

Changes to the wire protocol, the on-bucket format, delete paths or any
invariant in [`docs/design/decisions.md`](docs/design/decisions.md) start as an
RFC: a markdown file in `rfcs/` describing the problem, the design, the
invariants it keeps or changes, the migration and the tests that will prove it.
Open it as a pull request; implementation follows once it is accepted.

## Reviews

Every pull request needs green CI, a signed contributor agreement from each of
its authors, and one maintainer review. See
[`GOVERNANCE.md`](GOVERNANCE.md) for how decisions are made.
