# Releasing

How a Cairnflow release is cut. Only maintainers release.

## What a release contains

- Binaries `cairnflow`, `cairn-query-worker`, `cairn`, `cairn-ctl` for Linux
  (x86_64, aarch64; static musl) and macOS (aarch64), attached to a GitHub
  Release, each archive with `LICENSE`, `NOTICE` and `THIRD_PARTY_LICENSES`.
- A container image `ghcr.io/cairnflowdev/cairnflow`, multi-arch,
  with `THIRD_PARTY_LICENSES` under `/usr/share/doc/cairnflow/`.
- The TypeScript SDK `@cairnflow/sdk` on npm, published with provenance.
- Release notes in `release-notes/vX.Y.Z.md`.

## Steps

1. **Freeze**: no merges to `main` except the release commit.
2. **Ready** passes on the release commit: `scripts/check.sh` (full profile), and
   the nightly rows (failpoints, ambiguous-op faults, simulation with 64 seeds,
   end-to-end on S3) are green on it.
3. **Upgrade test**: the mixed-binary upgrade test against the previous release
   tag (`scripts/mixed-binary.sh`) passes. Skipped for the first release (no
   previous tag).
4. **Release notes**: move `release-notes/unreleased/*` into
   `release-notes/vX.Y.Z.md`; every breaking change has a migration note (see
   [`docs/compatibility.md`](docs/compatibility.md)). Update `CHANGELOG.md`.
5. **Versions**: set `version` in the workspace `Cargo.toml` and in
   `sdk/typescript/package.json`; commit `chore: release vX.Y.Z`.
6. **Tag** `vX.Y.Z` (signed) and push the tag. The `release` workflow builds the
   binaries and the image, generates `THIRD_PARTY_LICENSES`
   (`scripts/third-party-licenses.sh`) and drafts the GitHub Release. The image
   job waits for an approval in the `release` environment before it pushes to
   `ghcr.io`.
7. **Check the draft**: download one archive per platform, run
   `cairnflow --version`, the quickstart from `README.md`, and read
   `THIRD_PARTY_LICENSES`. Publish the release.
8. **npm**: `cd sdk/typescript && npm run publish:beta` (dry run by default; see
   the script) or approve the publish job of the workflow.
9. Announce in Discussions.

## Repository settings a release relies on

- An environment named `release` with required reviewers (Settings →
  Environments). Without reviewers the image job pushes as soon as a `v*` tag
  exists.
- A tag ruleset that lets only maintainers create `v*` tags.

## License dates

Each version's Business Source License converts to Apache-2.0 four years after
that version is first published (see [`LICENSE`](LICENSE)). The release date in
`release-notes/vX.Y.Z.md` is the date that counts; do not edit it later.

## Advisories

Before tagging, re-read `.cargo/audit.toml`: every ignored advisory must still
have a correct justification on the release date.
