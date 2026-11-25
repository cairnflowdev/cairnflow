## What and why

<!-- What does this change, and why? Link the issue or RFC. -->

## How it was checked

- [ ] Behaviour change: a `test:` commit that fails on the old code, then the `fix:` commit
      (or: no behaviour change — say why)
- [ ] Rows of the "what to run" table in `AGENTS.md` §2 that this change touches (list them):
- [ ] Profile run: WIP (`cargo test -p …`) / Ready (`scripts/check.sh`) — paste failures you could not fix
- [ ] Docs and subsystem guides (`docs/guides/`) updated if an invariant changed
- [ ] A file in `release-notes/unreleased/` if users see the change (`Breaking:` + migration note when needed)

## Not checked

<!-- Rows skipped, platforms not tried, chaos not run, and why. -->
