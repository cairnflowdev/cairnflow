# Security policy

## Reporting a vulnerability

Please report vulnerabilities **privately** through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
("Report a vulnerability" under the repository's **Security** tab). Do not open
a public issue, discussion or pull request for a security problem.

If you cannot use GitHub's private reporting, email security@cairnflow.dev instead.

Include what you can: affected version or commit, configuration (single node or
cluster, object store, `--profile saas` or not, authentication mode), steps to
reproduce, and the impact you expect.

## What to expect

- Acknowledgement within **3 business days**.
- A first assessment (confirmed or not, severity) within **10 business days**.
- A fix or mitigation for confirmed high-severity issues as fast as we can; we
  coordinate the disclosure date with you, normally within 90 days of the
  report.
- Credit in the advisory and release notes unless you prefer otherwise.

## Supported versions

Cairnflow is in beta. Security fixes land on `main` and in the latest release
only.

| Version | Supported |
|---|---|
| latest `0.x` release | yes |
| older releases | no — upgrade (see [`docs/compatibility.md`](docs/compatibility.md)) |

## Scope

In scope: the engine (`cairnflow`, `cairn-query-worker`, `cairn`, `cairn-ctl`), the
web console, the TypeScript SDK, the container image and the sample deployment
files in `deploy/`.

Before deploying for several mutually untrusted tenants, read
[`docs/self-hosting-security.md`](docs/self-hosting-security.md): several
protections are off by default when self-hosting and must be enabled with flags
(or `--profile saas`). Reports that a protection documented there as "off by
default" is off are not vulnerabilities, but we welcome suggestions to make the
defaults safer.
