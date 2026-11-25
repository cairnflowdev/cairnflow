# syntax=docker/dockerfile:1
#
# The cairnflow image: built in a Rust image, runs on debian-slim as a non-root
# user.
#
#   docker build -t cairnflow:local .
#   docker run --rm -p 8080:8080 -v cairnflow-data:/var/lib/cairnflow cairnflow:local
#
# By default the image runs **one node** on `file:///var/lib/cairnflow`, listening
# on 0.0.0.0:8080. A cluster on S3: see `deploy/docker-compose.yml` and
# `docs/self-hosting.md` — in a cluster, `--host` must NOT be 0.0.0.0 (the address
# advertised to peers is the bound address).
#
# The SlateDB fork is the git submodule `vendor/slatedb`, wired in through
# `[patch.crates-io]` in `Cargo.toml` — run `git submodule update --init` before
# building the image, or cargo cannot find `vendor/slatedb/slatedb`.

ARG RUST_VERSION=1.97
ARG DEBIAN_RELEASE=bookworm
ARG NODE_VERSION=26
# The digest of each base image: a tag's content changes over time, a digest's
# does not — a rebuild gets exactly the base that was tested. When changing a
# version above, fetch the new digest with `docker buildx imagetools inspect
# <image>` (the digest of the multi-platform manifest list) and update it in the
# same commit; `scripts/lint-sources.sh` (check 10) fails when a `FROM` lacks a
# digest. Fetched on 2026-11-07.
ARG NODE_DIGEST=sha256:add5c17c14752c54d2f3d519ec46a3815684d41c33f198bcb09fb998f7dcbf68
ARG RUST_DIGEST=sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3
ARG DEBIAN_DIGEST=sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

# ───────────────────────────────────────────── web console
# Build `ui/` first: the `cairn-ui` crate embeds `ui/dist` at `cargo build`; without
# it the image serves a placeholder page at /ui/.
FROM node:${NODE_VERSION}-${DEBIAN_RELEASE}-slim@${NODE_DIGEST} AS ui
WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY ui ./
RUN npm run build

# ───────────────────────────────────────────── build
FROM rust:${RUST_VERSION}-slim-${DEBIAN_RELEASE}@${RUST_DIGEST} AS builder

# protoc: `cairn-protocol/build.rs` (prost-build). cmake + g++: -sys crates
# (aws-lc-sys, zstd-sys, lz4-sys) may need them depending on the version.
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler cmake g++ make pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
COPY --from=ui /ui/dist ./ui/dist

# The repository's `[profile.release] debug = true` is for profiling; the image does
# not need full debuginfo (the symbol table stays ⇒ backtraces still have function
# names).
ENV CARGO_PROFILE_RELEASE_DEBUG=false
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    test -f vendor/slatedb/slatedb/Cargo.toml \
      || { echo "vendor/slatedb is missing — run 'git submodule update --init' before building" >&2; exit 1; } \
 && cargo build --release --locked --bin cairnflow --bin cairn-query-worker --bin cairn --bin cairn-ctl \
 && mkdir -p /out \
 && cp target/release/cairnflow target/release/cairn-query-worker target/release/cairn target/release/cairn-ctl /out/

# ───────────────────────────────────────────── run
FROM debian:${DEBIAN_RELEASE}-slim@${DEBIAN_DIGEST}

# ca-certificates: TLS to a real S3. curl: healthcheck (`GET /health`).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 cairnflow \
 && useradd --system --uid 10001 --gid cairnflow --home-dir /var/lib/cairnflow \
      --shell /usr/sbin/nologin cairnflow \
 && mkdir -p /var/lib/cairnflow \
 && chown cairnflow:cairnflow /var/lib/cairnflow

# `cairn-ctl`: backup/restore, `verify`, migration gates — built by the same command,
# so the same version as `cairnflow` (`cairn-ctl --version`, `knows=`). It reads the
# bucket directly with the node's credentials: inside the image they are at hand.
# `cairn-query-worker` sits **next to** `cairnflow`: the node looks for it there and
# runs every `/query` statement in a child process of it.
COPY --from=builder /out/cairnflow /out/cairn-query-worker /out/cairn /out/cairn-ctl /usr/local/bin/
# License, notice and (in release builds) THIRD_PARTY_LICENSES — the release
# workflow generates dist-licenses/ with scripts/third-party-licenses.sh; a local
# build without it ships LICENSE, LICENSING.md and NOTICE only.
COPY LICENSE LICENSING.md NOTICE dist-license[s] /usr/share/doc/cairnflow/

# Every flag has a matching environment variable (`cairnflow --help`); a flag on the
# command line wins over the environment variable, `--main-store` wins over
# `--data-dir`.
ENV CAIRNFLOW_HOST=0.0.0.0 \
    CAIRNFLOW_PORT=8080 \
    CAIRNFLOW_DATA_DIR=/var/lib/cairnflow

USER 10001:10001
WORKDIR /var/lib/cairnflow
# 8080: ingress + admin for clients. 8081: the internal port between nodes
# (`--internal-port`, default `--port` + 1) — open it only inside the cluster network.
# Auth is on by default: pass `-e CAIRNFLOW_ADMIN_KEY=…`, or `--no-auth` for a quick
# try (docs/self-hosting.md §8).
EXPOSE 8080 8081

ENTRYPOINT ["cairnflow"]
