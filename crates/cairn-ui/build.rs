// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Picks the embedded folder: `ui/dist` if it has been built, otherwise the
//! crate's placeholder page. `cargo test` and every Rust row of `check.sh`
//! therefore do not need Node.
//!
//! Change tracking: if `dist` exists, track `dist`. Otherwise track `ui/` (if
//! present) — so that the first `npm run build` is noticed. Never track an
//! **absent** path: cargo treats an absent path as always changed, reruns the
//! build script and recompiles all of `cairn-server` on every build.
//!
//! Every emitted path is **relative to the crate**: cargo caches build-script
//! output, and hashes workspace crates by relative path — an absolute path
//! survives moving the repo or switching to another worktree sharing `target/`,
//! and then points at a folder that no longer exists. rust-embed joins the
//! relative path with the `CARGO_MANIFEST_DIR` of the compilation itself.

use std::path::Path;

fn main() {
    const UI: &str = "../../ui";
    const DIST: &str = "../../ui/dist";
    const PLACEHOLDER: &str = "placeholder";
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={PLACEHOLDER}");
    // the build script runs with the crate folder as its working directory.
    let built = Path::new(DIST).join("index.html").is_file();
    let folder = if built {
        println!("cargo:rerun-if-changed={DIST}");
        DIST
    } else {
        if Path::new(UI).is_dir() {
            println!("cargo:rerun-if-changed={UI}");
        }
        PLACEHOLDER
    };
    println!("cargo:rustc-env=CAIRN_UI_FOLDER={folder}");
    println!("cargo:rustc-env=CAIRN_UI_BUILT={built}");
}
