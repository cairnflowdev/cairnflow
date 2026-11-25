// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! **`cairn-query-worker`** — the child process running one `/query` statement
//! (`cairn_query::worker`). The `cairnflow` node spawns it, hands it one statement over
//! stdin, serves scans over a pipe, reads the answer from stdout, then kills it. Not
//! for manual use.
//!
//! Its own global allocator counts the bytes allocated: over `--memory-bytes` ⇒ exit at
//! once with code `cairn_query::worker::EXIT_MEMORY`. Only this binary declares it — the
//! node's hot path counts nothing.

#[global_allocator]
static ALLOC: cairn_query::worker::CappedAlloc = cairn_query::worker::CappedAlloc::new();

fn main() {
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--memory-bytes" => {
                if let Some(n) = args.next().and_then(|v| v.parse::<usize>().ok()) {
                    ALLOC.set_limit(n);
                }
            }
            "--version" => {
                println!("cairn-query-worker {}", env!("CARGO_PKG_VERSION"));
                return;
            }
            _ => {
                eprintln!(
                    "cairn-query-worker runs one /query for a cairnflow node; it is started by the node, not by hand"
                );
                std::process::exit(2);
            }
        }
    }
    std::process::exit(cairn_query::worker::child_main())
}
