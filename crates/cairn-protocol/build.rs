// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1

fn main() {
    println!("cargo:rerun-if-changed=proto/protocol.proto");
    let mut cfg = prost_build::Config::new();
    cfg.bytes([".dev.cairnflow.service.protocol"]);
    cfg.compile_protos(&["proto/protocol.proto"], &["proto"])
        .expect("compiling protocol.proto failed — `protoc` must be on PATH");
}
