//! Generates the gRPC code from the `.proto` files in `/proto`.
//!
//! Uses the `protoc` binary vendored by `protoc-bin-vendored`, so building
//! needs no system-wide Protocol Buffers compiler.

use std::io;

fn main() -> io::Result<()> {
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["../../proto/raft.proto", "../../proto/kv.proto"],
        &["../../proto"],
    )
}
