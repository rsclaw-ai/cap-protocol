fn main() {
    #[cfg(feature = "grpc")]
    tonic_build::compile_protos("proto/openclaude.proto")
        .unwrap_or_else(|e| panic!("Failed to compile proto: {e}"));
}
