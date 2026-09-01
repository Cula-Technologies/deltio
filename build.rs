fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the PubSub gRPC service definitions.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .field_attribute(".", "#[allow(clippy::all)]")
        .compile_protos(
            &["proto/googleapis/google/pubsub/v1/pubsub.proto"],
            &["proto/googleapis"],
        )?;

    // The `rerun-if-env-changed` below opts out of cargo's default "rerun on any package
    // change", so the proto sources have to be declared explicitly or edits to them are
    // silently not picked up.
    println!("cargo:rerun-if-changed=proto");

    // If we set CARGO_PKG_VERSION this way, then it will override the default value, which is
    // taken from the `version` in Cargo.toml.
    if let Ok(val) = std::env::var("DELTIO_RELEASE_VERSION") {
        println!("cargo:rustc-env=CARGO_PKG_VERSION={}", val);
    }
    println!("cargo:rerun-if-env-changed=DELTIO_RELEASE_VERSION");
    Ok(())
}
