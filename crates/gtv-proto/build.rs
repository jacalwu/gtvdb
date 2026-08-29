fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the protoc binary bundled by protoc-bin-vendored so builds do not
    // require a system-wide protoc install.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);
    tonic_prost_build::configure()
        .compile_with_config(config, &["proto/gtvquery.proto"], &["proto"])?;
    Ok(())
}
