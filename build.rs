fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/pubsub.proto")?;
    tonic_build::compile_protos("proto/s3_file.proto")?;
    Ok(())
}