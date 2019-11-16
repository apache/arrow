fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../format/Flight.proto")?;
    Ok(())
}