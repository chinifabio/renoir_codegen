use renoir_codegen::make_environment;

make_environment!("example.json");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    execute_environment()?;
    Ok(())
}