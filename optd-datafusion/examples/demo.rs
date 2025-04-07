use std::env;
use std::error::Error;
use std::fs;

use optd_datafusion::run_queries;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <queries>.sql", args[0]);
        return Ok(());
    }

    let file_path = &args[1];
    let file = fs::read_to_string(file_path)?;

    // Retrieve all of the SQL queries from the file.
    let queries: Vec<&str> = file
        .split(';')
        .filter(|query| !query.trim().is_empty())
        .collect();

    run_queries(&queries).await?;

    Ok(())
}
