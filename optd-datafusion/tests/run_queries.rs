//! Simple tests that only check that the program doesn't panic.

use std::error::Error;
use std::fs;

use optd_datafusion::run_queries;

#[tokio::test]
async fn test_scan() -> Result<(), Box<dyn Error>> {
    let file = fs::read_to_string("./sql/test_scan.sql")?;

    // Retrieve all of the SQL queries from the file.
    let queries: Vec<&str> = file
        .split(';')
        .filter(|query| !query.trim().is_empty())
        .collect();

    run_queries(&queries).await?;

    Ok(())
}

#[tokio::test]
async fn test_filter() -> Result<(), Box<dyn Error>> {
    let file = fs::read_to_string("./sql/test_filter.sql")?;

    // Retrieve all of the SQL queries from the file.
    let queries: Vec<&str> = file
        .split(';')
        .filter(|query| !query.trim().is_empty())
        .collect();

    run_queries(&queries).await?;

    Ok(())
}

#[tokio::test]
async fn test_join() -> Result<(), Box<dyn Error>> {
    let file = fs::read_to_string("./sql/test_join.sql")?;

    // Retrieve all of the SQL queries from the file.
    let queries: Vec<&str> = file
        .split(';')
        .filter(|query| !query.trim().is_empty())
        .collect();

    run_queries(&queries).await?;

    Ok(())
}
