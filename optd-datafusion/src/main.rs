use datafusion::common::Result;
use optd_datafusion::run_queries;
use std::io;

#[tokio::main]
async fn main() -> Result<()> {
    let queries = io::read_to_string(io::stdin())?;
    run_queries(queries).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use optd_datafusion::run_queries;

    #[tokio::test]
    async fn test_scan() {
        let mut file = std::fs::File::open("sql/test_scan.sql").expect("Failed to open file");
        let mut queries = String::new();
        file.read_to_string(&mut queries)
            .expect("Failed to read from file");
        run_queries(queries).await.expect("Failed to run queries");
    }

    #[tokio::test]
    async fn test_filter() {
        let mut file = std::fs::File::open("sql/test_filter.sql").expect("Failed to open file");
        let mut queries = String::new();
        file.read_to_string(&mut queries)
            .expect("Failed to read from file");
        run_queries(queries).await.expect("Failed to run queries");
    }
}
