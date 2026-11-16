//! Demonstrates persistent statistics and catalog snapshot advancement.

use std::{
    error::Error,
    fs::OpenOptions,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use datafusion::arrow::array::{Float64Array, Int64Array};
use optd_catalog::Catalog;
use optd_datafusion::{
    persistent_catalog::install_persistent_catalog, setup::session_context_with_information_schema,
};
use sea_orm::{
    SqlxSqliteConnector,
    sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = match std::env::args_os().nth(1) {
        Some(path) => PathBuf::from(path),
        None => std::env::temp_dir().join(format!(
            "optd-statistics-demo-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        )),
    };
    // Reserve a new file atomically. Never reset an existing caller-supplied path.
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    // Use an absolute literal filename, not a URL (or SQLite's `file:` URI syntax).
    let options = SqliteConnectOptions::new()
        .filename(path.canonicalize()?)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let db = SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
    let session = session_context_with_information_schema();
    install_persistent_catalog(&session, "datafusion", db.clone()).await?;
    session
        .sql(
            "CREATE TABLE numbers AS \
             SELECT * FROM (VALUES (1), (2), (2)) AS source(value)",
        )
        .await?
        .collect()
        .await?;

    let catalog = Catalog::new(db.clone());
    let relation_snapshot = catalog.reader().await?.snapshot_id();
    let relation_id = catalog.reader().await?.get_all_relations().await?[0].relation_id;
    let estimate_before = float_scalar(&session, "SELECT cardest('SELECT * FROM numbers')").await?;
    let statistics_snapshot = integer_scalar(&session, "SELECT collect_stats('numbers')").await?;
    let estimate_after = float_scalar(&session, "SELECT cardest('SELECT * FROM numbers')").await?;

    let historical_statistics = catalog
        .reader_at(relation_snapshot)
        .await?
        .get_relation_statistics(relation_id)
        .await?;
    let current_statistics = catalog
        .reader()
        .await?
        .get_relation_statistics(relation_id)
        .await?
        .expect("statistics were just collected");

    println!("catalog database: {}", path.display());
    println!("relation snapshot: {relation_snapshot}");
    println!("estimate before collection: {estimate_before}");
    println!("statistics snapshot: {statistics_snapshot}");
    println!(
        "statistics visible at relation snapshot: {}",
        historical_statistics.is_some()
    );
    println!("persisted row count: {:?}", current_statistics.row_count);
    println!("estimate after collection: {estimate_after}");

    drop(catalog);
    db.close().await?;
    Ok(())
}

async fn float_scalar(
    session: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<f64, Box<dyn Error>> {
    let batches = session.sql(sql).await?.collect().await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("query should return Float64")
        .value(0))
}

async fn integer_scalar(
    session: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<i64, Box<dyn Error>> {
    let batches = session.sql(sql).await?.collect().await?;
    Ok(batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("query should return Int64")
        .value(0))
}
