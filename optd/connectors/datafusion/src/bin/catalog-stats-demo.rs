//! Demonstrates persistent statistics and catalog snapshot advancement.

use std::{error::Error, path::PathBuf};

use datafusion::arrow::array::{Float64Array, Int64Array};
use optd_catalog::Catalog;
use optd_datafusion::{
    persistent_catalog::install_persistent_catalog, setup::session_context_with_information_schema,
};
use sea_orm::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("optd-statistics-demo.sqlite"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }

    let db = Database::connect(format!("sqlite://{}?mode=rwc", path.display())).await?;
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
