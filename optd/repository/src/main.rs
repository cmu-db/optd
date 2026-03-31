use optd_repository::api::snapshot::{SnapshotInfo, commit_snapshot};
use sea_orm::{Database, DatabaseConnection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let db = Database::connect(database_url).await?;
    // let db = Database::connect("sqlite::memory:").await?;

    db.get_schema_registry("optd_repository::entity::*")
        .sync(&db)
        .await?;

    smoke_test_all_apis(&db).await?;

    Ok(())
}

async fn smoke_test_all_apis(db: &DatabaseConnection) -> Result<(), Box<dyn std::error::Error>> {
    commit_snapshot(db, SnapshotInfo::default()).await?;

    Ok(())
}
