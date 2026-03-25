use optd_repository::api::snapshot::{SnapshotInfo, commit_snapshot, get_current_snapshot_info};
use sea_orm::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;
    let database_url = std::env::var("DATABASE_URL")?;
    let db = Database::connect(database_url).await?;
    // let db = Database::connect("sqlite::memory:").await?;

    db.get_schema_registry("optd_repository::entity::*")
        .sync(&db)
        .await?;

    commit_snapshot(&db, SnapshotInfo::default()).await?;

    let one_snapshot = get_current_snapshot_info(&db).await?;

    assert!(one_snapshot.is_some());

    Ok(())
}
