use optd_repository::entity::{prelude::*, *};
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

    let one_snapshot = Snapshot::load().one(&db).await?;

    assert!(one_snapshot.is_none());

    Ok(())
}
