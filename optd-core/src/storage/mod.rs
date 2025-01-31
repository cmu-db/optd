use sqlx::{pool::PoolConnection, SqlitePool};

pub mod sequence;

/// A Storage manager that manages connections to the database.
pub struct StorageManager {
    /// A async connection pool to the SQLite database.
    /// TODO(yuchen): We can support more backend in the future.
    db: SqlitePool,
}

impl StorageManager {
    /// Create a new StorageManager.
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let db = SqlitePool::connect(database_url).await?;
        Ok(Self { db })
    }

    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Migrate the database.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::migrate!("src/storage/migrations")
            .run(&self.db)
            .await?;
        Ok(())
    }

    pub async fn db(&self) -> anyhow::Result<PoolConnection<sqlx::Sqlite>> {
        let connection = self.db.acquire().await?;
        Ok(connection)
    }
}
