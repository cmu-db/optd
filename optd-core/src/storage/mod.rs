//! The storage module provides the storage interface for the optimizer.

use std::ops::{Deref, DerefMut};

use sequence::Sequence;
use sqlx::{pool::PoolConnection, SqliteConnection, SqlitePool};

use crate::memo::{
    GroupId, LogicalExpressionId, PhysicalExpressionId, ScalarExpressionId, ScalarGroupId,
};

pub mod sequence;

/// A Storage manager that manages connections to the database.
pub struct StorageManager {
    /// A async connection pool to the SQLite database.
    /// TODO(yuchen): We can support more backend in the future.
    db: SqlitePool,
}

impl StorageManager {
    /// Create a new storage manager that connects to the SQLite database at the given URL.
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let db = SqlitePool::connect(database_url).await?;
        Ok(Self { db })
    }

    /// Begin a new transaction.
    pub async fn begin(&self) -> anyhow::Result<Transaction<'_>> {
        let mut txn = self.db.begin().await?;
        let current_value = Sequence::value(&mut *txn).await?;

        Ok(Transaction { txn, current_value })
    }

    /// Create a new storage manager backed by an in-memory SQLite database.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Runs pending migrations.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        // sqlx::migrate! takes the path relative to the root of the crate.
        sqlx::migrate!("src/storage/migrations")
            .run(&self.db)
            .await?;
        Ok(())
    }

    /// Get a connection to the database.
    pub async fn db(&self) -> anyhow::Result<PoolConnection<sqlx::Sqlite>> {
        let connection = self.db.acquire().await?;
        Ok(connection)
    }
}

/// A transaction that wraps a SQLite transaction.
pub struct Transaction<'c> {
    /// An active SQLite transaction.
    txn: sqlx::Transaction<'c, sqlx::Sqlite>,
    current_value: i64,
}

impl Transaction<'_> {
    /// Commit the transaction.
    pub async fn commit(mut self) -> anyhow::Result<()> {
        Sequence::set_value(&mut *self.txn, self.current_value).await?;
        self.txn.commit().await?;
        Ok(())
    }

    /// Gets a new group id.
    pub async fn new_group_id(&mut self) -> anyhow::Result<GroupId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(GroupId(id))
    }

    /// Gets a new scalar group id.
    pub async fn new_scalar_group_id(&mut self) -> anyhow::Result<ScalarGroupId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(ScalarGroupId(id))
    }

    /// Gets a new logical expression id.
    pub async fn new_logical_expression_id(&mut self) -> anyhow::Result<LogicalExpressionId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(LogicalExpressionId(id))
    }

    /// Gets a new physical expression id.
    pub async fn new_physical_expression_id(&mut self) -> anyhow::Result<PhysicalExpressionId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(PhysicalExpressionId(id))
    }

    /// Gets a new physical expression id.
    pub async fn new_scalar_expression_id(&mut self) -> anyhow::Result<ScalarExpressionId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(ScalarExpressionId(id))
    }
}
