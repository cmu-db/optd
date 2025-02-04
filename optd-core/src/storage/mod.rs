//! The storage module provides the storage interface for the optimizer.

use sequence::Sequence;
use serde::{Deserialize, Serialize};
use sqlx::{pool::PoolConnection, SqliteConnection, SqlitePool};
use std::ops::{Deref, DerefMut};

pub mod sequence;

/// A unique identifier for a logical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct LogicalExpressionId(pub i64);

/// A unique identifier for a physical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct PhysicalExpressionId(pub i64);

/// A unique identifier for a scalar expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ScalarExpressionId(pub i64);

/// A unique identifier for a group of relational expressions in the memo table.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Serialize, Deserialize,
)]
#[sqlx(transparent)]
pub struct RelationalGroupId(pub i64);

/// A unique identifier for a group of scalar expressions in the memo table.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Serialize, Deserialize,
)]
#[sqlx(transparent)]
pub struct ScalarGroupId(pub i64);

/// The exploration status of a group or a logical expression in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i32)]
pub enum ExplorationStatus {
    /// The group or the logical expression has not been explored.
    Unexplored,
    /// The group or the logical expression is currently being explored.
    Exploring,
    /// The group or the logical expression has been explored.
    Explored,
}

/// The optimization status of a goal or a physical expression in the memo table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i32)]
pub enum OptimizationStatus {
    /// The group or the physical expression has not been optimized.
    Unoptimized,
    /// The group or the physical expression is currently being optimized.
    Pending,
    /// The group or the physical expression has been optimized.
    Optimized,
}

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
    pub async fn new_relational_group_id(&mut self) -> anyhow::Result<RelationalGroupId> {
        let id = self.current_value;
        self.current_value += 1;
        Ok(RelationalGroupId(id))
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

impl Deref for Transaction<'_> {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DerefMut for Transaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.txn
    }
}
