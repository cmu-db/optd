use sqlx::SqliteConnection;
use std::ops::{Deref, DerefMut};

use crate::cascades::{
    expressions::{LogicalExpressionId, PhysicalExpressionId, ScalarExpressionId},
    groups::{RelationalGroupId, ScalarGroupId},
};

/// Sequence is a unique generator for the entities in the optd storage layer.
struct Sequence;

impl Sequence {
    /// Returns the current value in the sequence.
    pub async fn value(db: &mut SqliteConnection) -> anyhow::Result<i64> {
        let value = sqlx::query_scalar!("SELECT current_value FROM id_sequences WHERE id = 0")
            .fetch_one(db)
            .await?;
        Ok(value)
    }

    /// Sets the current value of the sequence to the given value.
    pub async fn set_value(db: &mut SqliteConnection, value: i64) -> anyhow::Result<()> {
        sqlx::query!(
            "UPDATE id_sequences SET current_value = ? WHERE id = 0",
            value
        )
        .execute(db)
        .await?;
        Ok(())
    }
}

/// A transaction that wraps a SQLite transaction.
pub struct Transaction<'c> {
    /// An active SQLite transaction.
    txn: sqlx::Transaction<'c, sqlx::Sqlite>,
    current_value: i64,
}

impl Transaction<'_> {
    pub async fn new(
        mut txn: sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> anyhow::Result<Transaction<'_>> {
        let current_value = Sequence::value(&mut txn).await?;
        Ok(Transaction { txn, current_value })
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> anyhow::Result<()> {
        Sequence::set_value(&mut self.txn, self.current_value).await?;
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
        &mut self.txn
    }
}

// TODO(alexis): This just checks the sequencing logic.
#[cfg(test)]
mod tests {

    use crate::storage::memo::SqliteMemo;

    use super::*;

    #[tokio::test]
    async fn test_sequence() -> anyhow::Result<()> {
        let storage = SqliteMemo::new_in_memory().await?;

        // Make sure the sequence is initialized.
        {
            let mut txn = storage.begin().await?;
            Sequence::set_value(&mut txn, 0).await?;
            txn.commit().await?;
        }

        // Test the sequence, rollback in the end.
        {
            let mut txn = storage.begin().await?;
            let value = Sequence::value(&mut txn).await?;
            assert_eq!(value, 0);

            for i in 0..5 {
                Sequence::set_value(&mut txn, i).await?;
                let value = Sequence::value(&mut txn).await?;
                assert_eq!(value, i);
            }
        }

        // Test the sequence, commit in the end.
        {
            let mut txn = storage.begin().await?;
            let value = Sequence::value(&mut txn).await?;
            assert_eq!(value, 0);

            for i in 0..5 {
                Sequence::set_value(&mut txn, i).await?;
                let value = Sequence::value(&mut txn).await?;
                assert_eq!(value, i);
            }
        }

        Ok(())
    }
}
