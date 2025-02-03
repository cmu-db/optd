//! A unique id sequence generator for the entities in the optd storage layer.

use sqlx::SqliteConnection;

/// Sequence is a unique generator for the entities in the optd storage layer.
pub struct Sequence;

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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::storage::StorageManager;

    #[tokio::test]
    async fn test_sequence() -> anyhow::Result<()> {
        let storage = StorageManager::new_in_memory().await?;
        storage.migrate().await?;

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
