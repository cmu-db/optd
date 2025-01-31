use sqlx::SqliteConnection;

/// Sequence is a unique generator for the entities in the optd storage layer.
pub struct Sequence;

impl Sequence {
    /// Returns the current value in the sequence.
    pub async fn value(db: &mut SqliteConnection) -> anyhow::Result<i64> {
        let v = sqlx::query_scalar!("SELECT current_value FROM id_sequences WHERE id = 0")
            .fetch_one(db)
            .await?;
        Ok(v)
    }

    /// Returns the next value in the sequence.
    pub async fn next_value(db: &mut SqliteConnection) -> anyhow::Result<i64> {
        let v = sqlx::query_scalar!(
            "UPDATE id_sequences SET current_value = current_value + 1 where id = 0 RETURNING current_value"
        )
        .fetch_one(db)
        .await?;

        Ok(v)
    }

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
    use sqlx::Connection;

    #[tokio::test]
    async fn test_sequence() -> anyhow::Result<()> {
        let storage = StorageManager::new_in_memory().await?;
        storage.migrate().await?;

        let mut db = storage.db().await?;
        Sequence::set_value(&mut db, 0).await?;
        let v = Sequence::value(&mut db).await?;
        assert_eq!(v, 0);

        let v = Sequence::next_value(&mut db).await?;
        assert_eq!(v, 1);

        let v = Sequence::value(&mut db).await?;
        assert_eq!(v, 1);

        Sequence::set_value(&mut db, 0).await?;
        let v = Sequence::value(&mut db).await?;
        assert_eq!(v, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sequence_in_txn() -> anyhow::Result<()> {
        let storage = StorageManager::new_in_memory().await?;
        storage.migrate().await?;

        let mut db = storage.db().await?;
        db.transaction(|txn| {
            Box::pin(async move {
                Sequence::set_value(txn, 0).await?;
                let v = Sequence::value(txn).await?;
                assert_eq!(v, 0);

                for i in 1..=10 {
                    let v = Sequence::next_value(txn).await?;

                    assert_eq!(v, i);
                }

                Err::<(), anyhow::Error>(anyhow::anyhow!("rollback"))
            })
        })
        .await
        .expect_err("transaction should have rolled back");

        db.transaction(|txn| {
            Box::pin(async move {
                let v = Sequence::value(txn).await?;
                assert_eq!(v, 0);
                anyhow::Ok(())
            })
        })
        .await?;

        Ok(())
    }
}
