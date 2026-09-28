//! Snapshot-aware catalog API.

use sea_orm::{ConnectionTrait, DbErr, TransactionTrait};

use crate::{relation, schema, snapshot};

/// Catalog backed by a SeaORM connection or transaction manager.
pub struct Catalog<T> {
    db: T,
}

/// Read-only catalog handle pinned to one immutable snapshot.
pub struct CatalogReader<'a, T> {
    db: &'a T,
    snapshot: snapshot::SnapshotInfo,
}

fn flatten_transaction_err<T>(
    result: Result<T, sea_orm::TransactionError<DbErr>>,
) -> Result<T, DbErr> {
    result.map_err(|error| match error {
        sea_orm::TransactionError::Connection(error)
        | sea_orm::TransactionError::Transaction(error) => error,
    })
}

impl<T> Catalog<T> {
    /// Creates a catalog backed by `db`.
    pub fn new(db: T) -> Self {
        Self { db }
    }

    /// Returns the underlying database handle.
    pub fn connection(&self) -> &T {
        &self.db
    }
}

impl<T: ConnectionTrait> CatalogReader<'_, T> {
    /// Returns the snapshot used by every read from this handle.
    pub fn snapshot(&self) -> &snapshot::SnapshotInfo {
        &self.snapshot
    }

    /// Returns all schemas visible at the pinned snapshot.
    pub async fn get_all_schemas(&self) -> Result<Vec<schema::SchemaInfo>, DbErr> {
        schema::get_all_schema_infos(self.db, &self.snapshot).await
    }

    /// Returns one schema visible at the pinned snapshot.
    pub async fn get_schema(
        &self,
        info: schema::GetSchemaInfo,
    ) -> Result<schema::SchemaInfo, DbErr> {
        let schema_id = info.schema_id;
        schema::get_schema(info, self.db, &self.snapshot)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("schema {schema_id} not found")))
    }

    /// Returns all relations visible at the pinned snapshot.
    pub async fn get_all_relations(&self) -> Result<Vec<relation::RelationInfo>, DbErr> {
        relation::get_all_relation_infos(self.db, &self.snapshot).await
    }

    /// Returns one relation visible at the pinned snapshot.
    pub async fn get_relation(
        &self,
        info: relation::GetRelationInfo,
    ) -> Result<relation::RelationInfo, DbErr> {
        let relation_id = info.relation_id;
        relation::get_relation(info, self.db, &self.snapshot)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("relation {relation_id} not found")))
    }
}

impl<T: ConnectionTrait> Catalog<T> {
    /// Creates a reader pinned to `snapshot_id`.
    pub async fn reader_at(&self, snapshot_id: i64) -> Result<CatalogReader<'_, T>, DbErr> {
        let snapshot = snapshot::get_snapshot_info(&self.db, snapshot_id)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(format!("snapshot {snapshot_id} not found")))?;
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
        })
    }

    /// Creates a reader pinned to the latest snapshot.
    pub async fn reader(&self) -> Result<CatalogReader<'_, T>, DbErr> {
        let snapshot = snapshot::get_current_snapshot_info(&self.db)
            .await?
            .unwrap_or_default();
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
        })
    }
}

impl<T: TransactionTrait> Catalog<T> {
    /// Creates a schema and publishes it in a new snapshot.
    pub async fn create_schema(&self, info: schema::CreateSchemaInfo) -> Result<i64, DbErr> {
        flatten_transaction_err(
            self.db
                .transaction::<_, _, DbErr>(|txn| {
                    Box::pin(async move {
                        let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                            .await?
                            .unwrap_or_default();
                        let schema_id = current_snapshot.get_next_catalog_id();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        schema::create_new_schema(info, schema_id, txn, new_snapshot_id).await?;
                        Ok(schema_id)
                    })
                })
                .await,
        )
    }

    /// Drops a schema and publishes the change in a new snapshot.
    pub async fn drop_schema(&self, info: schema::DropSchemaInfo) -> Result<(), DbErr> {
        flatten_transaction_err(
            self.db
                .transaction::<_, _, DbErr>(|txn| {
                    Box::pin(async move {
                        let current_snapshot = snapshot::get_current_snapshot_info(txn)
                            .await?
                            .unwrap_or_default();
                        let read_snapshot = current_snapshot.snapshot_id;
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        schema::drop_schema(info, txn, read_snapshot, new_snapshot_id).await
                    })
                })
                .await,
        )
    }

    /// Creates a relation and its columns and publishes them in a new snapshot.
    pub async fn create_relation(&self, info: relation::CreateRelationInfo) -> Result<i64, DbErr> {
        flatten_transaction_err(
            self.db
                .transaction::<_, _, DbErr>(|txn| {
                    Box::pin(async move {
                        let mut current_snapshot = snapshot::get_current_snapshot_info(txn)
                            .await?
                            .unwrap_or_default();
                        let read_snapshot = current_snapshot.snapshot_id;
                        let relation_id = current_snapshot.get_next_catalog_id();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        relation::create_relation(
                            info,
                            relation_id,
                            txn,
                            read_snapshot,
                            new_snapshot_id,
                        )
                        .await
                    })
                })
                .await,
        )
    }

    /// Drops a relation and its columns and publishes the change in a new snapshot.
    pub async fn drop_relation(&self, info: relation::DropRelationInfo) -> Result<i64, DbErr> {
        let relation_id = info.relation_id;
        flatten_transaction_err(
            self.db
                .transaction::<_, _, DbErr>(|txn| {
                    Box::pin(async move {
                        let current_snapshot = snapshot::get_current_snapshot_info(txn)
                            .await?
                            .unwrap_or_default();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        relation::drop_relation(info, txn, new_snapshot_id).await?;
                        Ok(relation_id)
                    })
                })
                .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use sea_orm::{Database, DbErr};
    use sea_orm_migration::MigratorTrait;

    use crate::{
        entity::column::ColumnType,
        migration::Migrator,
        relation::{CreateColumnInfo, CreateRelationInfo, GetRelationInfo},
        schema::CreateSchemaInfo,
        snapshot::get_current_snapshot_info,
    };

    use super::Catalog;

    #[tokio::test]
    async fn historical_reader_does_not_see_later_relation() -> Result<(), DbErr> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        let catalog = Catalog::new(db.clone());

        let schema_id = catalog
            .create_schema(CreateSchemaInfo {
                schema_name: "public".to_owned(),
            })
            .await?;
        let schema_snapshot = get_current_snapshot_info(&db)
            .await?
            .expect("schema commit should publish a snapshot");

        let relation_id = catalog
            .create_relation(CreateRelationInfo {
                schema_name: "public".to_owned(),
                relation_name: "orders".to_owned(),
                columns: vec![CreateColumnInfo {
                    column_name: "order_id".to_owned(),
                    column_type: ColumnType(DataType::Int64),
                    nulls_allowed: false,
                }],
            })
            .await?;

        let historical = catalog.reader_at(schema_snapshot.snapshot_id).await?;
        assert_eq!(historical.get_all_relations().await?, Vec::new());

        let current = catalog.reader().await?;
        let relation = current
            .get_relation(GetRelationInfo { relation_id })
            .await?;
        assert_eq!(relation.schema_id, schema_id);
        assert_eq!(relation.relation_name, "orders");
        assert_eq!(relation.columns.len(), 1);
        assert_eq!(relation.columns[0].column_name, "order_id");

        Ok(())
    }
}
