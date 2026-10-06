//! Snapshot-aware catalog API.

use sea_orm::{ConnectionTrait, TransactionTrait};

use crate::{CatalogError, CatalogResult, relation, schema, snapshot, statistics};

/// Default catalog branch.
pub const DEFAULT_BRANCH_NAME: &str = "main";

/// Catalog backed by a SeaORM connection or transaction manager.
pub struct Catalog<T> {
    db: T,
    branch_name: String,
}

/// Read-only catalog handle pinned to one immutable snapshot.
pub struct CatalogReader<'a, T> {
    db: &'a T,
    snapshot: snapshot::SnapshotInfo,
}

fn flatten_transaction_err<T>(
    result: Result<T, sea_orm::TransactionError<CatalogError>>,
) -> CatalogResult<T> {
    result.map_err(|error| match error {
        sea_orm::TransactionError::Connection(error) => error.into(),
        sea_orm::TransactionError::Transaction(error) => error,
    })
}

impl<T> Catalog<T> {
    /// Creates a catalog backed by `db` on the `main` branch.
    pub fn new(db: T) -> Self {
        Self::on_branch(db, DEFAULT_BRANCH_NAME)
    }

    /// Creates a catalog backed by `db` on `branch_name`.
    pub fn on_branch(db: T, branch_name: impl Into<String>) -> Self {
        Self {
            db,
            branch_name: branch_name.into(),
        }
    }

    /// Returns the underlying database handle.
    pub fn connection(&self) -> &T {
        &self.db
    }

    /// Returns the branch used by current reads and writes.
    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }
}

impl<T: ConnectionTrait> CatalogReader<'_, T> {
    /// Returns the snapshot used by every read from this handle.
    pub fn snapshot_id(&self) -> snapshot::SnapshotId {
        self.snapshot.snapshot_id
    }

    /// Returns all schemas visible at the pinned snapshot.
    pub async fn get_all_schemas(&self) -> CatalogResult<Vec<schema::SchemaInfo>> {
        Ok(schema::get_all_schema_infos(self.db, &self.snapshot).await?)
    }

    /// Returns one schema visible at the pinned snapshot.
    pub async fn get_schema(
        &self,
        info: schema::GetSchemaInfo,
    ) -> CatalogResult<schema::SchemaInfo> {
        let schema_id = info.schema_id;
        schema::get_schema(info, self.db, &self.snapshot)
            .await?
            .ok_or(CatalogError::SchemaNotFound { schema_id })
    }

    /// Returns all relations visible at the pinned snapshot.
    pub async fn get_all_relations(&self) -> CatalogResult<Vec<relation::RelationInfo>> {
        Ok(relation::get_all_relation_infos(self.db, &self.snapshot).await?)
    }

    /// Returns one relation visible at the pinned snapshot.
    pub async fn get_relation(
        &self,
        info: relation::GetRelationInfo,
    ) -> CatalogResult<relation::RelationInfo> {
        let relation_id = info.relation_id;
        relation::get_relation(info, self.db, &self.snapshot)
            .await?
            .ok_or(CatalogError::RelationNotFound { relation_id })
    }

    /// Returns the latest statistics payload visible for `relation_id`.
    pub async fn get_relation_statistics(
        &self,
        relation_id: i64,
    ) -> CatalogResult<Option<statistics::RelationStatistics>> {
        statistics::get_relation_statistics(self.db, &self.snapshot, relation_id).await
    }
}

impl<T: ConnectionTrait> Catalog<T> {
    /// Creates a reader pinned to `snapshot_id`.
    pub async fn reader_at(
        &self,
        snapshot_id: snapshot::SnapshotId,
    ) -> CatalogResult<CatalogReader<'_, T>> {
        let snapshot = snapshot::get_snapshot_info(&self.db, snapshot_id)
            .await?
            .ok_or(CatalogError::SnapshotNotFound { snapshot_id })?;
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
        })
    }

    /// Creates a reader pinned to the branch's published head snapshot.
    pub async fn reader(&self) -> CatalogResult<CatalogReader<'_, T>> {
        let snapshot = snapshot::get_current_snapshot_info(&self.db, &self.branch_name)
            .await?
            .ok_or_else(|| CatalogError::BranchNotFound {
                branch_name: self.branch_name.clone(),
            })?;
        Ok(CatalogReader {
            db: &self.db,
            snapshot,
        })
    }
}

impl<T: TransactionTrait> Catalog<T> {
    /// Creates a schema from `expected_snapshot_id` and publishes a new branch head.
    pub async fn create_schema(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        info: schema::CreateSchemaInfo,
    ) -> CatalogResult<i64> {
        let branch_name = self.branch_name.clone();
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let mut current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let schema_id = current_snapshot.get_next_catalog_id();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        schema::create_new_schema(info, schema_id, txn, new_snapshot_id.as_i64())
                            .await?;
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await?;
                        Ok(schema_id)
                    })
                })
                .await,
        )
    }

    /// Drops a schema from `expected_snapshot_id` and publishes a new branch head.
    pub async fn drop_schema(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        info: schema::DropSchemaInfo,
    ) -> CatalogResult<()> {
        let branch_name = self.branch_name.clone();
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let read_snapshot = current_snapshot.snapshot_id.as_i64();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        schema::drop_schema(info, txn, read_snapshot, new_snapshot_id.as_i64())
                            .await?;
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await
                    })
                })
                .await,
        )
    }

    /// Creates a base table from `expected_snapshot_id` and publishes a new branch head.
    pub async fn create_base_table(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        info: relation::CreateBaseTableInfo,
    ) -> CatalogResult<i64> {
        let branch_name = self.branch_name.clone();
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let mut current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let read_snapshot = current_snapshot.snapshot_id.as_i64();
                        let relation_id = current_snapshot.get_next_catalog_id();
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        relation::create_relation(
                            info.into(),
                            relation_id,
                            txn,
                            read_snapshot,
                            new_snapshot_id.as_i64(),
                        )
                        .await?;
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await?;
                        Ok(relation_id)
                    })
                })
                .await,
        )
    }

    /// Publishes collected relation statistics as a new catalog snapshot.
    pub async fn set_relation_statistics(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        info: statistics::SetRelationStatisticsInfo,
    ) -> CatalogResult<snapshot::SnapshotId> {
        let branch_name = self.branch_name.clone();
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let mut current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let read_snapshot = current_snapshot.clone();
                        let definition = statistics::get_statistics_definition(
                            txn,
                            &read_snapshot,
                            info.relation_id,
                        )
                        .await?;
                        let (statistics_id, definition_begin_snapshot, create_definition) =
                            match definition {
                                Some(definition) => {
                                    (definition.statistics_id, definition.begin_snapshot, false)
                                }
                                None => (current_snapshot.get_next_catalog_id(), -1, true),
                            };
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        let definition_begin_snapshot = if create_definition {
                            new_snapshot_id.as_i64()
                        } else {
                            definition_begin_snapshot
                        };
                        statistics::insert_relation_statistics(
                            txn,
                            &read_snapshot,
                            new_snapshot_id.as_i64(),
                            statistics_id,
                            definition_begin_snapshot,
                            create_definition,
                            info,
                        )
                        .await?;
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await?;
                        Ok(new_snapshot_id)
                    })
                })
                .await,
        )
    }

    /// Publishes a data-change snapshot and invalidates visible statistics for a relation.
    ///
    /// Future DML implementations should perform their storage mutation and call the underlying
    /// invalidation helper in the same transaction rather than publishing a second snapshot.
    pub async fn invalidate_relation_statistics(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        relation_id: i64,
    ) -> CatalogResult<snapshot::SnapshotId> {
        let branch_name = self.branch_name.clone();
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let read_snapshot = current_snapshot.clone();
                        relation::get_relation(
                            relation::GetRelationInfo { relation_id },
                            txn,
                            &read_snapshot,
                        )
                        .await?
                        .ok_or(CatalogError::RelationNotFound { relation_id })?;
                        let definition =
                            statistics::get_statistics_definition(txn, &read_snapshot, relation_id)
                                .await?;
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        if let Some(definition) = definition {
                            statistics::close_active_statistics_payload(
                                txn,
                                definition.statistics_id,
                                definition.begin_snapshot,
                                new_snapshot_id.as_i64(),
                            )
                            .await?;
                        }
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await?;
                        Ok(new_snapshot_id)
                    })
                })
                .await,
        )
    }

    /// Drops a relation from `expected_snapshot_id` and publishes a new branch head.
    pub async fn drop_relation(
        &self,
        expected_snapshot_id: snapshot::SnapshotId,
        info: relation::DropRelationInfo,
    ) -> CatalogResult<i64> {
        let branch_name = self.branch_name.clone();
        let relation_id = info.relation_id;
        flatten_transaction_err(
            self.db
                .transaction::<_, _, CatalogError>(|txn| {
                    Box::pin(async move {
                        let current_snapshot = snapshot::get_snapshot_for_write(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                        )
                        .await?;
                        let new_snapshot_id =
                            snapshot::commit_snapshot(txn, current_snapshot).await?;
                        relation::drop_relation(info, txn, new_snapshot_id.as_i64()).await?;
                        snapshot::publish_snapshot(
                            txn,
                            &branch_name,
                            expected_snapshot_id,
                            new_snapshot_id,
                        )
                        .await?;
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
    use sea_orm::Database;
    use sea_orm_migration::MigratorTrait;

    use crate::{
        CatalogError, CatalogResult,
        entity::column::ColumnType,
        migration::Migrator,
        relation::{
            BaseTableStorage, CreateBaseTableInfo, CreateColumnInfo, DropRelationInfo,
            GetRelationInfo, RelationKind, StorageKind,
        },
        schema::CreateSchemaInfo,
        snapshot::SnapshotId,
        statistics::{
            ColumnStatistics, STATISTICS_VALUE_ENCODING_VERSION, SetRelationStatisticsInfo,
            StatisticsValue,
        },
    };

    use super::Catalog;

    #[tokio::test]
    async fn historical_reader_does_not_see_later_relation() -> CatalogResult<()> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        let catalog = Catalog::new(db.clone());
        let initial_snapshot_id = catalog.reader().await?.snapshot_id();
        assert_eq!(initial_snapshot_id, SnapshotId::INITIAL);

        let schema_id = catalog
            .create_schema(
                initial_snapshot_id,
                CreateSchemaInfo {
                    schema_name: "public".to_owned(),
                },
            )
            .await?;
        let schema_snapshot_id = catalog.reader().await?.snapshot_id();

        let relation_id = catalog
            .create_base_table(
                schema_snapshot_id,
                CreateBaseTableInfo {
                    schema_name: "public".to_owned(),
                    relation_name: "orders".to_owned(),
                    storage: BaseTableStorage::Relational,
                    columns: vec![CreateColumnInfo {
                        column_name: "order_id".to_owned(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: false,
                    }],
                },
            )
            .await?;

        let historical = catalog.reader_at(schema_snapshot_id).await?;
        assert_eq!(historical.get_all_relations().await?, Vec::new());

        let current = catalog.reader().await?;
        let relation = current
            .get_relation(GetRelationInfo { relation_id })
            .await?;
        assert_eq!(relation.schema_id, schema_id);
        assert_eq!(relation.relation_name, "orders");
        assert_eq!(relation.relation_kind, RelationKind::Table);
        assert_eq!(relation.storage_kind, StorageKind::Relational);
        assert_eq!(relation.columns.len(), 1);
        assert_eq!(relation.columns[0].column_name, "order_id");

        let relation_snapshot_id = current.snapshot_id();
        catalog
            .drop_relation(relation_snapshot_id, DropRelationInfo { relation_id })
            .await?;

        assert_eq!(
            catalog.reader().await?.get_all_relations().await?,
            Vec::new()
        );
        let historical = catalog.reader_at(relation_snapshot_id).await?;
        let relation = historical
            .get_relation(GetRelationInfo { relation_id })
            .await?;
        assert_eq!(relation.relation_name, "orders");
        assert_eq!(relation.columns.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn statistics_payloads_advance_snapshots_and_preserve_history() -> CatalogResult<()> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        let catalog = Catalog::new(db);
        let schema_id = catalog
            .create_schema(
                SnapshotId::INITIAL,
                CreateSchemaInfo {
                    schema_name: "public".to_owned(),
                },
            )
            .await?;
        let schema_snapshot_id = catalog.reader().await?.snapshot_id();
        let relation_id = catalog
            .create_base_table(
                schema_snapshot_id,
                CreateBaseTableInfo {
                    schema_name: "public".to_owned(),
                    relation_name: "orders".to_owned(),
                    storage: BaseTableStorage::Relational,
                    columns: vec![CreateColumnInfo {
                        column_name: "order_id".to_owned(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: false,
                    }],
                },
            )
            .await?;
        let relation_snapshot_id = catalog.reader().await?.snapshot_id();
        let relation = catalog
            .reader()
            .await?
            .get_relation(GetRelationInfo { relation_id })
            .await?;
        assert_eq!(relation.schema_id, schema_id);
        let column_id = relation.columns[0].column_id;
        assert!(
            catalog
                .reader()
                .await?
                .get_relation_statistics(relation_id)
                .await?
                .is_none()
        );

        let first_statistics_snapshot = catalog
            .set_relation_statistics(
                relation_snapshot_id,
                SetRelationStatisticsInfo {
                    relation_id,
                    row_count: Some(10),
                    size_bytes: None,
                    column_statistics: vec![ColumnStatistics {
                        column_id,
                        frequency: Some(10),
                        distinct_count: Some(5),
                        lower_bound: Some(StatisticsValue::Int64(1)),
                        upper_bound: Some(StatisticsValue::Int64(9)),
                    }],
                },
            )
            .await?;
        assert!(
            catalog
                .reader_at(relation_snapshot_id)
                .await?
                .get_relation_statistics(relation_id)
                .await?
                .is_none()
        );
        let first = catalog
            .reader_at(first_statistics_snapshot)
            .await?
            .get_relation_statistics(relation_id)
            .await?
            .unwrap();
        assert_eq!(first.row_count, Some(10));
        assert_eq!(
            first.value_encoding_version,
            STATISTICS_VALUE_ENCODING_VERSION
        );
        assert_eq!(first.column_statistics[0].distinct_count, Some(5));

        let second_statistics_snapshot = catalog
            .set_relation_statistics(
                first_statistics_snapshot,
                SetRelationStatisticsInfo {
                    relation_id,
                    row_count: Some(20),
                    size_bytes: None,
                    column_statistics: vec![ColumnStatistics {
                        column_id,
                        frequency: Some(20),
                        distinct_count: Some(8),
                        lower_bound: Some(StatisticsValue::Int64(1)),
                        upper_bound: Some(StatisticsValue::Int64(20)),
                    }],
                },
            )
            .await?;
        let second = catalog
            .reader_at(second_statistics_snapshot)
            .await?
            .get_relation_statistics(relation_id)
            .await?
            .unwrap();
        assert_eq!(second.statistics_id, first.statistics_id);
        assert_eq!(second.begin_snapshot, second_statistics_snapshot);
        assert_eq!(second.end_snapshot, None);
        assert_eq!(second.row_count, Some(20));
        let historical_first = catalog
            .reader_at(first_statistics_snapshot)
            .await?
            .get_relation_statistics(relation_id)
            .await?
            .unwrap();
        assert_eq!(historical_first.row_count, Some(10));
        assert_eq!(
            historical_first.end_snapshot,
            Some(second_statistics_snapshot)
        );

        let invalidation_snapshot = catalog
            .invalidate_relation_statistics(second_statistics_snapshot, relation_id)
            .await?;
        assert!(
            catalog
                .reader_at(invalidation_snapshot)
                .await?
                .get_relation_statistics(relation_id)
                .await?
                .is_none()
        );
        let historical_second = catalog
            .reader_at(second_statistics_snapshot)
            .await?
            .get_relation_statistics(relation_id)
            .await?
            .unwrap();
        assert_eq!(historical_second.row_count, Some(20));
        assert_eq!(historical_second.end_snapshot, Some(invalidation_snapshot));

        catalog
            .drop_relation(invalidation_snapshot, DropRelationInfo { relation_id })
            .await?;
        assert!(
            catalog
                .reader()
                .await?
                .get_relation_statistics(relation_id)
                .await?
                .is_none()
        );
        assert!(
            catalog
                .reader_at(second_statistics_snapshot)
                .await?
                .get_relation_statistics(relation_id)
                .await?
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn failed_write_does_not_publish_snapshot() -> CatalogResult<()> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        let catalog = Catalog::new(db);
        let initial_snapshot_id = catalog.reader().await?.snapshot_id();

        let error = catalog
            .create_base_table(
                initial_snapshot_id,
                CreateBaseTableInfo {
                    schema_name: "missing".to_owned(),
                    relation_name: "orders".to_owned(),
                    storage: BaseTableStorage::Files,
                    columns: Vec::new(),
                },
            )
            .await
            .expect_err("a relation cannot be created in a missing schema");
        assert!(matches!(
            error,
            CatalogError::SchemaNameNotFound { schema_name } if schema_name == "missing"
        ));

        assert_eq!(catalog.reader().await?.snapshot_id(), initial_snapshot_id);
        let missing_snapshot_id = SnapshotId::from_raw(initial_snapshot_id.as_i64() + 1);
        assert!(matches!(
            catalog.reader_at(missing_snapshot_id).await,
            Err(CatalogError::SnapshotNotFound { snapshot_id })
                if snapshot_id == missing_snapshot_id
        ));

        Ok(())
    }

    #[tokio::test]
    async fn stale_writer_cannot_advance_branch_head() -> CatalogResult<()> {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let path = temp_dir.path().join("catalog.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let db1 = Database::connect(&url).await?;
        Migrator::up(&db1, None).await?;
        let db2 = Database::connect(&url).await?;
        {
            let catalog1 = Catalog::new(db1.clone());
            let catalog2 = Catalog::new(db2.clone());

            let reader1 = catalog1.reader().await?;
            let reader2 = catalog2.reader().await?;
            let initial_snapshot_id = reader1.snapshot_id();
            assert_eq!(initial_snapshot_id, reader2.snapshot_id());

            catalog1
                .create_schema(
                    initial_snapshot_id,
                    CreateSchemaInfo {
                        schema_name: "public".to_owned(),
                    },
                )
                .await?;

            let error = catalog2
                .create_schema(
                    initial_snapshot_id,
                    CreateSchemaInfo {
                        schema_name: "stale".to_owned(),
                    },
                )
                .await
                .expect_err("the second client should have a stale base snapshot");
            assert!(matches!(
                error,
                CatalogError::StaleSnapshot {
                    expected,
                    actual,
                    ..
                } if expected == initial_snapshot_id && actual != expected
            ));

            assert_eq!(reader2.get_all_schemas().await?, Vec::new());
            let current = catalog2.reader().await?;
            assert_ne!(current.snapshot_id(), initial_snapshot_id);
            let schemas = current.get_all_schemas().await?;
            assert_eq!(schemas.len(), 1);
            assert_eq!(schemas[0].schema_name, "public");
        }

        db1.close().await?;
        db2.close().await?;

        Ok(())
    }
}
