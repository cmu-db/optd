use arrow_schema::DataType;
use sea_orm::Database;
use sea_orm_migration::MigratorTrait;

use crate::{
    CatalogError, CatalogResult,
    entity::column::ColumnType,
    index::{
        CreateIndexInfo, DropIndexInfo, GetIndexInfo, IndexField, IndexFieldRole, IndexStatus,
        NullOrdering, SortDirection,
    },
    migration::Migrator,
    relation::{
        BaseTableStorage, CreateBaseTableInfo, CreateColumnInfo, DropRelationInfo, GetRelationInfo,
        RelationKind, StorageKind,
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
async fn relational_indexes_are_temporal_and_files_reject_them() -> CatalogResult<()> {
    let db = Database::connect("sqlite::memory:").await?;
    Migrator::up(&db, None).await?;
    let catalog = Catalog::new(db);
    catalog
        .create_schema(
            SnapshotId::INITIAL,
            CreateSchemaInfo {
                schema_name: "public".to_owned(),
            },
        )
        .await?;
    let schema_snapshot = catalog.reader().await?.snapshot_id();
    let relation_id = catalog
        .create_base_table(
            schema_snapshot,
            CreateBaseTableInfo {
                schema_name: "public".to_owned(),
                relation_name: "orders".to_owned(),
                storage: BaseTableStorage::Relational,
                columns: vec![
                    CreateColumnInfo {
                        column_name: "customer_id".to_owned(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: false,
                    },
                    CreateColumnInfo {
                        column_name: "status".to_owned(),
                        column_type: ColumnType(DataType::Utf8),
                        nulls_allowed: true,
                    },
                ],
            },
        )
        .await?;
    let relation_snapshot = catalog.reader().await?.snapshot_id();
    let relation = catalog
        .reader()
        .await?
        .get_relation(GetRelationInfo { relation_id })
        .await?;
    let customer_id = relation.columns[0].column_id;
    let status = relation.columns[1].column_id;
    let fields = vec![
        IndexField {
            role: IndexFieldRole::Key,
            column_id: customer_id,
            direction: Some(SortDirection::Desc),
            null_order: Some(NullOrdering::Last),
            collation: None,
            operator_class: Some("int8_ops".to_owned()),
        },
        IndexField {
            role: IndexFieldRole::Include,
            column_id: status,
            direction: None,
            null_order: None,
            collation: None,
            operator_class: None,
        },
    ];
    let index_id = catalog
        .create_index(
            relation_snapshot,
            CreateIndexInfo {
                relation_id,
                index_name: "orders_customer_idx".to_owned(),
                access_method: "btree".to_owned(),
                is_unique: false,
                nulls_not_distinct: false,
                fields: fields.clone(),
                status: IndexStatus::Ready,
            },
        )
        .await?;
    assert!(
        catalog
            .reader_at(relation_snapshot)
            .await?
            .get_relation_indexes(relation_id)
            .await?
            .is_empty()
    );
    let index_snapshot = catalog.reader().await?.snapshot_id();
    let index = catalog
        .reader()
        .await?
        .get_index(GetIndexInfo { index_id })
        .await?;
    assert_eq!(index.access_method, "btree");
    assert_eq!(index.fields, fields);
    assert_eq!(index.status, IndexStatus::Ready);

    catalog
        .drop_index(index_snapshot, DropIndexInfo { index_id })
        .await?;
    assert!(
        catalog
            .reader()
            .await?
            .get_relation_indexes(relation_id)
            .await?
            .is_empty()
    );
    assert_eq!(
        catalog
            .reader_at(index_snapshot)
            .await?
            .get_index(GetIndexInfo { index_id })
            .await?
            .index_name,
        "orders_customer_idx"
    );

    let current_snapshot = catalog.reader().await?.snapshot_id();
    let files_relation_id = catalog
        .create_base_table(
            current_snapshot,
            CreateBaseTableInfo {
                schema_name: "public".to_owned(),
                relation_name: "events".to_owned(),
                storage: BaseTableStorage::Files,
                columns: vec![CreateColumnInfo {
                    column_name: "event_id".to_owned(),
                    column_type: ColumnType(DataType::Int64),
                    nulls_allowed: false,
                }],
            },
        )
        .await?;
    let files_snapshot = catalog.reader().await?.snapshot_id();
    let event_id = catalog
        .reader()
        .await?
        .get_relation(GetRelationInfo {
            relation_id: files_relation_id,
        })
        .await?
        .columns[0]
        .column_id;
    assert!(matches!(
        catalog
            .create_index(
                files_snapshot,
                CreateIndexInfo {
                    relation_id: files_relation_id,
                    index_name: "events_idx".to_owned(),
                    access_method: "btree".to_owned(),
                    is_unique: false,
                    nulls_not_distinct: false,
                    fields: vec![IndexField {
                        role: IndexFieldRole::Key,
                        column_id: event_id,
                        direction: None,
                        null_order: None,
                        collation: None,
                        operator_class: None,
                    }],
                    status: IndexStatus::Ready,
                },
            )
            .await,
        Err(CatalogError::IndexUnsupportedStorage { .. })
    ));
    assert_eq!(catalog.reader().await?.snapshot_id(), files_snapshot);

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
    assert!(second.column_ids.is_empty());
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

    // A failure after closing the previous version must roll its interval back too.
    assert!(
        catalog
            .set_relation_statistics(
                second_statistics_snapshot,
                SetRelationStatisticsInfo {
                    relation_id,
                    row_count: Some(u64::MAX),
                    size_bytes: None,
                    column_statistics: vec![],
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        catalog.reader().await?.snapshot_id(),
        second_statistics_snapshot
    );
    assert_eq!(
        catalog
            .reader()
            .await?
            .get_relation_statistics(relation_id)
            .await?
            .unwrap(),
        second
    );

    let invalidation_snapshot = catalog
        .invalidate_relation_statistics(second_statistics_snapshot, relation_id)
        .await?;
    for id in [
        relation_snapshot_id,
        first_statistics_snapshot,
        second_statistics_snapshot,
        invalidation_snapshot,
    ] {
        assert_eq!(catalog.reader_at(id).await?.schema_version(), 2);
    }
    assert_eq!(
        catalog
            .reader_at(first_statistics_snapshot)
            .await?
            .snapshot_change()
            .await?,
        crate::snapshot::SnapshotChange::RefreshStatistics { relation_id }
    );
    assert_eq!(
        catalog
            .reader_at(invalidation_snapshot)
            .await?
            .snapshot_change()
            .await?,
        crate::snapshot::SnapshotChange::InvalidateStatistics { relation_id }
    );
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

    let allocator = catalog.reader().await?.snapshot_info().next_catalog_id;
    let refreshed_snapshot = catalog
        .set_relation_statistics(
            invalidation_snapshot,
            SetRelationStatisticsInfo {
                relation_id,
                row_count: Some(30),
                size_bytes: None,
                column_statistics: vec![],
            },
        )
        .await?;
    let refreshed = catalog
        .reader()
        .await?
        .get_relation_statistics(relation_id)
        .await?
        .unwrap();
    assert_eq!(refreshed.statistics_id, first.statistics_id);
    assert_eq!(refreshed.row_count, Some(30));
    assert_eq!(
        catalog.reader().await?.snapshot_info().next_catalog_id,
        allocator
    );
    // Keep the established base table/column statistics layout unchanged.
    use sea_orm::ConnectionTrait;
    let db = catalog.connection();
    assert!(
        db.execute_unprepared(
            "SELECT statistics_id, begin_snapshot, end_snapshot FROM optd_statistics"
        )
        .await
        .is_ok()
    );
    assert!(
        db.execute_unprepared("SELECT statistics_id, statistics_begin_snapshot, payload_begin_snapshot FROM optd_column_statistics")
            .await
            .is_ok()
    );
    assert!(
        db.execute_unprepared("SELECT statistics_id, statistics_begin_snapshot, begin_snapshot, end_snapshot, row_count FROM optd_statistics_payload")
            .await
            .is_ok()
    );

    catalog
        .drop_relation(refreshed_snapshot, DropRelationInfo { relation_id })
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
async fn stale_writer_cannot_advance_global_history() -> CatalogResult<()> {
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
