use arrow_schema::DataType;
use optd_catalog::{
    Catalog, CatalogError, SnapshotId,
    advanced_statistics::{AdvancedStatisticsContents, CreateAdvancedStatisticsInfo},
    entity::column::ColumnType,
    migration::{Migrator, MigratorTrait},
    relation::{BaseTableStorage, CreateBaseTableInfo, CreateColumnInfo, DropRelationInfo},
    schema::CreateSchemaInfo,
    snapshot::SnapshotChange,
    statistics::{
        InvalidateStatisticsInfo, SetRelationStatisticsInfo, StatisticsInvalidationCause,
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection};

async fn head(catalog: &Catalog<DatabaseConnection>) -> SnapshotId {
    catalog.reader().await.unwrap().snapshot_id()
}
async fn setup() -> Catalog<DatabaseConnection> {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let catalog = Catalog::new(db);
    catalog
        .create_schema(
            head(&catalog).await,
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap();
    catalog
}
async fn table(catalog: &Catalog<DatabaseConnection>, name: &str) -> (i64, i64) {
    let relation_id = catalog
        .create_base_table(
            head(catalog).await,
            CreateBaseTableInfo {
                schema_name: "public".into(),
                relation_name: name.into(),
                storage: BaseTableStorage::Relational,
                columns: ["x", "y"]
                    .into_iter()
                    .map(|name| CreateColumnInfo {
                        column_name: name.into(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: true,
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
    catalog
        .set_relation_statistics(
            head(catalog).await,
            SetRelationStatisticsInfo {
                relation_id,
                row_count: Some(10),
                size_bytes: None,
                column_statistics: vec![],
            },
        )
        .await
        .unwrap();
    let advanced_id = catalog
        .create_advanced_statistics(
            head(catalog).await,
            CreateAdvancedStatisticsInfo {
                relation_id,
                statistics_name: "xy".into(),
                column_ids: vec![0, 1],
                contents: AdvancedStatisticsContents::JointNdv { distinct_count: 10 },
            },
        )
        .await
        .unwrap();
    (relation_id, advanced_id)
}

#[tokio::test]
async fn batch_invalidation_is_atomic_canonical_and_nonstructural() {
    let catalog = setup().await;
    let (a, a_stats) = table(&catalog, "a").await;
    let (b, b_stats) = table(&catalog, "b").await;
    let before = catalog.reader().await.unwrap();
    let descriptor = before.snapshot_info().clone();
    let end = catalog
        .invalidate_statistics(
            before.snapshot_id(),
            InvalidateStatisticsInfo {
                relation_ids: vec![b, a, a],
                cause: StatisticsInvalidationCause::ObservedSql,
            },
        )
        .await
        .unwrap();
    let current = catalog.reader().await.unwrap();
    assert_eq!(end.as_i64(), descriptor.snapshot_id.as_i64() + 1);
    assert_eq!(current.schema_version(), descriptor.schema_version);
    assert_eq!(
        current.snapshot_info().next_catalog_id,
        descriptor.next_catalog_id
    );
    assert_eq!(
        current.snapshot_change().await.unwrap(),
        SnapshotChange::InvalidateStatisticsBatch {
            relation_ids: vec![a, b],
            cause: StatisticsInvalidationCause::ObservedSql,
        }
    );
    for (relation, advanced) in [(a, a_stats), (b, b_stats)] {
        assert!(
            current
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            current
                .get_relation_advanced_statistics(relation)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            before
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .unwrap()
                .end_snapshot,
            Some(end)
        );
        assert_eq!(
            before
                .get_advanced_statistics(advanced)
                .await
                .unwrap()
                .unwrap()
                .end_snapshot,
            Some(end)
        );
    }
}

#[tokio::test]
async fn invalid_batch_or_summary_failure_never_leaks_partial_closures() {
    let catalog = setup().await;
    let (a, advanced) = table(&catalog, "a").await;
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let base = catalog
        .reader()
        .await
        .unwrap()
        .get_relation_statistics(a)
        .await
        .unwrap();
    for ids in [vec![], vec![a, 9999]] {
        assert!(
            catalog
                .invalidate_statistics(
                    before.snapshot_id,
                    InvalidateStatisticsInfo {
                        relation_ids: ids,
                        cause: StatisticsInvalidationCause::Explicit,
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_relation_statistics(a)
                .await
                .unwrap(),
            base
        );
    }
    catalog.connection().execute_unprepared("CREATE TRIGGER fail_batch BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected batch failure'); END").await.unwrap();
    assert!(
        catalog
            .invalidate_statistics(
                before.snapshot_id,
                InvalidateStatisticsInfo {
                    relation_ids: vec![a],
                    cause: StatisticsInvalidationCause::Explicit,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(a)
            .await
            .unwrap(),
        base
    );
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(advanced)
            .await
            .unwrap()
            .is_some()
    );
    catalog
        .connection()
        .execute_unprepared("DROP TRIGGER fail_batch")
        .await
        .unwrap();
    catalog
        .invalidate_statistics(
            before.snapshot_id,
            InvalidateStatisticsInfo {
                relation_ids: vec![a],
                cause: StatisticsInvalidationCause::Explicit,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn foreign_batch_rejected_and_drop_closes_basic_and_advanced_at_one_snapshot() {
    let catalog = setup().await;
    let (relation_id, advanced_id) = table(&catalog, "a").await;
    // A nonexistent namespace cannot use globally allocated IDs to mutate this table.
    let foreign = Catalog::on_catalog(catalog.connection().clone(), 9999);
    let snapshot = head(&catalog).await;
    assert!(matches!(
        foreign
            .invalidate_statistics(
                snapshot,
                InvalidateStatisticsInfo {
                    relation_ids: vec![relation_id],
                    cause: StatisticsInvalidationCause::Explicit,
                }
            )
            .await,
        Err(CatalogError::CatalogNotFound { .. })
    ));
    assert_eq!(head(&catalog).await, snapshot);
    catalog
        .drop_relation(snapshot, DropRelationInfo { relation_id })
        .await
        .unwrap();
    let end = head(&catalog).await;
    let historical = catalog.reader_at(snapshot).await.unwrap();
    assert_eq!(
        historical
            .get_relation_statistics(relation_id)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(end)
    );
    assert_eq!(
        historical
            .get_advanced_statistics(advanced_id)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(end)
    );
}

#[tokio::test]
async fn mixed_catalog_batch_validates_every_target_before_closing_anything() {
    let catalog = setup().await;
    let (local, _) = table(&catalog, "local").await;
    let foreign_id = catalog
        .register_catalog(
            head(&catalog).await,
            optd_catalog::source::RegisterCatalogInfo {
                catalog_name: "other".into(),
                catalog_kind: "datafusion".into(),
                external_catalog_name: None,
                adapter_name: "datafusion".into(),
                connection_profile_reference: "runtime://other".into(),
            },
        )
        .await
        .unwrap();
    let other = Catalog::on_catalog(catalog.connection().clone(), foreign_id);
    other
        .create_schema(
            head(&other).await,
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap();
    let (foreign, _) = table(&other, "foreign").await;
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    assert!(
        catalog
            .invalidate_statistics(
                before.snapshot_id,
                InvalidateStatisticsInfo {
                    relation_ids: vec![local, foreign],
                    cause: StatisticsInvalidationCause::ObservedSql,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(local)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        other
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(foreign)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn duplicate_column_statistics_do_not_replace_the_previous_payload() {
    use optd_catalog::statistics::ColumnStatistics;
    let catalog = setup().await;
    let (relation_id, _) = table(&catalog, "a").await;
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let original = catalog
        .reader()
        .await
        .unwrap()
        .get_relation_statistics(relation_id)
        .await
        .unwrap();
    let column = ColumnStatistics {
        column_id: 0,
        frequency: Some(10),
        distinct_count: Some(5),
        lower_bound: None,
        upper_bound: None,
    };
    let conflicting = ColumnStatistics {
        distinct_count: Some(9),
        ..column.clone()
    };
    assert!(
        catalog
            .set_relation_statistics(
                before.snapshot_id,
                SetRelationStatisticsInfo {
                    relation_id,
                    row_count: Some(99),
                    size_bytes: None,
                    column_statistics: vec![column, conflicting],
                }
            )
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation_id)
            .await
            .unwrap(),
        original
    );
}

#[tokio::test]
async fn duplicate_persisted_column_statistics_fail_closed_on_read() {
    use optd_catalog::statistics::ColumnStatistics;
    let catalog = setup().await;
    let (relation_id, _) = table(&catalog, "a").await;
    catalog
        .set_relation_statistics(
            head(&catalog).await,
            SetRelationStatisticsInfo {
                relation_id,
                row_count: Some(10),
                size_bytes: None,
                column_statistics: vec![ColumnStatistics {
                    column_id: 0,
                    frequency: Some(10),
                    distinct_count: Some(5),
                    lower_bound: None,
                    upper_bound: None,
                }],
            },
        )
        .await
        .unwrap();
    catalog.connection().execute_unprepared("INSERT INTO optd_column_statistics (statistics_id, statistics_begin_snapshot, payload_begin_snapshot, column_id, frequency, distinct_count, lower_bound, upper_bound) SELECT statistics_id, statistics_begin_snapshot, payload_begin_snapshot, column_id, frequency, distinct_count, lower_bound, upper_bound FROM optd_column_statistics").await.unwrap();
    assert!(matches!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation_id)
            .await,
        Err(CatalogError::DuplicateColumnStatistics { column_id: 0, .. })
    ));
}

#[tokio::test]
async fn nonstructural_refresh_does_not_need_an_available_generation_or_object_id() {
    let catalog = setup().await;
    let (relation_id, _) = table(&catalog, "a").await;
    let snapshot = head(&catalog).await;
    catalog
        .connection()
        .execute_unprepared(&format!(
            "UPDATE optd_snapshot SET schema_version={}, next_catalog_id={} WHERE snapshot_id={}",
            i64::MAX,
            i64::MAX,
            snapshot.as_i64()
        ))
        .await
        .unwrap();
    catalog
        .set_relation_statistics(
            snapshot,
            SetRelationStatisticsInfo {
                relation_id,
                row_count: Some(20),
                size_bytes: None,
                column_statistics: vec![],
            },
        )
        .await
        .unwrap();
    let reader = catalog.reader().await.unwrap();
    assert_eq!(reader.schema_version(), i64::MAX);
    assert_eq!(reader.snapshot_info().next_catalog_id, i64::MAX);
    assert_eq!(
        reader
            .get_relation_statistics(relation_id)
            .await
            .unwrap()
            .unwrap()
            .row_count,
        Some(20)
    );
}

#[tokio::test]
async fn exhausted_catalog_counters_fail_without_publication_or_partial_objects() {
    use sea_orm::Statement;
    for field in ["next_catalog_id", "schema_version", "snapshot_id"] {
        let catalog = setup().await;
        let current = head(&catalog).await.as_i64();
        catalog
            .connection()
            .execute_unprepared(&format!(
                "UPDATE optd_snapshot SET {field} = {} WHERE snapshot_id = {current}",
                i64::MAX
            ))
            .await
            .unwrap();
        let before = catalog.reader().await.unwrap().snapshot_info().clone();
        let schemas = catalog
            .reader()
            .await
            .unwrap()
            .get_all_schemas()
            .await
            .unwrap();
        let count_sql = "SELECT COUNT(*) AS n FROM optd_snapshot_changes";
        let count_before: i64 = catalog
            .connection()
            .query_one_raw(Statement::from_string(
                catalog.connection().get_database_backend(),
                count_sql,
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert!(
            catalog
                .create_schema(
                    before.snapshot_id,
                    CreateSchemaInfo {
                        schema_name: "overflow".into()
                    }
                )
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
        assert_eq!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_all_schemas()
                .await
                .unwrap(),
            schemas
        );
        let count_after: i64 = catalog
            .connection()
            .query_one_raw(Statement::from_string(
                catalog.connection().get_database_backend(),
                count_sql,
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(count_before, count_after);
    }
}

/// Opt-in disposable PostgreSQL, isolated in a per-run schema.
#[tokio::test]
async fn postgres_batch_invalidation_rolls_back_all_relations_on_summary_failure() {
    let Ok(url) = std::env::var("OPTD_TEST_POSTGRES_URL") else {
        return;
    };
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!(
        "optd_batch_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(&schema).max_connections(1);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let catalog = Catalog::new(db.clone());
    catalog
        .create_schema(
            head(&catalog).await,
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap();
    let (a, a_stats) = table(&catalog, "a").await;
    let (b, b_stats) = table(&catalog, "b").await;
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    db.execute_unprepared("CREATE FUNCTION fail_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected batch failure'; END $$; CREATE TRIGGER fail_batch BEFORE INSERT ON optd_snapshot_changes FOR EACH ROW EXECUTE FUNCTION fail_batch()").await.unwrap();
    assert!(
        catalog
            .invalidate_statistics(
                before.snapshot_id,
                InvalidateStatisticsInfo {
                    relation_ids: vec![a, b],
                    cause: StatisticsInvalidationCause::ObservedSql,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    for (relation, advanced) in [(a, a_stats), (b, b_stats)] {
        assert!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_advanced_statistics(advanced)
                .await
                .unwrap()
                .is_some()
        );
    }
    db.execute_unprepared(
        "DROP TRIGGER fail_batch ON optd_snapshot_changes; DROP FUNCTION fail_batch()",
    )
    .await
    .unwrap();
    let end = catalog
        .invalidate_statistics(
            before.snapshot_id,
            InvalidateStatisticsInfo {
                relation_ids: vec![b, a, a],
                cause: StatisticsInvalidationCause::ObservedSql,
            },
        )
        .await
        .unwrap();
    let reader = catalog.reader().await.unwrap();
    assert_eq!(reader.schema_version(), before.schema_version);
    assert_eq!(
        reader.snapshot_info().next_catalog_id,
        before.next_catalog_id
    );
    assert_eq!(end.as_i64(), before.snapshot_id.as_i64() + 1);
    for (relation, advanced) in [(a, a_stats), (b, b_stats)] {
        assert!(
            reader
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            reader
                .get_advanced_statistics(advanced)
                .await
                .unwrap()
                .is_none()
        );
    }
    drop(catalog);
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
}

#[test]
fn old_single_relation_summaries_remain_decodable() {
    let old: SnapshotChange =
        serde_json::from_str(r#"{"kind":"invalidate_statistics","relation_id":42}"#).unwrap();
    assert_eq!(
        old,
        SnapshotChange::InvalidateStatistics { relation_id: 42 }
    );
}
