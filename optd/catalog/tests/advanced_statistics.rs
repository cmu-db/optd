//! Advanced objects are additive: basic statistics retain their original layout/lifecycle.
use arrow_schema::DataType;
use optd_catalog::{
    Catalog, CatalogError, SnapshotId,
    advanced_statistics::{
        AdvancedStatisticsContents as Contents, CreateAdvancedStatisticsInfo as Create,
        DropAdvancedStatisticsInfo as Drop, HistogramBucket,
        UpdateAdvancedStatisticsInfo as Update,
    },
    entity::column::ColumnType,
    migration::{Migrator, MigratorTrait},
    relation::{BaseTableStorage, CreateBaseTableInfo, CreateColumnInfo, DropRelationInfo},
    schema::CreateSchemaInfo,
    snapshot::SnapshotChange,
    statistics::{SetRelationStatisticsInfo, StatisticsValue},
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};

async fn head(catalog: &Catalog<DatabaseConnection>) -> SnapshotId {
    catalog.reader().await.unwrap().snapshot_id()
}

async fn table(catalog: &Catalog<DatabaseConnection>) -> i64 {
    catalog
        .create_schema(
            head(catalog).await,
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap();
    catalog
        .create_base_table(
            head(catalog).await,
            CreateBaseTableInfo {
                schema_name: "public".into(),
                relation_name: "numbers".into(),
                storage: BaseTableStorage::Relational,
                columns: vec![
                    CreateColumnInfo {
                        column_name: "x".into(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: true,
                    },
                    CreateColumnInfo {
                        column_name: "y".into(),
                        column_type: ColumnType(DataType::Int64),
                        nulls_allowed: true,
                    },
                    CreateColumnInfo {
                        column_name: "d".into(),
                        column_type: ColumnType(DataType::Decimal128(38, 0)),
                        nulls_allowed: false,
                    },
                    CreateColumnInfo {
                        column_name: "f".into(),
                        column_type: ColumnType(DataType::Float64),
                        nulls_allowed: false,
                    },
                ],
            },
        )
        .await
        .unwrap()
}

async fn setup() -> (Catalog<DatabaseConnection>, i64) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let catalog = Catalog::new(db);
    let relation = table(&catalog).await;
    basic(&catalog, relation, 100).await;
    (catalog, relation)
}

async fn basic(catalog: &Catalog<DatabaseConnection>, relation_id: i64, rows: u64) {
    catalog
        .set_relation_statistics(
            head(catalog).await,
            SetRelationStatisticsInfo {
                relation_id,
                row_count: Some(rows),
                size_bytes: Some(rows * 8),
                column_statistics: vec![],
            },
        )
        .await
        .unwrap();
}

fn joint(relation_id: i64, name: &str) -> Create {
    Create {
        relation_id,
        statistics_name: name.into(),
        column_ids: vec![1, 0],
        contents: Contents::JointNdv { distinct_count: 10 },
    }
}

fn histogram(relation_id: i64, name: &str, column_id: i64, values: Vec<StatisticsValue>) -> Create {
    Create {
        relation_id,
        statistics_name: name.into(),
        column_ids: vec![column_id],
        contents: Contents::Histogram {
            buckets: values
                .into_iter()
                .map(|upper_bound| HistogramBucket {
                    upper_bound,
                    frequency: 5,
                })
                .collect(),
        },
    }
}

async fn count(db: &DatabaseConnection, table: &str) -> i64 {
    db.query_one_raw(Statement::from_string(
        db.get_database_backend(),
        format!("SELECT COUNT(*) AS n FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "n")
    .unwrap()
}

#[tokio::test]
async fn temporal_advanced_lifecycle_does_not_change_basic_statistics() {
    let (catalog, relation) = setup().await;
    let before = catalog.reader().await.unwrap();
    let base = before.get_relation_statistics(relation).await.unwrap();
    let schema_version = before.schema_version();
    let expected_id = before.snapshot_info().next_catalog_id;
    let before_snapshot = before.snapshot_id();
    let id = catalog
        .create_advanced_statistics(before_snapshot, joint(relation, "xy_ndv"))
        .await
        .unwrap();
    assert_eq!(id, expected_id);
    let created = head(&catalog).await;
    let old = catalog
        .reader_at(created)
        .await
        .unwrap()
        .get_advanced_statistics(id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.column_ids, vec![0, 1]);
    assert!(
        catalog
            .reader_at(before_snapshot)
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .snapshot_change()
            .await
            .unwrap(),
        SnapshotChange::CreateAdvancedStatistics {
            statistics_id: id,
            relation_id: relation
        }
    );
    let hist_id = catalog
        .create_advanced_statistics(
            head(&catalog).await,
            histogram(
                relation,
                "x_hist",
                0,
                vec![StatisticsValue::Int64(10), StatisticsValue::Int64(20)],
            ),
        )
        .await
        .unwrap();
    assert_ne!(hist_id, id);
    let allocator = catalog
        .reader()
        .await
        .unwrap()
        .snapshot_info()
        .next_catalog_id;
    let changed = catalog
        .update_advanced_statistics(
            head(&catalog).await,
            Update {
                statistics_id: id,
                contents: Contents::JointNdv { distinct_count: 20 },
            },
        )
        .await
        .unwrap();
    let current = catalog.reader().await.unwrap();
    assert_eq!(current.schema_version(), schema_version);
    assert_eq!(current.snapshot_info().next_catalog_id, allocator);
    assert_eq!(
        current.get_relation_statistics(relation).await.unwrap(),
        base
    );
    assert_eq!(
        current
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .unwrap()
            .contents,
        Contents::JointNdv { distinct_count: 20 }
    );
    assert_eq!(
        current.snapshot_change().await.unwrap(),
        SnapshotChange::UpdateAdvancedStatistics {
            statistics_id: id,
            relation_id: relation
        }
    );
    let historical = catalog
        .reader_at(created)
        .await
        .unwrap()
        .get_advanced_statistics(id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(historical.contents, old.contents);
    assert_eq!(historical.end_snapshot, Some(changed));
    let dropped = catalog
        .drop_advanced_statistics(head(&catalog).await, Drop { statistics_id: id })
        .await
        .unwrap();
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .snapshot_change()
            .await
            .unwrap(),
        SnapshotChange::DropAdvancedStatistics {
            statistics_id: id,
            relation_id: relation
        }
    );
    assert_eq!(
        catalog
            .reader_at(changed)
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(dropped)
    );
    assert!(matches!(
        catalog
            .update_advanced_statistics(
                dropped,
                Update {
                    statistics_id: id,
                    contents: Contents::JointNdv { distinct_count: 30 }
                }
            )
            .await,
        Err(CatalogError::AdvancedStatisticsNotFound { .. })
    ));
    assert_eq!(head(&catalog).await, dropped);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap(),
        base
    );
    catalog
        .drop_relation(
            dropped,
            DropRelationInfo {
                relation_id: relation,
            },
        )
        .await
        .unwrap();
    let end = head(&catalog).await;
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(hist_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        catalog
            .reader_at(dropped)
            .await
            .unwrap()
            .get_advanced_statistics(hist_id)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(end)
    );
}

#[tokio::test]
async fn relation_invalidation_closes_basic_and_all_advanced_objects() {
    let (catalog, relation) = setup().await;
    let base_id = catalog
        .reader()
        .await
        .unwrap()
        .get_relation_statistics(relation)
        .await
        .unwrap()
        .unwrap()
        .statistics_id;
    let first = catalog
        .create_advanced_statistics(head(&catalog).await, joint(relation, "xy_ndv"))
        .await
        .unwrap();
    let second = catalog
        .create_advanced_statistics(
            head(&catalog).await,
            histogram(relation, "x_hist", 0, vec![StatisticsValue::Int64(10)]),
        )
        .await
        .unwrap();
    // Basic recollection does not implicitly recollect or invalidate advanced objects.
    basic(&catalog, relation, 200).await;
    let before = head(&catalog).await;
    let invalidated = catalog
        .invalidate_relation_statistics(before, relation)
        .await
        .unwrap();
    let reader = catalog.reader().await.unwrap();
    assert!(
        reader
            .get_relation_statistics(relation)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        reader
            .get_relation_advanced_statistics(relation)
            .await
            .unwrap()
            .is_empty()
    );
    let past = catalog.reader_at(before).await.unwrap();
    assert_eq!(
        past.get_relation_statistics(relation)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(invalidated)
    );
    for id in [first, second] {
        assert_eq!(
            past.get_advanced_statistics(id)
                .await
                .unwrap()
                .unwrap()
                .end_snapshot,
            Some(invalidated)
        );
    }
    catalog
        .invalidate_relation_statistics(invalidated, relation)
        .await
        .unwrap();
    let head_before = head(&catalog).await;
    assert!(
        catalog
            .update_advanced_statistics(
                head_before,
                Update {
                    statistics_id: first,
                    contents: Contents::JointNdv { distinct_count: 40 }
                }
            )
            .await
            .is_err()
    );
    assert_eq!(head(&catalog).await, head_before);
    let replacement = catalog
        .create_advanced_statistics(head_before, joint(relation, "xy_ndv"))
        .await
        .unwrap();
    assert!(replacement > second);
    basic(&catalog, relation, 300).await;
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap()
            .unwrap()
            .statistics_id,
        base_id
    );
}

#[tokio::test]
async fn invalid_contents_and_stale_writes_do_not_publish_or_change_allocator() {
    let (catalog, relation) = setup().await;
    let id = catalog
        .create_advanced_statistics(head(&catalog).await, joint(relation, "same_name"))
        .await
        .unwrap();
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let mut cases = vec![
        joint(relation, "same_name"),
        joint(relation, " "),
        histogram(relation, "same_name", 0, vec![StatisticsValue::Int64(1)]),
        histogram(relation, "empty", 0, vec![]),
        histogram(
            relation,
            "wrong_type",
            0,
            vec![StatisticsValue::Utf8("x".into())],
        ),
        histogram(
            relation,
            "decreasing",
            0,
            vec![StatisticsValue::Int64(2), StatisticsValue::Int64(1)],
        ),
        histogram(
            relation,
            "duplicate_bounds",
            0,
            vec![StatisticsValue::Int64(1), StatisticsValue::Int64(1)],
        ),
    ];
    for ids in [vec![], vec![0], vec![0, 0], vec![0, 999], vec![-1, 0]] {
        let mut info = joint(relation, "bad_targets");
        info.column_ids = ids;
        cases.push(info);
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        cases.push(histogram(
            relation,
            "nonfinite",
            3,
            vec![StatisticsValue::Float64(value)],
        ));
    }
    let mut overflow = histogram(
        relation,
        "overflow",
        0,
        vec![StatisticsValue::Int64(1), StatisticsValue::Int64(2)],
    );
    if let Contents::Histogram { buckets } = &mut overflow.contents {
        buckets[0].frequency = u64::MAX;
    }
    cases.push(overflow);
    for info in cases {
        assert!(
            catalog
                .create_advanced_statistics(before.snapshot_id, info)
                .await
                .is_err()
        );
        assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
        assert_eq!(count(catalog.connection(), "optd_adv_stats").await, 1);
    }
    assert!(
        catalog
            .update_advanced_statistics(
                before.snapshot_id,
                Update {
                    statistics_id: id,
                    contents: Contents::Histogram { buckets: vec![] }
                }
            )
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert!(matches!(
        catalog
            .create_advanced_statistics(SnapshotId::INITIAL, joint(relation, "stale"))
            .await,
        Err(CatalogError::StaleSnapshot { .. })
    ));
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
}

#[tokio::test]
async fn summary_failure_rolls_back_creation_updates_drops_and_invalidation() {
    let (catalog, relation) = setup().await;
    let db = catalog.connection();
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    db.execute_unprepared("CREATE TRIGGER fail_adv_summary BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected advanced summary failure'); END").await.unwrap();
    assert!(
        catalog
            .create_advanced_statistics(before.snapshot_id, joint(relation, "xy"))
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(count(db, "optd_adv_stats").await, 0);
    db.execute_unprepared("DROP TRIGGER fail_adv_summary")
        .await
        .unwrap();
    let id = catalog
        .create_advanced_statistics(before.snapshot_id, joint(relation, "xy"))
        .await
        .unwrap();
    assert_eq!(id, before.next_catalog_id);
    let snapshot = head(&catalog).await;
    let old = catalog
        .reader()
        .await
        .unwrap()
        .get_advanced_statistics(id)
        .await
        .unwrap();
    let base = catalog
        .reader()
        .await
        .unwrap()
        .get_relation_statistics(relation)
        .await
        .unwrap();
    db.execute_unprepared("CREATE TRIGGER fail_adv_summary BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected advanced summary failure'); END").await.unwrap();
    assert!(
        catalog
            .update_advanced_statistics(
                snapshot,
                Update {
                    statistics_id: id,
                    contents: Contents::JointNdv { distinct_count: 99 }
                }
            )
            .await
            .is_err()
    );
    assert!(
        catalog
            .drop_advanced_statistics(snapshot, Drop { statistics_id: id })
            .await
            .is_err()
    );
    assert!(
        catalog
            .invalidate_relation_statistics(snapshot, relation)
            .await
            .is_err()
    );
    assert_eq!(head(&catalog).await, snapshot);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap(),
        old
    );
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap(),
        base
    );
    assert_eq!(count(db, "optd_adv_stats").await, 1);
    db.execute_unprepared("DROP TRIGGER fail_adv_summary")
        .await
        .unwrap();
    catalog
        .update_advanced_statistics(
            snapshot,
            Update {
                statistics_id: id,
                contents: Contents::JointNdv { distinct_count: 99 },
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn advanced_ids_and_mutations_are_catalog_scoped() {
    let (first, relation) = setup().await;
    let catalog_id = first
        .register_catalog(
            head(&first).await,
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
    let second = Catalog::on_catalog(first.connection().clone(), catalog_id);
    let other_relation = table(&second).await;
    let id = first
        .create_advanced_statistics(head(&first).await, joint(relation, "xy"))
        .await
        .unwrap();
    let other_id = second
        .create_advanced_statistics(head(&second).await, joint(other_relation, "xy"))
        .await
        .unwrap();
    let before = first.reader().await.unwrap().snapshot_info().clone();
    assert!(
        second
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        first
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(other_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        second
            .reader()
            .await
            .unwrap()
            .get_relation_advanced_statistics(relation)
            .await
            .is_err()
    );
    assert!(
        second
            .create_advanced_statistics(before.snapshot_id, joint(relation, "foreign"))
            .await
            .is_err()
    );
    assert!(matches!(
        second
            .update_advanced_statistics(
                before.snapshot_id,
                Update {
                    statistics_id: id,
                    contents: Contents::JointNdv { distinct_count: 99 }
                }
            )
            .await,
        Err(CatalogError::AdvancedStatisticsNotFound { .. })
    ));
    assert!(matches!(
        second
            .drop_advanced_statistics(before.snapshot_id, Drop { statistics_id: id })
            .await,
        Err(CatalogError::AdvancedStatisticsNotFound { .. })
    ));
    assert!(
        second
            .invalidate_relation_statistics(before.snapshot_id, relation)
            .await
            .is_err()
    );
    assert_eq!(first.reader().await.unwrap().snapshot_info(), &before);
    second
        .drop_advanced_statistics(
            before.snapshot_id,
            Drop {
                statistics_id: other_id,
            },
        )
        .await
        .unwrap();
    assert!(
        first
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn decimal_histograms_round_trip_and_corrupt_or_unknown_contents_fail_closed() {
    let (catalog, relation) = setup().await;
    let value = 10_i128.pow(37) + 1;
    let contents = histogram(
        relation,
        "decimal_hist",
        2,
        vec![StatisticsValue::Decimal128 {
            value,
            precision: 38,
            scale: 0,
        }],
    );
    let id = catalog
        .create_advanced_statistics(head(&catalog).await, contents.clone())
        .await
        .unwrap();
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .unwrap()
            .contents,
        contents.contents
    );
    let db = catalog.connection();
    db.execute_unprepared("UPDATE optd_adv_stats SET value_encoding_version=999")
        .await
        .unwrap();
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .is_err()
    );
    db.execute_unprepared("UPDATE optd_adv_stats SET value_encoding_version=1, contents='broken'")
        .await
        .unwrap();
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .is_err()
    );
    // Well-formed JSON must also obey the semantic contract on reads.
    db.execute_unprepared("UPDATE optd_adv_stats SET contents='{\"histogram\":{\"buckets\":[]}}'")
        .await
        .unwrap();
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn additive_upgrade_keeps_basic_tables_and_legacy_history_unchanged() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(include_str!("fixtures/legacy.sql"))
        .await
        .unwrap();
    Migrator::up(&db, Some(1)).await.unwrap();
    let sql = "SELECT s.statistics_id, s.begin_snapshot AS definition_begin, p.begin_snapshot, p.end_snapshot, p.row_count, c.payload_begin_snapshot, c.lower_bound FROM optd_statistics s JOIN optd_statistics_payload p ON p.statistics_id=s.statistics_id AND p.statistics_begin_snapshot=s.begin_snapshot JOIN optd_column_statistics c ON c.statistics_id=s.statistics_id AND c.statistics_begin_snapshot=s.begin_snapshot AND c.payload_begin_snapshot=p.begin_snapshot";
    let before = db
        .query_one_raw(Statement::from_string(db.get_database_backend(), sql))
        .await
        .unwrap()
        .unwrap();
    let before: (i64, i64, i64, i64, i64, i64, String) = (
        before.try_get("", "statistics_id").unwrap(),
        before.try_get("", "definition_begin").unwrap(),
        before.try_get("", "begin_snapshot").unwrap(),
        before.try_get("", "end_snapshot").unwrap(),
        before.try_get("", "row_count").unwrap(),
        before.try_get("", "payload_begin_snapshot").unwrap(),
        before.try_get("", "lower_bound").unwrap(),
    );
    Migrator::up(&db, None).await.unwrap();
    let after = db
        .query_one_raw(Statement::from_string(db.get_database_backend(), sql))
        .await
        .unwrap()
        .unwrap();
    let after = (
        after.try_get("", "statistics_id").unwrap(),
        after.try_get("", "definition_begin").unwrap(),
        after.try_get("", "begin_snapshot").unwrap(),
        after.try_get("", "end_snapshot").unwrap(),
        after.try_get("", "row_count").unwrap(),
        after.try_get("", "payload_begin_snapshot").unwrap(),
        after.try_get("", "lower_bound").unwrap(),
    );
    assert_eq!(before, after);
    assert_eq!(count(&db, "optd_adv_stats").await, 0);
    assert_eq!(count(&db, "optd_snapshot").await, 9);
    let catalog = Catalog::new(db);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .snapshot_info()
            .next_catalog_id,
        5
    );
    assert_eq!(
        catalog
            .reader_at(SnapshotId::from_raw(5))
            .await
            .unwrap()
            .get_relation_statistics(2)
            .await
            .unwrap()
            .unwrap()
            .row_count,
        Some(1)
    );
    Migrator::up(catalog.connection(), None).await.unwrap();
}

/// Opt-in disposable local PostgreSQL; every run isolates itself in a new schema.
#[tokio::test]
async fn postgres_advanced_statistics_publication_and_invalidation() {
    let Ok(url) = std::env::var("OPTD_TEST_POSTGRES_URL") else {
        return;
    };
    let admin = Database::connect(&url).await.unwrap();
    let name = format!(
        "optd_adv_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {name}"))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(&name).max_connections(1);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let catalog = Catalog::new(db.clone());
    let relation = table(&catalog).await;
    basic(&catalog, relation, 100).await;
    let basic_before = catalog
        .reader()
        .await
        .unwrap()
        .get_relation_statistics(relation)
        .await
        .unwrap();
    let id = catalog
        .create_advanced_statistics(head(&catalog).await, joint(relation, "xy"))
        .await
        .unwrap();
    let created = head(&catalog).await;
    let updated = catalog
        .update_advanced_statistics(
            created,
            Update {
                statistics_id: id,
                contents: Contents::JointNdv { distinct_count: 99 },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .reader_at(created)
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(updated)
    );
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap(),
        basic_before
    );
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    db.execute_unprepared("CREATE FUNCTION fail_adv_summary() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected advanced summary failure'; END $$; CREATE TRIGGER fail_adv_summary BEFORE INSERT ON optd_snapshot_changes FOR EACH ROW EXECUTE FUNCTION fail_adv_summary()").await.unwrap();
    assert!(
        catalog
            .invalidate_relation_statistics(updated, relation)
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap(),
        basic_before
    );
    db.execute_unprepared(
        "DROP TRIGGER fail_adv_summary ON optd_snapshot_changes; DROP FUNCTION fail_adv_summary()",
    )
    .await
    .unwrap();
    catalog
        .invalidate_relation_statistics(updated, relation)
        .await
        .unwrap();
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(relation)
            .await
            .unwrap()
            .is_none()
    );
    drop(catalog);
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {name} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
}

#[tokio::test]
async fn failed_additive_migration_rolls_back_ddl_and_can_retry() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(include_str!("fixtures/legacy.sql"))
        .await
        .unwrap();
    Migrator::up(&db, Some(1)).await.unwrap();
    // Conflict after the new table and its first index have been created.
    db.execute_unprepared(
        "CREATE INDEX idx_optd_adv_stats_visibility ON optd_snapshot (snapshot_id)",
    )
    .await
    .unwrap();
    assert!(Migrator::up(&db, None).await.is_err());
    assert!(
        db.execute_unprepared("SELECT * FROM optd_adv_stats")
            .await
            .is_err()
    );
    assert_eq!(count(&db, "seaql_migrations").await, 3);
    assert_eq!(count(&db, "optd_statistics_payload").await, 1);
    db.execute_unprepared("DROP INDEX idx_optd_adv_stats_visibility")
        .await
        .unwrap();
    Migrator::up(&db, None).await.unwrap();
    assert_eq!(
        count(&db, "seaql_migrations").await,
        i64::try_from(Migrator::migrations().len()).unwrap()
    );
    assert_eq!(count(&db, "optd_adv_stats").await, 0);
}
