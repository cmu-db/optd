use optd_catalog::{
    Catalog, CatalogError,
    migration::{Migrator, MigratorTrait},
    schema::CreateSchemaInfo,
    snapshot::{SnapshotChange, SnapshotId},
    source::RegisterCatalogInfo,
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    TransactionTrait,
};

async fn count(db: &DatabaseConnection, table: &str) -> i64 {
    db.query_one_raw(Statement::from_string(
        db.get_database_backend(),
        format!("SELECT COUNT(*) AS count FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "count")
    .unwrap()
}

#[tokio::test]
async fn publication_uses_global_history_not_branch_heads() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let branches = db
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM sqlite_master WHERE type='table' AND name='optd_branch'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        branches.try_get::<i64>("", "count").unwrap(),
        0,
        "branch heads must not exist"
    );
    exercise_history(&db).await;
}

async fn exercise_history(db: &DatabaseConnection) {
    let catalog = Catalog::new(db.clone());
    let initial = catalog.reader().await.unwrap();
    assert_eq!(initial.schema_version(), 0);
    let id = catalog
        .create_schema(
            initial.snapshot_id(),
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap();
    let current = catalog.reader().await.unwrap();
    assert_eq!(current.snapshot_id().as_i64(), 1);
    assert_eq!(current.schema_version(), 1);
    assert_eq!(
        current.snapshot_change().await.unwrap(),
        SnapshotChange::CreateSchema { schema_id: id }
    );
    assert!(initial.get_all_schemas().await.unwrap().is_empty());
    let duplicate = catalog
        .create_schema(
            current.snapshot_id(),
            CreateSchemaInfo {
                schema_name: "public".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate,
        CatalogError::SchemaAlreadyExists { .. }
    ));
    assert_eq!(
        catalog.reader().await.unwrap().snapshot_info(),
        current.snapshot_info()
    );
    assert_eq!(
        count(db, "optd_snapshot").await,
        count(db, "optd_snapshot_changes").await
    );
}

async fn exercise_overlapping_writers(first: DatabaseConnection, second: DatabaseConnection) {
    let catalog = Catalog::new(first.clone());
    let base = catalog.reader().await.unwrap().snapshot_id();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let b = barrier.clone();
    let left = tokio::spawn(async move {
        b.wait().await;
        Catalog::new(first)
            .create_schema(
                base,
                CreateSchemaInfo {
                    schema_name: "left".into(),
                },
            )
            .await
    });
    let right = tokio::spawn(async move {
        barrier.wait().await;
        Catalog::new(second)
            .create_schema(
                base,
                CreateSchemaInfo {
                    schema_name: "right".into(),
                },
            )
            .await
    });
    let results = [left.await.unwrap(), right.await.unwrap()];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|r| matches!(r, Err(CatalogError::StaleSnapshot { expected, actual }) if *expected == base && actual.as_i64() == base.as_i64() + 1)).count(), 1);
    assert_eq!(
        catalog.reader().await.unwrap().snapshot_id().as_i64(),
        base.as_i64() + 1
    );
}

async fn exercise_mutex_wait(first: DatabaseConnection, second: DatabaseConnection) {
    let base = Catalog::new(first.clone())
        .reader()
        .await
        .unwrap()
        .snapshot_id();
    let holder = first.begin().await.unwrap();
    holder
        .execute_unprepared(
            "UPDATE optd_catalog_metadata SET format_version = format_version WHERE id = 1",
        )
        .await
        .unwrap();
    let mut waiter = tokio::spawn(async move {
        Catalog::new(second)
            .create_schema(
                base,
                CreateSchemaInfo {
                    schema_name: format!("waiter_{}", base.as_i64()),
                },
            )
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter)
            .await
            .is_err(),
        "writer must wait for the held mutex rather than read or mutate first"
    );
    // Commit a mutex-row version so a waiter inheriting PostgreSQL REPEATABLE READ
    // would fail serialization; the public commit path must request READ COMMITTED.
    holder.commit().await.unwrap();
    waiter.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_overlapping_writers_serialize_before_reading() {
    for mode in ["DELETE", "WAL"] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("catalog.sqlite").display()
        );
        let first = Database::connect(&url).await.unwrap();
        first
            .execute_unprepared(&format!("PRAGMA journal_mode={mode}"))
            .await
            .unwrap();
        Migrator::up(&first, None).await.unwrap();
        let second = Database::connect(&url).await.unwrap();
        exercise_mutex_wait(first.clone(), second.clone()).await;
        exercise_overlapping_writers(first, second).await;
    }
}

#[tokio::test]
async fn namespaces_share_history_but_not_names_or_mutations() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let root = Catalog::new(db.clone());
    let mut ids = Vec::new();
    for name in ["alpha", "beta"] {
        let id = root
            .register_catalog(
                root.reader().await.unwrap().snapshot_id(),
                RegisterCatalogInfo {
                    catalog_name: name.into(),
                    catalog_kind: "postgresql".into(),
                    external_catalog_name: Some("app".into()),
                    adapter_name: "postgresql".into(),
                    connection_profile_reference: format!("connection-profile://{name}"),
                },
            )
            .await
            .unwrap();
        let scoped = Catalog::on_catalog(db.clone(), id);
        let schema_id = scoped
            .create_schema(
                scoped.reader().await.unwrap().snapshot_id(),
                CreateSchemaInfo {
                    schema_name: "public".into(),
                },
            )
            .await
            .unwrap();
        ids.push((id, schema_id));
    }
    assert_eq!(
        root.reader_at(SnapshotId::INITIAL)
            .await
            .unwrap()
            .get_catalogs()
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        root.reader()
            .await
            .unwrap()
            .get_catalogs()
            .await
            .unwrap()
            .len(),
        3
    );
    let alpha = Catalog::on_catalog(db.clone(), ids[0].0);
    let beta = Catalog::on_catalog(db.clone(), ids[1].0);
    assert_eq!(
        alpha
            .reader()
            .await
            .unwrap()
            .get_all_schemas()
            .await
            .unwrap()[0]
            .schema_id,
        ids[0].1
    );
    assert_eq!(
        beta.reader()
            .await
            .unwrap()
            .get_all_schemas()
            .await
            .unwrap()[0]
            .schema_id,
        ids[1].1
    );
    let base = beta.reader().await.unwrap().snapshot_id();
    assert!(matches!(
        beta.drop_schema(
            base,
            optd_catalog::schema::DropSchemaInfo {
                schema_id: ids[0].1
            }
        )
        .await,
        Err(CatalogError::SchemaNotFound { .. })
    ));
    assert_eq!(root.reader().await.unwrap().snapshot_id(), base);
    assert_ne!(ids[0].1, ids[1].1);
}

#[tokio::test]
async fn legacy_upgrade_preserves_history_ids_and_statistics() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(include_str!("fixtures/legacy.sql"))
        .await
        .unwrap();
    exercise_legacy_upgrade(&db).await;
}

async fn exercise_legacy_upgrade(db: &DatabaseConnection) {
    let before = db.query_all_raw(Statement::from_string(db.get_database_backend(), "SELECT snapshot_id, CAST(snapshot_time AS TEXT) AS time, next_catalog_id FROM optd_snapshot ORDER BY snapshot_id")).await.unwrap();
    let before = before
        .into_iter()
        .map(|r| {
            (
                r.try_get::<i64>("", "snapshot_id").unwrap(),
                r.try_get::<String>("", "time").unwrap(),
                r.try_get::<i64>("", "next_catalog_id").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    Migrator::up(db, None).await.unwrap();
    let after = db.query_all_raw(Statement::from_string(db.get_database_backend(), "SELECT snapshot_id, CAST(snapshot_time AS TEXT) AS time, next_catalog_id FROM optd_snapshot ORDER BY snapshot_id")).await.unwrap();
    let after = after
        .into_iter()
        .map(|r| {
            (
                r.try_get::<i64>("", "snapshot_id").unwrap(),
                r.try_get::<String>("", "time").unwrap(),
                r.try_get::<i64>("", "next_catalog_id").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        before, after,
        "migration must preserve snapshot IDs, times, and allocator values"
    );
    let catalog = Catalog::new(db.clone());
    let current = catalog.reader().await.unwrap();
    assert_eq!(current.snapshot_id().as_i64(), 9);
    assert_eq!(current.snapshot_info().next_catalog_id, 5);
    assert_eq!(current.schema_version(), 6);
    assert!(current.get_all_relations().await.unwrap().is_empty());
    for (id, version) in [
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 3),
        (5, 3),
        (6, 3),
        (7, 4),
        (8, 5),
        (9, 6),
    ] {
        let reader = catalog.reader_at(SnapshotId::from_raw(id)).await.unwrap();
        assert_eq!(reader.schema_version(), version);
        assert_eq!(
            reader.snapshot_change().await.unwrap(),
            SnapshotChange::LegacyImport
        );
    }
    let historical = catalog.reader_at(SnapshotId::from_raw(5)).await.unwrap();
    assert_eq!(
        historical
            .get_relation_statistics(2)
            .await
            .unwrap()
            .unwrap()
            .row_count,
        Some(1)
    );
    assert_eq!(
        historical.get_relation_indexes(2).await.unwrap()[0].index_id,
        3
    );
    assert!(
        catalog
            .reader_at(SnapshotId::from_raw(6))
            .await
            .unwrap()
            .get_relation_statistics(2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(count(db, "optd_column_statistics").await, 1);
    assert_eq!(count(db, "optd_snapshot").await, 9);
    assert_eq!(count(db, "optd_snapshot_changes").await, 9);
    Migrator::up(db, None).await.unwrap();
    let id = catalog
        .create_schema(
            current.snapshot_id(),
            CreateSchemaInfo {
                schema_name: "restored".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(id, 5);
    assert_eq!(catalog.reader().await.unwrap().snapshot_id().as_i64(), 10);
    assert_eq!(catalog.reader().await.unwrap().schema_version(), 7);
}

#[tokio::test]
async fn migration_rejects_ambiguous_heads_without_partial_ddl() {
    for mutation in [
        "INSERT INTO optd_branch VALUES ('other',9)",
        "DELETE FROM optd_branch",
        "UPDATE optd_branch SET branch_name='other'",
        "UPDATE optd_branch SET current_snapshot_id=8",
        "UPDATE optd_branch SET current_snapshot_id=999",
    ] {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(include_str!("fixtures/legacy.sql"))
            .await
            .unwrap();
        db.execute_unprepared(mutation).await.unwrap();
        assert!(
            Migrator::up(&db, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("requires exactly main")
        );
        assert!(
            db.execute_unprepared("SELECT schema_version FROM optd_snapshot")
                .await
                .is_err()
        );
        assert_eq!(count(&db, "seaql_migrations").await, 2);
        assert_eq!(count(&db, "optd_snapshot").await, 9);
    }
}

#[tokio::test]
async fn migration_ddl_and_backfill_roll_back_and_can_retry() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared(include_str!("fixtures/legacy.sql"))
        .await
        .unwrap();
    // Fails after new tables/columns and registration have been inserted.
    db.execute_unprepared("CREATE TRIGGER fail_backfill BEFORE UPDATE ON optd_snapshot BEGIN SELECT RAISE(ABORT, 'injected migration failure'); END").await.unwrap();
    assert!(
        Migrator::up(&db, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("injected migration failure")
    );
    assert!(
        db.execute_unprepared("SELECT schema_version FROM optd_snapshot")
            .await
            .is_err()
    );
    assert_eq!(count(&db, "optd_branch").await, 1);
    assert_eq!(count(&db, "seaql_migrations").await, 2);
    db.execute_unprepared("DROP TRIGGER fail_backfill")
        .await
        .unwrap();
    Migrator::up(&db, None).await.unwrap();
}

async fn exercise_summary_rollback(db: &DatabaseConnection) {
    let catalog = Catalog::new(db.clone());
    let keep = catalog
        .create_schema(
            catalog.reader().await.unwrap().snapshot_id(),
            CreateSchemaInfo {
                schema_name: "keep".into(),
            },
        )
        .await
        .unwrap();
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.execute_unprepared("CREATE TRIGGER fail_summary BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected summary failure'); END").await.unwrap();
    } else {
        db.execute_unprepared("CREATE FUNCTION fail_summary() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected summary failure'; END $$; CREATE TRIGGER fail_summary BEFORE INSERT ON optd_snapshot_changes FOR EACH ROW EXECUTE FUNCTION fail_summary()").await.unwrap();
    }
    let error = catalog
        .create_schema(
            before.snapshot_id,
            CreateSchemaInfo {
                schema_name: "failed".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected summary failure"));
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_all_schemas()
            .await
            .unwrap()
            .iter()
            .all(|s| s.schema_name != "failed")
    );
    let error = catalog
        .drop_schema(
            before.snapshot_id,
            optd_catalog::schema::DropSchemaInfo { schema_id: keep },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected summary failure"));
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    let drop = if db.get_database_backend() == DatabaseBackend::Sqlite {
        "DROP TRIGGER fail_summary"
    } else {
        "DROP TRIGGER fail_summary ON optd_snapshot_changes; DROP FUNCTION fail_summary()"
    };
    db.execute_unprepared(drop).await.unwrap();
    let id = catalog
        .create_schema(
            before.snapshot_id,
            CreateSchemaInfo {
                schema_name: "retry".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(id, before.next_catalog_id);
    // The failed drop's interval closure must not leak into the successful next snapshot.
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_schema(optd_catalog::schema::GetSchemaInfo { schema_id: keep })
            .await
            .unwrap()
            .schema_name,
        "keep"
    );
    assert_eq!(
        catalog.reader().await.unwrap().snapshot_id().as_i64(),
        before.snapshot_id.as_i64() + 1
    );
}

#[tokio::test]
async fn failed_summary_rolls_back_metadata_snapshot_and_allocator() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    exercise_summary_rollback(&db).await;
}

#[tokio::test]
async fn missing_writer_mutex_fails_closed() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    db.execute_unprepared("DELETE FROM optd_catalog_metadata")
        .await
        .unwrap();
    let catalog = Catalog::new(db.clone());
    assert!(matches!(
        catalog
            .create_schema(
                SnapshotId::INITIAL,
                CreateSchemaInfo {
                    schema_name: "forbidden".into()
                }
            )
            .await,
        Err(CatalogError::MissingWriterMutex)
    ));
    assert_eq!(count(&db, "optd_schema").await, 0);
    assert_eq!(count(&db, "optd_snapshot").await, 1);
}

#[tokio::test]
async fn postgres_legacy_upgrade_preserves_history() {
    let Ok(url) = std::env::var("OPTD_TEST_POSTGRES_URL") else {
        eprintln!("PostgreSQL verification skipped: OPTD_TEST_POSTGRES_URL not set");
        return;
    };
    let admin = Database::connect(&url).await.unwrap();
    let name = format!(
        "optd_legacy_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {name}"))
        .await
        .unwrap();
    let mut opts = ConnectOptions::new(url);
    opts.set_schema_search_path(&name).max_connections(1);
    let db = Database::connect(opts).await.unwrap();
    let fixture = include_str!("fixtures/legacy.sql")
        .replace(
            "snapshot_id INTEGER PRIMARY KEY",
            "snapshot_id BIGSERIAL PRIMARY KEY",
        )
        .replace("row_id INTEGER PRIMARY KEY", "row_id BIGSERIAL PRIMARY KEY")
        .replace(" TIMESTAMP", " TIMESTAMPTZ");
    db.execute_unprepared(&fixture).await.unwrap();
    // Raw fixture inserts must leave legacy row-key sequences as a real old writer would.
    for table in [
        "optd_schema",
        "optd_relation",
        "optd_column",
        "optd_index",
        "optd_statistics",
        "optd_statistics_payload",
        "optd_column_statistics",
    ] {
        db.execute_unprepared(&format!("SELECT setval(pg_get_serial_sequence('{table}', 'row_id'), (SELECT MAX(row_id) FROM {table}))")).await.unwrap();
    }
    exercise_legacy_upgrade(&db).await;
    db.close().await.unwrap();
    admin.close().await.unwrap();
}

/// Opt-in disposable LOCAL PostgreSQL instance, never production. Each run creates a fresh schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_single_history_and_concurrency() {
    let Ok(url) = std::env::var("OPTD_TEST_POSTGRES_URL") else {
        eprintln!("PostgreSQL verification skipped: OPTD_TEST_POSTGRES_URL not set");
        return;
    };
    let admin = Database::connect(&url).await.unwrap();
    let name = format!(
        "optd_test_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {name}"))
        .await
        .unwrap();
    let mut opts = ConnectOptions::new(url);
    opts.set_schema_search_path(&name).max_connections(1);
    let first = Database::connect(opts.clone()).await.unwrap();
    // Force a non-default server/session default: commit must explicitly request READ COMMITTED.
    first
        .execute_unprepared("SET default_transaction_isolation = 'repeatable read'")
        .await
        .unwrap();
    Migrator::up(&first, Some(2)).await.unwrap();
    first.execute_unprepared("CREATE FUNCTION fail_backfill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected migration failure'; END $$; CREATE TRIGGER fail_backfill BEFORE UPDATE ON optd_snapshot FOR EACH ROW EXECUTE FUNCTION fail_backfill()").await.unwrap();
    assert!(
        Migrator::up(&first, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("injected migration failure")
    );
    assert_eq!(count(&first, "optd_branch").await, 1);
    assert_eq!(count(&first, "seaql_migrations").await, 2);
    // The failed migration must not leave an added column or partial registration.
    let columns = first.query_one_raw(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT COUNT(*) AS count FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'optd_snapshot' AND column_name = 'schema_version'"))
        .await.unwrap().unwrap();
    assert_eq!(columns.try_get::<i64>("", "count").unwrap(), 0);
    first
        .execute_unprepared(
            "DROP TRIGGER fail_backfill ON optd_snapshot; DROP FUNCTION fail_backfill()",
        )
        .await
        .unwrap();
    Migrator::up(&first, None).await.unwrap();
    exercise_history(&first).await;
    exercise_summary_rollback(&first).await;
    let second = Database::connect(opts).await.unwrap();
    exercise_mutex_wait(first.clone(), second.clone()).await;
    exercise_mutex_wait(second.clone(), first.clone()).await;
    exercise_overlapping_writers(first.clone(), second.clone()).await;
    assert_eq!(
        count(&first, "optd_snapshot").await,
        count(&first, "optd_snapshot_changes").await
    );
    first.close().await.unwrap();
    second.close().await.unwrap();
    admin.close().await.unwrap();
}
