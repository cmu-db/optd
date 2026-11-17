use optd_catalog::{
    Catalog, SnapshotId,
    entity::{query, query_instance},
    migration::{Migrator, MigratorTrait},
    query_history::{
        BeginQueryInstance, FinishQueryInstance, HistoryError, InstanceFilter, QueryHistoryStore,
        QueryInstanceId, QueryStatus, QueryTemplate,
    },
    schema::CreateSchemaInfo,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, sea_query::Expr,
};
use serde_json::json;

async fn setup() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    db
}
fn template() -> QueryTemplate {
    QueryTemplate {
        normalization_version: 1,
        dialect: "generic".into(),
        resolution_scope: json!({"catalog_id": 0, "schema": "public"}),
        template: "SELECT * FROM t WHERE a = $optd_slot_0".into(),
        parameter_shape: json!(["integer_literal"]),
    }
}
fn request(template: Option<QueryTemplate>) -> BeginQueryInstance {
    BeginQueryInstance {
        instance_id: QueryInstanceId::new(),
        template,
        request: json!({"input_kind": "sql", "capture": "template_only"}),
        resolution_context: json!({"default_catalog": "datafusion", "default_schema": "public"}),
        optimizer_configuration: json!({"optd_enabled": true}),
        implementation_descriptor: json!({"crate_version": env!("CARGO_PKG_VERSION"), "revision": null}),
    }
}
fn completion(status: QueryStatus) -> FinishQueryInstance {
    FinishQueryInstance {
        status,
        optimization_duration_ns: 100,
        outcome_details: json!({"path": "optd"}),
    }
}

#[tokio::test]
async fn instances_share_templates_but_not_context_or_outcomes() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    let first = store.begin(request(Some(template()))).await.unwrap();
    let mut second_input = request(Some(template()));
    second_input.optimizer_configuration = json!({"optd_enabled": false});
    let second = store.begin(second_input).await.unwrap();
    assert_eq!(first.query_id, second.query_id);
    assert_ne!(first.instance_id, second.instance_id);
    assert_eq!(
        first
            .instance_id
            .to_string()
            .parse::<QueryInstanceId>()
            .unwrap(),
        first.instance_id
    );
    assert_eq!(query::Entity::find().count(&db).await.unwrap(), 1);
    let stored = store
        .get_query(first.query_id.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.definition, template());
    assert_eq!(stored.fingerprint, template().fingerprint().unwrap());
    store
        .finish(first.instance_id, completion(QueryStatus::Optimized))
        .await
        .unwrap();
    store
        .finish(second.instance_id, completion(QueryStatus::Bypassed))
        .await
        .unwrap();
    let a = store
        .get_instance(first.instance_id)
        .await
        .unwrap()
        .unwrap();
    let b = store
        .get_instance(second.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.status, QueryStatus::Optimized);
    assert_eq!(b.status, QueryStatus::Bypassed);
    assert_ne!(a.optimizer_configuration, b.optimizer_configuration);
    assert!(a.finished_at.is_some());
}

#[tokio::test]
async fn history_does_not_publish_snapshots_or_allocate_catalog_objects() {
    let db = setup().await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let summary = catalog
        .reader()
        .await
        .unwrap()
        .snapshot_change()
        .await
        .unwrap();
    let store = QueryHistoryStore::new(db);
    let instance = store.begin(request(Some(template()))).await.unwrap();
    assert!(instance.catalog_snapshot_id.is_none());
    store
        .attach_snapshot(instance.instance_id, before.snapshot_id)
        .await
        .unwrap();
    store
        .finish(instance.instance_id, completion(QueryStatus::Optimized))
        .await
        .unwrap();
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .snapshot_change()
            .await
            .unwrap(),
        summary
    );
}

#[tokio::test]
async fn template_fingerprints_are_canonical_scoped_and_versioned() {
    let base = template();
    let mut reordered = base.clone();
    reordered.resolution_scope =
        serde_json::from_str(r#"{"schema":"public","catalog_id":0}"#).unwrap();
    assert_eq!(
        base.fingerprint().unwrap(),
        reordered.fingerprint().unwrap()
    );
    for changed in [
        QueryTemplate {
            normalization_version: 2,
            ..base.clone()
        },
        QueryTemplate {
            dialect: "postgresql".into(),
            ..base.clone()
        },
        QueryTemplate {
            resolution_scope: json!({"catalog_id": 1, "schema":"public"}),
            ..base.clone()
        },
        QueryTemplate {
            parameter_shape: json!(["string_literal"]),
            ..base.clone()
        },
        QueryTemplate {
            template: "SELECT 1".into(),
            ..base.clone()
        },
    ] {
        assert_ne!(base.fingerprint().unwrap(), changed.fingerprint().unwrap());
    }
}

#[tokio::test]
async fn fingerprint_collision_never_associates_a_different_template() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    store.begin(request(Some(template()))).await.unwrap();
    // Simulate a digest collision by placing different canonical contents under the key.
    query::Entity::update_many()
        .col_expr(
            query::Column::Template,
            Expr::value("SELECT a_different_query"),
        )
        .exec(&db)
        .await
        .unwrap();
    let input = request(Some(template()));
    let id = input.instance_id;
    assert!(matches!(
        store.begin(input).await,
        Err(HistoryError::FingerprintCollision)
    ));
    assert!(store.get_instance(id).await.unwrap().is_none());
    let mut unassociated = request(None);
    unassociated.instance_id = id;
    assert!(store.begin(unassociated).await.unwrap().query_id.is_none());
}

#[tokio::test]
async fn duplicate_instance_rolls_back_new_template_registration() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    let mut input = request(Some(template()));
    let first = store.begin(input.clone()).await.unwrap();
    input.template.as_mut().unwrap().normalization_version = 2;
    assert!(store.begin(input).await.is_err());
    assert_eq!(query::Entity::find().count(&db).await.unwrap(), 1);
    assert_eq!(
        store
            .get_instance(first.instance_id)
            .await
            .unwrap()
            .unwrap(),
        first
    );
}

#[tokio::test]
async fn terminal_states_are_idempotent_and_conflicts_do_not_overwrite() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db);
    let instance = store.begin(request(None)).await.unwrap();
    let terminal = store
        .finish(instance.instance_id, completion(QueryStatus::Failed))
        .await
        .unwrap();
    assert_eq!(
        store
            .finish(instance.instance_id, completion(QueryStatus::Failed))
            .await
            .unwrap(),
        terminal
    );
    assert!(matches!(
        store
            .finish(instance.instance_id, completion(QueryStatus::Optimized))
            .await,
        Err(HistoryError::LifecycleConflict { .. })
    ));
    assert!(matches!(
        store
            .attach_snapshot(instance.instance_id, SnapshotId::INITIAL)
            .await,
        Err(HistoryError::LifecycleConflict { .. })
    ));
    assert!(
        store
            .finish(QueryInstanceId::new(), completion(QueryStatus::Cancelled))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .get_instance(instance.instance_id)
            .await
            .unwrap()
            .unwrap(),
        terminal
    );
}

#[tokio::test]
async fn snapshot_attachment_is_real_and_immutable() {
    let db = setup().await;
    let catalog = Catalog::new(db.clone());
    let store = QueryHistoryStore::new(db);
    let instance = store.begin(request(None)).await.unwrap();
    assert!(
        store
            .attach_snapshot(instance.instance_id, SnapshotId::from_raw(12345))
            .await
            .is_err()
    );
    store
        .attach_snapshot(instance.instance_id, SnapshotId::INITIAL)
        .await
        .unwrap();
    store
        .attach_snapshot(instance.instance_id, SnapshotId::INITIAL)
        .await
        .unwrap();
    catalog
        .create_schema(
            SnapshotId::INITIAL,
            CreateSchemaInfo {
                schema_name: "other".into(),
            },
        )
        .await
        .unwrap();
    let next = catalog.reader().await.unwrap().snapshot_id();
    assert!(matches!(
        store.attach_snapshot(instance.instance_id, next).await,
        Err(HistoryError::LifecycleConflict { .. })
    ));
    let page = store
        .list_instances(InstanceFilter {
            catalog_snapshot_id: Some(SnapshotId::INITIAL),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
}

#[tokio::test]
async fn pagination_handles_equal_timestamps_and_enforces_bounds() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    let first = store.begin(request(Some(template()))).await.unwrap();
    store.begin(request(None)).await.unwrap();
    store.begin(request(Some(template()))).await.unwrap();
    query_instance::Entity::update_many()
        .col_expr(
            query_instance::Column::StartedAt,
            Expr::value(first.started_at),
        )
        .exec(&db)
        .await
        .unwrap();
    let all = store
        .list_instances(InstanceFilter::default())
        .await
        .unwrap();
    let mut cursor = None;
    let mut paged = vec![];
    loop {
        let rows = store
            .list_instances(InstanceFilter {
                after: cursor,
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let Some(row) = rows.into_iter().next() else {
            break;
        };
        cursor = Some(row.cursor());
        paged.push(row);
    }
    assert_eq!(paged, all);
    assert_eq!(
        store
            .list_instances(InstanceFilter {
                query_id: first.query_id,
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
        2
    );
    for limit in [0, 201] {
        assert!(
            store
                .list_instances(InstanceFilter {
                    limit,
                    ..Default::default()
                })
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn invalid_inputs_and_corrupt_persisted_lifecycle_fail_closed() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    let mut invalid = request(Some(template()));
    invalid.template.as_mut().unwrap().normalization_version = 0;
    assert!(store.begin(invalid).await.is_err());
    let mut oversized = request(None);
    oversized.request = json!("x".repeat(1024 * 1024));
    assert!(store.begin(oversized).await.is_err());
    let instance = store.begin(request(None)).await.unwrap();
    assert!(
        store
            .finish(instance.instance_id, completion(QueryStatus::Running))
            .await
            .is_err()
    );
    let mut invalid = completion(QueryStatus::Failed);
    invalid.optimization_duration_ns = -1;
    assert!(store.finish(instance.instance_id, invalid).await.is_err());
    query_instance::Entity::update_many()
        .col_expr(query_instance::Column::Status, Expr::value("optimized"))
        .filter(query_instance::Column::QueryInstanceId.eq(instance.instance_id.to_string()))
        .exec(&db)
        .await
        .unwrap();
    assert!(matches!(
        store.get_instance(instance.instance_id).await,
        Err(HistoryError::InvalidRecord { .. })
    ));
}

#[tokio::test]
async fn unknown_encoding_is_rejected_before_lifecycle_writes() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    for attach_snapshot in [true, false] {
        let instance = store.begin(request(None)).await.unwrap();
        query_instance::Entity::update_many()
            .col_expr(query_instance::Column::EncodingVersion, Expr::value(2))
            .filter(query_instance::Column::QueryInstanceId.eq(instance.instance_id.to_string()))
            .exec(&db)
            .await
            .unwrap();
        let before = query_instance::Entity::find_by_id(instance.instance_id.to_string())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let failed = if attach_snapshot {
            store
                .attach_snapshot(instance.instance_id, SnapshotId::INITIAL)
                .await
                .is_err()
        } else {
            store
                .finish(instance.instance_id, completion(QueryStatus::Optimized))
                .await
                .is_err()
        };
        assert!(failed, "unknown encodings must be rejected before updates");
        let after = query_instance::Entity::find_by_id(instance.instance_id.to_string())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before, after,
            "a rejected lifecycle write must not alter a newer encoding"
        );
    }
}

#[tokio::test]
async fn additive_migration_preserves_existing_metadata_and_statistics_tables() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, Some(4)).await.unwrap();
    let catalog = Catalog::new(db.clone());
    catalog
        .create_schema(
            SnapshotId::INITIAL,
            CreateSchemaInfo {
                schema_name: "existing".into(),
            },
        )
        .await
        .unwrap();
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    Migrator::up(&db, None).await.unwrap();
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_all_schemas()
            .await
            .unwrap()[0]
            .schema_name,
        "existing"
    );
    let store = QueryHistoryStore::new(db);
    store.begin(request(Some(template()))).await.unwrap();
}

#[tokio::test]
async fn concurrent_template_registration_uses_one_identity() {
    let db = setup().await;
    let store = QueryHistoryStore::new(db.clone());
    let (a, b) = tokio::join!(
        store.begin(request(Some(template()))),
        store.begin(request(Some(template())))
    );
    assert_eq!(a.unwrap().query_id, b.unwrap().query_id);
    assert_eq!(query::Entity::find().count(&db).await.unwrap(), 1);
    assert_eq!(query_instance::Entity::find().count(&db).await.unwrap(), 2);
}

#[tokio::test]
async fn postgres_query_history_deduplication_lifecycle_and_snapshot_invariance() {
    let Ok(url) = std::env::var("OPTD_TEST_POSTGRES_URL") else {
        return;
    };
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!(
        "optd_history_{}",
        QueryInstanceId::new().to_string().replace('-', "")
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    // Restrict search_path for every connection in this independent pool.
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(schema.clone());
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let store = QueryHistoryStore::new(db.clone());
    let (a, b) = tokio::join!(
        store.begin(request(Some(template()))),
        store.begin(request(Some(template())))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.query_id, b.query_id);
    store
        .attach_snapshot(a.instance_id, before.snapshot_id)
        .await
        .unwrap();
    let terminal = store
        .finish(a.instance_id, completion(QueryStatus::Optimized))
        .await
        .unwrap();
    assert_eq!(
        store
            .finish(a.instance_id, completion(QueryStatus::Optimized))
            .await
            .unwrap(),
        terminal
    );
    assert!(
        store
            .finish(a.instance_id, completion(QueryStatus::Failed))
            .await
            .is_err()
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(
        store
            .list_instances(InstanceFilter::default())
            .await
            .unwrap()
            .len(),
        2
    );
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
}
