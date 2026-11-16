use super::*;
use crate::{
    persistent_catalog::{
        catalog_for_name, install_persistent_catalog, uninstall_persistent_catalog,
    },
    runner::OptdRunner,
    setup::session_context_with_information_schema,
    test_utils::test_sqlite_connection,
};
use datafusion::arrow::array::Int64Array;
use optd_catalog::{
    advanced_statistics::{AdvancedStatisticsContents, CreateAdvancedStatisticsInfo},
    snapshot::SnapshotChange,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};

#[derive(Default)]
struct CaptureGate {
    reached: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

tokio::task_local! {
    static CAPTURE_GATE: std::sync::Arc<CaptureGate>;
}

pub(super) async fn after_identity_check() {
    if let Ok(gate) = CAPTURE_GATE.try_with(std::sync::Arc::clone) {
        gate.reached.notify_one();
        gate.resume.notified().await;
    }
}

#[derive(Debug)]
struct FailingDelete {
    inner: std::sync::Arc<dyn datafusion_catalog::TableProvider>,
    mutate_first: bool,
}

#[async_trait::async_trait]
impl datafusion_catalog::TableProvider for FailingDelete {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn datafusion_catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
    async fn delete_from(
        &self,
        state: &dyn datafusion_catalog::Session,
        filters: Vec<datafusion::logical_expr::Expr>,
    ) -> DFResult<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        if self.mutate_first {
            let _ = self.inner.delete_from(state, filters).await?;
        }
        Err(DataFusionError::Execution(
            "test provider write outcome uncertain".into(),
        ))
    }
}

async fn ok(session: &SessionContext, sql: &str) -> ObservedExecution {
    let output = execute_observed_sql(session, sql).await.unwrap();
    assert!(
        output.result.is_ok(),
        "{sql}: {:?}",
        output.result.as_ref().err()
    );
    output
}

async fn setup() -> (tempfile::TempDir, SessionContext, DatabaseConnection) {
    let (temp, db) = test_sqlite_connection().await;
    let session = session_context_with_information_schema();
    install_persistent_catalog(&session, "datafusion", db.clone())
        .await
        .unwrap();
    ok(
        &session,
        "CREATE TABLE numbers AS SELECT CAST(1 AS BIGINT) AS x, CAST(1 AS BIGINT) AS y",
    )
    .await;
    (temp, session, db)
}

async fn seed(
    session: &SessionContext,
    db: &DatabaseConnection,
    catalog_name: &str,
    table: &str,
) -> (i64, i64) {
    ok(
        session,
        &format!("SELECT collect_stats('{catalog_name}.public.{table}')"),
    )
    .await;
    let catalog = catalog_for_name(db.clone(), catalog_name).await.unwrap();
    let reader = catalog.reader().await.unwrap();
    let relation_id = reader
        .get_all_relations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.relation_name == table)
        .unwrap()
        .relation_id;
    let advanced = catalog
        .create_advanced_statistics(
            reader.snapshot_id(),
            CreateAdvancedStatisticsInfo {
                relation_id,
                statistics_name: "xy".into(),
                column_ids: vec![0, 1],
                contents: AdvancedStatisticsContents::JointNdv { distinct_count: 1 },
            },
        )
        .await
        .unwrap();
    (relation_id, advanced)
}

async fn count(session: &SessionContext, table: &str) -> i64 {
    let rows = session
        .sql(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_dml_preserves_source_effects_and_closes_both_statistics() {
    let (_temp, session, db) = setup().await;
    let catalog = Catalog::new(db.clone());
    for (sql, rows) in [
        ("INSERT INTO numbers VALUES (2, 2)", 2),
        ("UPDATE numbers SET x=4 WHERE x=2", 2),
        ("DELETE FROM numbers WHERE x=4", 1),
    ] {
        let (relation, advanced) = seed(&session, &db, "datafusion", "numbers").await;
        let before = catalog.reader().await.unwrap();
        let descriptor = before.snapshot_info().clone();
        let output = ok(&session, sql).await;
        let InvalidationStatus::Applied {
            request,
            snapshot_id,
        } = &output.observation.invalidation
        else {
            panic!("{:?}", output.observation);
        };
        assert_eq!(request.relation_ids, vec![relation]);
        assert_eq!(snapshot_id.as_i64(), descriptor.snapshot_id.as_i64() + 1);
        assert_eq!(count(&session, "numbers").await, rows);
        let after = catalog.reader().await.unwrap();
        assert_eq!(after.schema_version(), descriptor.schema_version);
        assert_eq!(
            after.snapshot_info().next_catalog_id,
            descriptor.next_catalog_id
        );
        assert!(
            after
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            after
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
            Some(*snapshot_id)
        );
        assert_eq!(
            before
                .get_advanced_statistics(advanced)
                .await
                .unwrap()
                .unwrap()
                .end_snapshot,
            Some(*snapshot_id)
        );
        assert!(matches!(
            after.snapshot_change().await.unwrap(),
            SnapshotChange::InvalidateStatisticsBatch {
                cause: StatisticsInvalidationCause::ObservedSql,
                ..
            }
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_runner_handles_writes_and_ddl_with_optimizer_on_or_off() {
    for enabled in [true, false] {
        let (_temp, session, db) = setup().await;
        ok(&session, &format!("SET optd.optd_enabled = {enabled}")).await;
        let (relation, _) = seed(&session, &db, "datafusion", "numbers").await;
        // Binding discovery must not depend on the runner's statistics source.
        let runner = OptdRunner::new(session.clone());
        runner
            .execute_sql("INSERT INTO numbers VALUES (2,2)")
            .await
            .unwrap();
        assert_eq!(count(&session, "numbers").await, 2);
        assert!(matches!(
            runner.last_observation().unwrap().invalidation,
            InvalidationStatus::Applied { .. }
        ));
        assert!(
            Catalog::new(db.clone())
                .reader()
                .await
                .unwrap()
                .get_relation_statistics(relation)
                .await
                .unwrap()
                .is_none()
        );
        runner
            .execute_sql("CREATE TABLE extra AS SELECT CAST(1 AS BIGINT) AS x")
            .await
            .unwrap();
        let observation = runner.last_observation().unwrap();
        assert!(matches!(observation.kind, StatementKind::Ddl { .. }));
        assert_eq!(observation.ddl_changes.len(), 1);
        assert_eq!(
            observation.ddl_changes[0].operation,
            DdlOperation::CreateTable
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_ddl_uses_actual_callbacks_without_double_publication() {
    let (_temp, session, db) = setup().await;
    let catalog = Catalog::new(db.clone());
    for (sql, operation) in [
        ("CREATE SCHEMA analytics", DdlOperation::CreateSchema),
        (
            "CREATE TABLE analytics.extra AS SELECT 1 AS x",
            DdlOperation::CreateTable,
        ),
        ("DROP TABLE analytics.extra", DdlOperation::DropTable),
        ("DROP SCHEMA analytics", DdlOperation::DropSchema),
    ] {
        let before = catalog.reader().await.unwrap().snapshot_id();
        let output = ok(&session, sql).await;
        assert!(matches!(output.observation.kind, StatementKind::Ddl { .. }));
        assert_eq!(output.observation.ddl_changes.len(), 1);
        assert_eq!(output.observation.ddl_changes[0].operation, operation);
        assert_eq!(
            output.observation.ddl_changes[0].outcome,
            DdlOutcome::Confirmed
        );
        assert_eq!(
            catalog.reader().await.unwrap().snapshot_id().as_i64(),
            before.as_i64() + 1
        );
    }
    let before = catalog.reader().await.unwrap().snapshot_id();
    for sql in [
        "CREATE TABLE IF NOT EXISTS numbers AS SELECT 9 AS x",
        "DROP TABLE IF EXISTS absent",
        "CREATE DATABASE non_persistent",
    ] {
        let output = ok(&session, sql).await;
        assert!(output.observation.ddl_changes.is_empty());
        assert_eq!(catalog.reader().await.unwrap().snapshot_id(), before);
    }
    let (relation, advanced) = seed(&session, &db, "datafusion", "numbers").await;
    let before = catalog.reader().await.unwrap();
    ok(&session, "DROP TABLE numbers").await;
    let end = catalog.reader().await.unwrap().snapshot_id();
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

#[tokio::test(flavor = "multi_thread")]
async fn observed_ddl_reports_suppressed_mirroring_failure_and_partial_replacement() {
    let (_temp, session, db) = setup().await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_id();
    db.execute_unprepared("CREATE TRIGGER fail_ddl BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected DDL failure'); END").await.unwrap();
    // DataFusion suppresses provider errors under IF EXISTS; the receipt must not.
    let output = ok(&session, "DROP TABLE IF EXISTS numbers").await;
    assert!(output.observation.has_metadata_failure());
    assert!(
        matches!(&output.observation.ddl_changes[0].outcome, DdlOutcome::Failed {compensation_errors, ..} if compensation_errors.is_empty())
    );
    assert_eq!(count(&session, "numbers").await, 1);
    assert_eq!(catalog.reader().await.unwrap().snapshot_id(), before);
    db.execute_unprepared("DROP TRIGGER fail_ddl")
        .await
        .unwrap();
    let (relation, _) = seed(&session, &db, "datafusion", "numbers").await;
    let historical = catalog.reader().await.unwrap();
    let replacement = execute_observed_sql(&session, "CREATE OR REPLACE TABLE numbers AS SELECT CAST(CAST(x AS VARCHAR) || 'bad' AS BIGINT) AS x, y FROM numbers").await.unwrap();
    assert!(replacement.result.is_err());
    assert!(
        replacement
            .observation
            .ddl_changes
            .iter()
            .any(|c| c.operation == DdlOperation::DropTable && c.outcome == DdlOutcome::Confirmed)
    );
    assert!(session.table_provider("numbers").await.is_err());
    assert_eq!(
        historical
            .get_relation_statistics(relation)
            .await
            .unwrap()
            .unwrap()
            .end_snapshot,
        Some(catalog.reader().await.unwrap().snapshot_id())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_write_survives_metadata_failure_and_retries_only_invalidation() {
    let (_temp, session, db) = setup().await;
    let (relation, advanced) = seed(&session, &db, "datafusion", "numbers").await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    db.execute_unprepared("CREATE TRIGGER fail_notice BEFORE INSERT ON optd_snapshot_changes BEGIN SELECT RAISE(ABORT, 'injected notice failure'); END").await.unwrap();
    let output = ok(&session, "INSERT INTO numbers VALUES (2,2)").await;
    let InvalidationStatus::Pending { request, .. } = output.observation.invalidation else {
        panic!("expected pending invalidation");
    };
    assert_eq!(count(&session, "numbers").await, 2);
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
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
    db.execute_unprepared("DROP TRIGGER fail_notice")
        .await
        .unwrap();
    retry_invalidation(&session, &request).await.unwrap();
    assert_eq!(count(&session, "numbers").await, 2);
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
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_advanced_statistics(advanced)
            .await
            .unwrap()
            .is_none()
    );
    uninstall_persistent_catalog(&session, "datafusion").unwrap();
    install_persistent_catalog(&session, "datafusion", db.clone())
        .await
        .unwrap();
    assert!(
        retry_invalidation(&session, &request).await.is_err(),
        "do not replay against a replaced binding"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_capture_failure_does_not_block_source_write() {
    let (_temp, session, db) = setup().await;
    seed(&session, &db, "datafusion", "numbers").await;
    db.execute_unprepared("ALTER TABLE optd_snapshot RENAME TO hidden_snapshot")
        .await
        .unwrap();
    let output = ok(&session, "INSERT INTO numbers VALUES (2,2)").await;
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Unavailable { .. }
    ));
    assert_eq!(count(&session, "numbers").await, 2);
    let runner = OptdRunner::new_with_persistent_catalog(session.clone(), "datafusion", db.clone());
    runner
        .execute_sql("INSERT INTO numbers VALUES (3,3)")
        .await
        .unwrap();
    assert_eq!(count(&session, "numbers").await, 3);
    assert!(matches!(
        runner.last_observation().unwrap().invalidation,
        InvalidationStatus::Unavailable { .. }
    ));
    db.execute_unprepared("ALTER TABLE hidden_snapshot RENAME TO optd_snapshot")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_explain_is_read_only_but_analyze_executes_and_invalidates() {
    let (_temp, session, db) = setup().await;
    let (relation, _) = seed(&session, &db, "datafusion", "numbers").await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_id();
    for sql in [
        "EXPLAIN INSERT INTO numbers VALUES (2,2)",
        "EXPLAIN UPDATE numbers SET x=9",
        "EXPLAIN FORMAT TREE UPDATE numbers SET x=9",
        "EXPLAIN DELETE FROM numbers",
    ] {
        let output = ok(&session, sql).await;
        assert_eq!(output.observation.kind, StatementKind::Explain);
        assert_eq!(
            output.observation.invalidation,
            InvalidationStatus::NotApplicable
        );
        assert_eq!(count(&session, "numbers").await, 1);
        assert_eq!(catalog.reader().await.unwrap().snapshot_id(), before);
    }
    let output = ok(&session, "EXPLAIN ANALYZE INSERT INTO numbers VALUES (2,2)").await;
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Applied { .. }
    ));
    assert_eq!(count(&session, "numbers").await, 2);
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
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_prepared_dml_and_stale_plan_identity_are_handled() {
    let (_temp, session, db) = setup().await;
    let catalog = Catalog::new(db.clone());
    let (relation, _) = seed(&session, &db, "datafusion", "numbers").await;
    let before = catalog.reader().await.unwrap().snapshot_id();
    let prepared = ok(&session, "PREPARE p AS INSERT INTO numbers VALUES (2,2)").await;
    assert_eq!(
        prepared.observation.invalidation,
        InvalidationStatus::NotApplicable
    );
    assert_eq!(catalog.reader().await.unwrap().snapshot_id(), before);
    let output = ok(&session, "EXECUTE p").await;
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Applied { .. }
    ));
    assert_eq!(count(&session, "numbers").await, 2);
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
    let old_plan = session
        .state()
        .create_logical_plan("INSERT INTO numbers VALUES (3,3)")
        .await
        .unwrap();
    ok(&session, "DROP TABLE numbers").await;
    ok(
        &session,
        "CREATE TABLE numbers AS SELECT CAST(9 AS BIGINT) AS x, CAST(9 AS BIGINT) AS y",
    )
    .await;
    let (replacement, _) = seed(&session, &db, "datafusion", "numbers").await;
    assert_ne!(replacement, relation);
    let output = execute_observed_plan(&session, old_plan).await;
    assert!(output.result.is_ok());
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Unavailable { .. }
    ));
    assert_eq!(count(&session, "numbers").await, 1);
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(replacement)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_capture_rechecks_identity_after_concurrent_replacement() {
    let (_temp, session, db) = setup().await;
    seed(&session, &db, "datafusion", "numbers").await;
    let plan = session
        .state()
        .create_logical_plan("INSERT INTO numbers VALUES (2,2)")
        .await
        .unwrap();
    let gate = std::sync::Arc::new(CaptureGate::default());
    let executing = session.clone();
    let paused = std::sync::Arc::clone(&gate);
    let task = tokio::spawn(async move {
        CAPTURE_GATE
            .scope(paused, execute_observed_plan(&executing, plan))
            .await
    });
    gate.reached.notified().await;
    // First identity check saw the old provider. The metadata snapshot read next
    // will instead see this fully published/recollected replacement.
    ok(&session, "DROP TABLE numbers").await;
    ok(
        &session,
        "CREATE TABLE numbers AS SELECT CAST(9 AS BIGINT) AS x, CAST(9 AS BIGINT) AS y",
    )
    .await;
    let (replacement, advanced) = seed(&session, &db, "datafusion", "numbers").await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    gate.resume.notify_one();
    let output = task.await.unwrap();
    assert!(output.result.is_ok());
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Unavailable { .. }
    ));
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert!(
        catalog
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(replacement)
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
    assert_eq!(count(&session, "numbers").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_untracked_stale_plans_do_not_report_metadata_failure() {
    let (_temp, session, db) = setup().await;
    ok(&session, "CREATE DATABASE raw").await;
    ok(&session, "CREATE SCHEMA raw.public").await;
    ok(&session, "CREATE TABLE raw.public.numbers AS SELECT 1 AS x").await;
    let plan = session
        .state()
        .create_logical_plan("INSERT INTO raw.public.numbers VALUES (2)")
        .await
        .unwrap();
    ok(&session, "DROP TABLE raw.public.numbers").await;
    ok(&session, "CREATE TABLE raw.public.numbers AS SELECT 9 AS x").await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let output = execute_observed_plan(&session, plan).await;
    assert!(output.result.is_ok());
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Untracked { .. }
    ));
    assert!(!output.observation.has_metadata_failure());
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
    assert_eq!(count(&session, "raw.public.numbers").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_noop_and_unsupported_writes_keep_native_results() {
    let (_temp, session, db) = setup().await;
    for (sql, supported) in [
        ("UPDATE numbers SET x=9 WHERE x=999", true),
        ("DELETE FROM numbers WHERE x=999", true),
        ("INSERT OVERWRITE numbers VALUES (2,2)", false),
        ("TRUNCATE TABLE numbers", false),
    ] {
        seed(&session, &db, "datafusion", "numbers").await;
        let output = execute_observed_sql(&session, sql).await.unwrap();
        assert_eq!(output.result.is_ok(), supported, "{sql}");
        if let Err(error) = output.result {
            assert!(
                !error
                    .to_string()
                    .contains("DML is not supported for persistent table")
            );
        }
        // Without a source-specific proof of no effect, extra invalidation is safe.
        assert!(matches!(
            output.observation.invalidation,
            InvalidationStatus::Applied { .. }
        ));
        assert_eq!(count(&session, "numbers").await, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_partial_source_errors_keep_the_source_error_and_invalidate_conservatively() {
    for mutate_first in [false, true] {
        let (_temp, session, db) = setup().await;
        let inner = session.table_provider("numbers").await.unwrap();
        session
            .register_table(
                "partial",
                std::sync::Arc::new(FailingDelete {
                    inner,
                    mutate_first,
                }),
            )
            .unwrap();
        let (relation, advanced) = seed(&session, &db, "datafusion", "partial").await;
        let output = execute_observed_sql(&session, "DELETE FROM partial")
            .await
            .unwrap();
        assert!(
            output
                .result
                .err()
                .unwrap()
                .to_string()
                .contains("test provider write outcome uncertain")
        );
        assert!(matches!(
            output.observation.invalidation,
            InvalidationStatus::Applied { .. }
        ));
        assert_eq!(
            count(&session, "partial").await,
            if mutate_first { 0 } else { 1 }
        );
        let catalog = Catalog::new(db.clone());
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
        assert!(
            catalog
                .reader()
                .await
                .unwrap()
                .get_advanced_statistics(advanced)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_concurrent_writes_never_replay_source_sql() {
    let (_temp, session, db) = setup().await;
    seed(&session, &db, "datafusion", "numbers").await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_id();
    let (a, b) = tokio::join!(
        execute_observed_sql(&session, "INSERT INTO numbers VALUES (2,2)"),
        execute_observed_sql(&session, "INSERT INTO numbers VALUES (3,3)"),
    );
    for output in [a.unwrap(), b.unwrap()] {
        assert!(output.result.is_ok());
        assert!(matches!(
            output.observation.invalidation,
            InvalidationStatus::Applied { .. }
        ));
        assert!(output.observation.ddl_changes.is_empty());
    }
    assert_eq!(count(&session, "numbers").await, 3);
    assert_eq!(
        catalog.reader().await.unwrap().snapshot_id().as_i64(),
        before.as_i64() + 2
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn observed_writes_use_actual_target_catalog_and_ignore_read_sources() {
    let (_temp, session, db) = setup().await;
    let (default_relation, _) = seed(&session, &db, "datafusion", "numbers").await;
    ok(&session, "CREATE DATABASE other").await;
    install_persistent_catalog(&session, "other", db.clone())
        .await
        .unwrap();
    ok(&session, "CREATE SCHEMA other.public").await;
    ok(&session, "CREATE TABLE other.public.numbers AS SELECT CAST(2 AS BIGINT) AS x, CAST(2 AS BIGINT) AS y").await;
    let (other_relation, _) = seed(&session, &db, "other", "numbers").await;
    let output = ok(
        &session,
        "INSERT INTO other.public.numbers SELECT * FROM datafusion.public.numbers",
    )
    .await;
    let InvalidationStatus::Applied { request, .. } = output.observation.invalidation else {
        panic!("expected observed target");
    };
    assert_eq!(request.catalog_name, "other");
    assert_eq!(request.relation_ids, vec![other_relation]);
    assert!(
        Catalog::new(db.clone())
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(default_relation)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        catalog_for_name(db.clone(), "other")
            .await
            .unwrap()
            .reader()
            .await
            .unwrap()
            .get_relation_statistics(other_relation)
            .await
            .unwrap()
            .is_none()
    );
    ok(&session, "CREATE DATABASE raw").await;
    ok(&session, "CREATE SCHEMA raw.public").await;
    ok(&session, "CREATE TABLE raw.public.numbers AS SELECT 1 AS x").await;
    let output = ok(&session, "INSERT INTO raw.public.numbers VALUES (2)").await;
    assert!(matches!(
        output.observation.invalidation,
        InvalidationStatus::Untracked { .. }
    ));
}
