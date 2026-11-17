use std::{
    any::Any,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use datafusion::{
    arrow::array::Int64Array,
    catalog::{SchemaProvider, TableProvider},
    error::Result as DFResult,
    prelude::SessionContext,
};
use optd_catalog::{
    Catalog,
    query_history::{InstanceFilter, QueryHistoryStore, QueryStatus},
};
use optd_datafusion::{
    persistent_catalog::install_persistent_catalog,
    query_history::{CapturePolicy, QueryHistoryOptions},
    runner::{OptdRunner, RunnerOutput},
    setup::session_context_with_information_schema,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};

async fn setup(
    options: Option<QueryHistoryOptions>,
) -> (OptdRunner, DatabaseConnection, tempfile::TempDir) {
    setup_with_session(session_context_with_information_schema(), options).await
}

async fn setup_with_session(
    session: SessionContext,
    options: Option<QueryHistoryOptions>,
) -> (OptdRunner, DatabaseConnection, tempfile::TempDir) {
    // Provider callbacks can cross runtime boundaries; keep a real temporary file
    // alive rather than relying on in-memory connection lifetime.
    let directory = tempfile::tempdir().unwrap();
    let options_db = sea_orm::sqlx::sqlite::SqliteConnectOptions::new()
        .filename(directory.path().join("history.sqlite"))
        .create_if_missing(true);
    let pool = sea_orm::sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options_db)
        .await
        .unwrap();
    let db = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
    install_persistent_catalog(&session, "datafusion", db.clone())
        .await
        .unwrap();
    session
        .sql("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2)) AS v(a)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let runner = OptdRunner::new_with_persistent_catalog(session, "datafusion", db.clone());
    let runner = if let Some(options) = options {
        runner.with_query_history(options).unwrap()
    } else {
        runner
    };
    (runner, db, directory)
}
fn rows(output: RunnerOutput) -> Vec<i64> {
    let RunnerOutput::Rows { batches, .. } = output else {
        panic!("expected rows");
    };
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn history_groups_queries_without_rewriting_results_or_snapshots() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_info().clone();
    let a = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a = 1")
        .await;
    let b = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a = 2")
        .await;
    assert_eq!(rows(a.result.unwrap()), vec![1]);
    assert_eq!(rows(b.result.unwrap()), vec![2]);
    let (a, b) = (a.history.unwrap(), b.history.unwrap());
    assert!(a.begin_acknowledged && a.completion_acknowledged && a.issues.is_empty());
    assert_eq!(a.query_id, b.query_id);
    assert_ne!(a.instance_id, b.instance_id);
    let store = QueryHistoryStore::new(db);
    let instance = store.get_instance(a.instance_id).await.unwrap().unwrap();
    assert_eq!(instance.status, QueryStatus::Optimized);
    assert_eq!(
        instance.optimizer_configuration["statistics_catalog"],
        "datafusion"
    );
    assert_eq!(instance.catalog_snapshot_id, Some(before.snapshot_id));
    assert!(instance.request.get("sql").is_none());
    assert!(instance.request.get("parameters").is_none());
    assert_eq!(catalog.reader().await.unwrap().snapshot_info(), &before);
}

#[tokio::test]
async fn disabled_history_does_not_write_and_runtime_only_cannot_enable_it() {
    let (runner, db, _directory) = setup(None).await;
    let result = runner.execute_sql_with_history("SELECT a FROM t").await;
    assert!(result.result.is_ok());
    assert!(result.history.is_none());
    assert!(
        QueryHistoryStore::new(db)
            .list_instances(InstanceFilter::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        OptdRunner::new(SessionContext::new())
            .with_query_history(QueryHistoryOptions::default())
            .is_err()
    );
}

#[tokio::test]
async fn planning_errors_and_fallback_have_one_instance_and_distinct_outcomes() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    let store = QueryHistoryStore::new(db);
    let invalid = runner.execute_sql_with_history("SELECT FROM").await;
    assert!(invalid.result.is_err());
    let invalid = store
        .get_instance(invalid.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invalid.status, QueryStatus::Failed);
    assert!(invalid.query_id.is_none());
    assert!(invalid.catalog_snapshot_id.is_none());
    assert!(invalid.outcome_details.unwrap().get("error").is_none());
    let ddl = runner
        .execute_sql_with_history("CREATE TABLE written AS SELECT 7 AS a")
        .await;
    assert!(ddl.result.is_ok());
    let ddl = store
        .get_instance(ddl.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ddl.status, QueryStatus::Bypassed);
    assert!(ddl.catalog_snapshot_id.is_none());
    assert_eq!(
        store
            .list_instances(InstanceFilter::default())
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        rows(runner.execute_sql("SELECT a FROM written").await.unwrap()),
        vec![7]
    );
}

#[tokio::test]
async fn full_capture_is_explicit_and_set_reset_configuration_is_live() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions {
        capture: CapturePolicy::Full,
        ..Default::default()
    }))
    .await;
    runner
        .execute_sql("SET optd.optd_enabled = false")
        .await
        .unwrap();
    let bypass = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a = 2")
        .await;
    assert_eq!(rows(bypass.result.unwrap()), vec![2]);
    let store = QueryHistoryStore::new(db);
    let instance = store
        .get_instance(bypass.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.status, QueryStatus::Bypassed);
    assert_eq!(instance.optimizer_configuration["optd_enabled"], false);
    assert_eq!(instance.request["sql"], "SELECT a FROM t WHERE a = 2");
    assert_eq!(instance.request["parameters"], serde_json::json!(["2"]));
    // DataFusion 53 RESET supports builtin settings, not extension namespaces.
    runner
        .execute_sql("SET optd.optd_enabled = true")
        .await
        .unwrap();
    let planned = runner.execute_sql_with_history("SELECT a FROM t").await;
    assert!(planned.result.is_ok());
    let instance = store
        .get_instance(planned.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.status, QueryStatus::Optimized);
    assert_eq!(instance.optimizer_configuration["optd_enabled"], true);
    let default_partitions = instance.optimizer_configuration["target_partitions"].clone();
    runner
        .execute_sql("SET datafusion.execution.target_partitions = 7")
        .await
        .unwrap();
    let changed = runner.execute_sql_with_history("SELECT a FROM t").await;
    assert!(changed.result.is_ok());
    let changed = store
        .get_instance(changed.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(changed.optimizer_configuration["target_partitions"], 7);
    runner
        .execute_sql("RESET datafusion.execution.target_partitions")
        .await
        .unwrap();
    let reset = runner.execute_sql_with_history("SELECT a FROM t").await;
    assert!(reset.result.is_ok());
    let reset = store
        .get_instance(reset.history.unwrap().instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reset.optimizer_configuration["target_partitions"],
        default_partitions
    );
}

#[tokio::test]
async fn history_database_failure_does_not_change_results_or_replay_writes() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    db.execute_unprepared("DROP TABLE optd_query_instance")
        .await
        .unwrap();
    assert_eq!(
        rows(
            runner
                .execute_sql_with_history("SELECT a FROM t WHERE a = 1")
                .await
                .result
                .unwrap()
        ),
        vec![1]
    );
    let ddl = runner
        .execute_sql_with_history("CREATE TABLE once_only AS SELECT 9 AS a")
        .await;
    assert!(ddl.result.is_ok());
    assert!(!ddl.history.unwrap().issues.is_empty());
    assert_eq!(
        rows(runner.execute_sql("SELECT a FROM once_only").await.unwrap()),
        vec![9]
    );
    assert!(
        runner
            .execute_sql("SELECT definitely_missing FROM t")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_calls_have_independent_reports_and_ids() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions {
        persistence_timeout: Duration::from_secs(5),
        ..Default::default()
    }))
    .await;
    let (a, b) = tokio::join!(
        runner.execute_sql_with_history("SELECT a FROM t WHERE a = 1"),
        runner.execute_sql_with_history("SELECT a FROM t WHERE a = 2"),
    );
    assert_eq!(rows(a.result.unwrap()), vec![1]);
    assert_eq!(rows(b.result.unwrap()), vec![2]);
    let (a, b) = (a.history.unwrap(), b.history.unwrap());
    assert_ne!(a.instance_id, b.instance_id);
    assert_eq!(a.query_id, b.query_id);
    assert!(a.issues.is_empty() && b.issues.is_empty());
    assert_eq!(
        QueryHistoryStore::new(db)
            .list_instances(InstanceFilter::default())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn optimization_success_is_not_execution_success() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    let execution = runner.execute_sql_with_history("SELECT a / 0 FROM t").await;
    assert!(execution.result.is_err());
    let report = execution.history.unwrap();
    assert!(report.completion_acknowledged);
    let instance = QueryHistoryStore::new(db)
        .get_instance(report.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.status, QueryStatus::Optimized);
}

#[tokio::test]
async fn completion_failure_is_reported_without_changing_query_results() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    db.execute_unprepared("CREATE TRIGGER fail_history_finish BEFORE UPDATE OF status ON optd_query_instance BEGIN SELECT RAISE(ABORT, 'injected history failure'); END").await.unwrap();
    let execution = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a = 1")
        .await;
    assert_eq!(rows(execution.result.unwrap()), vec![1]);
    let report = execution.history.unwrap();
    assert!(report.begin_acknowledged);
    assert!(!report.completion_acknowledged);
    assert!(
        report
            .issues
            .contains(&optd_datafusion::query_history::HistoryIssue::FinishFailed)
    );
    let instance = QueryHistoryStore::new(db)
        .get_instance(report.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.status, QueryStatus::Running);
}

#[derive(Debug)]
struct GatedSchema {
    inner: Arc<dyn SchemaProvider>,
    armed: AtomicBool,
    lookups: AtomicUsize,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl SchemaProvider for GatedSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Parsing resolves the first lookup; the persistent builder has pinned its
        // reader before the second lookup. Publication then races the history write.
        if self.armed.load(Ordering::Acquire) && self.lookups.fetch_add(1, Ordering::AcqRel) == 1 {
            self.reached.notify_one();
            self.release.notified().await;
        }
        self.inner.table(name).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_records_the_consumed_snapshot_not_a_later_publication() {
    let session = session_context_with_information_schema();
    let catalog_provider = session.catalog("datafusion").unwrap();
    let gate = Arc::new(GatedSchema {
        inner: catalog_provider.schema("public").unwrap(),
        armed: AtomicBool::new(false),
        lookups: AtomicUsize::new(0),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    catalog_provider
        .register_schema("public", gate.clone())
        .unwrap();
    let (runner, db, _directory) = setup_with_session(
        session,
        Some(QueryHistoryOptions {
            persistence_timeout: Duration::from_secs(5),
            ..Default::default()
        }),
    )
    .await;
    let catalog = Catalog::new(db.clone());
    let before = catalog.reader().await.unwrap().snapshot_id();
    gate.armed.store(true, Ordering::Release);
    let runner = Arc::new(runner);
    let task = tokio::spawn({
        let runner = runner.clone();
        async move {
            runner
                .execute_sql_with_history("SELECT a FROM t WHERE a = 1")
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
        .await
        .unwrap();
    catalog
        .create_schema(
            before,
            optd_catalog::schema::CreateSchemaInfo {
                schema_name: "after_pin".into(),
            },
        )
        .await
        .unwrap();
    assert_ne!(catalog.reader().await.unwrap().snapshot_id(), before);
    gate.release.notify_one();
    let execution = task.await.unwrap();
    assert_eq!(rows(execution.result.unwrap()), vec![1]);
    let report = execution.history.unwrap();
    assert!(report.issues.is_empty());
    let instance = QueryHistoryStore::new(db)
        .get_instance(report.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance.catalog_snapshot_id, Some(before));
}

#[tokio::test]
async fn default_schema_changes_separate_templates_and_reset_restores_identity() {
    let (runner, db, _directory) = setup(Some(QueryHistoryOptions::default())).await;
    let first = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a > 0")
        .await;
    assert!(first.result.is_ok());
    runner.execute_sql("CREATE SCHEMA analytics").await.unwrap();
    runner
        .execute_sql("CREATE TABLE analytics.t AS SELECT 9 AS a")
        .await
        .unwrap();
    runner
        .execute_sql("SET datafusion.catalog.default_schema = 'analytics'")
        .await
        .unwrap();
    let other = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a > 0")
        .await;
    assert_eq!(rows(other.result.unwrap()), vec![9]);
    assert_ne!(
        first.history.as_ref().unwrap().query_id,
        other.history.unwrap().query_id
    );
    runner
        .execute_sql("RESET datafusion.catalog.default_schema")
        .await
        .unwrap();
    let reset = runner
        .execute_sql_with_history("SELECT a FROM t WHERE a > 0")
        .await;
    assert_eq!(rows(reset.result.unwrap()), vec![1, 2]);
    assert_eq!(
        first.history.unwrap().query_id,
        reset.history.unwrap().query_id
    );
    assert!(
        !QueryHistoryStore::new(db)
            .list_instances(InstanceFilter::default())
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn cli_history_flags_require_explicit_catalog_and_capture_opt_in() {
    let output = Command::new(env!("CARGO_BIN_EXE_optd-cli"))
        .args(["--query-history", "-c", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let output = Command::new(env!("CARGO_BIN_EXE_optd-cli"))
        .args(["--capture-query-input", "-c", "SELECT 1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}

#[tokio::test]
async fn cli_stores_history_at_the_literal_catalog_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("catalog%2Edb");
    let neighbor = dir.path().join("catalog.db");
    std::fs::write(&neighbor, []).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_optd-cli"))
        .args(["--query-history", "--no-log-explain-steps", "--catalog-db"])
        .arg(&path)
        .args(["-c", "SELECT 1 AS a"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("optd query instance:"));
    assert!(std::fs::read(neighbor).unwrap().is_empty());
    let options = sea_orm::sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
    let pool = sea_orm::sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let db = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
    let instances = QueryHistoryStore::new(db.clone())
        .list_instances(InstanceFilter::default())
        .await
        .unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].status, QueryStatus::Optimized);
    assert!(instances[0].request.get("sql").is_none());
    db.close().await.unwrap();
}
