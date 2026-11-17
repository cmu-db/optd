//! Per-request persistence; no shared last-query field and no source-SQL retries.
use std::{
    future::Future,
    time::{Duration, Instant},
};

use datafusion::execution::session_state::SessionState;
use optd_catalog::{
    Catalog, SnapshotId,
    query_history::{
        BeginQueryInstance, FinishQueryInstance, HistoryError, QueryHistoryStore, QueryId,
        QueryInstanceId, QueryStatus,
    },
};
use sea_orm::DatabaseConnection;
use serde_json::{Value, json};

use super::normalize_sql;

/// Raw capture is deliberately separate from enabling history. Templates still
/// contain identifiers and retained literals: template-only is not anonymization.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CapturePolicy {
    #[default]
    TemplateOnly,
    Full,
}

/// Opt-in history settings. Used with a persistent-catalog runner so snapshot
/// references cannot accidentally point into a different metadata database.
#[derive(Debug, Clone)]
pub struct QueryHistoryOptions {
    pub capture: CapturePolicy,
    /// Deadline per persistence operation; timeout can leave an unacknowledged row.
    pub persistence_timeout: Duration,
    /// Caller-supplied build revision; absence is recorded as unknown.
    pub build_revision: Option<String>,
}
impl Default for QueryHistoryOptions {
    fn default() -> Self {
        Self {
            capture: CapturePolicy::TemplateOnly,
            persistence_timeout: Duration::from_secs(1),
            build_revision: None,
        }
    }
}

/// Safe, non-payload diagnostics. They must not replace the query's own result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryIssue {
    ResolutionUnavailable,
    BeginFailed,
    SnapshotFailed,
    FinishFailed,
    FingerprintCollision,
    Timeout,
}

/// Per-call acknowledgment, not a guarantee about rows after an I/O timeout.
#[derive(Debug, Clone)]
pub struct QueryHistoryReport {
    pub instance_id: QueryInstanceId,
    pub query_id: Option<QueryId>,
    pub begin_acknowledged: bool,
    pub completion_acknowledged: bool,
    pub issues: Vec<HistoryIssue>,
}

pub(crate) struct QueryHistory {
    db: DatabaseConnection,
    store: QueryHistoryStore,
    options: QueryHistoryOptions,
    session_scope: QueryInstanceId,
}
impl QueryHistory {
    pub(crate) fn new(db: DatabaseConnection, options: QueryHistoryOptions) -> Self {
        Self {
            store: QueryHistoryStore::new(db.clone()),
            db,
            options,
            session_scope: QueryInstanceId::new(),
        }
    }
    async fn bounded<T>(
        &self,
        future: impl Future<Output = Result<T, HistoryError>>,
    ) -> Result<T, HistoryIssue> {
        match tokio::time::timeout(self.options.persistence_timeout, future).await {
            Err(_) => Err(HistoryIssue::Timeout),
            Ok(Err(HistoryError::FingerprintCollision)) => Err(HistoryIssue::FingerprintCollision),
            Ok(Err(_)) => Err(HistoryIssue::BeginFailed),
            Ok(Ok(value)) => Ok(value),
        }
    }
    async fn scope(&self, state: &SessionState) -> Result<Value, HistoryIssue> {
        let defaults = &state.config_options().catalog;
        let lookup =
            async {
                let catalog = Catalog::new(self.db.clone());
                let reader = catalog.reader().await?;
                let catalogs = reader.get_catalogs().await?;
                let Some(registration) = catalogs
                    .iter()
                    .find(|c| c.catalog_name == defaults.default_catalog)
                else {
                    return Ok::<_, optd_catalog::CatalogError>(None);
                };
                let scoped = Catalog::on_catalog(self.db.clone(), registration.catalog_id);
                let scoped_reader = scoped.reader_at(reader.snapshot_id()).await?;
                let schemas = scoped_reader.get_all_schemas().await?;
                Ok(schemas.into_iter().find(|s| s.schema_name == defaults.default_schema)
                .map(|s| json!({"catalog_id": registration.catalog_id, "schema_id": s.schema_id})))
            };
        let identity = match tokio::time::timeout(self.options.persistence_timeout, lookup).await {
            Ok(Ok(Some(identity))) => identity,
            Ok(Ok(None)) => json!({"session_scope": self.session_scope}),
            _ => return Err(HistoryIssue::ResolutionUnavailable),
        };
        let parser: std::collections::BTreeMap<_, _> = state
            .config_options()
            .entries()
            .into_iter()
            .filter(|entry| entry.key.starts_with("datafusion.sql_parser."))
            .map(|entry| (entry.key, entry.value))
            .collect();
        Ok(
            json!({"namespace": identity, "default_catalog": defaults.default_catalog,
            "default_schema": defaults.default_schema, "parser": parser}),
        )
    }

    pub(crate) async fn begin<'a>(
        &'a self,
        state: &SessionState,
        sql: &str,
        configuration: Value,
    ) -> RequestHistory<'a> {
        let mut request = RequestHistory {
            history: self,
            started: Instant::now(),
            report: QueryHistoryReport {
                instance_id: QueryInstanceId::new(),
                query_id: None,
                begin_acknowledged: false,
                completion_acknowledged: false,
                issues: vec![],
            },
        };
        let scope = match self.scope(state).await {
            Ok(scope) => Some(scope),
            Err(issue) => {
                request.report.issues.push(issue);
                None
            }
        };
        let normalized = scope
            .as_ref()
            .and_then(|scope| normalize_sql(state, sql, scope.clone()).ok().flatten());
        let mut payload = json!({"input_kind": "sql", "capture": "template_only"});
        if self.options.capture == CapturePolicy::Full {
            payload = json!({"input_kind": "sql", "capture": "full", "sql": sql,
                "parameters": normalized.as_ref().map(|n| &n.parameters)});
        }
        let mut info = BeginQueryInstance {
            instance_id: request.report.instance_id,
            template: normalized.map(|n| n.template),
            request: payload,
            resolution_context: scope.unwrap_or(Value::Null),
            optimizer_configuration: configuration,
            implementation_descriptor: json!({"crate_version": env!("CARGO_PKG_VERSION"),
                "build_revision": self.options.build_revision}),
        };
        let mut result = self.bounded(self.store.begin(info.clone())).await;
        if matches!(result, Err(HistoryIssue::FingerprintCollision)) {
            request
                .report
                .issues
                .push(HistoryIssue::FingerprintCollision);
            info.template = None;
            // Only history is retried, after a confirmed collision rollback.
            result = self.bounded(self.store.begin(info)).await;
        }
        match result {
            Ok(instance) => {
                request.report.begin_acknowledged = true;
                request.report.query_id = instance.query_id;
            }
            Err(issue) => request.report.issues.push(issue),
        }
        request
    }
}

pub(crate) struct RequestHistory<'a> {
    history: &'a QueryHistory,
    started: Instant,
    report: QueryHistoryReport,
}
impl RequestHistory<'_> {
    pub(crate) async fn attach_snapshot(&mut self, snapshot: SnapshotId) {
        if self.report.begin_acknowledged
            && let Err(issue) = self
                .history
                .bounded(
                    self.history
                        .store
                        .attach_snapshot(self.report.instance_id, snapshot),
                )
                .await
        {
            self.report.issues.push(if issue == HistoryIssue::Timeout {
                issue
            } else {
                HistoryIssue::SnapshotFailed
            });
        }
    }
    pub(crate) async fn finish(&mut self, status: QueryStatus, reason: &str, error: Option<&str>) {
        if !self.report.begin_acknowledged {
            return;
        }
        let mut details = json!({"reason": reason});
        if self.history.options.capture == CapturePolicy::Full
            && let Some(error) = error
        {
            details["error"] = json!(error);
        }
        let completion = FinishQueryInstance {
            status,
            optimization_duration_ns: i64::try_from(self.started.elapsed().as_nanos())
                .unwrap_or(i64::MAX),
            outcome_details: details,
        };
        match self
            .history
            .bounded(
                self.history
                    .store
                    .finish(self.report.instance_id, completion),
            )
            .await
        {
            Ok(_) => self.report.completion_acknowledged = true,
            Err(issue) => self.report.issues.push(if issue == HistoryIssue::Timeout {
                issue
            } else {
                HistoryIssue::FinishFailed
            }),
        }
    }
    pub(crate) fn into_report(self) -> QueryHistoryReport {
        self.report
    }
}
