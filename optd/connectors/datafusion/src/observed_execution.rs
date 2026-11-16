//! Shared, opt-in SQL observation for source DML and write-through DDL.
//!
//! The source result and metadata-maintenance status are deliberately separate.
//! Raw SessionContext/provider calls outside this helper remain unobserved for DML.
use std::cell::RefCell;

use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    error::{DataFusionError, Result as DFResult},
    execution::session_state::SessionStateBuilder,
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};
use optd_catalog::{
    Catalog, CatalogError, SnapshotId,
    statistics::{InvalidateStatisticsInfo, StatisticsInvalidationCause},
};

use crate::persistent_catalog::{PersistentBinding, persistent_binding};

/// An actual successful write-through callback, not an inferred SQL-text event.
/// Idempotent imports can confirm existing metadata without creating a new version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlChange {
    pub catalog_id: i64,
    pub operation: DdlOperation,
    pub outcome: DdlOutcome,
    pub schema_name: Option<String>,
    pub object_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlOutcome {
    Confirmed,
    Noop,
    Failed {
        error: String,
        compensation_errors: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlOperation {
    CreateSchema,
    DropSchema,
    CreateTable,
    DropTable,
}

tokio::task_local! {
    static DDL_CHANGES: RefCell<Vec<DdlChange>>;
}

pub(crate) fn record_ddl_change(change: DdlChange) {
    let _ = DDL_CHANGES.try_with(|changes| changes.borrow_mut().push(change));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementKind {
    Query,
    Explain,
    Dml { operation: String },
    Ddl { operation: String },
    Other,
}

/// A replayable metadata notification: retry this request, NEVER the source SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidationRequest {
    pub catalog_name: String,
    pub catalog_id: i64,
    /// Session-local installation identity; prevents replay against a replaced database binding.
    pub binding_id: u64,
    pub relation_ids: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidationStatus {
    NotApplicable,
    /// Target has no installed persistent binding or no durable relation registration.
    Untracked {
        target: String,
    },
    Applied {
        request: InvalidationRequest,
        snapshot_id: SnapshotId,
    },
    /// Source execution already ran; retry only the metadata request.
    Pending {
        request: InvalidationRequest,
        error: String,
    },
    /// Capture failed before source execution. No stable ID is safe to infer afterward.
    Unavailable {
        target: String,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementObservation {
    pub kind: StatementKind,
    /// Actual provider callbacks; empty for untracked/unsupported/no-op DDL.
    /// DDL is not replayed by the observer: existing wrappers own its persistence.
    pub ddl_changes: Vec<DdlChange>,
    pub invalidation: InvalidationStatus,
}

impl StatementObservation {
    pub fn has_pending_invalidation(&self) -> bool {
        matches!(
            self.invalidation,
            InvalidationStatus::Pending { .. } | InvalidationStatus::Unavailable { .. }
        )
    }
    pub fn has_metadata_failure(&self) -> bool {
        self.has_pending_invalidation()
            || self
                .ddl_changes
                .iter()
                .any(|change| matches!(change.outcome, DdlOutcome::Failed { .. }))
    }
}

pub struct ExecutedStatement {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Source failure can coexist with successful conservative invalidation (partial or
/// uncertain writes). Source success can coexist with pending metadata maintenance.
pub struct ObservedExecution {
    pub result: DFResult<ExecutedStatement>,
    pub observation: StatementObservation,
}

/// Parse once, then execute and observe both DML and DDL. Planning errors precede
/// execution and are returned directly. This helper does not infer source transaction
/// commits; source-specific transactions require an explicit committed-write notification.
pub async fn execute_observed_sql(
    session: &SessionContext,
    sql: &str,
) -> DFResult<ObservedExecution> {
    let plan = session.state().create_logical_plan(sql).await?;
    Ok(execute_observed_plan(session, plan).await)
}

fn effect_plan(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Analyze(analyze) => effect_plan(&analyze.input),
        _ => plan,
    }
}

fn statement_kind(plan: &LogicalPlan) -> StatementKind {
    match effect_plan(plan) {
        LogicalPlan::Dml(dml) => StatementKind::Dml {
            operation: dml.op.name().into(),
        },
        LogicalPlan::Ddl(ddl) => StatementKind::Ddl {
            operation: ddl.name().into(),
        },
        LogicalPlan::Explain(_) => StatementKind::Explain,
        LogicalPlan::Statement(_) | LogicalPlan::Copy(_) => StatementKind::Other,
        _ => StatementKind::Query,
    }
}

struct Target {
    binding: PersistentBinding,
    request: InvalidationRequest,
}

async fn capture_target(
    session: &SessionContext,
    dml: &datafusion::logical_expr::DmlStatement,
) -> Result<Option<Target>, DataFusionError> {
    let target = &dml.table_name;
    let config = session.copied_config();
    let defaults = &config.options().catalog;
    let resolved = target
        .clone()
        .resolve(&defaults.default_catalog, &defaults.default_schema);
    let Some(binding) = persistent_binding(session, &resolved.catalog) else {
        return Ok(None);
    };
    // Untracked sources need neither provider-identity checks nor metadata I/O.
    let captured_provider = datafusion::datasource::source_as_provider(&dml.target)?;
    let current_provider = session.table_provider(target.clone()).await?;
    if !std::sync::Arc::ptr_eq(&captured_provider, &current_provider) {
        return Err(stale_provider());
    }
    #[cfg(test)]
    tests::after_identity_check().await;
    let catalog = Catalog::on_catalog(binding.db.clone(), binding.catalog_id);
    let reader = catalog.reader().await.map_err(external)?;
    let Some(schema) = reader
        .get_all_schemas()
        .await
        .map_err(external)?
        .into_iter()
        .find(|s| s.schema_name == resolved.schema.as_ref())
    else {
        return Ok(None);
    };
    let Some(relation) = reader
        .get_all_relations()
        .await
        .map_err(external)?
        .into_iter()
        .find(|r| r.schema_id == schema.schema_id && r.relation_name == resolved.table.as_ref())
    else {
        return Ok(None);
    };
    // Metadata I/O yielded: DROP/recreate/recollection or reinstallation may
    // have happened since the first check. Never attach old-plan writes to a
    // replacement's durable ID, even if the replacement now has collected stats.
    let current_provider = session.table_provider(target.clone()).await?;
    if !std::sync::Arc::ptr_eq(&captured_provider, &current_provider) {
        return Err(stale_provider());
    }
    if persistent_binding(session, &resolved.catalog)
        .is_none_or(|current| current.binding_id != binding.binding_id)
    {
        return Err(DataFusionError::Execution(
            "persistent binding changed during target capture".into(),
        ));
    }
    let request = InvalidationRequest {
        catalog_name: resolved.catalog.to_string(),
        catalog_id: binding.catalog_id,
        binding_id: binding.binding_id,
        relation_ids: vec![relation.relation_id],
    };
    Ok(Some(Target { binding, request }))
}

fn stale_provider() -> DataFusionError {
    DataFusionError::Execution("DML plan refers to an old/replaced source provider; cannot safely infer its original durable relation ID".into())
}

/// Explicitly retry a pending metadata notification against the same installed binding.
/// Bounded stale-base retries never rerun SQL. Uncertain commit errors remain errors.
pub async fn retry_invalidation(
    session: &SessionContext,
    request: &InvalidationRequest,
) -> DFResult<SnapshotId> {
    let binding = persistent_binding(session, &request.catalog_name).ok_or_else(|| DataFusionError::Execution("persistent binding was removed; invalidate explicitly against its original metadata database".into()))?;
    if binding.catalog_id != request.catalog_id || binding.binding_id != request.binding_id {
        return Err(DataFusionError::Execution(
            "persistent catalog identity changed".into(),
        ));
    }
    apply_invalidation(&binding, request)
        .await
        .map_err(external)
}

async fn apply_invalidation(
    binding: &PersistentBinding,
    request: &InvalidationRequest,
) -> Result<SnapshotId, CatalogError> {
    let catalog = Catalog::on_catalog(binding.db.clone(), binding.catalog_id);
    for _ in 0..3 {
        let snapshot = catalog.reader().await?.snapshot_id();
        match catalog
            .invalidate_statistics(
                snapshot,
                InvalidateStatisticsInfo {
                    relation_ids: request.relation_ids.clone(),
                    cause: StatisticsInvalidationCause::ObservedSql,
                },
            )
            .await
        {
            Err(CatalogError::StaleSnapshot { .. }) => continue,
            result => return result,
        }
    }
    Err(CatalogError::Database {
        source: sea_orm::DbErr::Custom(
            "invalidation exhausted three stale-base attempts; retry metadata only".into(),
        ),
    })
}

/// Execute an already resolved plan. DML targets are captured before source effects;
/// all source execution errors are conservatively treated as potentially partial writes.
/// DDL effects come from the real write-through callbacks and are never double-published.
pub async fn execute_observed_plan(
    session: &SessionContext,
    plan: LogicalPlan,
) -> ObservedExecution {
    let original_kind = statement_kind(&plan);
    let (result, capture, kind, ddl_changes) = DDL_CHANGES
        .scope(RefCell::new(vec![]), async {
            let (result, capture, kind) = execute_source(session, plan, original_kind).await;
            let changes = DDL_CHANGES.with(|changes| std::mem::take(&mut *changes.borrow_mut()));
            (result, capture, kind, changes)
        })
        .await;
    let invalidation = match capture {
        None => InvalidationStatus::NotApplicable,
        Some((target, Ok(None))) => InvalidationStatus::Untracked { target },
        Some((target, Err(error))) => InvalidationStatus::Unavailable {
            target,
            error: error.to_string(),
        },
        Some((_, Ok(Some(target)))) => {
            match apply_invalidation(&target.binding, &target.request).await {
                Ok(snapshot_id) => InvalidationStatus::Applied {
                    request: target.request,
                    snapshot_id,
                },
                Err(error) => InvalidationStatus::Pending {
                    request: target.request,
                    error: error.to_string(),
                },
            }
        }
    };
    ObservedExecution {
        result,
        observation: StatementObservation {
            kind,
            ddl_changes,
            invalidation,
        },
    }
}

type CapturedTarget = Option<(String, Result<Option<Target>, DataFusionError>)>;

async fn execute_source(
    session: &SessionContext,
    plan: LogicalPlan,
    original_kind: StatementKind,
) -> (DFResult<ExecutedStatement>, CapturedTarget, StatementKind) {
    // In DF 53 MemTable UPDATE/DELETE may mutate inside physical planning. For
    // EXPLAIN of mutations, only explain the logical plan; never invoke those hooks.
    let df = match session.execute_logical_plan(plan).await {
        Ok(df) => df,
        Err(error) => return (Err(error), None, original_kind),
    };
    let df = if let LogicalPlan::Explain(explain) = df.logical_plan() {
        if matches!(
            effect_plan(&explain.plan),
            LogicalPlan::Dml(_) | LogicalPlan::Ddl(_)
        ) {
            let config = session
                .copied_config()
                .set_bool("datafusion.explain.logical_plan_only", true)
                .set_bool("datafusion.explain.physical_plan_only", false)
                // with_config must not undo new_from_existing's protection and
                // recreate the default catalog in the shared registry.
                .with_create_default_catalog_and_schema(false);
            let state = SessionStateBuilder::new_from_existing(session.state())
                .with_config(config)
                .build();
            let mut logical_only = df.logical_plan().clone();
            if let LogicalPlan::Explain(explain) = &mut logical_only {
                // TREE bypasses logical_plan_only in DF's planner; force the safe
                // logical indent renderer for every mutation EXPLAIN format.
                explain.explain_format = datafusion::common::format::ExplainFormat::Indent;
            }
            datafusion::dataframe::DataFrame::new(state, logical_only)
        } else {
            df
        }
    } else {
        df
    };
    // EXECUTE expands a prepared plan here. Capture the actual provider/target
    // before collection can invoke eager UPDATE/DELETE or a lazy INSERT sink.
    let expanded = df.logical_plan();
    let expanded_kind = statement_kind(expanded);
    let kind = if matches!(original_kind, StatementKind::Other)
        && matches!(
            expanded_kind,
            StatementKind::Dml { .. } | StatementKind::Ddl { .. } | StatementKind::Explain
        ) {
        expanded_kind
    } else {
        original_kind
    };
    let capture = match effect_plan(expanded) {
        LogicalPlan::Dml(dml) => Some((
            dml.table_name.to_string(),
            capture_target(session, dml).await,
        )),
        _ => None,
    };
    let schema = df.schema().inner().clone();
    let result = df.collect().await.map(|batches| {
        let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
        ExecutedStatement { schema, batches }
    });
    (result, capture, kind)
}

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
