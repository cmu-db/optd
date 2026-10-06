//! SQL helpers for collecting persistent statistics and inspecting cardinality estimates.

use std::{future::Future, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float64Array, StringArray},
        datatypes::DataType,
    },
    common::{ScalarValue as DFScalarValue, TableReference},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    prelude::SessionContext,
};
use optd_catalog::{
    Catalog as PersistentCatalog,
    relation::RelationInfo,
    statistics::{
        ColumnStatistics as PersistentColumnStatistics, SetRelationStatisticsInfo, StatisticsValue,
    },
};
use optd_core::{
    CardinalityEstimationV1, Catalog as CoreCatalog, ColumnStatistics, MemoryCatalog, ScalarValue,
    TableStatistics,
};
use sea_orm::DatabaseConnection;

use crate::{
    runner::{default_pass_manager, optimizer_context_from_logical_plan_with_catalog},
    runtime_statistics::{referenced_scan_columns, table_ref_from_df},
    statistics::collect_table_statistics_for_table_ref,
};

pub(crate) const CARDEST_UDF: &str = "cardest";
pub(crate) const COLLECT_STATS_UDF: &str = "collect_stats";

pub(crate) fn register_statistics_udfs(ctx: &SessionContext, db: DatabaseConnection) {
    let state = Arc::new(ctx.state());
    ctx.register_udf(ScalarUDF::from(CardinalityEstimateUdf {
        state: IgnoredForEq(Arc::clone(&state)),
        db: IgnoredForEq(db.clone()),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    }));
    ctx.register_udf(ScalarUDF::from(CollectStatisticsUdf {
        state: IgnoredForEq(state),
        db: IgnoredForEq(db),
        signature: Signature::exact(vec![DataType::Utf8], Volatility::Volatile),
    }));
}

#[derive(Debug, Clone)]
struct IgnoredForEq<T>(T);

impl<T> PartialEq for IgnoredForEq<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl<T> Eq for IgnoredForEq<T> {}
impl<T> std::hash::Hash for IgnoredForEq<T> {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CardinalityEstimateUdf {
    state: IgnoredForEq<Arc<datafusion::execution::SessionState>>,
    db: IgnoredForEq<DatabaseConnection>,
    signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CollectStatisticsUdf {
    state: IgnoredForEq<Arc<datafusion::execution::SessionState>>,
    db: IgnoredForEq<DatabaseConnection>,
    signature: Signature,
}

impl ScalarUDFImpl for CardinalityEstimateUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        CARDEST_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let sql_array = string_array_arg(&args.args[0])?;
        let sql = sql_array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string argument type was checked");
        let values = (0..sql.len())
            .map(|index| {
                if sql.is_null(index) {
                    return Ok(None);
                }
                let state = Arc::clone(&self.state.0);
                let db = self.db.0.clone();
                let query = sql.value(index).to_owned();
                run_async(async move { estimate_cardinality(&state, db, &query).await }).map(Some)
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
    }
}

impl ScalarUDFImpl for CollectStatisticsUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        COLLECT_STATS_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let Some(table) = string_scalar_arg(&args.args[0], COLLECT_STATS_UDF)? else {
            return Ok(ColumnarValue::Scalar(DFScalarValue::Int64(None)));
        };
        let state = Arc::clone(&self.state.0);
        let db = self.db.0.clone();
        let snapshot =
            run_async(async move { collect_and_persist_statistics(&state, db, &table).await })?;
        Ok(ColumnarValue::Scalar(DFScalarValue::Int64(Some(snapshot))))
    }
}

fn string_scalar_arg(value: &ColumnarValue, function_name: &str) -> DFResult<Option<String>> {
    match value {
        ColumnarValue::Scalar(DFScalarValue::Utf8(value)) => Ok(value.clone()),
        ColumnarValue::Array(_) => Err(DataFusionError::Plan(format!(
            "{function_name} expects one non-column string argument"
        ))),
        ColumnarValue::Scalar(_) => Err(DataFusionError::Plan(format!(
            "{function_name} expected a Utf8 argument"
        ))),
    }
}

fn string_array_arg(value: &ColumnarValue) -> DFResult<ArrayRef> {
    let array = match value {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(value) => value.to_array()?,
    };
    if !array.as_any().is::<StringArray>() {
        return Err(DataFusionError::Plan("expected Utf8 argument".to_owned()));
    }
    Ok(array)
}

async fn collect_and_persist_statistics(
    state: &datafusion::execution::SessionState,
    db: DatabaseConnection,
    table_name: &str,
) -> DFResult<i64> {
    let session = SessionContext::from(state.clone());
    let table = TableReference::from(table_name);
    let provider = session.table_provider(table.clone()).await?;
    let column_names = provider
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let column_refs = column_names.iter().map(String::as_str).collect::<Vec<_>>();
    let collected = collect_table_statistics_for_table_ref(&session, &table, &column_refs).await?;

    let catalog = PersistentCatalog::new(db);
    let reader = catalog.reader().await.map_err(external_error)?;
    let schemas = reader.get_all_schemas().await.map_err(external_error)?;
    let relations = reader.get_all_relations().await.map_err(external_error)?;
    let relation = resolve_relation(&schemas, &relations, &table, &session)?;
    let mut columns = Vec::with_capacity(collected.column_statistics.len());
    for (column_name, statistics) in collected.column_statistics {
        let column = relation
            .columns
            .iter()
            .find(|column| column.column_name == column_name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "column '{column_name}' is not present in persistent relation '{}'",
                    relation.relation_name
                ))
            })?;
        columns.push(PersistentColumnStatistics {
            column_id: column.column_id,
            frequency: statistics.frequency.map(|value| value as u64),
            distinct_count: statistics.distinct.map(|value| value as u64),
            lower_bound: statistics.lower_bound.and_then(to_persistent_value),
            upper_bound: statistics.upper_bound.and_then(to_persistent_value),
        });
    }
    let new_snapshot = catalog
        .set_relation_statistics(
            reader.snapshot_id(),
            SetRelationStatisticsInfo {
                relation_id: relation.relation_id,
                data_snapshot_id: None,
                row_count: collected.row_count.map(|value| value as u64),
                size_bytes: collected.size_bytes.map(|value| value as u64),
                column_statistics: columns,
            },
        )
        .await
        .map_err(external_error)?;
    Ok(new_snapshot.as_i64())
}

async fn estimate_cardinality(
    state: &datafusion::execution::SessionState,
    db: DatabaseConnection,
    sql: &str,
) -> DFResult<f64> {
    let session = SessionContext::from(state.clone());
    let plan = state.create_logical_plan(sql).await?;
    let catalog = build_persistent_statistics_catalog(&session, db, &plan).await?;
    let mut optimizer = optimizer_context_from_logical_plan_with_catalog(&session, &plan, catalog)
        .await
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    default_pass_manager()
        .run(&mut optimizer)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    if let Some(root) = optimizer.query.root() {
        let resolved = optimizer.rewrites.resolve(root);
        optimizer.query.set_root(resolved);
    }
    let planned = optimizer.into_planned_query();
    let root = planned
        .query
        .root()
        .ok_or_else(|| DataFusionError::Plan("query has no root operator".to_owned()))?;
    let mut analyses = planned.analyze();
    Ok(analyses
        .get::<CardinalityEstimationV1>(&planned.query, root)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?
        .rows
        .value)
}

async fn build_persistent_statistics_catalog(
    session: &SessionContext,
    db: DatabaseConnection,
    plan: &datafusion::logical_expr::LogicalPlan,
) -> DFResult<Arc<dyn optd_core::Catalog>> {
    let catalog = PersistentCatalog::new(db);
    let reader = catalog.reader().await.map_err(external_error)?;
    let schemas = reader.get_all_schemas().await.map_err(external_error)?;
    let relations = reader.get_all_relations().await.map_err(external_error)?;
    let memory = Arc::new(MemoryCatalog::new("datafusion", "public"));

    for scan in referenced_scan_columns(plan) {
        let provider = session.table_provider(scan.table.clone()).await?;
        let table_ref = table_ref_from_df(&scan.table);
        memory
            .create_table(table_ref.clone(), provider.schema(), None)
            .map_err(external_error)?;
        let Some(relation) = resolve_relation_optional(&schemas, &relations, &scan.table, session)
        else {
            continue;
        };
        let Some(statistics) = reader
            .get_relation_statistics(relation.relation_id)
            .await
            .map_err(external_error)?
        else {
            continue;
        };
        let column_statistics = statistics
            .column_statistics
            .into_iter()
            .filter_map(|statistics| {
                let column = relation
                    .columns
                    .iter()
                    .find(|column| column.column_id == statistics.column_id)?;
                Some((
                    column.column_name.clone(),
                    ColumnStatistics {
                        lower_bound: statistics.lower_bound.map(from_persistent_value),
                        upper_bound: statistics.upper_bound.map(from_persistent_value),
                        frequency: statistics
                            .frequency
                            .and_then(|value| usize::try_from(value).ok()),
                        distinct: statistics
                            .distinct_count
                            .and_then(|value| usize::try_from(value).ok()),
                        distribution: None,
                        sketches: None,
                    },
                ))
            })
            .collect();
        memory
            .set_table_statistics(
                table_ref,
                TableStatistics {
                    row_count: statistics
                        .row_count
                        .and_then(|value| usize::try_from(value).ok()),
                    size_bytes: statistics
                        .size_bytes
                        .and_then(|value| usize::try_from(value).ok()),
                    column_statistics,
                    constraints: Default::default(),
                },
            )
            .map_err(external_error)?;
    }

    Ok(memory)
}

fn resolve_relation<'a>(
    schemas: &[optd_catalog::schema::SchemaInfo],
    relations: &'a [RelationInfo],
    table: &TableReference,
    session: &SessionContext,
) -> DFResult<&'a RelationInfo> {
    resolve_relation_optional(schemas, relations, table, session).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "relation '{table}' is not present in the persistent catalog"
        ))
    })
}

fn resolve_relation_optional<'a>(
    schemas: &[optd_catalog::schema::SchemaInfo],
    relations: &'a [RelationInfo],
    table: &TableReference,
    session: &SessionContext,
) -> Option<&'a RelationInfo> {
    let default_schema = session
        .copied_config()
        .options()
        .catalog
        .default_schema
        .clone();
    let schema_name = table.schema().unwrap_or(default_schema.as_str());
    let schema = schemas
        .iter()
        .find(|schema| schema.schema_name == schema_name)?;
    relations.iter().find(|relation| {
        relation.schema_id == schema.schema_id && relation.relation_name == table.table()
    })
}

fn to_persistent_value(value: ScalarValue) -> Option<StatisticsValue> {
    Some(match value {
        ScalarValue::Null(_)
        | ScalarValue::IntervalMonthDayNano { .. }
        | ScalarValue::IntervalDayTime { .. } => return None,
        ScalarValue::Boolean(value) => StatisticsValue::Boolean(value),
        ScalarValue::Int32(value) => StatisticsValue::Int32(value),
        ScalarValue::Int64(value) => StatisticsValue::Int64(value),
        ScalarValue::Float64(value) => StatisticsValue::Float64(value),
        ScalarValue::Utf8(value) => StatisticsValue::Utf8(value),
        ScalarValue::Date32(value) => StatisticsValue::Date32(value),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => StatisticsValue::Decimal128 {
            value,
            precision,
            scale,
        },
    })
}

fn from_persistent_value(value: StatisticsValue) -> ScalarValue {
    match value {
        StatisticsValue::Boolean(value) => ScalarValue::Boolean(value),
        StatisticsValue::Int32(value) => ScalarValue::Int32(value),
        StatisticsValue::Int64(value) => ScalarValue::Int64(value),
        StatisticsValue::Float64(value) => ScalarValue::Float64(value),
        StatisticsValue::Utf8(value) => ScalarValue::Utf8(value),
        StatisticsValue::Date32(value) => ScalarValue::Date32(value),
        StatisticsValue::Decimal128 {
            value,
            precision,
            scale,
        } => ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        },
    }
}

fn run_async<F, T>(future: F) -> DFResult<T>
where
    F: Future<Output = DFResult<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(external_error)?
            .block_on(future)
    })
    .join()
    .map_err(|_| DataFusionError::Execution("statistics UDF thread panicked".to_owned()))?
}

fn external_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
