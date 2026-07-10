//! Converts optd IR directly into DataFusion physical [`ExecutionPlan`]s.
//!
//! Operator construction in this module should track DataFusion's planner
//! conventions in:
//! <https://github.com/apache/datafusion/blob/b426232b8a13c641a522bba414e932914f3b79f5/datafusion/core/src/physical_planner.rs>

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;
use datafusion_catalog::ScanArgs;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, JoinSide, NullEquality, TableReference};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use optd_core::{
    BinaryOp, Catalog, ExprData, JoinType, NaryOp, Operator, OperatorData, PlannedQuery,
    QueryContext, TableRef,
};

use crate::to_df_logical::{
    ColumnQualifiers, TableMap, ToDFError, ToDfContext, convert_agg_expr, convert_expr,
    convert_join_type, optd_table_ref_to_df,
};

/// Error type for the optd → DataFusion physical converter.
#[derive(Debug)]
pub enum ToPhysicalError {
    Unsupported(String),
    Build(datafusion::error::DataFusionError),
    TableNotFound(TableRef),
}

impl std::fmt::Display for ToPhysicalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "unsupported physical IR node: {msg}"),
            Self::Build(e) => write!(f, "physical plan build error: {e}"),
            Self::TableNotFound(t) => write!(f, "table not found: {t}"),
        }
    }
}

impl std::error::Error for ToPhysicalError {}

impl From<datafusion::error::DataFusionError> for ToPhysicalError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Build(e)
    }
}

impl From<ToDFError> for ToPhysicalError {
    fn from(e: ToDFError) -> Self {
        match e {
            ToDFError::Unsupported(msg) => Self::Unsupported(msg),
            ToDFError::Build(e) => Self::Build(e),
            ToDFError::TableNotFound(t) => Self::TableNotFound(t),
        }
    }
}

pub type ToPhysicalResult<T> = Result<T, ToPhysicalError>;

struct PhysicalPlanNode {
    exec: Arc<dyn ExecutionPlan>,
    df_schema: DFSchemaRef,
    columns: Vec<optd_core::Column>,
}

/// Converts a optd `QueryContext` root directly into a DataFusion physical plan.
pub async fn to_physical_plan(
    planned: &PlannedQuery,
    session: &SessionContext,
) -> ToPhysicalResult<Arc<dyn ExecutionPlan>> {
    let ctx = &planned.query;
    let root = ctx
        .root()
        .ok_or_else(|| ToPhysicalError::Unsupported("query has no root".into()))?;
    let required = output_columns(root, ctx, &planned.catalog)?;
    let node = convert_operator(root, &required, ctx, &planned.catalog, session).await?;
    Ok(node.exec)
}

// ---------------------------------------------------------------------------
// Operator conversion and parent-driven column demand
// ---------------------------------------------------------------------------

// Conversion is demand-driven: each parent passes the exact columns it needs so
// scans and intermediate projections can be narrowed before DataFusion physical
// operators are built. Each operator projects back to its promised output order
// before returning to keep parent schema assumptions stable.
async fn convert_operator(
    op: Operator,
    required: &[optd_core::Column],
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    match op.get(ctx) {
        OperatorData::Scan(scan) => convert_scan(scan, required, &[], ctx, catalog, session).await,

        OperatorData::Selection(sel) => {
            reject_subquery_exprs(sel.predicate, ctx)?;
            if let OperatorData::Scan(scan) = sel.input.get(ctx) {
                let input_required =
                    merge_columns(required, &expr_columns(sel.predicate, ctx, catalog)?);
                let input = Box::pin(convert_scan(
                    scan,
                    &input_required,
                    &[sel.predicate],
                    ctx,
                    catalog,
                    session,
                ))
                .await?;
                let predicate = physical_expr(
                    sel.predicate,
                    ctx,
                    catalog,
                    session,
                    input.df_schema.as_ref(),
                    input.exec.schema().as_ref(),
                )?;
                let exec = FilterExecBuilder::new(predicate, input.exec)
                    .with_batch_size(session.copied_config().batch_size())
                    .build()?;
                return project_columns(
                    required,
                    PhysicalPlanNode {
                        exec: Arc::new(exec),
                        df_schema: input.df_schema,
                        columns: input.columns,
                    },
                    ctx,
                    session,
                );
            }

            let input_required =
                merge_columns(required, &expr_columns(sel.predicate, ctx, catalog)?);
            let input = Box::pin(convert_operator(
                sel.input,
                &input_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let predicate = physical_expr(
                sel.predicate,
                ctx,
                catalog,
                session,
                input.df_schema.as_ref(),
                input.exec.schema().as_ref(),
            )?;
            let exec = FilterExecBuilder::new(predicate, input.exec)
                .with_batch_size(session.copied_config().batch_size())
                .build()?;
            project_columns(
                required,
                PhysicalPlanNode {
                    exec: Arc::new(exec),
                    df_schema: input.df_schema,
                    columns: input.columns,
                },
                ctx,
                session,
            )
        }

        OperatorData::Projection(proj) => {
            let input_required = filter_columns(&proj.columns, required);
            let input = Box::pin(convert_operator(
                proj.input,
                &input_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            project_columns(required, input, ctx, session)
        }

        OperatorData::Map(map) => {
            for (_, expr) in &map.computations {
                reject_subquery_exprs(*expr, ctx)?;
            }
            let input_available = available_columns(map.input, ctx, catalog)?;
            let mut input_required = Vec::new();
            for column in required {
                if input_available.contains(column) {
                    push_unique(&mut input_required, *column);
                } else if let Some((_, expr)) = map
                    .computations
                    .iter()
                    .find(|(computed, _)| computed == column)
                {
                    input_required =
                        merge_columns(&input_required, &expr_columns(*expr, ctx, catalog)?);
                }
            }
            let input = Box::pin(convert_operator(
                map.input,
                &input_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let available = input.columns.clone();
            let mut exprs = projection_exprs_for_columns(
                &available,
                ctx,
                input.df_schema.as_ref(),
                input.exec.schema().as_ref(),
            )?;
            for (column, expr) in &map.computations {
                if !required.contains(column) {
                    continue;
                }
                let physical = physical_expr(
                    *expr,
                    ctx,
                    catalog,
                    session,
                    input.df_schema.as_ref(),
                    input.exec.schema().as_ref(),
                )?;
                exprs.push(ProjectionExpr::new(
                    physical,
                    ctx.column(*column).name.clone(),
                ));
            }
            let mut columns = available;
            columns.extend(
                map.computations
                    .iter()
                    .map(|(column, _)| *column)
                    .filter(|column| required.contains(column)),
            );
            let schema = df_schema_for_columns(ctx, &columns)?;
            project_columns(
                required,
                PhysicalPlanNode {
                    exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
                    df_schema: schema,
                    columns,
                },
                ctx,
                session,
            )
        }

        OperatorData::Aggregation(agg) => {
            for key in &agg.keys {
                reject_subquery_exprs(*key, ctx)?;
            }
            for (_, agg_expr) in &agg.aggregates {
                if let optd_core::AggregateExpr::Func { arg, .. } = agg_expr {
                    reject_subquery_exprs(*arg, ctx)?;
                }
            }
            let mut input_required = Vec::new();
            for key in &agg.keys {
                input_required = merge_columns(&input_required, &expr_columns(*key, ctx, catalog)?);
            }
            for (_, agg_expr) in &agg.aggregates {
                if let optd_core::AggregateExpr::Func { arg, .. } = agg_expr {
                    input_required =
                        merge_columns(&input_required, &expr_columns(*arg, ctx, catalog)?);
                }
            }
            let input = Box::pin(convert_operator(
                agg.input,
                &input_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let input_schema = input.exec.schema();
            let empty_tables = TableMap::new();
            let mut expr_ctx = ToDfContext::new(ctx, Arc::clone(catalog), &empty_tables, session);
            let group_exprs = agg
                .keys
                .iter()
                .map(|expr| {
                    let physical = physical_expr(
                        *expr,
                        ctx,
                        catalog,
                        session,
                        input.df_schema.as_ref(),
                        input.exec.schema().as_ref(),
                    )?;
                    let logical = convert_expr(
                        *expr,
                        &mut expr_ctx,
                        &Default::default(),
                        &ColumnQualifiers::new(),
                    )?;
                    Ok((physical, logical.schema_name().to_string()))
                })
                .collect::<ToPhysicalResult<Vec<_>>>()?;
            let groups = PhysicalGroupBy::new_single(group_exprs);
            let aggregate_exprs = agg
                .aggregates
                .iter()
                .map(|(column, agg_expr)| {
                    let logical = convert_agg_expr(
                        agg_expr,
                        &mut expr_ctx,
                        &Default::default(),
                        &ColumnQualifiers::new(),
                    )?
                    .alias(ctx.column(*column).name.clone());
                    let (agg, filter, _) = create_aggregate_expr_and_maybe_filter(
                        &logical,
                        input.df_schema.as_ref(),
                        input_schema.as_ref(),
                        session.state().execution_props(),
                    )?;
                    if filter.is_some() {
                        return Err(ToPhysicalError::Unsupported(
                            "aggregate FILTER clause".into(),
                        ));
                    }
                    Ok(agg)
                })
                .collect::<ToPhysicalResult<Vec<_>>>()?;
            let filters = vec![None; aggregate_exprs.len()];
            let partial = Arc::new(AggregateExec::try_new(
                AggregateMode::Partial,
                groups.clone(),
                aggregate_exprs,
                filters.clone(),
                input.exec,
                Arc::clone(&input_schema),
            )?);
            let final_groups = partial.group_expr().as_final();
            let final_exec = Arc::new(AggregateExec::try_new(
                AggregateMode::Final,
                final_groups,
                partial.aggr_expr().to_vec(),
                filters,
                partial,
                input_schema,
            )?);
            let columns = output_columns(op, ctx, catalog)?;
            project_columns(
                required,
                PhysicalPlanNode {
                    exec: final_exec,
                    df_schema: df_schema_for_columns(ctx, &columns)?,
                    columns,
                },
                ctx,
                session,
            )
        }

        OperatorData::Sort(sort) => {
            for key in &sort.keys {
                reject_subquery_exprs(key.expr, ctx)?;
            }
            let mut input_required = required.to_vec();
            for key in &sort.keys {
                input_required =
                    merge_columns(&input_required, &expr_columns(key.expr, ctx, catalog)?);
            }
            let input = Box::pin(convert_operator(
                sort.input,
                &input_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let sort_exprs = sort
                .keys
                .iter()
                .map(|key| {
                    let expr = physical_expr(
                        key.expr,
                        ctx,
                        catalog,
                        session,
                        input.df_schema.as_ref(),
                        input.exec.schema().as_ref(),
                    )?;
                    Ok(PhysicalSortExpr::new(
                        expr,
                        SortOptions {
                            descending: matches!(key.direction, optd_core::SortDirection::Desc),
                            nulls_first: matches!(key.nulls, optd_core::NullOrdering::First),
                        },
                    ))
                })
                .collect::<ToPhysicalResult<Vec<_>>>()?;
            let ordering = LexOrdering::new(sort_exprs)
                .ok_or_else(|| ToPhysicalError::Unsupported("empty sort".into()))?;
            let input_exec = ensure_single_partition(input.exec);
            project_columns(
                required,
                PhysicalPlanNode {
                    exec: Arc::new(SortExec::new(ordering, input_exec)),
                    df_schema: input.df_schema,
                    columns: input.columns,
                },
                ctx,
                session,
            )
        }

        OperatorData::Limit(limit) => {
            let input = Box::pin(convert_operator(
                limit.input,
                required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let mut input_exec = input.exec;
            if input_exec.output_partitioning().partition_count() > 1 {
                if let Some(fetch) = limit.fetch {
                    input_exec = Arc::new(LocalLimitExec::new(input_exec, fetch + limit.offset));
                }
                input_exec = Arc::new(CoalescePartitionsExec::new(input_exec));
            }
            Ok(PhysicalPlanNode {
                exec: Arc::new(GlobalLimitExec::new(input_exec, limit.offset, limit.fetch)),
                df_schema: input.df_schema,
                columns: input.columns,
            })
        }

        OperatorData::Join(join) => convert_join(join, required, ctx, catalog, session).await,

        OperatorData::CrossProduct(cross) => {
            let left_available = available_columns(cross.outer, ctx, catalog)?;
            let right_available = available_columns(cross.inner, ctx, catalog)?;
            let left_required = filter_columns(&left_available, required);
            let right_required = filter_columns(&right_available, required);
            let left = Box::pin(convert_operator(
                cross.outer,
                &left_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let right = Box::pin(convert_operator(
                cross.inner,
                &right_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            project_columns(
                required,
                join_node(
                    Arc::new(CrossJoinExec::new(left.exec, right.exec)),
                    merge_columns(&left.columns, &right.columns),
                    ctx,
                )?,
                ctx,
                session,
            )
        }

        OperatorData::Output(out) => {
            Box::pin(convert_operator(out.input, required, ctx, catalog, session)).await
        }

        OperatorData::Rename(rename) => {
            let renamed_required = filter_columns(
                &rename
                    .defs
                    .iter()
                    .map(|(renamed, _)| *renamed)
                    .collect::<Vec<_>>(),
                required,
            );
            let original_required = rename
                .defs
                .iter()
                .filter_map(|(renamed, original)| {
                    renamed_required.contains(renamed).then_some(*original)
                })
                .collect::<Vec<_>>();
            let input = Box::pin(convert_operator(
                rename.input,
                &original_required,
                ctx,
                catalog,
                session,
            ))
            .await?;
            let mut exprs = Vec::new();
            let mut renamed_columns = Vec::new();
            for (renamed, original) in &rename.defs {
                if !renamed_required.contains(renamed) {
                    continue;
                }
                let mut original_expr = projection_exprs_for_columns(
                    &[*original],
                    ctx,
                    input.df_schema.as_ref(),
                    input.exec.schema().as_ref(),
                )?;
                let expr = original_expr.pop().expect("one projection expr");
                exprs.push(ProjectionExpr::new(
                    Arc::clone(&expr.expr),
                    ctx.column(*renamed).name.clone(),
                ));
                renamed_columns.push(*renamed);
            }
            project_columns(
                required,
                PhysicalPlanNode {
                    exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
                    df_schema: df_schema_for_columns(ctx, &renamed_columns)?,
                    columns: renamed_columns,
                },
                ctx,
                session,
            )
        }

        OperatorData::ConstScan(const_scan) => match const_scan.rows.len() {
            0 => {
                let schema = schema_for_columns(ctx, &const_scan.columns);
                Ok(PhysicalPlanNode {
                    exec: Arc::new(EmptyExec::new(Arc::clone(&schema))),
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
                    columns: const_scan.columns.clone(),
                })
            }
            1 if const_scan.rows[0].is_empty() => {
                let schema = schema_for_columns(ctx, &const_scan.columns);
                Ok(PhysicalPlanNode {
                    exec: Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
                    columns: const_scan.columns.clone(),
                })
            }
            _ => {
                if const_scan
                    .rows
                    .iter()
                    .any(|row| row.len() != const_scan.columns.len())
                {
                    return Err(ToPhysicalError::Unsupported(
                        "ConstScan row width must match number of columns".into(),
                    ));
                }
                let schema = schema_for_columns(ctx, &const_scan.columns);
                let df_schema = DFSchema::empty();
                let values = const_scan
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|expr| {
                                physical_expr(*expr, ctx, catalog, session, &df_schema, &schema)
                            })
                            .collect::<ToPhysicalResult<Vec<_>>>()
                    })
                    .collect::<ToPhysicalResult<Vec<_>>>()?;
                Ok(PhysicalPlanNode {
                    exec: MemorySourceConfig::try_new_as_values(Arc::clone(&schema), values)?,
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
                    columns: const_scan.columns.clone(),
                })
            }
        },

        OperatorData::TableFunction(_) => Err(ToPhysicalError::Unsupported("TableFunction".into())),
    }
}

async fn convert_scan(
    scan: &optd_core::Scan,
    required: &[optd_core::Column],
    filters: &[optd_core::Expr],
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    let provider = session
        .table_provider(optd_table_ref_to_df(&scan.table))
        .await
        .map_err(|_| ToPhysicalError::TableNotFound(scan.table.clone()))?;
    let provider_schema = provider.schema();
    let mut columns = filter_columns(&scan.columns, required);
    for filter in filters {
        columns = merge_columns(&columns, &expr_columns(*filter, ctx, catalog)?);
    }
    let projection = columns
        .iter()
        .map(|column| {
            let name = &ctx.column(*column).name;
            provider_schema
                .index_of(name)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let logical_filters = filters
        .iter()
        .map(|filter| scan_filter_expr(*filter, ctx, catalog, session))
        .collect::<ToPhysicalResult<Vec<_>>>()?;
    let mut args = ScanArgs::default().with_projection(Some(&projection));
    if !logical_filters.is_empty() {
        // Push filters to the provider for file/Parquet pruning. The caller
        // still keeps a FilterExec above the scan because scan providers may
        // only use these filters for pruning and not for complete evaluation.
        args = args.with_filters(Some(&logical_filters));
    }
    let state = session.state();
    let scanned = provider.scan_with_args(&state, args).await?;
    Ok(PhysicalPlanNode {
        exec: Arc::clone(scanned.plan()),
        df_schema: df_schema_for_columns(ctx, &columns)?,
        columns,
    })
}

async fn convert_join(
    join: &optd_core::Join,
    required: &[optd_core::Column],
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    if !free_columns_for(join.inner, ctx, catalog)?.is_empty() {
        return Err(ToPhysicalError::Unsupported("correlated join input".into()));
    }
    if matches!(join.join_type, JoinType::Single) && !is_at_most_one_row(join.inner, ctx, catalog)?
    {
        return Err(ToPhysicalError::Unsupported(
            "Single join without proven single-row inner input".into(),
        ));
    }
    reject_subquery_exprs(join.on, ctx)?;
    let left_available = available_columns(join.outer, ctx, catalog)?;
    let right_available = available_columns(join.inner, ctx, catalog)?;
    let predicate_columns = expr_columns(join.on, ctx, catalog)?;
    let left_required = merge_columns(
        &filter_columns(&left_available, required),
        &filter_columns(&left_available, &predicate_columns),
    );
    let right_required = merge_columns(
        &filter_columns(&right_available, required),
        &filter_columns(&right_available, &predicate_columns),
    );
    let left = Box::pin(convert_operator(
        join.outer,
        &left_required,
        ctx,
        catalog,
        session,
    ))
    .await?;
    let right = Box::pin(convert_operator(
        join.inner,
        &right_required,
        ctx,
        catalog,
        session,
    ))
    .await?;
    let df_join_type = convert_join_type(&join.join_type)?;
    if is_true_expr(join.on, ctx) {
        if matches!(join.join_type, JoinType::Inner) {
            let output_columns = join_output_columns_from_nodes(&join.join_type, &left, &right);
            return project_columns(
                required,
                join_node(
                    Arc::new(CrossJoinExec::new(left.exec, right.exec)),
                    output_columns,
                    ctx,
                )?,
                ctx,
                session,
            );
        }
        let output_columns = join_output_columns_from_nodes(&join.join_type, &left, &right);
        let exec = NestedLoopJoinExec::try_new(left.exec, right.exec, None, &df_join_type, None)?;
        return project_columns(
            required,
            join_node(Arc::new(exec), output_columns, ctx)?,
            ctx,
            session,
        );
    }

    let split = split_join_predicate(join, ctx, catalog, session, &left, &right)?;
    if !split.on.is_empty() {
        // Mixed equi/residual predicates should still use hash joins: the equi
        // keys drive partition/build/probe work and DataFusion evaluates the
        // remaining predicate as a JoinFilter.
        let filter = split
            .residual
            .map(|expr| join_filter(expr, ctx, catalog, session, &left, &right))
            .transpose()?;
        let output_columns = join_output_columns_from_nodes(&join.join_type, &left, &right);
        let exec = HashJoinExec::try_new(
            left.exec,
            right.exec,
            split.on,
            filter,
            &df_join_type,
            None,
            PartitionMode::CollectLeft,
            split.null_equality,
            false,
        )?;
        return project_columns(
            required,
            join_node(Arc::new(exec), output_columns, ctx)?,
            ctx,
            session,
        );
    }

    let filter = join_filter(join.on, ctx, catalog, session, &left, &right)?;
    let output_columns = join_output_columns_from_nodes(&join.join_type, &left, &right);
    let exec =
        NestedLoopJoinExec::try_new(left.exec, right.exec, Some(filter), &df_join_type, None)?;
    project_columns(
        required,
        join_node(Arc::new(exec), output_columns, ctx)?,
        ctx,
        session,
    )
}

// ---------------------------------------------------------------------------
// Projection, schema, and required-column helpers
// ---------------------------------------------------------------------------

fn project_columns(
    columns: &[optd_core::Column],
    input: PhysicalPlanNode,
    ctx: &QueryContext,
    _session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    if input.columns == columns {
        return Ok(input);
    }
    let exprs = projection_exprs_for_columns(
        columns,
        ctx,
        input.df_schema.as_ref(),
        input.exec.schema().as_ref(),
    )?;
    Ok(PhysicalPlanNode {
        exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
        df_schema: df_schema_for_columns(ctx, columns)?,
        columns: columns.to_vec(),
    })
}

fn projection_exprs_for_columns(
    columns: &[optd_core::Column],
    ctx: &QueryContext,
    input_schema: &DFSchema,
    physical_schema: &Schema,
) -> ToPhysicalResult<Vec<ProjectionExpr>> {
    columns
        .iter()
        .map(|column| {
            let logical = logical_column_expr(*column, ctx);
            let datafusion::logical_expr::Expr::Column(df_column) = logical else {
                unreachable!("logical_column_expr always returns a column expression");
            };
            let index = input_schema.index_of_column(&df_column)?;
            let physical_name = physical_schema.field(index).name();
            let physical = Arc::new(PhysicalColumn::new(physical_name, index));
            Ok(ProjectionExpr::new(
                physical,
                ctx.column(*column).name.clone(),
            ))
        })
        .collect()
}

fn filter_columns(
    columns: &[optd_core::Column],
    required: &[optd_core::Column],
) -> Vec<optd_core::Column> {
    columns
        .iter()
        .copied()
        .filter(|column| required.contains(column))
        .collect()
}

fn push_unique(columns: &mut Vec<optd_core::Column>, column: optd_core::Column) {
    if !columns.contains(&column) {
        columns.push(column);
    }
}

fn merge_columns(
    left: &[optd_core::Column],
    right: &[optd_core::Column],
) -> Vec<optd_core::Column> {
    let mut merged = left.to_vec();
    for column in right {
        push_unique(&mut merged, *column);
    }
    merged
}

fn expr_columns(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<Vec<optd_core::Column>> {
    let mut analyses = optd_core::AnalysisContext::new(Arc::clone(catalog));
    optd_core::expr_used_columns(ctx, &mut analyses, expr)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))
}

fn logical_column_expr(
    column: optd_core::Column,
    ctx: &QueryContext,
) -> datafusion::logical_expr::Expr {
    let column_data = ctx.column(column);
    let column = match column_data.qualifier.as_deref() {
        Some(qualifier) => datafusion_common::Column::new(Some(qualifier), &column_data.name),
        None => datafusion_common::Column::new_unqualified(&column_data.name),
    };
    datafusion::logical_expr::Expr::Column(column)
}

fn scan_filter_expr(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
) -> ToPhysicalResult<datafusion::logical_expr::Expr> {
    let empty_tables = TableMap::new();
    let mut expr_ctx = ToDfContext::new(ctx, Arc::clone(catalog), &empty_tables, session);
    let logical = convert_expr(
        expr,
        &mut expr_ctx,
        &Default::default(),
        &ColumnQualifiers::new(),
    )?;
    Ok(unnormalize_logical_columns(logical)?)
}

fn unnormalize_logical_columns(
    expr: datafusion::logical_expr::Expr,
) -> datafusion::error::Result<datafusion::logical_expr::Expr> {
    Ok(expr
        .transform_down(|expr| {
            if let datafusion::logical_expr::Expr::Column(column) = expr {
                Ok(Transformed::yes(datafusion::logical_expr::Expr::Column(
                    datafusion_common::Column::new_unqualified(column.name),
                )))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

fn physical_expr(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
    input_schema: &DFSchema,
    physical_schema: &Schema,
) -> ToPhysicalResult<Arc<dyn PhysicalExpr>> {
    let empty_tables = TableMap::new();
    let mut expr_ctx = ToDfContext::new(ctx, Arc::clone(catalog), &empty_tables, session);
    let logical = convert_expr(
        expr,
        &mut expr_ctx,
        &Default::default(),
        &ColumnQualifiers::new(),
    )?;
    physical_expr_from_logical(logical, session, input_schema, physical_schema)
}

fn physical_expr_from_logical(
    logical: datafusion::logical_expr::Expr,
    session: &SessionContext,
    input_schema: &DFSchema,
    physical_schema: &Schema,
) -> ToPhysicalResult<Arc<dyn PhysicalExpr>> {
    let state = session.state();
    let simplify_context = datafusion::logical_expr::simplify::SimplifyContext::default()
        .with_schema(Arc::new(input_schema.clone()))
        .with_config_options(Arc::clone(state.config_options()))
        .with_query_execution_start_time(state.execution_props().query_execution_start_time);
    let simplifier = ExprSimplifier::new(simplify_context);
    let logical = simplifier.simplify(logical)?;
    let physical = state.create_physical_expr(logical, input_schema)?;
    align_physical_columns_to_schema(physical, physical_schema)
}

fn align_physical_columns_to_schema(
    expr: Arc<dyn PhysicalExpr>,
    physical_schema: &Schema,
) -> ToPhysicalResult<Arc<dyn PhysicalExpr>> {
    Ok(expr
        .transform_down(|expr| {
            if let Some(column) = expr.as_any().downcast_ref::<PhysicalColumn>() {
                let index = column.index();
                let physical_name = physical_schema.field(index).name();
                Ok(Transformed::yes(
                    Arc::new(PhysicalColumn::new(physical_name, index)) as Arc<dyn PhysicalExpr>,
                ))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

// ---------------------------------------------------------------------------
// Join predicate analysis
// ---------------------------------------------------------------------------

struct SplitJoinPredicate {
    on: JoinOn,
    residual: Option<optd_core::Expr>,
    null_equality: NullEquality,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JoinKeyCandidate {
    left_expr: optd_core::Expr,
    right_expr: optd_core::Expr,
    left_col: optd_core::Column,
    right_col: optd_core::Column,
    null_equality: NullEquality,
}

fn split_join_predicate(
    join: &optd_core::Join,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
    left: &PhysicalPlanNode,
    right: &PhysicalPlanNode,
) -> ToPhysicalResult<SplitJoinPredicate> {
    let expr = join.on;
    let conjuncts = split_conjuncts(expr, ctx);
    let outer_cols = available_columns_set(ctx, join.outer, catalog)?;
    let inner_cols = available_columns_set(ctx, join.inner, catalog)?;
    let mut candidates = Vec::new();
    let mut all_conjuncts_are_keys = true;
    for conjunct in &conjuncts {
        if let Some(candidate) = join_key_candidate(*conjunct, ctx, &outer_cols, &inner_cols)? {
            candidates.push(candidate);
        } else {
            all_conjuncts_are_keys = false;
        }
    }

    if candidates.is_empty() {
        candidates = common_or_join_key_candidates(expr, ctx, &outer_cols, &inner_cols)?;
        all_conjuncts_are_keys = false;
    }

    let (on, null_equality) = physical_join_keys(&candidates, ctx, catalog, session, left, right)?;
    Ok(SplitJoinPredicate {
        on,
        residual: (!candidates.is_empty() && !all_conjuncts_are_keys).then_some(expr),
        null_equality,
    })
}

fn physical_join_keys(
    candidates: &[JoinKeyCandidate],
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
    left: &PhysicalPlanNode,
    right: &PhysicalPlanNode,
) -> ToPhysicalResult<(JoinOn, NullEquality)> {
    let mut keys = Vec::new();
    let mut null_equality = None;
    for candidate in candidates {
        let key_null_equality = candidate.null_equality;
        if let Some(existing) = null_equality {
            if existing != key_null_equality {
                return Ok((Vec::new(), NullEquality::NullEqualsNothing));
            }
        } else {
            null_equality = Some(key_null_equality);
        }
        keys.push((
            physical_expr(
                candidate.left_expr,
                ctx,
                catalog,
                session,
                left.df_schema.as_ref(),
                left.exec.schema().as_ref(),
            )?,
            physical_expr(
                candidate.right_expr,
                ctx,
                catalog,
                session,
                right.df_schema.as_ref(),
                right.exec.schema().as_ref(),
            )?,
        ));
    }
    Ok((
        keys,
        null_equality.unwrap_or(NullEquality::NullEqualsNothing),
    ))
}

fn join_key_candidate(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    outer_cols: &HashSet<optd_core::Column>,
    inner_cols: &HashSet<optd_core::Column>,
) -> ToPhysicalResult<Option<JoinKeyCandidate>> {
    let ExprData::Binary {
        op: op @ (BinaryOp::Eq | BinaryOp::IsNotDistinctFrom),
        left,
        right,
    } = expr.get(ctx)
    else {
        return Ok(None);
    };
    let null_equality = match op {
        BinaryOp::Eq => NullEquality::NullEqualsNothing,
        BinaryOp::IsNotDistinctFrom => NullEquality::NullEqualsNull,
        _ => unreachable!(),
    };
    let Some(left_col) = column_ref(*left, ctx) else {
        return Ok(None);
    };
    let Some(right_col) = column_ref(*right, ctx) else {
        return Ok(None);
    };
    if outer_cols.contains(&left_col) && inner_cols.contains(&right_col) {
        Ok(Some(JoinKeyCandidate {
            left_expr: *left,
            right_expr: *right,
            left_col,
            right_col,
            null_equality,
        }))
    } else if outer_cols.contains(&right_col) && inner_cols.contains(&left_col) {
        Ok(Some(JoinKeyCandidate {
            left_expr: *right,
            right_expr: *left,
            left_col: right_col,
            right_col: left_col,
            null_equality,
        }))
    } else {
        Ok(None)
    }
}

fn common_or_join_key_candidates(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    outer_cols: &HashSet<optd_core::Column>,
    inner_cols: &HashSet<optd_core::Column>,
) -> ToPhysicalResult<Vec<JoinKeyCandidate>> {
    let arms = split_disjuncts(expr, ctx);
    if arms.len() <= 1 {
        return Ok(Vec::new());
    }
    let mut common = split_conjuncts(arms[0], ctx)
        .into_iter()
        .filter_map(|conjunct| {
            join_key_candidate(conjunct, ctx, outer_cols, inner_cols).transpose()
        })
        .collect::<ToPhysicalResult<Vec<_>>>()?;
    for arm in arms.iter().skip(1) {
        let arm_keys = split_conjuncts(*arm, ctx)
            .into_iter()
            .filter_map(|conjunct| {
                join_key_candidate(conjunct, ctx, outer_cols, inner_cols).transpose()
            })
            .collect::<ToPhysicalResult<Vec<_>>>()?;
        common.retain(|candidate| {
            arm_keys.iter().any(|arm_key| {
                candidate.left_col == arm_key.left_col
                    && candidate.right_col == arm_key.right_col
                    && candidate.null_equality == arm_key.null_equality
            })
        });
    }
    Ok(common)
}

fn join_filter(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
    session: &SessionContext,
    left: &PhysicalPlanNode,
    right: &PhysicalPlanNode,
) -> ToPhysicalResult<JoinFilter> {
    let mut fields = Vec::new();
    let mut qualified = Vec::new();
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    for (idx, (qualifier, field)) in left.df_schema.iter().enumerate() {
        qualified.push((qualifier.cloned(), Arc::clone(field)));
        fields.push(left.exec.schema().field(idx).clone());
        left_indices.push(idx);
    }
    for (idx, (qualifier, field)) in right.df_schema.iter().enumerate() {
        qualified.push((qualifier.cloned(), Arc::clone(field)));
        fields.push(right.exec.schema().field(idx).clone());
        right_indices.push(idx);
    }
    let df_schema = DFSchema::new_with_metadata(qualified, Default::default())?;
    let schema = Arc::new(Schema::new(fields));
    let expression = physical_expr(expr, ctx, catalog, session, &df_schema, &schema)?;
    let column_indices = left_indices
        .into_iter()
        .map(
            |index| datafusion::physical_plan::joins::utils::ColumnIndex {
                index,
                side: JoinSide::Left,
            },
        )
        .chain(right_indices.into_iter().map(|index| {
            datafusion::physical_plan::joins::utils::ColumnIndex {
                index,
                side: JoinSide::Right,
            }
        }))
        .collect();
    Ok(JoinFilter::new(expression, column_indices, schema))
}

fn join_node(
    exec: Arc<dyn ExecutionPlan>,
    columns: Vec<optd_core::Column>,
    ctx: &QueryContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    Ok(PhysicalPlanNode {
        exec,
        df_schema: df_schema_for_columns(ctx, &columns)?,
        columns,
    })
}

fn join_output_columns_from_nodes(
    join_type: &JoinType,
    left: &PhysicalPlanNode,
    right: &PhysicalPlanNode,
) -> Vec<optd_core::Column> {
    let mut columns = left.columns.clone();
    if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return columns;
    }
    if let JoinType::LeftMark { marker, .. } = join_type {
        columns.push(*marker);
        return columns;
    }
    columns.extend(right.columns.iter().copied());
    columns
}

fn output_columns(
    op: Operator,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<Vec<optd_core::Column>> {
    available_columns(op, ctx, catalog)
}

fn available_columns(
    op: Operator,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<Vec<optd_core::Column>> {
    let mut analyses = optd_core::AnalysisContext::new(Arc::clone(catalog));
    analyses
        .get::<optd_core::AvailableColumns>(ctx, op)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))
}

fn available_columns_set(
    ctx: &QueryContext,
    op: Operator,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<HashSet<optd_core::Column>> {
    Ok(available_columns(op, ctx, catalog)?.into_iter().collect())
}

fn free_columns_for(
    op: Operator,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<HashSet<optd_core::Column>> {
    let mut analyses = optd_core::AnalysisContext::new(Arc::clone(catalog));
    Ok(analyses
        .get::<optd_core::FreeColumns>(ctx, op)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))?
        .into_iter()
        .collect())
}

fn schema_for_columns(ctx: &QueryContext, columns: &[optd_core::Column]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| {
                let column = ctx.column(*column);
                Field::new(&column.name, column.ty.clone(), true)
            })
            .collect::<Vec<_>>(),
    ))
}

fn df_schema_for_columns(
    ctx: &QueryContext,
    columns: &[optd_core::Column],
) -> ToPhysicalResult<DFSchemaRef> {
    let qualified_fields = columns
        .iter()
        .map(|column| {
            let column = ctx.column(*column);
            let qualifier = column
                .qualifier
                .as_ref()
                .map(|q| TableReference::bare(q.as_str()));
            (
                qualifier,
                Arc::new(Field::new(&column.name, column.ty.clone(), true)),
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(DFSchema::new_with_metadata(
        qualified_fields,
        Default::default(),
    )?))
}

fn split_conjuncts(expr: optd_core::Expr, ctx: &QueryContext) -> Vec<optd_core::Expr> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .iter()
            .flat_map(|expr| split_conjuncts(*expr, ctx))
            .collect(),
        _ => vec![expr],
    }
}

fn split_disjuncts(expr: optd_core::Expr, ctx: &QueryContext) -> Vec<optd_core::Expr> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::Or,
            exprs,
        } => exprs
            .iter()
            .flat_map(|expr| split_disjuncts(*expr, ctx))
            .collect(),
        _ => vec![expr],
    }
}

fn column_ref(expr: optd_core::Expr, ctx: &QueryContext) -> Option<optd_core::Column> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) => Some(*col),
        _ => None,
    }
}

fn is_true_expr(expr: optd_core::Expr, ctx: &QueryContext) -> bool {
    matches!(
        ctx.expr(expr),
        ExprData::Literal(optd_core::ScalarValue::Boolean(true))
    )
}

fn is_at_most_one_row(
    op: Operator,
    ctx: &QueryContext,
    catalog: &Arc<dyn Catalog>,
) -> ToPhysicalResult<bool> {
    let mut analyses = optd_core::AnalysisContext::new(Arc::clone(catalog));
    analyses
        .get::<optd_core::AtMostOneRow>(ctx, op)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))
}

fn reject_subquery_exprs(expr: optd_core::Expr, ctx: &QueryContext) -> ToPhysicalResult<()> {
    match expr.get(ctx) {
        ExprData::Exists { .. } | ExprData::InSubquery { .. } | ExprData::ScalarSubquery { .. } => {
            Err(ToPhysicalError::Unsupported("subquery expression".into()))
        }
        ExprData::Unary { expr, .. } | ExprData::Cast { expr, .. } => {
            reject_subquery_exprs(*expr, ctx)
        }
        ExprData::Binary { left, right, .. } => {
            reject_subquery_exprs(*left, ctx)?;
            reject_subquery_exprs(*right, ctx)
        }
        ExprData::Nary { exprs, .. } | ExprData::ScalarFunction { args: exprs, .. } => {
            for expr in exprs {
                reject_subquery_exprs(*expr, ctx)?;
            }
            Ok(())
        }
        ExprData::Like { expr, pattern, .. } => {
            reject_subquery_exprs(*expr, ctx)?;
            reject_subquery_exprs(*pattern, ctx)
        }
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                reject_subquery_exprs(*when, ctx)?;
                reject_subquery_exprs(*then, ctx)?;
            }
            if let Some(expr) = else_expr {
                reject_subquery_exprs(*expr, ctx)?;
            }
            Ok(())
        }
        ExprData::ColumnRef(_) | ExprData::Literal(_) => Ok(()),
    }
}

fn ensure_single_partition(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if input.output_partitioning().partition_count() == 1 {
        input
    } else {
        Arc::new(CoalescePartitionsExec::new(input))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ToPhysicalError, to_physical_plan};
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use optd_core::{
        ColumnData, ConstScan, ExprData, Join, JoinType, MemoryCatalog, OperatorData, PlannedQuery,
        QueryContext, ScalarValue,
    };

    fn planned(query: QueryContext) -> PlannedQuery {
        PlannedQuery::new(query, Arc::new(MemoryCatalog::new("memory", "public")))
    }

    #[tokio::test]
    async fn converts_literal_const_scan_values() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let col = ColumnData::new("a", DataType::Int32).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int32(1)).add(&mut ctx);
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![col],
            rows: vec![vec![one]],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let plan = to_physical_plan(&planned(ctx), &session).await.unwrap();
        let batches = collect(plan, session.state().task_ctx()).await.unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
    }

    #[tokio::test]
    async fn unsafe_single_join_fails_loudly() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let left_col = ColumnData::new("a", DataType::Int32).add(&mut ctx);
        let right_col = ColumnData::new("b", DataType::Int32).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int32(1)).add(&mut ctx);
        let two = ExprData::Literal(ScalarValue::Int32(2)).add(&mut ctx);
        let true_expr = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let left = OperatorData::ConstScan(ConstScan {
            columns: vec![left_col],
            rows: vec![vec![one]],
        })
        .add(&mut ctx);
        let right = OperatorData::ConstScan(ConstScan {
            columns: vec![right_col],
            rows: vec![vec![one], vec![two]],
        })
        .add(&mut ctx);
        let root = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on: true_expr,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let err = to_physical_plan(&planned(ctx), &session).await.unwrap_err();
        assert!(matches!(err, ToPhysicalError::Unsupported(msg) if msg.contains("Single join")));
    }
}
