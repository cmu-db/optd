//! Converts optd IR directly into DataFusion physical [`ExecutionPlan`]s.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr::create_physical_expr;
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
use datafusion_common::{DFSchema, DFSchemaRef, JoinSide, NullEquality, TableReference};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, create_physical_sort_exprs};
use optd_core::{
    BinaryOp, ExprData, JoinType, NaryOp, Operator, OperatorData, QueryContext, TableRef,
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
}

/// Converts a optd `QueryContext` root directly into a DataFusion physical plan.
pub async fn to_physical_plan(
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToPhysicalResult<Arc<dyn ExecutionPlan>> {
    let root = ctx
        .root()
        .ok_or_else(|| ToPhysicalError::Unsupported("query has no root".into()))?;
    let planned = convert_operator(root, ctx, session).await?;
    Ok(planned.exec)
}

async fn convert_operator(
    op: Operator,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    match op.get(ctx) {
        OperatorData::Scan(scan) => {
            let provider = session
                .table_provider(optd_table_ref_to_df(&scan.table))
                .await
                .map_err(|_| ToPhysicalError::TableNotFound(scan.table.clone()))?;
            let provider_schema = provider.schema();
            let projection = scan
                .columns
                .iter()
                .map(|column| {
                    let name = &ctx.column(*column).name;
                    provider_schema.index_of(name).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let args = ScanArgs::default().with_projection(Some(&projection));
            let state = session.state();
            let scanned = provider.scan_with_args(&state, args).await?;
            let schema = df_schema_for_columns(ctx, &scan.columns)?;
            Ok(PhysicalPlanNode {
                exec: Arc::clone(scanned.plan()),
                df_schema: schema,
            })
        }

        OperatorData::Selection(sel) => {
            reject_subquery_exprs(sel.predicate, ctx)?;
            let input = Box::pin(convert_operator(sel.input, ctx, session)).await?;
            let predicate = physical_expr(sel.predicate, ctx, session, input.df_schema.as_ref())?;
            let exec = FilterExecBuilder::new(predicate, input.exec)
                .with_batch_size(session.copied_config().batch_size())
                .build()?;
            Ok(PhysicalPlanNode {
                exec: Arc::new(exec),
                df_schema: input.df_schema,
            })
        }

        OperatorData::Projection(proj) => {
            let input = Box::pin(convert_operator(proj.input, ctx, session)).await?;
            project_columns(&proj.columns, input, ctx, session)
        }

        OperatorData::Map(map) => {
            for (_, expr) in &map.computations {
                reject_subquery_exprs(*expr, ctx)?;
            }
            let input = Box::pin(convert_operator(map.input, ctx, session)).await?;
            let available = available_columns(map.input, ctx)?;
            let mut exprs =
                projection_exprs_for_columns(&available, ctx, session, input.df_schema.as_ref())?;
            for (column, expr) in &map.computations {
                let physical = physical_expr(*expr, ctx, session, input.df_schema.as_ref())?;
                exprs.push(ProjectionExpr::new(
                    physical,
                    ctx.column(*column).name.clone(),
                ));
            }
            let mut columns = available;
            columns.extend(map.computations.iter().map(|(column, _)| *column));
            let schema = df_schema_for_columns(ctx, &columns)?;
            Ok(PhysicalPlanNode {
                exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
                df_schema: schema,
            })
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
            let input = Box::pin(convert_operator(agg.input, ctx, session)).await?;
            let input_schema = input.exec.schema();
            let empty_tables = TableMap::new();
            let mut expr_ctx = ToDfContext::new(ctx, &empty_tables, session);
            let group_exprs = agg
                .keys
                .iter()
                .map(|expr| {
                    let physical = physical_expr(*expr, ctx, session, input.df_schema.as_ref())?;
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
            let columns = output_columns(op, ctx)?;
            Ok(PhysicalPlanNode {
                exec: final_exec,
                df_schema: df_schema_for_columns(ctx, &columns)?,
            })
        }

        OperatorData::Sort(sort) => {
            for key in &sort.keys {
                reject_subquery_exprs(key.expr, ctx)?;
            }
            let input = Box::pin(convert_operator(sort.input, ctx, session)).await?;
            let empty_tables = TableMap::new();
            let mut expr_ctx = ToDfContext::new(ctx, &empty_tables, session);
            let logical = sort
                .keys
                .iter()
                .map(|key| {
                    Ok(datafusion::logical_expr::SortExpr {
                        expr: convert_expr(
                            key.expr,
                            &mut expr_ctx,
                            &Default::default(),
                            &ColumnQualifiers::new(),
                        )?,
                        asc: matches!(key.direction, optd_core::SortDirection::Asc),
                        nulls_first: matches!(key.nulls, optd_core::NullOrdering::First),
                    })
                })
                .collect::<ToPhysicalResult<Vec<_>>>()?;
            let sort_exprs = create_physical_sort_exprs(
                &logical,
                input.df_schema.as_ref(),
                session.state().execution_props(),
            )?;
            let ordering = LexOrdering::new(sort_exprs)
                .ok_or_else(|| ToPhysicalError::Unsupported("empty sort".into()))?;
            let input_exec = ensure_single_partition(input.exec);
            Ok(PhysicalPlanNode {
                exec: Arc::new(SortExec::new(ordering, input_exec)),
                df_schema: input.df_schema,
            })
        }

        OperatorData::Limit(limit) => {
            let input = Box::pin(convert_operator(limit.input, ctx, session)).await?;
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
            })
        }

        OperatorData::Join(join) => convert_join(join, ctx, session).await,

        OperatorData::CrossProduct(cross) => {
            let left = Box::pin(convert_operator(cross.outer, ctx, session)).await?;
            let right = Box::pin(convert_operator(cross.inner, ctx, session)).await?;
            Ok(join_node(
                Arc::new(CrossJoinExec::new(left.exec, right.exec)),
                join_output_columns_for_type(&JoinType::Inner, cross.outer, cross.inner, ctx)?,
                ctx,
            )?)
        }

        OperatorData::Output(out) => Box::pin(convert_operator(out.input, ctx, session)).await,

        OperatorData::Rename(rename) => {
            let input = Box::pin(convert_operator(rename.input, ctx, session)).await?;
            let original_columns = rename
                .defs
                .iter()
                .map(|(_, original)| *original)
                .collect::<Vec<_>>();
            let mut exprs = projection_exprs_for_columns(
                &original_columns,
                ctx,
                session,
                input.df_schema.as_ref(),
            )?;
            for (expr, (renamed, _)) in exprs.iter_mut().zip(&rename.defs) {
                *expr =
                    ProjectionExpr::new(Arc::clone(&expr.expr), ctx.column(*renamed).name.clone());
            }
            let renamed_columns = rename
                .defs
                .iter()
                .map(|(renamed, _)| *renamed)
                .collect::<Vec<_>>();
            Ok(PhysicalPlanNode {
                exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
                df_schema: df_schema_for_columns(ctx, &renamed_columns)?,
            })
        }

        OperatorData::ConstScan(const_scan) => match const_scan.rows.len() {
            0 => {
                let schema = schema_for_columns(ctx, &const_scan.columns);
                Ok(PhysicalPlanNode {
                    exec: Arc::new(EmptyExec::new(Arc::clone(&schema))),
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
                })
            }
            1 if const_scan.rows[0].is_empty() => {
                let schema = schema_for_columns(ctx, &const_scan.columns);
                Ok(PhysicalPlanNode {
                    exec: Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
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
                            .map(|expr| physical_expr(*expr, ctx, session, &df_schema))
                            .collect::<ToPhysicalResult<Vec<_>>>()
                    })
                    .collect::<ToPhysicalResult<Vec<_>>>()?;
                Ok(PhysicalPlanNode {
                    exec: MemorySourceConfig::try_new_as_values(Arc::clone(&schema), values)?,
                    df_schema: df_schema_for_columns(ctx, &const_scan.columns)?,
                })
            }
        },

        OperatorData::TableFunction(_) => Err(ToPhysicalError::Unsupported("TableFunction".into())),
    }
}

async fn convert_join(
    join: &optd_core::Join,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    if !free_columns_for(join.inner, ctx)?.is_empty() {
        return Err(ToPhysicalError::Unsupported("correlated join input".into()));
    }
    if matches!(join.join_type, JoinType::LeftMark(_)) {
        return Err(ToPhysicalError::Unsupported("LeftMark join".into()));
    }
    if matches!(join.join_type, JoinType::Single) && !is_at_most_one_row(join.inner, ctx)? {
        return Err(ToPhysicalError::Unsupported(
            "Single join without proven single-row inner input".into(),
        ));
    }
    reject_subquery_exprs(join.on, ctx)?;
    let left = Box::pin(convert_operator(join.outer, ctx, session)).await?;
    let right = Box::pin(convert_operator(join.inner, ctx, session)).await?;
    let df_join_type = convert_join_type(&join.join_type)?;
    if is_true_expr(join.on, ctx) {
        if matches!(join.join_type, JoinType::Inner) {
            return Ok(join_node(
                Arc::new(CrossJoinExec::new(left.exec, right.exec)),
                join_output_columns_for_type(&join.join_type, join.outer, join.inner, ctx)?,
                ctx,
            )?);
        }
        let exec = NestedLoopJoinExec::try_new(left.exec, right.exec, None, &df_join_type, None)?;
        return Ok(join_node(
            Arc::new(exec),
            join_output_columns_for_type(&join.join_type, join.outer, join.inner, ctx)?,
            ctx,
        )?);
    }

    if let Some((on, null_equality)) =
        equijoin_keys(join.on, join.outer, join.inner, ctx, session, &left, &right)?
    {
        let exec = HashJoinExec::try_new(
            left.exec,
            right.exec,
            on,
            None,
            &df_join_type,
            None,
            PartitionMode::CollectLeft,
            null_equality,
            false,
        )?;
        return Ok(join_node(
            Arc::new(exec),
            join_output_columns_for_type(&join.join_type, join.outer, join.inner, ctx)?,
            ctx,
        )?);
    }

    let filter = join_filter(join.on, ctx, session, &left, &right)?;
    let exec =
        NestedLoopJoinExec::try_new(left.exec, right.exec, Some(filter), &df_join_type, None)?;
    Ok(join_node(
        Arc::new(exec),
        join_output_columns_for_type(&join.join_type, join.outer, join.inner, ctx)?,
        ctx,
    )?)
}

fn project_columns(
    columns: &[optd_core::Column],
    input: PhysicalPlanNode,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToPhysicalResult<PhysicalPlanNode> {
    let exprs = projection_exprs_for_columns(columns, ctx, session, input.df_schema.as_ref())?;
    Ok(PhysicalPlanNode {
        exec: Arc::new(ProjectionExec::try_new(exprs, input.exec)?),
        df_schema: df_schema_for_columns(ctx, columns)?,
    })
}

fn projection_exprs_for_columns(
    columns: &[optd_core::Column],
    ctx: &QueryContext,
    session: &SessionContext,
    input_schema: &DFSchema,
) -> ToPhysicalResult<Vec<ProjectionExpr>> {
    columns
        .iter()
        .map(|column| {
            let logical = logical_column_expr(*column, ctx);
            let physical =
                create_physical_expr(&logical, input_schema, session.state().execution_props())?;
            Ok(ProjectionExpr::new(
                physical,
                ctx.column(*column).name.clone(),
            ))
        })
        .collect()
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

fn physical_expr(
    expr: optd_core::Expr,
    ctx: &QueryContext,
    session: &SessionContext,
    input_schema: &DFSchema,
) -> ToPhysicalResult<Arc<dyn PhysicalExpr>> {
    let empty_tables = TableMap::new();
    let mut expr_ctx = ToDfContext::new(ctx, &empty_tables, session);
    let logical = convert_expr(
        expr,
        &mut expr_ctx,
        &Default::default(),
        &ColumnQualifiers::new(),
    )?;
    Ok(create_physical_expr(
        &logical,
        input_schema,
        session.state().execution_props(),
    )?)
}

fn equijoin_keys(
    expr: optd_core::Expr,
    outer: Operator,
    inner: Operator,
    ctx: &QueryContext,
    session: &SessionContext,
    left: &PhysicalPlanNode,
    right: &PhysicalPlanNode,
) -> ToPhysicalResult<Option<(JoinOn, NullEquality)>> {
    let conjuncts = split_conjuncts(expr, ctx);
    let outer_cols = available_columns_set(ctx, outer)?;
    let inner_cols = available_columns_set(ctx, inner)?;
    let mut keys = Vec::new();
    let mut null_equality = None;
    for conjunct in conjuncts {
        let ExprData::Binary {
            op: BinaryOp::Eq | BinaryOp::IsNotDistinctFrom,
            left: l,
            right: r,
        } = conjunct.get(ctx)
        else {
            return Ok(None);
        };
        let key_null_equality = match conjunct.get(ctx) {
            ExprData::Binary {
                op: BinaryOp::Eq, ..
            } => NullEquality::NullEqualsNothing,
            ExprData::Binary {
                op: BinaryOp::IsNotDistinctFrom,
                ..
            } => NullEquality::NullEqualsNull,
            _ => unreachable!(),
        };
        if let Some(existing) = null_equality {
            if existing != key_null_equality {
                return Ok(None);
            }
        } else {
            null_equality = Some(key_null_equality);
        }
        let Some(l_col) = column_ref(*l, ctx) else {
            return Ok(None);
        };
        let Some(r_col) = column_ref(*r, ctx) else {
            return Ok(None);
        };
        let (left_expr, right_expr) = if outer_cols.contains(&l_col) && inner_cols.contains(&r_col)
        {
            (*l, *r)
        } else if outer_cols.contains(&r_col) && inner_cols.contains(&l_col) {
            (*r, *l)
        } else {
            return Ok(None);
        };
        keys.push((
            physical_expr(left_expr, ctx, session, left.df_schema.as_ref())?,
            physical_expr(right_expr, ctx, session, right.df_schema.as_ref())?,
        ));
    }
    Ok(Some((
        keys,
        null_equality.unwrap_or(NullEquality::NullEqualsNothing),
    )))
}

fn join_filter(
    expr: optd_core::Expr,
    ctx: &QueryContext,
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
    let expression = physical_expr(expr, ctx, session, &df_schema)?;
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
    })
}

fn join_output_columns_for_type(
    join_type: &JoinType,
    outer: Operator,
    inner: Operator,
    ctx: &QueryContext,
) -> ToPhysicalResult<Vec<optd_core::Column>> {
    let mut columns = available_columns(outer, ctx)?;
    if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return Ok(columns);
    }
    columns.extend(available_columns(inner, ctx)?);
    Ok(columns)
}

fn output_columns(op: Operator, ctx: &QueryContext) -> ToPhysicalResult<Vec<optd_core::Column>> {
    available_columns(op, ctx)
}

fn available_columns(op: Operator, ctx: &QueryContext) -> ToPhysicalResult<Vec<optd_core::Column>> {
    let mut analyses = optd_core::AnalysisContext::new();
    analyses
        .get::<optd_core::AvailableColumns>(ctx, op)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))
}

fn available_columns_set(
    ctx: &QueryContext,
    op: Operator,
) -> ToPhysicalResult<HashSet<optd_core::Column>> {
    Ok(available_columns(op, ctx)?.into_iter().collect())
}

fn free_columns_for(
    op: Operator,
    ctx: &QueryContext,
) -> ToPhysicalResult<HashSet<optd_core::Column>> {
    let mut analyses = optd_core::AnalysisContext::new();
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

fn is_at_most_one_row(op: Operator, ctx: &QueryContext) -> ToPhysicalResult<bool> {
    let mut analyses = optd_core::AnalysisContext::new();
    let profile = analyses
        .get::<optd_core::CardinalityEstimationV1>(ctx, op)
        .map_err(|e| ToPhysicalError::Unsupported(e.to_string()))?;
    Ok(profile.rows.upper.is_some_and(|upper| upper <= 1.0))
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
    use super::{ToPhysicalError, to_physical_plan};
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use optd_core::{
        ColumnData, ConstScan, ExprData, Join, JoinType, OperatorData, QueryContext, ScalarValue,
    };

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

        let plan = to_physical_plan(&ctx, &session).await.unwrap();
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

        let err = to_physical_plan(&ctx, &session).await.unwrap_err();
        assert!(matches!(err, ToPhysicalError::Unsupported(msg) if msg.contains("Single join")));
    }
}
