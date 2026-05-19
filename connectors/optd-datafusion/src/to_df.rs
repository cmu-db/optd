//! Converts optd IR into DataFusion [`LogicalPlan`] for execution.
//!
//! Table providers are collected asynchronously once in [`to_logical_plan`];
//! all subsequent conversion is synchronous.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::common::{Column as DFColumn, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{
    Expr as DFExpr, JoinType as DFJoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, TableSource,
    logical_plan::Values,
};
use datafusion::prelude::SessionContext;
use optd::{
    AggregateExpr, AggregateFunction, BinaryOp, ExprData, NaryOp, Operator, OperatorData,
    QueryContext, Relation, ScalarValue, TableRef, UnaryOp,
};

/// Error type for the optd → DataFusion converter.
#[derive(Debug)]
pub enum ToDFError {
    Unsupported(String),
    Build(datafusion::error::DataFusionError),
    TableNotFound(TableRef),
}

impl std::fmt::Display for ToDFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "unsupported IR node: {msg}"),
            Self::Build(e) => write!(f, "plan build error: {e}"),
            Self::TableNotFound(t) => write!(f, "table not found: {t}"),
        }
    }
}

impl std::error::Error for ToDFError {}

impl From<datafusion::error::DataFusionError> for ToDFError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Build(e)
    }
}

pub type ToDFResult<T> = Result<T, ToDFError>;

type TableMap = HashMap<TableRef, Arc<dyn TableSource>>;
type ColumnQualifiers = HashMap<optd::Column, String>;

fn optd_table_ref_to_df(table: &TableRef) -> TableReference {
    match table {
        TableRef::Bare { table } => TableReference::bare(table.as_ref()),
        TableRef::Partial { schema, table } => {
            TableReference::partial(schema.as_ref(), table.as_ref())
        }
        TableRef::Full {
            catalog,
            schema,
            table,
        } => TableReference::full(catalog.as_ref(), schema.as_ref(), table.as_ref()),
    }
}

/// Converts a optd `QueryContext` root into a DataFusion `LogicalPlan`.
pub async fn to_logical_plan(
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToDFResult<LogicalPlan> {
    let root = ctx
        .root()
        .ok_or_else(|| ToDFError::Unsupported("query has no root".into()))?;

    // Collect all table sources referenced in the plan (async, done once).
    let tables = collect_tables(root, ctx, session).await?;

    convert_operator(root, ctx, &tables, session)
}

/// Walks the plan and collects a `TableSource` for every `Scan`.
async fn collect_tables(
    root: Operator,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToDFResult<TableMap> {
    let mut map: TableMap = HashMap::new();
    let mut stack = vec![root];
    let mut visited = std::collections::HashSet::new();
    while let Some(op) = stack.pop() {
        if !visited.insert(op) {
            continue;
        }
        if let OperatorData::Scan(scan) = op.get(ctx)
            && !map.contains_key(&scan.table)
        {
            let provider = session
                .table_provider(optd_table_ref_to_df(&scan.table))
                .await
                .map_err(|_| ToDFError::TableNotFound(scan.table.clone()))?;
            map.insert(scan.table.clone(), provider_as_source(provider));
        }
        stack.extend(op.get(ctx).inputs());
        // Also walk into subquery operators embedded in expressions.
        stack.extend(collect_subquery_operators(op, ctx));
    }
    Ok(map)
}

/// Collects subquery `Operator` handles embedded in expressions of `op`.
fn collect_subquery_operators(op: Operator, ctx: &QueryContext) -> Vec<Operator> {
    let mut result = Vec::new();
    collect_expr_subqueries_for_operator(op, ctx, &mut result);
    result
}

fn collect_expr_subqueries_for_operator(op: Operator, ctx: &QueryContext, out: &mut Vec<Operator>) {
    let collect_expr = |expr: optd::Expr, out: &mut Vec<Operator>| {
        collect_expr_subqueries(expr, ctx, out);
    };
    match op.get(ctx) {
        OperatorData::Selection(s) => collect_expr(s.predicate, out),
        OperatorData::Map(m) => {
            for (_, e) in &m.computations {
                collect_expr(*e, out);
            }
        }
        OperatorData::Join(j) => collect_expr(j.on, out),
        OperatorData::Aggregation(a) => {
            for e in &a.keys {
                collect_expr(*e, out);
            }
        }
        _ => {}
    }
}

fn collect_expr_subqueries(expr: optd::Expr, ctx: &QueryContext, out: &mut Vec<Operator>) {
    match expr.get(ctx) {
        ExprData::Exists { subquery, .. }
        | ExprData::ScalarSubquery { subquery }
        | ExprData::InSubquery { subquery, .. } => out.push(*subquery),
        ExprData::Unary { expr, .. } | ExprData::Cast { expr, .. } => {
            collect_expr_subqueries(*expr, ctx, out);
        }
        ExprData::Binary { left, right, .. } => {
            collect_expr_subqueries(*left, ctx, out);
            collect_expr_subqueries(*right, ctx, out);
        }
        ExprData::Nary { exprs, .. } | ExprData::ScalarFunction { args: exprs, .. } => {
            for e in exprs {
                collect_expr_subqueries(*e, ctx, out);
            }
        }
        ExprData::Like { expr, pattern, .. } => {
            collect_expr_subqueries(*expr, ctx, out);
            collect_expr_subqueries(*pattern, ctx, out);
        }
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (w, t) in when_then {
                collect_expr_subqueries(*w, ctx, out);
                collect_expr_subqueries(*t, ctx, out);
            }
            if let Some(e) = else_expr {
                collect_expr_subqueries(*e, ctx, out);
            }
        }
        _ => {}
    }
}

fn convert_operator(
    op: Operator,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
) -> ToDFResult<LogicalPlan> {
    convert_operator_inner(
        op,
        ctx,
        tables,
        session,
        &std::collections::HashSet::new(),
        &ColumnQualifiers::new(),
    )
}

fn convert_operator_inner(
    op: Operator,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<LogicalPlan> {
    match op.get(ctx) {
        OperatorData::Scan(scan) => {
            let source = tables
                .get(&scan.table)
                .ok_or_else(|| ToDFError::TableNotFound(scan.table.clone()))?
                .clone();
            let plan = LogicalPlanBuilder::scan(optd_table_ref_to_df(&scan.table), source, None)?
                .build()?;
            if let Some(alias) = scan
                .columns
                .iter()
                .find_map(|col| column_qualifiers.get(col))
            {
                Ok(LogicalPlanBuilder::from(plan).alias(alias)?.build()?)
            } else {
                Ok(plan)
            }
        }

        OperatorData::Selection(sel) => {
            if let OperatorData::Join(join) = sel.input.get(ctx)
                && matches!(join.join_type, optd::JoinType::Single)
                && let Some(scalar_col) = single_available_column(join.inner, ctx)
            {
                let input = convert_operator_inner(
                    join.outer,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?;
                let scalar_expr = scalar_subquery_expr(join.inner, join.on, ctx, tables, session)?;
                let predicate = convert_expr_replacing_column(
                    sel.predicate,
                    scalar_col,
                    &scalar_expr,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?;
                return Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?);
            }
            if let OperatorData::Join(join) = sel.input.get(ctx)
                && let optd::JoinType::LeftMark(marker) = join.join_type
                && let Some((join_type, remaining)) =
                    classify_mark_selection(sel.predicate, marker, ctx)
            {
                let join = optd::Join {
                    join_type,
                    on: join.on,
                    outer: join.outer,
                    inner: join.inner,
                };
                let mut plan = convert_semi_anti_join(
                    &join,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?;
                if !remaining.is_empty() {
                    let predicate = convert_conjuncts(
                        remaining,
                        ctx,
                        tables,
                        session,
                        outer_refs,
                        column_qualifiers,
                    )?;
                    plan = LogicalPlanBuilder::from(plan).filter(predicate)?.build()?;
                }
                return Ok(plan);
            }
            let input = convert_operator_inner(
                sel.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let predicate = convert_expr(
                sel.predicate,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?)
        }

        OperatorData::Projection(proj) => {
            let input = convert_operator_inner(
                proj.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let input_schema = input.schema().clone();
            let exprs: Vec<DFExpr> = proj
                .columns
                .iter()
                .map(|col| {
                    let cd = ctx.column(*col);
                    let qualifier = column_qualifiers
                        .get(col)
                        .map(String::as_str)
                        .or(cd.qualifier.as_deref());
                    // Use qualified ref only if the name is genuinely ambiguous
                    // (multiple distinct qualified fields with that name).
                    let count = input_schema
                        .qualified_fields_with_unqualified_name(&cd.name)
                        .len();
                    if count > 1 {
                        match qualifier {
                            Some(q) => DFExpr::Column(DFColumn::new(Some(q), &cd.name)),
                            None => DFExpr::Column(DFColumn::new_unqualified(&cd.name)),
                        }
                    } else {
                        DFExpr::Column(DFColumn::new_unqualified(&cd.name))
                    }
                })
                .collect();
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Map(map) => {
            let input = convert_operator_inner(
                map.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let input_schema = input.schema().clone();
            let mut exprs: Vec<DFExpr> = (0..input_schema.fields().len())
                .map(|i| {
                    let (qualifier, field) = input_schema.qualified_field(i);
                    match qualifier {
                        Some(q) => DFExpr::Column(DFColumn::new(Some(q.table()), field.name())),
                        None => DFExpr::Column(DFColumn::new_unqualified(field.name())),
                    }
                })
                .collect();
            for (col, expr) in &map.computations {
                let name = ctx.column(*col).name.clone();
                exprs.push(
                    convert_expr(*expr, ctx, tables, session, outer_refs, column_qualifiers)?
                        .alias(name),
                );
            }
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Aggregation(agg) => {
            let input = convert_operator_inner(
                agg.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let input_schema = input.schema().clone();
            let group_exprs: Vec<DFExpr> = agg
                .keys
                .iter()
                .map(|e| {
                    let expr =
                        convert_expr(*e, ctx, tables, session, outer_refs, column_qualifiers)?;
                    Ok(normalize_col_ref(expr, &input_schema))
                })
                .collect::<ToDFResult<_>>()?;
            let aggr_exprs: Vec<DFExpr> = agg
                .aggregates
                .iter()
                .map(|(col, agg_expr)| {
                    let name = ctx.column(*col).name.clone();
                    let expr = convert_agg_expr(
                        agg_expr,
                        ctx,
                        tables,
                        session,
                        outer_refs,
                        column_qualifiers,
                    )?;
                    // Normalize column refs inside aggregate args.
                    Ok(normalize_expr_cols(expr, &input_schema).alias(name))
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input)
                .aggregate(group_exprs, aggr_exprs)?
                .build()?)
        }

        OperatorData::Sort(sort) => {
            let input = convert_operator_inner(
                sort.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let sort_exprs: Vec<SortExpr> = sort
                .keys
                .iter()
                .map(|key| {
                    Ok(SortExpr {
                        expr: convert_expr(
                            key.expr,
                            ctx,
                            tables,
                            session,
                            outer_refs,
                            column_qualifiers,
                        )?,
                        asc: matches!(key.direction, optd::SortDirection::Asc),
                        nulls_first: matches!(key.nulls, optd::NullOrdering::First),
                    })
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input).sort(sort_exprs)?.build()?)
        }

        OperatorData::Limit(limit) => {
            let input = convert_operator_inner(
                limit.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            Ok(LogicalPlanBuilder::from(input)
                .limit(limit.offset, limit.fetch)?
                .build()?)
        }

        OperatorData::Join(join) => {
            let outer = convert_operator_inner(
                join.outer,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let join_type = convert_join_type(&join.join_type)?;
            let semi_anti_outer_refs = correlated_semi_anti_outer_refs(ctx, join);
            if matches!(
                join.join_type,
                optd::JoinType::LeftSemi | optd::JoinType::LeftAnti
            ) && !semi_anti_outer_refs.is_empty()
            {
                let conflicts = available_qualifiers(join.outer, ctx);
                let inner_qualifiers = lateral_column_qualifiers(join.inner, ctx, &conflicts);
                let mut inner = convert_operator_inner(
                    join.inner,
                    ctx,
                    tables,
                    session,
                    &semi_anti_outer_refs,
                    &inner_qualifiers,
                )?;
                if !is_true_expr(join.on, ctx) {
                    let condition = convert_expr(
                        join.on,
                        ctx,
                        tables,
                        session,
                        &semi_anti_outer_refs,
                        &inner_qualifiers,
                    )?;
                    inner = LogicalPlanBuilder::from(inner).filter(condition)?.build()?;
                }
                let exists = DFExpr::Exists(datafusion::logical_expr::expr::Exists {
                    subquery: datafusion::logical_expr::Subquery {
                        subquery: Arc::new(inner),
                        outer_ref_columns: free_to_df_cols(ctx, &semi_anti_outer_refs),
                        spans: Default::default(),
                    },
                    negated: matches!(join.join_type, optd::JoinType::LeftAnti),
                });
                return Ok(LogicalPlanBuilder::from(outer).filter(exists)?.build()?);
            }

            if let optd::JoinType::LeftMark(marker) = join.join_type {
                let inner = convert_operator_inner(
                    join.inner,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?;
                let condition = convert_join_condition(
                    join.on,
                    join.outer,
                    join.inner,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?;
                let plan = LogicalPlanBuilder::from(outer)
                    .join_on(inner, DFJoinType::LeftMark, Some(condition))?
                    .build()?;
                return project_mark_column(plan, &ctx.column(marker).name);
            }

            let free = free_columns_for(ctx, join.inner);
            let use_lateral_subquery = !free.is_empty()
                && matches!(
                    join.join_type,
                    optd::JoinType::Inner | optd::JoinType::Single
                );
            let lateral_qualifiers;
            let inner_qualifiers = if use_lateral_subquery {
                let conflicts = available_qualifiers(join.outer, ctx);
                lateral_qualifiers = lateral_column_qualifiers(join.inner, ctx, &conflicts);
                &lateral_qualifiers
            } else {
                column_qualifiers
            };
            let inner_refs = if use_lateral_subquery {
                &free
            } else {
                outer_refs
            };
            let mut inner = convert_operator_inner(
                join.inner,
                ctx,
                tables,
                session,
                inner_refs,
                inner_qualifiers,
            )?;
            if use_lateral_subquery {
                inner = LogicalPlan::Subquery(datafusion::logical_expr::Subquery {
                    subquery: Arc::new(inner),
                    outer_ref_columns: free_to_df_cols(ctx, &free),
                    spans: Default::default(),
                });
            }
            let condition = convert_join_condition(
                join.on,
                join.outer,
                join.inner,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let df_join_type = if use_lateral_subquery {
                DFJoinType::Inner
            } else {
                join_type
            };
            Ok(LogicalPlanBuilder::from(outer)
                .join_on(inner, df_join_type, Some(condition))?
                .build()?)
        }

        OperatorData::CrossProduct(cross) => {
            let outer = convert_operator_inner(
                cross.outer,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let inner = convert_operator_inner(
                cross.inner,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            Ok(LogicalPlanBuilder::from(outer).cross_join(inner)?.build()?)
        }

        OperatorData::Output(out) => convert_operator_inner(
            out.input,
            ctx,
            tables,
            session,
            outer_refs,
            column_qualifiers,
        ),

        OperatorData::Rename(r) => {
            let input = convert_operator_inner(
                r.input,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let input_schema = input.schema().clone();
            let exprs = r
                .defs
                .iter()
                .map(|(renamed, original)| {
                    let renamed_name = ctx.column(*renamed).name.clone();
                    let original_name = ctx.column(*original).name.clone();
                    let expr = normalize_col_ref(
                        DFExpr::Column(DFColumn::new_unqualified(original_name)),
                        &input_schema,
                    );
                    expr.alias(renamed_name)
                })
                .collect::<Vec<_>>();
            Ok(LogicalPlanBuilder::from(input)
                .project(exprs)?
                .alias(r.alias.as_str())?
                .build()?)
        }

        OperatorData::TableFunction(_) => Err(ToDFError::Unsupported("TableFunction".into())),

        OperatorData::ConstScan(const_scan) => {
            use datafusion::logical_expr::EmptyRelation;
            let schema = const_scan_schema(const_scan, ctx)?;
            match const_scan.rows.len() {
                0 => Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema,
                })),
                1 if const_scan.rows[0].is_empty() => {
                    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: true,
                        schema,
                    }))
                }
                _ => {
                    let values = const_scan
                        .rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|expr| {
                                    convert_expr(
                                        *expr,
                                        ctx,
                                        tables,
                                        session,
                                        outer_refs,
                                        column_qualifiers,
                                    )
                                })
                                .collect::<ToDFResult<Vec<_>>>()
                        })
                        .collect::<ToDFResult<Vec<_>>>()?;
                    if values
                        .iter()
                        .any(|row| row.len() != const_scan.columns.len())
                    {
                        return Err(ToDFError::Unsupported(
                            "ConstScan row width must match number of columns".into(),
                        ));
                    }
                    Ok(LogicalPlan::Values(Values { schema, values }))
                }
            }
        }
    }
}

fn const_scan_schema(
    const_scan: &optd::ConstScan,
    ctx: &QueryContext,
) -> ToDFResult<datafusion::common::DFSchemaRef> {
    use datafusion::common::DFSchema;
    let qualified_fields = const_scan
        .columns
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
    let schema = if qualified_fields.is_empty() {
        DFSchema::empty()
    } else {
        DFSchema::new_with_metadata(qualified_fields, HashMap::new())?
    };
    Ok(Arc::new(schema))
}

fn convert_semi_anti_join(
    join: &optd::Join,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<LogicalPlan> {
    let outer = convert_operator_inner(
        join.outer,
        ctx,
        tables,
        session,
        outer_refs,
        column_qualifiers,
    )?;
    let semi_anti_outer_refs = correlated_semi_anti_outer_refs(ctx, join);
    if !semi_anti_outer_refs.is_empty() {
        let conflicts = available_qualifiers(join.outer, ctx);
        let inner_qualifiers = lateral_column_qualifiers(join.inner, ctx, &conflicts);
        let mut inner = convert_operator_inner(
            join.inner,
            ctx,
            tables,
            session,
            &semi_anti_outer_refs,
            &inner_qualifiers,
        )?;
        if !is_true_expr(join.on, ctx) {
            let condition = convert_expr(
                join.on,
                ctx,
                tables,
                session,
                &semi_anti_outer_refs,
                &inner_qualifiers,
            )?;
            inner = LogicalPlanBuilder::from(inner).filter(condition)?.build()?;
        }
        let exists = DFExpr::Exists(datafusion::logical_expr::expr::Exists {
            subquery: datafusion::logical_expr::Subquery {
                subquery: Arc::new(inner),
                outer_ref_columns: free_to_df_cols(ctx, &semi_anti_outer_refs),
                spans: Default::default(),
            },
            negated: matches!(join.join_type, optd::JoinType::LeftAnti),
        });
        return Ok(LogicalPlanBuilder::from(outer).filter(exists)?.build()?);
    }

    let inner = convert_operator_inner(
        join.inner,
        ctx,
        tables,
        session,
        outer_refs,
        column_qualifiers,
    )?;
    let condition = convert_join_condition(
        join.on,
        join.outer,
        join.inner,
        ctx,
        tables,
        session,
        outer_refs,
        column_qualifiers,
    )?;
    Ok(LogicalPlanBuilder::from(outer)
        .join_on(inner, convert_join_type(&join.join_type)?, Some(condition))?
        .build()?)
}

#[allow(clippy::too_many_arguments)]
fn convert_join_condition(
    on: optd::Expr,
    outer: Operator,
    inner: Operator,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    let outer_cols = available_columns_set(ctx, outer);
    let inner_cols = available_columns_set(ctx, inner);
    convert_join_condition_inner(
        on,
        &outer_cols,
        &inner_cols,
        ctx,
        tables,
        session,
        outer_refs,
        column_qualifiers,
    )
}

#[allow(clippy::too_many_arguments)]
fn convert_join_condition_inner(
    expr: optd::Expr,
    outer_cols: &HashSet<optd::Column>,
    inner_cols: &HashSet<optd::Column>,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            let mut exprs = exprs.iter();
            let first = exprs
                .next()
                .ok_or_else(|| ToDFError::Unsupported("empty join condition".into()))?;
            let first = convert_join_condition_inner(
                *first,
                outer_cols,
                inner_cols,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            exprs.try_fold(first, |acc, expr| {
                Ok(acc.and(convert_join_condition_inner(
                    *expr,
                    outer_cols,
                    inner_cols,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?))
            })
        }
        ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            let left_col = column_ref(*left, ctx);
            let right_col = column_ref(*right, ctx);
            let swap = left_col.is_some_and(|col| inner_cols.contains(&col))
                && right_col.is_some_and(|col| outer_cols.contains(&col));
            let (left, right) = if swap {
                (*right, *left)
            } else {
                (*left, *right)
            };
            let left = convert_expr(left, ctx, tables, session, outer_refs, column_qualifiers)?;
            let right = convert_expr(right, ctx, tables, session, outer_refs, column_qualifiers)?;
            Ok(left.eq(right))
        }
        _ => convert_expr(expr, ctx, tables, session, outer_refs, column_qualifiers),
    }
}

fn convert_conjuncts(
    mut exprs: Vec<optd::Expr>,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    let first = exprs
        .pop()
        .ok_or_else(|| ToDFError::Unsupported("empty conjunct list".into()))?;
    let first = convert_expr(first, ctx, tables, session, outer_refs, column_qualifiers)?;
    exprs.into_iter().try_fold(first, |acc, expr| {
        Ok(acc.and(convert_expr(
            expr,
            ctx,
            tables,
            session,
            outer_refs,
            column_qualifiers,
        )?))
    })
}

fn single_available_column(op: Operator, ctx: &QueryContext) -> Option<optd::Column> {
    let mut analyses = optd::AnalysisContext::new();
    let cols = analyses
        .get::<optd::AvailableColumns>(ctx, op)
        .unwrap_or_default();
    (cols.len() == 1).then_some(cols[0])
}

fn available_columns_set(ctx: &QueryContext, op: Operator) -> HashSet<optd::Column> {
    let mut analyses = optd::AnalysisContext::new();
    analyses
        .get::<optd::AvailableColumns>(ctx, op)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn column_ref(expr: optd::Expr, ctx: &QueryContext) -> Option<optd::Column> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) => Some(*col),
        _ => None,
    }
}

fn scalar_subquery_expr(
    subquery: Operator,
    on: optd::Expr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
) -> ToDFResult<DFExpr> {
    let free = correlated_subquery_outer_refs(ctx, subquery, on);
    let conflicts = free
        .iter()
        .filter_map(|column| ctx.column(*column).qualifier.clone())
        .collect();
    let inner_qualifiers = lateral_column_qualifiers(subquery, ctx, &conflicts);
    let mut inner =
        convert_operator_inner(subquery, ctx, tables, session, &free, &inner_qualifiers)?;
    if !is_true_expr(on, ctx) {
        let condition = convert_expr(on, ctx, tables, session, &free, &inner_qualifiers)?;
        inner = LogicalPlanBuilder::from(inner).filter(condition)?.build()?;
    }
    Ok(DFExpr::ScalarSubquery(datafusion::logical_expr::Subquery {
        subquery: Arc::new(inner),
        outer_ref_columns: free_to_df_cols(ctx, &free),
        spans: Default::default(),
    }))
}

fn correlated_subquery_outer_refs(
    ctx: &QueryContext,
    subquery: Operator,
    on: optd::Expr,
) -> std::collections::HashSet<optd::Column> {
    let mut outer_refs = free_columns_for(ctx, subquery);
    let mut analyses = optd::AnalysisContext::new();
    let inner_cols: std::collections::HashSet<_> = analyses
        .get::<optd::AvailableColumns>(ctx, subquery)
        .unwrap_or_default()
        .into_iter()
        .collect();
    if let Ok(used) = optd::expr_used_columns(ctx, on) {
        outer_refs.extend(used.into_iter().filter(|col| !inner_cols.contains(col)));
    }
    outer_refs
}

fn convert_expr_replacing_column(
    expr: optd::Expr,
    replace_col: optd::Column,
    replacement: &DFExpr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) if *col == replace_col => Ok(replacement.clone()),
        ExprData::ColumnRef(_) | ExprData::Literal(_) => {
            convert_expr(expr, ctx, tables, session, outer_refs, column_qualifiers)
        }
        ExprData::Unary { op, expr } => {
            let inner = convert_expr_replacing_column(
                *expr,
                replace_col,
                replacement,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            Ok(match op {
                UnaryOp::Not => datafusion::logical_expr::not(inner),
                UnaryOp::IsNull => inner.is_null(),
                UnaryOp::IsNotNull => inner.is_not_null(),
                UnaryOp::Negate => DFExpr::Negative(Box::new(inner)),
            })
        }
        ExprData::Binary { op, left, right } => {
            let l = convert_expr_replacing_column(
                *left,
                replace_col,
                replacement,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            let r = convert_expr_replacing_column(
                *right,
                replace_col,
                replacement,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?;
            Ok(match op {
                BinaryOp::Eq => l.eq(r),
                BinaryOp::NotEq => l.not_eq(r),
                BinaryOp::Lt => l.lt(r),
                BinaryOp::LtEq => l.lt_eq(r),
                BinaryOp::Gt => l.gt(r),
                BinaryOp::GtEq => l.gt_eq(r),
                BinaryOp::Add => l + r,
                BinaryOp::Subtract => l - r,
                BinaryOp::Multiply => l * r,
                BinaryOp::Divide => l / r,
            })
        }
        ExprData::Nary { op, exprs } => {
            let mut iter = exprs.iter().map(|expr| {
                convert_expr_replacing_column(
                    *expr,
                    replace_col,
                    replacement,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )
            });
            let first = iter
                .next()
                .ok_or_else(|| ToDFError::Unsupported("empty nary expr".into()))??;
            iter.try_fold(first, |acc, expr| {
                let expr = expr?;
                Ok(match op {
                    NaryOp::And => acc.and(expr),
                    NaryOp::Or => acc.or(expr),
                })
            })
        }
        _ => convert_expr(expr, ctx, tables, session, outer_refs, column_qualifiers),
    }
}

fn convert_expr(
    expr: optd::Expr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) => {
            let cd = ctx.column(*col);
            if outer_refs.contains(col) {
                // Correlated outer reference.
                let field = std::sync::Arc::new(datafusion::arrow::datatypes::Field::new(
                    &cd.name,
                    cd.ty.clone(),
                    true,
                ));
                let df_col = match &cd.qualifier {
                    Some(q) => DFColumn::new(Some(q.as_str()), &cd.name),
                    None => DFColumn::new_unqualified(&cd.name),
                };
                Ok(DFExpr::OuterReferenceColumn(field, df_col))
            } else {
                let qualifier = column_qualifiers
                    .get(col)
                    .map(String::as_str)
                    .or(cd.qualifier.as_deref());
                Ok(match qualifier {
                    Some(q) => DFExpr::Column(DFColumn::new(Some(q), &cd.name)),
                    None => DFExpr::Column(DFColumn::new_unqualified(&cd.name)),
                })
            }
        }
        ExprData::Literal(scalar) => Ok(DFExpr::Literal(convert_scalar(scalar)?, None)),
        ExprData::Unary { op, expr } => {
            let inner = convert_expr(*expr, ctx, tables, session, outer_refs, column_qualifiers)?;
            Ok(match op {
                UnaryOp::Not => datafusion::logical_expr::not(inner),
                UnaryOp::IsNull => inner.is_null(),
                UnaryOp::IsNotNull => inner.is_not_null(),
                UnaryOp::Negate => DFExpr::Negative(Box::new(inner)),
            })
        }
        ExprData::Binary { op, left, right } => {
            let l = convert_expr(*left, ctx, tables, session, outer_refs, column_qualifiers)?;
            let r = convert_expr(*right, ctx, tables, session, outer_refs, column_qualifiers)?;
            Ok(match op {
                BinaryOp::Eq => l.eq(r),
                BinaryOp::NotEq => l.not_eq(r),
                BinaryOp::Lt => l.lt(r),
                BinaryOp::LtEq => l.lt_eq(r),
                BinaryOp::Gt => l.gt(r),
                BinaryOp::GtEq => l.gt_eq(r),
                BinaryOp::Add => l + r,
                BinaryOp::Subtract => l - r,
                BinaryOp::Multiply => l * r,
                BinaryOp::Divide => l / r,
            })
        }
        ExprData::Nary { op, exprs } => {
            let mut iter = exprs
                .iter()
                .map(|e| convert_expr(*e, ctx, tables, session, outer_refs, column_qualifiers));
            let first = iter
                .next()
                .ok_or_else(|| ToDFError::Unsupported("empty nary expr".into()))??;
            iter.try_fold(first, |acc, e| {
                let e = e?;
                Ok(match op {
                    NaryOp::And => acc.and(e),
                    NaryOp::Or => acc.or(e),
                })
            })
        }
        ExprData::Cast { expr, ty } => Ok(DFExpr::Cast(datafusion::logical_expr::Cast {
            expr: Box::new(convert_expr(
                *expr,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?),
            data_type: ty.clone(),
        })),
        ExprData::ScalarFunction { function, args } => {
            use optd::ScalarFunction;
            let df_args: Vec<DFExpr> = args
                .iter()
                .map(|a| convert_expr(*a, ctx, tables, session, outer_refs, column_qualifiers))
                .collect::<ToDFResult<_>>()?;
            match function {
                ScalarFunction::Extension(name) => match session.udf(name) {
                    Ok(udf) => Ok(DFExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: udf,
                            args: df_args,
                        },
                    )),
                    Err(_) => Err(ToDFError::Unsupported(format!("scalar function {name}"))),
                },
                _ => Err(ToDFError::Unsupported(format!(
                    "scalar function {function}"
                ))),
            }
        }
        ExprData::Like {
            negated,
            expr,
            pattern,
            case_insensitive,
        } => Ok(DFExpr::Like(datafusion::logical_expr::Like {
            negated: *negated,
            expr: Box::new(convert_expr(
                *expr,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?),
            pattern: Box::new(convert_expr(
                *pattern,
                ctx,
                tables,
                session,
                outer_refs,
                column_qualifiers,
            )?),
            escape_char: None,
            case_insensitive: *case_insensitive,
        })),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            let mut builder = datafusion::logical_expr::when(
                convert_expr(
                    when_then[0].0,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?,
                convert_expr(
                    when_then[0].1,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?,
            );
            for (w, t) in &when_then[1..] {
                builder = builder.when(
                    convert_expr(*w, ctx, tables, session, outer_refs, column_qualifiers)?,
                    convert_expr(*t, ctx, tables, session, outer_refs, column_qualifiers)?,
                );
            }
            Ok(if let Some(else_e) = else_expr {
                builder.otherwise(convert_expr(
                    *else_e,
                    ctx,
                    tables,
                    session,
                    outer_refs,
                    column_qualifiers,
                )?)?
            } else {
                builder.end()?
            })
        }
        ExprData::Exists { subquery, negated } => {
            let free = free_columns_for(ctx, *subquery);
            let inner =
                convert_operator_inner(*subquery, ctx, tables, session, &free, column_qualifiers)?;
            Ok(DFExpr::Exists(datafusion::logical_expr::expr::Exists {
                subquery: datafusion::logical_expr::Subquery {
                    subquery: Arc::new(inner),
                    outer_ref_columns: free_to_df_cols(ctx, &free),
                    spans: Default::default(),
                },
                negated: *negated,
            }))
        }
        ExprData::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let inner_expr =
                convert_expr(*expr, ctx, tables, session, outer_refs, column_qualifiers)?;
            let free = free_columns_for(ctx, *subquery);
            let inner_plan =
                convert_operator_inner(*subquery, ctx, tables, session, &free, column_qualifiers)?;
            Ok(DFExpr::InSubquery(
                datafusion::logical_expr::expr::InSubquery {
                    expr: Box::new(inner_expr),
                    subquery: datafusion::logical_expr::Subquery {
                        subquery: Arc::new(inner_plan),
                        outer_ref_columns: free_to_df_cols(ctx, &free),
                        spans: Default::default(),
                    },
                    negated: *negated,
                },
            ))
        }
        ExprData::ScalarSubquery { subquery } => {
            let free = free_columns_for(ctx, *subquery);
            let inner =
                convert_operator_inner(*subquery, ctx, tables, session, &free, column_qualifiers)?;
            Ok(DFExpr::ScalarSubquery(datafusion::logical_expr::Subquery {
                subquery: Arc::new(inner),
                outer_ref_columns: free_to_df_cols(ctx, &free),
                spans: Default::default(),
            }))
        }
    }
}

/// Returns the free (correlated outer reference) columns for a subquery operator.
/// Strips the qualifier from a `Column` expression if the column name is
/// unambiguous in `schema` (only one field with that name). This avoids
/// the "qualified AND unqualified field" ambiguity error from DataFusion's
/// join schema normalization.
fn normalize_col_ref(expr: DFExpr, schema: &datafusion::common::DFSchema) -> DFExpr {
    if let DFExpr::Column(ref col) = expr
        && col.relation.is_some()
    {
        // Count distinct qualified fields with this name (ignores unqualified aliases).
        let count = schema
            .qualified_fields_with_unqualified_name(&col.name)
            .len();
        if count <= 1 {
            return DFExpr::Column(DFColumn::new_unqualified(&col.name));
        }
    }
    expr
}

/// Recursively normalizes column refs in an expression tree.
fn normalize_expr_cols(expr: DFExpr, schema: &datafusion::common::DFSchema) -> DFExpr {
    use datafusion::logical_expr::expr::AggregateFunction as DFAggFn;
    match expr {
        DFExpr::Column(_) => normalize_col_ref(expr, schema),
        DFExpr::AggregateFunction(DFAggFn { func, mut params }) => {
            params.args = params
                .args
                .into_iter()
                .map(|a| normalize_expr_cols(a, schema))
                .collect();
            DFExpr::AggregateFunction(DFAggFn { func, params })
        }
        other => other,
    }
}

fn free_columns_for(
    ctx: &QueryContext,
    subquery: optd::Operator,
) -> std::collections::HashSet<optd::Column> {
    let mut analyses = optd::AnalysisContext::new();
    let free: std::collections::HashSet<_> = analyses
        .get::<optd::FreeColumns>(ctx, subquery)
        .unwrap_or_default()
        .into_iter()
        .collect();
    free
}

/// Converts free columns to DataFusion column expressions for `outer_ref_columns`.
fn free_to_df_cols(
    ctx: &QueryContext,
    free: &std::collections::HashSet<optd::Column>,
) -> Vec<DFExpr> {
    free.iter()
        .map(|col| {
            let cd = ctx.column(*col);
            match &cd.qualifier {
                Some(q) => DFExpr::Column(DFColumn::new(Some(q.as_str()), &cd.name)),
                None => DFExpr::Column(DFColumn::new_unqualified(&cd.name)),
            }
        })
        .collect()
}

fn correlated_semi_anti_outer_refs(
    ctx: &QueryContext,
    join: &optd::Join,
) -> std::collections::HashSet<optd::Column> {
    let mut outer_refs = free_columns_for(ctx, join.inner);
    let mut analyses = optd::AnalysisContext::new();
    let inner_cols: std::collections::HashSet<_> = analyses
        .get::<optd::AvailableColumns>(ctx, join.inner)
        .unwrap_or_default()
        .into_iter()
        .collect();
    if let Ok(used) = optd::expr_used_columns(ctx, join.on) {
        outer_refs.extend(used.into_iter().filter(|col| !inner_cols.contains(col)));
    }
    outer_refs
}

fn project_mark_column(plan: LogicalPlan, marker_name: &str) -> ToDFResult<LogicalPlan> {
    let schema = plan.schema().clone();
    let last_idx = schema.fields().len().saturating_sub(1);
    let exprs = (0..schema.fields().len())
        .map(|idx| {
            let (qualifier, field) = schema.qualified_field(idx);
            let expr = match qualifier {
                Some(q) => DFExpr::Column(DFColumn::new(Some(q.table()), field.name())),
                None => DFExpr::Column(DFColumn::new_unqualified(field.name())),
            };
            if idx == last_idx {
                expr.alias(marker_name)
            } else {
                expr
            }
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(plan).project(exprs)?.build()?)
}

fn classify_mark_selection(
    predicate: optd::Expr,
    marker: optd::Column,
    ctx: &QueryContext,
) -> Option<(optd::JoinType, Vec<optd::Expr>)> {
    let mut join_type = None;
    let mut remaining = Vec::new();
    for conjunct in split_conjuncts(predicate, ctx) {
        match classify_marker(conjunct, marker, ctx) {
            Some(classified) if join_type.is_none() => join_type = Some(classified),
            _ => remaining.push(conjunct),
        }
    }
    join_type.map(|join_type| (join_type, remaining))
}

fn classify_marker(
    expr: optd::Expr,
    marker: optd::Column,
    ctx: &QueryContext,
) -> Option<optd::JoinType> {
    match ctx.expr(expr) {
        ExprData::ColumnRef(col) if *col == marker => Some(optd::JoinType::LeftSemi),
        ExprData::Unary {
            op: UnaryOp::Not,
            expr,
        } => match ctx.expr(*expr) {
            ExprData::ColumnRef(col) if *col == marker => Some(optd::JoinType::LeftAnti),
            _ => None,
        },
        _ => None,
    }
}

fn split_conjuncts(expr: optd::Expr, ctx: &QueryContext) -> Vec<optd::Expr> {
    match ctx.expr(expr) {
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

fn is_true_expr(expr: optd::Expr, ctx: &QueryContext) -> bool {
    matches!(
        ctx.expr(expr),
        ExprData::Literal(ScalarValue::Boolean(true))
    )
}

fn available_qualifiers(op: Operator, ctx: &QueryContext) -> std::collections::HashSet<String> {
    let mut analyses = optd::AnalysisContext::new();
    analyses
        .get::<optd::AvailableColumns>(ctx, op)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|column| ctx.column(column).qualifier.clone())
        .collect()
}

fn lateral_column_qualifiers(
    op: Operator,
    ctx: &QueryContext,
    conflicts: &std::collections::HashSet<String>,
) -> ColumnQualifiers {
    let mut qualifiers = ColumnQualifiers::new();
    let mut stack = vec![op];
    let mut visited = std::collections::HashSet::new();
    while let Some(op) = stack.pop() {
        if !visited.insert(op) {
            continue;
        }
        if let OperatorData::Scan(scan) = op.get(ctx) {
            if conflicts.contains(scan.table.table()) {
                let alias = format!("optd_lateral_{}", op).replace('@', "");
                for column in &scan.columns {
                    qualifiers.insert(*column, alias.clone());
                }
            }
        }
        stack.extend(op.get(ctx).inputs());
        stack.extend(collect_subquery_operators(op, ctx));
    }
    qualifiers
}

fn convert_agg_expr(
    agg: &AggregateExpr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
    column_qualifiers: &ColumnQualifiers,
) -> ToDFResult<DFExpr> {
    match agg {
        AggregateExpr::CountStar => Ok(count_all()),
        AggregateExpr::Func { func, arg, .. } => {
            let arg_expr = convert_expr(*arg, ctx, tables, session, outer_refs, column_qualifiers)?;
            Ok(match func {
                AggregateFunction::Count => count(arg_expr),
                AggregateFunction::Sum => sum(arg_expr),
                AggregateFunction::Avg => avg(arg_expr),
                AggregateFunction::Min => min(arg_expr),
                AggregateFunction::Max => max(arg_expr),
                AggregateFunction::Extension(name) => {
                    return Err(ToDFError::Unsupported(format!(
                        "extension aggregate {name}"
                    )));
                }
            })
        }
    }
}

fn convert_scalar(scalar: &ScalarValue) -> ToDFResult<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue as DFSv;
    Ok(match scalar {
        ScalarValue::Null(_) => DFSv::Null,
        ScalarValue::Boolean(v) => DFSv::Boolean(Some(*v)),
        ScalarValue::Int32(v) => DFSv::Int32(Some(*v)),
        ScalarValue::Int64(v) => DFSv::Int64(Some(*v)),
        ScalarValue::Float64(v) => DFSv::Float64(Some(*v)),
        ScalarValue::Utf8(v) => DFSv::Utf8(Some(v.clone())),
        ScalarValue::Date32(v) => DFSv::Date32(Some(*v)),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => DFSv::Decimal128(Some(*value), *precision, *scale),
        ScalarValue::IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        } => DFSv::IntervalMonthDayNano(Some(datafusion::arrow::datatypes::IntervalMonthDayNano {
            months: *months,
            days: *days,
            nanoseconds: *nanoseconds,
        })),
        ScalarValue::IntervalDayTime { days, milliseconds } => {
            DFSv::IntervalDayTime(Some(datafusion::arrow::datatypes::IntervalDayTime {
                days: *days,
                milliseconds: *milliseconds,
            }))
        }
    })
}

fn convert_join_type(jt: &optd::JoinType) -> ToDFResult<DFJoinType> {
    match jt {
        optd::JoinType::Inner => Ok(DFJoinType::Inner),
        optd::JoinType::LeftOuter => Ok(DFJoinType::Left),
        optd::JoinType::RightOuter => Ok(DFJoinType::Right),
        optd::JoinType::FullOuter => Ok(DFJoinType::Full),
        optd::JoinType::LeftSemi => Ok(DFJoinType::LeftSemi),
        optd::JoinType::LeftAnti => Ok(DFJoinType::LeftAnti),
        optd::JoinType::LeftMark(_) => Ok(DFJoinType::LeftMark),
        optd::JoinType::Single => Ok(DFJoinType::Left),
    }
}

#[cfg(test)]
mod tests {
    use super::{ToDFError, to_logical_plan};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use optd::{ColumnData, ConstScan, ExprData, OperatorData, QueryContext, ScalarValue};

    #[tokio::test]
    async fn exports_zero_row_const_scan_as_empty_relation() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![],
            rows: vec![],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let plan = to_logical_plan(&ctx, &session).await.unwrap();
        match plan {
            LogicalPlan::EmptyRelation(empty) => assert!(!empty.produce_one_row),
            other => panic!("expected EmptyRelation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exports_one_empty_row_const_scan_as_single_row_empty_relation() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![],
            rows: vec![vec![]],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let plan = to_logical_plan(&ctx, &session).await.unwrap();
        match plan {
            LogicalPlan::EmptyRelation(empty) => assert!(empty.produce_one_row),
            other => panic!("expected EmptyRelation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exports_const_scan_with_non_empty_row_values_as_values_plan() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let value = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![a],
            rows: vec![vec![value]],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let plan = to_logical_plan(&ctx, &session).await.unwrap();
        match plan {
            LogicalPlan::Values(values) => {
                assert_eq!(values.values.len(), 1);
                assert_eq!(values.values[0].len(), 1);
            }
            other => panic!("expected Values plan, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_const_scan_values_with_mismatched_row_width() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let value = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![a],
            rows: vec![vec![value], vec![]],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let error = to_logical_plan(&ctx, &session).await.unwrap_err();
        match error {
            ToDFError::Unsupported(message) => {
                assert!(message.contains("row width must match number of columns"))
            }
            other => panic!("expected Unsupported error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exports_const_scan_schema_into_empty_relation_schema() {
        let session = SessionContext::new();
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let root = OperatorData::ConstScan(ConstScan {
            columns: vec![a],
            rows: vec![],
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let plan = to_logical_plan(&ctx, &session).await.unwrap();
        match plan {
            LogicalPlan::EmptyRelation(empty) => {
                assert_eq!(empty.schema.fields().len(), 1);
                let (_, field) = empty.schema.qualified_field(0);
                assert_eq!(field.name(), "a");
            }
            other => panic!("expected EmptyRelation, got {other:?}"),
        }
    }
}
