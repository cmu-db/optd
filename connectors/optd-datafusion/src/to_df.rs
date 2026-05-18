//! Converts optd IR into DataFusion [`LogicalPlan`] for execution.
//!
//! Table providers are collected asynchronously once in [`to_logical_plan`];
//! all subsequent conversion is synchronous.

use std::collections::HashMap;
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
                .table_provider(TableReference::bare(scan.table.table()))
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
    convert_operator_inner(op, ctx, tables, session, &std::collections::HashSet::new())
}

fn convert_operator_inner(
    op: Operator,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
) -> ToDFResult<LogicalPlan> {
    match op.get(ctx) {
        OperatorData::Scan(scan) => {
            let source = tables
                .get(&scan.table)
                .ok_or_else(|| ToDFError::TableNotFound(scan.table.clone()))?
                .clone();
            Ok(
                LogicalPlanBuilder::scan(TableReference::bare(scan.table.table()), source, None)?
                    .build()?,
            )
        }

        OperatorData::Selection(sel) => {
            let input = convert_operator_inner(sel.input, ctx, tables, session, outer_refs)?;
            let predicate = convert_expr(sel.predicate, ctx, tables, session, outer_refs)?;
            Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?)
        }

        OperatorData::Projection(proj) => {
            let input = convert_operator_inner(proj.input, ctx, tables, session, outer_refs)?;
            let input_schema = input.schema().clone();
            let exprs: Vec<DFExpr> = proj
                .columns
                .iter()
                .map(|col| {
                    let cd = ctx.column(*col);
                    // Use qualified ref only if the name is genuinely ambiguous
                    // (multiple distinct qualified fields with that name).
                    let count = input_schema
                        .qualified_fields_with_unqualified_name(&cd.name)
                        .len();
                    if count > 1 {
                        match &cd.qualifier {
                            Some(q) => DFExpr::Column(DFColumn::new(Some(q.as_str()), &cd.name)),
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
            let input = convert_operator_inner(map.input, ctx, tables, session, outer_refs)?;
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
                exprs.push(convert_expr(*expr, ctx, tables, session, outer_refs)?.alias(name));
            }
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Aggregation(agg) => {
            let input = convert_operator_inner(agg.input, ctx, tables, session, outer_refs)?;
            let input_schema = input.schema().clone();
            let group_exprs: Vec<DFExpr> = agg
                .keys
                .iter()
                .map(|e| {
                    let expr = convert_expr(*e, ctx, tables, session, outer_refs)?;
                    Ok(normalize_col_ref(expr, &input_schema))
                })
                .collect::<ToDFResult<_>>()?;
            let aggr_exprs: Vec<DFExpr> = agg
                .aggregates
                .iter()
                .map(|(col, agg_expr)| {
                    let name = ctx.column(*col).name.clone();
                    let expr = convert_agg_expr(agg_expr, ctx, tables, session, outer_refs)?;
                    // Normalize column refs inside aggregate args.
                    Ok(normalize_expr_cols(expr, &input_schema).alias(name))
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input)
                .aggregate(group_exprs, aggr_exprs)?
                .build()?)
        }

        OperatorData::Sort(sort) => {
            let input = convert_operator_inner(sort.input, ctx, tables, session, outer_refs)?;
            let sort_exprs: Vec<SortExpr> = sort
                .keys
                .iter()
                .map(|key| {
                    Ok(SortExpr {
                        expr: convert_expr(key.expr, ctx, tables, session, outer_refs)?,
                        asc: matches!(key.direction, optd::SortDirection::Asc),
                        nulls_first: matches!(key.nulls, optd::NullOrdering::First),
                    })
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input).sort(sort_exprs)?.build()?)
        }

        OperatorData::Limit(limit) => {
            let input = convert_operator_inner(limit.input, ctx, tables, session, outer_refs)?;
            Ok(LogicalPlanBuilder::from(input)
                .limit(limit.offset, limit.fetch)?
                .build()?)
        }

        OperatorData::Join(join) => {
            let outer = convert_operator_inner(join.outer, ctx, tables, session, outer_refs)?;
            let inner = convert_operator_inner(join.inner, ctx, tables, session, outer_refs)?;
            let join_type = convert_join_type(&join.join_type)?;
            let condition = convert_expr(join.on, ctx, tables, session, outer_refs)?;
            Ok(LogicalPlanBuilder::from(outer)
                .join_on(inner, join_type, Some(condition))?
                .build()?)
        }

        OperatorData::CrossProduct(cross) => {
            let outer = convert_operator_inner(cross.outer, ctx, tables, session, outer_refs)?;
            let inner = convert_operator_inner(cross.inner, ctx, tables, session, outer_refs)?;
            Ok(LogicalPlanBuilder::from(outer).cross_join(inner)?.build()?)
        }

        OperatorData::Output(out) => {
            convert_operator_inner(out.input, ctx, tables, session, outer_refs)
        }

        OperatorData::Rename(r) => {
            let input = convert_operator_inner(r.input, ctx, tables, session, outer_refs)?;
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
                                .map(|expr| convert_expr(*expr, ctx, tables, session, outer_refs))
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

fn convert_expr(
    expr: optd::Expr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
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
                Ok(match &cd.qualifier {
                    Some(q) => DFExpr::Column(DFColumn::new(Some(q.as_str()), &cd.name)),
                    None => DFExpr::Column(DFColumn::new_unqualified(&cd.name)),
                })
            }
        }
        ExprData::Literal(scalar) => Ok(DFExpr::Literal(convert_scalar(scalar)?, None)),
        ExprData::Unary { op, expr } => {
            let inner = convert_expr(*expr, ctx, tables, session, outer_refs)?;
            Ok(match op {
                UnaryOp::Not => datafusion::logical_expr::not(inner),
                UnaryOp::IsNull => inner.is_null(),
                UnaryOp::IsNotNull => inner.is_not_null(),
                UnaryOp::Negate => DFExpr::Negative(Box::new(inner)),
            })
        }
        ExprData::Binary { op, left, right } => {
            let l = convert_expr(*left, ctx, tables, session, outer_refs)?;
            let r = convert_expr(*right, ctx, tables, session, outer_refs)?;
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
                .map(|e| convert_expr(*e, ctx, tables, session, outer_refs));
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
            expr: Box::new(convert_expr(*expr, ctx, tables, session, outer_refs)?),
            data_type: ty.clone(),
        })),
        ExprData::ScalarFunction { function, args } => {
            use optd::ScalarFunction;
            let df_args: Vec<DFExpr> = args
                .iter()
                .map(|a| convert_expr(*a, ctx, tables, session, outer_refs))
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
            expr: Box::new(convert_expr(*expr, ctx, tables, session, outer_refs)?),
            pattern: Box::new(convert_expr(*pattern, ctx, tables, session, outer_refs)?),
            escape_char: None,
            case_insensitive: *case_insensitive,
        })),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            let mut builder = datafusion::logical_expr::when(
                convert_expr(when_then[0].0, ctx, tables, session, outer_refs)?,
                convert_expr(when_then[0].1, ctx, tables, session, outer_refs)?,
            );
            for (w, t) in &when_then[1..] {
                builder = builder.when(
                    convert_expr(*w, ctx, tables, session, outer_refs)?,
                    convert_expr(*t, ctx, tables, session, outer_refs)?,
                );
            }
            Ok(if let Some(else_e) = else_expr {
                builder.otherwise(convert_expr(*else_e, ctx, tables, session, outer_refs)?)?
            } else {
                builder.end()?
            })
        }
        ExprData::Exists { subquery, negated } => {
            let free = free_columns_for(ctx, *subquery);
            let inner = convert_operator_inner(*subquery, ctx, tables, session, &free)?;
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
            let inner_expr = convert_expr(*expr, ctx, tables, session, outer_refs)?;
            let free = free_columns_for(ctx, *subquery);
            let inner_plan = convert_operator_inner(*subquery, ctx, tables, session, &free)?;
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
            let inner = convert_operator_inner(*subquery, ctx, tables, session, &free)?;
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

fn convert_agg_expr(
    agg: &AggregateExpr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
    outer_refs: &std::collections::HashSet<optd::Column>,
) -> ToDFResult<DFExpr> {
    match agg {
        AggregateExpr::CountStar => Ok(count_all()),
        AggregateExpr::Func { func, arg, .. } => {
            let arg_expr = convert_expr(*arg, ctx, tables, session, outer_refs)?;
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
        other => Err(ToDFError::Unsupported(format!("join type {other:?}"))),
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
