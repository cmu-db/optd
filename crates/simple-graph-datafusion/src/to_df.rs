//! Converts simple-graph IR into DataFusion [`LogicalPlan`] for execution.
//!
//! Table providers are collected asynchronously once in [`to_logical_plan`];
//! all subsequent conversion is synchronous.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Column as DFColumn, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::count::count_all;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{
    Expr as DFExpr, JoinType as DFJoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, TableSource,
};
use datafusion::prelude::SessionContext;
use simple_graph::{
    AggregateExpr, AggregateFunction, BinaryOp, ExprData, NaryOp, Operator, OperatorData,
    QueryContext, Relation, ScalarValue, TableRef, UnaryOp,
};

/// Error type for the simple-graph → DataFusion converter.
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

/// Converts a simple-graph `QueryContext` root into a DataFusion `LogicalPlan`.
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
        if let OperatorData::Scan(scan) = op.get(ctx) {
            if !map.contains_key(&scan.table) {
                let provider = session
                    .table_provider(TableReference::bare(scan.table.table()))
                    .await
                    .map_err(|_| ToDFError::TableNotFound(scan.table.clone()))?;
                map.insert(scan.table.clone(), provider_as_source(provider));
            }
        }
        stack.extend(op.get(ctx).inputs());
    }
    Ok(map)
}

fn convert_operator(
    op: Operator,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
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
            let input = convert_operator(sel.input, ctx, tables, session)?;
            let predicate = convert_expr(sel.predicate, ctx, tables, session)?;
            Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?)
        }

        OperatorData::Projection(proj) => {
            let input = convert_operator(proj.input, ctx, tables, session)?;
            let exprs: Vec<DFExpr> = proj
                .columns
                .iter()
                .map(|col| DFExpr::Column(DFColumn::new_unqualified(&ctx.column(*col).name)))
                .collect();
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Map(map) => {
            let input = convert_operator(map.input, ctx, tables, session)?;
            let input_schema = input.schema().clone();
            let mut exprs: Vec<DFExpr> = input_schema
                .fields()
                .iter()
                .map(|f| DFExpr::Column(DFColumn::new_unqualified(f.name())))
                .collect();
            for (col, expr) in &map.computations {
                let name = ctx.column(*col).name.clone();
                exprs.push(convert_expr(*expr, ctx, tables, session)?.alias(name));
            }
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Aggregation(agg) => {
            let input = convert_operator(agg.input, ctx, tables, session)?;
            let group_exprs: Vec<DFExpr> = agg
                .keys
                .iter()
                .map(|e| convert_expr(*e, ctx, tables, session))
                .collect::<ToDFResult<_>>()?;
            let aggr_exprs: Vec<DFExpr> = agg
                .aggregates
                .iter()
                .map(|(col, agg_expr)| {
                    let name = ctx.column(*col).name.clone();
                    Ok(convert_agg_expr(agg_expr, ctx, tables, session)?.alias(name))
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input)
                .aggregate(group_exprs, aggr_exprs)?
                .build()?)
        }

        OperatorData::Sort(sort) => {
            let input = convert_operator(sort.input, ctx, tables, session)?;
            let sort_exprs: Vec<SortExpr> = sort
                .keys
                .iter()
                .map(|key| {
                    Ok(SortExpr {
                        expr: convert_expr(key.expr, ctx, tables, session)?,
                        asc: matches!(key.direction, simple_graph::SortDirection::Asc),
                        nulls_first: matches!(key.nulls, simple_graph::NullOrdering::First),
                    })
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input).sort(sort_exprs)?.build()?)
        }

        OperatorData::Limit(limit) => {
            let input = convert_operator(limit.input, ctx, tables, session)?;
            Ok(LogicalPlanBuilder::from(input)
                .limit(limit.offset, limit.fetch)?
                .build()?)
        }

        OperatorData::Join(join) => {
            let outer = convert_operator(join.outer, ctx, tables, session)?;
            let inner = convert_operator(join.inner, ctx, tables, session)?;
            let join_type = convert_join_type(&join.join_type)?;
            let condition = convert_expr(join.on, ctx, tables, session)?;
            Ok(LogicalPlanBuilder::from(outer)
                .join_on(inner, join_type, Some(condition))?
                .build()?)
        }

        OperatorData::CrossProduct(cross) => {
            let outer = convert_operator(cross.outer, ctx, tables, session)?;
            let inner = convert_operator(cross.inner, ctx, tables, session)?;
            Ok(LogicalPlanBuilder::from(outer).cross_join(inner)?.build()?)
        }

        OperatorData::Output(out) => convert_operator(out.input, ctx, tables, session),

        OperatorData::TableFunction(_) => Err(ToDFError::Unsupported("TableFunction".into())),
    }
}

fn convert_expr(
    expr: simple_graph::Expr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
) -> ToDFResult<DFExpr> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) => Ok(DFExpr::Column(DFColumn::new_unqualified(
            &ctx.column(*col).name,
        ))),
        ExprData::Literal(scalar) => Ok(DFExpr::Literal(convert_scalar(scalar)?, None)),
        ExprData::Unary { op, expr } => {
            let inner = convert_expr(*expr, ctx, tables, session)?;
            Ok(match op {
                UnaryOp::Not => datafusion::logical_expr::not(inner),
                UnaryOp::IsNull => inner.is_null(),
                UnaryOp::IsNotNull => inner.is_not_null(),
                UnaryOp::Negate => DFExpr::Negative(Box::new(inner)),
            })
        }
        ExprData::Binary { op, left, right } => {
            let l = convert_expr(*left, ctx, tables, session)?;
            let r = convert_expr(*right, ctx, tables, session)?;
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
            let mut iter = exprs.iter().map(|e| convert_expr(*e, ctx, tables, session));
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
            expr: Box::new(convert_expr(*expr, ctx, tables, session)?),
            data_type: ty.clone(),
        })),
        ExprData::ScalarFunction { function, args } => {
            use simple_graph::ScalarFunction;
            let df_args: Vec<DFExpr> = args
                .iter()
                .map(|a| convert_expr(*a, ctx, tables, session))
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
            expr: Box::new(convert_expr(*expr, ctx, tables, session)?),
            pattern: Box::new(convert_expr(*pattern, ctx, tables, session)?),
            escape_char: None,
            case_insensitive: *case_insensitive,
        })),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            let mut builder = datafusion::logical_expr::when(
                convert_expr(when_then[0].0, ctx, tables, session)?,
                convert_expr(when_then[0].1, ctx, tables, session)?,
            );
            for (w, t) in &when_then[1..] {
                builder = builder.when(
                    convert_expr(*w, ctx, tables, session)?,
                    convert_expr(*t, ctx, tables, session)?,
                );
            }
            Ok(if let Some(else_e) = else_expr {
                builder.otherwise(convert_expr(*else_e, ctx, tables, session)?)?
            } else {
                builder.end()?
            })
        }
        ExprData::Exists { subquery, negated } => {
            let inner = convert_operator(*subquery, ctx, tables, session)?;
            Ok(DFExpr::Exists(datafusion::logical_expr::expr::Exists {
                subquery: datafusion::logical_expr::Subquery {
                    subquery: Arc::new(inner),
                    outer_ref_columns: vec![],
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
            let inner_expr = convert_expr(*expr, ctx, tables, session)?;
            let inner_plan = convert_operator(*subquery, ctx, tables, session)?;
            Ok(DFExpr::InSubquery(
                datafusion::logical_expr::expr::InSubquery {
                    expr: Box::new(inner_expr),
                    subquery: datafusion::logical_expr::Subquery {
                        subquery: Arc::new(inner_plan),
                        outer_ref_columns: vec![],
                        spans: Default::default(),
                    },
                    negated: *negated,
                },
            ))
        }
        ExprData::ScalarSubquery { subquery } => {
            let inner = convert_operator(*subquery, ctx, tables, session)?;
            Ok(DFExpr::ScalarSubquery(datafusion::logical_expr::Subquery {
                subquery: Arc::new(inner),
                outer_ref_columns: vec![],
                spans: Default::default(),
            }))
        }
        other => Err(ToDFError::Unsupported(format!("expr {other:?}"))),
    }
}

fn convert_agg_expr(
    agg: &AggregateExpr,
    ctx: &QueryContext,
    tables: &TableMap,
    session: &SessionContext,
) -> ToDFResult<DFExpr> {
    match agg {
        AggregateExpr::CountStar => Ok(count_all()),
        AggregateExpr::Func { func, arg, .. } => {
            let arg_expr = convert_expr(*arg, ctx, tables, session)?;
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

fn convert_join_type(jt: &simple_graph::JoinType) -> ToDFResult<DFJoinType> {
    match jt {
        simple_graph::JoinType::Inner => Ok(DFJoinType::Inner),
        simple_graph::JoinType::LeftOuter => Ok(DFJoinType::Left),
        simple_graph::JoinType::RightOuter => Ok(DFJoinType::Right),
        simple_graph::JoinType::FullOuter => Ok(DFJoinType::Full),
        simple_graph::JoinType::LeftSemi => Ok(DFJoinType::LeftSemi),
        simple_graph::JoinType::LeftAnti => Ok(DFJoinType::LeftAnti),
        other => Err(ToDFError::Unsupported(format!("join type {other:?}"))),
    }
}
