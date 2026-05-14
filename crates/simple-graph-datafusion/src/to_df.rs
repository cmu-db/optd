//! Converts simple-graph IR into DataFusion [`LogicalPlan`] for execution.

use datafusion::common::{Column as DFColumn, TableReference};
use datafusion::datasource::provider_as_source;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{
    Expr as DFExpr, JoinType as DFJoinType, LogicalPlan, LogicalPlanBuilder, SortExpr,
};
use datafusion::prelude::SessionContext;
use simple_graph::{
    AggregateExpr, AggregateFunction, BinaryOp, ExprData, NaryOp, Operator, OperatorData,
    QueryContext, ScalarValue, UnaryOp,
};

/// Error type for the simple-graph → DataFusion converter.
#[derive(Debug)]
pub enum ToDFError {
    Unsupported(String),
    Build(datafusion::error::DataFusionError),
    TableNotFound(String),
}

impl std::fmt::Display for ToDFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "unsupported IR node: {msg}"),
            Self::Build(e) => write!(f, "plan build error: {e}"),
            Self::TableNotFound(name) => write!(f, "table not found: {name}"),
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

/// Converts a simple-graph `QueryContext` root into a DataFusion `LogicalPlan`.
///
/// `session` is used to resolve table names to registered `TableProvider`s.
/// This is async because `SessionContext::table_provider` is async.
pub async fn to_logical_plan(
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToDFResult<LogicalPlan> {
    let root = ctx
        .root()
        .ok_or_else(|| ToDFError::Unsupported("query has no root".into()))?;
    convert_operator(root, ctx, session).await
}

async fn convert_operator(
    op: Operator,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToDFResult<LogicalPlan> {
    // Use Box::pin for recursive async — avoids infinite-size future.
    Box::pin(convert_operator_inner(op, ctx, session)).await
}

async fn convert_operator_inner(
    op: Operator,
    ctx: &QueryContext,
    session: &SessionContext,
) -> ToDFResult<LogicalPlan> {
    match op.get(ctx) {
        OperatorData::Scan(scan) => {
            let table_name = scan.table.table().to_string();
            let provider = session
                .table_provider(TableReference::bare(table_name.clone()))
                .await
                .map_err(|_| ToDFError::TableNotFound(table_name.clone()))?;
            let schema = provider.schema();
            let projection: Option<Vec<usize>> = {
                let indices: Vec<usize> = scan
                    .columns
                    .iter()
                    .filter_map(|col| schema.index_of(&ctx.column(*col).name).ok())
                    .collect();
                if indices.len() == schema.fields().len() {
                    None
                } else {
                    Some(indices)
                }
            };
            Ok(LogicalPlanBuilder::scan(
                TableReference::bare(table_name),
                provider_as_source(provider),
                projection,
            )?
            .build()?)
        }

        OperatorData::Selection(sel) => {
            let input = convert_operator(sel.input, ctx, session).await?;
            let predicate = convert_expr(sel.predicate, ctx)?;
            Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?)
        }

        OperatorData::Projection(proj) => {
            let input = convert_operator(proj.input, ctx, session).await?;
            let exprs: Vec<DFExpr> = proj
                .columns
                .iter()
                .map(|col| {
                    let name = &ctx.column(*col).name;
                    DFExpr::Column(DFColumn::new_unqualified(name))
                })
                .collect();
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Map(map) => {
            let input = convert_operator(map.input, ctx, session).await?;
            let input_schema = input.schema().clone();
            let mut exprs: Vec<DFExpr> = input_schema
                .fields()
                .iter()
                .map(|f| DFExpr::Column(DFColumn::new_unqualified(f.name())))
                .collect();
            for (col, expr) in &map.computations {
                let name = ctx.column(*col).name.clone();
                exprs.push(convert_expr(*expr, ctx)?.alias(name));
            }
            Ok(LogicalPlanBuilder::from(input).project(exprs)?.build()?)
        }

        OperatorData::Aggregation(agg) => {
            let input = convert_operator(agg.input, ctx, session).await?;
            let group_exprs: Vec<DFExpr> = agg
                .keys
                .iter()
                .map(|e| convert_expr(*e, ctx))
                .collect::<ToDFResult<_>>()?;
            let aggr_exprs: Vec<DFExpr> = agg
                .aggregates
                .iter()
                .map(|(col, agg_expr)| {
                    let name = ctx.column(*col).name.clone();
                    Ok(convert_agg_expr(agg_expr, ctx)?.alias(name))
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input)
                .aggregate(group_exprs, aggr_exprs)?
                .build()?)
        }

        OperatorData::Sort(sort) => {
            let input = convert_operator(sort.input, ctx, session).await?;
            let sort_exprs: Vec<SortExpr> = sort
                .keys
                .iter()
                .map(|key| {
                    Ok(SortExpr {
                        expr: convert_expr(key.expr, ctx)?,
                        asc: matches!(key.direction, simple_graph::SortDirection::Asc),
                        nulls_first: matches!(key.nulls, simple_graph::NullOrdering::First),
                    })
                })
                .collect::<ToDFResult<_>>()?;
            Ok(LogicalPlanBuilder::from(input).sort(sort_exprs)?.build()?)
        }

        OperatorData::Limit(limit) => {
            let input = convert_operator(limit.input, ctx, session).await?;
            Ok(LogicalPlanBuilder::from(input)
                .limit(limit.offset, limit.fetch)?
                .build()?)
        }

        OperatorData::Join(join) => {
            let outer = convert_operator(join.outer, ctx, session).await?;
            let inner = convert_operator(join.inner, ctx, session).await?;
            let join_type = convert_join_type(&join.join_type)?;
            let condition = convert_expr(join.on, ctx)?;
            Ok(LogicalPlanBuilder::from(outer)
                .join_on(inner, join_type, Some(condition))?
                .build()?)
        }

        OperatorData::CrossProduct(cross) => {
            let outer = convert_operator(cross.outer, ctx, session).await?;
            let inner = convert_operator(cross.inner, ctx, session).await?;
            Ok(LogicalPlanBuilder::from(outer).cross_join(inner)?.build()?)
        }

        OperatorData::Output(out) => convert_operator(out.input, ctx, session).await,

        OperatorData::TableFunction(_) => Err(ToDFError::Unsupported("TableFunction".into())),
    }
}

fn convert_expr(expr: simple_graph::Expr, ctx: &QueryContext) -> ToDFResult<DFExpr> {
    match expr.get(ctx) {
        ExprData::ColumnRef(col) => {
            let name = &ctx.column(*col).name;
            Ok(DFExpr::Column(DFColumn::new_unqualified(name)))
        }
        ExprData::Literal(scalar) => Ok(DFExpr::Literal(convert_scalar(scalar)?, None)),
        ExprData::Unary { op, expr } => {
            let inner = convert_expr(*expr, ctx)?;
            Ok(match op {
                UnaryOp::Not => datafusion::logical_expr::not(inner),
                UnaryOp::IsNull => inner.is_null(),
                UnaryOp::IsNotNull => inner.is_not_null(),
                UnaryOp::Negate => DFExpr::Negative(Box::new(inner)),
            })
        }
        ExprData::Binary { op, left, right } => {
            let l = convert_expr(*left, ctx)?;
            let r = convert_expr(*right, ctx)?;
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
            let mut iter = exprs.iter().map(|e| convert_expr(*e, ctx));
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
        ExprData::Cast { expr, ty } => {
            let inner = convert_expr(*expr, ctx)?;
            Ok(DFExpr::Cast(datafusion::logical_expr::Cast {
                expr: Box::new(inner),
                data_type: ty.clone(),
            }))
        }
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            let mut builder = datafusion::logical_expr::when(
                convert_expr(when_then[0].0, ctx)?,
                convert_expr(when_then[0].1, ctx)?,
            );
            for (w, t) in &when_then[1..] {
                builder = builder.when(convert_expr(*w, ctx)?, convert_expr(*t, ctx)?);
            }
            Ok(if let Some(else_e) = else_expr {
                builder.otherwise(convert_expr(*else_e, ctx)?)?
            } else {
                builder.end()?
            })
        }
        other => Err(ToDFError::Unsupported(format!("expr {other:?}"))),
    }
}

fn convert_agg_expr(agg: &AggregateExpr, ctx: &QueryContext) -> ToDFResult<DFExpr> {
    match agg {
        AggregateExpr::CountStar => Ok(count(DFExpr::Literal(
            datafusion::common::ScalarValue::Int64(Some(1)),
            None,
        ))),
        AggregateExpr::Func { func, arg, .. } => {
            let arg_expr = convert_expr(*arg, ctx)?;
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
