//! Converts DataFusion [`LogicalPlan`] trees into simple-graph IR.
//!
//! Column resolution uses a [`BindingContext`] scope stack. Each relational
//! operator pushes a new scope for the columns it introduces, so qualified
//! references are resolved precisely and ambiguous unqualified references are
//! rejected rather than silently picking the wrong column.

use std::collections::HashMap;

use datafusion::common::ScalarValue as DFScalarValue;
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr as DFExpr, Filter, Join, JoinType as DFJoinType, Limit,
    LogicalPlan, Operator as DFOperator, Projection, Sort, TableScan,
    expr::{AggregateFunction as DFAggregateFunction, Case},
};
use simple_graph::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Column, ColumnData, ExprData,
    Join as SGJoin, JoinType, Limit as SGLimit, Map, NaryOp, NullOrdering, Operator, OperatorData,
    Projection as SGProjection, QueryContext, ScalarValue, Scan, Selection, Sort as SGSort,
    SortDirection, SortKey, TableRef,
};

/// Error type for the DataFusion → simple-graph converter.
#[derive(Debug)]
pub enum FromDFError {
    Unsupported(String),
    Schema(String),
}

impl std::fmt::Display for FromDFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(msg) => write!(f, "unsupported plan node: {msg}"),
            Self::Schema(msg) => write!(f, "schema error: {msg}"),
        }
    }
}

impl std::error::Error for FromDFError {}

pub type FromDFResult<T> = Result<T, FromDFError>;

// ---------------------------------------------------------------------------
// Binding context
// ---------------------------------------------------------------------------

/// One level of column bindings introduced by a single relational operator.
#[derive(Default)]
struct Scope {
    bindings: HashMap<(Option<String>, String), Column>,
}

/// Stack of scopes for resolving column references during plan conversion.
///
/// Each operator that introduces columns pushes a scope. Subquery boundaries
/// push a fresh stack. Resolution walks from innermost to outermost scope.
pub struct BindingContext {
    scopes: Vec<Scope>,
}

impl BindingContext {
    pub fn new() -> Self {
        Self {
            scopes: vec![Scope::default()],
        }
    }

    /// Pushes a new empty scope.
    fn push(&mut self) {
        self.scopes.push(Scope::default());
    }

    /// Pops the innermost scope.
    fn pop(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    /// Binds a column in the current (innermost) scope.
    fn bind(&mut self, qualifier: Option<String>, name: String, col: Column) {
        self.scopes
            .last_mut()
            .unwrap()
            .bindings
            .insert((qualifier, name), col);
    }

    /// Resolves a column reference, walking from innermost to outermost scope.
    ///
    /// - Qualified references (`table.col`) match exactly.
    /// - Unqualified references succeed only if exactly one column with that
    ///   name exists across all visible scopes.
    fn resolve(&self, qualifier: Option<&str>, name: &str) -> FromDFResult<Column> {
        let q = qualifier.map(str::to_string);

        // Exact qualified match — innermost wins.
        for scope in self.scopes.iter().rev() {
            if let Some(c) = scope.bindings.get(&(q.clone(), name.to_string())) {
                return Ok(*c);
            }
        }

        // Unqualified fallback — only if unambiguous.
        let matches: Vec<Column> = self
            .scopes
            .iter()
            .rev()
            .flat_map(|s| s.bindings.iter())
            .filter(|((_, n), _)| n == name)
            .map(|(_, c)| *c)
            .collect();

        match matches.len() {
            1 => Ok(matches[0]),
            0 => Err(FromDFError::Schema(format!(
                "column not found: {}{}",
                qualifier.map(|q| format!("{q}.")).unwrap_or_default(),
                name
            ))),
            _ => Err(FromDFError::Schema(format!(
                "ambiguous column reference '{name}' matches {} columns",
                matches.len()
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Converts a DataFusion `LogicalPlan` into simple-graph IR.
pub fn from_logical_plan(plan: &LogicalPlan, ctx: &mut QueryContext) -> FromDFResult<Operator> {
    let mut bindings = BindingContext::new();
    convert_plan(plan, ctx, &mut bindings)
}

// ---------------------------------------------------------------------------
// Plan conversion
// ---------------------------------------------------------------------------

fn convert_plan(
    plan: &LogicalPlan,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    match plan {
        LogicalPlan::TableScan(scan) => convert_scan(scan, ctx, bindings),
        LogicalPlan::Filter(filter) => convert_filter(filter, ctx, bindings),
        LogicalPlan::Projection(proj) => convert_projection(proj, ctx, bindings),
        LogicalPlan::Aggregate(agg) => convert_aggregate(agg, ctx, bindings),
        LogicalPlan::Sort(sort) => convert_sort(sort, ctx, bindings),
        LogicalPlan::Limit(limit) => convert_limit(limit, ctx, bindings),
        LogicalPlan::Join(join) => convert_join(join, ctx, bindings),
        LogicalPlan::SubqueryAlias(alias) => {
            // Transparent — pass through, but re-qualify columns with the alias.
            convert_plan(&alias.input, ctx, bindings)
        }
        other => Err(FromDFError::Unsupported(format!(
            "{}",
            other.display_indent()
        ))),
    }
}

fn convert_scan(
    scan: &TableScan,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let table = TableRef::bare(scan.table_name.table());
    let schema = scan.projected_schema.as_ref();

    bindings.push();
    let columns: Vec<Column> = (0..schema.fields().len())
        .map(|i| {
            let (qualifier, field) = schema.qualified_field(i);
            let col = ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx);
            let q = qualifier.map(|r| r.to_string());
            bindings.bind(q, field.name().clone(), col);
            col
        })
        .collect();

    let scan_op = OperatorData::Scan(Scan { table, columns }).add(ctx);

    if scan.filters.is_empty() {
        Ok(scan_op)
    } else {
        let predicate = combine_predicates(&scan.filters, ctx, bindings)?;
        Ok(OperatorData::Selection(Selection {
            predicate,
            input: scan_op,
        })
        .add(ctx))
    }
}

fn convert_filter(
    filter: &Filter,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let input = convert_plan(&filter.input, ctx, bindings)?;
    let predicate = convert_expr(&filter.predicate, ctx, bindings)?;
    Ok(OperatorData::Selection(Selection { predicate, input }).add(ctx))
}

fn convert_projection(
    proj: &Projection,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let input = convert_plan(&proj.input, ctx, bindings)?;

    let all_columns = proj.expr.iter().all(|e| matches!(e, DFExpr::Column(_)));

    if all_columns {
        let columns: Vec<Column> = proj
            .expr
            .iter()
            .map(|e| {
                let DFExpr::Column(col) = e else {
                    unreachable!()
                };
                bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)
            })
            .collect::<FromDFResult<_>>()?;
        Ok(OperatorData::Projection(SGProjection { columns, input }).add(ctx))
    } else {
        let mut computations = Vec::new();
        let mut output_cols = Vec::new();

        bindings.push();
        for (i, expr) in proj.expr.iter().enumerate() {
            let (qualifier, field) = proj.schema.qualified_field(i);
            let q = qualifier.map(|r| r.to_string());

            if let DFExpr::Column(col) = expr {
                let sg_col =
                    bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)?;
                output_cols.push(sg_col);
            } else {
                let sg_expr = convert_expr(expr, ctx, bindings)?;
                let col = ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx);
                bindings.bind(q, field.name().clone(), col);
                computations.push((col, sg_expr));
                output_cols.push(col);
            }
        }

        let after_map = if computations.is_empty() {
            input
        } else {
            OperatorData::Map(Map {
                computations,
                input,
            })
            .add(ctx)
        };

        Ok(OperatorData::Projection(SGProjection {
            columns: output_cols,
            input: after_map,
        })
        .add(ctx))
    }
}

fn convert_aggregate(
    agg: &Aggregate,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let input = convert_plan(&agg.input, ctx, bindings)?;

    let keys: Vec<_> = agg
        .group_expr
        .iter()
        .map(|e| convert_expr(e, ctx, bindings))
        .collect::<FromDFResult<_>>()?;

    // Aggregate output columns live in a new scope.
    bindings.push();
    let mut aggregates = Vec::new();
    let agg_start = agg.group_expr.len();
    for (i, expr) in agg.aggr_expr.iter().enumerate() {
        let (qualifier, field) = agg.schema.qualified_field(agg_start + i);
        let q = qualifier.map(|r| r.to_string());
        let col = ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx);
        bindings.bind(q, field.name().clone(), col);
        let agg_expr = convert_agg_expr(expr, ctx, bindings)?;
        aggregates.push((col, agg_expr));
    }

    Ok(OperatorData::Aggregation(Aggregation {
        keys,
        aggregates,
        input,
    })
    .add(ctx))
}

fn convert_sort(
    sort: &Sort,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let input = convert_plan(&sort.input, ctx, bindings)?;
    let keys: Vec<SortKey> = sort
        .expr
        .iter()
        .map(|s| {
            Ok(SortKey {
                expr: convert_expr(&s.expr, ctx, bindings)?,
                direction: if s.asc {
                    SortDirection::Asc
                } else {
                    SortDirection::Desc
                },
                nulls: if s.nulls_first {
                    NullOrdering::First
                } else {
                    NullOrdering::Last
                },
            })
        })
        .collect::<FromDFResult<_>>()?;

    let sort_op = OperatorData::Sort(SGSort { keys, input }).add(ctx);

    // DataFusion may push a LIMIT into Sort.fetch.
    if let Some(fetch) = sort.fetch {
        return Ok(OperatorData::Limit(SGLimit {
            fetch: Some(fetch),
            offset: 0,
            input: sort_op,
        })
        .add(ctx));
    }
    Ok(sort_op)
}

fn convert_limit(
    limit: &Limit,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let input = convert_plan(&limit.input, ctx, bindings)?;
    let fetch = limit.fetch.as_ref().and_then(|e| eval_usize_expr(e));
    let offset = limit
        .skip
        .as_ref()
        .and_then(|e| eval_usize_expr(e))
        .unwrap_or(0);
    Ok(OperatorData::Limit(SGLimit {
        fetch,
        offset,
        input,
    })
    .add(ctx))
}

fn convert_join(
    join: &Join,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    // Outer side first — its columns are visible to the inner side (correlated joins).
    let outer = convert_plan(&join.left, ctx, bindings)?;
    let inner = convert_plan(&join.right, ctx, bindings)?;
    let join_type = convert_join_type(&join.join_type)?;

    // For semi/anti joins the inner side's columns are not exposed after the join.
    // Pop the inner scope so they don't leak into subsequent operators.
    let inner_scope_visible = !matches!(
        join.join_type,
        DFJoinType::LeftSemi | DFJoinType::LeftAnti | DFJoinType::RightSemi | DFJoinType::RightAnti
    );
    if !inner_scope_visible {
        bindings.pop();
    }

    let mut exprs: Vec<simple_graph::Expr> = join
        .on
        .iter()
        .map(|(l, r)| {
            let left = convert_expr(l, ctx, bindings)?;
            let right = convert_expr(r, ctx, bindings)?;
            Ok(ExprData::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            }
            .add(ctx))
        })
        .collect::<FromDFResult<_>>()?;

    if let Some(filter) = &join.filter {
        exprs.push(convert_expr(filter, ctx, bindings)?);
    }

    let on = match exprs.len() {
        0 => ExprData::Literal(ScalarValue::Boolean(true)).add(ctx),
        1 => exprs.remove(0),
        _ => ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(ctx),
    };

    Ok(OperatorData::Join(SGJoin {
        join_type,
        on,
        outer,
        inner,
    })
    .add(ctx))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn convert_join_type(jt: &DFJoinType) -> FromDFResult<JoinType> {
    match jt {
        DFJoinType::Inner => Ok(JoinType::Inner),
        DFJoinType::Left => Ok(JoinType::LeftOuter),
        DFJoinType::Right => Ok(JoinType::RightOuter),
        DFJoinType::Full => Ok(JoinType::FullOuter),
        DFJoinType::LeftSemi => Ok(JoinType::LeftSemi),
        DFJoinType::LeftAnti => Ok(JoinType::LeftAnti),
        other => Err(FromDFError::Unsupported(format!("join type {other:?}"))),
    }
}

fn combine_predicates(
    predicates: &[DFExpr],
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<simple_graph::Expr> {
    let exprs: Vec<_> = predicates
        .iter()
        .map(|e| convert_expr(e, ctx, bindings))
        .collect::<FromDFResult<_>>()?;
    if exprs.len() == 1 {
        Ok(exprs.into_iter().next().unwrap())
    } else {
        Ok(ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(ctx))
    }
}

fn convert_expr(
    expr: &DFExpr,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<simple_graph::Expr> {
    let data = match expr {
        DFExpr::Column(col) => {
            let c = bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)?;
            ExprData::ColumnRef(c)
        }
        DFExpr::Literal(scalar, _) => ExprData::Literal(convert_scalar(scalar)?),
        DFExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = convert_expr(left, ctx, bindings)?;
            let right = convert_expr(right, ctx, bindings)?;
            match op {
                DFOperator::And => ExprData::Nary {
                    op: NaryOp::And,
                    exprs: vec![left, right],
                },
                DFOperator::Or => ExprData::Nary {
                    op: NaryOp::Or,
                    exprs: vec![left, right],
                },
                other => match convert_binary_op(other) {
                    Some(sg_op) => ExprData::Binary {
                        op: sg_op,
                        left,
                        right,
                    },
                    None => {
                        return Err(FromDFError::Unsupported(format!("operator {other:?}")));
                    }
                },
            }
        }
        DFExpr::Not(inner) => ExprData::Unary {
            op: simple_graph::UnaryOp::Not,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::IsNull(inner) => ExprData::Unary {
            op: simple_graph::UnaryOp::IsNull,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::IsNotNull(inner) => ExprData::Unary {
            op: simple_graph::UnaryOp::IsNotNull,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::Negative(inner) => ExprData::Unary {
            op: simple_graph::UnaryOp::Negate,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::Cast(cast) => ExprData::Cast {
            expr: convert_expr(&cast.expr, ctx, bindings)?,
            ty: cast.data_type.clone(),
        },
        DFExpr::Case(Case {
            when_then_expr,
            else_expr,
            ..
        }) => {
            let when_then = when_then_expr
                .iter()
                .map(|(w, t)| {
                    Ok((
                        convert_expr(w, ctx, bindings)?,
                        convert_expr(t, ctx, bindings)?,
                    ))
                })
                .collect::<FromDFResult<_>>()?;
            let else_expr = else_expr
                .as_ref()
                .map(|e| convert_expr(e, ctx, bindings))
                .transpose()?;
            ExprData::CaseWhen {
                when_then,
                else_expr,
            }
        }
        other => {
            return Err(FromDFError::Unsupported(format!("expr {other:?}")));
        }
    };
    Ok(data.add(ctx))
}

fn convert_agg_expr(
    expr: &DFExpr,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<AggregateExpr> {
    let DFExpr::AggregateFunction(DFAggregateFunction { func, params }) = expr else {
        return Err(FromDFError::Unsupported(format!(
            "expected aggregate function, got {expr:?}"
        )));
    };

    let name = func.name().to_lowercase();
    let args = &params.args;
    let distinct = params.distinct;

    if name == "count" && args.is_empty() {
        return Ok(AggregateExpr::CountStar);
    }

    let func = match name.as_str() {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "avg" => AggregateFunction::Avg,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        other => AggregateFunction::extension(other),
    };

    let arg = args
        .first()
        .ok_or_else(|| FromDFError::Unsupported("aggregate with no args".into()))?;
    let arg = convert_expr(arg, ctx, bindings)?;

    Ok(AggregateExpr::Func {
        func,
        arg,
        distinct,
    })
}

fn convert_scalar(scalar: &DFScalarValue) -> FromDFResult<ScalarValue> {
    use datafusion::arrow::datatypes::DataType;
    match scalar {
        DFScalarValue::Null => Ok(ScalarValue::Null(DataType::Null)),
        DFScalarValue::Boolean(Some(v)) => Ok(ScalarValue::Boolean(*v)),
        DFScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(*v)),
        DFScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(*v)),
        DFScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(*v)),
        DFScalarValue::Utf8(Some(v)) | DFScalarValue::LargeUtf8(Some(v)) => {
            Ok(ScalarValue::Utf8(v.clone()))
        }
        DFScalarValue::Date32(Some(v)) => Ok(ScalarValue::Date32(*v)),
        DFScalarValue::Decimal128(Some(v), p, s) => Ok(ScalarValue::Decimal128 {
            value: *v,
            precision: *p,
            scale: *s,
        }),
        other => Err(FromDFError::Unsupported(format!("scalar {other:?}"))),
    }
}

fn convert_binary_op(op: &DFOperator) -> Option<BinaryOp> {
    match op {
        DFOperator::Eq => Some(BinaryOp::Eq),
        DFOperator::NotEq => Some(BinaryOp::NotEq),
        DFOperator::Lt => Some(BinaryOp::Lt),
        DFOperator::LtEq => Some(BinaryOp::LtEq),
        DFOperator::Gt => Some(BinaryOp::Gt),
        DFOperator::GtEq => Some(BinaryOp::GtEq),
        DFOperator::Plus => Some(BinaryOp::Add),
        DFOperator::Minus => Some(BinaryOp::Subtract),
        DFOperator::Multiply => Some(BinaryOp::Multiply),
        DFOperator::Divide => Some(BinaryOp::Divide),
        _ => None,
    }
}

/// Evaluates a constant integer expression (used for LIMIT/OFFSET values).
fn eval_usize_expr(expr: &DFExpr) -> Option<usize> {
    match expr {
        DFExpr::Literal(DFScalarValue::Int64(Some(n)), _) => (*n).try_into().ok(),
        DFExpr::Literal(DFScalarValue::Int32(Some(n)), _) => (*n).try_into().ok(),
        DFExpr::Literal(DFScalarValue::UInt64(Some(n)), _) => (*n).try_into().ok(),
        _ => None,
    }
}
