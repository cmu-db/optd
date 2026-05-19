//! Converts DataFusion [`LogicalPlan`] trees into optd IR.
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
    logical_plan::{EmptyRelation, Values as DFValues},
};
use optd::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Column, ColumnData, ConstScan,
    CrossProduct, ExprData, Join as OptdJoin, JoinType, Limit as OptdLimit, Map, NaryOp,
    NullOrdering, Operator, OperatorData, Projection as OptdProjection, QueryContext, ScalarValue,
    Scan, Selection, Sort as OptdSort, SortDirection, SortKey, TableRef,
};

/// Error type for the DataFusion → optd converter.
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

impl Default for BindingContext {
    fn default() -> Self {
        Self::new()
    }
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

        // Unqualified fallback — only if unambiguous (or all matches are the same handle).
        let matches: Vec<Column> = self
            .scopes
            .iter()
            .rev()
            .flat_map(|s| s.bindings.iter())
            .filter(|((_, n), _)| n == name)
            .map(|(_, c)| *c)
            .collect();

        // Dedup: if all matches resolve to the same Column handle, it's unambiguous.
        let unique: std::collections::HashSet<Column> = matches.iter().copied().collect();
        match unique.len() {
            1 => Ok(*unique.iter().next().unwrap()),
            0 => Err(FromDFError::Schema(format!(
                "column not found: {}{}",
                qualifier.map(|q| format!("{q}.")).unwrap_or_default(),
                name
            ))),
            _ => Err(FromDFError::Schema(format!(
                "ambiguous column reference '{name}' matches {} columns",
                unique.len()
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Converts a DataFusion `LogicalPlan` into optd IR.
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
        LogicalPlan::EmptyRelation(empty) => convert_empty_relation(empty, ctx, bindings),
        LogicalPlan::Values(values) => convert_values(values, ctx, bindings),
        LogicalPlan::Filter(filter) => convert_filter(filter, ctx, bindings),
        LogicalPlan::Projection(proj) => convert_projection(proj, ctx, bindings),
        LogicalPlan::Aggregate(agg) => convert_aggregate(agg, ctx, bindings),
        LogicalPlan::Sort(sort) => convert_sort(sort, ctx, bindings),
        LogicalPlan::Limit(limit) => convert_limit(limit, ctx, bindings),
        LogicalPlan::Join(join) => convert_join(join, ctx, bindings),
        LogicalPlan::SubqueryAlias(alias) => {
            let depth = bindings.scopes.len();
            let input = convert_plan(&alias.input, ctx, bindings)?;
            let alias_name = alias.alias.table().to_string();
            // Build defs: for each column in the alias schema, create a renamed handle.
            let schema = alias.schema.as_ref();
            let mut defs = Vec::new();
            for i in 0..schema.fields().len() {
                let (_, field) = schema.qualified_field(i);
                let original = bindings.resolve(None, field.name()).unwrap_or_else(|_| {
                    // Fall back: find by name in scopes added since depth.
                    bindings.scopes[depth..]
                        .iter()
                        .flat_map(|s| s.bindings.iter())
                        .find(|((_, n), _)| n == field.name())
                        .map(|(_, c)| *c)
                        .unwrap_or_else(|| {
                            ColumnData::new(field.name().clone(), field.data_type().clone())
                                .add(ctx)
                        })
                });
                let renamed = ColumnData::with_qualifier(
                    field.name().clone(),
                    field.data_type().clone(),
                    alias_name.clone(),
                )
                .add(ctx);
                bindings.bind(Some(alias_name.clone()), field.name().clone(), renamed);
                defs.push((renamed, original));
            }

            Ok(OperatorData::Rename(optd::Rename {
                alias: alias_name,
                defs,
                input,
            })
            .add(ctx))
        }
        other => Err(FromDFError::Unsupported(format!(
            "{}",
            other.display_indent()
        ))),
    }
}

fn convert_empty_relation(
    empty: &EmptyRelation,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    bindings.push();
    let mut columns = Vec::new();
    let schema = empty.schema.as_ref();
    for i in 0..schema.fields().len() {
        let (qualifier, field) = schema.qualified_field(i);
        let qualifier = qualifier.map(|q| q.table().to_string());
        let column = match &qualifier {
            Some(q) => ColumnData::with_qualifier(
                field.name().clone(),
                field.data_type().clone(),
                q.clone(),
            )
            .add(ctx),
            None => ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx),
        };
        bindings.bind(qualifier, field.name().clone(), column);
        columns.push(column);
    }
    Ok(OperatorData::ConstScan(ConstScan {
        columns,
        rows: if empty.produce_one_row {
            vec![vec![]]
        } else {
            vec![]
        },
    })
    .add(ctx))
}

fn convert_values(
    values: &DFValues,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    bindings.push();
    let mut columns = Vec::new();
    let schema = values.schema.as_ref();
    for i in 0..schema.fields().len() {
        let (qualifier, field) = schema.qualified_field(i);
        let qualifier = qualifier.map(|q| q.table().to_string());
        let column = match &qualifier {
            Some(q) => ColumnData::with_qualifier(
                field.name().clone(),
                field.data_type().clone(),
                q.clone(),
            )
            .add(ctx),
            None => ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx),
        };
        bindings.bind(qualifier, field.name().clone(), column);
        columns.push(column);
    }

    let rows = values
        .values
        .iter()
        .map(|row| {
            row.iter()
                .map(|expr| convert_expr(expr, ctx, bindings))
                .collect::<FromDFResult<Vec<_>>>()
        })
        .collect::<FromDFResult<Vec<_>>>()?;

    Ok(OperatorData::ConstScan(ConstScan { columns, rows }).add(ctx))
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
            let q = qualifier.map(|r| r.to_string());
            let col = if let Some(ref q_str) = q {
                ColumnData::with_qualifier(
                    field.name().clone(),
                    field.data_type().clone(),
                    q_str.clone(),
                )
                .add(ctx)
            } else {
                ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx)
            };
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

    let projected_columns: Option<Vec<Column>> = proj
        .expr
        .iter()
        .map(|e| resolve_projected_column(e, bindings))
        .collect::<FromDFResult<Option<Vec<_>>>>()?;

    if let Some(columns) = projected_columns {
        bind_projection_output_names(proj, &columns, bindings);
        Ok(OperatorData::Projection(OptdProjection { columns, input }).add(ctx))
    } else {
        let mut computations = Vec::new();
        let mut output_cols = Vec::new();

        bindings.push();
        for (i, expr) in proj.expr.iter().enumerate() {
            let (qualifier, field) = proj.schema.qualified_field(i);
            let q = qualifier.map(|r| r.to_string());

            if let DFExpr::Column(col) = expr {
                let optd_col =
                    bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)?;
                output_cols.push(optd_col);
            } else {
                let optd_expr = convert_expr(expr, ctx, bindings)?;
                let col = ColumnData::new(field.name().clone(), field.data_type().clone()).add(ctx);
                bindings.bind(q, field.name().clone(), col);
                computations.push((col, optd_expr));
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

        Ok(OperatorData::Projection(OptdProjection {
            columns: output_cols,
            input: after_map,
        })
        .add(ctx))
    }
}

fn resolve_projected_column(
    expr: &DFExpr,
    bindings: &BindingContext,
) -> FromDFResult<Option<Column>> {
    match expr {
        DFExpr::Column(col) => Ok(Some(
            bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)?,
        )),
        DFExpr::Alias(alias) => resolve_projected_column(&alias.expr, bindings),
        _ => Ok(None),
    }
}

fn bind_projection_output_names(
    proj: &Projection,
    columns: &[Column],
    bindings: &mut BindingContext,
) {
    for (i, column) in columns.iter().enumerate() {
        let (qualifier, field) = proj.schema.qualified_field(i);
        bindings.bind(
            qualifier.map(|r| r.to_string()),
            field.name().clone(),
            *column,
        );
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

    let sort_op = OperatorData::Sort(OptdSort { keys, input }).add(ctx);

    // DataFusion may push a LIMIT into Sort.fetch.
    if let Some(fetch) = sort.fetch {
        return Ok(OperatorData::Limit(OptdLimit {
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
    Ok(OperatorData::Limit(OptdLimit {
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

    let mut exprs: Vec<optd::Expr> = join
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

    if matches!(join.join_type, DFJoinType::Inner)
        && (exprs.is_empty() || (exprs.len() == 1 && is_true_expr(exprs[0], ctx)))
    {
        return Ok(OperatorData::CrossProduct(CrossProduct { outer, inner }).add(ctx));
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

    Ok(OperatorData::Join(OptdJoin {
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

fn is_true_expr(expr: optd::Expr, ctx: &QueryContext) -> bool {
    matches!(expr.get(ctx), ExprData::Literal(ScalarValue::Boolean(true)))
}

fn combine_predicates(
    predicates: &[DFExpr],
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<optd::Expr> {
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

/// Converts a subquery `LogicalPlan` while preserving outer scope visibility
/// for correlated references. Saves the current scope depth and restores it
/// after conversion so inner columns don't leak into the outer context.
fn convert_subquery_plan(
    plan: &LogicalPlan,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<Operator> {
    let depth = bindings.scopes.len();
    let result = convert_plan(plan, ctx, bindings);
    bindings.scopes.truncate(depth);
    result
}

fn convert_expr(
    expr: &DFExpr,
    ctx: &mut QueryContext,
    bindings: &mut BindingContext,
) -> FromDFResult<optd::Expr> {
    let data = match expr {
        DFExpr::Column(col) => {
            let c = bindings.resolve(col.relation.as_ref().map(|r| r.table()), &col.name)?;
            ExprData::ColumnRef(c)
        }
        // Correlated outer reference — resolve the same way as a regular column.
        DFExpr::OuterReferenceColumn(_, col) => {
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
                    Some(optd_op) => ExprData::Binary {
                        op: optd_op,
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
            op: optd::UnaryOp::Not,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::IsNull(inner) => ExprData::Unary {
            op: optd::UnaryOp::IsNull,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::IsNotNull(inner) => ExprData::Unary {
            op: optd::UnaryOp::IsNotNull,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::Negative(inner) => ExprData::Unary {
            op: optd::UnaryOp::Negate,
            expr: convert_expr(inner, ctx, bindings)?,
        },
        DFExpr::Cast(cast) => ExprData::Cast {
            expr: convert_expr(&cast.expr, ctx, bindings)?,
            ty: cast.data_type.clone(),
        },
        DFExpr::Alias(alias) => {
            // Strip the alias — optd doesn't have named expressions.
            return convert_expr(&alias.expr, ctx, bindings);
        }
        DFExpr::Exists(exists) => {
            let subquery = convert_subquery_plan(&exists.subquery.subquery, ctx, bindings)?;
            ExprData::Exists {
                subquery,
                negated: exists.negated,
            }
        }
        DFExpr::InSubquery(insub) => {
            let expr = convert_expr(&insub.expr, ctx, bindings)?;
            let subquery = convert_subquery_plan(&insub.subquery.subquery, ctx, bindings)?;
            ExprData::InSubquery {
                expr,
                subquery,
                negated: insub.negated,
            }
        }
        DFExpr::ScalarSubquery(sub) => {
            let subquery = convert_subquery_plan(&sub.subquery, ctx, bindings)?;
            ExprData::ScalarSubquery { subquery }
        }
        DFExpr::Between(between) => {
            // expr BETWEEN low AND high → (expr >= low) AND (expr <= high)
            let expr = convert_expr(&between.expr, ctx, bindings)?;
            let low = convert_expr(&between.low, ctx, bindings)?;
            let high = convert_expr(&between.high, ctx, bindings)?;
            let ge = ExprData::Binary {
                op: BinaryOp::GtEq,
                left: expr,
                right: low,
            }
            .add(ctx);
            let le = ExprData::Binary {
                op: BinaryOp::LtEq,
                left: expr,
                right: high,
            }
            .add(ctx);
            let and = ExprData::Nary {
                op: NaryOp::And,
                exprs: vec![ge, le],
            };
            if between.negated {
                // NOT (expr BETWEEN low AND high)
                let inner = and.add(ctx);
                ExprData::Unary {
                    op: optd::UnaryOp::Not,
                    expr: inner,
                }
            } else {
                and
            }
        }
        DFExpr::InList(inlist) => {
            // expr IN (a, b, c) → (expr = a) OR (expr = b) OR (expr = c)
            let expr = convert_expr(&inlist.expr, ctx, bindings)?;
            let exprs: Vec<optd::Expr> = inlist
                .list
                .iter()
                .map(|item| {
                    let item_expr = convert_expr(item, ctx, bindings)?;
                    Ok(ExprData::Binary {
                        op: BinaryOp::Eq,
                        left: expr,
                        right: item_expr,
                    }
                    .add(ctx))
                })
                .collect::<FromDFResult<_>>()?;
            let or_expr = ExprData::Nary {
                op: NaryOp::Or,
                exprs,
            };
            if inlist.negated {
                let inner = or_expr.add(ctx);
                ExprData::Unary {
                    op: optd::UnaryOp::Not,
                    expr: inner,
                }
            } else {
                or_expr
            }
        }
        DFExpr::Like(like) => {
            let expr = convert_expr(&like.expr, ctx, bindings)?;
            let pattern = convert_expr(&like.pattern, ctx, bindings)?;
            ExprData::Like {
                negated: like.negated,
                expr,
                pattern,
                case_insensitive: like.case_insensitive,
            }
        }
        DFExpr::ScalarFunction(sf) => {
            let args: Vec<optd::Expr> = sf
                .args
                .iter()
                .map(|a| convert_expr(a, ctx, bindings))
                .collect::<FromDFResult<_>>()?;
            ExprData::ScalarFunction {
                function: optd::ScalarFunction::extension(sf.func.name()),
                args,
            }
        }
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

    // count(literal) is semantically COUNT(*) — counting rows, not column values.
    if name == "count" && matches!(args.as_slice(), [DFExpr::Literal(_, _)]) {
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
        DFScalarValue::IntervalMonthDayNano(Some(v)) => Ok(ScalarValue::IntervalMonthDayNano {
            months: v.months,
            days: v.days,
            nanoseconds: v.nanoseconds,
        }),
        DFScalarValue::IntervalDayTime(Some(v)) => Ok(ScalarValue::IntervalDayTime {
            days: v.days,
            milliseconds: v.milliseconds,
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

#[cfg(test)]
mod tests {
    use super::from_logical_plan;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;
    use optd::{OperatorData, QueryContext};
    use std::sync::Arc;

    #[test]
    fn imports_empty_relation_as_zero_row_const_scan() {
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        match ctx.operator(root) {
            OperatorData::ConstScan(const_scan) => {
                assert!(const_scan.columns.is_empty());
                assert!(const_scan.rows.is_empty());
            }
            other => panic!("expected ConstScan, got {other:?}"),
        }
    }

    #[test]
    fn imports_single_row_empty_relation_as_one_empty_row_const_scan() {
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        match ctx.operator(root) {
            OperatorData::ConstScan(const_scan) => {
                assert!(const_scan.columns.is_empty());
                assert_eq!(const_scan.rows, vec![vec![]]);
            }
            other => panic!("expected ConstScan, got {other:?}"),
        }
    }

    #[test]
    fn imports_empty_relation_schema_as_const_scan_columns() {
        let schema = DFSchema::from_unqualified_fields(
            vec![Arc::new(Field::new("a", DataType::Int64, true))].into(),
            std::collections::HashMap::new(),
        )
        .unwrap();
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        });
        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        match ctx.operator(root) {
            OperatorData::ConstScan(const_scan) => {
                assert_eq!(const_scan.columns.len(), 1);
                assert_eq!(ctx.column(const_scan.columns[0]).name, "a");
                assert!(const_scan.rows.is_empty());
            }
            other => panic!("expected ConstScan, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn imports_values_as_const_scan_with_rows_and_columns() {
        let session = SessionContext::new();
        let plan = session
            .state()
            .create_logical_plan("VALUES (1, 2), (3, 4)")
            .await
            .unwrap();
        let mut ctx = QueryContext::new();
        let root = from_logical_plan(&plan, &mut ctx).unwrap();
        match ctx.operator(root) {
            OperatorData::ConstScan(const_scan) => {
                assert_eq!(const_scan.columns.len(), 2);
                assert_eq!(const_scan.rows.len(), 2);
                assert_eq!(const_scan.rows[0].len(), 2);
            }
            other => panic!("expected ConstScan, got {other:?}"),
        }
    }
}
