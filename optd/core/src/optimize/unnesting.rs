//! Unnesting pass for correlated subqueries.
//!
//! The active DataFusion runner still uses the logical converter while this pass
//! grows TPC-H decorrelation support. The first rewrite handles common
//! subquery-derived joins by lifting correlated conjuncts from the inner input
//! into the parent join condition.

use crate::{
    AnalysisContext, AvailableColumns, Column, Expr, ExprData, FreeColumns, Join, JoinType, Limit,
    Map, NaryOp, Operator, OperatorData, OptimizerContext, Projection, Relation, Rename, Selection,
    Sort, expr_used_columns,
    optimize::{OptimizeResult, Pass, PassResult, QueryPass},
};

/// Decorrelates subquery-derived joins before later relational rewrites.
pub struct Unnesting;

impl Pass for Unnesting {
    fn name(&self) -> &'static str {
        "Unnesting"
    }
}

impl QueryPass for Unnesting {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        let rewritten = rewrite_operator(root, ctx)?;
        if let Some(root) = rewritten {
            ctx.query.set_root(root);
            Ok(PassResult::Changed)
        } else {
            Ok(PassResult::Unchanged)
        }
    }
}

fn rewrite_operator(op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Option<Operator>> {
    let operator = op.get(&ctx.query).clone();
    match operator {
        OperatorData::Join(join) => rewrite_join(op, join, ctx),
        OperatorData::Selection(selection) => {
            rewrite_operator(selection.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Selection(Selection {
                        predicate: selection.predicate,
                        input,
                    })
                    .add(&mut ctx.query),
                ))
            })
        }
        OperatorData::Map(map) => rewrite_operator(map.input, ctx)?.map_or(Ok(None), |input| {
            Ok(Some(
                OperatorData::Map(Map {
                    computations: map.computations,
                    input,
                })
                .add(&mut ctx.query),
            ))
        }),
        OperatorData::Projection(projection) => {
            rewrite_operator(projection.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Projection(Projection {
                        columns: projection.columns,
                        input,
                    })
                    .add(&mut ctx.query),
                ))
            })
        }
        OperatorData::Aggregation(aggregation) => {
            rewrite_operator(aggregation.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Aggregation(crate::Aggregation {
                        keys: aggregation.keys,
                        aggregates: aggregation.aggregates,
                        input,
                    })
                    .add(&mut ctx.query),
                ))
            })
        }
        OperatorData::Sort(sort) => rewrite_operator(sort.input, ctx)?.map_or(Ok(None), |input| {
            Ok(Some(
                OperatorData::Sort(Sort {
                    keys: sort.keys,
                    input,
                })
                .add(&mut ctx.query),
            ))
        }),
        OperatorData::Limit(limit) => {
            rewrite_operator(limit.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Limit(Limit {
                        fetch: limit.fetch,
                        offset: limit.offset,
                        input,
                    })
                    .add(&mut ctx.query),
                ))
            })
        }
        OperatorData::Rename(rename) => {
            rewrite_operator(rename.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Rename(Rename {
                        alias: rename.alias,
                        defs: rename.defs,
                        input,
                    })
                    .add(&mut ctx.query),
                ))
            })
        }
        OperatorData::Output(output) => {
            rewrite_operator(output.input, ctx)?.map_or(Ok(None), |input| {
                Ok(Some(
                    OperatorData::Output(crate::Output { input }).add(&mut ctx.query),
                ))
            })
        }
        OperatorData::CrossProduct(cross) => {
            let outer = rewrite_operator(cross.outer, ctx)?;
            let inner = rewrite_operator(cross.inner, ctx)?;
            if outer.is_none() && inner.is_none() {
                Ok(None)
            } else {
                Ok(Some(
                    OperatorData::CrossProduct(crate::CrossProduct {
                        outer: outer.unwrap_or(cross.outer),
                        inner: inner.unwrap_or(cross.inner),
                    })
                    .add(&mut ctx.query),
                ))
            }
        }
        _ => Ok(None),
    }
}

fn rewrite_join(
    _op: Operator,
    join: Join,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Option<Operator>> {
    let original_outer = join.outer;
    let original_inner = join.inner;
    let outer = rewrite_operator(original_outer, ctx)?.unwrap_or(original_outer);
    let inner = rewrite_operator(original_inner, ctx)?.unwrap_or(original_inner);
    let join = Join {
        outer,
        inner,
        ..join
    };
    let children_changed = outer != original_outer || inner != original_inner;

    if !is_subquery_derived_join(&join.join_type) {
        if !children_changed {
            return Ok(None);
        }
        return Ok(Some(OperatorData::Join(join).add(&mut ctx.query)));
    }

    if free_columns_for(ctx, join.inner)?.is_empty() {
        if !children_changed {
            return Ok(None);
        }
        return Ok(Some(OperatorData::Join(join).add(&mut ctx.query)));
    }

    let rewrite = lift_correlated_predicates(join.inner, ctx)?;
    if rewrite.predicates.is_empty() {
        if outer == join.outer && inner == join.inner {
            return Ok(None);
        }
        return Ok(Some(OperatorData::Join(join).add(&mut ctx.query)));
    }

    let inner = expose_columns(rewrite.input(join.inner), &rewrite.inner_columns, ctx)?;
    let join_type = if matches!(join.join_type, JoinType::Single) && rewrite.saw_aggregation {
        JoinType::LeftOuter
    } else {
        join.join_type
    };
    let on = make_and(
        std::iter::once(join.on).chain(rewrite.predicates).collect(),
        &mut ctx.query,
    );

    Ok(Some(
        OperatorData::Join(Join {
            join_type,
            on,
            outer,
            inner,
        })
        .add(&mut ctx.query),
    ))
}

#[derive(Debug, Default)]
struct InnerRewrite {
    input: Option<Operator>,
    predicates: Vec<Expr>,
    inner_columns: Vec<Column>,
    saw_aggregation: bool,
}

impl InnerRewrite {
    fn unchanged(input: Operator) -> Self {
        Self {
            input: Some(input),
            ..Self::default()
        }
    }

    fn input(&self, fallback: Operator) -> Operator {
        self.input.unwrap_or(fallback)
    }

    fn merge_child(&mut self, child: InnerRewrite) {
        self.predicates.extend(child.predicates);
        extend_unique_columns(&mut self.inner_columns, child.inner_columns);
        self.saw_aggregation |= child.saw_aggregation;
    }
}

fn lift_correlated_predicates(
    op: Operator,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<InnerRewrite> {
    match op.get(&ctx.query).clone() {
        OperatorData::Selection(selection) => lift_from_selection(selection, ctx),
        OperatorData::Aggregation(aggregation) => {
            let child = lift_correlated_predicates(aggregation.input, ctx)?;
            if child.predicates.is_empty() {
                return Ok(InnerRewrite::unchanged(op));
            }
            if aggregation_has_count(&aggregation) {
                return Ok(InnerRewrite::unchanged(op));
            }

            let input = child.input(aggregation.input);
            let new_op = OperatorData::Aggregation(crate::Aggregation {
                keys: aggregation.keys,
                aggregates: aggregation.aggregates,
                input,
            })
            .add(&mut ctx.query);

            Ok(InnerRewrite {
                input: Some(new_op),
                saw_aggregation: true,
                ..child
            })
        }
        OperatorData::Map(map) => lift_from_unary(op, map.input, ctx, |input, ctx| {
            OperatorData::Map(Map {
                computations: map.computations,
                input,
            })
            .add(ctx)
        }),
        OperatorData::Projection(projection) => {
            lift_from_unary(op, projection.input, ctx, |input, ctx| {
                OperatorData::Projection(Projection {
                    columns: projection.columns,
                    input,
                })
                .add(ctx)
            })
        }
        OperatorData::Sort(sort) => lift_from_unary(op, sort.input, ctx, |input, ctx| {
            OperatorData::Sort(Sort {
                keys: sort.keys,
                input,
            })
            .add(ctx)
        }),
        OperatorData::Limit(_) => Ok(InnerRewrite::unchanged(op)),
        OperatorData::Rename(rename) => lift_from_unary(op, rename.input, ctx, |input, ctx| {
            OperatorData::Rename(Rename {
                alias: rename.alias,
                defs: rename.defs,
                input,
            })
            .add(ctx)
        }),
        OperatorData::Join(join) => {
            lift_from_binary(op, join.outer, join.inner, ctx, |outer, inner, ctx| {
                OperatorData::Join(Join {
                    join_type: join.join_type,
                    on: join.on,
                    outer,
                    inner,
                })
                .add(ctx)
            })
        }
        OperatorData::CrossProduct(cross) => {
            lift_from_binary(op, cross.outer, cross.inner, ctx, |outer, inner, ctx| {
                OperatorData::CrossProduct(crate::CrossProduct { outer, inner }).add(ctx)
            })
        }
        _ => Ok(InnerRewrite::unchanged(op)),
    }
}

fn lift_from_selection(
    selection: Selection,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<InnerRewrite> {
    let child = lift_correlated_predicates(selection.input, ctx)?;
    let input = child.input(selection.input);
    let input_columns = available_columns_for(ctx, input)?;
    let conjuncts = split_conjuncts(selection.predicate, &ctx.query);
    let mut keep = Vec::new();
    let mut lift = Vec::new();
    let mut inner_columns = Vec::new();

    for conjunct in conjuncts {
        let used = expr_used_columns(&ctx.query, conjunct).unwrap_or_default();
        let uses_input = used.iter().any(|column| input_columns.contains(column));
        let uses_outer = used.iter().any(|column| !input_columns.contains(column));
        if uses_input && uses_outer {
            extend_unique_columns(
                &mut inner_columns,
                used.into_iter()
                    .filter(|column| input_columns.contains(column))
                    .collect::<Vec<_>>(),
            );
            lift.push(conjunct);
        } else {
            keep.push(conjunct);
        }
    }

    if lift.is_empty() && child.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(selection.input));
    }

    let input = if keep.is_empty() {
        input
    } else {
        OperatorData::Selection(Selection {
            predicate: make_and(keep, &mut ctx.query),
            input,
        })
        .add(&mut ctx.query)
    };

    let mut result = InnerRewrite {
        input: Some(input),
        predicates: lift,
        inner_columns,
        ..InnerRewrite::default()
    };
    result.merge_child(child);
    Ok(result)
}

fn lift_from_unary(
    original: Operator,
    input: Operator,
    ctx: &mut OptimizerContext,
    rebuild: impl FnOnce(Operator, &mut crate::QueryContext) -> Operator,
) -> OptimizeResult<InnerRewrite> {
    let child = lift_correlated_predicates(input, ctx)?;
    if child.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let new_input = child.input(input);
    let rebuilt = rebuild(new_input, &mut ctx.query);
    Ok(InnerRewrite {
        input: Some(rebuilt),
        ..child
    })
}

fn lift_from_binary(
    original: Operator,
    outer: Operator,
    inner: Operator,
    ctx: &mut OptimizerContext,
    rebuild: impl FnOnce(Operator, Operator, &mut crate::QueryContext) -> Operator,
) -> OptimizeResult<InnerRewrite> {
    let left = lift_correlated_predicates(outer, ctx)?;
    let right = lift_correlated_predicates(inner, ctx)?;
    if left.predicates.is_empty() && right.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let rebuilt = rebuild(left.input(outer), right.input(inner), &mut ctx.query);
    let mut result = InnerRewrite {
        input: Some(rebuilt),
        ..InnerRewrite::default()
    };
    result.merge_child(left);
    result.merge_child(right);
    Ok(result)
}

fn expose_columns(
    op: Operator,
    columns: &[Column],
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Operator> {
    // Lifted correlation predicates move from inside the subquery input to the
    // parent join condition, so any inner-side ColumnRef they use must be
    // produced by the rewritten join.inner. This is a handle-level requirement:
    // Projection can drop a Column, and Aggregation can only output grouping
    // keys or aggregate results. It is not connector name disambiguation.
    if columns.is_empty() {
        return Ok(op);
    }
    if columns_available(ctx, op, columns)? {
        return Ok(op);
    }

    match op.get(&ctx.query).clone() {
        OperatorData::Aggregation(mut aggregation) => {
            let input = expose_columns(aggregation.input, columns, ctx)?;
            aggregation.input = input;
            for column in columns {
                if !aggregation
                    .keys
                    .iter()
                    .any(|key| matches!(key.get(&ctx.query), ExprData::ColumnRef(existing) if existing == column))
                {
                    aggregation
                        .keys
                        .push(ExprData::ColumnRef(*column).add(&mut ctx.query));
                }
            }
            Ok(OperatorData::Aggregation(aggregation).add(&mut ctx.query))
        }
        OperatorData::Projection(mut projection) => {
            let input = expose_columns(projection.input, columns, ctx)?;
            projection.input = input;
            for column in columns {
                if !projection.columns.contains(column) {
                    projection.columns.push(*column);
                }
            }
            Ok(OperatorData::Projection(projection).add(&mut ctx.query))
        }
        OperatorData::Map(mut map) => {
            map.input = expose_columns(map.input, columns, ctx)?;
            Ok(OperatorData::Map(map).add(&mut ctx.query))
        }
        OperatorData::Selection(mut selection) => {
            selection.input = expose_columns(selection.input, columns, ctx)?;
            Ok(OperatorData::Selection(selection).add(&mut ctx.query))
        }
        OperatorData::Sort(mut sort) => {
            sort.input = expose_columns(sort.input, columns, ctx)?;
            Ok(OperatorData::Sort(sort).add(&mut ctx.query))
        }
        OperatorData::Limit(mut limit) => {
            limit.input = expose_columns(limit.input, columns, ctx)?;
            Ok(OperatorData::Limit(limit).add(&mut ctx.query))
        }
        _ => Ok(op),
    }
}

fn free_columns_for(ctx: &mut OptimizerContext, op: Operator) -> OptimizeResult<Vec<Column>> {
    ctx.analyses
        .get::<FreeColumns>(&ctx.query, op)
        .map_err(|error| crate::OptimizeError::PassError {
            pass: "Unnesting",
            message: error.to_string(),
        })
}

fn available_columns_for(ctx: &mut OptimizerContext, op: Operator) -> OptimizeResult<Vec<Column>> {
    ctx.analyses
        .get::<AvailableColumns>(&ctx.query, op)
        .map_err(|error| crate::OptimizeError::PassError {
            pass: "Unnesting",
            message: error.to_string(),
        })
}

fn columns_available(
    ctx: &mut OptimizerContext,
    op: Operator,
    columns: &[Column],
) -> OptimizeResult<bool> {
    let available = available_columns_for(ctx, op)?;
    Ok(columns.iter().all(|column| available.contains(column)))
}

fn aggregation_has_count(aggregation: &crate::Aggregation) -> bool {
    aggregation.aggregates.iter().any(|(_, aggregate)| {
        matches!(
            aggregate,
            crate::AggregateExpr::CountStar
                | crate::AggregateExpr::Func {
                    func: crate::AggregateFunction::Count,
                    ..
                }
        )
    })
}

fn split_conjuncts(expr: Expr, query: &crate::QueryContext) -> Vec<Expr> {
    match expr.get(query).clone() {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .into_iter()
            .flat_map(|expr| split_conjuncts(expr, query))
            .collect(),
        _ => vec![expr],
    }
}

fn make_and(mut exprs: Vec<Expr>, query: &mut crate::QueryContext) -> Expr {
    exprs.retain(|expr| !is_true_literal(*expr, query));
    match exprs.len() {
        0 => ExprData::Literal(crate::ScalarValue::Boolean(true)).add(query),
        1 => exprs.remove(0),
        _ => ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(query),
    }
}

fn is_true_literal(expr: Expr, query: &crate::QueryContext) -> bool {
    matches!(
        expr.get(query),
        ExprData::Literal(crate::ScalarValue::Boolean(true))
    )
}

fn extend_unique_columns(columns: &mut Vec<Column>, incoming: impl IntoIterator<Item = Column>) {
    for column in incoming {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
}

/// Returns subquery-derived joins whose inner input still depends on outer
/// columns, according to [`FreeColumns`].
pub fn correlated_subquery_joins(
    query: &crate::QueryContext,
    root: Operator,
) -> OptimizeResult<Vec<CorrelatedSubqueryJoin>> {
    // This inspection helper is intentionally independent of OptimizerContext;
    // the optimizer pass itself reuses ctx.analyses.
    let mut analyses = AnalysisContext::new();
    let mut out = Vec::new();
    collect_correlated_subquery_joins(query, root, &mut analyses, &mut out)?;
    Ok(out)
}

/// A subquery-derived join input that still contains outer references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatedSubqueryJoin {
    pub join: Operator,
    pub join_type: JoinType,
    pub inner: Operator,
    pub free_columns: Vec<Column>,
}

fn collect_correlated_subquery_joins(
    query: &crate::QueryContext,
    op: Operator,
    analyses: &mut AnalysisContext,
    out: &mut Vec<CorrelatedSubqueryJoin>,
) -> OptimizeResult<()> {
    if let OperatorData::Join(join) = op.get(query)
        && is_subquery_derived_join(&join.join_type)
    {
        let free_columns = analyses
            .get::<FreeColumns>(query, join.inner)
            .map_err(|error| crate::OptimizeError::PassError {
                pass: "Unnesting",
                message: error.to_string(),
            })?;

        if !free_columns.is_empty() {
            out.push(CorrelatedSubqueryJoin {
                join: op,
                join_type: join.join_type.clone(),
                inner: join.inner,
                free_columns,
            });
        }
    }

    for input in op.get(query).inputs() {
        collect_correlated_subquery_joins(query, input, analyses, out)?;
    }

    Ok(())
}

fn is_subquery_derived_join(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Single | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark { .. }
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        AggregateExpr, AggregateFunction, Aggregation, AvailableColumns, BinaryOp, ColumnData,
        ExprData, FreeColumns, Join, JoinType, Limit, NaryOp, OperatorData, OptimizerContext,
        PassResult, QueryContext, Rename, ScalarValue, Scan, Selection, TableRef,
        optimize::{
            OperatorRewriteAdaptor, PassManager, QueryPass, unnesting::correlated_subquery_joins,
        },
    };
    use arrow_schema::DataType;

    #[test]
    fn reports_single_join_inner_free_columns() {
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_a", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_a", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_col],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_col],
        })
        .add(&mut query);

        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_col).add(&mut query),
            right: ExprData::ColumnRef(outer_col).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);

        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let correlated = correlated_subquery_joins(&query, join).unwrap();
        assert_eq!(correlated.len(), 1);
        assert_eq!(correlated[0].join, join);
        assert_eq!(correlated[0].join_type, JoinType::Single);
        assert_eq!(correlated[0].inner, inner);
        assert_eq!(correlated[0].free_columns, vec![outer_col]);
    }

    #[test]
    fn ignores_decorrelated_subquery_join() {
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_a", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_a", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_col],
        })
        .add(&mut query);
        let inner = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_col],
        })
        .add(&mut query);

        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(outer_col).add(&mut query),
            right: ExprData::ColumnRef(inner_col).add(&mut query),
        }
        .add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let correlated = correlated_subquery_joins(&query, join).unwrap();
        assert!(correlated.is_empty());
    }

    #[test]
    fn unnests_correlated_scalar_aggregate_as_left_outer_join() {
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let inner_value = ColumnData::new("inner_value", DataType::Int64).add(&mut query);
        let min_value = ColumnData::new("min_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key, inner_value],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                min_value,
                AggregateExpr::Func {
                    func: AggregateFunction::Min,
                    arg: ExprData::ColumnRef(inner_value).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );

        let OperatorData::Aggregation(aggregation) = join.inner.get(&opt.query) else {
            panic!("inner should remain an aggregation");
        };
        assert_eq!(aggregation.keys.len(), 1);
        assert!(matches!(
            aggregation.keys[0].get(&opt.query),
            ExprData::ColumnRef(column) if *column == inner_key
        ));
    }

    #[test]
    fn does_not_unnest_correlated_scalar_count_aggregate() {
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let count_value = ColumnData::new("count_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(count_value, AggregateExpr::CountStar)],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Unchanged);
        assert_eq!(opt.query.root(), Some(join));
        assert_eq!(
            correlated_subquery_joins(&opt.query, join).unwrap()[0].free_columns,
            vec![outer_key]
        );
    }

    #[test]
    fn unnests_correlated_semijoin_by_lifting_inner_predicate() {
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
        assert_eq!(join.inner, inner_scan);
        assert!(matches!(
            join.on.get(&opt.query),
            ExprData::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn does_not_lift_correlated_predicate_through_limit() {
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let limit = OperatorData::Limit(Limit {
            fetch: Some(1),
            offset: 0,
            input: selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner: limit,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Unchanged);
        assert_eq!(opt.query.root(), Some(join));
        assert_eq!(
            correlated_subquery_joins(&opt.query, join).unwrap()[0].free_columns,
            vec![outer_key]
        );
    }

    #[test]
    fn keeps_distinct_inner_key_handle_when_field_metadata_conflicts() {
        let mut query = QueryContext::new();
        let outer_key =
            ColumnData::with_qualifier("ps_partkey", DataType::Int64, "partsupp").add(&mut query);
        let inner_key =
            ColumnData::with_qualifier("ps_partkey", DataType::Int64, "partsupp").add(&mut query);
        let inner_value = ColumnData::with_qualifier("ps_supplycost", DataType::Int64, "partsupp")
            .add(&mut query);
        let min_value = ColumnData::new("min_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_partsupp"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_partsupp"),
            columns: vec![inner_key, inner_value],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(outer_key).add(&mut query),
            right: ExprData::ColumnRef(inner_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                min_value,
                AggregateExpr::Func {
                    func: AggregateFunction::Min,
                    arg: ExprData::ColumnRef(inner_value).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        let inner_columns = opt
            .analyses
            .get::<AvailableColumns>(&opt.query, join.inner)
            .unwrap();
        assert_ne!(outer_key, inner_key);
        assert!(inner_columns.contains(&inner_key));
        assert!(matches!(
            join.on.get(&opt.query),
            ExprData::Binary { right, .. }
                if matches!(right.get(&opt.query), ExprData::ColumnRef(column) if *column == inner_key)
        ));
    }

    #[test]
    fn unnests_scalar_aggregate_through_map_and_projection_wrappers() {
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("p_partkey", DataType::Int64).add(&mut query);
        let outer_quantity = ColumnData::new("l_quantity", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("l_partkey", DataType::Int64).add(&mut query);
        let inner_quantity = ColumnData::new("l2_quantity", DataType::Int64).add(&mut query);
        let avg_quantity = ColumnData::new("avg_quantity", DataType::Float64).add(&mut query);
        let scaled_avg = ColumnData::new("scaled_avg", DataType::Float64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("joined"),
            columns: vec![outer_key, outer_quantity],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("lineitem"),
            columns: vec![inner_key, inner_quantity],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                avg_quantity,
                AggregateExpr::Func {
                    func: AggregateFunction::Avg,
                    arg: ExprData::ColumnRef(inner_quantity).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let scaled_expr = ExprData::Binary {
            op: BinaryOp::Multiply,
            left: ExprData::Literal(ScalarValue::Float64(0.2)).add(&mut query),
            right: ExprData::ColumnRef(avg_quantity).add(&mut query),
        }
        .add(&mut query);
        let map = OperatorData::Map(crate::Map {
            computations: vec![(scaled_avg, scaled_expr)],
            input: aggregate,
        })
        .add(&mut query);
        let projection = OperatorData::Projection(crate::Projection {
            columns: vec![scaled_avg],
            input: map,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: projection,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = OptimizerContext::new(query);
        let result = super::Unnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
        let inner_columns = opt
            .analyses
            .get::<AvailableColumns>(&opt.query, join.inner)
            .unwrap();
        assert!(inner_columns.contains(&inner_key));
    }

    #[test]
    fn unnests_mark_join_with_renamed_inner_then_mark_to_semi_can_fire() {
        let mut query = QueryContext::new();
        let outer_orderkey = ColumnData::new("l_orderkey", DataType::Int64).add(&mut query);
        let outer_suppkey = ColumnData::new("l_suppkey", DataType::Int64).add(&mut query);
        let base_orderkey = ColumnData::new("l_orderkey", DataType::Int64).add(&mut query);
        let base_suppkey = ColumnData::new("l_suppkey", DataType::Int64).add(&mut query);
        let inner_orderkey =
            ColumnData::with_qualifier("l_orderkey", DataType::Int64, "l2").add(&mut query);
        let inner_suppkey =
            ColumnData::with_qualifier("l_suppkey", DataType::Int64, "l2").add(&mut query);
        let marker = ColumnData::new("exists_mark", DataType::Boolean).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_lineitem"),
            columns: vec![outer_orderkey, outer_suppkey],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("lineitem"),
            columns: vec![base_orderkey, base_suppkey],
        })
        .add(&mut query);
        let rename = OperatorData::Rename(Rename {
            alias: "l2".to_string(),
            defs: vec![
                (inner_orderkey, base_orderkey),
                (inner_suppkey, base_suppkey),
            ],
            input: inner_scan,
        })
        .add(&mut query);
        let same_order = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_orderkey).add(&mut query),
            right: ExprData::ColumnRef(outer_orderkey).add(&mut query),
        }
        .add(&mut query);
        let different_supplier = ExprData::Binary {
            op: BinaryOp::NotEq,
            left: ExprData::ColumnRef(inner_suppkey).add(&mut query),
            right: ExprData::ColumnRef(outer_suppkey).add(&mut query),
        }
        .add(&mut query);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![same_order, different_supplier],
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: rename,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let mark_join = OperatorData::Join(Join {
            join_type: JoinType::LeftMark {
                marker,
                nullable: false,
            },
            on,
            outer,
            inner,
        })
        .add(&mut query);
        let marker_expr = ExprData::ColumnRef(marker).add(&mut query);
        let selection = OperatorData::Selection(Selection {
            predicate: marker_expr,
            input: mark_join,
        })
        .add(&mut query);
        query.set_root(selection);

        let mut opt = OptimizerContext::new(query);
        let mut pm = PassManager::new();
        pm.add_pass(super::Unnesting);
        pm.add_pass(OperatorRewriteAdaptor::new(crate::MarkJoinToSemiJoin));
        pm.run(&mut opt).unwrap();

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should become a join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
        assert!(matches!(join.on.get(&opt.query), ExprData::Nary { .. }));
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
    }
}
