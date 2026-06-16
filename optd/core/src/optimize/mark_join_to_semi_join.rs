//! Converts `LeftMarkJoin` into `LeftSemi` or `LeftAnti` joins.
//!
//! Pattern: a `Selection` immediately above a `LeftMarkJoin` where the marker
//! column appears only as a top-level conjunct in the selection predicate.
//!
//! - `exists_mark` used positively → `LeftSemi`
//! - `NOT(exists_mark)` used positively → `LeftAnti`
//!
//! The marker conjunct is removed from the predicate; if the remaining predicate
//! is empty the `Selection` is dropped entirely.
//!
//! This pass relies on the current producer invariant that `SubqueryToJoin`
//! creates `LeftMark` only for expression-context `EXISTS`. Expression-context
//! `IN`/`NOT IN` remains as `InSubquery` because optd does not yet have a
//! tri-valued marker column.

use crate::{
    Column, Expr, ExprData, Join, JoinType, NaryOp, Operator, OperatorData, OptimizerContext,
    Selection, UnaryOp, expr_used_columns,
    optimize::{OperatorRewrite, OptimizeResult, Pass, Rewrite},
};

pub struct MarkJoinToSemiJoin;

impl Pass for MarkJoinToSemiJoin {
    fn name(&self) -> &'static str {
        "MarkJoinToSemiJoin"
    }
}

impl OperatorRewrite for MarkJoinToSemiJoin {
    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite> {
        // Match: Selection over a LeftMarkJoin.
        let OperatorData::Selection(sel) = ctx.query.operator(op).clone() else {
            return Ok(Rewrite::Keep);
        };
        let OperatorData::Join(join) = ctx.query.operator(sel.input).clone() else {
            return Ok(Rewrite::Keep);
        };
        let JoinType::LeftMark(marker) = join.join_type else {
            return Ok(Rewrite::Keep);
        };

        // Split the selection predicate into conjuncts.
        let conjuncts = split_conjuncts(sel.predicate, ctx);

        // Find exactly one conjunct that is `marker` (semi) or `NOT(marker)`
        // (anti). If the marker is referenced anywhere else, the marker remains
        // observable and replacing the mark join would leave dangling columns or
        // change three-valued boolean behavior.
        let mut semi_type = None;
        let mut remaining: Vec<Expr> = Vec::new();

        for c in conjuncts {
            match classify_marker(c, marker, ctx) {
                Some(jt) if semi_type.is_none() => semi_type = Some(jt),
                Some(_) => return Ok(Rewrite::Keep),
                _ => remaining.push(c),
            }
        }

        let Some(new_join_type) = semi_type else {
            return Ok(Rewrite::Keep);
        };
        if remaining
            .iter()
            .any(|expr| expr_references_column(*expr, marker, ctx))
        {
            return Ok(Rewrite::Keep);
        }

        // Build the new semi/anti join.
        let new_join = OperatorData::Join(Join {
            join_type: new_join_type,
            on: join.on,
            outer: join.outer,
            inner: join.inner,
        })
        .add(&mut ctx.query);

        // Wrap in a Selection if there are remaining conjuncts.
        let result = if remaining.is_empty() {
            new_join
        } else {
            let pred = conjuncts_to_expr(remaining, ctx);
            OperatorData::Selection(Selection {
                predicate: pred,
                input: new_join,
            })
            .add(&mut ctx.query)
        };

        Ok(Rewrite::Replace(result))
    }
}

/// Returns `Some(LeftSemi)` if `expr` is `ColumnRef(marker)`,
/// `Some(LeftAnti)` if it is `NOT(ColumnRef(marker))`, else `None`.
fn classify_marker(expr: Expr, marker: Column, ctx: &OptimizerContext) -> Option<JoinType> {
    match expr.get(&ctx.query) {
        ExprData::ColumnRef(col) if *col == marker => Some(JoinType::LeftSemi),
        ExprData::Unary {
            op: UnaryOp::Not,
            expr: inner,
        } => match inner.get(&ctx.query) {
            ExprData::ColumnRef(col) if *col == marker => Some(JoinType::LeftAnti),
            _ => None,
        },
        _ => None,
    }
}

fn split_conjuncts(expr: Expr, ctx: &OptimizerContext) -> Vec<Expr> {
    match expr.get(&ctx.query).clone() {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .into_iter()
            .flat_map(|e| split_conjuncts(e, ctx))
            .collect(),
        _ => vec![expr],
    }
}

fn conjuncts_to_expr(mut exprs: Vec<Expr>, ctx: &mut OptimizerContext) -> Expr {
    if exprs.len() == 1 {
        exprs.remove(0)
    } else {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(&mut ctx.query)
    }
}

fn expr_references_column(expr: Expr, column: Column, ctx: &OptimizerContext) -> bool {
    expr_used_columns(&ctx.query, expr)
        .map(|columns| columns.contains(&column))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use crate::{
        ColumnData, ExprData, Join, JoinType, NaryOp, OperatorData, OptimizerContext, QueryContext,
        ScalarValue, Scan, Selection, TableRef,
        optimize::{OperatorRewrite, Rewrite},
    };

    fn mark_join_query() -> (
        OptimizerContext,
        crate::Operator,
        crate::Column,
        crate::Column,
    ) {
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let marker = ColumnData::new("exists_mark", DataType::Boolean).add(&mut query);

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
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftMark(marker),
            on,
            outer,
            inner,
        })
        .add(&mut query);

        (OptimizerContext::new(query), join, marker, outer_col)
    }

    #[test]
    fn converts_single_top_level_marker_filter_to_semi_join() {
        let (mut opt, join, marker, _) = mark_join_query();
        let predicate = ExprData::ColumnRef(marker).add(&mut opt.query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: join,
        })
        .add(&mut opt.query);

        let rewrite = super::MarkJoinToSemiJoin
            .rewrite(selection, &mut opt)
            .unwrap();

        let Rewrite::Replace(root) = rewrite else {
            panic!("expected rewrite");
        };
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("expected join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
    }

    #[test]
    fn refuses_marker_referenced_in_remaining_predicate() {
        let (mut opt, join, marker, outer_col) = mark_join_query();
        let marker_expr = ExprData::ColumnRef(marker).add(&mut opt.query);
        let outer_eq = ExprData::Binary {
            op: crate::BinaryOp::Eq,
            left: ExprData::ColumnRef(outer_col).add(&mut opt.query),
            right: ExprData::Literal(ScalarValue::Int64(1)).add(&mut opt.query),
        }
        .add(&mut opt.query);
        let marker_or_local = ExprData::Nary {
            op: crate::NaryOp::Or,
            exprs: vec![marker_expr, outer_eq],
        }
        .add(&mut opt.query);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![marker_expr, marker_or_local],
        }
        .add(&mut opt.query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: join,
        })
        .add(&mut opt.query);

        let rewrite = super::MarkJoinToSemiJoin
            .rewrite(selection, &mut opt)
            .unwrap();

        assert!(matches!(rewrite, Rewrite::Keep));
    }

    #[test]
    fn refuses_multiple_top_level_marker_filters() {
        let (mut opt, join, marker, _) = mark_join_query();
        let marker_expr = ExprData::ColumnRef(marker).add(&mut opt.query);
        let not_marker = ExprData::Unary {
            op: crate::UnaryOp::Not,
            expr: marker_expr,
        }
        .add(&mut opt.query);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![marker_expr, not_marker],
        }
        .add(&mut opt.query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: join,
        })
        .add(&mut opt.query);

        let rewrite = super::MarkJoinToSemiJoin
            .rewrite(selection, &mut opt)
            .unwrap();

        assert!(matches!(rewrite, Rewrite::Keep));
    }
}
