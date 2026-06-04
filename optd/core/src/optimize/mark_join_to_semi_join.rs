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

use crate::{
    Column, Expr, ExprData, Join, JoinType, NaryOp, Operator, OperatorData, OptimizerContext,
    Selection, UnaryOp,
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

        // Find a conjunct that is exactly `marker` (semi) or `NOT(marker)` (anti).
        let mut semi_type = None;
        let mut remaining: Vec<Expr> = Vec::new();

        for c in conjuncts {
            match classify_marker(c, marker, ctx) {
                Some(jt) if semi_type.is_none() => semi_type = Some(jt),
                _ => remaining.push(c),
            }
        }

        let Some(new_join_type) = semi_type else {
            return Ok(Rewrite::Keep);
        };

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
