//! Predicate pushdown: moves filter predicates into join conditions or onto inputs.
//!
//! For `Selection(pred, Join/CrossProduct(outer, inner))`, splits `pred` into conjuncts
//! and routes each one:
//! - refs only outer columns → `Selection` on outer input
//! - refs only inner columns → `Selection` on inner input
//! - refs columns from both sides → join `ON` condition
//! - refs columns outside this join → stays in the `Selection` above

use crate::{
    AvailableColumns, Expr, ExprData, NaryOp, Operator, OperatorData, OptimizerContext,
    ScalarValue, Selection, expr_used_columns,
    optimize::{OperatorRewrite, OptimizeResult, Pass, Rewrite},
};

pub struct PredicatePushdown;

impl Pass for PredicatePushdown {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }
}

impl OperatorRewrite for PredicatePushdown {
    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite> {
        let OperatorData::Selection(sel) = ctx.query.operator(op).clone() else {
            return Ok(Rewrite::Keep);
        };

        let (join_type, existing_on, outer, inner) = match ctx.query.operator(sel.input).clone() {
            OperatorData::Join(j) => (j.join_type, Some(j.on), j.outer, j.inner),
            OperatorData::CrossProduct(cp) => (crate::JoinType::Inner, None, cp.outer, cp.inner),
            _ => return Ok(Rewrite::Keep),
        };

        let outer_cols: Vec<_> = ctx
            .analyses
            .get::<AvailableColumns>(&ctx.query, outer)
            .unwrap_or_default();
        let inner_cols: Vec<_> = ctx
            .analyses
            .get::<AvailableColumns>(&ctx.query, inner)
            .unwrap_or_default();

        let conjuncts = split_conjuncts(sel.predicate, &ctx.query);
        let mut push_outer: Vec<Expr> = Vec::new();
        let mut push_inner: Vec<Expr> = Vec::new();
        let mut push_join: Vec<Expr> = Vec::new();
        let mut keep: Vec<Expr> = Vec::new();

        for c in conjuncts {
            let used = expr_used_columns(&ctx.query, c).unwrap_or_default();
            if used.iter().all(|col| outer_cols.contains(col)) {
                push_outer.push(c);
            } else if used.iter().all(|col| inner_cols.contains(col)) {
                push_inner.push(c);
            } else if used
                .iter()
                .all(|col| outer_cols.contains(col) || inner_cols.contains(col))
            {
                push_join.push(c);
            } else {
                keep.push(c);
            }
        }

        if push_outer.is_empty() && push_inner.is_empty() && push_join.is_empty() {
            return Ok(Rewrite::Keep);
        }

        let new_outer = wrap_selection(push_outer, outer, &mut ctx.query);
        let new_inner = wrap_selection(push_inner, inner, &mut ctx.query);

        let new_on = {
            let mut all_on: Vec<Expr> = existing_on.into_iter().collect();
            all_on.extend(push_join);
            match all_on.len() {
                0 => ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx.query),
                1 => all_on.remove(0),
                _ => ExprData::Nary {
                    op: NaryOp::And,
                    exprs: all_on,
                }
                .add(&mut ctx.query),
            }
        };

        let new_join = OperatorData::Join(crate::Join {
            join_type,
            on: new_on,
            outer: new_outer,
            inner: new_inner,
        })
        .add(&mut ctx.query);

        let result = if keep.is_empty() {
            new_join
        } else {
            let pred = make_and(keep, &mut ctx.query);
            OperatorData::Selection(Selection {
                predicate: pred,
                input: new_join,
            })
            .add(&mut ctx.query)
        };

        Ok(Rewrite::Replace(result))
    }
}

fn wrap_selection(preds: Vec<Expr>, input: Operator, ctx: &mut crate::QueryContext) -> Operator {
    if preds.is_empty() {
        return input;
    }
    let predicate = make_and(preds, ctx);
    OperatorData::Selection(Selection { predicate, input }).add(ctx)
}

fn make_and(mut exprs: Vec<Expr>, ctx: &mut crate::QueryContext) -> Expr {
    if exprs.len() == 1 {
        exprs.remove(0)
    } else {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(ctx)
    }
}

fn split_conjuncts(expr: Expr, ctx: &crate::QueryContext) -> Vec<Expr> {
    match ctx.expr(expr) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .iter()
            .flat_map(|&e| split_conjuncts(e, ctx))
            .collect(),
        _ => vec![expr],
    }
}
