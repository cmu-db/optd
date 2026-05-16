//! Predicate pushdown: moves cross-relation filter predicates into join conditions.
//!
//! Rewrites `Selection(pred, Join(outer, inner, on))` where `pred` (or any of its
//! conjuncts) references only columns available from the join's inputs into
//! `Join(outer, inner, on AND pred)`, removing the `Selection` if all conjuncts
//! were pushed.

use crate::{
    AvailableColumns, Expr, ExprData, Join, JoinType, NaryOp, Operator, OperatorData,
    OptimizerContext, Selection, expr_used_columns,
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

        // Extract join parts from either a Join or CrossProduct input.
        let (join_type, existing_on, outer, inner) = match ctx.query.operator(sel.input).clone() {
            OperatorData::Join(j) => (j.join_type, Some(j.on), j.outer, j.inner),
            OperatorData::CrossProduct(cp) => (crate::JoinType::Inner, None, cp.outer, cp.inner),
            _ => return Ok(Rewrite::Keep),
        };

        // Columns available from the join's two inputs combined.
        let available: Vec<_> = ctx
            .analyses
            .get::<AvailableColumns>(&ctx.query, outer)
            .unwrap_or_default()
            .into_iter()
            .chain(
                ctx.analyses
                    .get::<AvailableColumns>(&ctx.query, inner)
                    .unwrap_or_default(),
            )
            .collect();

        let conjuncts = split_conjuncts(sel.predicate, &ctx.query);
        let mut push: Vec<Expr> = Vec::new();
        let mut keep: Vec<Expr> = Vec::new();

        for c in conjuncts {
            let used = expr_used_columns(&ctx.query, c).unwrap_or_default();
            if used.iter().all(|col| available.contains(col)) {
                push.push(c);
            } else {
                keep.push(c);
            }
        }

        if push.is_empty() {
            return Ok(Rewrite::Keep);
        }

        // Build new join ON = existing ON (if any) AND pushed conjuncts.
        let new_on = {
            let mut all_on: Vec<Expr> = existing_on.into_iter().collect();
            all_on.extend(push);
            if all_on.len() == 1 {
                all_on.remove(0)
            } else {
                ExprData::Nary {
                    op: NaryOp::And,
                    exprs: all_on,
                }
                .add(&mut ctx.query)
            }
        };

        let new_join = OperatorData::Join(crate::Join {
            join_type,
            on: new_on,
            outer,
            inner,
        })
        .add(&mut ctx.query);

        let result = if keep.is_empty() {
            new_join
        } else {
            let remaining_pred = if keep.len() == 1 {
                keep.remove(0)
            } else {
                ExprData::Nary {
                    op: NaryOp::And,
                    exprs: keep,
                }
                .add(&mut ctx.query)
            };
            OperatorData::Selection(Selection {
                predicate: remaining_pred,
                input: new_join,
            })
            .add(&mut ctx.query)
        };

        Ok(Rewrite::Replace(result))
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
