//! Predicate pushdown: moves filter predicates into join conditions or onto inputs.
//!
//! For `Selection(pred, Join/CrossProduct(outer, inner))`, splits `pred` into conjuncts
//! and routes each one when that movement preserves the join's NULL-extension semantics:
//! - refs only preserved-side columns → `Selection` on that input
//! - refs columns from both sides of an inner join → join `ON` condition
//! - refs columns from a NULL-supplying side, a full outer join side, or outside this join
//!   → stays in the `Selection` above

use crate::{
    AvailableColumns, Expr, ExprData, JoinType, NaryOp, Operator, OperatorData, OptimizerContext,
    ScalarValue, Selection, expr_used_columns,
    optimize::{OperatorRewrite, OptimizeResult, Pass, Rewrite},
};

pub struct PredicatePushdown;

impl Pass for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }
}

impl OperatorRewrite for PredicatePushdown {
    fn direction(&self) -> crate::optimize::Direction {
        crate::optimize::Direction::TopDown
    }

    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite> {
        let OperatorData::Selection(sel) = ctx.query.operator(op).clone() else {
            return Ok(Rewrite::Keep);
        };

        let (join_type, existing_on, outer, inner, was_cross_product) =
            match ctx.query.operator(sel.input).clone() {
                OperatorData::Join(j) => (j.join_type, Some(j.on), j.outer, j.inner, false),
                OperatorData::CrossProduct(cp) => {
                    (crate::JoinType::Inner, None, cp.outer, cp.inner, true)
                }
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
            let refs_outer = used.iter().all(|col| outer_cols.contains(col));
            let refs_inner = used.iter().all(|col| inner_cols.contains(col));
            let refs_join_inputs = used
                .iter()
                .all(|col| outer_cols.contains(col) || inner_cols.contains(col));

            if refs_outer && can_push_outer(&join_type) {
                push_outer.push(c);
            } else if refs_inner && can_push_inner(&join_type) {
                push_inner.push(c);
            } else if refs_join_inputs && can_push_into_join(&join_type) {
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

        let new_join_input = if was_cross_product && push_join.is_empty() {
            OperatorData::CrossProduct(crate::CrossProduct {
                outer: new_outer,
                inner: new_inner,
            })
            .add(&mut ctx.query)
        } else {
            let mut all_on: Vec<Expr> = existing_on.into_iter().collect();
            all_on.extend(push_join);
            let new_on = match all_on.len() {
                0 => ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx.query),
                1 => all_on.remove(0),
                _ => ExprData::Nary {
                    op: NaryOp::And,
                    exprs: all_on,
                }
                .add(&mut ctx.query),
            };

            OperatorData::Join(crate::Join {
                join_type,
                on: new_on,
                outer: new_outer,
                inner: new_inner,
            })
            .add(&mut ctx.query)
        };

        let result = if keep.is_empty() {
            new_join_input
        } else {
            let pred = make_and(keep, &mut ctx.query);
            OperatorData::Selection(Selection {
                predicate: pred,
                input: new_join_input,
            })
            .add(&mut ctx.query)
        };

        Ok(Rewrite::Replace(result))
    }
}

fn can_push_outer(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftOuter
            | JoinType::Single
            | JoinType::LeftMark { .. }
    )
}

fn can_push_inner(join_type: &JoinType) -> bool {
    matches!(join_type, JoinType::Inner | JoinType::RightOuter)
}

fn can_push_into_join(join_type: &JoinType) -> bool {
    matches!(join_type, JoinType::Inner)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryOp, ColumnData, CrossProduct, OperatorRewriteAdaptor, PassManager, QueryContext,
        Scan, TableRef,
    };
    use arrow_schema::DataType;

    #[test]
    fn filtered_cross_product_stays_cross_product_without_join_predicate() {
        let mut query = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut query);
        let b = ColumnData::new("b", DataType::Int64).add(&mut query);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut query);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut query);
        let cross = OperatorData::CrossProduct(CrossProduct {
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Gt,
            left: ExprData::ColumnRef(a).add(&mut query),
            right: ExprData::Literal(ScalarValue::Int64(1)).add(&mut query),
        }
        .add(&mut query);
        let root = OperatorData::Selection(Selection {
            predicate,
            input: cross,
        })
        .add(&mut query);
        query.set_root(root);

        let mut opt = OptimizerContext::new(query);
        let mut pm = PassManager::new();
        pm.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
        pm.run(&mut opt).expect("predicate pushdown should succeed");

        let root = opt.query.root().expect("root should be set");
        let OperatorData::CrossProduct(cross) = opt.query.operator(root) else {
            panic!("expected cross product root");
        };
        assert!(matches!(
            opt.query.operator(cross.outer),
            OperatorData::Selection(_)
        ));
        assert_eq!(cross.inner, scan_b);
    }
}
