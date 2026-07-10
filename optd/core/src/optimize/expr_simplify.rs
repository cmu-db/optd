//! Expression simplification pass.
//!
//! Rewrites boolean constant-folding patterns throughout all operator expressions:
//! - `true AND e` / `e AND true` → `e`
//! - `false AND e` / `e AND false` → `false`
//! - `true OR e` / `e OR true` → `true`
//! - `false OR e` / `e OR false` → `e`
//! - `NOT(true)` → `false`, `NOT(false)` → `true`
//! - `NOT(NOT(e))` → `e`
//!
//! Also removes `Selection(true, input)` → `input`.

use crate::{
    Expr, ExprData, NaryOp, Operator, OperatorData, OptimizerContext, ScalarValue, Selection,
    UnaryOp,
    optimize::{OperatorRewrite, OptimizeResult, Pass, Rewrite},
};

pub struct ExprSimplify;

impl Pass for ExprSimplify {
    fn name(&self) -> &'static str {
        "ExprSimplify"
    }
}

impl OperatorRewrite for ExprSimplify {
    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite> {
        let data = ctx.query.operator(op).clone();
        let mut changed = false;

        let new_data = simplify_operator_exprs(data, ctx, &mut changed);

        if !changed {
            return Ok(Rewrite::Keep);
        }

        // Selection(true, input) → input
        if let OperatorData::Selection(Selection { predicate, input }) = &new_data
            && is_true(*predicate, ctx)
        {
            return Ok(Rewrite::Replace(*input));
        }

        Ok(Rewrite::Replace(new_data.add(&mut ctx.query)))
    }
}

fn is_true(expr: Expr, ctx: &OptimizerContext) -> bool {
    matches!(
        expr.get(&ctx.query),
        ExprData::Literal(ScalarValue::Boolean(true))
    )
}

fn simplify_operator_exprs(
    data: OperatorData,
    ctx: &mut OptimizerContext,
    changed: &mut bool,
) -> OperatorData {
    match data {
        OperatorData::Selection(mut s) => {
            s.predicate = simplify(s.predicate, ctx, changed);
            OperatorData::Selection(s)
        }
        OperatorData::Join(mut j) => {
            j.on = simplify(j.on, ctx, changed);
            OperatorData::Join(j)
        }
        OperatorData::Map(mut m) => {
            for (_, e) in &mut m.computations {
                *e = simplify(*e, ctx, changed);
            }
            OperatorData::Map(m)
        }
        OperatorData::Aggregation(mut a) => {
            for e in &mut a.keys {
                *e = simplify(*e, ctx, changed);
            }
            OperatorData::Aggregation(a)
        }
        OperatorData::Sort(mut s) => {
            for key in &mut s.keys {
                key.expr = simplify(key.expr, ctx, changed);
            }
            OperatorData::Sort(s)
        }
        other => other,
    }
}

/// Recursively simplifies an expression. Sets `*changed = true` if any rewrite fired.
fn simplify(expr: Expr, ctx: &mut OptimizerContext, changed: &mut bool) -> Expr {
    match expr.get(&ctx.query).clone() {
        ExprData::Unary {
            op: UnaryOp::Not,
            expr: original_inner,
        } => {
            let inner = simplify(original_inner, ctx, changed);
            match inner.get(&ctx.query).clone() {
                ExprData::Literal(ScalarValue::Boolean(b)) => {
                    *changed = true;
                    ExprData::Literal(ScalarValue::Boolean(!b)).add(&mut ctx.query)
                }
                ExprData::Unary {
                    op: UnaryOp::Not,
                    expr: double_inner,
                } => {
                    *changed = true;
                    double_inner
                }
                _ => {
                    if inner != original_inner {
                        *changed = true;
                        ExprData::Unary {
                            op: UnaryOp::Not,
                            expr: inner,
                        }
                        .add(&mut ctx.query)
                    } else {
                        expr
                    }
                }
            }
        }
        ExprData::Nary { op, exprs } => {
            let simplified: Vec<Expr> = exprs.iter().map(|&e| simplify(e, ctx, changed)).collect();
            let exprs_changed = simplified.iter().zip(exprs.iter()).any(|(a, b)| a != b);
            simplify_nary(op, simplified, exprs_changed, expr, ctx, changed)
        }
        ExprData::Binary { op, left, right } => {
            let new_left = simplify(left, ctx, changed);
            let new_right = simplify(right, ctx, changed);
            if new_left != left || new_right != right {
                *changed = true;
                ExprData::Binary {
                    op,
                    left: new_left,
                    right: new_right,
                }
                .add(&mut ctx.query)
            } else {
                expr
            }
        }
        _ => expr,
    }
}

fn simplify_nary(
    op: NaryOp,
    exprs: Vec<Expr>,
    exprs_changed: bool,
    original: Expr,
    ctx: &mut OptimizerContext,
    changed: &mut bool,
) -> Expr {
    match op {
        NaryOp::And => {
            let mut kept = Vec::new();
            let mut filtered = false;
            for e in exprs {
                match e.get(&ctx.query) {
                    ExprData::Literal(ScalarValue::Boolean(true)) => {
                        filtered = true;
                    }
                    ExprData::Literal(ScalarValue::Boolean(false)) => {
                        *changed = true;
                        return ExprData::Literal(ScalarValue::Boolean(false)).add(&mut ctx.query);
                    }
                    _ => kept.push(e),
                }
            }
            if !filtered && !exprs_changed {
                return original;
            }
            *changed = true;
            match kept.len() {
                0 => ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx.query),
                1 => kept.remove(0),
                _ => ExprData::Nary {
                    op: NaryOp::And,
                    exprs: kept,
                }
                .add(&mut ctx.query),
            }
        }
        NaryOp::Or => {
            let mut kept = Vec::new();
            let mut filtered = false;
            for e in exprs {
                match e.get(&ctx.query) {
                    ExprData::Literal(ScalarValue::Boolean(false)) => {
                        filtered = true;
                    }
                    ExprData::Literal(ScalarValue::Boolean(true)) => {
                        *changed = true;
                        return ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx.query);
                    }
                    _ => kept.push(e),
                }
            }
            if !filtered && !exprs_changed {
                return original;
            }
            *changed = true;
            match kept.len() {
                0 => ExprData::Literal(ScalarValue::Boolean(false)).add(&mut ctx.query),
                1 => kept.remove(0),
                _ => ExprData::Nary {
                    op: NaryOp::Or,
                    exprs: kept,
                }
                .add(&mut ctx.query),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColumnData, ExprData, OperatorData, OperatorRewriteAdaptor, PassManager, QueryContext,
        Scan, TableRef,
    };
    use arrow_schema::DataType;

    #[test]
    fn non_foldable_not_predicate_converges() {
        let mut query = QueryContext::new();
        let id = ColumnData::new("id", DataType::Boolean).add(&mut query);
        let id_ref = ExprData::ColumnRef(id).add(&mut query);
        let predicate = ExprData::Unary {
            op: UnaryOp::Not,
            expr: id_ref,
        }
        .add(&mut query);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![id],
        })
        .add(&mut query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut query);
        query.set_root(selection);

        let mut ctx = crate::test_optimizer_context(query);
        let mut pm = PassManager::with_max_iterations(3);
        pm.add_pass(OperatorRewriteAdaptor::new(ExprSimplify));

        pm.run(&mut ctx).unwrap();

        assert_eq!(pm.profiles().len(), 1);
        assert_eq!(pm.profiles()[0].result, Some(crate::PassResult::Unchanged));
    }
}
