use super::context::Context;
use crate::analyzer::hir::{CoreData, Expr, FunKind, Literal, Value};
use core_eval::evaluate_core_expr;
use op_eval::{eval_binary_op, eval_unary_op};
use optd_core::cascades::memo::Memoize;

use CoreData::*;
use Expr::*;
use FunKind::*;

mod core_eval;
mod op_eval;

/// Evaluates a collection of expressions to generate all possible combinations of their values.
///
/// This function performs a cartesian product of all possible values that each expression
/// can evaluate to. For example, if we have expressions that evaluate to:
/// - expr1 -> [A, B]
/// - expr2 -> [X, Y]
/// The result will be: [[A, X], [A, Y], [B, X], [B, Y]]
///
/// This is particularly useful for:
/// 1. Function arguments where each argument might evaluate to multiple values
/// 2. Collection literals (arrays, tuples) where each element might be non-deterministic
/// 3. Struct fields where each field might have multiple possible values
pub(super) fn evaluate_all_combinations<'a, I, M>(
    items: I,
    context: &mut Context,
    memo: &M,
) -> Vec<Vec<Value>>
where
    I: Iterator<Item = &'a Expr>,
    M: Memoize,
{
    items.fold(vec![vec![]], |acc, item| {
        acc.into_iter()
            .flat_map(|current| {
                item.evaluate(context, memo).into_iter().map(move |value| {
                    let mut new_items = current.clone();
                    new_items.push(value);
                    new_items
                })
            })
            .collect()
    })
}

impl Expr {
    pub(super) fn evaluate<M: Memoize>(&self, context: &mut Context, memo: &M) -> Vec<Value> {
        match self {
            PatternMatch(_expr, _match_arms) => todo!(),

            IfThenElse(cond, then_expr, else_expr) => cond
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|cond| match cond.0 {
                    Literal(Literal::Bool(b)) => {
                        if b { then_expr } else { else_expr }.evaluate(context, memo)
                    }
                    _ => panic!("Expected boolean in condition"),
                })
                .collect(),

            Let(ident, expr, body) => expr
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|value| {
                    let mut new_ctx = context.clone();
                    new_ctx.bind(ident.to_string(), value);
                    body.evaluate(&mut new_ctx, memo)
                })
                .collect(),

            Binary(left, op, right) => left
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|l| {
                    right
                        .evaluate(context, memo)
                        .into_iter()
                        .map(move |r| eval_binary_op(l.clone(), op, r))
                })
                .collect(),

            Unary(op, expr) => expr
                .evaluate(context, memo)
                .into_iter()
                .map(|e| eval_unary_op(op, e))
                .collect(),

            Call(fun, args) => {
                let fun_values = fun.evaluate(context, memo);
                let arg_combinations = evaluate_all_combinations(args.iter(), context, memo);

                fun_values
                    .into_iter()
                    .flat_map(|fun| match &fun.0 {
                        Function(Closure(params, body)) => arg_combinations
                            .iter()
                            .flat_map(|args| {
                                let mut new_ctx = context.clone();
                                new_ctx.push_scope();
                                params.iter().zip(args).for_each(|(p, a)| {
                                    new_ctx.bind(p.clone(), a.clone());
                                });
                                body.evaluate(&mut new_ctx, memo)
                            })
                            .collect::<Vec<_>>(),
                        Function(RustUDF(udf)) => arg_combinations
                            .iter()
                            .map(|args| udf(args.clone()))
                            .collect(),
                        _ => panic!("Expected function value"),
                    })
                    .collect()
            }

            Ref(ident) => vec![context.lookup(ident).expect("Variable not found").clone()],
            CoreExpr(expr) => evaluate_core_expr(expr, context, memo),
            CoreVal(val) => vec![val.clone()],
        }
    }
}
