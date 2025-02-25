use crate::{
    analyzer::hir::{CoreData, Expr, FunKind, Literal, Value},
    utils::context::Context,
};
use anyhow::Error;
use async_recursion::async_recursion;
use core_eval::evaluate_core_expr;
use futures::future::try_join_all;
use op_eval::{eval_binary_op, eval_unary_op};

use pat_eval::match_pattern;
use CoreData::*;
use Expr::*;
use FunKind::*;
mod core_eval;
mod op_eval;
mod pat_eval;

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
pub(super) async fn evaluate_all_combinations<'a, I>(
    items: I,
    context: &mut Context,
) -> Result<Vec<Vec<Value>>, Error>
where
    I: Iterator<Item = &'a Expr>,
{
    let mut result = vec![vec![]];

    for item in items {
        let item_values = item.evaluate(context).await?;
        let mut new_result = Vec::new();

        for current in result {
            for value in &item_values {
                let mut new_items = current.clone();
                new_items.push(value.clone());
                new_result.push(new_items);
            }
        }

        result = new_result;
    }

    Ok(result)
}

impl Expr {
    // TODO: Handle the context copying better. Right now it is a bit inefficient and
    // hurts parallelization. There are many places where we could try_join_all.
    // Probably making the context owned is a good approach, and slightly change the Context API.
    #[async_recursion]
    pub(super) async fn evaluate(&self, context: &mut Context) -> Result<Vec<Value>, Error> {
        match self {
            PatternMatch(expr, match_arms) => {
                let expr_values = expr.evaluate(context).await?;
                let result_futures = expr_values.iter().map(|value| {
                    let ctx_clone = context.clone();
                    async move {
                        for arm in match_arms {
                            let matched_contexts =
                                match_pattern(value, &arm.pattern, ctx_clone.clone()).await?;

                            if !matched_contexts.is_empty() {
                                let mut all_results = Vec::new();

                                for mut match_ctx in matched_contexts {
                                    let results = arm.expr.evaluate(&mut match_ctx).await?;
                                    all_results.extend(results);
                                }

                                return Ok::<_, Error>(all_results);
                            }
                        }

                        panic!("No pattern matched for value: {:?}", value);
                    }
                });

                let results = try_join_all(result_futures).await?;
                Ok(results.into_iter().flatten().collect())
            }

            IfThenElse(cond, then_expr, else_expr) => {
                let cond_values = cond.evaluate(context).await?;
                let mut results = Vec::new();

                for cond in cond_values {
                    match cond.0 {
                        Literal(Literal::Bool(b)) => {
                            let branch_results = if b {
                                then_expr.evaluate(context).await?
                            } else {
                                else_expr.evaluate(context).await?
                            };
                            results.extend(branch_results);
                        }
                        _ => panic!("Expected boolean in condition"),
                    }
                }

                Ok(results)
            }

            Let(ident, expr, body) => {
                let expr_values = expr.evaluate(context).await?;
                let mut results = Vec::new();

                for value in expr_values {
                    let mut new_ctx = context.clone();
                    new_ctx.bind(ident.to_string(), value);
                    let body_results = body.evaluate(&mut new_ctx).await?;
                    results.extend(body_results);
                }

                Ok(results)
            }

            Binary(left, op, right) => {
                let left_values = left.evaluate(context).await?;
                let right_values = right.evaluate(context).await?;
                let mut results = Vec::new();

                for l in &left_values {
                    for r in &right_values {
                        results.push(eval_binary_op(l.clone(), op, r.clone()));
                    }
                }

                Ok(results)
            }

            Unary(op, expr) => {
                let expr_values = expr.evaluate(context).await?;
                let results = expr_values
                    .into_iter()
                    .map(|e| eval_unary_op(op, e))
                    .collect();

                Ok(results)
            }

            Call(fun, args) => {
                let fun_values = fun.evaluate(context).await?;
                let arg_combinations = evaluate_all_combinations(args.iter(), context).await?;
                let mut results = Vec::new();

                for fun in fun_values {
                    match &fun.0 {
                        Function(Closure(params, body)) => {
                            for args in &arg_combinations {
                                let mut new_ctx = context.clone();
                                new_ctx.push_scope();
                                params.iter().zip(args).for_each(|(p, a)| {
                                    new_ctx.bind(p.clone(), a.clone());
                                });
                                let body_results = body.evaluate(&mut new_ctx).await?;
                                results.extend(body_results);
                            }
                        }
                        Function(RustUDF(udf)) => {
                            for args in &arg_combinations {
                                results.push(udf(args.clone()));
                            }
                        }
                        _ => panic!("Expected function value"),
                    }
                }

                Ok(results)
            }

            Ref(ident) => Ok(vec![context
                .lookup(ident)
                .expect("Variable not found")
                .clone()]),

            CoreExpr(expr) => evaluate_core_expr(expr, context).await,

            CoreVal(val) => Ok(vec![val.clone()]),
        }
    }
}
