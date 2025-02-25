use crate::{
    analyzer::hir::{CoreData, Expr, FunKind, Literal, Value},
    utils::context::Context,
};
use anyhow::Error;
use async_recursion::async_recursion;
use core_eval::evaluate_core_expr;
use futures::{future::try_join_all, stream, StreamExt};
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
    context: Context,
) -> Result<Vec<Vec<Value>>, Error>
where
    I: Iterator<Item = &'a Expr>,
{
    let result = stream::iter(items)
        .fold(
            Ok(vec![vec![]]),
            |acc_result: Result<_, Error>, item| async {
                let acc = acc_result?;
                let item_values = item.evaluate(context.clone()).await?;

                let new_combinations = acc
                    .into_iter()
                    .flat_map(|current_combination| {
                        item_values.iter().map(move |value| {
                            let mut new_combination = current_combination.clone();
                            new_combination.push(value.clone());
                            new_combination
                        })
                    })
                    .collect();

                Ok(new_combinations)
            },
        )
        .await?;

    Ok(result)
}

impl Expr {
    #[async_recursion]
    pub(super) async fn evaluate(&self, context: Context) -> Result<Vec<Value>, Error> {
        match self {
            PatternMatch(expr, match_arms) => {
                let expr_values = expr.evaluate(context.clone()).await?;
                let result_futures = expr_values.iter().map(|value| {
                    let ctx_clone = context.clone();
                    let match_arms = match_arms.clone();
                    async move {
                        for arm in match_arms {
                            let matched_contexts =
                                match_pattern(value, &arm.pattern, ctx_clone.clone()).await?;

                            if !matched_contexts.is_empty() {
                                let result_futures = matched_contexts
                                    .into_iter()
                                    .map(|match_ctx| arm.expr.evaluate(match_ctx));

                                let results = try_join_all(result_futures).await?;
                                return Ok::<_, Error>(
                                    results.into_iter().flatten().collect::<Vec<_>>(),
                                );
                            }
                        }

                        panic!("No pattern matched for value: {:?}", value);
                    }
                });

                let results: Vec<_> = try_join_all(result_futures).await?;
                Ok(results.into_iter().flatten().collect())
            }

            IfThenElse(cond, then_expr, else_expr) => {
                let cond_values = cond.evaluate(context.clone()).await?;

                let branch_result_futures = cond_values.into_iter().map(|cond_value| {
                    let ctx = context.clone();
                    async move {
                        match cond_value.0 {
                            Literal(Literal::Bool(b)) => {
                                if b {
                                    then_expr.evaluate(ctx.clone()).await
                                } else {
                                    else_expr.evaluate(ctx).await
                                }
                            }
                            _ => panic!("Expected boolean in condition"),
                        }
                    }
                });

                let branch_results = try_join_all(branch_result_futures).await?;
                Ok(branch_results.into_iter().flatten().collect())
            }

            Let(ident, expr, body) => {
                let expr_values = expr.evaluate(context.clone()).await?;

                let body_futures = expr_values.into_iter().map(|value| {
                    let mut new_ctx = context.clone();
                    new_ctx.bind(ident.to_string(), value);
                    body.evaluate(new_ctx)
                });

                let body_results = try_join_all(body_futures).await?;
                Ok(body_results.into_iter().flatten().collect())
            }

            Binary(left, op, right) => {
                let left_values = left.evaluate(context.clone()).await?;
                let right_values = right.evaluate(context.clone()).await?;

                let results = left_values
                    .iter()
                    .flat_map(|l| {
                        right_values
                            .iter()
                            .map(move |r| eval_binary_op(l.clone(), op, r.clone()))
                    })
                    .collect();

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
                let fun_values = fun.evaluate(context.clone()).await?;
                let arg_combinations =
                    evaluate_all_combinations(args.iter(), context.clone()).await?;

                let mut all_results = Vec::new();

                for fun_value in fun_values {
                    match &fun_value.0 {
                        Function(Closure(params, body)) => {
                            let mut futures = Vec::new();

                            for args in arg_combinations.clone() {
                                let context_clone = context.clone();

                                let future = async move {
                                    let mut new_ctx = context_clone;
                                    new_ctx.push_scope();
                                    params.iter().zip(args).for_each(|(p, a)| {
                                        new_ctx.bind(p.clone(), a.clone());
                                    });
                                    body.evaluate(new_ctx).await
                                };

                                futures.push(future);
                            }

                            let results = try_join_all(futures).await?;
                            all_results.extend(results.into_iter().flatten());
                        }
                        Function(RustUDF(udf)) => {
                            for args in arg_combinations.clone() {
                                let result = udf(args);
                                all_results.push(result);
                            }
                        }
                        _ => panic!("Expected function value"),
                    }
                }

                Ok(all_results)
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
