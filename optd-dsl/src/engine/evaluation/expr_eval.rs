use crate::{
    analyzer::hir::{CoreData, Expr, FunKind, Literal, Value},
    engine::errors::EngineError,
    utils::context::Context,
};
use futures::stream;
use futures::{FutureExt, StreamExt};

use CoreData::*;
use Expr::*;
use FunKind::*;

use super::{
    core_eval::evaluate_core_expr,
    op_eval::{eval_binary_op, eval_unary_op},
    stream_cache::StreamCache,
    ValueStream, VecValueStream,
};

pub(super) fn propagate_error(e: EngineError) -> ValueStream {
    Box::new(stream::once(async move { Err(e) }).boxed())
}

pub(super) fn propagate_value(value: Value) -> ValueStream {
    Box::new(stream::once(async move { Ok(value) }).boxed())
}

/// Generates a stream of all possible value combinations from a series of expressions.
///
/// This function computes the cartesian product of all values each expression can
/// evaluate to. For expressions with multiple possible values, it creates all combinations
/// while preserving the original ordering.
///
/// # Example
/// Given expressions that evaluate to:
/// - expr1 -> [A, B]
/// - expr2 -> [X, Y]
///
/// The resulting stream produces: [A, X], [A, Y], [B, X], [B, Y]
///
/// # Implementation
/// - Uses recursion with caching to avoid redundant evaluations
/// - Handles errors by propagating the first encountered error
/// - Processes values lazily for stream compatibility
///
/// Particularly useful for:
/// - Function arguments with multiple possible values
/// - Collection literals with non-deterministic elements
/// - Struct fields that may have multiple possible values
pub(super) fn evaluate_all_combinations<I>(mut items: I, context: Context) -> VecValueStream
where
    I: Iterator<Item = Expr> + Send + Clone,
{
    Box::new(match items.next() {
        None => stream::once(async { Ok(vec![]) }).boxed(),
        Some(first) => {
            let has_more = items.next().is_some();
            if !has_more {
                first
                    .evaluate(context)
                    .map(|result| result.map(|val| vec![val]))
                    .boxed()
            } else {
                let remaining_items = items.collect::<Vec<_>>();
                let cache = StreamCache::new();

                first
                    .evaluate(context.clone())
                    .flat_map_unordered(None, {
                        let cache = cache.clone();
                        move |first_result| match first_result {
                            Ok(first_val) => {
                                let cache = cache.clone();
                                let remaining_items = remaining_items.clone();
                                let context_clone = context.clone();

                                async move {
                                    let rest_results = cache
                                        .get_or_compute(|| {
                                            evaluate_all_combinations(
                                                remaining_items.into_iter(),
                                                context_clone,
                                            )
                                            .collect::<Vec<_>>()
                                        })
                                        .await;

                                    stream::iter(rest_results).map(move |rest_result| {
                                        rest_result.map(|rest_vals| {
                                            let mut result = vec![first_val.clone()];
                                            result.extend(rest_vals);
                                            result
                                        })
                                    })
                                }
                                .flatten_stream()
                                .boxed()
                            }
                            Err(e) => stream::once(async move { Err(e) }).boxed(),
                        }
                    })
                    .boxed()
            }
        }
    })
}

impl Expr {
    pub(crate) fn evaluate(&self, context: Context) -> ValueStream {
        match self {
            PatternMatch(_expr, _match_arms) => todo!(),

            IfThenElse(cond, then_expr, else_expr) => {
                let then_expr = then_expr.clone();
                let else_expr = else_expr.clone();

                let stream =
                    cond.evaluate(context.clone())
                        .flat_map_unordered(None, move |cond_result| {
                            let context = context.clone();

                            match cond_result {
                                Ok(value) => match value.0 {
                                    Literal(Literal::Bool(b)) => {
                                        if b {
                                            then_expr.evaluate(context)
                                        } else {
                                            else_expr.evaluate(context)
                                        }
                                    }
                                    _ => panic!("Expected boolean in condition"),
                                },
                                Err(e) => propagate_error(e),
                            }
                        });

                Box::new(stream)
            }

            Let(ident, assignee, after) => {
                let ident = ident.clone();
                let after = after.clone();

                let stream = assignee.evaluate(context.clone()).flat_map_unordered(
                    None,
                    move |expr_result| {
                        let context = context.clone();
                        let ident = ident.clone();

                        match expr_result {
                            Ok(value) => {
                                let mut new_ctx = context;
                                new_ctx.bind(ident, value);
                                after.evaluate(new_ctx)
                            }
                            Err(e) => propagate_error(e),
                        }
                    },
                );

                Box::new(stream)
            }

            Binary(left, op, right) => {
                let op = op.clone();

                let stream = evaluate_all_combinations(
                    [left.clone(), right.clone()].into_iter().map(|b| *b),
                    context.clone(),
                )
                .map(move |combo_result| {
                    combo_result.map(|mut values| {
                        let right_val = values.pop().expect("Right operand not found");
                        let left_val = values.pop().expect("Left operand not found");
                        eval_binary_op(left_val, &op, right_val)
                    })
                });

                Box::new(stream)
            }

            Unary(op, expr) => {
                let op = op.clone();
                let stream = expr
                    .evaluate(context.clone())
                    .map(move |expr_result| expr_result.map(|value| eval_unary_op(&op, value)))
                    .boxed();

                Box::new(stream)
            }

            Call(fun, args) => {
                let fun_stream = fun.evaluate(context.clone());
                let args = args.clone();

                let stream = fun_stream.flat_map_unordered(None, move |fun_result| {
                    let context = context.clone();

                    match fun_result {
                        Ok(fun_value) => match fun_value.0 {
                            Function(Closure(params, body)) => evaluate_all_combinations(
                                args.iter().map(|arg| (*arg).clone()),
                                context.clone(),
                            )
                            .flat_map_unordered(None, move |args_result| {
                                let context = context.clone();
                                match args_result {
                                    Ok(args) => {
                                        let mut new_ctx = context;
                                        new_ctx.push_scope();
                                        params.iter().zip(args).for_each(|(p, a)| {
                                            new_ctx.bind(p.clone(), a);
                                        });
                                        body.evaluate(new_ctx)
                                    }
                                    Err(e) => propagate_error(e),
                                }
                            })
                            .boxed(),
                            Function(RustUDF(udf)) => evaluate_all_combinations(
                                args.iter().map(|arg| (*arg).clone()),
                                context,
                            )
                            .map(move |args_result| args_result.map(|args| udf(args)))
                            .boxed(),
                            _ => panic!("Expected function value"),
                        },
                        Err(e) => stream::once(async move { Err(e) }).boxed(),
                    }
                });

                Box::new(stream)
            }

            Ref(ident) => Box::new(propagate_value(
                context.lookup(ident).expect("Variable not found").clone(),
            )),

            CoreExpr(expr) => evaluate_core_expr(expr, context),

            CoreVal(val) => Box::new(propagate_value(val.clone())),
        }
    }
}
