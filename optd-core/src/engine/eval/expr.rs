//! This module provides implementation of expression evaluation, handling different
//! expression types and evaluation strategies in a non-blocking, streaming manner.

use super::{
    binary::eval_binary_op,
    core::evaluate_core_expr,
    r#match::{expand_top_level, try_match_arms},
    unary::eval_unary_op,
    Expander,
};
use crate::{
    capture,
    engine::{
        utils::streams::{
            evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
        },
        Engine, Evaluate,
    },
};
use futures::{stream, StreamExt};
use optd_dsl::analyzer::hir::{BinOp, CoreData, Expr, FunKind, Literal, MatchArm, UnaryOp, Value};
use std::sync::Arc;
use CoreData::*;
use Expr::*;
use FunKind::*;

impl Evaluate for Arc<Expr> {
    /// Evaluates an expression to a stream of possible values.
    ///
    /// This function takes a reference to the expression, dispatching to specialized
    /// handlers for each expression type.
    ///
    /// # Parameters
    /// * `self` - Reference to the expression to evaluate
    /// * `engine` - The evaluation engine with an expander implementation
    ///
    /// # Returns
    /// A stream of all possible evaluation results
    fn evaluate<E>(self, engine: Engine<E>) -> ValueStream
    where
        E: Expander,
    {
        match &*self {
            PatternMatch(expr, match_arms) => {
                evaluate_pattern_match(expr.clone(), match_arms.clone(), engine)
            }
            IfThenElse(cond, then_expr, else_expr) => {
                evaluate_if_then_else(cond.clone(), then_expr.clone(), else_expr.clone(), engine)
            }
            Let(ident, assignee, after) => {
                evaluate_let_binding(ident.clone(), assignee.clone(), after.clone(), engine)
            }
            Binary(left, op, right) => {
                evaluate_binary_expr(left.clone(), op.clone(), right.clone(), engine)
            }
            Unary(op, expr) => evaluate_unary_expr(op.clone(), expr.clone(), engine),
            Call(fun, args) => evaluate_function_call(fun.clone(), args.clone(), engine),
            Ref(ident) => evaluate_reference(ident.clone(), engine),
            CoreExpr(expr) => evaluate_core_expr(expr.clone(), engine),
            CoreVal(val) => propagate_success(val.clone()).boxed(),
        }
    }
}

/// Evaluates a pattern match expression.
///
/// First evaluates the expression to match, then tries each match arm in order
/// until a pattern matches.
///
/// # Parameters
/// * `expr` - The expression to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `engine` - The evaluation engine
///
/// # Returns
/// A stream of all possible evaluation results
fn evaluate_pattern_match<E>(
    expr: Arc<Expr>,
    match_arms: Vec<MatchArm>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    // First evaluate the expression
    expr.evaluate(engine.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([engine, match_arms], move |value| {
                    let expansion_future = expand_top_level(value, engine.clone());
                    stream::once(expansion_future)
                        .flat_map(move |expanded_values| {
                            // For each expanded value, try all match arms
                            stream::iter(expanded_values).flat_map(capture!(
                                [match_arms, engine],
                                move |expanded_value| {
                                    try_match_arms(
                                        expanded_value,
                                        match_arms.clone(),
                                        engine.clone(),
                                    )
                                }
                            ))
                        })
                        .boxed()
                }),
            )
        })
        .boxed()
}
/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true,
/// or the 'else' branch if the condition is false.
fn evaluate_if_then_else<E>(
    cond: Arc<Expr>,
    then_expr: Arc<Expr>,
    else_expr: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    cond.evaluate(engine.clone())
        .flat_map(move |cond_result| {
            stream_from_result(
                cond_result,
                capture!([engine, then_expr, else_expr], move |value| {
                    match value.0 {
                        // If condition is a boolean, evaluate the appropriate branch
                        Literal(Literal::Bool(b)) => {
                            if b {
                                then_expr.evaluate(engine)
                            } else {
                                else_expr.evaluate(engine)
                            }
                        }
                        // Condition must be a boolean
                        _ => panic!("Expected boolean in condition"),
                    }
                }),
            )
        })
        .boxed()
}

/// Evaluates a let binding expression.
///
/// Binds the result of evaluating the assignee to the identifier in the context,
/// then evaluates the 'after' expression in the updated context.
fn evaluate_let_binding<E>(
    ident: String,
    assignee: Arc<Expr>,
    after: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    assignee
        .evaluate(engine.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([engine, after, ident], move |value| {
                    // Create updated context with the new binding
                    let mut new_ctx = engine.context.clone();
                    new_ctx.bind(ident, value);
                    after.evaluate(engine.with_context(new_ctx))
                }),
            )
        })
        .boxed()
}

/// Evaluates a binary expression.
///
/// Evaluates both operands in all possible combinations, then applies the binary operation.
fn evaluate_binary_expr<E>(
    left: Arc<Expr>,
    op: BinOp,
    right: Arc<Expr>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    let exprs = vec![left, right];
    evaluate_all_combinations(exprs.into_iter(), engine)
        .map(move |combo_result| {
            combo_result.map(|mut values| {
                let right_val = values.pop().expect("Right operand not found");
                let left_val = values.pop().expect("Left operand not found");
                eval_binary_op(left_val, &op, right_val)
            })
        })
        .boxed()
}

/// Evaluates a unary expression.
///
/// Evaluates the operand, then applies the unary operation.
fn evaluate_unary_expr<E>(op: UnaryOp, expr: Arc<Expr>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    expr.evaluate(engine)
        .map(move |expr_result| expr_result.map(|value| eval_unary_op(&op, value)))
        .boxed()
}

/// Evaluates a function call expression.
///
/// First evaluates the function expression, then the arguments,
/// and finally applies the function to the arguments.
fn evaluate_function_call<E>(fun: Arc<Expr>, args: Vec<Arc<Expr>>, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    let fun_stream = fun.evaluate(engine.clone());

    fun_stream
        .flat_map(move |fun_result| {
            stream_from_result(
                fun_result,
                capture!([engine, args], move |fun_value| {
                    match fun_value.0 {
                        // Handle closure (user-defined function)
                        Function(Closure(params, body)) => {
                            evaluate_closure_call(params, body, args.clone(), engine)
                        }
                        // Handle Rust UDF (built-in function)
                        Function(RustUDF(udf)) => evaluate_rust_udf_call(udf, args, engine),
                        // Value must be a function
                        _ => panic!("Expected function value"),
                    }
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a closure (user-defined function).
///
/// Evaluates the arguments, binds them to the parameters in a new context,
/// then evaluates the function body in that context.
fn evaluate_closure_call<E>(
    params: Vec<String>,
    body: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    evaluate_all_combinations(args.into_iter(), engine.clone())
        .flat_map(move |args_result| {
            stream_from_result(
                args_result,
                capture!([engine, params, body], move |args| {
                    // Create a new context with parameters bound to arguments
                    let mut new_ctx = engine.context.clone();
                    new_ctx.push_scope();
                    params.iter().zip(args).for_each(|(p, a)| {
                        new_ctx.bind(p.clone(), a);
                    });
                    body.evaluate(engine.with_context(new_ctx))
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments.
fn evaluate_rust_udf_call<E>(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Arc<Expr>>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    evaluate_all_combinations(args.into_iter(), engine)
        .map(move |args_result| args_result.map(udf))
        .boxed()
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and returns its value.
fn evaluate_reference<E>(ident: String, engine: Engine<E>) -> ValueStream
where
    E: Expander,
{
    propagate_success(
        engine
            .context
            .lookup(&ident)
            .expect("Variable not found")
            .clone(),
    )
    .boxed()
}
