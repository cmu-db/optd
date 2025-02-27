//! This module provides implementation of expression evaluation, handling different
//! expression types and evaluation strategies in a non-blocking, streaming manner.

use crate::{
    analyzer::hir::{BinOp, CoreData, Expr, FunKind, Literal, MatchArm, UnaryOp, Value},
    capture,
    engine::utils::streams::{
        evaluate_all_combinations, propagate_success, stream_from_result, ValueStream,
    },
    utils::context::Context,
};
use futures::StreamExt;

use CoreData::*;
use Expr::*;
use FunKind::*;

use super::{
    binary::eval_binary_op, core::evaluate_core_expr, r#match::try_match_arms, unary::eval_unary_op,
};

impl Expr {
    /// Evaluates an expression to a stream of possible values.
    ///
    /// This function consumes the expression, dispatching to specialized
    /// handlers for each expression type.
    ///
    /// # Parameters
    /// * `self` - The expression to evaluate (consumed)
    /// * `context` - The evaluation context containing variable bindings
    ///
    /// # Returns
    /// A stream of all possible evaluation results
    pub(crate) fn evaluate(self, context: Context) -> ValueStream {
        match self {
            PatternMatch(expr, match_arms) => evaluate_pattern_match(*expr, match_arms, context),
            IfThenElse(cond, then_expr, else_expr) => {
                evaluate_if_then_else(*cond, *then_expr, *else_expr, context)
            }
            Let(ident, assignee, after) => evaluate_let_binding(ident, *assignee, *after, context),
            Binary(left, op, right) => evaluate_binary_expr(*left, op, *right, context),
            Unary(op, expr) => evaluate_unary_expr(op, *expr, context),
            Call(fun, args) => evaluate_function_call(*fun, args, context),
            Ref(ident) => evaluate_reference(ident, context),
            CoreExpr(expr) => evaluate_core_expr(expr, context),
            CoreVal(val) => propagate_success(val).boxed(),
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
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of all possible evaluation results
pub(super) fn evaluate_pattern_match(
    expr: Expr,
    match_arms: Vec<MatchArm>,
    context: Context,
) -> ValueStream {
    // First evaluate the expression
    expr.evaluate(context.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([context, match_arms], move |value| {
                    // Try each match arm in sequence
                    try_match_arms(value, match_arms, context)
                }),
            )
        })
        .boxed()
}

/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true,
/// or the 'else' branch if the condition is false.
fn evaluate_if_then_else(
    cond: Expr,
    then_expr: Expr,
    else_expr: Expr,
    context: Context,
) -> ValueStream {
    cond.evaluate(context.clone())
        .flat_map(move |cond_result| {
            stream_from_result(
                cond_result,
                capture!([context, then_expr, else_expr], move |value| {
                    match value.0 {
                        // If condition is a boolean, evaluate the appropriate branch
                        Literal(Literal::Bool(b)) => {
                            if b {
                                then_expr.evaluate(context)
                            } else {
                                else_expr.evaluate(context)
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
fn evaluate_let_binding(
    ident: String,
    assignee: Expr,
    after: Expr,
    context: Context,
) -> ValueStream {
    assignee
        .evaluate(context.clone())
        .flat_map(move |expr_result| {
            stream_from_result(
                expr_result,
                capture!([context, after, ident], move |value| {
                    // Create updated context with the new binding
                    let mut new_ctx = context.clone();
                    new_ctx.bind(ident, value);
                    after.evaluate(new_ctx)
                }),
            )
        })
        .boxed()
}

/// Evaluates a binary expression.
///
/// Evaluates both operands in all possible combinations, then applies the binary operation.
fn evaluate_binary_expr(left: Expr, op: BinOp, right: Expr, context: Context) -> ValueStream {
    evaluate_all_combinations(vec![left, right].into_iter(), context)
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
fn evaluate_unary_expr(op: UnaryOp, expr: Expr, context: Context) -> ValueStream {
    expr.evaluate(context)
        .map(move |expr_result| expr_result.map(|value| eval_unary_op(&op, value)))
        .boxed()
}

/// Evaluates a function call expression.
///
/// First evaluates the function expression, then the arguments,
/// and finally applies the function to the arguments.
fn evaluate_function_call(fun: Expr, args: Vec<Expr>, context: Context) -> ValueStream {
    let fun_stream = fun.evaluate(context.clone());

    fun_stream
        .flat_map(move |fun_result| {
            stream_from_result(
                fun_result,
                capture!([context, args], move |fun_value| {
                    match fun_value.0 {
                        // Handle closure (user-defined function)
                        Function(Closure(params, body)) => {
                            evaluate_closure_call(params, body, args.clone(), context.clone())
                        }
                        // Handle Rust UDF (built-in function)
                        Function(RustUDF(udf)) => evaluate_rust_udf_call(udf, args, context),
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
fn evaluate_closure_call(
    params: Vec<String>,
    body: Box<Expr>,
    args: Vec<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(args.into_iter(), context.clone())
        .flat_map(move |args_result| {
            stream_from_result(
                args_result,
                capture!([context, params, body], move |args| {
                    // Create a new context with parameters bound to arguments
                    let mut new_ctx = context;
                    new_ctx.push_scope();
                    params.iter().zip(args).for_each(|(p, a)| {
                        new_ctx.bind(p.clone(), a);
                    });
                    (*body).evaluate(new_ctx)
                }),
            )
        })
        .boxed()
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments.
fn evaluate_rust_udf_call(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(args.into_iter(), context)
        .map(move |args_result| args_result.map(udf))
        .boxed()
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and returns its value.
fn evaluate_reference(ident: String, context: Context) -> ValueStream {
    propagate_success(context.lookup(&ident).expect("Variable not found").clone()).boxed()
}
