use super::{binary::eval_binary_op, unary::eval_unary_op, Evaluate};
use crate::{
    capture,
    engine::{
        generator::{Continuation, Generator},
        Engine,
    },
};
use optd_dsl::analyzer::hir::{
    BinOp, CoreData, Expr, FunKind, Identifier, Literal, MatchArm, UnaryOp, Value,
};
use std::{future::Future, pin::Pin, sync::Arc};
use CoreData::*;
use FunKind::*;

/// Specialized continuation type for vectors of values
type ArgsContinuation =
    Arc<dyn Fn(Vec<Value>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

/// Evaluates a pattern match expression.
///
/// First evaluates the expression to match, then tries each match arm in order
/// until a pattern matches, passing results to the continuation.
///
/// # Parameters
/// * `expr` - The expression to match against patterns
/// * `match_arms` - The list of pattern-expression pairs to try
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_pattern_match<G>(
    expr: Arc<Expr>,
    match_arms: Vec<MatchArm>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    todo!()
}

/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true,
/// or the 'else' branch if the condition is false, passing results to the continuation.
///
/// # Parameters
/// * `cond` - The condition expression
/// * `then_expr` - The expression to evaluate if condition is true
/// * `else_expr` - The expression to evaluate if condition is false
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_if_then_else<G>(
    cond: Arc<Expr>,
    then_expr: Arc<Expr>,
    else_expr: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    // First evaluate the condition
    cond.evaluate(
        engine.clone(),
        Arc::new(move |value| {
            Box::pin(capture!([then_expr, else_expr, engine, k], async move {
                match value.0 {
                    Literal(Literal::Bool(b)) => {
                        if b {
                            then_expr.evaluate(engine, k).await;
                        } else {
                            else_expr.evaluate(engine, k).await;
                        }
                    }
                    _ => panic!("Expected boolean in condition"),
                }
            }))
        }),
    )
    .await;
}

/// Evaluates a let binding expression.
///
/// Binds the result of evaluating the assignee to the identifier in the context,
/// then evaluates the 'after' expression in the updated context, passing results
/// to the continuation.
///
/// # Parameters
/// * `ident` - The identifier to bind the value to
/// * `assignee` - The expression to evaluate and bind
/// * `after` - The expression to evaluate in the updated context
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) fn evaluate_let_binding<G>(
    ident: String,
    assignee: Arc<Expr>,
    after: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        // Evaluate the assignee first
        assignee
            .evaluate(
                engine.clone(),
                Arc::new(move |value| {
                    Box::pin(capture!([ident, after, engine, k], async move {
                        // Create updated context with the new binding
                        let mut new_ctx = engine.context.clone();
                        new_ctx.bind(ident, value);

                        // Evaluate the after expression in the updated context
                        after.evaluate(engine.with_context(new_ctx), k).await;
                    }))
                }),
            )
            .await;
    })
}

/// Evaluates a binary expression.
///
/// Evaluates both operands, then applies the binary operation,
/// passing the result to the continuation.
///
/// # Parameters
/// * `left` - The left operand
/// * `op` - The binary operator
/// * `right` - The right operand
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_binary_expr<G>(
    left: Arc<Expr>,
    op: BinOp,
    right: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    // Helper function to evaluate the right operand after the left is evaluated
    async fn evaluate_right<G>(
        left_val: Value,
        right: Arc<Expr>,
        op: BinOp,
        engine: Engine<G>,
        k: Continuation,
    ) where
        G: Generator,
    {
        right
            .evaluate(
                engine,
                Arc::new(move |right_val| {
                    Box::pin(capture!([left_val, op, k], async move {
                        // Apply the binary operation and pass result to continuation
                        let result = eval_binary_op(left_val, &op, right_val);
                        k(result).await;
                    }))
                }),
            )
            .await;
    }

    // First evaluate the left operand
    left.evaluate(
        engine.clone(),
        Arc::new(move |left_val| {
            Box::pin(capture!([right, op, engine, k], async move {
                evaluate_right(left_val, right, op, engine, k).await;
            }))
        }),
    )
    .await;
}

/// Evaluates a unary expression.
///
/// Evaluates the operand, then applies the unary operation,
/// passing the result to the continuation.
///
/// # Parameters
/// * `op` - The unary operator
/// * `expr` - The operand expression
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_unary_expr<G>(
    op: UnaryOp,
    expr: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    // Evaluate the operand, then apply the unary operation
    expr.evaluate(
        engine,
        Arc::new(move |value| {
            Box::pin(capture!([op, k], async move {
                // Apply the unary operation and pass result to continuation
                let result = eval_unary_op(&op, value);
                k(result).await;
            }))
        }),
    )
    .await;
}

/// Evaluates a function call expression.
///
/// First evaluates the function expression, then the arguments,
/// and finally applies the function to the arguments, passing results to the continuation.
///
/// # Parameters
/// * `fun` - The function expression to evaluate
/// * `args` - The argument expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_function_call<G>(
    fun: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    // First evaluate the function expression
    fun.evaluate(
        engine.clone(),
        Arc::new(move |fun_value| {
            Box::pin(capture!([args, engine, k], async move {
                match fun_value.0 {
                    // Handle closure (user-defined function)
                    Function(Closure(params, body)) => {
                        evaluate_closure_call(params, body, args, engine, k).await;
                    }
                    // Handle Rust UDF (built-in function)
                    Function(RustUDF(udf)) => {
                        evaluate_rust_udf_call(udf, args, engine, k).await;
                    }
                    // Value must be a function
                    _ => panic!("Expected function value"),
                }
            }))
        }),
    )
    .await;
}

/// Evaluates a call to a closure (user-defined function).
///
/// Evaluates the arguments, binds them to the parameters in a new context,
/// then evaluates the function body in that context, passing results to the continuation.
///
/// # Parameters
/// * `params` - The parameter names of the closure
/// * `body` - The body expression of the closure
/// * `args` - The argument expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_closure_call<G>(
    params: Vec<Identifier>,
    body: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    let length = args.len();

    // Evaluate arguments one by one, collecting results
    evaluate_args_sequentially(
        args,
        0,
        Vec::with_capacity(length),
        engine.clone(),
        Arc::new(move |arg_values| {
            Box::pin(capture!([params, body, engine, k], async move {
                // Create a new context with parameters bound to arguments
                let mut new_ctx = engine.context.clone();
                new_ctx.push_scope();

                params.iter().zip(arg_values).for_each(|(p, a)| {
                    new_ctx.bind(p.clone(), a);
                });

                // Evaluate the body in the new context
                body.evaluate(engine.with_context(new_ctx), k).await;
            }))
        }),
    )
    .await;
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments,
/// passing the result to the continuation.
///
/// # Parameters
/// * `udf` - The Rust function to call
/// * `args` - The argument expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_rust_udf_call<G>(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    let length = args.len();

    // Evaluate all arguments and then call the function
    evaluate_args_sequentially(
        args,
        0,
        Vec::with_capacity(length),
        engine,
        Arc::new(move |arg_values| {
            Box::pin(capture!([udf, k], async move {
                // Call the UDF with the argument values
                let result = udf(arg_values);

                // Pass the result to the continuation
                k(result).await;
            }))
        }),
    )
    .await;
}

/// Helper function to evaluate arguments sequentially.
///
/// Evaluates each argument in order, collecting results to pass to a continuation.
///
/// # Parameters
/// * `args` - The argument expressions to evaluate
/// * `index` - The current argument index
/// * `values` - Accumulated argument values
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive all evaluated argument values
fn evaluate_args_sequentially<G>(
    args: Vec<Arc<Expr>>,
    index: usize,
    values: Vec<Value>,
    engine: Engine<G>,
    k: ArgsContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        if index >= args.len() {
            // All arguments evaluated, call continuation directly with the vector
            k(values).await;
            return;
        }

        // Evaluate the current argument
        let arg = args[index].clone();

        arg.evaluate(
            engine.clone(),
            Arc::new(move |arg_value| {
                let mut next_values = values.clone();
                next_values.push(arg_value);

                Box::pin(capture!([args, index, engine, k], async move {
                    evaluate_args_sequentially(args, index + 1, next_values, engine, k).await;
                }))
            }),
        )
        .await;
    })
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and passes its value to the continuation.
///
/// # Parameters
/// * `ident` - The identifier to look up
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive the variable value
pub(super) async fn evaluate_reference<G>(ident: String, engine: Engine<G>, k: Continuation)
where
    G: Generator,
{
    // Look up the variable in the context
    let value = engine
        .context
        .lookup(&ident)
        .unwrap_or_else(|| panic!("Variable not found: {}", ident))
        .clone();

    // Pass the value to the continuation
    k(value).await;
}
