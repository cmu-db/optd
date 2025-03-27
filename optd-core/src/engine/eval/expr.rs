use super::{Evaluate, binary::eval_binary_op, unary::eval_unary_op};
use crate::{
    capture,
    engine::{Continuation, Engine, Generator, utils::evaluate_sequence},
};
use CoreData::*;
use FunKind::*;
use optd_dsl::analyzer::hir::{
    BinOp, CoreData, Expr, FunKind, Identifier, Literal, UnaryOp, Value,
};
use std::sync::Arc;

/// Evaluates an if-then-else expression.
///
/// First evaluates the condition, then either the 'then' branch if the condition is true, or the
/// 'else' branch if the condition is false, passing results to the continuation.
///
/// # Parameters
///
/// * `cond` - The condition expression.
/// * `then_expr` - The expression to evaluate if condition is true.
/// * `else_expr` - The expression to evaluate if condition is false.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(super) async fn evaluate_if_then_else<G>(
    cond: Arc<Expr>,
    then_expr: Arc<Expr>,
    else_expr: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation<Value>,
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
/// Binds the result of evaluating the assignee to the identifier in the context, then evaluates the
/// 'after' expression in the updated context, passing results to the continuation.
///
/// # Parameters
///
/// * `ident` - The identifier to bind the value to.
/// * `assignee` - The expression to evaluate and bind.
/// * `after` - The expression to evaluate in the updated context.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(super) async fn evaluate_let_binding<G>(
    ident: String,
    assignee: Arc<Expr>,
    after: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation<Value>,
) where
    G: Generator,
{
    // Evaluate the assignee first.
    assignee
        .evaluate(
            engine.clone(),
            Arc::new(move |value| {
                Box::pin(capture!([ident, after, engine, k], async move {
                    // Create updated context with the new binding.
                    let mut new_ctx = engine.context.clone();
                    new_ctx.bind(ident, value);

                    // Evaluate the after expression in the updated context.
                    after.evaluate(engine.with_new_context(new_ctx), k).await;
                }))
            }),
        )
        .await;
}

/// Evaluates a binary expression.
///
/// Evaluates both operands, then applies the binary operation, passing the result to the
/// continuation.
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
    k: Continuation<Value>,
) where
    G: Generator,
{
    // Helper function to evaluate the right operand after the left is evaluated.
    async fn evaluate_right<G>(
        left_val: Value,
        right: Arc<Expr>,
        op: BinOp,
        engine: Engine<G>,
        k: Continuation<Value>,
    ) where
        G: Generator,
    {
        right
            .evaluate(
                engine,
                Arc::new(move |right_val| {
                    Box::pin(capture!([left_val, op, k], async move {
                        // Apply the binary operation and pass result to continuation.
                        let result = eval_binary_op(left_val, &op, right_val);
                        k(result).await;
                    }))
                }),
            )
            .await;
    }

    // First evaluate the left operand.
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
/// Evaluates the operand, then applies the unary operation, passing the result to the continuation.
///
/// # Parameters
///
/// * `op` - The unary operator.
/// * `expr` - The operand expression.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(super) async fn evaluate_unary_expr<G>(
    op: UnaryOp,
    expr: Arc<Expr>,
    engine: Engine<G>,
    k: Continuation<Value>,
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
/// First evaluates the function expression, then the arguments, and finally applies the function to
/// the arguments, passing results to the continuation.
///
/// # Parameters
///
/// * `fun` - The function expression to evaluate.
/// * `args` - The argument expressions to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(super) async fn evaluate_function_call<G>(
    fun: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation<Value>,
) where
    G: Generator,
{
    // First evaluate the function expression.
    fun.evaluate(
        engine.clone(),
        Arc::new(move |fun_value| {
            Box::pin(capture!([args, engine, k], async move {
                match fun_value.0 {
                    // Handle closure (user-defined function).
                    Function(Closure(params, body)) => {
                        evaluate_closure_call(params, body, args, engine, k).await;
                    }
                    // Handle Rust UDF (built-in function).
                    Function(RustUDF(udf)) => {
                        evaluate_rust_udf_call(udf, args, engine, k).await;
                    }
                    // Value must be a function.
                    _ => panic!("Expected function value"),
                }
            }))
        }),
    )
    .await;
}

/// Evaluates a call to a closure (user-defined function).
///
/// Evaluates the arguments, binds them to the parameters in a new context, then evaluates the
/// function body in that context, passing results to the continuation.
///
/// # Parameters
///
/// * `params` - The parameter names of the closure.
/// * `body` - The body expression of the closure.
/// * `args` - The argument expressions to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(super) async fn evaluate_closure_call<G>(
    params: Vec<Identifier>,
    body: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation<Value>,
) where
    G: Generator,
{
    evaluate_sequence(
        args,
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
                body.evaluate(engine.with_new_context(new_ctx), k).await;
            }))
        }),
    )
    .await
}

/// Evaluates a call to a Rust UDF (built-in function).
///
/// Evaluates the arguments, then calls the Rust function with those arguments, passing the result
/// to the continuation.
///
/// # Parameters
///
/// * `udf` - The Rust function to call
/// * `args` - The argument expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_rust_udf_call<G>(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation<Value>,
) where
    G: Generator,
{
    evaluate_sequence(
        args,
        engine,
        Arc::new(move |arg_values| {
            Box::pin(capture!([udf, k], async move {
                // Call the UDF with the argument values.
                let result = udf(arg_values);

                // Pass the result to the continuation.
                k(result).await;
            }))
        }),
    )
    .await
}

/// Evaluates a reference to a variable.
///
/// Looks up the variable in the context and passes its value to the continuation.
///
/// # Parameters
///
/// * `ident` - The identifier to look up
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive the variable value
pub(super) async fn evaluate_reference<G>(ident: String, engine: Engine<G>, k: Continuation<Value>)
where
    G: Generator,
{
    // Look up the variable in the context.
    let value = engine
        .context
        .lookup(&ident)
        .unwrap_or_else(|| panic!("Variable not found: {}", ident))
        .clone();

    // Pass the value to the continuation.
    k(value).await;
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        Engine,
        test_utils::{
            MockGenerator, array_val, boolean, evaluate_and_collect, int, lit_expr, lit_val,
            ref_expr, string,
        },
    };
    use optd_dsl::analyzer::{
        context::Context,
        hir::{BinOp, CoreData, Expr, FunKind, Literal, Value},
    };
    use std::sync::Arc;

    /// Test if-then-else expressions with true and false conditions
    #[tokio::test]
    async fn test_if_then_else() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx, mock_gen);

        // if true then "yes" else "no"
        let true_condition = Arc::new(Expr::IfThenElse(
            lit_expr(boolean(true)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        ));
        let true_results = evaluate_and_collect(true_condition, engine.clone()).await;

        // if false then "yes" else "no"
        let false_condition = Arc::new(Expr::IfThenElse(
            lit_expr(boolean(false)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        ));
        let false_results = evaluate_and_collect(false_condition, engine.clone()).await;

        // Let's create a more complex condition: if x > 10 then x * 2 else x / 2
        let mut ctx = Context::default();
        ctx.bind("x".to_string(), lit_val(int(20)));
        let engine_with_x = Engine::new(ctx, MockGenerator::new());

        let complex_condition = Arc::new(Expr::IfThenElse(
            Arc::new(Expr::Binary(ref_expr("x"), BinOp::Lt, lit_expr(int(10)))),
            Arc::new(Expr::Binary(ref_expr("x"), BinOp::Div, lit_expr(int(2)))),
            Arc::new(Expr::Binary(ref_expr("x"), BinOp::Mul, lit_expr(int(2)))),
        ));

        let complex_results = evaluate_and_collect(complex_condition, engine_with_x).await;

        // Check results
        match &true_results[0].0 {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "yes"); // true condition should select "yes"
            }
            _ => panic!("Expected string value"),
        }

        match &false_results[0].0 {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "no"); // false condition should select "no"
            }
            _ => panic!("Expected string value"),
        }

        match &complex_results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 40); // 20 * 2 = 40 (since x > 10)
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test let bindings and variable references
    #[tokio::test]
    async fn test_let_binding() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx, mock_gen);

        // let x = 10 in x + 5
        let let_expr = Arc::new(Expr::Let(
            "x".to_string(),
            lit_expr(int(10)),
            Arc::new(Expr::Binary(ref_expr("x"), BinOp::Add, lit_expr(int(5)))),
        ));

        let results = evaluate_and_collect(let_expr, engine).await;

        // Check result
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 15); // 10 + 5 = 15
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test nested let bindings
    #[tokio::test]
    async fn test_nested_let_bindings() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx, mock_gen);

        // let x = 10 in
        //   let y = x * 2 in
        //     x + y
        let nested_let_expr = Arc::new(Expr::Let(
            "x".to_string(),
            lit_expr(int(10)),
            Arc::new(Expr::Let(
                "y".to_string(),
                Arc::new(Expr::Binary(ref_expr("x"), BinOp::Mul, lit_expr(int(2)))),
                Arc::new(Expr::Binary(ref_expr("x"), BinOp::Add, ref_expr("y"))),
            )),
        ));

        let results = evaluate_and_collect(nested_let_expr, engine).await;

        // Check result
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 30); // 10 + (10 * 2) = 30
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test function calls with user-defined functions (closures)
    #[tokio::test]
    async fn test_function_call_closure() {
        let mock_gen = MockGenerator::new();
        let mut ctx = Context::default();

        // Define a function: fn(x, y) => x + y
        let add_function = Value(CoreData::Function(FunKind::Closure(
            vec!["x".to_string(), "y".to_string()],
            Arc::new(Expr::Binary(ref_expr("x"), BinOp::Add, ref_expr("y"))),
        )));

        ctx.bind("add".to_string(), add_function);
        let engine = Engine::new(ctx, mock_gen);

        // Call the function: add(10, 20)
        let call_expr = Arc::new(Expr::Call(
            ref_expr("add"),
            vec![lit_expr(int(10)), lit_expr(int(20))],
        ));

        let results = evaluate_and_collect(call_expr, engine).await;

        // Check result
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 30); // 10 + 20 = 30
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test function calls with built-in functions (Rust UDFs)
    #[tokio::test]
    async fn test_function_call_rust_udf() {
        let mock_gen = MockGenerator::new();
        let mut ctx = Context::default();

        // Define a Rust UDF that calculates the sum of array elements
        let sum_function = Value(CoreData::Function(FunKind::RustUDF(|args| {
            match &args[0].0 {
                CoreData::Array(elements) => {
                    let mut sum = 0;
                    for elem in elements {
                        if let CoreData::Literal(Literal::Int64(value)) = &elem.0 {
                            sum += value;
                        }
                    }
                    Value(CoreData::Literal(Literal::Int64(sum)))
                }
                _ => panic!("Expected array argument"),
            }
        })));

        ctx.bind("sum".to_string(), sum_function);
        let engine = Engine::new(ctx, mock_gen);

        // Call the function: sum([1, 2, 3, 4, 5])
        let call_expr = Arc::new(Expr::Call(
            ref_expr("sum"),
            vec![Arc::new(Expr::CoreVal(array_val(vec![
                lit_val(int(1)),
                lit_val(int(2)),
                lit_val(int(3)),
                lit_val(int(4)),
                lit_val(int(5)),
            ])))],
        ));

        let results = evaluate_and_collect(call_expr, engine).await;

        // Check result
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 15); // 1 + 2 + 3 + 4 + 5 = 15
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test complex program with multiple expression types
    #[tokio::test]
    async fn test_complex_program() {
        let mock_gen = MockGenerator::new();
        let mut ctx = Context::default();

        // Define a function to compute factorial: fn(n) => if n <= 1 then 1 else n * factorial(n-1)
        let factorial_function = Value(CoreData::Function(FunKind::Closure(
            vec!["n".to_string()],
            Arc::new(Expr::IfThenElse(
                Arc::new(Expr::Binary(
                    ref_expr("n"),
                    BinOp::Lt,
                    lit_expr(int(2)), // n < 2
                )),
                lit_expr(int(1)), // then 1
                Arc::new(Expr::Binary(
                    ref_expr("n"),
                    BinOp::Mul,
                    Arc::new(Expr::Call(
                        ref_expr("factorial"),
                        vec![Arc::new(Expr::Binary(
                            ref_expr("n"),
                            BinOp::Sub,
                            lit_expr(int(1)),
                        ))],
                    )),
                )), // else n * factorial(n-1)
            )),
        )));

        ctx.bind("factorial".to_string(), factorial_function);
        let engine = Engine::new(ctx, mock_gen);

        // Create a program that:
        // 1. Defines variables for different values
        // 2. Calls factorial on one of them
        // 3. Performs some arithmetic on the result
        let program = Arc::new(Expr::Let(
            "a".to_string(),
            lit_expr(int(5)), // a = 5
            Arc::new(Expr::Let(
                "b".to_string(),
                lit_expr(int(3)), // b = 3
                Arc::new(Expr::Let(
                    "fact_a".to_string(),
                    Arc::new(Expr::Call(ref_expr("factorial"), vec![ref_expr("a")])), // fact_a = factorial(a)
                    Arc::new(Expr::Binary(ref_expr("fact_a"), BinOp::Div, ref_expr("b"))), // fact_a / b
                )),
            )),
        ));

        let results = evaluate_and_collect(program, engine).await;

        // Check result: factorial(5) / 3 = 120 / 3 = 40
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 40);
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test variable reference in various contexts
    #[tokio::test]
    async fn test_variable_references() {
        let mock_gen = MockGenerator::new();

        // Test that variables from outer scope are visible in inner scope
        let mut ctx = Context::default();
        ctx.bind("outer_var".to_string(), lit_val(int(100)));
        ctx.push_scope();
        ctx.bind("inner_var".to_string(), lit_val(int(200)));

        let engine = Engine::new(ctx, mock_gen);

        // Reference to a variable in the current (inner) scope
        let inner_ref = Arc::new(Expr::Ref("inner_var".to_string()));
        let inner_results = evaluate_and_collect(inner_ref, engine.clone()).await;

        // Reference to a variable in the outer scope
        let outer_ref = Arc::new(Expr::Ref("outer_var".to_string()));
        let outer_results = evaluate_and_collect(outer_ref, engine.clone()).await;

        // Check results
        match &inner_results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 200);
            }
            _ => panic!("Expected integer value"),
        }

        match &outer_results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 100);
            }
            _ => panic!("Expected integer value"),
        }
    }
}
