use super::{binary::eval_binary_op, unary::eval_unary_op};
use crate::analyzer::hir::{BinOp, CoreData, Expr, FunKind, Identifier, Literal, UnaryOp, Value};
use crate::analyzer::map::Map;
use crate::engine::{Continuation, EngineResponse};
use crate::{
    capture,
    engine::{Engine, utils::evaluate_sequence},
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
pub(crate) async fn evaluate_if_then_else<O>(
    cond: Arc<Expr>,
    then_expr: Arc<Expr>,
    else_expr: Arc<Expr>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // First evaluate the condition
    engine
        .clone()
        .evaluate(
            cond,
            Arc::new(move |value| {
                Box::pin(capture!([then_expr, else_expr, engine, k], async move {
                    match value.0 {
                        CoreData::Literal(Literal::Bool(b)) => {
                            if b {
                                engine.evaluate(then_expr, k).await
                            } else {
                                engine.evaluate(else_expr, k).await
                            }
                        }
                        _ => panic!("Expected boolean in condition"),
                    }
                }))
            }),
        )
        .await
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
pub(crate) async fn evaluate_let_binding<O>(
    ident: String,
    assignee: Arc<Expr>,
    after: Arc<Expr>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Evaluate the assignee first.
    engine
        .clone()
        .evaluate(
            assignee,
            Arc::new(move |value| {
                Box::pin(capture!([ident, after, engine, k], async move {
                    // Create updated context with the new binding.
                    let mut new_ctx = engine.context.clone();
                    new_ctx.bind(ident, value);

                    // Evaluate the after expression in the updated context.
                    engine.with_new_context(new_ctx).evaluate(after, k).await
                }))
            }),
        )
        .await
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
pub(crate) async fn evaluate_binary_expr<O>(
    left: Arc<Expr>,
    op: BinOp,
    right: Arc<Expr>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Helper function to evaluate the right operand after the left is evaluated.
    async fn evaluate_right<O>(
        left_val: Value,
        right: Arc<Expr>,
        op: BinOp,
        engine: Engine,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> EngineResponse<O>
    where
        O: Send + 'static,
    {
        engine
            .evaluate(
                right,
                Arc::new(move |right_val| {
                    Box::pin(capture!([left_val, op, k], async move {
                        // Apply the binary operation and pass result to continuation.
                        let result = eval_binary_op(left_val, &op, right_val);
                        k(result).await
                    }))
                }),
            )
            .await
    }

    // First evaluate the left operand.
    engine
        .clone()
        .evaluate(
            left,
            Arc::new(move |left_val| {
                Box::pin(capture!([right, op, engine, k], async move {
                    evaluate_right(left_val, right, op, engine, k).await
                }))
            }),
        )
        .await
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
pub(crate) async fn evaluate_unary_expr<O>(
    op: UnaryOp,
    expr: Arc<Expr>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Evaluate the operand, then apply the unary operation.
    engine
        .evaluate(
            expr,
            Arc::new(move |value| {
                Box::pin(capture!([op, k], async move {
                    // Apply the unary operation and pass result to continuation.
                    let result = eval_unary_op(&op, value);
                    k(result).await
                }))
            }),
        )
        .await
}

/// Evaluates a call expression.
///
/// First evaluates the called expression, then the arguments, and finally applies the call to
/// the arguments, passing results to the continuation.
///
/// Extended to support indexing into collections (Array, Tuple, Struct, Map) when the called
/// expression evaluates to one of these types and a single argument is provided.
///
/// # Parameters
///
/// * `called` - The called expression to evaluate.
/// * `args` - The argument expressions to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(crate) async fn evaluate_call<O>(
    called: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // First evaluate the function expression.
    engine
        .clone()
        .evaluate(
            called,
            Arc::new(move |called_value| {
                Box::pin(capture!([args, engine, k], async move {
                    match called_value.0 {
                        // Handle closure (user-defined function).
                        CoreData::Function(FunKind::Closure(params, body)) => {
                            evaluate_closure_call(params, body, args, engine, k).await
                        }
                        // Handle Rust UDF (built-in function).
                        CoreData::Function(FunKind::RustUDF(udf)) => {
                            evaluate_rust_udf_call(udf, args, engine, k).await
                        }
                        // Handle indexing into collections (Array, Tuple, Struct).
                        CoreData::Array(_) | CoreData::Tuple(_) | CoreData::Struct(_, _) => {
                            evaluate_indexed_access(called_value, args, engine, k).await
                        }
                        // Handle Map lookup.
                        CoreData::Map(_) => {
                            evaluate_map_lookup(called_value, args, engine, k).await
                        }
                        // Value must be a function or indexable collection.
                        _ => panic!(
                            "Expected function or indexable collection, got: {:?}",
                            called_value
                        ),
                    }
                }))
            }),
        )
        .await
}

/// Evaluates indexing into a collection (Array, Tuple, or Struct).
///
/// # Parameters
///
/// * `collection` - The collection value to index into.
/// * `args` - The argument expressions (should be a single index).
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
async fn evaluate_indexed_access<O>(
    collection: Value,
    args: Vec<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Check that there's exactly one argument.
    if args.len() != 1 {
        panic!("Indexed access requires exactly one index argument");
    }

    // Evaluate the index expression.
    engine
        .evaluate(
            args[0].clone(),
            Arc::new(move |index_value| {
                Box::pin(capture!([collection, k], async move {
                    // Extract the index as an integer.
                    let index = match &index_value.0 {
                        CoreData::Literal(Literal::Int64(i)) => *i as usize,
                        _ => panic!("Index must be an integer, got: {:?}", index_value),
                    };

                    // Index into the collection based on its type.
                    let result = match &collection.0 {
                        CoreData::Array(items) => get_indexed_item(items, index),
                        CoreData::Tuple(items) => get_indexed_item(items, index),
                        CoreData::Struct(_, fields) => get_indexed_item(fields, index),
                        _ => panic!("Attempted to index a non-indexable value: {:?}", collection),
                    };

                    fn get_indexed_item(items: &[Value], index: usize) -> Value {
                        if index < items.len() {
                            items[index].clone()
                        } else {
                            panic!("index out of bounds: {} >= {}", index, items.len());
                        }
                    }

                    // Pass the indexed value to the continuation.
                    k(result).await
                }))
            }),
        )
        .await
}

/// Evaluates a map lookup.
///
/// # Parameters
///
/// * `map_value` - The map value to look up in.
/// * `args` - The argument expressions (should be a single key).
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
async fn evaluate_map_lookup<O>(
    map_value: Value,
    args: Vec<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Check that there's exactly one argument
    if args.len() != 1 {
        panic!("Map lookup requires exactly one key argument");
    }

    // Evaluate the key expression
    engine
        .evaluate(
            args[0].clone(),
            Arc::new(move |key_value| {
                Box::pin(capture!([map_value, k], async move {
                    // Extract the map
                    match &map_value.0 {
                        CoreData::Map(map) => {
                            // Look up the key in the map, returning None if not found
                            let result = map.get(&key_value);
                            k(result).await
                        }
                        _ => panic!(
                            "Attempted to perform map lookup on non-map value: {:?}",
                            map_value
                        ),
                    }
                }))
            }),
        )
        .await
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
pub(crate) async fn evaluate_closure_call<O>(
    params: Vec<Identifier>,
    body: Arc<Expr>,
    args: Vec<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
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
                engine.with_new_context(new_ctx).evaluate(body, k).await
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
pub(crate) async fn evaluate_rust_udf_call<O>(
    udf: fn(Vec<Value>) -> Value,
    args: Vec<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    evaluate_sequence(
        args,
        engine,
        Arc::new(move |arg_values| {
            Box::pin(capture!([udf, k], async move {
                // Call the UDF with the argument values.
                let result = udf(arg_values);

                // Pass the result to the continuation.
                k(result).await
            }))
        }),
    )
    .await
}

/// Evaluates a map expression.
///
/// # Parameters
///
/// * `items` - The key-value pairs to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(crate) async fn evaluate_map<O>(
    items: Vec<(Arc<Expr>, Arc<Expr>)>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Extract keys and values.
    let (keys, values): (Vec<Arc<Expr>>, Vec<Arc<Expr>>) = items.into_iter().unzip();

    // First evaluate all key expressions.
    evaluate_sequence(
        keys,
        engine.clone(),
        Arc::new(move |keys_values| {
            Box::pin(capture!([values, engine, k], async move {
                // Then evaluate all value expressions.
                evaluate_sequence(
                    values,
                    engine,
                    Arc::new(move |values_values| {
                        Box::pin(capture!([keys_values, k], async move {
                            // Create a map from keys and values.
                            let map_items = keys_values.into_iter().zip(values_values).collect();
                            k(Value(CoreData::Map(Map::from_pairs(map_items)))).await
                        }))
                    }),
                )
                .await
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
pub(crate) async fn evaluate_reference<O>(
    ident: String,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    // Look up the variable in the context.
    let value = engine
        .context
        .lookup(&ident)
        .unwrap_or_else(|| panic!("Variable not found: {}", ident))
        .clone();

    // Pass the value to the continuation.
    k(value).await
}

#[cfg(test)]
mod tests {
    use crate::engine::Engine;
    use crate::utils::tests::{array_val, assert_values_equal, ref_expr, struct_val};
    use crate::{
        analyzer::{
            context::Context,
            hir::{BinOp, CoreData, Expr, ExprKind, FunKind, Literal, Value},
        },
        utils::tests::{
            TestHarness, boolean, evaluate_and_collect, int, lit_expr, lit_val, string,
        },
    };
    use ExprKind::*;
    use std::sync::Arc;

    /// Test if-then-else expressions with true and false conditions
    #[tokio::test]
    async fn test_if_then_else() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // if true then "yes" else "no"
        let true_condition = Arc::new(Expr::new(IfThenElse(
            lit_expr(boolean(true)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        )));
        let true_results =
            evaluate_and_collect(true_condition, engine.clone(), harness.clone()).await;

        // if false then "yes" else "no"
        let false_condition = Arc::new(Expr::new(IfThenElse(
            lit_expr(boolean(false)),
            lit_expr(string("yes")),
            lit_expr(string("no")),
        )));
        let false_results =
            evaluate_and_collect(false_condition, engine.clone(), harness.clone()).await;

        // Let's create a more complex condition: if x > 10 then x * 2 else x / 2
        let mut ctx = Context::default();
        ctx.bind("x".to_string(), lit_val(int(20)));
        let engine_with_x = Engine::new(ctx);

        let complex_condition = Arc::new(Expr::new(IfThenElse(
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Lt,
                lit_expr(int(10)),
            ))),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Div,
                lit_expr(int(2)),
            ))),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Mul,
                lit_expr(int(2)),
            ))),
        )));

        let complex_results = evaluate_and_collect(complex_condition, engine_with_x, harness).await;

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
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // let x = 10 in x + 5
        let let_expr = Arc::new(Expr::new(Let(
            "x".to_string(),
            lit_expr(int(10)),
            Arc::new(Expr::new(Binary(
                ref_expr("x"),
                BinOp::Add,
                lit_expr(int(5)),
            ))),
        )));

        let results = evaluate_and_collect(let_expr, engine, harness).await;

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
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // let x = 10 in
        //   let y = x * 2 in
        //     x + y
        let nested_let_expr = Arc::new(Expr::new(Let(
            "x".to_string(),
            lit_expr(int(10)),
            Arc::new(Expr::new(Let(
                "y".to_string(),
                Arc::new(Expr::new(Binary(
                    ref_expr("x"),
                    BinOp::Mul,
                    lit_expr(int(2)),
                ))),
                Arc::new(Expr::new(Binary(ref_expr("x"), BinOp::Add, ref_expr("y")))),
            ))),
        )));

        let results = evaluate_and_collect(nested_let_expr, engine, harness).await;

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
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a function: fn(x, y) => x + y
        let add_function = Value(CoreData::Function(FunKind::Closure(
            vec!["x".to_string(), "y".to_string()],
            Arc::new(Expr::new(Binary(ref_expr("x"), BinOp::Add, ref_expr("y")))),
        )));

        ctx.bind("add".to_string(), add_function);
        let engine = Engine::new(ctx);

        // Call the function: add(10, 20)
        let call_expr = Arc::new(Expr::new(Call(
            ref_expr("add"),
            vec![lit_expr(int(10)), lit_expr(int(20))],
        )));

        let results = evaluate_and_collect(call_expr, engine, harness).await;

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
        let harness = TestHarness::new();
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
        let engine = Engine::new(ctx);

        // Call the function: sum([1, 2, 3, 4, 5])
        let call_expr = Arc::new(Expr::new(Call(
            ref_expr("sum"),
            vec![Arc::new(Expr::new(CoreVal(array_val(vec![
                lit_val(int(1)),
                lit_val(int(2)),
                lit_val(int(3)),
                lit_val(int(4)),
                lit_val(int(5)),
            ]))))],
        )));

        let results = evaluate_and_collect(call_expr, engine, harness).await;

        // Check result
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 15); // 1 + 2 + 3 + 4 + 5 = 15
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test to verify the Map implementation works correctly.
    #[tokio::test]
    async fn test_map_creation() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a map with key-value pairs: { "a": 1, "b": 2, "c": 3 }
        let map_expr = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("a")), lit_expr(int(1))),
            (lit_expr(string("b")), lit_expr(int(2))),
            (lit_expr(string("c")), lit_expr(int(3))),
        ])));

        // Evaluate the map expression
        let results = evaluate_and_collect(map_expr, engine.clone(), harness.clone()).await;

        // Check that we got a Map value
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Map(map) => {
                // Check that map has the correct key-value pairs
                assert_values_equal(&map.get(&lit_val(string("a"))), &lit_val(int(1)));
                assert_values_equal(&map.get(&lit_val(string("b"))), &lit_val(int(2)));
                assert_values_equal(&map.get(&lit_val(string("c"))), &lit_val(int(3)));

                // Check that non-existent key returns None value
                assert_values_equal(&map.get(&lit_val(string("d"))), &Value(CoreData::None));
            }
            _ => panic!("Expected Map value"),
        }

        // Test map with expressions that need evaluation as keys and values
        // Map: { "x" + "y": 10 + 5, "a" + "b": 20 * 2 }
        let complex_map_expr = Arc::new(Expr::new(Map(vec![
            (
                Arc::new(Expr::new(Binary(
                    lit_expr(string("x")),
                    BinOp::Concat,
                    lit_expr(string("y")),
                ))),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(10)),
                    BinOp::Add,
                    lit_expr(int(5)),
                ))),
            ),
            (
                Arc::new(Expr::new(Binary(
                    lit_expr(string("a")),
                    BinOp::Concat,
                    lit_expr(string("b")),
                ))),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(20)),
                    BinOp::Mul,
                    lit_expr(int(2)),
                ))),
            ),
        ])));

        // Evaluate the complex map expression
        let complex_results = evaluate_and_collect(complex_map_expr, engine, harness).await;

        // Check that we got a Map value with correctly evaluated keys and values
        assert_eq!(complex_results.len(), 1);
        match &complex_results[0].0 {
            CoreData::Map(map) => {
                // Check that map has the correct key-value pairs after evaluation
                assert_values_equal(&map.get(&lit_val(string("xy"))), &lit_val(int(15)));
                assert_values_equal(&map.get(&lit_val(string("ab"))), &lit_val(int(40)));
            }
            _ => panic!("Expected Map value"),
        }
    }

    /// Test map operations with nested maps and lookup
    #[tokio::test]
    async fn test_map_nested_and_lookup() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Add a map lookup function
        ctx.bind(
            "get".to_string(),
            Value(CoreData::Function(FunKind::RustUDF(|args| {
                if args.len() != 2 {
                    panic!("get function requires 2 arguments");
                }

                match &args[0].0 {
                    CoreData::Map(map) => map.get(&args[1]),
                    _ => panic!("First argument must be a map"),
                }
            }))),
        );

        let engine = Engine::new(ctx);

        // Create a nested map:
        // {
        //   "user": {
        //     "name": "Alice",
        //     "age": 30,
        //     "address": {
        //       "city": "San Francisco",
        //       "zip": 94105
        //     }
        //   },
        //   "settings": {
        //     "theme": "dark",
        //     "notifications": true
        //   }
        // }

        // First, create the address map
        let address_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("city")), lit_expr(string("San Francisco"))),
            (lit_expr(string("zip")), lit_expr(int(94105))),
        ])));

        // Then, create the user map with the nested address map
        let user_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("name")), lit_expr(string("Alice"))),
            (lit_expr(string("age")), lit_expr(int(30))),
            (lit_expr(string("address")), address_map),
        ])));

        // Create the settings map
        let settings_map = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("theme")), lit_expr(string("dark"))),
            (lit_expr(string("notifications")), lit_expr(boolean(true))),
        ])));

        // Finally, create the top-level map
        let nested_map_expr = Arc::new(Expr::new(Map(vec![
            (lit_expr(string("user")), user_map),
            (lit_expr(string("settings")), settings_map),
        ])));

        // First, evaluate the nested map to bind it to a variable
        let program = Arc::new(Expr::new(Let(
            "data".to_string(),
            nested_map_expr,
            // Extract user.address.city using get function
            Arc::new(Expr::new(Call(
                ref_expr("get"),
                vec![
                    Arc::new(Expr::new(Call(
                        ref_expr("get"),
                        vec![
                            Arc::new(Expr::new(Call(
                                ref_expr("get"),
                                vec![ref_expr("data"), lit_expr(string("user"))],
                            ))),
                            lit_expr(string("address")),
                        ],
                    ))),
                    lit_expr(string("city")),
                ],
            ))),
        )));

        // Evaluate the program
        let results = evaluate_and_collect(program, engine, harness).await;

        // Check that we got the correct value from the nested lookup
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(Literal::String(value)) => {
                assert_eq!(value, "San Francisco");
            }
            _ => panic!("Expected string value"),
        }
    }

    /// Test complex program with multiple expression types
    #[tokio::test]
    async fn test_complex_program() {
        let harness = TestHarness::new();
        let mut ctx = Context::default();

        // Define a function to compute factorial: fn(n) => if n <= 1 then 1 else n * factorial(n-1)
        let factorial_function = Value(CoreData::Function(FunKind::Closure(
            vec!["n".to_string()],
            Arc::new(Expr::new(IfThenElse(
                Arc::new(Expr::new(Binary(
                    ref_expr("n"),
                    BinOp::Lt,
                    lit_expr(int(2)), // n < 2
                ))),
                lit_expr(int(1)), // then 1
                Arc::new(Expr::new(Binary(
                    ref_expr("n"),
                    BinOp::Mul,
                    Arc::new(Expr::new(Call(
                        ref_expr("factorial"),
                        vec![Arc::new(Expr::new(Binary(
                            ref_expr("n"),
                            BinOp::Sub,
                            lit_expr(int(1)),
                        )))],
                    ))),
                ))), // else n * factorial(n-1)
            ))),
        )));

        ctx.bind("factorial".to_string(), factorial_function);
        let engine = Engine::new(ctx);

        // Create a program that:
        // 1. Defines variables for different values
        // 2. Calls factorial on one of them
        // 3. Performs some arithmetic on the result
        let program = Arc::new(Expr::new(Let(
            "a".to_string(),
            lit_expr(int(5)), // a = 5
            Arc::new(Expr::new(Let(
                "b".to_string(),
                lit_expr(int(3)), // b = 3
                Arc::new(Expr::new(Let(
                    "fact_a".to_string(),
                    Arc::new(Expr::new(Call(ref_expr("factorial"), vec![ref_expr("a")]))), // fact_a = factorial(a)
                    Arc::new(Expr::new(Binary(
                        ref_expr("fact_a"),
                        BinOp::Div,
                        ref_expr("b"),
                    ))), // fact_a / b
                ))),
            ))),
        )));

        let results = evaluate_and_collect(program, engine, harness).await;

        // Check result: factorial(5) / 3 = 120 / 3 = 40
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 40);
            }
            _ => panic!("Expected integer value"),
        }
    }

    /// Test array indexing with call syntax
    #[tokio::test]
    async fn test_array_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create an array [10, 20, 30, 40, 50]
        let array_expr = Arc::new(Expr::new(CoreVal(array_val(vec![
            lit_val(int(10)),
            lit_val(int(20)),
            lit_val(int(30)),
            lit_val(int(40)),
            lit_val(int(50)),
        ]))));

        // Access array[2] which should be 30
        let index_expr = Arc::new(Expr::new(Call(array_expr, vec![lit_expr(int(2))])));

        let results = evaluate_and_collect(index_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(30));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test tuple indexing with call syntax
    #[tokio::test]
    async fn test_tuple_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a tuple (10, "hello", true)
        let tuple_expr = Arc::new(Expr::new(CoreVal(Value(CoreData::Tuple(vec![
            lit_val(int(10)),
            lit_val(string("hello")),
            lit_val(Literal::Bool(true)),
        ])))));

        // Access tuple[1] which should be "hello"
        let index_expr = Arc::new(Expr::new(Call(tuple_expr, vec![lit_expr(int(1))])));

        let results = evaluate_and_collect(index_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::String("hello".to_string()));
            }
            _ => panic!("Expected string literal"),
        }
    }

    /// Test struct field access with call syntax
    #[tokio::test]
    async fn test_struct_indexing() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a struct Point { x: 10, y: 20 }
        let struct_expr = Arc::new(Expr::new(CoreVal(struct_val(
            "Point",
            vec![lit_val(int(10)), lit_val(int(20))],
        ))));

        // Access struct[1] which should be 20 (the y field)
        let index_expr = Arc::new(Expr::new(Call(struct_expr, vec![lit_expr(int(1))])));

        let results = evaluate_and_collect(index_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(20));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test map lookup with call syntax
    #[tokio::test]
    async fn test_map_lookup() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a map with key-value pairs: { "a": 1, "b": 2, "c": 3 }
        // Use a let expression to bind the map and do lookups directly
        let test_expr = Arc::new(Expr::new(Let(
            "map".to_string(),
            Arc::new(Expr::new(Map(vec![
                (lit_expr(string("a")), lit_expr(int(1))),
                (lit_expr(string("b")), lit_expr(int(2))),
                (lit_expr(string("c")), lit_expr(int(3))),
            ]))),
            // Create a tuple of map["b"] and map["d"] to test both existing and missing keys
            Arc::new(Expr::new(CoreExpr(CoreData::Tuple(vec![
                // map["b"] - should be 2
                Arc::new(Expr::new(Call(
                    ref_expr("map"),
                    vec![lit_expr(string("b"))],
                ))),
                // map["d"] - should be None
                Arc::new(Expr::new(Call(
                    ref_expr("map"),
                    vec![lit_expr(string("d"))],
                ))),
            ])))),
        )));

        let results = evaluate_and_collect(test_expr, engine, harness).await;

        // Check result - should be a tuple (2, None)
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Tuple(elements) => {
                assert_eq!(elements.len(), 2);

                // Check first element: map["b"] should be 2
                match &elements[0].0 {
                    CoreData::Literal(lit) => {
                        assert_eq!(lit, &Literal::Int64(2));
                    }
                    _ => panic!("Expected integer literal for existing key lookup"),
                }

                // Check second element: map["d"] should be None
                match &elements[1].0 {
                    CoreData::None => {}
                    _ => panic!("Expected None for missing key lookup"),
                }
            }
            _ => panic!("Expected tuple result"),
        }
    }

    /// Test complex expressions for both collection and index
    #[tokio::test]
    async fn test_complex_collection_and_index() {
        let harness = TestHarness::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a let expression that binds an array and then accesses it
        // let arr = [10, 20, 30, 40, 50] in
        // let idx = 2 + 1 in
        //   arr[idx]  // should be 40
        let complex_expr = Arc::new(Expr::new(Let(
            "arr".to_string(),
            Arc::new(Expr::new(CoreExpr(CoreData::Array(vec![
                lit_expr(int(10)),
                lit_expr(int(20)),
                lit_expr(int(30)),
                lit_expr(int(40)),
                lit_expr(int(50)),
            ])))),
            Arc::new(Expr::new(Let(
                "idx".to_string(),
                Arc::new(Expr::new(Binary(
                    lit_expr(int(2)),
                    BinOp::Add,
                    lit_expr(int(1)),
                ))),
                Arc::new(Expr::new(Call(ref_expr("arr"), vec![ref_expr("idx")]))),
            ))),
        )));

        let results = evaluate_and_collect(complex_expr, engine, harness).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(lit) => {
                assert_eq!(lit, &Literal::Int64(40));
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test variable reference in various contexts
    #[tokio::test]
    async fn test_variable_references() {
        let harness = TestHarness::new();

        // Test that variables from outer scope are visible in inner scope
        let mut ctx = Context::default();
        ctx.bind("outer_var".to_string(), lit_val(int(100)));
        ctx.push_scope();
        ctx.bind("inner_var".to_string(), lit_val(int(200)));

        let engine = Engine::new(ctx);

        // Reference to a variable in the current (inner) scope
        let inner_ref = Arc::new(Expr::new(Ref("inner_var".to_string())));
        let inner_results = evaluate_and_collect(inner_ref, engine.clone(), harness.clone()).await;

        // Reference to a variable in the outer scope
        let outer_ref = Arc::new(Expr::new(Ref("outer_var".to_string())));
        let outer_results = evaluate_and_collect(outer_ref, engine.clone(), harness).await;

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
