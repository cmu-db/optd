use super::operator::{evaluate_logical_operator, evaluate_physical_operator};
use crate::analyzer::hir::{CoreData, Expr, Value};
use crate::engine::utils::evaluate_sequence;
use crate::engine::{Continuation, EngineResponse};
use crate::{capture, engine::Engine};
use CoreData::*;
use std::sync::Arc;

/// Evaluates a core expression by generating all possible evaluation paths.
///
/// This function dispatches to specialized handlers based on the expression type, passing all
/// possible values the expression could evaluate to the continuation.
///
/// # Parameters
///
/// * `data` - The core expression data to evaluate.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
pub(crate) async fn evaluate_core_expr<O>(
    data: CoreData<Arc<Expr>>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    match data {
        Literal(lit) => {
            // Directly continue with the literal value.
            k(Value(Literal(lit))).await
        }
        Array(items) => evaluate_collection(items, Array, engine, k).await,
        Tuple(items) => evaluate_collection(items, Tuple, engine, k).await,
        Struct(name, items) => {
            evaluate_collection(items, move |values| Struct(name, values), engine, k).await
        }
        Map(items) => evaluate_map(items, engine, k).await,
        Function(fun_type) => {
            // Directly continue with the function value.
            k(Value(Function(fun_type))).await
        }
        Fail(msg) => evaluate_fail(*msg, engine, k).await,
        Logical(op) => evaluate_logical_operator(op, engine, k).await,
        Physical(op) => evaluate_physical_operator(op, engine, k).await,
        Null => {
            // Directly continue with null value.
            k(Value(Null)).await
        }
    }
}

/// Evaluates a collection expression (Array, Tuple, or Struct).
///
/// # Parameters
///
/// * `items` - The collection items to evaluate.
/// * `constructor` - Function to construct the appropriate collection type.
/// * `engine` - The evaluation engine.
/// * `k` - The continuation to receive evaluation results.
async fn evaluate_collection<F, O>(
    items: Vec<Arc<Expr>>,
    constructor: F,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    F: FnOnce(Vec<Value>) -> CoreData<Value> + Clone + Send + Sync + 'static,
    O: Send + 'static,
{
    evaluate_sequence(
        items,
        engine,
        Arc::new(move |values| {
            Box::pin(capture!([constructor, k], async move {
                let result = Value(constructor(values));
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
async fn evaluate_map<O>(
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
                            k(Value(Map(map_items))).await
                        }))
                    }),
                )
                .await
            }))
        }),
    )
    .await
}

/// Evaluates a fail expression.
///
/// # Parameters
///
/// * `msg` - The message expression to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
async fn evaluate_fail<O>(
    msg: Arc<Expr>,
    engine: Engine,
    k: Continuation<Value, EngineResponse<O>>,
) -> EngineResponse<O>
where
    O: Send + 'static,
{
    engine
        .evaluate(
            msg,
            Arc::new(move |value| {
                Box::pin(capture!(
                    [k],
                    async move { k(Value(Fail(value.into()))).await }
                ))
            }),
        )
        .await
}

#[cfg(test)]
mod tests {
    use crate::engine::test_utils::{MockGenerator, evaluate_and_collect, int, lit_expr, string};
    use crate::{
        analyzer::{
            context::Context,
            hir::{CoreData, Expr, FunKind, Literal, Value},
        },
        engine::Engine,
    };
    use std::sync::Arc;

    /// Test evaluation of literal values
    #[tokio::test]
    async fn test_literal_evaluation() {
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a literal expression
        let literal_expr = Arc::new(Expr::CoreExpr(CoreData::Literal(int(42))));
        let results = evaluate_and_collect(literal_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Literal(Literal::Int64(value)) => {
                assert_eq!(*value, 42);
            }
            _ => panic!("Expected integer literal"),
        }
    }

    /// Test evaluation of array expressions
    #[tokio::test]
    async fn test_array_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create an array expression with values to evaluate
        let array_expr = Arc::new(Expr::CoreExpr(CoreData::Array(vec![
            lit_expr(int(1)),
            lit_expr(int(2)),
            lit_expr(int(3)),
        ])));

        let results = evaluate_and_collect(array_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Array(elements) => {
                assert_eq!(elements.len(), 3);
                match &elements[0].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 1),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[1].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 2),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[2].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 3),
                    _ => panic!("Expected integer literal"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    /// Test evaluation of tuple expressions
    #[tokio::test]
    async fn test_tuple_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a tuple expression with mixed types
        let tuple_expr = Arc::new(Expr::CoreExpr(CoreData::Tuple(vec![
            lit_expr(int(42)),
            lit_expr(string("hello")),
            lit_expr(Literal::Bool(true)),
        ])));

        let results = evaluate_and_collect(tuple_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Tuple(elements) => {
                assert_eq!(elements.len(), 3);
                match &elements[0].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 42),
                    _ => panic!("Expected integer literal"),
                }
                match &elements[1].0 {
                    CoreData::Literal(Literal::String(value)) => assert_eq!(value, "hello"),
                    _ => panic!("Expected string literal"),
                }
                match &elements[2].0 {
                    CoreData::Literal(Literal::Bool(value)) => assert!(*value),
                    _ => panic!("Expected boolean literal"),
                }
            }
            _ => panic!("Expected tuple"),
        }
    }

    /// Test evaluation of struct expressions
    #[tokio::test]
    async fn test_struct_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a struct expression
        let struct_expr = Arc::new(Expr::CoreExpr(CoreData::Struct(
            "Point".to_string(),
            vec![lit_expr(int(10)), lit_expr(int(20))],
        )));

        let results = evaluate_and_collect(struct_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Struct(name, fields) => {
                assert_eq!(name, "Point");
                assert_eq!(fields.len(), 2);
                match &fields[0].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 10),
                    _ => panic!("Expected integer literal"),
                }
                match &fields[1].0 {
                    CoreData::Literal(Literal::Int64(value)) => assert_eq!(*value, 20),
                    _ => panic!("Expected integer literal"),
                }
            }
            _ => panic!("Expected struct"),
        }
    }

    /// Test evaluation of map expressions
    #[tokio::test]
    async fn test_map_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a map expression
        let map_expr = Arc::new(Expr::CoreExpr(CoreData::Map(vec![
            (lit_expr(string("a")), lit_expr(int(1))),
            (lit_expr(string("b")), lit_expr(int(2))),
            (lit_expr(string("c")), lit_expr(int(3))),
        ])));

        let results = evaluate_and_collect(map_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Map(items) => {
                assert_eq!(items.len(), 3);

                // Find key "a" and check value
                let a_found = items.iter().any(|(k, v)| {
                    if let CoreData::Literal(Literal::String(key)) = &k.0 {
                        if key == "a" {
                            if let CoreData::Literal(Literal::Int64(val)) = &v.0 {
                                return *val == 1;
                            }
                        }
                    }
                    false
                });
                assert!(a_found, "Key 'a' with value 1 not found");

                // Find key "b" and check value
                let b_found = items.iter().any(|(k, v)| {
                    if let CoreData::Literal(Literal::String(key)) = &k.0 {
                        if key == "b" {
                            if let CoreData::Literal(Literal::Int64(val)) = &v.0 {
                                return *val == 2;
                            }
                        }
                    }
                    false
                });
                assert!(b_found, "Key 'b' with value 2 not found");

                // Find key "c" and check value
                let c_found = items.iter().any(|(k, v)| {
                    if let CoreData::Literal(Literal::String(key)) = &k.0 {
                        if key == "c" {
                            if let CoreData::Literal(Literal::Int64(val)) = &v.0 {
                                return *val == 3;
                            }
                        }
                    }
                    false
                });
                assert!(c_found, "Key 'c' with value 3 not found");
            }
            _ => panic!("Expected map"),
        }
    }

    /// Test evaluation of function expressions
    #[tokio::test]
    async fn test_function_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a function expression (just a simple closure)
        let fn_expr = Arc::new(Expr::CoreExpr(CoreData::Function(FunKind::Closure(
            vec!["x".to_string()],
            lit_expr(int(42)), // Just returns 42 regardless of argument
        ))));

        let results = evaluate_and_collect(fn_expr, engine).await;

        // Check that we got a function value
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Function(_) => {
                // Successfully evaluated to a function
            }
            _ => panic!("Expected function"),
        }
    }

    /// Test evaluation of null expressions
    #[tokio::test]
    async fn test_null_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a null expression
        let null_expr = Arc::new(Expr::CoreExpr(CoreData::Null));

        let results = evaluate_and_collect(null_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Null => {
                // Successfully evaluated to null
            }
            _ => panic!("Expected null"),
        }
    }

    /// Test evaluation of fail expressions
    #[tokio::test]
    async fn test_fail_evaluation() {
        let mock_gen = MockGenerator::new();
        let ctx = Context::default();
        let engine = Engine::new(ctx);

        // Create a fail expression with a message
        let fail_expr = Arc::new(Expr::CoreExpr(CoreData::Fail(Box::new(Arc::new(
            Expr::CoreVal(Value(CoreData::Literal(string("error message")))),
        )))));

        let results = evaluate_and_collect(fail_expr, engine).await;

        // Check result
        assert_eq!(results.len(), 1);
        match &results[0].0 {
            CoreData::Fail(boxed_value) => match &boxed_value.0 {
                CoreData::Literal(Literal::String(msg)) => {
                    assert_eq!(msg, "error message");
                }
                _ => panic!("Expected string message in fail"),
            },
            _ => panic!("Expected fail"),
        }
    }
}
