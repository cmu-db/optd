use super::{
    operator::{evaluate_logical_operator, evaluate_physical_operator},
    Engine, Evaluate, Generator,
};
use crate::capture;
use crate::engine::generator::Continuation;
use crate::engine::utils::evaluate_sequence;
use optd_dsl::analyzer::hir::{CoreData, Expr, Value};
use std::sync::Arc;
use CoreData::*;

/// Evaluates a core expression by generating all possible evaluation paths.
///
/// This function dispatches to specialized handlers based on the expression type,
/// passing all possible values the expression could evaluate to the continuation.
///
/// # Parameters
/// * `data` - The core expression data to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
pub(super) async fn evaluate_core_expr<G>(
    data: CoreData<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    match data {
        Literal(lit) => {
            // Directly continue with the literal value
            k(Value(Literal(lit))).await;
        }
        Array(items) => {
            evaluate_collection(items, Array, engine, k).await;
        }
        Tuple(items) => {
            evaluate_collection(items, Tuple, engine, k).await;
        }
        Struct(name, items) => {
            evaluate_collection(items, move |values| Struct(name, values), engine, k).await;
        }
        Map(items) => {
            evaluate_map(items, engine, k).await;
        }
        Function(fun_type) => {
            // Directly continue with the function value
            k(Value(Function(fun_type))).await;
        }
        Fail(msg) => {
            evaluate_fail(*msg, engine, k).await;
        }
        Logical(op) => {
            evaluate_logical_operator(op, engine, k).await;
        }
        Physical(op) => {
            evaluate_physical_operator(op, engine, k).await;
        }
        Null => {
            // Directly continue with null value
            k(Value(Null)).await;
        }
    }
}

/// Evaluates a collection expression (Array, Tuple, or Struct).
///
/// # Parameters
/// * `items` - The collection items to evaluate
/// * `constructor` - Function to construct the appropriate collection type
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
async fn evaluate_collection<G, F>(
    items: Vec<Arc<Expr>>,
    constructor: F,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
    F: FnOnce(Vec<Value>) -> CoreData<Value> + Clone + Send + Sync + 'static,
{
    evaluate_sequence(
        items,
        engine,
        Arc::new(move |values| {
            Box::pin(capture!([constructor, k], async move {
                let result = Value(constructor(values));
                k(result).await;
            }))
        }),
    )
    .await
}

/// Evaluates a map expression.
///
/// # Parameters
/// * `items` - The key-value pairs to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
async fn evaluate_map<G>(items: Vec<(Arc<Expr>, Arc<Expr>)>, engine: Engine<G>, k: Continuation)
where
    G: Generator,
{
    // Extract keys and values
    let (keys, values): (Vec<Arc<Expr>>, Vec<Arc<Expr>>) = items.into_iter().unzip();

    // First evaluate all key expressions
    evaluate_sequence(
        keys,
        engine.clone(),
        Arc::new(move |keys_values| {
            Box::pin(capture!([values, engine, k], async move {
                // Then evaluate all value expressions
                evaluate_sequence(
                    values,
                    engine,
                    Arc::new(move |values_values| {
                        Box::pin(capture!([keys_values, k], async move {
                            // Create map from keys and values
                            let map_items = keys_values.into_iter().zip(values_values).collect();
                            k(Value(Map(map_items))).await;
                        }))
                    }),
                )
                .await;
            }))
        }),
    )
    .await;
}

/// Evaluates a fail expression.
///
/// # Parameters
/// * `msg` - The message expression to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
async fn evaluate_fail<G>(msg: Arc<Expr>, engine: Engine<G>, k: Continuation)
where
    G: Generator,
{
    msg.evaluate(
        engine,
        Arc::new(move |value| {
            Box::pin(capture!([k], async move {
                k(Value(Fail(value.into()))).await;
            }))
        }),
    )
    .await;
}
