use super::{Engine, Evaluate, Generator};
use crate::capture;
use crate::engine::generator::Continuation;
use optd_dsl::analyzer::hir::{CoreData, Expr, Value};
use std::pin::Pin;
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
        Array(ref items) => {
            evaluate_collection(items.clone(), data, engine, k).await;
        }
        Tuple(ref items) => {
            evaluate_collection(items.clone(), data, engine, k).await;
        }
        Struct(_, ref items) => {
            evaluate_collection(items.clone(), data, engine, k).await;
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
            // evaluate_logical_operator(op, engine, k).await;
        }
        Physical(op) => {
            // evaluate_physical_operator(op, engine, k).await;
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
/// * `data` - The original collection type (for reconstruction)
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive evaluation results
async fn evaluate_collection<G>(
    items: Vec<Arc<Expr>>,
    data: CoreData<Arc<Expr>>,
    engine: Engine<G>,
    k: Continuation,
) where
    G: Generator,
{
    let length = items.len();

    // Use a helper function to evaluate all items sequentially
    evaluate_items_sequentially(
        items,
        0,
        Vec::with_capacity(length),
        engine,
        Arc::new(move |values_array| {
            Box::pin(capture!([data, k], async move {
                // Extract the actual values from the Array
                if let CoreData::Array(values) = values_array.0 {
                    // Convert to the appropriate collection type
                    let result = match &data {
                        Array(_) => Value(Array(values)),
                        Tuple(_) => Value(Tuple(values)),
                        Struct(name, _) => Value(Struct(name.clone(), values)),
                        _ => unreachable!("Unexpected collection type"),
                    };

                    // Pass the result to the continuation
                    k(result).await;
                } else {
                    panic!("Expected Array of values from evaluate_items_sequentially");
                }
            }))
        }),
    )
    .await;
}

/// Helper function to evaluate collection items sequentially.
///
/// # Parameters
/// * `items` - The items to evaluate
/// * `index` - The current item index
/// * `values` - Accumulated item values
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive all evaluated item values
fn evaluate_items_sequentially<G>(
    items: Vec<Arc<Expr>>,
    index: usize,
    values: Vec<Value>,
    engine: Engine<G>,
    k: Continuation,
) -> Pin<Box<dyn futures::Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        if index >= items.len() {
            // All items evaluated, call continuation
            k(Value(Array(values))).await;
            return;
        }

        // Evaluate the current item
        let item = items[index].clone();

        item.evaluate(
            engine.clone(),
            Arc::new(move |item_value| {
                let mut next_values = values.clone();
                next_values.push(item_value);

                Box::pin(capture!([items, engine, k], async move {
                    // Continue with the next item
                    evaluate_items_sequentially(items, index + 1, next_values, engine, k).await;
                }))
            }),
        )
        .await;
    })
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
    // Helper function to evaluate values once keys are evaluated
    async fn evaluate_values<G>(
        keys: Vec<Value>,
        values_exprs: Vec<Arc<Expr>>,
        engine: Engine<G>,
        k: Continuation,
    ) where
        G: Generator,
    {
        let values_length = values_exprs.len();

        // Evaluate value expressions
        evaluate_items_sequentially(
            values_exprs,
            0,
            Vec::with_capacity(values_length),
            engine,
            Arc::new(move |values_result| {
                Box::pin(capture!([keys, k], async move {
                    if let CoreData::Array(values) = values_result.0 {
                        // Create map from keys and values
                        let map_items = keys.iter().cloned().zip(values).collect();
                        k(Value(Map(map_items))).await;
                    } else {
                        panic!("Expected Array of values");
                    }
                }))
            }),
        )
        .await;
    }

    // Extract keys and values using iterators
    let (keys, values): (Vec<Arc<Expr>>, Vec<Arc<Expr>>) = items.into_iter().unzip();
    let keys_length = keys.len();

    // First evaluate all key expressions
    evaluate_items_sequentially(
        keys,
        0,
        Vec::with_capacity(keys_length),
        engine.clone(),
        Arc::new(move |keys_result| {
            Box::pin(capture!([values, engine, k], async move {
                if let CoreData::Array(keys) = keys_result.0 {
                    // Then evaluate all value expressions
                    evaluate_values(keys, values, engine, k).await;
                } else {
                    panic!("Expected Array of keys");
                }
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
