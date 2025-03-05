//! This module provides stream-based utilities for evaluating expressions in a non-blocking,
//! asynchronous manner. It includes tools for handling success and error cases consistently,
//! computing all possible combinations of expression values, and transforming Result types within
//! stream processing pipelines.

use crate::engine::eval::Evaluate;
use crate::engine::expander::Expander;
use crate::engine::Engine;
use crate::error::Error;
use crate::ir::plans::PartialPhysicalPlan;
use crate::{capture, ir::plans::PartialLogicalPlan};
use futures::{stream, Stream, StreamExt};
use optd_dsl::analyzer::hir::{Expr, Value};
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for a stream of evaluation results.
///
/// This represents a stream that yields Result<Value, Error> items,
/// allowing for asynchronous processing of values that might fail with errors.
pub(crate) type ValueStream = Pin<Box<dyn Stream<Item = Result<Value, Error>> + Send>>;

/// Type alias for a stream of vector evaluation results.
///
/// This represents a stream that yields Result<Vec<Value>, Error> items,
/// which is useful for representing collections of values from multiple expressions.
pub(crate) type VecValueStream = Pin<Box<dyn Stream<Item = Result<Vec<Value>, Error>> + Send>>;

/// Type alias for a stream of partial logical plans.
///
/// This represents a stream that yields Result<PartialLogicalPlan, Error> items,
/// which is used for returning multiple possible transformations of a plan.
pub(crate) type PartialLogicalPlanStream =
    Pin<Box<dyn Stream<Item = Result<PartialLogicalPlan, Error>> + Send>>;

/// Type alias for a stream of partial physical plans.
///
/// This represents a stream that yields Result<PartialPhysicalPlan, Error> items,
/// which is used for returning multiple possible transformations of a plan.
pub(crate) type PartialPhysicalPlanStream =
    Pin<Box<dyn Stream<Item = Result<PartialPhysicalPlan, Error>> + Send>>;

/// Generates a stream of all possible value combinations from a series of expressions.
///
/// This implementation uses a recursive approach that processes expressions
/// one at a time, avoiding materializing the entire vector upfront.
///
/// # How it works
///
/// For expressions [A, B, C]:
///
/// ```text
///                ┌────────────┐
///                │ Evaluate A │
///                └─────┬──────┘
///                      │
///        ┌─────────────┴─────────────┐
///        │                           │
///        ▼                           ▼
///   ┌─────────┐                 ┌─────────┐
///   │Result a1│                 │Result a2│
///   └────┬────┘                 └────┬────┘
///        │                           │
///        ▼                           ▼
/// ┌─────────────┐             ┌─────────────┐
/// │ Evaluate BC │             │ Evaluate BC │
/// └──────┬──────┘             └──────┬──────┘
///        │                           │
///        ▼                           ▼
/// ┌──────────────┐            ┌──────────────┐
/// │Combine a1 w/ │            │ Combine a2 w/│
/// │  BC results  │            │  BC results  │
/// └──────────────┘            └──────────────┘
/// ```
///
/// This recursion generates all combinations without storing intermediate results.
///
/// # Type Parameters
/// * E - The type that implements Expander
/// * I - The type of the iterator over expressions
///
/// # Parameters
/// * items - Iterator over expressions to evaluate
/// * engine - The evaluation engine
///
/// # Returns
/// A stream of all possible combinations of values from the expressions
pub(crate) fn evaluate_all_combinations<E, I>(mut items: I, engine: Engine<E>) -> VecValueStream
where
    E: Expander,
    I: Iterator<Item = Arc<Expr>> + Send + Clone + 'static,
{
    match items.next() {
        // Base case: no expressions
        None => propagate_success(vec![]),

        // Single expression case
        Some(expr) => {
            if items.clone().next().is_none() {
                // Only one expression, return its value in a vector
                expr.evaluate(engine)
                    .map(|result| result.map(|val| vec![val]))
                    .boxed()
            } else {
                // Multiple expressions: process recursively with a fold-like approach
                expr.evaluate(engine.clone())
                    .flat_map(move |result| {
                        stream_from_result(
                            result,
                            capture!([engine, items], move |value| {
                                // Recursively evaluate the remaining expressions
                                // and combine with the current value
                                evaluate_all_combinations(items.clone(), engine).map(
                                    move |rest_result| {
                                        rest_result.map(|mut rest_values| {
                                            let mut result =
                                                Vec::with_capacity(rest_values.len() + 1);
                                            result.push(value.clone());
                                            result.append(&mut rest_values);
                                            result
                                        })
                                    },
                                )
                            }),
                        )
                    })
                    .boxed()
            }
        }
    }
}

/// Creates a stream that produces a single error result.
///
/// This function is used to convert an error into a stream that emits just that error.
/// It's useful for propagating errors through stream-based pipelines.
///
/// # Parameters
/// * e - The error to propagate
///
/// # Returns
/// A boxed stream that yields a single Err result
pub(crate) fn propagate_error<T>(e: Error) -> Pin<Box<dyn Stream<Item = Result<T, Error>> + Send>>
where
    T: Send,
{
    stream::once(async move { Err(e) }).boxed()
}

/// Creates a stream that produces a single success result.
///
/// This function is used to convert a value into a stream that emits just that value.
/// It's useful for returning simple values through stream-based interfaces.
///
/// # Parameters
/// * value - The value to emit in the stream
///
/// # Returns
/// A boxed stream that yields a single Ok result
pub(crate) fn propagate_success<T>(value: T) -> Pin<Box<dyn Stream<Item = Result<T, Error>> + Send>>
where
    T: Send + 'static,
{
    stream::once(async move { Ok(value) }).boxed()
}

/// Transforms a Result into a stream by applying a handler to success values
/// or propagating errors.
///
/// This function provides a consistent way to handle Result types in stream transformations.
/// It applies the success_handler to Ok values and propagates errors using propagate_error.
///
/// # Type Parameters
/// * T - The type inside the Ok variant of the input Result
/// * U - The type inside the Ok variant of the output Result
/// * F - The type of the success handler function
/// * Fut - The stream type returned by the success handler
///
/// # Parameters
/// * result - The Result to process
/// * success_handler - Function to apply to the Ok value, which returns a stream
///
/// # Returns
/// A boxed stream that either contains the results of the success handler or propagates the error
pub(crate) fn stream_from_result<'a, T, U, F, Fut>(
    result: Result<T, Error>,
    success_handler: F,
) -> Pin<Box<dyn Stream<Item = Result<U, Error>> + Send + 'a>>
where
    T: Send,
    U: Send,
    F: FnOnce(T) -> Fut + Send,
    Fut: Stream<Item = Result<U, Error>> + Send + 'a,
{
    match result {
        Ok(value) => Box::pin(success_handler(value)),
        Err(e) => propagate_error(e),
    }
}
