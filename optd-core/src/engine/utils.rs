use super::eval::Evaluate;
use crate::capture;
use crate::engine::{generator::Generator, Engine};
use optd_dsl::analyzer::hir::{Expr, Value};
use std::{future::Future, pin::Pin, sync::Arc};

/// A macro that automatically clones variables before they're captured by a closure.
///
/// This macro takes a list of variables to clone and a closure expression.
/// It clones all the specified variables *before* creating the closure,
/// so the closure captures the clones rather than the originals.
///
/// # Examples
///
/// ```ignore
/// let tag = String::from("tag");
/// let context = Context::new();
///
/// // Without the macro, you would manually clone before the closure:
/// let tag_clone = tag.clone();
/// let context_clone = context.clone();
/// some_function(move |x| {
/// // Use tag_clone and context_clone
/// process(tag_clone, context_clone, x);
/// });
///
/// // With the capture! macro:
/// some_function(capture!([tag, context], move |x| {
/// // The closure receives already-cloned variables
/// process(tag, context, x);
/// }));
/// ```
///
/// This is particularly useful in async code with many nested closures
/// that need to capture the same variables.
#[macro_export]
macro_rules! capture {
    ([$($var:ident),* $(,)?], $($closure:tt)*) => {
        {
            $(let $var = $var.clone();)*
            $($closure)*
        }
    };
}

/// Specialized continuation type for vectors of values
type ValueSequenceContinuation =
    Arc<dyn Fn(Vec<Value>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static>;

/// Public function to evaluate a sequence of expressions and collect their values.
///
/// # Parameters
/// * `exprs` - The expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive all evaluated values
pub(super) fn evaluate_sequence<G>(
    exprs: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: ValueSequenceContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    let exprs_len = exprs.len();
    evaluate_sequence_internal(exprs, 0, Vec::with_capacity(exprs_len), engine, k)
}

/// Internal implementation for sequential evaluation.
///
/// # Parameters
/// * `exprs` - The expressions to evaluate
/// * `index` - The current expression index
/// * `values` - Accumulated expression values
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive all evaluated values
fn evaluate_sequence_internal<G>(
    exprs: Vec<Arc<Expr>>,
    index: usize,
    values: Vec<Value>,
    engine: Engine<G>,
    k: ValueSequenceContinuation,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    G: Generator,
{
    Box::pin(async move {
        if index >= exprs.len() {
            // All expressions evaluated, call continuation with the vector
            k(values).await;
            return;
        }

        // Evaluate the current expression
        let expr = exprs[index].clone();
        expr.evaluate(
            engine.clone(),
            Arc::new(move |expr_value| {
                let mut next_values = values.clone();
                next_values.push(expr_value);

                Box::pin(capture!([exprs, index, engine, k], async move {
                    evaluate_sequence_internal(exprs, index + 1, next_values, engine, k).await;
                }))
            }),
        )
        .await;
    })
}
