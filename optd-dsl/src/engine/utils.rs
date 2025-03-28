use futures::future::BoxFuture;

use crate::analyzer::hir::{Expr, Value};
use crate::capture;
use crate::engine::{Engine, generator::Generator};
use std::{future::Future, pin::Pin, sync::Arc};

/// A type alias for a future that completes with no return value.
pub type UnitFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub type PinnedFuture<T> = BoxFuture<'static, T>;

/// A type alias for continuations used in the rule engine.
///
/// The engine uses continuation-passing-style (CPS) since it requires advanced control flow to
/// expand (enumerate) expressions within a group.
pub type RealContinuation<I, O> = Arc<dyn Fn(I) -> PinnedFuture<O> + Send + Sync + 'static>;
pub type EngineContinuation<I> = RealContinuation<I, ()>;

/// Evaluates a sequence of expressions and collects their values using continuation passing style.
///
/// # Parameters
/// * `exprs` - The expressions to evaluate
/// * `engine` - The evaluation engine
/// * `k` - The continuation to receive all evaluated values
pub(super) fn evaluate_sequence<G>(
    exprs: Vec<Arc<Expr>>,
    engine: Engine<G>,
    k: EngineContinuation<Vec<Value>>,
) -> UnitFuture
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
    k: EngineContinuation<Vec<Value>>,
) -> UnitFuture
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
        engine
            .clone()
            .evaluate(
                expr,
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
