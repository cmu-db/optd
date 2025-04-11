use crate::analyzer::hir::{Expr, Value};
use crate::capture;
use crate::engine::{Engine, EngineResponse};
use futures::future::BoxFuture;
use std::sync::Arc;

/// A type alias for continuations used in the rule engine.
///
/// The engine uses continuation-passing-style (CPS) since it requires advanced control flow to
/// expand (enumerate) expressions within a group.
pub type Continuation<I, O> = Arc<dyn Fn(I) -> BoxFuture<'static, O> + Send + Sync>;

impl<O: Clone + Send + 'static> Engine<O> {
    /// Evaluates a sequence of expressions and collects their values using continuation passing style.
    ///
    /// # Parameters
    /// * `exprs` - The expressions to evaluate
    /// * `k` - The continuation to receive all evaluated values
    pub(crate) fn evaluate_sequence(
        self,
        exprs: Vec<Arc<Expr>>,
        k: Continuation<Vec<Value>, EngineResponse<O>>,
    ) -> BoxFuture<'static, EngineResponse<O>> {
        let exprs_len = exprs.len();
        self.evaluate_sequence_internal(exprs, 0, Vec::with_capacity(exprs_len), k)
    }

    fn evaluate_sequence_internal(
        self,
        exprs: Vec<Arc<Expr>>,
        index: usize,
        values: Vec<Value>,
        k: Continuation<Vec<Value>, EngineResponse<O>>,
    ) -> BoxFuture<'static, EngineResponse<O>> {
        Box::pin(async move {
            if index >= exprs.len() {
                return k(values).await;
            }

            let expr = exprs[index].clone();
            self.clone()
                .evaluate(
                    expr,
                    Arc::new(move |expr_value| {
                        let mut next_values = values.clone();
                        next_values.push(expr_value);
                        let engine = self.clone();

                        Box::pin(capture!([exprs, index, k], async move {
                            engine
                                .evaluate_sequence_internal(exprs, index + 1, next_values, k)
                                .await
                        }))
                    }),
                )
                .await
        })
    }
}
