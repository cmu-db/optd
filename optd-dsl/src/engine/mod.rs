use crate::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Literal, Value},
};
use eval::core::evaluate_core_expr;
use eval::expr::{
    evaluate_binary_expr, evaluate_function_call, evaluate_if_then_else, evaluate_let_binding,
    evaluate_reference, evaluate_unary_expr,
};
use eval::r#match::evaluate_pattern_match;
use std::sync::Arc;

mod eval;

mod generator;
pub use generator::Generator;

mod utils;
pub use utils::*;

#[cfg(test)]
mod test_utils;

/// The engine for evaluating HIR expressions and applying rules.
#[derive(Debug, Clone)]
pub struct Engine<G: Generator> {
    /// The original HIR context containing all defined expressions and rules
    pub(crate) context: Context,
    /// The expander for resolving group references
    pub(crate) generator: G,
}

impl<G: Generator> Engine<G> {
    /// Creates a new engine with the given context and expander.
    pub fn new(context: Context, generator: G) -> Self {
        Self { context, generator }
    }

    /// Creates a new engine with an updated context but the same expander.
    ///
    /// This is useful when you need to create a new engine with modifications to the context
    /// while preserving the original expander implementation.
    ///
    /// # Parameters
    ///  `context` - The new context to use
    ///
    /// # Returns
    /// A new engine with the provided context and the existing expander
    pub fn with_new_context(&self, context: Context) -> Self {
        Self {
            context,
            generator: self.clone().generator,
        }
    }

    /// Evaluates an expression and passes results to the provided continuation.
    ///
    /// # Parameters
    ///
    /// * `self` - The evaluation engine (owned)
    /// * `expr` - The expression to evaluate
    /// * `k` - The continuation to receive each evaluation result
    pub fn evaluate(
        self,
        expr: Arc<Expr>,
        k: EngineContinuation<Value>,
    ) -> impl Future<Output = ()> + Send {
        Box::pin(async move {
            match expr.as_ref() {
                Expr::PatternMatch(expr, match_arms) => {
                    evaluate_pattern_match(expr.clone(), match_arms.clone(), self, k).await
                }
                Expr::IfThenElse(cond, then_expr, else_expr) => {
                    evaluate_if_then_else(
                        cond.clone(),
                        then_expr.clone(),
                        else_expr.clone(),
                        self,
                        k,
                    )
                    .await
                }
                Expr::Let(ident, assignee, after) => {
                    evaluate_let_binding(ident.clone(), assignee.clone(), after.clone(), self, k)
                        .await
                }
                Expr::Binary(left, op, right) => {
                    evaluate_binary_expr(left.clone(), op.clone(), right.clone(), self, k).await
                }
                Expr::Unary(op, expr) => {
                    evaluate_unary_expr(op.clone(), expr.clone(), self, k).await
                }
                Expr::Call(fun, args) => {
                    evaluate_function_call(fun.clone(), args.clone(), self, k).await
                }
                Expr::Ref(ident) => evaluate_reference(ident.clone(), self, k).await,
                Expr::CoreExpr(expr) => evaluate_core_expr(expr.clone(), self, k).await,
                Expr::CoreVal(val) => k(val.clone()).await,
            }
        })
    }

    /// Launches a rule application with the given values and transformation, and passes everything
    /// to the continuation `k`.
    pub async fn launch_rule<T>(
        self,
        name: &str,
        values: Vec<Value>,
        transform: fn(&Value) -> T,
        k: EngineContinuation<T>,
    ) where
        T: 'static,
    {
        let rule_call = self.create_rule_call(name, values);

        self.evaluate(
            rule_call,
            Arc::new(move |result| {
                let k = k.clone();
                Box::pin(async move {
                    Self::process_result(result, transform, k).await;
                })
            }),
        )
        .await
    }

    /// Creates a rule call expression with the given name and arguments.
    ///
    /// # Parameters
    /// * `rule_name` - The name of the rule to call
    /// * `args` - The arguments to pass to the rule
    ///
    /// # Returns
    /// A call expression representing the rule invocation
    fn create_rule_call(&self, rule_name: &str, args: Vec<Value>) -> Arc<Expr> {
        let rule_name_expr = Expr::Ref(rule_name.to_string());
        let arg_exprs = args
            .into_iter()
            .map(|arg| Expr::CoreVal(arg).into())
            .collect();

        Expr::Call(rule_name_expr.into(), arg_exprs).into()
    }

    /// Helper function to process values and handle failures
    ///
    /// This abstracts the common pattern of handling failures and value transformation
    /// for all rule application functions.
    ///
    /// # Parameters
    /// * `value` - The value from rule evaluation
    /// * `transform` - Function to transform value to desired type
    /// * `context` - Context string for error messages
    /// * `k` - Continuation to call with transformed value on success
    async fn process_result<T, F>(
        value: Value,
        transform: F,
        k: Arc<dyn Fn(T) -> UnitFuture + Send + Sync + 'static>,
    ) where
        F: FnOnce(&Value) -> T,
    {
        match &value.0 {
            CoreData::Fail(boxed_msg) => {
                if let CoreData::Literal(Literal::String(error_message)) = &boxed_msg.0 {
                    eprintln!("Error processing result: {}", error_message);
                    // Don't call continuation for failed rules
                } else {
                    panic!("Fail expression must evaluate to a string message");
                }
            }
            _ => {
                // Transform and pass to continuation
                let transformed = transform(&value);
                k(transformed).await;
            }
        }
    }
}
