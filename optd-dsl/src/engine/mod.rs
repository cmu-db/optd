use crate::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Goal, GroupId, Literal, Value},
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

mod utils;
pub use utils::*;

#[cfg(test)]
mod test_utils;

pub enum EngineResponse<O> {
    Return(Value, fn(&Value) -> O),
    YieldGroup(GroupId, Continuation<Value, EngineResponse<O>>),
    YieldGoal(Goal, Continuation<Value, EngineResponse<O>>),
    Fail(String),
}

/// The engine for evaluating HIR expressions and applying rules.
#[derive(Debug, Clone)]
pub struct Engine {
    /// The original HIR context containing all defined expressions and rules
    pub(crate) context: Context,
}

impl Engine {
    /// Creates a new engine with the given context and expander.
    pub fn new(context: Context) -> Self {
        Self { context }
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
        Self { context }
    }

    /// Evaluates an expression and passes results to the provided continuation.
    ///
    /// # Parameters
    ///
    /// * `self` - The evaluation engine (owned)
    /// * `expr` - The expression to evaluate
    /// * `k` - The continuation to receive each evaluation result
    pub fn evaluate<O>(
        self,
        expr: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> impl Future<Output = EngineResponse<O>> + Send
    where
        O: Send + 'static,
    {
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

    /// Launches a rule application with the given values and transformation.
    pub async fn launch_rule<O>(
        self,
        name: &str,
        values: Vec<Value>,
        transform: fn(&Value) -> O,
    ) -> EngineResponse<O>
    where
        O: Send + 'static,
    {
        let rule_call = self.create_rule_call(name, values);

        self.evaluate(
            rule_call,
            Arc::new(move |result| {
                Box::pin(async move {
                    match result {
                        Value(CoreData::Fail(boxed_value)) => match boxed_value.0 {
                            CoreData::Literal(Literal::String(msg)) => EngineResponse::Fail(msg),
                            _ => panic!("Expected string message in fail"),
                        },
                        value => EngineResponse::Return(value, transform),
                    }
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

    // /// Helper function to process values and handle failures
    // ///
    // /// This abstracts the common pattern of handling failures and value transformation
    // /// for all rule application functions.
    // ///
    // /// # Parameters
    // /// * `value` - The value from rule evaluation
    // /// * `transform` - Function to transform value to desired type
    // /// * `context` - Context string for error messages
    // /// * `k` - Continuation to call with transformed value on success
    // async fn process_result<T, F>(value: Value, transform: F) -> T
    // where
    //     F: FnOnce(&Value) -> T,
    // {
    //     match value.0 {
    //         CoreData::Fail(boxed_msg) => {
    //             if let CoreData::Literal(Literal::String(error_message)) = boxed_msg.0 {
    //                 Err(error_message)
    //             } else {
    //                 panic!("Fail expression must evaluate to a string message");
    //             }
    //         }
    //         _ => Ok(transform(&value)),
    //     }
    // }
}
