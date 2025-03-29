use crate::analyzer::{
    context::Context,
    hir::{Expr, Goal, GroupId, Value},
};
use eval::core::evaluate_core_expr;
use eval::expr::{
    evaluate_binary_expr, evaluate_function_call, evaluate_if_then_else, evaluate_let_binding,
    evaluate_reference, evaluate_unary_expr,
};
use eval::r#match::evaluate_pattern_match;
use std::sync::Arc;

mod eval;

mod utils;
pub use utils::*;

#[cfg(test)]
mod test_utils;

/// The engine response type, which can be either a return value with a converter callback
/// or a yield group/goal with a continuation for further processing.
pub enum EngineResponse<O> {
    /// The engine has returned a value, and the final continuation
    /// should be called on it to produce the desired output.
    Return(Value, Continuation<Value, O>),
    /// The engine has yielded a group ID, and the continuation
    /// should be called for each value after expanding the group.
    YieldGroup(GroupId, Continuation<Value, EngineResponse<O>>),
    /// The engine has yielded a goal, and the continuation
    /// should be called for each value after expanding the goal.
    YieldGoal(Goal, Continuation<Value, EngineResponse<O>>),
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
        return_k: Continuation<Value, O>,
    ) -> EngineResponse<O>
    where
        O: Send + 'static,
    {
        let rule_call = self.create_rule_call(name, values);

        self.evaluate(
            rule_call,
            Arc::new(move |result| {
                let return_k = return_k.clone();
                Box::pin(async move { EngineResponse::Return(result, return_k) })
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
}
