use crate::analyzer::{
    context::Context,
    hir::{Expr, ExprKind, Goal, GroupId, Value},
};
use std::sync::Arc;

mod eval;

mod utils;
pub use utils::*;

/// The engine response type, which can be either a return value with a converter callback
/// or a yielded group/goal with a continuation for further processing.
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
#[derive(Clone)]
pub struct Engine<O: Clone + Send + 'static> {
    /// The original HIR context containing all defined expressions and rules.
    pub(crate) context: Context,
    /// The continuation to return from the current function scope.
    pub(crate) fun_return: Option<Continuation<Value, EngineResponse<O>>>,
}

impl<O: Clone + Send + 'static> Engine<O> {
    /// Creates a new engine with the given context and expander.
    pub fn new(context: Context) -> Self {
        Self {
            context,
            fun_return: None,
        }
    }

    /// Creates a new engine with an updated context.
    pub fn with_new_context(self, context: Context) -> Self {
        Self {
            context,
            fun_return: self.fun_return,
        }
    }

    /// Creates a new engine with an updated function return continuation.
    pub fn with_new_return(self, fun_return: Continuation<Value, EngineResponse<O>>) -> Self {
        Self {
            context: self.context,
            fun_return: Some(fun_return),
        }
    }

    /// Evaluates an expression and passes results to the provided continuation.
    ///
    /// # Parameters
    ///
    /// * `self` - The evaluation engine (owned).
    /// * `expr` - The expression to evaluate.
    /// * `k` - The continuation to receive each evaluation result.
    pub fn evaluate(
        self,
        expr: Arc<Expr>,
        k: Continuation<Value, EngineResponse<O>>,
    ) -> impl Future<Output = EngineResponse<O>> + Send {
        use ExprKind::*;

        Box::pin(async move {
            match &expr.as_ref().kind {
                PatternMatch(sub_expr, match_arms) => {
                    self.evaluate_pattern_match(sub_expr.clone(), match_arms.clone(), k)
                        .await
                }
                IfThenElse(cond, then_expr, else_expr) => {
                    self.evaluate_if_then_else(
                        cond.clone(),
                        then_expr.clone(),
                        else_expr.clone(),
                        k,
                    )
                    .await
                }
                NewScope(expr) => self.evaluate_new_scope(expr.clone(), k).await,
                Let(binding, after) => {
                    self.evaluate_let_binding(binding.clone(), after.clone(), k)
                        .await
                }
                Binary(left, op, right) => {
                    self.evaluate_binary_expr(left.clone(), op.clone(), right.clone(), k)
                        .await
                }
                Unary(op, expr) => self.evaluate_unary_expr(op.clone(), expr.clone(), k).await,
                Call(fun, args) => self.evaluate_call(fun.clone(), args.clone(), k).await,
                Map(map) => self.evaluate_map(map.clone(), k).await,
                Ref(ident) => self.evaluate_reference(ident.clone(), k).await,
                Return(expr) => self.evaluate_return(expr.clone()).await,
                FieldAccess(_, _) => panic!("Field should have been transformed to a call"),
                CoreExpr(expr) => self.evaluate_core_expr(expr.clone(), k).await,
                CoreVal(val) => k(val.clone()).await,
            }
        })
    }

    /// Launches a rule application with the given values and transformation.
    ///
    /// # Parameters
    /// * `self` - The evaluation engine (owned).
    /// * `name` - The name of the rule to apply.
    /// * `values` - The values to pass to the rule.
    /// * `return_k` - The continuation to receive the result of the rule application.
    ///
    /// # Returns
    /// The result of the rule application.
    pub async fn launch_rule(
        self,
        name: &str,
        values: Vec<Value>,
        return_k: Continuation<Value, O>,
    ) -> EngineResponse<O> {
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
        use ExprKind::*;

        let rule_name_expr = Expr::new(Ref(rule_name.to_string())).into();
        let arg_exprs = args
            .into_iter()
            .map(|arg| Expr::new(CoreVal(arg)).into())
            .collect();

        Expr::new(Call(rule_name_expr, arg_exprs)).into()
    }
}
