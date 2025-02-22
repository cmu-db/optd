use crate::analyzer::hir::{AnnotatedValue, CoreData, Expr, HIR};
use context::Context;
use optd_core::cascades::ir::PartialLogicalPlan;
use optd_core::cascades::memo::Memoize;

mod context;
mod evaluator;

/// The interpreter for evaluating HIR expressions
pub struct Interpreter<'a, M: Memoize> {
    hir: &'a HIR,
    memo: &'a M,
}

impl<'a, M: Memoize> Interpreter<'a, M> {
    pub fn new(hir: &'a HIR, memo: &'a M) -> Self {
        Self { hir, memo }
    }

    /// Interpret a function with the given name and input
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule
    pub fn match_and_apply_logical_rule(
        &self,
        rule_name: &str,
        input: PartialLogicalPlan,
    ) -> Vec<PartialLogicalPlan> {
        // Create a context for evaluation
        let mut context = Context::new(
            self.hir
                .expressions
                .iter()
                .map(|(id, AnnotatedValue { value, .. })| (id.clone(), value.clone()))
                .collect(),
        );

        // Create input expression and call expression
        let input_expr = Expr::Core(CoreData::Logical(input));
        let call_expr = Expr::Call(Box::new(Expr::Ref(rule_name.to_string())), vec![input_expr]);

        // Evaluate the expression with our context and memo
        let result = call_expr.evaluate(&mut context, self.memo);

        // Extract the results
        match result.0 {
            CoreData::Array(transformations) => transformations
                .into_iter()
                .filter_map(|v| match v.0 {
                    CoreData::Logical(plan) => Some(plan),
                    _ => None,
                })
                .collect(),
            CoreData::Logical(single_plan) => {
                vec![single_plan]
            }
            _ => {
                unreachable!("Rule '{}' did not return valid transformations", rule_name)
            }
        }
    }
}
