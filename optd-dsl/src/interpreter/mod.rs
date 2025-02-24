use context::Context;
use from_core::partial_logical_to_value;
use into_core::value_to_partial_logical;
use optd_core::cascades::{ir::PartialLogicalPlan, memo::Memoize};

use crate::analyzer::hir::{AnnotatedValue, CoreData, Expr, Literal, Value, HIR};

mod context;
mod evaluator;
mod from_core;
mod into_core;

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
        plan: PartialLogicalPlan,
    ) -> Vec<PartialLogicalPlan> {
        let mut context = Context::new(
            self.hir
                .expressions
                .iter()
                .map(|(id, AnnotatedValue { value, .. })| (id.clone(), value.clone()))
                .collect(),
        );

        let call = Expr::Call(
            Expr::CoreVal(Value(CoreData::Literal(Literal::String(
                rule_name.to_string(),
            ))))
            .into(),
            vec![Expr::CoreVal(partial_logical_to_value(&plan))],
        );

        call.evaluate(&mut context, self.memo)
            .iter()
            .map(|v| value_to_partial_logical(v))
            .collect()
    }
}
