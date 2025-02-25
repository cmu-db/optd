use crate::{
    analyzer::hir::{AnnotatedValue, CoreData, Expr, Literal, Value, HIR},
    utils::context::Context,
};
use anyhow::Error;
use bridge::{from_optd::partial_logical_to_value, into_optd::value_to_partial_logical};
use optd_core::cascades::ir::PartialLogicalPlan;
use std::sync::Arc;

use CoreData::*;
use Expr::*;
use Literal::*;

mod bridge;
mod expr_eval;

/// The interpreter for evaluating HIR expressions
pub struct Interpreter {
    hir: Arc<HIR>,
}

impl Interpreter {
    pub fn new(hir: Arc<HIR>) -> Self {
        Self { hir }
    }

    /// Helper function to check results for failures and convert them to appropriate errors
    /// Returns the original results if no failures are found
    fn check_for_failures(&self, results: &[Value]) -> Result<(), Error> {
        if results.is_empty() {
            panic!("Interpreter returned no results");
        }

        let first_result = &results[0];
        match &first_result.0 {
            Fail(boxed_msg) => match &boxed_msg.0 {
                Literal(String(error_message)) => {
                    return Err(anyhow::anyhow!("{}", error_message));
                }
                _ => panic!("Fail expression must evaluate to a string message"),
            },
            _ => Ok(()),
        }
    }

    /// Interpret a function with the given name and input
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule
    pub async fn match_and_apply_logical_rule(
        &self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> Result<Vec<PartialLogicalPlan>, Error> {
        let mut context = Context::new(
            self.hir
                .expressions
                .iter()
                .map(|(id, AnnotatedValue { value, .. })| (id.clone(), value.clone()))
                .collect(),
        );

        let call = Call(
            CoreVal(Value(Literal(String(rule_name.to_string())))).into(),
            vec![CoreVal(partial_logical_to_value(&plan))],
        );

        let results = call.evaluate(&mut context).await?;

        self.check_for_failures(&results)?;

        Ok(results
            .iter()
            .map(|v| value_to_partial_logical(v))
            .collect())
    }
}
