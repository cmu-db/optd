//! Interface to the Optimizer engine for rule and function evaluation.
//!
//! This module provides the primary interface for invoking optimization rules and functions
//! that are defined in the OPTD language. It handles the execution of rules against input
//! plans and manages the transformation of plans according to those rules.

use crate::{
    driver::{cascades::Driver, memo::Memoize},
    ir::plans::PartialLogicalPlan,
};
// use bridge::{from_optd::partial_logical_to_value, into_optd::value_to_partial_logical};
use eval::Evaluate;
use futures::StreamExt;
use optd_dsl::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Literal, Value},
};
use std::sync::Arc;
use utils::{error::Error, streams::{PartialLogicalPlanStream, PartialPhysicalPlanStream}};
use CoreData::*;
use Expr::*;
use Literal::*;

mod bridge;
mod eval;
mod utils;

/// The engine for evaluating HIR expressions and applying rules.

pub struct Engine<M: Memoize> {
    /// The original HIR context containing all defined expressions and rules
    context: Context,

    /// The optimization driver instance
    driver: Arc<Driver<M>>,
}
impl<M: Memoize> Engine<M> {
    pub fn new(context: Context, driver: Arc<Driver<M>>) -> Self {
        Self { context, driver }
    }
    pub async fn match_and_apply_logical_rule(
        &self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> PartialLogicalPlanStream {
        todo!()
    }

    pub async fn match_and_apply_implementation_rule(
        &self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> PartialPhysicalPlanStream {
        todo!()
    }
}

/*impl<M: Memoize> Engine<M> {
    /// Creates a new engine with the given context and driver.
    pub fn new(context: Context, driver: Arc<Driver<M>>) -> Self {
        Self { context, driver }
    }

    /// Interprets a function with the given name and input.
    ///
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule.
    pub async fn match_and_apply_logical_rule(
        &self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> PartialLogicalPlanStream {
        // Create a call expression to invoke the rule
        let call = Call(
            CoreVal(Value(Literal(String(rule_name.to_string())))).into(),
            vec![CoreVal(partial_logical_to_value(&plan))],
        );

        // Evaluate the call and transform the results
        call.evaluate(self.context)
            .map(|result| {
                result.and_then(|value| match &value.0 {
                    Fail(boxed_msg) => match &boxed_msg.0 {
                        Literal(String(error_message)) => Err(Error::Fail(error_message.clone())),
                        _ => panic!("Fail expression must evaluate to a string message"),
                    },
                    _ => Ok(value_to_partial_logical(&value)),
                })
            })
            .boxed()
    }
}
*/
