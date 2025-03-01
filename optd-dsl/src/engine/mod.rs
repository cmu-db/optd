//! Interface to the Optimizer engine for rule and function evaluation.
//!
//! This module provides the primary interface for invoking optimization rules and functions
//! that are defined in the OPTD language. It handles the execution of rules against input
//! plans and manages the transformation of plans according to those rules.

use crate::{
    analyzer::hir::{AnnotatedValue, CoreData, Expr, Literal, Value, HIR},
    utils::context::Context,
};
use bridge::{from_optd::partial_logical_to_value, into_optd::value_to_partial_logical};
use futures::StreamExt;
use optd_core::cascades::ir::PartialLogicalPlan;
use std::sync::Arc;
use utils::{error::Error, streams::PartialLogicalPlanStream};

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

impl Engine {
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
        // Create a context with all expressions from the HIR
        let context = Context::new(
            self.hir
                .expressions
                .iter()
                .map(|(id, AnnotatedValue { value, .. })| (id.clone(), value.clone()))
                .collect(),
        );

        // Create a call expression to invoke the rule
        let call = Call(
            Box::new(CoreVal(Value(Literal(String(rule_name.to_string()))))),
            vec![CoreVal(partial_logical_to_value(&plan))],
        );

        // Evaluate the call and transform the results
        call.evaluate(context)
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
