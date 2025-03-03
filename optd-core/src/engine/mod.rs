//! Interface to the Optimizer engine for rule and function evaluation.
//!
//! This module provides the primary interface for invoking optimization rules and functions
//! that are defined in the OPTD language. It handles the execution of rules against input
//! plans and manages the transformation of plans according to those rules.

use crate::ir::{plans::PartialLogicalPlan, properties::PhysicalProperties};
use bridge::{from::partial_logical_to_value, into::value_to_partial_logical};
use eval::Evaluate;
use expander::Expander;
use futures::StreamExt;
use optd_dsl::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Literal, Value},
};
use std::sync::Arc;
use utils::{
    error::Error,
    streams::{PartialLogicalPlanStream, PartialPhysicalPlanStream},
};
use CoreData::*;
use Expr::*;
use Literal::*;

mod bridge;
mod eval;
pub mod expander;
mod utils;

/// The engine for evaluating HIR expressions and applying rules.
#[derive(Debug, Clone)]
pub(crate) struct Engine<E: Expander> {
    /// The original HIR context containing all defined expressions and rules
    pub(crate) context: Context,
    /// The expander for resolving group references
    pub(crate) expander: E,
}

impl<E: Expander> Engine<E> {
    /// Creates a new engine with the given context and expander.
    pub fn new(context: Context, expander: E) -> Self {
        Self { context, expander }
    }

    /// Creates a new engine with an updated context but the same expandable.
    ///
    /// This is useful when you need to create a new engine with modifications to the context
    /// while preserving the original expandable implementation.
    ///
    /// # Parameters
    /// * `context` - The new context to use
    ///
    /// # Returns
    /// A new engine with the provided context and the existing expandable
    pub fn with_context(self, context: Context) -> Self {
        Self {
            context,
            expander: self.expander,
        }
    }

    /// Interprets a function with the given name and input.
    ///
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule.
    pub async fn match_and_apply_logical_rule(
        self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> PartialLogicalPlanStream {
        // Create a call expression to invoke the rule
        let call = Arc::new(Call(
            CoreVal(Value(Literal(String(rule_name.to_string())))).into(),
            vec![CoreVal(partial_logical_to_value(&plan)).into()],
        ));

        // Evaluate the call and transform the results
        call.evaluate(self)
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

    pub async fn match_and_apply_implementation_rule(
        self,
        _rule_name: &str,
        _plan: PartialLogicalPlan,
        _props: &PhysicalProperties,
    ) -> PartialPhysicalPlanStream {
        todo!()
    }
}
