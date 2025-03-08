//! Interface to the Optimizer engine for rule and function evaluation.
//!
//! This module provides the primary interface for invoking optimization rules and functions
//! defined in the OPTD language. It handles the execution of rules against input
//! plans and manages the transformation of plans according to those rules.

use crate::{
    error::Error,
    cir::{
        plans::PartialLogicalPlan,
        properties::{LogicalProperties, PhysicalProperties},
    },
};
use bridge::{
    from_cir::{partial_logical_to_value, physical_properties_to_value},
    into_cir::{value_to_logical_properties, value_to_partial_logical, value_to_partial_physical},
};
use error::EngineError;
use eval::Evaluate;
use expander::Expander;
use futures::StreamExt;
use optd_dsl::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Literal, Value},
};
use std::sync::Arc;
use utils::streams::{PartialLogicalPlanStream, PartialPhysicalPlanStream};
use EngineError::*;
use Error::*;
use Expr::*;
use Literal::*;

mod bridge;
pub(crate) mod error;
mod eval;
pub mod expander;
pub(crate) mod utils;

/// Result type for rule applications
type RuleResult<T> = Result<T, Error>;

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
    pub(crate) fn new(context: Context, expander: E) -> Self {
        Self { context, expander }
    }

    /// Creates a new engine with an updated context but the same expander.
    ///
    /// This is useful when you need to create a new engine with modifications to the context
    /// while preserving the original expander implementation.
    ///
    /// # Parameters
    /// * `context` - The new context to use
    ///
    /// # Returns
    /// A new engine with the provided context and the existing expander
    pub(crate) fn with_context(self, context: Context) -> Self {
        Self {
            context,
            expander: self.expander,
        }
    }

    /// Interprets a function with the given name and input.
    ///
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule.
    ///
    /// # Parameters
    /// * `rule_name` - The name of the rule to apply
    /// * `plan` - The logical plan to transform
    ///
    /// # Returns
    /// A stream of possible transformed logical plans
    pub(crate) fn match_and_apply_logical_rule(
        self,
        rule_name: &str,
        plan: &PartialLogicalPlan,
    ) -> PartialLogicalPlanStream {
        let rule_call = self.create_rule_call(rule_name, vec![partial_logical_to_value(plan)]);

        rule_call
            .evaluate(self)
            .map(move |result| Self::process_rule_result(result, value_to_partial_logical))
            .boxed()
    }

    /// Interprets a function with the given name and inputs.
    ///
    /// This applies an implementation rule to an input logical plan and required physical properties,
    /// returning all possible physical implementations according to the rule.
    ///
    /// # Parameters
    /// * `rule_name` - The name of the rule to apply
    /// * `plan` - The logical plan to transform
    /// * `props` - The physical properties required for the implementation
    ///
    /// # Returns
    /// A stream of possible physical plan implementations
    pub(crate) fn match_and_apply_implementation_rule(
        self,
        rule_name: &str,
        plan: &PartialLogicalPlan,
        props: &PhysicalProperties,
    ) -> PartialPhysicalPlanStream {
        let plan_value = partial_logical_to_value(plan);
        let props_value = physical_properties_to_value(&props);

        let rule_call = self.create_rule_call(rule_name, vec![plan_value, props_value]);

        rule_call
            .evaluate(self)
            .map(|result| Self::process_rule_result(result, value_to_partial_physical))
            .boxed()
    }

    /// Derives logical properties for a given logical plan.
    ///
    /// This calls the reserved "derive" function of the DSL to compute
    /// the logical properties for a given logical plan. These properties are
    /// used for various optimization decisions like cardinality estimation,
    /// schema inference, and cost calculations.
    ///
    /// # Parameters
    /// * `plan` - The logical plan to derive properties for
    ///
    /// # Returns
    /// The logical properties derived from the plan
    pub(crate) async fn derive_properties(
        self,
        plan: &PartialLogicalPlan,
    ) -> Result<LogicalProperties, Error> {
        // Create a call to the reserved "derive" function
        let rule_call = self.create_rule_call("derive", vec![partial_logical_to_value(plan)]);

        // Evaluate the rule and transform the result into logical properties
        let result = rule_call
            .evaluate(self)
            .next()
            .await
            .ok_or(Engine(NoResult))?;

        // Process the result and transform to logical properties
        Self::process_rule_result(result, value_to_logical_properties)
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
        let rule_name_expr = Ref(rule_name.to_string());
        let arg_exprs = args.into_iter().map(|arg| CoreVal(arg).into()).collect();

        Call(rule_name_expr.into(), arg_exprs).into()
    }

    /// Processes the result of a rule evaluation.
    ///
    /// # Parameters
    /// * `result` - The result of evaluating a rule
    /// * `transform` - A function to transform the result value to the desired output type
    ///
    /// # Returns
    /// Either a transformed value or an error
    fn process_rule_result<T, F>(result: Result<Value, Error>, transform: F) -> RuleResult<T>
    where
        F: FnOnce(&Value) -> T,
    {
        result.and_then(|value| match &value.0 {
            CoreData::Fail(boxed_msg) => match &boxed_msg.0 {
                CoreData::Literal(String(error_message)) => {
                    Err(Engine(Fail(error_message.clone())))
                }
                _ => panic!("Fail expression must evaluate to a string message"),
            },
            _ => Ok(transform(&value)),
        })
    }
}
