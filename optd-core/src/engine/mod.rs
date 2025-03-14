use crate::{
    bridge::{
        from_cir::{
            partial_logical_to_value, partial_physical_to_value, physical_properties_to_value,
        },
        into_cir::{
            value_to_cost, value_to_logical_properties, value_to_partial_logical,
            value_to_partial_physical,
        },
    },
    capture,
    cir::{
        goal::Cost,
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
        properties::{LogicalProperties, PhysicalProperties},
    },
};
use eval::Evaluate;
use generator::Generator;
use optd_dsl::analyzer::{
    context::Context,
    hir::{CoreData, Expr, Literal, Value},
};
use std::sync::Arc;
use utils::UnitFuture;
use Expr::*;

mod eval;
pub(crate) mod generator;
#[cfg(test)]
pub(super) mod test_utils;
pub(crate) mod utils;

/// Type alias for a continuation that receives a PartialLogicalPlan
pub(crate) type LogicalPlanContinuation =
    Arc<dyn Fn(PartialLogicalPlan) -> UnitFuture + Send + Sync + 'static>;

/// Type alias for a continuation that receives a PartialPhysicalPlan
pub(crate) type PhysicalPlanContinuation =
    Arc<dyn Fn(PartialPhysicalPlan) -> UnitFuture + Send + Sync + 'static>;

/// Type alias for a continuation that receives a cost value
pub(crate) type CostContinuation = Arc<dyn Fn(Cost) -> UnitFuture + Send + Sync + 'static>;

/// Type alias for a continuation that receives LogicalProperties
pub(crate) type PropertiesContinuation =
    Arc<dyn Fn(LogicalProperties) -> UnitFuture + Send + Sync + 'static>;

/// The engine for evaluating HIR expressions and applying rules.
#[derive(Debug, Clone)]
pub(crate) struct Engine<G: Generator> {
    /// The original HIR context containing all defined expressions and rules
    pub(crate) context: Context,
    /// The expander for resolving group references
    pub(crate) generator: G,
}

impl<E: Generator> Engine<E> {
    /// Creates a new engine with the given context and expander.
    pub(crate) fn new(context: Context, generator: E) -> Self {
        Self { context, generator }
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
            generator: self.generator,
        }
    }

    /// Launches a logical rule application for a given plan.
    ///
    /// This applies a logical rule to an input plan and passes all possible
    /// transformations of the plan to the continuation.
    ///
    /// # Parameters
    /// * `rule_name` - The name of the rule to apply
    /// * `plan` - The logical plan to transform
    /// * `k` - The continuation to receive transformed logical plans
    pub(crate) fn launch_logical_rule(
        self,
        rule_name: String,
        plan: &PartialLogicalPlan,
        k: LogicalPlanContinuation,
    ) -> UnitFuture {
        let rule_call = self.create_rule_call(&rule_name, vec![partial_logical_to_value(plan)]);

        Box::pin(async move {
            rule_call
                .evaluate(
                    self,
                    Arc::new(move |result| {
                        Box::pin(capture!([k, rule_name], async move {
                            Self::process_result(
                                result,
                                value_to_partial_logical,
                                &format!("logical rule '{}'", rule_name),
                                k,
                            )
                            .await;
                        }))
                    }),
                )
                .await;
        })
    }

    /// Launches an implementation rule application for a given plan and properties.
    ///
    /// This applies an implementation rule to an input logical plan and required physical
    /// properties, passing all possible physical implementations to the continuation.
    ///
    /// # Parameters
    /// * `rule_name` - The name of the rule to apply
    /// * `plan` - The logical plan to transform
    /// * `props` - The physical properties required for the implementation
    /// * `k` - The continuation to receive physical plan implementations
    pub(crate) fn launch_implementation_rule(
        self,
        rule_name: String,
        plan: &PartialLogicalPlan,
        props: &PhysicalProperties,
        k: PhysicalPlanContinuation,
    ) -> UnitFuture {
        let plan_value = partial_logical_to_value(plan);
        let props_value = physical_properties_to_value(props);

        let rule_call = self.create_rule_call(&rule_name, vec![plan_value, props_value]);

        Box::pin(async move {
            rule_call
                .evaluate(
                    self,
                    Arc::new(move |result| {
                        Box::pin(capture!([k, rule_name], async move {
                            Self::process_result(
                                result,
                                value_to_partial_physical,
                                &format!("implementation rule '{}'", rule_name),
                                k,
                            )
                            .await;
                        }))
                    }),
                )
                .await;
        })
    }

    /// Evaluates the cost of a physical plan.
    ///
    /// This calls the reserved "cost" function of the DSL to compute the cost
    /// of a given physical plan, passing results to the continuation.
    ///
    /// # Parameters
    /// * `plan` - The physical plan to evaluate the cost for
    /// * `k` - The continuation to receive cost values
    pub(crate) fn launch_cost_plan(
        self,
        plan: &PartialPhysicalPlan,
        k: CostContinuation,
    ) -> UnitFuture {
        // Create a call to the reserved "cost" function
        let rule_call = self.create_rule_call("cost", vec![partial_physical_to_value(plan)]);

        Box::pin(async move {
            rule_call
                .evaluate(
                    self,
                    Arc::new(move |result| {
                        Box::pin(capture!([k], async move {
                            Self::process_result(result, value_to_cost, "cost function", k).await;
                        }))
                    }),
                )
                .await;
        })
    }

    /// Derives logical properties for a given logical plan.
    ///
    /// This calls the reserved "derive" function of the DSL to compute
    /// the logical properties for a given logical plan and passes them to the continuation.
    ///
    /// # Parameters
    /// * `plan` - The logical plan to derive properties for
    /// * `k` - The continuation to receive the logical properties
    pub(crate) fn launch_derive_properties(
        self,
        plan: &PartialLogicalPlan,
        k: PropertiesContinuation,
    ) -> UnitFuture {
        // Create a call to the reserved "derive" function
        let rule_call = self.create_rule_call("derive", vec![partial_logical_to_value(plan)]);

        Box::pin(async move {
            rule_call
                .evaluate(
                    self,
                    Arc::new(move |result| {
                        Box::pin(capture!([k], async move {
                            Self::process_result(
                                result,
                                value_to_logical_properties,
                                "derive function",
                                k,
                            )
                            .await;
                        }))
                    }),
                )
                .await;
        })
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

    /// Helper function to process values and handle failures
    ///
    /// This abstracts the common pattern of handling failures and value transformation
    /// for all rule application functions.
    ///
    /// # Parameters
    /// * `value` - The value from rule evaluation
    /// * `transform` - Function to transform value to desired type
    /// * `context` - Context string for error messages
    /// * `k` - Continuation to call with transformed value on success
    async fn process_result<T, F>(
        value: Value,
        transform: F,
        context: &str,
        k: Arc<dyn Fn(T) -> UnitFuture + Send + Sync + 'static>,
    ) where
        F: FnOnce(&Value) -> T,
    {
        match &value.0 {
            CoreData::Fail(boxed_msg) => {
                if let CoreData::Literal(Literal::String(error_message)) = &boxed_msg.0 {
                    eprintln!("Error in {}: {}", context, error_message);
                    // Don't call continuation for failed rules
                } else {
                    panic!("Fail expression must evaluate to a string message");
                }
            }
            _ => {
                // Transform and pass to continuation
                let transformed = transform(&value);
                k(transformed).await;
            }
        }
    }
}
