use crate::{
    analyzer::hir::{AnnotatedValue, CoreData, Expr, Literal, Value, HIR},
    utils::context::Context,
};
use bridge::{from_optd::partial_logical_to_value, into_optd::value_to_partial_logical};
use errors::EngineError;
use futures::{Stream, StreamExt};
use optd_core::cascades::ir::PartialLogicalPlan;
use std::sync::Arc;

use CoreData::*;
use Expr::*;
use Literal::*;

mod bridge;
mod errors;
mod evaluation;

pub type PartialLogicalPlanStream =
    Box<dyn Stream<Item = Result<PartialLogicalPlan, EngineError>> + Send + Unpin>;

/// The interpreter for evaluating HIR expressions
pub struct Interpreter {
    hir: Arc<HIR>,
}

impl Interpreter {
    pub fn new(hir: Arc<HIR>) -> Self {
        Self { hir }
    }

    /// Interpret a function with the given name and input
    /// This applies a logical rule to an input plan and returns all possible
    /// transformations of the plan according to the rule
    pub async fn match_and_apply_logical_rule(
        &self,
        rule_name: &str,
        plan: PartialLogicalPlan,
    ) -> PartialLogicalPlanStream {
        let context = Context::new(
            self.hir
                .expressions
                .iter()
                .map(|(id, AnnotatedValue { value, .. })| (id.clone(), value.clone()))
                .collect(),
        );

        let call = Call(
            Box::new(CoreVal(Value(Literal(String(rule_name.to_string()))))),
            vec![CoreVal(partial_logical_to_value(&plan))],
        );

        let results_stream = call.evaluate(context);

        Box::new(
            results_stream
                .map(|result| {
                    result.and_then(|value| match &value.0 {
                        Fail(boxed_msg) => match &boxed_msg.0 {
                            Literal(String(error_message)) => {
                                Err(EngineError::Placeholder(error_message.clone()))
                            }
                            _ => panic!("Fail expression must evaluate to a string message"),
                        },
                        _ => Ok(value_to_partial_logical(&value)),
                    })
                })
                .boxed(),
        )
    }
}
