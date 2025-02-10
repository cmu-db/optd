use std::collections::HashMap;

use crate::{plans::PartialPlanExpr, values::OptdValue};

/// Evaluates a PartialPlanExpr to an PartialPlan using provided bindings.
impl<Plan: Clone> PartialPlanExpr<Plan> {
    pub fn evaluate(
        &self,
        plan_bindings: &HashMap<String, Plan>,
        value_bindings: &HashMap<String, OptdValue>,
    ) -> Plan {
        match self {
            PartialPlanExpr::Plan(plan) => plan.clone(),

            PartialPlanExpr::Ref(name) => plan_bindings.get(name).cloned().unwrap_or_else(|| {
                panic!("Undefined reference: {}", name);
            }),

            PartialPlanExpr::IfThenElse {
                cond,
                then,
                otherwise,
            } => match cond.evaluate(value_bindings) {
                OptdValue::Bool(true) => then.evaluate(plan_bindings, value_bindings),
                OptdValue::Bool(false) => otherwise.evaluate(plan_bindings, value_bindings),
                _ => panic!("IfThenElse condition must be boolean"),
            },
        }
    }
}
