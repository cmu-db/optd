// PartialLogicalPlan + Transformation IR => PartialLogicalPlan

use std::collections::HashMap;

use crate::{
    engine::patterns::{scalar::ScalarPattern, value::ValuePattern},
    plans::scalar::PartialScalarPlan,
    values::OptdValue,
};

use super::scalar::ScalarAnalyzer;

// TODO(Alexis): it is totally fair for analyzers to have transformer compostions actually. just their return type should differ.
// This is much more powerful. No reason not to do it.
pub struct Context {
    pub value_bindings: HashMap<String, OptdValue>,
    pub scalar_bindings: HashMap<String, PartialScalarPlan>,
}

pub fn scalar_analyze(
    plan: PartialScalarPlan,
    transformer: &ScalarAnalyzer,
) -> anyhow::Result<Option<OptdValue>> {
    for matcher in transformer.matches.iter() {
        let mut context = Context {
            value_bindings: HashMap::new(),
            scalar_bindings: HashMap::new(),
        };
        if match_scalar(&plan, &matcher.pattern, &mut context)? {
            for (name, comp) in matcher.composition.iter() {
                let value = scalar_analyze(context.scalar_bindings[name].clone(), &comp.borrow())?;
                let Some(value) = value else {
                    return Ok(None);
                };
                context.value_bindings.insert(name.clone(), value);
            }

            return Ok(Some(matcher.output.evaluate(&context.value_bindings)));
        }
    }
    Ok(None)
}

fn match_scalar(
    plan: &PartialScalarPlan,
    pattern: &ScalarPattern,
    context: &mut Context,
) -> anyhow::Result<bool> {
    match pattern {
        ScalarPattern::Any => Ok(true),
        ScalarPattern::Not(scalar_pattern) => {
            let x = match_scalar(plan, scalar_pattern, context)?;
            Ok(!x)
        }
        ScalarPattern::Bind(name, scalar_pattern) => {
            context.scalar_bindings.insert(name.clone(), plan.clone());
            match_scalar(plan, scalar_pattern, context)
        }
        ScalarPattern::Operator {
            op_type,
            content,
            scalar_children,
        } => {
            let PartialScalarPlan::PartialMaterialized { operator } = plan else {
                return Ok(false); //TODO: Call memo!!
            };

            if operator.operator_kind() != *op_type {
                return Ok(false);
            }

            for (subpattern, subplan) in scalar_children
                .iter()
                .zip(operator.children_scalars().iter())
            {
                if !match_scalar(subplan, subpattern, context)? {
                    return Ok(false);
                }
            }

            for (subpattern, value) in content.iter().zip(operator.values().iter()) {
                if !match_value(value, subpattern, context) {
                    return Ok(false);
                }
            }

            Ok(true)
        }
    }
}

fn match_value(value: &OptdValue, pattern: &ValuePattern, context: &mut Context) -> bool {
    match pattern {
        ValuePattern::Any => true,
        ValuePattern::Bind(name, optd_expr) => {
            context.value_bindings.insert(name.clone(), value.clone());
            match_value(value, optd_expr, context)
        }
        ValuePattern::Match { expr } => expr.evaluate(&context.value_bindings) == *value,
    }
}
