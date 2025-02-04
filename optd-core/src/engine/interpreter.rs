// PartialLogicalPlan + Transformation IR => PartialLogicalPlan

use std::collections::HashMap;

use crate::{plans::scalar::PartialScalarPlan, values::OptdValue};

use super::{
    actions::transformers::scalar::{Composition, ScalarTransformer},
    patterns::{scalar::ScalarPattern, value::ValuePattern},
};

struct Context {
    scalar_bindings: HashMap<String, PartialScalarPlan>,
    value_bindings: HashMap<String, OptdValue>,
}

pub fn scalar_transform(
    plan: PartialScalarPlan,
    transformer: &ScalarTransformer,
) -> anyhow::Result<Option<PartialScalarPlan>> {
    for matcher in transformer.matches.iter() {
        let mut context = Context {
            scalar_bindings: HashMap::new(),
            value_bindings: HashMap::new(),
        };
        if match_scalar(&plan, &matcher.pattern, &mut context)? {
            // Apply compositions
            for (name, comp) in matcher.composition.iter() {
                match comp.as_ref() {
                    Composition::ScalarTransformer(scalar_transformer) => {
                        let new_plan = scalar_transform(
                            context.scalar_bindings[name].clone(),
                            scalar_transformer,
                        )?;
                        let Some(new_plan) = new_plan else {
                            return Ok(None);
                        };
                        context.scalar_bindings.insert(name.clone(), new_plan);
                    }
                    Composition::ScalarAnalyzer(_scalar_analyzer) => {
                        // TODO(alexis): Implement scalar analyzer
                        let _value: Option<OptdValue> = None;
                        let Some(value) = _value else {
                            return Ok(None);
                        };
                        context.value_bindings.insert(name.clone(), value);
                    }
                }
            }

            // TODO(alexis): evaluate partial logical plan matcher.output.evaluate(&context.value_bindings);
            let plan = plan;
            return Ok(Some(plan));
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
