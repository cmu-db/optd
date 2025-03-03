use std::sync::Arc;

use crate::ir::{
    groups::{LogicalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, OperatorData, PhysicalOperator, ScalarOperator},
    plans::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan, PhysicalGoal, ScalarPlan},
    properties::{PhysicalProperties, PropertiesData},
};
use optd_dsl::analyzer::hir::{CoreData, Literal, Materializable, Value};
use Child::*;
use CoreData::*;
use Literal::*;
use Materializable::*;

//=============================================================================
// Main conversion functions
//=============================================================================

/// Converts a HIR Value into a PartialLogicalPlan representation.
///
/// Transforms the DSL's HIR into the optimizer's IR for logical operators.
pub(crate) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        Logical(logical_op) => match &logical_op.0 {
            UnMaterialized(group_id) => {
                PartialLogicalPlan::UnMaterialized(LogicalGroupId(group_id.0))
            }
            Materialized(op) => PartialLogicalPlan::PartialMaterialized {
                node: LogicalOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.operator_data),
                    relational_children: convert_children_into(
                        &op.relational_children,
                        value_to_partial_logical,
                    ),
                    scalar_children: convert_children_into(
                        &op.scalar_children,
                        value_to_partial_scalar,
                    ),
                },
            },
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value into a PartialScalarPlan representation.
///
/// Transforms the DSL's HIR into the optimizer's IR for scalar operators.
pub(crate) fn value_to_partial_scalar(value: &Value) -> PartialScalarPlan {
    match &value.0 {
        Scalar(scalar_op) => match &scalar_op.0 {
            UnMaterialized(group_id) => {
                PartialScalarPlan::UnMaterialized(ScalarGroupId(group_id.0))
            }
            Materialized(op) => PartialScalarPlan::PartialMaterialized {
                node: ScalarOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.operator_data),
                    children: convert_children_into(&op.scalar_children, value_to_partial_scalar),
                },
            },
        },
        _ => panic!("Expected Scalar CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value into a complete ScalarPlan (not a partial plan).
///
/// Used when fully materializing a scalar expression for use in properties.
fn value_to_scalar(value: &Value) -> ScalarPlan {
    match &value.0 {
        Scalar(scalar_op) => match &scalar_op.0 {
            UnMaterialized(_) => {
                panic!("Cannot convert UnMaterialized ScalarOperator to ScalarPlan")
            }
            Materialized(op) => ScalarPlan {
                node: ScalarOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.operator_data),
                    children: convert_children_into(&op.scalar_children, value_to_scalar),
                },
            },
        },
        _ => panic!("Expected Scalar CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value into a PartialPhysicalPlan representation.
///
/// Transforms the DSL's HIR into the optimizer's IR for physical operators.
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        Physical(physical_op) => match &physical_op.0 {
            UnMaterialized(hir_goal) => {
                let ir_goal = PhysicalGoal {
                    group_id: LogicalGroupId(hir_goal.group_id.0),
                    properties: convert_hir_properties_to_ir(&hir_goal.properties),
                };

                PartialPhysicalPlan::UnMaterialized(ir_goal)
            }
            Materialized(op) => PartialPhysicalPlan::PartialMaterialized {
                node: PhysicalOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.operator_data),
                    relational_children: convert_children_into(
                        &op.relational_children,
                        value_to_partial_physical,
                    ),
                    scalar_children: convert_children_into(
                        &op.scalar_children,
                        value_to_partial_scalar,
                    ),
                },
            },
        },
        _ => panic!("Expected Physical CoreData variant, found: {:?}", value.0),
    }
}

/// Convert HIR properties value to IR PhysicalProperties
fn convert_hir_properties_to_ir(properties_value: &Value) -> PhysicalProperties {
    match &properties_value.0 {
        Null => PhysicalProperties(None),
        _ => PhysicalProperties(Some(value_to_properties_data(properties_value))),
    }
}

//=============================================================================
// Generic conversion helpers
//=============================================================================

/// Converts a Vec of Values to Vec of Children for any target type.
fn convert_children_into<T, F>(values: &[Value], converter: F) -> Vec<Child<Arc<T>>>
where
    F: Fn(&Value) -> T,
    T: 'static,
{
    values
        .iter()
        .map(|value| match &value.0 {
            Array(elements) => {
                VarLength(elements.iter().map(|el| Arc::new(converter(el))).collect())
            }
            _ => Singleton(Arc::new(converter(value))),
        })
        .collect()
}

/// Converts a slice of HIR Values to a Vec of OperatorData.
fn convert_values_to_operator_data(values: &[Value]) -> Vec<OperatorData> {
    values.iter().map(value_to_operator_data).collect()
}

/// Converts a slice of HIR Values to a Vec of PropertiesData.
fn convert_values_to_properties_data(values: &[Value]) -> Vec<PropertiesData> {
    values.iter().map(value_to_properties_data).collect()
}

//=============================================================================
// Data conversion functions
//=============================================================================

/// Converts a HIR Value to an OperatorData representation.
fn value_to_operator_data(value: &Value) -> OperatorData {
    match &value.0 {
        Literal(constant) => match constant {
            Int64(i) => OperatorData::Int64(*i),
            Float64(f) => OperatorData::Float64((*f).into()),
            String(s) => OperatorData::String(s.clone()),
            Bool(b) => OperatorData::Bool(*b),
            Unit => panic!("Cannot convert Unit constant to OperatorData"),
        },
        Array(elements) => OperatorData::Array(convert_values_to_operator_data(elements)),
        Struct(name, elements) => {
            OperatorData::Struct(name.clone(), convert_values_to_operator_data(elements))
        }
        _ => panic!("Cannot convert {:?} to OperatorData", value.0),
    }
}

/// Converts a HIR Value to a PropertiesData representation.
fn value_to_properties_data(value: &Value) -> PropertiesData {
    match &value.0 {
        Literal(constant) => match constant {
            Int64(i) => PropertiesData::Int64(*i),
            Float64(f) => PropertiesData::Float64((*f).into()),
            String(s) => PropertiesData::String(s.clone()),
            Bool(b) => PropertiesData::Bool(*b),
            Unit => panic!("Cannot convert Unit constant to PropertyData"),
        },
        Array(elements) => PropertiesData::Array(convert_values_to_properties_data(elements)),
        Struct(name, elements) => {
            PropertiesData::Struct(name.clone(), convert_values_to_properties_data(elements))
        }
        Scalar(scalar_op) => PropertiesData::Scalar(value_to_scalar(value)),
        _ => panic!("Cannot convert {:?} to PropertyData content", value.0),
    }
}
