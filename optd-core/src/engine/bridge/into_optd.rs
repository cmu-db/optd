use std::sync::Arc;

use crate::ir::{
    goal::GoalId,
    groups::{RelationalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, OperatorData, PhysicalOperator, ScalarOperator},
    plans::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan, ScalarPlan},
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
        LogicalOperator(materialization) => match materialization {
            UnMaterialized(group_id, _) => {
                PartialLogicalPlan::UnMaterialized(RelationalGroupId(*group_id))
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
        _ => panic!(
            "Expected LogicalOperator CoreData variant, found: {:?}",
            value.0
        ),
    }
}

/// Converts a HIR Value into a PartialScalarPlan representation.
///
/// Transforms the DSL's HIR into the optimizer's IR for scalar operators.
pub(crate) fn value_to_partial_scalar(value: &Value) -> PartialScalarPlan {
    match &value.0 {
        ScalarOperator(materialization) => match materialization {
            UnMaterialized(group_id, _) => {
                PartialScalarPlan::UnMaterialized(ScalarGroupId(*group_id))
            }
            Materialized(op) => PartialScalarPlan::PartialMaterialized {
                node: ScalarOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.operator_data),
                    children: convert_children_into(&op.scalar_children, value_to_partial_scalar),
                },
            },
        },
        _ => panic!(
            "Expected ScalarOperator CoreData variant, found: {:?}",
            value.0
        ),
    }
}

/// Converts a HIR Value into a complete ScalarPlan (not a partial plan).
///
/// Used when fully materializing a scalar expression for use in properties.
fn value_to_scalar(value: &Value) -> ScalarPlan {
    match &value.0 {
        ScalarOperator(materialization) => match materialization {
            UnMaterialized(_, _) => {
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
        _ => panic!(
            "Expected ScalarOperator CoreData variant, found: {:?}",
            value.0
        ),
    }
}

/// Converts a HIR Value into a PartialPhysicalPlan representation.
///
/// Transforms the DSL's HIR into the optimizer's IR for physical operators.
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        PhysicalOperator(materialization) => match materialization {
            UnMaterialized(goal_id, _) => PartialPhysicalPlan::UnMaterialized(GoalId(*goal_id)),
            Materialized(physical_op) => {
                let op = &physical_op.operator;

                let properties = match physical_op.properties.as_ref() {
                    Value(Null) => None,
                    other => Some(value_to_properties_data(&other)),
                };

                PartialPhysicalPlan::PartialMaterialized {
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
                    properties: PhysicalProperties(properties),
                    group_id: RelationalGroupId(physical_op.group_id),
                }
            }
        },
        _ => panic!(
            "Expected PhysicalOperator CoreData variant, found: {:?}",
            value.0
        ),
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
        ScalarOperator(_) => PropertiesData::Scalar(value_to_scalar(value)),
        _ => panic!("Cannot convert {:?} to PropertyData conten", value.0),
    }
}
