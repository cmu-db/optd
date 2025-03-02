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
/// This function transforms the DSL's internal representation of a logical operator
/// into the optimizer's intermediate representation, allowing the optimizer to work
/// with plans defined or modified by the DSL.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// A PartialLogicalPlan representation of the input value
///
/// # Panics
/// Panics if the Value does not contain a LogicalOperator variant
pub(crate) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        LogicalOperator(materialization) => match materialization {
            UnMaterialized(group_id) => {
                PartialLogicalPlan::UnMaterialized(RelationalGroupId(*group_id))
            }
            Materialized(op) => PartialLogicalPlan::PartialMaterialized {
                node: LogicalOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.data),
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
/// This function transforms the DSL's internal representation of a scalar operator
/// into the optimizer's intermediate representation.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// A PartialScalarPlan representation of the input value
///
/// # Panics
/// Panics if the Value does not contain a ScalarOperator variant
pub(crate) fn value_to_partial_scalar(value: &Value) -> PartialScalarPlan {
    match &value.0 {
        ScalarOperator(materialization) => match materialization {
            UnMaterialized(group_id) => PartialScalarPlan::UnMaterialized(ScalarGroupId(*group_id)),
            Materialized(op) => PartialScalarPlan::PartialMaterialized {
                node: ScalarOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.data),
                    children: convert_children_into(&op.children, value_to_partial_scalar),
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
/// This function is used when fully materializing a scalar expression
/// for use in properties or other fully-realized contexts.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// A ScalarPlan representation of the input value
///
/// # Panics
/// Panics if the Value does not contain a materialized ScalarOperator variant
fn value_to_scalar(value: &Value) -> ScalarPlan {
    match &value.0 {
        ScalarOperator(materialization) => match materialization {
            UnMaterialized(_) => {
                panic!("Cannot convert UnMaterialized ScalarOperator to ScalarPlan")
            }
            Materialized(op) => ScalarPlan {
                node: ScalarOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.data),
                    children: convert_children_into(&op.children, value_to_scalar),
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
/// This function transforms the DSL's internal representation of a physical operator
/// into the optimizer's intermediate representation.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// A PartialPhysicalPlan representation of the input value
///
/// # Panics
/// Panics if the Value does not contain a PhysicalOperator variant
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        PhysicalOperator(materialization) => match materialization {
            UnMaterialized(goal_id) => PartialPhysicalPlan::UnMaterialized(GoalId(*goal_id)),
            Materialized(op) => PartialPhysicalPlan::PartialMaterialized {
                node: PhysicalOperator {
                    tag: op.tag.clone(),
                    data: convert_values_to_operator_data(&op.data),
                    relational_children: convert_children_into(
                        &op.relational_children,
                        value_to_partial_physical,
                    ),
                    scalar_children: convert_children_into(
                        &op.scalar_children, // Fixed: was using relational_children
                        value_to_partial_scalar,
                    ),
                },
                properties: PhysicalProperties(value_to_properties_data(&op.properties)),
                group_id: RelationalGroupId(op.group_id),
            },
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

/// Generic function to convert a Vec of Values to Vec of Children.
///
/// This function provides a generic way to convert HIR Values to IR Child structures
/// for any type that can be created from a Value using a converter function.
///
/// # Type Parameters
/// * `T` - The target type which will be wrapped in Arc<T> inside Child<Arc<T>>
///
/// # Parameters
/// * `values` - The slice of Values to convert
/// * `converter` - A function that converts a single Value to the target type T
///
/// # Returns
/// A Vec of Child<Arc<T>> representing the converted values
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
///
/// This function maps each Value to its corresponding OperatorData representation.
///
/// # Parameters
/// * `values` - The slice of Values to convert
///
/// # Returns
/// A Vec of OperatorData representing the converted values
fn convert_values_to_operator_data(values: &[Value]) -> Vec<OperatorData> {
    values.iter().map(value_to_operator_data).collect()
}

/// Converts a slice of HIR Values to a Vec of PropertiesData.
///
/// This function maps each Value to its corresponding PropertiesData representation.
///
/// # Parameters
/// * `values` - The slice of Values to convert
///
/// # Returns
/// A Vec of PropertiesData representing the converted values
fn convert_values_to_properties_data(values: &[Value]) -> Vec<PropertiesData> {
    values.iter().map(value_to_properties_data).collect()
}

//=============================================================================
// Data conversion functions
//=============================================================================

/// Converts a HIR Value to an OperatorData representation.
///
/// This function handles the conversion of various HIR Value types to their
/// corresponding OperatorData representations, which are used in the IR.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// An OperatorData representation of the input value
///
/// # Panics
/// Panics if the Value contains a type that cannot be converted to OperatorData
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
///
/// This function handles the conversion of various HIR Value types to their
/// corresponding PropertiesData representations, which are used for physical
/// plan properties in the IR.
///
/// # Parameters
/// * `value` - The HIR Value to convert
///
/// # Returns
/// A PropertiesData representation of the input value
///
/// # Panics
/// Panics if the Value contains a type that cannot be converted to PropertiesData
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
        Struct(name, elements) => PropertiesData::Struct(
            name.clone(),
            elements.iter().map(value_to_properties_data).collect(),
        ),
        ScalarOperator(_) => PropertiesData::Scalar(value_to_scalar(value)),
        _ => panic!("Cannot convert {:?} to PropertyData", value.0),
    }
}
