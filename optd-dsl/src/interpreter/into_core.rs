use crate::analyzer::hir::{Literal, CoreData, Materializable, Value};
use optd_core::cascades::{
    groups::{RelationalGroupId, ScalarGroupId},
    ir::{Children, OperatorData, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan},
};
use Literal::*;
use CoreData::*;
use Materializable::*;

/// Helper trait to convert from Value to partial plans
trait FromValue: Sized {
    fn from_value(value: &Value) -> Self;
}

impl FromValue for PartialLogicalPlan {
    fn from_value(value: &Value) -> Self {
        value_to_partial_logical(value)
    }
}

impl FromValue for PartialScalarPlan {
    fn from_value(value: &Value) -> Self {
        value_to_partial_scalar(value)
    }
}

impl FromValue for PartialPhysicalPlan {
    fn from_value(value: &Value) -> Self {
        value_to_partial_physical(value)
    }
}

/// Convert a Vec of Values to Vec of Children
fn convert_from_values<T: FromValue>(values: &[Value]) -> Vec<Children<T>> {
    values
        .iter()
        .map(|value| match &value.0 {
            Array(elements) => {
                Children::VarLength(elements.iter().map(|el| T::from_value(el)).collect())
            }
            _ => Children::Singleton(T::from_value(value)),
        })
        .collect()
}

/// Converts Values back to operator data
fn convert_from_operator_data(values: &[Value]) -> Vec<OperatorData> {
    values.iter().map(value_to_operator_data).collect()
}

/// Converts a HIR Value back to a PartialLogicalPlan
pub(super) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        Logical(materialization) => match materialization {
            Group(group_id) => PartialLogicalPlan::UnMaterialized(RelationalGroupId(*group_id)),
            Data(logical_op) => PartialLogicalPlan::PartialMaterialized {
                tag: logical_op.tag.clone(),
                data: convert_from_operator_data(&logical_op.operator_data),
                relational_children: convert_from_values(&logical_op.relational_children),
                scalar_children: convert_from_values(&logical_op.scalar_children),
            },
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value back to a PartialScalarPlan
pub(super) fn value_to_partial_scalar(value: &Value) -> PartialScalarPlan {
    match &value.0 {
        Scalar(materialization) => match materialization {
            Group(group_id) => PartialScalarPlan::UnMaterialized(ScalarGroupId(*group_id)),
            Data(scalar_op) => PartialScalarPlan::PartialMaterialized {
                tag: scalar_op.tag.clone(),
                data: convert_from_operator_data(&scalar_op.operator_data),
                scalar_children: convert_from_values(&scalar_op.scalar_children),
            },
        },
        _ => panic!("Expected Scalar CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value back to a PartialPhysicalPlan
pub(super) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        Physical(materialization) => match materialization {
            Group(group_id) => PartialPhysicalPlan::UnMaterialized(RelationalGroupId(*group_id)),
            Data(physical_op) => PartialPhysicalPlan::PartialMaterialized {
                tag: physical_op.tag.clone(),
                data: convert_from_operator_data(&physical_op.operator_data),
                relational_children: convert_from_values(&physical_op.relational_children),
                scalar_children: convert_from_values(&physical_op.scalar_children),
            },
        },
        _ => panic!("Expected Physical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value back to OperatorData
fn value_to_operator_data(value: &Value) -> OperatorData {
    match &value.0 {
        Literal(constant) => match constant {
            Int64(i) => OperatorData::Int64(*i),
            Float64(f) => OperatorData::Float64((*f).into()),
            String(s) => OperatorData::String(s.clone()),
            Bool(b) => OperatorData::Bool(*b),
            Unit => panic!("Cannot convert Unit constant to OperatorData"),
        },
        Array(elements) => OperatorData::Array(convert_from_operator_data(elements)),
        Struct(name, elements) => {
            OperatorData::Struct(name.clone(), convert_from_operator_data(elements))
        }
        _ => panic!("Cannot convert {:?} to OperatorData", value.0),
    }
}
