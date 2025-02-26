use crate::analyzer::hir::{CoreData, Literal, Materializable, Operator, OperatorKind, Value};
use optd_core::cascades::{
    groups::{RelationalGroupId, ScalarGroupId},
    ir::{Children, OperatorData, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan},
};
use Children::*;
use CoreData::*;
use Literal::*;
use Materializable::*;
use OperatorKind::*;

/// Helper trait to convert from Value to partial plans
trait FromValue {
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
            Array(elements) => VarLength(elements.iter().map(|el| T::from_value(el)).collect()),
            _ => Singleton(T::from_value(value)),
        })
        .collect()
}

/// Converts Values back to operator data
fn convert_from_operator_data(values: &[Value]) -> Vec<OperatorData> {
    values.iter().map(value_to_operator_data).collect()
}

/// Converts a HIR Value back to a PartialLogicalPlan
pub(crate) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        Operator(materialization) => {
            validate_operator_kind(materialization, Logical);

            match materialization {
                Group(group_id, _) => {
                    PartialLogicalPlan::UnMaterialized(RelationalGroupId(*group_id))
                }
                Data(op) => PartialLogicalPlan::PartialMaterialized {
                    tag: op.tag.clone(),
                    data: convert_from_operator_data(&op.operator_data),
                    relational_children: convert_from_values(&op.relational_children),
                    scalar_children: convert_from_values(&op.scalar_children),
                },
            }
        }
        _ => panic!("Expected Operator CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value back to a PartialScalarPlan
pub(crate) fn value_to_partial_scalar(value: &Value) -> PartialScalarPlan {
    match &value.0 {
        Operator(materialization) => {
            validate_operator_kind(materialization, Scalar);

            match materialization {
                Group(group_id, _) => PartialScalarPlan::UnMaterialized(ScalarGroupId(*group_id)),
                Data(op) => PartialScalarPlan::PartialMaterialized {
                    tag: op.tag.clone(),
                    data: convert_from_operator_data(&op.operator_data),
                    scalar_children: convert_from_values(&op.scalar_children),
                },
            }
        }
        _ => panic!("Expected Operator CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value back to a PartialPhysicalPlan
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        Operator(materialization) => {
            validate_operator_kind(materialization, Physical);

            match materialization {
                Group(group_id, _) => {
                    PartialPhysicalPlan::UnMaterialized(RelationalGroupId(*group_id))
                }
                Data(op) => PartialPhysicalPlan::PartialMaterialized {
                    tag: op.tag.clone(),
                    data: convert_from_operator_data(&op.operator_data),
                    relational_children: convert_from_values(&op.relational_children),
                    scalar_children: convert_from_values(&op.scalar_children),
                },
            }
        }
        _ => panic!("Expected Operator CoreData variant, found: {:?}", value.0),
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

/// Helper function to extract and validate operator kind
fn validate_operator_kind<T>(op: &Materializable<Operator<T>>, expected_kind: OperatorKind) {
    let actual_kind = match op {
        Data(concrete_op) => concrete_op.kind,
        Group(_, kind) => *kind,
    };

    if actual_kind != expected_kind {
        panic!(
            "Expected operator kind {:?}, but found {:?}",
            expected_kind, actual_kind
        );
    }
}
