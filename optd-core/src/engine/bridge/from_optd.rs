use std::sync::Arc;

use crate::ir::{
    operators::{Child, OperatorData},
    plans::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan, ScalarPlan},
    properties::{PhysicalProperties, PropertiesData},
};
use optd_dsl::analyzer::hir::{
    CoreData, GroupId, Literal, LogicalOp, Materializable, Operator, OperatorKind, PhysicalGoal,
    PhysicalOp, ScalarOp, Value,
};
use CoreData::*;
use Literal::*;
use Materializable::*;

//=============================================================================
// Main conversion functions
//=============================================================================

/// Converts a PartialLogicalPlan into a HIR Value representation.
pub(crate) fn partial_logical_to_value(plan: &PartialLogicalPlan) -> Value {
    match plan {
        PartialLogicalPlan::UnMaterialized(group_id) => {
            // For unmaterialized logical operators, we create a Value with the group ID
            Value(Logical(LogicalOp(UnMaterialized(GroupId(group_id.0)))))
        }
        PartialLogicalPlan::PartialMaterialized { node } => {
            // For materialized logical operators, we create a Value with the operator data
            let operator = Operator {
                kind: OperatorKind::Logical,
                tag: node.tag.clone(),
                operator_data: convert_operator_data_to_values(&node.data),
                relational_children: convert_children_to_values(
                    &node.relational_children,
                    partial_logical_to_value,
                ),
                scalar_children: convert_children_to_values(
                    &node.scalar_children,
                    partial_scalar_to_value,
                ),
            };

            Value(Logical(LogicalOp(Materialized(operator))))
        }
    }
}

/// Converts a PartialScalarPlan into a HIR Value representation.
pub(crate) fn partial_scalar_to_value(plan: &PartialScalarPlan) -> Value {
    match plan {
        PartialScalarPlan::UnMaterialized(group_id) => {
            // For unmaterialized scalar operators, we create a Value with the group ID
            Value(Scalar(ScalarOp(UnMaterialized(GroupId(group_id.0)))))
        }
        PartialScalarPlan::PartialMaterialized { node } => {
            // For materialized scalar operators, we create a Value with the operator data
            let operator = Operator {
                kind: OperatorKind::Scalar,
                tag: node.tag.clone(),
                operator_data: convert_operator_data_to_values(&node.data),
                relational_children: vec![], // Scalar ops don't have relational children
                scalar_children: convert_children_to_values(
                    &node.children,
                    partial_scalar_to_value,
                ),
            };

            Value(Scalar(ScalarOp(Materialized(operator))))
        }
    }
}

/// Converts a ScalarPlan into a HIR Value representation.
fn scalar_to_value(plan: &ScalarPlan) -> Value {
    // For scalar plans, we create a Value with the operator data
    let operator = Operator {
        kind: OperatorKind::Scalar,
        tag: plan.node.tag.clone(),
        operator_data: convert_operator_data_to_values(&plan.node.data),
        relational_children: vec![], // Scalar ops don't have relational children
        scalar_children: convert_children_to_values(&plan.node.children, scalar_to_value),
    };

    Value(Scalar(ScalarOp(Materialized(operator))))
}

/// Converts a PartialPhysicalPlan into a HIR Value representation.
pub(crate) fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    match plan {
        PartialPhysicalPlan::UnMaterialized(source_goal) => {
            // Convert source PhysicalGoal to HIR PhysicalGoal
            let hir_goal = PhysicalGoal {
                group_id: GroupId(source_goal.group_id.0),
                properties: physical_properties_to_value(&source_goal.properties).into(),
            };

            // For unmaterialized physical operators, we create a Value with the PhysicalGoal
            Value(Physical(PhysicalOp(UnMaterialized(hir_goal))))
        }
        PartialPhysicalPlan::PartialMaterialized { node } => {
            // For materialized physical operators, we create an Operator
            let operator = Operator {
                kind: OperatorKind::Physical,
                tag: node.tag.clone(),
                operator_data: convert_operator_data_to_values(&node.data),
                relational_children: convert_children_to_values(
                    &node.relational_children,
                    partial_physical_to_value,
                ),
                scalar_children: convert_children_to_values(
                    &node.scalar_children,
                    partial_scalar_to_value,
                ),
            };

            Value(Physical(PhysicalOp(Materialized(operator))))
        }
    }
}

//=============================================================================
// Generic conversion helpers
//=============================================================================

/// Generic function to convert a Vec of Children to Vec of Values.
fn convert_children_to_values<T, F>(children: &[Child<Arc<T>>], converter: F) -> Vec<Value>
where
    F: Fn(&T) -> Value,
    T: 'static,
{
    children
        .iter()
        .map(|child| match child {
            Child::Singleton(item) => converter(item),
            Child::VarLength(items) => {
                Value(Array(items.iter().map(|item| converter(item)).collect()))
            }
        })
        .collect()
}

/// Converts a slice of OperatorData to a Vec of HIR Values.
fn convert_operator_data_to_values(data: &[OperatorData]) -> Vec<Value> {
    data.iter().map(operator_data_to_value).collect()
}

//=============================================================================
// Data conversion functions
//=============================================================================

/// Converts an OperatorData to a HIR Value representation.
fn operator_data_to_value(data: &OperatorData) -> Value {
    match data {
        OperatorData::Int64(i) => Value(Literal(Int64(*i))),
        OperatorData::Float64(f) => Value(Literal(Float64(**f))),
        OperatorData::String(s) => Value(Literal(String(s.clone()))),
        OperatorData::Bool(b) => Value(Literal(Bool(*b))),
        OperatorData::Struct(name, elements) => Value(Struct(
            name.clone(),
            convert_operator_data_to_values(elements),
        )),
        OperatorData::Array(elements) => Value(Array(convert_operator_data_to_values(elements))),
    }
}

/// Converts PhysicalProperties to a HIR Value representation.
fn physical_properties_to_value(properties: &PhysicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        None => Value(Null),
    }
}

/// Converts a PropertiesData to a HIR Value representation.
fn properties_data_to_value(data: &PropertiesData) -> Value {
    match data {
        PropertiesData::Int64(i) => Value(Literal(Int64(*i))),
        PropertiesData::Float64(f) => Value(Literal(Float64(**f))),
        PropertiesData::String(s) => Value(Literal(String(s.clone()))),
        PropertiesData::Bool(b) => Value(Literal(Bool(*b))),
        PropertiesData::Struct(name, elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value(Struct(name.clone(), values))
        }
        PropertiesData::Array(elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value(Array(values))
        }
        PropertiesData::Scalar(scalar) => scalar_to_value(scalar),
    }
}
