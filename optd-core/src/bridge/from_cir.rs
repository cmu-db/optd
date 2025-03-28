//! Converts optd's type representations (CIR) into DSL [`Value`]s (HIR).

use crate::cir::*;
use CoreData::*;
use Literal::*;
use Materializable::*;
use optd_dsl::analyzer::hir::{
    self, CoreData, Literal, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use std::sync::Arc;

/// Converts a [`PartialLogicalPlan`] into a [`Value`].
pub(crate) fn partial_logical_to_value(plan: &PartialLogicalPlan) -> Value {
    match plan {
        PartialLogicalPlan::UnMaterialized(group_id) => {
            // For unmaterialized logical operators, we create a `Value` with the group ID.
            Value(Logical(LogicalOp(UnMaterialized(hir::GroupId(group_id.0)))))
        }
        PartialLogicalPlan::Materialized(node) => {
            // For materialized logical operators, we create a `Value` with the operator data.
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: convert_children_to_values(&node.children, partial_logical_to_value),
            };

            Value(Logical(LogicalOp(Materialized(operator))))
        }
    }
}

/// Converts a [`PartialPhysicalPlan`] into a [`Value`].
pub(crate) fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    match plan {
        PartialPhysicalPlan::UnMaterialized(goal) => {
            // For unmaterialized physical operators, we create a `Value` with the goal
            let hir_goal = cir_goal_to_hir(goal);
            Value(Physical(PhysicalOp(UnMaterialized(hir_goal))))
        }
        PartialPhysicalPlan::Materialized(node) => {
            // For materialized physical operators, we create a Value with the operator data
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: convert_children_to_values(&node.children, partial_physical_to_value),
            };

            Value(Physical(PhysicalOp(Materialized(operator))))
        }
    }
}

/// Converts [`LogicalProperties`] into a [`Value`].
#[allow(dead_code)]
pub(crate) fn logical_properties_to_value(properties: &LogicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        None => Value(Null),
    }
}

/// Converts [`PhysicalProperties`] into a [`Value`].
pub(crate) fn physical_properties_to_value(properties: &PhysicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        None => Value(Null),
    }
}

/// Converts a CIR [`Goal`] to a HIR [`Goal`](hir::Goal).
fn cir_goal_to_hir(goal: &Goal) -> hir::Goal {
    let group_id = cir_group_id_to_hir(&goal.0);
    let properties = physical_properties_to_value(&goal.1);

    hir::Goal {
        group_id,
        properties: Box::new(properties),
    }
}

/// Converts a CIR [`GroupId`] to a HIR [`GroupId`](hir::GroupId).
fn cir_group_id_to_hir(group_id: &GroupId) -> hir::GroupId {
    hir::GroupId(group_id.0)
}

/// A generic function to convert a slice of children into a vector of [`Value`]s.
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

/// Converts a slice of [`OperatorData`] into a vector of [`Value`]s.
fn convert_operator_data_to_values(data: &[OperatorData]) -> Vec<Value> {
    data.iter().map(operator_data_to_value).collect()
}

/// Converts an [`OperatorData`] into a [`Value`].
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

/// Converts a [`PropertiesData`] into a [`Value`].
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
        PropertiesData::Logical(plan) => partial_logical_to_value(&plan.clone().into()),
    }
}
