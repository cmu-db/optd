//! Converts optd's type representations (CIR) into DSL [`Value`]s (HIR).

use crate::core::cir::*;
use crate::dsl::analyzer::hir::{
    self, CoreData, Literal, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};
use CoreData::*;
use Literal::*;
use Materializable::*;
use std::sync::Arc;

/// Converts a [`PartialLogicalPlan`] into a [`Value`].
pub(crate) fn partial_logical_to_value(plan: &PartialLogicalPlan) -> Value {
    match plan {
        PartialLogicalPlan::UnMaterialized(group_id) => {
            // For unmaterialized logical operators, we create a `Value` with the group ID.
            Value::new(Logical(UnMaterialized(hir::GroupId(group_id.0))))
        }
        PartialLogicalPlan::Materialized(node) => {
            // For materialized logical operators, we create a `Value` with the operator data.
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: convert_children_to_values(&node.children, partial_logical_to_value),
            };

            Value::new(Logical(Materialized(LogicalOp::logical(operator))))
        }
    }
}

/// Converts a [`PartialPhysicalPlan`] into a [`Value`].
pub(crate) fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    match plan {
        PartialPhysicalPlan::UnMaterialized(goal) => {
            // For unmaterialized physical operators, we create a `Value` with the goal
            let hir_goal = cir_goal_to_hir(goal);
            Value::new(Physical(UnMaterialized(hir_goal)))
        }
        PartialPhysicalPlan::Materialized(node) => {
            // For materialized physical operators, we create a Value with the operator data
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: convert_children_to_values(&node.children, partial_physical_to_value),
            };

            Value::new(Physical(Materialized(PhysicalOp::physical(operator))))
        }
    }
}

// TODO(Alexis): Once we define statistics, there should be a custom CIR representation.
/// Converts a [`PartialPhysicalPlan`]  with its cost into a [`Value`].
pub(crate) fn costed_physical_to_value(plan: PartialPhysicalPlan, cost: Cost) -> Value {
    let operator = partial_physical_to_value(&plan);
    Value::new(Tuple(vec![
        partial_physical_to_value(&plan),
        Value::new(Literal(Float64(cost.0))),
    ]))
}

/// Converts [`LogicalProperties`] into a [`Value`].
#[allow(dead_code)]
pub(crate) fn logical_properties_to_value(properties: &LogicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        Option::None => Value::new(None),
    }
}

/// Converts [`PhysicalProperties`] into a [`Value`].
pub(crate) fn physical_properties_to_value(properties: &PhysicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        Option::None => Value::new(None),
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
                Value::new(Array(items.iter().map(|item| converter(item)).collect()))
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
        OperatorData::Int64(i) => Value::new(Literal(Int64(*i))),
        OperatorData::Float64(f) => Value::new(Literal(Float64(**f))),
        OperatorData::String(s) => Value::new(Literal(String(s.clone()))),
        OperatorData::Bool(b) => Value::new(Literal(Bool(*b))),
        OperatorData::Struct(name, elements) => Value::new(Struct(
            name.clone(),
            convert_operator_data_to_values(elements),
        )),
        OperatorData::Array(elements) => {
            Value::new(Array(convert_operator_data_to_values(elements)))
        }
    }
}

/// Converts a [`PropertiesData`] into a [`Value`].
fn properties_data_to_value(data: &PropertiesData) -> Value {
    match data {
        PropertiesData::Int64(i) => Value::new(Literal(Int64(*i))),
        PropertiesData::Float64(f) => Value::new(Literal(Float64(**f))),
        PropertiesData::String(s) => Value::new(Literal(String(s.clone()))),
        PropertiesData::Bool(b) => Value::new(Literal(Bool(*b))),
        PropertiesData::Struct(name, elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value::new(Struct(name.clone(), values))
        }
        PropertiesData::Array(elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value::new(Array(values))
        }
    }
}
