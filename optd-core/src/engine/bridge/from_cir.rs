use crate::cir::{
    operators::{Child, OperatorData},
    plans::PartialLogicalPlan,
    properties::{PhysicalProperties, PropertiesData},
};
use optd_dsl::analyzer::hir::{
    CoreData, GroupId, Literal, LogicalOp, Materializable, Operator, Value,
};
use std::sync::Arc;
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
        PartialLogicalPlan::Materialized(node) => {
            // For materialized logical operators, we create a Value with the operator data
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: convert_children_to_values(&node.children, partial_logical_to_value),
            };

            Value(Logical(LogicalOp(Materialized(operator))))
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
        OperatorData::Float64(f) => Value(Literal(Float64(*f))),
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
pub(crate) fn physical_properties_to_value(properties: &PhysicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        None => Value(Null),
    }
}

/// Converts a PropertiesData to a HIR Value representation.
fn properties_data_to_value(data: &PropertiesData) -> Value {
    match data {
        PropertiesData::Int64(i) => Value(Literal(Int64(*i))),
        PropertiesData::Float64(f) => Value(Literal(Float64(*f))),
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
