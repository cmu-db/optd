use crate::cir::{
    goal::Goal,
    group::GroupId,
    operators::{Child, Operator, OperatorData},
    plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan},
    properties::{LogicalProperties, PhysicalProperties, PropertiesData},
};
use optd_dsl::analyzer::hir::{self, CoreData, Literal, Materializable, Value};
use std::sync::Arc;
use Child::*;
use CoreData::*;
use Literal::*;
use Materializable::*;

//=============================================================================
// Main conversion functions
//=============================================================================

/// Converts a HIR Value into a PartialLogicalPlan representation.
///
/// Transforms the DSL's HIR into the CIR for logical operators.
pub(crate) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        Logical(logical_op) => match &logical_op.0 {
            UnMaterialized(group_id) => PartialLogicalPlan::UnMaterialized(GroupId(group_id.0)),
            Materialized(op) => PartialLogicalPlan::Materialized(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_children_into(&op.children, value_to_partial_logical),
            }),
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a HIR Value into a PartialPhysicalPlan representation.
///
/// Transforms the DSL's HIR into the CIR for physical operators.
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        Physical(physical_op) => match &physical_op.0 {
            UnMaterialized(hir_goal) => {
                let ir_goal = Goal(
                    GroupId(hir_goal.group_id.0),
                    value_to_physical_properties(&hir_goal.properties),
                );

                PartialPhysicalPlan::UnMaterialized(ir_goal)
            }
            Materialized(op) => PartialPhysicalPlan::Materialized(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_children_into(&op.children, value_to_partial_physical),
            }),
        },
        _ => panic!("Expected Physical CoreData variant, found: {:?}", value.0),
    }
}

/// Convert HIR properties value to CIR LogicalProperties
pub(crate) fn value_to_logical_properties(properties_value: &Value) -> LogicalProperties {
    match &properties_value.0 {
        Null => LogicalProperties(None),
        _ => LogicalProperties(Some(value_to_properties_data(properties_value))),
    }
}

/// Converts a HIR GroupId to a CIR GroupId.
///
/// This function provides a consistent way to convert group identifiers
/// from the HIR representation to the optimizer's internal representation.
pub(crate) fn hir_to_cir_group_id(hir_group_id: &hir::GroupId) -> GroupId {
    GroupId(hir_group_id.0)
}

/// Converts a HIR Goal to a CIR Goal.
///
/// Transforms the DSL's HIR Goal into the optimizer's IR Goal representation,
/// handling both the group ID and physical properties components.
pub(crate) fn hir_to_cir_goal(hir_goal: &hir::Goal) -> Goal {
    let group_id = hir_to_cir_group_id(&hir_goal.group_id);
    let properties = value_to_physical_properties(&hir_goal.properties);
    Goal(group_id, properties)
}

/// Converts a HIR Value into a complete LogicalPlan (not a partial plan).
///
/// Used when fully materializing a logical expression for use in properties.
fn value_to_logical(value: &Value) -> LogicalPlan {
    match &value.0 {
        Logical(logical_op) => match &logical_op.0 {
            UnMaterialized(_) => {
                panic!("Cannot convert UnMaterialized LogicalOperator to LogicalPlan")
            }
            Materialized(op) => LogicalPlan(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_children_into(&op.children, value_to_logical),
            }),
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// Convert HIR properties value to CIR PhysicalProperties
fn value_to_physical_properties(properties_value: &Value) -> PhysicalProperties {
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
            Float64(f) => OperatorData::Float64(*f),
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
            Float64(f) => PropertiesData::Float64(*f),
            String(s) => PropertiesData::String(s.clone()),
            Bool(b) => PropertiesData::Bool(*b),
            Unit => panic!("Cannot convert Unit constant to PropertyData"),
        },
        Array(elements) => PropertiesData::Array(convert_values_to_properties_data(elements)),
        Struct(name, elements) => {
            PropertiesData::Struct(name.clone(), convert_values_to_properties_data(elements))
        }
        Logical(_) => PropertiesData::Logical(value_to_logical(value)),
        _ => panic!("Cannot convert {:?} to PropertyData content", value.0),
    }
}
