//! Converts HIR [`Value`]s into optd's type representations (CIR).
use crate::cir::*;
use Child::*;
use CoreData::*;
use Literal::*;
use Materializable::*;
use optd_dsl::analyzer::hir::{self, CoreData, Literal, Materializable, Value};
use std::sync::Arc;

/// Converts a [`Value`] into a [`PartialLogicalPlan`].
///
/// # Panics
///
/// Panics if the [`Value`] is not a [`Logical`] variant.
pub(crate) fn value_to_partial_logical(value: &Value) -> PartialLogicalPlan {
    match &value.0 {
        Logical(logical_op) => match &logical_op.0 {
            UnMaterialized(group_id) => {
                PartialLogicalPlan::UnMaterialized(hir_group_id_to_cir(group_id))
            }
            Materialized(op) => PartialLogicalPlan::Materialized(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_values_to_children(&op.children, value_to_partial_logical),
            }),
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a [`Value`] into a [`PartialPhysicalPlan`].
///
/// # Panics
///
/// Panics if the [`Value`] is not a [`Physical`] variant.
pub(crate) fn value_to_partial_physical(value: &Value) -> PartialPhysicalPlan {
    match &value.0 {
        Physical(physical_op) => match &physical_op.0 {
            UnMaterialized(hir_goal) => {
                PartialPhysicalPlan::UnMaterialized(hir_goal_to_cir(hir_goal))
            }
            Materialized(op) => PartialPhysicalPlan::Materialized(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_values_to_children(&op.children, value_to_partial_physical),
            }),
        },
        _ => panic!("Expected Physical CoreData variant, found: {:?}", value.0),
    }
}

/// Converts a [`Value`] into a CIR [`Cost`].
///
/// # Panics
///
/// Panics if the [`Value`] is not a [`Literal`] variant with a [`Float64`] value.
pub(crate) fn value_to_cost(value: &Value) -> Cost {
    match &value.0 {
        Literal(Float64(f)) => Cost(*f),
        _ => panic!("Expected Float64 literal, found: {:?}", value.0),
    }
}

/// Converts an HIR properties [`Value`] into a CIR [`LogicalProperties`].
pub(crate) fn value_to_logical_properties(properties_value: &Value) -> LogicalProperties {
    match &properties_value.0 {
        Null => LogicalProperties(None),
        _ => LogicalProperties(Some(value_to_properties_data(properties_value))),
    }
}

/// Convert an HIR properties [`Value`] into a CIR [`PhysicalProperties`].
fn value_to_physical_properties(properties_value: &Value) -> PhysicalProperties {
    match &properties_value.0 {
        Null => PhysicalProperties(None),
        _ => PhysicalProperties(Some(value_to_properties_data(properties_value))),
    }
}

/// Converts an HIR [`GroupId`](hir::GroupId) to a CIR [`GroupId`].
///
/// This function provides a consistent way to convert group identifiers from the HIR into the
/// optimizer's internal representation (CIR).
pub(crate) fn hir_group_id_to_cir(hir_group_id: &hir::GroupId) -> GroupId {
    GroupId(hir_group_id.0)
}

/// Converts an HIR [`Goal`](hir::Goal) to a CIR [`Goal`].
pub(crate) fn hir_goal_to_cir(hir_goal: &hir::Goal) -> Goal {
    let group_id = hir_group_id_to_cir(&hir_goal.group_id);
    let properties = value_to_physical_properties(&hir_goal.properties);
    Goal(group_id, properties)
}

/// Converts a [`Value`] into a fully materialized [`LogicalPlan`].
///
/// We use this function when materializing a logical expression for use in properties.
///
/// # Panics
///
/// Panics if the [`Value`] is not a [`Logical`] variant or if the [`Logical`] variant is not a
/// [`Materialized`] variant.
fn value_to_logical(value: &Value) -> LogicalPlan {
    match &value.0 {
        Logical(logical_op) => match &logical_op.0 {
            UnMaterialized(_) => {
                panic!("Cannot convert UnMaterialized LogicalOperator to LogicalPlan")
            }
            Materialized(op) => LogicalPlan(Operator {
                tag: op.tag.clone(),
                data: convert_values_to_operator_data(&op.data),
                children: convert_values_to_children(&op.children, value_to_logical),
            }),
        },
        _ => panic!("Expected Logical CoreData variant, found: {:?}", value.0),
    }
}

/// A generic function to convert a slice of [`Value`]s into a vector of mapped results via the
/// input `converter` function.
fn convert_values_to_children<T, F>(values: &[Value], converter: F) -> Vec<Child<Arc<T>>>
where
    F: Fn(&Value) -> T,
    T: 'static,
{
    values
        .iter()
        .map(|value| match &value.0 {
            Array(elements) => VarLength(
                elements
                    .iter()
                    .map(|elem| Arc::new(converter(elem)))
                    .collect(),
            ),
            _ => Singleton(Arc::new(converter(value))),
        })
        .collect()
}

/// Converts a slice of [`Value`]s into a vector of [`OperatorData`].
fn convert_values_to_operator_data(values: &[Value]) -> Vec<OperatorData> {
    values.iter().map(value_to_operator_data).collect()
}

/// Converts a slice of [`Value`]s into a vector of [`PropertiesData`].
fn convert_values_to_properties_data(values: &[Value]) -> Vec<PropertiesData> {
    values.iter().map(value_to_properties_data).collect()
}

/// Converts a [`Value`] into an [`OperatorData`] representation.
///
/// # Panics
///
/// Panics if the [`Value`] cannot be converted to [`OperatorData`], such as a [`Unit`] literal.
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

/// Converts a [`Value`] into a [`PropertiesData`] representation.
///
/// # Panics
///
/// Panics if the [`Value`] cannot be converted to [`PropertiesData`], such as a [`Unit`] literal.
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
        Logical(_) => PropertiesData::Logical(value_to_logical(value)),
        _ => panic!("Cannot convert {:?} to PropertyData content", value.0),
    }
}
