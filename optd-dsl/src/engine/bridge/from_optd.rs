//! Contains functions to convert from Optd-IR representations to HIR Value objects.
//!
//! This submodule provides the functionality to transform the optimizer's intermediate
//! representation (Optd-IR) into the DSL's internal representation (HIR). It allows
//! query plans from the optimization engine to be manipulated using the DSL.

use crate::analyzer::hir::{CoreData, Literal, Materializable, Operator, OperatorKind, Value};
use optd_core::cascades::ir::{
    Child, OperatorData, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan,
};
use CoreData::*;
use Literal::*;
use Materializable::*;
use OperatorKind::*;

/// Converts a PartialLogicalPlan into a HIR Value representation.
///
/// This function transforms the optimizer's intermediate representation of a logical
/// operator into the DSL's internal representation, allowing the DSL to work with
/// plans generated by the optimizer.
///
/// # Parameters
/// * `plan` - The PartialLogicalPlan to convert
///
/// # Returns
/// A HIR Value representation of the input plan
pub(crate) fn partial_logical_to_value(plan: &PartialLogicalPlan) -> Value {
    match plan {
        PartialLogicalPlan::UnMaterialized(group_id) => Value(Operator(Group(group_id.0, Logical))),
        PartialLogicalPlan::PartialMaterialized {
            tag,
            data,
            relational_children,
            scalar_children,
        } => Value(Operator(Data(Operator {
            kind: Logical,
            tag: tag.clone(),
            operator_data: convert_operator_data(data),
            relational_children: convert_children(relational_children),
            scalar_children: convert_children(scalar_children),
        }))),
    }
}

/// Converts a PartialScalarPlan into a HIR Value representation.
///
/// This function transforms the optimizer's intermediate representation of a scalar
/// operator into the DSL's internal representation.
///
/// # Parameters
/// * `plan` - The PartialScalarPlan to convert
///
/// # Returns
/// A HIR Value representation of the input plan
pub(crate) fn partial_scalar_to_value(plan: &PartialScalarPlan) -> Value {
    match plan {
        PartialScalarPlan::UnMaterialized(group_id) => Value(Operator(Group(group_id.0, Scalar))),
        PartialScalarPlan::PartialMaterialized {
            tag,
            data,
            scalar_children,
        } => Value(Operator(Data(Operator {
            kind: Scalar,
            tag: tag.clone(),
            operator_data: convert_operator_data(data),
            relational_children: vec![], // Scalar ops don't have relational children
            scalar_children: convert_children(scalar_children),
        }))),
    }
}

/// Converts a PartialPhysicalPlan into a HIR Value representation.
///
/// This function transforms the optimizer's intermediate representation of a physical
/// operator into the DSL's internal representation.
///
/// # Parameters
/// * `plan` - The PartialPhysicalPlan to convert
///
/// # Returns
/// A HIR Value representation of the input plan
pub(crate) fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    match plan {
        PartialPhysicalPlan::UnMaterialized(group_id) => {
            Value(Operator(Group(group_id.0, Physical)))
        }
        PartialPhysicalPlan::PartialMaterialized {
            tag,
            data,
            relational_children,
            scalar_children,
        } => Value(Operator(Data(Operator {
            kind: Physical,
            tag: tag.clone(),
            operator_data: convert_operator_data(data),
            relational_children: convert_children(relational_children),
            scalar_children: convert_children(scalar_children),
        }))),
    }
}

/// Helper trait to convert children to Value
trait ToValue {
    fn to_value(&self) -> Value;
}

impl ToValue for PartialLogicalPlan {
    fn to_value(&self) -> Value {
        partial_logical_to_value(self)
    }
}

impl ToValue for PartialScalarPlan {
    fn to_value(&self) -> Value {
        partial_scalar_to_value(self)
    }
}

impl ToValue for PartialPhysicalPlan {
    fn to_value(&self) -> Value {
        partial_physical_to_value(self)
    }
}

/// Convert a Vec of Children to Vec of Values
fn convert_children<T: ToValue>(children: &[Child<T>]) -> Vec<Value> {
    children
        .iter()
        .map(|child_group| match child_group {
            Child::Singleton(child) => child.to_value(),
            Child::VarLength(children) => Value(Array(
                children.iter().map(|child| child.to_value()).collect(),
            )),
        })
        .collect()
}

/// Converts operator data to Values
fn convert_operator_data(data: &[OperatorData]) -> Vec<Value> {
    data.iter().map(operator_data_to_value).collect()
}

/// Converts an OperatorData into a HIR Value representation
fn operator_data_to_value(data: &OperatorData) -> Value {
    match data {
        OperatorData::Int64(i) => Value(Literal(Int64(*i))),
        OperatorData::Float64(f) => Value(Literal(Float64(**f))),
        OperatorData::String(s) => Value(Literal(String(s.clone()))),
        OperatorData::Bool(b) => Value(Literal(Bool(*b))),
        OperatorData::Struct(name, elements) => {
            Value(Struct(name.clone(), convert_operator_data(elements)))
        }
        OperatorData::Array(elements) => Value(Array(convert_operator_data(elements))),
    }
}
