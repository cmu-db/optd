use crate::analyzer::hir::{CoreData, Literal, Materializable, Operator, OperatorKind, Value};
use optd_core::cascades::ir::{
    Children, OperatorData, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan,
};
use CoreData::*;
use Literal::*;
use Materializable::*;
use OperatorKind::*;

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
fn convert_children<T: ToValue>(children: &[Children<T>]) -> Vec<Value> {
    children
        .iter()
        .map(|child_group| match child_group {
            Children::Singleton(child) => child.to_value(),
            Children::VarLength(children) => Value(Array(
                children.iter().map(|child| child.to_value()).collect(),
            )),
        })
        .collect()
}

/// Converts operator data to Values
fn convert_operator_data(data: &[OperatorData]) -> Vec<Value> {
    data.iter().map(operator_data_to_value).collect()
}

/// Converts a PartialLogicalPlan into a HIR Value representation
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

/// Converts a PartialScalarPlan into a HIR Value representation
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

/// Converts a PartialPhysicalPlan into a HIR Value representation
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
