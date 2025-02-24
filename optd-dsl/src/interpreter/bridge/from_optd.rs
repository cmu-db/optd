use crate::analyzer::hir::{
    CoreData, Literal, LogicalOp, Materializable, PhysicalOp, ScalarOp, Value,
};
use optd_core::cascades::ir::{
    Children, OperatorData, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan,
};
use CoreData::*;
use Literal::*;
use Materializable::*;

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
        PartialLogicalPlan::UnMaterialized(group_id) => Value(Logical(Group(group_id.0))),
        PartialLogicalPlan::PartialMaterialized {
            tag,
            data,
            relational_children,
            scalar_children,
        } => Value(Logical(Data(LogicalOp {
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
        PartialScalarPlan::UnMaterialized(group_id) => Value(Scalar(Group(group_id.0))),
        PartialScalarPlan::PartialMaterialized {
            tag,
            data,
            scalar_children,
        } => Value(Scalar(Data(ScalarOp {
            tag: tag.clone(),
            operator_data: convert_operator_data(data),
            scalar_children: convert_children(scalar_children),
        }))),
    }
}

/// Converts a PartialPhysicalPlan into a HIR Value representation
pub(crate) fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    match plan {
        PartialPhysicalPlan::UnMaterialized(group_id) => Value(Physical(Group(group_id.0))),
        PartialPhysicalPlan::PartialMaterialized {
            tag,
            data,
            relational_children,
            scalar_children,
        } => Value(Physical(Data(PhysicalOp {
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
