//! Converts optd's type representations (CIR) into DSL [`Value`]s (HIR).

use crate::cir::*;
use crate::dsl::analyzer::hir::{
    self, CoreData, Literal, LogicalOp, Materializable, Operator, PhysicalOp, Value,
};

/// Converts a [`PartialLogicalPlan`] into a [`Value`], optionally associating it with a [`GroupId`].
///
/// If the plan is unmaterialized, it will be represented as a [`Value`] containing a group reference.
/// If the plan is materialized, the resulting [`Value`] contains the operator data, and may include
/// a group ID if provided.
pub fn partial_logical_to_value(plan: &PartialLogicalPlan, group_id: Option<GroupId>) -> Value {
    use Child::*;
    use Materializable::*;

    match plan {
        PartialLogicalPlan::UnMaterialized(gid) => {
            // Represent unmaterialized logical operators using their group ID.
            Value::new(CoreData::Logical(UnMaterialized(hir::GroupId(gid.0))))
        }
        PartialLogicalPlan::Materialized(node) => {
            // Convert each child to a Value recursively.
            let children: Vec<Value> = node
                .children
                .iter()
                .map(|child| match child {
                    Singleton(item) => partial_logical_to_value(item, None),
                    VarLength(items) => Value::new(CoreData::Array(
                        items
                            .iter()
                            .map(|item| partial_logical_to_value(item, None))
                            .collect(),
                    )),
                })
                .collect();

            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children,
            };

            let logical_op = if let Some(gid) = group_id {
                LogicalOp::stored_logical(operator, cir_group_id_to_hir(&gid))
            } else {
                LogicalOp::logical(operator)
            };

            Value::new(CoreData::Logical(Materialized(logical_op)))
        }
    }
}

/// Converts a [`PartialPhysicalPlan`] into a [`Value`].
pub fn partial_physical_to_value(plan: &PartialPhysicalPlan) -> Value {
    use Child::*;
    use Materializable::*;

    match plan {
        PartialPhysicalPlan::UnMaterialized(goal) => {
            // For unmaterialized physical operators, we create a `Value` with the goal
            let hir_goal = cir_goal_to_hir(goal);
            Value::new(CoreData::Physical(UnMaterialized(hir_goal)))
        }
        PartialPhysicalPlan::Materialized(node) => {
            // For materialized physical operators, we create a Value with the operator data
            let operator = Operator {
                tag: node.tag.clone(),
                data: convert_operator_data_to_values(&node.data),
                children: node
                    .children
                    .iter()
                    .map(|child| match child {
                        Singleton(item) => partial_physical_to_value(item),
                        VarLength(items) => Value::new(CoreData::Array(
                            items
                                .iter()
                                .map(|item| partial_physical_to_value(item))
                                .collect(),
                        )),
                    })
                    .collect(),
            };

            Value::new(CoreData::Physical(Materialized(PhysicalOp::physical(
                operator,
            ))))
        }
    }
}

/// Converts [`LogicalProperties`] into a [`Value`].
pub fn logical_properties_to_value(properties: &LogicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        Option::None => Value::new(CoreData::None),
    }
}

/// Converts [`PhysicalProperties`] into a [`Value`].
pub fn physical_properties_to_value(properties: &PhysicalProperties) -> Value {
    match &properties.0 {
        Some(data) => properties_data_to_value(data),
        Option::None => Value::new(CoreData::None),
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

/// Converts a slice of [`OperatorData`] into a vector of [`Value`]s.
fn convert_operator_data_to_values(data: &[OperatorData]) -> Vec<Value> {
    data.iter().map(operator_data_to_value).collect()
}

/// Converts an [`OperatorData`] into a [`Value`].
fn operator_data_to_value(data: &OperatorData) -> Value {
    use Literal::*;

    match data {
        OperatorData::Int64(i) => Value::new(CoreData::Literal(Int64(*i))),
        OperatorData::Float64(f) => Value::new(CoreData::Literal(Float64(**f))),
        OperatorData::String(s) => Value::new(CoreData::Literal(String(s.clone()))),
        OperatorData::Bool(b) => Value::new(CoreData::Literal(Bool(*b))),
        OperatorData::Struct(name, elements) => Value::new(CoreData::Struct(
            name.clone(),
            convert_operator_data_to_values(elements),
        )),
        OperatorData::Array(elements) => {
            Value::new(CoreData::Array(convert_operator_data_to_values(elements)))
        }
    }
}

/// Converts a [`PropertiesData`] into a [`Value`].
fn properties_data_to_value(data: &PropertiesData) -> Value {
    use Literal::*;

    match data {
        PropertiesData::Int64(i) => Value::new(CoreData::Literal(Int64(*i))),
        PropertiesData::Float64(f) => Value::new(CoreData::Literal(Float64(**f))),
        PropertiesData::String(s) => Value::new(CoreData::Literal(String(s.clone()))),
        PropertiesData::Bool(b) => Value::new(CoreData::Literal(Bool(*b))),
        PropertiesData::Struct(name, elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value::new(CoreData::Struct(name.clone(), values))
        }
        PropertiesData::Array(elements) => {
            let values = elements.iter().map(properties_data_to_value).collect();
            Value::new(CoreData::Array(values))
        }
    }
}
