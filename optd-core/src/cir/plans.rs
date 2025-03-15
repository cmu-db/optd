use super::{
    goal::Goal,
    group::GroupId,
    operators::{Child, Operator},
};
use std::sync::Arc;

/// Represents a fully materialized logical query plan.
///
/// A logical plan consists of logical operators arranged in a tree structure, where each logical
/// operator has logical plans as children.
///
/// Can also represent a DAG structure due to the use of shared [`Arc`]s.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalPlan(pub Operator<Arc<LogicalPlan>>);

/// Represents a fully materialized physical query plan.
///
/// A physical plan consists of physical operators arranged in a tree structure, where each physical
/// operator has physical plans as children.
///
/// Can also represent a DAG structure due to the use of shared [`Arc`]s.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan(pub Operator<Arc<PhysicalPlan>>);

/// Represents partially materialized logical plans.
///
/// Partial logical plans are partially materialized [`LogicalPlan`]s, representing a connected
/// sub-tree/DAG. When we say partial, we mean that parts of the plan are not fully materialized,
/// allowing for more efficient rule matching and binding. However, the parts that _are_
/// materialized are connected, and they form a valid tree/DAG.
///
/// The nodes of a partial plan can either be unmaterialized [`GroupId`]s or materialized
/// [`Operator`]s (that have other partial plan nodes as children). Because of this, you can cast
/// both logical expressions (a single operator with groups as children) and logical plans (a
/// tree/DAG of operators) into a `PartialLogicalPlan`. Note that a partial plan with only an
/// unmaterialized group at the root is not only possible, but common.
///
/// We use partial plans because most rules only need to match and bind to a specific section of a
/// plan (the root and maybe some of its children), and it would be expensive to materialize the
/// entire plan when the rule engine only needs to know about the top. In other words, partial plans
/// allow us to give the rule engine exactly what it needs and nothing else.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    /// A fully materialized logical plan with explicit operator and children
    Materialized(Operator<Arc<PartialLogicalPlan>>),
    /// A reference to a logical group in the memo structure
    UnMaterialized(GroupId),
}

/// Represents a partially materialized physical plan.
///
/// Partial physical plans are partially materialized [`PhysicalPlan`]s, representing a connected
/// sub-tree/DAG. When we say partial, we mean that parts of the plan are not fully materialized,
/// allowing for more efficient rule matching and binding. However, the parts that _are_
/// materialized are connected, and they form a valid tree/DAG.
///
/// The nodes of a partial plan can either be unmaterialized [`Goal`]s or materialized [`Operator`]s
/// (that have other partial plan nodes as children). Because of this, you can cast both physical
/// expressions (a single operator with groups as children) and physical plans (a tree/DAG of
/// operators) into a `PartialPhysicalPlan`. Note that a partial plan with only an unmaterialized
/// group at the root is not only possible, but common.
///
/// We use partial plans because most rules only need to match and bind to the top of the
/// plan (the root and maybe some of its children), and it would be expensive to materialize the
/// entire plan when the rule engine only needs to know about the top. In other words, partial plans
/// allow us to give the rule engine exactly what it needs and nothing else.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    /// A fully materialized physical plan with explicit operator and children.
    Materialized(Operator<Arc<PartialPhysicalPlan>>),
    /// A reference to a physical goal.
    UnMaterialized(Goal),
}

impl From<GroupId> for PartialLogicalPlan {
    /// Converts a logical group ID to an unmaterialized partial logical plan.
    fn from(group_id: GroupId) -> PartialLogicalPlan {
        PartialLogicalPlan::UnMaterialized(group_id)
    }
}

impl From<Goal> for PartialPhysicalPlan {
    /// Converts a physical goal to an unmaterialized partial physical plan.
    fn from(goal: Goal) -> PartialPhysicalPlan {
        PartialPhysicalPlan::UnMaterialized(goal)
    }
}

impl From<LogicalPlan> for PartialLogicalPlan {
    /// Converts a fully materialized logical plan to a partial logical plan.
    ///
    /// This recursively converts all children to their partial equivalents.
    fn from(plan: LogicalPlan) -> Self {
        PartialLogicalPlan::Materialized(Operator {
            tag: plan.0.tag,
            data: plan.0.data,
            children: convert_children::<LogicalPlan, Self>(plan.0.children),
        })
    }
}

impl From<PhysicalPlan> for PartialPhysicalPlan {
    /// Converts a fully materialized physical plan to a partial physical plan.
    ///
    /// This recursively converts all children to their partial equivalents.
    fn from(plan: PhysicalPlan) -> Self {
        PartialPhysicalPlan::Materialized(Operator {
            tag: plan.0.tag,
            data: plan.0.data,
            children: convert_children::<PhysicalPlan, Self>(plan.0.children),
        })
    }
}

/// A generic function that converts children with type `Arc<S>` into a `Arc<T>`.
///
/// This handles both singleton and variable-length children, converting each source type into its
/// equivalent target wrapped in an [`Arc`].
fn convert_children<S, T>(children: Vec<Child<Arc<S>>>) -> Vec<Child<Arc<T>>>
where
    S: Clone,
    S: Into<T>,
{
    children
        .into_iter()
        .map(|child| match child {
            Child::Singleton(plan) => Child::Singleton(Arc::new((*plan).clone().into())),
            Child::VarLength(plans) => Child::VarLength(
                plans
                    .into_iter()
                    .map(|plan| Arc::new((*plan).clone().into()))
                    .collect(),
            ),
        })
        .collect()
}
