use super::{
    goal::Goal,
    group::GroupId,
    operators::{Child, Operator},
};
use std::sync::Arc;

//=============================================================================
// Core Plan Types
//=============================================================================

/// Represents a fully materialized logical query plan.
///
/// A logical plan consists of logical operators arranged in a tree structure,
/// with each operator having children.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan(pub Operator<Arc<LogicalPlan>>);

/// Represents a fully materialized physical query plan.
///
/// A physical plan consists of physical operators arranged in a tree structure,
/// with each operator having children.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan(pub Operator<Arc<PhysicalPlan>>);

//=============================================================================
// Partial Plan Types
//=============================================================================

/// Represents a partially materialized logical plan.
///
/// Partial logical plans can either be fully materialized with operator trees,
/// or unmaterialized references to logical groups in the memo structure.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    /// A fully materialized logical plan with explicit operator and children
    Materialized(Operator<Arc<PartialLogicalPlan>>),
    /// A reference to a logical group in the memo structure
    UnMaterialized(GroupId),
}

/// Represents a partially materialized physical plan.
///
/// Partial physical plans can either be fully materialized with operator trees,
/// or unmaterialized references to physical goals.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    /// A fully materialized physical plan with explicit operator and children
    Materialized(Operator<Arc<PartialPhysicalPlan>>),
    /// A reference to a physical goal
    UnMaterialized(Goal),
}

//=============================================================================
// Conversion Implementations
//=============================================================================

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
            children: convert_children(plan.0.children),
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
            children: convert_children(plan.0.children),
        })
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

/// Generic function to convert a collection of plan children to partial plan children.
///
/// This handles both singleton and variable-length children, recursively
/// converting each child plan to its partial equivalent.
///
/// # Type Parameters
/// * `S` - Source plan type
/// * `T` - Target partial plan type
///
/// # Arguments
/// * `children` - Collection of child plans to convert
///
/// # Returns
/// * Converted collection of partial plan children
pub(super) fn convert_children<S, T>(children: Vec<Child<Arc<S>>>) -> Vec<Child<Arc<T>>>
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
