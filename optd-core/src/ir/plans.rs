use super::{
    goal::PhysicalGoal,
    groups::{LogicalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, PhysicalOperator, ScalarOperator},
};
use std::sync::Arc;

//=============================================================================
// Core Plan Types
//=============================================================================

/// Represents a fully materialized logical query plan.
///
/// A logical plan consists of logical operators arranged in a tree structure,
/// with each operator having possible relational and scalar children.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    /// The root operator of this logical plan
    pub node: LogicalOperator<Arc<LogicalPlan>, Arc<ScalarPlan>>,
}

/// Represents a fully materialized scalar expression plan.
///
/// A scalar plan consists of scalar operators arranged in a tree structure.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarPlan {
    /// The root operator of this scalar plan
    pub node: ScalarOperator<Arc<ScalarPlan>>,
}

/// Represents a fully materialized physical query plan.
///
/// A physical plan consists of physical operators arranged in a tree structure,
/// with each operator having possible physical and scalar children.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    /// The root operator of this physical plan
    pub node: PhysicalOperator<Arc<PhysicalPlan>, Arc<ScalarPlan>>,
}

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
    PartialMaterialized {
        /// The root operator of this partial logical plan
        node: LogicalOperator<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },
    /// A reference to a logical group in the memo structure
    UnMaterialized(LogicalGroupId),
}

/// Represents a partially materialized scalar plan.
///
/// Partial scalar plans can either be fully materialized with operator trees,
/// or unmaterialized references to scalar groups in the memo structure.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    /// A fully materialized scalar plan with explicit operator and children
    PartialMaterialized {
        /// The root operator of this partial scalar plan
        node: ScalarOperator<Arc<PartialScalarPlan>>,
    },
    /// A reference to a scalar group in the memo structure
    UnMaterialized(ScalarGroupId),
}

/// Represents a partially materialized physical plan.
///
/// Partial physical plans can either be fully materialized with operator trees,
/// or unmaterialized references to physical goals.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialPhysicalPlan {
    /// A fully materialized physical plan with explicit operator and children
    PartialMaterialized {
        /// The root operator of this partial physical plan
        node: PhysicalOperator<Arc<PartialPhysicalPlan>, Arc<PartialScalarPlan>>,
    },
    /// A reference to a physical goal
    UnMaterialized(PhysicalGoal),
}

//=============================================================================
// Conversion Implementations
//=============================================================================

impl From<LogicalGroupId> for PartialLogicalPlan {
    /// Converts a logical group ID to an unmaterialized partial logical plan.
    fn from(group_id: LogicalGroupId) -> PartialLogicalPlan {
        PartialLogicalPlan::UnMaterialized(group_id)
    }
}

impl From<ScalarGroupId> for PartialScalarPlan {
    /// Converts a scalar group ID to an unmaterialized partial scalar plan.
    fn from(group_id: ScalarGroupId) -> PartialScalarPlan {
        PartialScalarPlan::UnMaterialized(group_id)
    }
}

impl From<PhysicalGoal> for PartialPhysicalPlan {
    /// Converts a physical goal to an unmaterialized partial physical plan.
    fn from(goal: PhysicalGoal) -> PartialPhysicalPlan {
        PartialPhysicalPlan::UnMaterialized(goal)
    }
}

impl From<LogicalPlan> for PartialLogicalPlan {
    /// Converts a fully materialized logical plan to a partial logical plan.
    ///
    /// This recursively converts all children (both relational and scalar)
    /// to their partial equivalents.
    fn from(plan: LogicalPlan) -> Self {
        PartialLogicalPlan::PartialMaterialized {
            node: LogicalOperator {
                tag: plan.node.tag,
                data: plan.node.data,
                relational_children: convert_children(plan.node.relational_children),
                scalar_children: convert_children(plan.node.scalar_children),
            },
        }
    }
}

impl From<ScalarPlan> for PartialScalarPlan {
    /// Converts a fully materialized scalar plan to a partial scalar plan.
    ///
    /// This recursively converts all children to their partial equivalents.
    fn from(plan: ScalarPlan) -> Self {
        PartialScalarPlan::PartialMaterialized {
            node: ScalarOperator {
                tag: plan.node.tag,
                data: plan.node.data,
                children: convert_children(plan.node.children),
            },
        }
    }
}

impl From<PhysicalPlan> for PartialPhysicalPlan {
    /// Converts a fully materialized physical plan to a partial physical plan.
    ///
    /// This recursively converts all children (both physical and scalar)
    /// to their partial equivalents.
    fn from(plan: PhysicalPlan) -> Self {
        PartialPhysicalPlan::PartialMaterialized {
            node: PhysicalOperator {
                tag: plan.node.tag,
                data: plan.node.data,
                relational_children: convert_children(plan.node.relational_children),
                scalar_children: convert_children(plan.node.scalar_children),
            },
        }
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
