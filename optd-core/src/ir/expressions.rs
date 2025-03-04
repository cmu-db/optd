use super::{
    goal::PhysicalGoal,
    groups::{LogicalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, PhysicalOperator, ScalarOperator},
    plans::{PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan},
};
use std::sync::Arc;

//=============================================================================
// Expression IDs
//=============================================================================

/// Unique identifier for a logical expression within the memo structure.
///
/// Logical expressions represent relational algebra operations like joins,
/// filters, projections, etc. Each expression has a unique ID in the memo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogicalExpressionId(pub i64);

/// Unique identifier for a physical expression within the memo structure.
///
/// Physical expressions represent concrete implementation strategies for
/// logical operations, like hash joins, index scans, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalExpressionId(pub i64);

/// Unique identifier for a scalar expression within the memo structure.
///
/// Scalar expressions represent computations that produce scalar values,
/// like arithmetic operations, function calls, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScalarExpressionId(pub i64);

//=============================================================================
// Expression Types
//=============================================================================

/// A logical expression in the memo structure.
///
/// Logical expressions use group IDs rather than full plans for their children,
/// representing a compact form suitable for the memo optimization structure.
pub type LogicalExpression = LogicalOperator<LogicalGroupId, ScalarGroupId>;

/// A scalar expression in the memo structure.
///
/// Scalar expressions use group IDs rather than full plans for their children,
/// representing a compact form suitable for the memo optimization structure.
pub type ScalarExpression = ScalarOperator<ScalarGroupId>;

/// A physical expression in the memo structure.
///
/// Physical expressions use goal IDs and group IDs rather than full plans for
/// their children, representing a compact form suitable for the memo structure.
pub type PhysicalExpression = PhysicalOperator<PhysicalGoal, ScalarGroupId>;

//=============================================================================
// Conversion Implementations
//=============================================================================

impl From<LogicalExpression> for PartialLogicalPlan {
    /// Converts a logical expression to a partial logical plan.
    ///
    /// This creates a materialized partial plan where each child group ID
    /// is converted to an unmaterialized partial plan reference.
    fn from(expr: LogicalExpression) -> Self {
        PartialLogicalPlan::PartialMaterialized {
            node: LogicalOperator {
                tag: expr.tag,
                data: expr.data,
                relational_children: convert_children(expr.relational_children),
                scalar_children: convert_children(expr.scalar_children),
            },
        }
    }
}

impl From<ScalarExpression> for PartialScalarPlan {
    /// Converts a scalar expression to a partial scalar plan.
    ///
    /// This creates a materialized partial plan where each child group ID
    /// is converted to an unmaterialized partial plan reference.
    fn from(expr: ScalarExpression) -> Self {
        PartialScalarPlan::PartialMaterialized {
            node: ScalarOperator {
                tag: expr.tag,
                data: expr.data,
                children: convert_children(expr.children),
            },
        }
    }
}

impl From<PhysicalExpression> for PartialPhysicalPlan {
    /// Converts a physical expression to a partial physical plan.
    ///
    /// This creates a materialized partial plan where each child goal ID
    /// is converted to an unmaterialized partial plan reference.
    fn from(expr: PhysicalExpression) -> Self {
        PartialPhysicalPlan::PartialMaterialized {
            node: PhysicalOperator {
                tag: expr.tag,
                data: expr.data,
                relational_children: convert_children(expr.relational_children),
                scalar_children: convert_children(expr.scalar_children),
            },
        }
    }
}

//=============================================================================
// Helper Functions
//=============================================================================

/// Generic function to convert a collection of children to partial plan children.
///
/// This handles both singleton and variable-length children, converting
/// each source type into its equivalent target wrapped in an Arc.
///
/// # Type Parameters
/// * `S` - Source type (e.g., LogicalGroupId)
/// * `T` - Target type (e.g., PartialLogicalPlan)
///
/// # Arguments
/// * `children` - Collection of child items to convert
///
/// # Returns
/// * Converted collection of partial plan children wrapped in Arc
pub(super) fn convert_children<S, T>(children: Vec<Child<S>>) -> Vec<Child<Arc<T>>>
where
    S: Into<T>,
{
    children
        .into_iter()
        .map(|child| match child {
            Child::Singleton(source) => Child::Singleton(Arc::new(source.into())),
            Child::VarLength(sources) => Child::VarLength(
                sources
                    .into_iter()
                    .map(|source| Arc::new(source.into()))
                    .collect(),
            ),
        })
        .collect()
}
