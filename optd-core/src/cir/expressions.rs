use super::{
    goal::{Cost, Goal, GoalId},
    group::GroupId,
    operators::{Child, Operator},
    plans::{PartialLogicalPlan, PartialPhysicalPlan},
};
use std::sync::Arc;

//=============================================================================
// Expression Types
//=============================================================================

/// A logical expression in the memo structure.
///
/// Logical expressions use group IDs rather than full plans for their children,
/// representing a compact form suitable for the memo structure.
pub type LogicalExpression = Operator<GroupId>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalExpressionId(pub i64);

/// A physical expression in the memo structure.
///
/// Physical expressions use goal IDs rather than full plans for
/// their children, representing a compact form suitable for the memo structure.
pub type PhysicalExpression = Operator<GoalId>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalExpressionId(pub i64);

/// An optimized physical expression with its associated cost.
#[derive(Clone, Debug)]
pub struct OptimizedExpression(pub PhysicalExpression, pub Cost);

//=============================================================================
// Conversion Implementations
//=============================================================================

impl From<LogicalExpression> for PartialLogicalPlan {
    /// Converts a logical expression to a partial logical plan.
    ///
    /// This creates a materialized partial plan where each child group ID
    /// is converted to an unmaterialized partial plan reference.
    fn from(expr: LogicalExpression) -> Self {
        PartialLogicalPlan::Materialized(Operator {
            tag: expr.tag,
            data: expr.data,
            children: convert_children(expr.children),
        })
    }
}

impl From<Operator<Goal>> for PartialPhysicalPlan {
    /// Converts an operator with Goals to a partial physical plan.
    ///
    /// Note: This takes Operator<Goal> rather than PhysicalExpression (which is Operator<GoalId>)
    /// because physical expressions in partial plans still need the full Goal information.
    fn from(expr: Operator<Goal>) -> Self {
        PartialPhysicalPlan::Materialized(Operator {
            tag: expr.tag,
            data: expr.data,
            children: convert_children(expr.children),
        })
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
/// * `S` - Source type (e.g., GroupId)
/// * `T` - Target type (e.g., PartialLogicalPlan)
///
/// # Arguments
/// * `children` - Collection of child items to convert
///
/// # Returns
/// * Converted collection of partial plan children wrapped in Arc
fn convert_children<S, T>(children: Vec<Child<S>>) -> Vec<Child<Arc<T>>>
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
