use super::{
    goal::{Cost, Goal},
    group::GroupId,
    operators::{Child, Operator},
    plans::{PartialLogicalPlan, PartialPhysicalPlan},
};
use std::sync::Arc;

/// A logical expression in the memo structure.
///
/// Logical expressions have [`GroupId`]s as children rather than full plans, representing a compact
/// form suitable for the memo structure.
pub type LogicalExpression = Operator<GroupId>;

/// A unique identifier for a logical expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct LogicalExpressionId(pub i64);

/// A physical expression in the memo structure.
///
/// Physical expressions use [`Goal`]s rather than full plans for their children, representing a
/// compact form suitable for the memo structure.
pub type PhysicalExpression = Operator<Goal>;

/// A unique identifier for a physical expression.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct PhysicalExpressionId(pub i64);

/// An optimized physical expression with its associated cost.
#[derive(Clone, Debug)]
pub struct OptimizedExpression(pub PhysicalExpression, pub Cost);

impl From<LogicalExpression> for PartialLogicalPlan {
    /// Converts a logical expression into a [`PartialLogicalPlan`].
    ///
    /// This creates a materialized partial plan where each child group ID is converted to an
    /// unmaterialized partial plan reference.
    fn from(expr: LogicalExpression) -> Self {
        PartialLogicalPlan::Materialized(Operator {
            tag: expr.tag,
            data: expr.data,
            children: convert_children::<GroupId, Self>(expr.children),
        })
    }
}

impl From<PhysicalExpression> for PartialPhysicalPlan {
    /// Converts a physical expression to a partial physical plan.
    ///
    /// This creates a materialized partial plan where each child goal ID is converted to an
    /// unmaterialized partial plan reference.
    fn from(expr: PhysicalExpression) -> Self {
        PartialPhysicalPlan::Materialized(Operator {
            tag: expr.tag,
            data: expr.data,
            children: convert_children::<Goal, Self>(expr.children),
        })
    }
}

/// A generic function that converts children with type `S` into a `Arc<T>`.
///
/// This handles both singleton and variable-length children, converting each source type into its
/// equivalent target wrapped in an [`Arc`].
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
