//! Type definitions of logical operators in `optd`.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::Filter;
use join::Join;
use project::Project;
use scan::Scan;

/// Each variant of `LogicalOperator` represents a specific kind of logical operator.
///
/// This type is generic over two types:
/// - `RelLink`: Specifies whether the children relations are other logical operators or a group id.
/// - `ScalarLink`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `LogicalOperator` type in [`LogicalPlan`],
/// [`PartialLogicalPlan`], and [`LogicalExpr`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`LogicalExpr`]: crate::expression::relational::LogicalExpr
#[derive(Clone)]
pub enum LogicalOperator<RelLink, ScalarLink> {
    Scan(Scan<ScalarLink>),
    Filter(Filter<RelLink, ScalarLink>),
    Project(Project<RelLink, ScalarLink>),
    Join(Join<RelLink, ScalarLink>),
}
