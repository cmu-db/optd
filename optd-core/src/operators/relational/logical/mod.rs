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
/// - `Relation`: Specifies whether the children relations are other logical operators or a group id.
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `LogicalOperator` type in [`LogicalPlan`],
/// [`PartialLogicalPlan`], and [`LogicalExpression`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`LogicalExpression`]: crate::expression::LogicalExpression
#[derive(Clone)]
pub enum LogicalOperator<Metadata, Relation, Scalar> {
    Scan(Scan<Metadata, Scalar>),
    Filter(Filter<Relation, Scalar>),
    Project(Project<Relation, Scalar>),
    Join(Join<Metadata, Relation, Scalar>),
}
