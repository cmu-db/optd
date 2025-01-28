//! Type representations of logical operators in query plans and expressions.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::FilterOperator;
use join::JoinOperator;
use project::ProjectOperator;
use scan::ScanOperator;

/// A type representing a logical operator in an input logical query plan.
///
/// Each variant of `LogicalOperator` represents a specific kind of logical operator. The current
/// supported operators are `Scan`, `Filter`, `Project` and `Join`.
///
/// This type is generic over two types:
/// - `RelLink`: Specifies what kind of children this operator can have for relational operators
/// - `ScalarLink`: Specifies what kind of children scalar expressions can have
///
/// This makes it possible to reuse the `LogicalOperator` type in different kinds of DAGs.
///
/// For example, `LogicalOperator` is a valid operator in [`LogicalPlan`], [`LogicalExpression`],
/// [`PartialLogicalPlan`].
pub enum LogicalOperator<RelLink, ScalarLink> {
    Scan(ScanOperator),
    Filter(FilterOperator<RelLink, ScalarLink>),
    Project(ProjectOperator<RelLink, ScalarLink>),
    Join(JoinOperator<RelLink, ScalarLink>),
}
