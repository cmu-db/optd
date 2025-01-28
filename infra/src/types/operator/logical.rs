//! Type representations of logical operators in (materialized) query plans.

use crate::types::operator::Scalar;

/// A type representing a logical operator in an input logical query plan.
///
/// Each variant of `LogicalOperator` represents a specific kind of logical operator. The current
/// supported operators are `Scan`, `Filter`, and `Join`.
///
/// This type is generic over a `Link` type, which specifies what kind of children this operator is
/// allowed to have in whatever kind of plan it is contained in. This makes it possible to reuse the
/// `LogicalOperator` type in differnt kinds of trees.
///
/// For example, `LogicalOperator` is a valid operator in [`LogicalPlan`], [`PhysicalPlan`], and
/// [`PartialLogicalPlan`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
pub enum LogicalOperator<Link> {
    Scan(LogicalScanOperator),
    Filter(LogicalFilterOperator<Link>),
    Join(LogicalJoinOperator<Link>),
}

/// TODO Add docs.
pub struct LogicalScanOperator {
    pub table_name: String,
    pub predicate: Option<Scalar>,
}

/// TODO Add docs.
pub struct LogicalFilterOperator<Link> {
    pub child: Link,
    pub predicate: Scalar,
}

/// TODO Add docs.
pub struct LogicalJoinOperator<Link> {
    pub join_type: (),
    pub left: Link,
    pub right: Link,
    pub condition: Scalar,
}
