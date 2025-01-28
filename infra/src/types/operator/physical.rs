//! Type representations of physical operators in (materialized) query plans.

use std::sync::Arc;

use crate::types::operator::Scalar;

/// A type representing a physical operator in an output physical query execution plan.
///
/// Each variant of `PhysicalOperator` represents a specific kind of logical operator. The current
/// supported operators are `TableScan`, `PhysicalFilter`, and `HashJoin`.
///
/// This type is generic over a `Link` type, which specifies what kind of children this operator is
/// allowed to have in whatever kind of plan it is contained in. This makes it possible to reuse the
/// `PhysicalOperator` type in differnt kinds of trees.
///
/// For example, `PhysicalOperator` _**is**_ a valid operator in [`PhysicalPlan`], _**but it is not
/// a valid operator in**_ [`LogicalPlan`] nor [`PartialLogicalPlan`].
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`PartialPhysicalPlan`]: crate::plan::partial_physical_plan::PartialPhysicalPlan
pub enum PhysicalOperator<Link> {
    TableScan(TableScanOperator),
    Filter(PhysicalFilterOperator<Link>),
    HashJoin(HashJoinOperator<Link>),
}

/// TODO Add docs.
pub struct TableScanOperator {
    pub table_name: String,
    pub predicate: Option<Scalar>,
}

/// TODO Add docs.
pub struct PhysicalFilterOperator<Link> {
    pub child: Link,
    pub predicate: Scalar,
}

/// TODO Add docs.
pub struct HashJoinOperator<Link> {
    pub join_type: (),
    pub left: Link,
    pub right: Link,
    pub condition: Arc<Vec<(Scalar, Scalar)>>,
}
