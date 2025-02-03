//! Type definitions of physical operators in optd.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::filter::Filter;
use join::{hash_join::HashJoin, merge_join::MergeJoin, nested_loop_join::NestedLoopJoin};
use project::project::Project;
use scan::table_scan::TableScan;

/// Each variant of `PhysicalOperator` represents a specific kind of physical operator.
///
/// This type is generic over two types:
/// - `Relation`: Specifies whether the children relations are other physical operators or a group
///   id.
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `PhysicalOperator` type in [`PhysicalPlan`]
/// and [`PhysicalExpression`].
///
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PhysicalExpression`]: crate::expression::PhysicalExpression
#[derive(Clone)]
pub enum PhysicalOperator<Metadata, Relation, Scalar> {
    TableScan(TableScan<Metadata, Scalar>),
    Filter(Filter<Relation, Scalar>),
    Project(Project<Relation, Scalar>),
    HashJoin(HashJoin<Metadata, Relation, Scalar>),
    NestedLoopJoin(NestedLoopJoin<Metadata, Relation, Scalar>),
    SortMergeJoin(MergeJoin<Metadata, Relation, Scalar>),
}
