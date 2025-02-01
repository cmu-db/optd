//! Type definitions of physical operators in optd.

// TODO(connor):
// The module structure here is somewhat questionable, as it has multiple physical operators that
// should really only have 1 implementor (filter and project).
// For now, we can hold off on documenting stuff here until that is stabilized.
#![allow(missing_docs)]

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::filter::PhysicalFilter;
use join::{hash_join::HashJoin, merge_join::MergeJoin, nested_loop_join::NestedLoopJoin};
use project::project::Project;
use scan::table_scan::TableScan;

use super::RelationChildren;

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
#[allow(missing_docs)]
#[derive(Clone)]
pub enum PhysicalOperator<Relation, Scalar> {
    TableScan(TableScan<Relation, Scalar>),
    Filter(PhysicalFilter<Relation, Scalar>),
    Project(Project<Relation, Scalar>),
    HashJoin(HashJoin<Relation, Scalar>),
    NestedLoopJoin(NestedLoopJoin<Relation, Scalar>),
    SortMergeJoin(MergeJoin<Relation, Scalar>),
}

impl<Relation, Scalar> RelationChildren for PhysicalOperator<Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    type Relation = Relation;
    type Scalar = Scalar;

    fn children_relations(&self) -> Vec<Self::Relation> {
        match self {
            PhysicalOperator::TableScan(scan) => scan.children_relations(),
            PhysicalOperator::Filter(filter) => filter.children_relations(),
            PhysicalOperator::Project(project) => project.children_relations(),
            PhysicalOperator::HashJoin(join) => join.children_relations(),
            PhysicalOperator::NestedLoopJoin(join) => join.children_relations(),
            PhysicalOperator::SortMergeJoin(join) => join.children_relations(),
        }
    }

    fn children_scalars(&self) -> Vec<Self::Scalar> {
        match self {
            PhysicalOperator::TableScan(scan) => scan.children_scalars(),
            PhysicalOperator::Filter(filter) => filter.children_scalars(),
            PhysicalOperator::Project(project) => project.children_scalars(),
            PhysicalOperator::HashJoin(join) => join.children_scalars(),
            PhysicalOperator::NestedLoopJoin(join) => join.children_scalars(),
            PhysicalOperator::SortMergeJoin(join) => join.children_scalars(),
        }
    }
}
