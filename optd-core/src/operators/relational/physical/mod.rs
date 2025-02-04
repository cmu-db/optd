//! Type definitions of physical operators in optd.

// TODO(connor):
// The module structure here is somewhat questionable, as it has multiple physical operators that
// should really only have 1 implementor (filter and project).
// For now, we can hold off on documenting stuff here until that is stabilized.
#![allow(missing_docs)]

pub mod filter;
pub mod join;
pub mod scan;

use filter::filter::PhysicalFilter;
use join::{hash_join::HashJoin, merge_join::MergeJoin, nested_loop_join::NestedLoopJoin};
use scan::table_scan::TableScan;
use serde::{Deserialize, Serialize};

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
pub enum PhysicalOperator<Metadata, Relation, Scalar> {
    TableScan(TableScan<Metadata, Scalar>),
    Filter(PhysicalFilter<Relation, Scalar>),
    HashJoin(HashJoin<Metadata, Relation, Scalar>),
    NestedLoopJoin(NestedLoopJoin<Metadata, Relation, Scalar>),
    SortMergeJoin(MergeJoin<Metadata, Relation, Scalar>),
}

#[derive(Serialize, Deserialize)]
pub enum PhysicalOperatorKind {
    TableScan,
    Filter,
    HashJoin,
    NestedLoopJoin,
    SortMergeJoin,
}

impl<Value, Relation, Scalar> PhysicalOperator<Value, Relation, Scalar>
where
    Value: Clone,
    Relation: Clone,
    Scalar: Clone,
{
    fn children_relations(&self) -> Vec<Relation> {
        match self {
            PhysicalOperator::TableScan(_) => vec![],
            PhysicalOperator::Filter(filter) => vec![filter.child.clone()],
            PhysicalOperator::HashJoin(join) => {
                vec![join.probe_side.clone(), join.build_side.clone()]
            }
            PhysicalOperator::NestedLoopJoin(join) => vec![join.outer.clone(), join.inner.clone()],
            PhysicalOperator::SortMergeJoin(join) => {
                vec![join.left_sorted.clone(), join.right_sorted.clone()]
            }
        }
    }

    fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            PhysicalOperator::TableScan(scan) => vec![scan.predicate.clone()],
            PhysicalOperator::Filter(filter) => vec![filter.predicate.clone()],
            PhysicalOperator::HashJoin(join) => vec![join.condition.clone()],
            PhysicalOperator::NestedLoopJoin(join) => vec![join.condition.clone()],
            PhysicalOperator::SortMergeJoin(join) => vec![join.condition.clone()],
        }
    }
}
