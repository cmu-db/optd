//! Type definitions of physical operators in optd.
//!
//! This module provides the core physical operator types and implementations for the query optimizer.
//! Physical operators represent the concrete execution strategies for query operations.

pub mod filter;
pub mod join;
pub mod scan;

use crate::{
    cascades::{
        expressions::PhysicalExpression,
        groups::{RelationalGroupId, ScalarGroupId},
    },
    values::OptdValue,
};
use filter::filter::PhysicalFilter;
use join::{hash_join::HashJoin, merge_join::MergeJoin, nested_loop_join::NestedLoopJoin};
use scan::table_scan::TableScan;

/// Each variant of `PhysicalOperator` represents a specific kind of physical operator.
///
/// This type is generic over three types:
/// - `Value`: The type of values stored in the operator (e.g., for table names, join types)
/// - `Relation`: Specifies whether the children relations are other physical operators or a group id
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id
///
/// This makes it possible to reuse the `PhysicalOperator` type in different contexts:
/// - Pattern matching: Using physical operators for matching rule patterns
/// - Partially materialized plans: Using physical operators during optimization
/// - Fully materialized plans: Using physical operators in physical execution
#[derive(Clone)]
pub enum PhysicalOperator<Value, Relation, Scalar> {
    /// Table scan operator
    TableScan(TableScan<Value, Scalar>),
    /// Filter operator
    Filter(PhysicalFilter<Relation, Scalar>),
    /// Hash join operator
    HashJoin(HashJoin<Value, Relation, Scalar>),
    /// Nested loop join operator
    NestedLoopJoin(NestedLoopJoin<Value, Relation, Scalar>),
    /// Sort-merge join operator
    SortMergeJoin(MergeJoin<Value, Relation, Scalar>),
}

/// The kind of physical operator.
///
/// This enum represents the different types of physical operators available
/// in the system, used for classification and pattern matching.
#[derive(Debug, Clone, PartialEq, sqlx::Type)]
pub enum PhysicalOperatorKind {
    /// Represents a table scan operation
    TableScan,
    /// Represents a filter operation
    Filter,
    /// Represents a hash join operation
    HashJoin,
    /// Represents a nested loop join operation
    NestedLoopJoin,
    /// Represents a sort-merge join operation
    SortMergeJoin,
}

impl<Relation, Scalar> PhysicalOperator<OptdValue, Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    /// Returns the kind of physical operator.
    pub fn operator_kind(&self) -> PhysicalOperatorKind {
        match self {
            PhysicalOperator::TableScan(_) => PhysicalOperatorKind::TableScan,
            PhysicalOperator::Filter(_) => PhysicalOperatorKind::Filter,
            PhysicalOperator::HashJoin(_) => PhysicalOperatorKind::HashJoin,
            PhysicalOperator::NestedLoopJoin(_) => PhysicalOperatorKind::NestedLoopJoin,
            PhysicalOperator::SortMergeJoin(_) => PhysicalOperatorKind::SortMergeJoin,
        }
    }

    /// Returns a vector of child relations for this operator.
    pub fn children_relations(&self) -> Vec<Relation> {
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

    /// Returns a vector of scalar expressions associated with this operator.
    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            PhysicalOperator::TableScan(scan) => vec![scan.predicate.clone()],
            PhysicalOperator::Filter(filter) => vec![filter.predicate.clone()],
            PhysicalOperator::HashJoin(join) => vec![join.condition.clone()],
            PhysicalOperator::NestedLoopJoin(join) => vec![join.condition.clone()],
            PhysicalOperator::SortMergeJoin(join) => vec![join.condition.clone()],
        }
    }

    /// Converts the operator into a physical expression with the given children.
    pub fn into_expr(
        &self,
        children_relations: &[RelationalGroupId],
        children_scalars: &[ScalarGroupId],
    ) -> PhysicalExpression {
        let rel_size = children_relations.len();
        let scalar_size = children_scalars.len();

        match self {
            PhysicalOperator::TableScan(scan) => {
                assert_eq!(rel_size, 0, "TableScan: wrong number of relations");
                assert_eq!(scalar_size, 1, "TableScan: wrong number of scalars");

                PhysicalExpression::TableScan(TableScan {
                    table_name: scan.table_name.clone(),
                    predicate: children_scalars[0],
                })
            }
            PhysicalOperator::Filter(_) => {
                assert_eq!(rel_size, 1, "Filter: wrong number of relations");
                assert_eq!(scalar_size, 1, "Filter: wrong number of scalars");

                PhysicalExpression::Filter(PhysicalFilter {
                    child: children_relations[0],
                    predicate: children_scalars[0],
                })
            }
            PhysicalOperator::HashJoin(join) => {
                assert_eq!(rel_size, 2, "HashJoin: wrong number of relations");
                assert_eq!(scalar_size, 1, "HashJoin: wrong number of scalars");

                PhysicalExpression::HashJoin(HashJoin {
                    join_type: join.join_type.clone(),
                    probe_side: children_relations[0],
                    build_side: children_relations[1],
                    condition: children_scalars[0],
                })
            }
            PhysicalOperator::NestedLoopJoin(join) => {
                assert_eq!(rel_size, 2, "NestedLoopJoin: wrong number of relations");
                assert_eq!(scalar_size, 1, "NestedLoopJoin: wrong number of scalars");

                PhysicalExpression::NestedLoopJoin(NestedLoopJoin {
                    join_type: join.join_type.clone(),
                    outer: children_relations[0],
                    inner: children_relations[1],
                    condition: children_scalars[0],
                })
            }
            PhysicalOperator::SortMergeJoin(join) => {
                assert_eq!(rel_size, 2, "SortMergeJoin: wrong number of relations");
                assert_eq!(scalar_size, 1, "SortMergeJoin: wrong number of scalars");

                PhysicalExpression::SortMergeJoin(MergeJoin {
                    join_type: join.join_type.clone(),
                    left_sorted: children_relations[0],
                    right_sorted: children_relations[1],
                    condition: children_scalars[0],
                })
            }
        }
    }
}
