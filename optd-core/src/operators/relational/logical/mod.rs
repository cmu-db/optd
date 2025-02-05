//! Type definitions of logical operators in `optd`.
//!
//! This module provides the core logical operator types and implementations for the query optimizer.
//! Logical operators represent the high-level operations in a query plan without specifying
//! physical execution details.

pub mod filter;
pub mod join;
pub mod scan;

use filter::Filter;
use join::Join;
use scan::Scan;
use serde::Deserialize;

use crate::{
    cascades::{
        expressions::LogicalExpression,
        groups::{RelationalGroupId, ScalarGroupId},
    },
    values::OptdValue,
};

/// Each variant of `LogicalOperator` represents a specific kind of logical operator.
///
/// This type is generic over three types:
/// - `Value`: The type of values stored in the operator (e.g., for table names, join types)
/// - `Relation`: Specifies whether the children relations are other logical operators or a group id
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id
///
/// This makes it possible to reuse the `LogicalOperator` type in different contexts:
/// - Pattern matching: Using logical operators for matching rule patterns
/// - Partially materialized plans: Using logical operators during optimization
/// - Fully materialized plans: Using logical operators in physical execution
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum LogicalOperator<Value, Relation, Scalar> {
    /// Table scan operator
    Scan(Scan<Value, Scalar>),
    /// Filter operator
    Filter(Filter<Relation, Scalar>),
    /// Join operator
    Join(Join<Value, Relation, Scalar>),
}

/// The kind of logical operator.
///
/// This enum represents the different types of logical operators available
/// in the system, used for classification and pattern matching.
#[derive(Debug, Clone, PartialEq, sqlx::Type)]
pub enum LogicalOperatorKind {
    /// Represents a table scan operation
    Scan,
    /// Represents a filter operation
    Filter,
    /// Represents a join operation
    Join,
}

impl<Relation, Scalar> LogicalOperator<OptdValue, Relation, Scalar>
where
    Relation: Clone,
    Scalar: Clone,
{
    /// Returns the kind of logical operator.
    ///
    /// This method identifies the variant of the logical operator
    /// without exposing its internal structure.
    pub fn operator_kind(&self) -> LogicalOperatorKind {
        match self {
            LogicalOperator::Scan(_) => LogicalOperatorKind::Scan,
            LogicalOperator::Filter(_) => LogicalOperatorKind::Filter,
            LogicalOperator::Join(_) => LogicalOperatorKind::Join,
        }
    }

    /// Returns a vector of values associated with this operator.
    ///
    /// Different operators store different kinds of values:
    /// - Scan: table name
    /// - Filter: no values
    /// - Join: join type
    pub fn values(&self) -> Vec<OptdValue> {
        match self {
            LogicalOperator::Scan(scan) => vec![scan.table_name.clone()],
            LogicalOperator::Filter(_) => vec![],
            LogicalOperator::Join(join) => vec![join.join_type.clone()],
        }
    }

    /// Returns a vector of child relations for this operator.
    ///
    /// The number of children depends on the operator type:
    /// - Scan: no children
    /// - Filter: one child
    /// - Join: two children (left and right)
    pub fn children_relations(&self) -> Vec<Relation> {
        match self {
            LogicalOperator::Scan(_) => vec![],
            LogicalOperator::Filter(filter) => vec![filter.child.clone()],
            LogicalOperator::Join(join) => vec![join.left.clone(), join.right.clone()],
        }
    }

    /// Returns a vector of scalar expressions associated with this operator.
    ///
    /// Each operator type has specific scalar expressions:
    /// - Scan: predicate
    /// - Filter: predicate
    /// - Join: condition
    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            LogicalOperator::Scan(scan) => vec![scan.predicate.clone()],
            LogicalOperator::Filter(filter) => vec![filter.predicate.clone()],
            LogicalOperator::Join(join) => vec![join.condition.clone()],
        }
    }

    /// Converts the operator into a logical expression with the given children.
    ///
    /// # Arguments
    /// * `children_relations` - Vector of relation group IDs for child relations
    /// * `children_scalars` - Vector of scalar group IDs for scalar expressions
    ///
    /// # Returns
    /// Returns a new `LogicalExpression` representing this operator
    ///
    /// # Panics
    /// Panics if the number of provided children doesn't match the operator's requirements
    pub fn into_expr(
        &self,
        children_relations: &[RelationalGroupId],
        children_scalars: &[ScalarGroupId],
    ) -> LogicalExpression {
        let rel_size = children_relations.len();
        let scalar_size = children_scalars.len();

        match self {
            LogicalOperator::Scan(scan) => {
                assert_eq!(rel_size, 0, "Scan: wrong number of relations");
                assert_eq!(scalar_size, 1, "Scan: wrong number of scalars");

                LogicalExpression::Scan(Scan {
                    table_name: scan.table_name.clone(),
                    predicate: children_scalars[0],
                })
            }
            LogicalOperator::Filter(_) => {
                assert_eq!(rel_size, 1, "Filter: wrong number of relations");
                assert_eq!(scalar_size, 1, "Filter: wrong number of scalars");

                LogicalExpression::Filter(Filter {
                    child: children_relations[0],
                    predicate: children_scalars[0],
                })
            }
            LogicalOperator::Join(join) => {
                assert_eq!(rel_size, 2, "Join: wrong number of relations");
                assert_eq!(scalar_size, 1, "Join: wrong number of scalars");

                LogicalExpression::Join(Join {
                    left: children_relations[0],
                    right: children_relations[1],
                    condition: children_scalars[0],
                    join_type: join.join_type.clone(),
                })
            }
        }
    }
}
