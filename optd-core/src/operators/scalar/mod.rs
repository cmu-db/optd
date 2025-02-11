//! Type definitions for scalar operators in `optd`.
//!
//! This module provides the core scalar operator types and implementations for the query optimizer.
//! Scalar operators represent expressions and computations that operate on individual values
//! rather than relations.

pub mod add;
pub mod and;
pub mod column_ref;
pub mod constants;
pub mod equal;

use crate::{
    cascades::{expressions::ScalarExpression, groups::ScalarGroupId},
    values::OptdValue,
};
use add::Add;
use and::And;
use column_ref::ColumnRef;
use constants::Constant;
use equal::Equal;
use serde::Deserialize;

/// Each variant of `ScalarOperator` represents a specific kind of scalar operator.
///
/// This type is generic over two types:
/// - `Value`: The type of values stored in the operator (e.g., constants, column indices)
/// - `Scalar`: Specifies whether the children scalars are other scalar operators or a group id
///
/// This makes it possible to reuse the `ScalarOperator` type in different contexts:
/// - Pattern matching: Using scalar operators for matching rule patterns
/// - Partially materialized plans: Using scalar operators during optimization
/// - Fully materialized plans: Using scalar operators in physical execution
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub enum ScalarOperator<Value, Scalar> {
    /// Constant value operator
    Constant(Constant<Value>),
    /// Column reference operator
    ColumnRef(ColumnRef<Value>),
    /// Addition operator
    Add(Add<Scalar>),
    /// Equality comparison operator
    Equal(Equal<Scalar>),
    /// Conjunction (AND) operator
    And(And<Scalar>),
}

/// The kind of scalar operator.
///
/// This enum represents the different types of scalar operators available
/// in the system, used for classification and pattern matching.
#[derive(Debug, Clone, PartialEq, sqlx::Type)]
pub enum ScalarOperatorKind {
    /// Represents a constant value
    Constant,
    /// Represents a column reference
    ColumnRef,
    /// Represents an addition operation
    Add,
    /// Represents an equality comparison
    Equal,
    /// Represents a conjunction (AND) operation
    And,
}

impl<Scalar> ScalarOperator<OptdValue, Scalar>
where
    Scalar: Clone,
{
    /// Returns the kind of scalar operator.
    pub fn operator_kind(&self) -> ScalarOperatorKind {
        match self {
            ScalarOperator::Constant(_) => ScalarOperatorKind::Constant,
            ScalarOperator::ColumnRef(_) => ScalarOperatorKind::ColumnRef,
            ScalarOperator::Add(_) => ScalarOperatorKind::Add,
            ScalarOperator::Equal(_) => ScalarOperatorKind::Equal,
            ScalarOperator::And(_) => ScalarOperatorKind::And,
        }
    }

    /// Returns a vector of values associated with this operator.
    pub fn values(&self) -> Vec<OptdValue> {
        match self {
            ScalarOperator::Constant(constant) => vec![constant.value.clone()],
            ScalarOperator::ColumnRef(column_ref) => vec![column_ref.column_index.clone()],
            ScalarOperator::Add(_) => vec![],
            ScalarOperator::Equal(_) => vec![],
            ScalarOperator::And(_) => vec![],
        }
    }

    /// Returns a vector of scalar expressions that are children of this operator.
    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            ScalarOperator::Constant(_) => vec![],
            ScalarOperator::ColumnRef(_) => vec![],
            ScalarOperator::Add(add) => vec![add.left.clone(), add.right.clone()],
            ScalarOperator::Equal(equal) => vec![equal.left.clone(), equal.right.clone()],
            ScalarOperator::And(and) => vec![and.left.clone(), and.right.clone()],
        }
    }

    /// Converts the operator into a scalar expression with the given children.
    pub fn into_expr(&self, children_scalars: &[ScalarGroupId]) -> ScalarExpression {
        let scalar_size = children_scalars.len();

        match self {
            ScalarOperator::Constant(constant) => {
                assert_eq!(scalar_size, 0, "Constant: expected no children");
                ScalarExpression::Constant(Constant {
                    value: constant.value.clone(),
                })
            }
            ScalarOperator::ColumnRef(column_ref) => {
                assert_eq!(scalar_size, 0, "ColumnRef: expected no children");
                ScalarExpression::ColumnRef(ColumnRef {
                    column_index: column_ref.column_index.clone(),
                })
            }
            ScalarOperator::Add(_) => {
                assert_eq!(scalar_size, 2, "Add: expected 2 children");
                ScalarExpression::Add(Add {
                    left: children_scalars[0],
                    right: children_scalars[1],
                })
            }
            ScalarOperator::Equal(_) => {
                assert_eq!(scalar_size, 2, "Equal: expected 2 children");
                ScalarExpression::Equal(Equal {
                    left: children_scalars[0],
                    right: children_scalars[1],
                })
            }
            ScalarOperator::And(_) => {
                assert_eq!(scalar_size, 2, "And: expected 2 children");
                ScalarExpression::And(And {
                    left: children_scalars[0],
                    right: children_scalars[1],
                })
            }
        }
    }
}
