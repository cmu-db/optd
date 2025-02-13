//! Type definitions for scalar operators in `optd`.
//!
//! This module provides the core scalar operator types and implementations for the query optimizer.
//! Scalar operators represent expressions and computations that operate on individual values
//! rather than relations.

pub mod binary_op;
pub mod column_ref;
pub mod constants;
pub mod logic_op;
pub mod unary_op;

use crate::{
    cascades::{expressions::ScalarExpression, groups::ScalarGroupId},
    values::OptdValue,
};
use binary_op::BinaryOp;
use column_ref::ColumnRef;
use constants::Constant;
use logic_op::LogicOp;
use serde::Deserialize;
use unary_op::UnaryOp;

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
    /// Binary operator (e.g., +, -, *, /, ==, >, etc.)
    BinaryOp(BinaryOp<Value, Scalar>),
    /// Unary operator (e.g., NOT, -)
    UnaryOp(UnaryOp<Value, Scalar>),
    /// Logic operator (e.g., AND, OR)
    LogicOp(LogicOp<Value, Scalar>),
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
    /// Represents a binary operation (e.g., +, -, *, /, ==, >, etc.)
    Binary,
    /// Represents a unary operation (e.g., NOT, -)
    Unary,
    /// Represents a logic operation (e.g., AND, OR)
    Logic,
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
            ScalarOperator::BinaryOp(_) => ScalarOperatorKind::Binary,
            ScalarOperator::UnaryOp(_) => ScalarOperatorKind::Unary,
            ScalarOperator::LogicOp(_) => ScalarOperatorKind::Logic,
        }
    }

    /// Returns a vector of values associated with this operator.
    pub fn values(&self) -> Vec<OptdValue> {
        match self {
            ScalarOperator::Constant(constant) => vec![constant.value.clone()],
            ScalarOperator::ColumnRef(column_ref) => vec![column_ref.column_index.clone()],
            ScalarOperator::BinaryOp(binary_op) => {
                vec![binary_op.kind.clone()]
            }
            ScalarOperator::UnaryOp(unary_op) => vec![unary_op.kind.clone()],
            ScalarOperator::LogicOp(logic_op) => vec![logic_op.kind.clone()],
        }
    }

    /// Returns a vector of scalar expressions that are children of this operator.
    pub fn children_scalars(&self) -> Vec<Scalar> {
        match self {
            ScalarOperator::Constant(_) => vec![],
            ScalarOperator::ColumnRef(_) => vec![],
            ScalarOperator::BinaryOp(binary_op) => {
                vec![binary_op.left.clone(), binary_op.right.clone()]
            }
            ScalarOperator::UnaryOp(unary_op) => vec![unary_op.child.clone()],
            ScalarOperator::LogicOp(logic_op) => logic_op.children.clone(),
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
            ScalarOperator::BinaryOp(binary) => {
                assert_eq!(scalar_size, 2, "Binary: expected two children");
                ScalarExpression::BinaryOp(BinaryOp {
                    kind: binary.kind.clone(),
                    left: children_scalars[0].clone(),
                    right: children_scalars[1].clone(),
                })
            }
            ScalarOperator::UnaryOp(unary) => {
                assert_eq!(scalar_size, 1, "Unary: expected one child");
                ScalarExpression::UnaryOp(UnaryOp {
                    kind: unary.kind.clone(),
                    child: children_scalars[0].clone(),
                })
            }
            ScalarOperator::LogicOp(logic) => {
                assert!(scalar_size > 0, "Logic: expected at least one child");
                ScalarExpression::LogicOp(LogicOp {
                    kind: logic.kind.clone(),
                    children: children_scalars.to_vec(),
                })
            }
        }
    }
}
