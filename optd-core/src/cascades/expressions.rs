//! Types for logical and physical expressions in the optimizer.

use crate::operators::relational::logical::LogicalOperator;
use crate::operators::relational::physical::PhysicalOperator;
use crate::operators::scalar::ScalarOperator;
use serde::Deserialize;

use super::goal::GoalId;
use super::groups::{RelationalGroupId, ScalarGroupId};
use super::ir::OperatorData;

/// A logical expression in the memo table.
pub type LogicalExpression = LogicalOperator<OperatorData, RelationalGroupId, ScalarGroupId>;

/// A physical expression in the memo table.
pub type PhysicalExpression = PhysicalOperator<OperatorData, GoalId, ScalarGroupId>;

/// A scalar expression in the memo table.
pub type ScalarExpression = ScalarOperator<OperatorData, ScalarGroupId>;

/// A stored logical expression in the memo table.
pub type StoredLogicalExpression = (LogicalExpression, LogicalExpressionId);

/// A stored physical expression in the memo table.
pub type StoredPhysicalExpression = (PhysicalExpression, PhysicalExpressionId);

/// A unique identifier for a logical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct LogicalExpressionId(pub i64);

/// A unique identifier for a physical expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct PhysicalExpressionId(pub i64);

/// A unique identifier for a scalar expression in the memo table.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type, Deserialize)]
#[sqlx(transparent)]
pub struct ScalarExpressionId(pub i64);
