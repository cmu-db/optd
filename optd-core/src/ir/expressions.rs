use serde::Deserialize;

use super::{
    groups::{RelationalGroupId, ScalarGroupId},
    operators::{Child, OperatorData},
};

pub struct LogicalExpression {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<RelationalGroupId>>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: RelationalGroupId,
}

pub struct ScalarExpression {
    tag: String,
    data: Vec<OperatorData>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: ScalarGroupId,
}

pub struct PhysicalExpression {
    tag: String,
    data: Vec<OperatorData>,
    relational_children: Vec<Child<RelationalGroupId>>,
    scalar_children: Vec<Child<ScalarGroupId>>,
    group_id: RelationalGroupId,
}

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

/// A stored logical expression in the memo table.
pub type StoredLogicalExpression = (LogicalExpression, LogicalExpressionId);

/// A stored physical expression in the memo table.
pub type StoredPhysicalExpression = (PhysicalExpression, PhysicalExpressionId);
