use super::{
    groups::{RelationalGroupId, ScalarGroupId},
    operators::{LogicalOperator, PhysicalOperator, ScalarOperator},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalExpressionId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalExpressionId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarExpressionId(pub i64);

pub type StoredLogicalExpression = (LogicalExpression, LogicalExpressionId);
pub type StoredPhysicalExpression = (PhysicalExpression, PhysicalExpressionId);

pub type LogicalExpression = LogicalOperator<RelationalGroupId, ScalarGroupId>;
pub type ScalarExpression = ScalarOperator<ScalarGroupId>;
pub type PhysicalExpression = PhysicalOperator<RelationalGroupId, ScalarGroupId>;
