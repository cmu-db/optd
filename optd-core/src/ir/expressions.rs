use std::sync::Arc;

use super::{
    groups::{RelationalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, PhysicalOperator, ScalarOperator},
    plans::{PartialLogicalPlan, PartialScalarPlan},
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

impl Into<PartialLogicalPlan> for LogicalExpression {
    fn into(self) -> PartialLogicalPlan {
        PartialLogicalPlan::PartialMaterialized {
            node: LogicalOperator::<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>> {
                tag: self.tag,
                data: self.data,
                relational_children: self
                    .relational_children
                    .into_iter()
                    .map(|child| Child::from(child))
                    .collect(),
                scalar_children: self
                    .scalar_children
                    .into_iter()
                    .map(|child| Child::from(child))
                    .collect(),
            },
        }
    }
}
