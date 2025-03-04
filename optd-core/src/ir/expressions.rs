use std::sync::Arc;

use super::{
    goal::PhysicalGoalId,
    groups::{LogicalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator, PhysicalOperator, ScalarOperator},
    plans::{PartialLogicalPlan, PartialScalarPlan},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalExpressionId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalExpressionId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalarExpressionId(pub i64);

pub type LogicalExpression = LogicalOperator<LogicalGroupId, ScalarGroupId>;
pub type ScalarExpression = ScalarOperator<ScalarGroupId>;
pub type PhysicalExpression = PhysicalOperator<PhysicalGoalId, ScalarGroupId>;

impl From<LogicalExpression> for PartialLogicalPlan {
    fn from(expression: LogicalExpression) -> Self {
        PartialLogicalPlan::PartialMaterialized {
            node: LogicalOperator::<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>> {
                tag: expression.tag,
                data: expression.data,
                relational_children: expression
                    .relational_children
                    .into_iter()
                    .map(Child::from)
                    .collect(),
                scalar_children: expression
                    .scalar_children
                    .into_iter()
                    .map(Child::from)
                    .collect(),
            },
        }
    }
}
