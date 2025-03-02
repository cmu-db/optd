use std::sync::Arc;

use async_recursion::async_recursion;

use crate::ir::{
    cost::Cost,
    expressions::{PhysicalExpression, StoredLogicalExpression, StoredPhysicalExpression},
    goal::GoalId,
    groups::{RelationalGroupId, ScalarGroupId},
    plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan, ScalarPlan},
};

use super::memo::Memoize;

/// Gets the cost of a physical plan by calling the cost model.
/// It also stores the cost in the memo table.
async fn get_physical_expression_cost(
    physical_expression: &PhysicalExpression,
    children_costs: &[Cost],
    children_scalars: &[ScalarGroupId],
) -> anyhow::Result<Cost> {
    todo!("Unimiplemented cost model")
}

/// Ingests a partial physical plan and returns the cost of top physical expression and the stored top physical expression.
#[async_recursion]
pub async fn ingest_partial_physical_plan(
    memo: &impl Memoize,
    parital_physical_plan: &PartialPhysicalPlan,
) -> anyhow::Result<(Cost, StoredPhysicalExpression)> {
    todo!()
}

#[async_recursion]
pub async fn ingest_partial_physical_plan_inner(
    memo: &impl Memoize,
    partial_physical_plan: &PartialPhysicalPlan,
) -> anyhow::Result<(Cost, Option<StoredPhysicalExpression>, GoalId)> {
    todo!()
}

#[async_recursion]
pub async fn ingest_partial_logical_plan(
    memo: &impl Memoize,
    partial_logical_plan: &PartialLogicalPlan,
) -> anyhow::Result<StoredLogicalExpression> {
    todo!()
}

#[async_recursion]
pub async fn ingest_partial_logical_plan_inner(
    memo: &impl Memoize,
    partial_logical_plan: &PartialLogicalPlan,
) -> anyhow::Result<(Option<StoredLogicalExpression>, RelationalGroupId)> {
    todo!()
}

#[async_recursion]
pub async fn ingest_full_logical_plan(
    memo: &impl Memoize,
    logical_plan: &LogicalPlan,
) -> anyhow::Result<RelationalGroupId> {
    todo!()
}

#[async_recursion]
pub async fn ingest_partial_scalar_plan(
    memo: &impl Memoize,
    partial_scalar_plan: &PartialScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    todo!()
}

#[async_recursion]
pub async fn ingest_full_scalar_plan(
    memo: &impl Memoize,
    scalar_plan: &ScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    todo!()
}
