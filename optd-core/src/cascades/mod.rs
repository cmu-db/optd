pub mod expressions;
pub mod goal;
pub mod groups;
pub mod ir;
pub mod memo;
pub mod properties;
pub mod rules;
pub mod tasks;

use std::sync::Arc;

use async_recursion::async_recursion;
use expressions::{PhysicalExpression, StoredLogicalExpression, StoredPhysicalExpression};
use goal::{Goal, GoalId};
use groups::{RelationalGroupId, ScalarGroupId};
use memo::Memoize;

use crate::{
    cost_model::Cost,
    plans::{
        logical::{LogicalPlan, PartialLogicalPlan},
        physical::PartialPhysicalPlan,
        scalar::{PartialScalarPlan, ScalarPlan},
    },
};

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
    let (cost, physical_expr, _) =
        ingest_partial_physical_plan_inner(memo, parital_physical_plan).await?;
    Ok((cost, physical_expr.unwrap()))
}

#[async_recursion]
pub async fn ingest_partial_physical_plan_inner(
    memo: &impl Memoize,
    partial_physical_plan: &PartialPhysicalPlan,
) -> anyhow::Result<(Cost, Option<StoredPhysicalExpression>, Arc<Goal>)> {
    match partial_physical_plan {
        PartialPhysicalPlan::PartialMaterialized {
            operator,
            properties,
            group_id,
        } => {
            let mut children_relations = Vec::new();
            for child in operator.children_relations().iter() {
                children_relations.push(ingest_partial_physical_plan_inner(memo, child).await?);
            }

            let mut children_scalars = Vec::new();
            for child in operator.children_scalars().iter() {
                children_scalars.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            let children_goals: Vec<Arc<Goal>> = children_relations
                .iter()
                .map(|&(_, _, goal)| goal.clone())
                .collect();

            let children_goals_ids: Vec<GoalId> = children_goals
                .iter()
                .map(|goal| goal.representative_goal_id)
                .collect();
            let children_cost: Vec<Cost> = children_relations
                .iter()
                .map(|(cost, _, _)| *cost)
                .collect();
            let new_physical_expr = operator.into_expr(&children_goals_ids, &children_scalars);
            let cost =
                get_physical_expression_cost(&new_physical_expr, &children_cost, &children_scalars)
                    .await?;

            let goal = memo
                .create_or_get_goal(*group_id, properties.clone())
                .await?;
            // Now we store the physical expression in the goal.
            let (_, new_physical_expresion_id) = memo
                .add_physical_expr_to_goal(&new_physical_expr, cost, goal.representative_goal_id)
                .await?;

            Ok((
                cost,
                Some((new_physical_expr, new_physical_expresion_id)),
                goal.clone(),
            ))
        }
        PartialPhysicalPlan::UnMaterialized(goal_id) => {
            // If this unwrap fails, then the child goal has not been optimized yet. This should never happen because the rule engine should only return a goal id if the goal has been optimized.
            let (_, _, best_goal_cost) = memo
                .get_winner_physical_expr_in_goal(*goal_id)
                .await?
                .unwrap();
            let goal = memo.get_goal(*goal_id).await?;
            Ok((best_goal_cost, None, goal.clone()))
        }
    }
}

#[async_recursion]
pub async fn ingest_partial_logical_plan(
    memo: &impl Memoize,
    partial_logical_plan: &PartialLogicalPlan,
) -> anyhow::Result<StoredLogicalExpression> {
    let (logical_expr, group_id) =
        ingest_partial_logical_plan_inner(memo, partial_logical_plan).await?;
    Ok(logical_expr.unwrap())
}

#[async_recursion]
pub async fn ingest_partial_logical_plan_inner(
    memo: &impl Memoize,
    partial_logical_plan: &PartialLogicalPlan,
) -> anyhow::Result<(Option<StoredLogicalExpression>, RelationalGroupId)> {
    match partial_logical_plan {
        PartialLogicalPlan::PartialMaterialized { operator, group_id } => {
            let mut children_relations = Vec::new();
            for child in operator.children_relations().iter() {
                children_relations.push(ingest_partial_logical_plan_inner(memo, child).await?.1);
            }

            let mut children_scalars = Vec::new();
            for child in operator.children_scalars().iter() {
                children_scalars.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            let logical_expr = operator.into_expr(&children_relations, &children_scalars);

            let (group_id, logical_expr_id) = memo.add_logical_expr(&logical_expr).await?;
            Ok((Some((logical_expr, logical_expr_id)), group_id))
        }

        PartialLogicalPlan::UnMaterialized(group_id) => Ok((None, *group_id)),
    }
}

#[async_recursion]
pub async fn ingest_full_logical_plan(
    memo: &impl Memoize,
    logical_plan: &LogicalPlan,
) -> anyhow::Result<RelationalGroupId> {
    let mut children_relations = Vec::new();
    for child in logical_plan.operator.children_relations().iter() {
        children_relations.push(ingest_full_logical_plan(memo, child).await?);
    }

    let mut children_scalars = Vec::new();
    for child in logical_plan.operator.children_scalars().iter() {
        children_scalars.push(ingest_full_scalar_plan(memo, child).await?);
    }

    memo.add_logical_expr(
        &logical_plan
            .operator
            .into_expr(&children_relations, &children_scalars),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Error ingesting full logical plan: {:?}", e))
    .map(|(group_id, _)| group_id)
}

#[async_recursion]
pub async fn ingest_partial_scalar_plan(
    memo: &impl Memoize,
    partial_scalar_plan: &PartialScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    match partial_scalar_plan {
        PartialScalarPlan::PartialMaterialized { operator } => {
            let mut children = Vec::new();
            for child in operator.children_scalars().iter() {
                children.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            memo.add_scalar_expr(&operator.into_expr(&children)).await
        }

        PartialScalarPlan::UnMaterialized(group_id) => {
            return Ok(*group_id);
        }
    }
}

#[async_recursion]
pub async fn ingest_full_scalar_plan(
    memo: &impl Memoize,
    scalar_plan: &ScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    let mut children = Vec::new();
    for child in scalar_plan.operator.children_scalars().iter() {
        children.push(ingest_full_scalar_plan(memo, child).await?);
    }

    memo.add_scalar_expr(&scalar_plan.operator.into_expr(&children))
        .await
}
