use std::sync::Arc;

use async_recursion::async_recursion;

use crate::ir::{
    cost::Cost,
    expressions::{PhysicalExpression, StoredLogicalExpression, StoredPhysicalExpression},
    goal::PhysicalGoalId,
    groups::{LogicalGroupId, ScalarGroupId},
    operators::Child,
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
    let (cost, physical_expr, _) =
        ingest_partial_physical_plan_inner(memo, parital_physical_plan).await?;
    Ok((cost, physical_expr.unwrap()))
}

#[async_recursion]
pub async fn ingest_partial_physical_plan_inner(
    memo: &impl Memoize,
    partial_physical_plan: &PartialPhysicalPlan,
) -> anyhow::Result<(Cost, Option<StoredPhysicalExpression>, PhysicalGoalId)> {
    /*
    match partial_physical_plan {
        PartialPhysicalPlan::PartialMaterialized {
            node,
            properties,
            group_id,
        } => {
            let mut children_relations = Vec::new();
            for child in node.relational_children.iter() {
                match child {
                    Child::Singleton(child) => {
                        children_relations
                            .push(ingest_partial_physical_plan_inner(memo, child).await?);
                    }
                    Child::VarLength(children) => {
                        for child in children.iter() {
                            children_relations
                                .push(ingest_partial_physical_plan_inner(memo, child).await?);
                        }
                    }
                }
            }

            let mut children_scalars = Vec::new();
            for child in operator.children_scalars().iter() {
                children_scalars.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            let children_goals: Vec<Arc<Goal>> = children_relations
                .iter()
                .map(|(_, _, goal)| goal.clone())
                .collect();
            let children_goals_ids: Vec<PhysicalGoalId> = children_goals
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
            // TODO: handle merging of goals.
            // what happens when the parialphysicalplan is unmaterialized?
            let (_, _, best_goal_cost) = memo
                .get_winner_physical_expr_in_goal(*goal_id)
                .await?
                .unwrap();
            // let goal = memo.get_goal(*goal_id).await?;
            // Ok((best_goal_cost, None, goal.clone()))
            todo!()
        }
    }
    */
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
) -> anyhow::Result<(Option<StoredLogicalExpression>, LogicalGroupId)> {
    todo!()
}

#[async_recursion]
pub async fn ingest_full_logical_plan(
    memo: &impl Memoize,
    logical_plan: &LogicalPlan,
) -> anyhow::Result<LogicalGroupId> {
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
