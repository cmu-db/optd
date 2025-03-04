use std::sync::Arc;

use super::memo::Memoize;
use crate::ir::{
    cost::Cost,
    expressions::{
        LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
    },
    goal::PhysicalGoalId,
    groups::{LogicalGroupId, ScalarGroupId},
    operators::{Child, LogicalOperator},
    plans::{LogicalPlan, PartialLogicalPlan, PartialPhysicalPlan, PartialScalarPlan, ScalarPlan},
};
use anyhow::Result;
use async_recursion::async_recursion;
use Child::*;

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
) -> anyhow::Result<(Cost, (PhysicalExpression, PhysicalExpressionId))> {
    let (cost, physical_expr, _) =
        ingest_partial_physical_plan_inner(memo, parital_physical_plan).await?;
    Ok((cost, physical_expr.unwrap()))
}

/*************  ✨ Codeium Command ⭐  *************/
/// Ingests a partial physical plan and returns the cost of the top physical expression, the stored top physical expression and the physical goal id.
///
/// This function is async because it calls itself when it encounters a PartialPhysicalPlan::PartialMaterialized.
/// This is because the PartialPhysicalPlan::PartialMaterialized nodes can have children that are PartialPhysicalPlan::PartialMaterialized themselves.
/// This function will recursively call itself until it reaches a PartialPhysicalPlan::UnMaterialized node.
///
/// The PartialPhysicalPlan::UnMaterialized node is the base case of the recursion.
/// The PartialPhysicalPlan::UnMaterialized node contains the physical goal id of the goal that it represents.
/// The function will return the cost of the top physical expression, the stored top physical expression and the physical goal id.
/// The stored top physical expression is the expression that was stored in the memo table and the cost is the cost of that expression.
/// The physical goal id is the id of the goal that the PartialPhysicalPlan::UnMaterialized node represents.
/******  05e8b280-0737-4c22-9458-8f84a0663a26  *******/
#[async_recursion]
pub async fn ingest_partial_physical_plan_inner(
    memo: &impl Memoize,
    partial_physical_plan: &PartialPhysicalPlan,
) -> Result<(
    Cost,
    Option<(PhysicalExpression, PhysicalExpressionId)>,
    PhysicalGoalId,
)> {
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

async fn ingest_partial_logical_plan2(
    memo: &impl Memoize,
    plan: &PartialLogicalPlan,
) -> Result<LogicalGroupId> {
    todo!()
}

#[async_recursion]
pub async fn ingest_partial_logical_plan(
    memo: &impl Memoize,
    operator: &LogicalOperator<Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    group_id: Option<LogicalGroupId>,
) -> Result<(LogicalExpression, LogicalExpressionId, LogicalGroupId)> {
    let mut relational_children = Vec::new();
    for child in operator.relational_children.iter() {
        let child_group_id = ingest_partial_logical_plan_inner(memo, child).await?;
        relational_children.push(child_group_id);
    }

    let mut scalar_children = Vec::new();
    for child in operator.scalar_children.iter() {
        let child_scalar_id = ingest_partial_scalar_plan_inner(memo, child).await?;
        scalar_children.push(child_scalar_id);
    }

    let local_expr = LogicalOperator {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        relational_children,
        scalar_children,
    };

    let (new_group_id, expr_id) = memo.add_logical_expr(&local_expr).await?;

    Ok((local_expr, expr_id, new_group_id))
}

#[async_recursion]
pub async fn ingest_partial_logical_plan_inner(
    memo: &impl Memoize,
    child: &Child<Arc<PartialLogicalPlan>>,
) -> Result<Child<LogicalGroupId>> {
    match child {
        Singleton(child) => {
            let child = child.clone();
            // access the operator inside the arc child
            match &*child {
                PartialLogicalPlan::PartialMaterialized { node } => {
                    let (_, _, repr_group) = ingest_partial_logical_plan(memo, node, None).await?;
                    Ok(Child::Singleton(repr_group))
                }
                PartialLogicalPlan::UnMaterialized(goal_id) => Ok(Child::Singleton(*goal_id)),
            }
        }
        VarLength(children) => {
            let mut children_groups = Vec::new();
            for child in children.iter() {
                let child = child.clone();
                match &*child {
                    PartialLogicalPlan::PartialMaterialized { node } => {
                        let (_, _, repr_group) =
                            ingest_partial_logical_plan(memo, node, None).await?;
                        children_groups.push(repr_group);
                    }
                    PartialLogicalPlan::UnMaterialized(goal_id) => {
                        children_groups.push(*goal_id);
                    }
                }
            }
            Ok(Child::VarLength(children_groups))
        }
    }
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
pub async fn ingest_partial_scalar_plan_inner(
    memo: &impl Memoize,
    child: &Child<Arc<PartialScalarPlan>>,
) -> anyhow::Result<Child<ScalarGroupId>> {
    todo!()
}

#[async_recursion]
pub async fn ingest_full_scalar_plan(
    memo: &impl Memoize,
    scalar_plan: &ScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    todo!()
}
