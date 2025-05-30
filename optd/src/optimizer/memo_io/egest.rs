use crate::{
    cir::{Child, GoalMemberId, Operator, PartialPhysicalPlan, PhysicalExpressionId, PhysicalPlan},
    memo::Memo,
    optimizer::Optimizer,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::sync::Arc;

// TODO: Until costing is resolved, this file is not used (and just left here for reference)
impl<M: Memo> Optimizer<M> {
    /// Recursively transforms a physical expression ID in the memo into a complete physical plan.
    ///
    /// This function retrieves the physical expression from the memo and recursively
    /// transforms any child goal members into their corresponding best physical plans.
    ///
    /// # Parameters
    /// * `expression_id` - ID of the physical expression to transform into a complete plan.
    ///
    /// # Returns
    /// * `Ok(Some(PhysicalPlan))` if all children plans were successfully constructed from their IDs.
    /// * `Ok(None)` if any goal ID lacks a best expression ID.
    /// * `Err(Error)` if a memo operation fails.
    #[async_recursion]
    pub(crate) async fn egest_best_plan(
        &self,
        expression_id: PhysicalExpressionId,
    ) -> Result<Option<PhysicalPlan>, M::MemoError> {
        let expression = self.memo.materialize_physical_expr(expression_id).await?;

        let child_results = try_join_all(
            expression
                .children
                .iter()
                .map(|child| self.egest_child_plan(child)),
        )
        .await?;

        let child_plans = match child_results.into_iter().collect() {
            Some(plans) => plans,
            None => return Ok(None),
        };

        Ok(Some(PhysicalPlan(Operator {
            tag: expression.tag,
            data: expression.data,
            children: child_plans,
        })))
    }

    /// Converts a physical expression ID to a partial physical plan.
    ///
    /// This method materializes the expression and recursively processes its children,
    /// preserving goal references as unmaterialized plans.
    ///
    /// # Parameters
    /// * `expression_id` - ID of the physical expression to convert to a partial plan.
    ///
    /// # Returns
    /// * `PartialPhysicalPlan` - The materialized partial plan.
    /// * `Err(Error)` if a memo operation fails.
    pub(crate) async fn egest_partial_plan(
        &self,
        expression_id: PhysicalExpressionId,
    ) -> Result<PartialPhysicalPlan, M::MemoError> {
        let expression = self.memo.materialize_physical_expr(expression_id).await?;

        let children = try_join_all(
            expression
                .children
                .iter()
                .map(|child| self.egest_partial_child(child.clone())),
        )
        .await?;

        Ok(PartialPhysicalPlan::Materialized(Operator {
            tag: expression.tag,
            data: expression.data,
            children,
        }))
    }

    async fn egest_child_plan(
        &self,
        child: &Child<GoalMemberId>,
    ) -> Result<Option<Child<Arc<PhysicalPlan>>>, M::MemoError> {
        use Child::*;

        match child {
            Singleton(member) => {
                let plan = match self.process_goal_member(*member).await? {
                    Some(plan) => plan,
                    None => return Ok(None),
                };
                Ok(Some(Singleton(plan.into())))
            }
            VarLength(members) => {
                let futures = members.iter().map(|member| async move {
                    let plan = match self.process_goal_member(*member).await? {
                        Some(plan) => plan,
                        None => return Ok(None),
                    };
                    Ok(Some(plan.into()))
                });

                let result_plans = match try_join_all(futures).await?.into_iter().collect() {
                    Some(plans) => plans,
                    None => return Ok(None),
                };

                Ok(Some(VarLength(result_plans)))
            }
        }
    }

    async fn process_goal_member(
        &self,
        member: GoalMemberId,
    ) -> Result<Option<PhysicalPlan>, M::MemoError> {
        use GoalMemberId::*;

        match member {
            PhysicalExpressionId(expr_id) => self.egest_best_plan(expr_id).await,
            GoalId(goal_id) => {
                let (best_expr_id, _) =
                    match self.memo.get_best_optimized_physical_expr(goal_id).await? {
                        Some(expr) => expr,
                        None => return Ok(None),
                    };

                self.egest_best_plan(best_expr_id).await
            }
        }
    }

    async fn egest_partial_child(
        &self,
        child: Child<GoalMemberId>,
    ) -> Result<Child<Arc<PartialPhysicalPlan>>, M::MemoError> {
        use Child::*;

        match child {
            Singleton(member) => match member {
                GoalMemberId::GoalId(goal_id) => {
                    let goal = self.memo.materialize_goal(goal_id).await?;
                    Ok(Singleton(PartialPhysicalPlan::UnMaterialized(goal).into()))
                }
                GoalMemberId::PhysicalExpressionId(expr_id) => {
                    let expr = self.memo.materialize_physical_expr(expr_id).await?;

                    let children = try_join_all(
                        expr.children
                            .iter()
                            .map(|child| self.egest_partial_child(child.clone())),
                    )
                    .await?;

                    let op = Operator {
                        tag: expr.tag,
                        data: expr.data,
                        children,
                    };

                    Ok(Singleton(PartialPhysicalPlan::Materialized(op).into()))
                }
            },
            VarLength(members) => {
                let goals = try_join_all(members.into_iter().map(|member| async move {
                    match member {
                        GoalMemberId::GoalId(goal_id) => {
                            let goal = self.memo.materialize_goal(goal_id).await?;
                            Ok(PartialPhysicalPlan::UnMaterialized(goal).into())
                        }
                        GoalMemberId::PhysicalExpressionId(expr_id) => {
                            let expr = self.memo.materialize_physical_expr(expr_id).await?;

                            let children = try_join_all(
                                expr.children
                                    .iter()
                                    .map(|child| self.egest_partial_child(child.clone())),
                            )
                            .await?;

                            let op = Operator {
                                tag: expr.tag,
                                data: expr.data,
                                children,
                            };

                            Ok(PartialPhysicalPlan::Materialized(op).into())
                        }
                    }
                }))
                .await?;

                Ok(VarLength(goals))
            }
        }
    }
}
