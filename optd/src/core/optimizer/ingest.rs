use super::Optimizer;
use crate::{
    core::cir::{
        Child, GoalMemberId, GroupId, LogicalExpression, Operator, PartialLogicalPlan,
        PartialPhysicalPlan, PhysicalExpression,
    },
    core::error::Error,
    core::memo::Memoize,
};
use Child::*;
use std::sync::Arc;

impl<M: Memoize> Optimizer<M> {
    /// Ingests a logical plan into the memo.
    ///
    /// Returns the group id of the root logical expression.
    pub(super) async fn ingest_logical_plan(
        &mut self,
        logical_plan: &PartialLogicalPlan,
    ) -> Result<GroupId, Error> {
        Box::pin(async {
            match logical_plan {
                PartialLogicalPlan::Materialized(operator) => {
                    self.ingest_logical_operator(operator).await
                }
                PartialLogicalPlan::UnMaterialized(group_id) => Ok(*group_id),
            }
        })
        .await
    }

    /// Ingests a physical plan in the memo.
    ///
    /// Recursively processes the physical plan to obtain the goal member identifiers
    /// throughout the tree.
    ///
    /// # Parameters
    /// * `physical_plan` - The physical plan to probe.
    ///
    /// # Returns
    /// The goal member identifier corresponding to the physical plan,
    /// which can be either a physical expression ID or a goal ID.
    pub(super) async fn ingest_physical_plan(
        &mut self,
        physical_plan: &PartialPhysicalPlan,
    ) -> Result<GoalMemberId, Error> {
        Box::pin(async {
            match physical_plan {
                PartialPhysicalPlan::Materialized(operator) => {
                    self.ingest_physical_operator(operator).await
                }
                PartialPhysicalPlan::UnMaterialized(goal) => {
                    // Base case: is a goal.
                    let goal_id = self.memo.get_goal_id(goal).await?;
                    Ok(GoalMemberId::GoalId(goal_id))
                }
            }
        })
        .await
    }

    async fn ingest_logical_operator(
        &mut self,
        operator: &Operator<Arc<PartialLogicalPlan>>,
    ) -> Result<GroupId, Error> {
        let mut children = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let child = self.ingest_logical_child(child).await?;
            children.push(child);
        }
        let logical_expr = LogicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };

        let logical_expr_id = self.memo.get_logical_expr_id(&logical_expr).await?;

        match self.memo.find_logical_expr_group(logical_expr_id).await? {
            Some(group_id) => Ok(group_id),
            None => {
                // Create a new group for the logical expression.
                let new_group_id = self.memo.create_group(logical_expr_id).await?;
                Ok(new_group_id)
            }
        }
    }

    async fn ingest_logical_child(
        &mut self,
        child: &Child<Arc<PartialLogicalPlan>>,
    ) -> Result<Child<GroupId>, Error> {
        match child {
            Singleton(plan) => {
                let result = self.ingest_logical_plan(plan).await?;
                Ok(Singleton(result))
            }
            VarLength(plans) => {
                let mut children = Vec::with_capacity(plans.len());
                for plan in plans {
                    let child = self.ingest_logical_plan(plan).await?;
                    children.push(child);
                }
                Ok(VarLength(children))
            }
        }
    }

    async fn ingest_physical_operator(
        &mut self,
        operator: &Operator<Arc<PartialPhysicalPlan>>,
    ) -> Result<GoalMemberId, Error> {
        // Sequentially process the children ingestion.
        let mut children = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let processed_child = self.process_physical_child(child).await?;
            children.push(processed_child);
        }
        // Base case: is an expression.
        let physical_expr = PhysicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };
        let physical_expr_id = self.memo.get_physical_expr_id(&physical_expr).await?;

        Ok(GoalMemberId::PhysicalExpressionId(physical_expr_id))
    }

    async fn process_physical_child(
        &mut self,
        child: &Child<Arc<PartialPhysicalPlan>>,
    ) -> Result<Child<GoalMemberId>, Error> {
        match child {
            Singleton(plan) => {
                let member = self.ingest_physical_plan(plan).await?;
                Ok(Singleton(member))
            }
            VarLength(plans) => {
                let mut members = Vec::with_capacity(plans.len());
                for plan in plans {
                    let member = self.ingest_physical_plan(plan).await?;
                    members.push(member);
                }
                Ok(VarLength(members))
            }
        }
    }
}
