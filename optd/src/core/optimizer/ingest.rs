use super::Optimizer;
use crate::core::{
    cir::{
        Child, GoalMemberId, GroupId, LogicalExpression, LogicalExpressionId, Operator,
        PartialLogicalPlan, PartialPhysicalPlan, PhysicalExpression,
    },
    error::Error,
    memo::Memoize,
};
use Child::*;
use LogicalIngest::*;
use async_recursion::async_recursion;
use std::sync::Arc;

/// Result type for logical plan probing.
pub(super) enum LogicalIngest {
    /// Plan was successfully found in the memo.
    Found(GroupId),
    /// Plan requires groups to be created for missing expressions.
    Missing(Vec<LogicalExpressionId>),
}

impl<M: Memoize> Optimizer<M> {
    /// Probes for a logical plan in the memo.
    ///
    /// Logical plans need property derivation which occurs asynchronously
    /// in separate jobs after this function returns. This separation lets
    /// the optimizer continue processing while properties are computed
    /// in the background.
    ///
    /// # Returns
    /// - `Found(group_id)`: Plan exists in the memo.
    /// - `Missing(expressions)`: Expressions needing property derivation.
    pub(super) async fn probe_ingest_logical_plan(
        &mut self,
        logical_plan: &PartialLogicalPlan,
    ) -> Result<LogicalIngest, Error> {
        match logical_plan {
            PartialLogicalPlan::Materialized(operator) => {
                self.probe_ingest_logical_operator(operator).await
            }
            PartialLogicalPlan::UnMaterialized(group_id) => Ok(Found(*group_id)),
        }
    }

    /// Probes for a physical plan in the memo.
    ///
    /// Recursively processes the physical plan to obtain the goal member identifiers
    /// throughout the tree. Unlike logical plans, this doesn't require
    /// asynchronous property derivation.
    ///
    /// # Parameters
    /// * `physical_plan` - The physical plan to probe.
    ///
    /// # Returns
    /// The goal member identifier corresponding to the physical plan,
    /// which can be either a physical expression ID or a goal ID.
    #[async_recursion]
    pub(super) async fn probe_ingest_physical_plan(
        &mut self,
        physical_plan: &PartialPhysicalPlan,
    ) -> Result<GoalMemberId, Error> {
        match physical_plan {
            PartialPhysicalPlan::Materialized(operator) => {
                self.probe_ingest_physical_operator(operator).await
            }
            PartialPhysicalPlan::UnMaterialized(goal) => {
                // Base case: is a goal.
                let goal_id = self.memo.get_goal_id(goal).await?;
                Ok(GoalMemberId::GoalId(goal_id))
            }
        }
    }

    async fn probe_ingest_logical_operator(
        &mut self,
        operator: &Operator<Arc<PartialLogicalPlan>>,
    ) -> Result<LogicalIngest, Error> {
        // Sequentially process the children ingestion.
        let mut children_results = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let result = self.probe_ingest_logical_child(child).await?;
            children_results.push(result);
        }

        // Collect *all* missing expressions from children in order to reduce
        // the number of times the optimizer needs to probe the ingestion.
        let mut missing_expressions = Vec::new();
        let children = children_results
            .into_iter()
            .map(|child_result| match child_result {
                Singleton(Found(group_id)) => Singleton(group_id),
                Singleton(Missing(expressions)) => {
                    missing_expressions.extend(expressions);
                    Singleton(GroupId(0)) // Placeholder.
                }
                VarLength(results) => {
                    let group_ids = results
                        .into_iter()
                        .map(|result| match result {
                            Found(group_id) => group_id,
                            Missing(expressions) => {
                                missing_expressions.extend(expressions);
                                GroupId(0) // Placeholder.
                            }
                        })
                        .collect();
                    VarLength(group_ids)
                }
            })
            .collect();

        // If any children have missing expressions, return those.
        if !missing_expressions.is_empty() {
            return Ok(Missing(missing_expressions));
        }

        let logical_expression = LogicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };
        let logical_expression_id = self.memo.get_logical_expr_id(&logical_expression).await?;

        // Base case: check if the expression already exists in the memo.
        match self
            .memo
            .find_logical_expr_group(logical_expression_id)
            .await?
        {
            Some(group_id) => Ok(Found(group_id)),
            None => Ok(Missing(vec![logical_expression_id])),
        }
    }

    #[async_recursion]
    async fn probe_ingest_logical_child(
        &mut self,
        child: &Child<Arc<PartialLogicalPlan>>,
    ) -> Result<Child<LogicalIngest>, Error> {
        match child {
            Singleton(plan) => {
                let result = self.probe_ingest_logical_plan(plan).await?;
                Ok(Singleton(result))
            }
            VarLength(plans) => {
                let mut results = Vec::with_capacity(plans.len());
                for plan in plans {
                    let result = self.probe_ingest_logical_plan(plan).await?;
                    results.push(result);
                }
                Ok(VarLength(results))
            }
        }
    }

    async fn probe_ingest_physical_operator(
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
        let expression = PhysicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };
        let expression_id = self.memo.get_physical_expr_id(&expression).await?;

        Ok(GoalMemberId::PhysicalExpressionId(expression_id))
    }

    async fn process_physical_child(
        &mut self,
        child: &Child<Arc<PartialPhysicalPlan>>,
    ) -> Result<Child<GoalMemberId>, Error> {
        match child {
            Singleton(plan) => {
                let member = self.probe_ingest_physical_plan(plan).await?;
                Ok(Singleton(member))
            }
            VarLength(plans) => {
                let mut members = Vec::with_capacity(plans.len());
                for plan in plans {
                    let member = self.probe_ingest_physical_plan(plan).await?;
                    members.push(member);
                }
                Ok(VarLength(members))
            }
        }
    }
}
