use super::{memo::Memoize, Optimizer};
use crate::{
    cir::{
        expressions::{
            LogicalExpression, LogicalExpressionId, PhysicalExpression, PhysicalExpressionId,
        },
        goal::GoalId,
        group::GroupId,
        operators::{Child, Operator},
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
    },
    error::Error,
};
use async_recursion::async_recursion;
use std::sync::Arc;
use Child::*;
use LogicalIngest::*;

/// Result type for logical plan probing.
pub(super) enum LogicalIngest {
    /// Plan was successfully found in the memo.
    Found(GroupId),
    /// Plan requires groups to be created for missing expressions.
    Missing(Vec<LogicalExpressionId>),
}

/// Result type for physical plan ingestion.
pub(super) struct PhysicalIngest {
    /// The goal id matched with the ingested physical plan.
    pub goal_id: GoalId,
    /// The physical expression if newly created, None if it already existed.
    pub new_expression: Option<PhysicalExpressionId>,
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
    /// - `Found(group_id)`: Plan exists in the memo
    /// - `Missing(expressions)`: Expressions needing property derivation
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

    /// Ingests a physical plan into the memo.
    ///
    /// Unlike logical plans, physical plans can be directly ingested without
    /// requiring asynchronous property derivation.
    ///
    /// # Returns
    /// Information about the ingested physical plan.
    #[async_recursion]
    pub(super) async fn ingest_physical_plan(
        &mut self,
        physical_plan: &PartialPhysicalPlan,
    ) -> Result<PhysicalIngest, Error> {
        match physical_plan {
            PartialPhysicalPlan::Materialized(operator) => {
                self.ingest_physical_operator(operator).await
            }
            PartialPhysicalPlan::UnMaterialized(goal) => {
                let goal_id = self.memo.get_goal_id(goal).await?;
                Ok(PhysicalIngest {
                    goal_id,
                    new_expression: None,
                })
            }
        }
    }

    async fn probe_ingest_logical_operator(
        &mut self,
        operator: &Operator<Arc<PartialLogicalPlan>>,
    ) -> Result<LogicalIngest, Error> {
        // Sequentially process the children ingestion
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
                    Singleton(GroupId(0)) // Placeholder
                }
                VarLength(results) => {
                    let group_ids = results
                        .into_iter()
                        .map(|result| match result {
                            Found(group_id) => group_id,
                            Missing(expressions) => {
                                missing_expressions.extend(expressions);
                                GroupId(0) // Placeholder
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
        match self.memo.find_logical_expr(logical_expression_id).await? {
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

    async fn ingest_physical_operator(
        &mut self,
        operator: &Operator<Arc<PartialPhysicalPlan>>,
    ) -> Result<PhysicalIngest, Error> {
        // Sequentially process the children ingestion.
        let mut children = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let processed_child = self.process_physical_child(child).await?;
            children.push(processed_child);
        }

        let expression = PhysicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };
        let expression_id = self.memo.get_physical_expr_id(&expression).await?;

        // Base case: try to find the expression in the memo or create it if missing.
        if let Some(goal_id) = self.memo.find_physical_expr(expression_id).await? {
            Ok(PhysicalIngest {
                goal_id,
                new_expression: None,
            })
        } else {
            Ok(PhysicalIngest {
                goal_id: self.memo.create_goal(expression_id).await?,
                new_expression: Some(expression_id),
            })
        }
    }

    async fn process_physical_child(
        &mut self,
        child: &Child<Arc<PartialPhysicalPlan>>,
    ) -> Result<Child<GoalId>, Error> {
        match child {
            Singleton(plan) => {
                let PhysicalIngest { goal_id, .. } = self.ingest_physical_plan(plan).await?;
                Ok(Singleton(goal_id))
            }
            VarLength(plans) => {
                let mut goals = Vec::with_capacity(plans.len());
                for plan in plans {
                    let result = self.ingest_physical_plan(plan).await?;
                    goals.push(result.goal_id);
                }
                Ok(VarLength(goals))
            }
        }
    }
}
