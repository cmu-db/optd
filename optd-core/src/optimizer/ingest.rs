use super::{memo::Memoize, Optimizer};
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        operators::{Child, Operator},
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
    },
    error::Error,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::sync::Arc;
use Child::*;
use LogicalIngest::*;

/// Result type for logical plan probing.
pub(super) enum LogicalIngest {
    /// Plan was successfully found in the memo.
    Found(GroupId),
    /// Plan requires groups to be created for missing expressions.
    Missing(Vec<LogicalExpression>),
}

/// Result type for physical plan ingestion.
pub(super) struct PhysicalIngest {
    /// The goal matched with the ingested physical plan.
    pub goal: Goal,
    /// The physical expression if newly created, None if it already existed.
    pub new_expression: Option<PhysicalExpression>,
}

impl<M: Memoize> Optimizer<M> {
    /// Probes for a logical plan in the memo without modifying it.
    ///
    /// This is read-only by design. Logical plans need property derivation
    /// which occurs asynchronously in separate jobs after this function returns.
    /// This separation lets the optimizer continue processing while properties
    /// are computed in the background.
    ///
    /// # Returns
    /// - `Found(group_id)`: Plan exists in the memo
    /// - `Missing(expressions)`: Expressions needing property derivation
    pub(super) async fn probe_ingest_logical_plan(
        &self,
        logical_plan: &PartialLogicalPlan,
    ) -> LogicalIngest {
        match logical_plan {
            PartialLogicalPlan::Materialized(operator) => self
                .probe_ingest_logical_operator(operator)
                .await
                .expect("Failed to probe logical operator"),
            PartialLogicalPlan::UnMaterialized(group_id) => Found(*group_id),
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
            PartialPhysicalPlan::UnMaterialized(goal) => Ok(PhysicalIngest {
                goal: goal.clone(),
                new_expression: None,
            }),
        }
    }

    async fn probe_ingest_logical_operator(
        &self,
        operator: &Operator<Arc<PartialLogicalPlan>>,
    ) -> Result<LogicalIngest, Error> {
        // Recursively process the children ingestion.
        let children = try_join_all(
            operator
                .children
                .iter()
                .map(|child| self.probe_ingest_logical_child(child)),
        )
        .await?;

        // Collect *all* missing expressions from children in order to reduce
        // the number of times the optimizer needs to probe the ingestion.
        let mut missing_expressions = Vec::new();
        let children = children
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

        // Base case: check if the expression already exists in the memo.
        match self.memo.find_logical_expr(&logical_expression).await? {
            Some(group_id) => Ok(Found(group_id)),
            None => Ok(Missing(vec![logical_expression])),
        }
    }

    #[async_recursion]
    async fn probe_ingest_logical_child(
        &self,
        child: &Child<Arc<PartialLogicalPlan>>,
    ) -> Result<Child<LogicalIngest>, Error> {
        match child {
            Singleton(plan) => {
                let result = self.probe_ingest_logical_plan(plan).await;
                Ok(Singleton(result))
            }
            VarLength(plans) => {
                let futures: Vec<_> = plans
                    .iter()
                    .map(|plan| async move { Ok(self.probe_ingest_logical_plan(plan).await) })
                    .collect();

                let results = try_join_all(futures).await?;
                Ok(VarLength(results))
            }
        }
    }

    async fn ingest_physical_operator(
        &mut self,
        operator: &Operator<Arc<PartialPhysicalPlan>>,
    ) -> Result<PhysicalIngest, Error> {
        // Recursively process the children ingestion.
        let mut children = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let processed_child = self.process_physical_child(child).await?;
            children.push(processed_child);
        }

        let physical_expression = PhysicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };

        // Base case: try to find the expression in the memo or create it if missing.
        if let Some(goal) = self.memo.find_physical_expr(&physical_expression).await? {
            Ok(PhysicalIngest {
                goal,
                new_expression: None,
            })
        } else {
            Ok(PhysicalIngest {
                goal: self.memo.create_goal(&physical_expression).await?,
                new_expression: Some(physical_expression),
            })
        }
    }

    async fn process_physical_child(
        &mut self,
        child: &Child<Arc<PartialPhysicalPlan>>,
    ) -> Result<Child<Goal>, Error> {
        match child {
            Singleton(plan) => {
                let result = self.ingest_physical_plan(plan).await?;
                Ok(Singleton(result.goal))
            }
            VarLength(plans) => {
                let mut results = Vec::with_capacity(plans.len());
                for plan in plans {
                    let result = self.ingest_physical_plan(plan).await?;
                    results.push(result);
                }

                let goals = results.into_iter().map(|ingest| ingest.goal).collect();
                Ok(VarLength(goals))
            }
        }
    }
}
