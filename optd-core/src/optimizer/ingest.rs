use super::{
    jobs::{JobId, JobKind},
    memo::Memoize,
    tasks::TaskId,
    Optimizer,
};
use crate::{
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::{Goal, GoalId},
        group::GroupId,
        operators::{Child, Operator},
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
    },
    error::Error,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::{collections::HashSet, sync::Arc};
use Child::*;

/// Result type for logical plan ingestion exposed to clients
pub(super) enum LogicalIngest {
    /// Plan was successfully ingested
    Success(GroupId),

    /// Plan requires dependencies to be created
    /// Contains a set of job IDs for the launched dependency jobs
    NeedsDependencies(HashSet<JobId>),
}

/// Internal result type for logical expression processing
/// This enum is used internally during recursive plan traversal
enum InternalLogicalIngest {
    Found(GroupId),
    NeedsProperties(Vec<LogicalExpression>),
}

/// Result type for physical plan ingestion that includes the new expression if created
pub(super) struct PhysicalIngest {
    pub goal_id: GoalId,
    /// The new goal matched with the physical plan
    pub goal: Goal,
    /// The physical expression if newly created, None if it already existed
    pub new_expression: Option<PhysicalExpression>,
}

impl<M: Memoize> Optimizer<M> {
    /// Process a logical plan for ingestion into the memo.
    ///
    /// Attempts direct ingestion or schedules property derivation jobs when needed.
    ///
    /// # Parameters
    /// * `logical_plan` - The logical plan to ingest
    /// * `task_id` - The ID of the task that will track these jobs
    ///
    /// # Returns
    /// - `Success(group_id)`: Plan was successfully ingested
    ///    - `group_id`: The ID of the group that contains the plan
    /// - `NeedsDependencies(job_ids)`: Property derivation jobs were launched
    ///    - `job_ids`: Set of job IDs for the launched jobs
    pub(super) async fn try_ingest_logical(
        &mut self,
        logical_plan: &PartialLogicalPlan,
        task_id: TaskId,
    ) -> LogicalIngest {
        let ingest_result = self
            .ingest_logical_plan(logical_plan)
            .await
            .expect("Failed to ingest logical plan");

        match ingest_result {
            InternalLogicalIngest::Found(group_id) => LogicalIngest::Success(group_id),
            InternalLogicalIngest::NeedsProperties(expressions) => {
                let pending_dependencies = expressions
                    .into_iter()
                    .map(|expr| self.schedule_job(task_id, JobKind::DeriveLogicalProperties(expr)))
                    .collect();

                LogicalIngest::NeedsDependencies(pending_dependencies)
            }
        }
    }

    /// Process a physical plan for ingestion into the memo.
    ///
    /// Directly ingests the physical plan and returns the associated goal along with the
    /// physical expression if it was newly created.
    ///
    /// # Returns
    /// A PhysicalIngest containing the Goal associated with the physical plan
    /// and the PhysicalExpression if newly created (None if it already existed)
    pub(super) async fn try_ingest_physical(
        &mut self,
        physical_plan: &PartialPhysicalPlan,
    ) -> PhysicalIngest {
        self.ingest_physical_plan(physical_plan)
            .await
            .expect("Failed to ingest physical plan")
    }

    /// Ingests a partial logical plan into the memo table.
    #[async_recursion]
    async fn ingest_logical_plan(
        &self,
        partial_plan: &PartialLogicalPlan,
    ) -> Result<InternalLogicalIngest, Error> {
        match partial_plan {
            PartialLogicalPlan::Materialized(operator) => {
                self.ingest_logical_operator(operator).await
            }
            PartialLogicalPlan::UnMaterialized(group_id) => {
                Ok(InternalLogicalIngest::Found(*group_id))
            }
        }
    }

    /// Ingests a partial physical plan into the memo table.
    #[async_recursion]
    async fn ingest_physical_plan(
        &mut self,
        partial_plan: &PartialPhysicalPlan,
    ) -> Result<PhysicalIngest, Error> {
        match partial_plan {
            PartialPhysicalPlan::Materialized(operator) => {
                self.ingest_physical_operator(operator).await
            }
            PartialPhysicalPlan::UnMaterialized(goal) => {
                let goal_id = self.memo.get_goal_id(goal).await?;
                Ok(PhysicalIngest {
                    goal_id,
                    goal: goal.clone(),
                    new_expression: None,
                })
            }
        }
    }

    /// Processes a logical operator and attempts to find it in the memo table.
    ///
    /// Uses parallel processing for children since it has an immutable self reference.
    /// This allows for better performance when processing multiple children.
    async fn ingest_logical_operator(
        &self,
        operator: &Operator<Arc<PartialLogicalPlan>>,
    ) -> Result<InternalLogicalIngest, Error> {
        // Process children
        let children = try_join_all(
            operator
                .children
                .iter()
                .map(|child| self.process_logical_child(child)),
        )
        .await?;

        // Transform in a single pass, tracking expressions that need properties
        let mut need_properties = Vec::new();
        let children = children
            .into_iter()
            .map(|child_result| match child_result {
                Singleton(InternalLogicalIngest::Found(group_id)) => Singleton(group_id),
                Singleton(InternalLogicalIngest::NeedsProperties(expressions)) => {
                    need_properties.extend(expressions);
                    Singleton(GroupId(0)) // Placeholder
                }
                VarLength(results) => {
                    let group_ids = results
                        .into_iter()
                        .map(|result| match result {
                            InternalLogicalIngest::Found(group_id) => group_id,
                            InternalLogicalIngest::NeedsProperties(expressions) => {
                                need_properties.extend(expressions);
                                GroupId(0) // Placeholder
                            }
                        })
                        .collect();
                    VarLength(group_ids)
                }
            })
            .collect();

        // If any children need properties, return the expressions
        if !need_properties.is_empty() {
            return Ok(InternalLogicalIngest::NeedsProperties(need_properties));
        }

        // Create the logical expression with processed children
        let logical_expression = LogicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };

        // Try to add the expression to memo
        let group_maybe = self.memo.find_logical_expr(&logical_expression).await?;

        match group_maybe {
            Some(group_id) => {
                // Expression already exists in this group
                Ok(InternalLogicalIngest::Found(group_id))
            }
            None => {
                // Expression doesn't exist, needs property derivation
                Ok(InternalLogicalIngest::NeedsProperties(vec![
                    logical_expression,
                ]))
            }
        }
    }

    /// Processes a physical operator and integrates it into the memo table.
    ///
    /// Uses sequential processing for children since it has a mutable self reference.
    /// This is required to avoid multiple mutable borrows of self.
    async fn ingest_physical_operator(
        &mut self,
        operator: &Operator<Arc<PartialPhysicalPlan>>,
    ) -> Result<PhysicalIngest, Error> {
        // Process children
        let mut children = Vec::with_capacity(operator.children.len());
        for child in &operator.children {
            let processed_child = self.process_physical_child(child).await?;
            children.push(processed_child);
        }

        // Create the physical expression with processed children
        let physical_expression = PhysicalExpression {
            tag: operator.tag.clone(),
            data: operator.data.clone(),
            children,
        };

        // Try to find the expression in the memo
        if let Some((goal_id, goal)) = self.memo.find_physical_expr(&physical_expression).await? {
            return Ok(PhysicalIngest {
                goal_id,
                goal,
                new_expression: None,
            });
        }

        // Expression doesn't exist, create a new goal
        let (goal_id, goal) = self.memo.create_goal(&physical_expression).await?;

        Ok(PhysicalIngest {
            goal_id,
            goal,
            new_expression: Some(physical_expression),
        })
    }

    /// Helper function to process a Child structure containing a logical plan.
    ///
    /// This uses parallel processing with try_join_all since it operates on an immutable &self
    /// reference, allowing multiple children to be processed concurrently for better performance.
    async fn process_logical_child(
        &self,
        child: &Child<Arc<PartialLogicalPlan>>,
    ) -> Result<Child<InternalLogicalIngest>, Error> {
        match child {
            Singleton(plan) => {
                let result = self.ingest_logical_plan(plan).await?;
                Ok(Singleton(result))
            }
            VarLength(plans) => {
                // Parallel processing is safe with immutable self reference
                let results =
                    try_join_all(plans.iter().map(|plan| self.ingest_logical_plan(plan))).await?;
                Ok(VarLength(results))
            }
        }
    }

    /// Helper function to process a Child structure containing a physical plan.
    ///
    /// This uses sequential processing since it operates on a mutable &mut self reference,
    /// which cannot be shared across multiple concurrent operations.
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
                // Sequential processing required for mutable self reference
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
