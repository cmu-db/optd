use super::{memo::Memoize, Optimizer, OptimizerMessage};
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
use futures::{future::try_join_all, SinkExt};
use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};
use Child::*;
use OptimizerMessage::CreateGroup;

/// Result type for logical plan ingestion exposed to clients
pub(super) enum LogicalIngest {
    /// Plan was successfully ingested
    Success(GroupId),

    /// Plan requires dependencies to be created
    /// Contains a set of job IDs for the launched dependency tasks
    NeedsDependencies(HashSet<i64>),
}

/// Internal result type for logical expression processing
/// This enum is used internally during recursive plan traversal
enum InternalLogicalIngest {
    Found(GroupId),
    NeedsProperties(Vec<LogicalExpression>),
}

impl<M: Memoize> Optimizer<M> {
    /// Process a logical plan for ingestion into the memo
    ///
    /// Attempts direct ingestion or spawns property derivation tasks when needed.
    ///
    /// # Returns
    /// - `Success(group_id)`: Plan was successfully ingested
    ///    - `group_id`: The ID of the group that contains the plan
    /// - `NeedsDependencies(job_ids)`: Property derivation tasks were launched
    ///    - `job_ids`: Set of job IDs for the launched tasks
    pub(super) async fn try_ingest_logical(
        &mut self,
        logical_plan: PartialLogicalPlan,
    ) -> LogicalIngest {
        let ingest_result = ingest_logical_plan(&self.memo, &logical_plan)
            .await
            .expect("Failed to ingest logical plan");

        match ingest_result {
            InternalLogicalIngest::Found(group_id) => LogicalIngest::Success(group_id),
            InternalLogicalIngest::NeedsProperties(expressions) => {
                let pending_dependencies = expressions
                    .into_iter()
                    .map(|expr| {
                        let job_id = self.next_dep_id;
                        self.next_dep_id += 1;

                        let mut message_tx = self.message_tx.clone();
                        let engine = self.engine.clone();

                        tokio::spawn(async move {
                            let properties = engine
                                .derive_properties(&expr.clone().into())
                                .await
                                .expect("Failed to derive properties");

                            message_tx
                                .send(CreateGroup(properties, expr, job_id))
                                .await
                                .expect("Failed to send CreateGroup message");
                        });

                        job_id
                    })
                    .collect();

                LogicalIngest::NeedsDependencies(pending_dependencies)
            }
        }
    }
}

/// Ingests a partial logical plan into the memo table.
///
/// This function handles both materialized and unmaterialized logical plans,
/// recursively processing operators in materialized plans.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `partial_plan` - The partial logical plan to ingest
///
/// # Returns
/// * For unmaterialized plans: Found(group_id) since the group already exists
/// * For materialized plans: Result from recursive operator ingestion
#[async_recursion]
async fn ingest_logical_plan<M>(
    memo: &M,
    partial_plan: &PartialLogicalPlan,
) -> Result<InternalLogicalIngest, Error>
where
    M: Memoize,
{
    match partial_plan {
        PartialLogicalPlan::Materialized(operator) => ingest_logical_operator(memo, operator).await,
        PartialLogicalPlan::UnMaterialized(group_id) => Ok(InternalLogicalIngest::Found(*group_id)),
    }
}

/// Ingests a partial physical plan into the memo table.
///
/// This function handles both materialized and unmaterialized physical plans,
/// recursively processing operators in materialized plans.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `partial_plan` - The partial physical plan to ingest
///
/// # Returns
/// * The Goal associated with the physical plan
#[async_recursion]
async fn ingest_physical_plan<M>(
    memo: &M,
    partial_plan: &PartialPhysicalPlan,
) -> Result<Goal, Error>
where
    M: Memoize,
{
    match partial_plan {
        PartialPhysicalPlan::Materialized(operator) => {
            let (_, goal) = ingest_physical_operator(memo, operator).await?;
            Ok(goal)
        }
        PartialPhysicalPlan::UnMaterialized(goal) => Ok(goal.clone()),
    }
}

/// Processes a logical operator and attempts to find it in the memo table.
///
/// This function ingests a logical operator into the memo structure, recursively
/// processing its children. If the expression is not found in the memo, it collects
/// expressions that need property derivation.
///
/// # Arguments
/// * `memo` - The memoization table for storing plan expressions
/// * `operator` - The logical operator to ingest
///
/// # Returns
/// * LogicalIngestion::Found when expression is in memo or added to memo
/// * LogicalIngestion::NeedsProperties when properties are needed before adding to memo
async fn ingest_logical_operator<M>(
    memo: &M,
    operator: &Operator<Arc<PartialLogicalPlan>>,
) -> Result<InternalLogicalIngest, Error>
where
    M: Memoize,
{
    // Process children
    let children = try_join_all(
        operator
            .children
            .iter()
            .map(|child| process_child(memo, child, |m, p| ingest_logical_plan(m, p))),
    )
    .await?;

    // Transform in a single pass, tracking expressions that need properties
    let mut need_properties = Vec::new();
    let children = children
        .into_iter()
        .map(|child_result| match child_result {
            Singleton(InternalLogicalIngest::Found(group_id)) => Singleton(group_id),
            Singleton(InternalLogicalIngest::NeedsProperties(exprs)) => {
                need_properties.extend(exprs);
                Singleton(GroupId(0)) // Placeholder
            }
            VarLength(results) => {
                let group_ids = results
                    .into_iter()
                    .map(|result| match result {
                        InternalLogicalIngest::Found(group_id) => group_id,
                        InternalLogicalIngest::NeedsProperties(exprs) => {
                            need_properties.extend(exprs);
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
    let logical_expr = LogicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children,
    };

    // Try to add the expression to memo
    let group_maybe = memo.find_logical_expr(&logical_expr).await?;

    match group_maybe {
        Some(group_id) => {
            // Expression already exists in this group
            Ok(InternalLogicalIngest::Found(group_id))
        }
        None => {
            // Expression doesn't exist, needs property derivation
            Ok(InternalLogicalIngest::NeedsProperties(vec![logical_expr]))
        }
    }
}

/// Processes a physical operator and integrates it into the memo table.
///
/// This function ingests a physical operator into the memo structure, recursively
/// processing its children. Unlike logical operators, physical operators don't
/// require property derivation as they already have concrete implementations.
///
/// # Arguments
/// * `memo` - The memoization table for storing plan expressions
/// * `operator` - The physical operator to ingest
///
/// # Returns
/// * The created physical expression and its assigned goal
async fn ingest_physical_operator<M>(
    memo: &M,
    operator: &Operator<Arc<PartialPhysicalPlan>>,
) -> Result<(PhysicalExpression, Goal), Error>
where
    M: Memoize,
{
    // Process children
    let children = try_join_all(
        operator
            .children
            .iter()
            .map(|child| process_child(memo, child, |m, p| ingest_physical_plan(m, p))),
    )
    .await?;

    // Create the physical expression with processed children
    let physical_expr = PhysicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children,
    };

    // Add the expression to memo
    let goal = memo.add_physical_expr(&physical_expr).await?;
    Ok((physical_expr, goal))
}

/// Generic function to process a Child structure containing any logical plan type.
///
/// This function handles both singleton and variable-length children,
/// applying the appropriate ingestion function to each.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `child` - The Child structure to process
/// * `ingest_fn` - Function to ingest each plan
///
/// # Returns
/// * Processed Child structure with LogicalIngestion results
async fn process_child<M, P, G, F>(
    memo: &M,
    child: &Child<Arc<P>>,
    ingest_fn: F,
) -> Result<Child<G>, Error>
where
    M: Memoize,
    F: for<'a> Fn(&'a M, &'a P) -> Pin<Box<dyn Future<Output = Result<G, Error>> + Send + 'a>>,
{
    match child {
        Singleton(plan) => {
            let result = ingest_fn(memo, plan).await?;
            Ok(Singleton(result))
        }
        VarLength(plans) => {
            let results = try_join_all(plans.iter().map(|plan| ingest_fn(memo, plan))).await?;
            Ok(VarLength(results))
        }
    }
}
