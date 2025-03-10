use super::memo::Memoize;
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
use std::{future::Future, pin::Pin, sync::Arc};
use Child::*;

/// Represents the result of attempting to ingest a logical expression
pub enum LogicalIngestion {
    Found(GroupId, bool), // The boolean flag indicates whether an expr has been added
    NeedsProperties(Vec<LogicalExpression>),
}

/// Processes a logical operator and attempts to find it in the memo table.
///
/// This function ingests a logical operator into the memo structure, recursively
/// processing its children. If the expression is not found in the memo, it returns
/// the expression for property derivation.
///
/// # Arguments
/// * `memo` - The memoization table for storing plan expressions
/// * `operator` - The logical operator to ingest
///
/// # Returns
/// * Either the found group ID or the logical expression that needs properties
pub(super) async fn ingest_logical_operator<M>(
    memo: &M,
    operator: &Operator<Arc<PartialLogicalPlan>>,
) -> Result<LogicalIngestion, Error>
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
            Singleton(LogicalIngestion::Found(group_id, _)) => Singleton(group_id),
            Singleton(LogicalIngestion::NeedsProperties(exprs)) => {
                need_properties.extend(exprs);
                Singleton(GroupId(0)) // Placeholder
            }
            VarLength(results) => {
                let group_ids = results
                    .into_iter()
                    .map(|result| match result {
                        LogicalIngestion::Found(group_id, _) => group_id,
                        LogicalIngestion::NeedsProperties(exprs) => {
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
        return Ok(LogicalIngestion::NeedsProperties(need_properties));
    }

    // Create the logical expression with processed children
    let logical_expr = LogicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children,
    };

    // Try to add the expression to memo
    let maybe_added = memo.try_add_logical_expr(&logical_expr).await?;

    match maybe_added {
        Some((group_id, already_existed)) => Ok(LogicalIngestion::Found(group_id, already_existed)),
        None => Ok(LogicalIngestion::NeedsProperties(vec![logical_expr])),
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
pub(super) async fn ingest_physical_operator<M>(
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

/// Generic function to process a Child structure containing any plan type.
///
/// This function handles both singleton and variable-length children,
/// applying the appropriate ingestion function to each.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `child` - The Child structure to process
/// * `ingest_fn` - Function to ingest each plan and get group ID or goal
///
/// # Returns
/// * Processed Child structure with appropriate GroupId or Goal
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
/// * The ID of the logical group created or updated
#[async_recursion]
pub(super) async fn ingest_logical_plan<M>(
    memo: &M,
    partial_plan: &PartialLogicalPlan,
) -> Result<LogicalIngestion, Error>
where
    M: Memoize,
{
    match partial_plan {
        PartialLogicalPlan::Materialized(operator) => ingest_logical_operator(memo, operator).await,
        PartialLogicalPlan::UnMaterialized(group_id) => {
            // For unmaterialized plans, we know the group has to exist
            Ok(LogicalIngestion::Found(*group_id, false))
        }
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
/// * The goal associated with the physical plan
#[async_recursion]
pub(super) async fn ingest_physical_plan<M>(
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
