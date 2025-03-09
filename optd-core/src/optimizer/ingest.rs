use super::{expander::Expander, memo::Memoize};
use crate::{
    capture,
    cir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        operators::{Child, Operator},
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
    },
    engine::Engine,
    error::Error,
};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::{future::Future, pin::Pin, sync::Arc};
use Child::*;

/// Processes a logical operator and integrates it into the memo table.
///
/// This function ingests a logical operator into the memo structure, recursively
/// processing its children and deriving logical properties for cost-based optimization.
///
/// # Arguments
/// * `memo` - The memoization table for storing plan expressions
/// * `engine` - The optimization engine for deriving properties
/// * `operator` - The logical operator to ingest
///
/// # Returns
/// * The created logical expression and its assigned group ID
pub(super) async fn ingest_logical_operator<M, E>(
    memo: &M,
    engine: &Engine<E>,
    operator: &Operator<Arc<PartialLogicalPlan>>,
) -> Result<(LogicalExpression, GroupId), Error>
where
    M: Memoize,
    E: Expander,
{
    // Process children
    let children =
        try_join_all(operator.children.iter().map(|child| {
            process_child(memo, engine, child, |m, e, p| ingest_logical_plan(m, e, p))
        }))
        .await?;

    // Create the logical expression with processed children
    let logical_expr = LogicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children,
    };

    // Add the expression to memo with a callback to derive properties
    let group_id = memo
        .add_logical_expr(&logical_expr, || {
            capture!([logical_expr, engine], async move {
                let partial_plan = logical_expr.into();
                engine.derive_properties(&partial_plan).await
            })
        })
        .await?;

    Ok((logical_expr, group_id))
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
            .map(|child| process_child(memo, &(), child, |m, _, p| ingest_physical_plan(m, p))),
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
/// * `ctx` - Additional context needed for ingestion (e.g., engine for logical plans)
/// * `child` - The Child structure to process
/// * `ingest_fn` - Function to ingest each plan and get group ID or goal
///
/// # Returns
/// * Processed Child structure with appropriate GroupId or Goal
async fn process_child<M, C, P, G, F>(
    memo: &M,
    ctx: &C,
    child: &Child<Arc<P>>,
    ingest_fn: F,
) -> Result<Child<G>, Error>
where
    M: Memoize,
    F: for<'a> Fn(
        &'a M,
        &'a C,
        &'a P,
    ) -> Pin<Box<dyn Future<Output = Result<G, Error>> + Send + 'a>>,
{
    match child {
        Singleton(plan) => {
            let result = ingest_fn(memo, ctx, plan).await?;
            Ok(Singleton(result))
        }
        VarLength(plans) => {
            let results = try_join_all(plans.iter().map(|plan| ingest_fn(memo, ctx, plan))).await?;
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
/// * `engine` - The optimization engine for deriving properties
/// * `partial_plan` - The partial logical plan to ingest
///
/// # Returns
/// * The ID of the logical group created or updated
#[async_recursion]
pub(super) async fn ingest_logical_plan<M, E>(
    memo: &M,
    engine: &Engine<E>,
    partial_plan: &PartialLogicalPlan,
) -> Result<GroupId, Error>
where
    M: Memoize,
    E: Expander,
{
    match partial_plan {
        PartialLogicalPlan::Materialized(operator) => {
            let (_, group_id) = ingest_logical_operator(memo, engine, operator).await?;
            Ok(group_id)
        }
        PartialLogicalPlan::UnMaterialized(group_id) => Ok(*group_id),
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
