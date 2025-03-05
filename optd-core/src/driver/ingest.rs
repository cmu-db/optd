use super::memo::Memoize;
use crate::{
    engine::{expander::Expander, Engine},
    error::Error,
    ir::{
        expressions::{LogicalExpression, PhysicalExpression},
        goal::Goal,
        group::GroupId,
        operators::{Child, Operator},
        plans::{PartialLogicalPlan, PartialPhysicalPlan},
    },
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
/// * `bool` - Whether a new logical expression was added to the memoization table
pub(super) async fn ingest_logical_operator<M, E>(
    memo: &M,
    engine: &Engine<E>,
    operator: &Operator<Arc<PartialLogicalPlan>>,
) -> Result<(LogicalExpression, GroupId, bool), Error>
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

    let new_expr_added_by_children = children.iter().any(|(_, added)| *added);

    // Create the logical expression with processed children
    let logical_expr = LogicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children: children.into_iter().map(|(c, _)| c).collect(),
    };

    // clone the engine and logical_expr for the closure
    let engine = engine.clone();
    let logical_expr_for_props = logical_expr.clone();

    // First try to add the expression to an existing group
    let (group_id_result, new_expr_added) = memo
        .add_logical_expr(&logical_expr, move || async {
            // If no existing group found, create a new group with derived properties
            // Create a partial logical plan from the logical expression to derive properties
            let partial_plan = logical_expr_for_props.into();

            // Derive properties using the engine
            let props = engine.derive_properties(&partial_plan).await?;
            Ok(props)
        })
        .await?;
    Ok((
        logical_expr,
        group_id_result,
        new_expr_added_by_children || new_expr_added,
    ))
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
/// * `bool` - Whether a new physical expression was added to the memoization table
pub(super) async fn ingest_physical_operator<M>(
    memo: &M,
    operator: &Operator<Arc<PartialPhysicalPlan>>,
) -> Result<(PhysicalExpression, Goal, bool), Error>
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

    let new_expr_added_by_children = children.iter().any(|(_, added)| *added);

    // Create the physical expression with processed children
    let physical_expr = PhysicalExpression {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children: children.into_iter().map(|(c, _)| c).collect(),
    };

    // Add to memo table and get goal
    let (goal_result, new_expr_added) = memo.add_physical_expr(&physical_expr).await?;
    Ok((
        physical_expr,
        goal_result,
        new_expr_added_by_children || new_expr_added,
    ))
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
/// * `bool` - Whether a new expression was added to the memoization table
async fn process_child<M, C, P, G, F>(
    memo: &M,
    ctx: &C,
    child: &Child<Arc<P>>,
    ingest_fn: F,
) -> Result<(Child<G>, bool), Error>
where
    M: Memoize,
    F: for<'a> Fn(
        &'a M,
        &'a C,
        &'a P,
    ) -> Pin<Box<dyn Future<Output = Result<(G, bool), Error>> + Send + 'a>>,
{
    match child {
        Singleton(plan) => {
            let result = ingest_fn(memo, ctx, plan).await?;
            Ok((Singleton(result.0), result.1))
        }
        VarLength(plans) => {
            let results = try_join_all(plans.iter().map(|plan| ingest_fn(memo, ctx, plan))).await?;
            let new_expr_added = results.iter().any(|(_, added)| *added);
            Ok((
                VarLength(results.into_iter().map(|(c, b)| c).collect()),
                new_expr_added,
            ))
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
) -> Result<(GroupId, bool), Error>
where
    M: Memoize,
    E: Expander,
{
    match partial_plan {
        PartialLogicalPlan::Materialized(operator) => {
            let (_, group_id, new_expr_added) =
                ingest_logical_operator(memo, engine, operator).await?;
            Ok((group_id, new_expr_added))
        }
        PartialLogicalPlan::UnMaterialized(group_id) => Ok((*group_id, false)),
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
) -> Result<(Goal, bool), Error>
where
    M: Memoize,
{
    match partial_plan {
        PartialPhysicalPlan::Materialized(operator) => {
            let (_, goal, new_expr_added) = ingest_physical_operator(memo, operator).await?;
            Ok((goal, new_expr_added))
        }
        PartialPhysicalPlan::UnMaterialized(goal) => Ok((goal.clone(), false)),
    }
}
