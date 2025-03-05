use super::memo::Memoize;
use crate::ir::{
    expressions::{LogicalExpression, LogicalExpressionId},
    group::GroupId,
    operators::{Child, Operator},
    plans::PartialLogicalPlan,
};
use anyhow::Result;
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::{future::Future, pin::Pin, sync::Arc};
use Child::*;

/// Processes a logical operator and integrates it into the memo table.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `operator` - The logical operator to ingest
///
/// # Returns
/// * The created logical expression, its ID in the memo table, and its group ID
pub(super) async fn ingest_logical_operator<M>(
    memo: &M,
    operator: &Operator<Arc<PartialLogicalPlan>>,
) -> Result<(LogicalExpression, LogicalExpressionId, GroupId)>
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

    // Create the logical expression with processed children
    let logical_expr = Operator {
        tag: operator.tag.clone(),
        data: operator.data.clone(),
        children,
    };

    // Add to memo table and get IDs
    let (new_group_id, expr_id) = memo.add_logical_expr(&logical_expr).await?;

    Ok((logical_expr, expr_id, new_group_id))
}

/// Generic function to process a Child structure containing any plan type.
///
/// # Arguments
/// * `memo` - The memoization table
/// * `child` - The Child structure to process
/// * `ingest_fn` - Function to ingest each plan and get group ID
///
/// # Returns
/// * Processed Child structure with appropriate GroupId
async fn process_child<M, P, G, F>(
    memo: &M,
    child: &Child<Arc<P>>,
    ingest_fn: F,
) -> Result<Child<G>>
where
    M: Memoize,
    F: for<'a> Fn(&'a M, &'a P) -> Pin<Box<dyn Future<Output = Result<G>> + Send + 'a>>,
{
    match child {
        Singleton(plan) => {
            let group_id = ingest_fn(memo, plan).await?;
            Ok(Singleton(group_id))
        }
        VarLength(plans) => {
            let group_ids = try_join_all(plans.iter().map(|plan| ingest_fn(memo, plan))).await?;
            Ok(VarLength(group_ids))
        }
    }
}

/// Ingests a partial logical plan into the memo table.
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
) -> Result<GroupId>
where
    M: Memoize,
{
    match partial_plan {
        PartialLogicalPlan::Materialized(node) => {
            let (_, _, group_id) = ingest_logical_operator(memo, node).await?;
            Ok(group_id)
        }
        PartialLogicalPlan::UnMaterialized(group_id) => Ok(*group_id),
    }
}
