use std::sync::Arc;

use async_recursion::async_recursion;
use expressions::{LogicalExpression, ScalarExpression};
use groups::{RelationalGroupId, ScalarGroupId};
use memo::Memoize;

use crate::{
    operators::{
        relational::logical::{filter::Filter, join::Join, scan::Scan, LogicalOperator},
        scalar::{add::Add, equal::Equal, ScalarOperator},
    },
    plans::{logical::PartialLogicalPlan, scalar::PartialScalarPlan},
};

pub mod expressions;
pub mod groups;
pub mod memo;

#[async_recursion]
pub async fn ingest_partial_logical_plan(
    memo: &impl Memoize,
    partial_logical_plan: &PartialLogicalPlan,
) -> anyhow::Result<RelationalGroupId> {
    match partial_logical_plan {
        PartialLogicalPlan::PartialMaterialized { operator } => {
            let mut children_relations = Vec::new();
            for child in operator.children_relations().iter() {
                children_relations.push(ingest_partial_logical_plan(memo, child).await?);
            }

            let mut children_scalars = Vec::new();
            for child in operator.children_scalars().iter() {
                children_scalars.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            memo.add_logical_expr(&operator.into_expr(&children_relations, &children_scalars))
                .await
        }

        PartialLogicalPlan::UnMaterialized(group_id) => Ok(*group_id),
    }
}

#[async_recursion]
pub async fn ingest_partial_scalar_plan(
    memo: &impl Memoize,
    partial_scalar_plan: &PartialScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    match partial_scalar_plan {
        PartialScalarPlan::PartialMaterialized { operator } => {
            let mut children = Vec::new();
            for child in operator.children_scalars().iter() {
                children.push(ingest_partial_scalar_plan(memo, child).await?);
            }

            memo.add_scalar_expr(&operator.into_expr(&children)).await
        }

        PartialScalarPlan::UnMaterialized(group_id) => {
            return Ok(*group_id);
        }
    }
}

#[async_recursion]
async fn match_any_partial_logical_plan(
    memo: &impl Memoize,
    group: RelationalGroupId,
) -> anyhow::Result<Arc<PartialLogicalPlan>> {
    let logical_exprs = memo.get_all_logical_exprs_in_group(group).await?;
    let last_logical_expr = logical_exprs.last().unwrap().1.clone();

    match last_logical_expr.as_ref() {
        LogicalExpression::Scan(scan) => {
            let predicate = match_any_partial_scalar_plan(memo, scan.predicate).await?;
            Ok(Arc::new(PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::Scan(Scan {
                    predicate,
                    table_name: scan.table_name.clone(),
                }),
            }))
        }
        LogicalExpression::Filter(filter) => {
            let child = match_any_partial_logical_plan(memo, filter.child).await?;
            let predicate = match_any_partial_scalar_plan(memo, filter.predicate).await?;
            Ok(Arc::new(PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::Filter(Filter { child, predicate }),
            }))
        }
        LogicalExpression::Join(join) => {
            let left = match_any_partial_logical_plan(memo, join.left).await?;
            let right = match_any_partial_logical_plan(memo, join.right).await?;
            let condition = match_any_partial_scalar_plan(memo, join.condition).await?;
            Ok(Arc::new(PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::Join(Join {
                    left,
                    right,
                    condition,
                    join_type: join.join_type.clone(),
                }),
            }))
        }
    }
}

#[async_recursion]
async fn match_any_partial_scalar_plan(
    memo: &impl Memoize,
    group: ScalarGroupId,
) -> anyhow::Result<Arc<PartialScalarPlan>> {
    let scalar_exprs = memo.get_all_scalar_exprs_in_group(group).await?;
    let last_scalar_expr = scalar_exprs.last().unwrap().1.clone();
    match last_scalar_expr.as_ref() {
        ScalarExpression::Constant(constant) => {
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::Constant(constant.clone()),
            }))
        }
        ScalarExpression::ColumnRef(column_ref) => {
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::ColumnRef(column_ref.clone()),
            }))
        }
        ScalarExpression::Add(add) => {
            let left = match_any_partial_scalar_plan(memo, add.left).await?;
            let right = match_any_partial_scalar_plan(memo, add.right).await?;
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::Add(Add { left, right }),
            }))
        }
        ScalarExpression::Equal(equal) => {
            let left = match_any_partial_scalar_plan(memo, equal.left).await?;
            let right = match_any_partial_scalar_plan(memo, equal.right).await?;
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::Equal(Equal { left, right }),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::memo::SqliteMemo, test_utils::*};
    use anyhow::Ok;

    #[tokio::test]
    async fn test_ingest_partial_logical_plan() -> anyhow::Result<()> {
        let memo = SqliteMemo::new("sqlite://memo.db").await?;
        // select * from t1, t2 where t1.id = t2.id and t2.name = 'Memo' and t2.v1 = 1 + 1
        let partial_logical_plan = filter(
            join(
                "inner",
                scan("t1", boolean(true)),
                scan("t2", equal(column_ref(1), add(int64(1), int64(1)))),
                equal(column_ref(1), column_ref(2)),
            ),
            equal(column_ref(2), string("Memo")),
        );

        let group_id = ingest_partial_logical_plan(&memo, &partial_logical_plan).await?;
        let group_id_2 = ingest_partial_logical_plan(&memo, &partial_logical_plan).await?;
        assert_eq!(group_id, group_id_2);

        // The plan should be the same, there is only one expression per group.
        let result: Arc<PartialLogicalPlan> =
            match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, partial_logical_plan);
        Ok(())
    }
}
