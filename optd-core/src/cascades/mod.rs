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
        PartialLogicalPlan::PartialMaterialized { operator } => match operator {
            LogicalOperator::Scan(scan) => {
                let predicate = ingest_partial_scalar_plan(memo, &scan.predicate).await?;
                let scan_expr = LogicalExpression::Scan(Scan {
                    predicate,
                    table_name: scan.table_name.clone(),
                });
                Ok(memo.add_logical_expr(&scan_expr).await?)
            }
            LogicalOperator::Filter(filter) => {
                let child = ingest_partial_logical_plan(memo, &filter.child).await?;
                let predicate = ingest_partial_scalar_plan(memo, &filter.predicate).await?;
                let filter_expr = LogicalExpression::Filter(Filter { child, predicate });
                Ok(memo.add_logical_expr(&filter_expr).await?)
            }
            LogicalOperator::Join(join) => {
                let left = ingest_partial_logical_plan(memo, &join.left).await?;
                let right = ingest_partial_logical_plan(memo, &join.right).await?;
                let condition = ingest_partial_scalar_plan(memo, &join.condition).await?;
                let join_expr = LogicalExpression::Join(Join {
                    left,
                    right,
                    condition,
                    join_type: join.join_type.clone(),
                });
                Ok(memo.add_logical_expr(&join_expr).await?)
            }
        },
        PartialLogicalPlan::UnMaterialized(group_id) => Ok(*group_id),
    }
}

#[async_recursion]
pub async fn ingest_partial_scalar_plan(
    memo: &impl Memoize,
    partial_scalar_plan: &PartialScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    match partial_scalar_plan {
        PartialScalarPlan::PartialMaterialized { operator } => match operator {
            ScalarOperator::Constant(constant) => {
                let constant_expr = ScalarExpression::Constant(constant.clone());
                Ok(memo.add_scalar_expr(&constant_expr).await?)
            }
            ScalarOperator::ColumnRef(column_ref) => {
                let column_ref_expr = ScalarExpression::ColumnRef(column_ref.clone());
                Ok(memo.add_scalar_expr(&column_ref_expr).await?)
            }
            ScalarOperator::Add(add) => {
                let left = ingest_partial_scalar_plan(memo, &add.left).await?;
                let right = ingest_partial_scalar_plan(memo, &add.right).await?;
                let add_expr = ScalarExpression::Add(Add { left, right });
                Ok(memo.add_scalar_expr(&add_expr).await?)
            }
            ScalarOperator::Equal(equal) => {
                let left = ingest_partial_scalar_plan(memo, &equal.left).await?;
                let right = ingest_partial_scalar_plan(memo, &equal.right).await?;
                let equal_expr = ScalarExpression::Equal(Equal { left, right });
                Ok(memo.add_scalar_expr(&equal_expr).await?)
            }
        },
        PartialScalarPlan::UnMaterialized(group_id) => {
            return Ok(*group_id);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{storage::memo::SqliteMemo, test_utils::*};

    #[tokio::test]
    async fn test_ingest_partial_logical_plan() -> anyhow::Result<()> {
        let memo = SqliteMemo::new("memo.db").await?;

        // select * from t1, t2 where t1.a = t2.b and t2.c = 'Memo';
        let partial_logical_plan = filter(
            join(
                "inner",
                scan("t1", boolean(true)),
                scan("t2", equal(column_ref(1), int64(12))),
                equal(column_ref(1), column_ref(2)),
            ),
            equal(column_ref(2), string("Memo")),
        );

        let group_id = ingest_partial_logical_plan(&memo, &partial_logical_plan).await?;
        let group_id_2 = ingest_partial_logical_plan(&memo, &partial_logical_plan).await?;
        assert_eq!(group_id, group_id_2);
        Ok(())
    }
}
