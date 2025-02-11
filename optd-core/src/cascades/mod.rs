pub mod expressions;
pub mod goal;
pub mod groups;
pub mod memo;
pub mod properties;

use std::sync::Arc;

use async_recursion::async_recursion;
use expressions::{LogicalExpression, PhysicalExpression, ScalarExpression};
use groups::{RelationalGroupId, ScalarGroupId};
use memo::Memoize;

use crate::{
    operators::{
        relational::{
            logical::{filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator},
            physical::{
                self, filter::filter::PhysicalFilter, join::nested_loop_join::NestedLoopJoin,
                project::PhysicalProject, scan::table_scan::TableScan,
            },
        },
        scalar::{add::Add, and::And, equal::Equal, ScalarOperator},
    },
    plans::{
        logical::{LogicalPlan, PartialLogicalPlan},
        physical::{PartialPhysicalPlan, PhysicalPlan},
        scalar::{PartialScalarPlan, ScalarPlan},
    },
};

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
pub async fn ingest_full_logical_plan(
    memo: &impl Memoize,
    logical_plan: &LogicalPlan,
) -> anyhow::Result<RelationalGroupId> {
    let mut children_relations = Vec::new();
    for child in logical_plan.operator.children_relations().iter() {
        children_relations.push(ingest_full_logical_plan(memo, child).await?);
    }

    let mut children_scalars = Vec::new();
    for child in logical_plan.operator.children_scalars().iter() {
        children_scalars.push(ingest_full_scalar_plan(memo, child).await?);
    }

    memo.add_logical_expr(
        &logical_plan
            .operator
            .into_expr(&children_relations, &children_scalars),
    )
    .await
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
pub async fn ingest_full_scalar_plan(
    memo: &impl Memoize,
    scalar_plan: &ScalarPlan,
) -> anyhow::Result<ScalarGroupId> {
    let mut children = Vec::new();
    for child in scalar_plan.operator.children_scalars().iter() {
        children.push(ingest_full_scalar_plan(memo, child).await?);
    }

    memo.add_scalar_expr(&scalar_plan.operator.into_expr(&children))
        .await
}

async fn mock_optimize_scalar_group(
    _memo: &impl Memoize,
    _group: ScalarGroupId,
) -> anyhow::Result<()> {
    Ok(())
}

#[async_recursion]
pub async fn mock_optimize_relation_group(
    memo: &impl Memoize,
    group_id: RelationalGroupId,
) -> anyhow::Result<()> {
    let logical_exprs = memo.get_all_logical_exprs_in_group(group_id).await?;
    let last_logical_expr = logical_exprs.last().unwrap().1.clone();

    mock_optimize_relation_expr(memo, group_id, &last_logical_expr).await?;

    Ok(())
}

#[async_recursion]
async fn mock_optimize_relation_expr(
    memo: &impl Memoize,
    group_id: RelationalGroupId,
    logical_expr: &LogicalExpression,
) -> anyhow::Result<()> {
    match logical_expr {
        LogicalExpression::Scan(scan) => {
            let physical_expr = PhysicalExpression::TableScan(TableScan {
                table_name: scan.table_name.clone(),
                predicate: scan.predicate.clone(),
            });
            memo.add_physical_expr_to_group(&physical_expr, group_id)
                .await?;
            mock_optimize_scalar_group(memo, scan.predicate).await?;
        }
        LogicalExpression::Filter(filter) => {
            let physical_expr = PhysicalExpression::Filter(PhysicalFilter {
                child: filter.child.clone(),
                predicate: filter.predicate.clone(),
            });
            memo.add_physical_expr_to_group(&physical_expr, group_id)
                .await?;
            mock_optimize_scalar_group(memo, filter.predicate).await?;
            mock_optimize_relation_group(memo, filter.child).await?;
        }
        LogicalExpression::Join(join) => {
            let physical_expr = PhysicalExpression::NestedLoopJoin(NestedLoopJoin {
                join_type: join.join_type.clone(),
                outer: join.left,
                inner: join.right,
                condition: join.condition,
            });
            memo.add_physical_expr_to_group(&physical_expr, group_id)
                .await?;
            mock_optimize_scalar_group(memo, join.condition).await?;
            mock_optimize_relation_group(memo, join.left).await?;
            mock_optimize_relation_group(memo, join.right).await?;
        }
        LogicalExpression::Project(project) => {
            let physical_expr = PhysicalExpression::Project(PhysicalProject {
                child: project.child,
                fields: project.fields.clone(),
            });
            memo.add_physical_expr_to_group(&physical_expr, group_id)
                .await?;
            mock_optimize_relation_group(memo, project.child).await?;
            for field in project.fields.iter() {
                mock_optimize_scalar_group(memo, *field).await?;
            }
        }
    }

    Ok(())
}

#[async_recursion]
pub async fn match_any_partial_logical_plan(
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
        LogicalExpression::Project(project) => {
            let child = match_any_partial_logical_plan(memo, project.child).await?;
            let mut fields = Vec::with_capacity(project.fields.len());

            for field in project.fields.iter() {
                fields.push(match_any_partial_scalar_plan(memo, *field).await?);
            }

            Ok(Arc::new(PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::Project(Project { child, fields }),
            }))
        }
    }
}

#[async_recursion]
async fn match_any_partial_physical_plan(
    memo: &impl Memoize,
    group: RelationalGroupId,
) -> anyhow::Result<Arc<PartialPhysicalPlan>> {
    let physical_exprs = memo.get_all_physical_exprs_in_group(group).await?;
    let last_physical_expr = physical_exprs.last().unwrap().1.clone();
    match last_physical_expr.as_ref() {
        PhysicalExpression::TableScan(table_scan) => {
            Ok(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::TableScan(TableScan {
                    table_name: table_scan.table_name.clone(),
                    predicate: match_any_partial_scalar_plan(memo, table_scan.predicate).await?,
                }),
            }))
        }
        PhysicalExpression::Filter(filter) => {
            Ok(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::Filter(PhysicalFilter {
                    child: match_any_partial_physical_plan(memo, filter.child).await?,
                    predicate: match_any_partial_scalar_plan(memo, filter.predicate).await?,
                }),
            }))
        }
        PhysicalExpression::NestedLoopJoin(nested_loop_join) => {
            Ok(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::NestedLoopJoin(NestedLoopJoin {
                    join_type: nested_loop_join.join_type.clone(),
                    outer: match_any_partial_physical_plan(memo, nested_loop_join.outer).await?,
                    inner: match_any_partial_physical_plan(memo, nested_loop_join.inner).await?,
                    condition: match_any_partial_scalar_plan(memo, nested_loop_join.condition)
                        .await?,
                }),
            }))
        }
        PhysicalExpression::Project(project) => {
            let mut fields = Vec::with_capacity(project.fields.len());
            for field in project.fields.iter() {
                fields.push(match_any_partial_scalar_plan(memo, *field).await?);
            }
            Ok(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::Project(PhysicalProject {
                    child: match_any_partial_physical_plan(memo, project.child).await?,
                    fields,
                }),
            }))
        }
        _ => unimplemented!(),
    }
}

#[async_recursion]
pub async fn match_any_physical_plan(
    memo: &impl Memoize,
    group: RelationalGroupId,
) -> anyhow::Result<Arc<PhysicalPlan>> {
    let physical_exprs = memo.get_all_physical_exprs_in_group(group).await?;
    let last_physical_expr = physical_exprs.last().unwrap().1.clone();
    match last_physical_expr.as_ref() {
        PhysicalExpression::TableScan(table_scan) => Ok(Arc::new(PhysicalPlan {
            operator: physical::PhysicalOperator::TableScan(TableScan {
                table_name: table_scan.table_name.clone(),
                predicate: match_any_scalar_plan(memo, table_scan.predicate).await?,
            }),
        })),
        PhysicalExpression::Filter(filter) => Ok(Arc::new(PhysicalPlan {
            operator: physical::PhysicalOperator::Filter(PhysicalFilter {
                child: match_any_physical_plan(memo, filter.child).await?,
                predicate: match_any_scalar_plan(memo, filter.predicate).await?,
            }),
        })),
        PhysicalExpression::NestedLoopJoin(nested_loop_join) => Ok(Arc::new(PhysicalPlan {
            operator: physical::PhysicalOperator::NestedLoopJoin(NestedLoopJoin {
                join_type: nested_loop_join.join_type.clone(),
                outer: match_any_physical_plan(memo, nested_loop_join.outer).await?,
                inner: match_any_physical_plan(memo, nested_loop_join.inner).await?,
                condition: match_any_scalar_plan(memo, nested_loop_join.condition).await?,
            }),
        })),
        PhysicalExpression::Project(project) => {
            let mut fields = Vec::with_capacity(project.fields.len());
            for field in project.fields.iter() {
                fields.push(match_any_scalar_plan(memo, *field).await?);
            }
            Ok(Arc::new(PhysicalPlan {
                operator: physical::PhysicalOperator::Project(PhysicalProject {
                    child: match_any_physical_plan(memo, project.child).await?,
                    fields,
                }),
            }))
        }
        _ => unimplemented!(),
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
        ScalarExpression::And(and) => {
            let left = match_any_partial_scalar_plan(memo, and.left).await?;
            let right = match_any_partial_scalar_plan(memo, and.right).await?;
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::And(And { left, right }),
            }))
        }
    }
}

#[async_recursion]
async fn match_any_scalar_plan(
    memo: &impl Memoize,
    group: ScalarGroupId,
) -> anyhow::Result<Arc<ScalarPlan>> {
    let scalar_exprs = memo.get_all_scalar_exprs_in_group(group).await?;
    let last_scalar_expr = scalar_exprs.last().unwrap().1.clone();
    match last_scalar_expr.as_ref() {
        ScalarExpression::Constant(constant) => Ok(Arc::new(ScalarPlan {
            operator: ScalarOperator::Constant(constant.clone()),
        })),
        ScalarExpression::ColumnRef(column_ref) => Ok(Arc::new(ScalarPlan {
            operator: ScalarOperator::ColumnRef(column_ref.clone()),
        })),
        ScalarExpression::Add(add) => {
            let left = match_any_scalar_plan(memo, add.left).await?;
            let right = match_any_scalar_plan(memo, add.right).await?;
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::Add(Add { left, right }),
            }))
        }
        ScalarExpression::Equal(equal) => {
            let left = match_any_scalar_plan(memo, equal.left).await?;
            let right = match_any_scalar_plan(memo, equal.right).await?;
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::Equal(Equal { left, right }),
            }))
        }
        ScalarExpression::And(and) => {
            let left = match_any_scalar_plan(memo, and.left).await?;
            let right = match_any_scalar_plan(memo, and.right).await?;
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::And(And { left, right }),
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
        let memo = SqliteMemo::new_in_memory().await?;
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

    #[tokio::test]
    async fn test_ingest_projection() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select 1, t1.1 from t1;
        let logical_plan = project(scan("t1", boolean(true)), vec![int64(1), column_ref(1)]);
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;
        let dup_group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;
        assert_eq!(group_id, dup_group_id);

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_and() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1 where t1.id = 1 and t1.name = 'Memo';
        let logical_plan = filter(
            scan("t1", boolean(true)),
            and(boolean(true), equal(column_ref(2), string("Memo"))),
        );

        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;
        let dup_group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;
        assert_eq!(group_id, dup_group_id);

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1;
        let logical_plan = scan("t1", boolean(true));
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = match_any_partial_physical_plan(&memo, group_id).await?;

        assert_eq!(physical_plan, table_scan("t1", boolean(true)));

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1 where t1.#0 = 1 and true;
        let logical_plan = filter(
            scan("t1", boolean(true)),
            and(equal(column_ref(0), int64(1)), boolean(true)),
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = match_any_partial_physical_plan(&memo, group_id).await?;

        assert_eq!(
            physical_plan,
            physical_filter(
                table_scan("t1", boolean(true)),
                and(equal(column_ref(0), int64(1)), boolean(true))
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1 where t1.#0 = 1 and true;
        let scan_t1 = scan("t1", boolean(true));
        let logical_plan = join(
            "inner",
            scan_t1.clone(),
            scan_t1,
            equal(column_ref(0), column_ref(0)),
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = match_any_partial_physical_plan(&memo, group_id).await?;

        let table_scan_t1 = table_scan("t1", boolean(true));
        assert_eq!(
            physical_plan,
            nested_loop_join(
                "inner",
                table_scan_t1.clone(),
                table_scan_t1,
                equal(column_ref(0), column_ref(0)),
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_project_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select t1.#0, t1.#1 + 1 from t1;
        let logical_plan = project(
            scan("t1", boolean(true)),
            vec![column_ref(0), add(column_ref(1), int64(1))],
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = match_any_partial_physical_plan(&memo, group_id).await?;

        assert_eq!(
            physical_plan,
            physical_project(
                table_scan("t1", boolean(true)),
                vec![column_ref(0), add(column_ref(1), int64(1))],
            )
        );

        Ok(())
    }
}
