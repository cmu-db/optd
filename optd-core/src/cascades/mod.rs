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
                self, filter::filter::PhysicalFilter, scan::table_scan::TableScan, PhysicalOperator,
            },
        },
        scalar::{add::Add, and::And, equal::Equal, ScalarOperator},
    },
    plans::{
        logical::PartialLogicalPlan, physical::PartialPhysicalPlan, scalar::PartialScalarPlan,
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

/// A mock optimization function for testing purposes.
///
/// This function takes a logical plan, and for each node in the logical plan, it will
/// recursively traverse the node and its children and replace the node with a physical
/// operator. The physical operator is chosen based on the type of the logical operator.
/// For example, if the logical operator is a scan, the physical operator will be a
/// TableScan, if the logical operator is a filter, the physical operator will be a
/// Filter, and so on.
///
/// The physical operators are chosen in a way that they mirror the structure of the
/// logical plan, but they are not actually optimized in any way. This is useful for
/// testing purposes, as it allows us to test the structure of the physical plan without
/// having to worry about the actual optimization process.
///
/// The function returns a PhysicalPlan, which is a struct that contains the root node of
/// the physical plan.
///
/// # Arguments
/// * `logical_plan` - The logical plan to optimize.
///
/// # Returns
/// * `PhysicalPlan` - The optimized physical plan.
pub fn mock_optimize_relation(
    partial_logical_plan: &PartialLogicalPlan,
) -> Arc<PartialPhysicalPlan> {
    let partial_physical_plan = match partial_logical_plan {
        PartialLogicalPlan::PartialMaterialized { operator } => {
            let operator = match operator {
                LogicalOperator::Scan(scan) => PhysicalOperator::TableScan(TableScan {
                    table_name: scan.table_name.clone(),
                    predicate: scan.predicate.clone(),
                }),
                // LogicalOperator::Filter(filter) => PhysicalOperator::Filter(PhysicalFilter {
                //     child: mock_optimize_relation(memo, &filter.child),
                //     predicate: filter.predicate.clone(),
                // }),
                // LogicalOperator::Project(project) => PhysicalOperator::Project(physical::project::PhysicalProject {
                //     child: mock_optimize_relation(memo, &project.child),
                //     fields: project.fields.clone(),
                // }),
                // LogicalOperator::Join(join) => PhysicalOperator::NestedLoopJoin(physical::join::nested_loop_join::NestedLoopJoin {
                //     join_type: join.join_type.clone(),
                //     outer: mock_optimize_relation(memo, &join.left),
                //     inner: mock_optimize_relation(memo, &join.right),
                //     condition: join.condition.clone(),
                // }),
                _ => unimplemented!(),
            };
            PartialPhysicalPlan::PartialMaterialized { operator }
        }
        PartialLogicalPlan::UnMaterialized(group_id) => {
            PartialPhysicalPlan::UnMaterialized(*group_id)
        }
    };
    Arc::new(partial_physical_plan)
}

async fn mock_optimize_scalar_group(
    _memo: &impl Memoize,
    _group: ScalarGroupId,
) -> anyhow::Result<()> {
    Ok(())
}

async fn mock_optimize_relation_group(
    memo: &impl Memoize,
    group_id: RelationalGroupId,
) -> anyhow::Result<()> {
    let logical_exprs = memo.get_all_logical_exprs_in_group(group_id).await?;
    let last_logical_expr = logical_exprs.last().unwrap().1.clone();

    mock_optimize_relation_expr(memo, group_id, &last_logical_expr).await?;

    Ok(())
}

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
        // LogicalExpression::Filter(filter) => {
        //     mock_optimize_relation_group(memo, filter.child).await?;
        //     mock_optimize_scalar_group(memo, filter.predicate).await?;
        // }
        // LogicalExpression::Join(join) => {
        //     mock_optimize_relation_group(memo, join.left).await?;
        //     mock_optimize_relation_group(memo, join.right).await?;
        //     mock_optimize_scalar_group(memo, join.condition).await?;
        // }
        // LogicalExpression::Project(project) => {
        //     mock_optimize_relation_group(memo, project.child).await?;
        //     for field in project.fields.iter() {
        //         mock_optimize_scalar_group(memo, *field).await?;
        //     }
        // }
        _ => unimplemented!(),
    }

    Ok(())
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
        let memo = SqliteMemo::new("sqlite://memo.db").await?;

        // select * from t1;
        let logical_plan = scan("t1", boolean(true));

        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;
        mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = match_any_partial_physical_plan(&memo, group_id).await?;

        assert_eq!(physical_plan, table_scan("t1", boolean(true)));

        Ok(())
    }
}
