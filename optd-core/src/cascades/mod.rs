pub mod expressions;
pub mod goal;
pub mod groups;
pub mod memo;
pub mod properties;
pub mod rules;
pub mod tasks;

use std::sync::Arc;

use async_recursion::async_recursion;
use expressions::{LogicalExpression, PhysicalExpression, ScalarExpression};
use goal::GoalId;
use groups::{RelationalGroupId, ScalarGroupId};
use memo::Memoize;
use properties::PhysicalProperties;

use crate::{
    cost_model::Cost,
    operators::{
        relational::{
            logical::{filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator},
            physical::{
                self, filter::filter::PhysicalFilter, join::nested_loop_join::NestedLoopJoin,
                project::PhysicalProject, scan::table_scan::TableScan,
            },
        },
        scalar::{binary_op::BinaryOp, logic_op::LogicOp, unary_op::UnaryOp, ScalarOperator},
    },
    plans::{
        logical::{LogicalPlan, PartialLogicalPlan},
        physical::{PartialPhysicalPlan, PhysicalPlan},
        scalar::{PartialScalarPlan, ScalarPlan},
    },
};

/// Gets the cost of a physical plan by calling the cost model.
/// It also stores the cost in the memo table.
/// It also updates
async fn get_physical_plan_cost(physical_plan: &PhysicalPlan) -> anyhow::Result<Cost> {
    todo!("Unimiplemented cost model")
}


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
) -> anyhow::Result<GoalId> {
    let logical_exprs = memo.get_all_logical_exprs_in_group(group_id).await?;
    let last_logical_expr = logical_exprs.last().unwrap().1.clone();

    let goal = memo
        .create_or_get_goal(group_id, PhysicalProperties::default())
        .await?;
    println!("Optimizing goal: {:?}", goal);
    mock_optimize_relation_expr(memo, goal.representative_goal_id, &last_logical_expr).await
}

#[async_recursion]
async fn mock_optimize_relation_expr(
    memo: &impl Memoize,
    goal_id: GoalId,
    logical_expr: &LogicalExpression,
) -> anyhow::Result<GoalId> {
    let fake_cost = Cost(1.);

    match logical_expr {
        LogicalExpression::Scan(scan) => {
            mock_optimize_scalar_group(memo, scan.predicate).await?;
            let physical_expr = PhysicalExpression::TableScan(TableScan {
                table_name: scan.table_name.clone(),
                predicate: scan.predicate,
            });
            memo.add_physical_expr_to_goal(&physical_expr, fake_cost, goal_id)
                .await
        }
        LogicalExpression::Filter(filter) => {
            mock_optimize_scalar_group(memo, filter.predicate).await?;
            let child = mock_optimize_relation_group(memo, filter.child).await?;
            let physical_expr = PhysicalExpression::Filter(PhysicalFilter {
                child,
                predicate: filter.predicate,
            });
            memo.add_physical_expr_to_goal(&physical_expr, fake_cost, goal_id)
                .await
        }
        LogicalExpression::Join(join) => {
            mock_optimize_scalar_group(memo, join.condition).await?;
            let outer = mock_optimize_relation_group(memo, join.left).await?;
            let inner = mock_optimize_relation_group(memo, join.right).await?;
            let physical_expr = PhysicalExpression::NestedLoopJoin(NestedLoopJoin {
                join_type: join.join_type.clone(),
                outer,
                inner,
                condition: join.condition,
            });
            memo.add_physical_expr_to_goal(&physical_expr, fake_cost, goal_id)
                .await
        }
        LogicalExpression::Project(project) => {
            let child = mock_optimize_relation_group(memo, project.child).await?;
            for field in project.fields.iter() {
                mock_optimize_scalar_group(memo, *field).await?;
            }
            let physical_expr = PhysicalExpression::Project(PhysicalProject {
                child,
                fields: project.fields.clone(),
            });
            memo.add_physical_expr_to_goal(&physical_expr, fake_cost, goal_id)
                .await
        }
    }
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
async fn get_best_partial_physical_plan(
    memo: &impl Memoize,
    goal_id: GoalId,
) -> anyhow::Result<Option<Arc<PartialPhysicalPlan>>> {
    // let winner_expr =
    // let physical_exprs = memo.get_all_physical_exprs_in_goal(goal_id).await?;
    let Some((_id, winner_physical_expr, _cost)) =
        memo.get_winner_physical_expr_in_goal(goal_id).await?
    else {
        return Ok(None);
    };
    match winner_physical_expr.as_ref() {
        PhysicalExpression::TableScan(table_scan) => {
            Ok(Some(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::TableScan(TableScan {
                    table_name: table_scan.table_name.clone(),
                    predicate: match_any_partial_scalar_plan(memo, table_scan.predicate).await?,
                }),
            })))
        }
        PhysicalExpression::Filter(filter) => {
            let Some(child) = get_best_partial_physical_plan(memo, filter.child).await? else {
                return Ok(None);
            };
            Ok(Some(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::Filter(PhysicalFilter {
                    child,
                    predicate: match_any_partial_scalar_plan(memo, filter.predicate).await?,
                }),
            })))
        }
        PhysicalExpression::NestedLoopJoin(nested_loop_join) => {
            let Some(outer) = get_best_partial_physical_plan(memo, nested_loop_join.outer).await?
            else {
                return Ok(None);
            };
            let Some(inner) = get_best_partial_physical_plan(memo, nested_loop_join.inner).await?
            else {
                return Ok(None);
            };
            Ok(Some(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::NestedLoopJoin(NestedLoopJoin {
                    join_type: nested_loop_join.join_type.clone(),
                    outer,
                    inner,
                    condition: match_any_partial_scalar_plan(memo, nested_loop_join.condition)
                        .await?,
                }),
            })))
        }
        PhysicalExpression::Project(project) => {
            let mut fields = Vec::with_capacity(project.fields.len());
            for field in project.fields.iter() {
                fields.push(match_any_partial_scalar_plan(memo, *field).await?);
            }
            let Some(child) = get_best_partial_physical_plan(memo, project.child).await? else {
                return Ok(None);
            };
            Ok(Some(Arc::new(PartialPhysicalPlan::PartialMaterialized {
                operator: physical::PhysicalOperator::Project(PhysicalProject { child, fields }),
            })))
        }
        _ => unimplemented!(),
    }
}

#[async_recursion]
pub async fn match_any_physical_plan(
    memo: &impl Memoize,
    goal_id: GoalId,
) -> anyhow::Result<Arc<PhysicalPlan>> {
    let physical_exprs = memo.get_all_physical_exprs_in_goal(goal_id).await?;
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
        ScalarExpression::BinaryOp(binary_op) => {
            let left = match_any_partial_scalar_plan(memo, binary_op.left).await?;
            let right = match_any_partial_scalar_plan(memo, binary_op.right).await?;
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::BinaryOp(BinaryOp::new(
                    binary_op.kind.clone(),
                    left,
                    right,
                )),
            }))
        }
        ScalarExpression::UnaryOp(unary_op) => {
            let child = match_any_partial_scalar_plan(memo, unary_op.child).await?;
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::UnaryOp(UnaryOp::new(unary_op.kind.clone(), child)),
            }))
        }
        ScalarExpression::LogicOp(logic) => {
            let mut children = Vec::with_capacity(logic.children.len());
            for child in logic.children.iter() {
                children.push(match_any_partial_scalar_plan(memo, *child).await?);
            }
            Ok(Arc::new(PartialScalarPlan::PartialMaterialized {
                operator: ScalarOperator::LogicOp(LogicOp::new(logic.kind.clone(), children)),
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
        ScalarExpression::BinaryOp(binary_op) => {
            let left = match_any_scalar_plan(memo, binary_op.left).await?;
            let right = match_any_scalar_plan(memo, binary_op.right).await?;
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::BinaryOp(BinaryOp::new(
                    binary_op.kind.clone(),
                    left,
                    right,
                )),
            }))
        }
        ScalarExpression::UnaryOp(unary_op) => {
            let child = match_any_scalar_plan(memo, unary_op.child).await?;
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::UnaryOp(UnaryOp::new(unary_op.kind.clone(), child)),
            }))
        }
        ScalarExpression::LogicOp(logic_op) => {
            let mut children = Vec::with_capacity(logic_op.children.len());
            for child in logic_op.children.iter() {
                children.push(match_any_scalar_plan(memo, *child).await?);
            }
            Ok(Arc::new(ScalarPlan {
                operator: ScalarOperator::LogicOp(LogicOp::new(logic_op.kind.clone(), children)),
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
            and(vec![boolean(true), equal(column_ref(2), string("Memo"))]),
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

        let goal_id = mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = get_best_partial_physical_plan(&memo, goal_id).await?;

        assert_eq!(physical_plan, Some(table_scan("t1", boolean(true))));

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1 where t1.#0 = 1 and true;
        let logical_plan = filter(
            scan("t1", or(vec![boolean(true), boolean(false)])),
            and(vec![equal(column_ref(0), int64(1)), boolean(true)]),
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        let goal_id = mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = get_best_partial_physical_plan(&memo, goal_id).await?;

        assert_eq!(
            physical_plan,
            Some(physical_filter(
                table_scan("t1", or(vec![boolean(true), boolean(false)])),
                and(vec![equal(column_ref(0), int64(1)), boolean(true)])
            ))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select * from t1 where t1.#0 = 1 and NOT false;
        let scan_t1 = scan("t1", not(boolean(false)));
        let logical_plan = join(
            "inner",
            scan_t1.clone(),
            scan_t1,
            equal(column_ref(0), column_ref(0)),
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        let goal_id = mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = get_best_partial_physical_plan(&memo, goal_id).await?;

        let table_scan_t1 = table_scan("t1", not(boolean(false)));
        assert_eq!(
            physical_plan,
            Some(nested_loop_join(
                "inner",
                table_scan_t1.clone(),
                table_scan_t1,
                equal(column_ref(0), column_ref(0)),
            ))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_project_e2e() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        // select t1.#0, (t1.#1 + 1) - (-3) from t1;
        let logical_plan = project(
            scan("t1", boolean(true)),
            vec![
                column_ref(0),
                minus(add(column_ref(1), int64(1)), neg(int64(3))),
            ],
        );
        let group_id = ingest_partial_logical_plan(&memo, &logical_plan).await?;

        let result = match_any_partial_logical_plan(&memo, group_id).await?;
        assert_eq!(result, logical_plan);

        let goal_id = mock_optimize_relation_group(&memo, group_id).await?;
        let physical_plan = get_best_partial_physical_plan(&memo, goal_id).await?;

        assert_eq!(
            physical_plan,
            Some(physical_project(
                table_scan("t1", boolean(true)),
                vec![
                    column_ref(0),
                    minus(add(column_ref(1), int64(1)), neg(int64(3))),
                ],
            ))
        );

        Ok(())
    }
}
