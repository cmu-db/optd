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

    use crate::operators::scalar::{constants::Constant, ScalarOperatorKind};
    use anyhow::Ok;
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::{
        engine::{
            actions::analyzers::{
                interpreter::scalar_analyze,
                scalar::{Match, ScalarAnalyzer},
            },
            patterns::{scalar::ScalarPattern, value::ValuePattern},
        },
        storage::memo::SqliteMemo,
        test_utils::*,
        values::{OptdExpr, OptdValue},
    };

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

    pub fn create_constant_folder() -> Rc<RefCell<ScalarAnalyzer>> {
        let root = Rc::new(RefCell::new(ScalarAnalyzer {
            name: "constant_fold".to_string(),
            matches: vec![], // Temporarily empty, will populate later
        }));

        // Now mutate `matches` to set up self-references
        {
            let mut root_mut = root.borrow_mut();
            root_mut.matches = vec![
                Match {
                    pattern: ScalarPattern::Operator {
                        op_type: ScalarOperatorKind::Add,
                        content: vec![],
                        scalar_children: vec![
                            Box::new(ScalarPattern::Bind(
                                "left".to_string(),
                                Box::new(ScalarPattern::Any),
                            )),
                            Box::new(ScalarPattern::Bind(
                                "right".to_string(),
                                Box::new(ScalarPattern::Any),
                            )),
                        ],
                    },
                    composition: vec![
                        ("left".to_string(), Rc::clone(&root)),
                        ("right".to_string(), Rc::clone(&root)),
                    ],
                    output: OptdExpr::Add {
                        left: Box::new(OptdExpr::Ref("left".to_string())),
                        right: Box::new(OptdExpr::Ref("right".to_string())),
                    },
                },
                Match {
                    pattern: ScalarPattern::Operator {
                        op_type: ScalarOperatorKind::Constant,
                        content: vec![Box::new(ValuePattern::Bind(
                            "val".to_string(),
                            Box::new(ValuePattern::Any),
                        ))],
                        scalar_children: vec![],
                    },
                    composition: vec![],
                    output: OptdExpr::Ref("val".to_string()),
                },
            ];
        }

        root
    }

    #[tokio::test]
    async fn test_constant_fold() -> anyhow::Result<()> {
        /*let memo = SqliteMemo::new("sqlite://memo.db").await?;
        let group_id = ingest_partial_scalar_plan(&memo, &partial_scalar_plan).await?;*/
        let partial_scalar_plan = PartialScalarPlan::PartialMaterialized {
            operator: ScalarOperator::Add(Add {
                left: add(add(int64(5), int64(3)), int64(3)),
                right: int64(1),
            }),
        };

        let folder = create_constant_folder();
        let folded = scalar_analyze(partial_scalar_plan, &folder.borrow())?.unwrap();
        assert_eq!(folded, OptdValue::Int64(12));

        /*let new_plan = PartialScalarPlan::PartialMaterialized {
                    operator: ScalarOperator::Constant(Constant {
                        value: folded.clone(),
                    }),
                };
        */
        /*let folded_group_id = ingest_partial_scalar_plan(&memo, &new_plan).await?;

        assert_ne!(group_id, folded_group_id);*/

        Ok(())
    }
}
