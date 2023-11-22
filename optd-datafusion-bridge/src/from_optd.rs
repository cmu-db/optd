use std::sync::Arc;

use anyhow::Result;
use async_recursion::async_recursion;
use datafusion::{
    datasource::source_as_provider,
    parquet::basic::SortOrder,
    physical_expr,
    physical_plan::{
        aggregates::AggregateMode, projection::ProjectionExec, sorts::sort::SortExec,
        ExecutionPlan, PhysicalExpr,
    },
};
use optd_datafusion_repr::plan_nodes::{
    ColumnRefExpr, ConstantExpr, Expr, FuncExpr, LogOpExpr, LogOpType, OptRelNode, OptRelNodeRef,
    OptRelNodeTyp, PhysicalAgg, PhysicalFilter, PhysicalNestedLoopJoin, PhysicalProjection,
    PhysicalScan, PhysicalSort, PlanNode, SortOrderExpr, SortOrderType,
};

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    #[async_recursion]
    async fn from_optd_table_scan(
        &mut self,
        node: PhysicalScan,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let source = self.tables.get(node.table().as_ref()).unwrap();
        let provider = source_as_provider(source)?;
        let plan = provider.scan(self.session_state, None, &[], None).await?;
        Ok(plan)
    }

    fn from_optd_sort_order_expr(
        &mut self,
        sort_expr: SortOrderExpr,
    ) -> Result<physical_expr::PhysicalSortExpr> {
        let expr = self.from_optd_expr(sort_expr.child())?;
        Ok(physical_expr::PhysicalSortExpr {
            expr,
            options: match sort_expr.order() {
                SortOrderType::Asc => datafusion::arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                SortOrderType::Desc => datafusion::arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
        })
    }

    fn from_optd_expr(&mut self, expr: Expr) -> Result<Arc<dyn PhysicalExpr>> {
        match expr.typ() {
            OptRelNodeTyp::ColumnRef => {
                let expr = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let idx = expr.index();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Column::new("<expr>", idx),
                ))
            }
            OptRelNodeTyp::Constant(_) => {
                let expr = ConstantExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let value = expr.value();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Literal::new(value.clone()),
                ))
            }
            OptRelNodeTyp::Func(_) => {
                let expr = FuncExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let func = expr.func();
                let args = expr
                    .args()
                    .to_vec()
                    .into_iter()
                    .map(|expr| self.from_optd_expr(expr))
                    .collect::<Result<Vec<_>>>()?;
                let args = args.into_iter().map(|(expr, _)| expr).collect::<Vec<_>>();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::ScalarFunction::try_new(func, args)?,
                ))
            }
            OptRelNodeTyp::Sort => unreachable!(),
            OptRelNodeTyp::LogOp(typ) => {
                let expr = LogOpExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let children = expr.children().to_vec().into_iter();
                let first_expr = self.from_optd_expr(children.next().unwrap())?;
                let op = match typ {
                    LogOpType::And => datafusion::logical_expr::Operator::And,
                    LogOpType::Or => datafusion::logical_expr::Operator::Or,
                };
                children.try_fold(first_expr, |acc, expr| {
                    let expr = self.from_optd_expr(expr)?;
                    Ok(
                        Arc::new(datafusion::physical_plan::expressions::BinaryExpr::new(
                            acc, op, expr,
                        )) as Arc<dyn PhysicalExpr>,
                    )
                })
            }
            _ => unimplemented!("{}", expr.into_rel_node()),
        }
    }

    #[async_recursion]
    async fn from_optd_projection(
        &mut self,
        node: PhysicalProjection,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .enumerate()
            .map(|(idx, expr)| Ok((self.from_optd_expr(expr)?, format!("col{}", idx))))
            .collect::<Result<Vec<_>>>()?;

        Ok(
            Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_filter(
        &mut self,
        node: PhysicalFilter,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_expr = self.from_optd_expr(node.cond())?;
        Ok(
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                physical_expr,
                input_exec,
            )?) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_sort(
        &mut self,
        node: PhysicalSort,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .map(|expr| {
                self.from_optd_sort_order_expr(
                    SortOrderExpr::from_rel_node(expr.into_rel_node()).unwrap(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(
            Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
                physical_exprs,
                input_exec,
            )) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn from_optd_agg(
        &mut self,
        node: PhysicalAgg,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .aggrs()
            .to_vec()
            .into_iter()
            .map(|expr| self.from_optd_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = node
            .groups()
            .to_vec()
            .into_iter()
            .map(|expr| self.from_optd_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(
            datafusion::physical_plan::aggregates::AggregateExec::try_new(
                AggregateMode::Single,
                group_exprs,
                physical_exprs,
                vec![],
                vec![],
                input_exec,
                /* a schema?? */
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    #[async_recursion]
    async fn from_optd_nested_loop_join(
        &mut self,
        node: PhysicalNestedLoopJoin,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let left_exec = self.from_optd_plan_node(node.left()).await?;
        let right_exec = self.from_optd_plan_node(node.right()).await?;
        let physical_expr = self.from_optd_expr(node.cond())?;
        Ok(Arc::new(
            datafusion::physical_plan::joins::NestedLoopJoinExec::try_new(
                left_exec,
                right_exec,
                physical_expr,
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    async fn from_optd_plan_node(&mut self, node: PlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        match node.typ() {
            OptRelNodeTyp::PhysicalScan => {
                self.from_optd_table_scan(
                    PhysicalScan::from_rel_node(node.into_rel_node()).unwrap(),
                )
                .await
            }
            OptRelNodeTyp::PhysicalProjection => {
                self.from_optd_projection(
                    PhysicalProjection::from_rel_node(node.into_rel_node()).unwrap(),
                )
                .await
            }
            OptRelNodeTyp::PhysicalFilter => {
                self.from_optd_filter(PhysicalFilter::from_rel_node(node.into_rel_node()).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalSort => {
                self.from_optd_sort(PhysicalSort::from_rel_node(node.into_rel_node()).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalAgg => {
                self.from_optd_agg(PhysicalAgg::from_rel_node(node.into_rel_node()).unwrap())
                    .await
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                self.from_optd_nested_loop_join(
                    PhysicalNestedLoopJoin::from_rel_node(node.into_rel_node()).unwrap(),
                )
                .await
            }
            typ => unimplemented!("{}", typ),
        }
    }

    pub async fn from_optd(&mut self, root_rel: OptRelNodeRef) -> Result<Arc<dyn ExecutionPlan>> {
        self.from_optd_plan_node(PlanNode::from_rel_node(root_rel).unwrap())
            .await
    }
}
