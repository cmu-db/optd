use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::bail;
use async_recursion::async_recursion;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::JoinType,
    datasource::source_as_provider,
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal, NegativeExpr, NotExpr},
        joins::utils::{ColumnIndex, JoinFilter},
        projection::ProjectionExec,
        ExecutionPlan, PhysicalExpr,
    },
    scalar::ScalarValue,
};
use optd_core::{
    operators::{relational::physical::PhysicalOperator, scalar::ScalarOperator},
    plans::{physical::PhysicalPlan, scalar::ScalarPlan},
    values::OptdValue,
};

use super::ConversionContext;

impl ConversionContext<'_> {
    #[async_recursion]
    pub async fn conv_optd_to_df_relational(
        &self,
        optimized_plan: &PhysicalPlan,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        match &optimized_plan.operator {
            PhysicalOperator::TableScan(table_scan) => {
                let source = self
                    .tables
                    .get(table_scan.table_name.as_str().unwrap())
                    .ok_or_else(|| anyhow::anyhow!("Table not found"))?;
                let provider = source_as_provider(source)?;

                // TODO(yuchen): support filters inside table scan.
                let filters = vec![];
                let plan = provider
                    .scan(self.session_state, None, &filters, None)
                    .await?;
                Ok(plan)
            }
            PhysicalOperator::Filter(filter) => {
                let input_exec = self.conv_optd_to_df_relational(&filter.child).await?;
                let physical_expr =
                    Self::conv_optd_to_df_scalar(&filter.predicate, &input_exec.schema())?;
                Ok(
                    Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                        physical_expr,
                        input_exec,
                    )?) as Arc<dyn ExecutionPlan + 'static>,
                )
            }
            PhysicalOperator::Project(project) => {
                let input_exec = self.conv_optd_to_df_relational(&project.child).await?;
                let physical_exprs = project
                    .fields
                    .iter()
                    .cloned()
                    .filter_map(|field| {
                        Self::conv_optd_to_df_scalar(&field, &input_exec.schema()).ok()
                    })
                    .enumerate()
                    .map(|(idx, expr)| (expr, format!("col{}", idx)))
                    .collect::<Vec<(Arc<dyn PhysicalExpr>, String)>>();

                Ok(
                    Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                        as Arc<dyn ExecutionPlan + 'static>,
                )
            }
            PhysicalOperator::NestedLoopJoin(join) => {
                let left_exec = self.conv_optd_to_df_relational(&join.outer).await?;
                let right_exec = self.conv_optd_to_df_relational(&join.inner).await?;
                let filter_schema = {
                    let fields = left_exec
                        .schema()
                        .fields()
                        .into_iter()
                        .chain(right_exec.schema().fields().into_iter())
                        .cloned()
                        .collect::<Vec<_>>();
                    Schema::new_with_metadata(fields, HashMap::new())
                };

                let physical_expr = Self::conv_optd_to_df_scalar(
                    &join.condition,
                    &Arc::new(filter_schema.clone()),
                )?;

                let join_type = JoinType::from_str(join.join_type.as_str().unwrap())?;

                let mut column_idxs = vec![];
                for i in 0..left_exec.schema().fields().len() {
                    column_idxs.push(ColumnIndex {
                        index: i,
                        side: datafusion::common::JoinSide::Left,
                    });
                }
                for i in 0..right_exec.schema().fields().len() {
                    column_idxs.push(ColumnIndex {
                        index: i,
                        side: datafusion::common::JoinSide::Right,
                    });
                }

                Ok(Arc::new(
                    datafusion::physical_plan::joins::NestedLoopJoinExec::try_new(
                        left_exec,
                        right_exec,
                        Some(JoinFilter::new(
                            physical_expr,
                            column_idxs,
                            Arc::new(filter_schema),
                        )),
                        &join_type,
                        None,
                    )?,
                ) as Arc<dyn ExecutionPlan + 'static>)
            }
            PhysicalOperator::HashJoin(_hash_join) => todo!(),
            PhysicalOperator::SortMergeJoin(_merge_join) => todo!(),
        }
    }

    pub fn conv_optd_to_df_scalar(
        pred: &ScalarPlan,
        context: &SchemaRef,
    ) -> anyhow::Result<Arc<dyn PhysicalExpr>> {
        match &pred.operator {
            ScalarOperator::ColumnRef(column_ref) => {
                let idx = column_ref.column_index.as_i64().unwrap() as usize;
                Ok(Arc::new(
                    // Datafusion checks if col expr name matches the schema, so we have to supply the name inferred by datafusion,
                    // instead of using out own logical properties
                    Column::new(context.fields()[idx].name(), idx),
                ))
            }
            ScalarOperator::Constant(constant) => {
                let value = match &constant.value {
                    OptdValue::Int64(value) => ScalarValue::Int64(Some(*value)),
                    OptdValue::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    OptdValue::Bool(value) => ScalarValue::Boolean(Some(*value)),
                };
                Ok(Arc::new(Literal::new(value)))
            }
            ScalarOperator::BinaryOp(binary_op) => {
                let left = Self::conv_optd_to_df_scalar(&binary_op.left, context)?;
                let right = Self::conv_optd_to_df_scalar(&binary_op.right, context)?;
                // TODO(yuchen): really need the enums!
                let op = match binary_op.kind.as_str().unwrap() {
                    "add" => Operator::Plus,
                    "minus" => Operator::Minus,
                    "equal" => Operator::Eq,
                    s => panic!("Unsupported binary operator: {}", s),
                };
                Ok(Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>)
            }
            ScalarOperator::UnaryOp(unary_op) => {
                let child = Self::conv_optd_to_df_scalar(&unary_op.child, context)?;
                // TODO(yuchen): really need the enums!
                match unary_op.kind.as_str().unwrap() {
                    "not" => Ok(Arc::new(NotExpr::new(child)) as Arc<dyn PhysicalExpr>),
                    "neg" => Ok(Arc::new(NegativeExpr::new(child)) as Arc<dyn PhysicalExpr>),
                    s => bail!("Unsupported unary operator: {}", s),
                }
            }
            ScalarOperator::LogicOp(logic_op) => {
                let op = match logic_op.kind.as_str().unwrap() {
                    "and" => Operator::And,
                    "or" => Operator::Or,
                    s => bail!("Unsupported logic operator: {}", s),
                };
                let mut children = logic_op.children.iter();
                let first_child = Self::conv_optd_to_df_scalar(
                    children
                        .next()
                        .expect("LogicOp should have at least one child"),
                    context,
                )?;
                children.try_fold(first_child, |acc, expr| {
                    let expr = Self::conv_optd_to_df_scalar(expr, context)?;
                    Ok(Arc::new(BinaryExpr::new(acc, op, expr)) as Arc<dyn PhysicalExpr>)
                })
            }
        }
    }
}
