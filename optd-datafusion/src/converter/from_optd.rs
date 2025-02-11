use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_recursion::async_recursion;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    common::JoinType,
    datasource::source_as_provider,
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
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
                let physical_expr = self
                    .conv_optd_to_df_scalar(&filter.predicate, &input_exec.schema())
                    .clone();
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
                    .to_vec()
                    .into_iter()
                    .map(|field| {
                        self.conv_optd_to_df_scalar(&field, &input_exec.schema())
                            .clone()
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

                let physical_expr =
                    self.conv_optd_to_df_scalar(&join.condition, &Arc::new(filter_schema.clone()));

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
                        Some(JoinFilter::new(physical_expr, column_idxs, filter_schema)),
                        &join_type,
                    )?,
                ) as Arc<dyn ExecutionPlan + 'static>)
            }
            PhysicalOperator::HashJoin(_hash_join) => todo!(),
            PhysicalOperator::SortMergeJoin(_merge_join) => todo!(),
        }
    }

    pub fn conv_optd_to_df_scalar(
        &self,
        pred: &ScalarPlan,
        context: &SchemaRef,
    ) -> Arc<dyn PhysicalExpr> {
        match &pred.operator {
            ScalarOperator::ColumnRef(column_ref) => {
                let idx = column_ref.column_index.as_i64().unwrap() as usize;
                Arc::new(
                    // Datafusion checks if col expr name matches the schema, so we have to supply the name inferred by datafusion,
                    // instead of using out own logical properties
                    Column::new(context.fields()[idx].name(), idx),
                )
            }
            ScalarOperator::Constant(constant) => {
                let value = match &constant.value {
                    OptdValue::Int64(value) => ScalarValue::Int64(Some(*value)),
                    OptdValue::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    OptdValue::Bool(value) => ScalarValue::Boolean(Some(*value)),
                };
                Arc::new(Literal::new(value))
            }
            ScalarOperator::And(and) => {
                let left = self.conv_optd_to_df_scalar(&and.left, context);
                let right = self.conv_optd_to_df_scalar(&and.right, context);
                let op = Operator::And;
                Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>
            }
            ScalarOperator::Add(add) => {
                let left = self.conv_optd_to_df_scalar(&add.left, context);
                let right = self.conv_optd_to_df_scalar(&add.right, context);
                let op = Operator::Plus;
                Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>
            }
            ScalarOperator::Equal(equal) => {
                let left = self.conv_optd_to_df_scalar(&equal.left, context);
                let right = self.conv_optd_to_df_scalar(&equal.right, context);
                let op = Operator::Eq;
                Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>
            }
        }
    }
}
