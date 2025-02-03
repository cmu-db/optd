use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::source_as_provider,
    logical_expr::Operator,
    physical_plan::{
        expressions::{BinaryExpr, Column, Literal},
        projection::ProjectionExec,
        ExecutionPlan, PhysicalExpr,
    },
    scalar::ScalarValue,
};
use optd_core::{
    operator::{
        relational::physical::PhysicalOperator,
        scalar::{constants::Constant, ScalarOperator},
    },
    plan::{physical_plan::PhysicalPlan, scalar_plan::ScalarPlan},
};

use super::ConversionContext;

impl ConversionContext<'_> {
    #[async_recursion]
    pub async fn conv_optd_to_df_relational(
        &self,
        optimized_plan: &PhysicalPlan,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        match &*optimized_plan.node {
            PhysicalOperator::TableScan(table_scan) => {
                let source = self.tables.get(&table_scan.table_name).unwrap();
                let provider = source_as_provider(source)?;
                let filters = if let Some(ref _pred) = table_scan.predicate {
                    // split_binary_owned(pred, Operator::And)
                    todo!("Optd does not support filters inside table scan")
                } else {
                    vec![]
                };
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
                    .map(|expr| (expr, String::new()))
                    .collect::<Vec<(Arc<dyn PhysicalExpr>, String)>>();

                Ok(
                    Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                        as Arc<dyn ExecutionPlan + 'static>,
                )
            }
            PhysicalOperator::HashJoin(_hash_join) => todo!(),
            PhysicalOperator::NestedLoopJoin(_nested_loop_join) => todo!(),
            PhysicalOperator::SortMergeJoin(_merge_join) => todo!(),
        }
    }

    pub fn conv_optd_to_df_scalar(
        &self,
        pred: &ScalarPlan,
        context: &SchemaRef,
    ) -> Arc<dyn PhysicalExpr> {
        match &*pred.node {
            ScalarOperator::ColumnRef(column_ref) => {
                let idx = column_ref.column_idx;
                Arc::new(
                    // Datafusion checks if col expr name matches the schema, so we have to supply the name inferred by datafusion,
                    // instead of using out own logical properties
                    Column::new(context.fields()[idx].name(), idx),
                )
            }
            ScalarOperator::Constant(constant) => {
                let value = match constant {
                    Constant::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    Constant::Integer(value) => ScalarValue::Int64(Some(value.clone())),
                    Constant::Float(value) => ScalarValue::Float64(Some(value.clone())),
                    Constant::Boolean(value) => ScalarValue::Boolean(Some(value.clone())),
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
        }
    }
}
