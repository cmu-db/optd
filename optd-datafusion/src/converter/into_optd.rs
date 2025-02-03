use std::sync::Arc;

use datafusion::{
    common::DFSchema,
    logical_expr::{utils::conjunction, LogicalPlan as DatafusionLogicalPlan, Operator},
    prelude::Expr,
};
use optd_core::{
    operator::{
        relational::logical::{
            filter::Filter as OptdLogicalFilter, project::Project as OptdLogicalProjection,
            scan::Scan as OptdLogicalScan, LogicalOperator,
        },
        scalar::{add::Add, and::And, column_ref::ColumnRef, constants::Constant, ScalarOperator},
    },
    plan::{logical_plan::LogicalPlan, scalar_plan::ScalarPlan},
};

use super::ConversionContext;

impl ConversionContext<'_> {
    pub fn conv_df_to_optd_scalar(&self, df_expr: &Expr, context: &DFSchema) -> ScalarPlan {
        let node = match df_expr {
            Expr::Column(column) => Arc::new(ScalarOperator::<ScalarPlan>::ColumnRef(ColumnRef {
                column_idx: context.index_of_column(column).unwrap(),
            })),
            Expr::Literal(scalar_value) => match scalar_value {
                datafusion::scalar::ScalarValue::Boolean(val) => {
                    Arc::new(ScalarOperator::<ScalarPlan>::Constant(Constant::Boolean(
                        val.clone().unwrap(),
                    )))
                }
                datafusion::scalar::ScalarValue::Float64(val) => {
                    Arc::new(ScalarOperator::<ScalarPlan>::Constant(Constant::Float(
                        val.clone().unwrap(),
                    )))
                }
                datafusion::scalar::ScalarValue::Int64(val) => Arc::new(
                    ScalarOperator::<ScalarPlan>::Constant(Constant::Integer(val.clone().unwrap())),
                ),
                datafusion::scalar::ScalarValue::Utf8(val) => Arc::new(
                    ScalarOperator::<ScalarPlan>::Constant(Constant::String(val.clone().unwrap())),
                ),
                _ => panic!("OptD Only supports a limited number of literals"),
            },
            Expr::BinaryExpr(binary_expr) => {
                let left = self.conv_df_to_optd_scalar(&binary_expr.left, context);
                let right = self.conv_df_to_optd_scalar(&binary_expr.right, context);
                let op = match binary_expr.op {
                    Operator::Plus => {
                        ScalarOperator::<ScalarPlan>::Add(Add::<ScalarPlan> { left, right })
                    }
                    Operator::And => {
                        ScalarOperator::<ScalarPlan>::And(And::<ScalarPlan> { left, right })
                    }
                    _ => panic!("OptD does not support this scalar binary expression"),
                };
                Arc::new(op)
            }
            _ => panic!("OptD does not support this scalar expression"),
        };

        ScalarPlan { node: node }
    }

    pub fn conv_df_to_optd_relational(
        &mut self,
        df_logical_plan: &DatafusionLogicalPlan,
    ) -> LogicalPlan {
        let node = match df_logical_plan {
            DatafusionLogicalPlan::Filter(df_filter) => {
                let logical_optd_filter = OptdLogicalFilter::<LogicalPlan, ScalarPlan> {
                    child: self.conv_df_to_optd_relational(&df_filter.input),
                    predicate: self
                        .conv_df_to_optd_scalar(&df_filter.predicate, df_filter.input.schema()),
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Filter(logical_optd_filter);
                Arc::new(op)
            }
            DatafusionLogicalPlan::Join(_join) => todo!(),
            DatafusionLogicalPlan::TableScan(table_scan) => {
                self.tables.insert(
                    table_scan.table_name.to_quoted_string(),
                    table_scan.source.clone(),
                );
                let combine_filters = conjunction(table_scan.filters.to_vec());
                let logical_optd_scan = OptdLogicalScan::<ScalarPlan> {
                    table_name: table_scan.table_name.to_quoted_string(),
                    predicate: match combine_filters {
                        Some(df_expr) => {
                            let schema = DFSchema::try_from(table_scan.source.schema()).unwrap();
                            Some(self.conv_df_to_optd_scalar(&df_expr, &schema))
                        }
                        None => None,
                    },
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Scan(logical_optd_scan);

                Arc::new(op)
            }
            DatafusionLogicalPlan::Projection(projection) => {
                let input = self.conv_df_to_optd_relational(projection.input.as_ref());
                let mut exprs = Vec::new();
                for expr in &projection.expr {
                    exprs.push(self.conv_df_to_optd_scalar(expr, projection.input.schema()));
                }
                let logical_optd_filter = OptdLogicalProjection::<LogicalPlan, ScalarPlan> {
                    child: input,
                    fields: exprs,
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Project(logical_optd_filter);
                Arc::new(op)
            }
            _ => panic!("OptD does not support this operator"),
        };
        LogicalPlan { node: node }
    }
}
