use std::sync::Arc;

use datafusion::{
    common::DFSchema,
    logical_expr::{utils::conjunction, LogicalPlan as DatafusionLogicalPlan, Operator},
    prelude::Expr,
};
use optd_core::{
    operator::{
        relational::logical::{
            filter::Filter as OptdLogicalFilter, join::Join as OptdLogicalJoin,
            project::Project as OptdLogicalProjection, scan::Scan as OptdLogicalScan,
            LogicalOperator,
        },
        scalar::{
            add::Add, and::And, column_ref::ColumnRef, constants::Constant, equal::Equal,
            ScalarOperator,
        },
    },
    plan::{logical_plan::LogicalPlan, scalar_plan::ScalarPlan},
};

use super::ConversionContext;

impl ConversionContext<'_> {
    /// The col_offset is an offset added to the column index for all column references. It is useful for joins.
    pub fn conv_df_to_optd_scalar(
        &self,
        df_expr: &Expr,
        context: &DFSchema,
        col_offset: usize,
    ) -> ScalarPlan {
        let node = match df_expr {
            Expr::Column(column) => Arc::new(ScalarOperator::<ScalarPlan>::ColumnRef(ColumnRef {
                column_idx: context.index_of_column(column).unwrap() + col_offset,
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
                let left = self.conv_df_to_optd_scalar(&binary_expr.left, context, col_offset);
                let right = self.conv_df_to_optd_scalar(&binary_expr.right, context, col_offset);
                let op = match binary_expr.op {
                    Operator::Plus => {
                        ScalarOperator::<ScalarPlan>::Add(Add::<ScalarPlan> { left, right })
                    }
                    Operator::And => {
                        ScalarOperator::<ScalarPlan>::And(And::<ScalarPlan> { left, right })
                    }
                    Operator::Eq => {
                        ScalarOperator::<ScalarPlan>::Equal(Equal::<ScalarPlan> { left, right })
                    }
                    _ => panic!(
                        "OptD does not support this scalar binary expression: {:#?}",
                        df_expr
                    ),
                };
                Arc::new(op)
            }
            Expr::Cast(cast) => {
                return self.conv_df_to_optd_scalar(&cast.expr, context, col_offset);
            }
            _ => panic!(
                "OptD does not support this scalar expression: {:#?}",
                df_expr
            ),
        };

        ScalarPlan { node: node }
    }

    fn flatten_scalar_as_conjunction(join_cond: Vec<ScalarPlan>, idx: usize) -> ScalarPlan {
        if idx == join_cond.len() - 1 {
            join_cond[idx].clone()
        } else {
            ScalarPlan {
                node: Arc::new(ScalarOperator::<ScalarPlan>::And(And::<ScalarPlan> {
                    left: join_cond[idx].clone(),
                    right: Self::flatten_scalar_as_conjunction(join_cond.clone(), idx + 1),
                })),
            }
        }
    }

    pub fn conv_df_to_optd_relational(
        &mut self,
        df_logical_plan: &DatafusionLogicalPlan,
    ) -> LogicalPlan {
        let node = match df_logical_plan {
            DatafusionLogicalPlan::Filter(df_filter) => {
                let logical_optd_filter = OptdLogicalFilter::<LogicalPlan, ScalarPlan> {
                    child: self.conv_df_to_optd_relational(&df_filter.input),
                    predicate: self.conv_df_to_optd_scalar(
                        &df_filter.predicate,
                        df_filter.input.schema(),
                        0,
                    ),
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Filter(logical_optd_filter);
                Arc::new(op)
            }
            DatafusionLogicalPlan::Join(join) => {
                let mut join_cond = Vec::new();
                for (left, right) in &join.on {
                    let left = self.conv_df_to_optd_scalar(left, join.left.schema(), 0);
                    let offset = join.left.schema().fields().len();
                    let right = self.conv_df_to_optd_scalar(right, join.right.schema(), offset);
                    join_cond.push(ScalarPlan {
                        node: Arc::new(ScalarOperator::<ScalarPlan>::Equal(Equal { left, right })),
                    });
                }
                if join_cond.is_empty() {
                    join_cond.push(ScalarPlan {
                        node: Arc::new(ScalarOperator::<ScalarPlan>::Constant(Constant::Boolean(
                            true,
                        ))),
                    });
                }
                let logical_optd_join = OptdLogicalJoin::<LogicalPlan, ScalarPlan> {
                    join_type: join.join_type.to_string(),
                    left: self.conv_df_to_optd_relational(&join.left),
                    right: self.conv_df_to_optd_relational(&join.right),
                    condition: Self::flatten_scalar_as_conjunction(join_cond, 0),
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Join(logical_optd_join);
                Arc::new(op)
            }
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
                            Some(self.conv_df_to_optd_scalar(&df_expr, &schema, 0))
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
                    exprs.push(self.conv_df_to_optd_scalar(expr, projection.input.schema(), 0));
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
