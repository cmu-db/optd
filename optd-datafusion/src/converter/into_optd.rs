use std::sync::Arc;

use anyhow::bail;
use datafusion::{
    common::DFSchema,
    logical_expr::{utils::conjunction, LogicalPlan as DFLogicalPlan, Operator},
    prelude::Expr,
};
use optd_core::{
    operators::{
        relational::logical::{
            filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator,
        },
        scalar::{
            binary_op,
            column_ref::ColumnRef,
            constants::{self, Constant},
            logic_op, unary_op, ScalarOperator,
        },
    },
    plans::{logical::LogicalPlan, scalar::ScalarPlan},
    values::OptdValue,
};

use super::ConversionContext;

impl ConversionContext<'_> {
    /// The col_offset is an offset added to the column index for all column references. It is useful for joins.
    pub fn conv_df_to_optd_scalar(
        df_expr: &Expr,
        context: &DFSchema,
        col_offset: usize,
    ) -> anyhow::Result<Arc<ScalarPlan>> {
        let operator = match df_expr {
            Expr::Column(column) => ScalarOperator::ColumnRef(ColumnRef {
                column_index: OptdValue::Int64(
                    (context.index_of_column(column).unwrap() + col_offset) as i64,
                ),
            }),
            Expr::Literal(scalar_value) => match scalar_value {
                datafusion::scalar::ScalarValue::Boolean(val) => {
                    ScalarOperator::Constant(Constant {
                        value: OptdValue::Bool((*val).unwrap()),
                    })
                }
                datafusion::scalar::ScalarValue::Int64(val) => {
                    ScalarOperator::Constant(Constant::new(OptdValue::Int64((*val).unwrap())))
                }
                datafusion::scalar::ScalarValue::Utf8(val) => {
                    ScalarOperator::Constant(Constant::new(OptdValue::String(val.clone().unwrap())))
                }
                _ => panic!("optd Only supports a limited number of literals"),
            },
            Expr::BinaryExpr(binary_expr) => {
                let left = Self::conv_df_to_optd_scalar(&binary_expr.left, context, col_offset)?;
                let right = Self::conv_df_to_optd_scalar(&binary_expr.right, context, col_offset)?;
                match binary_expr.op {
                    Operator::Plus => binary_op::add(left, right),
                    Operator::Minus => binary_op::minus(left, right),
                    Operator::Eq => binary_op::equal(left, right),
                    // TODO(yuchen): flatten logic operations as an optimization.
                    Operator::And => logic_op::and(vec![left, right]),
                    Operator::Or => logic_op::or(vec![left, right]),
                    _ => todo!(),
                }
            }
            Expr::Not(expr) => unary_op::not(Self::conv_df_to_optd_scalar(
                expr.as_ref(),
                context,
                col_offset,
            )?),
            Expr::Cast(cast) => {
                return Self::conv_df_to_optd_scalar(&cast.expr, context, col_offset);
            }
            _ => panic!(
                "optd does not support this scalar expression: {:#?}",
                df_expr
            ),
        };

        Ok(Arc::new(ScalarPlan { operator }))
    }

    fn flatten_scalar_as_conjunction(
        join_cond: Vec<Arc<ScalarPlan>>,
        idx: usize,
    ) -> Arc<ScalarPlan> {
        if idx == join_cond.len() - 1 {
            join_cond[idx].clone()
        } else {
            Arc::new(ScalarPlan {
                operator: logic_op::and(vec![
                    join_cond[idx].clone(),
                    Self::flatten_scalar_as_conjunction(join_cond.clone(), idx + 1),
                ]),
            })
        }
    }

    pub fn conv_df_to_optd_relational(
        &mut self,
        df_logical_plan: &DFLogicalPlan,
    ) -> anyhow::Result<Arc<LogicalPlan>> {
        let operator = match df_logical_plan {
            DFLogicalPlan::Filter(df_filter) => LogicalOperator::Filter(Filter {
                child: self.conv_df_to_optd_relational(&df_filter.input)?,
                predicate: Self::conv_df_to_optd_scalar(
                    &df_filter.predicate,
                    df_filter.input.schema(),
                    0,
                )?,
            }),
            DFLogicalPlan::Join(join) => {
                let mut join_cond = Vec::new();
                for (left, right) in &join.on {
                    let left = Self::conv_df_to_optd_scalar(left, join.left.schema(), 0)?;
                    let offset = join.left.schema().fields().len();
                    let right = Self::conv_df_to_optd_scalar(right, join.right.schema(), offset)?;
                    join_cond.push(Arc::new(ScalarPlan {
                        operator: binary_op::equal(left, right),
                    }));
                }
                if let Some(filter) = &join.filter {
                    let filter =
                        Self::conv_df_to_optd_scalar(filter, df_logical_plan.schema().as_ref(), 0)?;
                    join_cond.push(filter);
                }
                if join_cond.is_empty() {
                    join_cond.push(Arc::new(ScalarPlan {
                        operator: constants::boolean(true),
                    }));
                }

                LogicalOperator::Join(Join::new(
                    &join.join_type.to_string(),
                    self.conv_df_to_optd_relational(&join.left)?,
                    self.conv_df_to_optd_relational(&join.right)?,
                    Self::flatten_scalar_as_conjunction(join_cond, 0),
                ))
            }
            DFLogicalPlan::TableScan(table_scan) => {
                let table_name = table_scan.table_name.to_quoted_string();

                let combine_filters = conjunction(table_scan.filters.to_vec());
                let scan = LogicalOperator::Scan(Scan::new(
                    &table_scan.table_name.to_quoted_string(),
                    match combine_filters {
                        Some(df_expr) => {
                            let schema = DFSchema::try_from(table_scan.source.schema()).unwrap();
                            Self::conv_df_to_optd_scalar(&df_expr, &schema, 0)?
                        }
                        None => Arc::new(ScalarPlan {
                            operator: ScalarOperator::Constant(Constant {
                                value: OptdValue::Bool(true),
                            }),
                        }),
                    },
                ));

                self.tables.insert(table_name, table_scan.source.clone());

                scan
            }
            DFLogicalPlan::Projection(projection) => {
                let input = self.conv_df_to_optd_relational(projection.input.as_ref())?;
                let mut exprs = Vec::new();
                for expr in &projection.expr {
                    exprs.push(Self::conv_df_to_optd_scalar(
                        expr,
                        projection.input.schema(),
                        0,
                    )?);
                }

                LogicalOperator::Project(Project {
                    child: input,
                    fields: exprs,
                })
            }
            _ => bail!("optd does not support this operator"),
        };
        Ok(Arc::new(LogicalPlan { operator }))
    }
}
