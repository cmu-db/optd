use anyhow::bail;
use datafusion::{
    common::DFSchema,
    logical_expr::{utils::conjunction, LogicalPlan as DataFusionLogicalPlan, Operator},
    prelude::Expr,
};
use optd_core::{
    cascades::ir::OperatorData,
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
};
use std::sync::Arc;

use super::OptdDataFusionContext;

impl OptdDataFusionContext<'_> {
    /// Given a DataFusion logical plan, returns an `optd` [`LogicalPlan`].
    pub(crate) fn df_to_optd_relational(
        &mut self,
        df_logical_plan: &DataFusionLogicalPlan,
    ) -> anyhow::Result<Arc<LogicalPlan>> {
        let operator = match df_logical_plan {
            DataFusionLogicalPlan::TableScan(table_scan) => {
                let table_name = table_scan.table_name.to_quoted_string();

                // Record the table name and source into the context.
                self.tables.insert(table_name, table_scan.source.clone());

                let combine_filters = conjunction(table_scan.filters.to_vec());
                let predicate = match combine_filters {
                    Some(df_expr) => {
                        let schema = DFSchema::try_from(table_scan.source.schema()).unwrap();
                        Self::df_to_optd_scalar(&df_expr, &schema, 0)?
                    }
                    None => Arc::new(ScalarPlan {
                        operator: ScalarOperator::Constant(Constant {
                            value: OperatorData::Bool(true),
                        }),
                    }),
                };

                LogicalOperator::Scan(Scan::new(
                    &table_scan.table_name.to_quoted_string(),
                    predicate,
                ))
            }
            DataFusionLogicalPlan::Projection(projection) => {
                let child = self.df_to_optd_relational(projection.input.as_ref())?;

                let exprs = projection
                    .expr
                    .iter()
                    .map(|expr| Self::df_to_optd_scalar(expr, projection.input.schema(), 0))
                    .collect::<anyhow::Result<Vec<_>>>()?;

                LogicalOperator::Project(Project {
                    child,
                    fields: exprs,
                })
            }
            DataFusionLogicalPlan::Filter(df_filter) => LogicalOperator::Filter(Filter {
                child: self.df_to_optd_relational(&df_filter.input)?,
                predicate: Self::df_to_optd_scalar(
                    &df_filter.predicate,
                    df_filter.input.schema(),
                    0,
                )?,
            }),
            DataFusionLogicalPlan::Join(join) => {
                let mut join_cond = join
                    .on
                    .iter()
                    .map(|(left, right)| {
                        let left = Self::df_to_optd_scalar(left, join.left.schema(), 0)?;
                        let offset = join.left.schema().fields().len();
                        let right = Self::df_to_optd_scalar(right, join.right.schema(), offset)?;
                        Ok(Arc::new(ScalarPlan {
                            operator: binary_op::equal(left, right),
                        }))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                if let Some(filter) = &join.filter {
                    let filter =
                        Self::df_to_optd_scalar(filter, df_logical_plan.schema().as_ref(), 0)?;
                    join_cond.push(filter);
                }

                if join_cond.is_empty() {
                    join_cond.push(Arc::new(ScalarPlan {
                        operator: constants::boolean(true),
                    }));
                }

                LogicalOperator::Join(Join::new(
                    &join.join_type.to_string(),
                    self.df_to_optd_relational(&join.left)?,
                    self.df_to_optd_relational(&join.right)?,
                    Self::flatten_scalar_as_conjunction(&join_cond, 0),
                ))
            }
            logical_plan => bail!("optd does not support this operator {:?}", logical_plan),
        };
        Ok(Arc::new(LogicalPlan { operator }))
    }

    /// Given a DataFusion [`Expr`], returns an `optd` [`ScalarPlan`].
    ///
    /// The `col_offset` input is an offset added to the column index for all column references,
    /// which is useful for joins.
    pub(crate) fn df_to_optd_scalar(
        df_expr: &Expr,
        context: &DFSchema,
        col_offset: usize,
    ) -> anyhow::Result<Arc<ScalarPlan>> {
        let operator = match df_expr {
            Expr::Column(column) => ScalarOperator::ColumnRef(ColumnRef {
                column_index: OperatorData::Int64(
                    (context.index_of_column(column)? + col_offset) as i64,
                ),
            }),
            Expr::Literal(scalar_value) => match scalar_value {
                datafusion::scalar::ScalarValue::Boolean(val) => {
                    ScalarOperator::Constant(Constant {
                        value: OperatorData::Bool((*val).unwrap()),
                    })
                }
                datafusion::scalar::ScalarValue::Int64(val) => {
                    ScalarOperator::Constant(Constant::new(OperatorData::Int64((*val).unwrap())))
                }
                datafusion::scalar::ScalarValue::Utf8(val) => ScalarOperator::Constant(
                    Constant::new(OperatorData::String(val.clone().unwrap())),
                ),
                _ => panic!("optd Only supports a limited number of literals"),
            },
            Expr::BinaryExpr(binary_expr) => {
                let left = Self::df_to_optd_scalar(&binary_expr.left, context, col_offset)?;
                let right = Self::df_to_optd_scalar(&binary_expr.right, context, col_offset)?;
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
            Expr::Not(expr) => {
                unary_op::not(Self::df_to_optd_scalar(expr.as_ref(), context, col_offset)?)
            }
            Expr::Cast(cast) => {
                return Self::df_to_optd_scalar(&cast.expr, context, col_offset);
            }
            _ => panic!(
                "optd does not support this scalar expression: {:#?}",
                df_expr
            ),
        };

        Ok(Arc::new(ScalarPlan { operator }))
    }

    /// Flattens a vector of scalar plans into a single scalar conjucntion tree. The `left_index`
    /// parameter specifies the index of the left side of the conjugation.
    fn flatten_scalar_as_conjunction(
        join_cond: &[Arc<ScalarPlan>],
        left_index: usize,
    ) -> Arc<ScalarPlan> {
        if left_index == join_cond.len() - 1 {
            join_cond[left_index].clone()
        } else {
            Arc::new(ScalarPlan {
                operator: logic_op::and(vec![
                    join_cond[left_index].clone(),
                    Self::flatten_scalar_as_conjunction(join_cond, left_index + 1),
                ]),
            })
        }
    }
}
