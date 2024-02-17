use anyhow::{bail, Result};
use datafusion::{
    common::DFSchema,
    logical_expr::{self, logical_plan, LogicalPlan, Operator},
    scalar::ScalarValue,
};
use datafusion_expr::Expr as DFExpr;
use optd_core::rel_node::RelNode;
use optd_datafusion_repr::{
    plan_nodes::{
        BetweenExpr, BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, Expr, ExprList,
        FuncExpr, FuncType, JoinType, LikeExpr, LogOpExpr, LogOpType, LogicalAgg,
        LogicalEmptyRelation, LogicalFilter, LogicalJoin, LogicalLimit, LogicalProjection,
        LogicalScan, LogicalSort, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode,
        SortOrderExpr, SortOrderType,
    },
    Value,
};

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    fn conv_into_optd_table_scan(&mut self, node: &logical_plan::TableScan) -> Result<PlanNode> {
        let table_name = node.table_name.to_string();
        if node.fetch.is_some() {
            bail!("fetch")
        }
        if !node.filters.is_empty() {
            bail!("no filters")
        }
        self.tables.insert(table_name.clone(), node.source.clone());
        let scan = LogicalScan::new(table_name);
        if let Some(ref projection) = node.projection {
            let mut exprs = Vec::with_capacity(projection.len());
            for &p in projection {
                exprs.push(ColumnRefExpr::new(p).into_expr());
            }
            let projection = LogicalProjection::new(scan.into_plan_node(), ExprList::new(exprs));
            return Ok(projection.into_plan_node());
        }
        Ok(scan.into_plan_node())
    }

    fn conv_into_optd_expr(
        &mut self,
        expr: &logical_expr::Expr,
        context: &DFSchema,
    ) -> Result<Expr> {
        use logical_expr::Expr;
        match expr {
            Expr::BinaryExpr(node) => {
                let left = self.conv_into_optd_expr(node.left.as_ref(), context)?;
                let right = self.conv_into_optd_expr(node.right.as_ref(), context)?;
                let op = match node.op {
                    Operator::Eq => BinOpType::Eq,
                    Operator::NotEq => BinOpType::Neq,
                    Operator::LtEq => BinOpType::Leq,
                    Operator::Lt => BinOpType::Lt,
                    Operator::GtEq => BinOpType::Geq,
                    Operator::Gt => BinOpType::Gt,
                    Operator::And => BinOpType::And,
                    Operator::Or => BinOpType::Or,
                    Operator::Plus => BinOpType::Add,
                    Operator::Minus => BinOpType::Sub,
                    Operator::Multiply => BinOpType::Mul,
                    Operator::Divide => BinOpType::Div,
                    op => unimplemented!("{}", op),
                };
                Ok(BinOpExpr::new(left, right, op).into_expr())
            }
            Expr::Column(col) => {
                let idx = context.index_of_column(col)?;
                Ok(ColumnRefExpr::new(idx).into_expr())
            }
            Expr::Literal(x) => match x {
                ScalarValue::UInt8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::uint8(*x).into_expr())
                }
                ScalarValue::UInt16(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::uint16(*x).into_expr())
                }
                ScalarValue::UInt32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::uint32(*x).into_expr())
                }
                ScalarValue::UInt64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::uint64(*x).into_expr())
                }
                ScalarValue::Int8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::int8(*x).into_expr())
                }
                ScalarValue::Int16(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::int16(*x).into_expr())
                }
                ScalarValue::Int32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::int32(*x).into_expr())
                }
                ScalarValue::Int64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::int64(*x).into_expr())
                }
                ScalarValue::Float64(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::float64(*x).into_expr())
                }
                ScalarValue::Utf8(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::string(x).into_expr())
                }
                ScalarValue::Date32(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::date(*x as i64).into_expr())
                }
                ScalarValue::Decimal128(x, _, _) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::decimal(*x as f64).into_expr())
                }
                ScalarValue::Boolean(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::bool(*x).into_expr())
                }
                _ => bail!("{:?}", x),
            },
            Expr::Alias(x) => self.conv_into_optd_expr(x.expr.as_ref(), context),
            Expr::ScalarFunction(x) => {
                let args = self.conv_into_optd_expr_list(&x.args, context)?;
                Ok(FuncExpr::new(FuncType::new_scalar(x.fun), args).into_expr())
            }
            Expr::AggregateFunction(x) => {
                let args = self.conv_into_optd_expr_list(&x.args, context)?;
                Ok(FuncExpr::new(FuncType::new_agg(x.fun.clone()), args).into_expr())
            }
            Expr::Case(x) => {
                let when_then_expr = &x.when_then_expr;
                assert_eq!(when_then_expr.len(), 1);
                let (when_expr, then_expr) = &when_then_expr[0];
                let when_expr = self.conv_into_optd_expr(when_expr, context)?;
                let then_expr = self.conv_into_optd_expr(then_expr, context)?;
                let else_expr = self.conv_into_optd_expr(x.else_expr.as_ref().unwrap(), context)?;
                assert!(x.expr.is_none());
                Ok(FuncExpr::new(
                    FuncType::Case,
                    ExprList::new(vec![when_expr, then_expr, else_expr]),
                )
                .into_expr())
            }
            Expr::Sort(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                Ok(SortOrderExpr::new(
                    if x.asc {
                        SortOrderType::Asc
                    } else {
                        SortOrderType::Desc
                    },
                    expr,
                )
                .into_expr())
            }
            Expr::Between(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                let low = self.conv_into_optd_expr(x.low.as_ref(), context)?;
                let high = self.conv_into_optd_expr(x.high.as_ref(), context)?;
                assert!(!x.negated, "unimplemented");
                Ok(BetweenExpr::new(expr, low, high).into_expr())
            }
            Expr::Cast(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                let data_type = x.data_type.clone();
                let val = match data_type {
                    arrow_schema::DataType::Int8 => Value::Int8(0),
                    arrow_schema::DataType::Int16 => Value::Int16(0),
                    arrow_schema::DataType::Int32 => Value::Int32(0),
                    arrow_schema::DataType::Int64 => Value::Int64(0),
                    arrow_schema::DataType::UInt8 => Value::UInt8(0),
                    arrow_schema::DataType::UInt16 => Value::UInt16(0),
                    arrow_schema::DataType::UInt32 => Value::UInt32(0),
                    arrow_schema::DataType::UInt64 => Value::UInt64(0),
                    arrow_schema::DataType::Date32 => Value::Date32(0),
                    arrow_schema::DataType::Decimal128(_, _) => Value::Decimal128(0),
                    other => unimplemented!("unimplemented datatype {:?}", other),
                };
                Ok(CastExpr::new(expr, val).into_expr())
            }
            Expr::Like(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                let pattern = self.conv_into_optd_expr(x.pattern.as_ref(), context)?;
                Ok(LikeExpr::new(x.negated, x.case_insensitive, expr, pattern).into_expr())
            }
            _ => bail!("Unsupported expression: {:?}", expr),
        }
    }

    fn conv_into_optd_projection(
        &mut self,
        node: &logical_plan::Projection,
    ) -> Result<LogicalProjection> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.conv_into_optd_expr_list(&node.expr, node.input.schema())?;
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn conv_into_optd_filter(&mut self, node: &logical_plan::Filter) -> Result<LogicalFilter> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref())?;
        let expr = self.conv_into_optd_expr(&node.predicate, node.input.schema())?;
        Ok(LogicalFilter::new(input, expr))
    }

    fn conv_into_optd_expr_list(
        &mut self,
        exprs: &[logical_expr::Expr],
        context: &DFSchema,
    ) -> Result<ExprList> {
        let exprs = exprs
            .iter()
            .map(|expr| self.conv_into_optd_expr(expr, context))
            .collect::<Result<Vec<_>>>()?;
        Ok(ExprList::new(exprs))
    }

    fn conv_into_optd_sort(&mut self, node: &logical_plan::Sort) -> Result<LogicalSort> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.conv_into_optd_expr_list(&node.expr, node.input.schema())?;
        Ok(LogicalSort::new(input, expr_list))
    }

    fn conv_into_optd_agg(&mut self, node: &logical_plan::Aggregate) -> Result<LogicalAgg> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref())?;
        let agg_exprs = self.conv_into_optd_expr_list(&node.aggr_expr, node.input.schema())?;
        let group_exprs = self.conv_into_optd_expr_list(&node.group_expr, node.input.schema())?;
        Ok(LogicalAgg::new(input, agg_exprs, group_exprs))
    }

    fn add_column_offset(offset: usize, expr: Expr) -> Expr {
        if expr.typ() == OptRelNodeTyp::ColumnRef {
            let expr = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
            return ColumnRefExpr::new(expr.index() + offset).into_expr();
        }
        let rel_node = expr.into_rel_node();
        let children = rel_node
            .children
            .iter()
            .map(|child| {
                let child = child.clone();
                let child = Expr::from_rel_node(child).unwrap();
                let child = Self::add_column_offset(offset, child);
                child.into_rel_node()
            })
            .collect();
        Expr::from_rel_node(
            RelNode {
                typ: rel_node.typ.clone(),
                children,
                data: rel_node.data.clone(),
            }
            .into(),
        )
        .unwrap()
    }

    fn conv_into_optd_join(&mut self, node: &logical_plan::Join) -> Result<LogicalJoin> {
        use logical_plan::JoinType as DFJoinType;
        let left = self.conv_into_optd_plan_node(node.left.as_ref())?;
        let right = self.conv_into_optd_plan_node(node.right.as_ref())?;
        let join_type = match node.join_type {
            DFJoinType::Inner => JoinType::Inner,
            DFJoinType::Left => JoinType::LeftOuter,
            DFJoinType::Right => JoinType::RightOuter,
            DFJoinType::Full => JoinType::FullOuter,
            DFJoinType::LeftAnti => JoinType::LeftAnti,
            DFJoinType::RightAnti => JoinType::RightAnti,
            DFJoinType::LeftSemi => JoinType::LeftSemi,
            DFJoinType::RightSemi => JoinType::RightSemi,
        };
        let mut log_ops = Vec::with_capacity(node.on.len());
        for (left, right) in &node.on {
            let left = self.conv_into_optd_expr(left, node.left.schema())?;
            let right = self.conv_into_optd_expr(right, node.right.schema())?;
            let right = Self::add_column_offset(node.left.schema().fields().len(), right);
            let op = BinOpType::Eq;
            let expr = BinOpExpr::new(left, right, op).into_expr();
            log_ops.push(expr);
        }

        if log_ops.is_empty() {
            // optd currently only supports
            // 1. normal equal condition join
            //    select * from a join b on a.id = b.id
            // 2. join on false/true
            //    select * from a join b on false/true
            // 3. join on other literals or other filters are not supported
            //  instead of converting them to a join on true, we bail out

            match node.filter {
                Some(DFExpr::Literal(ScalarValue::Boolean(Some(val)))) => Ok(LogicalJoin::new(
                    left,
                    right,
                    ConstantExpr::bool(val).into_expr(),
                    join_type,
                )),
                None => Ok(LogicalJoin::new(
                    left,
                    right,
                    ConstantExpr::bool(true).into_expr(),
                    join_type,
                )),
                _ => bail!("unsupported join filter: {:?}", node.filter),
            }
        } else if log_ops.len() == 1 {
            Ok(LogicalJoin::new(left, right, log_ops.remove(0), join_type))
        } else {
            let expr_list = ExprList::new(log_ops);
            Ok(LogicalJoin::new(
                left,
                right,
                LogOpExpr::new(LogOpType::And, expr_list).into_expr(),
                join_type,
            ))
        }
    }

    fn conv_into_optd_cross_join(&mut self, node: &logical_plan::CrossJoin) -> Result<LogicalJoin> {
        let left = self.conv_into_optd_plan_node(node.left.as_ref())?;
        let right = self.conv_into_optd_plan_node(node.right.as_ref())?;
        Ok(LogicalJoin::new(
            left,
            right,
            ConstantExpr::bool(true).into_expr(),
            JoinType::Cross,
        ))
    }

    fn conv_into_optd_empty_relation(
        &mut self,
        node: &logical_plan::EmptyRelation,
    ) -> Result<LogicalEmptyRelation> {
        Ok(LogicalEmptyRelation::new(node.produce_one_row))
    }

    fn conv_into_optd_limit(&mut self, node: &logical_plan::Limit) -> Result<LogicalLimit> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref())?;
        // try_into guys are converting usize to u64.
        let converted_skip = node.skip.try_into().unwrap();
        let converted_fetch = if let Some(x) = node.fetch {
            x.try_into().unwrap()
        } else {
            u64::MAX // u64 MAX represents infinity (not the best way to do this)
        };
        Ok(LogicalLimit::new(
            input,
            ConstantExpr::uint64(converted_skip).into_expr(),
            ConstantExpr::uint64(converted_fetch).into_expr(),
        ))
    }

    fn conv_into_optd_plan_node(&mut self, node: &LogicalPlan) -> Result<PlanNode> {
        let node = match node {
            LogicalPlan::TableScan(node) => self.conv_into_optd_table_scan(node)?.into_plan_node(),
            LogicalPlan::Projection(node) => self.conv_into_optd_projection(node)?.into_plan_node(),
            LogicalPlan::Sort(node) => self.conv_into_optd_sort(node)?.into_plan_node(),
            LogicalPlan::Aggregate(node) => self.conv_into_optd_agg(node)?.into_plan_node(),
            LogicalPlan::SubqueryAlias(node) => {
                self.conv_into_optd_plan_node(node.input.as_ref())?
            }
            LogicalPlan::Join(node) => self.conv_into_optd_join(node)?.into_plan_node(),
            LogicalPlan::Filter(node) => self.conv_into_optd_filter(node)?.into_plan_node(),
            LogicalPlan::CrossJoin(node) => self.conv_into_optd_cross_join(node)?.into_plan_node(),
            LogicalPlan::EmptyRelation(node) => {
                self.conv_into_optd_empty_relation(node)?.into_plan_node()
            }
            LogicalPlan::Limit(node) => self.conv_into_optd_limit(node)?.into_plan_node(),
            _ => bail!(
                "unsupported plan node: {}",
                format!("{:?}", node).split('\n').next().unwrap()
            ),
        };
        Ok(node)
    }

    pub fn conv_into_optd(&mut self, root_rel: &LogicalPlan) -> Result<OptRelNodeRef> {
        Ok(self.conv_into_optd_plan_node(root_rel)?.into_rel_node())
    }
}
