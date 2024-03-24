use anyhow::{bail, Result};
use datafusion::{
    common::DFSchema,
    logical_expr::{self, logical_plan, LogicalPlan, Operator},
    scalar::ScalarValue,
};
use optd_core::rel_node::RelNode;
use optd_datafusion_repr::properties::schema::Schema as OPTDSchema;
use optd_datafusion_repr::{
    plan_nodes::{
        BetweenExpr, BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, Expr, ExprList,
        FuncExpr, FuncType, InListExpr, JoinType, LikeExpr, LogOpExpr, LogOpType, LogicalAgg,
        LogicalEmptyRelation, LogicalFilter, LogicalJoin, LogicalLimit, LogicalProjection,
        LogicalScan, LogicalSort, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode,
        SortOrderExpr, SortOrderType,
    },
    Value,
};

use crate::OptdPlanContext;

// flatten_nested_logical is a helper function to flatten nested logical operators with same op type
// eg. (a AND (b AND c)) => ExprList([a, b, c])
//    (a OR (b OR c)) => ExprList([a, b, c])
// It assume the children of the input expr_list are already flattened
//  and can only be used in bottom up manner
fn flatten_nested_logical(op: LogOpType, expr_list: ExprList) -> ExprList {
    // conv_into_optd_expr is building the children bottom up so there is no need to
    // call flatten_nested_logical recursively
    let mut new_expr_list = Vec::new();
    for child in expr_list.to_vec() {
        if let OptRelNodeTyp::LogOp(child_op) = child.typ() {
            if child_op == op {
                let child_log_op_expr = LogOpExpr::from_rel_node(child.into_rel_node()).unwrap();
                new_expr_list.extend(child_log_op_expr.children().to_vec());
                continue;
            }
        }
        new_expr_list.push(child.clone());
    }
    ExprList::new(new_expr_list)
}

impl OptdPlanContext<'_> {
    fn conv_into_optd_table_scan(&mut self, node: &logical_plan::TableScan) -> Result<PlanNode> {
        let table_name = Value::String(node.table_name.to_string().into());

        let converted_fetch = if let Some(x) = node.fetch {
            x.try_into().unwrap()
        } else {
            u64::MAX // u64 MAX represents infinity (not the best way to do this)
        };
        let converted_fetch = ConstantExpr::uint64(converted_fetch).into_expr();

        let converted_projections = if let Some(projection) = &node.projection {
            let mut exprs = Vec::with_capacity(projection.len());
            for &p in projection {
                exprs.push(ColumnRefExpr::new(p).into_expr());
            }
            ExprList::new(exprs)
        } else {
            ExprList::new(vec![])
        };

        let converted_filters = LogOpExpr::new(
            LogOpType::And,
            self.conv_into_optd_expr_list(&node.filters, &node.projected_schema)
                .unwrap(),
        )
        .into_expr();

        self.tables
            .insert(table_name.as_str().to_string(), node.source.clone());
        let scan = LogicalScan::new(
            converted_filters,
            converted_projections,
            converted_fetch,
            table_name,
        );
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
                match node.op {
                    Operator::And => {
                        let op = LogOpType::And;
                        let expr_list = ExprList::new(vec![left, right]);
                        let expr_list = flatten_nested_logical(op, expr_list);
                        return Ok(LogOpExpr::new(op, expr_list).into_expr());
                    }
                    Operator::Or => {
                        let op = LogOpType::Or;
                        let expr_list = ExprList::new(vec![left, right]);
                        let expr_list = flatten_nested_logical(op, expr_list);
                        return Ok(LogOpExpr::new(op, expr_list).into_expr());
                    }
                    _ => {}
                }

                let op = match node.op {
                    Operator::Eq => BinOpType::Eq,
                    Operator::NotEq => BinOpType::Neq,
                    Operator::LtEq => BinOpType::Leq,
                    Operator::Lt => BinOpType::Lt,
                    Operator::GtEq => BinOpType::Geq,
                    Operator::Gt => BinOpType::Gt,
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
                ScalarValue::IntervalMonthDayNano(x) => {
                    let x = x.as_ref().unwrap();
                    Ok(ConstantExpr::interval_month_day_nano(*x).into_expr())
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
                Ok(CastExpr::new(expr, x.data_type.clone()).into_expr())
            }
            Expr::Like(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                let pattern = self.conv_into_optd_expr(x.pattern.as_ref(), context)?;
                Ok(LikeExpr::new(x.negated, x.case_insensitive, expr, pattern).into_expr())
            }
            Expr::InList(x) => {
                let expr = self.conv_into_optd_expr(x.expr.as_ref(), context)?;
                let list = self.conv_into_optd_expr_list(&x.list, context)?;
                Ok(InListExpr::new(expr, list, x.negated).into_expr())
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
        if node.filter.is_some() {
            let filter =
                self.conv_into_optd_expr(node.filter.as_ref().unwrap(), node.schema.as_ref())?;
            log_ops.push(filter);
        }

        if log_ops.is_empty() {
            Ok(LogicalJoin::new(
                left,
                right,
                ConstantExpr::bool(true).into_expr(),
                join_type,
            ))
        } else if log_ops.len() == 1 {
            Ok(LogicalJoin::new(left, right, log_ops.remove(0), join_type))
        } else {
            let expr_list = ExprList::new(log_ops);
            // the expr from filter is already flattened in conv_into_optd_expr
            let expr_list = flatten_nested_logical(LogOpType::And, expr_list);
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
        // empty_relation from datafusion always have an empty schema
        let empty_schema = OPTDSchema { fields: vec![] };
        Ok(LogicalEmptyRelation::new(
            node.produce_one_row,
            empty_schema,
        ))
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
