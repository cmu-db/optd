use anyhow::{bail, Result};
use datafusion::{
    common::DFSchema,
    logical_expr::{self, logical_plan, LogicalPlan, Operator},
    scalar::ScalarValue,
};
use datafusion_expr::Subquery;
use optd_core::rel_node::RelNode;
use optd_datafusion_repr::plan_nodes::{
    BetweenExpr, BinOpExpr, BinOpType, CastExpr, ColumnRefExpr, ConstantExpr, Expr, ExprList,
    ExternColumnRefExpr, FuncExpr, FuncType, InListExpr, JoinType, LikeExpr, LogOpExpr, LogOpType,
    LogicalAgg, LogicalEmptyRelation, LogicalFilter, LogicalJoin, LogicalLimit, LogicalProjection,
    LogicalScan, LogicalSort, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode, RawDependentJoin,
    SortOrderExpr, SortOrderType,
};
use optd_datafusion_repr::properties::schema::Schema as OptdSchema;

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    fn subqueries_to_dependent_joins(
        &mut self,
        subqueries: &[&Subquery],
        input: PlanNode,
        input_schema: &DFSchema,
    ) -> Result<PlanNode> {
        let mut node = input;
        for Subquery {
            subquery,
            outer_ref_columns,
        } in subqueries.iter()
        {
            let subquery_root = self.conv_into_optd_plan_node(subquery, Some(input_schema))?;
            let dep_join = RawDependentJoin::new(
                node,
                subquery_root,
                ConstantExpr::bool(true).into_expr(),
                ExprList::new(
                    outer_ref_columns
                        .iter()
                        .filter_map(|col| {
                            if let datafusion_expr::Expr::OuterReferenceColumn(_, col) = col {
                                Some(
                                    ExternColumnRefExpr::new(
                                        input_schema.index_of_column(col).unwrap(),
                                    )
                                    .into_expr(),
                                )
                            } else {
                                None
                            }
                        })
                        .collect(),
                ),
                JoinType::Cross,
            );
            node = PlanNode::from_rel_node(dep_join.into_rel_node()).unwrap();
        }
        Ok(node)
    }

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

    fn conv_into_optd_expr<'a>(
        &mut self,
        expr: &'a logical_expr::Expr,
        context: &DFSchema,
        dep_ctx: Option<&DFSchema>,
        subqueries: &mut Vec<&'a Subquery>,
    ) -> Result<Expr> {
        use logical_expr::Expr;
        match expr {
            Expr::BinaryExpr(node) => {
                let left =
                    self.conv_into_optd_expr(node.left.as_ref(), context, dep_ctx, subqueries)?;
                let right =
                    self.conv_into_optd_expr(node.right.as_ref(), context, dep_ctx, subqueries)?;
                match node.op {
                    Operator::And => {
                        let op = LogOpType::And;
                        let expr_list = ExprList::new(vec![left, right]);
                        return Ok(
                            LogOpExpr::new_flattened_nested_logical(op, expr_list).into_expr()
                        );
                    }
                    Operator::Or => {
                        let op = LogOpType::Or;
                        let expr_list = ExprList::new(vec![left, right]);
                        return Ok(
                            LogOpExpr::new_flattened_nested_logical(op, expr_list).into_expr()
                        );
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
            Expr::OuterReferenceColumn(_, col) => {
                let idx = dep_ctx.unwrap().index_of_column(col)?;
                Ok(ExternColumnRefExpr::new(idx).into_expr())
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
            Expr::Alias(x) => {
                self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)
            }
            Expr::ScalarFunction(x) => {
                let args = self.conv_into_optd_expr_list(&x.args, context, dep_ctx, subqueries)?;
                Ok(FuncExpr::new(FuncType::new_scalar(x.fun), args).into_expr())
            }
            Expr::AggregateFunction(x) => {
                let args = self.conv_into_optd_expr_list(&x.args, context, dep_ctx, subqueries)?;
                Ok(FuncExpr::new(FuncType::new_agg(x.fun.clone()), args).into_expr())
            }
            Expr::Case(x) => {
                let when_then_expr = &x.when_then_expr;
                assert_eq!(when_then_expr.len(), 1);
                let (when_expr, then_expr) = &when_then_expr[0];
                let when_expr =
                    self.conv_into_optd_expr(when_expr, context, dep_ctx, subqueries)?;
                let then_expr =
                    self.conv_into_optd_expr(then_expr, context, dep_ctx, subqueries)?;
                let else_expr = self.conv_into_optd_expr(
                    x.else_expr.as_ref().unwrap(),
                    context,
                    dep_ctx,
                    subqueries,
                )?;
                assert!(x.expr.is_none());
                Ok(FuncExpr::new(
                    FuncType::Case,
                    ExprList::new(vec![when_expr, then_expr, else_expr]),
                )
                .into_expr())
            }
            Expr::Sort(x) => {
                let expr =
                    self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
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
                let expr =
                    self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let low = self.conv_into_optd_expr(x.low.as_ref(), context, dep_ctx, subqueries)?;
                let high =
                    self.conv_into_optd_expr(x.high.as_ref(), context, dep_ctx, subqueries)?;
                assert!(!x.negated, "unimplemented");
                Ok(BetweenExpr::new(expr, low, high).into_expr())
            }
            Expr::Cast(x) => {
                let expr =
                    self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                Ok(CastExpr::new(expr, x.data_type.clone()).into_expr())
            }
            Expr::Like(x) => {
                let expr =
                    self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let pattern =
                    self.conv_into_optd_expr(x.pattern.as_ref(), context, dep_ctx, subqueries)?;
                Ok(LikeExpr::new(x.negated, x.case_insensitive, expr, pattern).into_expr())
            }
            Expr::InList(x) => {
                let expr =
                    self.conv_into_optd_expr(x.expr.as_ref(), context, dep_ctx, subqueries)?;
                let list = self.conv_into_optd_expr_list(&x.list, context, dep_ctx, subqueries)?;
                Ok(InListExpr::new(expr, list, x.negated).into_expr())
            }
            Expr::ScalarSubquery(sq) => {
                // This relies on a left-deep tree of dependent joins being
                // generated below this node, in response to all pushed subqueries.
                let new_column_ref_idx = context.fields().len() + subqueries.len();
                subqueries.push(sq);
                Ok(ColumnRefExpr::new(new_column_ref_idx).into_expr())
            }
            _ => bail!("Unsupported expression: {:?}", expr),
        }
    }

    fn conv_into_optd_projection(
        &mut self,
        node: &logical_plan::Projection,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalProjection> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr_list = self.conv_into_optd_expr_list(
            &node.expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        let input = self.subqueries_to_dependent_joins(&subqueries, input, node.input.schema())?;
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn conv_into_optd_filter(
        &mut self,
        node: &logical_plan::Filter,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalFilter> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr = self.conv_into_optd_expr(
            &node.predicate,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;

        let input = self.subqueries_to_dependent_joins(&subqueries, input, node.input.schema())?;

        Ok(LogicalFilter::new(input, expr))
    }

    fn conv_into_optd_expr_list<'a>(
        &mut self,
        exprs: &'a [logical_expr::Expr],
        context: &DFSchema,
        dep_ctx: Option<&DFSchema>,
        subqueries: &mut Vec<&'a Subquery>,
    ) -> Result<ExprList> {
        let exprs = exprs
            .iter()
            .map(|expr| self.conv_into_optd_expr(expr, context, dep_ctx, subqueries))
            .collect::<Result<Vec<_>>>()?;
        Ok(ExprList::new(exprs))
    }

    fn conv_into_optd_sort(
        &mut self,
        node: &logical_plan::Sort,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalSort> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let expr_list = self.conv_into_optd_expr_list(
            &node.expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_sort---not supported currently"
        );
        Ok(LogicalSort::new(input, expr_list))
    }

    fn conv_into_optd_agg(
        &mut self,
        node: &logical_plan::Aggregate,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalAgg> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?;
        let mut subqueries = vec![];
        let agg_exprs = self.conv_into_optd_expr_list(
            &node.aggr_expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        let group_exprs = self.conv_into_optd_expr_list(
            &node.group_expr,
            node.input.schema(),
            dep_ctx,
            &mut subqueries,
        )?;
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_agg---not supported currently"
        );
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
                predicates: Vec::new(), /* TODO: refactor */
            }
            .into(),
        )
        .unwrap()
    }

    fn conv_into_optd_join(
        &mut self,
        node: &logical_plan::Join,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalJoin> {
        use logical_plan::JoinType as DFJoinType;
        let left = self.conv_into_optd_plan_node(node.left.as_ref(), dep_ctx)?;
        let right = self.conv_into_optd_plan_node(node.right.as_ref(), dep_ctx)?;
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
        let mut subqueries = vec![];
        for (left, right) in &node.on {
            let left =
                self.conv_into_optd_expr(left, node.left.schema(), dep_ctx, &mut subqueries)?;
            let right =
                self.conv_into_optd_expr(right, node.right.schema(), dep_ctx, &mut subqueries)?;
            let right = Self::add_column_offset(node.left.schema().fields().len(), right);
            let op = BinOpType::Eq;
            let expr = BinOpExpr::new(left, right, op).into_expr();
            log_ops.push(expr);
        }
        if node.filter.is_some() {
            let filter = self.conv_into_optd_expr(
                node.filter.as_ref().unwrap(),
                node.schema.as_ref(),
                dep_ctx,
                &mut subqueries,
            )?;
            log_ops.push(filter);
        }
        assert!(
            subqueries.is_empty(),
            "Subqueries encountered in conv_into_optd_join---not supported currently"
        );

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
            let log_op = LogOpExpr::new_flattened_nested_logical(LogOpType::And, expr_list);
            Ok(LogicalJoin::new(left, right, log_op.into_expr(), join_type))
        }
    }

    fn conv_into_optd_cross_join(
        &mut self,
        node: &logical_plan::CrossJoin,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalJoin> {
        let left = self.conv_into_optd_plan_node(node.left.as_ref(), dep_ctx)?;
        let right = self.conv_into_optd_plan_node(node.right.as_ref(), dep_ctx)?;
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
        let empty_schema = OptdSchema { fields: vec![] };
        Ok(LogicalEmptyRelation::new(
            node.produce_one_row,
            empty_schema,
        ))
    }

    fn conv_into_optd_limit(
        &mut self,
        node: &logical_plan::Limit,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<LogicalLimit> {
        let input = self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?;
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

    fn conv_into_optd_plan_node(
        &mut self,
        node: &LogicalPlan,
        dep_ctx: Option<&DFSchema>,
    ) -> Result<PlanNode> {
        let node = match node {
            LogicalPlan::TableScan(node) => self.conv_into_optd_table_scan(node)?.into_plan_node(),
            LogicalPlan::Projection(node) => self
                .conv_into_optd_projection(node, dep_ctx)?
                .into_plan_node(),
            LogicalPlan::Sort(node) => self.conv_into_optd_sort(node, dep_ctx)?.into_plan_node(),
            LogicalPlan::Aggregate(node) => {
                self.conv_into_optd_agg(node, dep_ctx)?.into_plan_node()
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.conv_into_optd_plan_node(node.input.as_ref(), dep_ctx)?
            }
            LogicalPlan::Join(node) => self.conv_into_optd_join(node, dep_ctx)?.into_plan_node(),
            LogicalPlan::Filter(node) => {
                self.conv_into_optd_filter(node, dep_ctx)?.into_plan_node()
            }
            LogicalPlan::CrossJoin(node) => self
                .conv_into_optd_cross_join(node, dep_ctx)?
                .into_plan_node(),
            LogicalPlan::EmptyRelation(node) => {
                self.conv_into_optd_empty_relation(node)?.into_plan_node()
            }
            LogicalPlan::Limit(node) => self.conv_into_optd_limit(node, dep_ctx)?.into_plan_node(),
            _ => bail!(
                "unsupported plan node: {}",
                format!("{:?}", node).split('\n').next().unwrap()
            ),
        };
        Ok(node)
    }

    pub fn conv_into_optd(&mut self, root_rel: &LogicalPlan) -> Result<OptRelNodeRef> {
        let res = self.conv_into_optd_plan_node(root_rel, None)?;
        Ok(res.into_rel_node())
    }
}
