use anyhow::{bail, Result};
use datafusion::{
    common::DFSchema,
    logical_expr::{self, logical_plan, LogicalPlan, Operator},
    scalar::ScalarValue,
};
use optd_core::rel_node::RelNode;
use optd_datafusion_repr::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, FuncExpr, FuncType,
    JoinType, LogOpExpr, LogOpType, LogicalAgg, LogicalFilter, LogicalJoin, LogicalProjection,
    LogicalScan, LogicalSort, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode, SortOrderExpr,
    SortOrderType,
};

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    fn into_optd_table_scan(&mut self, node: &logical_plan::TableScan) -> Result<PlanNode> {
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

    fn into_optd_expr(&mut self, expr: &logical_expr::Expr, context: &DFSchema) -> Result<Expr> {
        use logical_expr::Expr;
        match expr {
            Expr::BinaryExpr(node) => {
                let left = self.into_optd_expr(node.left.as_ref(), context)?;
                let right = self.into_optd_expr(node.right.as_ref(), context)?;
                let op = match node.op {
                    Operator::Eq => BinOpType::Eq,
                    Operator::NotEq => BinOpType::Neq,
                    Operator::LtEq => BinOpType::Leq,
                    Operator::GtEq => BinOpType::Geq,
                    Operator::And => BinOpType::And,
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
                _ => bail!("{:?}", x),
            },
            Expr::Alias(x) => self.into_optd_expr(x.expr.as_ref(), context),
            Expr::ScalarFunction(x) => {
                let args = self.into_optd_expr_list(&x.args, context)?;
                Ok(FuncExpr::new(FuncType::new_scalar(x.fun), args).into_expr())
            }
            Expr::AggregateFunction(x) => {
                let args = self.into_optd_expr_list(&x.args, context)?;
                Ok(FuncExpr::new(FuncType::new_agg(x.fun.clone()), args).into_expr())
            }
            Expr::Case(x) => {
                let when_then_expr = &x.when_then_expr;
                assert_eq!(when_then_expr.len(), 1);
                let (when_expr, then_expr) = &when_then_expr[0];
                let when_expr = self.into_optd_expr(&when_expr, context)?;
                let then_expr = self.into_optd_expr(&then_expr, context)?;
                let else_expr = self.into_optd_expr(x.else_expr.as_ref().unwrap(), context)?;
                assert!(x.expr.is_none());
                Ok(FuncExpr::new(
                    FuncType::Case,
                    ExprList::new(vec![when_expr, then_expr, else_expr]),
                )
                .into_expr())
            }
            Expr::Sort(x) => {
                let expr = self.into_optd_expr(x.expr.as_ref(), context)?;
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
            _ => bail!("{:?}", expr),
        }
    }

    fn into_optd_projection(
        &mut self,
        node: &logical_plan::Projection,
    ) -> Result<LogicalProjection> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.into_optd_expr_list(&node.expr, node.input.schema())?;
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn into_optd_filter(&mut self, node: &logical_plan::Filter) -> Result<LogicalFilter> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr = self.into_optd_expr(&node.predicate, node.input.schema())?;
        Ok(LogicalFilter::new(input, expr))
    }

    fn into_optd_expr_list(
        &mut self,
        exprs: &[logical_expr::Expr],
        context: &DFSchema,
    ) -> Result<ExprList> {
        let exprs = exprs
            .iter()
            .map(|expr| self.into_optd_expr(expr, context))
            .collect::<Result<Vec<_>>>()?;
        Ok(ExprList::new(exprs))
    }

    fn into_optd_sort(&mut self, node: &logical_plan::Sort) -> Result<LogicalSort> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.into_optd_expr_list(&node.expr, node.input.schema())?;
        Ok(LogicalSort::new(input, expr_list))
    }

    fn into_optd_agg(&mut self, node: &logical_plan::Aggregate) -> Result<LogicalAgg> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let agg_exprs = self.into_optd_expr_list(&node.aggr_expr, node.input.schema())?;
        let group_exprs = self.into_optd_expr_list(&node.group_expr, node.input.schema())?;
        Ok(LogicalAgg::new(input, agg_exprs, group_exprs))
    }

    fn add_column_offset(&mut self, offset: usize, expr: Expr) -> Expr {
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
                let child = self.add_column_offset(offset, child);
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

    fn into_optd_join(&mut self, node: &logical_plan::Join) -> Result<LogicalJoin> {
        use logical_plan::JoinType as DFJoinType;
        let left = self.into_optd_plan_node(node.left.as_ref())?;
        let right = self.into_optd_plan_node(node.right.as_ref())?;
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
        let mut log_ops = vec![];
        log_ops.reserve(node.on.len());
        for (left, right) in &node.on {
            let left = self.into_optd_expr(left, node.left.schema())?;
            let right = self.into_optd_expr(right, node.right.schema())?;
            let right = self.add_column_offset(node.left.schema().fields().len(), right);
            let op = BinOpType::Eq;
            let expr = BinOpExpr::new(left, right, op).into_expr();
            log_ops.push(expr);
        }

        if log_ops.len() == 1 {
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

    fn into_optd_plan_node(&mut self, node: &LogicalPlan) -> Result<PlanNode> {
        let node = match node {
            LogicalPlan::TableScan(node) => self.into_optd_table_scan(node)?.into_plan_node(),
            LogicalPlan::Projection(node) => self.into_optd_projection(node)?.into_plan_node(),
            LogicalPlan::Sort(node) => self.into_optd_sort(node)?.into_plan_node(),
            LogicalPlan::Aggregate(node) => self.into_optd_agg(node)?.into_plan_node(),
            LogicalPlan::SubqueryAlias(node) => self.into_optd_plan_node(node.input.as_ref())?,
            LogicalPlan::Join(node) => self.into_optd_join(node)?.into_plan_node(),
            LogicalPlan::Filter(node) => self.into_optd_filter(node)?.into_plan_node(),
            _ => bail!("{:?}", node),
        };
        Ok(node)
    }

    pub fn into_optd(&mut self, root_rel: &LogicalPlan) -> Result<OptRelNodeRef> {
        Ok(self.into_optd_plan_node(root_rel)?.into_rel_node())
    }
}
