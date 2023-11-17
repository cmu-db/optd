use anyhow::{bail, Result};
use datafusion::logical_expr::{self, logical_plan, LogicalPlan};
use optd_datafusion_repr::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, Expr, ExprList, JoinType, LogicalAgg,
    LogicalFilter, LogicalJoin, LogicalProjection, LogicalScan, LogicalSort, OptRelNode,
    OptRelNodeRef, PlanNode,
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

    fn into_optd_expr(&mut self, expr: &logical_expr::Expr) -> Result<Expr> {
        use logical_expr::Expr;
        match expr {
            Expr::BinaryExpr(node) => {
                let left = self.into_optd_expr(node.left.as_ref())?;
                let right = self.into_optd_expr(node.right.as_ref())?;
                let op = BinOpType::Add;
                Ok(BinOpExpr::new(left, right, op).into_expr())
            }
            _ => bail!("{:?}", expr),
        }
    }

    fn into_optd_projection(
        &mut self,
        node: &logical_plan::Projection,
    ) -> Result<LogicalProjection> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.into_optd_expr_list(&node.expr)?;
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn into_optd_filter(&mut self, node: &logical_plan::Filter) -> Result<LogicalFilter> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr = self.into_optd_expr(&node.predicate)?;
        Ok(LogicalFilter::new(input, expr))
    }

    fn into_optd_expr_list(&mut self, exprs: &[logical_expr::Expr]) -> Result<ExprList> {
        let exprs = exprs
            .iter()
            .map(|expr| self.into_optd_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        Ok(ExprList::new(exprs))
    }

    fn into_optd_sort(&mut self, node: &logical_plan::Sort) -> Result<LogicalSort> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let expr_list = self.into_optd_expr_list(&node.expr)?;
        Ok(LogicalSort::new(input, expr_list))
    }

    fn into_optd_agg(&mut self, node: &logical_plan::Aggregate) -> Result<LogicalAgg> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let agg_exprs = self.into_optd_expr_list(&node.aggr_expr)?;
        let group_exprs = self.into_optd_expr_list(&node.group_expr)?;
        Ok(LogicalAgg::new(input, agg_exprs, group_exprs))
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
        // TODO: on condition

        Ok(LogicalJoin::new(
            left,
            right,
            ConstantExpr::bool(true).into_expr(),
            join_type,
        ))
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
