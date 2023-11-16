use anyhow::{bail, Result};
use datafusion::logical_expr::{self, logical_plan, LogicalPlan};
use optd_datafusion_repr::plan_nodes::{
    ColumnRefExpr, Expr, ExprList, LogicalProjection, LogicalScan, OptRelNode, OptRelNodeRef,
    PlanNode,
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
                bail!("binary expr")
            }
            _ => bail!("{:?}", expr),
        }
    }

    fn into_optd_projection(
        &mut self,
        node: &logical_plan::Projection,
    ) -> Result<LogicalProjection> {
        let input = self.into_optd_plan_node(node.input.as_ref())?;
        let exprs = node
            .expr
            .iter()
            .map(|expr| self.into_optd_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        let expr_list = ExprList::new(exprs);
        Ok(LogicalProjection::new(input, expr_list))
    }

    fn into_optd_plan_node(&mut self, node: &LogicalPlan) -> Result<PlanNode> {
        let node = match node {
            LogicalPlan::TableScan(node) => self.into_optd_table_scan(node)?.into_plan_node(),
            LogicalPlan::Projection(node) => self.into_optd_projection(node)?.into_plan_node(),
            _ => bail!("{:?}", node),
        };
        Ok(node)
    }

    pub fn into_optd(&mut self, root_rel: &LogicalPlan) -> Result<OptRelNodeRef> {
        Ok(self.into_optd_plan_node(root_rel)?.into_rel_node())
    }
}
