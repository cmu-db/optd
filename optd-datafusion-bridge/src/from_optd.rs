use std::sync::Arc;

use anyhow::Result;
use async_recursion::async_recursion;
use datafusion::{
    datasource::source_as_provider,
    physical_plan::{projection::ProjectionExec, ExecutionPlan, PhysicalExpr},
};
use optd_datafusion_repr::plan_nodes::{
    Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PhysicalProjection, PhysicalScan, PlanNode,
};

use crate::OptdPlanContext;

impl OptdPlanContext<'_> {
    #[async_recursion]
    async fn from_optd_table_scan(
        &mut self,
        node: PhysicalScan,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let source = self.tables.get(node.table().as_ref()).unwrap();
        let provider = source_as_provider(source)?;
        let plan = provider.scan(self.session_state, None, &[], None).await?;
        Ok(plan)
    }

    fn from_optd_expr(&mut self, expr: Expr) -> Result<(Arc<dyn PhysicalExpr>, String)> {
        unimplemented!()
    }

    #[async_recursion]
    async fn from_optd_projection(
        &mut self,
        node: PhysicalProjection,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.from_optd_plan_node(node.child()).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .map(|expr| self.from_optd_expr(expr))
            .collect::<Result<Vec<_>>>()?;

        Ok(
            Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    async fn from_optd_plan_node(&mut self, node: PlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        match node.typ() {
            OptRelNodeTyp::PhysicalScan => {
                self.from_optd_table_scan(
                    PhysicalScan::from_rel_node(node.into_rel_node()).unwrap(),
                )
                .await
            }
            OptRelNodeTyp::PhysicalProjection => {
                self.from_optd_projection(
                    PhysicalProjection::from_rel_node(node.into_rel_node()).unwrap(),
                )
                .await
            }
            typ => unimplemented!("{}", typ),
        }
    }

    pub async fn from_optd(&mut self, root_rel: OptRelNodeRef) -> Result<Arc<dyn ExecutionPlan>> {
        self.from_optd_plan_node(PlanNode::from_rel_node(root_rel).unwrap())
            .await
    }
}
