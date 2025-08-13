use std::sync::Arc;

use datafusion::{
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{LogicalPlan, logical_plan},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use optd_core::{
    cascades::Cascades,
    ir::{IRContext, rule::RuleSet},
    rules,
};
use tracing::info;

#[derive(Debug)]
pub struct OptdQueryPlanner;

impl OptdQueryPlanner {
    pub fn try_into_optd_logical_plan(
        &self,
        df_logical: &LogicalPlan,
    ) -> Option<Arc<optd_core::ir::Operator>> {
        match df_logical {
            // LogicalPlan::TableScan(table_scan) => todo!(),
            _ => None,
        }
    }

    pub fn try_into_optd_logical_get(
        &self,
        node: &logical_plan::TableScan,
    ) -> Option<Arc<optd_core::ir::Operator>> {
        let table_name = node
            .table_name
            .clone()
            .resolve("datafusion", "public")
            .to_string();
        todo!()
    }

    pub fn try_from_optd_physical_plan(
        &self,
        optd_physical: &optd_core::ir::Operator,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // use optd_core::ir::OperatorKind;
        match &optd_physical.kind {
            _ => None,
        }
    }
}

impl OptdQueryPlanner {
    /// Optd's actual implementation of [`QueryPlanner::create_physical_plan`].
    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let res = self.try_into_optd_logical_plan(logical_plan);

        let Some(optd_logical) = res else {
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        };
        // TODO: Catalog!!!
        let ctx = IRContext::with_empty_magic();

        let rule_set = RuleSet::builder()
            .add_rule(rules::LogicalJoinAsPhysicalNLJoinRule::new())
            .add_rule(rules::LogicalJoinInnerCommuteRule::new())
            .add_rule(rules::LogicalJoinInnerAssocRule::new())
            .build();
        let opt = Arc::new(Cascades::new(ctx, rule_set));
        let Some(optd_physical) = opt.optimize(&optd_logical, Arc::default()).await else {
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        };

        let res = self.try_from_optd_physical_plan(&optd_physical);
        let Some(physical_plan) = res else {
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        };

        Ok(physical_plan)
    }

    async fn create_physical_plan_default(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        info!(node = %logical_plan, "not yet supported in optd, fallback to default planner");
        let planner = DefaultPhysicalPlanner::default();
        return Ok(planner
            .create_physical_plan(logical_plan, session_state)
            .await?);
    }
}

impl QueryPlanner for OptdQueryPlanner {
    fn create_physical_plan<'a, 'b, 'c, 'ret>(
        &'a self,
        logical_plan: &'b LogicalPlan,
        session_state: &'c SessionState,
    ) -> ::core::pin::Pin<
        Box<dyn Future<Output = datafusion::common::Result<Arc<dyn ExecutionPlan>>> + Send + 'ret>,
    >
    where
        'a: 'ret,
        'b: 'ret,
        'c: 'ret,
        Self: 'ret,
    {
        Box::pin(self.create_physical_plan_inner(logical_plan, session_state))
    }
}
