use std::sync::Arc;

use async_trait::async_trait;
use optd_core::{
    operator::relational::{
        logical::LogicalOperator,
        physical::{
            filter::filter::Filter, join::nested_loop_join::NestedLoopJoin, project::project::Project, scan::table_scan::TableScan, PhysicalOperator
        },
    },
    plan::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan},
};

use datafusion::{
    execution::{context::QueryPlanner, SessionState},
    logical_expr::LogicalPlan as DatafusionLogicalPlan,
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};

use crate::converter::ConversionContext;

pub struct OptdOptimizer {}

impl OptdOptimizer {
    pub fn mock_optimize(&self, logical_plan: LogicalPlan) -> PhysicalPlan {
        let node = match &*logical_plan.node {
            LogicalOperator::Scan(scan) => Arc::new(PhysicalOperator::TableScan(TableScan {
                table_name: scan.table_name.clone(),
                predicate: scan.predicate.clone(),
            })),
            LogicalOperator::Filter(filter) => Arc::new(PhysicalOperator::Filter(Filter {
                child: self.mock_optimize(filter.child.clone()),
                predicate: filter.predicate.clone(),
            })),
            LogicalOperator::Project(project) => Arc::new(PhysicalOperator::Project(Project {
                child: self.mock_optimize(project.child.clone()),
                fields: project.fields.clone(),
            })),
            LogicalOperator::Join(join) => Arc::new(PhysicalOperator::NestedLoopJoin(NestedLoopJoin {
                join_type: "NLJ".to_string(),
                outer: self.mock_optimize(join.left.clone()),
                inner: self.mock_optimize(join.right.clone()),
                condition: join.condition.clone(),
            })),
        };
        PhysicalPlan { node: node }
    }
}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<OptdOptimizer>,
}

impl OptdQueryPlanner {
    pub fn new(optimizer: OptdOptimizer) -> Self {
        Self {
            optimizer: Arc::new(optimizer),
        }
    }

    async fn create_physical_plan_inner(
        &self,
        logical_plan: &DatafusionLogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        // Fallback to the datafusion planner for DML/DDL operations. optd cannot handle this.
        if let DatafusionLogicalPlan::Dml(_)
        | DatafusionLogicalPlan::Ddl(_)
        | DatafusionLogicalPlan::EmptyRelation(_) = logical_plan
        {
            let planner = DefaultPhysicalPlanner::default();
            return Ok(planner
                .create_physical_plan(logical_plan, session_state)
                .await?);
        }

        let mut converter = ConversionContext::new(session_state);
        // convert the logical plan to OptD
        let optd_logical_plan = converter.conv_df_to_optd_relational(logical_plan);
        // run the optd optimizer
        let optd_optimized_physical_plan = self.optimizer.mock_optimize(optd_logical_plan);
        // convert the physical plan to OptD
        converter
            .conv_optd_to_df_relational(&optd_optimized_physical_plan)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }
}

impl std::fmt::Debug for OptdQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OptdQueryPlanner")
    }
}

#[async_trait]
impl QueryPlanner for OptdQueryPlanner {
    async fn create_physical_plan(
        &self,
        datafusion_logical_plan: &DatafusionLogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self
            .create_physical_plan_inner(datafusion_logical_plan, session_state)
            .await
            .unwrap())
    }
}
