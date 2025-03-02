use crate::converter::OptdContext;
use async_trait::async_trait;
use datafusion::{
    common::Result as DataFusionResult,
    execution::{context::QueryPlanner, SessionState},
    logical_expr::{
        Explain, LogicalPlan as DataFusionLogicalPlan, PlanType as DataFusionPlanType,
        ToStringifiedPlan,
    },
    physical_plan::{displayable, explain::ExplainExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use optd_core::{
    cascades,
    plans::{logical::LogicalPlan, physical::PhysicalPlan},
    storage::memo::SqliteMemo,
};
use std::sync::Arc;

/// A mock `optd` optimizer.
#[derive(Debug)]
pub(crate) struct MockOptdOptimizer {
    /// The memo table used for dynamic programming during query optimization.
    memo: SqliteMemo,
}

impl MockOptdOptimizer {
    /// Creates a new `optd` optimizer with an in-memory memo table.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Ok(Self {
            memo: SqliteMemo::new_in_memory().await?,
        })
    }

    /// A mock optimization function for testing purposes.
    ///
    /// This function takes a [`LogicalPlan`], and for each node in the [`LogicalPlan`], it will
    /// recursively traverse the node and its children and replace the node with a physical
    /// operator. The physical operator is chosen based on the type of the logical operator.
    ///
    /// For example, if the logical operator is a scan, the physical operator will be a `TableScan`.
    /// If the logical operator is a filter, the physical operator will be a `Filter`, and so on.
    ///
    /// The physical operators are chosen in a way that they mirror the structure of the logical
    /// plan, but they are not actually optimized in any way. This is useful for testing purposes,
    /// as it allows us to test the structure of the physical plan without having to worry about the
    /// actual optimization process.
    ///
    /// This function returns a [`PhysicalPlan`], which is an `optd` struct that contains the root
    /// node of the physical plan.
    pub async fn mock_optimize(
        &self,
        logical_plan: &LogicalPlan,
    ) -> anyhow::Result<Arc<PhysicalPlan>> {
        let root_group_id = cascades::ingest_full_logical_plan(&self.memo, logical_plan).await?;
        let goal_id = cascades::mock_optimize_relation_group(&self.memo, root_group_id).await?;
        let optimized_plan = cascades::match_any_physical_plan(&self.memo, goal_id).await?;

        Ok(optimized_plan)
    }
}

#[async_trait]
impl QueryPlanner for MockOptdOptimizer {
    /// This function is the entry point for the physical planner. It will attempt to optimize the
    /// given DataFusion [`DataFusionLogicalPlan`] using the `optd` optimizer.
    ///
    /// If the [`DataFusionLogicalPlan`] is a DML/DDL operation, it will fall back to the DataFusion planner.
    ///
    /// Otherwise, this function will convert the DataFusion [`DataFusionLogicalPlan`] into an
    /// `optd` [`LogicalPlan`] in order to pass it to the `optd` optimizer.
    ///
    /// Once `optd` has finished optimization, it will convert the output `optd` [`PhysicalPlan`]
    /// into a physical plan that can be executed by DataFusion ([`ExecutionPlan`]).
    ///
    /// # Arguments
    /// * `logical_plan` - The logical plan in DataFusion's type system to optimize.
    /// * `session_state` - The session state to use for creating the physical plan.
    ///
    ///
    /// # Returns
    /// * `anyhow::Result<Arc<dyn ExecutionPlan>>` - The physical plan that can be executed by
    ///   DataFusion.
    async fn create_physical_plan(
        &self,
        datafusion_logical_plan: &DataFusionLogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Fallback to the default DataFusion planner for DML/DDL operations.
        if let DataFusionLogicalPlan::Dml(_)
        | DataFusionLogicalPlan::Ddl(_)
        | DataFusionLogicalPlan::EmptyRelation(_) = datafusion_logical_plan
        {
            return DefaultPhysicalPlanner::default()
                .create_physical_plan(datafusion_logical_plan, session_state)
                .await;
        }

        let (datafusion_logical_plan, _verbose, mut explains) = match datafusion_logical_plan {
            DataFusionLogicalPlan::Explain(Explain { plan, verbose, .. }) => {
                (plan.as_ref(), *verbose, Some(Vec::new()))
            }
            _ => (datafusion_logical_plan, false, None),
        };

        if let Some(explains) = &mut explains {
            explains.push(datafusion_logical_plan.to_stringified(
                DataFusionPlanType::OptimizedLogicalPlan {
                    optimizer_name: "datafusion".to_string(),
                },
            ));
        }

        let mut optd_ctx = OptdContext::new(session_state);

        // convert the DataFusion logical plan to `optd`'s version of a `LogicalPlan`.
        let logical_plan = optd_ctx
            .df_to_optd_relational(datafusion_logical_plan)
            .expect("TODO FIX ERROR HANDLING");

        // Run the `optd` optimizer on the `LogicalPlan`.
        let optd_optimized_physical_plan = self
            .mock_optimize(&logical_plan)
            .await
            .expect("TODO FIX ERROR HANDLING");

        // Convert the output `optd` `PhysicalPlan` to DataFusion's `ExecutionPlan`.
        let physical_plan = optd_ctx
            .optd_to_df_relational(&optd_optimized_physical_plan)
            .await
            .expect("TODO FIX ERROR HANDLING");

        if let Some(mut explains) = explains {
            explains.push(
                displayable(&*physical_plan)
                    .to_stringified(false, DataFusionPlanType::FinalPhysicalPlan),
            );

            return Ok(Arc::new(ExplainExec::new(
                DataFusionLogicalPlan::explain_schema(),
                explains,
                true,
            )));
        }

        Ok(physical_plan)
    }
}
