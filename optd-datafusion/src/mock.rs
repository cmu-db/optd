use crate::df_conversion::context::OptdDFContext;
use async_trait::async_trait;
use datafusion::{
    common::Result as DataFusionResult,
    execution::{context::QueryPlanner, SessionState},
    logical_expr::{
        Explain, LogicalPlan as DFLogicalPlan, PlanType as DFPlanType, ToStringifiedPlan,
    },
    physical_plan::{displayable, explain::ExplainExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use iceberg::io::FileIOBuilder;
use iceberg_catalog_memory::MemoryCatalog;
use optd_core::{
    cascades,
    catalog::OptdCatalog,
    plans::{logical::LogicalPlan, physical::PhysicalPlan},
    storage::memo::SqliteMemo,
};
use std::sync::Arc;

/// A mock `optd` optimizer.
#[derive(Debug)]
pub(crate) struct MockOptdOptimizer {
    /// The memo table used for dynamic programming during query optimization.
    memo: Arc<SqliteMemo>,
    catalog: Arc<OptdCatalog<MemoryCatalog>>,
}

impl MockOptdOptimizer {
    /// Creates a new `optd` optimizer with an in-memory memo table.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        let memo = Arc::new(SqliteMemo::new_in_memory().await?);

        let file_io = FileIOBuilder::new("memory").build()?;
        let memory_catalog = MemoryCatalog::new(file_io, None);
        let catalog = Arc::new(OptdCatalog::new(memo.clone(), memory_catalog));

        Ok(Self { memo, catalog })
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
        let root_group_id =
            cascades::ingest_full_logical_plan(self.memo.as_ref(), logical_plan).await?;
        let goal_id =
            cascades::mock_optimize_relation_group(self.memo.as_ref(), root_group_id).await?;
        let optimized_plan = cascades::match_any_physical_plan(self.memo.as_ref(), goal_id).await?;

        std::hint::black_box(&self.catalog);

        Ok(optimized_plan)
    }
}

#[async_trait]
impl QueryPlanner for MockOptdOptimizer {
    /// This function is the entry point for the physical planner. It will attempt to optimize the
    /// given DataFusion [`DFLogicalPlan`] using the `optd` optimizer.
    ///
    /// If the [`DFLogicalPlan`] is a DML/DDL operation, it will fall back to the DataFusion planner.
    ///
    /// Otherwise, this function will convert the DataFusion [`DFLogicalPlan`] into an
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
        datafusion_logical_plan: &DFLogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Fallback to the default DataFusion planner for DML/DDL operations.
        if let DFLogicalPlan::Dml(_) | DFLogicalPlan::Ddl(_) | DFLogicalPlan::EmptyRelation(_) =
            datafusion_logical_plan
        {
            return DefaultPhysicalPlanner::default()
                .create_physical_plan(datafusion_logical_plan, session_state)
                .await;
        }

        let (datafusion_logical_plan, _verbose, mut explains) = match datafusion_logical_plan {
            DFLogicalPlan::Explain(Explain { plan, verbose, .. }) => {
                (plan.as_ref(), *verbose, Some(Vec::new()))
            }
            _ => (datafusion_logical_plan, false, None),
        };

        if let Some(explains) = &mut explains {
            explains.push(datafusion_logical_plan.to_stringified(
                DFPlanType::OptimizedLogicalPlan {
                    optimizer_name: "datafusion".to_string(),
                },
            ));
        }

        let mut optd_ctx = OptdDFContext::new(session_state);

        // convert the DataFusion logical plan to `optd`'s version of a `LogicalPlan`.
        let logical_plan = optd_ctx
            .df_to_optd_relational(datafusion_logical_plan)
            .expect("TODO FIX ERROR HANDLING");

        // The DataFusion to `optd` conversion will have read in all of the tables necessary to
        // execute the query. Now we can update our own catalog with any new tables.
        crate::iceberg_conversion::ingest_providers(
            self.catalog.as_ref().catalog(),
            &optd_ctx.providers,
        )
        .await
        .expect("Unable to ingest providers");

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
                displayable(&*physical_plan).to_stringified(false, DFPlanType::FinalPhysicalPlan),
            );

            return Ok(Arc::new(ExplainExec::new(
                DFLogicalPlan::explain_schema(),
                explains,
                true,
            )));
        }

        Ok(physical_plan)
    }
}
