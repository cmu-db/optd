use std::sync::Arc;

use anyhow::Ok;
use async_trait::async_trait;
use datafusion::{
    execution::{context::QueryPlanner, SessionState},
    logical_expr::{
        Explain, LogicalPlan as DFLogicalPlan, PlanType as DFPlanType, ToStringifiedPlan,
    },
    physical_plan::{displayable, explain::ExplainExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use optd_core::{
    plans::{logical::LogicalPlan, physical::PhysicalPlan},
    storage::memo::SqliteMemo,
};

use crate::converter::OptdDFContext;

/// A mock optimizer for testing purposes.
#[derive(Debug)]
pub struct OptdOptimizer {
    memo: SqliteMemo,
}

impl OptdOptimizer {
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Ok(Self {
            memo: SqliteMemo::new_in_memory().await?,
        })
    }

    /// A mock optimization function for testing purposes.
    ///
    /// This function takes a logical plan, and for each node in the logical plan, it will
    /// recursively traverse the node and its children and replace the node with a physical
    /// operator. The physical operator is chosen based on the type of the logical operator.
    /// For example, if the logical operator is a scan, the physical operator will be a
    /// TableScan, if the logical operator is a filter, the physical operator will be a
    /// Filter, and so on.
    ///
    /// The physical operators are chosen in a way that they mirror the structure of the
    /// logical plan, but they are not actually optimized in any way. This is useful for
    /// testing purposes, as it allows us to test the structure of the physical plan without
    /// having to worry about the actual optimization process.
    ///
    /// The function returns a PhysicalPlan, which is a struct that contains the root node of
    /// the physical plan.
    ///
    /// # Arguments
    /// * `logical_plan` - The logical plan to optimize.
    ///
    /// # Returns
    /// * `PhysicalPlan` - The optimized physical plan.
    pub async fn mock_optimize(
        &self,
        logical_plan: &LogicalPlan,
    ) -> anyhow::Result<Arc<PhysicalPlan>> {
        let root_group_id =
            optd_core::cascades::ingest_full_logical_plan(&self.memo, logical_plan).await?;
        let goal_id =
            optd_core::cascades::mock_optimize_relation_group(&self.memo, root_group_id).await?;
        let optimized_plan =
            optd_core::cascades::match_any_physical_plan(&self.memo, goal_id).await?;

        Ok(optimized_plan)
    }
}

/// A struct that implements the `QueryPlanner` trait for the `OptdQueryPlanner`.
/// This trait is used to create a physical plan for a given logical plan.
/// The physical plan is created by converting the logical plan to an optd logical plan,
/// and then running the optd optimizer on the logical plan and then converting it back.
/// This is the entry point for optd.
#[derive(Debug)]
pub struct OptdQueryPlanner {
    pub optimizer: Arc<OptdOptimizer>,
}

impl OptdQueryPlanner {
    /// Creates a new instance of `OptdQueryPlanner` with the given optimizer.
    ///
    /// The optimizer is cloned and stored in an `Arc` so that it can be safely shared
    /// across threads.
    ///
    /// # Arguments
    /// * `optimizer` - The optimizer to use for creating the physical plan.
    ///
    /// # Returns
    /// * `OptdQueryPlanner` - A new instance of `OptdQueryPlanner` with the given optimizer.
    pub fn new(optimizer: OptdOptimizer) -> Self {
        Self {
            optimizer: Arc::new(optimizer),
        }
    }

    /// This function is the entry point for the physical planner. It will attempt
    /// to optimize the logical plan using the optd optimizer. If the logical plan
    /// is a DML/DDL operation, it will fall back to the datafusion planner.
    ///
    /// The steps of this function are the following:
    ///
    /// 1. Check if the logical plan is a DML/DDL operation. If it is, fall back
    ///    to the datafusion planner.
    /// 2. Convert the logical plan to an optd logical plan.
    /// 3. Run the optd optimizer on the logical plan.
    /// 4. Convert the physical plan to a physical plan that can be executed by
    ///    datafusion.
    ///
    /// # Arguments
    /// * `logical_plan` - The logical plan in Datafusion's type system to optimize.
    /// * `session_state` - The session state to use for creating the physical plan.
    ///
    ///
    /// # Returns
    /// * `anyhow::Result<Arc<dyn ExecutionPlan>>` - The physical plan that can be executed by
    ///   datafusion.
    async fn create_physical_plan_inner(
        &self,
        logical_plan: &DFLogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        // Fallback to the datafusion planner for DML/DDL operations. optd cannot handle this.
        if let DFLogicalPlan::Dml(_) | DFLogicalPlan::Ddl(_) | DFLogicalPlan::EmptyRelation(_) =
            logical_plan
        {
            let planner = DefaultPhysicalPlanner::default();
            return Ok(planner
                .create_physical_plan(logical_plan, session_state)
                .await?);
        }

        let (logical_plan, _verbose, mut explains) = match logical_plan {
            DFLogicalPlan::Explain(Explain { plan, verbose, .. }) => {
                (plan.as_ref(), *verbose, Some(Vec::new()))
            }
            _ => (logical_plan, false, None),
        };

        if let Some(explains) = &mut explains {
            explains.push(
                logical_plan.to_stringified(DFPlanType::OptimizedLogicalPlan {
                    optimizer_name: "datafusion".to_string(),
                }),
            );
        }

        let mut converter = OptdDFContext::new(session_state);
        // convert the logical plan to optd
        let logical_plan = converter.conv_df_to_optd_relational(logical_plan)?;
        // run the optd optimizer
        let optd_optimized_physical_plan = self.optimizer.mock_optimize(&logical_plan).await?;
        // convert the physical plan to optd
        let physical_plan = converter
            .conv_optd_to_df_relational(&optd_optimized_physical_plan)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        if let Some(explains) = &mut explains {
            explains.push(
                displayable(&*physical_plan).to_stringified(false, DFPlanType::FinalPhysicalPlan),
            );
        }

        if let Some(explains) = explains {
            Ok(Arc::new(ExplainExec::new(
                DFLogicalPlan::explain_schema(),
                explains,
                true,
            )))
        } else {
            Ok(physical_plan)
        }
    }
}

// making it `async_trait` only because datafusion is taking it.
#[async_trait]
impl QueryPlanner for OptdQueryPlanner {
    /// This function is the entry point for the physical planner. It calls the inner function
    /// `create_physical_plan_inner` to optimize the logical plan using the optd optimizer. If the logical plan
    /// is a DML/DDL operation, it will fall back to the datafusion planner.
    ///
    /// The steps of this function are the following:
    ///
    /// 1. Check if the logical plan is a DML/DDL operation. If it is, fall back
    ///    to the datafusion planner.
    /// 2. Convert the logical plan to an optd logical plan.
    /// 3. Run the optd optimizer on the logical plan.
    /// 4. Convert the physical plan to a physical plan that can be executed by
    ///    datafusion.
    ///
    ///
    /// # Arguments
    /// * `datafusion_logical_plan` - The logical plan in Datafusion's type system to optimize.
    /// * `session_state` - The session state to use for creating the physical plan.
    ///
    /// # Returns
    /// * `datafusion::common::Result<Arc<dyn ExecutionPlan>>` - The physical plan that can be executed by
    ///   datafusion.
    ///
    /// Also see [`OptdQueryPlanner::create_physical_plan`]
    async fn create_physical_plan(
        &self,
        datafusion_logical_plan: &DFLogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan_inner(datafusion_logical_plan, session_state)
            .await
            .map_err(|x| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create physical plan: {:?}",
                    x
                ))
            })
    }
}
