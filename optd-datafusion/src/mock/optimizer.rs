use super::memo::MockMemo;
use crate::{NAMESPACE, df_conversion::context::OptdDFContext};
use anyhow::Result;
use async_trait::async_trait;
use datafusion::{
    common::Result as DataFusionResult,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{
        Explain, LogicalPlan as DFLogicalPlan, PlanType as DFPlanType, ToStringifiedPlan,
    },
    physical_plan::{ExecutionPlan, displayable, explain::ExplainExec},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use futures::{
    SinkExt, StreamExt,
    channel::mpsc::{self, Sender},
};
use iceberg::{Catalog, NamespaceIdent, io::FileIOBuilder};
use iceberg_catalog_memory::MemoryCatalog;
use optd_core::optimizer::{OptimizeRequest, Optimizer};
use optd_dsl::analyzer::hir::HIR;
use std::fmt::Debug;
use std::{collections::HashMap, sync::Arc};
use tokio::fs::File;
use tokio::io::AsyncReadExt; // for read_to_end()

const DSL_SOURCE: &str = "example.opt";

/// A mock `optd` optimizer.
pub(crate) struct MockOptimizer<C: Catalog> {
    optimize_requester: Sender<OptimizeRequest>,
    catalog: C,
}

impl<C: Catalog> MockOptimizer<C> {
    pub async fn new(dsl_file: &str, catalog: C) -> Result<Self> {
        let memo = MockMemo::new().await;

        // let file_io = FileIOBuilder::new("memory").build()?;
        // let catalog = MemoryCatalog::new(file_io, Some("datafusion".to_string()));

        // Initialize the default namespace.
        let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()])?;
        catalog
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;

        let mut file = File::open(dsl_file).await?;
        let mut dsl_source = String::new();
        file.read_to_string(&mut dsl_source);

        let hir = HIR::default();

        let optimize_requester = Optimizer::launch(memo, hir);

        Ok(Self {
            optimize_requester,
            catalog,
        })
    }
}

impl<C: Catalog> Debug for MockOptimizer<C> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("Instead of doing this, implement `Debug` on `Optimizer`")
    }
}

#[async_trait]
impl<C: Catalog> QueryPlanner for MockOptimizer<C> {
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
        crate::iceberg_conversion::ingest_providers(&self.catalog, optd_ctx.session_state())
            .await
            .expect("Unable to ingest providers");

        let (response_tx, mut response_rx) = mpsc::channel(0);

        // Send a single request and get a physical plan back.
        self.optimize_requester
            .send(OptimizeRequest {
                plan: logical_plan,
                response_tx,
            })
            .await;

        let optimized_physical_plan = response_rx
            .next()
            .await
            .expect("unable to find a query plan");

        // Convert the output `optd` `PhysicalPlan` to DataFusion's `ExecutionPlan`.
        let physical_plan = optd_ctx
            .optd_to_df_relational(&optimized_physical_plan)
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
