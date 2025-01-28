// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]
#[allow(deprecated)]
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::CatalogProviderList;
use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Explain, LogicalPlan, PlanType, TableSource, ToStringifiedPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{log, Expr, SessionConfig, SessionContext};

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
#[allow(dead_code)]
pub struct ExprId(u64);

mod types;
use types::operator::logical::{
    LogicalFilterOperator, LogicalJoinOperator, LogicalOperator, LogicalScanOperator,
};
use types::operator::ScalarOperator;
use types::plan::logical_plan::{LogicalLink, LogicalPlan as OptDLogicalPlan, ScalarLink};

struct OptdOptimizer {}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<Mutex<Option<Box<OptdOptimizer>>>>,
}

impl OptdQueryPlanner {
    fn convert_into_optd_scalar(predicate_expr: Expr) -> Arc<ScalarOperator<ScalarLink>> {
        // TODO: Implement the conversion logic here
        Arc::new(ScalarOperator::new())
    }

    fn convert_into_optd_logical(plan_node: &LogicalPlan) -> Arc<LogicalOperator<LogicalLink>> {
        match plan_node {
            LogicalPlan::Filter(filter) => {
                Arc::new(LogicalOperator::Filter(LogicalFilterOperator {
                    child: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&filter.input)),
                    predicate: LogicalLink::ScalarNode(Self::convert_into_optd_scalar(
                        filter.predicate.clone(),
                    )),
                }))
            }

            LogicalPlan::Join(join) => Arc::new(LogicalOperator::Join(
                (LogicalJoinOperator {
                    join_type: (),
                    left: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&join.left)),
                    right: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&join.right)),
                    condition: LogicalLink::ScalarNode(Arc::new(todo!())),
                }),
            )),

            LogicalPlan::TableScan(table_scan) => Arc::new(LogicalOperator::Scan(
                (LogicalScanOperator {
                    table_name: table_scan.table_name.to_quoted_string(),
                    predicate: None, // TODO fix this: there are multiple predicates in the scan but our IR only accepts one
                }),
            )),
            _ => panic!("OptD does not support this type of query yet"),
        }
    }

    fn get_optd_logical_plan(plan_node: &LogicalPlan) -> OptDLogicalPlan {
        OptDLogicalPlan { root: Self::convert_into_optd_logical(plan_node) }
    }

    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        // Fallback to the datafusion planner for DML/DDL operations. optd cannot handle this.
        if let LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::EmptyRelation(_) =
            logical_plan
        {
            let planner = DefaultPhysicalPlanner::default();
            return Ok(planner
                .create_physical_plan(logical_plan, session_state)
                .await?);
        }

        // TODO: convert the logical plan to OptD
        // let mut optd_rel = ctx.conv_into_optd(logical_plan)?;
        let optdLogicalPlan = Self::get_optd_logical_plan(logical_plan);
        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();

        // For now we are not sending anything to Opt-D
        // instead we are making datafusion create a physical plan for us and return it
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn new(optimizer: OptdOptimizer) -> Self {
        Self {
            optimizer: Arc::new(Mutex::new(Some(Box::new(optimizer)))),
        }
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
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self
            .create_physical_plan_inner(logical_plan, session_state)
            .await
            .unwrap())
    }
}
/// Utility function to create a session context for datafusion + optd.
pub async fn create_df_context(
    session_config: Option<SessionConfig>,
    rn_config: Option<RuntimeConfig>,
    catalog: Option<Arc<dyn CatalogProviderList>>,
) -> anyhow::Result<SessionContext> {
    let mut session_config = if let Some(session_config) = session_config {
        session_config
    } else {
        SessionConfig::from_env()?.with_information_schema(true)
    };

    // Disable Datafusion's heuristic rule based query optimizer
    session_config.options_mut().optimizer.max_passes = 0;

    let rn_config = if let Some(rn_config) = rn_config {
        rn_config
    } else {
        RuntimeConfig::new()
    };
    let runtime_env = Arc::new(rn_config.build()?);

    let catalog = if let Some(catalog) = catalog {
        catalog
    } else {
        Arc::new(MemoryCatalogProviderList::new())
    };

    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(runtime_env)
        .with_catalog_list(catalog.clone())
        .with_default_features();

    let optimizer = OptdOptimizer {};
    // clean up optimizer rules so that we can plug in our own optimizer
    builder = builder.with_optimizer_rules(vec![]);
    builder = builder.with_physical_optimizer_rules(vec![]);

    // use optd-bridge query planner
    let optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
    builder = builder.with_query_planner(optimizer.clone());
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    Ok(ctx)
}
