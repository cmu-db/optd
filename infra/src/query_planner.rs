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
use datafusion::prelude::{SessionConfig, SessionContext};

struct OptdOptimizer {}

pub struct OptdPlanContext<'a> {
    tables: HashMap<String, Arc<dyn TableSource>>,
    session_state: &'a SessionState,
    pub optimizer: Option<&'a OptdOptimizer>,
}

impl<'a> OptdPlanContext<'a> {
    pub fn new(session_state: &'a SessionState) -> Self {
        Self {
            tables: HashMap::new(),
            session_state,
            optimizer: None,
        }
    }
}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<Mutex<Option<Box<OptdOptimizer>>>>,
}

impl OptdQueryPlanner {
    pub fn enable_adaptive(&self) {
        // TODO make the adaptive flag configurable
        // self.optimizer
        //     .lock()
        //     .unwrap()
        //     .as_mut()
        //     .unwrap()
        //     .enable_adaptive(true);
    }

    pub fn disable_adaptive(&self) {
        // TODO make the adaptive flag configurable
        // self.optimizer
        //     .lock()
        //     .unwrap()
        //     .as_mut()
        //     .unwrap()
        //     .enable_adaptive(false);
    }

    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::EmptyRelation(_) =
            logical_plan
        {
            // Fallback to the datafusion planner for DML/DDL operations. optd cannot handle this.
            let planner = DefaultPhysicalPlanner::default();
            return Ok(planner
                .create_physical_plan(logical_plan, session_state)
                .await?);
        }
        let (mut explains, verbose, logical_plan) = match logical_plan {
            LogicalPlan::Explain(Explain { plan, verbose, .. }) => {
                (Some(Vec::new()), *verbose, plan.as_ref())
            }
            _ => (None, false, logical_plan),
        };
        let mut ctx = OptdPlanContext::new(session_state);
        if let Some(explains) = &mut explains {
            explains.push(logical_plan.to_stringified(PlanType::OptimizedLogicalPlan {
                optimizer_name: "datafusion".to_string(),
            }));
        }

        // TODO: convert the logical plan to OptD
        // let mut optd_rel = ctx.conv_into_optd(logical_plan)?;
        let mut optimizer = self.optimizer.lock().unwrap().take().unwrap();

        /*
        For now we are not sending anything to Opt-D
         */

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

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
}

impl std::fmt::Display for JoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOrder::Table(name) => write!(f, "{}", name),
            JoinOrder::HashJoin(left, right) => {
                write!(f, "(HashJoin {} {})", left, right)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                write!(f, "(NLJ {} {})", left, right)
            }
        }
    }
}

pub struct OptdDfContext {
    pub ctx: SessionContext,
    pub catalog: Arc<dyn CatalogProviderList>,
    pub optimizer: Arc<OptdQueryPlanner>,
}

/// Utility function to create a session context for datafusion + optd.
pub async fn create_df_context(
    session_config: Option<SessionConfig>,
    rn_config: Option<RuntimeConfig>,
    catalog: Option<Arc<dyn CatalogProviderList>>,
    use_df_logical: bool,
) -> anyhow::Result<OptdDfContext> {
    let mut session_config = if let Some(session_config) = session_config {
        session_config
    } else {
        SessionConfig::from_env()?.with_information_schema(true)
    };

    if !use_df_logical {
        session_config.options_mut().optimizer.max_passes = 0;
    }

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
    if !use_df_logical {
        // clean up optimizer rules so that we can plug in our own optimizer
        builder = builder.with_optimizer_rules(vec![]);
    }
    builder = builder.with_physical_optimizer_rules(vec![]);
    // use optd-bridge query planner
    let optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
    builder = builder.with_query_planner(optimizer.clone());
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    Ok(OptdDfContext {
        ctx,
        catalog,
        optimizer,
    })
}
