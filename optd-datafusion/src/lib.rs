// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]
use core::panic;
#[allow(deprecated)]
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::CatalogProviderList;
use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::{session_state, SessionStateBuilder};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    Explain, LogicalPlan as DatafusionLogicalPlan, PlanType, TableSource, ToStringifiedPlan,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{log, Expr, SessionConfig, SessionContext};
use optd_core::operator::relational::logical::filter::Filter as OptdLogicalFilter;
use optd_core::operator::relational::logical::scan::Scan as OptdLogicalScan;
use optd_core::operator::relational::logical::LogicalOperator;
use optd_core::operator::scalar::column_ref::ColumnRef;
use optd_core::operator::scalar::constants::Constant;
use optd_core::operator::scalar::ScalarOperator;
use optd_core::plan::logical_plan::LogicalPlan;
use optd_core::plan::scalar_plan::{self, ScalarPlan};

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
#[allow(dead_code)]
pub struct ExprId(u64);

struct OptdOptimizer {
    tables: HashMap<String, Arc<dyn TableSource>>,
    session_state: Option<SessionState>,
}

impl OptdOptimizer {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            session_state: None,
        }
    }
}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<OptdOptimizer>,
}

impl OptdQueryPlanner {
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

        // TODO: convert the logical plan to OptD
        // let mut optd_rel = ctx.conv_into_optd(logical_plan)?;
        let optdLogicalPlan = Self::conv_df_to_optd_relational(logical_plan);
        let mut optimizer = self.optimizer.clone();

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
            optimizer: Arc::new(optimizer),
        }
    }

    fn conv_df_to_optd_scalar(df_expr: &Expr) -> ScalarPlan {
        let node = match df_expr {
            Expr::Column(column) => Arc::new(ScalarOperator::<ScalarPlan>::ColumnRef(ColumnRef {
                column_idx: todo!(),
            })),
            Expr::Literal(scalar_value) => match scalar_value {
                datafusion::scalar::ScalarValue::Boolean(val) => {
                    Arc::new(ScalarOperator::<ScalarPlan>::Constant(Constant::Boolean(
                        val.clone().unwrap(),
                    )))
                }
                datafusion::scalar::ScalarValue::Float64(val) => {
                    Arc::new(ScalarOperator::<ScalarPlan>::Constant(Constant::Float(
                        val.clone().unwrap(),
                    )))
                }
                datafusion::scalar::ScalarValue::Int64(val) => Arc::new(
                    ScalarOperator::<ScalarPlan>::Constant(Constant::Integer(val.clone().unwrap())),
                ),
                datafusion::scalar::ScalarValue::Utf8(val) => Arc::new(
                    ScalarOperator::<ScalarPlan>::Constant(Constant::String(val.clone().unwrap())),
                ),
                _ => panic!("OptD Only supports a limited number of literals"),
            },
            Expr::BinaryExpr(binary_expr) => todo!(),
            _ => panic!("OptD does not support this scalar expression"),
        };

        ScalarPlan { node: node }
    }
    fn conv_df_to_optd_relational(df_logical_plan: &DatafusionLogicalPlan) -> LogicalPlan {
        let node = match df_logical_plan {
            DatafusionLogicalPlan::Filter(df_filter) => {
                let logical_optd_filter = OptdLogicalFilter::<LogicalPlan, ScalarPlan> {
                    child: Self::conv_df_to_optd_relational(&df_filter.input),
                    predicate: Self::conv_df_to_optd_scalar(&df_filter.predicate),
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Filter(logical_optd_filter);
                Arc::new(op)
            }
            DatafusionLogicalPlan::Join(join) => todo!(),
            DatafusionLogicalPlan::TableScan(table_scan) => {
                let combine_filters = conjunction(table_scan.filters.to_vec());
                let logical_optd_scan = OptdLogicalScan::<ScalarPlan> {
                    table_name: table_scan.table_name.to_quoted_string(),
                    predicate: match combine_filters {
                        Some(df_expr) => Some(Self::conv_df_to_optd_scalar(&df_expr)),
                        None => None,
                    },
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Scan(logical_optd_scan);
                Arc::new(op)
            }
            _ => panic!("OptD does not support this operator"),
        };
        LogicalPlan { node: node }
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

    let optimizer = OptdOptimizer::new();
    let optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
    // clean up optimizer rules so that we can plug in our own optimizer
    builder = builder.with_optimizer_rules(vec![]);
    builder = builder.with_physical_optimizer_rules(vec![]);

    // use optd-bridge query planner
    builder = builder.with_query_planner(optimizer.clone());

    let state = builder.build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    Ok(ctx)
}
