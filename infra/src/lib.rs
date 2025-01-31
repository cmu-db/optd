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
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    Explain, LogicalPlan as DatafusionLogicalPlan, PlanType, TableSource, ToStringifiedPlan,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{log, Expr, SessionConfig, SessionContext};
use types::operator::relational::logical::filter::Filter as OptdLogicalFilter;
use types::operator::relational::logical::LogicalOperator;
use types::operator::scalar::column_ref::ColumnRef;
use types::plan::logical_plan::LogicalPlan;
use types::plan::scalar_plan::ScalarPlan;

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
#[allow(dead_code)]
pub struct ExprId(u64);

mod types;

struct OptdOptimizer {}

impl OptdOptimizer {
    // fn conv_logical_to_physical(
    //     logical_node: Arc<LogicalOperator<LogicalLink>>,
    // ) -> Arc<PhysicalOperator<PhysicalLink>> {
    //     match &*logical_node {
    //         LogicalOperator::Scan(logical_scan_operator) => {
    //             Arc::new(PhysicalOperator::TableScan(TableScanOperator {
    //                 table_name: logical_scan_operator.table_name.clone(),
    //                 predicate: None,
    //             }))
    //         }
    //         LogicalOperator::Filter(logical_filter_operator) => {
    //             let LogicalLink::LogicalNode(ref child) = logical_filter_operator.child;
    //             let predicate = logical_filter_operator.predicate.clone();
    //             Arc::new(PhysicalOperator::Filter(PhysicalFilterOperator::<
    //                 PhysicalLink,
    //             > {
    //                 child: PhysicalLink::PhysicalNode(Self::conv_logical_to_physical(
    //                     child.clone(),
    //                 )),
    //                 predicate: predicate,
    //             }))
    //         }
    //         LogicalOperator::Join(logical_join_operator) => {
    //             let LogicalLink::LogicalNode(ref left_join) = logical_join_operator.left;
    //             let LogicalLink::LogicalNode(ref right_join) = logical_join_operator.right;
    //             let condition = logical_join_operator.condition.clone();
    //             Arc::new(PhysicalOperator::HashJoin(
    //                 HashJoinOperator::<PhysicalLink> {
    //                     join_type: (),
    //                     left: PhysicalLink::PhysicalNode(Self::conv_logical_to_physical(
    //                         left_join.clone(),
    //                     )),
    //                     right: PhysicalLink::PhysicalNode(Self::conv_logical_to_physical(
    //                         right_join.clone(),
    //                     )),
    //                     condition: condition,
    //                 },
    //             ))
    //         }
    //     }
    // }
    // pub fn mock_optimize(logical_plan: OptDLogicalPlan) -> PhysicalPlan {
    //     todo!()
    // }
}

pub struct OptdQueryPlanner {
    pub optimizer: Arc<OptdOptimizer>,
}

impl OptdQueryPlanner {
    // fn convert_into_optd_scalar(predicate_expr: Expr) -> Scalar {
    //     // TODO: Implement the conversion logic here
    //     Scalar {}
    // }

    // fn convert_into_optd_logical(plan_node: &LogicalPlan) -> Arc<LogicalOperator<LogicalLink>> {
    //     match plan_node {
    //         LogicalPlan::Filter(filter) => {
    //             Arc::new(LogicalOperator::Filter(LogicalFilterOperator {
    //                 child: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&filter.input)),
    //                 predicate: Self::convert_into_optd_scalar(filter.predicate.clone()),
    //             }))
    //         }

    //         LogicalPlan::Join(join) => Arc::new(LogicalOperator::Join(LogicalJoinOperator {
    //             join_type: (),
    //             left: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&join.left)),
    //             right: LogicalLink::LogicalNode(Self::convert_into_optd_logical(&join.right)),
    //             condition: Arc::new(
    //                 join.on
    //                     .iter()
    //                     .map(|(left, right)| {
    //                         let left_scalar = Self::convert_into_optd_scalar(left.clone());
    //                         let right_scalar = Self::convert_into_optd_scalar(right.clone());
    //                         (left_scalar, right_scalar)
    //                     })
    //                     .collect(),
    //             ),
    //         })),

    //         LogicalPlan::TableScan(table_scan) => {
    //             Arc::new(LogicalOperator::Scan(LogicalScanOperator {
    //                 table_name: table_scan.table_name.to_quoted_string(),
    //                 predicate: None, // TODO fix this: there are multiple predicates in the scan but our IR only accepts one
    //             }))
    //         }
    //         _ => panic!("OptD does not support this type of query yet"),
    //     }
    // }

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
            Expr::Column(column) => todo!(),
            Expr::Literal(scalar_value) => todo!(),
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
            DatafusionLogicalPlan::TableScan(table_scan) => todo!(),
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
