// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#![allow(clippy::new_without_default)]
use core::panic;
#[allow(deprecated)]
use std::collections::HashMap;
use std::process::id;
use std::sync::{Arc, Mutex};

use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProviderList, Session};
use datafusion::catalog_common::MemoryCatalogProviderList;
use datafusion::common::DFSchema;
use datafusion::datasource::{provider, source_as_provider};
use datafusion::execution::context::{self, QueryPlanner, SessionState};
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::{session_state, SessionStateBuilder};
use datafusion::logical_expr::utils::{conjunction, disjunction, split_binary, split_binary_owned};
use datafusion::logical_expr::{
    expr, Explain, LogicalPlan as DatafusionLogicalPlan, Operator, PlanType, TableSource,
    ToStringifiedPlan,
};
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{log, Expr, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use optd_core::operator::relational::logical::filter::Filter as OptdLogicalFilter;
use optd_core::operator::relational::logical::project::Project as OptdLogicalProjection;
use optd_core::operator::relational::logical::scan::Scan as OptdLogicalScan;
use optd_core::operator::relational::logical::LogicalOperator;
use optd_core::operator::relational::physical::filter::filter::Filter;
use optd_core::operator::relational::physical::project::project::Project;
use optd_core::operator::relational::physical::scan::table_scan::TableScan;
use optd_core::operator::relational::physical::{self, PhysicalOperator};
use optd_core::operator::scalar::add::Add;
use optd_core::operator::scalar::and::And;
use optd_core::operator::scalar::column_ref::ColumnRef;
use optd_core::operator::scalar::constants::Constant;
use optd_core::operator::scalar::ScalarOperator;
use optd_core::plan::logical_plan::LogicalPlan;
use optd_core::plan::physical_plan::PhysicalPlan;
use optd_core::plan::scalar_plan::{self, ScalarPlan};
use planner::OptdOptimizer;
use planner::OptdQueryPlanner;

pub mod planner;
pub mod converter;

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
    let planner = Arc::new(OptdQueryPlanner::new(optimizer));
    // clean up optimizer rules so that we can plug in our own optimizer
    builder = builder.with_optimizer_rules(vec![]);
    builder = builder.with_physical_optimizer_rules(vec![]);

    // use optd-bridge query planner
    builder = builder.with_query_planner(planner);

    let state = builder.build();
    let ctx = SessionContext::new_with_state(state).enable_url_table();
    ctx.refresh_catalogs().await?;
    Ok(ctx)
}
