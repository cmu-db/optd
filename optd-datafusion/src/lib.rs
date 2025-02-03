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

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
#[allow(dead_code)]
pub struct ExprId(u64);

struct OptdOptimizer {}

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
            LogicalOperator::Join(join) => todo!(),
        };
        PhysicalPlan { node: node }
    }
}

struct ConversionContext<'a> {
    pub tables: HashMap<String, Arc<dyn TableSource>>,
    pub session_state: &'a SessionState,
}

impl ConversionContext<'_> {
    pub fn new(session_state: &SessionState) -> ConversionContext {
        ConversionContext {
            tables: HashMap::new(),
            session_state,
        }
    }

    pub fn conv_df_to_optd_scalar(&self, df_expr: &Expr, context: &DFSchema) -> ScalarPlan {
        let node = match df_expr {
            Expr::Column(column) => Arc::new(ScalarOperator::<ScalarPlan>::ColumnRef(ColumnRef {
                column_idx: context.index_of_column(column).unwrap(),
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
            Expr::BinaryExpr(binary_expr) => {
                let left = self.conv_df_to_optd_scalar(&binary_expr.left, context);
                let right = self.conv_df_to_optd_scalar(&binary_expr.right, context);
                let op = match binary_expr.op {
                    Operator::Plus => {
                        ScalarOperator::<ScalarPlan>::Add(Add::<ScalarPlan> { left, right })
                    }
                    Operator::And => {
                        ScalarOperator::<ScalarPlan>::And(And::<ScalarPlan> { left, right })
                    }
                    _ => panic!("OptD does not support this scalar binary expression"),
                };
                Arc::new(op)
            }
            _ => panic!("OptD does not support this scalar expression"),
        };

        ScalarPlan { node: node }
    }

    pub fn conv_df_to_optd_relational(
        &mut self,
        df_logical_plan: &DatafusionLogicalPlan,
    ) -> LogicalPlan {
        let node = match df_logical_plan {
            DatafusionLogicalPlan::Filter(df_filter) => {
                let logical_optd_filter = OptdLogicalFilter::<LogicalPlan, ScalarPlan> {
                    child: self.conv_df_to_optd_relational(&df_filter.input),
                    predicate: self
                        .conv_df_to_optd_scalar(&df_filter.predicate, df_filter.input.schema()),
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Filter(logical_optd_filter);
                Arc::new(op)
            }
            DatafusionLogicalPlan::Join(join) => todo!(),
            DatafusionLogicalPlan::TableScan(table_scan) => {
                self.tables.insert(
                    table_scan.table_name.to_quoted_string(),
                    table_scan.source.clone(),
                );
                let combine_filters = conjunction(table_scan.filters.to_vec());
                let logical_optd_scan = OptdLogicalScan::<ScalarPlan> {
                    table_name: table_scan.table_name.to_quoted_string(),
                    predicate: match combine_filters {
                        Some(df_expr) => {
                            let schema = DFSchema::try_from(table_scan.source.schema()).unwrap();
                            Some(self.conv_df_to_optd_scalar(&df_expr, &schema))
                        }
                        None => None,
                    },
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Scan(logical_optd_scan);

                Arc::new(op)
            }
            DatafusionLogicalPlan::Projection(projection) => {
                let input = self.conv_df_to_optd_relational(projection.input.as_ref());
                let mut exprs = Vec::new();
                for expr in &projection.expr {
                    exprs.push(self.conv_df_to_optd_scalar(expr, projection.input.schema()));
                }
                let logical_optd_filter = OptdLogicalProjection::<LogicalPlan, ScalarPlan> {
                    child: input,
                    fields: exprs,
                };
                let op = LogicalOperator::<LogicalPlan, ScalarPlan>::Project(logical_optd_filter);
                Arc::new(op)
            }
            _ => panic!("OptD does not support this operator"),
        };
        LogicalPlan { node: node }
    }

    #[async_recursion]
    async fn conv_optd_to_df_relational(
        &self,
        optimized_plan: &PhysicalPlan,
    ) -> anyhow::Result<Arc<dyn ExecutionPlan>> {
        match &*optimized_plan.node {
            PhysicalOperator::TableScan(table_scan) => {
                let source = self.tables.get(&table_scan.table_name).unwrap();
                let provider = source_as_provider(source)?;
                let filters = if let Some(ref pred) = table_scan.predicate {
                    // split_binary_owned(pred, Operator::And)
                    todo!("Optd does not support filters inside table scan")
                } else {
                    vec![]
                };
                let plan = provider
                    .scan(self.session_state, None, &filters, None)
                    .await?;
                Ok(plan)
            }
            PhysicalOperator::Filter(filter) => {
                let input_exec = self.conv_optd_to_df_relational(&filter.child).await?;
                let physical_expr = self
                    .conv_optd_to_df_scalar(&filter.predicate, &input_exec.schema())
                    .clone();
                Ok(
                    Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                        physical_expr,
                        input_exec,
                    )?) as Arc<dyn ExecutionPlan + 'static>,
                )
            }
            PhysicalOperator::Project(project) => {
                let input_exec = self.conv_optd_to_df_relational(&project.child).await?;
                let physical_exprs = project
                    .fields
                    .to_vec()
                    .into_iter()
                    .map(|field| {
                        self.conv_optd_to_df_scalar(&field, &input_exec.schema())
                            .clone()
                    })
                    .map(|expr| (expr, String::new()))
                    .collect::<Vec<(Arc<dyn PhysicalExpr>, String)>>();

                Ok(
                    Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                        as Arc<dyn ExecutionPlan + 'static>,
                )
            }
            PhysicalOperator::HashJoin(hash_join) => todo!(),
            PhysicalOperator::NestedLoopJoin(nested_loop_join) => todo!(),
            PhysicalOperator::SortMergeJoin(merge_join) => todo!(),
        }
    }

    fn conv_optd_to_df_scalar(
        &self,
        pred: &ScalarPlan,
        context: &SchemaRef,
    ) -> Arc<dyn PhysicalExpr> {
        match &*pred.node {
            ScalarOperator::ColumnRef(column_ref) => {
                let idx = column_ref.column_idx;
                Arc::new(
                    // Datafusion checks if col expr name matches the schema, so we have to supply the name inferred by datafusion,
                    // instead of using out own logical properties
                    Column::new(context.fields()[idx].name(), idx),
                )
            }
            ScalarOperator::Constant(constant) => {
                let value = match constant {
                    Constant::String(value) => ScalarValue::Utf8(Some(value.clone())),
                    Constant::Integer(value) => ScalarValue::Int64(Some(value.clone())),
                    Constant::Float(value) => ScalarValue::Float64(Some(value.clone())),
                    Constant::Boolean(value) => ScalarValue::Boolean(Some(value.clone())),
                };
                Arc::new(Literal::new(value))
            }
            ScalarOperator::And(and) => {
                let left = self.conv_optd_to_df_scalar(&and.left, context);
                let right = self.conv_optd_to_df_scalar(&and.right, context);
                let op = Operator::And;
                Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>
            }
            ScalarOperator::Add(add) => {
                let left = self.conv_optd_to_df_scalar(&add.left, context);
                let right = self.conv_optd_to_df_scalar(&add.right, context);
                let op = Operator::Plus;
                Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>
            }
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

    pub fn new(optimizer: OptdOptimizer) -> Self {
        Self {
            optimizer: Arc::new(optimizer),
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
