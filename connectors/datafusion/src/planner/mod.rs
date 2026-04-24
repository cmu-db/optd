mod from_optd;
mod into_optd;
mod utils;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datafusion::{
    catalog::memory::DataSourceExec,
    datasource::{physical_plan::ParquetSource, source_as_provider},
    error::DataFusionError,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{LogicalPlan, PlanType, Statement, StringifiedPlan, TableSource},
    physical_plan::{
        ExecutionPlan, displayable,
        explain::ExplainExec,
        joins::{HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec},
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    sql::{TableReference, unparser::plan_to_sql},
};
use optd_core::{
    cascades::Cascades,
    error::CatalogSnafu,
    ir::{
        IRContext,
        explain::quick_explain,
        rule::RuleSet,
        statistics::{ColumnStatistics, TableStatistics},
    },
    magic::{MagicCardinalityEstimator, MagicCostModel},
    rules::{self, PassExtension, PassManager, PassProfilingExtension},
};
use optd_repository_api::optd_catalog::RepositoryCatalog;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::{OptdExtension, OptdExtensionConfig};

const DEFAULT_ROW_COUNT: usize = 1000;

#[derive(Default)]
pub struct OptdQueryPlanner {
    default: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for OptdQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptdQueryPlanner").finish_non_exhaustive()
    }
}

#[derive(Debug, Snafu)]
pub enum OptdDFConnectorError {
    #[snafu(display("optd internal error: {}", source))]
    OptdError { source: optd_core::error::Error },
    #[snafu(display("DataFusion error: {}", source))]
    DataFusionError { source: DataFusionError },
    #[snafu(whatever, display("{message}"))]
    Whatever {
        /// The error message.
        message: String,
        /// The underlying error.
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}

pub type Result<T> = std::result::Result<T, OptdDFConnectorError>;

pub struct OptdQueryPlannerContext<'a> {
    pub inner: Arc<IRContext>,
    pub session_state: &'a SessionState,
    pub table_reference_to_source: HashMap<TableReference, Arc<dyn TableSource + 'static>>,
    pub df_mark_columns: HashMap<datafusion::common::Column, optd_core::ir::Column>,
    pub optd_mark_columns: HashMap<optd_core::ir::Column, datafusion::common::Column>,
}

impl<'a> OptdQueryPlannerContext<'a> {
    pub fn new(inner: Arc<IRContext>, session_state: &'a SessionState) -> Self {
        Self {
            inner,
            session_state,
            table_reference_to_source: HashMap::new(),
            df_mark_columns: HashMap::new(),
            optd_mark_columns: HashMap::new(),
        }
    }

    pub async fn collect_statistics(&self) -> Result<()> {
        for (table_reference, source) in self.table_reference_to_source.iter() {
            let table_ref = Self::into_optd_table_ref(table_reference);
            let provider = source_as_provider(source).context(DataFusionSnafu)?;
            let exec = provider
                .scan(self.session_state, None, &[], None)
                .await
                .context(DataFusionSnafu)?;

            self.inner
                .catalog
                .set_table_statistics(
                    table_ref,
                    exec.partition_statistics(None)
                        .map(|statistics| {
                            let column_statistics = statistics.column_statistics;

                            let row_count =
                                precision_value_or(statistics.num_rows, DEFAULT_ROW_COUNT);
                            let size_bytes = precision_to_option(&statistics.total_byte_size);

                            TableStatistics {
                                row_count,
                                size_bytes,
                                column_statistics: column_statistics
                                    .iter()
                                    .map(|column_stat| {
                                        let min_value = column_stat
                                            .min_value
                                            .get_value()
                                            .and_then(|v| {
                                                Self::try_into_optd_scalar_value(v.clone()).ok()
                                            })
                                            .and_then(|x| {
                                                x.try_into_nullable_string().ok().flatten()
                                            });
                                        let max_value = column_stat
                                            .max_value
                                            .get_value()
                                            .and_then(|v| {
                                                Self::try_into_optd_scalar_value(v.clone()).ok()
                                            })
                                            .and_then(|x| {
                                                x.try_into_nullable_string().ok().flatten()
                                            });
                                        ColumnStatistics {
                                            // TODO(Aditya): populate with stuff from HLL, digests, etc.
                                            advanced_stats: Vec::new(),
                                            min_value,
                                            max_value,
                                            null_count: precision_to_option(
                                                &column_stat.null_count,
                                            ),
                                            distinct_count: precision_to_option(
                                                &column_stat.distinct_count,
                                            ),
                                        }
                                    })
                                    .enumerate()
                                    .collect(),
                            }
                        })
                        .unwrap_or_else(|_| TableStatistics {
                            row_count: DEFAULT_ROW_COUNT,
                            size_bytes: None,
                            // TODO(Aditya): add some default column stats?
                            column_statistics: HashMap::new(),
                        }),
                )
                .context(CatalogSnafu)
                .context(OptdSnafu)?
        }
        Ok(())
    }
}

/// Extract value from Precision, returning default if Absent.
fn precision_value_or<T: Copy + PartialOrd + Eq + std::fmt::Debug>(
    precision: datafusion::common::stats::Precision<T>,
    default: T,
) -> T {
    match precision {
        datafusion::common::stats::Precision::Exact(v) => v,
        datafusion::common::stats::Precision::Inexact(v) => v,
        datafusion::common::stats::Precision::Absent => default,
    }
}

/// Extract value from Precision as Option.
fn precision_to_option<T: Copy + PartialOrd + Eq + std::fmt::Debug>(
    precision: &datafusion::common::stats::Precision<T>,
) -> Option<T> {
    match precision {
        datafusion::common::stats::Precision::Exact(v) => Some(*v),
        datafusion::common::stats::Precision::Inexact(v) => Some(*v),
        datafusion::common::stats::Precision::Absent => None,
    }
}

fn warm_explain_properties(op: &Arc<optd_core::ir::Operator>, ctx: &IRContext) {
    for input in op.input_operators() {
        warm_explain_properties(input, ctx);
    }
    let _ = op.output_columns(ctx);
    let _ = op.cardinality(ctx);
}

/// One rendered optd logical-plan snapshot captured after a named pass.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PassExplainSnapshot {
    /// Stable pass name used to label the `EXPLAIN` section.
    pass_name: &'static str,
    /// Fully rendered optd plan after the pass completed.
    rendered_plan: String,
}

/// Pass extension that records post-pass plans for `EXPLAIN` output.
#[derive(Default)]
struct ExplainPassExtension {
    /// Captured snapshots in pass-execution order.
    snapshots: Mutex<Vec<PassExplainSnapshot>>,
}

impl ExplainPassExtension {
    /// Returns a copy of the collected pass snapshots.
    fn snapshots(&self) -> Vec<PassExplainSnapshot> {
        self.snapshots.lock().unwrap().clone()
    }
}

impl PassExtension for ExplainPassExtension {
    fn after_pass(
        &self,
        pass_name: &'static str,
        _before: &Arc<optd_core::ir::Operator>,
        after: &Arc<optd_core::ir::Operator>,
        ctx: &IRContext,
    ) -> optd_core::error::Result<()> {
        warm_explain_properties(after, ctx);
        self.snapshots.lock().unwrap().push(PassExplainSnapshot {
            pass_name,
            rendered_plan: quick_explain(after, ctx),
        });
        Ok(())
    }
}

/// Converts captured pass snapshots into `EXPLAIN` rows.
///
/// If a pass leaves the rendered plan unchanged, its section is collapsed to
/// `same as above` instead of repeating the full tree.
fn pass_explain_plans(
    initial_plan: &str,
    snapshots: &[PassExplainSnapshot],
) -> Vec<StringifiedPlan> {
    let mut previous_rendered = initial_plan.to_owned();
    let mut plans = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let display_plan = if snapshot.rendered_plan == previous_rendered {
            "same as above".to_string()
        } else {
            previous_rendered = snapshot.rendered_plan.clone();
            snapshot.rendered_plan.clone()
        };
        plans.push(StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: format!("optd-{}", snapshot.pass_name),
            },
            display_plan,
        ));
    }
    plans
}

impl OptdQueryPlanner {
    /// Builds the pass manager for the current planning request.
    ///
    /// Runtime pass profiling is enabled by `optd.profile_passes`, and
    /// `EXPLAIN` requests may register a snapshot collector for per-pass
    /// logical-plan display.
    fn create_pass_manager(
        session_state: &SessionState,
        explain_pass_extension: Option<Arc<ExplainPassExtension>>,
    ) -> PassManager {
        let profile_passes = session_state
            .config_options()
            .extensions
            .get::<OptdExtensionConfig>()
            .map(|conf| conf.profile_passes)
            .unwrap_or(false);

        let mut builder = PassManager::builder();
        if profile_passes {
            builder = builder.add_extension(PassProfilingExtension::default());
        }
        if let Some(extension) = explain_pass_extension {
            builder = builder.add_extension_arc(extension);
        }
        builder.build()
    }

    fn optd_extension(session_state: &SessionState) -> Result<Arc<OptdExtension>> {
        session_state
            .config()
            .get_extension::<OptdExtension>()
            .whatever_context("missing optd session extension")
    }

    fn create_context(session_state: &SessionState) -> Result<Arc<IRContext>> {
        let extension = Self::optd_extension(session_state)?;
        Ok(Arc::new(IRContext::new(
            extension.catalog(),
            Arc::new(MagicCardinalityEstimator),
            Arc::new(MagicCostModel),
        )))
    }

    async fn log_query_instance(
        session_state: &SessionState,
        actual_logical_plan: &LogicalPlan,
        optd_logical: &Arc<optd_core::ir::Operator>,
        optd_physical: &Arc<optd_core::ir::Operator>,
    ) -> datafusion::common::Result<()> {
        let extension =
            Self::optd_extension(session_state).map_err(|e| DataFusionError::External(e.into()))?;
        let catalog = extension.catalog();
        let Some(repository_catalog) = catalog.as_any().downcast_ref::<RepositoryCatalog>() else {
            return Ok(());
        };

        let sql = plan_to_sql(actual_logical_plan)?.to_string();
        let initial_plan = serde_json::to_value(optd_logical.as_ref())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let final_plan = serde_json::to_value(optd_physical.as_ref())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;

        repository_catalog
            .log_query_instance(sql, Some(initial_plan), Some(final_plan))
            .await
            .map(|_| ())
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }
}

impl OptdQueryPlanner {
    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Statement(Statement::SetVariable(_)) = logical_plan
        {
            // Fallback to the datafusion planner for DML/DDL operations. optd currently do not handle this.
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        }

        let inner =
            Self::create_context(session_state).map_err(|e| DataFusionError::External(e.into()))?;
        let mut ctx = OptdQueryPlannerContext::new(inner, session_state);
        let (actual_logical_plan, mut explain) = match logical_plan {
            LogicalPlan::Explain(explain) => (explain.plan.as_ref(), Some(explain.clone())),
            _ => (logical_plan, None),
        };

        let optd_logical = ctx
            .try_into_optd_plan(actual_logical_plan)
            .map_err(|e| match e {
                OptdDFConnectorError::DataFusionError { source } => source,
                err => DataFusionError::External(err.into()),
            })?;

        ctx.collect_statistics().await.map_err(|e| match e {
            OptdDFConnectorError::DataFusionError { source } => source,
            err => DataFusionError::External(err.into()),
        })?;

        let rule_set = RuleSet::builder()
            .add_rule(rules::LogicalGetAsPhysicalTableScanRule::new())
            .add_rule(rules::LogicalAggregateAsPhysicalHashAggregateRule::new())
            .add_rule(rules::LogicalJoinAsPhysicalHashJoinRule::new())
            .add_rule(rules::LogicalJoinAsPhysicalNestedLoopRule::new())
            .add_rule(rules::LogicalSelectSimplifyRule::new())
            .add_rule(rules::LogicalJoinInnerCommuteRule::new())
            .add_rule(rules::LogicalJoinInnerAssocRule::new())
            .build();

        warm_explain_properties(&optd_logical, &ctx.inner);

        let explain_pass_extension = explain
            .as_ref()
            .map(|_| Arc::new(ExplainPassExtension::default()));
        let pass_manager = Self::create_pass_manager(session_state, explain_pass_extension.clone());
        let opt = Arc::new(Cascades::with_pass_manager(
            ctx.inner.clone(),
            rule_set,
            pass_manager,
        ));

        let optd_physical = match opt.optimize(&optd_logical, Arc::default()).await {
            Ok(plan) => plan,
            Err(e) => {
                {
                    opt.memo.read().await.dump();
                }
                return Err(DataFusionError::External(e.into()));
            }
        };

        let logical_plan = ctx
            .try_from_optd_plan(&optd_physical)
            .map_err(|e| match e {
                OptdDFConnectorError::DataFusionError { source } => source,
                e => DataFusionError::External(e.into()),
            })?;

        let physical_plan = self
            .default
            .create_physical_plan(&logical_plan, session_state)
            .await?;

        Self::log_query_instance(
            session_state,
            actual_logical_plan,
            &optd_logical,
            &optd_physical,
        )
        .await?;

        if let Some(x) = explain.as_mut() {
            // `quick_explain` only displays cached operator properties.
            // Precompute them on the original logical tree so we don't print `?`
            // for `(.output_columns)` and `(.cardinality)` in explain output.
            warm_explain_properties(&optd_logical, &ctx.inner);
            let s = quick_explain(&optd_logical, &ctx.inner);
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: "optd-initial".to_string(),
                },
                s.clone(),
            ));
            if let Some(extension) = explain_pass_extension.as_ref() {
                x.stringified_plans
                    .extend(pass_explain_plans(&s, &extension.snapshots()));
            }
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::FinalLogicalPlan,
                logical_plan.display_indent().to_string(),
            ));
        }

        if let Some(x) = explain.as_mut() {
            let s = quick_explain(&optd_logical, &opt.ctx);

            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-initial".to_string(),
                },
                s.clone(),
            ));
            x.stringified_plans
                .push(StringifiedPlan::new(PlanType::FinalLogicalPlan, s));
        }

        if let Some(x) = explain.as_mut() {
            let s = quick_explain(&optd_physical, &opt.ctx);
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-finalized".to_string(),
                },
                s.clone(),
            ));
            x.stringified_plans
                .push(StringifiedPlan::new(PlanType::FinalPhysicalPlan, s));
        }

        if let Some(x) = explain.as_mut() {
            let config = &session_state.config_options().explain;
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::FinalPhysicalPlan,
                displayable(physical_plan.as_ref())
                    .set_show_statistics(config.show_statistics)
                    .set_show_schema(config.show_schema)
                    .indent(x.verbose)
                    .to_string(),
            ));

            // Show statistics + schema in verbose output even if not
            // explicitly requested
            if x.verbose {
                if !config.show_statistics {
                    x.stringified_plans.push(StringifiedPlan::new(
                        PlanType::FinalPhysicalPlanWithStats,
                        displayable(physical_plan.as_ref())
                            .set_show_statistics(true)
                            .indent(x.verbose)
                            .to_string(),
                    ));
                }
                if !config.show_schema {
                    x.stringified_plans.push(StringifiedPlan::new(
                        PlanType::FinalPhysicalPlanWithSchema,
                        // This will include schema if show_schema is on
                        // and will be set to true if verbose is on
                        displayable(physical_plan.as_ref())
                            .set_show_schema(true)
                            .indent(x.verbose)
                            .to_string(),
                    ));
                }
            }
        }

        Ok(explain
            .map(|x| {
                Arc::new(ExplainExec::new(
                    Arc::clone(x.schema.inner()),
                    x.stringified_plans,
                    x.verbose,
                )) as Arc<dyn ExecutionPlan>
            })
            .unwrap_or(physical_plan))
    }
    // /// optd's actual implementation of [`QueryPlanner::create_physical_plan`].
    // async fn create_physical_plan_inner(
    //     &self,
    //     logical_plan: &LogicalPlan,
    //     session_state: &SessionState,
    // ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    //     if let LogicalPlan::Dml(_)
    //     | LogicalPlan::Ddl(_)
    //     | LogicalPlan::EmptyRelation(_)
    //     | LogicalPlan::Statement(Statement::SetVariable(_)) = logical_plan
    //     {
    //         // Fallback to the datafusion planner for DML/DDL operations. optd currently do not handle this.
    //         return self
    //             .create_physical_plan_default(logical_plan, session_state)
    //             .await;
    //     }

    //     let ctx = IRContext::with_empty_magic();
    //     let (actual_logical_plan, mut explain) = match logical_plan {
    //         LogicalPlan::Explain(explain) => (explain.plan.as_ref(), Some(explain.clone())),
    //         _ => (logical_plan, None),
    //     };

    //     let res = self
    //         .try_into_optd_plan(actual_logical_plan, &ctx, session_state)
    //         .map_err(|e| DataFusionError::External(e.into()));

    //     let Ok(optd_logical) = res else {
    //         return self
    //             .create_physical_plan_default(logical_plan, session_state)
    //             .await;
    //     };

    //     let rule_set = RuleSet::builder()
    //         .add_rule(rules::LogicalGetAsPhysicalTableScanRule::new())
    //         .add_rule(rules::LogicalAggregateAsPhysicalHashAggregateRule::new())
    //         .add_rule(rules::LogicalJoinAsPhysicalHashJoinRule::new())
    //         .add_rule(rules::LogicalJoinAsPhysicalNLJoinRule::new())
    //         .add_rule(rules::LogicalProjectAsPhysicalProjectRule::new())
    //         .add_rule(rules::LogicalSelectAsPhysicalFilterRule::new())
    //         .add_rule(rules::LogicalSelectSimplifyRule::new())
    //         .add_rule(rules::LogicalJoinInnerCommuteRule::new())
    //         .add_rule(rules::LogicalJoinInnerAssocRule::new())
    //         .build();
    //     let opt = Arc::new(Cascades::new(ctx, rule_set));
    //     let Some(optd_physical) = opt.optimize(&optd_logical, Arc::default()).await else {
    //         {
    //             opt.memo.read().await.dump();
    //         }
    //         warn!("optimization failed");
    //         return self
    //             .create_physical_plan_default(logical_plan, session_state)
    //             .await;
    //     };

    //     {
    //         opt.memo.read().await.dump();
    //     }
    //     info!("got a plan:\n{}", quick_explain(&optd_physical, &opt.ctx));

    //     let physical_plan = self
    //         .try_from_optd_physical_plan(&optd_physical, &opt.ctx, session_state)
    //         .await?;

    //     info!("Converted into df");

    //     if let Some(x) = explain.as_mut() {
    //         let s = quick_explain(&optd_logical, &opt.ctx);
    //         x.stringified_plans.push(StringifiedPlan::new(
    //             PlanType::OptimizedLogicalPlan {
    //                 optimizer_name: "optd-initial".to_string(),
    //             },
    //             s.clone(),
    //         ));
    //         x.stringified_plans
    //             .push(StringifiedPlan::new(PlanType::FinalLogicalPlan, s));
    //     }

    //     if let Some(x) = explain.as_mut() {
    //         let s = quick_explain(&optd_physical, &opt.ctx);
    //         x.stringified_plans.push(StringifiedPlan::new(
    //             PlanType::OptimizedPhysicalPlan {
    //                 optimizer_name: "optd-finalized".to_string(),
    //             },
    //             s.clone(),
    //         ));
    //         x.stringified_plans
    //             .push(StringifiedPlan::new(PlanType::FinalPhysicalPlan, s));
    //     }

    //     if let Some(x) = explain.as_mut() {
    //         let config = &session_state.config_options().explain;
    //         x.stringified_plans.push(StringifiedPlan::new(
    //             PlanType::FinalPhysicalPlan,
    //             displayable(physical_plan.as_ref())
    //                 .set_show_statistics(config.show_statistics)
    //                 .set_show_schema(config.show_schema)
    //                 .indent(x.verbose)
    //                 .to_string(),
    //         ));

    //         // Show statistics + schema in verbose output even if not
    //         // explicitly requested
    //         if x.verbose {
    //             if !config.show_statistics {
    //                 x.stringified_plans.push(StringifiedPlan::new(
    //                     PlanType::FinalPhysicalPlanWithStats,
    //                     displayable(physical_plan.as_ref())
    //                         .set_show_statistics(true)
    //                         .indent(x.verbose)
    //                         .to_string(),
    //                 ));
    //             }
    //             if !config.show_schema {
    //                 x.stringified_plans.push(StringifiedPlan::new(
    //                     PlanType::FinalPhysicalPlanWithSchema,
    //                     // This will include schema if show_schema is on
    //                     // and will be set to true if verbose is on
    //                     displayable(physical_plan.as_ref())
    //                         .set_show_schema(true)
    //                         .indent(x.verbose)
    //                         .to_string(),
    //                 ));
    //             }
    //         }
    //     }

    //     Ok(explain
    //         .map(|x| {
    //             Arc::new(ExplainExec::new(
    //                 Arc::clone(x.schema.inner()),
    //                 x.stringified_plans,
    //                 x.verbose,
    //             )) as Arc<dyn ExecutionPlan>
    //         })
    //         .unwrap_or(physical_plan))
    // }

    async fn create_physical_plan_default(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        return self
            .default
            .create_physical_plan(logical_plan, session_state)
            .await;
    }
}

impl QueryPlanner for OptdQueryPlanner {
    fn create_physical_plan<'a, 'b, 'c, 'ret>(
        &'a self,
        logical_plan: &'b LogicalPlan,
        session_state: &'c SessionState,
    ) -> ::core::pin::Pin<
        Box<dyn Future<Output = datafusion::common::Result<Arc<dyn ExecutionPlan>>> + Send + 'ret>,
    >
    where
        'a: 'ret,
        'b: 'ret,
        'c: 'ret,
        Self: 'ret,
    {
        Box::pin(async move {
            let (optd_enabled, optd_strict_mode, optd_only) = {
                session_state
                    .config_options()
                    .extensions
                    .get::<OptdExtensionConfig>()
                    .map(|conf| (conf.optd_enabled, conf.optd_strict_mode, conf.optd_only))
                    .unwrap_or((true, false, false))
            };

            if optd_only && !optd_enabled {
                return Err(DataFusionError::Plan(
                    "optd.optd_only requires optd.optd_enabled = true".to_string(),
                ));
            }

            if !optd_enabled {
                return self
                    .create_physical_plan_default(logical_plan, session_state)
                    .await;
            }

            let res = self
                .create_physical_plan_inner(logical_plan, session_state)
                .await;

            match res {
                Err(e) => {
                    if optd_strict_mode || optd_only {
                        Err(e)
                    } else {
                        eprintln!(
                            "optd planner does not support this query yet, fallback to default planner:\n{e}"
                        );
                        self.create_physical_plan_default(logical_plan, session_state)
                            .await
                    }
                }
                Ok(plan) => Ok(plan),
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    MergeJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
    Other(Box<Self>),
}

#[allow(dead_code)]
impl JoinOrder {
    fn write_graphviz(&self, graphviz: &mut String) {
        graphviz.push_str("digraph G {\n");
        graphviz.push_str("\trankdir = BT\n");

        let mut counter = 0;
        self.visit(graphviz, &mut counter);
        graphviz.push('}');
    }

    fn add_base_table(label: String, graphviz: &mut String, counter: &mut usize) -> usize {
        let id = *counter;
        *counter += 1;
        graphviz.push_str(&format!("\tnode{id} [label=\"{label}\"]\n"));
        id
    }

    fn add_edge(from: usize, to: usize, graphviz: &mut String) {
        graphviz.push_str(&format!("\tnode{from} -> node{to}\n"));
    }

    fn add_join(
        join_method: &str,
        _joined_tables: &Vec<&str>,
        left_id: usize,
        right_id: usize,
        graphviz: &mut String,
        counter: &mut usize,
    ) -> usize {
        let id = *counter;
        *counter += 1;
        // let label = format!(
        //     "{join_method} (joined=[{}])",
        //     joined_tables.iter().join(",")
        // );

        graphviz.push_str(&format!("\tnode{id} [label=\"{join_method}\"]\n"));
        Self::add_edge(left_id, id, graphviz);
        Self::add_edge(right_id, id, graphviz);
        id
    }

    fn visit(&self, graphviz: &mut String, counter: &mut usize) -> (usize, Vec<&str>) {
        match self {
            JoinOrder::Table(name) => {
                let id = Self::add_base_table(name.clone(), graphviz, counter);
                (id, vec![name])
            }
            JoinOrder::HashJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id = Self::add_join("HJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::MergeJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id = Self::add_join("MJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id =
                    Self::add_join("NLJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::Other(child) => child.visit(graphviz, counter),
        }
    }
}

impl std::fmt::Display for JoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOrder::Table(name) => write!(f, "{}", name),
            JoinOrder::HashJoin(left, right) => {
                write!(f, "(HJ {} {})", left, right)
            }
            JoinOrder::MergeJoin(left, right) => {
                write!(f, "(MJ {} {})", left, right)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                write!(f, "(NLJ {} {})", left, right)
            }
            JoinOrder::Other(child) => {
                write!(f, "<{}>", child)
            }
        }
    }
}

#[allow(dead_code)]
fn get_join_order_from_df_exec(rel_node: &Arc<dyn ExecutionPlan>) -> Option<JoinOrder> {
    if let Some(x) = rel_node.as_any().downcast_ref::<DataSourceExec>() {
        let (config, _) = x.downcast_to_file_source::<ParquetSource>()?;
        let location = config.file_groups[0].files()[0]
            .object_meta
            .location
            .to_string();
        let maybe_table_name = location.split('/').rev().nth(1)?;
        return Some(JoinOrder::Table(maybe_table_name.to_string()));
    }
    if let Some(x) = rel_node.as_any().downcast_ref::<HashJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::HashJoin(Box::new(left), Box::new(right)));
    }

    if let Some(x) = rel_node.as_any().downcast_ref::<SortMergeJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::MergeJoin(Box::new(left), Box::new(right)));
    }

    if let Some(x) = rel_node.as_any().downcast_ref::<NestedLoopJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::NestedLoopJoin(Box::new(left), Box::new(right)));
    }

    if rel_node.children().len() == 1 {
        let child = get_join_order_from_df_exec(rel_node.children()[0])?;
        if matches!(child, JoinOrder::Other(_) | JoinOrder::Table(_)) {
            return Some(child);
        }
        return Some(JoinOrder::Other(Box::new(child)));
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{execution::runtime_env::RuntimeEnv, prelude::SessionConfig};
    use tokio::runtime::Runtime;

    use super::{OptdQueryPlanner, PassExplainSnapshot, pass_explain_plans};
    use crate::create_optd_session_context;

    #[test]
    fn pass_explain_plans_collapses_unchanged_plans() {
        let plans = pass_explain_plans(
            "initial",
            &[
                PassExplainSnapshot {
                    pass_name: "decorrelation",
                    rendered_plan: "initial".to_string(),
                },
                PassExplainSnapshot {
                    pass_name: "simplification",
                    rendered_plan: "simplified".to_string(),
                },
                PassExplainSnapshot {
                    pass_name: "pruning",
                    rendered_plan: "simplified".to_string(),
                },
            ],
        );

        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].plan.as_ref().as_str(), "same as above");
        assert_eq!(plans[1].plan.as_ref().as_str(), "simplified");
        assert_eq!(plans[2].plan.as_ref().as_str(), "same as above");
    }

    #[test]
    fn create_pass_manager_respects_profile_passes_config() {
        let disabled =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let disabled_state = disabled.state();
        let disabled_manager = OptdQueryPlanner::create_pass_manager(&disabled_state, None);
        assert_eq!(disabled_manager.extension_count(), 0);

        let enabled =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        Runtime::new().unwrap().block_on(async {
            enabled
                .sql("set optd.profile_passes = true")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
        });
        let enabled_state = enabled.state();
        let enabled_manager = OptdQueryPlanner::create_pass_manager(&enabled_state, None);
        assert_eq!(enabled_manager.extension_count(), 1);
    }
}
