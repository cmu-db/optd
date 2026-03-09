mod from_optd;
mod into_optd;
mod utils;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use datafusion::{
    catalog::memory::DataSourceExec,
    common::{
        DFSchema,
        tree_node::{TransformedResult, TreeNode},
    },
    datasource::{physical_plan::ParquetSource, source_as_provider},
    error::DataFusionError,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{
        self, ExprSchemable, LogicalPlan, PlanType, Statement, StringifiedPlan, TableScan,
        TableSource, logical_plan,
    },
    physical_expr::{LexOrdering, aggregate::AggregateExprBuilder, create_physical_sort_expr},
    physical_plan::{
        ExecutionPlan,
        aggregates::PhysicalGroupBy,
        displayable,
        explain::ExplainExec,
        filter::FilterExec,
        joins::{
            HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec, utils::JoinFilter,
        },
        projection::ProjectionExec,
        sorts::sort::SortExec,
        udaf::AggregateFunctionExpr,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    sql::TableReference,
};
use itertools::{Either, Itertools};
use optd_core::{
    cascades::Cascades,
    connector_err,
    error::Result as OptdResult,
    ir::{
        Column, IRContext, Scalar,
        builder::{self as optd_builder, column_assign, column_ref, literal},
        catalog::{DataSourceId, Field, Schema},
        convert::{IntoOperator, IntoScalar},
        explain::quick_explain,
        operator::{
            EnforcerSort, LogicalOrderBy, LogicalRemap, PhysicalFilter, PhysicalHashAggregate,
            PhysicalHashJoin, PhysicalNLJoin, PhysicalProject, PhysicalTableScan, join,
        },
        properties::TupleOrderingDirection,
        rule::RuleSet,
        scalar::{
            BinaryOp, Cast, ColumnAssign, ColumnRef, Function, FunctionKind, Like, List, NaryOp,
            NaryOpKind,
        },
        statistics::{ColumnStatistics, TableStatistics},
        table_ref::TableRef,
    },
    rules,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{info, warn};

use crate::{
    OptdExtensionConfig,
    value::{from_optd_value, try_into_optd_value},
};

const DEFAULT_ROW_COUNT: usize = 1000;

#[derive(Default)]
pub struct OptdQueryPlanner {
    default: DefaultPhysicalPlanner,
    table_reference_to_source: Mutex<HashMap<TableReference, Arc<dyn TableSource + 'static>>>,
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

pub type OptdDFConnectorResult<T> = std::result::Result<T, OptdDFConnectorError>;

pub struct OptdQueryPlannerContext<'a> {
    pub inner: IRContext,
    pub session_state: &'a SessionState,
    pub table_reference_to_source: HashMap<TableReference, Arc<dyn TableSource + 'static>>,
}

impl<'a> OptdQueryPlannerContext<'a> {
    pub fn new(inner: IRContext, session_state: &'a SessionState) -> Self {
        Self {
            inner,
            session_state,
            table_reference_to_source: HashMap::new(),
        }
    }
}

fn tuple_err<T, R>(
    value: (datafusion::common::Result<T>, datafusion::common::Result<R>),
) -> datafusion::common::Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
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

/// Extract value from Precision as Option<String>.
/// TODO(Aditya): this should not be required after we move from `String` to `Value`.
fn precision_to_string<T: ToString + PartialOrd + Eq + Clone + std::fmt::Debug>(
    precision: &datafusion::common::stats::Precision<T>,
) -> Option<String> {
    match precision {
        datafusion::common::stats::Precision::Exact(v) => Some(v.to_string()),
        datafusion::common::stats::Precision::Inexact(v) => Some(v.to_string()),
        datafusion::common::stats::Precision::Absent => None,
    }
}

impl OptdQueryPlanner {
    pub async fn collect_statistics(
        tables: &[(DataSourceId, i64, TableScan)],
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<()> {
        for (source, table_index, node) in tables {
            let provider = source_as_provider(&node.source).unwrap();
            let exec = provider
                .scan(session_state, node.projection.as_ref(), &[], None)
                .await
                .unwrap();

            ctx.catalog.set_table_stats(
                *source,
                exec.partition_statistics(None)
                    .map(|statistics| {
                        let column_statistics = statistics.column_statistics;

                        let row_count = precision_value_or(statistics.num_rows, DEFAULT_ROW_COUNT);
                        let size_bytes = precision_to_option(&statistics.total_byte_size);

                        TableStatistics {
                            row_count,
                            size_bytes,
                            column_statistics: column_statistics
                                .iter()
                                .enumerate()
                                .map(|(index, column_stat)| {
                                    let column = Column(*table_index, index);
                                    let column_meta = ctx.get_column_meta(&column);

                                    ColumnStatistics {
                                        column_id: column.0 as i64,
                                        column_type: format!("{:?}", column_meta.data_type),
                                        name: column_meta.name.clone(),
                                        // TODO(Aditya): populate with stuff from HLL, digests, etc.
                                        advanced_stats: Vec::new(),
                                        min_value: precision_to_string(&column_stat.min_value),
                                        max_value: precision_to_string(&column_stat.max_value),
                                        null_count: precision_to_option(&column_stat.null_count),
                                        distinct_count: precision_to_option(
                                            &column_stat.distinct_count,
                                        ),
                                    }
                                })
                                .collect(),
                        }
                    })
                    .unwrap_or_else(|_| TableStatistics {
                        row_count: DEFAULT_ROW_COUNT,
                        size_bytes: None,
                        // TODO(Aditya): add some default column stats?
                        column_statistics: vec![],
                    }),
            );
        }
        Ok(())
    }
}

fn into_optd_schema(df_schema: &DFSchema) -> OptdResult<Arc<optd_core::ir::catalog::Schema>> {
    let x = df_schema
        .columns()
        .iter()
        .zip(df_schema.as_arrow().fields())
        .map(|(column, f)| {
            Ok(Arc::new(Field::new(
                column.flat_name(),
                f.data_type().clone(),
                f.is_nullable(),
            )))
        })
        .collect::<OptdResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(x)))
}

fn from_optd_schema(
    schema: &optd_core::ir::catalog::Schema,
) -> Result<Arc<DFSchema>, DataFusionError> {
    let mut builder = datafusion::arrow::datatypes::SchemaBuilder::new();
    let qualifiers = schema
        .fields()
        .iter()
        .map(|f| {
            let column = datafusion::prelude::Column::from_qualified_name(f.name().clone());

            builder.push(Arc::new(datafusion::arrow::datatypes::Field::new(
                column.name,
                f.data_type().clone(),
                f.is_nullable(),
            )));
            column.relation
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(builder.finish()),
    )?))
}

// Handle the case where the name of a physical column expression does not match the corresponding physical input fields names.
// Physical column names are derived from the physical schema, whereas physical column expressions are derived from the logical column names.
//
// This is a special case that applies only to column expressions. Logical plans may slightly modify column names by appending a suffix (e.g., using ':'),
// to avoid duplicates—since DFSchemas do not allow duplicate names. For example: `count(Int64(1)):1`.
fn maybe_fix_physical_column_name(
    expr: datafusion::common::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    input_physical_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    use datafusion::common::tree_node::Transformed;
    use datafusion::physical_plan::expressions::Column;
    let Ok(expr) = expr else { return expr };
    expr.transform_down(|node| {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            let idx = column.index();
            let physical_field = input_physical_schema.field(idx);
            let expr_col_name = column.name();
            let physical_name = physical_field.name();

            if expr_col_name != physical_name {
                // handle edge cases where the physical_name contains ':'.
                let colon_count = physical_name.matches(':').count();
                let mut splits = expr_col_name.match_indices(':');
                let split_pos = splits.nth(colon_count);

                if let Some((i, _)) = split_pos {
                    let base_name = &expr_col_name[..i];
                    if base_name == physical_name {
                        let updated_column =
                            datafusion::physical_plan::expressions::Column::new(physical_name, idx);
                        return Ok(datafusion::common::tree_node::Transformed::yes(Arc::new(
                            updated_column,
                        )));
                    }
                }
            }

            // If names already match or fix is not possible, just leave it as it is
            Ok(Transformed::no(node))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .data()
}

impl OptdQueryPlanner {
    async fn create_physical_plan_inner_v2(
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

        let inner = IRContext::with_empty_magic();
        let mut ctx = OptdQueryPlannerContext::new(inner, session_state);
        let (actual_logical_plan, mut explain) = match logical_plan {
            LogicalPlan::Explain(explain) => (explain.plan.as_ref(), Some(explain.clone())),
            _ => (logical_plan, None),
        };

        let res = ctx
            .try_into_optd_plan(actual_logical_plan)
            .map_err(|e| DataFusionError::External(e.into()));

        let Ok(optd_logical) = res else {
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        };

        let optd_physical = optd_logical.clone();

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

        if let Some(x) = explain.as_mut() {
            let s = quick_explain(&optd_logical, &ctx.inner);
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: "optd-initial".to_string(),
                },
                s.clone(),
            ));
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::FinalLogicalPlan,
                logical_plan.display_indent().to_string(),
            ));
        }

        // if let Some(x) = explain.as_mut() {
        //     let s = quick_explain(&optd_physical, &ctx.inner);
        //     x.stringified_plans.push(StringifiedPlan::new(
        //         PlanType::OptimizedPhysicalPlan {
        //             optimizer_name: "optd-finalized".to_string(),
        //         },
        //         s.clone(),
        //     ));
        //     x.stringified_plans
        //         .push(StringifiedPlan::new(PlanType::FinalPhysicalPlan, physical_plan));
        // }

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
    //         .try_into_optd_logical_plan(actual_logical_plan, &ctx, session_state)
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
            let (optd_enabled, optd_strict_mode) = {
                session_state
                    .config_options()
                    .extensions
                    .get::<OptdExtensionConfig>()
                    .map(|conf| (conf.optd_enabled, conf.optd_strict_mode))
                    .unwrap_or((true, false))
            };

            if !optd_enabled {
                return self
                    .create_physical_plan_default(logical_plan, session_state)
                    .await;
            }

            let res = self
                .create_physical_plan_inner_v2(logical_plan, session_state)
                .await;

            match res {
                Err(e) => {
                    if optd_strict_mode {
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
