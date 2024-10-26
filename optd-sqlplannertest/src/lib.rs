use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::CatalogList;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_cli::helper::unescape_input;
use itertools::Itertools;
use lazy_static::lazy_static;
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::cost::BaseTableStats;
use optd_datafusion_repr::DatafusionOptimizer;
use regex::Regex;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{Context, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct DatafusionDBMS {
    ctx: SessionContext,
    /// Context enabling datafusion's logical optimizer.
    use_df_logical_ctx: SessionContext,
}

impl DatafusionDBMS {
    pub async fn new() -> Result<Self> {
        let ctx = DatafusionDBMS::new_session_ctx(false, None).await?;
        let use_df_logical_ctx =
            DatafusionDBMS::new_session_ctx(true, Some(ctx.state().catalog_list().clone())).await?;
        Ok(Self {
            ctx,
            use_df_logical_ctx,
        })
    }

    /// Creates a new session context. If the `use_df_logical` flag is set, datafusion's logical optimizer will be used.
    async fn new_session_ctx(
        use_df_logical: bool,
        catalog: Option<Arc<dyn CatalogList>>,
    ) -> Result<SessionContext> {
        let mut session_config = SessionConfig::from_env()?.with_information_schema(true);
        if !use_df_logical {
            session_config.options_mut().optimizer.max_passes = 0;
        }

        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;

        let ctx = {
            let mut state = if let Some(catalog) = catalog {
                SessionState::new_with_config_rt_and_catalog_list(
                    session_config.clone(),
                    Arc::new(runtime_env),
                    catalog,
                )
            } else {
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env))
            };
            let optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
                Arc::new(DatafusionCatalog::new(state.catalog_list())),
                BaseTableStats::default(),
                false,
            );
            if !use_df_logical {
                // clean up optimizer rules so that we can plug in our own optimizer
                state = state.with_optimizer_rules(vec![]);
            }
            state = state.with_physical_optimizer_rules(vec![]);
            // use optd-bridge query planner
            state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(ctx)
    }

    pub async fn execute(&self, sql: &str, use_df_logical: bool) -> Result<Vec<Vec<String>>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let df = if use_df_logical {
                let plan = self
                    .use_df_logical_ctx
                    .state()
                    .statement_to_plan(statement)
                    .await?;
                self.use_df_logical_ctx.execute_logical_plan(plan).await?
            } else {
                let plan = self.ctx.state().statement_to_plan(statement).await?;
                self.ctx.execute_logical_plan(plan).await?
            };

            let batches = df.collect().await?;

            let options = FormatOptions::default();

            for batch in batches {
                let converters = batch
                    .columns()
                    .iter()
                    .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
                    .collect::<Result<Vec<_>, _>>()?;
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(batch.num_columns());
                    for converter in converters.iter() {
                        let mut buffer = String::with_capacity(8);
                        converter.value(row_idx).write(&mut buffer)?;
                        row.push(buffer);
                    }
                    result.push(row);
                }
            }
        }
        Ok(result)
    }

    /// Executes the `execute` task.
    async fn task_execute(&mut self, r: &mut String, sql: &str, flags: &[String]) -> Result<()> {
        use std::fmt::Write;
        let use_df_logical = flags.contains(&"use_df_logical".to_string());
        let result = self.execute(sql, use_df_logical).await?;
        writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
        writeln!(r)?;
        Ok(())
    }

    /// Executes the `explain` task.
    async fn task_explain(
        &mut self,
        r: &mut String,
        sql: &str,
        task: &str,
        flags: &[String],
    ) -> Result<()> {
        use std::fmt::Write;

        let use_df_logical = flags.contains(&"use_df_logical".to_string());
        let verbose = flags.contains(&"verbose".to_string());
        let explain_sql = if verbose {
            format!("explain verbose {}", &sql)
        } else {
            format!("explain {}", &sql)
        };
        let result = self.execute(&explain_sql, use_df_logical).await?;
        let subtask_start_pos = task.find(':').unwrap() + 1;
        for subtask in task[subtask_start_pos..].split(',') {
            let subtask = subtask.trim();
            if subtask == "logical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after datafusion")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd_heuristic" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd-heuristic")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "physical_optd" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "join_orders" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd-all-join-orders")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
                writeln!(r)?;
            } else if subtask == "logical_join_orders" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd-all-logical-join-orders")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
                writeln!(r)?;
            } else if subtask == "physical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl sqlplannertest::PlannerTestRunner for DatafusionDBMS {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        for before in &test_case.before_sql {
            self.execute(before, true)
                .await
                .context("before execution error")?;
        }

        let mut result = String::new();
        let r = &mut result;
        for task in &test_case.tasks {
            let flags = extract_flags(task)?;
            if task.starts_with("execute") {
                self.task_execute(r, &test_case.sql, &flags).await?;
            } else if task.starts_with("explain") {
                self.task_explain(r, &test_case.sql, task, &flags).await?;
            }
        }
        Ok(result)
    }
}

lazy_static! {
    static ref FLAGS_REGEX: Regex = Regex::new(r"\[(.*)\]").unwrap();
}

/// Extract the flags from a task. The flags are specified in square brackets.
/// For example, the flags for the task `explain[use_df_logical, verbose]` are `["use_df_logical", "verbose"]`.
fn extract_flags(task: &str) -> Result<Vec<String>> {
    if let Some(captures) = FLAGS_REGEX.captures(task) {
        Ok(captures
            .get(1)
            .unwrap()
            .as_str()
            .split(',')
            .map(|x| x.trim().to_string())
            .collect())
    } else {
        Ok(vec![])
    }
}
