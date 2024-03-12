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
use optd_datafusion_repr::DatafusionOptimizer;
use regex::Regex;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{Context, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct DatafusionDb {
    ctx: SessionContext,
    /// Context enabling datafusion's logical optimizer.
    with_logical_ctx: SessionContext,
}

impl DatafusionDb {
    pub async fn new() -> Result<Self> {
        let ctx = DatafusionDb::new_session_ctx(false, None).await?;
        let with_logical_ctx =
            DatafusionDb::new_session_ctx(true, Some(ctx.state().catalog_list().clone())).await?;
        Ok(Self {
            ctx,
            with_logical_ctx,
        })
    }

    /// Creates a new session context. If the `with_logical` flag is set, datafusion's logical optimizer will be used.
    async fn new_session_ctx(
        with_logical: bool,
        catalog: Option<Arc<dyn CatalogList>>,
    ) -> Result<SessionContext> {
        let mut session_config = SessionConfig::from_env()?.with_information_schema(true);
        if !with_logical {
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
            let optimizer = DatafusionOptimizer::new_physical(Arc::new(DatafusionCatalog::new(
                state.catalog_list(),
            )));
            if !with_logical {
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

    pub async fn execute(&self, sql: &str, with_logical: bool) -> Result<Vec<Vec<String>>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let df = if with_logical {
                let plan = self
                    .with_logical_ctx
                    .state()
                    .statement_to_plan(statement)
                    .await?;
                self.with_logical_ctx.execute_logical_plan(plan).await?
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
        let with_logical = flags.contains(&"with_logical".to_string());
        let result = self.execute(sql, with_logical).await?;
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

        let with_logical = flags.contains(&"with_logical".to_string());
        let _verbose = flags.contains(&"verbose".to_string());

        let result = self
            .execute(&format!("explain {}", &sql), with_logical)
            .await?;
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
impl sqlplannertest::PlannerTestRunner for DatafusionDb {
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
/// For example, the flags for the task `explain[with_logical, verbose]` are `["with_logical", "verbose"]`.
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
