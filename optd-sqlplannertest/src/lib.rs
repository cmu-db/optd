use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_cli::helper::unescape_input;
use itertools::Itertools;
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{Context, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct DatafusionDb {
    ctx: SessionContext,
}

impl DatafusionDb {
    pub async fn new() -> Result<Self> {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);

        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;

        let ctx = {
            let mut state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            let optimizer = DatafusionOptimizer::new_physical(Box::new(DatafusionCatalog::new(
                state.catalog_list(),
            )));
            state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(Self { ctx })
    }

    async fn execute(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let plan = self.ctx.state().statement_to_plan(statement).await?;

            let df = self.ctx.execute_logical_plan(plan).await?;
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
                    for (_, converter) in converters.iter().enumerate() {
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
}

#[async_trait]
impl sqlplannertest::PlannerTestRunner for DatafusionDb {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        for before in &test_case.before_sql {
            self.execute(before)
                .await
                .context("before execution error")?;
        }

        use std::fmt::Write;
        let mut result = String::new();
        let r = &mut result;
        for task in &test_case.tasks {
            if task == "execute" {
                let result = self.execute(&test_case.sql).await?;
                writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
                writeln!(r)?;
            } else if task.starts_with("explain:") {
                let result = self.execute(&format!("explain {}", test_case.sql)).await?;
                for subtask in task["explain:".len()..].split(",") {
                    let subtask = subtask.trim();
                    if subtask == "join_orders" {
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
                    }
                }
            }
        }
        Ok(result)
    }
}
