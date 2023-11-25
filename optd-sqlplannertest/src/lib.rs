use datafusion::arrow::csv::WriterBuilder;
use datafusion::common::format;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::{dialect_from_str, GenericDialect};
use datafusion_optd_cli::helper::unescape_input;
use datafusion_optd_cli::{
    exec::exec_from_commands,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
};
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;

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

    async fn execute(&self, sql: &str) -> Result<String> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = String::new();
        for statement in statements {
            let plan = self.ctx.state().statement_to_plan(statement).await?;

            let df = self.ctx.execute_logical_plan(plan).await?;
            let batches = df.collect().await?;

            let mut bytes = vec![];
            {
                let builder = WriterBuilder::new()
                    .has_headers(true)
                    .with_delimiter(' ' as u8);
                let mut writer = builder.build(&mut bytes);
                for batch in batches {
                    writer.write(&batch)?;
                }
            }
            let formatted =
                String::from_utf8(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
            result += &formatted;
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
            if task == "explain" {
                writeln!(
                    r,
                    "{}",
                    self.execute(&format!("explain {}", test_case.sql))
                        .await
                        .context("execution error")?
                )?;
                writeln!(r)?;
            }
        }
        Ok(result)
    }
}
