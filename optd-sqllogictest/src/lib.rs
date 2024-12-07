// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_cli::helper::unescape_input;
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr_adv_cost::adv_stats::stats::DataFusionBaseTableStats;
use optd_datafusion_repr_adv_cost::new_physical_adv_cost;
use std::sync::Arc;
use thiserror::Error;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::Result;
use async_trait::async_trait;
use sqllogictest::{DBOutput, DefaultColumnType};

#[derive(Default)]
pub struct DatafusionDBMS {
    ctx: SessionContext,
}

impl DatafusionDBMS {
    async fn new_inner() -> Result<Self> {
        let ctx = DatafusionDBMS::new_session_ctx().await?;
        Ok(Self { ctx })
    }

    pub async fn new() -> Result<Self, DbError> {
        Ok(Self::new_inner().await?)
    }

    async fn new_inner_no_optd() -> Result<Self> {
        let ctx = DatafusionDBMS::new_session_ctx_no_optd().await?;
        Ok(Self { ctx })
    }

    pub async fn new_no_optd() -> Result<Self, DbError> {
        Ok(Self::new_inner_no_optd().await?)
    }

    /// Creates a new session context.
    async fn new_session_ctx() -> Result<SessionContext> {
        let mut session_config = SessionConfig::from_env()?.with_information_schema(true);
        session_config.options_mut().optimizer.max_passes = 0;
        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let optd_optimizer;
        let ctx = {
            let mut state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            let optimizer = new_physical_adv_cost(
                Arc::new(DatafusionCatalog::new(state.catalog_list())),
                DataFusionBaseTableStats::default(),
                false,
            );
            // clean up optimizer rules so that we can plug in our own optimizer
            state = state.with_optimizer_rules(vec![]);
            state = state.with_physical_optimizer_rules(vec![]);
            // use optd-bridge query planner
            optd_optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
            state = state.with_query_planner(optd_optimizer.clone());
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(ctx)
    }

    /// Creates a new session context without optd
    async fn new_session_ctx_no_optd() -> Result<SessionContext> {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let ctx = {
            let state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(ctx)
    }

    pub(crate) async fn execute(&self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        assert!(
            statements.len() == 1,
            "only one statement per execution is supported"
        );
        let mut rows = Vec::new();
        let mut types = Vec::new();
        for statement in statements {
            let df = {
                let plan = self.ctx.state().statement_to_plan(statement).await?;
                self.ctx.execute_logical_plan(plan).await?
            };

            let batches = df.collect().await?;
            let options = FormatOptions::default().with_null("NULL");

            for batch in batches {
                if types.is_empty() {
                    types = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| match f.data_type() {
                            DataType::Utf8 => DefaultColumnType::Text,
                            DataType::Int32 | DataType::Int64 => DefaultColumnType::Integer,
                            DataType::Float32 | DataType::Float64 => {
                                DefaultColumnType::FloatingPoint
                            }
                            _ => DefaultColumnType::Any,
                        })
                        .collect();
                }
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
                    rows.push(row);
                }
            }
        }
        Ok(DBOutput::Rows { types, rows })
    }
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Error in DatafusionDBMS: {0}")]
    Disconnect(#[from] anyhow::Error),
}

#[async_trait]
impl sqllogictest::AsyncDB for DatafusionDBMS {
    type Error = DbError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, DbError> {
        Ok(self.execute(sql).await?)
    }
}
