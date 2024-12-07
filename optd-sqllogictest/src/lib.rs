// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_cli::helper::unescape_input;
use mimalloc::MiMalloc;
use optd_datafusion_bridge::create_df_context;
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
        let ctx = create_df_context(None, None, None, false, false, true, None)
            .await?
            .ctx;
        Ok(ctx)
    }

    /// Creates a new session context without optd
    async fn new_session_ctx_no_optd() -> Result<SessionContext> {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let runtime_env = Arc::new(RuntimeConfig::new().build()?);
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_env)
            .build();
        let ctx = SessionContext::new_with_state(state);
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
            let options = FormatOptions::default();

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
