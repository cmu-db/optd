use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBHelper,
    tpch::{TpchConfig, TpchKit},
};
use async_trait::async_trait;
use datafusion::{
    arrow::util::display::{ArrayFormatter, FormatOptions},
    execution::{
        config::SessionConfig,
        context::{SessionContext, SessionState},
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    sql::{parser::DFParser, sqlparser::dialect::GenericDialect},
};
use datafusion_optd_cli::helper::unescape_input;
use lazy_static::lazy_static;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::{cost::BaseTableStats, DatafusionOptimizer};
use regex::Regex;

pub struct DatafusionDb {
    workspace_dpath: PathBuf,
    ctx: SessionContext,
}

#[async_trait]
impl CardtestRunnerDBHelper for DatafusionDb {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        self.clear_state().await?;
        self.load_benchmark_data(benchmark).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_estcards(tpch_config).await,
        }
    }

    async fn eval_benchmark_truecards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        self.clear_state().await?;
        self.load_benchmark_data(benchmark).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_truecards(tpch_config).await,
        }
    }
}

impl DatafusionDb {
    pub async fn new<P: AsRef<Path>>(workspace_dpath: P) -> anyhow::Result<Self> {
        Ok(DatafusionDb {
            workspace_dpath: workspace_dpath.as_ref().to_path_buf(),
            ctx: Self::new_session_ctx().await?,
        })
    }

    /// Reset data and metadata.
    async fn clear_state(&mut self) -> anyhow::Result<()> {
        self.ctx = Self::new_session_ctx().await?;
        Ok(())
    }

    async fn new_session_ctx() -> anyhow::Result<SessionContext> {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let ctx = {
            let mut state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            let optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
                Arc::new(DatafusionCatalog::new(state.catalog_list())),
                BaseTableStats::default(),
                true,
            );
            state = state.with_physical_optimizer_rules(vec![]);
            state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(ctx)
    }

    async fn execute(&self, sql: &str) -> anyhow::Result<Vec<Vec<String>>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let df = {
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

    async fn eval_tpch_estcards(&self, tpch_config: &TpchConfig) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut estcards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(&sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_tpch_truecards(&self, tpch_config: &TpchConfig) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut truecards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_truecard(&sql).await?;
            truecards.push(estcard);
        }

        Ok(truecards)
    }

    async fn eval_query_truecard(&self, sql: &str) -> anyhow::Result<usize> {
        let rows = self.execute(sql).await?;
        let num_rows = rows.len();
        Ok(num_rows)
    }

    async fn eval_query_estcard(&self, sql: &str) -> anyhow::Result<usize> {
        lazy_static! {
            static ref ROW_CNT_RE: Regex = Regex::new(r"row_cnt=(\d+\.\d+)").unwrap();
        }
        let explains = self.execute(&format!("explain verbose {}", sql)).await?;
        // Find first occurrence of row_cnt=... in the output.
        let row_cnt = explains
            .iter()
            .find_map(|explain| {
                // First element is task name, second is the actual explain output.
                assert!(explain.len() == 2);
                let explain = &explain[1];
                if let Some(caps) = ROW_CNT_RE.captures(explain) {
                    caps.get(1)
                        .map(|row_cnt| row_cnt.as_str().parse::<f32>().unwrap() as usize)
                } else {
                    None
                }
            })
            .unwrap();
        Ok(row_cnt)
    }

    async fn load_benchmark_data(&mut self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Tpch(tpch_config) => self.load_tpch_data(tpch_config).await,
            _ => unimplemented!(),
        }
    }

    async fn load_tpch_data(&mut self, tpch_config: &TpchConfig) -> anyhow::Result<()> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_tables(tpch_config)?;

        // Create the tables.
        let ddls = fs::read_to_string(&tpch_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            self.execute(ddl).await?;
        }

        // Load the data by creating an external table first and copying the data to real tables.
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_iter(tpch_config).unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            self.execute(&format!(
                "create external table {}_tbl stored as csv delimiter '|' location '{}';",
                tbl_name,
                tbl_fpath.to_str().unwrap()
            ))
            .await?;

            // Get the number of columns of this table.
            let schema = self
                .ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table(tbl_name)
                .await
                .unwrap()
                .schema();
            let projection_list = (1..=schema.fields().len())
                .map(|i| format!("column_{}", i))
                .collect::<Vec<_>>()
                .join(", ");
            self.execute(&format!(
                "insert into {} select {} from {}_tbl;",
                tbl_name, projection_list, tbl_name,
            ))
            .await?;
        }
        Ok(())
    }
}

unsafe impl Send for DatafusionDb {}
