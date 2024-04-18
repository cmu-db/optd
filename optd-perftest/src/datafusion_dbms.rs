use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBMSHelper,
    job::{JobConfig, JobKit},
    tpch::{TpchConfig, TpchKit},
};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatchIterator,
        csv::ReaderBuilder,
        util::display::{ArrayFormatter, FormatOptions},
    },
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
use optd_datafusion_repr::{
    cost::{DataFusionBaseTableStats, DataFusionPerTableStats},
    DatafusionOptimizer,
};
use regex::Regex;
pub struct DatafusionDBMS {
    workspace_dpath: PathBuf,
    rebuild_cached_stats: bool,
    ctx: SessionContext,
}

#[async_trait]
impl CardtestRunnerDBMSHelper for DatafusionDBMS {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        let base_table_stats = self.get_benchmark_stats(benchmark).await?;
        self.clear_state(Some(base_table_stats)).await?;

        match benchmark {
            Benchmark::Tpch(tpch_config) => {
                // Create the tables. This must be done after clear_state because that clears everything
                let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
                self.create_tpch_tables(&tpch_kit).await?;
                self.eval_tpch_estcards(tpch_config).await
            }
            Benchmark::Job(job_config) => {
                let job_kit = JobKit::build(&self.workspace_dpath)?;
                self.create_job_tables(&job_kit).await?;
                self.eval_job_estcards(job_config).await
            }
        }
    }
}

impl DatafusionDBMS {
    pub async fn new<P: AsRef<Path>>(
        workspace_dpath: P,
        rebuild_cached_stats: bool,
    ) -> anyhow::Result<Self> {
        Ok(DatafusionDBMS {
            workspace_dpath: workspace_dpath.as_ref().to_path_buf(),
            rebuild_cached_stats,
            ctx: Self::new_session_ctx(None).await?,
        })
    }

    /// Reset [`SessionContext`] to a clean state. But initialize the optimizer
    /// with pre-generated statistics.
    ///
    /// A more ideal way to generate statistics would be to use the `ANALYZE`
    /// command in SQL, but DataFusion does not support that yet.
    async fn clear_state(&mut self, stats: Option<DataFusionBaseTableStats>) -> anyhow::Result<()> {
        self.ctx = Self::new_session_ctx(stats).await?;
        Ok(())
    }

    async fn new_session_ctx(
        stats: Option<DataFusionBaseTableStats>,
    ) -> anyhow::Result<SessionContext> {
        let session_config = SessionConfig::from_env()?.with_information_schema(true);
        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let ctx = {
            let mut state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            let optimizer: DatafusionOptimizer = DatafusionOptimizer::new_physical(
                Arc::new(DatafusionCatalog::new(state.catalog_list())),
                stats.unwrap_or_default(),
                true,
            );
            state = state.with_physical_optimizer_rules(vec![]);
            state = state.with_query_planner(Arc::new(OptdQueryPlanner::new(optimizer)));
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok(ctx)
    }

    async fn execute(ctx: &SessionContext, sql: &str) -> anyhow::Result<Vec<Vec<String>>> {
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let df = {
                let plan = ctx.state().statement_to_plan(statement).await?;
                ctx.execute_logical_plan(plan).await?
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
        for (query_id, sql_fpath) in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            println!(
                "about to evaluate datafusion's estcard for TPC-H Q{}",
                query_id
            );
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(&sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_job_estcards(&self, job_config: &JobConfig) -> anyhow::Result<Vec<usize>> {
        let job_kit = JobKit::build(&self.workspace_dpath)?;

        let mut estcards = vec![];
        for (query_id, sql_fpath) in job_kit.get_sql_fpath_ordered_iter(job_config)? {
            println!(
                "about to evaluate datafusion's estcard for TPC-H Q{}",
                query_id
            );
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(&sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    fn log_explain(&self, explains: &[Vec<String>]) {
        // row_cnt is exclusively in physical_plan after optd
        let physical_plan_after_optd_lines = explains
            .iter()
            .find(|explain| explain.first().unwrap() == "physical_plan after optd")
            .unwrap();
        let explain_str = physical_plan_after_optd_lines.join("\n");
        log::info!("{} {}", self.get_name(), explain_str);
    }

    async fn eval_query_estcard(&self, sql: &str) -> anyhow::Result<usize> {
        lazy_static! {
            static ref ROW_CNT_RE: Regex = Regex::new(r"row_cnt=(\d+\.\d+)").unwrap();
        }
        let explains = Self::execute(&self.ctx, &format!("explain verbose {}", sql)).await?;
        self.log_explain(&explains);
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

    /// Load the data into DataFusion without building the stats used by optd.
    /// Unlike Postgres, where both data and stats are used by the same program, for this class the
    ///   data is used by DataFusion while the stats are used by optd. That is why there are two
    ///   separate functions to load them.
    #[allow(dead_code)]
    async fn load_benchmark_data_no_stats(&mut self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Tpch(tpch_config) => self.load_tpch_data_no_stats(tpch_config).await,
            _ => unimplemented!(),
        }
    }

    /// Build the stats that optd's cost model uses.
    async fn get_benchmark_stats(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<DataFusionBaseTableStats> {
        let benchmark_fname = benchmark.get_fname();
        let stats_cache_fpath = self
            .workspace_dpath
            .join("datafusion_stats_caches")
            .join(format!("{}.json", benchmark_fname));
        if !self.rebuild_cached_stats && stats_cache_fpath.exists() {
            let file = File::open(&stats_cache_fpath)?;
            Ok(serde_json::from_reader(file)?)
        } else {
            let base_table_stats = match benchmark {
                Benchmark::Tpch(tpch_config) => self.get_tpch_stats(tpch_config).await?,
                Benchmark::Job(job_config) => self.get_job_stats(job_config).await?,
            };

            // When self.rebuild_cached_optd_stats is true, we *don't read* from the cache but we
            //   still *do write* to the cache.
            fs::create_dir_all(stats_cache_fpath.parent().unwrap())?;
            let file = File::create(&stats_cache_fpath)?;
            serde_json::to_writer(file, &base_table_stats)?;

            Ok(base_table_stats)
        }
    }

    async fn create_tpch_tables(&mut self, tpch_kit: &TpchKit) -> anyhow::Result<()> {
        let ddls = fs::read_to_string(&tpch_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(&self.ctx, ddl).await?;
        }
        Ok(())
    }

    async fn create_job_tables(&mut self, job_kit: &JobKit) -> anyhow::Result<()> {
        let ddls = fs::read_to_string(&job_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(&self.ctx, ddl).await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn load_tpch_data_no_stats(&mut self, tpch_config: &TpchConfig) -> anyhow::Result<()> {
        // Generate the tables.
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_tables(tpch_config)?;

        // Create the tables.
        self.create_tpch_tables(&tpch_kit).await?;

        // Load the data by creating an external table first and copying the data to real tables.
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_iter(tpch_config).unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            Self::execute(
                &self.ctx,
                &format!(
                    "create external table {}_tbl stored as csv delimiter '|' location '{}';",
                    tbl_name,
                    tbl_fpath.to_str().unwrap()
                ),
            )
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
            Self::execute(
                &self.ctx,
                &format!(
                    "insert into {} select {} from {}_tbl;",
                    tbl_name, projection_list, tbl_name,
                ),
            )
            .await?;
        }

        Ok(())
    }

    async fn get_tpch_stats(
        &mut self,
        tpch_config: &TpchConfig,
    ) -> anyhow::Result<DataFusionBaseTableStats> {
        // Generate the tables
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_tables(tpch_config)?;

        // To get the schema of each table.
        let ctx = Self::new_session_ctx(None).await?;
        let ddls = fs::read_to_string(&tpch_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(&ctx, ddl).await?;
        }

        // Build the DataFusionBaseTableStats object.
        let mut base_table_stats = DataFusionBaseTableStats::default();
        for tbl_fpath in tpch_kit.get_tbl_fpath_iter(tpch_config).unwrap() {
            let tbl_name = TpchKit::get_tbl_name_from_tbl_fpath(&tbl_fpath);
            let schema = ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table(&tbl_name)
                .await
                .unwrap()
                .schema();

            let nb_cols = schema.fields().len();
            let single_cols = (0..nb_cols).map(|v| vec![v]);
            /*let pairwise_cols = iproduct!(0..nb_cols, 0..nb_cols)
            .filter(|(i, j)| i != j)
            .map(|(i, j)| vec![i, j]);*/

            base_table_stats.insert(
                tbl_name.to_string(),
                DataFusionPerTableStats::from_record_batches(
                    || {
                        let tbl_file = fs::File::open(&tbl_fpath)?;
                        let csv_reader1 = ReaderBuilder::new(schema.clone())
                            .has_header(false)
                            .with_delimiter(b'|')
                            .build(tbl_file)
                            .unwrap();
                        Ok(RecordBatchIterator::new(csv_reader1, schema.clone()))
                    },
                    single_cols.collect(),
                )?,
            );
        }

        Ok(base_table_stats)
    }

    async fn get_job_stats(
        &mut self,
        job_config: &JobConfig,
    ) -> anyhow::Result<DataFusionBaseTableStats> {
        // Generate the tables
        let job_kit = JobKit::build(&self.workspace_dpath)?;
        job_kit.download_tables(job_config)?;

        // To get the schema of each table.
        let ctx = Self::new_session_ctx(None).await?;
        let ddls = fs::read_to_string(&job_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(&ctx, ddl).await?;
        }

        // Build the DataFusionBaseTableStats object.
        let mut base_table_stats = DataFusionBaseTableStats::default();
        for tbl_fpath in job_kit.get_tbl_fpath_iter().unwrap() {
            let tbl_name = JobKit::get_tbl_name_from_tbl_fpath(&tbl_fpath);
            let schema = ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table(&tbl_name)
                .await
                .unwrap()
                .schema();

            let nb_cols = schema.fields().len();
            let single_cols = (0..nb_cols).map(|v| vec![v]);
            /*let pairwise_cols = iproduct!(0..nb_cols, 0..nb_cols)
            .filter(|(i, j)| i != j)
            .map(|(i, j)| vec![i, j]);*/

            base_table_stats.insert(
                tbl_name.to_string(),
                DataFusionPerTableStats::from_record_batches(
                    || {
                        let tbl_file = fs::File::open(&tbl_fpath)?;
                        let csv_reader1 = ReaderBuilder::new(schema.clone())
                            .has_header(false)
                            .with_delimiter(b',')
                            .with_escape(b'\\')
                            .build(tbl_file)
                            .unwrap();
                        Ok(RecordBatchIterator::new(csv_reader1, schema.clone()))
                    },
                    single_cols.collect(),
                )?,
            );
        }

        Ok(base_table_stats)
    }
}

unsafe impl Send for DatafusionDBMS {}
