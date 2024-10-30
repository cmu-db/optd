use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    benchmark::Benchmark,
    cardbench::CardbenchRunnerDBMSHelper,
    job::{JobKit, JobKitConfig},
    tpch::{TpchKit, TpchKitConfig},
};
use async_trait::async_trait;
use datafusion::{
    arrow::util::display::{ArrayFormatter, FormatOptions},
    execution::{
        config::SessionConfig,
        context::{SessionContext, SessionState},
        options::CsvReadOptions,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    sql::{parser::DFParser, sqlparser::dialect::GenericDialect},
};

use datafusion_optd_cli::helper::unescape_input;
use lazy_static::lazy_static;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use optd_datafusion_repr_adv_cost::{
    adv_cost::stats::{DataFusionBaseTableStats, DataFusionPerTableStats},
    new_physical_adv_cost,
};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use rayon::prelude::*;
use regex::Regex;

pub struct DatafusionDBMS {
    workspace_dpath: PathBuf,
    rebuild_cached_stats: bool,
    adaptive: bool,
    ctx: Option<SessionContext>,
}

const WITH_LOGICAL_FOR_TPCH: bool = true;
const WITH_LOGICAL_FOR_JOB: bool = true;

#[async_trait]
impl CardbenchRunnerDBMSHelper for DatafusionDBMS {
    fn get_name(&self) -> &str {
        "DataFusion"
    }

    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        let base_table_stats = self.get_benchmark_stats(benchmark).await?;
        // clear_state() is how we "load" the stats into datafusion
        self.clear_state(Some(base_table_stats), benchmark).await?;

        if self.adaptive {
            // We need to load the stats if we're doing adaptivity because that involves executing the queries in datafusion.
            // This function also calls create_tables().
            self.load_benchmark_data_no_stats(benchmark).await?;
        } else {
            // We only create the tables so that the optimizer doesn't work. However, we can save on the time of loading
            //   the data if we're not doing adaptivity because we won't be executing queries.
            self.create_benchmark_tables(benchmark).await?;
        }

        match benchmark {
            Benchmark::Tpch(tpch_kit_config) => self.eval_tpch_estcards(tpch_kit_config).await,
            Benchmark::Job(job_kit_config) | Benchmark::Joblight(job_kit_config) => {
                self.eval_job_estcards(job_kit_config).await
            }
        }
    }
}

impl DatafusionDBMS {
    pub async fn new<P: AsRef<Path>>(
        workspace_dpath: P,
        rebuild_cached_stats: bool,
        adaptive: bool,
    ) -> anyhow::Result<Self> {
        Ok(DatafusionDBMS {
            workspace_dpath: workspace_dpath.as_ref().to_path_buf(),
            rebuild_cached_stats,
            adaptive,
            ctx: None,
        })
    }

    /// Reset [`SessionContext`] to a clean state. But initialize the optimizer
    /// with pre-generated statistics.
    ///
    /// A more ideal way to generate statistics would be to use the `ANALYZE`
    /// command in SQL, but DataFusion does not support that yet.
    async fn clear_state(
        &mut self,
        stats: Option<DataFusionBaseTableStats>,
        benchmark: &Benchmark,
    ) -> anyhow::Result<()> {
        let use_df_logical = match benchmark {
            Benchmark::Tpch(_) => WITH_LOGICAL_FOR_TPCH,
            Benchmark::Job(_) | Benchmark::Joblight(_) => WITH_LOGICAL_FOR_JOB,
        };
        self.ctx = Some(Self::new_session_ctx(stats, self.adaptive, use_df_logical).await?);
        Ok(())
    }

    async fn new_session_ctx(
        stats: Option<DataFusionBaseTableStats>,
        adaptive: bool,
        use_df_logical: bool,
    ) -> anyhow::Result<SessionContext> {
        let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

        if !use_df_logical {
            session_config.options_mut().optimizer.max_passes = 0;
        }

        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let ctx = {
            let mut state =
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
            let optimizer: DatafusionOptimizer = new_physical_adv_cost(
                Arc::new(DatafusionCatalog::new(state.catalog_list())),
                stats.unwrap_or_default(),
                adaptive,
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

    async fn eval_tpch_estcards(
        &self,
        tpch_kit_config: &TpchKitConfig,
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_kit_config)?;

        let mut estcards = vec![];
        for (query_id, sql_fpath) in tpch_kit.get_sql_fpath_ordered_iter(tpch_kit_config)? {
            log::debug!(
                "about to evaluate datafusion's estcard for TPC-H Q{}",
                query_id
            );
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(&sql).await?;
            estcards.push(estcard);

            if self.adaptive {
                // If we're in adaptive mode, execute the query to fill the true cardinality cache.
                self.execute_query(&sql).await?;
            }
        }

        Ok(estcards)
    }

    async fn eval_job_estcards(&self, job_kit_config: &JobKitConfig) -> anyhow::Result<Vec<usize>> {
        let job_kit = JobKit::build(&self.workspace_dpath)?;

        let mut estcards = vec![];
        for (query_id, sql_fpath) in job_kit.get_sql_fpath_ordered_iter(job_kit_config)? {
            let benchmark_name = if job_kit_config.is_light {
                "JOB"
            } else {
                "JOB-light"
            };
            log::debug!(
                "about to evaluate datafusion's estcard for {} Q{}",
                benchmark_name,
                query_id
            );
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(&sql).await?;
            estcards.push(estcard);

            if self.adaptive {
                // Execute the query to fill the true cardinality cache.
                self.execute_query(&sql).await?;
            }
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

    fn get_ctx(&self) -> &SessionContext {
        self.ctx.as_ref().unwrap()
    }

    async fn eval_query_estcard(&self, sql: &str) -> anyhow::Result<usize> {
        lazy_static! {
            static ref ROW_CNT_RE: Regex = Regex::new(r"row_cnt=(\d+\.\d+)").unwrap();
        }
        let explains = Self::execute(self.get_ctx(), &format!("explain verbose {}", sql)).await?;
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

    /// This is used to execute the query in order to load the true cardinalities back into optd
    /// in order to use the adaptive cost model.
    async fn execute_query(&self, sql: &str) -> anyhow::Result<()> {
        Self::execute(self.get_ctx(), sql).await?;
        Ok(())
    }

    /// Load the data into DataFusion without building the stats used by optd.
    /// Unlike Postgres, where both data and stats are used by the same program, for this class the
    ///   data is used by DataFusion while the stats are used by optd. That is why there are two
    ///   separate functions to load them.
    async fn load_benchmark_data_no_stats(&mut self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Tpch(tpch_kit_config) => self.load_tpch_data_no_stats(tpch_kit_config).await,
            Benchmark::Job(job_kit_config) | Benchmark::Joblight(job_kit_config) => {
                self.load_job_data_no_stats(job_kit_config).await
            }
        }
    }

    /// Build the stats that optd's cost model uses, or get the stats from the cache.
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
                Benchmark::Tpch(tpch_kit_config) => self.get_tpch_stats(tpch_kit_config).await?,
                Benchmark::Job(job_kit_config) => self.get_job_stats(job_kit_config).await?,
                Benchmark::Joblight(job_kit_config) => self.get_job_stats(job_kit_config).await?,
            };

            // When self.rebuild_cached_optd_stats is true, we *don't read* from the cache but we
            //   still *do write* to the cache.
            fs::create_dir_all(stats_cache_fpath.parent().unwrap())?;
            let file = File::create(&stats_cache_fpath)?;
            serde_json::to_writer(file, &base_table_stats)?;

            Ok(base_table_stats)
        }
    }

    /// This function creates the tables for the benchmark without loading the data.
    async fn create_benchmark_tables(&mut self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Tpch(_) => {
                let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
                Self::create_tpch_tables(self.get_ctx(), &tpch_kit).await?;
            }
            Benchmark::Job(_) | Benchmark::Joblight(_) => {
                let job_kit = JobKit::build(&self.workspace_dpath)?;
                Self::create_job_tables(self.get_ctx(), &job_kit).await?;
            }
        };
        Ok(())
    }

    async fn create_tpch_tables(ctx: &SessionContext, tpch_kit: &TpchKit) -> anyhow::Result<()> {
        let ddls = fs::read_to_string(&tpch_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(ctx, ddl).await?;
        }
        Ok(())
    }

    async fn create_job_tables(ctx: &SessionContext, job_kit: &JobKit) -> anyhow::Result<()> {
        let ddls = fs::read_to_string(&job_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(ctx, ddl).await?;
        }
        Ok(())
    }

    async fn load_tpch_data_no_stats(
        &mut self,
        tpch_kit_config: &TpchKitConfig,
    ) -> anyhow::Result<()> {
        // Generate the tables and convert them to Parquet.
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_tables(tpch_kit_config)?;

        // Create the tables.
        Self::create_tpch_tables(self.get_ctx(), &tpch_kit).await?;

        // Load the data by creating an external table first and copying the data to real tables.
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_vec(tpch_kit_config, "tbl").unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            Self::execute(
                self.get_ctx(),
                &format!(
                    "create external table {}_tbl stored as csv delimiter '|' location '{}';",
                    tbl_name,
                    tbl_fpath.to_str().unwrap()
                ),
            )
            .await?;

            // Get the number of columns of this table.
            let schema = self
                .get_ctx()
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table(tbl_name)
                .await
                .unwrap()
                .schema();

            // DEBUG(phw2)
            println!("schema={}", serde_json::to_string_pretty(&schema).unwrap());

            let projection_list = (1..=schema.fields().len())
                .map(|i| format!("column_{}", i))
                .collect::<Vec<_>>()
                .join(", ");
            Self::execute(
                self.get_ctx(),
                &format!(
                    "insert into {} select {} from {}_tbl;",
                    tbl_name, projection_list, tbl_name,
                ),
            )
            .await?;
        }

        Ok(())
    }

    fn build_batch_reader(
        tbl_fpath: PathBuf,
        num_row_groups: usize,
    ) -> impl FnOnce() -> Vec<ParquetRecordBatchReader> {
        move || {
            let groups: Vec<ParquetRecordBatchReader> = (0..num_row_groups)
                .map(|group_num| {
                    let tbl_file = File::open(tbl_fpath.clone()).expect("Failed to open file");
                    let metadata =
                        ArrowReaderMetadata::load(&tbl_file, Default::default()).unwrap();

                    ParquetRecordBatchReaderBuilder::new_with_metadata(
                        tbl_file.try_clone().unwrap(),
                        metadata.clone(),
                    )
                    .with_row_groups(vec![group_num])
                    .build()
                    .unwrap()
                })
                .collect();

            groups
        }
    }

    fn gen_base_stats(tbl_paths: Vec<PathBuf>) -> anyhow::Result<DataFusionBaseTableStats> {
        let base_table_stats = Mutex::new(DataFusionBaseTableStats::default());
        let now = Instant::now();

        tbl_paths.par_iter().for_each(|tbl_fpath| {
            let tbl_name = TpchKit::get_tbl_name_from_tbl_fpath(tbl_fpath);
            let start = Instant::now();

            // We get the schema from the Parquet file, to ensure there's no divergence between
            // the context and the file we are going to read.
            // Further rounds of refactoring should adapt the entry point of stat gen.
            let tbl_file = File::open(tbl_fpath).expect("Failed to open file");
            let parquet =
                ParquetRecordBatchReaderBuilder::try_new(tbl_file.try_clone().unwrap()).unwrap();
            let schema = parquet.schema();
            println!("Table {:?} schema: {:#?}", tbl_name, schema);

            let nb_cols = schema.fields().len();
            let single_cols = (0..nb_cols).map(|v| vec![v]).collect::<Vec<_>>();

            let stats_result = DataFusionPerTableStats::from_record_batches(
                Self::build_batch_reader(tbl_fpath.clone(), parquet.metadata().num_row_groups()),
                Self::build_batch_reader(tbl_fpath.clone(), parquet.metadata().num_row_groups()),
                single_cols,
                schema.clone(),
            );

            if let Ok(per_table_stats) = stats_result {
                let mut stats = base_table_stats.lock().unwrap();
                stats.insert(tbl_name.to_string(), per_table_stats);
            }

            println!(
                "Table {:?} took in total {:?}...",
                tbl_name,
                start.elapsed()
            );
        });

        println!("Total execution time {:?}...", now.elapsed());

        let stats = base_table_stats.into_inner();
        let l = stats.unwrap();
        // Useful for debugging stats so I kept it
        // l.iter().for_each(|(table_name, stats)| {
        //     println!("Table: {} (num_rows: {})", table_name, stats.row_cnt);
        //     stats
        //         .column_comb_stats
        //         .iter()
        //         .sorted_by_key(|x| x.0[0])
        //         .for_each(|x| {
        //             let sum_freq: f64 = x.1.mcvs.frequencies().values().copied().sum();
        //             println!(
        //                 "Col: {} (n_distinct: {}) (n_frac: {}) (mcvs: {} {}) (tdigests: {:?} {:?} {:?} {:?} {:?})",
        //                 x.0[0],
        //                 x.1.ndistinct,
        //                 x.1.null_frac,
        //                 x.1.mcvs.frequencies().len(),
        //                 sum_freq,
        //                 x.1.distr.as_ref().map(|d| d.quantile(0.01)),
        //                 x.1.distr.as_ref().map(|d| d.quantile(0.25)),
        //                 x.1.distr.as_ref().map(|d| d.quantile(0.50)),
        //                 x.1.distr.as_ref().map(|d| d.quantile(0.75)),
        //                 x.1.distr.as_ref().map(|d| d.quantile(0.99)),
        //             );
        //         });
        // });
        // println!("{:#?}", stats);

        Ok(l)
    }

    // Load job data from a .csv file.
    async fn load_job_data_no_stats(
        &mut self,
        job_kit_config: &JobKitConfig,
    ) -> anyhow::Result<()> {
        let ctx = Self::new_session_ctx(None, self.adaptive, WITH_LOGICAL_FOR_JOB).await?;

        // Download the tables and convert them to Parquet.
        let job_kit = JobKit::build(&self.workspace_dpath)?;
        job_kit.download_tables(job_kit_config)?;

        // Create the tables.
        Self::create_job_tables(&ctx, &job_kit).await?;

        // Load each table using register_csv().
        let tbl_fpath_iter = job_kit.get_tbl_fpath_vec("csv").unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            let schema = ctx
                .catalog("datafusion")
                .unwrap()
                .schema("public")
                .unwrap()
                .table(tbl_name)
                .await
                .unwrap()
                .schema();
            self.get_ctx()
                .register_csv(
                    tbl_name,
                    tbl_fpath.to_str().unwrap(),
                    CsvReadOptions::new()
                        .schema(&schema)
                        .delimiter(b',')
                        .escape(b'\\'),
                )
                .await?;
        }
        Ok(())
    }

    async fn get_tpch_stats(
        &mut self,
        tpch_kit_config: &TpchKitConfig,
    ) -> anyhow::Result<DataFusionBaseTableStats> {
        // Create tables in a temporary context to get the schema provider.
        let ctx = Self::new_session_ctx(None, self.adaptive, WITH_LOGICAL_FOR_TPCH).await?;
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        Self::create_tpch_tables(&ctx, &tpch_kit).await?;
        let schema_provider = ctx.catalog("datafusion").unwrap().schema("public").unwrap();

        // Generate the tables
        tpch_kit.gen_tables(tpch_kit_config)?;
        tpch_kit
            .make_parquet_files(tpch_kit_config, schema_provider)
            .await?;
        // Compute base statistics on Parquet.
        let tbl_paths = tpch_kit.get_tbl_fpath_vec(tpch_kit_config, "parquet")?;
        assert!(tbl_paths.len() == tpch_kit.get_tbl_fpath_vec(tpch_kit_config, "tbl")?.len());
        Self::gen_base_stats(tbl_paths)
    }

    async fn get_job_stats(
        &mut self,
        job_kit_config: &JobKitConfig,
    ) -> anyhow::Result<DataFusionBaseTableStats> {
        // Create tables in a temporary context to get the schema provider.
        let ctx = Self::new_session_ctx(None, self.adaptive, WITH_LOGICAL_FOR_JOB).await?;
        let job_kit = JobKit::build(&self.workspace_dpath)?;
        Self::create_job_tables(&ctx, &job_kit).await?;
        let schema_provider = ctx.catalog("datafusion").unwrap().schema("public").unwrap();

        // Generate the tables.
        let job_kit = JobKit::build(&self.workspace_dpath)?;
        job_kit.download_tables(job_kit_config)?;
        job_kit
            .make_parquet_files(job_kit_config, schema_provider)
            .await?;

        // To get the schema of each table.
        let ctx = Self::new_session_ctx(None, self.adaptive, WITH_LOGICAL_FOR_JOB).await?;
        let ddls = fs::read_to_string(&job_kit.schema_fpath)?;
        let ddls = ddls
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        for ddl in ddls {
            Self::execute(&ctx, ddl).await?;
        }

        // Compute base statistics on Parquet.
        let tbl_paths = job_kit.get_tbl_fpath_vec("parquet").unwrap();
        assert!(tbl_paths.len() == job_kit.get_tbl_fpath_vec("csv")?.len());
        Self::gen_base_stats(tbl_paths)
    }
}

unsafe impl Send for DatafusionDBMS {}
