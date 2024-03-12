use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBHelper,
    shell,
    tpch::{TpchConfig, TpchKit},
};
use async_trait::async_trait;
use regex::Regex;
use std::{
    env::{self, consts::OS},
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};
use tokio_postgres::{Client, NoTls};

const OPTD_DBNAME: &str = "optd";

pub struct PostgresDb {
    // is an option because we need to initialize the struct before setting this
    client: Option<Client>,

    // cache these paths so we don't have to build them multiple times
    _postgres_db_dpath: PathBuf,
    pgdata_dpath: PathBuf,
    log_fpath: PathBuf,
}

/// Conventions I keep for methods of this class:
///   - Functions should be idempotent. For instance, start_postgres() should not fail if Postgres is already running
///       - For instance, this is why "createdb" is _not_ a function
///   - Stop and start functions should be separate
///   - Setup should be done in build() unless it requires more information (like benchmark)
impl PostgresDb {
    pub async fn build() -> anyhow::Result<Self> {
        // build paths, sometimes creating them if they don't exist
        let curr_dpath = env::current_dir()?;
        let postgres_db_dpath = Path::new(file!())
            .parent()
            .unwrap()
            .join("postgres_db")
            .to_path_buf();
        let postgres_db_dpath = curr_dpath.join(postgres_db_dpath); // make it absolute
        if !postgres_db_dpath.exists() {
            fs::create_dir(&postgres_db_dpath)?;
        }
        let pgdata_dpath = postgres_db_dpath.join("pgdata");
        let log_fpath = postgres_db_dpath.join("postgres_log");

        // create Self
        let mut db = PostgresDb {
            client: None,
            _postgres_db_dpath: postgres_db_dpath,
            pgdata_dpath,
            log_fpath,
        };

        // start postgres and our connection to it
        db.install_postgres().await?;
        db.init_pgdata().await?;
        db.start_postgres().await?;
        db.connect_to_postgres().await?;

        Ok(db)
    }

    /// Installs an up-to-date version of Postgres using the OS's package manager
    async fn install_postgres(&self) -> anyhow::Result<()> {
        match OS {
            "macos" => {
                log::debug!("[start] updating and upgrading brew");
                shell::run_command_with_status_check("brew update")?;
                shell::run_command_with_status_check("brew upgrade")?;
                log::debug!("[end] updating and upgrading brew");

                log::debug!("[start] installing postgresql");
                shell::run_command_with_status_check("brew install postgresql")?;
                log::debug!("[end] installing postgresql");
            }
            _ => unimplemented!(),
        };
        Ok(())
    }

    /// Remove the pgdata dir, making sure to stop a running Postgres process if there is one
    /// If there is a Postgres process running on pgdata, it's important to stop it to avoid
    ///   corrupting it (not stopping it leads to lots of weird behavior)
    async fn remove_pgdata(&self) -> anyhow::Result<()> {
        if PostgresDb::get_is_postgres_running()? {
            self.stop_postgres().await?;
        }
        shell::make_into_empty_dir(&self.pgdata_dpath)?;
        Ok(())
    }

    /// Initializes pgdata_dpath directory if it wasn't already initialized
    /// Create the optd database if initdb was just called. The reason I create
    ///   it here is because createdb crashes if the database already exists,
    ///   and there's no simple way to say "create db if not existing".
    async fn init_pgdata(&self) -> anyhow::Result<()> {
        let done_fpath = self.pgdata_dpath.join("initdb_done");
        if !done_fpath.exists() {
            log::debug!("[start] initializing pgdata");

            // call initdb
            shell::make_into_empty_dir(&self.pgdata_dpath)?;
            shell::run_command_with_status_check(&format!(
                "initdb {}",
                self.pgdata_dpath.to_str().unwrap()
            ))?;

            // create the database. createdb should not fail since we just make a fresh pgdata
            self.start_postgres().await?;
            shell::run_command_with_status_check(&format!("createdb {}", OPTD_DBNAME))?;
            self.stop_postgres().await?;

            // mark done
            File::create(done_fpath)?;

            log::debug!("[end] initializing pgdata");
        } else {
            log::debug!("[skip] initializing pgdata");
        }
        Ok(())
    }

    /// Start the Postgres process if it's not already started
    /// It will always be started using the pg_ctl binary installed with the package manager
    /// It will always be started on port 5432
    async fn start_postgres(&self) -> anyhow::Result<()> {
        if !PostgresDb::get_is_postgres_running()? {
            log::debug!("[start] starting postgres");
            shell::run_command_with_status_check(&format!(
                "pg_ctl -D{} -l{} start",
                self.pgdata_dpath.to_str().unwrap(),
                self.log_fpath.to_str().unwrap()
            ))?;
            log::debug!("[end] starting postgres");
        } else {
            log::debug!("[skip] starting postgres");
        }

        Ok(())
    }

    /// Stop the Postgres process started by start_postgres()
    async fn stop_postgres(&self) -> anyhow::Result<()> {
        if PostgresDb::get_is_postgres_running()? {
            log::debug!("[start] stopping postgres");
            shell::run_command_with_status_check(&format!(
                "pg_ctl -D{} stop",
                self.pgdata_dpath.to_str().unwrap()
            ))?;
            log::debug!("[end] stopping postgres");
        } else {
            log::debug!("[skip] stopping postgres");
        }

        Ok(())
    }

    /// Check whether postgres is running
    fn get_is_postgres_running() -> anyhow::Result<bool> {
        Ok(Command::new("pg_isready").output()?.status.success())
    }

    /// Create a connection to the postgres database
    async fn connect_to_postgres(&mut self) -> anyhow::Result<()> {
        let (client, connection) =
            tokio_postgres::connect(&format!("host=localhost dbname={}", OPTD_DBNAME), NoTls)
                .await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        self.client = Some(client);
        Ok(())
    }

    async fn load_benchmark_data(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        let benchmark_stringid = benchmark.get_stringid();
        if benchmark.is_readonly() {
            let done_fname = format!("{}_done", benchmark_stringid);
            let done_fpath = self.pgdata_dpath.join(done_fname);
            if !done_fpath.exists() {
                log::debug!("[start] loading data for {}", benchmark_stringid);
                self.load_benchmark_data_raw(benchmark).await?;
                File::create(done_fpath)?;
                log::debug!("[end] loading data for {}", benchmark_stringid);
            } else {
                log::debug!("[skip] loading data for {}", benchmark_stringid);
            }
        } else {
            log::debug!("[start] loading data for {}", benchmark_stringid);
            self.load_benchmark_data_raw(benchmark).await?;
            log::debug!("[end] loading data for {}", benchmark_stringid);
        }
        Ok(())
    }

    /// Load the benchmark data without worrying about caching
    async fn load_benchmark_data_raw(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        match benchmark {
            Benchmark::Tpch(tpch_config) => self.load_tpch_data_raw(tpch_config).await?,
            _ => unimplemented!(),
        };
        Ok(())
    }

    /// Load the TPC-H data without worrying about caching
    async fn load_tpch_data_raw(&self, tpch_config: &TpchConfig) -> anyhow::Result<()> {
        // start from a clean slate
        self.remove_pgdata().await?;
        // since we deleted pgdata we'll need to re-init it
        self.init_pgdata().await?;
        // postgres must be started again since remove_pgdata() stops it
        self.start_postgres().await?;
        // load the schema
        let tpch_kit = TpchKit::build()?;
        shell::run_command_with_status_check(&format!(
            "psql {} -f {}",
            OPTD_DBNAME,
            tpch_kit.schema_fpath.to_str().unwrap()
        ))?;
        // load the tables
        tpch_kit.gen_tables(tpch_config)?;
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_iter(tpch_config).unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            let copy_table_cmd = format!(
                "\\copy {} from {} csv delimiter '|'",
                tbl_name,
                tbl_fpath.to_str().unwrap()
            );
            shell::run_command_with_status_check(&format!(
                "psql {} -c \"{}\"",
                OPTD_DBNAME, copy_table_cmd
            ))?;
        }
        Ok(())
    }
}

#[async_trait]
impl CardtestRunnerDBHelper for PostgresDb {
    fn get_name(&self) -> &str {
        "Postgres"
    }

    async fn eval_benchmark_estcards(&self, benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_estcards(tpch_config).await,
        }
    }

    async fn eval_benchmark_truecards(&self, benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_truecards(tpch_config).await,
        }
    }
}

/// This impl has helpers for ```impl CardtestRunnerDBHelper for PostgresDb```
impl PostgresDb {
    async fn eval_tpch_estcards(&self, tpch_config: &TpchConfig) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build()?;
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
        let tpch_kit = TpchKit::build()?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut truecards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let truecard = self.eval_query_truecard(&sql).await?;
            truecards.push(truecard);
        }

        Ok(truecards)
    }

    async fn eval_query_estcard(&self, sql: &str) -> anyhow::Result<usize> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .query(&format!("EXPLAIN {}", sql), &[])
            .await?;
        // the first line contains the explain of the root node
        let first_explain_line: &str = result.first().unwrap().get(0);
        let estcard = PostgresDb::extract_row_count(first_explain_line).unwrap();
        Ok(estcard)
    }

    async fn eval_query_truecard(&self, sql: &str) -> anyhow::Result<usize> {
        let rows = self.client.as_ref().unwrap().query(sql, &[]).await?;
        let truecard = rows.len();
        Ok(truecard)
    }

    /// Extract the row count from a line of an EXPLAIN output
    fn extract_row_count(explain_line: &str) -> Option<usize> {
        let re = Regex::new(r"rows=(\d+)").unwrap();
        if let Some(caps) = re.captures(explain_line) {
            if let Some(matched) = caps.get(1) {
                let rows_str = matched.as_str();
                match rows_str.parse::<usize>() {
                    Ok(row_cnt) => Some(row_cnt),
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}
