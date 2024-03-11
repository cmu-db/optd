use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBHelper,
    shell,
    tpch::{TpchConfig, TpchKit},
};
use anyhow::Result;
use async_trait::async_trait;
use std::{
    env::{self, consts::OS},
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};

const OPTD_DB_NAME: &str = "optd";

pub struct PostgresDb {
    verbose: bool,

    // cache these paths so we don't have to build them multiple times
    _postgres_db_dpath: PathBuf,
    pgdata_dpath: PathBuf,
    log_fpath: PathBuf,
}

/// Conventions I keep for methods of this class:
///   - Functions should be idempotent. For instance, start_postgres() should not fail if Postgres is already running
///   - Stop and start functions should be separate
///   - Setup should be done in build() unless it requires more information (like benchmark)
impl PostgresDb {
    pub async fn build(verbose: bool) -> Result<Self> {
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
        let db = PostgresDb {
            verbose,
            _postgres_db_dpath: postgres_db_dpath,
            pgdata_dpath,
            log_fpath,
        };

        // (re)start postgres
        db.install_postgres().await?;
        db.init_pgdata().await?;
        db.start_postgres().await?;

        Ok(db)
    }

    /// Installs an up-to-date version of Postgres using the OS's package manager
    async fn install_postgres(&self) -> Result<()> {
        match OS {
            "macos" => {
                if self.verbose {
                    println!("updating and upgrading brew...");
                }
                shell::run_command_with_status_check("brew update")?;
                shell::run_command_with_status_check("brew upgrade")?;

                if self.verbose {
                    println!("installing postgresql...");
                }
                shell::run_command_with_status_check("brew install postgresql")?;
            }
            _ => unimplemented!(),
        };
        Ok(())
    }

    /// Remove the pgdata dir, making sure to stop a running Postgres process if there is one
    /// If there is a Postgres process running on pgdata, it's important to stop it to avoid
    ///   corrupting it (not stopping it leads to lots of weird behavior)
    async fn remove_pgdata(&self) -> Result<()> {
        if PostgresDb::get_is_postgres_running()? {
            self.stop_postgres().await?;
        }
        shell::make_into_empty_dir(&self.pgdata_dpath)?;
        Ok(())
    }

    /// Initializes pgdata_dpath directory if it wasn't already initialized
    async fn init_pgdata(&self) -> Result<()> {
        let done_fpath = self.pgdata_dpath.join("initdb_done");
        if !done_fpath.exists() {
            if self.verbose {
                println!("running initdb...");
            }
            shell::make_into_empty_dir(&self.pgdata_dpath)?;
            shell::run_command_with_status_check(&format!(
                "initdb {}",
                self.pgdata_dpath.to_str().unwrap()
            ))?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!("skipped running initdb");
            }
        }
        Ok(())
    }

    /// Start the Postgres process if it's not already started
    /// It will always be started using the pg_ctl binary installed with the package manager
    /// It will always be started on port 5432
    async fn start_postgres(&self) -> Result<()> {
        if !PostgresDb::get_is_postgres_running()? {
            if self.verbose {
                println!("starting postgres...");
            }
            shell::run_command_with_status_check(&format!(
                "pg_ctl -D{} -l{} start",
                self.pgdata_dpath.to_str().unwrap(),
                self.log_fpath.to_str().unwrap()
            ))?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!("skipped starting postgres");
            }
        }

        Ok(())
    }

    /// Stop the Postgres process started by start_postgres()
    async fn stop_postgres(&self) -> Result<()> {
        if PostgresDb::get_is_postgres_running()? {
            if self.verbose {
                println!("stopping postgres...");
            }
            shell::run_command_with_status_check(&format!(
                "pg_ctl -D{} stop",
                self.pgdata_dpath.to_str().unwrap()
            ))?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!("skipped stopping postgres");
            }
        }

        Ok(())
    }

    /// Check whether postgres is running
    fn get_is_postgres_running() -> Result<bool> {
        Ok(Command::new("pg_isready").output()?.status.success())
    }

    /// Load the benchmark data without worrying about caching
    async fn load_benchmark_data_raw(&self, benchmark: &Benchmark) -> Result<()> {
        match benchmark {
            Benchmark::Tpch(tpch_cfg) => self.load_tpch_data(tpch_cfg).await?,
            _ => unimplemented!(),
        };
        Ok(())
    }

    async fn load_tpch_data(&self, tpch_cfg: &TpchConfig) -> Result<()> {
        // start from a clean slate
        self.remove_pgdata().await?;
        // since we deleted pgdata we'll need to re-init it
        self.init_pgdata().await?;
        // postgres must be started again since remove_pgdata() stops it
        self.start_postgres().await?;
        // load the schema. createdb should not fail since we just make a fresh pgdata
        shell::run_command_with_status_check(&format!("createdb {}", OPTD_DB_NAME))?;
        let tpch_kit = TpchKit::build(self.verbose)?;
        tpch_kit.gen_tables(tpch_cfg)?;
        shell::run_command_with_status_check(&format!(
            "psql {} -f {}",
            OPTD_DB_NAME,
            tpch_kit.schema_fpath.to_str().unwrap()
        ))?;
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_iter(tpch_cfg).unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            let copy_table_cmd = format!(
                "\\copy {} from {} csv delimiter '|'",
                tbl_name,
                tbl_fpath.to_str().unwrap()
            );
            shell::run_command_with_status_check(&format!(
                "psql {} -c \"{}\"",
                OPTD_DB_NAME, copy_table_cmd
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

    /// Load the data of a benchmark with parameters
    /// As an optimization, if this benchmark only has read-only queries and the
    ///   data currently loaded was with the same benchmark and parameters, we don't
    ///   need to load it again
    async fn load_database(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        if benchmark.is_readonly() {
            let benchmark_strid = benchmark.get_strid();
            let done_fname = format!("{}_done", benchmark_strid);
            let done_fpath = self.pgdata_dpath.join(done_fname);
            if !done_fpath.exists() {
                if self.verbose {
                    println!("loading data for {}...", benchmark_strid);
                }
                self.load_benchmark_data_raw(benchmark).await?;
                File::create(done_fpath)?;
            } else {
                #[allow(clippy::collapsible_else_if)]
                if self.verbose {
                    println!("skipped loading data for {}", benchmark_strid);
                }
            }
        } else {
            self.load_benchmark_data_raw(benchmark).await?
        }
        Ok(())
    }

    async fn eval_true_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(1)
    }

    async fn eval_est_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(5)
    }
}
