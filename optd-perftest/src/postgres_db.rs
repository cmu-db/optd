use crate::{cardtest::{Benchmark, CardtestRunnerDBHelper}, shell, tpch_kit::TpchKit};
use anyhow::Result;
use async_trait::async_trait;
use std::{env::{self, consts::OS}, fs::{self, File}, path::{Path, PathBuf}, process::Command};

const OPTD_DB_NAME: &str = "optd";

pub struct PostgresDb {
    verbose: bool,

    // cache these paths so we don't have to build them multiple times
    _postgres_db_dpath: PathBuf,
    pgdata_dpath: PathBuf,
    log_fpath: PathBuf,
}

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
            panic!("postgres_db_dpath ({:?}) doesn't exist. Make sure to run this script from the base optd/ dir", postgres_db_dpath);
        }
        let pgdata_dpath = postgres_db_dpath.join("pgdata");
        let log_fpath = postgres_db_dpath.join("log");

        // create Self
        let db = PostgresDb {verbose, _postgres_db_dpath: postgres_db_dpath, pgdata_dpath, log_fpath};

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
            },
            _ => unimplemented!(),
        };
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
            shell::run_command_with_status_check(&format!("initdb {}", self.pgdata_dpath.to_str().unwrap()))?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!(
                    "skipped running initdb"
                );
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
            shell::run_command_with_status_check(&format!("pg_ctl -D{} -l{} start", self.pgdata_dpath.to_str().unwrap(), self.log_fpath.to_str().unwrap()))?;
        } else {
            println!("skipped starting postgres");
        }
        
        Ok(())
    }

    fn get_is_postgres_running() -> Result<bool> {
        Ok(Command::new("pg_isready").output()?.status.success())
    }

    /// Load the data of a benchmark with parameters
    /// As an optimization, if this benchmark only has read-only queries and the
    ///   data currently loaded was with the same benchmark and parameters, we don't
    ///   need to load it again
    pub async fn load_benchmark_data(&self) -> Result<()> {
        // TODO(phw2): cache
        if self.verbose {
            println!("loading TPC-H data");
        }
        // delete pgdata entirely to start from a clean slate
        shell::make_into_empty_dir(&self.pgdata_dpath)?;
        // since we deleted pgdata we'll need to re-init it
        self.init_pgdata().await?;
        // load the schema. createdb should not fail since we just make a fresh pgdata
        shell::run_command_with_status_check(&format!("createdb {}", OPTD_DB_NAME))?;
        let tpch_kit = TpchKit::build(self.verbose)?;
        shell::run_command_with_status_check(&format!("psql {} -f {}", OPTD_DB_NAME, tpch_kit.schema_fpath.to_str().unwrap()))?;
        Ok(())
    }
}

#[async_trait]
impl CardtestRunnerDBHelper for PostgresDb {
    fn get_name(&self) -> &str {
        "Postgres"
    }

    async fn load_database(&self, _benchmark: &Benchmark) -> anyhow::Result<()> {
        Ok(())
    }

    async fn eval_true_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(1)
    }

    async fn eval_est_card(&self, _sql: &str) -> anyhow::Result<usize> {
        Ok(5)
    }
}
