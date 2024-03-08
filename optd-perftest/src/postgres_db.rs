use crate::{cardtest::{Benchmark, CardtestRunnerDBHelper}, shell};
use anyhow::Result;
use async_trait::async_trait;
use std::{env::{self, consts::OS}, fs::{self, File}, path::{Path, PathBuf}};

const OPTD_DB_NAME: &str = "optd";

pub struct PostgresDb {
    verbose: bool,

    // cache these paths so we don't have to build them multiple times
    postgres_db_dpath: PathBuf,
    pgdata_dpath: PathBuf,
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

        // (re)start postgres
        // TODO(phw2): do this in next commit
        PostgresDb::install_postgres(verbose).await?;
        PostgresDb::init_pgdata(&pgdata_dpath, verbose).await?;

        // create Self
        let db = PostgresDb {verbose, postgres_db_dpath, pgdata_dpath};
        Ok(db)
    }

    /// Installs an up-to-date version of Postgres using the OS's package manager
    async fn install_postgres(verbose: bool) -> Result<()> {
        match OS {
            "macos" => {
                if verbose {
                    println!("updating and upgrading brew...");
                }
                shell::run_command_with_status_check("brew update")?;
                shell::run_command_with_status_check("brew upgrade")?;

                if verbose {
                    println!("installing postgresql...");
                }
                shell::run_command_with_status_check("brew install postgresql")?;
            },
            _ => unimplemented!(),
        };
        Ok(())
    }

    /// Initializes pgdata_dpath directory if it wasn't already initialized
    async fn init_pgdata<P>(pgdata_dpath: P, verbose: bool) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let done_fpath = pgdata_dpath.as_ref().join("initdb_done");
        if !done_fpath.exists() {
            if verbose {
                println!("running initdb...");
            }
            shell::make_into_empty_dir(&pgdata_dpath)?;
            shell::run_command_with_status_check(&format!("initdb {}", pgdata_dpath.as_ref().to_str().unwrap()))?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if verbose {
                println!(
                    "skipped running initdb"
                );
            }
        }
        Ok(())
    }

    /// Load the data of a benchmark with parameters
    /// As an optimization, if this benchmark only has read-only queries and the
    ///   data currently loaded was with the same benchmark and parameters, we don't
    ///   need to load it again
    pub async fn load_benchmark_data<P>(pgdata_dpath: P, verbose: bool) -> Result<()>
    where
        P: AsRef<Path>,
    {
        shell::run_command_with_status_check(&format!("create db {}", OPTD_DB_NAME))?;
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
