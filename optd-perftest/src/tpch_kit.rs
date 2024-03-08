use crate::shell;
/// A wrapper around tpch-kit (https://github.com/gregrahn/tpch-kit)
use std::env;
use std::env::consts::OS;
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

const TPCH_KIT_REPO_URL: &str = "git@github.com:gregrahn/tpch-kit.git";
pub const TPCH_KIT_POSTGRES: &str = "POSTGRESQL";

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
pub struct TpchKit {
    verbose: bool,

    // cache these paths so we don't have to build them multiple times
    _tpch_kit_dpath: PathBuf,
    tpch_kit_repo_dpath: PathBuf,
    queries_dpath: PathBuf,
    dbgen_dpath: PathBuf,
    genned_tables_dpath: PathBuf,
    genned_queries_dpath: PathBuf,
    pub schema_fpath: PathBuf,
}

/// I keep the same conventions for these methods as I do for PostgresDb
impl TpchKit {
    pub fn build(verbose: bool) -> io::Result<Self> {
        // build paths, sometimes creating them if they don't exist
        let curr_dpath = env::current_dir()?;
        let tpch_kit_dpath = Path::new(file!())
            .parent()
            .unwrap()
            .join("tpch_kit")
            .to_path_buf();
        let tpch_kit_dpath = curr_dpath.join(tpch_kit_dpath); // make it absolute
        if !tpch_kit_dpath.exists() {
            panic!("tpch_kit_dpath ({:?}) doesn't exist. Make sure to run this script from the base optd/ dir", tpch_kit_dpath);
        }
        let tpch_kit_repo_dpath = tpch_kit_dpath.join("tpch-kit");
        let dbgen_dpath = tpch_kit_repo_dpath.join("dbgen");
        let queries_dpath = dbgen_dpath.join("queries");
        let genned_tables_dpath = tpch_kit_dpath.join("genned_tables");
        if !genned_tables_dpath.exists() {
            fs::create_dir(&genned_tables_dpath)?;
        }
        let genned_queries_dpath = tpch_kit_dpath.join("genned_queries");
        if !genned_queries_dpath.exists() {
            fs::create_dir(&genned_queries_dpath)?;
        }
        let schema_fpath = dbgen_dpath.join("dss.ddl");

        // create Self
        let kit = TpchKit {
            verbose,
            _tpch_kit_dpath: tpch_kit_dpath,
            tpch_kit_repo_dpath,
            queries_dpath,
            dbgen_dpath,
            genned_tables_dpath,
            genned_queries_dpath,
            schema_fpath,
        };

        // set envvars (DSS_PATH can change so we don't set it now)
        env::set_var("DSS_CONFIG", kit.dbgen_dpath.to_str().unwrap());
        env::set_var("DSS_QUERY", kit.queries_dpath.to_str().unwrap());

        // install the tpch-kit repo
        kit.clonepull_tpch_kit_repo()?;

        Ok(kit)
    }

    fn clonepull_tpch_kit_repo(&self) -> io::Result<()> {
        if !self.tpch_kit_repo_dpath.exists() {
            if self.verbose {
                println!("cloning tpch-kit repo...");
            }
            shell::run_command_with_status_check(&format!(
                "git clone {} {}",
                TPCH_KIT_REPO_URL,
                self.tpch_kit_repo_dpath.to_str().unwrap()
            ))?;
        } else {
            env::set_current_dir(&self.tpch_kit_repo_dpath)?;
            if self.verbose {
                println!("pulling latest tpch-kit repo...");
            }
            shell::run_command_with_status_check("git pull")?;
        }
        Ok(())
    }

    fn build_dbgen(&self, database: &str) -> io::Result<()> {
        env::set_current_dir(&self.dbgen_dpath)?;
        if self.verbose {
            println!("building dbgen...")
        }
        shell::run_command_with_status_check(&format!(
            "make MACHINE={} DATABASE={}",
            TpchKit::get_machine(),
            database
        ))?;
        Ok(())
    }

    fn get_machine() -> &'static str {
        match OS {
            "linux" => "LINUX",
            "macos" => "MACOS",
            "windows" => "WIN32",
            _ => unimplemented!(),
        }
    }

    pub fn gen_tables(&self, database: &str, scale_factor: i32) -> io::Result<()> {
        let this_genned_tables_dpath = self
            .genned_tables_dpath
            .join(format!("{}-sf{}", database, scale_factor));
        let done_fpath = this_genned_tables_dpath.join("dbgen_done");
        if !done_fpath.exists() {
            self.build_dbgen(database)?;
            shell::make_into_empty_dir(&this_genned_tables_dpath)?;
            env::set_current_dir(&self.dbgen_dpath)?;
            env::set_var("DSS_PATH", this_genned_tables_dpath.to_str().unwrap());
            if self.verbose {
                println!("generating tables for scale factor {}...", scale_factor);
            }
            shell::run_command_with_status_check(&format!("./dbgen -s{}", scale_factor))?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!(
                    "skipped generating tables for database={} scale_factor={}",
                    database, scale_factor
                );
            }
        }
        Ok(())
    }

    pub fn gen_queries(&self, database: &str, scale_factor: i32, seed: i32) -> io::Result<()> {
        let this_genned_queries_dpath = self
            .genned_queries_dpath
            .join(format!("{}-sf{}-sd{}", database, scale_factor, seed));
        let this_genned_queries_fpath = this_genned_queries_dpath.join("queries.sql");
        let done_fpath = this_genned_queries_dpath.join("qgen_done");
        if !done_fpath.exists() {
            self.build_dbgen(database)?;
            shell::make_into_empty_dir(&this_genned_queries_dpath)?;
            env::set_current_dir(&self.dbgen_dpath)?;
            if self.verbose {
                println!("generating queries for scale factor {}...", scale_factor);
            }
            let output = shell::run_command_with_status_check(&format!(
                "./qgen -s{} -r{}",
                scale_factor, seed
            ))?;
            fs::write(this_genned_queries_fpath, output.stdout)?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!(
                    "skipped generating queries for database={} scale_factor={} seed={}",
                    database, scale_factor, seed
                );
            }
        }
        Ok(())
    }
}
