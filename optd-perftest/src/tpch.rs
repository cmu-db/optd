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

#[derive(Clone)]
pub struct TpchConfig {
    pub database: String,
    pub scale_factor: i32,
    pub seed: i32,
}

impl TpchConfig {
    pub fn get_strid(&self) -> String {
        format!("{}_sf{}_sd{}", self.database, self.scale_factor, self.seed)
    }
}

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
pub struct TpchKit {
    verbose: bool,

    // cache these paths so we don't have to build them multiple times
    _tpch_dpath: PathBuf,
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
        let tpch_dpath = Path::new(file!())
            .parent()
            .unwrap()
            .join("tpch")
            .to_path_buf();
        let tpch_dpath = curr_dpath.join(tpch_dpath); // make it absolute
        if !tpch_dpath.exists() {
            fs::create_dir(&tpch_dpath)?;
        }
        let tpch_kit_repo_dpath = tpch_dpath.join("tpch-kit");
        let dbgen_dpath = tpch_kit_repo_dpath.join("dbgen");
        let queries_dpath = dbgen_dpath.join("queries");
        let genned_tables_dpath = tpch_dpath.join("genned_tables");
        if !genned_tables_dpath.exists() {
            fs::create_dir(&genned_tables_dpath)?;
        }
        let genned_queries_dpath = tpch_dpath.join("genned_queries");
        if !genned_queries_dpath.exists() {
            fs::create_dir(&genned_queries_dpath)?;
        }
        let schema_fpath = dbgen_dpath.join("dss.ddl");

        // create Self
        let kit = TpchKit {
            verbose,
            _tpch_dpath: tpch_dpath,
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

    pub fn gen_tables(&self, cfg: &TpchConfig) -> io::Result<()> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(cfg);
        let done_fpath = this_genned_tables_dpath.join("dbgen_done");
        if !done_fpath.exists() {
            self.build_dbgen(&cfg.database)?;
            shell::make_into_empty_dir(&this_genned_tables_dpath)?;
            env::set_current_dir(&self.dbgen_dpath)?;
            env::set_var("DSS_PATH", this_genned_tables_dpath.to_str().unwrap());
            if self.verbose {
                println!("generating tables for {}...", cfg.get_strid());
            }
            shell::run_command_with_status_check(&format!("./dbgen -s{}", cfg.scale_factor))?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!("skipped generating tables for {}", cfg.get_strid());
            }
        }
        Ok(())
    }

    pub fn gen_queries(&self, cfg: &TpchConfig) -> io::Result<()> {
        let this_genned_queries_dpath = self.get_this_genned_queries_dpath(cfg);
        let this_genned_queries_fpath = this_genned_queries_dpath.join("queries.sql");
        let done_fpath = this_genned_queries_dpath.join("qgen_done");
        if !done_fpath.exists() {
            self.build_dbgen(&cfg.database)?;
            shell::make_into_empty_dir(&this_genned_queries_dpath)?;
            env::set_current_dir(&self.dbgen_dpath)?;
            if self.verbose {
                println!("generating queries for {}...", cfg.get_strid());
            }
            let output = shell::run_command_with_status_check(&format!(
                "./qgen -s{} -r{}",
                cfg.scale_factor, cfg.seed
            ))?;
            fs::write(this_genned_queries_fpath, output.stdout)?;
            File::create(done_fpath)?;
        } else {
            #[allow(clippy::collapsible_else_if)]
            if self.verbose {
                println!("skipped generating queries for {}", cfg.get_strid());
            }
        }
        Ok(())
    }

    // TODO: migrate paths and then create the .tbl iterator
    fn get_this_genned_tables_dpath(&self, cfg: &TpchConfig) -> PathBuf {
        self.genned_tables_dpath.join(cfg.get_strid())
    }

    fn get_this_genned_queries_dpath(&self, cfg: &TpchConfig) -> PathBuf {
        self.genned_queries_dpath.join(cfg.get_strid())
    }

    pub fn get_tbl_fpath_iter(
        &self,
        cfg: &TpchConfig,
    ) -> io::Result<impl Iterator<Item = PathBuf>> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(cfg);
        let dirent_iter = fs::read_dir(this_genned_tables_dpath)?;
        // all results/options are fine to be unwrapped except for path.extension() because that could
        // return None in various cases
        let path_iter = dirent_iter.map(|dirent| dirent.unwrap().path());
        let tbl_fpath_iter = path_iter
            .filter(|path| path.extension().map(|ext| ext.to_str().unwrap()) == Some("tbl"));
        Ok(tbl_fpath_iter)
    }
}
