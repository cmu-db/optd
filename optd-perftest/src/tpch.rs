use serde::{Deserialize, Serialize};

use crate::shell;
/// A wrapper around tpch-kit (https://github.com/gregrahn/tpch-kit)
use std::env;
use std::env::consts::OS;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

const TPCH_KIT_REPO_URL: &str = "https://github.com/wangpatrick57/tpch-kit.git";
pub const TPCH_KIT_POSTGRES: &str = "POSTGRESQL";
const NUM_TPCH_QUERIES: usize = 22;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TpchConfig {
    pub dbms: String,
    pub scale_factor: f64,
    pub seed: i32,
    pub query_ids: Vec<u32>,
}

impl Display for TpchConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Use write! macro to write formatted string to `f`
        write!(
            f,
            "TpchConfig(scale_factor={}, seed={})",
            self.scale_factor, self.seed
        )
    }
}

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
/// Since it's conceptually a wrapper around the repo, I chose _not_ to make
///   TpchConfig an initialization parameter.
pub struct TpchKit {
    _workspace_dpath: PathBuf,

    // cache these paths so we don't have to build them multiple times
    _tpch_dpath: PathBuf,
    tpch_kit_repo_dpath: PathBuf,
    queries_dpath: PathBuf,
    dbgen_dpath: PathBuf,
    genned_tables_dpath: PathBuf,
    genned_queries_dpath: PathBuf,
    pub schema_fpath: PathBuf,
    pub constraints_fpath: PathBuf,
    pub indexes_fpath: PathBuf,
}

/// I keep the same conventions for these methods as I do for PostgresDBMS
impl TpchKit {
    pub fn build<P: AsRef<Path>>(workspace_dpath: P) -> io::Result<Self> {
        log::debug!("[start] building TpchKit");

        // build paths, sometimes creating them if they don't exist
        let workspace_dpath = workspace_dpath.as_ref().to_path_buf();
        let tpch_dpath = workspace_dpath.join("tpch");
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
        let constraints_fpath = dbgen_dpath.join("constraints.sql");
        let indexes_fpath = dbgen_dpath.join("indexes.sql");

        // create Self
        let kit = TpchKit {
            _workspace_dpath: workspace_dpath,
            _tpch_dpath: tpch_dpath,
            tpch_kit_repo_dpath,
            queries_dpath,
            dbgen_dpath,
            genned_tables_dpath,
            genned_queries_dpath,
            schema_fpath,
            constraints_fpath,
            indexes_fpath,
        };

        // set envvars (DSS_PATH can change so we don't set it now)
        env::set_var("DSS_CONFIG", kit.dbgen_dpath.to_str().unwrap());
        env::set_var("DSS_QUERY", kit.queries_dpath.to_str().unwrap());

        // do setup after creating kit
        kit.clonepull_tpch_kit_repo()?;

        log::debug!("[end] building TpchKit");
        Ok(kit)
    }

    fn clonepull_tpch_kit_repo(&self) -> io::Result<()> {
        if !self.tpch_kit_repo_dpath.exists() {
            log::debug!("[start] cloning tpch-kit repo");
            shell::run_command_with_status_check(&format!(
                "git clone {} {}",
                TPCH_KIT_REPO_URL,
                self.tpch_kit_repo_dpath.to_str().unwrap()
            ))?;
            log::debug!("[end] cloning tpch-kit repo");
        } else {
            log::debug!("[skip] cloning tpch-kit repo");
        }
        log::debug!("[start] pulling latest tpch-kit repo");
        shell::run_command_with_status_check_in_dir("git pull", Some(&self.tpch_kit_repo_dpath))?;
        log::debug!("[end] pulling latest tpch-kit repo");
        // make sure to do this so that get_optd_root() doesn't break
        Ok(())
    }

    pub fn make(&self, dbms: &str) -> io::Result<()> {
        log::debug!("[start] building dbgen");
        // we need to call "make clean" because we might have called make earlier with
        //   a different dbms
        shell::run_command_with_status_check_in_dir("make clean", Some(&self.dbgen_dpath))?;
        shell::run_command_with_status_check_in_dir(
            &format!("make MACHINE={} DATABASE={}", TpchKit::get_machine(), dbms),
            Some(&self.dbgen_dpath),
        )?;
        log::debug!("[end] building dbgen");
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

    /// Generates the .tbl files for all tables of TPC-H
    pub fn gen_tables(&self, tpch_config: &TpchConfig) -> io::Result<()> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(tpch_config);
        let done_fpath = this_genned_tables_dpath.join("dbgen_done");
        if !done_fpath.exists() {
            self.make(&tpch_config.dbms)?;
            shell::make_into_empty_dir(&this_genned_tables_dpath)?;
            env::set_var("DSS_PATH", this_genned_tables_dpath.to_str().unwrap());
            log::debug!("[start] generating tables for {}", tpch_config);
            shell::run_command_with_status_check_in_dir(
                &format!("./dbgen -s{}", tpch_config.scale_factor),
                Some(&self.dbgen_dpath),
            )?;
            File::create(done_fpath)?;
            log::debug!("[end] generating tables for {}", tpch_config);
        } else {
            log::debug!("[skip] generating tables for {}", tpch_config);
        }
        Ok(())
    }

    /// Generates the .sql files for all queries of TPC-H, with one .sql file per query
    pub fn gen_queries(&self, tpch_config: &TpchConfig) -> io::Result<()> {
        let this_genned_queries_dpath = self.get_this_genned_queries_dpath(tpch_config);
        let done_fpath = this_genned_queries_dpath.join("qgen_done");
        if !done_fpath.exists() {
            self.make(&tpch_config.dbms)?;
            shell::make_into_empty_dir(&this_genned_queries_dpath)?;
            log::debug!("[start] generating queries for {}", tpch_config);
            // we don't use -d in qgen because -r controls the substitution values we use
            for query_i in 1..=NUM_TPCH_QUERIES {
                let output = shell::run_command_with_status_check_in_dir(
                    &format!(
                        "./qgen -s{} -r{} {}",
                        tpch_config.scale_factor, tpch_config.seed, query_i
                    ),
                    Some(&self.dbgen_dpath),
                )?;
                let this_genned_queries_fpath =
                    this_genned_queries_dpath.join(format!("{}.sql", query_i));
                fs::write(&this_genned_queries_fpath, output.stdout)?;
            }
            File::create(done_fpath)?;
            log::debug!("[end] generating queries for {}", tpch_config);
        } else {
            log::debug!("[skip] generating queries for {}", tpch_config);
        }
        Ok(())
    }

    /// If two TpchConfig instances would always generate the same data, then their directory
    ///   names must be the same.
    /// If two TpchConfig instances would *not always* generate the same data, then their
    ///   directory names must be different.
    fn get_this_genned_tables_dpath(&self, tpch_config: &TpchConfig) -> PathBuf {
        let dname = format!("db{}_sf{}", tpch_config.dbms, tpch_config.scale_factor,);
        self.genned_tables_dpath.join(dname)
    }

    /// Same comment as for get_this_genned_tables_dpath, but replace "data" with "queries"
    fn get_this_genned_queries_dpath(&self, tpch_config: &TpchConfig) -> PathBuf {
        let dname = format!(
            "db{}_sf{}_sd{}",
            tpch_config.dbms, tpch_config.scale_factor, tpch_config.seed
        );
        self.genned_queries_dpath.join(dname)
    }

    /// Convert a tbl_fpath into the table name
    pub fn get_tbl_name_from_tbl_fpath<P: AsRef<Path>>(tbl_fpath: P) -> String {
        tbl_fpath
            .as_ref()
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Get an iterator through all generated .tbl files of a given config
    pub fn get_tbl_fpath_iter(
        &self,
        tpch_config: &TpchConfig,
    ) -> io::Result<impl Iterator<Item = PathBuf>> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(tpch_config);
        let dirent_iter = fs::read_dir(this_genned_tables_dpath)?;
        // all results/options are fine to be unwrapped except for path.extension() because that could
        // return None in various cases
        let path_iter = dirent_iter.map(|dirent| dirent.unwrap().path());
        let tbl_fpath_iter = path_iter
            .filter(|path| path.extension().map(|ext| ext.to_str().unwrap()) == Some("tbl"));
        Ok(tbl_fpath_iter)
    }

    /// Get an iterator through all generated .sql files _in order_ of a given config
    /// It's important to iterate _in order_ due to the interface of CardtestRunnerDBMSHelper
    pub fn get_sql_fpath_ordered_iter(
        &self,
        tpch_config: &TpchConfig,
    ) -> io::Result<impl Iterator<Item = (u32, PathBuf)>> {
        let this_genned_queries_dpath = self.get_this_genned_queries_dpath(tpch_config);
        let sql_fpath_ordered_iter =
            tpch_config
                .query_ids
                .clone()
                .into_iter()
                .map(move |query_id| {
                    (
                        query_id,
                        this_genned_queries_dpath.join(format!("{}.sql", query_id)),
                    )
                });
        Ok(sql_fpath_ordered_iter)
    }
}
