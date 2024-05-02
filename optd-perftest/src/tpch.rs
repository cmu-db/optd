/// A wrapper around tpch-kit
use csv2parquet::Opts;
use datafusion::catalog::schema::SchemaProvider;
use serde::{Deserialize, Serialize};

use crate::shell;
use std::env;
use std::env::consts::OS;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const TPCH_KIT_REPO_URL: &str = "https://github.com/wangpatrick57/tpch-kit.git";
pub const TPCH_KIT_POSTGRES: &str = "POSTGRESQL";
const NUM_TPCH_QUERIES: usize = 22;
pub const WORKING_QUERY_IDS: &[&str] = &[
    "2", "3", "5", "6", "7", "8", "9", "10", "12", "13", "14", "17", "19",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TpchKitConfig {
    pub dbms: String,
    pub scale_factor: f64,
    pub seed: i32,
    pub query_ids: Vec<String>,
}

impl Display for TpchKitConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Use write! macro to write formatted string to `f`
        write!(
            f,
            "TpchKitConfig(dbms={}, scale_factor={}, seed={}, query_ids={:?})",
            self.dbms, self.scale_factor, self.seed, self.query_ids
        )
    }
}

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
/// Since it's conceptually a wrapper around the repo, I chose _not_ to make
///   TpchKitConfig an initialization parameter.
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

        // setup
        env::set_var("DSS_CONFIG", kit.dbgen_dpath.to_str().unwrap());
        env::set_var("DSS_QUERY", kit.queries_dpath.to_str().unwrap());
        shell::clonepull_repo(TPCH_KIT_REPO_URL, &kit.tpch_kit_repo_dpath)?;

        log::debug!("[end] building TpchKit");
        Ok(kit)
    }

    pub fn make(&self, dbms: &str) -> io::Result<()> {
        log::debug!("[start] building dbgen");
        // we need to call "make clean" because we might have called make earlier with
        //   a different dbms
        shell::run_command_with_status_check_in_dir("make clean", &self.dbgen_dpath)?;
        shell::run_command_with_status_check_in_dir(
            &format!("make MACHINE={} DATABASE={}", TpchKit::get_machine(), dbms),
            &self.dbgen_dpath,
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
    pub fn gen_tables(&self, tpch_kit_config: &TpchKitConfig) -> io::Result<()> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(tpch_kit_config);
        let done_fpath = this_genned_tables_dpath.join("dbgen_done");
        if !done_fpath.exists() {
            self.make(&tpch_kit_config.dbms)?;
            shell::make_into_empty_dir(&this_genned_tables_dpath)?;
            env::set_var("DSS_PATH", this_genned_tables_dpath.to_str().unwrap());
            log::debug!("[start] generating tables for {}", tpch_kit_config);
            shell::run_command_with_status_check_in_dir(
                &format!("./dbgen -s{}", tpch_kit_config.scale_factor),
                &self.dbgen_dpath,
            )?;
            File::create(done_fpath)?;
            log::debug!("[end] generating tables for {}", tpch_kit_config);
        } else {
            log::debug!("[skip] generating tables for {}", tpch_kit_config);
        }
        Ok(())
    }

    pub async fn make_parquet_files(
        &self,
        tpch_kit_config: &TpchKitConfig,
        schema_provider: Arc<dyn SchemaProvider>,
    ) -> io::Result<()> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(tpch_kit_config);
        let done_fpath = this_genned_tables_dpath.join("make_parquet_done");

        if !done_fpath.exists() {
            log::debug!("[start] making parquet for {}", tpch_kit_config);
            for csv_tbl_fpath in self.get_tbl_fpath_vec(tpch_kit_config, "tbl").unwrap() {
                let tbl_name = Self::get_tbl_name_from_tbl_fpath(&csv_tbl_fpath);
                let schema = schema_provider.table(&tbl_name).await.unwrap().schema();
                let mut parquet_tbl_fpath = csv_tbl_fpath.clone();
                parquet_tbl_fpath.set_extension("parquet");
                let mut opts = Opts::new(csv_tbl_fpath, parquet_tbl_fpath.clone());
                opts.delimiter = '|';
                opts.schema = Some(schema.as_ref().clone());
                csv2parquet::convert(opts).unwrap();
            }
            File::create(done_fpath)?;
            log::debug!("[end] making parquet for {}", tpch_kit_config);
        } else {
            log::debug!("[skip] making parquet for {}", tpch_kit_config);
        }

        Ok(())
    }

    /// Generates the .sql files for all queries of TPC-H, with one .sql file per query
    pub fn gen_queries(&self, tpch_kit_config: &TpchKitConfig) -> io::Result<()> {
        let this_genned_queries_dpath = self.get_this_genned_queries_dpath(tpch_kit_config);
        let done_fpath = this_genned_queries_dpath.join("qgen_done");
        if !done_fpath.exists() {
            self.make(&tpch_kit_config.dbms)?;
            shell::make_into_empty_dir(&this_genned_queries_dpath)?;
            log::debug!("[start] generating queries for {}", tpch_kit_config);
            // we don't use -d in qgen because -r controls the substitution values we use
            for query_i in 1..=NUM_TPCH_QUERIES {
                let output = shell::run_command_with_status_check_in_dir(
                    &format!(
                        "./qgen -s{} -r{} {}",
                        tpch_kit_config.scale_factor, tpch_kit_config.seed, query_i
                    ),
                    &self.dbgen_dpath,
                )?;
                let this_genned_queries_fpath =
                    this_genned_queries_dpath.join(format!("{}.sql", query_i));
                fs::write(&this_genned_queries_fpath, output.stdout)?;
            }
            File::create(done_fpath)?;
            log::debug!("[end] generating queries for {}", tpch_kit_config);
        } else {
            log::debug!("[skip] generating queries for {}", tpch_kit_config);
        }
        Ok(())
    }

    /// If two TpchKitConfig instances would always generate the same data, then their directory
    ///   names must be the same.
    /// If two TpchKitConfig instances would *not always* generate the same data, then their
    ///   directory names must be different.
    fn get_this_genned_tables_dpath(&self, tpch_kit_config: &TpchKitConfig) -> PathBuf {
        let dname = format!(
            "db{}_sf{}",
            tpch_kit_config.dbms, tpch_kit_config.scale_factor,
        );
        self.genned_tables_dpath.join(dname)
    }

    /// Same comment as for get_this_genned_tables_dpath, but replace "data" with "queries"
    fn get_this_genned_queries_dpath(&self, tpch_kit_config: &TpchKitConfig) -> PathBuf {
        let dname = format!(
            "db{}_sf{}_sd{}",
            tpch_kit_config.dbms, tpch_kit_config.scale_factor, tpch_kit_config.seed
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

    /// Get a vector of all generated .tbl files of a given config
    pub fn get_tbl_fpath_vec(
        &self,
        tpch_kit_config: &TpchKitConfig,
        target_ext: &str,
    ) -> io::Result<Vec<PathBuf>> {
        let this_genned_tables_dpath = self.get_this_genned_tables_dpath(tpch_kit_config);
        let dirent_iter = fs::read_dir(this_genned_tables_dpath)?;

        let tbl_fpath_vec: Vec<PathBuf> = dirent_iter
            .filter_map(|dirent| dirent.ok())
            .map(|dirent| dirent.path())
            .filter(|path| {
                path.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext == target_ext)
                    .unwrap_or(false)
            })
            .collect();

        Ok(tbl_fpath_vec)
    }

    /// Get an iterator through all generated .sql files _in order_ of a given config
    /// It's important to iterate _in order_ due to the interface of CardtestRunnerDBMSHelper
    pub fn get_sql_fpath_ordered_iter(
        &self,
        tpch_kit_config: &TpchKitConfig,
    ) -> io::Result<impl Iterator<Item = (String, PathBuf)>> {
        let this_genned_queries_dpath = self.get_this_genned_queries_dpath(tpch_kit_config);
        let sql_fpath_ordered_iter =
            tpch_kit_config
                .query_ids
                .clone()
                .into_iter()
                .map(move |query_id| {
                    let this_genned_query_fpath =
                        this_genned_queries_dpath.join(format!("{}.sql", &query_id));
                    (query_id, this_genned_query_fpath)
                });
        Ok(sql_fpath_ordered_iter)
    }
}
