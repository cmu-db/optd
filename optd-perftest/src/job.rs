/// A wrapper around job-kit
use serde::{Deserialize, Serialize};

use crate::shell;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

const JOB_KIT_REPO_URL: &str = "https://github.com/wangpatrick57/job-kit.git";
const JOB_TABLES_URL: &str = "https://homepages.cwi.nl/~boncz/job/imdb.tgz";
pub const WORKING_QUERY_IDS: &[&str] = &[
    "1a", "1b", "1c", "1d", "2a", "2b", "2c", "2d", "3a", "3b", "3c", "4a", "4b", "4c", "5a", "5b",
    "5c", "6a", "6b", "6c", "6d", "8a", "8b", "8c", "8d", "10a", "10b", "10c", "12a", "12b", "12c",
    "13a", "14a",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobConfig {
    pub query_ids: Vec<String>,
}

impl Display for JobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Use write! macro to write formatted string to `f`
        write!(f, "JobConfig(query_ids={:?})", self.query_ids,)
    }
}

/// Provides many helper functions for running a JOB workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the job-kit repo.
/// Since it's conceptually a wrapper around the repo, I chose _not_ to make
///   JobConfig an initialization parameter.
pub struct JobKit {
    _workspace_dpath: PathBuf,

    // cache these paths so we don't have to build them multiple times
    job_dpath: PathBuf,
    job_kit_repo_dpath: PathBuf,
    downloaded_tables_dpath: PathBuf,
    queries_dpath: PathBuf,
    pub schema_fpath: PathBuf,
    pub indexes_fpath: PathBuf,
}

impl JobKit {
    pub fn build<P: AsRef<Path>>(workspace_dpath: P) -> io::Result<Self> {
        log::debug!("[start] building JobKit");

        // Build paths, sometimes creating them if they don't exist
        let workspace_dpath = workspace_dpath.as_ref().to_path_buf();
        let job_dpath = workspace_dpath.join("job");
        if !job_dpath.exists() {
            fs::create_dir(&job_dpath)?;
        }
        let job_kit_repo_dpath = job_dpath.join("job-kit");
        let queries_dpath = job_kit_repo_dpath.clone();
        let downloaded_tables_dpath = job_dpath.join("downloaded_tables");
        if !downloaded_tables_dpath.exists() {
            fs::create_dir(&downloaded_tables_dpath)?;
        }
        // Note that the downloaded tables directory has a file called schematext.sql.
        // I chose to use the schema.sql in the repo itself for one simple reason: since
        //   we forked the repo, we can modify the schema.sql if necessary.
        // Note also that I copied schematext.sql into our job-kit fork *for reference only*.
        let schema_fpath = job_kit_repo_dpath.join("schema.sql");
        let indexes_fpath = job_kit_repo_dpath.join("fkindexes.sql");

        // Create Self
        let kit = JobKit {
            _workspace_dpath: workspace_dpath,
            job_dpath,
            job_kit_repo_dpath,
            queries_dpath,
            downloaded_tables_dpath,
            schema_fpath,
            indexes_fpath,
        };

        // setup
        shell::clonepull_repo(JOB_KIT_REPO_URL, &kit.job_kit_repo_dpath)?;

        log::debug!("[end] building TpchKit");
        Ok(kit)
    }

    /// Download the .csv files for all tables of JOB
    pub fn download_tables(&self, job_config: &JobConfig) -> io::Result<()> {
        let done_fpath = self.downloaded_tables_dpath.join("download_tables_done");
        if !done_fpath.exists() {
            log::debug!("[start] downloading tables for {}", job_config);
            // Instructions are from https://cedardb.com/docs/guides/example_datasets/job/, not from the job-kit repo.
            shell::run_command_with_status_check_in_dir(
                &format!("curl -O {JOB_TABLES_URL}"),
                &self.job_dpath,
            )?;
            shell::make_into_empty_dir(&self.downloaded_tables_dpath)?;
            shell::run_command_with_status_check_in_dir(
                "tar -zxvf ../imdb.tgz",
                &self.downloaded_tables_dpath,
            )?;
            shell::run_command_with_status_check_in_dir("rm imdb.tgz", &self.job_dpath)?;
            File::create(done_fpath)?;
            log::debug!("[end] downloading tables for {}", job_config);
        } else {
            log::debug!("[skip] downloading tables for {}", job_config);
        }
        Ok(())
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
    pub fn get_tbl_fpath_iter(&self) -> io::Result<impl Iterator<Item = PathBuf>> {
        let dirent_iter = fs::read_dir(&self.downloaded_tables_dpath)?;
        // all results/options are fine to be unwrapped except for path.extension() because that could
        // return None in various cases
        let path_iter = dirent_iter.map(|dirent| dirent.unwrap().path());
        let tbl_fpath_iter = path_iter
            .filter(|path| path.extension().map(|ext| ext.to_str().unwrap()) == Some("csv"));
        Ok(tbl_fpath_iter)
    }

    /// Get an iterator through all generated .sql files _in order_ of a given config
    /// It's important to iterate _in order_ due to the interface of CardtestRunnerDBMSHelper
    pub fn get_sql_fpath_ordered_iter(
        &self,
        job_config: &JobConfig,
    ) -> io::Result<impl Iterator<Item = (String, PathBuf)>> {
        let queries_dpath = self.queries_dpath.clone();
        let sql_fpath_ordered_iter =
            job_config
                .query_ids
                .clone()
                .into_iter()
                .map(move |query_id| {
                    let this_genned_query_fpath = queries_dpath.join(format!("{}.sql", &query_id));
                    (query_id, this_genned_query_fpath)
                });
        Ok(sql_fpath_ordered_iter)
    }
}
