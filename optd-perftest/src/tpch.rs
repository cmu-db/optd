use std::env;
use std::io;
use std::path::{Path, PathBuf};
use crate::cmd;

const TPCH_KIT_REPO_URL: &str = "git@github.com:gregrahn/tpch-kit.git";

/// Describes a complete TPC-H "workload", which is all information needed to
/// generate the data and execute the queries.
pub struct TpchConfig {
    pub scale_factor: i32,
}

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
pub struct TpchKit {
    // cache these paths so we don't have to build them multiple times
    tpch_dpath: PathBuf,
    tpch_kit_dpath: PathBuf,
    cfg: TpchConfig,
}

impl TpchKit {
    pub fn build(cfg: TpchConfig) -> io::Result<Self> {
        let tpch_dpath = Path::new(file!()).parent().unwrap().join("tpch").to_path_buf();
        let tpch_kit_dpath = tpch_dpath.join("tpch-kit");
        let kit = TpchKit {tpch_dpath, tpch_kit_dpath, cfg};
        kit.setup_tpch_kit_repo()?;
        Ok(kit)
    }

    fn setup_tpch_kit_repo(&self) -> io::Result<()> {
        if !self.tpch_kit_dpath.exists() {
            cmd::run_command_with_status_check(format!("git clone {} {}", TPCH_KIT_REPO_URL, self.tpch_kit_dpath.to_str().unwrap()).as_str())?;
        } else {
            env::set_current_dir(&self.tpch_kit_dpath)?;
            cmd::run_command_with_status_check("git pull")?;
        }
        Ok(())
    }

    pub fn gen_tpch_tables(&self) {}
}