use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use crate::cmd;

const TPCH_KIT_REPO_URL: &str = "git@github.com:gregrahn/tpch-kit.git";
/// done files are used to indicate that an operation is done so that it can be skipped in the future
const DONE_FNAME: &str = "done";
pub const TPCH_KIT_POSTGRES: &str = "POSTGRESQL";

/// Provides many helper functions for running a TPC-H workload.
/// It does not actually execute the queries as it is meant to be DBMS-agnostic.
/// Is essentially a wrapper around the tpch-kit repo.
pub struct TpchKit {
    verbose: bool,
    // cache these paths so we don't have to build them multiple times
    tpch_kit_dpath: PathBuf,
    dbgen_dpath: PathBuf,
    tables_dpath: PathBuf,
}

impl TpchKit {
    pub fn build(verbose: bool) -> io::Result<Self> {
        let curr_dpath = env::current_dir()?;
        let tpch_dpath = Path::new(file!()).parent().unwrap().join("tpch").to_path_buf();
        let tpch_dpath = curr_dpath.join(tpch_dpath); // make it absolute
        if !tpch_dpath.exists() {
            panic!("tpch_dpath ({:?}) doesn't exist. Make sure to run this script from the base optd/ dir", tpch_dpath);
        }
        let tpch_kit_dpath = tpch_dpath.join("tpch-kit");
        let dbgen_dpath = tpch_kit_dpath.join("dbgen");
        let tables_dpath = tpch_dpath.join("tables");
        if !tables_dpath.exists() {
            fs::create_dir(&tables_dpath)?;
        }
        let kit = TpchKit {verbose, tpch_kit_dpath, dbgen_dpath, tables_dpath};
        kit.clonepull_tpch_kit_repo()?;
        Ok(kit)
    }

    fn clonepull_tpch_kit_repo(&self) -> io::Result<()> {
        if !self.tpch_kit_dpath.exists() {
            if self.verbose {
                println!("cloning tpch-kit repo...");
            }
            cmd::run_command_with_status_check(&format!("git clone {} {}", TPCH_KIT_REPO_URL, self.tpch_kit_dpath.to_str().unwrap()))?;
        } else {
            env::set_current_dir(&self.tpch_kit_dpath)?;
            if self.verbose {
                println!("pulling latest tpch-kit repo...");
            }
            cmd::run_command_with_status_check("git pull")?;
        }
        Ok(())
    }

    fn build_dbgen(&self, database: &str) -> io::Result<()> {
        env::set_current_dir(&self.dbgen_dpath)?;
        if self.verbose {
            println!("building dbgen...")
        }
        cmd::run_command_with_status_check(&format!("make MACHINE=MACOS DATABASE={}", database))?;
        Ok(())
    }

    pub fn gen_tpch_tables(&self, database: &str, scale_factor: i32) -> io::Result<()> {
        let tables_sf_dpath = self.tables_dpath.join(format!("sf_{}", scale_factor));
        let done_fpath = tables_sf_dpath.join(DONE_FNAME);
        println!("{:?}", tables_sf_dpath);
        if !done_fpath.exists() {
            self.build_dbgen(database)?;
            if !tables_sf_dpath.exists() {
                fs::create_dir(&tables_sf_dpath)?;
            }
            env::set_current_dir(&self.dbgen_dpath)?;
            env::set_var("DSS_PATH", tables_sf_dpath.to_str().unwrap());
            if self.verbose {
                println!("generating tables for scale factor {}...", scale_factor);
            }
            cmd::run_command_with_status_check(&format!("./dbgen -s{}", scale_factor))?;
            File::create(done_fpath)?;
        } else {
            if self.verbose {
                println!("skipped generating tables for scale factor {}", scale_factor);
            }
        }
        Ok(())
    }
}