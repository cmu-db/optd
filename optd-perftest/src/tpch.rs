use std::io::Result;
use std::process::Command;
use std::path::{Path, PathBuf};

const TPCH_KIT_REPO_URL: &str = "git@github.com:gregrahn/tpch-kit.git";

pub fn test_tpch() {
    clone_tpch_kit_repo().unwrap();
}

fn get_tpch_dpath() -> PathBuf {
    // This would ideally be a const, but unwrap() makes it so it can't be a const
    Path::new(file!()).parent().unwrap().join("tpch").to_path_buf()
}

/// Runs a command, exiting the program immediately if the command fails
fn run_command_fastexit(cmd_str: &str) {
    let mut cmd_components: Vec<&str> = cmd_str.split_whitespace().collect();
    let cmd = cmd_components.remove(0);
    let args = cmd_components;
    let output = Command::new(cmd)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "```{}``` failed with ```{}```", cmd_str, String::from_utf8_lossy(&output.stderr));
}

fn clone_tpch_kit_repo() -> Result<()> {
    let tpch_kit_dpath = get_tpch_dpath().join("tpch-kit").to_str().unwrap().to_string();
    run_command_fastexit(format!("git clone {} {}", TPCH_KIT_REPO_URL, tpch_kit_dpath).as_str());
    Ok(())
}