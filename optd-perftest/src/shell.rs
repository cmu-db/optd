use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::str;
use std::{fs, io};

/// Runs a command, exiting the program immediately if the command fails
pub fn run_command_with_status_check(cmd_str: &str) -> io::Result<Output> {
    // use shlex::split() instead of split_whitespace() to handle cases like quotes and escape chars
    let mut cmd_components: Vec<String> = shlex::split(cmd_str).unwrap();
    let cmd = cmd_components.remove(0);
    let args = cmd_components;
    let output = Command::new(cmd).args(args).output()?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "```{}``` failed with ```{}```",
                cmd_str,
                String::from_utf8_lossy(&output.stderr)
            )
            .as_str(),
        ))
    }
}

/// Make dpath an existent but empty directory.
pub fn make_into_empty_dir<P>(dpath: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    if dpath.as_ref().exists() {
        fs::remove_dir_all(&dpath)?;
    }
    if !dpath.as_ref().exists() {
        fs::create_dir(&dpath)?;
    }
    Ok(())
}

/// Get the path of the root "optd" repo directory
pub fn get_optd_root() -> io::Result<PathBuf> {
    let output = run_command_with_status_check("git rev-parse --show-toplevel")?;
    let path = str::from_utf8(&output.stdout).unwrap().trim();
    let path = PathBuf::from(path);
    Ok(path)
}

/// Can be an absolute path or a relative path. Regardless of where this CLI is run, relative paths are evaluated relative to the optd repo root.
pub fn parse_pathstr(pathstr: &str) -> io::Result<PathBuf> {
    let path = PathBuf::from(pathstr);
    let path = if path.is_relative() {
        get_optd_root()?.join(path)
    } else {
        path
    };
    Ok(path)
}
