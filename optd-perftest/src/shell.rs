use std::path::Path;
use std::process::{Command, Output};
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
