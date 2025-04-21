//! CLI tool for the Optimizer DSL
//!
//! This tool provides a command-line interface for the Optimizer DSL compiler.
//!
//! # Usage
//!
//! ```
//! # Compile a DSL file (parse and analyze):
//! optd-cli compile path/to/file.opt
//!
//! # Compile with verbose output:
//! optd-cli compile path/to/file.opt --verbose
//!
//! # Print intermediate representations:
//! optd-cli compile path/to/file.opt --verbose --show-ast --show-typedspan-hir --show-hir
//!
//! # Get help:
//! optd-cli --help
//! optd-cli compile --help
//! ```
//!
//! When developing, you can run through cargo:
//!
//! ```
//! cargo run --bin optd-cli -- compile path/to/example.opt
//! cargo run --bin optd-cli -- compile path/to/example.opt --verbose
//! cargo run --bin optd-cli -- compile path/to/example.opt --verbose --show-ast --show-hir
//! ```

use std::collections::HashMap;

use clap::{Parser, Subcommand};
use colored::Colorize;
use optd::catalog::Catalog;
use optd::dsl::analyzer::hir::{CoreData, Udf, Value};
use optd::dsl::compile::{Config, compile_hir};
use optd::dsl::utils::errors::{CompileError, Diagnose};

#[derive(Parser)]
#[command(
    name = "optd",
    about = "Optimizer DSL compiler and toolchain",
    version,
    author
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compile a DSL file (parse and analyze).
    Compile(Config),
}

pub fn temp_fake_udf(_args: &[Value], _catalog: &dyn Catalog) -> Value {
    println!("Hello from UDF!");
    Value::new(CoreData::<Value>::None)
}

fn main() -> Result<(), Vec<CompileError>> {
    let cli = Cli::parse();

    let mut udfs = HashMap::new();
    let udf = Udf {
        func: temp_fake_udf,
    };
    udfs.insert("temp_fake_udf".to_string(), udf);

    let Commands::Compile(config) = cli.command;

    let _hir = compile_hir(config, udfs).unwrap_or_else(|errors| handle_errors(&errors));

    Ok(())
}

fn handle_errors(errors: &[CompileError]) -> ! {
    eprintln!(
        "\n{} {}\n",
        "•".yellow(),
        format!("{} error(s) encountered:", errors.len()).yellow()
    );

    for error in errors {
        error
            .print(std::io::stderr())
            .expect("Failed to print error");
    }
    std::process::exit(1);
}
