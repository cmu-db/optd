//! CLI tool for the Optimizer DSL
//!
//! This tool provides a command-line interface for the Optimizer DSL compiler.
//!
//! # Usage
//!
//! ```
//! # Parse a DSL file (validate syntax):
//! optd parse path/to/file.op
//!
//! # Parse a file and print the AST:
//! optd parse path/to/file.op --print-ast
//!
//! # Get help:
//! optd --help
//! optd parse --help
//! ```
//!
//! When developing, you can run through cargo:
//!
//! ```
//! cargo run -- parse examples/example.dsl
//! cargo run -- parse examples/example.dsl --print-ast
//! ```

use clap::{Parser, Subcommand};
use optd_dsl::compiler::compile::{parse, CompileOptions};
use optd_dsl::utils::error::Diagnose;
use std::fs;
use std::path::PathBuf;

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
    /// Parse a DSL file and validate its syntax
    Parse {
        /// Input file to parse
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Print the AST in a readable format
        #[arg(long)]
        print_ast: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Parse { input, print_ast } => {
            println!("Parsing file: {}", input.display());

            // Improve file reading error handling
            let source = match fs::read_to_string(input) {
                Ok(content) => content,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        eprintln!("❌ Error: File not found: {}", input.display());
                        eprintln!(
                            "Please check that the file exists and you have correct permissions."
                        );
                    } else {
                        eprintln!("❌ Error reading file: {}", e);
                    }
                    std::process::exit(1);
                }
            };

            let options = CompileOptions {
                source_path: input.to_string_lossy().to_string(),
            };

            match parse(&source, &options) {
                Ok(ast) => {
                    println!("✅ Parse successful!");
                    if *print_ast {
                        println!("\nAST Structure:");
                        println!("{:#?}", ast);
                    }
                }
                Err(errors) => {
                    eprintln!("❌ Parse failed with {} errors:", errors.len());
                    for error in errors {
                        error.print(std::io::stderr())?;
                    }
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
