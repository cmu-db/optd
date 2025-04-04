//! CLI tool for the Optimizer DSL
//!
//! This tool provides a command-line interface for the Optimizer DSL compiler.
//!
//! # Usage
//!
//! ```
//! # Compile a DSL file (parse and analyze):
//! optd compile path/to/file.op
//!
//! # Compile with verbose output:
//! optd compile path/to/file.op --verbose
//!
//! # Print intermediate representations:
//! optd compile path/to/file.op --print-ast --print-hir
//!
//! # Get help:
//! optd --help
//! optd compile --help
//! ```
//!
//! When developing, you can run through cargo:
//!
//! ```
//! cargo run -- compile examples/example.dsl
//! cargo run -- compile examples/example.dsl --verbose
//! cargo run -- compile examples/example.dsl --print-ast --print-hir
//! ```
use clap::{Parser, Subcommand};
use compile::{CompileOptions, ast_to_hir, parse};
use optd_dsl::utils::error::{CompileError, Diagnose};
use std::error::Error;
use std::fs;
use std::path::PathBuf;

mod compile;

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
    Compile {
        /// Input file to compile
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Print detailed processing information.
        #[arg(short, long)]
        verbose: bool,

        /// Print the AST in a readable format.
        #[arg(long)]
        print_ast: bool,

        /// Print the HIR in a readable format.
        #[arg(long)]
        print_hir: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Compile {
            input,
            verbose,
            print_ast,
            print_hir,
        } => {
            if *verbose {
                println!("Compiling file: {}", input.display());
            }

            // Read the source file.
            let source = read_file(input)?;

            let options = CompileOptions {
                source_path: input.to_string_lossy().to_string(),
            };

            // Step 1: Parse the source to AST.
            if *verbose {
                println!("Parsing source code...");
            }

            let ast = match parse(&source, &options) {
                Ok(ast) => {
                    if *verbose {
                        println!("✅ Parse successful!");
                    }
                    if *print_ast {
                        println!("\nAST Structure:");
                        println!("{:#?}", ast);
                    }
                    ast
                }
                Err(errors) => handle_errors(&errors),
            };

            // Step 2: Convert AST to HIR<TypeSpanned>.
            if *verbose {
                println!("Performing semantic analysis...");
            }

            match ast_to_hir(&source, ast) {
                Ok((hir, type_registry)) => {
                    if *verbose {
                        println!("✅ Semantic analysis successful!");
                    }
                    if *print_hir {
                        println!("\nHIR Structure:");
                        println!("{:#?}", hir);
                        println!("\nType Registry:");
                        println!("{:#?}", type_registry);
                    }
                    if *verbose {
                        println!("\n✅ Compilation completed successfully!");
                    } else {
                        println!("✅ Compilation successful!");
                    }
                }
                Err(error) => handle_errors(&[error]),
            }
        }
    }

    Ok(())
}

/// Helper function to read a file with improved error handling
fn read_file(path: &PathBuf) -> Result<String, Box<dyn Error>> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!("❌ Error: File not found: {}", path.display());
                eprintln!("Please check that the file exists and you have correct permissions.");
            } else {
                eprintln!("❌ Error reading file: {}", e);
            }
            std::process::exit(1);
        }
    }
}

/// Helper function to handle and display errors
fn handle_errors(errors: &[CompileError]) -> ! {
    eprintln!("❌ Operation failed with {} errors:", errors.len());
    for error in errors {
        error
            .print(std::io::stderr())
            .expect("Failed to print error");
    }
    std::process::exit(1)
}
