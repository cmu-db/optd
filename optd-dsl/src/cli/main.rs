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
//! optd compile path/to/file.op --print-ast --print-typedspan-hir
//!
//! # Get help:
//! optd --help
//! optd compile --help
//! ```
//!
//! When developing, you can run through cargo:
//!
//! ```
//! cargo run -- compile examples/example.opt
//! cargo run -- compile examples/example.opt --verbose
//! cargo run -- compile examples/example.opt --print-ast --print-typedspan-hir
//! ```
use clap::{Parser, Subcommand};
use compile::{CompileOptions, adt_check, ast_to_hir, parse, scope_check};
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

        /// Print the typed-span HIR in a readable format.
        #[arg(long)]
        print_typedspan_hir: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Compile {
            input,
            verbose,
            print_ast,
            print_typedspan_hir,
        } => {
            if *verbose {
                println!("Compiling file: {}", input.display());
            }

            // Read the source file.
            let source = read_file(input)?;
            let source_path = input.to_string_lossy().to_string();

            let options = CompileOptions {
                source_path: source_path.clone(),
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

            let (hir, type_registry) = match ast_to_hir(&source, ast) {
                Ok(result) => {
                    if *verbose {
                        println!("✅ AST to HIR conversion successful!");
                    }
                    result
                }
                Err(error) => handle_errors(&[error]),
            };

            // Step 3: Perform scope checking
            if *verbose {
                println!("Performing scope checking...");
            }

            match scope_check(&source, &hir) {
                Ok(_) => {
                    if *verbose {
                        println!("✅ Scope checking successful!");
                    }
                }
                Err(error) => handle_errors(&[error]),
            }

            // Step 4: Perform ADT checking
            if *verbose {
                println!("Performing ADT checking...");
            }

            match adt_check(&source, &source_path, &type_registry) {
                Ok(_) => {
                    if *verbose {
                        println!("✅ ADT checking successful!");
                    }

                    if *print_typedspan_hir {
                        println!("\nTyped-Span HIR Structure:");
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
