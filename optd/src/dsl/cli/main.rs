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
use colored::*;
use optd::dsl::compile::{CompileOptions, ast_to_hir, infer, parse, registry_check};
use optd::dsl::utils::errors::{CompileError, Diagnose};
use std::error::Error;
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
                println!("{} Compiling file: {}", "⏳".blue(), input.display());
            }

            let source = read_file(input)?;
            let source_path = input.to_string_lossy().to_string();

            let options = CompileOptions {
                source_path: source_path.clone(),
            };

            // Step 1: Parse
            if *verbose {
                println!("{} Parsing source code...", "→".cyan());
            }

            let ast = match parse(&source, &options) {
                Ok(ast) => {
                    println!("{}", "Parse successful".green());
                    if *print_ast {
                        println!("\nAST Structure:\n{:#?}", ast);
                    }
                    ast
                }
                Err(errors) => handle_errors("Parse failed", &errors),
            };

            // Step 2: AST to HIR
            if *verbose {
                println!("{} Converting AST to HIR and TypeRegistry...", "→".cyan());
            }

            let (hir, mut type_registry) = match ast_to_hir(&source, ast) {
                Ok((hir, type_registry)) => {
                    println!("{}", "AST to HIR conversion successful".green());
                    if *print_typedspan_hir {
                        println!("\nTyped-Span HIR Structure:\n{:#?}", hir);
                    }
                    (hir, type_registry)
                }
                Err(error) => handle_errors("AST to HIR conversion failed", &[error]),
            };

            // Step 3: TypeRegistry Check
            if *verbose {
                println!("{} Checking TypeRegistry...", "→".cyan());
            }

            match registry_check(&source, &source_path, &type_registry) {
                Ok(_) => println!("{}", "TypeRegistry check successful".green()),
                Err(error) => handle_errors("TypeRegistry check failed", &[error]),
            }

            // Step 4: Type Inference
            if *verbose {
                println!("{} Performing type inference...", "→".cyan());
            }

            match infer(&source, &hir, &mut type_registry) {
                Ok(_) => {
                    "Type inference successful".green();
                    println!("\n{}", "Compilation completed successfully!".green().bold());
                }
                Err(errors) => handle_errors("Type inference failed", &errors),
            }
        }
    }

    Ok(())
}

fn read_file(path: &PathBuf) -> Result<String, Box<dyn Error>> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!(
                    "{} {}",
                    "✘".red().bold(),
                    format!("File not found: {}", path.display()).red()
                );
                eprintln!("  Please check the path and file permissions.\n");
            } else {
                eprintln!(
                    "{} {}",
                    "✘".red().bold(),
                    format!("Error reading file: {}", e).red()
                );
            }
            std::process::exit(1);
        }
    }
}

fn handle_errors(msg: &str, errors: &[CompileError]) -> ! {
    eprintln!("\n{} {}", "✘".red().bold(), msg.red().bold());
    eprintln!(
        "{} {}\n",
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
